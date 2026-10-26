"""Passive recorder for the History dashboard tab.

The recorder intentionally does not hook into the mining loop.  Everything it
needs is already written by the miner: the per-streamer chart JSON, the
``now_watching`` snapshot, and the account-scoped watch-streak cache.  Polling
those files keeps the feature independent from upstream mining code, which
makes rebasing a fork much less painful.
"""

import json
import logging
import os
import re
import threading
import time

from TwitchChannelPointsMiner.history.store import (
    DEFAULT_RETENTION_DAYS,
    HistoryStore,
)

logger = logging.getLogger(__name__)

ANALYTICS_POLL_SECONDS = 15.0
STALE_WATCH_SECONDS = 180.0
MAX_WATCH_TICK_SECONDS = 300.0
RETENTION_CHECK_SECONDS = 24 * 60 * 60

_ANALYTICS_EXCLUDED_FILES = {
    "now_watching.json",
    "dashboard_prefs.json",
    "drops_by_category.json",
}

_POINTS_PATTERN = re.compile(r"([+-]\d+)")


def _clean_streamer(value):
    return str(value or "").strip().lower()


def _read_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _load_list(path):
    try:
        payload = _read_json(path)
    except FileNotFoundError:
        return None
    except (OSError, ValueError, TypeError) as error:
        logger.warning("Unable to read history source %s: %s", path, error)
        return None
    if isinstance(payload, list):
        return payload
    return None


_ANNOTATION_COLORS = {
    "#45c1ff": "watch_streak",
    "#ffe045": "prediction_made",
    "#36b535": "win",
    "#ff4545": "lose",
}


def _annotation_type(text, border_color=None):
    color = str(border_color or "").strip().lower()
    if color in _ANNOTATION_COLORS:
        return _ANNOTATION_COLORS[color]
    upper = str(text or "").upper()
    if "WATCH_STREAK" in upper:
        return "watch_streak"
    if "PREDICTION" in upper:
        return "prediction_made"
    if "WIN" in upper:
        return "win"
    if "LOSE" in upper:
        return "lose"
    return "points"


def _annotation_points(text):
    match = _POINTS_PATTERN.search(str(text or ""))
    return int(match.group(1)) if match else None


class HistoryRecorder(threading.Thread):
    """Poll existing analytics files and persist durable history rows."""

    def __init__(
        self,
        store,
        analytics_path,
        streak_cache_path,
        username,
        *,
        version=None,
        interval=ANALYTICS_POLL_SECONDS,
        retention_days=DEFAULT_RETENTION_DAYS,
        clock=time.time,
    ):
        super(HistoryRecorder, self).__init__()
        self.daemon = True
        self.name = "History Recorder"
        self.store = store
        self.analytics_path = os.fspath(analytics_path)
        self.streak_cache_path = os.fspath(streak_cache_path)
        self.username = _clean_streamer(username)
        self.version = version
        self.interval = max(1.0, float(interval))
        self.retention_days = retention_days
        self.clock = clock
        self._stop_event = threading.Event()
        self._active_watch = {}
        self._now_watching_initialized = False
        self._now_watching_mtime = 0.0
        self._run_id = None
        self._next_retention_at = 0.0
        self._streak_cache_key = None

    # -- lifecycle ---------------------------------------------------------
    def stop(self):
        self._stop_event.set()

    def run(self):
        try:
            self._prepare()
            while not self._stop_event.wait(self.interval):
                self.poll_once()
        except Exception:
            logger.exception("History recorder stopped after an unexpected error")
        finally:
            self._shutdown()

    def _prepare(self):
        self.store.initialize()
        now = self.clock()
        # A session open in the database belongs to a previous process.  Close
        # it at its last observed time rather than letting it grow forever.  If
        # the miner restarted mid-broadcast, the remainder of that broadcast is
        # intentionally not counted until the next broadcast is observed.
        self.store.close_open_watch_sessions(now)
        self.store.close_open_streams(now)
        self.store.close_open_runs()
        self._run_id = self.store.start_run(self.version, started_at=now)
        self._next_retention_at = now
        # The cache may carry an open session from an old crash.  Import those
        # as already closed so they cannot become permanent phantom live rows.
        self._ingest_streak_cache(now, close_open=True, force=True)
        self._baseline_now_watching()

    def _shutdown(self):
        now = self.clock()
        for streamer in list(self._active_watch):
            self._close_watch(streamer, self._active_watch[streamer]["last_tick"])
        self.store.end_run(self._run_id, ended_at=now)
        self._run_id = None

    def poll_once(self):
        if self._run_id is None:
            return
        now = self.clock()
        self.store.heartbeat_run(self._run_id, now)
        self.store.touch_open_streams(now)
        self._ingest_streak_cache(now)
        self._poll_now_watching(now)
        self._ingest_analytics(now)
        if now >= self._next_retention_at:
            self._apply_retention(now)

    def _apply_retention(self, now):
        self._next_retention_at = now + RETENTION_CHECK_SECONDS
        try:
            self.store.apply_retention(now=now, retention_days=self.retention_days)
        except Exception as error:
            logger.warning("Unable to apply history retention: %s", error)

    # -- now_watching ------------------------------------------------------
    def _now_watching_path(self):
        return os.path.join(self.analytics_path, "now_watching.json")

    def _baseline_now_watching(self):
        """Ignore the snapshot left behind by a previous process."""

        try:
            self._now_watching_mtime = os.path.getmtime(self._now_watching_path())
        except OSError:
            self._now_watching_mtime = 0.0
        self._now_watching_initialized = True

    def _poll_now_watching(self, now=None):
        now = self.clock() if now is None else float(now)
        path = self._now_watching_path()
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            mtime = 0.0

        if not self._now_watching_initialized:
            self._now_watching_mtime = mtime
            self._now_watching_initialized = True
            return

        if mtime == self._now_watching_mtime:
            if self._active_watch and mtime > 0 and now - mtime > STALE_WATCH_SECONDS:
                for streamer in list(self._active_watch):
                    self._close_watch(
                        streamer, self._active_watch[streamer]["last_tick"]
                    )
            return

        self._now_watching_mtime = mtime
        entries = _load_list(path)
        if entries is None:
            entries = []

        seen = set()
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            streamer = _clean_streamer(entry.get("username"))
            if not streamer:
                continue
            seen.add(streamer)
            reason = str(entry.get("reason") or "points").strip().lower()
            game = entry.get("game") or None
            active = self._active_watch.get(streamer)
            if active is not None and active["reason"] == reason:
                self._tick_watch(streamer, now)
            else:
                if active is not None:
                    self._close_watch(streamer, now)
                session_id = self.store.start_watch_session(
                    streamer,
                    reason,
                    started_at=now,
                    broadcast_id=self.store.open_broadcast(streamer),
                )
                if session_id is not None:
                    self._active_watch[streamer] = {
                        "id": session_id,
                        "reason": reason,
                        "last_tick": now,
                    }
            if game:
                self.store.update_open_stream_details(
                    streamer, timestamp=now, game=game
                )

        for streamer in list(self._active_watch):
            if streamer not in seen:
                self._close_watch(streamer, now)

    def _tick_watch(self, streamer, now):
        active = self._active_watch[streamer]
        delta = max(0.0, float(now) - float(active["last_tick"]))
        delta = min(delta, MAX_WATCH_TICK_SECONDS)
        if delta > 0:
            self.store.tick_watch_session(
                active["id"], streamer, timestamp=now, delta_seconds=delta
            )
        active["last_tick"] = float(now)

    def _close_watch(self, streamer, ended_at):
        active = self._active_watch.pop(streamer, None)
        if active is None:
            return
        self.store.end_watch_session(active["id"], ended_at=ended_at)

    # -- watch-streak cache ------------------------------------------------
    def _ingest_streak_cache(self, now=None, close_open=False, force=False):
        now = self.clock() if now is None else float(now)
        try:
            stat = os.stat(self.streak_cache_path)
        except OSError:
            self._streak_cache_key = None
            return
        cache_key = (stat.st_mtime_ns, stat.st_size)
        if not force and cache_key == self._streak_cache_key:
            return

        try:
            payload = _read_json(self.streak_cache_path)
        except (OSError, ValueError, TypeError) as error:
            logger.warning(
                "Unable to read history watch-streak cache %s: %s",
                self.streak_cache_path,
                error,
            )
            return
        self._streak_cache_key = cache_key

        if not isinstance(payload, dict):
            return
        sessions = payload.get("sessions")
        if not isinstance(sessions, list):
            return

        for item in sessions:
            if not isinstance(item, dict):
                continue
            if _clean_streamer(item.get("account_name")) != self.username:
                continue
            streamer = _clean_streamer(item.get("streamer_login"))
            broadcast_id = str(item.get("broadcast_id") or "").strip()
            try:
                started_at = float(item["started_at"])
            except (KeyError, TypeError, ValueError):
                continue
            ended_at = item.get("ended_at")
            try:
                ended_at = float(ended_at) if ended_at is not None else None
            except (TypeError, ValueError):
                ended_at = None
            if not streamer or not broadcast_id:
                continue
            if close_open and ended_at is None:
                ended_at = started_at

            self.store.upsert_stream_session(
                streamer,
                broadcast_id,
                started_at=started_at,
                ended_at=ended_at,
                last_seen_at=now if ended_at is None else ended_at,
                source="streak_cache",
            )

            claimed = bool(item.get("claimed", False))
            claimed_at = item.get("claimed_at")
            if claimed and claimed_at is not None:
                try:
                    claimed_at = float(claimed_at)
                except (TypeError, ValueError):
                    continue
                self.store.record_event(
                    streamer,
                    claimed_at,
                    "watch_streak",
                    label="Watch streak claimed",
                    metadata={
                        "broadcast_id": broadcast_id,
                        "started_at": started_at,
                    },
                    dedupe_key=f"streak:{streamer}:{broadcast_id}",
                    dedupe_window=120.0,
                )

    # -- per-streamer analytics JSON --------------------------------------
    def _analytics_files(self):
        try:
            names = sorted(os.listdir(self.analytics_path))
        except OSError:
            return []
        result = []
        for name in names:
            if not name.endswith(".json") or name in _ANALYTICS_EXCLUDED_FILES:
                continue
            path = os.path.join(self.analytics_path, name)
            if os.path.isfile(path):
                result.append((name, path))
        return result

    def _ingest_analytics(self, now=None):
        for filename, path in self._analytics_files():
            streamer = _clean_streamer(filename[: -len(".json")])
            if not streamer:
                continue
            try:
                stat = os.stat(path)
            except OSError:
                continue

            series_source = f"analytics:{filename}:series"
            annotation_source = f"analytics:{filename}:annotations"
            series_state = self._safe_import_state(series_source)
            annotation_state = self._safe_import_state(annotation_source)
            series_fresh = self._state_matches(series_state, stat)
            annotation_fresh = self._state_matches(annotation_state, stat)
            if series_fresh and annotation_fresh:
                continue

            try:
                payload = _read_json(path)
            except (OSError, ValueError, TypeError) as error:
                logger.warning("Unable to import history from %s: %s", path, error)
                continue
            if not isinstance(payload, dict):
                continue

            if not series_fresh:
                self._ingest_series(
                    streamer,
                    payload.get("series"),
                    series_source,
                    stat,
                    series_state,
                )
            if not annotation_fresh:
                self._ingest_annotations(
                    streamer,
                    payload.get("annotations"),
                    annotation_source,
                    stat,
                    annotation_state,
                )

    @staticmethod
    def _state_matches(state, stat):
        return (
            state is not None
            and state.get("mtime") == stat.st_mtime
            and state.get("size") == stat.st_size
        )

    def _safe_import_state(self, source):
        try:
            return self.store.get_import_state(source)
        except Exception:
            return None

    def _ingest_series(self, streamer, entries, source, stat, state):
        if not isinstance(entries, list):
            entries = []
        usable = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                float(entry["x"])
            except (KeyError, TypeError, ValueError):
                continue
            usable.append(entry)
        usable.sort(key=lambda entry: float(entry["x"]))

        last_x = state.get("last_x") if state else None
        last_balance = state.get("last_balance") if state else None
        if last_balance is None and usable:
            last_balance = self._baseline_balance(
                streamer, float(usable[0]["x"]) / 1000.0
            )

        for entry in usable:
            try:
                x = float(entry["x"])
            except (KeyError, TypeError, ValueError):
                continue
            if last_x is not None and x <= last_x:
                continue
            balance = entry.get("y")
            if balance is None:
                continue
            try:
                balance = int(balance)
            except (TypeError, ValueError):
                continue
            if last_balance is None:
                # First observation is the baseline; there is no delta yet.
                last_balance = balance
                last_x = x
                continue
            delta = balance - int(last_balance)
            if delta != 0:
                self.store.record_points(
                    streamer,
                    timestamp=x / 1000.0,
                    balance=balance,
                    delta=delta,
                    reason=str(entry.get("z") or ""),
                    kind="spend" if delta < 0 else "earn",
                    dedupe_key=f"series:{streamer}:{x:.3f}:{balance}:{delta}",
                )
            last_balance = balance
            last_x = x

        self.store.set_import_state(
            source,
            mtime=stat.st_mtime,
            size=stat.st_size,
            last_x=last_x,
            last_balance=last_balance,
        )

    def _baseline_balance(self, streamer, first_timestamp):
        """Use the stored balance only when it predates the import window."""

        latest = self.store.latest_point(streamer)
        if latest is None or latest["balance"] is None:
            return None
        if latest["ts"] <= first_timestamp:
            return latest["balance"]
        return None

    def _ingest_annotations(self, streamer, entries, source, stat, state):
        if not isinstance(entries, list):
            entries = []
        usable = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                float(entry["x"])
            except (KeyError, TypeError, ValueError):
                continue
            usable.append(entry)
        usable.sort(key=lambda entry: float(entry["x"]))

        last_x = state.get("last_x") if state else None
        for entry in usable:
            try:
                x = float(entry["x"])
            except (KeyError, TypeError, ValueError):
                continue
            if last_x is not None and x <= last_x:
                continue
            label = ""
            event_type = "points"
            if isinstance(entry.get("label"), dict):
                label = str(entry["label"].get("text") or "")
                event_type = _annotation_type(label, entry.get("borderColor"))
            self.store.record_event(
                streamer,
                timestamp=x / 1000.0,
                event_type=event_type,
                label=label,
                points_delta=_annotation_points(label),
                dedupe_key=f"annotation:{streamer}:{x:.3f}:{label}",
            )
            last_x = x

        self.store.set_import_state(
            source,
            mtime=stat.st_mtime,
            size=stat.st_size,
            last_x=last_x,
            last_balance=None,
        )


def build_recorder(
    analytics_path,
    streak_cache_path,
    username,
    *,
    version=None,
    retention_days=DEFAULT_RETENTION_DAYS,
    interval=ANALYTICS_POLL_SECONDS,
):
    store = HistoryStore(os.path.join(analytics_path, "history.sqlite3"))
    recorder = HistoryRecorder(
        store,
        analytics_path=analytics_path,
        streak_cache_path=streak_cache_path,
        username=username,
        version=version,
        interval=interval,
        retention_days=retention_days,
    )
    return recorder
