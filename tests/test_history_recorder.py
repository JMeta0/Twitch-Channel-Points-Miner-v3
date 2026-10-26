import json
import os
from pathlib import Path

import pytest

from TwitchChannelPointsMiner.history.recorder import HistoryRecorder
from TwitchChannelPointsMiner.history.store import HistoryStore

NOW = 1_700_000_000.0


class FakeClock:
    def __init__(self, value=NOW):
        self.value = value

    def __call__(self):
        return self.value


@pytest.fixture
def recorder(tmp_path):
    analytics = tmp_path / "analytics"
    analytics.mkdir()
    cache = tmp_path / "watch_streak_cache.alice.json"
    store = HistoryStore(analytics / "history.sqlite3")
    clock = FakeClock()
    instance = HistoryRecorder(
        store,
        analytics_path=analytics,
        streak_cache_path=cache,
        username="alice",
        clock=clock,
    )
    instance._prepare()
    yield instance
    instance._shutdown()


def _write_json(path, payload, timestamp):
    target = Path(path)
    target.write_text(json.dumps(payload), encoding="utf-8")
    os.utime(target, (timestamp, timestamp))


def test_prepare_closes_sessions_left_by_a_previous_run(tmp_path):
    analytics = tmp_path / "analytics"
    analytics.mkdir()
    store = HistoryStore(analytics / "history.sqlite3")
    store.initialize()
    session_id = store.start_watch_session("alice", "points", NOW)
    store.tick_watch_session(session_id, "alice", NOW + 30, 30)
    store.upsert_stream_session("alice", "b1", NOW)

    recorder = HistoryRecorder(
        store,
        analytics_path=analytics,
        streak_cache_path=tmp_path / "missing.json",
        username="alice",
        clock=FakeClock(NOW + 100),
    )
    recorder._prepare()
    try:
        assert store.overview(days=1, now=NOW + 100)["active_watch_sessions"] == 0
        assert store.overview(days=1, now=NOW + 100)["live_streams"] == 0
    finally:
        recorder._shutdown()


def test_recorder_imports_series_and_annotations(recorder):
    analytics = recorder.analytics_path
    path = os.path.join(analytics, "alice.json")
    _write_json(
        path,
        {
            "version": 1,
            "series": [
                {"x": int(NOW * 1000), "y": 1000, "z": "Watch"},
                {"x": int((NOW + 60) * 1000), "y": 1050, "z": "WATCH"},
            ],
            "annotations": [
                {
                    "x": int((NOW + 120) * 1000),
                    "label": {"text": "+50 - WATCH_STREAK"},
                    "borderColor": "#45c1ff",
                },
                {
                    "x": int((NOW + 180) * 1000),
                    "label": {"text": "A prediction won"},
                    "borderColor": "#36b535",
                },
            ],
        },
        NOW + 1,
    )

    recorder._ingest_analytics(NOW + 2)
    overview = recorder.store.overview(days=30, now=NOW + 120)
    assert overview["window"]["points_gained"] == 50
    assert overview["window"]["streak_count"] == 1
    event_types = {
        item["type"] for item in recorder.store.events(days=30, now=NOW + 180)["events"]
    }
    assert event_types == {"watch_streak", "win"}

    # Re-reading an unchanged file must not duplicate anything.
    recorder._ingest_analytics(NOW + 2)
    overview = recorder.store.overview(days=30, now=NOW + 120)
    assert overview["window"]["points_gained"] == 50
    assert overview["window"]["streak_count"] == 1

    _write_json(
        path,
        {
            "version": 1,
            "series": [
                {"x": int(NOW * 1000), "y": 1000, "z": "Watch"},
                {"x": int((NOW + 60) * 1000), "y": 1050, "z": "WATCH"},
                {
                    "x": int((NOW + 300) * 1000),
                    "y": 1075,
                    "z": "WATCH",
                },
            ],
            "annotations": [
                {
                    "x": int((NOW + 120) * 1000),
                    "label": {"text": "+50 - WATCH_STREAK"},
                }
            ],
        },
        NOW + 3,
    )
    recorder._ingest_analytics(NOW + 4)
    overview = recorder.store.overview(days=30, now=NOW + 300)
    assert overview["window"]["points_gained"] == 75
    assert overview["window"]["streak_count"] == 1


def test_recorder_tracks_watch_sessions_from_now_watching(recorder):
    path = os.path.join(recorder.analytics_path, "now_watching.json")
    clock = recorder.clock

    clock.value = NOW + 10
    _write_json(
        path,
        [{"username": "alice", "reason": "points", "game": "Chess"}],
        clock.value,
    )
    recorder._poll_now_watching()
    assert (
        recorder.store.overview(days=30, now=clock.value)["active_watch_sessions"] == 1
    )

    clock.value = NOW + 40
    _write_json(
        path,
        [{"username": "alice", "reason": "points", "game": "Chess"}],
        clock.value,
    )
    recorder._poll_now_watching()

    clock.value = NOW + 50
    _write_json(path, [], clock.value)
    recorder._poll_now_watching()

    sessions = recorder.store.sessions(kind="watch", days=30, now=clock.value)
    assert sessions["total"] == 1
    assert sessions["sessions"][0]["watch_seconds"] == 30
    assert sessions["sessions"][0]["ended_at"] == NOW + 50


def test_recorder_imports_streak_cache_sessions(recorder):
    clock = recorder.clock
    clock.value = NOW + 130
    payload = {
        "version": 1,
        "sessions": [
            {
                "account_name": "alice",
                "streamer_login": "bob",
                "broadcast_id": "b1",
                "started_at": NOW,
                "claimed": True,
                "claimed_at": NOW + 120,
                "ended_at": None,
            }
        ],
    }
    _write_json(recorder.streak_cache_path, payload, clock.value)

    recorder._ingest_streak_cache()
    recorder._ingest_streak_cache()

    sessions = recorder.store.sessions(kind="stream", days=1, now=clock.value)
    assert sessions["total"] == 1
    assert sessions["sessions"][0]["active"] is True

    events = recorder.store.events(days=1, now=clock.value)
    assert events["total"] == 1
    assert events["events"][0]["type"] == "watch_streak"


def test_recorder_reason_change_starts_a_new_watch_session(recorder):
    path = os.path.join(recorder.analytics_path, "now_watching.json")
    clock = recorder.clock

    clock.value = NOW + 10
    _write_json(path, [{"username": "alice", "reason": "points"}], clock.value)
    recorder._poll_now_watching()

    clock.value = NOW + 35
    _write_json(path, [{"username": "alice", "reason": "streak"}], clock.value)
    recorder._poll_now_watching()

    clock.value = NOW + 50
    _write_json(path, [], clock.value)
    recorder._poll_now_watching()

    sessions = recorder.store.sessions(kind="watch", days=1, now=clock.value)
    assert sessions["total"] == 2
    reasons = sorted(item["reason"] for item in sessions["sessions"])
    assert reasons == ["points", "streak"]
