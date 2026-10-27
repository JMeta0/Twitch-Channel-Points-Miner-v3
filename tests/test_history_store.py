import os

import pytest

from TwitchChannelPointsMiner.history.store import (
    DEFAULT_RETENTION_DAYS,
    HistoryStore,
    utc_day,
)

NOW = 1_700_000_000.0  # 2023-11-14T22:13:20Z


@pytest.fixture
def store(tmp_path):
    instance = HistoryStore(tmp_path / "history.sqlite3")
    instance.initialize()
    return instance


def test_points_and_daily_rollups_are_idempotent(store):
    assert store.record_points("Alice", NOW, 150, 50, "WATCH")
    assert not store.record_points("Alice", NOW, 150, 50, "WATCH")
    assert store.record_points("Alice", NOW + 60, 140, -10, "Spent")

    daily = store.daily(days=1, now=NOW + 60)
    assert len(daily) == 1
    assert daily[0]["points_gained"] == 50
    assert daily[0]["points_spent"] == 10

    overview = store.overview(days=30, now=NOW + 60)
    assert overview["window"]["points_gained"] == 50
    assert overview["window"]["points_spent"] == 10
    assert overview["window"]["streamers"] == 1


def test_watch_sessions_accumulate_and_end(store):
    session_id = store.start_watch_session("Alice", "streak", NOW)
    store.tick_watch_session(session_id, "Alice", NOW + 30, 30)
    store.tick_watch_session(session_id, "Alice", NOW + 60, 30)
    store.end_watch_session(session_id, NOW + 60)

    sessions = store.sessions(kind="watch", days=30, now=NOW + 120)
    assert sessions["total"] == 1
    item = sessions["sessions"][0]
    assert item["reason"] == "streak"
    assert item["duration_seconds"] == 60
    assert item["active"] is False

    overview = store.overview(days=30, now=NOW + 120)
    assert overview["window"]["watch_seconds"] == 60
    assert overview["active_watch_sessions"] == 0


def test_live_sessions_are_merged_by_broadcast(store):
    first = store.upsert_stream_session("Alice", "b1", NOW)
    assert first is not None
    store.end_watch_session(store.start_watch_session("Alice", "points", NOW), NOW + 10)
    store.upsert_stream_session("Alice", "b1", NOW, ended_at=NOW + 120)

    sessions = store.sessions(kind="stream", days=1, now=NOW + 200)
    assert sessions["total"] == 1
    assert sessions["sessions"][0]["duration_seconds"] == 120

    second = store.upsert_stream_session("Alice", "b2", NOW + 200)
    assert second is not None
    sessions = store.sessions(kind="stream", days=1, now=NOW + 260)
    assert sessions["total"] == 2
    for item in sessions["sessions"]:
        if item["broadcast_id"] == "b1":
            assert item["ended_at"] == NOW + 120
        else:
            assert item["active"] is True


def test_stream_end_times_never_move_backwards(store):
    store.upsert_stream_session("Alice", "b1", NOW, ended_at=NOW + 100)
    store.upsert_stream_session("Alice", "b1", NOW, ended_at=NOW)
    sessions = store.sessions(kind="stream", days=1, now=NOW + 200)
    assert sessions["sessions"][0]["ended_at"] == NOW + 100

    store.upsert_stream_session("Alice", "b1", NOW, ended_at=NOW + 200)
    sessions = store.sessions(kind="stream", days=1, now=NOW + 300)
    assert sessions["sessions"][0]["ended_at"] == NOW + 200


def test_streak_events_deduplicate_nearby_claims(store):
    assert store.record_event("alice", NOW, "watch_streak", "+50 - WATCH_STREAK", 50)
    assert not store.record_event(
        "alice", NOW + 60, "watch_streak", "Watch streak claimed", 50
    )
    assert store.record_event(
        "alice", NOW + 600, "watch_streak", "+50 - WATCH_STREAK", 50
    )

    events = store.events(days=30, now=NOW + 700)
    assert events["total"] == 2
    assert store.daily(days=1, now=NOW + 700)[0]["streak_count"] == 2


def test_streamer_detail_and_activity(store):
    streamer = "alice"
    store.record_points(streamer, NOW, 100, 40, "WATCH")
    store.start_watch_session(streamer, "points", NOW)
    store.upsert_stream_session(streamer, "b1", NOW, ended_at=NOW + 3600)

    detail = store.streamer_detail(streamer, days=30, now=NOW + 3600)
    assert detail["streamer"] == streamer
    assert detail["summary"]["points_gained"] == 40
    assert detail["watch_sessions"]
    assert detail["streams"]

    activity = store.activity(days=1, now=NOW + 3600)
    assert len(activity["day_list"]) == 1
    assert activity["streamers"][0]["streamer"] == streamer
    cell = activity["streamers"][0]["days"][utc_day(NOW)]
    assert cell["points_gained"] == 40
    assert cell["live_seconds"] == 3600


def test_resume_stream_session_reopens_only_the_newest_broadcast(store):
    store.start_watch_session("alice", "points", NOW)
    store.upsert_stream_session("alice", "b1", NOW, ended_at=NOW + 60)

    assert store.resume_stream_session("alice", "b1", timestamp=NOW + 70) is True
    item = store.sessions(kind="stream", days=1, now=NOW + 90)["sessions"][0]
    assert item["active"] is True
    assert item["started_at"] == NOW
    assert item["last_seen_at"] == NOW + 70

    # A newer broadcast must never be replaced by an older one.
    store.upsert_stream_session("alice", "b2", NOW + 100)
    assert store.resume_stream_session("alice", "b1", timestamp=NOW + 300) is False
    active = {
        row["broadcast_id"]: row["active"]
        for row in store.sessions(kind="stream", days=1, now=NOW + 400)["sessions"]
    }
    assert active == {"b1": False, "b2": True}


def test_prune_untracked_streams_removes_live_only_rows(store):
    store.start_watch_session("alice", "points", NOW)
    store.upsert_stream_session("alice", "a1", NOW, ended_at=NOW + 10)
    store.upsert_stream_session("bob", "b1", NOW, ended_at=NOW + 10)

    store.prune_untracked_streams()

    streams = store.sessions(kind="stream", days=1, now=NOW + 20)
    assert streams["total"] == 1
    assert streams["sessions"][0]["streamer"] == "alice"

    streamers = store.streamers(days=1, now=NOW + 20)
    assert [item["streamer"] for item in streamers] == ["alice"]
    assert streamers[0]["stream_sessions"] == 1


def test_daily_stats_and_retention(store):
    store.set_import_state("source", mtime=1.0, size=2, last_x=3.0, last_balance=4)
    state = store.get_import_state("source")
    assert state["last_x"] == 3.0
    assert state["last_balance"] == 4

    old = NOW - ((DEFAULT_RETENTION_DAYS + 10) * 24 * 60 * 60)
    store.record_points("alice", old, 10, 10, "WATCH")
    store.record_points("alice", NOW, 20, 10, "WATCH")
    counts = store.apply_retention(now=NOW, retention_days=DEFAULT_RETENTION_DAYS)
    assert counts["points"] == 1

    daily = store.daily(days=3650, now=NOW)
    assert sum(row["points_gained"] for row in daily) == 20


def test_delete_streamer_removes_all_tables(store):
    store.record_points("alice", NOW, 10, 10, "WATCH")
    store.record_points("bob", NOW, 10, 10, "WATCH")
    store.start_watch_session("alice", "points", NOW)

    counts = store.delete_streamer("alice")
    assert counts["points"] == 1
    assert store.overview(days=30, now=NOW)["window"]["streamers"] == 1
    assert os.path.isfile(store.path)
