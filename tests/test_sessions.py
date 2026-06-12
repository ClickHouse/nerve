"""Tests for nerve.agent.sessions — SessionManager lifecycle, forking, cleanup."""

import asyncio
import contextlib

import pytest
import pytest_asyncio

from nerve.agent.sessions import SessionManager, SessionStatus
from nerve.db import Database


@pytest_asyncio.fixture
async def sm(db: Database):
    """Create a SessionManager backed by the test database."""
    return SessionManager(db)


@pytest.mark.asyncio
class TestSessionCreation:
    """Test session creation."""

    async def test_get_or_create_new(self, sm: SessionManager):
        session = await sm.get_or_create("new-1", title="New Session")
        assert session["id"] == "new-1"
        assert session["title"] == "New Session"

    async def test_get_or_create_existing(self, sm: SessionManager, db: Database):
        await sm.get_or_create("exist-1", title="First")
        session = await sm.get_or_create("exist-1", title="Second")
        # Should return existing, not overwrite
        real = await db.get_session("exist-1")
        assert real["title"] == "First"

    async def test_create_logs_event(self, sm: SessionManager, db: Database):
        await sm.get_or_create("evlog-1", source="web")
        events = await db.get_session_events("evlog-1")
        assert len(events) == 1
        assert events[0]["event_type"] == "created"


@pytest.mark.asyncio
class TestLifecycleTransitions:
    """Test session status transitions."""

    async def test_mark_active(self, sm: SessionManager, db: Database):
        await sm.get_or_create("trans-1")
        await sm.mark_active("trans-1", sdk_session_id="sdk-1", connected_at="2024-01-01T00:00:00")
        session = await db.get_session("trans-1")
        assert session["status"] == "active"
        assert session["sdk_session_id"] == "sdk-1"
        assert session["connected_at"] == "2024-01-01T00:00:00"

    async def test_mark_idle_preserves_sdk_id(self, sm: SessionManager, db: Database):
        await sm.get_or_create("trans-2")
        await sm.mark_active("trans-2", sdk_session_id="sdk-2")
        await sm.mark_idle("trans-2", preserve_sdk_id=True)
        session = await db.get_session("trans-2")
        assert session["status"] == "idle"
        assert session["sdk_session_id"] == "sdk-2"

    async def test_mark_idle_clears_sdk_id(self, sm: SessionManager, db: Database):
        await sm.get_or_create("trans-3")
        await sm.mark_active("trans-3", sdk_session_id="sdk-3")
        await sm.mark_idle("trans-3", preserve_sdk_id=False)
        session = await db.get_session("trans-3")
        assert session["status"] == "idle"
        assert session["sdk_session_id"] is None
        assert session["connected_at"] is None

    async def test_mark_stopped(self, sm: SessionManager, db: Database):
        await sm.get_or_create("trans-4")
        await sm.mark_stopped("trans-4")
        session = await db.get_session("trans-4")
        assert session["status"] == "stopped"

    async def test_mark_error(self, sm: SessionManager, db: Database):
        await sm.get_or_create("trans-5")
        await sm.mark_error("trans-5", "something broke")
        session = await db.get_session("trans-5")
        assert session["status"] == "error"
        assert session["sdk_session_id"] is None

    async def test_transitions_log_events(self, sm: SessionManager, db: Database):
        await sm.get_or_create("trans-ev")
        await sm.mark_active("trans-ev", sdk_session_id="x")
        await sm.mark_idle("trans-ev")
        events = await db.get_session_events("trans-ev")
        types = [e["event_type"] for e in events]
        assert "started" in types
        assert "idle" in types


@pytest.mark.asyncio
class TestChannelMapping:
    """Test DB-persisted channel-to-session mapping with auto-session creation."""

    async def test_auto_session_created_on_first_message(self, sm: SessionManager, db: Database):
        """When a channel has no mapping, a new session is created automatically."""
        sid = await sm.get_active_session("telegram:999", source="telegram")
        assert len(sid) == 8  # Short UUID
        session = await db.get_session(sid)
        assert session is not None
        assert session["source"] == "telegram"

    async def test_auto_session_reused_within_sticky_period(self, sm: SessionManager, db: Database):
        """Same session returned if last activity is within sticky period."""
        sid1 = await sm.get_active_session("telegram:111", source="telegram")
        # Simulate recent activity
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        await db.update_session_fields(sid1, {"last_activity_at": now})
        sid2 = await sm.get_active_session("telegram:111", source="telegram")
        assert sid1 == sid2

    async def test_auto_session_rotated_after_sticky_period(self, sm: SessionManager, db: Database):
        """New session created if last activity exceeds sticky period."""
        sid1 = await sm.get_active_session("telegram:222", source="telegram")
        # Simulate old activity (3 hours ago, beyond 2h default)
        await db.update_session_fields(sid1, {
            "last_activity_at": "2020-01-01T00:00:00+00:00",
        })
        await db.db.execute(
            "UPDATE sessions SET updated_at = '2020-01-01T00:00:00' WHERE id = ?",
            (sid1,),
        )
        await db.db.commit()
        sid2 = await sm.get_active_session("telegram:222", source="telegram")
        assert sid1 != sid2

    async def test_auto_session_reused_when_active_despite_old_timestamp(
        self, sm: SessionManager, db: Database,
    ):
        """An active session keeps the channel even if last_activity_at is stale.

        A hung turn never reaches mark_active() at engine.run's end, so
        last_activity_at freezes at turn-start. Without the active-status
        carve-out in _is_within_sticky_period, a hang lasting longer than
        sticky_period_minutes would orphan the session and route the
        user's follow-up message into a fresh, empty one.
        """
        sid1 = await sm.get_active_session("telegram:333", source="telegram")
        # Mark active and back-date timestamps to look like a hung turn that
        # started long before the sticky-period cutoff.
        await sm.mark_active(sid1, sdk_session_id="sdk-stuck")
        await db.update_session_fields(sid1, {
            "last_activity_at": "2020-01-01T00:00:00+00:00",
        })
        await db.db.execute(
            "UPDATE sessions SET updated_at = '2020-01-01T00:00:00' WHERE id = ?",
            (sid1,),
        )
        await db.db.commit()
        sid2 = await sm.get_active_session("telegram:333", source="telegram")
        assert sid1 == sid2

    async def test_auto_session_rotated_when_idle_after_sticky_period(
        self, sm: SessionManager, db: Database,
    ):
        """Idle sessions still roll over after the sticky period.

        Once a hung session has been recovered (status flipped to idle by
        the engine's exception path), the time-based cutoff applies again
        and a new follow-up message mints a fresh session.
        """
        sid1 = await sm.get_active_session("telegram:444", source="telegram")
        await sm.mark_active(sid1, sdk_session_id="sdk-x")
        await sm.mark_idle(sid1)
        await db.update_session_fields(sid1, {
            "last_activity_at": "2020-01-01T00:00:00+00:00",
        })
        await db.db.execute(
            "UPDATE sessions SET updated_at = '2020-01-01T00:00:00' WHERE id = ?",
            (sid1,),
        )
        await db.db.commit()
        sid2 = await sm.get_active_session("telegram:444", source="telegram")
        assert sid1 != sid2

    async def test_set_and_get_active_session(self, sm: SessionManager, db: Database):
        await sm.get_or_create("ch-1")
        await sm.set_active_session("telegram:123", "ch-1")
        # Simulate recent activity so sticky period passes
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        await db.update_session_fields("ch-1", {"last_activity_at": now})
        sid = await sm.get_active_session("telegram:123")
        assert sid == "ch-1"

    async def test_set_active_session_not_found(self, sm: SessionManager):
        with pytest.raises(ValueError, match="not found"):
            await sm.set_active_session("telegram:123", "nonexistent")


@pytest.mark.asyncio
class TestRunningState:
    """Test running session tracking."""

    async def test_mark_running(self, sm: SessionManager):
        assert not sm.is_running("run-1")
        sm.mark_running("run-1")
        assert sm.is_running("run-1")
        sm.mark_not_running("run-1")
        assert not sm.is_running("run-1")

    async def test_register_task_does_not_mark_running(self, sm: SessionManager):
        """register_task should NOT add to _running_sessions (that's mark_running's job)."""
        async def noop():
            pass
        task = asyncio.create_task(noop())
        sm.register_task("task-1", task)
        # register_task should NOT mark as running
        assert not sm.is_running("task-1")
        await task

    async def test_register_task_cleans_up_on_done(self, sm: SessionManager):
        async def noop():
            pass
        task = asyncio.create_task(noop())
        sm.register_task("task-cleanup", task)
        assert sm._running_tasks.get("task-cleanup") is task
        await task
        await asyncio.sleep(0.01)
        assert "task-cleanup" not in sm._running_tasks

    async def test_register_task_replacement_does_not_clobber_new_entry(
        self, sm: SessionManager,
    ):
        """An old task finishing must not pop the *new* task's registry entry.

        Regression: the old code used a closure-only ``pop(session_id, None)``
        which would clobber whatever was registered at the time, including a
        newer task scheduled by a concurrent register_task call.  The fix
        identity-checks the task in the done-callback.
        """
        async def quick():
            await asyncio.sleep(0.01)

        async def slow():
            await asyncio.sleep(1.0)

        old = asyncio.create_task(quick())
        sm.register_task("dup-1", old)
        # Replace before old finishes.
        new = asyncio.create_task(slow())
        sm.register_task("dup-1", new)
        # Wait for the old task to finish + its done-callback to fire.
        await old
        await asyncio.sleep(0.05)
        # New task must still be registered — its entry survived old's
        # done-callback.
        assert sm._running_tasks.get("dup-1") is new
        new.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await new

    async def test_stop_session_no_client(self, sm: SessionManager):
        """Stop when there's no client or task should return False."""
        result = await sm.stop_session("nonexistent")
        assert result is False

    async def test_stop_session_cancels_task(self, sm: SessionManager):
        async def long_running():
            await asyncio.sleep(100)

        task = asyncio.create_task(long_running())
        sm.register_task("stop-1", task)
        sm.mark_running("stop-1")  # Simulate what engine.run() does
        result = await sm.stop_session("stop-1")
        assert result is True
        # Let the cancellation propagate
        await asyncio.sleep(0.01)
        assert task.cancelled() or task.done()


@pytest.mark.asyncio
class TestFork:
    """Test session forking."""

    async def test_fork_session(self, sm: SessionManager, db: Database):
        await sm.get_or_create("source-1", title="Source")
        fork = await sm.fork_session("source-1", title="My Fork")
        assert fork["id"].startswith("fork-")
        assert fork["parent_session_id"] == "source-1"
        assert fork["title"] == "My Fork"

    async def test_fork_with_message_id(self, sm: SessionManager, db: Database):
        await sm.get_or_create("source-2")
        fork = await sm.fork_session("source-2", at_message_id="msg-42")
        session = await db.get_session(fork["id"])
        assert session["forked_from_message"] == "msg-42"

    async def test_fork_nonexistent_raises(self, sm: SessionManager):
        with pytest.raises(ValueError, match="not found"):
            await sm.fork_session("nonexistent")

    async def test_fork_auto_title(self, sm: SessionManager):
        await sm.get_or_create("source-3", title="Original")
        fork = await sm.fork_session("source-3")
        assert "Fork of Original" in fork["title"]


@pytest.mark.asyncio
class TestResumeInfo:
    """Test resume info retrieval."""

    async def test_get_resume_info(self, sm: SessionManager, db: Database):
        await sm.get_or_create("resume-1")
        await sm.mark_active("resume-1", sdk_session_id="sdk-resume")
        info = await sm.get_resume_info("resume-1")
        assert info["sdk_session_id"] == "sdk-resume"
        assert info["status"] == "active"

    async def test_get_resume_info_not_found(self, sm: SessionManager):
        info = await sm.get_resume_info("nonexistent")
        assert info is None


@pytest.mark.asyncio
class TestCronHookSessions:
    """Test cron and hook session creation."""

    async def test_cron_session_with_run_id(self, sm: SessionManager):
        session = await sm.create_cron_session("daily-check", run_id="20240101-120000")
        assert session["id"] == "cron:daily-check:20240101-120000"

    async def test_cron_session_without_run_id(self, sm: SessionManager):
        session = await sm.create_cron_session("daily-check")
        assert session["id"] == "cron:daily-check"

    async def test_hook_session(self, sm: SessionManager):
        session = await sm.create_hook_session("github", "pr-123")
        assert session["id"] == "hook:github:pr-123"


@pytest.mark.asyncio
class TestMessages:
    """Test message delegation."""

    async def test_add_and_get_messages(self, sm: SessionManager):
        await sm.get_or_create("msg-test")
        await sm.add_message("msg-test", "user", "hello")
        await sm.add_message("msg-test", "assistant", "hi there")
        history = await sm.get_conversation_history("msg-test")
        assert len(history) == 2
        assert history[0]["role"] == "user"
        assert history[1]["role"] == "assistant"


@pytest.mark.asyncio
class TestArchiveAndCleanup:
    """Test session archival and cleanup."""

    async def test_archive_session(self, sm: SessionManager, db: Database):
        await sm.get_or_create("arch-1")
        await sm.archive_session("arch-1")
        session = await db.get_session("arch-1")
        assert session["status"] == "archived"
        assert session["archived_at"] is not None

    async def test_archive_disconnects_client(self, sm: SessionManager):
        await sm.get_or_create("arch-2")
        # Simulate a client
        sm.set_client("arch-2", MockClient())
        await sm.archive_session("arch-2")
        assert sm.get_client("arch-2") is None

    async def test_cleanup_archives_stale(self, sm: SessionManager, db: Database):
        await sm.get_or_create("cleanup-1")
        await db.update_session_fields("cleanup-1", {"status": "idle"})
        # Force old timestamp
        await db.db.execute(
            "UPDATE sessions SET updated_at = '2020-01-01T00:00:00' WHERE id = 'cleanup-1'"
        )
        await db.db.commit()

        stats = await sm.run_cleanup(archive_after_days=30, max_sessions=1000)
        assert stats["archived_stale"] >= 1

        session = await db.get_session("cleanup-1")
        assert session["status"] == "archived"

    async def test_cleanup_archives_all_stale_sessions(self, sm: SessionManager, db: Database):
        """No session gets special treatment — all stale sessions are archived."""
        await sm.get_or_create("cleanup-any")
        await db.update_session_fields("cleanup-any", {"status": "idle"})
        await db.db.execute(
            "UPDATE sessions SET updated_at = '2020-01-01T00:00:00' WHERE id = 'cleanup-any'"
        )
        await db.db.commit()

        await sm.run_cleanup(archive_after_days=30)
        session = await db.get_session("cleanup-any")
        assert session["status"] == "archived"


@pytest.mark.asyncio
class TestOrphanRecovery:
    """Test orphan session recovery on startup."""

    async def test_recover_active_with_sdk_id(self, sm: SessionManager, db: Database):
        await db.create_session("orphan-1", status="active")
        await db.update_session_fields("orphan-1", {
            "status": "active", "sdk_session_id": "sdk-orphan",
        })
        count = await sm.recover_orphaned_sessions()
        assert count >= 1
        session = await db.get_session("orphan-1")
        assert session["status"] == "idle"

    async def test_recover_active_without_sdk_id(self, sm: SessionManager, db: Database):
        await db.create_session("orphan-2", status="active")
        await db.update_session_fields("orphan-2", {"status": "active"})
        count = await sm.recover_orphaned_sessions()
        assert count >= 1
        session = await db.get_session("orphan-2")
        assert session["status"] == "stopped"

    async def test_recover_skips_live_clients(self, sm: SessionManager, db: Database):
        await db.create_session("orphan-3", status="active")
        await db.update_session_fields("orphan-3", {"status": "active"})
        # Simulate a live client
        sm.set_client("orphan-3", MockClient())
        count = await sm.recover_orphaned_sessions()
        session = await db.get_session("orphan-3")
        # Should still be active since client is "live"
        assert session["status"] == "active"
        # Clean up
        sm.remove_client("orphan-3")


@pytest.mark.asyncio
class TestListing:
    """Test session listing."""

    async def test_list_sessions(self, sm: SessionManager, db: Database):
        await sm.get_or_create("list-1", title="Alpha")
        await sm.get_or_create("list-2", title="Beta")
        sessions = await sm.list_sessions()
        ids = [s["id"] for s in sessions]
        assert "list-1" in ids
        assert "list-2" in ids

    async def test_list_excludes_archived(self, sm: SessionManager, db: Database):
        await sm.get_or_create("list-arch")
        await sm.archive_session("list-arch")
        sessions = await sm.list_sessions(include_archived=False)
        ids = [s["id"] for s in sessions]
        assert "list-arch" not in ids

    async def test_list_includes_archived(self, sm: SessionManager, db: Database):
        await sm.get_or_create("list-arch2")
        await sm.archive_session("list-arch2")
        sessions = await sm.list_sessions(include_archived=True)
        ids = [s["id"] for s in sessions]
        assert "list-arch2" in ids


@pytest.mark.asyncio
class TestMemorizeCallback:
    """Test that memorize callback is invoked during archive and orphan recovery."""

    async def test_archive_calls_memorize(self, sm: SessionManager, db: Database):
        memorized = []

        async def mock_memorize(sid: str):
            memorized.append(sid)

        sm._on_memorize = mock_memorize
        await sm.get_or_create("memo-arch")
        await sm.archive_session("memo-arch")
        assert "memo-arch" in memorized

    async def test_orphan_recovery_memorizes_non_resumable(self, sm: SessionManager, db: Database):
        memorized = []

        async def mock_memorize(sid: str):
            memorized.append(sid)

        sm._on_memorize = mock_memorize
        # Create an active session without sdk_session_id (non-resumable)
        await db.create_session("memo-orphan", status="active")
        await db.update_session_fields("memo-orphan", {"status": "active"})
        await sm.recover_orphaned_sessions()
        assert "memo-orphan" in memorized

    async def test_orphan_recovery_skips_memorize_for_resumable(self, sm: SessionManager, db: Database):
        memorized = []

        async def mock_memorize(sid: str):
            memorized.append(sid)

        sm._on_memorize = mock_memorize
        # Create an active session WITH sdk_session_id (resumable)
        await db.create_session("memo-resumable", status="active")
        await db.update_session_fields("memo-resumable", {
            "status": "active", "sdk_session_id": "sdk-123",
        })
        await sm.recover_orphaned_sessions()
        # Resumable sessions should NOT be memorized (they can be resumed)
        assert "memo-resumable" not in memorized

    async def test_no_callback_doesnt_crash(self, sm: SessionManager, db: Database):
        """Archive should work even without a memorize callback."""
        sm._on_memorize = None
        await sm.get_or_create("memo-none")
        await sm.archive_session("memo-none")
        session = await db.get_session("memo-none")
        assert session["status"] == "archived"


@pytest.mark.asyncio
class TestRegisterTaskRaceCondition:
    """Regression test: register_task must not cause is_running race."""

    async def test_no_race_between_register_and_run(self, sm: SessionManager):
        """Simulates the server.py pattern:
        1. task = create_task(engine.run(...))
        2. engine.register_task(session_id, task)
        3. engine.run() executes and checks is_running()

        register_task must NOT mark the session as running, otherwise
        run() will see it as "already running" and raise RuntimeError.
        """
        call_log = []

        async def fake_run():
            # This simulates what engine.run() does first
            if sm.is_running("race-test"):
                call_log.append("RACE_BUG")
                raise RuntimeError("Session is already running")
            sm.mark_running("race-test")
            call_log.append("run_started")
            await asyncio.sleep(0.01)
            sm.mark_not_running("race-test")
            call_log.append("run_finished")

        task = asyncio.create_task(fake_run())
        sm.register_task("race-test", task)
        await task
        assert "RACE_BUG" not in call_log
        assert "run_started" in call_log
        assert "run_finished" in call_log


class MockClient:
    """Mock SDK client for testing."""

    def __init__(self):
        self.disconnected = False

    async def disconnect(self):
        self.disconnected = True

    async def interrupt(self):
        pass
