"""Tests for background memorization scheduling (engine.schedule_memorize).

Memorization serialises on a single global lock and can take minutes per
session.  Cron runs used to await it inline during client teardown, which
kept the cron run log "running" (and APScheduler skipping subsequent
fires) long after the agent turn had finished.  These tests pin the
non-blocking behaviour and the frozen-``connected_at`` window semantics.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.engine import AgentEngine

_CONNECTED_AT = "2026-01-01T00:00:00+00:00"


def _make_engine() -> AgentEngine:
    """AgentEngine with mocked config/db — no initialize(), no IO."""
    config = MagicMock()
    config.sessions.sticky_period_minutes = 5
    config.agent.max_concurrent = 3
    config.mcp_servers = []
    db = AsyncMock()
    return AgentEngine(config, db)


def _engine_with_bridge() -> AgentEngine:
    engine = _make_engine()
    engine._memory_bridge = MagicMock(available=True)
    engine.db.get_session = AsyncMock(
        return_value={"connected_at": _CONNECTED_AT},
    )
    return engine


# ---------------------------------------------------------------------------
# schedule_memorize
# ---------------------------------------------------------------------------

class TestScheduleMemorize:
    @pytest.mark.asyncio
    async def test_passes_frozen_connected_at(self):
        """The connected_at captured at scheduling time is handed to the task."""
        engine = _engine_with_bridge()
        engine._memorize_session = AsyncMock()

        await engine.schedule_memorize("s1")
        # Live session mutates after scheduling (e.g. mark_error clears it)
        engine.db.get_session = AsyncMock(return_value={"connected_at": None})

        await asyncio.gather(*engine._memorize_bg_tasks)
        engine._memorize_session.assert_awaited_once_with(
            "s1", connected_at_override=_CONNECTED_AT,
        )

    @pytest.mark.asyncio
    async def test_noop_without_connected_at(self):
        """Sessions that never connected have nothing to memorize."""
        engine = _engine_with_bridge()
        engine.db.get_session = AsyncMock(return_value={"connected_at": None})

        await engine.schedule_memorize("s1")

        assert not engine._memorize_bg_tasks

    @pytest.mark.asyncio
    async def test_noop_without_bridge(self):
        engine = _make_engine()
        engine._memory_bridge = None

        await engine.schedule_memorize("s1")

        assert not engine._memorize_bg_tasks
        engine.db.get_session.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_task_pruned_after_completion(self):
        """Done-callbacks remove finished tasks from the registry."""
        engine = _engine_with_bridge()
        engine._memorize_session = AsyncMock()

        await engine.schedule_memorize("s1")
        await asyncio.gather(*engine._memorize_bg_tasks)
        await asyncio.sleep(0)  # let done-callbacks run

        assert not engine._memorize_bg_tasks

    @pytest.mark.asyncio
    async def test_failed_task_pruned_and_logged(self):
        """A failing memorization neither lingers nor raises into the caller."""
        engine = _engine_with_bridge()
        engine._memorize_session = AsyncMock(side_effect=RuntimeError("boom"))

        await engine.schedule_memorize("s1")
        await asyncio.gather(*engine._memorize_bg_tasks, return_exceptions=True)
        await asyncio.sleep(0)

        assert not engine._memorize_bg_tasks


# ---------------------------------------------------------------------------
# _memorize_session — connected_at override semantics
# ---------------------------------------------------------------------------

class TestMemorizeSessionOverride:
    @pytest.mark.asyncio
    async def test_override_covers_window_after_connected_at_cleared(self):
        """Frozen bound still indexes messages after the live column is gone."""
        engine = _make_engine()
        bridge = MagicMock(available=True)
        bridge.memorize_conversation = AsyncMock()
        engine._memory_bridge = bridge
        # Live column already cleared (e.g. mark_error / rotation ran first)
        engine.db.get_session = AsyncMock(return_value={
            "connected_at": None, "last_memorized_at": None,
        })
        engine.db.get_messages = AsyncMock(return_value=[
            {"created_at": "2026-01-01 00:05:00", "role": "user", "content": "hi"},
        ])

        await engine._memorize_session(
            "s1", connected_at_override=_CONNECTED_AT,
        )

        bridge.memorize_conversation.assert_awaited_once()
        sid, msgs = bridge.memorize_conversation.call_args.args
        assert sid == "s1"
        assert len(msgs) == 1
        engine.db.update_session_fields.assert_awaited_once_with(
            "s1", {"last_memorized_at": "2026-01-01 00:05:00"},
        )

    @pytest.mark.asyncio
    async def test_no_override_and_no_connected_at_skips(self):
        engine = _make_engine()
        bridge = MagicMock(available=True)
        bridge.memorize_conversation = AsyncMock()
        engine._memory_bridge = bridge
        engine.db.get_session = AsyncMock(return_value={"connected_at": None})

        await engine._memorize_session("s1")

        bridge.memorize_conversation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_watermark_read_after_lock(self):
        """A queued memorize sees the watermark advanced by the previous one.

        The session row is read inside the global lock — a second queued
        task for the same session must not re-index the window the first
        one covered (which would also regress the watermark).
        """
        engine = _make_engine()
        bridge = MagicMock(available=True)
        bridge.memorize_conversation = AsyncMock()
        engine._memory_bridge = bridge

        watermark = {"value": None}

        async def get_session(_sid):
            return {
                "connected_at": _CONNECTED_AT,
                "last_memorized_at": watermark["value"],
            }

        async def update_fields(_sid, fields):
            watermark["value"] = fields["last_memorized_at"]

        engine.db.get_session = AsyncMock(side_effect=get_session)
        engine.db.update_session_fields = AsyncMock(side_effect=update_fields)
        engine.db.get_messages = AsyncMock(return_value=[
            {"created_at": "2026-01-01 00:05:00", "role": "user", "content": "hi"},
            {"created_at": "2026-01-01 00:06:00", "role": "assistant", "content": "yo"},
        ])

        await asyncio.gather(
            engine._memorize_session("s1", connected_at_override=_CONNECTED_AT),
            engine._memorize_session("s1", connected_at_override=_CONNECTED_AT),
        )

        # First pass indexes both messages; second sees the watermark and
        # finds nothing new.
        bridge.memorize_conversation.assert_awaited_once()
        assert watermark["value"] == "2026-01-01 00:06:00"


# ---------------------------------------------------------------------------
# _discard_client — background vs inline memorization
# ---------------------------------------------------------------------------

class TestDiscardClientMemorize:
    @pytest.mark.asyncio
    async def test_background_does_not_wait_for_memorize(self):
        """Discard returns while the memorization is still queued/running."""
        engine = _engine_with_bridge()
        release = asyncio.Event()

        async def slow_memorize(session_id, connected_at_override=None):
            await release.wait()

        engine._memorize_session = slow_memorize

        await asyncio.wait_for(
            engine._discard_client("s1", background_memorize=True),
            timeout=1.0,
        )

        assert len(engine._memorize_bg_tasks) == 1
        assert not next(iter(engine._memorize_bg_tasks)).done()

        release.set()
        await asyncio.gather(*engine._memorize_bg_tasks)

    @pytest.mark.asyncio
    async def test_inline_awaits_memorize(self):
        """Default discard keeps the old synchronous behaviour."""
        engine = _engine_with_bridge()
        done: list[str] = []

        async def memorize(session_id, connected_at_override=None):
            done.append(session_id)

        engine._memorize_session = memorize

        await engine._discard_client("s1")

        assert done == ["s1"]
        assert not engine._memorize_bg_tasks


# ---------------------------------------------------------------------------
# Cron / hook teardown contract
# ---------------------------------------------------------------------------

class TestCronRunDiscard:
    @pytest.mark.asyncio
    async def test_run_cron_discards_with_background_memorize(self):
        engine = _make_engine()
        engine.sessions.create_cron_session = AsyncMock(
            return_value={"id": "cron:job:1"},
        )
        engine.run = AsyncMock(return_value="done")
        engine._discard_client = AsyncMock()

        result = await engine.run_cron("job", "prompt")

        assert result == "done"
        engine._discard_client.assert_awaited_once_with(
            "cron:job:1", background_memorize=True,
        )

    @pytest.mark.asyncio
    async def test_run_persistent_cron_discards_with_background_memorize(self):
        engine = _make_engine()
        engine.sessions.get_or_create = AsyncMock(return_value={"id": "cron:job"})
        engine.run = AsyncMock(return_value="done")
        engine._discard_client = AsyncMock()

        result = await engine.run_persistent_cron("job", "prompt")

        assert result == "done"
        engine._discard_client.assert_awaited_once_with(
            "cron:job", background_memorize=True,
        )

    @pytest.mark.asyncio
    async def test_run_cron_discards_even_on_error(self):
        engine = _make_engine()
        engine.sessions.create_cron_session = AsyncMock(
            return_value={"id": "cron:job:1"},
        )
        engine.run = AsyncMock(side_effect=RuntimeError("boom"))
        engine._discard_client = AsyncMock()

        with pytest.raises(RuntimeError):
            await engine.run_cron("job", "prompt")

        engine._discard_client.assert_awaited_once_with(
            "cron:job:1", background_memorize=True,
        )


# ---------------------------------------------------------------------------
# shutdown — pending background memorizations are flushed
# ---------------------------------------------------------------------------

class TestShutdownFlush:
    @pytest.mark.asyncio
    async def test_shutdown_cancels_pending_memorize_tasks(self):
        engine = _engine_with_bridge()

        async def hang(session_id, connected_at_override=None):
            await asyncio.Event().wait()

        engine._memorize_session = hang
        await engine.schedule_memorize("s1")
        assert engine._memorize_bg_tasks

        await engine.shutdown()

        assert not engine._memorize_bg_tasks


# ---------------------------------------------------------------------------
# Cron teardown — keep the client alive while a background task is still live
# ---------------------------------------------------------------------------

class TestOneShotRunKeepsClientForLiveBackgroundTasks:
    """The capability this enables: a one-shot (cron / hook) agent can start a
    ``run_in_background`` build/test, yield its turn, and be resumed when the
    task completes — instead of being torn down and stranded.

    The idle-stream watcher (``_idle_stream_watcher``) already delivers a
    background task's completion as a fresh autonomous turn, and
    ``run_idle_client_sweep`` already SKIPS discarding a client that has a live
    background task for exactly that reason — but only while the SDK client is
    alive. The one place that violates the contract is the cron teardown:
    ``run_cron`` / ``run_persistent_cron`` discard the client in their
    ``finally`` unconditionally, killing the subprocess + watcher at the first
    yield, so the task is orphaned and the run never resumes to finish its work.

    Intended behaviour: when the run yields with a live background task, the
    cron teardown must NOT discard — leave the client alive so the watcher can
    deliver the completion turn; the existing idle sweep reaps it once the task
    settles. (A bounded safety-net for a task that never settles belongs in the
    sweep, not here.) NOTE: run_persistent_cron is excluded — its session_id is
    stable/reused across runs, so it always discards (keep-alive would let the
    next run collide with the parked task); only the unique-per-run isolated
    paths (run_cron / run_hook) keep the client alive.

    Regression for the 2026-06-22 fix-worker strand (clickhouse-java#2721:
    background-and-yield in an isolated cron run left the task locked, never
    resumed).
    """

    @pytest.mark.asyncio
    async def test_run_cron_keeps_client_when_bg_task_live(self):
        engine = _make_engine()
        engine.sessions.create_cron_session = AsyncMock(
            return_value={"id": "cron:job:1"},
        )
        engine.run = AsyncMock(return_value="done")
        engine._discard_client = AsyncMock()
        # Agent yielded while a backgrounded build was still running.
        engine._bg_task_registry["cron:job:1"] = {
            "bg1": {"task_id": "bg1", "label": "mvn -pl client-v2 test",
                    "tool": "Bash", "status": "running"},
        }
        assert engine._has_live_background_tasks("cron:job:1") is True

        result = await engine.run_cron("job", "prompt")

        assert result == "done"
        # Kept alive: the idle-stream watcher delivers the completion turn and
        # the idle sweep reaps the client once the task settles.
        engine._discard_client.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_run_persistent_cron_discards_even_with_live_bg_task(self):
        """Persistent crons reuse a STABLE session_id across runs, so parking
        the client on a live bg task would let the next scheduled run collide
        with the still-in-flight task on the same client/conversation. They
        therefore always discard — keep-alive is only safe for the unique-per-
        run isolated paths (run_cron / run_hook)."""
        engine = _make_engine()
        engine.sessions.get_or_create = AsyncMock(return_value={"id": "cron:job"})
        engine.run = AsyncMock(return_value="done")
        engine._discard_client = AsyncMock()
        engine._bg_task_registry["cron:job"] = {
            "bg1": {"task_id": "bg1", "label": "build", "tool": "Bash",
                    "status": "running"},
        }
        assert engine._has_live_background_tasks("cron:job") is True

        result = await engine.run_persistent_cron("job", "prompt")

        assert result == "done"
        # Discarded despite the live bg task (stable session reuse hazard).
        engine._discard_client.assert_awaited_once_with(
            "cron:job", background_memorize=True,
        )

    @pytest.mark.asyncio
    async def test_run_hook_keeps_client_when_bg_task_live(self):
        engine = _make_engine()
        engine.sessions.create_hook_session = AsyncMock(
            return_value={"id": "hook:deploy:1"},
        )
        engine.run = AsyncMock(return_value="done")
        engine._discard_client = AsyncMock()
        engine._bg_task_registry["hook:deploy:1"] = {
            "bg1": {"task_id": "bg1", "label": "deploy", "tool": "Bash",
                    "status": "running"},
        }
        assert engine._has_live_background_tasks("hook:deploy:1") is True

        result = await engine.run_hook("deploy", "1", "prompt")

        assert result == "done"
        engine._discard_client.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_run_hook_discards_when_no_bg_task(self):
        engine = _make_engine()
        engine.sessions.create_hook_session = AsyncMock(
            return_value={"id": "hook:deploy:2"},
        )
        engine.run = AsyncMock(return_value="done")
        engine._discard_client = AsyncMock()

        result = await engine.run_hook("deploy", "2", "prompt")

        assert result == "done"
        engine._discard_client.assert_awaited_once_with(
            "hook:deploy:2", background_memorize=True,
        )
