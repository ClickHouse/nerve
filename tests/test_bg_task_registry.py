"""Regression tests for the background-task registry lifecycle
(``AgentEngine._bg_task_registry``) — specifically the reconciliation that
prevents a session from being stuck showing a permanent "parked" dot.

The registry is event-driven: an entry goes "running" on ``task_started`` and
only becomes terminal on a ``task_updated``/``task_notification`` completion
event. If that terminal event never arrives (a detached ``&`` child the CLI
stops observing, a dropped event, a client torn down before completion) the
entry would otherwise stay "running" forever — the idle sweep keeps skipping
the session, so its client is never reaped and the sidebar dot never clears.

Two complementary defences are tested here:
  1. reconcile on client teardown (``_reconcile_bg_tasks_on_teardown``), and
  2. a staleness window in ``_has_live_background_tasks`` that lets the idle
     sweep proceed for an entry that has gone silent (the trap-breaker).
"""

import time
from unittest.mock import AsyncMock, patch

import pytest

from nerve.agent.backends import events as ev
from nerve.agent.engine import AgentEngine
from nerve.config import NerveConfig


async def _make_engine(db, tmp_path, session_id, *, stale_minutes=None):
    """A bare AgentEngine with one live session and no SDK client."""
    workspace = tmp_path / f"ws-{session_id}"
    workspace.mkdir(parents=True, exist_ok=True)
    conf: dict = {"workspace": str(workspace), "agent": {"backend": "claude"}}
    if stale_minutes is not None:
        conf["sessions"] = {"bg_task_stale_minutes": stale_minutes}
    cfg = NerveConfig.from_dict(conf)
    engine = AgentEngine(cfg, db)
    await engine.sessions.get_or_create(
        session_id, title="t", source="web", backend="claude",
    )
    return engine


async def _feed(engine, session_id, subtype, **data):
    """Drive one CLI background-task lifecycle event into the engine."""
    await engine._handle_system_event(
        session_id, ev.SystemEvent(subtype=subtype, data=data),
    )


# --------------------------------------------------------------------------- #
#  Fix #1 — reconcile on teardown                                             #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_teardown_reconcile_clears_running_entry(db, tmp_path):
    """A still-"running" entry is terminalized, pruned, and broadcast when
    the client is reconciled on teardown."""
    sid = "reconcile"
    engine = await _make_engine(db, tmp_path, sid)
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        await _feed(engine, sid, "task_started", task_id="t1", description="build")
        assert engine._has_live_background_tasks(sid) is True

        await engine._reconcile_bg_tasks_on_teardown(sid)

        # No live task, entry pruned, and the UI was told (both the task
        # panel update and the global session_running that drives the dot).
        assert engine._has_live_background_tasks(sid) is False
        assert engine._bg_task_registry.get(sid) in (None, {})
        types = [c.args[1]["type"] for c in bc.broadcast.await_args_list]
        assert "background_tasks_update" in types
        assert "session_running" in types
        running_evt = next(
            c.args[1] for c in bc.broadcast.await_args_list
            if c.args[1]["type"] == "session_running"
        )
        assert running_evt["has_background_tasks"] is False


@pytest.mark.asyncio
async def test_teardown_reconcile_noop_without_running(db, tmp_path):
    """Reconcile is a no-op (no broadcast) when nothing is running — a
    normally-completed task must not trigger a spurious update."""
    sid = "noop"
    engine = await _make_engine(db, tmp_path, sid)
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        await _feed(engine, sid, "task_started", task_id="t1", description="x")
        await _feed(engine, sid, "task_notification", task_id="t1", status="completed")
        bc.broadcast.reset_mock()

        await engine._reconcile_bg_tasks_on_teardown(sid)
        bc.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_discard_client_invokes_reconcile(db, tmp_path):
    """End-to-end: _discard_client (the shared teardown entrypoint) clears a
    stuck entry even with no terminal event."""
    sid = "discard"
    engine = await _make_engine(db, tmp_path, sid)
    engine._memorize_session = AsyncMock()  # skip the memU round-trip
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        await _feed(engine, sid, "task_started", task_id="t1", description="build")
        assert engine._has_live_background_tasks(sid) is True

        await engine._discard_client(sid)

        assert engine._has_live_background_tasks(sid) is False


# --------------------------------------------------------------------------- #
#  Fix #2 — staleness trap-breaker                                            #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_stale_running_entry_not_live(db, tmp_path):
    """An entry silent beyond the window reads as not-live — without any
    teardown and without mutating the registry (pure predicate)."""
    sid = "stale"
    engine = await _make_engine(db, tmp_path, sid, stale_minutes=360)
    await _feed(engine, sid, "task_started", task_id="t1", description="build")
    # Backdate its last event well past the 6h window.
    engine._bg_task_registry[sid]["t1"]["last_event_at"] = (
        time.monotonic() - 7 * 3600
    )

    assert engine._has_live_background_tasks(sid) is False
    # Pure: the entry is untouched (still "running") — the teardown reconcile
    # remains responsible for terminalizing + broadcasting.
    assert engine._bg_task_registry[sid]["t1"]["status"] == "running"


@pytest.mark.asyncio
async def test_fresh_running_entry_is_live(db, tmp_path):
    """A freshly-started task is live and protected from the idle sweep."""
    sid = "fresh"
    engine = await _make_engine(db, tmp_path, sid, stale_minutes=360)
    await _feed(engine, sid, "task_started", task_id="t1", description="build")
    assert engine._has_live_background_tasks(sid) is True


@pytest.mark.asyncio
async def test_progress_refreshes_liveness(db, tmp_path):
    """A no-op-for-UI task_progress (label already set) still refreshes the
    liveness stamp, so a long, actively-progressing workflow never goes
    stale."""
    sid = "progress"
    engine = await _make_engine(db, tmp_path, sid, stale_minutes=360)
    await _feed(engine, sid, "task_started", task_id="t1", description="wf")
    # Simulate a long silence, then a fresh progress ping.
    engine._bg_task_registry[sid]["t1"]["last_event_at"] = (
        time.monotonic() - 7 * 3600
    )
    assert engine._has_live_background_tasks(sid) is False  # would be reaped
    await _feed(engine, sid, "task_progress", task_id="t1", description="wf")
    assert engine._has_live_background_tasks(sid) is True   # revived by event


@pytest.mark.asyncio
async def test_stale_window_disabled_is_legacy(db, tmp_path):
    """bg_task_stale_minutes=0 disables the window (any running = live)."""
    sid = "legacy"
    engine = await _make_engine(db, tmp_path, sid, stale_minutes=0)
    await _feed(engine, sid, "task_started", task_id="t1", description="build")
    engine._bg_task_registry[sid]["t1"]["last_event_at"] = (
        time.monotonic() - 100 * 3600
    )
    assert engine._has_live_background_tasks(sid) is True


# --------------------------------------------------------------------------- #
#  Idle-sweep integration — the trap and its break                           #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_idle_sweep_skips_live_task(db, tmp_path):
    """Regression guard: the sweep must NOT discard a session with a genuine
    live background task (the keepalive the parked-skip exists for)."""
    sid = "keepalive"
    engine = await _make_engine(db, tmp_path, sid, stale_minutes=360)
    await _feed(engine, sid, "task_started", task_id="t1", description="build")
    engine.sessions.get_idle_client_ids = lambda _secs: [sid]
    engine._discard_client = AsyncMock()

    await engine.run_idle_client_sweep()
    engine._discard_client.assert_not_awaited()


@pytest.mark.asyncio
async def test_idle_sweep_reaps_stale_task(db, tmp_path):
    """The trap is broken: once the entry is stale the sweep proceeds to
    discard the session's client (which then reconciles the entry)."""
    sid = "trap"
    engine = await _make_engine(db, tmp_path, sid, stale_minutes=360)
    await _feed(engine, sid, "task_started", task_id="t1", description="build")
    engine._bg_task_registry[sid]["t1"]["last_event_at"] = (
        time.monotonic() - 7 * 3600
    )
    engine.sessions.get_idle_client_ids = lambda _secs: [sid]
    engine._discard_client = AsyncMock()

    await engine.run_idle_client_sweep()
    engine._discard_client.assert_awaited_once()


# --------------------------------------------------------------------------- #
#  Normal completion is unchanged                                             #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_normal_completion_settles_and_prunes(db, tmp_path):
    """A task that completes normally settles and prunes as before —
    the fix does not disturb the happy path."""
    sid = "happy"
    engine = await _make_engine(db, tmp_path, sid)
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        await _feed(engine, sid, "task_started", task_id="t1", description="x")
        await _feed(engine, sid, "task_notification", task_id="t1", status="completed")
        assert engine._has_live_background_tasks(sid) is False
        engine._prune_bg_tasks(sid)
        assert engine._bg_task_registry.get(sid) in (None, {})
