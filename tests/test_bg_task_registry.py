"""Regression tests for the background-task registry staleness / reconcile
that fixes the phantom "parked" session dot.

The registry is event-driven: an entry goes "running" on ``task_started`` and
only settles on a terminal ``task_updated``/``task_notification``. If that
terminal event never arrives (a detached ``&`` child the CLI stops observing, a
dropped event, a client torn down early) the entry would stay "running"
forever, the idle sweep would keep skipping the session, its client would never
be reaped, and the sidebar would show a permanent parked dot.

Two guards: a staleness cutoff in ``_has_live_background_tasks`` (so a silent
entry stops blocking the sweep) and a reconcile when the client is discarded
(so the dot clears at once).
"""

import time
from unittest.mock import AsyncMock, patch

import pytest

from nerve.agent.backends import events as ev
from nerve.agent.engine import AgentEngine
from nerve.config import NerveConfig


async def _make_engine(db, tmp_path, session_id):
    workspace = tmp_path / f"ws-{session_id}"
    workspace.mkdir(parents=True, exist_ok=True)
    cfg = NerveConfig.from_dict({"workspace": str(workspace), "agent": {"backend": "claude"}})
    engine = AgentEngine(cfg, db)
    await engine.sessions.get_or_create(session_id, source="web", actor=None)
    return engine


async def _feed(engine, session_id, subtype, **data):
    await engine._handle_system_event(
        session_id, ev.SystemEvent(subtype=subtype, data=data),
    )


def _backdate(engine, session_id, task_id, seconds):
    engine._bg_task_registry[session_id][task_id]["last_event_at"] = (
        time.monotonic() - seconds
    )


@pytest.mark.asyncio
async def test_stale_running_entry_not_live(db, tmp_path):
    """A "running" entry silent past the cutoff reads as not-live — without
    mutating the registry (pure predicate)."""
    engine = await _make_engine(db, tmp_path, "stale")
    await _feed(engine, "stale", "task_started", task_id="t1", description="build")
    _backdate(engine, "stale", "t1", AgentEngine._BG_TASK_STALE_SECONDS + 3600)
    assert engine._has_live_background_tasks("stale") is False
    assert engine._bg_task_registry["stale"]["t1"]["status"] == "running"


@pytest.mark.asyncio
async def test_fresh_running_entry_is_live(db, tmp_path):
    engine = await _make_engine(db, tmp_path, "fresh")
    await _feed(engine, "fresh", "task_started", task_id="t1", description="build")
    assert engine._has_live_background_tasks("fresh") is True


@pytest.mark.asyncio
async def test_progress_refreshes_liveness(db, tmp_path):
    """A no-op-for-UI task_progress still refreshes the stamp, so a long,
    actively-progressing task never goes stale."""
    engine = await _make_engine(db, tmp_path, "prog")
    await _feed(engine, "prog", "task_started", task_id="t1", description="wf")
    _backdate(engine, "prog", "t1", AgentEngine._BG_TASK_STALE_SECONDS + 3600)
    assert engine._has_live_background_tasks("prog") is False
    await _feed(engine, "prog", "task_progress", task_id="t1", description="wf")
    assert engine._has_live_background_tasks("prog") is True


@pytest.mark.asyncio
async def test_teardown_reconcile_clears_and_broadcasts(db, tmp_path):
    engine = await _make_engine(db, tmp_path, "rec")
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        await _feed(engine, "rec", "task_started", task_id="t1", description="build")
        assert engine._has_live_background_tasks("rec") is True

        await engine._reconcile_bg_tasks_on_teardown("rec")

        assert engine._has_live_background_tasks("rec") is False
        assert engine._bg_task_registry.get("rec") in (None, {})
        types = [c.args[1]["type"] for c in bc.broadcast.await_args_list]
        assert "background_tasks_update" in types and "session_running" in types
        running = next(
            c.args[1] for c in bc.broadcast.await_args_list
            if c.args[1]["type"] == "session_running"
        )
        assert running["has_background_tasks"] is False


@pytest.mark.asyncio
async def test_discard_client_reconciles(db, tmp_path):
    """End-to-end: _discard_client clears a stuck entry even with no terminal
    event (the idle-sweep / teardown path)."""
    engine = await _make_engine(db, tmp_path, "disc")
    engine._memorize_session = AsyncMock()
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        await _feed(engine, "disc", "task_started", task_id="t1", description="build")
        await engine._discard_client("disc")
        assert engine._has_live_background_tasks("disc") is False


@pytest.mark.asyncio
async def test_idle_sweep_keeps_live_reaps_stale(db, tmp_path):
    """The trap-break in context: the sweep skips a session with a fresh entry
    but proceeds to discard one whose entry has gone stale."""
    engine = await _make_engine(db, tmp_path, "keep")
    await engine.sessions.get_or_create("trap", source="web", actor=None)
    await _feed(engine, "keep", "task_started", task_id="t1", description="build")
    await _feed(engine, "trap", "task_started", task_id="t2", description="build")
    _backdate(engine, "trap", "t2", AgentEngine._BG_TASK_STALE_SECONDS + 3600)

    engine.sessions.get_idle_client_ids = lambda _secs: ["keep", "trap"]
    discarded: list[str] = []
    engine._discard_client = AsyncMock(side_effect=lambda sid, **_k: discarded.append(sid))

    await engine.run_idle_client_sweep()
    assert discarded == ["trap"]
