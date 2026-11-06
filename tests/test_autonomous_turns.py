"""Tests for autonomous-turn draining — nerve.agent.engine.

The CLI continues sessions on its own (background task settles →
task_notification → full agent turn inside the subprocess).  These tests
cover the engine pieces that surface that activity:

- ``_handle_system_event`` — task lifecycle → agent records (and the
  legacy background-task chips)
- ``_drain_pending_messages`` — buffered autonomous turns → UI + DB
  (consumed through the client's idle-event API); sub-agent (sidechain)
  traffic never opens, frames or persists a turn
- ``ClaudeClient.buffer_used`` — non-destructive buffer probe
"""

import asyncio
import contextlib
import logging
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import anyio
import pytest

from claude_agent_sdk import ResultMessage, SystemMessage

from nerve.agent.agents import AgentRegistry
from nerve.agent.backends import events as ev
from nerve.agent.backends.base import SessionSpec
from nerve.agent.backends.claude import (
    ClaudeClient,
    ClaudeToolPermissions,
    translate_message,
)
from nerve.agent.engine import AgentEngine, _TurnState
from nerve.agent.interactive import (
    InteractiveToolHandler,
    register_handler,
    unregister_handler,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_engine() -> AgentEngine:
    """Minimal AgentEngine stub for drain/system-message tests."""
    engine = AgentEngine.__new__(AgentEngine)
    engine.config = SimpleNamespace(
        workspace="/tmp",
        agent=SimpleNamespace(
            cli_idle_timeout_seconds=2,
            context_1m=False,
        ),
    )
    engine._workflows = {}
    engine._idle_watchers = {}
    engine._session_locks = {}
    engine._session_models = {}
    engine._observed_models = {}
    engine.agents = AgentRegistry.for_tests()
    engine._turn_watches = {}
    return engine


class _FakeStream:
    """Stand-in for the SDK's MemoryObjectReceiveStream."""

    def __init__(self, items: list[dict], max_buffer_size: int = 100):
        self._send, self._recv = anyio.create_memory_object_stream(
            max_buffer_size=max_buffer_size,
        )
        for item in items:
            self._send.send_nowait(item)

    def receive_nowait(self):
        return self._recv.receive_nowait()

    async def receive(self):
        return await self._recv.receive()

    def statistics(self):
        return self._recv.statistics()

    def push(self, item: dict) -> None:
        self._send.send_nowait(item)


def _fake_client(stream: _FakeStream) -> ClaudeClient:
    """A real ClaudeClient wired to a fake SDK whose receive stream is the
    given _FakeStream — the drain/watcher consume the client's idle-event
    API (try_receive_idle_events / receive_idle_events / buffer_used /
    is_alive) exactly as in production, fed by the same raw SDK payloads
    as before the backend split."""
    client = ClaudeClient.__new__(ClaudeClient)
    client._spec = SessionSpec(
        session_id="s1", source="web", model="m", effort="high",
        system_prompt="", cwd="/tmp",
    )
    client._sdk = SimpleNamespace(
        _query=SimpleNamespace(_message_receive=stream),
        # A live subprocess (returncode None) so is_alive() reports True.
        _transport=SimpleNamespace(_process=SimpleNamespace(returncode=None)),
    )
    client._native_session_id = None
    return client


async def _handle_system_message(engine: AgentEngine, session_id, message) -> None:
    """Route an SDK SystemMessage the way the live pipeline does: translate
    it into a normalized SystemEvent, then hand it to the engine."""
    for event in translate_message(message):
        await engine._handle_system_event(session_id, event)


def _sys_msg(subtype: str, **data) -> dict:
    return {"type": "system", "subtype": subtype, "session_id": "sdk-1",
            "uuid": "u-1", **data}


def _assistant_text(text: str) -> dict:
    return {
        "type": "assistant",
        "message": {
            "model": "claude-test",
            "content": [{"type": "text", "text": text}],
        },
        "session_id": "sdk-1",
    }


def _result_msg() -> dict:
    return {
        "type": "result",
        "subtype": "success",
        "duration_ms": 10,
        "duration_api_ms": 8,
        "is_error": False,
        "num_turns": 1,
        "session_id": "sdk-1",
        "total_cost_usd": 0.01,
        "usage": {"input_tokens": 5, "output_tokens": 7},
    }


# ---------------------------------------------------------------------------
# ClaudeClient.buffer_used
# ---------------------------------------------------------------------------


def test_buffer_used_counts_pending():
    stream = _FakeStream([_sys_msg("task_updated", task_id="t1", patch={})])
    client = _fake_client(stream)
    assert client.buffer_used() == 1
    stream.receive_nowait()
    assert client.buffer_used() == 0


def test_buffer_used_handles_missing_internals():
    client = _fake_client(_FakeStream([]))
    client._sdk = SimpleNamespace()
    assert client.buffer_used() == 0
    client._sdk = SimpleNamespace(_query=None)
    assert client.buffer_used() == 0


# ---------------------------------------------------------------------------
# _handle_system_event → agent records + legacy background-task chips
# (SystemMessage fixtures fed through translate_message, as in the live
# pipeline)
# ---------------------------------------------------------------------------


async def _seed_task(engine: AgentEngine, sid: str, task_id: str, **data) -> None:
    """A live background task, the way the CLI reports one."""
    await engine.agents.on_system_event(sid, ev.SystemEvent("task_started", {
        "task_id": task_id, "task_type": "local_bash", "description": task_id,
        **data,
    }))


def _record(engine: AgentEngine, sid: str, task_id: str):
    return next(
        r for r in engine.agents._sessions[sid].records.values()
        if r.task_id == task_id
    )


@pytest.mark.asyncio
async def test_task_lifecycle_updates_registry_and_broadcasts():
    engine = _make_engine()

    events = []
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock(
            side_effect=lambda sid, msg: events.append(msg),
        )

        started = SystemMessage(
            subtype="task_started",
            data=_sys_msg(
                "task_started", task_id="t1",
                description="run tests", task_type="local_bash",
            ),
        )
        await _handle_system_message(engine, "s1", started)

        rec = _record(engine, "s1", "t1")
        assert (rec.status, rec.kind, rec.description) == ("running", "shell", "run tests")
        assert engine.has_live_background_tasks("s1") is True

        notified = SystemMessage(
            subtype="task_notification",
            data=_sys_msg(
                "task_notification", task_id="t1", status="completed",
            ),
        )
        await _handle_system_message(engine, "s1", notified)
        assert (rec.status, rec.end_reason) == ("completed", "completed")
        assert engine.has_live_background_tasks("s1") is False

    # Legacy chips for tabs from before the roster: one update per change.
    assert len(events) == 2
    assert all(e["type"] == "background_tasks_update" for e in events)
    assert events[0]["tasks"] == [
        {"task_id": "t1", "label": "run tests", "tool": "Bash", "status": "running"},
    ]
    assert events[-1]["tasks"][0]["status"] == "done"


@pytest.mark.asyncio
async def test_task_failed_maps_to_failed_status():
    engine = _make_engine()
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        await _seed_task(engine, "s1", "t9")
        msg = SystemMessage(
            subtype="task_notification",
            data=_sys_msg("task_notification", task_id="t9", status="failed"),
        )
        await _handle_system_message(engine, "s1", msg)
    rec = _record(engine, "s1", "t9")
    assert (rec.status, rec.end_reason) == ("failed", "error")
    assert engine.agents.legacy_task_list("s1")[0]["status"] == "failed"


@pytest.mark.asyncio
async def test_killed_task_is_stopped_never_done():
    """A CLI ``killed`` is a stop, never shown as completed."""
    engine = _make_engine()
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        await _seed_task(engine, "s1", "t5")
        await _handle_system_message(engine, "s1", SystemMessage(
            subtype="task_updated",
            data=_sys_msg("task_updated", task_id="t5", patch={"status": "killed"}),
        ))
    rec = _record(engine, "s1", "t5")
    assert rec.status == "stopped" and rec.end_reason == "unknown_stop"


@pytest.mark.asyncio
async def test_agent_task_type_uses_agent_tool():
    engine = _make_engine()
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        msg = SystemMessage(
            subtype="task_started",
            data=_sys_msg(
                "task_started", task_id="t2",
                description="explore repo", task_type="local_agent",
            ),
        )
        await _handle_system_message(engine, "s1", msg)
    assert _record(engine, "s1", "t2").kind == "agent"
    assert engine.agents.legacy_task_list("s1")[0]["tool"] == "Agent"


@pytest.mark.asyncio
async def test_non_task_subtypes_ignored():
    engine = _make_engine()
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        await _handle_system_message(
            engine, "s1", SystemMessage(subtype="init", data={"type": "system"}),
        )
        bc.broadcast.assert_not_called()
    assert engine.agents.live("s1") == []
    assert engine.agents.summary("s1")["total"] == 0


@pytest.mark.asyncio
async def test_background_tasks_changed_reaches_the_registry():
    """The live background set (dropped before) now feeds the registry."""
    engine = _make_engine()
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        await _handle_system_message(engine, "s1", SystemMessage(
            subtype="background_tasks_changed",
            data=_sys_msg("background_tasks_changed", tasks=[
                {"task_id": "b1x2y3z4w", "task_type": "local_bash", "description": "npm run dev"},
            ]),
        ))
        bc.broadcast.assert_not_called()  # no legacy chip churn for the level
    rec = _record(engine, "s1", "b1x2y3z4w")
    assert rec.status == "running" and rec.in_live_set and rec.kind == "shell"


@pytest.mark.asyncio
async def test_workflow_task_emits_progress_and_persists():
    """A dynamic-workflow task (task_type=local_workflow, workflow_progress on
    task_progress, terminal via task_updated) drives workflow_progress events
    and persists the final snapshot. Shapes mirror a real captured run."""
    engine = _make_engine()
    engine.db = SimpleNamespace(merge_workflow_into_call=AsyncMock())
    # _process_sdk_message would have registered the Workflow tool_use id.
    engine._workflows["s1"] = {"wf-tool": {"name": "Workflow", "snapshot": None}}

    wf_events = []
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        bc.broadcast_workflow_progress = AsyncMock(
            side_effect=lambda sid, tid, snap: wf_events.append((tid, snap)),
        )
        await _handle_system_message(engine, "s1", SystemMessage(
            subtype="task_started",
            data=_sys_msg(
                "task_started", task_id="wt", tool_use_id="wf-tool",
                task_type="local_workflow", workflow_name="verify-ui",
                description="tiny workflow",
            ),
        ))
        await _handle_system_message(engine, "s1", SystemMessage(
            subtype="task_progress",
            data=_sys_msg(
                "task_progress", task_id="wt", tool_use_id="wf-tool",
                description="tiny workflow",
                workflow_progress=[
                    {"type": "workflow_agent", "label": "echo", "phaseIndex": 1,
                     "phaseTitle": "Echo", "state": "running", "model": "opus",
                     "tokens": 100, "toolCalls": 0},
                ],
            ),
        ))
        await _handle_system_message(engine, "s1", SystemMessage(
            subtype="task_updated",
            data=_sys_msg(
                "task_updated", task_id="wt", tool_use_id="wf-tool",
                patch={"status": "completed", "end_time": 1},
            ),
        ))

    # The workflow is one agent record carrying the latest snapshot.
    rec = _record(engine, "s1", "wt")
    assert rec.kind == "workflow" and rec.status == "completed"
    assert rec.workflow["name"] == "verify-ui"
    assert rec.workflow["status"] == "completed"
    assert engine.agents.legacy_task_list("s1")[0]["tool"] == "Workflow"
    # One progress broadcast per task message.
    assert len(wf_events) == 3
    tid, last = wf_events[-1]
    assert tid == "wf-tool"
    assert last["name"] == "verify-ui"
    assert last["status"] == "completed"
    assert last["agentCount"] == 1  # carried over from the progress snapshot
    # Terminal snapshot persisted onto the Workflow block.
    engine.db.merge_workflow_into_call.assert_awaited_once()


@pytest.mark.asyncio
async def test_prune_turn_state_clears_triggers_and_settled_chips():
    engine = _make_engine()
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        await _seed_task(engine, "s1", "t1")
        await _seed_task(engine, "s1", "t2")
        await engine.agents.on_system_event("s1", ev.SystemEvent(
            "task_notification", {"task_id": "t1", "status": "completed"},
        ))
    assert [t["task_id"] for t in engine.agents.legacy_task_list("s1")] == ["t1", "t2"]
    engine._prune_turn_state("s1")
    # A new user turn: settled chips drop, running ones stay, and the settled
    # agent no longer explains the next autonomous turn.
    assert [t["task_id"] for t in engine.agents.legacy_task_list("s1")] == ["t2"]
    assert engine.agents.take_turn_triggers("s1") == ([], 0)


# ---------------------------------------------------------------------------
# _drain_pending_messages
# ---------------------------------------------------------------------------


def _patch_finalize(engine: AgentEngine):
    """Replace _finalize_turn with a recorder (avoids DB plumbing).

    The recorded ``bump_updated_at`` and ``persist_message`` are stashed on
    the turn state so callers can assert them without every existing call
    site growing a second binding.
    """
    finalized: list[_TurnState] = []

    async def _record(
        session_id, st, channel, bump_updated_at=True, persist_message=True,
    ):
        st.recorded_bump_updated_at = bump_updated_at  # type: ignore[attr-defined]
        st.recorded_persist_message = persist_message  # type: ignore[attr-defined]
        finalized.append(st)

    engine._finalize_turn = _record  # type: ignore[method-assign]
    return finalized


def _framing_sessions(**extra) -> SimpleNamespace:
    """A SessionManager stand-in for drains that frame a turn."""
    return SimpleNamespace(
        mark_running=lambda sid: None,
        mark_not_running=lambda sid: None,
        set_turn_task=lambda sid, task: None,
        clear_turn_task=lambda sid, task: None,
        **extra,
    )


@pytest.mark.asyncio
async def test_drain_empty_buffer_returns_immediately():
    engine = _make_engine()
    stream = _FakeStream([])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)

    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        turns = await engine._drain_pending_messages("s1", client, "web", None)

    assert turns == 0
    assert finalized == []


@pytest.mark.asyncio
async def test_drain_processes_full_autonomous_turn():
    """task events + init + assistant + result → one finalized turn."""
    engine = _make_engine()
    stream = _FakeStream([
        _sys_msg("task_started", task_id="t1", task_type="local_bash",
                 description="nightly build"),
        _sys_msg("task_updated", task_id="t1",
                 patch={"status": "completed"}),
        _sys_msg("task_notification", task_id="t1", status="completed",
                 output_file="/tmp/x", summary="done"),
        _sys_msg("init", cwd="/tmp"),
        _assistant_text("Background job finished."),
        _result_msg(),
    ])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)

    events = []
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock(side_effect=lambda sid, m: events.append(m))
        bc.broadcast_token = AsyncMock()
        bc.is_buffering = lambda sid: False
        bc.start_buffering = lambda sid: None
        bc.stop_buffering = lambda sid: []
        bc.mark_turn_open = lambda sid: None
        bc.is_turn_open = lambda sid: False
        engine.sessions = _framing_sessions()
        turns = await engine._drain_pending_messages(
            "s1", client, "web", None, manage_framing=True,
        )

    assert turns == 1
    assert len(finalized) == 1
    st = finalized[0]
    assert st.full_response_text == "Background job finished."
    # Leading marker block names what woke the orchestrator: the task that
    # settled just before this autonomous turn.
    (agent_id,) = engine.agents._sessions["s1"].records
    trigger = {"agent_id": agent_id, "label": "nightly build", "kind": "shell",
               "status": "completed"}
    assert st.ordered_blocks[0] == {"type": "auto", "triggers": [trigger], "more": 0}
    # Chips updated + auto_turn marker + session_running framing
    types = [e.get("type") for e in events]
    assert "background_tasks_update" in types
    (auto_turn,) = [e for e in events if e.get("type") == "auto_turn"]
    assert auto_turn["triggers"] == [trigger] and auto_turn["more"] == 0
    framing = [e for e in events if e.get("type") == "session_running"]
    assert [e["is_running"] for e in framing] == [True, False]
    # Every transition carries the session's pending work so the sidebar can
    # keep a parked session in its Running group without a list refetch.
    assert all(
        e["session_id"] == "s1"
        and e["pending_wakeup_at"] is None
        and e["has_background_tasks"] is False
        for e in framing
    )
    # An autonomous turn is a continuation — it must not re-sort the sidebar.
    assert st.recorded_bump_updated_at is False
    # Buffer fully consumed — nothing left to desync the next turn
    assert client.buffer_used() == 0


@pytest.mark.asyncio
async def test_drain_standalone_task_events_do_not_open_turn():
    """Task events with no following turn: chips update, no turn framing."""
    engine = _make_engine()
    stream = _FakeStream([
        _sys_msg("task_started", task_id="t1", description="bg job",
                 task_type="local_bash"),
    ])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)

    events = []
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock(side_effect=lambda sid, m: events.append(m))
        turns = await engine._drain_pending_messages(
            "s1", client, "web", None, manage_framing=True,
        )

    assert turns == 0
    assert finalized == []
    types = [e.get("type") for e in events]
    assert types == ["background_tasks_update"]  # no session_running/auto_turn


@pytest.mark.asyncio
async def test_drain_consumes_stray_result_without_turn():
    """A buffered ResultMessage with no content is consumed, not rendered."""
    engine = _make_engine()
    stream = _FakeStream([_result_msg()])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)

    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        turns = await engine._drain_pending_messages("s1", client, "web", None)

    assert turns == 0
    assert finalized == []
    assert client.buffer_used() == 0


@pytest.mark.asyncio
async def test_drain_parks_mid_turn_until_result():
    """Mid-turn drain waits for late messages instead of exiting early."""
    engine = _make_engine()
    stream = _FakeStream([_assistant_text("working...")])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)

    async def _push_later():
        await asyncio.sleep(0.1)
        stream.push(_result_msg())

    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        bc.broadcast_token = AsyncMock()
        pusher = asyncio.create_task(_push_later())
        turns = await engine._drain_pending_messages("s1", client, "web", None)
        await pusher

    assert turns == 1
    assert finalized[0].full_response_text == "working..."


@pytest.mark.asyncio
async def test_drain_timeout_persists_partial_and_raises():
    """Hung CLI mid-turn: partial turn persisted, TimeoutError propagates."""
    engine = _make_engine()
    engine.config.agent.cli_idle_timeout_seconds = 0.1
    stream = _FakeStream([_assistant_text("partial output")])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)

    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        bc.broadcast_token = AsyncMock()
        with pytest.raises(asyncio.TimeoutError):
            await engine._drain_pending_messages("s1", client, "web", None)

    assert len(finalized) == 1
    assert "partial output" in finalized[0].full_response_text
    assert "interrupted" in finalized[0].full_response_text


@pytest.mark.asyncio
async def test_drain_parks_on_init_until_content_arrives():
    """``init`` means a turn is in flight — drain waits for its content
    (model latency) and completes the whole turn in one call."""
    engine = _make_engine()
    stream = _FakeStream([_sys_msg("init", cwd="/tmp")])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)

    async def _push_later():
        await asyncio.sleep(0.1)
        stream.push(_assistant_text("late content"))
        stream.push(_result_msg())

    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        bc.broadcast_token = AsyncMock()
        pusher = asyncio.create_task(_push_later())
        turns = await engine._drain_pending_messages(
            "s1", client, "web", None, first_content_timeout=5.0,
        )
        await pusher

    assert turns == 1
    assert finalized[0].full_response_text == "late content"


@pytest.mark.asyncio
async def test_drain_drops_empty_turn_on_first_content_timeout():
    """``init`` with no content within first_content_timeout: the empty
    turn is dropped (no persist, no exception) — the watcher's next poll
    picks the content up instead."""
    engine = _make_engine()
    stream = _FakeStream([_sys_msg("init", cwd="/tmp")])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)

    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        turns = await engine._drain_pending_messages(
            "s1", client, "web", None, first_content_timeout=0.05,
        )

    assert turns == 0
    assert finalized == []


@pytest.mark.asyncio
async def test_drain_handles_end_sentinel():
    """Reader 'end' sentinel closes the drain cleanly."""
    engine = _make_engine()
    stream = _FakeStream([{"type": "end"}])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)

    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        turns = await engine._drain_pending_messages("s1", client, "web", None)

    assert turns == 0
    assert finalized == []


@pytest.mark.asyncio
async def test_drain_multiple_turns_in_one_call():
    """Two buffered autonomous turns finalize as two assistant messages."""
    engine = _make_engine()
    stream = _FakeStream([
        _assistant_text("turn one"),
        _result_msg(),
        _assistant_text("turn two"),
        _result_msg(),
    ])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)

    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        bc.broadcast_token = AsyncMock()
        turns = await engine._drain_pending_messages("s1", client, "web", None)

    assert turns == 2
    assert [st.full_response_text for st in finalized] == ["turn one", "turn two"]


# ---------------------------------------------------------------------------
# _process_agent_event — ResultMessage (translated to TurnCompleted) via the
# shared path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_agent_event_returns_true_on_result():
    engine = _make_engine()
    st = _TurnState()
    result = ResultMessage(
        subtype="success", duration_ms=1, duration_api_ms=1,
        is_error=False, num_turns=1, session_id="sdk-9",
        total_cost_usd=0.5, usage={"input_tokens": 1},
    )
    done = False
    for event in translate_message(result):
        done = await engine._process_agent_event("s1", event, st)
    assert done is True
    assert st.sdk_session_id == "sdk-9"
    assert st.last_usage == {"input_tokens": 1}
    assert st.result_meta["total_cost_usd"] == 0.5


# ---------------------------------------------------------------------------
# run_idle_client_sweep — never discard a client parked on a live bg task
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_has_live_background_tasks():
    engine = _make_engine()

    async def _settle(sid, task_id):
        await engine.agents.on_system_event(sid, ev.SystemEvent(
            "task_notification", {"task_id": task_id, "status": "completed"},
        ))

    await _seed_task(engine, "running", "t1")
    await _seed_task(engine, "settled", "t2")
    await _settle("settled", "t2")
    await _seed_task(engine, "mixed", "t3")
    await _settle("mixed", "t3")
    await _seed_task(engine, "mixed", "t4")
    # Housekeeping tasks never count as background work.
    await _seed_task(engine, "ambient", "d1", task_type="dream")
    assert engine._has_live_background_tasks("running") is True
    assert engine._has_live_background_tasks("settled") is False
    assert engine._has_live_background_tasks("mixed") is True
    assert engine._has_live_background_tasks("ambient") is False
    assert engine._has_live_background_tasks("unknown") is False
    assert engine.background_agent_count("mixed") == 1


@pytest.mark.asyncio
async def test_idle_sweep_skips_sessions_with_live_background_tasks():
    """Regression: a session parked on a live background task must survive the
    idle sweep — discarding its client would tear down the idle-stream watcher
    that delivers the task's completion turn (the lost-wakeup bug)."""
    engine = _make_engine()
    engine.config.sessions = SimpleNamespace(client_idle_timeout_minutes=60)
    engine.sessions = SimpleNamespace(
        get_idle_client_ids=lambda _timeout: ["busy", "free"],
        _clients={"busy": object(), "free": object()},
    )
    await _seed_task(engine, "busy", "t1")
    await _seed_task(engine, "free", "t2")
    await engine.agents.on_system_event("free", ev.SystemEvent(
        "task_notification", {"task_id": "t2", "status": "completed"},
    ))
    discarded: list[str] = []

    async def _fake_discard(sid, **_kw):
        discarded.append(sid)

    engine._discard_client = _fake_discard

    n = await engine.run_idle_client_sweep()

    assert discarded == ["free"]  # only the session with no live bg task
    assert n == 1
    assert engine.agents.has_live("busy")  # left intact for its watcher


@pytest.mark.asyncio
async def test_idle_sweep_disabled_when_timeout_zero():
    engine = _make_engine()
    engine.config.sessions = SimpleNamespace(client_idle_timeout_minutes=0)
    called = False

    def _should_not_run(_timeout):
        nonlocal called
        called = True
        return []

    engine.sessions = SimpleNamespace(
        get_idle_client_ids=_should_not_run, _clients={},
    )
    assert await engine.run_idle_client_sweep() == 0
    assert called is False  # short-circuits before touching the session store


# ---------------------------------------------------------------------------
# End-to-end: a parked one-shot session resumes when its bg task completes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_idle_watcher_resumes_parked_session_on_background_completion():
    """The payoff of keeping a one-shot (cron/hook) client alive.

    A cron/hook run that yields while a ``run_in_background`` task is still live
    is now KEPT ALIVE (``_teardown_oneshot_client`` skips the discard). This
    drives the REAL ``_idle_stream_watcher`` against that parked client and
    proves the end-to-end resume: when the task settles, the watcher delivers
    the completion as an autonomous turn — the agent resumes and finishes its
    work with no new ``run()`` — and the task flips out of "live" so the idle
    sweep can then reap the client (the same lifecycle a web session uses).

    Together with the run_cron/run_hook keep-alive tests (teardown is deferred)
    and the idle-sweep tests (reaped once settled), this closes the loop on the
    2026-06-22 fix-worker strand.
    """
    engine = _make_engine()
    engine._IDLE_STREAM_POLL_SECONDS = 0.05  # snappy poll for the test
    stream = _FakeStream([])  # agent just yielded — nothing buffered yet
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)

    # The parked state: a background task still running after the yield.
    await _seed_task(engine, "s1", "t1", description="make test")
    assert engine._has_live_background_tasks("s1") is True
    # (the fake client's subprocess reports alive — see _fake_client)
    engine.sessions = _framing_sessions(
        get_client=lambda _sid: client,
        is_running=lambda _sid: False,
        register_task=lambda _sid, _task: None,
    )

    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        bc.broadcast_token = AsyncMock()
        bc.is_buffering = lambda sid: False
        bc.start_buffering = lambda sid: None
        bc.stop_buffering = lambda sid: []
        bc.mark_turn_open = lambda sid: None
        bc.is_turn_open = lambda sid: False

        watcher = asyncio.create_task(
            engine._idle_stream_watcher("s1", client, "cron", None),
        )
        try:
            # Background task completes: the CLI emits its settle + a follow-up
            # autonomous turn into the live client's stream.
            stream.push(_sys_msg("task_notification", task_id="t1",
                                 status="completed", output_file="/tmp/x",
                                 summary="done"))
            stream.push(_sys_msg("init", cwd="/tmp"))
            stream.push(_assistant_text(
                "Build passed; pushed the fix and updated the task."))
            stream.push(_result_msg())

            for _ in range(100):  # up to ~5s
                if finalized:
                    break
                await asyncio.sleep(0.05)
        finally:
            watcher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watcher

    # The watcher resumed the parked session as exactly one autonomous turn.
    assert len(finalized) == 1
    assert finalized[0].full_response_text == (
        "Build passed; pushed the fix and updated the task."
    )
    # Task settled → no longer live → the idle sweep may now reap the client.
    assert engine._has_live_background_tasks("s1") is False


# ---------------------------------------------------------------------------
# Sidechain routing: sub-agent traffic never opens, extends, frames or
# persists an orchestrator turn (the drain fix). Fixtures are raw SDK dicts
# tagged with parent_tool_use_id, fed through _FakeStream like the CLI's
# forwarded sub-agent frames.
# ---------------------------------------------------------------------------


def _sidechain_text(text: str, parent: str = "toolu_x", msg_id: str = "msg_side") -> dict:
    return {
        "type": "assistant",
        "message": {
            "id": msg_id,
            "model": "claude-sub",
            "content": [{"type": "text", "text": text}],
            "usage": {"input_tokens": 3, "output_tokens": 5},
        },
        "parent_tool_use_id": parent,
        "session_id": "sdk-1",
    }


def _sidechain_tool_use(
    name: str, tool_use_id: str, tool_input: dict, parent: str = "toolu_x",
) -> dict:
    return {
        "type": "assistant",
        "message": {
            "id": f"msg_{tool_use_id}",
            "model": "claude-sub",
            "content": [{
                "type": "tool_use", "id": tool_use_id, "name": name,
                "input": tool_input,
            }],
        },
        "parent_tool_use_id": parent,
        "session_id": "sdk-1",
    }


def _tool_result_msg(
    tool_use_id: str, content: str, parent: str | None = None,
    tool_use_result: dict | None = None,
) -> dict:
    msg: dict = {
        "type": "user",
        "message": {"role": "user", "content": [{
            "type": "tool_result", "tool_use_id": tool_use_id,
            "content": content,
        }]},
        "parent_tool_use_id": parent,
        "session_id": "sdk-1",
    }
    if tool_use_result is not None:
        msg["tool_use_result"] = tool_use_result
    return msg


def _main_tool_use(name: str, tool_use_id: str, tool_input: dict) -> dict:
    return {
        "type": "assistant",
        "message": {
            "model": "claude-test",
            "content": [{
                "type": "tool_use", "id": tool_use_id, "name": name,
                "input": tool_input,
            }],
        },
        "session_id": "sdk-1",
    }


class _RecordingSessions:
    """SessionManager stand-in that records the framing calls."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.messages: list[dict] = []

    def mark_running(self, sid):
        self.calls.append(("mark_running", sid))

    def mark_not_running(self, sid):
        self.calls.append(("mark_not_running", sid))

    def set_turn_task(self, sid, task):
        self.calls.append(("set_turn_task", sid, task))

    def clear_turn_task(self, sid, task):
        self.calls.append(("clear_turn_task", sid, task))

    async def add_message(self, sid, role, content, **kw):
        self.messages.append({"session_id": sid, "role": role, "content": content, **kw})

    def names(self) -> list[str]:
        return [c[0] for c in self.calls]


def _recording_broadcaster(bc, events: list[dict]) -> None:
    """Wire a patched broadcaster so every message lands in ``events``."""

    async def _broadcast(sid, msg, *a, **kw):
        events.append(msg)

    bc.broadcast = AsyncMock(side_effect=_broadcast)
    for kind, attr in (
        ("token", "broadcast_token"), ("thinking", "broadcast_thinking"),
    ):
        async def _send(sid, content, _kind=kind, **kw):
            events.append({"type": _kind, "session_id": sid, "content": content, **kw})
        setattr(bc, attr, AsyncMock(side_effect=_send))

    async def _tool_use(sid, tool, tool_input, **kw):
        events.append({"type": "tool_use", "tool": tool, "input": tool_input, **kw})

    async def _tool_result(sid, result, **kw):
        events.append({"type": "tool_result", "result": result, **kw})

    async def _done(sid, **kw):
        events.append({"type": "done", "session_id": sid, **kw})

    bc.broadcast_tool_use = AsyncMock(side_effect=_tool_use)
    bc.broadcast_tool_result = AsyncMock(side_effect=_tool_result)
    bc.broadcast_done = AsyncMock(side_effect=_done)
    bc.is_buffering = lambda sid: False
    bc.start_buffering = lambda sid: None
    bc.stop_buffering = lambda sid: []
    bc.mark_turn_open = lambda sid: None
    bc.is_turn_open = lambda sid: False
    bc.clear_turn_open = lambda sid: None


def _types(events: list[dict]) -> list[str]:
    return [e.get("type") for e in events]


async def _only_agent(engine: AgentEngine, sid: str = "s1"):
    """The session's single agent record and its transcript items."""
    (rec,) = engine.agents._sessions[sid].records.values()
    page = await engine.agents.transcript(sid, rec.id, limit=500)
    return rec, page["items"]


async def _spawn(engine: AgentEngine, tool_use_id: str = "toolu_x", **inp) -> None:
    """The orchestrator's Agent call that the sidechain frames route to."""
    await engine.agents.note_main_tool_use("s1", ev.ToolUse(
        tool_use_id=tool_use_id, name="Agent",
        input={"description": "build-api", "prompt": "Build it", **inp},
    ))


@pytest.mark.asyncio
async def test_drain_ignores_sidechain_when_no_turn_open():
    """The regression this guards against: a background agent's frames buffered between turns
    must not open an autonomous turn. No auto_turn, no mark_running, no
    session_running(True), nothing finalized or persisted — and the buffer
    is still consumed, into the agent registry."""
    engine = _make_engine()
    await _spawn(engine)
    stream = _FakeStream([
        _sidechain_text("looking around"),
        _sidechain_tool_use("Bash", "toolu_child", {"command": "pytest -q"}),
        _tool_result_msg("toolu_child", "212 passed", parent="toolu_x"),
    ])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)
    engine.sessions = _RecordingSessions()

    events: list[dict] = []
    with patch("nerve.agent.engine.broadcaster") as bc:
        _recording_broadcaster(bc, events)
        turns = await engine._drain_pending_messages(
            "s1", client, "web", None, manage_framing=True,
        )

    assert turns == 0
    assert finalized == []
    assert events == []  # no auto_turn, no session_running, no token/tool events
    assert engine.sessions.calls == []  # never mark_running / set_turn_task
    assert engine.sessions.messages == []  # nothing persisted
    assert client.buffer_used() == 0  # consumed, cannot desync the next turn
    # ... into the agent's own record and transcript.
    rec, items = await _only_agent(engine)
    assert rec.spawn_tool_use_id == "toolu_x"
    assert [i["kind"] for i in items] == ["text", "tool_call", "tool_result"]
    assert [i["seq"] for i in items] == [1, 2, 3]
    assert items[0]["text"] == "looking around"
    assert items[2]["result"] == "212 passed"
    assert rec.status == "running"  # its first frame proved it started
    assert rec.activity == "Bash: pytest -q"
    assert rec.tokens == 8 and rec.models == ["claude-sub"]


@pytest.mark.asyncio
async def test_sidechain_init_never_frames():
    """An ``init`` followed only by sub-agent traffic opens a tentative turn
    that frames nothing and ends at an absolute deadline, although batches
    keep arriving (sidechain traffic never extends the wait)."""
    engine = _make_engine()
    stream = _FakeStream([_sys_msg("init", cwd="/tmp")])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)
    engine.sessions = _RecordingSessions()

    async def _chatter():
        for i in range(40):  # ~0.8 s of steady sub-agent frames
            stream.push(_sidechain_text(f"step {i}", msg_id=f"m{i}"))
            await asyncio.sleep(0.02)

    events: list[dict] = []
    with patch("nerve.agent.engine.broadcaster") as bc:
        _recording_broadcaster(bc, events)
        pusher = asyncio.create_task(_chatter())
        started = time.monotonic()
        turns = await engine._drain_pending_messages(
            "s1", client, "web", None, manage_framing=True,
            first_content_timeout=0.2,
        )
        elapsed = time.monotonic() - started
        pusher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await pusher

    assert turns == 0
    assert finalized == []
    assert 0.15 <= elapsed < 0.6  # the deadline, not the chatter, ended it
    assert events == []
    assert engine.sessions.calls == []
    _rec, items = await _only_agent(engine)
    assert len(items) >= 3 and {i["kind"] for i in items} == {"text"}


@pytest.mark.asyncio
async def test_main_content_after_init_frames_once():
    """Main-chain content after a tentative ``init`` frames the turn exactly
    once; sub-agent frames before and inside it stay out of it."""
    engine = _make_engine()
    stream = _FakeStream([
        _sys_msg("init", cwd="/tmp"),
        _sidechain_text("sub before"),
        _assistant_text("Orchestrator speaks."),
        _sidechain_tool_use("Read", "toolu_r", {"file_path": "/tmp/a.py"}),
        _result_msg(),
    ])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)
    sessions = _RecordingSessions()
    engine.sessions = sessions

    events: list[dict] = []
    with patch("nerve.agent.engine.broadcaster") as bc:
        _recording_broadcaster(bc, events)
        drain = asyncio.create_task(engine._drain_pending_messages(
            "s1", client, "web", None, manage_framing=True,
            first_content_timeout=5.0,
        ))
        turns = await drain

    assert turns == 1
    assert len(finalized) == 1
    st = finalized[0]
    assert st.full_response_text == "Orchestrator speaks."
    assert st.ordered_blocks == [
        {"type": "auto"}, {"type": "text", "content": "Orchestrator speaks."},
    ]
    assert _types(events).count("auto_turn") == 1
    framing = [e for e in events if e.get("type") == "session_running"]
    assert [e["is_running"] for e in framing] == [True, False]
    assert sessions.names().count("mark_running") == 1
    # The framed drain is the Stop target while its turn runs, and only then.
    assert ("set_turn_task", "s1", drain) in sessions.calls
    assert ("clear_turn_task", "s1", drain) in sessions.calls
    # No sub-agent text reached the session channel.
    assert [e["content"] for e in events if e.get("type") == "token"] == [
        "Orchestrator speaks.",
    ]


@pytest.mark.asyncio
async def test_sidechain_inside_open_turn_not_persisted():
    """A framed autonomous turn persists only the orchestrator's own blocks:
    a sub-agent's text, tool calls and results streamed in the middle of it
    go to the registry."""
    engine = _make_engine()
    stream = _FakeStream([
        _sys_msg("init", cwd="/tmp"),
        _main_tool_use("Agent", "toolu_agent", {
            "description": "build-api", "prompt": "Build it",
            "subagent_type": "general-purpose",
        }),
        _sidechain_text("sub agent thinking out loud", parent="toolu_agent"),
        _sidechain_tool_use(
            "Edit", "toolu_edit", {"file_path": "/tmp/app.py"},
            parent="toolu_agent",
        ),
        _tool_result_msg("toolu_edit", "edited", parent="toolu_agent"),
        _tool_result_msg("toolu_agent", "All done: 4 files changed"),
        _assistant_text("build-api finished."),
        _result_msg(),
    ])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)
    engine.sessions = _RecordingSessions()

    events: list[dict] = []
    with patch("nerve.agent.engine.broadcaster") as bc, \
            patch("nerve.agent.engine._maybe_broadcast_file_changed"), \
            patch("nerve.agent.engine._maybe_broadcast_plan_update"):
        _recording_broadcaster(bc, events)
        turns = await engine._drain_pending_messages(
            "s1", client, "web", None, manage_framing=True,
            first_content_timeout=5.0,
        )

    assert turns == 1
    st = finalized[0]
    assert st.full_response_text == "build-api finished."
    assert [b["type"] for b in st.ordered_blocks] == ["auto", "tool_call", "text"]
    agent_block = st.ordered_blocks[1]
    assert agent_block["tool"] == "Agent"
    assert agent_block["result"] == "All done: 4 files changed"
    assert [c["tool"] for c in st.tool_calls_log] == ["Agent"]
    assert "sub agent" not in st.full_response_text
    rec, items = await _only_agent(engine)
    assert rec.spawn_tool_use_id == "toolu_agent"
    assert [i["kind"] for i in items] == ["text", "tool_call", "tool_result"]
    assert items[1]["tool"] == "Edit" and items[1]["input"] == {"file_path": "/tmp/app.py"}
    # The session channel carried only the orchestrator's events.
    assert not any(e.get("parent_tool_use_id") for e in events)


@pytest.mark.asyncio
async def test_drain_batch_cap_releases_lock():
    """With no turn open the drain stops after 200 batches, so a run()
    waiting on the run lock gets it (asyncio locks are FIFO) instead of
    starving behind sustained sub-agent traffic."""
    engine = _make_engine()
    stream = _FakeStream(
        [_sidechain_text(f"n{i}", msg_id=f"m{i}") for i in range(300)],
        max_buffer_size=1000,
    )
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)
    engine.sessions = _RecordingSessions()
    lock = asyncio.Lock()
    seen_by_run: list[int] = []

    async def _waiting_run():
        async with lock:
            seen_by_run.append(client.buffer_used())

    with patch("nerve.agent.engine.broadcaster") as bc:
        _recording_broadcaster(bc, [])
        async with lock:  # the watcher holds the run lock for its drain
            run = asyncio.create_task(_waiting_run())
            await asyncio.sleep(0)  # the run is now queued on the lock
            assert lock.locked() and not run.done()
            turns = await engine._drain_pending_messages(
                "s1", client, "web", None, manage_framing=True,
            )
            left_after_drain = client.buffer_used()
        await asyncio.wait_for(run, timeout=1.0)

    assert turns == 0
    assert finalized == []
    assert left_after_drain == 100  # returned after exactly 200 batches
    assert seen_by_run == [100]  # the waiting run got the lock next
    assert engine.sessions.calls == []
    rec, items = await _only_agent(engine)
    assert len(items) == 200 and rec.next_seq == 201


@pytest.mark.asyncio
async def test_quiet_subagent_survives_idle_timeout():
    """A background agent that goes silent for longer than the CLI idle
    timeout must not be treated as a hung runtime: no turn is open while
    only agents run, so the watcher never discards the client."""
    engine = _make_engine()
    engine.config.agent.cli_idle_timeout_seconds = 0.1
    engine._IDLE_STREAM_POLL_SECONDS = 0.02
    stream = _FakeStream([])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)
    discarded: list[str] = []

    async def _fake_discard(sid, **_kw):
        discarded.append(sid)

    engine._discard_client = _fake_discard
    engine.sessions = _framing_sessions(
        get_client=lambda _sid: client,
        is_running=lambda _sid: False,
    )

    with patch("nerve.agent.engine.broadcaster") as bc:
        _recording_broadcaster(bc, [])
        watcher = asyncio.create_task(
            engine._idle_stream_watcher("s1", client, "web", None),
        )
        try:
            stream.push(_sidechain_tool_use("Bash", "toolu_long", {
                "command": "sleep 900",
            }))
            await asyncio.sleep(0.5)  # 5x the idle timeout of silence
            assert not watcher.done()
        finally:
            watcher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watcher

    assert discarded == []
    assert finalized == []
    rec, items = await _only_agent(engine)
    assert [i["kind"] for i in items] == ["tool_call"]
    assert rec.live and rec.activity == "Bash: sleep 900"


@pytest.mark.asyncio
async def test_drain_waits_for_the_person_past_the_idle_timeout(caplog):
    """An autonomous turn paused on ``ExitPlanMode`` is silent because the
    runtime waits for Nerve's answer, which waits for the person: an idle
    timeout keeps the turn open (not interrupted, not closed) and the drain
    waits on. Once the decision is made, silence past the idle timeout
    interrupts the turn as before."""
    engine = _make_engine()
    engine.config.agent.cli_idle_timeout_seconds = 0.05
    plan = {"plan": "Ship it"}
    stream = _FakeStream([_main_tool_use("ExitPlanMode", "toolu_plan", plan)])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)
    engine.sessions = _RecordingSessions()

    asked: list[dict] = []

    async def _hub_broadcast(_channel, msg):
        asked.append(msg)

    hub = InteractiveToolHandler("s1", _hub_broadcast, interactive_capable=True)
    register_handler("s1", hub)
    decision = asyncio.create_task(
        ClaudeToolPermissions(hub).can_use_tool("ExitPlanMode", plan, None),
    )
    drain = None
    try:
        with patch("nerve.agent.engine.broadcaster") as bc, \
                caplog.at_level(logging.INFO, logger="nerve.agent.interactive"):
            _recording_broadcaster(bc, [])
            drain = asyncio.create_task(engine._drain_pending_messages(
                "s1", client, "web", None, manage_framing=True,
            ))
            await asyncio.sleep(0.4)  # eight idle timeouts, decision pending
            assert hub.has_pending
            assert not drain.done()
            assert finalized == []
            (interaction,) = [m for m in asked if m["type"] == "interaction"]
            assert hub.resolve(interaction["interaction_id"], {})
            await decision  # the pause is over: nothing pending any more
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(drain, timeout=2)
    finally:
        unregister_handler("s1")
        for task in (decision, drain):
            if task is not None:
                task.cancel()

    (st,) = finalized
    assert (st.status, st.error) == ("interrupted", "runtime went silent")
    assert st.full_response_text == "[Background turn interrupted: runtime went silent]"
    assert [
        r.getMessage() for r in caplog.records
        if "idle timeout deferred" in r.getMessage()
    ] == [
        "idle timeout deferred for session s1: waiting for the person "
        "(plan_exit pending)",
    ]


class _EventClient:
    """An idle-stream client fed normalized events directly (for shapes the
    Claude translation never produces on its own)."""

    def __init__(self, batches: list[list]):
        self._batches = list(batches)

    def try_receive_idle_events(self):
        return self._batches.pop(0) if self._batches else None

    async def receive_idle_events(self, timeout):
        if self._batches:
            return self._batches.pop(0)
        await asyncio.sleep(timeout if timeout is not None else 3600)
        raise asyncio.TimeoutError


@pytest.mark.asyncio
async def test_lone_subagent_started_opens_no_turn():
    """``SubagentStarted`` is not turn content: on its own (no spawning
    ToolUse in the same batch) it frames nothing."""
    engine = _make_engine()
    client = _EventClient([[ev.SubagentStarted(
        tool_use_id="toolu_agent", subagent_type="general-purpose",
        description="build-api",
    )]])
    finalized = _patch_finalize(engine)
    engine.sessions = _RecordingSessions()

    events: list[dict] = []
    with patch("nerve.agent.engine.broadcaster") as bc:
        _recording_broadcaster(bc, events)
        turns = await engine._drain_pending_messages(
            "s1", client, "web", None, manage_framing=True,
        )

    assert turns == 0
    assert finalized == []
    assert events == []
    assert engine.sessions.calls == []


@pytest.mark.asyncio
async def test_auto_marker_carries_triggers():
    """The autonomous turn's leading marker and its auto_turn event name what
    woke the orchestrator (agents settled since its last turn)."""
    engine = _make_engine()
    triggers = [{
        "agent_id": "ag_3f9a1c2b7d4e", "label": "build-chart",
        "kind": "agent", "status": "completed",
    }]
    engine.agents.take_turn_triggers = lambda sid: (list(triggers), 2)
    stream = _FakeStream([
        _sys_msg("init", cwd="/tmp"),
        _assistant_text("build-chart passed."),
        _result_msg(),
    ])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)
    engine.sessions = _RecordingSessions()

    events: list[dict] = []
    with patch("nerve.agent.engine.broadcaster") as bc:
        _recording_broadcaster(bc, events)
        await engine._drain_pending_messages(
            "s1", client, "web", None, manage_framing=True,
            first_content_timeout=5.0,
        )

    assert finalized[0].ordered_blocks[0] == {
        "type": "auto", "triggers": triggers, "more": 2,
    }
    (auto_turn,) = [e for e in events if e.get("type") == "auto_turn"]
    assert auto_turn == {
        "type": "auto_turn", "session_id": "s1", "triggers": triggers, "more": 2,
    }


@pytest.mark.asyncio
async def test_auto_marker_without_triggers_stays_bare():
    engine = _make_engine()
    stream = _FakeStream([_assistant_text("hello"), _result_msg()])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)

    events: list[dict] = []
    with patch("nerve.agent.engine.broadcaster") as bc:
        _recording_broadcaster(bc, events)
        await engine._drain_pending_messages("s1", client, "web", None)

    assert finalized[0].ordered_blocks[0] == {"type": "auto"}
    (auto_turn,) = [e for e in events if e.get("type") == "auto_turn"]
    assert auto_turn == {"type": "auto_turn", "session_id": "s1"}


@pytest.mark.asyncio
async def test_usage_only_autonomous_turn_not_persisted():
    """A turn that produced nothing visible but usage (init → result) records
    its usage without storing an empty "Background continuation" message."""
    engine = _make_engine()
    stream = _FakeStream([_sys_msg("init", cwd="/tmp"), _result_msg()])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)
    engine.sessions = _RecordingSessions()

    with patch("nerve.agent.engine.broadcaster") as bc:
        _recording_broadcaster(bc, [])
        turns = await engine._drain_pending_messages(
            "s1", client, "web", None, manage_framing=True,
            first_content_timeout=5.0,
        )

    assert turns == 1
    (st,) = finalized
    assert st.last_usage == {"input_tokens": 5, "output_tokens": 7}
    assert st.recorded_persist_message is False
    assert st.recorded_bump_updated_at is False


@pytest.mark.asyncio
@pytest.mark.parametrize("subtype, is_error", [("success", False), ("error_during_execution", True)])
async def test_empty_autonomous_turn_is_never_framed(subtype, is_error):
    """init then a zero-token result (a resumed runtime settling what the
    last process left, a turn Stop all interrupted at once): no session
    running, no auto_turn, nothing finalized. A failure still frames (it has
    an error to show)."""
    engine = _make_engine()
    result = {**_result_msg(), "subtype": subtype, "is_error": is_error,
              "usage": {"input_tokens": 0, "output_tokens": 0}}
    if is_error:
        result["errors"] = ["boom"]
    stream = _FakeStream([_sys_msg("init", cwd="/tmp"), result])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)
    engine.sessions = _RecordingSessions()
    events: list[dict] = []
    with patch("nerve.agent.engine.broadcaster") as bc:
        _recording_broadcaster(bc, events)
        turns = await engine._drain_pending_messages(
            "s1", client, "web", None, manage_framing=True,
            first_content_timeout=5.0,
        )
    if is_error:
        assert turns == 1 and "mark_running" in engine.sessions.names()
    else:
        assert turns == 0 and finalized == []
        assert "mark_running" not in engine.sessions.names()
        assert not [e for e in events if e.get("type") == "auto_turn"]


@pytest.mark.asyncio
async def test_visible_autonomous_turn_is_persisted():
    engine = _make_engine()
    stream = _FakeStream([_assistant_text("visible"), _result_msg()])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)
    with patch("nerve.agent.engine.broadcaster") as bc:
        _recording_broadcaster(bc, [])
        await engine._drain_pending_messages("s1", client, "web", None)
    assert finalized[0].recorded_persist_message is True


@pytest.mark.asyncio
@pytest.mark.parametrize("shutting_down, note", [
    (True, "[Interrupted: gateway shutdown]"),
    (False, "[Stopped by user]"),
])
async def test_shutdown_cancel_wording(shutting_down, note):
    """A drain cancelled mid-turn persists its partial turn; when the gateway
    is shutting down nobody stopped it, and the note says so."""
    engine = _make_engine()
    engine._shutting_down = shutting_down
    stream = _FakeStream([_assistant_text("halfway there")])
    client = _fake_client(stream)
    _patch_finalize(engine)
    sessions = _RecordingSessions()
    engine.sessions = sessions

    events: list[dict] = []
    with patch("nerve.agent.engine.broadcaster") as bc:
        _recording_broadcaster(bc, events)
        drain = asyncio.create_task(engine._drain_pending_messages(
            "s1", client, "web", None, manage_framing=True,
        ))
        for _ in range(50):
            if "auto_turn" in _types(events):
                break
            await asyncio.sleep(0.01)
        drain.cancel()
        with pytest.raises(asyncio.CancelledError):
            await drain

    (msg,) = sessions.messages
    assert msg["role"] == "assistant"
    assert msg["content"] == f"halfway there\n\n{note}"
    assert "stopped" in _types(events)
    # Framing is unwound: the Stop target is cleared, the session not running.
    assert sessions.names()[-2:] == ["clear_turn_task", "mark_not_running"]


@pytest.mark.asyncio
async def test_pre_query_drain_never_frames_session():
    """run()'s own pre-query drain (manage_framing=False) neither marks the
    session running nor claims the Stop target, even when it processes an
    autonomous turn."""
    engine = _make_engine()
    stream = _FakeStream([_assistant_text("late turn"), _result_msg()])
    client = _fake_client(stream)
    finalized = _patch_finalize(engine)
    sessions = _RecordingSessions()
    engine.sessions = sessions
    with patch("nerve.agent.engine.broadcaster") as bc:
        _recording_broadcaster(bc, [])
        turns = await engine._drain_pending_messages(
            "s1", client, "web", None, first_content_timeout=3.0,
        )
    assert turns == 1 and len(finalized) == 1
    assert sessions.calls == []


def test_translate_sidechain_assistant_emits_usage_and_keeps_model():
    """Sub-agent model calls keep their model and usage (dropped before) as a
    SubagentUsage; the main-chain ModelObserved stays gated out."""
    from claude_agent_sdk import AssistantMessage, TextBlock

    msg = AssistantMessage(
        content=[TextBlock(text="sub")], model="claude-sub",
        parent_tool_use_id="toolu_x", usage={"input_tokens": 2, "output_tokens": 3},
        message_id="msg_1",
    )
    events = translate_message(msg)
    assert [type(e).__name__ for e in events] == ["TextDelta", "SubagentUsage"]
    usage = events[1]
    assert usage == ev.SubagentUsage(
        parent_tool_use_id="toolu_x", message_id="msg_1", model="claude-sub",
        usage={"input_tokens": 2, "output_tokens": 3},
    )
    # Main chain: no SubagentUsage, ModelObserved as before.
    main = AssistantMessage(
        content=[TextBlock(text="main")], model="claude-main",
        usage={"input_tokens": 1},
    )
    assert [type(e).__name__ for e in translate_message(main)] == [
        "ModelObserved", "TextDelta",
    ]


def test_translate_sidechain_usage_falls_back_to_uuid():
    from claude_agent_sdk import AssistantMessage

    msg = AssistantMessage(
        content=[], model="claude-sub", parent_tool_use_id="toolu_x",
        usage={"output_tokens": 1}, uuid="uuid-9",
    )
    (usage,) = translate_message(msg)
    assert usage.message_id == "uuid-9"


def test_translate_tool_result_carries_structured_output():
    from claude_agent_sdk import ToolResultBlock, UserMessage

    receipt = {"status": "async_launched", "agentId": "a1b2c3d4e5f6a7b8c"}
    one = UserMessage(
        content=[ToolResultBlock(tool_use_id="toolu_a", content="launched")],
        tool_use_result=receipt,
    )
    (result,) = translate_message(one)
    assert result.structured == receipt
    # With two results the structured output cannot be attributed.
    two = UserMessage(
        content=[
            ToolResultBlock(tool_use_id="toolu_a", content="a"),
            ToolResultBlock(tool_use_id="toolu_b", content="b"),
        ],
        tool_use_result=receipt,
    )
    assert [r.structured for r in translate_message(two)] == [None, None]


def test_translate_nested_spawn_carries_parent():
    from claude_agent_sdk import AssistantMessage, ToolUseBlock

    nested = AssistantMessage(
        content=[ToolUseBlock(
            id="toolu_child", name="Agent",
            input={"description": "child", "subagent_type": "general-purpose"},
        )],
        model="claude-sub", parent_tool_use_id="toolu_parent",
    )
    events = translate_message(nested)
    started = [e for e in events if isinstance(e, ev.SubagentStarted)]
    assert started[0].parent_tool_use_id == "toolu_parent"
    top = AssistantMessage(
        content=[ToolUseBlock(id="toolu_top", name="Agent", input={})],
        model="claude-main",
    )
    started = [e for e in translate_message(top) if isinstance(e, ev.SubagentStarted)]
    assert started[0].parent_tool_use_id is None


@pytest.mark.asyncio
async def test_sidechain_tool_result_keeps_shared_side_effects():
    """A sub-agent's file edit still produces file_changed (and plan /
    MCP side effects) although it is no longer part of any turn."""
    calls: list[tuple] = []

    async def _side_effects(sid, tool_use_id, is_error, content, log):
        calls.append((sid, tool_use_id, is_error, content, log))

    reg = AgentRegistry.for_tests(side_effects=_side_effects)
    await reg.ingest_sidechain("s1", ev.ToolUse(
        tool_use_id="toolu_edit", name="Edit",
        input={"file_path": "/tmp/app.py", "old_string": "a" * 5000},
        parent_tool_use_id="toolu_agent",
    ))
    await reg.ingest_sidechain("s1", ev.ToolResult(
        tool_use_id="toolu_edit", content=[{"type": "text", "text": "ok"}],
        parent_tool_use_id="toolu_agent",
    ))
    (call,) = calls
    sid, tool_use_id, is_error, content, log = call
    assert (sid, tool_use_id, is_error) == ("s1", "toolu_edit", False)
    assert content == '[{"type": "text", "text": "ok"}]'
    # Only the keys the side effects read travel (bounded memory).
    assert log == [{
        "tool": "Edit", "tool_use_id": "toolu_edit",
        "input": {"file_path": "/tmp/app.py"},
    }]
