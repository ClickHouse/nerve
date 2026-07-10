"""Tests for autonomous-turn draining — nerve.agent.engine.

The CLI continues sessions on its own (background task settles →
task_notification → full agent turn inside the subprocess).  These tests
cover the engine pieces that surface that activity:

- ``_handle_system_event`` — task lifecycle → background-task chips
- ``_drain_pending_messages`` — buffered autonomous turns → UI + DB
  (consumed through the client's idle-event API)
- ``ClaudeClient.buffer_used`` — non-destructive buffer probe
"""

import asyncio
import contextlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import anyio
import pytest

from claude_agent_sdk import ResultMessage, SystemMessage

from nerve.agent.backends.base import SessionSpec
from nerve.agent.backends.claude import ClaudeClient, translate_message
from nerve.agent.engine import AgentEngine, _TurnState


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
    engine._bg_task_registry = {}
    engine._workflows = {}
    engine._idle_watchers = {}
    engine._session_locks = {}
    engine._session_models = {}
    engine._observed_models = {}
    return engine


class _FakeStream:
    """Stand-in for the SDK's MemoryObjectReceiveStream."""

    def __init__(self, items: list[dict]):
        self._send, self._recv = anyio.create_memory_object_stream(
            max_buffer_size=100,
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
# _handle_system_event → background-task chips (SystemMessage fixtures fed
# through translate_message, as in the live pipeline)
# ---------------------------------------------------------------------------


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

        reg = engine._bg_task_registry["s1"]
        assert reg["t1"]["status"] == "running"
        assert reg["t1"]["label"] == "run tests"
        assert reg["t1"]["tool"] == "Bash"

        notified = SystemMessage(
            subtype="task_notification",
            data=_sys_msg(
                "task_notification", task_id="t1", status="completed",
            ),
        )
        await _handle_system_message(engine, "s1", notified)
        assert reg["t1"]["status"] == "done"

    assert len(events) == 2
    assert all(e["type"] == "background_tasks_update" for e in events)
    assert events[-1]["tasks"][0]["status"] == "done"


@pytest.mark.asyncio
async def test_task_failed_maps_to_failed_status():
    engine = _make_engine()
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        msg = SystemMessage(
            subtype="task_notification",
            data=_sys_msg("task_notification", task_id="t9", status="failed"),
        )
        await _handle_system_message(engine, "s1", msg)
    assert engine._bg_task_registry["s1"]["t9"]["status"] == "failed"


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
    assert engine._bg_task_registry["s1"]["t2"]["tool"] == "Agent"


@pytest.mark.asyncio
async def test_non_task_subtypes_ignored():
    engine = _make_engine()
    with patch("nerve.agent.engine.broadcaster") as bc:
        bc.broadcast = AsyncMock()
        await _handle_system_message(
            engine, "s1", SystemMessage(subtype="init", data={"type": "system"}),
        )
        bc.broadcast.assert_not_called()
    assert "s1" not in engine._bg_task_registry


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

    # Chip relabeled; workflow name captured from the CLI's workflow_name.
    assert engine._bg_task_registry["s1"]["wt"]["tool"] == "Workflow"
    # One progress broadcast per task message.
    assert len(wf_events) == 3
    tid, last = wf_events[-1]
    assert tid == "wf-tool"
    assert last["name"] == "verify-ui"
    assert last["status"] == "completed"
    assert last["agentCount"] == 1  # carried over from the progress snapshot
    # Terminal snapshot persisted onto the Workflow block.
    engine.db.merge_workflow_into_call.assert_awaited_once()


def test_prune_bg_tasks_drops_settled():
    engine = _make_engine()
    engine._bg_task_registry["s1"] = {
        "t1": {"task_id": "t1", "label": "a", "tool": "Bash", "status": "done"},
        "t2": {"task_id": "t2", "label": "b", "tool": "Bash", "status": "running"},
    }
    engine._prune_bg_tasks("s1")
    assert list(engine._bg_task_registry["s1"]) == ["t2"]

    engine._bg_task_registry["s2"] = {
        "t3": {"task_id": "t3", "label": "c", "tool": "Bash", "status": "failed"},
    }
    engine._prune_bg_tasks("s2")
    assert "s2" not in engine._bg_task_registry


# ---------------------------------------------------------------------------
# _drain_pending_messages
# ---------------------------------------------------------------------------


def _patch_finalize(engine: AgentEngine):
    """Replace _finalize_turn with a recorder (avoids DB plumbing)."""
    finalized: list[_TurnState] = []

    async def _record(session_id, st, channel):
        finalized.append(st)

    engine._finalize_turn = _record  # type: ignore[method-assign]
    return finalized


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
        engine.sessions = SimpleNamespace(
            mark_running=lambda sid: None,
            mark_not_running=lambda sid: None,
        )
        turns = await engine._drain_pending_messages(
            "s1", client, "web", None, manage_framing=True,
        )

    assert turns == 1
    assert len(finalized) == 1
    st = finalized[0]
    assert st.full_response_text == "Background job finished."
    # Leading marker block for the "background continuation" chip
    assert st.ordered_blocks[0] == {"type": "auto"}
    # Chips updated + auto_turn marker + session_running framing
    types = [e.get("type") for e in events]
    assert "background_tasks_update" in types
    assert "auto_turn" in types
    assert {"type": "session_running", "session_id": "s1",
            "is_running": True} in events
    assert {"type": "session_running", "session_id": "s1",
            "is_running": False} in events
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


def test_has_live_background_tasks():
    engine = _make_engine()
    engine._bg_task_registry = {
        "running": {"t1": {"status": "running"}},
        "settled": {"t2": {"status": "done"}},
        "mixed": {"t3": {"status": "done"}, "t4": {"status": "running"}},
    }
    assert engine._has_live_background_tasks("running") is True
    assert engine._has_live_background_tasks("settled") is False
    assert engine._has_live_background_tasks("mixed") is True
    assert engine._has_live_background_tasks("unknown") is False


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
    engine._bg_task_registry = {
        "busy": {"t1": {"task_id": "t1", "status": "running"}},
        "free": {"t2": {"task_id": "t2", "status": "done"}},
    }
    discarded: list[str] = []

    async def _fake_discard(sid, **_kw):
        discarded.append(sid)

    engine._discard_client = _fake_discard

    n = await engine.run_idle_client_sweep()

    assert discarded == ["free"]  # only the session with no live bg task
    assert n == 1
    assert "busy" in engine._bg_task_registry  # left intact for its watcher


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
    engine._bg_task_registry["s1"] = {
        "t1": {"task_id": "t1", "label": "mvn -pl client-v2 test",
               "tool": "Bash", "status": "running"},
    }
    assert engine._has_live_background_tasks("s1") is True
    # (the fake client's subprocess reports alive — see _fake_client)
    engine.sessions = SimpleNamespace(
        get_client=lambda _sid: client,
        is_running=lambda _sid: False,
        register_task=lambda _sid, _task: None,
        mark_running=lambda _sid: None,
        mark_not_running=lambda _sid: None,
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
