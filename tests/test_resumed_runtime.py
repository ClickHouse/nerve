"""A runtime that was recreated or exited between turns.

A resumed CLI settles the agents the previous process left behind as soon as
it starts (task notifications, then an empty result of its own). That result
must not end the user's first turn. A CLI that exits between turns is
reconciled at once: its agents end as interrupted and nothing keeps it.
"""

from __future__ import annotations

import asyncio

import pytest

from nerve.agent.backends import events as ev
from nerve.agent.engine import AgentEngine
from nerve.config import NerveConfig

SID = "s-resume"


def _usage(n: int) -> ev.NormalizedUsage:
    return ev.NormalizedUsage.from_anthropic({"input_tokens": n, "output_tokens": n})


class _Client:
    """A runtime that answers each receive_turn() with the next script."""

    native_session_id = "native-r"
    model = "claude-test"
    interrupt_spares_background = True
    supports_task_stop = True

    def __init__(self, scripts: list[list], *, alive: bool = True) -> None:
        self.scripts = list(scripts)
        self.alive = alive
        self.reads = 0
        self.disconnected = False

    async def start_turn(self, turn) -> None:
        pass

    async def receive_turn(self):
        self.reads += 1
        for event in self.scripts.pop(0):
            yield event
            if isinstance(event, ev.TurnCompleted):
                return

    async def interrupt(self, *, cancel_queued: bool = False) -> None:
        pass

    async def stop_task(self, task_id: str) -> None:
        pass

    def is_alive(self) -> bool:
        return self.alive

    async def disconnect(self, timeout: float = 5.0) -> None:
        self.disconnected = True

    def try_receive_idle_events(self):
        return None

    def buffer_used(self) -> int:
        return 0


async def _engine(db, tmp_path, client: _Client) -> AgentEngine:
    workspace = tmp_path / "ws"
    workspace.mkdir(parents=True, exist_ok=True)
    cfg = NerveConfig.from_dict({"workspace": str(workspace), "agent": {"backend": "claude"}})
    engine = AgentEngine(cfg, db)
    await engine.sessions.get_or_create(SID, title="t", source="web", backend="claude", actor=None)
    engine.sessions.set_client(SID, client)

    async def _get_client(sid, source, model, **_kw):
        engine.sessions.set_client(sid, client)
        return client

    engine._get_or_create_client = _get_client
    return engine


async def _assistant_rows(db) -> list[str]:
    rows = await db.get_messages(SID, limit=50)
    return [r["content"] for r in rows if r["role"] == "assistant"]


def _leftover() -> list:
    """What a resumed CLI sends before the user's turn: its own init, the
    agents the previous process left, settled, and an empty result."""
    return [
        ev.SystemEvent("init", {}),
        ev.SystemEvent("task_notification", {"task_id": "aold00000001", "status": "stopped"}),
        ev.TurnCompleted(native_session_id="native-r", usage=_usage(0)),
    ]


def _answer(text: str) -> list:
    return [
        ev.SystemEvent("init", {}),
        ev.ModelObserved(model="claude-test"),
        ev.TextDelta(text=text),
        ev.TurnCompleted(native_session_id="native-r", usage=_usage(10)),
    ]


@pytest.mark.asyncio
async def test_the_first_turn_skips_a_result_left_over_from_the_runtime(db, tmp_path):
    client = _Client([_leftover(), _answer("pong")])
    engine = await _engine(db, tmp_path, client)
    out = await engine.run(SID, "Reply with the single word pong.", source="web", actor=None)
    assert out == "pong" and client.reads == 2
    assert await _assistant_rows(db) == ["pong"]  # no empty reply stored


@pytest.mark.asyncio
async def test_a_failed_result_before_the_turn_still_ends_it(db, tmp_path):
    failed = ev.TurnCompleted(native_session_id="native-r", usage=_usage(0),
                              status="failed", error="API error (HTTP 529)")
    client = _Client([[failed], _answer("late")])
    engine = await _engine(db, tmp_path, client)
    out = await engine.run(SID, "hello", source="web", actor=None)
    assert "API error (HTTP 529)" in out and client.reads == 1


@pytest.mark.asyncio
async def test_an_empty_result_after_the_turn_began_ends_it(db, tmp_path):
    client = _Client([[ev.SystemEvent("init", {}), ev.TextDelta(text="hi"),
                       ev.TurnCompleted(native_session_id="native-r", usage=_usage(0))],
                      _answer("never read")])
    engine = await _engine(db, tmp_path, client)
    assert await engine.run(SID, "hello", source="web", actor=None) == "hi"
    assert client.reads == 1


@pytest.mark.asyncio
async def test_only_a_few_leftover_results_are_skipped(db, tmp_path):
    client = _Client([_leftover() for _ in range(4)] + [_answer("late")])
    engine = await _engine(db, tmp_path, client)
    await engine.run(SID, "hello", source="web", actor=None)
    assert client.reads == 4  # three skipped, the fourth ends the turn


@pytest.mark.asyncio
async def test_a_runtime_that_exits_between_turns_is_reaped(db, tmp_path):
    client = _Client([], alive=True)
    engine = await _engine(db, tmp_path, client)
    engine.agents.on_client_connected(SID, 1, backend="claude")
    await engine.agents.note_main_tool_use(SID, ev.ToolUse(
        tool_use_id="toolu_A", name="Agent", input={"description": "work", "prompt": "p"},
    ))
    await engine.agents.on_system_event(SID, ev.SystemEvent("task_started", {
        "task_id": "alive0000001", "tool_use_id": "toolu_A", "task_type": "local_agent",
        "is_backgrounded": True,
    }))
    rec = engine.agents.live(SID)[0]
    engine._IDLE_STREAM_POLL_SECONDS = 0.01
    watcher = asyncio.create_task(engine._idle_stream_watcher(SID, client, "web", None))
    await asyncio.sleep(0.05)
    assert not watcher.done()          # alive: the watcher keeps watching
    client.alive = False               # the CLI exits between turns
    await asyncio.wait_for(watcher, timeout=2)
    assert engine.sessions.get_client(SID) is None
    assert (rec.status, rec.end_reason) == ("interrupted", "process_ended")
    assert client.disconnected is True


@pytest.mark.asyncio
async def test_the_idle_sweep_does_not_keep_a_dead_runtime(db, tmp_path):
    client = _Client([], alive=False)
    engine = await _engine(db, tmp_path, client)
    engine.config.sessions.client_idle_timeout_minutes = 1
    await engine.agents.on_system_event(SID, ev.SystemEvent("task_started", {
        "task_id": "alive0000002", "task_type": "local_agent", "is_backgrounded": True,
    }))
    assert engine.agents.has_live(SID)
    engine.sessions.get_idle_client_ids = lambda _seconds: [SID]
    assert await engine.run_idle_client_sweep() == 1
    assert engine.sessions.get_client(SID) is None and not engine.agents.has_live(SID)
