"""Scoped stop: the turn, one agent, all agents, force.

The red button stops the orchestrator's turn and spares background agents;
one agent stops through the runtime's per-task stop without touching the
turn or the run lock; stop-all and force are separate actions that say what
they end. A runtime that cannot spare background agents asks first instead
of killing them.
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from nerve.agent.agents.errors import AgentNotFound, NoRuntime, Unsupported
from nerve.agent.backends import events as ev
from nerve.agent.backends.base import TransportDiedError
from nerve.agent.engine import AgentEngine
from nerve.config import NerveConfig
from nerve.identity import attribution_for

SID = "s-1234"


class _Client:
    """A runtime whose turn runs until it is interrupted.

    ``calls`` is the ordered log of what the engine sent:
    ``("interrupt", cancel_queued)`` and ``("stop_task", task_id)``.
    """

    native_session_id = "native-1"
    model = "claude-test"

    def __init__(
        self, *, spares: bool = True, supports: bool = True,
        ends_on_interrupt: bool = True, stop_error: Exception | None = None,
    ) -> None:
        self._spares = spares
        self._supports = supports
        self._ends = ends_on_interrupt
        self._stop_error = stop_error
        self.calls: list[tuple] = []
        self.disconnected = False
        self.turns = 0
        self.turn_started = asyncio.Event()
        self._turn_end: asyncio.Event | None = None

    @property
    def interrupt_spares_background(self) -> bool:
        return self._spares

    @property
    def supports_task_stop(self) -> bool:
        return self._supports

    async def stop_task(self, task_id: str) -> None:
        self.calls.append(("stop_task", task_id))
        if self._stop_error is not None:
            raise self._stop_error

    async def interrupt(self, *, cancel_queued: bool = False) -> None:
        self.calls.append(("interrupt", cancel_queued))
        if self._ends and self._turn_end is not None:
            self._turn_end.set()

    def release(self) -> None:
        """End the current turn (test cleanup)."""
        if self._turn_end is not None:
            self._turn_end.set()

    async def wait_turns(self, n: int) -> None:
        async def _until():
            while self.turns < n:
                await asyncio.sleep(0.01)
        await asyncio.wait_for(_until(), timeout=5)

    async def connect(self) -> None:
        pass

    async def start_turn(self, turn) -> None:
        pass

    async def receive_turn(self):
        end = self._turn_end = asyncio.Event()
        self.turns += 1
        self.turn_started.set()
        await end.wait()
        yield ev.TurnCompleted(native_session_id="native-1", status="interrupted")

    def is_alive(self) -> bool:
        return True

    async def disconnect(self, timeout: float = 5.0) -> None:
        self.disconnected = True

    def try_receive_idle_events(self):
        return None

    def buffer_used(self) -> int:
        return 0


async def _engine(db, tmp_path, client: _Client | None = None) -> AgentEngine:
    workspace = tmp_path / "ws"
    workspace.mkdir(parents=True, exist_ok=True)
    cfg = NerveConfig.from_dict({"workspace": str(workspace), "agent": {"backend": "claude"}})
    engine = AgentEngine(cfg, db)
    engine._STOP_WAIT_SECONDS = 0.2
    engine._STOP_ALL_SETTLE_SECONDS = 0.2
    await engine.sessions.get_or_create(SID, title="t", source="web", backend="claude", actor=None)
    if client is not None:
        engine.sessions.set_client(SID, client)

        async def _get_client(sid, source, model, **_kw):
            engine.sessions.set_client(sid, client)
            return client

        engine._get_or_create_client = _get_client
    return engine


async def _start_turn(engine: AgentEngine, client: _Client) -> asyncio.Task:
    task = asyncio.create_task(engine.run(SID, "hello", source="web", actor=None))
    await asyncio.wait_for(client.turn_started.wait(), timeout=5)
    assert engine.sessions.turn_task(SID) is task
    return task


async def _agent(engine: AgentEngine, tool_use_id: str, task_id: str, *,
                 parent: str | None = None, background: bool = True,
                 task_type: str = "local_agent", tool: str = "Agent"):
    """A live agent: its spawning call (main or nested), then task_started."""
    call = ev.ToolUse(
        tool_use_id=tool_use_id, name=tool,
        input={"description": f"agent {task_id}", "prompt": "work",
               **({"run_in_background": background} if tool == "Agent" else {}),
               **({"command": "sleep 100", "run_in_background": True} if tool == "Bash" else {})},
        parent_tool_use_id=parent,
    )
    if parent is None:
        await engine.agents.note_main_tool_use(SID, call)
    else:
        await engine.agents.ingest_sidechain(SID, call)
    await engine.agents.on_system_event(SID, ev.SystemEvent("task_started", {
        "task_id": task_id, "tool_use_id": tool_use_id, "task_type": task_type,
        "is_backgrounded": background,
    }))
    rec = next(r for r in engine.agents._sessions[SID].records.values() if r.task_id == task_id)
    assert rec.status == "running"
    return rec


async def _events(db, event_type: str) -> list[dict]:
    rows = await db.get_session_events(SID, limit=200)
    return [r for r in sorted(rows, key=lambda r: r["id"]) if r["event_type"] == event_type]


async def _finish(task: asyncio.Task) -> None:
    if not task.done():
        task.cancel()
    with contextlib.suppress(BaseException):
        await task


# --------------------------------------------------------------------------- #
#  stop_agent                                                                  #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_stop_agent_takes_no_lock_runs_no_listener_and_cascades(db, tmp_path):
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    listened: list[str] = []

    async def _listener(sid, attribution):
        listened.append(sid)

    engine.add_stop_listener(_listener)
    parent = await _agent(engine, "toolu_P", "aparent0001")
    child = await _agent(engine, "toolu_C", "achild00001", parent="toolu_P")
    grandchild = await _agent(engine, "toolu_G", "agrand00001", parent="toolu_C")
    assert (child.parent_agent_id, child.depth) == (parent.id, 2)
    actor_id = db.system_actor_id

    # Another task holds the run lock (a turn is running): the stop never
    # waits for it.
    lock = engine._session_locks.setdefault(SID, asyncio.Lock())
    await lock.acquire()
    try:
        view, stopping = await asyncio.wait_for(
            engine.stop_agent(SID, parent.id, attribution=attribution_for(db.system_actor)),
            timeout=2,
        )
    finally:
        lock.release()

    # Deepest first, then the record itself.
    assert stopping == [grandchild.id, child.id, parent.id]
    assert client.calls == [
        ("stop_task", "agrand00001"), ("stop_task", "achild00001"),
        ("stop_task", "aparent0001"),
    ]
    assert listened == []  # a per-agent stop is no session stop
    assert view["id"] == parent.id and view["status"] == "stopping"
    for rec in (parent, child, grandchild):
        assert rec.status == "stopping"
        assert rec.stop_scope == "user"
        assert rec.stopped_by_actor_id == actor_id
    (event,) = await _events(db, "agent_stop_requested")
    assert event["actor_id"] == actor_id
    assert event["details"] == {
        "agent_ids": stopping,
        "task_ids": ["agrand00001", "achild00001", "aparent0001"],
        "cascade": True,
    }

    # The runtime confirms: the records settle as stopped by the user.
    for task_id in ("agrand00001", "achild00001", "aparent0001"):
        await engine.agents.on_system_event(SID, ev.SystemEvent(
            "task_updated", {"task_id": task_id, "patch": {"status": "killed"}},
        ))
    assert [(r.status, r.end_reason) for r in (parent, child, grandchild)] == [
        ("stopped", "user")] * 3
    assert not engine.agents.to_view(parent)["controls"]["message"]


@pytest.mark.asyncio
async def test_stop_agent_without_cascade_stops_only_the_record(db, tmp_path):
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    parent = await _agent(engine, "toolu_P", "aparent0001")
    child = await _agent(engine, "toolu_C", "achild00001", parent="toolu_P")
    _view, stopping = await engine.stop_agent(SID, parent.id, cascade=False)
    assert stopping == [parent.id]
    assert client.calls == [("stop_task", "aparent0001")]
    assert child.status == "running"


@pytest.mark.asyncio
async def test_stop_before_the_task_id_is_known_is_deferred(db, tmp_path):
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    await engine.agents.note_main_tool_use(SID, ev.ToolUse(
        tool_use_id="toolu_A", name="Agent",
        input={"description": "late", "prompt": "p"},
    ))
    rec = engine.agents._sessions[SID].records[engine.agents.agent_id_for_spawn(SID, "toolu_A")]
    assert (rec.status, rec.task_id) == ("starting", None)

    _view, stopping = await engine.stop_agent(SID, rec.id)
    assert stopping == [rec.id]
    assert client.calls == []          # nothing to address yet
    assert rec.stop_pending is True

    # The launch receipt binds the task id: the stop goes out now.
    await engine.agents.on_main_tool_result(SID, ev.ToolResult(
        tool_use_id="toolu_A", content="Async agent launched",
        structured={"status": "async_launched", "agentId": "alate0000001"},
    ))
    assert client.calls == [("stop_task", "alate0000001")]
    assert rec.status == "stopping"


@pytest.mark.asyncio
async def test_stopping_a_finished_agent_is_idempotent(db, tmp_path):
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    rec = await _agent(engine, "toolu_A", "adone0000001")
    await engine.agents.on_system_event(SID, ev.SystemEvent(
        "task_notification", {"task_id": "adone0000001", "status": "completed"},
    ))
    view, stopping = await engine.stop_agent(SID, rec.id)
    assert (view["status"], stopping, client.calls) == ("completed", [], [])


@pytest.mark.asyncio
async def test_stop_agent_errors(db, tmp_path):
    engine = await _engine(db, tmp_path)
    with pytest.raises(AgentNotFound) as missing:
        await engine.stop_agent(SID, "ag_000000000000")
    assert missing.value.detail()["code"] == "agent_not_found"

    # No runtime: the record is reconciled, and the caller is told.
    rec = await _agent(engine, "toolu_A", "aorphan00001")
    with pytest.raises(NoRuntime):
        await engine.stop_agent(SID, rec.id)
    assert (rec.status, rec.end_reason) == ("interrupted", "process_ended")

    # A runtime that cannot stop one task (Codex).
    engine.sessions.set_client(SID, _Client(supports=False))
    rec2 = await _agent(engine, "toolu_B", "acodex000001")
    with pytest.raises(Unsupported) as unsupported:
        await engine.stop_agent(SID, rec2.id)
    assert unsupported.value.status == 501
    assert rec2.status == "running"


@pytest.mark.asyncio
async def test_stop_agent_on_a_dead_transport_reconciles(db, tmp_path):
    client = _Client(stop_error=TransportDiedError("gone"))
    engine = await _engine(db, tmp_path, client)
    rec = await _agent(engine, "toolu_A", "adead0000001")
    with pytest.raises(NoRuntime):
        await engine.stop_agent(SID, rec.id)
    assert (rec.status, rec.end_reason) == ("interrupted", "process_ended")


@pytest.mark.asyncio
async def test_an_agent_of_another_session_names_its_owner(db, tmp_path):
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    rec = await _agent(engine, "toolu_A", "aowner000001")
    await engine.sessions.get_or_create("fork-1", source="web", actor=None)
    with pytest.raises(AgentNotFound) as other:
        await engine.stop_agent("fork-1", rec.id)
    assert other.value.detail() == {
        "code": "agent_in_other_session",
        "message": other.value.message,
        "owner_session_id": SID,
    }


@pytest.mark.asyncio
async def test_an_unconfirmed_stop_is_flagged(db, tmp_path):
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    now = [1000.0]
    engine.agents._clock = lambda: now[0]
    rec = await _agent(engine, "toolu_A", "aslow0000001")
    await engine.stop_agent(SID, rec.id)
    now[0] += 11
    engine.agents.sweep()
    assert rec.status == "stopping"
    assert engine.agents.to_view(rec)["attention"] == "stop_unconfirmed"


# --------------------------------------------------------------------------- #
#  stop_turn                                                                   #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_stop_turn_with_no_turn_sends_no_interrupt(db, tmp_path):
    client = _Client(spares=False)
    engine = await _engine(db, tmp_path, client)
    rec = await _agent(engine, "toolu_A", "abg000000001")
    result = await engine.stop_turn(SID)
    assert result == {"scope": "turn", "turn": "not_running", "would_end": [], "shell_count": 0}
    assert client.calls == []          # an interrupt would end the agent
    assert rec.status == "running"
    assert await _events(db, "stop_requested") == []
    assert await engine.stop_session(SID) is False


@pytest.mark.asyncio
async def test_stop_turn_spares_background_agents(db, tmp_path):
    client = _Client(spares=True)
    engine = await _engine(db, tmp_path, client)
    rec = await _agent(engine, "toolu_A", "abg000000001")
    turn = await _start_turn(engine, client)
    try:
        result = await engine.stop_turn(SID, attribution=attribution_for(db.system_actor))
        assert result == {"scope": "turn", "turn": "stopped", "would_end": [], "shell_count": 0}
        assert client.calls == [("interrupt", False)]
        await asyncio.wait_for(turn, timeout=5)
        assert rec.status == "running"           # spared
        assert engine.sessions.get_client(SID) is client
        (event,) = await _events(db, "stop_requested")
        assert event["details"] == {"scope": "turn", "outcome": "stopped", "live_agents": 1}
        assert event["actor_id"] == db.system_actor_id
    finally:
        await _finish(turn)


@pytest.mark.asyncio
async def test_degraded_runtime_asks_before_ending_agents(db, tmp_path):
    client = _Client(spares=False)
    engine = await _engine(db, tmp_path, client)
    agent = await _agent(engine, "toolu_A", "abg000000001")
    shell = await _agent(engine, "toolu_S", "bshell000001", task_type="local_bash", tool="Bash")
    assert shell.kind == "shell"
    turn = await _start_turn(engine, client)
    try:
        result = await engine.stop_turn(SID)
        assert result["scope"] == "turn"
        assert result["turn"] == "needs_confirmation"
        assert [b["id"] for b in result["would_end"]] == [agent.id]
        assert set(result["would_end"][0]) == {"id", "label", "kind", "status", "started_at", "tokens"}
        assert result["shell_count"] == 1
        assert client.calls == []                # nothing was sent
        assert not turn.done()
        assert await _events(db, "stop_requested") == []

        # Confirmed: the interrupt goes out; the kill it causes reads as a
        # stop with the turn.
        result = await engine.stop_turn(SID, confirm_kill_background=True)
        assert result["turn"] == "stopped"
        assert client.calls == [("interrupt", False)]
        await engine.agents.on_system_event(SID, ev.SystemEvent(
            "task_updated", {"task_id": "abg000000001", "patch": {"status": "killed"}},
        ))
        assert (agent.status, agent.end_reason) == ("stopped", "turn_stop")
    finally:
        await _finish(turn)


@pytest.mark.asyncio
async def test_a_wedged_turn_with_live_agents_is_not_cancelled(db, tmp_path):
    client = _Client(ends_on_interrupt=False)
    engine = await _engine(db, tmp_path, client)
    rec = await _agent(engine, "toolu_A", "abg000000001")
    turn = await _start_turn(engine, client)
    try:
        result = await engine.stop_turn(SID)
        assert result["turn"] == "not_stopped"
        assert [b["id"] for b in result["would_end"]] == [rec.id]
        assert result["shell_count"] == 0
        await asyncio.sleep(0.05)
        assert not turn.done()                   # no hard cancel
        assert engine.sessions.get_client(SID) is client  # no discard
        assert client.disconnected is False
        assert rec.status == "running"
        (event,) = await _events(db, "stop_requested")
        assert event["details"]["outcome"] == "not_stopped"
    finally:
        await _finish(turn)


@pytest.mark.asyncio
async def test_a_wedged_turn_without_agents_is_cancelled_as_before(db, tmp_path):
    client = _Client(ends_on_interrupt=False)
    engine = await _engine(db, tmp_path, client)
    turn = await _start_turn(engine, client)
    result = await engine.stop_turn(SID)
    assert result == {"scope": "turn", "turn": "killed", "would_end": [], "shell_count": 0}
    out = await asyncio.wait_for(turn, timeout=5)
    assert out.endswith("[Stopped by user]")
    assert engine.sessions.get_client(SID) is None   # its cancel path discarded it
    assert client.disconnected is True


@pytest.mark.asyncio
async def test_stop_targets_the_turn_not_a_run_queued_behind_it(db, tmp_path):
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    turn = await _start_turn(engine, client)
    queued = asyncio.create_task(engine.run(SID, "next", source="web", actor=None))
    engine.register_task(SID, queued)        # a legacy registration
    await asyncio.sleep(0.05)
    try:
        assert engine.sessions.turn_task(SID) is turn
        result = await engine.stop_turn(SID)
        assert result["turn"] == "stopped"
        await asyncio.wait_for(turn, timeout=5)
        assert not queued.cancelled()
        # The queued message runs next and is the turn now.
        await client.wait_turns(2)
        assert engine.sessions.turn_task(SID) is queued
    finally:
        client.release()
        await _finish(turn)
        await _finish(queued)


# --------------------------------------------------------------------------- #
#  stop_all and force_stop                                                     #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_stop_all_stops_tasks_then_interrupts_with_cancel_queued(db, tmp_path):
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    listened: list[str] = []

    async def _listener(sid, attribution):
        listened.append(sid)

    engine.add_stop_listener(_listener)
    parent = await _agent(engine, "toolu_P", "aparent0001")
    child = await _agent(engine, "toolu_C", "achild00001", parent="toolu_P")
    shell = await _agent(engine, "toolu_S", "bshell000001", task_type="local_bash", tool="Bash")
    turn = await _start_turn(engine, client)
    queued = asyncio.create_task(engine.run(SID, "next", source="web", actor=None))
    await asyncio.sleep(0.05)
    try:
        result = await engine.stop_all(SID, attribution=attribution_for(db.system_actor))
        assert result["scope"] == "all"
        assert result["turn"] == "stopped"
        assert set(result["stopping"]) == {parent.id, child.id, shell.id}
        assert result["stopping"].index(child.id) < result["stopping"].index(parent.id)
        assert result["unsupported"] == []
        # Every task stop goes out before the interrupt that drops what the
        # CLI queued.
        stops = [c for c in client.calls if c[0] == "stop_task"]
        assert {c[1] for c in stops} == {"aparent0001", "achild00001", "bshell000001"}
        assert client.calls[len(stops):] == [("interrupt", True)]
        assert listened == [SID]
        assert all(r.stop_scope == "stop_all" and r.status == "stopping"
                   for r in (parent, child, shell))
        # Nerve's queued message is untouched: it runs next.
        assert not queued.cancelled()
        await client.wait_turns(2)
        assert engine.sessions.turn_task(SID) is queued
        (event,) = await _events(db, "agents_stop_all_requested")
        assert event["details"]["include_shells"] is True
        assert set(event["details"]["agent_ids"]) == set(result["stopping"])
    finally:
        client.release()
        await _finish(turn)
        await _finish(queued)


@pytest.mark.asyncio
async def test_stop_all_drops_notifications_queued_after_its_interrupt(db, tmp_path):
    """The runtime queues a stopped agent's notification once the agent has
    ended, often after the interrupt: when the agents settled, the queue is
    dropped again (and an autonomous turn about them would end), but a turn
    someone started meanwhile is left alone."""
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    engine._STOP_ALL_SETTLE_SECONDS = 5.0
    a = await _agent(engine, "toolu_A", "aagent000001")
    await engine.stop_all(SID)
    assert client.calls == [("stop_task", "aagent000001"), ("interrupt", True)]
    await engine.agents.on_system_event(SID, ev.SystemEvent(
        "task_updated", {"task_id": "aagent000001", "patch": {"status": "killed"}},
    ))
    assert a.status == "stopped"
    await asyncio.wait_for(asyncio.gather(*engine._stop_followups), timeout=2)
    assert client.calls[-1] == ("interrupt", True) and len(client.calls) == 3

    # A turn of a new message runs by the time the agents settle: untouched.
    client.calls.clear()
    b = await _agent(engine, "toolu_B", "aagent000002")
    await engine.stop_all(SID)
    turn = await _start_turn(engine, client)
    try:
        await engine.agents.on_system_event(SID, ev.SystemEvent(
            "task_updated", {"task_id": "aagent000002", "patch": {"status": "killed"}},
        ))
        assert b.status == "stopped"
        await asyncio.wait_for(asyncio.gather(*engine._stop_followups), timeout=2)
        assert client.calls == [("stop_task", "aagent000002"), ("interrupt", True)]
        assert not turn.done()
    finally:
        client.release()
        await _finish(turn)


@pytest.mark.asyncio
async def test_stop_all_can_spare_shells(db, tmp_path):
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    agent = await _agent(engine, "toolu_A", "aagent000001")
    shell = await _agent(engine, "toolu_S", "bshell000001", task_type="local_bash", tool="Bash")
    result = await engine.stop_all(SID, include_shells=False)
    assert result == {"scope": "all", "turn": "not_running", "stopping": [agent.id], "unsupported": []}
    assert client.calls == [("stop_task", "aagent000001"), ("interrupt", True)]
    assert shell.status == "running"


@pytest.mark.asyncio
async def test_stop_all_on_a_runtime_without_task_stop(db, tmp_path):
    client = _Client(supports=False)
    engine = await _engine(db, tmp_path, client)
    rec = await _agent(engine, "toolu_A", "acodex000001")
    result = await engine.stop_all(SID)
    assert result["stopping"] == [] and result["unsupported"] == [rec.id]


@pytest.mark.asyncio
async def test_force_stop_cancels_the_turn_and_interrupts_every_agent(db, tmp_path):
    client = _Client(ends_on_interrupt=False)
    engine = await _engine(db, tmp_path, client)
    a = await _agent(engine, "toolu_A", "aagent000001")
    b = await _agent(engine, "toolu_S", "bshell000001", task_type="local_bash", tool="Bash")
    turn = await _start_turn(engine, client)
    result = await engine.force_stop(SID, attribution=attribution_for(db.system_actor))
    assert result["scope"] == "force"
    assert result["turn"] == "killed"
    assert set(result["ended"]) == {a.id, b.id}
    assert [(r.status, r.end_reason) for r in (a, b)] == [("interrupted", "force_stop")] * 2
    await asyncio.wait_for(asyncio.gather(turn, return_exceptions=True), timeout=5)
    assert engine.sessions.get_client(SID) is None
    assert client.disconnected is True
    (event,) = await _events(db, "force_stop_requested")
    assert set(event["details"]["ended_agent_ids"]) == {a.id, b.id}


@pytest.mark.asyncio
async def test_force_stop_without_a_turn_discards_the_runtime(db, tmp_path):
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    rec = await _agent(engine, "toolu_A", "aagent000001")
    result = await engine.force_stop(SID)
    assert result == {"scope": "force", "turn": "not_running", "ended": [rec.id]}
    assert (rec.status, rec.end_reason) == ("interrupted", "force_stop")
    assert engine.sessions.get_client(SID) is None
    assert client.disconnected is True


@pytest.mark.asyncio
async def test_the_workflow_stop_listener_still_runs_on_a_turn_stop(db, tmp_path):
    """A stop on a ``workflow:`` session is a kill (the listener CASes the run
    terminal first); stop_turn keeps calling the listeners."""
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    seen: list[tuple[str, object]] = []

    async def _listener(sid, attribution):
        seen.append((sid, attribution))

    engine.add_stop_listener(_listener)
    who = attribution_for(db.system_actor)
    await engine.stop_session(SID, attribution=who)
    await engine.force_stop(SID, attribution=who)
    assert seen == [(SID, who), (SID, who)]


@pytest.mark.asyncio
async def test_a_degraded_stop_that_asks_runs_no_stop_listener(db, tmp_path):
    """needs_confirmation means nothing was sent: not even the stop listeners
    (on a workflow: session the listener kills the run)."""
    client = _Client(spares=False)
    engine = await _engine(db, tmp_path, client)
    listened: list[str] = []

    async def _listener(sid, attribution):
        listened.append(sid)

    engine.add_stop_listener(_listener)
    await _agent(engine, "toolu_A", "abg000000001")
    turn = await _start_turn(engine, client)
    try:
        result = await engine.stop_turn(SID)
        assert result["turn"] == "needs_confirmation" and listened == []
        result = await engine.stop_turn(SID, confirm_kill_background=True)
        assert result["turn"] == "stopped" and listened == [SID]
    finally:
        await _finish(turn)


class _WedgedClient(_Client):
    """A runtime that never answers the interrupt (the SDK would wait 60 s)."""

    async def interrupt(self, *, cancel_queued: bool = False) -> None:
        self.calls.append(("interrupt", cancel_queued))
        await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_an_unanswered_interrupt_counts_against_the_stop_wait(db, tmp_path):
    client = _WedgedClient()
    engine = await _engine(db, tmp_path, client)
    rec = await _agent(engine, "toolu_A", "abg000000001")
    turn = await _start_turn(engine, client)
    loop = asyncio.get_running_loop()
    try:
        started = loop.time()
        result = await asyncio.wait_for(engine.stop_turn(SID), timeout=5)
        assert loop.time() - started < 1.0          # the 0.2 s wait, not a minute
        assert result["turn"] == "not_stopped"
        assert [b["id"] for b in result["would_end"]] == [rec.id]
        assert not turn.done()
    finally:
        client.release()
        await _finish(turn)


@pytest.mark.asyncio
async def test_stop_spares_a_queued_run_while_the_lock_changes_hands(db, tmp_path):
    """No turn task and the run lock momentarily free (released, the next
    waiter not resumed yet): a registered run that is still listed in the
    queue is not cancelled."""
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    release = asyncio.Event()

    async def _waiting_run():
        await release.wait()

    queued = asyncio.create_task(_waiting_run())
    engine.register_task(SID, queued)
    await asyncio.sleep(0)
    queues, _started, _msgs = engine._queue_state()

    class _Entry:
        task = queued

    queues.setdefault(SID, []).append(_Entry())
    try:
        result = await engine.stop_turn(SID)
        await asyncio.sleep(0)
        assert not queued.cancelled() and result["turn"] in ("not_running", "stopped")
    finally:
        queues.pop(SID, None)
        release.set()
        await _finish(queued)
