"""The smaller agent features: the orchestrator's reminder about
agents that ended outside its control, interactions that name the agent that
asked, and Codex collab agents as read-only records."""

from __future__ import annotations

import asyncio
import json

import pytest

from nerve.agent.backends import events as ev
from nerve.agent.engine import AgentEngine, _TurnState
from nerve.agent.interactive import InteractiveToolHandler
from nerve.config import NerveConfig

SID = "s-1234"


def _engine(db, tmp_path) -> AgentEngine:
    workspace = tmp_path / "ws"
    workspace.mkdir(parents=True, exist_ok=True)
    cfg = NerveConfig.from_dict({"workspace": str(workspace), "agent": {"backend": "claude"}})
    return AgentEngine(cfg, db)


class _Client:
    native_session_id = "native-1"
    model = "claude-test"

    def __init__(self) -> None:
        self.prompts: list[str] = []

    async def start_turn(self, turn) -> None:
        self.prompts.append(turn.text)

    async def receive_turn(self):
        yield ev.TurnCompleted(native_session_id="native-1")

    async def interrupt(self, *, cancel_queued: bool = False) -> None:
        pass

    def is_alive(self) -> bool:
        return True

    async def disconnect(self, timeout: float = 5.0) -> None:
        pass

    def try_receive_idle_events(self):
        return None

    def buffer_used(self) -> int:
        return 0


# --------------------------------------------------------------------------- #
#  Orchestrator notes                                                          #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_next_turn_hears_about_agents_a_restart_ended(db, tmp_path):
    engine = _engine(db, tmp_path)
    await engine.sessions.get_or_create(SID, source="web", backend="claude", actor=None)
    client = _Client()

    async def _get(sid, source, model, **_kw):
        engine.sessions.set_client(sid, client)
        return client

    engine._get_or_create_client = _get
    await engine.agents.on_system_event(SID, ev.SystemEvent("task_started", {
        "task_id": "a1b2c3d4e5f6a7b8c", "task_type": "local_agent",
        "description": "build-api", "is_backgrounded": True,
    }))
    await engine.agents.on_system_event(SID, ev.SystemEvent("task_started", {
        "task_id": "a9b9c9d9e9f9a9b9c", "task_type": "local_agent",
        "description": "stopped-by-me", "is_backgrounded": True,
    }))
    rec = next(r for r in engine.agents._sessions[SID].records.values()
               if r.task_id == "a1b2c3d4e5f6a7b8c")
    rec.activity = "Bash: pytest -q"
    # The user stopped one (the CLI reports that to the model itself) ...
    await engine.agents.on_system_event(SID, ev.SystemEvent("task_updated", {
        "task_id": "a9b9c9d9e9f9a9b9c", "patch": {"status": "killed"},
    }))
    # ... and the runtime went away under the other.
    engine.agents.on_client_gone(SID, "gateway_restart")

    await engine.run(SID, "what happened?", actor=None)
    await engine.run(SID, "and now?", actor=None)

    first, second = client.prompts
    assert first.startswith("what happened?")
    assert "<system-reminder>Background work ended outside your control" in first
    assert "- build-api (a1b2c3d4e5f6a7b8c): interrupted by a gateway restart at" in first
    assert "last: Bash: pytest -q" in first
    assert "stopped-by-me" not in first
    assert "Relaunch them if still needed.</system-reminder>" in first
    # Once only, and never persisted.
    assert "Background work ended" not in second
    rows = await db.get_messages(SID)
    assert all("Background work ended" not in (r["content"] or "") for r in rows)
    assert rec.orchestrator_notified_at is not None


# --------------------------------------------------------------------------- #
#  Interactions that name their agent                                          #
# --------------------------------------------------------------------------- #


async def _hub(engine: AgentEngine, sent: list[dict]) -> InteractiveToolHandler:
    async def _broadcast(_sid, message):
        sent.append(message)

    hub = InteractiveToolHandler(SID, _broadcast)
    engine._wire_interaction_hooks(SID, hub)
    return hub


@pytest.mark.asyncio
async def test_an_interaction_names_the_agent_that_asked(db, tmp_path):
    engine = _engine(db, tmp_path)
    await engine.agents.on_system_event(SID, ev.SystemEvent("task_started", {
        "task_id": "a1b2c3d4e5f6a7b8c", "task_type": "local_agent",
        "description": "build-api", "is_backgrounded": False,
    }))
    rec = next(iter(engine.agents._sessions[SID].records.values()))
    sent: list[dict] = []
    hub = await _hub(engine, sent)

    pause = asyncio.create_task(hub.request_interaction(
        "AskUserQuestion", {"questions": []}, agent_task_id="a1b2c3d4e5f6a7b8c",
    ))
    for _ in range(100):
        if any(m.get("type") == "interaction" for m in sent):
            break
        await asyncio.sleep(0.01)
    (asked,) = [m for m in sent if m.get("type") == "interaction"]
    assert asked["agent"] == {
        "agent_id": rec.id, "task_id": "a1b2c3d4e5f6a7b8c", "label": "build-api",
    }
    assert engine.agents.to_view(rec)["attention"] == "needs_input"

    hub.resolve(asked["interaction_id"], {"answer": "yes"})
    outcome = await asyncio.wait_for(pause, timeout=5)
    assert outcome.result == {"answer": "yes"}
    assert engine.agents.to_view(rec)["attention"] is None


@pytest.mark.asyncio
async def test_the_orchestrators_interaction_names_no_agent(db, tmp_path):
    engine = _engine(db, tmp_path)
    sent: list[dict] = []
    hub = await _hub(engine, sent)
    pause = asyncio.create_task(hub.request_interaction("AskUserQuestion", {}))
    for _ in range(100):
        if any(m.get("type") == "interaction" for m in sent):
            break
        await asyncio.sleep(0.01)
    (asked,) = [m for m in sent if m.get("type") == "interaction"]
    assert asked["agent"] is None
    hub.cancel_all()
    await asyncio.wait_for(pause, timeout=5)


@pytest.mark.asyncio
async def test_an_unknown_agent_still_gets_named_by_its_task(db, tmp_path):
    engine = _engine(db, tmp_path)
    sent: list[dict] = []
    hub = await _hub(engine, sent)
    pause = asyncio.create_task(hub.request_interaction(
        "AskUserQuestion", {}, agent_task_id="aunknown00001",
    ))
    for _ in range(100):
        if any(m.get("type") == "interaction" for m in sent):
            break
        await asyncio.sleep(0.01)
    (asked,) = [m for m in sent if m.get("type") == "interaction"]
    assert asked["agent"] == {"agent_id": None, "task_id": "aunknown00001", "label": "aunknown00001"}
    hub.cancel_all()
    await asyncio.wait_for(pause, timeout=5)


@pytest.mark.asyncio
async def test_claude_permissions_pass_the_asking_agent():
    from claude_agent_sdk.types import ToolPermissionContext

    from nerve.agent.backends.claude import ClaudeToolPermissions
    from nerve.agent.interactive import InteractionOutcome

    calls: list[tuple] = []

    class _Hub:
        session_id = SID
        interactive_capable = True
        snapshot_fn = None

        async def request_interaction(self, tool, payload, timeout=None, **kwargs):
            calls.append((tool, kwargs))
            return InteractionOutcome(result={"a": 1})

    perms = ClaudeToolPermissions(_Hub())
    await perms.can_use_tool("AskUserQuestion", {}, ToolPermissionContext(agent_id="a1b2c3d4e5f6a7b8c"))
    await perms.can_use_tool("AskUserQuestion", {}, ToolPermissionContext())
    assert calls == [
        ("AskUserQuestion", {"agent_task_id": "a1b2c3d4e5f6a7b8c"}),
        ("AskUserQuestion", {}),
    ]


# --------------------------------------------------------------------------- #
#  Codex collab agents                                                         #
# --------------------------------------------------------------------------- #


def _codex_client(tmp_path):
    from nerve.agent.backends import BackendDeps, SessionSpec
    from nerve.agent.backends.codex import CodexBackend
    from nerve.agent.backends.codex.backend import CodexClient

    workspace = tmp_path / "codex-ws"
    workspace.mkdir()
    cfg = NerveConfig.from_dict({
        "workspace": str(workspace), "codex": {"home_dir": str(tmp_path / "codex-home")},
    })
    backend = CodexBackend(BackendDeps(
        config=lambda: cfg, db=None, registry=None, tool_ctx_factory=lambda sid: None,
        external_mcp_servers=lambda: [], gateway_port=lambda: None,
        mint_session_token=lambda sid: "",
    ))
    return CodexClient(backend, SessionSpec(
        session_id=SID, source="web", model=cfg.codex.model, effort="high",
        system_prompt="", cwd=str(workspace),
    ))


@pytest.mark.asyncio
async def test_a_codex_collab_agent_spawns_like_an_agent_call(tmp_path):
    client = _codex_client(tmp_path)
    started = await client._map_item_started({
        "id": "item-7", "type": "collabAgentToolCall", "tool": "spawn_agent",
        "prompt": "Review the parser", "model": "model-x",
    })
    tool_use, subagent = started
    assert isinstance(tool_use, ev.ToolUse) and isinstance(subagent, ev.SubagentStarted)
    assert (tool_use.tool_use_id, tool_use.name) == ("item-7", "Agent")
    assert tool_use.input == {
        "description": "Review the parser", "prompt": "Review the parser",
        "model": "model-x", "subagent_type": "spawn_agent", "run_in_background": False,
    }
    assert subagent.tool_use_id == "item-7"
    (result,) = await client._map_item_completed({
        "id": "item-7", "type": "collabAgentToolCall", "status": "completed",
        "receiverThreadIds": ["t-1"], "agentsStates": {"t-1": "done"},
    })
    assert result.tool_use_id == "item-7" and result.is_error is False


@pytest.mark.asyncio
async def test_codex_collab_agents_without_an_id_get_distinct_ids(tmp_path):
    client = _codex_client(tmp_path)
    a = (await client._map_item_started({"type": "collabAgentToolCall", "prompt": "one"}))[0]
    b = (await client._map_item_started({"type": "collabAgentToolCall", "prompt": "two"}))[0]
    assert a.tool_use_id != b.tool_use_id
    assert a.tool_use_id.startswith("codex-agent-") and b.tool_use_id.startswith("codex-agent-")
    # Completions pair with their starts, in order.
    (ra,) = await client._map_item_completed({"type": "collabAgentToolCall", "status": "completed"})
    (rb,) = await client._map_item_completed({"type": "collabAgentToolCall", "status": "failed"})
    assert (ra.tool_use_id, rb.tool_use_id) == (a.tool_use_id, b.tool_use_id)
    assert rb.is_error is True


@pytest.mark.asyncio
async def test_a_codex_collab_agent_becomes_a_read_only_record(db, tmp_path):
    engine = _engine(db, tmp_path)
    engine.agents.on_client_connected(SID, 1, backend="codex")
    client = _codex_client(tmp_path)
    st = _TurnState()
    started = await client._map_item_started({
        "id": "item-7", "type": "collabAgentToolCall", "tool": "spawn_agent",
        "prompt": "Review the parser",
    })
    for event in started:
        await engine._process_agent_event(SID, event, st)
    rec = next(iter(engine.agents._sessions[SID].records.values()))
    assert (rec.backend, rec.kind, rec.status, rec.background) == ("codex", "agent", "running", False)
    view = engine.agents.to_view(rec)
    assert view["controls"]["stop"] is False         # no per-agent stop on Codex
    assert view["controls"]["transcript"] is False
    # The spawn persists as a tool_call block of the turn.
    assert [b["tool"] for b in st.ordered_blocks if b["type"] == "tool_call"] == ["Agent"]

    for event in await client._map_item_completed({
        "id": "item-7", "type": "collabAgentToolCall", "status": "completed",
        "receiverThreadIds": ["t-1"], "agentsStates": {},
    }):
        await engine._process_agent_event(SID, event, st)
    assert (rec.status, rec.end_reason) == ("completed", "completed")
    assert json.loads(rec.result_text)["receivers"] == ["t-1"]


@pytest.mark.asyncio
async def test_a_codex_collab_agent_without_a_result_ends_with_the_turn(db, tmp_path):
    """Codex collab agents live inside the turn: one the turn ended without a
    result for is lost, not live forever (it would block archive, the idle
    reap and model switches)."""
    engine = _engine(db, tmp_path)
    await engine.sessions.get_or_create(SID, source="web", backend="codex", actor=None)
    engine.agents.on_client_connected(SID, 1, backend="codex")
    codex = _codex_client(tmp_path)
    started = await codex._map_item_started({
        "id": "item-9", "type": "collabAgentToolCall", "prompt": "Review",
    })

    class _Turn(_Client):
        async def receive_turn(self):
            for event in started:
                yield event
            yield ev.TurnCompleted(native_session_id="native-1")

    client = _Turn()

    async def _get(sid, source, model, **_kw):
        engine.sessions.set_client(sid, client)
        return client

    engine._get_or_create_client = _get
    # A Claude foreground agent is left alone (it can move to the background
    # mid-run without a new start frame).
    await engine.agents.on_system_event(SID, ev.SystemEvent("task_started", {
        "task_id": "a1b2c3d4e5f6a7b8c", "task_type": "local_agent", "is_backgrounded": False,
    }))
    claude_fg = next(iter(engine.agents._sessions[SID].records.values()))
    claude_fg.backend = "claude"

    await engine.run(SID, "go", actor=None)

    codex_rec = next(
        r for r in engine.agents._sessions[SID].records.values() if r.spawn_tool_use_id == "item-9"
    )
    assert (codex_rec.status, codex_rec.end_reason) == ("lost", "vanished")
    assert claude_fg.status == "running"
    assert engine.agents.has_live(SID)        # the Claude one is still live
