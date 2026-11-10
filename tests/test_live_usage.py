"""Live cost during a turn: each main-chain model call's usage, as it becomes
known, gives the UI a running estimate (``usage_update``).

Covers the stream-event translation (``message_start`` / ``message_delta``
with partial messages on; the frames' input side with them off), the engine's
tally and estimate in a scripted turn, the turn end (exact context, the new
billed total, the reset), and the broadcasts.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import pytest_asyncio
from claude_agent_sdk import AssistantMessage, TextBlock
from claude_agent_sdk.types import StreamEvent

from nerve.agent.backends import events as ev
from nerve.agent.backends.claude import ClaudeClient, _live_usage_on, call_usage_from_stream
from nerve.agent.engine import AgentEngine, _TurnState
from nerve.agent.streaming import StreamBroadcaster, broadcaster
from nerve.config import NerveConfig

SID = "s-live"
START = {"input_tokens": 2, "cache_read_input_tokens": 100_000,
         "cache_creation_input_tokens": 5_000, "output_tokens": 3}


def _stream(event: dict, parent: str | None = None) -> StreamEvent:
    return StreamEvent(uuid="u", session_id="native", event=event, parent_tool_use_id=parent)


def _start(mid: str, usage: dict | None = None, model: str = "claude-opus-5-5") -> StreamEvent:
    message = {"id": mid, "model": model, "usage": usage if usage is not None else START}
    return _stream({"type": "message_start", "message": message})


def _delta(output: int) -> StreamEvent:
    return _stream({"type": "message_delta", "delta": {"stop_reason": "end_turn"},
                    "usage": {**START, "output_tokens": output}})


# --------------------------------------------------------------------- #
#  Stream events -> CallUsage                                            #
# --------------------------------------------------------------------- #

def test_message_start_and_delta_give_the_calls_usage():
    open_call: list = [None, None]
    [started] = call_usage_from_stream(_start("msg_1"), open_call)
    assert (started.message_id, started.model, started.final) == ("msg_1", "claude-opus-5-5", False)
    assert started.usage == START
    [ended] = call_usage_from_stream(_delta(380), open_call)
    assert (ended.message_id, ended.final, ended.usage["output_tokens"]) == ("msg_1", True, 380)


@pytest.mark.parametrize("event", [
    {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "hi"}},
    {"type": "content_block_start", "index": 0, "content_block": {"type": "text"}},
    {"type": "content_block_stop", "index": 0},
    {"type": "message_stop"},
])
def test_other_stream_events_give_nothing(event):
    assert call_usage_from_stream(_stream(event), [None, None]) == []


def test_sub_agent_stream_events_give_nothing():
    assert call_usage_from_stream(_stream({"type": "message_start", "message": {"id": "m",
                                  "usage": START}}, parent="toolu_1"), [None, None]) == []


def _client(partial: bool | None) -> ClaudeClient:
    client = ClaudeClient.__new__(ClaudeClient)
    if partial is not None:
        client._options = SimpleNamespace(include_partial_messages=partial)
    return client


def test_the_client_turns_stream_events_into_call_usage():
    client = _client(partial=True)
    assert [type(e).__name__ for e in client._translate_and_capture(_start("msg_9"))] == ["CallUsage"]
    [ended] = client._translate_and_capture(_delta(50))
    assert ended.message_id == "msg_9" and ended.final


def _frame(parent: str | None = None) -> AssistantMessage:
    return AssistantMessage(content=[TextBlock(text="hi")], model="claude-opus-5-5",
                            parent_tool_use_id=parent, usage=dict(START), message_id="msg_f")


def test_without_partial_messages_the_frames_give_the_input_side():
    events = _client(partial=False)._translate_and_capture(_frame())
    [usage] = [e for e in events if isinstance(e, ev.CallUsage)]
    assert (usage.message_id, usage.final, usage.usage) == ("msg_f", False, START)
    # A sub-agent's frame stays the registry's, and with partial messages on
    # the frames add nothing (message_start had the same numbers, earlier).
    assert not [e for e in _client(False)._translate_and_capture(_frame("toolu_1"))
                if isinstance(e, ev.CallUsage)]
    assert not [e for e in _client(True)._translate_and_capture(_frame())
                if isinstance(e, ev.CallUsage)]
    assert not [e for e in _client(None)._translate_and_capture(_frame())
                if isinstance(e, ev.CallUsage)]


def test_live_usage_setting():
    assert NerveConfig.from_dict({}).agent.live_usage is True
    assert NerveConfig.from_dict({"agent": {"live_usage": False}}).agent.live_usage is False
    assert _live_usage_on(SimpleNamespace(agent=SimpleNamespace())) is False


# --------------------------------------------------------------------- #
#  The engine in a scripted turn                                          #
# --------------------------------------------------------------------- #

def _usage(n: int) -> ev.NormalizedUsage:
    return ev.NormalizedUsage.from_anthropic({"input_tokens": n, "output_tokens": n})


class _Client:
    """A runtime that answers each receive_turn() with the next script."""

    native_session_id = "native-l"
    model = "claude-opus-5-5"
    interrupt_spares_background = True
    supports_task_stop = True

    def __init__(self, scripts: list[list]) -> None:
        self.scripts = list(scripts)

    async def start_turn(self, turn) -> None:
        pass

    async def receive_turn(self):
        for event in self.scripts.pop(0):
            yield event
            if isinstance(event, ev.TurnCompleted):
                return

    async def interrupt(self, *, cancel_queued: bool = False) -> None:
        pass

    async def stop_task(self, task_id: str) -> None:
        pass

    def is_alive(self) -> bool:
        return True

    async def disconnect(self, timeout: float = 5.0) -> None:
        pass

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
    await db.update_session_fields(SID, {"total_cost_usd": 10.0})
    engine.sessions.set_client(SID, client)

    async def _get_client(sid, source, model, **_kw):
        engine.sessions.set_client(sid, client)
        return client

    engine._get_or_create_client = _get_client
    return engine


def _call(mid: str, output: int, final: bool, cache_read: int = 100_000) -> ev.CallUsage:
    usage = {**START, "cache_read_input_tokens": cache_read, "output_tokens": output}
    return ev.CallUsage(message_id=mid, model="claude-opus-5-5", usage=usage, final=final)


@pytest_asyncio.fixture
async def heard():
    """Every message broadcast to SID."""
    got: list[dict] = []

    async def listen(session_id, message):
        got.append(message)

    await broadcaster.register(SID, "test-live", listen)
    yield got
    await broadcaster.unregister(SID, "test-live")


@pytest.mark.asyncio
async def test_a_turn_sends_a_running_estimate_per_call(db, tmp_path, heard):
    client = _Client([[
        ev.SystemEvent("init", {}),
        ev.ModelObserved(model="claude-opus-5-5"),
        _call("m1", 3, False),
        ev.TextDelta(text="working"),
        _call("m1", 3, False),          # one more frame of the same call
        _call("m1", 400, True),
        _call("m1", 3, False),          # a late start value never undoes the final one
        _call("m2", 2, False, cache_read=120_000),
        _call("m2", 900, True, cache_read=120_000),
        ev.TextDelta(text=" done"),
        ev.TurnCompleted(native_session_id="native-l", usage=_usage(10), total_cost_usd=1.5),
    ]])
    engine = await _engine(db, tmp_path, client)
    assert await engine.run(SID, "go", source="web", actor=None) == "working done"

    updates = [m for m in heard if m["type"] == "usage_update"]
    assert [u["calls"] for u in updates] == [1, 1, 2, 2]
    first, last = updates[0], updates[-1]
    # Opus 5.5 at $4 in, $20 out, $0.20 cache read, $5 cache write per million.
    one_call_start = (2 * 4 + 3 * 20 + 100_000 * 0.2 + 5_000 * 5) / 1e6
    assert first["turn_cost_usd"] == pytest.approx(one_call_start, abs=1e-6)
    assert first["session_cost_usd"] == pytest.approx(10.0 + one_call_start, abs=1e-6)
    assert first["context_tokens"] == 2 + 100_000 + 5_000
    assert last["usage"]["output_tokens"] == 400 + 900
    assert last["context_tokens"] == 2 + 120_000 + 5_000
    assert last["agents_cost_usd"] == 0
    assert all(u["session_id"] == SID for u in updates)

    [done] = [m for m in heard if m["type"] == "done"]
    assert done["context_tokens"] == 2 + 120_000 + 5_000
    assert done["session_cost_usd"] == pytest.approx(11.5)
    assert engine.live_usage(SID) is None
    session = await db.get_session(SID)
    assert session["total_cost_usd"] == pytest.approx(11.5)


@pytest.mark.asyncio
async def test_usage_updates_stay_out_of_the_replay_buffer(db, tmp_path):
    client = _Client([[
        ev.SystemEvent("init", {}), ev.ModelObserved(model="claude-opus-5-5"),
        ev.TextDelta(text="x"), _call("m1", 5, True),
        ev.TurnCompleted(native_session_id="native-l", usage=_usage(1), total_cost_usd=0.1),
    ]])
    engine = await _engine(db, tmp_path, client)
    seen: list[list] = []
    real = broadcaster.broadcast_usage_update

    async def spy(session_id, live):
        await real(session_id, live)
        seen.append([m["type"] for m in broadcaster.get_buffer(session_id)])

    broadcaster.broadcast_usage_update = spy
    try:
        await engine.run(SID, "go", source="web", actor=None)
    finally:
        broadcaster.broadcast_usage_update = real
    assert seen and all("usage_update" not in types for types in seen)


@pytest.mark.asyncio
async def test_the_agents_spend_since_the_last_turn_counts():
    st = _TurnState()
    sent: list[dict] = []
    agents = {"cost": 2.0}

    async def get_session(_sid):
        return {"total_cost_usd": 5.0}

    fake = SimpleNamespace(
        db=SimpleNamespace(get_session=get_session),
        agents=SimpleNamespace(summary=lambda _sid: {"cost_usd": agents["cost"]}),
        config=SimpleNamespace(agent=SimpleNamespace(context_1m_enabled_for=lambda _m: True)),
        _live_usage={}, _agents_cost_base={},
    )
    fake._agents_cost_now = lambda sid: AgentEngine._agents_cost_now(fake, sid)
    fake._live_payload = lambda sid, s: AgentEngine._live_payload(fake, sid, s)
    real = broadcaster.broadcast_usage_update

    async def capture(_sid, live):
        sent.append(live)

    broadcaster.broadcast_usage_update = capture
    try:
        await AgentEngine._on_call_usage(fake, SID, _call("m1", 3, False), st)
        agents["cost"] = 2.75  # a sub-agent spent $0.75 since
        await AgentEngine._on_call_usage(fake, SID, _call("m1", 3, True), st)
    finally:
        broadcaster.broadcast_usage_update = real
    assert sent[0]["agents_cost_usd"] == 0 and sent[0]["max_context_tokens"] == 1_048_576
    assert sent[1]["agents_cost_usd"] == pytest.approx(0.75)
    assert sent[1]["session_cost_usd"] == pytest.approx(5.0 + sent[1]["turn_cost_usd"] + 0.75)
    assert fake._live_usage[SID] is sent[1]


def test_end_turn_drops_the_estimate():
    fake = SimpleNamespace(_turn_watches={}, _live_usage={SID: {"calls": 1}})
    AgentEngine._end_turn(fake, SID, "outcome")
    assert SID not in fake._live_usage


# --------------------------------------------------------------------- #
#  Broadcasts                                                            #
# --------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_done_carries_the_new_totals():
    bc = StreamBroadcaster()
    got: list[dict] = []

    async def listen(_sid, message):
        got.append(message)

    await bc.register("s", "l", listen)
    await bc.broadcast_done("s", usage={"input_tokens": 1}, context_tokens=42,
                            session_cost_usd=3.5, agents_cost_base=1.25)
    assert got[-1]["context_tokens"] == 42 and got[-1]["session_cost_usd"] == 3.5
    assert got[-1]["agents_cost_base"] == 1.25
    await bc.broadcast_done("s")
    assert "context_tokens" not in got[-1] and "session_cost_usd" not in got[-1]


@pytest.mark.asyncio
async def test_session_status_carries_the_live_estimate(monkeypatch):
    from nerve.gateway import server

    sent: list[dict] = []

    class _Socket:
        async def send_json(self, message):
            sent.append(message)

    live = {"calls": 2, "session_cost_usd": 4.2}
    monkeypatch.setattr(server, "_engine", SimpleNamespace(
        live_usage=lambda sid: live, agents_cost_base=lambda sid: 1.0,
    ))
    await server._send_session_status(_Socket(), SID, True, {"status": "active"}, agents={})
    assert sent[-1]["live_usage"] == live and sent[-1]["agents_cost_base"] == 1.0
    await server._send_session_status(_Socket(), SID, False, {"status": "active"}, agents={})
    assert "live_usage" not in sent[-1]
