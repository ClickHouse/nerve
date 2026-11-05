"""The WebSocket handler's agent support, through the real
``/ws`` endpoint of ``create_app()``.

Every bind sends ``session_status`` with the agent snapshot; sockets follow
agent transcripts on per-agent channels (at most 8, dropped on switch and
disconnect); messages carry ``client_msg_id``.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
import pytest_asyncio
from starlette.websockets import WebSocketDisconnect

from nerve.agent.agents.emitter import agent_channel
from nerve.agent.backends import events as ev
from nerve.agent.engine import AgentEngine
from nerve.agent.streaming import broadcaster
from nerve.config import NerveConfig, set_config
from nerve.gateway import server
from nerve.gateway.auth import create_session_token, pin_jwt_secret

_SECRET = "test-secret-for-ws-agents-0123456789abcdef"
SID = "s-1234"
OTHER = "s-5678"


class _Socket:
    """A WebSocket whose client plays a script: dict frames are received in
    order; callables run in between (with the socket) to check the server's
    state mid-connection. The script's end disconnects."""

    def __init__(self, token: str, script: list) -> None:
        self.query_params = {"token": token}
        self.cookies: dict = {}
        self.sent: list[dict] = []
        self._script = list(script)

    async def accept(self) -> None:
        return None

    async def close(self, code: int = 1000, reason: str = "") -> None:
        return None

    async def send_json(self, payload: dict) -> None:
        self.sent.append(payload)

    async def receive_json(self) -> dict:
        while self._script:
            step = self._script.pop(0)
            if callable(step):
                result = step(self)
                if asyncio.iscoroutine(result):
                    await result
                continue
            return step
        raise WebSocketDisconnect(1000)

    def of_type(self, kind: str) -> list[dict]:
        return [m for m in self.sent if m.get("type") == kind]


def _ws_endpoint():
    app = server.create_app()
    return next(r.endpoint for r in app.routes if getattr(r, "path", "") == "/ws")


@pytest_asyncio.fixture
async def ws(tmp_path, open_identity_db, wire_identity_store, monkeypatch):
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
        "auth": {"jwt_secret": _SECRET},
    })
    config.workspace.mkdir(parents=True)
    set_config(config)
    pin_jwt_secret(_SECRET)
    database, identity = await open_identity_db(tmp_path / "nerve.db")
    wire_identity_store(database)
    engine = AgentEngine(config, database)
    monkeypatch.setattr(server, "_engine", engine)
    for sid in (SID, OTHER):
        await engine.sessions.get_or_create(sid, source="web", actor=None)
    # The socket binds to the channel's last session.
    await database.set_channel_session("web:default", SID)
    try:
        yield SimpleNamespace(
            engine=engine, db=database,
            token=create_session_token(_SECRET, identity.owner_account_id),
            actor_id=identity.owner_actor_id,
            endpoint=_ws_endpoint(),
        )
    finally:
        await database.close()
        set_config(NerveConfig())


async def _agent(engine: AgentEngine, task_id: str, sid: str = SID):
    await engine.agents.on_system_event(sid, ev.SystemEvent("task_started", {
        "task_id": task_id, "task_type": "local_agent", "description": f"agent {task_id}",
        "is_backgrounded": True,
    }))
    return next(r for r in engine.agents._sessions[sid].records.values() if r.task_id == task_id)


# --------------------------------------------------------------------------- #
#  session_status                                                              #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_initial_bind_always_sends_status_with_agents(ws):
    rec = await _agent(ws.engine, "a1b2c3d4e5f6a7b8c")
    socket = _Socket(ws.token, [])
    await ws.endpoint(socket)

    assert [m["type"] for m in socket.sent[:2]] == ["session_switched", "session_status"]
    status = socket.sent[1]
    assert (status["session_id"], status["is_running"]) == (SID, False)
    assert "buffered_events" not in status           # idle: nothing to replay
    snap = status["agents"]
    assert set(snap) == {"session_id", "agents", "summary", "capabilities", "queue", "generated_at"}
    assert [a["id"] for a in snap["agents"]] == [rec.id]
    assert snap["summary"]["live"] == 1


@pytest.mark.asyncio
async def test_switching_sends_the_new_sessions_snapshot(ws):
    other = await _agent(ws.engine, "a2b2c3d4e5f6a7b8c", sid=OTHER)
    socket = _Socket(ws.token, [{"type": "switch_session", "session_id": OTHER}])
    await ws.endpoint(socket)
    statuses = socket.of_type("session_status")
    assert [s["session_id"] for s in statuses] == [SID, OTHER]
    assert [a["id"] for a in statuses[1]["agents"]["agents"]] == [other.id]


# --------------------------------------------------------------------------- #
#  Agent subscriptions                                                         #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_subscribe_streams_the_agents_transcript_until_unsubscribed(ws):
    rec = await _agent(ws.engine, "a1b2c3d4e5f6a7b8c")
    channel = agent_channel(SID, rec.id)
    checks: list[bool] = []

    async def _item(_socket):
        # A transcript item of the subscribed agent goes out on its channel.
        ws.engine.agents._append_item(rec, {"kind": "text", "text": "hello"})
        await ws.engine.agents.emitter.flush()

    socket = _Socket(ws.token, [
        {"type": "agent_subscribe", "session_id": SID, "agent_id": rec.id},
        lambda _s: checks.append(broadcaster.has_listeners(channel)),
        _item,
        {"type": "agent_unsubscribe", "session_id": SID, "agent_id": rec.id},
        lambda _s: checks.append(broadcaster.has_listeners(channel)),
        _item,
    ])
    await ws.endpoint(socket)

    assert checks == [True, False]
    (event,) = socket.of_type("agent_event")
    assert set(event) == {"type", "session_id", "agent_id", "item"}
    assert (event["session_id"], event["agent_id"]) == (SID, rec.id)
    assert event["item"]["kind"] == "text" and event["item"]["text"] == "hello"
    assert socket.of_type("error") == []


@pytest.mark.asyncio
async def test_an_unknown_agent_is_an_error(ws):
    rec_elsewhere = await _agent(ws.engine, "a2b2c3d4e5f6a7b8c", sid=OTHER)
    socket = _Socket(ws.token, [
        {"type": "agent_subscribe", "session_id": SID, "agent_id": "ag_000000000000"},
        # Not the socket's active session.
        {"type": "agent_subscribe", "session_id": OTHER, "agent_id": rec_elsewhere.id},
        # Unsubscribing what is not followed is no error.
        {"type": "agent_unsubscribe", "session_id": SID, "agent_id": "ag_000000000000"},
    ])
    await ws.endpoint(socket)
    assert socket.of_type("error") == [
        {"type": "error", "session_id": SID, "error": "agent_not_found"},
        {"type": "error", "session_id": OTHER, "error": "agent_not_found"},
    ]
    assert not broadcaster.has_listeners(agent_channel(OTHER, rec_elsewhere.id))


@pytest.mark.asyncio
async def test_a_socket_follows_at_most_eight_agents(ws):
    recs = [await _agent(ws.engine, f"a{i:02d}b2c3d4e5f6a7b") for i in range(9)]
    seen: list[list[bool]] = []

    def _check(_socket):
        seen.append([broadcaster.has_listeners(agent_channel(SID, r.id)) for r in recs])

    script: list = [
        {"type": "agent_subscribe", "session_id": SID, "agent_id": r.id} for r in recs[:8]
    ]
    script += [
        _check,
        # Re-subscribing refreshes: the first becomes the newest.
        {"type": "agent_subscribe", "session_id": SID, "agent_id": recs[0].id},
        {"type": "agent_subscribe", "session_id": SID, "agent_id": recs[8].id},
        _check,
    ]
    await ws.endpoint(_Socket(ws.token, script))
    assert seen[0] == [True] * 8 + [False]
    # The oldest (recs[1], since recs[0] was refreshed) was dropped.
    assert seen[1] == [True, False] + [True] * 7
    # The disconnect dropped every subscription.
    assert not any(broadcaster.has_listeners(agent_channel(SID, r.id)) for r in recs)


@pytest.mark.asyncio
async def test_switching_sessions_drops_the_subscriptions(ws):
    rec = await _agent(ws.engine, "a1b2c3d4e5f6a7b8c")
    channel = agent_channel(SID, rec.id)
    states: list[bool] = []
    await ws.endpoint(_Socket(ws.token, [
        {"type": "agent_subscribe", "session_id": SID, "agent_id": rec.id},
        lambda _s: states.append(broadcaster.has_listeners(channel)),
        {"type": "switch_session", "session_id": OTHER},
        lambda _s: states.append(broadcaster.has_listeners(channel)),
    ]))
    assert states == [True, False]


@pytest.mark.asyncio
async def test_a_disconnect_drops_the_subscriptions(ws):
    rec = await _agent(ws.engine, "a1b2c3d4e5f6a7b8c")
    channel = agent_channel(SID, rec.id)
    await ws.endpoint(_Socket(ws.token, [
        {"type": "agent_subscribe", "session_id": SID, "agent_id": rec.id},
    ]))
    assert not broadcaster.has_listeners(channel)


# --------------------------------------------------------------------------- #
#  Messages                                                                    #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_message_carries_its_client_msg_id(ws, monkeypatch):
    runs: list[dict] = []
    done = asyncio.Event()

    async def _run(**kwargs):
        runs.append(kwargs)
        done.set()
        return ""

    monkeypatch.setattr(ws.engine, "run", _run)
    registered: list = []
    monkeypatch.setattr(ws.engine, "register_task", lambda *a: registered.append(a))
    echoes: list[dict] = []

    async def _listener(_sid, message):
        echoes.append(message)

    await broadcaster.register(SID, "other-tab", _listener)
    try:
        await ws.endpoint(_Socket(ws.token, [{
            "type": "message", "session_id": SID, "content": "run the e2e suite",
            "client_msg_id": "4b6f0c1e-2a1d-4c8e-9a51-0e2b7c4d9f10",
        }]))
        await asyncio.wait_for(done.wait(), timeout=5)
    finally:
        await broadcaster.unregister(SID, "other-tab")

    (run,) = runs
    assert run["client_msg_id"] == "4b6f0c1e-2a1d-4c8e-9a51-0e2b7c4d9f10"
    assert run["origin"] == "web"
    assert registered == []             # the run is the Stop target only once it holds the lock
    (echo,) = [m for m in echoes if m.get("type") == "user_message"]
    assert echo["client_msg_id"] == "4b6f0c1e-2a1d-4c8e-9a51-0e2b7c4d9f10"
    assert (echo["content"], echo["actor_id"]) == ("run the e2e suite", ws.actor_id)


@pytest.mark.asyncio
async def test_a_message_without_an_id_echoes_null(ws, monkeypatch):
    runs: list[dict] = []

    async def _run(**kwargs):
        runs.append(kwargs)
        return ""

    monkeypatch.setattr(ws.engine, "run", _run)
    echoes: list[dict] = []

    async def _listener(_sid, message):
        echoes.append(message)

    await broadcaster.register(SID, "other-tab", _listener)
    try:
        await ws.endpoint(_Socket(ws.token, [
            {"type": "message", "session_id": SID, "content": "hi"},
            {"type": "message", "session_id": SID, "content": "hi", "client_msg_id": 42},
        ]))
        for _ in range(100):
            if len(runs) == 2:
                break
            await asyncio.sleep(0.01)
    finally:
        await broadcaster.unregister(SID, "other-tab")
    assert [r["client_msg_id"] for r in runs] == [None, None]
    assert [m["client_msg_id"] for m in echoes if m.get("type") == "user_message"] == [None, None]
