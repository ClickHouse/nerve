"""A WebSocket frame that names a session that does not exist, through the
real ``/ws`` endpoint of ``create_app()``.

A link can outlive its session, and a tab can stay open on a session that
another tab deletes. The client names that session on every connect, so the
server answers and keeps the socket. A closed socket would reconnect without
end.
"""

from __future__ import annotations

import pytest

from nerve.agent.streaming import broadcaster
from nerve.gateway import server
from tests.test_ws_agents import OTHER, SID, _Socket, ws  # noqa: F401 — ws is the fixture

GONE = "gone1234"


def _bound_to(session_id: str):
    """A script step that checks which session the socket follows now."""
    def check(socket: _Socket) -> None:
        listeners = broadcaster._listeners
        assert listeners.get(session_id), f"no listener on {session_id}"
    return check


async def _channel_session(ws) -> str | None:
    """The session the web channel is mapped to: where the next page load starts."""
    row = await ws.db.get_channel_session("web:default")
    return row["session_id"] if row else None


@pytest.mark.asyncio
async def test_a_switch_to_a_session_that_does_not_exist_is_answered(ws):
    socket = _Socket(ws.token, [
        {"type": "switch_session", "session_id": GONE},
        _bound_to(SID),
    ])

    await ws.endpoint(socket)

    assert socket.of_type("session_not_found") == [{"type": "session_not_found", "session_id": GONE}]
    # Nothing was bound to the unknown ID, and the channel keeps its session.
    assert [m["session_id"] for m in socket.of_type("session_switched")] == [SID]
    assert await _channel_session(ws) == SID
    assert await ws.db.get_session(GONE) is None


@pytest.mark.asyncio
async def test_the_socket_takes_the_next_frame_after_the_answer(ws):
    socket = _Socket(ws.token, [
        {"type": "switch_session", "session_id": GONE},
        {"type": "switch_session", "session_id": OTHER},
        _bound_to(OTHER),
    ])

    await ws.endpoint(socket)

    assert len(socket.of_type("session_not_found")) == 1
    assert [m["session_id"] for m in socket.of_type("session_switched")] == [SID, OTHER]
    assert await _channel_session(ws) == OTHER


@pytest.mark.asyncio
async def test_every_connect_gets_the_same_answer(ws):
    """What the client's reconnect does: it names the session on screen again."""
    for _ in range(3):
        socket = _Socket(ws.token, [{"type": "switch_session", "session_id": GONE}])
        await ws.endpoint(socket)
        assert len(socket.of_type("session_not_found")) == 1


@pytest.mark.asyncio
async def test_a_message_for_a_session_that_does_not_exist_is_refused(ws, monkeypatch):
    runs: list[dict] = []

    async def run(**kwargs):
        runs.append(kwargs)

    monkeypatch.setattr(ws.engine, "run", run)
    socket = _Socket(ws.token, [
        {"type": "message", "session_id": GONE, "content": "hello", "client_msg_id": "m-1"},
        _bound_to(SID),
    ])

    await ws.endpoint(socket)

    assert socket.of_type("error") == [{
        "type": "error", "session_id": GONE,
        "error": server.SESSION_NOT_FOUND_ERROR, "client_msg_id": "m-1",
    }]
    assert runs == []
    assert await _channel_session(ws) == SID
    assert await ws.db.get_session(GONE) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, "", 7, ["x"], {"id": "x"}])
async def test_a_value_that_is_no_session_id_is_answered_too(ws, value):
    """A frame can hold any JSON."""
    socket = _Socket(ws.token, [
        {"type": "switch_session", "session_id": value},
        {"type": "message", "session_id": value, "content": "hello", "client_msg_id": "m-1"},
        {"type": "switch_session", "session_id": OTHER},
        _bound_to(OTHER),
    ])

    await ws.endpoint(socket)

    assert socket.of_type("session_not_found") == [{"type": "session_not_found", "session_id": value}]
    assert [m["error"] for m in socket.of_type("error")] == [server.SESSION_NOT_FOUND_ERROR]
    assert [m["session_id"] for m in socket.of_type("session_switched")] == [SID, OTHER]


@pytest.mark.asyncio
async def test_a_session_deleted_while_the_socket_is_open_is_answered(ws):
    async def delete_other(_socket: _Socket) -> None:
        await ws.db.delete_session(OTHER)

    socket = _Socket(ws.token, [
        delete_other,
        {"type": "switch_session", "session_id": OTHER},
        _bound_to(SID),
    ])

    await ws.endpoint(socket)

    assert socket.of_type("session_not_found") == [{"type": "session_not_found", "session_id": OTHER}]
    assert await _channel_session(ws) == SID


def test_the_error_of_a_missing_session_is_still_a_value_error():
    """Callers of the switch that catch ValueError keep working."""
    from nerve.agent.sessions import SessionNotFoundError

    assert issubclass(SessionNotFoundError, ValueError)
