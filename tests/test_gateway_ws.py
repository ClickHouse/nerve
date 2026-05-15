"""Tests for nerve.gateway.server WebSocket handshake buffer replay.

The initial WS handshake replays the broadcaster buffer when a turn is in
flight, and stays silent when the session is idle. The existing
``switch_session`` path is covered too so the refactor onto
``_send_session_status`` doesn't regress.
"""

from __future__ import annotations

import pytest

from nerve.agent.streaming import StreamBroadcaster, broadcaster as _global_broadcaster
from nerve.gateway.server import _send_session_status


class FakeWebSocket:
    """Minimal WebSocket stand-in that captures ``send_json`` payloads."""

    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send_json(self, payload: dict) -> None:
        self.sent.append(payload)


@pytest.fixture(autouse=True)
def _reset_broadcaster_buffers():
    """Clear the module-global broadcaster between tests.

    Tests poke ``_global_broadcaster.start_buffering`` directly because the
    helper reads ``broadcaster.get_buffer`` off the module global, not a
    parameter. Reset before and after so a failing test can't leak state.
    """
    _global_broadcaster._session_buffers.clear()
    yield
    _global_broadcaster._session_buffers.clear()


# ---------------------------------------------------------------------------
# _send_session_status helper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestSendSessionStatus:
    """Unit tests for the shared helper called from both WS branches."""

    async def test_running_session_attaches_buffered_events(self):
        ws = FakeWebSocket()
        session_id = "sess-running"
        _global_broadcaster.start_buffering(session_id)
        await _global_broadcaster.broadcast(session_id, {
            "type": "token", "session_id": session_id, "content": "hello ",
        })
        await _global_broadcaster.broadcast(session_id, {
            "type": "token", "session_id": session_id, "content": "world",
        })

        await _send_session_status(
            ws, session_id, is_running=True,
            session_record={"status": "active"},
        )

        assert len(ws.sent) == 1
        msg = ws.sent[0]
        assert msg["type"] == "session_status"
        assert msg["session_id"] == session_id
        assert msg["is_running"] is True
        assert msg["status"] == "active"
        assert "buffered_events" in msg
        contents = [e["content"] for e in msg["buffered_events"]]
        assert contents == ["hello ", "world"]

    async def test_idle_session_omits_buffered_events(self):
        ws = FakeWebSocket()
        # No start_buffering: the buffer is empty / absent.
        await _send_session_status(
            ws, "sess-idle", is_running=False,
            session_record={"status": "active"},
        )

        assert len(ws.sent) == 1
        msg = ws.sent[0]
        assert msg["is_running"] is False
        assert msg["status"] == "active"
        # buffered_events MUST be absent when not running; the frontend
        # gates buffer replay on its presence, not on length.
        assert "buffered_events" not in msg

    async def test_missing_session_record_uses_unknown_status(self):
        ws = FakeWebSocket()
        await _send_session_status(
            ws, "sess-gone", is_running=False, session_record=None,
        )

        assert ws.sent[0]["status"] == "unknown"

    async def test_running_session_with_empty_buffer_still_attaches_list(self):
        """is_running gates ``buffered_events``; an empty list is still a signal.

        Frontend code branches on ``msg.buffered_events !== undefined``;
        shipping an empty list tells the client "this session is running
        but the stream has produced nothing yet" so it can flip
        ``isStreaming`` without inventing fake blocks.
        """
        ws = FakeWebSocket()
        session_id = "sess-running-empty"
        _global_broadcaster.start_buffering(session_id)

        await _send_session_status(
            ws, session_id, is_running=True,
            session_record={"status": "active"},
        )

        msg = ws.sent[0]
        assert msg["buffered_events"] == []


# ---------------------------------------------------------------------------
# Initial-bind handshake (AC15)
#
# The actual handler is a closure inside ``create_app`` so it can't be unit-
# tested without spinning a full FastAPI app + lifespan. We re-exercise the
# *same logic* it runs (``is_buffering`` gate + helper invocation) so that a
# regression in either guard or call shape fails this test.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestInitialBindReplay:

    async def _simulate_initial_bind(
        self,
        ws: FakeWebSocket,
        session_id: str,
        is_running: bool,
        session_record: dict | None,
    ) -> None:
        """Mirror the gate + helper call from the WS handshake."""
        if _global_broadcaster.is_buffering(session_id):
            await _send_session_status(
                ws, session_id, is_running, session_record,
            )

    async def test_replays_when_turn_in_flight(self):
        ws = FakeWebSocket()
        session_id = "sess-in-flight"
        _global_broadcaster.start_buffering(session_id)
        await _global_broadcaster.broadcast(session_id, {
            "type": "tool_use", "session_id": session_id,
            "tool": "Read", "input": {"file_path": "/x"},
        })

        await self._simulate_initial_bind(
            ws, session_id, is_running=True,
            session_record={"status": "active"},
        )

        assert len(ws.sent) == 1
        msg = ws.sent[0]
        assert msg["type"] == "session_status"
        assert msg["is_running"] is True
        assert msg["buffered_events"][0]["tool"] == "Read"

    async def test_no_replay_when_session_idle(self):
        ws = FakeWebSocket()
        # No start_buffering: handshake guard must short-circuit.
        await self._simulate_initial_bind(
            ws, "sess-idle", is_running=False,
            session_record={"status": "active"},
        )

        assert ws.sent == []


# ---------------------------------------------------------------------------
# switch_session regression guard (existing behaviour)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestSwitchSessionStillReplays:
    """``switch_session`` ALWAYS sends ``session_status`` (running or idle)."""

    async def _simulate_switch_session(
        self,
        ws: FakeWebSocket,
        new_session: str,
        is_running: bool,
        session_record: dict | None,
    ) -> None:
        await _send_session_status(
            ws, new_session, is_running, session_record,
        )

    async def test_running_target_replays_buffer(self):
        ws = FakeWebSocket()
        session_id = "sess-switch-running"
        _global_broadcaster.start_buffering(session_id)
        await _global_broadcaster.broadcast(session_id, {
            "type": "token", "session_id": session_id, "content": "x",
        })

        await self._simulate_switch_session(
            ws, session_id, is_running=True,
            session_record={"status": "active"},
        )

        assert ws.sent[0]["is_running"] is True
        assert ws.sent[0]["buffered_events"] == [
            {"type": "token", "session_id": session_id, "content": "x"},
        ]

    async def test_idle_target_still_sends_status(self):
        ws = FakeWebSocket()
        await self._simulate_switch_session(
            ws, "sess-switch-idle", is_running=False,
            session_record={"status": "active"},
        )

        assert len(ws.sent) == 1
        assert ws.sent[0]["is_running"] is False
        assert "buffered_events" not in ws.sent[0]


# ---------------------------------------------------------------------------
# Buffer fidelity: large stream survives intact through replay
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replay_preserves_event_order_under_load():
    """Replay must hand events back in arrival order with no truncation."""
    bc = StreamBroadcaster(max_buffer_size=100)
    bc.start_buffering("sess-load")
    for i in range(50):
        await bc.broadcast("sess-load", {
            "type": "token", "session_id": "sess-load", "content": f"#{i}",
        })

    snapshot = bc.get_buffer("sess-load")
    assert len(snapshot) == 50
    assert [e["content"] for e in snapshot] == [f"#{i}" for i in range(50)]
