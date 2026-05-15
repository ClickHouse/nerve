"""Tests for nerve.agent.streaming — StreamBroadcaster bounded buffers."""

import asyncio

import pytest

from nerve.agent.streaming import StreamBroadcaster


@pytest.mark.asyncio
class TestBroadcaster:
    """Test basic broadcast operations."""

    async def test_register_and_broadcast(self):
        bc = StreamBroadcaster()
        received = []

        async def handler(sid, msg):
            received.append(msg)

        await bc.register("s1", "listener1", handler)
        await bc.broadcast("s1", {"type": "token", "content": "hello"})

        assert len(received) == 1
        assert received[0]["content"] == "hello"

    async def test_unregister(self):
        bc = StreamBroadcaster()
        received = []

        async def handler(sid, msg):
            received.append(msg)

        await bc.register("s1", "listener1", handler)
        await bc.unregister("s1", "listener1")
        await bc.broadcast("s1", {"type": "token", "content": "hello"})

        assert len(received) == 0

    async def test_multiple_listeners(self):
        bc = StreamBroadcaster()
        r1, r2 = [], []

        async def h1(sid, msg):
            r1.append(msg)

        async def h2(sid, msg):
            r2.append(msg)

        await bc.register("s1", "l1", h1)
        await bc.register("s1", "l2", h2)
        await bc.broadcast("s1", {"type": "test"})

        assert len(r1) == 1
        assert len(r2) == 1

    async def test_failed_listener_doesnt_block(self):
        bc = StreamBroadcaster()
        received = []

        async def bad_handler(sid, msg):
            raise RuntimeError("boom")

        async def good_handler(sid, msg):
            received.append(msg)

        await bc.register("s1", "bad", bad_handler)
        await bc.register("s1", "good", good_handler)
        await bc.broadcast("s1", {"type": "test"})

        # good handler should still have received
        assert len(received) == 1


@pytest.mark.asyncio
class TestBuffering:
    """Test event buffering for reconnect replay."""

    async def test_start_stop_buffering(self):
        bc = StreamBroadcaster()
        bc.start_buffering("s1")
        assert bc.is_buffering("s1")

        await bc.broadcast("s1", {"type": "token", "content": "a"})
        await bc.broadcast("s1", {"type": "token", "content": "b"})

        buf = bc.get_buffer("s1")
        assert len(buf) == 2

        events = bc.stop_buffering("s1")
        assert len(events) == 2
        assert not bc.is_buffering("s1")

    async def test_buffer_not_mutated_by_get(self):
        bc = StreamBroadcaster()
        bc.start_buffering("s1")
        await bc.broadcast("s1", {"type": "test"})

        buf = bc.get_buffer("s1")
        buf.append({"type": "injected"})

        # Original buffer should be unaffected
        assert len(bc.get_buffer("s1")) == 1

    async def test_no_buffering_when_not_started(self):
        bc = StreamBroadcaster()
        await bc.broadcast("s1", {"type": "test"})
        assert bc.get_buffer("s1") == []


@pytest.mark.asyncio
class TestOpenTurnTracking:
    """The engine's run() finally relies on open-turn tracking to ship a
    synthetic ``done`` when ``_run_inner`` exits without sending a
    terminal event.  Without it, the chat detail stays on
    "thinking..." forever even though the server has cleared
    is_running and dropped the session out of the sidebar's "Running"
    group.
    """

    async def test_mark_open_and_terminal_clears(self):
        bc = StreamBroadcaster()
        assert not bc.is_turn_open("s1")

        bc.mark_turn_open("s1")
        assert bc.is_turn_open("s1")

        # Each terminal type should clear the flag
        for terminal in ("done", "stopped", "error"):
            bc.mark_turn_open("s1")
            await bc.broadcast("s1", {"type": terminal})
            assert not bc.is_turn_open("s1"), (
                f"{terminal!r} should clear the open-turn flag"
            )

    async def test_non_terminal_does_not_clear(self):
        bc = StreamBroadcaster()
        bc.mark_turn_open("s1")

        # Streaming events must not close the turn
        for nonterminal in (
            {"type": "token", "content": "hi"},
            {"type": "thinking", "content": "..."},
            {"type": "tool_use", "tool": "Bash"},
            {"type": "tool_result", "tool_use_id": "x"},
        ):
            await bc.broadcast("s1", nonterminal)
            assert bc.is_turn_open("s1"), (
                f"{nonterminal['type']} should not clear the open-turn flag"
            )

    async def test_clear_turn_open_explicit(self):
        bc = StreamBroadcaster()
        bc.mark_turn_open("s1")
        assert bc.is_turn_open("s1")
        bc.clear_turn_open("s1")
        assert not bc.is_turn_open("s1")

    async def test_open_turns_isolated_per_session(self):
        bc = StreamBroadcaster()
        bc.mark_turn_open("s1")
        bc.mark_turn_open("s2")
        await bc.broadcast("s1", {"type": "done"})
        assert not bc.is_turn_open("s1")
        assert bc.is_turn_open("s2")

    async def test_broadcast_done_helper_clears_open_turn(self):
        # The engine backstop calls broadcaster.broadcast_done() to ship
        # the synthetic terminal event.  Verify the helper closes the
        # turn so a second call wouldn't double-broadcast.
        bc = StreamBroadcaster()
        bc.mark_turn_open("s1")
        await bc.broadcast_done("s1", usage={"input_tokens": 1})
        assert not bc.is_turn_open("s1")


@pytest.mark.asyncio
class TestBoundedBuffers:
    """Test that buffers are bounded to max_buffer_size."""

    async def test_buffer_bounded(self):
        bc = StreamBroadcaster(max_buffer_size=5)
        bc.start_buffering("s1")

        for i in range(10):
            await bc.broadcast("s1", {"type": "token", "i": i})

        buf = bc.get_buffer("s1")
        assert len(buf) == 5
        # Should keep the last 5
        assert buf[0]["i"] == 5
        assert buf[4]["i"] == 9

    async def test_buffer_stats(self):
        bc = StreamBroadcaster()
        bc.start_buffering("s1")
        bc.start_buffering("s2")

        await bc.broadcast("s1", {"type": "a"})
        await bc.broadcast("s1", {"type": "b"})
        await bc.broadcast("s2", {"type": "c"})

        stats = bc.get_buffer_stats()
        assert stats["s1"] == 2
        assert stats["s2"] == 1
