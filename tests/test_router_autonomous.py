"""Autonomous turns reach the channel that last messaged the session.

When a background task settles, the CLI runs an autonomous turn that no
inbound message started. The engine broadcasts it, but a channel's stream
adapter only lives for the run of the message it answers, so a Telegram
session used to see nothing of it. ``ChannelRouter.open_autonomous_stream``
and ``close_autonomous_stream`` give such a turn its own adapter.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from nerve.agent.streaming import StreamBroadcaster
from nerve.channels.base import (
    BaseChannel,
    ChannelCapability,
    ChannelConstraints,
    InboundMessage,
    OutboundMessage,
)
from nerve.channels.router import ChannelRouter


class _FakeChannel(BaseChannel):
    """A streaming, editable channel that records what it was asked to do."""

    def __init__(self, name: str = "chat"):
        self._name = name
        self.sent: list[str] = []
        self.placeholders: list[str] = []
        self.deleted: list[str] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def capabilities(self) -> ChannelCapability:
        return ChannelCapability.SEND_TEXT | ChannelCapability.STREAMING

    @property
    def constraints(self) -> ChannelConstraints:
        return ChannelConstraints(
            max_message_length=4096, min_edit_interval=0.0,
            supports_message_edit=True,
        )

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, message: OutboundMessage) -> None:
        self.sent.append(message.text)

    async def send_placeholder(self, target: str, session_id: str) -> str | None:
        placeholder_id = f"ph-{len(self.placeholders) + 1}"
        self.placeholders.append(placeholder_id)
        return placeholder_id

    async def edit_message(self, target: str, message_id: str, text: str) -> None:
        pass

    async def delete_message(self, target: str, message_id: str) -> None:
        self.deleted.append(message_id)


def _router(channel: _FakeChannel) -> ChannelRouter:
    """A router whose engine answers every message instantly."""
    engine = SimpleNamespace(
        sessions=SimpleNamespace(
            get_active_session=AsyncMock(return_value="s1"),
            set_active_session=AsyncMock(),
        ),
        run=AsyncMock(return_value="ok"),
        register_task=lambda session_id, task: None,
    )
    router = ChannelRouter(engine)
    router.register(channel)
    return router


async def _message(router: ChannelRouter, channel: _FakeChannel) -> None:
    """One inbound user message, answered and torn down like in production."""
    with patch.object(ChannelRouter, "BATCH_DEBOUNCE", 0):
        await router.handle_message(InboundMessage(
            channel_name=channel.name, channel_key=f"{channel.name}:42",
            sender_id="42", text="hi",
        ))


@pytest.fixture
def bc():
    """A fresh broadcaster wired into the router module."""
    fresh = StreamBroadcaster()
    with patch("nerve.channels.router.broadcaster", fresh):
        yield fresh


@pytest.mark.asyncio
async def test_autonomous_turn_is_sent_to_the_last_inbound_channel(bc):
    channel = _FakeChannel()
    router = _router(channel)
    await _message(router, channel)
    channel.sent.clear()  # the user run's own answer is not under test

    await router.open_autonomous_stream("s1")
    await bc.broadcast_token("s1", "Background job finished.")
    await bc.broadcast_done("s1")
    await router.close_autonomous_stream("s1")

    assert channel.sent == ["Background job finished."]
    # The streaming placeholder is replaced by the final message.
    assert channel.deleted == [channel.placeholders[-1]]
    assert "s1" not in bc._listeners


@pytest.mark.asyncio
async def test_session_never_messaged_through_a_channel_is_left_alone(bc):
    channel = _FakeChannel()
    router = _router(channel)  # no inbound message: web UI, cron, workflow

    await router.open_autonomous_stream("s1")
    await bc.broadcast_token("s1", "nobody is listening on the channel")
    await bc.broadcast_done("s1")
    await router.close_autonomous_stream("s1")

    assert channel.sent == []
    assert channel.placeholders == []


@pytest.mark.asyncio
async def test_empty_turn_leaves_no_message(bc):
    """No "(no response)" for a turn that produced nothing."""
    channel = _FakeChannel()
    router = _router(channel)
    await _message(router, channel)
    channel.sent.clear()

    await router.open_autonomous_stream("s1")
    placeholder = channel.placeholders[-1]
    await router.close_autonomous_stream("s1")  # no tokens, no done

    assert channel.sent == []
    assert channel.deleted[-1] == placeholder


@pytest.mark.asyncio
async def test_turn_cut_short_still_delivers_what_arrived(bc):
    channel = _FakeChannel()
    router = _router(channel)
    await _message(router, channel)
    channel.sent.clear()

    await router.open_autonomous_stream("s1")
    await bc.broadcast_token("s1", "Half a thought")
    await router.close_autonomous_stream("s1")  # cancelled before done

    assert channel.sent == ["Half a thought"]


@pytest.mark.asyncio
async def test_backstop_done_after_the_real_one_does_not_resend(bc):
    channel = _FakeChannel()
    router = _router(channel)
    await _message(router, channel)
    channel.sent.clear()

    await router.open_autonomous_stream("s1")
    await bc.broadcast_token("s1", "Once")
    await bc.broadcast_done("s1")
    await bc.broadcast_done("s1")
    await router.close_autonomous_stream("s1")

    assert channel.sent == ["Once"]


@pytest.mark.asyncio
async def test_no_second_stream_while_a_user_run_streams_to_the_target(bc):
    """A turn drained inside run() reaches the channel through the run's own
    adapter; a second adapter would send everything twice."""
    channel = _FakeChannel()
    router = _router(channel)
    await _message(router, channel)
    user_run_adapter = await router._setup_streaming(channel, "42", "s1")
    placeholders_before = list(channel.placeholders)

    await router.open_autonomous_stream("s1")

    assert channel.placeholders == placeholders_before
    assert "s1" not in router._autonomous
    assert router._adapters[(channel.name, "42")] is user_run_adapter


@pytest.mark.asyncio
async def test_open_and_close_are_idempotent(bc):
    channel = _FakeChannel()
    router = _router(channel)
    await _message(router, channel)
    channel.sent.clear()

    await router.open_autonomous_stream("s1")
    await router.open_autonomous_stream("s1")
    await bc.broadcast_token("s1", "One message")
    await bc.broadcast_done("s1")
    await router.close_autonomous_stream("s1")
    await router.close_autonomous_stream("s1")

    assert channel.sent == ["One message"]
    assert len(bc._listeners.get("s1", [])) == 0


@pytest.mark.asyncio
async def test_closing_does_not_unregister_a_user_run_listener(bc):
    """The autonomous listener has its own id, so tearing it down leaves a
    concurrently registered user-run adapter in place."""
    channel = _FakeChannel()
    router = _router(channel)
    await _message(router, channel)

    await router.open_autonomous_stream("s1")
    await bc.register("s1", f"{channel.name}:42", AsyncMock())
    await router.close_autonomous_stream("s1")

    assert [cid for cid, _ in bc._listeners["s1"]] == [f"{channel.name}:42"]
