"""Tests for the delivery-only notification-sink guard in TelegramChannel.

With ``notifications.delivery_only_sink`` on, a group ``telegram_chat_id`` is
one-way: neither an inbound message nor a reaction there starts an agent turn.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from nerve.channels.telegram import TelegramChannel

SINK = -1000000000001  # obviously-synthetic id — never a real chat


def _channel(configured_sink=SINK, delivery_only=True):
    """A TelegramChannel with only the state the guard/handlers read
    (``__init__`` builds the whole bot application, so bypass it)."""
    ch = TelegramChannel.__new__(TelegramChannel)
    ch._config = lambda: SimpleNamespace(notifications=SimpleNamespace(
        telegram_chat_id=configured_sink, delivery_only_sink=delivery_only))
    ch._touch = lambda: None
    ch._is_authorized = lambda _uid: True
    ch._message_cache = {}
    ch.router = SimpleNamespace(handle_message=AsyncMock())
    return ch


@pytest.mark.parametrize("chat_id, chat_type, delivery_only, expected", [
    (SINK, "group", True, True),          # opted-in group sink → one-way
    (SINK, "supergroup", True, True),
    (SINK, "group", False, False),        # flag off (the default) → unchanged
    (SINK, "private", True, False),       # a DM stays interactive
    (SINK, "channel", True, False),       # only group/supergroup qualify
    (SINK - 1, "group", True, False),     # a different chat isn't the sink
])
def test_is_delivery_only_sink(chat_id, chat_type, delivery_only, expected):
    ch = _channel(delivery_only=delivery_only)
    assert ch._is_delivery_only_sink(SimpleNamespace(id=chat_id, type=chat_type)) is expected


def _reaction(chat_type="group"):
    return SimpleNamespace(message_reaction=SimpleNamespace(
        user=SimpleNamespace(id=1), chat=SimpleNamespace(id=SINK, type=chat_type),
        message_id=1, new_reaction=[SimpleNamespace(emoji="👍")]))


@pytest.mark.asyncio
async def test_reaction_in_sink_is_dropped():
    # Regression: _handle_reaction has its own router call and needs the guard too.
    ch = _channel()
    await ch._handle_reaction(_reaction(), None)
    ch.router.handle_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_reaction_reaches_router_when_flag_off():
    ch = _channel(delivery_only=False)
    await ch._handle_reaction(_reaction(), None)
    ch.router.handle_message.assert_awaited_once()
