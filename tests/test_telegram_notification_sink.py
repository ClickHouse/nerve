"""Tests for the delivery-only notification-sink guard in TelegramChannel.

A dedicated notification group (``notifications.telegram_chat_id`` pointed at a
group) receives pushes but must never start an agent turn on inbound messages.
``_is_delivery_only_sink`` is the predicate the message handler consults.
"""

from types import SimpleNamespace

from nerve.channels.telegram import TelegramChannel

# Obviously-synthetic ids — never a real chat.
SINK = -1000000000001
OTHER_GROUP = -1000000000002
DM = 424242


def _channel(sink_chat_id):
    """A TelegramChannel exposing only the config the guard reads.

    Bypasses ``__init__`` (which builds the whole bot application); the guard
    depends on nothing but ``config.notifications.telegram_chat_id``.
    """
    ch = TelegramChannel.__new__(TelegramChannel)
    # `config` is a read-only property returning ``self._config()`` — a
    # zero-arg callable supplied at construction. Mirror that shape.
    ch._config = lambda: SimpleNamespace(
        notifications=SimpleNamespace(telegram_chat_id=sink_chat_id),
    )
    return ch


def _chat(chat_id, chat_type):
    return SimpleNamespace(id=chat_id, type=chat_type)


def test_group_sink_is_delivery_only():
    ch = _channel(SINK)
    assert ch._is_delivery_only_sink(_chat(SINK, "group")) is True
    assert ch._is_delivery_only_sink(_chat(SINK, "supergroup")) is True


def test_other_chats_are_not_sink():
    ch = _channel(SINK)
    assert ch._is_delivery_only_sink(_chat(DM, "private")) is False
    assert ch._is_delivery_only_sink(_chat(OTHER_GROUP, "group")) is False


def test_private_chat_with_sink_id_stays_interactive():
    # Defensive: a DM must stay interactive even if its id equals the sink id,
    # since the guard requires a non-private chat.
    ch = _channel(SINK)
    assert ch._is_delivery_only_sink(_chat(SINK, "private")) is False


def test_no_sink_configured_disables_guard():
    ch = _channel(None)
    assert ch._is_delivery_only_sink(_chat(SINK, "group")) is False
