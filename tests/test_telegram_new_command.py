"""Tests for ``/new [message]`` (nerve.channels.telegram).

``/new`` alone stops the chat's current session and switches to a fresh one.
``/new <text>`` also delivers ``<text>`` as the new session's first message,
through the same pipeline as a plain message — reply context, attachments,
message id, route recording — but with the new session forced as the target.
That makes it the escape hatch from reply routing: a reply sent as
``/new <text>`` starts a new session instead of continuing (or being refused
by) the replied-to message's session.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.engine import NotAcceptingTurnsError
from nerve.channels.telegram import REPLY_UNRESOLVED_MSG, TelegramChannel
from nerve.config import NerveConfig
from nerve.db import Database

_USER = 999
_CHAT = 1001
_PREVIOUS = "sess-old"
_NEW = "sess-new"
_REPLIED_TO_TEXT = "Nightly report: 2 checks failed."


def _channel(
    db: Database, sessions: dict | None = None, routing: bool | None = None,
) -> TelegramChannel:
    """A channel whose command and delivery paths run against a fake router.

    Attachment extraction is stubbed out; everything from authorization to the
    routing decision and the delivery call is the production code, over a real
    database for the reply-route table. ``routing`` left as None keeps the
    config default.
    """
    cfg = NerveConfig()
    cfg.telegram.allowed_users = [_USER]
    if routing is not None:
        cfg.telegram.reply_routes_to_origin_session = routing
    router = MagicMock()
    router.engine = SimpleNamespace(db=db)
    router.observe = AsyncMock(return_value=True)
    router.handle_message = AsyncMock()
    router.get_session = AsyncMock(side_effect=lambda sid: (sessions or {}).get(sid))
    router.get_active_session = AsyncMock(return_value="sess-active")
    router.get_last_session = AsyncMock(return_value=_PREVIOUS)
    router.stop_session = AsyncMock(return_value=True)
    router.create_session = AsyncMock(return_value=_NEW)
    ch = TelegramChannel(lambda: cfg, router)
    ch._extract_image = AsyncMock(return_value=None)
    ch._extract_sticker = AsyncMock(return_value=(None, ""))
    ch._extract_document = AsyncMock(return_value=([], ""))
    return ch


def _text_update(text: str, reply_to: int | None = None, message_id: int = 900):
    """A private-chat text message, replying to ``reply_to`` if given.

    Returns ``(update, context)`` shaped as PTB's CommandHandler hands them
    over: ``context.args`` is the whitespace-split text after the command.
    """
    update = MagicMock()
    update.effective_chat.id = _CHAT
    update.effective_chat.type = "private"
    update.effective_user.id = _USER
    message = update.message
    message.text = text
    message.caption = None
    message.message_id = message_id
    message.media_group_id = None
    message.forward_origin = None
    message.quote = None
    message.reply_text = AsyncMock()
    if reply_to is None:
        message.reply_to_message = None
    else:
        reply = MagicMock()
        reply.message_id = reply_to
        reply.text = _REPLIED_TO_TEXT
        reply.caption = None
        reply.from_user = SimpleNamespace(is_bot=True)
        message.reply_to_message = reply
    context = SimpleNamespace(args=text.split()[1:])
    return update, context


def _delivered(ch: TelegramChannel):
    """The single InboundMessage the router received."""
    ch.router.handle_message.assert_awaited_once()
    return ch.router.handle_message.await_args.args[0]


def _replies(update) -> list[str]:
    return [c.args[0] for c in update.message.reply_text.await_args_list]


@pytest.mark.asyncio
async def test_new_without_text_switches_sessions_and_sends_nothing(db):
    ch = _channel(db)
    update, context = _text_update("/new")

    await ch._handle_new_session(update, context)

    ch.router.stop_session.assert_awaited_once_with(_PREVIOUS)
    ch.router.create_session.assert_awaited_once_with(
        f"telegram:{_CHAT}", source="telegram",
    )
    ch.router.handle_message.assert_not_awaited()
    assert _replies(update) == [
        f"Stopped session `{_PREVIOUS}`.", f"New session: `{_NEW}`",
    ]


@pytest.mark.asyncio
async def test_new_with_text_delivers_it_as_the_first_message(db):
    ch = _channel(db)
    update, context = _text_update("/new hello world")
    order = MagicMock()
    order.attach_mock(update.message.reply_text, "reply_text")
    order.attach_mock(ch.router.handle_message, "handle_message")

    await ch._handle_new_session(update, context)

    msg = _delivered(ch)
    assert msg.session_id == _NEW
    assert msg.text == "hello world"
    assert msg.channel_key == f"telegram:{_CHAT}"
    assert msg.metadata["message_id"] == 900
    # The id is confirmed first, then the turn runs in that session.
    assert [name for name, _a, _kw in order.mock_calls] == [
        "reply_text", "reply_text", "handle_message",
    ]
    assert _replies(update)[-1] == f"New session: `{_NEW}`"
    # A later reply to the user's own /new message comes back to that session.
    assert await ch._lookup_reply_route(900, _CHAT) == _NEW


@pytest.mark.asyncio
async def test_new_keeps_the_messages_own_formatting(db):
    # context.args is whitespace-split; the first message is taken from the
    # raw text so a multi-line message survives.
    ch = _channel(db)
    update, context = _text_update("/new first line\n  second line")

    await ch._handle_new_session(update, context)

    assert _delivered(ch).text == "first line\n  second line"


@pytest.mark.asyncio
async def test_new_as_a_reply_bypasses_routing_but_keeps_the_context(db):
    # The replied-to message belongs to a live session, where a plain reply
    # would be routed. /new <text> goes to the new session, quote included.
    ch = _channel(db, {"sess-other": {"status": "active"}})
    await ch._record_reply_route(500, _CHAT, "sess-other")
    update, context = _text_update("/new look at this instead", reply_to=500)

    await ch._handle_new_session(update, context)

    msg = _delivered(ch)
    assert msg.session_id == _NEW
    assert msg.text.startswith(f'[Reply to assistant: "{_REPLIED_TO_TEXT}"]')
    assert msg.text.endswith("look at this instead")
    ch.router.get_session.assert_not_awaited()  # no routing lookup at all
    assert REPLY_UNRESOLVED_MSG not in _replies(update)


@pytest.mark.asyncio
async def test_new_as_a_reply_to_an_unrecorded_message_is_not_refused(db):
    # A plain reply to a message nobody recorded is refused with the nudge;
    # the same reply sent as /new <text> is the way out of that refusal.
    ch = _channel(db)
    plain, _ = _text_update("start over with this", reply_to=777, message_id=901)
    await ch._handle_message(plain, None)
    plain.message.reply_text.assert_awaited_once_with(REPLY_UNRESOLVED_MSG)
    ch.router.handle_message.assert_not_awaited()

    update, context = _text_update("/new start over with this", reply_to=777)
    await ch._handle_new_session(update, context)

    assert _delivered(ch).session_id == _NEW
    assert REPLY_UNRESOLVED_MSG not in _replies(update)


@pytest.mark.asyncio
async def test_new_as_a_reply_to_a_question_card_is_not_an_answer(db):
    # A plain reply to a pending ask_user card answers it; the same card
    # replied to with /new <text> is an explicit ask for a new session, so the
    # text is delivered there as written and the question stays open.
    await db.create_notification(
        "q1", "sess-q", "question", "Deploy to staging?", options=["yes", "no"],
    )
    await db.record_notification_delivery(
        "q1", "telegram", target=str(_CHAT), message_id="500",
    )
    ch = _channel(db)
    ch._notification_service = SimpleNamespace(
        answer_delivered_notification=AsyncMock(),
    )
    update, context = _text_update("/new a new topic, not an answer", reply_to=500)

    await ch._handle_new_session(update, context)

    ch._notification_service.answer_delivered_notification.assert_not_awaited()
    msg = _delivered(ch)
    assert msg.session_id == _NEW
    assert msg.text.endswith("a new topic, not an answer")


@pytest.mark.asyncio
async def test_new_targets_the_new_session_with_routing_off_too(db):
    ch = _channel(db, routing=False)
    update, context = _text_update("/new hello", reply_to=500)

    await ch._handle_new_session(update, context)

    assert _delivered(ch).session_id == _NEW
    # Recorded with routing off too, so turning it on later covers this message.
    assert await ch._lookup_reply_route(900, _CHAT) == _NEW


@pytest.mark.asyncio
async def test_new_reports_a_failed_turn_like_a_plain_message(db):
    ch = _channel(db)
    ch.router.handle_message = AsyncMock(side_effect=RuntimeError("boom"))
    update, context = _text_update("/new hello")

    await ch._handle_new_session(update, context)

    assert _replies(update)[-1] == "Error: boom"


@pytest.mark.asyncio
async def test_new_during_a_drain_is_refused_quietly(db):
    # The engine's error event already reached the chat via the stream adapter.
    ch = _channel(db)
    ch.router.handle_message = AsyncMock(side_effect=NotAcceptingTurnsError())
    update, context = _text_update("/new hello")

    await ch._handle_new_session(update, context)

    assert _replies(update) == [
        f"Stopped session `{_PREVIOUS}`.", f"New session: `{_NEW}`",
    ]
