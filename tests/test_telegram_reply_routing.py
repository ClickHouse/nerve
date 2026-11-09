"""Tests for reply → originating-session routing (nerve.channels.telegram).

When a user replies (Telegram reply) to a message the bot sent, the reply is
routed to the session that produced that message instead of the chat's active
session. The channel records ``(chat_id, message_id) -> session_id`` in the
``channel_reply_routes`` table on every outbound send and for each inbound
message's landing session — whether or not routing is enabled, so the map is
complete when the flag is turned on and survives a restart — then resolves it
back on inbound replies, including the user replying to their own message.
A reply to a pending ``ask_user`` card is the question's answer instead.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.engine import NotAcceptingTurnsError
from nerve.channels.base import OutboundMessage
from nerve.channels.telegram import REPLY_UNRESOLVED_MSG, TelegramChannel
from nerve.config import NerveConfig
from nerve.db import Database
from nerve.db.reply_routes import REPLY_ROUTE_RETENTION_DAYS

_USER = 999
_CHAT = 42


class _FakeRouter:
    """Minimal router exposing the methods the channel calls here."""

    def __init__(
        self, db: Database, sessions: dict | None = None, active: str | None = None,
    ):
        self.engine = SimpleNamespace(db=db)
        self._sessions = sessions or {}
        self._active = active

    async def get_session(self, session_id: str):
        return self._sessions.get(session_id)

    async def get_active_session(self, channel_key: str, source: str):
        return self._active


def _make_channel(
    db: Database, router: _FakeRouter | None = None, routing_enabled: bool = True,
) -> TelegramChannel:
    cfg = SimpleNamespace(telegram=SimpleNamespace(
        allowed_users=[],
        reply_routes_to_origin_session=routing_enabled,
    ))
    return TelegramChannel(lambda: cfg, router or _FakeRouter(db))


def _bot(message_id: int) -> SimpleNamespace:
    """A bot whose ``send_message`` reports ``message_id`` for the sent message."""

    async def send_message(**kwargs):
        return SimpleNamespace(message_id=message_id)

    return SimpleNamespace(send_message=send_message)


async def _deliver_question(
    db: Database, notification_id: str, message_id: int, *,
    session_id: str = "sess-q", title: str = "Deploy to staging?",
    status: str | None = None,
) -> None:
    """A question card ``session_id`` asked, delivered to _CHAT as ``message_id``."""
    await db.create_notification(
        notification_id, session_id, "question", title,
        options=["yes", "no"], status=status,
    )
    await db.record_notification_delivery(
        notification_id, "telegram", target=str(_CHAT), message_id=str(message_id),
    )


# --------------------------------------------------------------------------- #
#  Record / lookup                                                            #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_record_and_lookup_hit(db):
    ch = _make_channel(db)
    await ch._record_reply_route(100, _CHAT, "sess-a")
    assert await ch._lookup_reply_route(100, _CHAT) == "sess-a"


@pytest.mark.asyncio
async def test_lookup_unknown_message_is_none(db):
    ch = _make_channel(db)
    assert await ch._lookup_reply_route(999, _CHAT) is None


@pytest.mark.asyncio
async def test_lookup_requires_matching_chat(db):
    # A message_id recorded for one chat must never route a reply seen in
    # another chat (message ids are only unique per chat).
    ch = _make_channel(db)
    await ch._record_reply_route(100, _CHAT, "sess-a")
    assert await ch._lookup_reply_route(100, _CHAT + 1) is None


@pytest.mark.asyncio
async def test_empty_session_is_not_recorded(db):
    ch = _make_channel(db)
    await ch._record_reply_route(100, _CHAT, "")
    assert await ch._lookup_reply_route(100, _CHAT) is None


@pytest.mark.asyncio
async def test_same_message_id_in_two_chats_do_not_collide(db):
    # Telegram message ids repeat across chats; keying on the id alone would
    # let chat B's message clobber chat A's identically-numbered one.
    ch = _make_channel(db)
    await ch._record_reply_route(100, 42, "sess-a")   # chat 42, msg 100
    await ch._record_reply_route(100, 77, "sess-b")   # chat 77, SAME msg id 100
    assert await ch._lookup_reply_route(100, 42) == "sess-a"
    assert await ch._lookup_reply_route(100, 77) == "sess-b"


@pytest.mark.asyncio
async def test_re_record_replaces_the_session(db):
    ch = _make_channel(db)
    await ch._record_reply_route(100, _CHAT, "sess-a")
    await ch._record_reply_route(100, _CHAT, "sess-b")
    assert await ch._lookup_reply_route(100, _CHAT) == "sess-b"


@pytest.mark.asyncio
async def test_routes_survive_a_restart(db):
    # The map lives in the database: a route one channel instance recorded
    # resolves in a fresh instance over the same database, i.e. after a restart.
    await _make_channel(db)._record_reply_route(100, _CHAT, "sess-a")
    assert await _make_channel(db)._lookup_reply_route(100, _CHAT) == "sess-a"


@pytest.mark.asyncio
async def test_lookup_failure_resolves_to_a_refusal(db):
    # A database error on the lookup must not drop the reply on the floor:
    # it resolves like an unknown message, so the user gets the nudge.
    ch = _make_channel(db)
    ch.router.engine.db = SimpleNamespace(
        lookup_reply_route=AsyncMock(side_effect=RuntimeError("db unavailable")),
        find_notification_for_delivery=AsyncMock(),
    )
    assert await ch._resolve_reply(100, _CHAT) == ("reject", None)


@pytest.mark.asyncio
async def test_recording_happens_with_routing_disabled(db):
    # The flag gates routing, not recording: a map that only filled while the
    # flag was on would start empty the day it is turned on.
    ch = _make_channel(db, routing_enabled=False)
    await ch._record_reply_route(100, _CHAT, "sess-a")
    assert await ch._lookup_reply_route(100, _CHAT) == "sess-a"


@pytest.mark.asyncio
async def test_lookup_falls_back_to_the_notification_delivery(db):
    # A card delivered before routes were stored has no route row, but its
    # delivery record still names the session that raised it.
    ch = _make_channel(db)
    await _deliver_question(db, "n-1", 500, session_id="sess-q")
    assert await ch._lookup_reply_route(500, _CHAT) == "sess-q"
    assert await ch._lookup_reply_route(500, _CHAT + 1) is None


@pytest.mark.asyncio
async def test_a_failed_record_is_logged_not_raised(db, caplog):
    # The route is auxiliary to the message it rides on: a failed write must
    # not abort the send or drop the inbound message.
    ch = _make_channel(db)
    ch.router.engine.db = MagicMock()
    ch.router.engine.db.record_reply_route = AsyncMock(side_effect=RuntimeError("locked"))
    await ch._record_reply_route(100, _CHAT, "sess-a")
    assert "Could not record reply route" in caplog.text


# --------------------------------------------------------------------------- #
#  Store: retention sweep and delivery lookup                                 #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_cleanup_keeps_fresh_routes_and_removes_old_ones(db):
    await db.record_reply_route("telegram", str(_CHAT), "100", "sess-fresh")
    stale = datetime.now(timezone.utc) - timedelta(days=REPLY_ROUTE_RETENTION_DAYS + 1)
    await db._write(
        "INSERT INTO channel_reply_routes "
        "(channel, target, message_id, session_id, created_at) VALUES (?, ?, ?, ?, ?)",
        ("telegram", str(_CHAT), "1", "sess-stale", stale.isoformat()),
    )

    assert await db.cleanup_old_reply_routes() == 1

    assert await db.lookup_reply_route("telegram", str(_CHAT), "1") is None
    assert await db.lookup_reply_route("telegram", str(_CHAT), "100") == "sess-fresh"


@pytest.mark.asyncio
async def test_find_notification_for_delivery_matches_one_message(db):
    await _deliver_question(db, "n-1", 500, title="First?")
    await _deliver_question(db, "n-2", 501, title="Second?")
    # A delivery whose message id was not recorded.
    await db.create_notification("n-3", "sess-q", "notify", "No id")
    await db.record_notification_delivery("n-3", "telegram", target=str(_CHAT))

    found = await db.find_notification_for_delivery("telegram", str(_CHAT), "501")
    assert found["id"] == "n-2"
    assert found["title"] == "Second?"
    assert await db.find_notification_for_delivery("telegram", str(_CHAT), "502") is None
    assert await db.find_notification_for_delivery("telegram", "43", "501") is None
    assert await db.find_notification_for_delivery("telegram", str(_CHAT), "") is None


# --------------------------------------------------------------------------- #
#  Resolve — three-way: active / route / reject                               #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_resolve_active_when_not_a_reply(db):
    ch = _make_channel(db, _FakeRouter(db, {"sess-a": {"status": "active"}}))
    await ch._record_reply_route(100, _CHAT, "sess-a")
    assert await ch._resolve_reply(None, _CHAT) == ("active", None)


@pytest.mark.asyncio
async def test_resolve_routes_to_live_session(db):
    ch = _make_channel(db, _FakeRouter(db, {"sess-a": {"status": "active"}}))
    await ch._record_reply_route(100, _CHAT, "sess-a")
    assert await ch._resolve_reply(100, _CHAT) == ("route", "sess-a")


@pytest.mark.asyncio
async def test_resolve_rejects_when_session_gone(db):
    # Mapping exists but the session no longer exists → reject, never mis-route.
    ch = _make_channel(db, _FakeRouter(db, {}))
    await ch._record_reply_route(100, _CHAT, "sess-a")
    assert await ch._resolve_reply(100, _CHAT) == ("reject", None)


@pytest.mark.asyncio
async def test_resolve_rejects_when_session_archived(db):
    ch = _make_channel(db, _FakeRouter(db, {"sess-a": {"status": "archived"}}))
    await ch._record_reply_route(100, _CHAT, "sess-a")
    assert await ch._resolve_reply(100, _CHAT) == ("reject", None)


@pytest.mark.asyncio
async def test_resolve_rejects_unmapped_reply(db):
    ch = _make_channel(db, _FakeRouter(db, {"sess-a": {"status": "active"}}))
    # Replying to a message that was never recorded (or whose route was pruned).
    assert await ch._resolve_reply(555, _CHAT) == ("reject", None)


@pytest.mark.asyncio
async def test_resolve_rejects_on_chat_mismatch(db):
    ch = _make_channel(db, _FakeRouter(db, {"sess-a": {"status": "active"}}))
    await ch._record_reply_route(100, _CHAT, "sess-a")  # recorded for chat 42
    assert await ch._resolve_reply(100, _CHAT + 1) == ("reject", None)  # replied in chat 43


# --------------------------------------------------------------------------- #
#  Outbound send() records the route                                          #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
@pytest.mark.parametrize("routing_enabled", [True, False])
async def test_send_records_reply_route(db, routing_enabled):
    ch = _make_channel(db, routing_enabled=routing_enabled)
    ch._app = SimpleNamespace(bot=_bot(777))
    await ch.send(OutboundMessage(target=str(_CHAT), text="hello", session_id="sess-x"))
    assert await ch._lookup_reply_route(777, _CHAT) == "sess-x"


@pytest.mark.asyncio
async def test_send_without_session_id_records_nothing(db):
    ch = _make_channel(db)
    ch._app = SimpleNamespace(bot=_bot(778))
    await ch.send(OutboundMessage(target=str(_CHAT), text="hello"))  # session_id=""
    assert await ch._lookup_reply_route(778, _CHAT) is None


# --------------------------------------------------------------------------- #
#  Delivery session + recording the user's own messages                       #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_delivery_session_uses_routed_target(db):
    ch = _make_channel(db, _FakeRouter(db, active="active1"))
    assert await ch._delivery_session("sess-b", f"telegram:{_CHAT}") == "sess-b"


@pytest.mark.asyncio
async def test_delivery_session_falls_back_to_active(db):
    ch = _make_channel(db, _FakeRouter(db, active="active1"))
    assert await ch._delivery_session(None, f"telegram:{_CHAT}") == "active1"


@pytest.mark.asyncio
async def test_reply_to_own_recorded_message_routes_to_where_it_landed(db):
    # Recording the user's own inbound message against its landing session means
    # a later reply to that same message routes back there.
    ch = _make_channel(
        db, _FakeRouter(db, {"active1": {"status": "active"}}, active="active1"),
    )
    landing = await ch._delivery_session(None, f"telegram:{_CHAT}")  # → active1
    await ch._record_reply_route(500, _CHAT, landing)                 # user's own msg id 500
    assert await ch._resolve_reply(500, _CHAT) == ("route", "active1")


# --------------------------------------------------------------------------- #
#  Feature flag: telegram.reply_routes_to_origin_session (default off)         #
# --------------------------------------------------------------------------- #

def test_routing_enabled_reflects_config():
    db = MagicMock()
    assert _make_channel(db, routing_enabled=True)._reply_routing_enabled() is True
    assert _make_channel(db, routing_enabled=False)._reply_routing_enabled() is False


# --------------------------------------------------------------------------- #
#  Handler: _handle_message                                                   #
# --------------------------------------------------------------------------- #

def _handler_channel(
    db: Database, sessions: dict, active: str = "active1", routing_enabled: bool = True,
) -> TelegramChannel:
    """A channel whose live branch runs to ``router.handle_message``.

    Attachment extraction is stubbed out; everything from authorization to the
    question intercept, the reply-routing gate and the delivery call is the
    production code, over a real database.
    """
    cfg = NerveConfig()
    cfg.telegram.allowed_users = [_USER]
    cfg.telegram.reply_routes_to_origin_session = routing_enabled
    router = MagicMock()
    router.engine.db = db
    router.observe = AsyncMock(return_value=True)
    router.handle_message = AsyncMock()
    router.get_session = AsyncMock(side_effect=lambda sid: sessions.get(sid))
    router.get_active_session = AsyncMock(return_value=active)
    ch = TelegramChannel(lambda: cfg, router)
    ch._extract_image = AsyncMock(return_value=None)
    ch._extract_sticker = AsyncMock(return_value=(None, None))
    ch._extract_document = AsyncMock(return_value=([], None))
    return ch


def _reply_update(reply_to_id: int | None, message_id: int = 900):
    """A private-chat text message, replying to ``reply_to_id`` if given."""
    update = MagicMock()
    update.effective_chat.id = _CHAT
    update.effective_chat.type = "private"
    update.effective_user.id = _USER
    message = update.message
    message.text = "yes, go ahead"
    message.caption = None
    message.message_id = message_id
    message.media_group_id = None
    message.forward_origin = None
    message.quote = None
    message.reply_text = AsyncMock()
    if reply_to_id is None:
        message.reply_to_message = None
    else:
        reply = MagicMock()
        reply.message_id = reply_to_id
        reply.text = "Cron finished: 3 PRs need a look."
        reply.caption = None
        reply.from_user = None
        message.reply_to_message = reply
    return update


def _question_service(**answer_outcome) -> MagicMock:
    """A notification service whose ``answer_delivered_notification`` is scripted."""
    service = MagicMock()
    service.answer_delivered_notification = AsyncMock(**answer_outcome)
    return service


@pytest.mark.asyncio
async def test_handler_routes_a_reply_to_the_session_that_sent_it(db):
    ch = _handler_channel(db, {"cron-a": {"status": "active"}})
    await ch._record_reply_route(500, _CHAT, "cron-a")

    update = _reply_update(500)
    await ch._handle_message(update, None)

    ch.router.handle_message.assert_awaited_once()
    assert ch.router.handle_message.await_args.args[0].session_id == "cron-a"
    update.message.reply_text.assert_not_awaited()
    # A later reply to this inbound message lands in the same session.
    assert await ch._lookup_reply_route(900, _CHAT) == "cron-a"


@pytest.mark.asyncio
async def test_handler_routes_a_reply_after_a_restart(db):
    # Recorded by the instance that sent the message, resolved by a fresh
    # instance over the same database: the restart in between loses nothing.
    before = _make_channel(db)
    before._app = SimpleNamespace(bot=_bot(777))
    await before.send(OutboundMessage(target=str(_CHAT), text="done", session_id="cron-a"))

    after = _handler_channel(db, {"cron-a": {"status": "active"}})
    await after._handle_message(_reply_update(777), None)

    after.router.handle_message.assert_awaited_once()
    assert after.router.handle_message.await_args.args[0].session_id == "cron-a"


@pytest.mark.asyncio
@pytest.mark.parametrize("sessions", [{}, {"cron-a": {"status": "archived"}}])
async def test_handler_rejects_an_unresolved_reply_without_delivering(db, sessions):
    # Unrecorded message or archived session: nudge, never the active session.
    ch = _handler_channel(db, sessions)
    if sessions:
        await ch._record_reply_route(500, _CHAT, "cron-a")

    update = _reply_update(500)
    await ch._handle_message(update, None)

    ch.router.handle_message.assert_not_awaited()
    update.message.reply_text.assert_awaited_once_with(REPLY_UNRESOLVED_MSG)
    assert await ch._lookup_reply_route(900, _CHAT) is None


@pytest.mark.asyncio
async def test_handler_delivers_a_plain_message_to_the_active_session(db):
    ch = _handler_channel(db, {})

    await ch._handle_message(_reply_update(None), None)

    ch.router.handle_message.assert_awaited_once()
    assert ch.router.handle_message.await_args.args[0].session_id == "active1"
    assert await ch._lookup_reply_route(900, _CHAT) == "active1"


@pytest.mark.asyncio
async def test_handler_records_the_landing_session_with_routing_disabled(db):
    # Off: the reply is context-only and the router picks the active session
    # (session_id None) — but the landing session is still recorded, so the
    # map is complete the day routing is turned on.
    ch = _handler_channel(db, {}, routing_enabled=False)
    await ch._record_reply_route(500, _CHAT, "cron-a")  # would route when on

    await ch._handle_message(_reply_update(500), None)

    ch.router.handle_message.assert_awaited_once()
    assert ch.router.handle_message.await_args.args[0].session_id is None
    assert await ch._lookup_reply_route(900, _CHAT) == "active1"


@pytest.mark.asyncio
async def test_a_reply_to_a_sent_file_routes_to_its_session(db, tmp_path):
    ch = _handler_channel(db, {"sess-f": {"status": "active"}})
    doc = tmp_path / "report.md"
    doc.write_text("# report\n")
    ch._app = MagicMock()
    ch._app.bot.send_document = AsyncMock(return_value=SimpleNamespace(message_id=700))

    assert await ch.send_file(str(_CHAT), str(doc), session_id="sess-f") is True

    await ch._handle_message(_reply_update(700), None)
    assert ch.router.handle_message.await_args.args[0].session_id == "sess-f"


@pytest.mark.asyncio
async def test_router_send_file_passes_the_session_to_the_channel(tmp_path):
    from nerve.channels.base import ChannelCapability
    from nerve.channels.router import ChannelRouter

    channel = MagicMock()
    channel.capabilities = {ChannelCapability.SEND_FILES}
    channel.send_file = AsyncMock(return_value=True)
    router = ChannelRouter.__new__(ChannelRouter)
    router._channels = {"telegram": channel}
    router._message_context = {
        "sess-f": {"channel_name": "telegram", "target": str(_CHAT)},
    }

    assert await router.send_file("sess-f", str(tmp_path / "f"), channel="telegram")
    channel.send_file.assert_awaited_once_with(
        str(_CHAT), str(tmp_path / "f"), session_id="sess-f",
    )


# --------------------------------------------------------------------------- #
#  Handler: a reply to a pending question card answers it                      #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
@pytest.mark.parametrize("routing_enabled", [True, False])
async def test_handler_answers_a_pending_question_by_reply(db, routing_enabled):
    ch = _handler_channel(db, {}, routing_enabled=routing_enabled)
    await _deliver_question(db, "n-1", 500)
    ch._notification_service = _question_service(
        return_value={"id": "n-1", "title": "Deploy to staging?", "status": "answered"},
    )

    update = _reply_update(500)
    await ch._handle_message(update, None)

    ch._notification_service.answer_delivered_notification.assert_awaited_once_with(
        "n-1", "yes, go ahead", channel="telegram", target=str(_CHAT), actor=str(_USER),
    )
    update.message.reply_text.assert_awaited_once_with(
        "Answer recorded for: Deploy to staging?",
    )
    ch.router.handle_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_handler_answers_a_question_by_reply_in_a_delivery_only_sink(db):
    # The notifications group can be made one-way: a message there never
    # starts a turn. A reply to a card delivered there is still its answer,
    # as a button tap or /reply would be.
    ch = _handler_channel(db, {})
    ch.config.notifications.delivery_only_sink = True
    ch.config.notifications.telegram_chat_id = _CHAT
    await _deliver_question(db, "n-1", 500)
    ch._notification_service = _question_service(
        return_value={"id": "n-1", "title": "Deploy to staging?", "status": "answered"},
    )

    plain = _reply_update(None, message_id=901)
    plain.effective_chat.type = "supergroup"
    await ch._handle_message(plain, None)
    ch.router.handle_message.assert_not_awaited()
    plain.message.reply_text.assert_not_awaited()

    update = _reply_update(500)
    update.effective_chat.type = "supergroup"
    await ch._handle_message(update, None)

    ch._notification_service.answer_delivered_notification.assert_awaited_once_with(
        "n-1", "yes, go ahead", channel="telegram", target=str(_CHAT), actor=str(_USER),
    )
    update.message.reply_text.assert_awaited_once_with(
        "Answer recorded for: Deploy to staging?",
    )
    ch.router.handle_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_handler_falls_through_when_the_question_was_already_answered(db):
    ch = _handler_channel(db, {"sess-q": {"status": "active"}})
    await _deliver_question(db, "n-1", 500, status="answered")
    ch._notification_service = _question_service()

    update = _reply_update(500)
    await ch._handle_message(update, None)

    ch._notification_service.answer_delivered_notification.assert_not_awaited()
    # Normal handling: the reply routes to the session that asked, found
    # through the card's delivery record.
    ch.router.handle_message.assert_awaited_once()
    assert ch.router.handle_message.await_args.args[0].session_id == "sess-q"
    update.message.reply_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_handler_falls_through_when_the_answer_is_not_accepted(db):
    # Pending when looked up, answered (or expired) by the time the service
    # ran: the message takes the normal path rather than vanishing.
    ch = _handler_channel(db, {"sess-q": {"status": "active"}})
    await _deliver_question(db, "n-1", 500)
    ch._notification_service = _question_service(return_value=None)

    update = _reply_update(500)
    await ch._handle_message(update, None)

    ch.router.handle_message.assert_awaited_once()
    assert ch.router.handle_message.await_args.args[0].session_id == "sess-q"
    update.message.reply_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_handler_keeps_the_question_pending_while_draining(db):
    ch = _handler_channel(db, {})
    await _deliver_question(db, "n-1", 500)
    ch._notification_service = _question_service(side_effect=NotAcceptingTurnsError())

    update = _reply_update(500)
    await ch._handle_message(update, None)

    update.message.reply_text.assert_awaited_once_with(str(NotAcceptingTurnsError()))
    ch.router.handle_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_handler_does_not_answer_a_notify_card(db):
    # Only questions take answers; a reply to a plain notification is a
    # message for the session that sent it.
    ch = _handler_channel(db, {"sess-n": {"status": "active"}})
    await db.create_notification("n-1", "sess-n", "notify", "Build finished")
    await db.record_notification_delivery(
        "n-1", "telegram", target=str(_CHAT), message_id="500",
    )
    ch._notification_service = _question_service()

    await ch._handle_message(_reply_update(500), None)

    ch._notification_service.answer_delivered_notification.assert_not_awaited()
    assert ch.router.handle_message.await_args.args[0].session_id == "sess-n"


@pytest.mark.asyncio
async def test_handler_without_a_service_treats_the_reply_as_a_message(db):
    ch = _handler_channel(db, {"sess-q": {"status": "active"}})
    await _deliver_question(db, "n-1", 500)

    await ch._handle_message(_reply_update(500), None)

    ch.router.handle_message.assert_awaited_once()
    assert ch.router.handle_message.await_args.args[0].session_id == "sess-q"


@pytest.mark.asyncio
async def test_an_album_replying_to_a_question_is_not_an_answer(db):
    ch = _handler_channel(db, {"sess-q": {"status": "active"}})
    await _deliver_question(db, "n-1", 500)
    ch._notification_service = _question_service()
    first, second = _reply_update(500, message_id=901), _reply_update(500, message_id=902)
    for part in (first, second):
        part.message.media_group_id = "album-1"
        part.message.text = None
    first.message.caption = "two screenshots"
    ch._media_groups["album-1"] = [first, second]

    await ch._process_media_group("album-1")

    ch._notification_service.answer_delivered_notification.assert_not_awaited()
    ch.router.handle_message.assert_awaited_once()
    assert ch.router.handle_message.await_args.args[0].session_id == "sess-q"
    assert await ch._lookup_reply_route(901, _CHAT) == "sess-q"
