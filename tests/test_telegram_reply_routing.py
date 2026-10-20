"""Tests for reply → originating-session routing (nerve.channels.telegram).

When a user replies (Telegram reply) to a message the bot sent, the reply is
routed to the session that produced that message instead of the chat's active
session. The channel records ``message_id -> (chat_id, session_id)`` on every
outbound send and for each inbound message's landing session, then resolves it
back on inbound replies — including the user replying to their own message.
"""

from types import SimpleNamespace

import pytest

from nerve.channels.base import OutboundMessage
from nerve.channels.telegram import TelegramChannel


class _FakeRouter:
    """Minimal router exposing the methods the channel calls here."""

    def __init__(self, sessions: dict | None = None, active: str | None = None):
        self._sessions = sessions or {}
        self._active = active

    async def get_session(self, session_id: str):
        return self._sessions.get(session_id)

    async def get_active_session(self, channel_key: str, source: str):
        return self._active


def _make_channel(
    router: _FakeRouter | None = None, routing_enabled: bool = True,
) -> TelegramChannel:
    cfg = SimpleNamespace(telegram=SimpleNamespace(
        allowed_users=[],
        reply_routes_to_origin_session=routing_enabled,
    ))
    return TelegramChannel(lambda: cfg, router or _FakeRouter())


# --------------------------------------------------------------------------- #
#  Record / lookup                                                            #
# --------------------------------------------------------------------------- #

def test_record_and_lookup_hit():
    ch = _make_channel()
    ch._record_reply_route(100, 42, "sessA")
    assert ch._lookup_reply_route(100, 42) == "sessA"


def test_lookup_unknown_message_is_none():
    ch = _make_channel()
    assert ch._lookup_reply_route(999, 42) is None


def test_lookup_requires_matching_chat():
    # A message_id recorded for chat 42 must never route a reply seen in
    # another chat (message ids are only unique per chat).
    ch = _make_channel()
    ch._record_reply_route(100, 42, "sessA")
    assert ch._lookup_reply_route(100, 43) is None


def test_empty_session_is_not_recorded():
    ch = _make_channel()
    ch._record_reply_route(100, 42, "")
    assert ch._lookup_reply_route(100, 42) is None


def test_same_message_id_in_two_chats_do_not_collide():
    # Telegram message ids repeat across chats; keying on the id alone would
    # let chat B's message clobber chat A's identically-numbered one.
    ch = _make_channel()
    ch._record_reply_route(100, 42, "sessA")   # chat 42, msg 100
    ch._record_reply_route(100, 77, "sessB")   # chat 77, SAME msg id 100
    assert ch._lookup_reply_route(100, 42) == "sessA"
    assert ch._lookup_reply_route(100, 77) == "sessB"


def test_reply_routes_are_lru_bounded():
    ch = _make_channel()
    ch._reply_routes_max = 3
    for mid in range(1, 6):  # 1..5, cap 3 → 1 and 2 evicted
        ch._record_reply_route(mid, 42, f"s{mid}")
    assert ch._lookup_reply_route(1, 42) is None
    assert ch._lookup_reply_route(2, 42) is None
    assert ch._lookup_reply_route(3, 42) == "s3"
    assert ch._lookup_reply_route(5, 42) == "s5"
    assert len(ch._reply_routes) == 3


def test_re_record_refreshes_lru_recency():
    ch = _make_channel()
    ch._reply_routes_max = 2
    ch._record_reply_route(1, 42, "s1")
    ch._record_reply_route(2, 42, "s2")
    ch._record_reply_route(1, 42, "s1")  # touch 1 → 2 is now oldest
    ch._record_reply_route(3, 42, "s3")  # evicts 2
    assert ch._lookup_reply_route(2, 42) is None
    assert ch._lookup_reply_route(1, 42) == "s1"
    assert ch._lookup_reply_route(3, 42) == "s3"


# --------------------------------------------------------------------------- #
#  Resolve — three-way: active / route / reject                               #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_resolve_active_when_not_a_reply():
    ch = _make_channel(_FakeRouter({"sessA": {"status": "active"}}))
    ch._record_reply_route(100, 42, "sessA")
    assert await ch._resolve_reply(None, 42) == ("active", None)


@pytest.mark.asyncio
async def test_resolve_routes_to_live_session():
    ch = _make_channel(_FakeRouter({"sessA": {"status": "active"}}))
    ch._record_reply_route(100, 42, "sessA")
    assert await ch._resolve_reply(100, 42) == ("route", "sessA")


@pytest.mark.asyncio
async def test_resolve_rejects_when_session_gone():
    # Mapping exists but the session no longer exists → reject, never mis-route.
    ch = _make_channel(_FakeRouter({}))
    ch._record_reply_route(100, 42, "sessA")
    assert await ch._resolve_reply(100, 42) == ("reject", None)


@pytest.mark.asyncio
async def test_resolve_rejects_when_session_archived():
    ch = _make_channel(_FakeRouter({"sessA": {"status": "archived"}}))
    ch._record_reply_route(100, 42, "sessA")
    assert await ch._resolve_reply(100, 42) == ("reject", None)


@pytest.mark.asyncio
async def test_resolve_rejects_unmapped_reply():
    ch = _make_channel(_FakeRouter({"sessA": {"status": "active"}}))
    # Replying to a message the bot never recorded (user's own / LRU-evicted).
    assert await ch._resolve_reply(555, 42) == ("reject", None)


@pytest.mark.asyncio
async def test_resolve_rejects_on_chat_mismatch():
    ch = _make_channel(_FakeRouter({"sessA": {"status": "active"}}))
    ch._record_reply_route(100, 42, "sessA")  # recorded for chat 42
    assert await ch._resolve_reply(100, 43) == ("reject", None)  # replied in chat 43


# --------------------------------------------------------------------------- #
#  Outbound send() records the route                                          #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_send_records_reply_route():
    ch = _make_channel()

    class _Bot:
        async def send_message(self, **kwargs):
            return SimpleNamespace(message_id=777)

    ch._app = SimpleNamespace(bot=_Bot())
    await ch.send(OutboundMessage(target="42", text="hello", session_id="sX"))
    assert ch._lookup_reply_route(777, 42) == "sX"


@pytest.mark.asyncio
async def test_send_without_session_id_records_nothing():
    ch = _make_channel()

    class _Bot:
        async def send_message(self, **kwargs):
            return SimpleNamespace(message_id=778)

    ch._app = SimpleNamespace(bot=_Bot())
    await ch.send(OutboundMessage(target="42", text="hello"))  # session_id=""
    assert ch._lookup_reply_route(778, 42) is None


# --------------------------------------------------------------------------- #
#  Delivery session + recording the user's own messages                       #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_delivery_session_uses_routed_target():
    ch = _make_channel(_FakeRouter(active="active1"))
    assert await ch._delivery_session("sessB", "telegram:42") == "sessB"


@pytest.mark.asyncio
async def test_delivery_session_falls_back_to_active():
    ch = _make_channel(_FakeRouter(active="active1"))
    assert await ch._delivery_session(None, "telegram:42") == "active1"


@pytest.mark.asyncio
async def test_reply_to_own_recorded_message_routes_to_where_it_landed():
    # Recording the user's own inbound message against its landing session means
    # a later reply to that same message routes back there (serxa's request).
    ch = _make_channel(_FakeRouter({"active1": {"status": "active"}}, active="active1"))
    landing = await ch._delivery_session(None, "telegram:42")  # → active1
    ch._record_reply_route(500, 42, landing)                   # user's own msg id 500
    assert await ch._resolve_reply(500, 42) == ("route", "active1")


# --------------------------------------------------------------------------- #
#  Feature flag: telegram.reply_routes_to_origin_session (default off)         #
# --------------------------------------------------------------------------- #

def test_routing_enabled_reflects_config():
    assert _make_channel(routing_enabled=True)._reply_routing_enabled() is True
    assert _make_channel(routing_enabled=False)._reply_routing_enabled() is False


def test_recording_is_skipped_when_routing_disabled():
    # With the flag off, nothing is recorded — so no message is ever a route
    # target and replies fall through to the active session (legacy behavior).
    ch = _make_channel(routing_enabled=False)
    ch._record_reply_route(100, 42, "sessA")
    assert ch._lookup_reply_route(100, 42) is None


# --------------------------------------------------------------------------- #
#  Handler: _handle_message with the flag on                                  #
# --------------------------------------------------------------------------- #

from unittest.mock import AsyncMock, MagicMock

from nerve.channels.telegram import REPLY_UNRESOLVED_MSG
from nerve.config import NerveConfig

_USER = 999
_CHAT = 42


def _handler_channel(sessions: dict, active: str = "active1") -> TelegramChannel:
    """A channel whose live branch runs to ``router.handle_message``.

    Attachment extraction is stubbed out; everything from authorization to the
    reply-routing gate and the delivery call is the production code.
    """
    cfg = NerveConfig()
    cfg.telegram.allowed_users = [_USER]
    cfg.telegram.reply_routes_to_origin_session = True
    router = MagicMock()
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


@pytest.mark.asyncio
async def test_handler_routes_a_reply_to_the_session_that_sent_it():
    ch = _handler_channel({"cronA": {"status": "active"}})
    ch._record_reply_route(500, _CHAT, "cronA")

    update = _reply_update(500)
    await ch._handle_message(update, None)

    ch.router.handle_message.assert_awaited_once()
    assert ch.router.handle_message.await_args.args[0].session_id == "cronA"
    update.message.reply_text.assert_not_awaited()
    # A later reply to this inbound message lands in the same session.
    assert ch._lookup_reply_route(900, _CHAT) == "cronA"


@pytest.mark.asyncio
@pytest.mark.parametrize("sessions", [{}, {"cronA": {"status": "archived"}}])
async def test_handler_rejects_an_unresolved_reply_without_delivering(sessions):
    # Unrecorded message or archived session: nudge, never the active session.
    ch = _handler_channel(sessions)
    if sessions:
        ch._record_reply_route(500, _CHAT, "cronA")

    update = _reply_update(500)
    await ch._handle_message(update, None)

    ch.router.handle_message.assert_not_awaited()
    update.message.reply_text.assert_awaited_once_with(REPLY_UNRESOLVED_MSG)
    assert ch._lookup_reply_route(900, _CHAT) is None


@pytest.mark.asyncio
async def test_handler_delivers_a_plain_message_to_the_active_session():
    ch = _handler_channel({})

    await ch._handle_message(_reply_update(None), None)

    ch.router.handle_message.assert_awaited_once()
    assert ch.router.handle_message.await_args.args[0].session_id == "active1"
    assert ch._lookup_reply_route(900, _CHAT) == "active1"


@pytest.mark.asyncio
async def test_a_reply_to_a_sent_file_routes_to_its_session(tmp_path):
    ch = _handler_channel({"sessF": {"status": "active"}})
    doc = tmp_path / "report.md"
    doc.write_text("# report\n")
    ch._app = MagicMock()
    ch._app.bot.send_document = AsyncMock(return_value=SimpleNamespace(message_id=700))

    assert await ch.send_file(str(_CHAT), str(doc), session_id="sessF") is True

    await ch._handle_message(_reply_update(700), None)
    assert ch.router.handle_message.await_args.args[0].session_id == "sessF"


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
        "sessF": {"channel_name": "telegram", "target": str(_CHAT)},
    }

    assert await router.send_file("sessF", str(tmp_path / "f"), channel="telegram")
    channel.send_file.assert_awaited_once_with(
        str(_CHAT), str(tmp_path / "f"), session_id="sessF",
    )
