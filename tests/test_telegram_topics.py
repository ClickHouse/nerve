"""Telegram forum topics as sessions: one topic is one session.

A message in a topic routes to ``telegram:<chat>:<thread>`` and to the session
that topic is bound to; the chat itself (and a forum's General topic) keeps
``telegram:<chat>`` and today's routing. Everything sent back carries the
topic, and reactions find their topic through the message cache.
"""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram import (
    Chat,
    ForumTopicCreated,
    Message,
    MessageReactionUpdated,
    ReactionTypeEmoji,
    Update,
    User,
)
from telegram.error import RetryAfter

from nerve.channels.base import OutboundMessage
from nerve.channels.telegram import TelegramChannel, format_target, parse_target
from nerve.config import ChannelSourceConfig, NerveConfig, TelegramConfig, TelegramTopicsConfig

OWNER = 4242
CHAT = -1001234567890  # forum supergroup
TOPIC = 42


class _Bot:
    """Records every Bot API call. ``bot`` stands in for the cached getMe user."""

    defaults = None  # read by Message.reply_text

    def __init__(self, dm_topics: bool = False):
        self.calls: list[tuple[str, dict]] = []
        self.bot = SimpleNamespace(has_topics_enabled=dm_topics)
        self.fail_next: list[Exception] = []
        self._next_id = 100

    def __getattr__(self, name):
        async def call(*args, **kwargs):
            self.calls.append((name, kwargs))
            if self.fail_next:
                raise self.fail_next.pop(0)
            self._next_id += 1
            return SimpleNamespace(message_id=self._next_id)
        return call

    def named(self, name):
        return [kwargs for called, kwargs in self.calls if called == name]


class _Router:
    """The router surface the channel uses, with channel_sessions as a dict."""

    def __init__(self):
        self.bound: dict[str, str] = {}
        self.handled = []
        self.created: list[str] = []
        self.stopped: list[str] = []
        self.last_session_keys: list[str] = []
        self.active_session_keys: list[str] = []
        self.observed = []
        self._n = 0

    async def get_last_session(self, key):
        self.last_session_keys.append(key)
        return self.bound.get(key)

    async def get_active_session(self, key, source):
        self.active_session_keys.append(key)
        return self.bound.setdefault(key, "rolled")

    async def create_session(self, key, title=None, source="web"):
        await asyncio.sleep(0)  # the store awaits I/O here, so a race can show
        self._n += 1
        sid = f"s{self._n}"
        self.bound[key] = sid
        self.created.append(key)
        return sid

    async def switch_session(self, key, sid):
        self.bound[key] = sid

    async def stop_session(self, sid):
        self.stopped.append(sid)
        return True

    async def handle_message(self, msg):
        self.handled.append(msg)
        return "ok"

    async def list_interactive_sessions(self, limit, offset=0, current_id=None):
        return []

    async def get_session(self, sid):
        return None

    async def observe(self, observed, **_kwargs):
        self.observed.append(observed)
        return True


def _channel(router=None, *, topics=True, dm_topics=False, source=None):
    cfg = NerveConfig()
    cfg.telegram.allowed_users = [OWNER]
    cfg.telegram.topics = TelegramTopicsConfig(enabled=topics)
    if source is not None:
        cfg.telegram.source = source
    ch = TelegramChannel(lambda: cfg, router=router or _Router())
    ch._app = SimpleNamespace(bot=_Bot(dm_topics=dm_topics))
    return ch


def _chat(chat_id=CHAT, chat_type="supergroup", is_forum=True):
    if chat_type == "private":
        return Chat(id=chat_id, type=chat_type)
    return Chat(id=chat_id, type=chat_type, is_forum=is_forum or None)


def _message(
    chat, *, thread=None, topic=None, text="hello", message_id=7, user_id=OWNER,
    bot=None, reply_to=None,
):
    msg = Message(
        message_id=message_id,
        date=datetime.now(timezone.utc),
        chat=chat,
        from_user=User(id=user_id, first_name="Seva", is_bot=False),
        text=text,
        message_thread_id=thread,
        is_topic_message=topic,
        reply_to_message=reply_to,
    )
    if bot is not None:
        msg.set_bot(bot)
    return msg


def _update(chat=None, **kwargs):
    return Update(update_id=1, message=_message(chat or _chat(), **kwargs))


def _reaction(chat=None, message_id=7, emoji="👍"):
    return Update(update_id=2, message_reaction=MessageReactionUpdated(
        chat=chat or _chat(),
        message_id=message_id,
        date=datetime.now(timezone.utc),
        old_reaction=(),
        new_reaction=(ReactionTypeEmoji(emoji),),
        user=User(id=OWNER, first_name="Seva", is_bot=False),
    ))


# --------------------------------------------------------------------------- #
#  Targets                                                                     #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("chat_id, thread_id, target", [
    (OWNER, None, "4242"),
    (CHAT, None, "-1001234567890"),
    (CHAT, TOPIC, "-1001234567890:42"),
    (OWNER, 7, "4242:7"),
])
def test_targets_round_trip(chat_id, thread_id, target):
    assert format_target(chat_id, thread_id) == target
    assert parse_target(target) == (chat_id, thread_id)


# --------------------------------------------------------------------------- #
#  Which messages are in a topic                                               #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("chat, thread, topic, expected", [
    (_chat(), TOPIC, True, TOPIC),                              # forum topic
    (_chat(OWNER, "private"), TOPIC, True, TOPIC),              # topic in a DM
    (_chat(), 1, True, None),                                   # General is the chat
    (_chat(), None, None, None),                                # no topic at all
    # A reply in a plain group carries message_thread_id too: the id of the
    # message it replies to. It is not a topic, and must not split the group.
    (_chat(-100, "supergroup", is_forum=False), 99, False, None),
    (_chat(-100, "group", is_forum=False), 99, None, None),
])
def test_topic_of(chat, thread, topic, expected):
    ch = _channel()
    assert ch._topic_of(_message(chat, thread=thread, topic=topic)) == expected


def test_topic_of_with_topics_off():
    ch = _channel(topics=False)
    assert ch._topic_of(_message(_chat(), thread=TOPIC, topic=True)) is None


def test_topic_of_never_reads_a_mock_as_a_topic():
    from unittest.mock import MagicMock
    assert _channel()._topic_of(MagicMock()) is None


# --------------------------------------------------------------------------- #
#  Inbound routing and binding                                                 #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_topic_message_routes_to_the_topic_session():
    router = _Router()
    ch = _channel(router)
    await ch._handle_message(_update(thread=TOPIC, topic=True), None)

    key = f"telegram:{CHAT}:{TOPIC}"
    assert router.created == [key]
    (msg,) = router.handled
    assert msg.channel_key == key
    assert msg.sender_id == f"{CHAT}:{TOPIC}"
    assert msg.session_id == router.bound[key]


def _topic_root(chat=None, thread=TOPIC):
    """The service message that created a topic. Telegram hands it to every
    message in the topic as ``reply_to_message``, reply or not."""
    return Message(
        message_id=thread,
        date=datetime.now(timezone.utc),
        chat=chat or _chat(),
        from_user=User(id=OWNER, first_name="Seva", is_bot=False),
        forum_topic_created=ForumTopicCreated(name="Taxes", icon_color=7322096),
    )


@pytest.mark.asyncio
async def test_the_topic_root_is_not_a_reply():
    router = _Router()
    ch = _channel(router)
    await ch._handle_message(
        _update(thread=TOPIC, topic=True, reply_to=_topic_root()), None,
    )

    assert router.handled[0].text == "hello"


@pytest.mark.asyncio
async def test_a_real_reply_inside_a_topic_keeps_its_context():
    router = _Router()
    ch = _channel(router)
    earlier = _message(_chat(), thread=TOPIC, topic=True, text="earlier", message_id=50)
    await ch._handle_message(
        _update(thread=TOPIC, topic=True, reply_to=earlier), None,
    )

    assert router.handled[0].text == '[Reply to Seva: "earlier"]\n\nhello'


@pytest.mark.asyncio
async def test_root_message_routes_as_before():
    router = _Router()
    ch = _channel(router)
    await ch._handle_message(_update(), None)

    (msg,) = router.handled
    assert (msg.channel_key, msg.sender_id) == (f"telegram:{CHAT}", str(CHAT))
    assert msg.session_id == "rolled"  # the chat's active session, idle rollover applied
    assert msg.metadata == {"message_id": 7}
    assert router.created == []


@pytest.mark.asyncio
async def test_a_reply_in_a_plain_group_stays_in_the_group_session():
    router = _Router()
    ch = _channel(router)
    group = _chat(-100, "supergroup", is_forum=False)
    await ch._handle_message(_update(group, thread=99, topic=False), None)

    (msg,) = router.handled
    assert msg.channel_key == "telegram:-100"
    assert msg.session_id == "rolled"  # the chat's active session, idle rollover applied


@pytest.mark.asyncio
async def test_dm_topic_routes_to_its_own_session():
    router = _Router()
    ch = _channel(router, dm_topics=True)
    await ch._handle_message(_update(_chat(OWNER, "private"), thread=TOPIC, topic=True), None)

    (msg,) = router.handled
    assert msg.channel_key == f"telegram:{OWNER}:{TOPIC}"
    assert msg.sender_id == f"{OWNER}:{TOPIC}"


@pytest.mark.asyncio
async def test_with_topics_off_a_topic_message_goes_to_the_chat():
    router = _Router()
    ch = _channel(router, topics=False)
    await ch._handle_message(_update(thread=TOPIC, topic=True), None)

    (msg,) = router.handled
    assert msg.channel_key == f"telegram:{CHAT}"
    assert msg.session_id == "rolled"  # the chat's active session, idle rollover applied


@pytest.mark.asyncio
async def test_a_bound_topic_keeps_its_session():
    router = _Router()
    key = f"telegram:{CHAT}:{TOPIC}"
    router.bound[key] = "old1234"
    ch = _channel(router)
    await ch._handle_message(_update(thread=TOPIC, topic=True), None)

    assert router.created == []
    assert router.handled[0].session_id == "old1234"


@pytest.mark.asyncio
async def test_two_first_messages_in_a_new_topic_bind_one_session():
    router = _Router()
    ch = _channel(router)
    address = ch._address(_update(thread=TOPIC, topic=True))

    first, second = await asyncio.gather(
        ch._session_for(address), ch._session_for(address),
    )

    assert first == second
    assert router.created == [f"telegram:{CHAT}:{TOPIC}"]


@pytest.mark.asyncio
async def test_observed_topic_message_keeps_the_chat_key_and_names_the_topic():
    router = _Router()
    source = ChannelSourceConfig(enabled=True, allow_conversations=[str(CHAT)])
    ch = _channel(router, source=source)
    # A sender the allowlist refuses: observed, never routed.
    await ch._handle_message(_update(thread=TOPIC, topic=True, user_id=777), None)

    (observed,) = router.observed
    assert observed.channel_key == f"telegram:{CHAT}"
    assert observed.metadata["thread_id"] == str(TOPIC)
    assert router.handled == []


# --------------------------------------------------------------------------- #
#  Outbound                                                                    #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_outbound_to_a_topic_carries_the_thread(tmp_path):
    ch = _channel()
    bot = ch._app.bot
    target = f"{CHAT}:{TOPIC}"
    attachment = tmp_path / "note.md"
    attachment.write_text("x")

    await ch.send(OutboundMessage(target=target, text="hi"))
    await ch.send_placeholder(target, "s1")
    await ch.send_typing(target)
    await ch.send_sticker(target, "STICKER")
    assert await ch.send_file(target, str(attachment))

    for name in ("send_message", "send_chat_action", "send_sticker", "send_document"):
        for kwargs in bot.named(name):
            assert (kwargs["chat_id"], kwargs["message_thread_id"]) == (CHAT, TOPIC), name

    bot.calls.clear()
    await ch.edit_message(target, "5", "edited")
    await ch.delete_message(target, "5")
    await ch.set_reaction(target, 5, "👍")
    for name in ("edit_message_text", "delete_message", "set_message_reaction"):
        (kwargs,) = bot.named(name)
        assert kwargs["chat_id"] == CHAT
        assert "message_thread_id" not in kwargs, name


@pytest.mark.asyncio
async def test_outbound_to_the_chat_sends_no_thread(tmp_path):
    ch = _channel()
    bot = ch._app.bot
    attachment = tmp_path / "note.md"
    attachment.write_text("x")

    await ch.send(OutboundMessage(target=str(CHAT), text="hi"))
    await ch.send_placeholder(str(CHAT), "s1")
    await ch.send_typing(str(CHAT))
    await ch.send_sticker(str(CHAT), "STICKER")
    assert await ch.send_file(str(CHAT), str(attachment))

    assert bot.calls
    for name, kwargs in bot.calls:
        assert kwargs["chat_id"] == CHAT
        assert "message_thread_id" not in kwargs, name


@pytest.mark.asyncio
async def test_streaming_edits_share_one_budget_per_chat():
    ch = _channel()
    bot = ch._app.bot

    await ch.edit_message(f"{CHAT}:42", "5", "a", throttle=True)
    await ch.edit_message(f"{CHAT}:43", "6", "b", throttle=True)  # same chat: dropped
    await ch.edit_message("-200", "7", "c", throttle=True)       # another chat
    await ch.edit_message(f"{CHAT}:43", "6", "d")                # not droppable

    assert [
        (kwargs["chat_id"], kwargs["message_id"])
        for kwargs in bot.named("edit_message_text")
    ] == [(CHAT, 5), (-200, 7), (CHAT, 6)]


@pytest.mark.asyncio
async def test_final_send_waits_out_a_rate_limit_once():
    ch = _channel()
    bot = ch._app.bot
    bot.fail_next = [RetryAfter(0)]

    await ch.send(OutboundMessage(target=f"{CHAT}:{TOPIC}", text="answer"))

    sends = bot.named("send_message")
    assert len(sends) == 2  # the limited attempt, then the delivery
    assert sends[-1]["message_thread_id"] == TOPIC


@pytest.mark.asyncio
async def test_final_send_does_not_wait_out_a_long_rate_limit():
    ch = _channel()
    bot = ch._app.bot
    bot.fail_next = [RetryAfter(3600)]

    with pytest.raises(RetryAfter):
        await ch.send(OutboundMessage(target=str(CHAT), text="answer"))
    assert len(bot.named("send_message")) == 1


# --------------------------------------------------------------------------- #
#  Reactions                                                                   #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_reaction_follows_the_cached_message_into_its_topic():
    router = _Router()
    ch = _channel(router)
    await ch.send(OutboundMessage(target=f"{CHAT}:{TOPIC}", text="the answer"))
    sent_id = ch._app.bot._next_id

    await ch._handle_reaction(_reaction(message_id=sent_id), None)

    (msg,) = router.handled
    assert msg.channel_key == f"telegram:{CHAT}:{TOPIC}"
    assert msg.session_id == router.bound[msg.channel_key]
    assert "the answer" in msg.text


@pytest.mark.asyncio
async def test_reaction_to_an_unknown_message_in_a_forum_is_dropped():
    router = _Router()
    ch = _channel(router)
    await ch._handle_reaction(_reaction(message_id=999), None)
    assert router.handled == []


@pytest.mark.asyncio
async def test_reaction_to_an_unknown_message_in_a_dm_with_topics_is_dropped():
    router = _Router()
    ch = _channel(router, dm_topics=True)
    await ch._handle_reaction(_reaction(_chat(OWNER, "private"), message_id=999), None)
    assert router.handled == []


@pytest.mark.asyncio
async def test_reaction_to_an_unknown_message_in_a_plain_chat_goes_to_the_chat():
    router = _Router()
    ch = _channel(router)
    await ch._handle_reaction(_reaction(_chat(OWNER, "private"), message_id=999), None)

    (msg,) = router.handled
    assert msg.channel_key == f"telegram:{OWNER}"
    assert msg.session_id is None


@pytest.mark.asyncio
async def test_cached_message_ids_are_per_chat():
    router = _Router()
    ch = _channel(router)
    ch._cache_message(7, -555, "elsewhere", TOPIC)

    # Same message id, different chat: not this reaction's message.
    await ch._handle_reaction(_reaction(_chat(OWNER, "private"), message_id=7), None)

    (msg,) = router.handled
    assert msg.channel_key == f"telegram:{OWNER}"
    assert "elsewhere" not in msg.text


# --------------------------------------------------------------------------- #
#  Commands inside a topic act on that topic                                   #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_new_inside_a_topic_rebinds_only_that_topic():
    router = _Router()
    topic_key, root_key = f"telegram:{CHAT}:{TOPIC}", f"telegram:{CHAT}"
    router.bound.update({topic_key: "topic01", root_key: "root001"})
    ch = _channel(router)
    bot = _Bot()

    await ch._handle_new_session(
        _update(thread=TOPIC, topic=True, text="/new", bot=bot), SimpleNamespace(args=[]),
    )

    assert router.stopped == ["topic01"]
    assert router.created == [topic_key]
    assert router.bound[root_key] == "root001"
    # Replies land in the topic without the handler naming it.
    assert {kwargs["message_thread_id"] for kwargs in bot.named("send_message")} == {TOPIC}


@pytest.mark.asyncio
async def test_session_inside_a_topic_shows_the_bound_session():
    router = _Router()
    ch = _channel(router)
    bot = _Bot()

    await ch._handle_session(
        _update(thread=TOPIC, topic=True, text="/session", bot=bot), SimpleNamespace(args=[]),
    )

    key = f"telegram:{CHAT}:{TOPIC}"
    assert router.active_session_keys == []  # no idle rollover in a topic
    assert router.bound[key] in bot.named("send_message")[0]["text"]


@pytest.mark.asyncio
async def test_sessions_inside_a_topic_reads_the_topic_binding():
    router = _Router()
    ch = _channel(router)
    bot = _Bot()

    await ch._handle_sessions(
        _update(thread=TOPIC, topic=True, text="/sessions", bot=bot), None,
    )

    assert router.last_session_keys == [f"telegram:{CHAT}:{TOPIC}"]


@pytest.mark.asyncio
async def test_session_button_in_a_topic_switches_that_topic():
    router = _Router()
    ch = _channel(router)
    card = _message(_chat(), thread=TOPIC, topic=True, text="Sessions")
    query = SimpleNamespace(
        data="sess:abcd1234", message=card,
        answer=AsyncMock(), edit_message_text=AsyncMock(),
    )

    await ch._handle_session_button(query)

    assert router.bound == {f"telegram:{CHAT}:{TOPIC}": "abcd1234"}


@pytest.mark.asyncio
async def test_stop_inside_a_topic_stops_the_topic_session():
    router = _Router()
    router.bound[f"telegram:{CHAT}:{TOPIC}"] = "topic01"
    router.stop_turn = AsyncMock(return_value={"turn": "stopped"})
    ch = _channel(router)

    await ch._handle_stop(
        _update(thread=TOPIC, topic=True, text="/stop", bot=_Bot()), SimpleNamespace(args=[]),
    )

    router.stop_turn.assert_awaited_once_with("topic01")


# --------------------------------------------------------------------------- #
#  Store and config                                                            #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_chat_lists_its_topic_sessions(db):
    for sid in ("root", "topic", "other"):
        await db.create_session(sid, title=sid, source="telegram", actor=None)
    await db.set_channel_session("telegram:-100", "root")
    await db.set_channel_session("telegram:-100:42", "topic")
    await db.set_channel_session("telegram:-1001", "other")  # a lookalike chat id

    rows = await db.list_channel_sessions_for_conversation("telegram:-100")

    assert {row["session_id"] for row in rows} == {"root", "topic"}


@pytest.mark.parametrize("raw, enabled", [
    ({}, True),
    ({"topics": {"enabled": False}}, False),
    ({"topics": {"enabled": "false"}}, False),
    ({"topics": False}, False),
    ({"topics": None}, True),
])
def test_topics_config(raw, enabled):
    assert TelegramConfig.from_dict(raw).topics.enabled is enabled
