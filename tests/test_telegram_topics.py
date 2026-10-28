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
    ForumTopicClosed,
    ForumTopicCreated,
    ForumTopicEdited,
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
BOT_ID = 999000


class _Bot:
    """Records every Bot API call. ``bot`` stands in for the cached getMe user."""

    defaults = None  # read by Message.reply_text
    id = BOT_ID

    def __init__(self, dm_topics: bool = False):
        self.calls: list[tuple[str, dict]] = []
        self.bot = SimpleNamespace(has_topics_enabled=dm_topics)
        self.fail_next: list[Exception] = []
        self.refuse_topics: Exception | None = None
        self.refuse_threads: set[int] = set()  # topics that were deleted
        self._next_id = 100
        self._next_thread = 500

    async def create_forum_topic(self, chat_id, name, **kwargs):
        self.calls.append(("create_forum_topic", {"chat_id": chat_id, "name": name}))
        if self.refuse_topics is not None:
            raise self.refuse_topics
        self._next_thread += 1
        return SimpleNamespace(message_thread_id=self._next_thread, name=name)

    async def send_message(self, **kwargs):
        self.calls.append(("send_message", kwargs))
        if self.fail_next:
            raise self.fail_next.pop(0)
        if kwargs.get("message_thread_id") in self.refuse_threads:
            raise RuntimeError("Bad Request: message thread not found")
        self._next_id += 1
        return SimpleNamespace(message_id=self._next_id)

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
        self.sessions: dict[str, dict] = {}
        self.messages: dict[str, list[dict]] = {}
        self.titles: dict[str, str | None] = {}
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
        self.titles[sid] = title
        return sid

    async def channel_keys_for_session(self, sid):
        return [key for key, bound in self.bound.items() if bound == sid]

    async def forget_channel(self, key):
        self.bound.pop(key, None)

    async def set_session_title(self, sid, title):
        self.titles[sid] = title

    async def get_session_messages(self, sid, limit):
        return self.messages.get(sid, [])[-limit:]

    async def count_session_messages(self, sid):
        return len(self.messages.get(sid, []))

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
        return self.sessions.get(sid)

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


# --------------------------------------------------------------------------- #
#  Topic lifecycle: /new opens topics, ↗ Topic, transcripts, service messages  #
# --------------------------------------------------------------------------- #


def _service(chat=None, *, user_id=OWNER, thread=TOPIC, **service):
    """A topic service message: forum_topic_created / _edited / _closed."""
    return Update(update_id=3, message=Message(
        message_id=thread if "forum_topic_created" in service else 77,
        date=datetime.now(timezone.utc),
        chat=chat or _chat(),
        from_user=User(id=user_id, first_name="Seva", is_bot=user_id == BOT_ID),
        message_thread_id=thread,
        is_topic_message=True,
        **service,
    ))


def _card_query(thread=None, data="sess:new"):
    card = _message(_chat(), thread=thread, topic=True if thread else None, text="Sessions")
    return SimpleNamespace(
        data=data, message=card, answer=AsyncMock(), edit_message_text=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_new_in_a_forum_opens_a_topic_and_leaves_the_chat_session():
    router = _Router()
    router.bound[f"telegram:{CHAT}"] = "root001"
    ch = _channel(router)
    bot = ch._app.bot

    await ch._handle_new_session(
        _update(text="/new", bot=_Bot()), SimpleNamespace(args=[]),
    )

    (created,) = bot.named("create_forum_topic")
    assert created == {"chat_id": CHAT, "name": "New session"}
    topic_key = f"telegram:{CHAT}:501"
    assert router.bound[topic_key] in router.titles
    assert router.titles[router.bound[topic_key]] is None  # titled later, generated
    assert router.bound[f"telegram:{CHAT}"] == "root001"
    assert router.stopped == []
    (welcome,) = bot.named("send_message")
    assert welcome["message_thread_id"] == 501


@pytest.mark.asyncio
async def test_new_with_a_message_sends_it_in_the_new_topic():
    router = _Router()
    ch = _channel(router)

    await ch._handle_new_session(
        _update(text="/new Taxes 2026", bot=_Bot()), SimpleNamespace(args=["Taxes", "2026"]),
    )

    assert ch._app.bot.named("create_forum_topic")[0]["name"] == "New session"
    sid = router.bound[f"telegram:{CHAT}:501"]
    assert router.titles[sid] is None  # titled later, generated
    (msg,) = router.handled
    assert (msg.channel_key, msg.session_id, msg.text) == (f"telegram:{CHAT}:501", sid, "Taxes 2026")


@pytest.mark.asyncio
async def test_new_falls_back_to_the_chat_when_telegram_refuses_the_topic():
    router = _Router()
    router.bound[f"telegram:{CHAT}"] = "root001"
    ch = _channel(router)
    ch._app.bot.refuse_topics = RuntimeError("Not enough rights to create a topic")
    replies = _Bot()

    await ch._handle_new_session(_update(text="/new", bot=replies), SimpleNamespace(args=[]))

    texts = [kwargs["text"] for kwargs in replies.named("send_message")]
    assert "Not enough rights to create a topic" in texts[0]
    assert router.stopped == ["root001"]
    assert router.bound[f"telegram:{CHAT}"] != "root001"


@pytest.mark.parametrize("chat, dm_topics, creates_topics", [
    (_chat(), False, True),                               # forum
    (_chat(-100, "supergroup", is_forum=False), False, False),
    (_chat(OWNER, "private"), True, True),                # DM with topics on
    (_chat(OWNER, "private"), False, False),              # DM without
])
@pytest.mark.asyncio
async def test_new_opens_a_topic_only_where_topics_exist(chat, dm_topics, creates_topics):
    router = _Router()
    ch = _channel(router, dm_topics=dm_topics)
    await ch._handle_new_session(
        _update(chat, text="/new", bot=_Bot()), SimpleNamespace(args=[]),
    )
    assert bool(ch._app.bot.named("create_forum_topic")) is creates_topics


@pytest.mark.asyncio
async def test_new_creates_no_topic_when_that_is_switched_off():
    router = _Router()
    ch = _channel(router)
    ch.config.telegram.topics.new_session_creates_topic = False
    await ch._handle_new_session(_update(text="/new", bot=_Bot()), SimpleNamespace(args=[]))
    assert ch._app.bot.named("create_forum_topic") == []
    assert router.created == [f"telegram:{CHAT}"]


@pytest.mark.asyncio
async def test_new_session_button_in_a_forum_opens_a_topic():
    router = _Router()
    ch = _channel(router)
    query = _card_query()

    await ch._handle_session_button(query)

    assert router.created == [f"telegram:{CHAT}:501"]
    query.answer.assert_awaited_once_with("Started a new session in a new topic")


@pytest.mark.asyncio
async def test_new_session_button_answers_once_when_the_topic_is_refused():
    router = _Router()
    ch = _channel(router)
    ch._app.bot.refuse_topics = RuntimeError("CHAT_NOT_MODIFIED")
    query = _card_query()

    await ch._handle_session_button(query)

    assert router.created == [f"telegram:{CHAT}"]
    query.answer.assert_awaited_once()
    assert "CHAT_NOT_MODIFIED" in query.answer.await_args.args[0]


@pytest.mark.asyncio
async def test_sessions_list_offers_topics_only_in_a_topic_chat_outside_topics():
    router = _Router()
    router.list_interactive_sessions = AsyncMock(
        return_value=[{"id": "abcd1234", "title": "Work", "source": "web"}],
    )
    ch = _channel(router)

    _text, in_root = await ch._sessions_view_for(f"telegram:{CHAT}", chat=_chat())
    _text, in_topic = await ch._sessions_view_for(f"telegram:{CHAT}:{TOPIC}", chat=_chat())
    _text, plain = await ch._sessions_view_for(
        "telegram:-100", chat=_chat(-100, "supergroup", is_forum=False),
    )

    def callbacks(markup):
        return [b.callback_data for row in markup.inline_keyboard for b in row]

    assert "sesstopic:abcd1234" in callbacks(in_root)
    assert "sesstopic:abcd1234" not in callbacks(in_topic)
    assert "sesstopic:abcd1234" not in callbacks(plain)


@pytest.mark.asyncio
async def test_open_in_topic_moves_the_chat_session_into_a_new_topic():
    router = _Router()
    router.sessions["abcd1234"] = {"id": "abcd1234", "title": "Work", "status": "idle"}
    router.messages["abcd1234"] = [
        {"role": "user", "content": "hi", "created_at": "2026-10-28T01:00:00+00:00"},
    ]
    router.bound[f"telegram:{CHAT}"] = "abcd1234"
    ch = _channel(router)
    bot = ch._app.bot

    await ch._handle_session_button(_card_query(data="sesstopic:abcd1234"))

    assert bot.named("create_forum_topic")[0]["name"] == "Work"
    assert router.bound == {f"telegram:{CHAT}:501": "abcd1234"}  # the chat let go
    (card,) = bot.named("send_message")
    assert card["message_thread_id"] == 501
    buttons = [b.callback_data for row in card["reply_markup"].inline_keyboard for b in row]
    assert "sesstx:abcd1234" in buttons


@pytest.mark.asyncio
async def test_open_in_topic_brings_up_the_topic_a_session_already_has():
    router = _Router()
    router.sessions["abcd1234"] = {"id": "abcd1234", "title": "Work", "status": "idle"}
    router.bound[f"telegram:{CHAT}:77"] = "abcd1234"
    ch = _channel(router)
    query = _card_query(data="sesstopic:abcd1234")

    await ch._handle_session_button(query)

    assert ch._app.bot.named("create_forum_topic") == []
    assert ch._app.bot.named("send_message")[0]["message_thread_id"] == 77


@pytest.mark.asyncio
async def test_open_in_topic_replaces_a_topic_that_was_deleted():
    router = _Router()
    router.sessions["abcd1234"] = {"id": "abcd1234", "title": "Work", "status": "idle"}
    router.bound[f"telegram:{CHAT}:77"] = "abcd1234"
    ch = _channel(router)
    ch._app.bot.refuse_threads = {77}

    await ch._handle_session_button(_card_query(data="sesstopic:abcd1234"))

    assert router.bound == {f"telegram:{CHAT}:501": "abcd1234"}


@pytest.mark.asyncio
async def test_send_transcript_delivers_the_conversation_into_the_topic():
    router = _Router()
    router.sessions["abcd1234"] = {"id": "abcd1234", "title": "Taxes / 2026", "status": "idle"}
    router.messages["abcd1234"] = [
        {"role": "user", "content": "first", "created_at": "2026-10-28T01:00:00+00:00"},
        {"role": "assistant", "content": "second", "created_at": "2026-10-28T01:01:00+00:00"},
        {"role": "system", "content": "internal", "created_at": "2026-10-28T01:02:00+00:00"},
    ]
    ch = _channel(router)

    await ch._handle_session_button(_card_query(thread=TOPIC, data="sesstx:abcd1234"))

    (sent,) = ch._app.bot.named("send_document")
    assert sent["message_thread_id"] == TOPIC
    assert sent["filename"] == "Taxes-2026.md"
    text = sent["document"].decode()
    assert text.index("first") < text.index("second")
    assert "internal" not in text


@pytest.mark.asyncio
async def test_a_topic_someone_creates_gets_a_session_named_after_it():
    router = _Router()
    ch = _channel(router)

    await ch._handle_topic_service(_service(
        forum_topic_created=ForumTopicCreated(name="Taxes", icon_color=7322096),
    ), None)

    sid = router.bound[f"telegram:{CHAT}:{TOPIC}"]
    assert router.titles[sid] == "Taxes"


@pytest.mark.parametrize("user_id, dm_policy", [
    # The bot's own topic: /new binds it, with no placeholder title. Under an
    # open policy the bot passes authorization, so only the bot check stops it.
    (BOT_ID, "open"),
    (777, "pairing"),  # a stranger
])
@pytest.mark.asyncio
async def test_topic_creation_by_the_bot_or_a_stranger_binds_nothing(user_id, dm_policy):
    router = _Router()
    ch = _channel(router)
    ch.config.telegram.dm_policy = dm_policy
    await ch._handle_topic_service(_service(
        user_id=user_id,
        forum_topic_created=ForumTopicCreated(name="x", icon_color=7322096),
    ), None)
    assert router.bound == {}


@pytest.mark.asyncio
async def test_renaming_a_topic_retitles_its_session():
    router = _Router()
    router.bound[f"telegram:{CHAT}:{TOPIC}"] = "topic01"
    ch = _channel(router)

    await ch._handle_topic_service(_service(forum_topic_edited=ForumTopicEdited(name="Renamed")), None)
    await ch._handle_topic_service(_service(forum_topic_edited=ForumTopicEdited(icon_custom_emoji_id="")), None)

    assert router.titles == {"topic01": "Renamed"}


@pytest.mark.asyncio
async def test_closing_a_topic_stops_its_session():
    router = _Router()
    router.bound[f"telegram:{CHAT}:{TOPIC}"] = "topic01"
    ch = _channel(router)

    await ch._handle_topic_service(_service(forum_topic_closed=ForumTopicClosed()), None)

    assert router.stopped == ["topic01"]


@pytest.mark.asyncio
async def test_a_generated_title_renames_the_session_topics():
    router = _Router()
    router.bound.update({
        f"telegram:{CHAT}:{TOPIC}": "topic01",
        f"telegram:{CHAT}": "topic01",   # the chat itself has no topic name
        "web:default": "topic01",
    })
    ch = _channel(router)

    await ch.on_session_titled("topic01", "Quarterly taxes")

    (renamed,) = ch._app.bot.named("edit_forum_topic")
    assert renamed == {"chat_id": CHAT, "message_thread_id": TOPIC, "name": "Quarterly taxes"}


@pytest.mark.asyncio
async def test_no_rename_when_auto_rename_is_off():
    router = _Router()
    router.bound[f"telegram:{CHAT}:{TOPIC}"] = "topic01"
    ch = _channel(router)
    ch.config.telegram.topics.auto_rename = False
    await ch.on_session_titled("topic01", "Quarterly taxes")
    assert ch._app.bot.named("edit_forum_topic") == []


@pytest.mark.parametrize("root_author, expected_title", [
    (OWNER, "Taxes"),   # a topic a person made: its name titles the session
    (BOT_ID, None),     # one the bot made: its name is a placeholder
])
@pytest.mark.asyncio
async def test_first_message_in_an_unbound_topic_takes_the_topic_name(root_author, expected_title):
    router = _Router()
    ch = _channel(router)
    root = Message(
        message_id=TOPIC, date=datetime.now(timezone.utc), chat=_chat(),
        from_user=User(id=root_author, first_name="x", is_bot=root_author == BOT_ID),
        forum_topic_created=ForumTopicCreated(name="Taxes", icon_color=7322096),
    )

    await ch._handle_message(_update(thread=TOPIC, topic=True, reply_to=root), None)

    assert router.titles[router.handled[0].session_id] == expected_title


@pytest.mark.asyncio
async def test_router_passes_a_title_to_every_channel_and_survives_a_failure():
    from nerve.channels.router import ChannelRouter

    router = ChannelRouter(SimpleNamespace())
    broken = SimpleNamespace(name="broken", on_session_titled=AsyncMock(side_effect=RuntimeError))
    working = SimpleNamespace(name="working", on_session_titled=AsyncMock())
    router._channels = {"broken": broken, "working": working}

    await router.session_titled("s1", "Title")

    working.on_session_titled.assert_awaited_once_with("s1", "Title")


@pytest.mark.asyncio
async def test_store_lists_and_forgets_channel_keys(db):
    await db.create_session("s1", title="s1", source="telegram", actor=None)
    await db.set_channel_session("telegram:-100", "s1")
    await db.set_channel_session("telegram:-100:42", "s1")

    assert set(await db.get_channel_keys_for_session("s1")) == {"telegram:-100", "telegram:-100:42"}
    await db.delete_channel_session("telegram:-100")
    assert await db.get_channel_keys_for_session("s1") == ["telegram:-100:42"]
    assert await db.get_session("s1")  # the session itself stays


def test_topic_names_fit_telegram():
    from nerve.channels.telegram import _topic_name
    assert _topic_name(None) == "New session"
    assert _topic_name("  two\nlines ") == "two lines"
    assert len(_topic_name("x" * 300)) == 128


@pytest.mark.parametrize("raw, expected", [
    ({"topics": {"new_session_creates_topic": False}}, (True, False, True)),
    ({"topics": {"auto_rename": "no"}}, (True, True, False)),
    ({}, (True, True, True)),
])
def test_lifecycle_config(raw, expected):
    topics = TelegramConfig.from_dict(raw).topics
    assert (topics.enabled, topics.new_session_creates_topic, topics.auto_rename) == expected
