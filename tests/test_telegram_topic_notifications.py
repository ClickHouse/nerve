"""Notifications follow a session into its Telegram topic.

A push for a session bound to a topic is delivered there, without the session
footer, and recorded with the topic's target, so /reply and a button tap answer
it only inside that topic. Other pushes go to the sink, which may itself be a
topic (``notifications.telegram_thread_id``).
"""

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from telegram import Chat, Message, Update, User

from nerve.channels.telegram import TelegramChannel
from nerve.config import NerveConfig, NotificationsConfig
from nerve.db import Database
from nerve.notifications.service import NotificationService
from tests.actor_rows import ensure_system_principal

OWNER = 4242
SINK = -1000000000001  # notification group
CHAT = -1001234567890  # forum where the session lives
TOPIC = 42


@pytest_asyncio.fixture
async def db(db):  # noqa: F811 — the conftest database, with an identity
    await ensure_system_principal(db)
    return db


class _Bot:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []
        self._next_id = 100

    async def send_message(self, **kwargs):
        self.calls.append(("send_message", kwargs))
        self._next_id += 1
        return SimpleNamespace(message_id=self._next_id)

    async def edit_message_text(self, **kwargs):
        self.calls.append(("edit_message_text", kwargs))

    def named(self, name):
        return [kwargs for called, kwargs in self.calls if called == name]


@pytest.fixture
def bot():
    return _Bot()


@pytest.fixture
def config(tmp_path: Path) -> NerveConfig:
    cfg = NerveConfig()
    cfg.workspace = tmp_path
    cfg.telegram.allowed_users = [OWNER]
    cfg.notifications = NotificationsConfig(
        channels=["telegram"],
        telegram_chat_id=SINK,
        priority_prefixes={"high": "", "urgent": ""},
    )
    return cfg


@pytest.fixture
def service(config, db, bot, monkeypatch) -> NotificationService:
    from nerve.agent import streaming

    monkeypatch.setattr(streaming, "broadcaster", SimpleNamespace(broadcast=AsyncMock()))
    channel = SimpleNamespace(
        _app=SimpleNamespace(bot=bot), _cache_message=MagicMock(),
        _record_reply_route=AsyncMock(),
    )
    engine = MagicMock()
    engine.sessions.is_running.return_value = False
    engine.router.get_channel.return_value = channel
    engine.run = AsyncMock()
    return NotificationService(config, db, engine)


async def _session_in_topic(db, sid="s1"):
    await db.create_session(sid, title=sid, source="telegram", actor=None)
    await db.set_channel_session(f"telegram:{CHAT}:{TOPIC}", sid)


@pytest.mark.asyncio
async def test_a_topic_session_is_notified_in_its_topic(service, db, bot):
    await _session_in_topic(db)

    nid = await service.send_notification("s1", "Build done", "All green")

    (sent,) = bot.named("send_message")
    assert (sent["chat_id"], sent["message_thread_id"]) == (CHAT, TOPIC)
    assert "Session:" not in sent["text"]  # the topic is the label
    assert await db.get_notification_delivery(nid, "telegram", f"{CHAT}:{TOPIC}")
    # Cached with its topic, so a reaction to the card reaches this session.
    cached = service.engine.router.get_channel.return_value._cache_message.call_args.args
    assert (cached[1], cached[3]) == (CHAT, TOPIC)


@pytest.mark.asyncio
async def test_other_sessions_go_to_the_sink_as_before(service, db, bot):
    await db.create_session("s2", title="s2", source="web", actor=None)

    await service.send_notification("s2", "Build done", "All green")

    (sent,) = bot.named("send_message")
    assert sent["chat_id"] == SINK
    assert "message_thread_id" not in sent
    assert "Session: s2" in sent["text"]


@pytest.mark.asyncio
async def test_the_sink_can_be_a_topic(service, db, bot, config):
    config.notifications.telegram_thread_id = 7
    await db.create_session("s2", title="s2", source="cron", actor=None)

    nid = await service.send_notification("s2", "Nightly", "ok")

    (sent,) = bot.named("send_message")
    assert (sent["chat_id"], sent["message_thread_id"]) == (SINK, 7)
    assert "Session: s2" in sent["text"]  # a shared topic keeps the label
    assert await db.get_notification_delivery(nid, "telegram", f"{SINK}:7")


@pytest.mark.asyncio
async def test_with_topics_off_a_topic_session_goes_to_the_sink(service, db, bot, config):
    config.telegram.topics.enabled = False
    await _session_in_topic(db)

    await service.send_notification("s1", "Build done")

    assert bot.named("send_message")[0]["chat_id"] == SINK


@pytest.mark.asyncio
async def test_a_question_in_a_topic_is_answered_only_from_that_topic(service, db, bot):
    await _session_in_topic(db)

    result = await service.ask_question(session_id="s1", title="Ship it?", options=["yes", "no"])
    nid = result["notification_id"]

    (sent,) = bot.named("send_message")
    assert sent["message_thread_id"] == TOPIC
    assert sent["reply_markup"] is not None
    assert await service.answer_delivered_notification(
        nid, "yes", channel="telegram", target=str(CHAT), actor="1",
    ) is None  # the chat itself is not where it was asked
    answered = await service.answer_delivered_notification(
        nid, "yes", channel="telegram", target=f"{CHAT}:{TOPIC}", actor="1",
    )
    assert answered["status"] == "answered"


@pytest.mark.asyncio
async def test_an_expired_topic_card_is_edited_in_place(service, db, bot):
    await _session_in_topic(db)
    result = await service.ask_question(session_id="s1", title="Ship it?", options=["yes", "no"])
    nid = result["notification_id"]
    await db.update_notification(nid, expires_at="2000-01-01T00:00:00+00:00")

    assert await service.expire_stale() == 1

    (edit,) = bot.named("edit_message_text")
    assert edit["chat_id"] == CHAT
    assert "Session:" not in edit["text"]
    assert "Expired unanswered" in edit["text"]


# --------------------------------------------------------------------------- #
#  Answers typed or tapped in Telegram                                         #
# --------------------------------------------------------------------------- #


def _channel(**notifications):
    cfg = NerveConfig()
    cfg.telegram.allowed_users = [OWNER]
    cfg.notifications = NotificationsConfig(**notifications)
    router = SimpleNamespace(
        handle_message=AsyncMock(),
        get_last_session=AsyncMock(return_value="s1"),
        create_session=AsyncMock(return_value="s1"),
        get_active_session=AsyncMock(return_value="s1"),
    )
    ch = TelegramChannel(lambda: cfg, router=router)
    ch._app = SimpleNamespace(bot=_Bot())
    ch._notification_service = SimpleNamespace(
        answer_latest_question=AsyncMock(return_value=None),
        answer_delivered_notification=AsyncMock(return_value=None),
        resolve_telegram_answer=AsyncMock(return_value="yes"),
    )
    return ch


def _message(chat_id=CHAT, chat_type="supergroup", thread=None, text="hello", bot=None):
    chat = Chat(id=chat_id, type=chat_type, is_forum=True if chat_type == "supergroup" else None)
    msg = Message(
        message_id=7, date=datetime.now(timezone.utc), chat=chat,
        from_user=User(id=OWNER, first_name="Seva", is_bot=False),
        text=text, message_thread_id=thread, is_topic_message=True if thread else None,
    )
    if bot is not None:
        msg.set_bot(bot)
    return msg


@pytest.mark.parametrize("thread, target", [(TOPIC, f"{CHAT}:{TOPIC}"), (None, str(CHAT))])
@pytest.mark.asyncio
async def test_reply_answers_the_question_asked_where_it_is_typed(thread, target):
    ch = _channel()
    update = Update(update_id=1, message=_message(thread=thread, text="/reply yes", bot=_Bot()))

    await ch._handle_reply(update, SimpleNamespace(args=["yes"]))

    assert ch._notification_service.answer_latest_question.await_args.kwargs["target"] == target


@pytest.mark.asyncio
async def test_a_button_tap_answers_where_the_card_is():
    ch = _channel()
    card = _message(thread=TOPIC, text="Ship it?")
    query = SimpleNamespace(
        data="notifopt:ask-1:0", message=card, from_user=SimpleNamespace(id=OWNER),
        answer=AsyncMock(), edit_message_text=AsyncMock(),
    )
    update = SimpleNamespace(callback_query=query, effective_chat=card.chat)

    await ch._handle_callback_query(update, None)

    kwargs = ch._notification_service.answer_delivered_notification.await_args.kwargs
    assert kwargs["target"] == f"{CHAT}:{TOPIC}"


@pytest.mark.parametrize("thread, routed", [(7, False), (TOPIC, True), (None, True)])
@pytest.mark.asyncio
async def test_a_sink_topic_is_one_way_and_the_rest_of_the_group_is_not(thread, routed):
    ch = _channel(telegram_chat_id=CHAT, telegram_thread_id=7, delivery_only_sink=True)

    await ch._handle_message(Update(update_id=1, message=_message(thread=thread)), None)

    assert ch.router.handle_message.await_count == (1 if routed else 0)


@pytest.mark.asyncio
async def test_a_sink_without_a_topic_is_one_way_everywhere():
    ch = _channel(telegram_chat_id=CHAT, delivery_only_sink=True)
    await ch._handle_message(Update(update_id=1, message=_message(thread=TOPIC)), None)
    ch.router.handle_message.assert_not_awaited()


def test_thread_id_config():
    assert NotificationsConfig.from_dict({"telegram_thread_id": "42"}).telegram_thread_id == 42
    assert NotificationsConfig.from_dict({}).telegram_thread_id is None
