"""A session's Telegram topic from code: the HTTP routes and the agent tools.

``POST``/``DELETE /api/sessions/{id}/telegram-topic`` and the
``session_open_topic``/``session_close_topic`` tools hand over to
``TelegramChannel.open_session_topic``/``close_session_topic`` (tested in
test_telegram_topics.py). These tests cover what sits around them: lookup of
the channel, defaults, status codes and wording.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from nerve.agent.backends.base import config_excluded_tools
from nerve.agent.tools import build_default_registry
from nerve.agent.tools.handlers.session_topics import (
    session_close_topic_handler,
    session_open_topic_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.channels.telegram import ClosedTopic, SessionTopic
from nerve.config import NerveConfig

from tests.actor_rows import ensure_actor_row
from tests.conftest import TEST_ACTOR

TOOLS = {"session_open_topic", "session_close_topic"}
KEY = "telegram:4242:501"


def _telegram(opened=SessionTopic(KEY, opened=True), closed=([], "")):
    return SimpleNamespace(
        _app=object(),
        open_session_topic=AsyncMock(return_value=opened),
        close_session_topic=AsyncMock(return_value=closed),
    )


def _engine(channel):
    return SimpleNamespace(channel=lambda name: channel if name == "telegram" else None)


# --------------------------------------------------------------------------- #
#  Tools                                                                       #
# --------------------------------------------------------------------------- #


def test_the_registry_has_both_tools():
    names = {spec.name for spec in build_default_registry().list(include_hoa=False)}
    assert TOOLS <= names


@pytest.mark.parametrize("enabled, token, topics, offered", [
    (True, "123:abc", True, True),
    (True, "", True, False),          # no bot
    (False, "123:abc", True, False),  # Telegram switched off
    (True, "123:abc", False, False),  # topics switched off
])
def test_the_tools_are_offered_only_with_a_bot_that_has_topics(enabled, token, topics, offered):
    cfg = NerveConfig()
    cfg.telegram.enabled = enabled
    cfg.telegram.bot_token = token
    cfg.telegram.topics.enabled = topics

    assert (not TOOLS & config_excluded_tools(cfg)) is offered


@pytest.mark.asyncio
async def test_open_tool_defaults_to_this_session():
    channel = _telegram()
    ctx = ToolContext(session_id="cron0001", engine=_engine(channel))

    result = await session_open_topic_handler(ctx, {})

    channel.open_session_topic.assert_awaited_once_with("cron0001", title=None, chat_id=None)
    assert not result.is_error
    assert result.text_content == f"Opened a new topic: {KEY} (session cron0001)."
    assert result.structured == {"channel_key": KEY, "opened": True}


@pytest.mark.asyncio
async def test_open_tool_passes_a_session_a_title_and_a_chat():
    channel = _telegram(opened=SessionTopic(KEY, opened=False))
    ctx = ToolContext(session_id="cron0001", engine=_engine(channel))

    result = await session_open_topic_handler(
        ctx, {"session_id": " case0001 ", "title": "INC-1 Keeper", "chat_id": -100},
    )

    channel.open_session_topic.assert_awaited_once_with(
        "case0001", title="INC-1 Keeper", chat_id=-100,
    )
    assert result.text_content.startswith("The session has a topic already")


@pytest.mark.asyncio
async def test_open_tool_reports_a_failure_as_an_error():
    channel = _telegram(opened=SessionTopic(None, error="Couldn't open a topic: nope"))
    ctx = ToolContext(session_id="s1", engine=_engine(channel))

    result = await session_open_topic_handler(ctx, {})

    assert result.is_error
    assert result.text_content == "No topic for session s1: Couldn't open a topic: nope"


@pytest.mark.parametrize("engine", [
    None,                                   # no engine wired
    _engine(None),                          # no Telegram channel
    _engine(SimpleNamespace(_app=None)),    # registered, not started
])
@pytest.mark.asyncio
async def test_the_tools_need_a_running_telegram(engine):
    ctx = ToolContext(session_id="s1", engine=engine)
    for handler in (session_open_topic_handler, session_close_topic_handler):
        result = await handler(ctx, {})
        assert result.is_error and result.text_content == "Telegram is not running."


@pytest.mark.asyncio
async def test_close_tool_lists_what_happened_to_each_topic():
    channel = _telegram(closed=(
        [ClosedTopic("telegram:-100:42", "closed"), ClosedTopic(KEY, "left_open")], "",
    ))
    ctx = ToolContext(session_id="case0001", engine=_engine(channel))

    result = await session_close_topic_handler(ctx, {"note": "Resolved"})

    channel.close_session_topic.assert_awaited_once_with("case0001", note="Resolved")
    assert not result.is_error
    assert result.text_content.splitlines() == [
        "Session case0001 keeps running; its topics:",
        "- telegram:-100:42: closed",
        f"- {KEY}: unbound; Telegram cannot close a topic in a private chat, so it stays open",
    ]
    assert result.structured["topics"][1] == {"channel_key": KEY, "outcome": "left_open"}


@pytest.mark.asyncio
async def test_close_tool_with_no_topic_and_with_a_failure():
    none = _telegram(closed=([], ""))
    result = await session_close_topic_handler(
        ToolContext(session_id="s1", engine=_engine(none)), {},
    )
    none.close_session_topic.assert_awaited_once_with("s1", note=None)
    assert result.text_content == "Session s1 has no Telegram topic; nothing to close."

    failing = _telegram(closed=([], "Couldn't close telegram:-100:42: no rights"))
    result = await session_close_topic_handler(
        ToolContext(session_id="s1", engine=_engine(failing)), {},
    )
    assert result.is_error
    assert result.text_content == "Failed: Couldn't close telegram:-100:42: no rights"


# --------------------------------------------------------------------------- #
#  Routes                                                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestTopicRoutes:
    @pytest_asyncio.fixture
    async def setup(self, db, bypass_auth):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        import nerve.config as cfg_mod
        from nerve.agent.sessions import SessionManager
        from nerve.gateway.routes._deps import init_deps
        from nerve.gateway.routes.sessions import router as sessions_router

        cfg_mod._config = NerveConfig()
        await ensure_actor_row(db, TEST_ACTOR)
        await SessionManager(db).get_or_create("web00001", source="web", actor=None)
        state = SimpleNamespace(channel=_telegram())
        engine = SimpleNamespace(channel=lambda name: state.channel)
        init_deps(engine=engine, db=db)  # type: ignore[arg-type]

        app = FastAPI()
        app.include_router(sessions_router)
        bypass_auth(app)
        state.client = TestClient(app)
        yield state

        cfg_mod._config = None

    async def test_open(self, setup):
        r = setup.client.post(
            "/api/sessions/web00001/telegram-topic", json={"title": "Taxes", "chat_id": -100},
        )

        assert r.status_code == 200
        assert r.json() == {"channel_key": KEY, "opened": True}
        setup.channel.open_session_topic.assert_awaited_once_with(
            "web00001", title="Taxes", chat_id=-100,
        )

    async def test_open_needs_no_body(self, setup):
        r = setup.client.post("/api/sessions/web00001/telegram-topic")

        assert r.status_code == 200
        setup.channel.open_session_topic.assert_awaited_once_with(
            "web00001", title=None, chat_id=None,
        )

    async def test_open_refused(self, setup):
        setup.channel.open_session_topic.return_value = SessionTopic(
            None, error="Couldn't open a topic: no rights",
        )

        r = setup.client.post("/api/sessions/web00001/telegram-topic", json={})

        assert (r.status_code, r.json()) == (400, {"detail": "Couldn't open a topic: no rights"})

    async def test_open_rate_limited(self, setup):
        setup.channel.open_session_topic.return_value = SessionTopic(
            None, error="Telegram is rate limiting this chat; retry in 12 s", retry_after=11.2,
        )

        r = setup.client.post("/api/sessions/web00001/telegram-topic")

        assert r.status_code == 429
        assert r.headers["Retry-After"] == "12"

    async def test_unknown_session(self, setup):
        assert setup.client.post("/api/sessions/nope/telegram-topic").status_code == 404
        assert setup.client.delete("/api/sessions/nope/telegram-topic").status_code == 404
        setup.channel.open_session_topic.assert_not_awaited()

    @pytest.mark.parametrize("channel", [None, SimpleNamespace(_app=None)])
    async def test_no_telegram(self, setup, channel):
        setup.channel = channel

        for r in (
            setup.client.post("/api/sessions/web00001/telegram-topic"),
            setup.client.delete("/api/sessions/web00001/telegram-topic"),
        ):
            assert (r.status_code, r.json()) == (503, {"detail": "Telegram is not running"})

    async def test_close(self, setup):
        setup.channel.close_session_topic.return_value = ([ClosedTopic(KEY, "left_open")], "")

        r = setup.client.request(
            "DELETE", "/api/sessions/web00001/telegram-topic", json={"note": "Done"},
        )

        assert r.status_code == 200
        assert r.json() == {"closed": True, "topics": [{"channel_key": KEY, "outcome": "left_open"}]}
        setup.channel.close_session_topic.assert_awaited_once_with("web00001", note="Done")

    async def test_close_needs_no_body(self, setup):
        r = setup.client.delete("/api/sessions/web00001/telegram-topic")

        assert (r.status_code, r.json()) == (200, {"closed": True, "topics": []})
        setup.channel.close_session_topic.assert_awaited_once_with("web00001", note=None)

    async def test_close_partly_refused_lists_what_was_done(self, setup):
        setup.channel.close_session_topic.return_value = (
            [ClosedTopic(KEY, "left_open")], "Couldn't close telegram:-100:42: no rights",
        )

        r = setup.client.delete("/api/sessions/web00001/telegram-topic")

        assert r.status_code == 400
        assert r.json() == {
            "detail": "Couldn't close telegram:-100:42: no rights",
            "closed": False,
            "topics": [{"channel_key": KEY, "outcome": "left_open"}],
        }
