"""send_image on chat channels: the Telegram photo, replies to it, and the
fallback to a file where a channel can't show an image inline."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import nerve.config as cfg
from nerve.agent.tools.handlers.notifications import send_image_handler
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig

pytestmark = pytest.mark.asyncio

_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16


@pytest.fixture
def ws(tmp_path, monkeypatch):
    ws = tmp_path / "ws"
    ws.mkdir()
    monkeypatch.setattr(cfg, "_config", NerveConfig(workspace=ws))
    return ws


class _PhotoBot:
    def __init__(self, fail_html: bool = False, fail_all: bool = False):
        self.calls: list[dict] = []
        self.fail_html = fail_html
        self.fail_all = fail_all

    async def send_photo(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail_all or (self.fail_html and kwargs.get("parse_mode")):
            raise RuntimeError("Bad Request: can't parse entities")
        return SimpleNamespace(message_id=77)


def _telegram(bot):
    from nerve.channels.telegram import TelegramChannel

    channel = TelegramChannel(lambda: NerveConfig(), router=MagicMock())
    channel._app = SimpleNamespace(bot=bot)
    channel._record_reply_route = AsyncMock()
    return channel


class TestTelegramPhoto:
    async def test_a_photo_records_its_reply_route(self, ws):
        (ws / "a.png").write_bytes(_PNG)
        bot = _PhotoBot()
        channel = _telegram(bot)

        sent = await channel.send_image("4242", file_path=str(ws / "a.png"), caption="**hi**", session_id="s1")

        assert sent and bot.calls[0]["parse_mode"] is not None
        channel._record_reply_route.assert_awaited_once_with(77, 4242, "s1")

    async def test_the_plain_caption_fallback_records_it_too(self, ws):
        (ws / "a.png").write_bytes(_PNG)
        bot = _PhotoBot(fail_html=True)
        channel = _telegram(bot)

        assert await channel.send_image("4242", file_path=str(ws / "a.png"), caption="<b", session_id="s1")
        assert len(bot.calls) == 2 and "parse_mode" not in bot.calls[1]
        channel._record_reply_route.assert_awaited_once_with(77, 4242, "s1")

    async def test_a_refused_photo_records_nothing(self, ws):
        (ws / "a.png").write_bytes(_PNG)
        channel = _telegram(_PhotoBot(fail_all=True))

        assert not await channel.send_image("4242", file_path=str(ws / "a.png"), session_id="s1")
        channel._record_reply_route.assert_not_awaited()


async def test_the_router_names_the_session():
    from nerve.channels.base import ChannelCapability
    from nerve.channels.router import ChannelRouter

    router = ChannelRouter(MagicMock())
    chan = SimpleNamespace(
        capabilities={ChannelCapability.SEND_IMAGES}, send_image=AsyncMock(return_value=True),
    )
    router._channels["telegram"] = chan
    router._message_context["s1"] = {"channel_name": "telegram", "target": "4242"}

    assert await router.send_image("s1", file_path="/x.png", channel="telegram")
    assert chan.send_image.call_args.kwargs["session_id"] == "s1"


def _engine(channel, image_ok, file_ok=False):
    engine = MagicMock()
    engine.get_active_channel.return_value = channel
    engine.router.send_image = AsyncMock(return_value=image_ok)
    engine.router.send_file = AsyncMock(return_value=file_ok)
    return engine


class TestFallback:
    async def test_a_channel_without_images_gets_the_file(self, ws):
        (ws / "a.png").write_bytes(_PNG)
        engine = _engine("slack", image_ok=False, file_ok=True)
        ctx = ToolContext(session_id="s1", workspace=ws, db=None, engine=engine)

        result = await send_image_handler(ctx, {"file_path": str(ws / "a.png")})

        assert result.content[0]["text"].startswith("Image sent as a file: a.png")
        engine.router.send_file.assert_awaited_once_with("s1", str((ws / "a.png").resolve()), channel="slack")

    async def test_a_channel_that_takes_neither_is_told_as_an_error(self, ws):
        (ws / "a.png").write_bytes(_PNG)
        engine = _engine("telegram", image_ok=False, file_ok=False)
        ctx = ToolContext(session_id="s1", workspace=ws, db=None, engine=engine)

        result = await send_image_handler(ctx, {"file_path": str(ws / "a.png")})

        assert result.content[0]["text"].startswith("Error: the image could not be delivered on telegram: a.png.")

    async def test_a_url_on_such_a_channel_is_an_error_too(self, ws):
        engine = _engine("slack", image_ok=False)
        ctx = ToolContext(session_id="s1", workspace=ws, db=None, engine=engine)

        result = await send_image_handler(ctx, {"url": "https://example.com/a.png"})

        assert "could not be delivered on slack" in result.content[0]["text"]
        engine.router.send_file.assert_not_awaited()
