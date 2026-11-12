"""send_image in the web UI.

The web channel takes images (its card is the persisted tool call), and a
local image is copied into the session's uploads so the card still shows
it after the file is gone. The result names the copy on a ``web_url:`` line.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

import nerve.config as cfg
from nerve.agent.tools.handlers.notifications import send_image_handler
from nerve.agent.tools.registry import ToolContext
from nerve.channels.base import ChannelCapability
from nerve.channels.router import ChannelRouter
from nerve.channels.web import WebChannel
from nerve.config import NerveConfig

pytestmark = pytest.mark.asyncio

_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16


class _Db:
    def __init__(self):
        self.saved: list[dict] = []

    async def save_uploaded_file(self, **row):
        self.saved.append(row)


@pytest.fixture
def ws(tmp_path, monkeypatch):
    ws = tmp_path / "ws"
    (ws / "tmp").mkdir(parents=True)
    monkeypatch.setattr(cfg, "_config", NerveConfig(workspace=ws))
    return ws


def _engine(channel: str | None, delivered: bool = True):
    engine = MagicMock()
    engine.get_active_channel.return_value = channel
    engine.router.send_image = AsyncMock(return_value=delivered)
    return engine


def _ctx(ws, engine, db=None, session_id="s1"):
    return ToolContext(session_id=session_id, workspace=ws, db=db, engine=engine)


def _text(result) -> str:
    return result.content[0]["text"]


class TestWebChannel:
    async def test_declares_images(self):
        assert ChannelCapability.SEND_IMAGES in WebChannel().capabilities

    async def test_the_router_delivers_to_the_web(self):
        router = ChannelRouter(MagicMock())
        router.register(WebChannel())
        assert await router.send_image("s1", url="https://x/a.png", channel="web") is True
        # The cron guard still holds: no channel, no delivery.
        assert await router.send_image("s1", url="https://x/a.png", channel=None) is False


class TestSnapshot:
    async def test_a_sent_image_is_copied_into_the_session_uploads(self, ws):
        src = ws / "tmp" / "chart.png"
        src.write_bytes(_PNG)
        db = _Db()
        result = await send_image_handler(_ctx(ws, _engine("web"), db), {"file_path": str(src)})

        [row] = db.saved
        assert row["session_id"] == "s1" and row["filename"] == "chart.png"
        assert row["media_type"] == "image/png" and row["file_type"] == "image"
        assert row["file_size"] == len(_PNG)
        copy = ws / ".uploads" / "s1" / f"{row['file_id']}_chart.png"
        assert row["disk_path"] == str(copy.resolve()) or row["disk_path"] == str(copy)
        assert copy.read_bytes() == _PNG
        assert _text(result) == f"Image sent: chart.png\nweb_url: /api/files/uploads/{row['file_id']}"

        # The card outlives the file.
        src.unlink()
        assert copy.exists()

    async def test_the_channel_still_gets_the_original(self, ws):
        src = ws / "a.png"
        src.write_bytes(_PNG)
        engine = _engine("telegram")
        await send_image_handler(_ctx(ws, engine, _Db()), {"file_path": str(src), "caption": "hi"})
        kwargs = engine.router.send_image.call_args.kwargs
        assert kwargs["file_path"] == str(src.resolve())
        assert kwargs["caption"] == "hi" and kwargs["channel"] == "telegram"

    async def test_with_no_channel_the_web_ui_still_shows_it(self, ws):
        src = ws / "a.png"
        src.write_bytes(_PNG)
        result = await send_image_handler(_ctx(ws, _engine(None, delivered=False), _Db()), {"file_path": str(src)})
        assert _text(result).startswith("Image shown in the web UI only: a.png\nweb_url: /api/files/uploads/")

    @pytest.mark.parametrize("name", ["a.svg", "a.bmp", "a.txt"])
    async def test_other_types_are_not_copied(self, ws, name):
        src = ws / name
        src.write_bytes(b"x")
        db = _Db()
        result = await send_image_handler(_ctx(ws, _engine("web"), db), {"file_path": str(src)})
        assert db.saved == []
        assert _text(result) == f"Image sent: {name}"

    async def test_an_oversized_image_is_not_copied(self, ws, monkeypatch):
        import nerve.agent.tools.handlers.notifications as mod

        monkeypatch.setattr(mod, "_WEB_IMAGE_MAX_BYTES", 4)
        src = ws / "a.png"
        src.write_bytes(_PNG)
        db = _Db()
        await send_image_handler(_ctx(ws, _engine("web"), db), {"file_path": str(src)})
        assert db.saved == []

    async def test_a_session_id_that_names_no_directory_gets_no_copy(self, ws):
        src = ws / "a.png"
        src.write_bytes(_PNG)
        db = _Db()
        result = await send_image_handler(_ctx(ws, _engine("web"), db, session_id="../x"), {"file_path": str(src)})
        assert db.saved == [] and "web_url" not in _text(result)

    async def test_a_failing_db_does_not_fail_the_send(self, ws):
        src = ws / "a.png"
        src.write_bytes(_PNG)
        db = MagicMock()
        db.save_uploaded_file = AsyncMock(side_effect=RuntimeError("locked"))
        result = await send_image_handler(_ctx(ws, _engine("web"), db), {"file_path": str(src)})
        assert _text(result) == "Image sent: a.png"

    async def test_outside_the_workspace_is_refused_and_not_copied(self, ws, tmp_path):
        src = tmp_path / "secret.png"
        src.write_bytes(_PNG)
        db = _Db()
        result = await send_image_handler(_ctx(ws, _engine("web"), db), {"file_path": str(src)})
        assert _text(result) == "Error: file must be within the workspace."
        assert db.saved == []


class TestUrl:
    async def test_a_remote_url_is_passed_on_and_not_copied(self, ws):
        engine = _engine("web")
        db = _Db()
        result = await send_image_handler(_ctx(ws, engine, db), {"url": "https://example.com/a.png"})
        assert _text(result) == "Image sent: https://example.com/a.png"
        assert db.saved == []

    @pytest.mark.parametrize("url", ["javascript:alert(1)", "file:///etc/passwd", "data:image/png;base64,AA"])
    async def test_a_non_http_url_is_refused(self, ws, url):
        engine = _engine("web")
        result = await send_image_handler(_ctx(ws, engine, _Db()), {"url": url})
        assert _text(result) == "Error: url must be an http(s) URL."
        engine.router.send_image.assert_not_called()
