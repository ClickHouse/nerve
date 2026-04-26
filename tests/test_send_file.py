"""Tests for the send_file dispatch path.

Covers:
- ChannelRouter.send_file fan-out: missing context, missing capability,
  successful dispatch, and explicit-channel routing (cross-channel
  leakage prevention).
- TelegramChannel.send_file: missing file, oversized file, success path.
- _send_file_impl in agent.tools: workspace scope, engine-unavailable
  fallback, native-delivered success message, fallback message when
  the channel cannot deliver, active-channel propagation.
- AgentEngine.get_active_channel: accessor reflects internal state.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nerve.channels.base import BaseChannel, ChannelCapability
from nerve.channels.router import ChannelRouter
from nerve.channels.telegram import TelegramChannel
from nerve.config import NerveConfig


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _StubChannel(BaseChannel):
    """Minimal channel stub with configurable capabilities + send_file return."""

    def __init__(
        self,
        name: str = "stub",
        caps: ChannelCapability = ChannelCapability.SEND_TEXT,
        send_file_returns: bool = True,
    ):
        self._name = name
        self._caps = caps
        self._send_file_returns = send_file_returns
        self.send_file_calls: list[tuple[str, str]] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def capabilities(self) -> ChannelCapability:
        return self._caps

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, message) -> None:
        pass

    async def send_file(self, target: str, file_path: str) -> bool:  # type: ignore[override]
        self.send_file_calls.append((target, file_path))
        return self._send_file_returns


# ---------------------------------------------------------------------------
# ChannelRouter.send_file
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestRouterSendFile:
    async def test_no_channel_arg_returns_false_no_context(self):
        """No active channel + no message context → False."""
        engine = MagicMock()
        router = ChannelRouter(engine)
        assert await router.send_file("missing-session", "/tmp/x") is False

    async def test_no_channel_arg_refuses_even_with_context(self, tmp_path):
        """Channel=None refuses delivery even when ``_message_context``
        has a valid entry. This is the cross-channel leakage guard for
        cron/planner sessions that don't pass a channel through
        ``engine.run()`` — Codex P1 review on commit 28454ab.
        """
        engine = MagicMock()
        router = ChannelRouter(engine)
        ch = _StubChannel(
            name="telegram",
            caps=ChannelCapability.SEND_TEXT | ChannelCapability.SEND_FILES,
        )
        router._channels["telegram"] = ch
        # Stale context pointing at a Telegram chat.
        router._message_context["sess-1"] = {
            "channel_name": "telegram",
            "target": "999",
            "message_id": 1,
        }
        f = tmp_path / "a.txt"
        f.write_text("hi")
        # Without an explicit ``channel`` arg the router MUST NOT
        # dispatch via the cached context — otherwise cron/planner runs
        # would leak files to that Telegram chat.
        assert await router.send_file("sess-1", str(f)) is False
        assert ch.send_file_calls == []

    async def test_channel_without_capability_returns_false(self, tmp_path):
        engine = MagicMock()
        router = ChannelRouter(engine)
        ch = _StubChannel(name="stub", caps=ChannelCapability.SEND_TEXT)
        router._channels["stub"] = ch
        router._message_context["sess-1"] = {
            "channel_name": "stub",
            "target": "12345",
            "message_id": 1,
        }
        f = tmp_path / "a.txt"
        f.write_text("hi")
        assert await router.send_file("sess-1", str(f), channel="stub") is False
        assert ch.send_file_calls == []

    async def test_dispatches_when_capability_present(self, tmp_path):
        engine = MagicMock()
        router = ChannelRouter(engine)
        ch = _StubChannel(
            name="stub",
            caps=ChannelCapability.SEND_TEXT | ChannelCapability.SEND_FILES,
            send_file_returns=True,
        )
        router._channels["stub"] = ch
        router._message_context["sess-1"] = {
            "channel_name": "stub",
            "target": "12345",
            "message_id": 1,
        }
        f = tmp_path / "a.txt"
        f.write_text("hi")
        assert await router.send_file("sess-1", str(f), channel="stub") is True
        assert ch.send_file_calls == [("12345", str(f))]

    async def test_propagates_channel_failure(self, tmp_path):
        engine = MagicMock()
        router = ChannelRouter(engine)
        ch = _StubChannel(
            name="stub",
            caps=ChannelCapability.SEND_TEXT | ChannelCapability.SEND_FILES,
            send_file_returns=False,
        )
        router._channels["stub"] = ch
        router._message_context["sess-1"] = {
            "channel_name": "stub",
            "target": "12345",
            "message_id": 1,
        }
        f = tmp_path / "a.txt"
        f.write_text("hi")
        assert await router.send_file("sess-1", str(f), channel="stub") is False
        assert ch.send_file_calls == [("12345", str(f))]

    # ---- Cross-channel safety (explicit ``channel`` arg) ------------- #

    async def test_explicit_channel_overrides_stale_context(self, tmp_path):
        """Stale Telegram ctx must NOT leak to Telegram when caller asks for web.

        Reproduces the cross-channel leakage path: a session previously
        received a Telegram message (so router._message_context points
        at a Telegram chat), and is now being driven by a web prompt.
        send_file with channel="web" must dispatch to the web channel,
        never to the Telegram chat.
        """
        engine = MagicMock()
        router = ChannelRouter(engine)
        telegram_stub = _StubChannel(
            name="telegram",
            caps=ChannelCapability.SEND_TEXT | ChannelCapability.SEND_FILES,
        )
        web_stub = _StubChannel(
            name="web",
            caps=ChannelCapability.SEND_TEXT | ChannelCapability.SEND_FILES,
        )
        router._channels["telegram"] = telegram_stub
        router._channels["web"] = web_stub
        # Stale context from a prior Telegram inbound message.
        router._message_context["sess-1"] = {
            "channel_name": "telegram",
            "target": "999",
            "message_id": 1,
        }
        f = tmp_path / "a.txt"
        f.write_text("hi")

        ok = await router.send_file("sess-1", str(f), channel="web")
        assert ok is True
        # Telegram MUST NOT have been called — that would leak the file.
        assert telegram_stub.send_file_calls == []
        # Web received the dispatch (target irrelevant for web).
        assert len(web_stub.send_file_calls) == 1
        assert web_stub.send_file_calls[0][1] == str(f)

    async def test_explicit_channel_uses_cached_target_when_match(self, tmp_path):
        """When channel arg matches cached ctx, reuse the cached target."""
        engine = MagicMock()
        router = ChannelRouter(engine)
        telegram_stub = _StubChannel(
            name="telegram",
            caps=ChannelCapability.SEND_TEXT | ChannelCapability.SEND_FILES,
        )
        router._channels["telegram"] = telegram_stub
        router._message_context["sess-1"] = {
            "channel_name": "telegram",
            "target": "12345",
            "message_id": 1,
        }
        f = tmp_path / "a.txt"
        f.write_text("hi")

        ok = await router.send_file("sess-1", str(f), channel="telegram")
        assert ok is True
        assert telegram_stub.send_file_calls == [("12345", str(f))]

    async def test_explicit_channel_empty_target_when_no_match(self, tmp_path):
        """No matching context → empty target. Channels needing a real
        target (Telegram) should fail safely; broadcast channels (web)
        succeed regardless.
        """
        engine = MagicMock()
        router = ChannelRouter(engine)
        # Channel that records target — verify it's empty.
        telegram_stub = _StubChannel(
            name="telegram",
            caps=ChannelCapability.SEND_TEXT | ChannelCapability.SEND_FILES,
            send_file_returns=False,  # Real Telegram would fail on int("")
        )
        router._channels["telegram"] = telegram_stub
        # Context points at a different channel.
        router._message_context["sess-1"] = {
            "channel_name": "web",
            "target": "client-abc",
            "message_id": 1,
        }
        f = tmp_path / "a.txt"
        f.write_text("hi")

        ok = await router.send_file("sess-1", str(f), channel="telegram")
        assert ok is False
        # Called with empty target — no leak of cached "client-abc".
        assert telegram_stub.send_file_calls == [("", str(f))]

    async def test_explicit_channel_unknown_returns_false(self, tmp_path):
        engine = MagicMock()
        router = ChannelRouter(engine)
        f = tmp_path / "a.txt"
        f.write_text("hi")
        assert await router.send_file(
            "sess-1", str(f), channel="nonexistent",
        ) is False

    async def test_explicit_channel_without_capability_returns_false(self, tmp_path):
        engine = MagicMock()
        router = ChannelRouter(engine)
        ch = _StubChannel(name="text-only", caps=ChannelCapability.SEND_TEXT)
        router._channels["text-only"] = ch
        f = tmp_path / "a.txt"
        f.write_text("hi")
        assert await router.send_file(
            "sess-1", str(f), channel="text-only",
        ) is False
        assert ch.send_file_calls == []


# ---------------------------------------------------------------------------
# TelegramChannel.send_file
# ---------------------------------------------------------------------------


def _make_telegram_channel() -> TelegramChannel:
    """Build a TelegramChannel with a minimal config and a mocked _app."""
    cfg = NerveConfig()
    cfg.telegram.bot_token = "TEST:TOKEN"
    cfg.telegram.allowed_users = [1]
    ch = TelegramChannel(cfg, router=MagicMock())
    # Bypass real PTB Application — only need send_document to exist.
    mock_app = MagicMock()
    mock_app.bot = MagicMock()
    mock_app.bot.send_document = AsyncMock()
    ch._app = mock_app
    return ch


@pytest.mark.asyncio
class TestTelegramSendFile:
    async def test_missing_file_returns_false(self, tmp_path):
        ch = _make_telegram_channel()
        bogus = tmp_path / "does-not-exist.txt"
        assert await ch.send_file("12345", str(bogus)) is False
        ch._app.bot.send_document.assert_not_awaited()

    async def test_directory_returns_false(self, tmp_path):
        ch = _make_telegram_channel()
        assert await ch.send_file("12345", str(tmp_path)) is False
        ch._app.bot.send_document.assert_not_awaited()

    async def test_success_path_calls_send_document(self, tmp_path):
        ch = _make_telegram_channel()
        f = tmp_path / "note.md"
        f.write_text("hello")
        ok = await ch.send_file("12345", str(f))
        assert ok is True
        ch._app.bot.send_document.assert_awaited_once()
        kwargs = ch._app.bot.send_document.await_args.kwargs
        assert kwargs["chat_id"] == 12345
        assert kwargs["filename"] == "note.md"

    async def test_send_document_failure_returns_false(self, tmp_path):
        ch = _make_telegram_channel()
        ch._app.bot.send_document.side_effect = RuntimeError("boom")
        f = tmp_path / "note.md"
        f.write_text("hello")
        assert await ch.send_file("12345", str(f)) is False

    async def test_no_app_returns_false(self, tmp_path):
        ch = _make_telegram_channel()
        ch._app = None
        f = tmp_path / "note.md"
        f.write_text("hi")
        assert await ch.send_file("12345", str(f)) is False

    async def test_capability_includes_send_files(self):
        ch = _make_telegram_channel()
        assert ChannelCapability.SEND_FILES in ch.capabilities


# ---------------------------------------------------------------------------
# tools._send_file_impl
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestSendFileImpl:
    async def test_missing_path_returns_error(self):
        from nerve.agent import tools

        result = await tools._send_file_impl({}, "sess")
        assert "file_path is required" in result["content"][0]["text"]

    async def test_file_not_found_returns_error(self, tmp_path):
        from nerve.agent import tools

        bogus = tmp_path / "missing"
        result = await tools._send_file_impl({"file_path": str(bogus)}, "sess")
        assert "not found" in result["content"][0]["text"]

    async def test_outside_workspace_blocked(self, tmp_path):
        from nerve.agent import tools

        workspace = tmp_path / "ws"
        workspace.mkdir()
        outside = tmp_path / "secret.txt"
        outside.write_text("nope")

        with patch.object(tools, "_workspace", workspace), \
             patch.object(tools, "_engine", MagicMock()):
            result = await tools._send_file_impl(
                {"file_path": str(outside)}, "sess"
            )
        assert "must be within the workspace" in result["content"][0]["text"]

    async def test_sibling_prefix_bypass_blocked(self, tmp_path):
        """Sibling directory whose path *string-prefixes* the workspace
        must NOT pass the workspace guard. Without ``is_relative_to``-
        style containment, ``/tmp/ws-evil/secret.txt`` would slip past
        ``startswith("/tmp/ws")`` — Codex P1 review on PR #1 / e773296.
        """
        from nerve.agent import tools

        workspace = tmp_path / "ws"
        workspace.mkdir()
        # Sibling directory whose name shares the workspace prefix.
        sibling = tmp_path / "ws-evil"
        sibling.mkdir()
        evil = sibling / "secret.txt"
        evil.write_text("would be exfiltrated")

        with patch.object(tools, "_workspace", workspace), \
             patch.object(tools, "_engine", MagicMock()):
            result = await tools._send_file_impl(
                {"file_path": str(evil)}, "sess"
            )
        assert "must be within the workspace" in result["content"][0]["text"]

    async def test_engine_unavailable_falls_back(self, tmp_path):
        from nerve.agent import tools

        workspace = tmp_path / "ws"
        workspace.mkdir()
        f = workspace / "a.txt"
        f.write_text("hi")

        with patch.object(tools, "_workspace", workspace), \
             patch.object(tools, "_engine", None):
            result = await tools._send_file_impl(
                {"file_path": str(f)}, "sess"
            )
        text = result["content"][0]["text"]
        assert "File ready: a.txt" in text
        assert "open the web panel" in text

    async def test_native_delivery_success_message(self, tmp_path):
        from nerve.agent import tools

        workspace = tmp_path / "ws"
        workspace.mkdir()
        f = workspace / "a.txt"
        f.write_text("hi")

        engine = MagicMock()
        engine.get_active_channel = MagicMock(return_value="telegram")
        engine.router = MagicMock()
        engine.router.send_file = AsyncMock(return_value=True)

        with patch.object(tools, "_workspace", workspace), \
             patch.object(tools, "_engine", engine):
            result = await tools._send_file_impl(
                {"file_path": str(f)}, "sess-1"
            )

        engine.router.send_file.assert_awaited_once_with(
            "sess-1", str(f.resolve()), channel="telegram",
        )
        text = result["content"][0]["text"]
        assert text.startswith("Sent file: a.txt")

    async def test_dispatch_failure_returns_fallback(self, tmp_path):
        from nerve.agent import tools

        workspace = tmp_path / "ws"
        workspace.mkdir()
        f = workspace / "a.txt"
        f.write_text("hi")

        engine = MagicMock()
        engine.get_active_channel = MagicMock(return_value="telegram")
        engine.router = MagicMock()
        engine.router.send_file = AsyncMock(return_value=False)

        with patch.object(tools, "_workspace", workspace), \
             patch.object(tools, "_engine", engine):
            result = await tools._send_file_impl(
                {"file_path": str(f)}, "sess-1"
            )
        text = result["content"][0]["text"]
        assert "File ready: a.txt" in text
        assert "open the web panel" in text

    async def test_dispatch_exception_is_caught(self, tmp_path):
        from nerve.agent import tools

        workspace = tmp_path / "ws"
        workspace.mkdir()
        f = workspace / "a.txt"
        f.write_text("hi")

        engine = MagicMock()
        engine.get_active_channel = MagicMock(return_value="web")
        engine.router = MagicMock()
        engine.router.send_file = AsyncMock(side_effect=RuntimeError("boom"))

        with patch.object(tools, "_workspace", workspace), \
             patch.object(tools, "_engine", engine):
            result = await tools._send_file_impl(
                {"file_path": str(f)}, "sess-1"
            )
        text = result["content"][0]["text"]
        assert "File ready: a.txt" in text
        assert "open the web panel" in text

    async def test_active_channel_passed_through_to_router(self, tmp_path):
        """_send_file_impl must read engine.get_active_channel and pass it
        as the ``channel`` kwarg to router.send_file. This is what
        prevents cross-channel leakage when ``_message_context`` is
        stale.
        """
        from nerve.agent import tools

        workspace = tmp_path / "ws"
        workspace.mkdir()
        f = workspace / "a.txt"
        f.write_text("hi")

        engine = MagicMock()
        engine.get_active_channel = MagicMock(return_value="web")
        engine.router = MagicMock()
        engine.router.send_file = AsyncMock(return_value=True)

        with patch.object(tools, "_workspace", workspace), \
             patch.object(tools, "_engine", engine):
            await tools._send_file_impl(
                {"file_path": str(f)}, "sess-1"
            )

        engine.get_active_channel.assert_called_once_with("sess-1")
        engine.router.send_file.assert_awaited_once_with(
            "sess-1", str(f.resolve()), channel="web",
        )

    async def test_no_active_channel_passes_none(self, tmp_path):
        """When no channel is active (e.g. cron sessions before any
        inbound message), send_file falls back to the legacy lookup —
        but channel=None is still explicitly passed.
        """
        from nerve.agent import tools

        workspace = tmp_path / "ws"
        workspace.mkdir()
        f = workspace / "a.txt"
        f.write_text("hi")

        engine = MagicMock()
        engine.get_active_channel = MagicMock(return_value=None)
        engine.router = MagicMock()
        engine.router.send_file = AsyncMock(return_value=False)

        with patch.object(tools, "_workspace", workspace), \
             patch.object(tools, "_engine", engine):
            await tools._send_file_impl(
                {"file_path": str(f)}, "sess-1"
            )

        engine.router.send_file.assert_awaited_once_with(
            "sess-1", str(f.resolve()), channel=None,
        )


# ---------------------------------------------------------------------------
# AgentEngine.get_active_channel
# ---------------------------------------------------------------------------


class TestEngineActiveChannel:
    """Smoke tests for the per-session active-channel accessor.

    The accessor is read by ``_send_file_impl`` to prevent stale
    ``_message_context`` entries from misrouting files. We verify the
    bookkeeping shape directly — driving a full engine.run() requires a
    heavy fixture and is covered by integration smoke tests.
    """

    def test_get_returns_none_when_unset(self):
        from nerve.agent.engine import AgentEngine

        # Build a bare instance without running __init__ (no DB required).
        engine = AgentEngine.__new__(AgentEngine)
        engine._active_channel = {}
        assert engine.get_active_channel("sess-1") is None

    def test_get_returns_set_value(self):
        from nerve.agent.engine import AgentEngine

        engine = AgentEngine.__new__(AgentEngine)
        engine._active_channel = {"sess-1": "telegram"}
        assert engine.get_active_channel("sess-1") == "telegram"
        assert engine.get_active_channel("sess-other") is None
