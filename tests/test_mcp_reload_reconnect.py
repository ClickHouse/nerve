"""mcp_reload reconnects MCP servers that failed in the calling session."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.backends.claude import ClaudeClient
from nerve.agent.tools.handlers.mcp_admin import mcp_reload_handler
from nerve.agent.tools.registry import ToolContext


def _client(status, reconnect=None):
    c = ClaudeClient.__new__(ClaudeClient)
    c._sdk = MagicMock()
    c._sdk.get_mcp_status = AsyncMock(
        side_effect=status if isinstance(status, Exception) else None,
        return_value=None if isinstance(status, Exception) else status,
    )
    c._sdk.reconnect_mcp_server = reconnect or AsyncMock()
    return c


@pytest.mark.asyncio
async def test_only_failed_servers_are_reconnected():
    c = _client({"mcpServers": [
        {"name": "notion", "status": "failed"},
        {"name": "linear", "status": "connected"},
        {"name": "x", "status": "pending"},
    ]})
    assert await c.reconnect_failed_mcp_servers() == {"notion": "reconnected"}
    c._sdk.reconnect_mcp_server.assert_awaited_once_with("notion")


@pytest.mark.asyncio
async def test_reconnect_error_is_reported_and_others_continue():
    async def rc(name):
        if name == "a":
            raise RuntimeError("timeout")
    c = _client({"mcpServers": [
        {"name": "a", "status": "failed"}, {"name": "b", "status": "failed"},
    ]}, AsyncMock(side_effect=rc))
    assert await c.reconnect_failed_mcp_servers() == {
        "a": "failed: timeout", "b": "reconnected",
    }


@pytest.mark.asyncio
async def test_status_error_returns_empty():
    assert await _client(RuntimeError("x")).reconnect_failed_mcp_servers() == {}


@pytest.mark.asyncio
async def test_handler_appends_reconnect_results():
    client = MagicMock()
    client.reconnect_failed_mcp_servers = AsyncMock(return_value={"notion": "reconnected"})
    engine = MagicMock()
    engine.reload_mcp_config = AsyncMock(return_value=[SimpleNamespace(name="notion")])
    engine.sessions.get_client.return_value = client
    ctx = ToolContext(session_id="s1", engine=engine)
    r = await mcp_reload_handler(ctx, {})
    engine.sessions.get_client.assert_called_once_with("s1")
    assert "notion: reconnected" in str(r)


@pytest.mark.asyncio
async def test_handler_without_client_or_support_is_unchanged():
    engine = MagicMock()
    engine.reload_mcp_config = AsyncMock(return_value=[])
    for client in (None, object()):
        engine.sessions.get_client.return_value = client
        r = await mcp_reload_handler(ToolContext(session_id="s1", engine=engine), {})
        assert "Reconnect" not in str(r)
