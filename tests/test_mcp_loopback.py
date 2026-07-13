"""Dedicated loopback ASGI listener tests for backend-managed MCP clients."""

from __future__ import annotations

import asyncio

import pytest

from nerve.mcp_server.loopback import McpLoopbackServer


@pytest.mark.asyncio
async def test_loopback_server_serves_asgi_directly():
    async def app(scope, receive, send):
        assert scope["type"] == "http"
        assert scope["path"] == "/mcp/v1/"
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"text/plain")],
        })
        await send({"type": "http.response.body", "body": b"MCP PONG"})

    server = await McpLoopbackServer.start(app)
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        writer.write(
            b"GET /mcp/v1/ HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"Connection: close\r\n\r\n"
        )
        await writer.drain()
        response = await reader.read()
        assert b"200 OK" in response
        assert b"MCP PONG" in response
        writer.close()
        await writer.wait_closed()
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_loopback_server_uses_ephemeral_owner_local_port():
    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    first = await McpLoopbackServer.start(app)
    second = await McpLoopbackServer.start(app)
    try:
        assert first.port > 0
        assert second.port > 0
        assert first.port != second.port
        assert first._socket.getsockname()[0] == "127.0.0.1"
        assert second._socket.getsockname()[0] == "127.0.0.1"
    finally:
        await first.close()
        await second.close()
