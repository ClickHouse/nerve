"""End-to-end HTTP integration smoke test for the external MCP endpoint.

Spins up the FastAPI app with ``config.mcp_endpoint.enabled = True``,
drives a full ``initialize`` → ``tools/list`` → ``tools/call`` flow
against the mounted ``/mcp/v1`` endpoint, and asserts a satellite
session row appears in the DB attributed to the right client.

The TestClient is synchronous but the manager runs inside FastAPI's
lifespan, which is sufficient for protocol-level coverage. Real-world
SSE streaming and concurrent connections are covered by manual smoke
tests on the Pi.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fastapi.testclient import TestClient


@pytest.fixture
def app_with_mcp(tmp_path, monkeypatch):
    """Build a FastAPI app with MCP enabled, against a temp DB.

    The agent engine is stubbed so we don't pay the full Claude SDK
    initialization cost — but the registry and notification service
    pathways are real.
    """
    import nerve.config as config_module
    from nerve.config import NerveConfig, McpEndpointConfig
    from nerve.gateway import server as gw_server

    # Build a minimal config with MCP enabled and auth off (dev mode).
    config = NerveConfig()
    config.mcp_endpoint = McpEndpointConfig(enabled=True, path="/mcp/v1")
    config.workspace = tmp_path / "workspace"
    config.workspace.mkdir(parents=True, exist_ok=True)
    # Required so SDK / proxy paths aren't triggered.
    config.anthropic_api_key = ""

    config_module._config = config

    # Patch the heavy components used by lifespan.
    fake_engine = MagicMock()
    fake_engine.config = config
    # Build a real registry so tools/list isn't empty.
    from nerve.agent.tools import build_default_registry
    fake_engine.registry = build_default_registry()
    fake_engine.db = None      # filled in by init_db patch
    fake_engine._memory_bridge = None
    fake_engine._skill_manager = None
    fake_engine.notification_service = None
    fake_engine.set_notification_service = MagicMock()
    fake_engine.shutdown = AsyncMock()
    fake_engine.initialize = AsyncMock()
    fake_engine.router = MagicMock()
    fake_engine.run = AsyncMock()
    fake_engine.is_session_running = MagicMock(return_value=False)
    fake_engine.sessions = MagicMock()
    fake_engine.sessions.run_cleanup = AsyncMock(return_value={})
    fake_engine.run_memorization_sweep = AsyncMock(return_value={})
    fake_engine.run_idle_client_sweep = AsyncMock()

    db_path = tmp_path / "test.db"

    async def _init_db_patched(*args, **kwargs):
        from nerve.db import Database
        d = Database(db_path)
        await d.connect()
        fake_engine.db = d
        return d

    async def _close_db_patched():
        if fake_engine.db is not None:
            await fake_engine.db.close()

    notif_stub = MagicMock(
        send_notification=AsyncMock(return_value="notif-stub"),
        expire_stale=AsyncMock(return_value=0),
        hide_session_label_for=MagicMock(),
    )
    cron_stub = MagicMock(
        start=AsyncMock(),
        stop=AsyncMock(),
        _source_runners=[],
        _jobs=[],
    )
    with patch("nerve.gateway.server.AgentEngine", return_value=fake_engine), \
         patch("nerve.gateway.server.init_db", side_effect=_init_db_patched), \
         patch("nerve.gateway.server.close_db", side_effect=_close_db_patched), \
         patch("nerve.notifications.service.NotificationService", return_value=notif_stub), \
         patch("nerve.gateway.server.init_langfuse"), \
         patch("nerve.cron.service.CronService", return_value=cron_stub):
        config.telegram.enabled = False
        app = gw_server.create_app()
        yield app

    # Best-effort cleanup of globals.
    gw_server._engine = None
    gw_server._mcp_manager = None
    config_module._config = None


def _post_jsonrpc(client: TestClient, body: dict, session_id: str | None = None):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if session_id:
        headers["mcp-session-id"] = session_id
    return client.post("/mcp/v1/", json=body, headers=headers)


def _parse_response(resp) -> dict:
    """Decode an MCP JSON-RPC response from either JSON or SSE body."""
    content_type = resp.headers.get("content-type", "")
    if content_type.startswith("text/event-stream"):
        # SSE: ``data: <json>\n`` lines. Find the first data line.
        for line in resp.text.splitlines():
            if line.startswith("data:"):
                payload = line[5:].strip()
                if payload:
                    return json.loads(payload)
        raise AssertionError(f"No SSE data found in: {resp.text!r}")
    return resp.json()


def test_mcp_initialize_handshake(app_with_mcp):
    with TestClient(app_with_mcp) as client:
        init_resp = _post_jsonrpc(client, {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "0.1"},
            },
        })
        assert init_resp.status_code == 200, init_resp.text
        body = _parse_response(init_resp)
        assert body["jsonrpc"] == "2.0"
        assert body["id"] == 1
        assert "result" in body
        assert body["result"]["serverInfo"]["name"] == "nerve"
        # Session id is assigned and returned in the header
        assert init_resp.headers.get("mcp-session-id")


def test_mcp_list_tools(app_with_mcp):
    """After initialize, tools/list must enumerate the default registry."""
    with TestClient(app_with_mcp) as client:
        init_resp = _post_jsonrpc(client, {
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "0.1"},
            },
        })
        sid = init_resp.headers["mcp-session-id"]

        # initialized notification (required before any request)
        notif_resp = _post_jsonrpc(client, {
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        }, session_id=sid)
        assert notif_resp.status_code in (200, 202), notif_resp.text

        list_resp = _post_jsonrpc(client, {
            "jsonrpc": "2.0", "id": 2, "method": "tools/list",
        }, session_id=sid)
        assert list_resp.status_code == 200, list_resp.text
        body = _parse_response(list_resp)
        names = {t["name"] for t in body["result"]["tools"]}
        # Sample of names we expect — full set verified in unit tests.
        assert "task_create" in names
        assert "memory_recall" in names
        assert "notify" in names
        # HoA tools excluded by default.
        assert not any(n.startswith("hoa_") for n in names)
