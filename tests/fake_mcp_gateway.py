"""A fake MCP gateway for tests: an HTTP server on loopback.

It serves ``GET /catalog`` in the shape of the gateway's catalog contract
(schema version 1) and answers ``POST /s/<id>/mcp`` with JSON-RPC, including
the policy denial ``-32001``. A test can change the catalog, return an error
status, or stop the server to make the gateway unreachable.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

DENIAL_MESSAGE = "tool not allowed by tenant policy"


def catalog_payload(generation: int, servers: dict[str, list[str]], **extra) -> dict:
    """A catalog in the contract shape. ``servers`` maps IDs to tool names."""
    return {
        "schemaVersion": 1,
        "generation": generation,
        "digest": "sha256:" + format(generation, "x").rjust(64, "0"),
        "servers": [
            {
                "id": server_id,
                "displayName": server_id.title(),
                "description": f"The {server_id} server.",
                "path": f"/s/{server_id}/mcp",
                "tools": sorted(tools),
            }
            for server_id, tools in sorted(servers.items())
        ],
        **extra,
    }


class FakeMcpGateway:
    """The VM-facing routes of the MCP gateway, for one agent."""

    def __init__(self, catalog: dict | None = None):
        self.catalog: dict | None = catalog if catalog is not None else catalog_payload(
            7, {"docs": ["fetch_page", "search"], "github": ["get_issue"]},
        )
        self.catalog_status = 200
        self.catalog_body: bytes | None = None
        self.content_type = "application/json"
        self.denied_tools: set[str] = set()
        self.catalog_requests = 0
        self.catalog_gate: threading.Event | None = None
        self.mcp_requests: list[tuple[str, dict]] = []
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.port = 0

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> "FakeMcpGateway":
        gateway = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:  # quiet test output
                pass

            def do_GET(self) -> None:  # noqa: N802 - http.server API
                if self.path != "/catalog":
                    self._json(404, {"reason": "not_found", "requestId": _ZERO_ID})
                    return
                gateway.catalog_requests += 1
                if gateway.catalog_gate is not None:
                    gateway.catalog_gate.wait(5)
                if gateway.catalog_status != 200:
                    self._json(gateway.catalog_status, {
                        "reason": "catalog_unavailable",
                        "requestId": "0192f000-0000-7000-8000-000000000001",
                    })
                    return
                body = gateway.catalog_body
                if body is None:
                    body = json.dumps(gateway.catalog).encode()
                self._send(200, body, gateway.content_type)

            def do_POST(self) -> None:  # noqa: N802 - http.server API
                parts = self.path.split("/")
                if len(parts) != 4 or parts[1] != "s" or parts[3] != "mcp":
                    self._json(404, {"reason": "not_found", "requestId": _ZERO_ID})
                    return
                length = int(self.headers.get("Content-Length") or 0)
                message = json.loads(self.rfile.read(length) or b"{}")
                gateway.mcp_requests.append((parts[2], message))
                if "id" not in message:
                    self._send(202, b"", "application/json")
                    return
                self._json(200, gateway._answer(parts[2], message))

            def _json(self, status: int, payload: dict) -> None:
                self._send(status, json.dumps(payload).encode(), "application/json")

            def _send(self, status: int, body: bytes, content_type: str) -> None:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)

        self._server = _Server(("127.0.0.1", self.port), Handler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True,
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        """Close the listener: requests to the gateway are refused."""
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(5)
            self._thread = None

    def _tools(self, server_id: str) -> list[str]:
        for server in (self.catalog or {}).get("servers", []):
            if server.get("id") == server_id:
                return list(server.get("tools", []))
        return []

    def _answer(self, server_id: str, message: dict) -> dict:
        method = message.get("method")
        reply: dict[str, Any] = {"jsonrpc": "2.0", "id": message["id"]}
        if method == "initialize":
            reply["result"] = {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "fake-gateway", "version": "1"},
            }
        elif method == "tools/list":
            reply["result"] = {"tools": [
                {"name": name, "inputSchema": {"type": "object"}}
                for name in self._tools(server_id)
            ]}
        elif method == "tools/call":
            name = (message.get("params") or {}).get("name")
            if name in self.denied_tools or name not in self._tools(server_id):
                reply["error"] = {"code": -32001, "message": DENIAL_MESSAGE}
            else:
                reply["result"] = {"content": [{"type": "text", "text": "ok"}]}
        else:
            reply["error"] = {"code": -32601, "message": "method not available"}
        return reply


_ZERO_ID = "00000000-0000-0000-0000-000000000000"


class _Server(ThreadingHTTPServer):
    # A stopped gateway starts again on the same port.
    allow_reuse_address = True
    daemon_threads = True
