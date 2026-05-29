"""Nerve-as-MCP-server — expose the tool registry to external agents.

This package implements the inbound side of MCP: external clients (Codex,
Claude Code, Cursor) connect over Streamable HTTP and invoke the same
tool handlers that drive native Nerve sessions. Each MCP connection is
attributed to a "satellite session" in the ``sessions`` table so external
tool calls show up in the UI alongside native ones.

Public API:
  * :func:`mount_mcp_http` — attach the ``/mcp/v1`` endpoint to the
    existing FastAPI app and return a context-manager pair (run + stop)
    for the gateway lifespan to enter/exit.
  * :func:`build_mcp_server` — lower-level constructor used by tests and
    by :func:`mount_mcp_http`. Wraps a :class:`ToolRegistry` in an
    :class:`mcp.server.Server`.

Not to be confused with the *external* MCP servers Nerve connects to as
a client — those live in :mod:`nerve.config` (``McpServerConfig``) and
are configured per-session via the Claude Agent SDK.
"""

from __future__ import annotations

from nerve.mcp_server.server import build_mcp_server
from nerve.mcp_server.http import build_manager, mount_deferred, mount_mcp_http
from nerve.mcp_server.session import SatelliteSessionResolver

__all__ = [
    "build_mcp_server",
    "build_manager",
    "mount_deferred",
    "mount_mcp_http",
    "SatelliteSessionResolver",
]
