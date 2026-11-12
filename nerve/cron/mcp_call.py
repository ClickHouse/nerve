"""Call one tool of an MCP server that Nerve knows, outside an agent session.

A cron gate uses this to ask an MCP server a question before a job runs (see
:class:`nerve.cron.gates.ClickHouseQueryGate`). The server is one of the MCP
servers that new sessions get, found by name:

* In local mode, an entry of the ``mcp_servers`` config. Only ``type: http``
  servers can be called. The call opens an MCP session with the configured
  headers (an ``Authorization`` header holds the credential), calls the tool
  and closes the session.
* In external mode, a server of the MCP gateway's catalog. The call is one
  stateless ``tools/call`` request to the gateway
  (:class:`nerve.mcp_gateway.McpGatewayServer`, as Loom uses it). The gateway
  holds the credential and applies the agent's policy.

A failure is a :class:`McpCallError`. Its message names the server and the
kind of failure, never a header or a credential.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from nerve.mcp_gateway import GatewayCallError, GatewayToolResult, McpGatewayServer

if TYPE_CHECKING:
    from nerve.config import McpServerConfig

logger = logging.getLogger(__name__)

#: The longest a single call may take, whatever a caller asks for (seconds).
MAX_CALL_TIMEOUT_SECONDS = 120.0


class McpCallError(Exception):
    """A tool call that gave no tool result: no server, no answer, a refusal."""


def find_server(servers: list[McpServerConfig], name: str) -> McpServerConfig | None:
    """The server named *name* in *servers*, or ``None``."""
    return next((server for server in servers if server.name == name), None)


async def call_tool(
    server: McpServerConfig,
    tool: str,
    arguments: dict[str, Any],
    *,
    managed: bool,
    timeout: float,
) -> GatewayToolResult:
    """Call *tool* of *server* once and return its result.

    *managed* says that *server* is a server of the MCP gateway's catalog
    (external mode). A tool that ran and refused is a result with
    ``is_error``; every other failure raises :class:`McpCallError`.
    """
    if not server.enabled:
        raise McpCallError(f"MCP server {server.name!r} is disabled")
    timeout = min(float(timeout), MAX_CALL_TIMEOUT_SECONDS)
    if managed:
        return await _call_gateway(server, tool, arguments, timeout)
    if server.type != "http":
        raise McpCallError(
            f"MCP server {server.name!r} has type {server.type!r}; "
            "only http servers can be called here"
        )
    return await _call_http(server, tool, arguments, timeout)


async def _call_gateway(
    server: McpServerConfig, tool: str, arguments: dict[str, Any], timeout: float,
) -> GatewayToolResult:
    client = McpGatewayServer(server.url, timeout=timeout)
    try:
        return await client.call_tool(tool, arguments)
    except GatewayCallError as e:
        raise McpCallError(f"MCP server {server.name!r}: {e}") from None
    finally:
        await client.close()


async def _call_http(
    server: McpServerConfig, tool: str, arguments: dict[str, Any], timeout: float,
) -> GatewayToolResult:
    import httpx2
    from mcp import Client
    from mcp.client.streamable_http import streamable_http_client
    from mcp.shared._httpx_utils import create_mcp_http_client
    from mcp.shared.exceptions import MCPError

    try:
        async with asyncio.timeout(timeout):
            async with create_mcp_http_client(
                headers=dict(server.headers or {}),
                timeout=httpx2.Timeout(timeout),
            ) as http:
                transport = streamable_http_client(server.url, http_client=http)
                async with Client(transport, cache=None) as client:
                    result = await client.call_tool(tool, arguments)
    except Exception as e:  # noqa: BLE001 (named by type only, see the module)
        leaf = _leaf(e)
        if isinstance(leaf, TimeoutError):
            raise McpCallError(
                f"MCP server {server.name!r} did not answer {tool} in {timeout:g} s"
            ) from None
        if isinstance(leaf, MCPError):
            raise McpCallError(
                f"MCP server {server.name!r} refused {tool}: {one_line(leaf.message)}"
            ) from None
        logger.debug("MCP call to %s failed", server.name, exc_info=True)
        raise McpCallError(
            f"MCP request to server {server.name!r} for {tool} failed "
            f"({type(leaf).__name__})"
        ) from None
    return _result(result)


def one_line(text: str, limit: int = 300) -> str:
    """*text* on one line, at most *limit* characters."""
    return " ".join(text.split())[:limit]


def _leaf(error: BaseException) -> BaseException:
    """The first exception that is not a group: the MCP client runs its
    transport in a task group, which wraps what went wrong."""
    while isinstance(error, BaseExceptionGroup) and error.exceptions:
        error = error.exceptions[0]
    return error


def _result(result: Any) -> GatewayToolResult:
    """A ``CallToolResult`` of the MCP client in the form of the gateway's
    results: the text blocks joined, and ``structured`` as its
    ``structuredContent`` (else ``None``)."""
    texts = [
        str(getattr(block, "text", "") or "")
        for block in getattr(result, "content", None) or []
        if getattr(block, "type", None) == "text"
    ]
    return GatewayToolResult(
        is_error=bool(getattr(result, "is_error", False)),
        structured=getattr(result, "structured_content", None),
        text="\n".join(texts),
    )
