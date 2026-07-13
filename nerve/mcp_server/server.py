"""Build an :class:`mcp.server.lowlevel.Server` from a :class:`ToolRegistry`.

The same registry that backs the in-process Claude SDK MCP (via
``nerve.agent.tools.claude_sdk_adapter``) is reused here, so external
clients see exactly the same tool surface Nerve's own agents do. No
behaviour forks per runtime.

The ``ctx_resolver`` callable is invoked per ``call_tool`` request to
build a fresh :class:`ToolContext` for the satellite session that owns
this MCP connection. It returns an awaitable so the resolver can fetch
state from the DB on first use and cache it for subsequent calls.

The ``audit_writer`` callable is invoked after every successful tool
call to persist an ``external_tool_call`` event into ``session_events``.
Errors during audit logging never abort the tool call — they're logged
but swallowed so a transient DB hiccup can't break the conversation.
"""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable

from mcp.server.lowlevel import Server
from mcp.types import CallToolResult, TextContent, Tool

from nerve.agent.tools import ToolContext, ToolRegistry, ToolResult

logger = logging.getLogger(__name__)


CtxResolver = Callable[[], Awaitable[ToolContext]]
AuditWriter = Callable[[ToolContext, str, dict, ToolResult, float, bool], Awaitable[None]]


def build_mcp_server(
    registry: ToolRegistry,
    *,
    ctx_resolver: CtxResolver,
    audit_writer: AuditWriter | None = None,
    include_hoa: bool = False,
    name: str = "nerve",
    version: str = "1.0.0",
) -> Server:
    """Construct an MCP :class:`Server` backed by the supplied registry.

    Args:
        registry: The :class:`ToolRegistry` containing every handler the
            external endpoint should expose.
        ctx_resolver: Async callable returning a :class:`ToolContext` for
            the current request. The resolver is responsible for
            attributing the call to a satellite session.
        audit_writer: Optional async callable receiving
            ``(tool_context, tool_name, args, result, duration_ms, is_error)``
            after every call. Used to record ``external_tool_call``
            session_events. Failures are logged but never propagate.
        include_hoa: If ``True``, expose HoA tools (``hoa_*``) to the
            external endpoint. Off by default since these tools spawn
            subprocess agents and warrant a separate trust decision.
        name: Server name advertised to clients.
        version: Server version advertised to clients.

    Returns:
        The configured :class:`Server` instance, ready to be plugged
        into a transport (``stdio_server`` or
        :class:`StreamableHTTPSessionManager`).
    """
    import time

    server: Server = Server(name=name, version=version)

    @server.list_tools()
    async def _list_tools() -> list[Tool]:
        return [
            Tool(
                name=spec.name,
                description=spec.description,
                inputSchema=spec.input_schema,
            )
            for spec in registry.list(include_hoa=include_hoa)
        ]

    @server.call_tool()
    async def _call_tool(name: str, arguments: dict[str, Any]) -> CallToolResult:
        spec = registry.get(name)
        if spec is None:
            return CallToolResult(
                content=[TextContent(type="text", text=f"Unknown tool: {name!r}")],
                isError=True,
            )

        # HoA gating: registry.list() filters by include_hoa, but a
        # malicious caller could still invoke a HoA tool by name. Enforce
        # the same allowlist here.
        if not include_hoa and name.startswith("hoa_"):
            return CallToolResult(
                content=[TextContent(type="text", text=f"Tool not available: {name!r}")],
                isError=True,
            )

        start = time.monotonic()
        try:
            ctx = await ctx_resolver()
        except Exception as e:
            logger.exception("Failed to resolve ToolContext for %s", name)
            return CallToolResult(
                content=[TextContent(type="text", text=f"Context error: {e}")],
                isError=True,
            )

        try:
            result = await spec.handler(ctx, arguments)
        except Exception as e:
            logger.exception("Tool %s raised", name)
            duration_ms = (time.monotonic() - start) * 1000.0
            err_result = ToolResult.text(f"Tool error: {e}", is_error=True)
            if audit_writer is not None:
                try:
                    audit_subject = (
                        ctx if getattr(audit_writer, "_accepts_tool_context", False) is True
                        else ctx.session_id
                    )
                    await audit_writer(
                        audit_subject, name, arguments, err_result,
                        duration_ms, True,
                    )
                except Exception:
                    logger.exception("Audit writer failed for %s", name)
            return CallToolResult(
                content=[TextContent(type="text", text=f"Tool error: {e}")],
                isError=True,
            )

        duration_ms = (time.monotonic() - start) * 1000.0
        if audit_writer is not None:
            try:
                audit_subject = (
                    ctx if getattr(audit_writer, "_accepts_tool_context", False) is True
                    else ctx.session_id
                )
                await audit_writer(
                    audit_subject, name, arguments, result,
                    duration_ms, result.is_error,
                )
            except Exception:
                logger.exception("Audit writer failed for %s", name)

        content = [TextContent(**block) for block in result.content if block.get("type") == "text"]
        # Non-text blocks (image, embedded_resource, ...) would be passed
        # through here if any handler ever emits them. Nerve's handlers
        # only emit text today, but we keep the shape flexible.
        for block in result.content:
            if block.get("type") != "text":
                # Best-effort: stringify and append as text. Logged so we
                # can spot any handler that grows non-text output.
                logger.warning(
                    "Tool %s returned non-text block %s; flattening to text",
                    name, block.get("type"),
                )
                content.append(TextContent(type="text", text=str(block)))

        return CallToolResult(content=content, isError=result.is_error)

    return server
