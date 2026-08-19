"""Build an :class:`mcp.server.lowlevel.Server` from a :class:`ToolRegistry`.

The same registry that backs the in-process Claude SDK MCP (via
``nerve.agent.tools.claude_sdk_adapter``) is reused here, so external
clients see exactly the same tool surface Nerve's own agents do. No
behaviour forks per runtime.

The ``ctx_resolver`` callable is invoked per ``call_tool`` request to
build a fresh :class:`ToolContext` for the satellite session that owns
this MCP connection. It receives the request's
:class:`~mcp.server.context.ServerRequestContext` — mcp 2.x hands that to
handlers as an argument rather than exposing it through a contextvar — and
returns an awaitable so the resolver can fetch state from the DB on first
use and cache it for subsequent calls.

Argument validation against each tool's ``inputSchema`` happens here,
explicitly. mcp 1.x's ``@server.call_tool()`` decorator did it for us by
default (``validate_input=True``); the 2.x ``on_call_tool`` callback does
not, so relying on the library would have silently dropped validation on an
endpoint external clients can reach.

The ``audit_writer`` callable is invoked after every successful tool
call to persist an ``external_tool_call`` event into ``session_events``.
Errors during audit logging never abort the tool call — they're logged
but swallowed so a transient DB hiccup can't break the conversation.
"""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable

import jsonschema
from mcp.server.context import ServerRequestContext
from mcp.server.lowlevel import Server
from mcp.types import (
    CallToolRequestParams,
    CallToolResult,
    ListToolsResult,
    PaginatedRequestParams,
    TextContent,
    Tool,
)

from nerve.agent.tools import ToolContext, ToolRegistry, ToolResult

logger = logging.getLogger(__name__)


CtxResolver = Callable[[ServerRequestContext], Awaitable[ToolContext]]
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

    def _error(message: str) -> CallToolResult:
        """A failed call, shaped exactly like every other failure here."""
        return CallToolResult(
            content=[TextContent(type="text", text=message)],
            isError=True,
        )

    async def _list_tools(
        rctx: ServerRequestContext,
        params: PaginatedRequestParams | None = None,
    ) -> ListToolsResult:
        return ListToolsResult(
            tools=[
                Tool(
                    name=spec.name,
                    description=spec.description,
                    inputSchema=spec.input_schema,
                )
                for spec in registry.list(include_hoa=include_hoa)
            ]
        )

    async def _call_tool(
        rctx: ServerRequestContext,
        params: CallToolRequestParams,
    ) -> CallToolResult:
        name = params.name
        arguments: dict[str, Any] = dict(params.arguments or {})

        spec = registry.get(name)
        if spec is None:
            return _error(f"Unknown tool: {name!r}")

        # HoA gating: registry.list() filters by include_hoa, but a
        # malicious caller could still invoke a HoA tool by name. Enforce
        # the same allowlist here.
        if not include_hoa and name.startswith("hoa_"):
            return _error(f"Tool not available: {name!r}")

        # Validate arguments against the tool's declared inputSchema before
        # any handler sees them. mcp 1.x's call_tool decorator did this by
        # default; the 2.x callback does not, and this endpoint is reachable
        # by external clients, so the check lives here explicitly rather
        # than depending on a library default.
        if spec.input_schema:
            try:
                jsonschema.validate(instance=arguments, schema=spec.input_schema)
            except jsonschema.ValidationError as e:
                logger.info("Invalid arguments for %s: %s", name, e.message)
                return _error(f"Invalid arguments for {name!r}: {e.message}")
            except jsonschema.SchemaError:
                # A malformed schema is our bug, not the caller's. Refuse the
                # call rather than dispatching unvalidated arguments.
                logger.exception("Tool %s has an invalid inputSchema", name)
                return _error(f"Tool {name!r} has an invalid input schema")

        start = time.monotonic()
        try:
            ctx = await ctx_resolver(rctx)
        except Exception as e:
            logger.exception("Failed to resolve ToolContext for %s", name)
            return _error(f"Context error: {e}")

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
            return _error(f"Tool error: {e}")

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

    # mcp 2.x registers handlers as constructor callbacks; the 1.x
    # ``@server.list_tools()`` / ``@server.call_tool()`` decorators are gone.
    return Server(
        name=name,
        version=version,
        on_list_tools=_list_tools,
        on_call_tool=_call_tool,
    )
