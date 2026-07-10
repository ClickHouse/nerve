"""Claude Agent SDK adapter for the runtime-agnostic tool registry.

Builds in-process MCP servers (``claude_agent_sdk.create_sdk_mcp_server``)
from a :class:`ToolRegistry` and a per-session :class:`ToolContext`. Other
runtime adapters (stdio MCP, Streamable HTTP, JSON-RPC) will live alongside
this one and consume the same registry without code duplication.

The two public entry points:

  * :func:`build_session_mcp_server` — production path. The session_id and
    every collaborator are captured in closures per tool, so concurrent
    sessions never share state.

  * :func:`build_nerve_mcp_server` — legacy shared server. Kept only so
    existing tests that exercise the "shared" path continue to pass; new
    code should never call it.

The wrapper :func:`tool` and :func:`_shim_schema` preserve a behavior
contract from the previous monolithic tools.py: shorthand input schemas
(bare property dicts without a top-level ``"type"``) are promoted to the
explicit JSON Schema form before being handed to the SDK, so fields with
a documented ``default`` aren't silently forced into ``required``.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from claude_agent_sdk import SdkMcpTool, create_sdk_mcp_server
from claude_agent_sdk import tool as _sdk_tool

from nerve.agent.tools.registry import (
    ToolContext,
    ToolRegistry,
    ToolResult,
    ToolSpec,
)

if TYPE_CHECKING:
    pass


def _shim_schema(input_schema: dict) -> dict:
    """Promote a shorthand schema to explicit JSON Schema form.

    The Claude Agent SDK's ``_build_schema`` converts shorthand dicts
    (those without a top-level ``"type"`` key) by forcing every property
    into ``required: list(properties.keys())`` and silently discarding
    descriptions and defaults. The result was that fields with a
    ``"default"`` annotation were still advertised as required to the
    model, and descriptions never made it through.

    Combined with the Claude Code CLI's behaviour of throwing
    ``McpToolCallError`` on validation failures (which propagates up the
    streaming connection and ends the agent's turn early), this caused
    agents to abort mid-task whenever the model trusted a documented
    default and omitted the field.

    The fix: pre-promote shorthand to the explicit form. Properties are
    kept intact (so descriptions and defaults survive) and ``required``
    only lists fields that have no ``default``. Tools that already supply
    an explicit ``{"type": "object", ...}`` schema are untouched.

    Most schemas in ``schemas.py`` are already explicit; this shim mostly
    exists for ad-hoc schemas built outside that module.
    """
    if isinstance(input_schema, dict) and "type" not in input_schema:
        return {
            "type": "object",
            "properties": dict(input_schema),
            "required": [
                field
                for field, spec in input_schema.items()
                if not isinstance(spec, dict) or "default" not in spec
            ],
        }
    return input_schema


def tool(name: str, description: str, input_schema, *args, **kwargs):
    """Schema-promotion wrapper around ``claude_agent_sdk.tool``.

    Kept as a public re-export so callers outside this package that built
    ad-hoc tools through the old shorthand can continue to do so.
    """
    return _sdk_tool(name, description, _shim_schema(input_schema), *args, **kwargs)


def _wrap_for_sdk(spec: ToolSpec, ctx: ToolContext) -> SdkMcpTool:
    """Build an SdkMcpTool that calls ``spec.handler`` with the supplied ctx.

    The ctx is captured in the closure, so concurrent sessions each get
    their own wrapped tool — no module global, no race.
    """
    schema = _shim_schema(spec.input_schema)

    @_sdk_tool(spec.name, spec.description, schema)
    async def _sdk_handler(args: dict) -> dict:
        result = await spec.handler(ctx, args)
        return result.to_dict()

    return _sdk_handler


def build_session_mcp_server(
    registry: ToolRegistry,
    ctx: ToolContext,
    *,
    include_hoa: bool = False,
    exclude: "set[str] | None" = None,
) -> dict:
    """Build the per-session in-process MCP server.

    ``ctx.session_id`` is bound into each tool's closure so notify/
    ask_user/react/etc. always reference the correct session — no shared
    global, no race under concurrent sessions.

    ``exclude`` drops tools by name — used by agent backends to hide
    tools that duplicate a runtime built-in (e.g. the Claude backend
    excludes ``schedule_wakeup``; the CLI's ScheduleWakeup covers it).

    The returned dict matches the SDK's ``McpSdkServerConfig`` shape;
    ``alwaysLoad`` is set to ``True`` so the Claude Code CLI skips tool-
    search deferral on first turn (requires CLI >= 2.1.121; silently
    ignored on older versions). The nerve MCP's tools are used on almost
    every turn — deferring them adds a ToolSearch round-trip for no benefit.
    """
    excluded = exclude or set()
    sdk_tools = [
        _wrap_for_sdk(spec, ctx)
        for spec in registry.list(include_hoa=include_hoa)
        if spec.name not in excluded
    ]
    config = create_sdk_mcp_server(name="nerve", version="1.0.0", tools=sdk_tools)
    config["alwaysLoad"] = True  # type: ignore[typeddict-unknown-key]
    return config


def build_nerve_mcp_server(registry: ToolRegistry) -> dict:
    """Legacy shared server — kept for test backwards-compat only.

    Notification-style tools (notify/ask_user/react/send_sticker) in this
    server have no session_id bound, so they fall back to the legacy
    ``_current_session_id`` global if anyone calls them through this path.
    New code MUST NOT use this server; ``build_session_mcp_server`` is the
    production path.
    """
    ctx = ToolContext(session_id="unknown")
    sdk_tools = [_wrap_for_sdk(spec, ctx) for spec in registry.list(include_hoa=False)]
    config = create_sdk_mcp_server(name="nerve", version="1.0.0", tools=sdk_tools)
    config["alwaysLoad"] = True  # type: ignore[typeddict-unknown-key]
    return config
