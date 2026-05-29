"""Protocol-level tests for the external MCP server module.

Covers the contract between :class:`ToolRegistry` and
:class:`mcp.server.lowlevel.Server` exposed by
:func:`nerve.mcp_server.build_mcp_server`. Stays clear of full HTTP
transport — those flows are exercised indirectly through the lifespan
in :mod:`test_satellite_sessions`. Here we just want to verify that
``list_tools`` enumerates the registry, ``call_tool`` dispatches to the
right handler, errors are returned correctly, the HoA gate is honored,
and the audit writer is invoked with the expected payload.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from mcp.types import (
    CallToolRequest,
    CallToolRequestParams,
    ClientRequest,
    ListToolsRequest,
    ListToolsResult,
    CallToolResult,
)

from nerve.agent.tools import ToolContext, ToolRegistry, ToolResult, ToolSpec
from nerve.mcp_server.server import build_mcp_server


def _make_spec(
    name: str,
    *,
    response_text: str = "ok",
    is_error: bool = False,
    raises: Exception | None = None,
) -> ToolSpec:
    """Build a deterministic ToolSpec for protocol tests."""

    async def handler(ctx: ToolContext, args: dict) -> ToolResult:
        if raises is not None:
            raise raises
        return ToolResult.text(response_text, is_error=is_error)

    return ToolSpec(
        name=name,
        description=f"test tool {name}",
        input_schema={"type": "object", "properties": {}, "required": []},
        handler=handler,
    )


async def _resolve_static_ctx(session_id: str = "external:test:s1") -> ToolContext:
    return ToolContext(session_id=session_id)


def _ctx_resolver(session_id: str = "external:test:s1"):
    async def _r() -> ToolContext:
        return ToolContext(session_id=session_id)
    return _r


@pytest.mark.asyncio
class TestBuildMcpServer:
    """Verify the Server <-> ToolRegistry binding."""

    async def test_list_tools_returns_registry_entries(self):
        registry = ToolRegistry()
        registry.register(_make_spec("alpha"))
        registry.register(_make_spec("beta"))
        server = build_mcp_server(registry, ctx_resolver=_ctx_resolver())

        # Invoke the registered list_tools handler directly via the
        # SDK's request_handlers dispatch table.
        handler = server.request_handlers[ListToolsRequest]
        result = await handler(ListToolsRequest(method="tools/list"))
        # ServerResult is the wrapping discriminated union.
        tools_result: ListToolsResult = result.root
        assert isinstance(tools_result, ListToolsResult)
        assert {t.name for t in tools_result.tools} == {"alpha", "beta"}

    async def test_list_tools_hides_hoa_by_default(self):
        registry = ToolRegistry()
        registry.register(_make_spec("regular"))
        registry.register(_make_spec("hoa_execute"))
        server = build_mcp_server(registry, ctx_resolver=_ctx_resolver())

        handler = server.request_handlers[ListToolsRequest]
        result = await handler(ListToolsRequest(method="tools/list"))
        names = {t.name for t in result.root.tools}
        assert names == {"regular"}

    async def test_list_tools_includes_hoa_when_opted_in(self):
        registry = ToolRegistry()
        registry.register(_make_spec("regular"))
        registry.register(_make_spec("hoa_execute"))
        server = build_mcp_server(
            registry, ctx_resolver=_ctx_resolver(), include_hoa=True,
        )

        handler = server.request_handlers[ListToolsRequest]
        result = await handler(ListToolsRequest(method="tools/list"))
        names = {t.name for t in result.root.tools}
        assert names == {"regular", "hoa_execute"}


def _build_call_request(name: str, args: dict | None = None) -> CallToolRequest:
    return CallToolRequest(
        method="tools/call",
        params=CallToolRequestParams(name=name, arguments=args or {}),
    )


@pytest.mark.asyncio
class TestCallToolDispatch:
    """Verify call_tool routes to the correct registry handler."""

    async def test_dispatches_to_registered_handler(self):
        registry = ToolRegistry()
        registry.register(_make_spec("alpha", response_text="hello-alpha"))
        server = build_mcp_server(registry, ctx_resolver=_ctx_resolver())

        handler = server.request_handlers[CallToolRequest]
        result = await handler(_build_call_request("alpha"))
        call_result: CallToolResult = result.root
        assert call_result.isError is False
        assert call_result.content[0].text == "hello-alpha"

    async def test_returns_error_for_unknown_tool(self):
        registry = ToolRegistry()
        registry.register(_make_spec("alpha"))
        server = build_mcp_server(registry, ctx_resolver=_ctx_resolver())

        handler = server.request_handlers[CallToolRequest]
        result = await handler(_build_call_request("missing"))
        call_result: CallToolResult = result.root
        assert call_result.isError is True
        assert "Unknown tool" in call_result.content[0].text

    async def test_propagates_handler_is_error(self):
        registry = ToolRegistry()
        registry.register(_make_spec("alpha", response_text="boom", is_error=True))
        server = build_mcp_server(registry, ctx_resolver=_ctx_resolver())

        handler = server.request_handlers[CallToolRequest]
        result = await handler(_build_call_request("alpha"))
        call_result: CallToolResult = result.root
        assert call_result.isError is True
        assert call_result.content[0].text == "boom"

    async def test_handler_exception_returns_tool_error(self):
        registry = ToolRegistry()
        registry.register(
            _make_spec("crash", raises=RuntimeError("explosion")),
        )
        server = build_mcp_server(registry, ctx_resolver=_ctx_resolver())

        handler = server.request_handlers[CallToolRequest]
        result = await handler(_build_call_request("crash"))
        call_result: CallToolResult = result.root
        assert call_result.isError is True
        assert "explosion" in call_result.content[0].text

    async def test_hoa_tool_rejected_when_include_hoa_false(self):
        registry = ToolRegistry()
        registry.register(_make_spec("hoa_execute", response_text="should-not-run"))
        server = build_mcp_server(
            registry, ctx_resolver=_ctx_resolver(), include_hoa=False,
        )

        handler = server.request_handlers[CallToolRequest]
        result = await handler(_build_call_request("hoa_execute"))
        call_result: CallToolResult = result.root
        assert call_result.isError is True
        assert "not available" in call_result.content[0].text

    async def test_audit_writer_called_on_success(self):
        registry = ToolRegistry()
        registry.register(_make_spec("alpha", response_text="ack"))
        audit = AsyncMock()
        server = build_mcp_server(
            registry, ctx_resolver=_ctx_resolver("external:test:abc"),
            audit_writer=audit,
        )

        handler = server.request_handlers[CallToolRequest]
        await handler(_build_call_request("alpha", {"foo": "bar"}))

        audit.assert_awaited_once()
        args, _kwargs = audit.call_args
        sid, tool_name, args_dict, result_obj, duration_ms, is_error = args
        assert sid == "external:test:abc"
        assert tool_name == "alpha"
        assert args_dict == {"foo": "bar"}
        assert result_obj.content[0]["text"] == "ack"
        assert is_error is False
        assert duration_ms >= 0.0

    async def test_audit_writer_called_on_exception(self):
        registry = ToolRegistry()
        registry.register(_make_spec("crash", raises=RuntimeError("nope")))
        audit = AsyncMock()
        server = build_mcp_server(
            registry, ctx_resolver=_ctx_resolver(),
            audit_writer=audit,
        )

        handler = server.request_handlers[CallToolRequest]
        await handler(_build_call_request("crash"))

        audit.assert_awaited_once()
        args, _kwargs = audit.call_args
        _sid, _name, _args, result_obj, _ms, is_error = args
        assert is_error is True
        assert "nope" in result_obj.content[0]["text"]
