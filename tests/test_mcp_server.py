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

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from mcp.types import (
    CallToolRequestParams,
    CallToolResult,
    ListToolsResult,
)

from nerve.agent.tools import ToolContext, ToolRegistry, ToolResult, ToolSpec
from nerve.mcp_server.server import build_mcp_server


def _make_spec(
    name: str,
    *,
    response_text: str = "ok",
    is_error: bool = False,
    raises: Exception | None = None,
    input_schema: dict | None = None,
    seen: list[dict] | None = None,
) -> ToolSpec:
    """Build a deterministic ToolSpec for protocol tests.

    ``seen`` records the arguments each invocation receives, so a test can
    assert a rejected call never reached the handler at all.
    """

    async def handler(ctx: ToolContext, args: dict) -> ToolResult:
        if seen is not None:
            seen.append(args)
        if raises is not None:
            raise raises
        return ToolResult.text(response_text, is_error=is_error)

    return ToolSpec(
        name=name,
        description=f"test tool {name}",
        input_schema=(
            input_schema
            if input_schema is not None
            else {"type": "object", "properties": {}, "required": []}
        ),
        handler=handler,
    )


async def _resolve_static_ctx(session_id: str = "external:test:s1") -> ToolContext:
    return ToolContext(session_id=session_id)


def _ctx_resolver(session_id: str = "external:test:s1"):
    # Takes the request context mcp 2.x passes to handlers; these tests don't
    # exercise per-request attribution, so it's accepted and ignored.
    async def _r(rctx: Any = None) -> ToolContext:
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

        # Invoke the registered list_tools handler directly, bypassing the
        # transport. mcp 2.x hands back the result model unwrapped.
        tools_result: ListToolsResult = await _invoke_list_tools(server)
        assert isinstance(tools_result, ListToolsResult)
        assert {t.name for t in tools_result.tools} == {"alpha", "beta"}

    async def test_list_tools_hides_hoa_by_default(self):
        registry = ToolRegistry()
        registry.register(_make_spec("regular"))
        registry.register(_make_spec("hoa_execute"))
        server = build_mcp_server(registry, ctx_resolver=_ctx_resolver())

        result = await _invoke_list_tools(server)
        names = {t.name for t in result.tools}
        assert names == {"regular"}

    async def test_list_tools_includes_hoa_when_opted_in(self):
        registry = ToolRegistry()
        registry.register(_make_spec("regular"))
        registry.register(_make_spec("hoa_execute"))
        server = build_mcp_server(
            registry, ctx_resolver=_ctx_resolver(), include_hoa=True,
        )

        result = await _invoke_list_tools(server)
        names = {t.name for t in result.tools}
        assert names == {"regular", "hoa_execute"}


# mcp 2.x dispatches through `get_request_handler(method)`, keyed by method
# string, and hands the handler the request context plus a params model. The
# old `request_handlers[RequestClass]` table and the `ServerResult` root
# wrapper are both gone, so these two helpers are the whole test seam.
_FAKE_RCTX = SimpleNamespace(request=None, session=None)


async def _invoke_list_tools(server: Any) -> ListToolsResult:
    entry = server.get_request_handler("tools/list")
    return await entry.handler(_FAKE_RCTX, None)


async def _invoke_call_tool(
    server: Any, name: str, args: dict | None = None,
) -> CallToolResult:
    entry = server.get_request_handler("tools/call")
    return await entry.handler(
        _FAKE_RCTX, CallToolRequestParams(name=name, arguments=args or {}),
    )


@pytest.mark.asyncio
class TestCallToolDispatch:
    """Verify call_tool routes to the correct registry handler."""

    async def test_dispatches_to_registered_handler(self):
        registry = ToolRegistry()
        registry.register(_make_spec("alpha", response_text="hello-alpha"))
        server = build_mcp_server(registry, ctx_resolver=_ctx_resolver())

        result = await _invoke_call_tool(server, "alpha")
        call_result: CallToolResult = result
        assert call_result.is_error is False
        assert call_result.content[0].text == "hello-alpha"

    async def test_returns_error_for_unknown_tool(self):
        registry = ToolRegistry()
        registry.register(_make_spec("alpha"))
        server = build_mcp_server(registry, ctx_resolver=_ctx_resolver())

        result = await _invoke_call_tool(server, "missing")
        call_result: CallToolResult = result
        assert call_result.is_error is True
        assert "Unknown tool" in call_result.content[0].text

    async def test_propagates_handler_is_error(self):
        registry = ToolRegistry()
        registry.register(_make_spec("alpha", response_text="boom", is_error=True))
        server = build_mcp_server(registry, ctx_resolver=_ctx_resolver())

        result = await _invoke_call_tool(server, "alpha")
        call_result: CallToolResult = result
        assert call_result.is_error is True
        assert call_result.content[0].text == "boom"

    async def test_handler_exception_returns_tool_error(self):
        registry = ToolRegistry()
        registry.register(
            _make_spec("crash", raises=RuntimeError("explosion")),
        )
        server = build_mcp_server(registry, ctx_resolver=_ctx_resolver())

        result = await _invoke_call_tool(server, "crash")
        call_result: CallToolResult = result
        assert call_result.is_error is True
        assert "explosion" in call_result.content[0].text

    async def test_hoa_tool_rejected_when_include_hoa_false(self):
        registry = ToolRegistry()
        registry.register(_make_spec("hoa_execute", response_text="should-not-run"))
        server = build_mcp_server(
            registry, ctx_resolver=_ctx_resolver(), include_hoa=False,
        )

        result = await _invoke_call_tool(server, "hoa_execute")
        call_result: CallToolResult = result
        assert call_result.is_error is True
        assert "not available" in call_result.content[0].text

    async def test_audit_writer_called_on_success(self):
        registry = ToolRegistry()
        registry.register(_make_spec("alpha", response_text="ack"))
        audit = AsyncMock()
        server = build_mcp_server(
            registry, ctx_resolver=_ctx_resolver("external:test:abc"),
            audit_writer=audit,
        )

        await _invoke_call_tool(server, "alpha", {"foo": "bar"})

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

        await _invoke_call_tool(server, "crash")

        audit.assert_awaited_once()
        args, _kwargs = audit.call_args
        _sid, _name, _args, result_obj, _ms, is_error = args
        assert is_error is True
        assert "nope" in result_obj.content[0]["text"]


_TYPED_SCHEMA = {
    "type": "object",
    "properties": {"count": {"type": "integer"}, "label": {"type": "string"}},
    "required": ["count"],
    "additionalProperties": False,
}


@pytest.mark.asyncio
class TestArgumentValidation:
    """Arguments are validated against the tool's inputSchema before dispatch.

    mcp 1.x's ``@server.call_tool()`` decorator validated by default
    (``validate_input=True``); the 2.x ``on_call_tool`` callback does not. The
    check therefore lives in ``build_mcp_server`` itself, and these tests pin
    it there — an externally reachable endpoint must not hand unvalidated
    arguments to a tool handler, and nothing in the library will complain if
    that protection quietly disappears again.
    """

    async def test_wrong_type_is_rejected_before_the_handler_runs(self):
        seen: list[dict] = []
        registry = ToolRegistry()
        registry.register(
            _make_spec("typed", input_schema=_TYPED_SCHEMA, seen=seen),
        )
        server = build_mcp_server(registry, ctx_resolver=_ctx_resolver())

        result = await _invoke_call_tool(server, "typed", {"count": "not-an-int"})

        assert result.is_error is True
        assert "Invalid arguments" in result.content[0].text
        assert seen == [], "handler ran despite invalid arguments"

    async def test_missing_required_argument_is_rejected(self):
        seen: list[dict] = []
        registry = ToolRegistry()
        registry.register(
            _make_spec("typed", input_schema=_TYPED_SCHEMA, seen=seen),
        )
        server = build_mcp_server(registry, ctx_resolver=_ctx_resolver())

        result = await _invoke_call_tool(server, "typed", {"label": "x"})

        assert result.is_error is True
        assert "Invalid arguments" in result.content[0].text
        assert seen == []

    async def test_unknown_property_is_rejected(self):
        seen: list[dict] = []
        registry = ToolRegistry()
        registry.register(
            _make_spec("typed", input_schema=_TYPED_SCHEMA, seen=seen),
        )
        server = build_mcp_server(registry, ctx_resolver=_ctx_resolver())

        result = await _invoke_call_tool(
            server, "typed", {"count": 1, "surprise": "!"},
        )

        assert result.is_error is True
        assert seen == []

    async def test_valid_arguments_reach_the_handler(self):
        seen: list[dict] = []
        registry = ToolRegistry()
        registry.register(
            _make_spec(
                "typed", response_text="done",
                input_schema=_TYPED_SCHEMA, seen=seen,
            ),
        )
        server = build_mcp_server(registry, ctx_resolver=_ctx_resolver())

        result = await _invoke_call_tool(
            server, "typed", {"count": 3, "label": "ok"},
        )

        assert result.is_error is False
        assert result.content[0].text == "done"
        assert seen == [{"count": 3, "label": "ok"}]

    async def test_malformed_schema_refuses_the_call(self):
        """A broken schema is our bug — refuse rather than skip validation."""
        seen: list[dict] = []
        registry = ToolRegistry()
        registry.register(
            _make_spec(
                "broken",
                input_schema={"type": "object", "properties": {"x": {"type": 5}}},
                seen=seen,
            ),
        )
        server = build_mcp_server(registry, ctx_resolver=_ctx_resolver())

        result = await _invoke_call_tool(server, "broken", {"x": 1})

        assert result.is_error is True
        assert "invalid input schema" in result.content[0].text
        assert seen == []
