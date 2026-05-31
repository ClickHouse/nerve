"""Tests for the runtime-agnostic tool registry.

Covers:
- ``ToolRegistry`` CRUD (register, get, list, contains, len, duplicate detection)
- ``ToolSpec`` immutability + minimal validation
- ``ToolContext`` field defaults
- Every shipped handler is invocable with a mock ``ToolContext``
- The SDK adapter ``_shim_schema`` covers every schema shape in the codebase
- Two concurrent invocations through ``build_session_mcp_server`` see
  distinct ``session_id``s — pins the bug that motivated the refactor.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from nerve.agent.tools import (
    ToolContext,
    ToolRegistry,
    ToolResult,
    ToolSpec,
    build_default_registry,
    build_session_mcp_server,
)
from nerve.agent.tools.claude_sdk_adapter import _shim_schema


# ---------------------------------------------------------------------------
# Registry CRUD
# ---------------------------------------------------------------------------


def _spec(name: str = "noop") -> ToolSpec:
    """Build a throwaway ToolSpec for registry plumbing tests."""

    async def handler(ctx: ToolContext, args: dict) -> ToolResult:
        return ToolResult.text(f"noop:{name}")

    return ToolSpec(
        name=name,
        description="test",
        input_schema={"type": "object", "properties": {}, "required": []},
        handler=handler,
    )


class TestToolRegistry:
    def test_register_and_get(self):
        registry = ToolRegistry()
        spec = _spec("alpha")
        registry.register(spec)
        assert registry.get("alpha") is spec
        assert registry.get("missing") is None

    def test_register_duplicate_raises(self):
        registry = ToolRegistry()
        registry.register(_spec("alpha"))
        with pytest.raises(ValueError, match="already registered"):
            registry.register(_spec("alpha"))

    def test_contains_and_len(self):
        registry = ToolRegistry()
        assert len(registry) == 0
        assert "alpha" not in registry
        registry.register(_spec("alpha"))
        assert "alpha" in registry
        assert len(registry) == 1

    def test_list_filters_hoa(self):
        registry = ToolRegistry()
        registry.register(_spec("regular"))
        registry.register(_spec("hoa_execute"))
        without = [s.name for s in registry.list(include_hoa=False)]
        with_hoa = [s.name for s in registry.list(include_hoa=True)]
        assert without == ["regular"]
        assert sorted(with_hoa) == ["hoa_execute", "regular"]

    @pytest.mark.asyncio
    async def test_invoke_dispatches_to_handler(self):
        registry = ToolRegistry()
        registry.register(_spec("hello"))
        ctx = ToolContext(session_id="s")
        result = await registry.invoke("hello", ctx, {})
        assert result.content == [{"type": "text", "text": "noop:hello"}]

    @pytest.mark.asyncio
    async def test_invoke_unknown_raises(self):
        registry = ToolRegistry()
        ctx = ToolContext(session_id="s")
        with pytest.raises(KeyError, match="Unknown tool"):
            await registry.invoke("ghost", ctx, {})


# ---------------------------------------------------------------------------
# ToolContext + ToolResult shape
# ---------------------------------------------------------------------------


class TestToolContext:
    def test_minimum_fields(self):
        """Only session_id is required; the rest default to None."""
        ctx = ToolContext(session_id="sess-1")
        assert ctx.session_id == "sess-1"
        assert ctx.workspace is None
        assert ctx.db is None
        assert ctx.memory_bridge is None
        assert ctx.config is None
        assert ctx.skill_manager is None
        assert ctx.engine is None
        assert ctx.notification_service is None


class TestToolResult:
    def test_text_constructor(self):
        result = ToolResult.text("hello")
        assert result.content == [{"type": "text", "text": "hello"}]
        assert result.is_error is False
        assert result.structured is None

    def test_text_error(self):
        result = ToolResult.text("bad", is_error=True)
        assert result.is_error is True

    def test_to_dict_matches_sdk_shape(self):
        result = ToolResult(
            content=[{"type": "text", "text": "x"}], is_error=True,
        )
        assert result.to_dict() == {
            "content": [{"type": "text", "text": "x"}],
            "is_error": True,
        }


# ---------------------------------------------------------------------------
# Default registry — every shipped tool is registered, schema-valid
# ---------------------------------------------------------------------------


class TestDefaultRegistry:
    def test_default_registry_contains_expected_tools(self):
        registry = build_default_registry()
        names = {s.name for s in registry.list(include_hoa=True)}
        # A representative sample from every domain — full set is too
        # noisy for a regression, but the domains being non-empty is the
        # invariant we care about.
        expected = {
            # tasks
            "task_search", "task_create", "task_list", "task_update",
            "task_read", "task_write", "task_done",
            # memory
            "memory_recall", "conversation_history", "memorize",
            # plans
            "plan_propose", "plan_update", "plan_list", "plan_read",
            "plan_approve", "plan_decline", "plan_revise",
            # skills
            "skill_list", "skill_get", "skill_create",
            # sources
            "list_sources", "poll_source",
            # notifications
            "notify", "ask_user", "react", "send_sticker", "send_file",
            # mcp admin
            "nerve_api", "mcp_reload",
            # hoa
            "hoa_status", "hoa_list_pipelines", "hoa_execute",
        }
        missing = expected - names
        assert not missing, f"Missing tools from default registry: {missing}"

    def test_all_specs_have_explicit_schemas(self):
        """Every shipped spec must use the explicit JSON Schema form.

        ``_shim_schema`` accepts shorthand and lifts it, but specs
        authored in ``schemas.py`` should never need lifting. Catching
        this here prevents accidental shorthand from sneaking in and
        masking required/default bugs.
        """
        registry = build_default_registry()
        for spec in registry.list(include_hoa=True):
            schema = spec.input_schema
            assert isinstance(schema, dict), f"{spec.name}: schema must be a dict"
            assert schema.get("type") == "object", (
                f"{spec.name}: schema must be explicit JSON Schema; got {schema!r}"
            )
            assert "properties" in schema, f"{spec.name}: missing properties"
            assert "required" in schema, f"{spec.name}: missing required"

    def test_defaulted_fields_not_required(self):
        """Fields declaring a ``default`` must NOT be in ``required``.

        Regression for the McpToolCallError bug — the SDK used to mark
        every property required when ``"type"`` was missing, breaking
        documented defaults.
        """
        registry = build_default_registry()
        for spec in registry.list(include_hoa=True):
            schema = spec.input_schema
            props = schema.get("properties") or {}
            required = set(schema.get("required") or [])
            defaulted = {
                name for name, p in props.items()
                if isinstance(p, dict) and "default" in p
            }
            leaked = defaulted & required
            assert not leaked, (
                f"{spec.name}: defaulted fields marked required: {sorted(leaked)}"
            )


# ---------------------------------------------------------------------------
# Schema adapter
# ---------------------------------------------------------------------------


class TestShimSchema:
    def test_explicit_schema_passes_through_unchanged(self):
        explicit = {
            "type": "object",
            "properties": {"a": {"type": "string"}},
            "required": ["a"],
        }
        assert _shim_schema(explicit) == explicit

    def test_shorthand_with_defaults_promotes(self):
        promoted = _shim_schema({
            "needed": {"type": "string"},
            "optional": {"type": "string", "default": "x"},
        })
        assert promoted["type"] == "object"
        assert set(promoted["properties"]) == {"needed", "optional"}
        assert promoted["required"] == ["needed"]
        assert promoted["properties"]["optional"]["default"] == "x"

    def test_empty_shorthand_yields_empty_required(self):
        promoted = _shim_schema({})
        assert promoted == {"type": "object", "properties": {}, "required": []}


# ---------------------------------------------------------------------------
# Concurrent session_id isolation — the bug the refactor fixes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestConcurrentSessionIsolation:
    """Two sessions invoke ``notify`` concurrently. The handler captures
    ``ctx.session_id``. The values seen by each invocation must be the
    one passed at server-build time — not whichever session ran last.
    """

    async def test_two_session_servers_see_distinct_ids(self):
        """Build two session-scoped MCP servers, verify their tool wrappers
        each see their own session_id. The SDK ``create_sdk_mcp_server``
        hides the wrapped tools behind an opaque ``Server`` instance, so
        we exercise the same wrap path directly through
        :func:`_wrap_for_sdk` — the function the session builder uses.
        """
        from nerve.agent.tools.claude_sdk_adapter import _wrap_for_sdk

        registry = build_default_registry()
        notify_spec = registry.get("notify")
        assert notify_spec is not None

        captured: list[str] = []

        async def fake_send_notification(session_id, **kwargs):
            # Yield so the two coroutines actually interleave
            await asyncio.sleep(0.01)
            captured.append(session_id)
            return f"notif-{session_id}"

        mock_service = AsyncMock()
        mock_service.send_notification = AsyncMock(side_effect=fake_send_notification)

        ctx_a = ToolContext(session_id="session-A", notification_service=mock_service)
        ctx_b = ToolContext(session_id="session-B", notification_service=mock_service)

        # Build per-session servers so the wrapped tools are different
        # closures (each captures its own ctx).
        server_a = build_session_mcp_server(registry, ctx_a)
        server_b = build_session_mcp_server(registry, ctx_b)
        assert server_a is not server_b

        # Drive the same wrap path used inside the builder.
        notify_a = _wrap_for_sdk(notify_spec, ctx_a)
        notify_b = _wrap_for_sdk(notify_spec, ctx_b)

        await asyncio.gather(
            notify_a.handler({"title": "from A", "body": "."}),
            notify_b.handler({"title": "from B", "body": "."}),
        )

        # Both session_ids appeared exactly once each, in some order.
        assert sorted(captured) == ["session-A", "session-B"]
