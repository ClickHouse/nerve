"""Tests for the tools.py schema-promotion wrapper.

Regression test for the bug where the Claude Agent SDK's _build_schema
forces every property of a shorthand input_schema into "required",
ignoring "default" annotations and silently dropping descriptions.
The wrapper in nerve.agent.tools.tool fixes this by pre-promoting
shorthand dicts to explicit JSON Schema before handing them to the SDK.
"""

import pytest

from nerve.agent.tools import ALL_TOOLS, tool


def _properties_with_defaults(schema: dict) -> set[str]:
    """Return the set of property names that declare a default."""
    props = schema.get("properties") or {}
    return {
        name
        for name, spec in props.items()
        if isinstance(spec, dict) and "default" in spec
    }


class TestSchemaWrapper:
    """The wrapper promotes shorthand dicts to explicit JSON Schema."""

    def test_shorthand_with_defaults_is_promoted(self):
        """Fields with a "default" land outside "required"; bare fields stay required."""

        @tool(
            "fixture",
            "doc",
            {
                "required_field": {"type": "string", "description": "must be set"},
                "optional_with_default": {
                    "type": "string",
                    "description": "has a default",
                    "default": "x",
                },
            },
        )
        async def _handler(args: dict) -> dict:  # pragma: no cover - never invoked
            return {"content": [{"type": "text", "text": "ok"}]}

        schema = _handler.input_schema
        assert schema["type"] == "object"
        assert set(schema["properties"]) == {"required_field", "optional_with_default"}
        # The original property dicts (including descriptions and defaults) are preserved.
        assert schema["properties"]["optional_with_default"]["description"] == "has a default"
        assert schema["properties"]["optional_with_default"]["default"] == "x"
        # Only fields without a default are required.
        assert schema["required"] == ["required_field"]

    def test_explicit_schema_is_passed_through_unchanged(self):
        """An explicit JSON Schema is preserved verbatim."""
        explicit = {
            "type": "object",
            "properties": {
                "a": {"type": "string"},
                "b": {"type": "integer", "default": 0},
            },
            "required": ["a"],
        }

        @tool("fixture-explicit", "doc", explicit)
        async def _handler(args: dict) -> dict:  # pragma: no cover - never invoked
            return {"content": [{"type": "text", "text": "ok"}]}

        assert _handler.input_schema == explicit

    def test_no_defaults_means_everything_required(self):
        """Backwards-compatible: a shorthand schema with no defaults stays fully required."""

        @tool(
            "fixture-bare",
            "doc",
            {
                "x": {"type": "string"},
                "y": {"type": "string"},
            },
        )
        async def _handler(args: dict) -> dict:  # pragma: no cover - never invoked
            return {"content": [{"type": "text", "text": "ok"}]}

        assert sorted(_handler.input_schema["required"]) == ["x", "y"]

    def test_zero_field_shorthand(self):
        """A tool with no parameters yields an empty required list."""

        @tool("fixture-empty", "doc", {})
        async def _handler(args: dict) -> dict:  # pragma: no cover - never invoked
            return {"content": [{"type": "text", "text": "ok"}]}

        schema = _handler.input_schema
        assert schema["type"] == "object"
        assert schema["properties"] == {}
        assert schema["required"] == []


class TestAllToolsAreCorrectlyBuilt:
    """Every registered tool must expose an explicit JSON Schema with a
    correct "required" list (no field that declares a default appears
    in required). This catches future regressions if a tool is added
    using the bare claude_agent_sdk.tool decorator instead of the
    Nerve wrapper.
    """

    @pytest.mark.parametrize("tool_def", ALL_TOOLS, ids=lambda t: t.name)
    def test_tool_schema_is_explicit(self, tool_def):
        schema = tool_def.input_schema
        assert isinstance(schema, dict), (
            f"{tool_def.name} input_schema must be a dict, got {type(schema)!r}"
        )
        assert schema.get("type") == "object", (
            f"{tool_def.name} input_schema must be promoted to "
            f'{{"type": "object", ...}}; got {schema!r}'
        )
        assert "properties" in schema, f"{tool_def.name} schema is missing properties"
        assert "required" in schema, f"{tool_def.name} schema is missing required"

    @pytest.mark.parametrize("tool_def", ALL_TOOLS, ids=lambda t: t.name)
    def test_no_defaulted_field_is_required(self, tool_def):
        schema = tool_def.input_schema
        defaulted = _properties_with_defaults(schema)
        required = set(schema.get("required") or [])
        leaked = defaulted & required
        assert not leaked, (
            f"{tool_def.name} marks fields with a default as required: "
            f"{sorted(leaked)} (this would cause the model to receive a "
            f'schema where "optional with default" fields are still required, '
            f"reproducing the McpToolCallError stream-tear-down bug)"
        )
