"""Tests for nerve.agent.prompts — system-prompt assembly."""
from __future__ import annotations

from pathlib import Path

from nerve.agent import prompts
from nerve.agent.prompts import (
    _format_skills_list,
    _format_tool_list,
    build_system_prompt,
)


def test_format_tool_list_uses_mcp_prefix():
    """Tools must be advertised with the ``mcp__nerve__`` prefix.

    The Claude Agent SDK exposes Nerve's in-process MCP server tools as
    ``mcp__nerve__<name>``. If the prompt names the bare ``spec.name``
    instead, the agent calls the short form and the CLI returns
    "No such tool available". Regression test for that.
    """
    out = _format_tool_list()
    assert out, "tool list must not be empty"
    for line in out.splitlines():
        assert line.startswith("- `mcp__nerve__"), f"unprefixed tool in prompt: {line!r}"


def test_format_skills_list_uses_mcp_prefix():
    """The skills-section header must direct the agent at ``mcp__nerve__skill_get``."""
    section = _format_skills_list(
        skill_summaries=[{"id": "demo", "name": "demo", "description": "Test skill"}]
    )
    assert section is not None
    assert "mcp__nerve__skill_get" in section
    # And the bare form should NOT appear standalone — that's the bug we're fixing
    assert "Use `skill_get(name)`" not in section


def test_format_skills_list_returns_none_for_empty():
    assert _format_skills_list(None) is None
    assert _format_skills_list([]) is None


def test_build_system_prompt_smoke(tmp_path: Path):
    """Sanity check: full prompt builder includes the prefixed tool names."""
    # Reset cached registry so previous tests don't influence this one
    prompts._PROMPT_TOOL_REGISTRY = None

    prompt = build_system_prompt(workspace=tmp_path, session_id="t1", source="web")
    assert "# Session Context" in prompt
    assert "mcp__nerve__" in prompt, "prompt must advertise tools with mcp__nerve__ prefix"
