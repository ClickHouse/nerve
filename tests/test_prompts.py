"""Tests for nerve.agent.prompts — system-prompt assembly."""
from __future__ import annotations

import re
from pathlib import Path

from nerve.agent import prompts
from nerve.agent.prompts import (
    SESSION_CONTEXT_TAG,
    _format_skills_list,
    _format_tool_list,
    build_session_preamble,
    build_system_prompt,
    prepend_session_preamble,
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


# ---------------------------------------------------------------------------
# Static system prompt + per-session preamble.
#
# Anthropic prompt caching is exact-prefix: the appended system prompt is
# shared between sessions only if it is byte-identical, so nothing
# per-session (id, recall list) may be rendered into it.
# ---------------------------------------------------------------------------


def test_static_prompt_identical_across_sessions(tmp_path: Path):
    """Different ids and different recalled memories → the same bytes."""
    prompts._PROMPT_TOOL_REGISTRY = None
    (tmp_path / "SOUL.md").write_text("# Soul\nBe useful.\n", encoding="utf-8")

    a = build_system_prompt(
        workspace=tmp_path, session_id="sess-aaa-111", source="cron",
        recalled_memories=["alpha prior fact"],
    )
    b = build_system_prompt(
        workspace=tmp_path, session_id="sess-bbb-222", source="cron",
        recalled_memories=["beta prior fact", "gamma prior fact"],
    )
    assert a == b
    for leaked in (
        "sess-aaa-111", "sess-bbb-222", "alpha prior fact", "beta prior fact",
        "Session ID", "# Recalled Memories",
    ):
        assert leaked not in a, leaked
    # Everything else stays: identity files, context block, tools.
    assert "Be useful." in a
    assert "# Session Context" in a
    assert "- **Source:** cron" in a
    assert f"- **Workspace:** {tmp_path}" in a
    assert "mcp__nerve__" in a
    # The date is day-resolution only (a minute would roll the bytes).
    date_line = next(ln for ln in a.splitlines() if "Current date" in ln)
    assert re.search(r"\d{4}-\d{2}-\d{2}", date_line)
    assert not re.search(r"\d{2}:\d{2}", date_line)
    # The prompt says where the per-session details went.
    assert f"<{SESSION_CONTEXT_TAG}>" in a
    assert "mcp__nerve__session_context" in a


def test_build_session_preamble_carries_id_source_and_memories():
    pre = build_session_preamble(
        "sess-123", "telegram", ["remember x", "remember y"],
    )
    assert pre.startswith(
        "# Session Context\n- **Session ID:** sess-123\n- **Source:** telegram"
    )
    assert "# Recalled Memories\n\n- remember x\n- remember y" in pre
    # No memories → no memories section, id still present.
    bare = build_session_preamble("sess-123", "web", None)
    assert "- **Session ID:** sess-123" in bare
    assert "Recalled Memories" not in bare


def test_prepend_session_preamble_wraps_and_leads():
    out = prepend_session_preamble("do the thing", "# Session Context\n- x")
    assert out == (
        f"<{SESSION_CONTEXT_TAG}>\n# Session Context\n- x\n</{SESSION_CONTEXT_TAG}>"
        "\n\ndo the thing"
    )
    # Empty user text: just the block, no dangling separator.
    assert prepend_session_preamble("", "P") == (
        f"<{SESSION_CONTEXT_TAG}>\nP\n</{SESSION_CONTEXT_TAG}>"
    )


def test_static_false_restores_legacy_shape(tmp_path: Path):
    """``agent.static_system_prompt: false`` → id and recall inline again."""
    prompts._PROMPT_TOOL_REGISTRY = None
    p = build_system_prompt(
        workspace=tmp_path, session_id="sess-old", source="web",
        recalled_memories=["old fact"], static=False,
    )
    assert "- **Session ID:** sess-old" in p
    assert "# Recalled Memories\n\n- old fact" in p
    assert f"<{SESSION_CONTEXT_TAG}>" not in p
    # ...and two sessions therefore differ, as they did before.
    q = build_system_prompt(
        workspace=tmp_path, session_id="sess-new", source="web",
        recalled_memories=["old fact"], static=False,
    )
    assert p != q
