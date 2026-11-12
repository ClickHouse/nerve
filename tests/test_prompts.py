"""Tests for nerve.agent.prompts — system-prompt assembly."""
from __future__ import annotations

import re
from pathlib import Path

from nerve.agent import prompts
from nerve.agent.prompts import (
    AUTOMATION_OFFER_SECTION,
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


# ---------------------------------------------------------------------------
# platform.md in the config directory, written by whoever runs the instance.
# ---------------------------------------------------------------------------


def _workspace_and_config_dir(tmp_path: Path) -> tuple[Path, Path]:
    prompts._PROMPT_TOOL_REGISTRY = None
    ws, config_dir = tmp_path / "ws", tmp_path / "cfg"
    ws.mkdir()
    config_dir.mkdir()
    (ws / "TOOLS.md").write_text("# TOOLS\ntools file\n", encoding="utf-8")
    (ws / "MEMORY.md").write_text("# MEMORY\nmemory file\n", encoding="utf-8")
    return ws, config_dir


def test_platform_md_goes_after_tools_and_before_memory(tmp_path: Path):
    ws, config_dir = _workspace_and_config_dir(tmp_path)
    (config_dir / "platform.md").write_text("# This instance\n4 vCPUs\n", encoding="utf-8")
    prompt = build_system_prompt(workspace=ws, config_dir=config_dir)
    assert (
        "# TOOLS\ntools file\n\n---\n\n# This instance\n4 vCPUs\n\n---\n\n# MEMORY\nmemory file"
    ) in prompt


def test_no_platform_md_leaves_the_prompt_byte_identical(tmp_path: Path):
    ws, config_dir = _workspace_and_config_dir(tmp_path)
    before = build_system_prompt(workspace=ws)
    assert build_system_prompt(workspace=ws, config_dir=config_dir) == before


def test_platform_md_over_16_kib_is_cut_with_a_marker(tmp_path: Path):
    ws, config_dir = _workspace_and_config_dir(tmp_path)
    kept = "a" * (16 * 1024)
    (config_dir / "platform.md").write_text(kept + "dropped", encoding="utf-8")
    prompt = build_system_prompt(workspace=ws, config_dir=config_dir)
    assert f"\n\n---\n\n{kept}\n[platform notes truncated at 16 KiB]\n\n---\n\n# MEMORY" in prompt
    assert "dropped" not in prompt


# ---------------------------------------------------------------------------
# The "Rendering" line: what the surface a session is read on draws.
# ---------------------------------------------------------------------------


def _rendering_line(prompt: str) -> str:
    return next(ln for ln in prompt.splitlines() if ln.startswith("- **Rendering:**"))


def test_web_prompt_names_the_fence_languages_the_chat_renders(tmp_path: Path):
    """``web`` sessions, and the ``api`` sessions the web UI shows as well."""
    prompts._PROMPT_TOOL_REGISTRY = None
    for source in ("web", "api"):
        line = _rendering_line(build_system_prompt(workspace=tmp_path, source=source))
        assert "renders" in line, source
        for fence in ("```mermaid", "```vega-lite", "```svg"):
            assert fence in line, source


def test_text_channels_are_told_not_to_draw(tmp_path: Path):
    prompts._PROMPT_TOOL_REGISTRY = None
    for source in ("telegram", "slack", "cron"):
        prompt = build_system_prompt(workspace=tmp_path, source=source)
        assert f"- **Source:** {source}" in prompt
        assert "Do not emit ```mermaid" in _rendering_line(prompt), source


def test_rendering_follows_the_session_surface_not_the_turn(tmp_path: Path):
    """A wakeup turn of a web session is still read in the web chat."""
    prompts._PROMPT_TOOL_REGISTRY = None
    prompt = build_system_prompt(workspace=tmp_path, source="wakeup", rendering_source="web")
    assert "- **Source:** wakeup" in prompt
    assert "renders fenced ```mermaid" in _rendering_line(prompt)
    legacy = build_system_prompt(
        workspace=tmp_path, source="wakeup", rendering_source="web", static=False,
    )
    assert "renders fenced ```mermaid" in _rendering_line(legacy)
    assert "Do not emit" in _rendering_line(build_system_prompt(workspace=tmp_path, source="wakeup"))


# ---------------------------------------------------------------------------
# The automation offer: only sessions that a person chats in.
# ---------------------------------------------------------------------------


def test_chat_sessions_are_told_to_offer_automation(tmp_path: Path):
    prompts._PROMPT_TOOL_REGISTRY = None
    for source in ("web", "telegram", "slack"):
        for static in (True, False):
            prompt = build_system_prompt(workspace=tmp_path, source=source, static=static)
            assert prompt.count(AUTOMATION_OFFER_SECTION) == 1, (source, static)


def test_sessions_without_a_person_get_no_automation_offer(tmp_path: Path):
    """A cron, hook or workflow session has no person to answer the offer."""
    prompts._PROMPT_TOOL_REGISTRY = None
    for source in ("cron", "hook", "wakeup", "workflow", "api"):
        prompt = build_system_prompt(workspace=tmp_path, source=source)
        assert "# Offer to automate" not in prompt, source


def test_automation_offer_follows_the_session_surface_not_the_turn(tmp_path: Path):
    """A wakeup turn of a web session gets the offer; one of a cron session does not."""
    prompts._PROMPT_TOOL_REGISTRY = None
    web = build_system_prompt(workspace=tmp_path, source="wakeup", rendering_source="web")
    assert AUTOMATION_OFFER_SECTION in web
    cron = build_system_prompt(workspace=tmp_path, source="wakeup", rendering_source="cron")
    assert "# Offer to automate" not in cron


def test_automation_offer_comes_before_the_skills(tmp_path: Path):
    prompts._PROMPT_TOOL_REGISTRY = None
    prompt = build_system_prompt(
        workspace=tmp_path, source="web",
        skill_summaries=[{"id": "s1", "name": "S1", "description": "Does a thing."}],
    )
    assert prompt.index("# Offer to automate") < prompt.index("# Available Skills")


def test_automation_offer_covers_delivery_reload_and_refusal():
    """The job must send its result itself, a direct edit needs a reload, and
    a "no" goes to a file that every later session reads."""
    for words in (
        "`notify`", "only to the cron log", "`nerve reload`", "after the merge",
        "`USER.md`", "`MEMORY.md`", "`nerve-workspace` skill", "only after a yes",
    ):
        assert words in AUTOMATION_OFFER_SECTION, words
