"""System prompt builder — loads SOUL.md, IDENTITY.md, and injects recalled memories."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from nerve.agent.tools import build_default_registry

logger = logging.getLogger(__name__)

# Module-level reference set during engine initialization
_skill_manager: Any = None

# Tool registry used purely for system-prompt listing. The engine has
# its own ``self.registry``; this one is built lazily on first use so
# importing ``prompts`` doesn't force handlers' optional imports to run.
_PROMPT_TOOL_REGISTRY = None


def _get_prompt_tool_registry():
    global _PROMPT_TOOL_REGISTRY
    if _PROMPT_TOOL_REGISTRY is None:
        _PROMPT_TOOL_REGISTRY = build_default_registry()
    return _PROMPT_TOOL_REGISTRY


def set_skill_manager(manager: Any) -> None:
    """Set the skill manager reference for system prompt building."""
    global _skill_manager
    _skill_manager = manager


def _format_tool_list(excluded_tools: "set[str] | None" = None) -> str:
    """Generate tool list for system prompt from the default registry.

    Tool names are prefixed with ``mcp__nerve__`` because that's how both
    runtimes expose them: the MCP server is named "nerve", and MCP tools
    are namespaced as ``mcp__<server>__<name>`` (Claude CLI and Codex use
    the same convention). Calling the bare ``spec.name`` fails.

    HoA tools are excluded — they don't usefully appear in the prompt
    when houseofagents is disabled, and including them when enabled
    bloats the prompt with rarely-used surface. The model still
    discovers them via the MCP tools/list call on first turn.

    ``excluded_tools`` mirrors the active backend's tool exclusions so
    the prompt never advertises a tool the session's MCP server doesn't
    serve (e.g. ``schedule_wakeup`` on the Claude backend, which has the
    ScheduleWakeup built-in instead).
    """
    excluded = excluded_tools or set()
    lines = []
    for spec in _get_prompt_tool_registry().list(include_hoa=False):
        if spec.name in excluded:
            continue
        # Take the first sentence of the description as the summary
        desc = spec.description.split("\n")[0].rstrip(".")
        lines.append(f"- `mcp__nerve__{spec.name}` — {desc}")
    return "\n".join(lines)


# Files loaded in order; missing files are silently skipped
PROMPT_FILES = ["SOUL.md", "TASK.md", "IDENTITY.md", "USER.md", "AGENTS.md", "TOOLS.md"]


def current_time_str(timezone_name: str = "America/New_York") -> str:
    """Minute-resolution local time, for the per-turn message reminder."""
    try:
        tz = ZoneInfo(timezone_name)
        return datetime.now(tz).strftime("%Y-%m-%d %H:%M %Z")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M")


def _read_if_exists(path: Path) -> str | None:
    """Read file content if it exists, otherwise return None."""
    try:
        if path.exists():
            return path.read_text(encoding="utf-8").strip()
    except Exception as e:
        logger.warning("Failed to read %s: %s", path, e)
    return None


def _format_skills_list(skill_summaries: list[dict] | None = None) -> str | None:
    """Generate the skills section for the system prompt (progressive disclosure level 1)."""
    if not skill_summaries:
        return None

    lines = [
        "# Available Skills",
        "",
        "The following skills are available. Use `mcp__nerve__skill_get(name)` to load a skill's full instructions when relevant.",
        "",
    ]
    for s in skill_summaries:
        desc = s["description"][:200].rstrip(".")
        lines.append(f"- **{s['name']}** (`{s['id']}`): {desc}")
    return "\n".join(lines)


def build_system_prompt(
    workspace: Path,
    session_id: str = "",
    source: str = "web",
    recalled_memories: list[str] | None = None,
    timezone_name: str = "America/New_York",
    skill_summaries: list[dict] | None = None,
    excluded_tools: "set[str] | None" = None,
) -> str:
    """Build the full system prompt for the agent.

    Loads identity files from workspace, adds session context,
    and appends any recalled memories from memU.
    """
    parts: list[str] = []

    # Load identity/soul files
    for filename in PROMPT_FILES:
        content = _read_if_exists(workspace / filename)
        if content:
            parts.append(content)

    # Load MEMORY.md (truncated to first 300 lines for context window)
    memory_content = _read_if_exists(workspace / "MEMORY.md")
    if memory_content:
        lines = memory_content.split("\n")
        if len(lines) > 300:
            truncated = "\n".join(lines[:300])
            parts.append(f"# MEMORY.md (first 300 lines)\n\n{truncated}\n\n... (truncated, {len(lines)} total lines)")
        else:
            parts.append(memory_content)

    # Session context. Deliberately date-resolution only: a minute-level
    # timestamp here changes the system-prompt bytes on every client
    # rebuild, invalidating the prompt cache for the entire conversation
    # replay (prefix match — see nerve/agent/cache_policy.py). Precise
    # wall-clock time is injected per turn as a trailing message reminder
    # by the engine instead, which is fresher and costs nothing cache-wise.
    try:
        tz = ZoneInfo(timezone_name)
        today = datetime.now(tz).strftime("%Y-%m-%d %Z")
    except Exception:
        today = datetime.now().strftime("%Y-%m-%d")

    context = f"""# Session Context
- **Session ID:** {session_id}
- **Source:** {source}
- **Current date:** {today}
- **Workspace:** {workspace}

You have access to the following custom tools:
{_format_tool_list(excluded_tools)}"""
    parts.append(context)

    # Skills summary (progressive disclosure level 1: name + description only)
    skills_section = _format_skills_list(skill_summaries)
    if skills_section:
        parts.append(skills_section)

    # Recalled memories from memU
    if recalled_memories:
        memories_text = "\n".join(f"- {m}" for m in recalled_memories)
        parts.append(f"# Recalled Memories\n\n{memories_text}")

    return "\n\n---\n\n".join(parts)
