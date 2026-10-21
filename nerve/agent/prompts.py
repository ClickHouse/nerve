"""System prompt builder — loads SOUL.md, IDENTITY.md, etc. into a
cross-session-stable system prompt, and renders the per-session preamble
(session id, recalled memories) that travels with the first user message."""

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


# --- The worker's first-boot onboarding prompt --------------------------------

WORKER_GUIDE_URL = "https://github.com/ClickHouse/nerve/blob/main/docs/worker-best-practices.md"

_ONBOARDING_STEPS = """\
## Steps

### 1. Research

Fetch the URLs the description names (repositories, docs, APIs, dashboards); \
clone the repositories the worker will work in; look at the CI systems, \
databases and APIs involved; search the web for what you do not know. Keep \
notes: the References section, the skills and the cron prompts come from them. \
Record where each credential lives and how it is passed — never its value.

### 2. Fill TASK.md in place

`<workspace>/TASK.md` (already in your system prompt) is the worker template \
with the description under `## Task description (as given)`. Fill every \
section where it stands:

- Keep every `##` heading, in its order, and keep the description section as \
the operator wrote it.
- Replace each `_(onboarding fills this in: …)_` placeholder and every \
`Example:` line with this worker's content; the guide's anatomy table says \
what belongs in each section and how long it may be.
- Hard rules: one `### ⛔ HARD RULE: <one sentence of rule> [operator directive \
YYYY-MM-DD]` heading per rule (`[added YYYY-MM-DD]` for a rule you derive), \
the rule before any why, failing closed by enumeration. Rules the operator \
stated in the description are operator directives; date them today.
- Actions: the transition table (`| From | Event | To | Who holds the ball |`) \
followed by an anti-patterns list. Statuses beyond `pending`, `in_progress`, \
`done` and `deferred` are created once, with `task_status_create`.
- Approval Required: the autonomous column is a standing grant; until it says \
so, `SOUL.md`'s default — ask before anything external — applies.
- References: pointers (auth mode, schema columns, canonical filters, label \
vocabulary), never secrets; the long form goes to `TOOLS.md` under a dated \
heading.
- Keep the closing `Read the full guide:` link. Keep the file within about 4 KB: \
rationale, measurements and history go to `memory/contexts/`, not here.

### 3. Create skills

`skill_create` one skill per procedure the worker will repeat: the \
build-and-test routine, querying the failure database or CI API, reproducing \
a failure class, publishing a PR the way the project expects. Steps, exact \
commands, gotchas and an example in each; a third-person description with \
trigger phrases.

### 4. Configure cron jobs

Append the worker's jobs to `<workspace>/config/cron/jobs.yaml` — Nerve's \
scheduler (`nerve cron <id>` runs one by hand), not the system crontab. One job \
per line of `TASK.md` § Triggers. Gate a job with `run_if` where the guide says, \
so it stays idle until there is work: a `tasks` gate on the status and tag a \
work job owns, a `messages` gate for inbox-driven \
work, a `github_pr_activity` gate (with the bot's login as `author`) for a PR \
monitor. Scan jobs that create the work run on their schedule, ungated. \
`lock: true` and `catchup: false` on anything that may outrun its interval. \
Procedure for one job goes in `config/cron/prompts/<job>.md`, loaded with \
`prompt_file` and opening with "`TASK.md` is the kernel; this file is your \
charter for the <job> phase." Format:

```yaml
jobs:
  - id: work-queue
    schedule: '*/15 * * * *'
    description: Work one actionable task per run
    session_mode: isolated          # persistent keeps context across runs
    lock: true
    catchup: false
    run_if:
      - type: tasks
        status: pending
        tag: source:ci-main
    enabled: true
    prompt_file: prompts/work-queue.md   # or an inline `prompt: |`
```

Then decide on the planner. The built-in jobs are in \
`<workspace>/config/cron/system.yaml`: for a worker `task-planner` ships \
disabled (after `nerve init --terminal` the skill jobs are disabled too). When \
this worker should plan through the approval loop, enable `task-planner` by \
overriding it by id in `jobs.yaml` (a job there replaces the `system.yaml` job \
with the same id): `enabled: true`, with a prompt and a `tasks` gate \
(`status: pending` and this worker's `tag`) scoped to the tasks it plans. Leave \
it off when a work job with a standing grant covers the routine items. Say \
which in `TASK.md` § Approval Required.

### 5. Create initial tasks

`task_create` one task per thing only the operator can do: credentials to \
grant, accounts to create, access to request, decisions the description leaves \
open.

### 6. Walk through everything you did, then give next steps

First `memorize` what you learned about the domain (`knowledge`) and what you \
decided (`event`); the next session starts from zero. Then `notify` (priority \
`high`) a walkthrough, and after step 7 end the session with the same \
walkthrough as your final message. Both enumerate every change:

- the `TASK.md` sections you filled;
- the statuses you created;
- the skills you created, by name;
- the cron jobs you added or enabled: id, schedule, gate, session mode, and \
whether it is enabled;
- the tasks you created: their ids and what the operator must do for each;
- what you memorized.

Close both with a numbered **Next steps for the operator**: credentials and \
access to grant (with the task ids), plans to approve, jobs to review or \
enable, how to run a job by hand (`nerve cron <id>`), and what to watch during \
the first runs.

### 7. Delete the marker — last

Only when every section of `TASK.md` is filled, delete its second line, the \
`<!-- nerve:onboarding pending … -->` comment. That line is how Nerve knows \
onboarding is still due: while it stands, the setup page reports onboarding as \
pending and the next start runs this session again; once it is gone, the file \
is the worker's. Do not remove it before the file is complete, and do not add \
it back.
"""


def worker_onboarding_prompt(task_description: str, guide: str) -> str:
    """The first-boot onboarding session's prompt for a worker.

    Role line, the operator's description, the worker guide in full (it is
    the standard the filled ``TASK.md`` is held to, so the session gets the
    whole text rather than a link), then the steps. ``guide`` is the text of
    ``docs/worker-best-practices.md`` (:func:`nerve.workspace.read_worker_guide`);
    when it is empty the prompt points at the published copy instead.
    """
    quoted = "\n".join(
        f"> {line}" if line.strip() else ">" for line in task_description.strip().splitlines()
    ) or "> (no description was given)"
    body = guide.strip() or (
        "(The guide could not be read from this installation. Fetch it from "
        f"{WORKER_GUIDE_URL} and read it in full before step 2.)"
    )
    return (
        "You are running the **first-boot onboarding** for this Nerve worker: one "
        "session with full tool access that turns the operator's description into "
        "a configured worker. Every later session is short and starts from zero; "
        "what you write here is what they start from.\n\n"
        "## The task, as the operator described it\n\n"
        f"{quoted}\n\n"
        "## The worker guide — apply it\n\n"
        "Between the two rules below is `docs/worker-best-practices.md`, in full. "
        "It is the standard this worker is held to: the `TASK.md` anatomy you are "
        "about to fill, how to write rules that survive context pressure, the "
        "plan-approve-execute loop, the task state machine, boundaries, execution "
        "hygiene, memory, escalation. Read it before you research; before you "
        "finish, check `TASK.md` against its One-Page Checklist.\n\n"
        "---\n\n"
        f"{body}\n\n"
        "---\n\n"
        f"{_ONBOARDING_STEPS}"
    )


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


# Wrapper tag for the per-session preamble prepended to a conversation's
# first user message (see ``prepend_session_preamble``).
SESSION_CONTEXT_TAG = "session-context"


def build_system_prompt(
    workspace: Path,
    session_id: str = "",
    source: str = "web",
    recalled_memories: list[str] | None = None,
    timezone_name: str = "America/New_York",
    skill_summaries: list[dict] | None = None,
    excluded_tools: "set[str] | None" = None,
    static: bool = True,
) -> str:
    """Build the system prompt for the agent.

    Loads identity files from the workspace, adds the session-context
    block, the tool list and the skills summary.

    ``static=True`` (default) renders a prompt that is byte-identical for
    every session of the same workspace, source and tool set: the
    per-session ``session_id`` and ``recalled_memories`` are accepted for
    signature compatibility but NOT rendered — they are delivered by
    :func:`build_session_preamble` at the top of the conversation's first
    user message instead. Anthropic prompt caching is exact-prefix, so a
    single differing byte (a session id, a per-session recall list) turns
    the whole ~100K-token appended block from a cross-session cache READ
    into a cache WRITE at every session start.

    ``static=False`` restores the legacy shape: session id and recalled
    memories inline in the system prompt.
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

    if static:
        # No per-session bytes here (see the docstring): the id and the
        # recall land in the first user message. The pointer below is
        # static text, so it costs nothing cache-wise.
        context = f"""# Session Context
- **Source:** {source}
- **Current date:** {today}
- **Workspace:** {workspace}

Per-session details — this session's id and the memories recalled at \
session start — are in the `<{SESSION_CONTEXT_TAG}>` block at the top of \
this conversation's first user message. `mcp__nerve__session_context` \
returns them live if that message is no longer in context.

You have access to the following custom tools:
{_format_tool_list(excluded_tools)}"""
    else:
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

    # Recalled memories from memU — legacy (non-static) shape only; the
    # static prompt delivers them via build_session_preamble.
    if not static and recalled_memories:
        parts.append(_format_recalled_memories(recalled_memories))

    return "\n\n---\n\n".join(parts)


def _format_recalled_memories(recalled_memories: list[str]) -> str:
    memories_text = "\n".join(f"- {m}" for m in recalled_memories)
    return f"# Recalled Memories\n\n{memories_text}"


def build_session_preamble(
    session_id: str,
    source: str = "web",
    recalled_memories: list[str] | None = None,
) -> str:
    """Render the per-session context the static system prompt leaves out.

    Returns the markdown block (session id, source, recalled memories)
    that the engine prepends — wrapped by :func:`prepend_session_preamble`
    — to the FIRST user message of a native conversation. Later turns
    replay the transcript, so the block reaches the model exactly once.
    """
    context = (
        "# Session Context\n"
        f"- **Session ID:** {session_id}\n"
        f"- **Source:** {source}"
    )
    parts = [context]
    if recalled_memories:
        parts.append(_format_recalled_memories(recalled_memories))
    return "\n\n".join(parts)


def prepend_session_preamble(text: str, preamble: str) -> str:
    """Prepend ``preamble`` to a user message inside a ``<session-context>``
    wrapper so the model can tell the injected context from the prompt."""
    block = f"<{SESSION_CONTEXT_TAG}>\n{preamble}\n</{SESSION_CONTEXT_TAG}>"
    return f"{block}\n\n{text}" if text else block
