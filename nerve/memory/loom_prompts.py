"""What agents are told when memory is Loom (``memory.backend: loom``).

Under Loom, agents use Loom's own tools: the MCP gateway's catalog lists Loom
as the server ``loom``, so they are ``mcp__loom__<tool>``, and Nerve's nine
memory tools are not served. Every place that names those tools has its Loom
wording here, kept to the tools Loom's default (``core``) tool list carries:
the system prompt's memory section, the worker onboarding step that saves
what it learned, the Codex backend note, the workspace templates, the
built-in cron prompts and what external agents are told. The memU wording
stays where it was. Two texts memU sessions also see did change: the worker
guide (``docs/worker-best-practices.md``, which the onboarding prompt
carries in full) has a bullet on Loom, and the bundled ``nerve-dev`` skill's
source map names the Loom backend.
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

# The memory section of the system prompt (nerve.agent.prompts). Static per
# instance, so it costs nothing in prompt caching.
SYSTEM_PROMPT_SECTION = """\
# Memory: Loom

Long-term memory on this instance is Loom. Its tools come from the `loom` MCP \
server, so you call them as `mcp__loom__<tool>`. Nerve's own memory tools \
(`memorize`, `memory_recall`, `memory_update`, `memory_delete`, \
`conversation_history`, `memory_records_by_date`, `memory_expand_category`, \
`category_update`, `session_context`) are not available here: where the \
workspace files name them, use these.

- **Recall**: `loom_search` answers one question from memory, profile, \
knowledge and past conversations together. Ask it before code, PRs, external \
services or decisions that depend on history, in full questions, one call per \
question. `loom_memory_get` reads a key you know; `loom_memory_list`, \
`loom_memory_history` and `loom_memory_related` list keys, show a key's \
versions and find its neighbours; `loom_context_pack` loads what a task needs \
in one call.
- **Save**: `loom_memory_set` stores one fact per call under a readable \
`memory_key` (`<scope>/<kind>/<id>`), the claim in plain text, facets in \
`tags`. A fact you read that has changed gets `loom_memory_update` under the \
same key, never a second copy. `loom_memory_pin` is for a standing rule, \
`loom_memory_delete` only for an explicit request to forget, and \
`loom_blacklist_add` for something that must never be remembered.
- **Scratch**: `loom_working_memory_set`, `loom_working_memory_append`, \
`loom_working_memory_replace` and `loom_working_memory_delete` hold state for \
the task at hand; it is not searched.
- **Anything else**: `loom_op` with `{"op": "help", "query": "<words>"}` lists \
the operations the tool list leaves out.

**Levels.** Your writes land in your home namespace, which is yours. Team and \
org namespaces are shared with other agents and people: `loom_search` reads \
them together with your home, a hit that names a `namespace` comes from one of \
them, and `loom_namespace_list_mine` lists the namespaces you reach with your \
role in each. Shared memories are read-only for you: never update, delete or \
overwrite one. To change one, propose instead: where your role there is \
`writer`, `loom_memory_set` your version in that namespace with \
`proposed: true` under a key of its own (a fact on trial, served once its \
owners accept it); otherwise tell the human what should change and why. Keep \
what you need meanwhile in your home namespace.

**What Loom answers.** Every write reports a `conflict_verdict`: `clear` \
(saved), `merge` (already stored; nothing written), `duplicate`, `conflict`, \
`refines` (it replaced an older version of the fact) or `rejected`. On \
`conflict`, `peers` and `next` name the memory it collided with: read it, then \
update whichever is stale under its own key, or leave both and tell the human; \
do not save the fact again. `rejected` (`PINNED_CONFLICT`) means a pinned fact \
stands in the way: ask the human. A search that abstains (`abstain: true`, \
with an `abstain_code`) found nothing it can stand behind: say you do not know \
rather than guessing from weak hits. An empty result is no proof of absence \
either: a conversation Loom is still extracting is not in it yet.

Nerve recalls this session's starting memories from Loom and sends each \
finished conversation to Loom, which extracts its facts under that \
conversation's session id: find them with `loom_search`, not by key. Save \
decisions, corrections and conclusions as they happen, not the whole \
exchange."""

# The memory section when Loom is the backend but its tools do not reach the
# session: no gateway catalog (local mode), or no `loom` server in it yet.
NO_TOOLS_SECTION = """\
# Memory: Loom

Long-term memory on this instance is Loom, but Loom's tools are not \
available to this session and Nerve's memory tools are not served, so you \
can neither search nor save memories now. Do not take that for an empty \
memory, and say so when an answer depends on what you would remember."""

# The worker onboarding prompt's steps (nerve.agent.prompts._ONBOARDING_STEPS):
# the memU sentence, then its Loom wording.
ONBOARDING_EDITS: tuple[tuple[str, str], ...] = (
    (
        (
            "First `memorize` what you learned about the domain (`knowledge`) and what you "
            "decided (`event`); the next session starts from zero."
        ),
        (
            "First save to Loom what you learned about the domain and what you decided, "
            "one fact per `loom_memory_set` call under a readable key; the next session "
            "starts from zero."
        ),
    ),
    ("- what you memorized.", "- what you saved to Loom."),
)

# The onboarding conversation's prompt (nerve.agent.prompts.onboarding_prompt):
# the memU sentence, then its Loom wording.
CONVERSATION_EDITS: tuple[tuple[str, str], ...] = (
    (
        "- `memorize` what you learned about the work and what was decided.",
        (
            "- Save to Loom what you learned about the work and what was decided, "
            "one fact per `loom_memory_set` call under a readable key."
        ),
    ),
)

# The Codex backend's developer-instructions note: the line naming Nerve's
# memory tools among Nerve's tools, and its Loom wording.
CODEX_NOTE_EDIT = (
    (
        "- Nerve's tools (memorize, memory_recall, task_*, notify, ask_user, skills,\n"
        "  schedule_wakeup, ...) are provided by the `nerve` MCP server — call them as\n"
        "  `mcp__nerve__<name>` / however your harness names MCP tools."
    ),
    (
        "- Nerve's tools (task_*, notify, ask_user, skills, schedule_wakeup, ...) are\n"
        "  provided by the `nerve` MCP server — call them as `mcp__nerve__<name>` /\n"
        "  however your harness names MCP tools. Memory is Loom's: its tools\n"
        "  (`loom_search`, `loom_memory_set`, ...) come from the `loom` MCP server."
    ),
)

# The same line for a session that gets no `loom` server: no memory tools.
CODEX_NOTE_NO_TOOLS = (
    "- Nerve's tools (task_*, notify, ask_user, skills, schedule_wakeup, ...) are\n"
    "  provided by the `nerve` MCP server — call them as `mcp__nerve__<name>` /\n"
    "  however your harness names MCP tools."
)

# What external agents (Claude Code, Codex on this machine) are told in place
# of the call to Nerve's session_context, which is not served under Loom.
EXTERNAL_AGENT_MEMORY = """\
# Memory — Loom

This Nerve instance keeps its memory in Loom, and Nerve's MCP server serves no
memory tools (no `session_context`, `memorize` or `memory_recall`). If this
client has Loom's MCP server configured, start a fresh session with a
`loom_search` question about what you are about to do, and save decisions with
`loom_memory_set`; otherwise say that you have no recalled context."""

EXTERNAL_AGENT_FOOTER_MEMORY = (
    "- **Memory:** kept in Loom; use Loom's MCP tools if this client has them "
    "(`loom_search` to recall, `loom_memory_set` to save).\n"
)

# --------------------------------------------------------------------------- #
# Workspace templates
# --------------------------------------------------------------------------- #

_TOOLS_MEMORY = (
    (
        "**Memory**\n"
        "- `memory_recall`: semantic search; returns items and `cat:<id>` topic breadcrumbs, which `memory_expand_category` opens\n"
        "- `conversation_history`: event items by the date they happened\n"
        "- `memory_records_by_date`: every record created or updated on a date (maintenance, audit)\n"
        "- `session_context`: startup context (metadata, recall priors, skills); the first call for external MCP clients\n"
        "- `memorize`: save a `profile`/`event`/`knowledge`/`behavior` fact; \"NOT saved\" means retry\n"
        "- `memory_update`/`memory_delete`: correct or remove an item by id; `category_update` rewrites a category summary\n"
    ),
    (
        "**Memory** (Loom: the `loom` MCP server, so call these as `mcp__loom__<name>`)\n"
        "- `loom_search`: one question over memory, profile, knowledge and past conversations; a hit from a shared namespace names it; `abstain: true` means Loom does not know\n"
        "- `loom_memory_get`/`loom_memory_list`/`loom_memory_history`/`loom_memory_related`: one key, the keys, a key's versions, its neighbours\n"
        "- `loom_memory_set`: save one fact under a readable `memory_key` in your home namespace; read the `conflict_verdict` it answers\n"
        "- `loom_memory_update`: a changed fact, under the same key; `loom_memory_pin` a standing rule; `loom_memory_delete` only when asked to forget\n"
        "- `loom_context_pack`: what a task needs, in one call; `loom_working_memory_*`: scratch state, not searched\n"
        "- `loom_namespace_list_mine`: the namespaces you reach and your role in each; `loom_op`: what the tool list leaves out (`{\"op\": \"help\"}`)\n"
    ),
)

_TOOLS_CALL = (
    "Call tools as `mcp__nerve__<name>`. The system prompt lists what is live; this is the grouped reference.",
    "Call Nerve's tools as `mcp__nerve__<name>` and Loom's as `mcp__loom__<name>`. The system prompt lists what is live; this is the grouped reference.",
)

_AGENTS_CALL = (
    "Tools are listed in `TOOLS.md`; call them as `mcp__nerve__<name>`.",
    "Tools are listed in `TOOLS.md`; call Nerve's as `mcp__nerve__<name>` and Loom's as `mcp__loom__<name>`.",
)

_SHARED_READ_ONLY = (
    "Team and org namespaces are read-only for you: propose a change instead of "
    "editing (the system prompt says how)."
)

# Per template (its path under nerve/templates): the memU wording, then its
# Loom wording. Each memU text occurs once in its template
# (tests/test_loom_prompts.py says so).
TEMPLATE_EDITS: dict[str, tuple[tuple[str, str], ...]] = {
    "personal/AGENTS.md": (
        _AGENTS_CALL,
        (
            "1. `conversation_history` for the last 3 days (limit=100).",
            "1. `loom_search` what you and the human worked on in the last 3 days.",
        ),
        (
            "run several `memory_recall` queries phrased as questions (\"What has alice decided about X?\").",
            "ask `loom_search` questions, one call each (\"What has alice decided about X?\").",
        ),
        (
            "5. `memorize` decisions, preferences and status changes when they appear.",
            "5. `loom_memory_set` decisions, preferences and status changes when they appear, one fact per call.",
        ),
        (
            "- Long-term: semantic memory — `memorize` writes, `memory_recall` reads.",
            "- Long-term: Loom — `loom_memory_set` writes to your home namespace, `loom_search` reads it "
            "with the team and org namespaces shared with you. " + _SHARED_READ_ONLY,
        ),
        (
            "Turn-ending calls (`memorize`, `ScheduleWakeup`, `notify`)",
            "Turn-ending calls (`loom_memory_set`, `ScheduleWakeup`, `notify`)",
        ),
    ),
    "worker/AGENTS.md": (
        _AGENTS_CALL,
        (
            "1. `conversation_history` for the last few days, then several `memory_recall` queries phrased as full questions (\"What do we know about the flaky test `test_x`?\").",
            "1. Several `loom_search` questions, phrased in full and one call each (\"What do we know about the flaky test `test_x`?\", \"What happened on this task in the last few days?\").",
        ),
        (
            "- Long-term: semantic memory. Before ending every session, `memorize` root causes, repro steps, what worked and what did not, decisions and why. Nothing new: one event line of what you checked.",
            "- Long-term: Loom, your home namespace. Before ending every session, `loom_memory_set` root causes, repro steps, what worked and what did not, decisions and why, one fact per call. Nothing new: one line of what you checked. "
            + _SHARED_READ_ONLY,
        ),
        (
            "- `memorize` failed twice:",
            "- `loom_memory_set` failed twice:",
        ),
        (
            "the status transition, `memorize`, `notify`.",
            "the status transition, `loom_memory_set`, `notify`.",
        ),
        (
            "editing the existing entry; `memorize` as backup.",
            "editing the existing entry; `loom_memory_set` as backup.",
        ),
    ),
    "personal/TOOLS.md": (_TOOLS_CALL, _TOOLS_MEMORY),
    "worker/TOOLS.md": (_TOOLS_CALL, _TOOLS_MEMORY),
    "personal/MEMORY.md": (
        (
            "Stable history goes to `memorize`; delete resolved items.",
            "Stable history goes to Loom (`loom_memory_set`); delete resolved items.",
        ),
        (
            "Untouched for about two weeks: `memorize` it, then delete it.",
            "Untouched for about two weeks: `loom_memory_set` it, then delete it.",
        ),
        (
            "7. Retire a workstream: `memorize` the essentials,",
            "7. Retire a workstream: `loom_memory_set` the essentials,",
        ),
    ),
    "worker/MEMORY.md": (
        (
            "stable history to `memorize`; delete resolved items.",
            "stable history to Loom (`loom_memory_set`); delete resolved items.",
        ),
    ),
    "skills/nerve-dev/SKILL.md": (
        (
            "2. **Recall** — `memory_recall` for past issues, conventions and preferences on the area",
            "2. **Recall** — `loom_search` for past issues, conventions and preferences on the area",
        ),
        (
            "4. **ALWAYS `memory_recall` before modifying Nerve code.**",
            "4. **ALWAYS `loom_search` before modifying Nerve code.**",
        ),
    ),
}


def apply_edits(text: str, edits: tuple[tuple[str, str], ...], *, name: str) -> str:
    """``text`` with each memU wording replaced by its Loom wording.

    A wording that no longer occurs exactly once (the template changed) is
    left as it is and logged: a workspace with one memU phrase left is
    better than none, and the system prompt's Loom section still applies.
    """
    for old, new in edits:
        count = text.count(old)
        if count != 1:
            logger.warning(
                "Loom wording for %s not applied: the text it replaces occurs %d times", name, count,
            )
            continue
        text = text.replace(old, new)
    return text


def template_text(relative: str, text: str) -> str:
    """The Loom wording of the template at ``relative`` (under
    nerve/templates), or ``text`` unchanged when it names no memory tool."""
    edits = TEMPLATE_EDITS.get(relative)
    return apply_edits(text, edits, name=relative) if edits else text

# --------------------------------------------------------------------------- #
# Built-in cron jobs
# --------------------------------------------------------------------------- #

MAINTENANCE_DESCRIPTION = "Daily memory check — Loom's integrity report on the home namespace"

MAINTENANCE_PROMPT = (
    "You are running a daily memory check on Loom. Work completely silently — do not output any text, only think.\n\n"
    "1. Call loom_namespace_validate for your home namespace (read-only). It reports errors and warnings, each with sample memory keys and a `detail` naming the operation that fixes it.\n"
    "2. For a finding on a key in your home namespace: fix it as its `detail` says when that is one of your Loom tools — a stale or wrong value with loom_memory_update under the same key, never a second copy.\n"
    "3. Leave conflicts to their owners (Loom's review queue) and everything in team or org namespaces alone: shared memories are read-only for you.\n"
    "4. Do not delete memories as cleanup: Loom merges duplicates itself, and a wrong fact is corrected, not deleted.\n"
    "5. Save nothing about this run. Notify only when a finding needs the human (an error that keeps coming back, a conflict you cannot judge), once, with the keys involved.\n"
)

# The inbox processor's line about saving facts: memU's, then Loom's.
INBOX_MEMORIZE_EDIT = (
    "- **Memorize** important facts (via memorize) worth remembering\n",
    "- **Save** important facts worth remembering to Loom (via loom_memory_set, one fact per call)\n",
)


def tool_names() -> set[str]:
    """Every Loom tool the wording here names; ``loom_working_memory_*``
    stands for the tools with that prefix. ``tests/test_loom_contract.py``
    checks them against the tools Loom lists by default."""
    texts = [
        SYSTEM_PROMPT_SECTION, EXTERNAL_AGENT_MEMORY, EXTERNAL_AGENT_FOOTER_MEMORY,
        MAINTENANCE_PROMPT, CODEX_NOTE_EDIT[1], INBOX_MEMORIZE_EDIT[1],
        *(new for _, new in ONBOARDING_EDITS),
        *(new for _, new in CONVERSATION_EDITS),
        *(new for edits in TEMPLATE_EDITS.values() for _, new in edits),
    ]
    names: set[str] = set()
    for text in texts:
        for name in re.findall(r"\bloom_[a-z_]+\*?", text):
            names.add(name if not name.endswith("_") else name + "*")
    return names
