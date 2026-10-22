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
from nerve.memory import loom_prompts

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
followed by an anti-patterns list. Statuses beyond the built-in `backlog`, \
`pending`, `plan_review`, `in_progress`, `done` and `deferred` are created \
once, with `task_status_create`.
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
which in `TASK.md` § Approval Required. A planner prompt sends no `notify` \
about a plan: `plan_propose` moves the task to `plan_review` and notifies the \
operator with a link.

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

### 7. Mark onboarding done, last

Only when every section of `TASK.md` is filled, call `onboarding_done`. It \
deletes the second line of `TASK.md`, the `<!-- nerve:onboarding pending … -->` \
comment. That line is how Nerve knows onboarding is still due: while it stands, \
the web UI shows onboarding as not done; once it is gone, the file is the \
worker's. Do not delete the line yourself, and do not add it back.
"""


def worker_onboarding_prompt(task_description: str, guide: str, memory_backend: str = "memu") -> str:
    """The first-boot onboarding session's prompt for a worker.

    Role line, the operator's description, the worker guide in full (it is
    the standard the filled ``TASK.md`` is held to, so the session gets the
    whole text rather than a link), then the steps. ``guide`` is the text of
    ``docs/worker-best-practices.md`` (:func:`nerve.workspace.read_worker_guide`);
    when it is empty the prompt points at the published copy instead. Under
    ``memory_backend`` ``loom`` the step that saves what was learned names
    Loom's tool.
    """
    quoted = "\n".join(
        f"> {line}" if line.strip() else ">" for line in task_description.strip().splitlines()
    ) or "> (no description was given)"
    body = guide.strip() or (
        "(The guide could not be read from this installation. Fetch it from "
        f"{WORKER_GUIDE_URL} and read it in full before step 2.)"
    )
    steps = _ONBOARDING_STEPS
    if memory_backend == "loom":
        steps = loom_prompts.apply_edits(steps, loom_prompts.ONBOARDING_EDITS, name="onboarding steps")
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
        f"{steps}"
    )


# --- The onboarding conversation's prompt -----------------------------------

# What the agent offers in its first message, in this order. A personal
# install moves the personal-assistant item to the top. Only a conversation
# that starts without a description of the work offers the list.
ONBOARDING_OFFERS = (
    "Pick up an issue, fix the bug, open a PR, and follow it all the way to deployment",
    "Hunt down flaky CI tests and propose fixes",
    "Detect, investigate, and fix performance regressions",
    "Review PRs",
    "Watch alerts and do the first round of digging",
    "Handle security notices and dependency bumps",
    "Read busy Slack channels and post summaries",
    "Competitor and product research",
    "Personal assistant: inbox triage, reminders, planning your week",
)

# The first message without a description: the person names the work.
_FIRST_MESSAGE_OFFERS = """\
## Your first message

Greet the person (by name, if `USER.md` gives it). In two or three sentences, \
say that you are set up by talking to you, now and at any time later. Then \
offer this list, as it is, and ask which items apply; they can choose more \
than one, or describe something else:

{offers}

Change nothing in this first message.
"""

# The first message with a description: no menu, the work starts at once.
_FIRST_MESSAGE_DESCRIBED = """\
## Your first message

The work is described above, so offer no menu of what you can do. In this one \
turn:

1. Greet the person (by name, if `USER.md` gives it).
2. Restate the work as you understand it, in one sentence.
3. Start the explorer (step 1 below) on what the description names to study, \
and say so in one line.
4. Ask your first questions (step 2 below).

When the work needs targets that the description does not name, ask for them \
instead of 3 and 4, as step 1 says.
"""

_ONBOARDING_RULES = """\
## Rules

- Speak to the person directly, in short messages.
- **Change nothing and start no real work before the person approves your \
proposal** (step 3). Until then you and the explorer read, clone, study and \
ask. You do not create cron jobs, skills, statuses, tasks or Nerve plans \
(`plan_propose`), and you do not edit configuration files; the one file you \
write is your plan file, in plan mode.
- Use placeholders for anything you do not know; never invent a repository, \
a channel, a person or a credential. Record where a credential lives, never \
its value.
- If the person wants to stop, say that they can come back to this \
conversation at any time, or skip onboarding on the setup page.
- A question card or a plan decision that times out (no answer within the \
hour) is no answer: say in one line that you wait for the person, end your \
turn, and ask again, or call `ExitPlanMode` again, when they are back.
- `AskUserQuestion`, `EnterPlanMode` and `ExitPlanMode` may be deferred \
tools: load them with `ToolSearch` before their first use. Only when a tool \
this message names is not available even there, do that step without it: \
study the targets yourself, ask in a message, and send the proposal as one \
message with the plan's headings for the person to approve or change.
"""

_FEATURE_RULES = """\
## Nerve features: applied, not advertised

- Name a Nerve mechanism only as the answer to a need of this work, in a few \
words: "the review job gates on tasks tagged `pr-review`, so it stays idle \
when nothing waits".
- After the first message, never list what you can do in general: no menu, \
no feature tour. Pitch nothing that setup already configured (channels, \
sources, quiet hours, memory) and nothing this work does not need.
- Optional extras go only into the plan's last section, "Later, if you \
want": at most three one-liners. Outside it, pitch nothing.
"""

_EXPLORE_STEP = """\
### 1. Explore in the background

As soon as you know what the work needs studied (repositories, documentation \
sites, data sources, alert sources, topics), start **one** explorer: an \
`Agent` tool call with `run_in_background: true` and the brief below, with \
the brackets and `<workspace>` filled in. Then tell the person, in one line, \
what it studies and that its progress shows on the agent line below the chat. \
When the work needs targets nobody has named yet (which repositories, which \
channels), ask for them first, in one plain message, and end your turn.

Do not study the same targets yourself. Nothing is studied twice: when the \
person names more targets later, a second explorer studies only those. Skip \
the explorer when there is nothing to clone or fetch (a personal assistant \
for the inbox, reminders and planning).

```text
Study [the targets] for a Nerve agent that will [the work items]. Change \
nothing outside <workspace>/repos/, and ask the person nothing: open \
questions go under Risks and missing access.

- Clone each repository into <workspace>/repos/<name>: `gh repo clone \
<owner>/<name> <workspace>/repos/<name>` when `gh auth status` succeeds, else \
`git clone` of its public URL. Report what you could not access; do not guess.
- Find how to build and test; the CI system and its entry points; the \
contribution, review and labelling rules (CONTRIBUTING, PR templates, \
CODEOWNERS, bots); the volume of the work (open PRs and issues, new ones per \
week, from `gh` when it works); where things live (docs, scripts, package \
layout); the risks (private dependencies, required credentials, a large \
repository or a monorepo).
- For alerts, channels or research: each source's access mode (API, token, \
read-only), its volume, and what a first result would look like.

Return a Findings report of at most 60 lines under these headings: \
Repositories, Build and test, CI, Rules and conventions, Volume, Risks and \
missing access, Suggested first task. Give each fact with its source path or \
URL; "not found" is a valid finding.
```
"""

_ASK_STEP = """\
### 2. Ask in the foreground, while the explorer works

Ask only what neither the explorer nor setup can tell you, with \
`AskUserQuestion`: one to four questions per call, each with two to four \
concrete options, the recommended default first with "(recommended)" at the \
end of its label. At most two rounds. Typical questions:

- the account you act as on the code host, and the git author to use;
- the approval boundary; recommended: drafts the person approves for anything \
public (review comments, issues, PRs), and on your own for reading, cloning \
and local checks;
- the cadence; recommended: every 30 minutes for PR review, hourly for issue \
triage, daily for research;
- where results go: this chat, a channel, or notifications only;
- for a personal assistant: which of the connected inboxes and calendars to \
read, and how reminders reach the person.

Setup already recorded the mode, the person's name, the time zone and quiet \
hours (`USER.md`, `config/settings.yaml`) and the connected channels and \
sources (the configuration): read them, never ask. Ask nothing in plain text \
in a turn that calls `AskUserQuestion`: until the turn ends, the person can \
answer only the question card.

Do not check on the explorer or poll it: its findings reach you on their own \
when it finishes. When the answers are in before the findings, say so in one \
line and end your turn.
"""

# The plan's headings, in order. ``TASK.md`` is a worker's only.
_PLAN_HEADINGS = (
    "# Setup proposal — <the work, in one line>",
    "## What I understood",
    "## What I found",
    "## TASK.md",
    "## Jobs",
    "## Statuses and tags",
    "## Skills",
    "## What I will remember",
    "## What I need from you",
    "## Approval boundaries",
    "## Later, if you want",
)

_PLAN_STEP = """\
### 3. Propose the setup as a plan

Exploring and asking end before plan mode: first wait for the answers, and \
for the explorer's Findings when one runs. Then call `EnterPlanMode`, write \
the proposal to the plan file path that plan mode names, and call \
`ExitPlanMode`. Plan mode's own reminder suggests Explore and Plan agents and \
more questions: in plan mode start no further explorer and ask nothing more, \
unless a gap in the findings blocks the proposal, and then say so in one \
line. The plan is your proposal, and approving it is the person's one \
decision. Use these headings, in this order:

```markdown
{headings}
```

- **What I understood**: the work items, the targets, the person's answers.
- **What I found**: the findings that shape the setup, with their sources.
{task_md}\
- **Jobs**: a table with id, schedule, gate (`run_if`), session mode and what \
one run does. A scan job that finds new work runs on its schedule without a \
gate; a job that works the queue gates on a `tasks` status and tag, so it \
stays idle when there is no work. Add the built-in jobs you turn on or off: a \
built-in job's state is `config/cron/system.yaml` with a job of the same id \
in `config/cron/jobs.yaml` replacing it, so read both files first. Say \
whether `task-planner` proposes plans that the person approves before an \
implementation session does the work, or a work job has a standing grant for \
routine items.
- **Statuses and tags**: the built-in statuses are `backlog` (not ready; no \
built-in job picks it up), `pending` (ready; the queue), `plan_review` (a plan \
waits for a decision; `plan_propose` moves the task there and notifies the \
person), `in_progress`, `done` and `deferred`. Add a status only for another \
kind of wait, such as a PR in review. One tag for each kind of work, so that \
each job selects its own tasks.
- **Skills**: one for each procedure that runs repeatedly.
- **What I will remember**: what you will `memorize`.
- **What I need from you**: access to grant, accounts to create, decisions \
left open; each becomes a task.
- **Approval boundaries**: what you do on your own, and what waits for the \
person.
- **Later, if you want**: optional; at most three one-liners.

When the person asks for changes, `ExitPlanMode` comes back refused with their \
words. Stay in plan mode, revise the plan file and call `ExitPlanMode` again, \
as often as it takes; never ask what their words already answer.
"""

_PLAN_TASK_MD = "- **TASK.md**: the sections you will fill, in short.\n"

_APPLY_STEPS = """\
### 4. Apply what the person approved

Only after the approval, and only what the plan says. Apply it yourself, in \
this session, without teammates or sub-agents:

- Add jobs to `<workspace>/config/cron/jobs.yaml` (Nerve's scheduler, not the \
system crontab). Put a long prompt in `config/cron/prompts/<job>.md` and load \
it with `prompt_file`. Set `lock: true` and `catchup: false` on a job that \
claims or moves tasks. To change a built-in job, add a job with its id to \
`jobs.yaml`. Example:

```yaml
jobs:
  - id: work-queue
    schedule: '*/15 * * * *'
    description: Work one ready task per run
    session_mode: isolated
    lock: true
    catchup: false
    run_if:
      - type: tasks
        status: pending
        tag: source:ci
    prompt_file: prompts/work-queue.md
```

- Create statuses with `task_status_create`, skills with `skill_create`, and \
tasks with `task_create` for what only the person can do (access to grant, \
accounts to create).
- `memorize` what you learned about the work and what was decided.
- When configuration changes arrive by pull request on this instance, use \
`propose_config_change` instead of editing the files.

### 5. Summarize

End with one message that lists every change: jobs (id, schedule, gate, on or \
off), statuses, tags, skills, tasks with their ids, notes. Then give numbered \
next steps for the person. Say that they can change the setup at any time by \
asking you.

### 6. Mark onboarding done

After the summary, call `onboarding_done`. Until you call it, the web UI \
shows a notice that sends the person back to this conversation.
"""


def _conversation_steps(worker: bool) -> str:
    """The steps after the first message, the same for both of its variants."""
    headings = [h for h in _PLAN_HEADINGS if worker or h != "## TASK.md"]
    plan = _PLAN_STEP.replace("{headings}", "\n".join(headings)).replace(
        "{task_md}", _PLAN_TASK_MD if worker else "",
    )
    return "\n".join((
        "## How the conversation goes\n", _EXPLORE_STEP, _ASK_STEP, plan, _APPLY_STEPS,
    ))


_WORKER_TASK_MD = """\
## TASK.md

This agent is a worker. `<workspace>/TASK.md` (already in your system prompt) \
is the worker template. Fill it when you apply the setup (step 4):

- Keep every `##` heading in its order. Under `## Task description (as \
given)`, keep what the operator wrote at setup; when it is empty, write the \
work the person described in this conversation.
- Replace each `_(onboarding fills this in: …)_` placeholder and every \
`Example:` line; the guide's anatomy table says what belongs in each section.
- Hard rules: one `### ⛔ HARD RULE: <one sentence> [operator directive \
YYYY-MM-DD]` heading per rule, the rule before the reason.
- Actions: the transition table (`| From | Event | To | Who holds the ball |`) \
and a list of anti-patterns.
- Approval Required: what the worker may do on its own; until it says so, \
`SOUL.md`'s default applies (ask before anything external).
- Keep the file within about 4 KB. `onboarding_done` removes the marker \
comment on line 2; do not remove it yourself.

The worker guide follows between the two rules. It is the standard for \
`TASK.md`, the task state machine, cron jobs and boundaries. Read it before \
you propose the setup.
"""


def onboarding_prompt(
    mode: str, task_description: str = "", guide: str = "", *, memory_backend: str = "memu",
) -> str:
    """The hidden first message of the onboarding conversation.

    The person who opens the conversation does not see it; the agent's
    answer is the first message they see. ``mode`` is ``personal`` or
    ``worker``.

    The description of the work (a worker's note from the Mode step) decides
    the first message: without one, the agent offers
    :data:`ONBOARDING_OFFERS` and asks what to do; with one, it restates the
    work, starts a background explorer and asks its first questions in the
    same turn. The steps after it are shared: explore in the background, ask
    with ``AskUserQuestion``, propose the setup as a native plan
    (``EnterPlanMode`` … ``ExitPlanMode``), apply, summarize,
    ``onboarding_done``.

    A worker's prompt also carries its description (or a line that none was
    given), the ``TASK.md`` rules and ``guide``, the worker guide in full
    (:func:`nerve.workspace.read_worker_guide`); when ``guide`` is empty, it
    points at the published copy. Under ``memory_backend`` ``loom`` the step
    that saves what was learned names Loom's tool
    (:data:`nerve.memory.loom_prompts.CONVERSATION_EDITS`).
    """
    worker = mode == "worker"
    description = task_description.strip()
    kind = "a worker for a team" if worker else "a personal assistant"
    parts = [
        "This is the **onboarding conversation** of this Nerve agent, which is "
        f"set up as {kind}. The person who set it up opens this chat. They do "
        "not see this message: your answer is the first message of the "
        "conversation, so you speak first.",
    ]
    if worker or description:
        quoted = "\n".join(
            f"> {line}" if line.strip() else ">" for line in description.splitlines()
        ) or "> (no description was given at setup)"
        who = "operator" if worker else "person"
        parts.append(f"## What the {who} wrote at setup\n\n{quoted}")
    if description:
        parts.append(_FIRST_MESSAGE_DESCRIBED)
    else:
        offers = list(ONBOARDING_OFFERS)
        if not worker:
            offers.insert(0, offers.pop())
        parts.append(_FIRST_MESSAGE_OFFERS.replace(
            "{offers}", "\n".join(f"- {item}" for item in offers),
        ))
    parts += [_ONBOARDING_RULES, _FEATURE_RULES, _conversation_steps(worker)]
    if worker:
        body = guide.strip() or (
            "(The guide could not be read from this installation. Fetch it from "
            f"{WORKER_GUIDE_URL} and read it in full before you propose the setup.)"
        )
        parts += [_WORKER_TASK_MD, f"---\n\n{body}\n\n---"]
    prompt = "\n\n".join(part.strip() for part in parts)
    if memory_backend == "loom":
        prompt = loom_prompts.apply_edits(
            prompt, loom_prompts.CONVERSATION_EDITS, name="onboarding conversation",
        )
    return prompt


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
    memory_note: str | None = None,
    memory_backend: str = "memu",
    memory_tools: bool = True,
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
    memories inline in the system prompt. ``memory_note`` takes the place of
    the memories when none could be recalled (see
    :func:`memory_unavailable_note`), in either shape's place for them.

    ``memory_backend`` ``loom`` adds the memory section that names Loom's
    tools in place of Nerve's (:data:`nerve.memory.loom_prompts.SYSTEM_PROMPT_SECTION`),
    or, when ``memory_tools`` says Loom's tools do not reach the session, a
    short section saying memory cannot be used
    (:data:`nerve.memory.loom_prompts.NO_TOOLS_SECTION`). Either depends on
    the configuration and the gateway's catalog only, so the prompt stays
    static across sessions.
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
        live = (
            " `mcp__nerve__session_context` returns them live if that message "
            "is no longer in context."
            if "session_context" not in (excluded_tools or set()) else ""
        )
        context = f"""# Session Context
- **Source:** {source}
- **Current date:** {today}
- **Workspace:** {workspace}

Per-session details — this session's id and the memories recalled at \
session start — are in the `<{SESSION_CONTEXT_TAG}>` block at the top of \
this conversation's first user message.{live}

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

    if memory_backend == "loom":
        parts.append(
            loom_prompts.SYSTEM_PROMPT_SECTION if memory_tools else loom_prompts.NO_TOOLS_SECTION,
        )

    # Skills summary (progressive disclosure level 1: name + description only)
    skills_section = _format_skills_list(skill_summaries)
    if skills_section:
        parts.append(skills_section)

    # Recalled memories — legacy (non-static) shape only; the static prompt
    # delivers them via build_session_preamble.
    if not static and (recalled_memories or memory_note):
        parts.append(_format_recalled_memories(recalled_memories, memory_note))

    return "\n\n---\n\n".join(parts)


def _format_recalled_memories(
    recalled_memories: list[str] | None, memory_note: str | None = None,
) -> str:
    if memory_note:
        return f"# Recalled Memories\n\n{memory_note}"
    memories_text = "\n".join(f"- {m}" for m in recalled_memories or [])
    return f"# Recalled Memories\n\n{memories_text}"


def memory_unavailable_note(reason: str) -> str:
    """What a session is told when its memories could not be recalled: an
    outage is not an empty memory."""
    reason = reason.removeprefix("memory backend unavailable: ").strip()
    return (
        "Memory is unavailable right now, so none was recalled for this session"
        + (f" ({reason[:300]})" if reason else "")
        + ". Do not take this for an empty memory: memory tools may fail too. "
        "Say so when an answer depends on what you would remember."
    )


def build_session_preamble(
    session_id: str,
    source: str = "web",
    recalled_memories: list[str] | None = None,
    memory_note: str | None = None,
) -> str:
    """Render the per-session context the static system prompt leaves out.

    Returns the markdown block (session id, source, recalled memories, or
    ``memory_note`` in their place) that the engine prepends — wrapped by
    :func:`prepend_session_preamble` — to the FIRST user message of a
    native conversation. Later turns replay the transcript, so the block
    reaches the model exactly once.
    """
    context = (
        "# Session Context\n"
        f"- **Session ID:** {session_id}\n"
        f"- **Source:** {source}"
    )
    parts = [context]
    if recalled_memories or memory_note:
        parts.append(_format_recalled_memories(recalled_memories, memory_note))
    return "\n\n".join(parts)


def prepend_session_preamble(text: str, preamble: str) -> str:
    """Prepend ``preamble`` to a user message inside a ``<session-context>``
    wrapper so the model can tell the injected context from the prompt."""
    block = f"<{SESSION_CONTEXT_TAG}>\n{preamble}\n</{SESSION_CONTEXT_TAG}>"
    return f"{block}\n\n{text}" if text else block
