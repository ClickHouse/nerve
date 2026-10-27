# Worker Mode Guide

Worker mode deploys Nerve as a task-focused autonomous agent. Give it a job description, and it researches the domain, creates its own skills and cron jobs, then operates in a plan-approve-execute loop — all with human oversight.

> How to run a worker *well* — `TASK.md` anatomy and hard rules, plan-approve-execute discipline, the task state machine, memory tiers and running several workers — is covered in [Worker Best Practices](worker-best-practices.md).

## Setup

```bash
# Clone and install
git clone https://github.com/ClickHouse/nerve.git nerve
cd nerve
uv sync
source .venv/bin/activate

# Decide how Nerve runs here, start it, get the setup page and token
nerve init
```

`nerve init` asks only about this machine (server or Docker, paths, port and
bind address), starts Nerve and prints the setup page with a setup token. The
worker is configured on that page:

1. **Claim** — your username and password (or keep it passwordless), plus the token
2. **Mode**: choose **Worker**, and tick what it should do: review pull
   requests, triage issues, fix issues and open pull requests, keep CI
   healthy, dependency and security updates, first response to alerts (needs
   an alert source; a Slack channel is the first-class one today), channel
   digests and answers, research and reports, docs and release notes, or
   **Custom…** in your own words; optionally add anything it must know or
   never do. All of it is optional: the conversation sorts out the details.
   This writes the worker templates (`SOUL.md`, `AGENTS.md`, `TOOLS.md`,
   `MEMORY.md`), `TASK.md` from its template with your choice in place (the
   onboarding conversation fills the rest), and `config/cron/system.yaml` with
   `skill-extractor` and `skill-reviser` on and `task-planner` off
   (`memory-maintenance` is always on; the onboarding conversation turns
   `task-planner` on when the worker should plan through the approval loop)
3. **Model** — an Anthropic API key, a Claude subscription token, the Claude
   Code login found on the server, or AWS Bedrock; test it, pick a model
4. **Operator** — who runs this worker, in which time zone it should plan its
   day, and quiet hours if it should keep any (off for a worker by default);
   **Memory**, **Channels** and **Automation** are optional and can be
   skipped. Sources and external agents are not offered to a worker
5. **Review and finish** — the server check-up, then **Restart Nerve** (the
   mode's memory settings are read at startup) or **Finish**. The ending reads
   "Your worker is ready" with **Start the conversation**, which opens the
   onboarding conversation

The same choice from a terminal is `nerve init --terminal`, which asks for the
mode and an optional task description itself and does not start the daemon
(`nerve start` afterwards). Its worker path has no cron step, so it writes
`skill-extractor` and `skill-reviser` with `enabled: false` too (`task-planner` is
off on every path until onboarding enables it): set `enabled: true` in
`config/cron/system.yaml` (or override the job by id in `jobs.yaml`) and `nerve reload`. Unattended installs set `NERVE_MODE=worker` and
`NERVE_TASK="…"` for `nerve init --non-interactive` (see
[Unattended installation](setup.md#unattended-installation)). On these two
paths, a worker with a task description sets itself up without a person at its
first start (see [Without the browser](#without-the-browser)); without one, it
waits for its onboarding conversation in the web UI.

## How Onboarding Works

After setup in the browser, you set the worker up by talking to it, in the onboarding conversation (see [Setup → The onboarding conversation](setup.md#the-onboarding-conversation)). The setup page's Finish opens it, or the next start of the daemon when a restart comes first. When setup is complete and nobody pressed Finish, the banner in the web UI offers to start it. Until onboarding is done, `TASK.md` keeps the onboarding marker (the `<!-- nerve:onboarding pending … -->` comment on its second line). The agent's hidden instruction holds what you chose on the Mode step (each ticked item with what it does, the connection it needs and the follow-up questions it raises), the full [Worker Best Practices](worker-best-practices.md) guide and the rules below. The agent speaks first: with work chosen it offers no menu, restates the work in one sentence, says what it studies and sketches the setup it has in mind before it asks anything; with nothing ticked, or only rules, it shows one form that asks which work applies, with four common kinds of work as checkboxes and **Other…** for work in your own words. A free-text note (from an older setup page or `nerve init --terminal`) that describes the work counts as chosen work; one with only rules or only repositories gets the form. Then:

1. **Explore and ask**: one background explorer clones the repositories into `<workspace>/repos/` and reads them, without installing, building or running tests (build and test, CI, contribution and review rules, the volume of the work, risks and missing access); once its findings are in, the agent asks you, in question cards with its recommendation first, only what neither the explorer nor setup can tell (a first card asks only for missing repositories or channels, so the explorer can start). It works from the follow-up questions of each item you ticked (which repositories, what a review may do, the label vocabulary, and so on) and asks once what the items share: the account it acts as on the code host, what waits for your approval, where results go. What does not fit into two rounds becomes a default in the proposal; your rules ("Anything it must know or never do?") become hard rules and approval boundaries
2. **Propose the setup as a plan**: cron jobs with schedule and `run_if` gate, whether `task-planner` plans through the approval loop (it ships disabled for workers; the agent turns it on by overriding the job by id in `jobs.yaml`, with a `tasks` gate scoped to the worker's tag), task statuses (the built-in `backlog` and `plan_review`, plus a status for each other kind of wait), tags, skills, the `TASK.md` sections, what it needs from you (including a connection a chosen item needs that the instance lacks) and the approval boundaries. **Apply setup** approves it, **Edit** lets you change the text and approve your version, and **Change something…** sends your words back and the agent revises the plan. It is instructed to change nothing before you approve
3. **Apply what you approved**: fills `TASK.md` in place (every `_(onboarding fills this in: …)_` placeholder and example line, keeping the headings and what you chose, the `### ⛔ HARD RULE: … [operator directive YYYY-MM-DD]` shape for the non-negotiables, the Actions transition table, and the closing "Read the full guide" link), adds the jobs to `<workspace>/config/cron/jobs.yaml`, creates statuses, skills and the tasks only you can do, and saves what it learned to memory
4. **Summarize**: lists every change and the next steps for you
5. **Mark onboarding done**: calls `onboarding_done`, which also removes the marker line from `TASK.md`

Until onboarding is done or skipped, a banner in the web UI sends you back to the conversation. A `TASK.md` from an older install (the raw `# Task` form, no marker) is rewritten into the template's shape with its text under "Task description (as given)" when the conversation opens; such a file counts as onboarded once it has a `## Mission` heading. A `TASK.md` without its marker counts as onboarded.

### Without the browser

A worker set up by `nerve init --terminal` or `nerve init --non-interactive` with a task description sets itself up without a person, once, at the first start of the daemon with a model credential (see [Setup → A worker set up without the browser](setup.md#a-worker-set-up-without-the-browser)). One session gets your description, the full worker guide and these steps:

1. **Research**: fetches what the description names, clones the repositories, and looks at the CI systems and APIs involved
2. **Fill `TASK.md` in place**, to the same rules as above
3. **Create skills** for the procedures the worker repeats
4. **Configure cron jobs** in `jobs.yaml`, with gates, and decide on `task-planner`
5. **Create initial tasks** for what only you can do (credentials, access, open decisions)
6. **Walk through every change**: saves what it learned to memory, then notifies you (priority `high`) with every change and numbered next steps
7. **Mark onboarding done**: calls `onboarding_done`, which removes the marker line from `TASK.md`

While the session runs, the banner in the web UI links to it. With an empty task description, the worker waits for its onboarding conversation in the web UI instead.

After onboarding, the worker operates on its configured schedule. You can change the setup at any time by asking it in a chat.

## TASK.md

The task file lives at `workspace/TASK.md` and is injected into every session's system prompt. It is the worker's mission brief. What each section holds, and how to write rules that hold up, is in [Worker Best Practices → TASK.md Anatomy](worker-best-practices.md#taskmd-anatomy).

**Before onboarding.** The Mode step, `nerve init --terminal` and `nerve init --non-interactive` write the worker template with what you chose or described, if anything, in place. The Mode step writes its choice as one comment line Nerve reads (the ticked items' ids) and one line each for people, each left out when empty: `Work:` (the ticked items), `Also:` (your own words) and `Must know or never do:`; the terminal paths write your task description as a plain paragraph. The comment on the second line marks the file as not onboarded yet. The example lines the template carries under each heading are left out here:

```markdown
# Task
<!-- nerve:onboarding pending: the onboarding conversation fills this file in; this line goes when onboarding is done. -->

## Task description (as given)
<!-- nerve:work ci-health -->
Work: Keep CI healthy
Also: Hunt down flaky tests: when a test fails intermittently, investigate the root cause and propose a fix.
Must know or never do: Never touch PRs or issues you did not open unless addressed.

## Identity
_(onboarding fills this in: which account acts, exact git author, fork/remote names)_

## Mission
_(onboarding fills this in: one paragraph ending in the quality bar)_

## Capabilities
_(onboarding fills this in: what this worker is expected to be able to do, and what it may never dismiss; at most 10 lines)_

## Scope
_(onboarding fills this in: repos, CI, data sources, and the one exhaustive reference doc or skill; pointers, no credentials)_

## Hard rules
_(onboarding fills this in: the non-negotiables, each its own heading with a marker and provenance; as few as you can defend)_

## Triggers
_(onboarding fills this in: what creates work, one line each: scan crons, inbox and mentions, human directives)_

## Actions
_(onboarding fills this in: the status state machine as a transition table, then an anti-patterns list)_

| From | Event | To | Who holds the ball |
|---|---|---|---|

## Approval Required
_(onboarding fills this in: what needs a human, what is autonomous, where decisions are recorded)_

## References
_(onboarding fills this in: endpoints with auth mode (never passwords), schema columns, canonical filters, label vocabulary; the long form lives in `TOOLS.md`)_

---
Read the full guide: https://github.com/ClickHouse/nerve/blob/main/docs/worker-best-practices.md
```

**After onboarding.** The onboarding conversation has filled every section in place and kept the headings and the description, and `onboarding_done` has deleted the marker line:

```markdown
# Task

## Task description (as given)
<!-- nerve:work ci-health -->
Work: Keep CI healthy
Also: Hunt down flaky tests: when a test fails intermittently, investigate the root cause and propose a fix.
Must know or never do: Never touch PRs or issues you did not open unless addressed.

## Identity
You are `ci-bot`. Git author `CI Bot <ci-bot@example.com>`; fork `ci-bot/repo-a`
(remote `fork`); upstream `org/repo-a` (remote `origin`). Every public action
(commit, push, comment) uses this account and no other.

## Mission
Watch CI on `org/repo-a`, find flaky tests and regressions, fix them at the
source, and tell maintainers what they need to know. Fewer, better PRs beat
many quick ones.

## Capabilities
You are a developer on `repo-a` with the full source, a warm debug build
(`skill_get build-and-test`) and read access to the failure database. Do not
dismiss a failure as too deep, "infra noise" or someone else's code. If a tool
is missing, name it, then install it or flag the gap.

## Scope
- Repository: `org/repo-a` on GitHub · CI: GitHub Actions · tests: pytest
- Failure DB: endpoint and auth mode in `TOOLS.md` § Failure DB (read-only user)
- CI internals: `docs/ci-internals.md` in the repo, or the `ci-db-query` skill

## Hard rules
### ⛔ HARD RULE: builds and tests run in the FOREGROUND as ONE call [operator directive 2026-03-02]
One synchronous Bash call with a `timeout` above the expected duration that
captures the producer's exit code: `make test > build.log 2>&1; rc=$?;
tail -50 build.log; echo RC=$rc`. Never `$?` after a pipe; never
background-and-exit.
### ⛔ HARD RULE: never touch PRs or issues you did not open unless addressed [operator directive 2026-03-02]
Being right does not make it welcome. Track external fixes; do not comment.

## Triggers
- `ci-scan` cron (every 30 min): new main-branch failures → tasks tagged `source:ci-main`
- `pr-ci` cron (`github_pr_activity` gate): CI finished on an own PR → CI results comment
- Human directives (chat, task notes tagged `human-directive`) → outrank everything

## Actions
| From | Event | To | Who holds the ball |
|---|---|---|---|
| `pending` | a run claims it | `in_progress` | this worker |
| `in_progress` | PR pushed, CI running | `waiting_ci` | CI |
| `waiting_ci` | CI green, review requested | `on_review` | reviewer |
| `in_progress` | someone else's fix PR exists | `monitoring_external` | the other author |
| any waiting | human comment or directive | `pending` + `needs-response` | this worker |
| `on_review` | merged | `verifying` → `done` | post-merge window |

Anti-patterns: pushing a cleanly waiting task back to `pending` (burns a run);
a waiting status with no PR and no posted question (a phantom block).

## Approval Required
- Opening a public issue · anything irreversible or external → human approval
- Classifying a test as flaky · the CI results comment on an own PR · notifying
  → autonomous, recorded in the task note
- Self-raised work → only with a passing `## Impact` section

## References
- Failure DB columns: `job, test, status, log_url`; failures `status IN ('failed','errored')`
- Labels: `bugfix`, `flaky-test`
- Toolchain pin lives in `ci/toolchain.lock`: read it, never quote it

---
Read the full guide: https://github.com/ClickHouse/nerve/blob/main/docs/worker-best-practices.md
```

Statuses in the Actions table other than the built-in `pending`, `in_progress`, `done` and `deferred` are created once with `task_status_create`. You can edit `TASK.md` at any time. Changes take effect on the next session.

## The Plan-Approve Loop

Workers operate through a structured cycle:

```
Cron detects issue → Agent proposes plan → Notification sent
→ Human reviews (approve / decline / revise) → Implementation session runs
```

**In detail:**
1. **Detection** — A cron job (the built-in `task-planner`, when onboarding enabled it, or a custom monitoring cron) identifies something actionable
2. **Task creation** — The agent creates a task describing the issue (`task_create`)
3. **Plan proposal** — The agent researches the issue, explores the codebase, and writes a plan (`plan_propose`)
4. **Notification**: `plan_propose` moves the task to `plan_review` and sends one notification with links to the plan and the task
5. **Review** — You review the plan in the web UI:
   - **Approve** → An implementation session spawns automatically with the plan as instructions
   - **Decline** → The related task is closed (moved to `done`) with your feedback recorded as the note; reopen or recreate the task if you want a fresh plan
   - **Revise** → Your feedback is sent back to the planner session (context preserved), which proposes a revised plan
6. **Implementation** — The approved plan executes in a dedicated session with full tool access
7. **Verification** — Implementation session runs checks, tests, reports results

A revision goes back to the session that proposed the plan — for the planner's plans, the same persistent planner session — so the planning context is preserved across revision rounds.

## Skills

Workers create their own skills during onboarding and refine them over time.

**How skills work:**
- Skills are plain Markdown files in `workspace/skills/<name>/SKILL.md`
- The agent reads skill descriptions in every session's system prompt
- Full skill content is loaded on demand via `skill_get`
- Skills can include reference docs (`references/`), scripts (`scripts/`), and assets (`assets/`)

**Automated skill lifecycle:**
- `skill-extractor` (every 12h) — watches for repeated workflows in conversations and completed tasks, proposes new skills via task+plan
- `skill-reviser` (weekly) — reviews existing skills for accuracy, completeness, and quality, proposes revisions

When a skill-related plan is approved, an `impl-<id>` session is spawned like for any plan; its prompt tells it to apply the plan (a full SKILL.md) with `skill_create` or `skill_update` and close the task with `task_done`.

## Running Multiple Workers

Each worker needs its own config directory (`-c`), state directory
(`NERVE_HOME`), workspace and port. `nerve init` asks for the workspace and the
port; the other two come from the command line and the environment:

```bash
# Worker 1: CI monitor
NERVE_HOME=~/.nerve-ci-monitor nerve -c ~/nerve-ci-monitor init --port 8901
# → open the setup page it prints, choose Worker, tick what it should do

# Worker 2: PR reviewer
NERVE_HOME=~/.nerve-pr-reviewer nerve -c ~/nerve-pr-reviewer init --port 8902
```

Keep `NERVE_HOME` set (and pass `-c`) for every later `nerve` command that
addresses that worker. Each worker has:
- Separate config files (`config.yaml`, `config.local.yaml`)
- Separate databases and logs (its `NERVE_HOME`)
- Separate workspace (its own skills, tasks, memory)
- Separate cron jobs and schedules
- Separate web UI port (`gateway.port`, chosen at `nerve init`)

Workers don't share state. They're fully independent processes.

## Monitoring

### `nerve doctor`

Run `nerve doctor` to verify the worker is healthy:

```bash
nerve doctor
#   [OK] Config loaded
#   [OK] Database schema: v14
#   [OK] Cron jobs: 3/4 enabled, 1 overridden in jobs.yaml
#   [OK] Proxy: running on port 8317
#   [OK] Setup checklist complete
```

While setup is unfinished the last line reads `[WARN] Setup checklist
incomplete (steps: provider, profile) — finish at /setup`; `nerve status`
prints the same count with the page's address. Neither prints the setup token
after the claim.

### Web UI

The web UI provides:
- **Cron logs** — see every job run with status, duration, and output
- **Task manager** — track open tasks and their plans
- **Plan review** — approve, decline, or revise proposed plans
- **Notification center** — all alerts and questions from the worker
- **Session history** — browse all agent sessions (onboarding, planning, implementation)

### Notifications

Workers communicate through the notification system:
- **`notify`** — status updates, completion alerts, issues found
- **`ask_user`** — questions that need decisions (rendered as buttons in web UI and Telegram)

Priority levels: `urgent`, `high`, `normal`, `low`. Quiet hours are optional and off for a worker by default (`quiet_start` and `quiet_end` empty in `<workspace>/config/settings.yaml`; see [Configuration → Quiet Hours](config.md#quiet-hours)). When set, they record the human's quiet window but gate nothing the agent sends; `notify` is not held back by them, so have the worker apply quiet hours itself for anything below `urgent` (see [Personal Best Practices → Notifications](personal-best-practices.md#notifications)).

### Logs

```bash
nerve logs                           # Tail the daemon log
nerve status -f                      # Status + follow logs
```

Cron run history is stored in the `cron_logs` SQLite table and visible in the web UI under Cron Logs. Each run records job ID, timestamps, status (success/error/missed), and output.

## Worker vs Personal — Key Differences

| | Personal | Worker |
|---|---|---|
| **Purpose** | Full-featured assistant for one human | Task-focused autonomous agent |
| **Workspace files** | SOUL.md, IDENTITY.md, USER.md, MEMORY.md, AGENTS.md, TOOLS.md | SOUL.md, TASK.md, MEMORY.md, AGENTS.md, TOOLS.md |
| **Memory categories** | Life-oriented (relationships, finances, health, travel) | Operational (patterns, procedures, decisions, approvals) |
| **Default crons** | inbox-processor, task-planner, memory-maintenance | skill-extractor, skill-reviser, memory-maintenance (task-planner off until onboarding enables it) |
| **Quiet hours** | 02:00–08:00 | none |
| **Sync sources** | Telegram, Gmail, GitHub | None by default (can add custom sources) |
| **Channels** | Web UI + Telegram bot | Web UI (+ Telegram if configured) |
| **Onboarding** | A conversation with the agent | A conversation with the agent; set up without the browser with a task description, the worker sets itself up |
| **Primary workflow** | Conversational (user asks, agent responds) | Plan-driven (detect → plan → approve → execute) |
