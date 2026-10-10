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
2. **Mode** — choose **Worker** and describe the task in plain English (three
   starter templates: monitor a build, review pull requests, watch logs). This
   writes the worker templates (`SOUL.md`, `AGENTS.md`, `TOOLS.md`,
   `MEMORY.md`), the raw `TASK.md`, and `config/cron/system.yaml` with
   `task-planner`, `skill-extractor` and `skill-reviser` on
   (`memory-maintenance` is always on)
3. **Model** — an Anthropic API key, a Claude subscription token, the Claude
   Code login found on the server, or AWS Bedrock; test it, pick a model
4. **Operator** — who runs this worker and in which time zone it should plan
   its day; **Memory**, **Channels** and **Automation** are optional and can be
   skipped. Sources and external agents are not offered to a worker
5. **Review and finish** — the server check-up, then **Restart Nerve** (the
   mode's memory settings are read at startup) or **Finish**. The ending reads
   "Your worker is ready" with **Onboarding session starting** and a link to
   that session

The same choice from a terminal is `nerve init --terminal`, which asks for the
mode and the task description itself and does not start the daemon (`nerve
start` afterwards). Its worker path has no cron step, so it writes `task-planner`,
`skill-extractor` and `skill-reviser` with `enabled: false`: set `enabled: true` in
`config/cron/system.yaml` (or override the job by id in `jobs.yaml`) and `nerve reload`. Unattended installs set `NERVE_MODE=worker` and
`NERVE_TASK="…"` for `nerve init --non-interactive` — see
[Unattended installation](setup.md#unattended-installation).

## How Onboarding Works

When Nerve starts — or when the setup page's Finish runs on an instance that needs no restart — and `TASK.md` still contains the raw description (no `## Mission` section yet), it runs a **setup agent session** with full tool access. The setup page shows it as "Onboarding session starting" and then links to the running session:

1. **Research** — fetches URLs, searches the web, clones repos, explores CI systems mentioned in the task
2. **Rewrite TASK.md** — restructures the raw description into a structured format:
   - `## Mission` — what the worker does (1–2 sentences)
   - `## Scope` — repos, services, systems to monitor
   - `## Triggers` — events to watch for
   - `## Actions` — step-by-step procedures when triggered
   - `## Approval` — what needs human approval vs autonomous action
   - `## References` — links to docs, APIs, tools
3. **Create skills** — writes domain-specific procedures as reusable skills (`skill_create`)
4. **Configure cron jobs** — adds monitoring and task-specific crons to `<workspace>/config/cron/jobs.yaml`
5. **Create initial tasks** — files setup tasks that need manual work (credentials, access tokens, etc.)
6. **Notify** — sends a notification that onboarding is complete with a summary of what was configured

After onboarding, the worker operates autonomously on its configured schedule.

## TASK.md

The task file lives at `workspace/TASK.md` and is injected into every session's system prompt. It's the worker's mission brief.

**Before onboarding** (raw, written by the Mode step or `nerve init --terminal`):
```markdown
# Task

Monitor the CI pipeline for flaky tests. When a test fails intermittently,
investigate the root cause and propose a fix.
```

**After onboarding** (structured by the setup agent):
```markdown
# Task

## Mission
Monitor CI pipelines for flaky test failures and propose targeted fixes.

## Scope
- Repository: github.com/org/repo
- CI system: GitHub Actions
- Test framework: pytest

## Triggers
- CI run fails with a test that passed on the previous commit
- Same test fails >2 times in the last 7 days with different commits

## Actions
1. Identify the flaky test from CI logs
2. Check git blame and recent changes to the test and tested code
3. Reproduce locally if possible
4. Propose a fix via plan-approve workflow

## Approval
- Bug fixes: propose plan, wait for approval
- Test-only changes: propose plan, wait for approval
- Infra changes: always escalate via notification

## References
- CI dashboard: https://github.com/org/repo/actions
- Test conventions: docs/testing.md
```

You can edit `TASK.md` at any time. Changes take effect on the next session.

## The Plan-Approve Loop

Workers operate through a structured cycle:

```
Cron detects issue → Agent proposes plan → Notification sent
→ Human reviews (approve / decline / revise) → Implementation session runs
```

**In detail:**
1. **Detection** — A cron job (built-in `task-planner` or custom monitoring cron) identifies something actionable
2. **Task creation** — The agent creates a task describing the issue (`task_create`)
3. **Plan proposal** — The agent researches the issue, explores the codebase, and writes a plan (`plan_propose`)
4. **Notification** — A notification is sent so you know there's a plan to review
5. **Review** — You review the plan in the web UI:
   - **Approve** → An implementation session spawns automatically with the plan as instructions
   - **Decline** → The related task is closed (moved to `done`) with your feedback recorded as the note; reopen or recreate the task if you want a fresh plan
   - **Revise** → Your feedback is sent back to the planner session (context preserved), which proposes a revised plan
6. **Implementation** — The approved plan executes in a dedicated session with full tool access
7. **Verification** — Implementation session runs checks, tests, reports results

Plans and their revisions happen in the same persistent planner session — full conversation context is preserved across revision rounds.

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

When a skill-related plan is approved, the plan approval handler creates/updates the skill directly from the plan content (no implementation session needed — the plan IS the skill).

## Running Multiple Workers

Each worker needs its own config directory (`-c`), state directory
(`NERVE_HOME`), workspace and port. `nerve init` asks for the workspace and the
port; the other two come from the command line and the environment:

```bash
# Worker 1: CI monitor
NERVE_HOME=~/.nerve-ci-monitor nerve -c ~/nerve-ci-monitor init --port 8901
# → open the setup page it prints, choose Worker, describe the task

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

Priority levels: `urgent`, `high`, `normal`, `low`. `quiet_start`/`quiet_end` in `<workspace>/config/settings.yaml` suppress only task-deadline escalation reminders; `notify` is not gated by them, so have the worker apply quiet hours itself for anything below `urgent` (see [Personal Best Practices → Notifications](personal-best-practices.md#notifications)).

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
| **Default crons** | inbox-processor, task-planner, memory-maintenance | task-planner, skill-extractor, skill-reviser, memory-maintenance |
| **Sync sources** | Telegram, Gmail, GitHub | None by default (can add custom sources) |
| **Channels** | Web UI + Telegram bot | Web UI (+ Telegram if configured) |
| **Onboarding** | Interactive — user configures identity and preferences | Autonomous — agent researches task and self-configures |
| **Primary workflow** | Conversational (user asks, agent responds) | Plan-driven (detect → plan → approve → execute) |
