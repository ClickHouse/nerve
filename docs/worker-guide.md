# Worker Mode Guide

Worker mode deploys Nerve as a task-focused autonomous agent. Give it a job description, and it researches the domain, creates its own skills and cron jobs, then operates in a plan-approve-execute loop — all with human oversight.

## Setup

```bash
# Clone and install
git clone https://github.com/ClickHouse/nerve.git nerve
cd nerve
uv venv && source .venv/bin/activate
uv pip install -e .

# Initialize in worker mode
nerve init --mode worker
```

The wizard walks through:
1. **Deployment** — `server` (bare metal) or `docker`
2. **Task description** — plain English description of what this worker should do
3. **API configuration** — Anthropic API key or CLIProxyAPI proxy
4. **Workspace setup** — creates workspace with worker-specific templates
5. **Cron configuration** — enables `task-planner`, `skill-extractor`, `skill-reviser`, `memory-maintenance`

```bash
nerve start -f    # Start in foreground (first boot triggers onboarding)
```

## How Onboarding Works

On first boot, Nerve detects that `TASK.md` contains a raw description (no `## Mission` section yet) and runs a **setup agent session** with full tool access:

1. **Research** — fetches URLs, searches the web, clones repos, explores CI systems mentioned in the task
2. **Rewrite TASK.md** — restructures the raw description into a structured format:
   - `## Mission` — what the worker does (1–2 sentences)
   - `## Scope` — repos, services, systems to monitor
   - `## Triggers` — events to watch for
   - `## Actions` — step-by-step procedures when triggered
   - `## Approval` — what needs human approval vs autonomous action
   - `## References` — links to docs, APIs, tools
3. **Create skills** — writes domain-specific procedures as reusable skills (`skill_create`)
4. **Configure cron jobs** — adds monitoring and task-specific crons to `~/.nerve/cron/jobs.yaml`
5. **Create initial tasks** — files setup tasks that need manual work (credentials, access tokens, etc.)
6. **Notify** — sends a notification that onboarding is complete with a summary of what was configured

After onboarding, the worker operates autonomously on its configured schedule.

## TASK.md

The task file lives at `workspace/TASK.md` and is injected into every session's system prompt. It's the worker's mission brief.

**Before onboarding** (raw, written by `nerve init`):
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
   - **Decline** → Task stays open, agent won't re-propose unless conditions change
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

Each worker needs its own workspace and config. Deploy multiple workers by cloning Nerve into separate directories:

```bash
# Worker 1: CI monitor
cd ~/nerve-ci-monitor
nerve init --mode worker
nerve start

# Worker 2: PR reviewer
cd ~/nerve-pr-reviewer
nerve init --mode worker
nerve start
```

Each worker has:
- Separate config files (`config.yaml`, `config.local.yaml`)
- Separate databases (`~/.nerve-ci-monitor/`, `~/.nerve-pr-reviewer/` — or use different `--config-dir`)
- Separate workspace (its own skills, tasks, memory)
- Separate cron jobs and schedules
- Separate web UI port (configure `gateway.port` in each)

Workers don't share state. They're fully independent processes.

## Monitoring

### `nerve doctor`

Run `nerve doctor` to verify the worker is healthy:

```bash
nerve doctor
#   [OK] Config loaded
#   [OK] Database schema: v14
#   [OK] System crons: ~/.nerve/cron/system.yaml (3/4 enabled)
#   [OK] User crons: ~/.nerve/cron/jobs.yaml (2 jobs)
#   [OK] Proxy: running on port 8317
```

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

Priority levels: `urgent`, `high`, `normal`, `low`. Configure quiet hours in `config.yaml` to avoid late-night pings.

### Logs

```bash
nerve logs                           # Tail the daemon log
nerve status -f                      # Status + follow logs
```

Cron run history is stored in the `cron_logs` SQLite table and visible in the web UI under Cron Logs. Each run records job ID, timestamps, status (success/error), and output.

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
