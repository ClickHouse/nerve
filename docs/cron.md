# Cron

## Overview

Nerve uses APScheduler for in-process async job scheduling. Jobs can run in isolated sessions (fresh each time) or persistent sessions (context preserved across runs) and deliver output to configured channels.

## Two-File Layout

Cron jobs live in two YAML files under `~/.nerve/cron/`:

| File | Purpose | Managed by |
|------|---------|------------|
| `system.yaml` | Built-in crons (core + productivity) | `nerve init` — safe to regenerate |
| `jobs.yaml` | Your custom crons | You — Nerve never touches this file |

Both files use the same format. On startup, CronService loads and merges both:
- If a job ID appears in both files, the **user version wins** (with a warning in the log).
- Old installs with everything in `jobs.yaml` still work — if `system.yaml` doesn't exist, all jobs load from `jobs.yaml`.

Running `nerve init` on an existing install regenerates `system.yaml` (e.g., to pick up updated prompts from a Nerve update) without touching `jobs.yaml`.

## Job Definition

```yaml
# ~/.nerve/cron/jobs.yaml (or system.yaml — same format)
jobs:
  - id: morning-briefing
    schedule: "30 11 * * *"        # 11:30 AM daily
    prompt: "Give me a morning briefing..."
    description: "Daily morning summary"
    model: claude-sonnet-4-6       # Optional model override
    target: telegram               # Delivery channel
    session_mode: isolated         # "isolated", "persistent", or "main"
    enabled: true

  - id: system-monitor
    schedule: "30m"                  # Every 30 minutes
    prompt: "Check system health and report changes since your last check."
    session_mode: persistent         # Keeps context across runs
    context_rotate_hours: 48         # Fresh context every 48h
    enabled: true

  - id: task-reminder
    schedule: "0 */2 * * *"        # Every 2 hours
    prompt: "Check for overdue tasks..."
    target: telegram
    enabled: true
```

## Job Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | yes | Unique job identifier |
| `schedule` | string | yes | Crontab expression or interval (`2h`, `30m`) |
| `prompt` | string | yes | Message sent to the agent |
| `description` | string | no | Human-readable description |
| `model` | string | no | Override model (default: `agent.cron_model`) |
| `target` | string | no | Delivery channel (default: `telegram`) |
| `session_mode` | string | no | `isolated` (new session per run), `persistent` (reuse context), or `main` |
| `context_rotate_hours` | int | no | Hours before persistent context resets (default: 24, 0 = never) |
| `reminder_mode` | bool | no | Persistent only: send short reminder instead of full prompt on subsequent runs (default: false) |
| `catchup` | bool | no | Fire once on startup if the job missed a run while the server was down (default: true) |
| `enabled` | bool | no | Whether the job is active (default: true) |

## Session Modes

### Isolated (default)

Each run creates a fresh session (`cron:{job_id}:{timestamp}`). The agent has no in-context memory of previous runs. This is best for self-contained jobs like daily briefings or cleanup tasks.

### Persistent

Jobs with `session_mode: persistent` maintain SDK conversation context across runs:

- **First trigger**: Creates a fresh session (`cron:{job_id}`) and runs the prompt.
- **Subsequent triggers**: Resumes the same SDK session and sends the prompt as a new message. The agent sees all prior runs in-context.
- **Context rotation**: Every `context_rotate_hours` (default: 24), the context is reset. Old messages remain in the database and are searchable via memU, but the agent starts with a clean slate.

This is useful for jobs that benefit from accumulated context:
- Monitoring jobs that track changes over time
- Summary jobs that should remember what was already reported
- Multi-step workflows that build on previous results

Between runs, the SDK client subprocess is freed (no resource leak). On the next trigger, the SDK resumes the session from its stored state.

#### Reminder Mode

Persistent jobs with `reminder_mode: true` avoid resending the full prompt on every trigger. Instead:

- **First run** (or after context rotation): The full prompt is sent, with a note explaining that subsequent runs will use a short reminder.
- **Subsequent runs**: A short message ("Scheduled run — continue with the same task as before.") is sent instead of the full prompt. The agent already has the original instructions in-context from the first run.

This significantly reduces token usage for frequently-triggered persistent jobs (e.g., every 15 minutes).

### Main

Jobs with `session_mode: main` run in the main user session instead of an isolated one.

## CLI Usage

```bash
# List available jobs (shows source and status)
nerve cron
#   [system] memory-maintenance: Daily memory cleanup (enabled)
#   [system] inbox-processor: Polls sources every 30 min (enabled)
#   [user  ] my-custom-monitor: Checks CI status (enabled)

# Run a specific job manually
nerve cron morning-briefing

# Check cron status
nerve doctor
#   [OK] System crons: ~/.nerve/cron/system.yaml (3/5 enabled)
#   [OK] User crons: ~/.nerve/cron/jobs.yaml (1 jobs)
```

## Built-in System Crons

These ship in `~/.nerve/cron/system.yaml` and are managed by `nerve init`. Running `nerve init` regenerates this file (e.g., to pick up updated prompts from a Nerve update) without touching your custom `jobs.yaml`.

| Job | Schedule | Session Mode | Description | Personal | Worker |
|-----|----------|-------------|-------------|:--------:|:------:|
| `memory-maintenance` | Daily 5 AM | isolated | Dedup, prune stale entries, improve memory wording. Runs silently. | ✅ always | ✅ always |
| `inbox-processor` | Every 30 min | persistent (24h rotation, reminder mode) | Polls all sync sources (email, GitHub, Telegram). Triages, creates tasks, memorizes facts, sends notifications for urgent items. | ✅ default | — |
| `task-planner` | Every 4 hours | persistent (168h rotation) | Reviews open tasks, explores codebases, proposes implementation plans via plan-approve workflow. | ✅ default | ✅ default |
| `skill-extractor` | Every 12 hours | persistent | Identifies repeated workflows from recent conversations, memory, and completed tasks. Proposes new skills via task+plan system. | ✅ optional | ✅ default |
| `skill-reviser` | Weekly (Sun 3 AM) | persistent | Reviews existing skills for accuracy (outdated paths, credentials), completeness (missing steps), and quality (trigger phrases, examples). Proposes revisions via task+plan. | ✅ optional | ✅ default |

**Mode defaults:**
- **Personal** — `memory-maintenance` (always on) + `inbox-processor` + `task-planner` enabled by default. `skill-extractor` and `skill-reviser` are presented as optional during `nerve init`.
- **Worker** — `memory-maintenance` (always on) + `task-planner` + `skill-extractor` + `skill-reviser` enabled by default. `inbox-processor` is not included (workers don't have sync sources).

Both skill jobs use `source="skill-extractor"` or `source="skill-reviser"` on created tasks. When their plans are approved, the plan approval handler creates/updates the skill directly from the plan content (which is a full SKILL.md file) instead of spawning an implementation session.

## Persistent Timers

Cron schedules survive server restarts. On startup, the cron service queries `cron_logs` for each job's last successful run and uses that to restore correct timing.

### Interval alignment

For interval schedules (e.g. `4h`), the trigger is anchored to the last run time. If a job last ran 2.5 hours ago and the interval is 4 hours, the next fire is in 1.5 hours — not 4 hours from now.

### Startup catch-up

If a job should have fired while the server was down, it fires **once** on startup — regardless of how many runs were missed. This applies to both interval and crontab schedules.

- **First-ever run**: No catch-up (no history to compare against).
- **Multiple missed fires**: Coalesced into a single catch-up run.
- **Catch-up runs concurrently**: All overdue jobs fire in parallel, in the background (doesn't block startup).

### Opting out

Set `catchup: false` on jobs where a late run doesn't make sense:

```yaml
  - id: morning-briefing
    schedule: "0 12 * * *"
    catchup: false        # no point running a morning briefing at 3pm
```

Interval alignment still applies even with `catchup: false` — only the startup catch-up fire is skipped.

## Source Runners

In addition to YAML-defined cron jobs, the cron service auto-registers **source runners** from the `sync:` config. Each enabled source becomes an APScheduler job with ID `source:<name>` (e.g., `source:gmail`, `source:github`).

Source runners:
- Run on the schedule defined in their config (`sync.<source>.schedule`)
- Use `SourceRunner` to fetch → process → advance cursor
- Are logged in both `cron_logs` and `source_run_log` tables
- Appear in `list_jobs()` alongside regular cron jobs

See [sources.md](sources.md) for full documentation.

## Logging

Every cron and source run is logged in the `cron_logs` SQLite table:
- `job_id` — Which job ran (e.g., `morning-briefing` or `source:gmail`)
- `started_at` / `finished_at` — Timestamps
- `status` — `success` or `error`
- `output` — First 2000 chars of response / summary
- `error` — Error message if failed

Source runs also log to `source_run_log` with per-source diagnostics (records fetched/processed, errors).

View logs via API: `GET /api/cron/logs?job_id=morning-briefing&limit=10`
