# Cron

## Overview

Nerve uses APScheduler for in-process async job scheduling. Jobs can run in isolated sessions (fresh each time) or persistent sessions (context preserved across runs) and deliver output to configured channels.

## Two-File Layout

Cron jobs live in two YAML files under `<workspace>/config/cron/` (the
git-syncable workspace subtree):

| File | Purpose | Managed by |
|------|---------|------------|
| `system.yaml` | Built-in crons (core + productivity) | `nerve init` — safe to regenerate |
| `jobs.yaml` | Your custom crons | You — Nerve never touches this file |

> **Location & migration.** New installs write cron config to
> `<workspace>/config/cron/`. Installs that still have the legacy
> `~/.nerve/cron/` keep working: if the workspace location doesn't exist yet,
> Nerve reads from `~/.nerve/cron/` automatically. You can also pin the paths
> explicitly with `cron.jobs_file` / `cron.system_file` / `cron.gate_plugins_dir`
> — except under [lockdown](config.md#lockdown-remote-only-read-only), where all
> three must resolve inside `<workspace>/config/`. An escaping path is ignored in
> favour of the in-workspace default; if the default is what escapes (a symlinked
> `config/cron` or `config/cron/gates`) the instance refuses to start.

Both files use the same format. On startup, CronService loads and merges both:
- If a job ID appears in both files, the **user version wins** (with a warning in the log).
- Old installs with everything in `jobs.yaml` still work — if `system.yaml` doesn't exist, all jobs load from `jobs.yaml`.

Running `nerve init` on an existing install regenerates `system.yaml` (e.g., to pick up updated prompts from a Nerve update) without touching `jobs.yaml`.

### Hot-reload (no restart)

`POST /api/cron/reload` re-reads `jobs.yaml` and `system.yaml` and applies them
to the running daemon, so a job you add, remove, reschedule, enable or disable
takes effect **without a restart**. The same call re-reads the gate-plugins
directory, so an added, edited or deleted plugin file lands with it. A
`prompt_file` needs no reload: its contents are read on every run.

A reload rebuilds every enabled job from the file, and leaves source runners and
internal cleanup/wakeup jobs alone.

Notes:
- Rebuilding a job normally leaves its next fire time alone: a crontab is
  absolute, and an interval is measured from the job's last successful run. An
  interval job that has never succeeded has no such anchor, so each reload
  restarts its countdown. On a fresh install, where nothing has run yet, editing
  the file repeatedly keeps every interval job's wait starting over — they
  settle once each has run for the first time.
- A **malformed** `jobs.yaml`/`system.yaml` (bad YAML, or a job that doesn't
  parse) is refused: reload returns `400` and the running schedule is left
  untouched, so a typo can't wipe your crons.
- A reload is **all-or-nothing**. The whole change set is computed (files parsed,
  every trigger built) before the scheduler is touched, so a reload that fails
  for any reason leaves every job on its existing schedule instead of applying
  half the change. Gate plugins are covered too: a refused reload leaves the
  registered gates as they were, so a deleted plugin file only takes its gate
  away once a reload succeeds.
- A job holding a [reserved id](#job-fields) is **skipped**, at reload and at
  startup alike. The reload itself still succeeds; only that job is dropped, and
  its id comes back in the reload's `rejected` list, so a job that never runs is
  visible from the API and not only in the log.
- An **invalid schedule** (a crontab whose fields the scheduler rejects, e.g.
  `99 * * * *`) is refused the same way at reload: `400`, nothing applied.
  Startup is the one case that differs — the daemon logs an error naming the job
  and comes up without it, rather than taking every other cron down over one
  typo. `GET /api/cron/jobs` still lists the job, with a null `next_run`.
- `show_session_label` is **restart-only**: changing it and reloading has no
  effect until the daemon restarts.

## Job Definition

```yaml
# <workspace>/config/cron/jobs.yaml (or system.yaml — same format)
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

  - id: repo-watch-nerve
    schedule: "1h"
    prompt_file: prompts/repo-watch.md   # Relative to this YAML's directory
    enabled: true

  - id: repo-watch-other
    schedule: "1h"
    prompt_file: prompts/repo-watch.md   # Same prompt, shared between jobs
    enabled: true
```

### Prompt Files

Instead of an inline `prompt`, a job can point at a file with `prompt_file`.
This keeps long prompts out of the YAML and lets multiple jobs share one
prompt definition.

- Relative paths resolve against the directory of the YAML file the job was
  loaded from (e.g. `prompts/repo-watch.md` next to `jobs.yaml` →
  `<workspace>/config/cron/prompts/repo-watch.md`). Absolute paths and `~` work too.
- The file is read fresh on **every run** — edits take effect on the next
  trigger without a restart.
- If both `prompt` and `prompt_file` are set, the file wins; the inline
  prompt is used as a fallback when the file can't be read.
- A job must define at least one of `prompt` / `prompt_file` / `workflow`.

## Job Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | yes | Unique job identifier. `cleanup`, `wakeup_sweep` and anything starting with `source:` are **reserved by the daemon** — a job using one is skipped (with a warning naming it in the daemon log) and never scheduled, so rename it |
| `schedule` | string | yes | Crontab expression or interval (`2h`, `30m`, `1h30m`, `0.5h`) — see [Interval syntax](#interval-syntax). A 5-field crontab with an out-of-range field (`99 * * * *`) is **rejected**, never reinterpreted as an interval — reload returns `400` and startup skips that one job with an error in the daemon log. Anything that names no usable interval (`hourly`, `@daily`, `1h junk`, or a zero like `0h`) silently becomes a 2-hour interval at run time; `nerve config validate` fails on both, which is the only warning you get about the second |
| `prompt` | string | yes* | Message sent to the agent |
| `prompt_file` | string | yes* | Path to a file containing the prompt (relative to the YAML's directory). Read fresh each run; shareable between jobs. *One of `prompt`/`prompt_file` is required |
| `description` | string | no | Human-readable description |
| `model` | string | no | Override model (default: `agent.cron_model`) |
| `cache_ttl` | string | no | Prompt-cache TTL override for this job's sessions: `5m`, `1h`, or `auto` (default: `agent.cache_ttl`). Sparse-schedule persistent jobs benefit from `1h` — see `nerve/agent/cache_policy.py` |
| `target` | string | no | Delivery channel (default: `telegram`) |
| `session_mode` | string | no | `isolated` (new session per run), `persistent` (reuse context), or `main` |
| `context_rotate_hours` | int | no | Hours before a persistent job rotates to a fresh chat (default: 24, 0 = never). The old chat is preserved |
| `context_rotate_at` | string | no | Time of day to rotate (e.g. `"04:00"`, in the configured timezone). Overrides the hours-based rotation |
| `reminder_mode` | bool | no | Persistent only: send short reminder instead of full prompt on subsequent runs (default: false) |
| `catchup` | bool | no | Fire once on startup if the job missed a run while the server was down (default: true) |
| `enabled` | bool | no | Whether the job is active (default: true) |
| `lock` | bool | no | Prevent concurrent runs of this job — the next fire waits for the previous one (default: false) |
| `run_if` | list | no | Run gates — preconditions that must all hold for the job to fire. See [Run Gates](#run-gates) |
| `workflow` | map | yes* | Launch a budget-capped [workflow run](workflow-runs.md) instead of a prompt: `{engine, prompt, budget_usd[, title, model, effort, cwd]}`. Takes precedence over `prompt`/`prompt_file`; the cron job only launches the run (fire-and-forget) — the run notifies on its own |

### Interval syntax

A `schedule` that isn't a 5-field crontab is read as an interval: a run of
`<number><unit>` tokens, where the unit is `h`, `m` or `s`. Tokens add up, and
fractions are allowed.

| Schedule | Interval |
|----------|----------|
| `4h` | 4 hours |
| `30m` | 30 minutes |
| `90s` | 90 seconds |
| `1h30m` | 90 minutes (`1h 30m` works too) |
| `0.5h` | 30 minutes |
| `10.5m` | 10 minutes 30 seconds |

A fractional interval is rounded to the nearest whole second (`1.333m` → 80s).

Anything that isn't a whole string of such tokens is **not an interval** and
silently falls back to **every 2 hours** — `hourly`, `@daily`, `4x`, `1h junk`,
and a zero interval (`0h`, or a fraction that rounds down to zero seconds), which
would otherwise mean "fire as fast as you can". The daemon keeps running on a
conservative cadence rather than refusing to start over one mistyped field, so a
job firing every 2 hours when you asked for something else means the schedule
string didn't parse.

## Run Gates

A **run gate** is a precondition evaluated right before a job fires. It answers
one question: *should this cron run right now?* Gates let a job stay idle until
there is actually something to do — no agent session is spawned (and nothing is
logged beyond a skip line) when a gate is unsatisfied.

Declare gates with the `run_if` key — a list of gate specs. **All gates must be
satisfied (logical AND)** for the job to run:

```yaml
jobs:
  - id: task-planner
    schedule: "0 */4 * * *"
    prompt: "Review open tasks and propose plans..."
    run_if:
      - type: tasks            # only when there's something to plan
        status: pending
```

When multiple gates are listed, the job runs only if every one passes:

```yaml
  - id: triage
    schedule: "30m"
    prompt: "Triage incoming work..."
    run_if:
      - type: tasks            # there is an open task AND
        status: pending
      - type: messages         # a source has unread mail
        sources: [gmail, github]
```

Gates are **fail-open while checking**: if a gate raises during evaluation (e.g.
a transient DB issue), the run proceeds rather than being skipped — an occasional
wasted run beats a cron that silently never fires.

A gate that can't be *built* is a different matter and is **refused**. If a
`run_if` entry names a type nothing provides — a typo, or a gate plugin that was
deleted or fails to import — the job does not load: reload returns `400` naming
it, and at startup the daemon logs an error and comes up without that job. The
alternative would be to drop the gate and run the job anyway, which turns "only
when this holds" into "every time" — a precondition removed by accident is not a
precondition.

### Gate types

#### `tasks`

Satisfied when enough tasks match a status/tag filter. The canonical use is
"only run the planner when there is something to plan."

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `status` | string \| list \| `"all"` | omitted = any **open** (non-done) task | Status name(s) to count. A list counts across all of them; `"all"` counts every task regardless of status |
| `tag` | string | — | Optional tag filter |
| `min_count` | int | 1 | Minimum number of matching tasks required to run |

```yaml
run_if:
  - type: tasks
    status: [pending, in_progress]   # any of these statuses
    tag: backend                     # ...tagged "backend"
    min_count: 3                     # ...and at least 3 of them
```

#### `messages`

Satisfied when the sync sources feeding a consumer have unread messages
(compares each source's max ingested rowid against the consumer cursor; never
advances it).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sources` | list | *(any source)* | Source names to check (e.g. `gmail`, `github`). **Omit** to fire when *any* source the consumer already tracks has unread messages — handy for a shared/default inbox job that shouldn't hard-code the connected sources. |
| `consumer` | string | `inbox` | Consumer cursor name used for the unread check |

```yaml
run_if:
  - type: messages
    sources: [gmail, github]   # omit `sources` entirely for "any source"
    consumer: inbox
```

> **Legacy shorthand.** The older `skip_when_idle: [<sources>]` /
> `idle_consumer: <name>` fields still work — they are translated into an
> equivalent `messages` gate at load time. Prefer `run_if` for new jobs.

#### `github_pr_activity`

Satisfied when something actionable changed on an author's open PRs: a merge or
close (`state`), a new commit (`headRefOid`), a review verdict
(`reviewDecision`), a CI transition (`statusCheckRollup`), or a human
comment/review. Those fields are fingerprinted via `gh` and the job fires only
when the fingerprint moves, so an expensive PR-monitor job can stay idle for the
whole life of an open PR while nothing actually happens on it.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `author` | string | — (required) | GitHub login whose open PRs to watch. Also ignored as a comment/review actor, so the job's own replies don't wake it |
| `force_run_after_hours` | number | 8 | Fire regardless if this long has elapsed since the last run; `0` disables the safety net |
| `ignore_actors` | list | — | Logins whose comments/reviews don't count as activity, for chatty bots that would otherwise wake the job on every push. Matching is case-insensitive and a trailing `[bot]` is ignored, so `codecov` also covers `codecov[bot]` |

```yaml
run_if:
  - type: tasks                  # cheap DB check first...
    status: in_progress
    tag: pr-open
  - type: github_pr_activity     # ...then the network check
    author: my-bot
    force_run_after_hours: 8
    ignore_actors: [codecov, sonarqubecloud]
```

Two behaviours worth knowing. Any `gh` failure fails **open** — the job fires —
so a transient error can never strand a PR. And `ignore_actors` is a denylist
rather than bot autodetection, because bots are not reliably identifiable here:
`author.is_bot` is `null` inside `gh`'s comment payloads and GraphQL strips the
`[bot]` login suffix, while `authorAssociation` reports real maintainers as
`CONTRIBUTOR` — the same value as some review bots. A denylist fails in the safe
direction: an unlisted bot costs one extra wake, whereas a wrong autodetect
would silently drop human feedback.

### Adding a built-in gate type

Built-in gates live in `nerve/cron/gates.py`. To add one: subclass `CronGate`,
set its `type`, implement `is_satisfied`, `describe`, and `from_config`, then
register the class in `GATE_REGISTRY`. It becomes usable from `run_if`
immediately. This is the right path for gates that ship with Nerve.

### Custom gate plugins (drop-in)

To add your **own** gate without editing core source, drop a `.py` file into
the gate-plugins directory — `<workspace>/config/cron/gates/` by default (overridable via
the `cron.gate_plugins_dir` config key). On daemon startup Nerve imports each
file and registers every `CronGate` subclass it defines with a non-empty
`type`. After that, `run_if` can reference your gate by `type` exactly like a
built-in. Because this never touches `nerve/cron/gates.py`, your custom gates
don't conflict when you pull Nerve upstream.

```python
# <workspace>/config/cron/gates/stale_tasks.py
from nerve.cron.gates import CronGate, GateContext


class StaleTasksGate(CronGate):
    type = "stale_tasks"

    def __init__(self, min_age_minutes: int = 30):
        self.min_age_minutes = min_age_minutes

    async def is_satisfied(self, ctx: GateContext) -> bool:
        # ctx exposes {job_id, db} — DB-only (see note below).
        ...

    def describe(self) -> str:
        return f"stale tasks older than {self.min_age_minutes}m"

    @classmethod
    def from_config(cls, spec: dict) -> "StaleTasksGate":
        return cls(min_age_minutes=int(spec.get("min_age_minutes", 30)))
```

```yaml
# <workspace>/config/cron/jobs.yaml — reference it like any built-in gate
run_if:
  - type: stale_tasks
    min_age_minutes: 60
```

A gate must implement the same three methods as a built-in (`is_satisfied`,
`describe`, `from_config`).

**Rules** (a bad plugin never crashes the daemon — though it does cost the jobs
that name its gate, see below):

- Files whose name starts with `_` (and `__pycache__`) are ignored.
- A plugin whose `type` collides with an already-registered gate is skipped
  with a warning: a **built-in always wins**, and among two plugins the **first
  loaded (filename-sorted) wins**.
- Any import error in a plugin file is logged (naming the file) and that file
  is skipped; the rest still load.
- **Hot-reloadable:** `POST /api/cron/reload` re-reads the directory from
  scratch, so an added, edited, deleted or renamed plugin takes effect without a
  daemon restart. Built-in gates are never dropped.
- A plugin that stops registering its gate — deleted, renamed, or newly failing
  to import — takes the jobs that name that gate with it: the reload is refused
  with a `400`, and the running schedule is left as it was. Restore the file (or
  remove the `run_if` entry) and reload again. Deleting a plugin is not a way to
  switch a gate off; it stops those jobs instead of ungating them.

> **Context is DB-only.** A gate receives `GateContext{job_id, db}`, which is
> enough for DB-driven conditions (task counts, source cursors, age filters). A
> gate that needs live runtime state — e.g. which sessions are currently
> running — is **not** supported by this loader; that would require widening
> the gate context, a separate change.

> **Trust note.** Files in the gate-plugins directory are imported (executed)
> at daemon startup. This is the same trust model as `config.yaml`, configured
> MCP servers, and cron prompt files — all user-controlled code/config the
> daemon already loads. Only place files you trust in this directory.

## Session Modes

### Isolated (default)

Each run creates a fresh session (`cron:{job_id}:{timestamp}`). The agent has no in-context memory of previous runs. This is best for self-contained jobs like daily briefings or cleanup tasks.

### Persistent

Jobs with `session_mode: persistent` maintain SDK conversation context across runs. Each job owns a **generation chat** — one session that is reused run after run until rotation:

- **First trigger**: Creates a fresh generation session (`cron:{job_id}:{timestamp}`) and runs the prompt.
- **Subsequent triggers**: Resumes the same SDK session and sends the prompt as a new message. The agent sees all prior runs of this generation in-context.
- **Context rotation**: Every `context_rotate_hours` (default: 24) — or daily at `context_rotate_at` — the current chat is **retired and a brand-new chat is started**. The old chat is preserved exactly as it was: it keeps its full message history, stays browsable (and resumable) in the UI like any other session, and ages out via the normal session archival policy. Retiring also indexes the old context into memU, cancels the old session's pending wakeups (so a retired thread can't resurrect itself alongside the new one), and retitles it with its end date (`Cron: {job_id} (until 2026-07-05)`).

> **Upgrade note.** Installs that predate generation chats used the stable id
> `cron:{job_id}`. That session is adopted as the current generation on the
> first run after upgrading — its SDK context carries over seamlessly — and
> moves to the generation scheme on its next rotation.

This is useful for jobs that benefit from accumulated context:
- Monitoring jobs that track changes over time
- Summary jobs that should remember what was already reported
- Multi-step workflows that build on previous results

Between runs, the SDK client subprocess is freed (no resource leak). On the next trigger, the SDK resumes the session from its stored state.

Rotation can also be forced from the Cron page ("Rotate") or via
`POST /api/cron/jobs/{job_id}/rotate` — same behavior: the current chat is
preserved and the next run starts in a fresh one.

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
#   [OK] Cron jobs: 3/5 enabled, 1 overridden in jobs.yaml
#
# system.yaml and jobs.yaml are merged first (a user job overrides a system
# job with the same id), so the counts are for the merged set. If either file
# can't be parsed, doctor falls back to just listing the paths it found.
```

## Built-in System Crons

These ship in `<workspace>/config/cron/system.yaml` and are managed by `nerve init`. Running `nerve init` regenerates this file (e.g., to pick up updated prompts from a Nerve update) without touching your custom `jobs.yaml`.

| Job | Schedule | Session Mode | Description | Personal | Worker |
|-----|----------|-------------|-------------|:--------:|:------:|
| `memory-maintenance` | Daily 5 AM | isolated | Dedup, prune stale entries, improve memory wording. Runs silently. | ✅ always | ✅ always |
| `inbox-processor` | Every 30 min | persistent (24h rotation, reminder mode) | Polls all sync sources (email, GitHub, Telegram). Triages, creates tasks, memorizes facts, sends notifications for urgent items. | ✅ default | — |
| `task-planner` | Every 4 hours | persistent (168h rotation) | Reviews open tasks, explores codebases, proposes implementation plans via plan-approve workflow. Gated on `tasks` (status `pending`) — stays idle when there's nothing to plan. | ✅ default | ✅ default |
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
