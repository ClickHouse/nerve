# Workflow Runs

Budget-capped, tracked, killable multi-agent jobs. A workflow run wraps one
dedicated agent session — Claude driving the harness's built-in `Workflow`
tool, or Codex driving Ultracode — in a **hard dollar budget** enforced from
Nerve's own usage metering, with run-scoped termination and a durable journal
directory.

Use a run whenever an agent should burn real money unattended: large fan-out
research, dataset audits, adversarial review loops. The run is the unit you
budget, watch, and kill.

## Anatomy of a Run

Starting a run creates:

- A `workflow_runs` row (id `wfr-<8 hex>`), status `pending`.
- A journal directory `<workflows.runs_dir>/<run-id>/` (mode `0700`).
- When a concurrency slot frees, a **dedicated session** `workflow:<run-id>`
  with source `"workflow"` on the backend the engine kind selects. The run's
  whole life is typically one long agentic turn in that session.

The spec `prompt` is sent wrapped in run framing: the budget and metering
cadence, the orchestration hint (Workflow tool / Ultracode), a "you run
autonomously — interactive questions are auto-denied" note, and an instruction
to end with a concise result summary (recorded as the run's `result`).

Spec fields:

| Field | Required | Description |
|-------|----------|-------------|
| `prompt` | yes | Task instructions, written for an autonomous orchestrating agent |
| `budget_usd` | yes* | Hard dollar cap. *Optional only with `workflows.allow_unbudgeted: true` |
| `title` | no | Short human-readable label |
| `model` | no | Model override (default: the backend's configured model — `agent.model` / `codex.model`) |
| `effort` | no | Reasoning effort override (`low`/`medium`/`high`/`xhigh`/`max`) |
| `cwd` | no | Working directory for the run's session; must exist (default: `workspace`) |

## Engines

| Engine | Backend | Orchestrates via |
|--------|---------|------------------|
| `claude-workflow` | claude | The harness's built-in `Workflow` tool — parallel sub-agents inside the run's own Claude session |
| `codex-ultracode` | codex | Ultracode multi-agent runs — the managed plugin in the isolated Codex home |

**Pick `claude-workflow`** by default: no extra setup beyond a working Claude
backend, Anthropic models, and the most accurate mid-turn spend signal (live
`Workflow` snapshot tokens).

**Pick `codex-ultracode`** for GPT-family models or when you specifically want
Ultracode's worker orchestration. It needs a working Codex backend
(`nerve codex doctor` green) with `codex.ultracode.enabled: true` — the run's
session is a normal Codex session whose prompt instructs Ultracode
orchestration, so without the plugin the agent just works single-handed.
Ultracode's own caps (`codex.ultracode.max_concurrency`, token budget,
`max_agents`) still bound the inner workers. With ChatGPT subscription auth,
spend is metered from API-equivalent estimates rather than billed dollars
(see below).

## Lifecycle

```
pending ──▶ running ──▶ done
   │           ├──────▶ failed
   │           ├──────▶ killed
   │           └──────▶ budget_exhausted
   └──────────────────▶ killed          (killed before dispatch)
```

- At most `workflows.max_concurrent_runs` execute at once; excess runs queue
  in `pending` (costing nothing) and dispatch oldest-first as slots free.
- `done` stores the final message (last 4 KB in the DB row, full text in
  `result.md`) and notifies with spend vs budget. `failed` stores the error
  and notifies at high priority.
- Stopping the run's session directly (web Stop button, `stop_session`) lands
  the run in `killed` with reason "session stopped". Killing via the run API
  is idempotent on already-terminal runs.
- Every status or spend change broadcasts a `workflow_run_update` event on
  the global WebSocket channel (see [api.md](api.md)).

**Restarts.** Runs do **not** survive a daemon restart. On startup a recovery
pass marks any orphaned `pending`/`running` run `failed` ("interrupted by
nerve restart"), journals the interruption, and sends a high-priority
notification. Nothing resumes on its own — restart runs explicitly if still
needed. This is deliberate: a paid job can never burn unmetered.

## Budget

Budgets are **metered dollars**, not advisory token counts. Every
`workflows.poll_interval_seconds` the monitor recomputes each running run's
spend as:

```
spent = recorded + live
```

- **recorded** — the run session's per-turn costs from usage accounting:
  billed `cost_usd` when present, falling back to the API-equivalent
  `estimated_cost_usd` for subscription-auth Codex turns (ChatGPT auth bills
  no dollars; the estimate column carries the real spend).
- **live** — best-effort estimate for the in-flight turn:
  - *claude-workflow*: sub-agent output tokens summed from the session's live
    `Workflow` snapshots, priced as output tokens at the run's model.
  - *codex-ultracode*: Ultracode run journals under the Codex home created
    since the run started, priced per worker. Attribution is heuristic
    (start time + `cwd` match) — concurrent Codex runs sharing one `cwd` may
    cross-attribute mid-turn; the turn-end fold into recorded cost is exact.

**Meter accuracy.** Recorded cost lands only at turn end, and a run's whole
life can be one long turn — the live estimates bridge that gap but don't see
everything (e.g. the orchestrator's own tokens on the Claude side). The
practical overshoot bound is roughly **one turn's cost** beyond what the live
estimates catch. Treat a budget as a cap with real teeth, not an accounting
guarantee to the cent.

Enforcement:

- **At `warn_fraction` (default 80%)** — a one-time high-priority
  notification plus a `budget_warning` journal event; `warned_at` is stamped
  so it never repeats.
- **At 100%** — the run flips to `budget_exhausted` and is terminated:
  graceful `stop_session` interrupt, up to `kill_grace_seconds` for the turn
  to wind down, then a force client discard that kills the session's own CLI
  subprocess (Claude) or app-server process group (Codex). Termination is
  scoped strictly to the run's own session/process group — never a
  pattern-matched `pkill`. Other runs and sessions are untouched.

The prompt framing tells the run its budget and the metering cadence, so a
well-built spec paces fan-out and checkpoints partial results before the axe
falls (see the template below).

## Journal

```
<workflows.runs_dir>/<run-id>/
  run.json        # current run row, atomically rewritten on every change
  events.ndjson   # append-only lifecycle log: created, started, budget_warning,
                  # done/failed/killed/budget_exhausted, interrupted, enforced_stop
  result.md       # full final message (written on 'done' only)
```

The DB row references its session with `ON DELETE SET NULL`, so run history
survives session deletion — the journal directory keeps the details.

## Launching Runs

### Agent (MCP tools)

| Tool | Arguments |
|------|-----------|
| `workflow_run_start` | `engine`, `prompt`, `budget_usd` (required); `title`, `model`, `effort`, `cwd` |
| `workflow_run_status` | `run_id` |
| `workflow_run_kill` | `run_id`, `reason` |
| `workflow_run_list` | `status` (`active`, exact status, or empty), `limit` |

`workflow_run_start` returns the run id immediately; execution happens in the
background. Typical agent usage:

```
workflow_run_start(engine="claude-workflow", title="Sample batch audit",
                   budget_usd=12, prompt="Audit samples/batch-07/ for ...")
→ Workflow run wfr-3fa2b81c created (pending). Engine: claude-workflow,
  budget: $12.00. Journal: ~/.nerve/workflow-runs/wfr-3fa2b81c. ...

# later — poll instead of waiting; the run notifies on finish/warn/kill
workflow_run_status(run_id="wfr-3fa2b81c")
```

Start/kill are engine-owned operations and are rejected for external MCP
client sessions (Codex CLI, Claude Code, etc. connected via the MCP
endpoint) — external clients can inspect via `workflow_run_list`/`status`
only.

### REST

| Endpoint | Description |
|----------|-------------|
| `GET /api/workflow-runs?status=&limit=&offset=` | List runs, newest first (`status`: `active`, exact, or empty) |
| `POST /api/workflow-runs` | Start a run: `{engine, prompt, budget_usd, title?, model?, effort?, cwd?}` |
| `GET /api/workflow-runs/{id}` | Run detail (`spec.prompt` trimmed to 500 chars on the wire) |
| `POST /api/workflow-runs/{id}/kill` | Terminate: `{reason?}`; idempotent on terminal runs |
| `GET /api/workflow-runs/{id}/journal` | Journal contents: run snapshot, events, result |

See [api.md](api.md) for request/response shapes.

### CLI

```bash
nerve workflow list                 # runs with status + spend vs budget
nerve workflow status <run-id>      # one run in detail
nerve workflow start ...            # start a run (engine, prompt, budget)
nerve workflow kill <run-id>        # terminate a run
```

See `nerve workflow --help` for flags.

### Cron

A cron job may declare a `workflow` block **instead of** `prompt` — each
trigger starts a workflow run:

```yaml
# ~/.nerve/cron/jobs.yaml
jobs:
  - id: nightly-sample-audit
    schedule: "0 3 * * *"
    workflow:
      engine: claude-workflow
      prompt: "Audit yesterday's ingest batch under samples/ for ..."
      budget_usd: 10
      title: Nightly sample audit   # optional — model/effort/cwd too
```

The block mirrors the REST/MCP spec: `engine`, `prompt`, `budget_usd`
(required, positive), `title` (defaults to the job id), `model`, `effort`,
`cwd` (optional). `workflow` takes precedence over `prompt`/`prompt_file`.
The job is fire-and-forget: the cron log records only the launch — no cron
session is created, and the run sends its own budget/completion/failure
notifications. General job fields (schedule, gates, enabled) are unchanged —
see [cron.md](cron.md).

## Spec Template: Re-Review Until Agreement

A robust pattern for quality-critical jobs: the inner workflow alternates
**generate** and **adversarial verify** cycles and stops only when a verify
pass produces no new findings and no disproofs. Budget is sliced per cycle so
a budget stop degrades to a smaller-but-verified report instead of nothing.

```
workflow_run_start(
  engine="claude-workflow",
  title="Sample batch quality audit",
  budget_usd=12,
  cwd="/data/audits",
  prompt="""
Audit the dataset sample under samples/batch-07/ for quality issues:
mislabeled records, duplicates, schema drift, impossible values.

Run generate/verify cycles until the findings converge.

Cycle N:
1. GENERATE — fan out one worker per file group. Each worker writes
   candidate findings to findings/cycle-N/<group>.md as
   (id, issue, severity, evidence: file + record ids).
2. VERIFY — fan out fresh adversarial reviewers, one per file group,
   assigned so no reviewer checks findings they generated. Their brief:
   try to DISPROVE every candidate finding against the raw data, and
   hunt for issues the generators missed. Verdict per finding:
   confirmed / disproved (with reason) / new.
3. RECONCILE — merge verdicts into findings/confirmed.md. If this
   verify pass produced zero new findings and zero disproofs, STOP:
   agreement reached. Otherwise start cycle N+1 from the corrected
   findings.

BUDGET: hard cap $12, metered by Nerve. Plan roughly $3 per full cycle;
before starting a cycle, if less than one cycle's cost remains, skip
straight to the final report with what is already confirmed. Checkpoint
the findings/ files at the end of every cycle so a budget stop loses at
most one cycle of work.

Final message: confirmed findings as a severity-sorted table with
evidence pointers, plus how many cycles convergence took.
""")
```

Why this shape works:

- Fresh verifiers per cycle stop the generator from grading its own work.
- "No new findings and no disproofs" is a concrete convergence test, not
  vibes — the loop terminates itself.
- Per-cycle budget slices + end-of-cycle checkpoints mean the 100% enforcement
  (which does not wait for in-flight work beyond the grace window) still
  leaves a usable, verified partial result in the journal's `cwd` artifacts.

## Concurrency

- **Each running workflow occupies one `agent.max_concurrent` slot for the
  entire duration of its (typically very long) turn.** Keep
  `workflows.max_concurrent_runs` (default 2) well below `agent.max_concurrent`,
  or interactive chats will queue behind unattended batch jobs.
- Inside a run, engine limits still apply — Ultracode workers are capped by
  `codex.ultracode.max_concurrency` / `max_agents`.
- Excess runs queue in `pending` and are promoted oldest-first; queued runs
  cost nothing.

## Configuration

```yaml
workflows:
  enabled: true                       # master switch (service + tools + API)
  runs_dir: ~/.nerve/workflow-runs    # journal root
  poll_interval_seconds: 60           # budget monitor cadence
  warn_fraction: 0.8                  # one-time warning threshold
  kill_grace_seconds: 30              # graceful stop → grace → force kill
  max_concurrent_runs: 2              # excess runs queue as 'pending'
  allow_unbudgeted: false             # require budget_usd on every run
```

See [config.md](config.md#workflow-runs) for the full reference.
