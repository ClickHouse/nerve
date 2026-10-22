---
name: Nerve Development
description: >
  Nerve backend (Python) and frontend (React/TS) development and code contribution.
  Use when writing Python code for Nerve, fixing bugs, adding features, reviewing Nerve PRs,
  building the frontend, running tests, or working with the Nerve codebase.
  Triggers on "nerve code", "nerve PR", "fix nerve", "nerve feature", "nerve test",
  "build nerve UI", "nerve migration", "nerve lint".
version: 1.1.0
context: domain
---

# Nerve Development Skill

> **Changing your own config (skills, cron, settings), not the app code?** Use
> the `nerve-workspace` skill instead — that covers the workspace, and which of a
> pull request or a direct edit applies to this instance. This skill is for
> developing the Nerve *application* codebase.

## Repository

- **Upstream:** https://github.com/ClickHouse/nerve (`git@github.com:ClickHouse/nerve.git`)
- **Issues & PRs:** https://github.com/ClickHouse/nerve/pulls

All changes go through PRs — **never push directly to main**. The repo is
public; see the security section before every commit.

### Getting Started (Contributors)

Prerequisites: Python **3.13+** (`pyproject.toml` pins `requires-python = ">=3.13"`),
[uv](https://docs.astral.sh/uv/), Node.js **22.12+**, `git`, `gh`.

```bash
# 1. Fork on GitHub, then clone your fork
git clone git@github.com:<your-username>/nerve.git
cd nerve

# 2. Add upstream remote
git remote add upstream git@github.com:ClickHouse/nerve.git

# 3. Python environment — same resolution as CI (fails if uv.lock is stale)
uv sync --locked --extra test
source .venv/bin/activate

# 4. Frontend
cd web && npm ci --strict-peer-deps && cd ..

# 5. Verify
.venv/bin/pytest tests/ -v
cd web && npm run build && npm test && npm run lint && cd ..
```

Run the commands below from the repository root (wherever you cloned it).

## Project Layout

```
<repo>/
├── nerve/                  # Python backend (FastAPI + agent backends)
│   ├── cli.py              # Click CLI: init/start/stop/restart/status/logs/doctor/upgrade/reload/backup/
│   │                       #   restore/pair/sync/cron/migrate/migrate-openclaw/backfill-timestamps/setup-telegram
│   │                       #   + groups `config`, `codex`, `db`, `workflow`
│   ├── config.py           # Config dataclasses + three-layer loader, ${VAR} interpolation
│   ├── config_validate.py  # `nerve config validate` (also used by sync and config PRs)
│   ├── config_reload.py    # hot reload (`nerve reload`, POST /api/config/reload)
│   ├── config_pr.py        # propose_config_change — PR against the workspace repo
│   ├── config_repo.py      # `nerve config init-repo` scaffold
│   ├── sync_service.py     # git-backed workspace sync, lockdown clean-tree checks
│   ├── paths.py            # ~/.nerve (NERVE_HOME) state-dir resolver — never hardcode ~/.nerve
│   ├── identity.py         # Actor (human / system) attribution model
│   ├── setup_token.py      # first-start claim token      pairing.py — Telegram pairing codes
│   ├── bootstrap.py        # `nerve init` wizard, CORE_CRONS, template installation
│   ├── workspace.py        # workspace templates (MANIFEST), bundled skills
│   ├── backup.py           # `nerve backup` / `nerve restore` bundles
│   ├── migrate.py          # legacy config layout migration
│   ├── models_catalog.py   # model picker discovery   ollama.py, coerce.py, _env.py
│   ├── db/                 # SQLite (aiosqlite): Database = base.py + domain mixins
│   │   ├── base.py         # connection, pragmas, write lock, migrations, FTS health
│   │   ├── migrations/     # vNNN_*.py + runner.py (SCHEMA_VERSION = highest file number); fork/ = a fork's own track
│   │   └── sessions.py messages.py tasks.py task_statuses.py plans.py notifications.py
│   │       sources.py cron.py skills.py mcp.py audit.py accounts.py files.py usage.py
│   │       wakeups.py workflow_runs.py review_loops.py observations.py maintenance.py
│   ├── agent/              # engine.py, prompts.py, sessions.py, streaming.py, interactive.py,
│   │   │                   #   plan_service.py, cache_policy.py
│   │   ├── backends/       # AgentBackend protocol: claude.py (Claude Agent SDK), codex/ (app-server), events.py
│   │   └── tools/          # registry.py (ToolSpec/ToolContext/ToolResult), schemas.py, claude_sdk_adapter.py
│   │       └── handlers/   #   one handler module per tool domain
│   ├── gateway/            # server.py (app factory, lifespan, WS, static), auth.py (JWT, require_auth), diff.py
│   │   └── routes/         # one module per API domain, assembled by register_all_routes()
│   ├── channels/           # web.py, telegram.py, slack*.py, router.py, access.py,
│   │                       #   observation.py, archives.py, stream_adapter.py, base.py
│   ├── sources/            # inbox sources: telegram, gmail, imap, github, github_events, github_repos,
│   │                       #   channel (observed chats), codex_threads/; runner.py, filters.py, registry.py
│   ├── cron/               # APScheduler service, jobs.py, gates.py, gate_plugins.py
│   ├── memory/             # files.py, memu_bridge.py (memU), loom.py (Loom), xmemory_bridge.py
│   ├── skills/             # SkillManager — filesystem skills + DB index
│   ├── tasks/              # task markdown files + manager
│   ├── notifications/      # NotificationService, silences, approval dispatch
│   ├── workflows/          # workflow runs (service.py) and review loops (review_loop.py)
│   ├── mcp_server/         # external MCP endpoint (/mcp/v1) + Codex loopback listener
│   ├── external_agents/    # Codex / Claude Code bootstrap, memory-bundle renderers, sync service
│   ├── observability/      # Langfuse tracing, usage rewrite
│   ├── proxy/              # CLIProxyAPI daemon (subscription auth, no API key)
│   ├── utils/              # aio.py, fs.py, time.py
│   └── templates/          # personal/, worker/ (+ MANIFEST), config/settings.yaml,
│                           #   config-repo/, skills/ (bundled: nerve-dev, nerve-workspace)
├── web/                    # React + TypeScript + Vite frontend
│   ├── src/api/            # REST client, WebSocket client
│   ├── src/components/     # Auth, Chat, Cron, ExternalAgents, Files, Layout, Memory, Notifications, Sources, Tasks, ui
│   ├── src/pages/          # route pages (ChatPage, TasksPage, PlansPage, WorkflowRunsPage, ...)
│   ├── src/stores/         # Zustand stores; chatStore + handlers/ (WS dispatch) + helpers/
│   ├── src/hooks/ src/utils/ src/types/
│   ├── src/test/setup.ts   # vitest setup (jsdom + Testing Library)
│   └── eslint.config.js, vite.config.ts, package.json, package-lock.json
├── tests/                  # pytest suite; conftest.py isolates NERVE_HOME per test
├── scripts/                # check_migration_numbers.py, check_codex_schema.py, codex_smoke.py, backtest_cache_ttl.py
├── docs/                   # architecture, config, api, cron, memory, ... — update alongside code
├── .github/workflows/      # ci.yml (backend tests, migration check, frontend build + vitest, Node floor), slack-live.yml
├── config.example.yaml     # committed example; config.yaml and config.local.yaml are gitignored
├── install.sh
├── pyproject.toml, uv.lock # commit uv.lock together with pyproject.toml
└── README.md
```

## Gateway Routes Architecture

Routes are split into domain modules under `nerve/gateway/routes/`: `auth`,
`accounts`, `setup`, `actors`, `sessions`, `tasks`, `plans`, `skills`,
`mcp_servers`, `memory`, `diagnostics`, `cron`, `sources`, `notifications`,
`files`, `external_agents`, `prompt_rewrite`, `models`, `codex`,
`workflow_runs`, `review_loops`, `config`. Each module:

- defines its own `router = APIRouter()` with related endpoints,
- imports `get_deps()` from `_deps.py` to reach the engine, DB, notification
  service and external-agents sync service,
- keeps its Pydantic request models co-located with the handlers.

**Adding an endpoint:** add the handler to the matching domain module; nothing
else to wire. **Adding a domain:** create `routes/new_domain.py` with a
`router = APIRouter()`, then import it and `include_router` it in
`routes/__init__.py::register_all_routes()` — inclusion is explicit, not by
discovery. (`accounts` and `setup` are included only in local-accounts auth mode.)

**Dependency and auth pattern:**
```python
from fastapi import APIRouter, Depends

from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps
from nerve.identity import Actor

router = APIRouter(dependencies=[Depends(require_auth)])

@router.get("/api/example")
async def example():
    deps = get_deps()
    # deps.engine, deps.db, deps.notification_service, deps.external_agents_sync
```

Use `actor: Actor = Depends(require_auth)` on handlers that consume identity
for attribution or account authority; otherwise gate the module through the
router dependency. Routes that invoke agent tools go through the shared
registry (`_deps.get_tool_registry()` / `build_route_tool_context()`), so the
REST and MCP paths share one implementation.

## Agent Tools

Every agent tool is a `ToolSpec` (name, description, JSON schema, async
handler) — see `nerve/agent/tools/registry.py`. Agents call them as
`mcp__nerve__<name>`.

**Adding a tool:** put the schema in `nerve/agent/tools/schemas.py`, the handler
and its `ToolSpec` in `nerve/agent/tools/handlers/<domain>.py`, and append the
spec to that module's `*_SPECS` list — `handlers/__init__.py::build_default_registry()`
registers every list. Handlers read state only from the `ToolContext` they are
handed and return a `ToolResult`. The same registry serves the in-process MCP
server of Claude sessions, the Codex loopback listener and the external
`/mcp/v1` endpoint, and the system prompt lists every served tool
automatically. Then document the tool in `docs/` and, if users should know it,
in the `TOOLS.md` templates under `nerve/templates/`.

## Development Workflow

**Order for every change:**

1. **Branch** — `git checkout -b <username>/<feature-name>` from up-to-date main
2. **Recall** — `memory_recall` for past issues, conventions and preferences on the area
3. **Backend first** — Python under `nerve/`; add or renumber a migration if the schema changes
4. **Frontend second** — TypeScript/React under `web/src/`
5. **Tests** — `.venv/bin/pytest tests/ -v`; `cd web && npm test && npm run lint`
6. **Build UI** — `cd web && npm run build` (the server serves `web/dist/`)
7. **Docs** — update the matching `docs/*.md` (and README tables) in the same change
8. **Commit** — focused, scoped commits on the feature branch
9. **Review** — launch a review subagent, act on its feedback, run the leak check below
10. **Push to fork** — `git push origin <username>/<feature-name>`
11. **PR** — open it against upstream `main` (https://github.com/ClickHouse/nerve)

### PR Workflow

```bash
# Keep main up to date
git fetch upstream
git checkout main
git merge upstream/main

# Create feature branch
git checkout -b <username>/<feature-name>

# ... make changes, test, build ...

# Push to your fork
git push origin <username>/<feature-name>

# Create PR via gh CLI
gh pr create --repo https://github.com/ClickHouse/nerve \
  --title "Short description" \
  --body "Details of the change"
```

**PR conventions:**
- Keep PRs focused — one feature or fix per PR
- Include test coverage for new functionality
- Run the full backend suite, the frontend build and `npm test` before opening
- CI runs: backend tests on Python 3.13 with `uv sync --locked`, the migration
  number check, `npm ci --strict-peer-deps && npm run build && npm test`, and an
  install + build on the Node floor (22.12.0). `npm run lint` is not in CI — run
  it yourself.

## Build Commands

### Backend

```bash
# Install / resync exactly what uv.lock says (what CI does)
uv sync --locked --extra test

# Run all tests (~1 minute)
.venv/bin/pytest tests/ -v

# Run one file, one class, or a keyword selection
.venv/bin/pytest tests/test_sessions.py -v
.venv/bin/pytest tests/test_db.py::TestClassName -v
.venv/bin/pytest tests/ -k "task and not slack" -q

# After changing pyproject.toml dependencies: relock, then sync.
# Commit uv.lock alongside pyproject.toml — CI runs `uv sync --locked`
# and fails if the two have drifted apart.
uv lock && uv sync --locked --extra test

# What CI runs on your new migrations (see Critical Rules); a fork of Nerve adds --fork
python3 scripts/check_migration_numbers.py origin/main

# Health check of a running install
.venv/bin/nerve doctor
```

`tests/conftest.py` points `NERVE_HOME` at a temp directory, so the suite never
touches a live install. The live Slack tests (`tests/test_slack_live*.py`) skip
unless `NERVE_SLACK_TEST_*` variables are set — see `docs/testing-slack.md`.

### Frontend

```bash
cd web

npm ci --strict-peer-deps   # install from package-lock.json (what CI does)
npm install                 # only when you change package.json; commit package-lock.json

npm run build               # tsc -b && vite build → web/dist/ (served by FastAPI)
npm test                    # vitest run — jsdom, specs are src/**/*.test.{ts,tsx}
npm run test:watch          # vitest in watch mode
npm run lint                # eslint .
npm run dev                 # Vite dev server; proxies /api and /ws to localhost:8900
```

## Critical Rules

1. **NEVER push directly to main.** All changes require a PR to the upstream repository (https://github.com/ClickHouse/nerve).

2. **ALWAYS run `npm run build` after ANY frontend change.** The server serves pre-built static files from `web/dist/`. Skip the build = changes invisible. This is the #1 gotcha.

3. **ALWAYS run tests before committing** — backend `pytest`, and `npm test` for frontend changes. All tests must pass.

4. **ALWAYS `memory_recall` before modifying Nerve code.** Check for past issues, conventions, and preferences.

5. **Config layers:** `<workspace>/config/settings.yaml` (shareable, git-tracked with the workspace) < `config.yaml` (machine-local, gitignored) < `config.local.yaml` (secrets, gitignored). `config.example.yaml` is the committed example. Never put credentials in a tracked file — reference them as `${ENV_VAR}` or keep them in `config.local.yaml`. New keys go in the dataclasses in `nerve/config.py` and in the `docs/config.md` table, with their layer and whether they hot-reload.

6. **Database migrations:** each migration is a numbered Python file in `nerve/db/migrations/` (`vNNN_your_feature.py`) exporting `async def up(db)`. `SCHEMA_VERSION` is derived from the highest file number. Number a new migration **above the highest version on `main`** — the runner skips versions at or below the database's current one, so a lower number never runs on an already-migrated database. CI enforces this with `scripts/check_migration_numbers.py`; gaps in the sequence are fine, renumbering an existing migration or reusing its number is not. **Forks add migrations only under `fork/`** (`nerve/db/migrations/fork/`). They are numbered from `v001`, apart from upstream's numbers, and recorded in their own `fork_schema_version` table, so a fork's migration never shadows an upstream one. Never add a fork's migration to the upstream track. Upstream never ships a fork migration (the check fails on one; a fork runs it with `--fork`). No transaction wraps a migration, so make each one idempotent. A database newer than the code on either track refuses to start: one line, exit status 65 (override only deliberately with `NERVE_ALLOW_NEWER_SCHEMA=1`).

7. **Database domain modules:** data access lives in mixin classes by domain under `nerve/db/` (sessions, messages, tasks, plans, notifications, sources, cron, skills, mcp, audit, accounts, usage, workflow_runs, review_loops, ...). `Database` in `base.py` inherits them all. Add methods to the matching mixin, not to `base.py`.

8. **State paths:** anything under `~/.nerve` is resolved through `nerve/paths.py` (`NERVE_HOME` can move it). Use the accessors; never write `~/.nerve/...` literals in code or operator-facing output.

9. **Commit style:** focused, scoped commits. Backend + frontend + docs for one feature = one commit. Don't bundle unrelated changes.

## ⚠️ Security: Pre-Commit Leak Check

**Nerve is open source. This repo is PUBLIC.** Before every commit and push,
scan what you are about to publish for leaked secrets and private data.

**What to scan for:**
- API keys and tokens of any provider (`sk-`, `ghp_`, `xoxb-`/`xapp-`, bearer tokens, JWT secrets)
- Passwords and hashes (bcrypt hashes, keyring passwords, Telegram `api_id`/`api_hash`/`bot_token`)
- Personal emails or usernames, real names
- Service URLs with credentials, internal hostnames, IPs, private network names
- Anything that looks like it belongs in `config.local.yaml` or an `${ENV_VAR}`

**Scan the whole range you push, not only the last diff:** a cleanup commit on
top of pushed history removes nothing.

```bash
git diff --cached                              # before each commit
git log -p origin/main..HEAD | grep -n -i -E "sk-|ghp_|xox[abp]-|api_key|password|@[a-z0-9-]+\.(com|net|io)"
```

If any commit in the range is dirty, squash or rewrite it locally, re-scan the
rewritten range, then push once.

**Safe to commit:**
- Generic field names with empty defaults (`anthropic_api_key: str = ""`)
- References to `config.local.yaml` or `${ENV_VAR}` in comments/docs
- Example placeholder values in docs

**If anything leaks:** do NOT push. Unstage the file, fix it, re-scan. If
already pushed, the secret must be rotated immediately — a force-push does not
guarantee removal from a public fork network.

### No Real-Life Data in Examples

**Never use real-world information in code comments, docstrings, test fixtures, or LLM prompt examples.** This includes:

- **Real project/company names** — use generic placeholders like `"deploy the app"`, `"fix issue #101"`
- **Real incidents or tasks** — don't copy from your task list into test data; invent obviously synthetic scenarios
- **Real bot/service names** — use generic names like `some-ci[bot]`
- **Real credit card numbers, even partial** — use standard test numbers like `4111111111111111`
- **Realistic-looking issue numbers** from real trackers — use low, obviously synthetic numbers
- **Real usernames, hosts or domains** in examples — use `<your-username>`, `alice`/`bob`, `example.com`

**Why:** Even "innocent" examples build an identity fingerprint when cross-referenced. Multiple small leaks across comments, tests, and docs compound into a deanonymization risk.

**Rule of thumb:** If an example could be traced back to a specific person, company, or incident — replace it with something generic.

## Restarting Nerve

`nerve restart` works the same way in every deployment and is the only restart
you should need:

```bash
nerve restart                  # stop the old daemon, start a new one
nerve restart --resume <id>    # also re-drive an interrupted session afterwards
nerve status                   # PID, uptime, port
nerve logs                     # tail the daemon log
nerve doctor                   # config, DB, keys, cron, sync, connectivity
```

- **Bare metal:** a detached helper sends SIGTERM to the old PID, waits for it
  (SIGKILL after 15 s), then starts `nerve start --foreground` in the
  background — so it also works when triggered from inside the daemon.
- **Docker** (`deployment: docker` in config): the same commands proxy to
  `docker compose` (`restart`, `down`, `up`).
- **systemd** (`Restart=always`): `nerve restart` just stops the process and
  systemd respawns it; `sudo systemctl restart nerve` is equivalent.
- **Config-only edits:** prefer `nerve reload` — it hot-reloads cron, gates,
  sources, skills, MCP servers, notifications and more, and prints
  `restart_required` for keys that still need a restart (`docs/config.md`, "Hot-Reload").
- **To update a running install:** `nerve upgrade` (git pull + Python deps + frontend build; `--no-pull` to build local changes only), then `nerve restart`.
- A restart interrupts live sessions and kills in-flight workflow runs (they are
  marked failed and the user is notified). Never `pkill -f nerve` — use the
  commands above, which act on the recorded PID.

Key paths (under `~/.nerve`, or `$NERVE_HOME` when set):
- PID: `nerve.pid`
- Log: `nerve.log`
- DB: `nerve.db` (memory index: `memu.sqlite`)
- Config-dir pointer: `config_dir`

## Key Tech Stack

| Layer | Tech |
|-------|------|
| Backend | Python 3.13+, FastAPI, Uvicorn, aiosqlite, Click |
| Agents | Claude Agent SDK (in-process MCP tools), Codex app-server backend, `mcp` 2.x for the external endpoint |
| Model access | Anthropic API, Bedrock, or Claude subscription via the CLIProxyAPI proxy |
| Memory | memU (`memu-py`, pinned), or Loom through the MCP gateway (`memory.backend: loom`); optional xmemory |
| Scheduler | APScheduler |
| Channels | python-telegram-bot + Telethon, slack-sdk (Socket Mode), WebSocket web UI |
| Auth | JWT (pyjwt) + bcrypt, local accounts with actor attribution |
| Observability | Langfuse (optional) |
| Frontend | React 19, TypeScript 5.9, Vite 7, Tailwind 4, Zustand |
| Tests | pytest + pytest-asyncio; vitest + Testing Library (jsdom); eslint (typescript-eslint) |
| Packaging | uv + `uv.lock`, hatchling; Node 22.12+ for the UI build |
