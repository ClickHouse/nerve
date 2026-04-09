---
name: Nerve Development
description: >
  Nerve backend (Python) and frontend (React/TS) development and code contribution.
  Use when writing Python code for Nerve, fixing bugs, adding features, reviewing Nerve PRs,
  building the frontend, running tests, or working with the Nerve codebase.
  Triggers on "nerve code", "nerve PR", "fix nerve", "nerve feature", "nerve test",
  "build nerve UI", "nerve migration".
version: 1.0.0
context: domain
---

# Nerve Development Skill

## Repository

Nerve lives under the **ClickHouse organization** on GitHub.

- **Upstream:** `git@github.com:ClickHouse/nerve.git`
- **Issues & PRs:** https://github.com/ClickHouse/nerve

All changes go through PRs — **never push directly to main**.

### Getting Started (Contributors)

```bash
# 1. Fork on GitHub, then clone your fork
git clone git@github.com:<your-username>/nerve.git
cd nerve

# 2. Add upstream remote
git remote add upstream git@github.com:ClickHouse/nerve.git

# 3. Set up Python environment
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

# 4. Set up frontend
cd web && npm install && cd ..

# 5. Verify
.venv/bin/pytest tests/ -v
cd web && npm run build
```

## Project Layout

```
nerve/
├── nerve/              # Python backend (FastAPI + Claude Agent SDK)
│   ├── cli.py          # Click CLI (start/stop/restart/status/logs/doctor)
│   ├── config.py       # YAML config loader
│   ├── db/             # SQLite async database package
│   │   ├── base.py         # Database class (connection, _atomic, FTS, mixin composition)
│   │   ├── migrations/     # File-based migration system (numbered .py files + runner.py)
│   │   ├── sessions.py     # SessionStore mixin
│   │   ├── messages.py     # MessageStore mixin
│   │   ├── tasks.py        # TaskStore mixin
│   │   ├── plans.py        # PlanStore mixin
│   │   ├── notifications.py # NotificationStore mixin
│   │   ├── sources.py      # SourceStore mixin (inbox, sync/consumer cursors)
│   │   ├── cron.py         # CronStore mixin
│   │   ├── skills.py       # SkillStore mixin
│   │   ├── mcp.py          # McpStore mixin
│   │   └── audit.py        # AuditStore mixin
│   ├── bootstrap.py    # Self-configuring onboarding (personal/worker mode)
│   ├── workspace.py    # Workspace directory management
│   ├── agent/          # Agent engine, tools, sessions, streaming
│   ├── gateway/        # FastAPI server, WebSocket, auth
│   │   ├── server.py       # App factory, lifespan, WebSocket, static serving
│   │   ├── auth.py         # JWT auth, password verification
│   │   └── routes/         # Domain-specific REST API route modules
│   │       ├── __init__.py      # register_all_routes() assembler
│   │       ├── _deps.py         # RouteDeps container (engine, db, notification_service)
│   │       ├── sessions.py      # /api/sessions/*, /api/chat
│   │       ├── tasks.py         # /api/tasks/*
│   │       ├── plans.py         # /api/plans/*, /api/tasks/{id}/plans
│   │       ├── skills.py        # /api/skills/*
│   │       ├── mcp_servers.py   # /api/mcp-servers/*
│   │       ├── memory.py        # /api/memory/*
│   │       ├── diagnostics.py   # /api/diagnostics
│   │       ├── cron.py          # /api/cron/*
│   │       ├── sources.py       # /api/sources/*
│   │       ├── notifications.py # /api/notifications/*
│   │       └── houseofagents.py # /api/houseofagents/*
│   ├── channels/       # Telegram bot, web UI channel adapters
│   ├── cron/           # APScheduler job runner
│   ├── sources/        # Data source adapters (Telegram, Gmail, GitHub)
│   ├── memory/         # Markdown files + memU semantic indexing
│   ├── skills/         # Filesystem-based skill loader
│   ├── tasks/          # Task markdown files + SQLite index
│   ├── notifications/  # Fire-and-forget + async question delivery
│   ├── proxy/          # CLIProxyAPI daemon (Docker/worker mode)
│   ├── houseofagents/  # Optional multi-agent runtime
│   └── templates/      # Mode templates for nerve init (personal/, worker/)
├── web/                # React + TypeScript + Vite frontend
│   ├── src/
│   │   ├── components/ # React components
│   │   ├── pages/      # Route pages
│   │   ├── stores/     # Zustand state stores
│   │   │   ├── chatStore.ts    # Chat state + thin WS dispatcher
│   │   │   ├── handlers/       # Domain-specific WS message handlers
│   │   │   └── helpers/        # Stateless helpers (block ops, buffer replay)
│   │   ├── api/        # API client utilities
│   │   └── types/      # TypeScript definitions
│   └── package.json
├── tests/              # pytest suite
├── docs/               # Architecture, config, API docs
├── config.yaml         # Main config (committed)
├── config.local.yaml   # Secrets (gitignored)
└── pyproject.toml      # Python project metadata + entry point
```

## Gateway Routes Architecture

Routes are split into domain-specific modules under `nerve/gateway/routes/`. Each module:
- Defines its own `router = APIRouter()` with related endpoints
- Imports `get_deps()` from `_deps.py` to access engine/db/notification_service
- Keeps Pydantic request models co-located with the handlers

**Adding a new endpoint:** Create or edit the appropriate domain module, add the route handler — it's automatically included via `register_all_routes()` in `__init__.py`.

**Adding a new domain:** Create `routes/new_domain.py` with a `router = APIRouter()`, add endpoints, then import and include it in `routes/__init__.py`.

**Dependency access pattern:**
```python
from nerve.gateway.routes._deps import get_deps

@router.get("/api/example")
async def example(user: dict = Depends(require_auth)):
    deps = get_deps()
    # Use deps.engine, deps.db, deps.notification_service
```

## Development Workflow

**Mandatory order for every change:**

1. **Create a feature branch** — `git checkout -b <username>/<feature-name>` from up-to-date main
2. **Backend first** — modify Python files under `nerve/`
3. **Frontend second** — modify TypeScript/React under `web/src/`
4. **Run tests** — `cd ~/nerve && .venv/bin/pytest tests/ -v`
5. **Build UI** — `cd ~/nerve/web && npm run build`
6. **Commit** — focused, scoped commits to the feature branch
7. **Push to fork** — `git push origin <username>/<feature-name>`
8. **Create a PR** — open PR targeting `ClickHouse/nerve:main`

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
gh pr create --repo ClickHouse/nerve \
  --title "Short description" \
  --body "Details of the change"
```

**PR conventions:**
- Keep PRs focused — one feature or fix per PR
- Include test coverage for new functionality
- Run the full test suite before opening

## Build Commands

### Backend

```bash
cd ~/nerve

# Run all tests
.venv/bin/pytest tests/ -v

# Run specific test file or class
.venv/bin/pytest tests/test_sessions.py -v
.venv/bin/pytest tests/test_db.py::TestClassName -v

# Install/update dependencies after pyproject.toml changes
.venv/bin/uv pip install -e .

# Health check
.venv/bin/nerve doctor
```

### Frontend

```bash
cd ~/nerve/web

# Build for production (TypeScript check + Vite bundle → dist/)
npm run build

# Install dependencies (only if package.json changed)
npm install
```

`npm run build` runs `tsc -b && vite build`. Output goes to `web/dist/` which FastAPI serves as static files.

## Critical Rules

1. **NEVER push directly to main.** All changes require a PR to `ClickHouse/nerve`.

2. **ALWAYS run `npm run build` after ANY frontend change.** The server serves pre-built static files from `web/dist/`. Skip the build = changes invisible. This is the #1 gotcha.

3. **ALWAYS run tests before committing.** All tests must pass.

4. **ALWAYS `memory_recall` before modifying Nerve code.** Check for past issues, conventions, and preferences.

5. **Config files:** `config.yaml` (committed) + `config.local.yaml` (gitignored, secrets). Never put API keys in `config.yaml`.

6. **Database migrations:** Each migration is a numbered Python file in `nerve/db/migrations/` (e.g. `v018_your_feature.py`) exporting an `async def up(db)` function. `SCHEMA_VERSION` is derived automatically from the highest migration file number. To add a new migration, create the next numbered file — no other changes needed.

7. **Database domain modules:** Data access methods are organized into mixin classes by domain (sessions, tasks, plans, etc.) under `nerve/db/`. The `Database` class in `base.py` inherits all mixins. Add new methods to the appropriate domain mixin, not to base.py.

8. **Commit style:** Focused, scoped commits. Backend + frontend + docs for one feature = one commit. Don't bundle unrelated changes.

## ⚠️ Security: Pre-Commit Leak Check

**Nerve is open source. This repo is PUBLIC.** Before every commit and push, scan the staged diff for leaked secrets.

**What to scan for:**
- API keys and tokens (Anthropic, OpenAI, Brave, Telegram, Grafana, any `sk-`, `glsa_`, bearer tokens)
- Passwords and hashes (bcrypt hashes, JWT secrets, keyring passwords)
- Personal emails or usernames
- Service URLs with credentials or internal hostnames
- Telegram API credentials (`api_id`, `api_hash`, `bot_token`)
- Anything that looks like it belongs in `config.local.yaml`

**Safe to commit:**
- Generic field names with empty defaults (`anthropic_api_key: str = ""`)
- References to `config.local.yaml` in comments/docs
- Example placeholder values in docs

**If anything leaks:** Do NOT push. Unstage the file, fix it, re-scan. If already pushed, the secret must be rotated immediately.

### No Real-Life Data in Examples

**Never use real-world information in code comments, docstrings, test fixtures, or LLM prompt examples.** This includes:

- **Real project/company names** — use generic placeholders like `"deploy the app"`, `"fix issue #101"`
- **Real incidents or tasks** — don't copy from your task list into test data; invent obviously synthetic scenarios
- **Real bot/service names** — use generic names like `some-ci[bot]`
- **Real credit card numbers, even partial** — use standard test numbers like `4111111111111111`
- **Realistic-looking issue numbers** from real trackers — use low, obviously synthetic numbers
- **Real usernames** in examples — use `<your-username>` or `alice`/`bob`

**Why:** Even "innocent" examples build an identity fingerprint when cross-referenced. Multiple small leaks across comments, tests, and docs compound into a deanonymization risk.

**Rule of thumb:** If an example could be traced back to a specific person, company, or incident — replace it with something generic.

## Restarting Nerve

```bash
nerve restart

# If that fails:
nerve stop && nerve start

# Verify:
nerve status
nerve logs
```

Key paths:
- PID: `~/.nerve/nerve.pid`
- Log: `~/.nerve/nerve.log`
- DB: `~/.nerve/nerve.db`

## Key Tech Stack

| Layer | Tech |
|-------|------|
| Backend | Python 3.12+, FastAPI, Uvicorn, aiosqlite |
| AI | Claude Agent SDK, Anthropic API |
| Scheduler | APScheduler |
| Telegram | python-telegram-bot + Telethon |
| Auth | JWT (pyjwt) + bcrypt |
| Frontend | React 19, TypeScript 5.9, Vite 7, Tailwind 4 |
| State | Zustand |
| Tests | pytest + pytest-asyncio |
