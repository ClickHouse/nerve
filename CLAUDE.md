# CLAUDE.md

Guide for Claude Code working in **nerve**. Keep it short — depth lives in `docs/`.

## Overview

Nerve is a self-hosted, single-process personal AI assistant runtime: one Python
process (FastAPI + asyncio) wrapping the **Claude Agent SDK**, backed by SQLite +
a markdown file store. Runs in two modes — **personal** (your assistant) and
**worker** (headless agent). Python backend in `nerve/`, React + Vite + TS web UI
in `web/`. See `docs/architecture.md`.

## Commands

Backend uses **uv** (no `uv.lock`; CI keys its cache on `pyproject.toml`).

```bash
# Setup
uv venv --python 3.13 && source .venv/bin/activate && uv pip install -e ".[test]"

# Tests (CI runs the second form)
pytest tests/ -v
.venv/bin/pytest tests/ -v

# Run app
nerve init                 # first-time setup
nerve start -f             # foreground
nerve stop|restart|status|doctor   # lifecycle (see nerve/cli.py)

# Web UI (from web/)
cd web && npm ci
npm run build              # tsc -b && vite build
npm run dev                # vite dev server
npm run lint               # eslint
```

CI (`.github/workflows/ci.yml`): backend tests on Python 3.13 + frontend Vite
build. No Python linter configured.

## Layout

Backend subsystems under `nerve/`:

| Path | Role |
|------|------|
| `gateway/` | FastAPI — REST, WebSocket, static UI serving |
| `agent/` | Claude SDK engine: sessions, interactive tool handler, diff hooks |
| `channels/` | Telegram + web channel adapters |
| `sources/` | External content ingestion (cursor-based) |
| `memory/` | L1 `MEMORY.md` + L2 memU bridge |
| `tasks/` · `cron/` | Background tasks + scheduled jobs |
| `skills/` · `mcp_server/` | Agent skills + nerve's own MCP server |
| `proxy/` | CLIProxyAPI OAuth |
| `notifications/` · `observability/` · `sync/` · `external_agents/` · `houseofagents/` | named per role |
| `db/` | `Database` composed from domain mixins (`sessions.py`, `tasks.py`, …) + `migrations/` |
| `cli.py` · `config.py` · `_env.py` · `bootstrap.py` | entry points + config |

`web/src/` = React UI · `tests/` = pytest suite · `docs/` = deep docs.

## Conventions & gotchas

- **Python 3.13 is the real floor** despite `requires-python>=3.12` — `memu-py==1.4.0`
  needs 3.13. That pin is intentional: nerve monkey-patches memU internals
  (`memu_bridge._patch_sqlite_bugs`). Don't bump casually.
- **`import nerve._env` must run before numpy loads** — it caps BLAS threads to
  avoid fork/atfork deadlocks. Kept as the first import in `nerve/cli.py`.
- **DB migrations**: add `nerve/db/migrations/vNNN_description.py` exporting
  `async def up(db)`. `SCHEMA_VERSION` is derived from the highest file number
  (`nerve/db/base.py`).
- **Config**: `config.example.yaml` is the reference; the live `config.yaml`
  deep-merges with gitignored `config.local.yaml` for secrets.
- Async-first; tests use **pytest-asyncio** (`tests/conftest.py`).
- Never commit `.env`, secrets, or `config.local.yaml`.

## Docs

`architecture` · `setup` · `config` · `api` · `sdk-sessions` · `memory` ·
`tasks` · `cron` · `plans` · `sources` · `web-ui` · `worker-guide` ·
`observability` · `migration` · `external-mcp` · `codex-sync` (all under `docs/`).
