<p align="center">
  <h1 align="center">Nerve</h1>
  <p align="center">
    <strong>A home for your agents.</strong>
    <br />
    Self-hosted AI agent runtime — personal assistants, autonomous workers, and everything in between.
  </p>
  <p align="center">
    <a href="docs/setup.md">Setup Guide</a> ·
    <a href="docs/architecture.md">Architecture</a> ·
    <a href="docs/config.md">Configuration</a> ·
    <a href="docs/worker-guide.md">Worker Guide</a> ·
    <a href="docs/api.md">API Reference</a>
  </p>
</p>

---

Nerve is a self-hosted runtime for AI agents, built around the [Claude Agent SDK](https://github.com/anthropics/claude-agent-sdk). It gives agents everything they need to be useful long-term: persistent memory, scheduled execution, task management, learnable skills, and channels to reach you through — web UI, Telegram, or autonomous cron jobs.

Ship a **personal assistant** that develops a personality, remembers your preferences, and manages your inbox. Or deploy a **worker agent** that monitors your CI, reviews PRs, and fixes flaky tests — all plan-driven with human approval. Same engine, different mission.

## Two Modes

### 🧑 Personal Mode

A full-featured assistant for one human. Syncs your email, remembers your preferences, develops personality over time. Comes with a web UI, Telegram bot, memory, cron jobs, and notifications out of the box.

> *"You're not a chatbot. You're becoming someone."* — from the Personal SOUL template

- Workspace files define personality (`SOUL.md`), identity (`IDENTITY.md`), and user context (`USER.md`)
- Memory categories are life-oriented: relationships, finances, travel, health, work
- Default crons: inbox processor, task planner, memory maintenance
- The agent develops opinions, follows up on past conversations, and gets better the longer you use it

### 🔧 Worker Mode

A task-focused autonomous agent for teams or programmatic deployment. Monitors something, proposes fixes, implements after approval. Plan-driven with a full audit trail.

> *"You're a specialist, not a script."* — from the Worker SOUL template

- **Self-configuring**: give it a plain-English task description and it researches, writes its own `TASK.md`, creates skills, sets up cron jobs, and starts working
- Memory categories are operational: patterns, procedures, decisions, approvals, infrastructure
- No `IDENTITY.md` or `USER.md` — workers execute a mission, not build a relationship
- Built for reliability: plan before acting, verify after executing, log everything

```bash
# Spin up a worker that monitors CI and fixes flaky tests
nerve init --mode worker
# → Wizard asks for a task description
# → Agent onboards itself on first boot
```

## Why Nerve?

| | |
|---|---|
| 🧠 **Memory that persists** | Dual-layer memory: curated hot memory loaded every session + semantic search over your entire history. Agents remember context, preferences, and lessons learned — permanently. |
| 🔧 **Self-evolving skills** | Agents watch for repeated workflows and propose reusable skills automatically. Skills are plain Markdown — agents read, create, and improve their own procedures over time. |
| 📋 **Autonomous task planning** | A background planner explores open tasks, researches codebases, and proposes implementation plans — without being asked. You review and approve; the agent executes. |
| 📡 **Multi-source awareness** | Gmail, GitHub, and Telegram flow into a unified inbox with Kafka-style cursors. Multiple consumers process the same data independently — inbox triage, digest generation, task extraction. |
| ⚡ **Single process, zero ops** | No Docker required, no message queues, no service mesh. FastAPI + uvicorn + asyncio. Runs on a Raspberry Pi. |
| 🔑 **No API key required** | Optional OAuth proxy routes through your Claude Max/Pro subscription. Zero API costs. |
| 🔄 **Session persistence** | Sessions survive server restarts. Fork conversations, resume from any point, run up to 4 concurrent agent sessions. |
| 🛡️ **Human-in-the-loop** | Agents can pause mid-turn to ask questions via any channel. Plans require approval before execution. Destructive actions need explicit consent. |

## Channels

Every channel shares the same agent engine, memory, and tools.

### 💬 Web UI

Full React + Vite + TailwindCSS frontend served by FastAPI. Real-time streaming over WebSocket.

- Chat with session history, branching, and resume
- Inline diff viewer for file changes (no git required)
- Task manager, memory browser, skill editor
- Source inbox, notification center, cron logs
- Plan review with approval/decline/revision workflow
- JWT auth, self-signed HTTPS via mkcert

### 🤖 Telegram Bot

Powered by `python-telegram-bot` v21+.

- Streaming responses via edit-in-place
- Inline keyboard buttons for interactive tool questions
- `/reply` command for free-text answers
- Configurable DM policy (`open` or `pairing`)

### ⏰ Cron Jobs

Scheduled AI sessions via APScheduler. Three session modes:

| Mode | Behavior |
|------|----------|
| **Isolated** | Fresh session per run. No prior context. Best for briefings, cleanup, reports. |
| **Persistent** | Accumulates context across runs. Optional rotation to manage token usage. Reminder mode for lightweight follow-ups. |
| **Main** | Runs inside the user's primary conversation — full context access. |

Built-in crons (personal mode): `skill-extractor` (12h), `skill-reviser` (weekly), `inbox-processor` (15min), `task-planner` (4h). Worker mode ships with `skill-reviser`, `skill-extractor`, and `task-planner` by default; additional crons are configured during onboarding.

### 📡 Source Sync

Cursor-based data ingestion pipeline. Each source runs as an independent APScheduler job.

- **Telegram** — Telethon user-account API (`updates.getDifference`)
- **Gmail** — `gog` CLI with 2-step fetch and LLM condensation
- **GitHub Notifications** — `gh api` enriched with PR/issue content
- **GitHub Events** — Pushes, PRs, reviews, comments

Multiple independent consumers read the same inbox at their own pace. Content is prefixed with untrusted-data warnings to prevent prompt injection.

## Core Systems

### 🧠 Memory

Two layers, one seamless experience.

**L1 — Hot Memory (MEMORY.md)**
Curated facts injected into every system prompt. Active projects, current deadlines, operational lessons. Tagged with dates, automatically evicted when stale.

**L2 — Deep Memory (memU)**
Semantic search over everything — conversations, facts, preferences, events. SQLite-persisted with `text-embedding-3-small` embeddings.

- Four memory types: `profile`, `event`, `knowledge`, `behavior`
- Automatic conversation indexing on session close
- Pre-recall: relevant memories injected into system prompt when sessions start
- 3-level quality filtering prevents generic facts from polluting memory
- Semantic deduplication (cosine similarity 0.85 threshold)
- Full audit log of all mutations

Memory categories adapt to mode — personal agents track relationships, health, and finances; workers track patterns, procedures, and approvals.

### 📋 Tasks

Markdown files backed by SQLite FTS5 full-text search.

- Statuses: `pending` → `in_progress` → `done` / `deferred`
- Duplicate detection: exact URL match + fuzzy FTS fallback
- Escalation reminders: deadline → +30min follow-up → +2h URGENT (respects quiet hours)
- Full markdown editor in the web UI

### 🔧 Skills

The agent's procedural knowledge. Pure Markdown files that agents read, create, and update themselves.

```
workspace/skills/
└── my-skill/
    ├── SKILL.md          # Instructions (YAML frontmatter + markdown)
    ├── references/       # On-demand documentation
    ├── scripts/          # Executable code (30s timeout)
    └── assets/           # Supporting files
```

- Progressive disclosure: only name + description in system prompt; full content loaded on demand
- `skill-extractor` cron proposes new skills from repeated workflows
- `skill-reviser` cron reviews existing skills for accuracy
- Usage statistics tracked per skill

### 📐 Plans

Proactive task planning with human-in-the-loop approval.

1. Planner cron picks open tasks autonomously
2. Agent explores codebase, researches approaches, proposes implementation plan
3. User reviews → approves / declines / requests revision
4. Approved plans spawn implementation sessions automatically

Revisions happen in the same persistent planner session — full context preserved.

### 🔔 Notifications

Async communication between agent and human, delivered to both web UI and Telegram.

- **`notify`** — Fire-and-forget alerts (status updates, completions, reminders)
- **`ask_user`** — Questions with predefined options, rendered as buttons
- Priority levels: `urgent`, `high`, `normal`, `low`

## Architecture

```
nerve (single Python process)
│
├── Gateway (FastAPI)
│   ├── REST API — sessions, tasks, memory, plans, sources, diagnostics
│   ├── WebSocket — real-time streaming, answer routing
│   └── Static files — serves the React web UI
│
├── Agent Engine (Claude Agent SDK)
│   ├── ~30 custom MCP tools (tasks, memory, skills, sources, notifications)
│   ├── Interactive mid-turn pausing (AskUserQuestion, plan approval)
│   ├── File snapshot hooks → session-scoped diffs without git
│   ├── Session lifecycle: created → active → idle → stopped → archived
│   └── Up to 4 concurrent sessions (configurable)
│
├── Channels
│   ├── Web — passive WebSocket channel
│   └── Telegram — bot with streaming + inline keyboards
│
├── Cron (APScheduler)
│   ├── AI jobs (isolated / persistent / main session modes)
│   └── Source sync jobs (Telegram, Gmail, GitHub)
│
├── Memory
│   ├── File-based hot memory (L1)
│   └── memU semantic index (L2, SQLite + embeddings)
│
├── Tasks — Markdown + SQLite FTS5
├── Skills — Filesystem + DB index + usage tracking
├── Plans — SQLite with approval workflow
├── Notifications — Multi-channel delivery
└── Proxy (optional) — Claude OAuth, no API key needed
```

## Quick Start

```bash
# Install
git clone https://github.com/pufitdev/nerve.git
cd nerve
uv venv && source .venv/bin/activate
uv pip install -e .

# Interactive setup — creates config, workspace, and cron jobs
nerve init                    # Personal mode (default)
nerve init --mode worker      # Worker mode

# Start
nerve start -f

# Verify
nerve doctor
```

**No API key?** Use your Claude subscription instead:
```bash
NERVE_USE_PROXY=1 nerve init --non-interactive
```

**Docker:**
```bash
nerve init           # Generates Dockerfile + docker-compose.yml
nerve start          # All CLI commands proxy to docker compose
```

## Configuration

Two config files:
- `config.yaml` — Template settings (committed)
- `config.local.yaml` — Secrets and overrides (gitignored)

See [docs/config.md](docs/config.md) for all options.

## Requirements

- Python 3.12+
- Node.js 18+ (for web UI build)
- Claude Code CLI (bundled with `claude-agent-sdk`)
- Anthropic API key **or** Claude subscription via [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) proxy
- Optional: OpenAI API key (for memU embeddings), Telegram bot token, `gog` CLI, `gh` CLI

## Documentation

| Doc | Description |
|-----|-------------|
| [Architecture](docs/architecture.md) | System overview, modes, deployment, data flow |
| [Setup](docs/setup.md) | Installation, config, Docker, HTTPS, systemd |
| [Config](docs/config.md) | All config options with descriptions and defaults |
| [Worker Guide](docs/worker-guide.md) | Worker mode setup, onboarding, plan-approve loop |
| [API Reference](docs/api.md) | REST API and WebSocket protocol |
| [SDK Sessions](docs/sdk-sessions.md) | Session lifecycle, resume, forking |
| [Sources](docs/sources.md) | Sync engine, cursors, consumer model |
| [Tasks](docs/tasks.md) | Task file format, escalation rules |
| [Memory](docs/memory.md) | Memory file conventions, memU integration |
| [Cron](docs/cron.md) | Job format, session modes, built-in crons |
| [Plans](docs/plans.md) | Plan proposal, approval, and implementation workflow |
| [Web UI](docs/web-ui.md) | Frontend architecture, building, development |

## Testing

```bash
pytest tests/ -v
```

## License

MIT
