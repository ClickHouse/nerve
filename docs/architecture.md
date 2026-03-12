# Architecture

## Overview

Nerve is a single-process Python application that serves as a personal AI assistant. All components run within one process using async Python (asyncio).

```
┌─────────────────────────────────────────────────────┐
│                    nerve (Python)                     │
│                                                       │
│  ┌─────────┐  ┌──────────┐  ┌────────┐  ┌────────┐  │
│  │ Gateway  │  │  Agent   │  │Sources │  │  Cron  │  │
│  │ (FastAPI)│──│  Engine  │  │ Layer  │  │ Service│  │
│  │ WS + HTTP│  │ (Claude) │  │(cursor)│  │        │  │
│  └────┬─────┘  └────┬─────┘  └────┬───┘  └───┬────┘  │
│       │              │             │           │       │
│  ┌────┴──────────────┴─────────────┴───────────┴──┐   │
│  │              SQLite + File Store                │   │
│  │  sessions, tasks, sync state │ markdown memory  │   │
│  └────────────────────────────────────────────────┘   │
│                                                       │
│  Interfaces:  Telegram Bot  │  React Web UI           │
└─────────────────────────────────────────────────────┘
```

## Components

### Gateway (`nerve/gateway/`)
FastAPI application serving:
- REST API for sessions, tasks, memory files, diagnostics
- WebSocket endpoint for real-time chat streaming
- Static file serving for the built React web UI
- JWT authentication

### Agent Engine (`nerve/agent/`)
Claude Agent SDK wrapper providing:
- Agent configuration (model, tools, system prompt)
- Agentic loop execution (tool calls → results → continuation)
- **Interactive tool handler** — `can_use_tool` callback replaces `bypassPermissions`; pauses the agent mid-turn for tools that need user input (`AskUserQuestion`, `ExitPlanMode`, `EnterPlanMode`), auto-approves everything else. Answers routed via WebSocket `answer_interaction` messages. See `interactive.py`.
- **File snapshot hooks** — `PreToolUse` hook on `Edit|Write|NotebookEdit` captures original file content to `session_file_snapshots` table before the tool executes. Enables session-scoped unified diffs without git. See `engine.py::_build_snapshot_hooks()`.
- **Diff computation** — `gateway/diff.py` computes structured unified diffs (hunks with line numbers) using `difflib.unified_diff` from stored snapshots vs current file on disk.
- Streaming bridge to broadcast events to all interfaces (bounded buffers, max 10k events)
- **Session lifecycle management** — explicit states (created → active → idle → stopped → archived → error) with transitions logged to `session_events` table
- **Session resume** via SDK `--resume` flag (survives server restarts; `sdk_session_id` stored as a first-class DB column)
- **Session forking** — branch conversations via SDK `fork_session=True`, exposed through REST API and WebSocket
- **Session stop** — uses SDK `client.interrupt()` for clean stop, falls back to `task.cancel()`
- **Persistent channel mappings** — channel-to-session mapping stored in `channel_sessions` table (survives restarts)
- **Orphan recovery** — on startup, sessions marked `active` in DB but with no live client are transitioned to `idle` (resumable) or `stopped`
- **Automatic cleanup** — periodic task (every 6h) archives stale sessions (default 30 days) and enforces max session count (default 500)
- **Per-run cron sessions** — each cron run gets a unique session ID (`cron:{job_id}:{timestamp}`) to prevent unbounded message accumulation
- AI-generated session titles via lightweight Haiku API call
- Custom MCP tools (tasks, memory recall, conversation history, sync status, skills CRUD, notifications)

### Channels (`nerve/channels/`)
Abstract communication layer with three components:
- **BaseChannel** — abstract interface with capability declarations (`ChannelCapability` flags: `SEND_TEXT`, `STREAMING`, `MARKDOWN`, `INTERACTIVE`, `TYPING_INDICATOR`) and streaming protocol (`send_placeholder`, `edit_message`). Channels declare capabilities; the router checks them before calling optional methods.
- **ChannelRouter** — centralized session resolution, streaming adapter lifecycle, interactive tool routing, and cron output delivery. Replaces per-channel session management.
- **StreamAdapter** — translates `StreamBroadcaster` events into channel-appropriate output (edit-in-place for Telegram, accumulated send for simple channels). Created per inbound message.

Implementations:
- **Telegram** — python-telegram-bot v21+ with partial message streaming (edit-in-place, 1.5s rate limit), inline keyboard buttons for notification questions, `/reply` command for free-text answers
- **Web** — Passive channel using gateway WebSocket

Adding a new channel (Discord, WhatsApp, etc.) requires implementing ~5 methods and zero session/routing logic.

### Notifications (`nerve/notifications/`)
Async notification system for agent→user communication:
- **`notify` tool** — fire-and-forget notifications (status updates, alerts, reminders)
- **`ask_user` tool** — questions with predefined options (rendered as buttons) + free-text input. Supports blocking mode (`wait=true`) and async mode (answer injected as session message)
- **NotificationService** — centralized fanout to configurable channels (web + Telegram by default), answer routing, periodic expiry
- **Multi-channel delivery** — web UI via `__global__` WebSocket broadcast channel, Telegram via direct bot API with inline keyboard buttons for questions
- **Answer routing** — answers from any channel (web UI, Telegram inline button, `/reply` command) are persisted and either unblock a waiting tool or injected as a user message into the originating session
- **Web UI** — `/notifications` page with status/type filters, inline answer buttons, dismiss, dismiss-all; real-time toast overlay for new notifications; NavRail badge for pending count

### Cron Service (`nerve/cron/`)
APScheduler-based job scheduler:
- Crontab and interval triggers
- Isolated sessions per job
- Persistent logging

### Sources (`nerve/sources/`)
Cursor-based data ingestion with agent processing:
- Pull-based source adapters (Telegram/Telethon, Gmail/gog CLI, GitHub/gh CLI)
- Records routed through processors: `agent` (LLM review), `memorize` (direct memU), `notify` (channel forward)
- Opaque cursor per source in SQLite — advances only after successful processing
- Auto-registered as APScheduler jobs alongside cron jobs
- See [sources.md](sources.md) for details

### Memory (`nerve/memory/`)
Dual-layer memory:
- **File-based** — MEMORY.md, identity files (curated source of truth, loaded into system prompt)
- **memU** — Semantic index over conversations and files (SQLite-persisted at `~/.nerve/memu.sqlite`)
- **Knowledge quality filtering** — Custom extraction prompt + post-extraction Haiku filter + semantic deduplication prevent generic CS/DevOps facts from polluting memory
- **Session rotation** — Main session rotates daily; conversations are indexed into memU on close
- **Session resume** — SDK session IDs stored as dedicated DB columns; sessions resume with full context via `--resume` flag
- **Session forking** — Fork conversations from any point; new session branches via SDK `fork_session=True`
- **Crash recovery** — On startup, sessions marked `active` in DB with no live client are recovered: those with `sdk_session_id` become `idle` (resumable), others become `stopped`

### Skills (`nerve/skills/`)
Filesystem-based skill system (Claude SDK compatible):
- Skills stored as `workspace/skills/<name>/SKILL.md` with YAML frontmatter
- Optional `references/`, `scripts/`, `assets/` subdirectories
- SQLite index for metadata + usage statistics (`skills`, `skill_usage` tables)
- Progressive disclosure: name+description in system prompt, full content loaded on demand via `skill_get` tool
- Agent can create/update skills dynamically via `skill_create`/`skill_update` MCP tools
- Automated extraction: `skill-extractor` cron identifies repeated workflows and proposes new skills via task+plan system
- Automated revision: `skill-reviser` cron reviews existing skills for accuracy, completeness, and quality
- Plan approval handler creates/updates skills directly when approving skill-extractor/skill-reviser proposals

### Tasks (`nerve/tasks/`)
Markdown + SQLite task system:
- Task files in `workspace/memory/tasks/`
- SQLite index for queries
- Escalation reminders (soft → medium → urgent)

## Data Flow

### User Message (Telegram)
1. User sends message via Telegram
2. `TelegramChannel` receives it, resolves active session
3. `AgentEngine.run()` is called with the message
4. System prompt is built (SOUL.md + IDENTITY.md + memories)
5. Claude Agent SDK processes the message with tools
6. Streaming events broadcast to Telegram (edit-in-place) and any WebSocket clients
7. Final response stored in SQLite

### User Message (Web UI)
1. User sends message via WebSocket
2. Gateway's WebSocket handler receives it
3. `AgentEngine.run()` called in background task
4. Streaming events sent back via WebSocket
5. Client renders tokens in real-time

### Cron Job
1. APScheduler triggers at schedule
2. Isolated per-run session created (`cron:{job_id}:{timestamp}`)
3. Agent runs with job prompt and cron model
4. Agent uses `notify`/`ask_user` tools to communicate with user
5. Run logged in `cron_logs` table

## Database Schema

SQLite with WAL mode (schema version 14):
- `sessions` — Session metadata with lifecycle columns (`status`, `sdk_session_id`, `connected_at`, `parent_session_id`, `forked_from_message`, `last_activity_at`, `archived_at`, `message_count`, `total_cost_usd`)
- `messages` — Conversation messages with tool call data and ordered `blocks` JSON column (preserves interleaving of text/thinking/tool_call blocks across page reloads)
- `session_events` — Append-only lifecycle audit log (created, started, idle, stopped, archived, error)
- `channel_sessions` — Persistent channel-to-session mapping (survives restarts)
- `session_file_snapshots` — Pre-modification file content captured via `PreToolUse` hook for session-scoped diff computation. Keyed by `(session_id, file_path)`, first-touch only. Cleaned up on session delete.
- `tasks` — Task index (mirrors markdown files)
- `sync_cursors` — Opaque cursor positions per source
- `source_run_log` — Per-source run diagnostics (records fetched/processed, errors)
- `cron_logs` — Job execution history (includes source runs as `source:<name>`)
- `memu_audit_log` — memU operation audit trail
- `skills` — Skill registry (id, name, description, version, enabled, metadata)
- `skill_usage` — Skill invocation tracking (skill_id, session_id, invoked_by, duration, success/error)
- `notifications` — Async notifications and questions (id, session_id, type, title, body, priority, status, options, answer, delivery tracking, expiry)

memU SQLite (`~/.nerve/memu.sqlite`):
- `memu_resources` — Indexed source files/conversations
- `memu_memory_items` — Extracted facts with embeddings
- `memu_memory_categories` — Topic categories with rolling summaries
- `memu_category_items` — Category-item links

## Security

- JWT authentication for all API/WebSocket access
- bcrypt password hashing
- Path traversal prevention on file operations
- Self-signed HTTPS (mkcert)
- Single-user system — no multi-tenancy
