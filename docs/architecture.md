# Architecture

## Overview

Nerve is a single-process Python application that serves as a personal AI assistant. All components run within one process using async Python (asyncio).

```
┌──────────────────────────────────────────────────────────┐
│                      nerve (Python)                       │
│                                                           │
│  ┌─────────┐  ┌──────────┐  ┌────────┐  ┌────────┐      │
│  │ Gateway  │  │  Agent   │  │Sources │  │  Cron  │      │
│  │ (FastAPI)│──│  Engine  │  │ Layer  │  │ Service│      │
│  │ WS + HTTP│  │ (Claude) │  │(cursor)│  │        │      │
│  └────┬─────┘  └────┬─────┘  └────┬───┘  └───┬────┘      │
│       │              │             │           │          │
│  ┌────┴──────────────┴─────────────┴───────────┴──┐      │
│  │              SQLite + File Store                │      │
│  │  sessions, tasks, sync state │ markdown memory  │      │
│  └────────────────────────────────────────────────┘      │
│                                                           │
│  Interfaces:  Telegram Bot  │  React Web UI              │
│                                                           │
│  Optional:  CLIProxyAPI (OAuth proxy, no API key needed) │
└──────────────────────────────────────────────────────────┘
```

## Modes

Nerve supports two modes, chosen once — on the Mode step of the browser setup, in `nerve init --terminal`, or by `NERVE_MODE` for an unattended install. The mode is not a config key: it is read back from the workspace templates (`SOUL.md`'s first line, then the other files), and until it is chosen the gateway runs with no templates and the setup page asks for it.

**Personal** — a full-featured assistant for one human. Ships with sync sources (Telegram, Gmail, GitHub), life-oriented memory categories, identity files (`IDENTITY.md`, `USER.md`), and the inbox-processor cron. The agent develops personality over time.

**Worker** — a task-focused autonomous agent. Ships with operational memory categories, a `TASK.md` mission brief, no sync sources, and a self-configuring onboarding flow. Operates in a plan-approve-execute loop.

Mode affects which workspace templates are generated, which cron jobs are enabled, and which memory categories are seeded — but the underlying engine is identical. Both modes run the same agent, tools, and infrastructure.

How to operate each mode well is covered in [Personal Best Practices](personal-best-practices.md) (brain files, memory tiers, approval classes) and [Worker Best Practices](worker-best-practices.md) (`TASK.md` anatomy, plan-approve-execute, the task state machine).

## Deployment

Two deployment options:

**Server (bare metal)** — Nerve runs directly as a Python process. Uses `nerve start` for daemon management, `nerve stop` / `nerve restart` for lifecycle.

**Docker** — `nerve init` generates `Dockerfile`, `docker-compose.yml`, and `docker-entrypoint.sh`, carrying the chosen port and bind address. The code is bind-mounted (editable), and the state directory and workspace are bind-mounted from the host. CLI commands (`nerve start`, `nerve logs`, etc.) automatically proxy to `docker compose` when `deployment: docker` is set in config. A Claude Code credential found on the host (macOS Keychain, env vars, credential files) is forwarded into `config.local.yaml` for the container — `~/.claude` is NOT mounted. A restart asked for from the browser makes the container exit; Compose (`restart: unless-stopped`) brings it back.

## Components

### Gateway (`nerve/gateway/`)
FastAPI application serving:
- REST API for sessions, tasks, memory files, diagnostics
- The setup API (`/api/setup/*`): the first-account claim (local mode, guarded by the setup token) and the setup checklist, whose step status is derived from configuration, workspace files and the database (`nerve/setup_checklist.py`) and whose writes go through the same writer as `nerve init` (`nerve/setup_writer.py`); plus `POST /api/system/restart`, which shares its detached restart helper with `nerve restart` (`nerve/daemon.py`)
- WebSocket endpoint for real-time chat streaming
- Static file serving for the built React web UI — or, when `web/dist` is missing, a plain page at `/` and `/setup` saying how to build it
- `GET /health` with a per-process boot `generation`, so a client can tell the new process from the one shutting down
- JWT authentication

### Agent Engine (`nerve/agent/`)
Claude Agent SDK wrapper providing:
- Agent configuration (model, tools, system prompt)
- Agentic loop execution (tool calls → results → continuation)
- **Interactive tool handler** — `can_use_tool` callback replaces `bypassPermissions`; pauses the agent mid-turn for tools that need user input (`AskUserQuestion`, `ExitPlanMode`; `EnterPlanMode` is allowed without a pause), auto-approves everything else. Answers routed via WebSocket `answer_interaction` messages. See `interactive.py`.
- **File snapshot hooks** — `PreToolUse` hook on `Edit|Write|NotebookEdit` captures original file content to `session_file_snapshots` table before the tool executes. Enables session-scoped unified diffs without git. See `engine.py::_build_snapshot_hooks()`.
- **Diff computation** — `gateway/diff.py` computes structured unified diffs (hunks with line numbers) using `difflib.unified_diff` from stored snapshots vs current file on disk.
- Streaming bridge to broadcast events to all interfaces (bounded buffers, max 10k events)
- **Session lifecycle management** — explicit states (created → active → idle → stopped → archived → error) with transitions logged to `session_events` table
- **Session resume** via SDK `--resume` flag (survives server restarts; `sdk_session_id` stored as a first-class DB column)
- **Session forking** — branch conversations via SDK `fork_session=True`, exposed through REST API and WebSocket
- **Session stop** — scoped: Stop interrupts only the orchestrator's turn (the task that holds the session's run lock) and spares background agents; one agent stops through the runtime's per-task stop; stop-all and force are separate actions. A turn is cancelled only when no agent would die with it, or by force
- **Turn queue** — runs that wait for a session's run lock are listed (`queue_update`) and can be cancelled by their sender; `client_msg_id` makes a resend run once
- **Persistent channel mappings** — channel-to-session mapping stored in `channel_sessions` table (survives restarts)
- **Orphan recovery** — on startup, sessions marked `active` in DB but with no live client are transitioned to `idle` (resumable) or `stopped`
- **Automatic cleanup** — periodic task (every 6h) archives stale sessions (default 30 days) and enforces max session count (default 500)
- **Per-run cron sessions**: each cron run gets a unique session ID (`cron:{job_id}:{run_id}`, the start time and a random suffix) to prevent unbounded message accumulation
- AI-generated session titles via lightweight Haiku API call
- Custom MCP tools (tasks, memory recall, conversation history, sync status, skills CRUD, notifications)
- API calls routed through configurable base URL (direct Anthropic API or local CLIProxyAPI proxy)

**Sub-agents and background tasks** (`nerve/agent/agents/`). The runtime
streams a sub-agent's work in the parent's stream, tagged with
`parent_tool_use_id`. The engine routes every such event (and every
sub-agent model call's usage) to the **agent registry** instead of the
current turn, so a sub-agent's work never opens, extends or persists into an
orchestrator turn, and background agents alone never make a session look
running. The registry also takes the spawning tool calls (`Agent`, `Task`,
`Workflow`, `Monitor`, background `Bash`) and the CLI's `task_*` and
`background_tasks_changed` frames, and keeps one record per agent: status,
brief, progress, deduplicated usage and an estimated cost, and a per-agent
transcript with a gap-free `seq`. Records and transcript items are written
behind to `session_agents` / `session_agent_events` (batched in one
transaction per session about once a second; status changes are written at
once) and broadcast through a single pump task
(`agent_update` to the session, `agent_event` only to sockets that follow
that agent), so ingestion never waits on the database or a socket while it
holds the run lock. Every teardown of a session's runtime goes through
`SessionManager.remove_client(reason=)`, which marks its live agents
`interrupted` with the reason; shutdown and boot reconcile what bypasses it.
See [agents.md](agents.md).

### Channels (`nerve/channels/`)
Abstract communication layer with three components:
- **BaseChannel** — abstract interface with capability declarations (`ChannelCapability` flags: `SEND_TEXT`, `STREAMING`, `MARKDOWN`, `INTERACTIVE`, `TYPING_INDICATOR`) and streaming protocol (`send_placeholder`, `edit_message`). Channels declare capabilities; the router checks them before calling optional methods.
- **ChannelRouter** — centralized session resolution, streaming adapter lifecycle, interactive tool routing, and cron output delivery. Replaces per-channel session management.
- **StreamAdapter** — translates `StreamBroadcaster` events into channel-appropriate output (edit-in-place for Telegram, accumulated send for simple channels). Created per inbound message.

- **Access matching** (`access.py`) — transport-neutral identity aliases and fail-closed allow/deny pattern matching. Channels compose these primitives into their own policy before creating an `InboundMessage`.
- **Archives** (`archives.py`) — bounded one-level ZIP unpacking shared by Telegram and Slack. The download cap is on compressed bytes, so entry count, per-entry and aggregate uncompressed size, and compression ratio are all checked against the archive directory before an entry is read.

Implementations:
- **Telegram** — python-telegram-bot v21+ with partial message streaming (edit-in-place, 1.5s rate limit), inline keyboard buttons for notification questions, `/reply` command for free-text answers
- **Slack** — slack_sdk Socket Mode (outbound WebSocket, no public URL) with partial streaming via `chat.update`, Block Kit buttons, `/nerve` slash command, and per-thread sessions. `SlackAccessPolicy` (`slack_access.py`) composes user, channel, and DM guardrails; in channels the bot answers only on mention or in a thread it already owns.
- **Hosted** (`hosted/`): provider traffic that a shared channel gateway holds (`slack.mode: hosted`). Nerve has no provider tokens. Each gateway replica opens a WebSocket to `/_internal/channel/v1/stream`, authenticated with a token that the gateway signs (ES256, issuer `nerve-gateway`, audience `nerve-channel`, subject naming this exact tenant and agent, at most 300 seconds of lifetime). One inbox reader pulls admitted events over the preferred stream: read, process, acknowledge, and read again until a page is empty. `HostedChannel` registers under the provider's name, so the router and tools see `slack` as with Socket Mode, and uses the same session keys. The gateway decides which events invoke the agent: a direct message, a mention of the agent, a thread reply, a reaction, an interaction, or a command. A mention of the agent carries the `self` flag, so intake needs no capabilities. Outbound messages are contract operations: `OperationRunner` (`hosted/operations.py`) sends each on a stream that advertised it for the connection, within the stream and connection in-flight limits, and never sends a side-effecting operation again after its result is lost. File uploads and attachment reads use transfers on the stream of their operation; a read is sent only while its length fits Nerve's stream memory limit. Notification cards carry buttons, and a press arrives as an `interaction` event that answers the notification. `hosted/contract/` holds the version 1 channel contract frames that Nerve reads and sends. The gateway checks the contract; Nerve checks only the members and values that its code uses, and its size limits.
- **Web** — Passive channel using gateway WebSocket

Adding a new channel (Discord, WhatsApp, etc.) requires implementing ~5 methods and zero session/routing logic.

### Notifications (`nerve/notifications/`)
Async notification system for agent→user communication:
- **`notify` tool** — fire-and-forget notifications (status updates, alerts, reminders)
- **`ask_user` tool** — questions with predefined options (rendered as buttons) + free-text input. Supports blocking mode (`wait=true`) and async mode (answer injected as session message)
- **NotificationService** — centralized fanout to configurable channels (web + Telegram by default), answer routing, periodic expiry
- **Multi-channel delivery** — web UI via `__global__` WebSocket broadcast channel, Telegram via direct bot API with inline keyboard buttons, Slack via Block Kit action buttons
- **Answer routing**: answers from any channel (web UI, Telegram inline button, Slack button, `/reply` command) are persisted and either unblock a waiting tool or injected as a user message into the originating session. A web answer records the person in `answered_by_actor_id`, and the relayed message is written by that person; the agent's work in that turn has the session's cause. A channel answer keeps the provider user ID in metadata and records no actor. An approval dispatcher (`nerve/notifications/handlers.py`) receives the person who answered as its `actor` keyword argument, `None` for a channel answer
  A dispatcher that asks for a snooze can return an `after_snooze` callback; the service awaits it with the result of its snooze write (False when the card was no longer pending)
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
- **memU** — Semantic index over conversations and files (SQLite-persisted at `~/.nerve/memu.sqlite`); with `memory.backend: loom`, a Loom memory service reached through the MCP gateway instead ([memory.md](memory.md#loom-backend))
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

### House of Agents (retired)
The houseofagents multi-agent runtime was retired in favor of **workflow runs** (`nerve/workflows/`, the `workflow_run_*` tools) — budget-capped multi-agent runs with journals, live spend tracking, and run-scoped kill. See [workflow-runs.md](workflow-runs.md). The `hoa_*` tool names remain registered as deprecation stubs (gated by `houseofagents.enabled`) that point callers at `workflow_run_start`.

### Proxy (`nerve/proxy/`)
Optional local HTTP proxy that routes Anthropic API calls through Claude Code's OAuth authentication:
- Eliminates the need for a direct Anthropic API key
- Uses [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) binary (auto-downloaded on first start)
- OAuth tokens refreshed automatically
- Configured via `proxy.*` settings in config — see [config.md](config.md#proxy-cliproxyapi)

### Platform activity (`nerve/platform/`)
For a machine that a platform parks while it is idle. When the platform's
local activity socket (a Unix socket, `platform.activity_socket`) exists at
startup, `ActivityReporter` keeps it current: running turns, open web client
connections, and the next scheduled task the cron service forecasts. These go
out on every change (at most one post every 250 ms) and on a fixed interval.
Background work that is not a turn (memorization, workspace sync,
backups, background agents) holds keep-awake leases through `keep_awake()`.
Without the socket nothing is sent. See
[platform-activity.md](platform-activity.md).

### Tasks (`nerve/tasks/`)
Markdown + SQLite task system:
- Task files in `workspace/memory/tasks/`
- SQLite index for queries
- Escalation reminders (soft → medium → urgent)

### Agent Backends

The engine is backend-agnostic: per-session clients implement the
`AgentBackend`/`AgentClient` protocols (`nerve/agent/backends/`) and
translate their native streams into normalized events
(`backends/events.py`) — the only vocabulary the engine consumes.
`backends/claude.py` wraps the Claude Agent SDK (options, hooks,
`can_use_tool`, idle-stream drains, hardened disconnect);
`backends/codex/` speaks JSON-RPC to a per-session `codex app-server`
subprocess (threads/turns, approval server-requests routed to the
interaction hub, per-turn pricing). Backend choice is sticky per session
(`sessions.backend`); capabilities (cumulative vs per-turn cost,
idle-stream support, cache-TTL policy) gate engine behavior — never
`isinstance`. Details: `docs/plans/codex-backend.md`.

## Data Flow

### User Message (Telegram)
1. User sends message via Telegram
2. `TelegramChannel` receives it, resolves active session
3. `AgentEngine.run()` is called with the message
4. System prompt is built (SOUL.md + IDENTITY.md + MEMORY.md — identical across sessions); the session id and pre-recalled memories are prepended to the first message
5. Claude Agent SDK processes the message with tools
6. Streaming events broadcast to Telegram (edit-in-place) and any WebSocket clients
7. Final response stored in SQLite

The system prompt in step 4 also takes `platform.md` from the config directory
(next to `config.yaml`) when that file exists and is not empty. In it, whoever
runs the instance, an operator or a hosting platform, tells the agent about the
instance. Nerve never writes the file. Its content goes in as written, after
`TOOLS.md` and before `MEMORY.md`, and at most 16 KiB of it is used: a longer
file is cut there and ends with `[platform notes truncated at 16 KiB]`. The
file is read whenever a session's prompt is built, so a change applies to new
sessions.

### User Message (Web UI)
1. User sends message via WebSocket
2. Gateway's WebSocket handler receives it — the connection's actor was resolved once, at accept
3. `AgentEngine.run()` called in background task, carrying that actor
4. The user message is stored under it; the assistant's reply is stored unattributed
5. Streaming events sent back via WebSocket
6. Client renders tokens in real-time

### Cron Job
1. APScheduler triggers at schedule
2. Isolated per-run session created (`cron:{job_id}:{run_id}`)
3. Agent runs with job prompt and cron model, acting as the system principal — the session and the prompt are attributed to it, not to whoever wrote the schedule
4. Agent uses `notify`/`ask_user` tools to communicate with user
5. Run logged in `cron_logs` table

## Database

### Package Structure

The database layer lives in `nerve/db/` as a Python package with domain-specific modules:

```
nerve/db/
├── __init__.py        # Re-exports Database, init_db, close_db, SCHEMA_VERSION
├── base.py            # Database class (connection, _atomic, FTS check)
├── migrations/        # File-based migration system
│   ├── runner.py      # Discovers and applies vNNN_*.py files in order
│   ├── v001_initial.py
│   ├── ...
│   ├── v016_mcp_servers.py
│   └── fork/          # A fork's own migrations; empty upstream (see Migrations)
├── sessions.py        # SessionStore mixin
├── messages.py        # MessageStore mixin
├── tasks.py           # TaskStore mixin
├── plans.py           # PlanStore mixin
├── notifications.py   # NotificationStore mixin
├── sources.py         # SourceStore mixin (inbox, sync cursors, consumer cursors)
├── cron.py            # CronStore mixin
├── skills.py          # SkillStore mixin
├── mcp.py             # McpStore mixin
├── agents.py          # AgentStore mixin (session_agents, session_agent_events)
└── audit.py           # AuditStore mixin
```

The `Database` class in `base.py` inherits all domain mixins, preserving a single `Database` interface. All existing imports (`from nerve.db import Database`) work unchanged via `__init__.py` re-exports.

### Migrations

Migrations are individual Python files named `vNNN_description.py`. Each exports an `async def up(db)` function. They come in two tracks. Each track is numbered on its own and recorded in its own version table:

| Track | Files | Version table | Highest shipped |
|---|---|---|---|
| upstream | `nerve/db/migrations/vNNN_*.py` | `schema_version` | `SCHEMA_VERSION` |
| fork | `nerve/db/migrations/fork/vNNN_*.py` | `fork_schema_version`, created when the first fork migration runs | `FORK_SCHEMA_VERSION` (0 upstream) |

Every database open runs the runner: the gateway's start and every CLI command that uses the database. The runner applies the pending upstream migrations in version order, then the pending fork migrations. A migration is recorded in its track's table once `up()` returns. Nothing wraps a migration in a transaction (`executescript()` commits as it goes), so a migration that is interrupted runs again from the start on the next open. Write migrations to be idempotent (`CREATE TABLE IF NOT EXISTS`, check a column before adding it).

**Numbering.** Within a track, the runner skips every version at or below the database's current one. To add a migration upstream, create `nerve/db/migrations/vNNN_your_feature.py` numbered above the highest version on `main`. CI (`scripts/check_migration_numbers.py`) fails a PR whose new migration is not numbered that way, or that gives two migrations the same number. Upstream never ships a fork migration: `fork/` holds only its package files, and the same check fails on any migration there. A fork of Nerve adds its own migrations only under `fork/`, numbered from `v001` on their own (`nerve/db/migrations/fork/README.md`). A fork's number therefore never shadows an upstream migration that ships later, and the fork runs the check with `--fork`.

**Newer-schema guard.** Before anything is applied, each track's version in the database is compared with the highest migration this code ships for that track. If the database is ahead on either track, a newer Nerve or a fork migrated it, and the database is refused untouched. The gateway process (`nerve start --foreground`, which a supervisor runs) and every CLI command that opens the database print one line that names the track and both versions, then exit with status 65 (`NEWER_SCHEMA_EXIT_CODE` in `nerve/db/migrations/runner.py`). That status differs from any other failure, so a supervisor can stop restarting on it (systemd: `RestartPreventExitStatus=65`). The daemonizing `nerve start` does not pass it on: it checks the process it started once, a second later, and exits with status 1 if that process has exited by then. `NERVE_ALLOW_NEWER_SCHEMA=1` starts anyway, with a warning. See [Going back to an older Nerve](setup.md#going-back-to-an-older-nerve).

### Schema

SQLite with WAL mode (schema version 16):
- `sessions` — Session metadata with lifecycle columns (`status`, `sdk_session_id`, `connected_at`, `parent_session_id`, `forked_from_message`, `last_activity_at`, `archived_at`, `message_count`, `total_cost_usd`) and `created_by_actor_id` → `actor_refs`: who caused the session to exist
- `messages` — Conversation messages with tool call data and ordered `blocks` JSON column (preserves interleaving of text/thinking/tool_call blocks across page reloads), plus `actor_id` → `actor_refs`: whose input the row records. NULL means attribution was not recorded (legacy history, unidentified external people, and assistant/tool output). Nothing is backfilled — see [Accounts and identity](accounts.md)
- `session_events`: append-only lifecycle audit log (created, started, idle, stopped, archived, error, and the changes listed in [Session changes](accounts.md#session-changes)), with `actor_id` and `caused_by_actor_id` → `actor_refs`
- `session_deletions`: one row for each deleted session: last title, time, `actor_id` and `caused_by_actor_id`. Written in the transaction of the delete, which removes the session's events
- `channel_sessions` — Persistent channel-to-session mapping (survives restarts)
- `session_file_snapshots` — Pre-modification file content captured via `PreToolUse` hook for session-scoped diff computation. Keyed by `(session_id, file_path)`, first-touch only. Cleaned up on session delete.
- `session_agents` — One row per sub-agent or background task of a session: kind, status and end reason, brief, progress, usage and estimated cost, stop attribution (`stopped_by_actor_id`, `stopped_caused_by_actor_id` → `actor_refs`). Deleted with the session (see [agents.md](agents.md))
- `session_agent_events` — Each agent's transcript items (`agent_id`, gap-free `seq`, `kind`, JSON `payload`), capped per item; follows telemetry retention when it is enabled
- `tasks` — Task index (mirrors markdown files)
- `tasks_fts` — FTS5 full-text search index for tasks
- `sync_cursors` — Opaque cursor positions per source
- `source_run_log` — Per-source run diagnostics (records fetched/processed, errors)
- `source_messages` — Source inbox with TTL-based expiry
- `consumer_cursors` — Kafka-like consumer positions per source
- `cron_logs` — Job execution history (includes source runs as `source:<name>`)
- `memu_audit_log` — memU operation audit trail
- `plans` — Plans proposed by planner agent for async human review
- `skills` — Skill registry (id, name, description, version, enabled, metadata)
- `skill_usage` — Skill invocation tracking (skill_id, session_id, invoked_by, duration, success/error)
- `notifications` — Async notifications and questions (id, session_id, type, title, body, priority, status, options, answer, delivery tracking, expiry)
- `mcp_servers` — MCP server registry (config is source of truth, DB tracks metadata)
- `mcp_tool_usage` — MCP tool invocation tracking
- `actor_refs`: attribution identity: stable id, `kind` (`human`/`system`), display name (see [Accounts and identity](accounts.md))
- `accounts`: local login state, one per human actor: username, `credential_source` (`config`/`local`/`none`), credential, enabled
- `instance_secrets`: machine-local secrets that are state, not configuration (the generated JWT signing secret)

memU SQLite (`~/.nerve/memu.sqlite`):
- `memu_resources` — Indexed source files/conversations
- `memu_memory_items` — Extracted facts with embeddings
- `memu_memory_categories` — Topic categories with rolling summaries
- `memu_category_items` — Category-item links

## Startup Sequence

1. Load config (`settings.yaml` + `config.yaml` + `config.local.yaml` deep merge). The loaded object is remembered as the *startup configuration*: the setup page later compares it with what is on disk to say whether a restart is pending. The previous process's setup notes (restart debts, start-up errors) are retired
2. Run database migrations (the upstream track, then the fork track; see [Migrations](#migrations)). A database newer than the code stops the start before uvicorn runs, with exit status 65. Bootstrap the owner account and the setup token (local mode) or delete a leftover token (external mode)
3. Initialize memory system (memU SQLite, load categories). With no model credential yet the warm-up is skipped and recall answers empty; nothing calls out
4. Start CLIProxyAPI proxy if enabled (download binary if missing, authenticate, start subprocess)
5. Recover orphan sessions (active in DB but no live client → idle or stopped)
6. Start cron service (load system.yaml + jobs.yaml, register source runners, catch up missed jobs). Sources the installer left `enabled: false` are not scheduled. Then the activity reporter, which does nothing unless the platform's local activity socket exists ([platform-activity.md](platform-activity.md))
7. Start gateway (FastAPI + uvicorn, WebSocket, static files or the "web UI not built" page)
8. Start Telegram bot if configured. A token Telegram rejects does not stop the boot: the failure is logged with the token scrubbed, recorded for the setup page and `nerve doctor`, and the gateway runs without the bot
9. **Onboarding**: the gateway opens the onboarding when the `onboarding` row asks for it, and only with a model credential: the conversation, when setup finished before a restart, or the autonomous session of a worker that `nerve init` set up with a task description (`nerve/onboarding.py`; the setup page's Finish opens the conversation when no restart is needed). One-shot CLI commands never open it

The gateway boots and serves `/setup` with nothing configured — no mode, no provider — and stays idle rather than failing, so the browser setup can do its work against a running instance.

## Security

- JWT authentication for all API/WebSocket access. The signing secret is `auth.jwt_secret` or, when unset, one generated on first start and kept in `nerve.db`. There is no unauthenticated mode. The one unauthenticated write — the first-account claim — requires the setup token, which never appears in a URL, response, log, state file or browser storage; every other setup endpoint needs a signed-in person, and system or MCP tokens cannot drive setup
- Channel gateway streams (`/_internal/channel/v1/stream`) have their own authentication: a gateway-signed token in the `Authorization` header, verified against the public keys in a local key file (`channels.hosted.gateway_jwks_file`). Nerve fetches no keys over the network. The web session token never opens a stream, a stream token never reaches the web UI or the API, and an upgrade with an `Origin` header (every browser sends one) is refused
- bcrypt password hashing
- Path traversal prevention on file operations
- Self-signed HTTPS (mkcert)
- Local accounts with a stable actor identity per account (one owner account, created at first start; see [Accounts and identity](accounts.md)). No multi-tenancy
