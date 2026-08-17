# Web UI

## Overview

React + Vite + TailwindCSS frontend served by FastAPI as static files. Communicates via WebSocket for real-time streaming and REST API for CRUD operations.

## Architecture

```
web/src/
├── api/
│   ├── client.ts       # REST API client with JWT auth
│   └── websocket.ts    # WebSocket client with auto-reconnect
├── stores/
│   ├── chatStore.ts    # Chat/session state + thin WS dispatcher (Zustand)
│   ├── handlers/       # Domain-specific WebSocket message handlers
│   │   ├── streamingHandlers.ts  # thinking, token, tool_use, tool_result, done, stopped, error
│   │   ├── sessionHandlers.ts    # session lifecycle (updated/status/switched/forked/resumed/archived/running)
│   │   ├── panelHandlers.ts      # plan_update, subagent_start/complete, workflow_progress
│   │   ├── auxiliaryHandlers.ts  # interaction, file_changed, notifications, background_tasks
│   │   └── types.ts              # Shared Get/Set type aliases for handlers
│   ├── helpers/        # Stateless utility functions for chat state
│   │   ├── blockHelpers.ts       # Panel block append/update, auto-close timers
│   │   └── bufferReplay.ts       # Session reconnect replay, deriveStatus, extractTodos
│   ├── authStore.ts    # Auth state (Zustand)
│   ├── taskStore.ts    # Task list/detail state (Zustand)
│   └── skillsStore.ts  # Skills CRUD + usage stats (Zustand)
├── components/
│   ├── Auth/           # Login page
│   ├── Chat/           # Message list, input, session sidebar, diff viewer
│   │   ├── tools/      # Specialized tool call renderers
│   │   ├── FileChangesPanel.tsx  # Modified files list + detail navigation
│   │   └── DiffView.tsx          # GitHub PR-style unified diff renderer
│   ├── Tasks/          # Task list, search, detail editor
│   ├── Memory/         # File browser with markdown editor
│   ├── Memu/           # Semantic memory browser (categories, items, sources)
│   └── Diagnostics/    # System status dashboard
├── utils/
│   ├── dateGroups.ts       # Date grouping (Today/Yesterday/This Week/Older)
│   ├── extractResultText.ts # Extract text from MCP content blocks
│   ├── hydrateMessage.ts
│   └── toolSummary.ts
└── App.tsx             # Main layout
```

## Layout

```
┌──────────┬──────────────────────────┬──────────────────┐
│ Sidebar  │  Header [≡] [title]      │  Side Panel      │
│ (toggle) │  [status] [context bar]  │  [tab1] [tab2]   │
│          ├──────────────────────────┤  [header] [X]    │
│ Today    │                          │                  │
│  Chat 1  │  Message list            │  Live tool calls │
│  Chat 2  │  (streaming)             │  (same as chat)  │
│ Yesterday│                          │  ─────────────   │
│  Chat 3  │  ┌─ compact card ──────┐ │  Final result    │
│          │  │ 🔍 Explore  [View →]│ │  (markdown)      │
│          ├──┴─────────────────────┴─┤                  │
│ ▶ System │  [input] [send/stop]     │  [Approve] (plan)│
└──────────┴──────────────────────────┴──────────────────┘
```

The sidebar is collapsible (toggle in header, persists via localStorage). The side panel opens on the right when any sub-agent runs (Plan, Explore, general-purpose). Multiple sub-agents get their own tabs. Panel is resizable via drag handle (width persists in localStorage). Toggle with `Cmd/Ctrl + \`.

## Features

### Session Management
- **Sidebar** — Collapsible sidebar with sessions split into four groups: **Starred** (pinned, any source), **Conversations** (the feed, grouped by date and paginated with a "…" load-more), **Archived** (lazy, collapsed — archived conversations only; cron/hook sessions are excluded), and **System** (lazy, collapsed — live cron/hook sessions). The Archived and System groups fetch on first expand and drop their rows again on collapse. Toggle the sidebar via header button; state persists in localStorage.
- **Auto-naming** — New sessions get AI-generated titles via Haiku (e.g. "Italy Summer Vacation Planning" instead of the first message text)
- **Resumable sessions** — Sessions persist across server restarts via SDK `--resume` flag; full conversation context is restored
- **Stop button** — Red stop button replaces send during streaming; cancels agent task, saves partial response

### Agent Status
- **Live indicator** — Header shows current agent state: "Thinking...", "Writing...", "Using Read...", etc.
- **Per-session** — Sidebar shows spinner on the active session when agent is working

### Tool Call Rendering
Tool calls are collapsed by default with specialized renderers:
- **Edit** — Unified diff with red/green highlighting
- **Bash** — Terminal-styled with `$` prompt and output
- **Read/Write** — File path prominent, line count, collapsible content
- **Memory** (recall/memorize/history) — Parsed memory items with colored type badges (event, profile, knowledge, behavior)
- **Tasks** (create/list/update/done) — Task cards with status badges
- **Skills** (skill_list/get/create/update/read_reference/run_script) — Purple-themed cards. Load Skill shows skill name badge + line count; Create Skill shows name, description, and content preview; List Skills parses into individual skill cards; Update shows content diff preview.
- **Subagents** (Task tool) — Compact card in main chat with icon, type, description, summary line, and "View →" button. Full tool calls and results are routed to the side panel instead of cluttering the main chat. Expand chevron still available as inline fallback.
- **AskUserQuestion** — Interactive question card with clickable options (radio for single-select, checkboxes for multi-select). Multiple questions grouped in one card with a shared Submit button. Markdown previews on hover. When the agent is paused mid-turn (via `can_use_tool`), answers are sent through the interaction protocol (`answer_interaction` WebSocket message), injecting them into the SDK's `answers` field so the agent continues seamlessly. Falls back to a regular chat message for historical/non-interactive renders.
- **ExitPlanMode / EnterPlanMode** — Approval card with Allow/Decline buttons. Agent pauses mid-turn until the user responds. Plan panel auto-closes on approval.

### Modified Files Panel
GitHub PR-style diff viewer for files modified during a session. Accessible via the `[📁 N]` badge button in the chat header (appears when files have been modified).

- **File list view** — Cards for each modified file showing status badge (M/+/D), filename, parent directory, and `+N -M` diff stats. Click to drill into the diff.
- **Diff detail view** — Unified diff with dual line-number gutters, colored backgrounds (green additions, red deletions), hunk headers (`@@`), and collapsed context between hunks. Powered by `difflib.unified_diff` on the backend — works without git.
- **Snapshot-based** — Original file content is captured via `PreToolUse` hook before the first modification in each session. Only first touch is stored (`INSERT OR IGNORE`). Subsequent edits accumulate in the diff.
- **Reload resilient** — Snapshots persist in SQLite (`session_file_snapshots` table). On page reload/session switch, `fetchModifiedFiles` re-fetches from the REST API.
- **Real-time badge** — `file_changed` WebSocket events increment the header badge count as the agent works.
- **Persistent tab** — The files tab in the side panel does not auto-close (unlike sub-agent tabs).

### Side Panel
Generic tabbed panel that replaces the old plan-only preview panel. Auto-opens when any sub-agent runs:

- **Tabbed interface** — Each sub-agent gets its own tab (Plan, Explore, general-purpose). Tab bar appears when multiple tabs exist. Tabs show icon, label, elapsed time (running) or duration (complete).
- **Live activity feed** — Sub-agent's internal tool calls (Read, Grep, Bash, etc.) and thinking blocks are rendered in the panel using the **same components as the main chat** (ToolCallBlock, ThinkingBlock, etc.), not the main message stream. Routing uses `parent_tool_use_id` from the SDK to correctly attribute events to the right panel, even for parallel sub-agents.
- **Final result** — When the sub-agent completes, its markdown result appears below the activity feed, separated by a divider.
- **Plan actions** — Plan tabs get Approve/Decline buttons in the footer. When `ExitPlanMode` fires, both buttons appear. Approve resolves the interaction; Decline denies so the agent can revise.
- **Plan live updates** — Backend broadcasts `plan_update` WS events when Write/Edit targets `.claude/plans/` files, updating the panel content in real-time.
- **Auto-close** — Non-plan tabs (Explore, general-purpose) auto-close 5 seconds after completion. Plan tabs only close on explicit approve/decline.
- **Resizable** — Drag the left edge to resize (20%–65%). Width persists in localStorage.
- **Keyboard shortcut** — `Cmd/Ctrl + \` toggles panel visibility.
- **Animated** — Panel slides in/out with a 200ms width transition matching the sidebar animation.
- **Selection comments** — Select text in plan content to add/remove/improve/ask/note, same as in chat messages.

### Diagnostics Panel
System status dashboard (`/diagnostics`) with:
- **System** — Hostname, platform, memory (RSS), disk usage
- **Sources** — Per-source sync status: cursor, last run, records fetched/processed, errors
- **Tasks / FTS Index** — Active/done counts, FTS indexed vs total, in-sync status indicator (green ✓ / red ✗)
- **Recent Cron Logs** — Job ID, status, timestamps, errors

### Reload Resilience
- Server buffers streaming events per session
- On reconnect/tab switch, buffered events are replayed to reconstruct streaming state
- REST `/api/sessions/{id}/status` fallback for session running state
- **Ordered blocks** — Assistant messages store an ordered `blocks` JSON column in the DB, preserving the exact interleaving of thinking/text/tool_call blocks from streaming. On page reload, `hydrateMessage` uses this column directly instead of reconstructing from separate fields. Pre-migration messages fall back to the old thinking→tools→text ordering.

## Development

```bash
cd web

# Install dependencies
npm install

# Dev server (proxies to backend on :8900)
npm run dev

# Production build
npx vite build
```

The dev server proxies `/api` and `/ws` to `localhost:8900`.

## Build & Deploy

```bash
# Build production bundle
cd web && npx vite build

# Output goes to web/dist/
# FastAPI serves this directory automatically
```

## State Management

Uses Zustand for lightweight state management:
- `authStore` — Login/logout, token management
- `chatStore` — Sessions, messages, streaming state, agent status, side panel state (tabs, visibility, width), pending interactions (mid-turn user input), sidebar collapsed state, text selection quotes, modified files tracking. WebSocket message handling is dispatched to domain-specific handler modules under `handlers/`, with stateless helpers under `helpers/`.
- `taskStore` — Task list, search, filters, detail view with content editing
- `skillsStore` — Skills list with usage stats, detail view with SKILL.md editor, create/update/delete/toggle, filesystem sync
