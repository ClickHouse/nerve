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
│   ├── agentStore.ts   # Agent roster, transcripts, turn queue, stops (Zustand)
│   ├── handlers/       # Domain-specific WebSocket message handlers
│   │   ├── streamingHandlers.ts  # thinking, token, tool_use, tool_result, done, stopped, error, auto_turn
│   │   ├── sessionHandlers.ts    # session lifecycle (updated/status/switched/forked/resumed/archived/running)
│   │   ├── panelHandlers.ts      # plan_update, workflow_progress
│   │   ├── auxiliaryHandlers.ts  # interaction, file_changed, notifications
│   │   └── types.ts              # Shared Get/Set type aliases for handlers
│   ├── helpers/        # Stateless utility functions for chat state
│   │   ├── agentSelectors.ts     # Agent groups, dock line, spawn lookup, transcript segments
│   │   ├── agentSeen.ts          # Per-session "seen" agents (localStorage)
│   │   ├── blockHelpers.ts       # Panel tab limits (completed tabs kept)
│   │   └── bufferReplay.ts       # Session reconnect replay, deriveStatus, extractTodos
│   ├── authStore.ts    # Auth state (Zustand)
│   ├── setupStore.ts   # Setup checklist state, mutations, restart wait, banner (Zustand)
│   ├── taskStore.ts    # Task list/detail state (Zustand)
│   └── skillsStore.ts  # Skills CRUD + usage stats (Zustand)
├── components/
│   ├── Auth/           # Login page and session-expired overlay
│   ├── Setup/          # Setup wizard: shell, progress rail, step forms, restart overlay, banner
│   │   ├── steps/      # ClaimForm, ModeStep, ProviderStep, ProfileStep, …, ReviewStep
│   │   └── copy.ts     # Every heading, intro and hint of the wizard (no JSX)
│   ├── Chat/           # Message list, input, session sidebar, diff viewer
│   │   ├── agents/     # Agent lines, dock, Agents panel, transcripts, stop controls, queue chips
│   │   ├── tools/      # Specialized tool call renderers
│   │   ├── FileChangesPanel.tsx  # Modified files list + detail navigation
│   │   └── DiffView.tsx          # GitHub PR-style unified diff renderer
│   ├── Tasks/          # Task list, search, detail editor
│   ├── Memory/         # File browser with markdown editor
│   ├── Memu/           # Semantic memory browser (categories, items, sources); a Loom note under memory.backend: loom
│   └── Diagnostics/    # System status dashboard
├── utils/
│   ├── dateGroups.ts       # Date grouping (Today/Yesterday/This Week/Older)
│   ├── extractResultText.ts # Extract text from MCP content blocks
│   ├── hydrateMessage.ts
│   ├── passwordStrength.ts # The claim form's strength meter
│   ├── timeZones.ts        # Typed Intl.supportedValuesOf shim, browser zone and language
│   └── toolSummary.ts
└── App.tsx             # Main layout
```

## Layout

```
┌──────────┬────────────────────────────────┬──────────────────────┐
│ Sidebar  │  Header [≡] [title]            │  Side Panel          │
│ (toggle) │  [status] [context bar]        │  [Agents][build-api] │
│          ├────────────────────────────────┤  AGENTS · 2 running  │
│ Today    │  Message list                  │  NEEDS ATTENTION     │
│  Chat 1⁵ │  ⟳ build-api  Bash: pytest  ↗■ │  RUNNING             │
│  Chat 2  │  ✓ build-chart "4 files"    ↗  │   ⟳ build-api     ↗■ │
│ Yesterday│  ── Background · build-chart ──│  DONE (1)            │
│  Chat 3  │  ⟳ 2 running · ✓ 1  (dock)     │                      │
│          │  ⏱ Queued: "also run e2e" [×]  │                      │
│ ▶ System │  [input] [Queue] [■▾]          │                      │
└──────────┴────────────────────────────────┴──────────────────────┘
```

The sidebar is collapsible (toggle in header, persists via localStorage). The side panel holds the Agents roster, one tab per opened agent, plan and workflow tabs and the modified files; it opens when you open something in it (spawning an agent opens nothing by itself). Panel is resizable via drag handle (width persists in localStorage). Toggle with `Cmd/Ctrl + \`.

## Features

### Session Management
- **Sidebar** — Collapsible sidebar with sessions split into four groups: **Starred** (pinned, any source), **Conversations** (the feed, grouped by date and paginated with a "…" load-more), **Archived** (lazy, collapsed — archived conversations only; cron/hook sessions are excluded), and **System** (lazy, collapsed — live cron/hook sessions). The Archived and System groups fetch on first expand and drop their rows again on collapse. Toggle the sidebar via header button; state persists in localStorage.
- **Auto-naming** — New sessions get AI-generated titles via Haiku (e.g. "Italy Summer Vacation Planning" instead of the first message text)
- **Resumable sessions** — Sessions persist across server restarts via SDK `--resume` flag; full conversation context is restored
- **Stop button** — While a turn streams, a split red Stop replaces send. The button stops the orchestrator's turn only: background agents keep running. Its caret offers "Stop turn and send" (stop, then send the composer's text) and "Stop all agents…". If the turn does not end within 5 s while agents run, a banner offers "Force stop…" (it restarts the runtime and lists what ends); on a runtime that cannot spare background agents, the banner asks before stopping them. See [agents.md](agents.md#stopping)
- **Queued messages** — The composer always sends. While a turn streams, Enter queues the message: a chip above the composer shows it (also in other tabs, with its source when it came from a channel or the API) until the turn ends and it runs. Your own chips can be edited (✎: cancel it and continue editing its text in the composer) or cancelled (×); either way its text comes back into the composer, so nothing typed is lost

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
- **Agents** (Agent/Task tool) — One live line at the spawn point: status, name, type, activity or result, elapsed time, tokens, `[Open]` and a two-step `[■]` stop; the chevron reveals the prompt and result. Groups of agent calls show the last 8. Old history without an agent record keeps the legacy card with its "View →" result tab. A sub-agent's own work never appears in the main chat; it is in the agent's tab.
- **Background continuation** — An autonomous turn (the runtime resumed after background work settled) starts with a divider naming what settled ("Background · build-chart finished").
- **AskUserQuestion** — Interactive question card with clickable options (radio for single-select, checkboxes for multi-select). Multiple questions grouped in one card with a shared Submit button. Markdown previews on hover. When the agent is paused mid-turn (via `can_use_tool`), answers are sent through the interaction protocol (`answer_interaction` WebSocket message), injecting them into the SDK's `answers` field so the agent continues seamlessly. Falls back to a regular chat message for historical/non-interactive renders.
- **ExitPlanMode / EnterPlanMode** — Approval card with Allow/Decline buttons. Agent pauses mid-turn until the user responds. Plan panel auto-closes on approval.

### Modified Files Panel
GitHub PR-style diff viewer for files modified during a session. Accessible via the `[📁 N]` badge button in the chat header (appears when files have been modified).

- **File list view** — Cards for each modified file showing status badge (M/+/D), filename, parent directory, and `+N -M` diff stats. Click to drill into the diff.
- **Diff detail view** — Unified diff with dual line-number gutters, colored backgrounds (green additions, red deletions), hunk headers (`@@`), and collapsed context between hunks. Powered by `difflib.unified_diff` on the backend — works without git.
- **Snapshot-based** — Original file content is captured via `PreToolUse` hook before the first modification in each session. Only first touch is stored (`INSERT OR IGNORE`). Subsequent edits accumulate in the diff.
- **Reload resilient** — Snapshots persist in SQLite (`session_file_snapshots` table). On page reload/session switch, `fetchModifiedFiles` re-fetches from the REST API.
- **Real-time badge** — `file_changed` WebSocket events increment the header badge count as the agent works.
- **Persistent tab** — The files tab in the side panel does not auto-close.

### Agents
Sub-agents and background tasks (shells, monitors, workflows) of the session; see [agents.md](agents.md) for statuses and stops.

- **Agent dock** — One line directly above the composer while the session has agents: counts (running, needing attention, done) and the latest activity. Click it, press `Cmd/Ctrl + Shift + E`, or `↓` in an empty composer to open the Agents panel.
- **Agents panel** — A side-panel tab grouped **Needs attention** (failed, interrupted, lost, a stop not confirmed, a question waiting) → **Running** (children nested under their parent; quiet agents get an amber chip) → **Done** → **Ended** (stopped, and failures you have seen). Each row shows status, name, type, activity, elapsed time, tokens and `~$` cost, with Open and Stop. The header has **Stop all…**, which lists what ends. Keyboard: `↑ ↓ Home End` move, `Enter` opens, `x x` stops, `Esc` returns to the composer.
- **Agent tabs** — One tab per opened agent (at most 8): the live transcript (rendered with the chat's own blocks), "Load earlier" for older items, and actions Stop, Copy result, Jump to spawn, Follow up… and Relaunch…. Shells and monitors show their output tail; workflows their tree; Codex agents only the prompt and result. Agent tabs never close on their own.
- **Reload and restarts** — The roster comes back with the session (agent records are stored); after a gateway restart, agents that were running show as interrupted, with Relaunch….
- **Sidebar** — A session whose agents still run between turns stays in the Running group with a violet "parked" dot ("N agents running"; the count shows when two or more run).

### Side Panel
Generic tabbed panel for the Agents roster, agent transcripts, plans, workflows and modified files:

- **Tabbed interface** — Tab bar appears when multiple tabs exist. Each tab has its own labelled close button.
- **Plan actions** — Plan tabs get Approve/Decline buttons in the footer. When `ExitPlanMode` fires, both buttons appear. Approve resolves the interaction; Decline denies so the agent can revise.
- **Plan live updates** — Backend broadcasts `plan_update` WS events when Write/Edit targets `.claude/plans/` files (also by a sub-agent), updating the panel content in real-time.
- **Auto-close** — No side-panel tab closes on its own timer. Plan tabs close on approve/decline; finished plan and old-history result tabs beyond the five newest are pruned (never the focused one).
- **Resizable** — Drag the left edge to resize (20%–65%). Width persists in localStorage.
- **Keyboard shortcut** — `Cmd/Ctrl + \` toggles panel visibility.
- **Animated** — Panel slides in/out with a 200ms width transition matching the sidebar animation.
- **Selection comments** — Select text in plan content to add/remove/improve/ask/note, same as in chat messages.

### Setup

`/setup` is two things, decided by `SetupPage`:

- **The claim form** (`steps/ClaimForm.tsx`) while `GET /api/auth/status`
  says `login: setup` in local mode: username, password with a strength meter,
  "Keep this installation passwordless" with its warning, display name, and
  the setup token in a password field. It is mounted outside the app shell
  (nobody is signed in yet). The token lives in component state only and is
  cleared on success; nothing about it reaches `localStorage`. A successful
  claim stores the session token, refreshes auth and the actor directory,
  loads the checklist and navigates to `/setup/mode` — not to chat.
- **The wizard** (`WizardShell`) for a signed-in person, in both
  authentication modes, at `/setup` (the Overview) and `/setup/:stepId`.

`App` mounts `SetupLanding` beside the banner once the app is live — behind a
front gateway, that is after the session check. The first read of the
checklist in a sign-in sends an instance that has never chosen a mode (the
Mode step still `pending`) to `/setup`, once; a deep link into the wizard is
left alone, and a partially set-up instance — Mode chosen, steps left — gets
the banner instead.

The wizard is a `PageHeader` ("Set up Nerve"), a progress rail
(`ProgressRail`: `<nav aria-label="Setup steps">` with an ordered list,
`aria-current="step"`, a status badge per step — done, To do, Skipped,
Blocked with the reason as helper text, a lock on read-only steps) and the
current step. On desktop the rail is a 220 px column; on phones it lives in a
drawer opened from the header and a line under the header reads "Step 3 of 9 ·
Model". Every step is a `<form aria-label>` with an `<h1 tabIndex={-1}>` that
receives focus on navigation, so Enter in a field submits and screen readers
land on the heading. The footer is sticky inside the scroller (never fixed, so
it cannot collide with the bottom nav): Back · Skip for now (or Undo skip on a
skipped step) · Continue — which reads **Next** on a blocked or read-only
step and sends nothing — and, once a provider is configured, **Finish now**.
Back on a dirty form asks "Leave without saving?" inline (`role="alertdialog"`).

The **Overview** orients: "Signed in as alice" (in external mode: "Signed in
as Alice Example through your organisation's sign-in"), what the wizard does,
N of M steps done, the step list with badges and Change links, a notice when a
restart is pending, and Continue to the first pending step. M counts only the
steps that apply on this instance: a step closed by policy ("Not available on
this instance") or by a lockdown stays in the list but is not counted.

Step forms (`steps/*.tsx`) open on the server's values and are keyed by
`${viewer.id}:${step.etag}`, so another person's save — or a restart —
remounts them with fresh data, while a test result or a pairing poll (which
leave the persisted values alone) never wipes what you are typing. The server
is the source of truth: every save replaces the whole state with the
response; the client never computes a status. On a pending or skipped step
Continue always sends the step's decision (so accepting a prefilled time zone
is saved and recorded); on a done step it sends only what differs from the
server's detail. A `400` with a `field` lands under that input
(`aria-describedby`, `aria-invalid`); anything else is a `role="alert"` above
the footer; a `409` is shown as the server worded it, then the state is
reloaded. Secrets render through `SecretField`: "Configured · …ab12 — leave
blank to keep", with Replace and Remove (which sends `null`). Choice cards
are a `radiogroup`; test buttons report in a `role="status"` region.

Per-step behaviour worth knowing: Mode is read-only once chosen; the Model
step shows the "Claude Code login on this server" card only when
`GET /api/setup/provider/host-login` finds one, offers "Test connection", and
shows the model and effort picker (with one line of guidance each) once a
credential works; About you prefills the browser's time zone, shows quiet
hours as a switch above the two times (on with 22:00–07:00 when a personal
instance still has the defaults, off for a worker; off sends
`quiet_enabled: false`), and offers theme pills (client-side only); Channels
verifies a Telegram token with the test button,
shows the saved token's start error when the bot failed to boot, and embeds
the `PairingWidget` (t.me link, code, 2 s poll) when the bot is live, with a
manual user id fallback; Sources load `GET /api/setup/sources/preflight` and
warn when a toggle is on but its tool is missing; Automation renders
schedules in the chosen time zone and, for a worker, shows `task-planner` off
by default with a note that the onboarding conversation turns it on when the
worker should plan through the approval loop; a job that `jobs.yaml` replaces
shows that file's state, read-only, with a note; Agents default-checks what is
installed.

**Review** (`steps/ReviewStep.tsx`) lists the checklist with Change links,
runs the server check-up (`GET /api/setup/environment`) with its hints — an
HTTPS warning when the page is served over plain HTTP from a non-loopback
address, a passwordless-on-LAN warning — shows what was written where from
`steps[].files`, and ends with one button: **Finish** (`POST
/api/setup/finish`, hot reload) or **Restart Nerve** when something needs a
restart, with the reasons listed. Under systemd (or an operator-declared
supervisor) the operator's command is shown before the click — it arrives as
`restart_command` in the setup state (and `command` from `/api/setup/environment`),
never parsed out of the message. Then `ReadyScreen`: "Nerve is ready" (a
worker: "Your worker is ready"), three summary lines, **Start the
conversation** into the onboarding conversation (or, after a restart is
needed first, a line that it opens after the restart; when onboarding is over,
**Say hello** with a suggested first message, or **Open chat** for a worker),
a pairing card when a live Telegram bot has nobody paired yet, and "Open the
setup page". The setup Overview shows `OnboardingPanel` while onboarding is
pending or waits: the way into the conversation and **Skip onboarding**
behind an inline confirmation. `SetupBanner` shows the onboarding reminder
first: on every page except the onboarding chat, to every signed-in person,
with **Continue**; its close button hides it until the next page, and it
reads the setup state again on each page change while it shows.

**RestartOverlay** covers the page while the daemon restarts and is never a
trap: "Restarting Nerve… (12 s)" with **Not now** after ten seconds; for an
`unsupported` restart, who must act and the command with Copy; after the time
box, "Nerve did not come back" with **Keep waiting** and Close. Escape maps to
the secondary action. The wait itself is owned by `setupStore`
(`waitForGeneration`): it polls `/health` once a second until `generation`
differs from the one the restart answered with, then clears the hosted
"unavailable" state, re-reads the auth descriptor and the viewer, and reloads
the checklist — closing the overlay does not stop it, so the banner and the
Review step update on their own when Nerve is back. 5xx and network errors
count as "still restarting". If `/health` is not reachable through a front
gateway (two 404 or non-JSON answers), the store reads `generation` from a raw
`GET /api/setup` instead. Neither probe goes through the normal request
helper, so no sign-in handling fires mid-restart.

**SetupBanner** is mounted next to the notification toast: "Finish setting up
Nerve — 3 steps left · Continue" (plus " · A restart is pending"), or "Restart
Nerve to apply your setup · Restart" linking to `/setup/review` when only the
restart is left. Bottom-left on desktop, top on phones. Hidden on `/setup`,
when setup is finished, while the state is unknown, during a restart, and
after Dismiss (remembered per viewer in `sessionStorage` for the tab session).
The **Setup** entry in the navigation appears as soon as the checklist has
been read, in both auth modes, and leaves the rail's list when setup is
finished. Then a gear button labelled "Setup" in the footer of the nav rail
opens `/setup`. On a phone the entry stays in the **More** drawer. The
chat page's empty state reads "Connect a model to start a conversation" with a
link to `/setup/provider` while the server says no provider is configured.

**External authentication.** `/setup` and `/setup/:stepId` are registered in
the hosted frontend too; only the claim form is local-only, and the switch
keys on the hosted store's `mode === 'external'`, never on `login`. In that
mode no password, passwordless or token control renders anywhere in the
wizard, About you takes the name from the gateway's sign-in and only sets what
Nerve writes in `USER.md`, the profile step never calls an account endpoint,
and the reconnect after a restart goes through the hosted adapter's own
session probe, so no login form can appear.

### Accounts

`/accounts` — the local accounts: username, display name, whether each is
enabled and whether it has a password. Add someone, rename, disable and
re-enable, and change your own password.

Every account can do all of that to every other account; there are no roles, so
adding a person gives them the power to disable you. Disabling is the only
destructive action here — there is no delete — and it asks before it acts, with
stronger wording for your own account, because the click that disables you is
also the click that signs you out and getting back in needs somebody else. The
only thing the server refuses outright is disabling the last enabled account. Two more refusals apply until the
first account has both a password and a username, which is what has to be true
before a second account can exist — the page shows the reason rather than
guessing at it.

A standalone instance with no password opens `/accounts` directly so its owner
can name and secure the first account.

### When a session expires

The app stays mounted and asks for the password over the top, so nothing you had
typed is lost. It unlocks **the account whose app is on screen** and no other:
the username is shown rather than asked for, because everything underneath —
unsent drafts, what has been read, the loaded session list — belongs to that
person, and a form that took any username would let somebody else unlock a
colleague's tab and inherit it. Using a different account goes through "log
out", which is what discards all of that first.

### Attribution

Messages and sessions store stable actor ids, never names. The UI resolves their
current names from `GET /api/actors`: Nerve is always labelled, while a human is
labelled only when their actor id differs from the signed-in viewer. The viewer
is the `actor` from `GET /api/auth/me`, not the account. Thus Alice
does not see repeated labels on her own history, but Bob sees Alice throughout
an Alice-only transcript and sidebar. The same rule applies to the open-session
header; on phones it keeps the name visible and hides only the "Started by"
verb visually.

Null ids in legacy or unidentified-human history render no attribution element.
Names are held only in memory. Equal rendered names get collision-safe id
suffixes in visible text. A person without a display name is labelled by their
username; `Unnamed account` appears only when they have neither. The system
principal uses `Nerve` and a bot glyph.

Optimistic and live messages carry actor ids so labels are correct before a
reload. The actor directory coalesces concurrent reads and performs one
follow-up for ids arriving while a read is in flight. Account mutations force a
refresh, so same-tab renames update immediately without rewriting messages;
renames in another tab converge on reload. Request generations prevent a stale
lookup from repopulating the map after logout or overwriting a newer refresh.

### Diagnostics Panel
System status dashboard (`/diagnostics`) with:
- **System** — Hostname, platform, memory (RSS), disk usage
- **Usage & Cost** — 7-day totals, daily cost, and cost breakdowns by model, by source, and by cron job (with runs and average cost per run)
- **Sources** — Per-source sync status: cursor, last run, records fetched/processed, errors
- **Tasks / FTS Index** — Active/done counts, FTS indexed vs total, in-sync status indicator (green ✓ / red ✗)
- **Memory service** — memU's status, item counts and operations; with `memory.backend: loom`, Loom's: the gateway it is reached through, the org and namespace, where the namespace came from, the last error and when it was checked, and a link to Loom's dashboard when `memory.loom.dashboard_url` is set
- **Recent Cron Logs** — Job ID, status, timestamps, errors

### Reload Resilience
- Server buffers streaming events per session
- On reconnect/tab switch, buffered events are replayed to reconstruct streaming state
- Every bind (and every socket reconnect, which re-binds the active session) sends `session_status` with the agent snapshot; open agent tabs resubscribe and fetch what they missed
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
- `setupStore` — The setup checklist exactly as the server last sent it
  (`state`), one mutation in flight at a time (`busy`), field errors, the
  restart wait (`waitForGeneration`, `retryWait`, `dismissRestartOverlay`),
  cached preflights (models, host login, sources, agents, environment) and
  the banner dismissal. Loaded once per app session in both auth modes;
  silent on an older backend without the endpoint. Selectors:
  `setupIsUnfinished`, `stepsLeft`, `providerConfigured`, `stepById`
- `actorStore` — The actor id → display name map behind attribution labels. In
  memory only; same-tab account mutations refresh it, while cross-tab renames
  converge on reload
- `chatStore` — Sessions, messages, streaming state, agent status, side panel state (tabs, visibility, width), pending interactions (mid-turn user input), sidebar collapsed state, text selection quotes, modified files tracking. WebSocket message handling is dispatched to domain-specific handler modules under `handlers/`, with stateless helpers under `helpers/`.
- `agentStore` — The active session's agent roster (records keyed by id, newest `rev` wins), summary and capabilities, the turn queue and pending sends, open agent transcripts (live items by `seq`, gaps refetched), the stop escalation and dialogs, and the seen set. Hydrated from `session_status` and `GET /api/sessions/{id}/agents`, kept current by `agent_update`, `agent_event` and `queue_update`.
- `taskStore` — Task list, search, filters, detail view with content editing
- `skillsStore` — Skills list with usage stats, detail view with SKILL.md editor, create/update/delete/toggle, filesystem sync
