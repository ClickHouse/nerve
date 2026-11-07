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
│   ├── Settings/       # Settings sections, their rows and controls
│   │   ├── steps/      # ClaimForm, ModeStep, ProviderStep, ProfileStep, …, ReviewStep
│   │   └── copy.ts     # Every heading, intro and hint of the wizard (no JSX)
│   ├── Chat/           # Message list, input, session sidebar, diff viewer
│   │   ├── agents/     # Agent lines, dock, Agents panel, transcripts, stop controls, queued messages
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

The sidebar is collapsible (toggle in header, persists via localStorage). The side panel holds the Agents roster, one tab per opened agent, plan and workflow tabs, the modified files and the session's tasks; it opens when you open something in it (spawning an agent opens nothing by itself). Panel is resizable via drag handle (width persists in localStorage). Toggle with `Cmd/Ctrl + \`.

On a phone (narrower than 768 px), a bar at the bottom replaces the nav rail: **Inbox**, **Chat**, **Tasks**, **Notifs** (with the count of pending notifications) and **More** (all other pages, Plans included). On the chat page the session list is a drawer over the chat.

## Features

### Session Management
- **Sidebar** — Collapsible sidebar with sessions split into four groups: **Starred** (pinned, any source), **Conversations** (the feed, grouped by date and paginated with a "…" load-more), **Archived** (lazy, collapsed — archived conversations only; cron/hook sessions are excluded), and **System** (lazy, collapsed — live cron/hook sessions). The Archived and System groups fetch on first expand and drop their rows again on collapse. Toggle the sidebar via header button; state persists in localStorage.
- **Needs you:** the first group of the sidebar holds each session that waits for you, of any source: a pending question or approval, a pending plan that it proposed, a question inside a turn, or a review loop that waits for a decision. Each row tells why in a second line ("Question", "2 approvals", "Plan to review", "Waiting for input", "Review loop"). When the agent is blocked on you (a question, an approval, a review loop on a decision), the row shows a red "!" in place of its status dot; a plan to review does not block the agent and keeps the normal dot. A session in this group is not in Running, Starred or its parent. One line above the list counts the groups, for example "2 need you · 3 running". The list reloads 500 ms after a notification event, after a session starts or stops waiting for input or for a review loop decision, after a plan decision in the tab, and after a `task_updated` event (a plan decision from another tab, Telegram or the API writes the plan's task).
- **Inbox (phone):** `/inbox` shows the session list on the full page, with the Needs you group first, and a composer at the bottom ("Message Nerve...", with the agent name from `IDENTITY.md`). Sending starts a new chat with the message and opens it. When the message cannot be sent, the text stays in the box with "Could not send. Try again.", and the next send goes to the chat that the failed send created. On a phone `/` opens the Inbox. On desktop `/inbox` opens `/chat`, which shows the same list next to the chat.
- **Origin**: the leading icon of a session row tells where the session comes from: Slack, Telegram, email, the API or an external client (plug), a cron job or "Run later" with a delay (clock), a plan (hammer), a workflow run, or a fork (branch). A plain web chat keeps the chat icon. The tooltip is the origin as a sentence, for example "Started from Slack #general". Above the first message, one line names the origin, for example "From Slack #general" or "From cron job daily-digest", and links to the plan, the cron jobs or the workflow runs. It names no person: the header names who started the session. A fork keeps its own "Forked from" line under the header. See `trigger` in [api.md](api.md#sessions)
- **Tasks from this session**: when a session created, implemented or updated tasks, the chat header shows a tasks button with their count, next to the modified files button. It opens the **Tasks** tab of the side panel: each task with its title, what the session did ("Created", "Implemented", "Updated") and its status (`GET /api/sessions/{id}/tasks`). Click a task to open it in the task dialog over the chat; closing the dialog goes back to the chat with the panel open. The list loads when the session opens and when the tab opens, and loads again on each `task_updated` event that changes a task, and after a reconnect
- **Auto-naming** — New sessions get AI-generated titles via Haiku (e.g. "Italy Summer Vacation Planning" instead of the first message text)
- **Resumable sessions** — Sessions persist across server restarts via SDK `--resume` flag; full conversation context is restored
- **Stop button** — While a turn streams, a split red Stop replaces send. The button stops the orchestrator's turn only: background agents keep running. Its caret offers "Stop turn and send" (stop, then send the composer's text) and "Stop all agents…". If the turn does not end within 5 s while agents run, a banner offers "Force stop…" (it restarts the runtime and lists what ends); on a runtime that cannot spare background agents, the banner asks before stopping them. See [agents.md](agents.md#stopping)
- **Queued messages**: The composer always sends. While a turn streams, Enter queues the message: a row above the composer shows it (also in other tabs, with its source when it came from a channel or the API) until it runs, and its bubble shows at the end of the transcript when it starts, in every tab. The rows are in run order, the top one runs next; the arrows on a row move it up or down, for any queued message. Your own rows can be edited (✎: cancel it and continue editing its text in the composer) or cancelled (×); either way its text comes back into the composer, so nothing typed is lost

### Agent Status
- **Live indicator** — Header shows current agent state: "Thinking...", "Writing...", "Using Read...", etc.
- **Per-session** — Sidebar shows spinner on the active session when agent is working

### Context window
- **Context bar**: the chat header shows the context use of the last turn and how full the window is (on a phone, the percentage). A `~` marks an estimate from the last turn's usage.
- **Context popover**: a click on the bar opens it (Escape or a click outside closes it). For a Claude session with a live runtime it shows a bar with one segment per category (system prompt, system tools, MCP tools, skills, messages, and so on), the tokens of each category and the auto-compact threshold. For a Codex session, or a Claude session with no live runtime, it shows the totals only. Below that are the last turn's tokens and the session cost.
- **Compact**: in a live Claude session, **Compact…** asks first ("Summarizes earlier messages to free context. The full transcript stays in Nerve.") and then runs the compact as a turn. Compact is available when the agent is idle; while a turn runs, the button is disabled. The popover then shows the new categories and the tokens it freed. When the runtime has nothing to compact, the popover says so; nothing goes into the chat. See [`POST /api/sessions/{id}/compact`](api.md#post-apisessionsidcompact).

### Tool Call Rendering
In the chat, each run of tool calls and thinking between two text blocks of a message is one **tool burst**. A finished burst with a tool call and at least two blocks shows as one muted line that names the work, for example "Read 3 files, ran 4 commands, searched 2 times, edited 2 files" (reads and edits count distinct files; MCP tools count per server, "used 2 nerve tools"), with "1 failed" in red when calls failed. A click opens the rows. While a turn runs, its last burst stays open, so the newest calls show; it folds when text follows or the turn ends. Question cards (`ask_user`, AskUserQuestion, `ask_form`), the ExitPlanMode decision card and the first card of a workflow run are never in a burst. Agent tabs and plan tabs show the rows without bursts.

Tool calls are collapsed by default with specialized renderers:
- **Edit** — Unified diff with red/green highlighting
- **Bash** — Terminal-styled with `$` prompt and output
- **Read/Write** — File path prominent, line count, collapsible content
- **Memory** (recall/memorize/history) — Parsed memory items with colored type badges (event, profile, knowledge, behavior)
- **Tasks** (create/list/update/done) — Task cards with status badges
- **Skills** (skill_list/get/create/update/read_reference/run_script) — Purple-themed cards. Load Skill shows skill name badge + line count; Create Skill shows name, description, and content preview; List Skills parses into individual skill cards; Update shows content diff preview.
- **Agents** (Agent/Task tool) — One live line at the spawn point: status, name, type, activity or result, elapsed time, tokens, `[Open]` and a two-step `[■]` stop; the chevron reveals the prompt and result. Groups of agent calls show the last 8. Old history without an agent record keeps the legacy card with its "View →" result tab. A sub-agent's own work never appears in the main chat; it is in the agent's tab.
- **Background continuation** — An autonomous turn (the runtime resumed after background work settled) starts with a divider naming what settled ("Background · build-chart finished").
- **AskUserQuestion** — Interactive question card with clickable options (radio for single-select, checkboxes for multi-select). Each question also offers **Other…**, which opens a "Your answer" field: the typed text (required once chosen) is that question's answer — it replaces the option in a single-select question and is appended to the chosen labels in a multi-select one. A card with one single-select question still answers on one click of an option. Multiple questions grouped in one card with a shared Submit button. Markdown previews on hover. When the agent is paused mid-turn (via `can_use_tool`), answers are sent through the interaction protocol (`answer_interaction` WebSocket message), injecting them into the SDK's `answers` field so the agent continues seamlessly. Once the question is no longer pending (answered here or in another tab, after a reload, or closed by a timeout), the card is read-only and shows the chosen answers; it never posts an answer as a chat message.
- **ask_form** (Nerve's form tool, `mcp__nerve__ask_form`) — One card for up to six questions while the agent's tool call waits for the answers (up to an hour). The call's optional intro (Markdown, the agent's explanation of why it asks) shows above the card as chat text, and stays there after the card is answered and folds. In the card: an optional title, then each question with its label chip (`*` when required, or "Optional"), the question and the agent's reason in muted text. By kind: `choice` is option rows to pick one and `multi` option rows to pick any, each with **Other…** for the person's own words unless the agent turned it off (in `multi` the Other… text is one more answer); `text` is a two-row box that grows (Enter is a new line); `url` is checked to be http(s) with a host and no spaces; `list` is chips (a comma or Enter adds one, Backspace in the empty field removes the last, a paste splits at commas and line breaks, and leaving the field keeps the typed text); `repos` is chips like `list`, each `owner/name` (a pasted GitHub URL becomes `owner/name`; an invalid one is red with a hint and blocks Submit). Defaults are prefilled. **Submit** (or Cmd/Ctrl+Enter anywhere in the card) is enabled once every required question has an answer and nothing is invalid; Escape inside the card never stops the waiting turn. The answers go back through `answer_interaction` (`result: { answers }`), and the agent gets one line per question plus the same answers as JSON. The card answers only while its own form waits; after an answer here or in another tab, a timeout or a reload it is read-only. Answered cards fold, `ask_form` and AskUserQuestion alike: an answered or closed card shows one line per question, `LABEL → answer` (chips for a list, "—" for no answer); **Show questions** opens the full card, read-only, and **Hide** folds it again. Cards start folded, also after a reload, where the answers are read from the tool result; a card that still waits is shown in full. "Closed" means it ended without an answer (a timeout, the form was closed, or the session stopped).
- **ask_user** (Nerve's question tool, `mcp__nerve__ask_user`): the question and its options, answerable in the chat while it waits. The card reads the question's notification, so the chat, the Notifications page and the toast show the same question with the same option list: one full-width row per option with a letter key (A, B, C). While focus is in the list, a letter or a digit 1 to 9 chooses that row. An option that ends in "(recommended)" shows first with a "Recommended" tag. The last row, **Other…**, opens a field for an answer in your own words (Enter sends, Escape closes the field). An approval (`propose_action`, on the Notifications page and in the toast) has no Other… row and says "Nothing happens until you choose."; a question does not, because the agent can continue without the answer. In an approval, Approve (green, ✓), Decline (red, ✕) and Snooze 24h (moon) show their icon in place of the letter, and the answer line shows that icon in place of `✓`. The letter keys also choose these rows. After an answer from any place (the chat, the Notifications page, the toast, Telegram, Slack), each place shows `✓ <answer>` with who answered, when, and through which channel; **Show options** opens the list, read-only. When the server refuses an answer, the card reads the question again and shows the answer from a different place, if there is one; if not, "Could not send the answer. Try again." shows under the options. A question that expired shows "Expired without an answer". The toast shows all options and stays open until you answer or dismiss it; when the question is answered in another place, the toast shows the answer and closes after 5 seconds. A `notify` call shows as a "Notify" row.
- **EnterPlanMode** — A status line, "Entering plan mode…" while it runs and "Entered plan mode" when complete ("Plan mode declined" on an error), with no buttons: entering plan mode only restricts the agent, so no approval is involved (the Claude CLI switches the mode itself; if a CLI asks, the server allows it at once).
- **ExitPlanMode** — The decision card. The agent pauses mid-turn until the person decides. The card shows an autogenerated summary, not the plan itself: up to six short bullets that the server writes with `agent.title_model` when the pause starts (the `plan_summary` event), with "Summarizing…" until they arrive and the plan's first heading and two lines when no summary can be made; the full plan (the interaction's `tool_input.plan`, which the CLI fills with the plan file's text) is in the side panel, which **Read the plan** opens. **Approve** lets the agent go on (it starts on the plan in the same turn) and closes the plan tab. **Request changes…** opens a required "What should change?" field; **Send** refuses the plan with that text, which reaches the agent verbatim: it stays in plan mode, revises the plan and asks again with a new card. **Edit**, offered when the pause carries the plan text, opens the side panel with the plan as a text field ("Edit the plan"); there **Apply edited plan** (enabled once the text changed) approves the person's text instead (`answer_interaction` with `result: { plan }`): the CLI saves it to the plan file and tells the agent the plan was approved with the person's edits. **Cancel** drops the edits. A decided card keeps its outcome line and a **Read the plan** link that opens that plan read-only (the edited text, for an edited approval). In the onboarding conversation the card is titled "Setup proposal" with **Apply setup**, **Change something…** and "Edit the setup proposal". After an approval, while that turn still runs, the card reads "Approved — applying…" ("Approved with your edits — applying…"; "Applying the setup…" in the onboarding conversation), then "Approved" or "Approved with your edits"; a change request reads "Changes requested" with the first line of the feedback, and an error that is not the person's feedback (a timeout, a stopped session, a CLI validation error) reads "Closed" with its reason.

### PR Cards
An assistant message that links to GitHub pull requests
(`https://github.com/<owner>/<repo>/pull/<number>`) shows one card for each
pull request below its content. The links come from the message text. Only when
the text has no such link, they come from each tool result that links to
exactly one pull request, for example the URL that `gh pr create` prints. A
tool result that lists more pull requests gives no cards. A message shows at
most 5 cards. A card shows the title (a link to GitHub), a state chip (Draft,
Open, Merged or Closed), `owner/repo#number` with the branches, the checks
("2 failed", "3 running", "12 passed") and the review decision. The data comes
from `GET /api/github/pulls`, so the server needs a `gh` login. Without one,
the card shows `owner/repo#number` as the link and the reason
("No access to details", "GitHub CLI not available"). The cards of a message
read their data the first time they come into view (or within 200 px of it).
While a pull request is open with checks running, its card reads it again 60 s
after the last read, but only while the card is in view and the tab shows. The
card stops when the checks end or the pull request merges or closes. When the
request fails, the card shows "Could not load details" and reads again 60 s
later, also only while it is in view and the tab shows. A message that still
streams shows no cards.

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
Generic tabbed panel for the Agents roster, agent transcripts, plans, workflows, modified files and the session's tasks:

- **Tabbed interface** — Tab bar appears when multiple tabs exist. Each tab has its own labelled close button.
- **Plan actions** — While `ExitPlanMode` waits, the plan tab's footer offers the same decision as the chat card, with the same labels, feedback field and Edit: Approve resolves the interaction and closes the tab (from either surface); Request changes… denies it with the person's text and keeps the tab, so the revised plan updates in place. With no `ExitPlanMode` waiting and no turn running, the footer's Approve sends "Looks good — implement it." as a message and closes the tab; the onboarding conversation has no such fallback, because its setup proposal is decided once, on its pause.
- **Plan live updates** — Backend broadcasts `plan_update` WS events when Write/Edit targets a file in the CLI's plans directory (`~/.claude/plans/`, or `$CLAUDE_CONFIG_DIR/plans/`; also by a sub-agent), updating the panel content in real-time.
- **Plans open on demand** — In Claude sessions the plan tab no longer opens by itself, neither when the agent writes its plan file nor when `ExitPlanMode` waits: the decision card's summary is what the person sees first, and **Read the plan** or **Edit** on the card open the tab. Codex sessions, which have no decision card, still open the tab on a plan update.
- **Auto-close** — No side-panel tab closes on its own timer. Plan tabs close on approve; finished plan and old-history result tabs beyond the five newest are pruned (never the focused one).
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
(`ProgressRail`: `<nav aria-label="Setup steps">` with an ordered list of the
person's own steps and Review, `aria-current="step"`, a status badge per step:
Done, To do, Skipped, After Mode, a lock on a read-only step) and the
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
restart is pending, and Continue to the first pending step.

`stepPlace` in `navigation.ts` puts each step in one of three places, and every
list and walk of the steps uses it: the rail, the Overview list and its count,
the Review, Back/Continue and the phone's position line show only the
person's **own** steps. A done step that the server marks read-only (under
lockdown, or a step the operator provided on an instance that keeps no
credentials) is the **operator's**: the Overview lists it under "Your operator
has configured", with its title and summary and no Change link. The Mode step
and the Account (the claim) are never the operator's: the person chose them.
The Mode step is read-only once chosen, but it stays an own step. A step that
a lockdown or the policy closed and that is not done is **hidden**: the page
does not show it (also the Mode step, when a lockdown closed it before a mode
was chosen). A link to an operator's or a hidden step goes to the Overview.
The chat's empty state and the Ready screen's Telegram sync line do not link
to a step that is not one of the person's own. When no step is
left for the person (external mode under lockdown, before a mode is chosen),
the Overview and the Review say that there is nothing to set up on the page
and that the operator sets up the instance, in place of the count and the
list.

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

**Review** (`steps/ReviewStep.tsx`) lists the own steps as a checklist with Change links,
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
pending or waits: the way into the conversation (**Start the conversation**
once setup is complete) and **Skip onboarding** behind an inline
confirmation. `SetupBanner` shows the onboarding reminder first: on every
page except the onboarding chat, to every signed-in person, with **Continue**;
while the conversation runs and the agent waits for the person
(`onboarding.awaiting` in the setup state), "Your agent has a question for
you" with **Answer** (`question`) or "Your agent proposed a setup" with
**Review** (`plan`), both linking to the conversation;
when setup is complete and the conversation has not started, with **Start**,
which calls `POST /api/setup/finish` and opens the conversation; while a
worker sets itself up without a person, with **Open**, a link to that
session. Its close button hides it until the next page, and it reads the
setup state again on each page change while a session runs.

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
finished. Then "Run setup again" in [Settings](#settings) opens `/setup`. On
a phone the entry stays in the **More** drawer. The
chat page's empty state reads "Connect a model to start a conversation" with a
link to `/setup/provider` while the server says no provider is configured.
When a lockdown closed the Model step, it reads "No model is connected" with
the step's reason from the server, and no link.

**External authentication.** `/setup` and `/setup/:stepId` are registered in
the hosted frontend too; only the claim form is local-only, and the switch
keys on the hosted store's `mode === 'external'`, never on `login`. In that
mode no password, passwordless or token control renders anywhere in the
wizard, About you takes the name from the gateway's sign-in and only sets what
Nerve writes in `USER.md`, the profile step never calls an account endpoint,
and the reconnect after a restart goes through the hosted adapter's own
session probe, so no login form can appear.

### Settings

`/settings/:section?`: the settings that people otherwise change in YAML.
The gear in the nav rail footer opens it (on a phone: Settings in the More
menu). A desktop shows the section list on the left and opens the first
section; a phone shows the list at `/settings` and one section at
`/settings/<id>`. "Run setup again" under the list opens `/setup`; while setup
is unfinished, Setup also stays in the rail.

Each control saves when it changes (a text field on Enter or when it loses
focus) through `PUT /api/settings/<section>` (see
[api.md](api.md#settings)), and the row shows the answer of the server. A
row says in small text where its changes are saved (`settings.yaml`, or
`config.yaml` on this machine when that file states the value). A field that
the environment or the control plane sets shows its reason and no control.
Under lockdown the section is read-only, with one line that says why.

**Notifications** (`NotificationsSection`):

- **Where notifications go**: the Nerve panel (always on), This browser,
  Slack and Telegram. This browser (`ThisBrowserRow`) has the switch and the
  level of the [browser alerts](#browser-alerts), the switch of the
  [push notifications](#push-notifications) and the other devices of the
  person that get them. Slack and Telegram have a
  switch (`notifications.channels`),
  a destination (the Slack channel ID with the channel name, the Telegram
  chat ID) and "What to send": Everything or Questions and important
  (`notifications.levels`). A channel that is not connected links to the
  Channels step of Setup.
- On a hosted instance (`hosted` in the API, `slack.mode: hosted`), there is
  no Telegram row. The Slack row shows only while the control plane has a
  Slack app connected for the agent. It has the switch and "What to send",
  and in place of the destination one line: "Cards go to the notification
  channel that an admin sets in the control plane. With no channel set
  there, Slack gets none."
- **Send a test** sends one notification to each place that is on. Each row
  then shows "Test arrived" or "Test did not arrive". This browser shows it
  for push while push is on there: whether a push service took the test.
- **Quiet hours**: a switch and a start and end time (`quiet_start`,
  `quiet_end`), in the instance's time zone, which the row names. On fills in
  22:00 to 07:00; off empties both times. During them, notifications below `urgent` arrive without sound (see
  [config.md](config.md#quiet-hours)).
- **Silences** opens the silences manager (`/notifications?silences=1`).
- **Advanced**: when an unanswered question expires, and how often a
  snoozed question comes back.

### Browser alerts

A panel tab that is open but not in front can show a system alert for a new
notification. The choice is per browser, in localStorage
(`nerve-browser-alerts`), not on the server. It is made in Settings >
Notifications > This browser ("Alerts while a tab is open"), or with "Turn
on" in the offer on the first toast that waits for an answer ("Turn on · Not
now", shown until the person answers it). On the Notifications page, a line
above the list ("Browser alerts are on" or "off") opens the setting.

- **What alerts:** "Questions and important" (questions, approvals, `high`
  and `urgent`; the default) or "Everything". A silenced item never alerts.
- **One alert across tabs:** the tabs of a browser share a `BroadcastChannel`.
  A tab in front tells the others that the person sees the item; a tab in
  the background waits a moment, then shows the alert and tells the others.
  Before it shows the alert, a tab takes a Web Lock for the item and keeps it
  while the alert is open, so two tabs that wait the same time give one
  alert. The alert's tag is the notification id.
- **Click and close:** a click focuses the tab and opens `/notifications`.
  The alert closes when the item is answered or expires. Where the browser
  runs the panel's service worker, the worker shows the alert and handles
  the click (see [Push notifications](#push-notifications)).
- **Quiet hours:** an item that the server sends with `silent: true` gives an
  alert without sound.
- **Title and favicon:** on every panel tab, in front or not, the title
  starts with "!" while a session waits for an answer from the person (a
  question in the turn, a pending question or approval, or a review loop on a
  decision). Otherwise it starts with "(N)", the number of pending
  notifications that the Notifs badge shows. The favicon gets a red dot for "!" and a
  blue dot for "(N)". A page that sets its own title gets the prefix again.
  This needs no permission and no setting.
- **Limits:** browsers show alerts only for HTTPS addresses (and
  `localhost`); on plain HTTP the row says "Needs HTTPS". A browser that
  blocks the site shows "Blocked", and a browser that has no Notification
  API, or that shows alerts only through a service worker and runs none for
  the panel, shows "Not supported". A tab must be open. The alert has no
  answer buttons.

### Push notifications

"Alerts when no tab is open", in Settings > Notifications > This browser,
turns on Web Push for the browser. The instance sends each item to
the push service of the browser, and the panel's service worker (`/sw.js`)
shows it as a system notification, also when no panel tab is open. The server
keeps one row for each browser (see [api.md](api.md#web-push)). Under "Your
other devices" the row lists the other browsers of the person, with the day
the person turned push on there and the last notification, and a button that
removes each one.

- **What arrives:** the items at the level of the row ("What to send"),
  which the browser sends with its subscription; `notifications.levels.webpush`
  applies too ([config.md](config.md#web-push)). The notification shows
  "<agent name>: <title>" and the first lines of the body, with the
  notification id as tag. During quiet hours a push below `urgent` makes no
  sound. A click focuses a panel tab, or opens one, at `/notifications`. The
  browsers of a disabled account get no push (see
  [Request identity](accounts.md#request-identity), and
  [External mode](accounts.md#external-mode) for a person who loses access
  at the gateway).
- **When it shows:** not while a panel tab has focus, because the tab shows
  the item; at all other times, also while a panel tab is open but not in
  front, because browsers require a notification for each push then. On
  WebKit (Safari, and a Home Screen app on iPhone and iPad), the worker also
  shows the push while a panel tab has focus, with `silent: true`, because
  WebKit removes the subscription after three pushes that show nothing. The
  worker also shows the browser alerts of the tabs, with the same tag, so an
  alert and a push for one item give one notification, and phone browsers
  that show alerts only through a service worker can show them.
- **Server side:** the instance signs each push with its VAPID key, made at
  first use and kept in `nerve.db` (`instance_secrets`), never in a
  configuration file. A backup without secrets has no key, so after a restore
  each browser must turn push on again. When a push service answers `404` or
  `410`, the browser dropped the subscription, and Nerve deletes it.
- **Install:** the panel has a web app manifest (`/api/manifest.webmanifest`)
  with the agent's name and Nerve's app icon, so a browser can install it as an
  app. `apple-touch-icon.png` is the Home Screen icon on iPhone and iPad. Push
  notifications and browser alerts show the app icon, and Android shows a white
  N in the status bar. The worker
  has no fetch handler, so every page comes from the server.
- **Limits:** HTTPS only (and `localhost`). On iPhone and iPad, push works
  only after "Add to Home Screen" (iOS 16.4 or later), and a Safari tab says
  so. The instance must reach `fcm.googleapis.com` (Chrome and most Chromium
  browsers), `updates.push.services.mozilla.com` (Firefox),
  `*.push.apple.com` (Safari) and `*.notify.windows.com` (Edge). On Nerve
  Cloud, an agent in restricted internet mode cannot reach them, so Web Push
  works only for agents in open mode, and each agent has its own address, so
  a device turns push on for each agent. With several people on one agent,
  each person with push on gets every notification. No answer buttons.

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

### Cron Page
`/cron` opens on the sheet of the first job: the **Active** switch, **Run
now**, the schedule in words (`cronstrue` for crontab expressions), the next
run with its forecast, the latest run and the 7-day success count (for a
workflow job, the number of starts). **History**
lists the runs with ✓, ✗ or −, the duration and a link to the run's session,
and hides the source runs that found nothing new until you select **Show runs
with no new items**. See [The Cron page](cron.md#the-cron-page).

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
  - A platform skill has a **Platform** badge. Its detail view is read only, without Save or Delete.
