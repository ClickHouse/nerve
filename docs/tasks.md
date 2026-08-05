# Task System

## Overview

Tasks are stored as markdown files with a SQLite index for querying. The agent manages tasks via built-in MCP tools.

## File Structure

```
workspace/memory/tasks/
├── active/
│   ├── 2026-02-25-fix-auth.md
│   └── 2026-02-26-review-pr.md
└── done/
    └── 2026-02-20-setup-cron.md
```

## Task File Format

```markdown
# Fix Auth Token Expiry

**Source:** https://github.com/...
**Deadline:** 2026-02-28

Context and details here...

## Updates
- 2026-02-25: Created
- 2026-02-26: Started investigation
- 2026-02-27: DONE — Fixed in PR #123
```

## Task ID

Generated from date + slugified title: `2026-02-25-fix-auth-token-expiry`

## Statuses

- `pending` — Not started
- `in_progress` — Being worked on
- `done` — Completed (file moved to `done/`)
- `deferred` — Postponed

## Agent Tools

| Tool | Description |
|------|-------------|
| `task_create` | Create task with duplicate detection (writes .md + inserts SQLite row) |
| `task_search` | Full-text search on title + content (FTS5) |
| `task_list` | List tasks with status filter (queries SQLite) |
| `task_update` | Update status/deadline/notes (updates both) |
| `task_read` | Read full task content |
| `task_done` | Mark complete, move to `done/` |

### Duplicate Detection

`task_create` automatically checks for potential duplicates before creating a task:

1. **Primary: `source_url` exact match** — If the task has a `source_url` (e.g., a GitHub issue URL), checks for any existing task with the same URL. This is the most reliable dedup for source-generated tasks, since the agent may paraphrase titles differently each time.
2. **Fallback: Fuzzy FTS5 search** — Uses OR semantics (any word can match) ranked by BM25 relevance. This catches similar tasks even with different wording — e.g., "database backup failed" matches "Fix database backup failure". Stop words and short tokens (≤1 char) are stripped to reduce noise.

If matches are found, the tool returns them and refuses to create — the caller must re-invoke with `confirm_duplicate=true` to override.

### Search

`task_search` performs an FTS5 full-text search on task titles and content using AND semantics (all words must match). Supports an optional status filter (`all` to include done tasks, specific status, or empty for open tasks only).

Note: `task_search` (user-facing) uses strict AND matching for precision. Duplicate detection (internal) uses fuzzy OR matching for recall — these are intentionally different trade-offs.

### Status Transitions

Setting a task's status to `done` via `task_update` automatically delegates to `task_done`, which:
- Moves the markdown file from `active/` to `done/`
- Syncs the FTS index
- Appends a completion note

This prevents orphan tasks (status=done in DB but file still in active/).

Leaving `done` delegates the same way, to the inverse path: `task_update`
routes a task whose stored status is `done` through the reopen handler, which
moves the file back to `active/` and appends a `REOPENED` line. Both
directions check the tracked-config guard *before* the status flip, so a
refused move never leaves a task whose status and file disagree.

The inverse matters because the disagreement is silent rather than loud.
`reindex()` treats a file under `done/` as terminal by definition, so a row
pointing there with an active status is an orphan it force-resets back to
`done` — a status change that appeared to work would quietly undo itself the
next time anything reindexed. A *missing* file is worse: `reindex()` only
walks files that exist, so it never sees that row to repair it. Hence the
reopen refuses outright when the file is already gone.

### Ordering (`position`)

Board lanes are hand-ordered, so the order is stored rather than derived:
`tasks.position` is a sparse REAL rank, ascending (lower sorts higher in the
lane), added in migration v043 and backfilled per status.

A move sends *intent* — "put this card between A and B" — and the server takes
the midpoint of the two neighbours' ranks. One drag is one UPDATE, with no
renumbering of the lane. If repeated midpoint inserts ever exhaust float
precision between two neighbours, that lane is re-spaced at even intervals and
the move retried.

Two rules keep ranks meaningful:

- **Preserved on omit.** `upsert_task(position=None)` keeps the stored rank.
  Nothing in the markdown file encodes a rank, so callers that rebuild a row
  from disk (`reindex`, `task_write`, the PATCH route) *cannot* supply one —
  under replace semantics, appending a note would silently reset the card's
  place in its lane.
- **Re-ranked across lanes.** A rank only means something relative to its own
  lane, so a status change places the card at the top of its destination
  instead of carrying over a number it was never ordered against.

### Live Updates

Every task mutation broadcasts a `task_updated` WebSocket event on the
`__global__` channel carrying the whole row (`event` is one of `created`,
`updated`, `moved`, `done`). It fires from both the HTTP routes and the tool
handlers, so a card moves on any open board whether the change came from the
web UI, another tab, or the agent working in an unrelated session. Broadcast
failures are logged and swallowed — a stale card is never worth failing a
write over.

### FTS Index

Tasks are indexed in an FTS5 virtual table (`tasks_fts`) for fast full-text search. The index is synced on every `upsert_task()` call. On startup, an integrity check compares task count vs FTS count — if they diverge, the index is automatically reseeded from the database.

## Escalation

When a task has a deadline, reminders escalate:

| Level | Trigger | Label |
|-------|---------|-------|
| 1 | At deadline | Reminder |
| 2 | +30 minutes | Follow-up |
| 3 | +2 hours | URGENT |

Escalation respects quiet hours (configurable, default 2AM-12PM).

## Web UI

### Task List (`/tasks`)

Tasks are listed with status filter buttons and a search input (250ms debounce). Status can be changed via dropdown on each card. Clicking a task card navigates to the detail page.

### Task Detail (`/tasks/:taskId`)

Full-page markdown editor for task content:

- **Edit / Preview toggle** — raw markdown editing or rendered preview
- **Ctrl+S / Cmd+S** to save; Save button appears when content is modified
- **Status dropdown** — change status inline in the header
- **Metadata** — deadline, source, external link displayed in header
- **Back button** — returns to the task list

Saving writes the full markdown file to disk and re-syncs the title and deadline to SQLite.
