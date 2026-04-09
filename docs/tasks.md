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
