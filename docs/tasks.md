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

The slug is the title truncated to 40 characters, so two titles that differ only past that cut collide on the same day. `task_create` never writes over an existing task: an ID already held by a task row, or by a file in `active/` or `done/`, is skipped and the new task takes the next free suffix (`2026-02-25-fix-auth-token-expiry-2`). Completed tasks keep their IDs reserved, so a finished task is never resurrected as a new one. Past 99 suffixes on one base the create is refused instead. The response names the ID that was actually claimed — use that one, not an ID derived from the title.

## Statuses

Statuses are configurable (`task_status_list`, `task_status_create`, and the board's status manager). These ship with Nerve:

- `backlog`: not ready for work yet. No built-in cron job or gate selects it, so the agent does not pick it up. The New Task dialog uses it as its default.
- `pending`: ready for work. The default of `task_create`. The `task-planner` job and the example work jobs gate on it.
- `plan_review`: a plan for the task waits for a human decision. `plan_propose` and `plan_update` move a `pending`, `backlog` or `in_progress` task to it ([plans.md](plans.md)).
- `in_progress`: being worked on. Approving a plan moves the task to it.
- `done`: completed. The file moves to `done/`.
- `deferred`: postponed.

`pending`, `plan_review` and `done` are protected and cannot be deleted. Migration v059 adds `backlog` and `plan_review` to an existing install, before and after `pending`; a status that already has one of these names stays as it is.

An install that existed before keeps the `task-planner` prompt in its `config/cron/system.yaml`, which lists all open tasks, until the Automation step writes that file again. On such an install the planner can propose a plan for a `backlog` task.

## Agent Tools

| Tool | Description |
|------|-------------|
| `task_create` | Create task with duplicate detection (writes .md + inserts SQLite row) |
| `task_search` | Full-text search on title + content (FTS5) |
| `task_list` | List tasks with status filter (queries SQLite) |
| `task_update` | Update status/deadline/notes (updates both) |
| `task_read` | Read full task content |
| `task_done` | Mark complete, move to `done/` |
| `task_comment` | Add a comment to a task, signed as the agent (see [Comments](#comments)) |

### Duplicate Detection

`task_create` checks what a new task would double before it writes anything, and refuses in two cases:

1. **Same `source_url`** — a task already tracks that URL. The URL is the identity of a source-generated item (an issue, a PR, an email), whose title the agent may word differently each time. Pass the most specific URL you have — a comment permalink for a directive, the issue URL for the issue itself — or several deliberate tasks under one issue will each need `confirm_duplicate`.
2. **Nearly the same title** — the title scores ≥ 0.85 against an existing task (done ones included). The score is IDF-weighted token overlap over all task titles: words every title of the install shares weigh nothing, the words that identify the work (a test name, an issue number, a function) weigh the most, and dates, small numbers and time stamps are ignored. It is 0..1 and does not depend on the size of the corpus or the length of the title, so the threshold holds on a ten-task install and a ten-thousand-task one.

The refusal lists each match with its status, its similarity and the words the titles share. Tasks that merely look alike never block: those scoring ≥ 0.5, and those that share a **rare identifier** with the new title (a test name, an issue number, a symbol — a token with a digit or underscore that appears in at most five titles) whatever their score, are listed as **related** — under the duplicates on a refusal, after the new ID on a success — so the caller can link or merge them. `confirm_duplicate=true` means "create a second task": the new task gets its own ID and file, and never replaces the one it resembles; the look-alikes are still reported with it.

Earlier versions matched the new title against every task's title *and body* with an FTS5 OR query and a fixed BM25 cut-off. BM25 grows with query length and corpus size, so on a large install the cut-off fell below the score of one shared rare word and every create was refused; the title index replaced it.

### Search

`task_search` performs an FTS5 full-text search on task titles and content using AND semantics (all words must match). Supports an optional status filter (`all` to include done tasks, specific status, or empty for open tasks only).

Note: `task_search` (user-facing) uses strict AND matching for precision. Duplicate detection does not use the FTS index at all — it scores titles against titles (see above), so it keeps working when the index is being rebuilt.

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

### Status History

Every status transition is appended to `task_events` (`task_id`,
`from_status`, `to_status`, `actor`, `actor_id`, `caused_by_actor_id`,
`created_at`), added in migration v044 and seeded with one origin row per
existing task. `from_status` is NULL on
the row recording a task's creation, so an aging calculation can tell
"created here" from "moved here".

Recording happens inside the same transaction as the status write, from all
three paths that can change one: `transition_task` (which `update_task_status`
wraps), `move_task`, and the full-row `upsert_task` (which is how
`task_update` flips status when it also
writes a note). A no-op transition records nothing — that single rule is
what keeps `reindex()` from doubling the table on every run, since it
rewrites every row with the status it already had. The one case where
reindex *does* change a status, resetting an orphaned row to match its
directory, is a real correction and gets a row.

`actor` is the origin of the change, not an identity: the session id for
agent-driven changes, `web` for the HTTP API, `backfill` on the origin rows
v044 seeded for tasks that predate the table, and `system` otherwise.
`backfill` is load-bearing rather than cosmetic: a real creation also records
a NULL `from_status`, so the actor is the only thing distinguishing a
synthesized origin from one that actually happened.

`actor_id` and `caused_by_actor_id` (migration v053) name who made the change
and on whose behalf, as actor IDs (see
[Attribution](accounts.md#attribution)). A change through the HTTP API records
the person as both. A change by an agent tool call records the system actor
and the cause of its session: the person who started the session, or the
system actor for a cron job. Rows written before v053 have NULL in both
columns. The `GET /api/tasks/{id}/events` rows include both columns, and the
timeline shows "Nerve for <person>" when the agent changed the status in a
session that the person started.

Changes to the set of statuses (create, update, reorder, delete, through the
HTTP API or the `task_status_create` tool) append a row to
`task_status_events` (`change`, `status_name`, `detail`, `actor_id`,
`caused_by_actor_id`, `created_at`), added in migration v053. `status_name` is
NULL for a reorder, and `detail` is a JSON object with the new values.

This powers the aging indicator on board cards and the timeline in the task
detail view. Tasks whose last transition predates v044 report no entry time
rather than a guessed one, so the UI stays silent instead of showing an age
that isn't true.

### Revisions (`revision`)

Every write to a task row advances `tasks.revision` by one, added in
migration v052. The row routes return it, and `task_read` ends with a
`<!-- nerve: revision=N ... -->` line that `task_write` strips again.

Changes of one task that write its file and its row run one at a time: the
`PATCH` and `move` routes, `task_update`, `task_write`, `task_done` and the
reopen path hold a lock per task from their first read of the row to their
last write (`nerve/tasks/locks.py`). So an edit always lands in the file that
the row names, also when another change moves the file into or out of
`done/` at the same time. Changes of different tasks do not wait for each
other.

Pass the value you read as `expect_revision` to `task_update` or `task_done`
to make the write conditional: if the row has changed since, nothing is
written and the tool reports a conflict, so re-read and retry. Without it,
writes stay last-write-wins, except that a reopen is always conditional on
the task still being done. In code, `upsert_task`, `update_task_tags` and
`transition_task` take the same argument and return `False` instead of
writing.

The fence protects the row. Two writers that both pass it can still lose one
another's edits to the markdown body, since each builds the new body from its
own read of the file.

### Comments

People and the agent can add comments to a task. A comment is markdown text
of at most 10,000 characters. Comments are rows in `task_comments` (`id`,
`task_id`, `body`, `actor_id`, `caused_by_actor_id`, `created_at`), added in
migration v061. They are not in the task's markdown file: a file cannot hold a
trustworthy author, and an agent that rewrites the file would rewrite the
comments too.

`actor_id` is who wrote the comment and `caused_by_actor_id` on whose behalf
(see [Attribution](accounts.md#attribution)). A comment through the HTTP API
records the caller as both: the person, or the system actor for a system or
MCP token. A `task_comment` tool call records the system
actor and the cause of its session, so the web UI shows "Nerve for <person>"
when the agent wrote it in a session that the person started.

Adding or deleting a comment also updates `tasks.comment_count`,
`updated_at` and `revision` in the same transaction. Thus the count is on
every task row, the `task_updated` broadcast (`event` `commented` or
`comment_deleted`) refreshes open task views, and a writer that read the task
before the comment gets a revision conflict on a guarded write.

`task_read` shows the newest 50 comments between the file content and the
revision line, inside a `<!-- nerve: comments ... -->` block, with the time,
the author and the text of each. `task_write` strips the block, so a
read-edit-write round trip does not copy comments into the file. In the
`task_read` output, a file line that starts with `<!-- nerve:` is indented by
two spaces, so only Nerve's own block and revision line start with a marker:
file content cannot pose as comments, and the strip cannot remove file
content. Such a line stays indented after a `task_write` round trip; markdown
still reads it as an HTML comment.

Only the author of a comment can delete it, through
`DELETE /api/tasks/{id}/comments/{comment_id}`. Accounts have no roles, so
no person can delete the words of another. Comments cannot be edited.

### Sessions (`created_session_id`)

`tasks.created_session_id` (migration v066) is the session that created the
task. `task_create` in an agent session sets it. It is NULL for a task created
through the HTTP API or by a reindex, because their actor (`web`, `system`) is
not a session ID. For tasks that existed before v066, the migration copies the
actor of the creation event in `task_events`, but only when a session with that
ID exists.

Task rows from the list, search, board and detail routes, and the
`task_updated` broadcast, have `created_session_title`: the title of that
session, or its ID when it has no title. It is `null` when the session does not
exist, for example after a delete.

A session relates to a task in one of three ways, in this order:

| Relation | Source |
|---|---|
| `created` | `tasks.created_session_id` |
| `implemented` | `plans.impl_session_id` of a plan of the task |
| `updated` | `task_events.actor`: the session changed the status |

A session that relates in more than one way gets the first relation. The task
detail (`GET /api/tasks/{id}`) lists the sessions of the task in `sessions`,
and `GET /api/sessions/{id}/tasks` lists the tasks of a session. Both include
only sessions that exist. A change that keeps the status (a note, a content
edit) records no event, so it does not make a session `updated`.

### Live Updates

Every task mutation broadcasts a `task_updated` WebSocket event on the
`__global__` channel carrying the whole row (`event` is one of `created`,
`updated`, `moved`, `done`, `commented`, `comment_deleted`). It fires from both the HTTP routes and the tool
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

The escalation check pauses during quiet hours when they are set (`quiet_start`/`quiet_end`, see [config.md](config.md#quiet-hours)); with none set, a worker's default, it never pauses.

## Web UI

### Task List (`/tasks`)

Tasks are listed with status filter buttons and a search input (250ms debounce). Status can be changed via dropdown on each card. Clicking a task card navigates to the detail page.

The New Task dialog has a status picker with the configured statuses. It starts on `backlog`, or on the status of the board column whose add button opened it, and falls back to `pending` when `backlog` does not exist. A hint under the picker says that the agent may pick up a `pending` task.

Task notes that the plan tools write link to the plan (`/plans/<id>`) and to the implementation session (`/chat/<id>`). The markdown preview opens these links in the app.

### Task Detail (`/tasks/:taskId`)

Full-page markdown editor for task content:

- **Edit / Preview toggle** — raw markdown editing or rendered preview
- **Ctrl+S / Cmd+S** to save; Save button appears when content is modified
- **Status dropdown** — change status inline in the header. It is the only display of the status, with the status colour on its background and border
- **Metadata** — deadline, source, external link displayed in header. On a phone, they go in a second row under the status and the buttons
- **Created in**: when the session that created the task exists, "from <session title>" links to its chat in place of the source. Task cards on the board and in the list show the same link
- **Sessions**: below the content in preview mode, the sessions that created, implemented or updated the task, each with a link to its chat
- **Plan**: the latest plan of the task (`GET /api/tasks/{id}/plans`), with its version, its status and **Open plan**. A pending plan with no revision request shows one row, "Plan vN waits for your review", with **Review**; the full row opens the plan. The section reads the plans again when the task changes, for example when `plan_propose` writes its note
- **Comments**: below the content in preview mode. Each comment shows its author ("Nerve for <person>" for an agent comment), its age and its markdown text. **Comment** (or Ctrl+Enter / Cmd+Enter) adds one; the delete button shows on your own comments only. The list reads the comments again when the task changes, so a comment by another person or by the agent shows without a reload. Board cards show the comment count
- **Back button** — returns to the task list

Saving writes the full markdown file to disk and re-syncs the title and deadline to SQLite.
