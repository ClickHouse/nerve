"""V61: comments on tasks, with the person or agent that wrote each one.

Adds the table ``task_comments``. One row is one comment: a markdown
``body``, the task it belongs to, and two nullable ``actor_refs`` IDs:

- ``actor_id``: who wrote the comment. A person through the UI or REST, or
  the system actor for an agent tool call.
- ``caused_by_actor_id``: on whose behalf it was written. For a person, the
  person. For the agent, the cause of its session.

Comments live here and not in the task's markdown file. A file can be edited
by anyone with access to the workspace, so it cannot hold a trustworthy
author, and an agent that rewrites the file would rewrite the comments too.

Adds ``tasks.comment_count``. The comment store keeps it equal to the number
of rows for the task, in the same transaction as each insert and delete, so
every reader of a task row (the board, the list, the task broadcast) has the
count with no extra query. ``NOT NULL DEFAULT 0`` is correct for every
existing row, because no task has comments yet.

The foreign keys follow v048: an ID that resolves to no actor, or to no
task, can never be stored. NULL is exempt from the check. ``CREATE ... IF NOT
EXISTS`` and the column check make a second run do nothing.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

SQL = """
CREATE TABLE IF NOT EXISTS task_comments (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id            TEXT NOT NULL REFERENCES tasks(id),
    body               TEXT NOT NULL,
    actor_id           TEXT REFERENCES actor_refs(id),
    caused_by_actor_id TEXT REFERENCES actor_refs(id),
    created_at         TEXT NOT NULL
);

-- The only read pattern: one task's comments in ``id`` order. An index on
-- ``task_id`` holds the rowid too, so it serves that order.
CREATE INDEX IF NOT EXISTS idx_task_comments_task
    ON task_comments(task_id);
"""


async def _has_column(db: aiosqlite.Connection, table: str, column: str) -> bool:
    async with db.execute(f"PRAGMA table_info({table})") as cursor:
        rows = await cursor.fetchall()
    return any(row[1] == column for row in rows)


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    if not await _has_column(db, "tasks", "comment_count"):
        await db.execute(
            "ALTER TABLE tasks ADD COLUMN comment_count INTEGER NOT NULL DEFAULT 0"
        )
    logger.info("v061: task_comments ready, tasks.comment_count present")
