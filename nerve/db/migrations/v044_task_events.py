"""V44: status-transition history for tasks.

``tasks`` stores only the current status, so every transition overwrote
the last one and nothing recorded that it happened. The markdown file's
``## Updates`` section was the closest thing to a history, and it only
gains a line when a note is passed or on done/reopen — a plain status
flip left no trace at all, at day granularity and with no actor.

That was tolerable while status changes were rare and deliberate. The
task board makes them a drag, so they're frequent, and it raises the
questions this table answers: how long has this been in progress, which
cards are aging, how often does work bounce back out of review.

Rows are append-only. ``from_status`` is NULL for the row recording a
task's creation, so a task's first event is its birth rather than an
implicit gap before the first transition.

Timestamps are TEXT written Python-side as UTC ISO strings, matching every
other table here. They are not fixed width — ``isoformat()`` drops the
fractional part on an exact second — but lexicographic order is still
chronological, because the only characters that can follow the seconds
field are ``+`` (0x2B) and ``.`` (0x2E), and both sort below every digit.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

SQL = """
CREATE TABLE IF NOT EXISTS task_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    actor TEXT NOT NULL DEFAULT 'system',
    created_at TEXT NOT NULL
);

-- The only read pattern: one task's history, oldest first.
CREATE INDEX IF NOT EXISTS idx_task_events_task
    ON task_events(task_id, created_at);

-- Aging queries sweep by time across all tasks.
CREATE INDEX IF NOT EXISTS idx_task_events_created
    ON task_events(created_at);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)

    # Seed one event per existing task so history has a floor rather than
    # starting mid-air. created_at (not updated_at) is the honest stamp:
    # we know when the task appeared, not when it reached its current
    # status. The `backfill` actor is what marks these as synthesized — not
    # the NULL from_status, which a genuine creation records too.
    await db.execute(
        """
        INSERT INTO task_events (task_id, from_status, to_status, actor, created_at)
        SELECT id, NULL, status, 'backfill', COALESCE(created_at, updated_at)
        FROM tasks
        WHERE COALESCE(created_at, updated_at) IS NOT NULL
        """
    )

    async with db.execute("SELECT COUNT(*) FROM task_events") as cursor:
        seeded = (await cursor.fetchone())[0]
    logger.info("v044: task_events created, seeded %d origin row(s)", seeded)
