"""V66: the session that created a task.

Adds ``tasks.created_session_id``. ``upsert_task`` sets it when it inserts a
row and its ``actor`` is the ID of a session, that is, when the agent creates
the task with ``task_create``. It is NULL for a task created through the HTTP
API (actor ``web``), by a reindex (actor ``system``), or before this column.

Backfill: a task's creation event in ``task_events`` (the row with a NULL
``from_status``) has the creating actor. The backfill copies that actor only
when a row in ``sessions`` has it as its ID. Thus ``web``, ``system``, the
v044 ``backfill`` rows, a person's actor ID and a deleted session give NULL.
When a task has more than one creation event, the earliest one is used.

Adds two indexes for the session-to-tasks read: on
``tasks(created_session_id)`` and on ``task_events(actor)``.

The column check, ``IF NOT EXISTS`` and ``WHERE created_session_id IS NULL``
make a second run do nothing.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

INDEXES = """
CREATE INDEX IF NOT EXISTS idx_tasks_created_session
    ON tasks(created_session_id);

CREATE INDEX IF NOT EXISTS idx_task_events_actor
    ON task_events(actor);
"""

BACKFILL = """
UPDATE tasks SET created_session_id = (
    SELECT e.actor FROM task_events e
    JOIN sessions s ON s.id = e.actor
    WHERE e.task_id = tasks.id AND e.from_status IS NULL
    ORDER BY e.created_at, e.id
    LIMIT 1
)
WHERE created_session_id IS NULL
"""


async def up(db: aiosqlite.Connection) -> None:
    async with db.execute("PRAGMA table_info(tasks)") as cursor:
        columns = {row[1] for row in await cursor.fetchall()}
    if "created_session_id" not in columns:
        await db.execute("ALTER TABLE tasks ADD COLUMN created_session_id TEXT")
    await db.executescript(INDEXES)
    cursor = await db.execute(BACKFILL)
    await cursor.close()
    async with db.execute(
        "SELECT COUNT(*) FROM tasks WHERE created_session_id IS NOT NULL"
    ) as cursor:
        linked = (await cursor.fetchone())[0]
    logger.info("v066: tasks.created_session_id present, %d task(s) linked", linked)
