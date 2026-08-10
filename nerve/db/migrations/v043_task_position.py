"""V43: manual ordering rank for the task board.

The board lets a card be dragged to an arbitrary slot inside its lane, so
lane order becomes user-owned state rather than something derived from
deadline/created_at. ``position`` stores it as a *sparse* REAL rank: a
move takes the midpoint of its two neighbours (see
:meth:`nerve.db.tasks.TaskStore.move_task`), so one drag is one UPDATE
instead of a renumber of the whole lane.

Existing rows are backfilled per status, walking the order the task list
already used (deadline first, then newest) with a wide gap between ranks.
Without the backfill every row would sit at the column default of 0 and
lane order would be arbitrary until each card had been dragged once.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

# Spacing between backfilled ranks. Keep in sync with
# ``nerve.db.tasks.POSITION_GAP`` — duplicated rather than imported so the
# migration keeps describing the schema as it was at v43 even if the
# runtime constant is retuned later.
_GAP = 1024.0


async def up(db: aiosqlite.Connection) -> None:
    await db.execute("ALTER TABLE tasks ADD COLUMN position REAL NOT NULL DEFAULT 0")
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_tasks_status_position "
        "ON tasks(status, position)"
    )

    # One pass over every task, grouped by lane. The secondary keys match
    # ``TaskStore._SORT_CLAUSES["deadline"]`` so the board's initial order
    # is the order people were already looking at.
    async with db.execute(
        "SELECT id, status FROM tasks "
        "ORDER BY status ASC, deadline ASC NULLS LAST, created_at DESC, id DESC"
    ) as cursor:
        rows = await cursor.fetchall()

    updates: list[tuple[float, str]] = []
    current_lane: object = object()  # sentinel: never equal to a status string
    rank = 0.0
    for task_id, status in rows:
        if status != current_lane:
            current_lane = status
            rank = 0.0
        rank += _GAP
        updates.append((rank, task_id))

    if updates:
        await db.executemany("UPDATE tasks SET position = ? WHERE id = ?", updates)

    logger.info("v043: added tasks.position, backfilled %d row(s)", len(updates))
