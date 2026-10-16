"""V57: record who schedules a wakeup and who starts a cron run.

Adds four nullable columns that hold an ``actor_refs.id``:

- ``session_wakeups.created_by_actor_id``: who scheduled the wakeup. A person
  who deferred a message with "Run later", or the system actor when the agent
  called ``ScheduleWakeup`` / ``schedule_wakeup`` in a turn.
- ``session_wakeups.caused_by_actor_id``: on whose behalf the wakeup turn
  runs. The person for "Run later", the cause of the turn that called the
  tool for an agent wakeup. The turn that the wakeup fires runs for this
  cause.
- ``cron_logs.actor_id``: who started the run. The system actor for a
  scheduled run, a catch-up run or a missed run; the person who used "Run
  now" through the REST API.
- ``cron_logs.caused_by_actor_id``: on whose behalf the run happens. The
  system actor for scheduled work, the person for "Run now".

**Existing rows stay NULL and nothing is backfilled.** NULL reads as "not
recorded". A wakeup row from before this migration fires with an unknown
cause.

The foreign keys follow v048: an ID that resolves to no actor can never be
stored, and NULL is exempt from the check. Each column is added only when the
table does not have it yet, so a second run does nothing.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

# (table, column, DDL type + constraints)
_COLUMNS = (
    ("session_wakeups", "created_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("session_wakeups", "caused_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("cron_logs", "actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("cron_logs", "caused_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
)


async def _has_column(db: aiosqlite.Connection, table: str, column: str) -> bool:
    async with db.execute(f"PRAGMA table_info({table})") as cursor:
        rows = await cursor.fetchall()
    return any(row[1] == column for row in rows)


async def up(db: aiosqlite.Connection) -> None:
    added = []
    for table, column, ddl in _COLUMNS:
        if await _has_column(db, table, column):
            continue
        # SQLite allows a REFERENCES clause on ADD COLUMN when the default is
        # NULL, so the table is not rebuilt and every existing row is NULL.
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
        added.append(f"{table}.{column}")
    logger.info(
        "v057: schedule attribution columns %s",
        ", ".join(added) if added else "already present",
    )
