"""V53: record the cause of a turn and who changed a task's status.

Adds three nullable columns that hold an ``actor_refs.id``:

- ``messages.caused_by_actor_id``: on a user row that starts a turn, the
  principal on whose behalf the turn runs. That is the person who sent the
  message, or the person whose request started work by the system actor, or
  the system actor for autonomous work. Assistant and tool output rows keep
  it ``NULL``.
- ``task_events.actor_id``: who changed the status. A person through the UI or
  REST, or the system actor for an agent tool call or another internal
  process.
- ``task_events.caused_by_actor_id``: on whose behalf the change was made.

``task_events.actor`` stays. It is the origin of the change (``web``, a
session ID, ``system``, ``backfill``), not an identity.

**Existing rows stay NULL and nothing is backfilled.** NULL reads as "not
recorded". The free-text origin of an old task event does not name a person,
so a backfill would invent attribution.

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
    ("messages", "caused_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("task_events", "actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("task_events", "caused_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
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
        "v053: cause and task attribution columns %s",
        ", ".join(added) if added else "already present",
    )
