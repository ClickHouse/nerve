"""V53: record the cause of a session and who changes tasks and task statuses.

Adds three nullable columns that hold an ``actor_refs.id``:

- ``sessions.caused_by_actor_id``: the session's cause, the principal on
  whose behalf all agent work in the session runs. It is set once when the
  session is created and never changes: the person who started the session,
  the system actor for a session that the instance starts for itself, or
  ``NULL`` when unknown (channel input with no known person, an imported or
  MCP satellite session).
- ``task_events.actor_id``: who changed the status. A person through the UI or
  REST, or the system actor for an agent tool call or another internal
  process.
- ``task_events.caused_by_actor_id``: on whose behalf the change was made.

``task_events.actor`` stays. It is the origin of the change (``web``, a
session ID, ``system``, ``backfill``), not an identity.

Adds the table ``task_status_events``: one append-only row for each change to
the set of task statuses (``created``, ``updated``, ``reordered``,
``deleted``), with its actor and cause. A status definition row can be
deleted, so its history cannot live on the row.

**Existing rows stay NULL and nothing is backfilled.** NULL reads as "not
recorded". The free-text origin of an old task event or the creator of an old
session does not prove a cause, so a backfill would invent attribution.

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
    ("sessions", "caused_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("task_events", "actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("task_events", "caused_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
)

# ``status_name`` is NULL for ``reordered``, which changes the order of all
# statuses; ``detail`` is a JSON object with the changed values.
_STATUS_EVENTS_SQL = """
CREATE TABLE IF NOT EXISTS task_status_events (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    change             TEXT NOT NULL,
    status_name        TEXT,
    detail             TEXT,
    actor_id           TEXT REFERENCES actor_refs(id),
    caused_by_actor_id TEXT REFERENCES actor_refs(id),
    created_at         TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_status_events_created
    ON task_status_events(created_at);
"""


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
    await db.executescript(_STATUS_EVENTS_SQL)
    logger.info(
        "v053: cause and task attribution columns %s; task_status_events ready",
        ", ".join(added) if added else "already present",
    )
