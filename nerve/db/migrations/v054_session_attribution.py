"""V54: record who changes a session, and who deletes it.

Adds two nullable columns to ``session_events`` that hold an
``actor_refs.id``:

- ``session_events.actor_id``: who made the change that the event records. A
  person through the web UI, REST or the browser WebSocket, or the system
  actor for a change by the agent or by Nerve (a generated title, an
  automatic archive).
- ``session_events.caused_by_actor_id``: on whose behalf the change was made.

Events that record the state of the runtime (``started``, ``idle``,
``error``, a ``stopped`` status after a cancel) are not changes by a
principal and keep both columns ``NULL``.

Adds the table ``session_deletions``: one row for each deleted session, with
its last title, the time of the delete, and its actor and cause. A delete
removes the session's events with the session, so the record of the delete
cannot be a session event. The row is written in the same transaction as the
delete. A later delete of a session with the same ID replaces the row.

**Existing rows stay NULL and nothing is backfilled.** NULL reads as "not
recorded".

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
    ("session_events", "actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("session_events", "caused_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
)

# ``session_id`` has no foreign key: the session row is gone.
_DELETIONS_SQL = """
CREATE TABLE IF NOT EXISTS session_deletions (
    session_id         TEXT PRIMARY KEY,
    title              TEXT,
    deleted_at         TEXT NOT NULL,
    actor_id           TEXT REFERENCES actor_refs(id),
    caused_by_actor_id TEXT REFERENCES actor_refs(id)
);

CREATE INDEX IF NOT EXISTS idx_session_deletions_deleted
    ON session_deletions(deleted_at);
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
    await db.executescript(_DELETIONS_SQL)
    logger.info(
        "v054: session attribution columns %s; session_deletions ready",
        ", ".join(added) if added else "already present",
    )
