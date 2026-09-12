"""V48: who created a session, and who sent a message.

The expand half of prospective attribution (RFC 10.2 steps 1 and 4). Two
nullable columns, both holding an ``actor_refs.id``:

- ``sessions.created_by_actor_id`` — the principal that caused the session row
  to exist: the person who asked for it, or the agent's system principal for
  the sessions the instance mints for itself (cron generations, workflow legs,
  MCP satellites, ingested Codex threads, channel conversations).
- ``messages.actor_id`` — the principal that supplied the message's content as
  *input*. Assistant and tool output keeps its own authorship in ``role`` and
  is left ``NULL``; the optional ``caused_by_actor_id`` of RFC section 8 is
  deliberately not added here.

**Existing rows stay NULL and nothing is backfilled.** A legacy session's
``source`` string and a legacy message's ``channel`` are provenance, not
identity bindings, so inventing an actor from them would fabricate audit
history (RFC 10.2 step 3). `NULL` reads as "this predates attribution", which
is a true statement and the one the UI renders.

Both columns reference ``actor_refs(id)``. The reference can never dangle:
``actor_refs`` rows are never deleted, and since local accounts are tombstoned
rather than removed, neither are the accounts that own them. What the foreign
key buys is that an id which resolves to nothing can never be stored — an
unresolvable actor id is attribution that silently renders as a blank name.
NULL is exempt from foreign-key checks, so unattributed rows are unaffected.

Idempotent: each column is added only if the table does not already have it,
so a re-run against a database that already has them is a no-op rather than a
"duplicate column name" failure.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

# (table, column, DDL type + constraints)
_COLUMNS = (
    ("sessions", "created_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("messages", "actor_id", "TEXT REFERENCES actor_refs(id)"),
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
        # SQLite allows a REFERENCES clause on ADD COLUMN as long as the
        # column's default is NULL, which is exactly the shape wanted here:
        # no table rebuild, and every existing row is left unattributed.
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
        added.append(f"{table}.{column}")
    logger.info(
        "v048: attribution columns %s",
        ", ".join(added) if added else "already present",
    )
