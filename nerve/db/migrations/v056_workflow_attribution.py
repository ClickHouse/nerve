"""V56: record who starts, kills and decides workflow runs and review loops.

Adds nullable columns that hold an ``actor_refs.id``:

- ``workflow_runs.created_by_actor_id``, ``review_loops.created_by_actor_id``:
  who created the run or the loop. A person through the web UI or REST, or
  the system actor for a tool call, a cron job or a review-loop leg.
- ``workflow_runs.caused_by_actor_id``, ``review_loops.caused_by_actor_id``:
  on whose behalf. A run's session (and each leg session of a loop) is
  created with this cause, so the agent work in it names the person who
  caused it.
- ``workflow_runs.killed_by_actor_id``, ``review_loops.killed_by_actor_id``:
  who killed the run or the loop.
- ``workflow_runs.killed_caused_by_actor_id``,
  ``review_loops.killed_caused_by_actor_id``: on whose behalf it was killed.
  A kill through a tool records the system actor and the calling session's
  cause.

Adds the table ``review_loop_decisions``. A decision on a parked loop (accept,
abandon, more iterations, more budget, adopt and continue, remind later)
changes the loop row and leaves no other record, so one row per decision keeps
the history: the decision, whether it was applied, the result message, the
origin (``decided_by``: ``api`` or ``approval-card``), and the actor and cause.
A decision that arrives when the loop is not parked is recorded with
``applied`` 0.

The free-text origin columns stay and are written as before.
``workflow_runs.created_by`` is also the pointer from a run to the session it
was started from, so its format does not change.

**Existing rows stay NULL and nothing is backfilled.** NULL reads as "not
recorded". The free-text columns of an old row do not name a person, so a
backfill would invent attribution.

The foreign keys follow v048: an ID that resolves to no actor can never be
stored, and NULL is exempt from the check. Each column is added only when the
table does not have it yet, and the table only when it does not exist, so a
second run does nothing.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

# (table, column, DDL type + constraints)
_COLUMNS = (
    ("workflow_runs", "created_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("workflow_runs", "caused_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("workflow_runs", "killed_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("workflow_runs", "killed_caused_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("review_loops", "created_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("review_loops", "caused_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("review_loops", "killed_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
    ("review_loops", "killed_caused_by_actor_id", "TEXT REFERENCES actor_refs(id)"),
)

# ``decided_by`` is the origin text (``api``, ``approval-card``); ``applied``
# is 1 when the decision changed the loop.
_DECISIONS_SQL = """
CREATE TABLE IF NOT EXISTS review_loop_decisions (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    loop_id            TEXT NOT NULL REFERENCES review_loops(id) ON DELETE CASCADE,
    decision           TEXT NOT NULL,
    applied            INTEGER NOT NULL DEFAULT 0,
    message            TEXT NOT NULL DEFAULT '',
    decided_by         TEXT NOT NULL DEFAULT '',
    actor_id           TEXT REFERENCES actor_refs(id),
    caused_by_actor_id TEXT REFERENCES actor_refs(id),
    created_at         TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_review_loop_decisions_loop
    ON review_loop_decisions(loop_id, id);
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
    await db.executescript(_DECISIONS_SQL)
    logger.info(
        "v056: workflow run and review loop attribution columns %s; "
        "review_loop_decisions table present",
        ", ".join(added) if added else "already present",
    )
