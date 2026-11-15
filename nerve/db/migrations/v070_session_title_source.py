"""V70: who chose a session's title.

Adds ``sessions.title_source``:

- ``placeholder``: the first words of the first message, until a model title arrives.
- ``generated``: the model title made from the first message.
- ``fork``: the "Fork of …" title a fork starts with.
- ``outcome``: the model title made from what the session did
  (``agent.outcome_titles``, or Regenerate name).
- ``manual``: a person renamed the session.
- NULL: the title the code that created the session gave it, or none yet.

Nerve's own title writes check it, so a model title that arrives after a
rename never replaces the person's title.

Existing rows take the source from their last ``title_changed`` event: its
``reason`` when that is ``placeholder`` or ``generated``, and ``manual`` when
the event has no reason. Rows without such an event stay NULL. The column
check makes a second run do nothing.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

BACKFILL = """
UPDATE sessions SET title_source = (
    SELECT CASE
        WHEN json_extract(e.details, '$.reason') IS NULL THEN 'manual'
        WHEN json_extract(e.details, '$.reason') IN ('placeholder', 'generated')
            THEN json_extract(e.details, '$.reason')
    END
    FROM session_events e
    WHERE e.session_id = sessions.id AND e.event_type = 'title_changed'
    ORDER BY e.id DESC LIMIT 1
)
"""


async def _has_column(db: aiosqlite.Connection, table: str, column: str) -> bool:
    async with db.execute(f"PRAGMA table_info({table})") as cursor:
        rows = await cursor.fetchall()
    return any(row[1] == column for row in rows)


async def up(db: aiosqlite.Connection) -> None:
    if await _has_column(db, "sessions", "title_source"):
        return
    await db.execute("ALTER TABLE sessions ADD COLUMN title_source TEXT")
    await db.execute(BACKFILL)
    logger.info("v070: sessions.title_source added and backfilled")
