"""V27: Add last_rotated_at to sessions for daily context rotation tracking.

Persistent cron sessions (e.g. inbox-processor) rotate their SDK context once
per day at a configured local time (`context_rotate_at`).  The previous
implementation compared `connected_at` to today's rotate time, but
`connected_at` is reset every time the session reconnects (including after
every nerve restart).  After any restart that lands past the rotate time, the
predicate `connected_at < today_rotate_utc` becomes false for the rest of the
day, so rotation never fires.

This column tracks when the session was last rotated independently from the
connect lifecycle, fixing the race.  `NULL` means "never rotated" — treated
the same as "rotated before any past rotate-at boundary" so first-time
rotation still fires correctly.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript("""
        ALTER TABLE sessions ADD COLUMN last_rotated_at TEXT;
    """)
    logger.info("v027: added sessions.last_rotated_at for daily rotation tracking")
