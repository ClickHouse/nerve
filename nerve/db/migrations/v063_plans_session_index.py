"""V63: an index for the pending-plan count per session.

The session list counts the pending plans of each proposing session with
``WHERE status = 'pending' GROUP BY session_id``. An index on
``(status, session_id)`` holds every column of that query, in group order,
so SQLite reads only the pending entries of the index and no table rows.

``CREATE INDEX IF NOT EXISTS`` makes a second run do nothing.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_plans_status_session"
        " ON plans(status, session_id)"
    )
    logger.info("v063: plans index on (status, session_id) present")
