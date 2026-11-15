"""V69: add ``created_at`` to the index for the pending-plan count.

The session list counts the pending plans of each proposing session that
are newer than ``sessions.plan_attention_days``:
``WHERE status = 'pending' AND created_at >= ? GROUP BY session_id``. An
index on ``(status, session_id, created_at)`` holds every column of that
query, in group order, so SQLite reads only the pending entries of the
index and no table rows. The v063 index on ``(status, session_id)`` is a
prefix of the new one, so it goes.

``IF NOT EXISTS`` and ``IF EXISTS`` make a second run do nothing.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_plans_status_session_created"
        " ON plans(status, session_id, created_at)"
    )
    await db.execute("DROP INDEX IF EXISTS idx_plans_status_session")
    logger.info("v069: plans index on (status, session_id, created_at) present")
