"""V25: Hot-path indexes for cron_logs and messages.

The diagnostics page and cron log views were doing in-memory sorts of
the entire ``cron_logs`` table because the only existing index was
``(job_id)``.  With ~40k rows that meant a full-table scan + memory
sort on every load — measurable as ~130 ms cold cache.

Similarly, ``messages`` was indexed on ``(session_id, created_at)``
ASC, but the API queries it with ``ORDER BY created_at DESC, id DESC``
— SQLite can scan the index in reverse for the leading column but
still needs to break ties on ``id``, so a compound index that includes
``id`` lets the query be served entirely from the index.

This migration:
1. Adds ``idx_cron_logs_started`` for unfiltered "latest N entries" queries.
2. Adds ``idx_messages_session_created`` covering the ORDER BY clause
   used by the session message reader.

The old ``idx_messages_session`` is kept — it's still useful for
existence/count queries and dropping it would require a table rewrite.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript("""
        CREATE INDEX IF NOT EXISTS idx_cron_logs_started
            ON cron_logs(started_at DESC);

        CREATE INDEX IF NOT EXISTS idx_messages_session_created
            ON messages(session_id, created_at DESC, id DESC);
    """)
    await db.commit()
    logger.info(
        "V25 migration: added idx_cron_logs_started and "
        "idx_messages_session_created for diagnostics hot paths"
    )
