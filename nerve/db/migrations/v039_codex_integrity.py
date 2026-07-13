"""V39: Make Codex session identity and accounting explicit.

The first Codex backend migration deliberately left legacy ``backend``
values NULL and relied on a read-time default.  That is unsafe once the
global default can change: an old Claude session may otherwise be opened by
Codex.  This migration turns the implicit invariant into persisted state and
adds the mappings needed for native-thread de-duplication and turn-point
forks.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def _columns(db: aiosqlite.Connection, table: str) -> set[str]:
    async with db.execute(f"PRAGMA table_info({table})") as cursor:
        return {str(row[1]) for row in await cursor.fetchall()}


async def up(db: aiosqlite.Connection) -> None:
    session_cols = await _columns(db, "sessions")
    if "cwd" not in session_cols:
        await db.execute("ALTER TABLE sessions ADD COLUMN cwd TEXT")

    message_cols = await _columns(db, "messages")
    if "native_turn_id" not in message_cols:
        await db.execute("ALTER TABLE messages ADD COLUMN native_turn_id TEXT")

    usage_cols = await _columns(db, "session_usage")
    if "cost_basis" not in usage_cols:
        await db.execute(
            "ALTER TABLE session_usage ADD COLUMN cost_basis TEXT NOT NULL "
            "DEFAULT 'unknown'"
        )
    if "estimated_cost_usd" not in usage_cols:
        await db.execute(
            "ALTER TABLE session_usage ADD COLUMN estimated_cost_usd REAL"
        )

    # Every session created before backend selection existed was a Claude
    # session.  This must be a data migration (v038 may already be applied).
    await db.execute(
        "UPDATE sessions SET backend = 'claude' "
        "WHERE backend IS NULL OR TRIM(backend) = ''"
    )

    await db.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_messages_native_turn
            ON messages(session_id, native_turn_id)
            WHERE native_turn_id IS NOT NULL;

        CREATE TABLE IF NOT EXISTS session_native_threads (
            backend TEXT NOT NULL,
            native_thread_id TEXT NOT NULL,
            session_id TEXT NOT NULL REFERENCES sessions(id)
                ON DELETE CASCADE ON UPDATE CASCADE,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (backend, native_thread_id),
            UNIQUE (backend, session_id)
        );
        CREATE INDEX IF NOT EXISTS idx_native_threads_session
            ON session_native_threads(session_id);

        INSERT OR IGNORE INTO session_native_threads
            (backend, native_thread_id, session_id)
        SELECT backend, sdk_session_id, id
        FROM sessions
        WHERE backend IS NOT NULL
          AND sdk_session_id IS NOT NULL
          AND TRIM(sdk_session_id) != '';
        """
    )
    logger.info(
        "v039: backfilled session backends; added cwd, native turn/thread "
        "mapping, and cost-basis columns"
    )
