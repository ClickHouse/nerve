"""V72: the native conversation that a fork branches from.

Adds two columns to ``sessions``:

- ``fork_source_native_id``: the parent's native session id (Claude session
  id, Codex thread id) at fork time.
- ``fork_source_turn_id``: the parent's native turn at which the fork
  branches (Claude ``--resume-session-at``, Codex ``lastTurnId``). NULL for a
  fork of the whole conversation.

``fork_session`` writes both. The engine forks from them until the fork's
first completed turn records the fork's own native id, and then clears them.

Existing forks that have not run a turn get the parent's current native id,
and the turn that ``get_native_turn_at_message`` resolves for
``forked_from_message``. A fork that already has a native id keeps it. The
column check makes a second run do nothing.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

PENDING_FORKS = """
SELECT f.id, p.id, p.sdk_session_id, f.forked_from_message
FROM sessions f JOIN sessions p ON p.id = f.parent_session_id
WHERE f.status = 'created'
  AND COALESCE(f.source, '') != 'workflow'
  AND f.sdk_session_id IS NULL
  AND p.sdk_session_id IS NOT NULL
"""

# The two lookups of ``MessageStore.get_native_turn_at_message``.
TURN_AT_OR_AFTER = """
SELECT native_turn_id FROM messages
WHERE session_id = ? AND id >= ? AND native_turn_id IS NOT NULL
ORDER BY id ASC LIMIT 1
"""
TURN_AT_OR_BEFORE = """
SELECT native_turn_id FROM messages
WHERE session_id = ? AND id <= ? AND native_turn_id IS NOT NULL
ORDER BY id DESC LIMIT 1
"""


async def _has_column(db: aiosqlite.Connection, table: str, column: str) -> bool:
    async with db.execute(f"PRAGMA table_info({table})") as cursor:
        rows = await cursor.fetchall()
    return any(row[1] == column for row in rows)


async def _turn_at_message(
    db: aiosqlite.Connection, session_id: str, message_id: str,
) -> str | None:
    try:
        numeric_id = int(message_id)
    except (TypeError, ValueError):
        return None
    for sql in (TURN_AT_OR_AFTER, TURN_AT_OR_BEFORE):
        async with db.execute(sql, (session_id, numeric_id)) as cursor:
            row = await cursor.fetchone()
        if row:
            return str(row[0])
    return None


async def up(db: aiosqlite.Connection) -> None:
    if await _has_column(db, "sessions", "fork_source_native_id"):
        return
    await db.execute("ALTER TABLE sessions ADD COLUMN fork_source_native_id TEXT")
    await db.execute("ALTER TABLE sessions ADD COLUMN fork_source_turn_id TEXT")
    async with db.execute(PENDING_FORKS) as cursor:
        pending = await cursor.fetchall()
    filled = 0
    for fork_id, parent_id, parent_native_id, at_message in pending:
        turn_id = None
        if at_message is not None:
            turn_id = await _turn_at_message(db, parent_id, at_message)
            if turn_id is None:
                # fork_session refuses a message without a turn mapping, so
                # only a hand-made row gets here. Do not guess a fork point.
                continue
        await db.execute(
            "UPDATE sessions SET fork_source_native_id = ?, fork_source_turn_id = ? "
            "WHERE id = ?",
            (parent_native_id, turn_id, fork_id),
        )
        filled += 1
    logger.info("v072: sessions fork source columns added, %d pending fork(s) filled", filled)
