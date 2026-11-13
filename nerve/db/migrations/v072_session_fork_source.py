"""V72: the native conversation that a fork branches from.

Adds two columns to ``sessions``:

- ``fork_source_native_id``: the parent's native session id (Claude session
  id, Codex thread id) at fork time.
- ``fork_source_turn_id``: the parent's native turn at which the fork
  branches (Claude ``--resume-session-at``, Codex ``lastTurnId``). NULL for a
  fork of the whole conversation.

``fork_session`` writes both. The engine forks from them until the fork's
first completed turn records the fork's own native id, and then clears them.

Existing forks get a source when they never had a native conversation of
their own. A fork is a row with a ``fork-`` id, a ``forked_from_message``, or
a ``fork_of`` origin, whatever its status. The source is, in this order:

1. The native id of the fork's first ``started`` event, which its first
   client build recorded: the parent's id that it branched from (NULL when
   that build did not fork).
2. The parent's current native id.
3. The parent's last native id in a ``started`` event before the fork was
   created (a parent whose id an archive cleared).

A fork that recorded any other native id had a conversation of its own and
gets no source. The fork turn is the turn that ``get_native_turn_at_message``
resolves for ``forked_from_message``, and NULL for a whole fork. A fork whose
own native id is its source (a first turn that did not complete bound it to
the parent's conversation) loses that id, so that it branches again instead
of writing into the parent's conversation.

Each column is added only when it is missing, and the backfill writes only
rows with no source yet, so a run that was interrupted can run again.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

FORKS = """
SELECT f.id, f.parent_session_id, f.forked_from_message, f.sdk_session_id,
       p.sdk_session_id
FROM sessions f LEFT JOIN sessions p ON p.id = f.parent_session_id
WHERE f.parent_session_id IS NOT NULL
  AND f.fork_source_native_id IS NULL
  AND COALESCE(f.source, '') != 'workflow'
  AND (f.id LIKE 'fork-%'
       OR f.forked_from_message IS NOT NULL
       OR CASE WHEN json_valid(f.origin_detail)
               THEN json_extract(f.origin_detail, '$.fork_of') END IS NOT NULL)
"""

_STARTED_ID = (
    "CASE WHEN json_valid(details) "
    "THEN json_extract(details, '$.sdk_session_id') END"
)
FIRST_STARTED_ID = f"""
SELECT {_STARTED_ID} FROM session_events
WHERE session_id = ? AND event_type = 'started'
ORDER BY id ASC LIMIT 1
"""
OTHER_STARTED_ID = f"""
SELECT 1 FROM session_events
WHERE session_id = ? AND event_type = 'started' AND {_STARTED_ID} IS NOT NULL
  AND {_STARTED_ID} != ?
LIMIT 1
"""
LAST_STARTED_ID_BEFORE = f"""
SELECT {_STARTED_ID} FROM session_events
WHERE session_id = ? AND event_type = 'started' AND {_STARTED_ID} IS NOT NULL
  AND id < ?
ORDER BY id DESC LIMIT 1
"""
FIRST_EVENT_ID = "SELECT MIN(id) FROM session_events WHERE session_id = ?"

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


async def _one(db: aiosqlite.Connection, sql: str, params: tuple):
    async with db.execute(sql, params) as cursor:
        row = await cursor.fetchone()
    return row[0] if row else None


async def _turn_at_message(
    db: aiosqlite.Connection, session_id: str, message_id: str,
) -> str | None:
    try:
        numeric_id = int(message_id)
    except (TypeError, ValueError):
        return None
    for sql in (TURN_AT_OR_AFTER, TURN_AT_OR_BEFORE):
        turn = await _one(db, sql, (session_id, numeric_id))
        if turn is not None:
            return str(turn)
    return None


async def _source_of(
    db: aiosqlite.Connection, fork_id: str, parent_id: str,
    parent_native_id: str | None,
) -> str | None:
    recorded = await _one(db, FIRST_STARTED_ID, (fork_id,))
    if recorded:
        return str(recorded)
    if parent_native_id:
        return parent_native_id
    created = await _one(db, FIRST_EVENT_ID, (fork_id,))
    before = await _one(
        db, LAST_STARTED_ID_BEFORE,
        (parent_id, created if created is not None else 2**62),
    )
    return str(before) if before else None


async def up(db: aiosqlite.Connection) -> None:
    for column in ("fork_source_native_id", "fork_source_turn_id"):
        if not await _has_column(db, "sessions", column):
            await db.execute(f"ALTER TABLE sessions ADD COLUMN {column} TEXT")
    async with db.execute(FORKS) as cursor:
        forks = await cursor.fetchall()
    filled = detached = 0
    for fork_id, parent_id, at_message, own_id, parent_native_id in forks:
        source = await _source_of(db, fork_id, parent_id, parent_native_id)
        if source is None or (own_id and own_id != source):
            continue
        if await _one(db, OTHER_STARTED_ID, (fork_id, source)):
            continue
        turn_id = None
        if at_message is not None:
            turn_id = await _turn_at_message(db, parent_id, at_message)
            if turn_id is None:
                # fork_session refuses a message without a turn mapping, so
                # only a hand-made row gets here. Do not guess a fork point.
                continue
        await db.execute(
            "UPDATE sessions SET fork_source_native_id = ?, fork_source_turn_id = ?,"
            " sdk_session_id = NULL WHERE id = ?",
            (source, turn_id, fork_id),
        )
        filled += 1
        detached += bool(own_id)
    logger.info(
        "v072: sessions fork source columns present, %d fork(s) filled "
        "(%d bound to their source's conversation)", filled, detached,
    )
