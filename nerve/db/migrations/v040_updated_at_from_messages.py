"""V40: Realign ``sessions.updated_at`` to actual last-message time.

``updated_at`` now means "last message activity" and drives the session
list order exclusively. Historically every ``update_session_fields``
call auto-bumped it, so incidental writes polluted the ordering: opening
a chat (``set_active_session``'s stickiness bookkeeping), star/rename,
status flips, memorization watermarks. The visible symptom was chats
teleporting to the top of the sidebar merely because they were clicked,
or because a background sweep touched them.

The auto-bump is removed alongside this migration; here we repair the
stored values by resetting each session's ``updated_at`` to its latest
message timestamp. Sessions with no messages keep their current value
(creation time).

Format note: ``messages.created_at`` is mixed-format — SQLite
``CURRENT_TIMESTAMP`` ("YYYY-MM-DD HH:MM:SS") for native inserts, ISO
8601 for externally-ingested rows with preserved timestamps. MAX() over
mixed formats is unreliable lexicographically (space sorts before 'T'),
so values are normalized to 'T'-form in SQL before MAX, then re-parsed
in Python and written back as timezone-aware UTC ISO — the same format
the ``add_message*`` bumps use.

Idempotent: re-running recomputes the same values.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import aiosqlite

logger = logging.getLogger(__name__)


def _to_utc_iso(raw: str) -> str | None:
    """Normalize a 'T'-form timestamp string to timezone-aware UTC ISO."""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        # Naive timestamps in this DB are UTC (SQLite CURRENT_TIMESTAMP).
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


async def up(db: aiosqlite.Connection) -> None:
    async with db.execute(
        "SELECT session_id, MAX(REPLACE(created_at, ' ', 'T')) "
        "FROM messages WHERE created_at IS NOT NULL GROUP BY session_id"
    ) as cur:
        rows = await cur.fetchall()

    repaired = skipped = 0
    for session_id, last_ts in rows:
        iso = _to_utc_iso(last_ts) if last_ts else None
        if iso is None:
            skipped += 1
            continue
        await db.execute(
            "UPDATE sessions SET updated_at = ? WHERE id = ?",
            (iso, session_id),
        )
        repaired += 1

    await db.commit()
    logger.info(
        "V40 migration: realigned updated_at to last message time for %d "
        "sessions (%d unparseable timestamps skipped)",
        repaired, skipped,
    )
