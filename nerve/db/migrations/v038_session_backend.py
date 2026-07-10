"""V38: Track each session's agent backend.

Multi-backend support (docs/plans/codex-backend.md): a session created
on one backend (claude / codex) must never be resumed on another — the
stored native session id (``sdk_session_id``) is meaningless across
runtimes. The engine stamps ``backend`` at first client build and the
sticky resolution rule makes the stored value always win over config,
so flipping ``agent.backend`` / ``agent.cron_backend`` never corrupts
existing conversations (including their scheduled wakeups).

NULL means "created before this migration" and is read as "claude" —
every pre-existing session is a Claude session by definition.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.execute("ALTER TABLE sessions ADD COLUMN backend TEXT")
    logger.info("v038: added sessions.backend")
