"""Reply routes — which session a channel message belongs to.

A channel records every message it sends or receives against the session
that produced it or that it landed in, and resolves a later user *reply*
back to that session (see ``TelegramChannel._resolve_reply``). The map used
to live in memory and vanished with each restart, after which every reply to
an earlier message was refused. Stored here it survives restarts, and it is
filled whether or not reply routing is enabled, so turning the flag on also
covers messages from before.

Keyed by ``(channel, target, message_id)``: provider message ids are unique
only within a conversation, so the target is part of the key. Ids are text
because providers differ in shape (Telegram ints, Slack timestamps).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

# Routes older than this are swept by the maintenance tick. A reply to a
# message this old is rare, and the nudge tells the user how to recover.
REPLY_ROUTE_RETENTION_DAYS = 90


class ReplyRouteStore:
    """Mixin for the ``channel_reply_routes`` table."""

    async def record_reply_route(
        self, channel: str, target: str, message_id: str, session_id: str,
    ) -> None:
        """Map one message to its session, replacing an earlier mapping."""
        await self._write(
            "INSERT OR REPLACE INTO channel_reply_routes "
            "(channel, target, message_id, session_id, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                channel, target, message_id, session_id,
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    async def lookup_reply_route(
        self, channel: str, target: str, message_id: str,
    ) -> str | None:
        """The session mapped to one message, or None."""
        async with self.db.execute(
            "SELECT session_id FROM channel_reply_routes "
            "WHERE channel = ? AND target = ? AND message_id = ?",
            (channel, target, message_id),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

    async def cleanup_old_reply_routes(
        self, max_age_days: int = REPLY_ROUTE_RETENTION_DAYS,
    ) -> int:
        """Delete routes recorded more than ``max_age_days`` ago. Returns count deleted."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        result = await self._write(
            "DELETE FROM channel_reply_routes WHERE created_at < ?",
            (cutoff.isoformat(),),
        )
        return result.rowcount or 0
