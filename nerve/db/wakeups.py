"""Session wakeup data access methods (ScheduleWakeup harness).

A *wakeup* is a one-shot, self-scheduled prompt the model requested via
the ``ScheduleWakeup`` tool. Rows are written by the PostToolUse capture
hook and consumed by the cron-service sweep, which fires the prompt
through ``engine.run(..., source="wakeup")`` at ``fire_at``.
"""

from __future__ import annotations


class WakeupStore:
    """Mixin providing session wakeup persistence."""

    async def add_wakeup(
        self,
        session_id: str,
        prompt: str,
        fire_at: str,
        reason: str = "",
    ) -> int:
        """Record a pending wakeup, replacing any prior pending one.

        ``ScheduleWakeup`` keeps a single active wakeup per session (the
        ``/loop`` self-pacing model re-calls the tool each turn). So a new
        request supersedes any earlier pending wakeup for the session.

        Returns the new wakeup id.
        """
        async with self._atomic():
            await self.db.execute(
                "DELETE FROM session_wakeups "
                "WHERE session_id = ? AND status = 'pending'",
                (session_id,),
            )
            cursor = await self.db.execute(
                """INSERT INTO session_wakeups (session_id, prompt, reason, fire_at)
                   VALUES (?, ?, ?, ?)""",
                (session_id, prompt, reason, fire_at),
            )
            wakeup_id = cursor.lastrowid
            await cursor.close()
        return wakeup_id

    async def get_due_wakeups(self, now_iso: str, limit: int = 50) -> list[dict]:
        """Return pending wakeups whose ``fire_at`` is at or before ``now_iso``.

        ``fire_at`` is a UTC ISO-8601 string (fixed width, ``+00:00``
        offset) so lexicographic comparison matches chronological order.
        """
        async with self.db.execute(
            """SELECT * FROM session_wakeups
               WHERE status = 'pending' AND fire_at <= ?
               ORDER BY fire_at ASC LIMIT ?""",
            (now_iso, limit),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def claim_wakeup(self, wakeup_id: int) -> bool:
        """Atomically transition a wakeup pending -> fired.

        Returns ``True`` only for the caller that actually flipped the row,
        so overlapping sweeps can never fire the same wakeup twice.
        """
        result = await self._write(
            "UPDATE session_wakeups SET status = 'fired' "
            "WHERE id = ? AND status = 'pending'",
            (wakeup_id,),
        )
        return (result.rowcount or 0) == 1

    async def cancel_wakeups_for_session(self, session_id: str) -> int:
        """Delete all pending wakeups for a session. Returns rows removed."""
        result = await self._write(
            "DELETE FROM session_wakeups "
            "WHERE session_id = ? AND status = 'pending'",
            (session_id,),
        )
        return result.rowcount or 0

    async def list_pending_wakeups(self, session_id: str | None = None) -> list[dict]:
        """List pending wakeups, optionally scoped to one session."""
        if session_id is not None:
            query = (
                "SELECT * FROM session_wakeups "
                "WHERE status = 'pending' AND session_id = ? ORDER BY fire_at ASC"
            )
            params: tuple = (session_id,)
        else:
            query = (
                "SELECT * FROM session_wakeups "
                "WHERE status = 'pending' ORDER BY fire_at ASC"
            )
            params = ()
        async with self.db.execute(query, params) as cursor:
            return [dict(row) async for row in cursor]
