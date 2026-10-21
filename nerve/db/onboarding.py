"""Onboarding state data access (the single ``onboarding`` row, migration v060)."""

from __future__ import annotations

from datetime import datetime, timezone


class OnboardingStore:
    """Mixin for the onboarding row."""

    async def get_onboarding(self) -> dict | None:
        async with self.db.execute("SELECT * FROM onboarding WHERE id = 1") as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def set_onboarding(
        self,
        status: str,
        *,
        session_id: str | None,
        actor_id: str | None,
        expect: tuple[str | None, str | None],
    ) -> bool:
        """Write the row if it still holds ``expect``. True when written.

        ``expect`` is the ``(status, session_id)`` that the caller read;
        ``(None, None)`` means no row. The read and the write run in one
        transaction, so of two callers that read the same row, only one
        writes.
        """
        now = datetime.now(timezone.utc).isoformat()
        async with self._atomic():
            async with self.db.execute(
                "SELECT status, session_id FROM onboarding WHERE id = 1"
            ) as cursor:
                row = await cursor.fetchone()
            current = (row[0], row[1]) if row else (None, None)
            if current != tuple(expect):
                return False
            await self.db.execute(
                """INSERT INTO onboarding (id, status, session_id, actor_id, updated_at)
                   VALUES (1, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                       status = excluded.status, session_id = excluded.session_id,
                       actor_id = excluded.actor_id, updated_at = excluded.updated_at""",
                (status, session_id, actor_id, now),
            )
        return True
