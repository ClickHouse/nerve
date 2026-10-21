"""Session windows queued for extraction by the memory backend.

One row per batch the Loom memory backend queued with deferred extraction, so
a batch Loom gives up on can be sent again: see
``nerve/db/migrations/v059_memory_extractions.py`` and
:meth:`nerve.memory.loom_bridge.LoomBridge.reconcile_extractions`.
"""

from __future__ import annotations

from datetime import UTC, datetime

# ``state`` values: queued in Loom (not yet known to be done), or given up on.
EXTRACTION_QUEUED = "queued"
EXTRACTION_FAILED = "failed"


def _now() -> str:
    return datetime.now(UTC).isoformat()


class MemoryExtractionStore:
    """Mixin providing the memory extraction batch records."""

    async def add_memory_extraction(
        self,
        session_id: str,
        queue_id: str,
        window_start: str,
        window_end: str,
        message_count: int,
        *,
        sends: int = 1,
    ) -> int:
        """Record a queued batch; returns its row id."""
        now = _now()
        result = await self._write(
            "INSERT INTO memory_extractions (session_id, queue_id, window_start, "
            "window_end, message_count, sends, state, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                session_id, queue_id, window_start, window_end, int(message_count),
                int(sends), EXTRACTION_QUEUED, now, now,
            ),
        )
        return int(result.lastrowid or 0)

    async def memory_extractions(
        self, state: str = EXTRACTION_QUEUED, limit: int = 500,
    ) -> list[dict]:
        """Batches in ``state``, oldest first."""
        async with self.db.execute(
            "SELECT * FROM memory_extractions WHERE state = ? "
            "ORDER BY created_at, id LIMIT ?",
            (state, int(limit)),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def update_memory_extraction(
        self,
        row_id: int,
        *,
        queue_id: str | None = None,
        sends: int | None = None,
        state: str | None = None,
        last_error: str | None = None,
    ) -> None:
        """Change the given fields of one batch."""
        sets: list[str] = []
        params: list = []
        for column, value in (
            ("queue_id", queue_id),
            ("sends", sends),
            ("state", state),
            ("last_error", last_error),
        ):
            if value is not None:
                sets.append(f"{column} = ?")
                params.append(value)
        sets.append("updated_at = ?")
        params.extend([_now(), int(row_id)])
        await self._write(
            f"UPDATE memory_extractions SET {', '.join(sets)} WHERE id = ?", params,
        )

    async def delete_memory_extraction(self, row_id: int) -> None:
        await self._write("DELETE FROM memory_extractions WHERE id = ?", (int(row_id),))

    async def prune_memory_extractions(self, state: str, older_than: str) -> int:
        """Delete batches in ``state`` last changed before ``older_than`` (ISO)."""
        result = await self._write(
            "DELETE FROM memory_extractions WHERE state = ? AND updated_at < ?",
            (state, older_than),
        )
        return result.rowcount

    async def memory_extraction_counts(self) -> dict[str, int]:
        """How many batches are in each state."""
        async with self.db.execute(
            "SELECT state, COUNT(*) AS n FROM memory_extractions GROUP BY state",
        ) as cursor:
            return {str(row["state"]): int(row["n"]) async for row in cursor}
