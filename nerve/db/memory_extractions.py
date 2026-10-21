"""Session windows queued for extraction by the memory backend.

One row per batch the Loom memory backend queued with deferred extraction, so
a batch Loom gives up on can be sent again: see
``nerve/db/migrations/v059_memory_extractions.py`` and
:meth:`nerve.memory.loom_bridge.LoomBridge.reconcile_extractions`.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

# ``state`` values: queued in Loom (not yet known to be done), or given up on.
EXTRACTION_QUEUED = "queued"
EXTRACTION_FAILED = "failed"


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ids_json(message_ids: list[int]) -> str:
    return json.dumps([int(i) for i in message_ids])


def _row(row: Any) -> dict:
    out = dict(row)
    try:
        ids = json.loads(out.get("message_ids") or "[]")
    except ValueError:
        ids = []
    out["message_ids"] = [int(i) for i in ids] if isinstance(ids, list) else []
    return out


class MemoryExtractionStore:
    """Mixin providing the memory extraction batch records."""

    async def add_memory_extraction(
        self,
        session_id: str,
        queue_id: str,
        message_ids: list[int],
        *,
        org: str,
        namespace: str,
        message_count: int | None = None,
        sends: int = 1,
        last_error: str = "",
    ) -> int:
        """Record a queued batch; returns its row id.

        ``message_ids`` are the batch's messages, which a resend reads back;
        empty when they cannot be named. ``message_count`` defaults to their
        number.
        """
        now = _now()
        count = len(message_ids) if message_count is None else int(message_count)
        result = await self._write(
            "INSERT INTO memory_extractions (session_id, org, namespace, queue_id, "
            "message_ids, message_count, sends, state, last_error, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                session_id, org, namespace, queue_id, _ids_json(message_ids), count,
                int(sends), EXTRACTION_QUEUED, last_error, now, now,
            ),
        )
        return int(result.lastrowid or 0)

    async def memory_extractions(
        self,
        state: str = EXTRACTION_QUEUED,
        *,
        org: str | None = None,
        namespace: str | None = None,
        limit: int = 500,
    ) -> list[dict]:
        """Batches in ``state``, oldest first; only those queued in ``org``
        and ``namespace`` when they are given. ``message_ids`` is a list."""
        sql = "SELECT * FROM memory_extractions WHERE state = ?"
        params: list[Any] = [state]
        if org is not None:
            sql += " AND org = ?"
            params.append(org)
        if namespace is not None:
            sql += " AND namespace = ?"
            params.append(namespace)
        sql += " ORDER BY created_at, id LIMIT ?"
        params.append(int(limit))
        async with self.db.execute(sql, params) as cursor:
            return [_row(row) async for row in cursor]

    async def update_memory_extraction(
        self,
        row_id: int,
        *,
        queue_id: str | None = None,
        message_ids: list[int] | None = None,
        sends: int | None = None,
        state: str | None = None,
        last_error: str | None = None,
    ) -> None:
        """Change the given fields of one batch (new ``message_ids`` set its
        ``message_count`` too)."""
        changes: list[tuple[str, Any]] = [
            ("queue_id", queue_id),
            ("sends", sends),
            ("state", state),
            ("last_error", last_error),
        ]
        if message_ids is not None:
            changes += [("message_ids", _ids_json(message_ids)), ("message_count", len(message_ids))]
        sets: list[str] = []
        params: list[Any] = []
        for column, value in changes:
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

    async def prune_memory_extractions(
        self, older_than: str, *, org: str, namespace: str,
    ) -> int:
        """Delete batches last changed before ``older_than`` (ISO) that were
        given up on, or that were queued in another org or namespace."""
        result = await self._write(
            "DELETE FROM memory_extractions WHERE updated_at < ? "
            "AND (state = ? OR org != ? OR namespace != ?)",
            (older_than, EXTRACTION_FAILED, org, namespace),
        )
        return result.rowcount

    async def memory_extraction_counts(
        self, *, org: str | None = None, namespace: str | None = None,
    ) -> dict[str, int]:
        """How many batches are in each state; only those queued in ``org``
        and ``namespace`` when they are given."""
        sql = "SELECT state, COUNT(*) AS n FROM memory_extractions"
        clauses: list[str] = []
        params: list[Any] = []
        if org is not None:
            clauses.append("org = ?")
            params.append(org)
        if namespace is not None:
            clauses.append("namespace = ?")
            params.append(namespace)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " GROUP BY state"
        async with self.db.execute(sql, params) as cursor:
            return {str(row["state"]): int(row["n"]) async for row in cursor}
