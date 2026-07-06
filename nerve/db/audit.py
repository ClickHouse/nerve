"""memU audit log data access methods."""

from __future__ import annotations

import json
from datetime import datetime, timezone


class AuditStore:
    """Mixin providing memU audit log operations."""

    async def log_audit(
        self,
        action: str,
        target_type: str,
        target_id: str | None = None,
        source: str | None = None,
        details: dict | None = None,
    ) -> int:
        """Write an entry to the memU audit log."""
        now = datetime.now(timezone.utc).isoformat()
        result = await self._write(
            "INSERT INTO memu_audit_log (timestamp, action, target_type, target_id, source, details) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (now, action, target_type, target_id, source, json.dumps(details) if details else None),
        )
        return result.lastrowid

    async def get_audit_logs(
        self,
        action: str | None = None,
        target_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Retrieve audit log entries, newest first."""
        conditions: list[str] = []
        params: list = []
        if action:
            conditions.append("action = ?")
            params.append(action)
        if target_type:
            conditions.append("target_type = ?")
            params.append(target_type)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        query = f"SELECT * FROM memu_audit_log {where} ORDER BY timestamp DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        async with self.db.execute(query, tuple(params)) as cursor:
            rows = [dict(row) async for row in cursor]
        for row in rows:
            if row.get("details"):
                try:
                    row["details"] = json.loads(row["details"])
                except (json.JSONDecodeError, TypeError):
                    pass
        return rows
