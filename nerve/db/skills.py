"""Skill data access methods."""

from __future__ import annotations

import json
from datetime import datetime, timezone


class SkillStore:
    """Mixin providing skill registry and usage tracking operations."""

    async def upsert_skill(
        self,
        skill_id: str,
        name: str,
        description: str,
        version: str = "1.0.0",
        enabled: bool = True,
        user_invocable: bool = True,
        model_invocable: bool = True,
        allowed_tools: list[str] | None = None,
        metadata: dict | None = None,
    ) -> None:
        """Insert or update a skill in the registry."""
        now = datetime.now(timezone.utc).isoformat()
        await self._write(
            """INSERT INTO skills (id, name, description, version, enabled,
               user_invocable, model_invocable, allowed_tools, metadata, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 name=excluded.name, description=excluded.description,
                 version=excluded.version, user_invocable=excluded.user_invocable,
                 model_invocable=excluded.model_invocable,
                 allowed_tools=excluded.allowed_tools, metadata=excluded.metadata,
                 updated_at=excluded.updated_at""",
            (skill_id, name, description, version, enabled,
             user_invocable, model_invocable,
             json.dumps(allowed_tools) if allowed_tools else None,
             json.dumps(metadata or {}), now, now),
        )

    async def get_skill_row(self, skill_id: str) -> dict | None:
        """Get a single skill record."""
        async with self.db.execute(
            "SELECT * FROM skills WHERE id = ?", (skill_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def list_skills(self) -> list[dict]:
        """List all skills."""
        async with self.db.execute(
            "SELECT * FROM skills ORDER BY name"
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def delete_skill_row(self, skill_id: str) -> None:
        """Remove a skill and its usage records."""
        async with self._atomic():
            await self.db.execute("DELETE FROM skill_usage WHERE skill_id = ?", (skill_id,))
            await self.db.execute("DELETE FROM skills WHERE id = ?", (skill_id,))

    async def update_skill_enabled(self, skill_id: str, enabled: bool) -> None:
        """Toggle a skill's enabled state."""
        now = datetime.now(timezone.utc).isoformat()
        await self._write(
            "UPDATE skills SET enabled = ?, updated_at = ? WHERE id = ?",
            (enabled, now, skill_id),
        )

    async def record_skill_usage(
        self,
        skill_id: str,
        session_id: str | None = None,
        invoked_by: str = "model",
        duration_ms: int | None = None,
        success: bool = True,
        error: str | None = None,
    ) -> None:
        """Log a skill invocation."""
        await self._write(
            """INSERT INTO skill_usage (skill_id, session_id, invoked_by, duration_ms, success, error)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (skill_id, session_id, invoked_by, duration_ms, success, error),
        )

    async def get_skill_usage(self, skill_id: str, limit: int = 50) -> list[dict]:
        """Get usage history for a skill."""
        async with self.db.execute(
            "SELECT * FROM skill_usage WHERE skill_id = ? ORDER BY created_at DESC LIMIT ?",
            (skill_id, limit),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_skill_stats(self, skill_id: str | None = None) -> list[dict]:
        """Get aggregate usage stats per skill."""
        where = "WHERE skill_id = ?" if skill_id else ""
        params: list = [skill_id] if skill_id else []
        query = f"""
            SELECT
                skill_id,
                COUNT(*) as total_invocations,
                SUM(CASE WHEN success THEN 1 ELSE 0 END) as success_count,
                ROUND(AVG(duration_ms), 0) as avg_duration_ms,
                MAX(created_at) as last_used
            FROM skill_usage
            {where}
            GROUP BY skill_id
        """
        async with self.db.execute(query, tuple(params)) as cursor:
            return [dict(row) async for row in cursor]

    async def get_all_skills_with_stats(self) -> list[dict]:
        """List all skills with aggregated usage stats."""
        async with self.db.execute("""
            SELECT s.*,
                   COALESCE(u.total_invocations, 0) as total_invocations,
                   COALESCE(u.success_count, 0) as success_count,
                   u.avg_duration_ms,
                   u.last_used
            FROM skills s
            LEFT JOIN (
                SELECT skill_id,
                       COUNT(*) as total_invocations,
                       SUM(CASE WHEN success THEN 1 ELSE 0 END) as success_count,
                       ROUND(AVG(duration_ms), 0) as avg_duration_ms,
                       MAX(created_at) as last_used
                FROM skill_usage
                GROUP BY skill_id
            ) u ON s.id = u.skill_id
            ORDER BY s.name
        """) as cursor:
            rows = [dict(row) async for row in cursor]
        for row in rows:
            if row.get("allowed_tools"):
                try:
                    row["allowed_tools"] = json.loads(row["allowed_tools"])
                except (json.JSONDecodeError, TypeError):
                    pass
            if row.get("metadata"):
                try:
                    row["metadata"] = json.loads(row["metadata"])
                except (json.JSONDecodeError, TypeError):
                    pass
        return rows
