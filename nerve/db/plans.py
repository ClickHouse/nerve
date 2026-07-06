"""Plan data access methods."""

from __future__ import annotations

from datetime import datetime, timezone


class PlanStore:
    """Mixin providing plan CRUD operations."""

    async def create_plan(
        self,
        plan_id: str,
        task_id: str,
        content: str,
        session_id: str | None = None,
        model: str | None = None,
        version: int = 1,
        parent_plan_id: str | None = None,
        plan_type: str = "generic",
    ) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        await self._write(
            """INSERT INTO plans (id, task_id, session_id, content, model, version, parent_plan_id, plan_type, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (plan_id, task_id, session_id, content, model, version, parent_plan_id, plan_type, now),
        )
        return {"id": plan_id, "task_id": task_id, "version": version, "plan_type": plan_type}

    async def get_plan(self, plan_id: str) -> dict | None:
        async with self.db.execute(
            """SELECT p.*, t.title AS task_title
               FROM plans p LEFT JOIN tasks t ON p.task_id = t.id
               WHERE p.id = ?""",
            (plan_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def list_plans(
        self, status: str | None = None, task_id: str | None = None, limit: int = 100,
    ) -> list[dict]:
        conditions = []
        params: list = []
        if status:
            conditions.append("p.status = ?")
            params.append(status)
        if task_id:
            conditions.append("p.task_id = ?")
            params.append(task_id)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        async with self.db.execute(
            f"""SELECT p.*, t.title AS task_title
                FROM plans p LEFT JOIN tasks t ON p.task_id = t.id
                {where}
                ORDER BY p.created_at DESC LIMIT ?""",
            tuple(params),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def update_plan(self, plan_id: str, **fields) -> None:
        if not fields:
            return
        sets = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values())
        vals.append(plan_id)
        await self._write(
            f"UPDATE plans SET {sets} WHERE id = ?", tuple(vals),
        )

    async def get_plans_for_task(self, task_id: str) -> list[dict]:
        async with self.db.execute(
            "SELECT * FROM plans WHERE task_id = ? ORDER BY version DESC",
            (task_id,),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_pending_plan_task_ids(self) -> list[str]:
        """Get task IDs that have a pending or implementing plan."""
        async with self.db.execute(
            "SELECT DISTINCT task_id FROM plans WHERE status IN ('pending', 'implementing')"
        ) as cursor:
            return [row[0] async for row in cursor]
