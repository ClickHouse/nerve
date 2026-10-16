"""Plan data access methods."""

from __future__ import annotations

from datetime import datetime, timezone

from nerve.identity import UNATTRIBUTED, Attribution


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
        *,
        attribution: Attribution = UNATTRIBUTED,
    ) -> dict:
        """Insert a plan version.

        ``attribution`` is who wrote this version and on whose behalf:
        written to ``created_by_actor_id`` and ``caused_by_actor_id``.
        """
        now = datetime.now(timezone.utc).isoformat()
        await self._write(
            """INSERT INTO plans (id, task_id, session_id, content, model, version, parent_plan_id, plan_type, created_at,
                                  created_by_actor_id, caused_by_actor_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (plan_id, task_id, session_id, content, model, version, parent_plan_id, plan_type, now,
             attribution.actor_id, attribution.caused_by_actor_id),
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

    async def compare_and_update_plan(
        self,
        plan_id: str,
        *,
        expect_status: str | tuple[str, ...],
        attribution: Attribution | None = None,
        **fields,
    ) -> bool:
        """Write ``fields`` only while the plan's status is ``expect_status``.

        ``expect_status`` is one status or a tuple of statuses. A decision
        reads the plan, checks its status and then acts, and a second
        decision can read the same status before the first one writes. This
        one statement decides: of several decisions made from the same
        status, only one changes the row, and only that caller may start the
        work that the decision causes. Each decision must therefore change
        the status (a revision request moves the plan to ``revising``), or a
        second decision from the same status still matches.

        ``attribution`` marks a review decision (approve, decline, a
        revision request, a status or feedback change through REST). Its
        pair goes to ``reviewed_by_actor_id`` and
        ``reviewed_caused_by_actor_id`` in the same statement. ``None`` (for
        example the planner superseding its own plan) leaves them as they
        are.

        Returns True when this call changed the row.
        """
        if attribution is not None:
            fields["reviewed_by_actor_id"] = attribution.actor_id
            fields["reviewed_caused_by_actor_id"] = attribution.caused_by_actor_id
        if not fields:
            raise ValueError("compare_and_update_plan needs at least one field")
        expected = (expect_status,) if isinstance(expect_status, str) else tuple(expect_status)
        if not expected:
            raise ValueError("compare_and_update_plan needs an expected status")
        sets = ", ".join(f"{k} = ?" for k in fields)
        placeholders = ", ".join("?" for _ in expected)
        result = await self._write(
            f"UPDATE plans SET {sets} WHERE id = ? AND status IN ({placeholders})",
            (*fields.values(), plan_id, *expected),
        )
        return result.rowcount == 1

    async def reopen_revising_plans(self) -> int:
        """Move every ``revising`` plan back to ``pending``; return the count.

        A plan is ``revising`` while the turn that asks its planner for a new
        version runs. At startup no such turn runs in this process, so a
        plan left ``revising`` by a stop would otherwise never be open for a
        decision again.
        """
        result = await self._write(
            "UPDATE plans SET status = 'pending' WHERE status = 'revising'",
        )
        return result.rowcount

    async def get_plans_for_task(self, task_id: str) -> list[dict]:
        async with self.db.execute(
            "SELECT * FROM plans WHERE task_id = ? ORDER BY version DESC",
            (task_id,),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_pending_plan_task_ids(self) -> list[str]:
        """Get task IDs that have a pending, revising or implementing plan."""
        async with self.db.execute(
            "SELECT DISTINCT task_id FROM plans"
            " WHERE status IN ('pending', 'revising', 'implementing')"
        ) as cursor:
            return [row[0] async for row in cursor]
