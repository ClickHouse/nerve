"""Workflow run data access methods.

A *workflow run* row tracks one budget-capped multi-agent job: which
engine executes it (``claude-workflow`` / ``codex-ultracode``), the
engine-native spec, dollar budget vs. metered spend, the dedicated
session that hosts the execution, and the journal directory.

Status lifecycle::

    pending ──> running ──> done
       │           ├──────> failed
       │           ├──────> killed
       │           └──────> budget_exhausted
       └──────────────────> killed          (killed before dispatch)

Transitions go through :meth:`transition_workflow_run` — a compare-and-set
UPDATE so concurrent finishers (monitor kill vs. natural completion vs.
manual kill) can never double-apply a terminal state.
"""

from __future__ import annotations

import json

from nerve.utils.time import utc_now_iso

ACTIVE_STATUSES = ("pending", "running")
TERMINAL_STATUSES = ("done", "failed", "killed", "budget_exhausted")
ALL_STATUSES = ACTIVE_STATUSES + TERMINAL_STATUSES

# Columns writable via update_workflow_run (everything else is
# lifecycle-managed or immutable).
_UPDATABLE = {
    "title", "spent_usd", "warned_at", "session_id", "journal_dir",
    "error", "result", "started_at", "finished_at",
}


def _parse_run(row: dict) -> dict:
    """Decode the JSON spec column in place."""
    try:
        row["spec"] = json.loads(row.get("spec") or "{}")
    except (TypeError, ValueError):
        row["spec"] = {}
    return row


class WorkflowRunStore:
    """Mixin providing workflow run persistence."""

    async def create_workflow_run(
        self,
        run_id: str,
        engine: str,
        spec: dict,
        budget_usd: float | None,
        title: str = "",
        created_by: str = "user",
        journal_dir: str | None = None,
    ) -> dict:
        """Insert a new run in status ``pending`` and return the row."""
        now = utc_now_iso()
        await self._write(
            """INSERT INTO workflow_runs
               (id, engine, title, spec, status, budget_usd, spent_usd,
                created_by, journal_dir, created_at, updated_at)
               VALUES (?, ?, ?, ?, 'pending', ?, 0.0, ?, ?, ?, ?)""",
            (
                run_id, engine, title, json.dumps(spec), budget_usd,
                created_by, journal_dir, now, now,
            ),
        )
        run = await self.get_workflow_run(run_id)
        assert run is not None
        return run

    async def get_workflow_run(self, run_id: str) -> dict | None:
        async with self.db.execute(
            "SELECT * FROM workflow_runs WHERE id = ?", (run_id,)
        ) as cursor:
            async for row in cursor:
                return _parse_run(dict(row))
        return None

    async def list_workflow_runs(
        self,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        """List runs, newest first. ``status`` filters exactly; ``active``
        selects pending+running; None/empty returns everything."""
        conditions: list[str] = []
        params: list = []
        if status == "active":
            placeholders = ",".join("?" for _ in ACTIVE_STATUSES)
            conditions.append(f"status IN ({placeholders})")
            params.extend(ACTIVE_STATUSES)
        elif status:
            conditions.append("status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.extend([limit, offset])
        async with self.db.execute(
            f"""SELECT * FROM workflow_runs {where}
                ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?""",
            params,
        ) as cursor:
            return [_parse_run(dict(row)) async for row in cursor]

    async def count_workflow_runs(self, status: str | None = None) -> int:
        conditions: list[str] = []
        params: list = []
        if status == "active":
            placeholders = ",".join("?" for _ in ACTIVE_STATUSES)
            conditions.append(f"status IN ({placeholders})")
            params.extend(ACTIVE_STATUSES)
        elif status:
            conditions.append("status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        async with self.db.execute(
            f"SELECT COUNT(*) FROM workflow_runs {where}", params
        ) as cursor:
            async for row in cursor:
                return int(row[0])
        return 0

    async def get_active_workflow_runs(self) -> list[dict]:
        """All pending/running runs, oldest first (dispatch order)."""
        placeholders = ",".join("?" for _ in ACTIVE_STATUSES)
        async with self.db.execute(
            f"""SELECT * FROM workflow_runs WHERE status IN ({placeholders})
                ORDER BY created_at ASC, id ASC""",
            ACTIVE_STATUSES,
        ) as cursor:
            return [_parse_run(dict(row)) async for row in cursor]

    async def update_workflow_run(self, run_id: str, fields: dict) -> bool:
        """Update whitelisted columns; stamps ``updated_at``."""
        cols = {k: v for k, v in fields.items() if k in _UPDATABLE}
        if not cols:
            return False
        cols["updated_at"] = utc_now_iso()
        assignments = ", ".join(f"{k} = ?" for k in cols)
        result = await self._write(
            f"UPDATE workflow_runs SET {assignments} WHERE id = ?",
            (*cols.values(), run_id),
        )
        return (result.rowcount or 0) == 1

    async def transition_workflow_run(
        self,
        run_id: str,
        to_status: str,
        expect: tuple[str, ...] = ACTIVE_STATUSES,
        **fields,
    ) -> bool:
        """Atomically move a run to ``to_status`` iff its current status is
        in ``expect``. Returns True only for the caller that actually
        flipped the row — the winner owns the terminal side effects
        (notification, journal finalize).

        Extra ``fields`` (whitelisted) are applied in the same UPDATE.
        Terminal transitions stamp ``finished_at`` unless provided.
        """
        if to_status not in ALL_STATUSES:
            raise ValueError(f"unknown workflow run status: {to_status}")
        now = utc_now_iso()
        cols = {k: v for k, v in fields.items() if k in _UPDATABLE}
        if to_status in TERMINAL_STATUSES and "finished_at" not in cols:
            cols["finished_at"] = now
        if to_status == "running" and "started_at" not in cols:
            cols["started_at"] = now
        assignments = ", ".join(
            ["status = ?", "updated_at = ?"] + [f"{k} = ?" for k in cols]
        )
        placeholders = ",".join("?" for _ in expect)
        result = await self._write(
            f"""UPDATE workflow_runs SET {assignments}
                WHERE id = ? AND status IN ({placeholders})""",
            (to_status, now, *cols.values(), run_id, *expect),
        )
        return (result.rowcount or 0) == 1
