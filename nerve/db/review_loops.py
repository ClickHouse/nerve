"""Review loop data access methods.

A *review loop* row is the deterministic implement→verify state machine
driven by :class:`nerve.workflows.review_loop.ReviewLoopService`. Legs
execute as ordinary workflow runs; the ``review_loop_attempts`` table is
the append-only dispatch ledger/outbox that links loop iterations to run
ids (written in the same transaction as the loop-state CAS, *before* the
run is started, so a crash can never orphan a paid leg).

Status lifecycle::

    pending ──> implementing ──> verifying ──> implementing   (iterate)
       │              │               ├──────> passed
       │              │               │
       │              └───────┬───────┴──────> awaiting_user  (parked:
       │                      │                 caps, no-progress,
       │                      │                 verifier errors, restart)
       │                      │
       │        awaiting_user ├──────> implementing | verifying (decision)
       │                      ├──────> passed  (accept as-is)
       │                      └──────> failed  (abandon / escalation expiry)
       └─────────────────────────────> killed  (user kill, any pre-terminal)

Transitions go through :meth:`transition_review_loop` — a compare-and-set
UPDATE so duplicate leg-completion signals and kill races can never
double-apply. ``begin_leg_attempt`` combines the state CAS with the
attempt-ledger insert in one transaction (the dispatch outbox).
"""

from __future__ import annotations

import json

from nerve.utils.time import utc_now_iso

RL_RUNNING_STATUSES = ("pending", "implementing", "verifying")
RL_PARKED_STATUSES = ("awaiting_user",)
RL_OPEN_STATUSES = RL_RUNNING_STATUSES + RL_PARKED_STATUSES
RL_TERMINAL_STATUSES = ("passed", "failed", "killed")
RL_ALL_STATUSES = RL_OPEN_STATUSES + RL_TERMINAL_STATUSES

# Columns writable via update_review_loop. budget_usd is mutable ONLY via
# the user's explicit budget-grant decision.
_RL_UPDATABLE = {
    "title", "session_id", "failure_reason", "criteria", "iteration",
    "current_run_id", "escalation_count", "discovery_rounds", "spent_usd",
    "warned_at", "max_iterations", "budget_usd", "ended_at",
}

_JSON_COLS = ("criteria", "implementer", "verifier")


def _parse_loop(row: dict) -> dict:
    for col in _JSON_COLS:
        try:
            row[col] = json.loads(row.get(col) or ("[]" if col == "criteria" else "{}"))
        except (TypeError, ValueError):
            row[col] = [] if col == "criteria" else {}
    return row


def _parse_attempt(row: dict) -> dict:
    for col in ("verdict", "detail"):
        raw = row.get(col)
        if raw is None:
            continue
        try:
            row[col] = json.loads(raw)
        except (TypeError, ValueError):
            row[col] = None
    return row


def _encode(fields: dict) -> dict:
    out = dict(fields)
    for col in ("criteria", "verdict", "detail"):
        if col in out and not isinstance(out[col], (str, type(None))):
            out[col] = json.dumps(out[col])
    return out


class ReviewLoopStore:
    """Mixin providing review loop persistence."""

    # ------------------------------------------------------------------ #
    #  Loops                                                              #
    # ------------------------------------------------------------------ #

    async def create_review_loop(
        self,
        loop_id: str,
        *,
        title: str,
        session_id: str | None,
        goal_prompt: str,
        verifier_prompt: str,
        criteria_adoption: str,
        criteria: list,
        implementer: dict,
        verifier: dict,
        cwd: str | None,
        max_iterations: int,
        budget_usd: float,
        created_by: str = "user",
    ) -> dict:
        now = utc_now_iso()
        await self._write(
            """INSERT INTO review_loops
               (id, title, session_id, status, goal_prompt, verifier_prompt,
                criteria_adoption, criteria, implementer, verifier, cwd,
                max_iterations, budget_usd, created_by, created_at, updated_at)
               VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                loop_id, title, session_id, goal_prompt, verifier_prompt,
                criteria_adoption, json.dumps(criteria),
                json.dumps(implementer), json.dumps(verifier), cwd,
                max_iterations, budget_usd, created_by, now, now,
            ),
        )
        loop = await self.get_review_loop(loop_id)
        assert loop is not None
        return loop

    async def get_review_loop(self, loop_id: str) -> dict | None:
        async with self.db.execute(
            "SELECT * FROM review_loops WHERE id = ?", (loop_id,)
        ) as cursor:
            async for row in cursor:
                return _parse_loop(dict(row))
        return None

    async def get_review_loop_for_session(self, session_id: str) -> dict | None:
        """Most recent loop attached to a session (open first, then any)."""
        placeholders = ",".join("?" for _ in RL_OPEN_STATUSES)
        async with self.db.execute(
            f"""SELECT * FROM review_loops WHERE session_id = ?
                ORDER BY (status IN ({placeholders})) DESC, created_at DESC
                LIMIT 1""",
            (session_id, *RL_OPEN_STATUSES),
        ) as cursor:
            async for row in cursor:
                return _parse_loop(dict(row))
        return None

    async def list_review_loops(
        self, status: str | None = None, limit: int = 50, offset: int = 0,
    ) -> list[dict]:
        """List loops, newest first. ``status='open'`` selects running +
        parked; a concrete status filters exactly; empty returns all."""
        conditions: list[str] = []
        params: list = []
        if status == "open":
            placeholders = ",".join("?" for _ in RL_OPEN_STATUSES)
            conditions.append(f"status IN ({placeholders})")
            params.extend(RL_OPEN_STATUSES)
        elif status:
            conditions.append("status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.extend([limit, offset])
        async with self.db.execute(
            f"""SELECT * FROM review_loops {where}
                ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?""",
            params,
        ) as cursor:
            return [_parse_loop(dict(row)) async for row in cursor]

    async def get_open_review_loops(self) -> list[dict]:
        placeholders = ",".join("?" for _ in RL_OPEN_STATUSES)
        async with self.db.execute(
            f"""SELECT * FROM review_loops WHERE status IN ({placeholders})
                ORDER BY created_at ASC, id ASC""",
            RL_OPEN_STATUSES,
        ) as cursor:
            return [_parse_loop(dict(row)) async for row in cursor]

    async def update_review_loop(self, loop_id: str, fields: dict) -> bool:
        cols = _encode({k: v for k, v in fields.items() if k in _RL_UPDATABLE})
        if not cols:
            return False
        cols["updated_at"] = utc_now_iso()
        assignments = ", ".join(f"{k} = ?" for k in cols)
        result = await self._write(
            f"UPDATE review_loops SET {assignments} WHERE id = ?",
            (*cols.values(), loop_id),
        )
        return (result.rowcount or 0) == 1

    async def transition_review_loop(
        self,
        loop_id: str,
        to_status: str,
        expect: tuple[str, ...],
        **fields,
    ) -> bool:
        """CAS the loop to ``to_status`` iff current status ∈ ``expect``.
        The winner owns the transition's side effects (dispatch, cards,
        milestones). Terminal transitions stamp ``ended_at``."""
        if to_status not in RL_ALL_STATUSES:
            raise ValueError(f"unknown review loop status: {to_status}")
        now = utc_now_iso()
        cols = _encode({k: v for k, v in fields.items() if k in _RL_UPDATABLE})
        if to_status in RL_TERMINAL_STATUSES and "ended_at" not in cols:
            cols["ended_at"] = now
        assignments = ", ".join(
            ["status = ?", "updated_at = ?"] + [f"{k} = ?" for k in cols]
        )
        placeholders = ",".join("?" for _ in expect)
        result = await self._write(
            f"""UPDATE review_loops SET {assignments}
                WHERE id = ? AND status IN ({placeholders})""",
            (to_status, now, *cols.values(), loop_id, *expect),
        )
        return (result.rowcount or 0) == 1

    # ------------------------------------------------------------------ #
    #  Attempts (dispatch outbox / ledger)                                #
    # ------------------------------------------------------------------ #

    async def begin_leg_attempt(
        self,
        loop_id: str,
        to_status: str,
        expect: tuple[str, ...],
        *,
        iteration: int,
        role: str,
        run_id: str,
        loop_fields: dict | None = None,
    ) -> dict | None:
        """Atomically CAS the loop into a leg state AND insert the attempt
        row (status ``dispatching``) with a pre-generated run id — the
        dispatch outbox. Returns the attempt row, or None when the CAS
        lost (loop moved: killed, decided, or a duplicate signal)."""
        if to_status not in RL_ALL_STATUSES:
            raise ValueError(f"unknown review loop status: {to_status}")
        now = utc_now_iso()
        cols = _encode(
            {k: v for k, v in (loop_fields or {}).items() if k in _RL_UPDATABLE}
        )
        cols["current_run_id"] = run_id
        cols["iteration"] = iteration
        assignments = ", ".join(
            ["status = ?", "updated_at = ?"] + [f"{k} = ?" for k in cols]
        )
        placeholders = ",".join("?" for _ in expect)
        async with self._atomic():
            cursor = await self.db.execute(
                f"""UPDATE review_loops SET {assignments}
                    WHERE id = ? AND status IN ({placeholders})""",
                (to_status, now, *cols.values(), loop_id, *expect),
            )
            flipped = (cursor.rowcount or 0) == 1
            await cursor.close()
            if not flipped:
                return None
            async with self.db.execute(
                """SELECT COALESCE(MAX(attempt_no), 0) FROM review_loop_attempts
                   WHERE loop_id = ? AND iteration = ? AND role = ?""",
                (loop_id, iteration, role),
            ) as c2:
                attempt_no = 1
                async for row in c2:
                    attempt_no = int(row[0]) + 1
            await self.db.execute(
                """INSERT INTO review_loop_attempts
                   (loop_id, iteration, role, attempt_no, run_id, status, created_at)
                   VALUES (?, ?, ?, ?, ?, 'dispatching', ?)""",
                (loop_id, iteration, role, attempt_no, run_id, now),
            )
        return await self.get_attempt_by_run(run_id)

    async def get_attempt_by_run(self, run_id: str) -> dict | None:
        async with self.db.execute(
            "SELECT * FROM review_loop_attempts WHERE run_id = ?", (run_id,)
        ) as cursor:
            async for row in cursor:
                return _parse_attempt(dict(row))
        return None

    async def list_loop_attempts(self, loop_id: str) -> list[dict]:
        # id ASC == dispatch order == chronology (implement precedes verify
        # within an iteration; retries follow their originals).
        async with self.db.execute(
            """SELECT * FROM review_loop_attempts WHERE loop_id = ?
               ORDER BY id ASC""",
            (loop_id,),
        ) as cursor:
            return [_parse_attempt(dict(row)) async for row in cursor]

    async def settle_loop_attempt(
        self,
        run_id: str,
        status: str,
        *,
        spend_usd: float | None = None,
        verdict: dict | None = None,
        detail: dict | None = None,
    ) -> bool:
        """Settle an attempt exactly once (CAS on non-settled statuses).
        Returns False for a duplicate signal."""
        cols: dict = {"status": status, "settled_at": utc_now_iso()}
        if spend_usd is not None:
            cols["spend_usd"] = round(float(spend_usd), 6)
        if verdict is not None:
            cols["verdict"] = json.dumps(verdict)
        if detail is not None:
            cols["detail"] = json.dumps(detail)
        assignments = ", ".join(f"{k} = ?" for k in cols)
        result = await self._write(
            f"""UPDATE review_loop_attempts SET {assignments}
                WHERE run_id = ? AND status IN ('dispatching', 'running')""",
            (*cols.values(), run_id),
        )
        return (result.rowcount or 0) == 1

    async def mark_attempt_running(self, run_id: str) -> bool:
        result = await self._write(
            """UPDATE review_loop_attempts SET status = 'running'
               WHERE run_id = ? AND status = 'dispatching'""",
            (run_id,),
        )
        return (result.rowcount or 0) == 1

    async def sum_loop_spend(self, loop_id: str) -> float:
        async with self.db.execute(
            "SELECT COALESCE(SUM(spend_usd), 0.0) FROM review_loop_attempts WHERE loop_id = ?",
            (loop_id,),
        ) as cursor:
            async for row in cursor:
                return float(row[0] or 0.0)
        return 0.0

    # ------------------------------------------------------------------ #
    #  Escalation support                                                 #
    # ------------------------------------------------------------------ #

    async def get_pending_approval_for_target(
        self, target_kind: str, target_id: str,
    ) -> dict | None:
        """Newest pending approval-kind notification for a target — used by
        the review-loop reconciliation tick to detect dead cards."""
        async with self.db.execute(
            """SELECT * FROM notifications
               WHERE type = 'approval' AND status = 'pending'
                 AND target_kind = ? AND target_id = ?
               ORDER BY created_at DESC LIMIT 1""",
            (target_kind, target_id),
        ) as cursor:
            async for row in cursor:
                return dict(row)
        return None
