"""Cron log data access methods."""

from __future__ import annotations

from nerve.identity import UNATTRIBUTED, Attribution
from nerve.utils.time import utc_now_iso

#: A source run that found nothing new. Agent and workflow runs have no
#: ``records`` (NULL), so they never match. ``IS`` keeps the result true or
#: false for a NULL, so ``NOT EMPTY_RUN`` keeps those rows.
EMPTY_RUN = "(records IS 0 AND status IS 'success')"


def _logs_filter(job_id: str | None, empty: bool | None) -> tuple[str, tuple]:
    """The WHERE clause and its parameters for a list of runs.

    ``empty``: None for all runs, False without the runs that found nothing
    new, True for only those runs.
    """
    clauses: list[str] = []
    params: tuple = ()
    if job_id:
        clauses.append("job_id = ?")
        params = (job_id,)
    if empty is not None:
        clauses.append(EMPTY_RUN if empty else f"NOT {EMPTY_RUN}")
    return ("WHERE " + " AND ".join(clauses) if clauses else ""), params


class CronStore:
    """Mixin providing cron job logging operations.

    Each run row records who started the run (``actor_id``) and on whose
    behalf it happens (``caused_by_actor_id``): the system actor for a
    scheduled run, the person for a run that they started through the API.
    """

    async def log_cron_start(
        self, job_id: str, *, attribution: Attribution = UNATTRIBUTED,
    ) -> int:
        result = await self._write(
            "INSERT INTO cron_logs (job_id, actor_id, caused_by_actor_id) "
            "VALUES (?, ?, ?)",
            (job_id, attribution.actor_id, attribution.caused_by_actor_id),
        )
        return result.lastrowid

    async def set_cron_log_session(self, log_id: int, session_id: str) -> None:
        """Link a run log to its session while the run is still in flight.

        Written at run start so the UI can open the chat of a running cron;
        log_cron_finish keeps it as a fallback for older code paths.
        """
        await self._write(
            "UPDATE cron_logs SET session_id = ? WHERE id = ?",
            (session_id, log_id),
        )

    async def log_cron_finish(
        self,
        log_id: int,
        status: str,
        output: str | None = None,
        error: str | None = None,
        session_id: str | None = None,
        records: int | None = None,
    ) -> None:
        """Close a run. ``records`` is the number of new records of a source run."""
        now = utc_now_iso()
        await self._write(
            "UPDATE cron_logs SET finished_at = ?, status = ?, output = ?, error = ?, "
            "session_id = COALESCE(?, session_id), records = ? WHERE id = ?",
            (now, status, output, error, session_id, records, log_id),
        )

    async def log_cron_missed(
        self, job_id: str, scheduled_run_time: str, error: str | None = None,
        *, attribution: Attribution = UNATTRIBUTED,
    ) -> int:
        """Record a run the scheduler dropped for starting too late.

        Written already closed out: the run never started, so nothing will call
        log_cron_finish for it. ``started_at`` is when the run was *due*, which
        is what lines the row up with the runs around it. The ``missed`` status
        keeps it out of get_last_successful_cron_run, so interval alignment and
        startup catch-up are unaffected.
        """
        now = utc_now_iso()
        result = await self._write(
            "INSERT INTO cron_logs (job_id, started_at, finished_at, status, error, "
            "actor_id, caused_by_actor_id) VALUES (?, ?, ?, 'missed', ?, ?, ?)",
            (
                job_id, scheduled_run_time, now, error,
                attribution.actor_id, attribution.caused_by_actor_id,
            ),
        )
        return result.lastrowid

    async def get_cron_logs(
        self, job_id: str | None = None, limit: int = 50, offset: int = 0,
        *, hide_empty: bool = False,
    ) -> list[dict]:
        """Runs newest first. ``hide_empty`` leaves out the source runs that
        found nothing new (see :data:`EMPTY_RUN`)."""
        where, params = _logs_filter(job_id, False if hide_empty else None)
        # Secondary sort on id keeps pagination stable when several runs
        # share the same started_at second.
        async with self.db.execute(
            f"SELECT * FROM cron_logs {where} "
            "ORDER BY started_at DESC, id DESC LIMIT ? OFFSET ?",
            (*params, limit, offset),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def count_cron_logs(
        self, job_id: str | None = None, *, hide_empty: bool = False,
    ) -> int:
        return await self._count_cron_logs(job_id, False if hide_empty else None)

    async def count_empty_cron_logs(self, job_id: str | None = None) -> int:
        """The number of source runs that found nothing new."""
        return await self._count_cron_logs(job_id, True)

    async def _count_cron_logs(self, job_id: str | None, empty: bool | None) -> int:
        where, params = _logs_filter(job_id, empty)
        async with self.db.execute(
            f"SELECT COUNT(*) FROM cron_logs {where}", params,
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_cron_job_stats(self, days: int = 7) -> dict[str, dict[str, int]]:
        """Per job, the runs that started in the last ``days`` days, by status.

        ``{job_id: {"runs", "success", "error", "missed"}}``. ``runs`` counts
        every run, also one that is still going.
        """
        stats: dict[str, dict[str, int]] = {}
        async with self.db.execute(
            "SELECT job_id, COUNT(*) AS runs, "
            "SUM(status = 'success') AS success, SUM(status = 'error') AS error, "
            "SUM(status = 'missed') AS missed "
            "FROM cron_logs WHERE started_at >= datetime('now', ? || ' days') "
            "GROUP BY job_id",
            (f"-{days}",),
        ) as cursor:
            async for row in cursor:
                stats[row["job_id"]] = {
                    key: int(row[key] or 0) for key in ("runs", "success", "error", "missed")
                }
        return stats

    async def get_last_cron_runs(self) -> dict[str, dict]:
        """Per job, its latest run: ``{job_id: {"status", "started_at", "finished_at"}}``.

        ``status`` is None while the run is going.
        """
        # With MAX(), SQLite takes the bare columns from the row that has the
        # maximum, so this is the latest row of each job in one pass.
        async with self.db.execute(
            "SELECT job_id, status, started_at, finished_at, MAX(started_at) "
            "FROM cron_logs GROUP BY job_id",
        ) as cursor:
            return {
                row["job_id"]: {
                    "status": row["status"],
                    "started_at": row["started_at"],
                    "finished_at": row["finished_at"],
                }
                async for row in cursor
            }

    async def get_last_successful_cron_run(self, job_id: str) -> dict | None:
        """Get the most recent successful cron_logs entry for a job."""
        async with self.db.execute(
            "SELECT * FROM cron_logs WHERE job_id = ? AND status = 'success' ORDER BY finished_at DESC LIMIT 1",
            (job_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_latest_cron_session_id(self, job_id: str) -> str | None:
        """Return the most recently active session id for a cron job.

        Matches both the persistent form (``cron:<job>``) and per-run
        isolated sessions (``cron:<job>:<run>``).
        """
        escaped = (
            job_id.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        )
        async with self.db.execute(
            """SELECT id FROM sessions
               WHERE id = ? OR id LIKE ? ESCAPE '\\'
               ORDER BY COALESCE(last_activity_at, updated_at, created_at) DESC
               LIMIT 1""",
            (f"cron:{job_id}", f"cron:{escaped}:%"),
        ) as cursor:
            row = await cursor.fetchone()
            return row["id"] if row else None

    async def get_recent_cron_runs(self, hours: int = 6) -> list[dict]:
        """Get all successful cron runs within the last N hours."""
        async with self.db.execute(
            """SELECT job_id, finished_at FROM cron_logs
               WHERE status = 'success'
               AND finished_at > datetime('now', ? || ' hours')
               ORDER BY finished_at DESC""",
            (f"-{hours}",),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def cleanup_old_cron_logs(self, days: int = 14) -> int:
        """Delete cron_logs entries older than ``days``. Returns rows deleted.

        Cron logs grow unbounded — a single source running every 5 minutes
        produces ~100 rows/day. Without retention the table reaches tens of
        thousands of rows and the unfiltered "latest N" query (used by the
        diagnostics endpoint) degrades to a full-table scan + memory sort.
        """
        result = await self._write(
            "DELETE FROM cron_logs WHERE started_at < datetime('now', ? || ' days')",
            (f"-{days}",),
        )
        return result.rowcount or 0
