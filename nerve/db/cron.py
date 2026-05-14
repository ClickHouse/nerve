"""Cron log data access methods."""

from __future__ import annotations

from nerve.utils.time import utc_now_iso


class CronStore:
    """Mixin providing cron job logging operations."""

    async def log_cron_start(self, job_id: str) -> int:
        async with self.db.execute(
            "INSERT INTO cron_logs (job_id) VALUES (?)", (job_id,)
        ) as cursor:
            log_id = cursor.lastrowid
        await self.db.commit()
        return log_id

    async def log_cron_finish(
        self, log_id: int, status: str, output: str | None = None, error: str | None = None
    ) -> None:
        now = utc_now_iso()
        await self.db.execute(
            "UPDATE cron_logs SET finished_at = ?, status = ?, output = ?, error = ? WHERE id = ?",
            (now, status, output, error, log_id),
        )
        await self.db.commit()

    async def get_cron_logs(self, job_id: str | None = None, limit: int = 50) -> list[dict]:
        if job_id:
            query = "SELECT * FROM cron_logs WHERE job_id = ? ORDER BY started_at DESC LIMIT ?"
            params = (job_id, limit)
        else:
            query = "SELECT * FROM cron_logs ORDER BY started_at DESC LIMIT ?"
            params = (limit,)
        async with self.db.execute(query, params) as cursor:
            return [dict(row) async for row in cursor]

    async def get_last_successful_cron_run(self, job_id: str) -> dict | None:
        """Get the most recent successful cron_logs entry for a job."""
        async with self.db.execute(
            "SELECT * FROM cron_logs WHERE job_id = ? AND status = 'success' ORDER BY finished_at DESC LIMIT 1",
            (job_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

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
        cursor = await self.db.execute(
            "DELETE FROM cron_logs WHERE started_at < datetime('now', ? || ' days')",
            (f"-{days}",),
        )
        deleted = cursor.rowcount or 0
        await cursor.close()
        await self.db.commit()
        return deleted
