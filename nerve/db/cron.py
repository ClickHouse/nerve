"""Cron log data access methods."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime

from nerve.db.usage import _CRON_RUN_STAMP_GLOB
from nerve.identity import UNATTRIBUTED, Attribution
from nerve.utils.time import utc_now_iso

#: A source run that found nothing new. Agent and workflow runs have no
#: ``records`` (NULL), so they never match. ``IS`` keeps the result true or
#: false for a NULL, so ``NOT EMPTY_RUN`` keeps those rows.
EMPTY_RUN = "(records IS 0 AND status IS 'success')"


#: The finished runs that tell whether a job works. A missed run did not run,
#: and a run that is still going has no result.
_COMPLETED = "status IN ('success', 'error')"


#: What follows ``cron:<job>:`` in the id of a session of one run or one
#: generation of the job: the run id that ``new_cron_run_id`` makes, and the
#: older one without the random tail.
_RUN_ID_GLOBS = (
    _CRON_RUN_STAMP_GLOB + "-" + "[0-9a-f]" * 8,
    _CRON_RUN_STAMP_GLOB,
)


def _cron_session_of(expr: str, job_id: str) -> tuple[str, list]:
    """SQL, with its parameters, that is true when *expr* is the id of a cron
    session of *job_id*: ``cron:<job>`` or ``cron:<job>:<run id>``.

    The job id is compared as text, never as a pattern, and what follows it
    must be a run id: ``cron:a:b`` is the session of the job ``a:b``, not a
    run of ``a``.
    """
    prefix = f"cron:{job_id}:"
    rest = f"substr({expr}, ?)"
    return (
        f"({expr} = ? OR (substr({expr}, 1, ?) = ? "
        f"AND ({rest} GLOB ? OR {rest} GLOB ?)))",
        [
            f"cron:{job_id}", len(prefix), prefix,
            len(prefix) + 1, _RUN_ID_GLOBS[0], len(prefix) + 1, _RUN_ID_GLOBS[1],
        ],
    )


def _sessions_of_jobs(
    column: str, job_ids: list[str], *, runs_by_id: bool = False,
) -> tuple[str, list]:
    """SQL, with its parameters, that is true when *column* holds the id of a
    session of one of *job_ids*. *column* names its table, since the SQL
    refers to it from a subquery.

    A session of a job is a cron session of it, or the session of a workflow
    run that the job started (``created_by`` is ``cron:<job>``), that one of
    its cron sessions started (``session:<session id>``), or that is a leg of
    a review loop which one of its cron sessions started
    (``review-loop:<loop id>``, the loop's ``session_id``). A workflow run
    can start neither, so there is nothing further down.

    The run of a session is the one whose ``session_id`` is that session.
    The column is emptied when the session is deleted, so a record that
    keeps a session id for longer than the session lives passes
    ``runs_by_id``: the run is then the one named in the id itself,
    ``workflow:<run id>``.
    """
    run_of_session = (
        f"w.id = substr({column}, 10) AND {column} GLOB 'workflow:?*'"
        if runs_by_id else f"w.session_id = {column}"
    )
    direct, started, params, started_params = [], [], [], []
    for job_id in job_ids:
        sql, sql_params = _cron_session_of(column, job_id)
        direct.append(sql)
        params += sql_params
        by_session, by_session_params = _cron_session_of("substr(w.created_by, 9)", job_id)
        by_loop, by_loop_params = _cron_session_of("l.session_id", job_id)
        started.append(
            "w.created_by = ? "
            f"OR (substr(w.created_by, 1, 8) = 'session:' AND {by_session}) "
            "OR (substr(w.created_by, 1, 12) = 'review-loop:' AND EXISTS ("
            "SELECT 1 FROM review_loops l "
            f"WHERE l.id = substr(w.created_by, 13) AND {by_loop}))"
        )
        started_params += [f"cron:{job_id}", *by_session_params, *by_loop_params]
    return (
        f"{' OR '.join(direct)} OR EXISTS ("
        f"SELECT 1 FROM workflow_runs w WHERE {run_of_session} "
        f"AND ({' OR '.join(started)}))",
        params + started_params,
    )


def _ranked(where: str = "") -> str:
    """The rows of ``cron_logs`` (with ``where``), each with ``row_rank``: 1 for
    the latest row of its job, by start and then id, as the history lists them."""
    return (
        "SELECT *, ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY started_at DESC, id DESC) "
        f"AS row_rank FROM cron_logs {where}"
    )


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
        """Close a run. ``records``: the records that a source run fetched
        (stored, plus dropped by the inbox guardrail)."""
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
        async with self.db.execute(
            f"SELECT job_id, status, started_at, finished_at FROM ({_ranked()}) "
            "WHERE row_rank = 1",
        ) as cursor:
            return {
                row["job_id"]: {
                    "status": row["status"],
                    "started_at": row["started_at"],
                    "finished_at": row["finished_at"],
                }
                async for row in cursor
            }

    async def get_previous_cron_status(self, log_id: int) -> str | None:
        """The status of the run before run ``log_id``: the latest run of the
        same job that started before it and has a result.

        Only ``success`` and ``error`` count (``_COMPLETED``). A run that
        started earlier and is still going is skipped, so two runs that
        overlap can both see the run before them, but neither misses it.
        None when there is no such run.
        """
        async with self.db.execute(
            "SELECT p.status FROM cron_logs AS p JOIN cron_logs AS c ON c.id = ? "
            "AND p.job_id = c.job_id "
            f"WHERE p.{_COMPLETED} AND (p.started_at < c.started_at "
            "OR (p.started_at = c.started_at AND p.id < c.id)) "
            "ORDER BY p.started_at DESC, p.id DESC LIMIT 1",
            (log_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return row["status"] if row else None

    async def get_last_successful_cron_run(self, job_id: str) -> dict | None:
        """Get the most recent successful cron_logs entry for a job."""
        async with self.db.execute(
            "SELECT * FROM cron_logs WHERE job_id = ? AND status = 'success' ORDER BY finished_at DESC LIMIT 1",
            (job_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def has_activity_since_cron_run(
        self,
        job_id: str,
        *,
        ignore_jobs: Iterable[str] = (),
        overlap_seconds: int = 0,
        until: datetime | None = None,
    ) -> bool:
        """Whether the agent has worked since the job's last successful run
        started, not counting the work of the job itself or of a job of
        ``ignore_jobs``.

        Two records count as work, each in a session that belongs to none of
        those jobs:

        * a row of ``session_usage``: a turn ended and reported its usage,
          whatever started it (a chat, a channel message, a wakeup, a hook,
          another cron job, a workflow run);
        * a session whose ``updated_at`` moved: a message was stored in it.
          A turn that somebody asked for stores what it was asked when it
          starts, so such a turn counts from then on, and also when it is
          stopped or fails before it reports any usage; so does a
          conversation taken in from an external agent. A turn that the
          agent starts for itself (a wakeup, a continuation after background
          work) stores its messages without moving ``updated_at``: it counts
          by its usage row, when it ends with one.

        ``overlap_seconds`` moves the start of the window back by that much,
        so what happened in that time before the last run counts again. A job
        with no successful run in ``cron_logs`` has no window start: any work
        counts. The cleanup keeps the latest finished run of each job, so the
        start outlives the 14 days unless a later run failed.

        ``until`` ends the window: only work recorded before it counts. A
        catch-up asks about the time the run it stands in for came due. It
        must be timezone-aware: a naive time raises ``ValueError``. A session
        has one ``updated_at``, the time of its latest message, so a message
        stored after ``until`` hides the session's earlier ones from this
        record; a turn that reported its usage before ``until`` still counts.

        The sessions of a job are its cron sessions, ``cron:<job>`` and
        ``cron:<job>:<run id>``, and the sessions of the workflow runs that
        the job or one of those sessions started, a review loop's legs
        included (:func:`_sessions_of_jobs`).
        """
        async with self.db.execute(
            "SELECT datetime(MAX(datetime(started_at)), ?) FROM cron_logs "
            "WHERE job_id = ? AND status = 'success'",
            (f"-{max(0, int(overlap_seconds))} seconds", job_id),
        ) as cursor:
            row = await cursor.fetchone()
        since = row[0] if row else None
        if until is not None and until.utcoffset() is None:
            raise ValueError(f"has_activity_since_cron_run() needs a timezone-aware until, not {until!r}")
        before = until.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S") if until else None

        jobs = list(dict.fromkeys((job_id, *ignore_jobs)))
        # Both columns hold UTC. session_usage.created_at is SQLite's
        # CURRENT_TIMESTAMP text, the form datetime() returns, so it compares
        # as text. sessions.updated_at is ISO 8601 where a message moved it
        # and CURRENT_TIMESTAMP text where none has: the day picks the rows
        # through the index, and datetime() compares what either form says.
        checks = (
            ("session_usage.session_id", "created_at >= ?", [since], "created_at < ?", ""),
            (
                "sessions.id",
                "updated_at >= ? AND datetime(updated_at) >= ?",
                [since[:10], since] if since else [],
                "datetime(updated_at) < ?",
                "COALESCE(message_count, 0) > 0",
            ),
        )
        for column, window, window_params, end, extra in checks:
            clauses, params = [], []
            if since is not None:
                clauses.append(window)
                params += window_params
            if before is not None:
                clauses.append(end)
                params.append(before)
            if extra:
                clauses.append(extra)
            mine, mine_params = _sessions_of_jobs(column, jobs)
            clauses.append(f"NOT ({mine})")
            params += mine_params
            table = column.partition(".")[0]
            async with self.db.execute(
                f"SELECT 1 FROM {table} WHERE {' AND '.join(clauses)} LIMIT 1",
                tuple(params),
            ) as cursor:
                if await cursor.fetchone() is not None:
                    return True
        return False

    async def count_cron_job_plans_in_review(self, job_id: str) -> int:
        """How many of the job's proposals wait for review.

        A proposal is a task with a plan in review (``pending``: proposed,
        and neither approved nor declined). A plan is in review until
        somebody decides it, however old it is and whatever became of its
        task: closing a task decides no plan, and the Plans page goes on
        listing it. So the count is what a person finds there to decide. A
        plan whose task has no row is left out: no decision could be made
        on it.

        The proposal is the job's when one of the job's sessions
        (:func:`_sessions_of_jobs`) proposed the plan in review, or a plan
        that it replaces: a ``superseded`` plan of the task with no decided
        plan after it. So a revision counts whichever session wrote it: a
        person's chat, or the planner session that takes the revision of a
        plan whose own session was deleted. A plan of the job that was
        decided does not make a later plan for the same task the job's.

        A plan keeps the id of the session that proposed it after that
        session is deleted, so a workflow run is found by the run id in the
        session id. A leg of a review loop is still known by the loop's link
        to the session that started it, which goes with that session.
        """
        mine, params = _sessions_of_jobs("q.session_id", [job_id], runs_by_id=True)
        async with self.db.execute(
            "SELECT COUNT(DISTINCT p.task_id) FROM plans p "
            "JOIN tasks t ON t.id = p.task_id "
            "WHERE p.status = 'pending' AND EXISTS ("
            "SELECT 1 FROM plans q WHERE q.task_id = p.task_id "
            f"AND q.status IN ('pending', 'superseded') AND ({mine}) "
            "AND NOT EXISTS (SELECT 1 FROM plans d WHERE d.task_id = q.task_id "
            "AND d.version > q.version "
            "AND d.status NOT IN ('pending', 'superseded')))",
            tuple(params),
        ) as cursor:
            row = await cursor.fetchone()
        return int(row[0]) if row else 0

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

        The latest finished run of each job (``_COMPLETED``) stays, however
        old: the failure notice of the next run compares with it, so a job
        that runs less often than every ``days`` days and keeps failing does
        not send the notice again.
        """
        result = await self._write(
            "DELETE FROM cron_logs WHERE started_at < datetime('now', ? || ' days') "
            f"AND id NOT IN (SELECT id FROM ({_ranked('WHERE ' + _COMPLETED)}) "
            "WHERE row_rank = 1)",
            (f"-{days}",),
        )
        return result.rowcount or 0
