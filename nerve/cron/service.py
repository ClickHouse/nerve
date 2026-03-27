"""Cron scheduler — APScheduler integration.

Runs cron jobs and source runners on schedule.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from nerve.agent.engine import AgentEngine
from nerve.config import NerveConfig
from nerve.cron.jobs import CronJob, load_jobs
from nerve.db import Database

if TYPE_CHECKING:
    from nerve.sources.runner import SourceRunner

logger = logging.getLogger(__name__)


def _parse_interval(interval: str) -> int:
    """Parse an interval string like '2h', '30m', '1h30m' into seconds."""
    import re
    total = 0
    parts = re.findall(r"(\d+)([hms])", interval.lower())
    for value, unit in parts:
        v = int(value)
        if unit == "h":
            total += v * 3600
        elif unit == "m":
            total += v * 60
        elif unit == "s":
            total += v
    return total or 7200  # Default 2h


def _parse_timestamp(ts: str) -> datetime:
    """Parse a UTC timestamp string from the database into an aware datetime."""
    if "T" not in ts:
        ts = ts.replace(" ", "T")
    if not ts.endswith(("Z", "+00:00")):
        ts += "+00:00"
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


class CronService:
    """Manages scheduled cron jobs."""

    def __init__(self, config: NerveConfig, engine: AgentEngine, db: Database):
        self.config = config
        self.engine = engine
        self.db = db
        self.scheduler = AsyncIOScheduler()
        self._jobs: list[CronJob] = []
        self._source_runners: list[SourceRunner] = []
        self._job_locks: dict[str, asyncio.Lock] = {}

    async def start(self) -> None:
        """Load jobs and start the scheduler."""
        # Load job definitions from both files
        self._jobs = self._load_merged_jobs()

        # Register cron jobs with persistent timer alignment
        for job in self._jobs:
            if not job.enabled:
                continue

            trigger = await self._make_trigger(job)

            self.scheduler.add_job(
                self._run_job_wrapper,
                trigger,
                args=[job],
                id=job.id,
                name=job.description or job.id,
                replace_existing=True,
            )
            logger.info("Scheduled job: %s (%s)", job.id, job.schedule)

        # Register source runners (pure ingestors — no engine needed)
        try:
            from nerve.sources.registry import build_source_runners

            self._source_runners = build_source_runners(self.config, self.db)

            for runner in self._source_runners:
                source_name = runner.source.source_name
                # Source names can be compound (e.g. "gmail:account@email.com").
                # The config key is the base type before the colon.
                config_key = source_name.split(":")[0]
                source_config = getattr(self.config.sync, config_key, None)
                if source_config is None:
                    continue
                schedule_str = getattr(source_config, "schedule", "*/15 * * * *")

                try:
                    trigger = CronTrigger.from_crontab(schedule_str)
                except ValueError:
                    seconds = _parse_interval(schedule_str)
                    trigger = IntervalTrigger(seconds=seconds)

                self.scheduler.add_job(
                    self._run_source_wrapper,
                    trigger,
                    args=[runner],
                    id=runner.job_id,
                    name=f"Source: {source_name}",
                    replace_existing=True,
                )
                logger.info("Scheduled source: %s (%s)", source_name, schedule_str)
        except Exception as e:
            logger.warning("Failed to register source runners: %s", e, exc_info=True)

        # Daily cleanup of expired messages and consumer cursors
        self.scheduler.add_job(
            self._cleanup_expired,
            CronTrigger(hour=3, minute=0),
            id="cleanup",
            name="Cleanup expired data",
            replace_existing=True,
        )

        self.scheduler.start()
        logger.info(
            "Cron service started with %d jobs + %d sources",
            len(self._jobs), len(self._source_runners),
        )

        # Catch up missed jobs in background (don't block startup)
        asyncio.create_task(self._catchup_missed_jobs())

    async def stop(self) -> None:
        """Stop the scheduler."""
        self.scheduler.shutdown(wait=False)
        logger.info("Cron service stopped")

    # -- Persistent timers -------------------------------------------------

    async def _make_trigger(self, job: CronJob) -> CronTrigger | IntervalTrigger:
        """Create an APScheduler trigger for a job.

        For interval schedules, anchors to the last successful run so
        the cadence survives restarts (persistent timer).
        """
        try:
            return CronTrigger.from_crontab(job.schedule)
        except ValueError:
            pass

        seconds = _parse_interval(job.schedule)
        last_run = await self.db.get_last_successful_cron_run(job.id)
        if last_run and last_run.get("finished_at"):
            start_date = _parse_timestamp(last_run["finished_at"])
            logger.debug(
                "Aligning interval for %s: start_date=%s", job.id, start_date,
            )
            return IntervalTrigger(seconds=seconds, start_date=start_date)
        return IntervalTrigger(seconds=seconds)

    async def _catchup_missed_jobs(self) -> None:
        """Fire jobs that should have run while the server was down.

        Each overdue job fires exactly once regardless of how many runs
        were missed.  Jobs run concurrently.
        """
        now = datetime.now(timezone.utc)
        overdue: list[CronJob] = []

        for job in self._jobs:
            if not job.enabled or not job.catchup:
                continue

            last_run = await self.db.get_last_successful_cron_run(job.id)
            if not last_run or not last_run.get("finished_at"):
                continue  # first-ever run — no catch-up

            last_time = _parse_timestamp(last_run["finished_at"])
            if self._is_overdue(job, last_time, now):
                overdue.append(job)

        if not overdue:
            return

        logger.info(
            "Catching up %d missed jobs: %s",
            len(overdue), [j.id for j in overdue],
        )
        await asyncio.gather(
            *(self._run_job_wrapper(job) for job in overdue),
        )

    @staticmethod
    def _is_overdue(job: CronJob, last_run: datetime, now: datetime) -> bool:
        """Check if a job should have fired between *last_run* and *now*."""
        try:
            trigger = CronTrigger.from_crontab(job.schedule)
            next_fire = trigger.get_next_fire_time(last_run, last_run)
            return next_fire is not None and next_fire < now
        except ValueError:
            seconds = _parse_interval(job.schedule)
            return (now - last_run).total_seconds() >= seconds

    # -- End persistent timers ---------------------------------------------

    async def _maybe_rotate_context(
        self, session_id: str, rotate_hours: int,
        rotate_at: str = "",
    ) -> bool:
        """Check if a persistent cron session's context should be rotated.

        Rotation clears the sdk_session_id so the next run starts a fresh
        SDK client.  Old messages remain in the DB for memU search.

        If rotate_at is set (e.g. "04:00"), rotation happens once per day
        at that local time instead of using the hours-based approach.

        Returns True if rotation was performed.
        """
        session = await self.db.get_session(session_id)
        if not session or not session.get("connected_at"):
            return False

        connected_at_str = session["connected_at"]
        try:
            ts = connected_at_str
            if "T" not in ts:
                ts = ts.replace(" ", "T")
            if not ts.endswith(("Z", "+00:00")):
                ts += "+00:00"
            connected_at = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            logger.warning(
                "Invalid connected_at for %s: %s", session_id, connected_at_str,
            )
            return False

        now = datetime.now(timezone.utc)
        should_rotate = False
        reason = ""

        if rotate_at:
            # Time-of-day rotation: rotate if session started before today's
            # rotate_at and current time is past it.
            try:
                hour, minute = (int(x) for x in rotate_at.split(":"))
            except (ValueError, TypeError):
                logger.warning("Invalid context_rotate_at: %s", rotate_at)
                return False

            local_tz = datetime.now().astimezone().tzinfo
            today_rotate = datetime.now(local_tz).replace(
                hour=hour, minute=minute, second=0, microsecond=0,
            )
            today_rotate_utc = today_rotate.astimezone(timezone.utc)

            if now >= today_rotate_utc and connected_at < today_rotate_utc:
                should_rotate = True
                reason = f"rotate_at={rotate_at}"
        elif rotate_hours > 0:
            age_hours = (now - connected_at).total_seconds() / 3600
            if age_hours >= rotate_hours:
                should_rotate = True
                reason = f"age {age_hours:.1f}h >= {rotate_hours}h"

        if not should_rotate:
            return False

        # Memorize current context before rotation (safety net)
        try:
            await self.engine._memorize_session(session_id)
        except Exception as e:
            logger.warning("Pre-rotation memorize failed for %s: %s", session_id, e)

        # Clear sdk_session_id + connected_at → next run starts fresh
        await self.engine.sessions.mark_idle(session_id, preserve_sdk_id=False)
        logger.info(
            "Rotated context for persistent cron %s (%s)",
            session_id, reason,
        )
        return True

    def _load_merged_jobs(self) -> list[CronJob]:
        """Load and merge jobs from system.yaml and jobs.yaml.

        System jobs come from system.yaml (managed by `nerve init`).
        User jobs come from jobs.yaml (user-defined, never touched by Nerve).
        If a user job has the same ID as a system job, the user version wins.
        """
        system_file = self.config.cron.system_file
        jobs_file = self.config.cron.jobs_file

        system_jobs = load_jobs(system_file)
        user_jobs = load_jobs(jobs_file)

        if not system_jobs and user_jobs:
            # Backward compat: old install with everything in jobs.yaml
            logger.info(
                "No system.yaml found — loading all crons from jobs.yaml "
                "(run 'nerve init' to split)"
            )
            # Tag all as user-sourced (no system file yet)
            for j in user_jobs:
                j.metadata["_source"] = "user"
            return user_jobs

        # Tag sources for display in CLI
        for j in system_jobs:
            j.metadata["_source"] = "system"
        for j in user_jobs:
            j.metadata["_source"] = "user"

        # Merge: user jobs override system jobs with same ID
        system_ids = {j.id for j in system_jobs}
        for job in user_jobs:
            if job.id in system_ids:
                logger.warning(
                    "User job '%s' shadows system job — user version used",
                    job.id,
                )

        jobs_by_id = {j.id: j for j in system_jobs}
        for j in user_jobs:
            jobs_by_id[j.id] = j

        return list(jobs_by_id.values())

    async def _has_pending_messages(
        self, consumer: str, sources: list[str],
    ) -> bool:
        """Check if any of the listed sources have unread messages.

        Uses existing consumer cursor position vs source max rowid.
        Does not advance any cursors.
        """
        for source in sources:
            cursor_seq = await self.db.get_consumer_cursor(consumer, source)
            max_seq = await self.db.get_source_max_rowid(source)
            if max_seq > cursor_seq:
                return True
        return False

    async def _run_job_wrapper(self, job: CronJob) -> None:
        """Wrapper to run a cron job with logging and optional lock."""
        if job.lock:
            lock = self._job_locks.setdefault(job.id, asyncio.Lock())
            async with lock:
                await self._run_job_inner(job)
        else:
            await self._run_job_inner(job)

    async def _run_job_inner(self, job: CronJob) -> None:
        """Inner implementation of job execution."""
        # Pre-check: skip if no new messages in monitored sources
        if job.skip_when_idle:
            has_pending = await self._has_pending_messages(
                job.idle_consumer, job.skip_when_idle,
            )
            if not has_pending:
                logger.info(
                    "Skipping cron job %s: no new messages in %s",
                    job.id, job.skip_when_idle,
                )
                return

        log_id = await self.db.log_cron_start(job.id)
        logger.info("Running cron job: %s (mode=%s)", job.id, job.session_mode)

        try:
            model = job.model or self.config.agent.cron_model
            rotated = False

            if job.session_mode == "persistent":
                # Persistent mode: reuse SDK context across runs
                if job.context_rotate_at or job.context_rotate_hours > 0:
                    rotated = await self._maybe_rotate_context(
                        f"cron:{job.id}", job.context_rotate_hours,
                        rotate_at=job.context_rotate_at,
                    )

                # Determine prompt: full on first run, short reminder on subsequent
                prompt = job.prompt
                if job.reminder_mode:
                    session = await self.db.get_session(f"cron:{job.id}")
                    is_resume = (
                        session
                        and session.get("sdk_session_id")
                        and not rotated
                    )
                    if is_resume:
                        prompt = (
                            "Scheduled run — continue with the same "
                            "task as before."
                        )
                    else:
                        prompt = job.prompt.rstrip() + (
                            "\n\n---\n"
                            "NOTE: This is a persistent cron with reminder "
                            "mode. On subsequent triggers you will receive "
                            "a short reminder instead of this full prompt. "
                            "Continue executing these instructions each time."
                        )

                response = await self.engine.run_persistent_cron(
                    job_id=job.id,
                    prompt=prompt,
                    model=model,
                )
            else:
                # Isolated mode: per-run session (existing behaviour)
                run_id = (
                    datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
                    if self.config.sessions.cron_session_mode == "per_run"
                    else None
                )
                response = await self.engine.run_cron(
                    job_id=job.id,
                    prompt=job.prompt,
                    model=model,
                    run_id=run_id,
                )

            output = response[:2000]
            if rotated:
                output = "[context rotated] " + output
            await self.db.log_cron_finish(log_id, "success", output=output)
            logger.info("Cron job %s completed (%d chars)", job.id, len(response))

        except Exception as e:
            logger.error("Cron job %s failed: %s", job.id, e, exc_info=True)
            await self.db.log_cron_finish(log_id, "error", error=str(e))

    async def _run_source_wrapper(self, runner: SourceRunner) -> None:
        """Wrapper to run a source ingestion with cron and source logging."""
        log_id = await self.db.log_cron_start(runner.job_id)
        logger.info("Running source: %s", runner.source.source_name)

        try:
            result = await runner.run()
            summary = f"{result.records_ingested} ingested"
            if result.error:
                summary += f", error: {result.error}"

            status = "success" if result.error is None else "error"
            await self.db.log_cron_finish(log_id, status, output=summary[:2000])
            await self.db.log_source_run(
                source=runner.source.source_name,
                records_fetched=result.records_ingested,
                records_processed=result.records_ingested,
                error=result.error,
            )
            logger.info("Source %s done: %s", runner.source.source_name, summary)
        except Exception as e:
            logger.error("Source %s failed: %s", runner.source.source_name, e, exc_info=True)
            await self.db.log_cron_finish(log_id, "error", error=str(e))
            await self.db.log_source_run(
                source=runner.source.source_name,
                error=str(e),
            )

    async def _cleanup_expired(self) -> None:
        """Clean up expired source messages and consumer cursors."""
        try:
            msg_count = await self.db.cleanup_expired_messages()
            cursor_count = await self.db.cleanup_expired_consumer_cursors()
            if msg_count or cursor_count:
                logger.info(
                    "Cleanup: %d expired messages, %d expired consumer cursors",
                    msg_count, cursor_count,
                )
        except Exception as e:
            logger.error("Cleanup failed: %s", e, exc_info=True)

    async def run_job(self, job_id: str) -> None:
        """Run a specific job manually (used by CLI)."""
        job = next((j for j in self._jobs if j.id == job_id), None)
        if not job:
            # Try loading fresh from both files
            self._jobs = self._load_merged_jobs()
            job = next((j for j in self._jobs if j.id == job_id), None)

        if not job:
            raise ValueError(f"Job not found: {job_id}")

        await self._run_job_wrapper(job)

    async def rotate_session(self, job_id: str) -> dict:
        """Force-rotate a persistent cron session's context.

        Runs pre-rotation memorization, then clears the sdk_session_id
        so the next run starts a fresh SDK client.

        Returns a dict with rotation details.
        Raises ValueError if job not found or not persistent.
        """
        job = next((j for j in self._jobs if j.id == job_id), None)
        if not job:
            self._jobs = self._load_merged_jobs()
            job = next((j for j in self._jobs if j.id == job_id), None)

        if not job:
            raise ValueError(f"Job not found: {job_id}")
        if job.session_mode != "persistent":
            raise ValueError(
                f"Job {job_id!r} is not persistent (mode={job.session_mode!r})"
            )

        session_id = f"cron:{job_id}"
        session = await self.db.get_session(session_id)

        # Calculate current age for the response
        session_age_hours: float | None = None
        if session and session.get("connected_at"):
            try:
                ts = session["connected_at"]
                if "T" not in ts:
                    ts = ts.replace(" ", "T")
                if not ts.endswith(("Z", "+00:00")):
                    ts += "+00:00"
                ca = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                session_age_hours = round(
                    (datetime.now(timezone.utc) - ca).total_seconds() / 3600, 2,
                )
            except (ValueError, TypeError):
                pass

        # Force rotation (rotate_hours=0 ensures any positive age passes)
        rotated = await self._maybe_rotate_context(session_id, rotate_hours=0)

        logger.info(
            "Manual rotation for %s: rotated=%s age=%.1fh",
            job_id, rotated,
            session_age_hours if session_age_hours is not None else -1,
        )
        return {
            "job_id": job_id,
            "rotated": rotated,
            "session_age_hours": session_age_hours,
        }

    def list_jobs(self) -> list[dict]:
        """List all registered jobs (cron + sources) with their next run times."""
        result = []
        for job in self._jobs:
            sched_job = self.scheduler.get_job(job.id)
            next_run = sched_job.next_run_time if sched_job else None
            result.append({
                "id": job.id,
                "type": "cron",
                "source": job.metadata.get("_source", "unknown"),
                "schedule": job.schedule,
                "description": job.description,
                "enabled": job.enabled,
                "session_mode": job.session_mode,
                "lock": job.lock,
                "next_run": next_run.isoformat() if next_run else None,
            })

        # Include source runners
        for runner in self._source_runners:
            source_name = runner.source.source_name
            config_key = source_name.split(":")[0]
            sched_job = self.scheduler.get_job(runner.job_id)
            next_run = sched_job.next_run_time if sched_job else None
            source_config = getattr(self.config.sync, config_key, None)
            schedule = getattr(source_config, "schedule", "?") if source_config else "?"
            result.append({
                "id": runner.job_id,
                "type": "source",
                "schedule": schedule,
                "description": f"Source: {source_name} (ingestor)",
                "enabled": True,
                "next_run": next_run.isoformat() if next_run else None,
            })

        return result
