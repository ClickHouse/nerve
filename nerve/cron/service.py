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

# How often to scan for due session wakeups (ScheduleWakeup harness). The
# tool clamps delays to >= 60s, so a 20s sweep keeps fire latency well under
# the granularity the model can request.
_WAKEUP_SWEEP_SECONDS = 20

# ScheduleWakeup autonomous-loop sentinels (Claude Code /loop). Nerve has no
# /loop command, so resolve them to a plain continuation instruction.
_WAKEUP_SENTINELS = {"<<autonomous-loop>>", "<<autonomous-loop-dynamic>>"}
_WAKEUP_SENTINEL_PROMPT = (
    "[Scheduled wakeup] Continue the task you were pacing. If there is "
    "nothing left to do, stop and don't reschedule."
)


def _resolve_wakeup_prompt(prompt: str) -> str:
    """Map an autonomous-loop sentinel to a usable prompt; pass others through."""
    return _WAKEUP_SENTINEL_PROMPT if prompt.strip() in _WAKEUP_SENTINELS else prompt


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

        # Fire due session wakeups (ScheduleWakeup harness). The CLI's own
        # scheduler is disabled; Nerve owns wakeup timing here.
        self.scheduler.add_job(
            self._sweep_wakeups,
            IntervalTrigger(seconds=_WAKEUP_SWEEP_SECONDS),
            id="wakeup_sweep",
            name="Fire due session wakeups",
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
        *,
        force: bool = False,
    ) -> bool:
        """Check if a persistent cron session's context should be rotated.

        Rotation clears the sdk_session_id so the next run starts a fresh
        SDK client.  Old messages remain in the DB for memU search.

        If rotate_at is set (e.g. "04:00"), rotation happens once per day
        at that local time instead of using the hours-based approach.

        Returns True if rotation was performed.
        """
        session = await self.db.get_session(session_id)
        if not session:
            return False

        now = datetime.now(timezone.utc)
        should_rotate = force
        reason = "manual" if force else ""

        connected_at = None
        connected_at_str = session.get("connected_at")
        if connected_at_str:
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
                if not force:
                    return False
        elif not force:
            return False

        if not should_rotate and rotate_at:
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
        elif not should_rotate and rotate_hours > 0:
            age_hours = (now - connected_at).total_seconds() / 3600
            if age_hours >= rotate_hours:
                should_rotate = True
                reason = f"age {age_hours:.1f}h >= {rotate_hours}h"

        if not should_rotate:
            return False

        # Schedule memorization of the pre-rotation context (safety net).
        # Scheduled, not awaited: memorization queues on a global lock and
        # awaiting it would delay the run start by the whole queue wait.
        # The lower bound is frozen at scheduling time, so clearing
        # connected_at below cannot shrink the covered window.
        try:
            await self.engine.schedule_memorize(session_id)
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
        # Pre-check: skip if any configured run gate is unsatisfied.
        if job.gates:
            from nerve.cron.gates import GateContext, evaluate_gates

            decision = await evaluate_gates(
                job.gates, GateContext(job_id=job.id, db=self.db),
            )
            if not decision.should_run:
                logger.info("Skipping cron job %s: %s", job.id, decision.reason)
                return

        log_id = await self.db.log_cron_start(job.id)
        logger.info("Running cron job: %s (mode=%s)", job.id, job.session_mode)
        session_id: str | None = None

        try:
            model = job.model or self.config.agent.cron_model
            rotated = False
            base_prompt = job.resolve_prompt()

            # Determine the session id up front and link the run log to it
            # immediately, so the UI can open the chat of a *running* cron
            # instead of waiting for the run to finish.
            run_id: str | None = None
            if job.session_mode == "persistent":
                session_id = f"cron:{job.id}"
            else:
                # Isolated mode: per-run session. The run_id is generated
                # here (the engine would otherwise generate an identical
                # timestamp-based one) so the session id is known for the
                # run log.
                run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
                session_id = f"cron:{job.id}:{run_id}"
            try:
                await self.db.set_cron_log_session(log_id, session_id)
            except Exception as e:
                logger.warning(
                    "Failed to link cron log %s to session %s: %s",
                    log_id, session_id, e,
                )

            if job.session_mode == "persistent":
                # Persistent mode: reuse SDK context across runs
                if job.context_rotate_at or job.context_rotate_hours > 0:
                    rotated = await self._maybe_rotate_context(
                        session_id, job.context_rotate_hours,
                        rotate_at=job.context_rotate_at,
                    )

                # Determine prompt: full on first run, short reminder on subsequent
                prompt = base_prompt
                if job.reminder_mode:
                    session = await self.db.get_session(session_id)
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
                        prompt = base_prompt.rstrip() + (
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
                response = await self.engine.run_cron(
                    job_id=job.id,
                    prompt=base_prompt,
                    model=model,
                    run_id=run_id,
                )

            # Keep the tail of the response — for multi-message runs the
            # final summary lives at the end, not the beginning.
            output = response if len(response) <= 2000 else "…" + response[-2000:]
            if rotated:
                output = "[context rotated] " + output
            await self.db.log_cron_finish(
                log_id, "success", output=output, session_id=session_id,
            )
            logger.info("Cron job %s completed (%d chars)", job.id, len(response))

        except Exception as e:
            logger.error("Cron job %s failed: %s", job.id, e, exc_info=True)
            await self.db.log_cron_finish(
                log_id, "error", error=str(e), session_id=session_id,
            )

    async def _run_source_wrapper(self, runner: SourceRunner) -> None:
        """Wrapper to run a source ingestion with cron and source logging."""
        log_id = await self.db.log_cron_start(runner.job_id)
        logger.info("Running source: %s", runner.source.source_name)

        try:
            result = await runner.run()
            summary = f"{result.records_ingested} ingested"
            if result.records_dropped:
                summary += f", {result.records_dropped} dropped by guardrail"
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
        """Clean up expired source messages, consumer cursors, and old cron logs."""
        try:
            msg_count = await self.db.cleanup_expired_messages()
            cursor_count = await self.db.cleanup_expired_consumer_cursors()
            cron_log_count = await self.db.cleanup_old_cron_logs(days=14)
            if msg_count or cursor_count or cron_log_count:
                logger.info(
                    "Cleanup: %d expired messages, %d expired consumer cursors, "
                    "%d cron logs older than 14 days",
                    msg_count, cursor_count, cron_log_count,
                )
        except Exception as e:
            logger.error("Cleanup failed: %s", e, exc_info=True)

    async def _sweep_wakeups(self) -> None:
        """Fire due session wakeups recorded by the ScheduleWakeup hook.

        Each due wakeup is atomically claimed (pending -> fired) so
        overlapping sweeps can't double-fire it, then re-injected into its
        session via ``engine.run(..., source="wakeup")``. The run is
        dispatched (not awaited) so one long turn can't stall the sweep; the
        per-session lock inside ``run`` serialises it behind any live turn.
        """
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            due = await self.db.get_due_wakeups(now_iso)
        except Exception as e:
            logger.error("Wakeup sweep query failed: %s", e, exc_info=True)
            return

        for wakeup in due:
            session_id = wakeup["session_id"]
            # Skip sessions mid-turn; a still-running turn may itself be
            # rescheduling. Leave the wakeup pending and retry next sweep.
            if self.engine.sessions.is_running(session_id):
                continue
            try:
                claimed = await self.db.claim_wakeup(wakeup["id"])
            except Exception as e:
                logger.error(
                    "Failed to claim wakeup %s: %s", wakeup["id"], e,
                )
                continue
            if not claimed:
                continue
            self._dispatch_wakeup(session_id, wakeup)

    def _dispatch_wakeup(self, session_id: str, wakeup: dict) -> None:
        """Spawn the engine run for a claimed wakeup with error logging."""
        prompt = _resolve_wakeup_prompt(wakeup["prompt"])
        logger.info(
            "Firing wakeup %s for session %s", wakeup["id"], session_id[:8],
        )
        task = asyncio.create_task(
            self.engine.run(
                session_id=session_id,
                user_message=prompt,
                source="wakeup",
                internal=True,
            )
        )

        def _done(t: asyncio.Task) -> None:
            exc = t.exception() if not t.cancelled() else None
            if exc is not None:
                logger.error(
                    "Wakeup %s run failed for session %s: %s",
                    wakeup["id"], session_id, exc,
                )

        task.add_done_callback(_done)

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

        rotated = await self._maybe_rotate_context(
            session_id, rotate_hours=0, force=True,
        )

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

    async def list_jobs(self) -> list[dict]:
        """List all registered jobs (cron + sources) with their next run times."""
        result = []
        for job in self._jobs:
            sched_job = self.scheduler.get_job(job.id)
            next_run = sched_job.next_run_time if sched_job else None
            try:
                last_session_id = await self.db.get_latest_cron_session_id(job.id)
            except Exception:
                last_session_id = None
            result.append({
                "id": job.id,
                "type": "cron",
                "source": job.metadata.get("_source", "unknown"),
                "schedule": job.schedule,
                "description": job.description,
                "prompt_file": job.prompt_file,
                "enabled": job.enabled,
                "session_mode": job.session_mode,
                "lock": job.lock,
                "gates": [gate.describe() for gate in job.gates],
                "next_run": next_run.isoformat() if next_run else None,
                "last_session_id": last_session_id,
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
                "last_session_id": None,
            })

        return result
