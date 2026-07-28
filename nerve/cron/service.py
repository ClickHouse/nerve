"""Cron scheduler — APScheduler integration.

Runs cron jobs and source runners on schedule.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone, tzinfo
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from nerve.agent.engine import AgentEngine
from nerve.config import ConfigError, NerveConfig
from nerve.cron.jobs import (
    CronJob,
    describe_reserved_job_ids,
    is_reserved_job_id,
    load_jobs,
)
from nerve.db import Database

if TYPE_CHECKING:
    from nerve.cron.gates import CronGate
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


def _drop_reserved(jobs: list[CronJob]) -> list[CronJob]:
    """Drop jobs holding a reserved id, warning once per offender.

    Nothing but a hand-edited YAML file can produce one of these, and a job that
    simply never runs is near-impossible to diagnose from the outside, so the
    warning has to name the job and the way out of it.
    """
    kept: list[CronJob] = []
    for job in jobs:
        if is_reserved_job_id(job.id):
            logger.warning(
                "Cron job '%s' (from %s) will not be scheduled: its id is "
                "reserved by the daemon (%s). Rename the job to schedule it.",
                job.id,
                job.metadata.get("_source", "user"),
                describe_reserved_job_ids(),
            )
        else:
            kept.append(job)
    return kept


class NotCrontabError(ValueError):
    """A schedule string that isn't a crontab expression at all.

    Signals "try the interval parser instead" — as opposed to
    :class:`InvalidScheduleError`, which means the string *is* a crontab and
    the interval parser must never see it.
    """


class InvalidScheduleError(ConfigError):
    """A 5-field crontab expression the scheduler rejects.

    A ``ConfigError`` because that is exactly what it is — an operator typo in
    a schedule — and because the reload route turns ConfigError into a 400
    naming the offending job instead of a bare 500.
    """


def _interval_seconds(interval: str) -> int | None:
    """Seconds for an interval string like '2h', '30m', '1h30m', '0.5h'.

    ``None`` when the string names no usable interval: either it is not a run of
    ``<number><unit>`` tokens at all (``hourly``, ``@daily``, ``???``,
    ``1h junk``), or it names zero (``0h``, or ``0.4s`` rounding down to it).
    The daemon has to keep running either way and substitutes a default (see
    :func:`_parse_interval`); config validation, which can still refuse the
    file, needs to tell both cases apart from a real '2h'.
    """
    import re
    # One token, and the same expression used to check that the string is
    # *only* tokens. An unanchored scan (the obvious `findall`) matches just the
    # digits touching each unit, so it reads "0.5h" as 5 hours and "1.5h" as 5
    # hours with the leading 1 dropped entirely — a partial match silently
    # mistaken for a whole one. fullmatch makes that impossible: leftovers mean
    # the string isn't an interval, not that the parser gets to keep the part
    # it liked. Whitespace between tokens is allowed ("1h 30m"); a 5-field
    # crontab has already been ruled out by the time we get here.
    #
    # The number is spelled out the long way instead of as `\d*\.?\d+` so that
    # each token matches exactly one way. Two ways to match a multi-digit number
    # would make the repetition backtrack through every combination of them
    # before giving up on a string that doesn't match — seconds of CPU for a
    # schedule of twenty-odd tokens, which is a config typo away.
    token = r"(\d+(?:\.\d+)?|\.\d+)([hms])"
    text = interval.strip().lower()
    if not re.fullmatch(rf"(?:{token}\s*)+", text):
        return None
    total = 0.0
    for value, unit in re.findall(token, text):
        v = float(value)
        if unit == "h":
            total += v * 3600
        elif unit == "m":
            total += v * 60
        elif unit == "s":
            total += v
    # Fractions mean what they say ("0.5h" is 1800s), rounded to the nearest
    # whole second because IntervalTrigger counts seconds and half a second of
    # drift on a cadence measured in minutes is noise. Zero is not a schedule —
    # IntervalTrigger(seconds=0) is a fire-as-fast-as-you-can loop — so a zero,
    # written or rounded down to, is reported as no interval at all.
    return round(total) or None


def _parse_interval(interval: str) -> int:
    """Parse an interval string like '2h', '30m', '1h30m', '0.5h' into seconds.

    Falls back to 2h when the string names no usable interval, because the
    daemon has to keep running: a job on a conservative cadence beats a
    scheduler that refuses to start.
    """
    return _interval_seconds(interval) or 7200  # Default 2h


# Unix crontab day-of-week numbering is 0=Sun..6=Sat (7 also means Sun).
# APScheduler's numeric day_of_week is 0=Mon..6=Sun, and CronTrigger.from_crontab
# does NOT remap, so a numeric DOW like "1" (Unix Monday) gets read as APScheduler
# 1 = Tuesday, i.e. every numeric-DOW cron fires one weekday late. APScheduler does
# accept unambiguous three-letter day names, so we translate the numbers to names.
_UNIX_DOW_TO_NAME = {
    0: "sun", 1: "mon", 2: "tue", 3: "wed",
    4: "thu", 5: "fri", 6: "sat", 7: "sun",
}


def _remap_dow_value(value: str) -> str:
    """Map a single Unix DOW number to an APScheduler day name.

    Non-numeric atoms (already a name like ``mon``, or ``*``) and numbers
    outside 0-7 pass through unchanged so APScheduler can validate them.
    """
    v = value.strip()
    if v.isdigit() and int(v) in _UNIX_DOW_TO_NAME:
        return _UNIX_DOW_TO_NAME[int(v)]
    return v


def _remap_dow_atom(atom: str) -> str:
    """Remap one comma-separated DOW atom, preserving range and step syntax.

    Handles ``*``, single values (``1``), ranges (``1-5``), and any of those
    with a step suffix (``*/2``, ``1-5/2``). Only the numeric components are
    translated; everything else is left intact.
    """
    base, sep, step = atom.partition("/")
    if base in ("*", ""):
        remapped = base
    elif "-" in base:
        lo, _, hi = base.partition("-")
        remapped = f"{_remap_dow_value(lo)}-{_remap_dow_value(hi)}"
    else:
        remapped = _remap_dow_value(base)
    return f"{remapped}{sep}{step}" if sep else remapped


def _crontab_to_trigger(
    schedule: str, timezone: tzinfo | None = None,
) -> CronTrigger:
    """Build a CronTrigger from a 5-field crontab string with Unix DOW semantics.

    Drop-in replacement for ``CronTrigger.from_crontab`` that fixes the
    day-of-week off-by-one (see ``_UNIX_DOW_TO_NAME``). Only the DOW field is
    treated differently; the other four fields and the no-explicit-timezone
    behaviour are identical to ``from_crontab``.

    The two failure modes are different exceptions, because callers have to
    tell them apart: :class:`NotCrontabError` for anything that is not a
    5-field expression, so interval strings like ``4h`` keep falling through
    to the IntervalTrigger path, and :class:`InvalidScheduleError` when it is
    a crontab whose fields the scheduler rejects. Both subclass ``ValueError``.
    """
    fields = schedule.split()
    if len(fields) != 5:
        raise NotCrontabError(f"Not a 5-field crontab expression: {schedule!r}")
    minute, hour, day, month, day_of_week = fields
    remapped_dow = ",".join(
        _remap_dow_atom(atom) for atom in day_of_week.split(",")
    )
    try:
        return CronTrigger(
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=remapped_dow,
            timezone=timezone,
        )
    except ValueError as e:
        # Five fields and one of them is bad ("99 * * * *"): a typo in a
        # crontab, not an interval. Re-raised as its own type so no caller can
        # mistake it for "not a crontab" and hand it to _parse_interval, which
        # finds no h/m/s token and returns its 2h default — turning the typo
        # into a job that quietly runs on a cadence nobody asked for.
        raise InvalidScheduleError(
            f"Invalid crontab expression {schedule!r}: {e}",
        ) from e


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
        self.timezone = ZoneInfo(config.timezone)
        self.scheduler = AsyncIOScheduler(timezone=self.timezone)
        self._jobs: list[CronJob] = []
        self._source_runners: list[SourceRunner] = []
        self._job_locks: dict[str, asyncio.Lock] = {}

    async def start(self) -> None:
        """Load jobs and start the scheduler."""
        # Register drop-in custom gate plugins BEFORE jobs are parsed, so their
        # `type` keys are present in GATE_REGISTRY when each job's run_if specs
        # are built (CronJob builds its gates at construction time).
        from nerve.cron.gate_plugins import load_gate_plugins

        load_gate_plugins(self.config.cron.gate_plugins_dir)

        # Load job definitions from both files
        self._jobs = self._load_merged_jobs()

        # Register cron jobs with persistent timer alignment
        for job in self._jobs:
            if not job.enabled:
                continue

            try:
                trigger = await self._make_trigger(job)
            except InvalidScheduleError as e:
                # Deliberately the opposite of reload(), which refuses the whole
                # change set: there the running schedule survives untouched and
                # a synchronous caller gets a 400 naming the job. Nothing can be
                # handed back that way here — gateway/server.py wraps this call
                # in `except Exception: logger.warning(...)`, so raising over one
                # typo would cost every other cron, every source runner and the
                # reload endpoint itself (503 with no service) until someone
                # noticed one warning line and restarted the daemon. Skip the
                # offender loudly instead; it stays in _jobs, so /api/cron/jobs
                # keeps listing it with a null next run rather than dropping it
                # out of sight.
                # _make_trigger's message already names the job; don't say it
                # twice.
                logger.error(
                    "%s. Not scheduled — fix the schedule and reload.", e,
                )
                continue

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
                    trigger = _crontab_to_trigger(
                        schedule_str, timezone=self.timezone,
                    )
                except NotCrontabError:
                    seconds = _parse_interval(schedule_str)
                    trigger = IntervalTrigger(
                        seconds=seconds, timezone=self.timezone,
                    )
                except InvalidScheduleError as e:
                    # Same call as the cron loop above, same answer — skip this
                    # one, keep the rest. Letting it reach the enclosing
                    # `except Exception` would abandon every source after it in
                    # the list under a single "failed to register source
                    # runners" warning that names the wrong problem.
                    logger.error(
                        "Source '%s' will not be scheduled: %s", source_name, e,
                    )
                    continue

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
            CronTrigger(hour=3, minute=0, timezone=self.timezone),
            id="cleanup",
            name="Cleanup expired data",
            replace_existing=True,
        )

        # Fire due session wakeups (ScheduleWakeup harness). The CLI's own
        # scheduler is disabled; Nerve owns wakeup timing here.
        self.scheduler.add_job(
            self._sweep_wakeups,
            IntervalTrigger(
                seconds=_WAKEUP_SWEEP_SECONDS, timezone=self.timezone,
            ),
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

    async def reload(self) -> dict:
        """Re-read cron config and apply changes to the running scheduler.

        Picks up added / removed / rescheduled / enabled-toggled jobs without a
        daemon restart, mirroring the MCP-config reload pattern:

          * jobs removed or newly disabled → unscheduled
          * every other enabled job        → rebuilt from the new definition

        Every enabled job is rebuilt, not only the ones whose YAML changed.
        Rebuilding a trigger keeps the next fire time as it was for a crontab
        job, and for an interval job anchored to a successful run; an interval
        job that has never succeeded has no anchor and restarts its countdown,
        which is the only thing this costs (documented in docs/cron.md).

        Source runners and the fixed cleanup/wakeup jobs are left untouched.
        Custom gate plugins are re-read from scratch (replace=True): a new file
        registers, an edited file's code takes effect, and a deleted file's gate
        unregisters (see nerve/cron/gate_plugins.py). Rebuilding every job is
        what puts those gates in front of the jobs that run: the scheduler
        executes the CronJob object it holds, and gates are built into that
        object when it is constructed.

        All-or-nothing: the complete change set — every trigger included — is
        built before the scheduler is touched, so a reload the daemon cannot
        carry out raises with the running schedule exactly as it was. That
        covers the gate registry too; see :meth:`_reload_from_disk`.

        Returns a summary dict: ``{"added", "removed", "updated", "enabled"}``,
        where "updated" is every enabled job that already had a trigger. It says
        what the reload rescheduled, not which jobs the file changed.
        """
        from nerve.cron.gates import GATE_REGISTRY

        # The one piece of reload state that isn't local: GATE_REGISTRY is
        # process-global and has to be replaced *before* jobs are rebuilt,
        # because a CronJob builds its gates from it at construction time. That
        # puts it ahead of every check that can still refuse the reload, so it
        # needs undoing by hand — leaving the scheduler untouched is not enough
        # when a deleted plugin's gate has already been unregistered. The next
        # CronJob built from disk (run_job, rotate_session) would then fail to
        # build for want of a gate type, off a reload that returned 400: the
        # instance would lose jobs it had never agreed to change.
        gates_before = dict(GATE_REGISTRY)
        try:
            return await self._reload_from_disk(gates_before)
        except BaseException:
            # BaseException, not Exception: a cancelled reload has to put the
            # registry back too.
            GATE_REGISTRY.clear()
            GATE_REGISTRY.update(gates_before)
            raise

    async def _reload_from_disk(
        self, gates_before: dict[str, type["CronGate"]],
    ) -> dict:
        """The body of :meth:`reload`. Only call it through there.

        Takes the pre-reload registry snapshot so it can report which gates
        vanished once the change is committed, and so its caller can restore
        that snapshot if this raises.
        """
        from nerve.cron.gate_plugins import load_gate_plugins, warn_vanished_gates

        # Re-read drop-in gate plugins before jobs are rebuilt (their gates are
        # constructed at CronJob build time). replace=True so an edited/deleted
        # plugin's code is picked up, not just newly-added files. The loader's own
        # "gate vanished" warning is held back until the reload commits, since a
        # refused one leaves those jobs holding the gate after all.
        load_gate_plugins(
            self.config.cron.gate_plugins_dir, replace=True, warn_vanished=False,
        )

        # -- Plan: everything that can fail, before anything is applied --------
        #
        # Both the strict load below and the trigger construction further down
        # can raise: the load on a malformed file, _make_trigger on a schedule
        # the scheduler rejects (InvalidScheduleError → 400 — unlike at startup,
        # where the offending job is skipped and the daemon comes up anyway,
        # because here the running schedule survives and someone is waiting on
        # an answer) or a failing DB read (interval jobs anchor their timer to
        # the last successful run). Neither may run after a scheduler
        # mutation. Unscheduling first and only then discovering a job whose
        # trigger won't build would leave the daemon half-reloaded — part of the
        # crons unscheduled, part still on their old triggers, and nothing to
        # tell an operator which is which. Refusing the whole reload is the only
        # outcome anyone can reason about, and the only one the API's 400 and
        # the docs actually promise.
        #
        # Load strictly so a malformed jobs.yaml/system.yaml raises (and the
        # route returns 400) instead of silently reading as "all jobs removed"
        # and unscheduling everything.
        # The loader already drops (and warns about) reserved ids, so nothing
        # here can replace or remove the internal cleanup/wakeup/source
        # triggers. _jobs is re-filtered in case it was seeded directly.
        new_jobs = self._load_merged_jobs(strict=True)
        new_by_id = {j.id: j for j in new_jobs}
        old_by_id = {j.id: j for j in self._jobs if not is_reserved_job_id(j.id)}

        added: list[str] = []
        removed: list[str] = []
        updated: list[str] = []

        unschedule: list[str] = []
        reschedule: list[tuple[CronJob, CronTrigger | IntervalTrigger]] = []

        # Jobs that disappeared or became disabled → unschedule.
        for jid, old in old_by_id.items():
            gone = jid not in new_by_id
            disabled = (not gone) and (not new_by_id[jid].enabled)
            if (gone or disabled) and self.scheduler.get_job(jid) is not None:
                unschedule.append(jid)
                if gone:
                    removed.append(jid)

        # Every enabled job is rebuilt from the file, whether or not its YAML
        # changed. The scheduler lookup here still sees the pre-reload schedule,
        # which is what it is meant to report against; the two loops never visit
        # the same id anyway (this one skips disabled jobs and only walks ids
        # present in the new config).
        for jid, job in new_by_id.items():
            if not job.enabled:
                continue
            existing = self.scheduler.get_job(jid) is not None
            reschedule.append((job, await self._make_trigger(job)))
            # Report against the scheduler, not against _jobs: a job that was in
            # _jobs but had no trigger is newly scheduled, not re-scheduled.
            if existing:
                updated.append(jid)
            else:
                added.append(jid)

        # -- Apply: scheduler bookkeeping only --------------------------------
        # No parsing, no DB, no awaits past this point, so the whole diff lands
        # in one event-loop step and no job can fire against a half-applied set.
        for jid in unschedule:
            self.scheduler.remove_job(jid)
        for job, trigger in reschedule:
            self.scheduler.add_job(
                self._run_job_wrapper,
                trigger,
                args=[job],
                id=job.id,
                name=job.description or job.id,
                replace_existing=True,
            )

        self._jobs = new_jobs
        # Committed, so the gates that disappeared really are gone from the jobs
        # that are now live — say so (the loader was told to keep quiet above).
        warn_vanished_gates(gates_before)
        enabled = sum(1 for j in new_by_id.values() if j.enabled)
        logger.info(
            "Cron reloaded: +%d added, ~%d updated, -%d removed (%d enabled)",
            len(added), len(updated), len(removed), enabled,
        )
        return {
            "added": added,
            "removed": removed,
            "updated": updated,
            "enabled": enabled,
        }

    async def stop(self) -> None:
        """Stop the scheduler."""
        self.scheduler.shutdown(wait=False)
        logger.info("Cron service stopped")

    # -- Persistent timers -------------------------------------------------

    async def _make_trigger(self, job: CronJob) -> CronTrigger | IntervalTrigger:
        """Create an APScheduler trigger for a job.

        For interval schedules, anchors to the last successful run so
        the cadence survives restarts (persistent timer).

        Raises :class:`InvalidScheduleError` for a crontab the scheduler
        rejects; each caller decides what refusing means (see start() and
        reload()).
        """
        try:
            return _crontab_to_trigger(job.schedule, timezone=self.timezone)
        except NotCrontabError:
            pass  # not a crontab → an interval string like "4h"
        except InvalidScheduleError as e:
            # Name the job: the schedule alone doesn't say which YAML entry to
            # go and fix, and this message is what an operator sees, either in
            # the startup log or in the reload's 400.
            raise InvalidScheduleError(f"Cron job '{job.id}': {e}") from e

        seconds = _parse_interval(job.schedule)
        last_run = await self.db.get_last_successful_cron_run(job.id)
        if last_run and last_run.get("finished_at"):
            start_date = _parse_timestamp(last_run["finished_at"])
            logger.debug(
                "Aligning interval for %s: start_date=%s", job.id, start_date,
            )
            return IntervalTrigger(
                seconds=seconds,
                start_date=start_date,
                timezone=self.timezone,
            )
        return IntervalTrigger(seconds=seconds, timezone=self.timezone)

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
            if self._is_overdue(job, last_time, now, self.timezone):
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
    def _is_overdue(
        job: CronJob,
        last_run: datetime,
        now: datetime,
        trigger_timezone: tzinfo | None = None,
    ) -> bool:
        """Check if a job should have fired between *last_run* and *now*."""
        try:
            trigger = _crontab_to_trigger(
                job.schedule, timezone=trigger_timezone or timezone.utc,
            )
            next_fire = trigger.get_next_fire_time(last_run, last_run)
            return next_fire is not None and next_fire < now
        except NotCrontabError:
            seconds = _parse_interval(job.schedule)
            return (now - last_run).total_seconds() >= seconds
        except InvalidScheduleError:
            # start() refused to schedule this job, so it has no fire times it
            # could have missed. Catching it up would run, once per restart,
            # precisely the job the daemon declined to schedule. start() already
            # logged the typo, so this stays quiet rather than reporting it
            # twice per boot.
            return False

    # -- End persistent timers ---------------------------------------------

    # -- Persistent session generations --------------------------------------
    #
    # A persistent cron runs in a "generation" chat session. Instead of
    # resetting the SDK context in place (which piles every context epoch
    # into one endless chat), rotation RETIRES the current chat — keeping it
    # and its full history as a normal browsable session — and mints a fresh
    # chat for subsequent runs. The current generation for a job is tracked
    # in channel_sessions under the key ``cron:{job_id}``.

    def _channel_key(self, job_id: str) -> str:
        return f"cron:{job_id}"

    async def _current_persistent_session_id(self, job_id: str) -> str | None:
        """Resolve the current generation session for a persistent job.

        Returns None when there is no usable current session (never ran,
        chat deleted, or mapped session archived) — the caller mints a new
        generation. Pre-generation installs used the stable id
        ``cron:{job_id}`` directly; such a legacy session is adopted as the
        current generation once, unless it was already rotated out (its
        metadata carries ``rotated_at``).
        """
        key = self._channel_key(job_id)
        row = await self.db.get_channel_session(key)
        if row and row.get("session_id"):
            session = await self.db.get_session(row["session_id"])
            if session and session.get("status") != "archived":
                return row["session_id"]
            # Mapped chat was deleted or archived → start a new generation.
            return None

        # Legacy fallback: adopt the stable-id session from installs that
        # predate generation chats, so their SDK context carries over.
        legacy = await self.db.get_session(key)
        if legacy and legacy.get("status") != "archived":
            try:
                meta = json.loads(legacy.get("metadata") or "{}")
            except (TypeError, ValueError):
                meta = {}
            if not meta.get("rotated_at"):
                await self.db.set_channel_session(key, key)
                logger.info(
                    "Adopted legacy persistent cron session %s as current "
                    "generation", key,
                )
                return key
        return None

    async def _start_new_generation(self, job_id: str) -> str:
        """Create a fresh chat session for a persistent job and map it."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        session_id = f"cron:{job_id}:{ts}"
        await self.engine.sessions.get_or_create(
            session_id, title=f"Cron: {job_id}", source="cron",
        )
        await self.db.set_channel_session(self._channel_key(job_id), session_id)
        logger.info(
            "Started new chat for persistent cron %s: %s", job_id, session_id,
        )
        return session_id

    async def _retire_session(
        self, job_id: str, session_id: str, reason: str,
    ) -> None:
        """Retire a persistent cron generation, preserving its chat history.

        The session row, its messages, usage, and events are left untouched —
        the chat stays browsable (and even resumable) in the UI and ages out
        via the normal session-archival cleanup. This only:

        - schedules memU indexing of the retiring context (safety net),
        - cancels the session's pending wakeups (a retired thread must not
          resurrect itself alongside the new generation),
        - stamps ``rotated_at`` in the session metadata (prevents legacy
          re-adoption) and retitles the chat with its end date.
        """
        # Scheduled, not awaited: memorization queues on a global lock and
        # awaiting it would delay the run start by the whole queue wait.
        # The lower bound is frozen at scheduling time.
        try:
            await self.engine.schedule_memorize(session_id)
        except Exception as e:
            logger.warning(
                "Pre-rotation memorize failed for %s: %s", session_id, e,
            )

        try:
            cancelled = await self.db.cancel_wakeups_for_session(session_id)
            if cancelled:
                logger.info(
                    "Cancelled %d pending wakeup(s) for retired cron "
                    "session %s", cancelled, session_id,
                )
        except Exception as e:
            logger.warning(
                "Failed to cancel wakeups for %s: %s", session_id, e,
            )

        try:
            session = await self.db.get_session(session_id) or {}
            try:
                meta = json.loads(session.get("metadata") or "{}")
            except (TypeError, ValueError):
                meta = {}
            meta["rotated_at"] = datetime.now(timezone.utc).isoformat()
            await self.db.update_session_metadata(session_id, meta)

            date_str = datetime.now(self.timezone).strftime("%Y-%m-%d")
            title = session.get("title") or f"Cron: {job_id}"
            await self.db.update_session_title(
                session_id, f"{title} (until {date_str})",
            )
        except Exception as e:
            logger.warning(
                "Failed to stamp retired session %s: %s", session_id, e,
            )

        logger.info(
            "Retired persistent cron session %s (%s) — history preserved",
            session_id, reason,
        )

    def _rotation_reason(
        self, session: dict, rotate_hours: int, rotate_at: str,
    ) -> str | None:
        """Decide whether a generation is due for rotation.

        Returns a human-readable reason string, or None when the session
        should keep running. ``connected_at`` marks the start of the current
        SDK context (it is preserved across resumes), so it doubles as the
        generation's epoch.

        If rotate_at is set (e.g. "04:00"), rotation happens once per day
        at that local time instead of using the hours-based approach.
        """
        now = datetime.now(timezone.utc)

        connected_at_str = session.get("connected_at")
        if not connected_at_str:
            return None
        try:
            ts = connected_at_str
            if "T" not in ts:
                ts = ts.replace(" ", "T")
            if not ts.endswith(("Z", "+00:00")):
                ts += "+00:00"
            connected_at = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            logger.warning(
                "Invalid connected_at for cron session: %s", connected_at_str,
            )
            return None

        if rotate_at:
            # Time-of-day rotation: rotate if session started before today's
            # rotate_at and current time is past it.
            try:
                hour, minute = (int(x) for x in rotate_at.split(":"))
            except (ValueError, TypeError):
                logger.warning("Invalid context_rotate_at: %s", rotate_at)
                return None

            today_rotate = now.astimezone(self.timezone).replace(
                hour=hour, minute=minute, second=0, microsecond=0,
            )
            today_rotate_utc = today_rotate.astimezone(timezone.utc)

            if now >= today_rotate_utc and connected_at < today_rotate_utc:
                return f"rotate_at={rotate_at}"
        elif rotate_hours > 0:
            age_hours = (now - connected_at).total_seconds() / 3600
            if age_hours >= rotate_hours:
                return f"age {age_hours:.1f}h >= {rotate_hours}h"
        return None

    async def _resolve_persistent_session(self, job: CronJob) -> tuple[str, bool]:
        """Pick the chat session for a persistent run, rotating if due.

        Returns ``(session_id, rotated)``. When rotation is due, the current
        generation is retired (chat + history preserved as its own session)
        and a brand-new chat is minted for this and subsequent runs.
        """
        current = await self._current_persistent_session_id(job.id)
        rotated = False

        if current and (job.context_rotate_at or job.context_rotate_hours > 0):
            session = await self.db.get_session(current)
            if session:
                reason = self._rotation_reason(
                    session, job.context_rotate_hours, job.context_rotate_at,
                )
                if reason:
                    await self._retire_session(job.id, current, reason)
                    current = None
                    rotated = True

        if current is None:
            current = await self._start_new_generation(job.id)
        return current, rotated

    # -- End persistent session generations ----------------------------------

    def _load_merged_jobs(self, strict: bool = False) -> list[CronJob]:
        """Load and merge jobs from system.yaml and jobs.yaml.

        System jobs come from system.yaml (managed by `nerve init`).
        User jobs come from jobs.yaml (user-defined, never touched by Nerve).
        If a user job has the same ID as a system job, the user version wins.

        Jobs holding an id the daemon reserves for itself are dropped here, so
        no caller can schedule, catch up or hand-run one.

        With ``strict=True`` a malformed file raises ConfigError instead of
        being silently treated as empty (used by reload()).
        """
        system_file = self.config.cron.system_file
        jobs_file = self.config.cron.jobs_file

        system_jobs = load_jobs(system_file, strict=strict)
        user_jobs = load_jobs(jobs_file, strict=strict)

        if not system_jobs and user_jobs:
            # Backward compat: old install with everything in jobs.yaml
            logger.info(
                "No system.yaml found — loading all crons from jobs.yaml "
                "(run 'nerve init' to split)"
            )
            # Tag all as user-sourced (no system file yet)
            for j in user_jobs:
                j.metadata["_source"] = "user"
            return _drop_reserved(user_jobs)

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

        return _drop_reserved(list(jobs_by_id.values()))

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

        if job.workflow is not None:
            # Workflow-run job: launch a budget-capped workflow run and
            # return — the run executes and notifies on its own, so none
            # of the cron session machinery below applies.
            await self._start_workflow_run(job, log_id)
            return

        session_id: str | None = None

        try:
            model = job.model or self.config.agent.cron_model
            effort = job.effort or None  # per-job effort override; None = source default (cron_effort)
            rotated = False
            base_prompt = job.resolve_prompt()

            # Determine the session id up front and link the run log to it
            # immediately, so the UI can open the chat of a *running* cron
            # instead of waiting for the run to finish.
            run_id: str | None = None
            if job.session_mode == "persistent":
                # Resolve the current generation chat, rotating to a fresh
                # one first when due (the old chat is preserved).
                session_id, rotated = await self._resolve_persistent_session(job)
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
                    session_id=session_id,
                    cache_ttl=job.cache_ttl,
                    effort=effort,
                )
            else:
                response = await self.engine.run_cron(
                    job_id=job.id,
                    prompt=base_prompt,
                    model=model,
                    run_id=run_id,
                    cache_ttl=job.cache_ttl,
                    effort=effort,
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

    async def _start_workflow_run(self, job: CronJob, log_id: int) -> None:
        """Start the workflow run a job declares instead of a prompt.

        Fire-and-forget: the cron log records only the launch. The run
        owns its ``workflow:<run-id>`` session and sends its own budget /
        completion / failure notifications — no cron session is created
        and the job does not wait for the run to finish.
        """
        from nerve.workflows import get_workflow_run_service

        service = get_workflow_run_service()
        if service is None:
            logger.warning(
                "Cron job %s declares a workflow run but workflow runs "
                "are disabled", job.id,
            )
            await self.db.log_cron_finish(
                log_id, "error", error="workflow runs disabled",
            )
            return

        if job.lock:
            # ``lock`` promises no overlapping work for this job. The
            # per-job asyncio.Lock only covers the (instant) launch, so
            # extend the guarantee to run duration: skip while a previous
            # run from this job is still pending/running. Without this, a
            # schedule shorter than the run duration stacks full-budget
            # runs.
            active = await service.db.get_active_workflow_runs()
            mine = [
                r for r in active
                if r.get("created_by") == f"cron:{job.id}"
            ]
            if mine:
                await self.db.log_cron_finish(
                    log_id, "success",
                    output=(
                        f"skipped: workflow run {mine[0]['id']} from this "
                        "job is still active (lock)"
                    ),
                )
                return

        w = job.workflow or {}
        try:
            run = await service.start_run(
                engine_kind=w["engine"],
                spec={
                    "prompt": w["prompt"],
                    "model": w.get("model") or "",
                    "effort": w.get("effort") or "",
                    "cwd": w.get("cwd") or "",
                },
                budget_usd=w["budget_usd"],
                title=w.get("title") or job.id,
                created_by=f"cron:{job.id}",
            )
            session_id = run.get("session_id") or ("workflow:" + run["id"])
            await self.db.set_cron_log_session(log_id, session_id)
            budget = float(run.get("budget_usd") or w["budget_usd"])
            await self.db.log_cron_finish(
                log_id, "success",
                output=(
                    f"workflow run {run['id']} started (budget ${budget:.2f})"
                ),
            )
            logger.info(
                "Cron job %s started workflow run %s", job.id, run["id"],
            )
        except Exception as e:
            logger.error(
                "Cron job %s failed to start workflow run: %s",
                job.id, e, exc_info=True,
            )
            await self.db.log_cron_finish(log_id, "error", error=str(e))

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
        """Force-rotate a persistent cron to a fresh chat session.

        Retires the current generation chat (its history is preserved as a
        normal session) and starts a new empty chat that the next run — and
        the CronPage chat link — picks up immediately.

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

        session_id = await self._current_persistent_session_id(job_id)
        session = await self.db.get_session(session_id) if session_id else None

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

        rotated = False
        new_session_id: str | None = None
        if session_id and session:
            await self._retire_session(job_id, session_id, "manual")
            new_session_id = await self._start_new_generation(job_id)
            rotated = True

        logger.info(
            "Manual rotation for %s: rotated=%s age=%.1fh new=%s",
            job_id, rotated,
            session_age_hours if session_age_hours is not None else -1,
            new_session_id,
        )
        return {
            "job_id": job_id,
            "rotated": rotated,
            "session_age_hours": session_age_hours,
            "old_session_id": session_id if rotated else None,
            "new_session_id": new_session_id,
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
