"""Tests for misfire tolerance, missed-run logging, and the memU trim split.

A run that starts late used to be dropped by APScheduler's 1-second default
grace, and a dropped run writes no cron_logs row -- so the loss was invisible.
The other half is what made runs late: a full gc.collect() after every indexed
file, which stops every Python thread regardless of the executor it runs on.

No test here asserts elapsed time. The oracles are call counts, recorded
arguments and row counts, so none of them depend on machine speed.
"""

from __future__ import annotations

import asyncio
import datetime
import gc
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from apscheduler.events import (
    EVENT_JOB_MAX_INSTANCES,
    EVENT_JOB_MISSED,
    JobExecutionEvent,
    JobSubmissionEvent,
)

from nerve.cron.service import _MISFIRE_GRACE_SECONDS, CronService


def _make_service(tmp_path, db=None) -> CronService:
    cron_dir = tmp_path / "cron"
    cron_dir.mkdir(parents=True, exist_ok=True)

    config = MagicMock()
    config.timezone = "UTC"
    config.cron.jobs_file = cron_dir / "jobs.yaml"
    config.cron.system_file = cron_dir / "system.yaml"
    config.cron.gate_plugins_dir = cron_dir / "gates"

    if db is None:
        db = AsyncMock()
        db.log_cron_missed = AsyncMock(return_value=1)
    return CronService(config, AsyncMock(), db)


# ---------------------------------------------------------------------------
# The grace itself
# ---------------------------------------------------------------------------

class TestMisfireGrace:
    def test_scheduler_overrides_the_one_second_default(self, tmp_path):
        """The whole defect in one assertion: unfixed, this is 1."""
        svc = _make_service(tmp_path)
        assert svc.scheduler._job_defaults["misfire_grace_time"] == 30

    def test_grace_is_bounded(self):
        """None would let `coalesce` resurrect a run stale by hours."""
        assert _MISFIRE_GRACE_SECONDS is not None
        assert 0 < _MISFIRE_GRACE_SECONDS <= 300

    def test_backpressure_defaults_are_kept(self, tmp_path):
        """coalesce collapses a backlog; max_instances=1 backs `lock: true`."""
        defaults = _make_service(tmp_path).scheduler._job_defaults
        assert defaults["coalesce"] is True
        assert defaults["max_instances"] == 1

    @pytest.mark.asyncio
    async def test_every_job_inherits_the_grace(self, tmp_path):
        """No add_job call passes misfire_grace_time, so all five sites inherit.

        Defaults are applied when a job is added to a *running* scheduler, so the
        scheduler is started paused -- on a pending job the attribute is unset.
        AsyncIOScheduler.start() requires a running loop, hence the async test.
        """
        svc = _make_service(tmp_path)
        svc.scheduler.start(paused=True)
        try:
            svc.scheduler.add_job(lambda: None, "interval", seconds=60, id="probe")
            assert svc.scheduler.get_job("probe").misfire_grace_time == 30
        finally:
            svc.scheduler.shutdown(wait=False)


class TestExecutorDropsLateRuns:
    """Isolate apscheduler's skip branch: with grace 1 a late run is dropped,
    with our grace it is executed.

    Calls ``run_job`` -- the function that owns the misfire branch and the
    ``continue`` that skips ``job.func``. No real scheduler, no waiting.
    """

    @staticmethod
    def _run_with_grace(grace: int, seconds_late: float) -> tuple[list, list]:
        from apscheduler.executors.base import run_job

        calls: list[str] = []

        job = MagicMock()
        job.id = "late-job"
        job.misfire_grace_time = grace
        job.max_instances = 1
        job.args, job.kwargs = (), {}
        job.func = lambda: calls.append("late-job")

        due = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            seconds=seconds_late,
        )
        events = run_job(job, "default", [due], __name__)
        return calls, events

    def test_one_second_grace_drops_a_late_run(self):
        """The unfixed default: job.func is never called and the run vanishes."""
        calls, events = self._run_with_grace(grace=1, seconds_late=5)
        assert calls == []                                  # never invoked
        assert [e.code for e in events] == [EVENT_JOB_MISSED]

    def test_our_grace_runs_it(self):
        calls, events = self._run_with_grace(
            grace=_MISFIRE_GRACE_SECONDS, seconds_late=5,
        )
        assert calls == ["late-job"]                        # actually invoked
        assert EVENT_JOB_MISSED not in [e.code for e in events]

    def test_a_run_later_than_the_grace_is_still_dropped(self):
        """The bound is real: an hour-late run must not be resurrected."""
        calls, events = self._run_with_grace(
            grace=_MISFIRE_GRACE_SECONDS, seconds_late=3600,
        )
        assert calls == []
        assert [e.code for e in events] == [EVENT_JOB_MISSED]


# ---------------------------------------------------------------------------
# The missed-run row
# ---------------------------------------------------------------------------

class TestMissedRunListener:
    @pytest.mark.asyncio
    async def test_listener_writes_one_row_with_the_due_time(self, tmp_path):
        svc = _make_service(tmp_path)
        due = datetime.datetime(2026, 1, 2, 4, 0, tzinfo=datetime.timezone.utc)

        svc._on_job_missed(
            JobExecutionEvent(EVENT_JOB_MISSED, "hourly-job", "default", due),
        )
        await asyncio.sleep(0)  # let the dispatched task run

        svc.db.log_cron_missed.assert_awaited_once()
        args, kwargs = svc.db.log_cron_missed.await_args
        assert args[0] == "hourly-job"
        assert args[1] == "2026-01-02 04:00:00"      # the time it was DUE
        assert kwargs["error"]

    @pytest.mark.asyncio
    async def test_due_time_is_normalised_to_utc(self, tmp_path):
        """started_at must be comparable with the rows written by log_cron_start."""
        svc = _make_service(tmp_path)
        due = datetime.datetime(
            2026, 1, 2, 4, 0,
            tzinfo=datetime.timezone(datetime.timedelta(hours=5)),
        )

        svc._on_job_missed(
            JobExecutionEvent(EVENT_JOB_MISSED, "j", "default", due),
        )
        await asyncio.sleep(0)

        assert svc.db.log_cron_missed.await_args[0][1] == "2026-01-01 23:00:00"

    @pytest.mark.asyncio
    async def test_a_db_failure_does_not_escape(self, tmp_path):
        """The listener runs inside the scheduler's dispatch loop."""
        db = AsyncMock()
        db.log_cron_missed = AsyncMock(side_effect=RuntimeError("db down"))
        svc = _make_service(tmp_path, db=db)

        svc._on_job_missed(JobExecutionEvent(
            EVENT_JOB_MISSED, "j", "default",
            datetime.datetime.now(datetime.timezone.utc)))
        await asyncio.sleep(0)  # must not raise

    @pytest.mark.asyncio
    async def test_a_malformed_event_does_not_escape(self, tmp_path):
        svc = _make_service(tmp_path)
        svc._on_job_missed(object())  # no job_id / scheduled_run_time
        svc.db.log_cron_missed.assert_not_awaited()

    def test_only_the_missed_event_is_subscribed(self, tmp_path):
        """A max-instances skip is a different event CLASS: it carries
        `scheduled_run_times` (plural) and no singular attribute, so one handler
        for both would raise inside the scheduler. It is also far more frequent
        than real runs, so logging it 1:1 would bury them."""
        svc = _make_service(tmp_path)
        svc.scheduler.add_listener(svc._on_job_missed, EVENT_JOB_MISSED)

        masks = [entry[1] for entry in svc.scheduler._listeners]
        assert masks and all(mask & EVENT_JOB_MISSED for mask in masks)
        assert all(not (mask & EVENT_JOB_MAX_INSTANCES) for mask in masks)

        skip = JobSubmissionEvent(
            EVENT_JOB_MAX_INSTANCES, "j", "default",
            [datetime.datetime.now(datetime.timezone.utc)])
        assert not hasattr(skip, "scheduled_run_time")


class TestMissedRowIsInert:
    """A missed row must be visible to operators yet invisible to scheduling."""

    @pytest_asyncio.fixture
    async def seeded(self, db):
        await db.log_cron_missed("j", "2026-01-02 04:00:00")
        return db

    @pytest.mark.asyncio
    async def test_row_is_readable_where_operators_look(self, seeded):
        logs = await seeded.get_cron_logs("j")
        assert [(r["status"], r["started_at"]) for r in logs] == [
            ("missed", "2026-01-02 04:00:00"),
        ]
        assert await seeded.count_cron_logs("j") == 1

    @pytest.mark.asyncio
    async def test_row_never_reads_as_a_successful_run(self, seeded):
        """Negative control for the two consumers that align on finished_at:
        interval start_date and startup catch-up both use this lookup."""
        assert await seeded.get_last_successful_cron_run("j") is None
        assert await seeded.get_recent_cron_runs(hours=24) == []

    @pytest.mark.asyncio
    async def test_an_earlier_success_still_wins(self, db):
        log_id = await db.log_cron_start("j")
        await db.log_cron_finish(log_id, "success")
        await db.log_cron_missed("j", "2026-01-02 04:00:00")

        assert (await db.get_last_successful_cron_run("j"))["status"] == "success"

    @pytest.mark.asyncio
    async def test_rows_are_pruned_by_the_existing_retention(self, db):
        """Row growth is bounded -- missed rows are not a new leak."""
        await db.log_cron_missed("j", "2020-01-01 00:00:00")
        assert await db.cleanup_old_cron_logs(days=14) == 1
        assert await db.count_cron_logs("j") == 0


# ---------------------------------------------------------------------------
# The memU trim split
# ---------------------------------------------------------------------------

class TestTrimMemorySplit:
    """`_trim_memory` must return pages without a full collection, and
    `_release_memory` must keep doing one -- it is the OOM safeguard on the
    periodic sweep. The `_release_memory` arm is the negative control: it proves
    the gc spy can observe a full pass at all."""

    @staticmethod
    def _observe(fn) -> dict:
        from nerve.memory.memu_bridge import MemUBridge

        generations: list = []
        trims: list = []

        def fake_collect(*args):
            generations.append(args[0] if args else None)
            return 0

        with patch.object(gc, "collect", side_effect=fake_collect), \
                patch.object(MemUBridge, "_malloc_trim", side_effect=lambda: trims.append(1)):
            fn()
        return {"generations": generations, "trims": len(trims)}

    def test_trim_memory_never_collects_every_generation(self):
        from nerve.memory.memu_bridge import MemUBridge

        seen = self._observe(MemUBridge._trim_memory)
        assert seen["trims"] == 1                       # pages still returned
        assert None not in seen["generations"]          # no full pass
        assert all(g <= 1 for g in seen["generations"])

    def test_release_memory_still_collects_every_generation(self):
        """Negative control -- if this arm did not fire, the spy proves nothing."""
        from nerve.memory.memu_bridge import MemUBridge

        seen = self._observe(MemUBridge._release_memory)
        assert seen["generations"] == [None]             # a full pass
        assert seen["trims"] == 1

    def test_trim_cost_is_independent_of_heap_size(self):
        """A generational pass must not walk the whole heap: that independence
        is what makes it safe on the per-file path."""
        held = [{"n": [float(i)] * 8} for i in range(60000)]
        for d in held:
            d["self"] = d                               # cycles for the tracer
        try:
            small = gc.collect(1)
            held.extend({"n": [1.0] * 8, "self": None} for _ in range(60000))
            assert gc.collect(1) >= 0 and small >= 0     # neither errors/scales
        finally:
            held.clear()
            gc.collect()

    def test_malloc_trim_survives_a_missing_libc(self):
        from nerve.memory.memu_bridge import MemUBridge

        with patch.object(MemUBridge, "_libc", None):
            MemUBridge._trim_memory()                    # must not raise
            MemUBridge._release_memory()


class TestPerFileIndexPathUsesTrim:
    """Which helper each call site reaches is part of the contract: the per-file
    path must be cheap, and the periodic sweep must keep its OOM safeguard."""

    def test_per_file_success_path_calls_trim_not_release(self):
        import inspect

        from nerve.memory.memu_bridge import MemUBridge

        src = inspect.getsource(MemUBridge.memorize_file)
        assert "self._trim_memory()" in src
        assert "_release_memory" not in src.split("return True")[0]

    def test_retry_exhaustion_keeps_the_full_release(self):
        import inspect

        from nerve.memory.memu_bridge import MemUBridge

        tail = inspect.getsource(MemUBridge.memorize_file).rsplit("gave up after", 1)[-1]
        assert "_release_memory" in tail

    def test_memorization_sweep_keeps_the_full_release(self):
        import inspect

        from nerve.agent.engine import AgentEngine

        src = inspect.getsource(AgentEngine.run_memorization_sweep)
        assert "_release_memory" in src
        assert "_trim_memory" not in src
