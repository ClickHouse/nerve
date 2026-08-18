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
import weakref
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

# Enough cycles that a partial collection cannot be mistaken for none.
_CYCLES = 500


class _Cycle:
    """Half of a two-object reference cycle: only the tracer can free it."""

    __slots__ = ("peer", "__weakref__")


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


async def _start_without_scheduling(svc: CronService) -> None:
    """Run ``start()`` for its side effects on the scheduler object only.

    Job loading, source runners, the catch-up task and the scheduler's own
    ``start`` are stubbed, so what remains observable is the wiring ``start()``
    performs -- which is what the listener tests are about.
    """
    with patch.object(svc, "_load_merged_jobs", return_value=[]), \
            patch.object(svc, "_register_source_runners"), \
            patch.object(svc, "_catchup_missed_jobs", AsyncMock()), \
            patch.object(svc.scheduler, "start"):
        await svc.start()


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

    @pytest.mark.asyncio
    async def test_only_the_missed_event_is_subscribed(self, tmp_path):
        """A max-instances skip is a different event CLASS: it carries
        `scheduled_run_times` (plural) and no singular attribute, so one handler
        for both would raise inside the scheduler. It is also far more frequent
        than real runs, so logging it 1:1 would bury them.

        ``start()`` is driven so the subscription observed is the one production
        registers -- asserting on a listener the test added itself would hold
        even if ``start()`` registered none.
        """
        svc = _make_service(tmp_path)
        await _start_without_scheduling(svc)

        masks = [entry[1] for entry in svc.scheduler._listeners]
        assert masks and all(mask & EVENT_JOB_MISSED for mask in masks)
        assert all(not (mask & EVENT_JOB_MAX_INSTANCES) for mask in masks)

        skip = JobSubmissionEvent(
            EVENT_JOB_MAX_INSTANCES, "j", "default",
            [datetime.datetime.now(datetime.timezone.utc)])
        assert not hasattr(skip, "scheduled_run_time")

    @pytest.mark.asyncio
    async def test_the_subscription_is_the_one_start_registers(self, tmp_path):
        """Reddens if the ``add_listener`` call is dropped from ``start()``."""
        svc = _make_service(tmp_path)
        assert svc.scheduler._listeners == []

        await _start_without_scheduling(svc)

        assert [entry[0] for entry in svc.scheduler._listeners] == [svc._on_job_missed]

    @pytest.mark.asyncio
    async def test_a_reload_does_not_double_subscribe(self, tmp_path):
        """Two listeners would write two rows for one dropped run."""
        svc = _make_service(tmp_path)
        await _start_without_scheduling(svc)

        with patch.object(svc, "_load_merged_jobs", return_value=[]), \
                patch.object(svc, "_register_source_runners"):
            await svc.reload()

        assert len(svc.scheduler._listeners) == 1


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

    @staticmethod
    def _generations_walked(fn) -> list[int]:
        """Generations the real collector entered while ``fn`` ran.

        Automatic collection is disabled so only the pass under test is
        recorded, and ``_malloc_trim`` is stubbed to keep the observation to
        the Python heap.
        """
        from nerve.memory.memu_bridge import MemUBridge

        walked: list[int] = []

        def record(phase, info):
            if phase == "start":
                walked.append(info["generation"])

        gc.disable()
        gc.callbacks.append(record)
        try:
            with patch.object(MemUBridge, "_malloc_trim"):
                fn()
        finally:
            gc.callbacks.remove(record)
            gc.enable()
        return walked

    @staticmethod
    def _survivors(fn) -> tuple[int, int]:
        """``(aged, young)`` unreachable cycles still alive after ``fn``.

        ``aged`` cycles are forced into the oldest generation by collecting
        three times while they are still referenced; ``young`` ones have never
        survived a pass. Both are unreachable before ``fn`` runs, so whether
        they die reports which generations the pass covered -- no timing.
        """
        from nerve.memory.memu_bridge import MemUBridge

        def unreachable_cycles(count: int) -> tuple[list, list]:
            strong, refs = [], []
            for _ in range(count):
                first, second = _Cycle(), _Cycle()
                first.peer, second.peer = second, first
                strong.append(first)
                refs.append(weakref.ref(first))
            return strong, refs

        gc.disable()
        try:
            aged_strong, aged = unreachable_cycles(_CYCLES)
            for _ in range(3):
                gc.collect()             # age them into the oldest generation
            aged_strong.clear()          # unreachable, but no longer young
            young_strong, young = unreachable_cycles(_CYCLES)
            young_strong.clear()

            with patch.object(MemUBridge, "_malloc_trim"):
                fn()

            return (
                sum(1 for ref in aged if ref() is not None),
                sum(1 for ref in young if ref() is not None),
            )
        finally:
            gc.enable()
            gc.collect()

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

    def test_trim_skips_the_oldest_generation_that_release_walks(self):
        """Cost is bounded because the oldest generation -- the one holding the
        long-lived heap -- is never walked. Observed through ``gc.callbacks``,
        which reports the generation each pass actually enters.

        Reddens if ``_trim_memory`` calls ``gc.collect()``: the recorded
        generation becomes the oldest and equals ``_release_memory``'s.
        """
        from nerve.memory.memu_bridge import MemUBridge

        oldest = len(gc.get_threshold()) - 1

        assert self._generations_walked(MemUBridge._trim_memory) == [1]
        assert self._generations_walked(MemUBridge._release_memory) == [oldest]

    def test_trim_leaves_the_aged_heap_untouched(self):
        """The behavioural half: garbage aged into the oldest generation
        survives a trim and dies under a release, so the pass really is
        narrower rather than merely reported as such.

        ``_release_memory`` is the negative control -- if its arm did not
        reclaim, the fixture would not be producing collectable garbage and
        the trim arm would prove nothing. Young garbage dies either way,
        which is what makes the per-file path still reclaim what a memorize
        just left behind.
        """
        from nerve.memory.memu_bridge import MemUBridge

        aged_after_trim, young_after_trim = self._survivors(MemUBridge._trim_memory)
        aged_after_release, young_after_release = self._survivors(
            MemUBridge._release_memory,
        )

        assert aged_after_trim == _CYCLES        # oldest generation not walked
        assert aged_after_release == 0           # control: it IS collectable
        assert young_after_trim == 0             # the memorize leftovers still go
        assert young_after_release == 0

    def test_malloc_trim_survives_a_missing_libc(self):
        from nerve.memory.memu_bridge import MemUBridge

        with patch.object(MemUBridge, "_libc", None):
            MemUBridge._trim_memory()                    # must not raise
            MemUBridge._release_memory()


class TestPerFileIndexPathUsesTrim:
    """Which helper each call site reaches is part of the contract: the per-file
    path must be cheap, and the periodic sweep must keep its OOM safeguard.

    The two ``memorize_file`` paths are driven for real, so they redden if the
    call site moves rather than only if its source text changes.
    """

    @staticmethod
    def _bridge(tmp_path):
        """Available bridge with a mocked service and no memU loop, so
        ``_submit`` awaits inline and no retry sleeps."""
        from nerve.config import MemoryConfig, NerveConfig
        from nerve.memory.memu_bridge import MemUBridge

        config = NerveConfig()
        config.memory = MemoryConfig(
            sqlite_dsn=f"sqlite:///{tmp_path / 'memu.sqlite'}",
        )
        config.anthropic_api_key = "test-key"

        bridge = MemUBridge(config)
        bridge._available = True
        bridge._service = MagicMock()
        bridge._MEMORIZE_RETRY_DELAY = 0
        return bridge

    @pytest.mark.asyncio
    async def test_per_file_success_path_calls_trim_not_release(self, tmp_path):
        """Reddens if the success path is reverted to ``_release_memory``."""
        from nerve.memory.memu_bridge import MemUBridge

        bridge = self._bridge(tmp_path)
        bridge._service.memorize = AsyncMock(return_value={"items": []})
        target = tmp_path / "note.txt"
        target.write_text("knowledge: a fact")

        with patch.object(MemUBridge, "_trim_memory") as trim, \
                patch.object(MemUBridge, "_release_memory") as release:
            assert await bridge.memorize_file(str(target)) is True

        assert trim.call_count == 1
        assert release.call_count == 0

    @pytest.mark.asyncio
    async def test_retry_exhaustion_keeps_the_full_release(self, tmp_path):
        """The cold path still pays for a full pass -- and the success path's
        trim is not what is being observed here, since it never runs."""
        from nerve.memory.memu_bridge import MemUBridge

        bridge = self._bridge(tmp_path)
        bridge._service.memorize = AsyncMock(side_effect=asyncio.TimeoutError())
        target = tmp_path / "note.txt"
        target.write_text("knowledge: a fact")

        with patch.object(MemUBridge, "_trim_memory") as trim, \
                patch.object(MemUBridge, "_release_memory") as release, \
                patch.object(bridge, "_reset_llm_clients", AsyncMock()):
            assert await bridge.memorize_file(str(target)) is False

        assert release.call_count == 1
        assert trim.call_count == 0

    def test_memorization_sweep_keeps_the_full_release(self):
        """Text oracle by exception: driving the sweep needs a whole engine,
        and the assertion is only that this call site was left alone.
        """
        import inspect

        from nerve.agent.engine import AgentEngine

        src = inspect.getsource(AgentEngine.run_memorization_sweep)
        assert "_release_memory" in src
        assert "_trim_memory" not in src
