"""After a pause of the whole machine — host sleep, suspend, a VM snapshot
restore — the cron service moves its triggers past the pause and runs once what
came due during it, instead of letting the scheduler drop those runs as late.

The service reads its clocks through the ``time`` module it imported, which these
tests replace with :class:`FakeClock`: from inside a process, a pause is the wall
clock jumping ahead while the monotonic clock did not move.
"""

from __future__ import annotations

import asyncio
import logging
import time as real_time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from apscheduler.triggers.cron import CronTrigger

from nerve.agent.engine import TurnOutcome, TurnResult
from nerve.cron import service as cron_service
from nerve.cron.jobs import CronJob
from nerve.cron.service import CronService
from nerve.sources.models import IngestResult
from tests.actor_rows import mock_system_principal

HOUR = 3600.0
EVERY_TEN_MINUTES = "*/10 * * * *"


class FakeClock:
    """Stands in for the ``time`` module the cron service reads its clocks from.

    Both clocks follow the real ones; the wall clock adds an offset the test
    moves. Moving it alone is what a pause looks like from inside the process.
    """

    def __init__(self, wall_offset: float = 0.0) -> None:
        self.wall_offset = wall_offset

    def time(self) -> float:
        return real_time.time() + self.wall_offset

    @staticmethod
    def monotonic() -> float:
        return real_time.monotonic()

    def pause(self, seconds: float) -> None:
        """The machine stood still for *seconds*: only the wall clock moved."""
        self.wall_offset += seconds


def _mock_db() -> AsyncMock:
    db = AsyncMock()
    db.log_cron_start = AsyncMock(return_value=1)
    db.get_last_successful_cron_run = AsyncMock(return_value=None)
    mock_system_principal(db)
    return db


def _service(tmp_path, db=None) -> CronService:
    config = MagicMock()
    config.timezone = "UTC"
    config.lockdown = False
    config.cron.gate_plugins_dir = tmp_path / "gates"  # absent -> no plugins
    config.agent.cron_model = "test-model"
    engine = AsyncMock()
    engine.run_cron = AsyncMock(return_value=TurnResult("ok", TurnOutcome()))
    return CronService(config, engine, db if db is not None else _mock_db())


async def _start(svc: CronService, jobs=(), runners=()) -> None:
    """Start *svc* with *jobs* and source *runners* as its whole schedule."""

    def register() -> None:
        svc._source_runners = list(runners)
        svc._schedule_source_runners(svc._source_runners)

    with (
        patch.object(svc, "_load_merged_jobs", return_value=list(jobs)),
        patch.object(svc, "_register_source_runners", side_effect=register),
        patch.object(svc, "_catchup_missed_jobs", AsyncMock()),
    ):
        await svc.start()


def _runner(name: str = "demo") -> SimpleNamespace:
    """A source runner as the scheduler sees one."""
    return SimpleNamespace(
        source=SimpleNamespace(source_name=name),
        job_id=f"source:{name}",
        schedule="*/15 * * * *",
        run=AsyncMock(return_value=IngestResult(records_ingested=1)),
        set_notification_service=MagicMock(),
    )


def _first_fire_after(schedule: str, moment: datetime) -> datetime:
    return CronTrigger.from_crontab(schedule, timezone=UTC).get_next_fire_time(
        None, moment,
    )


async def _wait_for(predicate, timeout: float = 5.0) -> None:
    deadline = real_time.monotonic() + timeout
    while not predicate():
        if real_time.monotonic() > deadline:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0.01)


def _ok() -> TurnResult:
    return TurnResult("ok", TurnOutcome())


# ---------------------------------------------------------------------------
# The watcher
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def watched(tmp_path, monkeypatch):
    """A clock watcher sampling every 10 ms, with its step handler recorded."""
    clock = FakeClock()
    monkeypatch.setattr(cron_service, "time", clock)
    monkeypatch.setattr(cron_service, "_CLOCK_WATCH_SECONDS", 0.01)
    svc = _service(tmp_path)
    svc._on_clock_step = AsyncMock()
    # Held, so the scheduler never wakes up and takes a sample of its own.
    svc.scheduler.start(paused=True)
    svc._clock_watch_task = asyncio.create_task(svc._watch_clock())
    await _wait_for(lambda: svc._clock_sample is not None)
    yield svc, clock
    await svc.stop()


@pytest.mark.asyncio
async def test_a_wall_clock_jump_is_handled_as_a_pause(watched):
    svc, clock = watched
    before = datetime.now(UTC)

    clock.pause(3 * HOUR)
    await _wait_for(lambda: svc._on_clock_step.await_count)

    paused_from, paused_to, step = svc._on_clock_step.await_args.args
    assert step == pytest.approx(3 * HOUR, abs=1)
    # From the last sample before the pause to the first one after it.
    assert paused_from <= before + timedelta(seconds=1)
    assert paused_to - paused_from >= timedelta(hours=3)
    assert paused_to - paused_from < timedelta(hours=3, seconds=1)
    await asyncio.sleep(0.05)
    assert svc._on_clock_step.await_count == 1


@pytest.mark.asyncio
async def test_a_two_second_jump_is_ignored(watched):
    svc, clock = watched

    clock.pause(2)
    await asyncio.sleep(0.1)

    svc._on_clock_step.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_clock_set_back_is_not_a_pause(watched):
    svc, clock = watched

    clock.pause(-HOUR)
    await asyncio.sleep(0.1)

    svc._on_clock_step.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_watcher_stops_with_stop(tmp_path, monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(cron_service, "time", clock)
    monkeypatch.setattr(cron_service, "_CLOCK_WATCH_SECONDS", 0.01)
    svc = _service(tmp_path)
    svc._on_clock_step = AsyncMock()
    await _start(svc)
    watcher = svc._clock_watch_task
    await asyncio.sleep(0.05)
    assert not watcher.done()

    await svc.stop()

    assert watcher.cancelled()
    assert svc._clock_watch_task is None
    clock.pause(3 * HOUR)
    await asyncio.sleep(0.05)
    svc._on_clock_step.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_watcher_ends_when_the_scheduler_is_shut_down(tmp_path, monkeypatch):
    """Callers that shut the scheduler down directly leave nothing running."""
    monkeypatch.setattr(cron_service, "_CLOCK_WATCH_SECONDS", 0.01)
    svc = _service(tmp_path)
    await _start(svc)
    watcher = svc._clock_watch_task

    svc.scheduler.shutdown(wait=False)

    await _wait_for(watcher.done)
    assert not watcher.cancelled()


# ---------------------------------------------------------------------------
# What a pause does to the schedule
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def held_service(tmp_path, monkeypatch):
    """Factory: a started service whose scheduler is held and whose watcher
    stays idle, so a test stages a pause and hands it to ``_on_clock_step``."""
    monkeypatch.setattr(cron_service, "_CLOCK_WATCH_SECONDS", HOUR)
    started: list[CronService] = []

    async def make(jobs=(), runners=(), db=None) -> CronService:
        svc = _service(tmp_path, db)
        await _start(svc, jobs, runners)
        svc.scheduler.pause()
        started.append(svc)
        return svc

    yield make
    for svc in started:
        await svc.stop()


def _pause_window(hours: float = 3) -> tuple[datetime, datetime]:
    resumed = datetime.now(UTC)
    return resumed - timedelta(hours=hours), resumed


@pytest.mark.asyncio
async def test_a_job_due_during_the_pause_runs_once_and_moves_past_it(held_service):
    job = CronJob(id="every-ten", schedule=EVERY_TEN_MINUTES, prompt="check")
    svc = await held_service([job])
    paused, resumed = _pause_window()
    # The run the scheduler was waiting for when the machine stopped.
    svc.scheduler.modify_job(
        "every-ten", next_run_time=_first_fire_after(EVERY_TEN_MINUTES, paused),
    )

    await svc._on_clock_step(paused, resumed)

    svc.engine.run_cron.assert_awaited_once()
    upcoming = svc.scheduler.get_job("every-ten").next_run_time
    assert resumed < upcoming <= resumed + timedelta(minutes=10)
    assert (upcoming.minute % 10, upcoming.second) == (0, 0)
    # Let the scheduler look: nothing is due, so nothing runs again or is missed.
    svc.scheduler.resume()
    await asyncio.sleep(0.1)
    svc.engine.run_cron.assert_awaited_once()
    svc.db.log_cron_missed.assert_not_awaited()


@pytest.mark.asyncio
async def test_only_catch_up_jobs_run_but_every_trigger_moves_on(held_service):
    wants = CronJob(id="wants", schedule=EVERY_TEN_MINUTES, prompt="p")
    opted_out = CronJob(
        id="no-catchup", schedule=EVERY_TEN_MINUTES, prompt="p", catchup=False,
    )
    yearly = CronJob(id="yearly", schedule="0 0 1 1 *", prompt="p")
    runner = _runner("demo")
    svc = await held_service([wants, opted_out, yearly], [runner])
    paused, resumed = _pause_window()
    came_due = resumed - timedelta(hours=2)
    moved = ("wants", "no-catchup", "source:demo", "cleanup", "wakeup_sweep")
    for job_id in moved:
        svc.scheduler.modify_job(job_id, next_run_time=came_due)
    not_due = svc.scheduler.get_job("yearly").next_run_time
    # Not the service's: keeps the scheduler's own handling.
    svc.scheduler.add_job(AsyncMock(), "date", run_date=came_due, id="foreign")

    await svc._on_clock_step(paused, resumed)

    ran = [c.kwargs["job_id"] for c in svc.engine.run_cron.await_args_list]
    assert ran == ["wants"]
    runner.run.assert_awaited_once()
    # Cleanup and the wakeup sweep were re-armed, not run.
    svc.db.cleanup_expired_messages.assert_not_awaited()
    svc.db.get_due_wakeups.assert_not_awaited()
    for job_id in moved:
        assert svc.scheduler.get_job(job_id).next_run_time > resumed, job_id
    assert svc.scheduler.get_job("yearly").next_run_time == not_due
    assert svc.scheduler.get_job("foreign").next_run_time == came_due


@pytest.mark.asyncio
async def test_an_interval_job_stays_on_its_anchored_cadence(held_service):
    resumed = datetime.now(UTC).replace(microsecond=0)
    last_success = resumed - timedelta(hours=5)
    db = _mock_db()
    db.get_last_successful_cron_run = AsyncMock(
        return_value={"finished_at": last_success.isoformat()},
    )
    job = CronJob(id="every-four", schedule="4h", prompt="p")
    svc = await held_service([job], db=db)
    # Anchored at the last success: the run at +4h came due during the pause.
    svc.scheduler.modify_job(
        "every-four", next_run_time=last_success + timedelta(hours=4),
    )

    await svc._on_clock_step(resumed - timedelta(hours=3), resumed)

    svc.engine.run_cron.assert_awaited_once()
    # The next run on the same grid, not four hours from the resume.
    assert svc.scheduler.get_job("every-four").next_run_time == (
        last_success + timedelta(hours=8)
    )


@pytest.mark.asyncio
async def test_a_catch_up_is_gated_like_a_scheduled_run(held_service):
    job = CronJob(
        id="planner", schedule="0 * * * *", prompt="p",
        run_if=[{"type": "tasks", "status": "pending"}],
    )
    svc = await held_service([job])
    svc.db.count_tasks = AsyncMock(return_value=0)
    paused, resumed = _pause_window()
    svc.scheduler.modify_job("planner", next_run_time=resumed - timedelta(hours=1))

    await svc._on_clock_step(paused, resumed)

    svc.db.count_tasks.assert_awaited()
    svc.engine.run_cron.assert_not_awaited()
    svc.db.log_cron_start.assert_not_awaited()
    assert svc.scheduler.get_job("planner").next_run_time > resumed


@pytest.mark.asyncio
async def test_a_locked_job_catch_up_waits_for_its_running_run(held_service):
    job = CronJob(id="locked", schedule=EVERY_TEN_MINUTES, prompt="p", lock=True)
    svc = await held_service([job])
    release = asyncio.Event()
    order: list[str] = []

    async def run_cron(**_kwargs) -> TurnResult:
        n = sum(1 for o in order if o.startswith("start")) + 1
        order.append(f"start {n}")
        if n == 1:
            await release.wait()
        order.append(f"end {n}")
        return _ok()

    svc.engine.run_cron = run_cron
    in_flight = asyncio.create_task(svc._run_job_wrapper(job))
    await _wait_for(lambda: order == ["start 1"])
    paused, resumed = _pause_window()
    svc.scheduler.modify_job("locked", next_run_time=resumed - timedelta(hours=1))

    catch_up = asyncio.create_task(svc._on_clock_step(paused, resumed))
    await asyncio.sleep(0.05)
    assert order == ["start 1"]
    release.set()
    await asyncio.gather(in_flight, catch_up)

    assert order == ["start 1", "end 1", "start 2", "end 2"]


@pytest.mark.asyncio
async def test_a_pause_during_a_catch_up_waits_for_it(held_service):
    """The second pause re-arms at once; its runs wait for the first catch-up."""
    first = CronJob(id="first", schedule=EVERY_TEN_MINUTES, prompt="p")
    second = CronJob(id="second", schedule=EVERY_TEN_MINUTES, prompt="p")
    svc = await held_service([first, second])
    release = asyncio.Event()
    order: list[str] = []

    async def run_cron(*, job_id, **_kwargs) -> TurnResult:
        order.append(f"{job_id} start")
        if job_id == "first":
            await release.wait()
        order.append(f"{job_id} end")
        return _ok()

    svc.engine.run_cron = run_cron
    paused, resumed = _pause_window()
    svc.scheduler.modify_job("first", next_run_time=resumed - timedelta(hours=2))
    first_pause = asyncio.create_task(svc._on_clock_step(paused, resumed))
    await _wait_for(lambda: order == ["first start"])

    paused_again, resumed_again = _pause_window(hours=1)
    svc.scheduler.modify_job(
        "second", next_run_time=resumed_again - timedelta(minutes=30),
    )
    second_pause = asyncio.create_task(svc._on_clock_step(paused_again, resumed_again))
    await asyncio.sleep(0.05)

    assert svc.scheduler.get_job("second").next_run_time > resumed_again
    assert order == ["first start"]
    release.set()
    await asyncio.gather(first_pause, second_pause)
    assert order == ["first start", "first end", "second start", "second end"]


@pytest.mark.asyncio
async def test_a_pause_during_the_startup_catch_up_waits_for_it(held_service):
    overdue = CronJob(id="overdue", schedule=EVERY_TEN_MINUTES, prompt="p")
    due = CronJob(id="due", schedule=EVERY_TEN_MINUTES, prompt="p")
    db = _mock_db()
    long_ago = (datetime.now(UTC) - timedelta(days=1)).isoformat()
    db.get_last_successful_cron_run = AsyncMock(
        side_effect=lambda job_id: (
            {"finished_at": long_ago} if job_id == "overdue" else None
        ),
    )
    svc = await held_service([overdue, due], db=db)
    release = asyncio.Event()
    order: list[str] = []

    async def run_cron(*, job_id, **_kwargs) -> TurnResult:
        order.append(f"{job_id} start")
        if job_id == "overdue":
            await release.wait()
        order.append(f"{job_id} end")
        return _ok()

    svc.engine.run_cron = run_cron
    startup = asyncio.create_task(svc._catchup_missed_jobs())
    await _wait_for(lambda: order == ["overdue start"])
    paused, resumed = _pause_window()
    svc.scheduler.modify_job("due", next_run_time=resumed - timedelta(hours=1))

    after_pause = asyncio.create_task(svc._on_clock_step(paused, resumed))
    await asyncio.sleep(0.05)
    assert order == ["overdue start"]
    release.set()
    await asyncio.gather(startup, after_pause)

    assert order == ["overdue start", "overdue end", "due start", "due end"]


@pytest.mark.asyncio
async def test_one_info_line_per_pause(held_service, caplog):
    job = CronJob(id="every-ten", schedule=EVERY_TEN_MINUTES, prompt="p")
    svc = await held_service([job])
    paused, resumed = _pause_window()
    svc.scheduler.modify_job("every-ten", next_run_time=resumed - timedelta(hours=1))

    with caplog.at_level(logging.INFO, logger="nerve.cron.service"):
        await svc._on_clock_step(paused, resumed, 3 * HOUR)

    lines = [
        r.getMessage() for r in caplog.records
        if r.getMessage().startswith("Clock jumped")
    ]
    assert len(lines) == 1
    assert "10800s ahead" in lines[0]
    assert paused.isoformat(timespec="seconds") in lines[0]
    assert resumed.isoformat(timespec="seconds") in lines[0]
    assert "catching up 1" in lines[0]


# ---------------------------------------------------------------------------
# End to end: a running scheduler, a real database
# ---------------------------------------------------------------------------


async def _cron_statuses(db, job_id: str) -> list[str]:
    return [row["status"] for row in await db.get_cron_logs(job_id)]


async def _stage_ten_minute_job(tmp_path, db, monkeypatch, *, watch_seconds):
    """A service with one ten-minute job, its clocks three hours behind: the
    world as the process saw it before a three-hour pause."""
    clock = FakeClock(wall_offset=-3 * HOUR)
    monkeypatch.setattr(cron_service, "time", clock)
    monkeypatch.setattr(cron_service, "_CLOCK_WATCH_SECONDS", watch_seconds)
    svc = _service(tmp_path, db)
    job = CronJob(id="every-ten", schedule=EVERY_TEN_MINUTES, prompt="check")
    return svc, job, clock


@pytest.mark.asyncio
async def test_after_a_three_hour_pause_a_ten_minute_job_runs_once(
    tmp_path, db, monkeypatch,
):
    """The watcher notices the pause first."""
    svc, job, clock = await _stage_ten_minute_job(
        tmp_path, db, monkeypatch, watch_seconds=0.01,
    )
    handled = asyncio.Event()
    steps: list[float] = []
    on_clock_step = svc._on_clock_step

    async def spy(paused_from, paused_to, step=None) -> None:
        steps.append(step)
        await on_clock_step(paused_from, paused_to, step)
        handled.set()

    svc._on_clock_step = spy
    await _start(svc, [job])
    try:
        # Hold the scheduler and clear its timer while the pause is staged, so
        # only the watcher can notice it, and let the watcher take its samples.
        svc.scheduler.pause()
        svc.scheduler.wakeup()
        await asyncio.sleep(0.05)
        when_stopped = datetime.now(UTC) - timedelta(hours=3)
        svc.scheduler.modify_job(
            "every-ten",
            next_run_time=_first_fire_after(EVERY_TEN_MINUTES, when_stopped),
        )

        clock.pause(3 * HOUR)
        await asyncio.wait_for(handled.wait(), timeout=5)
        svc.scheduler.resume()
        await asyncio.sleep(0.2)

        assert steps == [pytest.approx(3 * HOUR, abs=1)]
        assert await _cron_statuses(db, "every-ten") == ["success"]
        svc.engine.run_cron.assert_awaited_once()
        assert svc.scheduler.get_job("every-ten").next_run_time > datetime.now(UTC)
    finally:
        await svc.stop()


@pytest.mark.asyncio
async def test_a_pause_the_scheduler_notices_first_is_caught_up_too(
    tmp_path, db, monkeypatch,
):
    """The scheduler's wakeup comes before the watcher's next sample: it has the
    clock checked before judging the run, so the run is caught up, not missed."""
    svc, job, clock = await _stage_ten_minute_job(
        tmp_path, db, monkeypatch, watch_seconds=HOUR,
    )
    await _start(svc, [job])
    try:
        await asyncio.sleep(0)  # the scheduler's first pass samples the clock
        when_stopped = datetime.now(UTC) - timedelta(hours=3)

        # As a resume presents it to the scheduler, in one step: the run it
        # waits for is three hours old, and the wall clock has jumped.
        svc.scheduler.modify_job(
            "every-ten",
            next_run_time=_first_fire_after(EVERY_TEN_MINUTES, when_stopped),
        )
        clock.pause(3 * HOUR)
        await _wait_for(lambda: svc.engine.run_cron.await_count)
        await _wait_for(lambda: not svc._catchup_tasks)
        await asyncio.sleep(0.2)

        assert await _cron_statuses(db, "every-ten") == ["success"]
        svc.engine.run_cron.assert_awaited_once()
        assert svc.scheduler.get_job("every-ten").next_run_time > datetime.now(UTC)
    finally:
        await svc.stop()
