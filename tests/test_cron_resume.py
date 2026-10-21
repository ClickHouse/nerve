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


async def _resume(
    svc: CronService, paused: datetime, resumed: datetime, step: float | None = None,
) -> None:
    """Hand *svc* a pause from *paused* to *resumed*, and wait for its catch-up."""
    catch_up = svc._on_clock_step(paused, resumed, step)
    if catch_up is not None:
        await catch_up


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
    svc._on_clock_step = MagicMock(return_value=None)
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
    await _wait_for(lambda: svc._on_clock_step.call_count)

    paused_from, paused_to, step = svc._on_clock_step.call_args.args
    assert step == pytest.approx(3 * HOUR, abs=1)
    # From the last sample before the pause to the first one after it.
    assert paused_from <= before + timedelta(seconds=1)
    assert paused_to - paused_from >= timedelta(hours=3)
    assert paused_to - paused_from < timedelta(hours=3, seconds=1)
    await asyncio.sleep(0.05)
    assert svc._on_clock_step.call_count == 1


@pytest.mark.asyncio
async def test_a_two_second_jump_is_ignored(watched):
    svc, clock = watched

    clock.pause(2)
    await asyncio.sleep(0.1)

    svc._on_clock_step.assert_not_called()


@pytest.mark.asyncio
async def test_a_clock_set_back_is_not_a_pause(watched):
    svc, clock = watched

    clock.pause(-HOUR)
    await asyncio.sleep(0.1)

    svc._on_clock_step.assert_not_called()


@pytest.mark.asyncio
async def test_the_watcher_stops_with_stop(tmp_path, monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(cron_service, "time", clock)
    monkeypatch.setattr(cron_service, "_CLOCK_WATCH_SECONDS", 0.01)
    svc = _service(tmp_path)
    svc._on_clock_step = MagicMock(return_value=None)
    await _start(svc)
    watcher = svc._clock_watch_task
    await asyncio.sleep(0.05)
    assert not watcher.done()

    await svc.stop()

    assert watcher.cancelled()
    assert svc._clock_watch_task is None
    clock.pause(3 * HOUR)
    await asyncio.sleep(0.05)
    svc._on_clock_step.assert_not_called()


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


@pytest.mark.asyncio
async def test_the_watcher_keeps_watching_while_a_catch_up_runs(tmp_path, monkeypatch):
    """A second pause moves its triggers at once, not after the first catch-up."""
    clock = FakeClock()
    monkeypatch.setattr(cron_service, "time", clock)
    monkeypatch.setattr(cron_service, "_CLOCK_WATCH_SECONDS", 0.01)
    first = CronJob(id="first", schedule=EVERY_TEN_MINUTES, prompt="p")
    second = CronJob(id="second", schedule=EVERY_TEN_MINUTES, prompt="p")
    svc = _service(tmp_path)
    release = asyncio.Event()
    started: list[str] = []

    async def run_cron(*, job_id, **_kwargs) -> TurnResult:
        started.append(job_id)
        if job_id == "first":
            await release.wait()
        return _ok()

    svc.engine.run_cron = run_cron
    await _start(svc, [first, second])
    try:
        svc.scheduler.pause()  # only the watcher notices the pauses
        await _wait_for(lambda: svc._clock_sample is not None)

        def due_in(job_id: str, hours: float) -> datetime:
            due = datetime.fromtimestamp(clock.time(), UTC) + timedelta(hours=hours)
            svc.scheduler.modify_job(job_id, next_run_time=due)
            return due

        due_in("first", 1)
        due_in("second", 5)  # after the first pause
        clock.pause(3 * HOUR)
        await _wait_for(lambda: started == ["first"])

        due = due_in("second", 1)
        clock.pause(3 * HOUR)
        await _wait_for(
            lambda: svc.scheduler.get_job("second").next_run_time > due + timedelta(hours=2),
            timeout=2,
        )
        assert started == ["first"]  # the first catch-up is still running
    finally:
        release.set()
        await svc.stop()

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

    await _resume(svc, paused, resumed)

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

    await _resume(svc, paused, resumed)

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

    await _resume(svc, resumed - timedelta(hours=3), resumed)

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

    await _resume(svc, paused, resumed)

    svc.db.count_tasks.assert_awaited()
    svc.engine.run_cron.assert_not_awaited()
    svc.db.log_cron_start.assert_not_awaited()
    assert svc.scheduler.get_job("planner").next_run_time > resumed


def _first_run_waits_for(release: asyncio.Event, order: list[str]):
    """An ``engine.run_cron`` that records each run's start and end in *order*;
    the first run waits for *release*."""

    async def run_cron(**_kwargs) -> TurnResult:
        n = sum(1 for o in order if o.startswith("start")) + 1
        order.append(f"start {n}")
        if n == 1:
            await release.wait()
        order.append(f"end {n}")
        return _ok()

    return run_cron


def test_runs_in_flight_tracks_each_run_and_how_it_started():
    runs = cron_service._RunsInFlight()
    assert not runs.running("job")
    assert runs.started("job") == 0

    with runs.track("job", "schedule"):
        with runs.track("job", "catchup"):
            assert runs.running("job", "catchup")
        assert runs.running("job")
        assert runs.running("job", "schedule")
        assert not runs.running("job", "catchup")
        assert not runs.running("other")

    assert not runs.running("job")
    assert runs.started("job") == 2
    assert runs.started("other") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("lock", [False, True])
async def test_a_job_whose_run_is_still_going_is_not_caught_up(held_service, caplog, lock):
    """As the scheduler skips a fire that finds the job's previous run going."""
    job = CronJob(id="busy", schedule=EVERY_TEN_MINUTES, prompt="p", lock=lock)
    svc = await held_service([job])
    release = asyncio.Event()
    order: list[str] = []
    svc.engine.run_cron = _first_run_waits_for(release, order)
    # Started before the pause, and still going after it.
    in_flight = asyncio.create_task(svc._run_job_wrapper(job))
    await _wait_for(lambda: order == ["start 1"])
    paused, resumed = _pause_window()
    svc.scheduler.modify_job("busy", next_run_time=resumed - timedelta(hours=1))

    with caplog.at_level(logging.INFO, logger="nerve.cron.service"):
        catch_up = svc._on_clock_step(paused, resumed)

    assert catch_up is None
    assert "Not catching up busy: a run of it is still going" in caplog.text
    assert svc.scheduler.get_job("busy").next_run_time > resumed
    release.set()
    await in_flight
    await asyncio.sleep(0.05)
    assert order == ["start 1", "end 1"]


@pytest.mark.asyncio
async def test_a_scheduled_fire_is_skipped_while_its_catch_up_runs(held_service, caplog):
    job = CronJob(id="every-ten", schedule=EVERY_TEN_MINUTES, prompt="p")
    svc = await held_service([job])
    release = asyncio.Event()
    order: list[str] = []
    svc.engine.run_cron = _first_run_waits_for(release, order)
    paused, resumed = _pause_window()
    svc.scheduler.modify_job("every-ten", next_run_time=resumed - timedelta(hours=1))
    catch_up = svc._on_clock_step(paused, resumed)
    await _wait_for(lambda: order == ["start 1"])

    # The job's next fire comes while its catch-up run is still going.
    with caplog.at_level(logging.INFO, logger="nerve.cron.service"):
        svc.scheduler.modify_job("every-ten", next_run_time=datetime.now(UTC))
        svc.scheduler.resume()
        await _wait_for(
            lambda: "Skipping the scheduled run of every-ten: its catch-up run is "
            "still going" in caplog.text,
        )
    release.set()
    await catch_up
    await asyncio.sleep(0.05)

    assert order == ["start 1", "end 1"]


@pytest.mark.asyncio
async def test_a_manual_run_waits_for_a_locked_catch_up(held_service):
    job = CronJob(id="locked", schedule=EVERY_TEN_MINUTES, prompt="p", lock=True)
    svc = await held_service([job])
    release = asyncio.Event()
    order: list[str] = []
    svc.engine.run_cron = _first_run_waits_for(release, order)
    paused, resumed = _pause_window()
    svc.scheduler.modify_job("locked", next_run_time=resumed - timedelta(hours=1))
    catch_up = svc._on_clock_step(paused, resumed)
    await _wait_for(lambda: order == ["start 1"])

    manual = asyncio.create_task(svc.run_job("locked"))
    await asyncio.sleep(0.05)
    assert order == ["start 1"]
    release.set()
    await asyncio.gather(catch_up, manual)

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
    first_pause = svc._on_clock_step(paused, resumed)
    await _wait_for(lambda: order == ["first start"])

    paused_again, resumed_again = _pause_window(hours=1)
    svc.scheduler.modify_job(
        "second", next_run_time=resumed_again - timedelta(minutes=30),
    )
    second_pause = svc._on_clock_step(paused_again, resumed_again)
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

    after_pause = svc._on_clock_step(paused, resumed)
    await asyncio.sleep(0.05)
    assert order == ["overdue start"]
    release.set()
    await asyncio.gather(startup, after_pause)

    assert order == ["overdue start", "overdue end", "due start", "due end"]


@pytest.mark.asyncio
async def test_a_queued_catch_up_runs_each_job_as_it_is_now(held_service, caplog):
    """Jobs are looked up when the catch-up's turn comes, not when it queues."""
    edited = CronJob(id="edited", schedule=EVERY_TEN_MINUTES, prompt="old prompt")
    removed = CronJob(id="removed", schedule=EVERY_TEN_MINUTES, prompt="p")
    svc = await held_service([edited, removed])
    paused, resumed = _pause_window()
    for job_id in ("edited", "removed"):
        svc.scheduler.modify_job(job_id, next_run_time=resumed - timedelta(hours=1))
    await svc._catchup_lock.acquire()  # an earlier catch-up is still running
    catch_up = svc._on_clock_step(paused, resumed)

    # A reload while it waits: one job is rewritten, the other one goes.
    svc.scheduler.add_job(
        svc._run_job_wrapper, svc.scheduler.get_job("edited").trigger,
        args=[CronJob(id="edited", schedule=EVERY_TEN_MINUTES, prompt="new prompt")],
        id="edited", replace_existing=True,
    )
    svc.scheduler.remove_job("removed")
    with caplog.at_level(logging.INFO, logger="nerve.cron.service"):
        svc._catchup_lock.release()
        await catch_up

    ran = [(c.kwargs["job_id"], c.kwargs["prompt"]) for c in svc.engine.run_cron.await_args_list]
    assert ran == [("edited", "new prompt")]
    assert "Not catching up removed: it is no longer scheduled" in caplog.text


@pytest.mark.asyncio
async def test_a_job_that_ran_while_its_catch_up_waited_is_not_run_again(
    held_service, caplog,
):
    job = CronJob(id="every-ten", schedule=EVERY_TEN_MINUTES, prompt="p")
    svc = await held_service([job])
    paused, resumed = _pause_window()
    svc.scheduler.modify_job("every-ten", next_run_time=resumed - timedelta(hours=1))
    await svc._catchup_lock.acquire()  # an earlier catch-up is still running
    catch_up = svc._on_clock_step(paused, resumed)

    # Its next fire comes while the catch-up waits its turn.
    await svc._run_job_wrapper(job)
    with caplog.at_level(logging.INFO, logger="nerve.cron.service"):
        svc._catchup_lock.release()
        await catch_up

    svc.engine.run_cron.assert_awaited_once()
    assert "Not catching up every-ten: it has run since the pause" in caplog.text


@pytest.mark.asyncio
async def test_a_job_due_in_two_queued_pauses_runs_once(held_service):
    job = CronJob(id="every-ten", schedule=EVERY_TEN_MINUTES, prompt="p")
    svc = await held_service([job])
    await svc._catchup_lock.acquire()  # an earlier catch-up is still running
    queued = []
    for hours_ago in (2, 1):
        paused, resumed = _pause_window(hours=hours_ago)
        svc.scheduler.modify_job("every-ten", next_run_time=paused + timedelta(minutes=5))
        queued.append(svc._on_clock_step(paused, resumed))

    svc._catchup_lock.release()
    await asyncio.gather(*queued)

    svc.engine.run_cron.assert_awaited_once()


@pytest.mark.asyncio
async def test_one_info_line_per_pause(held_service, caplog):
    job = CronJob(id="every-ten", schedule=EVERY_TEN_MINUTES, prompt="p")
    svc = await held_service([job])
    paused, resumed = _pause_window()
    svc.scheduler.modify_job("every-ten", next_run_time=resumed - timedelta(hours=1))

    with caplog.at_level(logging.INFO, logger="nerve.cron.service"):
        await _resume(svc, paused, resumed, 3 * HOUR)

    lines = [
        r.getMessage() for r in caplog.records
        if r.getMessage().startswith("Clock jumped")
    ]
    assert len(lines) == 1
    assert "10800s ahead" in lines[0]
    assert paused.isoformat(timespec="seconds") in lines[0]
    assert resumed.isoformat(timespec="seconds") in lines[0]
    assert "catching up 1" in lines[0]


@pytest.mark.asyncio
async def test_a_job_that_cannot_be_moved_does_not_hold_up_the_rest(held_service, caplog):
    stuck = CronJob(id="stuck", schedule=EVERY_TEN_MINUTES, prompt="p")
    fine = CronJob(id="fine", schedule=EVERY_TEN_MINUTES, prompt="p")
    svc = await held_service([stuck, fine])
    paused, resumed = _pause_window()
    # "stuck" comes first: the scheduler lists jobs by their next run time.
    svc.scheduler.modify_job("stuck", next_run_time=resumed - timedelta(hours=2))
    svc.scheduler.modify_job("fine", next_run_time=resumed - timedelta(hours=1))
    modify_job = svc.scheduler.modify_job

    def refuse_stuck(job_id, **changes):
        if job_id == "stuck":
            raise RuntimeError("job store unavailable")
        return modify_job(job_id, **changes)

    with (
        caplog.at_level(logging.ERROR, logger="nerve.cron.service"),
        patch.object(svc.scheduler, "modify_job", side_effect=refuse_stuck),
    ):
        await _resume(svc, paused, resumed)

    assert "Could not move stuck past the pause" in caplog.text
    assert svc.scheduler.get_job("fine").next_run_time > resumed
    assert [c.kwargs["job_id"] for c in svc.engine.run_cron.await_args_list] == ["fine"]


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
    catch_ups: list[asyncio.Task | None] = []
    steps: list[float] = []
    on_clock_step = svc._on_clock_step

    def spy(paused_from, paused_to, step=None) -> asyncio.Task | None:
        steps.append(step)
        catch_ups.append(on_clock_step(paused_from, paused_to, step))
        return catch_ups[-1]

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
        await _wait_for(lambda: catch_ups)
        assert catch_ups[0] is not None
        await asyncio.wait_for(catch_ups[0], timeout=5)
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
