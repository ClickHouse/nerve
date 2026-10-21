"""CronService.on_source_delivered: drain at once, then run the gated jobs.

The scheduler runs paused, so nothing fires on its own; source runners are
stand-ins with the runner's lock semantics, and each job run is recorded
instead of reaching an agent.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from apscheduler.triggers.cron import CronTrigger

from nerve.config import NerveConfig
from nerve.cron.jobs import CronJob
from nerve.cron.service import CronService
from nerve.identity import attribution_for
from nerve.sources.github import GitHubSource
from nerve.sources.github_hosted import HostedGitHubSource
from nerve.sources.models import IngestResult
from tests.actor_rows import FAKE_SYSTEM_ACTOR, mock_system_principal

pytestmark = pytest.mark.asyncio

NEVER = "0 0 1 1 *"


class FakeRunner:
    """A source runner: one run at a time, and a run while busy returns at once."""

    def __init__(self, name: str = "github") -> None:
        self.source = SimpleNamespace(source_name=name)
        self.job_id = f"source:{name}"
        self.runs = 0
        self.hold: asyncio.Event | None = None
        self.started = asyncio.Event()
        self._lock = asyncio.Lock()

    async def run(self) -> IngestResult:
        if self._lock.locked():
            return IngestResult(records_ingested=0)
        async with self._lock:
            self.runs += 1
            self.started.set()
            if self.hold is not None:
                await self.hold.wait()
        return IngestResult(records_ingested=1)

    async def wait_until_idle(self) -> None:
        if self._lock.locked():
            async with self._lock:
                pass


class Runs:
    """Records job runs; a job id in ``hold`` blocks its runs until released."""

    def __init__(self) -> None:
        self.started: list[tuple[str, Any]] = []
        self.finished: list[str] = []
        self.hold: dict[str, asyncio.Event] = {}
        self.changed = asyncio.Event()

    async def __call__(self, job: CronJob, attribution: Any = None) -> None:
        self.started.append((job.id, attribution))
        self.changed.set()
        gate = self.hold.get(job.id)
        if gate is not None:
            await gate.wait()
        self.finished.append(job.id)
        self.changed.set()

    def ids(self) -> list[str]:
        return [job_id for job_id, _ in self.started]


async def eventually(predicate, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("timed out waiting for the condition")
        await asyncio.sleep(0.005)


def job(job_id: str, *, sources: list[str] | None = None, gated: bool = True, **fields: Any) -> CronJob:
    run_if = [{"type": "messages", **({"sources": sources} if sources is not None else {})}] if gated else []
    return CronJob(id=job_id, schedule=NEVER, prompt="do it", run_if=run_if, **fields)


@pytest_asyncio.fixture
async def svc(monkeypatch):
    config = MagicMock()
    config.timezone = "UTC"
    config.cron.delivery_debounce_seconds = 0
    db = AsyncMock()
    mock_system_principal(db)
    service = CronService(config, AsyncMock(), db)
    runs = Runs()
    monkeypatch.setattr(service, "_run_job_inner", runs)
    service.scheduler.start(paused=True)
    service.runs = runs
    try:
        yield service
    finally:
        if service.scheduler.running:
            await service.stop()


def install(service: CronService, *jobs: CronJob, runners: list[FakeRunner] | None = None) -> None:
    service._jobs = list(jobs)
    for item in jobs:
        if item.enabled:
            service.scheduler.add_job(
                service._run_job_wrapper, CronTrigger.from_crontab(NEVER), args=[item], id=item.id,
            )
    service._source_runners = runners if runners is not None else [FakeRunner()]


class TestDrain:
    async def test_the_runner_drains_at_once(self, svc):
        runner = FakeRunner()
        install(svc, runners=[runner])

        await svc.on_source_delivered("github")

        assert runner.runs == 1
        svc.db.log_cron_start.assert_awaited()
        assert svc.db.log_cron_start.await_args.args[0] == "source:github"

    async def test_deliveries_during_a_drain_make_it_run_once_more(self, svc):
        runner = FakeRunner()
        runner.hold = asyncio.Event()
        install(svc, runners=[runner])

        first = asyncio.create_task(svc.on_source_delivered("github"))
        await runner.started.wait()
        for _ in range(3):
            await svc.on_source_delivered("github")  # returns at once
        runner.hold.set()
        await first

        assert runner.runs == 2

    async def test_a_run_in_progress_is_waited_for(self, svc):
        runner = FakeRunner()
        runner.hold = asyncio.Event()
        install(svc, runners=[runner])
        scheduled = asyncio.create_task(runner.run())
        await runner.started.wait()

        delivered = asyncio.create_task(svc.on_source_delivered("github"))
        await asyncio.sleep(0.05)
        assert not delivered.done()
        runner.hold.set()
        await asyncio.gather(scheduled, delivered)

        assert runner.runs == 2

    async def test_a_source_without_a_runner_runs_nothing(self, svc):
        install(svc, job("pr-monitor", sources=["github"]), runners=[FakeRunner("gmail:me@example.net")])

        await svc.on_source_delivered("github")
        await asyncio.sleep(0.05)

        assert svc.runs.started == []
        assert svc._delivery_tasks == {}

    async def test_a_stopped_service_runs_nothing(self, svc):
        runner = FakeRunner()
        install(svc, job("pr-monitor", sources=["github"]), runners=[runner])
        await svc.stop()
        await asyncio.sleep(0)  # the scheduler applies its shutdown on the loop

        await svc.on_source_delivered("github")

        assert runner.runs == 0


class TestJobs:
    async def test_only_jobs_gated_on_the_source_or_on_any_source_run(self, svc):
        unscheduled = job("broken-schedule", sources=["github"])
        install(
            svc,
            job("pr-monitor", sources=["github", "gmail"]),
            job("inbox", sources=None),
            job("mail", sources=["gmail"]),
            job("briefing", gated=False),
            job("paused", sources=["github"], enabled=False),
        )
        svc._jobs.append(unscheduled)

        await svc.on_source_delivered("github")

        await eventually(lambda: len(svc.runs.finished) == 2)
        await asyncio.sleep(0.05)
        assert sorted(svc.runs.ids()) == ["inbox", "pr-monitor"]
        assert all(attribution is None for _, attribution in svc.runs.started)

    async def test_a_burst_within_the_debounce_window_is_one_run(self, svc):
        svc.config.cron.delivery_debounce_seconds = 1
        install(svc, job("pr-monitor", sources=["github"]))

        for _ in range(3):
            await svc.on_source_delivered("github")
            await asyncio.sleep(0.1)
        assert svc.runs.started == []

        await eventually(lambda: svc.runs.finished == ["pr-monitor"])
        await asyncio.sleep(0.2)
        assert svc.runs.ids() == ["pr-monitor"]

    async def test_a_delivery_after_the_run_started_gets_a_run_of_its_own(self, svc):
        install(svc, job("pr-monitor", sources=["github"]))
        svc.runs.hold["pr-monitor"] = asyncio.Event()

        await svc.on_source_delivered("github")
        await eventually(lambda: svc.runs.ids() == ["pr-monitor"])
        await svc.on_source_delivered("github")
        assert "pr-monitor" in svc._delivery_tasks
        svc.runs.hold["pr-monitor"].set()

        await eventually(lambda: svc.runs.finished == ["pr-monitor", "pr-monitor"])

    async def test_the_delivery_run_waits_for_a_run_in_flight(self, svc):
        monitor = job("pr-monitor", sources=["github"])
        install(svc, monitor)
        svc.runs.hold["pr-monitor"] = asyncio.Event()
        scheduled = asyncio.create_task(svc._run_job_wrapper(monitor))
        await eventually(lambda: svc.runs.ids() == ["pr-monitor"])

        await svc.on_source_delivered("github")
        await asyncio.sleep(0.05)
        assert len(svc.runs.started) == 1
        svc.runs.hold["pr-monitor"].set()
        await scheduled

        await eventually(lambda: svc.runs.finished == ["pr-monitor", "pr-monitor"])

    async def test_a_fire_of_its_own_is_skipped_while_a_delivery_run_goes_on(self, svc):
        monitor = job("pr-monitor", sources=["github"])
        install(svc, monitor)
        svc.runs.hold["pr-monitor"] = asyncio.Event()
        await svc.on_source_delivered("github")
        await eventually(lambda: svc.runs.ids() == ["pr-monitor"])

        await svc._run_job_wrapper(monitor)
        assert svc.runs.ids() == ["pr-monitor"]

        person = attribution_for(FAKE_SYSTEM_ACTOR)
        manual = asyncio.create_task(svc._run_job_wrapper(monitor, person))
        await eventually(lambda: len(svc.runs.started) == 2)
        svc.runs.hold["pr-monitor"].set()
        await manual
        await eventually(lambda: len(svc.runs.finished) == 2)
        assert svc.runs.started[1] == ("pr-monitor", person)

    async def test_a_locked_job_waits_for_the_delivery_run_instead(self, svc):
        monitor = job("pr-monitor", sources=["github"], lock=True)
        install(svc, monitor)
        svc.runs.hold["pr-monitor"] = asyncio.Event()
        await svc.on_source_delivered("github")
        await eventually(lambda: svc.runs.ids() == ["pr-monitor"])

        scheduled = asyncio.create_task(svc._run_job_wrapper(monitor))
        await asyncio.sleep(0.05)
        assert len(svc.runs.started) == 1
        svc.runs.hold["pr-monitor"].set()
        await scheduled

        assert svc.runs.finished == ["pr-monitor", "pr-monitor"]

    async def test_a_job_removed_by_a_reload_before_its_run_is_not_run(self, svc):
        svc.config.cron.delivery_debounce_seconds = 1
        install(svc, job("pr-monitor", sources=["github"]))
        await svc.on_source_delivered("github")

        svc._jobs = []
        await asyncio.sleep(1.2)

        assert svc.runs.started == []
        assert svc._delivery_tasks == {}

    async def test_stop_cancels_the_pending_runs(self, svc):
        svc.config.cron.delivery_debounce_seconds = 600
        install(svc, job("pr-monitor", sources=["github"]))
        await svc.on_source_delivered("github")
        task = svc._delivery_tasks["pr-monitor"]

        await svc.stop()
        await asyncio.sleep(0)

        assert task.cancelled()
        assert svc.runs.started == []

    async def test_schedule_listeners_hear_of_the_drain_and_the_run(self, svc):
        install(svc, job("pr-monitor", sources=["github"]))
        heard: list[int] = []
        svc.add_schedule_listener(lambda: heard.append(1))

        await svc.on_source_delivered("github")
        await eventually(lambda: svc.runs.finished == ["pr-monitor"])

        assert len(heard) >= 2

    async def test_a_debounce_that_is_not_a_number_falls_back_to_the_default(self, svc):
        svc.config.cron.delivery_debounce_seconds = "soon"

        assert svc._delivery_debounce() == 60.0


async def test_source_reloads_keep_the_github_mode_the_service_started_with():
    config = NerveConfig()
    config.sync.github.mode = "hosted"
    service = CronService(config, AsyncMock(), MagicMock())
    service.scheduler.start(paused=True)
    try:
        service._register_source_runners()
        config.sync.github.mode = "poll"

        await service.reload_sources()

        (runner,) = [r for r in service._source_runners if r.job_id == "source:github"]
        assert isinstance(runner.source, HostedGitHubSource)
    finally:
        await service.stop()


async def test_a_service_started_in_poll_mode_keeps_polling():
    config = NerveConfig()
    service = CronService(config, AsyncMock(), MagicMock())
    service.scheduler.start(paused=True)
    try:
        config.sync.github.mode = "hosted"

        await service.reload_sources()

        (runner,) = [r for r in service._source_runners if r.job_id == "source:github"]
        assert isinstance(runner.source, GitHubSource)
    finally:
        await service.stop()
