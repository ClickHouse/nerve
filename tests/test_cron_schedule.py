"""Gate forecasts and the next scheduled tick: what the instance will do on its
own next, judged from what is known now."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import pytest_asyncio
from click.testing import CliRunner
from fastapi import FastAPI

from nerve.agent.engine import TurnOutcome, TurnResult
from nerve.cron import gates as gates_module
from nerve.cron import service as cron_service
from nerve.cron.gates import (
    CronGate,
    GateContext,
    GitHubPrActivityGate,
    MessagesGate,
    TasksGate,
    forecast_gates,
)
from nerve.cron.jobs import CronJob
from nerve.cron.service import CronService, ScheduledTick
from nerve.sources.channel import ChannelSource
from nerve.sources.github import GitHubSource
from nerve.sources.github_hosted import HostedGitHubSource
from nerve.sources.github_repos import GitHubReposSource
from nerve.sources.models import IngestResult, SourceRecord
from tests.actor_rows import mock_system_principal

IN_AN_HOUR = timedelta(hours=1)


def _ctx(db, job_id: str = "job") -> GateContext:
    return GateContext(job_id=job_id, db=db)


@pytest.fixture(autouse=True)
def _no_forecast_failure_warned_yet(monkeypatch):
    monkeypatch.setattr(gates_module, "_forecast_warned", {})


# ---------------------------------------------------------------------------
# Gate forecasts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 1, 5])
async def test_the_default_forecast_is_the_gate_evaluated_now(count):
    db = AsyncMock()
    db.count_tasks = AsyncMock(return_value=count)
    gate = TasksGate(targets=["pending"], min_count=2)
    later = datetime.now(UTC) + timedelta(days=2)

    assert await gate.forecast(_ctx(db), later) == await gate.is_satisfied(_ctx(db))


@pytest.mark.asyncio
@pytest.mark.parametrize("unread", [True, False])
async def test_a_messages_gate_over_any_source_forecasts_its_answer(unread):
    db = AsyncMock()
    db.consumer_has_unread = AsyncMock(return_value=unread)
    gate = MessagesGate(consumer="inbox")

    assert await gate.forecast(_ctx(db), datetime.now(UTC)) is unread
    assert await gate.is_satisfied(_ctx(db)) is unread


def _record(record_id: str) -> SourceRecord:
    return SourceRecord(
        id=record_id, source="mail", record_type="note", summary=f"note {record_id}",
        content="hello", timestamp="2026-01-01T00:00:00Z", metadata={},
    )


@pytest.mark.asyncio
async def test_a_messages_gate_forecast_never_starts_a_cursor(db):
    """The check starts a missing cursor at the newest message, so that source
    has nothing unread; the forecast gives the same answer without the write."""
    await db.insert_source_messages([_record("m1")], source="mail")
    gate = MessagesGate(sources=["mail"], consumer="inbox")

    assert await gate.forecast(_ctx(db), datetime.now(UTC)) is False
    assert await db.peek_consumer_cursor("inbox", "mail") is None
    assert await gate.is_satisfied(_ctx(db)) is False
    assert await db.peek_consumer_cursor("inbox", "mail") == (
        await db.get_source_max_rowid("mail")
    )


@pytest.mark.asyncio
async def test_a_messages_gate_forecast_sees_unread_messages(db):
    await db.insert_source_messages([_record("m1")], source="mail")
    await db.set_consumer_cursor("inbox", "mail", 0)
    gate = MessagesGate(sources=["mail"], consumer="inbox")

    assert await gate.forecast(_ctx(db), datetime.now(UTC)) is True
    assert await gate.is_satisfied(_ctx(db)) is True


@pytest.mark.asyncio
async def test_a_messages_gate_forecast_sees_its_cursor_expire_before_the_run(db):
    """When the run comes, an expired cursor starts again at the newest message,
    so the check finds nothing unread there."""
    await db.insert_source_messages([_record("m1")], source="mail")
    await db.set_consumer_cursor("inbox", "mail", 0, ttl_days=1)
    gate = MessagesGate(sources=["mail"], consumer="inbox")
    now = datetime.now(UTC)

    assert await gate.forecast(_ctx(db), now + timedelta(hours=3)) is True
    assert await gate.forecast(_ctx(db), now + timedelta(days=2)) is False
    assert await db.peek_consumer_cursor("inbox", "mail") == 0  # still live now


@pytest.mark.asyncio
async def test_the_github_gate_forecasts_a_run_without_polling(tmp_path, monkeypatch):
    spawned = AsyncMock(side_effect=AssertionError("no process may be started"))
    monkeypatch.setattr(gates_module.asyncio, "create_subprocess_exec", spawned)
    gate = GitHubPrActivityGate(author="example-bot")
    gate._save_state("watched", "fingerprint", datetime.now(UTC) - timedelta(hours=1))
    state = gate._state_path("watched")
    before = (state.read_bytes(), state.stat().st_mtime_ns)

    assert await gate.forecast(_ctx(AsyncMock(), "watched"), datetime.now(UTC)) is True
    assert await gate.forecast(_ctx(AsyncMock(), "never-run"), datetime.now(UTC)) is True

    spawned.assert_not_called()
    assert (state.read_bytes(), state.stat().st_mtime_ns) == before
    assert not state.with_name("pr_activity_never-run.json").exists()


class _Gate(CronGate):
    """A gate whose forecast and check give fixed answers."""

    type = "fixed"

    def __init__(self, forecast=True, now=True, raises=False):
        self.answer, self.now, self.raises = forecast, now, raises
        self.asked_at: list[datetime] = []

    async def is_satisfied(self, ctx):
        return self.now

    async def forecast(self, ctx, at):
        self.asked_at.append(at)
        if self.raises:
            raise RuntimeError("boom")
        return self.answer

    def describe(self):
        return f"fixed {self.answer}"

    @classmethod
    def from_config(cls, _config):
        return cls()


@pytest.mark.asyncio
async def test_forecast_gates_asks_each_gate_about_the_given_time():
    at = datetime.now(UTC) + IN_AN_HOUR
    first, second = _Gate(), _Gate(forecast=False, now=True)

    decision = await forecast_gates([first, second], _ctx(AsyncMock()), at)

    assert decision.should_run is False
    assert decision.reason == "gate 'fixed' not satisfied (fixed False)"
    assert first.asked_at == second.asked_at == [at]


@pytest.mark.asyncio
async def test_forecast_gates_fails_open_and_says_so(caplog):
    with caplog.at_level(logging.WARNING, logger="nerve.cron.gates"):
        decision = await forecast_gates(
            [_Gate(raises=True), _Gate()], _ctx(AsyncMock(), "flaky"),
            datetime.now(UTC),
        )

    assert decision.should_run is True
    assert any("flaky" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_a_failing_forecast_is_warned_about_once_per_job_in_ten_minutes(caplog):
    async def read(job_id: str) -> None:
        await forecast_gates(
            [_Gate(raises=True)], _ctx(AsyncMock(), job_id), datetime.now(UTC),
        )

    def warned() -> list[str]:
        return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]

    with caplog.at_level(logging.DEBUG, logger="nerve.cron.gates"):
        for _ in range(5):
            await read("flaky")
        assert len(warned()) == 1
        assert sum(r.levelno == logging.DEBUG for r in caplog.records) == 4

        await read("other")  # each job is warned about on its own
        assert len(warned()) == 2

        gates_module._forecast_warned["flaky"] -= 600  # ten minutes on
        await read("flaky")
        assert len(warned()) == 3


_PLUGIN = '''
from nerve.cron.gates import CronGate


class {name}(CronGate):
    type = "{type}"

    async def is_satisfied(self, ctx):
        return True
{forecast}
    def describe(self):
        return "{type}"

    @classmethod
    def from_config(cls, _config):
        return cls()
'''

_OWN_FORECAST = '''
    async def forecast(self, ctx, at):
        return True
'''


def test_a_plugin_gate_on_the_default_forecast_is_warned_about(
    tmp_path, clean_registry, caplog,
):
    from nerve.cron.gate_plugins import load_gate_plugins

    (tmp_path / "checks.py").write_text(
        _PLUGIN.format(name="ChecksGate", type="checks_now", forecast=""),
    )
    (tmp_path / "forecasts.py").write_text(
        _PLUGIN.format(name="ForecastsGate", type="forecasts", forecast=_OWN_FORECAST),
    )

    with caplog.at_level(logging.WARNING, logger="nerve.cron.gate_plugins"):
        assert load_gate_plugins(tmp_path) == 2

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "'checks_now'" in warnings[0]
    assert "every schedule read" in warnings[0]


# ---------------------------------------------------------------------------
# The next scheduled tick
# ---------------------------------------------------------------------------


def _mock_db(pending_wakeups=()) -> AsyncMock:
    db = AsyncMock()
    db.log_cron_start = AsyncMock(return_value=1)
    db.get_last_successful_cron_run = AsyncMock(return_value=None)
    db.list_pending_wakeups = AsyncMock(return_value=list(pending_wakeups))
    db.count_tasks = AsyncMock(return_value=0)
    db.get_sync_cursor = AsyncMock(return_value=None)
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


def _runner(name: str = "demo") -> SimpleNamespace:
    return SimpleNamespace(
        source=SimpleNamespace(source_name=name),
        job_id=f"source:{name}",
        schedule="*/15 * * * *",
        run=AsyncMock(return_value=IngestResult(records_ingested=1)),
        set_notification_service=MagicMock(),
    )


async def _start(svc: CronService, jobs=(), runners=()) -> None:
    def register() -> None:
        svc._source_runners = list(runners)
        svc._schedule_source_runners(svc._source_runners)

    with (
        patch.object(svc, "_load_merged_jobs", return_value=list(jobs)),
        patch.object(svc, "_register_source_runners", side_effect=register),
        patch.object(svc, "_catchup_missed_jobs", AsyncMock()),
    ):
        await svc.start()


@pytest_asyncio.fixture
async def held(tmp_path, monkeypatch):
    """Factory: a started service, its scheduler held so a test sets each
    job's next run time itself, its clock watcher idle."""
    monkeypatch.setattr(cron_service, "_CLOCK_WATCH_SECONDS", 3600)
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


def _job(job_id: str, **kwargs) -> CronJob:
    return CronJob(id=job_id, schedule="0 * * * *", prompt="p", **kwargs)


def _due(svc: CronService, job_id: str, at: datetime) -> None:
    svc.scheduler.modify_job(job_id, next_run_time=at)


PENDING = [{"type": "tasks", "status": "pending"}]


@pytest.mark.asyncio
async def test_housekeeping_is_never_the_next_tick(held):
    svc = await held()

    assert svc.scheduler.get_job("cleanup") is not None
    assert svc.scheduler.get_job("wakeup_sweep") is not None
    assert await svc.next_scheduled() is None


@pytest.mark.asyncio
async def test_a_job_whose_gate_forecasts_a_skip_is_passed_over(held):
    now = datetime.now(UTC)
    svc = await held([_job("planner", run_if=PENDING), _job("digest")])
    _due(svc, "planner", now + timedelta(minutes=10))
    _due(svc, "digest", now + timedelta(minutes=40))

    tick = await svc.next_scheduled(now=now)
    assert tick == ScheduledTick(
        at=now + timedelta(minutes=40), name="digest", kind="cron",
    )

    svc.db.count_tasks.return_value = 1  # a pending task appears
    tick = await svc.next_scheduled(now=now)
    assert (tick.name, tick.at) == ("planner", now + timedelta(minutes=10))


@pytest.mark.asyncio
async def test_a_source_runner_counts(held):
    now = datetime.now(UTC)
    svc = await held([_job("digest")], [_runner("mail")])
    _due(svc, "digest", now + timedelta(minutes=30))
    _due(svc, "source:mail", now + timedelta(minutes=5))

    tick = await svc.next_scheduled(now=now)

    assert tick == ScheduledTick(
        at=now + timedelta(minutes=5), name="source:mail", kind="source",
    )


@pytest.mark.asyncio
async def test_a_source_that_runs_on_delivery_is_not_counted_with_nothing_waiting(held):
    """The hosted GitHub source reads a buffer that fills when records are
    delivered, and it runs then. With nothing waiting in the buffer its
    scheduled run is not the next scheduled task, and a platform that parks the
    machine is not told to wake it for that run."""
    now = datetime.now(UTC)
    drain = _runner("github")
    drain.source.push_fed = True
    svc = await held([_job("digest")], [_runner("mail"), drain])
    _due(svc, "digest", now + timedelta(minutes=30))
    _due(svc, "source:mail", now + timedelta(minutes=10))
    _due(svc, "source:github", now + timedelta(minutes=5))

    tick = await svc.next_scheduled(now=now)

    assert tick == ScheduledTick(
        at=now + timedelta(minutes=10), name="source:mail", kind="source",
    )
    by_id = {j["id"]: j for j in (await svc.schedule_report())["jobs"]}
    assert by_id["source:github"]["forecast"] == "skip"
    assert by_id["source:github"]["reason"] == cron_service._PUSH_FED_REASON
    assert by_id["source:github"]["next_run"] == (now + timedelta(minutes=5)).isoformat()
    assert by_id["source:mail"]["forecast"] == "run"


@pytest.mark.asyncio
async def test_a_source_that_runs_on_delivery_alone_schedules_nothing(held):
    now = datetime.now(UTC)
    drain = _runner("github")
    drain.source.push_fed = True
    svc = await held([], [drain])
    _due(svc, "source:github", now + timedelta(minutes=5))

    assert await svc.next_scheduled(now=now) is None


def _channel_drain(rows=()) -> SimpleNamespace:
    """A ``slack:observed`` runner over a real ChannelSource whose buffer holds *rows*."""
    db = AsyncMock()
    db.read_channel_observations = AsyncMock(return_value=list(rows))
    drain = _runner("slack:observed")
    drain.source = ChannelSource("slack", db)
    return drain


@pytest.mark.asyncio
async def test_a_channel_drain_with_nothing_waiting_is_never_the_next_tick(held):
    """Only the running instance writes a channel drain's buffer, so nothing is
    added to it while the machine is parked. With nothing waiting, the drain is
    not the next scheduled task, and a platform that parks the machine is not
    kept up, or woken, for each scheduled drain. A pull source and a cron job
    still count."""
    now = datetime.now(UTC)
    drain = _channel_drain()
    db = _mock_db()
    db.get_sync_cursor = AsyncMock(return_value="3")
    svc = await held([_job("digest")], [_runner("mail"), drain], db=db)
    _due(svc, "digest", now + timedelta(minutes=30))
    _due(svc, "source:mail", now + timedelta(minutes=10))
    _due(svc, "source:slack:observed", now + timedelta(minutes=5))

    tick = await svc.next_scheduled(now=now)

    assert tick == ScheduledTick(
        at=now + timedelta(minutes=10), name="source:mail", kind="source",
    )
    by_id = {j["id"]: j for j in (await svc.schedule_report())["jobs"]}
    assert by_id["source:slack:observed"]["forecast"] == "skip"
    assert by_id["source:slack:observed"]["reason"] == cron_service._PUSH_FED_REASON
    assert by_id["source:slack:observed"]["next_run"] == (now + timedelta(minutes=5)).isoformat()
    db.get_sync_cursor.assert_awaited_with("slack:observed")
    drain.source._db.read_channel_observations.assert_awaited_with("slack", after_id=3, limit=1)


@pytest.mark.asyncio
async def test_a_channel_drain_with_messages_waiting_counts(held):
    """A message buffered since the last drain is work the next drain will do,
    so that drain counts, and the machine is kept up, or woken, to take it."""
    now = datetime.now(UTC)
    drain = _channel_drain([(4, {"text": "hi"})])
    svc = await held([_job("digest")], [_runner("mail"), drain])
    _due(svc, "digest", now + timedelta(minutes=30))
    _due(svc, "source:mail", now + timedelta(minutes=10))
    _due(svc, "source:slack:observed", now + timedelta(minutes=5))

    tick = await svc.next_scheduled(now=now)

    assert tick == ScheduledTick(
        at=now + timedelta(minutes=5), name="source:slack:observed", kind="source",
    )
    by_id = {j["id"]: j for j in (await svc.schedule_report())["jobs"]}
    assert by_id["source:slack:observed"]["forecast"] == "run"


@pytest.mark.asyncio
async def test_a_buffer_check_that_fails_leaves_the_drain_uncounted(held):
    now = datetime.now(UTC)
    drain = _channel_drain()
    drain.source._db.read_channel_observations = AsyncMock(side_effect=RuntimeError("db"))
    svc = await held([], [drain])
    _due(svc, "source:slack:observed", now + timedelta(minutes=5))

    assert await svc.next_scheduled(now=now) is None


@pytest.mark.asyncio
async def test_a_cursor_that_cannot_be_read_leaves_the_drain_uncounted(held):
    """The forecast never fails for the buffer check: a cursor read that fails
    answers no, even with a message waiting."""
    now = datetime.now(UTC)
    drain = _channel_drain([(4, {})])
    db = _mock_db()
    db.get_sync_cursor = AsyncMock(side_effect=RuntimeError("db"))
    svc = await held([], [drain], db=db)
    _due(svc, "source:slack:observed", now + timedelta(minutes=5))

    assert await svc.next_scheduled(now=now) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("make", "channel"),
    [
        (lambda db: ChannelSource("slack", db), "slack"),
        (HostedGitHubSource, "github"),
        (lambda db: HostedGitHubSource(db, "github_repos"), "github_repos"),
    ],
)
async def test_push_fed_sources_look_past_their_cursor_for_waiting_records(make, channel):
    db = AsyncMock()
    db.read_channel_observations = AsyncMock(return_value=[(8, {})])
    source = make(db)

    assert await source.has_undrained("7") is True
    db.read_channel_observations.assert_awaited_once_with(channel, after_id=7, limit=1)

    db.read_channel_observations = AsyncMock(return_value=[])
    assert await source.has_undrained(None) is False
    db.read_channel_observations.assert_awaited_once_with(channel, after_id=0, limit=1)


@pytest.mark.parametrize(
    ("source", "fed"),
    [
        (HostedGitHubSource, True), (HostedGitHubSource(AsyncMock(), "github_repos"), True),
        (ChannelSource, True), (GitHubSource, False), (GitHubReposSource, False),
    ],
)
def test_the_sources_say_whether_deliveries_feed_them(source, fed):
    assert source.push_fed is fed


@pytest.mark.asyncio
async def test_the_earliest_pending_wakeup_counts(held):
    now = datetime.now(UTC)
    db = _mock_db(pending_wakeups=[
        {"session_id": "9f8e7d6c5b4a-later", "fire_at": (now + timedelta(minutes=9)).isoformat()},
        {"session_id": "0a1b2c3d4e5f-sooner", "fire_at": (now + timedelta(minutes=3)).isoformat()},
    ])
    svc = await held([_job("digest")], db=db)
    _due(svc, "digest", now + timedelta(minutes=30))

    tick = await svc.next_scheduled(now=now)

    assert tick == ScheduledTick(
        at=now + timedelta(minutes=3), name="wakeup:0a1b2c3d", kind="wakeup",
    )


@pytest.mark.asyncio
async def test_nothing_beyond_the_horizon_counts(held):
    now = datetime.now(UTC)
    db = _mock_db(pending_wakeups=[
        {"session_id": "0a1b2c3d4e5f", "fire_at": (now + timedelta(days=8)).isoformat()},
    ])
    svc = await held([_job("monthly")], db=db)
    _due(svc, "monthly", now + timedelta(days=8, hours=1))

    assert await svc.next_scheduled(now=now) is None
    tick = await svc.next_scheduled(now=now, horizon=timedelta(days=9))
    assert tick.name == "wakeup:0a1b2c3d"


@pytest.mark.asyncio
async def test_now_must_be_timezone_aware(held):
    svc = await held([_job("digest")])
    # Naive on purpose: a time without a zone is what must be refused.
    naive = datetime.now(UTC).replace(tzinfo=None)

    with pytest.raises(ValueError, match="timezone-aware"):
        await svc.next_scheduled(now=naive)


@pytest.mark.asyncio
async def test_a_wakeup_due_but_not_yet_fired_keeps_its_due_time(held):
    now = datetime.now(UTC)
    db = _mock_db(pending_wakeups=[
        {"session_id": "0a1b2c3d4e5f", "fire_at": (now - timedelta(seconds=15)).isoformat()},
    ])
    svc = await held(db=db)

    tick = await svc.next_scheduled(now=now)

    assert tick.at == now - timedelta(seconds=15)


@pytest.mark.asyncio
async def test_names_are_safe_to_report(held):
    now = datetime.now(UTC)
    long_id = "a-very-long-job-name-" * 5
    svc = await held(
        [_job("weekly report @home"), _job(long_id)], [_runner("mail:me@example.org")],
    )
    _due(svc, "weekly report @home", now + timedelta(minutes=1))
    _due(svc, long_id, now + timedelta(minutes=2))
    _due(svc, "source:mail:me@example.org", now + timedelta(minutes=3))

    first = await svc.next_scheduled(now=now)
    assert first.name == "weekly_report__home"
    svc.scheduler.remove_job("weekly report @home")
    second = await svc.next_scheduled(now=now)
    assert second.name == long_id[:64] and len(second.name) == 64
    svc.scheduler.remove_job(long_id)
    third = await svc.next_scheduled(now=now)
    assert third.name == "source:mail:me_example.org"


@pytest.mark.asyncio
async def test_the_schedule_report(held):
    now = datetime.now(UTC)
    svc = await held(
        [_job("planner", run_if=PENDING), _job("digest"), _job("off", enabled=False)],
        [_runner("mail")],
    )
    _due(svc, "planner", now + timedelta(minutes=10))
    _due(svc, "digest", now + timedelta(minutes=40))
    _due(svc, "source:mail", now + timedelta(minutes=50))

    report = await svc.schedule_report()

    assert report["next"] == {
        "at": (now + timedelta(minutes=40)).isoformat(),
        "name": "digest",
        "kind": "cron",
    }
    by_id = {j["id"]: j for j in report["jobs"]}
    assert list(by_id) == ["planner", "digest", "off", "source:mail"]
    assert by_id["planner"]["forecast"] == "skip"
    assert by_id["planner"]["reason"].startswith("gate 'tasks' not satisfied")
    assert by_id["planner"]["next_run"] == (now + timedelta(minutes=10)).isoformat()
    assert (by_id["digest"]["forecast"], by_id["digest"]["reason"]) == ("run", None)
    assert by_id["off"] == {"id": "off", "next_run": None, "forecast": None, "reason": None}
    assert by_id["source:mail"]["forecast"] == "run"


class _OpensAt(CronGate):
    """A gate that reasons about the time: runs may go ahead from ``opens`` on."""

    type = "opens_at"

    def __init__(self) -> None:
        self.opens = datetime.max.replace(tzinfo=UTC)
        self.asked_at: list[datetime] = []

    async def is_satisfied(self, ctx):
        return datetime.now(UTC) >= self.opens

    async def forecast(self, ctx, at):
        self.asked_at.append(at)
        return at >= self.opens

    def describe(self):
        return "opens later"

    @classmethod
    def from_config(cls, _config):
        return cls()


class _ChecksNow(CronGate):
    """A gate on the default forecast that counts how often its check runs."""

    type = "checks_now"

    def __init__(self, answer: bool) -> None:
        self.answer, self.checks = answer, 0

    async def is_satisfied(self, ctx):
        self.checks += 1
        return self.answer

    def describe(self):
        return "checks now"

    @classmethod
    def from_config(cls, _config):
        return cls(True)


async def _hourly_job_with(held, *gates) -> tuple[CronService, datetime]:
    """A started service whose one job runs hourly behind *gates*, and that
    job's next run."""
    job = _job("hourly")
    svc = await held([job])
    job.gates = list(gates)
    return svc, svc.scheduler.get_job("hourly").next_run_time


@pytest.mark.asyncio
async def test_a_later_run_a_gate_will_let_through_is_the_next_tick(held):
    opens = _OpensAt()
    svc, first = await _hourly_job_with(held, opens)
    opens.opens = first + timedelta(hours=2)

    tick = await svc.next_scheduled()

    assert tick == ScheduledTick(at=first + timedelta(hours=2), name="hourly", kind="cron")
    assert opens.asked_at == [first + timedelta(hours=n) for n in range(3)]
    # The job's next run itself is still forecast to skip.
    [job] = (await svc.schedule_report())["jobs"]
    assert job["next_run"] == first.isoformat()
    assert job["forecast"] == "skip"


@pytest.mark.asyncio
@pytest.mark.parametrize(("opens_after_hours", "found"), [(8, True), (9, False)])
async def test_eight_more_runs_are_tried_at_most(held, opens_after_hours, found):
    opens = _OpensAt()
    svc, first = await _hourly_job_with(held, opens)
    opens.opens = first + timedelta(hours=opens_after_hours)

    tick = await svc.next_scheduled()

    assert (tick is not None) is found
    assert len(opens.asked_at) == 9


@pytest.mark.asyncio
async def test_no_run_beyond_the_horizon_is_tried(held):
    opens = _OpensAt()
    svc, first = await _hourly_job_with(held, opens)
    opens.opens = first + timedelta(hours=2)

    tick = await svc.next_scheduled(
        now=first - timedelta(minutes=30), horizon=timedelta(hours=2),
    )

    assert tick is None
    assert opens.asked_at == [first, first + timedelta(hours=1)]


@pytest.mark.asyncio
async def test_a_gate_on_the_default_forecast_has_its_check_run_once(held):
    checks, opens = _ChecksNow(answer=True), _OpensAt()
    svc, first = await _hourly_job_with(held, checks, opens)
    opens.opens = first + timedelta(hours=3)

    tick = await svc.next_scheduled()

    assert tick.at == first + timedelta(hours=3)
    assert checks.checks == 1


@pytest.mark.asyncio
async def test_no_later_run_is_tried_when_a_default_forecast_says_no(held):
    opens, checks = _OpensAt(), _ChecksNow(answer=False)
    svc, first = await _hourly_job_with(held, opens, checks)
    opens.opens = first

    assert await svc.next_scheduled() is None
    assert checks.checks == 1
    assert len(opens.asked_at) <= 1


# ---------------------------------------------------------------------------
# Schedule listeners
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_listeners_hear_start_reload_and_sources_reload(tmp_path, monkeypatch):
    monkeypatch.setattr(cron_service, "_CLOCK_WATCH_SECONDS", 3600)
    svc = _service(tmp_path)
    heard = MagicMock()
    svc.add_schedule_listener(heard)
    await _start(svc, [_job("digest")])
    try:
        assert heard.call_count == 1
        with patch.object(svc, "_load_merged_jobs", return_value=[_job("digest")]):
            await svc.reload()
        assert heard.call_count == 2
        with patch("nerve.sources.registry.build_source_runners", return_value=[]):
            await svc.reload_sources()
        assert heard.call_count == 3
    finally:
        await svc.stop()


@pytest.mark.asyncio
async def test_listeners_hear_every_run_including_a_gate_skip(held):
    runner = _runner("mail")
    svc = await held([_job("digest"), _job("planner", run_if=PENDING)], [runner])
    heard = MagicMock()
    svc.add_schedule_listener(heard)

    await svc._run_job_wrapper(svc._jobs[0])
    assert heard.call_count == 1
    await svc._run_job_wrapper(svc._jobs[1])  # skipped by its gate
    svc.engine.run_cron.assert_awaited_once()
    assert heard.call_count == 2
    await svc._run_source_wrapper(runner)
    assert heard.call_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["job", "source"])
async def test_listeners_hear_a_scheduled_fire_skipped_for_its_catch_up(held, kind):
    """A fire that comes while the job's catch-up still runs is skipped; the
    listeners hear about it as about any other run that ends."""
    runner = _runner("mail")
    svc = await held([_job("digest")], [runner])
    if kind == "job":
        work, wrapper, target = svc.engine.run_cron, svc._run_job_wrapper, svc._jobs[0]
    else:
        work, wrapper, target = runner.run, svc._run_source_wrapper, runner
    started, release = asyncio.Event(), asyncio.Event()

    async def until_released(*_args, **_kwargs):
        started.set()
        await release.wait()
        return work.return_value

    work.side_effect = until_released
    heard = MagicMock()
    svc.add_schedule_listener(heard)
    catch_up = asyncio.create_task(wrapper(target, origin="catchup"))
    await asyncio.wait_for(started.wait(), timeout=2)

    await wrapper(target)  # the scheduler's fire

    work.assert_awaited_once()
    heard.assert_called_once_with()
    release.set()
    await catch_up
    assert heard.call_count == 2


@pytest.mark.asyncio
async def test_listeners_hear_a_wakeup_claim(held):
    svc = await held()
    svc.engine.sessions = MagicMock()
    svc.engine.sessions.is_running = MagicMock(return_value=False)
    svc.engine.run = AsyncMock()
    svc.db.get_due_wakeups = AsyncMock(return_value=[
        {"id": 7, "session_id": "0a1b2c3d4e5f", "prompt": "continue", "reason": ""},
    ])
    heard = MagicMock()
    svc.add_schedule_listener(heard)

    svc.db.claim_wakeup = AsyncMock(return_value=False)  # another sweep won
    await svc._sweep_wakeups()
    heard.assert_not_called()
    svc.db.claim_wakeup = AsyncMock(return_value=True)
    await svc._sweep_wakeups()
    heard.assert_called_once_with()


@pytest.mark.asyncio
async def test_async_and_failing_listeners(held, caplog):
    svc = await held([_job("digest")])
    heard = asyncio.Event()
    calls: list[str] = []

    def broken() -> None:
        calls.append("broken")
        raise RuntimeError("listener bug")

    async def eager() -> None:
        calls.append("async")
        heard.set()

    svc.add_schedule_listener(broken)
    svc.add_schedule_listener(eager)
    with caplog.at_level(logging.ERROR, logger="nerve.cron.service"):
        await svc._run_job_wrapper(svc._jobs[0])
        await asyncio.wait_for(heard.wait(), timeout=2)

    assert calls == ["broken", "async"]
    assert any("listener" in r.getMessage().lower() for r in caplog.records)
    svc.remove_schedule_listener(broken)
    svc.remove_schedule_listener(eager)
    await svc._run_job_wrapper(svc._jobs[0])
    assert calls == ["broken", "async"]


@pytest.mark.asyncio
async def test_a_failing_async_listener_is_logged_with_its_traceback(held, caplog):
    svc = await held([_job("digest")])

    async def broken() -> None:
        raise RuntimeError("listener bug")

    svc.add_schedule_listener(broken)
    with caplog.at_level(logging.ERROR, logger="nerve.cron.service"):
        await svc._run_job_wrapper(svc._jobs[0])
        for _ in range(10):
            await asyncio.sleep(0)

    [record] = [r for r in caplog.records if "listener" in r.getMessage().lower()]
    assert record.exc_info is not None
    assert record.exc_info[0] is RuntimeError


@pytest.mark.asyncio
async def test_listeners_hear_nothing_once_the_service_is_stopping(held):
    svc = await held([_job("digest")])
    heard = MagicMock()
    svc.add_schedule_listener(heard)

    async def run_until_cancelled(**_kwargs) -> TurnResult:
        await asyncio.Event().wait()

    svc.engine.run_cron = run_until_cancelled
    run = asyncio.create_task(svc._run_job_wrapper(svc._jobs[0]))
    await asyncio.sleep(0)
    await svc.stop()
    # The shutdown cancels the runs in flight; each still ends in the wrapper.
    run.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await run

    heard.assert_not_called()
    assert not svc._listener_tasks


# ---------------------------------------------------------------------------
# GET /api/cron/schedule and `nerve cron next`
# ---------------------------------------------------------------------------

_REPORT = {
    "next": {"at": "2026-03-02T09:00:00+00:00", "name": "digest", "kind": "cron"},
    "jobs": [
        {"id": "planner", "next_run": "2026-03-02T08:00:00+00:00",
         "forecast": "skip", "reason": "gate 'tasks' not satisfied (pending tasks exist)"},
        {"id": "digest", "next_run": "2026-03-02T09:00:00+00:00",
         "forecast": "run", "reason": None},
        {"id": "off", "next_run": None, "forecast": None, "reason": None},
    ],
}


def _cron_app(bypass_auth) -> FastAPI:
    from nerve.gateway.routes import cron as cron_routes

    app = FastAPI()
    app.include_router(cron_routes.router)
    return bypass_auth(app)


@pytest.mark.asyncio
async def test_the_schedule_route_returns_the_report(bypass_auth, monkeypatch):
    service = SimpleNamespace(schedule_report=AsyncMock(return_value=_REPORT))
    monkeypatch.setattr("nerve.gateway.server._cron_service", service)
    transport = httpx.ASGITransport(app=_cron_app(bypass_auth))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/cron/schedule")

    assert response.status_code == 200
    assert response.json() == _REPORT


@pytest.mark.asyncio
async def test_the_schedule_route_without_a_cron_service(bypass_auth, monkeypatch):
    monkeypatch.setattr("nerve.gateway.server._cron_service", None)
    transport = httpx.ASGITransport(app=_cron_app(bypass_auth))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/cron/schedule")

    assert response.json() == {"next": None, "jobs": []}


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/api/sources/mail/sync", "/api/sources/sync-all"])
async def test_a_source_synced_by_hand_tells_the_schedule_listeners(
    bypass_auth, monkeypatch, path,
):
    """New messages can change what a gated job's next run will do."""
    from nerve.gateway.routes import sources as source_routes

    runner = _runner("mail")
    service = SimpleNamespace(
        _source_runners=[runner], notify_schedule_changed=MagicMock(),
    )
    monkeypatch.setattr("nerve.gateway.server._cron_service", service)
    monkeypatch.setattr(source_routes, "get_deps", lambda: SimpleNamespace(db=AsyncMock()))
    app = FastAPI()
    app.include_router(source_routes.router)
    transport = httpx.ASGITransport(app=bypass_auth(app))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(path)

    assert response.status_code == 200
    runner.run.assert_awaited_once()
    service.notify_schedule_changed.assert_called_once_with()


@pytest.mark.asyncio
async def test_work_done_elsewhere_reaches_the_listeners(held):
    svc = await held()
    heard = MagicMock()
    svc.add_schedule_listener(heard)

    svc.notify_schedule_changed()

    heard.assert_called_once_with()


def _config_dir(tmp_path):
    config_dir, workspace = tmp_path / "cfg", tmp_path / "ws"
    config_dir.mkdir()
    (workspace / "config").mkdir(parents=True)
    (config_dir / "config.yaml").write_text(
        f"workspace: {workspace}\ntimezone: UTC\n", encoding="utf-8",
    )
    (config_dir / "config.local.yaml").write_text("", encoding="utf-8")
    return config_dir


def test_cron_next_prints_the_next_tick_and_each_forecast(tmp_path, monkeypatch):
    from nerve import cli

    calls = []

    def fake_request(ctx, config, method, path, body=None):
        calls.append((method, path))
        return _REPORT

    monkeypatch.setattr(cli, "_gateway_request", fake_request)
    result = CliRunner().invoke(cli.main, ["-c", str(_config_dir(tmp_path)), "cron", "next"])

    assert result.exit_code == 0, result.output
    assert calls == [("GET", "/api/cron/schedule")]
    lines = result.output.splitlines()
    assert lines[0] == "Next: 2026-03-02 09:00:00 UTC — digest (cron)"
    assert any(
        "planner" in line and "skip — gate 'tasks' not satisfied" in line for line in lines
    )
    assert any("digest" in line and line.rstrip().endswith("run") for line in lines)
    assert any(line.split()[:1] == ["off"] and line.rstrip().endswith("—") for line in lines)


def test_cron_next_with_nothing_scheduled(tmp_path, monkeypatch):
    from nerve import cli

    monkeypatch.setattr(
        cli, "_gateway_request", lambda *a, **k: {"next": None, "jobs": []},
    )
    result = CliRunner().invoke(cli.main, ["-c", str(_config_dir(tmp_path)), "cron", "next"])

    assert result.exit_code == 0, result.output
    assert result.output.splitlines()[0] == "Next: nothing scheduled in the next 7 days"
