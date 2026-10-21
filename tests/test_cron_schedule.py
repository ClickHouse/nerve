"""Gate forecasts and the next scheduled tick: what the instance will do on its
own next, judged from what is known now."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

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
from nerve.sources.models import IngestResult, SourceRecord
from tests.actor_rows import mock_system_principal

IN_AN_HOUR = timedelta(hours=1)


def _ctx(db, job_id: str = "job") -> GateContext:
    return GateContext(job_id=job_id, db=db)


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


# ---------------------------------------------------------------------------
# The next scheduled tick
# ---------------------------------------------------------------------------


def _mock_db(pending_wakeups=()) -> AsyncMock:
    db = AsyncMock()
    db.log_cron_start = AsyncMock(return_value=1)
    db.get_last_successful_cron_run = AsyncMock(return_value=None)
    db.list_pending_wakeups = AsyncMock(return_value=list(pending_wakeups))
    db.count_tasks = AsyncMock(return_value=0)
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
