"""The notices that an agent cron job fails and that it works again."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from nerve.agent.engine import TurnOutcome, TurnResult
from nerve.cron.jobs import CronJob
from nerve.cron.service import CronService
from nerve.sources.models import IngestResult
from tests.actor_rows import ensure_system_principal

pytestmark = pytest.mark.asyncio

OK = TurnResult("done", TurnOutcome())
FAILED = TurnResult("partial", TurnOutcome("failed", "max turns (50) exhausted"))


@pytest_asyncio.fixture
async def db(db):  # noqa: F811 - the conftest database, with an identity
    await ensure_system_principal(db)
    return db


@pytest_asyncio.fixture
async def svc(db):
    config = MagicMock()
    config.timezone = "UTC"
    config.agent.cron_model = "test-model"
    engine = AsyncMock()
    engine.run_cron = AsyncMock(return_value=OK)
    service = CronService(config, engine, db)
    service.notification_service = MagicMock()
    service.notification_service.send_notification = AsyncMock(return_value="notif-1")
    return service


def _job(job_id: str = "digest", **kwargs) -> CronJob:
    return CronJob(id=job_id, schedule="1h", prompt="write the digest", **kwargs)


async def _run(svc: CronService, result: TurnResult | Exception, job: CronJob | None = None):
    if isinstance(result, Exception):
        svc.engine.run_cron = AsyncMock(side_effect=result)
    else:
        svc.engine.run_cron = AsyncMock(return_value=result)
    await svc._run_job_inner(job or _job())


def _notices(svc: CronService) -> list[tuple[str, str]]:
    return [
        (call.kwargs["title"], call.kwargs["priority"])
        for call in svc.notification_service.send_notification.call_args_list
    ]


async def _logs(db, job_id: str) -> list[dict]:
    return sorted(await db.get_cron_logs(job_id), key=lambda row: row["id"])


async def test_a_failing_job_sends_one_notice_then_one_when_it_works_again(svc, db):
    await _run(svc, OK)
    await _run(svc, FAILED)
    await _run(svc, RuntimeError("the MCP server did not answer"))
    await _run(svc, OK)
    await _run(svc, OK)

    assert _notices(svc) == [
        ("Cron job digest failed", "high"),
        ("Cron job digest works again", "low"),
    ]
    logs = await _logs(db, "digest")
    failed, recovered = svc.notification_service.send_notification.call_args_list
    assert failed.kwargs["session_id"] == logs[1]["session_id"]
    assert failed.kwargs["metadata"] == {"cron_job_id": "digest", "cron_log_id": logs[1]["id"]}
    assert "max turns (50) exhausted" in failed.kwargs["body"]
    assert recovered.kwargs["session_id"] == logs[3]["session_id"]
    assert recovered.kwargs["metadata"] == {"cron_job_id": "digest", "cron_log_id": logs[3]["id"]}


async def test_a_job_whose_first_run_fails_sends_the_notice(svc):
    await _run(svc, RuntimeError("boom"))

    assert _notices(svc) == [("Cron job digest failed", "high")]
    assert "boom" in svc.notification_service.send_notification.call_args.kwargs["body"]


async def test_two_overlapping_failures_still_send_the_notice(svc, db):
    await _run(svc, OK)
    first = await db.log_cron_start("digest")
    second = await db.log_cron_start("digest")
    await db.log_cron_finish(first, "error", error="boom")
    await db.log_cron_finish(second, "error", error="boom")

    # Each run compares with the run that started before it, not with the
    # run that finished last.
    await svc._send_health_notice("digest", second, "error")
    await svc._send_health_notice("digest", first, "error")

    assert _notices(svc) == [("Cron job digest failed", "high")]


async def test_an_earlier_run_that_is_still_going_does_not_count(svc, db):
    await _run(svc, OK)
    await db.log_cron_start("digest")
    later = await db.log_cron_start("digest")
    await db.log_cron_finish(later, "error", error="boom")

    await svc._send_health_notice("digest", later, "error")

    assert _notices(svc) == [("Cron job digest failed", "high")]


async def test_a_job_that_fails_less_often_than_the_cleanup_notifies_once(svc, db):
    await db._write(
        "INSERT INTO cron_logs (job_id, started_at, finished_at, status, error) "
        "VALUES ('digest', datetime('now', '-30 days'), datetime('now', '-30 days'), "
        "'error', 'boom')",
    )
    await db.cleanup_old_cron_logs(days=14)

    await _run(svc, FAILED)

    assert _notices(svc) == []


async def test_a_missed_run_between_two_failures_does_not_repeat_the_notice(svc, db):
    await _run(svc, FAILED)
    await db.log_cron_missed("digest", "2030-01-01 00:00:00")
    await _run(svc, FAILED)

    assert _notices(svc) == [("Cron job digest failed", "high")]


async def test_the_notice_has_the_first_lines_of_the_error(svc):
    error = "\n".join(f"line {n}" for n in range(1, 20))
    await _run(svc, RuntimeError(error))

    body = svc.notification_service.send_notification.call_args.kwargs["body"]
    assert "line 5" in body and "line 6" not in body


async def test_a_notice_that_fails_does_not_change_the_run(svc, db):
    svc.notification_service.send_notification = AsyncMock(side_effect=RuntimeError("down"))

    await _run(svc, FAILED)

    [log] = await _logs(db, "digest")
    assert log["status"] == "error"


async def test_a_workflow_job_sends_no_notice(svc, monkeypatch):
    workflows = MagicMock()
    workflows.start_run = AsyncMock(side_effect=RuntimeError("budget refused"))
    monkeypatch.setattr("nerve.workflows.get_workflow_run_service", lambda: workflows)
    job = CronJob(
        id="audit", schedule="1h",
        workflow={"engine": "claude-workflow", "prompt": "audit", "budget_usd": 1},
    )

    await svc._run_job_inner(job)

    assert _notices(svc) == []


async def test_a_failing_source_run_sends_no_notice(svc, db):
    runner = MagicMock()
    runner.job_id = "source:mail"
    runner.source.source_name = "mail"
    runner.run = AsyncMock(return_value=IngestResult(records_ingested=0, error="timeout"))

    await svc._run_source_inner(runner)

    assert _notices(svc) == []
