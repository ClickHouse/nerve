"""Cron job health: run counts, latest runs and source record counts."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from nerve.agent.engine import TurnOutcome, TurnResult
from nerve.cron.jobs import CronJob
from nerve.cron.service import CronService
from nerve.db.migrations import v064_cron_log_records
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
    return CronService(config, engine, db)


def _job(job_id: str = "digest", **kwargs) -> CronJob:
    return CronJob(id=job_id, schedule="1h", prompt="write the digest", **kwargs)


async def _run(svc: CronService, result: TurnResult | Exception, job: CronJob | None = None):
    if isinstance(result, Exception):
        svc.engine.run_cron = AsyncMock(side_effect=result)
    else:
        svc.engine.run_cron = AsyncMock(return_value=result)
    await svc._run_job_inner(job or _job())


async def _logs(db, job_id: str) -> list[dict]:
    return sorted(await db.get_cron_logs(job_id), key=lambda row: row["id"])


async def _add_log(db, job_id: str, status: str | None, *, started: str, records=None) -> int:
    result = await db._write(
        "INSERT INTO cron_logs (job_id, started_at, finished_at, status, records) "
        "VALUES (?, ?, ?, ?, ?)",
        (job_id, started, started if status else None, status, records),
    )
    return result.lastrowid


# --------------------------------------------------------------------------- #
#  Source record counts and the "no new items" filter                          #
# --------------------------------------------------------------------------- #


async def test_a_source_run_records_what_it_fetched(svc, db):
    runner = MagicMock()
    runner.job_id = "source:mail"
    runner.source.source_name = "mail"
    runner.run = AsyncMock(return_value=IngestResult(records_ingested=3, records_dropped=2))
    await svc._run_source_inner(runner)
    runner.run = AsyncMock(return_value=IngestResult(records_ingested=0, records_dropped=4))
    await svc._run_source_inner(runner)
    runner.run = AsyncMock(return_value=IngestResult(records_ingested=0))
    await svc._run_source_inner(runner)

    # A run whose records the guardrail all dropped is not an empty run.
    assert [log["records"] for log in await _logs(db, "source:mail")] == [5, 4, 0]


async def test_hide_empty_leaves_out_only_successful_runs_with_no_records(db):
    empty = await _add_log(db, "source:mail", "success", started="2030-01-01 00:00:00", records=0)
    new = await _add_log(db, "source:mail", "success", started="2030-01-01 00:05:00", records=2)
    failed = await _add_log(db, "source:mail", "error", started="2030-01-01 00:10:00", records=0)
    agent = await _add_log(db, "digest", "success", started="2030-01-01 00:15:00")
    running = await _add_log(db, "source:mail", None, started="2030-01-01 00:20:00", records=None)

    shown = await db.get_cron_logs(hide_empty=True)

    assert [row["id"] for row in shown] == [running, agent, failed, new]
    assert empty not in [row["id"] for row in shown]
    assert await db.count_cron_logs(hide_empty=True) == 4
    assert await db.count_cron_logs() == 5
    assert await db.count_empty_cron_logs() == 1
    assert await db.count_empty_cron_logs("source:mail") == 1
    assert await db.count_empty_cron_logs("digest") == 0
    assert [row["id"] for row in await db.get_cron_logs("source:mail", hide_empty=True)] == [
        running, failed, new,
    ]


async def test_the_migration_counts_the_records_of_older_source_runs(db):
    await db.db.execute("ALTER TABLE cron_logs DROP COLUMN records")
    for job_id, output in (
        ("source:mail", "0 ingested"),
        ("source:mail", "12 ingested, 1 dropped by guardrail"),
        ("source:mail", "0 ingested, 4 dropped by guardrail, error: timeout"),
        ("source:mail", "0 ingested, error: 2 dropped by guardrail"),
        ("source:mail", None),
        ("digest", "3 ingested"),
    ):
        await db.db.execute(
            "INSERT INTO cron_logs (job_id, status, output) VALUES (?, 'success', ?)",
            (job_id, output),
        )

    await v064_cron_log_records.up(db.db)
    await v064_cron_log_records.up(db.db)

    async with db.db.execute("SELECT records FROM cron_logs ORDER BY id") as cursor:
        assert [row[0] for row in await cursor.fetchall()] == [0, 13, 4, 0, None, None]


# --------------------------------------------------------------------------- #
#  Latest run and 7-day counts                                                 #
# --------------------------------------------------------------------------- #


async def test_the_counts_cover_seven_days_in_one_query(db):
    recent = "datetime('now', '-1 days')"
    for status in ("success", "success", "error", "missed", None):
        await db._write(
            f"INSERT INTO cron_logs (job_id, started_at, status) VALUES ('digest', {recent}, ?)",
            (status,),
        )
    await db._write(
        "INSERT INTO cron_logs (job_id, started_at, status) "
        "VALUES ('digest', datetime('now', '-8 days'), 'error')",
    )

    stats = await db.get_cron_job_stats(7)

    assert stats == {"digest": {"runs": 5, "success": 2, "error": 1, "missed": 1}}


async def test_the_latest_run_of_each_job(db):
    await _add_log(db, "digest", "success", started="2030-01-01 08:00:00")
    await _add_log(db, "digest", "success", started="2030-01-02 08:00:00")
    await _add_log(db, "digest", "error", started="2030-01-02 08:00:00")
    await _add_log(db, "digest", "missed", started="2030-01-01 12:00:00")
    await _add_log(db, "triage", None, started="2030-01-03 08:00:00")

    last = await db.get_last_cron_runs()

    # Two runs in the same second: the later row is the latest run.
    assert last["digest"]["status"] == "error"
    assert last["digest"]["started_at"] == "2030-01-02 08:00:00"
    assert last["triage"] == {
        "status": None, "started_at": "2030-01-03 08:00:00", "finished_at": None,
    }


async def test_the_cleanup_keeps_the_latest_finished_run_of_each_job(db):
    async def old(job_id: str, status: str | None, days: int) -> int:
        result = await db._write(
            "INSERT INTO cron_logs (job_id, started_at, status) "
            f"VALUES (?, datetime('now', '-{days} days'), ?)",
            (job_id, status),
        )
        return result.lastrowid

    await old("monthly", "error", 60)
    kept = await old("monthly", "error", 30)
    await old("monthly", "missed", 20)
    await old("hourly", "success", 30)
    recent = await _add_log(db, "hourly", "success", started="2030-01-01 00:00:00")

    assert await db.cleanup_old_cron_logs(days=14) == 3

    async with db.db.execute("SELECT id FROM cron_logs ORDER BY id") as cursor:
        assert [row[0] for row in await cursor.fetchall()] == [kept, recent]


async def test_the_job_list_has_kind_latest_run_and_counts(svc, db, tmp_path):
    svc.config.lockdown = False
    svc.config.cron.jobs_file = tmp_path / "jobs.yaml"
    svc.config.cron.system_file = tmp_path / "system.yaml"
    svc._jobs = [
        _job("digest"),
        CronJob(id="audit", schedule="1h",
                workflow={"engine": "claude-workflow", "prompt": "audit", "budget_usd": 1}),
    ]
    await _run(svc, FAILED)

    rows = {row["id"]: row for row in await svc.list_jobs()}

    assert rows["digest"]["kind"] == "agent"
    assert rows["audit"]["kind"] == "workflow"
    assert rows["digest"]["last_run"]["status"] == "error"
    assert rows["digest"]["stats_7d"] == {"runs": 1, "success": 0, "error": 1, "missed": 0}
    assert rows["audit"]["last_run"] is None
    assert rows["audit"]["stats_7d"] == {"runs": 0, "success": 0, "error": 0, "missed": 0}
