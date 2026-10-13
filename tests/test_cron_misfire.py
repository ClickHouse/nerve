"""A run that starts late within the misfire grace still runs, and a run dropped
past it leaves a `missed` row in cron_logs."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nerve.cron.service import CronService


@contextlib.asynccontextmanager
async def _running_service(tmp_path, db, tz: str = "UTC"):
    """CronService with its real scheduler running and no configured jobs."""
    config = MagicMock()
    config.timezone = tz
    config.lockdown = False
    config.cron.gate_plugins_dir = tmp_path / "gates"  # absent -> no-op
    svc = CronService(config, AsyncMock(), db)
    with patch.object(svc, "_load_merged_jobs", return_value=[]), \
            patch.object(svc, "_register_source_runners"), \
            patch.object(svc, "_catchup_missed_jobs", AsyncMock()):
        await svc.start()
    try:
        yield svc
    finally:
        svc.scheduler.shutdown(wait=False)


def _schedule_run(svc: CronService, job_id: str, seconds_late: int, ran: asyncio.Event) -> datetime:
    """Add a one-off run that was due ``seconds_late`` seconds ago.

    The due time is given in the service's timezone, as the cron triggers
    produce it. Returns it in UTC.
    """
    async def run():
        ran.set()

    due = (datetime.now(timezone.utc) - timedelta(seconds=seconds_late)).replace(microsecond=0)
    svc.scheduler.add_job(run, "date", run_date=due.astimezone(svc.timezone), id=job_id)
    return due


@pytest.mark.asyncio
async def test_a_run_a_few_seconds_late_still_runs(tmp_path, db):
    """APScheduler's default grace of 1s would drop this run."""
    ran = asyncio.Event()
    async with _running_service(tmp_path, db) as svc:
        _schedule_run(svc, "late", seconds_late=5, ran=ran)
        await asyncio.wait_for(ran.wait(), timeout=10)


@pytest.mark.asyncio
async def test_a_dropped_run_is_logged_as_missed(tmp_path, db):
    """The row is stamped with the due time in UTC, like every other row, and
    is not a successful run, so interval alignment and catch-up ignore it."""
    ran = asyncio.Event()
    async with _running_service(tmp_path, db, tz="America/New_York") as svc:
        due = _schedule_run(svc, "stale", seconds_late=3600, ran=ran)
        for _ in range(1000):
            rows = await db.get_cron_logs("stale")
            if rows:
                break
            await asyncio.sleep(0.01)

    assert [(r["status"], r["started_at"]) for r in rows] == [
        ("missed", due.strftime("%Y-%m-%d %H:%M:%S")),
    ]
    assert not ran.is_set()
    assert await db.get_last_successful_cron_run("stale") is None
