"""Tests for cron persistent timers and startup catch-up."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from nerve.cron.jobs import CronJob
from nerve.cron.service import CronService, _parse_interval, _parse_timestamp


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_job(
    id: str = "test-job",
    schedule: str = "4h",
    catchup: bool = True,
    enabled: bool = True,
    **kwargs,
) -> CronJob:
    return CronJob(
        id=id,
        schedule=schedule,
        prompt="do stuff",
        catchup=catchup,
        enabled=enabled,
        **kwargs,
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _hours_ago(h: float) -> str:
    """Return an ISO timestamp string h hours in the past."""
    return (_utc_now() - timedelta(hours=h)).isoformat()


def _make_cron_log(finished_at: str) -> dict:
    return {"job_id": "test-job", "finished_at": finished_at, "status": "success"}


@pytest_asyncio.fixture
async def cron_service():
    """Minimal CronService with mocked dependencies."""
    config = MagicMock()
    config.cron.system_file = MagicMock()
    config.cron.jobs_file = MagicMock()
    config.agent.cron_model = "test-model"
    config.sessions.cron_session_mode = "per_run"

    engine = AsyncMock()
    engine.run_cron = AsyncMock(return_value="ok")
    engine.run_persistent_cron = AsyncMock(return_value="ok")

    db = AsyncMock()
    db.log_cron_start = AsyncMock(return_value=1)
    db.log_cron_finish = AsyncMock()
    db.get_last_successful_cron_run = AsyncMock(return_value=None)

    svc = CronService(config, engine, db)
    return svc


# ---------------------------------------------------------------------------
# _parse_timestamp
# ---------------------------------------------------------------------------

class TestParseTimestamp:
    def test_iso_with_timezone(self):
        ts = "2026-03-10T12:00:00+00:00"
        result = _parse_timestamp(ts)
        assert result.tzinfo is not None
        assert result.hour == 12

    def test_iso_with_z(self):
        ts = "2026-03-10T12:00:00Z"
        result = _parse_timestamp(ts)
        assert result.tzinfo is not None

    def test_space_separated(self):
        ts = "2026-03-10 12:00:00"
        result = _parse_timestamp(ts)
        assert result.tzinfo is not None
        assert result.year == 2026

    def test_no_tz_suffix(self):
        ts = "2026-03-10T12:00:00"
        result = _parse_timestamp(ts)
        assert result.tzinfo is not None


# ---------------------------------------------------------------------------
# _parse_interval
# ---------------------------------------------------------------------------

class TestParseInterval:
    def test_hours(self):
        assert _parse_interval("4h") == 14400

    def test_minutes(self):
        assert _parse_interval("30m") == 1800

    def test_combined(self):
        assert _parse_interval("1h30m") == 5400

    def test_seconds(self):
        assert _parse_interval("90s") == 90

    def test_default_on_garbage(self):
        assert _parse_interval("???") == 7200


# ---------------------------------------------------------------------------
# _is_overdue
# ---------------------------------------------------------------------------

class TestIsOverdue:
    def test_interval_overdue(self):
        job = _make_job(schedule="4h")
        last_run = _utc_now() - timedelta(hours=5)
        assert CronService._is_overdue(job, last_run, _utc_now()) is True

    def test_interval_not_overdue(self):
        job = _make_job(schedule="4h")
        last_run = _utc_now() - timedelta(hours=2)
        assert CronService._is_overdue(job, last_run, _utc_now()) is False

    def test_interval_exactly_on_boundary(self):
        job = _make_job(schedule="4h")
        last_run = _utc_now() - timedelta(hours=4)
        assert CronService._is_overdue(job, last_run, _utc_now()) is True

    def test_crontab_overdue(self):
        """Crontab schedule that should have fired yesterday."""
        job = _make_job(schedule="0 5 * * *")  # daily at 5am UTC
        last_run = _utc_now() - timedelta(days=2)
        assert CronService._is_overdue(job, last_run, _utc_now()) is True

    def test_crontab_not_overdue(self):
        """Crontab that just ran — next fire is in the future."""
        job = _make_job(schedule="0 5 * * *")
        # Set last_run to 1 minute ago — next fire is ~24h away
        last_run = _utc_now() - timedelta(minutes=1)
        assert CronService._is_overdue(job, last_run, _utc_now()) is False

    def test_interval_multiple_missed(self):
        """Multiple missed intervals still returns True (not a count)."""
        job = _make_job(schedule="1h")
        last_run = _utc_now() - timedelta(hours=10)
        assert CronService._is_overdue(job, last_run, _utc_now()) is True


# ---------------------------------------------------------------------------
# _make_trigger (interval alignment)
# ---------------------------------------------------------------------------

class TestMakeTrigger:
    @pytest.mark.asyncio
    async def test_interval_aligned_to_last_run(self, cron_service):
        """Interval trigger should anchor to last successful run."""
        last_finished = _hours_ago(2)
        cron_service.db.get_last_successful_cron_run.return_value = (
            _make_cron_log(last_finished)
        )

        job = _make_job(schedule="4h")
        trigger = await cron_service._make_trigger(job)

        from apscheduler.triggers.interval import IntervalTrigger
        assert isinstance(trigger, IntervalTrigger)

        # Next fire should be ~2h from now (4h - 2h elapsed), not 4h
        next_fire = trigger.get_next_fire_time(None, _utc_now())
        delta = next_fire - _utc_now()
        # Allow some tolerance (1.5h to 2.5h)
        assert timedelta(hours=1.5) < delta < timedelta(hours=2.5)

    @pytest.mark.asyncio
    async def test_interval_no_last_run(self, cron_service):
        """First-ever run: no alignment, default interval from now."""
        cron_service.db.get_last_successful_cron_run.return_value = None

        job = _make_job(schedule="4h")
        trigger = await cron_service._make_trigger(job)

        from apscheduler.triggers.interval import IntervalTrigger
        assert isinstance(trigger, IntervalTrigger)

        next_fire = trigger.get_next_fire_time(None, _utc_now())
        delta = next_fire - _utc_now()
        # Should be close to 4h from now
        assert timedelta(hours=3.5) < delta < timedelta(hours=4.5)

    @pytest.mark.asyncio
    async def test_crontab_unchanged(self, cron_service):
        """Crontab triggers are returned as-is (already absolute)."""
        job = _make_job(schedule="0 5 * * *")
        trigger = await cron_service._make_trigger(job)

        from apscheduler.triggers.cron import CronTrigger
        assert isinstance(trigger, CronTrigger)


# ---------------------------------------------------------------------------
# _catchup_missed_jobs
# ---------------------------------------------------------------------------

class TestCatchupMissedJobs:
    @pytest.mark.asyncio
    async def test_fires_overdue_jobs(self, cron_service):
        """Overdue jobs should be fired on catch-up."""
        job = _make_job(id="overdue-job", schedule="4h")
        cron_service._jobs = [job]
        cron_service.db.get_last_successful_cron_run.return_value = (
            _make_cron_log(_hours_ago(6))
        )

        await cron_service._catchup_missed_jobs()

        cron_service.db.log_cron_start.assert_called_once_with("overdue-job")
        cron_service.engine.run_cron.assert_called_once()

    @pytest.mark.asyncio
    async def test_skips_not_overdue(self, cron_service):
        """Jobs that ran recently should not catch up."""
        job = _make_job(id="recent-job", schedule="4h")
        cron_service._jobs = [job]
        cron_service.db.get_last_successful_cron_run.return_value = (
            _make_cron_log(_hours_ago(1))
        )

        await cron_service._catchup_missed_jobs()

        cron_service.db.log_cron_start.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_first_ever_run(self, cron_service):
        """New jobs with no history should not catch up."""
        job = _make_job(id="new-job", schedule="4h")
        cron_service._jobs = [job]
        cron_service.db.get_last_successful_cron_run.return_value = None

        await cron_service._catchup_missed_jobs()

        cron_service.db.log_cron_start.assert_not_called()

    @pytest.mark.asyncio
    async def test_respects_catchup_false(self, cron_service):
        """Jobs with catchup=False should not fire on startup."""
        job = _make_job(id="no-catchup", schedule="4h", catchup=False)
        cron_service._jobs = [job]
        cron_service.db.get_last_successful_cron_run.return_value = (
            _make_cron_log(_hours_ago(10))
        )

        await cron_service._catchup_missed_jobs()

        cron_service.db.log_cron_start.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_disabled_jobs(self, cron_service):
        """Disabled jobs should not catch up."""
        job = _make_job(id="disabled", schedule="4h", enabled=False)
        cron_service._jobs = [job]
        cron_service.db.get_last_successful_cron_run.return_value = (
            _make_cron_log(_hours_ago(10))
        )

        await cron_service._catchup_missed_jobs()

        cron_service.db.log_cron_start.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_overdue_run_concurrently(self, cron_service):
        """Multiple overdue jobs should fire concurrently."""
        jobs = [
            _make_job(id="job-a", schedule="4h"),
            _make_job(id="job-b", schedule="2h"),
            _make_job(id="job-c", schedule="1h"),
        ]
        cron_service._jobs = jobs

        # All overdue
        cron_service.db.get_last_successful_cron_run.return_value = (
            _make_cron_log(_hours_ago(10))
        )

        await cron_service._catchup_missed_jobs()

        # All three should have been fired
        assert cron_service.db.log_cron_start.call_count == 3
        assert cron_service.engine.run_cron.call_count == 3

    @pytest.mark.asyncio
    async def test_multiple_missed_fires_only_once(self, cron_service):
        """A job that missed 5 intervals should still only fire once."""
        job = _make_job(id="multi-miss", schedule="1h")
        cron_service._jobs = [job]
        # Last ran 5h ago — missed 5 intervals
        cron_service.db.get_last_successful_cron_run.return_value = (
            _make_cron_log(_hours_ago(5))
        )

        await cron_service._catchup_missed_jobs()

        # Exactly one catch-up fire
        cron_service.db.log_cron_start.assert_called_once()
        cron_service.engine.run_cron.assert_called_once()

    @pytest.mark.asyncio
    async def test_crontab_overdue_catches_up(self, cron_service):
        """A crontab job that missed its window should catch up."""
        job = _make_job(id="daily-5am", schedule="0 5 * * *")
        cron_service._jobs = [job]
        # Last ran 2 days ago
        cron_service.db.get_last_successful_cron_run.return_value = (
            _make_cron_log(_hours_ago(48))
        )

        await cron_service._catchup_missed_jobs()

        cron_service.db.log_cron_start.assert_called_once()


# ---------------------------------------------------------------------------
# CronJob.catchup field
# ---------------------------------------------------------------------------

class TestCronJobCatchup:
    def test_default_true(self):
        job = _make_job()
        assert job.catchup is True

    def test_from_dict_default(self):
        job = CronJob.from_dict({"id": "x", "schedule": "1h", "prompt": "p"})
        assert job.catchup is True

    def test_from_dict_explicit_false(self):
        job = CronJob.from_dict({
            "id": "x", "schedule": "1h", "prompt": "p", "catchup": False,
        })
        assert job.catchup is False


# ---------------------------------------------------------------------------
# CronJob.lock field
# ---------------------------------------------------------------------------

class TestCronJobLock:
    def test_default_false(self):
        job = _make_job()
        assert job.lock is False

    def test_from_dict_default(self):
        job = CronJob.from_dict({"id": "x", "schedule": "1h", "prompt": "p"})
        assert job.lock is False

    def test_from_dict_explicit_true(self):
        job = CronJob.from_dict({
            "id": "x", "schedule": "1h", "prompt": "p", "lock": True,
        })
        assert job.lock is True


# ---------------------------------------------------------------------------
# Job lock (concurrent run serialization)
# ---------------------------------------------------------------------------

class TestJobLock:
    @pytest.mark.asyncio
    async def test_lock_serializes_concurrent_runs(self, cron_service):
        """When lock=True, overlapping runs execute sequentially."""
        call_order = []

        async def slow_cron(*args, **kwargs):
            call_order.append("start")
            await asyncio.sleep(0.1)
            call_order.append("end")
            return "ok"

        cron_service.engine.run_cron = slow_cron
        job = _make_job(id="locked-job", lock=True)

        await asyncio.gather(
            cron_service._run_job_wrapper(job),
            cron_service._run_job_wrapper(job),
        )

        # With lock: runs are sequential — start/end/start/end
        assert call_order == ["start", "end", "start", "end"]

    @pytest.mark.asyncio
    async def test_no_lock_allows_concurrent_runs(self, cron_service):
        """When lock=False (default), runs can overlap."""
        call_order = []

        async def slow_cron(*args, **kwargs):
            call_order.append("start")
            await asyncio.sleep(0.1)
            call_order.append("end")
            return "ok"

        cron_service.engine.run_cron = slow_cron
        job = _make_job(id="unlocked-job", lock=False)

        await asyncio.gather(
            cron_service._run_job_wrapper(job),
            cron_service._run_job_wrapper(job),
        )

        # Without lock: runs overlap — start/start/end/end
        assert call_order == ["start", "start", "end", "end"]

    @pytest.mark.asyncio
    async def test_lock_uses_per_job_locks(self, cron_service):
        """Different locked jobs get independent locks (don't block each other)."""
        call_order = []

        async def slow_cron(*args, **kwargs):
            call_order.append(f"start")
            await asyncio.sleep(0.1)
            call_order.append(f"end")
            return "ok"

        cron_service.engine.run_cron = slow_cron
        job_a = _make_job(id="job-a", lock=True)
        job_b = _make_job(id="job-b", lock=True)

        await asyncio.gather(
            cron_service._run_job_wrapper(job_a),
            cron_service._run_job_wrapper(job_b),
        )

        # Different jobs run concurrently even with lock=True
        assert call_order == ["start", "start", "end", "end"]
