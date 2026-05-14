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


# ---------------------------------------------------------------------------
# Context rotation: _maybe_rotate_context + rotate_session (force-rotate)
# ---------------------------------------------------------------------------


def _hours_ahead(h: float) -> datetime:
    return _utc_now() + timedelta(hours=h)


def _today_at_local(hour: int, minute: int = 0) -> datetime:
    """Today's HH:MM in the local timezone, returned as UTC-aware datetime."""
    local_tz = datetime.now().astimezone().tzinfo
    today = datetime.now(local_tz).replace(
        hour=hour, minute=minute, second=0, microsecond=0,
    )
    return today.astimezone(timezone.utc)


class TestMaybeRotateContext:
    """Tests for the rotation predicate.

    Together with TestRotateSession this covers two long-standing bugs:
      * force-rotate API silently doing nothing
      * `rotate_at` predicate going dead after any nerve restart past the
        configured local time, because `connected_at` was getting reset.
    """

    def _wire(self, cron_service, session: dict | None) -> tuple:
        cron_service.db.get_session = AsyncMock(return_value=session)
        cron_service.db.update_session_fields = AsyncMock()
        cron_service.engine._memorize_session = AsyncMock()
        cron_service.engine.sessions = MagicMock()
        cron_service.engine.sessions.mark_idle = AsyncMock()
        return (
            cron_service.engine.sessions.mark_idle,
            cron_service.db.update_session_fields,
        )

    @pytest.mark.asyncio
    async def test_returns_false_when_session_missing(self, cron_service):
        mark_idle, _ = self._wire(cron_service, None)
        assert not await cron_service._maybe_rotate_context(
            "cron:x", rotate_hours=24,
        )
        mark_idle.assert_not_called()

    @pytest.mark.asyncio
    async def test_force_bypasses_all_predicates(self, cron_service):
        """force=True must rotate regardless of schedule config."""
        # Session with no connected_at, no last_rotated_at, rotate_hours=0,
        # rotate_at unset — every normal predicate would return False.
        session = {"id": "cron:x", "connected_at": None, "last_rotated_at": None}
        mark_idle, update_fields = self._wire(cron_service, session)

        rotated = await cron_service._maybe_rotate_context(
            "cron:x", rotate_hours=0, force=True,
        )

        assert rotated is True
        mark_idle.assert_awaited_once_with("cron:x", preserve_sdk_id=False)
        # last_rotated_at must be persisted on success.
        update_fields.assert_awaited_once()
        args, _ = update_fields.call_args
        assert args[0] == "cron:x"
        assert "last_rotated_at" in args[1]

    @pytest.mark.asyncio
    async def test_rotate_at_first_rotation_with_null_last_rotated(
        self, cron_service,
    ):
        """NULL last_rotated_at should not block first-time daily rotation
        once today's boundary has passed."""
        # rotate_at = 1h ago in local time → already past boundary today.
        boundary_local_hour = (datetime.now().astimezone().hour - 1) % 24
        session = {
            "id": "cron:x",
            "connected_at": _hours_ago(0.1),  # connected just now
            "last_rotated_at": None,
        }
        self._wire(cron_service, session)

        rotated = await cron_service._maybe_rotate_context(
            "cron:x", rotate_hours=0,
            rotate_at=f"{boundary_local_hour:02d}:00",
        )
        assert rotated is True

    @pytest.mark.asyncio
    async def test_rotate_at_skipped_if_already_rotated_today(
        self, cron_service,
    ):
        """Daily rotation must run at most once per day."""
        boundary_local_hour = (datetime.now().astimezone().hour - 1) % 24
        boundary_utc = _today_at_local(boundary_local_hour, 0)
        # Already rotated AFTER today's boundary — should NOT rotate again.
        session = {
            "id": "cron:x",
            "connected_at": _hours_ago(0.1),
            "last_rotated_at": (boundary_utc + timedelta(minutes=5)).isoformat(),
        }
        mark_idle, _ = self._wire(cron_service, session)

        rotated = await cron_service._maybe_rotate_context(
            "cron:x", rotate_hours=0,
            rotate_at=f"{boundary_local_hour:02d}:00",
        )
        assert rotated is False
        mark_idle.assert_not_called()

    @pytest.mark.asyncio
    async def test_rotate_at_survives_restart_past_boundary(
        self, cron_service,
    ):
        """Regression: nerve restart that lands AFTER today's rotate_at must
        not break daily rotation for the rest of the day.

        Old code compared `connected_at` (which is reset on every reconnect /
        restart) to today's rotate_at boundary, so once `connected_at` was
        past the boundary, rotation was dead until the next calendar day.

        With `last_rotated_at`, the connect timestamp is irrelevant: as long
        as we haven't rotated since today's boundary, rotation must fire.
        """
        boundary_local_hour = (datetime.now().astimezone().hour - 1) % 24
        boundary_utc = _today_at_local(boundary_local_hour, 0)
        # connected_at reset to a moment AFTER today's boundary — exactly
        # the post-restart scenario that used to break rotation.
        session = {
            "id": "cron:x",
            "connected_at": (boundary_utc + timedelta(minutes=10)).isoformat(),
            # Last rotated yesterday → still eligible today.
            "last_rotated_at": (boundary_utc - timedelta(days=1)).isoformat(),
        }
        self._wire(cron_service, session)

        rotated = await cron_service._maybe_rotate_context(
            "cron:x", rotate_hours=0,
            rotate_at=f"{boundary_local_hour:02d}:00",
        )
        assert rotated is True

    @pytest.mark.asyncio
    async def test_rotate_at_before_boundary_does_not_rotate(
        self, cron_service,
    ):
        """Before today's rotate_at boundary, must wait."""
        boundary_local_hour = (datetime.now().astimezone().hour + 2) % 24
        session = {
            "id": "cron:x",
            "connected_at": _hours_ago(1),
            "last_rotated_at": None,
        }
        mark_idle, _ = self._wire(cron_service, session)

        rotated = await cron_service._maybe_rotate_context(
            "cron:x", rotate_hours=0,
            rotate_at=f"{boundary_local_hour:02d}:00",
        )
        assert rotated is False
        mark_idle.assert_not_called()

    @pytest.mark.asyncio
    async def test_hours_uses_last_rotated_when_present(self, cron_service):
        """rotate_hours-based: prefer `last_rotated_at` over `connected_at`
        for the age baseline (so it survives restarts the same way)."""
        # connected_at is "fresh" (just reconnected), but last rotation was
        # 30h ago — must be eligible for 24h rotation.
        session = {
            "id": "cron:x",
            "connected_at": _hours_ago(0.1),
            "last_rotated_at": _hours_ago(30),
        }
        self._wire(cron_service, session)

        assert await cron_service._maybe_rotate_context(
            "cron:x", rotate_hours=24,
        )

    @pytest.mark.asyncio
    async def test_hours_falls_back_to_connected_at_when_never_rotated(
        self, cron_service,
    ):
        """Pre-v027 sessions without last_rotated_at still rotate using
        connected_at as the baseline."""
        session = {
            "id": "cron:x",
            "connected_at": _hours_ago(48),
            "last_rotated_at": None,
        }
        self._wire(cron_service, session)

        assert await cron_service._maybe_rotate_context(
            "cron:x", rotate_hours=24,
        )

    @pytest.mark.asyncio
    async def test_hours_no_baseline_returns_false(self, cron_service):
        """If both timestamps are NULL, hours-based rotation can't decide
        — wait for next reconnect to set a baseline."""
        session = {
            "id": "cron:x",
            "connected_at": None,
            "last_rotated_at": None,
        }
        mark_idle, _ = self._wire(cron_service, session)

        assert not await cron_service._maybe_rotate_context(
            "cron:x", rotate_hours=24,
        )
        mark_idle.assert_not_called()


class TestRotateSession:
    """Force-rotate via the admin API endpoint.

    Regression for bug where `rotate_session` always returned `rotated=False`
    because it called `_maybe_rotate_context(rotate_hours=0)` without
    `rotate_at`, and the inner `elif rotate_hours > 0` filtered zero out.
    """

    @pytest.mark.asyncio
    async def test_force_rotate_actually_rotates(self, cron_service):
        cron_service._jobs = [_make_job(id="inbox", session_mode="persistent")]
        cron_service.db.get_session = AsyncMock(return_value={
            "id": "cron:inbox",
            "connected_at": _hours_ago(2),
            "last_rotated_at": None,
        })
        cron_service.db.update_session_fields = AsyncMock()
        cron_service.engine._memorize_session = AsyncMock()
        cron_service.engine.sessions = MagicMock()
        cron_service.engine.sessions.mark_idle = AsyncMock()

        result = await cron_service.rotate_session("inbox")

        assert result["rotated"] is True
        assert result["job_id"] == "inbox"
        assert result["session_age_hours"] is not None
        cron_service.engine.sessions.mark_idle.assert_awaited_once_with(
            "cron:inbox", preserve_sdk_id=False,
        )

    @pytest.mark.asyncio
    async def test_rotate_session_unknown_job_raises(self, cron_service):
        cron_service._jobs = []
        cron_service._load_merged_jobs = MagicMock(return_value=[])
        with pytest.raises(ValueError, match="Job not found"):
            await cron_service.rotate_session("nope")

    @pytest.mark.asyncio
    async def test_rotate_session_non_persistent_raises(self, cron_service):
        cron_service._jobs = [_make_job(id="oneshot", session_mode="per_run")]
        with pytest.raises(ValueError, match="not persistent"):
            await cron_service.rotate_session("oneshot")
