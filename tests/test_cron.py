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
# Context rotation — memorization must not block the run lifecycle
# ---------------------------------------------------------------------------

class TestRotationMemorize:
    @pytest.mark.asyncio
    async def test_rotation_schedules_background_memorize(self, cron_service):
        """Rotation schedules memorization instead of awaiting it inline."""
        cron_service.db.get_session = AsyncMock(return_value={
            "connected_at": _hours_ago(30),
        })

        rotated = await cron_service._maybe_rotate_context(
            "cron:pers", rotate_hours=24,
        )

        assert rotated is True
        cron_service.engine.schedule_memorize.assert_awaited_once_with(
            "cron:pers",
        )
        cron_service.engine._memorize_session.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_rotation_no_memorize(self, cron_service):
        """A session younger than the rotation window is left alone."""
        cron_service.db.get_session = AsyncMock(return_value={
            "connected_at": _hours_ago(1),
        })

        rotated = await cron_service._maybe_rotate_context(
            "cron:pers", rotate_hours=24,
        )

        assert rotated is False
        cron_service.engine.schedule_memorize.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_manual_rotation_forces_disabled_rotation_window(self, cron_service):
        """Manual rotation clears context even when scheduled rotation is disabled."""
        cron_service._jobs = [
            _make_job(
                id="pers", session_mode="persistent", context_rotate_hours=0,
            ),
        ]
        cron_service.db.get_session = AsyncMock(return_value={
            "connected_at": _hours_ago(1),
            "sdk_session_id": "sdk-123",
        })

        result = await cron_service.rotate_session("pers")

        assert result["rotated"] is True
        assert result["session_age_hours"] is not None
        cron_service.engine.schedule_memorize.assert_awaited_once_with(
            "cron:pers",
        )
        cron_service.engine.sessions.mark_idle.assert_awaited_once_with(
            "cron:pers", preserve_sdk_id=False,
        )


# ---------------------------------------------------------------------------
# Run gates — service-level skip/run behaviour
# ---------------------------------------------------------------------------

class TestRunGates:
    @pytest.mark.asyncio
    async def test_skips_when_tasks_gate_unsatisfied(self, cron_service):
        """A tasks gate with no matching tasks skips the run entirely."""
        cron_service.db.count_tasks = AsyncMock(return_value=0)
        job = _make_job(
            id="planner", run_if=[{"type": "tasks", "status": "pending"}],
        )

        await cron_service._run_job_inner(job)

        cron_service.db.log_cron_start.assert_not_called()
        cron_service.engine.run_cron.assert_not_called()

    @pytest.mark.asyncio
    async def test_runs_when_tasks_gate_satisfied(self, cron_service):
        """A tasks gate with matching tasks lets the run proceed."""
        cron_service.db.count_tasks = AsyncMock(return_value=2)
        job = _make_job(
            id="planner", run_if=[{"type": "tasks", "status": "pending"}],
        )

        await cron_service._run_job_inner(job)

        cron_service.db.log_cron_start.assert_called_once_with("planner")
        cron_service.engine.run_cron.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_gates_always_runs(self, cron_service):
        """A job with no gates runs unconditionally (no gate queries)."""
        job = _make_job(id="ungated")

        await cron_service._run_job_inner(job)

        cron_service.engine.run_cron.assert_called_once()

    @pytest.mark.asyncio
    async def test_legacy_skip_when_idle_skips(self, cron_service):
        """Legacy skip_when_idle still gates via the messages gate path."""
        cron_service.db.get_consumer_cursor = AsyncMock(return_value=9)
        cron_service.db.get_source_max_rowid = AsyncMock(return_value=9)
        job = _make_job(id="inbox", skip_when_idle=["gmail"])

        await cron_service._run_job_inner(job)

        cron_service.engine.run_cron.assert_not_called()

    @pytest.mark.asyncio
    async def test_and_semantics_one_gate_blocks(self, cron_service):
        """With two gates, one unsatisfied is enough to skip (AND)."""
        cron_service.db.count_tasks = AsyncMock(return_value=5)        # tasks: ok
        cron_service.db.get_consumer_cursor = AsyncMock(return_value=9)
        cron_service.db.get_source_max_rowid = AsyncMock(return_value=9)  # msgs: not ok
        job = _make_job(
            id="both",
            run_if=[
                {"type": "tasks", "status": "pending"},
                {"type": "messages", "sources": ["gmail"]},
            ],
        )

        await cron_service._run_job_inner(job)

        cron_service.engine.run_cron.assert_not_called()


# ---------------------------------------------------------------------------
# Prompt files (prompt_file)
# ---------------------------------------------------------------------------

class TestPromptFile:
    def test_requires_prompt_or_prompt_file(self):
        with pytest.raises(ValueError):
            CronJob(id="x", schedule="1h")

    def test_inline_prompt_resolves(self):
        job = _make_job()
        assert job.resolve_prompt() == "do stuff"

    def test_prompt_file_resolves(self, tmp_path):
        pf = tmp_path / "prompt.md"
        pf.write_text("from file", encoding="utf-8")
        job = CronJob(id="x", schedule="1h", prompt_file=str(pf))
        assert job.resolve_prompt() == "from file"

    def test_prompt_file_wins_over_inline(self, tmp_path):
        pf = tmp_path / "prompt.md"
        pf.write_text("file wins", encoding="utf-8")
        job = CronJob(id="x", schedule="1h", prompt="inline", prompt_file=str(pf))
        assert job.resolve_prompt() == "file wins"

    def test_prompt_file_read_fresh_each_run(self, tmp_path):
        pf = tmp_path / "prompt.md"
        pf.write_text("v1", encoding="utf-8")
        job = CronJob(id="x", schedule="1h", prompt_file=str(pf))
        assert job.resolve_prompt() == "v1"
        pf.write_text("v2", encoding="utf-8")
        assert job.resolve_prompt() == "v2"

    def test_missing_file_falls_back_to_inline(self, tmp_path):
        job = CronJob(
            id="x", schedule="1h",
            prompt="fallback", prompt_file=str(tmp_path / "nope.md"),
        )
        assert job.resolve_prompt() == "fallback"

    def test_missing_file_no_fallback_raises(self, tmp_path):
        job = CronJob(
            id="x", schedule="1h", prompt_file=str(tmp_path / "nope.md"),
        )
        with pytest.raises(RuntimeError):
            job.resolve_prompt()

    def test_from_dict_relative_to_base_dir(self, tmp_path):
        (tmp_path / "prompts").mkdir()
        (tmp_path / "prompts" / "shared.md").write_text("shared!", encoding="utf-8")
        job = CronJob.from_dict(
            {"id": "x", "schedule": "1h", "prompt_file": "prompts/shared.md"},
            base_dir=tmp_path,
        )
        assert job.resolve_prompt() == "shared!"

    def test_load_jobs_shared_prompt_file(self, tmp_path):
        from nerve.cron.jobs import load_jobs

        (tmp_path / "prompts").mkdir()
        (tmp_path / "prompts" / "shared.md").write_text("same prompt", encoding="utf-8")
        yaml_file = tmp_path / "jobs.yaml"
        yaml_file.write_text(
            "jobs:\n"
            "  - id: a\n"
            "    schedule: 1h\n"
            "    prompt_file: prompts/shared.md\n"
            "  - id: b\n"
            "    schedule: 2h\n"
            "    prompt_file: prompts/shared.md\n",
            encoding="utf-8",
        )
        jobs = load_jobs(yaml_file)
        assert len(jobs) == 2
        assert jobs[0].resolve_prompt() == "same prompt"
        assert jobs[1].resolve_prompt() == "same prompt"

    def test_load_jobs_skips_job_without_any_prompt(self, tmp_path):
        from nerve.cron.jobs import load_jobs

        yaml_file = tmp_path / "jobs.yaml"
        yaml_file.write_text(
            "jobs:\n"
            "  - id: bad\n"
            "    schedule: 1h\n"
            "  - id: good\n"
            "    schedule: 1h\n"
            "    prompt: hi\n",
            encoding="utf-8",
        )
        jobs = load_jobs(yaml_file)
        assert [j.id for j in jobs] == ["good"]

    def test_save_jobs_round_trips_prompt_file(self, tmp_path):
        from nerve.cron.jobs import load_jobs, save_jobs

        (tmp_path / "p.md").write_text("x", encoding="utf-8")
        job = CronJob.from_dict(
            {"id": "x", "schedule": "1h", "prompt_file": "p.md"},
            base_dir=tmp_path,
        )
        out = tmp_path / "out.yaml"
        save_jobs([job], out)
        loaded = load_jobs(out)
        assert loaded[0].prompt_file == "p.md"
        assert loaded[0].resolve_prompt() == "x"

    @pytest.mark.asyncio
    async def test_run_uses_prompt_file_content(self, cron_service, tmp_path):
        pf = tmp_path / "prompt.md"
        pf.write_text("file instructions", encoding="utf-8")
        job = CronJob(id="filed", schedule="1h", prompt_file=str(pf))

        await cron_service._run_job_inner(job)

        kwargs = cron_service.engine.run_cron.call_args.kwargs
        assert kwargs["prompt"] == "file instructions"

    @pytest.mark.asyncio
    async def test_run_unreadable_prompt_file_logs_error(self, cron_service, tmp_path):
        job = CronJob(id="filed", schedule="1h", prompt_file=str(tmp_path / "nope.md"))

        await cron_service._run_job_inner(job)

        cron_service.engine.run_cron.assert_not_called()
        args, kwargs = cron_service.db.log_cron_finish.call_args
        assert args[1] == "error"


# ---------------------------------------------------------------------------
# Run log output + session linking
# ---------------------------------------------------------------------------

class TestRunLogOutput:
    @pytest.mark.asyncio
    async def test_stores_tail_of_long_response(self, cron_service):
        long = "begin " + ("x" * 3000) + " THE END"
        cron_service.engine.run_cron = AsyncMock(return_value=long)
        job = _make_job()

        await cron_service._run_job_inner(job)

        kwargs = cron_service.db.log_cron_finish.call_args.kwargs
        output = kwargs["output"]
        assert output.endswith("THE END")
        assert output.startswith("…")
        assert len(output) <= 2001  # tail + ellipsis

    @pytest.mark.asyncio
    async def test_stores_short_response_verbatim(self, cron_service):
        cron_service.engine.run_cron = AsyncMock(return_value="all done")
        job = _make_job()

        await cron_service._run_job_inner(job)

        kwargs = cron_service.db.log_cron_finish.call_args.kwargs
        assert kwargs["output"] == "all done"

    @pytest.mark.asyncio
    async def test_isolated_run_links_session_id(self, cron_service):
        job = _make_job(id="iso-job")

        await cron_service._run_job_inner(job)

        run_id = cron_service.engine.run_cron.call_args.kwargs["run_id"]
        assert run_id  # service always generates one
        kwargs = cron_service.db.log_cron_finish.call_args.kwargs
        assert kwargs["session_id"] == f"cron:iso-job:{run_id}"

    @pytest.mark.asyncio
    async def test_persistent_run_links_session_id(self, cron_service):
        cron_service.db.get_session = AsyncMock(return_value=None)
        job = _make_job(id="pers-job", session_mode="persistent", context_rotate_hours=0)

        await cron_service._run_job_inner(job)

        kwargs = cron_service.db.log_cron_finish.call_args.kwargs
        assert kwargs["session_id"] == "cron:pers-job"

    @pytest.mark.asyncio
    async def test_error_run_still_links_session_id(self, cron_service):
        cron_service.engine.run_cron = AsyncMock(side_effect=RuntimeError("boom"))
        job = _make_job(id="err-job")

        await cron_service._run_job_inner(job)

        args, kwargs = cron_service.db.log_cron_finish.call_args
        assert args[1] == "error"
        assert kwargs["session_id"].startswith("cron:err-job:")


# ---------------------------------------------------------------------------
# Live session linking (chat available while a run is in flight)
# ---------------------------------------------------------------------------

class TestLiveSessionLink:
    @pytest.mark.asyncio
    async def test_isolated_links_session_before_run(self, cron_service):
        order: list[str] = []
        cron_service.db.set_cron_log_session = AsyncMock(
            side_effect=lambda *a, **k: order.append("link"),
        )

        async def _run(**kwargs):
            order.append("run")
            return "ok"

        cron_service.engine.run_cron = AsyncMock(side_effect=_run)
        job = _make_job(id="live-job")

        await cron_service._run_job_inner(job)

        assert order == ["link", "run"]
        log_id, session_id = cron_service.db.set_cron_log_session.call_args.args
        assert log_id == 1
        assert session_id.startswith("cron:live-job:")
        # Same session id must be used for the engine run
        assert (
            f"cron:live-job:{cron_service.engine.run_cron.call_args.kwargs['run_id']}"
            == session_id
        )

    @pytest.mark.asyncio
    async def test_persistent_links_session_before_run(self, cron_service):
        cron_service.db.get_session = AsyncMock(return_value=None)
        job = _make_job(
            id="pers-live", session_mode="persistent", context_rotate_hours=0,
        )

        await cron_service._run_job_inner(job)

        cron_service.db.set_cron_log_session.assert_awaited_once_with(
            1, "cron:pers-live",
        )

    @pytest.mark.asyncio
    async def test_no_link_when_prompt_unresolvable(self, cron_service, tmp_path):
        job = CronJob(id="bad", schedule="1h", prompt_file=str(tmp_path / "nope.md"))

        await cron_service._run_job_inner(job)

        cron_service.db.set_cron_log_session.assert_not_called()

    @pytest.mark.asyncio
    async def test_link_failure_does_not_break_run(self, cron_service):
        cron_service.db.set_cron_log_session = AsyncMock(
            side_effect=RuntimeError("db locked"),
        )
        job = _make_job(id="resilient")

        await cron_service._run_job_inner(job)

        cron_service.engine.run_cron.assert_called_once()
        args, kwargs = cron_service.db.log_cron_finish.call_args
        assert args[1] == "success"
