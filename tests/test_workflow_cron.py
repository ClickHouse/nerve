"""Tests for cron jobs that declare a workflow run instead of a prompt."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

import nerve.workflows
from nerve.cron.jobs import CronJob, load_jobs, save_jobs
from nerve.cron.service import CronService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _workflow_decl(**overrides) -> dict:
    decl = {
        "engine": "claude-workflow",
        "prompt": "audit the repo",
        "budget_usd": 5.0,
    }
    decl.update(overrides)
    return decl


def _workflow_job(**kwargs) -> CronJob:
    kwargs.setdefault("id", "wf-job")
    kwargs.setdefault("schedule", "1h")
    kwargs.setdefault("workflow", _workflow_decl())
    return CronJob(**kwargs)


def _make_cron_service() -> CronService:
    """Minimal CronService with mocked dependencies (see tests/test_cron.py)."""
    config = MagicMock()
    config.timezone = "UTC"
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
    db.set_cron_log_session = AsyncMock()
    db.get_last_successful_cron_run = AsyncMock(return_value=None)
    db.get_channel_session = AsyncMock(return_value=None)
    db.get_session = AsyncMock(return_value=None)
    db.set_channel_session = AsyncMock()

    return CronService(config, engine, db)


@pytest_asyncio.fixture
async def cron_service():
    """Minimal CronService with mocked dependencies."""
    return _make_cron_service()


@pytest.fixture
def workflow_service(monkeypatch):
    """Fake WorkflowRunService installed as the module singleton."""
    service = MagicMock()
    service.start_run = AsyncMock(
        return_value={"id": "wfr-x", "session_id": "workflow:wfr-x"},
    )
    monkeypatch.setattr(nerve.workflows, "_service", service)
    return service


# ---------------------------------------------------------------------------
# CronJob.workflow — parsing and validation
# ---------------------------------------------------------------------------

class TestCronJobWorkflowField:
    def test_from_dict_parses_workflow(self):
        decl = _workflow_decl(title="Audit", model="model-x", effort="high")
        job = CronJob.from_dict({"id": "x", "schedule": "1h", "workflow": decl})
        assert job.workflow == decl
        assert job.prompt == ""

    def test_from_dict_defaults_to_none(self):
        job = CronJob.from_dict({"id": "x", "schedule": "1h", "prompt": "p"})
        assert job.workflow is None

    def test_workflow_only_job_is_valid(self):
        job = _workflow_job()  # no prompt, no prompt_file
        assert job.prompt == ""
        assert job.prompt_file == ""
        assert job.workflow["engine"] == "claude-workflow"

    def test_without_prompt_or_workflow_raises(self):
        with pytest.raises(ValueError, match="workflow"):
            CronJob(id="x", schedule="1h")

    def test_workflow_missing_engine_raises(self):
        with pytest.raises(ValueError, match="engine"):
            _workflow_job(workflow={"prompt": "p", "budget_usd": 1.0})

    def test_workflow_blank_engine_raises(self):
        with pytest.raises(ValueError, match="engine"):
            _workflow_job(workflow=_workflow_decl(engine="  "))

    def test_workflow_missing_prompt_raises(self):
        with pytest.raises(ValueError, match="prompt"):
            _workflow_job(
                workflow={"engine": "claude-workflow", "budget_usd": 1.0},
            )

    def test_workflow_missing_budget_raises(self):
        with pytest.raises(ValueError, match="budget_usd"):
            _workflow_job(
                workflow={"engine": "claude-workflow", "prompt": "p"},
            )

    @pytest.mark.parametrize("budget", [0, -2, "cheap", True, None])
    def test_workflow_bad_budget_raises(self, budget):
        with pytest.raises(ValueError, match="budget_usd"):
            _workflow_job(workflow=_workflow_decl(budget_usd=budget))

    def test_workflow_integer_budget_is_valid(self):
        job = _workflow_job(workflow=_workflow_decl(budget_usd=3))
        assert job.workflow["budget_usd"] == 3

    def test_workflow_not_a_mapping_raises(self):
        with pytest.raises(ValueError, match="mapping"):
            _workflow_job(workflow="claude-workflow")

    def test_save_jobs_round_trips_workflow(self, tmp_path):
        decl = _workflow_decl(
            title="Audit", model="model-x", effort="high", cwd="/tmp",
        )
        job = CronJob.from_dict({"id": "x", "schedule": "1h", "workflow": decl})
        out = tmp_path / "out.yaml"
        save_jobs([job], out)
        assert load_jobs(out)[0].workflow == decl

    def test_save_jobs_round_trips_no_workflow(self, tmp_path):
        job = CronJob.from_dict({"id": "x", "schedule": "1h", "prompt": "p"})
        out = tmp_path / "out.yaml"
        save_jobs([job], out)
        assert load_jobs(out)[0].workflow is None

    def test_load_jobs_drops_invalid_workflow_job_keeps_valid(self, tmp_path):
        yaml_file = tmp_path / "jobs.yaml"
        yaml_file.write_text(
            "jobs:\n"
            "  - id: bad\n"
            "    schedule: 1h\n"
            "    workflow:\n"
            "      engine: claude-workflow\n"
            "      prompt: do it\n"  # missing budget_usd → dropped
            "  - id: good\n"
            "    schedule: 1h\n"
            "    workflow:\n"
            "      engine: codex-ultracode\n"
            "      prompt: do it\n"
            "      budget_usd: 2.5\n"
            "  - id: plain\n"
            "    schedule: 1h\n"
            "    prompt: hi\n",
            encoding="utf-8",
        )
        jobs = load_jobs(yaml_file)
        assert [j.id for j in jobs] == ["good", "plain"]
        assert jobs[0].workflow["budget_usd"] == 2.5


# ---------------------------------------------------------------------------
# _run_job_inner — workflow branch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestRunWorkflowJob:
    async def test_starts_run_with_declared_spec(
        self, cron_service, workflow_service,
    ):
        job = _workflow_job(workflow=_workflow_decl(
            title="Nightly audit", model="model-x", effort="high",
        ))

        await cron_service._run_job_inner(job)

        workflow_service.start_run.assert_awaited_once_with(
            engine_kind="claude-workflow",
            spec={
                "prompt": "audit the repo",
                "model": "model-x",
                "effort": "high",
                "cwd": "",
            },
            budget_usd=5.0,
            title="Nightly audit",
            created_by="cron:wf-job",
        )

    async def test_links_run_session_and_logs_success(
        self, cron_service, workflow_service,
    ):
        job = _workflow_job()

        await cron_service._run_job_inner(job)

        cron_service.db.set_cron_log_session.assert_awaited_once_with(
            1, "workflow:wfr-x",
        )
        args, kwargs = cron_service.db.log_cron_finish.call_args
        assert args == (1, "success")
        assert kwargs["output"] == "workflow run wfr-x started (budget $5.00)"

    async def test_title_defaults_to_job_id(
        self, cron_service, workflow_service,
    ):
        job = _workflow_job()

        await cron_service._run_job_inner(job)

        assert workflow_service.start_run.call_args.kwargs["title"] == "wf-job"

    async def test_session_id_falls_back_to_run_id(
        self, cron_service, workflow_service,
    ):
        workflow_service.start_run.return_value = {"id": "wfr-y"}
        job = _workflow_job()

        await cron_service._run_job_inner(job)

        cron_service.db.set_cron_log_session.assert_awaited_once_with(
            1, "workflow:wfr-y",
        )

    async def test_skips_cron_session_machinery(
        self, cron_service, workflow_service,
    ):
        # Even a "persistent" workflow job must not touch cron sessions —
        # the run owns its own workflow:<run-id> session.
        job = _workflow_job(session_mode="persistent")

        await cron_service._run_job_inner(job)

        cron_service.engine.run_cron.assert_not_called()
        cron_service.engine.run_persistent_cron.assert_not_called()
        cron_service.engine.sessions.get_or_create.assert_not_called()
        cron_service.db.set_channel_session.assert_not_called()

    async def test_does_not_wait_for_run_completion(
        self, cron_service, workflow_service,
    ):
        # start_run returns as soon as the run is queued; the cron log
        # records success while the run itself is still pending.
        workflow_service.start_run.return_value = {
            "id": "wfr-x", "session_id": "workflow:wfr-x", "status": "pending",
        }
        job = _workflow_job()

        await cron_service._run_job_inner(job)

        args, _kwargs = cron_service.db.log_cron_finish.call_args
        assert args == (1, "success")

    async def test_start_run_error_logs_error(
        self, cron_service, workflow_service,
    ):
        from nerve.workflows.service import WorkflowRunError

        workflow_service.start_run.side_effect = WorkflowRunError(
            "budget_usd is required",
        )
        job = _workflow_job()

        await cron_service._run_job_inner(job)

        args, kwargs = cron_service.db.log_cron_finish.call_args
        assert args == (1, "error")
        assert kwargs["error"] == "budget_usd is required"

    async def test_unexpected_error_logs_error(
        self, cron_service, workflow_service,
    ):
        workflow_service.start_run.side_effect = RuntimeError("boom")
        job = _workflow_job()

        await cron_service._run_job_inner(job)

        args, kwargs = cron_service.db.log_cron_finish.call_args
        assert args == (1, "error")
        assert kwargs["error"] == "boom"

    async def test_service_disabled_logs_error(self, cron_service, monkeypatch):
        monkeypatch.setattr(nerve.workflows, "_service", None)
        job = _workflow_job()

        await cron_service._run_job_inner(job)

        args, kwargs = cron_service.db.log_cron_finish.call_args
        assert args == (1, "error")
        assert kwargs["error"] == "workflow runs disabled"
        cron_service.engine.run_cron.assert_not_called()

    async def test_gates_still_apply(self, cron_service, workflow_service):
        cron_service.db.count_tasks = AsyncMock(return_value=0)
        job = _workflow_job(run_if=[{"type": "tasks", "status": "pending"}])

        await cron_service._run_job_inner(job)

        cron_service.db.log_cron_start.assert_not_called()
        workflow_service.start_run.assert_not_called()

    async def test_prompt_jobs_unaffected(self, cron_service, workflow_service):
        job = CronJob(id="plain", schedule="1h", prompt="do stuff")

        await cron_service._run_job_inner(job)

        workflow_service.start_run.assert_not_called()
        cron_service.engine.run_cron.assert_called_once()
        args, kwargs = cron_service.db.log_cron_finish.call_args
        assert args[1] == "success"
