"""Switching a cron job on and off: the YAML rewrite, the service and the route."""

from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import pytest_asyncio
import yaml
from fastapi import FastAPI

from nerve import setup_writer
from nerve.config import ConfigError
from nerve.cron.service import CronService, CronToggleRefused
from tests.conftest import TEST_ACTOR

HEADER = "# Custom cron jobs\n\n"


def _dump(jobs: list[dict]) -> str:
    return HEADER + yaml.safe_dump({"jobs": jobs}, default_flow_style=False, sort_keys=False)


def _job(job_id: str, **kwargs) -> dict:
    return {"id": job_id, "schedule": "1h", "prompt": "do it", **kwargs}


def _enabled(path: Path) -> dict[str, object]:
    jobs = yaml.safe_load(path.read_text(encoding="utf-8"))["jobs"]
    return {job["id"]: job.get("enabled") for job in jobs}


# --------------------------------------------------------------------------- #
#  The rewrite (setup_writer)                                                  #
# --------------------------------------------------------------------------- #


class TestPlanCronJobEnabled:
    def test_it_changes_the_one_flag_and_keeps_the_header(self, tmp_path):
        path = tmp_path / "jobs.yaml"
        path.write_text(_dump([_job("a", enabled=True), _job("b", enabled=True)]))

        plan = setup_writer.plan_cron_job_enabled(path, "a", False)
        assert plan.ok and plan.disabled == ("a",)
        plan.publish()

        assert path.read_text() == _dump([_job("a", enabled=False), _job("b", enabled=True)])

    def test_a_job_without_the_key_gets_it(self, tmp_path):
        path = tmp_path / "jobs.yaml"
        path.write_text(_dump([_job("a")]))

        setup_writer.plan_cron_job_enabled(path, "a", False).publish()

        assert _enabled(path) == {"a": False}

    def test_a_job_already_in_that_state_is_not_rewritten(self, tmp_path):
        path = tmp_path / "jobs.yaml"
        path.write_text(_dump([_job("a")]))

        plan = setup_writer.plan_cron_job_enabled(path, "a", True)

        assert plan.ok and not plan.changes_anything

    @pytest.mark.parametrize("text", [
        HEADER + "jobs:\n# the digest\n- id: a\n  schedule: 1h\n  prompt: do it\n",
        HEADER + "jobs:\n  - id: a\n    schedule: 1h\n    prompt: |\n      do it\n",
        HEADER + "jobs:\n- id: a\n  schedule: 1h  # hourly\n  prompt: do it\n",
    ])
    def test_a_hand_edited_file_is_refused_and_left_alone(self, tmp_path, text):
        path = tmp_path / "jobs.yaml"
        path.write_text(text)

        plan = setup_writer.plan_cron_job_enabled(path, "a", False)

        assert plan.problem == setup_writer.CRON_FILE_HAND_EDITED
        assert setup_writer.cron_file_problem(path) == setup_writer.CRON_FILE_HAND_EDITED
        assert path.read_text() == text

    def test_a_file_without_the_job_is_refused(self, tmp_path):
        path = tmp_path / "jobs.yaml"
        path.write_text(_dump([_job("a")]))

        plan = setup_writer.plan_cron_job_enabled(path, "b", False)

        assert plan.problem == setup_writer.CRON_JOB_MISSING

    @pytest.mark.skipif(os.geteuid() == 0, reason="root writes read-only files")
    def test_a_file_in_a_read_only_directory_is_refused(self, tmp_path):
        path = tmp_path / "cron" / "jobs.yaml"
        path.parent.mkdir()
        path.write_text(_dump([_job("a")]))
        path.parent.chmod(0o500)
        try:
            plan = setup_writer.plan_cron_job_enabled(path, "a", False)
        finally:
            path.parent.chmod(0o700)

        assert plan.problem == setup_writer.CRON_FILE_READ_ONLY

    def test_a_missing_file_is_refused(self, tmp_path):
        plan = setup_writer.plan_cron_job_enabled(tmp_path / "jobs.yaml", "a", False)

        assert plan.problem == "missing"


# --------------------------------------------------------------------------- #
#  The service                                                                 #
# --------------------------------------------------------------------------- #


@pytest_asyncio.fixture
async def svc(tmp_path):
    cron_dir = tmp_path / "cron"
    cron_dir.mkdir()
    (cron_dir / "system.yaml").write_text(_dump([
        _job("memory-maintenance", enabled=True),
        _job("task-planner", enabled=False),
    ]))
    (cron_dir / "jobs.yaml").write_text(_dump([
        _job("digest", enabled=False),
        _job("task-planner", enabled=True),
    ]))
    config = MagicMock()
    config.timezone = "UTC"
    config.lockdown = False
    config.cron.jobs_file = cron_dir / "jobs.yaml"
    config.cron.system_file = cron_dir / "system.yaml"
    config.cron.gate_plugins_dir = cron_dir / "gates"
    db = AsyncMock()
    db.get_last_successful_cron_run = AsyncMock(return_value=None)
    db.get_last_cron_runs = AsyncMock(return_value={})
    db.get_cron_job_stats = AsyncMock(return_value={})
    db.get_latest_cron_session_id = AsyncMock(return_value=None)
    service = CronService(config, AsyncMock(), db)
    service.scheduler.start(paused=True)
    await service.reload()
    try:
        yield service
    finally:
        service.scheduler.shutdown(wait=False)


class TestSetJobEnabled:
    @pytest.mark.asyncio
    async def test_a_user_job_is_written_to_jobs_yaml_and_scheduled(self, svc):
        system_before = svc.config.cron.system_file.read_text()

        await svc.set_job_enabled("digest", True)

        assert _enabled(svc.config.cron.jobs_file) == {"digest": True, "task-planner": True}
        assert svc.config.cron.system_file.read_text() == system_before
        assert svc.scheduler.get_job("digest") is not None

    @pytest.mark.asyncio
    async def test_a_job_that_jobs_yaml_replaces_is_written_there(self, svc):
        await svc.set_job_enabled("task-planner", False)

        assert _enabled(svc.config.cron.jobs_file)["task-planner"] is False
        assert _enabled(svc.config.cron.system_file)["task-planner"] is False
        assert svc.scheduler.get_job("task-planner") is None

    @pytest.mark.asyncio
    async def test_a_system_job_is_written_to_system_yaml(self, svc):
        await svc.set_job_enabled("memory-maintenance", False)

        assert _enabled(svc.config.cron.system_file)["memory-maintenance"] is False
        assert svc.scheduler.get_job("memory-maintenance") is None

    @pytest.mark.asyncio
    async def test_lockdown_refuses_and_writes_nothing(self, svc):
        svc.config.lockdown = True
        before = svc.config.cron.jobs_file.read_text()

        with pytest.raises(CronToggleRefused, match="lockdown"):
            await svc.set_job_enabled("digest", True)

        assert svc.config.cron.jobs_file.read_text() == before

    @pytest.mark.asyncio
    async def test_a_hand_edited_file_is_refused(self, svc):
        svc.config.cron.jobs_file.write_text(
            svc.config.cron.jobs_file.read_text() + "# a note\n",
        )

        with pytest.raises(CronToggleRefused, match="Switch this job in jobs.yaml"):
            await svc.set_job_enabled("digest", True)

    @pytest.mark.asyncio
    async def test_a_refused_reload_puts_the_file_back(self, svc, monkeypatch):
        before = svc.config.cron.jobs_file.read_bytes()
        monkeypatch.setattr(
            svc, "_reload_from_disk", AsyncMock(side_effect=ConfigError("bad gate")),
        )

        with pytest.raises(ConfigError, match="bad gate"):
            await svc.set_job_enabled("digest", True)

        assert svc.config.cron.jobs_file.read_bytes() == before
        assert svc.scheduler.get_job("digest") is None

    @pytest.mark.asyncio
    async def test_an_unknown_job_is_a_value_error(self, svc):
        with pytest.raises(ValueError):
            await svc.set_job_enabled("nope", True)

    @pytest.mark.asyncio
    async def test_the_job_list_says_which_jobs_cannot_be_switched(self, svc):
        svc.config.cron.jobs_file.write_text(
            svc.config.cron.jobs_file.read_text() + "# a note\n",
        )

        rows = {row["id"]: row for row in await svc.list_jobs()}

        assert rows["memory-maintenance"]["toggle_blocked"] is None
        assert "jobs.yaml" in rows["digest"]["toggle_blocked"]
        svc.config.lockdown = True
        rows = {row["id"]: row for row in await svc.list_jobs()}
        assert "lockdown" in rows["memory-maintenance"]["toggle_blocked"]


# --------------------------------------------------------------------------- #
#  The routes                                                                  #
# --------------------------------------------------------------------------- #


def _client(bypass_auth, actor=None) -> httpx.AsyncClient:
    from nerve.gateway.auth import require_auth
    from nerve.gateway.routes import cron as cron_routes

    app = FastAPI()
    app.include_router(cron_routes.router)
    bypass_auth(app)
    if actor is not None:
        app.dependency_overrides[require_auth] = lambda: actor
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_the_active_switch_round_trips(svc, bypass_auth, monkeypatch):
    monkeypatch.setattr("nerve.gateway.server._cron_service", svc)

    async with _client(bypass_auth) as client:
        on = await client.put("/api/cron/jobs/digest/enabled", json={"enabled": True})
        listed = (await client.get("/api/cron/jobs")).json()["jobs"]
        off = await client.put("/api/cron/jobs/digest/enabled", json={"enabled": False})

    assert on.status_code == 200 and on.json() == {"job_id": "digest", "enabled": True}
    assert next(job for job in listed if job["id"] == "digest")["enabled"] is True
    assert off.status_code == 200
    assert _enabled(svc.config.cron.jobs_file)["digest"] is False


@pytest.mark.asyncio
async def test_the_switch_is_refused_under_lockdown(svc, bypass_auth, monkeypatch):
    monkeypatch.setattr("nerve.gateway.server._cron_service", svc)
    svc.config.lockdown = True

    async with _client(bypass_auth) as client:
        response = await client.put("/api/cron/jobs/digest/enabled", json={"enabled": True})

    assert response.status_code == 409
    assert "lockdown" in response.json()["detail"]


@pytest.mark.asyncio
async def test_a_refused_reload_is_400_and_changes_nothing(svc, bypass_auth, monkeypatch):
    monkeypatch.setattr("nerve.gateway.server._cron_service", svc)
    monkeypatch.setattr(svc, "_reload_from_disk", AsyncMock(side_effect=ConfigError("bad gate")))
    before = svc.config.cron.jobs_file.read_bytes()

    async with _client(bypass_auth) as client:
        response = await client.put("/api/cron/jobs/digest/enabled", json={"enabled": True})

    assert response.status_code == 400
    assert response.json()["detail"] == "bad gate"
    assert svc.config.cron.jobs_file.read_bytes() == before


@pytest.mark.asyncio
async def test_the_switch_of_an_unknown_job_is_404(svc, bypass_auth, monkeypatch):
    monkeypatch.setattr("nerve.gateway.server._cron_service", svc)

    async with _client(bypass_auth) as client:
        response = await client.put("/api/cron/jobs/source:mail/enabled", json={"enabled": False})

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_a_token_cannot_switch_a_job(svc, bypass_auth, monkeypatch):
    monkeypatch.setattr("nerve.gateway.server._cron_service", svc)
    token = replace(TEST_ACTOR, kind="system", account_id=None)

    async with _client(bypass_auth, token) as client:
        response = await client.put("/api/cron/jobs/digest/enabled", json={"enabled": True})

    assert response.status_code == 403
    assert _enabled(svc.config.cron.jobs_file)["digest"] is False


@pytest.mark.asyncio
async def test_the_logs_route_hides_empty_runs_and_counts_them(bypass_auth, monkeypatch):
    from nerve.gateway.routes import cron as cron_routes

    db = AsyncMock()
    db.get_cron_logs = AsyncMock(return_value=[])
    db.count_cron_logs = AsyncMock(return_value=4)
    db.count_empty_cron_logs = AsyncMock(return_value=7)
    monkeypatch.setattr(cron_routes, "get_deps", lambda: SimpleNamespace(db=db))

    async with _client(bypass_auth) as client:
        body = (await client.get("/api/cron/logs?job_id=source:mail&hide_empty=1")).json()

    assert (body["total"], body["empty"]) == (4, 7)
    assert db.get_cron_logs.call_args.kwargs["hide_empty"] is True
    assert db.count_cron_logs.call_args.kwargs == {"job_id": "source:mail", "hide_empty": True}
    db.count_empty_cron_logs.assert_awaited_once_with(job_id="source:mail")
