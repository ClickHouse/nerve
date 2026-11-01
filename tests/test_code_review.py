"""Review lifecycle, billing outbox and artifact admission with synthetic PRs."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from nerve.code_review.artifacts import freeze, validate
from nerve.code_review.config import CodeReviewConfig, ReviewProfile
from nerve.code_review.github import GitHub, ObservationError
from nerve.code_review.service import CodeReviewService
from nerve.config import NerveConfig
from nerve.db import Database

pytestmark = pytest.mark.asyncio


def pr(number=1, **kwargs):
    result = {
        "number": number, "title": "Synthetic change", "html_url": f"https://example.com/pull/{number}",
        "state": "open", "draft": False, "merged": False, "updated_at": "2026-01-01T00:00:00Z",
        "head": {"sha": "a" * 40}, "base": {"sha": "b" * 40, "ref": "main", "repo": {"id": 1, "full_name": "alice/example"}},
        "changed_files": 0,
    }
    result.update(kwargs)
    return result


def snapshot(data=None, **kwargs):
    data = data or pr()
    return {"head": data["head"]["sha"], "base": data["base"]["sha"], "base_ref": data["base"]["ref"],
            "merge_base": "c" * 40, "body": "Example", "number": data["number"], "repository": "alice/example",
            "files": [], "ci": {}, **kwargs}


@pytest_asyncio.fixture
async def db(tmp_path):
    database = Database(tmp_path / "state.db")
    await database.connect()
    await database.cr_profiles(["example"])
    yield database
    await database.close()


@pytest.fixture
def service(db, tmp_path):
    profile = ReviewProfile("example", "alice/example", "team-review-requested:alice/team", enabled=True)
    config = NerveConfig(workspace=tmp_path, code_review=CodeReviewConfig(profiles=[profile]))
    github = SimpleNamespace(pr=AsyncMock(return_value=pr()), snapshot=AsyncMock(return_value=snapshot()),
                             search=AsyncMock(return_value=[1]), sources=AsyncMock(return_value=[]))
    runs = SimpleNamespace(get_run=AsyncMock(return_value=None), start_run=AsyncMock(), kill_run=AsyncMock(),
                           settled_spend=AsyncMock(return_value=0))
    return CodeReviewService(config, db, runs, github=github)


async def test_dedup_heartbeat_and_no_hijacking(db, service):
    await db.upsert_task("unrelated", "memory/tasks/active/unrelated.md", "Rebase", source_url=pr()["html_url"])
    first = await service.observe(service.profile("example"), 1)
    task = await db.get_task(first["task_id"])
    second = await service.observe(service.profile("example"), 1)
    assert first["id"] == second["id"] != "unrelated"
    assert first["content_updated_at"] == second["content_updated_at"]
    assert task["revision"] == (await db.get_task(first["task_id"]))["revision"]
    assert len(await db.cr_rows("SELECT * FROM code_review_versions")) == 1


async def test_head_base_description_and_ci_versions(db):
    data = pr()
    first = await db.cr_observe("example", data, snapshot(data))
    ci = await db.cr_observe("example", data, snapshot(data, ci={"success": True}))
    assert ci["current_version"] == first["current_version"]
    for field in ("head", "base"):
        data[field]["sha"] = "d" * 40
        changed = await db.cr_observe("example", data, snapshot(data))
        assert changed["current_version"] != ci["current_version"]
        ci = changed
    changed = await db.cr_observe("example", data, snapshot(data, body="New requirements"))
    assert changed["current_version"] != ci["current_version"]
    versions = await db.cr_rows("SELECT * FROM code_review_versions")
    assert [v["status"] for v in versions].count("superseded") == 3


async def test_draft_close_reopen_and_search_removal(service, db):
    draft = pr(draft=True)
    assert await db.cr_observe("example", draft) is None
    row = await service.observe(service.profile("example"), 1)
    service.github.search.return_value = []  # team request removed
    service.github.pr.return_value = draft
    await service.poll("example")
    paused = await db.cr_one("SELECT * FROM code_review_prs")
    assert paused["review_state"] == "paused"
    assert not await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    service.github.pr.return_value = pr(state="closed", merged=True)
    await service.poll("example")
    assert (await db.get_task(row["task_id"]))["status"] == "done"
    service.github.pr.return_value = pr()
    await service.poll("example")
    assert (await db.get_task(row["task_id"]))["status"] == "in_progress"
    assert (await db.cr_one("SELECT * FROM code_review_prs"))["id"] == row["id"]


@pytest.mark.parametrize("message", ["403", "404", "429", "incomplete pagination"])
async def test_observation_errors_never_close(service, db, message):
    row = await service.observe(service.profile("example"), 1)
    service.github.search.side_effect = ObservationError(message)
    service.github.pr.side_effect = ObservationError(message)
    await service.poll("example")
    assert (await db.get_task(row["task_id"]))["status"] == "in_progress"
    current = await db.cr_one("SELECT * FROM code_review_prs")
    assert current["error"] == message
    assert current["content_updated_at"] == row["content_updated_at"]


async def test_atomic_reservation_concurrency_and_restart(db, tmp_path):
    for number in (1, 2):
        data = pr(number)
        await db.cr_observe("example", data, snapshot(data))
    other = Database(tmp_path / "state.db")
    await other.connect()
    try:
        claims = await asyncio.gather(db.cr_reserve("example", "2026-01-01", 5, 30, 3),
                                      other.cr_reserve("example", "2026-01-01", 5, 30, 3))
        assert sum(c is not None for c in claims) == 1
        assert await other.cr_day_spend("2026-01-02") == 5  # unsettled across midnight
    finally:
        await other.close()


async def test_version_budget_retries_and_midnight(db):
    data = pr()
    await db.cr_observe("example", data, snapshot(data))
    first = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    await db.cr_settle(first["run_id"], 3, "2026-01-02", "retry", "")
    assert await db.cr_day_spend("2026-01-01") == 3
    assert await db.cr_day_spend("2026-01-02") == 3
    second = await db.cr_reserve("example", "2026-01-02", 5, 30, 3)
    assert second["budget"] == 2
    await db.cr_settle(second["run_id"], 2.1, "2026-01-02", "retry", "")
    assert not await db.cr_reserve("example", "2026-01-03", 5, 30, 3)
    assert (await db.cr_one("SELECT * FROM code_review_versions"))["status"] == "review_limit"
    assert not await db.cr_settle(first["run_id"], 0, "2026-01-02", "complete", "")


async def test_daily_budget_shared_across_profiles(db):
    await db.cr_profiles(["another"])
    await db.cr_observe("example", pr(), snapshot())
    await db.cr_observe("another", pr(), snapshot())
    first = await db.cr_reserve("example", "2026-01-01", 5, 5, 3)
    assert first["budget"] == 5
    assert not await db.cr_reserve("another", "2026-01-01", 5, 5, 3)
    await db.cr_settle(first["run_id"], 5.5, "2026-01-01", "complete", "")
    assert not await db.cr_reserve("another", "2026-01-01", 5, 5, 3)
    assert await db.cr_day_spend("2026-01-01") == 5.5


async def test_result_for_old_sha_never_becomes_current(db):
    old = await db.cr_observe("example", pr(), snapshot())
    claim = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    new = pr(head={"sha": "d" * 40})
    await db.cr_observe("example", new, snapshot(new))
    await db.cr_settle(claim["run_id"], 1, "2026-01-01", "complete", "old-result.json", severity="high")
    current = await db.cr_one("SELECT * FROM code_review_prs")
    assert current["current_version"] != old["current_version"]
    assert current["reviewed_version"] is None
    assert current["review_state"] == "queued"


async def test_dispatch_rechecks_draft_without_start(service, db):
    await service.observe(service.profile("example"), 1)
    service.github.pr.return_value = pr(draft=True)
    await service.dispatch(service.profile("example"))
    service.runs.start_run.assert_not_called()
    assert not await db.cr_rows("SELECT * FROM code_review_attempts")


def complete_output():
    verdict = dict(head="a" * 40, base="b" * 40, severity="none", behavior_changed=False, findings=[])
    verdict.update({field: "Checked with limitations" for field in ("coverage", "limitations", "compatibility", "enablement", "invariants", "tests", "previous_findings")})
    return {"description.md": "Description", "diagrams.md": "```mermaid\nsequenceDiagram\nA->>B: call\n```", "review.md": "Review", "verdict.json": json.dumps(verdict)}


async def test_artifact_validation_and_immutability(tmp_path):
    output = complete_output()
    assert validate(output, snapshot())["severity"] == "none"
    with pytest.raises(ValueError, match="missing"):
        validate({"description.md": "partial"}, snapshot())
    freeze(tmp_path / "result.json", output)
    freeze(tmp_path / "result.json", output)
    with pytest.raises(ValueError, match="immutable"):
        freeze(tmp_path / "result.json", {"changed": True})


async def test_recovery_partial_and_missing_outbox_run(service, db):
    await service.observe(service.profile("example"), 1)
    claim = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    directory = service.root / claim["pr_id"] / claim["id"] / "attempts" / claim["run_id"] / "work"
    directory.mkdir(parents=True)
    (directory / "description.md").write_text("Saved partial")
    await service.reconcile()
    version = await db.cr_one("SELECT * FROM code_review_versions")
    assert version["status"] == "retry"
    saved = json.loads((service.root / version["artifact_path"]).read_text())
    assert saved["output"]["description.md"] == "Saved partial"
    assert (await db.cr_one("SELECT * FROM code_review_attempts"))["settled"] == 1


async def test_complete_run_then_draft_is_not_admitted(service, db):
    await service.observe(service.profile("example"), 1)
    claim = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    directory = service.root / claim["pr_id"] / claim["id"] / "attempts" / claim["run_id"] / "work"
    directory.mkdir(parents=True)
    for name, content in complete_output().items():
        (directory / name).write_text(content)
    service.runs.get_run.return_value = {"status": "done", "spent_usd": 1}
    service.runs.settled_spend.return_value = 1
    service.github.pr.return_value = pr(draft=True)
    await service.reconcile()
    assert (await db.cr_one("SELECT * FROM code_review_versions"))["status"] == "stale"
    current = await db.cr_one("SELECT * FROM code_review_prs")
    assert current["reviewed_version"] is None and current["draft"]


async def test_search_rejects_incomplete_and_duplicate_pages():
    github = GitHub()
    github.get = AsyncMock(return_value={"incomplete_results": True, "total_count": 1, "items": []})
    with pytest.raises(ObservationError, match="incomplete"):
        await github.search(ReviewProfile("example", "alice/example", "team-review-requested:alice/team"))


async def test_config_rejects_unmeterable_budget():
    for budget in (0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            ReviewProfile("example", "alice/example", "draft:false", version_budget_usd=budget)


async def test_reopen_completed_same_input_restores_current(db):
    row = await db.cr_observe("example", pr(), snapshot())
    claim = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    await db.cr_settle(claim["run_id"], 1, "2026-01-01", "complete", "result.json")
    await db.cr_observe("example", pr(state="closed"))
    reopened = await db.cr_observe("example", pr(), snapshot())
    assert reopened["review_state"] == "current"
    assert reopened["reviewed_version"] == row["current_version"]


async def test_stale_draft_result_ready_requeues_same_budget(db):
    await db.cr_observe("example", pr(), snapshot())
    claim = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    await db.cr_observe("example", pr(draft=True))
    await db.cr_settle(claim["run_id"], 2, "2026-01-01", "stale", "result.json")
    await db.cr_observe("example", pr(), snapshot())
    new = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    assert new["id"] == claim["id"] and new["budget"] == 3


async def test_queue_claim_is_bound_to_refreshed_version(service, db):
    first = await service.observe(service.profile("example"), 1)
    second_data = pr(2)
    second = await db.cr_observe("example", second_data, snapshot(second_data))
    changed = pr(head={"sha": "d" * 40})
    service.github.pr.return_value = changed
    service.github.snapshot.return_value = snapshot(changed)
    await service.dispatch(service.profile("example"))
    attempt = await db.cr_one("SELECT * FROM code_review_attempts")
    assert attempt["version_id"] != second["current_version"]
    assert attempt["version_id"] != first["current_version"]
    assert (await db.cr_one("SELECT status FROM code_review_versions WHERE id=?", (second["current_version"],)))["status"] == "queued"


async def test_inaccessible_oldest_does_not_starve_next(service, db):
    first = await service.observe(service.profile("example"), 1)
    await db.cr_observe("example", pr(2), snapshot(pr(2)))
    service.github.pr.side_effect = ObservationError("403")
    await service.dispatch(service.profile("example"))
    service.github.pr.side_effect = None
    service.github.pr.return_value = pr(2)
    service.github.snapshot.return_value = snapshot(pr(2))
    await service.dispatch(service.profile("example"))
    claim = await db.cr_one("SELECT * FROM code_review_attempts")
    version = await db.cr_one("SELECT * FROM code_review_versions WHERE id=?", (claim["version_id"],))
    assert version["pr_id"] != first["id"]
    service.runs.start_run.assert_awaited_once()


async def test_ci_recovery_and_repeated_failure_context_do_not_reaudit(db):
    first = await db.cr_observe("example", pr(), snapshot(failures=[]))
    failed = await db.cr_observe("example", pr(), snapshot(failures=[{"name": "unit", "summary": "failure"}], ci={"run": 1}))
    assert failed["current_version"] != first["current_version"]
    success = await db.cr_observe("example", pr(), snapshot(failures=[], ci={"run": 2, "status": "success"}))
    assert success["current_version"] == failed["current_version"]
    repeated = await db.cr_observe("example", pr(), snapshot(failures=[{"name": "unit", "summary": "failure"}], ci={"run": 3}))
    assert repeated["current_version"] == failed["current_version"]


async def test_finished_audit_retries_admission_without_new_workflow(service, db):
    await service.observe(service.profile("example"), 1)
    claim = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    directory = service.root / claim["pr_id"] / claim["id"] / "attempts" / claim["run_id"] / "work"
    directory.mkdir(parents=True)
    for name, content in complete_output().items():
        (directory / name).write_text(content)
    service.runs.get_run.return_value = {"status": "done", "spent_usd": 1}
    service.runs.settled_spend.return_value = 1
    service.github.pr.side_effect = ObservationError("429")
    await service.reconcile()
    assert (await db.cr_one("SELECT * FROM code_review_versions"))["status"] == "awaiting_validation"
    service.github.pr.side_effect = None
    await db._write("UPDATE code_review_versions SET retry_at='' WHERE status='awaiting_validation'")
    await service.reconcile()
    assert (await db.cr_one("SELECT * FROM code_review_versions"))["status"] == "complete"
    assert len(await db.cr_rows("SELECT * FROM code_review_attempts")) == 1
    service.runs.start_run.assert_not_called()


async def test_active_review_skips_queue_github_reads(service, db):
    await service.observe(service.profile("example"), 1)
    await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    service.github.pr.reset_mock()
    await service.dispatch(service.profile("example"))
    service.github.pr.assert_not_called()


async def test_severity_and_behavior_schema_cannot_hide_findings():
    output = complete_output()
    verdict = json.loads(output["verdict.json"])
    verdict["findings"] = [{"severity": "critical", "sha": "a" * 40, "path": "file.py", "line": 1,
                            "condition": "trigger", "impact": "loss", "evidence": "call"}]
    output["verdict.json"] = json.dumps(verdict)
    with pytest.raises(ValueError, match="severity"):
        validate(output, snapshot())
    verdict["severity"] = "critical"
    verdict.pop("behavior_changed")
    output["verdict.json"] = json.dumps(verdict)
    with pytest.raises(ValueError, match="boolean"):
        validate(output, snapshot())


async def test_task_title_survives_reindex(service, db):
    row = await service.observe(service.profile("example"), 1)
    service.github.pr.return_value = pr(title="Renamed PR")
    await service.observe(service.profile("example"), 1)
    from nerve.tasks.manager import TaskManager
    await TaskManager(service.config.workspace, db).reindex()
    assert "Renamed PR" in (await db.get_task(row["task_id"]))["title"]


async def test_notification_digest_and_serious_finding_dedup(service, db):
    service.notifications = SimpleNamespace(send_notification=AsyncMock())
    await service.observe(service.profile("example"), 1)
    await service.notify()  # one initial discovery summary
    claim = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    await db.cr_settle(claim["run_id"], 1, "2026-01-01", "complete", "result.json", severity="high", serious_keys=["problem"])
    await service.notify()
    assert service.notifications.send_notification.call_count == 2
    assert service.notifications.send_notification.call_args.kwargs["priority"] == "high"
    changed = pr(head={"sha": "d" * 40})
    await db.cr_observe("example", changed, snapshot(changed))
    next_claim = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    await db.cr_settle(next_claim["run_id"], 1, "2026-01-01", "complete", "next.json", severity="high", serious_keys=["problem"])
    await service.notify()
    assert service.notifications.send_notification.call_count == 2  # held for digest
    await db._write("UPDATE code_review_profiles SET last_digest_at='2020-01-01T00:00:00+00:00'")
    await service.notify()
    assert service.notifications.send_notification.call_count == 3
    assert service.notifications.send_notification.call_args.kwargs["priority"] == "normal"


async def test_real_schedule_registers_and_shutdown_cancels_owned_jobs(service):
    service.runs.add_dispatch_guard = lambda guard: None
    service.runs.remove_dispatch_guard = lambda guard: None
    await service.start()
    try:
        await asyncio.gather(*list(service._tasks))
        status = await service.status()
        assert status["profiles"][0]["registered"]
        assert status["profiles"][0]["next_run_at"]
        assert status["profiles"][0]["last_success_at"]
    finally:
        await service.stop()
    assert not service._tasks
    assert not service.started


async def test_bad_last_artifact_keeps_prior_stages(tmp_path):
    from nerve.code_review.artifacts import read_output
    (tmp_path / "description.md").write_text("Complete first stage")
    (tmp_path / "verdict.json").write_bytes(b'\xff')
    output, errors = read_output(tmp_path)
    assert output["description.md"] == "Complete first stage"
    assert errors and "verdict.json" in errors[0]


async def test_stopped_controller_guard_fails_closed(service):
    assert await service.guard_dispatch({"id": "owned", "created_by": "code-review:example"})
    service.github.pr.assert_not_called()
    assert await service.guard_dispatch({"id": "foreign", "created_by": "user"}) == ""


async def test_snapshot_directories_are_private(service):
    await service.observe(service.profile("example"), 1)
    await service.dispatch(service.profile("example"))
    import stat
    assert stat.S_IMODE(service.root.stat().st_mode) == 0o700
    for work in service.root.glob("*/*/attempts/*/work"):
        assert stat.S_IMODE(work.stat().st_mode) == 0o700


async def test_base_churn_preserves_waiting_pr_fairness(db):
    for number in (1, 2):
        await db.cr_observe("example", pr(number), snapshot(pr(number)))
    first = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    assert first["number"] == 1
    await db.cr_settle(first["run_id"], 1, "2026-01-01", "complete", "result.json")
    for number in (1, 2):
        changed = pr(number)
        changed["base"]["sha"] = "d" * 40
        await db.cr_observe("example", changed, snapshot(changed))
    second = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    assert second["number"] == 2


async def test_pending_workflow_restart_does_not_consume_review_budget(service, db):
    await service.observe(service.profile("example"), 1)
    claim = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    service.runs.get_run.return_value = {"status": "failed", "error": "interrupted by nerve restart", "started_at": None}
    await service.reconcile()
    assert (await db.cr_one("SELECT * FROM code_review_attempts"))["spent"] == 0
    await db._write("UPDATE code_review_versions SET retry_at=''")
    next_claim = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    assert next_claim["id"] == claim["id"] and next_claim["budget"] == 5


async def test_shutdown_waits_for_inflight_dispatch_guard(service, db):
    await service.observe(service.profile("example"), 1)
    claim = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    async def blocked_get(*args):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
    service.github.pr.side_effect = blocked_get
    service.started = True
    guard = asyncio.create_task(service.guard_dispatch({"id": claim["run_id"], "created_by": "code-review:example"}))
    await asyncio.wait_for(entered.wait(), 2)
    await service.stop()
    assert cancelled.is_set()
    assert await asyncio.wait_for(guard, 2)
    assert not service._tasks


async def test_awaiting_admission_does_not_hold_other_prs(service, db):
    await service.observe(service.profile("example"), 1)
    first = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    await db.cr_settle(first["run_id"], 1, "2026-01-01", "awaiting_validation", "result.json", error="403")
    await db.cr_observe("example", pr(2), snapshot(pr(2)))
    second = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    assert second["number"] == 2


async def test_migration_rerun_preserves_tasks(db):
    from nerve.db.migrations.v062_code_review import up
    await db.upsert_task("unrelated", "memory/tasks/active/unrelated.md", "Keep this task")
    await up(db.db)
    await up(db.db)
    await db.db.commit()
    assert (await db.get_task("unrelated"))["title"] == "Keep this task"


async def test_authenticated_board_pagination_and_history(service, db, monkeypatch):
    from fastapi import FastAPI
    from httpx import AsyncClient, ASGITransport
    from nerve.gateway.routes import code_review as routes
    from nerve.gateway.auth import require_auth
    app = FastAPI()
    app.include_router(routes.router)
    app.dependency_overrides[require_auth] = lambda: db.system_actor
    monkeypatch.setattr(routes, "get_deps", lambda: SimpleNamespace(db=db, engine=SimpleNamespace(config=service.config)))
    first = await service.observe(service.profile("example"), 1)
    await db.cr_observe("example", pr(2), snapshot(pr(2)))
    await db.cr_observe("example", pr(3), snapshot(pr(3)))
    await db.cr_observe("example", pr(3, state="closed"))
    async with AsyncClient(transport=ASGITransport(app), base_url="http://example.com") as client:
        page = (await client.get("/api/code-review/board?limit=1")).json()
        next_page = (await client.get("/api/code-review/board?limit=1&offset=1")).json()
        assert page["total"] == 2 and next_page["total"] == 2
        assert page["items"][0]["id"] != next_page["items"][0]["id"]
        archive = (await client.get("/api/code-review/board?column=archive")).json()
        assert archive["total"] == 1
        detail = (await client.get(f"/api/code-review/prs/{first['id']}")).json()
        assert detail["versions"][0]["head"] == "a" * 40
        version = await client.get(f"/api/code-review/prs/{first['id']}/versions/{first['current_version']}")
        assert version.status_code == 200 and version.json()["version"]["snapshot"]["head"] == "a" * 40
        assert (await client.get("/api/code-review/prs/missing")).status_code == 404
    for route in app.routes:
        if getattr(route, "path", "").startswith("/api/code-review"):
            assert any(d.call == require_auth for d in route.dependant.dependencies)


async def test_interrupted_started_run_keeps_uncertainty_separate(service, db):
    await service.observe(service.profile("example"), 1)
    claim = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    service.runs.get_run.return_value = {"status": "failed", "error": "interrupted by nerve restart", "started_at": "2026-01-01T00:00:00Z"}
    service.runs.settled_spend.return_value = 1.5
    await service.reconcile()
    attempt = await db.cr_one("SELECT * FROM code_review_attempts")
    assert attempt["spent"] == 1.5
    assert attempt["uncertain"] == 3.5
    assert await db.cr_day_spend(service.day(service.profile("example"))) == 5
    await db._write("UPDATE code_review_versions SET retry_at=''")
    assert not await db.cr_reserve("example", service.day(service.profile("example")), 5, 30, 3)


async def test_pause_resume_preserves_spend_and_disabled_config(service, db):
    await service.observe(service.profile("example"), 1)
    claim = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    await db.cr_settle(claim["run_id"], 2, "2026-01-01", "retry", "")
    await service.pause("example")
    assert not await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    service.started = True
    await service.resume("example")
    await asyncio.gather(*list(service._tasks))
    new = await db.cr_reserve("example", "2026-01-01", 5, 30, 3)
    assert new["budget"] == 3
    service.profile("example").enabled = False
    with pytest.raises(ValueError, match="reviewed configuration"):
        await service.resume("example")
    await service.stop()
