"""Tests for the per-cron-job spend breakdown on the diagnostics page.

``UsageStore.get_usage_by_cron_job`` attributes each ``session_usage`` row
to the cron job whose session produced it, and the diagnostics route
exposes the result as ``usage.by_cron_job``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import pytest_asyncio

from nerve.db import Database


async def _turn(
    db: Database, session_id: str, cost: float, *,
    source: str = "cron", tokens: int = 100, days_ago: int | None = None,
) -> None:
    """Record one turn of usage in ``session_id`` (session made on demand)."""
    if await db.get_session(session_id) is None:
        await db.create_session(session_id, source=source, actor=None)
    await db.record_turn_usage(
        session_id=session_id,
        input_tokens=tokens,
        output_tokens=tokens // 2,
        cache_creation=0,
        cache_read=0,
        max_context=200_000,
        cost_usd=cost,
        estimated_cost_usd=cost,
    )
    if days_ago is not None:
        await db.db.execute(
            "UPDATE session_usage SET created_at = datetime('now', ?) "
            "WHERE id = (SELECT MAX(id) FROM session_usage)",
            (f"-{days_ago} days",),
        )
        await db.db.commit()


async def _runs(
    db: Database, job_id: str, n: int, *,
    days_ago: int | None = None, session_id: str | None = None,
):
    """Log ``n`` runs of ``job_id`` that started agent work.

    Real runs link their log row to a session before doing any work; the
    session id defaults to a per-run ``cron:<job>:run<i>``.
    """
    log_ids = []
    for i in range(n):
        log_id = await db.log_cron_start(job_id)
        await db.set_cron_log_session(log_id, session_id or f"cron:{job_id}:run{i}")
        log_ids.append(log_id)
    if days_ago is not None:
        # Backdate only after every write: an open raw transaction is rolled
        # back by the next ``_write``.
        for log_id in log_ids:
            await db.db.execute(
                "UPDATE cron_logs SET started_at = datetime('now', ?) WHERE id = ?",
                (f"-{days_ago} days", log_id),
            )
        await db.db.commit()


async def _workflow_run(db: Database, run_id: str, created_by: str) -> str:
    """A workflow run linked to its ``workflow:<run>`` session."""
    session_id = f"workflow:{run_id}"
    await db.create_session(session_id, source="workflow", actor=None)
    await db.create_workflow_run(
        run_id, "claude-workflow", {"prompt": "x"}, 5.0, created_by=created_by,
    )
    await db.update_workflow_run(run_id, {"session_id": session_id})
    return session_id


def _by_job(rows: list[dict]) -> dict[str, dict]:
    return {r["job_id"]: r for r in rows}


@pytest.mark.asyncio
class TestUsageByCronJob:
    async def test_empty(self, db: Database):
        assert await db.get_usage_by_cron_job() == []

    async def test_isolated_runs_roll_up_per_job(self, db: Database):
        await _turn(db, "cron:alpha:20260101-100000", 0.25)
        await _turn(db, "cron:alpha:20260101-110000", 0.50)
        await _turn(db, "cron:alpha:20260101-110000", 0.25)  # 2nd turn, same run
        await _runs(db, "alpha", 2)

        rows = await db.get_usage_by_cron_job()
        assert len(rows) == 1
        row = rows[0]
        assert row["job_id"] == "alpha"
        assert row["runs"] == 2
        assert row["sessions"] == 2
        assert row["turns"] == 3
        assert row["input_tokens"] == 300
        assert row["output_tokens"] == 150
        assert row["cost_usd"] == pytest.approx(1.0)
        assert row["estimated_cost_usd"] == pytest.approx(1.0)

    async def test_persistent_legacy_and_generation_sessions(self, db: Database):
        # Legacy stable id and a rotated generation chat both belong to "beta".
        await _turn(db, "cron:beta", 1.0)
        await _turn(db, "cron:beta:20260102-000000", 2.0)

        row = _by_job(await db.get_usage_by_cron_job())["beta"]
        assert row["sessions"] == 2
        assert row["cost_usd"] == pytest.approx(3.0)

    async def test_job_id_containing_colons(self, db: Database):
        await _turn(db, "cron:team:deploy:20260103-120000", 0.5)
        await _turn(db, "cron:team:deploy", 0.5)
        # A trailing segment that is not a run timestamp is part of the id.
        await _turn(db, "cron:team:deploy:v2", 0.1)

        rows = _by_job(await db.get_usage_by_cron_job())
        assert set(rows) == {"team:deploy", "team:deploy:v2"}
        assert rows["team:deploy"]["sessions"] == 2
        assert rows["team:deploy"]["cost_usd"] == pytest.approx(1.0)

    async def test_job_id_with_glob_metacharacters(self, db: Database):
        # The job id is data, never part of a GLOB pattern.
        await _turn(db, "cron:[x]*?:20260103-120000", 0.5)

        rows = await db.get_usage_by_cron_job()
        assert [r["job_id"] for r in rows] == ["[x]*?"]

    async def test_workflow_run_started_by_cron_job(self, db: Database):
        cron_run = await _workflow_run(db, "wfr-cron", "cron:gamma")
        await _turn(db, cron_run, 4.0)

        # A run a user started is not cron spend.
        user_run = await _workflow_run(db, "wfr-user", "session:abc")
        await _turn(db, user_run, 9.0)

        await _turn(db, "cron:gamma:20260104-000000", 1.0)

        await _runs(db, "gamma", 1, session_id=cron_run)

        rows = _by_job(await db.get_usage_by_cron_job())
        assert set(rows) == {"gamma"}
        assert rows["gamma"]["sessions"] == 2
        assert rows["gamma"]["runs"] == 1
        assert rows["gamma"]["cost_usd"] == pytest.approx(5.0)

    async def test_workflow_run_started_from_a_cron_session(self, db: Database):
        # The agent of a cron run called workflow_run_start: the run's
        # created_by names the cron session it came from.
        run = await _workflow_run(
            db, "wfr-tool", "session:cron:team:deploy:20260104-000000",
        )
        await _turn(db, run, 2.5)
        # A run started from a non-cron chat stays unattributed.
        other = await _workflow_run(db, "wfr-web", "session:web-1")
        await _turn(db, other, 9.0)

        rows = _by_job(await db.get_usage_by_cron_job())
        assert set(rows) == {"team:deploy"}
        assert rows["team:deploy"]["cost_usd"] == pytest.approx(2.5)

    async def test_non_cron_sessions_excluded(self, db: Database):
        await _turn(db, "web-session", 7.0, source="web")
        await _turn(db, "hook:something:1", 3.0, source="hook")
        await _turn(db, "cron:", 2.0)  # no job id at all

        assert await db.get_usage_by_cron_job() == []

    async def test_window_applies_to_usage_and_runs(self, db: Database):
        await _turn(db, "cron:delta:20260105-000000", 1.0)
        await _turn(db, "cron:delta:20250105-000000", 50.0, days_ago=30)
        await _runs(db, "delta", 3)
        await _runs(db, "delta", 4, days_ago=30)

        row = _by_job(await db.get_usage_by_cron_job(days=7))["delta"]
        assert row["turns"] == 1
        assert row["runs"] == 3
        assert row["cost_usd"] == pytest.approx(1.0)

    async def test_runs_zero_without_logs_and_jobs_without_usage_omitted(
        self, db: Database,
    ):
        # Usage in a cron chat with no run log in the window (e.g. a wakeup
        # on an old persistent session).
        await _turn(db, "cron:epsilon", 0.3)
        # Logged runs (a source runner, a job that never reached a turn) but
        # no usage → not a spend row.
        await _runs(db, "source:github", 5)
        await _runs(db, "zeta", 2)

        rows = await db.get_usage_by_cron_job()
        assert [r["job_id"] for r in rows] == ["epsilon"]
        assert rows[0]["runs"] == 0

    async def test_runs_ignore_logs_without_agent_work(self, db: Database):
        await _turn(db, "cron:eta:20260105-000000", 0.6)
        await _runs(db, "eta", 2)
        # A run the scheduler dropped, and a launch skipped by the job lock
        # (logged, but never linked to a session).
        due = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        await db.log_cron_missed("eta", due)
        skipped = await db.log_cron_start("eta")
        await db.log_cron_finish(skipped, "success", output="skipped (lock)")

        row = _by_job(await db.get_usage_by_cron_job())["eta"]
        assert row["runs"] == 2

    async def test_sorted_by_cost_descending(self, db: Database):
        await _turn(db, "cron:cheap:20260106-000000", 0.1)
        await _turn(db, "cron:pricey:20260106-000000", 9.0)
        await _turn(db, "cron:middle:20260106-000000", 1.0)

        rows = await db.get_usage_by_cron_job()
        assert [r["job_id"] for r in rows] == ["pricey", "middle", "cheap"]

    async def test_matches_cron_source_total(self, db: Database):
        """Every turn in a ``cron`` session lands in exactly one job row."""
        await _turn(db, "cron:a:20260107-000000", 0.4)
        await _turn(db, "cron:b", 0.6)
        await _turn(db, "cron:b:20260107-000000", 1.1)
        await _turn(db, "web-1", 5.0, source="web")

        by_job = await db.get_usage_by_cron_job()
        by_source = {r["source"]: r for r in await db.get_usage_by_source()}
        assert sum(r["cost_usd"] for r in by_job) == pytest.approx(
            by_source["cron"]["cost_usd"],
        )
        assert sum(r["turns"] for r in by_job) == by_source["cron"]["turns"]


@pytest.mark.asyncio
class TestDiagnosticsRoute:
    @pytest_asyncio.fixture
    async def client(self, db: Database, bypass_auth, tmp_path):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        import nerve.config as cfg_mod
        from nerve.config import NerveConfig
        from nerve.gateway.routes._deps import init_deps
        from nerve.gateway.routes.diagnostics import router

        cfg_mod._config = NerveConfig(workspace=tmp_path)
        init_deps(engine=SimpleNamespace(), db=db)  # type: ignore[arg-type]
        app = FastAPI()
        app.include_router(router)
        bypass_auth(app)
        yield TestClient(app)
        cfg_mod._config = None

    async def test_by_cron_job_in_payload(self, client, db: Database):
        await _turn(db, "cron:alpha:20260108-000000", 0.75)
        await _runs(db, "alpha", 1)

        usage = client.get("/api/diagnostics").json()["usage"]
        assert [r["job_id"] for r in usage["by_cron_job"]] == ["alpha"]
        assert usage["by_cron_job"][0]["runs"] == 1
        assert usage["by_cron_job"][0]["cost_usd"] == pytest.approx(0.75)

    async def test_breakdown_failure_keeps_rest_of_usage(
        self, client, db: Database, monkeypatch,
    ):
        await _turn(db, "cron:alpha:20260108-000000", 0.75)

        async def boom(days: int = 7):
            raise RuntimeError("query failed")

        monkeypatch.setattr(db, "get_usage_by_cron_job", boom)

        usage = client.get("/api/diagnostics").json()["usage"]
        assert usage["by_cron_job"] == []
        assert usage["by_source"][0]["source"] == "cron"
        assert usage["last_7d"]["turns"] == 1
