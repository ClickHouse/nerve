"""Approving a plan is one-way — so nothing may fail after the turn.

``update_plan(status="implementing")`` is what stops a plan being approved
twice, and both approval paths refuse a plan that is not ``pending``. A failure
*after* that flip therefore does not roll anything back: it leaves a plan that
claims to be under implementation, with no implementation session, no prompt,
and no way to ask for one again.

Resolving the system principal is the step that can fail — since round 1 it
raises rather than degrading — so it has to happen before the flip. That is what
these tests pin, on both paths: the HTTP route and the agent's own tool.
"""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

from nerve.agent.engine import AgentEngine
from nerve.agent.tools.handlers.plans import plan_approve_handler
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig
from nerve.gateway.routes import plans as plan_routes
from nerve.gateway.routes._deps import init_deps
from nerve.identity import Actor, ActorResolutionError
from tests.actor_rows import ensure_actor_row, ensure_system_principal


@pytest_asyncio.fixture
async def approval(db, tmp_path, monkeypatch):
    """A pending plan on a real task, with the routes and the tool wired."""
    workspace = tmp_path / "ws"
    workspace.mkdir()
    (workspace / "task.md").write_text("# Demo task\n\nBody.\n", encoding="utf-8")

    await ensure_system_principal(db)
    # The route stamps the approver on the implementation session, and
    # sessions.created_by_actor_id references actor_refs.
    await ensure_actor_row(db, _REQUESTER)
    await db.upsert_task(
        task_id="t-approve", file_path="task.md", title="Demo task",
        status="pending", content="# Demo task\n\nBody.\n",
    )
    await db.create_plan(
        plan_id="plan-1", task_id="t-approve", content="the plan",
        session_id="sess-proposer", version=1, plan_type="generic",
    )

    config = NerveConfig.from_dict({
        "workspace": str(workspace),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
    })
    engine = AgentEngine(config, db)

    async def _refuse(*args, **kwargs):
        raise RuntimeError("no model in this test")

    monkeypatch.setattr(engine, "_get_or_create_client", _refuse)
    monkeypatch.setattr("nerve.config.get_config", lambda: config)
    monkeypatch.setattr(plan_routes, "get_config", lambda: config)
    init_deps(engine, db)
    return _Approval(db, engine, config)


class _Approval:
    def __init__(self, db, engine, config):
        self.db = db
        self.engine = engine
        self.config = config

    def break_the_principal(self, monkeypatch) -> None:
        async def _gone():
            return None

        monkeypatch.setattr(self.db, "get_system_principal", _gone)

    async def plan(self) -> dict:
        return await self.db.get_plan("plan-1")

    async def task_status(self) -> str:
        return (await self.db.get_task("t-approve"))["status"]

    def ctx(self) -> ToolContext:
        return ToolContext(
            session_id="system",
            workspace=self.config.workspace,
            db=self.db,
            config=self.config,
            engine=self.engine,
        )


_REQUESTER = Actor(
    actor_id="00000000-0000-4000-8000-00000000ac70",
    kind="human",
    account_id="00000000-0000-4000-8000-00000000acc7",
    display_name="Test Account",
)


@pytest.mark.asyncio
class TestTheRouteLeavesAnApprovablePlanAlone:
    async def test_a_failed_lookup_changes_nothing(self, approval, monkeypatch):
        approval.break_the_principal(monkeypatch)

        with pytest.raises(ActorResolutionError):
            await plan_routes.approve_plan("plan-1", actor=_REQUESTER)

        plan = await approval.plan()
        assert plan["status"] == "pending", "the plan was stranded"
        assert not plan["impl_session_id"]
        assert await approval.task_status() == "pending"

    async def test_and_the_next_attempt_still_works(self, approval, monkeypatch):
        approval.break_the_principal(monkeypatch)
        with pytest.raises(ActorResolutionError):
            await plan_routes.approve_plan("plan-1", actor=_REQUESTER)

        monkeypatch.undo()
        result = await plan_routes.approve_plan("plan-1", actor=_REQUESTER)
        await asyncio.sleep(0)

        assert result["impl_session_id"]
        plan = await approval.plan()
        assert plan["status"] == "implementing"
        assert plan["impl_session_id"] == result["impl_session_id"]
        # The session the person caused is theirs; the prompt nobody typed is
        # the instance's (asserted in test_attribution.py).
        session = await approval.db.get_session(result["impl_session_id"])
        assert session["created_by_actor_id"] == _REQUESTER.actor_id


@pytest.mark.asyncio
class TestTheToolLeavesAnApprovablePlanAlone:
    async def test_a_failed_lookup_changes_nothing(self, approval, monkeypatch):
        approval.break_the_principal(monkeypatch)

        with pytest.raises(ActorResolutionError):
            await plan_approve_handler(approval.ctx(), {"plan_id": "plan-1"})

        plan = await approval.plan()
        assert plan["status"] == "pending", "the plan was stranded"
        assert not plan["impl_session_id"]
        assert await approval.task_status() == "pending"

    async def test_and_the_next_attempt_still_works(self, approval, monkeypatch):
        approval.break_the_principal(monkeypatch)
        with pytest.raises(ActorResolutionError):
            await plan_approve_handler(approval.ctx(), {"plan_id": "plan-1"})

        monkeypatch.undo()
        result = await plan_approve_handler(approval.ctx(), {"plan_id": "plan-1"})
        await asyncio.sleep(0)

        plan = await approval.plan()
        assert plan["status"] == "implementing"
        assert plan["impl_session_id"]
        assert "approved" in result.content[0]["text"].lower()
        # The agent approved it through its own tool, so the session is the
        # instance's own work rather than any person's.
        session = await approval.db.get_session(plan["impl_session_id"])
        identity = await approval.db.get_local_identity()
        assert session["created_by_actor_id"] == identity.system_actor_id
