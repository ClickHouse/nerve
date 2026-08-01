"""Tests for review loops — deterministic implement→verify cycles.

Covers:
  * pure helpers — criteria derivation, verdict parsing, the pass gate;
  * ``ReviewLoopStore`` — CAS transitions, the dispatch outbox
    (``begin_leg_attempt``), settle idempotency, spend summing;
  * ``ReviewLoopService`` end-to-end over a REAL ``WorkflowRunService``
    with a fake engine — pass on iteration 1, iterate-then-pass with
    feedback, verifier schema-repair retry, auto criteria adoption,
    budget escalation + decisions (accept/iterate/stale), loop kill,
    restart recovery (done-but-unprocessed leg, crash-before-dispatch);
  * ``review_loop_*`` tool handlers — direct ToolContext invocation.

Legs execute instantly (``engine.run`` is an AsyncMock whose reply is
keyed on the leg role marker in the composed prompt); tests synchronize
by polling loop status with a bounded deadline — never by sleeping fixed
amounts.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from nerve.agent.tools.handlers.review_loops import (
    review_loop_kill_handler,
    review_loop_list_handler,
    review_loop_start_handler,
    review_loop_status_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig
from nerve.workflows.review_loop import (
    ReviewLoopError,
    ReviewLoopService,
    derive_criteria,
    extract_verdict_json,
    gate_verdict,
)
from nerve.workflows.service import ENGINE_CLAUDE, WorkflowRunService

# --------------------------------------------------------------------------- #
#  Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _make_config(tmp_path: Path) -> NerveConfig:
    cfg = NerveConfig()
    # Isolate the workspace: loops default legs' cwd to config.workspace,
    # which start_run validates as a real directory — the default
    # ~/nerve-workspace exists on dev machines but NOT on CI runners
    # (every dispatch would fail as 'cwd is not a directory').
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    cfg.workspace = workspace
    cfg.workflows.runs_dir = tmp_path / "runs"
    cfg.workflows.kill_grace_seconds = 0
    rl = cfg.workflows.review_loop
    rl.implementer.engine = ENGINE_CLAUDE
    rl.verifier.engine = ENGINE_CLAUDE  # avoid codex preflight in tests
    rl.default_budget_usd = 5.0
    rl.min_leg_budget_usd = 0.05
    return cfg


def _verdict_text(
    verdict: str = "pass",
    criteria: list | None = None,
    proposals: list | None = None,
    findings: list | None = None,
    gamed: bool = False,
    summary: str = "looks solid",
) -> str:
    payload = {
        "verdict": verdict,
        "summary": summary,
        "criteria": criteria if criteria is not None else [],
        "findings": findings or [],
        "proposed_criteria": proposals or [],
        "gamed": gamed,
    }
    return "verification report\n```json\n" + json.dumps(payload) + "\n```"


def _met(*ids: str) -> list[dict]:
    return [
        {"id": i, "status": "met", "evidence": f"ran check for {i} -> ok"}
        for i in ids
    ]


def _make_engine(db) -> MagicMock:
    """Fake AgentEngine; ``run`` replies per leg role (prompt marker)."""
    engine = MagicMock()

    async def _get_or_create(session_id: str, **kwargs):
        await db.create_session(
            session_id,
            title=kwargs.get("title"),
            source=kwargs.get("source", "web"),
            backend=kwargs.get("backend", "claude"),
            model=kwargs.get("model"),
            cwd=kwargs.get("cwd"),
        )
        return {"id": session_id}

    engine.sessions.get_or_create = AsyncMock(side_effect=_get_or_create)
    # Verifier replies pop from this list (last entry repeats).
    engine._verifier_replies = [
        _verdict_text("pass", criteria=_met("C1")),
    ]

    async def _run(session_id=None, user_message="", **kwargs):
        if "independent VERIFIER" in user_message:
            replies = engine._verifier_replies
            return replies.pop(0) if len(replies) > 1 else replies[0]
        return "implementation report: did the thing, self-verified."

    engine.run = AsyncMock(side_effect=_run)
    engine.register_task = MagicMock()
    engine.stop_session = AsyncMock(return_value=True)
    engine._discard_client = AsyncMock()
    engine.is_session_running = MagicMock(return_value=False)
    engine.get_live_workflow_tokens = MagicMock(return_value=0)
    engine.has_live_background_tasks = MagicMock(return_value=False)
    engine.get_codex_ultracode_run_ids = MagicMock(return_value=None)
    engine.add_stop_listener = MagicMock()
    engine.notification_service = MagicMock()
    engine.notification_service.send_notification = AsyncMock()
    engine.notification_service.propose_action = AsyncMock(
        return_value={"notification_id": "approval-test", "status": "sent"},
    )
    return engine


async def _wait_status(db, loop_id: str, statuses: tuple[str, ...], timeout: float = 8.0) -> dict:
    deadline = time.monotonic() + timeout
    loop = None
    while time.monotonic() < deadline:
        loop = await db.get_review_loop(loop_id)
        if loop and loop["status"] in statuses:
            return loop
        await asyncio.sleep(0.02)
    raise AssertionError(
        f"loop {loop_id} never reached {statuses}; last: "
        f"{(loop or {}).get('status')} / {(loop or {}).get('failure_reason')}"
    )


async def _wait_awaited(mock, timeout: float = 8.0) -> None:
    """Wait for an AsyncMock to be awaited. The status CAS flips BEFORE the
    decision card is proposed (single-winner transition; a card death in the
    gap is covered by the reconcile tick), so tests that observe the parked
    status must not assert the card synchronously — that's a race on slow
    runners."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if mock.await_count:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("mock was never awaited")


@pytest_asyncio.fixture
async def engine(db):
    return _make_engine(db)


@pytest_asyncio.fixture
async def services(db, engine, tmp_path):
    """(runs service, review loop service) — started, torn down in order."""
    cfg = _make_config(tmp_path)
    runs = WorkflowRunService(cfg, db, engine)
    rls = ReviewLoopService(lambda: cfg, db, engine, runs)
    runs.add_completion_listener(rls._on_run_terminal)
    rls._worker_task = asyncio.create_task(rls._worker())
    yield runs, rls
    await rls.stop()
    # drain runs exec/kill tasks
    deadline = time.monotonic() + 5
    while runs._exec_tasks or runs._kill_tasks:
        tasks = list(runs._exec_tasks.values()) + list(runs._kill_tasks)
        await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=max(0.1, deadline - time.monotonic()),
        )
    for task in list(rls._detached):
        task.cancel()


async def _mk_observer(db, session_id: str = "obs00001") -> str:
    await db.create_session(session_id, source="web")
    return session_id


# --------------------------------------------------------------------------- #
#  Pure helpers                                                                #
# --------------------------------------------------------------------------- #


class TestHelpers:
    def test_derive_criteria_bullets(self):
        crit = derive_criteria("- file exists\n* tests pass\n2) build is green\nprose")
        assert [c["id"] for c in crit] == ["C1", "C2", "C3"]
        assert crit[1]["statement"] == "tests pass"
        assert all(c["source"] == "user" for c in crit)

    def test_derive_criteria_prose_is_single(self):
        crit = derive_criteria("everything works\nno bullets here")
        assert len(crit) == 1 and crit[0]["id"] == "C1"

    def test_extract_verdict_fenced(self):
        v, err = extract_verdict_json(_verdict_text("needs_improvement", criteria=[
            {"id": "C1", "status": "unmet", "evidence": "", "fix_hint": "do X"},
        ]))
        assert err == "" and v["verdict"] == "needs_improvement"
        assert v["criteria"][0]["fix_hint"] == "do X"

    def test_extract_verdict_bare_tail(self):
        v, err = extract_verdict_json(
            'text\n{"verdict": "pass", "criteria": [], "summary": "s"}',
        )
        assert v is not None and v["verdict"] == "pass"

    def test_extract_verdict_rejects_garbage(self):
        assert extract_verdict_json("")[0] is None
        assert extract_verdict_json("no json here")[0] is None
        assert extract_verdict_json("```json\n{not json}\n```")[0] is None
        v, err = extract_verdict_json('```json\n{"verdict": "sure!"}\n```')
        assert v is None and "verdict must be one of" in err

    def test_extract_normalizes_proposals_and_findings(self):
        text = _verdict_text(
            "pass", criteria=_met("C1"),
            proposals=[{"statement": " check  nulls ", "severity": "BLOCKING"},
                       {"statement": ""}],
            findings=[{"title": "t", "priority": "0"}, "junk"],
        )
        v, _ = extract_verdict_json(text)
        assert v["proposed_criteria"] == [{
            "statement": "check nulls", "rationale": "", "severity": "advisory",
        }] or v["proposed_criteria"][0]["severity"] in ("advisory", "blocking")
        assert v["findings"][0]["priority"] == 0

    def test_gate_requires_every_known_id_with_evidence(self):
        crit = derive_criteria("- a\n- b")
        v, _ = extract_verdict_json(_verdict_text("pass", criteria=_met("C1")))
        ok, reasons = gate_verdict(v, crit)
        assert not ok and any("C2" in r for r in reasons)
        v2, _ = extract_verdict_json(_verdict_text("pass", criteria=[
            *_met("C1"), {"id": "C2", "status": "met", "evidence": ""},
        ]))
        ok2, reasons2 = gate_verdict(v2, crit)
        assert not ok2 and any("without evidence" in r for r in reasons2)

    def test_gate_blocks_p0_and_gamed(self):
        crit = derive_criteria("- a")
        v, _ = extract_verdict_json(_verdict_text(
            "pass", criteria=_met("C1"),
            findings=[{"title": "broken", "priority": 1}],
        ))
        ok, reasons = gate_verdict(v, crit)
        assert not ok and any("P1" in r for r in reasons)
        vg, _ = extract_verdict_json(_verdict_text("pass", criteria=_met("C1"), gamed=True))
        okg, rg = gate_verdict(vg, crit)
        assert not okg and any("gaming" in r for r in rg)

    def test_gate_passes_clean_verdict(self):
        crit = derive_criteria("- a\n- b")
        v, _ = extract_verdict_json(_verdict_text(
            "pass", criteria=_met("C1", "C2"),
            findings=[{"title": "nit", "priority": 3}],
        ))
        ok, reasons = gate_verdict(v, crit)
        assert ok, reasons


# --------------------------------------------------------------------------- #
#  Store                                                                       #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestReviewLoopStore:
    async def _mk_loop(self, db, loop_id="rvl-00000001", session_id=None):
        return await db.create_review_loop(
            loop_id, title="t", session_id=session_id,
            goal_prompt="g", verifier_prompt="- a",
            criteria_adoption="no", criteria=derive_criteria("- a"),
            implementer={"engine": ENGINE_CLAUDE}, verifier={"engine": ENGINE_CLAUDE},
            cwd=None, max_iterations=3, budget_usd=5.0,
        )

    async def test_create_get_roundtrip(self, db):
        loop = await self._mk_loop(db)
        assert loop["status"] == "pending"
        assert loop["criteria"][0]["id"] == "C1"      # parsed, not JSON text
        assert loop["implementer"]["engine"] == ENGINE_CLAUDE

    async def test_cas_transitions(self, db):
        loop = await self._mk_loop(db)
        assert await db.transition_review_loop(
            loop["id"], "implementing", expect=("pending",),
        )
        # duplicate signal loses
        assert not await db.transition_review_loop(
            loop["id"], "implementing", expect=("pending",),
        )
        assert await db.transition_review_loop(
            loop["id"], "killed", expect=("pending", "implementing", "verifying"),
            failure_reason="user",
        )
        fresh = await db.get_review_loop(loop["id"])
        assert fresh["status"] == "killed" and fresh["ended_at"]

    async def test_begin_leg_attempt_outbox(self, db):
        loop = await self._mk_loop(db)
        attempt = await db.begin_leg_attempt(
            loop["id"], "implementing", ("pending",),
            iteration=1, role="implementer", run_id="wfr-11111111",
        )
        assert attempt is not None and attempt["attempt_no"] == 1
        fresh = await db.get_review_loop(loop["id"])
        assert fresh["status"] == "implementing"
        assert fresh["current_run_id"] == "wfr-11111111"
        # CAS lost -> no attempt row inserted
        lost = await db.begin_leg_attempt(
            loop["id"], "implementing", ("pending",),
            iteration=1, role="implementer", run_id="wfr-22222222",
        )
        assert lost is None
        assert await db.get_attempt_by_run("wfr-22222222") is None
        # retry in the same iteration increments attempt_no
        second = await db.begin_leg_attempt(
            loop["id"], "implementing", ("implementing",),
            iteration=1, role="implementer", run_id="wfr-33333333",
        )
        assert second["attempt_no"] == 2

    async def test_settle_idempotent_and_spend_sum(self, db):
        loop = await self._mk_loop(db)
        await db.begin_leg_attempt(
            loop["id"], "implementing", ("pending",),
            iteration=1, role="implementer", run_id="wfr-aaaa1111",
        )
        assert await db.settle_loop_attempt("wfr-aaaa1111", "done", spend_usd=1.25)
        assert not await db.settle_loop_attempt("wfr-aaaa1111", "done", spend_usd=99)
        assert await db.sum_loop_spend(loop["id"]) == pytest.approx(1.25)


# --------------------------------------------------------------------------- #
#  Service end-to-end                                                          #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestReviewLoopService:
    async def test_pass_first_iteration(self, db, engine, services):
        runs, rls = services
        obs = await _mk_observer(db)
        loop = await rls.create_loop(
            goal="make it work", verifier="- it works", session_id=obs,
        )
        done = await _wait_status(db, loop["id"], ("passed",))
        assert done["iteration"] == 1
        attempts = await db.list_loop_attempts(loop["id"])
        roles = [(a["role"], a["status"]) for a in attempts]
        assert ("implementer", "done") in roles and ("verifier", "done") in roles
        verdict = [a for a in attempts if a["role"] == "verifier"][0]["verdict"]
        assert verdict["verdict"] == "pass"
        # milestones landed in the observer session
        messages = await db.get_messages(obs, limit=50)
        assert any("PASSED" in (m.get("content") or "") for m in messages)

    async def test_iterate_then_pass_with_feedback(self, db, engine, services):
        runs, rls = services
        obs = await _mk_observer(db)
        engine._verifier_replies = [
            _verdict_text("needs_improvement", criteria=[
                {"id": "C1", "status": "met", "evidence": "checked"},
                {"id": "C2", "status": "unmet", "evidence": "ran test -> fail",
                 "fix_hint": "handle empty input"},
            ]),
            _verdict_text("pass", criteria=_met("C1", "C2")),
        ]
        loop = await rls.create_loop(
            goal="do both", verifier="- part one\n- part two", session_id=obs,
        )
        done = await _wait_status(db, loop["id"], ("passed",))
        assert done["iteration"] == 2
        # iteration-2 implementer prompt carried the verifier feedback
        impl_calls = [
            c.kwargs.get("user_message") or (c.args[1] if len(c.args) > 1 else "")
            for c in engine.run.call_args_list
            if "You are the IMPLEMENTER" in str(c.kwargs.get("user_message") or "")
        ]
        assert len(impl_calls) == 2
        assert "handle empty input" in impl_calls[1]
        assert "UNVERIFIED" in impl_calls[1] or "LATEST VERIFIER FEEDBACK" in impl_calls[1]

    async def test_verifier_schema_repair_retry(self, db, engine, services):
        runs, rls = services
        obs = await _mk_observer(db)
        engine._verifier_replies = [
            "no json at all, oops",
            _verdict_text("pass", criteria=_met("C1")),
        ]
        loop = await rls.create_loop(goal="g", verifier="- a", session_id=obs)
        done = await _wait_status(db, loop["id"], ("passed",))
        attempts = await db.list_loop_attempts(done["id"])
        verifier_attempts = [a for a in attempts if a["role"] == "verifier"]
        assert len(verifier_attempts) == 2
        assert verifier_attempts[0]["attempt_no"] == 1
        assert verifier_attempts[1]["attempt_no"] == 2

    async def test_auto_adoption_blocks_pass_until_met(self, db, engine, services):
        runs, rls = services
        obs = await _mk_observer(db)
        engine._verifier_replies = [
            # Claims C1 met but discovers a blocking gap -> adopted as D2.
            _verdict_text("pass", criteria=_met("C1"), proposals=[
                {"statement": "nulls are handled", "rationale": "dataset has nulls",
                 "severity": "blocking"},
            ]),
            _verdict_text("pass", criteria=_met("C1", "D2")),
        ]
        loop = await rls.create_loop(
            goal="research it", verifier="- coverage is complete",
            session_id=obs, criteria_adoption="auto",
        )
        done = await _wait_status(db, loop["id"], ("passed",))
        ids = [c["id"] for c in done["criteria"]]
        assert ids == ["C1", "D2"]
        assert done["criteria"][1]["source"] == "verifier"
        assert done["iteration"] == 2  # adoption forced a second iteration

    async def test_gamed_verdict_escalates(self, db, engine, services):
        runs, rls = services
        obs = await _mk_observer(db)
        engine._verifier_replies = [
            _verdict_text("pass", criteria=_met("C1"), gamed=True),
        ]
        loop = await rls.create_loop(goal="g", verifier="- a", session_id=obs)
        parked = await _wait_status(db, loop["id"], ("awaiting_user",))
        assert parked["failure_reason"] == "gamed"
        await _wait_awaited(engine.notification_service.propose_action)

    async def test_budget_too_low_escalates_and_accept_decision(self, db, engine, services):
        runs, rls = services
        obs = await _mk_observer(db)
        loop = await rls.create_loop(
            goal="g", verifier="- a", session_id=obs, budget_usd=0.04,
        )
        parked = await _wait_status(db, loop["id"], ("awaiting_user",))
        assert parked["failure_reason"] == "budget"
        result = await rls.handle_decision(loop["id"], "accept", decided_by="test")
        assert result["ok"]
        fresh = await db.get_review_loop(loop["id"])
        assert fresh["status"] == "passed"
        # stale decision after terminal
        stale = await rls.handle_decision(loop["id"], "abandon")
        assert not stale["ok"] and "not applied" in stale["message"]

    async def test_iterate_decision_resumes(self, db, engine, services):
        runs, rls = services
        obs = await _mk_observer(db)
        engine._verifier_replies = [
            _verdict_text("needs_improvement", criteria=[
                {"id": "C1", "status": "unmet", "evidence": "nope", "fix_hint": "fix"},
            ]),
            _verdict_text("pass", criteria=_met("C1")),
        ]
        loop = await rls.create_loop(
            goal="g", verifier="- a", session_id=obs, max_iterations=1,
        )
        parked = await _wait_status(db, loop["id"], ("awaiting_user",))
        assert parked["failure_reason"] == "max_iterations"
        result = await rls.handle_decision(loop["id"], "iterate:2")
        assert result["ok"], result
        done = await _wait_status(db, loop["id"], ("passed",))
        assert done["iteration"] == 2 and done["max_iterations"] == 3

    async def test_budget_grant_decision_resumes(self, db, engine, services):
        runs, rls = services
        obs = await _mk_observer(db, "obs00b01")
        loop = await rls.create_loop(
            goal="g", verifier="- a", session_id=obs, budget_usd=0.04,
        )
        parked = await _wait_status(db, loop["id"], ("awaiting_user",))
        assert parked["failure_reason"] == "budget"
        result = await rls.handle_decision(loop["id"], "budget:5")
        assert result["ok"], result
        done = await _wait_status(db, loop["id"], ("passed",))
        assert done["budget_usd"] == pytest.approx(5.04)

    async def test_iterate_grant_never_reduces_cap(self, db, engine, services):
        runs, rls = services
        obs = await _mk_observer(db, "obs00b02")
        loop = await rls.create_loop(
            goal="g", verifier="- a", session_id=obs,
            budget_usd=0.04, max_iterations=4,
        )
        await _wait_status(db, loop["id"], ("awaiting_user",))
        # Iterating a budget-parked loop re-parks on budget — but must
        # never REDUCE the configured iteration cap (iteration=0, +2 → 2 < 4).
        await rls.handle_decision(loop["id"], "iterate:2")
        parked = await _wait_status(db, loop["id"], ("awaiting_user",))
        assert parked["max_iterations"] == 4

    async def test_budget_resume_redispatches_dead_verifier(self, db, engine, services):
        """An implementation that was never verified resumes at the
        VERIFIER after a budget grant — not at a wasted new iteration."""
        runs, rls = services
        obs = await _mk_observer(db, "obs00b03")
        loop = await rls.create_loop(
            goal="g", verifier="- a", session_id=obs,
            budget_usd=5.0, autostart=False,
        )
        await db.begin_leg_attempt(
            loop["id"], "implementing", ("pending",),
            iteration=1, role="implementer", run_id="wfr-rr000001",
        )
        await db.settle_loop_attempt("wfr-rr000001", "done", spend_usd=1.0)
        await db.begin_leg_attempt(
            loop["id"], "verifying", ("implementing",),
            iteration=1, role="verifier", run_id="wfr-rr000002",
        )
        await db.settle_loop_attempt("wfr-rr000002", "budget_exhausted", spend_usd=4.0)
        await db.transition_review_loop(
            loop["id"], "awaiting_user", expect=("verifying",),
            failure_reason="budget", spent_usd=5.0,
        )
        result = await rls.handle_decision(loop["id"], "budget:10")
        assert result["ok"], result
        assert "verifier" in result["message"]
        done = await _wait_status(db, loop["id"], ("passed",))
        verifier_attempts = [
            a for a in await db.list_loop_attempts(done["id"])
            if a["role"] == "verifier"
        ]
        assert len(verifier_attempts) == 2
        assert done["iteration"] == 1  # no wasted implementer iteration

    async def test_decisions_resolve_pending_cards(self, db, engine, services):
        runs, rls = services
        obs = await _mk_observer(db, "obs00b04")
        loop = await rls.create_loop(
            goal="g", verifier="- a", session_id=obs, budget_usd=0.04,
        )
        await _wait_status(db, loop["id"], ("awaiting_user",))
        await db.create_notification(
            notification_id="approval-rvltest1",
            session_id=obs, type="approval", title="t", body="b",
            priority="high", options=["accept"], expires_at=None,
            metadata={"target_kind": "review-loop", "target_id": loop["id"]},
            target_kind="review-loop", target_id=loop["id"],
        )
        result = await rls.handle_decision(loop["id"], "accept", decided_by="test")
        assert result["ok"]
        row = await db.get_notification("approval-rvltest1")
        assert row["status"] != "pending"

    async def test_default_codex_verifier_falls_back_to_claude(self, db, engine, tmp_path):
        cfg = _make_config(tmp_path)
        cfg.workflows.review_loop.verifier.engine = "codex-ultracode"
        runs = WorkflowRunService(cfg, db, engine)
        rls = ReviewLoopService(lambda: cfg, db, engine, runs)
        engine._backends = {}  # codex not configured
        loop = await rls.create_loop(
            goal="g", verifier="- a", session_id=None, autostart=False,
        )
        assert loop["verifier"]["engine"] == ENGINE_CLAUDE
        # An EXPLICITLY requested codex leg still fails loud.
        with pytest.raises(ReviewLoopError):
            await rls.create_loop(
                goal="g", verifier="- a", session_id=None, autostart=False,
                verifier_leg={"engine": "codex-ultracode"},
            )

    async def test_remind_decision_snoozes_without_state_change(self, db, engine, services):
        runs, rls = services
        obs = await _mk_observer(db)
        loop = await rls.create_loop(
            goal="g", verifier="- a", session_id=obs, budget_usd=0.04,
        )
        await _wait_status(db, loop["id"], ("awaiting_user",))
        result = await rls.handle_decision(loop["id"], "remind_4h")
        assert result["ok"] and result.get("snooze_until")
        assert (await db.get_review_loop(loop["id"]))["status"] == "awaiting_user"

    async def test_kill_loop_kills_current_leg(self, db, engine, services):
        runs, rls = services
        obs = await _mk_observer(db)

        started = asyncio.Event()
        release = asyncio.Event()

        async def _slow_run(session_id=None, user_message="", **kwargs):
            started.set()
            await release.wait()
            return "late"

        engine.run = AsyncMock(side_effect=_slow_run)
        loop = await rls.create_loop(goal="g", verifier="- a", session_id=obs)
        await asyncio.wait_for(started.wait(), timeout=5)
        killed = await rls.kill_loop(loop["id"], reason="test abort")
        assert killed["status"] == "killed"
        release.set()
        run = await db.get_workflow_run(killed["current_run_id"])
        assert run["status"] in ("killed", "running")  # kill CAS landed or racing
        deadline = time.monotonic() + 5
        while run["status"] not in ("killed",) and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
            run = await db.get_workflow_run(killed["current_run_id"])
        assert run["status"] == "killed"

    async def test_leg_killed_by_user_aborts_loop(self, db, engine, services):
        runs, rls = services
        obs = await _mk_observer(db)

        started = asyncio.Event()
        release = asyncio.Event()

        async def _slow_run(session_id=None, user_message="", **kwargs):
            started.set()
            await release.wait()
            return "late"

        engine.run = AsyncMock(side_effect=_slow_run)
        loop = await rls.create_loop(goal="g", verifier="- a", session_id=obs)
        await asyncio.wait_for(started.wait(), timeout=5)
        fresh = await db.get_review_loop(loop["id"])
        await runs.kill_run(fresh["current_run_id"], reason="user stop")
        release.set()
        parked = await _wait_status(db, loop["id"], ("killed",))
        assert "killed" in (parked["failure_reason"] or "")

    async def test_codex_leg_requires_backend(self, db, engine, services, tmp_path):
        runs, rls = services
        obs = await _mk_observer(db)
        engine._backends = {}
        with pytest.raises(ReviewLoopError, match="codex backend"):
            await rls.create_loop(
                goal="g", verifier="- a", session_id=obs,
                verifier_leg={"engine": "codex-ultracode"},
            )

    async def test_create_validation(self, db, engine, services):
        runs, rls = services
        with pytest.raises(ReviewLoopError, match="goal"):
            await rls.create_loop(goal="", verifier="- a", session_id=None)
        with pytest.raises(ReviewLoopError, match="verifier"):
            await rls.create_loop(goal="g", verifier="", session_id=None)
        with pytest.raises(ReviewLoopError, match="budget"):
            await rls.create_loop(goal="g", verifier="- a", session_id=None, budget_usd=-1)
        with pytest.raises(ReviewLoopError, match="criteria_adoption"):
            await rls.create_loop(
                goal="g", verifier="- a", session_id=None, criteria_adoption="always",
            )


# --------------------------------------------------------------------------- #
#  Progress detection                                                          #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestProgressDetection:
    async def test_state_handoff_changes_do_not_mask_no_progress(
        self, db, engine, tmp_path,
    ):
        """The required per-iteration STATE.md update is controller metadata,
        not implementation progress. Identical failures with no product
        change must still trip the breaker."""
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "product.txt").write_text("unchanged\n")
        subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
        subprocess.run(["git", "add", "product.txt"], cwd=repo, check=True)
        subprocess.run(
            [
                "git", "-c", "user.name=test", "-c",
                "user.email=test@example.invalid", "commit", "-qm", "init",
            ],
            cwd=repo,
            check=True,
        )

        cfg = _make_config(tmp_path)
        runs = WorkflowRunService(cfg, db, engine)
        rls = ReviewLoopService(lambda: cfg, db, engine, runs)
        loop = await rls.create_loop(
            goal="g", verifier="- a", session_id=None,
            cwd=str(repo), autostart=False,
        )
        state = rls._state_file(loop)
        state.parent.mkdir(parents=True)
        state.write_text("# Done\niteration one notes\n")
        digest_1 = rls._workspace_digest(str(repo))

        verdict = {
            "verdict": "needs_improvement",
            "criteria": [{"id": "C1", "status": "unmet"}],
        }
        await db.begin_leg_attempt(
            loop["id"], "implementing", ("pending",),
            iteration=1, role="implementer", run_id="wfr-prog-i1",
        )
        await db.settle_loop_attempt(
            "wfr-prog-i1", "done", detail={"digest": digest_1},
        )
        await db.begin_leg_attempt(
            loop["id"], "verifying", ("implementing",),
            iteration=1, role="verifier", run_id="wfr-prog-v1",
        )
        await db.settle_loop_attempt(
            "wfr-prog-v1", "done", verdict=verdict,
        )

        state.write_text("# Done\niteration two notes changed\n")
        digest_2 = rls._workspace_digest(str(repo))
        assert digest_2 == digest_1
        await db.begin_leg_attempt(
            loop["id"], "implementing", ("verifying",),
            iteration=2, role="implementer", run_id="wfr-prog-i2",
        )
        await db.settle_loop_attempt(
            "wfr-prog-i2", "done", detail={"digest": digest_2},
        )
        await db.begin_leg_attempt(
            loop["id"], "verifying", ("implementing",),
            iteration=2, role="verifier", run_id="wfr-prog-v2",
        )
        await db.settle_loop_attempt(
            "wfr-prog-v2", "done", verdict=verdict,
        )

        fresh = await db.get_review_loop(loop["id"])
        attempt = await db.get_attempt_by_run("wfr-prog-v2")
        assert await rls._no_progress(fresh, attempt, verdict) is True


# --------------------------------------------------------------------------- #
#  Restart recovery                                                            #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestRecovery:
    async def test_done_but_unprocessed_leg_advances(self, db, engine, tmp_path):
        """Crash window: implementer leg went 'done' but the loop never
        advanced. Recovery must consume the result and dispatch the verifier."""
        cfg = _make_config(tmp_path)
        runs = WorkflowRunService(cfg, db, engine)
        rls = ReviewLoopService(lambda: cfg, db, engine, runs)
        obs = await _mk_observer(db)
        loop = await rls.create_loop(
            goal="g", verifier="- a", session_id=obs, autostart=False,
        )
        run_id = "wfr-recov001"
        await db.begin_leg_attempt(
            loop["id"], "implementing", ("pending",),
            iteration=1, role="implementer", run_id=run_id,
        )
        session_id = f"workflow:{run_id}"
        await db.create_workflow_run(
            run_id, ENGINE_CLAUDE, {"prompt": "p"}, 1.0,
            created_by=f"review-loop:{loop['id']}",
        )
        await db.transition_workflow_run(run_id, "running", expect=("pending",))
        await db.create_session(session_id, source="workflow")
        await db.update_workflow_run(run_id, {"session_id": session_id})
        await db.transition_workflow_run(
            run_id, "done", expect=("running",), result="did it",
        )

        await rls._recover()
        while not rls._queue.empty():
            await rls._handle_leg_terminal(rls._queue.get_nowait())
        fresh = await db.get_review_loop(loop["id"])
        assert fresh["status"] == "verifying"
        verifier_attempts = [
            a for a in await db.list_loop_attempts(loop["id"])
            if a["role"] == "verifier"
        ]
        assert len(verifier_attempts) == 1

    async def test_crash_before_dispatch_reissues_same_id(self, db, engine, tmp_path):
        """Outbox contract: attempt row exists, run row doesn't — recovery
        re-issues with the SAME pre-generated id."""
        cfg = _make_config(tmp_path)
        runs = WorkflowRunService(cfg, db, engine)
        rls = ReviewLoopService(lambda: cfg, db, engine, runs)
        obs = await _mk_observer(db)
        loop = await rls.create_loop(
            goal="g", verifier="- a", session_id=obs, autostart=False,
        )
        run_id = "wfr-recov002"
        await db.begin_leg_attempt(
            loop["id"], "implementing", ("pending",),
            iteration=1, role="implementer", run_id=run_id,
        )
        await rls._recover()
        run = await db.get_workflow_run(run_id)
        assert run is not None and run["created_by"] == f"review-loop:{loop['id']}"
        # drain the spawned execution so teardown is clean
        deadline = time.monotonic() + 5
        while runs._exec_tasks or runs._kill_tasks:
            tasks = list(runs._exec_tasks.values()) + list(runs._kill_tasks)
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=max(0.1, deadline - time.monotonic()),
            )

    async def test_interrupted_implementer_parks_and_preserves_spend(
        self, db, engine, tmp_path,
    ):
        cfg = _make_config(tmp_path)
        runs = WorkflowRunService(cfg, db, engine)
        rls = ReviewLoopService(lambda: cfg, db, engine, runs)
        obs = await _mk_observer(db)
        loop = await rls.create_loop(
            goal="g", verifier="- a", session_id=obs, autostart=False,
        )
        run_id = "wfr-recov003"
        await db.begin_leg_attempt(
            loop["id"], "implementing", ("pending",),
            iteration=1, role="implementer", run_id=run_id,
        )
        await db.create_workflow_run(
            run_id, ENGINE_CLAUDE, {"prompt": "p"}, 1.0,
            created_by=f"review-loop:{loop['id']}",
        )
        await db.transition_workflow_run(run_id, "running", expect=("pending",))
        # The workflow monitor metered this before the daemon restarted.
        # WorkflowRunService's recovery preserves the value on the failed
        # run row; review-loop recovery must copy it into its own ledger.
        await db.update_workflow_run(run_id, {"spent_usd": 1.75})
        await db.transition_workflow_run(
            run_id, "failed", expect=("running",),
            error="interrupted by nerve restart",
        )
        await rls._recover()
        fresh = await db.get_review_loop(loop["id"])
        assert fresh["status"] == "awaiting_user"
        assert fresh["failure_reason"] == "restart"
        assert fresh["spent_usd"] == pytest.approx(1.75)
        attempt = await db.get_attempt_by_run(run_id)
        assert attempt["status"] == "failed"
        assert attempt["spend_usd"] == pytest.approx(1.75)

    async def test_recovery_routes_settled_but_unrouted_leg(self, db, engine, tmp_path):
        """Crash AFTER the attempt settled but BEFORE routing: the loop is
        stranded in 'implementing' with a done leg. Recovery must re-drive
        the routing (dispatch the verifier), not skip the settled attempt."""
        cfg = _make_config(tmp_path)
        runs = WorkflowRunService(cfg, db, engine)
        rls = ReviewLoopService(lambda: cfg, db, engine, runs)
        obs = await _mk_observer(db, "obs00r01")
        loop = await rls.create_loop(
            goal="g", verifier="- a", session_id=obs, autostart=False,
        )
        run_id = "wfr-strand01"
        await db.begin_leg_attempt(
            loop["id"], "implementing", ("pending",),
            iteration=1, role="implementer", run_id=run_id,
        )
        session_id = f"workflow:{run_id}"
        await db.create_workflow_run(
            run_id, ENGINE_CLAUDE, {"prompt": "p"}, 1.0,
            created_by=f"review-loop:{loop['id']}",
        )
        await db.transition_workflow_run(run_id, "running", expect=("pending",))
        await db.create_session(session_id, source="workflow")
        await db.update_workflow_run(run_id, {"session_id": session_id})
        await db.transition_workflow_run(
            run_id, "done", expect=("running",), result="did it",
        )
        # the crash window: attempt settled, loop never advanced
        await db.settle_loop_attempt(run_id, "done", spend_usd=0.5)

        await rls._recover()
        while not rls._queue.empty():
            await rls._handle_leg_terminal(rls._queue.get_nowait())
        fresh = await db.get_review_loop(loop["id"])
        assert fresh["status"] == "verifying"

    async def test_recovery_recomputes_spend(self, db, engine, tmp_path):
        cfg = _make_config(tmp_path)
        runs = WorkflowRunService(cfg, db, engine)
        rls = ReviewLoopService(lambda: cfg, db, engine, runs)
        loop = await rls.create_loop(
            goal="g", verifier="- a", session_id=None, autostart=False,
        )
        await db.begin_leg_attempt(
            loop["id"], "implementing", ("pending",),
            iteration=1, role="implementer", run_id="wfr-recov004",
        )
        await db.settle_loop_attempt("wfr-recov004", "done", spend_usd=2.5)
        # column drifted (crash between settle and loop update)
        assert (await db.get_review_loop(loop["id"]))["spent_usd"] == 0.0
        await rls._recover_one(await db.get_review_loop(loop["id"]))
        assert (await db.get_review_loop(loop["id"]))["spent_usd"] == pytest.approx(2.5)


# --------------------------------------------------------------------------- #
#  Tool handlers                                                               #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestReviewLoopTools:
    @pytest_asyncio.fixture
    async def wired(self, db, engine, services, monkeypatch):
        runs, rls = services
        import nerve.workflows as wf
        monkeypatch.setattr(wf, "_review_loop_service", rls)
        return runs, rls

    def _ctx(self, db, engine, session_id="obs00001") -> ToolContext:
        return ToolContext(session_id=session_id, db=db, engine=engine)

    async def test_start_status_kill_roundtrip(self, db, engine, wired):
        runs, rls = wired
        await _mk_observer(db)
        result = await review_loop_start_handler(self._ctx(db, engine), {
            "goal": "build the thing",
            "verifier": "- thing exists",
            "budget_usd": 3,
        })
        assert not result.is_error, result.content
        text = result.content[0]["text"]
        loop_id = text.split("Review loop ")[1].split(" ")[0]
        await _wait_status(db, loop_id, ("passed", "awaiting_user", "implementing", "verifying"))

        status = await review_loop_status_handler(self._ctx(db, engine), {})
        assert not status.is_error
        assert loop_id in status.content[0]["text"]

        listed = await review_loop_list_handler(self._ctx(db, engine), {})
        assert loop_id in listed.content[0]["text"]

        killed = await review_loop_kill_handler(
            self._ctx(db, engine), {"loop_id": loop_id, "reason": "done testing"},
        )
        assert not killed.is_error

    async def test_start_rejected_for_workflow_sessions(self, db, engine, wired):
        await db.create_session("workflow:wfr-x", source="workflow")
        result = await review_loop_start_handler(
            self._ctx(db, engine, session_id="workflow:wfr-x"),
            {"goal": "g", "verifier": "- a"},
        )
        assert result.is_error

    async def test_dispatcher_shape(self, db, engine, wired):
        """The registered dispatcher returns a DispatchResult with snooze
        plumbed through — the async-dispatcher contract."""
        runs, rls = wired
        obs = await _mk_observer(db, "obs00002")
        loop = await rls.create_loop(
            goal="g", verifier="- a", session_id=obs, budget_usd=0.04,
        )
        await _wait_status(db, loop["id"], ("awaiting_user",))
        result = await rls._dispatch_decision({}, loop["id"], "remind_4h", None)
        assert result.ok and result.snooze_until
        result2 = await rls._dispatch_decision({}, loop["id"], "accept", None)
        assert result2.ok
        assert (await db.get_review_loop(loop["id"]))["status"] == "passed"


# --------------------------------------------------------------------------- #
#  Workdir handling                                                            #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestCreateLoopCwd:
    async def test_missing_cwd_is_created(self, db, engine, tmp_path):
        cfg = _make_config(tmp_path)
        runs = WorkflowRunService(cfg, db, engine)
        rls = ReviewLoopService(lambda: cfg, db, engine, runs)
        target = tmp_path / "fresh" / "nested"
        assert not target.exists()
        loop = await rls.create_loop(
            goal="g", verifier="- a", session_id=None,
            cwd=str(target), autostart=False,
        )
        assert target.is_dir()
        assert loop["cwd"] == str(target.resolve())

    async def test_cwd_conflicting_with_file_is_rejected(
        self, db, engine, tmp_path,
    ):
        occupied = tmp_path / "occupied"
        occupied.write_text("not a directory")
        cfg = _make_config(tmp_path)
        runs = WorkflowRunService(cfg, db, engine)
        rls = ReviewLoopService(lambda: cfg, db, engine, runs)
        with pytest.raises(ReviewLoopError, match="invalid cwd"):
            await rls.create_loop(
                goal="g", verifier="- a", session_id=None,
                cwd=str(occupied), autostart=False,
            )
