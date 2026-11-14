"""Tests for cron run gates (nerve/cron/gates.py)."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

import nerve.workflows
from nerve.agent import tools as tools_mod
from nerve.agent.plan_service import approve_plan, decline_plan, request_plan_revision
from nerve.agent.sessions import new_cron_run_id
from nerve.agent.streaming import broadcaster
from nerve.agent.tools.handlers.plans import plan_propose_handler, plan_update_handler
from nerve.agent.tools.handlers.tasks import task_create_handler, task_done_handler
from nerve.agent.tools.registry import ToolContext
from nerve.cron.gates import (
    GATE_REGISTRY,
    AgentActivityGate,
    CronGate,
    GateConfigError,
    GateContext,
    GitHubPrActivityGate,
    MessagesGate,
    PlansInReviewGate,
    TasksGate,
    build_gate,
    build_gates,
    evaluate_gates,
)
from nerve.cron.jobs import CronJob
from nerve.cron.service import CronService
from nerve.sources.models import SourceRecord
from nerve.workflows.service import ENGINE_CLAUDE, WorkflowRunService
from tests.actor_rows import ensure_system_principal
from tests.test_plan_actions import FakeEngine
from tests.test_turn_outcomes import _engine, _session, _token_seen
from tests.test_turn_outcomes import _turn as _real_turn
from tests.test_workflow_runs import _drain, _make_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ctx(db: AsyncMock, job_id: str = "test-job") -> GateContext:
    return GateContext(job_id=job_id, db=db)


def _db(**methods) -> AsyncMock:
    """Build a mock db with the given async methods preconfigured."""
    db = AsyncMock()
    for name, value in methods.items():
        getattr(db, name).return_value = value
    return db


# ---------------------------------------------------------------------------
# TasksGate
# ---------------------------------------------------------------------------

class TestTasksGate:
    @pytest.mark.asyncio
    async def test_pending_status_satisfied(self):
        db = _db(count_tasks=3)
        gate = TasksGate(targets=["pending"])
        assert await gate.is_satisfied(_ctx(db)) is True
        db.count_tasks.assert_awaited_once_with(status="pending", tag=None)

    @pytest.mark.asyncio
    async def test_pending_status_unsatisfied(self):
        db = _db(count_tasks=0)
        gate = TasksGate(targets=["pending"])
        assert await gate.is_satisfied(_ctx(db)) is False

    @pytest.mark.asyncio
    async def test_default_status_means_open(self):
        """No status → count_tasks(status=None) (non-done)."""
        db = _db(count_tasks=1)
        gate = TasksGate(targets=[None])
        assert await gate.is_satisfied(_ctx(db)) is True
        db.count_tasks.assert_awaited_once_with(status=None, tag=None)

    @pytest.mark.asyncio
    async def test_min_count_threshold(self):
        db = _db(count_tasks=2)
        gate = TasksGate(targets=["pending"], min_count=3)
        assert await gate.is_satisfied(_ctx(db)) is False

        db2 = _db(count_tasks=3)
        gate2 = TasksGate(targets=["pending"], min_count=3)
        assert await gate2.is_satisfied(_ctx(db2)) is True

    @pytest.mark.asyncio
    async def test_status_list_sums_counts(self):
        """Counts across multiple statuses are summed until threshold."""
        db = AsyncMock()
        db.count_tasks.side_effect = [1, 2]  # pending=1, in_progress=2
        gate = TasksGate(targets=["pending", "in_progress"], min_count=3)
        assert await gate.is_satisfied(_ctx(db)) is True
        assert db.count_tasks.await_count == 2

    @pytest.mark.asyncio
    async def test_status_list_short_circuits(self):
        """Stops counting once the threshold is reached."""
        db = AsyncMock()
        db.count_tasks.side_effect = [5, 0]
        gate = TasksGate(targets=["pending", "in_progress"], min_count=1)
        assert await gate.is_satisfied(_ctx(db)) is True
        # Second status never queried — threshold already met.
        assert db.count_tasks.await_count == 1

    @pytest.mark.asyncio
    async def test_tag_filter_passed_through(self):
        db = _db(count_tasks=1)
        gate = TasksGate(targets=["pending"], tag="backend")
        await gate.is_satisfied(_ctx(db))
        db.count_tasks.assert_awaited_once_with(status="pending", tag="backend")

    @pytest.mark.asyncio
    async def test_tag_list_passed_through(self):
        """A list of tags is forwarded to count_tasks (OR-matched there)."""
        db = _db(count_tasks=1)
        gate = TasksGate(targets=["pending"], tag=["pr-open", "pr-fix"])
        await gate.is_satisfied(_ctx(db))
        db.count_tasks.assert_awaited_once_with(
            status="pending", tag=["pr-open", "pr-fix"])

    def test_describe_tag_list(self):
        d = TasksGate(targets=["pending"], tag=["a", "b"]).describe()
        assert "tagged 'a/b'" in d

    def test_min_count_floor_is_one(self):
        assert TasksGate(targets=["pending"], min_count=0).min_count == 1
        assert TasksGate(targets=["pending"], min_count=-5).min_count == 1

    def test_describe(self):
        assert "pending tasks" in TasksGate(targets=["pending"]).describe()
        assert "open tasks" in TasksGate(targets=[None]).describe()
        assert "pending/in_progress" in TasksGate(
            targets=["pending", "in_progress"]).describe()
        d = TasksGate(targets=["pending"], tag="urgent", min_count=2).describe()
        assert "urgent" in d and ">= 2" in d

    # -- from_config --------------------------------------------------------

    def test_from_config_string_status(self):
        gate = TasksGate.from_config({"type": "tasks", "status": "pending"})
        assert gate.targets == ["pending"]
        assert gate.min_count == 1

    def test_from_config_omitted_status(self):
        gate = TasksGate.from_config({"type": "tasks"})
        assert gate.targets == [None]

    def test_from_config_all_status(self):
        gate = TasksGate.from_config({"type": "tasks", "status": "all"})
        assert gate.targets == ["all"]

    def test_from_config_list_status(self):
        gate = TasksGate.from_config(
            {"type": "tasks", "status": ["pending", "blocked"]})
        assert gate.targets == ["pending", "blocked"]

    def test_from_config_empty_list_falls_back_to_open(self):
        gate = TasksGate.from_config({"type": "tasks", "status": []})
        assert gate.targets == [None]

    def test_from_config_with_tag_and_min_count(self):
        gate = TasksGate.from_config(
            {"type": "tasks", "status": "pending", "tag": "ci", "min_count": 4})
        assert gate.tag == "ci"
        assert gate.min_count == 4

    def test_from_config_tag_list(self):
        gate = TasksGate.from_config(
            {"type": "tasks", "tag": ["pr-open", "pr-fix"]})
        assert gate.tag == ["pr-open", "pr-fix"]

    def test_from_config_tag_tuple_coerced_to_str_list(self):
        gate = TasksGate.from_config({"type": "tasks", "tag": (1, 2)})
        assert gate.tag == ["1", "2"]

    def test_from_config_bad_tag_type(self):
        with pytest.raises(GateConfigError):
            TasksGate.from_config({"type": "tasks", "tag": 123})

    def test_from_config_bad_status_type(self):
        with pytest.raises(GateConfigError):
            TasksGate.from_config({"type": "tasks", "status": 123})

    def test_from_config_bad_min_count(self):
        with pytest.raises(GateConfigError):
            TasksGate.from_config(
                {"type": "tasks", "status": "pending", "min_count": "lots"})


# ---------------------------------------------------------------------------
# MessagesGate
# ---------------------------------------------------------------------------

class TestMessagesGate:
    @pytest.mark.asyncio
    async def test_satisfied_when_new_messages(self):
        db = _db(get_consumer_cursor=5, get_source_max_rowid=9)
        gate = MessagesGate(sources=["gmail"])
        assert await gate.is_satisfied(_ctx(db)) is True

    @pytest.mark.asyncio
    async def test_unsatisfied_when_caught_up(self):
        db = _db(get_consumer_cursor=9, get_source_max_rowid=9)
        gate = MessagesGate(sources=["gmail"])
        assert await gate.is_satisfied(_ctx(db)) is False

    @pytest.mark.asyncio
    async def test_any_source_with_new_messages_satisfies(self):
        db = AsyncMock()
        db.get_consumer_cursor.side_effect = [9, 2]   # gmail caught up, github behind
        db.get_source_max_rowid.side_effect = [9, 7]
        gate = MessagesGate(sources=["gmail", "github"])
        assert await gate.is_satisfied(_ctx(db)) is True

    @pytest.mark.asyncio
    async def test_no_sources_fires_when_consumer_has_unread(self):
        # Omitted sources → "any source": delegate to the read-only,
        # expiry-agnostic consumer_has_unread check.
        db = _db(consumer_has_unread=True)
        gate = MessagesGate()
        assert gate.sources == []
        assert await gate.is_satisfied(_ctx(db)) is True
        db.consumer_has_unread.assert_awaited_once_with("inbox")
        # Read-only: the any-source path must not initialize or advance a cursor.
        db.get_consumer_cursor.assert_not_called()
        db.set_consumer_cursor.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_sources_unsatisfied_when_consumer_caught_up(self):
        db = _db(consumer_has_unread=False)
        assert await MessagesGate().is_satisfied(_ctx(db)) is False

    def test_empty_sources_means_any_source(self):
        # Empty/omitted sources is now valid (no raise) and means "any source".
        assert MessagesGate(sources=[]).sources == []
        assert MessagesGate.from_config({"type": "messages"}).sources == []
        assert "any source" in MessagesGate().describe()

    def test_from_config_sources(self):
        gate = MessagesGate.from_config(
            {"type": "messages", "sources": ["gmail"], "consumer": "inbox2"})
        assert gate.sources == ["gmail"]
        assert gate.consumer == "inbox2"

    def test_from_config_string_source(self):
        gate = MessagesGate.from_config({"type": "messages", "sources": "gmail"})
        assert gate.sources == ["gmail"]

    def test_from_config_legacy_keys(self):
        """Legacy skip_when_idle / idle_consumer keys map onto this gate."""
        gate = MessagesGate.from_config({
            "type": "messages",
            "skip_when_idle": ["gmail", "github"],
            "idle_consumer": "inbox",
        })
        assert gate.sources == ["gmail", "github"]
        assert gate.consumer == "inbox"


def _message(record_id: str, source: str = "github") -> SourceRecord:
    return SourceRecord(
        id=record_id, source=source, record_type="github_notification", summary=f"message {record_id}",
        content="body", timestamp="2026-09-16T10:00:00Z", metadata={},
    )


async def _read_up_to_date(db, consumer: str, source: str, expires_in: timedelta) -> None:
    """A consumer that read *source* up to its newest message, its cursor
    expiring in *expires_in*."""
    await db.set_consumer_cursor(consumer, source, await db.get_source_max_rowid(source))
    await db._write(
        "UPDATE consumer_cursors SET expires_at = ? WHERE consumer = ? AND source = ?",
        ((datetime.now(UTC) + expires_in).isoformat(), consumer, source),
    )


async def _expiry(db, consumer: str, source: str) -> datetime:
    async with db.db.execute(
        "SELECT expires_at FROM consumer_cursors WHERE consumer = ? AND source = ?", (consumer, source),
    ) as cursor:
        row = await cursor.fetchone()
    return datetime.fromisoformat(row[0])


class _ThreeHoursLater(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime.now(tz) + timedelta(hours=3)


@pytest.mark.asyncio
class TestMessagesGateKeepsItsCursorsAlive:
    async def test_a_check_renews_a_live_cursor_about_to_expire(self, db):
        await db.insert_source_messages([_message("1")], "github")
        await _read_up_to_date(db, "inbox", "github", timedelta(hours=1))

        assert await MessagesGate(sources=["github"]).is_satisfied(_ctx(db)) is False

        assert await _expiry(db, "inbox", "github") > datetime.now(UTC) + timedelta(days=1)

    async def test_a_quiet_spell_does_not_cost_the_first_new_message(self, db, monkeypatch):
        """The job's gate is checked while its cursor has an hour left, and the
        next record arrives three hours later. Had the cursor expired, the gate
        would start it again at that record and never report it."""
        gate = MessagesGate(sources=["github"])
        await db.insert_source_messages([_message("1")], "github")
        await _read_up_to_date(db, "inbox", "github", timedelta(hours=1))
        assert await gate.is_satisfied(_ctx(db)) is False

        monkeypatch.setattr("nerve.db.sources.datetime", _ThreeHoursLater)
        await db.insert_source_messages([_message("2")], "github")

        assert await gate.is_satisfied(_ctx(db)) is True

    async def test_a_check_without_sources_renews_the_consumers_cursors_only(self, db):
        for source in ("github", "gmail"):
            await db.insert_source_messages([_message("1", source)], source)
            await _read_up_to_date(db, "inbox", source, timedelta(hours=1))
        await _read_up_to_date(db, "triage", "github", timedelta(hours=1))

        assert await MessagesGate().is_satisfied(_ctx(db)) is False

        later = datetime.now(UTC) + timedelta(days=1)
        assert await _expiry(db, "inbox", "github") > later
        assert await _expiry(db, "inbox", "gmail") > later
        assert await _expiry(db, "triage", "github") < later

    async def test_only_a_live_cursor_past_half_its_lifetime_is_renewed(self, db):
        await db.insert_source_messages([_message("1")], "github")
        for consumer, expires_in in (("due", timedelta(hours=1)), ("fresh", timedelta(days=1.5)),
                                     ("expired", timedelta(hours=-1))):
            await _read_up_to_date(db, consumer, "github", expires_in)

        renewed = [
            await db.keep_consumer_cursors_alive(consumer, ["github"], ttl_days=2)
            for consumer in ("due", "fresh", "expired")
        ]

        assert renewed == [1, 0, 0]
        assert await db.peek_consumer_cursor("expired", "github") is None

    async def test_the_forecast_renews_nothing(self, db):
        await db.insert_source_messages([_message("1")], "github")
        await _read_up_to_date(db, "inbox", "github", timedelta(hours=1))
        before = await _expiry(db, "inbox", "github")

        assert await MessagesGate(sources=["github"]).forecast(_ctx(db), datetime.now(UTC)) is False

        assert await _expiry(db, "inbox", "github") == before

    async def test_a_failed_renewal_leaves_the_answer_alone(self):
        db = _db(get_consumer_cursor=5, get_source_max_rowid=9)
        db.keep_consumer_cursors_alive.side_effect = RuntimeError("database is locked")

        assert await MessagesGate(sources=["gmail"]).is_satisfied(_ctx(db)) is True


# ---------------------------------------------------------------------------
# build_gate / build_gates
# ---------------------------------------------------------------------------

class TestBuildGate:
    def test_build_known_types(self):
        assert isinstance(build_gate({"type": "tasks"}), TasksGate)
        assert isinstance(
            build_gate({"type": "messages", "sources": ["gmail"]}), MessagesGate)

    def test_unknown_type_raises(self):
        with pytest.raises(GateConfigError):
            build_gate({"type": "weather"})

    def test_missing_type_raises(self):
        with pytest.raises(GateConfigError):
            build_gate({"status": "pending"})

    def test_non_dict_raises(self):
        with pytest.raises(GateConfigError):
            build_gate(["not", "a", "dict"])  # type: ignore[arg-type]

    def test_build_gates_refuses_an_invalid_spec(self):
        """One unbuildable spec fails the lot, rather than being left out.

        Silently dropping it would widen the job from "only when these hold" to
        "whenever the schedule says", which is the opposite of what a gate is
        for. The caller loses the job instead; see build_gates' docstring.
        """
        with pytest.raises(GateConfigError, match="bogus"):
            build_gates([
                {"type": "tasks", "status": "pending"},
                {"type": "bogus"},
            ])
        # A "messages" gate with no sources is now valid ("any source"),
        # so it builds rather than failing the lot.
        gates = build_gates([{"type": "messages"}])
        assert len(gates) == 1 and isinstance(gates[0], MessagesGate)

    def test_build_gates_builds_every_valid_spec(self):
        gates = build_gates([
            {"type": "tasks", "status": "pending"},
            {"type": "messages", "sources": ["gmail"]},
        ])
        assert len(gates) == 2
        assert isinstance(gates[0], TasksGate)

    def test_build_gates_empty(self):
        assert build_gates([]) == []
        assert build_gates(None) == []  # type: ignore[arg-type]

    def test_registry_keys_match_class_type(self):
        for key, cls in GATE_REGISTRY.items():
            assert issubclass(cls, CronGate)
            assert cls.type == key


# ---------------------------------------------------------------------------
# evaluate_gates (AND semantics + fail-open)
# ---------------------------------------------------------------------------

class _StubGate(CronGate):
    type = "stub"

    def __init__(self, satisfied: bool | Exception):
        self._satisfied = satisfied

    async def is_satisfied(self, ctx: GateContext) -> bool:
        if isinstance(self._satisfied, Exception):
            raise self._satisfied
        return self._satisfied

    def describe(self) -> str:
        return "stub gate"

    @classmethod
    def from_config(cls, spec: dict) -> "_StubGate":
        return cls(spec.get("satisfied", True))


class TestEvaluateGates:
    @pytest.mark.asyncio
    async def test_no_gates_runs(self):
        decision = await evaluate_gates([], _ctx(AsyncMock()))
        assert decision.should_run is True

    @pytest.mark.asyncio
    async def test_all_satisfied_runs(self):
        gates = [_StubGate(True), _StubGate(True)]
        decision = await evaluate_gates(gates, _ctx(AsyncMock()))
        assert decision.should_run is True

    @pytest.mark.asyncio
    async def test_one_unsatisfied_skips(self):
        gates = [_StubGate(True), _StubGate(False)]
        decision = await evaluate_gates(gates, _ctx(AsyncMock()))
        assert decision.should_run is False
        assert "stub" in decision.reason

    @pytest.mark.asyncio
    async def test_fail_open_on_error(self):
        """A gate that raises is treated as satisfied (run proceeds)."""
        gates = [_StubGate(RuntimeError("db down")), _StubGate(True)]
        decision = await evaluate_gates(gates, _ctx(AsyncMock()))
        assert decision.should_run is True


# ---------------------------------------------------------------------------
# CronJob integration (run_if + legacy translation)
# ---------------------------------------------------------------------------

class TestCronJobGates:
    def _job(self, **kwargs) -> CronJob:
        return CronJob(id="j", schedule="1h", prompt="p", **kwargs)

    def test_no_gates_by_default(self):
        assert self._job().gates == []

    def test_run_if_builds_gates(self):
        job = self._job(run_if=[{"type": "tasks", "status": "pending"}])
        assert len(job.gates) == 1
        assert isinstance(job.gates[0], TasksGate)

    def test_legacy_skip_when_idle_builds_messages_gate(self):
        job = self._job(skip_when_idle=["gmail"], idle_consumer="inbox")
        assert len(job.gates) == 1
        assert isinstance(job.gates[0], MessagesGate)
        assert job.gates[0].sources == ["gmail"]
        assert job.gates[0].consumer == "inbox"

    def test_run_if_and_legacy_combine(self):
        job = self._job(
            run_if=[{"type": "tasks", "status": "pending"}],
            skip_when_idle=["gmail"],
        )
        kinds = {type(g) for g in job.gates}
        assert kinds == {TasksGate, MessagesGate}

    def test_from_dict_parses_run_if(self):
        job = CronJob.from_dict({
            "id": "x", "schedule": "1h", "prompt": "p",
            "run_if": [{"type": "tasks", "status": "pending"}],
        })
        assert len(job.gates) == 1
        assert isinstance(job.gates[0], TasksGate)

    def test_from_dict_normalizes_only_a_bare_key(self):
        """A bare `run_if:` is None and means "no gates". Nothing else does:
        a wrong shape has to survive on the job so `nerve config validate` can
        reject it, instead of turning into an ungated job that looks correct."""
        bare = CronJob.from_dict({"id": "x", "schedule": "1h", "prompt": "p",
                                  "run_if": None, "skip_when_idle": None})
        assert bare.run_if == [] and bare.skip_when_idle == []

        # gates=False is how validation loads a bundle: the shape is kept intact
        # for inspection rather than built (and refused).
        for shape in ({}, "", 0, "tasks", {"type": "tasks"}):
            job = CronJob.from_dict({
                "id": "x", "schedule": "1h", "prompt": "p", "run_if": shape,
            }, gates=False)
            assert job.run_if == shape, f"{shape!r} was normalized away"
            job = CronJob.from_dict({
                "id": "x", "schedule": "1h", "prompt": "p",
                "skip_when_idle": shape,
            }, gates=False)
            assert job.skip_when_idle == shape, f"{shape!r} was normalized away"

    def test_a_wrong_shape_is_refused_not_run_unguarded(self):
        """__post_init__ builds the gates, so this drops the whole job — which is
        the intent. A gate block nobody can read is not permission to run."""
        with pytest.raises(GateConfigError, match="run_if"):
            CronJob.from_dict({
                "id": "x", "schedule": "1h", "prompt": "p", "run_if": 0,
            })
        with pytest.raises(GateConfigError, match="skip_when_idle"):
            CronJob.from_dict({
                "id": "x", "schedule": "1h", "prompt": "p",
                "skip_when_idle": {},
            })


# ---------------------------------------------------------------------------
# GitHubPrActivityGate
# ---------------------------------------------------------------------------

class TestGitHubPrActivityGate:
    """Fingerprints an author's open PRs; fires only when the fingerprint moves."""

    def _gate(self, tmp_path, fingerprint, *, force_hours=8.0):
        """A gate with the network fingerprint stubbed and state under tmp_path."""
        gate = GitHubPrActivityGate(author="bot", force_run_after_hours=force_hours)

        async def _fp():
            return fingerprint

        gate._fingerprint = _fp
        gate._state_path = lambda job_id: tmp_path / f"st_{job_id}.json"
        return gate

    @staticmethod
    def _seed(tmp_path, job_id, fingerprint, last_fire):
        (tmp_path / f"st_{job_id}.json").write_text(
            json.dumps({"fingerprint": fingerprint, "last_fire": last_fire.isoformat()})
        )

    # -- gating logic -------------------------------------------------------

    @pytest.mark.asyncio
    async def test_first_run_fires_and_records(self, tmp_path):
        gate = self._gate(tmp_path, "fp-abc")
        assert await gate.is_satisfied(_ctx(AsyncMock(), "j")) is True
        saved = json.loads((tmp_path / "st_j.json").read_text())
        assert saved["fingerprint"] == "fp-abc"

    @pytest.mark.asyncio
    async def test_unchanged_fingerprint_skips(self, tmp_path):
        gate = self._gate(tmp_path, "fp-abc")
        self._seed(tmp_path, "j", "fp-abc", datetime.now(timezone.utc))
        assert await gate.is_satisfied(_ctx(AsyncMock(), "j")) is False

    @pytest.mark.asyncio
    async def test_changed_fingerprint_fires_and_updates(self, tmp_path):
        gate = self._gate(tmp_path, "fp-new")
        self._seed(tmp_path, "j", "fp-old", datetime.now(timezone.utc))
        assert await gate.is_satisfied(_ctx(AsyncMock(), "j")) is True
        saved = json.loads((tmp_path / "st_j.json").read_text())
        assert saved["fingerprint"] == "fp-new"

    @pytest.mark.asyncio
    async def test_fail_open_when_gh_fails(self, tmp_path):
        # _fingerprint() == None models a total gh failure → run anyway.
        gate = self._gate(tmp_path, None)
        self._seed(tmp_path, "j", "fp-abc", datetime.now(timezone.utc))
        assert await gate.is_satisfied(_ctx(AsyncMock(), "j")) is True

    @pytest.mark.asyncio
    async def test_force_run_when_stale(self, tmp_path):
        gate = self._gate(tmp_path, "fp-abc", force_hours=8.0)
        self._seed(tmp_path, "j", "fp-abc",
                   datetime.now(timezone.utc) - timedelta(hours=9))
        assert await gate.is_satisfied(_ctx(AsyncMock(), "j")) is True

    @pytest.mark.asyncio
    async def test_no_force_when_recent(self, tmp_path):
        gate = self._gate(tmp_path, "fp-abc", force_hours=8.0)
        self._seed(tmp_path, "j", "fp-abc",
                   datetime.now(timezone.utc) - timedelta(hours=1))
        assert await gate.is_satisfied(_ctx(AsyncMock(), "j")) is False

    @pytest.mark.asyncio
    async def test_force_disabled_with_zero_hours(self, tmp_path):
        gate = self._gate(tmp_path, "fp-abc", force_hours=0)
        self._seed(tmp_path, "j", "fp-abc",
                   datetime.now(timezone.utc) - timedelta(days=30))
        assert await gate.is_satisfied(_ctx(AsyncMock(), "j")) is False

    # -- fingerprint computation -------------------------------------------

    @staticmethod
    def _fake_gh(prs, detail_by_number):
        async def _gh(*args, timeout=30.0):
            if args[:2] == ("search", "prs"):
                return json.dumps(prs)
            if args[:2] == ("pr", "view"):
                return json.dumps(detail_by_number[int(args[2])])
            return None
        return _gh

    @pytest.mark.asyncio
    async def test_fingerprint_stable_and_change_sensitive(self):
        prs = [{"repository": {"nameWithOwner": "owner/repo"}, "number": 1}]
        detail = {
            "state": "OPEN", "reviewDecision": None, "headRefOid": "sha1",
            "statusCheckRollup": [
                {"name": "ci", "status": "COMPLETED", "conclusion": "SUCCESS"}],
        }
        gate = GitHubPrActivityGate(author="bot")

        gate._gh = self._fake_gh(prs, {1: detail})
        fp1 = await gate._fingerprint()
        assert fp1 is not None

        # Identical data → identical hash (no spurious fire).
        gate._gh = self._fake_gh(prs, {1: dict(detail)})
        assert await gate._fingerprint() == fp1

        # A CI conclusion flip moves the hash — the signal a comment-based
        # source would miss entirely.
        flipped = dict(detail, statusCheckRollup=[
            {"name": "ci", "status": "COMPLETED", "conclusion": "FAILURE"}])
        gate._gh = self._fake_gh(prs, {1: flipped})
        assert await gate._fingerprint() != fp1

    @pytest.mark.asyncio
    async def test_fingerprint_none_on_gh_failure(self):
        gate = GitHubPrActivityGate(author="bot")

        async def _fail(*args, timeout=30.0):
            return None

        gate._gh = _fail
        assert await gate._fingerprint() is None

    # -- from_config / describe / registry ---------------------------------

    def test_from_config_requires_author(self):
        with pytest.raises(GateConfigError):
            GitHubPrActivityGate.from_config({"type": "github_pr_activity"})

    def test_from_config_parses_hours(self):
        gate = GitHubPrActivityGate.from_config({
            "type": "github_pr_activity", "author": "bot",
            "force_run_after_hours": 4})
        assert gate.author == "bot"
        assert gate.force_run_after_hours == 4.0

    def test_from_config_bad_hours(self):
        with pytest.raises(GateConfigError):
            GitHubPrActivityGate.from_config({
                "type": "github_pr_activity", "author": "bot",
                "force_run_after_hours": "lots"})

    def test_describe_mentions_author(self):
        assert "bot" in GitHubPrActivityGate(author="bot").describe()

    def test_build_gate_via_registry(self):
        gate = build_gate({"type": "github_pr_activity", "author": "bot"})
        assert isinstance(gate, GitHubPrActivityGate)


# ---------------------------------------------------------------------------
# GitHubPrActivityGate — comment/review activity in the fingerprint
# ---------------------------------------------------------------------------

def _comment(login: str, created: str = "2026-08-04T12:19:24Z", edited: bool = False):
    return {
        "author": {"login": login},
        "createdAt": created,
        "includesCreatedEdit": edited,
    }


def _review(
    login: str,
    state: str = "COMMENTED",
    submitted: str = "2026-08-04T12:19:24Z",
    edited: bool = False,
):
    return {
        "author": {"login": login},
        "state": state,
        "submittedAt": submitted,
        "includesCreatedEdit": edited,
    }


def _pr_payload(comments=None, reviews=None, **overrides):
    """A `gh pr view --json ...` payload with stable non-comment state."""
    payload = {
        "state": "OPEN",
        "reviewDecision": "REVIEW_REQUIRED",
        "headRefOid": "cd46fa400806f5634728a5a7c9963afefbc940bd",
        "statusCheckRollup": [
            {"name": "build", "status": "COMPLETED", "conclusion": "SUCCESS"},
        ],
        "comments": comments or [],
        "reviews": reviews or [],
    }
    payload.update(overrides)
    return payload


def _stub_gh(gate: GitHubPrActivityGate, payload: dict) -> None:
    """Point the gate's `gh` shell-out at a single fake open PR."""
    async def fake_gh(*args, **kwargs):
        if args[0] == "search":
            return json.dumps(
                [{"repository": {"nameWithOwner": "acme/widget"}, "number": 7}]
            )
        return json.dumps(payload)

    gate._gh = fake_gh  # type: ignore[method-assign]


async def _fp(payload: dict, **kwargs) -> str:
    gate = GitHubPrActivityGate(author="my-bot", **kwargs)
    _stub_gh(gate, payload)
    return await gate._fingerprint()


class TestGitHubPrActivityCommentActivity:
    """Regression cover for the 2026-08-04 comment-blindness bug.

    The gate originally hashed only state/reviewDecision/headRefOid/checks, so a
    maintainer's comment on one of our PRs never woke the monitor — and the
    `github` inbox path silently consumed it (reason=author on our own PR), so
    nothing observed it until the 8h force-run.
    """

    @pytest.mark.asyncio
    async def test_human_comment_changes_fingerprint(self):
        """THE regression: a new human comment must move the fingerprint."""
        before = await _fp(_pr_payload())
        after = await _fp(_pr_payload(comments=[_comment("alex-clickhouse")]))
        assert before != after

    @pytest.mark.asyncio
    async def test_human_commented_review_changes_fingerprint(self):
        """A COMMENTED review leaves reviewDecision untouched, so hash it too."""
        before = await _fp(_pr_payload())
        after = await _fp(_pr_payload(reviews=[_review("alex-clickhouse")]))
        assert before != after

    @pytest.mark.asyncio
    async def test_comment_edit_changes_fingerprint(self):
        """Editing a comment in place keeps count/timestamp — catch it anyway."""
        before = await _fp(_pr_payload(comments=[_comment("alex-clickhouse")]))
        after = await _fp(
            _pr_payload(comments=[_comment("alex-clickhouse", edited=True)])
        )
        assert before != after

    @pytest.mark.asyncio
    async def test_ignored_bot_comment_does_not_change_fingerprint(self):
        """Chatty bots must not wake an expensive monitor (CI covers them)."""
        quiet = await _fp(_pr_payload(), ignore_actors=["codecov"])
        noisy = await _fp(
            _pr_payload(comments=[_comment("codecov")]), ignore_actors=["codecov"]
        )
        assert quiet == noisy

    @pytest.mark.asyncio
    async def test_ignored_bot_review_does_not_change_fingerprint(self):
        quiet = await _fp(_pr_payload(), ignore_actors=["cursor"])
        noisy = await _fp(
            _pr_payload(reviews=[_review("cursor")]), ignore_actors=["cursor"]
        )
        assert quiet == noisy

    @pytest.mark.asyncio
    async def test_bot_suffix_is_normalised(self):
        """GraphQL strips `[bot]`; REST keeps it. One denylist entry covers both."""
        quiet = await _fp(_pr_payload(), ignore_actors=["codecov"])
        noisy = await _fp(
            _pr_payload(comments=[_comment("Codecov[bot]")]),
            ignore_actors=["codecov"],
        )
        assert quiet == noisy

    @pytest.mark.asyncio
    async def test_own_comment_does_not_change_fingerprint(self):
        """Our own replies must never wake us."""
        quiet = await _fp(_pr_payload())
        noisy = await _fp(_pr_payload(comments=[_comment("my-bot")]))
        assert quiet == noisy

    @pytest.mark.asyncio
    async def test_unlisted_bot_still_counts(self):
        """Denylist fails safe: an unknown actor costs a wake, never a miss."""
        before = await _fp(_pr_payload(), ignore_actors=["codecov"])
        after = await _fp(
            _pr_payload(comments=[_comment("brand-new-bot")]),
            ignore_actors=["codecov"],
        )
        assert before != after

    @pytest.mark.asyncio
    async def test_ghost_author_counts_as_activity(self):
        """A deleted user's comment (author=null) must not be silently dropped."""
        before = await _fp(_pr_payload())
        after = await _fp(
            _pr_payload(
                comments=[{"author": None, "createdAt": "x", "includesCreatedEdit": False}]
            )
        )
        assert before != after

    @pytest.mark.asyncio
    async def test_non_comment_state_still_fingerprinted(self):
        """Pre-existing signals must keep working."""
        base = _pr_payload()
        assert await _fp(base) != await _fp(_pr_payload(state="MERGED"))
        assert await _fp(base) != await _fp(_pr_payload(headRefOid="deadbeef"))
        assert await _fp(base) != await _fp(
            _pr_payload(reviewDecision="CHANGES_REQUESTED")
        )
        assert await _fp(base) != await _fp(
            _pr_payload(
                statusCheckRollup=[
                    {"name": "build", "status": "COMPLETED", "conclusion": "FAILURE"}
                ]
            )
        )

    @pytest.mark.asyncio
    async def test_comments_are_requested_from_gh(self):
        """The payload is useless if the fields aren't asked for."""
        gate = GitHubPrActivityGate(author="my-bot")
        seen: list[tuple] = []

        async def fake_gh(*args, **kwargs):
            seen.append(args)
            if args[0] == "search":
                return json.dumps(
                    [{"repository": {"nameWithOwner": "acme/widget"}, "number": 7}]
                )
            return json.dumps(_pr_payload())

        gate._gh = fake_gh  # type: ignore[method-assign]
        await gate._fingerprint()
        view = next(a for a in seen if a[0] == "pr")
        # Compare tokens, not substrings: `"reviews" in "...,latestReviews"` is
        # true, so a substring check would still pass if the code asked for a
        # differently-named field that `_pr_detail` never reads.
        fields = set(view[view.index("--json") + 1].split(","))
        assert {"state", "reviewDecision", "headRefOid",
                "statusCheckRollup", "comments", "reviews"} <= fields

    def test_author_is_always_ignored(self):
        gate = GitHubPrActivityGate(author="My-Bot")
        assert "my-bot" in gate.ignore_actors

    def test_from_config_ignore_actors(self):
        gate = build_gate({
            "type": "github_pr_activity",
            "author": "my-bot",
            "ignore_actors": ["Codecov[bot]", "cursor"],
        })
        assert {"codecov", "cursor", "my-bot"} <= gate.ignore_actors

    def test_from_config_ignore_actors_string_coerced(self):
        gate = build_gate({
            "type": "github_pr_activity",
            "author": "my-bot",
            "ignore_actors": "codecov",
        })
        assert "codecov" in gate.ignore_actors

    def test_from_config_ignore_actors_omitted(self):
        gate = build_gate({"type": "github_pr_activity", "author": "my-bot"})
        assert gate.ignore_actors == {"my-bot"}

    def test_from_config_bare_ignore_actors_means_unset(self):
        """A bare `ignore_actors:` is None — the one value that means "not set"."""
        gate = build_gate({
            "type": "github_pr_activity", "author": "my-bot",
            "ignore_actors": None,
        })
        assert gate.ignore_actors == {"my-bot"}

    def test_from_config_bad_ignore_actors(self):
        with pytest.raises(GateConfigError):
            build_gate({
                "type": "github_pr_activity",
                "author": "my-bot",
                "ignore_actors": [{"login": "codecov"}],
            })

    @pytest.mark.parametrize("bad", [0, False, "", [""], ["  "], ["ok", ""], {}])
    def test_from_config_falsy_ignore_actors_is_refused(self, bad):
        """Refused, not read as an empty denylist.

        `spec.get(...) or []` would have taken every one of these as "not set",
        leaving a job that still wakes on every bot comment while the config
        says otherwise.
        """
        with pytest.raises(GateConfigError, match="ignore_actors"):
            build_gate({
                "type": "github_pr_activity",
                "author": "my-bot",
                "ignore_actors": bad,
            })

    @pytest.mark.asyncio
    async def test_empty_login_cannot_mute_a_ghost_author(self):
        """An empty entry must never reach the denylist.

        `_actor` reports "" for a deleted/ghost user, so a stored "" would mute
        exactly the comments `test_ghost_author_counts_as_activity` says must
        count. from_config refuses one; a direct caller gets it dropped.
        """
        gate = GitHubPrActivityGate(author="my-bot", ignore_actors=["", "  "])
        assert gate.ignore_actors == {"my-bot"}

        quiet = await _fp(_pr_payload(), ignore_actors=["", "  "])
        loud = await _fp(
            _pr_payload(comments=[_comment("")]), ignore_actors=["", "  "]
        )
        assert quiet != loud

    def test_describe_mentions_comments(self):
        assert "comment" in GitHubPrActivityGate(author="my-bot").describe()


# ---------------------------------------------------------------------------
# AgentActivityGate
# ---------------------------------------------------------------------------

def _ago(minutes: float) -> datetime:
    return datetime.now(UTC) - timedelta(minutes=minutes)


def _run_session(job_id: str) -> str:
    """The id of a session of one run, or one generation, of ``job_id``."""
    return f"cron:{job_id}:{new_cron_run_id()}"


async def _turn(db, session_id: str, *, minutes_ago: float = 0) -> None:
    """A turn that ended that long ago in ``session_id`` (made on demand) and
    reported its usage. Nothing else is stored: only the usage row says so."""
    if await db.get_session(session_id) is None:
        await db.create_session(session_id, source="test", actor=None)
    await db.record_turn_usage(
        session_id=session_id, input_tokens=10, output_tokens=5,
        cache_creation=0, cache_read=0, max_context=200_000,
    )
    await db.db.execute(
        "UPDATE session_usage SET created_at = ? WHERE id = (SELECT MAX(id) FROM session_usage)",
        (_ago(minutes_ago).strftime("%Y-%m-%d %H:%M:%S"),),
    )
    await db.db.commit()


async def _stored_message(db, session_id: str, *, minutes_ago: float = 0, **kwargs) -> None:
    """A message stored that long ago in ``session_id`` (made on demand), as a
    turn stores what it was asked. No usage row."""
    if await db.get_session(session_id) is None:
        await db.create_session(session_id, source="test", actor=None)
        await db.db.execute(  # the session is older than anything a test asks about
            "UPDATE sessions SET updated_at = datetime('now', '-400 days') WHERE id = ?",
            (session_id,),
        )
        await db.db.commit()
    await db.add_message(session_id, "user", "hello", actor=None, **kwargs)
    if kwargs.get("bump_updated_at", True):
        await db.db.execute(  # the form add_message writes
            "UPDATE sessions SET updated_at = ? WHERE id = ?",
            (_ago(minutes_ago).isoformat(), session_id),
        )
        await db.db.commit()


async def _ran(db, job_id: str, *, minutes_ago: float = 0, status: str = "success") -> None:
    """A finished run of ``job_id`` that started that long ago."""
    log_id = await db.log_cron_start(job_id)
    await db.log_cron_finish(log_id, status)
    await db.db.execute(
        "UPDATE cron_logs SET started_at = ? WHERE id = ?",
        (_ago(minutes_ago).strftime("%Y-%m-%d %H:%M:%S"), log_id),
    )
    await db.db.commit()


async def _workflow_run(db, run_id: str, created_by: str) -> str:
    """A dispatched workflow run, linked to its ``workflow:<run>`` session."""
    session_id = f"workflow:{run_id}"
    await db.create_session(session_id, source="workflow", actor=None)
    await db.create_workflow_run(run_id, "claude-workflow", {"prompt": "x"}, 5.0, created_by=created_by)
    assert await db.transition_workflow_run(run_id, "running", expect=("pending",))
    await db.update_workflow_run(run_id, {"session_id": session_id})
    return session_id


# How the agent's work reaches the database: a turn that ended and reported
# its usage, or a message that a turn stored.
WORK = pytest.mark.parametrize("work", [_turn, _stored_message], ids=["a turn that ended", "a stored message"])


@pytest.mark.asyncio
class TestAgentActivityGate:
    async def _open(self, db, gate: AgentActivityGate | None = None, job_id: str = "tidy", jobs=()) -> bool:
        ctx = GateContext(job_id=job_id, db=db, jobs={job.id: job for job in jobs})
        return await (gate or AgentActivityGate()).is_satisfied(ctx)

    async def test_an_agent_that_never_worked_has_no_activity(self, db):
        assert await self._open(db) is False

    @WORK
    async def test_a_job_that_never_ran_runs_after_any_work(self, db, work):
        await work(db, "chat-1", minutes_ago=60 * 24 * 90)

        assert await self._open(db) is True

    @WORK
    async def test_work_before_the_last_run_is_not_activity(self, db, work):
        await work(db, "chat-1", minutes_ago=60)
        await _ran(db, "tidy", minutes_ago=30)

        assert await self._open(db) is False

    @WORK
    async def test_work_since_the_last_run_is_activity(self, db, work):
        await _ran(db, "tidy", minutes_ago=60)
        await work(db, "chat-1", minutes_ago=30)

        assert await self._open(db) is True

    @pytest.mark.parametrize("stored", ["2026-01-02T03:04:05.678901+00:00", "2026-01-02 03:04:05"])
    async def test_a_session_is_compared_by_its_time_in_either_stored_form(self, db, stored):
        """``updated_at`` is ISO 8601 where a message moved it and SQLite's
        own text where a status change did. As text, the first is later than
        any time of its day in the second form, and the second earlier."""
        await _stored_message(db, "chat-1")
        await _ran(db, "tidy")
        await db.db.execute("UPDATE sessions SET updated_at = ?", (stored,))

        for run_started, open_ in (("2026-01-02 03:04:06", False), ("2026-01-02 03:04:05", True),
                                   ("2026-01-01 23:59:59", True), ("2026-01-03 00:00:00", False)):
            await db.db.execute("UPDATE cron_logs SET started_at = ?", (run_started,))
            await db.db.commit()
            assert await self._open(db) is open_, run_started

    async def test_a_turn_in_the_second_the_run_started_counts(self, db):
        """Seconds are all the two clocks have: a turn that ended in the
        second the run started may have ended after it began."""
        await _ran(db, "tidy")
        await _turn(db, "chat-1")
        await db.db.execute("UPDATE cron_logs SET started_at = '2026-01-02 03:04:05'")
        await db.db.execute("UPDATE session_usage SET created_at = '2026-01-02 03:04:04'")
        await db.db.commit()
        assert await self._open(db) is False

        await db.db.execute("UPDATE session_usage SET created_at = '2026-01-02 03:04:05'")
        await db.db.commit()
        assert await self._open(db) is True

    async def test_a_session_nothing_was_stored_in_is_not_activity(self, db):
        await _ran(db, "tidy", minutes_ago=60)
        await db.create_session("empty", source="test", actor=None)

        assert await self._open(db) is False

    async def test_a_wakeups_message_is_seen_through_its_turn(self, db):
        """A turn nobody asked for stores its messages without moving the
        session's ``updated_at``. Its usage row is what says it ran."""
        await _ran(db, "tidy", minutes_ago=60)
        await _stored_message(db, "chat-1", bump_updated_at=False)
        assert await self._open(db) is False

        await _turn(db, "chat-1")
        assert await self._open(db) is True

    async def test_a_conversation_taken_in_from_an_external_agent_is_activity(self, db):
        """Its messages keep their own times; the session moves when they are
        stored."""
        await _ran(db, "tidy", minutes_ago=60)
        await db.create_session("thread-1", source="codex", actor=None)
        await db.db.execute("UPDATE sessions SET updated_at = datetime('now', '-400 days')")
        await db.db.commit()
        assert await self._open(db) is False

        await db.add_message_idempotent(
            "thread-1", "assistant", "done", external_id="ext-1",
            created_at=_ago(60 * 24 * 3).isoformat(), actor=None,
        )
        assert await self._open(db) is True

    @WORK
    @pytest.mark.parametrize("session_id", [
        _run_session("tidy"), "cron:tidy:20260102-030405", "cron:tidy",
    ], ids=["a run", "a run of an older release", "the stable session of an old persistent job"])
    async def test_the_jobs_own_runs_are_not_activity(self, db, work, session_id):
        """A run of the job must not be the reason for its next run."""
        await _ran(db, "tidy", minutes_ago=60)
        await work(db, session_id, minutes_ago=59)

        assert await self._open(db) is False

    @WORK
    async def test_the_runs_of_another_job_are_activity(self, db, work):
        await _ran(db, "tidy", minutes_ago=60)
        await work(db, _run_session("report"), minutes_ago=30)

        assert await self._open(db) is True

    @WORK
    async def test_the_runs_of_an_ignored_job_are_not_activity(self, db, work):
        await _ran(db, "tidy", minutes_ago=60)
        await work(db, _run_session("sweep"), minutes_ago=30)
        await work(db, "cron:sweep", minutes_ago=30)

        assert await self._open(db, AgentActivityGate(ignore_jobs=["sweep"])) is False
        assert await self._open(db, AgentActivityGate(ignore_jobs=["other"])) is True

    @WORK
    @pytest.mark.parametrize(
        "session_id", [_run_session("tidy:deep"), "cron:tidy:deep"],
        ids=["a run of it", "its stable session"],
    )
    async def test_a_job_whose_id_extends_this_one_is_another_job(self, db, work, session_id):
        """What follows ``cron:tidy:`` in a session of ``tidy`` is a run id:
        these are sessions of the job ``tidy:deep``."""
        await _ran(db, "tidy", minutes_ago=60)
        await work(db, session_id, minutes_ago=30)

        assert await self._open(db) is True
        assert await self._open(db, AgentActivityGate(ignore_jobs=["tidy:deep"])) is False

    @pytest.mark.parametrize("job_id", ["50%_off", "a*b[c]?", "it's \"quoted\"", "задача"])
    async def test_a_job_id_is_text_not_a_pattern(self, db, job_id):
        await _ran(db, job_id, minutes_ago=60)
        await _turn(db, _run_session(job_id), minutes_ago=30)
        assert await self._open(db, job_id=job_id) is False

        await _turn(db, _run_session("50x_off"), minutes_ago=30)
        await _turn(db, _run_session("axb[c]?"), minutes_ago=30)
        assert await self._open(db, job_id=job_id) is True

    @pytest.mark.parametrize("started_by", ["cron:tidy", "session:cron:tidy", "session:{run}"])
    async def test_a_workflow_run_the_job_started_is_its_own_run(self, db, started_by):
        """A workflow-run job does its work in ``workflow:<run>``, and a
        prompt job can start a workflow run from its session. Counted as
        activity, either would start the job again at every scheduled time."""
        await _ran(db, "tidy", minutes_ago=60)
        mine = await _workflow_run(db, "run-1", started_by.format(run=_run_session("tidy")))
        await _turn(db, mine, minutes_ago=30)
        await _stored_message(db, mine, minutes_ago=30)
        assert await self._open(db) is False
        assert await self._open(db, AgentActivityGate(ignore_jobs=["tidy"]), "other") is False

        theirs = await _workflow_run(db, "run-2", "cron:report")
        await _turn(db, theirs, minutes_ago=30)
        assert await self._open(db) is True
        assert await self._open(db, AgentActivityGate(ignore_jobs=["report"])) is False

    async def test_a_workflow_run_a_person_started_is_activity(self, db):
        await _ran(db, "tidy", minutes_ago=60)
        await _turn(db, await _workflow_run(db, "run-1", "session:chat-1"), minutes_ago=30)

        assert await self._open(db) is True

    async def test_a_failed_run_does_not_move_the_window(self, db):
        """What the agent did before a run that failed is still to be looked
        at: the next run is not skipped for it."""
        await _ran(db, "tidy", minutes_ago=120)
        await _turn(db, "chat-1", minutes_ago=90)
        await _ran(db, "tidy", minutes_ago=60, status="error")

        assert await self._open(db) is True

    @WORK
    async def test_the_overlap_counts_the_time_before_the_last_run_again(self, db, work):
        await work(db, "chat-1", minutes_ago=120)
        await _ran(db, "tidy", minutes_ago=60)

        assert await self._open(db, AgentActivityGate()) is False
        assert await self._open(db, AgentActivityGate(overlap_hours=2)) is True
        assert await self._open(db, AgentActivityGate(overlap_hours=0.5)) is False

    @pytest.mark.parametrize("hours_before_first_run", [15, 3], ids=["an afternoon turn", "a turn in the small hours"])
    async def test_a_daily_job_with_a_days_overlap_runs_twice_after_the_last_work(
        self, db, hours_before_first_run,
    ):
        """The job reviews the day before each run. Whenever the turn was,
        the two runs between them review the day it fell on, and then the job
        waits: no third run."""
        gate = AgentActivityGate(overlap_hours=24)
        await _turn(db, "chat-1", minutes_ago=60 * (72 + hours_before_first_run))
        assert await self._open(db, gate) is True   # the first scheduled time after it

        await _ran(db, "tidy", minutes_ago=60 * 72)
        assert await self._open(db, gate) is True   # a day later: once more

        await _ran(db, "tidy", minutes_ago=60 * 48)
        assert await self._open(db, gate) is False  # and then it waits

    async def test_jobs_with_this_gate_are_not_activity_for_each_other(self, db):
        """One piece of work, then nothing: each job runs once for it. Neither
        names the other. A run of one that counted for the other would start
        it, whose run would start the first again, for as long as the
        instance lives."""
        gated = [{"type": "agent_activity"}]
        memory = CronJob(id="memory", schedule="1h", prompt="p", run_if=gated)
        skills = CronJob(id="skills", schedule="1h", prompt="p", run_if=gated)
        digest = CronJob(id="digest", schedule="1h", prompt="p")
        jobs = [memory, skills, digest]
        await _turn(db, "chat-1", minutes_ago=50)
        assert await self._open(db, memory.gates[0], "memory", jobs) is True
        assert await self._open(db, skills.gates[0], "skills", jobs) is True

        await _ran(db, "memory", minutes_ago=40)
        await _turn(db, _run_session("memory"), minutes_ago=39)
        assert await self._open(db, memory.gates[0], "memory", jobs) is False
        assert await self._open(db, skills.gates[0], "skills", jobs) is True

        await _ran(db, "skills", minutes_ago=30)
        await _turn(db, _run_session("skills"), minutes_ago=29)
        assert await self._open(db, memory.gates[0], "memory", jobs) is False
        assert await self._open(db, skills.gates[0], "skills", jobs) is False
        # Without the other jobs to look at, the run of ``skills`` is work.
        assert await self._open(db, memory.gates[0], "memory") is True

        # A job without the gate is work for both.
        await _turn(db, _run_session("digest"), minutes_ago=20)
        assert await self._open(db, memory.gates[0], "memory", jobs) is True
        assert await self._open(db, skills.gates[0], "skills", jobs) is True

    @WORK
    async def test_a_catch_up_counts_only_what_was_recorded_before_the_run_came_due(self, db, work):
        """A catch-up stands in for a run that came due while the machine was
        paused. What was recorded since, the message that woke the machine
        above all, is work for the job's next scheduled time."""
        gate = AgentActivityGate()
        await _ran(db, "tidy", minutes_ago=600)
        await work(db, "chat-1", minutes_ago=1)

        async def open_as_of(minutes_ago: float | None) -> bool:
            as_of = _ago(minutes_ago) if minutes_ago is not None else None
            return await gate.is_satisfied(GateContext(job_id="tidy", db=db, as_of=as_of))

        assert await open_as_of(60) is False
        assert await open_as_of(None) is True

        await work(db, "chat-2", minutes_ago=300)
        assert await open_as_of(60) is True
        assert await open_as_of(301) is False

    async def test_the_time_a_catch_up_asks_about_names_its_zone(self, db):
        """A time without a zone would be read as the machine's local time."""
        naive = datetime.now(UTC).replace(tzinfo=None)

        with pytest.raises(ValueError, match="timezone-aware"):
            await db.has_activity_since_cron_run("tidy", until=naive)

    async def test_the_legs_of_a_review_loop_the_job_started_are_its_own_run(self, db):
        """The legs run in workflow sessions that name the loop, and the loop
        names the session that started it."""
        async def loop(loop_id: str, session_id: str) -> str:
            if await db.get_session(session_id) is None:
                await db.create_session(session_id, source="test", actor=None)
            await db.create_review_loop(
                loop_id, title="t", session_id=session_id, goal_prompt="g", verifier_prompt="v",
                criteria_adoption="no", criteria=[], implementer={}, verifier={}, cwd=None,
                max_iterations=1, budget_usd=1.0,
            )
            return await _workflow_run(db, f"leg-{loop_id}", f"review-loop:{loop_id}")

        await _ran(db, "tidy", minutes_ago=60)
        await _turn(db, await loop("mine", _run_session("tidy")), minutes_ago=30)
        assert await self._open(db) is False
        assert await self._open(db, AgentActivityGate(ignore_jobs=["tidy"]), "other") is False

        await _turn(db, await loop("theirs", "chat-1"), minutes_ago=30)
        assert await self._open(db) is True


class TestAgentActivityGateConfig:
    def test_from_config(self):
        gate = build_gate({"type": "agent_activity"})
        assert isinstance(gate, AgentActivityGate)
        assert (gate.ignore_jobs, gate.overlap_hours) == ([], 0.0)

        gate = build_gate({
            "type": "agent_activity", "ignore_jobs": ["a", " b ", "a"], "overlap_hours": 24,
        })
        assert (gate.ignore_jobs, gate.overlap_hours) == (["a", "b"], 24.0)
        assert build_gate({"type": "agent_activity", "ignore_jobs": "a"}).ignore_jobs == ["a"]
        # A bare key is the one way to say "not set".
        bare = build_gate({"type": "agent_activity", "ignore_jobs": None, "overlap_hours": None})
        assert (bare.ignore_jobs, bare.overlap_hours) == ([], 0.0)

    @pytest.mark.parametrize("bad", [0, False, "", {}, {"a": 1}, [""], ["a", 3], [None]])
    def test_a_malformed_ignore_list_is_refused(self, bad):
        """Read as an empty list, it would count the runs the spec left out."""
        with pytest.raises(GateConfigError, match="ignore_jobs"):
            build_gate({"type": "agent_activity", "ignore_jobs": bad})

    @pytest.mark.parametrize("bad", [
        -1, True, "24", [24], float("nan"), float("inf"), 24 * 366 + 1, 10 ** 400,
    ])
    def test_a_malformed_overlap_is_refused(self, bad):
        """Refused as a gate that does not build, which costs the one job. An
        integer too large for a float must not escape as another error: that
        would cost every job of the file."""
        with pytest.raises(GateConfigError, match="overlap_hours"):
            build_gate({"type": "agent_activity", "overlap_hours": bad})

    def test_describe(self):
        assert AgentActivityGate().describe() == "agent activity since this job's last run"
        described = AgentActivityGate(ignore_jobs=["a", "b"], overlap_hours=24).describe()
        assert described == (
            "agent activity since 24h before this job's last run, "
            "not counting the runs of a, b"
        )

    def test_only_the_gates_that_estimate_let_a_manual_run_through(self):
        """They estimate whether a run is worth it. The other gates say
        whether the job has anything to act on."""
        unchecked = [key for key, cls in GATE_REGISTRY.items() if not cls.checked_on_manual_runs]
        assert unchecked == ["agent_activity", "plans_in_review"]


# ---------------------------------------------------------------------------
# AgentActivityGate on real turns and real runs
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def agent_db(db):
    """The database with the agent's identity, as a real engine needs it."""
    await ensure_system_principal(db)
    return db


async def _age(db, minutes: int) -> None:
    """Move everything recorded so far that long into the past."""
    step = f"-{minutes} minutes"
    await db.db.execute("UPDATE session_usage SET created_at = datetime(created_at, ?)", (step,))
    await db.db.execute("UPDATE sessions SET updated_at = datetime(updated_at, ?)", (step,))
    await db.db.execute("UPDATE cron_logs SET started_at = datetime(started_at, ?)", (step,))
    await db.db.commit()


OK = [("text", "done"), ("result", "sdk-ok")]


@pytest.mark.asyncio
class TestAgentActivityOfRealTurns:
    """Every way a turn can end leaves a record the gate reads, and leaves it
    before the session stops counting as running. That is the moment the next
    scheduled task is forecast again: a platform that parks an idle machine
    must hear then that the job will run."""

    @staticmethod
    def _asked_when_a_session_stops(engine, db) -> tuple[list[bool], list[asyncio.Future]]:
        answers: list[bool] = []
        asked: list[asyncio.Future] = []

        async def ask() -> None:
            answers.append(await AgentActivityGate().is_satisfied(GateContext(job_id="tidy", db=db)))

        def stopped(_session_id: str, running: bool) -> None:
            if not running:
                asked.append(asyncio.ensure_future(ask()))

        engine.sessions.add_running_listener(stopped)
        return answers, asked

    @pytest.mark.parametrize("script, status", [
        (OK, "completed"),
        ([("text", "half an answer"), ("raise",)], "failed"),
        ([("text", "done"), ("result", "sdk-1", {"usage": None})], "completed"),
    ], ids=["completed", "the runtime died", "a result without usage"])
    async def test_a_turn_is_activity_however_it_ends(self, agent_db, tmp_path, script, status):
        engine, _ = await _engine(agent_db, tmp_path, [script])
        await _session(engine, "chat")
        await _ran(agent_db, "tidy")
        await _age(agent_db, 10)
        gate = AgentActivityGate()
        assert await gate.is_satisfied(_ctx(agent_db, "tidy")) is False
        answers, asked = self._asked_when_a_session_stops(engine, agent_db)

        result = await _real_turn(engine, "chat")
        await asyncio.gather(*asked)

        assert result.outcome.status == status
        assert answers == [True]

    async def test_a_turn_that_was_stopped_is_activity(self, agent_db, tmp_path):
        engine, _ = await _engine(agent_db, tmp_path, [[("text", "x"), ("hang",)]])
        await _session(engine, "chat")
        await _ran(agent_db, "tidy")
        await _age(agent_db, 10)
        answers, asked = self._asked_when_a_session_stops(engine, agent_db)
        token = await _token_seen("chat")
        try:
            turn = asyncio.create_task(_real_turn(engine, "chat"))
            await asyncio.wait_for(token.wait(), 5)
            turn.cancel()
            result = await turn
        finally:
            await broadcaster.unregister("chat", "turn-outcome-test")
        await asyncio.gather(*asked)

        assert result.outcome.status == "interrupted"
        assert answers == [True]


@pytest.mark.asyncio
class TestAgentActivityOfRealRuns:
    """The gate on runs of the real scheduler, with the session ids the code
    itself makes: one piece of work, then several scheduled times with the
    agent otherwise idle."""

    GATED = [{"type": "agent_activity"}]

    async def _scheduled_times(self, svc: CronService, job: CronJob, times: int, settle=None) -> list[dict]:
        for _ in range(times):
            await svc._run_job_inner(job)
            if settle is not None:
                await settle()
            await _age(svc.db, 10)
        return await svc.db.get_cron_logs(job.id)

    async def _one_chat_turn(self, engine, db) -> None:
        await _session(engine, "chat")
        await _real_turn(engine, "chat")
        await _age(db, 10)

    @pytest.mark.parametrize("mode", ["isolated", "persistent"])
    async def test_a_job_runs_once_for_one_piece_of_work(self, agent_db, tmp_path, mode):
        engine, _ = await _engine(agent_db, tmp_path, [OK])
        svc = CronService(engine.config, engine, agent_db)
        job = CronJob(id="tidy", schedule="1h", prompt="p", session_mode=mode,
                      context_rotate_hours=0, run_if=self.GATED)
        assert await self._scheduled_times(svc, job, 1) == []  # nothing has happened yet

        await self._one_chat_turn(engine, agent_db)
        logs = await self._scheduled_times(svc, job, 3)

        assert [log["status"] for log in logs] == ["success"]

    async def test_a_workflow_run_job_runs_once_for_one_piece_of_work(
        self, agent_db, tmp_path, monkeypatch,
    ):
        """Its work is done in ``workflow:<run>``, not in a cron session."""
        engine, _ = await _engine(agent_db, tmp_path, [OK])
        service = WorkflowRunService(_make_config(tmp_path), agent_db, engine)
        monkeypatch.setattr(nerve.workflows, "_service", service)
        svc = CronService(engine.config, engine, agent_db)
        job = CronJob(id="nightly-audit", schedule="1h", run_if=self.GATED,
                      workflow={"engine": ENGINE_CLAUDE, "prompt": "audit", "budget_usd": 1.0})
        await self._one_chat_turn(engine, agent_db)

        logs = await self._scheduled_times(svc, job, 4, settle=lambda: _drain(service))

        assert len(logs) == 1
        assert logs[0]["session_id"].startswith("workflow:")

    async def test_jobs_with_the_gate_do_not_start_each_other(self, agent_db, tmp_path):
        """Neither names the other: the scheduler tells each gate which jobs
        carry it. One piece of work, then six scheduled times."""
        engine, _ = await _engine(agent_db, tmp_path, [OK])
        svc = CronService(engine.config, engine, agent_db)
        memory = CronJob(id="memory", schedule="1h", prompt="p", run_if=self.GATED)
        skills = CronJob(id="skills", schedule="1h", prompt="p", run_if=self.GATED)
        svc._jobs = [memory, skills]
        await self._one_chat_turn(engine, agent_db)

        for _ in range(3):
            await self._scheduled_times(svc, memory, 1)
            await self._scheduled_times(svc, skills, 1)

        assert len(await agent_db.get_cron_logs("memory")) == 1
        assert len(await agent_db.get_cron_logs("skills")) == 1

    @pytest.mark.parametrize("origin", ["schedule", "catchup", "delivery"])
    async def test_a_run_that_nobody_started_is_checked(self, agent_db, tmp_path, origin):
        engine, _ = await _engine(agent_db, tmp_path, [OK])
        svc = CronService(engine.config, engine, agent_db)
        job = CronJob(id="tidy", schedule="1h", prompt="p", run_if=self.GATED)
        svc._jobs = [job]

        await svc._run_job_wrapper(job, origin=origin)

        assert await agent_db.get_cron_logs("tidy") == []

    @pytest.mark.parametrize("worked_before", [False, True])
    async def test_the_startup_catch_up_asks_about_its_own_start(
        self, agent_db, tmp_path, monkeypatch, worked_before,
    ):
        """A run that the gate skipped leaves no row, so the job counts as
        overdue at every start. A message that arrives while the catch-up is
        under way is work for the job's next scheduled time; work from before
        the start is what the catch-up is for."""
        engine, _ = await _engine(agent_db, tmp_path, [OK])
        svc = CronService(engine.config, engine, agent_db)
        job = CronJob(id="tidy", schedule="1h", prompt="p", run_if=self.GATED)
        svc._jobs = [job]
        await _ran(agent_db, "tidy", minutes_ago=180)
        await agent_db.db.execute("UPDATE cron_logs SET finished_at = ?", (_ago(179).isoformat(),))
        await agent_db.db.commit()
        if worked_before:
            await _turn(agent_db, "chat-0", minutes_ago=30)
        last_run = agent_db.get_last_successful_cron_run

        async def then_a_message_arrives(job_id: str):
            row = await last_run(job_id)
            await _stored_message(agent_db, "chat-1")
            return row

        monkeypatch.setattr(agent_db, "get_last_successful_cron_run", then_a_message_arrives)

        await svc._catchup_missed_jobs()

        runs = [log["status"] for log in await agent_db.get_cron_logs("tidy")]
        assert len(runs) == (2 if worked_before else 1)

    async def test_a_run_that_a_person_starts_is_not_checked(self, agent_db, tmp_path):
        """The gate estimates whether a run is worth it. A person who starts
        the job may know of work it cannot see, such as a memory written
        through the MCP endpoint. A gate that says whether the job has
        anything to act on still holds."""
        engine, _ = await _engine(agent_db, tmp_path, [OK])
        svc = CronService(engine.config, engine, agent_db)
        tidy = CronJob(id="tidy", schedule="1h", prompt="p", run_if=self.GATED)
        planner = CronJob(id="planner", schedule="1h", prompt="p",
                          run_if=[*self.GATED, {"type": "tasks", "status": "pending"}])
        svc._jobs = [tidy, planner]

        await svc._run_job_wrapper(tidy)  # as the scheduler starts it
        assert await agent_db.get_cron_logs("tidy") == []

        await svc.run_job("tidy")
        await svc.run_job("planner")

        assert [log["status"] for log in await agent_db.get_cron_logs("tidy")] == ["success"]
        assert await agent_db.get_cron_logs("planner") == []


# ---------------------------------------------------------------------------
# PlansInReviewGate
# ---------------------------------------------------------------------------

async def _plan(
    db, task_id: str, session_id: str | None, *, status: str = "pending", version: int = 1,
) -> str:
    """A plan for ``task_id`` (made on demand) that ``session_id`` proposed."""
    if await db.get_task(task_id) is None:
        await db.upsert_task(task_id=task_id, file_path=f"{task_id}.md", title=task_id)
    plan_id = f"plan-{task_id}-v{version}"
    await db.create_plan(
        plan_id=plan_id, task_id=task_id, content="What, evidence, direction.",
        session_id=session_id, version=version,
    )
    if status != "pending":
        await db.update_plan(plan_id, status=status)
    return plan_id


# What a plan becomes when it is decided: declined, or approved and then
# under way, finished or failed.
DECIDED = ["declined", "implementing", "done", "failed"]


@pytest.mark.asyncio
class TestPlansInReviewGate:
    async def _open(self, db, below: int = 3, job_id: str = "scout") -> bool:
        return await PlansInReviewGate(below).is_satisfied(GateContext(job_id=job_id, db=db))

    async def test_a_job_that_proposed_nothing_runs(self, db):
        assert await self._open(db) is True

    async def test_the_job_stops_at_the_number_and_not_before(self, db):
        for task in ("t1", "t2"):
            await _plan(db, task, _run_session("scout"))
        assert await self._open(db) is True

        await _plan(db, "t3", _run_session("scout"))
        assert await self._open(db) is False

        await _plan(db, "t4", _run_session("scout"))
        assert await self._open(db) is False
        assert await self._open(db, below=5) is True

    async def test_below_one_lets_no_proposal_wait(self, db):
        assert await self._open(db, below=1) is True

        await _plan(db, "t1", "cron:scout")
        assert await self._open(db, below=1) is False

    @pytest.mark.parametrize("status", [*DECIDED, "superseded"])
    async def test_only_a_plan_in_review_counts(self, db, status):
        for task in ("t1", "t2", "t3"):
            await _plan(db, task, "cron:scout", status=status)

        assert await db.count_cron_job_plans_in_review("scout") == 0
        assert await self._open(db) is True

    @pytest.mark.parametrize("session_id", [
        "cron:scout", "cron:scout:20260102-030405-0a1b2c3d", "cron:scout:20260102-030405",
    ], ids=["the one session of a job", "a run or a generation", "a run of an older release"])
    async def test_a_plan_from_any_session_of_the_job_counts(self, db, session_id):
        await _plan(db, "t1", session_id)

        assert await db.count_cron_job_plans_in_review("scout") == 1

    @pytest.mark.parametrize("session_id", [
        None, "chat-1", "impl-0a1b2c3d", "cron:planner", "cron:planner:20260102-030405-0a1b2c3d",
        "cron:scout-2", "cron:scout:deep", "cron:scout:deep:20260102-030405-0a1b2c3d", "scout",
    ])
    async def test_what_anything_else_proposed_does_not_count(self, db, session_id):
        """A chat, an implementation session, another job: also one whose id
        begins with this job's."""
        for task in ("t1", "t2", "t3"):
            await _plan(db, task, session_id)

        assert await db.count_cron_job_plans_in_review("scout") == 0
        assert await self._open(db) is True

    async def test_each_job_counts_its_own_proposals(self, db):
        for task in ("t1", "t2", "t3"):
            await _plan(db, task, _run_session("scout"))
        await _plan(db, "t4", _run_session("planner"))

        assert await self._open(db, job_id="scout") is False
        assert await self._open(db, job_id="planner") is True

    @pytest.mark.parametrize("job_id", ["50%_off", "a*b[c]?", "it's \"quoted\"", "задача"])
    async def test_a_job_id_is_text_not_a_pattern(self, db, job_id):
        await _plan(db, "t1", _run_session("50x_off"))
        await _plan(db, "t2", _run_session("axb[c]?"))
        assert await db.count_cron_job_plans_in_review(job_id) == 0

        await _plan(db, "t3", _run_session(job_id))
        await _plan(db, "t4", f"cron:{job_id}")
        assert await db.count_cron_job_plans_in_review(job_id) == 2

    @pytest.mark.parametrize("started_by", ["cron:scout", "session:cron:scout", "session:{run}"])
    async def test_a_workflow_run_the_job_started_proposes_for_the_job(self, db, started_by):
        """A workflow-run job does its work in ``workflow:<run>``, and a
        prompt job can start a workflow run from its session."""
        mine = await _workflow_run(db, "run-1", started_by.format(run=_run_session("scout")))
        theirs = await _workflow_run(db, "run-2", "session:chat-1")
        await _plan(db, "t1", mine)
        await _plan(db, "t2", theirs)

        assert await db.count_cron_job_plans_in_review("scout") == 1

    @pytest.mark.parametrize("reviser", ["cron:scout", "cron:task-planner", "chat-1", None])
    async def test_a_revision_counts_with_the_plan_it_replaces(self, db, reviser):
        """Whoever wrote it: the job's own session, the planner session that
        takes the revision when the job's session was deleted, a person's
        chat. The proposal is the same one, and still waits."""
        await _plan(db, "t1", "cron:scout", status="superseded")
        await _plan(db, "t1", reviser, version=2)

        assert await db.count_cron_job_plans_in_review("scout") == 1

    async def test_a_revision_of_anothers_plan_is_not_the_jobs_proposal(self, db):
        await _plan(db, "t1", "chat-1", status="superseded")
        await _plan(db, "t1", "chat-1", version=2)

        assert await db.count_cron_job_plans_in_review("scout") == 0

    async def test_a_revision_of_a_revision_counts(self, db):
        await _plan(db, "t1", "cron:scout", status="superseded")
        await _plan(db, "t1", "chat-1", status="superseded", version=2)
        await _plan(db, "t1", "chat-2", version=3)

        assert await db.count_cron_job_plans_in_review("scout") == 1

    @pytest.mark.parametrize("decided", DECIDED)
    async def test_a_revised_proposal_that_was_decided_no_longer_waits(self, db, decided):
        await _plan(db, "t1", "cron:scout", status="superseded")
        await _plan(db, "t1", "cron:scout", status=decided, version=2)

        assert await db.count_cron_job_plans_in_review("scout") == 0

    @pytest.mark.parametrize("decided", DECIDED)
    async def test_a_task_with_no_plan_in_review_has_no_proposal(self, db, decided):
        """Also when a plan of the job was replaced and nothing took its
        place, as a revision that fails half-way leaves it: the one plan of
        the task that is not replaced was decided."""
        await _plan(db, "t1", "chat-1", status=decided)
        await _plan(db, "t1", "cron:scout", status="superseded", version=2)

        assert await db.count_cron_job_plans_in_review("scout") == 0

    @pytest.mark.parametrize("decided", DECIDED)
    @pytest.mark.parametrize("revised", [False, True], ids=["as proposed", "after a revision"])
    async def test_a_plan_for_a_task_whose_earlier_proposal_was_decided_is_not_the_jobs(
        self, db, decided, revised,
    ):
        """The job's proposal for the task was decided, as it was proposed or
        after a revision. A plan that a chat proposes for the task later
        replaces nothing of the job's."""
        if revised:
            await _plan(db, "t1", "cron:scout", status="superseded", version=0)
        await _plan(db, "t1", "cron:scout", status=decided)
        await _plan(db, "t1", "chat-1", version=2)

        assert await db.count_cron_job_plans_in_review("scout") == 0

    async def test_the_jobs_next_proposal_for_a_task_counts_after_a_decision(self, db):
        await _plan(db, "t1", "cron:scout", status="superseded", version=0)
        await _plan(db, "t1", "cron:scout", status="declined")
        await _plan(db, "t1", "cron:scout", version=2)

        assert await db.count_cron_job_plans_in_review("scout") == 1

    @pytest.mark.parametrize("status", [
        "pending", "plan_review", "in_progress", "backlog", "on_hold", "done",
    ])
    async def test_a_proposal_waits_whatever_became_of_its_task(self, db, status):
        """The plan tools move a task to ``plan_review`` only from some
        statuses, and a person can move it on, also to ``done``: closing a
        task decides no plan, and the Plans page goes on listing it."""
        await _plan(db, "t1", "cron:scout")
        await db.upsert_task(task_id="t1", file_path="t1.md", title="t1", status=status)

        assert (await db.get_plan("plan-t1-v1"))["status"] == "pending"
        assert await db.count_cron_job_plans_in_review("scout") == 1

    async def test_a_plan_proposed_for_a_task_that_is_done_waits(self, db):
        """The job comes back to an idea whose task was closed: the plan
        tools leave the task done and put the plan in review."""
        await db.upsert_task(task_id="t1", file_path="t1.md", title="t1", status="done")
        await _plan(db, "t1", "cron:scout", status="declined")
        await _plan(db, "t1", "cron:scout", version=2)

        assert (await db.get_task("t1"))["status"] == "done"
        assert await db.count_cron_job_plans_in_review("scout") == 1

    async def test_a_decided_plan_of_another_task_ends_nothing(self, db):
        await _plan(db, "t1", "cron:scout")
        await _plan(db, "t2", "chat-1", status="superseded")
        await _plan(db, "t2", "chat-1", status="declined", version=2)

        assert await db.count_cron_job_plans_in_review("scout") == 1

    async def test_a_plan_in_review_beside_a_decided_one_of_its_version_waits(self, db):
        """Two calls that propose for one task at the same moment can leave
        two plans of one version. Approving one leaves the other in review,
        where the Plans page lists it."""
        await _plan(db, "t1", "cron:scout")
        await db.create_plan(
            plan_id="plan-twin", task_id="t1", content="x", session_id="cron:scout", version=1,
        )
        await db.update_plan("plan-twin", status="implementing")

        assert await db.count_cron_job_plans_in_review("scout") == 1

    async def test_a_plan_without_a_task_does_not_count(self, db):
        """Nobody could decide it: every decision needs the task."""
        await db.create_plan(
            plan_id="plan-adrift", task_id="no-such-task", content="x", session_id="cron:scout",
        )

        assert await db.count_cron_job_plans_in_review("scout") == 0

    async def test_a_proposal_outlives_the_session_that_proposed_it(self, db):
        """A plan keeps the id of its session. Deleting the chat of a run, or
        of a workflow run that the job started, leaves the proposal the
        job's: the link from a workflow run to its session goes with the
        session, the run id in the session's id does not."""
        cron_session = _run_session("scout")
        await db.create_session(cron_session, source="cron", actor=None)
        workflow_session = await _workflow_run(db, "run-1", "cron:scout")
        await _plan(db, "t1", cron_session)
        await _plan(db, "t2", workflow_session)
        await _plan(db, "t3", await _workflow_run(db, "run-2", "session:chat-1"))
        assert await db.count_cron_job_plans_in_review("scout") == 2

        for session_id in (cron_session, workflow_session, "workflow:run-2"):
            assert await db.delete_session(session_id) is True

        assert await db.count_cron_job_plans_in_review("scout") == 2

    async def test_a_session_named_like_a_workflow_run_is_not_one(self, db):
        await _workflow_run(db, "run-1", "cron:scout")
        for task, session_id in (("t1", "workflow:"), ("t2", "workflow:run-9"), ("t3", "run-1"),
                                 ("t4", "chat:workflow:run-1"), ("t5", "nine-charrun-1"),
                                 ("t6", "telegram:run-1")):
            await _plan(db, task, session_id)

        assert await db.count_cron_job_plans_in_review("scout") == 0

    async def test_two_plans_in_review_for_one_task_are_one_proposal(self, db):
        """The tools keep one plan in review per task. Should two exist, the
        person still has one proposal to decide."""
        await _plan(db, "t1", "cron:scout")
        await _plan(db, "t1", "cron:scout", version=2)

        assert await db.count_cron_job_plans_in_review("scout") == 1

    async def test_the_gate_counts_now_whatever_time_it_is_asked_about(self, db):
        """A catch-up asks about the time its run came due. A plan has one
        status, the current one, so the gate cannot answer for then."""
        for task in ("t1", "t2", "t3"):
            await _plan(db, task, "cron:scout")
        gate = PlansInReviewGate()
        ctx = GateContext(job_id="scout", db=db, as_of=_ago(60 * 24 * 365))

        assert await gate.is_satisfied(ctx) is False
        assert await gate.forecast(ctx, datetime.now(UTC) + timedelta(days=30)) is False


class TestPlansInReviewGateConfig:
    def test_from_config(self):
        gate = build_gate({"type": "plans_in_review"})
        assert isinstance(gate, PlansInReviewGate)
        assert gate.below == 3

        assert build_gate({"type": "plans_in_review", "below": 5}).below == 5
        assert build_gate({"type": "plans_in_review", "below": 1}).below == 1
        # A bare key is the one way to say "not set".
        assert build_gate({"type": "plans_in_review", "below": None}).below == 3

    @pytest.mark.parametrize("bad", [
        0, -1, True, False, "3", 3.0, 2.5, float("nan"), float("inf"), [3], {}, "",
    ])
    def test_a_malformed_number_is_refused(self, bad):
        """Zero would never let the job run, which is what ``enabled: false``
        says. Refused as a gate that does not build, which costs the one job."""
        with pytest.raises(GateConfigError, match="below"):
            build_gate({"type": "plans_in_review", "below": bad})
        with pytest.raises(GateConfigError, match="below"):
            PlansInReviewGate(bad)

    def test_a_number_of_any_size_builds(self):
        """It is compared with a count and never handed to the database."""
        assert build_gate({"type": "plans_in_review", "below": 10 ** 400}).below == 10 ** 400

    def test_describe(self):
        assert PlansInReviewGate().describe() == (
            "fewer than 3 of this job's proposals wait for review"
        )
        assert PlansInReviewGate(1).describe() == "none of this job's proposals waits for review"

    def test_its_one_field_is_known_to_validation(self):
        assert PlansInReviewGate.spec_keys == frozenset({"below"})


@pytest.mark.asyncio
class TestPlansInReviewOfRealProposals:
    """Proposals that the plan tools make, in a session with an id the code
    itself makes, and decisions that the plan service takes."""

    @staticmethod
    def _tools(db, tmp_path, session_id: str) -> ToolContext:
        return ToolContext(
            session_id=session_id, workspace=tmp_path, db=db,
            notification_service=None, actor=db.system_actor,
        )

    @staticmethod
    async def _proposed(ctx: ToolContext, title: str) -> tuple[str, str]:
        import re

        created = await task_create_handler(ctx, {"title": title, "content": "What it is for."})
        task_id = re.match(r"Task created: (\S+)", created.content[0]["text"]).group(1)
        await plan_propose_handler(ctx, {"task_id": task_id, "content": "What, evidence, direction."})
        (plan,) = [p for p in await ctx.db.get_plans_for_task(task_id) if p["status"] == "pending"]
        return task_id, plan["id"]

    async def test_a_job_waits_at_three_proposals_and_goes_on_after_a_decision(self, agent_db, tmp_path):
        db = agent_db
        engine = FakeEngine(db)
        tools_mod.init_tools(workspace=tmp_path, db=db, engine=engine)
        scout = self._tools(db, tmp_path, _run_session("scout"))
        gate, ctx = PlansInReviewGate(3), GateContext(job_id="scout", db=db)

        first = await self._proposed(scout, "Release notes from merged pull requests")
        second = await self._proposed(scout, "Weekly audit of dependencies")
        assert await gate.is_satisfied(ctx) is True
        third = await self._proposed(scout, "Triage of flaky tests")
        assert await gate.is_satisfied(ctx) is False

        # What a chat proposes is not the job's.
        await self._proposed(self._tools(db, tmp_path, "chat-1"), "Fix the encoder")
        assert await db.count_cron_job_plans_in_review("scout") == 3

        # Sent back for a revision, the proposal still waits; so does the
        # revision, here written in another session.
        await request_plan_revision(db, engine, first[1], "Narrower, please.", actor=None, cause=None)
        assert await gate.is_satisfied(ctx) is False
        await plan_update_handler(
            self._tools(db, tmp_path, "cron:task-planner"),
            {"plan_id": first[1], "content": "Narrower."},
        )
        assert (await db.get_plan(first[1]))["status"] == "superseded"
        assert await db.count_cron_job_plans_in_review("scout") == 3

        # A task that somebody closes by hand leaves its plan in review, where
        # the Plans page lists it: the proposal still waits for a decision.
        await task_done_handler(scout, {"task_id": third[0]})
        assert (await db.get_plan(third[1]))["status"] == "pending"
        assert await gate.is_satisfied(ctx) is False

        await decline_plan(db=db, engine=engine, plan_id=third[1], actor=None, cause=None)
        assert await gate.is_satisfied(ctx) is True

        await self._proposed(scout, "Rotation of the on-call notes")
        assert await gate.is_satisfied(ctx) is False

        engine.run_event.clear()  # the revision request above ran a turn too
        await approve_plan(db=db, engine=engine, plan_id=second[1], actor=None, cause=None)
        await asyncio.wait_for(engine.run_event.wait(), timeout=5.0)
        assert (await db.get_plan(second[1]))["status"] == "implementing"
        assert await gate.is_satisfied(ctx) is True


@pytest.mark.asyncio
class TestPlansInReviewOfRealRuns:
    """The gate on runs of the real scheduler. Each run that happens proposes
    one plan in its own session, whose id the scheduler made."""

    GATED = [{"type": "plans_in_review", "below": 3}]

    @staticmethod
    async def _scheduled_time(svc: CronService, job: CronJob, **how) -> int:
        """One scheduled time of *job*. Returns the runs it has had so far."""
        before = len(await svc.db.get_cron_logs(job.id))
        await svc._run_job_wrapper(job, **how)
        logs = await svc.db.get_cron_logs(job.id)
        if len(logs) > before:
            await _plan(svc.db, f"proposal-{len(logs)}", logs[0]["session_id"])
        return len(logs)

    @pytest.mark.parametrize("mode", ["isolated", "persistent"])
    async def test_a_job_stops_after_three_proposals_until_one_is_decided(self, agent_db, tmp_path, mode):
        engine, _ = await _engine(agent_db, tmp_path, [OK])
        svc = CronService(engine.config, engine, agent_db)
        job = CronJob(id="scout", schedule="1h", prompt="p", session_mode=mode,
                      context_rotate_hours=0, run_if=self.GATED)
        svc._jobs = [job]

        runs = [await self._scheduled_time(svc, job) for _ in range(5)]
        assert runs == [1, 2, 3, 3, 3]

        await agent_db.update_plan("plan-proposal-2-v1", status="declined")
        runs = [await self._scheduled_time(svc, job) for _ in range(3)]
        assert runs == [4, 4, 4]

    @pytest.mark.parametrize("origin", ["schedule", "catchup", "delivery"])
    async def test_a_run_that_nobody_started_is_checked(self, agent_db, tmp_path, origin):
        engine, _ = await _engine(agent_db, tmp_path, [OK])
        svc = CronService(engine.config, engine, agent_db)
        job = CronJob(id="scout", schedule="1h", prompt="p", run_if=self.GATED)
        svc._jobs = [job]
        for task in ("t1", "t2", "t3"):
            await _plan(agent_db, task, "cron:scout")

        assert await self._scheduled_time(svc, job, origin=origin) == 0

    async def test_a_run_that_a_person_starts_is_not_checked(self, agent_db, tmp_path):
        """The gate estimates whether another proposal is worth making. A
        person who starts the job wants one."""
        engine, _ = await _engine(agent_db, tmp_path, [OK])
        svc = CronService(engine.config, engine, agent_db)
        job = CronJob(id="scout", schedule="1h", prompt="p", run_if=self.GATED)
        svc._jobs = [job]
        for task in ("t1", "t2", "t3"):
            await _plan(agent_db, task, "cron:scout")

        await svc._run_job_wrapper(job)  # as the scheduler starts it
        assert await agent_db.get_cron_logs("scout") == []

        await svc.run_job("scout")
        assert [log["status"] for log in await agent_db.get_cron_logs("scout")] == ["success"]


# ---------------------------------------------------------------------------
# The default gates of built-in jobs
# ---------------------------------------------------------------------------

def _load(tmp_path, system: list[dict] | None, user: list[dict] | None = None,
          backend: str = "memu", strict: bool = False) -> dict[str, CronJob]:
    """The jobs the scheduler loads from the two files, by id."""
    import yaml

    for name, jobs in (("system.yaml", system), ("jobs.yaml", user)):
        if jobs is not None:
            (tmp_path / name).write_text(yaml.safe_dump({"jobs": jobs}), encoding="utf-8")
    service = SimpleNamespace(
        config=SimpleNamespace(
            cron=SimpleNamespace(
                system_file=tmp_path / "system.yaml", jobs_file=tmp_path / "jobs.yaml",
            ),
            memory=SimpleNamespace(backend=backend),
        ),
        _rejected_job_ids=[],
    )
    return {job.id: job for job in CronService._load_merged_jobs(service, strict=strict)}


def _system_yaml(backend: str = "memu", mode: str = "worker") -> list[dict]:
    """The system.yaml job list the installer writes."""
    from nerve.setup_writer import DEFAULT_CRONS, SetupChoices, build_cron_jobs

    return build_cron_jobs(SetupChoices(
        mode=mode, enabled_crons=list(DEFAULT_CRONS[mode]), memory_backend=backend,
    ))


def _gates(job: CronJob) -> list[tuple]:
    return [
        (type(g), g.below) if isinstance(g, PlansInReviewGate)
        else (type(g), g.ignore_jobs, g.overlap_hours)
        for g in job.gates
    ]


def _said_about_defaults(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if "default gates" in r.getMessage()]


class TestDefaultGatesOfBuiltInJobs:
    """The four maintenance jobs (memory-maintenance, skill-extractor,
    skill-reviser, machine-maintenance) wait for the agent to work, and
    skill-extractor also for its proposals to be decided. The gates are
    not in system.yaml, which nothing writes again when Nerve is updated:
    the scheduler gives them to an entry that states none, so an install from
    before them has them too, and a later change reaches all."""

    MEMORY = ["skill-extractor", "skill-reviser", "machine-maintenance"]
    SKILLS = ["memory-maintenance", "skill-reviser", "machine-maintenance"]
    REVISER = ["memory-maintenance", "skill-extractor", "machine-maintenance"]
    MACHINE = ["memory-maintenance", "skill-extractor", "skill-reviser"]
    GATED = {"memory-maintenance", "skill-extractor", "skill-reviser", "machine-maintenance"}
    #: skill-extractor's: the review of its proposals first, so that a job
    #: that waits for both names the one a person can do something about.
    EXTRACTOR = [(PlansInReviewGate, 3), (AgentActivityGate, SKILLS, 0.0)]

    @pytest.mark.parametrize("strict", [False, True], ids=["at startup", "on a reload"])
    def test_the_jobs_have_their_gates_without_the_file_stating_them(self, tmp_path, strict):
        written = {job["id"]: job for job in _system_yaml()}
        for job_id in self.GATED:
            assert "run_if" not in written[job_id]

        loaded = _load(tmp_path, list(written.values()), strict=strict)

        assert _gates(loaded["memory-maintenance"]) == [(AgentActivityGate, self.MEMORY, 24.0)]
        assert _gates(loaded["skill-extractor"]) == self.EXTRACTOR
        assert _gates(loaded["skill-reviser"]) == [(AgentActivityGate, self.REVISER, 0.0)]
        assert _gates(loaded["machine-maintenance"]) == [(AgentActivityGate, self.MACHINE, 0.0)]

    def test_the_memory_backend_decides_the_overlap_not_the_file(self, tmp_path):
        """memU's job reviews the records of the day before the run, so it
        looks back a day before its last run; Loom's checks the namespace as
        it is. The file here was written for memU, and the backend changed."""
        loaded = _load(tmp_path, _system_yaml("memu"), backend="loom")

        assert _gates(loaded["memory-maintenance"]) == [(AgentActivityGate, self.MEMORY, 0.0)]
        assert _gates(loaded["skill-extractor"]) == self.EXTRACTOR
        # The weekly jobs look at what there is, under either backend.
        assert _gates(loaded["skill-reviser"]) == [(AgentActivityGate, self.REVISER, 0.0)]
        assert _gates(loaded["machine-maintenance"]) == [(AgentActivityGate, self.MACHINE, 0.0)]

    @pytest.mark.parametrize("job_id", ["memory-maintenance", "machine-maintenance"])
    @pytest.mark.parametrize("said", [
        {"run_if": []},
        {"run_if": None},
        {"skip_when_idle": []},
        {"run_if": [{"type": "tasks", "status": "pending"}]},
    ])
    def test_an_entry_that_says_anything_about_gates_is_left_alone(self, tmp_path, said, job_id):
        """``run_if: []`` is how a file says "on every schedule": a weekly
        cleanup that must run whatever the agent did, for example."""
        jobs = _system_yaml()
        for job in jobs:
            if job["id"] == job_id:
                job.update(said)
        loaded = _load(tmp_path, jobs)[job_id]

        assert not any(isinstance(gate, AgentActivityGate) for gate in loaded.gates)
        assert len(loaded.gates) == len(said.get("run_if") or [])

    def test_an_edited_entry_without_run_if_gets_them_too(self, tmp_path):
        """The gates go by the job's id and the missing key, nothing else: the
        wording of the prompt has changed between releases, so it cannot say
        whether an entry is the shipped one."""
        jobs = _system_yaml()
        for job in jobs:
            if job["id"] == "skill-extractor":
                job.update(prompt="Look for what I do twice.", schedule="0 9 * * 1")
        loaded = _load(tmp_path, jobs)["skill-extractor"]

        assert loaded.prompt == "Look for what I do twice."
        assert _gates(loaded) == self.EXTRACTOR

    def test_no_other_built_in_job_gets_a_gate_it_does_not_state(self, tmp_path):
        """inbox-processor and task-planner have their gates in the file. An
        entry of theirs without one goes on running on every schedule."""
        jobs = _system_yaml(mode="personal")
        for job in jobs:
            job.pop("run_if", None)
        loaded = _load(tmp_path, jobs)

        gated = {job_id for job_id, job in loaded.items() if job.gates}
        assert gated == self.GATED
        assert {"inbox-processor", "task-planner"} <= set(loaded)

    def test_a_job_that_jobs_yaml_replaces_keeps_what_that_file_says(self, tmp_path, caplog):
        mine = {"id": "memory-maintenance", "schedule": "0 6 * * *", "prompt": "My own tidy-up."}
        with caplog.at_level("INFO", logger="nerve.cron.jobs"):
            loaded = _load(tmp_path, _system_yaml(), [mine])

        assert loaded["memory-maintenance"].prompt == "My own tidy-up."
        assert loaded["memory-maintenance"].gates == []
        others = self.GATED - {"memory-maintenance"}
        assert all(loaded[job_id].gates for job_id in others)
        said = _said_about_defaults(caplog)
        assert len(said) == len(others)
        assert {job_id for job_id in self.GATED if any(f"Cron job {job_id}:" in line for line in said)} == others

    def test_built_in_jobs_kept_in_jobs_yaml_are_left_alone(self, tmp_path):
        """An old install with every job in jobs.yaml and no system.yaml: that
        file is the user's."""
        loaded = _load(tmp_path, None, _system_yaml())

        for job_id in self.GATED:
            assert loaded[job_id].gates == []

    def test_the_load_says_which_gates_it_gave(self, tmp_path, caplog):
        with caplog.at_level("INFO", logger="nerve.cron.jobs"):
            _load(tmp_path, _system_yaml())

        said = _said_about_defaults(caplog)
        assert len(said) == len(self.GATED)
        assert all(
            "system.yaml" in line and "agent activity since" in line and "run_if: []" in line
            for line in said
        )
        (extractor,) = [line for line in said if line.startswith("Cron job skill-extractor:")]
        assert "fewer than 3 of this job's proposals wait for review; agent activity since" in extractor

    def test_only_the_scheduler_gives_them(self, tmp_path):
        """A plain load of the file returns what the file says: the default
        is the scheduler's to give."""
        import yaml

        from nerve.cron.jobs import load_jobs

        path = tmp_path / "system.yaml"
        path.write_text(yaml.safe_dump({"jobs": _system_yaml()}), encoding="utf-8")

        assert all(job.gates == [] for job in load_jobs(path) if job.id != "task-planner")

    def test_a_default_that_does_not_build_leaves_the_job_as_the_file_has_it(self, tmp_path, caplog):
        """The file never stated the gate: a default that fails must not cost
        the job, nor blame the entry."""
        import yaml

        from nerve.cron.jobs import load_jobs

        path = tmp_path / "system.yaml"
        path.write_text(yaml.safe_dump({"jobs": [
            {"id": "tidy", "schedule": "1h", "prompt": "p"},
            "not a mapping",
            {"id": "broken", "schedule": "1h"},
        ]}), encoding="utf-8")

        with caplog.at_level("INFO", logger="nerve.cron.jobs"):
            jobs = load_jobs(path, default_run_if=lambda job_id: [{"type": "no-such-gate"}])

        assert [(job.id, job.gates) for job in jobs] == [("tidy", [])]
        errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
        assert len(errors) == 1 and "tidy" in errors[0] and "no-such-gate" in errors[0]
        assert _said_about_defaults(caplog) == errors
        # An entry that is itself at fault is refused for what it lacks, with
        # or without a default that builds.
        path.write_text(yaml.safe_dump({"jobs": [{"id": "broken", "schedule": "1h"}]}), encoding="utf-8")
        for default in ([{"type": "tasks"}], [{"type": "no-such-gate"}]):
            with pytest.raises(Exception, match="needs a 'prompt'"):
                load_jobs(path, strict=True, default_run_if=lambda job_id, d=default: d)

    def test_the_default_is_asked_only_for_an_entry_without_the_keys(self, tmp_path):
        import yaml

        from nerve.cron.jobs import load_jobs

        entry = {"schedule": "1h", "prompt": "p"}
        path = tmp_path / "jobs.yaml"
        path.write_text(yaml.safe_dump({"jobs": [
            {"id": "plain", **entry},
            {"id": "stated", **entry, "run_if": []},
            {"id": "legacy", **entry, "skip_when_idle": ["mail"]},
            {"id": "none-for-it", **entry},
        ]}), encoding="utf-8")
        asked: list[str] = []

        def default(job_id: str) -> list[dict]:
            asked.append(job_id)
            return [] if job_id == "none-for-it" else [{"type": "tasks", "status": "pending"}]

        jobs = {job.id: job for job in load_jobs(path, default_run_if=default)}

        assert asked == ["plain", "none-for-it"]
        assert [type(gate) for gate in jobs["plain"].gates] == [TasksGate]
        assert jobs["stated"].gates == [] and jobs["none-for-it"].gates == []
        assert [type(gate) for gate in jobs["legacy"].gates] == [MessagesGate]


@pytest.mark.asyncio
class TestBuiltInMaintenanceJobsOnAnAgent:
    """The four maintenance jobs as the scheduler loads them from the
    system.yaml that the installer writes, asked about an agent's records."""

    JOBS = ("memory-maintenance", "skill-extractor", "skill-reviser", "machine-maintenance")

    async def _due(self, db, loaded: dict[str, CronJob]) -> set[str]:
        """The maintenance jobs whose gates let a scheduled run start now."""
        due = set()
        for job_id in self.JOBS:
            ctx = GateContext(job_id=job_id, db=db, jobs=loaded)
            if (await evaluate_gates(loaded[job_id].gates, ctx)).should_run:
                due.add(job_id)
        return due

    @pytest.mark.parametrize("backend", ["memu", "loom"])
    async def test_none_runs_on_an_agent_that_does_nothing(self, db, tmp_path, backend):
        loaded = _load(tmp_path, _system_yaml(backend), backend=backend)
        assert await self._due(db, loaded) == set()  # nothing has ever happened

        for job_id in self.JOBS:
            await _ran(db, job_id, minutes_ago=60 * 24 * 8)

        assert await self._due(db, loaded) == set()  # more than a week on, still nothing

    @WORK
    async def test_a_run_of_one_starts_none_of_the_others(self, db, tmp_path, work):
        """A run is a turn of the agent too, in a session of its job."""
        loaded = _load(tmp_path, _system_yaml())
        for job_id in self.JOBS:
            await _ran(db, job_id, minutes_ago=60 * 24 * 3)
        for job_id in self.JOBS:
            await work(db, _run_session(job_id), minutes_ago=60)

        assert await self._due(db, loaded) == set()

    @WORK
    @pytest.mark.parametrize("ungated", ["skill-reviser", "machine-maintenance"])
    async def test_a_job_put_back_on_every_schedule_starts_none_either(self, db, tmp_path, work, ungated):
        """``run_if: []`` takes the gate off a job. Its runs still are not work
        for the others: each names the other three in ``ignore_jobs``."""
        jobs = _system_yaml()
        for job in jobs:
            if job["id"] == ungated:
                job["run_if"] = []
        loaded = _load(tmp_path, jobs)
        assert loaded[ungated].gates == []
        for job_id in self.JOBS:
            await _ran(db, job_id, minutes_ago=60 * 24 * 3)
        await work(db, _run_session(ungated), minutes_ago=60)

        assert await self._due(db, loaded) == {ungated}

    @WORK
    @pytest.mark.parametrize("backend, once_more", [("memu", {"memory-maintenance"}), ("loom", set())])
    async def test_work_starts_each_of_them_once(self, db, tmp_path, work, backend, once_more):
        """After their runs only memU's memory job is due again: it reviews
        the day before its run. The weekly jobs have no overlap: what
        machine-maintenance found too new to remove waits for the first run
        after the agent works again."""
        loaded = _load(tmp_path, _system_yaml(backend), backend=backend)
        for job_id in self.JOBS:
            await _ran(db, job_id, minutes_ago=60 * 24 * 3)
        await work(db, "chat-1", minutes_ago=120)

        assert await self._due(db, loaded) == set(self.JOBS)

        for job_id in self.JOBS:
            await _ran(db, job_id, minutes_ago=60)

        assert await self._due(db, loaded) == once_more

    async def test_the_weekly_jobs_on_the_real_scheduler(self, agent_db, tmp_path):
        """Loaded by the scheduler from the installer's file and run by it:
        no run while nothing has happened, one run of each for one piece of
        work, and neither run starts the other. A person can still start the
        cleanup of an agent that does nothing."""
        import yaml

        engine, _ = await _engine(agent_db, tmp_path, [OK])
        svc = CronService(engine.config, engine, agent_db)
        system_file = engine.config.cron.system_file
        system_file.parent.mkdir(parents=True, exist_ok=True)
        system_file.write_text(yaml.safe_dump({"jobs": _system_yaml()}), encoding="utf-8")
        svc._jobs = svc._load_merged_jobs()
        weekly = [job for job in svc._jobs if job.id in ("skill-reviser", "machine-maintenance")]
        assert len(weekly) == 2

        async def scheduled_time() -> None:
            for job in weekly:
                await svc._run_job_inner(job)
            await _age(agent_db, 10)

        async def runs() -> dict[str, list[str]]:
            return {
                job.id: [log["status"] for log in await agent_db.get_cron_logs(job.id)]
                for job in weekly
            }

        await scheduled_time()
        assert await runs() == {"skill-reviser": [], "machine-maintenance": []}

        await _session(engine, "chat")
        await _real_turn(engine, "chat")
        await _age(agent_db, 10)
        for _ in range(3):
            await scheduled_time()
        assert await runs() == {"skill-reviser": ["success"], "machine-maintenance": ["success"]}

        await svc.run_job("machine-maintenance")
        assert (await runs())["machine-maintenance"] == ["success", "success"]
