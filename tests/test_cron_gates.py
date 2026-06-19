"""Tests for cron run gates (nerve/cron/gates.py)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from nerve.cron.gates import (
    GATE_REGISTRY,
    CronGate,
    GateConfigError,
    GateContext,
    GitHubPrActivityGate,
    MessagesGate,
    TasksGate,
    build_gate,
    build_gates,
    evaluate_gates,
)
from nerve.cron.jobs import CronJob


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

    def test_empty_sources_raises(self):
        with pytest.raises(GateConfigError):
            MessagesGate(sources=[])

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

    def test_build_gates_skips_invalid(self):
        """An invalid spec is dropped; valid ones still build."""
        gates = build_gates([
            {"type": "tasks", "status": "pending"},
            {"type": "bogus"},
            {"type": "messages"},  # no sources → invalid
        ])
        assert len(gates) == 1
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
