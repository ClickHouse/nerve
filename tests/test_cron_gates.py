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
        fields = view[view.index("--json") + 1]
        for field in ("state", "reviewDecision", "headRefOid",
                      "statusCheckRollup", "comments", "reviews"):
            assert field in fields

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

    def test_from_config_bad_ignore_actors(self):
        with pytest.raises(GateConfigError):
            build_gate({
                "type": "github_pr_activity",
                "author": "my-bot",
                "ignore_actors": [{"login": "codecov"}],
            })

    def test_describe_mentions_comments(self):
        assert "comment" in GitHubPrActivityGate(author="my-bot").describe()
