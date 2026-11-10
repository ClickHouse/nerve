"""Inbox guardrails — declarative allow/deny filtering of source records.

Covers the pure matching logic of :class:`FieldRule` / :class:`InboxFilter`
plus an end-to-end runner test proving dropped records never reach the inbox
while the source cursor still advances, and :class:`DataRule`, the rules of a
generic hosted source, against the channel contract's rule vectors.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nerve.config import GitHubSyncConfig
from nerve.sources.base import Source
from nerve.sources.filters import DataRule, FieldRule, InboxFilter, data_rules
from nerve.sources.github import github_inbox_filter
from nerve.sources.models import FetchResult, SourceRecord
from nerve.sources.runner import SourceRunner


def _rec(repo: str = "owner/repo", **meta) -> SourceRecord:
    """Build a GitHub-ish SourceRecord with repo_name metadata."""
    metadata = {"repo_name": repo}
    metadata.update(meta)
    return SourceRecord(
        id=f"id-{repo}-{len(meta)}",
        source="github",
        record_type="github_notification",
        summary=f"[{repo}] something",
        content="body",
        timestamp="2026-01-01T00:00:00Z",
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# FieldRule semantics
# ---------------------------------------------------------------------------

def test_empty_rule_is_inactive_and_passes_everything():
    rule = FieldRule(field="repo_name")
    assert rule.active is False
    assert rule.passes(_rec("any/thing")) is True


def test_allowlist_keeps_only_matching():
    rule = FieldRule(field="repo_name", allow=["owner/repo"])
    assert rule.passes(_rec("owner/repo")) is True
    assert rule.passes(_rec("other/repo")) is False


def test_denylist_drops_matching():
    rule = FieldRule(field="repo_name", deny=["owner/secret"])
    assert rule.passes(_rec("owner/secret")) is False
    assert rule.passes(_rec("owner/public")) is True


def test_deny_takes_precedence_over_allow():
    rule = FieldRule(field="repo_name", allow=["owner/*"], deny=["owner/secret"])
    assert rule.passes(_rec("owner/public")) is True
    assert rule.passes(_rec("owner/secret")) is False


def test_glob_matching_is_case_insensitive():
    rule = FieldRule(field="repo_name", allow=["clickhouse/*"])
    assert rule.passes(_rec("ClickHouse/nerve")) is True
    assert rule.passes(_rec("ClickHouse/ClickHouse")) is True
    assert rule.passes(_rec("other-org/other-repo")) is False


def test_allowlist_with_absent_field_fails_closed():
    # Record has no "repo_name" → cannot satisfy a non-empty allowlist.
    rec = SourceRecord(
        id="x", source="github", record_type="t",
        summary="s", content="c", timestamp="t", metadata={},
    )
    assert FieldRule(field="repo_name", allow=["owner/repo"]).passes(rec) is False
    # deny-only with an absent field keeps the record (nothing to deny).
    assert FieldRule(field="repo_name", deny=["owner/repo"]).passes(rec) is True


def test_list_valued_metadata_matches_any_element():
    rule = FieldRule(field="labels", allow=["important"])
    assert rule.passes(_rec(labels=["important", "inbox"])) is True
    assert rule.passes(_rec(labels=["promotions"])) is False
    # deny matches if ANY element matches
    deny_rule = FieldRule(field="labels", deny=["spam"])
    assert deny_rule.passes(_rec(labels=["inbox", "spam"])) is False


def test_scalar_non_string_is_coerced():
    # Telegram chat_id is an int; matching coerces to str.
    rule = FieldRule(field="chat_id", deny=["12345"])
    assert rule.passes(_rec(chat_id=12345)) is False
    assert rule.passes(_rec(chat_id=999)) is True


def test_special_source_and_record_type_fields():
    src_rule = FieldRule(field="source", allow=["github"])
    assert src_rule.passes(_rec()) is True
    type_rule = FieldRule(field="record_type", deny=["github_notification"])
    assert type_rule.passes(_rec()) is False


# ---------------------------------------------------------------------------
# InboxFilter aggregation
# ---------------------------------------------------------------------------

# The glob dialect that the hosted GitHub gateway matches with too: Python
# fnmatch on lower-cased values, where "*" also crosses "/", "?" is one
# character, [...] is a class, and there is neither "**" nor escaping. Each
# pattern admits its value through an allow list, and drops it through a deny
# list, exactly when it matches.
@pytest.mark.parametrize(("pattern", "value", "matched"), [
    ("*nerve*", "OWNER/Nerve-Tools", True),
    ("owner*", "owner/repo", True),
    ("owner/*", "owner/sub/repo", True),
    ("*/repo", "a/b/repo", True),
    ("owner/rep?", "owner/repo", True),
    ("owner/rep?", "owner/repository", False),
    ("[ab]*/x", "B/x", True),
    ("[!a]*/x", "a/x", False),
    ("exact/name", "exact/name2", False),
])
def test_the_glob_vectors_shared_with_the_gateway(pattern, value, matched):
    assert FieldRule(field="repo_name", allow=[pattern]).passes(_rec(value)) is matched
    assert FieldRule(field="repo_name", deny=[pattern]).passes(_rec(value)) is not matched


def test_the_list_vector_shared_with_the_gateway():
    record = _rec(actors=["alice", "Codecov"])
    assert FieldRule(field="actors", allow=["codecov"]).passes(record) is True
    assert FieldRule(field="actors", deny=["codecov"]).passes(record) is False


def test_inbox_filter_inactive_partition_is_passthrough():
    flt = InboxFilter()
    records = [_rec("a/b"), _rec("c/d")]
    kept, dropped = flt.partition(records)
    assert kept == records
    assert dropped == []
    assert flt.active is False


def test_inbox_filter_partition_splits_and_preserves_order():
    flt = InboxFilter.from_field("repo_name", allow=["keep/*"], deny=[])
    records = [_rec("keep/one"), _rec("drop/one"), _rec("keep/two")]
    kept, dropped = flt.partition(records)
    assert [r.metadata["repo_name"] for r in kept] == ["keep/one", "keep/two"]
    assert [r.metadata["repo_name"] for r in dropped] == ["drop/one"]


def test_inbox_filter_requires_all_rules_to_pass():
    flt = InboxFilter(rules=[
        FieldRule(field="repo_name", allow=["owner/*"]),
        FieldRule(field="labels", deny=["muted"]),
    ])
    assert flt.passes(_rec("owner/repo", labels=["inbox"])) is True
    assert flt.passes(_rec("owner/repo", labels=["muted"])) is False
    assert flt.passes(_rec("other/repo", labels=["inbox"])) is False


# ---------------------------------------------------------------------------
# Runner integration — dropped records never hit the inbox
# ---------------------------------------------------------------------------

class _FakeSource(Source):
    """A source that returns a fixed batch of records once."""

    source_name = "github"

    def __init__(self, records: list[SourceRecord], next_cursor: str = "c1"):
        self._records = records
        self._next_cursor = next_cursor

    async def fetch(self, cursor, limit: int = 100) -> FetchResult:
        return FetchResult(records=list(self._records), next_cursor=self._next_cursor)


@pytest.mark.asyncio
async def test_runner_drops_filtered_records_before_persist(db):
    records = [_rec("ClickHouse/nerve"), _rec("evil/repo"), _rec("ClickHouse/ClickHouse")]
    flt = InboxFilter.from_field("repo_name", allow=["clickhouse/*"], deny=[])
    runner = SourceRunner(source=_FakeSource(records), db=db, inbox_filter=flt)

    result = await runner.run()

    assert result.records_ingested == 2
    assert result.records_dropped == 1
    assert result.error is None

    # Inbox contains only the two allowed repos.
    rows, _ = await db.list_source_messages(source="github", limit=100)
    repos = {r["summary"] for r in rows}
    assert repos == {"[ClickHouse/nerve] something", "[ClickHouse/ClickHouse] something"}

    # Cursor advanced even though one record was dropped.
    assert await db.get_sync_cursor("github") == "c1"


@pytest.mark.asyncio
async def test_runner_without_filter_ingests_all(db):
    records = [_rec("a/b"), _rec("c/d")]
    runner = SourceRunner(source=_FakeSource(records), db=db)

    result = await runner.run()

    assert result.records_ingested == 2
    assert result.records_dropped == 0
    rows, _ = await db.list_source_messages(source="github", limit=100)
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_runner_all_dropped_still_advances_cursor(db):
    records = [_rec("evil/one"), _rec("evil/two")]
    flt = InboxFilter.from_field("repo_name", allow=["trusted/*"], deny=[])
    runner = SourceRunner(source=_FakeSource(records, next_cursor="c2"), db=db, inbox_filter=flt)

    result = await runner.run()

    assert result.records_ingested == 0
    assert result.records_dropped == 2
    rows, _ = await db.list_source_messages(source="github", limit=100)
    assert rows == []
    assert await db.get_sync_cursor("github") == "c2"


# ---------------------------------------------------------------------------
# Actor guardrail — the "actors" metadata key (list of involved GitHub logins)
# ---------------------------------------------------------------------------

def test_actor_allowlist_keeps_record_if_any_involved_login_matches():
    rule = FieldRule(field="actors", allow=["alice", "bob"])
    # list-valued: kept if ANY involved login is on the allowlist
    assert rule.passes(_rec(actors=["bob", "stranger"])) is True
    assert rule.passes(_rec(actors=["alice"])) is True
    assert rule.passes(_rec(actors=["stranger", "drive-by"])) is False


def test_actor_denylist_drops_if_any_login_matches():
    rule = FieldRule(field="actors", deny=["spammer"])
    assert rule.passes(_rec(actors=["trusted", "spammer"])) is False
    assert rule.passes(_rec(actors=["trusted"])) is True


def test_actor_allowlist_absent_or_empty_actors_fails_closed():
    rule = FieldRule(field="actors", allow=["alice"])
    assert rule.passes(_rec()) is False             # no "actors" key at all
    assert rule.passes(_rec(actors=[])) is False    # present but empty


def test_repo_and_actor_guardrails_and_together():
    flt = InboxFilter(rules=[
        FieldRule(field="repo_name", allow=["ClickHouse/*"]),
        FieldRule(field="actors", allow=["alice", "bob"]),
    ])
    assert flt.passes(_rec("ClickHouse/nerve", actors=["bob"])) is True
    # right repo, untrusted actor → dropped
    assert flt.passes(_rec("ClickHouse/nerve", actors=["stranger"])) is False
    # trusted actor, wrong repo → dropped
    assert flt.passes(_rec("other/repo", actors=["bob"])) is False


def test_actor_deny_wins_even_when_an_allowed_actor_is_present():
    # Security-critical: a denied login co-occurring with an allowed one is
    # still dropped (deny takes precedence over allow).
    rule = FieldRule(field="actors", allow=["alice", "bob"], deny=["spammer"])
    assert rule.passes(_rec(actors=["alice"])) is True
    assert rule.passes(_rec(actors=["alice", "spammer"])) is False


def test_actor_allowlist_is_case_insensitive():
    rule = FieldRule(field="actors", allow=["Alice"])
    assert rule.passes(_rec(actors=["alice"])) is True
    assert rule.passes(_rec(actors=["ALICE"])) is True
    assert rule.passes(_rec(actors=["bob"])) is False


def test_deny_only_rule_ignores_absent_and_empty_fields():
    # The GitHub CI guardrail relies on this: ci_branch is "" (or absent) for
    # every non-CheckSuite notification, and a deny list must not touch those.
    rule = FieldRule(field="ci_branch", deny=["main", "master"])
    assert rule.passes(_rec(ci_branch="main")) is False
    assert rule.passes(_rec(ci_branch="fix/thing")) is True
    assert rule.passes(_rec(ci_branch="")) is True
    assert rule.passes(_rec()) is True          # key absent entirely


def test_rejects_names_the_first_failing_rule():
    flt = InboxFilter(rules=[
        FieldRule(field="repo_name", allow=["ClickHouse/*"]),
        FieldRule(field="ci_branch", deny=["main"]),
    ])
    kept = _rec("ClickHouse/nerve", ci_branch="fix/thing")
    assert flt.rejects(kept) is None

    by_branch = flt.rejects(_rec("ClickHouse/nerve", ci_branch="main"))
    assert by_branch is not None and by_branch.field == "ci_branch"

    # Rules are evaluated in order, so the repo rule is reported first.
    by_repo = flt.rejects(_rec("other/repo", ci_branch="main"))
    assert by_repo is not None and by_repo.field == "repo_name"


def test_a_rule_does_not_apply_to_the_record_types_it_exempts():
    rule = FieldRule(field="actors", allow=["alice"], exempt_record_types=("github_pr_activity",))
    exempt = SourceRecord(
        id="pr:owner/repo#12", source="github", record_type="github_pr_activity",
        summary="s", content="c", timestamp="t", metadata={"actors": ["stranger"]},
    )
    assert rule.passes(exempt) is True
    assert rule.passes(_rec(actors=["stranger"])) is False
    assert rule.passes(_rec(actors=["alice"])) is True


# ---------------------------------------------------------------------------
# GitHub: a record about the agent's own pull request meets the repo rule only
# ---------------------------------------------------------------------------

def _github(record_type: str, *, repo: str = "owner/repo") -> SourceRecord:
    """A GitHub record as hosted mode delivers it: the agent is the author,
    and the actors are a reviewer and a commenter."""
    return SourceRecord(
        id=f"{record_type}:{repo}", source="github", record_type=record_type,
        summary=f"[{repo}] Fix the flaky retry test (author)", content="body",
        timestamp="2026-01-01T00:00:00Z",
        metadata={"repo_name": repo, "reason": "author", "actors": ["alice", "bob"], "ci_branch": ""},
    )


def test_an_actor_allow_list_keeps_a_pull_request_record_but_not_a_notification():
    """The vector both ends of hosted GitHub check, the gateway before it
    stores a record and Nerve before the inbox."""
    flt = github_inbox_filter(GitHubSyncConfig(allow_actors=["someone-else"]))

    assert flt.passes(_github("github_pr_activity")) is True
    assert flt.passes(_github("github_notification")) is False


@pytest.mark.parametrize("settings", [
    {"allow_actors": ["someone-else"]},
    {"deny_actors": ["bob"]},
    {"allow_reasons": ["mention", "review_requested"]},
    {"deny_reasons": ["author"]},
    {"deny_ci_branches": ["*"]},
])
def test_only_the_repo_rule_applies_to_a_pull_request_record(settings):
    flt = github_inbox_filter(GitHubSyncConfig(**settings))

    assert flt.passes(_github("github_pr_activity")) is True
    assert flt.passes(_github("github_notification")) is False


@pytest.mark.parametrize("settings", [{"allow_repos": ["other/*"]}, {"deny_repos": ["owner/*"]}])
def test_the_repo_rule_still_applies_to_a_pull_request_record(settings):
    flt = github_inbox_filter(GitHubSyncConfig(**settings))

    rejected_by = flt.rejects(_github("github_pr_activity"))

    assert rejected_by is not None and rejected_by.field == "repo_name"


# ---------------------------------------------------------------------------
# DataRule: the rules of a generic hosted source, on JSON Pointers into the
# record's data (its metadata), with the semantics of the channel contract
# ---------------------------------------------------------------------------

# A byte-exact copy of the contract's rule vectors (testdata/source_rules.json);
# test_channel_contract_generic pins its bytes.
RULE_VECTORS = json.loads(
    (Path(__file__).parent / "fixtures" / "channel_contract" / "source_rules.json").read_text(encoding="utf-8")
)


def _data_record(data: dict) -> SourceRecord:
    return SourceRecord(
        id="note:1", source="example", record_type="example_note", summary="[NOTE-7] Plan",
        content="", timestamp="2026-11-10T08:00:00Z", metadata=data,
    )


@pytest.mark.parametrize("case", RULE_VECTORS["cases"], ids=lambda case: case["name"])
def test_the_rule_vectors_shared_with_the_gateway(case):
    record = _data_record(RULE_VECTORS["data"])

    assert InboxFilter(rules=data_rules(case["rules"])).passes(record) is case["keep"]


def test_the_values_at_a_path():
    record = _data_record(RULE_VECTORS["data"])

    assert DataRule("/issue/labels", deny=("x",)).values(record) == ["bug", "Frontend", "7", "true", "null"]
    assert DataRule("/issue/size", deny=("x",)).values(record) == ["2"]
    assert DataRule("/issue/team", deny=("x",)).values(record) == []
    assert DataRule("/issue/missing", deny=("x",)).values(record) == []
    # An index names an element only in decimal without a leading zero.
    assert DataRule("/issue/labels/1", deny=("x",)).values(record) == ["Frontend"]
    assert DataRule("/issue/labels/01", deny=("x",)).values(record) == []
    assert DataRule("/issue/labels/-", deny=("x",)).values(record) == []
    assert DataRule("not a pointer", deny=("x",)).values(record) == []


def test_a_data_rule_without_patterns_is_inactive_and_an_invalid_path_names_nothing():
    record = _data_record(RULE_VECTORS["data"])

    assert DataRule("/actor/kind").active is False
    assert InboxFilter(rules=[DataRule("/actor/kind")]).active is False
    assert DataRule("actor/kind", deny=("user",)).passes(record) is True
    assert DataRule("actor/kind", allow=("user",)).passes(record) is False


def test_rejects_names_the_path_and_the_value_of_the_data_rule():
    record = _data_record(RULE_VECTORS["data"])
    flt = InboxFilter(rules=data_rules([
        {"path": "/issue/team/key", "allow": ["ENG*"]},
        {"path": "/actor/kind", "deny": ["user"]},
    ]))

    rule = flt.rejects(record)

    assert (rule.field, rule.value(record)) == ("/actor/kind", "user")


class _DataSource(_FakeSource):
    source_name = "example"


@pytest.mark.asyncio
async def test_runner_drops_on_a_data_rule_and_logs_its_path_and_value(db, caplog):
    kept = _data_record({"actor": {"kind": "user"}})
    bot = SourceRecord(**{**kept.__dict__, "id": "note:2", "metadata": {"actor": {"kind": "bot"}}})
    flt = InboxFilter(rules=data_rules([{"path": "/actor/kind", "deny": ["bot"]}]))
    runner = SourceRunner(source=_DataSource([kept, bot]), db=db, inbox_filter=flt)

    with caplog.at_level("INFO", logger="nerve.sources.runner"):
        result = await runner.run()

    assert (result.records_ingested, result.records_dropped) == (1, 1)
    assert await db.get_source_message("example", "note:2") is None
    assert "dropped note:2 on /actor/kind='bot'" in caplog.text
