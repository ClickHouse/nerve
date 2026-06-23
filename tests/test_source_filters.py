"""Inbox guardrails — declarative allow/deny filtering of source records.

Covers the pure matching logic of :class:`FieldRule` / :class:`InboxFilter`
plus an end-to-end runner test proving dropped records never reach the inbox
while the source cursor still advances.
"""

from __future__ import annotations

import pytest

from nerve.sources.base import Source
from nerve.sources.filters import FieldRule, InboxFilter
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
