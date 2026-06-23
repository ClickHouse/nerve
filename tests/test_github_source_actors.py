"""Actor guardrail for the GitHub notifications source.

Covers the ``actors`` metadata key that ``GitHubSource`` surfaces (every login
involved in a notification), the ``allow_actors`` / ``deny_actors`` config, and
the registry wiring that turns them into an inbox guardrail — so an untrusted
GitHub user cannot drive the worker merely by @-mentioning it.
"""

from __future__ import annotations

import json

import pytest

from nerve.config import NerveConfig
from nerve.sources.github import GitHubSource, _collect_actors
from nerve.sources.models import SourceRecord
from nerve.sources.registry import build_source_runners


# ---------------------------------------------------------------------------
# _collect_actors — pure de-dup / ordering / placeholder handling
# ---------------------------------------------------------------------------

def test_collect_actors_orders_and_dedups_case_insensitively():
    actors = _collect_actors(
        subject_user="Alice",
        assignees=["bob", "alice"],          # "alice" is a case-dup of "Alice"
        comment={"user": "carol"},
        latest_review=None,
        inline_comments=[],
        recent_comments=[],
    )
    assert actors == ["Alice", "bob", "carol"]


def test_collect_actors_skips_empty_and_placeholder_logins():
    actors = _collect_actors(
        subject_user="",
        assignees=[],
        comment={"user": "?"},               # "?" is the enrichment placeholder
        latest_review={"user": ""},
        inline_comments=[{"user": "dave"}],
        recent_comments=[{"user": "?"}, {"user": "erin"}],
    )
    assert actors == ["dave", "erin"]


def test_collect_actors_spans_all_enrichment_sources():
    actors = _collect_actors(
        subject_user="author",
        assignees=["assignee"],
        comment={"user": "commenter"},
        latest_review={"user": "reviewer"},
        inline_comments=[{"user": "inline1"}, {"user": "inline2"}],
        recent_comments=[{"user": "recent"}],
    )
    assert actors == [
        "author", "assignee", "commenter", "reviewer",
        "inline1", "inline2", "recent",
    ]


# ---------------------------------------------------------------------------
# Config — allow_actors / deny_actors parsing
# ---------------------------------------------------------------------------

def test_github_sync_config_reads_actor_lists():
    cfg = NerveConfig.from_dict({
        "sync": {"github": {
            "allow_actors": ["alice", "bob"],
            "deny_actors": ["spammer"],
        }},
    })
    gh = cfg.sync.github
    assert gh.allow_actors == ["alice", "bob"]
    assert gh.deny_actors == ["spammer"]


def test_github_sync_config_actor_lists_default_empty():
    gh = NerveConfig.from_dict({}).sync.github
    assert gh.allow_actors == []
    assert gh.deny_actors == []


# ---------------------------------------------------------------------------
# Source — fetch() surfaces the "actors" key in record metadata
# ---------------------------------------------------------------------------

class _FakeProc:
    """Minimal stand-in for an asyncio subprocess returning canned stdout."""

    def __init__(self, stdout: bytes):
        self._stdout = stdout
        self.returncode = 0

    async def communicate(self):
        return self._stdout, b""


@pytest.mark.asyncio
async def test_fetch_populates_actors_metadata(monkeypatch):
    notifications = [{
        "id": "n1",
        "reason": "mention",
        "unread": True,
        "updated_at": "2026-01-02T10:00:00Z",
        "subject": {
            "title": "Bug",
            "type": "Issue",
            "url": "https://api.github.com/repos/owner/repo/issues/1",
        },
        "repository": {
            "full_name": "owner/repo",
            "html_url": "https://github.com/owner/repo",
        },
    }]

    async def fake_exec(*args, **kwargs):
        return _FakeProc(json.dumps(notifications).encode())

    monkeypatch.setattr(
        "nerve.sources.github.asyncio.create_subprocess_exec", fake_exec,
    )

    src = GitHubSource()

    async def fake_enrich(notif, sem):
        return {
            "html_url": "https://github.com/owner/repo/issues/1",
            "body": "desc",
            "state": "open",
            "user": "alice",
            "assignees": ["bob"],
            "labels": ["bug"],
            "latest_comment": {"user": "carol", "body": "ping", "created_at": "x"},
        }

    monkeypatch.setattr(src, "_enrich_notification", fake_enrich)

    result = await src.fetch(cursor="2026-01-02T09:00:00Z")

    assert len(result.records) == 1
    assert result.records[0].metadata["actors"] == ["alice", "bob", "carol"]


# ---------------------------------------------------------------------------
# Registry — config actor lists become an active inbox guardrail
# ---------------------------------------------------------------------------

def _gh_rec(rid: str, actors: list[str], repo: str = "ClickHouse/nerve") -> SourceRecord:
    return SourceRecord(
        id=rid, source="github", record_type="github_notification",
        summary=f"[{repo}] x", content="c", timestamp="2026-01-01T00:00:00Z",
        metadata={"repo_name": repo, "actors": actors},
    )


@pytest.mark.asyncio
async def test_build_source_runners_wires_actor_guardrail(db):
    cfg = NerveConfig.from_dict({
        "sync": {"github": {
            "enabled": True,
            "allow_actors": ["alice", "bob"],
        }},
    })
    runners = build_source_runners(cfg, db)
    gh = next(r for r in runners if r.source.source_name == "github")

    assert gh.inbox_filter is not None
    assert gh.inbox_filter.active is True
    # A trusted actor being involved keeps the record; only strangers (or no
    # identifiable actor) → dropped, fail-closed.
    assert gh.inbox_filter.passes(_gh_rec("a", ["bob", "x"])) is True
    assert gh.inbox_filter.passes(_gh_rec("b", ["stranger"])) is False
    assert gh.inbox_filter.passes(_gh_rec("c", [])) is False


@pytest.mark.asyncio
async def test_build_source_runners_no_actor_config_is_passthrough(db):
    # Without allow/deny actors (and no repo guardrail) the github filter must
    # stay inactive so normal notifications still flow.
    cfg = NerveConfig.from_dict({"sync": {"github": {"enabled": True}}})
    runners = build_source_runners(cfg, db)
    gh = next(r for r in runners if r.source.source_name == "github")
    assert gh.inbox_filter is None or gh.inbox_filter.active is False


@pytest.mark.asyncio
async def test_build_source_runners_actor_deny_wins(db):
    # deny_actors takes precedence even when an allowed actor co-occurs.
    cfg = NerveConfig.from_dict({
        "sync": {"github": {
            "enabled": True,
            "allow_actors": ["alice", "bob"],
            "deny_actors": ["spammer"],
        }},
    })
    runners = build_source_runners(cfg, db)
    gh = next(r for r in runners if r.source.source_name == "github")
    assert gh.inbox_filter.passes(_gh_rec("ok", ["alice"])) is True
    assert gh.inbox_filter.passes(_gh_rec("no", ["alice", "spammer"])) is False


@pytest.mark.asyncio
async def test_fetch_actors_empty_when_enrichment_fails(monkeypatch):
    # When enrichment raises, the loop falls back to extra={} — the record is
    # still produced (with no identifiable actor), proving every login variable
    # is defined on the failure path and `actors` degrades to [].
    notifications = [{
        "id": "n1",
        "reason": "mention",
        "unread": True,
        "updated_at": "2026-01-02T10:00:00Z",
        "subject": {
            "title": "Bug",
            "type": "Issue",
            "url": "https://api.github.com/repos/owner/repo/issues/1",
        },
        "repository": {
            "full_name": "owner/repo",
            "html_url": "https://github.com/owner/repo",
        },
    }]

    async def fake_exec(*args, **kwargs):
        return _FakeProc(json.dumps(notifications).encode())

    monkeypatch.setattr(
        "nerve.sources.github.asyncio.create_subprocess_exec", fake_exec,
    )

    src = GitHubSource()

    async def boom(notif, sem):
        raise RuntimeError("enrichment failed")

    monkeypatch.setattr(src, "_enrich_notification", boom)

    result = await src.fetch(cursor="2026-01-02T09:00:00Z")

    assert len(result.records) == 1
    assert result.records[0].metadata["actors"] == []
