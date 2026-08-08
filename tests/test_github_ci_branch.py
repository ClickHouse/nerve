"""CI-branch guardrail for the GitHub notifications source.

Covers the ``ci_branch`` metadata key that ``GitHubSource`` parses out of
CheckSuite titles, the ``deny_ci_branches`` config, and the registry wiring that
turns it into an inbox guardrail — so workflow runs on the default branch
(upstream syncs, schedules, deploys) never reach the inbox while CI failures on
your own PR branches still do.

GitHub itself cannot make this distinction: one workflow file serves both the
``push``-to-``main`` runs and the ``pull_request`` runs, and the Actions
notification preference is account-wide.
"""

from __future__ import annotations

import json

import pytest

from nerve.config import NerveConfig
from nerve.sources.github import GitHubSource, _ci_branch
from nerve.sources.models import SourceRecord
from nerve.sources.registry import build_source_runners


# ---------------------------------------------------------------------------
# _ci_branch — pure title parsing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("title,expected", [
    # Titles observed in the wild, including a retry and a slashed branch name.
    ("CI workflow run failed for main branch", "main"),
    ("Deploy workflow run failed for main branch", "main"),
    ("Remove old images workflow run failed for main branch", "main"),
    (
        "CI workflow run, Attempt #2 failed for chore/drop-anyio-patch branch",
        "chore/drop-anyio-patch",
    ),
    ("CI workflow run failed for fix/chat-new-route branch", "fix/chat-new-route"),
])
def test_ci_branch_parses_check_suite_titles(title, expected):
    assert _ci_branch("CheckSuite", title) == expected


def test_ci_branch_empty_for_non_check_suite():
    # A PR whose title happens to end like a CI title must not be misread.
    title = "CI workflow run failed for main branch"
    assert _ci_branch("PullRequest", title) == ""
    assert _ci_branch("Issue", title) == ""
    assert _ci_branch("", title) == ""


def test_ci_branch_empty_when_title_is_unparseable():
    assert _ci_branch("CheckSuite", "CI workflow run failed") == ""
    assert _ci_branch("CheckSuite", "") == ""
    assert _ci_branch("CheckSuite", "for branch") == ""


# ---------------------------------------------------------------------------
# Config — deny_ci_branches parsing
# ---------------------------------------------------------------------------

def test_github_sync_config_reads_deny_ci_branches():
    cfg = NerveConfig.from_dict({
        "sync": {"github": {"deny_ci_branches": ["main", "master"]}},
    })
    assert cfg.sync.github.deny_ci_branches == ["main", "master"]


def test_github_sync_config_deny_ci_branches_defaults_empty():
    assert NerveConfig.from_dict({}).sync.github.deny_ci_branches == []


# ---------------------------------------------------------------------------
# Source — fetch() surfaces the "ci_branch" key in record metadata
# ---------------------------------------------------------------------------

class _FakeProc:
    """Minimal stand-in for an asyncio subprocess returning canned stdout."""

    def __init__(self, stdout: bytes):
        self._stdout = stdout
        self.returncode = 0

    async def communicate(self):
        return self._stdout, b""


@pytest.mark.asyncio
async def test_fetch_populates_ci_branch_metadata(monkeypatch):
    notifications = [
        {
            "id": "ci-main",
            "reason": "ci_activity",
            "updated_at": "2026-01-02T10:00:00Z",
            # CheckSuite subjects carry no url — enrichment can never run.
            "subject": {
                "title": "CI workflow run failed for main branch",
                "type": "CheckSuite",
                "url": None,
            },
            "repository": {
                "full_name": "owner/repo",
                "html_url": "https://github.com/owner/repo",
            },
        },
        {
            "id": "pr",
            "reason": "author",
            "updated_at": "2026-01-02T11:00:00Z",
            "subject": {
                "title": "Fix the thing",
                "type": "PullRequest",
                "url": "https://api.github.com/repos/owner/repo/pulls/1",
            },
            "repository": {
                "full_name": "owner/repo",
                "html_url": "https://github.com/owner/repo",
            },
        },
    ]

    async def fake_exec(*args, **kwargs):
        return _FakeProc(json.dumps(notifications).encode())

    monkeypatch.setattr(
        "nerve.sources.github.asyncio.create_subprocess_exec", fake_exec,
    )

    src = GitHubSource()

    async def fake_enrich(notif, sem):
        return {}

    monkeypatch.setattr(src, "_enrich_notification", fake_enrich)

    result = await src.fetch(cursor="2026-01-02T09:00:00Z")

    by_id = {r.id: r for r in result.records}
    assert by_id["ci-main"].metadata["ci_branch"] == "main"
    # Non-CI records still carry the key, empty — a deny glob never matches it.
    assert by_id["pr"].metadata["ci_branch"] == ""


# ---------------------------------------------------------------------------
# Registry — deny_ci_branches becomes an active inbox guardrail
# ---------------------------------------------------------------------------

def _ci_rec(rid: str, branch: str) -> SourceRecord:
    return SourceRecord(
        id=rid, source="github", record_type="github_notification",
        summary="[owner/repo] CI workflow run failed (ci_activity)",
        content="c", timestamp="2026-01-01T00:00:00Z",
        metadata={"repo_name": "owner/repo", "actors": [], "ci_branch": branch},
    )


@pytest.mark.asyncio
async def test_build_source_runners_wires_ci_branch_guardrail(db):
    cfg = NerveConfig.from_dict({
        "sync": {"github": {
            "enabled": True,
            "deny_ci_branches": ["main", "master"],
        }},
    })
    runners = build_source_runners(cfg, db)
    gh = next(r for r in runners if r.source.source_name == "github")

    assert gh.inbox_filter is not None
    assert gh.inbox_filter.active is True
    assert gh.inbox_filter.passes(_ci_rec("main", "main")) is False
    assert gh.inbox_filter.passes(_ci_rec("master", "master")) is False
    assert gh.inbox_filter.passes(_ci_rec("pr", "fix/chat-new-route")) is True


@pytest.mark.asyncio
async def test_ci_branch_guardrail_leaves_non_ci_records_alone(db):
    # The whole design rests on this: ci_branch is "" everywhere except
    # CheckSuite, and a deny list must never match the empty string.
    cfg = NerveConfig.from_dict({
        "sync": {"github": {
            "enabled": True,
            "deny_ci_branches": ["main", "master"],
        }},
    })
    runners = build_source_runners(cfg, db)
    gh = next(r for r in runners if r.source.source_name == "github")

    assert gh.inbox_filter.passes(_ci_rec("empty", "")) is True
    # A record predating the change has no ci_branch key at all.
    legacy = SourceRecord(
        id="legacy", source="github", record_type="github_notification",
        summary="[owner/repo] Review requested (review_requested)",
        content="c", timestamp="2026-01-01T00:00:00Z",
        metadata={"repo_name": "owner/repo", "actors": ["alice"]},
    )
    assert gh.inbox_filter.passes(legacy) is True


@pytest.mark.asyncio
async def test_deny_reasons_and_ci_branch_and_together(db):
    cfg = NerveConfig.from_dict({
        "sync": {"github": {
            "enabled": True,
            "deny_reasons": ["comment", "subscribed", "manual", "state_change"],
            "deny_ci_branches": ["main", "master"],
        }},
    })
    runners = build_source_runners(cfg, db)
    gh = next(r for r in runners if r.source.source_name == "github")

    def rec(rid: str, reason: str, branch: str = "") -> SourceRecord:
        return SourceRecord(
            id=rid, source="github", record_type="github_notification",
            summary="[owner/repo] x", content="c",
            timestamp="2026-01-01T00:00:00Z",
            metadata={
                "repo_name": "owner/repo", "actors": [],
                "reason": reason, "ci_branch": branch,
            },
        )

    # Mine, or addressed at me → kept.
    assert gh.inbox_filter.passes(rec("a", "author")) is True
    assert gh.inbox_filter.passes(rec("b", "mention")) is True
    assert gh.inbox_filter.passes(rec("c", "review_requested")) is True
    assert gh.inbox_filter.passes(rec("d", "ci_activity", "fix/thing")) is True
    # Someone else's thread, or a run that isn't my PR → dropped.
    assert gh.inbox_filter.passes(rec("e", "comment")) is False
    assert gh.inbox_filter.passes(rec("f", "manual")) is False
    assert gh.inbox_filter.passes(rec("g", "ci_activity", "main")) is False
