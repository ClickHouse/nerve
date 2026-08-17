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
        assigner="assigner",
    )
    assert actors == [
        "author", "assignee", "assigner", "commenter", "reviewer",
        "inline1", "inline2", "recent",
    ]


def test_collect_actors_includes_assigner_when_it_is_the_only_third_party():
    # The self-authored, self-assigned, comment-free case: a bot files an issue
    # and a maintainer assigns it back. Every login except the assigner is the
    # bot itself, so without the assigner there is no third-party actor at all
    # and a non-empty allow_actors allowlist can never pass the notification.
    actors = _collect_actors(
        subject_user="bot",
        assignees=["bot"],
        comment=None,
        latest_review=None,
        inline_comments=[],
        recent_comments=[],
        assigner="maintainer",
    )
    assert actors == ["bot", "maintainer"]


def test_collect_actors_assigner_defaults_to_absent():
    # Every non-assign reason omits the argument entirely.
    actors = _collect_actors(
        subject_user="author",
        assignees=[],
        comment=None,
        latest_review=None,
        inline_comments=[],
        recent_comments=[],
    )
    assert actors == ["author"]


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


# ---------------------------------------------------------------------------
# _enrich_assignment — resolving who assigned a reason=assign notification
# ---------------------------------------------------------------------------

def _timeline_assigned(actor: str, assignee: str) -> dict:
    """An `assigned` entry in the /timeline shape (acting login in `actor`)."""
    return {
        "event": "assigned",
        "actor": {"login": actor},
        "assignee": {"login": assignee},
    }


@pytest.mark.asyncio
async def test_enrich_assignment_reads_last_assigned_event(monkeypatch):
    src = GitHubSource()
    calls: list[str] = []

    async def fake_get(url, timeout=30):
        calls.append(url)
        return [
            {"event": "labeled", "actor": {"login": "someone"}},
            _timeline_assigned("first-assigner", "bot"),
            {"event": "commented", "actor": {"login": "noise"}},
            _timeline_assigned("maintainer", "bot"),
        ]

    monkeypatch.setattr(src, "_gh_api_get", fake_get)

    result: dict = {"assignees": ["bot"]}
    await src._enrich_assignment(
        "https://api.github.com/repos/owner/repo/issues/521", "Issue", result,
    )

    # The *last* assigned event is the one that triggered the notification.
    assert result["assigner"] == "maintainer"
    assert calls == [
        "https://api.github.com/repos/owner/repo/issues/521/events?per_page=100",
    ]


@pytest.mark.asyncio
async def test_enrich_assignment_prefers_assigner_field_over_actor(monkeypatch):
    # /events disagrees with /timeline: it puts the assignee in
    # `actor` and the real actor in `assigner`. Preferring `assigner` keeps
    # either payload shape correct.
    src = GitHubSource()

    async def fake_get(url, timeout=30):
        return [{
            "event": "assigned",
            "actor": {"login": "bot"},
            "assigner": {"login": "maintainer"},
            "assignee": {"login": "bot"},
        }]

    monkeypatch.setattr(src, "_gh_api_get", fake_get)

    result: dict = {"assignees": ["bot"]}
    await src._enrich_assignment(
        "https://api.github.com/repos/owner/repo/issues/1", "Issue", result,
    )
    assert result["assigner"] == "maintainer"


@pytest.mark.asyncio
async def test_enrich_assignment_ignores_stale_assignment(monkeypatch):
    # The newest `assigned` event targets someone who is no longer assigned
    # (assignment since reverted) — that assigner must not become an actor.
    src = GitHubSource()

    async def fake_get(url, timeout=30):
        return [_timeline_assigned("stranger", "someone-else")]

    monkeypatch.setattr(src, "_gh_api_get", fake_get)

    result: dict = {"assignees": ["bot"]}
    await src._enrich_assignment(
        "https://api.github.com/repos/owner/repo/issues/1", "Issue", result,
    )
    assert "assigner" not in result


@pytest.mark.asyncio
async def test_enrich_assignment_does_not_fall_back_to_an_older_assigner(
    monkeypatch,
):
    # An unresolvable triggering event (deleted account → null actor) must leave
    # `assigner` absent rather than reaching back to an earlier assignment.
    # Crediting the previous assigner would admit the notification on the
    # authority of someone who did not act — and that login is exactly the kind
    # likely to be on the allowlist. Fail closed instead.
    src = GitHubSource()

    async def fake_get(url, timeout=30):
        return [
            _timeline_assigned("earlier-maintainer", "bot"),
            {"event": "assigned", "actor": None, "assignee": {"login": "bot"}},
        ]

    monkeypatch.setattr(src, "_gh_api_get", fake_get)

    result: dict = {"assignees": ["bot"]}
    await src._enrich_assignment(
        "https://api.github.com/repos/owner/repo/issues/1", "Issue", result,
    )
    assert "assigner" not in result


@pytest.mark.asyncio
async def test_enrich_assignment_uses_issues_path_for_prs(monkeypatch):
    # A PR's event history lives under /issues/{n}/, never /pulls/{n}/.
    src = GitHubSource()
    calls: list[str] = []

    async def fake_get(url, timeout=30):
        calls.append(url)
        return [_timeline_assigned("maintainer", "bot")]

    monkeypatch.setattr(src, "_gh_api_get", fake_get)

    result: dict = {"assignees": ["bot"]}
    await src._enrich_assignment(
        "https://api.github.com/repos/owner/repo/pulls/9", "PullRequest", result,
    )
    assert result["assigner"] == "maintainer"
    assert "/issues/9/events" in calls[0]
    assert "/pulls/" not in calls[0]


@pytest.mark.asyncio
async def test_enrich_assignment_tolerates_missing_history(monkeypatch):
    # A failed/empty event-history call must leave `assigner` absent, not raise.
    src = GitHubSource()

    async def fake_get(url, timeout=30):
        return None

    monkeypatch.setattr(src, "_gh_api_get", fake_get)

    result: dict = {"assignees": ["bot"]}
    await src._enrich_assignment(
        "https://api.github.com/repos/owner/repo/issues/1", "Issue", result,
    )
    assert "assigner" not in result


@pytest.mark.asyncio
async def test_enrich_notification_skips_event_fetch_for_non_assign_reasons(monkeypatch):
    # The extra API call is taken only for reason=assign.
    src = GitHubSource()
    calls: list[str] = []

    async def fake_get(url, timeout=30):
        calls.append(url)
        if url.endswith("/issues/1"):
            return {"html_url": "h", "user": {"login": "alice"}, "assignees": []}
        return []

    monkeypatch.setattr(src, "_gh_api_get", fake_get)

    import asyncio as _asyncio

    await src._enrich_notification(
        {
            "reason": "mention",
            "subject": {
                "type": "Issue",
                "url": "https://api.github.com/repos/owner/repo/issues/1",
            },
        },
        _asyncio.Semaphore(1),
    )
    assert not any("/events" in c for c in calls)


# ---------------------------------------------------------------------------
# Regression — a maintainer assigning an issue the bot itself filed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_maintainer_assignment_on_self_filed_issue_passes_guardrail(
    monkeypatch, db,
):
    """The hand-back route: bot files an issue, maintainer assigns it back.

    Author, assignee and (absent) commenters are all the bot, so before the
    assigner was collected this notification's only actor was the bot itself —
    a non-empty ``allow_actors`` dropped it fail-closed and the assignment was
    silently lost. Exercises the real fetch + enrichment + production guardrail.
    """
    subject_url = "https://api.github.com/repos/owner/repo/issues/521"
    notifications = [{
        "id": "n-assign",
        "reason": "assign",
        "unread": True,
        "updated_at": "2026-01-02T10:00:00Z",
        "subject": {"title": "Bug", "type": "Issue", "url": subject_url},
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

    async def fake_get(url, timeout=30):
        if url == subject_url:
            return {
                "html_url": "https://github.com/owner/repo/issues/521",
                "body": "the bug",
                "state": "open",
                "user": {"login": "bot"},          # the bot filed it
                "assignees": [{"login": "bot"}],   # ...and is the assignee
                "labels": [{"name": "bug"}],
            }
        if "/events" in url:
            return [_timeline_assigned("maintainer", "bot")]
        return []                                   # no comments at all

    src = GitHubSource()
    monkeypatch.setattr(src, "_gh_api_get", fake_get)

    result = await src.fetch(cursor="2026-01-02T09:00:00Z")
    assert len(result.records) == 1
    record = result.records[0]

    assert record.metadata["actors"] == ["bot", "maintainer"]
    # The agent can also see who asked, which an assignment otherwise never says.
    assert "Assigned by: maintainer" in record.content

    # ...and the production guardrail now keeps it.
    cfg = NerveConfig.from_dict({
        "sync": {"github": {"enabled": True, "allow_actors": ["maintainer"]}},
    })
    gh = next(
        r for r in build_source_runners(cfg, db)
        if r.source.source_name == "github"
    )
    assert gh.inbox_filter.passes(record) is True


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
