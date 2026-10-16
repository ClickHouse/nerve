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


# ---------------------------------------------------------------------------
# _enrich_review_request — who requested the review on reason=review_requested
# ---------------------------------------------------------------------------

def test_collect_actors_includes_review_requester():
    # A bot's PR with only bot comments: the requester is the one other login.
    actors = _collect_actors(
        subject_user="some-ci[bot]",
        assignees=[],
        comment={"user": "other-ci[bot]"},
        latest_review=None,
        inline_comments=[],
        recent_comments=[],
        review_requester="maintainer",
    )
    assert actors == ["some-ci[bot]", "maintainer", "other-ci[bot]"]


def _review_requested(requester: str | None, reviewer: str) -> dict:
    """A `review_requested` entry in the /events shape."""
    return {
        "event": "review_requested",
        "actor": {"login": requester} if requester else None,
        "review_requester": {"login": requester} if requester else None,
        "requested_reviewer": {"login": reviewer},
    }


def _fake_review_api(src, monkeypatch, events, login="bot"):
    """Stub the two API helpers; return the list of URLs requested."""
    calls: list[str] = []

    async def fake_get(url, timeout=30):
        calls.append(url)
        if url == "user":
            return {"login": login} if login else None
        return None

    async def fake_get_all(url, timeout=60):
        calls.append(url)
        return events

    monkeypatch.setattr(src, "_gh_api_get", fake_get)
    monkeypatch.setattr(src, "_gh_api_get_all", fake_get_all)
    return calls


@pytest.mark.asyncio
async def test_enrich_review_request_reads_last_request_naming_the_account(monkeypatch):
    src = GitHubSource()
    calls = _fake_review_api(src, monkeypatch, [
        _review_requested("some-ci[bot]", "alice"),     # opening the PR
        _review_requested("earlier-maintainer", "bot"),
        {"event": "labeled", "actor": {"login": "someone"}},
        _review_requested("maintainer", "Bot"),          # the trigger (any case)
        _review_requested("other", "carol"),             # later, someone else
    ])

    result: dict = {}
    await src._enrich_review_request(
        "https://api.github.com/repos/owner/repo/pulls/7", result,
    )

    assert result["review_requester"] == "maintainer"
    # A PR's event history lives under /issues/{n}/, never /pulls/{n}/.
    assert calls == [
        "user",
        "https://api.github.com/repos/owner/repo/issues/7/events?per_page=100",
    ]


@pytest.mark.asyncio
async def test_enrich_review_request_falls_back_to_actor(monkeypatch):
    src = GitHubSource()
    _fake_review_api(src, monkeypatch, [{
        "event": "review_requested",
        "actor": {"login": "maintainer"},
        "requested_reviewer": {"login": "bot"},
    }])

    result: dict = {}
    await src._enrich_review_request(
        "https://api.github.com/repos/owner/repo/pulls/7", result,
    )
    assert result["review_requester"] == "maintainer"


@pytest.mark.asyncio
async def test_enrich_review_request_ignores_requests_to_other_reviewers(monkeypatch):
    # The PR's own review requests (made by a bot to other people) are not a
    # request to this account; their requester must not become an actor.
    src = GitHubSource()
    _fake_review_api(src, monkeypatch, [
        _review_requested("maintainer", "alice"),
        _review_requested("maintainer", "some-team-bot"),
    ])

    result: dict = {}
    await src._enrich_review_request(
        "https://api.github.com/repos/owner/repo/pulls/7", result,
    )
    assert "review_requester" not in result


@pytest.mark.asyncio
async def test_enrich_review_request_does_not_fall_back_to_an_older_requester(
    monkeypatch,
):
    # The newest request naming the account has no identifiable requester
    # (deleted account). Reaching back to an older request would credit a login
    # that did not make this one. Fail closed.
    src = GitHubSource()
    _fake_review_api(src, monkeypatch, [
        _review_requested("earlier-maintainer", "bot"),
        _review_requested(None, "bot"),
    ])

    result: dict = {}
    await src._enrich_review_request(
        "https://api.github.com/repos/owner/repo/pulls/7", result,
    )
    assert "review_requester" not in result


@pytest.mark.asyncio
async def test_enrich_review_request_without_viewer_login_resolves_nothing(
    monkeypatch,
):
    # Without the account's own login there is no way to tell which request
    # is the trigger: skip the event fetch and leave the requester absent.
    src = GitHubSource()
    calls = _fake_review_api(
        src, monkeypatch, [_review_requested("maintainer", "bot")], login=None,
    )

    result: dict = {}
    await src._enrich_review_request(
        "https://api.github.com/repos/owner/repo/pulls/7", result,
    )
    assert "review_requester" not in result
    assert calls == ["user"]


@pytest.mark.asyncio
async def test_enrich_review_request_tolerates_missing_history(monkeypatch):
    src = GitHubSource()
    _fake_review_api(src, monkeypatch, None)

    result: dict = {}
    await src._enrich_review_request(
        "https://api.github.com/repos/owner/repo/pulls/7", result,
    )
    assert "review_requester" not in result


def _team_review_requested(requester: str | None, team: str) -> dict:
    """A `review_requested` entry for a team (no `requested_reviewer`)."""
    return {
        "event": "review_requested",
        "actor": {"login": requester} if requester else None,
        "review_requester": {"login": requester} if requester else None,
        "requested_team": {"slug": team},
    }


async def _requester_for(src, monkeypatch, events) -> str | None:
    _fake_review_api(src, monkeypatch, events)
    result: dict = {}
    await src._enrich_review_request(
        "https://api.github.com/repos/owner/repo/pulls/7", result,
    )
    return result.get("review_requester")


@pytest.mark.asyncio
async def test_enrich_review_request_withdrawn_request_credits_nobody(monkeypatch):
    removed = {**_review_requested("maintainer", "bot"), "event": "review_request_removed"}
    assert await _requester_for(GitHubSource(), monkeypatch, [
        _review_requested("maintainer", "bot"),
        removed,
    ]) is None


@pytest.mark.asyncio
async def test_enrich_review_request_re_request_after_removal_is_credited(monkeypatch):
    removed = {**_review_requested("maintainer", "bot"), "event": "review_request_removed"}
    assert await _requester_for(GitHubSource(), monkeypatch, [
        _review_requested("maintainer", "bot"),
        removed,
        _review_requested("other-maintainer", "bot"),
    ]) == "other-maintainer"


@pytest.mark.asyncio
async def test_enrich_review_request_self_request_credits_nobody(monkeypatch):
    assert await _requester_for(GitHubSource(), monkeypatch, [
        _review_requested("bot", "bot"),
    ]) is None


@pytest.mark.asyncio
async def test_enrich_review_request_newer_team_request_by_another_login_credits_nobody(
    monkeypatch,
):
    # The account may be in the team, so the team request may be the trigger,
    # and "maintainer" did not make it. An unidentified team requester blocks too.
    for team_requester in ("someone-else", None):
        assert await _requester_for(GitHubSource(), monkeypatch, [
            _review_requested("maintainer", "bot"),
            _team_review_requested(team_requester, "reviewers"),
        ]) is None


@pytest.mark.asyncio
async def test_enrich_review_request_team_request_by_the_same_login_does_not_block(
    monkeypatch,
):
    # One action that asks the account and a team writes two events, in either
    # order. Whichever is the trigger, "maintainer" made it.
    assert await _requester_for(GitHubSource(), monkeypatch, [
        _review_requested("maintainer", "bot"),
        _team_review_requested("Maintainer", "reviewers"),
    ]) == "maintainer"
    # An older team request by someone else is not newer than the trigger.
    assert await _requester_for(GitHubSource(), monkeypatch, [
        _team_review_requested("someone-else", "reviewers"),
        _review_requested("maintainer", "bot"),
    ]) == "maintainer"


@pytest.mark.asyncio
async def test_enrich_review_request_tolerates_malformed_events(monkeypatch):
    assert await _requester_for(GitHubSource(), monkeypatch, [
        _review_requested("maintainer", "bot"),
        "not-an-event",
        None,
        {"event": "labeled", "actor": {"login": None}},
    ]) == "maintainer"


@pytest.mark.asyncio
async def test_enrich_review_request_newer_unidentifiable_target_credits_nobody(
    monkeypatch,
):
    # A newer request whose target cannot be identified may include the
    # account, like a team request. Fail closed rather than credit "maintainer".
    for newer in (
        {"event": "review_requested", "requested_reviewer": {"login": None},
         "actor": {"login": "someone-else"}},
        {"event": "review_requested", "requested_reviewer": None, "actor": None},
    ):
        assert await _requester_for(GitHubSource(), monkeypatch, [
            _review_requested("maintainer", "bot"),
            newer,
        ]) is None


@pytest.mark.asyncio
async def test_enrich_review_request_credits_a_re_request_by_the_pr_author(
    monkeypatch,
):
    # A PR author can re-request a review from a past reviewer without write
    # access. That newer request is the trigger, so its author is credited —
    # never the maintainer who asked earlier. The allowlist then decides.
    assert await _requester_for(GitHubSource(), monkeypatch, [
        _review_requested("maintainer", "bot"),
        _review_requested("pr-author", "bot"),
    ]) == "pr-author"


@pytest.mark.asyncio
async def test_enrich_review_request_skips_a_non_pr_url(monkeypatch):
    src = GitHubSource()
    calls = _fake_review_api(src, monkeypatch, [_review_requested("maintainer", "bot")])
    result: dict = {}
    await src._enrich_review_request(
        "https://api.github.com/repos/owner/repo/issues/7", result,
    )
    assert "review_requester" not in result
    assert not any("/events" in c for c in calls)


@pytest.mark.asyncio
async def test_enrich_review_request_rewrites_only_the_trailing_pulls_segment(
    monkeypatch,
):
    src = GitHubSource()
    calls = _fake_review_api(src, monkeypatch, [])
    await src._enrich_review_request(
        "https://api.github.com/repos/pulls/repo/pulls/7", {},
    )
    assert calls[-1] == (
        "https://api.github.com/repos/pulls/repo/issues/7/events?per_page=100"
    )


class _FakeProcRC:
    """A subprocess stand-in with a chosen exit code and optional hang."""

    def __init__(self, stdout: bytes = b"", returncode: int = 0, hang: bool = False):
        self._stdout = stdout
        self.returncode = returncode
        self._hang = hang
        self.killed = False

    async def communicate(self):
        if self._hang:
            import asyncio as _asyncio
            await _asyncio.sleep(10)
        return self._stdout, b"some error"

    def kill(self):
        self.killed = True

    async def wait(self):
        return self.returncode


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stdout", "returncode", "expected"),
    [
        (b"", 0, []),                       # no pages
        (b'{"message": "x"}', 0, None),     # not a list of pages
        (b"[[1]]", 1, None),                # gh failed (e.g. no --slurp support)
        (b"not json", 0, None),
    ],
)
async def test_gh_api_get_all_failure_paths(monkeypatch, stdout, returncode, expected):
    async def fake_exec(*args, **kwargs):
        return _FakeProcRC(stdout, returncode)

    monkeypatch.setattr(
        "nerve.sources.github.asyncio.create_subprocess_exec", fake_exec,
    )
    assert await GitHubSource._gh_api_get_all("repos/owner/repo/issues/7/events") == expected


@pytest.mark.asyncio
async def test_gh_api_get_all_kills_the_process_on_timeout(monkeypatch):
    proc = _FakeProcRC(hang=True)

    async def fake_exec(*args, **kwargs):
        return proc

    monkeypatch.setattr(
        "nerve.sources.github.asyncio.create_subprocess_exec", fake_exec,
    )
    result = await GitHubSource._gh_api_get_all(
        "repos/owner/repo/issues/7/events", timeout=0.01,
    )
    assert result is None
    assert proc.killed is True


@pytest.mark.asyncio
async def test_viewer_login_is_cached_and_a_failure_is_retried(monkeypatch):
    src = GitHubSource()
    answers = [None, {"login": "bot"}]
    calls: list[str] = []

    async def fake_get(url, timeout=30):
        calls.append(url)
        return answers.pop(0) if answers else {"login": "unexpected"}

    monkeypatch.setattr(src, "_gh_api_get", fake_get)

    assert await src._viewer_login() == ""      # failure: not cached
    assert await src._viewer_login() == "bot"   # retried
    assert await src._viewer_login() == "bot"   # cached: no third call
    assert calls == ["user", "user"]


@pytest.mark.asyncio
async def test_gh_api_get_all_flattens_slurped_pages(monkeypatch):
    seen: list[tuple] = []

    async def fake_exec(*args, **kwargs):
        seen.append(args)
        return _FakeProc(json.dumps([[{"n": 1}, {"n": 2}], [{"n": 3}]]).encode())

    monkeypatch.setattr(
        "nerve.sources.github.asyncio.create_subprocess_exec", fake_exec,
    )

    items = await GitHubSource._gh_api_get_all("repos/owner/repo/issues/7/events")
    assert items == [{"n": 1}, {"n": 2}, {"n": 3}]
    assert seen[0][:4] == ("gh", "api", "--paginate", "--slurp")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reason", "s_type", "expected"),
    [
        ("review_requested", "PullRequest", True),
        ("mention", "PullRequest", False),
        ("assign", "PullRequest", False),
    ],
)
async def test_enrich_notification_resolves_requester_only_for_review_requests(
    monkeypatch, reason, s_type, expected,
):
    src = GitHubSource()
    called: list[str] = []

    async def fake_get(url, timeout=30):
        if url.endswith("/pulls/7"):
            return {"html_url": "h", "user": {"login": "alice"}, "assignees": []}
        return []

    async def fake_review_request(subject_url, result):
        called.append(subject_url)

    monkeypatch.setattr(src, "_gh_api_get", fake_get)
    monkeypatch.setattr(src, "_enrich_review_request", fake_review_request)

    import asyncio as _asyncio

    await src._enrich_notification(
        {
            "reason": reason,
            "subject": {
                "type": s_type,
                "url": "https://api.github.com/repos/owner/repo/pulls/7",
            },
        },
        _asyncio.Semaphore(1),
    )
    assert bool(called) is expected


@pytest.mark.asyncio
async def test_maintainer_review_request_on_bot_pr_passes_guardrail(monkeypatch, db):
    """A maintainer requests a review on a dependency-update bot's PR.

    The author, the reviewers the bot requested, and every commenter are bots,
    and a review request carries no comment. Before the requester was collected
    this notification had no allowlisted actor, so a non-empty ``allow_actors``
    dropped it fail-closed and the request was lost. Exercises the real fetch +
    enrichment + production guardrail.
    """
    subject_url = "https://api.github.com/repos/owner/repo/pulls/7"
    notifications = [{
        "id": "n-review",
        "reason": "review_requested",
        "unread": True,
        "updated_at": "2026-01-02T10:00:00Z",
        "subject": {"title": "Bump a dependency", "type": "PullRequest", "url": subject_url},
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
        if url == "user":
            return {"login": "bot"}
        if url == subject_url:
            return {
                "html_url": "https://github.com/owner/repo/pull/7",
                "body": "Bumps a dependency.",
                "state": "open",
                "user": {"login": "some-ci[bot]"},
                "assignees": [],
                "labels": [{"name": "dependencies"}],
            }
        if url.endswith("/reviews"):
            return [{
                "id": 1, "state": "COMMENTED", "body": "Looks fine.",
                "submitted_at": "2026-01-01T00:00:00Z",
                "user": {"login": "review-helper[bot]"},
            }]
        return []                                    # no human comments

    async def fake_get_all(url, timeout=60):
        assert url == "https://api.github.com/repos/owner/repo/issues/7/events?per_page=100"
        return [
            _review_requested("some-ci[bot]", "alice"),
            _review_requested("maintainer", "bot"),
        ]

    src = GitHubSource()
    monkeypatch.setattr(src, "_gh_api_get", fake_get)
    # raising=False: the stub also installs on a source without the helper, so
    # the test fails on the dropped request, not on the stub.
    monkeypatch.setattr(src, "_gh_api_get_all", fake_get_all, raising=False)

    result = await src.fetch(cursor="2026-01-02T09:00:00Z")
    assert len(result.records) == 1
    record = result.records[0]

    assert record.metadata["actors"] == [
        "some-ci[bot]", "maintainer", "review-helper[bot]",
    ]
    assert "Review requested by: maintainer" in record.content

    cfg = NerveConfig.from_dict({
        "sync": {"github": {"enabled": True, "allow_actors": ["maintainer"]}},
    })
    gh = next(
        r for r in build_source_runners(cfg, db)
        if r.source.source_name == "github"
    )
    assert gh.inbox_filter.passes(record) is True
