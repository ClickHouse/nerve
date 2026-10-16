"""Unit tests for :class:`GitHubRepoEventsSource`.

The source reads each repo's issue-event log and emits one record per event,
naming the one login that acted. These tests stub ``_gh_api_conditional`` with a
small fake of the GitHub API (no network, no gh CLI) and cover: which field
holds the doer per event type, target filtering, the per-repo cursor and its
overlap window, ETag handling, pagination, failure isolation, and the end-to-end
runner path (a re-read event must not re-surface).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from datetime import datetime, timezone

import pytest

from nerve.config import NerveConfig
from nerve.sources import github_repo_events as mod
from nerve.sources.github_repo_events import (
    GitHubRepoEventsSource,
    _split_http_response,
)
from nerve.sources.models import SourceRecord
from nerve.sources.registry import build_source_runners
from nerve.sources.runner import SourceRunner

ME = "bot"

# ---------------------------------------------------------------------------
# Event builders — shaped like GET /repos/{o}/{r}/issues/events entries
# ---------------------------------------------------------------------------


def _issue(number: int, *, pr: bool) -> dict:
    issue = {
        "number": number,
        "html_url": f"https://github.com/owner/repo/{'pull' if pr else 'issues'}/{number}",
        "title": "Some title the author wrote",
        "body": "Some body the author wrote",
        "state": "open",
        "labels": [{"name": "bug"}],
    }
    if pr:
        issue["pull_request"] = {"url": "https://api.github.com/repos/owner/repo/pulls/1"}
    return issue


def review_requested(eid, at, requester, *, reviewer=ME, team=None, number=7,
                     event="review_requested"):
    ev = {
        "id": eid, "event": event, "created_at": at,
        "actor": {"login": requester} if requester else None,
        "review_requester": {"login": requester} if requester else None,
        "issue": _issue(number, pr=True),
    }
    if team:
        ev["requested_team"] = {"slug": team}
    else:
        ev["requested_reviewer"] = {"login": reviewer}
    return ev


def assigned(eid, at, assigner, *, assignee=ME, number=9, event="assigned"):
    # The real shape: `actor` is the ASSIGNEE, the doer is only in `assigner`.
    return {
        "id": eid, "event": event, "created_at": at,
        "actor": {"login": assignee},
        "assigner": {"login": assigner} if assigner else None,
        "assignee": {"login": assignee},
        "issue": _issue(number, pr=False),
    }


def labeled(eid, at, who="alice"):
    return {"id": eid, "event": "labeled", "created_at": at,
            "actor": {"login": who}, "label": {"name": "bug"},
            "issue": _issue(3, pr=False)}


class FakeGitHub:
    """Serves /user and /repos/{r}/issues/events pages, with ETags and 304s."""

    def __init__(self, login: str | None = ME, date=None):
        self.login = login
        self.date = date            # the server's Date header, if any
        self.events: dict[str, list[dict]] = {}   # newest first
        self.fail: set[str] = set()
        self.fail_pages: set[tuple[str, int]] = set()
        self.calls: list[tuple[str, str | None]] = []

    def add(self, repo: str, *events: dict) -> None:
        merged = self.events.get(repo, []) + list(events)
        self.events[repo] = sorted(merged, key=lambda e: e["created_at"], reverse=True)

    def _etag(self, repo: str) -> str:
        page = json.dumps(self.events.get(repo, [])[:100], sort_keys=True)
        return 'W/"%s"' % hashlib.sha1(page.encode()).hexdigest()

    async def __call__(self, endpoint, etag, timeout=30):
        self.calls.append((endpoint, etag))
        if endpoint == "user":
            return mod._Response(200, data={"login": self.login}) if self.login else mod._Response(None)
        m = re.fullmatch(
            r"repos/([^/]+/[^/]+)/issues/events\?per_page=100(?:&page=(\d+))?", endpoint,
        )
        assert m, endpoint
        repo, page = m.group(1), int(m.group(2) or 1)
        if repo in self.fail or (repo, page) in self.fail_pages:
            return mod._Response(None)
        tag = self._etag(repo)
        if page == 1 and etag is not None and etag == tag:
            return mod._Response(304, etag=tag, date=self.date)
        chunk = self.events.get(repo, [])[(page - 1) * 100: page * 100]
        return mod._Response(200, data=chunk, etag=(tag if page == 1 else None), date=self.date)


def make_source(repos=("owner/repo",), gh=None, **config):
    gh = gh or FakeGitHub()
    src = GitHubRepoEventsSource(config={"repos": list(repos), **config})
    src._gh_api_conditional = gh   # instance attribute shadows the staticmethod
    return src, gh


def positions(result) -> dict:
    return json.loads(result.next_cursor)


async def baseline(src, gh, repo="owner/repo", at="2026-01-01T10:00:00Z"):
    """Seed one old event and run the baseline fetch; return its cursor."""
    gh.add(repo, labeled(1, at))
    result = await src.fetch(None)
    assert result.records == []
    return result.next_cursor


# ---------------------------------------------------------------------------
# Baseline and basic emission
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_first_run_starts_each_repo_at_its_newest_event_without_records():
    src, gh = make_source(repos=("owner/a", "owner/b"))
    gh.add("owner/a", review_requested(1, "2026-01-01T10:00:00Z", "alice"))
    gh.add("owner/b", assigned(2, "2026-01-01T11:00:00Z", "carol"))

    result = await src.fetch(None)

    assert result.records == []
    # The newest event: the next read starts strictly after it.
    assert positions(result) == {
        "owner/a": "2026-01-01T10:00:00Z", "owner/b": "2026-01-01T11:00:00Z",
    }


@pytest.mark.asyncio
async def test_baseline_never_emits_events_that_existed_before_it():
    # The first full read after the baseline (here: no ETag, as after a
    # restart) must not emit the request made 5 minutes before the newest
    # event — it existed before the source started.
    src, gh = make_source()
    gh.add("owner/repo",
           review_requested(1, "2026-01-01T09:55:00Z", "alice"),
           labeled(2, "2026-01-01T10:00:00Z"))
    cursor = (await src.fetch(None)).next_cursor

    restarted, _ = make_source(gh=gh)                 # empty ETag cache
    gh.add("owner/repo", labeled(3, "2026-01-01T10:20:00Z"))
    assert (await restarted.fetch(cursor)).records == []


@pytest.mark.asyncio
async def test_repo_without_events_starts_at_the_server_clock():
    server_now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    src, gh = make_source(gh=FakeGitHub(date=server_now))
    first = await src.fetch(None)                 # no events at all yet
    assert first.records == []
    # The server's time minus the overlap: no older event exists to leak.
    assert positions(first) == {"owner/repo": "2026-01-01T11:50:00Z"}

    gh.add("owner/repo", review_requested(5, "2026-01-01T11:59:30Z", "alice"))
    [rec] = (await src.fetch(first.next_cursor)).records
    assert rec.id == "5"


@pytest.mark.asyncio
async def test_review_request_to_me_names_the_requester():
    src, gh = make_source()
    cursor = await baseline(src, gh)
    gh.add("owner/repo", review_requested(11, "2026-01-01T10:05:00Z", "alice"))

    result = await src.fetch(cursor)

    assert len(result.records) == 1
    rec = result.records[0]
    assert rec.id == "11"
    assert rec.source == "github_repo_events"
    assert rec.record_type == "github_issue_event"
    assert rec.timestamp == "2026-01-01T10:05:00Z"
    assert rec.summary == "[owner/repo] alice requested a review from bot on PR #7"
    assert rec.metadata == {
        "repo_name": "owner/repo",
        "repo_url": "https://github.com/owner/repo",
        "subject_type": "PullRequest",
        "subject_url": "https://github.com/owner/repo/pull/7",
        "reason": "review_requested",
        "event": "review_requested",
        "number": 7,
        "actor": "alice",
        "target": "bot",
        "target_type": "user",
    }
    assert positions(result) == {"owner/repo": "2026-01-01T10:05:00Z"}


@pytest.mark.asyncio
async def test_assignment_credits_the_assigner_not_the_actor_field():
    # On this endpoint `actor` of an assigned event is the assignee (the
    # account itself here). Reading it would credit the account with its own
    # assignment — and drop the event as "own action".
    src, gh = make_source()
    cursor = await baseline(src, gh)
    gh.add("owner/repo", assigned(12, "2026-01-01T10:05:00Z", "carol"))

    [rec] = (await src.fetch(cursor)).records

    assert rec.metadata["actor"] == "carol"
    assert rec.metadata["target"] == "bot"
    assert rec.metadata["subject_type"] == "Issue"
    assert rec.summary == "[owner/repo] carol assigned bot on issue #9"


@pytest.mark.asyncio
async def test_event_without_its_doer_field_is_dropped_not_credited_to_actor():
    src, gh = make_source(events=["review_requested", "assigned"], only_targeting_me=False)
    cursor = await baseline(src, gh)
    gh.add(
        "owner/repo",
        assigned(13, "2026-01-01T10:05:00Z", None, assignee="dave"),   # actor=dave
        review_requested(14, "2026-01-01T10:06:00Z", None),           # deleted account
    )

    assert (await src.fetch(cursor)).records == []


@pytest.mark.asyncio
async def test_events_aimed_at_others_need_only_targeting_me_off():
    on, gh_on = make_source()
    off, gh_off = make_source(only_targeting_me=False)
    for src, gh in ((on, gh_on), (off, gh_off)):
        cursor = await baseline(src, gh)
        gh.add("owner/repo",
               review_requested(15, "2026-01-01T10:05:00Z", "alice", reviewer="erin"))
        records = (await src.fetch(cursor)).records
        if src is on:
            assert records == []
        else:
            assert [r.metadata["target"] for r in records] == ["erin"]


@pytest.mark.asyncio
async def test_team_request_never_counts_as_targeting_me():
    on, gh_on = make_source()
    cursor = await baseline(on, gh_on)
    gh_on.add("owner/repo", review_requested(16, "2026-01-01T10:05:00Z", "alice", team="reviewers"))
    assert (await on.fetch(cursor)).records == []

    off, gh_off = make_source(only_targeting_me=False)
    cursor = await baseline(off, gh_off)
    gh_off.add("owner/repo", review_requested(16, "2026-01-01T10:05:00Z", "alice", team="reviewers"))
    [rec] = (await off.fetch(cursor)).records
    assert (rec.metadata["target"], rec.metadata["target_type"]) == ("reviewers", "team")
    assert "team reviewers" in rec.summary


@pytest.mark.asyncio
async def test_own_actions_are_never_emitted():
    src, gh = make_source(only_targeting_me=False)
    cursor = await baseline(src, gh)
    gh.add(
        "owner/repo",
        review_requested(17, "2026-01-01T10:05:00Z", "Bot", reviewer="erin"),
        assigned(18, "2026-01-01T10:06:00Z", "bot", assignee="erin"),
        review_requested(19, "2026-01-01T10:07:00Z", "bot"),      # self-request
    )
    assert (await src.fetch(cursor)).records == []


@pytest.mark.asyncio
async def test_only_configured_event_types_are_emitted():
    src, gh = make_source()   # default: review_requested, assigned
    cursor = await baseline(src, gh)
    gh.add(
        "owner/repo",
        assigned(20, "2026-01-01T10:05:00Z", "carol", event="unassigned"),
        review_requested(21, "2026-01-01T10:06:00Z", "alice", event="review_request_removed"),
        labeled(22, "2026-01-01T10:07:00Z"),
    )
    assert (await src.fetch(cursor)).records == []

    src2, gh2 = make_source(events=["unassigned", "review_request_removed"])
    cursor = await baseline(src2, gh2)
    gh2.add(
        "owner/repo",
        assigned(20, "2026-01-01T10:05:00Z", "carol", event="unassigned"),
        review_requested(21, "2026-01-01T10:06:00Z", "alice", event="review_request_removed"),
    )
    records = (await src2.fetch(cursor)).records
    assert [(r.metadata["event"], r.metadata["actor"]) for r in records] == [
        ("unassigned", "carol"), ("review_request_removed", "alice"),
    ]


def test_unsupported_event_types_are_ignored_with_a_warning(caplog):
    with caplog.at_level(logging.WARNING):
        src = GitHubRepoEventsSource(config={
            "repos": ["owner/repo"], "events": ["mentioned", "assigned", "assigned"],
        })
    assert src._events == ["assigned"]
    assert "mentioned" in caplog.text


@pytest.mark.asyncio
async def test_no_supported_event_types_fetches_nothing():
    src, gh = make_source(events=["mentioned"])
    result = await src.fetch(None)
    assert result.records == [] and gh.calls == []


@pytest.mark.asyncio
async def test_records_carry_no_issue_text():
    src, gh = make_source()
    cursor = await baseline(src, gh)
    gh.add("owner/repo", review_requested(23, "2026-01-01T10:05:00Z", "alice"))
    [rec] = (await src.fetch(cursor)).records
    for text in (rec.summary, rec.content, json.dumps(rec.metadata)):
        assert "Some title" not in text and "Some body" not in text
        assert "open" not in text and "bug" not in text


# ---------------------------------------------------------------------------
# Cursor, overlap window, ETags
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_overlap_rereads_recent_events_identically_and_catches_late_ones():
    src, gh = make_source()
    cursor = await baseline(src, gh)
    gh.add("owner/repo", review_requested(30, "2026-01-01T10:30:00Z", "alice"))
    gh.date = datetime(2026, 1, 1, 10, 31, tzinfo=timezone.utc)
    first = await src.fetch(cursor)
    [rec30] = first.records
    # Held back to the read's time minus the overlap, so the next read covers
    # the last 10 minutes again.
    assert positions(first) == {"owner/repo": "2026-01-01T10:21:00Z"}

    # Next poll: a new event, plus one created before the cursor but listed only
    # now — inside the overlap — and one far below it.
    gh.add(
        "owner/repo",
        review_requested(31, "2026-01-01T10:40:00Z", "carol"),
        review_requested(32, "2026-01-01T10:25:00Z", "dave"),    # 5 min below cursor
        review_requested(33, "2026-01-01T10:05:00Z", "erin"),    # 25 min below
    )
    gh.date = datetime(2026, 1, 1, 10, 41, tzinfo=timezone.utc)
    second = await src.fetch(first.next_cursor)

    by_id = {r.id: r for r in second.records}
    assert set(by_id) == {"30", "31", "32"}
    # A re-read event is byte-identical, so the inbox stores nothing new.
    assert (by_id["30"].content, by_id["30"].metadata) == (rec30.content, rec30.metadata)
    assert positions(second) == {"owner/repo": "2026-01-01T10:31:00Z"}


@pytest.mark.asyncio
async def test_position_never_moves_backwards_on_a_skewed_clock():
    src, gh = make_source()
    cursor = await baseline(src, gh, at="2026-01-01T10:30:00Z")
    gh.add("owner/repo", labeled(2, "2026-01-01T10:35:00Z"))
    gh.date = datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)   # far behind
    result = await src.fetch(cursor)
    assert positions(result) == {"owner/repo": "2026-01-01T10:30:00Z"}


def _event_calls(gh):
    return [c for c in gh.calls if c[0] != "user"]


@pytest.mark.asyncio
async def test_unchanged_repo_answers_304_and_keeps_its_cursor():
    src, gh = make_source()
    cursor = await baseline(src, gh)
    gh.calls.clear()

    second = await src.fetch(cursor)

    assert second.records == []
    assert second.next_cursor == cursor
    [(endpoint, etag)] = _event_calls(gh)
    assert etag is not None


@pytest.mark.asyncio
async def test_no_etag_after_a_poll_that_emitted_records():
    # If persisting that poll's records failed, a 304 next time would hide
    # them; so the next poll is a full read (an identical re-read is a no-op).
    src, gh = make_source()
    cursor = await baseline(src, gh)
    gh.add("owner/repo", review_requested(40, "2026-01-01T10:05:00Z", "alice"))
    first = await src.fetch(cursor)
    assert [r.id for r in first.records] == ["40"]
    gh.calls.clear()

    again = await src.fetch(first.next_cursor)
    assert [etag for _, etag in _event_calls(gh)] == [None]
    # A quiet repo: the position settled on the newest event, so nothing is
    # re-read, nothing is emitted, and the next poll is a free 304 again.
    assert again.records == []
    gh.calls.clear()
    third = await src.fetch(again.next_cursor)
    assert third.records == []
    [(_, etag)] = _event_calls(gh)
    assert etag is not None


@pytest.mark.asyncio
async def test_etag_is_not_sent_for_a_cursor_it_did_not_produce():
    src, gh = make_source()
    await baseline(src, gh)                      # ETag kept for 10:00
    gh.calls.clear()

    await src.fetch('{"owner/repo": "2026-01-01T10:05:00Z"}')

    assert [etag for _, etag in _event_calls(gh)] == [None]


@pytest.mark.asyncio
async def test_failed_repo_keeps_its_cursor_while_others_advance():
    src, gh = make_source(repos=("owner/a", "owner/b"))
    gh.add("owner/a", labeled(1, "2026-01-01T10:00:00Z"))
    gh.add("owner/b", labeled(2, "2026-01-01T10:00:00Z"))
    cursor = (await src.fetch(None)).next_cursor

    gh.add("owner/a", review_requested(50, "2026-01-01T11:00:00Z", "alice"))
    gh.add("owner/b", review_requested(51, "2026-01-01T11:00:00Z", "carol"))
    gh.fail.add("owner/b")
    result = await src.fetch(cursor)

    assert [r.id for r in result.records] == ["50"]
    assert positions(result) == {
        "owner/a": "2026-01-01T11:00:00Z", "owner/b": "2026-01-01T10:00:00Z",
    }

    gh.fail.clear()
    recovered = await src.fetch(result.next_cursor)
    assert "51" in {r.id for r in recovered.records}


@pytest.mark.asyncio
async def test_newly_configured_repo_starts_at_baseline():
    src, gh = make_source(repos=("owner/a",))
    gh.add("owner/a", labeled(1, "2026-01-01T10:00:00Z"))
    cursor = (await src.fetch(None)).next_cursor

    widened, _ = make_source(repos=("owner/a", "owner/b"), gh=gh)
    gh.add("owner/b", review_requested(60, "2026-01-01T09:00:00Z", "alice"))
    result = await widened.fetch(cursor)

    assert result.records == []          # no backfill of owner/b's history
    assert positions(result)["owner/b"] == "2026-01-01T09:00:00Z"


@pytest.mark.asyncio
async def test_unreadable_cursor_starts_again_from_baseline(caplog):
    src, gh = make_source()
    gh.add("owner/repo", review_requested(61, "2026-01-01T10:00:00Z", "alice"))
    with caplog.at_level(logging.WARNING):
        result = await src.fetch("2026-01-01T09:00:00Z")   # not a JSON object
    assert result.records == []
    assert positions(result) == {"owner/repo": "2026-01-01T10:00:00Z"}
    assert "unreadable cursor" in caplog.text


@pytest.mark.asyncio
async def test_unknown_login_fetches_nothing_and_keeps_the_cursor():
    src, gh = make_source(gh=FakeGitHub(login=None))
    result = await src.fetch('{"owner/repo": "2026-01-01T10:00:00Z"}')
    assert result.records == []
    assert result.next_cursor == '{"owner/repo": "2026-01-01T10:00:00Z"}'
    assert [c[0] for c in gh.calls] == ["user"]


@pytest.mark.asyncio
async def test_login_is_cached_after_success():
    src, gh = make_source()
    await baseline(src, gh)
    await src.fetch(None)
    assert [c[0] for c in gh.calls].count("user") == 1


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

def _many(start_id, count, start_minute=1):
    return [
        review_requested(start_id + i, f"2026-01-01T1{1 + (start_minute + i) // 60}:"
                         f"{(start_minute + i) % 60:02d}:00Z", "alice")
        for i in range(count)
    ]


@pytest.mark.asyncio
async def test_reads_further_pages_back_to_the_floor():
    src, gh = make_source()
    cursor = await baseline(src, gh, at="2026-01-01T11:00:00Z")
    gh.add("owner/repo", *_many(1000, 150))

    result = await src.fetch(cursor)

    assert len(result.records) == 150
    pages = [e for e, _ in gh.calls if "issues/events" in e]
    assert pages[-1].endswith("&page=2")


@pytest.mark.asyncio
async def test_stops_at_the_page_cap_with_a_warning(monkeypatch, caplog):
    monkeypatch.setattr(mod, "_MAX_PAGES", 2)
    src, gh = make_source()
    cursor = await baseline(src, gh, at="2026-01-01T11:00:00Z")
    gh.add("owner/repo", *_many(1000, 250))

    with caplog.at_level(logging.WARNING):
        result = await src.fetch(cursor)

    assert len(result.records) == 200
    assert "are not read" in caplog.text


@pytest.mark.asyncio
async def test_failed_later_page_emits_nothing_and_keeps_the_cursor():
    src, gh = make_source()
    cursor = await baseline(src, gh, at="2026-01-01T11:00:00Z")
    gh.add("owner/repo", *_many(1000, 150))
    gh.fail_pages.add(("owner/repo", 2))

    result = await src.fetch(cursor)

    assert result.records == []
    assert result.next_cursor == cursor


# ---------------------------------------------------------------------------
# gh plumbing
# ---------------------------------------------------------------------------

def test_split_http_response_reads_status_headers_and_body():
    text = 'HTTP/2.0 200 OK\r\nEtag: W/"abc"\r\nX-Other: 1\r\n\r\n[{"id": 1}]'
    status, headers, body = _split_http_response(text)
    assert status == 200 and headers["etag"] == 'W/"abc"' and json.loads(body) == [{"id": 1}]

    status, headers, body = _split_http_response('HTTP/2.0 304 Not Modified\r\nEtag: W/"abc"\r\n\r\n')
    assert status == 304 and body == ""

    assert _split_http_response("gh: not found")[0] is None


class _FakeProc:
    """Like asyncio.subprocess.Process: returncode is None while running."""

    def __init__(self, stdout=b"", returncode=0, hang=False):
        self._stdout, self._exit, self._hang = stdout, returncode, hang
        self.returncode = None
        self.killed = False

    async def communicate(self):
        if self._hang:
            await asyncio.sleep(10)
        self.returncode = self._exit
        return self._stdout, b"gh: HTTP 304"

    def kill(self):
        self.killed = True
        self.returncode = -9

    async def wait(self):
        return self.returncode


@pytest.mark.asyncio
async def test_gh_conditional_treats_a_304_exit_as_not_modified(monkeypatch):
    seen = []

    async def fake_exec(*args, **kwargs):
        seen.append(args)
        return _FakeProc(b'HTTP/2.0 304 Not Modified\r\nEtag: W/"abc"\r\n\r\n', returncode=1)

    monkeypatch.setattr(mod.asyncio, "create_subprocess_exec", fake_exec)
    resp = await GitHubRepoEventsSource._gh_api_conditional(
        "repos/owner/repo/issues/events?per_page=100", 'W/"abc"',
    )
    assert (resp.status, resp.etag, resp.data) == (304, 'W/"abc"', None)
    assert seen[0] == ("gh", "api", "-i", "repos/owner/repo/issues/events?per_page=100",
                       "-H", 'If-None-Match: W/"abc"')


@pytest.mark.asyncio
async def test_gh_conditional_kills_the_process_on_timeout(monkeypatch):
    proc = _FakeProc(hang=True)

    async def fake_exec(*args, **kwargs):
        return proc

    monkeypatch.setattr(mod.asyncio, "create_subprocess_exec", fake_exec)
    resp = await GitHubRepoEventsSource._gh_api_conditional("user", None, timeout=0.01)
    assert resp.status is None
    assert proc.killed is True


@pytest.mark.asyncio
async def test_gh_conditional_reports_errors_as_no_response(monkeypatch):
    async def fake_exec(*args, **kwargs):
        return _FakeProc(b"", returncode=1)

    monkeypatch.setattr(mod.asyncio, "create_subprocess_exec", fake_exec)
    assert (await GitHubRepoEventsSource._gh_api_conditional("user", None)).status is None


# ---------------------------------------------------------------------------
# Config + registry
# ---------------------------------------------------------------------------

def test_config_defaults_and_values():
    default = NerveConfig.from_dict({}).sync.github_repo_events
    assert default.enabled is False
    assert default.repos == []
    assert default.events == ["review_requested", "assigned"]
    assert default.only_targeting_me is True
    assert default.allow_actors == [] and default.deny_actors == []

    cfg = NerveConfig.from_dict({"sync": {"github_repo_events": {
        "enabled": True, "repos": ["owner/repo"], "events": ["unassigned"],
        "only_targeting_me": False, "allow_actors": ["alice"], "deny_actors": ["mallory"],
        "schedule": "*/5 * * * *",
    }}}).sync.github_repo_events
    assert (cfg.enabled, cfg.repos, cfg.events, cfg.only_targeting_me) == (
        True, ["owner/repo"], ["unassigned"], False,
    )
    assert (cfg.allow_actors, cfg.deny_actors, cfg.schedule) == (
        ["alice"], ["mallory"], "*/5 * * * *",
    )


def _rec(actor: str) -> SourceRecord:
    return SourceRecord(
        id="1", source="github_repo_events", record_type="github_issue_event",
        summary="s", content="c", timestamp="2026-01-01T00:00:00Z",
        metadata={"actor": actor, "repo_name": "owner/repo"},
    )


@pytest.mark.asyncio
async def test_registry_wires_the_source_and_its_actor_guardrail(db):
    cfg = NerveConfig.from_dict({"sync": {
        "github": {"enabled": False},
        "github_repo_events": {
            "enabled": True, "repos": ["owner/repo"],
            "allow_actors": ["alice", "carol"], "deny_actors": ["carol"],
        },
    }})
    [runner] = [r for r in build_source_runners(cfg, db)
                if r.source.source_name == "github_repo_events"]
    assert isinstance(runner.source, GitHubRepoEventsSource)
    assert runner.inbox_filter.active is True
    assert runner.inbox_filter.passes(_rec("Alice")) is True
    assert runner.inbox_filter.passes(_rec("carol")) is False      # deny wins
    assert runner.inbox_filter.passes(_rec("stranger")) is False   # fail-closed


@pytest.mark.asyncio
async def test_registry_skips_the_source_when_disabled(db):
    cfg = NerveConfig.from_dict({"sync": {"github": {"enabled": False}}})
    assert not [r for r in build_source_runners(cfg, db)
                if r.source.source_name == "github_repo_events"]


# ---------------------------------------------------------------------------
# End to end through the runner and the inbox store
# ---------------------------------------------------------------------------

async def _stored(db) -> dict[str, int]:
    async with db.db.execute(
        "SELECT id, rowid FROM source_messages WHERE source = 'github_repo_events'",
    ) as cur:
        return {row[0]: row[1] async for row in cur}


@pytest.mark.asyncio
async def test_runner_stores_each_event_once_and_a_reread_does_not_resurface(db):
    src, gh = make_source()
    runner = SourceRunner(source=src, db=db, batch_size=50)
    gh.add("owner/repo", labeled(1, "2026-01-01T10:00:00Z"))
    await runner.run()                                   # baseline
    assert await _stored(db) == {}

    gh.add("owner/repo", review_requested(70, "2026-01-01T10:05:00Z", "alice"))
    gh.date = datetime(2026, 1, 1, 10, 7, tzinfo=timezone.utc)
    await runner.run()
    first = await _stored(db)
    assert set(first) == {"70"}

    # A newer event changes the first page; event 70 is re-read in the overlap.
    gh.add("owner/repo", review_requested(71, "2026-01-01T10:06:00Z", "carol"))
    gh.date = datetime(2026, 1, 1, 10, 8, tzinfo=timezone.utc)
    await runner.run()
    second = await _stored(db)
    assert set(second) == {"70", "71"}
    assert second["70"] == first["70"]                   # not re-surfaced


# ---------------------------------------------------------------------------
# Hardening
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_review_event_whose_actor_differs_from_its_requester_is_dropped():
    # GitHub writes the doer of a review event to both fields. An event where
    # they disagree is not understood, so nobody is credited.
    src, gh = make_source()
    cursor = await baseline(src, gh)
    odd = review_requested(80, "2026-01-01T10:05:00Z", "alice")
    odd["actor"] = {"login": "mallory"}
    gh.add("owner/repo", odd)
    assert (await src.fetch(cursor)).records == []


@pytest.mark.asyncio
async def test_url_is_built_not_read_from_the_issue():
    # html_url changes on a repo rename or an issue transfer; reading it would
    # make a re-read record differ and re-surface.
    src, gh = make_source()
    cursor = await baseline(src, gh)
    ev = review_requested(81, "2026-01-01T10:05:00Z", "alice")
    ev["issue"]["html_url"] = "https://github.com/new-owner/new-repo/pull/99"
    gh.add("owner/repo", ev)
    [rec] = (await src.fetch(cursor)).records
    assert rec.metadata["subject_url"] == "https://github.com/owner/repo/pull/7"
    assert "new-owner" not in rec.content


@pytest.mark.asyncio
async def test_malformed_events_are_skipped_without_failing_the_fetch():
    src, gh = make_source()
    cursor = await baseline(src, gh)
    gh.events["owner/repo"] = [
        review_requested(82, "2026-01-01T10:06:00Z", "alice"),
        {"id": 83, "event": "review_requested", "created_at": 12345},
        {"id": 84, "event": "assigned", "created_at": None},
        {"id": 85, "event": "review_requested", "created_at": "2026-01-01T10:05:00Z",
         "issue": {"number": True}},
    ] + gh.events["owner/repo"]
    result = await src.fetch(cursor)
    assert [r.id for r in result.records] == ["82"]


def test_cursor_positions_are_normalized_to_utc():
    assert mod._load_cursor(json.dumps({
        "owner/a": "2026-01-01T12:00:00+02:00",
        "owner/b": "2026-01-01T10:00:00",            # naive → UTC
        "owner/c": "not a time",
        "owner/d": 5,
    })) == {"owner/a": "2026-01-01T10:00:00Z", "owner/b": "2026-01-01T10:00:00Z"}


@pytest.mark.asyncio
async def test_configured_username_skips_the_user_lookup():
    src, gh = make_source(gh=FakeGitHub(login=None), username="bot")
    cursor = await baseline(src, gh)
    gh.add("owner/repo", review_requested(86, "2026-01-01T10:05:00Z", "alice"))
    [rec] = (await src.fetch(cursor)).records
    assert rec.metadata["target"] == "bot"
    assert "user" not in [c[0] for c in gh.calls]


def test_repos_are_deduplicated():
    src = GitHubRepoEventsSource(config={"repos": ["owner/a", " owner/a ", "", "owner/b"]})
    assert src._repos == ["owner/a", "owner/b"]


@pytest.mark.asyncio
async def test_gh_conditional_reads_the_date_header(monkeypatch):
    async def fake_exec(*args, **kwargs):
        return _FakeProc(
            b"HTTP/2.0 200 OK\r\nDate: Thu, 01 Jan 2026 12:00:00 GMT\r\n"
            b'Etag: W/"x"\r\n\r\n[]', returncode=0,
        )

    monkeypatch.setattr(mod.asyncio, "create_subprocess_exec", fake_exec)
    resp = await GitHubRepoEventsSource._gh_api_conditional("repos/o/r/issues/events", None)
    assert resp.status == 200 and resp.data == [] and resp.etag == 'W/"x"'
    assert resp.date == datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_gh_conditional_kills_the_process_when_cancelled(monkeypatch):
    proc = _FakeProc(hang=True)

    async def fake_exec(*args, **kwargs):
        return proc

    monkeypatch.setattr(mod.asyncio, "create_subprocess_exec", fake_exec)
    task = asyncio.ensure_future(GitHubRepoEventsSource._gh_api_conditional("user", None))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert proc.killed is True


def test_config_reads_username():
    cfg = NerveConfig.from_dict({"sync": {"github_repo_events": {"username": "bot"}}})
    assert cfg.sync.github_repo_events.username == "bot"
    assert NerveConfig.from_dict({}).sync.github_repo_events.username == ""


@pytest.mark.asyncio
async def test_gh_conditional_reads_a_zoneless_date_header_as_utc(monkeypatch):
    async def fake_exec(*args, **kwargs):
        return _FakeProc(
            b"HTTP/1.1 200 OK\r\nDate: Thu, 01 Jan 2026 12:00:00 -0000\r\n\r\n[]",
        )

    monkeypatch.setattr(mod.asyncio, "create_subprocess_exec", fake_exec)
    resp = await GitHubRepoEventsSource._gh_api_conditional("repos/o/r/issues/events", None)
    assert resp.date == datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def test_event_timestamps_are_normalized_before_comparison():
    assert mod._created({"created_at": "2026-01-01T12:00:00+02:00"}) == "2026-01-01T10:00:00Z"
    assert mod._created({"created_at": "2026-01-01T10:00:00.5Z"}) == "2026-01-01T10:00:00Z"
    assert mod._created({"created_at": "x"}) == ""
    assert mod._created({"created_at": 5}) == ""
