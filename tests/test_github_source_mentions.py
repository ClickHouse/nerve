"""GitHub source: whether an update of a ``mention`` thread mentions the account.

GitHub's ``reason`` belongs to the thread: once the account was mentioned in a
thread, every later notification of that thread says ``mention``. The source
reads what was posted since the thread's previous update and says in the
record whether that mentions the account (``mention``: ``new`` or ``old``).

The fake below answers the calls the source makes for one thread: the REST
reads (subject, comments, inline review comments, the gh login) and the
GraphQL read of the mention events and the reviews.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit

import pytest

from nerve.sources import github as gh
from nerve.sources.github import GitHubSource, notification_record
from nerve.sources.github_events import GitHubEventsSource

_API = "https://api.github.com/repos/acme/widgets"
_WEB = "https://github.com/acme/widgets"
_T0 = datetime(2026, 3, 2, 10, 0, 0, tzinfo=timezone.utc)

# The account gh acts as, and the moment its previous look at the thread ended.
_ME = "alice"
_START = _T0


def _ts(seconds: int) -> str:
    return (_T0 + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _comment(cid: int, user: str, body: str, at: int, edited: int | None = None) -> dict:
    return {
        "id": cid,
        "user": {"login": user},
        "body": body,
        "created_at": _ts(at),
        "updated_at": _ts(at if edited is None else edited),
        "html_url": f"{_WEB}/issues/7#issuecomment-{cid}",
    }


def _inline(cid: int, user: str, body: str, at: int) -> dict:
    return {
        "id": cid,
        "user": {"login": user},
        "body": body,
        "path": "src/widget.py",
        "line": 3,
        "created_at": _ts(at),
        "updated_at": _ts(at),
        "html_url": f"{_WEB}/pull/7#discussion_r{cid}",
    }


def _review(user: str, body: str, at: int) -> dict:
    return {
        "author": {"login": user},
        "body": body,
        "submittedAt": _ts(at),
        "url": f"{_WEB}/pull/7#pullrequestreview-{at}",
    }


def _notif(
    kind: str = "Issue",
    *,
    reason: str = "mention",
    updated_at: int = 60,
    thread_id: str = "t1",
) -> dict:
    path = "pulls" if kind == "PullRequest" else "issues"
    return {
        "id": thread_id,
        "reason": reason,
        "unread": True,
        "updated_at": _ts(updated_at),
        "subject": {
            "title": "Widget breaks",
            "type": kind,
            "url": f"{_API}/{path}/7",
            "latest_comment_url": None,
        },
        "repository": {"full_name": "acme/widgets", "html_url": _WEB},
    }


class _FakeGitHub:
    """One thread on GitHub, as the source reads it."""

    def __init__(self, kind: str = "Issue"):
        self.kind = kind
        self.login: str | None = _ME
        self.subject = {
            "html_url": f"{_WEB}/{'pull' if kind == 'PullRequest' else 'issues'}/7",
            "body": "It breaks.",
            "state": "open",
            "user": {"login": "carol"},
            "assignees": [],
            "labels": [],
            "created_at": _ts(-86_400),
        }
        self.comments: list[dict] = []
        self.inline: list[dict] | None = []
        # Mention events, oldest first: (seconds, login of the mentioned account).
        self.events: list[tuple[int, str]] = []
        self.event_page_size = 100
        self.reviews: list[dict] = []
        self.graphql_fails = False
        self.calls: list[str] = []

    async def get(self, url: str, timeout: float = 30):
        self.calls.append(url)
        if url == "user":
            return {"login": self.login} if self.login else None
        parts = urlsplit(url)
        query = parse_qs(parts.query)
        if parts.path.endswith(("/issues/7", "/pulls/7")):
            return self.subject
        if parts.path.endswith("/issues/7/comments"):
            since = query["since"][0]
            page = int(query.get("page", ["1"])[0])
            listed = [c for c in self.comments if c["updated_at"] >= since]
            return listed[(page - 1) * 100:page * 100]
        if parts.path.endswith("/pulls/7/comments"):
            if self.inline is None:
                return None
            assert query["sort"] == ["updated"] and query["direction"] == ["desc"]
            return sorted(self.inline, key=lambda c: c["updated_at"], reverse=True)[:100]
        if parts.path.endswith("/pulls/7/reviews"):
            return []
        return None

    async def graphql(self, query: str, variables: dict, timeout: float = 30):
        self.calls.append(f"graphql before={variables.get('before')}")
        if self.graphql_fails:
            return None
        assert variables["owner"] == "acme" and variables["repo"] == "widgets"
        assert variables["number"] == 7
        # Pages are cut from the newest end, as `last: 100, before: <cursor>` does.
        end = int(variables["before"]) if variables.get("before") else len(self.events)
        begin = max(0, end - self.event_page_size)
        thread: dict = {
            "timelineItems": {
                "pageInfo": {"hasPreviousPage": begin > 0, "startCursor": str(begin)},
                "nodes": [
                    {"createdAt": _ts(at), "actor": {"login": login}}
                    for at, login in self.events[begin:end]
                ],
            },
        }
        if self.kind == "PullRequest":
            thread["reviews"] = {"nodes": self.reviews}
        return {"repository": {"issueOrPullRequest": thread}}


def _source(monkeypatch, fake: _FakeGitHub, *, login: str = _ME) -> GitHubSource:
    src = GitHubSource()
    monkeypatch.setattr(src, "_gh_api_get", fake.get)
    monkeypatch.setattr(src, "_gh_graphql", fake.graphql)
    src._run = gh._Run(login=login, first_look=_START)
    return src


async def _record(src: GitHubSource, notif: dict):
    extra = await src._enrich_notification(notif, asyncio.Semaphore(5))
    return extra, notification_record(notif, extra)


# ---------------------------------------------------------------------------
# What counts as a mention in a text
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text,expected", [
    ("cc @alice", True),
    ("@Alice, please look", True),
    ("(@alice)", True),
    ("thanks @alice.", True),
    ("@alice's change", True),
    ("@alice-bot ping", False),      # another, longer handle
    ("@alicex", False),
    ("@alice2", False),
    ("@alice/reviewers", False),     # a team of an organization of that name
    ("mail carol@alice", False),     # the end of an address
    ("see `@alice` in the log", False),
    ("```\n@alice\n```", False),
    ("~~~\n@alice\n~~~", False),
    ("", False),
    (None, False),
])
def test_what_text_mentions_the_login(text, expected):
    assert gh._mentions(gh._mention_re("alice"), text) is expected


def test_a_login_with_a_hyphen_is_matched_whole():
    pattern = gh._mention_re("alice-dev")
    assert gh._mentions(pattern, "ping @alice-dev")
    assert not gh._mentions(pattern, "ping @alice")
    assert not gh._mentions(pattern, "ping @alice-developer")


# ---------------------------------------------------------------------------
# new / old / not known
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_a_mention_in_a_new_comment_is_new(monkeypatch):
    fake = _FakeGitHub()
    fake.comments = [
        _comment(11, "bob", "@alice can you look at this?", 30),
        _comment(12, "some-ci[bot]", "Build finished.", 40),
    ]
    fake.events = [(32, "alice")]   # GitHub writes the event after the comment
    src = _source(monkeypatch, fake)

    extra, record = await _record(src, _notif())

    assert extra["mention"] == {
        "state": "new",
        "at": _ts(30),
        "by": "bob",
        "url": f"{_WEB}/issues/7#issuecomment-11",
        "body": "@alice can you look at this?",
        "created_at": _ts(30),
    }
    assert record.summary == "[acme/widgets] Widget breaks (mention)"
    assert record.metadata["reason"] == "mention"
    assert record.metadata["mention"] == "new"
    assert record.metadata["mention_at"] == _ts(30)
    assert record.metadata["mention_by"] == "bob"
    assert record.metadata["mention_url"] == f"{_WEB}/issues/7#issuecomment-11"
    assert "bob" in record.metadata["actors"]
    assert f"Mention: new by bob ({_ts(30)}, {_WEB}/issues/7#issuecomment-11)" in record.content
    # The mention text is quoted once, under its own heading.
    assert f"--- Mention (bob, {_ts(30)}) ---\n@alice can you look at this?" in record.content
    assert record.content.count("@alice can you look at this?") == 1
    assert "New Comments" not in record.content


@pytest.mark.asyncio
async def test_a_mention_by_a_bot_is_quoted_and_its_author_is_an_actor(monkeypatch):
    # A bot is left out of the new comments, so without the mention text the
    # record would name neither the bot nor what it wrote.
    fake = _FakeGitHub()
    fake.comments = [
        _comment(11, "some-ci[bot]", "@alice the review found an issue.", 30),
        _comment(12, "bob", "Looking.", 45),
    ]
    fake.events = [(31, "alice")]
    src = _source(monkeypatch, fake)

    extra, record = await _record(src, _notif())

    assert extra["mention"]["by"] == "some-ci[bot]"
    assert "--- Mention (some-ci[bot], " in record.content
    assert "some-ci[bot]" in record.metadata["actors"]
    assert "--- New Comments (human, newest first) ---" in record.content
    assert "\nbob (" in record.content


@pytest.mark.asyncio
async def test_an_update_without_a_mention_on_a_mentioned_thread_is_old(monkeypatch):
    fake = _FakeGitHub()
    fake.comments = [_comment(12, "bob", "Fixed on all branches.", 30)]
    fake.events = [(-9_000_000, "alice"), (-8_000_000, "dave"), (29, "dave")]
    src = _source(monkeypatch, fake)

    extra, record = await _record(src, _notif())

    assert extra["mention"] == {"state": "old", "at": _ts(-9_000_000)}
    assert record.summary == "[acme/widgets] Widget breaks (update, no new mention)"
    # GitHub's reason stays as it is: the reason guardrail reads it.
    assert record.metadata["reason"] == "mention"
    assert record.metadata["mention"] == "old"
    assert record.metadata["mention_at"] == _ts(-9_000_000)
    assert "mention_by" not in record.metadata
    assert "mention_url" not in record.metadata
    assert "Mention: none in this update." in record.content
    assert f"(the last was at {_ts(-9_000_000)})" in record.content
    assert "--- Mention" not in record.content
    assert "Fixed on all branches." in record.content


@pytest.mark.asyncio
async def test_no_known_mention_leaves_the_record_as_github_says(monkeypatch):
    # GitHub says mention, and nothing this look can read explains it: the
    # record is not moved away from mention on a guess.
    fake = _FakeGitHub()
    fake.comments = [_comment(12, "bob", "Any news?", 30)]
    fake.events = [(20, "dave")]
    src = _source(monkeypatch, fake)

    extra, record = await _record(src, _notif())

    assert "mention" not in extra
    assert record.summary == "[acme/widgets] Widget breaks (mention)"
    assert not [k for k in record.metadata if k.startswith("mention")]
    assert "Mention:" not in record.content


@pytest.mark.asyncio
async def test_a_failed_read_of_the_mention_events_leaves_the_record(monkeypatch):
    fake = _FakeGitHub()
    fake.events = [(-9_000_000, "alice")]
    fake.graphql_fails = True
    src = _source(monkeypatch, fake)

    extra, record = await _record(src, _notif())

    assert "mention" not in extra
    assert record.summary.endswith("(mention)")
    assert "mention" not in record.metadata


@pytest.mark.asyncio
async def test_without_a_login_nothing_is_checked(monkeypatch):
    fake = _FakeGitHub()
    fake.events = [(-9_000_000, "alice")]
    src = _source(monkeypatch, fake, login="")

    extra, record = await _record(src, _notif())

    assert "mention" not in extra
    assert record.summary.endswith("(mention)")
    assert not [c for c in fake.calls if c.startswith("graphql")]


@pytest.mark.asyncio
async def test_other_reasons_are_not_checked(monkeypatch):
    fake = _FakeGitHub()
    fake.comments = [_comment(11, "bob", "@alice look", 30)]
    fake.events = [(31, "alice")]
    src = _source(monkeypatch, fake)

    extra, record = await _record(src, _notif(reason="review_requested"))

    assert "mention" not in extra
    assert record.summary.endswith("(review_requested)")
    assert not [c for c in fake.calls if c.startswith("graphql")]
    # The comments since the previous update are still added.
    assert "@alice look" in record.content


@pytest.mark.asyncio
async def test_a_mention_added_by_an_edit_is_new(monkeypatch):
    # The comment is older than the previous update; the edit is not, and
    # GitHub writes the mention event at the edit.
    fake = _FakeGitHub()
    fake.comments = [_comment(11, "bob", "Reproduced. cc @alice", -500, edited=30)]
    fake.events = [(31, "alice")]
    src = _source(monkeypatch, fake)

    extra, _ = await _record(src, _notif())

    assert extra["mention"]["state"] == "new"
    assert extra["mention"]["by"] == "bob"
    assert extra["mention"]["at"] == _ts(30)


@pytest.mark.asyncio
async def test_a_mention_in_the_description_of_a_new_thread_is_new(monkeypatch):
    fake = _FakeGitHub()
    fake.subject["created_at"] = _ts(30)
    fake.subject["body"] = "Found by @alice in testing."
    fake.events = [(31, "alice")]
    src = _source(monkeypatch, fake)

    extra, record = await _record(src, _notif())

    assert extra["mention"] == {
        "state": "new", "at": _ts(30), "by": "carol", "url": f"{_WEB}/issues/7",
    }
    # The description is in the record already and is not quoted twice.
    assert "--- Mention" not in record.content
    assert record.content.count("Found by @alice in testing.") == 1


@pytest.mark.asyncio
async def test_a_mention_event_without_its_text_is_still_new(monkeypatch):
    # The text is gone (a deleted comment) or is one this look does not read.
    fake = _FakeGitHub()
    fake.events = [(31, "alice")]
    src = _source(monkeypatch, fake)

    extra, record = await _record(src, _notif())

    assert extra["mention"] == {"state": "new", "at": _ts(31)}
    assert record.metadata["mention"] == "new"
    assert "mention_by" not in record.metadata
    assert f"Mention: new ({_ts(31)})" in record.content


# ---------------------------------------------------------------------------
# Reviews and inline review comments: GitHub writes no mention event for them
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_a_mention_in_an_inline_review_comment_is_new(monkeypatch):
    fake = _FakeGitHub("PullRequest")
    fake.inline = [
        _inline(21, "bob", "Old remark.", -400),
        _inline(22, "bob", "@alice is this path still used?", 30),
    ]
    src = _source(monkeypatch, fake)

    extra, record = await _record(src, _notif("PullRequest"))

    assert extra["mention"]["state"] == "new"
    assert extra["mention"]["by"] == "bob"
    assert extra["mention"]["url"] == f"{_WEB}/pull/7#discussion_r22"
    assert record.metadata["mention_url"] == f"{_WEB}/pull/7#discussion_r22"
    assert "--- Mention (bob, " in record.content
    assert "@alice is this path still used?" in record.content


@pytest.mark.asyncio
async def test_a_mention_in_a_review_is_new(monkeypatch):
    fake = _FakeGitHub("PullRequest")
    fake.reviews = [
        _review("bob", "Looks fine.", -400),
        _review("bob", "One question for @alice below.", 30),
        {"author": {"login": "alice"}, "body": "draft", "submittedAt": None, "url": ""},
    ]
    src = _source(monkeypatch, fake)

    extra, _ = await _record(src, _notif("PullRequest"))

    assert extra["mention"]["state"] == "new"
    assert extra["mention"]["by"] == "bob"
    assert extra["mention"]["url"] == f"{_WEB}/pull/7#pullrequestreview-30"


@pytest.mark.asyncio
async def test_an_earlier_inline_mention_explains_the_reason(monkeypatch):
    fake = _FakeGitHub("PullRequest")
    fake.inline = [_inline(22, "bob", "@alice is this path still used?", -400)]
    fake.comments = [_comment(12, "bob", "Rebased.", 30)]
    src = _source(monkeypatch, fake)

    extra, record = await _record(src, _notif("PullRequest"))

    assert extra["mention"] == {"state": "old", "at": _ts(-400)}
    assert record.summary.endswith("(update, no new mention)")


@pytest.mark.asyncio
async def test_a_failed_read_of_inline_comments_gives_no_old(monkeypatch):
    # The events explain the reason, but a mention may sit in the inline
    # comments that could not be read.
    fake = _FakeGitHub("PullRequest")
    fake.events = [(-9_000_000, "alice")]
    fake.inline = None
    src = _source(monkeypatch, fake)

    extra, record = await _record(src, _notif("PullRequest"))

    assert "mention" not in extra
    assert record.summary.endswith("(mention)")


@pytest.mark.asyncio
async def test_a_failed_read_does_not_hide_a_mention_that_was_found(monkeypatch):
    fake = _FakeGitHub("PullRequest")
    fake.graphql_fails = True
    fake.inline = [_inline(22, "bob", "@alice is this path still used?", 30)]
    src = _source(monkeypatch, fake)

    extra, _ = await _record(src, _notif("PullRequest"))

    assert extra["mention"]["state"] == "new"


@pytest.mark.asyncio
async def test_more_inline_comments_than_one_page_since_the_start_give_no_old(monkeypatch):
    fake = _FakeGitHub("PullRequest")
    fake.events = [(-9_000_000, "alice")]
    fake.inline = [_inline(100 + i, "bob", "nit", 1 + i % 50) for i in range(120)]
    src = _source(monkeypatch, fake)

    extra, _ = await _record(src, _notif("PullRequest"))

    assert "mention" not in extra


# ---------------------------------------------------------------------------
# The mention events are read newest first
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_older_pages_of_mention_events_are_read_until_one_is_of_the_account(monkeypatch):
    fake = _FakeGitHub()
    fake.event_page_size = 2
    fake.events = [(-900, "alice"), (-800, "dave"), (-700, "dave"), (-600, "erin"), (-500, "dave")]
    src = _source(monkeypatch, fake)

    extra, _ = await _record(src, _notif())

    assert extra["mention"] == {"state": "old", "at": _ts(-900)}
    assert [c for c in fake.calls if c.startswith("graphql")] == [
        "graphql before=None", "graphql before=3", "graphql before=1",
    ]


@pytest.mark.asyncio
async def test_reading_stops_at_the_first_page_with_a_mention_of_the_account(monkeypatch):
    fake = _FakeGitHub()
    fake.event_page_size = 2
    fake.events = [(-900, "alice"), (-800, "dave"), (-700, "alice"), (-600, "dave")]
    src = _source(monkeypatch, fake)

    extra, _ = await _record(src, _notif())

    assert extra["mention"] == {"state": "old", "at": _ts(-700)}
    assert [c for c in fake.calls if c.startswith("graphql")] == ["graphql before=None"]


@pytest.mark.asyncio
async def test_a_mention_beyond_the_pages_read_is_not_known(monkeypatch):
    fake = _FakeGitHub()
    fake.event_page_size = 1
    fake.events = [(-900, "alice")] + [(-800 + i, "dave") for i in range(gh._MENTION_LOG_PAGES)]
    src = _source(monkeypatch, fake)

    extra, _ = await _record(src, _notif())

    assert "mention" not in extra
    assert len([c for c in fake.calls if c.startswith("graphql")]) == gh._MENTION_LOG_PAGES


@pytest.mark.asyncio
async def test_the_login_is_compared_in_any_letter_case(monkeypatch):
    fake = _FakeGitHub()
    fake.events = [(31, "Alice")]
    src = _source(monkeypatch, fake, login="ALICE")

    extra, _ = await _record(src, _notif())

    assert extra["mention"]["state"] == "new"


# ---------------------------------------------------------------------------
# From one update of a thread to the next
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_the_same_update_listed_again_gives_the_same_record(monkeypatch):
    # The cursor's own second is listed again in every run. A record that came
    # out differently would be stored again and shown to its readers twice.
    fake = _FakeGitHub()
    fake.comments = [_comment(11, "bob", "@alice can you look at this?", 30)]
    fake.events = [(32, "alice")]
    src = _source(monkeypatch, fake)

    _, first = await _record(src, _notif())
    # A later run has a later cursor, so a later first look.
    src._run = gh._Run(login=_ME, first_look=_T0 + timedelta(seconds=50))
    _, again = await _record(src, _notif())

    assert again == first
    assert again.metadata["mention"] == "new"


@pytest.mark.asyncio
async def test_a_mention_is_new_for_one_update_only(monkeypatch):
    fake = _FakeGitHub()
    fake.comments = [_comment(11, "bob", "@alice can you look at this?", 30)]
    fake.events = [(32, "alice")]
    src = _source(monkeypatch, fake)

    _, first = await _record(src, _notif(updated_at=52))
    assert first.metadata["mention"] == "new"

    fake.comments.append(_comment(12, "bob", "Never mind, found it.", 300))
    fake.calls.clear()
    _, second = await _record(src, _notif(updated_at=320))

    assert second.metadata["mention"] == "old"
    assert second.metadata["mention_at"] == _ts(32)
    assert second.summary.endswith("(update, no new mention)")
    # The second look starts where the first ended: at the thread's update,
    # the newest moment the first look covered.
    assert f"{_API}/issues/7/comments?since={_ts(52)}&per_page=100" in fake.calls
    assert "Never mind, found it." in second.content
    assert "@alice can you look at this?" not in second.content


@pytest.mark.asyncio
async def test_a_look_that_failed_is_repeated_from_its_start(monkeypatch):
    fake = _FakeGitHub()
    fake.comments = [_comment(11, "bob", "@alice can you look at this?", 30)]
    fake.events = [(32, "alice")]
    fake.graphql_fails = True
    src = _source(monkeypatch, fake)

    _, first = await _record(src, _notif(updated_at=52))
    assert "mention" not in first.metadata

    # The next update: the read works again, and the look covers the time the
    # failed one could not, so the mention is not lost.
    fake.graphql_fails = False
    fake.comments.append(_comment(12, "bob", "Any news?", 300))
    _, second = await _record(src, _notif(updated_at=320))

    assert second.metadata["mention"] == "new"
    assert second.metadata["mention_by"] == "bob"


@pytest.mark.asyncio
async def test_a_thread_never_looked_at_starts_before_the_cursor(monkeypatch):
    fake = _FakeGitHub()
    src = _source(monkeypatch, fake)
    src._run = gh._Run(login=_ME, first_look=_T0 - gh._FIRST_LOOK_MARGIN)

    await _record(src, _notif())

    assert f"{_API}/issues/7/comments?since={_ts(-120)}&per_page=100" in fake.calls


@pytest.mark.asyncio
async def test_only_the_latest_looks_are_remembered(monkeypatch):
    monkeypatch.setattr(gh, "_REMEMBERED_THREADS", 2)
    fake = _FakeGitHub()
    src = _source(monkeypatch, fake)

    for thread_id in ("t1", "t2", "t3"):
        await _record(src, _notif(thread_id=thread_id))

    assert list(src._looks) == ["t2", "t3"]


# ---------------------------------------------------------------------------
# The comments since the previous update
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_new_comments_are_listed_by_time_and_shown_newest_first(monkeypatch):
    fake = _FakeGitHub()
    fake.comments = [_comment(1, "old-timer", "First!", -9_000)]
    fake.comments += [_comment(10 + i, f"user{i}", f"comment {i}", 10 + i) for i in range(7)]
    fake.comments.append(_comment(30, "some-ci[bot]", "Build finished.", 40))
    fake.comments.append(_comment(31, "deploy-bot", "Deployed.", 41))
    src = _source(monkeypatch, fake)

    extra, record = await _record(src, _notif(reason="assign"))

    # The listing of one thread's comments takes no sort order: it is asked
    # for the comments since the previous update instead.
    listing = [c for c in fake.calls if "/issues/7/comments" in c]
    assert listing == [f"{_API}/issues/7/comments?since={_ts(0)}&per_page=100"]
    assert [c["user"] for c in extra["recent_comments"]] == [
        "user6", "user5", "user4", "user3", "user2",
    ]
    assert "First!" not in record.content
    assert "Build finished." not in record.content
    assert "--- New Comments (human, newest first) ---" in record.content


@pytest.mark.asyncio
async def test_a_long_listing_of_comments_is_read_page_by_page(monkeypatch):
    fake = _FakeGitHub()
    fake.comments = [_comment(100 + i, "bob", f"comment {i}", 1 + i) for i in range(150)]
    src = _source(monkeypatch, fake)

    extra, _ = await _record(src, _notif(reason="assign", updated_at=400))

    listing = [c for c in fake.calls if "/issues/7/comments" in c]
    assert listing == [
        f"{_API}/issues/7/comments?since={_ts(0)}&per_page=100",
        f"{_API}/issues/7/comments?since={_ts(0)}&per_page=100&page=2",
    ]
    assert extra["recent_comments"][0]["body"] == "comment 149"


@pytest.mark.asyncio
async def test_a_listing_longer_than_the_pages_followed_is_not_used(monkeypatch):
    fake = _FakeGitHub()
    fake.comments = [
        _comment(1000 + i, "bob", f"comment {i}", 1) for i in range(100 * gh._COMMENT_PAGES)
    ]
    fake.events = [(-9_000_000, "alice")]
    src = _source(monkeypatch, fake)

    extra, record = await _record(src, _notif())

    assert "recent_comments" not in extra
    # The mention answer needs no comments; the look is not kept as complete,
    # so the next update's look repeats the range.
    assert record.metadata["mention"] == "old"
    assert src._looks["t1"].end == _START


@pytest.mark.asyncio
async def test_the_latest_comment_is_not_repeated_among_the_new_comments(monkeypatch):
    fake = _FakeGitHub()
    fake.comments = [
        _comment(11, "bob", "First reply.", 20),
        _comment(12, "erin", "Second reply.", 30),
    ]
    src = _source(monkeypatch, fake)
    notif = _notif(reason="assign")
    notif["subject"]["latest_comment_url"] = f"{_API}/issues/comments/12"
    real_get = fake.get

    async def get(url, timeout=30):
        if url.endswith("/issues/comments/12"):
            return fake.comments[1]
        return await real_get(url, timeout)

    monkeypatch.setattr(src, "_gh_api_get", get)

    extra, record = await _record(src, notif)

    assert [c["user"] for c in extra["recent_comments"]] == ["bob"]
    assert record.content.count("Second reply.") == 1


# ---------------------------------------------------------------------------
# The login is read in every run that needs it
# ---------------------------------------------------------------------------

class _FakeProc:
    def __init__(self, stdout: bytes, returncode: int = 0):
        self._stdout = stdout
        self.returncode = returncode

    async def communicate(self):
        return self._stdout, b""


def _list_with(monkeypatch, notifications: list[dict]) -> None:
    """Make ``gh api notifications?...`` print *notifications*."""
    async def fake_exec(*argv, **kwargs):
        assert argv[:2] == ("gh", "api") and argv[2].startswith("notifications?")
        return _FakeProc("\n".join(json.dumps(n) for n in notifications).encode())

    monkeypatch.setattr(gh.asyncio, "create_subprocess_exec", fake_exec)


@pytest.mark.asyncio
async def test_the_login_is_read_in_each_run_with_a_mention_thread(monkeypatch):
    fake = _FakeGitHub()
    fake.events = [(-9_000_000, "alice"), (-8_000_000, "zoe")]
    src = _source(monkeypatch, fake)
    _list_with(monkeypatch, [_notif()])

    first = await src.fetch(_ts(0))
    assert fake.calls.count("user") == 1
    assert first.records[0].metadata["mention"] == "old"
    assert first.records[0].metadata["mention_at"] == _ts(-9_000_000)

    # gh acts as another account from now on. The next run reads the login
    # again and looks for that account's mentions.
    fake.login = "zoe"
    second = await src.fetch(_ts(0))
    assert fake.calls.count("user") == 2
    assert second.records[0].metadata["mention_at"] == _ts(-8_000_000)


@pytest.mark.asyncio
async def test_a_run_without_a_mention_thread_reads_no_login(monkeypatch):
    fake = _FakeGitHub()
    src = _source(monkeypatch, fake)
    unreadable = _notif(thread_id="t2")
    unreadable["subject"]["url"] = None   # nothing to read mentions from
    _list_with(monkeypatch, [_notif(reason="author"), unreadable])

    await src.fetch(_ts(0))

    assert "user" not in fake.calls


@pytest.mark.asyncio
async def test_a_login_that_cannot_be_read_leaves_the_records_as_they_are(monkeypatch):
    fake = _FakeGitHub()
    fake.events = [(-9_000_000, "alice")]
    fake.login = None
    src = _source(monkeypatch, fake)
    _list_with(monkeypatch, [_notif()])

    result = await src.fetch(_ts(0))

    assert result.records[0].summary.endswith("(mention)")
    assert "mention" not in result.records[0].metadata


@pytest.mark.asyncio
async def test_another_account_forgets_the_looks(monkeypatch):
    fake = _FakeGitHub()
    src = _source(monkeypatch, fake)
    _list_with(monkeypatch, [_notif()])

    await src.fetch(_ts(0))
    assert list(src._looks) == ["t1"]

    fake.login = "zoe"
    _list_with(monkeypatch, [_notif(thread_id="t9")])
    await src.fetch(_ts(0))

    assert list(src._looks) == ["t9"]


@pytest.mark.asyncio
async def test_a_run_starts_its_first_looks_shortly_before_the_cursor(monkeypatch):
    fake = _FakeGitHub()
    src = _source(monkeypatch, fake)
    _list_with(monkeypatch, [_notif()])

    await src.fetch(_ts(0))

    assert f"{_API}/issues/7/comments?since={_ts(-120)}&per_page=100" in fake.calls


# ---------------------------------------------------------------------------
# gh api graphql
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_graphql_sends_strings_raw_and_integers_typed(monkeypatch):
    seen: list[tuple] = []

    async def fake_exec(*argv, **kwargs):
        seen.append(argv)
        return _FakeProc(json.dumps({"data": {"repository": None}}).encode())

    monkeypatch.setattr(gh.asyncio, "create_subprocess_exec", fake_exec)

    data = await GitHubSource._gh_graphql(
        "query { viewer { login } }", {"owner": "@acme", "number": 7},
    )

    assert data == {"repository": None}
    assert seen == [(
        "gh", "api", "graphql", "-f", "query=query { viewer { login } }",
        "-f", "owner=@acme", "-F", "number=7",
    )]


@pytest.mark.asyncio
@pytest.mark.parametrize("stdout,returncode", [
    (json.dumps({"data": {"repository": None}, "errors": [{"type": "NOT_FOUND"}]}), 0),
    (json.dumps({"data": {"repository": None}}), 1),
    ("not json", 0),
    (json.dumps([1, 2]), 0),
    (json.dumps({"data": None}), 0),
])
async def test_graphql_failures_return_none(monkeypatch, stdout, returncode):
    async def fake_exec(*argv, **kwargs):
        return _FakeProc(stdout.encode(), returncode)

    monkeypatch.setattr(gh.asyncio, "create_subprocess_exec", fake_exec)

    assert await GitHubSource._gh_graphql("query { viewer { login } }", {}) is None


# ---------------------------------------------------------------------------
# The record
# ---------------------------------------------------------------------------

def test_a_record_without_a_mention_answer_has_no_mention_keys():
    record = notification_record(_notif(), {"html_url": f"{_WEB}/issues/7"})

    assert record.summary == "[acme/widgets] Widget breaks (mention)"
    assert sorted(record.metadata) == [
        "actors", "ci_branch", "reason", "repo_name", "repo_url", "subject_type", "subject_url",
    ]


def test_the_author_of_an_old_mention_is_not_an_actor():
    record = notification_record(_notif(), {
        "user": "carol",
        "mention": {"state": "old", "at": _ts(-400), "by": "bob"},
    })

    assert record.metadata["actors"] == ["carol"]


# ---------------------------------------------------------------------------
# GitHub events source: the username is looked up in every run
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_events_source_looks_the_username_up_again(monkeypatch):
    answers = [(b"alice\n", 0), (b"zoe\n", 0), (b"", 1)]

    async def fake_exec(*argv, **kwargs):
        assert argv == ("gh", "api", "user", "--jq", ".login")
        return _FakeProc(*answers.pop(0))

    monkeypatch.setattr("nerve.sources.github_events.asyncio.create_subprocess_exec", fake_exec)
    src = GitHubEventsSource()

    assert await src._resolve_username() == "alice"
    assert await src._resolve_username() == "zoe"
    # A failed lookup falls back to the login the last one returned.
    assert await src._resolve_username() == "zoe"


@pytest.mark.asyncio
async def test_events_source_fails_without_any_username(monkeypatch):
    async def fake_exec(*argv, **kwargs):
        return _FakeProc(b"", 1)

    monkeypatch.setattr("nerve.sources.github_events.asyncio.create_subprocess_exec", fake_exec)

    with pytest.raises(RuntimeError):
        await GitHubEventsSource()._resolve_username()


@pytest.mark.asyncio
async def test_events_source_keeps_a_configured_username(monkeypatch):
    async def fake_exec(*argv, **kwargs):
        raise AssertionError("no lookup for a configured username")

    monkeypatch.setattr("nerve.sources.github_events.asyncio.create_subprocess_exec", fake_exec)

    assert await GitHubEventsSource({"username": "alice"})._resolve_username() == "alice"
