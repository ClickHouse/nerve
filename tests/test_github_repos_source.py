"""The GitHub Repos source in poll mode: what is new in repositories the agent follows.

A fake ``gh api`` serves the pulls, issues and releases listings of a few
repositories, newest change first as GitHub does; each test lists once for the
baseline, changes the repositories, and lists again. The records are pinned to
the shape the channel gateway builds for the hosted mode (same id, type,
summary, content, timestamp and metadata), so that the guardrails, the gates
and the agent see the same thing in both modes.
"""

from __future__ import annotations

import copy
import json
import logging
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from nerve.channels.hosted.contract import MetadataEntry
from nerve.channels.hosted.sources import buffered_record
from nerve.config import GitHubReposSyncConfig, NerveConfig
from nerve.sources import github_repos as module
from nerve.sources.github_hosted import record_from_buffer
from nerve.sources.github_repos import GitHubReposSource, github_repos_inbox_filter
from nerve.sources.models import SourceRecord

REPO = "acme/widgets"
OTHER = "other-org/other-repo"
START = datetime(2026, 10, 27, 10, 0, tzinfo=UTC)
SHA_A = "a" * 40
SHA_B = "b" * 40


def at(minutes: int) -> str:
    """A GitHub timestamp *minutes* after the start of the test day."""
    return (START + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")


def user(login: str = "alice", kind: str = "User") -> dict[str, Any]:
    return {"login": login, "type": kind}


def pull(
    number: int, *, created: int = 0, updated: int | None = None, author: str = "alice",
    kind: str = "User", draft: bool = False, state: str = "open", merged: int | None = None,
    sha: str = SHA_A, title: str | None = None, body: str = "", labels: tuple[str, ...] = (),
    repo: str = REPO,
) -> dict[str, Any]:
    """A pull request as the pulls listing returns it."""
    return {
        "number": number,
        "title": title or f"Pull request {number}",
        "user": user(author, kind),
        "state": state,
        "draft": draft,
        "merged_at": at(merged) if merged is not None else None,
        "closed_at": at(merged if merged is not None else updated or created) if state == "closed" else None,
        "created_at": at(created),
        "updated_at": at(created if updated is None else updated),
        "head": {"sha": sha, "ref": "feature", "label": f"{author}:feature"},
        "base": {"ref": "main"},
        "labels": [{"name": name} for name in labels],
        "body": body,
        "html_url": f"https://github.com/{repo}/pull/{number}",
    }


def issue(
    number: int, *, created: int = 0, updated: int | None = None, author: str = "alice",
    kind: str = "User", state: str = "open", title: str | None = None, body: str = "",
    labels: tuple[str, ...] = (), repo: str = REPO,
) -> dict[str, Any]:
    """An issue as the issues listing returns it."""
    return {
        "number": number,
        "title": title or f"Issue {number}",
        "user": user(author, kind),
        "state": state,
        "created_at": at(created),
        "updated_at": at(created if updated is None else updated),
        "labels": [{"name": name} for name in labels],
        "body": body,
        "html_url": f"https://github.com/{repo}/issues/{number}",
    }


def as_issue(pr: dict[str, Any]) -> dict[str, Any]:
    """A pull request as the issues listing returns it: with a pull_request key."""
    item = {k: pr[k] for k in ("number", "title", "user", "state", "created_at", "updated_at", "body")}
    item["pull_request"] = {"url": "https://api.github.com/repos/acme/widgets/pulls/1"}
    return item


def release(
    release_id: int, *, tag: str, name: str = "", created: int = 0, published: int | None = 0,
    author: str = "alice", kind: str = "User", draft: bool = False, prerelease: bool = False,
    body: str = "",
) -> dict[str, Any]:
    """A release as the releases listing returns it; a draft has no published_at."""
    return {
        "id": release_id,
        "tag_name": tag,
        "name": name,
        "author": user(author, kind),
        "draft": draft,
        "prerelease": prerelease,
        "created_at": at(created),
        "published_at": None if draft or published is None else at(published),
        "body": body,
        "html_url": f"https://github.com/{REPO}/releases/tag/{tag}",
    }


class FakeGitHub:
    """``gh api`` over a few repositories' listings; records every endpoint asked for."""

    _ENDPOINT = re.compile(r"repos/([^/]+/[^/?]+)/(pulls|issues|releases)\?(.*)")

    def __init__(self) -> None:
        self.pulls: dict[str, list[dict]] = {}
        self.issues: dict[str, list[dict]] = {}
        self.releases: dict[str, list[dict]] = {}
        self.calls: list[str] = []
        self.failing: set[str] = set()

    async def gh_api_get(self, endpoint: str, timeout: float = 30) -> Any:
        self.calls.append(endpoint)
        repo, listing, _ = self._ENDPOINT.fullmatch(endpoint).groups()
        if f"{repo} {listing}" in self.failing:
            raise RuntimeError("gh: Not Found (HTTP 404)")
        items = list(getattr(self, listing).get(repo, []))
        if listing == "issues":
            items += [as_issue(pr) for pr in self.pulls.get(repo, [])]
        key = "created_at" if listing == "releases" else "updated_at"
        return copy.deepcopy(sorted(items, key=lambda item: item[key], reverse=True))


# When the tests read the listings, unless a test moves the clock.
NOW = START + timedelta(hours=1)
STAMP = NOW.strftime("%Y-%m-%dT%H:%M:%SZ")


def source(fake: FakeGitHub, *, clock: Any = None, **settings: Any) -> GitHubReposSource:
    """A source over *fake*, reading at *clock* (``NOW``); settings as in sync.github_repos."""
    settings.setdefault("repos", [REPO])
    src = GitHubReposSource(config=settings, clock=clock or (lambda: NOW))
    src._gh_api_get = fake.gh_api_get
    return src


def read(since: str, *at: str, listed_at: str = STAMP) -> dict[str, Any]:
    """A listing of the cursor: its newest time, when it was read, and the
    keys of the items created in that newest second."""
    return {"since": since, "listed_at": listed_at, "at": list(at)}


async def baseline(src: GitHubReposSource) -> str:
    result = await src.fetch(None)
    assert result.records == []
    return result.next_cursor


def cursor_of(result) -> dict[str, Any]:
    return json.loads(result.next_cursor)


# ---------------------------------------------------------------------- #
#  Listings, baseline and cursor                                          #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
@pytest.mark.parametrize(("types", "listings"), [
    (None, ["pulls", "issues"]),
    (["pull_request"], ["pulls"]),
    (["issue"], ["issues"]),
    (["release"], ["releases"]),
    (["release", "issue", "pull_request"], ["pulls", "issues", "releases"]),
])
async def test_each_type_is_one_listing_of_each_repository(types, listings):
    fake = FakeGitHub()
    settings = {"repos": [REPO, OTHER]} if types is None else {"repos": [REPO, OTHER], "types": types}

    await source(fake, **settings).fetch(None)

    query = {
        "pulls": "state=all&sort=updated&direction=desc&per_page=100",
        "issues": "state=all&sort=updated&direction=desc&per_page=100",
        "releases": "per_page=30",
    }
    assert sorted(fake.calls) == sorted(
        f"repos/{repo}/{listing}?{query[listing]}" for repo in (REPO, OTHER) for listing in listings
    )


@pytest.mark.asyncio
async def test_the_listings_and_the_bot_filter():
    """New pull requests, issues and releases arrive from their listings. A
    pull request or an issue that a GitHub App or a [bot] account opened does
    only with include_bots; a release arrives whoever published it, release
    automation being the normal case."""
    fake = FakeGitHub()
    everything = ["pull_request", "issue", "release"]

    async def reported(**settings: Any) -> dict[str, str]:
        fake.pulls[REPO], fake.issues[REPO], fake.releases[REPO] = [], [], []
        src = source(fake, types=everything, **settings)
        cursor = await baseline(src)
        fake.pulls[REPO] = [
            pull(1, created=1, author="alice"),
            pull(2, created=2, author="widget-builder", kind="Bot"),
            pull(3, created=3, author="updater[bot]"),
        ]
        fake.issues[REPO] = [issue(4, created=4, author="bob"), issue(5, created=5, author="triage[bot]")]
        fake.releases[REPO] = [
            release(900, tag="v2.0.0", created=6, published=6, author="github-actions[bot]", kind="Bot"),
            release(901, tag="v2.0.1", created=7, published=7, author="carol"),
        ]
        result = await src.fetch(cursor)
        return {record.id: record.metadata["author_kind"] for record in result.records}

    assert await reported() == {
        f"{REPO}#1:opened:open": "human",
        f"{REPO}#4:opened:open": "human",
        f"{REPO}#:released:900": "bot",
        f"{REPO}#:released:901": "human",
    }
    assert await reported(include_bots=True) == {
        f"{REPO}#1:opened:open": "human",
        f"{REPO}#2:opened:open": "bot",
        f"{REPO}#3:opened:open": "bot",
        f"{REPO}#4:opened:open": "human",
        f"{REPO}#5:opened:open": "bot",
        f"{REPO}#:released:900": "bot",
        f"{REPO}#:released:901": "human",
    }


@pytest.mark.asyncio
async def test_the_first_listing_is_a_baseline_without_records():
    fake = FakeGitHub()
    fake.pulls[REPO] = [pull(1, created=-30, updated=-5), pull(2, created=-20)]
    fake.issues[REPO] = [issue(3, created=-40, updated=-10)]
    fake.releases[REPO] = [
        release(900, tag="v1.0.0", created=-60, published=-50),
        release(901, tag="v1.1.0-draft", created=-1, draft=True),
    ]
    src = source(fake, repos=[REPO, OTHER], types=["pull_request", "issue", "release"])

    result = await src.fetch(None)

    assert result.records == []
    assert cursor_of(result) == {"v": 1, "repos": {
        # The issues listing holds the pull requests too; drafts are not published.
        REPO: {"pulls": read(at(-5)), "issues": read(at(-5)), "releases": read(at(-50), "900")},
        # Nothing listed: everything listed later is new.
        OTHER: {"pulls": read(""), "issues": read(""), "releases": read("")},
    }}


@pytest.mark.asyncio
async def test_what_existed_before_is_never_opened_and_only_new_items_are():
    fake = FakeGitHub()
    fake.pulls[REPO] = [pull(1, created=-30)]
    fake.issues[REPO] = [issue(2, created=-30)]
    fake.pulls[OTHER] = []
    src = source(fake, repos=[REPO, OTHER])
    cursor = await baseline(src)
    # Old items move (a comment, a push, a close) and new ones appear.
    fake.pulls[REPO] = [pull(1, created=-30, updated=10, sha=SHA_B), pull(5, created=12)]
    fake.issues[REPO] = [issue(2, created=-30, updated=11, state="closed"), issue(6, created=13)]
    fake.pulls[OTHER] = [pull(1, created=14, repo=OTHER)]

    result = await src.fetch(cursor)

    assert [record.id for record in result.records] == [
        f"{REPO}#5:opened:open", f"{REPO}#6:opened:open", f"{OTHER}#1:opened:open",
    ]
    again = await src.fetch(result.next_cursor)
    assert again.records == []
    assert again.next_cursor == result.next_cursor


@pytest.mark.asyncio
async def test_a_pull_request_created_in_the_cursors_second_after_the_listing_is_reported_once():
    """GitHub's times have a resolution of one second. A pull request created
    in the cursor's own second that the listing did not show yet (created a
    moment later, or a replica behind) is new. The one the listing showed is
    not, also when it changes and is listed again: the inbox would take it as
    a changed record and show it a second time."""
    fake = FakeGitHub()
    fake.pulls[REPO] = [pull(1, created=5)]
    src = source(fake, types=["pull_request"])
    cursor = await baseline(src)
    assert json.loads(cursor)["repos"][REPO]["pulls"] == read(at(5), "1")
    fake.pulls[REPO] = [pull(1, created=5, updated=8, labels=("triage",)), pull(2, created=5)]

    result = await src.fetch(cursor)

    assert [record.id for record in result.records] == [f"{REPO}#2:opened:open"]
    assert (await src.fetch(result.next_cursor)).records == []


@pytest.mark.asyncio
async def test_an_issue_created_in_the_cursors_second_after_the_listing_is_reported_once():
    fake = FakeGitHub()
    fake.issues[REPO] = [issue(1, created=5)]
    src = source(fake, types=["issue"])
    cursor = await baseline(src)
    fake.issues[REPO] = [issue(1, created=5, updated=9), issue(2, created=5)]

    result = await src.fetch(cursor)

    assert [record.id for record in result.records] == [f"{REPO}#2:opened:open"]
    assert (await src.fetch(result.next_cursor)).records == []


@pytest.mark.asyncio
async def test_a_release_published_in_the_cursors_second_after_the_listing_is_reported_once():
    fake = FakeGitHub()
    fake.releases[REPO] = [release(900, tag="v1.0.0", created=4, published=5)]
    src = source(fake, types=["release"])
    cursor = await baseline(src)
    fake.releases[REPO].append(release(901, tag="v1.0.1", created=5, published=5))

    result = await src.fetch(cursor)

    assert [record.id for record in result.records] == [f"{REPO}#:released:901"]
    assert cursor_of(result)["repos"][REPO]["releases"] == read(at(5), "900", "901")
    assert (await src.fetch(result.next_cursor)).records == []


# Minutes in a day, for times given as minutes after START (NOW is at(60)).
DAY = 24 * 60


@pytest.mark.asyncio
async def test_a_check_a_day_and_a_second_after_the_last_reports_the_last_day():
    """A daily schedule comes back a day and a moment after the last check.
    Nothing older than a day is backfilled, but the last day is: what was
    opened since the last check is reported; an older item that only
    changed is not."""
    fake = FakeGitHub()
    fake.issues[REPO] = [issue(1, created=-600)]
    now = [NOW]
    src = source(fake, types=["pull_request", "issue"], clock=lambda: now[0])
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(2, created=120)]
    fake.issues[REPO] = [issue(1, created=-600, updated=1000), issue(3, created=1260)]
    now[0] = NOW + timedelta(hours=24, seconds=1)

    result = await src.fetch(cursor)

    assert [record.id for record in result.records] == [f"{REPO}#2:opened:open", f"{REPO}#3:opened:open"]
    assert (await src.fetch(result.next_cursor)).records == []


@pytest.mark.asyncio
async def test_a_check_a_day_and_five_seconds_after_the_last_keeps_an_item_of_the_moment_after_it():
    """The window reaches an hour further back than the day, for the
    scheduler's delay: a pull request created 2 s after the last check, which
    that check did not list, is still reported; the one it listed is not."""
    fake = FakeGitHub()
    fake.pulls[REPO] = [pull(1, created=-600)]
    now = [NOW]
    src = source(fake, types=["pull_request"], clock=lambda: now[0])
    cursor = await baseline(src)
    moment_after = pull(2, created=60)
    moment_after["created_at"] = moment_after["updated_at"] = (
        NOW + timedelta(seconds=2)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    fake.pulls[REPO] = [pull(1, created=-600), moment_after]
    now[0] = NOW + timedelta(hours=24, seconds=5)

    result = await src.fetch(cursor)

    assert [record.id for record in result.records] == [f"{REPO}#2:opened:open"]


@pytest.mark.asyncio
async def test_a_check_a_week_after_the_last_reports_only_the_last_day():
    fake = FakeGitHub()
    now = [NOW]
    src = source(fake, types=["pull_request", "issue", "release"], clock=lambda: now[0])
    cursor = await baseline(src)
    fake.pulls[REPO] = [
        pull(1, created=60 + DAY),
        pull(2, created=60 + 6 * DAY + 720),
        pull(4, created=60 + 6 * DAY - 30),
        pull(5, created=60 + 6 * DAY - 120),
    ]
    fake.issues[REPO] = [issue(3, created=60 + 6 * DAY + 1380)]
    fake.releases[REPO] = [
        release(900, tag="v1", created=60 + 3 * DAY, published=60 + 3 * DAY),
        release(901, tag="v2", created=60 + 6 * DAY + 1200, published=60 + 6 * DAY + 1200),
    ]
    now[0] = NOW + timedelta(days=7)

    result = await src.fetch(cursor)

    # Six days, four days and 26 h old: not reported. 24.5 h old (within the
    # hour of grace), 12, 4 and 1 h old: reported.
    assert [record.id for record in result.records] == [
        f"{REPO}#4:opened:open", f"{REPO}#2:opened:open", f"{REPO}#:released:901", f"{REPO}#3:opened:open",
    ]


@pytest.mark.asyncio
async def test_after_a_gap_the_followed_pull_requests_stay_followed():
    """updates with a daily schedule: a followed pull request that changed in
    the last day is reported after the gap, as it is without one."""
    fake = FakeGitHub()
    now = [NOW]
    src = source(fake, types=["pull_request"], updates=True, clock=lambda: now[0])
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=90)]
    now[0] = NOW + timedelta(hours=1)
    opened = await src.fetch(cursor)
    assert [record.id for record in opened.records] == [f"{REPO}#1:opened:open"]
    fake.pulls[REPO] = [pull(1, created=90, updated=60 + 2 * DAY, sha=SHA_B)]
    now[0] = NOW + timedelta(days=2, hours=1)

    result = await src.fetch(opened.next_cursor)

    assert [record.id for record in result.records] == [f"{REPO}#1:updated:{SHA_B}"]


@pytest.mark.asyncio
async def test_a_listing_whose_reading_time_is_unknown_is_read_against_the_last_day():
    fake = FakeGitHub()
    fake.pulls[REPO] = [pull(1, created=-3000), pull(2, created=5)]
    cursor = json.dumps({"v": 1, "repos": {REPO: {"pulls": {"since": at(-4000), "at": []}}}})

    result = await source(fake, types=["pull_request"]).fetch(cursor)

    assert [record.id for record in result.records] == [f"{REPO}#2:opened:open"]


@pytest.mark.asyncio
async def test_a_stale_listing_whose_cursor_falls_within_the_day_keeps_its_keys():
    """Its cursor second is still the window's: the item it saw there is not
    reported again, the one it did not see is."""
    fake = FakeGitHub()
    fake.pulls[REPO] = [pull(1, created=5), pull(2, created=5)]
    cursor = json.dumps({"v": 1, "repos": {REPO: {"pulls": {"since": at(5), "at": ["1"]}}}})

    result = await source(fake, types=["pull_request"]).fetch(cursor)

    assert [record.id for record in result.records] == [f"{REPO}#2:opened:open"]


@pytest.mark.asyncio
async def test_a_listing_read_within_a_day_picks_up_where_it_left():
    fake = FakeGitHub()
    now = [NOW]
    src = source(fake, types=["pull_request"], clock=lambda: now[0])
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1400)]
    now[0] = NOW + timedelta(hours=23, minutes=59)

    result = await src.fetch(cursor)

    assert [record.id for record in result.records] == [f"{REPO}#1:opened:open"]


@pytest.mark.asyncio
async def test_records_come_oldest_first_across_repositories():
    fake = FakeGitHub()
    src = source(fake, repos=[REPO, OTHER], types=["pull_request"])
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=30), pull(2, created=10)]
    fake.pulls[OTHER] = [pull(1, created=20, repo=OTHER)]

    result = await src.fetch(cursor)

    assert [record.timestamp for record in result.records] == [at(10), at(20), at(30)]


# ---------------------------------------------------------------------- #
#  Records: the gateway's shape                                           #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_new_pull_request_is_built_as_the_gateway_builds_it():
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"])
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(
        12, created=5, title="Add a widget cache", body="Caches widgets between requests.",
        labels=("enhancement", "cache"), sha=SHA_A,
    )]

    [record] = (await src.fetch(cursor)).records

    assert record.id == f"{REPO}#12:opened:open"
    assert (record.source, record.record_type) == ("github_repos", "github_repo_activity")
    assert record.summary == "[acme/widgets] New PR #12: Add a widget cache"
    assert record.content == (
        "Repository: acme/widgets\n"
        "Type: PullRequest\n"
        "Event: opened\n"
        "PR: #12 Add a widget cache\n"
        "Author: alice\n"
        "State: open\n"
        "Draft: false\n"
        "Base ← head: main ← alice:feature\n"
        "Labels: enhancement, cache\n"
        "Created: 2026-10-27T10:05:00Z\n"
        "Updated: 2026-10-27T10:05:00Z\n"
        "URL: https://github.com/acme/widgets/pull/12\n"
        "\n"
        "--- Description ---\n"
        "Caches widgets between requests."
    )
    assert record.timestamp == at(5)
    assert list(record.metadata.items()) == [
        ("repo_name", REPO), ("number", "12"), ("event", "opened"), ("subject_type", "PullRequest"),
        ("author", "alice"), ("author_kind", "human"), ("head_sha", SHA_A),
        ("subject_url", "https://github.com/acme/widgets/pull/12"),
        ("repo_url", "https://github.com/acme/widgets"),
    ]


@pytest.mark.asyncio
async def test_the_base_and_head_line_needs_both_as_the_gateway_prints_it():
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"])
    cursor = await baseline(src)
    headless = pull(12, created=5)
    headless["head"] = {"sha": SHA_A, "ref": "", "label": ""}
    fake.pulls[REPO] = [headless]

    [record] = (await src.fetch(cursor)).records

    assert "Base ← head:" not in record.content
    assert "Draft: false\nCreated: " in record.content


@pytest.mark.asyncio
async def test_a_new_issue_is_built_as_the_gateway_builds_it():
    fake = FakeGitHub()
    src = source(fake, types=["issue"])
    cursor = await baseline(src)
    fake.issues[REPO] = [issue(7, created=3, updated=4, title="Widgets leak", author="bob", labels=("bug",))]

    [record] = (await src.fetch(cursor)).records

    assert record.id == f"{REPO}#7:opened:open"
    assert record.summary == "[acme/widgets] New Issue #7: Widgets leak"
    assert record.content == (
        "Repository: acme/widgets\n"
        "Type: Issue\n"
        "Event: opened\n"
        "Issue: #7 Widgets leak\n"
        "Author: bob\n"
        "State: open\n"
        "Labels: bug\n"
        "Created: 2026-10-27T10:03:00Z\n"
        "Updated: 2026-10-27T10:04:00Z\n"
        "URL: https://github.com/acme/widgets/issues/7"
    )
    assert record.timestamp == at(3)
    assert record.metadata == {
        "repo_name": REPO, "number": "7", "event": "opened", "subject_type": "Issue",
        "author": "bob", "author_kind": "human", "head_sha": "",
        "subject_url": "https://github.com/acme/widgets/issues/7",
        "repo_url": "https://github.com/acme/widgets",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(("name", "summary", "line"), [
    ("Widgets 1.2", "[acme/widgets] New release v1.2.0: Widgets 1.2", "Release: v1.2.0 Widgets 1.2"),
    ("", "[acme/widgets] New release v1.2.0", "Release: v1.2.0"),
    ("v1.2.0", "[acme/widgets] New release v1.2.0", "Release: v1.2.0"),
])
async def test_a_release_is_built_as_the_gateway_builds_it(name, summary, line):
    fake = FakeGitHub()
    src = source(fake, types=["release"])
    cursor = await baseline(src)
    fake.releases[REPO] = [
        release(123456789, tag="v1.2.0", name=name, created=2, published=8, prerelease=True, body="Faster widgets."),
        release(123456790, tag="v1.3.0", created=9, draft=True),
    ]

    [record] = (await src.fetch(cursor)).records

    assert record.id == f"{REPO}#:released:123456789"
    assert record.summary == summary
    assert record.content == (
        "Repository: acme/widgets\n"
        "Type: Release\n"
        "Event: released\n"
        f"{line}\n"
        "Author: alice\n"
        "Prerelease: true\n"
        "Created: 2026-10-27T10:02:00Z\n"
        "Published: 2026-10-27T10:08:00Z\n"
        "URL: https://github.com/acme/widgets/releases/tag/v1.2.0\n"
        "\n"
        "--- Description ---\n"
        "Faster widgets."
    )
    assert record.timestamp == at(8)
    assert record.metadata == {
        "repo_name": REPO, "number": "", "event": "released", "subject_type": "Release",
        "author": "alice", "author_kind": "human", "head_sha": "",
        "subject_url": "https://github.com/acme/widgets/releases/tag/v1.2.0",
        "repo_url": "https://github.com/acme/widgets",
    }


@pytest.mark.asyncio
async def test_a_long_body_is_capped_and_an_empty_one_leaves_out_the_description():
    fake = FakeGitHub()
    src = source(fake, types=["issue"])
    cursor = await baseline(src)
    fake.issues[REPO] = [issue(1, created=1, body="x" * 5000), issue(2, created=2, body="")]

    long, empty = (await src.fetch(cursor)).records

    assert long.content.endswith("\n\n--- Description ---\n" + "x" * 4000 + "\n[... truncated]")
    assert "--- Description ---" not in empty.content


@pytest.mark.asyncio
async def test_a_record_reads_the_same_after_the_trip_through_the_gateway_buffer():
    """The metadata as the gateway sends it (strings, empty ones included),
    buffered and drained by the hosted source: the same record as here."""
    fake = FakeGitHub()
    src = source(fake, types=["pull_request", "release"])
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(12, created=5)]
    fake.releases[REPO] = [release(55, tag="v1", created=6, published=6)]

    for record in (await src.fetch(cursor)).records:
        event = _source_record_event(record)
        drained = record_from_buffer(buffered_record(event), source="github_repos")
        assert drained == SourceRecord(
            id=record.id, source="github_repos", record_type=record.record_type,
            summary=record.summary, content=record.content, timestamp=record.timestamp,
            metadata=record.metadata,
        )


def _source_record_event(record: SourceRecord):
    """The contract event of a record, as the gateway would deliver it."""
    from nerve.channels.hosted.contract.model import (
        ConversationReference,
        Event,
        SourceRecordPayload,
    )

    return Event(
        event_id="github_repos:1", provider="github_repos", kind="source_record",
        conversation=ConversationReference(id=REPO),
        record=SourceRecordPayload(
            id=record.id, record_type=record.record_type, summary=record.summary,
            content=record.content,
            timestamp=datetime.fromisoformat(record.timestamp),
            metadata=tuple(MetadataEntry(key=key, value=value) for key, value in record.metadata.items()),
        ),
    )


# ---------------------------------------------------------------------- #
#  Filters                                                                #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_deny_authors_are_case_insensitive_globs_and_drop_before_anything_is_reported():
    """Releases included: only the bot filter leaves them alone."""
    fake = FakeGitHub()
    src = source(
        fake, types=["pull_request", "issue", "release"], deny_authors=["Noisy-*", "spammer"],
    )
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1, author="noisy-ci"), pull(2, created=2, author="alice")]
    fake.issues[REPO] = [issue(3, created=3, author="SPAMMER"), issue(4, created=4, author="bob")]
    fake.releases[REPO] = [
        release(900, tag="v2.0.0", created=5, published=5, author="noisy-releaser", kind="Bot"),
        release(901, tag="v2.0.1", created=6, published=6, author="carol"),
    ]

    result = await src.fetch(cursor)

    assert [record.metadata["author"] for record in result.records] == ["alice", "bob", "carol"]


def test_the_inbox_filter_drops_what_the_settings_drop():
    """The second net, for records the gateway delivered before a reload."""
    settings = GitHubReposSyncConfig(types=["pull_request", "release"], deny_authors=["noisy-*"])

    def record(name: str, **metadata: str) -> SourceRecord:
        fields = {"author": "alice", "author_kind": "human", "subject_type": "PullRequest", **metadata}
        return SourceRecord(
            id=name, source="github_repos", record_type="github_repo_activity",
            summary="s", content="c", timestamp=at(0), metadata=fields,
        )

    kept, dropped = github_repos_inbox_filter(settings).partition([
        record("pr"),
        record("denied", author="Noisy-CI"),
        record("bot pr", author_kind="bot"),
        record("issue", subject_type="Issue"),
        record("bot release", subject_type="Release", author="github-actions[bot]", author_kind="bot"),
        record("denied release", subject_type="Release", author="noisy-releaser"),
    ])

    assert [r.id for r in kept] == ["pr", "bot release"]
    assert [r.id for r in dropped] == ["denied", "bot pr", "issue", "denied release"]
    with_bots = github_repos_inbox_filter(GitHubReposSyncConfig(include_bots=True))
    assert with_bots.passes(record("bot pr", author_kind="bot"))
    # A repository the watch no longer names: its records already on the way.
    watching = github_repos_inbox_filter(GitHubReposSyncConfig(repos=["acme/widgets"]))
    assert watching.passes(record("watched", repo_name="ACME/Widgets"))
    assert not watching.passes(record("dropped", repo_name="other-org/other-repo"))


# ---------------------------------------------------------------------- #
#  Drafts and updates                                                     #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_draft_is_reported_when_opened_by_default():
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"])
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1, draft=True)]

    [record] = (await src.fetch(cursor)).records

    assert record.id == f"{REPO}#1:opened:open"
    assert "Draft: true" in record.content


@pytest.mark.asyncio
async def test_without_drafts_a_draft_is_held_until_it_is_ready():
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"], include_drafts=False)
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1, draft=True), pull(2, created=2, draft=True)]

    held = await src.fetch(cursor)

    assert held.records == []
    assert cursor_of(held)["repos"][REPO]["prs"] == {
        "1": [SHA_A[:12], True, True], "2": [SHA_A[:12], True, True],
    }
    # A push while it is a draft reports nothing; the other draft is closed.
    fake.pulls[REPO] = [
        pull(1, created=1, updated=5, draft=True, sha=SHA_B),
        pull(2, created=2, updated=6, draft=True, state="closed"),
    ]
    pushed = await src.fetch(held.next_cursor)
    assert pushed.records == []
    assert cursor_of(pushed)["repos"][REPO]["prs"] == {"1": [SHA_B[:12], True, True]}

    fake.pulls[REPO][0] = pull(1, created=1, updated=9, draft=False, sha=SHA_B, title="Widget cache")
    ready = await src.fetch(pushed.next_cursor)

    [record] = ready.records
    assert record.id == f"{REPO}#1:ready:{SHA_B}"
    assert record.summary == "[acme/widgets] PR #1 ready for review: Widget cache"
    assert "Event: ready" in record.content and record.timestamp == at(9)
    assert record.metadata["event"] == "ready"
    # Reported once; without updates nothing is followed any more.
    assert "prs" not in cursor_of(ready)["repos"][REPO]
    assert (await src.fetch(ready.next_cursor)).records == []


@pytest.mark.asyncio
async def test_a_held_draft_still_waits_after_drafts_are_included():
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"], include_drafts=False)
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1, draft=True)]
    cursor = (await src.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=1, updated=4, draft=False)]

    [record] = (await source(fake, types=["pull_request"]).fetch(cursor)).records

    assert record.metadata["event"] == "ready"


@pytest.mark.asyncio
async def test_with_updates_a_reported_pull_request_is_followed_until_it_is_merged():
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"], updates=True)
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1, draft=True, sha=SHA_A)]

    def step(**changes: Any) -> None:
        current = fake.pulls[REPO][0]
        fake.pulls[REPO] = [pull(1, created=1, **{
            "draft": current["draft"], "sha": current["head"]["sha"], **changes,
        })]

    async def events() -> list[str]:
        nonlocal cursor
        result = await src.fetch(cursor)
        cursor = result.next_cursor
        return [record.id for record in result.records]

    assert await events() == [f"{REPO}#1:opened:open"]
    step(updated=2)  # a comment or a label: only updated_at moves
    assert await events() == []
    step(updated=3, sha=SHA_B)
    assert await events() == [f"{REPO}#1:updated:{SHA_B}"]
    step(updated=4, draft=False, sha="c" * 40)  # ready and pushed: one record, ready
    assert await events() == [f"{REPO}#1:ready:{'c' * 40}"]
    step(updated=5, draft=True)  # back to draft: nothing
    assert await events() == []
    step(updated=6, sha="d" * 40, state="closed", merged=6)  # pushed and merged: merged
    assert await events() == [f"{REPO}#1:merged:merged"]
    assert "prs" not in json.loads(cursor)["repos"][REPO]
    step(updated=7, state="open")  # reopened: no longer followed
    assert await events() == []


@pytest.mark.asyncio
async def test_with_updates_a_close_without_a_merge_is_closed():
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"], updates=True)
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1, title="Widget cache")]
    cursor = (await src.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=1, updated=8, state="closed", title="Widget cache")]

    [record] = (await src.fetch(cursor)).records

    assert record.id == f"{REPO}#1:closed:closed"
    assert record.summary == "[acme/widgets] PR #1 closed: Widget cache"
    assert record.timestamp == at(8)
    assert "State: closed" in record.content


@pytest.mark.asyncio
async def test_a_pull_request_first_seen_merged_is_only_opened():
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"], updates=True)
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1, updated=2, state="closed", merged=2)]

    result = await src.fetch(cursor)

    [record] = result.records
    assert record.id == f"{REPO}#1:opened:open" and "State: merged" in record.content
    assert "prs" not in cursor_of(result)["repos"][REPO]


@pytest.mark.asyncio
async def test_without_updates_nothing_is_followed_and_turning_them_off_forgets():
    fake = FakeGitHub()
    followed = source(fake, types=["pull_request"], updates=True)
    cursor = await baseline(followed)
    fake.pulls[REPO] = [pull(1, created=1)]
    cursor = (await followed.fetch(cursor)).next_cursor
    assert json.loads(cursor)["repos"][REPO]["prs"] == {"1": [SHA_A[:12], False, False]}
    fake.pulls[REPO] = [pull(1, created=1, updated=3, sha=SHA_B)]

    result = await source(fake, types=["pull_request"]).fetch(cursor)

    assert result.records == []
    assert "prs" not in cursor_of(result)["repos"][REPO]


@pytest.mark.asyncio
async def test_a_followed_pull_request_whose_author_is_denied_is_dropped_silently():
    fake = FakeGitHub()
    cursor = await baseline(source(fake, types=["pull_request"], updates=True))
    fake.pulls[REPO] = [pull(1, created=1, author="alice")]
    cursor = (await source(fake, types=["pull_request"], updates=True).fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=1, updated=2, author="alice", sha=SHA_B)]

    result = await source(fake, types=["pull_request"], updates=True, deny_authors=["alice"]).fetch(cursor)

    assert result.records == []
    assert "prs" not in cursor_of(result)["repos"][REPO]


@pytest.mark.asyncio
async def test_at_most_so_many_pull_requests_are_followed_the_one_listed_longest_ago_first(monkeypatch):
    monkeypatch.setattr(module, "MAX_FINGERPRINTS", 2)
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"], updates=True)
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1), pull(2, created=2)]
    cursor = (await src.fetch(cursor)).next_cursor
    # 1 is listed again (a comment), then 3 opens: 2 was listed longest ago.
    fake.pulls[REPO] = [pull(1, created=1, updated=5), pull(2, created=2), pull(3, created=6)]

    result = await src.fetch(cursor)

    assert list(cursor_of(result)["repos"][REPO]["prs"]) == ["1", "3"]


# ---------------------------------------------------------------------- #
#  Cursor upgrades and failures                                           #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_cursor_of_the_earlier_source_still_reads():
    """One ISO timestamp, the newest created_at the earlier version saw: it
    becomes every listing's cursor (and when it was read), so nothing is
    repeated or skipped. What was created in that very second was the earlier
    version's last report, and is not reported again."""
    fake = FakeGitHub()
    fake.pulls[REPO] = [pull(1, created=-10), pull(2, created=5)]
    fake.issues[REPO] = [issue(3, created=0, updated=7), issue(4, created=6)]
    src = source(fake)

    result = await src.fetch(at(0))

    assert [record.id for record in result.records] == [f"{REPO}#2:opened:open", f"{REPO}#4:opened:open"]
    assert cursor_of(result) == {"v": 1, "repos": {REPO: {"pulls": read(at(5), "2"), "issues": read(at(7))}}}


@pytest.mark.asyncio
async def test_the_cursor_of_the_earlier_source_more_than_a_day_old_reports_the_last_day():
    """24.5 h old is within the day and its hour of grace; 25 h 55 min old is not."""
    fake = FakeGitHub()
    fake.pulls[REPO] = [pull(1, created=5), pull(2, created=90)]
    src = source(fake, types=["pull_request"], clock=lambda: START + timedelta(hours=26))

    result = await src.fetch(at(0))

    assert [record.id for record in result.records] == [f"{REPO}#2:opened:open"]


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor", ["57", "not a cursor", "{broken json", '{"v": 1, "repos": []}'])
async def test_a_cursor_that_cannot_be_read_takes_a_baseline(cursor):
    """A buffer position left by the hosted mode, or anything unreadable,
    is no cursor: nothing is reported."""
    fake = FakeGitHub()
    fake.pulls[REPO] = [pull(1, created=5)]

    result = await source(fake, types=["pull_request"]).fetch(cursor)

    assert result.records == []
    assert cursor_of(result) == {"v": 1, "repos": {REPO: {"pulls": read(at(5), "1")}}}


@pytest.mark.asyncio
async def test_a_failed_listing_keeps_its_cursor_and_the_others_report(caplog):
    fake = FakeGitHub()
    src = source(fake, repos=[REPO, OTHER], types=["pull_request"])
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1)]
    fake.pulls[OTHER] = [pull(2, created=2, repo=OTHER)]
    fake.failing = {f"{OTHER} pulls"}

    result = await src.fetch(cursor)

    assert [record.id for record in result.records] == [f"{REPO}#1:opened:open"]
    assert cursor_of(result)["repos"][OTHER] == {"pulls": read("")}
    assert "1 of 2 listings failed" in caplog.text
    fake.failing = set()
    later = await src.fetch(result.next_cursor)
    assert [record.id for record in later.records] == [f"{OTHER}#2:opened:open"]


@pytest.mark.asyncio
async def test_a_run_in_which_every_listing_fails_fails():
    fake = FakeGitHub()
    fake.failing = {f"{REPO} pulls", f"{REPO} issues"}

    with pytest.raises(RuntimeError, match="every GitHub listing failed"):
        await source(fake).fetch(None)


@pytest.mark.asyncio
async def test_a_repository_or_type_added_takes_its_own_baseline_and_a_removed_one_is_forgotten():
    fake = FakeGitHub()
    cursor = await baseline(source(fake, repos=[REPO], types=["pull_request"]))
    fake.pulls[REPO] = [pull(1, created=1)]
    fake.pulls[OTHER] = [pull(2, created=2, repo=OTHER)]
    fake.issues[OTHER] = [issue(3, created=3, repo=OTHER)]

    result = await source(fake, repos=[OTHER], types=["pull_request", "issue"]).fetch(cursor)

    assert result.records == []
    assert cursor_of(result) == {"v": 1, "repos": {OTHER: {"pulls": read(at(2), "2"), "issues": read(at(3), "3")}}}


@pytest.mark.asyncio
async def test_no_repository_reads_nothing_and_keeps_the_cursor():
    fake = FakeGitHub()

    result = await source(fake, repos=[]).fetch("keep")

    assert (result.records, result.next_cursor, fake.calls) == ([], "keep", [])


@pytest.mark.asyncio
async def test_gh_failures_are_reported_with_what_failed(monkeypatch):
    class Proc:
        returncode = 1

        async def communicate(self):
            return b"", b"gh: Not Found (HTTP 404)"

    async def failing(*argv, **kwargs):
        return Proc()

    monkeypatch.setattr("nerve.sources.github_repos.asyncio.create_subprocess_exec", failing)

    with pytest.raises(RuntimeError, match=r"repos/acme/widgets/pulls.*HTTP 404"):
        await GitHubReposSource._gh_api_get("repos/acme/widgets/pulls?per_page=100")


def test_the_registry_hands_the_settings_to_the_polling_source():
    from nerve.sources.registry import build_source_runners

    config = NerveConfig.from_dict({"sync": {"github_repos": {
        "enabled": True, "repos": [REPO], "types": ["release"], "include_bots": True,
        "include_drafts": False, "updates": True, "deny_authors": ["noisy-*"],
    }}})

    (runner,) = [r for r in build_source_runners(config, db=None) if r.job_id == "source:github_repos"]

    src = runner.source
    assert isinstance(src, GitHubReposSource) and src.push_fed is False
    assert (src._repos, src._types, src._include_bots, src._include_drafts, src._updates, src._deny_authors) == (
        [REPO], ("release",), True, False, True, ["noisy-*"],
    )
    assert [rule.field for rule in runner.inbox_filter.rules if rule.active] == [
        "repo_name", "author", "subject_type",
    ]


@pytest.mark.asyncio
async def test_the_source_tools_show_a_long_cursor_cut(db):
    """The JSON cursor grows with the repositories and the pull requests it
    follows; the agent's source listings show its start and its length."""
    from nerve.agent.tools.handlers.sources import (
        list_sources_handler,
        sync_status_handler,
    )
    from nerve.agent.tools.registry import ToolContext

    state = {REPO: {"pulls": at(5), "prs": {str(n): [SHA_A[:12], False, False] for n in range(40)}}}
    cursor = json.dumps({"v": 1, "repos": state}, separators=(",", ":"))
    await db.set_sync_cursor("github_repos", cursor)
    await db.set_sync_cursor("telegram", '{"pts": 1, "qts": 0, "date": 1700000000, "seq": 0}')
    await db.insert_source_messages([SourceRecord(
        id="r1", source="github_repos", record_type="github_repo_activity", summary="s", content="c",
        timestamp=at(5), metadata={},
    )], source="github_repos")
    ctx = ToolContext(session_id="s", db=db)

    for handler, args in ((sync_status_handler, {"source": "all"}), (list_sources_handler, {})):
        text = (await handler(ctx, args)).text_content
        assert f"cursor: {cursor[:120]}… ({len(cursor)} chars)" in text
        assert cursor not in text
    status = (await sync_status_handler(ctx, {"source": "telegram"})).text_content
    assert 'cursor: {"pts": 1, "qts": 0, "date": 1700000000, "seq": 0}' in status


@pytest.mark.asyncio
async def test_the_runner_logs_a_long_cursor_cut(db, caplog):
    """The JSON cursor of a large watch is kilobytes; the runner's log lines
    show its start and its length, as the agent's tools do."""
    from nerve.sources.base import Source
    from nerve.sources.models import FetchResult
    from nerve.sources.runner import SourceRunner

    def cursor(n: int) -> str:
        state = {f"acme/repo-{k}": {"pulls": read(at(n))} for k in range(10)}
        return json.dumps({"v": 1, "repos": state}, separators=(",", ":"))

    class Repos(Source):
        source_name = "github_repos"

        async def fetch(self, cursor_value: str | None, limit: int = 100) -> FetchResult:
            return FetchResult(records=[], next_cursor=cursor(2))

    await db.set_sync_cursor("github_repos", cursor(1))
    caplog.set_level(logging.INFO, logger="nerve.sources.runner")

    await SourceRunner(Repos(), db).run()

    lines = [record.getMessage() for record in caplog.records if record.name == "nerve.sources.runner"]
    assert any(f"(cursor={cursor(1)[:120]}… ({len(cursor(1))} chars)," in line for line in lines)
    assert any(f"cursor advanced to {cursor(2)[:120]}… ({len(cursor(2))} chars)" in line for line in lines)
    assert not any(cursor(1) in line or cursor(2) in line for line in lines)
