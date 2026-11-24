"""The GitHub Repos source in poll mode: what is new in repositories the agent follows.

A fake ``gh api`` serves the pulls, issues and releases listings of a few
repositories, and the commits of their branches, newest change first as GitHub
does; each test lists once for the
baseline, changes the repositories, and lists again. The records are pinned to
the shape the channel gateway builds for the hosted mode (same id, type,
summary, content, timestamp and metadata), so that the guardrails, the gates
and the agent see the same thing in both modes.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
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
    closed: int | None = None, sha: str = SHA_A, title: str | None = None, body: str = "",
    labels: tuple[str, ...] = (), repo: str = REPO,
) -> dict[str, Any]:
    """A pull request as the pulls listing returns it. One whose state is
    closed was closed at *closed*, or else when it was merged or last updated."""
    if closed is None:
        closed = merged if merged is not None else updated or created
    return {
        "number": number,
        "title": title or f"Pull request {number}",
        "user": user(author, kind),
        "state": state,
        "draft": draft,
        "merged_at": at(merged) if merged is not None else None,
        "closed_at": at(closed) if state == "closed" else None,
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


def sha(number: int) -> str:
    """The commit of a number: 40 characters, as GitHub names a commit, which
    differ within the 12 characters that a record's id keeps of a commit."""
    return f"{number:08x}" + "c" * 32


def pushed_id(branch: str, before: int, head: int, repo: str | None = None) -> str:
    """The id of the record of a branch that moved from one commit to another."""
    return f"{repo or REPO}@{branch}:pushed:{sha(before)[:12]}..{sha(head)[:12]}"


def commit(number: int, message: str | None = None) -> dict[str, Any]:
    """A commit as the commits listing of a branch returns it."""
    person = {"name": "Alice Example", "date": at(-5)}
    return {
        "sha": sha(number),
        "commit": {
            "message": f"Commit {number}" if message is None else message,
            "author": person, "committer": person,
        },
        "author": user(),
        "html_url": f"https://github.com/{REPO}/commit/{sha(number)}",
    }


def history(head: int) -> list[dict[str, Any]]:
    """The commits of a branch whose head is *head*, down to 1, the head first."""
    return [commit(number) for number in range(head, 0, -1)]


class FakeGitHub:
    """``gh api`` over a few repositories' listings; records every endpoint asked for."""

    _ENDPOINT = re.compile(r"repos/([^/]+/[^/?]+)/(pulls|issues|releases)\?(.*)")
    _COMMITS = re.compile(r"repos/([^/]+/[^/?]+)/commits\?sha=(refs/heads/)?([^&]+)&per_page=100")
    _REPOSITORY = re.compile(r"repos/([^/]+/[^/?]+)")

    def __init__(self) -> None:
        self.pulls: dict[str, list[dict]] = {}
        self.issues: dict[str, list[dict]] = {}
        self.releases: dict[str, list[dict]] = {}
        # The commits of each branch, the head first, by (repository, branch),
        # and the default branch of each repository. GitHub lists a branch
        # for its ref, refs/heads/<branch>; for a bare name it also lists a
        # tag or a commit of that name, which other_refs holds.
        self.commits: dict[tuple[str, str], list[dict]] = {}
        self.other_refs: dict[tuple[str, str], list[dict]] = {}
        self.default_branch: dict[str, str] = {}
        self.calls: list[str] = []
        self.failing: set[str] = set()

    async def gh_api_get(self, endpoint: str, timeout: float = 30) -> Any:
        self.calls.append(endpoint)
        if (commits := self._COMMITS.fullmatch(endpoint)) is not None:
            repo, ref, name = commits.groups()
            listed = self.commits if ref else {**self.other_refs, **self.commits}
            if (repo, name) not in listed:
                raise RuntimeError("gh: Not Found (HTTP 404)")
            return copy.deepcopy(listed[(repo, name)][:100])
        if (repository := self._REPOSITORY.fullmatch(endpoint)) is not None:
            return {"full_name": repository[1], "default_branch": self.default_branch[repository[1]]}
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


def ago(**delta: int) -> str:
    """A GitHub timestamp that long before ``NOW``."""
    return (NOW - timedelta(**delta)).strftime("%Y-%m-%dT%H:%M:%SZ")


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
            metadata=tuple(_metadata_entry(key, value) for key, value in record.metadata.items()),
        ),
    )


def _metadata_entry(key: str, value: Any) -> MetadataEntry:
    """A metadata member as the gateway sends it: a list in ``values``, a flag as text."""
    if isinstance(value, list):
        return MetadataEntry(key=key, values=tuple(value))
    if isinstance(value, bool):
        return MetadataEntry(key=key, value="true" if value else "false")
    return MetadataEntry(key=key, value=value)


# ---------------------------------------------------------------------- #
#  Commits of a branch                                                    #
# ---------------------------------------------------------------------- #

MAIN = (REPO, "main")


def commits_source(fake: FakeGitHub, **settings: Any) -> GitHubReposSource:
    """A source that follows the commits of main of the repository, unless *settings* say otherwise."""
    settings.setdefault("types", ["commit"])
    settings.setdefault("branches", {REPO: ["main"]})
    return source(fake, **settings)


@pytest.mark.asyncio
async def test_the_first_listing_of_a_branch_only_stores_its_head():
    fake = FakeGitHub()
    fake.commits[MAIN] = history(3)
    src = commits_source(fake)

    first = await src.fetch(None)

    assert first.records == []
    assert cursor_of(first) == {"v": 1, "repos": {REPO: {"branches": {"main": sha(3)}}}}
    assert fake.calls == [f"repos/{REPO}/commits?sha=refs/heads/main&per_page=100"]
    again = await src.fetch(first.next_cursor)
    assert (again.records, again.next_cursor) == ([], first.next_cursor)


@pytest.mark.asyncio
async def test_a_head_that_moved_gives_one_record_as_the_gateway_builds_it():
    """One record for the branch, whatever the number of commits, with the
    time of the listing; the same head gives no second record."""
    fake = FakeGitHub()
    fake.commits[MAIN] = history(2)
    src = commits_source(fake)
    cursor = await baseline(src)
    fake.commits[MAIN] = history(5)

    result = await src.fetch(cursor)

    compare = f"https://github.com/{REPO}/compare/{sha(2)}...{sha(5)}"
    assert result.records == [SourceRecord(
        id=f"{REPO}@main:pushed:00000002cccc..00000005cccc",
        source="github_repos",
        record_type="github_repo_activity",
        summary=f"[{REPO}] 3 new commits on main",
        content=(
            f"Repository: {REPO}\nType: Commit\nEvent: pushed\nBranch: main\nCommits: 3\n"
            f"Head: {sha(5)}\nBefore: {sha(2)}\nURL: {compare}\n\n--- Commits ---\n"
            f"- {sha(5)} Commit 5\n- {sha(4)} Commit 4\n- {sha(3)} Commit 3"
        ),
        timestamp=STAMP,
        metadata={
            "repo_name": REPO, "event": "pushed", "subject_type": "Commit", "branch": "main",
            "head_sha": sha(5), "before_sha": sha(2), "commit_count": "3",
            "commits": [f"{sha(5)} Commit 5", f"{sha(4)} Commit 4", f"{sha(3)} Commit 3"],
            "truncated": False, "subject_url": compare, "repo_url": f"https://github.com/{REPO}",
        },
    )]
    assert list(result.records[0].metadata) == [
        "repo_name", "event", "subject_type", "branch", "head_sha", "before_sha", "commit_count",
        "commits", "truncated", "subject_url", "repo_url",
    ]
    assert cursor_of(result)["repos"][REPO]["branches"] == {"main": sha(5)}
    assert (await src.fetch(result.next_cursor)).records == []
    fake.commits[MAIN] = history(6)
    (one,) = (await src.fetch(result.next_cursor)).records
    assert (one.summary, one.metadata["before_sha"]) == (f"[{REPO}] 1 new commit on main", sha(5))
    assert one.id == pushed_id("main", 5, 6)


# A copy of the channel contract's sample of the new commits of a branch. It
# must stay byte for byte the same as the contract's
# ``testdata/events/github_repo_commits.json``.
COMMITS_SAMPLE = Path(__file__).parent / "fixtures" / "channel_contract" / "github_repo_commits.json"
COMMITS_SAMPLE_SHA256 = "84bb74bd0ad1d562466b85b14c3372e55e75586a8fa834f1d186d10911e577ea"


@pytest.mark.asyncio
async def test_the_record_is_the_one_of_the_contract_sample():
    """The gateway writes the contract's sample for two commits on main. This
    source builds the same record from the same listing, and the hosted source
    reads the sample into that record."""
    from nerve.channels.hosted.contract.model import Event
    from nerve.channels.hosted.contract.wire import decode_record

    assert hashlib.sha256(COMMITS_SAMPLE.read_bytes()).hexdigest() == COMMITS_SAMPLE_SHA256
    event = decode_record(Event, json.loads(COMMITS_SAMPLE.read_text(encoding="utf-8")))
    earlier = {**commit(0, "Earlier work"), "sha": "89abcdef0123456789abcdef0123456789abcdef"}
    fake = FakeGitHub()
    fake.commits[MAIN] = [earlier]
    src = commits_source(fake, clock=lambda: datetime(2026, 9, 16, 10, 35, 0, 987_000, tzinfo=UTC))
    cursor = await baseline(src)
    fake.commits[MAIN] = [
        {**commit(0, "Cache rendered widgets\n\nRenders each widget once and keeps it."),
         "sha": "5c4d3e2f1a0b99887766554433221100ffeeddcc"},
        {**commit(0, "Add the widget cache interface"), "sha": "1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"},
        earlier,
    ]

    (record,) = (await src.fetch(cursor)).records

    assert record == record_from_buffer(buffered_record(event), source="github_repos")
    assert record == record_from_buffer(buffered_record(_source_record_event(record)), source="github_repos")


@pytest.mark.asyncio
@pytest.mark.parametrize(("before", "head", "count", "listed", "truncated", "summary"), [
    (history(10), 60, "50", 50, False, "50 new commits on main"),
    (history(10), 61, "51", 50, True, "51 new commits on main"),
    (history(1), 100, "99", 50, True, "99 new commits on main"),
    # As many commits as the listing's page, or more: it does not show the
    # earlier head.
    (history(1), 101, "", 50, True, f"main moved to {sha(101)[:12]}"),
    (history(1), 140, "", 50, True, f"main moved to {sha(140)[:12]}"),
    # A rewritten branch does not hold the earlier head any more.
    ([commit(900)], 3, "", 3, True, f"main moved to {sha(3)[:12]}"),
])
async def test_a_record_says_when_it_does_not_list_every_new_commit(
    before, head, count, listed, truncated, summary,
):
    fake = FakeGitHub()
    fake.commits[MAIN] = before
    src = commits_source(fake)
    cursor = await baseline(src)
    fake.commits[MAIN] = history(head)

    (record,) = (await src.fetch(cursor)).records

    metadata = record.metadata
    assert (metadata["commit_count"], len(metadata["commits"]), metadata["truncated"]) == (count, listed, truncated)
    assert (metadata["commits"][0], metadata["before_sha"]) == (f"{sha(head)} Commit {head}", before[0]["sha"])
    assert record.summary == f"[{REPO}] {summary}"
    assert metadata["subject_url"] == f"https://github.com/{REPO}/compare/{before[0]['sha']}...{sha(head)}"
    assert record.content.endswith("\n[... not every new commit is listed]") is truncated
    assert ("\nCommits: " in record.content) is bool(count)
    assert record.content.count("\n- ") == listed


@pytest.mark.asyncio
async def test_each_branch_is_reported_and_takes_its_own_baseline():
    """Two branches that moved give two records in one run. A branch named
    later starts from its head, and one no longer named is forgotten."""
    fake = FakeGitHub()
    fake.commits[MAIN], fake.commits[(REPO, "release/1.2")] = history(4), history(2)
    src = commits_source(fake, branches={"Acme/Widgets": ["main", "release/1.2"]})
    cursor = await baseline(src)
    assert fake.calls == [
        f"repos/{REPO}/commits?sha=refs/heads/{branch}&per_page=100" for branch in ("main", "release/1.2")
    ]
    fake.commits[MAIN], fake.commits[(REPO, "release/1.2")] = history(6), history(3)

    result = await src.fetch(cursor)

    assert [record.id for record in result.records] == [
        pushed_id("main", 4, 6), pushed_id("release/1.2", 2, 3),
    ]
    fake.commits[(REPO, "next")] = history(9)
    fake.commits[(REPO, "release/1.2")] = history(5)
    later = await commits_source(fake, branches={REPO: ["release/1.2", "next"]}).fetch(result.next_cursor)
    assert [record.id for record in later.records] == [pushed_id("release/1.2", 3, 5)]
    assert cursor_of(later)["repos"][REPO]["branches"] == {"release/1.2": sha(5), "next": sha(9)}


@pytest.mark.asyncio
async def test_without_branches_the_default_branch_is_followed(caplog):
    """The repository names its default branch at every run. When it is
    another one, that branch starts from its head and the earlier one is
    forgotten; one with a name that the source cannot follow reports nothing."""
    fake = FakeGitHub()
    fake.default_branch[REPO] = "trunk"
    fake.commits[(REPO, "trunk")] = history(2)
    src = commits_source(fake, branches={})
    cursor = await baseline(src)
    assert fake.calls == [f"repos/{REPO}", f"repos/{REPO}/commits?sha=refs/heads/trunk&per_page=100"]
    fake.commits[(REPO, "trunk")] = history(3)

    result = await src.fetch(cursor)

    assert [record.id for record in result.records] == [pushed_id("trunk", 2, 3)]
    fake.default_branch[REPO], fake.commits[MAIN] = "main", history(7)
    moved = await src.fetch(result.next_cursor)
    assert (moved.records, cursor_of(moved)["repos"][REPO]["branches"]) == ([], {"main": sha(7)})
    fake.default_branch[REPO] = "feature+x"
    with caplog.at_level(logging.WARNING, logger="nerve.sources.github_repos"):
        unfollowed = await src.fetch(moved.next_cursor)
    assert (unfollowed.records, cursor_of(unfollowed)["repos"][REPO]) == ([], {})
    assert "the default branch of acme/widgets has a name that the source cannot follow" in caplog.text


@pytest.mark.asyncio
async def test_a_branch_that_cannot_be_listed_keeps_its_head_and_the_others_report(caplog):
    """A branch that GitHub does not find fails its listing alone: its head
    stays in the cursor, and its commits report once it is back."""
    fake = FakeGitHub()
    fake.commits[MAIN], fake.commits[(REPO, "gone")] = history(4), history(2)
    src = commits_source(fake, branches={REPO: ["gone", "main"]})
    cursor = await baseline(src)
    del fake.commits[(REPO, "gone")]
    fake.commits[MAIN] = history(5)

    with caplog.at_level(logging.WARNING, logger="nerve.sources.github_repos"):
        result = await src.fetch(cursor)

    assert [record.id for record in result.records] == [pushed_id("main", 4, 5)]
    assert cursor_of(result)["repos"][REPO]["branches"] == {"gone": sha(2), "main": sha(5)}
    assert "1 of 2 listings failed and keep their cursor, e.g. acme/widgets commits of gone" in caplog.text
    fake.commits[(REPO, "gone")] = history(3)
    back = await src.fetch(result.next_cursor)
    assert [record.id for record in back.records] == [pushed_id("gone", 2, 3)]
    del fake.commits[MAIN], fake.commits[(REPO, "gone")]
    with pytest.raises(RuntimeError, match=r"every GitHub listing failed \(2\)"):
        await src.fetch(back.next_cursor)


@pytest.mark.parametrize(("message", "subject"), [
    ("Fix the retry test", " Fix the retry test"),
    ("Fix the retry test\n\nIt waited for the wrong event.", " Fix the retry test"),
    ("Fix the retry test\r\nMore", " Fix the retry test"),
    ("  Fix\tthe\x07retry test  ", " Fix the retry test"),
    # The line and the paragraph separator, and the bidirectional controls.
    ("Fix\u2028the\u2029retry test", " Fix the retry test"),
    ("Fix the \u202etset yrter\u202c", " Fix the  tset yrter"),
    ("a\u202ab\u202bc\u202cd\u202de\u202ef\u2066g\u2067h\u2068i\u2069j", " a b c d e f g h i j"),
    # The characters next to them stay.
    ("a\u2027b\u202fc\u2065d\u206ae", " a\u2027b\u202fc\u2065d\u206ae"),
    ("", ""),
    ("\nBody alone", ""),
    ("   \nBody", ""),
    ("a" * 125, " " + "a" * 120),
    ("a" * 119 + " tail", " " + "a" * 119),
    # The entry is cut to 256 bytes, at a character.
    ("ü" * 125, " " + "ü" * 107),
])
def test_a_commit_is_listed_with_the_first_line_of_its_message(message, subject):
    """The rules of the gateway's record: control characters, the line and
    paragraph separators and the bidirectional controls as spaces, no spaces
    around the line, 120 characters, and the entry within 256 bytes."""
    assert module._commit_entry(commit(7, message)) == sha(7) + subject


@pytest.mark.asyncio
async def test_a_name_that_is_also_a_tag_or_a_commit_is_followed_as_a_branch_alone():
    """The listing is asked for the branch's ref. A name that GitHub would
    also read as a tag or as a commit finds no branch: its listing fails, no
    head is stored, and nothing is reported, whatever the tag does."""
    fake = FakeGitHub()
    fake.commits[MAIN] = history(1)
    names = ["v1.2.0", sha(7)]
    for name in names:
        fake.other_refs[(REPO, name)] = history(3)
    src = commits_source(fake, branches={REPO: [*names, "main"]})

    first = await src.fetch(None)
    for name in names:
        fake.other_refs[(REPO, name)] = history(4)
    second = await src.fetch(first.next_cursor)

    assert (first.records, second.records) == ([], [])
    assert cursor_of(second)["repos"][REPO]["branches"] == {"main": sha(1)}
    assert all("/commits?sha=refs/heads/" in call for call in fake.calls)


@pytest.mark.asyncio
async def test_a_branch_that_returns_to_an_earlier_head_has_a_record_of_its_own():
    """The id names the earlier head and the head. A branch that goes back,
    as after a push that was taken back, gives a record with another id, which
    counts no commit; only the same move again has the id of an earlier one."""
    fake = FakeGitHub()
    src = commits_source(fake)
    cursor, records = None, []
    for head in (2, 3, 2, 3):
        fake.commits[MAIN] = history(head)
        result = await src.fetch(cursor)
        cursor = result.next_cursor
        records += result.records

    assert [record.id for record in records] == [
        pushed_id("main", 2, 3), pushed_id("main", 3, 2), pushed_id("main", 2, 3),
    ]
    back = records[1].metadata
    assert (back["before_sha"], back["head_sha"], back["commit_count"], back["truncated"]) == (
        sha(3), sha(2), "", True,
    )
    assert back["subject_url"] == f"https://github.com/{REPO}/compare/{sha(3)}...{sha(2)}"


@pytest.mark.asyncio
async def test_the_record_lists_the_commits_that_the_listing_shows_above_the_earlier_head():
    """GitHub lists by commit date, not by ancestry. After a merge, a commit
    that the merge brought in and that is older than the earlier head is
    listed below it: the record neither lists nor counts it, and its URL
    compares the two heads."""
    fake = FakeGitHub()
    merge, head, side, base = (
        commit(30, "Merge the side branch"), commit(20, "The head before the merge"),
        commit(10, "An older commit of the side branch"), commit(1, "The common base"),
    )
    fake.commits[MAIN] = [head, base]
    src = commits_source(fake)
    cursor = await baseline(src)
    fake.commits[MAIN] = [merge, head, side, base]

    (record,) = (await src.fetch(cursor)).records

    assert (record.metadata["commit_count"], record.metadata["truncated"]) == ("1", False)
    assert record.metadata["commits"] == [f"{sha(30)} Merge the side branch"]
    assert record.metadata["subject_url"] == f"https://github.com/{REPO}/compare/{sha(20)}...{sha(30)}"


def test_a_record_without_an_earlier_head_counts_no_commit():
    """The contract's record for an unknown earlier head. The source itself
    reports only after it stored a head."""
    record = module.commits_record(REPO, "main", "", history(2), NOW)

    assert (record.metadata["before_sha"], record.metadata["commit_count"], record.metadata["truncated"]) == ("", "", True)
    assert record.metadata["subject_url"] == f"https://github.com/{REPO}/commits/main"
    assert "Before: " not in record.content


def test_the_inbox_filter_keeps_commit_records_only_with_the_commit_type():
    """A commit record has no author: neither deny_authors nor include_bots judges it."""
    record = module.commits_record(REPO, "main", sha(1), history(2), NOW)

    watching = GitHubReposSyncConfig(repos=[REPO], types=["commit"], deny_authors=["*"])
    assert github_repos_inbox_filter(watching).passes(record)
    assert not github_repos_inbox_filter(GitHubReposSyncConfig(repos=[REPO])).passes(record)


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


@pytest.mark.asyncio
async def test_a_denied_author_that_ends_in_bot_names_a_github_app():
    """With bots included, the Apps that the list names are left out by login."""
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"], include_bots=True, deny_authors=["Noisy-App[bot]"])
    cursor = await baseline(src)
    fake.pulls[REPO] = [
        pull(1, created=1, author="noisy-app[bot]", kind="Bot"),
        pull(2, created=2, author="lint[bot]", kind="Bot"),
        pull(3, created=3, author="noisy-appt"),
    ]

    result = await src.fetch(cursor)

    assert [record.metadata["author"] for record in result.records] == ["lint[bot]", "noisy-appt"]
    settings = GitHubReposSyncConfig(include_bots=True, deny_authors=["Noisy-App[bot]"])
    kept, dropped = github_repos_inbox_filter(settings).partition(result.records)
    assert (kept, dropped) == (result.records, [])


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
    # A push while it is a draft reports nothing; the other draft is closed,
    # which reports nothing either, and it stays held, marked closed.
    fake.pulls[REPO] = [
        pull(1, created=1, updated=5, draft=True, sha=SHA_B),
        pull(2, created=2, updated=6, draft=True, state="closed"),
    ]
    pushed = await src.fetch(held.next_cursor)
    assert pushed.records == []
    assert cursor_of(pushed)["repos"][REPO]["prs"] == {
        "1": [SHA_B[:12], True, True], "2": [SHA_A[:12], True, True, at(6)],
    }

    fake.pulls[REPO][0] = pull(1, created=1, updated=9, draft=False, sha=SHA_B, title="Widget cache")
    ready = await src.fetch(pushed.next_cursor)

    [record] = ready.records
    assert record.id == f"{REPO}#1:ready:{SHA_B}"
    assert record.summary == "[acme/widgets] PR #1 ready for review: Widget cache"
    assert "Event: ready" in record.content and record.timestamp == at(9)
    assert record.metadata["event"] == "ready"
    # Reported once; without updates it is not followed any more. The closed
    # draft is still held.
    assert cursor_of(ready)["repos"][REPO]["prs"] == {"2": [SHA_A[:12], True, True, at(6)]}
    assert (await src.fetch(ready.next_cursor)).records == []


@pytest.mark.asyncio
@pytest.mark.parametrize("updates", [False, True])
async def test_a_held_draft_stays_held_through_a_close_until_it_is_open_and_ready(updates):
    """A held draft that is closed without a merge reports nothing and stays
    held, with or without updates, and so does a new draft that a listing
    first shows closed. Reopened as a draft it is held again; reopened ready
    for review it is reported, as ready: its first record. A held draft that
    is merged is forgotten, and never reported."""
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"], include_drafts=False, updates=updates)
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1, draft=True), pull(2, created=2, draft=True)]
    held = await src.fetch(cursor)
    # 1 is closed, and commented on a minute later. 2 is marked ready and
    # merged. 3 and 4 are new drafts that are closed already; the listing
    # shows a merge of 4, so it is not held.
    ended = [
        pull(2, created=2, updated=5, state="closed", merged=5),
        pull(4, created=6, draft=True, state="closed", merged=6),
    ]
    fake.pulls[REPO] = [
        pull(1, created=1, updated=5, draft=True, state="closed", closed=4),
        pull(3, created=6, draft=True, state="closed"), *ended,
    ]

    closed = await src.fetch(held.next_cursor)

    assert closed.records == []
    # Each is held with the time of its close.
    assert cursor_of(closed)["repos"][REPO]["prs"] == {
        "1": [SHA_A[:12], True, True, at(4)], "3": [SHA_A[:12], True, True, at(6)],
    }
    # Both are reopened: 1 as a draft, with a new head, and 3 ready for review.
    fake.pulls[REPO] = [pull(1, created=1, updated=8, draft=True, sha=SHA_B), pull(3, created=6, updated=9), *ended]
    reopened = await src.fetch(closed.next_cursor)
    assert [record.id for record in reopened.records] == [f"{REPO}#3:ready:{SHA_A}"]
    followed = {"3": [SHA_A[:12], False, False]} if updates else {}
    assert cursor_of(reopened)["repos"][REPO]["prs"] == {"1": [SHA_B[:12], True, True], **followed}

    fake.pulls[REPO][0] = pull(1, created=1, updated=10, sha=SHA_B)
    ready = await src.fetch(reopened.next_cursor)

    assert [record.id for record in ready.records] == [f"{REPO}#1:ready:{SHA_B}"]
    if updates:
        followed["1"] = [SHA_B[:12], False, False]
    assert cursor_of(ready)["repos"][REPO].get("prs", {}) == followed


@pytest.mark.asyncio
async def test_a_held_draft_that_is_closed_after_it_was_marked_ready_stays_held():
    """Marked ready and closed since the last listing: the listing shows it
    closed, and no draft. It was never reported, so its close is not either,
    and it stays held. Listed open again and no draft, it is reported as
    ready."""
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"], include_drafts=False)
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1, draft=True)]
    cursor = (await src.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=1, updated=4, state="closed", closed=3)]

    closed = await src.fetch(cursor)

    assert closed.records == []
    assert cursor_of(closed)["repos"][REPO]["prs"] == {"1": [SHA_A[:12], False, True, at(3)]}
    fake.pulls[REPO] = [pull(1, created=1, updated=6)]
    reopened = await src.fetch(closed.next_cursor)
    assert [record.id for record in reopened.records] == [f"{REPO}#1:ready:{SHA_A}"]
    assert "prs" not in cursor_of(reopened)["repos"][REPO]


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
    # Made a draft, pushed to and closed, and commented on a minute later: the
    # listing shows the new head and both times.
    fake.pulls[REPO] = [pull(
        1, created=1, updated=8, state="closed", closed=7, draft=True, sha=SHA_B, title="Widget cache",
    )]

    result = await src.fetch(cursor)

    [record] = result.records
    # A pull request can close more than once, so the time of the close names
    # it, and it is the record's time.
    assert record.id == f"{REPO}#1:closed:{at(7)}"
    assert record.summary == "[acme/widgets] PR #1 closed: Widget cache"
    assert record.timestamp == at(7)
    assert "State: closed" in record.content
    # It stays followed, with the time of its close, and with the head and
    # the draft state as listed.
    assert cursor_of(result)["repos"][REPO]["prs"] == {"1": [SHA_B[:12], True, False, at(7)]}


@pytest.mark.asyncio
async def test_a_close_without_a_time_has_the_word_closed_in_its_id():
    """GitHub gives every close a time. Without one the record is named
    closed and dated by the update, and the pull request is not kept: there is
    no close to count the thirty days from."""
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"], updates=True)
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1)]
    cursor = (await src.fetch(cursor)).next_cursor
    timeless = pull(1, created=1, updated=8, state="closed")
    timeless["closed_at"] = None
    fake.pulls[REPO] = [timeless]

    result = await src.fetch(cursor)

    [record] = result.records
    assert (record.id, record.timestamp) == (f"{REPO}#1:closed:closed", at(8))
    assert "prs" not in cursor_of(result)["repos"][REPO]


def test_a_reopened_record_needs_the_close_that_it_ends():
    """The open pull request does not say when it was closed, so the record
    cannot be named without the close that the cursor kept."""
    item = pull(1, created=1, updated=5)

    with pytest.raises(ValueError, match="needs the close"):
        module.pull_request_record(REPO, item, "reopened")

    assert module.pull_request_record(REPO, item, "reopened", close=at(3)).id == f"{REPO}#1:reopened:{at(3)}"


@pytest.mark.asyncio
async def test_a_closed_pull_request_stays_followed_and_its_reopening_is_reported():
    """With updates, a reported pull request that is closed without a merge
    stays in the cursor, with the time of its close. Listed open again it is
    reported as reopened and is followed as before: its new heads and its
    merge report. The time of a close names its record and the record of the
    reopening that ends it, so each close and each reopening has an id of its
    own. A comment reports nothing, on the closed pull request or after its
    reopening."""
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"], updates=True)
    cursor = await baseline(src)
    listed: dict[str, Any] = {"created": 2, "title": "Add a widget cache", "author": "bob"}
    records: list[SourceRecord] = []

    async def after(**changes: Any) -> tuple[list[str], list[Any] | None]:
        """What is reported once the listing shows *changes*, and how the
        cursor follows the pull request then."""
        nonlocal cursor, records
        listed.update(changes)
        fake.pulls[REPO] = [pull(12, **listed)]
        result = await src.fetch(cursor)
        cursor, records = result.next_cursor, result.records
        return [record.id for record in records], cursor_of(result)["repos"][REPO].get("prs", {}).get("12")

    head_a, head_b, head_c = SHA_A[:12], SHA_B[:12], "c" * 12
    assert await after() == ([f"{REPO}#12:opened:open"], [head_a, False, False])
    assert await after(state="closed", updated=4, closed=4) == (
        [f"{REPO}#12:closed:{at(4)}"], [head_a, False, False, at(4)],
    )
    # A comment on the closed pull request: its update time moves, its close does not.
    assert await after(updated=6) == ([], [head_a, False, False, at(4)])
    assert await after(state="open", updated=8) == ([f"{REPO}#12:reopened:{at(4)}"], [head_a, False, False])
    [reopened] = records
    assert await after(updated=10, sha=SHA_B) == ([f"{REPO}#12:updated:{SHA_B}"], [head_b, False, False])
    assert await after(state="closed", updated=12, closed=12) == (
        [f"{REPO}#12:closed:{at(12)}"], [head_b, False, False, at(12)],
    )
    # Reopened with a new head: one record, reopened, which carries the head.
    assert await after(state="open", updated=14, sha="c" * 40) == (
        [f"{REPO}#12:reopened:{at(12)}"], [head_c, False, False],
    )
    assert records[0].metadata["head_sha"] == "c" * 40
    assert await after(updated=16) == ([], [head_c, False, False])
    assert await after(state="closed", updated=18, closed=18, merged=18) == ([f"{REPO}#12:merged:merged"], None)
    assert "prs" not in json.loads(cursor)["repos"][REPO]

    # The first reopening, as the gateway builds it: named by the close that
    # it ends, and dated by the update that shows the pull request open.
    assert reopened.id == f"{REPO}#12:reopened:{at(4)}"
    assert (reopened.source, reopened.record_type) == ("github_repos", "github_repo_activity")
    assert reopened.summary == "[acme/widgets] PR #12 reopened: Add a widget cache"
    assert reopened.content == (
        "Repository: acme/widgets\n"
        "Type: PullRequest\n"
        "Event: reopened\n"
        "PR: #12 Add a widget cache\n"
        "Author: bob\n"
        "State: open\n"
        "Draft: false\n"
        "Base ← head: main ← bob:feature\n"
        "Created: 2026-10-27T10:02:00Z\n"
        "Updated: 2026-10-27T10:08:00Z\n"
        "URL: https://github.com/acme/widgets/pull/12"
    )
    assert reopened.timestamp == at(8)
    assert list(reopened.metadata.items()) == [
        ("repo_name", REPO), ("number", "12"), ("event", "reopened"), ("subject_type", "PullRequest"),
        ("author", "bob"), ("author_kind", "human"), ("head_sha", SHA_A),
        ("subject_url", "https://github.com/acme/widgets/pull/12"),
        ("repo_url", "https://github.com/acme/widgets"),
    ]


@pytest.mark.asyncio
async def test_a_draft_that_is_reopened_ready_for_review_gives_one_record():
    """Reopened comes before ready and updated: the record carries the head
    and the draft state as listed, and neither is reported later."""
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"], updates=True)
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1, draft=True)]
    cursor = (await src.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=1, updated=3, draft=True, state="closed")]
    cursor = (await src.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=1, updated=5, sha=SHA_B)]

    result = await src.fetch(cursor)

    [record] = result.records
    assert (record.id, record.timestamp) == (f"{REPO}#1:reopened:{at(3)}", at(5))
    assert "Draft: false" in record.content and record.metadata["head_sha"] == SHA_B
    assert cursor_of(result)["repos"][REPO]["prs"] == {"1": [SHA_B[:12], False, False]}
    fake.pulls[REPO] = [pull(1, created=1, updated=6, sha=SHA_B)]
    assert (await src.fetch(result.next_cursor)).records == []


@pytest.mark.asyncio
async def test_a_reopening_has_the_same_id_when_the_listing_is_read_again_after_a_comment():
    """The close that it ends names the record, not the update time, which a
    comment moves. So a run that starts again from the same cursor, because
    its records were not stored, does not give the reopening a second id."""
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"], updates=True)
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1)]
    cursor = (await src.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=1, updated=3, state="closed")]
    cursor = (await src.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=1, updated=5)]
    [first] = (await src.fetch(cursor)).records
    fake.pulls[REPO] = [pull(1, created=1, updated=7)]

    [again] = (await src.fetch(cursor)).records

    assert first.id == again.id == f"{REPO}#1:reopened:{at(3)}"
    assert (first.timestamp, again.timestamp) == (at(5), at(7))


@pytest.mark.asyncio
async def test_a_closed_pull_request_that_is_reopened_and_ends_again_before_the_next_listing():
    """One record per pull request and run. Reopened and merged since the
    last listing: merged, and it is followed no longer. Reopened and closed
    again: nothing, since its close is reported already; it stays followed,
    with the later close, which names its next reopening."""
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"], updates=True)
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=2), pull(2, created=2, sha=SHA_B)]
    cursor = (await src.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [
        pull(1, created=2, updated=4, state="closed"),
        pull(2, created=2, updated=4, state="closed", sha=SHA_B),
    ]
    closed = await src.fetch(cursor)
    assert [record.id for record in closed.records] == [f"{REPO}#1:closed:{at(4)}", f"{REPO}#2:closed:{at(4)}"]
    fake.pulls[REPO] = [
        pull(1, created=2, updated=6, state="closed", merged=6),
        pull(2, created=2, updated=7, state="closed", closed=6, sha=SHA_B),
    ]

    result = await src.fetch(closed.next_cursor)

    assert [record.id for record in result.records] == [f"{REPO}#1:merged:merged"]
    assert cursor_of(result)["repos"][REPO]["prs"] == {"2": [SHA_B[:12], False, False, at(6)]}
    fake.pulls[REPO][1] = pull(2, created=2, updated=9, sha=SHA_B)
    reopened = await src.fetch(result.next_cursor)
    assert [record.id for record in reopened.records] == [f"{REPO}#2:reopened:{at(6)}"]


@pytest.mark.asyncio
async def test_a_new_pull_request_first_seen_closed_is_opened_and_its_reopening_is_reported():
    """It stays followed as one that was closed later, here a draft, with the
    draft state as listed; one first seen merged does not (the next test)."""
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"], updates=True)
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1, updated=3, draft=True, state="closed", closed=2)]

    opened = await src.fetch(cursor)

    [record] = opened.records
    assert record.id == f"{REPO}#1:opened:open" and "State: closed" in record.content
    assert cursor_of(opened)["repos"][REPO]["prs"] == {"1": [SHA_A[:12], True, False, at(2)]}
    fake.pulls[REPO] = [pull(1, created=1, updated=4, draft=True)]
    reopened = await src.fetch(opened.next_cursor)
    assert [record.id for record in reopened.records] == [f"{REPO}#1:reopened:{at(2)}"]


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
async def test_without_updates_a_closed_pull_request_is_not_kept():
    """A reported pull request is followed only with updates, so without
    them nothing is kept when it is closed, or first seen closed, and its
    reopening reports nothing. Turning updates off forgets a closed one too."""
    fake = FakeGitHub()
    followed = source(fake, types=["pull_request"], updates=True)
    cursor = await baseline(followed)
    fake.pulls[REPO] = [pull(1, created=1)]
    cursor = (await followed.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=1, updated=3, state="closed")]
    cursor = (await followed.fetch(cursor)).next_cursor
    assert json.loads(cursor)["repos"][REPO]["prs"] == {"1": [SHA_A[:12], False, False, at(3)]}
    # 1 is reopened, and 2 is new and closed already.
    fake.pulls[REPO] = [pull(1, created=1, updated=5), pull(2, created=6, state="closed")]
    unfollowed = source(fake, types=["pull_request"])

    result = await unfollowed.fetch(cursor)

    assert [record.id for record in result.records] == [f"{REPO}#2:opened:open"]
    assert "prs" not in cursor_of(result)["repos"][REPO]
    fake.pulls[REPO] = [pull(1, created=1, updated=5), pull(2, created=6, updated=8)]
    assert (await unfollowed.fetch(result.next_cursor)).records == []


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
async def test_a_closed_pull_request_whose_author_is_denied_is_forgotten_and_its_reopening_reports_nothing():
    """The author filters judge a pull request that is kept closed as they
    judge any followed one: listed again, here reopened, while its author is
    denied, it is forgotten without a record, and stays so when the author is
    allowed again."""
    fake = FakeGitHub()
    followed = source(fake, types=["pull_request"], updates=True)
    cursor = await baseline(followed)
    fake.pulls[REPO] = [pull(1, created=1, author="alice")]
    cursor = (await followed.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=1, updated=3, author="alice", state="closed")]
    cursor = (await followed.fetch(cursor)).next_cursor
    assert json.loads(cursor)["repos"][REPO]["prs"] == {"1": [SHA_A[:12], False, False, at(3)]}
    fake.pulls[REPO] = [pull(1, created=1, updated=5, author="alice")]

    result = await source(fake, types=["pull_request"], updates=True, deny_authors=["alice"]).fetch(cursor)

    assert result.records == []
    assert "prs" not in cursor_of(result)["repos"][REPO]
    fake.pulls[REPO] = [pull(1, created=1, updated=7, author="alice", sha=SHA_B)]
    assert (await followed.fetch(result.next_cursor)).records == []


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


@pytest.mark.asyncio
@pytest.mark.parametrize(("cap", "kept"), [
    (3, ["1", "3", "4"]),  # one too many: the closed one that was listed longest ago
    (2, ["1", "4"]),       # two: both closed ones, although 1 was listed before them
    (1, ["4"]),            # three: then the open one that was listed longest ago
])
async def test_beyond_the_cap_the_closed_pull_requests_are_forgotten_before_the_others(monkeypatch, cap, kept):
    monkeypatch.setattr(module, "MAX_FINGERPRINTS", cap)
    # In the order in which they were last listed: 2 and 3 are closed.
    followed = {
        "1": [SHA_A[:12], False, False],
        "2": [SHA_A[:12], False, False, at(2)],
        "3": [SHA_A[:12], False, False, at(3)],
        "4": [SHA_A[:12], False, False],
    }
    cursor = json.dumps({"v": 1, "repos": {REPO: {"pulls": read(at(4)), "prs": followed}}})

    result = await source(FakeGitHub(), types=["pull_request"], updates=True).fetch(cursor)

    assert list(cursor_of(result)["repos"][REPO]["prs"]) == kept


@pytest.mark.asyncio
async def test_a_closed_pull_request_whose_time_is_over_is_forgotten_before_the_cap_counts(monkeypatch):
    """The cap counts what is left after the forgetting by age. 2 goes by its
    age, so three are left and 1 stays, which is closed too and was listed
    longest ago: the cap would take it first."""
    monkeypatch.setattr(module, "MAX_FINGERPRINTS", 3)
    followed = {
        "1": [SHA_A[:12], False, False, ago(days=1)],
        "2": [SHA_A[:12], False, False, ago(days=31)],
        "3": [SHA_A[:12], False, False],
        "4": [SHA_A[:12], False, False],
    }
    cursor = json.dumps({"v": 1, "repos": {REPO: {"pulls": read(at(0)), "prs": followed}}})

    result = await source(FakeGitHub(), types=["pull_request"], updates=True).fetch(cursor)

    assert list(cursor_of(result)["repos"][REPO]["prs"]) == ["1", "3", "4"]


@pytest.mark.asyncio
async def test_a_closed_pull_request_is_forgotten_thirty_days_after_its_close(caplog):
    """The thirty days count from the close: a comment on the closed pull
    request does not move them. The reopening of one that is forgotten
    reports nothing, as that of a pull request that was never followed. The
    run that forgets says so in the log, once."""
    caplog.set_level(logging.INFO, logger="nerve.sources.github_repos")
    forgets = (
        "acme/widgets no longer follows 1 closed pull request whose 30 days after the close are over, "
        "or whose close has no time"
    )
    fake = FakeGitHub()
    now = [NOW]
    src = source(fake, types=["pull_request"], updates=True, clock=lambda: now[0])
    cursor = await baseline(src)

    async def listed(minutes: int, item: dict[str, Any]) -> tuple[list[str], list[str]]:
        """What a listing of *item*, read *minutes* after the start, reports and follows."""
        nonlocal cursor
        fake.pulls[REPO] = [item]
        now[0] = START + timedelta(minutes=minutes)
        result = await src.fetch(cursor)
        cursor = result.next_cursor
        return [record.id for record in result.records], list(cursor_of(result)["repos"][REPO].get("prs", {}))

    await listed(62, pull(1, created=61))
    assert await listed(65, pull(1, created=61, updated=64, state="closed")) == (
        [f"{REPO}#1:closed:{at(64)}"], ["1"],
    )
    # Twenty days later, a comment on the closed pull request.
    commented = pull(1, created=61, updated=64 + 20 * DAY, state="closed", closed=64)
    assert await listed(65 + 20 * DAY, commented) == ([], ["1"])
    # Thirty days after the close, to the minute, it is still followed.
    assert await listed(64 + 30 * DAY, commented) == ([], ["1"])
    assert forgets not in caplog.text
    assert await listed(65 + 30 * DAY, commented) == ([], [])
    assert caplog.text.count(forgets) == 1

    assert await listed(70 + 30 * DAY, pull(1, created=61, updated=69 + 30 * DAY)) == ([], [])
    assert caplog.text.count(forgets) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("settings", [
    {"repos": [REPO, OTHER], "types": ["pull_request"]},  # the other repository's listing is read
    {"types": ["pull_request", "issue"]},                 # the repository's issues listing is read
])
async def test_a_run_that_cannot_read_the_pulls_listing_forgets_no_closed_pull_request(settings):
    """Only a run that read the pulls listing forgets a closed pull request
    by its age: a run whose listing failed cannot know whether it was
    reopened. Here it was, within its thirty days; the next run that reads
    the listing reports the reopening."""
    fake = FakeGitHub()
    now = [NOW]
    src = source(fake, updates=True, clock=lambda: now[0], **settings)
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=61)]
    now[0] = START + timedelta(minutes=62)
    cursor = (await src.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=61, updated=64, state="closed")]
    now[0] = START + timedelta(minutes=65)
    cursor = (await src.fetch(cursor)).next_cursor
    # Reopened half an hour before its thirty days end. The next run comes a
    # quarter of an hour after them, and cannot read the listing.
    fake.pulls[REPO] = [pull(1, created=61, updated=34 + 30 * DAY)]
    fake.failing = {f"{REPO} pulls"}
    now[0] = START + timedelta(minutes=79 + 30 * DAY)

    failed = await src.fetch(cursor)

    assert failed.records == []
    assert cursor_of(failed)["repos"][REPO]["prs"] == {"1": [SHA_A[:12], False, False, at(64)]}
    fake.failing = set()
    now[0] = START + timedelta(minutes=94 + 30 * DAY)
    result = await src.fetch(failed.next_cursor)
    assert [record.id for record in result.records] == [f"{REPO}#1:reopened:{at(64)}"]
    assert cursor_of(result)["repos"][REPO]["prs"] == {"1": [SHA_A[:12], False, False]}


@pytest.mark.asyncio
@pytest.mark.parametrize("days", [20, 28])
async def test_a_closed_pull_request_that_was_reopened_early_in_a_gap_is_reported_after_it(days):
    """A run on weekdays: the pull request is reopened on Friday, after that
    day's run, and Monday's run reads the listing against the last day. The
    listing shows the pull request open, so its reopening is reported, with
    its own time, although it is older than that day. For one that was closed
    twenty-eight days before Friday the thirty days are over on Monday: it is
    reported and followed all the same."""
    fake = FakeGitHub()
    now = [NOW]
    src = source(fake, types=["pull_request"], updates=True, clock=lambda: now[0])
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=61)]
    now[0] = START + timedelta(minutes=62)
    cursor = (await src.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=61, updated=64, state="closed")]
    now[0] = START + timedelta(minutes=65)
    cursor = (await src.fetch(cursor)).next_cursor
    friday = 65 + days * DAY
    now[0] = START + timedelta(minutes=friday)
    cursor = (await src.fetch(cursor)).next_cursor
    # Reopened six hours after Friday's run.
    fake.pulls[REPO] = [pull(1, created=61, updated=friday + 360)]
    now[0] = START + timedelta(minutes=friday + 3 * DAY)

    monday = await src.fetch(cursor)

    [record] = monday.records
    assert (record.id, record.timestamp) == (f"{REPO}#1:reopened:{at(64)}", at(friday + 360))
    assert cursor_of(monday)["repos"][REPO]["prs"] == {"1": [SHA_A[:12], False, False]}
    # It is followed as before: Tuesday's run reports a push of Monday.
    fake.pulls[REPO] = [pull(1, created=61, updated=friday + 3 * DAY + 300, sha=SHA_B)]
    now[0] = START + timedelta(minutes=friday + 4 * DAY)
    tuesday = await src.fetch(monday.next_cursor)
    assert [record.id for record in tuesday.records] == [f"{REPO}#1:updated:{SHA_B}"]


@pytest.mark.asyncio
async def test_after_a_gap_nothing_else_from_before_the_last_day_is_judged():
    """A closed pull request that the listing shows past its close is the one
    thing older than a day that a run after a gap judges. A followed pull
    request that got a new head before the last day reports nothing, and a
    pull request that was opened before it is not reported. A closed one that
    only got a comment before it is not judged either: it is closed by the
    close that the cursor keeps, and stays in its place in the cursor."""
    fake = FakeGitHub()
    now = [NOW]
    src = source(fake, types=["pull_request"], updates=True, clock=lambda: now[0])
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=61), pull(2, created=61), pull(4, created=61)]
    now[0] = START + timedelta(minutes=62)
    cursor = (await src.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [
        pull(1, created=61, updated=64, state="closed"), pull(2, created=61),
        pull(4, created=61, updated=64, state="closed"),
    ]
    now[0] = START + timedelta(minutes=65)
    cursor = (await src.fetch(cursor)).next_cursor
    assert list(json.loads(cursor)["repos"][REPO]["prs"]) == ["2", "1", "4"]
    # Four days without a run. In the first hours 1 is reopened, 2 gets a new
    # head, 3 is opened, and 4 gets a comment.
    fake.pulls[REPO] = [
        pull(1, created=61, updated=300), pull(2, created=61, updated=310, sha=SHA_B), pull(3, created=320),
        pull(4, created=61, updated=330, state="closed", closed=64),
    ]
    now[0] = START + timedelta(minutes=65 + 4 * DAY)

    result = await src.fetch(cursor)

    assert [record.id for record in result.records] == [f"{REPO}#1:reopened:{at(64)}"]
    # Only 1 was judged, and is the one listed last.
    assert list(cursor_of(result)["repos"][REPO]["prs"].items()) == [
        ("2", [SHA_A[:12], False, False]), ("4", [SHA_A[:12], False, False, at(64)]),
        ("1", [SHA_A[:12], False, False]),
    ]


@pytest.mark.asyncio
async def test_a_closed_pull_request_that_was_closed_again_early_in_a_gap_keeps_the_later_close():
    """Reopened and closed again while no run came: the listing shows it
    closed by another close than the one that the cursor keeps, from before
    the last day. Nothing is reported, and the cursor takes the later close,
    which is the true one: the thirty days count from it, and it names the
    next reopening."""
    fake = FakeGitHub()
    now = [NOW]
    src = source(fake, types=["pull_request"], updates=True, clock=lambda: now[0])
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=61)]
    now[0] = START + timedelta(minutes=62)
    cursor = (await src.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=61, updated=64, state="closed")]
    now[0] = START + timedelta(minutes=65)
    cursor = (await src.fetch(cursor)).next_cursor
    # Thirty-one days without a run. On the twentieth it is reopened and closed again.
    again = 64 + 20 * DAY
    fake.pulls[REPO] = [pull(1, created=61, updated=again, state="closed")]
    now[0] = START + timedelta(minutes=65 + 31 * DAY)

    result = await src.fetch(cursor)

    assert result.records == []
    assert cursor_of(result)["repos"][REPO]["prs"] == {"1": [SHA_A[:12], False, False, at(again)]}
    fake.pulls[REPO] = [pull(1, created=61, updated=70 + 31 * DAY)]
    now[0] = START + timedelta(minutes=75 + 31 * DAY)
    reopened = await src.fetch(result.next_cursor)
    assert [record.id for record in reopened.records] == [f"{REPO}#1:reopened:{at(again)}"]


@pytest.mark.asyncio
async def test_a_listing_that_lags_behind_a_close_reports_no_reopening():
    """A listing can show an earlier state than the listing before it. Here
    it shows the pull request open, as it was a minute before the close that
    the cursor keeps. That is no reopening: its update time is before that
    close, so it is not judged. The reopening that comes later is reported."""
    fake = FakeGitHub()
    src = source(fake, types=["pull_request"], updates=True)
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=1)]
    cursor = (await src.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=1, updated=5, state="closed")]
    closed = await src.fetch(cursor)
    assert [record.id for record in closed.records] == [f"{REPO}#1:closed:{at(5)}"]
    fake.pulls[REPO] = [pull(1, created=1, updated=4)]

    lagging = await src.fetch(closed.next_cursor)

    assert lagging.records == []
    assert cursor_of(lagging)["repos"][REPO]["prs"] == {"1": [SHA_A[:12], False, False, at(5)]}
    fake.pulls[REPO] = [pull(1, created=1, updated=9)]
    reopened = await src.fetch(lagging.next_cursor)
    assert [record.id for record in reopened.records] == [f"{REPO}#1:reopened:{at(5)}"]


@pytest.mark.asyncio
async def test_an_entry_whose_close_has_no_time_is_not_judged_from_before_the_cursor():
    """Without a time the close cannot be compared with the listing, so a
    listing that shows the pull request open from before its cursor reports
    nothing. Such an entry does not come from a run: the run that reports a
    close without a time forgets it."""
    fake = FakeGitHub()
    fake.pulls[REPO] = [pull(1, created=5, updated=20)]
    followed = {"1": [SHA_A[:12], False, False, "closed"]}
    cursor = json.dumps({"v": 1, "repos": {REPO: {"pulls": read(at(50)), "prs": followed}}})

    result = await source(fake, types=["pull_request"], updates=True).fetch(cursor)

    assert result.records == []
    assert "prs" not in cursor_of(result)["repos"][REPO]


@pytest.mark.asyncio
async def test_a_closed_pull_request_that_was_reopened_and_merged_early_in_a_gap_is_reported_as_merged():
    """The listing shows it closed, with a merge from before the last day.
    The watch said that it is closed, and that would stay its last record:
    the run after the gap reports the merge, with its own time, and the pull
    request is followed no longer."""
    fake = FakeGitHub()
    now = [NOW]
    src = source(fake, types=["pull_request"], updates=True, clock=lambda: now[0])
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=61)]
    now[0] = START + timedelta(minutes=62)
    cursor = (await src.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=61, updated=64, state="closed")]
    now[0] = START + timedelta(minutes=65)
    cursor = (await src.fetch(cursor)).next_cursor
    # Four days without a run. Two days before the next one it is reopened and merged.
    merged = 65 + 2 * DAY
    fake.pulls[REPO] = [pull(1, created=61, updated=merged, state="closed", merged=merged)]
    now[0] = START + timedelta(minutes=65 + 4 * DAY)

    result = await src.fetch(cursor)

    [record] = result.records
    assert (record.id, record.timestamp) == (f"{REPO}#1:merged:merged", at(merged))
    assert "prs" not in cursor_of(result)["repos"][REPO]


@pytest.mark.asyncio
async def test_a_merge_in_the_second_of_the_kept_close_is_judged_from_before_the_cursor():
    """Reopened and merged in the second in which it was closed: the listing
    shows the close that the cursor keeps, with a merge. It is merged all the
    same, so it is judged, and the merge is reported."""
    fake = FakeGitHub()
    fake.pulls[REPO] = [pull(1, created=5, updated=10, state="closed", merged=10)]
    followed = {"1": [SHA_A[:12], False, False, at(10)]}
    cursor = json.dumps({"v": 1, "repos": {REPO: {"pulls": read(at(50)), "prs": followed}}})

    result = await source(fake, types=["pull_request"], updates=True).fetch(cursor)

    assert [(record.id, record.timestamp) for record in result.records] == [(f"{REPO}#1:merged:merged", at(10))]
    assert "prs" not in cursor_of(result)["repos"][REPO]


@pytest.mark.asyncio
async def test_a_held_draft_that_was_reopened_ready_early_in_a_gap_is_reported_after_it():
    """A held draft that is kept closed is judged the same way when a listing
    shows it open, however old that is: no draft any more, it is reported as
    ready, with its own time."""
    fake = FakeGitHub()
    now = [NOW]
    src = source(fake, types=["pull_request"], include_drafts=False, clock=lambda: now[0])
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=61, draft=True)]
    now[0] = START + timedelta(minutes=62)
    cursor = (await src.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=61, updated=64, draft=True, state="closed")]
    now[0] = START + timedelta(minutes=65)
    cursor = (await src.fetch(cursor)).next_cursor
    assert json.loads(cursor)["repos"][REPO]["prs"] == {"1": [SHA_A[:12], True, True, at(64)]}
    # Four days without a run. In the first hours it is reopened and marked ready.
    fake.pulls[REPO] = [pull(1, created=61, updated=300)]
    now[0] = START + timedelta(minutes=65 + 4 * DAY)

    result = await src.fetch(cursor)

    [record] = result.records
    assert (record.id, record.timestamp) == (f"{REPO}#1:ready:{SHA_A}", at(300))
    assert "prs" not in cursor_of(result)["repos"][REPO]


@pytest.mark.asyncio
async def test_the_cap_applies_in_a_run_that_cannot_read_the_pulls_listing(monkeypatch):
    """The cap is about the size of the cursor, so it applies in every run,
    and takes the closed pull request that was listed longest ago. Ages are
    not judged in this run: with the listing read, 3 would go by its age and
    2 would stay."""
    monkeypatch.setattr(module, "MAX_FINGERPRINTS", 3)
    followed = {
        "1": [SHA_A[:12], False, False],
        "2": [SHA_A[:12], False, False, ago(days=1)],
        "3": [SHA_A[:12], False, False, ago(days=31)],
        "4": [SHA_A[:12], False, False],
    }
    cursor = json.dumps({"v": 1, "repos": {REPO: {
        "pulls": read(at(0)), "issues": read(at(0)), "prs": followed,
    }}})
    fake = FakeGitHub()
    fake.failing = {f"{REPO} pulls"}

    result = await source(fake, types=["pull_request", "issue"], updates=True).fetch(cursor)

    assert list(cursor_of(result)["repos"][REPO]["prs"]) == ["1", "3", "4"]


@pytest.mark.asyncio
async def test_a_reopening_from_before_the_cursor_is_judged_in_every_run_and_before_what_changed(monkeypatch):
    """Not only after a gap: a listing that was read a moment ago shows, from
    before its cursor, a closed pull request open. It is reported. The items
    are judged oldest first, so for the cap it was listed before the pull
    request that changed since the cursor, and goes first."""
    monkeypatch.setattr(module, "MAX_FINGERPRINTS", 1)
    fake = FakeGitHub()
    fake.pulls[REPO] = [pull(1, created=5, updated=20), pull(2, created=5, updated=55)]
    followed = {"1": [SHA_A[:12], False, False, at(10)], "2": [SHA_A[:12], False, False]}
    cursor = json.dumps({"v": 1, "repos": {REPO: {"pulls": read(at(50)), "prs": followed}}})

    result = await source(fake, types=["pull_request"], updates=True).fetch(cursor)

    assert [record.id for record in result.records] == [f"{REPO}#1:reopened:{at(10)}"]
    assert list(cursor_of(result)["repos"][REPO]["prs"]) == ["2"]


@pytest.mark.asyncio
async def test_the_items_from_before_the_cursor_are_judged_oldest_first(monkeypatch):
    """Two closed pull requests are open again, both from before the cursor.
    2 was reopened first, so it was listed longest ago, and the cap takes it,
    although the listing shows it after 1 and its number is the higher one."""
    monkeypatch.setattr(module, "MAX_FINGERPRINTS", 1)
    fake = FakeGitHub()
    fake.pulls[REPO] = [pull(1, created=5, updated=30), pull(2, created=5, updated=20)]
    followed = {"1": [SHA_A[:12], False, False, at(10)], "2": [SHA_A[:12], False, False, at(12)]}
    cursor = json.dumps({"v": 1, "repos": {REPO: {"pulls": read(at(50)), "prs": followed}}})

    result = await source(fake, types=["pull_request"], updates=True).fetch(cursor)

    assert [record.id for record in result.records] == [
        f"{REPO}#2:reopened:{at(12)}", f"{REPO}#1:reopened:{at(10)}",
    ]
    assert list(cursor_of(result)["repos"][REPO]["prs"]) == ["1"]


@pytest.mark.asyncio
async def test_a_closed_pull_request_that_a_run_lists_open_is_reported_however_long_ago_it_closed():
    """The listing is read before anything is forgotten. A run that comes
    more than thirty days after the close, and whose listing shows the pull
    request open again, still reports the reopening and follows it."""
    fake = FakeGitHub()
    now = [NOW]
    src = source(fake, types=["pull_request"], updates=True, clock=lambda: now[0])
    cursor = await baseline(src)
    fake.pulls[REPO] = [pull(1, created=61)]
    now[0] = START + timedelta(minutes=62)
    cursor = (await src.fetch(cursor)).next_cursor
    fake.pulls[REPO] = [pull(1, created=61, updated=64, state="closed")]
    now[0] = START + timedelta(minutes=65)
    cursor = (await src.fetch(cursor)).next_cursor
    # No run for thirty-one days. It is reopened an hour before the next one.
    fake.pulls[REPO] = [pull(1, created=61, updated=5 + 31 * DAY)]
    now[0] = START + timedelta(minutes=65 + 31 * DAY)

    result = await src.fetch(cursor)

    assert [record.id for record in result.records] == [f"{REPO}#1:reopened:{at(64)}"]
    assert cursor_of(result)["repos"][REPO]["prs"] == {"1": [SHA_A[:12], False, False]}


@pytest.mark.asyncio
async def test_only_a_closed_pull_request_is_forgotten_after_a_time():
    """Thirty days after the close, to the second, are not enough; a second
    more is. A held draft that is closed goes the same way, a close without a
    time counts as long ago, and an open pull request has no such time."""
    followed = {
        "1": [SHA_A[:12], False, False, ago(days=30)],
        "2": [SHA_A[:12], False, False, ago(days=30, seconds=1)],
        "3": [SHA_A[:12], True, True, ago(days=31)],
        "4": [SHA_A[:12], False, False, "closed"],
        "5": [SHA_A[:12], False, False],
    }
    cursor = json.dumps({"v": 1, "repos": {REPO: {"pulls": read(at(0)), "prs": followed}}})

    result = await source(FakeGitHub(), types=["pull_request"], updates=True).fetch(cursor)

    assert cursor_of(result)["repos"][REPO]["prs"] == {"1": followed["1"], "5": followed["5"]}


# ---------------------------------------------------------------------- #
#  Cursor upgrades and failures                                           #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_cursor_reads_a_followed_pull_request_with_and_without_the_close_of_a_closed_one():
    """Three elements for an open pull request, and a fourth, the time of its
    close, for one that is kept closed. An entry of another shape is not
    followed."""
    fake = FakeGitHub()
    followed = {
        "1": [SHA_A[:12], False, False],
        "2": [SHA_A[:12], False, False, at(-5)],
        "3": [SHA_A[:12], False, False, ""],
        "4": [SHA_A[:12], False, False, None],
        "5": [SHA_A[:12], False, False, 5],
        "6": [SHA_A[:12], False, False, at(-5), at(-5)],
        "7": [SHA_A[:12], False],
    }
    cursor = json.dumps({"v": 1, "repos": {REPO: {"pulls": read(at(-5)), "prs": followed}}})
    src = source(fake, types=["pull_request"], updates=True)

    result = await src.fetch(cursor)

    assert result.records == []
    assert cursor_of(result)["repos"][REPO]["prs"] == {"1": followed["1"], "2": followed["2"]}
    # The one read as open has a new head, and the one read as closed is open.
    fake.pulls[REPO] = [pull(1, created=-9, updated=3, sha=SHA_B), pull(2, created=-8, updated=4)]
    changed = await src.fetch(result.next_cursor)
    assert [record.id for record in changed.records] == [f"{REPO}#1:updated:{SHA_B}", f"{REPO}#2:reopened:{at(-5)}"]


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
        "enabled": True, "repos": [REPO], "types": ["release", "commit"], "include_bots": True,
        "include_drafts": False, "updates": True, "deny_authors": ["noisy-*"],
        "branches": {"ACME/Widgets": ["main", "release/1.2"]},
    }}})

    (runner,) = [r for r in build_source_runners(config, db=None) if r.job_id == "source:github_repos"]

    src = runner.source
    assert isinstance(src, GitHubReposSource) and src.push_fed is False
    assert (src._repos, src._types, src._include_bots, src._include_drafts, src._updates, src._deny_authors) == (
        [REPO], ("release",), True, False, True, ["noisy-*"],
    )
    assert (src._commits, src._branches) == (True, {REPO: ["main", "release/1.2"]})
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
