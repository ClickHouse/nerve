"""GitHub Repos source: what is new in repositories that the agent follows.

Where the ``github`` source surfaces notifications involving *you* and the
``github_events`` source captures *your own* actions, this source watches a
configured set of repositories (``sync.github_repos.repos``) that the account
need not take part in, and reports new pull requests, issues and releases
there (``types``). Nothing is set on the GitHub account: no watch, no e-mail.

This module is the poll mode: the gh CLI, on the source's schedule. With
``sync.github.mode: hosted`` the channel gateway polls the same listings for
the agent and builds the same records, which
:class:`~nerve.sources.github_hosted.HostedGitHubSource` drains.

One request per repository and type, the most recently changed first:

* ``pull_request``: ``GET /repos/{repo}/pulls?state=all&sort=updated&direction=desc&per_page=100``
* ``issue``: ``GET /repos/{repo}/issues?state=all&sort=updated&direction=desc&per_page=100``;
  the issues endpoint lists the pull requests too, and they are skipped
* ``release``: ``GET /repos/{repo}/releases?per_page=30``; drafts are skipped

Events (the record's ``Event:`` line and ``event`` metadata):

* ``opened``: a pull request or an issue created since the last listing
* ``ready``: a draft marked ready for review. With ``include_drafts`` false
  a new draft is held and reported then, never as opened; with ``updates``,
  a reported draft too
* ``updated``, ``merged``, ``closed``: with ``updates``, a new head commit, a
  merge, or a close without one, of a pull request already reported. One
  record per pull request and run: merged or closed, else ready, else updated
* ``released``: a release published since the last listing

Comments, reviews and labels move ``updated_at`` and report nothing. Before
anything is reported or remembered, a pull request or an issue is dropped
whose author is a GitHub App or a ``[bot]`` account (unless
``include_bots``; a release is reported whoever published it, release
automation being the normal case), and any item whose author's login
matches a ``deny_authors`` glob (the guardrail rules).

The cursor is JSON: per repository, for each listing its newest
``updated_at`` (``published_at`` for releases), when it was read, and the keys
of the items created in that newest second; and the fingerprints of the pull
requests it follows (``[head commit, draft, held]``), a held draft or, with
``updates``, a reported pull request that is still open::

    {"v": 1, "repos": {"acme/widgets": {"pulls": {"since": "2026-10-27T10:05:00Z",
     "listed_at": "2026-10-27T10:20:00Z", "at": ["12"]},
     "prs": {"12": ["0123456789ab", false, false]}}}}

An item is new when it was created (published) after the last listing: later
than its cursor, or in the cursor's own second and not listed then, since
GitHub's times have a resolution of one second. At most
:data:`MAX_FINGERPRINTS` pull requests are followed per repository; the one
listed longest ago is forgotten first. A listing without a cursor is the
baseline: its cursor is set and nothing is reported, so switching the source
on, or adding a repository or a type, does not fill the inbox with history.
Nothing older than a day is backfilled: a listing last read more than a day
ago (:data:`STALE_AFTER`; a daily schedule, a long outage, the source switched
off for longer) is read as of a day and an hour ago (:data:`STALE_GRACE`, for
the scheduler's delay), so it reports what is new in the last day, with an
hour of grace, and nothing older, and the pull requests it follows stay
followed. A
listing that fails keeps its cursor. The earlier cursor of this source, one
ISO timestamp (the newest ``created_at`` seen), stands for every listing and
for when it was last read; a buffer position (digits), left by the hosted
mode, is no cursor.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from nerve.sources.base import Source
from nerve.sources.filters import FieldRule, InboxFilter, matches_any
from nerve.sources.models import FetchResult, SourceRecord

if TYPE_CHECKING:
    from nerve.config import GitHubReposSyncConfig

logger = logging.getLogger(__name__)

SOURCE_NAME = "github_repos"
# Every record of this source, in both modes.
RECORD_TYPE = "github_repo_activity"

# Cap for issue/PR/release body text to keep records reasonable.
_MAX_BODY_CHARS = 4_000

# Concurrent gh API calls, bounded.
_MAX_CONCURRENT_FETCHES = 5

# Pull requests followed per repository (held drafts, reported ones with
# updates); the one listed longest ago is forgotten first.
MAX_FINGERPRINTS = 1_000

# Head commit characters a fingerprint keeps: enough to tell two apart.
_FINGERPRINT_SHA_CHARS = 12

_CURSOR_VERSION = 1

# A listing last read longer ago than this is read as of this long ago, and
# STALE_GRACE more: nothing older than a day is backfilled (the horizon of the
# notifications source's first run too), and the last day is, with an hour of
# grace for the scheduler's delay. A daily check comes back a day and a moment
# after the last one, and an item created in that moment is not lost.
STALE_AFTER = timedelta(hours=24)
STALE_GRACE = timedelta(hours=1)

# What each type lists: its name in the cursor, and the endpoint.
_LISTINGS: dict[str, tuple[str, str]] = {
    "pull_request": (
        "pulls", "repos/{repo}/pulls?state=all&sort=updated&direction=desc&per_page=100",
    ),
    "issue": (
        "issues", "repos/{repo}/issues?state=all&sort=updated&direction=desc&per_page=100",
    ),
    "release": ("releases", "repos/{repo}/releases?per_page=30"),
}
_DEFAULT_TYPES = ("pull_request", "issue")

# The record's subject_type for each type.
SUBJECT_TYPES = {"pull_request": "PullRequest", "issue": "Issue", "release": "Release"}


@dataclass
class _PullRequestsAndIssuesRule(FieldRule):
    """A rule that a release passes: ``include_bots`` is about pull requests
    and issues, and a release is reported whoever published it."""

    def passes(self, record: SourceRecord) -> bool:
        if (record.metadata or {}).get("subject_type") == SUBJECT_TYPES["release"]:
            return True
        return super().passes(record)


def github_repos_inbox_filter(settings: GitHubReposSyncConfig) -> InboxFilter:
    """The inbox guardrail of the GitHub Repos source, from ``sync.github_repos``.

    The polling source and the gateway apply these settings before anything
    is stored; the guardrail applies them again to what arrives, as the
    GitHub guardrail does. It catches the records delivered before a reload
    changed the settings, and the ``deny_authors`` that the gateway was not
    sent (see :func:`nerve.channels.hosted.sources.github_repos_subscription`).
    """
    rules = [
        # A repository the watch no longer names: its records already on the way.
        FieldRule(field="repo_name", allow=list(settings.repos or [])),
        FieldRule(field="author", deny=list(settings.deny_authors or [])),
        FieldRule(
            field="subject_type",
            allow=[SUBJECT_TYPES[kind] for kind in settings.types or () if kind in SUBJECT_TYPES],
        ),
    ]
    if not settings.include_bots:
        rules.append(_PullRequestsAndIssuesRule(field="author_kind", deny=["bot"]))
    return InboxFilter(rules=rules)


# ---------------------------------------------------------------------- #
#  Cursor                                                                 #
# ---------------------------------------------------------------------- #


@dataclass
class _Listing:
    """What the cursor remembers of one listing of a repository.

    ``since`` is the newest time the listing showed (``updated_at``, or
    ``published_at`` for releases); ``""`` when it showed nothing, after which
    everything it shows is new. ``listed_at`` is when this source last read
    it. ``at`` holds the keys (the number, or the release id) of the items
    created in the ``since`` second: GitHub's times have a resolution of one
    second, so an item created in that second can come after the listing, and
    a later listing reports it unless its key is here. ``None`` when those
    items are unknown (the cursor of the earlier version of this source): then
    nothing created in that second is new.
    """

    since: str
    listed_at: str
    at: list[str] | None = None

    def as_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"since": self.since, "listed_at": self.listed_at}
        if self.at is not None:
            out["at"] = self.at
        return out


@dataclass
class _RepoState:
    """What the cursor remembers of one repository.

    Its listings by name (``pulls``, ``issues``, ``releases``), and in ``prs``
    the pull requests it follows: number → ``[head commit, draft, held]``, in
    the order in which they were last listed.
    """

    listings: dict[str, _Listing] = field(default_factory=dict)
    prs: dict[str, list[Any]] = field(default_factory=dict)

    def follow(self, number: str, head_sha: str, *, draft: bool, held: bool) -> None:
        """Remember a pull request, as the one listed last."""
        self.prs.pop(number, None)
        self.prs[number] = [head_sha[:_FINGERPRINT_SHA_CHARS], draft, held]

    def as_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {name: listing.as_json() for name, listing in self.listings.items()}
        if self.prs:
            out["prs"] = self.prs
        return out


def _read_listing(raw: object) -> _Listing | None:
    """One listing of the cursor, or ``None`` when it cannot be read.

    A listing that cannot be read takes a new baseline. One without a readable
    ``listed_at`` counts as read long ago, which comes to the same.
    """
    if not isinstance(raw, dict):
        return None
    since = raw.get("since")
    if not isinstance(since, str) or (since and _when(since) is None):
        return None
    at = raw.get("at")
    if at is not None and not (isinstance(at, list) and all(isinstance(key, str) for key in at)):
        return None
    listed_at = raw.get("listed_at")
    return _Listing(since=since, listed_at=listed_at if isinstance(listed_at, str) else "", at=at)


def _read_cursor(cursor: str | None) -> tuple[dict[str, _RepoState], str | None]:
    """The repository states in *cursor*, and the earlier source's timestamp.

    Anything that cannot be read is no cursor: a listing without one takes a
    baseline, which reports nothing.
    """
    text = (cursor or "").strip()
    if not text:
        return {}, None
    if text.isdigit():
        logger.info(
            "github_repos: cursor %s is a buffer position of the hosted mode; "
            "taking a baseline", text,
        )
        return {}, None
    if not text.startswith("{"):
        if _when(text) is not None:
            return {}, text
        logger.warning("github_repos: unreadable cursor %.80r; taking a baseline", text)
        return {}, None
    try:
        data = json.loads(text)
    except ValueError:
        logger.warning("github_repos: unreadable cursor %.80r; taking a baseline", text)
        return {}, None
    repos = data.get("repos") if isinstance(data, dict) else None
    states: dict[str, _RepoState] = {}
    for repo, raw in (repos.items() if isinstance(repos, dict) else ()):
        if not isinstance(raw, dict):
            continue
        state = _RepoState()
        for name, _ in _LISTINGS.values():
            listing = _read_listing(raw.get(name))
            if listing is not None:
                state.listings[name] = listing
        prs = raw.get("prs")
        for number, entry in (prs.items() if isinstance(prs, dict) else ()):
            if (
                isinstance(entry, list) and len(entry) == 3
                and isinstance(entry[0], str) and isinstance(entry[1], bool)
                and isinstance(entry[2], bool)
            ):
                state.prs[str(number)] = entry
        states[repo] = state
    return states, None


def _write_cursor(states: dict[str, _RepoState]) -> str:
    return json.dumps(
        {"v": _CURSOR_VERSION, "repos": {repo: state.as_json() for repo, state in states.items()}},
        separators=(",", ":"),
    )


def _when(value: object) -> datetime | None:
    """A GitHub timestamp as an aware datetime, or ``None``."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or text.isdigit():
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _newest(current: str | None, candidate: object) -> str | None:
    """The later of two GitHub timestamps, as written; unreadable ones lose."""
    when = _when(candidate)
    if when is None:
        return current
    if current is None or _when(current) is None or when > _when(current):
        return str(candidate)
    return current


def _stamp(now: datetime) -> str:
    """*now* as the cursor writes when a listing was read."""
    return now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stale(listing: _Listing, now: datetime) -> bool:
    """Whether *listing* was last read more than :data:`STALE_AFTER` before *now*."""
    when = _when(listing.listed_at)
    return when is None or now - when > STALE_AFTER


def _window(listing: _Listing, now: datetime) -> _Listing:
    """The cursor to read a stale *listing* against: the last day, with an hour of grace.

    A daily schedule comes back a day and a moment later, a weekday one after
    the weekend: what is new in the last day (and the hour of
    :data:`STALE_GRACE` before it, for the scheduler's delay) is reported,
    nothing older. The cursor is *listing*'s own when it falls within that
    window (its keys still name what it saw in its second); otherwise the
    window's start, with no keys. Never earlier than *listing*'s own cursor,
    so nothing is reported twice: nothing listed back then was created after
    that cursor.
    """
    # Cut to the second first, as the stored stamp is, so a cursor on exactly
    # that second keeps its keys (the gateway compares the same way).
    start = _stamp(now - STALE_AFTER - STALE_GRACE)
    since = _when(listing.since)
    if since is not None and since >= _when(start):
        return listing
    return _Listing(since=start, listed_at=listing.listed_at, at=[])


def _number_key(item: dict[str, Any]) -> str:
    return str(item.get("number"))


def _release_key(item: dict[str, Any]) -> str:
    return str(item.get("id"))


@dataclass
class _Scan:
    """One listing read against its cursor.

    ``changed`` holds the items listed at or after the cursor, oldest first:
    at, not only after, so that an item that changed in the cursor's own
    second after the last listing is not lost (one listed again changes
    nothing). At a baseline (``previous`` is ``None``) it is empty.
    ``listing`` is the listing's next cursor.
    """

    changed: list[dict[str, Any]]
    listing: _Listing
    previous: _Listing | None
    new_key: str
    key: Callable[[dict[str, Any]], str]

    def is_new(self, item: dict[str, Any]) -> bool:
        """Whether *item* was created (published) after the last listing:
        later than its cursor, or in the cursor's own second and not listed
        then."""
        when = _when(item.get(self.new_key))
        if when is None or self.previous is None:
            return False
        cutoff = _when(self.previous.since)
        if cutoff is None or when > cutoff:
            return True
        return (
            when == cutoff and self.previous.at is not None
            and self.key(item) not in self.previous.at
        )


def _scan(
    items: list[Any], previous: _Listing | None, now: datetime, *,
    cursor_key: str, new_key: str, key: Callable[[dict[str, Any]], str],
) -> _Scan:
    """Read a listing against its cursor, *previous* (``None``: a baseline).

    The next cursor is the newest *cursor_key* listed, with the keys of the
    items whose *new_key* falls in that second, and those of the cursor
    before it while the second stays the same.
    """
    listed = [
        item for item in items
        if isinstance(item, dict) and _when(item.get(cursor_key)) is not None
    ]
    newest = previous.since if previous is not None else None
    for item in listed:
        newest = _newest(newest, item.get(cursor_key))
    since = newest or ""
    boundary = _when(since)
    at: set[str] | None = {
        key(item) for item in listed
        if boundary is not None and _when(item.get(new_key)) == boundary
    }
    if previous is not None and since == previous.since:
        at = None if previous.at is None else at | set(previous.at)
    listing = _Listing(since=since, listed_at=_stamp(now), at=None if at is None else sorted(at))
    if previous is None:
        return _Scan([], listing, None, new_key, key)
    cutoff = _when(previous.since)
    changed = [
        item for item in listed
        if cutoff is None or _when(item.get(cursor_key)) >= cutoff
    ]
    changed.sort(key=lambda item: _when(item.get(cursor_key)))
    return _Scan(changed, listing, previous, new_key, key)


# ---------------------------------------------------------------------- #
#  Records                                                                #
# ---------------------------------------------------------------------- #


def _author(user: object) -> tuple[str, str]:
    """``(login, kind)`` of an item's author; kind is ``bot`` or ``human``."""
    if not isinstance(user, dict):
        return "", "human"
    login = str(user.get("login") or "")
    bot = user.get("type") == "Bot" or login.endswith("[bot]")
    return login, "bot" if bot else "human"


def _head_sha(item: dict[str, Any]) -> str:
    """A pull request's head commit, or ``""``."""
    head = item.get("head")
    return str(head.get("sha") or "") if isinstance(head, dict) else ""


def _description(body: object) -> str | None:
    """The record's description section, capped, or ``None`` without a body."""
    text = body if isinstance(body, str) else ""
    if not text:
        return None
    capped = text[:_MAX_BODY_CHARS]
    if len(text) > _MAX_BODY_CHARS:
        capped += "\n[... truncated]"
    return f"\n--- Description ---\n{capped}"


def _timestamp(item: dict[str, Any], key: str) -> str:
    """The event's time: *key*, else when the item was updated or created."""
    for name in (key, "updated_at", "created_at"):
        value = item.get(name)
        if isinstance(value, str) and value:
            return value
    return datetime.now(UTC).isoformat()


def _metadata(
    repo: str, item: dict[str, Any], number: str, event: str, subject_type: str,
    author: tuple[str, str], head_sha: str,
) -> dict[str, str]:
    """The record's metadata: the keys, values and order that the gateway sends."""
    return {
        "repo_name": repo,
        "number": number,
        "event": event,
        "subject_type": subject_type,
        "author": author[0],
        "author_kind": author[1],
        "head_sha": head_sha,
        # The item's page and the repository's, which the Sources page links to.
        "subject_url": str(item.get("html_url") or ""),
        "repo_url": f"https://github.com/{repo}",
    }


# The time each event is reported at, and the summary after "[repo] ".
_PR_EVENT_TIMES = {
    "opened": "created_at", "updated": "updated_at", "ready": "updated_at",
    "merged": "merged_at", "closed": "closed_at",
}
_PR_SUMMARIES = {
    "opened": "New PR #{number}: {title}",
    "updated": "PR #{number} updated: {title}",
    "ready": "PR #{number} ready for review: {title}",
    "merged": "PR #{number} merged: {title}",
    "closed": "PR #{number} closed: {title}",
}


def pull_request_record(repo: str, item: dict[str, Any], event: str) -> SourceRecord:
    """The record of *event* on a pull request of the pulls listing."""
    number = str(item.get("number"))
    title = item.get("title") or "?"
    author = _author(item.get("user"))
    base = item.get("base") if isinstance(item.get("base"), dict) else {}
    head = item.get("head") if isinstance(item.get("head"), dict) else {}
    head_sha = _head_sha(item)
    base_ref = str(base.get("ref") or "")
    head_name = str(head.get("label") or head.get("ref") or "")
    state = "merged" if item.get("merged_at") else str(item.get("state") or "")
    content = [
        f"Repository: {repo}",
        "Type: PullRequest",
        f"Event: {event}",
        f"PR: #{number} {title}",
        f"Author: {author[0]}" if author[0] else None,
        f"State: {state}" if state else None,
        f"Draft: {'true' if item.get('draft') else 'false'}",
        f"Base ← head: {base_ref} ← {head_name}" if base_ref and head_name else None,
        *_common_lines(item),
        _description(item.get("body")),
    ]
    fingerprint = {"opened": "open", "merged": "merged", "closed": "closed"}.get(event, head_sha)
    return SourceRecord(
        id=f"{repo}#{number}:{event}:{fingerprint}",
        source=SOURCE_NAME,
        record_type=RECORD_TYPE,
        summary=f"[{repo}] " + _PR_SUMMARIES[event].format(number=number, title=title),
        content="\n".join(line for line in content if line),
        timestamp=_timestamp(item, _PR_EVENT_TIMES[event]),
        metadata=_metadata(repo, item, number, event, "PullRequest", author, head_sha),
    )


def issue_record(repo: str, item: dict[str, Any]) -> SourceRecord:
    """The record of a new issue of the issues listing."""
    number = str(item.get("number"))
    title = item.get("title") or "?"
    author = _author(item.get("user"))
    state = str(item.get("state") or "")
    content = [
        f"Repository: {repo}",
        "Type: Issue",
        "Event: opened",
        f"Issue: #{number} {title}",
        f"Author: {author[0]}" if author[0] else None,
        f"State: {state}" if state else None,
        *_common_lines(item),
        _description(item.get("body")),
    ]
    return SourceRecord(
        id=f"{repo}#{number}:opened:open",
        source=SOURCE_NAME,
        record_type=RECORD_TYPE,
        summary=f"[{repo}] New Issue #{number}: {title}",
        content="\n".join(line for line in content if line),
        timestamp=_timestamp(item, "created_at"),
        metadata=_metadata(repo, item, number, "opened", "Issue", author, ""),
    )


def release_record(repo: str, item: dict[str, Any]) -> SourceRecord:
    """The record of a release published, from the releases listing."""
    tag = str(item.get("tag_name") or "")
    name = str(item.get("name") or "")
    named = f"{tag} {name}" if name and name != tag else tag
    author = _author(item.get("author"))
    content = [
        f"Repository: {repo}",
        "Type: Release",
        "Event: released",
        f"Release: {named}",
        f"Author: {author[0]}" if author[0] else None,
        f"Prerelease: {'true' if item.get('prerelease') else 'false'}",
        f"Created: {item['created_at']}" if item.get("created_at") else None,
        f"Published: {item['published_at']}" if item.get("published_at") else None,
        f"URL: {item.get('html_url') or ''}",
        _description(item.get("body")),
    ]
    return SourceRecord(
        id=f"{repo}#:released:{item.get('id')}",
        source=SOURCE_NAME,
        record_type=RECORD_TYPE,
        summary=f"[{repo}] New release {tag}" + (f": {name}" if name and name != tag else ""),
        content="\n".join(line for line in content if line),
        timestamp=_timestamp(item, "published_at"),
        metadata=_metadata(repo, item, "", "released", "Release", author, ""),
    )


def _common_lines(item: dict[str, Any]) -> list[str | None]:
    """The labels, the times and the URL of a pull request or an issue."""
    labels = [
        str(label["name"]) for label in item.get("labels") or ()
        if isinstance(label, dict) and label.get("name")
    ]
    return [
        f"Labels: {', '.join(labels)}" if labels else None,
        f"Created: {item['created_at']}" if item.get("created_at") else None,
        f"Updated: {item['updated_at']}" if item.get("updated_at") else None,
        f"URL: {item.get('html_url') or ''}",
    ]


# ---------------------------------------------------------------------- #
#  Source                                                                 #
# ---------------------------------------------------------------------- #


class GitHubReposSource(Source):
    """Reports what is new in a configured set of repositories, with the gh CLI.

    *config* holds the ``sync.github_repos`` settings: ``repos``, ``types``,
    ``include_bots``, ``include_drafts``, ``updates`` and ``deny_authors``;
    a setting left out takes its default.
    """

    source_name = SOURCE_NAME

    def __init__(
        self, config: dict[str, Any] | None = None, *, clock: Callable[[], datetime] | None = None,
    ):
        self._config = config or {}
        # Configured repos, e.g. ["acme/widgets", "other-org/other-repo"].
        self._repos: list[str] = [
            r.strip() for r in self._config.get("repos") or [] if r and r.strip()
        ]
        chosen = self._config.get("types")
        if chosen is None:
            chosen = _DEFAULT_TYPES
        elif isinstance(chosen, str):
            chosen = [chosen]
        self._types = tuple(kind for kind in _LISTINGS if kind in chosen)
        self._include_bots = bool(self._config.get("include_bots", False))
        self._include_drafts = bool(self._config.get("include_drafts", True))
        self._updates = bool(self._config.get("updates", False))
        self._deny_authors = [str(p) for p in self._config.get("deny_authors") or []]
        # When a listing is read, for its cursor (a test sets the time).
        self._clock = clock or (lambda: datetime.now(UTC))

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        """Report what changed in every repository since *cursor*.

        Every listing is read in full each run (one page), so ``limit`` does
        not cap this source. A listing that fails is left for the next run
        with its cursor; when every listing fails, the run fails.
        """
        if not self._repos or not self._types:
            logger.warning("github_repos: no repos or no types configured — nothing to fetch")
            return FetchResult(records=[], next_cursor=cursor)

        now = self._clock()
        states, earlier = _read_cursor(cursor)
        listings = [
            (repo, kind, *_LISTINGS[kind]) for repo in self._repos for kind in self._types
        ]
        sem = asyncio.Semaphore(_MAX_CONCURRENT_FETCHES)
        results = await asyncio.gather(
            *(self._list(endpoint.format(repo=repo), sem) for repo, _, _, endpoint in listings),
            return_exceptions=True,
        )
        listed = {(repo, kind): result for (repo, kind, _, _), result in zip(listings, results)}

        records: list[SourceRecord] = []
        failures: list[str] = []
        next_states: dict[str, _RepoState] = {}
        for repo in self._repos:
            old = states.get(repo)
            if old is None:
                # A repository new to the cursor takes a baseline, unless the
                # earlier version of this source left its one timestamp: it
                # stands for every listing, and for when it was last read.
                old = _RepoState(listings={
                    _LISTINGS[kind][0]: _Listing(since=earlier, listed_at=earlier)
                    for kind in self._types
                } if earlier else {})
            state = self._carried(old)
            for kind in self._types:
                name = _LISTINGS[kind][0]
                previous = old.listings.get(name)
                result = listed[(repo, kind)]
                if isinstance(result, BaseException):
                    failures.append(f"{repo} {name}: {result}")
                    if previous is not None:
                        state.listings[name] = previous
                    continue
                if previous is not None and _stale(previous, now):
                    # Nothing older than a day is backfilled, and the last day
                    # is: a daily schedule is always a moment past the day.
                    # The pull requests it follows stay followed.
                    logger.info(
                        "github_repos: %s %s was last read at %s, more than a day ago; "
                        "reporting what is new in the last day",
                        repo, name, previous.listed_at or "an unknown time",
                    )
                    previous = _window(previous, now)
                if kind == "pull_request":
                    records += self._pull_requests(repo, result, previous, state, now)
                elif kind == "issue":
                    records += self._issues(repo, result, previous, state, now)
                else:
                    records += self._releases(repo, result, previous, state, now)
            while len(state.prs) > MAX_FINGERPRINTS:
                state.prs.pop(next(iter(state.prs)))
            next_states[repo] = state

        if failures:
            if len(failures) == len(listings):
                raise RuntimeError(
                    f"every GitHub listing failed ({len(failures)}), e.g. {failures[0]}"
                )
            logger.warning(
                "github_repos: %d of %d listings failed and keep their cursor, e.g. %s",
                len(failures), len(listings), failures[0],
            )
        # Oldest first, for a natural reading order in the inbox.
        latest = datetime.max.replace(tzinfo=UTC)
        records.sort(key=lambda record: (_when(record.timestamp) or latest, record.id))
        return FetchResult(records=records, next_cursor=_write_cursor(next_states), has_more=False)

    def _carried(self, old: _RepoState) -> _RepoState:
        """A repository's state for this run, with the fingerprints it still needs.

        Held drafts stay; reported pull requests only with ``updates``, and
        none of them without the ``pull_request`` type.
        """
        state = _RepoState()
        if "pull_request" in self._types:
            state.prs = {
                number: entry for number, entry in old.prs.items()
                if entry[2] or self._updates
            }
        return state

    def _denied(self, login: str) -> bool:
        """Whether *login* matches a ``deny_authors`` glob."""
        return bool(self._deny_authors) and matches_any(login, self._deny_authors)

    def _wanted(self, author: tuple[str, str]) -> bool:
        """Whether a pull request or an issue of this author may be reported:
        the bot and author filters. A release meets the author filter only."""
        if author[1] == "bot" and not self._include_bots:
            return False
        return not self._denied(author[0])

    def _pull_requests(
        self, repo: str, items: list[Any], previous: _Listing | None, state: _RepoState,
        now: datetime,
    ) -> list[SourceRecord]:
        """The events of a pulls listing, following the pull requests that need it."""
        scan = _scan(
            items, previous, now, cursor_key="updated_at", new_key="created_at", key=_number_key,
        )
        state.listings["pulls"] = scan.listing
        records: list[SourceRecord] = []
        for item in scan.changed:
            number = item.get("number")
            if not isinstance(number, int):
                continue
            key = str(number)
            author = _author(item.get("user"))
            head_sha = _head_sha(item)
            draft = bool(item.get("draft"))
            closed = item.get("state") == "closed"
            followed = state.prs.get(key)
            if followed is None:
                if not scan.is_new(item) or not self._wanted(author):
                    continue
                if draft and not self._include_drafts:
                    if not closed:
                        state.follow(key, head_sha, draft=True, held=True)
                    continue
                records.append(pull_request_record(repo, item, "opened"))
                if self._updates and not closed:
                    state.follow(key, head_sha, draft=draft, held=False)
                continue
            if not self._wanted(author):
                del state.prs[key]
                continue
            known_sha, was_draft, held = followed
            if held:
                if closed:
                    del state.prs[key]
                elif draft:
                    state.follow(key, head_sha, draft=True, held=True)
                else:
                    records.append(pull_request_record(repo, item, "ready"))
                    if self._updates:
                        state.follow(key, head_sha, draft=False, held=False)
                    else:
                        del state.prs[key]
                continue
            # One record per pull request and run: merged or closed, else
            # ready, else updated. Back to draft reports nothing.
            if closed:
                event = "merged" if item.get("merged_at") else "closed"
                records.append(pull_request_record(repo, item, event))
                del state.prs[key]
                continue
            if was_draft and not draft:
                records.append(pull_request_record(repo, item, "ready"))
            elif head_sha and head_sha[:_FINGERPRINT_SHA_CHARS] != known_sha:
                records.append(pull_request_record(repo, item, "updated"))
            state.follow(key, head_sha, draft=draft, held=False)
        return records

    def _issues(
        self, repo: str, items: list[Any], previous: _Listing | None, state: _RepoState,
        now: datetime,
    ) -> list[SourceRecord]:
        """The issues opened since the last listing; its pull requests are skipped."""
        scan = _scan(
            items, previous, now, cursor_key="updated_at", new_key="created_at", key=_number_key,
        )
        state.listings["issues"] = scan.listing
        return [
            issue_record(repo, item) for item in scan.changed
            if "pull_request" not in item
            and isinstance(item.get("number"), int)
            and scan.is_new(item)
            and self._wanted(_author(item.get("user")))
        ]

    def _releases(
        self, repo: str, items: list[Any], previous: _Listing | None, state: _RepoState,
        now: datetime,
    ) -> list[SourceRecord]:
        """The releases published since the last listing; drafts are skipped.

        Whoever published a release, ``include_bots`` aside: release automation
        is the normal case. ``deny_authors`` still applies.
        """
        published = [item for item in items if isinstance(item, dict) and not item.get("draft")]
        scan = _scan(
            published, previous, now,
            cursor_key="published_at", new_key="published_at", key=_release_key,
        )
        state.listings["releases"] = scan.listing
        return [
            release_record(repo, item) for item in scan.changed
            if isinstance(item.get("id"), int)
            and scan.is_new(item)
            and not self._denied(_author(item.get("author"))[0])
        ]

    async def _list(self, endpoint: str, sem: asyncio.Semaphore) -> list[Any]:
        """One listing; raises when it cannot be read."""
        async with sem:
            data = await self._gh_api_get(endpoint)
        if not isinstance(data, list):
            raise TypeError(f"gh api {endpoint} did not return a list")
        return data

    @staticmethod
    async def _gh_api_get(endpoint: str, timeout: float = 30) -> Any:
        """``gh api <endpoint>``, parsed. Raises :class:`RuntimeError` when it fails."""
        try:
            proc = await asyncio.create_subprocess_exec(
                "gh", "api", endpoint,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as error:
            raise RuntimeError("gh CLI not found — install gh for the GitHub Repos source") from error
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(proc.wait(), timeout=5)
            raise RuntimeError(f"gh api {endpoint} timed out after {timeout:g}s") from None
        if proc.returncode != 0:
            detail = stderr.decode(errors="replace").strip()[:200]
            raise RuntimeError(f"gh api {endpoint} failed: {detail}")
        try:
            return json.loads(stdout.decode(errors="replace") or "null")
        except ValueError as error:
            raise RuntimeError(f"gh api {endpoint} returned no JSON: {error}") from None
