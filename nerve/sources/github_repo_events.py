"""GitHub repo events source — one record per issue/PR event, with the real doer.

Where the ``github`` source reads notifications (one record per *thread*, with
no actor and no event id) and ``github_repos`` reports new issues and PRs, this
source reads a repo's issue-event log and emits one record per *event*: "alice
requested a review from you on PR #7", "bob assigned you issue #9". Each record
names exactly one login as the account that acted, so an ``allow_actors``
guardrail judges who did the thing — not who is involved in the thread.

That is the gap the notifications API cannot close: it merges every event on a
thread into one record, does not say which event caused an update, and carries
no actor. Events without a comment (assignments, review requests) can only be
attributed there by guessing from the thread's history.

API: ``GET /repos/{owner}/{repo}/issues/events`` — newest first, PRs included.

Who acted is NOT the ``actor`` field for every event type. On this endpoint an
``assigned`` event puts the *assignee* in ``actor`` and the account that acted
in ``assigner``; a ``mentioned`` event names the user who was mentioned, and
does not say who wrote the mention at all. So every supported event type maps
to the field that holds the account that acted (``_EVENT_SPECS``), and an event
whose field is empty is dropped rather than credited to a guess. Event types
not in the map are not supported.

Records hold only fields of the event itself — no issue title, body, state or
labels, and a URL built from the repo and number rather than read from the
issue. Those change after the event, and the inbox re-surfaces a stored record
whose content changes; an event record must stay byte-identical when it is
fetched again. It also keeps the issue author's text away from an agent that
consumes this source.

Cursor semantics: a JSON object mapping each repo to a position (a UTC
timestamp); a fetch reads the repo's events created after it. After a read, the
position becomes the newest event seen — but never later than the read's own
server time minus a 10-minute overlap. So the last 10 minutes are read again
next time and an event that the API lists late is still caught, while in a
quiet repo the position settles on the newest event and nothing is re-read.
Re-reading an event already stored is a no-op (same ``(source, id)``, same
content). A repo without a position — every repo on the first run, or one added
to the config later — starts at its newest event, so nothing that exists then
is emitted. A repo whose fetch fails keeps its position.

A poll that emitted nothing for a repo keeps the ETag of its first page, and
the next poll sends ``If-None-Match``. An unchanged first page answers
``304 Not Modified``, which costs no API rate limit. Each event embeds its
issue, so on a busy repo the first page changes often and most polls are full
requests.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any

from nerve.sources.base import Source
from nerve.sources.models import FetchResult, SourceRecord

logger = logging.getLogger(__name__)

SOURCE_NAME = "github_repo_events"

# Concurrent gh API calls — one per repo, bounded.
_MAX_CONCURRENT_FETCHES = 5

# GitHub's maximum page size for this endpoint.
_PER_PAGE = 100

# Pages read per repo per fetch. Past this, older events in the window are not
# read; a warning says so (raise the schedule frequency for a busy repo).
_MAX_PAGES = 10

# How long before a read an event may be created and still be listed only
# after it: the window of the last read that the next read covers again.
_OVERLAP = timedelta(minutes=10)


@dataclass(frozen=True)
class _EventSpec:
    """Where an event type keeps the account that acted, and its target."""

    doer_field: str
    target_field: str
    verb: str
    team_field: str = ""
    # For review events GitHub writes the doer to both ``review_requester`` and
    # ``actor`` (verified on live events, for removals too). An event where
    # they differ is not understood, so it is dropped.
    doer_is_actor: bool = False


# Supported event types. ``doer_field`` is the account that acted — read only
# from this field, never from ``actor`` as a fallback: for assignments ``actor``
# is the assignee.
_EVENT_SPECS: dict[str, _EventSpec] = {
    "review_requested": _EventSpec(
        doer_field="review_requester",
        target_field="requested_reviewer",
        team_field="requested_team",
        verb="requested a review from",
        doer_is_actor=True,
    ),
    "review_request_removed": _EventSpec(
        doer_field="review_requester",
        target_field="requested_reviewer",
        team_field="requested_team",
        verb="removed the review request for",
        doer_is_actor=True,
    ),
    "assigned": _EventSpec(
        doer_field="assigner", target_field="assignee", verb="assigned",
    ),
    "unassigned": _EventSpec(
        doer_field="assigner", target_field="assignee", verb="unassigned",
    ),
}

SUPPORTED_EVENTS: tuple[str, ...] = tuple(_EVENT_SPECS)
DEFAULT_EVENTS: tuple[str, ...] = ("review_requested", "assigned")


def _login_of(user: object) -> str:
    """``user["login"]`` of a GitHub user object, or ``""`` (null-safe)."""
    if not isinstance(user, dict):
        return ""
    login = user.get("login")
    return login if isinstance(login, str) else ""


def _created(event: dict) -> str:
    """An event's ``created_at`` in the position format, or ``""`` if unreadable.

    Normalized so that string comparison with positions is time comparison.
    """
    parsed = _parse_ts(event.get("created_at"))
    return _format_ts(parsed) if parsed is not None else ""


def _parse_ts(value: object) -> datetime | None:
    """An ISO 8601 timestamp as an aware datetime; a naive one is taken as UTC."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _format_ts(value: datetime) -> str:
    """The one timestamp format used for positions — GitHub's own (``...Z``)."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_cursor(cursor: str | None) -> dict[str, str]:
    """The per-repo positions; ``{}`` for a missing or unreadable cursor.

    Positions are normalized to ``_format_ts`` so they compare correctly, as
    strings, with GitHub's ``created_at`` values.
    """
    if not cursor:
        return {}
    try:
        data = json.loads(cursor)
    except (TypeError, ValueError):
        data = None
    if not isinstance(data, dict):
        logger.warning(
            "%s: unreadable cursor %r; every repo starts again after its newest event",
            SOURCE_NAME, cursor,
        )
        return {}
    positions: dict[str, str] = {}
    for repo, ts in data.items():
        parsed = _parse_ts(ts)
        if parsed is not None:
            positions[str(repo)] = _format_ts(parsed)
    return positions


@dataclass
class _Response:
    """One ``gh api -i`` call. ``status`` is ``None`` when no HTTP response came."""

    status: int | None
    data: Any = None
    etag: str | None = None
    date: datetime | None = None   # the server's Date header


@dataclass
class _RepoFetch:
    """Result of reading one repo's event log."""

    events: list[dict]
    ok: bool
    not_modified: bool = False
    etag: str | None = None
    date: datetime | None = None


class GitHubRepoEventsSource(Source):
    """Issue/PR events in a configured set of repos, one record per event."""

    source_name = SOURCE_NAME

    def __init__(self, config: dict[str, Any] | None = None):
        self._config = config or {}
        self._repos: list[str] = []
        for repo in self._config.get("repos", []):
            repo = str(repo or "").strip()
            if repo and repo not in self._repos:
                self._repos.append(repo)
        requested = self._config.get("events")
        if requested is None:
            requested = list(DEFAULT_EVENTS)
        self._events: list[str] = []
        for name in requested:
            name = str(name).strip()
            if name in _EVENT_SPECS:
                if name not in self._events:
                    self._events.append(name)
            elif name:
                logger.warning(
                    "%s: event type %r is not supported and is ignored "
                    "(supported: %s)", SOURCE_NAME, name, ", ".join(SUPPORTED_EVENTS),
                )
        self._only_targeting_me: bool = bool(
            self._config.get("only_targeting_me", True),
        )
        # The account the events are judged against. Set ``username`` when the
        # token cannot read ``/user`` (a GitHub App installation token).
        self._viewer_login_cache: str | None = (
            str(self._config.get("username") or "").strip() or None
        )
        # Per repo: the ETag of the last first page read, and the position that
        # read produced. The ETag is sent only while the cursor handed in still
        # has that position, and is kept only after a poll that emitted nothing
        # for the repo. Otherwise a failed persist (the runner does not store
        # the cursor) could be followed by a 304 that hides the records it
        # lost. In memory only: after a restart the first poll of each repo is
        # one full request.
        self._etags: dict[str, tuple[str, str]] = {}

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        """Fetch new events since each repo's position.

        A repo without a position (first run, or newly configured) only gets
        one and emits nothing.
        """
        if not self._repos:
            logger.warning("%s: no repos configured — nothing to fetch", SOURCE_NAME)
            return FetchResult(records=[], next_cursor=cursor)
        if not self._events:
            logger.warning("%s: no supported event types configured", SOURCE_NAME)
            return FetchResult(records=[], next_cursor=cursor)

        # The account's own login decides "targeting me" and drops the
        # account's own actions. Without it nothing can be judged: keep every
        # position where it is and try again next run.
        me = await self._viewer_login()
        if not me:
            logger.warning(
                "%s: could not read the authenticated GitHub login (set "
                "sync.%s.username if the token cannot read /user); nothing "
                "fetched this run", SOURCE_NAME, SOURCE_NAME,
            )
            return FetchResult(records=[], next_cursor=cursor)

        positions = _load_cursor(cursor)
        sem = asyncio.Semaphore(_MAX_CONCURRENT_FETCHES)
        fetches = await asyncio.gather(
            *(self._fetch_repo(repo, positions.get(repo), sem) for repo in self._repos),
            return_exceptions=True,
        )

        records: list[SourceRecord] = []
        next_positions: dict[str, str] = {}
        for repo, fetched in zip(self._repos, fetches):
            previous = positions.get(repo)
            if isinstance(fetched, BaseException) or not fetched.ok or fetched.not_modified:
                if isinstance(fetched, BaseException):
                    logger.warning("%s: fetch failed for %s: %s", SOURCE_NAME, repo, fetched)
                if previous:
                    next_positions[repo] = previous
                continue

            newest = max((_created(e) for e in fetched.events), default="")
            read_at = fetched.date or datetime.now(timezone.utc)
            emitted = 0
            if previous is None:
                next_positions[repo] = self._baseline(repo, newest, read_at, fetched)
            else:
                for event in fetched.events:
                    if _created(event) <= previous:
                        continue
                    record = self._event_to_record(repo, event, me)
                    if record is not None:
                        records.append(record)
                        emitted += 1
                # The newest event seen, held back to the read's time minus
                # the overlap, and never moved backwards.
                held = min(max(previous, newest), _format_ts(read_at - _OVERLAP))
                next_positions[repo] = max(previous, held)

            if fetched.etag and not emitted:
                self._etags[repo] = (fetched.etag, next_positions[repo])
            else:
                self._etags.pop(repo, None)

        records.sort(key=lambda r: (r.timestamp, r.id))
        next_cursor = json.dumps(next_positions, sort_keys=True) if next_positions else cursor
        return FetchResult(records=records, next_cursor=next_cursor, has_more=False)

    @staticmethod
    def _baseline(repo: str, newest: str, read_at: datetime, fetched: _RepoFetch) -> str:
        """The first position for a repo: nothing that exists now is emitted.

        The newest event, so the next read starts strictly after it. A repo
        with no events yet starts at the read's server time minus the overlap:
        no older event exists, and an event created just before the read but
        not yet listed is still caught.
        """
        if newest:
            position = newest
        else:
            position = _format_ts(read_at - _OVERLAP)
        logger.info(
            "%s: %s starts at %s (%d existing events not emitted)",
            SOURCE_NAME, repo, position, len(fetched.events),
        )
        return position

    async def _fetch_repo(
        self, repo: str, previous: str | None, sem: asyncio.Semaphore,
    ) -> _RepoFetch:
        """Read one repo's events, newest first, back to its position."""
        floor = previous
        base = f"repos/{repo}/issues/events?per_page={_PER_PAGE}"

        cached = self._etags.get(repo)
        send_etag = cached[0] if cached and previous is not None and cached[1] == previous else None

        async with sem:
            first = await self._gh_api_conditional(base, send_etag)
            if first.status == 304:
                return _RepoFetch(events=[], ok=True, not_modified=True)
            if first.status != 200 or not isinstance(first.data, list):
                return _RepoFetch(events=[], ok=False)

            events = [e for e in first.data if isinstance(e, dict)]
            page_events = events
            page = 1
            # A baseline needs only the newest event. Otherwise read on while a
            # full page still reaches above the floor.
            while (
                floor is not None
                and len(page_events) == _PER_PAGE
                and min(_created(e) for e in page_events) > floor
            ):
                if page >= _MAX_PAGES:
                    logger.warning(
                        "%s: %s has more than %d events since its position; "
                        "events created after %s and before %s are not read "
                        "(poll this repo more often)",
                        SOURCE_NAME, repo, _MAX_PAGES * _PER_PAGE,
                        floor, min(_created(e) for e in page_events),
                    )
                    break
                page += 1
                more = await self._gh_api_conditional(f"{base}&page={page}", None)
                if more.status != 200 or not isinstance(more.data, list):
                    # Emit nothing for this repo and keep its position: the
                    # next run reads the whole window again, gap included.
                    return _RepoFetch(events=[], ok=False)
                page_events = [e for e in more.data if isinstance(e, dict)]
                events.extend(page_events)
        return _RepoFetch(events=events, ok=True, etag=first.etag, date=first.date)

    async def _viewer_login(self) -> str:
        """Login of the authenticated account, or ``""`` when it can't be read.

        Cached after the first success; a failure is retried on the next call.
        """
        if self._viewer_login_cache is None:
            response = await self._gh_api_conditional("user", None)
            login = _login_of(response.data) if response.status == 200 else ""
            if not login:
                return ""
            self._viewer_login_cache = login
        return self._viewer_login_cache

    # ------------------------------------------------------------------
    # Formatting
    # ------------------------------------------------------------------

    def _event_to_record(
        self, repo: str, event: dict, me: str,
    ) -> SourceRecord | None:
        """Turn one event into a record, or ``None`` when it is not emitted."""
        event_type = event.get("event")
        if event_type not in self._events:
            return None
        spec = _EVENT_SPECS[event_type]

        event_id = event.get("id")
        created_at = _created(event)
        issue = event.get("issue")
        if not event_id or not created_at or not isinstance(issue, dict):
            return None
        number = issue.get("number")
        if not isinstance(number, int) or isinstance(number, bool):
            return None

        doer = _login_of(event.get(spec.doer_field))
        if not doer:
            # Unknown doer (deleted account, or a payload without the field):
            # never fall back to ``actor``, which is not the doer for every type.
            return None
        if spec.doer_is_actor and _login_of(event.get("actor")).lower() != doer.lower():
            return None
        if doer.lower() == me.lower():
            # The account's own actions are not requests made of it.
            return None

        target = _login_of(event.get(spec.target_field))
        target_type = "user"
        if not target and spec.team_field:
            team = event.get(spec.team_field)
            if isinstance(team, dict) and isinstance(team.get("slug"), str):
                target, target_type = team["slug"], "team"
        if not target:
            return None
        # A team request names the team, not its members: it never counts as
        # targeting the account, even when the account is in the team.
        if self._only_targeting_me and (
            target_type != "user" or target.lower() != me.lower()
        ):
            return None

        is_pr = "pull_request" in issue
        kind = "PullRequest" if is_pr else "Issue"
        kind_label = "PR" if is_pr else "issue"
        # Built, not read from the issue: html_url changes on a repo rename or
        # an issue transfer, which would re-surface the stored record.
        url = f"https://github.com/{repo}/{'pull' if is_pr else 'issues'}/{number}"
        target_label = f"team {target}" if target_type == "team" else target

        content = "\n".join([
            f"Repository: {repo}",
            f"Event: {event_type}",
            f"By: {doer}",
            f"Target: {target_label}",
            f"{'PR' if is_pr else 'Issue'}: #{number}",
            f"Created: {created_at}",
            f"URL: {url}",
        ])
        return SourceRecord(
            id=str(event_id),
            source=SOURCE_NAME,
            record_type="github_issue_event",
            summary=f"[{repo}] {doer} {spec.verb} {target_label} on {kind_label} #{number}",
            content=content,
            timestamp=created_at,
            metadata={
                # Keys aligned with the `github` source so the frontend
                # GitHubRenderer renders a repo card + "View on GitHub" link.
                "repo_name": repo,
                "repo_url": f"https://github.com/{repo}",
                "subject_type": kind,
                "subject_url": url,
                "reason": event_type,
                "event": event_type,
                "number": number,
                # The one login that acted (from the type's doer field, not
                # necessarily GitHub's `actor`) — what the guardrail matches.
                "actor": doer,
                "target": target,
                "target_type": target_type,
            },
        )

    # ------------------------------------------------------------------
    # gh
    # ------------------------------------------------------------------

    @staticmethod
    async def _gh_api_conditional(
        endpoint: str, etag: str | None, timeout: float = 30,
    ) -> _Response:
        """``gh api -i`` with an optional ``If-None-Match``.

        ``gh`` exits non-zero on a 304, so the status line — not the exit code —
        decides. The process is killed if the call times out or is cancelled.
        """
        args = ["gh", "api", "-i", endpoint]
        if etag:
            args += ["-H", f"If-None-Match: {etag}"]
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            logger.error("gh CLI not found — install gh for GitHub sync")
            return _Response(status=None)
        except Exception as e:
            logger.warning("gh api %s error: %s", endpoint, e)
            return _Response(status=None)

        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except BaseException as e:
            # Timeout or cancellation: never leave a gh process behind.
            if proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                await proc.wait()
            if isinstance(e, asyncio.TimeoutError):
                logger.warning("gh api %s timed out", endpoint)
                return _Response(status=None)
            raise

        status, headers, body = _split_http_response(stdout.decode(errors="replace"))
        date = None
        if headers.get("date"):
            try:
                date = parsedate_to_datetime(headers["date"])
            except (TypeError, ValueError):
                date = None
            if date is not None and date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)   # "-0000" parses naive
        if status is None:
            logger.warning(
                "gh api %s failed: %s", endpoint, stderr.decode(errors="replace")[:200],
            )
            return _Response(status=None)
        if status == 304:
            return _Response(status=304, etag=headers.get("etag"), date=date)
        if status != 200:
            logger.warning("gh api %s: HTTP %s", endpoint, status)
            return _Response(status=status, date=date)
        try:
            data = json.loads(body) if body.strip() else None
        except json.JSONDecodeError as e:
            logger.warning("Failed to parse gh output for %s: %s", endpoint, e)
            return _Response(status=None)
        return _Response(status=200, data=data, etag=headers.get("etag"), date=date)


def _split_http_response(text: str) -> tuple[int | None, dict[str, str], str]:
    """Split ``gh api -i`` output into status code, lower-cased headers, body."""
    normalized = text.replace("\r\n", "\n")
    head, sep, body = normalized.partition("\n\n")
    if not sep:
        head, body = normalized, ""
    lines = head.split("\n")
    status: int | None = None
    if lines and lines[0].startswith("HTTP/"):
        parts = lines[0].split()
        if len(parts) >= 2 and parts[1].isdigit():
            status = int(parts[1])
    headers: dict[str, str] = {}
    for line in lines[1:]:
        name, colon, value = line.partition(":")
        if colon:
            headers[name.strip().lower()] = value.strip()
    return status, headers, body
