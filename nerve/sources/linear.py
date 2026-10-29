"""Linear source: the issue notifications of the user the API key belongs to.

The agent is a regular Linear user, a service account or a person, with a
personal API key (or an OAuth token). It is not a Linear agent app, so there
are no webhooks and no agent sessions: this source polls the user's
notifications on its schedule, and the agent writes back through Linear's MCP
server with the same key (see ``LinearSyncConfig.mcp_server``).

One GraphQL query per page, the user's ``notifications`` of the types that
``sync.linear.types`` asks for (:data:`nerve.config.LINEAR_TYPES`), created
since the cursor. Events (the record's ``Event:`` line and ``event``
metadata):

* ``assigned``: the issue was assigned to the user (``issueAssignedToYou``)
* ``mentioned``: the user was mentioned in an issue's description
  (``issueMention``) or in a comment (``issueCommentMention``)
* ``commented``, ``replied``: a new comment on an issue the user is
  subscribed to, or a reply in a thread (``issueNewComment``)

Linear sends a mention in a comment and the new comment as two notifications;
they are one record, the mention. Before anything is stored, a notification is
dropped that the user caused itself (its own comments, the issues it assigned
to itself), one whose actor is an app, a bot, an external user or Linear
itself (unless ``include_bots``; an assignment is reported whoever made it),
one whose actor matches a ``deny_actors`` glob, and one of a team that the
team lists leave out (the guardrail rules, which the runner applies again).

Nothing is marked read: the user's inbox in Linear stays as the person who
reads it left it, and a run that fails reports the same notifications again
on the next one. So the notifications are read whether or not they are read,
and the cursor is when they were created (``updatedAt`` moves when one is
read or e-mailed)::

    {"v": 1, "since": "2026-10-27T10:05:00.123Z", "at": ["<notification id>"],
     "listed_at": "2026-10-27T10:20:00Z", "viewer": "<user id>"}

``since`` is the newest ``createdAt`` seen, ``at`` the notifications created
in that millisecond (one created in it after the listing is new), and
``viewer`` the user the key belonged to. A run without a cursor, with one it
cannot read, or with a key of another user is the baseline: its cursor is set
and nothing is reported, so switching the source on does not fill the inbox
with history. Nothing older than a day is backfilled: a cursor last read more
than a day ago (:data:`STALE_AFTER`) is read as of a day and an hour ago
(:data:`STALE_GRACE`). A run that fails keeps its cursor.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx

from nerve.sources.base import Source
from nerve.sources.filters import FieldRule, InboxFilter, matches_any
from nerve.sources.models import FetchResult, SourceRecord

if TYPE_CHECKING:
    from nerve.config import LinearSyncConfig

logger = logging.getLogger(__name__)

SOURCE_NAME = "linear"
RECORD_TYPE = "linear_notification"
API_URL = "https://api.linear.app/graphql"

# Cap for description and comment text to keep records reasonable.
_MAX_BODY_CHARS = 4_000

PAGE_SIZE = 50
# Pages read per run. More is a backlog no five-minute schedule produces;
# the rest is left unread, with a warning.
MAX_PAGES = 10

_TIMEOUT_SECONDS = 30.0

_CURSOR_VERSION = 1

# As in the GitHub Repos source: nothing older than a day is backfilled, and
# the last day is, with an hour of grace for the scheduler's delay. The
# baseline reads the last STALE_GRACE too, to find where Linear's clock is.
STALE_AFTER = timedelta(hours=24)
STALE_GRACE = timedelta(hours=1)

# The event of each notification type this source asks for.
_EVENTS = {
    "issueAssignedToYou": "assigned",
    "issueMention": "mentioned",
    "issueCommentMention": "mentioned",
    "issueNewComment": "commented",
}

_QUERY = """
query Notifications($filter: NotificationFilter, $first: Int!, $after: String, $withViewer: Boolean!) {
  viewer @include(if: $withViewer) { id }
  notifications(filter: $filter, first: $first, after: $after, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id type createdAt
      ... on IssueNotification {
        isLinearActor
        actor { id name displayName email app }
        botActor { id name userDisplayName }
        externalUserActor { id name displayName email }
        team { key }
        issue {
          id identifier title url description priorityLabel
          state { name type } team { key } assignee { name displayName }
        }
        comment { id body url user { id } }
        parentComment { body user { name displayName } }
      }
    }
  }
}
"""


def linear_authorization(api_key: str) -> str:
    """The ``Authorization`` header of Linear's GraphQL API for *api_key*.

    A personal API key goes as it is, an OAuth access token as a bearer
    token. Linear's MCP server takes both as bearer tokens.
    """
    key = api_key.strip()
    if key.startswith("lin_api_") or key.lower().startswith("bearer "):
        return key
    return f"Bearer {key}"


# ---------------------------------------------------------------------- #
#  Guardrail                                                              #
# ---------------------------------------------------------------------- #


@dataclass
class _NotAssignmentsRule(FieldRule):
    """A rule that an assignment passes: ``include_bots`` is about comments
    and mentions, and an assignment is reported whoever made it."""

    def passes(self, record: SourceRecord) -> bool:
        if (record.metadata or {}).get("event") == "assigned":
            return True
        return super().passes(record)


# The actor kinds that ``include_bots`` lets in.
_NON_USER_KINDS = ["app", "bot", "external", "system"]


def linear_inbox_filter(settings: LinearSyncConfig) -> InboxFilter:
    """The inbox guardrail of the Linear source, from ``sync.linear``.

    The source applies these settings before anything is stored (with
    ``deny_actors`` on the display name too); the guardrail applies them again
    to what arrives, which catches the records fetched before a reload changed
    the settings.
    """
    from nerve.config import LINEAR_TYPES

    deny_actors = list(settings.deny_actors or [])
    rules = [
        FieldRule(
            field="team_key",
            allow=list(settings.allow_teams or []), deny=list(settings.deny_teams or []),
        ),
        FieldRule(field="actor", deny=deny_actors),
        FieldRule(field="actor_email", deny=deny_actors),
        FieldRule(
            field="notification_type",
            allow=[kind for name in settings.types or () for kind in LINEAR_TYPES.get(name, ())],
        ),
    ]
    if not settings.include_bots:
        rules.append(_NotAssignmentsRule(field="actor_kind", deny=list(_NON_USER_KINDS)))
    return InboxFilter(rules=rules)


# ---------------------------------------------------------------------- #
#  Cursor                                                                 #
# ---------------------------------------------------------------------- #


@dataclass
class _Cursor:
    since: str
    at: list[str] = field(default_factory=list)
    listed_at: str = ""
    viewer: str = ""

    def as_json(self) -> str:
        return json.dumps(
            {
                "v": _CURSOR_VERSION, "since": self.since, "at": self.at,
                "listed_at": self.listed_at, "viewer": self.viewer,
            },
            separators=(",", ":"),
        )


def _read_cursor(cursor: str | None) -> _Cursor | None:
    """The cursor in *cursor*, or ``None`` (a baseline) when there is none to read."""
    text = (cursor or "").strip()
    if not text:
        return None
    try:
        data = json.loads(text)
    except ValueError:
        data = None
    if (
        isinstance(data, dict)
        and data.get("v") == _CURSOR_VERSION
        and _when(data.get("since")) is not None
        and isinstance(data.get("at"), list)
        and all(isinstance(key, str) for key in data["at"])
        and isinstance(data.get("viewer"), str)
    ):
        listed_at = data.get("listed_at")
        return _Cursor(
            since=data["since"], at=list(data["at"]),
            listed_at=listed_at if isinstance(listed_at, str) else "", viewer=data["viewer"],
        )
    logger.warning("linear: unreadable cursor %.80r; taking a baseline", text)
    return None


def _when(value: object) -> datetime | None:
    """A Linear timestamp as an aware datetime, or ``None``."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _stamp(moment: datetime) -> str:
    """*moment* as Linear writes a time, to the millisecond."""
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _window(previous: _Cursor, now: datetime) -> _Cursor:
    """The cursor to read *previous* against when it was last read more than a day ago.

    What is new in the last day, and the hour of :data:`STALE_GRACE` before
    it, is reported, nothing older. Never earlier than *previous* itself, so
    nothing is reported twice.
    """
    listed = _when(previous.listed_at)
    if listed is not None and now - listed <= STALE_AFTER:
        return previous
    start = now - STALE_AFTER - STALE_GRACE
    if _when(previous.since) >= start:
        return previous
    logger.info(
        "linear: last read at %s, more than a day ago; reporting what is new in the last day",
        previous.listed_at or "an unknown time",
    )
    return _Cursor(since=_stamp(start), at=[], listed_at=previous.listed_at, viewer=previous.viewer)


# ---------------------------------------------------------------------- #
#  Records                                                                #
# ---------------------------------------------------------------------- #


@dataclass
class _Actor:
    id: str = ""
    name: str = ""
    display_name: str = ""
    email: str = ""
    kind: str = "system"   # user | app | bot | external | system

    @property
    def label(self) -> str:
        return self.name or self.display_name or "Someone"

    def names(self) -> list[str]:
        return [value for value in (self.name, self.display_name, self.email) if value]


def _dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _actor(node: dict[str, Any]) -> _Actor:
    """Who caused the notification, and what kind of actor that is."""
    user = _dict(node.get("actor"))
    if user:
        return _Actor(
            id=str(user.get("id") or ""), name=str(user.get("name") or ""),
            display_name=str(user.get("displayName") or ""), email=str(user.get("email") or ""),
            kind="app" if user.get("app") else "user",
        )
    bot = _dict(node.get("botActor"))
    if bot:
        return _Actor(
            id=str(bot.get("id") or ""), name=str(bot.get("name") or ""),
            display_name=str(bot.get("userDisplayName") or ""), kind="bot",
        )
    external = _dict(node.get("externalUserActor"))
    if external:
        return _Actor(
            id=str(external.get("id") or ""), name=str(external.get("name") or ""),
            display_name=str(external.get("displayName") or ""),
            email=str(external.get("email") or ""), kind="external",
        )
    return _Actor(name="Linear" if node.get("isLinearActor") else "")


def _event(node: dict[str, Any]) -> str:
    event = _EVENTS.get(str(node.get("type") or ""), "")
    if event == "commented" and _dict(node.get("parentComment")):
        return "replied"
    return event


def _section(title: str, body: object) -> str | None:
    """A capped text section of the record, or ``None`` without text."""
    text = body.strip() if isinstance(body, str) else ""
    if not text:
        return None
    capped = text[:_MAX_BODY_CHARS]
    if len(text) > _MAX_BODY_CHARS:
        capped += "\n[... truncated]"
    return f"\n--- {title} ---\n{capped}"


def _person(user: object) -> str:
    user = _dict(user)
    return str(user.get("name") or user.get("displayName") or "")


_SUMMARIES = {
    "assigned": "Assigned to you by {actor}: {title}",
    "mentioned": "{actor} mentioned you: {title}",
    "commented": "{actor} commented: {title}",
    "replied": "{actor} replied: {title}",
}


def notification_record(node: dict[str, Any]) -> SourceRecord:
    """The record of one issue notification, as the GraphQL query returns it.

    A notification about a comment is keyed by the comment, so that the
    mention in it and the new comment are one record; any other by the
    notification.
    """
    issue = _dict(node.get("issue"))
    comment = _dict(node.get("comment"))
    parent = _dict(node.get("parentComment"))
    state = _dict(issue.get("state"))
    identifier = str(issue.get("identifier") or "?")
    title = str(issue.get("title") or "?")
    team_key = str(_dict(node.get("team")).get("key") or _dict(issue.get("team")).get("key") or "")
    notification_type = str(node.get("type") or "")
    notification_id = str(node.get("id") or "")
    event = _event(node)
    actor = _actor(node)
    comment_id = str(comment.get("id") or "")
    issue_url = str(issue.get("url") or "")
    comment_url = str(comment.get("url") or "")
    assignee = _person(issue.get("assignee"))
    state_name = str(state.get("name") or "")
    state_type = str(state.get("type") or "")
    actor_line = actor.label + (f" <{actor.email}>" if actor.email else "")
    if actor.kind != "user":
        actor_line += f" ({actor.kind})"
    content = [
        f"Issue: {identifier} {title}",
        f"Event: {event}",
        f"Team: {team_key}" if team_key else None,
        f"State: {state_name}" + (f" ({state_type})" if state_type else "") if state_name else None,
        f"Priority: {issue['priorityLabel']}" if issue.get("priorityLabel") else None,
        f"Assignee: {assignee}" if assignee else None,
        f"Actor: {actor_line}",
        f"Issue URL: {issue_url}" if issue_url else None,
        f"Comment URL: {comment_url}" if comment_url else None,
    ]
    if comment:
        replied_to = _person(parent.get("user"))
        content += [
            _section(f"In reply to {replied_to}".strip(), parent.get("body")),
            _section("Comment", comment.get("body")),
        ]
    else:
        content.append(_section("Description", issue.get("description")))
    metadata = {
        "identifier": identifier,
        "issue_id": str(issue.get("id") or ""),
        "team_key": team_key,
        "event": event,
        "notification_type": notification_type,
        "notification_id": notification_id,
        "actor": actor.name or actor.display_name,
        "actor_email": actor.email,
        "actor_id": actor.id,
        "actor_kind": actor.kind,
        "state": state_name,
        "state_type": state_type,
        "assignee": assignee,
        "comment_id": comment_id,
        "issue_url": issue_url,
        "comment_url": comment_url,
        # The comment's page, else the issue's, which the Sources page links to.
        "subject_url": comment_url or issue_url,
    }
    key = f"comment:{comment_id}" if comment_id else f"notification:{notification_id}"
    return SourceRecord(
        id=f"{identifier}:{key}",
        source=SOURCE_NAME,
        record_type=RECORD_TYPE,
        summary=f"[{identifier}] " + _SUMMARIES.get(event, "{actor}: {title}").format(
            actor=actor.label, title=title,
        ),
        content="\n".join(line for line in content if line),
        timestamp=str(node.get("createdAt") or ""),
        metadata=metadata,
    )


# ---------------------------------------------------------------------- #
#  Source                                                                 #
# ---------------------------------------------------------------------- #


class LinearSource(Source):
    """Reports the Linear notifications of the user an API key belongs to.

    *config* holds the ``sync.linear`` settings: ``api_key``, ``types``,
    ``include_bots``, ``deny_actors``, ``allow_teams`` and ``deny_teams``; a
    setting left out takes its default. *transport* replaces the network (a
    test's :class:`httpx.MockTransport`), *clock* the time.
    """

    source_name = SOURCE_NAME

    def __init__(
        self,
        config: dict[str, Any] | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        from nerve.config import LINEAR_TYPES

        self._config = config or {}
        self._api_key = str(self._config.get("api_key") or "")
        chosen = self._config.get("types")
        if chosen is None:
            chosen = list(LINEAR_TYPES)
        elif isinstance(chosen, str):
            chosen = [chosen]
        self._notification_types = [
            kind for name in LINEAR_TYPES if name in chosen for kind in LINEAR_TYPES[name]
        ]
        self._include_bots = bool(self._config.get("include_bots", False))
        self._deny_actors = [str(p) for p in self._config.get("deny_actors") or []]
        self._allow_teams = [str(p) for p in self._config.get("allow_teams") or []]
        self._deny_teams = [str(p) for p in self._config.get("deny_teams") or []]
        self._clock = clock or (lambda: datetime.now(UTC))
        self._transport = transport

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        """Report the notifications created since *cursor*.

        Every notification since the cursor is read each run (up to
        :data:`MAX_PAGES`), so ``limit`` does not cap this source. Raises
        :class:`RuntimeError` when Linear cannot be read; the runner keeps the
        cursor then.
        """
        if not self._api_key or not self._notification_types:
            logger.warning("linear: no API key or no types configured — nothing to fetch")
            return FetchResult(records=[], next_cursor=cursor)

        now = self._clock()
        previous = _read_cursor(cursor)
        if previous is not None:
            previous = _window(previous, now)
        lower = previous.since if previous is not None else _stamp(now - STALE_GRACE)
        viewer, nodes = await self._list(lower)

        if previous is not None and previous.viewer != viewer:
            logger.info(
                "linear: the API key belongs to another user than the cursor's; taking a baseline",
            )
            previous = None
        cutoff = _when(lower)
        listed = sorted(
            (node for node in nodes if _when(node.get("createdAt")) is not None),
            key=lambda node: (_when(node["createdAt"]), str(node.get("id") or "")),
        )
        new = [] if previous is None else [
            node for node in listed
            if _when(node["createdAt"]) > cutoff
            or (_when(node["createdAt"]) == cutoff and str(node.get("id")) not in previous.at)
        ]

        since, at = lower, set(previous.at if previous is not None else ())
        if listed and _when(listed[-1]["createdAt"]) > cutoff:
            since, at = listed[-1]["createdAt"], set()
        newest = _when(since)
        at |= {str(node.get("id")) for node in listed if _when(node["createdAt"]) == newest}
        next_cursor = _Cursor(since=since, at=sorted(at), listed_at=_stamp(now), viewer=viewer)

        records: dict[str, SourceRecord] = {}
        for node in new:
            if not self._wanted(node, viewer):
                continue
            record = notification_record(node)
            known = records.get(record.id)
            # The mention in a comment wins over the new comment it is in.
            if known is None or known.metadata["event"] != "mentioned":
                records[record.id] = record
        return FetchResult(
            records=list(records.values()), next_cursor=next_cursor.as_json(), has_more=False,
        )

    def _wanted(self, node: dict[str, Any], viewer: str) -> bool:
        """Whether *node* may be reported: the self, bot, actor and team filters."""
        issue = _dict(node.get("issue"))
        if not issue or _event(node) == "":
            return False
        actor = _actor(node)
        comment_user = _dict(_dict(node.get("comment")).get("user"))
        if viewer and viewer in (actor.id, str(comment_user.get("id") or "")):
            return False
        if actor.kind != "user" and not self._include_bots and _event(node) != "assigned":
            return False
        if self._deny_actors and any(
            matches_any(name, self._deny_actors) for name in actor.names()
        ):
            return False
        team = str(_dict(node.get("team")).get("key") or _dict(issue.get("team")).get("key") or "")
        if self._deny_teams and matches_any(team, self._deny_teams):
            return False
        return not self._allow_teams or (bool(team) and matches_any(team, self._allow_teams))

    async def _list(self, since: str) -> tuple[str, list[dict[str, Any]]]:
        """The viewer's id and the notifications created at or after *since*."""
        variables: dict[str, Any] = {
            "filter": {
                "type": {"in": self._notification_types},
                "createdAt": {"gte": since},
            },
            "first": PAGE_SIZE,
            "after": None,
            "withViewer": True,
        }
        viewer = ""
        nodes: list[dict[str, Any]] = []
        async with httpx.AsyncClient(
            transport=self._transport, timeout=_TIMEOUT_SECONDS,
            headers={"Authorization": linear_authorization(self._api_key)},
        ) as client:
            for page in range(MAX_PAGES):
                data = await self._query(client, variables)
                if page == 0:
                    viewer = str(_dict(data.get("viewer")).get("id") or "")
                    if not viewer:
                        raise RuntimeError("Linear returned no viewer for the API key")
                connection = _dict(data.get("notifications"))
                nodes += [node for node in connection.get("nodes") or () if isinstance(node, dict)]
                info = _dict(connection.get("pageInfo"))
                if not info.get("hasNextPage") or not info.get("endCursor"):
                    return viewer, nodes
                variables = {**variables, "after": info["endCursor"], "withViewer": False}
        logger.warning(
            "linear: more than %d notifications since %s; reading the first %d, "
            "the rest are not reported",
            MAX_PAGES * PAGE_SIZE, since, MAX_PAGES * PAGE_SIZE,
        )
        return viewer, nodes

    @staticmethod
    async def _query(client: httpx.AsyncClient, variables: dict[str, Any]) -> dict[str, Any]:
        """One GraphQL request's ``data``. Raises :class:`RuntimeError` when it fails."""
        try:
            response = await client.post(API_URL, json={"query": _QUERY, "variables": variables})
        except httpx.TimeoutException:
            raise RuntimeError(f"Linear did not answer within {_TIMEOUT_SECONDS:g}s") from None
        except httpx.HTTPError as error:
            raise RuntimeError(f"Linear could not be reached: {error}") from None
        try:
            body = response.json()
        except ValueError:
            body = None
        errors = body.get("errors") if isinstance(body, dict) else None
        codes = {
            str(_dict(_dict(error).get("extensions")).get("code") or "")
            for error in errors or () if isinstance(error, dict)
        }
        if response.status_code == 429 or "RATELIMITED" in codes:
            raise RuntimeError("Linear rate limit reached; retrying on a later run")
        if response.status_code in (401, 403) or "AUTHENTICATION_ERROR" in codes:
            raise RuntimeError("Linear rejected sync.linear.api_key (authentication failed)")
        if errors:
            message = _dict(errors[0]).get("message") if isinstance(errors, list) else errors
            raise RuntimeError(f"Linear returned an error: {str(message)[:200]}")
        if response.status_code >= 400 or not isinstance(body, dict):
            raise RuntimeError(f"Linear answered HTTP {response.status_code}")
        data = body.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("Linear returned no data")
        return data
