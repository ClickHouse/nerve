"""GitHub source — fetches notifications via the gh CLI.

Which threads the ``/notifications`` listing returns follows
``sync.github.inbox``: ``participating`` (the default) lists only the threads
the account takes part in; ``all`` lists the account's whole GitHub inbox as
its watch settings produce it, watched repositories and CI included, for the
reason guardrail to pick from.

Cursor semantics: ISO 8601 `updated_at` of the first notification in the last
listing (GitHub lists the most recently notified thread first).
On first run (no cursor), fetches from the last 24 hours.

Each notification is enriched with actual content from the subject (PR/issue),
the latest comment that triggered the notification, and for PRs the latest
review state (APPROVED, CHANGES_REQUESTED, etc.) plus any inline review
comments attached to specific code lines.

GitHub's ``reason`` belongs to the thread, not to the update: once the account
was mentioned anywhere in a thread, every later notification of that thread
says ``mention`` ("The reason remains as mention, regardless of whether you're
ever mentioned again", GitHub's REST documentation). So the record of a
``mention`` thread also says whether the update itself mentions the account
(the ``mention`` metadata key, ``new`` or ``old``; see
:meth:`GitHubSource._check_mention`).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from nerve.sources.base import Source
from nerve.sources.filters import FieldRule, InboxFilter
from nerve.sources.models import FetchResult, SourceRecord

if TYPE_CHECKING:
    from nerve.config import GitHubSyncConfig

logger = logging.getLogger(__name__)

# Cap for PR/issue body and comment text to keep records reasonable.
_MAX_BODY_CHARS = 4_000
_MAX_COMMENT_CHARS = 2_000

# Concurrent API calls for enrichment.
_MAX_CONCURRENT_FETCHES = 5

# CheckSuite notifications carry no ``subject.url``, so they can never be
# enriched — the branch in the title is the only clue about whose run it was:
#   "CI workflow run failed for main branch"
#   "CI workflow run, Attempt #2 failed for chore/drop-anyio-patch branch"
# A run on the default branch is an upstream sync or a schedule, never a PR.
_CI_BRANCH_RE = re.compile(r"\bfor (?P<branch>\S+) branch$")

# The record of a change on one of the agent's own open pull requests, which
# the channel gateway delivers in hosted mode (``sync.github.pr_watch``). The
# agent is always the author: its ``reason`` is ``author`` and its ``actors``
# are the reviewers and commenters.
PR_ACTIVITY_RECORD_TYPE = "github_pr_activity"

# Reasons whose record also carries the comments posted since the thread's
# previous update (see GitHubSource._enrich_since_previous_update).
_NEW_COMMENT_REASONS = ("mention", "assign", "review_requested", "team_mention")

# How far before the sync cursor the first look at a thread starts. GitHub
# stamps a thread's ``updated_at`` some twenty seconds after the comment that
# moved it (23 and 24 seconds on two measured threads), so the comment behind
# a notification that is new in this run can be older than the cursor.
_FIRST_LOOK_MARGIN = timedelta(minutes=2)

# Threads whose last look is remembered; the least recently looked at is
# forgotten first.
_REMEMBERED_THREADS = 2048

# Pages of 100 that one read of a comments listing follows.
_COMMENT_PAGES = 5

# Pages of 100 of a thread's mention log read, newest first, to find a mention
# of the account.
_MENTION_LOG_PAGES = 3

# GitHub writes the mention event of a text 1 to 3 seconds after the text
# (measured on 29 mentions). A text this close before an event is the text
# the event was written for.
_EVENT_LAG = timedelta(seconds=15)

# Markdown code. GitHub notifies nobody for a handle written inside it.
_CODE_RE = re.compile(r"```.*?```|~~~.*?~~~|`[^`\n]*`", re.DOTALL)

# https://api.github.com/repos/{owner}/{repo}/issues|pulls/{number}
_SUBJECT_URL_RE = re.compile(
    r"/repos/(?P<owner>[^/]+)/(?P<repo>[^/]+)/(?:issues|pulls)/(?P<number>\d+)$"
)

# The mention events of a thread, newest last, and for a pull request its
# latest reviews. GitHub writes a ``MentionedEvent`` (``actor`` is the account
# that WAS mentioned) for a mention in the description or in a comment, also
# when an edit adds it. It writes none for a mention in a review or in an
# inline review comment, so those texts are read instead.
_MENTION_LOG_QUERY = """
query($owner: String!, $repo: String!, $number: Int!, $before: String) {
  repository(owner: $owner, name: $repo) {
    issueOrPullRequest(number: $number) {
      ... on Issue {
        timelineItems(last: 100, before: $before, itemTypes: [MENTIONED_EVENT]) {
          pageInfo { hasPreviousPage startCursor }
          nodes { ... on MentionedEvent { createdAt actor { login } } }
        }
      }
      ... on PullRequest {
        timelineItems(last: 100, before: $before, itemTypes: [MENTIONED_EVENT]) {
          pageInfo { hasPreviousPage startCursor }
          nodes { ... on MentionedEvent { createdAt actor { login } } }
        }
        reviews(last: 50) { nodes { author { login } body submittedAt url } }
      }
    }
  }
}
"""


def _when(value: Any) -> datetime | None:
    """A GitHub timestamp as an aware datetime, or None when it is not one."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _stamp(moment: datetime) -> str:
    """*moment* the way GitHub writes a timestamp (UTC, ``Z`` suffix)."""
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _mention_re(login: str) -> re.Pattern[str]:
    """Matches ``@login`` as a whole handle, in any letter case.

    Not a longer handle (``@login-bot``, ``@loginx``), not a team of an
    organization with that name (``@login/team``) and not the end of an email
    address (``someone@login``).
    """
    return re.compile(
        rf"(?<![\w@/])@{re.escape(login)}(?!-?[A-Za-z0-9])(?!/[A-Za-z0-9])",
        re.IGNORECASE,
    )


def _mentions(pattern: re.Pattern[str], text: Any) -> bool:
    """Whether *text* mentions the handle of *pattern* outside Markdown code."""
    if not isinstance(text, str) or not text:
        return False
    return bool(pattern.search(_CODE_RE.sub(" ", text)))


def _comment_time(comment: dict[str, Any]) -> datetime | None:
    """When a comment was last written: its edit, or else its creation."""
    return _when(comment.get("updated_at")) or _when(comment.get("created_at"))


@dataclass
class _Run:
    """What one fetch run shares with the enrichment of its notifications."""

    # Login of the account gh acts as in this run; "" when it was not needed
    # or could not be read.
    login: str = ""
    # Where the look at a thread starts that the source has no memory of.
    first_look: datetime | None = None


@dataclass
class _Look:
    """The source's last look at one thread (see GitHubSource._look_start)."""

    updated_at: str   # the thread's ``updated_at`` at that look
    start: datetime   # what came after this moment was new for that look
    end: datetime     # the newest moment that look covered


def _ci_branch(subject_type: str, title: str) -> str:
    """Branch of a CI run, or ``""`` for anything else.

    Surfaced as the ``ci_branch`` metadata key so the inbox guardrail can deny
    runs on branches that aren't yours (see :mod:`nerve.sources.filters`).
    Empty for non-CheckSuite subjects and for titles we can't parse — a *deny*
    rule never matches the empty string, so those records pass untouched.
    """
    if subject_type != "CheckSuite":
        return ""
    match = _CI_BRANCH_RE.search(title or "")
    return match.group("branch") if match else ""


def _collect_actors(
    subject_user: str,
    assignees: list[str],
    comment: dict[str, Any] | None,
    latest_review: dict[str, Any] | None,
    inline_comments: list[dict[str, Any]],
    recent_comments: list[dict[str, Any]],
    assigner: str = "",
    mentioner: str = "",
) -> list[str]:
    """Every GitHub login involved in a notification, de-duplicated.

    Order-preserving (first occurrence wins) with case-insensitive de-dup;
    empty and placeholder ("?") logins are skipped. Surfaced as the ``actors``
    metadata key so the inbox guardrail can allow/deny a notification by who is
    involved (see :mod:`nerve.sources.filters`). The raw ``/notifications``
    payload carries no actor, but enrichment has already fetched these logins
    for the rendered content.

    ``assigner`` is the login that performed the assignment on a
    ``reason=assign`` notification (see
    :meth:`GitHubSource._enrich_assignment`). It is the one actor such a
    notification is *about*, and it appears nowhere else in the payload: an
    assignment carries no comment, so when the subject was authored by — and
    assigned to — the same account (e.g. a bot files an issue and a maintainer
    hands it back), every other candidate below is that same account. Without
    this, the notification has no third-party login at all and a non-empty
    ``allow_actors`` allowlist can never pass it.

    ``mentioner`` is the login whose text mentions the account in this update
    (see :meth:`GitHubSource._check_mention`). The record quotes that text, and
    its author can be absent from every other candidate: a bot is left out of
    the new comments, and a mention in a review comment is no comment at all.
    """
    candidates: list[str] = [subject_user, *assignees, assigner, mentioner]
    if comment:
        candidates.append(comment.get("user", ""))
    if latest_review:
        candidates.append(latest_review.get("user", ""))
    candidates.extend(ic.get("user", "") for ic in inline_comments)
    candidates.extend(rc.get("user", "") for rc in recent_comments)

    actors: list[str] = []
    seen: set[str] = set()
    for login in candidates:
        if not login or login == "?":
            continue
        key = login.lower()
        if key not in seen:
            seen.add(key)
            actors.append(login)
    return actors


def github_inbox_filter(settings: GitHubSyncConfig) -> InboxFilter:
    """The inbox guardrail of the GitHub source, from ``sync.github``.

    Restricts which repos (the ``repo_name`` metadata key), which GitHub
    actors (``actors``: every login involved in a record) and which reasons
    (``reason``) reach the inbox, and which CI branches (``ci_branch``) do
    not. The rules AND together; within each, deny wins and a non-empty allow
    list is fail-closed. ``ci_branch`` is deny-only on purpose: it is empty
    for everything except CheckSuite notifications, so an allow list there
    would fail closed and drop the whole feed.

    Only the repo rule applies to a ``github_pr_activity`` record. It is about
    the agent's own pull request, so its reason is always ``author`` and its
    actors are the reviewers and commenters: the actor, reason and CI branch
    rules, written for notifications, would drop every such record.

    The polling source and the hosted one share this filter, and hosted mode
    publishes the same patterns to the gateway, which applies them the same
    way.
    """
    own_pull_requests = (PR_ACTIVITY_RECORD_TYPE,)
    return InboxFilter(rules=[
        FieldRule(field="repo_name", allow=settings.allow_repos, deny=settings.deny_repos),
        FieldRule(
            field="actors", allow=settings.allow_actors, deny=settings.deny_actors,
            exempt_record_types=own_pull_requests,
        ),
        FieldRule(
            field="reason", allow=settings.allow_reasons, deny=settings.deny_reasons,
            exempt_record_types=own_pull_requests,
        ),
        FieldRule(
            field="ci_branch", deny=settings.deny_ci_branches,
            exempt_record_types=own_pull_requests,
        ),
    ])


def notification_record(notif: dict[str, Any], extra: dict[str, Any] | BaseException) -> SourceRecord:
    """The inbox record of one notification and its enrichment.

    *extra* is what :meth:`GitHubSource._enrich_notification` returned for
    the notification, or the exception it raised (``gather`` hands back a
    cancellation too); a failed enrichment gives a record built from the
    notification alone.
    """
    subject = notif.get("subject", {})
    repo = notif.get("repository", {})
    updated_at = notif.get("updated_at", "")
    reason = notif.get("reason", "")
    repo_name = repo.get("full_name", "?")
    subject_title = subject.get("title", "?")
    subject_type = subject.get("type", "")

    # Build enriched content
    if isinstance(extra, BaseException):
        logger.warning("Enrichment failed for %s: %s", notif.get("id"), extra)
        extra = {}

    html_url = extra.get("html_url") or repo.get("html_url", "")
    subject_body = extra.get("body", "")
    subject_state = extra.get("state", "")
    subject_user = extra.get("user", "")
    assignees = extra.get("assignees", [])
    labels = extra.get("labels", [])
    comment = extra.get("latest_comment")
    assigner = extra.get("assigner", "")
    # Whether this update mentions the account: {} when that was not checked,
    # so the record says what GitHub says and no more.
    mention = extra.get("mention") or {}
    mention_state = mention.get("state", "")
    mention_at = mention.get("at", "")
    mention_by = mention.get("by", "")
    mention_url = mention.get("url", "")
    mention_key = (mention_by, mention.get("created_at", ""))

    content_parts = [
        f"Repository: {repo_name}",
        f"Type: {subject_type}",
        f"Title: {subject_title}",
        f"Reason: {reason}",
        _mention_line(mention),
        f"State: {subject_state}" if subject_state else None,
        f"Author: {subject_user}" if subject_user else None,
        f"Assignees: {', '.join(assignees)}" if assignees else None,
        # An assignment carries no comment text, so without this the
        # record never says who asked for the work.
        f"Assigned by: {assigner}" if assigner else None,
        f"Labels: {', '.join(labels)}" if labels else None,
        f"Updated: {updated_at}",
        f"URL: {html_url}",
    ]

    # Add the PR/issue body
    if subject_body:
        body_text = subject_body[:_MAX_BODY_CHARS]
        if len(subject_body) > _MAX_BODY_CHARS:
            body_text += "\n[... truncated]"
        content_parts.append(f"\n--- Description ---\n{body_text}")

    # Add the latest comment that triggered the notification
    if comment:
        comment_body = comment.get("body", "")[:_MAX_COMMENT_CHARS]
        comment_user = comment.get("user", "?")
        comment_date = comment.get("created_at", "")
        content_parts.append(
            f"\n--- Latest Comment ({comment_user}, {comment_date}) ---\n"
            f"{comment_body}"
        )

    # Add the text that mentions the account, unless it is the latest comment
    # above. Its author can be a bot, which the new comments below leave out,
    # and it can be a review comment, which is no comment of the thread.
    mention_body = mention.get("body", "")
    latest_key = (comment.get("user", ""), comment.get("created_at", "")) if comment else None
    if mention_state == "new" and mention_body and mention_key != latest_key:
        content_parts.append(
            f"\n--- Mention ({mention_by or '?'}, {mention_at}) ---\n"
            f"{mention_body[:_MAX_COMMENT_CHARS]}"
        )

    # Add latest review state (APPROVED, CHANGES_REQUESTED, etc.)
    latest_review = extra.get("latest_review")
    if latest_review:
        reviewer = latest_review.get("user", "?")
        review_state = latest_review.get("state", "")
        review_body = latest_review.get("body", "")
        content_parts.append(
            f"\n--- Latest Review ({reviewer}) ---\n"
            f"State: {review_state}"
        )
        if review_body:
            content_parts.append(
                review_body[:_MAX_COMMENT_CHARS]
            )

    # Add inline review comments (code-level feedback)
    inline_comments = extra.get("review_inline_comments", [])
    if inline_comments:
        content_parts.append("\n--- Inline Review Comments ---")
        for ic in inline_comments:
            ic_user = ic.get("user", "?")
            ic_path = ic.get("path", "")
            ic_line = ic.get("line")
            ic_body = ic.get("body", "")
            loc = f"{ic_path}:{ic_line}" if ic_line else ic_path
            content_parts.append(
                f"\n{ic_user} on {loc}:\n{ic_body}"
            )

    # Add the human comments posted since the thread's previous update
    # (catches a comment buried by bot comments — see
    # _enrich_since_previous_update). The mention text is already above.
    recent_comments = extra.get("recent_comments", [])
    shown_comments = [
        rc for rc in recent_comments
        if (rc.get("user", ""), rc.get("created_at", "")) != mention_key
    ]
    if shown_comments:
        content_parts.append(
            "\n--- New Comments (human, newest first) ---"
        )
        for rc in shown_comments:
            rc_user = rc.get("user", "?")
            rc_date = rc.get("created_at", "")
            rc_body = rc.get("body", "")
            content_parts.append(
                f"\n{rc_user} ({rc_date}):\n{rc_body}"
            )

    # Surface every involved login so the inbox guardrail can
    # allow/deny by actor (the raw notification carries no actor).
    actors = _collect_actors(
        subject_user, assignees, comment, latest_review,
        inline_comments, recent_comments, assigner,
        mention_by if mention_state == "new" else "",
    )

    metadata: dict[str, Any] = {
        "reason": reason,
        "subject_type": subject_type,
        "subject_url": html_url,
        "repo_name": repo_name,
        "repo_url": repo.get("html_url", ""),
        "actors": actors,
        "ci_branch": _ci_branch(subject_type, subject_title),
    }
    # The summary line and the metadata reach a reader of the inbox as they
    # are, also when the content was condensed, so the answer is in both.
    if mention_state:
        metadata["mention"] = mention_state
        if mention_at:
            metadata["mention_at"] = mention_at
    if mention_state == "new":
        if mention_by:
            metadata["mention_by"] = mention_by
        if mention_url:
            metadata["mention_url"] = mention_url
    label = "update, no new mention" if mention_state == "old" else reason

    return SourceRecord(
        id=notif.get("id", ""),
        source="github",
        record_type="github_notification",
        summary=f"[{repo_name}] {subject_title} ({label})",
        content="\n".join(p for p in content_parts if p),
        timestamp=updated_at or datetime.now(timezone.utc).isoformat(),
        metadata=metadata,
    )


def _mention_line(mention: dict[str, Any]) -> str | None:
    """The content line that says whether the update mentions the account."""
    state = mention.get("state")
    at = mention.get("at", "")
    if state == "new":
        who = f" by {mention['by']}" if mention.get("by") else ""
        where = f", {mention['url']}" if mention.get("url") else ""
        return f"Mention: new{who} ({at}{where})"
    if state == "old":
        return (
            "Mention: none in this update. GitHub keeps the reason of a thread "
            f"at mention after an earlier one (the last was at {at})"
        )
    return None


class GitHubSource(Source):
    """GitHub notification source using the gh CLI.

    ``config["inbox"]`` is ``sync.github.inbox``: ``"all"`` lists the whole
    inbox; anything else, or no config, the participating threads only.
    """

    source_name = "github"

    def __init__(self, config: dict[str, Any] | None = None):
        self._config = config or {}
        # Set by every fetch run for the enrichment of its notifications.
        self._run = _Run()
        # Last look at each thread, by notification thread id. Kept in this
        # process only: after a restart the first look at a thread starts at
        # the sync cursor again (see _look_start).
        self._looks: OrderedDict[str, _Look] = OrderedDict()
        # The account the looks above belong to.
        self._looks_login = ""

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        """Fetch new notifications since cursor (ISO 8601 timestamp).

        On first run (cursor=None): fetches from the last 24 hours.
        """
        # `since` is not advanced past the cursor: a notification can land in the
        # cursor's own second. A re-listed unchanged thread is skipped by storage.
        # IMPORTANT: use Z suffix, not +00:00 — the `+` in a URL query
        # string is interpreted as a space, silently breaking the filter.
        since = None
        since_dt: datetime | None = None
        if cursor:
            try:
                if cursor.isdigit():
                    # Digits are the buffer position of the hosted source, not
                    # a time, though eight of them parse as a date.
                    raise ValueError(cursor)
                cursor_dt = datetime.fromisoformat(cursor.replace("Z", "+00:00"))
                since = cursor_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                since_dt = _when(since)
            except ValueError:
                # Not a timestamp, for example the buffer position that the
                # hosted source keeps under the same name. GitHub refuses such a
                # `since`, which would fail every run, so look back as on a
                # first run instead.
                logger.warning(
                    "GitHub cursor %r is not a timestamp; fetching the last 24 hours", cursor,
                )
        if since is None or since_dt is None:
            since_dt = datetime.now(timezone.utc) - timedelta(hours=24)
            since = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        self._run = _Run(first_look=since_dt - _FIRST_LOOK_MARGIN)

        try:
            # Query params go in the URL — -f flags would force POST
            # `all=true` lists read threads too: GitHub marks a thread read as
            # soon as a mail client renders the notification email with its
            # images, which can happen within seconds of the notification, so
            # the unread state says nothing about what this instance has seen.
            # Storage skips a re-listed thread whose stored record is
            # unchanged, which is why the record carries no read state. A
            # read thread that moved long after it was last read is left out
            # (see _own_update): that is the account's own merge or review.
            # `participating=true` (inbox "participating", the default) keeps
            # only the threads the account takes part in: it opened them, was
            # mentioned, assigned or asked to review, or commented. Inbox
            # "all" leaves it out, and GitHub lists the account's whole inbox
            # as its watch settings produce it: also `subscribed` threads of
            # watched repositories (new issues and pull requests) and
            # `ci_activity` for the account's own workflow runs. The
            # guardrail picks from that.
            participating = "" if self._config.get("inbox") == "all" else "&participating=true"
            endpoint = f"notifications?since={since}{participating}&all=true"
            # `since` filters on when a thread last notified, not on the
            # returned `updated_at`, so read every page of the window.
            proc = await asyncio.create_subprocess_exec(
                "gh", "api", endpoint,
                "--paginate", "--jq", ".[]",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)

            if proc.returncode != 0:
                logger.error("gh api notifications failed: %s", stderr.decode())
                return FetchResult(records=[], next_cursor=cursor)

            # gh prints one JSON value per line; `splitlines()` also splits on U+0085 in a string.
            listing = [
                json.loads(line)
                for line in stdout.decode().split("\n")
                if line.strip()
            ]
            notifications = [
                n for n in {n.get("id", ""): n for n in listing}.values()
                if not self._own_update(n)
            ]

            # Whose mentions to look for. Read in every run that has a mention
            # thread and never kept between runs, so the account that gh acts
            # as is always the one whose notifications were just listed: a
            # different account, or a renamed one, is used from the next run
            # on. Without a login no mention is checked in this run.
            if any(self._has_mention_reason(n) for n in notifications):
                self._run.login = await self._gh_login()
                if not self._run.login:
                    logger.warning(
                        "GitHub login of gh could not be read; "
                        "mentions are not checked in this run",
                    )
                elif self._run.login.lower() != self._looks_login.lower():
                    if self._looks_login:
                        logger.info(
                            "gh acts as %s now, was %s; forgetting %d thread looks",
                            self._run.login, self._looks_login, len(self._looks),
                        )
                        self._looks.clear()
                    self._looks_login = self._run.login

            # Enrich notifications with actual content in parallel
            sem = asyncio.Semaphore(_MAX_CONCURRENT_FETCHES)
            enrich_tasks = [
                self._enrich_notification(notif, sem)
                for notif in notifications
            ]
            enriched = await asyncio.gather(*enrich_tasks, return_exceptions=True)

            records = [
                notification_record(notif, extra)
                for notif, extra in zip(notifications, enriched)
            ]

            # The first item came from the first page, so no notification that arrives
            # while later pages are read is older than its `updated_at`.
            next_cursor = (listing[0].get("updated_at") or cursor) if listing else cursor
            return FetchResult(
                records=records,
                next_cursor=next_cursor,
                has_more=False,
            )

        except FileNotFoundError:
            logger.error("gh CLI not found — install gh for GitHub sync")
            return FetchResult(records=[], next_cursor=cursor)
        except asyncio.TimeoutError:
            logger.error("gh api notifications timed out")
            return FetchResult(records=[], next_cursor=cursor)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse gh output: %s", e)
            return FetchResult(records=[], next_cursor=cursor)
        except Exception as e:
            logger.error("GitHub error: %s", e)
            return FetchResult(records=[], next_cursor=cursor)

    @staticmethod
    def _own_update(notif: dict) -> bool:
        """Whether a thread moved without becoming unread, long after it was
        last read.

        GitHub moves the thread of the account's own merge or review about
        twenty seconds after the action and leaves it read, so such a move
        is no news. A thread read within a minute before its update is kept:
        a mail client's read of the notification email can be stamped up to
        a few seconds before GitHub stamps the update itself.
        """
        if notif.get("unread", True) or not notif.get("last_read_at"):
            return False
        try:
            updated = datetime.fromisoformat(str(notif.get("updated_at", "")))
            read = datetime.fromisoformat(str(notif["last_read_at"]))
            return updated - read > timedelta(seconds=60)
        except (ValueError, TypeError, OverflowError):
            return False

    # ------------------------------------------------------------------
    # Enrichment helpers
    # ------------------------------------------------------------------

    async def _enrich_notification(
        self, notif: dict, sem: asyncio.Semaphore,
    ) -> dict:
        """Fetch subject details and latest comment for a notification.

        Returns a dict with: html_url, body, state, user, assignees, labels,
        and optionally latest_comment {user, body, created_at}.
        """
        subject = notif.get("subject", {})
        subject_url = subject.get("url", "")
        comment_url = subject.get("latest_comment_url", "")
        result: dict[str, Any] = {}

        if not subject_url:
            return result

        async with sem:
            # Fetch subject (PR / issue)
            subject_data = await self._gh_api_get(subject_url)
            if subject_data:
                result["html_url"] = subject_data.get("html_url", "")
                result["body"] = subject_data.get("body", "") or ""
                result["state"] = subject_data.get("state", "")
                result["user"] = subject_data.get("user", {}).get("login", "")
                result["assignees"] = [
                    a.get("login", "") for a in subject_data.get("assignees", [])
                ]
                result["labels"] = [
                    lb.get("name", "") for lb in subject_data.get("labels", [])
                ]

            # Fetch latest comment if it's different from the subject URL
            # (same URL means the notification IS the subject creation, no
            # separate comment to fetch)
            if comment_url and comment_url != subject_url:
                comment_data = await self._gh_api_get(comment_url)
                if comment_data and isinstance(comment_data, dict):
                    result["latest_comment"] = {
                        "user": comment_data.get("user", {}).get("login", "?"),
                        "body": comment_data.get("body", ""),
                        "created_at": comment_data.get("created_at", ""),
                    }

            # For PRs: fetch latest review state and inline comments.
            # GitHub often sets latest_comment_url to null for review
            # submissions, leaving the notification with no comment content.
            # The review state (APPROVED, CHANGES_REQUESTED) and inline
            # comments are only available via separate API endpoints.
            if subject.get("type") == "PullRequest" and subject_url:
                await self._enrich_pr_reviews(subject_url, result)

            # Read what was posted since the thread's previous update: the
            # human comments (GitHub's latest_comment_url always points to the
            # most recent comment — when a bot, e.g. some-ci[bot], posts
            # after a human comment, that comment is lost) and, for a mention
            # thread, whether any of it mentions the account.
            reason = notif.get("reason", "")
            s_type = subject.get("type", "")
            if reason in _NEW_COMMENT_REASONS and s_type in ("PullRequest", "Issue"):
                await self._enrich_since_previous_update(
                    notif, subject_data if isinstance(subject_data, dict) else {}, result,
                )

            # For assignments: fetch who assigned it. The notification payload
            # names the assignee but never the assigner, and an assignment has
            # no comment body to fall back on.
            if reason == "assign" and s_type in ("PullRequest", "Issue"):
                await self._enrich_assignment(subject_url, s_type, result)

        return result

    async def _enrich_assignment(
        self, subject_url: str, subject_type: str, result: dict[str, Any],
    ) -> None:
        """Resolve who performed the assignment on a ``reason=assign`` thread.

        Mutates *result* in place, adding ``assigner`` when found.

        The assigner is the whole point of an assignment notification — it is
        the account requesting the work — yet neither ``/notifications`` nor the
        issue payload carries it, and an assignment has no comment text. It is
        only available from the issue's event history, so this costs one extra
        API call, taken only for ``reason=assign`` (a rare reason).

        Reads the *last* ``assigned`` event, which is the one that triggered the
        notification, and skips it when that assignee is no longer assigned — so
        a stale assigner from a since-reverted assignment is not surfaced as an
        actor. When the assignee list is empty or unknown the event is accepted
        anyway: assigning requires write or triage access, so the assigner is
        privileged by construction, and refusing would just reinstate the
        fail-closed drop this method exists to prevent.
        """
        # A PR's event history lives under /issues/{n}/, never /pulls/{n}/.
        if subject_type == "PullRequest":
            base = subject_url.replace("/pulls/", "/issues/")
        else:
            base = subject_url

        # /events, not /timeline: both carry `assigned` entries, but /events
        # excludes comments, so a busy thread is far less likely to push the
        # assignment out of the single page read here (measured on real threads:
        # 11 events vs 30 timeline entries, 7 vs 26). Neither endpoint offers a
        # reverse sort, and paginating an unbounded history on every assignment
        # is not worth it — past 100 entries `assigner` is simply absent, exactly
        # as before this method existed.
        events = await self._gh_api_get(f"{base}/events?per_page=100")
        if not isinstance(events, list) or not events:
            return

        current = {a.lower() for a in result.get("assignees", [])}

        for ev in reversed(events):
            if ev.get("event") != "assigned":
                continue
            assignee = (ev.get("assignee") or {}).get("login", "")
            if current and assignee.lower() not in current:
                continue
            # The two endpoints carrying `assigned` disagree about where the
            # acting login sits: /events puts it in `assigner` and the *assignee*
            # in `actor` (verified on two separate real assignments), while
            # /timeline puts it in `actor` and omits `assigner`. Read both, so
            # reading `.actor` alone can't silently yield the assignee, and so
            # this survives switching endpoint or GitHub aligning the payloads.
            login = (
                (ev.get("assigner") or {}).get("login", "")
                or (ev.get("actor") or {}).get("login", "")
            )
            if login:
                result["assigner"] = login
            # Stop at the triggering event whether or not it resolved. Falling
            # back to an older `assigned` event would credit this notification to
            # whoever assigned it *last* time — and that login is precisely the
            # kind likely to be on the allowlist, so an unresolvable assignment
            # would be admitted on the authority of someone who did not act. An
            # actor we cannot identify (deleted account, null `actor`) must stay
            # unidentified: no assigner, no pass.
            return

    async def _enrich_pr_reviews(
        self, pr_url: str, result: dict[str, Any],
    ) -> None:
        """Fetch the latest review state and inline comments for a PR.

        Mutates *result* in place, adding ``latest_review`` and optionally
        ``review_inline_comments``.
        """
        reviews_data = await self._gh_api_get(f"{pr_url}/reviews")
        if not isinstance(reviews_data, list) or not reviews_data:
            return

        # Filter out PENDING (draft) reviews — they have submitted_at=null
        # which would crash max() when comparing str with None.
        submitted = [r for r in reviews_data if r.get("state") != "PENDING"]
        if not submitted:
            return

        latest_review = max(
            submitted,
            key=lambda r: r.get("submitted_at") or "",
        )
        review_state = latest_review.get("state", "")
        result["latest_review"] = {
            "user": latest_review.get("user", {}).get("login", "?"),
            "state": review_state,
            "body": latest_review.get("body", ""),
            "submitted_at": latest_review.get("submitted_at", ""),
        }

        # When the review body is empty the actual feedback lives in
        # inline comments attached to specific code lines.
        review_id = latest_review.get("id")
        if review_id and not latest_review.get("body"):
            rc_data = await self._gh_api_get(
                f"{pr_url}/reviews/{review_id}/comments",
            )
            if isinstance(rc_data, list) and rc_data:
                result["review_inline_comments"] = [
                    {
                        "user": rc.get("user", {}).get("login", "?"),
                        "body": (rc.get("body", "") or "")[:_MAX_COMMENT_CHARS],
                        "path": rc.get("path", ""),
                        "line": rc.get("line"),
                        "created_at": rc.get("created_at", ""),
                    }
                    for rc in rc_data[:5]  # cap to keep payload reasonable
                ]

    async def _enrich_since_previous_update(
        self, notif: dict, subject_data: dict[str, Any], result: dict[str, Any],
    ) -> None:
        """Add what was posted since the thread's previous update.

        Mutates *result* in place: ``recent_comments`` (the human comments
        posted since then, newest first, without the already-fetched
        ``latest_comment``) and, for a ``mention`` thread whose answer is
        known, ``mention`` (see :meth:`_check_mention`).

        GitHub's ``latest_comment_url`` points to the *most recent* comment,
        which is often a bot (CI coverage, AI review, merge conflict warning).
        When a human writes and a bot comments seconds later, the human
        comment is buried and never appears in the enriched content.
        """
        subject = notif.get("subject", {})
        subject_url = subject.get("url", "")
        subject_type = subject.get("type", "")
        thread_id = str(notif.get("id", ""))
        updated_at = str(notif.get("updated_at", ""))

        start = self._look_start(thread_id, updated_at)
        comments = await self._comments_since(subject_url, subject_type, start)
        complete = comments is not None
        covered: list[datetime | None] = [_when(updated_at)]
        covered += [_comment_time(c) for c in comments or []]
        self._attach_new_comments(comments or [], result)

        if notif.get("reason") == "mention":
            checked, seen = await self._check_mention(
                subject_url, subject_type, subject_data, comments or [], start, result,
            )
            complete = complete and checked
            covered += seen

        self._remember_look(thread_id, updated_at, start, covered if complete else [])

    def _look_start(self, thread_id: str, updated_at: str) -> datetime:
        """The moment after which a comment or a mention is new for this update.

        It is the end of the source's previous look at the thread. A thread
        that is listed again with the same ``updated_at`` gets the start that
        its look had the first time: its record comes out the same and storage
        skips it (the cursor's own second is listed again in every run). A
        thread the source has no look of, which is every thread after a
        restart, starts shortly before the sync cursor: what was posted since
        the last run.
        """
        look = self._looks.get(thread_id)
        if look is not None:
            return look.start if look.updated_at == updated_at else look.end
        if self._run.first_look is not None:
            return self._run.first_look
        # No run set a start: the enrichment was called on its own.
        updated = _when(updated_at) or datetime.now(timezone.utc)
        return updated - _FIRST_LOOK_MARGIN

    def _remember_look(
        self,
        thread_id: str,
        updated_at: str,
        start: datetime,
        covered: list[datetime | None],
    ) -> None:
        """Keep the look at a thread for the thread's next update.

        *covered* holds the moments of everything this look read. It is empty
        when a read failed, and the look of the next update then repeats the
        range instead of skipping what this one could not see.
        """
        if not thread_id:
            return
        previous = self._looks.get(thread_id)
        same_update = previous is not None and previous.updated_at == updated_at
        end = previous.end if previous is not None and same_update else start
        for moment in covered:
            if moment is not None and moment > end:
                end = moment
        self._looks[thread_id] = _Look(updated_at=updated_at, start=start, end=end)
        self._looks.move_to_end(thread_id)
        while len(self._looks) > _REMEMBERED_THREADS:
            self._looks.popitem(last=False)

    async def _comments_since(
        self, subject_url: str, subject_type: str, start: datetime,
    ) -> list[dict[str, Any]] | None:
        """The thread's comments created or edited after *start*, oldest first.

        None when a read failed, or when the listing is longer than the pages
        followed: nothing is concluded from a part of it.

        The listing of one thread's comments takes ``since`` but no sort
        order, and returns the OLDEST first (measured: with
        ``sort=created&direction=desc`` it returned the ten oldest comments
        of a long thread).
        """
        # PR issue-comments live under /issues/{n}/comments, not /pulls/
        if subject_type == "PullRequest":
            comments_url = subject_url.replace("/pulls/", "/issues/") + "/comments"
        else:
            comments_url = subject_url + "/comments"

        listed: list[dict[str, Any]] = []
        for page in range(1, _COMMENT_PAGES + 1):
            page_arg = f"&page={page}" if page > 1 else ""
            data = await self._gh_api_get(
                f"{comments_url}?since={_stamp(start)}&per_page=100{page_arg}",
            )
            if not isinstance(data, list):
                return None
            listed.extend(c for c in data if isinstance(c, dict))
            if len(data) < 100:
                break
        else:
            return None
        return [c for c in listed if (_comment_time(c) or start) > start]

    @staticmethod
    def _attach_new_comments(
        comments: list[dict[str, Any]], result: dict[str, Any],
    ) -> None:
        """Set ``recent_comments``: the human ones of *comments*, newest first."""
        # Deduplicate against the latest_comment already in result
        existing = result.get("latest_comment") or {}
        existing_key = (existing.get("user", ""), existing.get("created_at", ""))

        humans: list[dict[str, str]] = []
        newest_first = sorted(
            comments, key=lambda c: str(c.get("created_at") or ""), reverse=True,
        )
        for c in newest_first:
            user = (c.get("user") or {}).get("login", "")
            body = (c.get("body", "") or "")
            created = c.get("created_at", "")

            # Skip bots
            if user.endswith("[bot]") or user.endswith("-bot"):
                continue

            # Skip if identical to the already-fetched latest_comment
            if (user, created) == existing_key:
                continue

            humans.append({
                "user": user,
                "body": body[:_MAX_COMMENT_CHARS],
                "created_at": created,
            })

            # Cap at 5 human comments — enough context without bloat
            if len(humans) >= 5:
                break

        if humans:
            result["recent_comments"] = humans

    async def _check_mention(
        self,
        subject_url: str,
        subject_type: str,
        subject_data: dict[str, Any],
        comments: list[dict[str, Any]],
        start: datetime,
        result: dict[str, Any],
    ) -> tuple[bool, list[datetime | None]]:
        """Say whether the update of a ``mention`` thread mentions the account.

        Sets ``result["mention"]`` when the answer is known:

        * ``new``: the account is mentioned in something posted after *start*
          (the thread's previous update), with who wrote it and where when
          this look read that text.
        * ``old``: nothing posted after *start* mentions the account, and an
          earlier mention explains GitHub's reason.

        Left unset otherwise, and the record then says ``mention`` as GitHub
        does: when the login is not known, when a read failed, and when no
        mention of the account was found at all (GitHub knows of one that
        this look cannot see). A record is moved away from ``mention`` on
        evidence only.

        Where a mention is looked for, one place per kind of text:

        * The description and the comments: GitHub's own mention events of
          the thread (:meth:`_mention_log`). They are GitHub's decision of
          what a mention is, and they date a mention that an edit added.
        * A pull request's reviews and inline review comments: their texts.
          GitHub writes no mention event for them (measured: 0 of 6 mentions,
          against 30 of 30 in comments), though it does notify.

        Returns whether every read succeeded and reached back to *start*, and
        the moments of what was read.
        """
        login = self._run.login
        if not login:
            return False, []
        pattern = _mention_re(login)
        complete = True
        seen: list[datetime | None] = []
        # The mentions of the account: {"at"} for a mention event, and with
        # by / url / body / created_at for a review text that holds one.
        found: list[dict[str, Any]] = []
        reviews: list[dict[str, Any]] = []

        log = await self._mention_log(subject_url, login)
        if log is None:
            complete = False
        else:
            mine, reviews, event_times = log
            seen += event_times
            found += [{"at": at} for at in mine]

        if subject_type == "PullRequest":
            review_times: list[datetime] = []
            for review in reviews:
                at = _when(review.get("submittedAt"))
                if at is None:  # a pending review of the account itself
                    continue
                review_times.append(at)
                if _mentions(pattern, review.get("body")):
                    found.append({
                        "at": at,
                        "by": (review.get("author") or {}).get("login", ""),
                        "url": review.get("url", ""),
                        "body": review.get("body", ""),
                        "created_at": review.get("submittedAt", ""),
                    })
            seen += review_times
            if len(reviews) >= 50 and review_times and min(review_times) > start:
                complete = False  # more reviews since *start* than were read

            # Newest edit first, so an edit that adds a mention is read too.
            inline = await self._gh_api_get(
                f"{subject_url}/comments?sort=updated&direction=desc&per_page=100",
            )
            if not isinstance(inline, list):
                complete = False
            else:
                inline_times: list[datetime] = []
                for c in inline:
                    at = _comment_time(c) if isinstance(c, dict) else None
                    if at is None:
                        continue
                    inline_times.append(at)
                    if _mentions(pattern, c.get("body")):
                        found.append({
                            "at": at,
                            "by": (c.get("user") or {}).get("login", ""),
                            "url": c.get("html_url", ""),
                            "body": c.get("body", ""),
                            "created_at": c.get("created_at", ""),
                        })
                seen += inline_times
                if len(inline) >= 100 and inline_times and min(inline_times) > start:
                    complete = False  # more inline comments since *start*

        new = [m for m in found if m["at"] > start]
        if new:
            newest = max(new, key=lambda m: m["at"])
            if "by" not in newest:
                newest = self._mention_text(
                    pattern, newest["at"], comments, subject_data,
                ) or newest
            mention: dict[str, Any] = {"state": "new", "at": _stamp(newest["at"])}
            for key in ("by", "url", "body", "created_at"):
                if newest.get(key):
                    mention[key] = newest[key]
            result["mention"] = mention
        elif found and complete:
            last = max(m["at"] for m in found)
            result["mention"] = {"state": "old", "at": _stamp(last)}
        return complete, seen

    @staticmethod
    def _mention_text(
        pattern: re.Pattern[str],
        event_at: datetime,
        comments: list[dict[str, Any]],
        subject_data: dict[str, Any],
    ) -> dict[str, Any] | None:
        """The text that a mention event was written for, or None.

        The event carries neither author nor link. GitHub writes it 1 to 3
        seconds after the text (measured on 29 mentions), so the text is
        the comment that mentions the account and was written, or edited,
        shortly before the event; or the description, for a thread that was
        opened then.
        """
        for c in sorted(comments, key=lambda c: _comment_time(c) or event_at, reverse=True):
            at = _comment_time(c)
            if at is None or not timedelta(0) <= event_at - at <= _EVENT_LAG:
                continue
            if _mentions(pattern, c.get("body")):
                return {
                    "at": at,
                    "by": (c.get("user") or {}).get("login", ""),
                    "url": c.get("html_url", ""),
                    "body": c.get("body", ""),
                    "created_at": c.get("created_at", ""),
                }
        opened = _when(subject_data.get("created_at"))
        if (
            opened is not None
            and timedelta(0) <= event_at - opened <= _EVENT_LAG
            and _mentions(pattern, subject_data.get("body"))
        ):
            # No body: the record already carries the description.
            return {
                "at": opened,
                "by": (subject_data.get("user") or {}).get("login", ""),
                "url": subject_data.get("html_url", ""),
            }
        return None

    async def _mention_log(
        self, subject_url: str, login: str,
    ) -> tuple[list[datetime], list[dict[str, Any]], list[datetime | None]] | None:
        """GitHub's mention events of a thread, and a pull request's reviews.

        Returns the moments at which *login* was mentioned, the latest reviews
        (empty for an issue) and the moments of every event read; None when
        the read failed.

        The events come newest last, 100 a page. Older pages are read only
        until one holds a mention of *login*: one such mention is enough to
        explain GitHub's reason, and a mention since the thread's previous
        update is on the newest page.
        """
        match = _SUBJECT_URL_RE.search(subject_url)
        if match is None:
            return None
        variables: dict[str, Any] = {
            "owner": match["owner"],
            "repo": match["repo"],
            "number": int(match["number"]),
        }
        mine: list[datetime] = []
        reviews: list[dict[str, Any]] | None = None
        event_times: list[datetime | None] = []
        for _ in range(_MENTION_LOG_PAGES):
            data = await self._gh_graphql(_MENTION_LOG_QUERY, variables)
            thread = ((data or {}).get("repository") or {}).get("issueOrPullRequest")
            if not isinstance(thread, dict):
                return None
            items = thread.get("timelineItems")
            if not isinstance(items, dict):
                return None
            if reviews is None:
                nodes = (thread.get("reviews") or {}).get("nodes") or []
                reviews = [r for r in nodes if isinstance(r, dict)]
            for node in items.get("nodes") or []:
                at = _when((node or {}).get("createdAt"))
                if at is None:
                    continue
                event_times.append(at)
                actor = ((node or {}).get("actor") or {}).get("login", "")
                if actor.lower() == login.lower():
                    mine.append(at)
            page = items.get("pageInfo") or {}
            if mine or not page.get("hasPreviousPage") or not page.get("startCursor"):
                break
            variables["before"] = page["startCursor"]
        return mine, reviews or [], event_times

    async def _gh_login(self) -> str:
        """Login of the account gh acts as now, or "" when it cannot be read."""
        user = await self._gh_api_get("user")
        login = user.get("login") if isinstance(user, dict) else ""
        return login if isinstance(login, str) else ""

    @staticmethod
    def _has_mention_reason(notif: dict) -> bool:
        """Whether *notif* is of a ``mention`` thread whose mentions can be read."""
        subject = notif.get("subject") or {}
        return (
            notif.get("reason") == "mention"
            and subject.get("type") in ("PullRequest", "Issue")
            and bool(subject.get("url"))
        )

    @staticmethod
    async def _gh_graphql(
        query: str, variables: dict[str, Any], timeout: float = 30,
    ) -> dict | None:
        """Run a GraphQL query with gh; return its ``data``, or None on failure.

        A response with ``errors`` is a failure, whatever data came with it.
        """
        args = ["gh", "api", "graphql", "-f", f"query={query}"]
        for name, value in variables.items():
            # -F reads an integer as a number. A string goes with -f, which
            # takes it as it is: -F would read "@name" as a file to send.
            args += ["-F" if isinstance(value, int) else "-f", f"{name}={value}"]
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            if proc.returncode != 0:
                logger.debug("gh api graphql failed: %s", stderr.decode()[:200])
                return None
            payload = json.loads(stdout.decode())
        except Exception as e:
            logger.debug("gh api graphql error: %s", e)
            return None
        if not isinstance(payload, dict) or payload.get("errors"):
            return None
        data = payload.get("data")
        return data if isinstance(data, dict) else None

    @staticmethod
    async def _gh_api_get(url: str, timeout: float = 30) -> dict | list | None:
        """Call gh api with a full URL, return parsed JSON or None."""
        try:
            proc = await asyncio.create_subprocess_exec(
                "gh", "api", url,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            if proc.returncode != 0:
                logger.debug("gh api %s failed: %s", url, stderr.decode()[:200])
                return None
            text = stdout.decode()
            return json.loads(text) if text.strip() else None
        except Exception as e:
            logger.debug("gh api %s error: %s", url, e)
            return None
