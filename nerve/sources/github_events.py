"""GitHub Events source — fetches user activity via the gh CLI.

Unlike the notification source (which shows what others did involving you),
this source captures YOUR OWN actions: pushes, PR creates, issue comments, etc.
Useful for tracking what your AI agent has done on your behalf.

Cursor semantics: ISO 8601 timestamp (created_at of newest seen event).
On first run (no cursor), fetches the latest batch and sets the cursor to the
newest event's timestamp (no backfill).

Note: GitHub event IDs are NOT monotonically increasing — they can jump
non-sequentially. Timestamps are reliable for ordering.

API: GET /users/{username}/events — returns events newest-first.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from nerve.sources.base import Source
from nerve.sources.models import FetchResult, SourceRecord

logger = logging.getLogger(__name__)

# Cap for comment/body text in event payloads.
_MAX_BODY_CHARS = 2_000


class GitHubEventsSource(Source):
    """GitHub user events source using the gh CLI."""

    source_name = "github_events"

    def __init__(self, config: dict[str, Any] | None = None):
        self._config = config or {}
        self._repos: list[str] = self._config.get("repos", [])
        # Lowercase for case-insensitive matching
        self._repos_lower = [r.lower() for r in self._repos]
        self._username: str = self._config.get("username", "")
        # The login gh reported last; used only when a later lookup fails.
        self._detected_username = ""

    async def _resolve_username(self) -> str:
        """The configured GitHub username, or else the account gh acts as now.

        The account is looked up in every run and not kept as the answer: once
        gh is signed in as another account, or the account is renamed, the
        next run reads the events of the current one. A failed lookup falls
        back to the login the last one returned.
        """
        if self._username:
            return self._username

        try:
            proc = await asyncio.create_subprocess_exec(
                "gh", "api", "user", "--jq", ".login",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)
            if proc.returncode == 0 and stdout.strip():
                detected = stdout.decode().strip()
                if detected != self._detected_username:
                    logger.info("Auto-detected GitHub username: %s", detected)
                    self._detected_username = detected
                return detected
        except Exception as e:
            logger.error("Failed to detect GitHub username: %s", e)

        if self._detected_username:
            return self._detected_username
        raise RuntimeError("GitHub username not configured and auto-detection failed")

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        """Fetch new events since cursor (ISO timestamp).

        On first run (cursor=None): fetches the latest batch and sets the
        cursor to the newest event's timestamp — no backfill of historical events.
        """
        username = await self._resolve_username()

        # Fetch up to `limit` events (API max per_page is 100)
        per_page = min(limit, 100)
        try:
            endpoint = f"/users/{username}/events?per_page={per_page}"
            proc = await asyncio.create_subprocess_exec(
                "gh", "api", endpoint,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)

            if proc.returncode != 0:
                logger.error("gh api events failed: %s", stderr.decode())
                return FetchResult(records=[], next_cursor=cursor)

            stdout_text = stdout.decode()
            events = json.loads(stdout_text) if stdout_text.strip() else []

        except FileNotFoundError:
            logger.error("gh CLI not found — install gh for GitHub sync")
            return FetchResult(records=[], next_cursor=cursor)
        except asyncio.TimeoutError:
            logger.error("gh api events timed out")
            return FetchResult(records=[], next_cursor=cursor)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse gh events output: %s", e)
            return FetchResult(records=[], next_cursor=cursor)
        except Exception as e:
            logger.error("GitHub events error: %s", e)
            return FetchResult(records=[], next_cursor=cursor)

        if not events:
            return FetchResult(records=[], next_cursor=cursor)

        # Filter by repo if configured
        if self._repos_lower:
            events = [
                e for e in events
                if e.get("repo", {}).get("name", "").lower() in self._repos_lower
            ]

        # First run: establish baseline cursor, don't backfill
        if cursor is None:
            newest_ts = events[0].get("created_at") if events else None
            logger.info(
                "GitHub events: first run, establishing baseline cursor=%s (%d events skipped)",
                newest_ts, len(events),
            )
            return FetchResult(records=[], next_cursor=newest_ts)

        # Filter to only events newer than cursor (ISO timestamp).
        # GitHub event IDs are NOT monotonically increasing, so we
        # compare by created_at instead. DB-level dedup (INSERT OR IGNORE
        # on source+id) handles any overlap from same-second events.
        new_events = [
            e for e in events
            if e.get("created_at", "") > cursor
        ]

        if not new_events:
            return FetchResult(records=[], next_cursor=cursor)

        # Convert to SourceRecords (oldest-first for natural reading order)
        new_events.sort(key=lambda e: e.get("created_at", ""))
        records: list[SourceRecord] = []
        for event in new_events:
            record = self._event_to_record(event)
            if record:
                records.append(record)

        # Advance cursor to newest event's timestamp
        newest_ts = max(e.get("created_at", "") for e in new_events)
        return FetchResult(
            records=records,
            next_cursor=newest_ts,
            has_more=False,
        )

    # ------------------------------------------------------------------
    # Event formatting
    # ------------------------------------------------------------------

    def _event_to_record(self, event: dict) -> SourceRecord | None:
        """Convert a GitHub event to a SourceRecord with type-specific formatting."""
        event_id = event.get("id", "")
        event_type = event.get("type", "")
        repo = event.get("repo", {})
        repo_name = repo.get("name", "?")
        created_at = event.get("created_at", "")
        payload = event.get("payload", {})

        formatter = _EVENT_FORMATTERS.get(event_type, _format_generic)
        summary, content = formatter(repo_name, payload)

        repo_url = f"https://github.com/{repo_name}"

        return SourceRecord(
            id=event_id,
            source="github_events",
            record_type=f"github_event:{event_type}",
            summary=f"[{repo_name}] {summary}",
            content=f"Repository: {repo_name}\nType: {event_type}\n{content}\nTime: {created_at}\nRepo: {repo_url}",
            timestamp=created_at,
            metadata={
                "event_type": event_type,
                "repo_name": repo_name,
                "repo_url": repo_url,
                "action": payload.get("action", ""),
            },
        )


# ------------------------------------------------------------------
# Per-event-type formatters: return (summary, content_body)
# ------------------------------------------------------------------

def _format_push(repo: str, payload: dict) -> tuple[str, str]:
    ref = payload.get("ref", "").replace("refs/heads/", "")
    commits = payload.get("commits", [])
    size = payload.get("size", len(commits))
    head_sha = payload.get("head", "")[:7]

    # Events API may return truncated payloads without commits array
    if commits:
        summary = f"Pushed {size} commit{'s' if size != 1 else ''} to {ref}"
        lines = [f"Branch: {ref}", f"Commits: {size}"]
        for c in commits[:10]:
            sha = c.get("sha", "")[:7]
            msg = c.get("message", "").split("\n")[0]
            lines.append(f"  {sha} {msg}")
        if len(commits) > 10:
            lines.append(f"  ... and {len(commits) - 10} more")
    else:
        summary = f"Pushed to {ref}" if ref else f"Pushed ({head_sha})"
        lines = [f"Branch: {ref}", f"Head: {head_sha}"]

    return summary, "\n".join(lines)


def _format_pull_request(repo: str, payload: dict) -> tuple[str, str]:
    action = payload.get("action", "")
    pr = payload.get("pull_request", {})
    # Events API puts number at payload level too; PR object may have it
    number = payload.get("number") or pr.get("number", "?")
    # Events API often omits title/html_url/state — construct from what we have
    title = pr.get("title") or ""
    url = pr.get("html_url") or f"https://github.com/{repo}/pull/{number}"
    state = pr.get("state") or action
    # Events API uses action="merged" directly (unlike full API's closed+merged)
    action_str = action.capitalize()

    # base/head are objects with "ref" key in Events API
    base_ref = pr.get("base", {})
    head_ref = pr.get("head", {})
    base = base_ref.get("ref", "") if isinstance(base_ref, dict) else str(base_ref)
    head = head_ref.get("ref", "") if isinstance(head_ref, dict) else str(head_ref)

    if title:
        summary = f"{action_str} PR #{number}: {title}"
    else:
        summary = f"{action_str} PR #{number}"

    lines = [f"PR: #{number}"]
    if title:
        lines[0] += f" {title}"
    lines.append(f"Action: {action_str}")
    if state and state != action:
        lines.append(f"State: {state}")
    if head and base:
        lines.append(f"Branch: {head} → {base}")
    lines.append(f"URL: {url}")

    body = (pr.get("body") or "")[:_MAX_BODY_CHARS]
    if body:
        lines.append(f"\n--- Description ---\n{body}")

    return summary, "\n".join(lines)


def _format_issue_comment(repo: str, payload: dict) -> tuple[str, str]:
    action = payload.get("action", "")
    issue = payload.get("issue", {})
    comment = payload.get("comment", {})
    number = issue.get("number", "?")
    title = issue.get("title", "?")
    comment_body = (comment.get("body", "") or "")[:_MAX_BODY_CHARS]
    url = comment.get("html_url", "")
    is_pr = "pull_request" in issue

    kind = "PR" if is_pr else "issue"
    summary = f"Commented on {kind} #{number}: {title}"

    lines = [
        f"{kind.capitalize()}: #{number} {title}",
        f"Action: {action}",
        f"URL: {url}",
    ]
    if comment_body:
        lines.append(f"\n--- Comment ---\n{comment_body}")

    return summary, "\n".join(lines)


def _format_issues(repo: str, payload: dict) -> tuple[str, str]:
    action = payload.get("action", "")
    issue = payload.get("issue", {})
    number = issue.get("number", "?")
    title = issue.get("title", "?")
    url = issue.get("html_url", "")

    summary = f"{action.capitalize()} issue #{number}: {title}"

    body = (issue.get("body", "") or "")[:_MAX_BODY_CHARS]
    lines = [
        f"Issue: #{number} {title}",
        f"Action: {action}",
        f"URL: {url}",
    ]
    if body:
        lines.append(f"\n--- Body ---\n{body}")

    return summary, "\n".join(lines)


def _format_pr_review(repo: str, payload: dict) -> tuple[str, str]:
    action = payload.get("action", "")
    review = payload.get("review", {})
    pr = payload.get("pull_request", {})
    number = pr.get("number", "?")
    title = pr.get("title") or ""
    state = review.get("state", "")  # approved, changes_requested, commented
    url = review.get("html_url") or f"https://github.com/{repo}/pull/{number}"
    body = (review.get("body") or "")[:_MAX_BODY_CHARS]

    state_str = state.replace("_", " ").capitalize() if state else action

    if title:
        summary = f"Reviewed PR #{number}: {title} ({state_str})"
    else:
        summary = f"Reviewed PR #{number} ({state_str})"

    lines = [f"PR: #{number}"]
    if title:
        lines[0] += f" {title}"
    lines.append(f"Review: {state_str}")
    lines.append(f"URL: {url}")
    if body:
        lines.append(f"\n--- Review ---\n{body}")

    return summary, "\n".join(lines)


def _format_create(repo: str, payload: dict) -> tuple[str, str]:
    ref_type = payload.get("ref_type", "")  # branch, tag
    ref = payload.get("ref", "")

    summary = f"Created {ref_type} {ref}" if ref else f"Created {ref_type}"
    content = f"Ref type: {ref_type}\nRef: {ref}" if ref else f"Ref type: {ref_type}"
    return summary, content


def _format_delete(repo: str, payload: dict) -> tuple[str, str]:
    ref_type = payload.get("ref_type", "")
    ref = payload.get("ref", "")

    summary = f"Deleted {ref_type} {ref}" if ref else f"Deleted {ref_type}"
    content = f"Ref type: {ref_type}\nRef: {ref}" if ref else f"Ref type: {ref_type}"
    return summary, content


def _format_fork(repo: str, payload: dict) -> tuple[str, str]:
    forkee = payload.get("forkee", {})
    full_name = forkee.get("full_name", "")
    url = forkee.get("html_url", "")

    summary = f"Forked to {full_name}" if full_name else "Forked"
    lines = []
    if full_name:
        lines.append(f"Fork: {full_name}")
    if url:
        lines.append(f"URL: {url}")
    return summary, "\n".join(lines) if lines else "Forked repository"


def _format_generic(repo: str, payload: dict) -> tuple[str, str]:
    action = payload.get("action", "")
    summary = action.capitalize() if action else "Activity"
    content = f"Action: {action}" if action else "No additional details"
    return summary, content


_EVENT_FORMATTERS: dict[str, Any] = {
    "PushEvent": _format_push,
    "PullRequestEvent": _format_pull_request,
    "IssueCommentEvent": _format_issue_comment,
    "IssuesEvent": _format_issues,
    "PullRequestReviewEvent": _format_pr_review,
    "CreateEvent": _format_create,
    "DeleteEvent": _format_delete,
    "ForkEvent": _format_fork,
}
