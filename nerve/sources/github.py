"""GitHub source — fetches notifications via the gh CLI.

Cursor semantics: ISO 8601 timestamp of the newest notification's `updated_at`.
On first run (no cursor), fetches from the last 24 hours.

Each notification is enriched with actual content from the subject (PR/issue),
the latest comment that triggered the notification, and for PRs the latest
review state (APPROVED, CHANGES_REQUESTED, etc.) plus any inline review
comments attached to specific code lines.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from nerve.sources.base import Source
from nerve.sources.models import FetchResult, SourceRecord

logger = logging.getLogger(__name__)

# Cap for PR/issue body and comment text to keep records reasonable.
_MAX_BODY_CHARS = 4_000
_MAX_COMMENT_CHARS = 2_000

# Concurrent API calls for enrichment.
_MAX_CONCURRENT_FETCHES = 5


class GitHubSource(Source):
    """GitHub notification source using the gh CLI."""

    source_name = "github"

    def __init__(self, config: dict[str, Any] | None = None):
        self._config = config or {}

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        """Fetch new notifications since cursor (ISO 8601 timestamp).

        On first run (cursor=None): fetches from the last 24 hours.
        """
        # GitHub's `since` is inclusive (>=), so advance by 1s to skip
        # already-seen notifications.
        # IMPORTANT: use Z suffix, not +00:00 — the `+` in a URL query
        # string is interpreted as a space, silently breaking the filter.
        if cursor:
            try:
                cursor_dt = datetime.fromisoformat(cursor.replace("Z", "+00:00"))
                since = (cursor_dt + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                since = cursor
        else:
            since = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            # Query params go in the URL — -f flags would force POST
            endpoint = f"notifications?since={since}&participating=true"
            proc = await asyncio.create_subprocess_exec(
                "gh", "api", endpoint,
                "--jq", ".",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)

            if proc.returncode != 0:
                logger.error("gh api notifications failed: %s", stderr.decode())
                return FetchResult(records=[], next_cursor=cursor)

            stdout_text = stdout.decode()
            notifications = json.loads(stdout_text) if stdout_text.strip() else []

            # Enrich notifications with actual content in parallel
            sem = asyncio.Semaphore(_MAX_CONCURRENT_FETCHES)
            enrich_tasks = [
                self._enrich_notification(notif, sem)
                for notif in notifications
            ]
            enriched = await asyncio.gather(*enrich_tasks, return_exceptions=True)

            records: list[SourceRecord] = []
            newest_ts: str | None = None

            for notif, extra in zip(notifications, enriched):
                subject = notif.get("subject", {})
                repo = notif.get("repository", {})
                updated_at = notif.get("updated_at", "")
                reason = notif.get("reason", "")
                repo_name = repo.get("full_name", "?")
                subject_title = subject.get("title", "?")
                subject_type = subject.get("type", "")

                # Build enriched content
                if isinstance(extra, Exception):
                    logger.warning("Enrichment failed for %s: %s", notif.get("id"), extra)
                    extra = {}

                html_url = extra.get("html_url") or repo.get("html_url", "")
                subject_body = extra.get("body", "")
                subject_state = extra.get("state", "")
                subject_user = extra.get("user", "")
                assignees = extra.get("assignees", [])
                labels = extra.get("labels", [])
                comment = extra.get("latest_comment")

                content_parts = [
                    f"Repository: {repo_name}",
                    f"Type: {subject_type}",
                    f"Title: {subject_title}",
                    f"Reason: {reason}",
                    f"State: {subject_state}" if subject_state else None,
                    f"Author: {subject_user}" if subject_user else None,
                    f"Assignees: {', '.join(assignees)}" if assignees else None,
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

                # Add recent human comments (catches mentions buried by
                # bot comments — see _enrich_recent_comments).
                recent_comments = extra.get("recent_comments", [])
                if recent_comments:
                    content_parts.append(
                        "\n--- Recent Comments (human, newest first) ---"
                    )
                    for rc in recent_comments:
                        rc_user = rc.get("user", "?")
                        rc_date = rc.get("created_at", "")
                        rc_body = rc.get("body", "")
                        content_parts.append(
                            f"\n{rc_user} ({rc_date}):\n{rc_body}"
                        )

                records.append(SourceRecord(
                    id=notif.get("id", ""),
                    source="github",
                    record_type="github_notification",
                    summary=f"[{repo_name}] {subject_title} ({reason})",
                    content="\n".join(p for p in content_parts if p),
                    timestamp=updated_at or datetime.now(timezone.utc).isoformat(),
                    metadata={
                        "reason": reason,
                        "unread": notif.get("unread", False),
                        "subject_type": subject_type,
                        "subject_url": html_url,
                        "repo_name": repo_name,
                        "repo_url": repo.get("html_url", ""),
                    },
                ))

                if newest_ts is None or updated_at > (newest_ts or ""):
                    newest_ts = updated_at

            next_cursor = newest_ts if newest_ts else cursor
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

            # Fetch recent human comments to catch mentions buried by bot
            # comments.  GitHub's latest_comment_url always points to the
            # most recent comment — when a bot (e.g. some-ci[bot])
            # posts after a human @mention, the mention is lost.
            reason = notif.get("reason", "")
            s_type = subject.get("type", "")
            if reason in ("mention", "assign", "review_requested", "team_mention") and s_type in ("PullRequest", "Issue"):
                await self._enrich_recent_comments(subject_url, s_type, result)

        return result

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

    async def _enrich_recent_comments(
        self, subject_url: str, subject_type: str, result: dict[str, Any],
    ) -> None:
        """Fetch recent human comments for a PR/issue notification.

        GitHub's ``latest_comment_url`` points to the *most recent* comment,
        which is often a bot (CI coverage, AI review, merge conflict warning).
        When a human @mentions us and then a bot comments seconds later, the
        mention is buried and never appears in the enriched content.

        This method fetches the last few issue-level comments, filters out
        bots, and attaches human comments that are missing from the
        already-fetched ``latest_comment``.  It ensures the actual triggering
        comment is always available to downstream consumers.
        """
        # PR issue-comments live under /issues/{n}/comments, not /pulls/
        if subject_type == "PullRequest":
            comments_url = subject_url.replace("/pulls/", "/issues/") + "/comments"
        else:
            comments_url = subject_url + "/comments"

        # Fetch last 10 comments (newest first) — enough to find the mention
        # even if several bots posted afterwards.
        comments_data = await self._gh_api_get(
            f"{comments_url}?per_page=10&sort=created&direction=desc",
        )
        if not isinstance(comments_data, list) or not comments_data:
            return

        # Deduplicate against the latest_comment already in result
        existing = result.get("latest_comment") or {}
        existing_key = (existing.get("user", ""), existing.get("created_at", ""))

        recent_human: list[dict[str, str]] = []
        for c in comments_data:
            user = c.get("user", {}).get("login", "")
            body = (c.get("body", "") or "")
            created = c.get("created_at", "")

            # Skip bots
            if user.endswith("[bot]") or user.endswith("-bot"):
                continue

            # Skip if identical to the already-fetched latest_comment
            if (user, created) == existing_key:
                continue

            recent_human.append({
                "user": user,
                "body": body[:_MAX_COMMENT_CHARS],
                "created_at": created,
            })

            # Cap at 5 human comments — enough context without bloat
            if len(recent_human) >= 5:
                break

        if recent_human:
            result["recent_comments"] = recent_human

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
