"""GitHub Repos source — monitors a set of repos for newly-created issues and PRs.

Where the ``github`` source surfaces notifications involving *you* and the
``github_events`` source captures *your own* actions, this source watches a
configured set of repositories and surfaces brand-new issues and pull requests
as they are opened. It lets the agent monitor repos for new activity it would
not otherwise be notified about (you don't have to be a participant).

Cursor semantics: ISO 8601 timestamp = newest ``created_at`` seen across all
monitored repos. On first run (no cursor) it establishes a baseline cursor from
the newest item *without* backfilling history — same approach as
``github_events`` — so enabling the source doesn't dump the entire backlog into
the inbox.

A single cursor is shared across all repos. Every run fetches every repo, so any
item created after the cursor (in any repo) has ``created_at > cursor`` and is
caught. DB-level dedup on ``(source, id)`` handles same-second overlap.

API: ``GET /repos/{owner}/{repo}/issues?state=all&sort=created&direction=desc``
The issues endpoint returns BOTH issues and pull requests; PRs carry a
``pull_request`` key, which is how we tell them apart.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

from nerve.sources.base import Source
from nerve.sources.models import FetchResult, SourceRecord

logger = logging.getLogger(__name__)

# Cap for issue/PR body text to keep records reasonable.
_MAX_BODY_CHARS = 4_000

# Concurrent gh API calls — one per repo, bounded.
_MAX_CONCURRENT_FETCHES = 5


class GitHubReposSource(Source):
    """Monitors a configured set of repos for newly-created issues and PRs."""

    source_name = "github_repos"

    def __init__(self, config: dict[str, Any] | None = None):
        self._config = config or {}
        # Configured repos, e.g. ["ClickHouse/clickhouse-go", "owner/repo"].
        self._repos: list[str] = [
            r.strip() for r in self._config.get("repos", []) if r and r.strip()
        ]

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        """Fetch newly-created issues and PRs since cursor (ISO timestamp).

        On first run (cursor=None) establishes a baseline cursor from the newest
        item without emitting any records (no backfill).
        """
        if not self._repos:
            logger.warning("github_repos: no repos configured — nothing to fetch")
            return FetchResult(records=[], next_cursor=cursor)

        # GitHub caps per_page at 100. Each repo gets its own page-1 fetch.
        per_page = min(limit, 100)
        sem = asyncio.Semaphore(_MAX_CONCURRENT_FETCHES)
        fetch_tasks = [self._fetch_repo(repo, per_page, sem) for repo in self._repos]
        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        # Flatten into (repo, issue) pairs, skipping repos that errored.
        items: list[tuple[str, dict]] = []
        for repo, res in zip(self._repos, results):
            if isinstance(res, Exception):
                logger.warning("github_repos: fetch failed for %s: %s", repo, res)
                continue
            for issue in res:
                if isinstance(issue, dict):
                    items.append((repo, issue))

        if not items:
            return FetchResult(records=[], next_cursor=cursor)

        # First run: establish baseline cursor, don't backfill history.
        if cursor is None:
            newest_ts = max(
                (it.get("created_at", "") for _, it in items),
                default=None,
            )
            logger.info(
                "github_repos: first run, establishing baseline cursor=%s "
                "(%d items skipped across %d repos)",
                newest_ts, len(items), len(self._repos),
            )
            return FetchResult(records=[], next_cursor=newest_ts or cursor)

        # Keep only items created strictly after the cursor. DB-level dedup on
        # (source, id) covers any same-second overlap.
        new_items = [
            (repo, it) for repo, it in items
            if it.get("created_at", "") > cursor
        ]
        if not new_items:
            return FetchResult(records=[], next_cursor=cursor)

        # Oldest-first for natural reading order in the inbox.
        new_items.sort(key=lambda ri: ri[1].get("created_at", ""))
        records = [self._issue_to_record(repo, it) for repo, it in new_items]

        # Advance cursor to the newest created_at we just ingested.
        newest_ts = max(it.get("created_at", "") for _, it in new_items)
        return FetchResult(records=records, next_cursor=newest_ts, has_more=False)

    # ------------------------------------------------------------------
    # Fetch + formatting helpers
    # ------------------------------------------------------------------

    async def _fetch_repo(
        self, repo: str, per_page: int, sem: asyncio.Semaphore,
    ) -> list[dict]:
        """Fetch the most-recently-created issues+PRs for a single repo.

        Returns a list of issue dicts (PRs included — they carry a
        ``pull_request`` key). Returns [] on any error.
        """
        endpoint = (
            f"repos/{repo}/issues"
            f"?state=all&sort=created&direction=desc&per_page={per_page}"
        )
        async with sem:
            data = await self._gh_api_get(endpoint)
        if not isinstance(data, list):
            return []
        return data

    def _issue_to_record(self, repo: str, issue: dict) -> SourceRecord:
        """Convert an issue/PR dict into a SourceRecord."""
        number = issue.get("number")
        title = issue.get("title", "?")
        is_pr = "pull_request" in issue
        kind = "PullRequest" if is_pr else "Issue"
        kind_label = "PR" if is_pr else "Issue"
        url = issue.get("html_url", "")
        user = (issue.get("user") or {}).get("login", "")
        state = issue.get("state", "")
        created_at = issue.get("created_at", "")
        labels = [lb.get("name", "") for lb in (issue.get("labels") or [])]
        raw_body = issue.get("body") or ""

        content_parts = [
            f"Repository: {repo}",
            f"Type: {kind}",
            f"{kind_label}: #{number} {title}",
            f"Author: {user}" if user else None,
            f"State: {state}" if state else None,
            f"Labels: {', '.join(labels)}" if labels else None,
            f"Created: {created_at}",
            f"URL: {url}",
        ]
        if raw_body:
            body_text = raw_body[:_MAX_BODY_CHARS]
            if len(raw_body) > _MAX_BODY_CHARS:
                body_text += "\n[... truncated]"
            content_parts.append(f"\n--- Description ---\n{body_text}")

        # Prefer GitHub's globally-unique numeric id; fall back to repo#number.
        record_id = str(issue.get("id") or "") or f"{repo}#{number}"

        return SourceRecord(
            id=record_id,
            source="github_repos",
            record_type="github_pr" if is_pr else "github_issue",
            summary=f"[{repo}] New {kind_label} #{number}: {title}",
            content="\n".join(p for p in content_parts if p),
            timestamp=created_at or datetime.now(timezone.utc).isoformat(),
            metadata={
                # Keys aligned with the `github` source so the frontend
                # GitHubRenderer renders a repo card + "View on GitHub" link.
                "repo_name": repo,
                "repo_url": f"https://github.com/{repo}",
                "subject_type": kind,                       # Issue / PullRequest
                "subject_url": url,
                "reason": "new_pr" if is_pr else "new_issue",
                "number": number,
                "state": state,
                "author": user,
                "labels": labels,
            },
        )

    @staticmethod
    async def _gh_api_get(endpoint: str, timeout: float = 30) -> dict | list | None:
        """Call ``gh api <endpoint>`` and return parsed JSON, or None on error."""
        try:
            proc = await asyncio.create_subprocess_exec(
                "gh", "api", endpoint,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            if proc.returncode != 0:
                logger.debug("gh api %s failed: %s", endpoint, stderr.decode()[:200])
                return None
            text = stdout.decode()
            return json.loads(text) if text.strip() else None
        except FileNotFoundError:
            logger.error("gh CLI not found — install gh for GitHub sync")
            return None
        except asyncio.TimeoutError:
            logger.warning("gh api %s timed out", endpoint)
            return None
        except json.JSONDecodeError as e:
            logger.warning("Failed to parse gh output for %s: %s", endpoint, e)
            return None
        except Exception as e:
            logger.debug("gh api %s error: %s", endpoint, e)
            return None
