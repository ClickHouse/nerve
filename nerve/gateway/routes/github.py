"""GitHub pull request summaries for the PR cards in the chat transcript.

``GET /api/github/pulls?url=...`` reads each pull request with the ``gh`` CLI
and the daemon's own ``gh`` login. There is no GitHub HTTP client in Nerve.

The URLs come from the browser, so the route accepts only
``https://github.com/<owner>/<repo>/pull/<number>`` and gives ``gh`` the
parsed parts as separate arguments. No shell runs, and no part of a URL can
become a ``gh`` flag: an owner starts with a letter or a digit, and the
number has only digits.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from nerve.gateway.auth import require_auth

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_auth)])

MAX_URLS = 10
CACHE_SECONDS = 60.0
GH_TIMEOUT_SECONDS = 15.0
MAX_CONCURRENT_GH = 4

PULL_URL = re.compile(
    r"https://github\.com/"
    r"(?P<owner>[A-Za-z0-9][A-Za-z0-9-]{0,38})/"
    r"(?P<repo>(?!\.+/)[A-Za-z0-9._-]{1,100})/"
    r"pull/(?P<number>[1-9][0-9]{0,9})"
)

GH_FIELDS = "number,title,state,isDraft,reviewDecision,statusCheckRollup,headRefName,baseRefName"

NO_ACCESS = "no_access"
GH_UNAVAILABLE = "gh_unavailable"

# A check run that ends with one of these conclusions counts as passed.
_PASSED_CONCLUSIONS = {"SUCCESS", "NEUTRAL", "SKIPPED"}
# A commit status in one of these states has no result yet.
_PENDING_STATUS_STATES = {"PENDING", "EXPECTED"}

# URL -> (monotonic start time, task that reads the pull request). Requests
# for one URL inside CACHE_SECONDS share one task, also while it runs.
_cache: dict[str, tuple[float, asyncio.Task[dict[str, Any]]]] = {}
_gh_slots = asyncio.Semaphore(MAX_CONCURRENT_GH)


def reset_cache() -> None:
    """Forget all cached results (tests: each test has its own event loop)."""
    global _gh_slots
    _cache.clear()
    _gh_slots = asyncio.Semaphore(MAX_CONCURRENT_GH)


def _check_counts(rollup: list[dict[str, Any]] | None) -> dict[str, int]:
    """Passed, failed and running checks of ``statusCheckRollup``.

    The rollup has two kinds of item: a check run (``status`` and
    ``conclusion``) and a commit status (``state``).
    """
    counts = {"passed": 0, "failed": 0, "running": 0}
    for check in rollup or []:
        if check.get("__typename") == "StatusContext":
            state = check.get("state") or ""
            if state in _PENDING_STATUS_STATES:
                counts["running"] += 1
            elif state == "SUCCESS":
                counts["passed"] += 1
            else:
                counts["failed"] += 1
        elif check.get("status") != "COMPLETED":
            counts["running"] += 1
        elif check.get("conclusion") in _PASSED_CONCLUSIONS:
            counts["passed"] += 1
        else:
            counts["failed"] += 1
    return counts


def _summary(owner: str, repo: str, data: dict[str, Any]) -> dict[str, Any]:
    """The card fields of one ``gh pr view --json`` answer."""
    state = data.get("state")
    if state == "MERGED":
        card_state = "merged"
    elif state == "CLOSED":
        card_state = "closed"
    elif data.get("isDraft"):
        card_state = "draft"
    else:
        card_state = "open"
    return {
        "repo": f"{owner}/{repo}",
        "number": data.get("number"),
        "title": data.get("title") or "",
        "state": card_state,
        "review": (data.get("reviewDecision") or "").lower() or None,
        "checks": _check_counts(data.get("statusCheckRollup")),
        "head_ref": data.get("headRefName") or None,
        "base_ref": data.get("baseRefName") or None,
    }


async def _gh_pr_view(owner: str, repo: str, number: str) -> dict[str, Any] | str:
    """``gh pr view`` of one pull request: its JSON, or an error code.

    ``--repo`` names the host, so a ``GH_HOST`` in the daemon's environment
    cannot send the call to a different server.
    """
    async with _gh_slots:
        try:
            proc = await asyncio.create_subprocess_exec(
                "gh", "pr", "view", number,
                "--repo", f"github.com/{owner}/{repo}",
                "--json", GH_FIELDS,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except OSError as error:
            logger.debug("gh is not available: %s", error)
            return GH_UNAVAILABLE
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=GH_TIMEOUT_SECONDS
            )
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(proc.wait(), timeout=5)
            logger.debug("gh pr view %s/%s#%s timed out", owner, repo, number)
            return GH_UNAVAILABLE
    if proc.returncode != 0:
        logger.debug(
            "gh pr view %s/%s#%s failed: %s",
            owner, repo, number, stderr.decode(errors="replace")[:200],
        )
        return NO_ACCESS
    try:
        data = json.loads(stdout.decode(errors="replace"))
    except ValueError:
        return GH_UNAVAILABLE
    return data if isinstance(data, dict) else GH_UNAVAILABLE


async def _read_pull(url: str, owner: str, repo: str, number: str) -> dict[str, Any]:
    try:
        data = await _gh_pr_view(owner, repo, number)
    except Exception:  # noqa: BLE001 - one card falls back, the others still load
        logger.exception("Reading %s failed", url)
        data = GH_UNAVAILABLE
    if isinstance(data, str):
        return {"url": url, "pull": None, "error": data}
    return {"url": url, "pull": _summary(owner, repo, data), "error": None}


async def _cached_pull(url: str, match: re.Match[str]) -> dict[str, Any]:
    now = time.monotonic()
    for key in [k for k, (started, _) in _cache.items() if now - started >= CACHE_SECONDS]:
        del _cache[key]
    entry = _cache.get(url)
    if entry is None:
        task = asyncio.ensure_future(
            _read_pull(url, match["owner"], match["repo"], match["number"])
        )
        _cache[url] = (now, task)
    else:
        task = entry[1]
    # A request that ends early must not cancel the task that others share.
    return await asyncio.shield(task)


@router.get("/api/github/pulls")
async def get_pulls(url: list[str] = Query(default=[])):
    """Summaries of up to ``MAX_URLS`` pull requests, one per distinct URL.

    Each result has the requested ``url`` and either ``pull`` (the card
    fields) or ``error`` (``no_access`` or ``gh_unavailable``).
    """
    if not 1 <= len(url) <= MAX_URLS:
        raise HTTPException(status_code=400, detail=f"Give 1 to {MAX_URLS} url parameters")
    matches: dict[str, re.Match[str]] = {}
    for value in url:
        match = PULL_URL.fullmatch(value)
        if match is None:
            raise HTTPException(
                status_code=400,
                detail="Each url must be https://github.com/<owner>/<repo>/pull/<number>",
            )
        matches[value] = match
    pulls = await asyncio.gather(*(_cached_pull(u, m) for u, m in matches.items()))
    return {"pulls": pulls}
