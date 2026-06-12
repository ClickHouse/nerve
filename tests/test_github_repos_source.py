"""Unit tests for :class:`GitHubReposSource`.

The source watches a configured set of repos and surfaces newly-created issues
and PRs. These tests stub the ``_gh_api_get`` call (no network / no gh CLI) and
exercise the cursor semantics, issue-vs-PR classification, and error isolation.
"""

from __future__ import annotations

import re

import pytest

from nerve.sources.github_repos import GitHubReposSource


def _issue(num, gid, created_at, title, *, is_pr=False, state="open",
           body="Some body text."):
    """Build a minimal issue/PR dict shaped like the GitHub /issues API.

    ``gid=None`` omits the numeric id (to exercise the repo#number fallback);
    ``body`` is overridable to exercise truncation / empty-body handling.
    """
    d = {
        "number": num,
        "title": title,
        "created_at": created_at,
        "state": state,
        "html_url": f"https://github.com/owner/repo/issues/{num}",
        "user": {"login": "alice"},
        "labels": [{"name": "bug"}],
        "body": body,
    }
    if gid is not None:
        d["id"] = gid
    if is_pr:
        d["pull_request"] = {"url": "https://api.github.com/.../pulls/1"}
    return d


# Canned per-repo payloads (returned newest-first, as the real API does with
# sort=created&direction=desc).
_REPO_DATA = {
    "owner/repo-a": [
        _issue(2, 102, "2026-06-12T11:00:00Z", "PR A2", is_pr=True),
        _issue(1, 101, "2026-06-12T10:00:00Z", "Issue A1"),
    ],
    "owner/repo-b": [
        _issue(3, 103, "2026-06-12T09:00:00Z", "Issue B1"),
    ],
}


def _make_source(repos, data=None):
    """Build a source with ``_gh_api_get`` stubbed from a {repo: items} map."""
    data = _REPO_DATA if data is None else data
    src = GitHubReposSource(config={"repos": repos})

    async def fake_gh_api_get(endpoint, timeout=30):
        m = re.match(r"repos/([^/]+/[^/]+)/issues", endpoint)
        repo = m.group(1) if m else None
        return data.get(repo, [])

    # Instance attribute shadows the staticmethod; called as self._gh_api_get(endpoint).
    src._gh_api_get = fake_gh_api_get
    return src


@pytest.mark.asyncio
async def test_first_run_establishes_baseline_no_records():
    """cursor=None emits no records but sets the cursor to the newest item."""
    src = _make_source(["owner/repo-a", "owner/repo-b"])
    result = await src.fetch(cursor=None)

    assert result.records == []
    # Newest created_at across both repos.
    assert result.next_cursor == "2026-06-12T11:00:00Z"


@pytest.mark.asyncio
async def test_fetch_new_items_since_cursor():
    """All items created after the cursor are returned, oldest-first."""
    src = _make_source(["owner/repo-a", "owner/repo-b"])
    result = await src.fetch(cursor="2026-06-12T08:00:00Z")

    # 3 items, sorted oldest-first by created_at.
    titles = [r.summary for r in result.records]
    assert len(result.records) == 3
    assert "Issue B1" in titles[0]   # 09:00
    assert "Issue A1" in titles[1]   # 10:00
    assert "PR A2" in titles[2]      # 11:00
    # Cursor advances to the newest ingested item.
    assert result.next_cursor == "2026-06-12T11:00:00Z"


@pytest.mark.asyncio
async def test_cursor_filter_is_strictly_greater():
    """An item whose created_at equals the cursor is NOT re-emitted."""
    src = _make_source(["owner/repo-a", "owner/repo-b"])
    result = await src.fetch(cursor="2026-06-12T10:00:00Z")

    # Only PR A2 (11:00) is strictly newer than 10:00.
    assert len(result.records) == 1
    assert "PR A2" in result.records[0].summary
    assert result.next_cursor == "2026-06-12T11:00:00Z"


@pytest.mark.asyncio
async def test_no_new_items_keeps_cursor():
    """When nothing is newer than the cursor, the cursor is unchanged."""
    src = _make_source(["owner/repo-a", "owner/repo-b"])
    result = await src.fetch(cursor="2026-06-12T23:00:00Z")

    assert result.records == []
    assert result.next_cursor == "2026-06-12T23:00:00Z"


@pytest.mark.asyncio
async def test_issue_vs_pr_classification():
    """Records carry the right record_type and renderer-friendly metadata."""
    src = _make_source(["owner/repo-a"])
    result = await src.fetch(cursor="2026-06-12T08:00:00Z")

    by_type = {r.record_type: r for r in result.records}
    assert set(by_type) == {"github_issue", "github_pr"}

    issue = by_type["github_issue"]
    assert issue.source == "github_repos"
    assert issue.metadata["subject_type"] == "Issue"
    assert issue.metadata["reason"] == "new_issue"
    assert issue.metadata["repo_name"] == "owner/repo-a"
    assert issue.id == "101"  # numeric GitHub id, stringified
    assert "Some body text." in issue.content

    pr = by_type["github_pr"]
    assert pr.metadata["subject_type"] == "PullRequest"
    assert pr.metadata["reason"] == "new_pr"


@pytest.mark.asyncio
async def test_no_repos_is_noop():
    """Empty repo list returns nothing and preserves the cursor."""
    src = _make_source([])
    result = await src.fetch(cursor="2026-06-12T08:00:00Z")

    assert result.records == []
    assert result.next_cursor == "2026-06-12T08:00:00Z"


@pytest.mark.asyncio
async def test_one_repo_error_does_not_block_others():
    """A repo whose fetch raises is skipped; healthy repos still ingest."""
    src = GitHubReposSource(config={"repos": ["owner/repo-a", "owner/repo-b"]})

    async def flaky_gh_api_get(endpoint, timeout=30):
        if "repo-b" in endpoint:
            raise RuntimeError("boom")
        m = re.match(r"repos/([^/]+/[^/]+)/issues", endpoint)
        return _REPO_DATA.get(m.group(1), [])

    src._gh_api_get = flaky_gh_api_get
    result = await src.fetch(cursor="2026-06-12T08:00:00Z")

    # repo-b raised, so only repo-a's two items come through.
    repos_seen = {r.metadata["repo_name"] for r in result.records}
    assert repos_seen == {"owner/repo-a"}
    assert len(result.records) == 2


@pytest.mark.asyncio
async def test_long_body_is_truncated():
    """Bodies longer than the cap are cut to _MAX_BODY_CHARS with a marker."""
    src = _make_source(["owner/repo-a"], data={
        "owner/repo-a": [_issue(7, 201, "2026-06-12T12:00:00Z", "Big", body="x" * 5000)],
    })
    result = await src.fetch(cursor="2026-06-12T00:00:00Z")
    content = result.records[0].content
    assert "[... truncated]" in content
    # Only the first 4000 body chars survive (nothing else in the record has 'x').
    assert content.count("x") == 4000


@pytest.mark.asyncio
async def test_empty_body_omits_description_section():
    """An issue with no body produces no '--- Description ---' block."""
    src = _make_source(["owner/repo-a"], data={
        "owner/repo-a": [_issue(10, 202, "2026-06-12T12:00:00Z", "Empty", body="")],
    })
    result = await src.fetch(cursor="2026-06-12T00:00:00Z")
    assert "--- Description ---" not in result.records[0].content


@pytest.mark.asyncio
async def test_record_id_falls_back_to_repo_number():
    """When GitHub's numeric id is absent, the record id is '<repo>#<number>'."""
    src = _make_source(["owner/repo-a"], data={
        "owner/repo-a": [_issue(9, None, "2026-06-12T12:00:00Z", "No id")],
    })
    result = await src.fetch(cursor="2026-06-12T00:00:00Z")
    assert result.records[0].id == "owner/repo-a#9"
