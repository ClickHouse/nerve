"""The GitHub notifications source must not skip notifications beyond one page.

GitHub lists ``/notifications`` 50 per page, newest first by the time a thread
last notified the user, and applies ``since`` to that same time. The API never
returns it: ``updated_at`` also moves on events that do not notify (pushes,
labels, edits), so it can be far above the listing key. The fake below models
that key as a hidden ``_k <= updated_at`` per thread.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit

import pytest

from nerve.sources.github import GitHubSource

_PAGE_SIZE = 50
_T0 = datetime(2026, 1, 2, 10, 0, 0, tzinfo=timezone.utc)


def _ts(seconds: int) -> str:
    return (_T0 + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _thread(tid: str, k: int, updated_at: int | None = None) -> dict:
    return {
        "id": tid,
        "_k": _ts(k),
        "reason": "mention",
        "unread": True,
        "updated_at": _ts(k if updated_at is None else updated_at),
        "subject": {"title": f"Thread {tid}", "type": "Issue", "url": None},
        "repository": {
            "full_name": "owner/repo",
            "html_url": "https://github.com/owner/repo",
        },
    }


class _FakeProc:
    def __init__(self, stdout: bytes):
        self._stdout = stdout
        self.returncode = 0

    async def communicate(self):
        return self._stdout, b""


def _install_fake_gh(monkeypatch, state: dict[str, dict], between_pages=None) -> None:
    """Emulate ``gh api notifications?since=...`` over ``state`` (id -> thread).

    Each page is computed from the state at the time it is requested;
    ``between_pages(state)`` runs once, after the first page of the first call.
    ``--paginate`` follows every page, otherwise only the first is returned.
    ``--jq '.[]'`` prints one object per line, ``--jq .`` one array per page.
    """
    hook = [between_pages]

    def listed(since: str) -> list[dict]:
        threads = [t for t in state.values() if t["_k"] >= since]
        return sorted(threads, key=lambda t: (t["_k"], t["id"]), reverse=True)

    async def fake_exec(*argv, **kwargs):
        query = parse_qs(urlsplit(argv[2]).query)
        since = query["since"][0]
        per_page = min(int(query.get("per_page", [_PAGE_SIZE])[0]), _PAGE_SIZE)

        pages = []
        offset = 0
        while True:
            current = listed(since)
            page = current[offset:offset + per_page]
            if not page:
                break
            pages.append([{k: v for k, v in t.items() if k != "_k"} for t in page])
            has_next = len(current) > offset + per_page
            if hook[0] is not None:
                hook[0](state)
                hook[0] = None
            if "--paginate" not in argv or not has_next:
                break
            offset += per_page

        jq = argv[argv.index("--jq") + 1] if "--jq" in argv else None
        if jq == ".[]":
            lines = [json.dumps(t, ensure_ascii=False) for page in pages for t in page]
        else:
            lines = [json.dumps(page, ensure_ascii=False) for page in pages]
        return _FakeProc("\n".join(lines).encode())

    monkeypatch.setattr(
        "nerve.sources.github.asyncio.create_subprocess_exec", fake_exec,
    )


def _source(monkeypatch) -> GitHubSource:
    src = GitHubSource()

    async def no_enrich(notif, sem):
        return {}

    monkeypatch.setattr(src, "_enrich_notification", no_enrich)
    return src


@pytest.mark.asyncio
async def test_backlog_with_updated_at_ahead_of_listing_key_is_never_skipped(monkeypatch):
    displaced = {5, 25, 45, 65, 85}
    threads = [
        _thread(f"t{i:03d}", 60 * i, 60 * 119 + i if i in displaced else None)
        for i in range(120)
    ]
    _install_fake_gh(monkeypatch, {t["id"]: t for t in threads})
    src = _source(monkeypatch)

    cursor = _ts(-10)
    calls = []
    seen: set[str] = set()
    while len(calls) < 10:
        result = await src.fetch(cursor, limit=30)
        calls.append(result)
        cursor = result.next_cursor
        new = {r.id for r in result.records} - seen
        seen |= new
        if not new:
            break

    assert seen == {t["id"] for t in threads}
    assert len(calls[0].records) == 120
    assert len({r.id for r in calls[0].records}) == 120
    assert calls[0].next_cursor == _ts(60 * 119)
    assert [r.id for r in calls[1].records] == ["t119"]
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_notification_landing_between_pages_is_listed_by_the_next_call(monkeypatch):
    state = {t["id"]: t for t in (_thread(f"t{i:03d}", 60 * i) for i in range(60))}

    def between_pages(state):
        state["x"] = _thread("x", 60 * 59 + 30)
        state["t003"]["updated_at"] = _ts(60 * 59 + 45)
        state["t010"]["updated_at"] = _ts(60 * 59 + 40)

    _install_fake_gh(monkeypatch, state, between_pages)
    src = _source(monkeypatch)

    first = await src.fetch(_ts(-10), limit=30)
    ids = [r.id for r in first.records]
    assert len(ids) == 60
    assert len(set(ids)) == 60
    assert "x" not in ids
    by_id = {r.id: r for r in first.records}
    assert by_id["t010"].timestamp == _ts(60 * 59 + 40)
    assert first.next_cursor == _ts(60 * 59)

    second = await src.fetch(first.next_cursor, limit=30)
    assert [r.id for r in second.records] == ["x", "t059"]


@pytest.mark.asyncio
async def test_notification_landing_in_the_cursor_second_is_listed_by_the_next_call(monkeypatch):
    state = {t["id"]: t for t in (_thread(f"t{i:03d}", 60 * i) for i in range(60))}

    def between_pages(state):
        state["x"] = _thread("x", 60 * 59)

    _install_fake_gh(monkeypatch, state, between_pages)
    src = _source(monkeypatch)

    first = await src.fetch(_ts(-10), limit=30)
    ids = [r.id for r in first.records]
    assert len(ids) == 60
    assert "x" not in ids
    assert first.next_cursor == _ts(60 * 59)

    second = await src.fetch(first.next_cursor, limit=30)
    second_ids = {r.id for r in second.records}
    assert "x" in second_ids
    assert second_ids <= {"x", "t059"}


@pytest.mark.asyncio
async def test_raw_unicode_line_separators_inside_strings_are_parsed(monkeypatch):
    threads = [_thread("a", 0), _thread("b", 60)]
    threads[1]["subject"]["title"] = "Fix\u0085parser edge"
    _install_fake_gh(monkeypatch, {t["id"]: t for t in threads})
    src = _source(monkeypatch)

    result = await src.fetch(_ts(-10), limit=30)
    assert len(result.records) == 2
    by_id = {r.id: r for r in result.records}
    assert "Fix\u0085parser edge" in by_id["b"].summary
