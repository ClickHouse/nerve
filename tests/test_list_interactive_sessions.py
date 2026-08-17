"""Tests for SessionStore.list_interactive_sessions — the switchable-session
query behind the channel /sessions keyboards (current → starred → recent,
interactive sources only, non-empty, paginated)."""

import pytest

from nerve.db import Database


async def _make(db, sid, source, updated_at, *, starred=False, msg=True,
                status="idle"):
    await db.create_session(sid, title=sid, source=source, status=status)
    if msg:
        await db.add_message(sid, "user", "hi")   # bumps updated_at → override below
    if starred:
        await db.update_session_fields(sid, {"starred": 1})
    # updated_at is not settable via update_session_fields (it means "last
    # message activity"); set it directly, after add_message, to pin recency.
    await db._write(
        "UPDATE sessions SET updated_at = ? WHERE id = ?", (updated_at, sid),
    )


async def _seed(db):
    # Interactive, non-empty — the rows that should appear.
    await _make(db, "cur",     "telegram", "2026-01-02T00:00:00+00:00")           # current, oldest
    await _make(db, "starOld", "web",      "2026-01-01T00:00:00+00:00", starred=True)
    await _make(db, "starNew", "telegram", "2026-03-01T00:00:00+00:00", starred=True)
    await _make(db, "recent1", "web",      "2026-04-01T00:00:00+00:00")           # newest non-starred
    await _make(db, "recent2", "telegram", "2026-02-01T00:00:00+00:00")
    # Rows that must be excluded.
    await _make(db, "cronjob", "cron",     "2026-05-01T00:00:00+00:00")           # cron source
    await _make(db, "emptyone", "web",     "2026-04-15T00:00:00+00:00", msg=False)  # 0 messages
    await _make(db, "arch",    "web",      "2026-04-20T00:00:00+00:00",
                starred=True, status="archived")                                   # archived (even if starred)


@pytest.mark.asyncio
async def test_current_first_then_starred_then_recent(db: Database):
    await _seed(db)
    rows = await db.list_interactive_sessions(limit=50, current_id="cur")
    ids = [r["id"] for r in rows]
    # current pinned, then starred by recency, then recent non-starred by recency
    assert ids == ["cur", "starNew", "starOld", "recent1", "recent2"]


@pytest.mark.asyncio
async def test_excludes_cron_empty_and_archived(db: Database):
    await _seed(db)
    ids = {r["id"] for r in await db.list_interactive_sessions(limit=50)}
    assert "cronjob" not in ids      # cron/automation source never listed
    assert "emptyone" not in ids     # 0-message sessions are useless to switch to
    assert "arch" not in ids         # archived excluded even when starred


@pytest.mark.asyncio
async def test_starred_lead_when_no_current(db: Database):
    await _seed(db)
    ids = [r["id"] for r in await db.list_interactive_sessions(limit=50)]
    # No current pin: starred lead (by recency), then recent — including the
    # (non-starred, old) "cur" which now sinks to the bottom.
    assert ids == ["starNew", "starOld", "recent1", "recent2", "cur"]


@pytest.mark.asyncio
async def test_pagination_via_limit_offset(db: Database):
    await _seed(db)
    order = ["cur", "starNew", "starOld", "recent1", "recent2"]
    p1 = [r["id"] for r in await db.list_interactive_sessions(limit=2, offset=0, current_id="cur")]
    p2 = [r["id"] for r in await db.list_interactive_sessions(limit=2, offset=2, current_id="cur")]
    p3 = [r["id"] for r in await db.list_interactive_sessions(limit=2, offset=4, current_id="cur")]
    assert p1 == order[0:2]
    assert p2 == order[2:4]
    assert p3 == order[4:5]   # last partial page


@pytest.mark.asyncio
async def test_next_page_probe_detects_more(db: Database):
    await _seed(db)
    # The channel view fetches page_size + 1 to learn whether a further page
    # exists; with 5 rows and page_size 2, page 1's probe must return 3.
    probe = await db.list_interactive_sessions(limit=3, offset=0, current_id="cur")
    assert len(probe) == 3
