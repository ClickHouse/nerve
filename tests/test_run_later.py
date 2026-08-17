"""Tests for the "Run later" deferred-prompt endpoint.

Exercises the route handler directly against a real :class:`Database` with a
fake engine wired via ``init_deps``. Covers the deferred-scheduling path: a
timed option persists the pending user message + a synthetic assistant ack and
schedules exactly one one-shot wakeup with the stored prompt (never calling the
LLM); "Just accept it" persists the same messages but schedules no wakeup.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.db import Database
from nerve.gateway.routes._deps import init_deps
from nerve.gateway.routes.sessions import RunLaterRequest, run_later


def _fake_engine() -> MagicMock:
    engine = MagicMock()
    engine.run = AsyncMock()  # must never be awaited by run-later
    return engine


@pytest.mark.asyncio
async def test_run_later_timed_schedules_one_shot_wakeup(db: Database):
    engine = _fake_engine()
    init_deps(engine=engine, db=db)
    await db.create_session("rl-timed", source="web", backend="claude")

    res = await run_later(
        RunLaterRequest(session_id="rl-timed", message="do the thing", delay="30m"),
        user={},
    )

    assert res["scheduled"] is True
    assert "30 minutes" in res["ack"]

    # Pending user message + synthetic assistant ack, in order — no LLM call.
    msgs = await db.get_messages("rl-timed")
    assert [m["role"] for m in msgs] == ["user", "assistant"]
    assert msgs[0]["content"] == "do the thing"
    assert "30 minutes" in msgs[1]["content"]
    engine.run.assert_not_awaited()

    # Exactly one one-shot wakeup, carrying the stored prompt, ~30 min out.
    pending = await db.list_pending_wakeups("rl-timed")
    assert len(pending) == 1
    assert pending[0]["prompt"] == "do the thing"
    assert pending[0]["reason"] == "run-later"
    delta = (
        datetime.fromisoformat(pending[0]["fire_at"]) - datetime.now(timezone.utc)
    ).total_seconds()
    assert 1700 < delta < 1900


@pytest.mark.asyncio
async def test_run_later_24h_uses_full_delay_unclamped(db: Database):
    """The 24h option must not be clamped to the 1h wakeup-tool ceiling."""
    engine = _fake_engine()
    init_deps(engine=engine, db=db)
    await db.create_session("rl-24h", source="web", backend="claude")

    await run_later(
        RunLaterRequest(session_id="rl-24h", message="tomorrow", delay="24h"),
        user={},
    )

    pending = await db.list_pending_wakeups("rl-24h")
    assert len(pending) == 1
    delta = (
        datetime.fromisoformat(pending[0]["fire_at"]) - datetime.now(timezone.utc)
    ).total_seconds()
    assert 86000 < delta < 86500  # ~24h, not clamped to 3600s


@pytest.mark.asyncio
async def test_run_later_just_accept_schedules_nothing(db: Database):
    engine = _fake_engine()
    init_deps(engine=engine, db=db)
    await db.create_session("rl-manual", source="web", backend="claude")

    res = await run_later(
        RunLaterRequest(session_id="rl-manual", message="later", delay="none"),
        user={},
    )

    assert res["scheduled"] is False
    assert res["fire_at"] is None
    assert "manually" in res["ack"]

    msgs = await db.get_messages("rl-manual")
    assert [m["role"] for m in msgs] == ["user", "assistant"]
    assert "manually" in msgs[1]["content"]

    assert await db.list_pending_wakeups("rl-manual") == []
    engine.run.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_later_persists_all_attachments(db: Database):
    """Every uploaded attachment — images AND ordinary files — must be
    carried into the deferred session's pending user message, not just
    image blocks."""
    engine = _fake_engine()
    init_deps(engine=engine, db=db)
    await db.create_session("rl-files", source="web", backend="claude")
    await db.save_uploaded_file(
        "img1", "rl-files", "pic.png", "image/png", "image", 10, "/tmp/pic.png",
    )
    await db.save_uploaded_file(
        "doc1", "rl-files", "report.pdf", "application/pdf", "pdf", 20,
        "/tmp/report.pdf",
    )

    await run_later(
        RunLaterRequest(
            session_id="rl-files",
            message="see attached",
            delay="none",
            file_ids=["img1", "doc1"],
        ),
        user={},
    )

    msgs = await db.get_messages("rl-files")
    blocks = msgs[0]["blocks"]
    kinds = [(b["type"], b.get("filename")) for b in blocks]
    assert ("text", None) in kinds
    assert ("image", "pic.png") in kinds
    # The ordinary (non-image) file must survive as a "file" block.
    assert ("file", "report.pdf") in kinds


@pytest.mark.asyncio
async def test_run_later_unknown_session_404(db: Database):
    from fastapi import HTTPException

    init_deps(engine=_fake_engine(), db=db)
    with pytest.raises(HTTPException) as exc:
        await run_later(
            RunLaterRequest(session_id="missing", message="x", delay="1h"),
            user={},
        )
    assert exc.value.status_code == 404
