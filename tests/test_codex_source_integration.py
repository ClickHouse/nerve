"""Integration test for :class:`CodexThreadSyncService`.

End-to-end: a configured service points at a rollout file, the service
starts, observes events, persists them to the DB, and stops cleanly
flushing its cursor.
"""

from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path

import pytest
import pytest_asyncio

from nerve.config import (
    CodexOriginConfig,
    CodexSyncConfig,
    CodexWorkspaceFilterConfig,
    NerveConfig,
)
from nerve.sources.codex_threads import build_service
from nerve.sources.codex_threads.ingester import codex_session_id

from tests.actor_rows import ensure_system_principal


@pytest_asyncio.fixture
async def db(db):  # noqa: F811 — the conftest database, with an identity
    """The conftest database after local bootstrap.

    Autonomous code resolves the agent's system principal before it writes and
    fails the run rather than storing a row under nobody, so the paths this
    file drives need the identity every real database has: production opens
    nothing without bootstrapping it first.
    """
    await ensure_system_principal(db)
    return db

FIXTURE = Path(__file__).parent / "fixtures" / "codex" / "rollouts" / "in_scope.jsonl"
TEST_WORKSPACE = Path("/tmp/nerve-test-ws")


@pytest.mark.asyncio
async def test_service_starts_origin_and_ingests_events(db, tmp_path):
    sessions_dir = tmp_path / "sessions"
    archive_dir = tmp_path / "archived_sessions"
    sessions_dir.mkdir()
    archive_dir.mkdir()
    target_dir = sessions_dir / "2026" / "05" / "19"
    target_dir.mkdir(parents=True)
    shutil.copy(FIXTURE, target_dir / FIXTURE.name)

    config = NerveConfig(
        workspace=TEST_WORKSPACE,
        sync=_sync_config(sessions_dir, archive_dir),
    )

    service = build_service(config, db)
    assert service is not None
    await service.start()

    try:
        # Poll until the satellite session shows up. Origin polls every
        # 0.1 s — give it up to ~2 s.
        thread_id = "11111111-2222-3333-4444-555555555555"
        sid = codex_session_id(thread_id)
        for _ in range(40):
            session = await db.get_session(sid)
            if session is not None:
                msgs = await db.get_messages(sid)
                if msgs:
                    break
            await asyncio.sleep(0.1)

        session = await db.get_session(sid)
        assert session is not None, "service did not produce a satellite session"
        msgs = await db.get_messages(sid)
        roles = {m["role"] for m in msgs}
        assert "user" in roles
        assert "assistant" in roles

        # Cursor was persisted along the way.
        cursor = await db.get_sync_cursor("codex:local-pi")
        assert cursor, "cursor should have been persisted"
        parsed = json.loads(cursor)
        assert len(parsed["files"]) == 1

        # Diagnostics surface returns useful structure.
        status = service.status()
        assert status["started"] is True
        assert any(o["origin_id"] == "local-pi" for o in status["origins"])
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_a_failed_ingest_is_replayed_after_a_restart(db, tmp_path):
    """The cursor must never move past an event that was not persisted.

    The origin advances its own offset before it yields, so the live cursor
    during a failed ingest already covers the event. Checkpointing it there
    would skip the event for good: the next scan starts past it and nothing
    replays it. Driven with the real failure this can have — the system
    principal not resolving — rather than a synthetic one.
    """
    sessions_dir = tmp_path / "sessions"
    archive_dir = tmp_path / "archived_sessions"
    sessions_dir.mkdir()
    archive_dir.mkdir()
    target_dir = sessions_dir / "2026" / "05" / "19"
    target_dir.mkdir(parents=True)
    shutil.copy(FIXTURE, target_dir / FIXTURE.name)

    config = NerveConfig(
        workspace=TEST_WORKSPACE,
        sync=_sync_config(sessions_dir, archive_dir),
    )
    thread_id = "11111111-2222-3333-4444-555555555555"
    sid = codex_session_id(thread_id)

    # --- the run that fails -------------------------------------------------
    real_principal = db.get_system_principal

    async def _no_principal():
        return None

    db.get_system_principal = _no_principal
    service = build_service(config, db)
    await service.start()
    try:
        for _ in range(30):
            await asyncio.sleep(0.05)
            if await db.get_sync_cursor("codex:local-pi") is not None:
                break
    finally:
        await service.stop()
        db.get_system_principal = real_principal

    assert await db.get_session(sid) is None, "nothing should have been stored"
    assert await db.get_sync_cursor("codex:local-pi") is None, (
        "the cursor moved past an event that was never ingested"
    )

    # --- the restart that succeeds -----------------------------------------
    service = build_service(config, db)
    await service.start()
    try:
        for _ in range(40):
            if await db.get_session(sid) is not None:
                if await db.get_messages(sid):
                    break
            await asyncio.sleep(0.1)
    finally:
        await service.stop()

    session = await db.get_session(sid)
    assert session is not None, "the failed event was never replayed"
    assert {m["role"] for m in await db.get_messages(sid)} >= {"user", "assistant"}
    assert await db.get_sync_cursor("codex:local-pi"), (
        "a successful run should checkpoint"
    )


@pytest.mark.asyncio
async def test_build_service_returns_none_when_disabled(db, tmp_path):
    config = NerveConfig(
        workspace=tmp_path,
        sync=_sync_config(tmp_path / "s", tmp_path / "a", enabled=False),
    )
    assert build_service(config, db) is None


@pytest.mark.asyncio
async def test_build_service_skips_unknown_origin_types(db, tmp_path):
    sessions_dir = tmp_path / "sessions"
    archive_dir = tmp_path / "archived_sessions"
    sessions_dir.mkdir()
    archive_dir.mkdir()
    cfg = CodexSyncConfig(
        enabled=True,
        workspace_filter=CodexWorkspaceFilterConfig(
            mode="nerve_workspace",
        ),
        origins=[
            CodexOriginConfig(
                id="bogus",
                type="not_a_real_type",  # type: ignore[arg-type]
            ),
        ],
    )
    config = NerveConfig(workspace=tmp_path)
    config.sync.codex = cfg
    # No valid origins → build_service returns None
    assert build_service(config, db) is None


def _sync_config(
    sessions_dir: Path,
    archive_dir: Path,
    *,
    enabled: bool = True,
):
    """Helper — build a SyncConfig with a Codex origin pointing at
    the supplied test directories."""
    from nerve.config import SyncConfig

    sync = SyncConfig()
    sync.codex = CodexSyncConfig(
        enabled=enabled,
        workspace_filter=CodexWorkspaceFilterConfig(
            mode="nerve_workspace",
        ),
        origins=[
            CodexOriginConfig(
                id="local-pi",
                type="local_rollout",
                path=str(sessions_dir),
                archive_path=str(archive_dir),
                poll_interval_seconds=0.1,
            ),
        ],
    )
    return sync
