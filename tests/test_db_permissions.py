"""The state directory and the database files are owner-only.

``nerve.db`` can hold the JWT signing secret generated for installs without
``auth.jwt_secret``; a configured secret used to live only in a 0600 file, so
the files that carry the generated one must be no weaker. ``Database.connect``
asserts the modes on every open — fresh databases, installs created before
this existed under a permissive umask, and databases put in place by a
restore — and warns rather than fails when a filesystem refuses.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import stat
from pathlib import Path

import pytest

import nerve.db.base as base
from nerve.db import Database


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


@pytest.fixture(params=[0o022, 0o000], ids=["umask-022", "umask-000"])
def permissive_umask(request):
    old = os.umask(request.param)
    yield request.param
    os.umask(old)


@pytest.mark.asyncio
async def test_fresh_database_is_owner_only(tmp_path, permissive_umask):
    db_path = tmp_path / "state" / "nerve.db"
    db = Database(db_path)
    await db.connect()
    try:
        # A write so the WAL sidecars exist as they would on a real install.
        await db._write("CREATE TABLE IF NOT EXISTS t (x INTEGER)")
        assert _mode(db_path.parent) == 0o700
        assert _mode(db_path) == 0o600
        for suffix in ("-wal", "-shm"):
            sidecar = Path(f"{db_path}{suffix}")
            if sidecar.exists():
                assert _mode(sidecar) == 0o600, sidecar
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_existing_wide_open_files_are_tightened(tmp_path):
    """An install from before the hardening, or a database a restore copied
    with the bundle's modes: the directory is 0755 and the files 0644."""
    state = tmp_path / "state"
    state.mkdir(mode=0o755)
    os.chmod(state, 0o755)
    db_path = state / "nerve.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE schema_version (version INTEGER)")
    conn.commit()
    conn.close()
    stale_wal = Path(f"{db_path}-wal")
    stale_wal.touch()
    for p in (db_path, stale_wal):
        os.chmod(p, 0o644)
    assert _mode(state) == 0o755 and _mode(db_path) == 0o644

    db = Database(db_path)
    await db.connect()
    try:
        assert _mode(state) == 0o700
        assert _mode(db_path) == 0o600
        for suffix in ("-wal", "-shm"):
            sidecar = Path(f"{db_path}{suffix}")
            if sidecar.exists():
                assert _mode(sidecar) == 0o600, sidecar
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_a_filesystem_without_modes_warns_and_still_starts(tmp_path, monkeypatch, caplog):
    db_path = tmp_path / "state" / "nerve.db"

    def refuse(path, mode, *args, **kwargs):
        raise PermissionError(f"chmod refused for {path}")

    monkeypatch.setattr(base.os, "chmod", refuse)
    db = Database(db_path)
    with caplog.at_level(logging.WARNING, logger="nerve.db.base"):
        await db.connect()
    try:
        assert db.db is not None  # startup went ahead
        assert any(
            "Could not restrict permissions" in rec.getMessage() and "signing secret" in rec.getMessage()
            for rec in caplog.records
        ), [rec.getMessage() for rec in caplog.records]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_hardening_is_idempotent_across_reconnects(tmp_path):
    db_path = tmp_path / "state" / "nerve.db"
    for _ in range(2):
        db = Database(db_path)
        await db.connect()
        await db.close()
    assert _mode(db_path.parent) == 0o700
    assert _mode(db_path) == 0o600


def test_restore_re_tightens_the_database_file():
    """The restore side of the same promise: a bundle written by an older
    release (or by hand) may carry nerve.db at 0644, and it should not sit
    world-readable until the daemon's first start."""
    from nerve import backup

    assert "nerve.db" in backup._SECRET_RESTORE_PATHS
