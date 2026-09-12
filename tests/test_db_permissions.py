"""The state directory and the database files are owner-only — or the
signing secret does not go there.

``nerve.db`` can hold the JWT signing secret generated for installs without
``auth.jwt_secret``; a configured secret used to live only in a 0600 file, so
the files that carry the generated one must be no weaker. ``Database.connect``
asserts the modes on every open — fresh databases, installs created before
this existed under a permissive umask, databases put in place by a restore —
and *verifies* them afterwards, because a ``chmod`` that succeeds on a
filesystem without modes changes nothing. What it cannot secure it reports;
the bootstrap then refuses to generate a secret there, and refuses to start at
all unless a configured ``auth.jwt_secret`` makes the database irrelevant.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import stat
from pathlib import Path

import pytest

import nerve.db.base as base
from nerve.config import AuthConfig, NerveConfig
from nerve.db import Database
from nerve.db.accounts import JWT_SECRET_NAME
from nerve.gateway.auth import pinned_jwt_secret
from nerve.migrate import InsecureSecretStorage, bootstrap_identity, ensure_jwt_secret

_SECRET = "configured-secret-padded-to-thirty-two-bytes"


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


@pytest.fixture(params=[0o022, 0o000], ids=["umask-022", "umask-000"])
def permissive_umask(request):
    old = os.umask(request.param)
    yield request.param
    os.umask(old)


def _wide_open_install(state: Path) -> Path:
    """An install from before the hardening, or a database a restore copied
    with the bundle's modes: the directory is 0755 and the files 0644."""
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
    return db_path


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
        assert db.state_secured and db.unsecured_files == []
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_existing_wide_open_files_are_tightened(tmp_path):
    db_path = _wide_open_install(tmp_path / "state")
    db = Database(db_path)
    await db.connect()
    try:
        assert _mode(db_path.parent) == 0o700
        assert _mode(db_path) == 0o600
        for suffix in ("-wal", "-shm"):
            sidecar = Path(f"{db_path}{suffix}")
            if sidecar.exists():
                assert _mode(sidecar) == 0o600, sidecar
        assert db.state_secured
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


class TestWhenTheFilesystemCannotBeTightened:
    """Two ways a filesystem defeats ``chmod``: refusing it, and accepting it
    while changing nothing. Both must be caught by the verifying ``stat``."""

    @pytest.fixture
    def wide_db(self, tmp_path) -> Path:
        """Built *before* chmod is defeated: the setup itself needs it."""
        return _wide_open_install(tmp_path / "state")

    @pytest.fixture(params=["chmod-raises", "chmod-is-a-no-op"])
    def defeated_chmod(self, request, monkeypatch, wide_db):
        if request.param == "chmod-raises":
            def chmod(path, mode, *args, **kwargs):
                raise PermissionError(f"chmod refused for {path}")
        else:
            def chmod(path, mode, *args, **kwargs):
                return None  # "succeeds", changes nothing
        monkeypatch.setattr(base.os, "chmod", chmod)
        return request.param

    @pytest.mark.asyncio
    async def test_connect_records_what_stayed_open(self, wide_db, defeated_chmod, caplog):
        db_path = wide_db
        db = Database(db_path)
        with caplog.at_level(logging.ERROR, logger="nerve.db.base"):
            await db.connect()  # opening is allowed; deciding is the bootstrap's job
        try:
            assert not db.state_secured
            assert (db_path, 0o644) in db.unsecured_files
            assert (Path(f"{db_path}-wal"), 0o644) in db.unsecured_files
            assert _mode(db_path) == 0o644  # nothing changed, and nothing pretends it did
            assert any(
                "readable by other users" in r.getMessage() and str(db_path) in r.getMessage()
                and "0644" in r.getMessage() and "0600" in r.getMessage()
                for r in caplog.records
            ), [r.getMessage() for r in caplog.records]
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_a_configured_secret_lets_startup_continue_with_an_error(
        self, wide_db, defeated_chmod, caplog,
    ):
        """Nothing secret needs the database, so the gateway starts — but the
        operator is told, once, with the path and the modes."""
        db_path = wide_db
        db = Database(db_path)
        await db.connect()
        try:
            config = NerveConfig(auth=AuthConfig(jwt_secret=_SECRET))
            with caplog.at_level(logging.ERROR, logger="nerve.migrate"):
                report = await bootstrap_identity(db, config)
            assert report.bootstrapped_account
            assert pinned_jwt_secret() == _SECRET
            assert await db.get_instance_secret(JWT_SECRET_NAME) is None
            errors = [
                r.getMessage() for r in caplog.records
                if r.name == "nerve.migrate" and r.levelno == logging.ERROR
            ]
            assert len(errors) == 1, errors
            assert str(db_path) in errors[0] and "0644" in errors[0] and "0600" in errors[0]
            assert "auth.jwt_secret is configured" in errors[0]
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_without_a_configured_secret_startup_refuses(self, wide_db, defeated_chmod):
        """No secret to fall back on: generating one into a world-readable file
        would hand every local user the keys, so the bootstrap raises — before
        it writes anything — and names both ways out."""
        db_path = wide_db
        db = Database(db_path)
        await db.connect()
        try:
            with pytest.raises(InsecureSecretStorage) as ei:
                await bootstrap_identity(db, NerveConfig())
            message = str(ei.value)
            assert str(db_path) in message and "0644" in message and "0600" in message
            assert "chmod" in message                       # remedy one
            assert "auth.jwt_secret" in message             # remedy two
            assert "config.local.yaml" in message and "environment" in message
            assert await db.count_accounts() == 0           # nothing was written
            assert await db.get_instance_secret(JWT_SECRET_NAME) is None
            assert pinned_jwt_secret() == ""
            # The secret step alone refuses the same way.
            with pytest.raises(InsecureSecretStorage):
                await ensure_jwt_secret(db, NerveConfig())
            assert await db.get_instance_secret(JWT_SECRET_NAME) is None
        finally:
            await db.close()


def test_restore_re_tightens_the_database_file():
    """The restore side of the same promise: a bundle written by an older
    release (or by hand) may carry nerve.db at 0644, and it should not sit
    world-readable until the daemon's first start."""
    from nerve import backup

    assert "nerve.db" in backup._SECRET_RESTORE_PATHS
