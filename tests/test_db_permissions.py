"""State-file security: the directory and database files are owner-only, or the
instance does not trust them.

``nerve.db`` holds accounts, actors and history, and may hold the generated JWT
signing secret. ``Database.connect`` therefore, on every open:

* makes the state directory ``0700`` and ``nerve.db`` + its sidecars ``0600``,
  and *verifies* the result (a ``chmod`` that a mode-less filesystem accepts but
  ignores is caught);
* treats a **writable** or **uninspectable** file/directory as an integrity
  hazard — fatal regardless of ``auth.jwt_secret``;
* treats a **readable** database file as a confidentiality hazard — fatal
  unless a configured secret means nothing secret is stored there;
* rotates a stored signing secret that was group/world-readable before repair,
  because it may already have been copied.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

import nerve.db.base as base
from nerve.config import AuthConfig, NerveConfig
from nerve.db import Database
from nerve.db.accounts import JWT_SECRET_NAME
from nerve.gateway.auth import (
    create_token,
    decode_token,
    effective_jwt_secret,
    unpin_jwt_secret,
)
from nerve.migrate import (
    InsecureSecretStorage,
    InsecureStateStorage,
    bootstrap_identity,
)

_CONFIGURED = "configured-secret-padded-to-thirty-two-bytes!!"
_S1 = "s1-generated-secret-padded-to-32-bytes!!"


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


@pytest.fixture(params=[0o022, 0o000], ids=["umask-022", "umask-000"])
def permissive_umask(request):
    old = os.umask(request.param)
    yield request.param
    os.umask(old)


async def _make_db_with_secret(path: Path, secret: str) -> None:
    """A secured DB that holds ``secret`` as its jwt_secret, then closed."""
    db = Database(path)
    await db.connect()
    try:
        await db.ensure_instance_secret(JWT_SECRET_NAME, secret)
    finally:
        await db.close()
    unpin_jwt_secret()  # each Database open is a fresh "process"


# --------------------------------------------------------------------------- #
#  The good cases                                                              #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_fresh_database_is_owner_only(tmp_path, permissive_umask):
    db_path = tmp_path / "state" / "nerve.db"
    db = Database(db_path)
    await db.connect()
    try:
        await db._write("CREATE TABLE IF NOT EXISTS t (x INTEGER)")  # force the WAL sidecars
        assert _mode(db_path.parent) == 0o700
        assert _mode(db_path) == 0o600
        for suffix in ("-wal", "-shm"):
            sidecar = Path(f"{db_path}{suffix}")
            if sidecar.exists():
                assert _mode(sidecar) == 0o600, sidecar
        assert db.state_secured
        assert db.state_permissions.secured
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_a_readable_file_is_repaired_to_owner_only(tmp_path):
    db_path = tmp_path / "state" / "nerve.db"
    db = Database(db_path)
    await db.connect()
    await db.close()
    unpin_jwt_secret()
    os.chmod(db_path, 0o644)  # exposed, no key stored → repaired, nothing to rotate

    db = Database(db_path)
    await db.connect()
    try:
        assert _mode(db_path) == 0o600
        assert db.state_secured
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_a_0755_directory_is_not_a_hazard(tmp_path):
    """Read/traverse on the directory is fine; only write on it matters."""
    state = tmp_path / "state"
    state.mkdir()
    db_path = state / "nerve.db"
    db = Database(db_path)
    await db.connect()
    await db.close()
    unpin_jwt_secret()
    os.chmod(state, 0o755)

    db = Database(db_path)
    await db.connect()
    try:
        assert db.state_secured
        assert not db.state_permissions.writable
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_hardening_is_idempotent_across_reconnects(tmp_path):
    db_path = tmp_path / "state" / "nerve.db"
    for _ in range(2):
        db = Database(db_path)
        await db.connect()
        await db.close()
        unpin_jwt_secret()
    assert _mode(db_path.parent) == 0o700
    assert _mode(db_path) == 0o600


# --------------------------------------------------------------------------- #
#  F9 — writable state is an integrity hazard, fatal regardless of the secret  #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestWritableStateIsFatal:
    async def _prepare(self, tmp_path) -> Path:
        db_path = tmp_path / "state" / "nerve.db"
        db = Database(db_path)
        await db.connect()
        await db.close()
        unpin_jwt_secret()
        return db_path

    @pytest.mark.parametrize("configured", [False, True], ids=["no-secret", "configured"])
    async def test_group_world_writable_db_refuses_even_with_a_configured_secret(
        self, tmp_path, monkeypatch, configured,
    ):
        db_path = await self._prepare(tmp_path)
        os.chmod(db_path, 0o666)
        # Freeze the mode: connect() must not be able to repair it away.
        monkeypatch.setattr(base.os, "chmod", lambda *a, **k: None)

        db = Database(db_path)
        await db.connect()
        try:
            assert db.state_permissions.writable
            config = NerveConfig(auth=AuthConfig(jwt_secret=_CONFIGURED if configured else ""))
            with pytest.raises(InsecureStateStorage) as ei:
                await bootstrap_identity(db, config)
            msg = str(ei.value)
            assert "modify" in msg and str(db_path) in msg
            assert await db.count_accounts() == 0  # nothing written
        finally:
            await db.close()

    async def test_world_writable_directory_refuses(self, tmp_path, monkeypatch):
        db_path = await self._prepare(tmp_path)
        os.chmod(db_path.parent, 0o777)
        monkeypatch.setattr(base.os, "chmod", lambda *a, **k: None)

        db = Database(db_path)
        await db.connect()
        try:
            assert db.state_permissions.writable  # the directory is in it
            with pytest.raises(InsecureStateStorage):
                await bootstrap_identity(db, NerveConfig(auth=AuthConfig(jwt_secret=_CONFIGURED)))
        finally:
            await db.close()


# --------------------------------------------------------------------------- #
#  F10 — an uninspectable file fails closed                                    #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestUninspectableFailsClosed:
    @pytest.mark.parametrize("when", ["initial", "post-chmod"])
    async def test_uninspectable_db_is_unsecured(self, tmp_path, monkeypatch, when):
        db_path = tmp_path / "state" / "nerve.db"
        db0 = Database(db_path)
        await db0.connect()
        await db0.close()
        unpin_jwt_secret()

        real_mode_of = base._mode_of
        calls = {"n": 0}

        def flaky_mode_of(path):
            if str(path) == str(db_path):
                calls["n"] += 1
                # "initial": the very first inspection of the db is uninspectable.
                # "post-chmod": the first read succeeds (it is 0644, so a chmod
                # follows) and the *verifying* read after the chmod fails.
                if (when == "initial") or (when == "post-chmod" and calls["n"] >= 2):
                    return None
            return real_mode_of(path)

        os.chmod(db_path, 0o644)  # so a chmod happens, reaching the verifying read
        monkeypatch.setattr(base, "_mode_of", flaky_mode_of)

        db = Database(db_path)
        await db.connect()
        try:
            assert not db.state_secured
            assert db_path in db.state_permissions.uninspectable
            with pytest.raises(InsecureStateStorage):
                await bootstrap_identity(db, NerveConfig(auth=AuthConfig(jwt_secret=_CONFIGURED)))
        finally:
            await db.close()


# --------------------------------------------------------------------------- #
#  F11 — a key exposed while the file was readable is rotated                  #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestExposedKeyIsRotated:
    async def test_automatic_repair_rotates_the_exposed_key(self, tmp_path):
        db_path = tmp_path / "state" / "nerve.db"
        await _make_db_with_secret(db_path, _S1)
        s1_token = create_token(_S1)

        os.chmod(db_path, 0o644)  # exposed; copy S1
        db = Database(db_path)
        await db.connect()  # repairs to 0600 AND rotates S1
        try:
            assert _mode(db_path) == 0o600
            assert await db.get_instance_secret(JWT_SECRET_NAME) is None
            # Bootstrap now generates a fresh S3, distinct from S1.
            await bootstrap_identity(db, NerveConfig())
            s3 = await db.get_instance_secret(JWT_SECRET_NAME)
            assert s3 and s3 != _S1
            assert effective_jwt_secret(NerveConfig()) == s3
            with pytest.raises(Exception):
                decode_token(s1_token, effective_jwt_secret(NerveConfig()))
        finally:
            await db.close()

    async def test_exposed_but_unrepairable_rotates_then_refuses(self, tmp_path, monkeypatch):
        db_path = tmp_path / "state" / "nerve.db"
        await _make_db_with_secret(db_path, _S1)
        os.chmod(db_path, 0o644)
        monkeypatch.setattr(base.os, "chmod", lambda *a, **k: None)  # cannot repair

        db = Database(db_path)
        await db.connect()  # cannot repair, but still rotates the compromised key
        try:
            assert await db.get_instance_secret(JWT_SECRET_NAME) is None  # rotated
            # No configured secret + still-readable file → refuse to generate one.
            with pytest.raises(InsecureStateStorage):
                await bootstrap_identity(db, NerveConfig())
        finally:
            await db.close()

    async def test_exposed_with_a_configured_secret_rotates_and_uses_config(self, tmp_path):
        db_path = tmp_path / "state" / "nerve.db"
        await _make_db_with_secret(db_path, _S1)
        s1_token = create_token(_S1)

        os.chmod(db_path, 0o644)
        db = Database(db_path)
        await db.connect()  # repairs + rotates S1
        try:
            config = NerveConfig(auth=AuthConfig(jwt_secret=_CONFIGURED))
            await bootstrap_identity(db, config)
            assert await db.get_instance_secret(JWT_SECRET_NAME) is None
            assert effective_jwt_secret(config) == _CONFIGURED
            with pytest.raises(Exception):
                decode_token(s1_token, effective_jwt_secret(config))
        finally:
            await db.close()

    async def test_a_readable_db_that_never_held_a_key_is_not_rotated(self, tmp_path):
        """A fresh 0644 database from old code that never stored a secret is not
        an exposure: nothing to rotate, and once repaired a secret generates
        normally."""
        db_path = tmp_path / "state" / "nerve.db"
        db0 = Database(db_path)
        await db0.connect()
        await db0.close()
        unpin_jwt_secret()
        os.chmod(db_path, 0o644)

        db = Database(db_path)
        await db.connect()  # repaired; no key row → no rotation
        try:
            report = await bootstrap_identity(db, NerveConfig())
            assert report.generated_jwt_secret
            assert await db.get_instance_secret(JWT_SECRET_NAME)
        finally:
            await db.close()


def test_the_round2_exception_name_still_resolves():
    assert InsecureSecretStorage is InsecureStateStorage


def test_restore_re_tightens_the_database_file():
    from nerve import backup

    assert "nerve.db" in backup._SECRET_RESTORE_PATHS
