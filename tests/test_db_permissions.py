"""State-file security: the directory and database files are owner-only, or the
instance does not open them.

``nerve.db`` holds accounts, actors and history, and may hold the generated JWT
signing secret. ``Database.connect`` therefore, on every open and in this order:

* inspects before touching anything, and **refuses to open** — no migration, no
  repair — when a database file, a sidecar or the state directory is
  group/world-**writable**, or when a mode cannot be read (**uninspectable**).
  Another user may have altered the contents; that is evidence the operator
  acknowledges by fixing the modes by hand, not something to chmod away;
* repairs **read** exposure: makes the directory ``0700`` and the files ``0600``
  and *verifies* the result (a ``chmod`` that a mode-less filesystem accepts but
  ignores is caught). A file that stays readable is fatal at bootstrap unless a
  configured secret means nothing secret is stored there;
* rotates a stored signing secret that was readable before repair, because it
  may already have been copied — on disk *and* in this process's pin, so memory
  and disk never disagree about which key is live.

The policy lives in ``connect()``, so every opener — the gateway, each CLI
command, the installer — gets exactly this (the CLI's own refusal is covered in
``test_cli_openers.py``).
"""

from __future__ import annotations

import os
import sqlite3
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

import nerve.db.base as base
from nerve.config import AuthConfig, NerveConfig
from nerve.db import Database, init_db
from nerve.db.accounts import JWT_SECRET_NAME
from nerve.gateway.auth import (
    create_token,
    decode_token,
    effective_jwt_secret,
    pin_jwt_secret,
    pinned_jwt_secret,
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


def _tables(path: Path) -> set[str]:
    conn = sqlite3.connect(str(path))
    try:
        return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()


@pytest.fixture(params=[0o022, 0o000], ids=["umask-022", "umask-000"])
def permissive_umask(request):
    old = os.umask(request.param)
    yield request.param
    os.umask(old)


async def _make_db(path: Path) -> None:
    """A secured, migrated, empty database, then closed. Nothing is pinned:
    only the bootstrap pins, and this never bootstraps."""
    db = Database(path)
    await db.connect()
    await db.close()


async def _make_db_with_secret(path: Path, secret: str) -> None:
    """A secured DB that holds ``secret`` as its jwt_secret, then closed."""
    db = Database(path)
    await db.connect()
    try:
        await db.ensure_instance_secret(JWT_SECRET_NAME, secret)
    finally:
        await db.close()


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
async def test_a_directory_created_by_connect_is_owner_only_even_under_umask_000(
    tmp_path, permissive_umask,
):
    """The directory connect() itself creates must not trip its own refusal:
    it is created 0700, whatever the umask, and a 0777 pre-existing one is
    judged as found (below)."""
    db_path = tmp_path / "state" / "nested" / "nerve.db"
    await _make_db(db_path)
    assert _mode(db_path.parent) == 0o700


def test_nerve_creates_its_own_state_directory_owner_only(tmp_path, permissive_umask, monkeypatch):
    """Every place Nerve creates the state directory goes through this and
    gets 0700 under any umask, so the refusal below never fires on a
    directory Nerve made. An existing directory is left as found — judging it
    is connect()'s job."""
    from nerve import paths

    home = tmp_path / "home" / ".nerve"
    monkeypatch.setenv("NERVE_HOME", str(home))
    assert paths.ensure_nerve_home() == home
    assert _mode(home) == 0o700
    os.chmod(home, 0o755)
    assert paths.ensure_nerve_home() == home  # idempotent
    assert _mode(home) == 0o755  # not re-tightened here


@pytest.mark.asyncio
async def test_a_readable_file_is_repaired_to_owner_only(tmp_path):
    db_path = tmp_path / "state" / "nerve.db"
    await _make_db(db_path)
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
    """Read/traverse on the directory is fine; only write on it matters. It is
    still tightened to 0700 on the way through."""
    state = tmp_path / "state"
    state.mkdir()
    os.chmod(state, 0o755)  # explicit: a umask of 002 would make it 0775, which *is* a hazard
    db_path = state / "nerve.db"
    await _make_db(db_path)
    os.chmod(state, 0o755)

    db = Database(db_path)
    await db.connect()
    try:
        assert db.state_secured
        assert not db.state_permissions.writable
        assert _mode(state) == 0o700
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_hardening_is_idempotent_across_reconnects(tmp_path):
    db_path = tmp_path / "state" / "nerve.db"
    for _ in range(2):
        await _make_db(db_path)
    assert _mode(db_path.parent) == 0o700
    assert _mode(db_path) == 0o600


# --------------------------------------------------------------------------- #
#  F17 — writable or uninspectable state is refused at open, unrepaired        #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestRefusesToOpenWritableState:
    """Write access by another user is an integrity question, not a mode to
    fix: nothing is opened, migrated or repaired, whatever ``auth.jwt_secret``
    says (the configuration is not even consulted — the refusal is in
    ``connect()``), and the message names the manual remedy."""

    @pytest.mark.parametrize("mode", [0o666, 0o660, 0o606], ids=["0666", "group-w", "world-w"])
    async def test_a_writable_database_file_is_refused_before_anything_happens(
        self, tmp_path, mode,
    ):
        db_path = tmp_path / "state" / "nerve.db"
        await _make_db(db_path)
        os.chmod(db_path, mode)

        db = Database(db_path)
        with pytest.raises(InsecureStateStorage) as ei:
            await db.connect()
        assert db._db is None  # never opened
        assert _mode(db_path) == mode  # and not repaired: the evidence stands
        msg = str(ei.value)
        assert "Refusing to open" in msg and str(db_path) in msg
        assert f"{mode:04o}" in msg and "altered" in msg
        assert f"chmod 700 {db_path.parent}" in msg and f"chmod 600 {db_path}" in msg

    async def test_no_migration_runs_on_a_refused_database(self, tmp_path):
        """A database from older code, left writable: refused *before* the
        schema is touched, so nothing of this version is written into a file
        another user may have altered."""
        state = tmp_path / "state"
        state.mkdir(mode=0o700)
        old = state / "nerve.db"
        conn = sqlite3.connect(str(old))
        conn.execute("CREATE TABLE schema_version (version INTEGER)")
        conn.execute("INSERT INTO schema_version VALUES (1)")
        conn.commit()
        conn.close()
        os.chmod(old, 0o666)

        with pytest.raises(InsecureStateStorage):
            await Database(old).connect()
        assert _tables(old) == {"schema_version"}  # untouched
        assert not Path(f"{old}-wal").exists()

    @pytest.mark.parametrize(
        "mode", [0o777, 0o770, 0o707, 0o775], ids=["0777", "group-w", "world-w", "0775-umask-002"],
    )
    async def test_a_writable_directory_is_refused(self, tmp_path, mode):
        """0775 is what a plain mkdir leaves under a 002 umask — a ~/.nerve an
        older version created on Ubuntu. Group-writable is writable: refused
        once, with the chmod as the acknowledgement."""
        db_path = tmp_path / "state" / "nerve.db"
        await _make_db(db_path)
        os.chmod(db_path.parent, mode)

        with pytest.raises(InsecureStateStorage) as ei:
            await Database(db_path).connect()
        assert _mode(db_path.parent) == mode  # not repaired
        assert str(db_path.parent) in str(ei.value)

    async def test_a_writable_sidecar_is_refused(self, tmp_path):
        db_path = tmp_path / "state" / "nerve.db"
        await _make_db(db_path)
        wal = Path(f"{db_path}-wal")
        wal.touch(mode=0o600)
        os.chmod(wal, 0o666)

        with pytest.raises(InsecureStateStorage) as ei:
            await Database(db_path).connect()
        assert str(wal) in str(ei.value)
        assert _mode(wal) == 0o666

    async def test_a_configured_secret_does_not_excuse_writable_state(self, tmp_path):
        """Round 3 established this at bootstrap; it now holds at open, where
        no configuration exists to consult. The bootstrap check stays as the
        backstop and agrees."""
        db_path = tmp_path / "state" / "nerve.db"
        await _make_db(db_path)
        os.chmod(db_path, 0o666)
        with pytest.raises(InsecureStateStorage):
            await Database(db_path).connect()
        # Same verdict from the bootstrap layer, should anything ever reach it
        # with the same finding.
        from nerve.migrate import _refuse_insecure_secret_storage

        stub = SimpleNamespace(
            state_permissions=base.StatePermissions(writable=[(db_path, 0o666)]),
            db_path=db_path,
        )
        with pytest.raises(InsecureStateStorage):
            _refuse_insecure_secret_storage(
                stub, NerveConfig(auth=AuthConfig(jwt_secret=_CONFIGURED)), log=False,
            )

    async def test_the_manual_remedy_then_opens_normally(self, tmp_path):
        db_path = tmp_path / "state" / "nerve.db"
        await _make_db(db_path)
        os.chmod(db_path, 0o666)
        os.chmod(db_path.parent, 0o777)
        with pytest.raises(InsecureStateStorage):
            await Database(db_path).connect()

        os.chmod(db_path.parent, 0o700)  # the operator acknowledges
        os.chmod(db_path, 0o600)
        db = Database(db_path)
        await db.connect()
        try:
            assert db.state_secured
        finally:
            await db.close()

    async def test_the_package_level_opener_is_covered_too(self, tmp_path):
        """init_db() is Database.connect() underneath; there is no opener that
        bypasses the policy."""
        db_path = tmp_path / "state" / "nerve.db"
        await _make_db(db_path)
        os.chmod(db_path, 0o666)
        with pytest.raises(InsecureStateStorage):
            await init_db(db_path)


@pytest.mark.asyncio
class TestUninspectableIsRefused:
    """A mode that cannot be read is not "probably fine": an attacker cannot
    make a file uninspectable to hide a wide mode, but a transient failure
    must still fail closed rather than let the open proceed on a guess."""

    async def test_a_one_shot_stat_failure_refuses_the_open(self, tmp_path):
        db_path = tmp_path / "state" / "nerve.db"
        await _make_db(db_path)

        real_mode_of = base._mode_of
        calls = {"n": 0}

        def flaky_mode_of(path):
            if str(path) == str(db_path):
                calls["n"] += 1
                if calls["n"] == 1:
                    return None  # the first inspection of the db fails
            return real_mode_of(path)

        base_mode_of = base._mode_of
        base._mode_of = flaky_mode_of
        try:
            with pytest.raises(InsecureStateStorage) as ei:
                await Database(db_path).connect()
            assert "cannot be read" in str(ei.value) and str(db_path) in str(ei.value)
        finally:
            base._mode_of = base_mode_of
        assert base._mode_of is real_mode_of

        # It was transient: the next open — after the operator looked — works.
        db = Database(db_path)
        await db.connect()
        try:
            assert db.state_secured
        finally:
            await db.close()

    async def test_an_uninspectable_directory_refuses_the_open(self, tmp_path, monkeypatch):
        db_path = tmp_path / "state" / "nerve.db"
        await _make_db(db_path)
        real_mode_of = base._mode_of
        monkeypatch.setattr(
            base, "_mode_of",
            lambda p: None if str(p) == str(db_path.parent) else real_mode_of(p),
        )
        with pytest.raises(InsecureStateStorage) as ei:
            await Database(db_path).connect()
        assert str(db_path.parent) in str(ei.value)

    async def test_a_verifying_read_that_fails_after_repair_refuses_too(
        self, tmp_path, monkeypatch,
    ):
        """The pre-open inspection passed (0644 is read exposure, repairable);
        the read that *verifies* the chmod then fails. The repair is not
        trusted: the file is uninspectable, the connection is closed again and
        the open is refused — the backstop behind the first pass."""
        db_path = tmp_path / "state" / "nerve.db"
        await _make_db(db_path)
        os.chmod(db_path, 0o644)

        real_mode_of = base._mode_of
        calls = {"n": 0}

        def flaky_mode_of(path):
            if str(path) == str(db_path):
                calls["n"] += 1
                if calls["n"] >= 3:  # 1: inspect, 2: repair's pre-read, 3: verify
                    return None
            return real_mode_of(path)

        monkeypatch.setattr(base, "_mode_of", flaky_mode_of)
        db = Database(db_path)
        with pytest.raises(InsecureStateStorage) as ei:
            await db.connect()
        assert db._db is None
        assert "cannot be read" in str(ei.value) and str(db_path) in str(ei.value)


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
        await db.connect()  # read exposure is not a refusal; it still rotates
        try:
            assert db.state_permissions.readable and not db.state_secured
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
        await _make_db(db_path)
        os.chmod(db_path, 0o644)

        db = Database(db_path)
        await db.connect()  # repaired; no key row → no rotation
        try:
            report = await bootstrap_identity(db, NerveConfig())
            assert report.generated_jwt_secret
            assert await db.get_instance_secret(JWT_SECRET_NAME)
        finally:
            await db.close()


# --------------------------------------------------------------------------- #
#  F20 — rotation reaches the process pin                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestRotationReachesThePin:
    """Same process, no ``unpin_jwt_secret()`` between the opens: a verifier
    that kept the retired key pinned would go on accepting tokens minted with
    the copy. Retiring the row drops a matching pin, so requests fail closed
    until the bootstrap pins the replacement."""

    async def test_the_pinned_compromised_key_is_unpinned_and_then_replaced(self, tmp_path):
        db_path = tmp_path / "state" / "nerve.db"
        config = NerveConfig()

        db = Database(db_path)
        await db.connect()
        await bootstrap_identity(db, config)  # generates S1 and pins it
        s1 = await db.get_instance_secret(JWT_SECRET_NAME)
        await db.close()
        assert s1 and pinned_jwt_secret() == s1
        s1_token = create_token(s1)

        os.chmod(db_path, 0o644)  # exposed; someone copies S1

        db2 = Database(db_path)
        await db2.connect()  # repairs, retires S1 on disk *and* unpins it
        try:
            assert await db2.get_instance_secret(JWT_SECRET_NAME) is None
            assert pinned_jwt_secret() == ""
            assert effective_jwt_secret(config) == ""  # fail closed: nothing verifies
            with pytest.raises(Exception):
                decode_token(s1_token, effective_jwt_secret(config))

            await bootstrap_identity(db2, config)  # pins the replacement S3
            s3 = await db2.get_instance_secret(JWT_SECRET_NAME)
            assert s3 and s3 != s1
            assert effective_jwt_secret(config) == s3
            with pytest.raises(Exception):
                decode_token(s1_token, effective_jwt_secret(config))  # S1 rejected
            assert decode_token(create_token(s3), effective_jwt_secret(config))  # S3 accepted
        finally:
            await db2.close()

    async def test_a_pin_that_is_not_the_retired_key_is_left_alone(self, tmp_path):
        """A configured secret is pinned; the database happens to hold a stale
        stored key that gets exposed. That row is retired; the pin — which
        never was that key — stays, and requests keep verifying."""
        db_path = tmp_path / "state" / "nerve.db"
        await _make_db_with_secret(db_path, _S1)
        pin_jwt_secret(_CONFIGURED)
        os.chmod(db_path, 0o644)

        db = Database(db_path)
        await db.connect()
        try:
            assert await db.get_instance_secret(JWT_SECRET_NAME) is None
            assert pinned_jwt_secret() == _CONFIGURED
        finally:
            await db.close()


@pytest.mark.asyncio
class TestAFailedConnectLeavesNothingOpen:
    """F32: ``aiosqlite.connect`` starts a non-daemon thread. Anything that
    fails after it — a migration, the permission backstop, a cancellation — has
    to close the connection, or the caller gets an exception *and* a thread
    that keeps the process alive, with a retry opening another one."""

    async def test_a_failed_migration_closes_the_connection(self, tmp_path, monkeypatch):
        import asyncio
        import threading

        db_path = tmp_path / "state" / "nerve.db"
        before = threading.active_count()

        async def boom(_conn):
            raise sqlite3.OperationalError("migration exploded")

        monkeypatch.setattr(base, "run_migrations", boom)
        db = Database(db_path)
        with pytest.raises(sqlite3.OperationalError, match="migration exploded"):
            await db.connect()

        assert db._db is None
        for _ in range(50):  # the connection thread exits once it is closed
            if threading.active_count() <= before:
                break
            await asyncio.sleep(0.02)
        assert threading.active_count() <= before

    async def test_the_global_is_not_published_by_a_failed_open(
        self, tmp_path, monkeypatch,
    ):
        """``init_db`` used to assign the global first, so a refused open left
        ``get_db()`` handing out a database nobody can use."""
        import nerve.db as db_pkg

        db_path = tmp_path / "state" / "nerve.db"
        await _make_db(db_path)
        os.chmod(db_path, 0o666)  # the state-file policy refuses this
        monkeypatch.setattr(db_pkg, "_db", None)

        with pytest.raises(InsecureStateStorage):
            await init_db(db_path)
        assert db_pkg._db is None
        with pytest.raises(RuntimeError, match="not initialized"):
            await db_pkg.get_db()


def test_the_round2_exception_name_still_resolves():
    assert InsecureSecretStorage is InsecureStateStorage
    assert InsecureStateStorage is base.InsecureStateStorage


def test_restore_re_tightens_the_database_file():
    from nerve import backup

    assert "nerve.db" in backup._SECRET_RESTORE_PATHS
