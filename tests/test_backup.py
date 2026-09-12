"""Tests for nerve.backup — consistent snapshots, bundle round-trip, restore.

The databases run in WAL mode, so the snapshot must stay consistent under a
concurrent writer; restore must be verified and refuse to clobber a live or
non-empty target. These tests exercise the whole bundle lifecycle without a
running server (the CLI and lifespan task are thin wrappers over this module).
"""

from __future__ import annotations

import os
import sqlite3
import stat
import tarfile
import threading
import time
from pathlib import Path

import pytest

from nerve import backup as backup_mod
from nerve.backup import BackupError
from nerve.db import SCHEMA_VERSION


# --------------------------------------------------------------------------- #
#  Fixtures / helpers                                                          #
# --------------------------------------------------------------------------- #


def _make_nerve_db(path: Path, *, schema_version: int = SCHEMA_VERSION,
                   sessions: int = 7, messages: int = 11, tasks: int = 3) -> None:
    """Create a WAL-mode nerve.db with the tables the parity counts read."""
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(path) + suffix)
        if p.exists():
            p.unlink()
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE schema_version (version INTEGER)")
        conn.execute("INSERT INTO schema_version VALUES (?)", (schema_version,))
        conn.execute("CREATE TABLE sessions (id TEXT)")
        conn.execute("CREATE TABLE messages (id TEXT)")
        conn.execute("CREATE TABLE tasks (id TEXT)")
        conn.executemany("INSERT INTO sessions VALUES (?)",
                         [(str(i),) for i in range(sessions)])
        conn.executemany("INSERT INTO messages VALUES (?)",
                         [(str(i),) for i in range(messages)])
        conn.executemany("INSERT INTO tasks VALUES (?)",
                         [(str(i),) for i in range(tasks)])
        conn.commit()
    finally:
        conn.close()


def _make_memu_db(path: Path, *, items: int = 5) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE memu_memory_items (id TEXT)")
        conn.executemany("INSERT INTO memu_memory_items VALUES (?)",
                         [(str(i),) for i in range(items)])
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def nerve_dir(tmp_path: Path) -> Path:
    """A populated ~/.nerve replica, with secrets, state, and junk."""
    nd = tmp_path / "dot_nerve"
    nd.mkdir(mode=0o700)  # as Nerve creates it; a 002 umask would otherwise give 0775
    _make_nerve_db(nd / "nerve.db")
    _make_memu_db(nd / "memu.sqlite")

    # Secrets + state.
    (nd / "mcp-token").write_text("super-secret-token")
    os.chmod(nd / "mcp-token", 0o600)
    (nd / "telegram_sync.session").write_text("tg-session-blob")
    (nd / "config_dir").write_text(str(tmp_path / "cfg"))
    (nd / "cron").mkdir()
    (nd / "cron" / "jobs.yaml").write_text("jobs: []\n")
    (nd / "certs").mkdir()
    (nd / "certs" / "key.pem").write_text("PRIVATE-KEY")
    (nd / "memu-conversations").mkdir()
    (nd / "memu-conversations" / "c1.json").write_text("[]")
    (nd / "memu-manual").mkdir()
    (nd / "memu-resources").mkdir()

    # Junk that must NEVER be backed up.
    (nd / "nerve.log").write_text("x" * 5000)
    (nd / "nerve.pid").write_text("424242")
    (nd / "bin").mkdir()
    (nd / "bin" / "cli-proxy-api").write_text("BINARY-BLOB")
    (nd / "memu.backup.sqlite").write_text("STALE-BACKUP")
    (nd / "nerve.db-wal").write_text("WAL-SIDECAR")
    return nd


@pytest.fixture
def config_dir(tmp_path: Path) -> Path:
    cfg = tmp_path / "cfg"
    cfg.mkdir()
    (cfg / "config.local.yaml").write_text("anthropic_api_key: sk-secret\n")
    (cfg / "config.yaml").write_text("workspace: ~/ws\n")
    return cfg


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    """A workspace with BRAIN files and a lot of junk to exclude."""
    ws = tmp_path / "ws"
    ws.mkdir()
    # BRAIN
    (ws / "SOUL.md").write_text("soul")
    (ws / "MEMORY.md").write_text("memory")
    (ws / "memory").mkdir()
    (ws / "memory" / "people.md").write_text("people")
    (ws / "memory" / "tasks").mkdir()
    (ws / "memory" / "tasks" / "active").mkdir()
    (ws / "memory" / "tasks" / "active" / "t1.md").write_text("task one")
    (ws / "skills").mkdir()
    (ws / "skills" / "s1").mkdir()
    (ws / "skills" / "s1" / "SKILL.md").write_text("skill body")
    (ws / "scripts").mkdir()
    (ws / "scripts" / "helper.py").write_text("print('hi')")

    # Junk inside included dirs (must be pruned).
    (ws / "memory" / ".git").mkdir()
    (ws / "memory" / ".git" / "config").write_text("gitjunk")
    (ws / "skills" / "node_modules").mkdir()
    (ws / "skills" / "node_modules" / "dep.js").write_text("nodejunk")
    (ws / "scripts" / "__pycache__").mkdir()
    (ws / "scripts" / "__pycache__" / "helper.pyc").write_text("pycjunk")

    # Junk outside the include set (whole dirs ignored — not in the allowlist).
    (ws / "big-repo").mkdir()
    (ws / "big-repo" / "huge.bin").write_text("X" * 10000)
    (ws / "another-repo").mkdir()
    (ws / "another-repo" / "build.bin").write_text("Y" * 10000)
    return ws


def _bundle_members(path: Path) -> list[str]:
    comp = backup_mod._compression_for(path)
    with backup_mod._tar_reader(path, comp) as tar:
        return [m.name for m in tar]


# --------------------------------------------------------------------------- #
#  1. Consistent snapshot under a concurrent writer                            #
# --------------------------------------------------------------------------- #


def test_snapshot_consistent_with_concurrent_writer(tmp_path: Path):
    src = tmp_path / "nerve.db"
    _make_nerve_db(src, sessions=0, messages=10, tasks=0)

    stop = threading.Event()

    def writer():
        w = sqlite3.connect(str(src))
        i = 1000
        while not stop.is_set():
            try:
                w.execute("INSERT INTO messages VALUES (?)", (str(i),))
                w.commit()
                i += 1
            except sqlite3.OperationalError:
                pass
            time.sleep(0.001)
        w.close()

    t = threading.Thread(target=writer)
    t.start()
    try:
        dst = tmp_path / "snap.db"
        # Should not raise (integrity_check passes inside _snapshot_db).
        backup_mod._snapshot_db(src, dst)
    finally:
        stop.set()
        t.join()

    conn = sqlite3.connect(str(dst))
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        count = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    finally:
        conn.close()
    # The snapshot captured a consistent point in time; at least the initial
    # rows are present (more if the writer got ahead before the copy).
    assert count >= 10


# --------------------------------------------------------------------------- #
#  2. Round-trip: backup → verify → restore                                   #
# --------------------------------------------------------------------------- #


def test_backup_verify_restore_roundtrip(nerve_dir, workspace, config_dir, tmp_path):
    out = tmp_path / "out"
    result = backup_mod.create_backup(
        nerve_dir, workspace, out, config_dir=config_dir,
    )
    assert result.path.exists()
    assert backup_mod.BUNDLE_RE.match(result.path.name)
    assert result.counts == {
        "sessions": 7, "messages": 11, "tasks": 3, "memu_items": 5,
    }

    report = backup_mod.verify_bundle(result.path)
    assert report.ok, report.errors
    assert report.counts["sessions"] == 7

    # Restore into fresh dirs.
    nd2 = tmp_path / "restored_nerve"
    ws2 = tmp_path / "restored_ws"
    cfg2 = tmp_path / "restored_cfg"
    rep = backup_mod.restore_bundle(
        result.path, nd2, ws2, config_dir=cfg2,
    )
    assert rep.ok

    # DB row counts survive.
    conn = sqlite3.connect(str(nd2 / "nerve.db"))
    try:
        assert conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 7
        assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 11
    finally:
        conn.close()

    # Secrets restored with 0600 mode.
    assert (nd2 / "mcp-token").read_text() == "super-secret-token"
    assert (os.stat(nd2 / "mcp-token").st_mode & 0o777) == 0o600
    assert (os.stat(nd2 / "certs" / "key.pem").st_mode & 0o777) == 0o600

    # config.local.yaml restored to the explicit config dir.
    assert (cfg2 / "config.local.yaml").read_text() == "anthropic_api_key: sk-secret\n"

    # Workspace BRAIN restored.
    assert (ws2 / "SOUL.md").read_text() == "soul"
    assert (ws2 / "memory" / "tasks" / "active" / "t1.md").exists()
    assert (ws2 / "skills" / "s1" / "SKILL.md").exists()


def test_backup_excludes_junk(nerve_dir, workspace, config_dir, tmp_path):
    out = tmp_path / "out"
    result = backup_mod.create_backup(nerve_dir, workspace, out, config_dir=config_dir)
    members = _bundle_members(result.path)
    joined = "\n".join(members)

    # State junk excluded.
    assert "nerve.log" not in joined
    assert "nerve.pid" not in joined
    assert "/bin/" not in joined and not joined.endswith("/bin")
    assert "memu.backup" not in joined
    assert "db-wal" not in joined

    # Workspace junk excluded (dirs outside the BRAIN allowlist).
    assert "big-repo" not in joined
    assert "another-repo" not in joined
    assert "node_modules" not in joined
    assert "__pycache__" not in joined
    assert "/.git/" not in joined

    # BRAIN included.
    assert any(m.endswith("workspace/SOUL.md") for m in members)
    assert any(m.endswith("workspace/skills/s1/SKILL.md") for m in members)
    assert any(m.endswith("workspace/scripts/helper.py") for m in members)


def test_config_travels_in_the_bundle_but_is_not_written_back(
    nerve_dir, workspace, config_dir, tmp_path,
):
    """The two halves of the config asymmetry, on an ordinary bundle.

    The other restore test smuggles entries in to prove a hostile bundle is
    refused. This one uses a bundle this module wrote itself, because the
    surprising half is that our *own* config/ is not written back either — it is
    captured so nothing is lost, and withheld because tracked config comes from
    the git remote, not a tarball.
    """
    cfg = workspace / "config"
    (cfg / "cron").mkdir(parents=True)
    (cfg / "settings.yaml").write_text("timezone: UTC\n")
    (cfg / "cron" / "jobs.yaml").write_text("jobs:\n  - id: nightly\n")

    out = tmp_path / "out"
    result = backup_mod.create_backup(nerve_dir, workspace, out, config_dir=config_dir)

    # Captured.
    members = _bundle_members(result.path)
    assert any(m.endswith("workspace/config/settings.yaml") for m in members)
    assert any(m.endswith("workspace/config/cron/jobs.yaml") for m in members)

    # Not written back — and what is already on disk survives untouched.
    ws_out = tmp_path / "ws_restored"
    (ws_out / "config").mkdir(parents=True)
    (ws_out / "config" / "settings.yaml").write_text("timezone: Europe/Berlin\n")
    report = backup_mod.restore_bundle(result.path, tmp_path / "target", ws_out)

    assert (ws_out / "config" / "settings.yaml").read_text() == "timezone: Europe/Berlin\n"
    assert not (ws_out / "config" / "cron").exists()
    # The rest of the brain did come back.
    assert (ws_out / "SOUL.md").read_text() == "soul"
    assert (ws_out / "skills" / "s1" / "SKILL.md").exists()
    assert any("skipped" in w for w in report.warnings), report.warnings


def test_backup_captures_tracked_workspace_config(
    nerve_dir, workspace, config_dir, tmp_path,
):
    """The shareable config subtree has to survive a lost machine.

    Everything that decides *what the agent does on a schedule* lives here. A
    bundle that restores the identity files and the skills but not the settings
    or the cron jobs looks complete and is not, and the gap only shows up when
    someone restores it.
    """
    cfg = workspace / "config"
    (cfg / "cron" / "gates").mkdir(parents=True)
    (cfg / "settings.yaml").write_text("agent:\n  name: nerve\n")
    (cfg / "cron" / "jobs.yaml").write_text("jobs:\n  - id: nightly\n")
    (cfg / "cron" / "system.yaml").write_text("jobs:\n  - id: cleanup\n")
    (cfg / "cron" / "gates" / "quiet_hours.py").write_text("def gate():\n    return True\n")
    # Junk inside the subtree is still pruned like anywhere else.
    (cfg / "cron" / "gates" / "__pycache__").mkdir()
    (cfg / "cron" / "gates" / "__pycache__" / "quiet_hours.pyc").write_text("junk")

    out = tmp_path / "out"
    result = backup_mod.create_backup(nerve_dir, workspace, out, config_dir=config_dir)
    members = _bundle_members(result.path)

    for expected in (
        "workspace/config/settings.yaml",
        "workspace/config/cron/jobs.yaml",
        "workspace/config/cron/system.yaml",
        "workspace/config/cron/gates/quiet_hours.py",
    ):
        assert any(m.endswith(expected) for m in members), f"missing {expected}"
    assert "__pycache__" not in "\n".join(members)


def test_workspace_extra_excludes(nerve_dir, workspace, config_dir, tmp_path):
    # Plant a file the user wants excluded via config glob.
    (workspace / "memory" / "diagram.png").write_text("PNG")
    out = tmp_path / "out"
    result = backup_mod.create_backup(
        nerve_dir, workspace, out, config_dir=config_dir,
        workspace_excludes=["*.png"],
    )
    members = _bundle_members(result.path)
    assert not any(m.endswith("diagram.png") for m in members)
    assert any(m.endswith("memory/people.md") for m in members)


# --------------------------------------------------------------------------- #
#  3. --no-secrets / --state-only                                             #
# --------------------------------------------------------------------------- #


def test_no_secrets_omits_and_flags(nerve_dir, workspace, config_dir, tmp_path):
    out = tmp_path / "out"
    result = backup_mod.create_backup(
        nerve_dir, workspace, out, config_dir=config_dir, include_secrets=False,
    )
    members = _bundle_members(result.path)
    joined = "\n".join(members)
    assert not any(m.endswith("mcp-token") for m in members)
    assert "telegram_sync.session" not in joined
    assert "certs" not in joined
    assert "config.local.yaml" not in joined

    report = backup_mod.verify_bundle(result.path)
    assert report.manifest["flags"]["include_secrets"] is False
    # Non-secret state still present.
    assert any(m.endswith("state/cron/jobs.yaml") for m in members)


def _plant_instance_secret(db_path: Path) -> None:
    """The table v047 adds, holding the generated JWT signing secret."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "CREATE TABLE instance_secrets (name TEXT PRIMARY KEY, value TEXT NOT NULL, "
            "created_at TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO instance_secrets VALUES ('jwt_secret', 'planted-signing-secret', 't')"
        )
        conn.commit()
    finally:
        conn.close()


def _stored_secrets(db_path: Path) -> list[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        return [r[0] for r in conn.execute("SELECT value FROM instance_secrets").fetchall()]
    finally:
        conn.close()


def test_no_secrets_scrubs_the_stored_signing_secret(nerve_dir, workspace, config_dir, tmp_path):
    """The generated JWT signing secret lives inside nerve.db, so skipping
    files is not enough: whoever holds it can mint tokens for the live
    instance. It must be gone from a --no-secrets snapshot — and only there;
    the live database is not touched."""
    _plant_instance_secret(nerve_dir / "nerve.db")

    stripped = backup_mod.create_backup(
        nerve_dir, workspace, tmp_path / "out1", config_dir=config_dir, include_secrets=False,
    )
    staging = tmp_path / "x1"
    report = backup_mod.verify_bundle(stripped.path, extract_to=staging)
    assert report.ok, report.errors  # checksums were taken after the scrub
    assert _stored_secrets(staging / "state" / "nerve.db") == []
    assert "planted-signing-secret" not in (staging / "state" / "nerve.db").read_bytes().decode(
        "latin-1"
    )
    assert _stored_secrets(nerve_dir / "nerve.db") == ["planted-signing-secret"]

    kept = backup_mod.create_backup(
        nerve_dir, workspace, tmp_path / "out2", config_dir=config_dir, include_secrets=True,
    )
    staging = tmp_path / "x2"
    backup_mod.verify_bundle(kept.path, extract_to=staging)
    assert _stored_secrets(staging / "state" / "nerve.db") == ["planted-signing-secret"]


def test_restore_preserves_the_bootstrapped_identity_ids(workspace, config_dir, tmp_path):
    """Actor references must stay stable across a restore: sessions and
    messages will point at these ids for good, so a restored instance has to
    find the very same tenant, agent, system principal, owner and account —
    and the same signing secret, so live sessions keep verifying."""
    import asyncio

    from nerve.db import Database
    from nerve.db.accounts import JWT_SECRET_NAME

    nd = tmp_path / "real_nerve"
    nd.mkdir(mode=0o700)  # as Nerve creates it; a 002 umask would otherwise give 0775

    async def _bootstrap():
        db = Database(nd / "nerve.db")
        await db.connect()
        try:
            identity = await db.bootstrap_local_identity(credential_source="none")
            secret = await db.ensure_instance_secret(JWT_SECRET_NAME, "stable-signing-secret")
            return identity, secret, await db.list_accounts()
        finally:
            await db.close()

    async def _read_back(path: Path):
        db = Database(path)
        await db.connect()  # already at the schema head: no migration runs
        try:
            return (
                await db.get_local_identity(),
                await db.get_instance_secret(JWT_SECRET_NAME),
                await db.list_accounts(),
                await db.get_system_principal(),
            )
        finally:
            await db.close()

    identity, secret, accounts = asyncio.run(_bootstrap())
    _make_memu_db(nd / "memu.sqlite")

    result = backup_mod.create_backup(nd, workspace, tmp_path / "out", config_dir=config_dir)
    nd2 = tmp_path / "restored_nerve"
    rep = backup_mod.restore_bundle(
        result.path, nd2, tmp_path / "restored_ws", config_dir=tmp_path / "restored_cfg",
    )
    assert rep.ok, rep.errors

    found, found_secret, found_accounts, system = asyncio.run(_read_back(nd2 / "nerve.db"))
    assert (found.tenant_id, found.agent_id, found.system_actor_id) == (
        identity.tenant_id, identity.agent_id, identity.system_actor_id,
    )
    assert found_accounts == accounts
    assert found_accounts[0]["id"] == identity.owner_account_id
    assert system["id"] == identity.system_actor_id
    assert found_secret == secret == "stable-signing-secret"


def _member_mode(bundle: Path, name: str) -> int | None:
    comp = backup_mod._compression_for(bundle)
    with backup_mod._tar_reader(bundle, comp) as tar:
        for m in tar:
            if m.name == name:
                return stat.S_IMODE(m.mode)
    return None


def _stored_secret(db_file: Path) -> str | None:
    conn = sqlite3.connect(str(db_file))
    try:
        row = conn.execute(
            "SELECT value FROM instance_secrets WHERE name='jwt_secret'"
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def test_backup_archives_the_database_owner_only(nerve_dir, workspace, config_dir, tmp_path):
    """The archived nerve.db member is 0600, so extraction on restore yields an
    owner-only file with no readable window — it carries the signing secret."""
    result = backup_mod.create_backup(nerve_dir, workspace, tmp_path / "out", config_dir=config_dir)
    assert _member_mode(result.path, "state/nerve.db") == 0o600


def _nerve_dir_with_stored_key(tmp_path) -> Path:
    """A real nerve.db holding a jwt_secret row, ready to back up."""
    import asyncio

    from nerve.db import Database
    from nerve.db.accounts import JWT_SECRET_NAME
    from nerve.gateway.auth import unpin_jwt_secret

    nd = tmp_path / "src_nerve"
    nd.mkdir(mode=0o700)  # as Nerve creates it; a 002 umask would otherwise give 0775

    async def _seed():
        db = Database(nd / "nerve.db")
        await db.connect()
        try:
            await db.bootstrap_local_identity(credential_source="none")
            await db.ensure_instance_secret(JWT_SECRET_NAME, "backed-up-secret-32-bytes-padded!!")
        finally:
            await db.close()

    asyncio.run(_seed())
    unpin_jwt_secret()
    _make_memu_db(nd / "memu.sqlite")
    return nd


def test_restore_installs_the_database_owner_only(workspace, config_dir, tmp_path):
    nd = _nerve_dir_with_stored_key(tmp_path)
    result = backup_mod.create_backup(nd, workspace, tmp_path / "out", config_dir=config_dir)

    nd2 = tmp_path / "restored_nerve"
    rep = backup_mod.restore_bundle(
        result.path, nd2, tmp_path / "restored_ws", config_dir=tmp_path / "restored_cfg",
    )
    assert rep.ok, rep.errors
    assert (os.stat(nd2 / "nerve.db").st_mode & 0o777) == 0o600
    assert (os.stat(nd2).st_mode & 0o700) == 0o700
    assert (nd2 / "nerve.db").with_name("nerve.db.restore-tmp").exists() is False
    # A securable restore keeps the key (live sessions keep verifying).
    assert _stored_secret(nd2 / "nerve.db") == "backed-up-secret-32-bytes-padded!!"


class TestRestoreNeverLeavesAReadableKey:
    """F18: the order of operations, each step verified before the next. The
    destination directory is secured (0700, stat-verified) before anything
    lands in it; the temporary is *created* 0600 and its mode read back through
    the descriptor before a byte is copied; the rename is atomic; scrubbing the
    key from an installed file is a verified last resort whose failure
    propagates. Nothing here continues past an unverified step."""

    @staticmethod
    def _bundle(tmp_path, workspace, config_dir) -> Path:
        nd = _nerve_dir_with_stored_key(tmp_path)
        return backup_mod.create_backup(nd, workspace, tmp_path / "out", config_dir=config_dir).path

    @staticmethod
    def _restore(bundle: Path, nd2: Path, tmp_path: Path):
        return backup_mod.restore_bundle(
            bundle, nd2, tmp_path / "restored_ws", config_dir=tmp_path / "restored_cfg",
        )

    def test_an_unsecurable_destination_directory_aborts_before_anything_is_written(
        self, workspace, config_dir, tmp_path, monkeypatch,
    ):
        bundle = self._bundle(tmp_path, workspace, config_dir)
        nd2 = tmp_path / "restored_nerve"
        nd2.mkdir(mode=0o755)  # exists, empty, reachable by others
        real_chmod = os.chmod
        monkeypatch.setattr(  # the directory's mode cannot be changed
            backup_mod.os, "chmod",
            lambda p, m, *a, **k: None if Path(p) == nd2 else real_chmod(p, m, *a, **k),
        )
        with pytest.raises(BackupError, match="could not be made 0700"):
            self._restore(bundle, nd2, tmp_path)
        assert list(nd2.iterdir()) == []  # nothing landed in it

    def test_a_temporary_that_cannot_be_created_owner_only_aborts_before_the_copy(
        self, workspace, config_dir, tmp_path, monkeypatch,
    ):
        """The no-op chmod case, at the point it now matters: the temp is
        created with mode 0600 and the descriptor reads back wide (a
        filesystem without modes). Nothing has been copied yet — and nothing
        is."""
        bundle = self._bundle(tmp_path, workspace, config_dir)
        nd2 = tmp_path / "restored_nerve"
        copies: list[int] = []
        real_copy = backup_mod.shutil.copyfileobj
        monkeypatch.setattr(
            backup_mod.shutil, "copyfileobj",
            lambda *a, **k: (copies.append(1), real_copy(*a, **k))[1],
        )
        # Directories verify; every regular file reads back wide.
        monkeypatch.setattr(backup_mod, "_mode_is_private", lambda st_mode: stat.S_ISDIR(st_mode))

        with pytest.raises(BackupError, match="nothing was copied"):
            self._restore(bundle, nd2, tmp_path)
        assert copies == []
        assert not (nd2 / "nerve.db").exists()
        assert not (nd2 / "nerve.db.restore-tmp").exists()  # partial temporary removed

    @staticmethod
    def _installed_file_reads_back_wide(monkeypatch) -> None:
        """The temp verifies twice (so the copy proceeds and is published) and
        the installed file then reads back wide — the one case the scrub is
        still for."""
        seen = {"files": 0}

        def fake(st_mode: int) -> bool:
            if stat.S_ISDIR(st_mode):
                return True
            seen["files"] += 1
            # 1: the temp's fstat at create; 2: the fstat before publishing it;
            # 3: the final stat of the installed file.
            return seen["files"] <= 2

        monkeypatch.setattr(backup_mod, "_mode_is_private", fake)

    def test_the_last_resort_scrub_removes_the_key_and_the_restore_still_fails(
        self, workspace, config_dir, tmp_path, monkeypatch,
    ):
        """No success-with-exposure: the key is removed so nothing usable is
        readable, and the restore is reported as failed all the same."""
        bundle = self._bundle(tmp_path, workspace, config_dir)
        nd2 = tmp_path / "restored_nerve"
        self._installed_file_reads_back_wide(monkeypatch)
        with pytest.raises(BackupError, match="scrubbed") as ei:
            self._restore(bundle, nd2, tmp_path)
        assert "not complete" in str(ei.value)
        assert _stored_secret(nd2 / "nerve.db") is None  # scrubbed, verified

    def test_a_failing_scrub_propagates_instead_of_claiming_success(
        self, workspace, config_dir, tmp_path, monkeypatch,
    ):
        bundle = self._bundle(tmp_path, workspace, config_dir)
        nd2 = tmp_path / "restored_nerve"
        self._installed_file_reads_back_wide(monkeypatch)

        def failing_scrub(db_file: Path) -> None:
            raise BackupError(f"could not scrub the signing secret from {db_file}: database is locked")

        monkeypatch.setattr(backup_mod, "_scrub_db_secret", failing_scrub)
        with pytest.raises(BackupError, match="could not scrub"):
            self._restore(bundle, nd2, tmp_path)


class TestScrubIsVerified:
    """``_scrub_db_secret`` used to swallow every OperationalError as "older
    bundle, no table". Now the table's absence is checked explicitly, and a
    delete that did not happen — or cannot be verified — raises."""

    def test_a_database_without_the_table_has_nothing_to_scrub(self, tmp_path):
        old = tmp_path / "old.db"
        _make_nerve_db(old)  # pre-v047 shape: no instance_secrets table
        backup_mod._scrub_db_secret(old)  # no error, nothing to do

    def test_a_scrub_that_cannot_write_raises_and_leaves_the_row(self, tmp_path):
        if os.geteuid() == 0:
            pytest.skip("root bypasses file modes")
        nd = _nerve_dir_with_stored_key(tmp_path)
        db_file = nd / "nerve.db"
        os.chmod(db_file, 0o444)
        try:
            with pytest.raises(BackupError, match="could not (scrub|open)"):
                backup_mod._scrub_db_secret(db_file)
        finally:
            os.chmod(db_file, 0o600)
        assert _stored_secret(db_file) == "backed-up-secret-32-bytes-padded!!"


class TestStagingIsOutOfReach:
    """F27: the staged snapshot of ``nerve.db`` is every account and the
    signing secret, in the clear — ``--no-secrets`` scrubs it only *after* the
    snapshot exists. It used to be staged under the caller's output directory,
    where another user could rename the ``0700`` staging directory away and
    leave a readable one in its place. Staging now happens somewhere the
    caller does not choose, and the directory's identity is re-checked before
    anything secret is written into it and again before any of it is read back
    to be archived."""

    def test_staging_does_not_happen_in_the_output_directory(
        self, nerve_dir, workspace, config_dir, tmp_path,
    ):
        out = tmp_path / "out"
        out.mkdir()
        seen: list[list[str]] = []
        real_snapshot = backup_mod._snapshot_db

        def watch(src, dst):
            seen.append([p.name for p in out.iterdir()])
            return real_snapshot(src, dst)

        backup_mod._snapshot_db = watch
        try:
            result = backup_mod.create_backup(nerve_dir, workspace, out, config_dir=config_dir)
        finally:
            backup_mod._snapshot_db = real_snapshot
        # While the database was being copied, the output directory held at
        # most the bundle temporary — never the staged secrets.
        assert seen and all(
            all(name.endswith(".tmp") for name in names) for names in seen
        ), seen
        assert result.path.exists()

    def test_the_staging_parent_is_never_the_output_directory(self, tmp_path):
        """Directly: the chosen parent is the state dir when it is owner-only,
        and the system temp dir otherwise — never the caller's target, and
        never left to ``mkdtemp`` to decide from TMPDIR afterwards."""
        import tempfile

        system_temp = Path(tempfile.gettempdir()).resolve()
        nd = tmp_path / "state"
        nd.mkdir(mode=0o700)
        assert backup_mod._stage_parent(nd) == nd

        os.chmod(nd, 0o777)  # a state dir anyone can write in is not usable
        try:
            assert backup_mod._stage_parent(nd) == system_temp
        finally:
            os.chmod(nd, 0o700)
        assert backup_mod._stage_parent(tmp_path / "missing") == system_temp

    def test_a_temp_directory_owned_by_someone_else_is_refused(
        self, tmp_path, monkeypatch,
    ):
        """F30: sticky is necessary but not sufficient. The *owner* of a
        directory may rename anything inside it whatever the mode says, so a
        foreign-owned ``1777`` TMPDIR is exactly the trap — and the state dir
        here is unusable, so there is nowhere else to go."""
        import tempfile

        foreign = tmp_path / "foreign-tmp"
        foreign.mkdir()
        os.chmod(foreign, 0o1777)  # chmod, not mkdir: the umask would mask it
        real_stat = os.stat

        def someone_elses(path, *a, **k):
            st = real_stat(path, *a, **k)
            if Path(path) == foreign:
                return os.stat_result(
                    tuple(st)[:4] + (st.st_uid + 12345, st.st_gid) + tuple(st)[6:]
                )
            return st

        monkeypatch.setattr(backup_mod.os, "stat", someone_elses)
        monkeypatch.setattr(tempfile, "gettempdir", lambda: str(foreign))
        nd = tmp_path / "state"
        nd.mkdir()
        os.chmod(nd, 0o777)  # unusable, so the temp dir is the only candidate

        with pytest.raises(BackupError, match="owned by uid"):
            backup_mod._stage_parent(nd)

    def test_an_unsafe_ancestor_is_refused(self, tmp_path, monkeypatch):
        """The check walks the canonical ancestry: a directory anyone can write
        to *above* the parent can be swapped for one pointing elsewhere."""
        loose = tmp_path / "loose"
        (loose / "tmp").mkdir(parents=True)
        os.chmod(loose, 0o777)  # world-writable and not sticky
        try:
            reason = backup_mod._unsafe_stage_reason(loose / "tmp")
        finally:
            os.chmod(loose, 0o755)
        assert reason and "writable by other users and not sticky" in reason

    @pytest.mark.parametrize("include_secrets", [True, False], ids=["secrets", "no-secrets"])
    def test_a_swapped_staging_directory_is_refused(
        self, nerve_dir, workspace, config_dir, tmp_path, monkeypatch, include_secrets,
    ):
        """The reproduction: replace the staging directory after it is created.
        Both bundle kinds must refuse — ``--no-secrets`` included, since the
        snapshot carries the key until the scrub runs on it."""
        attacker = tmp_path / "attacker-stage"
        swapped: list[Path] = []
        real_writable = backup_mod._is_group_world_writable

        def swap_then_check(path):
            """Runs between the staging directory being opened and its first
            use — the attacker's window."""
            if not swapped:
                for stage in nerve_dir.glob(".nerve-backup-stage-*"):
                    stage.rename(attacker)
                    stage.mkdir(mode=0o777)  # a readable one in its place
                    swapped.append(stage)
            return real_writable(path)

        monkeypatch.setattr(backup_mod, "_is_group_world_writable", swap_then_check)
        out = tmp_path / "out"
        with pytest.raises(BackupError, match="replaced while it was being written"):
            backup_mod.create_backup(
                nerve_dir, workspace, out,
                config_dir=config_dir, include_secrets=include_secrets,
            )

        assert swapped, "the test did not manage to swap the staging directory"
        assert not (swapped[0] / "state").exists()  # no database was copied into it
        assert not out.exists() or list(out.iterdir()) == []  # nothing published

    @pytest.mark.parametrize("include_secrets", [True, False], ids=["secrets", "no-secrets"])
    def test_a_swap_after_the_first_verification_is_caught_too(
        self, nerve_dir, workspace, config_dir, tmp_path, monkeypatch, include_secrets,
    ):
        """F30: the swap moved later — *after* the staging directory passed its
        first check, while the snapshot is being taken. The snapshot is created
        and written through a descriptor of its own, so the planted directory
        receives nothing, and the checks before the archive refuse to go on."""
        attacker = tmp_path / "attacker-stage"
        swapped: list[Path] = []
        real_snapshot = backup_mod._snapshot_db

        def swap_during_the_snapshot(src, dst):
            result = real_snapshot(src, dst)
            if not swapped:
                for stage in nerve_dir.glob(".nerve-backup-stage-*"):
                    stage.rename(attacker)
                    stage.mkdir(mode=0o777)
                    swapped.append(stage)
            return result

        monkeypatch.setattr(backup_mod, "_snapshot_db", swap_during_the_snapshot)
        out = tmp_path / "out"
        with pytest.raises(BackupError, match="replaced while it was being written"):
            backup_mod.create_backup(
                nerve_dir, workspace, out,
                config_dir=config_dir, include_secrets=include_secrets,
            )

        assert swapped, "the test did not manage to swap the staging directory"
        assert list(swapped[0].iterdir()) == []  # nothing at all landed in theirs
        assert not out.exists() or list(out.iterdir()) == []
        # The snapshot went to the directory that was verified — the one they
        # renamed away — and not through the name they planted. That one is
        # left alone rather than cleaned up by name; ours stays owner-only.
        assert (attacker / "state" / "nerve.db").exists()
        assert stat.S_IMODE(os.stat(attacker).st_mode) == 0o700
        assert stat.S_IMODE(os.stat(attacker / "state" / "nerve.db").st_mode) == 0o600


class TestTheBundleItselfIsOwnerOnly:
    """The bundle carries nerve.db (accounts, history, and unless
    ``--no-secrets`` scrubbed it, the signing secret) and config.local.yaml
    with the password hash. Hardening the files it is made of and then writing
    them into a world-readable tarball would hand the same key to the same
    people, so the bundle is created 0600 before a byte is written."""

    def test_the_bundle_is_created_owner_only(self, nerve_dir, workspace, config_dir, tmp_path):
        result = backup_mod.create_backup(
            nerve_dir, workspace, tmp_path / "out", config_dir=config_dir,
        )
        assert (os.stat(result.path).st_mode & 0o777) == 0o600
        assert not (tmp_path / "out" / (result.path.name + ".tmp")).exists()

    @staticmethod
    def _output_filesystem_ignores_modes(monkeypatch, out: Path) -> None:
        """Only the *output* directory's filesystem loses the mode.

        That is the realistic split — a normal state directory, a bundle
        written to an exotic mount — and it is the only way to reach the
        bundle's own check, since the staging directory and the snapshot are
        verified before it (F27). The file is still created exclusively, as a
        mode-less filesystem does: it is the mode that fails to stick."""
        real_create = backup_mod._exclusive_create

        def fake(path, what):
            fd, private = real_create(path, what)
            return (fd, False) if path.parent == out else (fd, private)

        monkeypatch.setattr(backup_mod, "_exclusive_create", fake)

    def test_a_bundle_that_cannot_be_created_owner_only_is_refused(
        self, nerve_dir, workspace, config_dir, tmp_path, monkeypatch,
    ):
        """Caught while the file is still empty: no bundle, rather than one
        that leaks the key to every local user."""
        out = tmp_path / "out"
        out.mkdir()
        self._output_filesystem_ignores_modes(monkeypatch, out)
        with pytest.raises(BackupError, match="nothing was written"):
            backup_mod.create_backup(nerve_dir, workspace, out, config_dir=config_dir)
        assert list(out.iterdir()) == []

    def test_a_no_secrets_bundle_is_still_written_with_a_warning(
        self, nerve_dir, workspace, config_dir, tmp_path, monkeypatch, caplog,
    ):
        """``--no-secrets`` carries no credential — the signing secret is
        scrubbed and config.local.yaml is not collected — so the same failure
        is loud but not fatal: refusing to back up at all would be worse."""
        import logging

        out = tmp_path / "out"
        out.mkdir()
        self._output_filesystem_ignores_modes(monkeypatch, out)
        with caplog.at_level(logging.WARNING, logger="nerve.backup"):
            result = backup_mod.create_backup(
                nerve_dir, workspace, out, config_dir=config_dir, include_secrets=False,
            )
        assert result.path.exists()
        assert any("readable by other users" in r.getMessage() for r in caplog.records)

    def test_the_no_secrets_fallback_still_writes_through_its_own_descriptor(
        self, nerve_dir, workspace, config_dir, tmp_path, monkeypatch,
    ):
        """F31: tolerating a wide *mode* must not mean tolerating a wide
        *name*. The fallback used to reopen the path, so a symlink planted in
        the gap had its target truncated and overwritten with the bundle — any
        file the Nerve user could write, including live state. Now the bytes go
        to the descriptor that was created exclusively, and the name is proved
        before publication."""
        out = tmp_path / "out"
        out.mkdir()
        target = tmp_path / "precious.db"
        target.write_text("do not truncate me", encoding="utf-8")
        os.chmod(target, 0o644)
        real_create = backup_mod._exclusive_create
        swapped: list[Path] = []

        def wide_then_swapped(path, what):
            fd, private = real_create(path, what)
            if path.parent == out:
                path.unlink()
                path.symlink_to(target)  # the name now points at their file
                swapped.append(path)
                return fd, False  # ...and the mode did not stick either
            return fd, private

        monkeypatch.setattr(backup_mod, "_exclusive_create", wide_then_swapped)
        with pytest.raises(BackupError, match="replaced while it was being written"):
            backup_mod.create_backup(
                nerve_dir, workspace, out, config_dir=config_dir, include_secrets=False,
            )

        assert swapped, "the test did not manage to swap the bundle temporary"
        assert target.read_text(encoding="utf-8") == "do not truncate me"
        assert not any(p.is_file() and not p.is_symlink() for p in out.iterdir())

    def test_a_state_filesystem_that_ignores_modes_refuses_before_the_snapshot(
        self, nerve_dir, workspace, config_dir, tmp_path, monkeypatch,
    ):
        """And when it is the *staging* filesystem that loses the mode, the
        refusal comes before the database is copied anywhere at all."""
        monkeypatch.setattr(backup_mod, "_mode_is_private", lambda st_mode: False)
        with pytest.raises(BackupError, match="owner-only"):
            backup_mod.create_backup(
                nerve_dir, workspace, tmp_path / "out", config_dir=config_dir,
            )
        assert list((tmp_path / "out").iterdir()) == []
        assert not list(nerve_dir.glob(".nerve-backup-stage-*"))  # cleaned up

    def test_the_bundle_is_written_through_the_descriptor_it_verified(
        self, nerve_dir, workspace, config_dir, tmp_path, monkeypatch,
    ):
        """F26: the tar goes into the *descriptor* that was checked, never into
        the pathname reopened. Another user with write access to the output
        directory (a shared or mounted backup target) who replaces the
        temporary with a symlink must not receive the bundle."""
        out = tmp_path / "out"
        out.mkdir()
        target = tmp_path / "attacker.tar"
        target.write_text("", encoding="utf-8")
        os.chmod(target, 0o644)
        real_create = backup_mod._exclusive_create
        swapped: list[Path] = []

        def swap_right_after_creating_it(path, what):
            """The attacker's window: the instant after the temporary is
            created and verified. Code that then reopened the *name* would
            write the bundle straight into their file. Only the bundle
            temporary is swapped — the staged snapshot is a different file in
            a directory this attacker cannot reach (F27)."""
            fd, private = real_create(path, what)
            if path.parent == out:
                path.unlink()
                path.symlink_to(target)
                swapped.append(path)
            return fd, private

        monkeypatch.setattr(backup_mod, "_exclusive_create", swap_right_after_creating_it)
        with pytest.raises(BackupError, match="replaced while it was being written"):
            backup_mod.create_backup(nerve_dir, workspace, out, config_dir=config_dir)

        assert swapped, "the test did not manage to swap the temporary"
        assert target.read_bytes() == b""  # nothing was written through the symlink
        assert list(out.iterdir()) == []  # and no bundle was published

    def test_a_swapped_restore_temporary_is_not_published_either(
        self, workspace, config_dir, tmp_path, monkeypatch,
    ):
        """The same discipline on the restore side: the installed file must be
        the one whose mode was verified, not whatever the name points at by the
        time of the rename."""
        nd = _nerve_dir_with_stored_key(tmp_path)
        bundle = backup_mod.create_backup(
            nd, workspace, tmp_path / "out", config_dir=config_dir,
        ).path
        nd2 = tmp_path / "restored_nerve"
        target = tmp_path / "attacker.db"
        target.write_text("attacker", encoding="utf-8")
        real_copy = backup_mod.shutil.copyfileobj

        def swap_after_copy(inp, out, *a, **k):
            result = real_copy(inp, out, *a, **k)
            tmp = nd2 / "nerve.db.restore-tmp"
            if tmp.is_file():
                tmp.unlink()
                tmp.symlink_to(target)
            return result

        monkeypatch.setattr(backup_mod.shutil, "copyfileobj", swap_after_copy)
        with pytest.raises(BackupError, match="replaced while it was being written"):
            backup_mod.restore_bundle(
                bundle, nd2, tmp_path / "restored_ws", config_dir=tmp_path / "restored_cfg",
            )
        assert not (nd2 / "nerve.db").exists()
        assert target.read_text(encoding="utf-8") == "attacker"


class TestRestoredConfigLocalIsOwnerOnly:
    """config.local.yaml holds ``auth.password_hash`` and the machine-local
    secrets, and it lands in an ordinary config directory rather than the state
    directory restore verifies — so it goes through the same verified 0600
    temporary as nerve.db, and a failure aborts the restore instead of quietly
    finishing without it (an instance with no configured password is
    passwordless, which is not what a restore was asked to do)."""

    def test_it_is_installed_owner_only(self, nerve_dir, workspace, config_dir, tmp_path):
        result = backup_mod.create_backup(
            nerve_dir, workspace, tmp_path / "out", config_dir=config_dir,
        )
        cfg2 = tmp_path / "restored_cfg"
        cfg2.mkdir(mode=0o755)  # an ordinary config dir, reachable by others
        rep = backup_mod.restore_bundle(
            result.path, tmp_path / "restored_nerve", tmp_path / "restored_ws",
            config_dir=cfg2,
        )
        assert rep.ok, rep.errors
        assert (os.stat(cfg2 / "config.local.yaml").st_mode & 0o777) == 0o600
        assert not (cfg2 / "config.local.yaml.restore-tmp").exists()

    def test_a_failure_aborts_the_restore_instead_of_dropping_the_file(
        self, nerve_dir, workspace, config_dir, tmp_path, monkeypatch,
    ):
        result = backup_mod.create_backup(
            nerve_dir, workspace, tmp_path / "out", config_dir=config_dir,
        )
        real_create = backup_mod._secure_create

        def refuse_config_temp(path, what, **kwargs):
            if path.name.startswith("config.local.yaml"):
                raise BackupError(f"{what}: could not create {path} owner-only")
            return real_create(path, what, **kwargs)

        monkeypatch.setattr(backup_mod, "_secure_create", refuse_config_temp)
        cfg2 = tmp_path / "restored_cfg"
        with pytest.raises(BackupError, match="config.local.yaml"):
            backup_mod.restore_bundle(
                result.path, tmp_path / "restored_nerve", tmp_path / "restored_ws",
                config_dir=cfg2,
            )
        assert not (cfg2 / "config.local.yaml").exists()


def test_state_only_skips_workspace(nerve_dir, workspace, config_dir, tmp_path):
    out = tmp_path / "out"
    result = backup_mod.create_backup(
        nerve_dir, workspace, out, config_dir=config_dir, state_only=True,
    )
    members = _bundle_members(result.path)
    assert not any("workspace/" in m for m in members)
    assert result.include_workspace is False
    assert any(m.endswith("state/nerve.db") for m in members)


# --------------------------------------------------------------------------- #
#  4. Retention + restore safety rails                                         #
# --------------------------------------------------------------------------- #


def test_prune_only_matching_files(nerve_dir, workspace, tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    # Create several real bundles.
    for _ in range(5):
        backup_mod.create_backup(nerve_dir, workspace, out, state_only=True)
    assert len(backup_mod.list_bundles(out)) == 5

    # Decoys that must survive.
    (out / "unrelated.txt").write_text("keep me")
    (out / "nerve-backup-notes.md").write_text("not a bundle")
    (out / "README").write_text("keep")

    deleted = backup_mod.prune(out, keep_n=2)
    assert len(deleted) == 3
    assert len(backup_mod.list_bundles(out)) == 2
    assert (out / "unrelated.txt").exists()
    assert (out / "nerve-backup-notes.md").exists()
    assert (out / "README").exists()
    # Everything deleted was a real bundle.
    assert all(backup_mod.BUNDLE_RE.match(p.name) for p in deleted)


def test_restore_refuses_on_live_pidfile(nerve_dir, workspace, config_dir, tmp_path):
    out = tmp_path / "out"
    result = backup_mod.create_backup(nerve_dir, workspace, out, config_dir=config_dir)

    target = tmp_path / "live_nerve"
    target.mkdir()
    # A pidfile pointing at THIS process (definitely alive).
    (target / "nerve.pid").write_text(str(os.getpid()))

    with pytest.raises(BackupError, match="running"):
        backup_mod.restore_bundle(result.path, target, tmp_path / "ws_x")


def test_restore_refuses_nonempty_without_force(nerve_dir, workspace, config_dir, tmp_path):
    out = tmp_path / "out"
    result = backup_mod.create_backup(nerve_dir, workspace, out, config_dir=config_dir)

    target = tmp_path / "occupied"
    target.mkdir()
    (target / "existing.txt").write_text("data")

    with pytest.raises(BackupError, match="not empty"):
        backup_mod.restore_bundle(result.path, target, tmp_path / "ws_y")


def test_restore_force_relocates_old_dir(nerve_dir, workspace, config_dir, tmp_path):
    out = tmp_path / "out"
    result = backup_mod.create_backup(nerve_dir, workspace, out, config_dir=config_dir)

    target = tmp_path / "occupied"
    target.mkdir()
    (target / "old_marker.txt").write_text("previous state")

    backup_mod.restore_bundle(
        result.path, target, tmp_path / "ws_z", force=True,
    )
    # Old dir relocated, not deleted.
    relocated = list(tmp_path.glob("occupied.pre-restore-*"))
    assert len(relocated) == 1
    assert (relocated[0] / "old_marker.txt").read_text() == "previous state"
    # New state installed.
    assert (target / "nerve.db").exists()
    assert "old_marker.txt" not in [p.name for p in target.iterdir()]


def test_restore_refuses_workspace_paths_the_backup_would_never_take(
    nerve_dir, workspace, config_dir, tmp_path,
):
    """The workspace allowlist is applied when a bundle is *collected*; restore
    has to apply it again when a bundle is *read*.

    "config/ is never in a bundle" is true of bundles this module writes and says
    nothing about bundles it is handed. One carrying config/settings.yaml or a gate
    plugin would overwrite the git-tracked config subtree from a tarball — which on
    a locked instance replaces the reviewed remote config outright, and on any
    instance leaves a checkout dirty enough that the next sync refuses to merge.
    """
    out = tmp_path / "out"
    result = backup_mod.create_backup(nerve_dir, workspace, out, config_dir=config_dir)
    compression = backup_mod._compression_for(result.path)

    # Re-pack the bundle with two extra workspace entries the collector would
    # never have produced, using the module's own reader/writer so it stays a
    # bundle this code accepts.
    staging = tmp_path / "repack"
    staging.mkdir()
    with backup_mod._tar_reader(result.path, compression) as tf:
        tf.extractall(staging)
    smuggled = staging / "workspace" / "config" / "cron" / "gates"
    smuggled.mkdir(parents=True)
    (smuggled / "evil.py").write_text("MARKER = 1\n")
    (staging / "workspace" / "config" / "settings.yaml").write_text("lockdown: false\n")
    result.path.unlink()
    with backup_mod._tar_writer(result.path, compression) as tf:
        for entry in sorted(staging.iterdir()):
            tf.add(entry, arcname=entry.name)

    target = tmp_path / "restored"
    ws_out = tmp_path / "ws_restored"
    ws_out.mkdir()
    (ws_out / "config").mkdir()
    (ws_out / "config" / "settings.yaml").write_text("lockdown: true\n")

    report = backup_mod.restore_bundle(result.path, target, ws_out)

    # The tracked subtree is untouched; the allowlisted brain still came back.
    assert (ws_out / "config" / "settings.yaml").read_text() == "lockdown: true\n"
    assert not (ws_out / "config" / "cron").exists()
    assert (ws_out / "SOUL.md").read_text() == "soul"
    assert (ws_out / "memory" / "people.md").exists()
    assert any("skipped" in w for w in report.warnings), report.warnings


# --------------------------------------------------------------------------- #
#  5. Schema-version guard                                                     #
# --------------------------------------------------------------------------- #


def test_schema_guard_rejects_newer_bundle(nerve_dir, workspace, config_dir, tmp_path):
    # Bump the snapshot's schema version above the code's migration head.
    _make_nerve_db(nerve_dir / "nerve.db", schema_version=SCHEMA_VERSION + 100)
    out = tmp_path / "out"
    result = backup_mod.create_backup(nerve_dir, workspace, out, config_dir=config_dir)

    report = backup_mod.verify_bundle(result.path)
    assert not report.ok
    assert any("newer than this code" in e for e in report.errors)

    # Restore must refuse cleanly.
    with pytest.raises(BackupError, match="verification"):
        backup_mod.restore_bundle(result.path, tmp_path / "nd_new", tmp_path / "ws_new")


def test_verify_detects_checksum_tamper(nerve_dir, workspace, config_dir, tmp_path):
    out = tmp_path / "out"
    result = backup_mod.create_backup(
        nerve_dir, workspace, out, config_dir=config_dir, compression="gzip",
    )
    # Tamper: rewrite a file inside the gzip tar so a checksum mismatches.
    import tarfile

    extract = tmp_path / "x"
    extract.mkdir()
    with tarfile.open(result.path, "r:gz") as tar:
        tar.extractall(extract, filter="data")
    (extract / "workspace" / "SOUL.md").write_text("TAMPERED CONTENT")
    tampered = tmp_path / "tampered.tar.gz"
    with tarfile.open(tampered, "w:gz") as tar:
        tar.add(extract / "manifest.json", arcname="manifest.json")
        for sub in ("config", "state", "workspace"):
            if (extract / sub).exists():
                tar.add(extract / sub, arcname=sub)

    report = backup_mod.verify_bundle(tampered)
    assert not report.ok
    assert any("checksum mismatch" in e for e in report.errors)


def test_gzip_fallback_roundtrip(nerve_dir, workspace, config_dir, tmp_path):
    out = tmp_path / "out"
    result = backup_mod.create_backup(
        nerve_dir, workspace, out, config_dir=config_dir, compression="gzip",
    )
    assert result.path.name.endswith(".tar.gz")
    assert result.compression == "gzip"
    report = backup_mod.verify_bundle(result.path)
    assert report.ok, report.errors
