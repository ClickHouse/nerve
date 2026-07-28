"""Tests for nerve.backup — consistent snapshots, bundle round-trip, restore.

The databases run in WAL mode, so the snapshot must stay consistent under a
concurrent writer; restore must be verified and refuse to clobber a live or
non-empty target. These tests exercise the whole bundle lifecycle without a
running server (the CLI and lifespan task are thin wrappers over this module).
"""

from __future__ import annotations

import os
import sqlite3
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
    nd.mkdir()
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
