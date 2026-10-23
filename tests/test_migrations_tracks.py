"""Two migration tracks: upstream (``schema_version``), then fork
(``fork_schema_version``), each numbered and guarded on its own.

The synthetic tracks below are packages written to ``tmp_path``, run through
the same runner code as the real tracks. Migration ``vN`` of track ``t``
appends ``"tN"`` to the ``applied`` table, so a test can read what ran and in
which order.
"""

from __future__ import annotations

import contextlib
import importlib
import os
import sqlite3
import sys
import uuid
from pathlib import Path

import aiosqlite
import pytest

from nerve.db import FORK_SCHEMA_VERSION, SCHEMA_VERSION, Database
from nerve.db.base import InsecureStateStorage, check_database_file
from nerve.db.migrations import runner


def _write_migration(directory: Path, track: str, version: int) -> None:
    (directory / f"v{version:03d}_step.py").write_text(
        "async def up(db):\n"
        f"    await db.execute(\"INSERT INTO applied (name) VALUES ('{track}{version}')\")\n",
        encoding="utf-8",
    )
    importlib.invalidate_caches()


def _track(tmp_path, monkeypatch, name: str, table: str, versions) -> runner.MigrationTrack:
    """A track named ``name`` recorded in ``table`` with migrations ``versions``."""
    package = f"synthetic_{name}_{uuid.uuid4().hex[:12]}"
    directory = tmp_path / package
    directory.mkdir()
    (directory / "__init__.py").write_text("", encoding="utf-8")
    for version in versions:
        _write_migration(directory, name, version)
    monkeypatch.syspath_prepend(str(tmp_path))
    return runner.MigrationTrack(name, package, directory, table)


@contextlib.asynccontextmanager
async def _database(tmp_path, name: str = "nerve.db"):
    async with aiosqlite.connect(tmp_path / name) as db:
        await db.execute(
            "CREATE TABLE IF NOT EXISTS applied (id INTEGER PRIMARY KEY, name TEXT)"
        )
        await db.commit()
        yield db


async def _record(db, table: str, version: int) -> None:
    """Record ``version`` as applied, as an earlier run would have."""
    await db.execute(f"CREATE TABLE IF NOT EXISTS {table} (version INTEGER PRIMARY KEY)")
    await db.execute(f"INSERT INTO {table} (version) VALUES (?)", (version,))
    await db.commit()


async def _applied(db) -> list[str]:
    async with db.execute("SELECT name FROM applied ORDER BY id") as cursor:
        return [row[0] for row in await cursor.fetchall()]


async def _has_table(db, table: str) -> bool:
    async with db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,),
    ) as cursor:
        return await cursor.fetchone() is not None


@pytest.fixture(autouse=True)
def _no_override(monkeypatch):
    monkeypatch.delenv(runner.ALLOW_NEWER_SCHEMA_ENV, raising=False)


@pytest.mark.asyncio
async def test_the_upstream_track_runs_before_the_fork_track(tmp_path, monkeypatch):
    upstream = _track(tmp_path, monkeypatch, "u", "schema_version", [2, 1])
    fork = _track(tmp_path, monkeypatch, "f", "fork_schema_version", [1, 2])

    async with _database(tmp_path) as db:
        assert await runner.run_migrations(db, (upstream, fork)) == 2
        assert await _applied(db) == ["u1", "u2", "f1", "f2"]
        assert await runner.get_current_version(db, fork) == 2


@pytest.mark.asyncio
async def test_each_track_skips_versions_at_or_below_its_own(tmp_path, monkeypatch):
    upstream = _track(tmp_path, monkeypatch, "u", "schema_version", [1, 2, 3])
    fork = _track(tmp_path, monkeypatch, "f", "fork_schema_version", [1, 2])

    async with _database(tmp_path) as db:
        await _record(db, "schema_version", 2)
        await _record(db, "fork_schema_version", 1)
        assert await runner.run_migrations(db, (upstream, fork)) == 3
        assert await _applied(db) == ["u3", "f2"]

        # Nothing is pending any more.
        await runner.run_migrations(db, (upstream, fork))
        assert await _applied(db) == ["u3", "f2"]


@pytest.mark.asyncio
async def test_fork_numbers_never_shadow_upstream_numbers(tmp_path, monkeypatch):
    """With one shared sequence, upstream at v5 would hide fork v1, and the fork's
    v9 would hide every upstream migration up to v9. Each track has its own."""
    upstream = _track(tmp_path, monkeypatch, "u", "schema_version", [5, 6])
    fork = _track(tmp_path, monkeypatch, "f", "fork_schema_version", [1, 9])

    async with _database(tmp_path) as db:
        await _record(db, "schema_version", 5)
        await runner.run_migrations(db, (upstream, fork))
        assert await _applied(db) == ["u6", "f1", "f9"]

        # An upstream release adds v7. It runs although the fork is at v9.
        _write_migration(upstream.directory, "u", 7)
        await runner.run_migrations(db, (upstream, fork))
        assert await _applied(db) == ["u6", "f1", "f9", "u7"]
        assert await runner.get_current_version(db, upstream) == 7
        assert await runner.get_current_version(db, fork) == 9


@pytest.mark.asyncio
async def test_the_fork_table_is_created_by_the_first_fork_migration(tmp_path, monkeypatch):
    upstream = _track(tmp_path, monkeypatch, "u", "schema_version", [1])
    no_fork = _track(tmp_path, monkeypatch, "f", "fork_schema_version", [])

    async with _database(tmp_path) as db:
        await runner.run_migrations(db, (upstream, no_fork))
        assert not await _has_table(db, "fork_schema_version")
        assert await runner.get_current_version(db, no_fork) == 0

        _write_migration(no_fork.directory, "f", 1)
        await runner.run_migrations(db, (upstream, no_fork))
        assert await _has_table(db, "fork_schema_version")
        async with db.execute("SELECT version FROM fork_schema_version") as cursor:
            assert await cursor.fetchall() == [(1,)]
        assert await _applied(db) == ["u1", "f1"]


@pytest.mark.asyncio
async def test_the_log_names_the_track_of_each_migration(tmp_path, monkeypatch, caplog):
    upstream = _track(tmp_path, monkeypatch, "u", "schema_version", [1])
    fork = _track(tmp_path, monkeypatch, "f", "fork_schema_version", [])
    (fork.directory / "v001_broken.py").write_text(
        "async def up(db):\n    raise RuntimeError('broken')\n", encoding="utf-8",
    )
    importlib.invalidate_caches()

    async with _database(tmp_path) as db:
        with caplog.at_level("INFO", logger=runner.__name__):
            with pytest.raises(RuntimeError, match="broken"):
                await runner.run_migrations(db, (upstream, fork))

    messages = [r.getMessage() for r in caplog.records]
    assert "Applied u migration V1" in messages
    assert "Failed to apply f migration V1" in messages


@pytest.mark.asyncio
async def test_a_fork_migration_that_went_upstream_stays_as_a_no_op(tmp_path, monkeypatch):
    """Deleted, a fork migration that has run leaves the fork's databases ahead
    of the fork's own build. Kept as a no-op with its number, it lets them
    open, and the upstream migration that now ships the change runs."""
    upstream = _track(tmp_path, monkeypatch, "u", "schema_version", [1])
    fork = _track(tmp_path, monkeypatch, "f", "fork_schema_version", [1])
    migration = fork.directory / "v001_step.py"

    async with _database(tmp_path) as db:
        await runner.run_migrations(db, (upstream, fork))

        # The change went upstream as u2, and the fork deleted its migration.
        _write_migration(upstream.directory, "u", 2)
        migration.unlink()
        importlib.invalidate_caches()
        with pytest.raises(runner.SchemaNewerThanCodeError) as failure:
            await runner.run_migrations(db, (upstream, fork))
        assert (failure.value.track, failure.value.database_version) == ("f", 1)

        migration.write_text("async def up(db):\n    pass\n", encoding="utf-8")
        importlib.invalidate_caches()
        monkeypatch.delitem(sys.modules, f"{fork.package}.v001_step")  # a new process
        await runner.run_migrations(db, (upstream, fork))
        assert await _applied(db) == ["u1", "f1", "u2"]

    # A new database gets the change from upstream only.
    async with _database(tmp_path, "new.db") as new:
        await runner.run_migrations(new, (upstream, fork))
        assert await _applied(new) == ["u1", "u2"]
        assert await runner.get_current_version(new, fork) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("ahead, table, versions", [
    ("u", "schema_version", (7, 2)),
    ("f", "fork_schema_version", (3, 1)),
])
async def test_a_track_ahead_of_the_code_is_refused_before_anything_runs(
    tmp_path, monkeypatch, ahead, table, versions,
):
    """A newer database on either track is refused. Nothing runs, the other
    track's pending migrations included."""
    upstream = _track(tmp_path, monkeypatch, "u", "schema_version", [1, 2])
    fork = _track(tmp_path, monkeypatch, "f", "fork_schema_version", [1])

    async with _database(tmp_path) as db:
        await _record(db, table, versions[0])
        with pytest.raises(runner.SchemaNewerThanCodeError) as failure:
            await runner.run_migrations(db, (upstream, fork))
        assert await _applied(db) == []

    refusal = failure.value
    assert refusal.track == ahead
    assert (refusal.database_version, refusal.code_version) == versions
    message = str(refusal)
    assert f"at {ahead} schema version {versions[0]}" in message
    assert f"the version {versions[1]} this Nerve ships" in message
    assert runner.ALLOW_NEWER_SCHEMA_ENV in message
    assert "\n" not in message


@pytest.mark.asyncio
async def test_the_override_runs_on_a_newer_track_with_a_warning(
    tmp_path, monkeypatch, caplog,
):
    monkeypatch.setenv(runner.ALLOW_NEWER_SCHEMA_ENV, "1")
    upstream = _track(tmp_path, monkeypatch, "u", "schema_version", [1, 2])
    fork = _track(tmp_path, monkeypatch, "f", "fork_schema_version", [1])

    async with _database(tmp_path) as db:
        await _record(db, "fork_schema_version", 3)
        with caplog.at_level("WARNING", logger=runner.__name__):
            assert await runner.run_migrations(db, (upstream, fork)) == 2
        assert await _applied(db) == ["u1", "u2"]
        assert await runner.get_current_version(db, fork) == 3

    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert warnings == [(
        "Database f schema version 3 is newer than this code's 1; continuing "
        f"because {runner.ALLOW_NEWER_SCHEMA_ENV}=1 is set"
    )]


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["0", "yes", "true", ""])
async def test_only_the_value_1_overrides_the_refusal(tmp_path, monkeypatch, value):
    monkeypatch.setenv(runner.ALLOW_NEWER_SCHEMA_ENV, value)
    upstream = _track(tmp_path, monkeypatch, "u", "schema_version", [1])

    async with _database(tmp_path) as db:
        await _record(db, "schema_version", 2)
        with pytest.raises(runner.SchemaNewerThanCodeError):
            await runner.run_migrations(db, (upstream,))


# --------------------------------------------------------------------------- #
#  The real tracks                                                             #
# --------------------------------------------------------------------------- #


def test_the_real_tracks():
    migrations = Path(runner.__file__).parent
    assert runner.TRACKS == (runner.UPSTREAM_TRACK, runner.FORK_TRACK)
    assert (runner.UPSTREAM_TRACK.directory, runner.UPSTREAM_TRACK.table) == (
        migrations, "schema_version",
    )
    assert (runner.FORK_TRACK.directory, runner.FORK_TRACK.table) == (
        migrations / "fork", "fork_schema_version",
    )
    assert importlib.import_module(runner.FORK_TRACK.package).__file__ == str(
        migrations / "fork" / "__init__.py"
    )
    assert SCHEMA_VERSION == runner.code_version(runner.UPSTREAM_TRACK)
    assert FORK_SCHEMA_VERSION == runner.code_version(runner.FORK_TRACK)


@pytest.mark.asyncio
async def test_a_database_opens_at_both_heads(tmp_path):
    database = Database(tmp_path / "nerve.db")
    await database.connect()
    try:
        assert await runner.get_current_version(database.db) == SCHEMA_VERSION
        assert await runner.get_fork_version(database.db) == FORK_SCHEMA_VERSION
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_a_nerve_without_the_fork_migrations_refuses_a_database_a_fork_migrated(
    tmp_path,
):
    """The way back from a fork to a build without its migrations is a
    restore, not a start on a schema the build does not know."""
    database = Database(tmp_path / "nerve.db")
    await database.connect()
    await _record(database.db, "fork_schema_version", FORK_SCHEMA_VERSION + 1)
    await database.close()

    with pytest.raises(runner.SchemaNewerThanCodeError) as failure:
        await Database(tmp_path / "nerve.db").connect()
    assert failure.value.track == "fork"
    assert failure.value.database_version == FORK_SCHEMA_VERSION + 1


# --------------------------------------------------------------------------- #
#  check_database_file: the gateway's check before it starts                  #
# --------------------------------------------------------------------------- #


def _sqlite_at(path: Path, table: str, version: int) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"CREATE TABLE {table} (version INTEGER PRIMARY KEY)")
        conn.execute(f"INSERT INTO {table} (version) VALUES (?)", (version,))
        conn.commit()
    finally:
        conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("table, track, code", [
    ("schema_version", "upstream", SCHEMA_VERSION),
    ("fork_schema_version", "fork", FORK_SCHEMA_VERSION),
])
async def test_check_database_file_refuses_without_changing_the_file(
    tmp_path, table, track, code,
):
    path = tmp_path / "nerve state" / "nerve.db"  # a space: the file is opened by URI
    path.parent.mkdir()
    _sqlite_at(path, table, code + 1)
    before = path.read_bytes()

    with pytest.raises(runner.SchemaNewerThanCodeError) as failure:
        await check_database_file(path)
    assert (failure.value.track, failure.value.database_version) == (track, code + 1)
    assert path.read_bytes() == before


@pytest.mark.asyncio
async def test_check_database_file_opens_the_database_read_only(tmp_path):
    """A read-write connection that closes last checkpoints the write-ahead log
    into the database file and removes the log. The check leaves both as they
    were."""
    path = tmp_path / "nerve.db"
    _sqlite_at(path, "schema_version", SCHEMA_VERSION)
    writer = sqlite3.connect(path)
    try:
        # The newer version stays in the log after this close, as it would
        # after a crash.
        writer.setconfig(sqlite3.SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, True)
        writer.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION + 1,))
        writer.commit()
    finally:
        writer.close()
    wal = Path(f"{path}-wal")
    before = (path.read_bytes(), wal.read_bytes())
    assert before[1], "the newer version is in the log"

    with pytest.raises(runner.SchemaNewerThanCodeError):
        await check_database_file(path)
    assert (path.read_bytes(), wal.exists() and wal.read_bytes()) == before


@pytest.mark.asyncio
async def test_check_database_file_leaves_everything_else_to_the_real_open(
    tmp_path, monkeypatch,
):
    current = tmp_path / "current.db"
    _sqlite_at(current, "schema_version", SCHEMA_VERSION)
    await check_database_file(current)

    await check_database_file(tmp_path / "missing.db")
    assert not (tmp_path / "missing.db").exists()

    junk = tmp_path / "junk.db"
    junk.write_text("not a database", encoding="utf-8")
    await check_database_file(junk)

    newer = tmp_path / "newer.db"
    _sqlite_at(newer, "fork_schema_version", FORK_SCHEMA_VERSION + 1)
    monkeypatch.setenv(runner.ALLOW_NEWER_SCHEMA_ENV, "1")
    await check_database_file(newer)


@pytest.mark.asyncio
async def test_check_database_file_trusts_nothing_in_state_others_can_write(tmp_path):
    """Another user can write to the state directory, so the version in the
    database proves nothing. The check reads nothing there, and the real open
    refuses the storage, as it would without the check."""
    path = tmp_path / "state" / "nerve.db"
    path.parent.mkdir()
    _sqlite_at(path, "fork_schema_version", FORK_SCHEMA_VERSION + 1)
    os.chmod(path.parent, 0o777)
    try:
        await check_database_file(path)
        with pytest.raises(InsecureStateStorage, match="writable by other users"):
            await Database(path).connect()
    finally:
        os.chmod(path.parent, 0o700)
