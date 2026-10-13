"""An older Nerve must not start on a database a newer one migrated."""

from __future__ import annotations

import aiosqlite
import pytest

from nerve.db.migrations import runner


async def _database_at(path, version: int) -> None:
    async with aiosqlite.connect(path) as db:
        await db.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        await db.execute("INSERT INTO schema_version (version) VALUES (?)", (version,))
        await db.commit()


@pytest.mark.asyncio
async def test_newer_schema_refuses_to_start(tmp_path, monkeypatch):
    monkeypatch.delenv(runner.ALLOW_NEWER_SCHEMA_ENV, raising=False)
    code_version = runner.discover_migrations()[-1][0]
    db_path = tmp_path / "newer.db"
    await _database_at(db_path, code_version + 7)

    async with aiosqlite.connect(db_path) as db:
        with pytest.raises(runner.SchemaNewerThanCodeError) as failure:
            await runner.run_migrations(db)

    assert failure.value.database_version == code_version + 7
    assert failure.value.code_version == code_version
    assert runner.ALLOW_NEWER_SCHEMA_ENV in str(failure.value)


@pytest.mark.asyncio
async def test_newer_schema_runs_when_accepted(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv(runner.ALLOW_NEWER_SCHEMA_ENV, "1")
    code_version = runner.discover_migrations()[-1][0]
    db_path = tmp_path / "newer.db"
    await _database_at(db_path, code_version + 1)

    async with aiosqlite.connect(db_path) as db:
        with caplog.at_level("WARNING", logger=runner.__name__):
            final = await runner.run_migrations(db)

    assert final == code_version + 1
    assert any("newer than this code" in record.getMessage() for record in caplog.records)


@pytest.mark.asyncio
async def test_current_schema_is_not_a_newer_schema(tmp_path, monkeypatch):
    monkeypatch.delenv(runner.ALLOW_NEWER_SCHEMA_ENV, raising=False)
    code_version = runner.discover_migrations()[-1][0]
    db_path = tmp_path / "current.db"
    await _database_at(db_path, code_version)

    async with aiosqlite.connect(db_path) as db:
        assert await runner.run_migrations(db) == code_version
