"""V49: which databases start with setup complete."""

from __future__ import annotations

import importlib

import aiosqlite
import pytest

from nerve.db.migrations.runner import discover_migrations

_V049 = importlib.import_module("nerve.db.migrations.v049_instance_setup")


async def _schema_before_v049(db: aiosqlite.Connection) -> None:
    for version, name in discover_migrations():
        if version >= 49:
            break
        await importlib.import_module(f"nerve.db.migrations.{name}").up(db)
    await db.commit()


async def _setup_complete(db: aiosqlite.Connection) -> bool:
    async with db.execute("SELECT 1 FROM instance_setup") as cursor:
        return await cursor.fetchone() is not None


@pytest.mark.asyncio
async def test_a_new_database_starts_with_setup_not_complete(tmp_path):
    async with aiosqlite.connect(tmp_path / "nerve.db") as db:
        await _schema_before_v049(db)
        await _V049.up(db)
        assert not await _setup_complete(db)


@pytest.mark.asyncio
async def test_an_install_from_before_accounts_is_marked_complete(tmp_path):
    async with aiosqlite.connect(tmp_path / "nerve.db") as db:
        await _schema_before_v049(db)
        await db.execute("INSERT INTO sessions (id, title) VALUES ('s1', 'old chat')")
        await _V049.up(db)
        assert await _setup_complete(db)


@pytest.mark.asyncio
async def test_a_database_with_an_account_keeps_it_unclaimed(tmp_path):
    async with aiosqlite.connect(tmp_path / "nerve.db") as db:
        await _schema_before_v049(db)
        await db.execute("INSERT INTO sessions (id, title) VALUES ('s1', 'chat')")
        await db.execute(
            "INSERT INTO actor_refs (id, kind, created_at) VALUES ('a1', 'human', 't')"
        )
        await db.execute(
            "INSERT INTO accounts (id, actor_id, credential_source, created_at) "
            "VALUES ('acc1', 'a1', 'none', 't')"
        )
        await _V049.up(db)
        assert not await _setup_complete(db)
