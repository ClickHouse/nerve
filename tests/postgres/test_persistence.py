import asyncio
import os
import json
import sqlite3
from pathlib import Path

import pytest

from nerve.config import NerveConfig
from nerve.db import create_database
from nerve.db.postgres import PostgresDatabase
from nerve.tasks.manager import TaskManager


@pytest.mark.asyncio
async def test_isolation_reconnect_and_ephemeral_task_cache(db, tmp_path):
    db.workspace = tmp_path / "first"
    await db.upsert_task(
        "same-id",
        "memory/tasks/active/same-id.md",
        "Durable",
        content="# Durable\nBody",
    )
    assert (await db.get_task("same-id"))["content"] == "# Durable\nBody"
    other = PostgresDatabase(
        os.environ["NERVE_TEST_POSTGRES_DSN"], workflow=db.scope + "-other"
    )
    await other.connect()
    try:
        assert await other.get_task("same-id") is None
        await other.upsert_task(
            "same-id", "memory/tasks/active/same-id.md", "Other", content="Other body"
        )
        assert (await db.get_task("same-id"))["title"] == "Durable"
    finally:
        await other.close()
    tenant, workflow = json.loads(db.scope)
    restored = PostgresDatabase(
        os.environ["NERVE_TEST_POSTGRES_DSN"],
        tenant=tenant,
        workflow=workflow,
        workspace=tmp_path / "fresh",
    )
    await restored.connect()
    try:
        assert await TaskManager(restored.workspace, restored).reindex() == 1
        assert (
            restored.workspace / "memory/tasks/active/same-id.md"
        ).read_text() == "# Durable\nBody"
        assert not list(tmp_path.rglob("*.db"))
    finally:
        await restored.close()


@pytest.mark.asyncio
async def test_rollback_and_scope_foreign_keys(db):
    await db.create_session("session", actor=db.system_actor)
    with pytest.raises(RuntimeError, match="abort"):
        async with db._atomic():
            await db.db.execute(
                "UPDATE sessions SET title=? WHERE id=?", ("lost", "session")
            )
            raise RuntimeError("abort")
    assert (await db.get_session("session"))["title"] != "lost"
    with pytest.raises(sqlite3.IntegrityError):
        await db.add_message("missing", "user", "cannot cross scopes", actor=None)
    assert await db.get_messages("session") == []


@pytest.mark.asyncio
async def test_parallel_message_counts_and_native_binding(db):
    await db.create_session("session", actor=db.system_actor)
    await asyncio.gather(
        *(db.add_message("session", "user", str(i), actor=None) for i in range(12))
    )
    assert (await db.get_session("session"))["message_count"] == 12
    assert len(await db.get_messages("session")) == 12
    import psycopg

    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        async with db.db.execute("CREATE TABLE nerve_pg.forbidden (id integer)"):
            pass


@pytest.mark.asyncio
async def test_select_config_and_missing_database_does_not_fallback(tmp_path):
    config = NerveConfig.from_dict(
        {
            "use_postgresql": True,
            "postgresql_dsn": "postgresql://invalid",
            "workspace": str(tmp_path),
        }
    )
    assert isinstance(create_database(config), PostgresDatabase)
    assert "postgresql://invalid" not in repr(config)
    config.postgresql_dsn = ""
    with pytest.raises(ValueError, match="postgresql_dsn"):
        create_database(config)
    assert not list(tmp_path.rglob("*.db"))


def test_memory_reconnect_and_sources(db_scope, tmp_path):
    from nerve.memory.postgres import MemoryStore

    config = NerveConfig.from_dict(
        {
            "use_postgresql": True,
            "postgresql_dsn": os.environ["NERVE_TEST_POSTGRES_DSN"],
            "workflow_id": db_scope,
        }
    )
    source = tmp_path / "original"
    source.write_text("Synthetic durable source")
    store = MemoryStore(config)
    resource = store.resource_repo.create_resource(
        url="test://source",
        modality="document",
        local_path=str(source),
        caption="example",
        embedding=[1.0, 0.0, 0.0],
        user_data={},
    )
    item = store.memory_item_repo.create_item(
        resource_id=resource.id,
        memory_type="knowledge",
        summary="A retained fact",
        embedding=[1.0, 0.0, 0.0],
        user_data={},
    )
    store.close()
    source.unlink()
    restored = MemoryStore(config)
    try:
        assert restored.items[item.id].summary == "A retained fact"
        restored.restore_resources(tmp_path / "restored")
        assert (
            Path(restored.resources[resource.id].local_path).read_text()
            == "Synthetic durable source"
        )
        config.workflow_id += "-other"
        other = MemoryStore(config)
        try:
            assert other.items == {}
            assert other.resources == {}
        finally:
            other.close()
    finally:
        restored.close()


@pytest.mark.asyncio
async def test_reconnect_does_not_resurrect_deleted_status(db):
    await db.db.execute("DELETE FROM task_statuses WHERE name='deferred'")
    await db.close()
    await db.connect()
    async with db.db.execute(
        "SELECT name FROM task_statuses WHERE name='deferred'"
    ) as cursor:
        assert await cursor.fetchone() is None


@pytest.mark.asyncio
async def test_upload_bytes_survive_lost_cache(db, tmp_path):
    db.workspace = tmp_path / "restored"
    await db.create_session("session", actor=db.system_actor)
    source = tmp_path / "source"
    source.write_bytes(b"durable upload")
    await db.save_uploaded_file(
        "file", "session", "source.txt", "text/plain", "text", 14, str(source)
    )
    source.unlink()
    uploaded = await db.get_uploaded_file("file")
    assert Path(uploaded["disk_path"]).read_bytes() == b"durable upload"
    assert "content" not in uploaded


def test_memory_tools_read_postgres(db_scope, tmp_path):
    from nerve.memory.postgres import MemoryStore
    from nerve.agent.tools.handlers.memory import _fetch_records_rows
    from nerve.gateway.routes.memory import _read_memu_snapshot_sync

    config = NerveConfig.from_dict(
        {
            "use_postgresql": True,
            "postgresql_dsn": os.environ["NERVE_TEST_POSTGRES_DSN"],
            "workflow_id": db_scope,
        }
    )
    store = MemoryStore(config)
    try:
        item = store.memory_item_repo.create_item(
            resource_id=None,
            memory_type="knowledge",
            summary="Retained",
            embedding=None,
            user_data={},
        )
        assert (
            _fetch_records_rows(
                "unused", "2000-01-01", "2100-01-01", 10, False, config
            )[0]["id"]
            == item.id
        )
        assert (
            json.loads(_read_memu_snapshot_sync("unused", config))["items"][0][
                "summary"
            ]
            == "Retained"
        )
    finally:
        store.close()


@pytest.mark.asyncio
async def test_account_guard_serializes_across_connections(db):
    from nerve.db.accounts import LastAccountError

    first = await db.create_managed_account(username="alice", credential="synthetic")
    second = await db.create_managed_account(username="bob", credential="synthetic")
    tenant, workflow = json.loads(db.scope)
    other = PostgresDatabase(
        os.environ["NERVE_TEST_POSTGRES_DSN"], tenant=tenant, workflow=workflow
    )
    await other.connect()
    try:
        outcomes = await asyncio.gather(
            db.disable_account(first["id"]),
            other.disable_account(second["id"]),
            return_exceptions=True,
        )
        assert sum(isinstance(result, LastAccountError) for result in outcomes) == 1
        assert await db.count_accounts(enabled_only=True) == 1
    finally:
        await other.close()


@pytest.mark.asyncio
async def test_identity_bootstrap_reuses_owner_and_secret(db, tmp_path):
    from nerve.migrate import bootstrap_identity
    from nerve.db.accounts import read_instance_secret, JWT_SECRET_NAME

    config = NerveConfig.from_dict(
        {
            "use_postgresql": True,
            "postgresql_dsn": os.environ["NERVE_TEST_POSTGRES_DSN"],
            "workflow_id": json.loads(db.scope)[1],
            "workspace": str(tmp_path),
        }
    )
    await bootstrap_identity(db, config)
    accounts = await db.list_accounts()
    secret = read_instance_secret(
        tmp_path / "absent.db", JWT_SECRET_NAME, config=config
    )
    assert secret
    await db.close()
    await db.connect()
    await bootstrap_identity(db, config)
    assert await db.list_accounts() == accounts
    assert (
        read_instance_secret(tmp_path / "absent.db", JWT_SECRET_NAME, config=config)
        == secret
    )
    assert not (tmp_path / "absent.db").exists()


@pytest.mark.asyncio
async def test_schema_tracks_sqlite_columns(db, tmp_path):
    from nerve.db import Database, SCHEMA_VERSION

    assert SCHEMA_VERSION == 49, (
        "Review the PostgreSQL schema when adding an operational migration"
    )
    sqlite = Database(tmp_path / "schema.db")
    await sqlite.connect()
    try:
        async with sqlite.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ) as cursor:
            names = [row[0] async for row in cursor]
        for name in names:
            if name.startswith(("sqlite_", "tasks_fts")) or name == "schema_version":
                continue
            async with sqlite.db.execute(f'PRAGMA table_info("{name}")') as cursor:
                expected = {row[1] async for row in cursor}
            async with db.db.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='nerve_pg' AND table_name=?",
                (name,),
            ) as cursor:
                actual = {row[0] async for row in cursor}
            assert expected <= actual, (name, expected - actual)
    finally:
        await sqlite.close()


def test_semantic_memory_reinforces_paraphrases(db_scope):
    from nerve.memory.postgres import MemoryStore

    config = NerveConfig.from_dict(
        {
            "use_postgresql": True,
            "postgresql_dsn": os.environ["NERVE_TEST_POSTGRES_DSN"],
            "workflow_id": db_scope,
        }
    )
    store = MemoryStore(config)
    try:
        first = store.memory_item_repo.create_item_reinforce(
            memory_type="knowledge",
            summary="Synthetic fact",
            embedding=[1.0, 0.0, 0.0],
            user_data={},
        )
        second = store.memory_item_repo.create_item_reinforce(
            memory_type="knowledge",
            summary="The same fact paraphrased",
            embedding=[1.0, 0.0, 0.0],
            user_data={},
        )
        assert first.id == second.id
        assert second.extra["reinforcement_count"] == 2
    finally:
        store.close()
