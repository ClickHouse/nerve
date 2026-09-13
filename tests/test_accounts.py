"""V47 schema and the migration-guaranteed system actor."""

from __future__ import annotations

import sqlite3
import uuid

import pytest

from nerve.db import SCHEMA_VERSION, Database
from nerve.db.base import IdentityInvariantError


async def _columns(db: Database, table: str) -> set[str]:
    async with db.db.execute(f"PRAGMA table_info({table})") as cursor:
        return {row["name"] async for row in cursor}


async def _insert_human(db: Database, *, name: str | None = None) -> str:
    actor_id = str(uuid.uuid4())
    await db._write(
        "INSERT INTO actor_refs (id, kind, display_name, created_at) "
        "VALUES (?, 'human', ?, 't')",
        (actor_id, name),
    )
    return actor_id


def _corrupt_system_actor(path, shape: str) -> None:
    """Create exceptional state without adding a production mutation API."""
    conn = sqlite3.connect(path)
    try:
        if shape in {"zero", "wrong-kind"}:
            conn.execute("DROP TRIGGER system_actor_cannot_be_deleted")
        if shape == "wrong-kind":
            conn.execute("DROP TRIGGER system_actor_cannot_be_reclassified")
            conn.execute("UPDATE actor_refs SET kind = 'human' WHERE kind = 'system'")
        elif shape == "zero":
            conn.execute("DELETE FROM actor_refs WHERE kind = 'system'")
        else:
            conn.execute("DROP INDEX idx_actor_refs_one_system")
            conn.execute("DROP TRIGGER system_actor_cannot_be_replaced")
            conn.execute(
                "INSERT INTO actor_refs VALUES (?, 'system', NULL, 't')",
                (str(uuid.uuid4()),),
            )
        conn.commit()
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_v047_has_only_the_launch_identity_model(db: Database):
    from nerve.db.migrations import v047_accounts

    number = int(v047_accounts.__name__.rsplit(".", 1)[1].split("_", 1)[0][1:])
    assert number <= SCHEMA_VERSION
    async with db.db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ) as cursor:
        tables = {row[0] async for row in cursor}
    assert {"actor_refs", "accounts", "instance_secrets"} <= tables
    assert not {"tenants", "agents", "tenant_memberships", "agent_grants"} & tables
    assert await _columns(db, "actor_refs") == {
        "id", "kind", "display_name", "created_at",
    }
    assert await _columns(db, "accounts") == {
        "id", "actor_id", "username", "credential_source", "credential",
        "enabled", "created_at",
    }
    assert await _columns(db, "instance_secrets") == {"name", "value"}


@pytest.mark.asyncio
async def test_migration_creates_one_stable_system_actor(db: Database):
    from nerve.db.migrations import v047_accounts

    actor_id = db.system_actor_id
    assert uuid.UUID(actor_id).version == 4
    assert (await db.get_system_principal())["id"] == actor_id
    await v047_accounts.up(db.db)
    async with db.db.execute(
        "SELECT id FROM actor_refs WHERE kind = 'system'"
    ) as cursor:
        assert [row[0] async for row in cursor] == [actor_id]


@pytest.mark.asyncio
async def test_system_actor_cannot_be_duplicated_deleted_or_reclassified(db: Database):
    human = await _insert_human(db)
    with pytest.raises(sqlite3.IntegrityError):
        await db._write(
            "INSERT INTO actor_refs VALUES (?, 'system', NULL, 't')",
            (str(uuid.uuid4()),),
        )
    with pytest.raises(sqlite3.IntegrityError, match="cannot be deleted"):
        await db._write("DELETE FROM actor_refs WHERE id = ?", (db.system_actor_id,))
    with pytest.raises(sqlite3.IntegrityError, match="cannot be reclassified"):
        await db._write(
            "UPDATE actor_refs SET kind = 'human' WHERE id = ?",
            (db.system_actor_id,),
        )
    with pytest.raises(sqlite3.IntegrityError, match="id cannot change"):
        await db._write(
            "UPDATE actor_refs SET id = 'replacement' WHERE id = ?",
            (db.system_actor_id,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        await db._write(
            "UPDATE actor_refs SET id = NULL WHERE id = ?", (db.system_actor_id,)
        )
    with pytest.raises(sqlite3.IntegrityError, match="cannot be replaced"):
        await db._write(
            "INSERT OR REPLACE INTO actor_refs VALUES (?, 'system', NULL, 't')",
            (str(uuid.uuid4()),),
        )
    with pytest.raises(sqlite3.IntegrityError, match="cannot be replaced"):
        await db._write(
            "INSERT OR REPLACE INTO actor_refs VALUES (?, 'human', NULL, 't')",
            (db.system_actor_id,),
        )
    with pytest.raises(sqlite3.IntegrityError, match="cannot be replaced"):
        await db._write(
            "UPDATE OR REPLACE actor_refs SET kind = 'system' WHERE id = ?",
            (human,),
        )
    with pytest.raises(sqlite3.IntegrityError, match="cannot be replaced"):
        await db._write(
            "UPDATE OR REPLACE actor_refs SET id = ? WHERE id = ?",
            (db.system_actor_id, human),
        )
    assert (await db.get_system_principal())["id"] == db.system_actor_id


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["zero", "multiple", "wrong-kind"])
async def test_connect_fails_on_corrupt_system_identity(tmp_path, shape):
    path = tmp_path / "nerve.db"
    db = Database(path)
    await db.connect()
    await db.close()
    _corrupt_system_actor(path, shape)

    reopened = Database(path)
    with pytest.raises(IdentityInvariantError, match="exactly one"):
        await reopened.connect()
    assert reopened._db is None


@pytest.mark.asyncio
async def test_account_schema_guards_identity_and_login_state(db: Database):
    human = await _insert_human(db)
    with pytest.raises(sqlite3.IntegrityError):
        await db._write(
            "INSERT INTO actor_refs VALUES ('robot', 'robot', NULL, 't')"
        )
    with pytest.raises(sqlite3.IntegrityError):
        await db._write(
            """INSERT INTO accounts
                   (id, actor_id, credential_source, credential, enabled, created_at)
               VALUES ('bad-source', ?, 'ldap', NULL, 1, 't')""",
            (human,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        await db._write(
            """INSERT INTO accounts
                   (id, actor_id, credential_source, credential, enabled, created_at)
               VALUES ('bad-local', ?, 'local', NULL, 1, 't')""",
            (human,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        await db._write(
            """INSERT INTO accounts
                   (id, actor_id, credential_source, credential, enabled, created_at)
               VALUES ('bad-none', ?, 'none', 'stale', 1, 't')""",
            (human,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        await db._write(
            """INSERT INTO accounts
                   (id, actor_id, credential_source, credential, enabled, created_at)
               VALUES ('bad-enabled', ?, 'none', NULL, 2, 't')""",
            (human,),
        )
    with pytest.raises(sqlite3.IntegrityError, match="human actor_ref"):
        await db._write(
            """INSERT INTO accounts
                   (id, actor_id, credential_source, credential, enabled, created_at)
               VALUES ('system-login', ?, 'none', NULL, 1, 't')""",
            (db.system_actor_id,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        await db._write(
            """INSERT INTO accounts
                   (id, actor_id, credential_source, credential, enabled, created_at)
               VALUES ('ghost-login', 'ghost', 'none', NULL, 1, 't')"""
        )


@pytest.mark.asyncio
async def test_account_actor_ids_are_distinct_and_usernames_casefold(db: Database):
    first, second = await _insert_human(db), await _insert_human(db)
    await db._write(
        """INSERT INTO accounts
               (id, actor_id, username, credential_source, enabled, created_at)
           VALUES ('account-1', ?, 'Alice', 'none', 1, 't')""",
        (first,),
    )
    assert first != "account-1"
    with pytest.raises(sqlite3.IntegrityError):
        await db._write(
            """INSERT INTO accounts
                   (id, actor_id, username, credential_source, enabled, created_at)
               VALUES ('same-actor', ?, NULL, 'none', 1, 't')""",
            (first,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        await db._write(
            """INSERT INTO accounts
                   (id, actor_id, username, credential_source, enabled, created_at)
               VALUES ('account-2', ?, 'alice', 'none', 1, 't')""",
            (second,),
        )
    await db._write(
        """INSERT INTO accounts
               (id, actor_id, username, credential_source, enabled, created_at)
           VALUES ('account-2', ?, NULL, 'none', 1, 't')""",
        (second,),
    )
    third = await _insert_human(db)
    await db._write(
        """INSERT INTO accounts
               (id, actor_id, username, credential_source, enabled, created_at)
           VALUES ('account-3', ?, NULL, 'none', 1, 't')""",
        (third,),
    )
    with pytest.raises(sqlite3.IntegrityError, match="human actor_ref"):
        await db._write(
            "UPDATE accounts SET actor_id = ? WHERE id = 'account-1'",
            (db.system_actor_id,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        await db._write(
            "UPDATE actor_refs SET kind = 'system' WHERE id = ?", (first,)
        )
