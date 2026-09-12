"""Tests for nerve.db.accounts — the v047 schema and the AccountStore mixin.

Mechanism only: the configuration-aware bootstrap that decides a fresh
account's ``credential_source`` is covered in ``test_identity_bootstrap.py``.
"""

from __future__ import annotations

import sqlite3
import uuid

import pytest

from nerve.db import SCHEMA_VERSION, Database
from nerve.db.accounts import (
    JWT_SECRET_NAME,
    LOCAL_AGENT_SLUG,
    LOCAL_TENANT_SLUG,
    count_accounts_readonly,
    read_instance_secret,
)

_TABLES = (
    "actor_refs", "accounts", "tenants", "agents", "tenant_memberships",
    "agent_grants", "instance_secrets",
)


def _is_uuid4(value: str) -> bool:
    try:
        return uuid.UUID(value).version == 4
    except (ValueError, AttributeError, TypeError):
        return False


async def _tables(db: Database) -> set[str]:
    async with db.db.execute("SELECT name FROM sqlite_master WHERE type='table'") as cur:
        return {row[0] async for row in cur}


@pytest.mark.asyncio
class TestSchema:
    async def test_accounts_migration_is_applied_by_the_schema_head(self, db: Database):
        """Derived from the module name rather than pinned: the file number is
        expected to be renumbered when this lands after other migrations, and
        later migrations will move the head past it."""
        from nerve.db.migrations import v047_accounts

        number = int(v047_accounts.__name__.rsplit(".", 1)[1].split("_", 1)[0][1:])
        assert number <= SCHEMA_VERSION
        async with db.db.execute("SELECT MAX(version) FROM schema_version") as cur:
            assert (await cur.fetchone())[0] >= number

    async def test_tables_exist_and_start_empty(self, db: Database):
        present = await _tables(db)
        assert set(_TABLES) <= present
        for table in _TABLES:
            async with db.db.execute(f"SELECT COUNT(*) FROM {table}") as cur:
                assert (await cur.fetchone())[0] == 0, table

    async def test_migration_is_reentrant(self, db: Database):
        """Everything is IF NOT EXISTS — replaying the file on a migrated
        database is a no-op, which is what makes the expand step reversible."""
        from nerve.db.migrations import v047_accounts

        await v047_accounts.up(db.db)
        assert set(_TABLES) <= await _tables(db)

    async def test_credential_source_is_constrained(self, db: Database):
        actor = await db.create_actor_ref(kind="human")
        with pytest.raises(ValueError):
            await db.create_account(actor_id=actor["id"], credential_source="ldap")
        # The CHECK constraint holds even for a raw insert.
        with pytest.raises(sqlite3.IntegrityError):
            await db._write(
                """INSERT INTO accounts (id, actor_id, credential_source, enabled,
                                         created_at, updated_at)
                   VALUES ('x', ?, 'ldap', 1, 't', 't')""",
                (actor["id"],),
            )

    async def test_actor_kind_is_constrained(self, db: Database):
        with pytest.raises(ValueError):
            await db.create_actor_ref(kind="robot")


@pytest.mark.asyncio
class TestActorRefs:
    async def test_create_and_get(self, db: Database):
        actor = await db.create_actor_ref(kind="human", display_name="alice")
        assert _is_uuid4(actor["id"])
        assert actor["kind"] == "human"
        assert actor["display_name"] == "alice"
        assert actor["email"] is None
        assert actor["profile_version"] == 1
        assert await db.get_actor_ref(actor["id"]) == actor
        assert await db.get_actor_ref("nope") is None

    async def test_rename_bumps_the_profile_version_and_keeps_the_id(self, db: Database):
        """Display fields are presentation snapshots: a rename changes what is
        shown, never the id anything is attributed to."""
        actor = await db.create_actor_ref(kind="human", display_name="alice")
        renamed = await db.update_actor_profile(actor["id"], display_name="Alice A.")
        assert renamed["id"] == actor["id"]
        assert renamed["display_name"] == "Alice A."
        assert renamed["profile_version"] == 2
        # No-op update: nothing bumps.
        same = await db.update_actor_profile(actor["id"])
        assert same["profile_version"] == 2

    async def test_list_filters_by_kind(self, db: Database):
        await db.create_actor_ref(kind="human")
        await db.create_actor_ref(kind="system")
        assert {a["kind"] for a in await db.list_actor_refs()} == {"human", "system"}
        assert [a["kind"] for a in await db.list_actor_refs(kind="system")] == ["system"]


@pytest.mark.asyncio
class TestAccounts:
    async def test_create_and_lookups(self, db: Database):
        actor = await db.create_actor_ref(kind="human")
        account = await db.create_account(actor_id=actor["id"], credential_source="none")
        assert _is_uuid4(account["id"])
        assert account["actor_id"] == actor["id"]
        assert account["username"] is None
        assert account["credential_source"] == "none"
        assert account["credential"] is None
        assert account["enabled"] is True
        assert account["disabled_at"] is None
        assert await db.get_account(account["id"]) == account
        assert await db.get_account_by_actor(actor["id"]) == account
        assert await db.count_accounts() == 1
        assert await db.list_accounts() == [account]

    async def test_one_account_per_actor(self, db: Database):
        actor = await db.create_actor_ref(kind="human")
        await db.create_account(actor_id=actor["id"], credential_source="none")
        with pytest.raises(sqlite3.IntegrityError):
            await db.create_account(actor_id=actor["id"], credential_source="none")

    async def test_account_needs_a_real_actor(self, db: Database):
        with pytest.raises(sqlite3.IntegrityError):
            await db.create_account(actor_id="ghost", credential_source="none")

    async def test_usernames_are_unique_case_insensitively_and_nulls_are_not(
        self, db: Database,
    ):
        a1 = await db.create_actor_ref(kind="human")
        a2 = await db.create_actor_ref(kind="human")
        a3 = await db.create_actor_ref(kind="human")
        first = await db.create_account(
            actor_id=a1["id"], credential_source="local", credential="$2b$12$x",
            username="Alice",
        )
        with pytest.raises(sqlite3.IntegrityError):
            await db.create_account(
                actor_id=a2["id"], credential_source="local", credential="$2b$12$y",
                username="alice",
            )
        # Two username-less accounts coexist: NULLs are distinct in the index.
        await db.create_account(actor_id=a2["id"], credential_source="none")
        await db.create_account(actor_id=a3["id"], credential_source="none")
        assert (await db.get_account_by_username("ALICE"))["id"] == first["id"]
        assert await db.get_account_by_username("bob") is None

    async def test_disable_and_enable(self, db: Database):
        actor = await db.create_actor_ref(kind="human")
        account = await db.create_account(actor_id=actor["id"], credential_source="none")
        disabled = await db.set_account_enabled(account["id"], False)
        assert disabled["enabled"] is False
        assert disabled["disabled_at"] is not None
        assert await db.count_accounts(enabled_only=True) == 0
        assert await db.list_accounts(include_disabled=False) == []
        assert await db.count_accounts() == 1
        enabled = await db.set_account_enabled(account["id"], True)
        assert enabled["enabled"] is True
        assert enabled["disabled_at"] is None

    async def test_credential_moves(self, db: Database):
        actor = await db.create_actor_ref(kind="human")
        account = await db.create_account(actor_id=actor["id"], credential_source="config")
        local = await db.set_account_credential(
            account["id"], credential_source="local", credential="$2b$12$hash",
        )
        assert (local["credential_source"], local["credential"]) == ("local", "$2b$12$hash")
        # Leaving `local` drops the row's hash: only `local` carries one.
        back = await db.set_account_credential(account["id"], credential_source="none")
        assert (back["credential_source"], back["credential"]) == ("none", None)
        with pytest.raises(ValueError):
            await db.set_account_credential(account["id"], credential_source="oauth")

    async def test_set_username(self, db: Database):
        actor = await db.create_actor_ref(kind="human")
        account = await db.create_account(actor_id=actor["id"], credential_source="none")
        named = await db.set_account_username(account["id"], "alice")
        assert named["username"] == "alice"
        assert (await db.get_account_by_username("alice"))["id"] == account["id"]

    async def test_sole_account_is_only_defined_for_exactly_one(self, db: Database):
        assert await db.get_sole_account() is None
        a1 = await db.create_actor_ref(kind="human")
        acct = await db.create_account(actor_id=a1["id"], credential_source="none")
        assert (await db.get_sole_account())["id"] == acct["id"]
        # A disabled sole account is still the sole account — a row count, not
        # a permission — so the caller decides what disablement means.
        await db.set_account_enabled(acct["id"], False)
        assert (await db.get_sole_account())["id"] == acct["id"]
        a2 = await db.create_actor_ref(kind="human")
        await db.create_account(actor_id=a2["id"], credential_source="none")
        assert await db.get_sole_account() is None


@pytest.mark.asyncio
class TestLocalIdentityBootstrap:
    async def test_creates_the_whole_local_shape(self, db: Database):
        identity = await db.bootstrap_local_identity(credential_source="none")
        assert identity.created == {"tenant", "agent", "owner"}
        for value in (
            identity.tenant_id, identity.agent_id, identity.system_actor_id,
            identity.owner_account_id, identity.owner_actor_id,
        ):
            assert _is_uuid4(value), value

        # One tenant, one agent, two principals (owner + system), one account,
        # one membership, one grant.
        async with db.db.execute("SELECT slug FROM tenants") as cur:
            assert [r[0] async for r in cur] == [LOCAL_TENANT_SLUG]
        async with db.db.execute("SELECT slug, system_actor_id FROM agents") as cur:
            rows = [tuple(r) async for r in cur]
        assert rows == [(LOCAL_AGENT_SLUG, identity.system_actor_id)]
        kinds = {a["id"]: a["kind"] for a in await db.list_actor_refs()}
        assert kinds == {identity.system_actor_id: "system", identity.owner_actor_id: "human"}
        assert await db.count_accounts() == 1
        async with db.db.execute(
            "SELECT tenant_id, actor_id FROM tenant_memberships"
        ) as cur:
            assert [tuple(r) async for r in cur] == [(identity.tenant_id, identity.owner_actor_id)]
        async with db.db.execute(
            "SELECT agent_id, actor_id, role, source FROM agent_grants"
        ) as cur:
            assert [tuple(r) async for r in cur] == [
                (identity.agent_id, identity.owner_actor_id, "owner", "bootstrap"),
            ]

        account = await db.get_account(identity.owner_account_id)
        assert account["actor_id"] == identity.owner_actor_id
        assert account["username"] is None
        assert account["credential_source"] == "none"
        assert account["credential"] is None
        assert account["enabled"] is True

        system = await db.get_system_principal()
        assert system["id"] == identity.system_actor_id
        assert system["kind"] == "system"
        found = await db.get_local_identity()
        assert (found.tenant_id, found.agent_id, found.system_actor_id) == (
            identity.tenant_id, identity.agent_id, identity.system_actor_id,
        )

    async def test_repeat_finds_the_same_rows(self, db: Database):
        first = await db.bootstrap_local_identity(credential_source="config", display_name="alice")
        second = await db.bootstrap_local_identity(credential_source="none")
        assert second.created == frozenset()
        assert second.owner_account_id is None  # nothing new was made
        assert (second.tenant_id, second.agent_id, second.system_actor_id) == (
            first.tenant_id, first.agent_id, first.system_actor_id,
        )
        assert await db.count_accounts() == 1
        account = (await db.list_accounts())[0]
        assert account["id"] == first.owner_account_id
        # The second call's different source does not touch the existing row.
        assert account["credential_source"] == "config"
        assert len(await db.list_actor_refs()) == 2

    async def test_a_disabled_account_is_not_resurrected(self, db: Database):
        first = await db.bootstrap_local_identity(credential_source="none")
        await db.set_account_enabled(first.owner_account_id, False)
        again = await db.bootstrap_local_identity(credential_source="none")
        assert "owner" not in again.created
        accounts = await db.list_accounts()
        assert [a["id"] for a in accounts] == [first.owner_account_id]
        assert accounts[0]["enabled"] is False

    async def test_display_name_lands_on_the_owner_only(self, db: Database):
        identity = await db.bootstrap_local_identity(
            credential_source="none", display_name="alice",
        )
        assert (await db.get_actor_ref(identity.owner_actor_id))["display_name"] == "alice"
        assert (await db.get_actor_ref(identity.system_actor_id))["display_name"] is None

    async def test_before_bootstrap_there_is_no_identity(self, db: Database):
        assert await db.get_local_identity() is None
        assert await db.get_system_principal() is None

    async def test_rejects_an_unknown_source_before_writing(self, db: Database):
        with pytest.raises(ValueError):
            await db.bootstrap_local_identity(credential_source="oauth")
        assert await db.get_local_identity() is None


@pytest.mark.asyncio
class TestInstanceSecrets:
    async def test_first_writer_wins(self, db: Database):
        assert await db.get_instance_secret(JWT_SECRET_NAME) is None
        assert await db.ensure_instance_secret(JWT_SECRET_NAME, "first") == "first"
        assert await db.ensure_instance_secret(JWT_SECRET_NAME, "second") == "first"
        assert await db.get_instance_secret(JWT_SECRET_NAME) == "first"

    async def test_read_only_helpers(self, db: Database, tmp_path):
        # Missing database: nothing to read, nothing created.
        missing = tmp_path / "absent.db"
        assert read_instance_secret(missing, JWT_SECRET_NAME) == ""
        assert count_accounts_readonly(missing) is None
        assert not missing.exists()

        await db.ensure_instance_secret(JWT_SECRET_NAME, "stored")
        await db.bootstrap_local_identity(credential_source="none")
        assert read_instance_secret(db.db_path, JWT_SECRET_NAME) == "stored"
        assert read_instance_secret(db.db_path, "other") == ""
        assert count_accounts_readonly(db.db_path) == 1

    async def test_read_only_helpers_tolerate_a_pre_v047_database(self, tmp_path):
        old = tmp_path / "old.db"
        conn = sqlite3.connect(str(old))
        conn.execute("CREATE TABLE schema_version (version INTEGER)")
        conn.commit()
        conn.close()
        assert read_instance_secret(old, JWT_SECRET_NAME) == ""
        assert count_accounts_readonly(old) is None
