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
    RESERVED_USERNAMES,
    InvalidUsernameError,
    LastAccountError,
    LoginState,
    PasswordlessInstanceError,
    ReservedUsernameError,
    UnnamedAccountError,
    UsernameTakenError,
    count_accounts_readonly,
    normalise_username,
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


@pytest.mark.asyncio
class TestDalInvariants:
    """F16: the DAL and the schema reject internally inconsistent identity rows,
    so a later PR cannot attach a login to the system principal, keep a
    credential on a config/none account, create an unusable local account, or
    leave disablement half-set."""

    async def test_account_must_reference_a_human_actor(self, db: Database):
        system = await db.create_actor_ref(kind="system")
        with pytest.raises(ValueError, match="human"):
            await db.create_account(actor_id=system["id"], credential_source="none")

    async def test_the_trigger_backstops_a_raw_insert_for_a_system_actor(self, db: Database):
        system = await db.create_actor_ref(kind="system")
        with pytest.raises(sqlite3.IntegrityError):
            await db._write(
                """INSERT INTO accounts (id, actor_id, credential_source, enabled,
                                         created_at, updated_at)
                   VALUES ('x', ?, 'none', 1, 't', 't')""",
                (system["id"],),
            )

    async def test_the_trigger_backstops_re_pointing_an_account_at_a_system_actor(
        self, db: Database,
    ):
        """F23: the INSERT trigger alone leaves two ways to break the invariant
        after the fact. The first is a raw UPDATE of accounts.actor_id."""
        human = await db.create_actor_ref(kind="human")
        system = await db.create_actor_ref(kind="system")
        account = await db.create_account(actor_id=human["id"], credential_source="none")
        with pytest.raises(sqlite3.IntegrityError):
            await db._write(
                "UPDATE accounts SET actor_id = ? WHERE id = ?", (system["id"], account["id"]),
            )
        assert (await db.get_account(account["id"]))["actor_id"] == human["id"]

    async def test_the_trigger_backstops_changing_a_referenced_actors_kind(
        self, db: Database,
    ):
        """The second: turning the actor an account references into a system
        principal. An unreferenced actor may still change kind."""
        human = await db.create_actor_ref(kind="human")
        await db.create_account(actor_id=human["id"], credential_source="none")
        with pytest.raises(sqlite3.IntegrityError):
            await db._write("UPDATE actor_refs SET kind = 'system' WHERE id = ?", (human["id"],))
        assert (await db.get_actor_ref(human["id"]))["kind"] == "human"

        loose = await db.create_actor_ref(kind="human")
        await db._write("UPDATE actor_refs SET kind = 'system' WHERE id = ?", (loose["id"],))
        assert (await db.get_actor_ref(loose["id"]))["kind"] == "system"

    async def test_local_requires_a_credential(self, db: Database):
        actor = await db.create_actor_ref(kind="human")
        with pytest.raises(ValueError, match="credential is required"):
            await db.create_account(actor_id=actor["id"], credential_source="local")

    async def test_config_and_none_reject_a_credential(self, db: Database):
        a1 = await db.create_actor_ref(kind="human")
        a2 = await db.create_actor_ref(kind="human")
        with pytest.raises(ValueError, match="must be None"):
            await db.create_account(
                actor_id=a1["id"], credential_source="config", credential="$2b$12$x",
            )
        with pytest.raises(ValueError, match="must be None"):
            await db.create_account(
                actor_id=a2["id"], credential_source="none", credential="$2b$12$x",
            )

    async def test_credential_check_holds_for_a_raw_insert(self, db: Database):
        actor = await db.create_actor_ref(kind="human")
        # local without a credential violates the schema CHECK directly.
        with pytest.raises(sqlite3.IntegrityError):
            await db._write(
                """INSERT INTO accounts (id, actor_id, credential_source, credential,
                                         enabled, created_at, updated_at)
                   VALUES ('x', ?, 'local', NULL, 1, 't', 't')""",
                (actor["id"],),
            )

    async def test_set_credential_local_requires_a_credential(self, db: Database):
        actor = await db.create_actor_ref(kind="human")
        account = await db.create_account(actor_id=actor["id"], credential_source="none")
        with pytest.raises(ValueError, match="credential is required"):
            await db.set_account_credential(account["id"], credential_source="local")

    async def test_creating_a_disabled_account_sets_disabled_at(self, db: Database):
        actor = await db.create_actor_ref(kind="human")
        account = await db.create_account(
            actor_id=actor["id"], credential_source="none", enabled=False,
        )
        assert account["enabled"] is False
        assert account["disabled_at"] is not None

    async def test_disabled_at_check_holds_for_a_raw_insert(self, db: Database):
        actor = await db.create_actor_ref(kind="human")
        # enabled=0 with disabled_at NULL is an inconsistent disablement state.
        with pytest.raises(sqlite3.IntegrityError):
            await db._write(
                """INSERT INTO accounts (id, actor_id, credential_source, enabled,
                                         created_at, updated_at, disabled_at)
                   VALUES ('x', ?, 'none', 0, 't', 't', NULL)""",
                (actor["id"],),
            )


class TestUsernameNormalisation:
    """Character set, case folding and the reserved list (PR 3). Pure
    function — no database, so no asyncio mark."""

    @pytest.mark.parametrize("raw,expected", [
        ("alice", "alice"),
        ("Alice", "alice"),
        ("  Alice  ", "alice"),
        ("ALICE", "alice"),
        ("a1", "a1"),
        ("0bob", "0bob"),
        ("alice.b", "alice.b"),
        ("alice_b", "alice_b"),
        ("alice-b", "alice-b"),
        ("a" * 32, "a" * 32),
        # Surrounding whitespace, including a stray newline from a paste, is
        # stripped rather than refused.
        ("alice\n", "alice"),
    ])
    def test_accepted_and_lower_cased(self, raw, expected):
        assert normalise_username(raw) == expected

    @pytest.mark.parametrize("raw", [
        None, "", "   ", "a", ".alice", "-alice", "_alice", "al ice", "alice!",
        "alice@example", "a" * 33, "álice", "ALİCE", "ali\nce", "alice/../bob",
    ])
    def test_refused(self, raw):
        with pytest.raises(InvalidUsernameError):
            normalise_username(raw)

    def test_the_legacy_token_subject_is_reserved(self):
        """PR 2's grandfather clause gives the literal string a meaning in
        tokens, so it must never also be somebody's login."""
        from nerve.gateway.auth import LEGACY_SUBJECT

        assert LEGACY_SUBJECT in RESERVED_USERNAMES
        with pytest.raises(ReservedUsernameError):
            normalise_username(LEGACY_SUBJECT)

    @pytest.mark.parametrize("raw", sorted(RESERVED_USERNAMES))
    def test_every_reserved_name_is_refused_in_any_case(self, raw):
        with pytest.raises(ReservedUsernameError):
            normalise_username(raw.upper())

    def test_the_other_token_subjects_are_reserved(self):
        from nerve.gateway import auth as gw_auth

        assert gw_auth.SYSTEM_SUBJECT in RESERVED_USERNAMES
        for subject in ("backend-agent", "external-agent-mcp"):
            assert subject in RESERVED_USERNAMES


@pytest.mark.asyncio
class TestUsernameRulesInTheDal:
    async def test_every_dal_path_normalises(self, db: Database):
        a1 = await db.create_actor_ref(kind="human")
        created = await db.create_account(
            actor_id=a1["id"], credential_source="none", username="Alice",
        )
        assert created["username"] == "alice"

        renamed = await db.set_account_username(created["id"], "  BOB ")
        assert renamed["username"] == "bob"

        relogin = await db.update_account_login(created["id"], username="Carol")
        assert relogin["username"] == "carol"

        with pytest.raises(ReservedUsernameError):
            await db.create_account(
                actor_id=(await db.create_actor_ref(kind="human"))["id"],
                credential_source="none", username="user",
            )
        with pytest.raises(InvalidUsernameError):
            await db.set_account_username(created["id"], "no spaces")

    async def test_taken_usernames_are_reported_as_such(self, db: Database):
        a1 = await db.create_actor_ref(kind="human")
        a2 = await db.create_actor_ref(kind="human")
        await db.create_account(
            actor_id=a1["id"], credential_source="local", credential="$2b$12$x",
            username="alice",
        )
        second = await db.create_account(actor_id=a2["id"], credential_source="none")
        with pytest.raises(UsernameTakenError):
            await db.set_account_username(second["id"], "ALICE")
        with pytest.raises(UsernameTakenError):
            await db.update_account_login(second["id"], username="Alice")


@pytest.mark.asyncio
class TestLoginState:
    async def test_no_accounts(self, db: Database):
        state = await db.login_state()
        assert (state.accounts, state.single_account) == (0, False)
        assert not state.passwordless and not state.setup_pending

    async def test_sole_passwordless_unnamed_account_is_setup_pending(self, db: Database):
        actor = await db.create_actor_ref(kind="human")
        account = await db.create_account(actor_id=actor["id"], credential_source="none")
        state = await db.login_state()
        assert state == LoginState(
            accounts=1, single_account=True, passwordless=True,
            setup_pending=True, sole_account_id=account["id"],
        )

    async def test_naming_the_sole_account_ends_setup_without_ending_passwordless(
        self, db: Database,
    ):
        actor = await db.create_actor_ref(kind="human")
        account = await db.create_account(actor_id=actor["id"], credential_source="none")
        await db.set_account_username(account["id"], "alice")
        state = await db.login_state()
        assert state.passwordless is True
        assert state.setup_pending is False

    @pytest.mark.parametrize("source,credential", [
        ("config", None), ("local", "$2b$12$synthetic"),
    ])
    async def test_a_credential_ends_passwordless(self, db: Database, source, credential):
        actor = await db.create_actor_ref(kind="human")
        await db.create_account(
            actor_id=actor["id"], credential_source=source, credential=credential,
        )
        state = await db.login_state()
        assert state.single_account is True
        assert state.passwordless is False
        assert state.setup_pending is False

    async def test_two_accounts_are_never_single(self, db: Database):
        for _ in range(2):
            actor = await db.create_actor_ref(kind="human")
            await db.create_account(actor_id=actor["id"], credential_source="none")
        state = await db.login_state()
        assert (state.accounts, state.single_account) == (2, False)
        assert not state.passwordless and not state.setup_pending
        assert state.sole_account_id is None

    async def test_a_disabled_account_still_counts(self, db: Database):
        """The relaxations are bounded by how many accounts *exist*, not by how
        many work: a disabled second account still makes a legacy token
        ambiguous."""
        ids = []
        for _ in range(2):
            actor = await db.create_actor_ref(kind="human")
            ids.append((await db.create_account(
                actor_id=actor["id"], credential_source="none",
            ))["id"])
        await db.set_account_enabled(ids[1], False)
        state = await db.login_state()
        assert state.accounts == 2
        assert state.single_account is False


@pytest.mark.asyncio
class TestManagedAccountCreation:
    """create_managed_account — actor + account in one transaction, with the
    guards that must not be check-then-act."""

    async def _owner(self, db: Database, *, source="local", credential="$2b$12$synthetic",
                     username="alice"):
        actor = await db.create_actor_ref(kind="human", display_name="Alice")
        return await db.create_account(
            actor_id=actor["id"], credential_source=source, credential=credential,
            username=username,
        )

    async def test_creates_actor_and_account_together(self, db: Database):
        await self._owner(db)
        created = await db.create_managed_account(
            username="bob", credential="$2b$12$another", display_name="Bob",
        )
        assert created["username"] == "bob"
        assert created["credential_source"] == "local"
        assert created["credential"] == "$2b$12$another"
        assert created["enabled"] is True
        actor = await db.get_actor_ref(created["actor_id"])
        assert actor["kind"] == "human"
        assert actor["display_name"] == "Bob"
        assert await db.count_accounts() == 2

    async def test_refused_while_passwordless(self, db: Database):
        await self._owner(db, source="none", credential=None, username="alice")
        with pytest.raises(PasswordlessInstanceError):
            await db.create_managed_account(username="bob", credential="$2b$12$x")
        assert await db.count_accounts() == 1
        # ...and no orphaned actor was left behind by the refusal.
        assert len(await db.list_actor_refs(kind="human")) == 1

    async def test_refused_while_an_account_has_no_username(self, db: Database):
        await self._owner(db, username=None)
        with pytest.raises(UnnamedAccountError):
            await db.create_managed_account(username="bob", credential="$2b$12$x")
        assert await db.count_accounts() == 1
        assert len(await db.list_actor_refs(kind="human")) == 1

    async def test_naming_and_securing_the_first_account_unblocks_the_second(
        self, db: Database,
    ):
        owner = await self._owner(db, source="none", credential=None, username=None)
        with pytest.raises(PasswordlessInstanceError):
            await db.create_managed_account(username="bob", credential="$2b$12$x")
        await db.update_account_login(
            owner["id"], username="alice", credential="$2b$12$owner",
        )
        created = await db.create_managed_account(username="bob", credential="$2b$12$x")
        assert created["username"] == "bob"
        assert await db.count_accounts() == 2

    async def test_duplicate_username_leaves_no_orphan_actor(self, db: Database):
        await self._owner(db)
        before = len(await db.list_actor_refs())
        with pytest.raises(UsernameTakenError):
            await db.create_managed_account(username="ALICE", credential="$2b$12$x")
        assert len(await db.list_actor_refs()) == before
        assert await db.count_accounts() == 1

    async def test_reserved_and_malformed_usernames_are_refused(self, db: Database):
        await self._owner(db)
        with pytest.raises(ReservedUsernameError):
            await db.create_managed_account(username="user", credential="$2b$12$x")
        with pytest.raises(InvalidUsernameError):
            await db.create_managed_account(username="no spaces", credential="$2b$12$x")

    async def test_a_password_is_required(self, db: Database):
        await self._owner(db)
        with pytest.raises(ValueError):
            await db.create_managed_account(username="bob", credential="")

    async def test_concurrent_creates_of_the_same_username_produce_one_account(
        self, db: Database,
    ):
        import asyncio

        await self._owner(db)
        results = await asyncio.gather(
            db.create_managed_account(username="bob", credential="$2b$12$x"),
            db.create_managed_account(username="BOB", credential="$2b$12$y"),
            return_exceptions=True,
        )
        taken = [r for r in results if isinstance(r, UsernameTakenError)]
        made = [r for r in results if isinstance(r, dict)]
        assert len(taken) == 1 and len(made) == 1
        assert await db.count_accounts() == 2


@pytest.mark.asyncio
class TestLastAccountGuard:
    async def _two(self, db: Database):
        ids = []
        for name in ("alice", "bob"):
            actor = await db.create_actor_ref(kind="human")
            ids.append((await db.create_account(
                actor_id=actor["id"], credential_source="local",
                credential="$2b$12$synthetic", username=name,
            ))["id"])
        return ids

    async def test_the_only_account_cannot_be_disabled(self, db: Database):
        actor = await db.create_actor_ref(kind="human")
        account = await db.create_account(
            actor_id=actor["id"], credential_source="none",
        )
        with pytest.raises(LastAccountError):
            await db.disable_account(account["id"])
        assert (await db.get_account(account["id"]))["enabled"] is True

    async def test_the_last_enabled_of_several_cannot_be_disabled(self, db: Database):
        first, second = await self._two(db)
        await db.disable_account(second)
        with pytest.raises(LastAccountError):
            await db.disable_account(first)
        assert (await db.get_account(first))["enabled"] is True

    async def test_two_concurrent_disables_cannot_both_succeed(self, db: Database):
        """The race the guard exists for: both callers read 'two enabled' and
        both act. Only one may win."""
        import asyncio

        first, second = await self._two(db)
        results = await asyncio.gather(
            db.disable_account(first),
            db.disable_account(second),
            return_exceptions=True,
        )
        refused = [r for r in results if isinstance(r, LastAccountError)]
        assert len(refused) == 1
        assert await db.count_accounts(enabled_only=True) == 1

    async def test_two_connections_cannot_both_disable(self, db: Database, tmp_path):
        """The guard has to hold across *processes* too — a `nerve` CLI beside
        the daemon — which is what BEGIN IMMEDIATE inside the transaction buys:
        the in-process write lock is not in play on a second connection."""
        import asyncio

        first, second = await self._two(db)
        other = Database(db.db_path)
        await other.connect()
        try:
            results = await asyncio.gather(
                db.disable_account(first),
                other.disable_account(second),
                return_exceptions=True,
            )
        finally:
            await other.close()
        refused = [r for r in results if isinstance(r, LastAccountError)]
        assert len(refused) == 1, results
        assert await db.count_accounts(enabled_only=True) == 1

    async def test_disable_is_idempotent(self, db: Database):
        first, second = await self._two(db)
        once = await db.disable_account(second)
        again = await db.disable_account(second)
        assert once["enabled"] is False and again["enabled"] is False
        assert once["disabled_at"] == again["disabled_at"]
        assert await db.count_accounts(enabled_only=True) == 1

    async def test_enable_is_idempotent_and_clears_disabled_at(self, db: Database):
        first, second = await self._two(db)
        await db.disable_account(second)
        back = await db.enable_account(second)
        assert back["enabled"] is True and back["disabled_at"] is None
        again = await db.enable_account(second)
        assert again["enabled"] is True and again["disabled_at"] is None

    async def test_unknown_account(self, db: Database):
        assert await db.disable_account("nope") is None
        assert await db.enable_account("nope") is None

    async def test_the_row_is_never_removed(self, db: Database):
        """Disable is the removal primitive; the row is the tombstone, which is
        what keeps the account count monotone (see the DAL note)."""
        first, second = await self._two(db)
        await db.disable_account(second)
        assert await db.count_accounts() == 2
        assert await db.get_account(second) is not None
        assert not hasattr(db, "delete_account")


@pytest.mark.asyncio
class TestUpdateAccountLogin:
    async def test_sets_username_and_credential_in_one_step(self, db: Database):
        """PR 6's claim: the sole account is named and secured together, so a
        half-claimed account never exists."""
        actor = await db.create_actor_ref(kind="human")
        account = await db.create_account(actor_id=actor["id"], credential_source="none")
        claimed = await db.update_account_login(
            account["id"], username="alice", credential="$2b$12$claimed",
        )
        assert claimed["username"] == "alice"
        assert claimed["credential_source"] == "local"
        assert claimed["credential"] == "$2b$12$claimed"
        state = await db.login_state()
        assert not state.passwordless and not state.setup_pending

    async def test_setting_only_a_password_moves_a_config_row_to_local(self, db: Database):
        actor = await db.create_actor_ref(kind="human")
        account = await db.create_account(actor_id=actor["id"], credential_source="config")
        updated = await db.update_account_login(account["id"], credential="$2b$12$own")
        assert updated["credential_source"] == "local"
        assert updated["username"] is None

    async def test_renaming_does_not_move_the_actor_id(self, db: Database):
        """Usernames are lookup keys, not identity (0.7)."""
        actor = await db.create_actor_ref(kind="human")
        account = await db.create_account(
            actor_id=actor["id"], credential_source="local",
            credential="$2b$12$x", username="alice",
        )
        renamed = await db.update_account_login(account["id"], username="alice2")
        assert renamed["actor_id"] == account["actor_id"] == actor["id"]
        assert renamed["id"] == account["id"]
        assert (await db.get_actor_ref(actor["id"]))["id"] == actor["id"]

    async def test_unknown_account_and_empty_update(self, db: Database):
        assert await db.update_account_login("nope", username="alice") is None
        actor = await db.create_actor_ref(kind="human")
        account = await db.create_account(actor_id=actor["id"], credential_source="none")
        assert (await db.update_account_login(account["id"]))["id"] == account["id"]

    async def test_an_empty_password_is_refused(self, db: Database):
        actor = await db.create_actor_ref(kind="human")
        account = await db.create_account(actor_id=actor["id"], credential_source="none")
        with pytest.raises(ValueError):
            await db.update_account_login(account["id"], credential="")
