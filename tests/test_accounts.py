"""V47 schema and the migration-guaranteed system actor."""

from __future__ import annotations

import sqlite3
import uuid

import pytest

from nerve.db import SCHEMA_VERSION, Database
from nerve.db.base import IdentityInvariantError
from nerve.db.accounts import (
    RESERVED_USERNAMES,
    InvalidUsernameError,
    LastAccountError,
    LoginState,
    PasswordlessInstanceError,
    ReservedUsernameError,
    UnnamedAccountError,
    UsernameTakenError,
    normalise_username,
)


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
    async with db.db.execute(
        "SELECT id FROM actor_refs WHERE kind = 'system'"
    ) as cursor:
        assert [row[0] async for row in cursor] == [db.system_actor_id]

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
async def test_connect_caches_the_system_actor(tmp_path):
    db = Database(tmp_path / "nerve.db")
    await db.connect()
    try:
        actor = db.system_actor
        assert actor is db.system_actor
        assert actor.actor_id == db.system_actor_id
        assert actor.is_system and actor.account_id is None
    finally:
        await db.close()
    with pytest.raises(RuntimeError, match="not connected"):
        db.system_actor


@pytest.mark.asyncio
async def test_account_schema_guards_identity_and_login_state(db: Database):
    human = await _insert_human(db)
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

    async def test_renaming_to_the_same_username_is_not_a_clash(self, db: Database):
        """The unique index sees the row being updated as itself, and a
        no-op rename must not be reported as somebody else's name."""
        actor = await db.create_actor_ref(kind="human")
        account = await db.create_account(
            actor_id=actor["id"], credential_source="local",
            credential="$2b$12$x", username="alice",
        )
        same = await db.update_account_login(account["id"], username="ALICE")
        assert same["username"] == "alice"
        again = await db.set_account_username(account["id"], "alice")
        assert again["username"] == "alice"

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
