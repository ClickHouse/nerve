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
    NotClaimableError,
    ReservedUsernameError,
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
class TestManagedAccountCreation:
    async def _owner(self, db: Database):
        actor = await db.create_actor_ref(kind="human", display_name="Alice")
        return await db.create_account(
            actor_id=actor["id"], credential_source="local",
            credential="$2b$12$synthetic", username="alice",
        )

    async def test_two_connections_create_one_account_and_no_orphan_actor(
        self, db: Database,
    ):
        import asyncio

        await self._owner(db)
        other = Database(db.db_path)
        await other.connect()
        try:
            results = await asyncio.gather(
                db.create_managed_account(username="bob", credential="$2b$12$x"),
                other.create_managed_account(username="BOB", credential="$2b$12$y"),
                return_exceptions=True,
            )
        finally:
            await other.close()
        taken = [r for r in results if isinstance(r, UsernameTakenError)]
        made = [r for r in results if isinstance(r, dict)]
        assert len(taken) == 1 and len(made) == 1
        assert await db.count_accounts() == 2
        assert len(await db.list_actor_refs(kind="human")) == 2


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


@pytest.mark.asyncio
class TestClaimingTheSoleAccount:
    """The first-run "claim and secure" step, for the setup wizard. One
    transaction, and the precondition checked inside it."""

    async def _unclaimed(self, db: Database) -> dict:
        actor = await db.create_actor_ref(kind="human")
        return await db.create_account(actor_id=actor["id"], credential_source="none")

    async def test_names_and_secures_in_one_step(self, db: Database):
        account = await self._unclaimed(db)
        claimed = await db.claim_sole_account(
            username="Alice", credential="$2b$12$claimed", display_name="Alice A",
        )
        assert claimed["id"] == account["id"]
        assert claimed["username"] == "alice"          # normalised on the way in
        assert claimed["credential_source"] == "local"
        assert claimed["credential"] == "$2b$12$claimed"
        actor = await db.get_actor_ref(account["actor_id"])
        assert actor["display_name"] == "Alice A"
        assert actor["profile_version"] == 2
        assert not (await db.login_state()).passwordless

    async def test_an_already_claimed_account_is_refused(self, db: Database):
        await self._unclaimed(db)
        await db.claim_sole_account(username="alice", credential="$2b$12$first")
        with pytest.raises(NotClaimableError):
            await db.claim_sole_account(username="bob", credential="$2b$12$second")
        (account,) = await db.list_accounts()
        assert account["username"] == "alice"
        assert account["credential"] == "$2b$12$first"

    async def test_an_install_without_one_account_is_refused(self, db: Database):
        with pytest.raises(NotClaimableError):
            await db.claim_sole_account(username="alice", credential="$2b$12$x")
        await self._unclaimed(db)
        await self._unclaimed(db)
        with pytest.raises(NotClaimableError):
            await db.claim_sole_account(username="alice", credential="$2b$12$x")

    async def test_two_connections_racing_to_claim_leave_one_winner(
        self, db: Database, tmp_path,
    ):
        """The reason this exists rather than get_sole_account() +
        update_account_login(): those are two transactions, so the second caller
        reads "one account, no password" before the first commits and quietly
        replaces its password with its own."""
        import asyncio

        await self._unclaimed(db)
        other = Database(db.db_path)
        await other.connect()
        try:
            results = await asyncio.gather(
                db.claim_sole_account(username="alice", credential="$2b$12$alice"),
                other.claim_sole_account(username="bob", credential="$2b$12$bob"),
                return_exceptions=True,
            )
        finally:
            await other.close()
        refused = [r for r in results if isinstance(r, NotClaimableError)]
        won = [r for r in results if isinstance(r, dict)]
        assert len(refused) == 1 and len(won) == 1, results
        (account,) = await db.list_accounts()
        assert account["username"] == won[0]["username"]
        assert account["credential"] == won[0]["credential"]
