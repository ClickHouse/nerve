"""The system actor takes the system principal ID that the environment names.

In external mode, the environment can give the ID of the agent's system
principal in ``NERVE_SYSTEM_PRINCIPAL_ID``. The identity bootstrap (server
start and ``nerve init``) gives the system actor that ID while no row refers to
the system actor and no other actor has the ID. It drops the trigger
``system_actor_id_cannot_change`` for that one update and creates it again in
the same transaction. Otherwise the local ID stays, a warning is logged, and
``nerve doctor`` reports the difference. Local mode ignores the variable.
"""

from __future__ import annotations

import asyncio
import sqlite3

import pytest
import pytest_asyncio

from nerve.db import Database
from nerve.db.accounts import (
    SYSTEM_ACTOR_ID_TAKEN,
    SYSTEM_ACTOR_ID_TRIGGER,
    SYSTEM_ACTOR_ID_TRIGGER_SQL,
    inspect_system_actor,
)
from nerve.db.migrations import v047_accounts
from nerve.identity import (
    SYSTEM_PRINCIPAL_ENV,
    Actor,
    InvalidSystemPrincipalId,
    parse_system_principal_id,
)

# Obviously fake IDs.
PRINCIPAL = "00000000-0000-4000-8000-00000000c2c2"
ALICE = "00000000-0000-4000-8000-0000000a11ce"


async def _open(path) -> Database:
    database = Database(path)
    await database.connect()
    return database


async def _trigger_sql(database: Database) -> str | None:
    async with database.db.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?",
        (SYSTEM_ACTOR_ID_TRIGGER,),
    ) as cursor:
        row = await cursor.fetchone()
    return row[0] if row else None


async def _system_rows(database: Database) -> list[str]:
    async with database.db.execute(
        "SELECT id FROM actor_refs WHERE kind = 'system'"
    ) as cursor:
        return [row[0] async for row in cursor]


async def _refer_to_system_actor(database: Database, reference: str) -> str:
    """Make one row refer to the system actor. Returns ``table.column``."""
    system = database.system_actor
    if reference == "session":
        await database.create_session("s1", source="cron", actor=system)
        return "sessions.created_by_actor_id"
    if reference == "message":
        await database.create_session("s1", actor=None)
        await database.add_message("s1", "user", "hello", actor=system)
        return "messages.actor_id"
    # A column that a later migration adds is found from the schema.
    await database._write(
        "CREATE TABLE later_story (id INTEGER PRIMARY KEY, "
        "caused_by_actor_id TEXT REFERENCES actor_refs(id))"
    )
    await database._write(
        "INSERT INTO later_story (caused_by_actor_id) VALUES (?)", (system.actor_id,),
    )
    return "later_story.caused_by_actor_id"


@pytest.fixture
def seeded(monkeypatch):
    """The environment names the system principal ID."""
    monkeypatch.setenv(SYSTEM_PRINCIPAL_ENV, PRINCIPAL)
    return PRINCIPAL


@pytest_asyncio.fixture
async def database(tmp_path):
    database = await _open(tmp_path / "nerve.db")
    yield database
    await database.close()


# --------------------------------------------------------------------------- #
#  The value                                                                   #
# --------------------------------------------------------------------------- #


class TestParse:
    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_unset_or_blank_is_none(self, value):
        assert parse_system_principal_id(value) is None

    def test_a_canonical_uuid_is_accepted(self):
        assert parse_system_principal_id(PRINCIPAL) == PRINCIPAL

    @pytest.mark.parametrize("value", [
        PRINCIPAL.upper(),
        "{" + PRINCIPAL + "}",
        "urn:uuid:" + PRINCIPAL,
        PRINCIPAL.replace("-", ""),
        " " + PRINCIPAL,
        PRINCIPAL + "\n",
        "not-a-uuid",
        "12345",
    ])
    def test_any_other_form_is_refused(self, value):
        with pytest.raises(InvalidSystemPrincipalId, match=SYSTEM_PRINCIPAL_ENV):
            parse_system_principal_id(value)


# --------------------------------------------------------------------------- #
#  The change of the ID in the database                                        #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestRekey:
    async def test_an_unreferenced_system_actor_takes_the_new_id(self, tmp_path):
        database = await _open(tmp_path / "nerve.db")
        try:
            local_id = database.system_actor_id
            result = await database._rekey_system_actor(PRINCIPAL)

            assert result.changed and result.blocker is None
            assert (result.previous_id, result.actor_id) == (local_id, PRINCIPAL)
            assert database.system_actor == Actor(actor_id=PRINCIPAL, kind="system")
            assert await _system_rows(database) == [PRINCIPAL]
            assert await database.get_actor_ref(local_id) is None
        finally:
            await database.close()

        reopened = await _open(tmp_path / "nerve.db")
        try:
            assert reopened.system_actor_id == PRINCIPAL
        finally:
            await reopened.close()

    async def test_the_same_id_changes_nothing(self, database):
        await database._rekey_system_actor(PRINCIPAL)
        result = await database._rekey_system_actor(PRINCIPAL)
        assert not result.changed and result.blocker is None
        assert database.system_actor_id == PRINCIPAL

    async def test_the_trigger_is_created_again_with_the_v047_statement(self, database):
        assert SYSTEM_ACTOR_ID_TRIGGER_SQL in v047_accounts.SQL
        fresh = await _trigger_sql(database)
        assert fresh is not None

        await database._rekey_system_actor(PRINCIPAL)

        assert await _trigger_sql(database) == fresh

    async def test_the_trigger_still_blocks_other_id_changes(self, database):
        local_id = database.system_actor_id
        await database._rekey_system_actor(PRINCIPAL)
        for other in (local_id, ALICE):
            with pytest.raises(sqlite3.IntegrityError, match="system actor id cannot change"):
                await database._write(
                    "UPDATE actor_refs SET id = ? WHERE kind = 'system'", (other,),
                )
        assert await _system_rows(database) == [PRINCIPAL]

    @pytest.mark.parametrize("reference", ["session", "message", "new_column"])
    async def test_a_reference_in_any_actor_column_keeps_the_id(self, database, reference):
        local_id = database.system_actor_id
        column = await _refer_to_system_actor(database, reference)

        result = await database._rekey_system_actor(PRINCIPAL)

        assert not result.changed
        assert result.blocker == f"{column} refers to the current ID"
        assert database.system_actor_id == local_id
        assert await _system_rows(database) == [local_id]

    async def test_an_id_that_another_actor_has_keeps_the_id(self, database):
        local_id = database.system_actor_id
        await database.upsert_external_actor(PRINCIPAL, "Alice Example")

        result = await database._rekey_system_actor(PRINCIPAL)

        assert not result.changed and result.blocker == SYSTEM_ACTOR_ID_TAKEN
        assert database.system_actor_id == local_id
        assert (await database.get_actor_ref(PRINCIPAL))["kind"] == "human"

    async def test_a_failure_rolls_back_the_id_and_the_trigger(self, database, monkeypatch):
        import nerve.db.accounts as accounts

        local_id = database.system_actor_id
        fresh = await _trigger_sql(database)
        monkeypatch.setattr(accounts, "SYSTEM_ACTOR_ID_TRIGGER_SQL", "CREATE TRIGGER broken")

        with pytest.raises(sqlite3.OperationalError):
            await database._rekey_system_actor(PRINCIPAL)

        assert await _system_rows(database) == [local_id]
        assert await _trigger_sql(database) == fresh
        assert database.system_actor_id == local_id

    async def test_two_connections_agree_on_the_new_id(self, tmp_path):
        first = await _open(tmp_path / "nerve.db")
        second = await _open(tmp_path / "nerve.db")
        try:
            results = await asyncio.gather(
                first._rekey_system_actor(PRINCIPAL),
                second._rekey_system_actor(PRINCIPAL),
            )
            assert sorted(r.changed for r in results) == [False, True]
            assert all(r.blocker is None for r in results)
            assert first.system_actor_id == second.system_actor_id == PRINCIPAL
        finally:
            await first.close()
            await second.close()

    async def test_the_checked_columns_come_from_the_schema(self, database):
        found = set(await database._actor_ref_columns())
        assert {
            ("accounts", "actor_id"),
            ("sessions", "created_by_actor_id"),
            ("messages", "actor_id"),
        } <= found
        await database._write(
            "CREATE TABLE later_story (a TEXT REFERENCES actor_refs, "
            "b TEXT REFERENCES actor_refs(id), c TEXT REFERENCES sessions(id))"
        )
        found_after = set(await database._actor_ref_columns())
        assert found_after - found == {("later_story", "a"), ("later_story", "b")}

    async def test_the_read_only_inspection_gives_the_same_answer(self, tmp_path):
        path = tmp_path / "nerve.db"
        assert inspect_system_actor(path, PRINCIPAL) is None

        database = await _open(path)
        try:
            local_id = database.system_actor_id
            assert inspect_system_actor(path, local_id) == (local_id, None)
            assert inspect_system_actor(path, PRINCIPAL) == (local_id, None)

            column = await _refer_to_system_actor(database, "session")
            blocker = f"{column} refers to the current ID"
            assert inspect_system_actor(path, PRINCIPAL) == (local_id, blocker)
            result = await database._rekey_system_actor(PRINCIPAL)
            assert result.blocker == blocker

            await database.upsert_external_actor(ALICE, None)
            assert inspect_system_actor(path, ALICE) == (local_id, SYSTEM_ACTOR_ID_TAKEN)
        finally:
            await database.close()
