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
import logging
import os
import sqlite3

import httpx
import pytest
import pytest_asyncio
from fastapi import Depends, FastAPI

from nerve import paths
from nerve.config import NerveConfig, load_config, set_config
from nerve.db import Database
from nerve.db.accounts import (
    SYSTEM_ACTOR_ID_TAKEN,
    SYSTEM_ACTOR_ID_TRIGGER,
    SYSTEM_ACTOR_ID_TRIGGER_SQL,
    SYSTEM_PRINCIPAL_CLAIM_NAME,
    inspect_system_actor,
)
from nerve.db.migrations import v047_accounts
from nerve.gateway.auth import (
    AUTH_MODE_ENV,
    AUTH_MODE_EXTERNAL,
    MCP_AUDIENCE,
    create_external_mcp_token,
    create_mcp_session_token,
    create_system_token,
    decode_token,
    pin_auth_mode,
    require_auth,
    resolve_actor_from_claims,
)
from nerve.identity import (
    SYSTEM_PRINCIPAL_ENV,
    Actor,
    InvalidSystemPrincipalId,
    parse_system_principal_id,
)
from nerve.migrate import MigrationReport, _inspect_identity, bootstrap_identity

_SECRET = "test-secret-for-system-principal-pad32"
# Obviously fake IDs.
PRINCIPAL = "00000000-0000-4000-8000-00000000c2c2"
ALICE = "00000000-0000-4000-8000-0000000a11ce"


def _config() -> NerveConfig:
    return NerveConfig.from_dict({"auth": {"jwt_secret": _SECRET}})


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
        await database.create_session("s1", source="cron", actor=system, cause=None)
        return "sessions.created_by_actor_id"
    if reference == "cause":
        await database.create_session("s1", source="cron", actor=None, cause=system)
        return "sessions.caused_by_actor_id"
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


def _claimed(claimed_id: str) -> str:
    return (
        f"the system actor already took the ID {claimed_id} once, and its ID "
        "changes only once"
    )


async def _claim(database: Database) -> str | None:
    return await database._get_instance_secret(SYSTEM_PRINCIPAL_CLAIM_NAME)


def _rekey_from_another_connection(path, new_id: str) -> None:
    """Change the system actor ID as another process would, and commit."""
    other = sqlite3.connect(path)
    try:
        other.execute("BEGIN IMMEDIATE")
        other.execute(f"DROP TRIGGER {SYSTEM_ACTOR_ID_TRIGGER}")
        other.execute("UPDATE actor_refs SET id = ? WHERE kind = 'system'", (new_id,))
        other.execute(SYSTEM_ACTOR_ID_TRIGGER_SQL)
        other.commit()
    finally:
        other.close()


def _warnings(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


@pytest.fixture
def external(monkeypatch):
    """External mode, as ``create_app()`` pins it."""
    monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
    pin_auth_mode(AUTH_MODE_EXTERNAL)


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

    @pytest.mark.parametrize("reference", ["session", "cause", "message", "new_column"])
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
        assert await _claim(database) is None

    async def test_the_change_records_the_claim(self, database):
        assert await _claim(database) is None
        await database._rekey_system_actor(PRINCIPAL)
        assert await _claim(database) == PRINCIPAL

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

    async def test_a_connection_does_not_change_an_id_that_another_one_set(self, tmp_path):
        """Both connections cached the local ID. The second one asks for another
        ID after the first changed it: it keeps the first one's ID, so the ID
        that each connection uses still exists."""
        first = await _open(tmp_path / "nerve.db")
        second = await _open(tmp_path / "nerve.db")
        try:
            assert (await first._rekey_system_actor(PRINCIPAL)).changed

            result = await second._rekey_system_actor(ALICE)

            assert not result.changed
            assert (result.previous_id, result.actor_id) == (PRINCIPAL, PRINCIPAL)
            assert result.blocker == _claimed(PRINCIPAL)
            assert first.system_actor_id == second.system_actor_id == PRINCIPAL
            assert await _system_rows(first) == [PRINCIPAL]
            # Both connections can still write rows attributed to the system actor.
            await first.create_session("s-first", actor=first.system_actor)
            await second.create_session("s-second", actor=second.system_actor)
        finally:
            await first.close()
            await second.close()

    async def test_concurrent_changes_to_different_ids_leave_one_live_id(self, tmp_path):
        first = await _open(tmp_path / "nerve.db")
        second = await _open(tmp_path / "nerve.db")
        try:
            results = await asyncio.gather(
                first._rekey_system_actor(PRINCIPAL),
                second._rekey_system_actor(ALICE),
            )
            assert sorted(r.changed for r in results) == [False, True]
            winner = next(r.actor_id for r in results if r.changed)
            loser = next(r for r in results if not r.changed)
            assert loser.blocker == _claimed(winner)
            assert first.system_actor_id == second.system_actor_id == winner
            assert await _system_rows(first) == [winner]
            await first.create_session("s-first", actor=first.system_actor)
            await second.create_session("s-second", actor=second.system_actor)
        finally:
            await first.close()
            await second.close()

    async def test_a_connection_that_opens_later_cannot_change_the_id_again(self, tmp_path):
        """The second connection opens after the change, so it caches the new
        ID. The claim still refuses a different ID, and the first connection
        keeps an ID that exists."""
        path = tmp_path / "nerve.db"
        first = await _open(path)
        try:
            assert (await first._rekey_system_actor(PRINCIPAL)).changed
            second = await _open(path)
            try:
                assert second.system_actor_id == PRINCIPAL

                result = await second._rekey_system_actor(ALICE)

                assert not result.changed and result.blocker == _claimed(PRINCIPAL)
                assert inspect_system_actor(path, ALICE) == (PRINCIPAL, _claimed(PRINCIPAL))
                assert first.system_actor_id == second.system_actor_id == PRINCIPAL
                assert await _system_rows(first) == [PRINCIPAL]
                await first.create_session("s-first", actor=first.system_actor)
                await second.create_session("s-second", actor=second.system_actor)
            finally:
                await second.close()
        finally:
            await first.close()

    async def test_the_result_gives_the_ids_of_its_own_transaction(
        self, tmp_path, monkeypatch,
    ):
        """Another connection opens between the commit and the cache refresh
        of the first one, and asks for a different ID. It is refused, and the
        first connection reports and caches the ID it set."""
        path = tmp_path / "nerve.db"
        first = await _open(path)
        late: list = []
        refresh = first._cache_system_actor

        async def refresh_after_a_late_change():
            second = await _open(path)
            try:
                late.append(await second._rekey_system_actor(ALICE))
            finally:
                await second.close()
            await refresh()

        try:
            monkeypatch.setattr(first, "_cache_system_actor", refresh_after_a_late_change)
            result = await first._rekey_system_actor(PRINCIPAL)

            assert result.changed and result.actor_id == PRINCIPAL
            assert late[0].blocker == _claimed(PRINCIPAL)
            assert first.system_actor_id == PRINCIPAL
            assert await _system_rows(first) == [PRINCIPAL]
        finally:
            await first.close()

    async def test_without_a_claim_a_connection_keeps_an_id_that_another_one_set(
        self, database,
    ):
        """A database without the claim row (for example from a backup made
        with --no-secrets): the compare with the cached ID still refuses to
        change an ID that another connection set."""
        local_id = database.system_actor_id
        _rekey_from_another_connection(database.db_path, PRINCIPAL)
        assert await _claim(database) is None

        result = await database._rekey_system_actor(ALICE)

        assert not result.changed
        assert result.blocker == (
            f"another process changed the ID from {local_id} to {PRINCIPAL} "
            "after this process read it"
        )
        assert database.system_actor_id == PRINCIPAL

    @pytest.mark.parametrize("referenced", [False, True])
    async def test_a_column_collation_that_is_not_registered_does_not_stop_the_check(
        self, database, referenced,
    ):
        """The probe compares with ``COLLATE BINARY``, so a column declared with
        a collation that only another connection registers can be checked."""
        local_id = database.system_actor_id
        other = sqlite3.connect(database.db_path)
        try:
            other.create_collation("app_order", lambda a, b: (a > b) - (a < b))
            other.execute(
                "CREATE TABLE extension (who TEXT COLLATE app_order "
                "REFERENCES actor_refs(id))"
            )
            if referenced:
                other.execute("INSERT INTO extension (who) VALUES (?)", (local_id,))
            other.commit()
        finally:
            other.close()

        assert inspect_system_actor(database.db_path, PRINCIPAL) == (
            local_id, "extension.who refers to the current ID" if referenced else None,
        )
        result = await database._rekey_system_actor(PRINCIPAL)
        if referenced:
            assert result.blocker == "extension.who refers to the current ID"
            assert database.system_actor_id == local_id
        else:
            assert result.changed and database.system_actor_id == PRINCIPAL

    async def test_a_check_that_cannot_run_keeps_the_id(self, database, monkeypatch):
        import nerve.db.accounts as accounts

        local_id = database.system_actor_id
        real = accounts._rekey_probes

        def with_a_broken_probe(columns, current_id, new_id):
            broken = accounts._Probe(
                "SELECT 1 FROM missing_table WHERE who = ?", (current_id,),
                "missing_table.who", "unused",
            )
            return [*real(columns, current_id, new_id), broken]

        monkeypatch.setattr(accounts, "_rekey_probes", with_a_broken_probe)

        current, blocker = inspect_system_actor(database.db_path, PRINCIPAL)
        assert current == local_id
        assert blocker.startswith("the check of missing_table.who could not run (")
        result = await database._rekey_system_actor(PRINCIPAL)
        assert result.blocker.startswith("the check of missing_table.who could not run (")
        assert database.system_actor_id == local_id
        assert await _system_rows(database) == [local_id]

    async def test_the_read_only_inspection_uses_one_snapshot(self, database, monkeypatch):
        """Another process changes the ID while the inspection runs. The answer
        is the state before that change, not a mix of the two states."""
        import nerve.db.accounts as accounts

        local_id = database.system_actor_id
        real = accounts._rekey_probes

        def probes_after_a_concurrent_change(columns, current_id, new_id):
            _rekey_from_another_connection(database.db_path, new_id)
            return real(columns, current_id, new_id)

        monkeypatch.setattr(accounts, "_rekey_probes", probes_after_a_concurrent_change)

        assert inspect_system_actor(database.db_path, PRINCIPAL) == (local_id, None)

    async def test_the_preview_uses_one_snapshot_and_writes_nothing(
        self, database, monkeypatch,
    ):
        import nerve.db.accounts as accounts

        local_id = database.system_actor_id
        real = accounts._rekey_probes

        def probes_after_a_concurrent_change(columns, current_id, new_id):
            _rekey_from_another_connection(database.db_path, new_id)
            return real(columns, current_id, new_id)

        monkeypatch.setattr(accounts, "_rekey_probes", probes_after_a_concurrent_change)

        result = await database._preview_system_actor_rekey(PRINCIPAL)

        assert (result.previous_id, result.actor_id, result.blocker) == (
            local_id, PRINCIPAL, None,
        )
        assert not database.db.in_transaction
        # The ID that the other connection set, not one that the preview wrote.
        assert await _system_rows(database) == [PRINCIPAL]
        assert database.system_actor_id == local_id

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


# --------------------------------------------------------------------------- #
#  The identity bootstrap                                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestBootstrap:
    async def test_a_fresh_database_takes_the_id(self, database, external, seeded):
        local_id = database.system_actor_id
        report = await bootstrap_identity(database, _config())

        assert report.rekeyed_system_actor and report.did_bootstrap
        assert report.identity_actions[0] == (
            f"changed the system actor ID from {local_id} to {PRINCIPAL}, the ID in "
            f"{SYSTEM_PRINCIPAL_ENV}"
        )
        assert not report.warnings
        assert database.system_actor_id == PRINCIPAL
        assert await database.count_accounts() == 0

    async def test_a_second_start_changes_nothing(self, database, external, seeded, caplog):
        await bootstrap_identity(database, _config())
        with caplog.at_level(logging.INFO, logger="nerve.migrate"):
            report = await bootstrap_identity(database, _config())
        assert not report.rekeyed_system_actor and not report.warnings
        assert database.system_actor_id == PRINCIPAL
        assert "system actor" not in caplog.text

    @pytest.mark.parametrize("reference", ["session", "cause", "message", "new_column"])
    async def test_a_referenced_system_actor_keeps_its_local_id(
        self, database, external, seeded, caplog, reference,
    ):
        local_id = database.system_actor_id
        column = await _refer_to_system_actor(database, reference)

        with caplog.at_level(logging.WARNING, logger="nerve.migrate"):
            report = await bootstrap_identity(database, _config())

        assert database.system_actor_id == local_id
        assert not report.rekeyed_system_actor
        assert len(report.warnings) == 1
        warning = report.warnings[0]
        assert f"keeps its current ID {local_id}" in warning
        assert f"{SYSTEM_PRINCIPAL_ENV} names {PRINCIPAL}" in warning
        assert f"{column} refers to the current ID" in warning
        assert _warnings(caplog) == [f"Identity: {warning}"]

    async def test_an_id_that_another_actor_has_keeps_the_local_id(
        self, database, external, seeded, caplog,
    ):
        local_id = database.system_actor_id
        await database.upsert_external_actor(PRINCIPAL, "Alice Example")

        with caplog.at_level(logging.WARNING, logger="nerve.migrate"):
            report = await bootstrap_identity(database, _config())

        assert database.system_actor_id == local_id
        assert len(report.warnings) == 1 and SYSTEM_ACTOR_ID_TAKEN in report.warnings[0]
        assert len(_warnings(caplog)) == 1

    async def test_local_mode_ignores_the_variable(self, database, seeded, caplog):
        local_id = database.system_actor_id
        with caplog.at_level(logging.INFO, logger="nerve.migrate"):
            report = await bootstrap_identity(database, _config())
        assert database.system_actor_id == local_id
        assert not report.rekeyed_system_actor and not report.warnings
        assert report.bootstrapped_account
        assert "no effect in local mode" in caplog.text
        assert _warnings(caplog) == []

    @pytest.mark.parametrize("value", [PRINCIPAL.upper(), "not-a-uuid"])
    async def test_an_invalid_value_keeps_the_local_id(
        self, database, external, monkeypatch, caplog, value,
    ):
        monkeypatch.setenv(SYSTEM_PRINCIPAL_ENV, value)
        local_id = database.system_actor_id
        with caplog.at_level(logging.WARNING, logger="nerve.migrate"):
            report = await bootstrap_identity(database, _config())
        assert database.system_actor_id == local_id
        assert not report.rekeyed_system_actor
        assert len(report.warnings) == 1 and "canonical" in report.warnings[0]
        assert len(_warnings(caplog)) == 1 and local_id in _warnings(caplog)[0]

    async def test_no_variable_changes_nothing(self, database, external):
        local_id = database.system_actor_id
        report = await bootstrap_identity(database, _config())
        assert database.system_actor_id == local_id
        assert not report.rekeyed_system_actor and not report.warnings

    async def test_a_dry_run_reports_and_writes_nothing(self, database, external, seeded):
        local_id = database.system_actor_id
        report = await bootstrap_identity(database, _config(), dry_run=True)
        assert report.rekeyed_system_actor
        assert report.identity_actions[0].startswith(
            f"change the system actor ID from {local_id} to {PRINCIPAL}"
        )
        assert database.system_actor_id == local_id
        assert await _system_rows(database) == [local_id]

    async def test_two_starts_at_once_agree_and_warn_about_nothing(
        self, tmp_path, external, seeded,
    ):
        first = await _open(tmp_path / "nerve.db")
        second = await _open(tmp_path / "nerve.db")
        try:
            reports = await asyncio.gather(
                bootstrap_identity(first, _config()),
                bootstrap_identity(second, _config()),
            )
            assert sorted(r.rekeyed_system_actor for r in reports) == [False, True]
            assert not any(r.warnings for r in reports)
            assert first.system_actor_id == second.system_actor_id == PRINCIPAL
        finally:
            await first.close()
            await second.close()

    async def test_a_start_that_asks_for_another_id_keeps_the_one_already_set(
        self, tmp_path, external, monkeypatch, caplog,
    ):
        first = await _open(tmp_path / "nerve.db")
        second = await _open(tmp_path / "nerve.db")
        try:
            monkeypatch.setenv(SYSTEM_PRINCIPAL_ENV, PRINCIPAL)
            assert (await bootstrap_identity(first, _config())).rekeyed_system_actor

            monkeypatch.setenv(SYSTEM_PRINCIPAL_ENV, ALICE)
            with caplog.at_level(logging.WARNING, logger="nerve.migrate"):
                report = await bootstrap_identity(second, _config())

            assert not report.rekeyed_system_actor
            assert len(report.warnings) == 1
            assert f"keeps its current ID {PRINCIPAL}" in report.warnings[0]
            assert _claimed(PRINCIPAL) in report.warnings[0]
            assert len(_warnings(caplog)) == 1
            assert first.system_actor_id == second.system_actor_id == PRINCIPAL
            await second.create_session("s-second", actor=second.system_actor)
        finally:
            await first.close()
            await second.close()

    async def test_a_later_start_with_another_value_keeps_the_id(
        self, tmp_path, external, monkeypatch, caplog,
    ):
        """The first process keeps running. A process that starts later, with
        another value, keeps the first ID and logs one warning."""
        path = tmp_path / "nerve.db"
        first = await _open(path)
        try:
            monkeypatch.setenv(SYSTEM_PRINCIPAL_ENV, PRINCIPAL)
            assert (await bootstrap_identity(first, _config())).rekeyed_system_actor

            monkeypatch.setenv(SYSTEM_PRINCIPAL_ENV, ALICE)
            second = await _open(path)
            try:
                with caplog.at_level(logging.WARNING, logger="nerve.migrate"):
                    report = await bootstrap_identity(second, _config())
                assert not report.rekeyed_system_actor
                assert len(report.warnings) == 1
                assert f"keeps its current ID {PRINCIPAL}" in report.warnings[0]
                assert _claimed(PRINCIPAL) in report.warnings[0]
                assert len(_warnings(caplog)) == 1
                assert second.system_actor_id == PRINCIPAL
            finally:
                await second.close()
            assert first.system_actor_id == PRINCIPAL
            await first.create_session("s-first", actor=first.system_actor)
        finally:
            await first.close()


# --------------------------------------------------------------------------- #
#  Tokens                                                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestTokens:
    async def test_system_and_mcp_tokens_resolve_to_the_new_id(
        self, database, external, seeded,
    ):
        await bootstrap_identity(database, _config())
        claims = [
            decode_token(create_system_token(_SECRET), _SECRET),
            decode_token(
                create_mcp_session_token(_SECRET, "engine-sess-1"), _SECRET,
                audience=MCP_AUDIENCE,
            ),
            decode_token(create_external_mcp_token(_SECRET), _SECRET, audience=MCP_AUDIENCE),
        ]
        for claim in claims:
            actor = await resolve_actor_from_claims(database, claim)
            assert actor.actor_id == PRINCIPAL and actor.is_system

    async def test_a_request_with_a_system_token_acts_as_the_new_id(
        self, database, external, seeded, wire_identity_store,
    ):
        set_config(_config())
        try:
            await bootstrap_identity(database, _config())
            wire_identity_store(database)
            app = FastAPI()

            @app.get("/whoami")
            async def whoami(actor: Actor = Depends(require_auth)):
                return {"actor_id": actor.actor_id, "kind": actor.kind}

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://nerve-test",
            ) as client:
                res = await client.get(
                    "/whoami",
                    headers={"Authorization": f"Bearer {create_system_token(_SECRET)}"},
                )
            assert res.status_code == 200
            assert res.json() == {"actor_id": PRINCIPAL, "kind": "system"}
        finally:
            set_config(NerveConfig())


# --------------------------------------------------------------------------- #
#  Entry points: nerve init, server start, the preview                         #
# --------------------------------------------------------------------------- #


class _ProxyThatFails:
    """Stops the lifespan right after the identity and setup-token steps."""

    async def start(self):
        raise RuntimeError("stop after the identity steps")

    async def stop(self):
        return None


class TestEntryPoints:
    def test_nerve_init_takes_the_id(self, monkeypatch, seeded):
        """``nerve init`` reads the mode and the ID from the environment."""
        from nerve.migrate import bootstrap_identity_sync

        monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
        report = bootstrap_identity_sync(NerveConfig())
        assert report.rekeyed_system_actor
        assert inspect_system_actor(paths.db_path(), PRINCIPAL) == (PRINCIPAL, None)

    @pytest.mark.asyncio
    async def test_the_server_start_takes_the_id_before_it_serves(
        self, tmp_path, monkeypatch, seeded,
    ):
        import nerve.proxy.service as proxy_module
        from nerve.gateway import server

        config = NerveConfig()
        config.workspace = tmp_path / "ws"
        config.workspace.mkdir()
        config.proxy.enabled = True
        config.mcp_endpoint.enabled = False
        set_config(config)
        try:
            monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
            app = server.create_app()
            monkeypatch.setattr(proxy_module, "ProxyService", lambda cfg: _ProxyThatFails())
            with pytest.raises(RuntimeError, match="stop after the identity steps"):
                async with server.lifespan(app):
                    pass
        finally:
            set_config(NerveConfig())

        assert inspect_system_actor(paths.db_path(), PRINCIPAL) == (PRINCIPAL, None)

    @pytest.mark.asyncio
    async def test_the_preview_reports_what_the_next_start_does(self, external, seeded):
        report = MigrationReport(dry_run=True)
        _inspect_identity(NerveConfig(), paths.db_path(), report)
        assert report.rekeyed_system_actor
        assert report.identity_actions[0] == (
            f"give the new system actor the ID {PRINCIPAL}, the ID in {SYSTEM_PRINCIPAL_ENV}"
        )

        database = await _open(paths.db_path())
        try:
            local_id = database.system_actor_id
            report = MigrationReport(dry_run=True)
            _inspect_identity(NerveConfig(), paths.db_path(), report)
            assert report.rekeyed_system_actor
            assert report.identity_actions[0].startswith(
                f"change the system actor ID from {local_id} to {PRINCIPAL}"
            )

            column = await _refer_to_system_actor(database, "session")
            report = MigrationReport(dry_run=True)
            _inspect_identity(NerveConfig(), paths.db_path(), report)
            assert not report.rekeyed_system_actor
            assert len(report.warnings) == 1 and column in report.warnings[0]
        finally:
            await database.close()

    def test_the_preview_reports_an_invalid_value(self, external, monkeypatch):
        monkeypatch.setenv(SYSTEM_PRINCIPAL_ENV, "not-a-uuid")
        report = MigrationReport(dry_run=True)
        _inspect_identity(NerveConfig(), paths.db_path(), report)
        assert not report.rekeyed_system_actor
        assert len(report.warnings) == 1 and "canonical" in report.warnings[0]

    @staticmethod
    def _install(tmp_path):
        """A configured install whose gateway starts the proxy, so that a
        failing proxy stops the start right after the identity steps."""
        config_dir, workspace = tmp_path / "cfg", tmp_path / "ws"
        config_dir.mkdir()
        (workspace / "config").mkdir(parents=True)
        (config_dir / "config.yaml").write_text(
            f"workspace: {workspace}\n"
            "proxy:\n  enabled: true\n"
            "mcp_endpoint:\n  enabled: false\n",
            encoding="utf-8",
        )
        (config_dir / "config.local.yaml").write_text("{}\n", encoding="utf-8")
        return config_dir

    def test_the_start_preview_leaves_out_the_system_actor_step(
        self, tmp_path, external, seeded,
    ):
        from nerve.migrate import maybe_migrate

        config_dir = self._install(tmp_path)
        start = maybe_migrate(config_dir, preview_system_principal=False)
        assert not start.rekeyed_system_actor
        assert not any("system actor" in a for a in start.identity_actions)
        # Without the flag (nerve upgrade), the preview shows the step.
        upgrade = maybe_migrate(config_dir)
        assert upgrade.rekeyed_system_actor

    @staticmethod
    def _watch_a_foreground_start(tmp_path, monkeypatch) -> tuple[list, list[str]]:
        """Prepare ``nerve start -f``: the server stops right after its identity
        bootstrap. Returns the list of env reads and the list of warnings of
        ``nerve.migrate``, both filled while the command runs. HOME points
        into ``tmp_path``, so a default path cannot reach a real install."""
        import nerve.identity as identity
        import nerve.migrate as migrate_module
        from nerve import cli, setup_token
        from nerve.gateway import server

        reads: list[str | None] = []
        read_env = identity.system_principal_id_from_env

        def counted_read():
            reads.append(os.environ.get(SYSTEM_PRINCIPAL_ENV))
            return read_env()

        warnings: list[str] = []

        def record_warning(msg, *args, **kwargs):
            warnings.append(msg % args if args else msg)

        async def stop_after_the_identity_steps(*args, **kwargs):
            raise RuntimeError("stop after the identity steps")

        def run_server(config):
            app = server.create_app()

            async def serve():
                with pytest.raises(RuntimeError, match="stop after the identity steps"):
                    async with server.lifespan(app):
                        pass

            asyncio.run(serve())

        home = tmp_path / "home"
        home.mkdir()
        monkeypatch.setenv("HOME", str(home))
        monkeypatch.setattr(identity, "system_principal_id_from_env", counted_read)
        monkeypatch.setattr(migrate_module.logger, "warning", record_warning)
        monkeypatch.setattr(setup_token, "ensure_setup_token", stop_after_the_identity_steps)
        monkeypatch.setattr(server, "run_server", run_server)
        monkeypatch.setattr(cli, "_get_daemon_status", lambda: (False, None))
        monkeypatch.setattr(
            cli, "_spawn_daemon", lambda *a, **k: pytest.fail("a foreground start spawns nothing"),
        )
        monkeypatch.setattr(cli, "setup_logging", lambda verbose=False: None)
        monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
        return reads, warnings

    def test_a_foreground_start_reads_the_variable_once_and_warns_once(
        self, tmp_path, monkeypatch, seeded,
    ):
        from click.testing import CliRunner

        from nerve import cli

        config_dir = self._install(tmp_path)

        async def refer_to_the_system_actor():
            database = await _open(paths.db_path())
            try:
                await _refer_to_system_actor(database, "session")
            finally:
                await database.close()

        asyncio.run(refer_to_the_system_actor())
        reads, warnings = self._watch_a_foreground_start(tmp_path, monkeypatch)
        try:
            result = CliRunner().invoke(
                cli.main, ["-c", str(config_dir), "start", "-f"], catch_exceptions=False,
            )
        finally:
            set_config(NerveConfig())

        assert result.exit_code == 0, result.output
        assert reads == [PRINCIPAL]
        system_warnings = [w for w in warnings if "system actor" in w]
        assert len(system_warnings) == 1
        assert system_warnings[0].startswith("Identity: the system actor keeps its current ID")

    @pytest.mark.parametrize("value", [PRINCIPAL, "not-a-uuid"])
    def test_a_fresh_foreground_start_reads_the_variable_once(
        self, tmp_path, monkeypatch, value,
    ):
        """``nerve start -f`` on a fresh install runs the slim init and then
        serves in the same process. Only the gateway's bootstrap reads the
        variable: a valid value is applied once, an invalid one warns once."""
        from click.testing import CliRunner

        from nerve import bootstrap, cli

        monkeypatch.setenv(SYSTEM_PRINCIPAL_ENV, value)
        monkeypatch.setattr(bootstrap, "_ensure_web_ui", lambda config_dir: "present")
        monkeypatch.setattr(bootstrap, "port_is_free", lambda port, host="127.0.0.1": True)
        monkeypatch.setattr(
            bootstrap, "propose_port", lambda preferred, bind: (preferred, False),
        )
        reads, warnings = self._watch_a_foreground_start(tmp_path, monkeypatch)
        config_dir, workspace = tmp_path / "cfg", tmp_path / "ws"
        try:
            result = CliRunner().invoke(
                cli.main, ["-c", str(config_dir), "start", "-f"],
                input=f"server\n{workspace}\n8977\nlocalhost\n", catch_exceptions=False,
            )
        finally:
            set_config(NerveConfig())

        assert result.exit_code == 0, result.output
        assert "Fresh install detected" in result.output
        assert (config_dir / "config.local.yaml").exists()
        assert reads == [value]
        system_warnings = [w for w in warnings if "system actor" in w]
        if value == PRINCIPAL:
            assert system_warnings == []
            assert inspect_system_actor(paths.db_path(), PRINCIPAL) == (PRINCIPAL, None)
        else:
            assert len(system_warnings) == 1 and "canonical" in system_warnings[0]


# --------------------------------------------------------------------------- #
#  nerve doctor                                                                #
# --------------------------------------------------------------------------- #


class TestDoctor:
    @staticmethod
    def _config(tmp_path) -> NerveConfig:
        config_dir, workspace = tmp_path / "cfg", tmp_path / "ws"
        config_dir.mkdir()
        (workspace / "config").mkdir(parents=True)
        (config_dir / "config.yaml").write_text(f"workspace: {workspace}\n", encoding="utf-8")
        (config_dir / "config.local.yaml").write_text("{}\n", encoding="utf-8")
        return load_config(config_dir)

    @staticmethod
    def _report(*, external: bool) -> tuple[list[str], list[str]]:
        from nerve.cli import _system_principal_report

        return _system_principal_report(external=external)

    def test_nothing_is_reported_without_the_variable(self):
        assert self._report(external=True) == ([], [])
        assert self._report(external=False) == ([], [])

    @pytest.mark.parametrize("value", [PRINCIPAL, "not-a-uuid"])
    def test_local_mode_says_the_variable_has_no_effect(self, monkeypatch, value):
        monkeypatch.setenv(SYSTEM_PRINCIPAL_ENV, value)
        lines, warnings = self._report(external=False)
        assert warnings == []
        assert len(lines) == 1 and lines[0].startswith("[--]")
        assert "no effect in local mode" in lines[0]

    def test_an_invalid_value_is_a_warning(self, monkeypatch):
        monkeypatch.setenv(SYSTEM_PRINCIPAL_ENV, "not-a-uuid")
        lines, warnings = self._report(external=True)
        assert lines == []
        assert len(warnings) == 1 and warnings[0].startswith("[WARN]")
        assert "canonical" in warnings[0] and "keeps the current system actor ID" in warnings[0]

    def test_no_database_yet(self, seeded):
        lines, warnings = self._report(external=True)
        assert warnings == []
        assert lines == [
            f"[--] System actor ({SYSTEM_PRINCIPAL_ENV} from this shell): no system "
            f"actor yet; the next start gives it {PRINCIPAL}"
        ]

    @pytest.mark.asyncio
    async def test_a_pending_change_and_a_kept_id(self, seeded):
        database = await _open(paths.db_path())
        try:
            local_id = database.system_actor_id
            lines, warnings = self._report(external=True)
            assert warnings == []
            assert lines == [
                f"[--] System actor ({SYSTEM_PRINCIPAL_ENV} from this shell): "
                f"{local_id}; the next start changes it to {PRINCIPAL}"
            ]

            column = await _refer_to_system_actor(database, "session")
            lines, warnings = self._report(external=True)
            assert lines == []
            assert len(warnings) == 1 and warnings[0].startswith("[WARN]")
            assert f"keeps the current ID {local_id}, not {PRINCIPAL}" in warnings[0]
            assert f"{column} refers to the current ID" in warnings[0]
        finally:
            await database.close()

    @pytest.mark.asyncio
    async def test_after_a_seed_a_different_or_invalid_value_names_the_current_id(
        self, external, monkeypatch,
    ):
        """The current ID is the seeded principal here, not a local ID."""
        monkeypatch.setenv(SYSTEM_PRINCIPAL_ENV, PRINCIPAL)
        database = await _open(paths.db_path())
        try:
            await bootstrap_identity(database, _config())
        finally:
            await database.close()

        monkeypatch.setenv(SYSTEM_PRINCIPAL_ENV, ALICE)
        lines, warnings = self._report(external=True)
        assert lines == []
        assert warnings == [
            f"[WARN] System actor ({SYSTEM_PRINCIPAL_ENV} from this shell): keeps the "
            f"current ID {PRINCIPAL}, not {ALICE}: {_claimed(PRINCIPAL)}. Autonomous "
            "work is attributed to that ID."
        ]

        monkeypatch.setenv(SYSTEM_PRINCIPAL_ENV, "not-a-uuid")
        lines, warnings = self._report(external=True)
        assert lines == []
        assert len(warnings) == 1 and "keeps the current system actor ID" in warnings[0]
        assert "local" not in warnings[0]

    @pytest.mark.asyncio
    async def test_the_doctor_report_shows_the_id_in_force(
        self, tmp_path, external, seeded,
    ):
        from nerve.cli import doctor_report

        database = await _open(paths.db_path())
        try:
            await bootstrap_identity(database, _config())
        finally:
            await database.close()
        report = doctor_report(self._config(tmp_path))
        assert (
            f"[OK] System actor ({SYSTEM_PRINCIPAL_ENV} from this shell): {PRINCIPAL}"
            in report
        )
