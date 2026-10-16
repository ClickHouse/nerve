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

    @pytest.mark.parametrize("reference", ["session", "message", "new_column"])
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
        assert f"keeps its local ID {local_id}" in warning
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
        assert "canonical" in warnings[0] and "keeps the local system actor ID" in warnings[0]

    def test_no_database_yet(self, seeded):
        lines, warnings = self._report(external=True)
        assert warnings == []
        assert lines == [
            f"[--] System actor ({SYSTEM_PRINCIPAL_ENV} from this shell): no system "
            f"actor yet; the next start gives it {PRINCIPAL}"
        ]

    @pytest.mark.asyncio
    async def test_a_pending_change_and_a_kept_local_id(self, seeded):
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
            assert f"keeps the local ID {local_id}, not {PRINCIPAL}" in warnings[0]
            assert f"{column} refers to the current ID" in warnings[0]
        finally:
            await database.close()

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
