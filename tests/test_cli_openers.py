"""CLI commands open the state database through ``open_production_db``.

That is ``Database.connect`` followed by loading the signing secret. Only the
gateway and ``nerve init`` create accounts, so the first command after an
upgrade leaves a signing secret and no account. Insecure state is refused with
the operator message.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import stat
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from click.testing import CliRunner

from nerve import paths
from nerve.db import Database
from nerve.db.migrations import runner

_OPENERS = pytest.mark.parametrize(
    "argv",
    [["sync"], ["cron"], ["db", "prune", "--dry-run"], ["workflow", "list"]],
    ids=["sync", "cron", "db-prune-dry-run", "workflow-list"],
)


def _config_dir(tmp_path: Path) -> Path:
    config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
    config_dir.mkdir()
    (ws / "config").mkdir(parents=True)
    (config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
    (config_dir / "config.local.yaml").write_text("", encoding="utf-8")
    return config_dir


def _migrated_but_unbootstrapped_db() -> Path:
    """A database at the schema head with its system actor, but no account
    and no signing secret."""

    async def _make() -> None:
        db = Database(paths.db_path())
        await db.connect()
        await db.close()

    asyncio.run(_make())
    return paths.db_path()


def _rows(sql: str) -> list[tuple]:
    conn = sqlite3.connect(str(paths.db_path()))
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def _identity_state() -> tuple[int, int, int]:
    """(accounts, system principals, stored signing secrets)."""
    return (
        _rows("SELECT COUNT(*) FROM accounts")[0][0],
        _rows("SELECT COUNT(*) FROM actor_refs WHERE kind = 'system'")[0][0],
        _rows("SELECT COUNT(*) FROM instance_secrets WHERE name = 'jwt_secret'")[0][0],
    )


@pytest.fixture
def no_engine(monkeypatch):
    """The commands build an AgentEngine they do not need for this."""
    import nerve.agent.engine as engine_mod

    monkeypatch.setattr(engine_mod, "AgentEngine", MagicMock(return_value=AsyncMock()))


class TestTheFirstCommandAfterAnUpgradeLoadsOnlyTheSecret:
    @_OPENERS
    def test_it_stores_a_signing_secret_and_creates_no_account(
        self, tmp_path, no_engine, argv,
    ):
        from nerve.cli import main

        config_dir = _config_dir(tmp_path)
        _migrated_but_unbootstrapped_db()
        assert _identity_state() == (0, 1, 0)

        result = CliRunner().invoke(main, ["-c", str(config_dir), *argv])
        assert result.exit_code == 0, result.output
        assert _identity_state() == (0, 1, 1)

        # A second run finds the secret and adds nothing.
        result = CliRunner().invoke(main, ["-c", str(config_dir), *argv])
        assert result.exit_code == 0, result.output
        assert _identity_state() == (0, 1, 1)

    def test_a_configured_secret_is_honoured_by_the_opener_too(self, tmp_path, no_engine):
        """With ``auth.jwt_secret`` configured, no secret is stored in the
        database."""
        from nerve.cli import main

        config_dir = _config_dir(tmp_path)
        (config_dir / "config.local.yaml").write_text(
            "auth:\n  jwt_secret: configured-secret-padded-to-thirty-two-bytes\n",
            encoding="utf-8",
        )
        _migrated_but_unbootstrapped_db()
        result = CliRunner().invoke(main, ["-c", str(config_dir), "cron"])
        assert result.exit_code == 0, result.output
        assert _identity_state() == (0, 1, 0)


class TestTheCliRefusesInsecureState:
    """``Database.connect`` refuses; the CLI prints the message, not a
    traceback."""

    @_OPENERS
    def test_a_writable_database_is_refused_unrepaired_and_unbootstrapped(
        self, tmp_path, no_engine, argv,
    ):
        from nerve.cli import main

        config_dir = _config_dir(tmp_path)
        db_path = _migrated_but_unbootstrapped_db()
        os.chmod(db_path, 0o666)

        result = CliRunner().invoke(main, ["-c", str(config_dir), *argv])
        assert result.exit_code != 0
        assert "Refusing to open" in result.output, result.output
        assert f"chmod 600 {db_path}" in result.output
        assert result.exception is None or isinstance(result.exception, SystemExit)
        # No repair, no migration, no rows.
        assert stat.S_IMODE(os.stat(db_path).st_mode) == 0o666
        assert _identity_state() == (0, 1, 0)

    def test_a_writable_state_directory_is_refused(self, tmp_path, no_engine):
        from nerve.cli import main

        config_dir = _config_dir(tmp_path)
        db_path = _migrated_but_unbootstrapped_db()
        os.chmod(db_path.parent, 0o777)
        try:
            result = CliRunner().invoke(main, ["-c", str(config_dir), "sync"])
            assert result.exit_code != 0
            assert "Refusing to open" in result.output and f"chmod 700 {db_path.parent}" in result.output
            assert stat.S_IMODE(os.stat(db_path.parent).st_mode) == 0o777
        finally:
            os.chmod(db_path.parent, 0o700)


_AHEAD = pytest.mark.parametrize(
    "table, track", [("schema_version", "upstream"), ("fork_schema_version", "fork")],
)


def _ahead_of_the_code(table: str) -> int:
    """Record a version on ``table`` that is above the one this code ships, as a
    newer Nerve (or a fork) would. Returns that version."""
    from nerve.db import FORK_SCHEMA_VERSION, SCHEMA_VERSION

    version = (SCHEMA_VERSION if table == "schema_version" else FORK_SCHEMA_VERSION) + 1
    conn = sqlite3.connect(str(paths.db_path()))
    try:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (version INTEGER PRIMARY KEY)")
        conn.execute(f"INSERT INTO {table} (version) VALUES (?)", (version,))
        conn.commit()
    finally:
        conn.close()
    return version


def test_the_refusal_status_is_65():
    """A supervisor keys on this number (systemd: RestartPreventExitStatus=65),
    so it must not change with the constant."""
    assert runner.NEWER_SCHEMA_EXIT_CODE == 65


def _assert_one_line_refusal(result, track: str, version: int) -> None:
    assert result.exit_code == 65, result.output
    assert "Traceback" not in result.output
    refusal = [line for line in result.output.splitlines() if "schema version" in line]
    assert len(refusal) == 1, result.output
    assert refusal[0].startswith(f"Error: the database is at {track} schema version {version},")
    assert f"{runner.ALLOW_NEWER_SCHEMA_ENV}=1" in refusal[0]


class TestTheCliRefusesANewerSchema:
    """A database migrated by a newer Nerve, on either track: every command
    that opens it, the gateway's start included, prints one line instead of a
    traceback and exits with ``NEWER_SCHEMA_EXIT_CODE``."""

    @_AHEAD
    @pytest.mark.parametrize("argv", [
        ["sync"], ["cron"], ["db", "prune", "--dry-run"], ["db", "vacuum"],
        ["workflow", "list"], ["workflow", "status", "wfr-0"],
    ], ids=["sync", "cron", "db-prune", "db-vacuum", "workflow-list", "workflow-status"])
    def test_a_command_that_opens_the_database(
        self, tmp_path, no_engine, argv, table, track,
    ):
        from nerve.cli import main

        config_dir = _config_dir(tmp_path)
        _migrated_but_unbootstrapped_db()
        version = _ahead_of_the_code(table)

        result = CliRunner().invoke(main, ["-c", str(config_dir), *argv])
        _assert_one_line_refusal(result, track, version)
        # Refused before anything was written.
        assert _identity_state() == (0, 1, 0)

    @_AHEAD
    def test_the_gateway_refuses_before_uvicorn_starts(
        self, tmp_path, monkeypatch, table, track,
    ):
        import uvicorn

        from nerve import cli

        config_dir = _config_dir(tmp_path)
        _migrated_but_unbootstrapped_db()
        version = _ahead_of_the_code(table)
        monkeypatch.setattr(cli, "_get_daemon_status", lambda: (False, None))
        monkeypatch.setattr(uvicorn, "run", lambda *a, **k: pytest.fail("uvicorn started"))

        result = CliRunner().invoke(cli.main, ["-c", str(config_dir), "start", "-f"])
        _assert_one_line_refusal(result, track, version)
        assert _identity_state() == (0, 1, 0)
        assert not paths.pid_file().exists()

    def test_the_gateway_leaves_state_others_can_write_to_the_real_open(
        self, tmp_path, monkeypatch,
    ):
        """The check before uvicorn reads nothing in a state directory that
        other users can write to, so a version forged there cannot give the
        status that stops a supervisor. The real open refuses the storage, as
        it does for every other command."""
        import uvicorn

        from nerve import cli
        from nerve.db import init_db
        from nerve.db.base import InsecureStateStorage
        from nerve.gateway import server

        config_dir = _config_dir(tmp_path)
        db_path = _migrated_but_unbootstrapped_db()
        _ahead_of_the_code("fork_schema_version")
        monkeypatch.setattr(cli, "_get_daemon_status", lambda: (False, None))
        monkeypatch.setattr(server, "create_app", lambda: None)
        # The lifespan opens the database before it starts anything else.
        monkeypatch.setattr(
            uvicorn, "run", lambda *a, **k: asyncio.run(init_db(paths.db_path())),
        )
        os.chmod(db_path.parent, 0o777)
        try:
            result = CliRunner().invoke(cli.main, ["-c", str(config_dir), "start", "-f"])
        finally:
            os.chmod(db_path.parent, 0o700)
        assert result.exit_code != runner.NEWER_SCHEMA_EXIT_CODE, result.output
        assert isinstance(result.exception, InsecureStateStorage), result.output
        assert "schema version" not in result.output

    def test_a_command_that_words_the_refusal_itself_keeps_the_status(self, monkeypatch):
        """``nerve init`` puts the refusal in its own message. Any other failure
        keeps its status, and reporting it never imports the database package."""
        import sys

        import click

        from nerve.cli import _NerveGroup

        group = _NerveGroup()

        @group.command()
        def init():
            try:
                raise runner.SchemaNewerThanCodeError(9, 8, "fork")
            except runner.SchemaNewerThanCodeError as e:
                raise click.ClickException(f"no owner account: {e}") from e

        @group.command()
        def other():
            raise click.ClickException("something else")

        result = CliRunner().invoke(group, ["init"])
        assert result.exit_code == runner.NEWER_SCHEMA_EXIT_CODE
        assert result.output.startswith(
            "Error: no owner account: the database is at fork schema version 9,"
        )
        assert CliRunner().invoke(group, ["other"]).exit_code == 1

        monkeypatch.delitem(sys.modules, runner.__name__)
        assert CliRunner().invoke(group, ["other"]).exit_code == 1
        assert runner.__name__ not in sys.modules


def test_the_gateway_and_the_cli_share_one_opener():
    """Only ``db/__init__.py``, ``db/base.py`` and ``migrate.py`` construct a
    Database, so no CLI command can skip connect()."""
    import re

    root = Path(__file__).resolve().parents[1] / "nerve"
    constructors = []
    for py in root.rglob("*.py"):
        text = py.read_text(encoding="utf-8")
        for m in re.finditer(r"\bDatabase\(", text):
            line = text[: m.start()].count("\n") + 1
            constructors.append((py.relative_to(root).as_posix(), line))
    files = {f for f, _ in constructors}
    assert "cli.py" not in files, constructors
    assert files <= {"db/__init__.py", "migrate.py", "db/base.py"}, constructors
