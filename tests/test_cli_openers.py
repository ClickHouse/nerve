"""Every CLI command that opens the state database opens it the production way.

``nerve.migrate.open_production_db`` is ``Database.connect`` (state-file
policy, migrations) followed by the configuration-aware identity bootstrap —
exactly what the gateway does at startup. So a maintenance command that happens
to be the first thing run after an upgrade leaves the same state ``nerve
start`` would: one owner account, one system principal, a signing secret. And
because the policy lives in ``connect()``, the same commands refuse insecure
state the same way, with the operator remedy instead of a traceback.
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
    """What an upgrade leaves before anything bootstraps: the schema at its
    head, no identity rows, no signing secret."""

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


class TestTheFirstCommandAfterAnUpgradeBootstraps:
    @_OPENERS
    def test_it_leaves_one_account_and_one_system_principal(self, tmp_path, no_engine, argv):
        from nerve.cli import main

        config_dir = _config_dir(tmp_path)
        _migrated_but_unbootstrapped_db()
        assert _identity_state() == (0, 0, 0)

        result = CliRunner().invoke(main, ["-c", str(config_dir), *argv])
        assert result.exit_code == 0, result.output
        assert _identity_state() == (1, 1, 1)

        # Running again — or `nerve start` next — finds the rows and adds none.
        result = CliRunner().invoke(main, ["-c", str(config_dir), *argv])
        assert result.exit_code == 0, result.output
        assert _identity_state() == (1, 1, 1)

    def test_a_configured_secret_is_honoured_by_the_opener_too(self, tmp_path, no_engine):
        """The same bootstrap as the gateway's: with ``auth.jwt_secret``
        configured, nothing secret is stored in the database."""
        from nerve.cli import main

        config_dir = _config_dir(tmp_path)
        (config_dir / "config.local.yaml").write_text(
            "auth:\n  jwt_secret: configured-secret-padded-to-thirty-two-bytes\n",
            encoding="utf-8",
        )
        _migrated_but_unbootstrapped_db()
        result = CliRunner().invoke(main, ["-c", str(config_dir), "cron"])
        assert result.exit_code == 0, result.output
        assert _identity_state() == (1, 1, 0)


class TestTheCliRefusesInsecureState:
    """The refusal is ``Database.connect``'s, so it needs no per-command code;
    what the CLI adds is the message instead of a traceback."""

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
        # Evidence untouched: no repair, no migration side effects, no rows.
        assert stat.S_IMODE(os.stat(db_path).st_mode) == 0o666
        assert _identity_state() == (0, 0, 0)

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


def test_the_gateway_and_the_cli_share_one_opener():
    """No opener in production code bypasses connect()+bootstrap: the CLI
    maintenance commands import the opener, and nothing but the opener, the
    gateway lifespan and the migrate bootstrap constructs a Database."""
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
