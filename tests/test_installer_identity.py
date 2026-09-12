"""``nerve init`` names the owner it creates.

The wizard's "Your name" answer exists only while the installer runs —
nothing it writes carries it, and the checkpoint it keeps for resuming is
deleted on completion — so the installer creates the local owner account
in-process, before the answer is thrown away. The gateway's own bootstrap at
first start then finds that account rather than creating an unnamed one.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import bcrypt
from click.testing import CliRunner

from nerve import paths
from nerve.config import load_config
from nerve.migrate import migrate

_HASH = bcrypt.hashpw(b"correct horse battery staple", bcrypt.gensalt(rounds=4)).decode()
_SECRET = "configured-secret-padded-to-thirty-two-bytes"


def _db_rows(db_path: Path, sql: str) -> list[tuple]:
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def _stub_wizard(ws: Path, *, name: str, local_yaml: str):
    """Stands in for SetupWizard: writes what the real one would and answers
    the identity step with ``name``."""

    class StubWizard:
        def __init__(self, cfg_dir, inside_docker=False):
            self.config_dir = cfg_dir

        def run(self):
            from nerve.bootstrap import SetupChoices

            (self.config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
            (self.config_dir / "config.local.yaml").write_text(local_yaml, encoding="utf-8")
            choices = SetupChoices()
            choices.user_name = name
            return choices

    return StubWizard


def _install_dirs(tmp_path: Path) -> tuple[Path, Path]:
    config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
    config_dir.mkdir()
    (ws / "config").mkdir(parents=True)
    return config_dir, ws


class TestInstallerDisplayName:
    def test_the_wizards_name_becomes_the_owner_display_name(self, tmp_path, monkeypatch):
        from nerve.cli import main

        config_dir, ws = _install_dirs(tmp_path)
        monkeypatch.setattr(
            "nerve.bootstrap.SetupWizard",
            _stub_wizard(
                ws, name="  alice  ",
                local_yaml=f"auth:\n  password_hash: '{_HASH}'\n  jwt_secret: {_SECRET}\n",
            ),
        )
        result = CliRunner().invoke(main, ["-c", str(config_dir), "init"])
        assert result.exit_code == 0, result.output
        assert "created the local owner account" in result.output

        rows = _db_rows(
            paths.db_path(),
            "SELECT actor.display_name, actor.kind, acc.credential_source, acc.username "
            "FROM accounts acc JOIN actor_refs actor ON actor.id = acc.actor_id",
        )
        assert rows == [("alice", "human", "config", None)]
        # The system principal is not a person and gets no name.
        assert _db_rows(
            paths.db_path(), "SELECT display_name FROM actor_refs WHERE kind = 'system'",
        ) == [(None,)]

    def test_first_start_after_init_keeps_the_named_owner(self, tmp_path, monkeypatch):
        """The gateway's own bootstrap at first start finds the account the
        installer made and does not create a second, unnamed one."""
        from nerve.cli import main

        config_dir, ws = _install_dirs(tmp_path)
        monkeypatch.setattr(
            "nerve.bootstrap.SetupWizard",
            _stub_wizard(ws, name="bob", local_yaml=f"auth:\n  jwt_secret: {_SECRET}\n"),
        )
        assert CliRunner().invoke(main, ["-c", str(config_dir), "init"]).exit_code == 0

        # What `nerve start` and the lifespan do next.
        report = migrate(config_dir, workspace=ws, config=load_config(config_dir))
        assert not report.bootstrapped_account
        assert _db_rows(
            paths.db_path(), "SELECT display_name FROM actor_refs WHERE kind = 'human'",
        ) == [("bob",)]

    def test_an_empty_answer_leaves_the_owner_unnamed(self, tmp_path, monkeypatch):
        from nerve.cli import main

        config_dir, ws = _install_dirs(tmp_path)
        monkeypatch.setattr(
            "nerve.bootstrap.SetupWizard",
            _stub_wizard(ws, name="   ", local_yaml=f"auth:\n  jwt_secret: {_SECRET}\n"),
        )
        assert CliRunner().invoke(main, ["-c", str(config_dir), "init"]).exit_code == 0
        assert _db_rows(
            paths.db_path(), "SELECT display_name FROM actor_refs WHERE kind = 'human'",
        ) == [(None,)]

    def test_headless_install_gets_an_unnamed_owner(self, tmp_path):
        """The headless path reads no name from the environment (there is no
        such variable), so its owner is unnamed until renamed."""
        from nerve.cli import main

        env = {
            "ANTHROPIC_API_KEY": "sk-ant-api03-test-key-for-the-headless-path",
            "NERVE_MODE": "personal",
            "NERVE_WORKSPACE": str(tmp_path / "ws"),
            "NERVE_TIMEZONE": "UTC",
        }
        result = CliRunner().invoke(
            main, ["-c", str(tmp_path), "init", "--non-interactive"], env=env,
        )
        assert result.exit_code == 0, result.output
        assert "created the local owner account" in result.output
        assert _db_rows(
            paths.db_path(), "SELECT display_name FROM actor_refs WHERE kind = 'human'",
        ) == [(None,)]
        # The headless installer always writes a jwt_secret, so none is generated.
        assert "generated a JWT signing secret" not in result.output
