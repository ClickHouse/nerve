"""``nerve init`` names the owner it creates.

The wizard's "Your name" answer exists only while the installer runs —
nothing it writes carries it, and the checkpoint it keeps for resuming is
deleted on completion — so the installer creates the local owner account
in-process, before the answer is thrown away. The gateway's own bootstrap at
first start then finds that account rather than creating an unnamed one.
"""

from __future__ import annotations

import os
import sqlite3
import stat
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


def _stub_wizard(ws: Path, *, name: str, local_yaml: str, checkpoint_ok: bool = True):
    """Stands in for SetupWizard: writes what the real one would and answers
    the identity step with ``name``. ``checkpoints`` counts how often the
    installer asked it to save the answers back; ``checkpoint_ok`` is what that
    save reports (True = written, False = could not be saved)."""

    class StubWizard:
        checkpoints = 0

        def __init__(self, cfg_dir, inside_docker=False):
            self.config_dir = cfg_dir

        def run(self):
            from nerve.bootstrap import SetupChoices

            (self.config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
            (self.config_dir / "config.local.yaml").write_text(local_yaml, encoding="utf-8")
            choices = SetupChoices()
            choices.user_name = name
            return choices

        def checkpoint(self) -> bool:
            type(self).checkpoints += 1
            return checkpoint_ok

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


class TestInstallerBootstrapFailure:
    """The wizard clears its checkpoint when it applies the configuration, and
    the name it collected exists nowhere else. If creating the owner then
    fails, the installer must not shrug: it saves the answers back and exits
    non-zero, so a re-run resumes with the same name instead of the gateway
    quietly creating an unnamed owner at first start."""

    def test_a_failed_bootstrap_keeps_the_answers_and_exits_non_zero(
        self, tmp_path, monkeypatch,
    ):
        import nerve.migrate as migrate_mod
        from nerve.cli import main

        config_dir, ws = _install_dirs(tmp_path)
        stub = _stub_wizard(ws, name="alice", local_yaml=f"auth:\n  jwt_secret: {_SECRET}\n")
        monkeypatch.setattr("nerve.bootstrap.SetupWizard", stub)

        def boom(config, *, display_name=None):
            raise RuntimeError("database is locked")

        monkeypatch.setattr(migrate_mod, "bootstrap_identity_sync", boom)

        result = CliRunner().invoke(main, ["-c", str(config_dir), "init"])

        assert result.exit_code != 0
        assert "could not be created" in result.output
        assert "database is locked" in result.output
        assert "answers" in result.output and "saved" in result.output
        assert stub.checkpoints == 1
        # The configuration the wizard wrote stays; only the account is missing.
        assert (config_dir / "config.local.yaml").exists()
        assert not paths.db_path().exists() or _db_rows(
            paths.db_path(), "SELECT COUNT(*) FROM accounts",
        ) == [(0,)]

    def test_a_failed_checkpoint_tells_the_truth_instead_of_promising_a_save(
        self, tmp_path, monkeypatch,
    ):
        """F15: if the answers could not be saved either, the installer must not
        claim they were — it says so and names the collected name."""
        import nerve.migrate as migrate_mod
        from nerve.cli import main

        config_dir, ws = _install_dirs(tmp_path)
        stub = _stub_wizard(
            ws, name="alice", local_yaml=f"auth:\n  jwt_secret: {_SECRET}\n",
            checkpoint_ok=False,
        )
        monkeypatch.setattr("nerve.bootstrap.SetupWizard", stub)

        def boom(config, *, display_name=None):
            raise RuntimeError("database is locked")

        monkeypatch.setattr(migrate_mod, "bootstrap_identity_sync", boom)

        result = CliRunner().invoke(main, ["-c", str(config_dir), "init"])
        assert result.exit_code != 0
        assert "could not be created" in result.output
        assert "could not be saved" in result.output
        assert "alice" in result.output
        assert stub.checkpoints == 1

    def test_the_real_checkpoint_keeps_the_name_and_reports_success(self, tmp_path):
        from nerve.bootstrap import SetupWizard, _init_state_file, _load_init_state

        wizard = SetupWizard(tmp_path)
        wizard.choices.user_name = "alice"
        wizard._completed_steps = {"mode", "identity"}
        assert wizard.checkpoint() is True

        state = _load_init_state()
        assert state["choices"]["user_name"] == "alice"
        assert set(state["completed"]) == {"mode", "identity"}
        # It holds API keys: owner-only, and no temporary left beside it.
        path = _init_state_file()
        assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
        assert not path.with_name(path.name + ".tmp").exists()

    def test_the_real_checkpoint_reports_failure_when_the_mode_is_not_honoured(
        self, tmp_path, monkeypatch,
    ):
        """F22: the checkpoint is created 0600 and the mode is read back through
        the descriptor before a byte is written. A filesystem that accepts the
        mode and ignores it — the no-op chmod case — yields no checkpoint
        rather than a readable one, and the wizard is told so."""
        import nerve.bootstrap as bootstrap_mod
        from nerve.bootstrap import SetupWizard, _init_state_file, _load_init_state

        wizard = SetupWizard(tmp_path)
        wizard.choices.user_name = "alice"
        monkeypatch.setattr(bootstrap_mod, "_private_fd", lambda fd: False)

        assert wizard.checkpoint() is False
        assert _load_init_state() is None
        path = _init_state_file()
        assert not path.exists()
        assert not path.with_name(path.name + ".tmp").exists()  # partial removed

    def test_the_real_checkpoint_reports_failure_when_it_cannot_write(self, tmp_path, monkeypatch):
        import nerve.bootstrap as bootstrap_mod
        from nerve.bootstrap import SetupWizard, _load_init_state

        wizard = SetupWizard(tmp_path)
        wizard.choices.user_name = "alice"
        real_open = os.open

        def refuse_open(path, flags, mode=0o777, *a, **k):
            if str(path).endswith(".tmp"):
                raise PermissionError("read-only state directory")
            return real_open(path, flags, mode, *a, **k)

        monkeypatch.setattr(bootstrap_mod.os, "open", refuse_open)
        assert wizard.checkpoint() is False
        assert _load_init_state() is None

    def test_a_headless_failure_exits_non_zero_without_pretending_to_save_answers(
        self, tmp_path, monkeypatch,
    ):
        import nerve.migrate as migrate_mod
        from nerve.cli import main

        def boom(config, *, display_name=None):
            raise RuntimeError("database is locked")

        monkeypatch.setattr(migrate_mod, "bootstrap_identity_sync", boom)
        env = {
            "ANTHROPIC_API_KEY": "sk-ant-api03-test-key-for-the-headless-path",
            "NERVE_MODE": "personal",
            "NERVE_WORKSPACE": str(tmp_path / "ws"),
        }
        result = CliRunner().invoke(
            main, ["-c", str(tmp_path), "init", "--non-interactive"], env=env,
        )
        assert result.exit_code != 0
        assert "could not be created" in result.output
        assert "--non-interactive" in result.output
        assert "answers" not in result.output
