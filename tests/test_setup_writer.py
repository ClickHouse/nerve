"""The installer's writing path, pinned byte for byte.

PR 6 extracts ``SetupWizard``'s writers into :mod:`nerve.setup_writer` so the
web wizard can call the same code the installer does. The extraction has to be
invisible to ``nerve init``: these tests run the writing path on two fixed
:class:`SetupChoices` and compare **every byte** it produces against goldens
captured from the installer before the move.

The goldens in ``tests/fixtures/setup_writer/`` are therefore evidence, not
convenience. A diff here means the installer's output changed — regenerating
them to make a failure go away is how a fresh install silently stops being
reproducible.

Two shapes, chosen to cover the branches in ``_build_config_layers``:

* ``personal`` — a personal install on a direct Anthropic key, with Telegram,
  sync sources, gmail accounts and two productivity crons;
* ``worker`` — a worker install on Bedrock, behind the local proxy, deployed
  with Docker, with an AWS profile and no Telegram.
"""

from __future__ import annotations

import re
import stat
from pathlib import Path

import pytest
import yaml

from nerve.bootstrap import SetupChoices, SetupWizard
from nerve.setup_writer import (
    write_config_yaml,
    write_cron_jobs,
    write_workspace_settings,
)

FIXTURES = Path(__file__).parent / "fixtures" / "setup_writer"

# Every secret here is a placeholder. They are in the goldens on purpose: the
# point of the comparison is that the wizard puts each value in exactly one
# file, and a test that elided them could not see a key landing in the tracked
# layer.
_ANTHROPIC_KEY = "anthropic-key-placeholder"
_OPENAI_KEY = "openai-key-placeholder"
_TELEGRAM_BOT_TOKEN = "0000000000:telegram-bot-token-placeholder"
_TELEGRAM_API_HASH = "telegram-api-hash-placeholder"
_GITHUB_TOKEN = "github-token-placeholder"
_OAUTH_TOKEN = "claude-oauth-token-placeholder"

# The generated signing secret is different on every run, and so is a bcrypt
# salt. Both are replaced before the comparison — what is being pinned is that
# they are written, to that file, under that key.
_JWT_SECRET = re.compile(r"jwt_secret: [0-9a-f]{64}")
_PASSWORD_HASH = re.compile(r"password_hash: \$2[aby]\$\d{2}\$[./A-Za-z0-9]{53}")

# The files the writing path produces, as ``golden name -> path under the run``.
#
# The goldens carry a ``.golden`` suffix because ``config.yaml`` and
# ``config.local.yaml`` are in the repository's .gitignore — an install's own
# files, named for what they are. Without the suffix the two most important
# fixtures are never committed, and this whole comparison passes for whoever
# generated them and fails for everybody else, which is the opposite of what a
# golden test is for.
_PRODUCED = {
    "config.yaml": "config/config.yaml",
    "config.local.yaml": "config/config.local.yaml",
    "settings.yaml": "workspace/config/settings.yaml",
    "cron-system.yaml": "workspace/config/cron/system.yaml",
    "cron-jobs.yaml": "workspace/config/cron/jobs.yaml",
}
_GOLDEN_SUFFIX = ".golden"


def personal_choices(workspace: Path) -> SetupChoices:
    return SetupChoices(
        deployment="server",
        mode="personal",
        anthropic_api_key=_ANTHROPIC_KEY,
        openai_api_key=_OPENAI_KEY,
        provider_type="anthropic",
        workspace_path=workspace,
        timezone="Europe/Berlin",
        user_name="Alice Example",
        telegram_bot_token=_TELEGRAM_BOT_TOKEN,
        telegram_allowed_users=[111111, 222222],
        enabled_crons=["inbox-processor", "task-planner"],
        github_sync=True,
        gmail_sync=True,
        gmail_accounts=["alice@example.invalid"],
        telegram_sync=True,
        telegram_api_id=1234567,
        telegram_api_hash=_TELEGRAM_API_HASH,
        github_token=_GITHUB_TOKEN,
    )


def worker_choices(workspace: Path) -> SetupChoices:
    return SetupChoices(
        deployment="docker",
        mode="worker",
        use_proxy=True,
        provider_type="bedrock",
        aws_region="eu-central-1",
        aws_profile="nerve-worker",
        workspace_path=workspace,
        timezone="UTC",
        enabled_crons=["skill-reviser"],
        claude_oauth_token=_OAUTH_TOKEN,
        task_description="Keep the build green.",
    )


def run_writers(root: Path, choices: SetupChoices) -> dict[str, str]:
    """Run the installer's writing path and return what it wrote.

    Exactly the four writers ``SetupWizard._apply`` calls, in the order it
    calls them, and nothing else: no workspace scaffold, no backups, no web
    build. The keys are the golden names of :data:`_PRODUCED`.
    """
    config_dir = root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)

    wizard = SetupWizard(config_dir)
    wizard.choices = choices
    wizard._write_config_yaml()
    wizard._write_workspace_settings()
    wizard._write_config_local_yaml()
    wizard._write_cron_jobs()

    produced = {}
    for name, relative in _PRODUCED.items():
        path = root / relative
        assert path.exists(), f"the writers did not produce {relative}"
        text = path.read_text(encoding="utf-8")
        text = _JWT_SECRET.sub("jwt_secret: <generated>", text)
        text = _PASSWORD_HASH.sub("password_hash: <bcrypt>", text)
        # The run's own temporary directory is the one other value that
        # cannot be fixed: config.yaml records the workspace as an absolute
        # path (expanded exactly as the config loader would).
        text = text.replace(str(root), "<root>")
        produced[name] = text
    return produced


@pytest.fixture
def personal(tmp_path: Path) -> dict[str, str]:
    return run_writers(tmp_path, personal_choices(tmp_path / "workspace"))


@pytest.fixture
def worker(tmp_path: Path) -> dict[str, str]:
    return run_writers(tmp_path, worker_choices(tmp_path / "workspace"))


def _assert_matches_golden(produced: dict[str, str], shape: str) -> None:
    for name, text in produced.items():
        golden = FIXTURES / shape / (name + _GOLDEN_SUFFIX)
        assert golden.exists(), (
            f"no golden for {shape}/{name} — it was captured from the installer "
            "before the writers moved; see this module's docstring"
        )
        expected = golden.read_text(encoding="utf-8")
        assert text == expected, (
            f"{shape}/{name} differs from what the installer used to write. "
            "This is the regression the extraction must not cause; do not "
            "regenerate the golden to silence it."
        )


class TestInstallerOutputIsUnchanged:
    """The whole point of the extraction: `nerve init` writes the same bytes."""

    def test_every_golden_is_committed(self) -> None:
        """A fixture the repository ignores is a comparison nobody else runs.

        ``config.yaml`` and ``config.local.yaml`` are gitignored by name — they
        are what a real install calls its own files — so goldens under those
        names exist only on the machine that generated them, and this file
        passes there and fails on a clean checkout. Asserted against git rather
        than against the filesystem, because the filesystem is exactly what
        cannot tell the difference.
        """
        import subprocess

        repo = Path(__file__).parent.parent
        try:
            listed = subprocess.run(
                ["git", "ls-files", "tests/fixtures/setup_writer"],
                cwd=repo, capture_output=True, text=True, check=True,
            )
        except (OSError, subprocess.CalledProcessError) as e:
            # A source export rather than a checkout — the files it has are by
            # definition the tracked ones, which is what this asks about.
            pytest.skip(f"not a git work tree: {e}")
        tracked = listed.stdout.split()
        expected = {
            f"tests/fixtures/setup_writer/{shape}/{name}{_GOLDEN_SUFFIX}"
            for shape in ("personal", "worker")
            for name in _PRODUCED
        }
        assert expected <= set(tracked), sorted(expected - set(tracked))

    def test_personal_install(self, personal: dict[str, str]) -> None:
        _assert_matches_golden(personal, "personal")

    def test_worker_install(self, worker: dict[str, str]) -> None:
        _assert_matches_golden(worker, "worker")

    def test_every_file_the_writers_produce_is_covered(
        self, tmp_path: Path,
    ) -> None:
        """A writer that starts producing a new file must land in the goldens.

        Without this the comparison silently stops covering whatever was added
        — the failure mode of every golden test.
        """
        run_writers(tmp_path, personal_choices(tmp_path / "workspace"))
        written = {
            str(path.relative_to(tmp_path))
            for path in tmp_path.rglob("*")
            if path.is_file()
        }
        assert written == set(_PRODUCED.values())


class TestSecretsLandInExactlyOneLayer:
    """Where each answer goes is the property the layering rests on.

    Byte comparison would catch a move, but not say what broke; this says it.
    """

    def test_no_secret_reaches_the_tracked_layer(self, personal: dict[str, str]) -> None:
        tracked = personal["settings.yaml"] + personal["cron-system.yaml"]
        for secret in (
            _ANTHROPIC_KEY, _OPENAI_KEY, _TELEGRAM_BOT_TOKEN,
            _TELEGRAM_API_HASH, _GITHUB_TOKEN,
        ):
            assert secret not in tracked

    def test_secrets_are_in_the_private_file(self, personal: dict[str, str]) -> None:
        local = yaml.safe_load(personal["config.local.yaml"])
        assert local["anthropic_api_key"] == _ANTHROPIC_KEY
        assert local["openai_api_key"] == _OPENAI_KEY
        assert local["telegram"]["bot_token"] == _TELEGRAM_BOT_TOKEN
        assert local["sync"]["telegram"]["api_hash"] == _TELEGRAM_API_HASH
        assert local["github_token"] == _GITHUB_TOKEN
        assert local["auth"]["jwt_secret"] == "<generated>"

    def test_the_machine_layer_holds_only_this_box(self, personal: dict[str, str]) -> None:
        machine = yaml.safe_load(personal["config.yaml"])
        assert machine["deployment"] == "server"
        assert machine["telegram"] == {"enabled": True}
        assert machine["sync"]["gmail"]["accounts"] == ["alice@example.invalid"]
        # Shared behaviour is not duplicated here: config.yaml shadows the
        # tracked file, so a portable key written to both would make the
        # tracked copy dead weight.
        assert "timezone" not in machine
        assert "agent" not in machine

    def test_the_tracked_layer_holds_shared_behaviour(self, personal: dict[str, str]) -> None:
        portable = yaml.safe_load(personal["settings.yaml"])
        assert portable["timezone"] == "Europe/Berlin"
        assert portable["gateway"] == {"host": "0.0.0.0", "port": 8900}
        assert portable["provider"] == {"type": "anthropic"}
        assert portable["sync"]["github"]["enabled"] is True

    def test_a_bedrock_worker_writes_its_region_and_prefixed_models(
        self, worker: dict[str, str],
    ) -> None:
        portable = yaml.safe_load(worker["settings.yaml"])
        assert portable["provider"] == {"type": "bedrock", "aws_region": "eu-central-1"}
        assert portable["agent"]["model"].startswith("eu.anthropic.")
        machine = yaml.safe_load(worker["config.yaml"])
        assert machine["provider"] == {"aws_profile": "nerve-worker"}
        assert machine["proxy"] == {"enabled": True, "port": 8317}
        assert machine["docker"] == {"extra_mounts": []}

    def test_a_password_is_hashed_into_the_private_file_only(
        self, tmp_path: Path,
    ) -> None:
        """The installer still hashes its own answer; nothing else changes.

        (The *wizard* never writes a config-file password — PR 3 deprecated
        ``auth.password_hash`` — but ``nerve init`` predates that and this
        pins that the extraction did not alter it.)
        """
        choices = personal_choices(tmp_path / "workspace")
        choices.password = "correct-horse-battery-staple"
        produced = run_writers(tmp_path, choices)
        assert "password_hash: <bcrypt>" in produced["config.local.yaml"]
        assert "password_hash" not in produced["settings.yaml"]
        assert "password_hash" not in produced["config.yaml"]


class TestPublicationNeverTruncates:
    """A failed write must leave the previous file, not part of the new one.

    ``settings.yaml`` is tracked, shared through git and read by the restart
    the wizard ends with; ``system.yaml`` is what the scheduler loads. Opening
    either with ``"w"`` truncates it before a single byte of the replacement
    is written, so a dump that raises leaves a working install with whatever
    got through.
    """

    def _install(self, tmp_path: Path) -> SetupChoices:
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        choices = personal_choices(tmp_path / "workspace")
        write_config_yaml(choices, config_dir)
        write_workspace_settings(choices)
        write_cron_jobs(choices)
        return choices

    def test_a_failed_settings_write_leaves_the_original(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        from nerve import setup_writer

        choices = self._install(tmp_path)
        settings = tmp_path / "workspace" / "config" / "settings.yaml"
        before = settings.read_bytes()
        assert b"agent" in before

        def _explode(*_args, **_kwargs):
            raise OSError("no room on the device")

        monkeypatch.setattr(setup_writer, "publish_text", _explode)
        with pytest.raises(OSError):
            setup_writer.merge_settings_paths(
                # Not the value the installer already wrote, or the merge is a
                # no-op and never reaches a writer at all.
                tmp_path / "workspace", {"timezone": "Pacific/Auckland"},
            )
        assert settings.read_bytes() == before
        assert not settings.with_name("settings.yaml.tmp").exists()

    def test_a_failed_dump_leaves_the_original(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """The other half: the failure inside the rendering, before anything
        is published at all."""
        from nerve import setup_writer

        self._install(tmp_path)
        settings = tmp_path / "workspace" / "config" / "settings.yaml"
        before = settings.read_bytes()

        def _explode(*_args, **_kwargs):
            raise ValueError("cannot represent that")

        monkeypatch.setattr(setup_writer.yaml, "safe_dump", _explode)
        with pytest.raises(ValueError):
            setup_writer.merge_settings_paths(
                tmp_path / "workspace", {"timezone": "Pacific/Auckland"},
            )
        assert settings.read_bytes() == before

    def test_a_failed_cron_publication_leaves_the_original(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        from nerve import setup_writer

        self._install(tmp_path)
        system = tmp_path / "workspace" / "config" / "cron" / "system.yaml"
        before = system.read_bytes()

        plan = setup_writer.plan_optional_crons(
            tmp_path / "workspace", {"inbox-processor"},
        )
        assert plan.changes_anything

        def _explode(*_args, **_kwargs):
            raise OSError("no room on the device")

        monkeypatch.setattr(setup_writer, "publish_text", _explode)
        with pytest.raises(OSError):
            plan.publish()
        assert system.read_bytes() == before

    def test_publishing_keeps_the_destination_mode(self, tmp_path: Path) -> None:
        from nerve import setup_writer

        target = tmp_path / "tracked.yaml"
        target.write_text("first: 1\n", encoding="utf-8")
        target.chmod(0o644)
        setup_writer.publish_text(target, "second: 2\n")
        assert target.read_text(encoding="utf-8") == "second: 2\n"
        assert stat.S_IMODE(target.stat().st_mode) == 0o644
