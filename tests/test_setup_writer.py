"""The installer's writing path, pinned byte for byte — and the web writers.

``SetupWizard``'s writers live in :mod:`nerve.setup_writer` so the web wizard
can call the same code the installer does. The extraction has to be invisible
to ``nerve init``: these tests run the writing path on fixed
:class:`SetupChoices` and compare **every byte** it produces against goldens
captured from the installer before the move.

The goldens in ``tests/fixtures/setup_writer/`` are therefore evidence, not
convenience. A diff here means the installer's output changed — regenerating
them to make a failure go away is how a fresh install silently stops being
reproducible.

Shapes:

* ``personal`` — a personal install on a direct Anthropic key, with Telegram,
  sync sources, gmail accounts and two productivity crons;
* ``worker`` — a worker install on Bedrock, behind the local proxy, deployed
  with Docker, with an AWS profile and no Telegram;
* ``unattended-personal`` / ``unattended-worker`` — ``run_non_interactive``
  with a deterministic environment, including the workspace files whose
  placeholders the unattended path leaves untouched;
* ``slim`` / ``slim-docker`` — the host-side half of the default
  ``nerve init`` (no mode, no templates, no cron catalogue).
"""

from __future__ import annotations

import os
import re
import stat
import subprocess
from pathlib import Path
from unittest import mock

import pytest
import yaml

from nerve import setup_writer
from nerve.bootstrap import SetupChoices, SetupWizard, run_non_interactive
from nerve.config import DEFAULT_MODEL, NerveConfig
from nerve.setup_writer import (
    TASK_DESCRIPTION_TOKEN,
    TASK_ONBOARDING_MARKER,
    apply_mode,
    apply_profile_templates,
    choices_from_config,
    describe_cron,
    merge_machine_paths,
    merge_private_paths,
    merge_settings_defaults,
    render_task_md,
    task_description_from,
    task_onboarding_pending,
    workspace_mode,
    write_config_yaml,
    write_cron_jobs,
    write_slim_install,
    write_workspace_settings,
)
from nerve.workspace import initialize_workspace, read_manifest, template_file

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
_SLIM_PRODUCED = {
    "config.yaml": "config/config.yaml",
    "config.local.yaml": "config/config.local.yaml",
    "settings.yaml": "workspace/config/settings.yaml",
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


def _scrub(text: str, root: Path) -> str:
    text = _JWT_SECRET.sub("jwt_secret: <generated>", text)
    text = _PASSWORD_HASH.sub("password_hash: <bcrypt>", text)
    # The run's own temporary directory is the one other value that cannot
    # be fixed: config.yaml records the workspace as an absolute path
    # (expanded exactly as the config loader would).
    return text.replace(str(root), "<root>")


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
        produced[name] = _scrub(path.read_text(encoding="utf-8"), root)
    return produced


def run_unattended(
    root: Path, mode: str, extra: dict[str, str], *, on_path: tuple[str, ...] = (),
) -> dict[str, str]:
    """``run_non_interactive`` under a deterministic environment.

    No CLIs on PATH but those *on_path* names (so by default no source is
    auto-enabled), no web build, a fixed credential and timezone. The extra
    files are the workspace files whose placeholders the unattended path must
    leave exactly as the template has them (only the terminal wizard, given a
    name, fills them in).
    """

    def which(name: str, *args: object, **kwargs: object) -> str | None:
        return f"/usr/bin/{name}" if name in on_path else None

    config_dir = root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith("NERVE_") or key == "NERVE_HOME"
    }
    for key in ("ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN", "GH_TOKEN", "OPENAI_API_KEY"):
        env.pop(key, None)
    env.update({
        "ANTHROPIC_API_KEY": "sk-ant-REPLACE",
        "NERVE_MODE": mode,
        "NERVE_WORKSPACE": str(root / "workspace"),
        "NERVE_TIMEZONE": "Europe/Berlin",
        "NERVE_TASK": "Keep the build green." if mode == "worker" else "",
    })
    with mock.patch.dict(os.environ, env, clear=True), \
            mock.patch("nerve.bootstrap.shutil.which", side_effect=which), \
            mock.patch.object(SetupWizard, "_build_web_ui", lambda self: None):
        run_non_interactive(config_dir)
    produced = {}
    for name, relative in {**_PRODUCED, **extra}.items():
        produced[name] = _scrub((root / relative).read_text(encoding="utf-8"), root)
    return produced


def run_slim(root: Path, choices: SetupChoices, **kwargs) -> dict[str, str]:
    write_slim_install(choices, root / "config", **kwargs)
    produced = {}
    for name, relative in _SLIM_PRODUCED.items():
        text = (root / relative).read_text(encoding="utf-8")
        text = _JWT_SECRET.sub("jwt_secret: REDACTED", text)
        produced[name] = text.replace(str(root), "<root>")
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

    def test_no_golden_is_gitignored(self) -> None:
        """A fixture the repository ignores is a comparison nobody else runs.

        ``config.yaml`` and ``config.local.yaml`` are gitignored by name — they
        are what a real install calls its own files — so goldens under those
        names would exist only on the machine that generated them. Asserted
        against ``git check-ignore`` rather than ``git ls-files`` so the test
        passes in a work tree where the fixtures are staged but not yet
        committed; a source export skips it.
        """
        repo = Path(__file__).parent.parent
        goldens = sorted(FIXTURES.rglob("*" + _GOLDEN_SUFFIX))
        assert goldens, "no goldens at all"
        try:
            result = subprocess.run(
                ["git", "check-ignore", "-q", *[str(g.relative_to(repo)) for g in goldens]],
                cwd=repo, capture_output=True, text=True,
            )
        except OSError as e:
            pytest.skip(f"git is not available: {e}")
        if result.returncode == 128:
            pytest.skip("not a git work tree")
        # 0 = at least one path is ignored; 1 = none is.
        assert result.returncode == 1, "a golden fixture is ignored by .gitignore"

    def test_personal_install(self, personal: dict[str, str]) -> None:
        _assert_matches_golden(personal, "personal")

    def test_worker_install(self, worker: dict[str, str]) -> None:
        _assert_matches_golden(worker, "worker")

    def test_unattended_personal_install(self, tmp_path: Path) -> None:
        produced = run_unattended(tmp_path, "personal", {"USER.md": "workspace/USER.md"})
        _assert_matches_golden(produced, "unattended-personal")
        # The unattended path has no name, so the placeholders stay.
        assert "{{NAME}}" in produced["USER.md"]
        assert "{{TIMEZONE}}" in produced["USER.md"]

    def test_unattended_worker_install(self, tmp_path: Path) -> None:
        produced = run_unattended(tmp_path, "worker", {"TASK.md": "workspace/TASK.md"})
        _assert_matches_golden(produced, "unattended-worker")
        # The worker template with the description in place — the same file
        # the web Mode step and ``nerve init --terminal`` write — still
        # carrying the marker that onboarding_done deletes.
        assert produced["TASK.md"].startswith("# Task\n")
        assert produced["TASK.md"] == render_task_md("Keep the build green.")
        assert produced["TASK.md"].splitlines()[1].startswith(TASK_ONBOARDING_MARKER)
        assert TASK_DESCRIPTION_TOKEN not in produced["TASK.md"]

    def test_slim_install(self, tmp_path: Path) -> None:
        produced = run_slim(tmp_path, SetupChoices(
            deployment="server", workspace_path=tmp_path / "workspace",
            gateway_host="127.0.0.1", gateway_port=8977,
        ))
        _assert_matches_golden(produced, "slim")

    def test_slim_docker_install_carries_the_published_port_in_config_yaml(
        self, tmp_path: Path,
    ) -> None:
        produced = run_slim(tmp_path, SetupChoices(
            deployment="docker", workspace_path=Path("/root/nerve-workspace"),
            gateway_host="0.0.0.0", gateway_port=8977,
        ), host_workspace=tmp_path / "workspace")
        _assert_matches_golden(produced, "slim-docker")
        machine = yaml.safe_load(produced["config.yaml"])
        assert machine["gateway"] == {"host": "0.0.0.0", "port": 8977}
        assert machine["workspace"] == "/root/nerve-workspace"

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

    def test_the_terminal_wizard_fills_the_name_when_given_one(self, tmp_path: Path) -> None:
        """C26: ``nerve init --terminal`` with a name now writes it into USER.md
        (the old code replaced ``{{USER_NAME}}``, a token the template never
        had). The timezone line is filled under the same gate."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        wizard = SetupWizard(config_dir)
        wizard.choices = personal_choices(tmp_path / "workspace")
        with mock.patch.object(SetupWizard, "_build_web_ui", lambda self: None):
            wizard._apply()
        user_md = (tmp_path / "workspace" / "USER.md").read_text(encoding="utf-8")
        assert "- **Name:** Alice Example" in user_md
        assert "- **Timezone:** Europe/Berlin" in user_md
        assert "{{" not in user_md.split("## Who They Are")[0]


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
        choices = personal_choices(tmp_path / "workspace")
        choices.password = "correct-horse-battery-staple"
        produced = run_writers(tmp_path, choices)
        assert "password_hash: <bcrypt>" in produced["config.local.yaml"]
        assert "password_hash" not in produced["settings.yaml"]
        assert "password_hash" not in produced["config.yaml"]

    def test_the_web_only_fields_leave_the_installer_bytes_alone(self, tmp_path: Path) -> None:
        """Every field added for the web steps defaults to "not stated"."""
        machine, portable, _ = setup_writer.build_config_layers(SetupChoices())
        assert "slack" not in machine and "slack" not in portable
        assert portable["agent"]["model"] == DEFAULT_MODEL
        assert portable["quiet_start"] == "02:00"
        assert "slack" not in setup_writer.build_config_local(SetupChoices())

    def test_the_web_fields_land_where_the_table_says(self) -> None:
        choices = SetupChoices(
            agent_model="claude-sonnet-4-6", agent_effort="medium",
            quiet_start="22:00", quiet_end="07:00",
            slack_enabled=True, slack_allow_users=["U0123456789"],
            slack_allow_direct_messages=True, slack_allow_channels=[],
            slack_bot_token="xoxb-REPLACE", slack_app_token="xapp-REPLACE",
            telegram_enabled=False, mode="worker",
        )
        machine, portable, _ = setup_writer.build_config_layers(choices)
        assert portable["agent"]["model"] == "claude-sonnet-4-6"
        assert portable["agent"]["cron_model"] == "claude-sonnet-4-6"  # the pick is the cron model too
        assert portable["agent"]["effort"] == "medium"
        assert (portable["quiet_start"], portable["quiet_end"]) == ("22:00", "07:00")
        assert machine["slack"] == {"enabled": True}
        assert portable["slack"] == {
            "allow_users": ["U0123456789"], "allow_direct_messages": True, "allow_channels": [],
        }
        # A worker can carry the Telegram switch when the web step states it.
        assert machine["telegram"] == {"enabled": False}
        local = setup_writer.build_config_local(choices)
        assert local["slack"] == {"bot_token": "xoxb-REPLACE", "app_token": "xapp-REPLACE"}


class TestPublicationNeverTruncates:
    """A failed write must leave the previous file, not part of the new one."""

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
        self._install(tmp_path)
        settings = tmp_path / "workspace" / "config" / "settings.yaml"
        before = settings.read_bytes()
        assert b"agent" in before

        def _explode(*_args, **_kwargs):
            raise OSError("no room on the device")

        monkeypatch.setattr(setup_writer, "publish_text", _explode)
        with pytest.raises(OSError):
            setup_writer.merge_settings_paths(
                tmp_path / "workspace", {"timezone": "Pacific/Auckland"},
            )
        assert settings.read_bytes() == before
        assert not settings.with_name("settings.yaml.tmp").exists()

    def test_a_failed_dump_leaves_the_original(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
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

    def test_a_failed_config_yaml_write_leaves_the_original(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """The init-path config.yaml is published atomically now too."""
        choices = self._install(tmp_path)
        target = tmp_path / "config" / "config.yaml"
        before = target.read_bytes()
        monkeypatch.setattr(setup_writer.yaml, "safe_dump", mock.Mock(side_effect=ValueError("x")))
        with pytest.raises(ValueError):
            write_config_yaml(choices, tmp_path / "config")
        assert target.read_bytes() == before
        assert not target.with_name("config.yaml.tmp").exists()

    def test_publishing_keeps_the_destination_mode(self, tmp_path: Path) -> None:
        target = tmp_path / "tracked.yaml"
        target.write_text("first: 1\n", encoding="utf-8")
        target.chmod(0o644)
        setup_writer.publish_text(target, "second: 2\n")
        assert target.read_text(encoding="utf-8") == "second: 2\n"
        assert stat.S_IMODE(target.stat().st_mode) == 0o644


class TestMergeWriters:
    def test_merge_private_keeps_mode_0600(self, tmp_path: Path) -> None:
        local = tmp_path / "config.local.yaml"
        merge_private_paths(local, {"anthropic_api_key": "sk-ant-REPLACE"})
        assert stat.S_IMODE(local.stat().st_mode) == 0o600
        merge_private_paths(local, {"openai_api_key": "sk-REPLACE", "anthropic_api_key": None})
        assert stat.S_IMODE(local.stat().st_mode) == 0o600
        data = yaml.safe_load(local.read_text(encoding="utf-8"))
        assert data == {"openai_api_key": "sk-REPLACE"}
        assert local.read_text(encoding="utf-8").startswith("# Nerve — Secrets")

    def test_merge_machine_keeps_mode_and_header(self, tmp_path: Path) -> None:
        machine = tmp_path / "config.yaml"
        write_config_yaml(SetupChoices(workspace_path=tmp_path / "ws"), tmp_path)
        machine.chmod(0o664)
        header = setup_writer.leading_comment(machine.read_text(encoding="utf-8"))
        merge_machine_paths(machine, {"telegram.enabled": True, "sync.gmail.accounts": ["a@example.com"]})
        text = machine.read_text(encoding="utf-8")
        assert text.startswith(header)
        assert stat.S_IMODE(machine.stat().st_mode) == 0o664
        data = yaml.safe_load(text)
        assert data["telegram"] == {"enabled": True}
        assert data["workspace"] == str(tmp_path / "ws")

    def test_merge_settings_defaults_only_fills_missing(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        setup_writer.merge_settings_paths(ws, {"agent.model": "claude-sonnet-4-6", "timezone": "UTC"})
        outcome = merge_settings_defaults(ws, {
            "agent.model": DEFAULT_MODEL, "agent.effort": "max", "quiet_start": "02:00",
        })
        assert outcome.wrote
        assert set(outcome.added) == {"agent.effort", "quiet_start"}
        data = yaml.safe_load((ws / "config" / "settings.yaml").read_text(encoding="utf-8"))
        assert data["agent"] == {"model": "claude-sonnet-4-6", "effort": "max"}
        assert data["timezone"] == "UTC"
        again = merge_settings_defaults(ws, {"agent.model": "x", "agent.effort": "y"})
        assert again.status == "unchanged"


class TestSlimInstall:
    def test_slim_layout_needs_no_migration(self, tmp_path: Path) -> None:
        """The slim layout must not read as a legacy monolith: `nerve start`
        would otherwise rewrite config.yaml into config.yaml.migrated."""
        from nerve.migrate import is_migrated, maybe_migrate

        config_dir = tmp_path / "config"
        ws = tmp_path / "workspace"
        write_slim_install(SetupChoices(
            deployment="server", workspace_path=ws, gateway_host="127.0.0.1", gateway_port=8977,
        ), config_dir)
        assert is_migrated(config_dir, workspace=ws)
        maybe_migrate(config_dir, workspace=ws)
        assert not (config_dir / "config.yaml.migrated").exists()
        assert (config_dir / "config.yaml").exists()

    def test_slim_docker_layout_needs_no_migration_either(self, tmp_path: Path) -> None:
        """`gateway.*` in config.yaml is portable, but the tracked settings
        already carry content, so the migration leaves it alone."""
        from nerve.migrate import is_migrated

        config_dir = tmp_path / "config"
        ws = tmp_path / "workspace"
        write_slim_install(SetupChoices(
            deployment="docker", workspace_path=ws, gateway_host="0.0.0.0", gateway_port=8977,
        ), config_dir, host_workspace=ws)
        assert is_migrated(config_dir, workspace=ws)

    def test_slim_rerun_keeps_jwt_secret_and_hand_edits(self, tmp_path: Path) -> None:
        config_dir = tmp_path / "config"
        ws = tmp_path / "workspace"
        choices = SetupChoices(deployment="server", workspace_path=ws, gateway_port=8977)
        first = write_slim_install(choices, config_dir)
        assert first.jwt_created
        local = config_dir / "config.local.yaml"
        local_before = local.read_bytes()
        # Hand edits in the other two files survive a re-run.
        merge_machine_paths(config_dir / "config.yaml", {"docker.extra_mounts": ["~/code:/code"]})
        setup_writer.merge_settings_paths(ws, {"timezone": "Europe/Berlin"})
        second = write_slim_install(choices, config_dir)
        assert not second.jwt_created
        assert local.read_bytes() == local_before
        assert yaml.safe_load((config_dir / "config.yaml").read_text())["docker"] == {
            "extra_mounts": ["~/code:/code"],
        }
        settings = yaml.safe_load((ws / "config" / "settings.yaml").read_text())
        assert settings["timezone"] == "Europe/Berlin"
        assert settings["gateway"] == {"host": "0.0.0.0", "port": 8977}

    def test_slim_writes_no_mode_templates_and_no_cron_catalogue(self, tmp_path: Path) -> None:
        ws = tmp_path / "workspace"
        write_slim_install(SetupChoices(workspace_path=ws), tmp_path / "config")
        assert not (ws / "SOUL.md").exists()
        assert not (ws / "config" / "cron" / "system.yaml").exists()
        assert (ws / "config" / "cron" / "gates").is_dir()
        assert (ws / "skills").is_dir()
        assert workspace_mode(ws) is None

    def test_docker_harvested_credentials_are_merged_into_an_existing_local_file(
        self, tmp_path: Path,
    ) -> None:
        config_dir = tmp_path / "config"
        write_slim_install(SetupChoices(workspace_path=tmp_path / "ws"), config_dir)
        local = config_dir / "config.local.yaml"
        secret = yaml.safe_load(local.read_text())["auth"]["jwt_secret"]
        write_slim_install(SetupChoices(
            workspace_path=tmp_path / "ws", claude_oauth_token="sk-ant-oat01-REPLACE",
            github_token="ghp_REPLACE",
        ), config_dir)
        data = yaml.safe_load(local.read_text())
        assert data["auth"]["jwt_secret"] == secret
        assert data["claude_oauth_token"] == "sk-ant-oat01-REPLACE"
        assert data["github_token"] == "ghp_REPLACE"
        assert stat.S_IMODE(local.stat().st_mode) == 0o600


class TestApplyMode:
    def test_personal_writes_templates_defaults_and_cron(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        outcome = apply_mode(ws, "personal")
        assert "SOUL.md" in outcome.created and "USER.md" in outcome.created
        assert outcome.cron_written
        assert workspace_mode(ws) == "personal"
        settings = yaml.safe_load((ws / "config" / "settings.yaml").read_text())
        assert settings["telegram"] == {"dm_policy": "pairing", "stream_mode": "partial"}
        assert settings["memory"]["categories"][0]["name"] == "personal_info"
        assert "timezone" not in settings, "the About-you step owns the timezone"
        crons = setup_writer.list_optional_crons(ws)
        assert {c.id for c in crons if c.enabled} == {"inbox-processor", "task-planner"}

    def test_apply_mode_never_overwrites_onboarded_task(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        first = apply_mode(ws, "worker", "Watch the nightly build for example/repo and propose fixes.")
        assert first.task_written
        task = ws / "TASK.md"
        text = task.read_text(encoding="utf-8")
        lines = text.splitlines()
        assert lines[0] == "# Task" and lines[1].startswith(TASK_ONBOARDING_MARKER)
        assert (
            "## Task description (as given)\n"
            "Watch the nightly build for example/repo and propose fixes.\n"
        ) in text
        assert TASK_DESCRIPTION_TOKEN not in text
        # Still pending (the marker stands): a second description replaces it.
        apply_mode(ws, "worker", "Something else entirely, twenty characters.")
        assert "Something else" in task.read_text(encoding="utf-8")
        # Once onboarding deleted the marker, the file is theirs.
        task.write_text(text.replace(lines[1] + "\n", "", 1), encoding="utf-8")
        again = apply_mode(ws, "worker", "A third description that must not land.")
        assert not again.task_written
        assert "third" not in task.read_text(encoding="utf-8")
        # So is a file an older version's onboarding structured (no marker ever).
        task.write_text("# Task\n\n## Mission\nKeep the build green.\n", encoding="utf-8")
        assert not apply_mode(ws, "worker", "A fourth description that must not land.").task_written
        assert "fourth" not in task.read_text(encoding="utf-8")
        assert workspace_mode(ws) == "worker"
        assert not (ws / "USER.md").exists()

    def test_worker_task_md_is_the_template_with_the_description(self, tmp_path: Path) -> None:
        """One ``TASK.md``: the template the MANIFEST lists, token replaced."""
        ws = tmp_path / "ws"
        description = (
            "Watch the nightly build for example/repo.\n\n"
            "Propose fixes before changing anything."
        )
        outcome = apply_mode(ws, "worker", description)
        assert outcome.task_written
        template = template_file("worker", "TASK.md").read_text(encoding="utf-8")
        produced = (ws / "TASK.md").read_text(encoding="utf-8")
        assert produced == template.replace(TASK_DESCRIPTION_TOKEN, description)
        assert produced == render_task_md(description)
        assert task_onboarding_pending(produced)
        assert task_description_from(produced) == description
        assert produced.rstrip().endswith(
            "Read the full guide: https://github.com/ClickHouse/nerve/blob/main/docs/worker-best-practices.md"
        )
        # The writer owns the file: the MANIFEST lists it (``nerve doctor``
        # expects it of a worker), the copier leaves it out, and without a
        # description the template gets an empty description section (never
        # a bare token), which the onboarding conversation fills.
        assert "TASK.md" in [name for name, _ in read_manifest("worker")]
        assert "TASK.md" not in outcome.created
        bare = tmp_path / "bare"
        assert apply_mode(bare, "worker").task_written is True
        written = (bare / "TASK.md").read_text(encoding="utf-8")
        assert TASK_DESCRIPTION_TOKEN not in written
        assert task_onboarding_pending(written) and task_description_from(written) == ""
        # A later call without a description keeps the file as it is.
        assert apply_mode(ws, "worker").task_written is False
        assert task_description_from((ws / "TASK.md").read_text(encoding="utf-8")) == description
        assert "TASK.md" in initialize_workspace(tmp_path / "copied", "worker")
        assert TASK_DESCRIPTION_TOKEN in (tmp_path / "copied" / "TASK.md").read_text(encoding="utf-8")

    def test_task_pending_is_the_marker_or_the_legacy_rule(self) -> None:
        filled = render_task_md("Keep the build green.")
        marker_line = filled.splitlines()[1]
        assert marker_line.startswith(TASK_ONBOARDING_MARKER)
        assert task_onboarding_pending(filled)
        # Sections filled but the marker left standing: the marker decides.
        half_done = filled.replace(
            "_(onboarding fills this in: one paragraph ending in the quality bar)_",
            "Keep CI green on org/repo-a.",
        )
        assert task_onboarding_pending(half_done)
        assert not task_onboarding_pending(filled.replace(marker_line + "\n", "", 1))
        # Files written by older versions: raw until onboarding added ``## Mission``.
        assert task_onboarding_pending("# Task\n\nKeep the build green.\n")
        assert not task_onboarding_pending("# Task\n\n## Mission\nKeep the build green.\n")
        assert not task_onboarding_pending("# Task: CI monitor — ci-bot\n\n## Identity\n…\n## Mission\n…\n")
        assert not task_onboarding_pending("")

    def test_task_description_from_reads_the_section(self) -> None:
        description = "Watch the nightly build.\n\nPropose fixes first."
        filled = render_task_md(description)
        assert task_description_from(filled) == description
        # Legacy raw form: everything after the heading.
        assert task_description_from("# Task\n\nKeep the build green.\n") == "Keep the build green."
        # Pending without a description yet (a bare template copy): empty, not None.
        assert task_description_from(template_file("worker", "TASK.md").read_text(encoding="utf-8")) == ""
        # Onboarded, either way: None — the file is the worker's.
        assert task_description_from(filled.replace(filled.splitlines()[1] + "\n", "", 1)) is None
        assert task_description_from("# Task\n\n## Mission\nKeep the build green.\n") is None

    def test_apply_mode_is_idempotent_and_keeps_edited_settings(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        apply_mode(ws, "personal")
        setup_writer.merge_settings_paths(ws, {"agent.model": "claude-sonnet-4-6"})
        (ws / "SOUL.md").write_text("# SOUL.md - Who You Are\n\nedited\n", encoding="utf-8")
        outcome = apply_mode(ws, "personal")
        assert outcome.created == ()
        assert not outcome.cron_written
        assert (ws / "SOUL.md").read_text(encoding="utf-8").endswith("edited\n")
        settings = yaml.safe_load((ws / "config" / "settings.yaml").read_text())
        assert settings["agent"]["model"] == "claude-sonnet-4-6"

    def test_apply_mode_docker_tools_section(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        apply_mode(ws, "personal", in_container=True)
        tools = (ws / "TOOLS.md").read_text(encoding="utf-8")
        assert tools.count("## Docker Environment") == 1
        apply_mode(ws, "personal", in_container=True)
        assert (ws / "TOOLS.md").read_text(encoding="utf-8").count("## Docker Environment") == 1

    def test_worker_mode_writes_no_quiet_hours_and_leaves_task_planner_off(self, tmp_path: Path) -> None:
        """A worker's defaults: quiet hours off — stated as empty strings, so
        the loader's own 02:00–08:00 cannot come back — and task-planner in
        the catalogue but disabled until the onboarding conversation decides."""
        ws = tmp_path / "ws"
        outcome = apply_mode(ws, "worker", "Watch the nightly build for example/repo and propose fixes.")
        assert {"quiet_start", "quiet_end"} <= set(outcome.settings_defaults_added)
        settings = yaml.safe_load((ws / "config" / "settings.yaml").read_text(encoding="utf-8"))
        assert (settings["quiet_start"], settings["quiet_end"]) == ("", "")
        config = NerveConfig.from_dict({"workspace": str(ws), **settings})
        assert (config.quiet_start, config.quiet_end) == ("", "")
        crons = {c.id: c.enabled for c in setup_writer.list_optional_crons(ws)}
        assert crons == {"task-planner": False, "skill-extractor": True, "skill-reviser": True}

    def test_personal_mode_keeps_the_installer_window(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        apply_mode(ws, "personal")
        settings = yaml.safe_load((ws / "config" / "settings.yaml").read_text(encoding="utf-8"))
        assert (settings["quiet_start"], settings["quiet_end"]) == ("02:00", "08:00")

    def test_apply_mode_under_lockdown(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        with pytest.raises(setup_writer.ModeRefused):
            apply_mode(ws, "worker", "Watch the nightly build and propose fixes.", locked=True)
        assert not (ws / "SOUL.md").exists()
        # Templates arrived from the repository: only TASK.md is written.
        apply_mode(ws, "worker")
        assert task_description_from((ws / "TASK.md").read_text(encoding="utf-8")) == ""
        outcome = apply_mode(ws, "worker", "Watch the nightly build and propose fixes.", locked=True)
        assert outcome.task_written and outcome.created == () and not outcome.cron_written
        # The same template-sourced file as the unlocked path writes.
        assert (ws / "TASK.md").read_text(encoding="utf-8") == render_task_md(
            "Watch the nightly build and propose fixes.",
        )


class TestProfileTemplates:
    def test_apply_profile_templates_is_pristine_guarded(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        apply_mode(ws, "personal")
        changed = apply_profile_templates(
            ws, name="Alice", timezone="Europe/Berlin", language="English",
            quiet=("22:00", "07:00"), agent_name="Nerve", agent_emoji="⚡",
            about="I run a two-person design studio.",
        )
        user = (ws / "USER.md").read_text(encoding="utf-8")
        identity = (ws / "IDENTITY.md").read_text(encoding="utf-8")
        assert "- **Name:** Alice" in user
        assert "- **Timezone:** Europe/Berlin" in user
        assert "- **Language:** English" in user
        assert "- **Quiet hours:** 22:00–07:00 (Europe/Berlin)" in user
        assert "**Work:** I run a two-person design studio." in user
        assert "- **Name:** Nerve" in identity
        assert "- **Emoji:** ⚡" in identity
        assert len(changed) == 7
        # Edited lines are kept: a second person cannot clobber them.
        assert apply_profile_templates(ws, name="Mallory", agent_name="Other") == []
        assert "- **Name:** Alice" in (ws / "USER.md").read_text(encoding="utf-8")
        assert setup_writer.user_md_pristine(ws) == {
            "name": False, "timezone": False, "language": False, "quiet_hours": False, "work": False,
        }

    def test_the_legacy_user_name_token_is_recognised(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        ws.mkdir()
        (ws / "USER.md").write_text("# USER.md\n\n- **Name:** {{USER_NAME}}\n", encoding="utf-8")
        assert setup_writer.user_md_pristine(ws)["name"] is True
        apply_profile_templates(ws, name="Alice")
        assert "- **Name:** Alice" in (ws / "USER.md").read_text(encoding="utf-8")

    def test_missing_files_are_skipped(self, tmp_path: Path) -> None:
        assert apply_profile_templates(tmp_path, name="Alice", agent_name="Nerve") == []


class TestSmallHelpers:
    @pytest.mark.parametrize(("expr", "text"), [
        ("*/30 * * * *", "every 30 minutes"),
        ("0 */4 * * *", "every 4 hours"),
        ("0 5 * * *", "daily at 05:00"),
        ("0 3 * * 0", "Sundays at 03:00"),
        ("15 3 * * 1", "15 3 * * 1"),
        ("garbage", "garbage"),
    ])
    def test_describe_cron(self, expr: str, text: str) -> None:
        assert describe_cron(expr) == text

    def test_workspace_mode_table(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        ws.mkdir()
        assert workspace_mode(ws) is None
        (ws / "SOUL.md").write_text("# SOUL.md — Worker Identity\n", encoding="utf-8")
        assert workspace_mode(ws) == "worker"
        (ws / "SOUL.md").write_text("# SOUL.md - Who You Are\n", encoding="utf-8")
        assert workspace_mode(ws) == "personal"
        # Edited header: the other files break the tie.
        (ws / "SOUL.md").write_text("# My own soul\n", encoding="utf-8")
        assert workspace_mode(ws) == "personal"
        (ws / "TASK.md").write_text("# Task\n", encoding="utf-8")
        assert workspace_mode(ws) == "worker"
        (ws / "USER.md").write_text("x", encoding="utf-8")
        assert workspace_mode(ws) == "personal"

    def test_choices_from_config_round_trips_without_secrets(self, tmp_path: Path) -> None:
        config = NerveConfig.from_dict({
            "workspace": str(tmp_path / "ws"),
            "timezone": "Europe/Berlin",
            "anthropic_api_key": "sk-ant-REPLACE",
            "openai_api_key": "sk-REPLACE",
            "telegram": {"bot_token": "123456:REPLACE", "allowed_users": [42]},
            "sync": {"github": {"enabled": True}, "gmail": {"accounts": ["a@example.com"]}},
            "agent": {"model": "claude-sonnet-4-6", "effort": "medium"},
            "provider": {"type": "bedrock", "aws_region": "eu-central-1"},
            "external_agents": {"targets": [{"name": "codex"}]},
        })
        choices = choices_from_config(config)
        assert choices.timezone == "Europe/Berlin"
        assert choices.github_sync is True and choices.gmail_accounts == ["a@example.com"]
        assert choices.agent_model == "claude-sonnet-4-6" and choices.agent_effort == "medium"
        assert choices.provider_type == "bedrock" and choices.aws_region == "eu-central-1"
        assert choices.telegram_allowed_users == [42]
        assert choices.external_agents == ["codex"]
        for secret_field in ("anthropic_api_key", "openai_api_key", "telegram_bot_token",
                             "telegram_api_hash", "claude_oauth_token", "github_token",
                             "slack_bot_token", "slack_app_token"):
            assert getattr(choices, secret_field) == ""
        assert choices.telegram_api_id == 0
        # And back out through the layers: the derived key follows.
        _machine, portable, _ = setup_writer.build_config_layers(choices)
        assert portable["sync"]["github_events"]["enabled"] is True

    def test_mcp_url(self) -> None:
        config = NerveConfig.from_dict({"gateway": {"host": "0.0.0.0", "port": 8977}})
        assert setup_writer.mcp_url(config) == "http://localhost:8977/mcp/v1/"
        config = NerveConfig.from_dict({"gateway": {"host": "192.0.2.5", "port": 8900}})
        assert setup_writer.mcp_url(config) == "http://192.0.2.5:8900/mcp/v1/"

    def test_merge_external_agents_keeps_the_header(self, tmp_path: Path) -> None:
        write_config_yaml(SetupChoices(workspace_path=tmp_path / "ws"), tmp_path)
        setup_writer.merge_external_agents(tmp_path, targets=["codex"], conflict_policy="skip")
        text = (tmp_path / "config.yaml").read_text(encoding="utf-8")
        assert text.startswith("# Nerve — Machine-local configuration")
        data = yaml.safe_load(text)
        assert data["external_agents"]["targets"] == [{"name": "codex", "enabled": True}]
        assert data["external_agents"]["conflict_policy"] == "skip"
        assert data["mcp_endpoint"] == {"enabled": True, "path": "/mcp/v1"}


class TestFixRoundTwoWriterRules:
    """Writer-level rules added after the second review round."""

    def test_apply_profile_templates_refuses_a_line_break(self, tmp_path: Path) -> None:
        """Defence in depth: each replacement is one line of a prompt file."""
        ws = tmp_path / "ws"
        setup_writer.apply_mode(ws, "personal")
        before = (ws / "USER.md").read_bytes()
        for kwargs in (
            {"name": "Alice\n# Injected"},
            {"agent_name": "Nerve\r\n# Injected"},
            {"about": "line one\nline two"},
            {"quiet": ("22:00\n", "07:00")},
            {"language": "English\r"},
        ):
            with pytest.raises(ValueError):
                setup_writer.apply_profile_templates(ws, **kwargs)
        assert (ws / "USER.md").read_bytes() == before
        assert not (ws / "IDENTITY.md").read_text(encoding="utf-8").count("Injected")

    def test_build_config_local_keeps_a_hash_without_an_id(self) -> None:
        """The web step saves the two sync credentials independently; a hash
        supplied first must not vanish. With an id the block is unchanged."""
        alone = setup_writer.build_config_local(SetupChoices(telegram_api_hash="REPLACE_HASH"))
        assert alone["sync"]["telegram"] == {"api_hash": "REPLACE_HASH"}
        both = setup_writer.build_config_local(SetupChoices(telegram_api_id=12345, telegram_api_hash="REPLACE_HASH"))
        assert both["sync"]["telegram"] == {"api_id": 12345, "api_hash": "REPLACE_HASH"}
        assert "sync" not in setup_writer.build_config_local(SetupChoices())

    def test_optional_cron_names_are_sentence_case(self, tmp_path: Path) -> None:
        """What the API serves next to "Memory maintenance"; the written
        system.yaml carries no names, so the goldens are untouched."""
        ws = tmp_path / "ws"
        setup_writer.apply_mode(ws, "personal")
        names = {t.id: t.name for t in setup_writer.list_optional_crons(ws)}
        assert names == {
            "inbox-processor": "Inbox processor",
            "task-planner": "Task planner",
            "skill-extractor": "Skill extractor",
            "skill-reviser": "Skill reviser",
        }
        assert "name" not in yaml.safe_load(setup_writer.system_cron_file(ws).read_text(encoding="utf-8"))["jobs"][0]


class TestModeDefaults:
    """Quiet hours are optional (a worker's are off) and a worker's
    task-planner is off until its onboarding conversation enables it."""

    @staticmethod
    def _quiet(**fields) -> tuple[str, str]:
        _machine, portable, _ = setup_writer.build_config_layers(SetupChoices(**fields))
        return portable["quiet_start"], portable["quiet_end"]

    def test_each_mode_writes_its_own_quiet_hours(self) -> None:
        assert self._quiet(mode="personal") == ("02:00", "08:00")
        assert self._quiet(mode="worker") == ("", "")
        assert setup_writer.DEFAULT_QUIET_HOURS == {"personal": ("02:00", "08:00"), "worker": ("", "")}

    @pytest.mark.parametrize("mode", ["personal", "worker"])
    def test_the_switch_beats_the_mode_default(self, mode: str) -> None:
        # Off: none, whatever times the choices still carry.
        assert self._quiet(mode=mode, quiet_enabled=False) == ("", "")
        assert self._quiet(mode=mode, quiet_enabled=False, quiet_start="22:00", quiet_end="07:00") == ("", "")
        # On: the times given; a missing one from the default window.
        assert self._quiet(mode=mode, quiet_enabled=True) == ("02:00", "08:00")
        assert self._quiet(mode=mode, quiet_enabled=True, quiet_start="22:00", quiet_end="07:00") == ("22:00", "07:00")
        assert self._quiet(mode=mode, quiet_enabled=True, quiet_start="22:00") == ("22:00", "08:00")
        # Not stated but times given: on with them, as a times-only answer always was.
        assert self._quiet(mode=mode, quiet_start="22:00", quiet_end="07:00") == ("22:00", "07:00")

    def test_choices_from_config_state_the_switch(self, tmp_path: Path) -> None:
        """An install that switched quiet hours off stays off when its choices
        are written again — the mode's default does not reapply."""
        off = choices_from_config(NerveConfig.from_dict({
            "workspace": str(tmp_path / "ws"), "quiet_start": "", "quiet_end": "",
        }))
        assert off.quiet_enabled is False
        assert setup_writer.quiet_hours(off) == ("", "")
        on = choices_from_config(NerveConfig.from_dict({
            "workspace": str(tmp_path / "ws"), "quiet_start": "22:00", "quiet_end": "07:00",
        }))
        assert on.quiet_enabled is True
        assert setup_writer.quiet_hours(on) == ("22:00", "07:00")
        # A key missing from every layer is the loader's default window.
        assert choices_from_config(NerveConfig.from_dict({"workspace": str(tmp_path / "ws")})).quiet_enabled is True

    def test_user_md_records_no_quiet_hours(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        apply_mode(ws, "personal")
        assert apply_profile_templates(ws, quiet=("22:00", "")) == [], "half a pair is skipped"
        assert apply_profile_templates(ws, quiet=("", "")) == ["- **Quiet hours:** none"]
        assert "- **Quiet hours:** none" in (ws / "USER.md").read_text(encoding="utf-8")
        # Pristine-guarded like every other line: a later window does not replace it.
        assert apply_profile_templates(ws, quiet=("22:00", "07:00")) == []

    def test_worker_default_crons_leave_task_planner_available_but_off(self) -> None:
        assert setup_writer.DEFAULT_CRONS["worker"] == ["skill-reviser", "skill-extractor"]
        assert setup_writer.DEFAULT_CRONS["personal"] == ["inbox-processor", "task-planner"]
        assert setup_writer.optional_cron_ids_for("worker") == ("task-planner", "skill-extractor", "skill-reviser")
        jobs = setup_writer.build_cron_jobs(SetupChoices(
            mode="worker", enabled_crons=list(setup_writer.DEFAULT_CRONS["worker"]),
        ))
        assert {job["id"]: job["enabled"] for job in jobs} == {
            "memory-maintenance": True, "task-planner": False,
            "skill-extractor": True, "skill-reviser": True,
        }


class TestHostedGitHubKeepsTheEventsSourceOff:
    """The events source reads with gh on this machine, which a GitHub that
    the channel gateway polls does not set up. Wherever the writers derive it
    from the GitHub switch, it stays off while GitHub is hosted."""

    @pytest.mark.parametrize(("mode", "events"), [(None, True), ("poll", True), ("hosted", False)])
    def test_the_layers_follow_the_switch_only_while_github_is_polled_here(self, mode, events) -> None:
        _machine, portable, _shadowed = setup_writer.build_config_layers(
            SetupChoices(mode="personal", github_sync=True, github_mode=mode),
        )
        assert portable["sync"]["github"] == {"enabled": True}
        assert portable["sync"]["github_events"] == {"enabled": events}

    @pytest.mark.parametrize(("settings", "events"), [
        ("", True),
        ("sync:\n  github:\n    mode: poll\n", True),
        ("sync:\n  github:\n    mode: hosted\n", False),
        ("sync:\n  github:\n    mode: ' Hosted '\n", False),
        ("sync:\n  github:\n    mode: elsewhere\n", True),
    ])
    def test_the_installer_takes_the_mode_from_the_settings_it_writes(
        self, tmp_path: Path, settings: str, events: bool,
    ) -> None:
        workspace = tmp_path / "workspace"
        settings_file = workspace / "config" / "settings.yaml"
        settings_file.parent.mkdir(parents=True)
        settings_file.write_text(settings, encoding="utf-8")

        run_writers(tmp_path, personal_choices(workspace))

        sync = yaml.safe_load(settings_file.read_text(encoding="utf-8"))["sync"]
        assert sync["github"]["enabled"] is True
        assert sync["github_events"] == {"enabled": events}

    def test_unattended_init_with_gh_on_the_path_keeps_it_off_over_hosted_settings(
        self, tmp_path: Path,
    ) -> None:
        """The unattended installer switches GitHub on whenever gh is on the
        PATH; over settings that say ``hosted`` the events source stays off."""
        settings_file = tmp_path / "workspace" / "config" / "settings.yaml"
        settings_file.parent.mkdir(parents=True)
        settings_file.write_text("sync:\n  github:\n    mode: hosted\n", encoding="utf-8")

        produced = run_unattended(tmp_path, "personal", {}, on_path=("gh",))

        sync = yaml.safe_load(produced["settings.yaml"])["sync"]
        assert sync["github"] == {"mode": "hosted", "enabled": True}
        assert sync["github_events"] == {"enabled": False}

    def test_the_web_steps_carry_the_running_mode(self, tmp_path: Path) -> None:
        hosted = choices_from_config(NerveConfig.from_dict({
            "workspace": str(tmp_path / "ws"), "sync": {"github": {"enabled": True, "mode": "hosted"}},
        }))
        assert hosted.github_mode == "hosted"
        _machine, portable, _shadowed = setup_writer.build_config_layers(hosted)
        assert portable["sync"]["github_events"] == {"enabled": False}
        polled = choices_from_config(NerveConfig.from_dict({"workspace": str(tmp_path / "ws")}))
        assert polled.github_mode == "poll"
