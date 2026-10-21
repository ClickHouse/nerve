"""``setup.local_secrets``: the setting, its environment anchor, the doctor line.

The checklist's and the routes' behaviour under the policy live with their
own suites (``test_setup_checklist.py``, ``test_setup_api.py``); this module
pins how the flag is read and reported.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from nerve import cli
from nerve.config import LOCAL_SECRETS_ENV, ConfigError, NerveConfig, load_config
from nerve.setup_writer import SetupChoices, merge_settings_paths, write_slim_install


def _install(tmp_path: Path, *, settings: str = "", base: str = "") -> Path:
    """A config directory and a workspace whose tracked settings say ``settings``."""
    config_dir = tmp_path / "cfg"
    workspace = tmp_path / "ws"
    config_dir.mkdir(parents=True)
    (workspace / "config").mkdir(parents=True)
    (config_dir / "config.yaml").write_text(f"workspace: {workspace}\n" + base, encoding="utf-8")
    (workspace / "config" / "settings.yaml").write_text(settings, encoding="utf-8")
    return config_dir


@pytest.fixture(autouse=True)
def _no_ambient_anchor(monkeypatch):
    monkeypatch.delenv(LOCAL_SECRETS_ENV, raising=False)
    # A GitHub token in the developer's shell would read as the operator's.
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)


class TestTheSetting:
    def test_defaults_to_keeping_secrets(self):
        assert NerveConfig().setup.local_secrets is True
        assert NerveConfig.from_dict({}).setup.local_secrets is True

    def test_read_from_the_tracked_settings(self, tmp_path):
        config_dir = _install(tmp_path, settings="setup:\n  local_secrets: false\n")
        assert load_config(config_dir).setup.local_secrets is False

    @pytest.mark.parametrize("spelling", ["false", "no", "0", "off", "False", " NO "])
    def test_string_spellings_are_read_as_lockdown_reads_them(self, spelling):
        """An env reference interpolates to a string; ``bool("false")`` must not win."""
        assert NerveConfig.from_dict({"setup": {"local_secrets": spelling}}).setup.local_secrets is False

    @pytest.mark.parametrize("spelling", ["true", "yes", "1", "on", "TRUE"])
    def test_truthy_spellings_keep_the_permission(self, spelling):
        assert NerveConfig.from_dict({"setup": {"local_secrets": spelling}}).setup.local_secrets is True

    @pytest.mark.parametrize("value", ["flase", "ture", "maybe", "off,", "", "  ", "${KEEP_SECRETS}", 0, 1, 2, [], {}])
    def test_an_unreadable_value_is_refused_not_read_as_allowed(self, value):
        """The lenient coercion every other flag gets would fall back to the
        default — ``True``, the position in which the page stores every key it
        is handed — with one log line as the only signal. Refused instead."""
        with pytest.raises(ConfigError) as ei:
            NerveConfig.from_dict({"setup": {"local_secrets": value}})
        assert "setup.local_secrets must be true or false" in str(ei.value)

    def test_a_bare_key_reads_as_off_not_as_allowed(self):
        """``local_secrets:`` with no value is "off", as a bare key is for every
        boolean here — never "use the default", which would be the permissive
        position for this one."""
        assert NerveConfig.from_dict({"setup": {"local_secrets": None}}).setup.local_secrets is False

    def test_a_bare_section_is_the_default(self):
        assert NerveConfig.from_dict({"setup": None}).setup.local_secrets is True
        assert NerveConfig.from_dict({"setup": {}}).setup.local_secrets is True


class TestTheTrackedFileValue:
    """What ``settings.yaml`` says is read strictly, the way ``lockdown`` is:
    a typo in the one key that decides whether the page stores credentials
    stops the load rather than quietly reading as "allowed"."""

    @pytest.mark.parametrize("text", ["flase", "ture", "maybe", '"maybe"', "off,", '""', "' '"])
    def test_an_unreadable_value_is_refused(self, tmp_path, text):
        config_dir = _install(tmp_path, settings=f"setup:\n  local_secrets: {text}\n")
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "setup.local_secrets must be true or false" in str(ei.value)

    @pytest.mark.parametrize("text", ["", "null", "~"])
    def test_a_bare_key_or_null_reads_as_off(self, tmp_path, text):
        """Fails closed without stopping the load: the page stores nothing."""
        config_dir = _install(tmp_path, settings=f"setup:\n  local_secrets: {text}\n")
        assert load_config(config_dir).setup.local_secrets is False

    @pytest.mark.parametrize(
        "text, expected",
        [
            ("true", True), ("false", False), ("yes", True), ("no", False), ("off", False),
            ('"false"', False), ("'no'", False), ('"0"', False), ('"1"', True), ('"on"', True),
        ],
    )
    def test_a_readable_value_still_works(self, tmp_path, text, expected):
        """Real YAML booleans and the quoted spellings an env reference leaves behind."""
        config_dir = _install(tmp_path, settings=f"setup:\n  local_secrets: {text}\n")
        assert load_config(config_dir).setup.local_secrets is expected

    def test_an_unset_reference_is_refused_by_name(self, tmp_path, monkeypatch):
        monkeypatch.delenv("KEEP_SECRETS", raising=False)
        config_dir = _install(tmp_path, settings="setup:\n  local_secrets: ${KEEP_SECRETS}\n")
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "KEEP_SECRETS" in str(ei.value)

    @pytest.mark.parametrize("value, expected", [("no", False), ("0", False), ("1", True), ("true", True)])
    def test_a_reference_is_judged_by_what_it_resolves_to(self, tmp_path, monkeypatch, value, expected):
        monkeypatch.setenv("KEEP_SECRETS", value)
        config_dir = _install(tmp_path, settings="setup:\n  local_secrets: ${KEEP_SECRETS}\n")
        assert load_config(config_dir).setup.local_secrets is expected

    @pytest.mark.parametrize("value", ["junk", "", " "])
    def test_a_reference_that_resolves_to_junk_or_nothing_is_refused(self, tmp_path, monkeypatch, value):
        monkeypatch.setenv("KEEP_SECRETS", value)
        config_dir = _install(tmp_path, settings="setup:\n  local_secrets: ${KEEP_SECRETS}\n")
        with pytest.raises(ConfigError) as ei:
            load_config(config_dir)
        assert "setup.local_secrets must be true or false" in str(ei.value)

    def test_a_default_in_the_reference_is_read(self, tmp_path, monkeypatch):
        monkeypatch.delenv("KEEP_SECRETS", raising=False)
        config_dir = _install(tmp_path, settings="setup:\n  local_secrets: ${KEEP_SECRETS:-false}\n")
        assert load_config(config_dir).setup.local_secrets is False

    def test_a_machine_layer_value_is_read_the_same_way(self, tmp_path):
        config_dir = _install(tmp_path, base="setup:\n  local_secrets: flase\n")
        with pytest.raises(ConfigError):
            load_config(config_dir)

    def test_the_validator_reports_it(self, tmp_path):
        from nerve.config_validate import validate_config_bundle

        config_dir = _install(tmp_path, settings="setup:\n  local_secrets: flase\n")
        result = validate_config_bundle(config_dir, workspace_override=tmp_path / "ws")
        assert any("setup.local_secrets must be true or false" in e for e in result.errors)

    def test_an_unknown_key_under_setup_is_reported_not_fatal(self):
        from nerve.config import validate_config_keys

        warnings = validate_config_keys({"setup": {"local_secret": False}})
        assert any("setup.local_secret" in w for w in warnings)
        assert not validate_config_keys({"setup": {"local_secrets": False}})


class TestTheEnvironmentAnchor:
    """``NERVE_SETUP_LOCAL_SECRETS`` can take the permission away, never grant it."""

    @pytest.mark.parametrize("value", ["0", "false", "no", "off", "False", " NO "])
    def test_a_falsy_value_turns_it_off_whatever_the_files_say(self, tmp_path, monkeypatch, value):
        monkeypatch.setenv(LOCAL_SECRETS_ENV, value)
        config_dir = _install(tmp_path, settings="setup:\n  local_secrets: true\n")
        assert load_config(config_dir).setup.local_secrets is False

    def test_a_falsy_value_needs_no_setup_section_at_all(self, tmp_path, monkeypatch):
        monkeypatch.setenv(LOCAL_SECRETS_ENV, "0")
        assert load_config(_install(tmp_path)).setup.local_secrets is False

    @pytest.mark.parametrize("value", ["1", "true", "yes", "on"])
    def test_a_truthy_value_cannot_turn_it_back_on(self, tmp_path, monkeypatch, value):
        monkeypatch.setenv(LOCAL_SECRETS_ENV, value)
        config_dir = _install(tmp_path, settings="setup:\n  local_secrets: false\n")
        assert load_config(config_dir).setup.local_secrets is False

    def test_a_truthy_value_leaves_the_default_alone(self, tmp_path, monkeypatch):
        monkeypatch.setenv(LOCAL_SECRETS_ENV, "1")
        assert load_config(_install(tmp_path)).setup.local_secrets is True

    @pytest.mark.parametrize("value", ["", "  "])
    def test_an_empty_value_is_no_opinion(self, tmp_path, monkeypatch, value):
        monkeypatch.setenv(LOCAL_SECRETS_ENV, value)
        assert load_config(_install(tmp_path)).setup.local_secrets is True
        locked = _install(tmp_path / "second", settings="setup:\n  local_secrets: false\n")
        assert load_config(locked).setup.local_secrets is False

    def test_an_unreadable_value_is_refused_not_ignored(self, tmp_path, monkeypatch):
        monkeypatch.setenv(LOCAL_SECRETS_ENV, "maybe")
        with pytest.raises(ConfigError) as ei:
            load_config(_install(tmp_path))
        assert LOCAL_SECRETS_ENV in str(ei.value)

    def test_the_anchor_keeps_the_rest_of_the_section(self, tmp_path, monkeypatch):
        """Only the one key is forced; a future sibling under ``setup:`` survives."""
        from nerve.config import _read_config_sources

        monkeypatch.setenv(LOCAL_SECRETS_ENV, "0")
        config_dir = _install(tmp_path, settings="setup:\n  local_secrets: true\n  other: 1\n")
        assert _read_config_sources(config_dir)["setup"] == {"local_secrets": False, "other": 1}

    def test_a_reload_follows_the_tracked_file(self, tmp_path):
        """Nothing caches the flag: the next load reads what the file says now."""
        config_dir = _install(tmp_path, settings="setup:\n  local_secrets: false\n")
        assert load_config(config_dir).setup.local_secrets is False
        (tmp_path / "ws" / "config" / "settings.yaml").write_text("setup:\n  local_secrets: true\n", encoding="utf-8")
        assert load_config(config_dir).setup.local_secrets is True


class TestDoctor:
    def _slim(self, tmp_path: Path) -> tuple[Path, Path]:
        config_dir = tmp_path / "config"
        workspace = tmp_path / "workspace"
        write_slim_install(
            SetupChoices(deployment="server", workspace_path=workspace, gateway_host="127.0.0.1", gateway_port=8976),
            config_dir,
        )
        return config_dir, workspace

    def test_prints_one_line_when_the_page_keeps_no_credentials(self, tmp_path):
        config_dir, workspace = self._slim(tmp_path)
        merge_settings_paths(workspace, {"setup.local_secrets": False})
        report = cli.doctor_report(load_config(config_dir))
        lines = [line for line in report.splitlines() if "setup.local_secrets: false" in line]
        assert len(lines) == 1
        assert lines[0].startswith("[--] Setup page keeps no credentials on this instance")
        assert "Memory, Channels, Sources and Agents" in lines[0]

    def test_names_the_github_source_the_environment_provides(self, tmp_path, monkeypatch):
        """With the operator's GitHub token in the environment the Sources
        step keeps its GitHub toggle: the line closes Gmail and Telegram
        instead of the whole step."""
        config_dir, workspace = self._slim(tmp_path)
        merge_settings_paths(workspace, {"setup.local_secrets": False})
        monkeypatch.setenv("GH_TOKEN", "ghp_REPLACE00000000000000")
        report = cli.doctor_report(load_config(config_dir))
        lines = [line for line in report.splitlines() if "setup.local_secrets: false" in line]
        assert len(lines) == 1
        assert lines[0].startswith("[--] Setup page keeps no credentials on this instance")
        assert "the Memory, Channels and Agents steps and the Gmail and Telegram sources are not offered" in lines[0]
        assert "GitHub comes from the environment" in lines[0]
        assert "Memory, Channels, Sources and Agents" not in lines[0]
        assert "REPLACE" not in report

    def test_says_nothing_otherwise(self, tmp_path):
        config_dir, _workspace = self._slim(tmp_path)
        assert "local_secrets" not in cli.doctor_report(load_config(config_dir))

    def test_reports_an_unreadable_value_instead_of_crashing(self, tmp_path):
        """The same path a ``lockdown`` typo takes: the config does not load,
        and the doctor says why rather than raising."""
        from click.testing import CliRunner

        config_dir, workspace = self._slim(tmp_path)
        merge_settings_paths(workspace, {"setup.local_secrets": "flase"})
        result = CliRunner().invoke(cli.main, ["-c", str(config_dir), "doctor"])
        assert result.exit_code == 1
        assert "config failed to load" in result.output
        assert "setup.local_secrets must be true or false" in result.output
        assert "Traceback" not in result.output
