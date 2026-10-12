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


class TestTheSetting:
    def test_defaults_to_keeping_secrets(self):
        assert NerveConfig().setup.local_secrets is True
        assert NerveConfig.from_dict({}).setup.local_secrets is True

    def test_read_from_the_tracked_settings(self, tmp_path):
        config_dir = _install(tmp_path, settings="setup:\n  local_secrets: false\n")
        assert load_config(config_dir).setup.local_secrets is False

    @pytest.mark.parametrize("spelling", ["false", "no", "0", "off"])
    def test_string_spellings_are_coerced_like_every_other_flag(self, spelling):
        """An env reference interpolates to a string; ``bool("false")`` must not win."""
        assert NerveConfig.from_dict({"setup": {"local_secrets": spelling}}).setup.local_secrets is False

    def test_a_bare_section_is_the_default(self):
        assert NerveConfig.from_dict({"setup": None}).setup.local_secrets is True

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

    def test_says_nothing_otherwise(self, tmp_path):
        config_dir, _workspace = self._slim(tmp_path)
        assert "local_secrets" not in cli.doctor_report(load_config(config_dir))
