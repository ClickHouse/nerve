"""``auth.mode`` — the identity mode, ``local`` only in this version.

Startup-only, settable from the environment, and refused loudly for any value
this version does not implement rather than defaulted: the only default it
could fall back to is the mode with the weakest guarantees, and an operator
who asked for something else must not get ``local`` in silence.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from nerve.config import (
    AUTH_MODE_ENV,
    AUTH_MODES,
    AuthConfig,
    ConfigError,
    NerveConfig,
    load_config,
    workspace_settings_file,
)
from nerve.config_reload import _RESTART_ONLY_PATHS, restart_required


def _install(tmp_path: Path, *, local: str = "", settings: str = "") -> Path:
    config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
    config_dir.mkdir()
    (ws / "config").mkdir(parents=True)
    (config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
    (config_dir / "config.local.yaml").write_text(local, encoding="utf-8")
    if settings:
        workspace_settings_file(ws).write_text(settings, encoding="utf-8")
    return config_dir


@pytest.fixture(autouse=True)
def _no_anchor(monkeypatch):
    monkeypatch.delenv(AUTH_MODE_ENV, raising=False)


class TestParsing:
    def test_only_local_exists(self):
        assert AUTH_MODES == ("local",)

    def test_default_is_local(self):
        assert NerveConfig().auth.mode == "local"
        assert AuthConfig.from_dict({}).mode == "local"
        assert AuthConfig.from_dict({"mode": None}).mode == "local"
        assert AuthConfig.from_dict({"mode": ""}).mode == "local"

    @pytest.mark.parametrize("spelling", ["local", "LOCAL", " Local "])
    def test_local_is_accepted_case_insensitively(self, spelling):
        assert AuthConfig.from_dict({"mode": spelling}).mode == "local"

    @pytest.mark.parametrize("value", ["external", "simple", "hosted", 1, True])
    def test_anything_else_is_refused_with_the_accepted_value_named(self, value):
        with pytest.raises(ConfigError) as ei:
            AuthConfig.from_dict({"mode": value})
        message = str(ei.value)
        assert "auth.mode" in message
        assert "'local'" in message
        assert repr(value) in message


class TestLoading:
    def test_unset_loads_as_local(self, tmp_path):
        assert load_config(_install(tmp_path)).auth.mode == "local"

    def test_external_in_a_file_fails_startup(self, tmp_path):
        config_dir = _install(tmp_path, local="auth:\n  mode: external\n")
        with pytest.raises(ConfigError, match="auth.mode"):
            load_config(config_dir)

    def test_external_in_the_tracked_settings_fails_too(self, tmp_path):
        config_dir = _install(tmp_path, settings="auth:\n  mode: external\n")
        with pytest.raises(ConfigError, match="auth.mode"):
            load_config(config_dir)

    def test_env_reference_in_a_file_resolves(self, tmp_path, monkeypatch):
        config_dir = _install(tmp_path, settings="auth:\n  mode: ${NERVE_AUTH_MODE:-local}\n")
        assert load_config(config_dir).auth.mode == "local"

    def test_validate_reports_it_instead_of_the_box_finding_out(self, tmp_path):
        from nerve.config_validate import validate_config_bundle

        config_dir = _install(tmp_path, local="auth:\n  mode: external\n")
        result = validate_config_bundle(config_dir)
        assert not result.ok
        assert any("auth.mode" in e for e in result.errors)


class TestEnvironmentAnchor:
    """``NERVE_AUTH_MODE`` wins over every file, in both directions: a file
    cannot switch the mode away from what the service definition says, which
    is what "never writable by a configuration push" means once other modes
    exist. Today the only effect is that an unsupported value is refused
    wherever it comes from."""

    def test_env_value_is_validated(self, tmp_path, monkeypatch):
        monkeypatch.setenv(AUTH_MODE_ENV, "external")
        with pytest.raises(ConfigError) as ei:
            load_config(_install(tmp_path))
        assert "auth.mode" in str(ei.value)
        assert AUTH_MODE_ENV in str(ei.value)

    def test_env_wins_over_a_file(self, tmp_path, monkeypatch):
        monkeypatch.setenv(AUTH_MODE_ENV, "local")
        config_dir = _install(tmp_path, local="auth:\n  mode: external\n")
        assert load_config(config_dir).auth.mode == "local"

    def test_env_wins_over_an_env_reference_in_a_file(self, tmp_path, monkeypatch):
        monkeypatch.setenv(AUTH_MODE_ENV, "local")
        config_dir = _install(tmp_path, settings="auth:\n  mode: ${OTHER_MODE:-external}\n")
        assert load_config(config_dir).auth.mode == "local"

    @pytest.mark.parametrize("raw", ["", "   "])
    def test_blank_env_has_no_opinion(self, tmp_path, monkeypatch, raw):
        monkeypatch.setenv(AUTH_MODE_ENV, raw)
        assert load_config(_install(tmp_path)).auth.mode == "local"

    def test_env_does_not_clobber_the_rest_of_the_auth_section(self, tmp_path, monkeypatch):
        monkeypatch.setenv(AUTH_MODE_ENV, "local")
        config_dir = _install(tmp_path, local="auth:\n  jwt_expiry_hours: 12\n")
        config = load_config(config_dir)
        assert (config.auth.mode, config.auth.jwt_expiry_hours) == ("local", 12)


class TestRestartOnly:
    def test_listed_as_restart_only(self):
        assert "auth.mode" in _RESTART_ONLY_PATHS

    def test_a_changed_mode_is_reported_not_applied(self):
        old = NerveConfig(auth=AuthConfig(mode="local"))
        # Constructed directly: the loader would refuse this value, which is
        # the point — the diff is about what a reload must not apply.
        new = NerveConfig(auth=AuthConfig(mode="external"))
        assert any(line.startswith("auth.mode:") for line in restart_required(old, new))
        assert restart_required(old, NerveConfig()) == []
