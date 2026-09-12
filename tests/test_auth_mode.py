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

    def test_the_tracked_settings_file_cannot_even_break_startup(self, tmp_path, caplog):
        """A pushed file is not a source for the mode — and must not be able to
        crash the instance either, so the key is ignored with a warning rather
        than refused."""
        config_dir = _install(tmp_path, settings="auth:\n  mode: external\n")
        with caplog.at_level("WARNING", logger="nerve.config"):
            assert load_config(config_dir).auth.mode == "local"
        assert any(
            "ignoring 'auth.mode'" in r.getMessage() and "settings.yaml" in r.getMessage()
            for r in caplog.records
        )

    def test_env_reference_in_a_machine_local_file_resolves(self, tmp_path, monkeypatch):
        config_dir = _install(tmp_path, local="auth:\n  mode: ${NERVE_AUTH_MODE:-local}\n")
        assert load_config(config_dir).auth.mode == "local"

    def test_validate_reports_it_instead_of_the_box_finding_out(self, tmp_path):
        from nerve.config_validate import validate_config_bundle

        config_dir = _install(tmp_path, local="auth:\n  mode: external\n")
        result = validate_config_bundle(config_dir)
        assert not result.ok
        assert any("auth.mode" in e for e in result.errors)


def _git_repo_with_remote(ws: Path) -> None:
    """What a locked workspace is on a real box; the remote is never contacted."""
    import shutil
    import subprocess

    if not shutil.which("git"):
        pytest.skip("git not available")
    subprocess.run(["git", "init", "-q"], cwd=str(ws), check=True, capture_output=True)
    subprocess.run(
        ["git", "remote", "add", "origin", "https://example.invalid/config.git"],
        cwd=str(ws), check=True, capture_output=True,
    )


class TestTrackedLayerIsNotASource:
    """0.1: the mode is never writable by an external configuration push, and
    the tracked workspace layer is exactly what a push or sync delivers. Only
    the machine-local layers and ``NERVE_AUTH_MODE`` count. Only one mode
    exists today, so a second one is pretended into existence; the point is
    where a value is allowed to come from."""

    @pytest.fixture(autouse=True)
    def _two_modes(self, monkeypatch):
        import nerve.config as cfgmod

        monkeypatch.setattr(cfgmod, "AUTH_MODES", ("local", "other"))

    def test_a_tracked_value_is_ignored_with_a_warning(self, tmp_path, caplog):
        config_dir = _install(tmp_path, settings="auth:\n  mode: other\n")
        with caplog.at_level("WARNING", logger="nerve.config"):
            assert load_config(config_dir).auth.mode == "local"
        assert any("ignoring 'auth.mode'" in r.getMessage() for r in caplog.records)

    def test_a_machine_local_value_is_applied(self, tmp_path):
        config_dir = _install(
            tmp_path, local="auth:\n  mode: other\n", settings="auth:\n  mode: local\n",
        )
        assert load_config(config_dir).auth.mode == "other"

    def test_the_environment_wins_over_a_machine_local_value(self, tmp_path, monkeypatch):
        monkeypatch.setenv(AUTH_MODE_ENV, "other")
        config_dir = _install(tmp_path, local="auth:\n  mode: local\n")
        assert load_config(config_dir).auth.mode == "other"

    def test_a_tracked_value_does_not_leak_through_the_rest_of_the_auth_section(
        self, tmp_path,
    ):
        """Only the mode is dropped; the tracked layer may still carry the
        other auth keys (a fleet supplies ``jwt_secret`` from there)."""
        config_dir = _install(
            tmp_path, settings="auth:\n  mode: other\n  jwt_expiry_hours: 12\n",
        )
        config = load_config(config_dir)
        assert (config.auth.mode, config.auth.jwt_expiry_hours) == ("local", 12)

    def test_a_machine_local_mode_survives_lockdown(self, tmp_path):
        """F13: lockdown drops the machine layers for everything else, but the
        identity mode is resolved from them independently — a locked box still
        reads its mode from config.local.yaml, never from the tracked file."""
        config_dir = _install(
            tmp_path,
            local="auth:\n  mode: other\n",
            settings="lockdown: true\nauth:\n  jwt_secret: test-secret-padded-to-32-bytes!!\n",
        )
        _git_repo_with_remote(tmp_path / "ws")
        config = load_config(config_dir)
        assert config.lockdown and config.auth.mode == "other"

    def test_a_tracked_lockdown_flip_cannot_change_the_mode(self, tmp_path):
        """The reproduction behind F13: flipping only the tracked lockdown flag
        must not move a machine-local mode. It did before, because lockdown
        dropped the layer that held it — an external push changing auth."""
        def _mode_with_lockdown(sub: str, locked: bool) -> str:
            root = tmp_path / sub
            root.mkdir()
            config_dir = _install(
                root,
                local="auth:\n  mode: other\n",
                settings=f"lockdown: {'true' if locked else 'false'}\n"
                         "auth:\n  jwt_secret: test-secret-padded-to-32-bytes!!\n",
            )
            _git_repo_with_remote(root / "ws")
            return load_config(config_dir).auth.mode

        assert _mode_with_lockdown("unlocked", False) == "other"
        assert _mode_with_lockdown("locked", True) == "other"

    def test_the_environment_still_overrides_a_machine_local_mode_under_lockdown(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setenv(AUTH_MODE_ENV, "local")
        config_dir = _install(
            tmp_path,
            local="auth:\n  mode: other\n",
            settings="lockdown: true\nauth:\n  jwt_secret: test-secret-padded-to-32-bytes!!\n",
        )
        _git_repo_with_remote(tmp_path / "ws")
        assert load_config(config_dir).auth.mode == "local"

    def test_the_layer_table_lists_it_as_machine_local(self):
        from nerve.migrate import _is_machine_local

        assert _is_machine_local("auth.mode")
        assert not _is_machine_local("auth.jwt_secret")


class TestValidatorHonoursTheEnvAnchor:
    """F14: `nerve config validate` must apply NERVE_AUTH_MODE the way runtime
    does, so a check cannot approve a config that will not start or reject one
    that will. A second mode is mocked so the anchor has something to carry."""

    @pytest.fixture(autouse=True)
    def _two_modes(self, monkeypatch):
        import nerve.config as cfgmod

        monkeypatch.setattr(cfgmod, "AUTH_MODES", ("local", "other"))

    def test_a_bad_env_value_fails_validation_like_runtime(self, tmp_path, monkeypatch):
        from nerve.config_validate import validate_config_bundle

        monkeypatch.setenv(AUTH_MODE_ENV, "bogus")
        result = validate_config_bundle(_install(tmp_path))
        assert not result.ok
        assert any("auth.mode" in e for e in result.errors)

    def test_env_overrides_an_invalid_file_value_in_validation_too(self, tmp_path, monkeypatch):
        from nerve.config_validate import validate_config_bundle

        monkeypatch.setenv(AUTH_MODE_ENV, "local")
        config_dir = _install(tmp_path, local="auth:\n  mode: bogus\n")
        result = validate_config_bundle(config_dir)
        assert not any("auth.mode" in e for e in result.errors)


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

    @pytest.mark.asyncio
    async def test_a_reload_reports_a_changed_mode_but_does_not_apply_it(
        self, tmp_path, monkeypatch,
    ):
        """Startup-pinned: the mode the daemon started with is carried onto
        every reloaded config object, so a reload — or a workspace sync — can
        report the change but cannot make it live. Only one mode exists today,
        so a second one is pretended into existence for the file to ask for;
        the point is what a reload does with a changed value."""
        import nerve.config as cfgmod
        from nerve.config import set_config
        from nerve.config_reload import reload_all

        config_dir = _install(tmp_path)
        workspace_settings_file(tmp_path / "ws").write_text("timezone: UTC\n", encoding="utf-8")
        set_config(load_config(config_dir))
        assert cfgmod.get_config().auth.mode == "local"

        monkeypatch.setattr(cfgmod, "AUTH_MODES", ("local", "other"))
        (config_dir / "config.local.yaml").write_text("auth:\n  mode: other\n", encoding="utf-8")
        summary = await reload_all(None, None, config_dir)

        assert summary["config"] == "reloaded"
        assert "auth.mode" in summary.get("restart_required", "")
        assert cfgmod.get_config().auth.mode == "local"
        set_config(NerveConfig())

    def test_a_changed_mode_is_reported_not_applied(self):
        old = NerveConfig(auth=AuthConfig(mode="local"))
        # Constructed directly: the loader would refuse this value, which is
        # the point — the diff is about what a reload must not apply.
        new = NerveConfig(auth=AuthConfig(mode="external"))
        assert any(line.startswith("auth.mode:") for line in restart_required(old, new))
        assert restart_required(old, NerveConfig()) == []
