"""Fail-closed parsing of the ``auth`` configuration section."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from nerve.config import ConfigError, load_config, workspace_settings_file
from nerve.config_validate import validate_config_bundle


def _install(
    tmp_path: Path, *, base: str = "", local: str = "", settings: str = "",
) -> Path:
    config_dir, workspace = tmp_path / "cfg", tmp_path / "ws"
    config_dir.mkdir()
    (workspace / "config").mkdir(parents=True)
    (config_dir / "config.yaml").write_text(
        f"workspace: {workspace}\n{base}", encoding="utf-8"
    )
    (config_dir / "config.local.yaml").write_text(local, encoding="utf-8")
    if settings:
        workspace_settings_file(workspace).write_text(settings, encoding="utf-8")
    return config_dir


def _locked_workspace(workspace: Path) -> None:
    if not shutil.which("git"):
        pytest.skip("git not available")
    subprocess.run(["git", "init", "-q"], cwd=workspace, check=True)
    subprocess.run(
        ["git", "remote", "add", "origin", "https://example.invalid/config.git"],
        cwd=workspace,
        check=True,
    )


_MALFORMED = pytest.mark.parametrize(
    "bad_auth",
    ["auth: garbage\n", "auth:\n  - a\n  - b\n", "auth: 42\n"],
    ids=["string", "list", "number"],
)


@_MALFORMED
@pytest.mark.parametrize("layer", ["base", "local", "settings"])
def test_malformed_auth_is_refused_per_layer(tmp_path, bad_auth, layer):
    kwargs = {layer: bad_auth}
    config_dir = _install(tmp_path, **kwargs)

    with pytest.raises(ConfigError, match="auth .* must be a mapping"):
        load_config(config_dir)
    result = validate_config_bundle(config_dir)
    assert not result.ok
    assert any("auth" in error and "mapping" in error for error in result.errors)


def test_a_higher_layer_cannot_hide_malformed_auth(tmp_path):
    config_dir = _install(
        tmp_path,
        base="auth: garbage\n",
        local="auth:\n  jwt_secret: test-secret-padded-to-32-bytes!!\n",
    )
    with pytest.raises(ConfigError, match="auth in config.yaml must be a mapping"):
        load_config(config_dir)
    assert not validate_config_bundle(config_dir).ok


def test_locked_loading_still_validates_every_auth_layer(tmp_path):
    config_dir = _install(
        tmp_path,
        local="auth: garbage\n",
        settings=(
            "lockdown: true\n"
            "auth:\n  jwt_secret: test-secret-padded-to-32-bytes!!\n"
        ),
    )
    _locked_workspace(tmp_path / "ws")
    with pytest.raises(ConfigError, match="auth .* must be a mapping"):
        load_config(config_dir)
    assert not validate_config_bundle(config_dir).ok


_HASH = "$2b$12$abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTU"
_JWT = "configured-secret-padded-to-thirty-two-bytes"
_CREDENTIALS = f"auth:\n  password_hash: '{_HASH}'\n  jwt_secret: {_JWT}\n"


@pytest.mark.parametrize(
    ("layers"),
    [
        {"settings": _CREDENTIALS, "local": "auth:\n"},
        {"settings": _CREDENTIALS, "base": "auth:\n"},
        {"base": _CREDENTIALS, "local": "auth:\n"},
        {"settings": _CREDENTIALS, "base": "auth:\n", "local": "auth:\n"},
    ],
)
def test_null_auth_is_an_empty_overlay(tmp_path, layers):
    config_dir = _install(tmp_path, **layers)
    config = load_config(config_dir)
    assert (config.auth.password_hash, config.auth.jwt_secret) == (_HASH, _JWT)
    assert not any(
        "auth" in error for error in validate_config_bundle(config_dir).errors
    )
