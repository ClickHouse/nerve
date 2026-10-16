"""Tests for nerve.agent.credential_store — pinning the Claude CLI's OAuth
credential store to ``~/.claude/.credentials.json`` via a ``security`` shim."""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from nerve.agent import credential_store as cs
from nerve.agent.backends.claude import ClaudeBackend


# ---------------------------------------------------------------------------
# Shim generation
# ---------------------------------------------------------------------------

def test_ensure_shim_writes_an_executable_script(tmp_path: Path):
    shim = cs.ensure_security_shim(tmp_path / "bin")
    assert shim == tmp_path / "bin" / "security"
    assert shim.read_text() == cs.SHIM_SCRIPT
    assert shim.stat().st_mode & stat.S_IXUSR
    assert cs.SHIM_SCRIPT.startswith("#!/bin/sh\n")
    assert f'"{cs.CLI_SERVICE_PREFIX}"*' in cs.SHIM_SCRIPT
    assert f"exec {cs.REAL_SECURITY}" in cs.SHIM_SCRIPT


def test_ensure_shim_is_idempotent_and_repairs_edits(tmp_path: Path):
    shim = cs.ensure_security_shim(tmp_path / "bin")
    before = shim.stat().st_mtime_ns
    assert cs.ensure_security_shim(tmp_path / "bin") == shim
    assert shim.stat().st_mtime_ns == before  # same content → not rewritten
    shim.write_text("#!/bin/sh\nexit 0\n")
    cs.ensure_security_shim(tmp_path / "bin")
    assert shim.read_text() == cs.SHIM_SCRIPT
    assert shim.stat().st_mode & stat.S_IXUSR
    # no temp files left behind
    assert sorted(p.name for p in (tmp_path / "bin").iterdir()) == ["security"]


def test_prepend_to_path_puts_the_shim_first_exactly_once(tmp_path: Path):
    d = tmp_path / "bin"
    assert cs.prepend_to_path("/usr/bin:/bin", d) == f"{d}:/usr/bin:/bin"
    # already present anywhere → moved to the front, not duplicated
    assert cs.prepend_to_path(f"/usr/bin:{d}:/bin", d) == f"{d}:/usr/bin:/bin"
    assert cs.prepend_to_path("", d) == str(d)
    assert cs.prepend_to_path(None, d) == str(d)


def test_file_store_env_uses_nerve_home(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("NERVE_HOME", str(tmp_path))
    env = cs.file_store_env("/usr/bin")
    assert env == {"PATH": f"{tmp_path / 'bin'}:/usr/bin"}
    assert (tmp_path / "bin" / "security").is_file()


def test_file_store_env_defaults_to_process_path(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("PATH", "/opt/x/bin:/usr/bin")
    env = cs.file_store_env(home=tmp_path)
    assert env["PATH"] == f"{tmp_path / 'bin'}:/opt/x/bin:/usr/bin"


# ---------------------------------------------------------------------------
# Shim behaviour (needs the real /usr/bin/security)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(sys.platform != "darwin", reason="uses macOS /usr/bin/security")
def test_shim_answers_cli_items_like_a_locked_keychain(tmp_path: Path):
    shim = cs.ensure_security_shim(tmp_path / "bin")
    for argv in (
        ["find-generic-password", "-a", "someone", "-w", "-s", "Claude Code-credentials"],
        ["find-generic-password", "-a", "someone", "-w", "-s", "Claude Code"],
        ["add-generic-password", "-U", "-a", "someone", "-s", "Claude Code-credentials", "-w", "x"],
        ["delete-generic-password", "-a", "someone", "-s", "Claude Code-credentials"],
    ):
        res = subprocess.run([str(shim), *argv], capture_output=True, text=True)
        assert res.returncode == 36, argv
        assert "User interaction is not allowed" in res.stderr


@pytest.mark.skipif(sys.platform != "darwin", reason="uses macOS /usr/bin/security")
def test_shim_passes_other_invocations_through(tmp_path: Path):
    shim = cs.ensure_security_shim(tmp_path / "bin")
    res = subprocess.run([str(shim), "list-keychains"], capture_output=True, text=True)
    assert res.returncode == 0
    assert "keychain" in res.stdout.lower()


# ---------------------------------------------------------------------------
# Backend env wiring
# ---------------------------------------------------------------------------

def _backend(store: str) -> ClaudeBackend:
    config = SimpleNamespace(
        provider=SimpleNamespace(
            is_bedrock=False, aws_region="", aws_profile="",
            aws_access_key_id="", aws_secret_access_key="",
        ),
        proxy=SimpleNamespace(enabled=False, host="", port=0),
        effective_api_key="",
        agent=SimpleNamespace(
            model_aliases={}, agent_teams=True, claude_credential_store=store,
        ),
    )
    return ClaudeBackend(SimpleNamespace(config=lambda: config))


def test_build_env_auto_leaves_path_alone(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("NERVE_HOME", str(tmp_path))
    monkeypatch.setattr(sys, "platform", "darwin")
    assert "PATH" not in _backend("auto")._build_env()
    assert not (tmp_path / "bin").exists()


def test_build_env_file_prepends_the_shim_on_darwin(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("NERVE_HOME", str(tmp_path))
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    monkeypatch.setattr(sys, "platform", "darwin")
    env = _backend("file")._build_env()
    assert env["PATH"] == f"{tmp_path / 'bin'}:/usr/bin:/bin"
    assert (tmp_path / "bin" / "security").is_file()


def test_build_env_file_is_a_noop_off_darwin(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("NERVE_HOME", str(tmp_path))
    monkeypatch.setattr(sys, "platform", "linux")
    assert "PATH" not in _backend("file")._build_env()
    assert not (tmp_path / "bin").exists()


def test_build_env_survives_an_unwritable_shim_dir(tmp_path: Path, monkeypatch, caplog):
    blocker = tmp_path / "bin"
    blocker.write_text("not a directory")  # mkdir(parents=True) must fail
    monkeypatch.setenv("NERVE_HOME", str(tmp_path))
    monkeypatch.setattr(sys, "platform", "darwin")
    env = _backend("file")._build_env()
    assert "PATH" not in env
    assert "credential-store shim" in caplog.text
    assert env["CLAUDE_CODE_DISABLE_CRON"] == "1"  # the rest of the env is intact
