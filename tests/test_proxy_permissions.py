"""Permission regression tests for the CLIProxyAPI integration.

The proxy config embeds an API key, its auth dir holds OAuth token JSON, and
both the stdout/stderr log and the proxy's own per-request error logs can carry
prompt text. All of it must be readable only by the owner even though it lives
under Nerve's shared state dir. These tests pin that contract:

* the files Nerve writes itself (config, log) are created — and repaired — at
  0600, and the directories it manages (auth dir, ``<auth-dir>/logs``) at 0700,
  regardless of how permissive the daemon's umask is;
* the children Nerve launches (proxy, login) run under a 0077 umask so the
  files *they* create land owner-only too, without disturbing the daemon's own
  umask or the proxy's process group;
* the tightening stays scoped — shared parents and symlink targets are left
  alone, and a chmod that fails does not abort start-up.

Everything here is Linux/POSIX and uses synthetic credentials + fake children;
no real login or daemon is involved.
"""

from __future__ import annotations

import contextlib
import os
import stat
import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from nerve.config import NerveConfig
from nerve.proxy.service import (
    ProxyService,
    _ensure_private_dir,
    _login_preexec,
    _open_private_append_log,
    _proxy_preexec,
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="POSIX permission bits / preexec_fn are not meaningful on Windows",
)


@contextlib.contextmanager
def permissive_umask(mask: int = 0o000):
    """Run the body under a wide-open umask, then restore the real one.

    A fresh file/dir created through ``open``/``mkdir`` under ``0o000`` would be
    world read/write — so any test that still sees 0600/0700 proves the code set
    the mode explicitly rather than riding on a lucky umask.
    """
    old = os.umask(mask)
    try:
        yield
    finally:
        os.umask(old)


def _current_umask() -> int:
    """Read the process umask without permanently changing it."""
    value = os.umask(0o022)
    os.umask(value)
    return value


def _mode(path: Path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


# ------------------------------------------------------------------ #
#  _ensure_private_dir                                                #
# ------------------------------------------------------------------ #


class TestEnsurePrivateDir:
    def test_creates_dir_0700_under_permissive_umask(self, tmp_path: Path) -> None:
        target = tmp_path / "auth"
        with permissive_umask(0o000):
            _ensure_private_dir(target)
        assert target.is_dir()
        assert _mode(target) == 0o700

    def test_repairs_existing_loose_dir(self, tmp_path: Path) -> None:
        target = tmp_path / "auth"
        target.mkdir()
        os.chmod(target, 0o777)
        _ensure_private_dir(target)
        assert _mode(target) == 0o700

    def test_shared_parent_left_untouched(self, tmp_path: Path) -> None:
        # A custom auth_dir nested under a directory the operator shares on
        # purpose: only the leaf is tightened, never the parent.
        shared = tmp_path / "shared"
        shared.mkdir()
        os.chmod(shared, 0o755)
        auth = shared / "auth"
        _ensure_private_dir(auth)
        assert _mode(auth) == 0o700
        assert _mode(shared) == 0o755  # parent preserved

    def test_symlink_target_not_followed(self, tmp_path: Path) -> None:
        # chmod follows symlinks; a symlinked auth dir must not silently
        # re-permission whatever unrelated target it points at.
        real_target = tmp_path / "unrelated"
        real_target.mkdir()
        os.chmod(real_target, 0o755)
        link = tmp_path / "auth"
        link.symlink_to(real_target, target_is_directory=True)

        _ensure_private_dir(link)

        assert real_target.is_symlink() is False
        assert _mode(real_target) == 0o755  # target's mode untouched

    def test_chmod_failure_is_not_fatal(self, tmp_path: Path, monkeypatch) -> None:
        target = tmp_path / "auth"

        def boom(*_a, **_k):
            raise PermissionError("nope")

        monkeypatch.setattr(os, "chmod", boom)
        # Must not raise — the files inside are still owner-only via umask +
        # explicit file modes, so start-up should survive a chmod refusal.
        _ensure_private_dir(target)
        assert target.is_dir()


# ------------------------------------------------------------------ #
#  _open_private_append_log                                           #
# ------------------------------------------------------------------ #


class TestOpenPrivateAppendLog:
    def test_creates_log_0600_under_permissive_umask(self, tmp_path: Path) -> None:
        log = tmp_path / "proxy.log"
        with permissive_umask(0o000):
            fh = _open_private_append_log(log)
        try:
            fh.write("hello\n")
        finally:
            fh.close()
        assert _mode(log) == 0o600
        assert log.read_text() == "hello\n"

    def test_repairs_existing_world_readable_log(self, tmp_path: Path) -> None:
        log = tmp_path / "proxy.log"
        log.write_text("old\n")
        os.chmod(log, 0o644)
        fh = _open_private_append_log(log)
        try:
            fh.write("new\n")
        finally:
            fh.close()
        assert _mode(log) == 0o600
        # Append, not truncate — the earlier bytes survive.
        assert log.read_text() == "old\nnew\n"


# ------------------------------------------------------------------ #
#  _write_proxy_config (config file + managed directories)            #
# ------------------------------------------------------------------ #


def _service(tmp_path: Path) -> ProxyService:
    cfg = NerveConfig.from_dict({
        "proxy": {
            "enabled": True,
            "port": 9000,
            "host": "127.0.0.1",
            "auth_dir": str(tmp_path / "auth"),
            "api_key": "sk-test-secret",
        },
    })
    svc = ProxyService(cfg)
    svc._config_path = tmp_path / "proxy-config.yaml"
    return svc


class TestWriteProxyConfig:
    def test_config_and_dirs_owner_only_under_permissive_umask(self, tmp_path: Path) -> None:
        svc = _service(tmp_path)
        with permissive_umask(0o000):
            written = svc._write_proxy_config()
        assert _mode(written) == 0o600
        assert _mode(tmp_path / "auth") == 0o700
        assert _mode(tmp_path / "auth" / "logs") == 0o700

    def test_existing_world_readable_config_repaired(self, tmp_path: Path) -> None:
        svc = _service(tmp_path)
        # An earlier run left the config world-readable.
        svc._config_path.write_text("stale: true\n")
        os.chmod(svc._config_path, 0o644)
        svc._write_proxy_config()
        assert _mode(svc._config_path) == 0o600
        # And it really was rewritten with the current config.
        import yaml
        data = yaml.safe_load(svc._config_path.read_text())
        assert data["api-keys"] == ["sk-test-secret"]


# ------------------------------------------------------------------ #
#  Child umask preexec functions                                      #
# ------------------------------------------------------------------ #


def _spawn_child_creating(tmp_path: Path, preexec) -> subprocess.Popen:
    """Launch a short-lived child that creates a file and a dir, then sleeps.

    The sleep keeps the child alive long enough to read its process group
    before it exits and gets reaped.
    """
    script = 'touch "$1/childfile"; mkdir "$1/childdir"; sleep 1'
    return subprocess.Popen(
        ["sh", "-c", script, "sh", str(tmp_path)],
        preexec_fn=preexec,
    )


class TestChildUmaskPreexec:
    def test_proxy_preexec_owner_only_files_and_own_group(self, tmp_path: Path) -> None:
        with permissive_umask(0o022):
            proc = _spawn_child_creating(tmp_path, _proxy_preexec)
            try:
                # setpgrp() put the child in its own process group.
                assert os.getpgid(proc.pid) == proc.pid
            finally:
                proc.wait()
            # The daemon's own umask was not touched by the child's preexec.
            assert _current_umask() == 0o022

        # umask(0o077) in the child made everything it created owner-only.
        assert _mode(tmp_path / "childfile") == 0o600
        assert _mode(tmp_path / "childdir") == 0o700

    def test_login_preexec_owner_only_but_keeps_process_group(self, tmp_path: Path) -> None:
        with permissive_umask(0o022):
            proc = _spawn_child_creating(tmp_path, _login_preexec)
            try:
                # No setpgrp(): login stays in the caller's process group.
                assert os.getpgid(proc.pid) == os.getpgrp()
            finally:
                proc.wait()

        assert _mode(tmp_path / "childfile") == 0o600
        assert _mode(tmp_path / "childdir") == 0o700


# ------------------------------------------------------------------ #
#  start()/stop() integration — real subprocess, fake binary          #
# ------------------------------------------------------------------ #


class TestStartLifecycleHardening:
    @pytest.mark.asyncio
    async def test_start_hardens_paths_and_preserves_process_group(self, tmp_path: Path) -> None:
        # A fake binary that ignores its args and just lives, so start()/stop()
        # exercise the real subprocess + preexec + log-open path without a proxy.
        binary = tmp_path / "cli-proxy-api"
        binary.write_bytes(b"#!/bin/sh\nexec sleep 30\n")
        binary.chmod(binary.stat().st_mode | stat.S_IXUSR)

        cfg = NerveConfig.from_dict({
            "proxy": {
                "enabled": True,
                "binary_path": str(binary),
                "auth_dir": str(tmp_path / "auth"),
                "log_file": str(tmp_path / "proxy.log"),
                "api_key": "sk-test-secret",
            },
        })
        svc = ProxyService(cfg)
        svc._config_path = tmp_path / "proxy-config.yaml"

        with permissive_umask(0o000):
            with patch.object(svc, "_wait_for_healthy", new_callable=AsyncMock, return_value=True):
                await svc.start()

        try:
            assert svc._process is not None
            pid = svc._process.pid
            # Own process group preserved (was preexec_fn=os.setpgrp).
            assert os.getpgid(pid) == pid
            # Every secret is owner-only despite the wide-open umask.
            assert _mode(tmp_path / "proxy.log") == 0o600
            assert _mode(tmp_path / "proxy-config.yaml") == 0o600
            assert _mode(tmp_path / "auth") == 0o700
            assert _mode(tmp_path / "auth" / "logs") == 0o700
        finally:
            await svc.stop()

        # Teardown reaped the child.
        assert svc._process is None
