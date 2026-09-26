"""Permission regression tests for the CLIProxyAPI integration.

The proxy config embeds an API key, its auth dir holds OAuth token JSON, and
both the stdout/stderr log and the proxy's own per-request error logs can carry
prompt text. All of it must be readable only by the owner even though it lives
under Nerve's shared state dir. These tests pin that contract:

* the files Nerve writes itself (config, log) are created — and repaired — at
  0600, and the auth directory it manages at 0700, regardless of how permissive
  the daemon's umask is;
* the children Nerve launches (proxy, login) run under a native 0077 umask so
  the files *they* create land owner-only too, without disturbing the daemon's
  own umask; the proxy additionally gets its own process group while login
  keeps the caller's;
* the directory tightening stays scoped — a shared parent is left alone, a
  symlinked auth directory's target is not chased, and a chmod that fails does
  not abort start-up. (The config/log writers do follow a symlinked path to its
  target, like a normal write; this module does not claim otherwise.)

Everything here is Linux/POSIX and uses synthetic credentials + fake children;
no real login or daemon is involved.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import stat
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from nerve.config import NerveConfig
from nerve.proxy.service import (
    ProxyService,
    _ensure_private_dir,
    _open_private_append_log,
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="POSIX permission bits / umask / process groups are not meaningful on Windows",
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


def _wait_own_group(pid: int, timeout: float = 2.0) -> bool:
    """Wait until ``pid`` leads its own process group.

    ``process_group=0`` is applied in the forked child, which can lag the
    parent's return from subprocess creation by a few instructions.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if os.getpgid(pid) == pid:
                return True
        except ProcessLookupError:
            return False
        time.sleep(0.02)
    return False


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

    def test_symlinked_dir_target_not_chased(self, tmp_path: Path) -> None:
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
        # Must not raise — new files inside are still owner-only via umask +
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
#  _write_proxy_config (config file + managed auth directory)         #
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
    def test_config_and_auth_dir_owner_only_under_permissive_umask(self, tmp_path: Path) -> None:
        svc = _service(tmp_path)
        with permissive_umask(0o000):
            written = svc._write_proxy_config()
        assert _mode(written) == 0o600
        assert _mode(tmp_path / "auth") == 0o700

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
#  Native launch arguments (umask / process_group), not preexec_fn    #
# ------------------------------------------------------------------ #


class TestNativeLaunchArgs:
    """The proxy and login children are launched with native subprocess
    ``umask``/``process_group`` arguments instead of a ``preexec_fn`` callback
    (which the stdlib documents as unsafe in a threaded process). These fake
    children confirm the arguments Nerve passes actually yield owner-only files
    and the intended process group.
    """

    async def _spawn_fake_child(self, tmp_path: Path, *, own_group: bool):
        script = 'touch "$1/childfile"; mkdir "$1/childdir"; sleep 1'
        kwargs: dict = {"umask": 0o077}
        if own_group:
            kwargs["process_group"] = 0
        return await asyncio.create_subprocess_exec(
            "sh", "-c", script, "sh", str(tmp_path), **kwargs,
        )

    @pytest.mark.asyncio
    async def test_proxy_style_owner_only_and_own_group(self, tmp_path: Path) -> None:
        with permissive_umask(0o022):
            proc = await self._spawn_fake_child(tmp_path, own_group=True)
            try:
                # process_group=0 puts the child in its own process group.
                assert _wait_own_group(proc.pid)
                # The child's umask did not touch the daemon's own umask.
                assert _current_umask() == 0o022
            finally:
                await proc.wait()
        # umask(0o077) in the child made everything it created owner-only.
        assert _mode(tmp_path / "childfile") == 0o600
        assert _mode(tmp_path / "childdir") == 0o700

    @pytest.mark.asyncio
    async def test_login_style_owner_only_but_keeps_process_group(self, tmp_path: Path) -> None:
        with permissive_umask(0o022):
            proc = await self._spawn_fake_child(tmp_path, own_group=False)
            try:
                # No process_group: login stays in the caller's process group.
                assert os.getpgid(proc.pid) == os.getpgrp()
            finally:
                await proc.wait()
        assert _mode(tmp_path / "childfile") == 0o600
        assert _mode(tmp_path / "childdir") == 0o700


# ------------------------------------------------------------------ #
#  start()/stop() integration — real subprocess, fake binary          #
# ------------------------------------------------------------------ #


class TestStartLifecycleHardening:
    @pytest.mark.asyncio
    async def test_start_hardens_paths_and_preserves_process_group(self, tmp_path: Path) -> None:
        # A fake binary that ignores its args and just lives, so start()/stop()
        # exercise the real subprocess + native launch args + log-open path
        # without a proxy.
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
            # process_group=0 gives the proxy its own group (was preexec setpgrp).
            assert _wait_own_group(svc._process.pid)
            # Every secret is owner-only despite the wide-open umask.
            assert _mode(tmp_path / "proxy.log") == 0o600
            assert _mode(tmp_path / "proxy-config.yaml") == 0o600
            assert _mode(tmp_path / "auth") == 0o700
        finally:
            await svc.stop()

        # Teardown reaped the child.
        assert svc._process is None
