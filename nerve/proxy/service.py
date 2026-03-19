"""CLIProxyAPI lifecycle management.

Downloads, configures, starts and stops the CLIProxyAPI binary which routes
Anthropic API calls through Claude Code's OAuth authentication.

See: https://github.com/router-for-me/CLIProxyAPI
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import platform
import signal
import stat
import tarfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from nerve.config import NerveConfig

logger = logging.getLogger(__name__)

GITHUB_RELEASES_API = "https://api.github.com/repos/router-for-me/CLIProxyAPI/releases/latest"

# Map platform.machine() → GitHub release asset suffix.
_ARCH_MAP: dict[str, str] = {
    "x86_64": "linux_amd64",
    "aarch64": "linux_arm64",
    "arm64": "linux_arm64",      # macOS-style
    "AMD64": "windows_amd64",    # Windows
}


def _detect_asset_suffix() -> str:
    """Return the GitHub release asset suffix for the current platform."""
    system = platform.system().lower()
    machine = platform.machine()

    if system == "darwin":
        return "darwin_arm64" if machine in ("arm64", "aarch64") else "darwin_amd64"
    if system == "linux":
        mapped = _ARCH_MAP.get(machine)
        if mapped:
            return mapped
    raise RuntimeError(f"Unsupported platform: {system}/{machine}")


class ProxyService:
    """Manages the CLIProxyAPI subprocess lifecycle."""

    def __init__(self, config: NerveConfig) -> None:
        self.config = config
        self._process: asyncio.subprocess.Process | None = None
        self._config_path = Path("~/.nerve/cli-proxy-config.yaml").expanduser()

    # ------------------------------------------------------------------ #
    #  Binary management                                                  #
    # ------------------------------------------------------------------ #

    async def ensure_binary(self) -> Path:
        """Ensure the CLIProxyAPI binary exists. Download if missing."""
        binary = self.config.proxy.binary_path.expanduser()
        if binary.exists() and os.access(binary, os.X_OK):
            return binary

        logger.info("CLIProxyAPI binary not found at %s — downloading...", binary)
        await self._download_binary(binary)
        return binary

    async def _download_binary(self, dest: Path) -> None:
        """Download the latest CLIProxyAPI release from GitHub."""
        import httpx

        suffix = _detect_asset_suffix()

        # Fetch latest release metadata.
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(GITHUB_RELEASES_API, timeout=30)
            resp.raise_for_status()
            release: dict[str, Any] = resp.json()

        tag = release.get("tag_name", "unknown")
        logger.info("Latest CLIProxyAPI release: %s", tag)

        # Find matching asset.
        asset_url: str | None = None
        for asset in release.get("assets", []):
            name: str = asset["name"]
            if suffix in name and name.endswith(".tar.gz"):
                asset_url = asset["browser_download_url"]
                break

        if not asset_url:
            raise RuntimeError(
                f"No CLIProxyAPI asset found for {suffix} in release {tag}"
            )

        logger.info("Downloading %s", asset_url)
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(asset_url, timeout=120)
            resp.raise_for_status()
            archive_bytes = resp.content

        # Extract the binary from the tarball.
        dest.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
            # The binary is named "cli-proxy-api" inside the archive.
            binary_member = None
            for member in tar.getmembers():
                if member.name.endswith("cli-proxy-api") and member.isfile():
                    binary_member = member
                    break

            if binary_member is None:
                raise RuntimeError("cli-proxy-api binary not found in archive")

            extracted = tar.extractfile(binary_member)
            if extracted is None:
                raise RuntimeError("Failed to extract cli-proxy-api from archive")

            dest.write_bytes(extracted.read())

        # Make executable.
        dest.chmod(dest.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        logger.info("Installed CLIProxyAPI to %s", dest)

    # ------------------------------------------------------------------ #
    #  Configuration                                                      #
    # ------------------------------------------------------------------ #

    def _write_proxy_config(self) -> Path:
        """Write the proxy's own config.yaml and return its path."""
        auth_dir = self.config.proxy.auth_dir.expanduser()
        auth_dir.mkdir(parents=True, exist_ok=True)

        proxy_cfg: dict[str, Any] = {
            "host": self.config.proxy.host,
            "port": self.config.proxy.port,
            "auth-dir": str(auth_dir),
            "api-keys": [self.config.proxy.api_key],
            "debug": False,
            "request-retry": 3,
        }

        self._config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._config_path, "w") as f:
            yaml.safe_dump(proxy_cfg, f, default_flow_style=False, sort_keys=False)

        return self._config_path

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                          #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Start the CLIProxyAPI subprocess."""
        binary = await self.ensure_binary()
        config_path = self._write_proxy_config()

        log_file = self.config.proxy.log_file.expanduser()
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_fd = open(log_file, "a")  # noqa: SIM115

        self._process = await asyncio.create_subprocess_exec(
            str(binary),
            "--config", str(config_path),
            stdout=log_fd,
            stderr=log_fd,
            # Detach from parent's process group so it doesn't get random signals.
            preexec_fn=os.setpgrp,
        )
        logger.info(
            "CLIProxyAPI started (pid=%d, port=%d)",
            self._process.pid, self.config.proxy.port,
        )

        # Wait for the proxy to become healthy.
        healthy = await self._wait_for_healthy(timeout=15)
        if not healthy:
            await self.stop()
            raise RuntimeError(
                f"CLIProxyAPI failed to become healthy within 15s. "
                f"Check logs: {log_file}"
            )

    async def stop(self) -> None:
        """Stop the CLIProxyAPI subprocess gracefully."""
        proc = self._process
        if proc is None or proc.returncode is not None:
            return

        logger.info("Stopping CLIProxyAPI (pid=%d)...", proc.pid)
        try:
            proc.send_signal(signal.SIGTERM)
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                logger.warning("CLIProxyAPI didn't stop within 5s — sending SIGKILL")
                proc.kill()
                await proc.wait()
        except ProcessLookupError:
            pass  # Already dead.
        finally:
            self._process = None

        logger.info("CLIProxyAPI stopped")

    # ------------------------------------------------------------------ #
    #  Health                                                             #
    # ------------------------------------------------------------------ #

    async def is_healthy(self) -> bool:
        """Check if the proxy is responding."""
        try:
            import httpx
            url = f"http://{self.config.proxy.host}:{self.config.proxy.port}/v1/models"
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    url,
                    headers={"x-api-key": self.config.proxy.api_key},
                    timeout=3,
                )
                return resp.status_code == 200
        except Exception:
            return False

    async def _wait_for_healthy(self, timeout: float = 15) -> bool:
        """Poll the health endpoint until it responds or timeout."""
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            if await self.is_healthy():
                return True
            # Check if process died.
            if self._process and self._process.returncode is not None:
                logger.error(
                    "CLIProxyAPI exited with code %d", self._process.returncode,
                )
                return False
            await asyncio.sleep(0.5)
        return False

    # ------------------------------------------------------------------ #
    #  OAuth login (interactive — for setup wizard)                       #
    # ------------------------------------------------------------------ #

    async def login(self, no_browser: bool = True) -> bool:
        """Run the OAuth login flow. Returns True on success."""
        binary = await self.ensure_binary()
        self._write_proxy_config()

        cmd = [
            str(binary),
            "--claude-login",
            "--config", str(self._config_path),
        ]
        if no_browser:
            cmd.append("--no-browser")

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        # Stream output so the user can see the OAuth URL.
        assert proc.stdout is not None
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            print(line.decode(errors="replace"), end="", flush=True)

        await proc.wait()
        return proc.returncode == 0
