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

from nerve import paths
from nerve.utils.fs import atomic_write_text

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


# ---------------------------------------------------------------------------- #
#  Owner-only creation of the proxy's credential + log files                    #
# ---------------------------------------------------------------------------- #
#
# The proxy config embeds an API key, the auth dir holds OAuth token JSON, and
# both the stdout/stderr log and the proxy's own per-request error logs can
# contain prompt/response text. All of it must be readable only by the owner,
# even though it lives under Nerve's state dir, which is shared with non-secret
# files (databases, the PID file) and is not itself owner-only.
#
# The proxy and login children are launched with native, child-only ``umask``
# and ``process_group`` subprocess arguments rather than a ``preexec_fn``
# callback: running arbitrary Python between fork and exec is documented as
# unsafe in a process that has threads (which the daemon does), so the callback
# is avoided in favour of the equivalent kwargs the stdlib runs in C.


def _ensure_private_dir(path: Path) -> None:
    """Ensure ``path`` is an owner-only (0700) directory, creating it if absent.

    Used for the proxy's auth directory, which holds OAuth token JSON. It sits
    under Nerve's shared state dir, so it must not inherit that dir's
    group/other-readable mode.

    Deliberately narrow, so the blast radius stays on the proxy's own directory:

    * Only this directory is touched. Missing parents are created like
      ``mkdir -p`` but never re-permissioned — tightening a shared parent such
      as ``~/.nerve`` would reach far beyond the proxy.
    * A symlinked *directory* is not chased. ``chmod`` acts on a link's target,
      so a symlinked auth dir would silently re-permission whatever it points
      at — possibly a shared directory the operator linked in on purpose. We
      leave it untouched, with a warning. (This is specific to the directory;
      the config and log writers do follow a symlinked path to its target, the
      way a normal write would.)
    * Failing to ``chmod`` our own directory is logged, not raised: newly
      created files inside it are still owner-only via the child umask, and a
      directory already at 0700 is unaffected. This is not a promise that
      pre-existing contents are private — only that start-up need not abort.
    """
    if path.is_symlink():
        logger.warning(
            "Proxy directory %s is a symlink; leaving its target's permissions "
            "untouched. Point it at an owner-only (0700) directory.", path,
        )
        return
    path.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path, 0o700)
    except OSError as exc:
        logger.warning("Could not set 0700 on proxy directory %s: %s", path, exc)


def _open_private_append_log(path: Path) -> io.TextIOWrapper:
    """Open ``path`` for appending as an owner-only (0600) file.

    The proxy's stdout/stderr can carry request and response fragments, so its
    log is treated like the credential files. ``os.open`` applies the mode only
    when it *creates* the file, so an existing log left at a looser mode by an
    earlier run is tightened explicitly with ``fchmod``. A ``chmod`` failure is
    raised rather than swallowed — writing secrets into a log whose privacy we
    could not establish is exactly what this guards against. The parent (Nerve's
    state dir) is created if missing but never re-permissioned.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.fchmod(fd, 0o600)
    except OSError:
        os.close(fd)
        raise
    return os.fdopen(fd, "a")


class ProxyService:
    """Manages the CLIProxyAPI subprocess lifecycle."""

    def __init__(self, config: NerveConfig) -> None:
        self.config = config
        self._process: asyncio.subprocess.Process | None = None
        self._config_path = paths.nerve_path("cli-proxy-config.yaml")

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

        def _extract_and_install() -> None:
            """Untar + write the binary (CPU + disk) — off the event loop."""
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

        await asyncio.to_thread(_extract_and_install)
        logger.info("Installed CLIProxyAPI to %s", dest)

    # ------------------------------------------------------------------ #
    #  Configuration                                                      #
    # ------------------------------------------------------------------ #

    def _write_proxy_config(self) -> Path:
        """Write the proxy's own config.yaml and return its path."""
        auth_dir = self.config.proxy.auth_dir.expanduser()
        # Create the auth directory owner-only *before* the proxy writes any
        # OAuth token JSON into it. The proxy's own files — token JSON, and the
        # error logs it writes (whose exact location varies by proxy version) —
        # are further constrained to owner-only by the child umask set at launch.
        _ensure_private_dir(auth_dir)

        proxy_cfg: dict[str, Any] = {
            "host": self.config.proxy.host,
            "port": self.config.proxy.port,
            "auth-dir": str(auth_dir),
            "api-keys": [self.config.proxy.api_key],
            "debug": False,
            "request-retry": 3,
        }

        # Register a local Ollama server as an OpenAI-compatible upstream so
        # its models become selectable. CLIProxyAPI translates the Anthropic
        # requests the SDK emits into OpenAI calls against Ollama's /v1 API.
        ollama_provider = self._build_ollama_provider()
        if ollama_provider is not None:
            proxy_cfg["openai-compatibility"] = [ollama_provider]

        # The config embeds the local proxy API key, so write it owner-only and
        # atomically: the bytes are created 0600 before any content lands, and a
        # crash can never leave a half-written or briefly world-readable file.
        # Passing an explicit mode also repairs a config left at a looser mode by
        # an earlier run, since the file is re-created as a fresh 0600 inode.
        content = yaml.safe_dump(proxy_cfg, default_flow_style=False, sort_keys=False)
        atomic_write_text(self._config_path, content, mode=0o600)

        return self._config_path

    def _build_ollama_provider(self) -> dict[str, Any] | None:
        """Build the CLIProxyAPI ``openai-compatibility`` entry for Ollama.

        Returns ``None`` when Ollama is disabled or no models are installed.
        Models are auto-discovered from Ollama's ``/api/tags`` so the picker
        reflects whatever is pulled locally. Each model is exposed under its
        own name as the alias the client selects.
        """
        ollama = self.config.ollama
        if not ollama.enabled:
            return None

        from nerve.ollama import discover_models

        models = discover_models(ollama.base_url)
        if not models:
            logger.warning(
                "Ollama enabled but no models discovered at %s — the local "
                "server may be down or have no models pulled. Skipping the "
                "Ollama proxy upstream.",
                ollama.base_url,
            )
            return None

        logger.info(
            "Registering Ollama upstream (%s) with %d model(s): %s",
            ollama.openai_base_url, len(models), ", ".join(models),
        )
        return {
            "name": "ollama",
            "base-url": ollama.openai_base_url,
            # Ollama ignores the API key, but CLIProxyAPI requires an entry.
            "api-key-entries": [{"api-key": "ollama"}],
            "models": [{"name": m, "alias": m} for m in models],
        }

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                          #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Start the CLIProxyAPI subprocess."""
        binary = await self.ensure_binary()
        config_path = await asyncio.to_thread(self._write_proxy_config)

        log_file = self.config.proxy.log_file.expanduser()

        log_fd = await asyncio.to_thread(_open_private_append_log, log_file)

        try:
            self._process = await asyncio.create_subprocess_exec(
                str(binary),
                "--config", str(config_path),
                stdout=log_fd,
                stderr=log_fd,
                # Native, child-only launch policy (no preexec_fn — unsafe in a
                # threaded process): its own process group so signals aimed at
                # Nerve's group miss the proxy, and a 0077 umask so the token
                # JSON and error logs the proxy creates land owner-only. The
                # daemon's own umask and process group are untouched.
                umask=0o077,
                process_group=0,
            )
        finally:
            # The child has its own inherited dup of the log fd; the parent's
            # copy is no longer needed, whether or not the launch succeeded.
            log_fd.close()
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
            # Native, child-only 0077 umask (no preexec_fn) so the OAuth token
            # JSON is written owner-only. Process group is left as-is: login runs
            # in the foreground and streams the OAuth URL to the operator.
            umask=0o077,
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
