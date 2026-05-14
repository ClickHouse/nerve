"""houseofagents binary lifecycle — download, version check, config generation."""

from __future__ import annotations

import io
import logging
import os
import platform
import shutil
import stat
import tarfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from nerve.config import NerveConfig

logger = logging.getLogger(__name__)

GITHUB_RELEASES_API = (
    "https://api.github.com/repos/ClickHouse/houseofagents/releases/latest"
)

_ARCH_MAP: dict[str, str] = {
    "x86_64": "linux-amd64",
    "aarch64": "linux-arm64",
    "arm64": "linux-arm64",
    "AMD64": "windows-amd64",
}


def _detect_asset_suffix() -> str:
    system = platform.system().lower()
    machine = platform.machine()
    if system == "darwin":
        return "darwin-arm64" if machine in ("arm64", "aarch64") else "darwin-amd64"
    if system == "linux":
        mapped = _ARCH_MAP.get(machine)
        if mapped:
            return mapped
    raise RuntimeError(f"Unsupported platform: {system}/{machine}")


class HoAService:
    """Manages the houseofagents binary and configuration."""

    def __init__(self, config: NerveConfig) -> None:
        self.config = config
        self._hoa = config.houseofagents

    # ------------------------------------------------------------------ #
    #  Binary management                                                  #
    # ------------------------------------------------------------------ #

    def is_available(self) -> bool:
        """Return True when the binary exists and is executable."""
        binary = self._hoa.binary_path.expanduser()
        return binary.exists() and os.access(binary, os.X_OK)

    async def ensure_binary(self) -> Path:
        """Download or compile the binary if it is missing."""
        binary = self._hoa.binary_path.expanduser()
        if binary.exists() and os.access(binary, os.X_OK):
            return binary

        logger.info("houseofagents binary not found at %s — installing…", binary)

        try:
            await self._download_binary(binary)
        except Exception as dl_err:
            logger.warning("Binary download failed (%s), trying cargo install…", dl_err)
            await self._cargo_install(binary)

        return binary

    async def _download_binary(self, dest: Path) -> None:
        """Download the latest release from GitHub."""
        import httpx

        suffix = _detect_asset_suffix()
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(GITHUB_RELEASES_API, timeout=30)
            resp.raise_for_status()
            release: dict[str, Any] = resp.json()

        tag = release.get("tag_name", "unknown")
        logger.info("Latest houseofagents release: %s", tag)

        asset_url: str | None = None
        for asset in release.get("assets", []):
            name: str = asset["name"]
            if suffix in name and (name.endswith(".tar.gz") or name.endswith(".zip")):
                asset_url = asset["browser_download_url"]
                break

        if not asset_url:
            raise RuntimeError(
                f"No houseofagents asset for {suffix} in release {tag}"
            )

        logger.info("Downloading %s", asset_url)
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(asset_url, timeout=300)
            resp.raise_for_status()
            archive_bytes = resp.content

        dest.parent.mkdir(parents=True, exist_ok=True)

        with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
            binary_member = None
            for member in tar.getmembers():
                if member.name.endswith("houseofagents") and member.isfile():
                    binary_member = member
                    break
            if binary_member is None:
                raise RuntimeError("houseofagents binary not found in archive")
            extracted = tar.extractfile(binary_member)
            if extracted is None:
                raise RuntimeError("Failed to extract houseofagents")
            dest.write_bytes(extracted.read())

        dest.chmod(dest.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        logger.info("Installed houseofagents to %s", dest)

    async def _cargo_install(self, dest: Path) -> None:
        """Compile from source via ``cargo install``.  Fallback for aarch64."""
        import asyncio

        # shutil.which may miss ~/.cargo/bin if PATH is restricted (e.g. cron)
        cargo = shutil.which("cargo")
        if not cargo:
            home_cargo = Path.home() / ".cargo" / "bin" / "cargo"
            if home_cargo.exists():
                cargo = str(home_cargo)
        if not cargo:
            raise RuntimeError(
                "Neither pre-built binary nor Rust toolchain found.  "
                "Install Rust (https://rustup.rs) or provide a binary manually."
            )

        logger.info("Compiling houseofagents from source (this may take a few minutes)…")
        dest.parent.mkdir(parents=True, exist_ok=True)

        proc = await asyncio.create_subprocess_exec(
            cargo, "install", "houseofagents",
            "--root", str(dest.parent.parent),   # cargo puts binary in <root>/bin/
            "--force",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(
                f"cargo install failed (exit {proc.returncode}): {stderr.decode(errors='replace')}"
            )
        logger.info("Compiled houseofagents via cargo install")

    # ------------------------------------------------------------------ #
    #  Version                                                             #
    # ------------------------------------------------------------------ #

    async def get_version(self) -> str | None:
        """Run ``houseofagents --help`` and try to extract version info."""
        import asyncio

        binary = self._hoa.binary_path.expanduser()
        if not binary.exists():
            return None
        try:
            proc = await asyncio.create_subprocess_exec(
                str(binary), "--help",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await proc.communicate()
            # houseofagents may not have --version; parse first line of --help
            first_line = stdout.decode(errors="replace").split("\n")[0].strip()
            return first_line or "unknown"
        except Exception:
            return None

    # ------------------------------------------------------------------ #
    #  Config generation                                                   #
    # ------------------------------------------------------------------ #

    def generate_config(self) -> Path:
        """Write a houseofagents config.toml seeded from Nerve's API keys."""
        config_path = self._hoa.config_path.expanduser()
        if config_path.exists():
            logger.debug("houseofagents config already exists at %s", config_path)
            return config_path

        config_path.parent.mkdir(parents=True, exist_ok=True)

        agents: list[str] = []

        # Anthropic agent — grant workspace access via --add-dir
        anthropic_key = self.config.anthropic_api_key
        workspace_dir = str(self.config.workspace.expanduser())
        # --add-dir grants filesystem access, --permission-mode acceptEdits
        # auto-approves file writes (no TTY for interactive prompts in HoA)
        extra_args = f'--add-dir {workspace_dir} --permission-mode acceptEdits' if self._hoa.use_cli else ''
        agents.append(
            f'[[agents]]\n'
            f'name = "Claude"\n'
            f'provider = "anthropic"\n'
            f'api_key = "{anthropic_key}"\n'
            f'model = "claude-opus-4-7"\n'
            f'thinking_effort = "high"\n'
            f'use_cli = {str(self._hoa.use_cli).lower()}\n'
            f'extra_cli_args = "{extra_args}"\n'
        )

        # OpenAI agent (if key available)
        openai_key = self.config.openai_api_key
        if openai_key:
            agents.append(
                f'[[agents]]\n'
                f'name = "OpenAI"\n'
                f'provider = "openai"\n'
                f'api_key = "{openai_key}"\n'
                f'model = "gpt-4.1"\n'
                f'use_cli = false\n'
                f'extra_cli_args = ""\n'
            )

        output_dir = str(Path("~/.nerve/houseofagents/output").expanduser())
        toml_content = (
            f'output_dir = "{output_dir}"\n'
            f'default_max_tokens = 4096\n'
            f'max_history_messages = 50\n'
            f'http_timeout_seconds = 120\n'
            f'cli_timeout_seconds = 600\n'
            f'max_history_bytes = 102400\n'
            f'\n'
            + "\n".join(agents)
        )

        config_path.write_text(toml_content, encoding="utf-8")
        logger.info("Generated houseofagents config at %s", config_path)
        return config_path
