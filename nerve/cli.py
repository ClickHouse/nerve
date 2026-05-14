"""CLI commands for Nerve.

Usage:
    nerve init           First-run setup wizard (interactive)
    nerve start          Start the Nerve server (daemonized by default)
    nerve stop           Stop the running Nerve daemon
    nerve restart        Restart the Nerve daemon
    nerve status         Show daemon status
    nerve upgrade        Pull latest code, install deps, rebuild frontend
    nerve doctor         Check config, DB, API keys, connectivity
    nerve sync [source]  Run sync manually
    nerve cron [job_id]  Run a cron job manually
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import click

from nerve.config import load_config, set_config

# Default PID file location
PID_DIR = Path("~/.nerve").expanduser()
PID_FILE = PID_DIR / "nerve.pid"
CONFIG_DIR_FILE = PID_DIR / "config_dir"
LOG_FILE = PID_DIR / "nerve.log"


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # Quiet noisy loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)


# --- PID file helpers ---

def _read_pid() -> int | None:
    """Read PID from file. Returns None if no valid PID file."""
    try:
        pid = int(PID_FILE.read_text().strip())
        return pid
    except (FileNotFoundError, ValueError):
        return None


def _is_running(pid: int) -> bool:
    """Check if a process with the given PID is alive."""
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # Process exists but we can't signal it


def _write_pid(pid: int, config_dir: Path | None = None) -> None:
    PID_DIR.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(pid))
    if config_dir is not None:
        CONFIG_DIR_FILE.write_text(str(config_dir.resolve()))


def _read_config_dir() -> Path | None:
    """Read stored config directory. Returns None if not available."""
    try:
        return Path(CONFIG_DIR_FILE.read_text().strip())
    except (FileNotFoundError, ValueError):
        return None


def _remove_pid() -> None:
    PID_FILE.unlink(missing_ok=True)


def _get_daemon_status() -> tuple[bool, int | None]:
    """Returns (is_running, pid)."""
    pid = _read_pid()
    if pid is None:
        return False, None
    if _is_running(pid):
        return True, pid
    # Stale PID file
    _remove_pid()
    return False, None


# --- Systemd helpers ---

def _is_systemd_managed() -> bool:
    """Check if the Nerve daemon is running under systemd."""
    return os.environ.get("INVOCATION_ID") is not None


# --- Docker Compose helpers ---

def _is_docker_mode(config) -> bool:
    """Check if CLI should proxy commands to Docker Compose.

    True when config.deployment == "docker" AND we are NOT inside
    the container (NERVE_DOCKER env var is not set).
    """
    deployment = getattr(config, "deployment", "server")
    if deployment != "docker":
        return False
    # Inside the container, NERVE_DOCKER=1 — don't proxy to self
    if os.environ.get("NERVE_DOCKER") == "1":
        return False
    return True


def _find_compose_file(config_dir: str | Path) -> Path:
    """Locate docker-compose.yml or raise."""
    compose_file = Path(config_dir) / "docker-compose.yml"
    if not compose_file.exists():
        raise click.ClickException(
            f"docker-compose.yml not found in {config_dir}\n"
            "Run 'nerve init' to generate Docker files."
        )
    return compose_file


def _docker_compose(
    config_dir: str | Path,
    args: list[str],
    replace_process: bool = False,
) -> int:
    """Run a docker compose command.

    Args:
        config_dir: Directory containing docker-compose.yml.
        args: Arguments after 'docker compose' (e.g. ["up", "-d"]).
        replace_process: Use os.execvp (for streaming commands like logs).

    Returns:
        Exit code (0 if replace_process since execvp never returns).
    """
    compose_file = _find_compose_file(config_dir)

    if not shutil.which("docker"):
        raise click.ClickException(
            "Docker not found. Install Docker: https://docs.docker.com/get-docker/"
        )

    cmd = ["docker", "compose", "-f", str(compose_file)] + args

    if replace_process:
        os.execvp("docker", cmd)
        return 0  # unreachable

    result = subprocess.run(cmd, cwd=str(Path(config_dir)))
    return result.returncode


@click.group()
@click.option("--config-dir", "-c", type=click.Path(), default=".", help="Config directory")
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
@click.pass_context
def main(ctx: click.Context, config_dir: str, verbose: bool) -> None:
    """Nerve — Personal AI Assistant"""
    setup_logging(verbose)
    config = load_config(Path(config_dir))
    set_config(config)
    ctx.ensure_object(dict)
    ctx.obj["config"] = config
    ctx.obj["config_dir"] = config_dir
    ctx.obj["verbose"] = verbose


@main.command()
@click.option("--if-needed", is_flag=True, help="Only run if fresh install detected")
@click.option("--non-interactive", is_flag=True, help="Use env vars, no prompts (for Docker)")
@click.option("--inside-docker", is_flag=True, hidden=True, help="Skip deployment step (running inside Docker)")
@click.pass_context
def init(ctx: click.Context, if_needed: bool, non_interactive: bool, inside_docker: bool) -> None:
    """First-run setup wizard — configure Nerve interactively."""
    from nerve.bootstrap import SetupWizard, is_fresh_install, run_non_interactive

    config_dir = Path(ctx.obj["config_dir"])

    if if_needed and not is_fresh_install(config_dir):
        return  # Already configured, exit silently

    if not is_fresh_install(config_dir):
        if non_interactive:
            click.echo("Nerve is already configured. Skipping.")
            return
        if not click.confirm("Nerve is already configured. Re-run setup? (Config files will be overwritten, workspace files won't.)"):
            return

    if non_interactive:
        run_non_interactive(config_dir)
    else:
        wizard = SetupWizard(config_dir, inside_docker=inside_docker)
        wizard.run()

    # Reload config after wizard writes files
    config = load_config(config_dir)
    set_config(config)
    ctx.obj["config"] = config


@main.command()
@click.option("--foreground", "-f", is_flag=True, help="Run in foreground (don't daemonize)")
@click.pass_context
def start(ctx: click.Context, foreground: bool) -> None:
    """Start the Nerve server."""
    config_dir = Path(ctx.obj["config_dir"])
    config = ctx.obj["config"]

    # Detect fresh install — offer to run setup wizard
    from nerve.bootstrap import is_fresh_install
    if is_fresh_install(config_dir):
        if foreground:
            click.echo("Fresh install detected — running setup wizard...")
            ctx.invoke(init)
            # Reload config after wizard
            config = ctx.obj["config"]
        else:
            click.echo("Fresh install detected. Run 'nerve init' first, or 'nerve start -f' for guided setup.")
            ctx.exit(1)
            return

    # Docker mode: proxy to docker compose
    if _is_docker_mode(config):
        if foreground:
            _docker_compose(config_dir, ["up"], replace_process=True)
        else:
            rc = _docker_compose(config_dir, ["up", "-d"])
            if rc == 0:
                click.echo("Nerve started (Docker)")
                click.echo(f"  Listening on http://localhost:{config.gateway.port}")
                click.echo("  Use 'nerve logs' to follow logs")
            ctx.exit(rc)
        return

    # Check if already running
    running, pid = _get_daemon_status()
    if running:
        click.echo(f"Nerve is already running (PID {pid})")
        ctx.exit(1)
        return

    if foreground:
        # Run directly in this process
        _write_pid(os.getpid(), config_dir=config_dir)
        try:
            from nerve.gateway.server import run_server
            click.echo(f"Starting Nerve on {config.gateway.host}:{config.gateway.port}")
            run_server(config)
        finally:
            _remove_pid()
    else:
        # Daemonize: spawn a background process
        config_dir = ctx.obj["config_dir"]
        verbose = ctx.obj["verbose"]

        # Build the command to run nerve start --foreground
        nerve_bin = sys.argv[0]
        cmd = [sys.executable, nerve_bin, "-c", config_dir]
        if verbose:
            cmd.append("-v")
        cmd.extend(["start", "--foreground"])

        # Ensure log directory exists
        PID_DIR.mkdir(parents=True, exist_ok=True)

        log_fd = open(LOG_FILE, "a")
        proc = subprocess.Popen(
            cmd,
            stdout=log_fd,
            stderr=log_fd,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
        log_fd.close()

        # Wait briefly to verify the process started
        time.sleep(1)
        if proc.poll() is not None:
            click.echo(f"Nerve failed to start (exit code {proc.returncode})")
            click.echo(f"Check logs: {LOG_FILE}")
            ctx.exit(1)
            return

        click.echo(f"Nerve started (PID {proc.pid})")
        click.echo(f"  Listening on {config.gateway.host}:{config.gateway.port}")
        click.echo(f"  Logs: {LOG_FILE}")


@main.command()
@click.pass_context
def stop(ctx: click.Context) -> None:
    """Stop the running Nerve daemon."""
    config = ctx.obj["config"]
    config_dir = Path(ctx.obj["config_dir"])

    # Docker mode: proxy to docker compose
    if _is_docker_mode(config):
        rc = _docker_compose(config_dir, ["down"])
        if rc == 0:
            click.echo("Nerve stopped (Docker)")
        ctx.exit(rc)
        return

    running, pid = _get_daemon_status()
    if not running:
        click.echo("Nerve is not running")
        return

    click.echo(f"Stopping Nerve (PID {pid})...")
    os.kill(pid, signal.SIGTERM)

    # Wait for graceful shutdown (up to 15 seconds)
    for i in range(30):
        time.sleep(0.5)
        if not _is_running(pid):
            _remove_pid()
            click.echo("Nerve stopped")
            return

    # Force kill if still running
    click.echo("Graceful shutdown timed out, sending SIGKILL...")
    try:
        os.kill(pid, signal.SIGKILL)
        time.sleep(0.5)
    except ProcessLookupError:
        pass
    _remove_pid()
    click.echo("Nerve killed")


@main.command()
@click.pass_context
def restart(ctx: click.Context) -> None:
    """Restart the Nerve daemon.

    Spawns a detached helper process that stops the old daemon and starts a
    new one.  This ensures restarts work even when triggered from *inside*
    the running daemon (e.g. via the Telegram bot / agent), because the
    helper runs in its own session and survives the parent's death.
    """
    config = ctx.obj["config"]
    config_dir = Path(ctx.obj["config_dir"])

    # Prefer the config dir stored by the running daemon — it's an absolute
    # path that doesn't depend on the caller's CWD.
    stored = _read_config_dir()
    if stored is not None and stored.is_dir():
        config_dir = stored

    # Docker mode: proxy to docker compose
    if _is_docker_mode(config):
        rc = _docker_compose(config_dir, ["restart"])
        if rc == 0:
            click.echo("Nerve restarted (Docker)")
        ctx.exit(rc)
        return

    # Systemd mode (Restart=always): just kill the process — systemd
    # will bring it back automatically.
    if _is_systemd_managed():
        running, old_pid = _get_daemon_status()
        if running:
            click.echo(f"Restarting Nerve (PID {old_pid})... systemd will respawn.")
            os.kill(old_pid, signal.SIGTERM)
        else:
            click.echo("Nerve is not running — systemd will start it shortly.")
        return

    # Build the command that `start` would use to launch the daemon.
    # Always use ``-m nerve`` so the restart works regardless of how *this*
    # process was invoked (console-script, ``python -m nerve``, etc.).
    verbose = ctx.obj["verbose"]
    start_cmd_parts = [sys.executable, "-m", "nerve", "-c", str(config_dir)]
    if verbose:
        start_cmd_parts.append("-v")
    start_cmd_parts.extend(["start", "--foreground"])

    running, old_pid = _get_daemon_status()

    # Spawn a detached helper that: waits for old PID to exit, then starts
    # a new daemon.  Written as an inline Python script so we don't need an
    # external shell script on disk.
    helper_script = (
        "import os, signal, subprocess, sys, time\n"
        f"old_pid = {old_pid if running else 'None'}\n"
        f"pid_file = {str(PID_FILE)!r}\n"
        f"log_file = {str(LOG_FILE)!r}\n"
        f"start_cmd = {start_cmd_parts!r}\n"
        "if old_pid is not None:\n"
        "    try:\n"
        "        os.kill(old_pid, signal.SIGTERM)\n"
        "    except ProcessLookupError:\n"
        "        pass\n"
        "    for _ in range(30):\n"
        "        time.sleep(0.5)\n"
        "        try:\n"
        "            os.kill(old_pid, 0)\n"
        "        except ProcessLookupError:\n"
        "            break\n"
        "    else:\n"
        "        try:\n"
        "            os.kill(old_pid, signal.SIGKILL)\n"
        "            time.sleep(0.5)\n"
        "        except ProcessLookupError:\n"
        "            pass\n"
        "    # Remove stale PID file\n"
        "    try:\n"
        "        os.unlink(pid_file)\n"
        "    except FileNotFoundError:\n"
        "        pass\n"
        "time.sleep(0.5)\n"
        "log_fd = open(log_file, 'a')\n"
        "proc = subprocess.Popen(\n"
        "    start_cmd,\n"
        "    stdout=log_fd,\n"
        "    stderr=log_fd,\n"
        "    stdin=subprocess.DEVNULL,\n"
        "    start_new_session=True,\n"
        ")\n"
        "log_fd.close()\n"
        "time.sleep(1)\n"
        "if proc.poll() is not None:\n"
        "    sys.exit(1)\n"
    )

    log_fd = open(LOG_FILE, "a")
    subprocess.Popen(
        [sys.executable, "-c", helper_script],
        stdout=log_fd,
        stderr=log_fd,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    log_fd.close()

    if running:
        click.echo(f"Restarting Nerve (PID {old_pid})... new instance will start shortly.")
    else:
        click.echo("Starting Nerve... new instance will start shortly.")


@main.command()
@click.option("--follow", "-f", is_flag=True, help="Follow log output (like tail -f)")
@click.pass_context
def status(ctx: click.Context, follow: bool) -> None:
    """Show Nerve daemon status."""
    config = ctx.obj["config"]
    config_dir = Path(ctx.obj["config_dir"])

    # Docker mode: proxy to docker compose ps
    if _is_docker_mode(config):
        rc = _docker_compose(config_dir, ["ps"])
        if follow:
            _docker_compose(config_dir, ["logs", "-f"], replace_process=True)
        ctx.exit(rc)
        return

    running, pid = _get_daemon_status()

    if running:
        click.echo(f"Nerve is running (PID {pid})")

        # Show some process info
        try:
            import resource
            # Get memory via /proc on Linux
            proc_status = Path(f"/proc/{pid}/status")
            if proc_status.exists():
                for line in proc_status.read_text().splitlines():
                    if line.startswith("VmRSS:"):
                        mem = line.split(":")[1].strip()
                        click.echo(f"  Memory: {mem}")
                        break
            # Get uptime from /proc
            proc_stat = Path(f"/proc/{pid}/stat")
            if proc_stat.exists():
                stat = proc_stat.read_text().split()
                # Field 22 is start time in clock ticks
                try:
                    boot_time = float(Path("/proc/stat").read_text().split("btime ")[1].split()[0])
                    start_ticks = int(stat[21])
                    clk_tck = os.sysconf("SC_CLK_TCK")
                    start_time = boot_time + start_ticks / clk_tck
                    uptime_secs = time.time() - start_time
                    hours = int(uptime_secs // 3600)
                    mins = int((uptime_secs % 3600) // 60)
                    click.echo(f"  Uptime: {hours}h {mins}m")
                except (IndexError, ValueError, OSError):
                    pass
        except Exception:
            pass

        config = ctx.obj["config"]
        click.echo(f"  Listening on {config.gateway.host}:{config.gateway.port}")
        click.echo(f"  Logs: {LOG_FILE}")
    else:
        click.echo("Nerve is not running")

    if follow and LOG_FILE.exists():
        click.echo(f"\n--- Tailing {LOG_FILE} ---")
        try:
            os.execlp("tail", "tail", "-f", str(LOG_FILE))
        except Exception:
            click.echo("Cannot tail log file")


@main.command()
@click.pass_context
def logs(ctx: click.Context) -> None:
    """Tail the Nerve daemon log."""
    config = ctx.obj["config"]
    config_dir = Path(ctx.obj["config_dir"])

    # Docker mode: proxy to docker compose logs
    if _is_docker_mode(config):
        _docker_compose(config_dir, ["logs", "-f"], replace_process=True)
        return  # unreachable

    if not LOG_FILE.exists():
        click.echo(f"No log file at {LOG_FILE}")
        return

    click.echo(f"--- {LOG_FILE} ---")
    try:
        os.execlp("tail", "tail", "-f", str(LOG_FILE))
    except Exception:
        # Fallback: print last 50 lines
        lines = LOG_FILE.read_text().splitlines()
        for line in lines[-50:]:
            click.echo(line)


# --- Upgrade helpers ---

def _find_source_root() -> Path:
    """Locate the Nerve source checkout (the directory containing pyproject.toml).

    Nerve is installed in editable mode, so ``__file__`` points at the actual
    source tree.  The repo root is the parent of the ``nerve`` package
    directory.
    """
    return Path(__file__).resolve().parent.parent


def _pip_install_cmd(source_root: Path) -> list[str]:
    """Return the command to (re)install Nerve from source in the current venv.

    Prefers ``uv pip`` when available (faster), falls back to ``python -m pip``.
    """
    uv_bin = shutil.which("uv")
    if uv_bin:
        # ``uv pip`` targets the active venv via ``VIRTUAL_ENV`` / ``sys.executable``
        return [uv_bin, "pip", "install", "-e", str(source_root), "--python", sys.executable]
    return [sys.executable, "-m", "pip", "install", "-e", str(source_root)]


@main.command()
@click.option("--no-frontend", is_flag=True, help="Skip frontend rebuild")
@click.option("--no-deps", is_flag=True, help="Skip Python dependency install")
@click.option("--no-pull", is_flag=True, help="Skip git pull (use local changes only)")
@click.pass_context
def upgrade(ctx: click.Context, no_frontend: bool, no_deps: bool, no_pull: bool) -> None:
    """Upgrade Nerve: git pull, install Python deps, rebuild frontend.

    Operates on the source checkout that this ``nerve`` binary was installed
    from.  Restart Nerve afterwards for the changes to take effect.
    """
    config = ctx.obj["config"]

    # Docker mode: the container is immutable — upgrading means rebuilding
    # the image or pulling a new one.  Bail out with a helpful hint.
    if _is_docker_mode(config):
        click.echo(
            "Docker deployment detected.  Upgrade the image instead:\n"
            "  docker compose pull && docker compose up -d\n"
            "  (or rebuild from source: docker compose build && docker compose up -d)"
        )
        ctx.exit(1)
        return

    source_root = _find_source_root()

    if not (source_root / "pyproject.toml").exists():
        raise click.ClickException(
            f"pyproject.toml not found in {source_root}. "
            "Nerve doesn't appear to be installed from a source checkout."
        )

    click.echo(f"Upgrading Nerve from source at {source_root}")

    # Step 1: git pull
    if no_pull:
        click.echo("\n[1/3] Skipping git pull (--no-pull)")
    elif not (source_root / ".git").exists():
        click.echo(f"\n[1/3] Not a git repository ({source_root}) — skipping git pull")
    elif not shutil.which("git"):
        raise click.ClickException("git not found on PATH — install git or re-run with --no-pull")
    else:
        click.echo("\n[1/3] git pull --ff-only")
        rc = subprocess.run(["git", "pull", "--ff-only"], cwd=source_root).returncode
        if rc != 0:
            raise click.ClickException(
                "git pull failed. Resolve conflicts manually, then re-run 'nerve upgrade --no-pull'."
            )

    # Step 2: install Python dependencies
    if no_deps:
        click.echo("\n[2/3] Skipping Python dependency install (--no-deps)")
    else:
        cmd = _pip_install_cmd(source_root)
        click.echo(f"\n[2/3] Installing Python dependencies: {' '.join(cmd)}")
        rc = subprocess.run(cmd, cwd=source_root).returncode
        if rc != 0:
            raise click.ClickException("Python dependency install failed")

    # Step 3: rebuild frontend
    web_dir = source_root / "web"
    if no_frontend:
        click.echo("\n[3/3] Skipping frontend rebuild (--no-frontend)")
    elif not web_dir.exists():
        click.echo(f"\n[3/3] No web/ directory at {web_dir} — skipping frontend rebuild")
    elif not shutil.which("npm"):
        raise click.ClickException(
            "npm not found on PATH. Install Node.js or re-run with --no-frontend."
        )
    else:
        click.echo("\n[3/3] Rebuilding frontend")
        rc = subprocess.run(["npm", "install"], cwd=web_dir).returncode
        if rc != 0:
            raise click.ClickException("npm install failed")
        rc = subprocess.run(["npm", "run", "build"], cwd=web_dir).returncode
        if rc != 0:
            raise click.ClickException("npm run build failed")

    click.echo("\nUpgrade complete. Restart Nerve for changes to take effect:")
    click.echo("  nerve restart")


def doctor_report(config) -> str:
    """Run doctor checks and return the report as a plain-text string.

    Used by both the CLI ``nerve doctor`` command and the Telegram ``/doctor``
    command so they share the same diagnostics.
    """
    lines: list[str] = []
    errors: list[str] = []
    warnings: list[str] = []

    lines.append("Nerve Doctor")
    lines.append("=" * 40)

    # Daemon status
    running, pid = _get_daemon_status()
    if running:
        lines.append(f"[OK] Daemon running (PID {pid})")
    else:
        lines.append("[--] Daemon not running")

    # Check workspace
    if config.workspace.exists():
        lines.append(f"[OK] Workspace: {config.workspace}")
        mode = getattr(config, "mode", "personal")
        try:
            from nerve.workspace import get_expected_files
            expected = get_expected_files(mode)
            for f in expected:
                if (config.workspace / f).exists():
                    lines.append(f"  [OK] {f}")
                else:
                    warnings.append(f"  [WARN] {f} not found (expected for {mode} mode)")
        except (FileNotFoundError, ValueError):
            for f in ["SOUL.md", "IDENTITY.md", "MEMORY.md"]:
                if (config.workspace / f).exists():
                    lines.append(f"  [OK] {f}")
                else:
                    warnings.append(f"  [WARN] {f} not found")
    else:
        errors.append(f"[ERR] Workspace not found: {config.workspace}")

    # Check proxy
    if config.proxy.enabled:
        binary = config.proxy.binary_path.expanduser()
        if binary.exists():
            lines.append(f"[OK] CLIProxyAPI binary: {binary}")
        else:
            warnings.append(f"[WARN] CLIProxyAPI binary not found (will download on start): {binary}")
        lines.append(f"[OK] Proxy configured: {config.proxy.host}:{config.proxy.port}")
        try:
            import httpx
            resp = httpx.get(
                f"http://{config.proxy.host}:{config.proxy.port}/v1/models",
                headers={"x-api-key": config.proxy.api_key},
                timeout=3,
            )
            if resp.status_code == 200:
                lines.append("[OK] Proxy is running and healthy")
            else:
                warnings.append(f"[WARN] Proxy returned status {resp.status_code}")
        except Exception:
            warnings.append("[WARN] Proxy not running (starts with Nerve)")

    # Check API keys
    if config.proxy.enabled:
        if config.anthropic_api_key:
            lines.append("[--] Anthropic API key set (proxy takes precedence)")
        else:
            lines.append("[--] Anthropic API key not set (using proxy)")
    elif config.anthropic_api_key:
        lines.append(f"[OK] Anthropic API key: ...{config.anthropic_api_key[-4:]}")
    else:
        errors.append("[ERR] Anthropic API key not set and proxy not enabled (config.local.yaml)")

    if config.openai_api_key:
        lines.append(f"[OK] OpenAI API key: ...{config.openai_api_key[-4:]} (vector embeddings enabled)")
    else:
        lines.append("[--] OpenAI API key not set (using LLM-based memory recall)")

    # Check Telegram
    if config.telegram.enabled:
        if config.telegram.bot_token:
            lines.append(f"[OK] Telegram bot token: ...{config.telegram.bot_token[-4:]}")
        else:
            errors.append("[ERR] Telegram enabled but bot_token not set")
    else:
        lines.append("[--] Telegram disabled")

    # Check SSL
    if config.gateway.ssl.enabled:
        if config.gateway.ssl.cert and config.gateway.ssl.cert.exists():
            lines.append(f"[OK] SSL cert: {config.gateway.ssl.cert}")
        else:
            warnings.append("[WARN] SSL cert not found")
        if config.gateway.ssl.key and config.gateway.ssl.key.exists():
            lines.append(f"[OK] SSL key: {config.gateway.ssl.key}")
        else:
            warnings.append("[WARN] SSL key not found")
    else:
        lines.append("[--] SSL not configured")

    # Check auth
    if config.auth.password_hash:
        lines.append("[OK] Auth password hash configured")
    else:
        warnings.append("[WARN] Auth password not set — running in dev mode (no auth)")

    if config.auth.jwt_secret:
        lines.append("[OK] JWT secret configured")
    else:
        warnings.append("[WARN] JWT secret not set — running in dev mode")

    # Check DB
    db_path = Path("~/.nerve/nerve.db").expanduser()
    if db_path.exists():
        lines.append(f"[OK] Database: {db_path} ({db_path.stat().st_size / 1024:.1f} KB)")
    else:
        lines.append(f"[--] Database will be created at: {db_path}")

    # Check cron files (merge like the scheduler does: user jobs override system by ID)
    try:
        from nerve.cron.jobs import load_jobs

        system_jobs = load_jobs(config.cron.system_file) if config.cron.system_file.exists() else []
        user_jobs = load_jobs(config.cron.jobs_file) if config.cron.jobs_file.exists() else []

        # Merge: user jobs override system jobs with the same ID
        merged: dict[str, object] = {j.id: j for j in system_jobs}
        overridden = sum(1 for j in user_jobs if j.id in merged)
        for j in user_jobs:
            merged[j.id] = j

        all_jobs = list(merged.values())
        enabled = sum(1 for j in all_jobs if j.enabled)

        if all_jobs:
            detail = f"{enabled}/{len(all_jobs)} enabled"
            if overridden:
                detail += f", {overridden} overridden in jobs.yaml"
            lines.append(f"[OK] Cron jobs: {detail}")
        else:
            lines.append("[--] No cron jobs configured")
    except Exception:
        # Fallback: just report file existence
        if config.cron.system_file.exists():
            lines.append(f"[OK] System crons: {config.cron.system_file}")
        if config.cron.jobs_file.exists():
            lines.append(f"[OK] User crons: {config.cron.jobs_file}")

    # Check external tools
    import shutil
    for tool_name in ["gog", "gh"]:
        if shutil.which(tool_name):
            lines.append(f"[OK] {tool_name} CLI found")
        else:
            warnings.append(f"[WARN] {tool_name} CLI not found (needed for sync)")

    # Summary
    lines.append("")
    lines.extend(warnings)
    lines.extend(errors)

    if errors:
        lines.append(f"\n{len(errors)} error(s), {len(warnings)} warning(s)")
    else:
        lines.append(f"\nAll good! {len(warnings)} warning(s)")

    return "\n".join(lines)


@main.command()
@click.pass_context
def doctor(ctx: click.Context) -> None:
    """Check config, DB, API keys, and connectivity."""
    config = ctx.obj["config"]
    report = doctor_report(config)
    click.echo(report)
    if "[ERR]" in report:
        ctx.exit(1)


@main.command()
@click.argument("source", default="all")
@click.pass_context
def sync(ctx: click.Context, source: str) -> None:
    """Run source sync manually (telegram, gmail, github, or all)."""
    config = ctx.obj["config"]

    async def _run():
        from nerve.agent.engine import AgentEngine
        from nerve.db import init_db, close_db
        from nerve.sources.registry import build_source_runners

        db = await init_db()
        try:
            engine = AgentEngine(config, db)
            await engine.initialize()

            runners = build_source_runners(config, db)
            if not runners:
                click.echo("No sources configured.")
                return

            target_runners = (
                runners if source == "all"
                else [r for r in runners if r.source.source_name == source]
            )

            if not target_runners:
                available = [r.source.source_name for r in runners]
                click.echo(f"Source not found: {source} (available: {', '.join(available)})")
                return

            for runner in target_runners:
                click.echo(f"  Running: {runner.source.source_name} ...", nl=False)
                result = await runner.run()
                status = "OK" if result.error is None else "ERROR"
                click.echo(
                    f" [{status}] "
                    f"{result.records_ingested} ingested"
                    + (f" — {result.error}" if result.error else "")
                )
                # Log to source_run_log
                await db.log_source_run(
                    source=runner.source.source_name,
                    records_fetched=result.records_ingested,
                    records_processed=result.records_ingested,
                    error=result.error,
                )
        finally:
            await close_db()

    click.echo(f"Running sync: {source}")
    asyncio.run(_run())


@main.command("setup-telegram")
@click.pass_context
def setup_telegram(ctx: click.Context) -> None:
    """Authenticate Telethon for the Telegram source (interactive)."""
    config = ctx.obj["config"]

    api_id = config.sync.telegram.api_id
    api_hash = config.sync.telegram.api_hash
    if not api_id or not api_hash:
        click.echo("Error: sync.telegram.api_id and api_hash must be set in config.")
        ctx.exit(1)
        return

    session_path = os.path.expanduser("~/.nerve/telegram_sync")
    click.echo(f"Telethon session: {session_path}.session")
    click.echo(f"API ID: {api_id}")
    click.echo()

    async def _run():
        from telethon import TelegramClient, functions

        client = TelegramClient(session_path, api_id, api_hash)
        await client.start()

        me = await client.get_me()
        click.echo(f"Authenticated as: {me.first_name} (@{me.username}, ID: {me.id})")

        # Show current state for reference
        state = await client(functions.updates.GetStateRequest())
        click.echo(f"Current state: pts={state.pts}, qts={state.qts}, date={state.date}, seq={state.seq}")

        await client.disconnect()
        click.echo()
        click.echo(f"Session saved to {session_path}.session")
        click.echo("Telegram source is now ready to use.")

    asyncio.run(_run())


@main.command()
@click.argument("job_id", default="")
@click.pass_context
def cron(ctx: click.Context, job_id: str) -> None:
    """Run a cron job manually."""
    config = ctx.obj["config"]

    async def _run():
        from nerve.agent.engine import AgentEngine
        from nerve.db import init_db, close_db

        db = await init_db()
        try:
            engine = AgentEngine(config, db)
            await engine.initialize()

            if job_id:
                from nerve.cron.service import CronService
                cron_svc = CronService(config, engine, db)
                await cron_svc.run_job(job_id)
            else:
                click.echo("Available jobs:")
                from nerve.cron.jobs import load_jobs

                # Load from both files, show provenance
                system_jobs = load_jobs(config.cron.system_file)
                user_jobs = load_jobs(config.cron.jobs_file)

                # Merge (user overrides system)
                seen_ids: set[str] = set()
                all_jobs: list[tuple[str, Any]] = []  # (source_label, job)
                for j in user_jobs:
                    seen_ids.add(j.id)
                    all_jobs.append(("user", j))
                for j in system_jobs:
                    if j.id not in seen_ids:
                        all_jobs.append(("system", j))

                for source, job in all_jobs:
                    status = "enabled" if job.enabled else "disabled"
                    click.echo(
                        f"  [{source:6s}] {job.id}: "
                        f"{job.description or job.schedule} ({status})"
                    )
        finally:
            await close_db()

    asyncio.run(_run())


@main.command("migrate-openclaw")
@click.option("--sessions-dir", default="~/.openclaw/agents/main/sessions", help="OpenClaw sessions directory")
@click.option("--dry-run", is_flag=True, help="Only show what would be migrated")
@click.option("--min-messages", default=2, help="Skip sessions with fewer messages")
@click.option("--timeout", default=60, help="Per-session timeout in seconds")
@click.pass_context
def migrate_openclaw(ctx: click.Context, sessions_dir: str, dry_run: bool, min_messages: int, timeout: int) -> None:
    """Migrate OpenClaw conversations into memU. Resumes automatically — skips already-indexed sessions."""
    import json
    from pathlib import Path

    config = ctx.obj["config"]
    sessions_path = Path(sessions_dir).expanduser()
    conv_dir = Path("~/.nerve/memu-conversations").expanduser()

    if not sessions_path.exists():
        click.echo(f"[ERR] Sessions directory not found: {sessions_path}")
        ctx.exit(1)
        return

    session_files = sorted(sessions_path.glob("*.jsonl"))
    # Also include deleted/archived sessions
    deleted_files = sorted(sessions_path.glob("*.jsonl.deleted.*"))
    session_files.extend(deleted_files)
    click.echo(f"Found {len(session_files)} OpenClaw sessions in {sessions_path} ({len(deleted_files)} deleted)")

    # Build set of already-indexed session IDs by checking which conversation
    # files are actually registered as resources in the memU database.
    # (Files may exist on disk from timed-out runs that never completed indexing.)
    already_done: set[str] = set()
    try:
        import sqlite3
        db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
        db = sqlite3.connect(db_path)
        for (url,) in db.execute("SELECT url FROM memu_resources WHERE url LIKE '%/session-%'"):
            # url looks like /home/.../.nerve/memu-conversations/session-{uuid}-{ts}.json
            fname = Path(url).stem  # session-{uuid}-{ts}
            parts = fname.split("-", 1)
            if len(parts) == 2 and len(parts[1]) > 36:
                already_done.add(parts[1][:36])
        db.close()
    except Exception:
        pass  # DB may not exist yet on first run

    def _parse_session(filepath: Path) -> list[dict]:
        """Parse an OpenClaw JSONL session into memU-compatible message dicts."""
        messages: list[dict] = []
        with open(filepath) as f:
            for line in f:
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if obj.get("type") != "message":
                    continue
                msg = obj.get("message", {})
                role = msg.get("role", "")
                if role not in ("user", "assistant"):
                    continue
                timestamp = obj.get("timestamp", "")

                # Extract text from content blocks
                content = msg.get("content", "")
                if isinstance(content, str):
                    text = content
                elif isinstance(content, list):
                    text_parts = []
                    for block in content:
                        if isinstance(block, dict) and block.get("type") == "text":
                            text_parts.append(block.get("text", ""))
                    text = "\n".join(text_parts)
                else:
                    continue

                if not text.strip():
                    continue

                entry: dict[str, str] = {"role": role, "content": text}
                if timestamp:
                    entry["created_at"] = timestamp
                messages.append(entry)
        return messages

    def _extract_session_id(filepath: Path) -> str:
        """Extract the session UUID from a filename.

        Regular: {uuid}.jsonl -> stem is {uuid}
        Deleted: {uuid}.jsonl.deleted.{ts} -> split on '.jsonl' to get uuid
        """
        name = filepath.name
        return name.split(".jsonl")[0]

    # Parse all sessions, skip already-done and too-small
    parsed: list[tuple[Path, list[dict]]] = []
    skipped_small = 0
    skipped_done = 0
    for fp in session_files:
        sid = _extract_session_id(fp)
        if sid in already_done:
            skipped_done += 1
            continue
        msgs = _parse_session(fp)
        if len(msgs) < min_messages:
            skipped_small += 1
            continue
        parsed.append((fp, msgs))

    click.echo(f"  {len(parsed)} to index, {skipped_done} already done, {skipped_small} too small")

    if dry_run:
        for fp, msgs in parsed:
            click.echo(f"  {fp.stem}: {len(msgs)} messages")
        return

    async def _run():
        from nerve.memory.memu_bridge import MemUBridge

        bridge = MemUBridge(config)
        ok = await bridge.initialize()
        if not ok:
            click.echo("[ERR] Failed to initialize memU")
            return

        success = 0
        failed = 0
        for i, (fp, msgs) in enumerate(parsed, 1):
            session_id = _extract_session_id(fp)
            try:
                result = await asyncio.wait_for(
                    bridge.memorize_conversation(session_id, msgs),
                    timeout=timeout,
                )
                if result:
                    success += 1
                else:
                    failed += 1
            except asyncio.TimeoutError:
                click.echo(f"  [TIMEOUT] {session_id} (>{timeout}s)")
                failed += 1
            except Exception as e:
                click.echo(f"  [ERR] {session_id}: {e}")
                failed += 1

            if i % 10 == 0 or i == len(parsed):
                click.echo(f"  [{i}/{len(parsed)}] {success} indexed, {failed} failed")

        click.echo(f"\nDone: {success} conversations indexed, {failed} failed")

    asyncio.run(_run())


@main.command("backfill-timestamps")
@click.option("--dry-run", is_flag=True, help="Only show what would be updated")
@click.pass_context
def backfill_timestamps(ctx: click.Context, dry_run: bool) -> None:
    """Backfill happened_at timestamps from conversation JSON files."""
    import json
    import sqlite3

    config = ctx.obj["config"]
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")

    if not Path(db_path).exists():
        click.echo(f"[ERR] memU database not found: {db_path}")
        ctx.exit(1)
        return

    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row

    # Find items without happened_at that have a linked resource
    rows = db.execute(
        "SELECT id, resource_id FROM memu_memory_items WHERE happened_at IS NULL AND resource_id IS NOT NULL"
    ).fetchall()

    click.echo(f"Found {len(rows)} items without happened_at")

    updated = 0
    skipped = 0
    for row in rows:
        item_id = row["id"]
        resource_id = row["resource_id"]

        # Look up the resource URL and local_path
        res = db.execute("SELECT url, local_path FROM memu_resources WHERE id = ?", (resource_id,)).fetchone()
        if not res:
            skipped += 1
            continue

        # Try url first, fall back to local_path (memU stores the actual
        # file at local_path while url may be a segment reference)
        conv_path = Path(res["url"])
        if not conv_path.exists() and res["local_path"]:
            conv_path = Path(res["local_path"])

        if not conv_path.exists():
            skipped += 1
            continue

        # Read the conversation JSON and find the earliest created_at
        try:
            data = json.loads(conv_path.read_text(encoding="utf-8"))
            if not isinstance(data, list) or not data:
                skipped += 1
                continue

            earliest = None
            for entry in data:
                ts = entry.get("created_at")
                if ts:
                    if earliest is None or ts < earliest:
                        earliest = ts

            if not earliest:
                skipped += 1
                continue

            if dry_run:
                click.echo(f"  {item_id[:8]}... -> {earliest}")
            else:
                db.execute(
                    "UPDATE memu_memory_items SET happened_at = ? WHERE id = ?",
                    (earliest, item_id),
                )
            updated += 1
        except Exception as e:
            click.echo(f"  [ERR] {item_id[:8]}...: {e}")
            skipped += 1

    if not dry_run:
        db.commit()
    db.close()

    click.echo(f"\n{'Would update' if dry_run else 'Updated'} {updated} items, skipped {skipped}")


if __name__ == "__main__":
    main()
