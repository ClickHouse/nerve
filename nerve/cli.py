"""CLI commands for Nerve.

Usage:
    nerve init           First-run setup wizard (interactive)
    nerve start          Start the Nerve server (daemonized by default)
    nerve stop           Stop the running Nerve daemon
    nerve restart        Restart the Nerve daemon
    nerve status         Show daemon status
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


def _write_pid(pid: int) -> None:
    PID_DIR.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(pid))


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
        _write_pid(os.getpid())
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
    """Restart the Nerve daemon."""
    config = ctx.obj["config"]
    config_dir = Path(ctx.obj["config_dir"])

    # Docker mode: proxy to docker compose
    if _is_docker_mode(config):
        rc = _docker_compose(config_dir, ["restart"])
        if rc == 0:
            click.echo("Nerve restarted (Docker)")
        ctx.exit(rc)
        return

    running, pid = _get_daemon_status()
    if running:
        click.echo(f"Stopping Nerve (PID {pid})...")
        os.kill(pid, signal.SIGTERM)

        # Wait for shutdown
        for _ in range(30):
            time.sleep(0.5)
            if not _is_running(pid):
                break
        else:
            click.echo("Graceful shutdown timed out, sending SIGKILL...")
            try:
                os.kill(pid, signal.SIGKILL)
                time.sleep(0.5)
            except ProcessLookupError:
                pass

        _remove_pid()
        click.echo("Nerve stopped")

    # Brief pause before restart
    time.sleep(0.5)

    # Start again — invoke start command
    ctx.invoke(start)


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


@main.command()
@click.pass_context
def doctor(ctx: click.Context) -> None:
    """Check config, DB, API keys, and connectivity."""
    config = ctx.obj["config"]
    errors = []
    warnings = []

    click.echo("Nerve Doctor")
    click.echo("=" * 40)

    # Daemon status
    running, pid = _get_daemon_status()
    if running:
        click.echo(f"[OK] Daemon running (PID {pid})")
    else:
        click.echo("[--] Daemon not running")

    # Check workspace
    if config.workspace.exists():
        click.echo(f"[OK] Workspace: {config.workspace}")
        # Check workspace files against mode's manifest
        mode = getattr(config, "mode", "personal")
        try:
            from nerve.workspace import get_expected_files
            expected = get_expected_files(mode)
            for f in expected:
                if (config.workspace / f).exists():
                    click.echo(f"  [OK] {f}")
                else:
                    warnings.append(f"  [WARN] {f} not found (expected for {mode} mode)")
        except (FileNotFoundError, ValueError):
            # Fallback if templates aren't available
            for f in ["SOUL.md", "IDENTITY.md", "MEMORY.md"]:
                if (config.workspace / f).exists():
                    click.echo(f"  [OK] {f}")
                else:
                    warnings.append(f"  [WARN] {f} not found")
    else:
        errors.append(f"[ERR] Workspace not found: {config.workspace}")

    # Check proxy
    if config.proxy.enabled:
        binary = config.proxy.binary_path.expanduser()
        if binary.exists():
            click.echo(f"[OK] CLIProxyAPI binary: {binary}")
        else:
            warnings.append(f"[WARN] CLIProxyAPI binary not found (will download on start): {binary}")
        click.echo(f"[OK] Proxy configured: {config.proxy.host}:{config.proxy.port}")
        try:
            import httpx
            resp = httpx.get(
                f"http://{config.proxy.host}:{config.proxy.port}/v1/models",
                headers={"x-api-key": config.proxy.api_key},
                timeout=3,
            )
            if resp.status_code == 200:
                click.echo("[OK] Proxy is running and healthy")
            else:
                warnings.append(f"[WARN] Proxy returned status {resp.status_code}")
        except Exception:
            warnings.append("[WARN] Proxy not running (starts with Nerve)")

    # Check API keys
    if config.proxy.enabled:
        if config.anthropic_api_key:
            click.echo(f"[--] Anthropic API key set (proxy takes precedence)")
        else:
            click.echo("[--] Anthropic API key not set (using proxy)")
    elif config.anthropic_api_key:
        click.echo(f"[OK] Anthropic API key: ...{config.anthropic_api_key[-4:]}")
    else:
        errors.append("[ERR] Anthropic API key not set and proxy not enabled (config.local.yaml)")

    if config.openai_api_key:
        click.echo(f"[OK] OpenAI API key: ...{config.openai_api_key[-4:]}")
    else:
        warnings.append("[WARN] OpenAI API key not set (needed for memU embeddings)")

    # Check Telegram
    if config.telegram.enabled:
        if config.telegram.bot_token:
            click.echo(f"[OK] Telegram bot token: ...{config.telegram.bot_token[-4:]}")
        else:
            errors.append("[ERR] Telegram enabled but bot_token not set")
    else:
        click.echo("[--] Telegram disabled")

    # Check SSL
    if config.gateway.ssl.enabled:
        if config.gateway.ssl.cert and config.gateway.ssl.cert.exists():
            click.echo(f"[OK] SSL cert: {config.gateway.ssl.cert}")
        else:
            warnings.append("[WARN] SSL cert not found")
        if config.gateway.ssl.key and config.gateway.ssl.key.exists():
            click.echo(f"[OK] SSL key: {config.gateway.ssl.key}")
        else:
            warnings.append("[WARN] SSL key not found")
    else:
        click.echo("[--] SSL not configured")

    # Check auth
    if config.auth.password_hash:
        click.echo("[OK] Auth password hash configured")
    else:
        warnings.append("[WARN] Auth password not set — running in dev mode (no auth)")

    if config.auth.jwt_secret:
        click.echo("[OK] JWT secret configured")
    else:
        warnings.append("[WARN] JWT secret not set — running in dev mode")

    # Check DB
    db_path = Path("~/.nerve/nerve.db").expanduser()
    if db_path.exists():
        click.echo(f"[OK] Database: {db_path} ({db_path.stat().st_size / 1024:.1f} KB)")
    else:
        click.echo(f"[--] Database will be created at: {db_path}")

    # Check cron files
    if config.cron.system_file.exists():
        try:
            from nerve.cron.jobs import load_jobs
            system_jobs = load_jobs(config.cron.system_file)
            enabled = sum(1 for j in system_jobs if j.enabled)
            click.echo(f"[OK] System crons: {config.cron.system_file} ({enabled}/{len(system_jobs)} enabled)")
        except Exception:
            click.echo(f"[OK] System crons: {config.cron.system_file}")
    else:
        click.echo(f"[--] No system crons at: {config.cron.system_file}")

    if config.cron.jobs_file.exists():
        try:
            from nerve.cron.jobs import load_jobs as _load_jobs
            user_jobs = _load_jobs(config.cron.jobs_file)
            if user_jobs:
                click.echo(f"[OK] User crons: {config.cron.jobs_file} ({len(user_jobs)} jobs)")
            else:
                click.echo(f"[OK] User crons: {config.cron.jobs_file} (empty)")
        except Exception:
            click.echo(f"[OK] User crons: {config.cron.jobs_file}")
    else:
        click.echo(f"[--] No user crons at: {config.cron.jobs_file}")

    # Check external tools
    import shutil
    for tool_name in ["gog", "gh"]:
        if shutil.which(tool_name):
            click.echo(f"[OK] {tool_name} CLI found")
        else:
            warnings.append(f"[WARN] {tool_name} CLI not found (needed for sync)")

    # Summary
    click.echo()
    for w in warnings:
        click.echo(w)
    for e in errors:
        click.echo(e)

    if errors:
        click.echo(f"\n{len(errors)} error(s), {len(warnings)} warning(s)")
        ctx.exit(1)
    else:
        click.echo(f"\nAll good! {len(warnings)} warning(s)")


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
