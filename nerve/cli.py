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

# Must be first: applies BLAS thread caps and other process-env defaults
# that have to be in place before numpy (or any BLAS user) is imported.
import nerve._env  # noqa: F401  isort: skip

import asyncio
import contextlib
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import click

from nerve import paths, pidfile
from nerve.config import (
    RESUME_QUEUE_FILE,
    load_config,
    resolve_config_dir,
    set_config,
    write_config_pointer,
)

# PID / log file locations resolve lazily through nerve.paths so that a
# NERVE_HOME override (e.g. in tests or multi-instance setups) is always
# honored — nothing is frozen at import time.


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

def _write_pid(config_dir: Path | None = None) -> bool:
    """Take the daemon lock and write the PID of this process.

    Returns False if a different process holds the lock.
    """
    paths.ensure_nerve_home()
    if not pidfile.acquire(paths.lock_file(), paths.pid_file()):
        return False
    if config_dir is not None:
        write_config_pointer(config_dir)
    return True


def _remove_pid() -> None:
    paths.pid_file().unlink(missing_ok=True)


def _get_daemon_status() -> tuple[bool, int | None]:
    """Returns (is_running, pid)."""
    pid = pidfile.live_pid(paths.lock_file(), paths.pid_file())
    return pid is not None, pid


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


# Commands that must keep working when the config itself is broken -- they are
# how an operator diagnoses or repairs it. They receive ctx.obj["config"] = None
# and ctx.obj["config_error"], and are responsible for reporting.
_SELF_DIAGNOSING_COMMANDS = frozenset({"config", "doctor", "init", "migrate"})


@click.group()
@click.option(
    "--config-dir", "-c", type=click.Path(), default=None,
    help="Config directory (default: auto-detected — see `nerve doctor`)",
)
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
@click.pass_context
def main(ctx: click.Context, config_dir: str | None, verbose: bool) -> None:
    """Nerve — Personal AI Assistant"""
    setup_logging(verbose)
    # Resolve the config directory via the waterfall (flag → env → cwd →
    # pointer file) so commands work from any directory, not just the
    # install dir.
    resolved_dir, config_source = resolve_config_dir(config_dir)
    if not resolved_dir.is_absolute():
        resolved_dir = resolved_dir.resolve()
    config = None
    config_error = None
    try:
        config = load_config(resolved_dir)
        set_config(config)
    except Exception as e:
        # This callback runs before *every* subcommand, so a config that won't
        # load would otherwise take down the commands you reach for to fix it.
        # The self-diagnosing ones get config=None plus the message and report
        # it themselves — for any failure at all, since a command whose job is to
        # explain a broken config is the last place a traceback helps.
        #
        # Everything else gets a clean error and a non-zero exit, but only for
        # the operator's own mistakes: ConfigError (malformed YAML, an
        # unresolved required ${VAR}) and the plain ValueError that
        # NerveConfig._validate_backend_config raises for a bad agent.backend or
        # codex section — ConfigError subclasses ValueError, so one check covers
        # both. Anything else means nerve itself is broken, not the config, and
        # keeps its traceback: presenting an internal defect as "Error: <config
        # problem>" sends the operator to edit a file that was never at fault.
        if ctx.invoked_subcommand not in _SELF_DIAGNOSING_COMMANDS:
            if isinstance(e, ValueError):
                raise click.ClickException(str(e)) from e
            raise
        config_error = str(e)
    ctx.ensure_object(dict)
    ctx.obj["config"] = config
    ctx.obj["config_error"] = config_error
    ctx.obj["config_dir"] = str(resolved_dir)
    ctx.obj["config_source"] = config_source
    ctx.obj["verbose"] = verbose


@main.command()
@click.option("--if-needed", is_flag=True, help="Only run if fresh install detected")
@click.option("--non-interactive", is_flag=True, help="Use env vars, no prompts (for Docker)")
@click.option("--inside-docker", is_flag=True, hidden=True, help="Skip deployment step (running inside Docker)")
@click.option("--terminal", is_flag=True, help="Ask everything in this terminal instead of the setup page")
@click.option("--deployment", type=click.Choice(["server", "docker"]), default=None, help="Slim setup: how to run Nerve")
@click.option("--workspace", "workspace_opt", default=None, help="Slim setup: the workspace directory")
@click.option("--port", type=int, default=None, help="Slim setup: the gateway port")
@click.option("--bind", type=click.Choice(["localhost", "lan"]), default=None, help="Slim setup: who can reach the web UI")
@click.option("--no-start", is_flag=True, help="Slim setup: write the files but do not start Nerve")
@click.option("--no-browser", is_flag=True, help="Slim setup: do not offer to open the setup page")
@click.option("--no-clipboard", is_flag=True, help="Slim setup: do not copy the setup token to the clipboard")
@click.option("--yes", "-y", is_flag=True, help="Slim setup: accept every default without asking")
@click.option(
    "--keep-env", is_flag=True,
    help="Slim setup: pass ANTHROPIC_API_KEY/CLAUDE_CODE_OAUTH_TOKEN from this shell to the daemon",
)
@click.option(
    "--serve-next", is_flag=True, hidden=True,
    help="Slim setup: this process serves next; leave NERVE_SYSTEM_PRINCIPAL_ID to the gateway",
)
@click.pass_context
def init(
    ctx: click.Context, if_needed: bool, non_interactive: bool, inside_docker: bool,
    terminal: bool, deployment: str | None, workspace_opt: str | None, port: int | None,
    bind: str | None, no_start: bool, no_browser: bool, no_clipboard: bool, yes: bool,
    keep_env: bool, serve_next: bool = False,
) -> None:
    """Set up Nerve: decide how it runs here, then finish in your browser.

    The default asks only what the browser cannot decide (deployment, paths,
    port), starts Nerve and prints the setup page and token. ``--terminal``
    asks everything here instead; ``--non-interactive`` reads the environment.
    """
    from nerve.bootstrap import SetupWizard, is_fresh_install, run_non_interactive

    config_dir = Path(ctx.obj["config_dir"])

    if if_needed and not is_fresh_install(config_dir):
        return  # Already configured, exit silently

    if not terminal and not non_interactive and not inside_docker:
        _slim_init(
            ctx, config_dir,
            deployment=deployment, workspace=workspace_opt, port=port, bind=bind,
            start=not no_start, browser=False if no_browser else None,
            clipboard=not no_clipboard, yes=yes, keep_env=keep_env,
            system_principal=not serve_next,
        )
        return

    if not is_fresh_install(config_dir):
        if non_interactive:
            click.echo("Nerve is already configured. Skipping.")
            return
        if not click.confirm(
            "Nerve is already configured. Re-run setup? (config.yaml and "
            "config.local.yaml are regenerated; workspace/config/settings.yaml "
            "keeps any key the wizard doesn't generate; other workspace files "
            "are untouched. Each of the three that holds any setting is copied "
            "to *.bak first — an empty or comments-only file is skipped, since "
            "there is nothing in it to lose.)"
        ):
            return

    wizard = None
    if non_interactive:
        choices = run_non_interactive(config_dir)
    else:
        wizard = SetupWizard(config_dir, inside_docker=inside_docker)
        choices = wizard.run()

    # Remember where the config lives so every future `nerve` command
    # finds it regardless of the caller's working directory.
    write_config_pointer(config_dir)

    # Reload config after wizard writes files
    config = load_config(config_dir)
    set_config(config)
    ctx.obj["config"] = config

    # Create the owner account now, while the wizard's "Your name" answer is
    # available. No file stores that answer, so the gateway would create the
    # owner unnamed. Headless installs have no name. For docker, this runs
    # inside the container (`--inside-docker`), not in the host-side wizard.
    from nerve.migrate import bootstrap_identity_sync

    display_name = (choices.user_name or "").strip() or None
    try:
        report = bootstrap_identity_sync(
            config, display_name=display_name, passwordless=choices.passwordless,
        )
    except Exception as e:  # noqa: BLE001 - any failure means there is no account
        # The wizard deleted its checkpoint when it applied the configuration.
        # Save the answers again so a re-run keeps the name, then fail. The
        # checkpoint write can fail for the same reason, so the message says
        # whether it worked.
        saved = wizard.checkpoint() if wizard is not None else False
        if wizard is None:
            remedy = " Fix the cause and run 'nerve init --non-interactive' again."
        elif saved:
            remedy = (
                " Fix the cause and run 'nerve init' again; your answers, the name "
                "included, are saved and will be reused."
            )
        else:
            name = (choices.user_name or "").strip()
            remedy = (
                " Your answers could not be saved either; re-run 'nerve init' and "
                "re-enter your details"
                + (f' (your name was "{name}")' if name else "")
                + "."
            )
        raise click.ClickException(
            f"Setup wrote the configuration, but the local owner account could not "
            f"be created: {e}.{remedy}"
        ) from e
    for action in report.identity_actions:
        click.echo(f"  {action}")
    from nerve.db.accounts import read_setup_required

    # In external mode there are no local accounts to claim, so the hint
    # would point at a token that is never generated.
    if not _external_auth_in_this_shell() and read_setup_required(
        paths.db_path(), configured_password=bool(config.auth.password_hash),
    ):
        click.echo(
            "  Setup is not complete: no password was set and a passwordless "
            "installation was not chosen. After the gateway starts, run "
            "'nerve status' to read the setup token, then complete setup at /setup."
        )


# --- The slim init: host decisions, then the browser --------------------------


def _external_auth_in_this_shell() -> bool:
    """Whether ``NERVE_AUTH_MODE`` in this shell says external (unknown → False)."""
    from nerve.config import ConfigError
    from nerve.gateway.auth import AUTH_MODE_EXTERNAL, auth_mode_from_env

    try:
        return auth_mode_from_env() == AUTH_MODE_EXTERNAL
    except ConfigError:
        return False


def _setup_url(config) -> str:
    host = config.gateway.host if config is not None else "localhost"
    port = config.gateway.port if config is not None else 8900
    if host in {"0.0.0.0", "::", "[::]", ""}:
        host = "localhost"
    return f"http://{host}:{port}/setup"


def _setup_unclaimed(config, external: bool | None = None) -> bool:
    """Whether the instance still waits for its first claim: the read-only
    database answer, which a CLI process can take without the daemon. Always
    ``False`` under external authentication, where there is no claim step."""
    from nerve.db.accounts import read_setup_required

    if external is None:
        external = _external_auth_in_this_shell()
    if external:
        return False
    return read_setup_required(paths.db_path(), configured_password=bool(config.auth.password_hash))


def _setup_summary(config) -> dict | None:
    """The setup checklist as a CLI process can derive it (no daemon needed).

    Same derivation as ``GET /api/setup``, fed with the facts a separate
    process can read: config, workspace files, the state file and — in local
    mode only — the read-only "is setup required" answer from the database.
    """
    if config is None:
        return None
    try:
        from nerve import setup_checklist, setup_provenance, setup_state, setup_telegram_login
        from nerve.db.accounts import read_setup_required
        from nerve.setup_writer import workspace_mode

        external = _external_auth_in_this_shell()
        unclaimed = _setup_unclaimed(config, external)
        facts = setup_checklist.Facts(
            config=config,
            workspace=Path(config.workspace),
            state=setup_state.load_state(),
            external_auth=external,
            unclaimed=unclaimed,
            lockdown=bool(config.lockdown),
            writable_problem=setup_checklist.LOCKDOWN_TEXT if config.lockdown else None,
            restart_pending=[],
            telegram_live=False,
            credential_sources=setup_provenance.credential_sources(config),
            telegram_session_present=setup_telegram_login.session_present(),
            env_credential=bool(
                os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
            ),
            env_oauth_only=bool(
                os.environ.get("CLAUDE_CODE_OAUTH_TOKEN") and not os.environ.get("ANTHROPIC_API_KEY")
            ),
            onboarding=setup_checklist.onboarding_from_task(
                Path(config.workspace), workspace_mode(Path(config.workspace)),
            ),
        )
        return setup_checklist.summarize(facts)
    except Exception as e:  # noqa: BLE001 — status/doctor must never die on the checklist
        logging.getLogger(__name__).debug("setup checklist unavailable: %s", e)
        return None


_SHELL_VIEW_CAVEAT = (
    " (as seen from this shell; set NERVE_AUTH_MODE/NERVE_LOCKDOWN as the service does)"
)


def _shell_view_caveat() -> str:
    """Suffix for checklist lines a CLI process derives from *its own* shell.

    The auth mode and lockdown come from the environment, so a shell without
    ``NERVE_AUTH_MODE``/``NERVE_LOCKDOWN`` sees a supervised external or
    locked instance the local way. While a daemon runs and neither variable is
    set here, say so instead of presenting that view as the instance's.
    """
    if os.environ.get("NERVE_AUTH_MODE") or os.environ.get("NERVE_LOCKDOWN"):
        return ""
    try:
        running, _pid = _get_daemon_status()
    except Exception:  # noqa: BLE001
        return ""
    return _SHELL_VIEW_CAVEAT if running else ""


def _compose_host_workspace(config_dir: str | Path) -> Path | None:
    """The host path bind-mounted as the container's workspace, from the
    generated ``docker-compose.yml`` (the volume whose container side is the
    container workspace path). ``None`` when the file or the mount is absent."""
    import yaml

    from nerve.setup_writer import _DOCKER_WORKSPACE

    compose_file = Path(config_dir) / "docker-compose.yml"
    try:
        data = yaml.safe_load(compose_file.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError):
        return None
    services = data.get("services") if isinstance(data, dict) else None
    if not isinstance(services, dict):
        return None
    for service in services.values():
        volumes = service.get("volumes") if isinstance(service, dict) else None
        for volume in volumes or []:
            if not isinstance(volume, str):
                continue
            host, sep, container = volume.rpartition(":")
            if sep and container == _DOCKER_WORKSPACE and host:
                return Path(host).expanduser()
    return None


_DOCKER_VIEW_UNAVAILABLE = (
    "the workspace mount could not be read from docker-compose.yml on this host"
)


def _checklist_config(config) -> tuple:
    """``(config, note)``: what the setup checklist is derived from, or
    ``(None, why)`` when nothing truthful can be derived from this host.

    On the host side of a Docker deployment ``config.workspace`` is the
    *container* path (``/root/nerve-workspace``): it does not exist here and
    the tracked settings under it were never loaded, so deriving against it
    would report a finished install as "3 steps left" forever (the mode
    pending, its dependents blocked). The checklist is derived against the
    same files at their bind-mounted host path instead; when that path cannot
    be found the caller prints a neutral line rather than a wrong count.
    """
    if config is None:
        return None, None
    if not _is_docker_mode(config):
        return config, None
    host_workspace = _compose_host_workspace(getattr(config, "config_dir", "") or Path.cwd())
    if host_workspace is None or not host_workspace.is_dir():
        return None, _DOCKER_VIEW_UNAVAILABLE
    try:
        return load_config(Path(config.config_dir), workspace=host_workspace), None
    except Exception as e:  # noqa: BLE001 — status/doctor must never die on the checklist
        logging.getLogger(__name__).debug("docker host view of the config unavailable: %s", e)
        return None, _DOCKER_VIEW_UNAVAILABLE


def _echo_checklist_line(config) -> None:
    """``Setup checklist: N steps left — <url>`` when claimed but unfinished."""
    view, note = _checklist_config(config)
    if view is None and note:
        # Docker on the host without a readable mount: nothing about the
        # workspace can be derived here (config names the container's path,
        # which this account may not even stat), so the only fact taken is the
        # database's — claimed or not — and the line says where to look.
        try:
            unclaimed = _setup_unclaimed(config)
        except Exception as e:  # noqa: BLE001 — status must never die on the checklist
            logging.getLogger(__name__).debug("setup claim state unavailable: %s", e)
            return
        if not unclaimed:  # while unclaimed the token lines already say what to do
            click.echo(f"  Setup checklist: open {_setup_url(config)} ({note})")
        return
    summary = _setup_summary(view if view is not None else config)
    if not summary or summary.get("finished"):
        return
    claim = next((s for s in summary["steps"] if s["id"] == "claim"), None)
    if claim is not None and claim["status"] != "done":
        return  # the token lines already say what to do
    left = summary.get("steps_left", 0)
    noun = "step" if left == 1 else "steps"
    click.echo(f"  Setup checklist: {left} {noun} left — {_setup_url(config)}{_shell_view_caveat()}")


def _spawn_daemon(
    config_dir: str | Path, verbose: bool, env: dict[str, str] | None = None,
) -> "subprocess.Popen":
    """Start ``nerve start --foreground`` detached, logging to the state dir.

    ``env`` replaces the inherited environment when given (the slim init drops
    shell credentials, see :func:`_daemon_environment`); ``None`` inherits.
    """
    nerve_bin = sys.argv[0]
    cmd = [sys.executable, nerve_bin, "-c", str(config_dir)]
    if verbose:
        cmd.append("-v")
    cmd.extend(["start", "--foreground"])
    paths.ensure_nerve_home()
    log_fd = open(paths.log_file(), "a")
    try:
        return subprocess.Popen(
            cmd,
            stdout=log_fd,
            stderr=log_fd,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            env=env,
        )
    finally:
        log_fd.close()


_ENV_CREDENTIALS = ("ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN")


def _daemon_environment(*, keep_env: bool) -> tuple[dict[str, str] | None, list[str]]:
    """The environment for the daemon the slim init spawns.

    A credential exported in the operator's interactive shell would otherwise
    become the daemon's provider for this run only: Setup would read "Model:
    done, from the environment", nothing would be saved, and the next
    ``nerve start`` from a clean shell (or a reboot) would lose it. Unless
    ``--keep-env`` asks for exactly that, the two variables are dropped and
    the setup page is where the key is saved. Returns ``(env, dropped)`` —
    ``env`` is ``None`` (inherit) when nothing had to change.
    """
    present = [name for name in _ENV_CREDENTIALS if os.environ.get(name)]
    if keep_env or not present:
        return None, []
    env = {k: v for k, v in os.environ.items() if k not in _ENV_CREDENTIALS}
    return env, present


def _spawned_daemon_problem(proc) -> str | None:
    """Why the process just spawned is not the one answering ``/health``.

    ``/health`` going green proves only that *something* listens on the port:
    when it was busy, the child dies on ``address already in use`` while the
    other program keeps answering. The child must still be alive and the pid
    file (which ``start --foreground`` writes first thing) must name it.
    """
    from nerve import daemon

    if proc.poll() is not None:
        return f"the new process exited with code {proc.returncode} right after starting"
    recorded = daemon.pid_file_pid()
    if recorded != proc.pid:
        return (
            "something else answers on that port (the pid file names "
            f"{recorded if recorded else 'no process'}, the process just started is {proc.pid})"
        )
    return None


_START_FAILED_HINT = (
    " The configuration is written. Fix the cause, start Nerve with: nerve start "
    "— then read the setup token with: nerve status"
)


@contextlib.contextmanager
def _quiet_console(log_path: Path | None):
    """Keep INFO lines off the terminal while the slim init prints its transcript.

    The skills install, the database migrations and the identity bootstrap
    each log at INFO; interleaved with the transcript they bury the token
    block. Console handlers are raised to WARNING for the duration and INFO
    goes to the log file instead, so nothing is lost. Restored afterwards.
    """
    root = logging.getLogger()
    console = [
        h for h in root.handlers
        if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
    ]
    previous = [(h, h.level) for h in console]
    for handler in console:
        handler.setLevel(max(handler.level, logging.WARNING))
    file_handler = None
    if log_path is not None:
        try:
            paths.ensure_nerve_home()
            file_handler = logging.FileHandler(log_path)
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(
                logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", "%H:%M:%S")
            )
            root.addHandler(file_handler)
        except OSError:
            file_handler = None
    try:
        yield
    finally:
        for handler, level in previous:
            handler.setLevel(level)
        if file_handler is not None:
            root.removeHandler(file_handler)
            file_handler.close()


def _wait_for_health(url: str, timeout: float) -> float | None:
    """Poll ``url`` every 0.5 s until it answers 200; seconds taken, or ``None``."""
    import urllib.error
    import urllib.request

    started = time.monotonic()
    while time.monotonic() - started < timeout:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:  # noqa: S310 — loopback health
                if response.status == 200:
                    return time.monotonic() - started
        except (urllib.error.URLError, OSError, ValueError):
            pass
        time.sleep(0.5)
    return None


_CLIPBOARD_COMMANDS = (
    ["pbcopy"],
    ["wl-copy"],
    ["xclip", "-selection", "clipboard"],
    ["xsel", "--clipboard", "--input"],
)


def _copy_to_clipboard(text: str) -> bool:
    """Copy ``text`` through the first clipboard tool on PATH. Stdin only:
    the value is never an argv element."""
    for argv in _CLIPBOARD_COMMANDS:
        if not shutil.which(argv[0]):
            continue
        try:
            result = subprocess.run(argv, input=text.encode("utf-8"), timeout=3, capture_output=True)
        except (OSError, subprocess.TimeoutExpired):
            return False
        return result.returncode == 0
    return False


def _print_setup_block(url: str, token: str | None, *, copied: bool, external: bool) -> None:
    width = 62
    top = "  ┌" + "─" * width + "┐"
    bottom = "  └" + "─" * width + "┘"

    def row(text: str = "") -> None:
        click.echo("  │  " + text.ljust(width - 2) + "│")

    click.echo()
    click.echo(top)
    row("Finish setup in your browser")
    row()
    row(f"Setup page   {url}")
    if external:
        # Two clauses on two lines — never a sentence broken mid-clause.
        row("Sign in through your gateway.")
        row("No setup token is used in external mode.")
    elif token:
        row(f"Setup token  {token}")
        if copied:
            row("             (copied to your clipboard)")
        row()
        row("The token is a one-time credential. It is pasted into")
        row("the page and never part of the URL. To see it again:")
        row("nerve status")
    else:
        row("Setup token  (run nerve status in a moment)")
    click.echo(bottom)
    click.echo()


def _print_configured_status(ctx: click.Context, config) -> None:
    """The slim path on an already configured install: say so, show status."""
    click.echo("Nerve is already configured.")
    if config is None:
        click.echo("  (its configuration could not be loaded; run 'nerve doctor')")
    elif _is_docker_mode(config):
        click.echo("  Deployment: docker — 'nerve status' shows the container state.")
        _echo_setup_token(config)
        _echo_checklist_line(config)
    else:
        running, pid = _get_daemon_status()
        if running:
            click.echo(f"  Nerve is running (PID {pid})")
            click.echo(f"  Listening on {config.gateway.host}:{config.gateway.port}")
        else:
            click.echo("  Nerve is not running — start it with: nerve start")
        _echo_setup_token(config)
        _echo_checklist_line(config)
    click.echo("  Reconfigure everything: nerve init --terminal")


def _slim_init(
    ctx: click.Context,
    config_dir: Path,
    *,
    deployment: str | None,
    workspace: str | None,
    port: int | None,
    bind: str | None,
    start: bool,
    browser: bool | None,
    clipboard: bool,
    yes: bool,
    keep_env: bool = False,
    system_principal: bool = True,
) -> None:
    from nerve.bootstrap import SlimFlags, SlimSetup, is_fresh_install

    if not is_fresh_install(config_dir):
        _print_configured_status(ctx, ctx.obj.get("config"))
        return

    # A non-TTY stdin still answers the prompts (click reads piped lines, as
    # the full wizard always has); only --yes skips them.
    flags = SlimFlags(
        deployment=deployment, workspace=workspace, port=port, bind=bind,
        yes=yes, start=start, browser=browser, clipboard=clipboard,
    )
    with _quiet_console(paths.log_file()):
        result = SlimSetup(config_dir, flags).run()

        write_config_pointer(config_dir)
        config = load_config(config_dir)
        set_config(config)
        ctx.obj["config"] = config

        # The owner account exists before the daemon answers, so the setup
        # token is available the moment /health is green. No name: the setup
        # page asks. ``system_principal`` is false when this process serves
        # next (``nerve start -f``): the gateway's bootstrap reads
        # NERVE_SYSTEM_PRINCIPAL_ID and logs the result, once.
        from nerve.migrate import bootstrap_identity_sync

        try:
            bootstrap_identity_sync(
                config, display_name=None, passwordless=False,
                system_principal=system_principal,
            )
        except Exception as e:  # noqa: BLE001 — the files are written; say what is missing
            raise click.ClickException(
                f"Setup wrote the configuration, but the local owner account could not "
                f"be created: {e}. Fix the cause and run 'nerve init' again."
            ) from e

    docker = result.choices.deployment == "docker"
    external = _external_auth_in_this_shell()
    url = f"http://{'127.0.0.1' if result.bind == 'localhost' else 'localhost'}:{result.choices.gateway_port}/setup"
    health = f"http://127.0.0.1:{result.choices.gateway_port}/health"

    if not start:
        click.echo()
        click.echo("  Not started (--no-start). Start it with: nerve start")
        click.echo(f"  Then open {url} — 'nerve status' prints the setup token.")
        return

    if docker:
        click.echo("    Starting the container (docker compose up -d --build)…")
        rc = _docker_compose(config_dir, ["up", "-d", "--build"])
        if rc != 0:
            raise click.ClickException(
                "docker compose up failed. Fix the cause and run: docker compose up -d --build"
            )
        waited = _wait_for_health(health, timeout=300)
        if waited is None:
            raise click.ClickException(
                "Nerve did not answer within 300 s. Logs: nerve logs (docker compose logs)."
                " The configuration is written. Fix the cause, then: docker compose up -d "
                "— and read the setup token with: nerve status"
            )
        click.secho(f"    ✓ Nerve started in Docker — ready in {waited:.1f} s", fg="green")
        click.echo("    Container restarts are automatic (restart: unless-stopped). nerve stop is docker compose down.")
    else:
        running, pid = _get_daemon_status()
        if running:
            click.secho(f"    ✓ Nerve is already running (PID {pid})", fg="green")
        else:
            env, dropped = _daemon_environment(keep_env=keep_env)
            if dropped:
                click.echo(
                    f"    {' and '.join(dropped)} is set in this shell; not passed to Nerve "
                    "— save a key on the setup page so it survives a restart "
                    "(or re-run with --keep-env to use it for this run only)."
                )
            proc = _spawn_daemon(config_dir, ctx.obj.get("verbose", False), env=env)
            waited = _wait_for_health(health, timeout=60)
            if waited is None:
                raise click.ClickException(
                    f"Nerve did not answer within 60 s. Logs: nerve logs.{_START_FAILED_HINT}"
                )
            problem = _spawned_daemon_problem(proc)
            if problem:
                raise click.ClickException(
                    f"Nerve did not start cleanly: {problem}. Logs: {paths.log_file()}.{_START_FAILED_HINT}"
                )
            click.secho(f"    ✓ Nerve started (PID {proc.pid}) — ready in {waited:.1f} s", fg="green")

    token = None
    if not external:
        from nerve.db.accounts import read_instance_secret
        from nerve.setup_token import SETUP_TOKEN_NAME

        token = read_instance_secret(paths.db_path(), SETUP_TOKEN_NAME) or None
    copied = bool(token) and clipboard and _copy_to_clipboard(token)
    _print_setup_block(url, token, copied=copied, external=external)

    open_it = browser
    if open_it is None:
        open_it = sys.stdin.isatty() and not yes and click.confirm(
            "  Open the setup page in your browser now?", default=True,
        )
    if open_it:
        click.launch(url)
    if external:
        click.echo("  Later: `nerve status` prints the page again.")
    else:
        click.echo("  Later: `nerve status` prints the page and token again.")
    click.echo("  Prefer the terminal? `nerve init --terminal` asks everything here.")


def _refuse_unknown_auth_mode() -> None:
    """Stop the command when ``NERVE_AUTH_MODE`` has an unknown value.

    The server reads the variable when it starts and refuses an unknown
    value. ``start`` and ``restart`` check it first, so that they show the
    error, and do not stop the running daemon or start one that stops at once.
    """
    from nerve.config import ConfigError
    from nerve.gateway.auth import auth_mode_from_env

    try:
        auth_mode_from_env()
    except ConfigError as e:
        raise click.ClickException(str(e)) from e


@main.command()
@click.option("--foreground", "-f", is_flag=True, help="Run in foreground (don't daemonize)")
@click.pass_context
def start(ctx: click.Context, foreground: bool) -> None:
    """Start the Nerve server."""
    _refuse_unknown_auth_mode()
    config_dir = Path(ctx.obj["config_dir"])
    config = ctx.obj["config"]

    # Migrate a legacy install to the workspace/config layout if needed
    # (idempotent, best-effort). Non-destructive — originals kept as *.migrated.
    # The gateway runs the identity bootstrap when it starts. It also reads
    # NERVE_SYSTEM_PRINCIPAL_ID and logs the result, so the preview here does not.
    if config is not None:
        from nerve.migrate import maybe_migrate
        report = maybe_migrate(
            config_dir, workspace=getattr(config, "workspace", None), config=config,
            preview_system_principal=False,
        )
        if report is not None and report.did_anything:
            # The config in hand was loaded from the old locations, and migration
            # has just moved the files out from under it — cron in particular now
            # lives in the workspace, so the stale object resolves to job files
            # that no longer exist and a --foreground run would schedule nothing.
            try:
                config = load_config(config_dir)
            except Exception as e:
                raise click.ClickException(
                    f"Config could not be reloaded after migration: {e}"
                ) from e
            set_config(config)
            ctx.obj["config"] = config

    # Detect fresh install — offer to run setup wizard
    from nerve.bootstrap import is_fresh_install
    if is_fresh_install(config_dir):
        if foreground:
            # The slim init writes the files; *this* process is the server, so
            # it must not spawn a second one (a supervisor unit running
            # `start --foreground` would otherwise exit 1 and leave an
            # unsupervised daemon holding the port).
            click.echo("Fresh install detected — running setup...")
            ctx.invoke(init, no_start=True, no_browser=True, no_clipboard=True, serve_next=True)
            config = ctx.obj.get("config")
            if config is None:
                raise click.ClickException(
                    "Setup did not produce a configuration. Run 'nerve init' and then 'nerve start -f'."
                )
            click.echo(
                f"  Setup page: {_setup_url(config)} — in another terminal, 'nerve status' "
                "prints the setup token once Nerve answers."
            )
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
        if not _write_pid(config_dir=config_dir):
            click.echo("Nerve is already running")
            ctx.exit(1)
            return
        try:
            from nerve.gateway.server import run_server
            click.echo(f"Starting Nerve on {config.gateway.host}:{config.gateway.port}")
            run_server(config)
        finally:
            _remove_pid()
    else:
        # Daemonize: spawn a background process (shared with the slim init)
        proc = _spawn_daemon(ctx.obj["config_dir"], ctx.obj["verbose"])

        # Wait briefly to verify the process started
        time.sleep(1)
        if proc.poll() is not None:
            click.echo(f"Nerve failed to start (exit code {proc.returncode})")
            click.echo(f"Check logs: {paths.log_file()}")
            ctx.exit(1)
            return

        click.echo(f"Nerve started (PID {proc.pid})")
        click.echo(f"  Listening on {config.gateway.host}:{config.gateway.port}")
        click.echo(f"  Logs: {paths.log_file()}")


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
        if not pidfile.pid_exists(pid):
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
@click.option(
    "--resume",
    "resume_ids",
    multiple=True,
    metavar="SESSION_ID",
    help=(
        "Session id to auto-resume once the daemon is back (repeatable). The "
        "fresh daemon re-drives an interrupted turn for each id that still has "
        "a resumable SDK session."
    ),
)
@click.pass_context
def restart(ctx: click.Context, resume_ids: tuple[str, ...]) -> None:
    """Restart the Nerve daemon.

    Spawns a detached helper process that stops the old daemon and starts a
    new one.  This ensures restarts work even when triggered from *inside*
    the running daemon (e.g. via the Telegram bot / agent), because the
    helper runs in its own session and survives the parent's death.

    ``--resume SESSION_ID`` enrolls one or more sessions to be continued after
    the restart: their ids are written to the resume queue now, and the fresh
    daemon re-drives each interrupted turn on startup.
    """
    _refuse_unknown_auth_mode()

    # Enroll ids before anything else so the queue file is on disk — and
    # survives even a hard kill — by the time the new instance reads it.
    # Append mode lets concurrent enrollments from different sessions coexist.
    # Written for every mode (normal / Docker / systemd) since all restart the
    # daemon. Invalid/dead ids are skipped by the daemon-side drainer.
    if resume_ids:
        ids = [s.strip() for s in resume_ids if s.strip()]
        if ids:
            paths.ensure_nerve_home()  # RESUME_QUEUE_FILE lives directly in the state dir
            with open(RESUME_QUEUE_FILE, "a") as fh:
                fh.writelines(f"{sid}\n" for sid in ids)
            click.echo(f"Will resume {len(ids)} session(s) after restart.")

    config = ctx.obj["config"]
    # Already resolved via the waterfall (flag → env → cwd → pointer), so
    # this is an absolute path that doesn't depend on the caller's CWD.
    config_dir = Path(ctx.obj["config_dir"])

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

    # The detached helper lives in nerve.daemon, shared with the setup page's
    # POST /api/system/restart, so a restart typed here and one clicked in the
    # browser are the same restart.
    from nerve import daemon

    running, old_pid = _get_daemon_status()
    outcome = daemon.restart_daemon(
        config_dir, verbose=ctx.obj["verbose"],
        old_pid=old_pid if running else None, systemd=False,
    )
    click.echo(outcome.message)


def _echo_setup_token(config) -> None:
    """Print the unclaimed instance's token only to this local terminal."""
    from nerve.db.accounts import read_instance_secret
    from nerve.setup_token import SETUP_TOKEN_NAME

    token = read_instance_secret(paths.db_path(), SETUP_TOKEN_NAME)
    if not token:
        return
    host = config.gateway.host if config is not None else "localhost"
    port = config.gateway.port if config is not None else 8900
    if host in {"0.0.0.0", "::", "[::]", ""}:
        host = "localhost"
    click.echo()
    click.secho(
        "  Setup of this instance is not complete, and sign-in is refused "
        "until it is. Complete it on the setup page with the token below.",
        fg="yellow",
    )
    click.echo(f"  Setup page: http://{host}:{port}/setup")
    click.echo(f"  Setup token: {token}")


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
        _echo_setup_token(config)
        _echo_checklist_line(config)
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
        click.echo(f"  Logs: {paths.log_file()}")
    else:
        click.echo("Nerve is not running")

    _echo_setup_token(config)
    _echo_checklist_line(config)

    if follow and paths.log_file().exists():
        click.echo(f"\n--- Tailing {paths.log_file()} ---")
        try:
            os.execlp("tail", "tail", "-f", str(paths.log_file()))
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

    if not paths.log_file().exists():
        click.echo(f"No log file at {paths.log_file()}")
        return

    click.echo(f"--- {paths.log_file()} ---")
    try:
        os.execlp("tail", "tail", "-f", str(paths.log_file()))
    except Exception:
        # Fallback: print last 50 lines
        lines = paths.log_file().read_text().splitlines()
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


def _dep_install_cmd(source_root: Path) -> tuple[list[str], dict[str, str]]:
    """Command and env overrides to reinstall Nerve's deps in the current venv.

    Prefers ``uv sync`` when uv is present and the checkout has a ``uv.lock``,
    so an upgrade lands on the locked, CI-tested dependency set instead of
    re-resolving against pyproject's bounds. That re-resolution is what made
    ``nerve upgrade`` able to reintroduce a broken release even after one had
    been diagnosed. Falls back to ``uv pip install -e .``, then
    ``python -m pip install -e .``.

    Returns ``(argv, extra_env)``; ``extra_env`` is empty for the fallbacks.
    """
    uv_bin = shutil.which("uv")
    # A venv is required for the uv sync path: UV_PROJECT_ENVIRONMENT below
    # points uv at sys.prefix, which is only a private environment when we're
    # inside one. Pointing it at a system prefix would be actively harmful.
    in_venv = sys.prefix != sys.base_prefix

    if uv_bin and in_venv and (source_root / "uv.lock").exists():
        return (
            [
                uv_bin, "sync",
                # Locate the project regardless of cwd.
                "--project", str(source_root),
                # Install what uv.lock says, and refuse if the lock no longer
                # matches pyproject.toml. `--locked` is the right assertion here
                # rather than `--frozen`: both avoid rewriting the lock, but
                # --frozen skips the freshness check entirely, so a checkout
                # whose pyproject has drifted would be upgraded to a lock that
                # doesn't describe it — silently. Failing tells the user their
                # tree is inconsistent, which is information they need.
                "--locked",
                # Do NOT remove packages absent from the lock. `uv sync` is
                # exact by default, which would silently uninstall optional
                # extras the user chose to add — a regression against the
                # purely additive `pip install -e .` this replaces.
                "--inexact",
            ],
            # Target the venv Nerve is actually installed in. Without this uv
            # syncs `<project>/.venv`, which needn't be the same directory, and
            # VIRTUAL_ENV is unset when the binary is invoked directly
            # (`.venv/bin/nerve upgrade`) rather than after activation.
            {"UV_PROJECT_ENVIRONMENT": sys.prefix},
        )
    # Fallbacks resolve from pyproject's bounds, NOT the lock, so the upgrade is
    # not reproducible. That is still better than refusing to upgrade at all —
    # installs predating the lockfile, and any non-uv environment, depend on
    # these paths — but it is worth saying out loud rather than degrading
    # quietly, since an unpinned upgrade is exactly how #316 kept resurfacing.
    if (source_root / "uv.lock").exists():
        reason = "uv is not installed" if not uv_bin else "not running inside a virtualenv"
        click.secho(
            f"  warning: {reason}, so dependencies will be resolved fresh instead of "
            f"from uv.lock. This upgrade is not reproducible.",
            fg="yellow",
        )

    if uv_bin:
        # ``uv pip`` targets the active venv via ``VIRTUAL_ENV`` / ``sys.executable``
        return (
            [uv_bin, "pip", "install", "-e", str(source_root), "--python", sys.executable],
            {},
        )
    return ([sys.executable, "-m", "pip", "install", "-e", str(source_root)], {})


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
        cmd, extra_env = _dep_install_cmd(source_root)
        click.echo(f"\n[2/3] Installing Python dependencies: {' '.join(cmd)}")
        env = {**os.environ, **extra_env} if extra_env else None
        rc = subprocess.run(cmd, cwd=source_root, env=env).returncode
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

    # Migrate a legacy config layout to workspace/config (idempotent), and show
    # what the gateway will do to nerve.db at its next start.
    if config is not None:
        from nerve.migrate import maybe_migrate
        report = maybe_migrate(
            Path(ctx.obj["config_dir"]),
            workspace=getattr(config, "workspace", None),
            config=config,
        )
        if report and report.identity_actions:
            click.echo("\nAt its next start, the gateway will:")
            for action in report.identity_actions:
                click.echo(f"  - {action}")
        if report and report.did_anything:
            click.echo("\nMigrated config to the workspace layout:")
            for action in report.actions:
                click.echo(f"  - {action}")
            if report.error:
                # The steps above did apply. Saying so beats a bare success
                # message, because the rest has to be finished by hand or by a
                # re-run, and the tracked file already exists either way.
                click.secho(
                    f"  Migration did not finish: {report.error}\n"
                    "  What is listed above was applied; re-run `nerve migrate` "
                    "once the cause is fixed.",
                    fg="yellow",
                )
            if report.secrets_moved:
                click.echo(
                    f"  (scrubbed {len(report.secrets_moved)} secret(s) into "
                    "config.local.yaml; review workspace/config/settings.yaml "
                    "before committing)"
                )
            if report.machine_local_kept:
                click.echo(
                    f"  ({len(report.machine_local_kept)} machine-local key(s) kept "
                    "in config.yaml instead of published: "
                    f"{', '.join(report.machine_local_kept)})"
                )
            if report.suspect_values:
                click.secho(
                    f"  ({len(report.suspect_values)} value(s) left in settings.yaml "
                    f"still look like credentials: {', '.join(report.suspect_values)})",
                    fg="yellow",
                )
            for warning in report.warnings:
                click.secho(f"  Note: {warning}", fg="yellow")

    click.echo("\nUpgrade complete. Restart Nerve for changes to take effect:")
    click.echo("  nerve restart")


def _config_source_label(source: str) -> str:
    """Human-readable name for where the config directory was discovered.

    A function rather than a module-level dict because the pointer label names
    a machine-local path: baked in at import time it would report whatever
    ``NERVE_HOME`` said before the process was fully set up, so doctor output
    could point the reader at a file that is not the one being consulted.
    """
    return {
        "flag": "--config-dir flag",
        "env": "NERVE_CONFIG_DIR env var",
        "cwd": "current directory",
        "pointer": f"{paths.config_pointer_file()} pointer",
        "default": "current directory (no config found anywhere)",
    }.get(source, source)


def _probe_principal_id() -> str | None:
    """The principal that the doctor's model probe names: the system actor.

    External mode only (``NERVE_AUTH_MODE`` in this shell). The doctor opens
    no server database, so it reads the stored system actor read-only.
    ``None`` in local mode, with an invalid mode, or without a database.
    """
    from nerve.config import ConfigError
    from nerve.db.accounts import read_system_actor_id
    from nerve.gateway.auth import is_external_mode

    try:
        if not is_external_mode():
            return None
    except ConfigError:
        return None
    return read_system_actor_id(paths.db_path())


def _check_api_connectivity(config) -> tuple[bool, str]:
    """Make a 1-token API call through the configured provider.

    Catches bad API keys, missing Bedrock model access, and wrong
    inference-profile prefixes — at diagnosis time instead of first message.
    """
    model = config.agent.title_model
    try:
        client = config.create_anthropic_client(
            timeout=15.0, principal_id=_probe_principal_id(),
        )
        client.messages.create(
            model=model,
            max_tokens=1,
            messages=[{"role": "user", "content": "ping"}],
        )
        return True, f"model {model} reachable"
    except Exception as e:
        detail = str(e)
        if len(detail) > 200:
            detail = detail[:200] + "…"
        return False, f"{model}: {detail}"


def _hosted_slack_lines(config, warnings: list[str]) -> list[str]:
    """Doctor lines for the local Slack settings that still apply in hosted mode.

    The gateway decides admission and where the agent may post, so only the
    send tool switch, its local conversation limit, and the notification
    conversation are reported. Slash commands do not reach Nerve.
    """
    from nerve.channels.slack import is_slack_id, notification_target
    from nerve.config import SLACK_HOST_COMMANDS

    slack = config.slack
    lines: list[str] = []
    if not slack.allow_outbound:
        lines.append("[--] Slack send tool off (slack.allow_outbound is false)")
    elif slack.allow_channels or slack.deny_channels:
        lines.append(
            f"[OK] Slack send tool on, limited to {len(slack.allow_channels)} allow "
            f"and {len(slack.deny_channels)} deny rule(s); the gateway also decides"
        )
        if any(not is_slack_id(rule) for rule in slack.deny_channels):
            warnings.append(
                "[WARN] slack.deny_channels has a rule that is not a conversation "
                "id; in hosted mode the send tool then refuses every conversation"
            )
        if any(not is_slack_id(rule) for rule in slack.allow_channels):
            warnings.append(
                "[WARN] slack.allow_channels has a rule that is not a conversation "
                "id; in hosted mode rules match conversation ids only"
            )
    else:
        lines.append("[OK] Slack send tool on; the gateway decides where it may post")
    target = notification_target(config)
    if target:
        lines.append(f"[OK] Slack notifications go to {target}")
    else:
        warnings.append(
            "[WARN] Slack notifications have no conversation: set "
            "notifications.slack_channel_id"
        )
    lines.append("[--] Slack /nerve commands are not available in hosted mode")
    host = [name for name in slack.commands or () if name in SLACK_HOST_COMMANDS]
    if host:
        warnings.append(
            f"[WARN] slack.commands enables {', '.join(host)}, but host commands "
            "are not available in hosted mode"
        )
    return lines


def _source_suffix(config, path: str) -> str:
    """`` (environment)`` / `` (config-repo)`` after a configured connection
    line when the value did not come from a file on this machine; ``""`` for
    the normal case. Never raises — doctor must not die on a report detail."""
    try:
        from nerve import setup_provenance

        return setup_provenance.describe(setup_provenance.credential_sources(config).get(path))
    except Exception:  # noqa: BLE001
        return ""


def _github_token_from_environment(config) -> bool:
    """Whether the GitHub token comes from the environment (``GH_TOKEN`` /
    ``GITHUB_TOKEN``, or a ``${VAR}`` reference) — the one source the setup
    page still offers on an instance that keeps no credentials. Never raises."""
    try:
        from nerve import setup_provenance

        source = setup_provenance.credential_sources(config).get("github_token")
        return source == setup_provenance.SOURCE_ENVIRONMENT
    except Exception:  # noqa: BLE001
        return False


def _telegram_sync_lines(config, warnings: list[str]) -> list[str]:
    """Doctor lines for the Telegram *message sync*: credentials, and whether
    the session is signed in. Reads the session file's presence and the state
    file's note of the last sign-in — no network from a CLI process."""
    lines: list[str] = []
    sync = config.sync.telegram
    if not sync.enabled:
        lines.append("[--] Telegram sync disabled")
        return lines
    if not (sync.api_id and sync.api_hash):
        warnings.append(
            "[WARN] Telegram sync: enabled but sync.telegram.api_id/api_hash are not set — "
            "add them on the setup page (Sources → Telegram) or in config.local.yaml"
        )
        return lines
    lines.append(
        f"[OK] Telegram sync: API credentials configured{_source_suffix(config, 'sync.telegram.api_hash')}"
    )
    try:
        from nerve import setup_state, setup_telegram_login

        present = setup_telegram_login.session_present()
        user = setup_state.load_state().telegram_sync_user
        where = f"{paths.home_label()}/{setup_telegram_login.SESSION_NAME}.session"
    except Exception:  # noqa: BLE001
        return lines
    both_routes = "sign in on the setup page (Sources → Telegram) or run 'nerve setup-telegram' on the server"
    if not present:
        warnings.append(f"[WARN] Telegram sync: no signed-in session at {where} — {both_routes}")
    elif user:
        handle = f"@{user['username']}" if user.get("username") else (user.get("first_name") or f"id {user.get('id')}")
        lines.append(f"[OK] Telegram sync: signed in as {handle}")
    else:
        warnings.append(
            f"[WARN] Telegram sync: a session file exists at {where} but no sign-in has been "
            f"verified — {both_routes}"
        )
    return lines


def _system_principal_report(*, external: bool) -> tuple[list[str], list[str]]:
    """Doctor lines about ``NERVE_SYSTEM_PRINCIPAL_ID``: ``(lines, warnings)``.

    Like the auth mode, the value comes from this shell, not from the running
    server. Nothing is reported when the variable is unset here.
    """
    from nerve.db.accounts import inspect_system_actor
    from nerve.identity import (
        SYSTEM_PRINCIPAL_ENV,
        InvalidSystemPrincipalId,
        system_principal_id_from_env,
    )

    label = f"System actor ({SYSTEM_PRINCIPAL_ENV} from this shell)"
    if not external:
        if os.environ.get(SYSTEM_PRINCIPAL_ENV, "").strip():
            return [f"[--] {label}: set, but it has no effect in local mode"], []
        return [], []
    try:
        wanted = system_principal_id_from_env()
    except InvalidSystemPrincipalId as e:
        return [], [f"[WARN] {label}: {e}. The server keeps the current system actor ID."]
    if wanted is None:
        return [], []
    state = inspect_system_actor(paths.db_path(), wanted)
    if state is None:
        return [f"[--] {label}: no system actor yet; the next start gives it {wanted}"], []
    current, blocker = state
    if current == wanted:
        return [f"[OK] {label}: {current}"], []
    if blocker is None:
        return [f"[--] {label}: {current}; the next start changes it to {wanted}"], []
    return [], [
        f"[WARN] {label}: keeps the current ID {current}, not {wanted}: {blocker}. "
        "Autonomous work is attributed to that ID."
    ]


def doctor_report(config, config_source: str = "", check_api: bool = False) -> str:
    """Run doctor checks and return the report as a plain-text string.

    Used by both the CLI ``nerve doctor`` command and the Telegram ``/doctor``
    command so they share the same diagnostics. ``check_api`` makes a real
    (1-token) API call through the configured provider — enabled for the CLI,
    skipped for the in-daemon Telegram handler to avoid blocking the loop.
    """
    lines: list[str] = []
    errors: list[str] = []
    warnings: list[str] = []

    lines.append("Nerve Doctor")
    lines.append("=" * 40)

    # Where the config came from — surfaces CWD mixups immediately
    config_dir = getattr(config, "config_dir", None)
    if config_dir is not None:
        source_label = _config_source_label(config_source)
        suffix = f" (via {source_label})" if source_label else ""
        has_base = (Path(config_dir) / "config.yaml").exists()
        has_local = (Path(config_dir) / "config.local.yaml").exists()
        from nerve.config import _path_exists, workspace_settings_file
        # The workspace may be a path this account cannot stat (the container's
        # path, seen from a Docker host): absent, not a crash.
        has_settings = _path_exists(workspace_settings_file(config.workspace))
        if has_base or has_local or has_settings:
            present = " + ".join(
                n for n, ok in (
                    ("config.yaml", has_base),
                    ("config.local.yaml", has_local),
                    ("workspace/config/settings.yaml", has_settings),
                ) if ok
            )
            lines.append(f"[OK] Config: {config_dir} ({present}){suffix}")
        else:
            errors.append(
                f"[ERR] No config files in {config_dir}{suffix} — "
                "run 'nerve init', or point at an existing install with "
                "-c/--config-dir or NERVE_CONFIG_DIR"
            )

    # Unknown / misspelled config keys — validate the same merged view that
    # load_config sees (workspace/config/settings.yaml + config.yaml +
    # config.local.yaml), so typos in the shared settings layer are caught too.
    try:
        from nerve.config import _read_config_sources, validate_config_keys
        merged = _read_config_sources(Path(config_dir)) if config_dir else {}
        for w in validate_config_keys(merged):
            warnings.append(f"[WARN] config: {w}")
    except Exception:
        pass

    # Daemon status
    running, pid = _get_daemon_status()
    if running:
        lines.append(f"[OK] Daemon running (PID {pid})")
    else:
        lines.append("[--] Daemon not running")

    # Check workspace
    from nerve.config import _path_exists as _ws_path_exists

    if _ws_path_exists(config.workspace):
        lines.append(f"[OK] Workspace: {config.workspace}")
        # The mode is what the workspace says it is (SOUL.md and friends), not
        # a config key — NerveConfig has none. Before a mode is chosen the
        # template files are not expected yet.
        from nerve.setup_writer import workspace_mode

        detected_mode = workspace_mode(config.workspace)
        mode = detected_mode or "personal"
        try:
            from nerve.workspace import get_expected_files
            expected = get_expected_files(mode)
            if detected_mode is None:
                warnings.append(
                    "  [WARN] no mode chosen yet — open /setup to choose personal or worker "
                    "(the identity files are written then)"
                )
            for f in expected:
                if (config.workspace / f).exists():
                    lines.append(f"  [OK] {f}")
                elif detected_mode is not None:
                    warnings.append(f"  [WARN] {f} not found (expected for {mode} mode)")
        except (FileNotFoundError, ValueError):
            for f in ["SOUL.md", "IDENTITY.md", "MEMORY.md"]:
                if (config.workspace / f).exists():
                    lines.append(f"  [OK] {f}")
                else:
                    warnings.append(f"  [WARN] {f} not found")
    elif _is_docker_mode(config):
        # The host side of a Docker deployment: config names the container's
        # path, which this host may not even stat. The files live at the mount.
        host_ws = _compose_host_workspace(getattr(config, "config_dir", "") or Path.cwd())
        where = f"bind-mounted from {host_ws}" if host_ws is not None else _DOCKER_VIEW_UNAVAILABLE
        lines.append(f"[--] Workspace: {config.workspace} (the container's path; {where})")
    else:
        errors.append(f"[ERR] Workspace not found: {config.workspace}")

    # Workspace sync. Two questions, because the answers come from different
    # places: what the daemon's loop last did (retained in-process, so only the
    # in-daemon /doctor sees it), and whether the reviewed files are clean right
    # now (a git check any process can run, which is the one that matters from a
    # shell — a blocked sync leaves the box pinned to an old revision with
    # nothing but a log line to say so).
    sync_cfg = getattr(config, "workspace_sync", None)
    # Also when locked with the loop off: `nerve config sync` is still the only
    # way config reaches that box, so a refusal is just as terminal there.
    if sync_cfg is not None and (sync_cfg.enabled or config.lockdown):
        try:
            from nerve.sync_service import (
                _describe_paths,
                last_sync_state,
                local_block_reasons,
            )

            state = last_sync_state()
            if state is not None:
                age = max(0, int(time.time() - state.checked_at))
                if state.ok:
                    lines.append(
                        f"[OK] Workspace sync: applied "
                        f"{state.applied_rev[:8] or '(unknown)'}, last checked "
                        f"{age}s ago"
                    )
                else:
                    # Covers the failures the local check below cannot see: a
                    # dead remote, a diverged branch, an invalid bundle.
                    warnings.append(
                        f"[WARN] Workspace sync: last cycle ({age}s ago) failed "
                        f"— {state.message}"
                    )
                for name, why in sorted(state.reload_errors.items()):
                    warnings.append(
                        f"[WARN] Workspace sync: {name} did not take the merged "
                        f"config ({why}) — retried every cycle"
                    )
            blocking = local_block_reasons(config.workspace, config.lockdown)
            if blocking:
                warnings.append(
                    f"[WARN] Workspace sync BLOCKED: the reviewed files have "
                    f"local changes, so no merged config reaches this instance "
                    f"— {_describe_paths(blocking)}. Commit them, discard them, "
                    f"or re-propose them as a PR with propose_config_change"
                )
            else:
                lines.append("[OK] Workspace sync: reviewed files clean")
        except Exception as e:
            warnings.append(f"[WARN] Workspace sync check failed: {e}")

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

    # Check API keys / provider
    if config.provider.is_bedrock:
        region = config.provider.aws_region or "(default)"
        lines.append(f"[OK] Provider: AWS Bedrock, region {region}")
        # Inference-profile prefix must match the region's geography
        # (us./eu./apac.) — a mismatch means instant 400s.
        from nerve.bootstrap import bedrock_geo_prefix
        expected = bedrock_geo_prefix(config.provider.aws_region)
        for label, model in (
            ("agent.model", config.agent.model),
            ("agent.cron_model", config.agent.cron_model),
        ):
            geo = model.split(".", 1)[0] if "." in model else ""
            if geo in ("us", "eu", "apac", "global") and geo not in (expected, "global"):
                warnings.append(
                    f"[WARN] {label} '{model}' uses the '{geo}.' inference profile "
                    f"but region {region} is '{expected}.' — expect 400 errors"
                )
    elif config.proxy.enabled:
        if config.anthropic_api_key:
            lines.append("[--] Anthropic API key set (proxy takes precedence)")
        else:
            lines.append("[--] Anthropic API key not set (using proxy)")
    elif config.anthropic_api_key:
        lines.append(
            f"[OK] Anthropic API key: ...{config.anthropic_api_key[-4:]}"
            f"{_source_suffix(config, 'anthropic_api_key')}"
        )
    elif os.environ.get("ANTHROPIC_API_KEY"):
        # Provisioned by the operator: the gateway's environment carries it
        # (Docker entrypoint, systemd Environment=), nothing in the files.
        lines.append("[OK] Anthropic API key: configured (environment)")
    elif config.claude_oauth_token or os.environ.get("CLAUDE_CODE_OAUTH_TOKEN"):
        where = _source_suffix(config, "claude_oauth_token") if config.claude_oauth_token else " (environment)"
        lines.append(f"[OK] Claude subscription token: configured{where}")
        warnings.append(
            "[WARN] Only a subscription token is configured — background jobs "
            "(memory, titles, cron) need an API key"
        )
    elif config.lockdown:
        # The generic message names config.local.yaml, which a locked instance
        # never reads. The cause is also often upstream of the key: if the
        # provider block was only in config.yaml, this instance has already
        # reverted to the Anthropic default and is failing for a key it would not
        # otherwise need.
        errors.append(
            "[ERR] No Anthropic API key. Lockdown does not read "
            "config.local.yaml, so supply it as ${ANTHROPIC_API_KEY} in "
            "workspace/config/settings.yaml. If this box should be using "
            "Bedrock, provider.type is absent from the tracked settings and this "
            "instance has fallen back to 'anthropic'"
        )
    else:
        errors.append("[ERR] Anthropic API key not set and proxy not enabled (config.local.yaml)")

    # Live connectivity probe (CLI only — see check_api docstring note)
    if check_api and (config.provider.is_bedrock or config.anthropic_api_key):
        ok, detail = _check_api_connectivity(config)
        if ok:
            lines.append(f"[OK] Claude API: {detail}")
        else:
            errors.append(f"[ERR] Claude API: {detail}")

    if config.openai_api_key:
        lines.append(
            f"[OK] OpenAI API key: ...{config.openai_api_key[-4:]}"
            f"{_source_suffix(config, 'openai_api_key')} (vector embeddings enabled)"
        )
    else:
        lines.append("[--] OpenAI API key not set (using LLM-based memory recall)")

    # Check Telegram
    if config.telegram.enabled:
        if config.telegram.bot_token:
            lines.append(
                f"[OK] Telegram bot token: ...{config.telegram.bot_token[-4:]}"
                f"{_source_suffix(config, 'telegram.bot_token')}"
            )
            if config.telegram.dm_policy == "open":
                warnings.append(
                    "[WARN] telegram.dm_policy is 'open' — any Telegram user "
                    "can talk to this bot"
                )
            elif not config.telegram.allowed_users:
                warnings.append(
                    "[WARN] telegram.allowed_users is empty — the bot rejects "
                    "all DMs until you pair (run 'nerve pair', then send the "
                    "bot /pair <code> in a private chat)"
                )
        else:
            errors.append("[ERR] Telegram enabled but bot_token not set")
    else:
        lines.append("[--] Telegram disabled")

    # Telegram message sync (the Telethon source): API credentials alone do
    # nothing — it needs a signed-in session of the person's own account.
    lines.extend(_telegram_sync_lines(config, warnings))

    # Check Slack
    if config.slack.mode == "hosted":
        # A gateway holds the tokens, so there are none to check here. The
        # settings are checked also when Slack is switched off, because the
        # hosted channel is built at startup and a reload cannot repair it.
        from nerve.channels.hosted.auth import KeyFileError, read_key_file

        if not config.slack.enabled:
            lines.append(
                "[--] Slack is hosted but switched off (slack.enabled is false); "
                "events wait in the gateway inbox"
            )
        hosted = config.channels.hosted
        problems = hosted.problems()
        if not problems:
            try:
                keys = read_key_file(hosted.gateway_jwks_file)
            except KeyFileError as error:
                problems = [f"channels.hosted.gateway_jwks_file: {error}"]
        if problems:
            errors.extend(
                f"[ERR] Slack is hosted but {problem}" for problem in problems
            )
        else:
            lines.append(
                f"[OK] Slack hosted by the channel gateway: agent "
                f"{hosted.agent_id}, {len(keys)} gateway key(s) from "
                f"{hosted.gateway_jwks_file}"
            )
            lines.extend(_hosted_slack_lines(config, warnings))
    elif config.slack.enabled:
        missing = [
            name for name, value in (
                ("bot_token", config.slack.bot_token),
                ("app_token", config.slack.app_token),
            ) if not value
        ]
        if missing:
            errors.append(
                f"[ERR] Slack enabled but {', '.join(missing)} not set "
                "(Socket Mode needs both)"
            )
        else:
            lines.append(
                f"[OK] Slack bot token: ...{config.slack.bot_token[-4:]}"
                f"{_source_suffix(config, 'slack.bot_token')}"
            )
            slack = config.slack
            if not (
                slack.allow_users
                or slack.allow_channels
                or slack.allow_direct_messages
            ):
                warnings.append(
                    "[WARN] slack.allow_users and slack.allow_channels are "
                    "empty and slack.allow_direct_messages is false — the bot "
                    "refuses every message. Add your Slack member id to "
                    "slack.allow_users"
                )
            else:
                lines.append(
                    f"[OK] Slack guardrails: {len(slack.allow_users)} allowed "
                    f"user(s), {len(slack.allow_channels)} allowed channel(s), "
                    f"{len(slack.deny_users) + len(slack.deny_channels)} deny "
                    f"rule(s), direct messages "
                    f"{'allowed' if slack.allow_direct_messages else 'refused'}"
                )
    else:
        lines.append("[--] Slack disabled")

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

    # Check auth. The credential lives on the account row, not in configuration:
    # the startup migration copied auth.password_hash across and removed the key,
    # so reading that key here would call a perfectly well-secured install
    # "passwordless". Read-only, and tolerant of a database that does not exist
    # yet (None) — that is a fresh install, which the wizard is about to shape.
    #
    # Which rows count as having a password is asked of the same function the
    # login route asks, not restated: a `none` row reads auth.password_hash too
    # (a reload that adds one takes effect before the next restart re-derives
    # the row), so judging by the row alone told operators that an *active*
    # password did nothing — and removing it on that advice would have opened
    # the instance.
    #
    # In external mode, the gateway names people and local accounts are not
    # used, so the account checks do not apply. The doctor does not ask the
    # running server for its mode: it reads NERVE_AUTH_MODE in its own shell,
    # and the environment of the daemon can be different.
    from nerve.config import ConfigError
    from nerve.db.accounts import inspect_bootstrap_state, read_setup_required
    from nerve.gateway.auth import (
        AUTH_MODE_ENV,
        AUTH_MODE_EXTERNAL,
        auth_mode_from_env,
        source_authenticates,
    )

    mode_source = f"from {AUTH_MODE_ENV} in this shell, not from the running server"
    try:
        mode = auth_mode_from_env()
    except ConfigError as e:
        mode = None
        errors.append(
            f"[ERR] Auth mode ({mode_source}): {e} The server does not start "
            "with this value."
        )
    else:
        lines.append(f"[OK] Auth mode: {mode} ({mode_source})")
    external = mode == AUTH_MODE_EXTERNAL

    configured = bool(config.auth.password_hash)
    identity_state = inspect_bootstrap_state(paths.db_path())
    sources = identity_state[0] if identity_state is not None else None
    usable = [
        source for source in (sources or [])
        if source_authenticates(source, configured_password=configured)
    ]
    if external:
        lines.append(
            "[--] Accounts: not used in external mode; the gateway names the "
            "person behind each request"
        )
    elif sources is None:
        lines.append("[--] Accounts: nerve.db not created yet (first start will)")
    elif not sources:
        warnings.append("[WARN] No local account yet — the next start creates one")
    elif not usable and read_setup_required(
        paths.db_path(), configured_password=configured,
    ):
        warnings.append(
            "[WARN] Setup is not complete — sign-in is refused until it is. "
            "Complete it at /setup with the setup token shown by `nerve status` "
            "on the host" + _shell_view_caveat()
        )
    elif not usable:
        warnings.append(
            "[WARN] No password set — passwordless by choice: anyone who can "
            "reach the gateway acts as the owner"
        )
    else:
        lines.append(
            f"[OK] Accounts: {len(sources)} ({len(usable)} with a password)"
        )
    if configured and sources is not None and not external:
        reading = [source for source in sources if source != "local"]
        if reading:
            lines.append(
                f"[OK] auth.password_hash is in use by {len(reading)} account(s) "
                "— the next start copies it onto them and removes the key"
            )
        else:
            warnings.append(
                "[WARN] auth.password_hash is set but no account uses it — every "
                "account has its own password now; the configured value does nothing"
            )
    if mode is not None:
        ok, warn = _system_principal_report(external=external)
        lines.extend(ok)
        warnings.extend(warn)

    # The setup checklist: the same derivation the setup page uses, over the
    # facts this process can read. Never prints the token — `nerve status`
    # is the one place that does. On the host of a Docker deployment the
    # files are read at their bind-mounted path (_checklist_config).
    checklist_config, host_note = _checklist_config(config)
    summary = _setup_summary(checklist_config) if checklist_config is not None else None
    if summary is None and host_note:
        lines.append(f"[--] Setup checklist: not derived here — {host_note}")
    if summary is not None:
        pending = [s["id"] for s in summary["steps"] if s["status"] == "pending"]
        caveat = _shell_view_caveat()
        if summary["finished"]:
            lines.append(f"[OK] Setup checklist complete{caveat}")
        elif pending:
            warnings.append(
                f"[WARN] Setup checklist incomplete (steps: {', '.join(pending)}) — "
                "finish at /setup"
                + ("" if external else " (run 'nerve status' for the token while unclaimed)")
                + caveat
            )
        else:
            lines.append(f"[--] Setup checklist: nothing left to decide here{caveat}")
    if not getattr(getattr(config, "setup", None), "local_secrets", True):
        # The setup page stores no credential here (setup.local_secrets:
        # false): say so once, next to the checklist it shapes. A GitHub
        # token from the environment keeps that one source on offer.
        closed = "the Memory, Channels, Sources and Agents steps are not offered"
        if _github_token_from_environment(config):
            closed = (
                "the Memory, Channels and Agents steps and the Gmail and Telegram "
                "sources are not offered; GitHub comes from the environment"
            )
        lines.append(
            "[--] Setup page keeps no credentials on this instance "
            f"(setup.local_secrets: false) — connections come from the operator; {closed}"
        )
    try:
        from nerve import setup_state as _setup_state

        telegram_error = _setup_state.load_state().boot_errors.get("telegram")
    except Exception:  # noqa: BLE001
        telegram_error = None
    if telegram_error:
        (errors if running else warnings).append(
            f"[{'ERR' if running else 'WARN'}] Telegram bot failed to start at the last "
            f"boot: {telegram_error} — replace the token at /setup/channels or in "
            "config.local.yaml"
        )

    if config.auth.jwt_secret:
        lines.append("[OK] JWT secret configured")
    elif _signing_secret(config):
        lines.append("[OK] JWT secret: generated on first start, kept in nerve.db")
    else:
        lines.append(
            "[--] JWT secret: none configured — one is generated into nerve.db on first start"
        )

    # Check DB
    db_path = paths.db_path()
    if db_path.exists():
        lines.append(f"[OK] Database: {db_path} ({db_path.stat().st_size / 1024:.1f} KB)")
    else:
        lines.append(f"[--] Database will be created at: {db_path}")

    # Check cron files (merge like the scheduler does: user jobs override system by ID)
    try:
        from nerve.cron.jobs import describe_reserved_job_ids, is_reserved_job_id, load_jobs

        system_jobs = load_jobs(config.cron.system_file) if _ws_path_exists(config.cron.system_file) else []
        user_jobs = load_jobs(config.cron.jobs_file) if _ws_path_exists(config.cron.jobs_file) else []

        # Merge: user jobs override system jobs with the same ID
        merged: dict[str, object] = {j.id: j for j in system_jobs}
        overridden = sum(1 for j in user_jobs if j.id in merged)
        for j in user_jobs:
            merged[j.id] = j

        # Jobs on a reserved id are dropped by the scheduler, so counting them
        # would report crons that can never fire. Call them out instead.
        reserved_ids = sorted(jid for jid in merged if is_reserved_job_id(jid))
        for jid in reserved_ids:
            del merged[jid]
        if reserved_ids:
            warnings.append(
                f"[WARN] Cron job(s) {', '.join(reserved_ids)} use ids reserved "
                f"by the daemon ({describe_reserved_job_ids()}) and are never "
                f"scheduled — rename them"
            )

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
        if _ws_path_exists(config.cron.system_file):
            lines.append(f"[OK] System crons: {config.cron.system_file}")
        if _ws_path_exists(config.cron.jobs_file):
            lines.append(f"[OK] User crons: {config.cron.jobs_file}")

    # Check backups
    backup_cfg = getattr(config, "backup", None)
    if backup_cfg is not None and backup_cfg.enabled:
        if not backup_cfg.target_dir:
            warnings.append(
                "[WARN] backup.enabled is true but backup.target_dir is empty "
                "— no backups will run"
            )
        else:
            try:
                from nerve import backup as _backup_mod

                target = Path(backup_cfg.target_dir).expanduser()
                age = _backup_mod.latest_bundle_age_seconds(target)
                kept = len(_backup_mod.list_bundles(target))
                stale_after = 2 * max(1, backup_cfg.interval_hours) * 3600
                if age is None:
                    warnings.append(
                        f"[WARN] Backups enabled but none found in {target}"
                    )
                else:
                    age_h = age / 3600
                    age_str = (
                        f"{age_h:.1f}h" if age_h < 48 else f"{age_h / 24:.1f}d"
                    )
                    if age > stale_after:
                        warnings.append(
                            f"[WARN] Last backup is {age_str} old "
                            f"(> 2× interval of {backup_cfg.interval_hours}h) "
                            f"— check the scheduled job ({target})"
                        )
                    else:
                        lines.append(
                            f"[OK] Backups: last {age_str} ago, {kept} kept in {target}"
                        )
            except Exception as e:
                warnings.append(f"[WARN] Backup check failed: {e}")
    else:
        lines.append(
            "[--] Backups not configured — set backup.target_dir + enabled to "
            f"protect {paths.home_label()} against disk loss"
        )

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
    if config is None:
        # The config wouldn't load at all — that *is* the diagnosis.
        click.echo("Nerve Doctor")
        click.echo("=" * 40)
        click.echo(f"Config dir: {ctx.obj.get('config_dir')}")
        click.secho(
            f"[ERR] config failed to load: {ctx.obj.get('config_error')}", fg="red"
        )
        ctx.exit(1)
    report = doctor_report(
        config,
        config_source=ctx.obj.get("config_source", ""),
        check_api=True,
    )
    click.echo(report)
    if "[ERR]" in report:
        ctx.exit(1)


# Bind addresses that name no connectable destination, so a client on the same
# box has to ask for loopback instead.
_WILDCARD_BINDS = ("", "0.0.0.0", "::", "*")


def _signing_secret(config) -> str:
    """The JWT secret the local daemon signs with, read from a CLI process.

    ``auth.jwt_secret`` if set, else the secret the daemon generated into
    ``nerve.db``, read directly from the file. Empty if the daemon has never
    started.
    """
    if config.auth.jwt_secret:
        return config.auth.jwt_secret
    from nerve.db.accounts import JWT_SECRET_NAME, read_instance_secret

    return read_instance_secret(paths.db_path(), JWT_SECRET_NAME)


def _gateway_url(config, path: str) -> str:
    """URL for a loopback call to this box's own gateway.

    ``gateway.host`` is a *bind* address, not necessarily somewhere a client can
    connect to; see :data:`_WILDCARD_BINDS`.
    """
    scheme = "https" if config.gateway.ssl.enabled else "http"
    host = config.gateway.host
    if host in _WILDCARD_BINDS:
        host = "127.0.0.1"
    return f"{scheme}://{host}:{config.gateway.port}{path}"


@main.command()
@click.pass_context
def reload(ctx: click.Context) -> None:
    """Apply config edits to the running daemon without restarting it.

    Re-reads config.yaml, config.local.yaml and the workspace settings, then
    reloads cron jobs, cron sources, MCP servers and skills. Anything that needs
    a restart instead is listed rather than silently skipped; `docs/config.md`
    has the full table.
    """
    import httpx

    from nerve.gateway.auth import create_system_token

    config = ctx.obj["config"]
    if config is None:
        raise click.ClickException(
            f"Config could not be loaded ({ctx.obj.get('config_error')}); "
            "run 'nerve config validate' to see why."
        )
    # Without a secret there is nothing to sign with, and the gateway refuses
    # unauthenticated requests, so fail here with a clear message.
    secret = _signing_secret(config)
    if not secret:
        raise click.ClickException(
            "No signing secret is available to this shell: auth.jwt_secret is not "
            "in the config read here and nerve.db holds no generated one yet. The "
            "gateway refuses unauthenticated requests"
            + (", and a locked gateway never runs open" if config.lockdown else "")
            + ", so nothing sent from here can succeed. If the secret comes from "
            "${ENV_VAR}, export it in this shell too; if the daemon has never "
            "started, start it first."
        )
    url = _gateway_url(config, "/api/config/reload")
    # Certificate verification stands except in the one case where it cannot
    # hold: a wildcard bind means the request goes to 127.0.0.1, and the
    # daemon's certificate is issued for a hostname, so checking it there would
    # fail on exactly the setups that bothered to configure TLS. When
    # gateway.host names a real host the certificate should match it, and a
    # failure there is worth hearing about rather than skipping past.
    verify = config.gateway.host not in _WILDCARD_BINDS
    # This shell is the instance talking to itself, not a person: the token
    # resolves to the agent's system principal rather than standing in for
    # whichever account happens to exist.
    headers = {"Authorization": f"Bearer {create_system_token(secret)}"}
    try:
        resp = httpx.post(
            url,
            headers=headers,
            verify=verify,
            # A reload re-reads config and rebuilds cron, sources, MCP and
            # skills; MCP servers in particular can take a while to come up.
            timeout=120,
        )
    except httpx.ConnectError:
        raise click.ClickException(
            f"No daemon answering at {url}, which is the address in the config "
            "read here. If gateway.host, gateway.port or the SSL settings were "
            "part of the edit, the running daemon is still bound to the old "
            "address and only a restart moves it. Otherwise nothing is running, "
            "and a stopped daemon reads config fresh when it starts "
            "('nerve start')."
        ) from None
    except httpx.HTTPError as e:
        raise click.ClickException(f"Could not reach {url}: {e}") from None
    if resp.status_code in (401, 403):
        raise click.ClickException(
            "The gateway rejected the request. The running daemon's "
            "auth.jwt_secret is not the one read here — it either started with a "
            "different value, or with one this config no longer supplies. Restart "
            "it to pick up the current config."
        )
    if resp.status_code != 200:
        raise click.ClickException(f"{url} returned {resp.status_code}: {resp.text[:400]}")

    body = resp.json()
    errors = body.get("errors") or {}
    for name, outcome in (body.get("detail") or {}).items():
        if name == "restart_required":
            continue
        if name in errors:
            click.secho(f"  [ERR] {name}: {errors[name]}", fg="red")
            continue
        if isinstance(outcome, dict):
            outcome = ", ".join(f"{k}={v}" for k, v in outcome.items())
        click.echo(f"  {name}: {outcome}")
    if body.get("restart_required"):
        click.secho(
            f"  [WARN] changed but needs a restart: {body['restart_required']}",
            fg="yellow",
        )
    if body.get("ok"):
        click.secho("Config reloaded", fg="green")
    else:
        # Partial by design: a bad settings.yaml must not block a valid cron
        # edit. Non-zero so a script doesn't read this as a clean apply.
        click.secho("Config reload incomplete — see the errors above", fg="red")
        ctx.exit(1)


@main.command()
@click.option("--dry-run", is_flag=True, help="Show what would change without writing.")
@click.pass_context
def migrate(ctx: click.Context, dry_run: bool) -> None:
    """Migrate a legacy config layout into the workspace/config subtree.

    Splits config.yaml: shareable keys → workspace/config/settings.yaml (secrets
    scrubbed into config.local.yaml as ${ENV_VAR} refs), machine-local keys stay
    in config.yaml. Also moves ~/.nerve/cron → workspace/config/cron.
    Non-destructive (originals kept as *.migrated) and idempotent.

    Also shows what the gateway will do to nerve.db at its next start: create
    the local owner account and, if auth.jwt_secret is not set, a JWT signing
    secret. This command does not write them.
    """
    from nerve.migrate import migrate as run_migrate

    config = ctx.obj["config"]
    config_dir = Path(ctx.obj["config_dir"])
    workspace = getattr(config, "workspace", None) if config is not None else None
    try:
        report = run_migrate(
            config_dir, workspace=workspace, dry_run=dry_run, config=config,
        )
    except Exception as e:  # e.g. a malformed config.local.yaml
        raise click.ClickException(f"Migration failed: {e}") from e

    if not report.did_anything and not report.did_bootstrap:
        click.secho(
            "Nothing to migrate: already on the workspace layout, and the local "
            "account is in place.",
            fg="green",
        )
        for warning in report.warnings:
            click.secho(f"  Note: {warning}", fg="yellow")
        return
    prefix = "[dry-run] would " if dry_run else ""
    for action in report.actions:
        click.echo(f"  {prefix}{action}")
    if report.identity_actions:
        click.echo("\n  At its next start, the gateway will:")
        for action in report.identity_actions:
            click.echo(f"  - {action}")
    for warning in report.warnings:
        click.secho(f"\n  Note: {warning}", fg="yellow")
    if report.secrets_moved:
        click.echo(f"\n  Secrets scrubbed to config.local.yaml: {', '.join(report.secrets_moved)}")
    if report.machine_local_kept:
        click.echo(
            "\n  Machine-local keys kept in config.yaml: "
            f"{', '.join(report.machine_local_kept)}"
        )
    if report.suspect_values:
        click.secho(
            f"\n  {len(report.suspect_values)} value(s) headed for settings.yaml still "
            f"look like credentials — review: {', '.join(report.suspect_values)}",
            fg="yellow",
        )
    if dry_run:
        click.secho("\nDry run — no changes written.", fg="yellow")
    elif report.did_anything:
        click.secho("\nMigration complete. Review workspace/config/settings.yaml before committing.", fg="green")
    else:
        click.secho("\nThe config layout needs no migration.", fg="green")


@main.group(name="config")
def config_group() -> None:
    """Configuration commands."""


@config_group.command("sync")
@click.option("--branch", default="", help="Branch to pull (default: current tracking branch).")
@click.option("--no-validate", is_flag=True, help="Skip validating the pulled bundle.")
@click.option(
    "--no-strict-env", is_flag=True,
    help="Allow unset ${ENV_VAR} references in the pulled bundle. Normally they "
         "block the merge, because the daemon refuses to load a config with an "
         "unresolved required variable. Use this when your shell doesn't carry "
         "the daemon's environment (systemd/docker). Otherwise follows "
         "workspace_sync.strict_env.",
)
@click.pass_context
def config_sync(
    ctx: click.Context, branch: str, no_validate: bool, no_strict_env: bool,
) -> None:
    """Pull the workspace from its git remote (the shared config repo).

    Fast-forward only. This moves the files; it does not tell a running daemon
    about them. The daemon applies them on its next sync cycle, because that loop
    compares what is on disk against the revision it last applied. For that to
    happen sooner, run `nerve reload`: POST /api/config/sync would find nothing
    left to merge and reload nothing.
    """
    from nerve.sync_service import sync_workspace

    config = ctx.obj["config"]
    if config is None:
        raise click.ClickException("Config could not be loaded; run 'nerve doctor'.")
    # Raw values, not Path(...): sync_workspace coerces inside its own
    # never-raises guard, so a bad `workspace` reports instead of traceback.
    result = sync_workspace(
        config.workspace, ctx.obj["config_dir"], branch=branch,
        validate=not no_validate,
        strict_env=config.workspace_sync.strict_env and not no_strict_env,
        locked=config.lockdown,
    )
    for warning in result.validation_warnings:
        click.secho(f"  [WARN] {warning}", fg="yellow")
    if result.ok:
        click.secho(result.message, fg="green")
    else:
        for err in result.validation_errors:
            click.secho(f"  [ERR] {err}", fg="red")
        click.secho(result.message, fg="red")
        ctx.exit(1)


@config_group.command("validate")
@click.option(
    "--workspace", "workspace", type=click.Path(), default=None,
    help="Validate this workspace's config (e.g. a checked-out config repo: "
         "--workspace .). Defaults to the resolved workspace.",
)
@click.option(
    "--strict-env", is_flag=True,
    help="Fail if any ${ENV_VAR} reference is unset. Default: allow (CI usually "
         "has no secrets); unset refs are reported as info.",
)
@click.option(
    "--strict-keys", is_flag=True,
    help="Fail on unknown/misspelled config keys. Default: warn (a key from a "
         "newer nerve shouldn't fail CI). Recommended in CI on a config repo, "
         "where the validator is pinned to the deployed nerve version.",
)
@click.option(
    "--portable-only", is_flag=True,
    help="Ignore this machine's config.yaml / config.local.yaml and validate "
         "only the portable workspace config — what a shared repo carries.",
)
@click.option(
    "--assume-lockdown", "assume_locked", is_flag=True,
    help="Validate the locked view whatever this bundle's lockdown flag resolves "
         "to here. A fleet repo writes `lockdown: ${NERVE_LOCKDOWN:-false}`, so "
         "CI resolves it to false and checks a config no locked box will run. Use "
         "this in CI on any repo one of whose instances is locked.",
)
@click.pass_context
def config_validate(
    ctx: click.Context, workspace: str | None, strict_env: bool, strict_keys: bool,
    portable_only: bool, assume_locked: bool,
) -> None:
    """Validate the configuration bundle. Non-zero exit on any error (CI-ready)."""
    from nerve.config_validate import render_result, validate_config_bundle

    config_dir = Path(ctx.obj["config_dir"])
    result = validate_config_bundle(
        config_dir, workspace_override=workspace,
        strict_env=strict_env, strict_keys=strict_keys,
        portable_only=portable_only, assume_locked=assume_locked,
    )
    # Same renderer as `python -m nerve.config_validate` (the no-install path),
    # just colorized here.
    lines, summary = render_result(result)
    _SEVERITY_COLORS = {"[WARN]": "yellow", "[ERR]": "red"}
    for line in lines:
        click.secho(line, fg=_SEVERITY_COLORS.get(line.split(" ", 1)[0]))
    click.secho(summary, fg="green" if result.ok else "red")
    if not result.ok:
        ctx.exit(1)


@config_group.command("init-repo")
@click.option(
    "--workspace", "workspace", type=click.Path(), default=None,
    help="Workspace to scaffold (default: the resolved workspace).",
)
@click.option(
    "--dry-run", is_flag=True, help="Show what would be created without writing.",
)
@click.pass_context
def config_init_repo(
    ctx: click.Context, workspace: str | None, dry_run: bool,
) -> None:
    """Scaffold the workspace into a shareable git config repo.

    Drops a CI validation workflow, a secrets-aware .gitignore, and a README into
    the workspace (never overwriting existing files), then prints the remaining
    git/gh and instance-config steps. Safe to re-run.
    """
    from nerve.config_repo import scaffold_config_repo

    if workspace:
        ws = Path(workspace).expanduser()
    else:
        config = ctx.obj["config"]
        if config is None:
            raise click.ClickException(
                "Config could not be loaded; pass --workspace or run 'nerve doctor'."
            )
        ws = Path(config.workspace)

    if not ws.is_dir():
        raise click.ClickException(f"Workspace does not exist: {ws}")

    result = scaffold_config_repo(ws, dry_run=dry_run)

    verb = "Would create" if dry_run else "Created"
    for rel in result.created:
        click.secho(f"  {verb}: {rel}", fg="green")
    for rel in result.skipped:
        click.secho(f"  Skipped (exists): {rel}", fg="yellow")
    if not result.created:
        click.echo("Config-repo scaffold already in place — nothing to create.")

    # Remaining steps the CLI can't do for you (they need a repo URL + auth).
    click.secho("\nNext steps:", bold=True)
    click.echo(f"  cd {ws}")
    if not result.is_git_repo:
        click.echo("  git init && git add -A && git commit -m 'Initial Nerve config'")
    else:
        click.echo("  git add -A && git commit -m 'Add config-repo scaffold'")
    click.echo(
        "  gh repo create <org>/nerve-config --private --source=. "
        "--remote=origin --push"
    )
    click.echo(
        "\nCI validates every PR out of the box (no secrets needed) — see\n"
        ".github/workflows/validate-config.yml. It installs nerve from the\n"
        "default branch, which stays at or ahead of this instance; append a ref\n"
        "to that install line to check against one release instead."
    )
    click.echo(
        "\nOn the instance, enable sync in workspace/config/settings.yaml:\n"
        "  workspace_sync:\n"
        "    enabled: true\n"
        "    branch: main\n"
        "    interval_minutes: 5\n"
        "Then verify — the same check CI runs, so a pass here means a green PR:\n"
        f"  nerve config validate --workspace {ws} --portable-only --strict-keys\n"
        "Drop --portable-only to include this machine's config.yaml and\n"
        "config.local.yaml, i.e. what the daemon itself loads.\n"
        "When ready to lock down, set 'lockdown: true' in settings.yaml (move\n"
        "secrets to ${ENV_VAR} first — see docs/config.md)."
    )


@main.group()
def codex() -> None:
    """Inspect and authenticate the Codex integration."""


@codex.command("token")
@click.option(
    "--hours", type=click.IntRange(1, 24), default=8, show_default=True,
    help="Token lifetime.",
)
@click.pass_context
def codex_token(ctx: click.Context, hours: int) -> None:
    """Print a fresh MCP-only token for NERVE_MCP_TOKEN.

    Example: export NERVE_MCP_TOKEN="$(nerve codex token)"
    """
    from nerve.gateway.auth import create_external_mcp_token

    secret = _signing_secret(ctx.obj["config"])
    if not secret:
        # No secret yet (the daemon has never started), so print nothing.
        return
    click.echo(create_external_mcp_token(secret, ttl_seconds=hours * 60 * 60))


@codex.command("doctor")
@click.option("--json-output", is_flag=True, help="Emit machine-readable JSON.")
@click.pass_context
def codex_doctor(ctx: click.Context, json_output: bool) -> None:
    """Check CLI version, auth, model protocol, and Ultracode state."""
    import json
    from types import SimpleNamespace

    from nerve.agent.backends.codex.backend import CodexBackend
    from nerve.agent.backends.codex.ultracode import (
        installation_status,
        recoverable_runs,
    )

    config = ctx.obj["config"]
    # deps.config is a callable, not the object — see BackendDeps. Nothing
    # reloads in a one-shot command, but the backend reads it as a callable.
    backend = CodexBackend(SimpleNamespace(config=lambda: config))
    status = asyncio.run(backend.preflight(force=True))
    report = {
        "preflight": status,
        "ultracode": installation_status(config),
        "recoverable_runs": recoverable_runs(config),
        "external_token_env": bool(os.environ.get("NERVE_MCP_TOKEN")),
    }
    if json_output:
        click.echo(json.dumps(report, indent=2, sort_keys=True))
    else:
        if status.get("available"):
            click.echo(f"[OK] {status.get('version')} ({status.get('auth')})")
            click.echo(f"[OK] Models: {', '.join(status.get('models') or [])}")
            hidden = status.get("hidden_models") or []
            if hidden:
                click.echo(
                    "[--] Hidden catalog entries (opt in via codex.models): "
                    + ", ".join(hidden)
                )
            if status.get("auth_mismatch"):
                click.echo(
                    "[ERR] Codex auth mismatch: configured "
                    f"{status.get('configured_auth')}, effective "
                    f"{status.get('auth')}"
                )
        else:
            click.echo(f"[ERR] Codex preflight: {status.get('reason', 'failed')}")
        plugin = report["ultracode"]
        marker = "OK" if plugin.get("installed") else "--"
        click.echo(
            f"[{marker}] Ultracode: "
            f"{plugin.get('version') or 'not installed'} "
            f"({plugin.get('revision') or 'no revision'})"
        )
        click.echo(
            f"[{'OK' if report['external_token_env'] else '--'}] "
            "NERVE_MCP_TOKEN environment"
        )
        if report["recoverable_runs"]:
            click.echo(
                f"[WARN] {len(report['recoverable_runs'])} recoverable "
                "Ultracode run(s)"
            )
    if not status.get("available") or status.get("auth_mismatch"):
        ctx.exit(1)


@main.command()
@click.pass_context
def pair(ctx: click.Context) -> None:
    """Generate a Telegram pairing code.

    Send /pair <code> to your bot in a private chat within an hour to
    authorize your Telegram account. The paired user ID is persisted to
    config.local.yaml (telegram.allowed_users).
    """
    config = ctx.obj["config"]

    if not config.telegram.enabled or not config.telegram.bot_token:
        click.echo("Telegram is not configured (telegram.bot_token missing).")
        click.echo("Set it up in config.local.yaml, or re-run 'nerve init'.")
        ctx.exit(1)
        return

    if config.telegram.dm_policy != "pairing":
        click.echo(
            f"telegram.dm_policy is '{config.telegram.dm_policy}' — pairing is "
            "only used with dm_policy: pairing."
        )
        ctx.exit(1)
        return

    from nerve.pairing import CODE_TTL_SECONDS, generate_pairing_code

    code = generate_pairing_code()
    running, _pid = _get_daemon_status()

    click.echo()
    click.secho(f"  Pairing code: {code}", bold=True)
    click.echo()
    click.echo(f"  In a private chat, send this to your bot:  /pair {code}")
    click.echo(f"  Valid for {CODE_TTL_SECONDS // 60} minutes, single use.")
    if not running:
        click.echo()
        click.secho(
            "  Note: Nerve is not running — start it first ('nerve start'), "
            "then send the command.",
            fg="yellow",
        )
    click.echo()


@main.command()
@click.argument("source", default="all")
@click.pass_context
def sync(ctx: click.Context, source: str) -> None:
    """Run source sync manually (telegram, gmail, github, or all)."""
    config = ctx.obj["config"]

    async def _run():
        from nerve.migrate import open_production_db
        from nerve.sources.registry import build_source_runners

        db = await open_production_db(config)
        engine = None
        try:
            engine = await _start_one_shot_engine(config, db)

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
                    + (f", {result.records_dropped} dropped" if result.records_dropped else "")
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
            await _stop_one_shot_engine(engine)
            await db.close()

    click.echo(f"Running sync: {source}")
    _run_against_db(ctx, _run())


@main.command("setup-telegram")
@click.pass_context
def setup_telegram(ctx: click.Context) -> None:
    """Authenticate Telethon for the Telegram source (interactive)."""
    config = ctx.obj["config"]

    api_id = config.sync.telegram.api_id
    api_hash = config.sync.telegram.api_hash
    if not api_id or not api_hash:
        click.echo(
            "Error: sync.telegram.api_id and api_hash are not set. Add them on the setup "
            "page (Sources → Telegram) or in config.local.yaml, then run this again."
        )
        ctx.exit(1)
        return

    from nerve import setup_telegram_login

    session_path = str(setup_telegram_login.session_base())
    click.echo(f"Telethon session: {session_path}.session")
    click.echo(f"API ID: {api_id}")
    click.echo("(The setup page's Sources step offers the same sign-in in the browser.)")
    click.echo()

    async def _run():
        from telethon import TelegramClient, functions

        from nerve import setup_state

        client = TelegramClient(session_path, api_id, api_hash)
        await client.start()

        me = await client.get_me()
        click.echo(f"Authenticated as: {me.first_name} (@{me.username}, ID: {me.id})")

        # Show current state for reference
        state = await client(functions.updates.GetStateRequest())
        click.echo(f"Current state: pts={state.pts}, qts={state.qts}, date={state.date}, seq={state.seq}")

        await client.disconnect()
        # Owner-only like every credential file, and noted in the state file
        # so the setup page and `nerve doctor` can say who is signed in.
        setup_telegram_login._make_private(setup_telegram_login.session_file())
        notes = setup_state.load_state()
        setup_state.retire_on_boot(notes)
        notes.telegram_sync_user = {
            "id": int(me.id), "username": me.username or None, "first_name": me.first_name or "",
        }
        setup_state.save_state(notes)
        click.echo()
        click.echo(f"Session saved to {session_path}.session")
        click.echo("Telegram source is now ready to use.")

    asyncio.run(_run())


# `nerve cron next` asks the running daemon what runs next instead of running a
# job of that name.
_CRON_NEXT = "next"


def _print_cron_schedule(report: dict, timezone_name: str) -> None:
    """Print a ``GET /api/cron/schedule`` body: the next tick, then each job's
    next run and what its gates forecast for it."""
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from nerve.cron.service import _SCHEDULE_HORIZON

    zone = ZoneInfo(timezone_name)

    def when(iso: str | None) -> str:
        if not iso:
            return "—"
        return datetime.fromisoformat(iso).astimezone(zone).strftime("%Y-%m-%d %H:%M:%S %Z")

    tick = report.get("next")
    if tick:
        click.echo(f"Next: {when(tick['at'])} — {tick['name']} ({tick['kind']})")
    else:
        click.echo(
            f"Next: nothing scheduled in the next {_SCHEDULE_HORIZON.days} days",
        )
    rows = []
    for job in report.get("jobs") or []:
        forecast = job.get("forecast") or "—"
        if forecast == "skip" and job.get("reason"):
            forecast = f"skip — {job['reason']}"
        rows.append((job["id"], when(job.get("next_run")), forecast))
    if not rows:
        return
    id_width = max(len("JOB"), *(len(row[0]) for row in rows))
    when_width = max(len("NEXT RUN"), *(len(row[1]) for row in rows))
    click.echo(f"  {'JOB':<{id_width}}  {'NEXT RUN':<{when_width}}  FORECAST")
    for job_id, next_run, forecast in rows:
        click.echo(f"  {job_id:<{id_width}}  {next_run:<{when_width}}  {forecast}")


@main.command()
@click.argument("job_id", default="")
@click.pass_context
def cron(ctx: click.Context, job_id: str) -> None:
    """Run a cron job manually, or list the jobs.

    `nerve cron next` shows what the running daemon will run next: the next
    tick, and for each job its next run and whether its gates, judged now,
    let that run happen.
    """
    config = ctx.obj["config"]

    if job_id == _CRON_NEXT:
        report = _gateway_request(ctx, config, "GET", "/api/cron/schedule")
        _print_cron_schedule(report, config.timezone)
        return

    async def _run():
        from nerve.migrate import open_production_db

        db = await open_production_db(config)
        engine = None
        try:
            engine = await _start_one_shot_engine(config, db)

            if job_id:
                from nerve.cron.service import CronService
                cron_svc = CronService(config, engine, db)
                await cron_svc.run_job(job_id)
            else:
                click.echo("Available jobs:")
                from nerve.cron.jobs import is_reserved_job_id, load_jobs

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
                    # A reserved id is never scheduled by the daemon. Show the
                    # job rather than hiding it — the reason it isn't running is
                    # the whole thing the reader is here to find out.
                    if is_reserved_job_id(job.id):
                        status = "RESERVED ID — never scheduled, rename it"
                    else:
                        status = "enabled" if job.enabled else "disabled"
                    click.echo(
                        f"  [{source:6s}] {job.id}: "
                        f"{job.description or job.schedule} ({status})"
                    )
        finally:
            await _stop_one_shot_engine(engine)
            await db.close()

    _run_against_db(ctx, _run())


@main.command()
@click.option("--output", "-o", default=None, help="Target directory (default: backup.target_dir from config)")
@click.option("--state-only", is_flag=True, help="Back up ~/.nerve state only (skip workspace BRAIN)")
@click.option("--no-secrets", is_flag=True, help="Omit secrets (mcp-token, telegram session, certs, config.local.yaml)")
@click.option("--quiet", "-q", is_flag=True, help="Only print errors")
@click.pass_context
def backup(ctx: click.Context, output: str | None, state_only: bool, no_secrets: bool, quiet: bool) -> None:
    """Create a portable backup bundle of Nerve state.

    Snapshots the SQLite DBs (consistent even while Nerve runs), the memU
    sidecars, secrets, cron jobs, and the workspace BRAIN into a single
    ``nerve-backup-<host>-<ts>.tar.zst`` file. Works without a running server.
    """
    from nerve import backup as backup_mod

    config = ctx.obj["config"]
    config_dir = Path(ctx.obj["config_dir"])

    target = output or config.backup.target_dir
    if not target:
        raise click.ClickException(
            "No target directory. Pass --output DIR, or set backup.target_dir "
            "in config.yaml (an external mount or synced dir is recommended)."
        )

    nerve_dir = paths.nerve_home()
    include_workspace = config.backup.include_workspace and not state_only

    if not quiet:
        click.echo(f"Backing up to {target} ...")
    try:
        result = backup_mod.create_backup(
            nerve_dir=nerve_dir,
            workspace=config.workspace,
            output_dir=Path(target).expanduser(),
            config_dir=config_dir,
            include_workspace=include_workspace,
            include_secrets=not no_secrets,
            state_only=state_only,
            workspace_excludes=config.backup.workspace_excludes,
        )
    except backup_mod.BackupError as e:
        raise click.ClickException(str(e))

    if not quiet:
        c = result.counts
        click.echo(f"  Bundle:   {result.path}")
        click.echo(f"  Size:     {result.size / (1024 ** 2):.1f} MB ({result.compression})")
        click.echo(f"  Files:    {result.file_count}")
        click.echo(
            f"  Parity:   sessions={c.get('sessions')}, messages={c.get('messages')}, "
            f"tasks={c.get('tasks')}, memU={c.get('memu_items')}"
        )
        if not result.include_secrets:
            click.echo("  Secrets:  OMITTED (--no-secrets) — re-provision on restore")
        if not result.include_workspace:
            click.echo("  Workspace: skipped (state-only)")

    # Apply retention when writing to the configured target.
    if config.backup.retention_count and target == config.backup.target_dir:
        deleted = backup_mod.prune(Path(target).expanduser(), config.backup.retention_count)
        if deleted and not quiet:
            click.echo(f"  Pruned:   {len(deleted)} old bundle(s) (keep {config.backup.retention_count})")


@main.command()
@click.argument("bundle")
@click.option("--verify-only", is_flag=True, help="Check integrity without installing")
@click.option("--force", is_flag=True, help="Relocate a non-empty ~/.nerve and restore over it")
@click.pass_context
def restore(ctx: click.Context, bundle: str, verify_only: bool, force: bool) -> None:
    """Verify and restore a backup bundle.

    Refuses to run against a live daemon (stop it first) and refuses to
    overwrite a non-empty ~/.nerve without --force (which relocates the old
    dir to ~/.nerve.pre-restore-<ts> rather than deleting it).
    """
    from nerve import backup as backup_mod

    config = ctx.obj["config"]
    bundle_path = Path(bundle).expanduser()
    if not bundle_path.is_file():
        raise click.ClickException(f"Bundle not found: {bundle_path}")

    nerve_dir = paths.nerve_home()

    if verify_only:
        try:
            report = backup_mod.verify_bundle(bundle_path)
        except backup_mod.BackupError as e:
            raise click.ClickException(str(e))
        m = report.manifest
        click.echo(f"Bundle:   {bundle_path.name}")
        click.echo(f"Created:  {m.get('created_at', '?')} on {m.get('host', '?')}")
        click.echo(f"Nerve:    v{m.get('nerve_version', '?')} (schema v{m.get('schema_version', '?')}, git {m.get('git_sha', '')[:8] or '?'})")
        click.echo(f"Parity:   {report.summary()}")
        flags = m.get("flags", {})
        click.echo(f"Flags:    secrets={flags.get('include_secrets')}, workspace={flags.get('include_workspace')}")
        for w in report.warnings:
            click.echo(f"  [WARN] {w}")
        if report.ok:
            click.echo("\n[OK] Bundle verified — safe to restore.")
        else:
            for e in report.errors:
                click.echo(f"  [ERR] {e}")
            click.echo(f"\n{len(report.errors)} error(s) — restore would be refused.")
            ctx.exit(1)
        return

    # Only override the bundle's recorded config path when this box actually
    # has a resolved config (otherwise let the bundle pointer decide — a
    # faithful disaster-recovery restore).
    cfg_dir = None
    if ctx.obj.get("config_source") not in (None, "", "default"):
        cfg_dir = Path(ctx.obj["config_dir"])

    click.echo(f"Restoring {bundle_path.name} → {nerve_dir} ...")
    try:
        report = backup_mod.restore_bundle(
            bundle_path,
            nerve_dir=nerve_dir,
            workspace=config.workspace,
            config_dir=cfg_dir,
            force=force,
        )
    except backup_mod.BackupError as e:
        raise click.ClickException(str(e))

    click.echo(f"  Parity:   {report.summary()}")
    flags = report.manifest.get("flags", {})
    if not flags.get("include_secrets", True):
        click.echo(
            "  [WARN] This bundle had NO secrets — re-provision mcp-token, the "
            "Telegram session, certs, and config.local.yaml before starting."
        )
    click.echo("\nRestore complete. Run 'nerve doctor' to verify, then 'nerve start'.")


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
    conv_dir = paths.nerve_path("memu-conversations")

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


def _fmt_bytes(n: int) -> str:
    """Render a byte count as MB with one decimal."""
    return f"{(n or 0) / (1024 * 1024):.1f} MB"


def _run_against_db(ctx: click.Context, coro):
    """``asyncio.run`` for a command that opens the state database.

    Prints an :class:`InsecureStateStorage` refusal as an error message
    instead of a traceback.
    """
    from nerve.migrate import InsecureStateStorage

    try:
        return asyncio.run(coro)
    except InsecureStateStorage as e:
        click.echo(f"[ERR] {e}")
        ctx.exit(1)


async def _start_one_shot_engine(config, db):
    """The agent engine of one CLI command (``nerve sync``, ``nerve cron``).

    While the gateway runs, the agent records a previous process left live
    are the gateway's own agents, so the engine leaves them alone instead of
    marking them interrupted.
    """
    from nerve.agent.engine import AgentEngine

    engine = AgentEngine(config, db)
    gateway_running, _pid = _get_daemon_status()
    await engine.initialize(reconcile_agents=not gateway_running)
    return engine


async def _stop_one_shot_engine(engine) -> None:
    """Shut a one-shot engine down before its database closes: the command's
    runtimes disconnect, their agents are settled, and the agent records
    still queued for writing reach the database."""
    if engine is None:
        return
    try:
        await engine.shutdown()
    except Exception as e:  # noqa: BLE001 — the database must still close
        logging.getLogger(__name__).warning("engine shutdown failed: %s", e)


@main.group(name="db")
def db_group() -> None:
    """Database maintenance (prune old data, vacuum to reclaim space)."""


@db_group.command("prune")
@click.option("--dry-run", is_flag=True, help="Report what would change without mutating")
@click.pass_context
def db_prune(ctx: click.Context, dry_run: bool) -> None:
    """Compact old memorized messages and prune old telemetry + file snapshots.

    Uses the configured retention windows (retention.retention_full_days for
    message compaction, retention.retention_days for telemetry/snapshots).
    Frees space inside the DB; run ``nerve db vacuum`` afterwards to shrink the
    file on disk.
    """
    from nerve.migrate import open_production_db

    config = ctx.obj["config"]
    db_path = paths.db_path()
    if not db_path.exists():
        click.echo(f"[ERR] Database not found: {db_path}")
        ctx.exit(1)
        return

    full_days = config.retention.retention_full_days
    days = config.retention.retention_days
    click.echo(
        f"{'[dry-run] ' if dry_run else ''}Pruning {db_path} "
        f"(compact messages > {full_days}d, telemetry/snapshots > {days}d)"
    )

    async def _run() -> None:
        database = await open_production_db(config, db_path=db_path)
        try:
            report = await database.run_retention(
                retention_days=days,
                retention_full_days=full_days,
                dry_run=dry_run,
            )
        finally:
            await database.close()

        verb = "Would compact" if dry_run else "Compacted"
        click.echo(
            f"  {verb} {report['messages_compacted']} messages "
            f"(~{_fmt_bytes(report['message_bytes_reclaimed'])} of blocks/thinking)"
        )
        verb = "Would delete" if dry_run else "Deleted"
        click.echo(f"  {verb} {report['telemetry_deleted']} telemetry rows:")
        for table, n in report["by_table"].items():
            click.echo(f"    {table}: {n}")
        click.echo(
            f"  {verb} {report['snapshots_deleted']} file snapshots "
            f"(~{_fmt_bytes(report['snapshot_bytes_reclaimed'])})"
        )
        if not dry_run:
            click.echo("\nFreed space inside the DB. Run `nerve db vacuum` to shrink the file.")

    _run_against_db(ctx, _run())


@db_group.command("vacuum")
@click.pass_context
def db_vacuum(ctx: click.Context) -> None:
    """Rewrite the DB file to reclaim freed pages (shrinks the file on disk).

    VACUUM takes a write lock and cannot run while the daemon holds the DB.
    Stop the daemon first (`nerve stop`) for a clean run.
    """
    from nerve.migrate import open_production_db

    config = ctx.obj["config"]
    db_path = paths.db_path()
    if not db_path.exists():
        click.echo(f"[ERR] Database not found: {db_path}")
        ctx.exit(1)
        return

    running, _pid = _get_daemon_status()
    if running:
        click.echo(
            "[WARN] Nerve daemon is running. VACUUM needs an exclusive lock and "
            "will likely fail with 'database is locked'. Run `nerve stop` first."
        )

    size_before = db_path.stat().st_size
    click.echo(f"Vacuuming {db_path} ({_fmt_bytes(size_before)})...")

    async def _run() -> None:
        database = await open_production_db(config, db_path=db_path)
        try:
            await database.vacuum()
        finally:
            await database.close()

    try:
        asyncio.run(_run())
    except Exception as e:
        click.echo(f"[ERR] VACUUM failed: {e}")
        ctx.exit(1)
        return

    size_after = db_path.stat().st_size
    click.echo(
        f"Done. {_fmt_bytes(size_before)} -> {_fmt_bytes(size_after)} "
        f"(reclaimed {_fmt_bytes(size_before - size_after)})"
    )


def _format_workflow_run(run: dict) -> str:
    """One compact status line: id, status, title, spent vs budget."""
    budget = run.get("budget_usd")
    budget_txt = f"${budget:.2f}" if budget else "none"
    spent = float(run.get("spent_usd") or 0.0)
    line = (
        f"{run['id']} [{run['status']}] {run.get('title') or run['engine']} — "
        f"spent ${spent:.2f} / budget {budget_txt}"
    )
    if run.get("error"):
        line += f" — {run['error']}"
    return line


def _workflow_db_path(ctx: click.Context) -> Path:
    db_path = paths.db_path()
    if not db_path.exists():
        click.echo(f"[ERR] Database not found: {db_path}")
        ctx.exit(1)
    return db_path


def _gateway_request(
    ctx: click.Context, config, method: str, path: str, body: dict | None = None,
) -> dict:
    """Call the local daemon's REST API; prints the failure and exits non-zero.

    Start/kill must execute inside the daemon (runs live in its event
    loop), so these commands go over HTTP instead of touching the DB.
    """
    import httpx

    scheme = "https" if config.gateway.ssl.enabled else "http"
    url = f"{scheme}://127.0.0.1:{config.gateway.port}{path}"
    headers = {}
    secret = _signing_secret(config)
    if secret:
        # The instance acting on its own behalf — see `nerve reload`.
        from nerve.gateway.auth import create_system_token

        headers["Authorization"] = f"Bearer {create_system_token(secret)}"
    try:
        # verify=False: gateway.ssl is normally a local self-signed cert,
        # and this call only ever targets loopback.
        resp = httpx.request(
            method, url, json=body, headers=headers, timeout=30.0, verify=False,
        )
    except httpx.ConnectError:
        click.echo(
            f"[ERR] Cannot connect to the gateway at {url} — "
            "is the nerve daemon running? (nerve start)"
        )
        ctx.exit(1)
    except httpx.HTTPError as e:
        click.echo(f"[ERR] Request to {url} failed: {e}")
        ctx.exit(1)
    if not (200 <= resp.status_code < 300):
        click.echo(f"[ERR] {method} {path} -> HTTP {resp.status_code}: {resp.text}")
        ctx.exit(1)
    return resp.json()


@main.group(name="workflow")
def workflow_group() -> None:
    """Inspect and control budget-capped workflow runs."""


@workflow_group.command("list")
@click.option(
    "--status", default="",
    help="Filter: 'active' (pending+running) or an exact status "
         "(pending/running/done/failed/killed/budget_exhausted).",
)
@click.option(
    "--limit", type=click.IntRange(1, 200), default=20, show_default=True,
    help="Max runs to show (newest first).",
)
@click.pass_context
def workflow_list(ctx: click.Context, status: str, limit: int) -> None:
    """List workflow runs straight from the database (daemon not required)."""
    from nerve.migrate import open_production_db

    config = ctx.obj["config"]
    db_path = _workflow_db_path(ctx)

    async def _run() -> list[dict]:
        # Read-only concurrent access is safe alongside the daemon (WAL).
        database = await open_production_db(config, db_path=db_path)
        try:
            return await database.list_workflow_runs(
                status=status or None, limit=limit,
            )
        finally:
            await database.close()

    runs = _run_against_db(ctx, _run())
    if not runs:
        click.echo("No workflow runs" + (f" with status '{status}'" if status else "") + ".")
        return
    for run in runs:
        click.echo(_format_workflow_run(run))


@workflow_group.command("status")
@click.argument("run_id")
@click.pass_context
def workflow_status(ctx: click.Context, run_id: str) -> None:
    """Show one run's status, spend vs budget, session, and journal path."""
    from nerve.migrate import open_production_db

    config = ctx.obj["config"]
    db_path = _workflow_db_path(ctx)

    async def _run() -> dict | None:
        database = await open_production_db(config, db_path=db_path)
        try:
            return await database.get_workflow_run(run_id)
        finally:
            await database.close()

    run = _run_against_db(ctx, _run())
    if run is None:
        click.echo(f"[ERR] No such workflow run: {run_id}")
        ctx.exit(1)
        return
    click.echo(_format_workflow_run(run))
    click.echo(f"  engine:    {run['engine']}")
    click.echo(f"  created:   {run.get('created_at')} (by {run.get('created_by')})")
    if run.get("started_at"):
        click.echo(f"  started:   {run['started_at']}")
    if run.get("finished_at"):
        click.echo(f"  finished:  {run['finished_at']}")
    if run.get("session_id"):
        click.echo(f"  session:   {run['session_id']}")
    if run.get("journal_dir"):
        click.echo(f"  journal:   {run['journal_dir']}")
    if run.get("result"):
        click.echo(f"  result:    {run['result'][:500]}")


@workflow_group.command("start")
@click.option(
    "--engine", "engine_kind", required=True,
    type=click.Choice(["claude-workflow", "codex-ultracode"]),
    help="Execution engine for the run.",
)
@click.option("--prompt", required=True, help="Task prompt for the orchestrating agent.")
@click.option(
    "--budget", "budget_usd", type=float, default=None,
    help="Hard dollar budget (warned at 80%, stopped at 100%). Required "
         "unless workflows.allow_unbudgeted is enabled.",
)
@click.option("--title", default="", help="Short human-readable label.")
@click.option("--model", default="", help="Model override (backend default otherwise).")
@click.option("--effort", default="", help="Reasoning effort override.")
@click.option("--cwd", default="", help="Working directory for the run's session.")
@click.pass_context
def workflow_start(
    ctx: click.Context,
    engine_kind: str,
    prompt: str,
    budget_usd: float | None,
    title: str,
    model: str,
    effort: str,
    cwd: str,
) -> None:
    """Start a workflow run in the running daemon."""
    config = ctx.obj["config"]
    run = _gateway_request(ctx, config, "POST", "/api/workflow-runs", {
        "engine": engine_kind,
        "prompt": prompt,
        "budget_usd": budget_usd,
        "title": title,
        "model": model,
        "effort": effort,
        "cwd": cwd,
    })
    budget = run.get("budget_usd")
    budget_txt = f"${budget:.2f}" if budget else "UNBUDGETED"
    click.echo(f"Workflow run {run['id']} created ({run['status']}), budget {budget_txt}.")
    if run.get("journal_dir"):
        click.echo(f"Journal: {run['journal_dir']}")
    click.echo(f"Track it with: nerve workflow status {run['id']}")


@workflow_group.command("kill")
@click.argument("run_id")
@click.option("--reason", default="", help="Why the run is being killed (recorded).")
@click.pass_context
def workflow_kill(ctx: click.Context, run_id: str, reason: str) -> None:
    """Kill a workflow run via the running daemon (scoped to that run)."""
    config = ctx.obj["config"]
    run = _gateway_request(
        ctx, config, "POST", f"/api/workflow-runs/{run_id}/kill", {"reason": reason},
    )
    click.echo(f"Workflow run {run['id']} is now '{run['status']}'.")
    click.echo(_format_workflow_run(run))


if __name__ == "__main__":
    main()
