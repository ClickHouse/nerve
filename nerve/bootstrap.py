"""First-run bootstrap wizard — interactive CLI that guides through initial setup.

Two interactive shapes live here. :class:`SlimSetup` is the default
``nerve init``: it decides only what a browser cannot — how Nerve runs on this
machine, where its files live, how the gateway listens — starts the daemon and
hands over to the setup page. :class:`SetupWizard` is the full terminal wizard
(``nerve init --terminal``) that asks everything; ``run_non_interactive`` is
the unattended path.

What the wizards *ask* lives here. What setup *writes* lives in
:mod:`nerve.setup_writer`, because the web setup wizard has to produce the
same configuration this does, and two implementations of setup drift.
"""

from __future__ import annotations

import errno
import json
import os
import socket
import stat
import secrets
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click
import yaml

from nerve import paths
from nerve.config import DEFAULT_MODEL, workspace_settings_file
from nerve.setup_writer import (
    CONFIG_YAML_HEADER,
    CORE_CRONS,
    DEFAULT_CRONS,
    PRODUCTIVITY_CRONS,
    SetupChoices,
    SlimOutcome,
    WRITER_OWNED_TEMPLATES,
    _DOCKER_NERVE_HOME,
    _DOCKER_TOOLS_SECTION,
    _DOCKER_WORKSPACE,
    _PERSONAL_MEMORY_CATEGORIES,
    _WORKER_MEMORY_CATEGORIES,
    apply_profile_templates,
    apply_task_md,
    bedrock_geo_prefix,
    build_config_layers,
    existing_memory_backend,
    # Re-exported under its old private name: this module has been its import
    # site since the wizard was written, and `nerve init` is not the only
    # caller that expands a workspace path the way the loader does.
    expand_workspace as _expand_workspace,
    leaf_paths,
    merge_external_agents,
    write_config_local_yaml,
    write_config_yaml,
    write_cron_jobs,
    write_slim_install,
    workspace_dir,
    write_workspace_settings,
)
from nerve.workspace import (
    initialize_workspace,
    install_bundled_skills,
    install_config_scaffold,
)

# The wizard's answers, the layering rules and the writers live in
# nerve.setup_writer, so the web setup wizard writes configuration through
# exactly the code `nerve init` does rather than through a second
# implementation that drifts. Re-exported here because this module has been
# their import site since the wizard was written.
__all__ = [
    "CORE_CRONS",
    "DEFAULT_CRONS",
    "PRODUCTIVITY_CRONS",
    "PASSWORDLESS_WARNING",
    "SetupChoices",
    "SetupWizard",
    "SlimSetup",
    "bedrock_geo_prefix",
    "is_fresh_install",
    "run_non_interactive",
]

PASSWORDLESS_WARNING = (
    "Without a password, anyone who can reach the gateway port is the owner: "
    "they can use the agent, its tools and its credentials. Keep the gateway "
    "on localhost or behind a protected network."
)


# --- Credential resolution (priority waterfall) ---


def _resolve_claude_credential() -> tuple[str, str, list[str]]:
    """Resolve Claude credential from host. First match wins.

    Waterfall (priority cascade), non-prompting sources first — the Keychain
    can raise an "allow access" dialog on the desktop, so it is asked last:
      1.  CLAUDE_CODE_OAUTH_TOKEN env var
      2.  ANTHROPIC_API_KEY env var
      3.  ~/.claude/.credentials.json file (claudeAiOauth.accessToken)
      4a. macOS Keychain "Claude Code-credentials" (OAuth JSON)
      4b. macOS Keychain "Claude Code" (raw API key)

    Returns (token_value, source_label, debug_log).
    debug_log contains details about each step tried, useful when nothing is found.
    """
    debug: list[str] = []

    # 1. CLAUDE_CODE_OAUTH_TOKEN env var
    token = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", "")
    if token:
        debug.append("CLAUDE_CODE_OAUTH_TOKEN env var: found")
        return (token, "CLAUDE_CODE_OAUTH_TOKEN env var", debug)
    debug.append("CLAUDE_CODE_OAUTH_TOKEN env var: not set")

    # 2. ANTHROPIC_API_KEY env var
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if key:
        debug.append("ANTHROPIC_API_KEY env var: found")
        return (key, "ANTHROPIC_API_KEY env var", debug)
    debug.append("ANTHROPIC_API_KEY env var: not set")

    # 3. ~/.claude/.credentials.json file (Linux stores creds here; on a Mac
    #    where Claude Code is signed in it may hold only mcpOAuth)
    creds_file = Path("~/.claude/.credentials.json").expanduser()
    if creds_file.exists():
        try:
            data = json.loads(creds_file.read_text(encoding="utf-8"))
            token = data.get("claudeAiOauth", {}).get("accessToken", "")
            if token:
                debug.append("~/.claude/.credentials.json: found OAuth token")
                return (token, "~/.claude/.credentials.json", debug)
            debug.append(
                f"~/.claude/.credentials.json: exists but no claudeAiOauth.accessToken "
                f"(keys: {list(data.keys())})"
            )
        except (json.JSONDecodeError, OSError) as exc:
            debug.append(f"~/.claude/.credentials.json: parse error — {exc}")
    else:
        debug.append(f"~/.claude/.credentials.json: file not found ({creds_file})")

    # 4a. macOS Keychain — OAuth entry ("Claude Code-credentials")
    #     This is where `claude login` stores OAuth tokens on macOS.
    #     The value is a JSON blob; we extract claudeAiOauth.accessToken.
    if sys.platform == "darwin" and shutil.which("security"):
        try:
            result = subprocess.run(
                ["security", "find-generic-password", "-s", "Claude Code-credentials", "-w"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                raw = result.stdout.strip()
                try:
                    data = json.loads(raw)
                    token = data.get("claudeAiOauth", {}).get("accessToken", "")
                    if token:
                        debug.append('keychain "Claude Code-credentials": found OAuth token')
                        return (token, "macOS Keychain (OAuth)", debug)
                    debug.append(
                        'keychain "Claude Code-credentials": JSON parsed but no '
                        f"claudeAiOauth.accessToken (keys: {list(data.keys())})"
                    )
                except json.JSONDecodeError:
                    # Not JSON — use raw value as-is (unlikely but handle gracefully)
                    debug.append(
                        'keychain "Claude Code-credentials": not JSON, using raw value'
                    )
                    return (raw, "macOS Keychain (credentials)", debug)
            else:
                debug.append(
                    f'keychain "Claude Code-credentials": not found (rc={result.returncode})'
                )
        except (subprocess.TimeoutExpired, OSError) as exc:
            debug.append(f'keychain "Claude Code-credentials": error — {exc}')

        # 4b. macOS Keychain — API key entry ("Claude Code")
        #     Plain string, not JSON. Used when an API key is stored directly.
        try:
            result = subprocess.run(
                ["security", "find-generic-password", "-s", "Claude Code", "-w"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                raw = result.stdout.strip()
                debug.append(f'keychain "Claude Code": found ({len(raw)} chars)')
                return (raw, "macOS Keychain (API key)", debug)
            else:
                debug.append(
                    f'keychain "Claude Code": not found (rc={result.returncode})'
                )
        except (subprocess.TimeoutExpired, OSError) as exc:
            debug.append(f'keychain "Claude Code": error — {exc}')
    else:
        debug.append(
            f"keychain: skipped (platform={sys.platform}, "
            f"security={'found' if shutil.which('security') else 'missing'})"
        )

    return ("", "none", debug)


def _resolve_gh_token() -> tuple[str, str]:
    """Resolve GitHub token from host. First match wins.

    Waterfall:
      1. gh auth token (CLI)
      2. GH_TOKEN env var

    Returns (token_value, source_label). Empty string if nothing found.
    """
    # 1. gh CLI
    if shutil.which("gh"):
        try:
            result = subprocess.run(
                ["gh", "auth", "token"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                return (result.stdout.strip(), "gh CLI")
        except (subprocess.TimeoutExpired, OSError):
            pass

    # 2. GH_TOKEN env var
    token = os.environ.get("GH_TOKEN", "")
    if token:
        return (token, "GH_TOKEN env var")

    return ("", "none")


# --- Credential validators (used by the wizard and preflight) ---


def _telegram_get_me(token: str, timeout: float = 7.0) -> tuple[bool, str]:
    """Validate a Telegram bot token via getMe.

    Returns (True, bot_username) or (False, error_description).
    """
    import httpx

    try:
        resp = httpx.get(
            f"https://api.telegram.org/bot{token}/getMe", timeout=timeout,
        )
        data = resp.json()
        if resp.status_code == 200 and data.get("ok"):
            return (True, data.get("result", {}).get("username", "") or "bot")
        return (False, data.get("description", f"HTTP {resp.status_code}"))
    except Exception as e:
        return (False, str(e))


def _check_openai_key(key: str, timeout: float = 7.0) -> tuple[bool, str]:
    """Validate an OpenAI API key with a models list call."""
    import httpx

    try:
        resp = httpx.get(
            "https://api.openai.com/v1/models",
            headers={"Authorization": f"Bearer {key}"},
            timeout=timeout,
        )
        if resp.status_code == 200:
            return (True, "key valid")
        detail = f"HTTP {resp.status_code}"
        try:
            detail = resp.json().get("error", {}).get("message", detail)
        except Exception:
            pass
        return (False, detail)
    except Exception as e:
        return (False, str(e))


# --- Wizard progress persistence ---
#
# The wizard collects ~10 steps of answers and used to be all-or-nothing:
# a Ctrl+C (or a killed installer) threw everything away and the user had
# to start over. Answers are now checkpointed after every step so an
# interrupted setup resumes where it left off.

def _init_state_file() -> Path:
    """Wizard checkpoint file under the machine-local state dir."""
    return paths.nerve_path("init-state.json")


def _private_fd(fd: int) -> bool:
    """True when the open file carries no group/world permission bits."""
    try:
        return (stat.S_IMODE(os.fstat(fd).st_mode) & 0o077) == 0
    except OSError:
        return False


def _save_init_state(choices: SetupChoices, completed: set[str]) -> bool:
    """Checkpoint wizard progress. Never breaks the wizard.

    The file holds API keys, so it is created ``0600`` with ``O_EXCL`` and its
    mode is checked before anything is written. Returns True only when the
    checkpoint is on disk and owner-only; on any failure no file is left.
    """
    import dataclasses
    from datetime import datetime

    path = _init_state_file()
    tmp = path.with_name(path.name + ".tmp")
    try:
        data = dataclasses.asdict(choices)
        data["workspace_path"] = str(choices.workspace_path)
        state = {
            "choices": data,
            "completed": sorted(completed),
            "saved_at": datetime.now().isoformat(timespec="seconds"),
        }
        # Database.connect refuses a state directory that others can write to.
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        tmp.unlink(missing_ok=True)
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        if not _private_fd(fd):
            os.close(fd)
            tmp.unlink(missing_ok=True)
            return False
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(json.dumps(state))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        if stat.S_IMODE(os.stat(path).st_mode) & 0o077:
            path.unlink(missing_ok=True)
            return False
        return True
    except OSError:
        for p in (tmp, path):
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass
        return False


def _load_init_state() -> dict | None:
    try:
        return json.loads(
            _init_state_file().read_text(encoding="utf-8")
        )
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def _clear_init_state() -> None:
    _init_state_file().unlink(missing_ok=True)


def _choices_from_dict(data: dict) -> SetupChoices:
    """Rebuild SetupChoices from a saved state dict (ignores unknown keys)."""
    import dataclasses

    valid = {f.name for f in dataclasses.fields(SetupChoices)}
    kwargs = {k: v for k, v in dict(data).items() if k in valid}
    if "workspace_path" in kwargs:
        kwargs["workspace_path"] = Path(kwargs["workspace_path"])
    return SetupChoices(**kwargs)


def _ensure_docker_files(
    config_dir: Path,
    workspace: str,
    *,
    port: int = 8900,
    bind: str = "lan",
    extra_mounts: list[str] | None = None,
) -> list[str]:
    """Generate the Docker files that are missing; never overwrite one.

    Returns the names of the files created. When an existing compose file
    publishes a different port than the one chosen now, the two lines to edit
    are printed rather than the file being rewritten under the operator.
    """
    files = {
        "Dockerfile": _build_dockerfile(port),
        "docker-compose.yml": _build_docker_compose(
            workspace_path=workspace, extra_mounts=extra_mounts, port=port, bind=bind,
        ),
        "docker-entrypoint.sh": _DOCKER_ENTRYPOINT_TEMPLATE,
        ".dockerignore": _DOCKERIGNORE_TEMPLATE,
    }
    created: list[str] = []
    wanted = f'"{"127.0.0.1:" if bind == "localhost" else ""}{int(port)}:{int(port)}"'
    for filename, content in files.items():
        filepath = Path(config_dir) / filename
        if filepath.exists():
            click.echo(f"  {filename} already exists — skipping")
            if filename == "docker-compose.yml":
                try:
                    existing = filepath.read_text(encoding="utf-8")
                except OSError:
                    existing = ""
                if wanted not in existing:
                    click.secho(
                        f"    It does not publish port {int(port)}. To use it, edit "
                        f"{filename} (ports: - {wanted}) and Dockerfile "
                        f"(EXPOSE {int(port)} and the HEALTHCHECK URL).",
                        fg="yellow",
                    )
            continue
        filepath.write_text(content.lstrip("\n"), encoding="utf-8")
        if filename == "docker-entrypoint.sh":
            try:
                os.chmod(filepath, 0o755)
            except OSError:
                pass
        click.echo(f"  Created {filename}")
        created.append(filename)
    return created


class SetupWizard:
    """Interactive first-run setup wizard."""

    def __init__(
        self, config_dir: Path, inside_docker: bool = False, memory_backend: str | None = None,
    ):
        self.config_dir = config_dir
        # ``nerve init --memory-backend``: the backend the workspace is worded
        # for; None reads it from an existing configuration.
        self._memory_backend = memory_backend
        self.choices = SetupChoices()
        self._inside_docker = inside_docker
        self._step_counter = 0
        self._completed_steps: set[str] = set()
        if inside_docker:
            self.choices.deployment = "docker"

    def _planned_total(self) -> int | None:
        """Total number of steps, known once the mode has been chosen."""
        if "mode" not in self._completed_steps:
            return None
        total = 0 if self._inside_docker else 1  # deployment
        total += 4  # mode, api keys, workspace, password
        total += 5 if self.choices.mode == "personal" else 1
        return total

    def _next_step(self, label: str) -> str:
        """Return a formatted step header with auto-incrementing number."""
        self._step_counter += 1
        total = self._planned_total()
        if total:
            return f"Step {self._step_counter}/{total}: {label}"
        return f"Step {self._step_counter}: {label}"

    def _do(self, name: str, step_fn) -> None:
        """Run a wizard step with checkpointing.

        Skips steps already answered in a resumed session (keeping the
        step counter consistent) and saves progress after each step so an
        interrupted wizard can resume instead of starting over.
        """
        if name in self._completed_steps:
            self._step_counter += 1
            return
        step_fn()
        self._completed_steps.add(name)
        _save_init_state(self.choices, self._completed_steps)

    def checkpoint(self) -> bool:
        """Save the answers so a re-run resumes rather than starting over.

        The installer calls this when owner-account creation fails after the
        wizard has cleared its checkpoint. The owner's name is stored nowhere
        else. Returns whether the checkpoint was written.
        """
        return _save_init_state(self.choices, self._completed_steps)

    def _maybe_resume(self) -> bool:
        """Offer to resume an interrupted setup. Returns True if resumed."""
        state = _load_init_state()
        if not state or not state.get("completed"):
            return False
        completed = list(state.get("completed", []))
        saved_at = state.get("saved_at", "")

        click.clear()
        click.secho("Interrupted setup found", fg="cyan", bold=True)
        click.echo()
        when = f" (saved {saved_at})" if saved_at else ""
        click.secho(
            f"A previous 'nerve init' didn't finish{when}.\n"
            f"Answered steps: {', '.join(completed)}",
            dim=True,
        )
        click.echo()
        if click.confirm("Resume where you left off?", default=True):
            try:
                self.choices = _choices_from_dict(state.get("choices", {}))
            except (TypeError, ValueError):
                click.secho("  Saved answers unreadable — starting fresh.", fg="yellow")
                _clear_init_state()
                return False
            if self._inside_docker:
                self.choices.deployment = "docker"
            self._completed_steps = set(completed)
            click.echo()
            return True
        _clear_init_state()
        return False

    def run(self) -> SetupChoices:
        """Run the full interactive wizard.

        Nothing is applied until the review step; answers are checkpointed
        along the way so Ctrl+C never loses progress.
        """
        resumed = self._maybe_resume()
        try:
            if not resumed:
                self._welcome()
            if not self._inside_docker:
                self._do("deployment", self._step_deployment)
                if self.choices.deployment == "docker":
                    self._do("docker_credentials", self._step_docker_credentials)
                    self._launch_docker()
                    return self.choices  # Never reached — execvp replaces process
            self._do("mode", self._step_mode)
            self._do("api_keys", self._step_api_keys)
            self._do("workspace", self._step_workspace)
            self._do("password", self._step_password)
            if self.choices.mode == "personal":
                self._do("identity", self._step_identity)
                self._do("channels", self._step_channels)
                self._do("sources", self._step_sources)
                self._do("crons", self._step_crons)
                self._do("external_agents", self._step_external_agents)
            else:
                self._do("worker_setup", self._step_worker_setup)
            self._step_review()
            self._apply()
            _clear_init_state()
            self._preflight()
            self._done()
        except (KeyboardInterrupt, click.Abort):
            click.echo()
            click.secho("  Setup interrupted — your answers are saved.", fg="yellow")
            click.secho("  Resume anytime with: nerve init", dim=True)
            raise SystemExit(130)
        return self.choices

    # --- Welcome ---

    def _welcome(self) -> None:
        click.clear()
        click.secho("=" * 56, fg="cyan")
        click.secho("  _   _                                ", fg="cyan")
        click.secho(" | \\ | | ___  _ __ __   __ ___       ", fg="cyan")
        click.secho(" |  \\| |/ _ \\| '__|\\ \\ / // _ \\  ", fg="cyan")
        click.secho(" | |\\  |  __/| |    \\ V /|  __/      ", fg="cyan")
        click.secho(" |_| \\_|\\___||_|     \\_/  \\___|    ", fg="cyan")
        click.secho("=" * 56, fg="cyan")
        click.echo()
        click.secho(
            "Nerve is a personal AI agent that lives on your server.\n"
            "It has memory, runs background jobs, connects to your\n"
            "services, and gets better over time.",
            dim=True,
        )
        click.echo()
        click.secho(
            "This wizard will walk you through the initial setup.\n"
            "Nothing is written until the final step — you can\n"
            "Ctrl+C at any point to abort.",
            dim=True,
        )
        click.echo()
        click.pause("Press Enter to begin...")

    # --- Step: Deployment ---

    def _step_deployment(self) -> None:
        click.clear()
        click.secho(self._next_step("Deployment"), fg="cyan", bold=True)
        click.echo()
        click.secho(
            "How do you want to run Nerve?\n",
            dim=True,
        )
        click.secho("  server", fg="green", bold=True, nl=False)
        click.secho(
            " — Run directly on this machine. You manage\n"
            "            Python, dependencies, and process lifecycle.",
            dim=True,
        )
        click.echo()
        click.secho("  docker", fg="green", bold=True, nl=False)
        click.secho(
            " — Run in a Docker container. Isolated environment,\n"
            "            easy cleanup, recommended for local use.",
            dim=True,
        )
        click.echo()
        self.choices.deployment = click.prompt(
            "Choose deployment",
            type=click.Choice(["server", "docker"], case_sensitive=False),
            default="server",
        )
        click.echo()
        click.secho(f"  → {self.choices.deployment} deployment selected.", fg="green")
        click.echo()

    # --- Step: Docker Credentials (host-side only) ---

    def _step_docker_credentials(self) -> None:
        """Extract Claude and GitHub credentials from host for Docker.

        Runs on the host before launching Docker. Tokens are passed via
        env vars to `docker compose run` and stored in config.local.yaml
        by the wizard inside the container.

        Credential resolution follows the priority waterfall pattern:
        first match wins, each source tried in priority order.
        """
        click.clear()
        click.secho(self._next_step("Docker Credentials"), fg="cyan", bold=True)
        click.echo()
        click.secho(
            "Docker containers can't access your macOS Keychain, so\n"
            "Nerve extracts credentials from your current logins.",
            dim=True,
        )
        click.echo()

        # --- Claude credential ---
        click.secho("  Claude:", bold=True)
        claude_token, claude_source, claude_debug = _resolve_claude_credential()

        if claude_token:
            # Distinguish OAuth tokens from API keys for storage
            is_api_key = claude_source in ("ANTHROPIC_API_KEY env var", "macOS Keychain (API key)")
            if is_api_key:
                click.secho(f"    ✓ Found API key (source: {claude_source})", fg="green")
                self.choices.anthropic_api_key = claude_token
            else:
                click.secho(f"    ✓ Found OAuth token (source: {claude_source})", fg="green")
                self.choices.claude_oauth_token = claude_token
        else:
            click.secho("    ✗ No credentials found automatically.", fg="yellow")
            click.echo()
            click.secho("    Debug — tried each source in order:", dim=True)
            for line in claude_debug:
                click.secho(f"      · {line}", dim=True)
            click.echo()
            click.secho("    1) Anthropic API key (sk-ant-...)", dim=True)
            click.secho("    2) Paste OAuth token (run `claude setup-token`)", dim=True)
            click.secho("    3) Skip — configure later in config.local.yaml", dim=True)
            click.echo()
            choice = click.prompt(
                "    Choose",
                type=click.Choice(["1", "2", "3"]),
                default="3",
            )
            if choice == "1":
                while True:
                    key = click.prompt("    Anthropic API key", hide_input=True)
                    if key.startswith("sk-ant-"):
                        self.choices.anthropic_api_key = key
                        click.secho("    ✓ API key saved", fg="green")
                        break
                    click.secho("    Invalid key — should start with 'sk-ant-'.", fg="yellow")
            elif choice == "2":
                click.echo()
                click.secho(
                    "    Run this in another terminal:\n"
                    "      claude setup-token\n\n"
                    "    Then paste the token below.",
                    dim=True,
                )
                click.echo()
                token = click.prompt("    OAuth token", hide_input=True)
                if token.strip():
                    self.choices.claude_oauth_token = token.strip()
                    click.secho("    ✓ OAuth token saved", fg="green")
            else:
                click.secho("    → Skipping — set anthropic_api_key in config.local.yaml later.", dim=True)

        click.echo()

        # --- GitHub credential ---
        click.secho("  GitHub:", bold=True)
        gh_token, gh_source = _resolve_gh_token()

        if gh_token:
            click.secho(f"    ✓ Found token (source: {gh_source})", fg="green")
            self.choices.github_token = gh_token
        else:
            click.secho("    — Not found (gh CLI not installed or not authenticated)", dim=True)
            click.secho("    → GitHub sync will be configured inside Docker.", dim=True)

        click.echo()

    # --- Docker orchestration ---

    def _launch_docker(self) -> None:
        """Build Docker image, start container, continue wizard inside it."""
        import subprocess

        # Check Docker is available
        if not shutil.which("docker"):
            click.secho(
                "\n  Docker not found. Install Docker first:\n"
                "  https://docs.docker.com/get-docker/",
                fg="red",
            )
            raise SystemExit(1)

        click.echo()
        click.echo("  Checking Docker...", nl=False)
        # Verify Docker daemon is running
        result = subprocess.run(["docker", "info"], capture_output=True)
        if result.returncode != 0:
            click.secho(" ✗", fg="red")
            click.secho("  Docker daemon is not running. Start Docker and try again.", fg="red")
            raise SystemExit(1)
        click.secho(" ✓", fg="green")

        # Check Docker Compose V2
        result = subprocess.run(["docker", "compose", "version"], capture_output=True)
        if result.returncode != 0:
            click.secho("  Docker Compose V2 not found.", fg="red")
            click.secho(
                "  Nerve requires 'docker compose' (V2, built into Docker Desktop).\n"
                "  Update Docker or install the compose plugin.",
                fg="red",
            )
            raise SystemExit(1)

        # Generate Docker files if they don't exist
        self._ensure_docker_files()

        # Build image
        click.echo("  Building image — this may take a few minutes on first run...", nl=False)
        result = subprocess.run(
            ["docker", "compose", "build"],
            capture_output=True,
            cwd=str(self.config_dir),
        )
        if result.returncode != 0:
            click.secho(" ✗", fg="red")
            click.echo(result.stderr.decode())
            raise SystemExit(1)
        click.secho(" ✓", fg="green")

        # Run the wizard inside the container (interactive).
        # Pass extracted credentials via env vars so the wizard inside
        # Docker can detect and store them without re-prompting.
        click.echo("  Starting container...\n")
        cmd = [
            "docker", "compose",
            "-f", str(self.config_dir / "docker-compose.yml"),
            "run", "--rm",
            "--service-ports",
        ]
        if self.choices.claude_oauth_token:
            cmd.extend(["-e", f"CLAUDE_CODE_OAUTH_TOKEN={self.choices.claude_oauth_token}"])
        elif self.choices.anthropic_api_key:
            cmd.extend(["-e", f"ANTHROPIC_API_KEY={self.choices.anthropic_api_key}"])
        if self.choices.github_token:
            cmd.extend(["-e", f"GH_TOKEN={self.choices.github_token}"])
        cmd.extend(["nerve", "nerve", "init", "--inside-docker"])
        os.execvp("docker", cmd)
        # execvp replaces this process — we never return here

    def _ensure_docker_files(self) -> None:
        """Generate Dockerfile, docker-compose.yml, entrypoint, and .dockerignore."""
        _ensure_docker_files(self.config_dir, str(self.choices.workspace_path))

    # --- Step: Mode ---

    def _step_mode(self) -> None:
        click.clear()
        click.secho(self._next_step("Mode"), fg="cyan", bold=True)
        click.echo()
        click.secho(
            "Nerve has two modes:\n",
            dim=True,
        )
        click.secho("  personal", fg="green", bold=True, nl=False)
        click.secho(
            " — Full-featured assistant for one person. Syncs your\n"
            "              email, remembers preferences, develops personality.\n"
            "              Has memory, cron jobs, notifications, and a web UI.",
            dim=True,
        )
        click.echo()
        click.secho("  worker", fg="green", bold=True, nl=False)
        click.secho(
            "   — Task-focused agent for teams. Monitors something,\n"
            "              proposes fixes, implements after approval. Plan-driven\n"
            "              with audit trail.",
            dim=True,
        )
        click.echo()
        self.choices.mode = click.prompt(
            "Choose mode",
            type=click.Choice(["personal", "worker"], case_sensitive=False),
            default="personal",
        )
        click.echo()
        click.secho(f"  → Setting up in {self.choices.mode} mode.", fg="green")
        click.echo()

    # --- Step: API Keys ---

    def _prompt_openai_key(self) -> None:
        """Prompt for optional OpenAI API key (used by both auth paths)."""
        click.echo()
        click.secho(
            "Optionally, an OpenAI key enables vector-based memory search\n"
            "(text-embedding-3-small for semantic embeddings). Nerve works\n"
            "without it using LLM-based recall, which uses more API tokens\n"
            "per query but requires no additional API key.",
            dim=True,
        )
        click.echo()
        openai_key = click.prompt("OpenAI API key (Enter to skip)", default="", hide_input=True)
        if openai_key:
            self.choices.openai_api_key = openai_key

        click.echo()
        click.secho("  ✓ API configuration complete", fg="green")
        click.echo()

    def _step_api_keys(self) -> None:
        # Check for tokens pre-extracted on the host (Docker credential flow).
        # When `_step_docker_credentials()` runs on the host and passes tokens
        # via env vars to `docker compose run`, the wizard inside Docker picks
        # them up here and skips the manual prompt.
        claude_token = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", "")
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        gh_token = os.environ.get("GH_TOKEN", "")

        if claude_token or api_key:
            click.clear()
            click.secho(self._next_step("API Configuration"), fg="cyan", bold=True)
            click.echo()
            if claude_token:
                self.choices.claude_oauth_token = claude_token
                click.secho("  ✓ Using Claude OAuth token from host", fg="green")
            elif api_key:
                self.choices.anthropic_api_key = api_key
                click.secho("  ✓ Using Anthropic API key from host", fg="green")
            if gh_token:
                self.choices.github_token = gh_token
                click.secho("  ✓ Using GitHub token from host", fg="green")
            self._prompt_openai_key()
            return

        # Normal flow — no pre-extracted tokens
        click.clear()
        click.secho(self._next_step("API Configuration"), fg="cyan", bold=True)
        click.echo()
        click.secho(
            "Nerve needs access to Claude's API for memory, titles, and\n"
            "other background tasks. Choose how to authenticate:",
            dim=True,
        )
        click.echo()
        click.secho("  1) Anthropic API key  — direct access (requires sk-ant-... key)", dim=True)
        click.secho("  2) Claude Code proxy  — routes through your Claude subscription", dim=True)
        click.secho("  3) AWS Bedrock        — uses AWS IAM credentials", dim=True)
        click.echo()

        auth_mode = click.prompt("Choose", type=click.Choice(["1", "2", "3"]), default="1")

        if auth_mode == "3":
            self._step_bedrock_setup()
        elif auth_mode == "2":
            self._step_proxy_setup()
        else:
            self._step_api_key_direct()

        self._prompt_openai_key()

    def _step_api_key_direct(self) -> None:
        """Prompt for a direct Anthropic API key."""
        click.echo()
        click.secho(
            "Get an API key at: https://console.anthropic.com",
            dim=True,
        )
        click.echo()

        while True:
            key = click.prompt("Anthropic API key", hide_input=True)
            if key.startswith("sk-ant-"):
                self.choices.anthropic_api_key = key
                break
            click.secho("  Invalid key — should start with 'sk-ant-'. Try again.", fg="yellow")

    def _step_bedrock_setup(self) -> None:
        """Configure AWS Bedrock as the provider."""
        click.echo()
        click.secho(
            "AWS Bedrock routes API calls through your AWS account.\n"
            "On EC2/ECS/EKS, IAM roles provide credentials automatically.\n"
            "Outside AWS, configure credentials via AWS CLI or environment variables.",
            dim=True,
        )
        click.echo()

        self.choices.provider_type = "bedrock"

        region = click.prompt("AWS Region", default="us-east-1")
        self.choices.aws_region = region

        profile = click.prompt(
            "AWS Profile (Enter to skip — use IAM role or env vars)", default="",
        )
        if profile:
            self.choices.aws_profile = profile

        # Bedrock model IDs are geography-scoped (us./eu./apac. inference
        # profiles) — derive the right prefix from the region instead of
        # assuming us. and failing with a 400 on the first message.
        prefix = bedrock_geo_prefix(region)
        click.echo()
        click.secho(
            f"  → Model IDs will use the '{prefix}.' inference-profile prefix\n"
            f"    (e.g. {prefix}.anthropic.{DEFAULT_MODEL}) based on region {region}.",
            fg="green",
        )
        click.echo()
        click.secho(
            "  Make sure you have enabled Claude model access in the\n"
            "  AWS Bedrock console for your region. Not every model is\n"
            "  offered in every geography — the preflight check at the end\n"
            "  of setup will tell you if this one isn't.",
            fg="yellow",
        )
        click.echo()
        click.secho("  → Bedrock provider configured.", fg="green")

    def _step_proxy_setup(self) -> None:
        """Set up CLIProxyAPI for Claude Code OAuth proxy."""
        import asyncio

        click.echo()
        click.secho(
            "Setting up CLIProxyAPI — this routes API calls through your\n"
            "Claude Max/Pro subscription using OAuth.\n\n"
            "Requires an active Claude subscription at claude.ai.",
            dim=True,
        )
        click.echo()

        self.choices.use_proxy = True

        # Download the binary.
        click.echo("  Downloading CLIProxyAPI...", nl=False)
        try:
            from nerve.config import ProxyConfig
            from nerve.proxy.service import ProxyService

            # Build a minimal config just for the proxy service to use.
            from nerve.config import NerveConfig
            tmp_config = NerveConfig(proxy=ProxyConfig(enabled=True))
            proxy = ProxyService(tmp_config)
            asyncio.get_event_loop().run_until_complete(proxy.ensure_binary())
            click.secho(" ✓", fg="green")
        except Exception as e:
            click.secho(f" ✗", fg="red")
            click.secho(f"  Failed to download: {e}", fg="red")
            click.secho("  You can install manually later. See:", dim=True)
            click.secho("  https://github.com/router-for-me/CLIProxyAPI", dim=True)
            click.echo()
            # Fall back to API key.
            if click.confirm("  Fall back to API key?", default=True):
                self.choices.use_proxy = False
                self._step_api_key_direct()
                return
            return

        # Run OAuth login.
        click.echo()
        click.secho(
            "  Now you need to authenticate with Claude. A URL will be\n"
            "  printed — open it in your browser and authorize access.",
            dim=True,
        )
        click.echo()

        if click.confirm("  Ready to authenticate?", default=True):
            click.echo()
            success = asyncio.get_event_loop().run_until_complete(
                proxy.login(no_browser=True),
            )
            click.echo()
            if success:
                click.secho("  ✓ Claude OAuth configured", fg="green")
            else:
                click.secho("  ✗ OAuth login failed", fg="red")
                if click.confirm("  Fall back to API key?", default=True):
                    self.choices.use_proxy = False
                    self._step_api_key_direct()
        else:
            # The operator is meant to paste this, so the paths have to be the
            # ones the proxy service actually uses (nerve/proxy/service.py).
            click.secho(
                "\n  You can authenticate later by running:\n"
                f"    {paths.path_label('bin', 'cli-proxy-api')}"
                " --claude-login --no-browser \\\n"
                f"      --config {paths.path_label('cli-proxy-config.yaml')}",
                dim=True,
            )

    # --- Step: Workspace ---

    def _step_workspace(self) -> None:
        click.clear()
        click.secho(self._next_step("Workspace"), fg="cyan", bold=True)
        click.echo()
        click.secho(
            "Your workspace is where Nerve keeps its identity files, tasks,\n"
            "skills, and memory. Think of it as Nerve's home directory.\n\n"
            "It contains markdown files that define who Nerve is and how it\n"
            "behaves — you can edit them anytime.",
            dim=True,
        )
        click.echo()

        if self.choices.mode == "personal":
            click.secho("  Files created:", dim=True)
            click.secho("    SOUL.md       — Personality, values, identity", dim=True)
            click.secho("    IDENTITY.md   — Name, vibe, communication style", dim=True)
            click.secho("    USER.md       — About you (the human)", dim=True)
            click.secho("    AGENTS.md     — Operational guidelines", dim=True)
            click.secho("    TOOLS.md      — Environment-specific notes", dim=True)
            click.secho("    MEMORY.md     — Working memory (L1 cache)", dim=True)
        else:
            click.secho("  Files created:", dim=True)
            click.secho("    SOUL.md       — Worker identity and principles", dim=True)
            click.secho("    AGENTS.md     — Plan-driven workflow guidelines", dim=True)
            click.secho("    TOOLS.md      — Environment-specific notes", dim=True)

        click.echo()
        default_ws = _DOCKER_WORKSPACE if self._inside_docker else "~/nerve-workspace"
        ws = click.prompt("Workspace path", default=default_ws)
        self.choices.workspace_path = Path(ws)
        click.echo()
        click.secho(
            "  Nerve also stores databases, logs, and session data in\n"
            f"  {paths.home_label()}/ — this is separate from your workspace.",
            dim=True,
        )
        click.echo()

    # --- Step: Password ---

    def _step_password(self) -> None:
        click.clear()
        click.secho(self._next_step("Web UI Password"), fg="cyan", bold=True)
        click.echo()
        click.secho(
            "Set a password for the web UI at localhost:8900, or press Enter to\n"
            "keep this installation passwordless.",
            dim=True,
        )
        click.echo()

        while True:
            pw = click.prompt(
                "Password (Enter for passwordless)", default="", hide_input=True,
            )
            if not pw:
                click.echo()
                click.secho(f"  {PASSWORDLESS_WARNING}", fg="yellow")
                if click.confirm("  Keep this installation passwordless?", default=False):
                    self.choices.password = ""
                    self.choices.passwordless = True
                    click.secho("  → Passwordless. You can set a password later "
                                "in the web UI.", dim=True)
                    break
                click.echo()
                continue
            pw2 = click.prompt("Confirm password", hide_input=True)
            if pw == pw2:
                self.choices.password = pw
                self.choices.passwordless = False
                click.echo()
                click.secho("  ✓ Password set", fg="green")
                break
            click.secho("  Passwords don't match. Try again.", fg="yellow")

        click.echo()

    # --- Step: Identity (personal only) ---

    def _step_identity(self) -> None:
        click.clear()
        click.secho(self._next_step("About You"), fg="cyan", bold=True)
        click.echo()
        click.secho(
            "In personal mode, Nerve develops a relationship with you\n"
            "over time. Let's set up the basics so it knows who it's\n"
            "talking to.",
            dim=True,
        )
        click.echo()
        self.choices.user_name = click.prompt("Your name", default="")
        self.choices.timezone = click.prompt("Your timezone", default="America/New_York")
        click.echo()
        click.secho(
            "  You can customize Nerve's name, personality, and style later\n"
            "  by editing SOUL.md and IDENTITY.md in your workspace.",
            dim=True,
        )
        click.echo()

    # --- Step: Channels (personal only) ---

    def _step_channels(self) -> None:
        click.clear()
        click.secho(self._next_step("Channels"), fg="cyan", bold=True)
        click.echo()
        click.secho(
            "Nerve communicates through channels. The web UI is always\n"
            "available at localhost:8900.\n\n"
            "Optionally, connect a Telegram bot for mobile notifications\n"
            "and chat. You'll need a bot token from @BotFather.",
            dim=True,
        )
        click.echo()
        if click.confirm("Set up Telegram bot?", default=False):
            token = click.prompt("  Bot token (from @BotFather)").strip()

            # Validate immediately — a bad token is much cheaper to fix now
            # than after the first silent failure.
            click.echo("  Verifying token...", nl=False)
            ok, detail = _telegram_get_me(token)
            if ok:
                click.secho(f" ✓ @{detail}", fg="green")
            else:
                click.secho(f" ✗ {detail}", fg="yellow")
                if not click.confirm("  Keep this token anyway?", default=False):
                    token = ""
                    click.secho("  → Skipping Telegram.", dim=True)

            if token:
                self.choices.telegram_bot_token = token
                click.echo()
                click.secho(
                    "  Only authorized users can talk to the bot. Enter your\n"
                    "  numeric Telegram user ID to authorize yourself now\n"
                    "  (message @userinfobot on Telegram to get it).\n\n"
                    "  Or press Enter to skip — after setup, run 'nerve pair'\n"
                    "  and send the bot /pair <code> in a private chat to "
                    "authorize.",
                    dim=True,
                )
                click.echo()
                uid_raw = click.prompt(
                    "  Your Telegram user ID (Enter to skip)", default="",
                ).strip()
                if uid_raw:
                    try:
                        self.choices.telegram_allowed_users = [int(uid_raw)]
                        click.secho("  ✓ You're authorized to DM the bot", fg="green")
                    except ValueError:
                        click.secho(
                            "  Not a number — skipping. Pair later with 'nerve pair'.",
                            fg="yellow",
                        )
                click.echo()
                click.secho("  ✓ Telegram bot configured", fg="green")
        else:
            click.secho("  → Skipping Telegram. You can set it up later in config.local.yaml.", dim=True)
        click.echo()

    # --- Step: Sync Sources (personal only) ---

    def _step_sources(self) -> None:
        click.clear()
        click.secho(self._next_step("Sync Sources"), fg="cyan", bold=True)
        click.echo()
        click.secho(
            "Nerve can poll external services for new messages and act\n"
            "on them — creating tasks, memorizing facts, sending you\n"
            "notifications. Each source needs its own CLI tool.",
            dim=True,
        )
        click.echo()

        # --- GitHub ---
        gh_available = bool(shutil.which("gh"))
        click.secho("  ┌─" + "─" * 52 + "┐", dim=True)
        click.secho(f"  │  {'GitHub Notifications':<52}│", bold=True)
        click.secho("  │" + " " * 53 + "│", dim=True)
        click.secho("  │  Syncs your GitHub notifications — PR reviews,   │", dim=True)
        click.secho("  │  issue mentions, CI failures. Creates tasks for  │", dim=True)
        click.secho("  │  things that need your attention.                │", dim=True)
        click.secho("  │" + " " * 53 + "│", dim=True)
        if gh_available:
            click.secho("  │  Requires: gh CLI  ✓ found                      │", fg="green")
        else:
            click.secho("  │  Requires: gh CLI  ✗ not found                  │", fg="yellow")
            click.secho("  │  Install: https://cli.github.com                │", fg="yellow")
        click.secho("  └─" + "─" * 52 + "┘", dim=True)

        if gh_available:
            if click.confirm("  Enable GitHub sync?", default=True):
                # Check if authenticated
                import subprocess
                result = subprocess.run(
                    ["gh", "auth", "status"], capture_output=True, text=True,
                )
                if result.returncode == 0:
                    self.choices.github_sync = True
                    click.secho("  ✓ GitHub sync enabled", fg="green")
                else:
                    click.secho("  gh is not authenticated. Run 'gh auth login' after setup.", fg="yellow")
                    if click.confirm("  Enable anyway (configure auth later)?", default=True):
                        self.choices.github_sync = True
        else:
            click.secho("  → Skipping — install gh CLI first.", dim=True)
        click.echo()

        # --- Gmail ---
        gog_available = bool(shutil.which("gog"))
        click.secho("  ┌─" + "─" * 52 + "┐", dim=True)
        click.secho(f"  │  {'Gmail':<52}│", bold=True)
        click.secho("  │" + " " * 53 + "│", dim=True)
        click.secho("  │  Syncs your email — surfaces actionable messages │", dim=True)
        click.secho("  │  and creates tasks. Ignores spam and newsletters.│", dim=True)
        click.secho("  │" + " " * 53 + "│", dim=True)
        if gog_available:
            click.secho("  │  Requires: gog CLI  ✓ found                     │", fg="green")
        else:
            click.secho("  │  Requires: gog CLI  ✗ not found                 │", fg="yellow")
            click.secho("  │  Install: https://github.com/steipete/gogcli    │", fg="yellow")
        click.secho("  └─" + "─" * 52 + "┘", dim=True)

        if gog_available:
            if click.confirm("  Enable Gmail sync?", default=False):
                accounts_str = click.prompt(
                    "  Gmail account(s) (comma-separated)",
                    default="",
                )
                if accounts_str.strip():
                    self.choices.gmail_sync = True
                    self.choices.gmail_accounts = [
                        a.strip() for a in accounts_str.split(",") if a.strip()
                    ]
                    click.secho(f"  ✓ Gmail sync enabled ({len(self.choices.gmail_accounts)} account(s))", fg="green")
                    click.secho(
                        "  Note: run 'gog gmail setup <account>' for each account\n"
                        "  after setup to complete OAuth authentication.",
                        dim=True,
                    )
                else:
                    click.secho("  → No accounts provided, skipping.", dim=True)
        else:
            click.secho("  → Skipping — install gog CLI first.", dim=True)
        click.echo()

        # --- Telegram Messages ---
        click.secho("  ┌─" + "─" * 52 + "┐", dim=True)
        click.secho(f"  │  {'Telegram Messages':<52}│", bold=True)
        click.secho("  │" + " " * 53 + "│", dim=True)
        click.secho("  │  Syncs messages from your Telegram chats and     │", dim=True)
        click.secho("  │  groups. Separate from the bot — this reads your │", dim=True)
        click.secho("  │  personal account via Telethon.                  │", dim=True)
        click.secho("  │" + " " * 53 + "│", dim=True)
        click.secho("  │  Requires: Telegram API credentials              │", fg="yellow")
        click.secho("  │  Get them at: https://my.telegram.org/apps       │", fg="yellow")
        click.secho("  └─" + "─" * 52 + "┘", dim=True)

        if click.confirm("  Enable Telegram message sync?", default=False):
            api_id_str = click.prompt("  API ID (from my.telegram.org)", default="")
            api_hash = click.prompt("  API Hash", default="")
            if api_id_str and api_hash:
                try:
                    self.choices.telegram_api_id = int(api_id_str)
                    self.choices.telegram_api_hash = api_hash
                    self.choices.telegram_sync = True
                    click.secho("  ✓ Telegram sync configured", fg="green")
                    click.secho(
                        "  Note: run 'nerve setup-telegram' after setup to\n"
                        "  complete the interactive authentication.",
                        dim=True,
                    )
                except ValueError:
                    click.secho("  Invalid API ID — must be a number. Skipping.", fg="yellow")
            else:
                click.secho("  → Missing credentials, skipping.", dim=True)
        else:
            click.secho("  → Skipping Telegram sync.", dim=True)
        click.echo()

        # Summary
        sources_enabled = []
        if self.choices.github_sync:
            sources_enabled.append("GitHub")
        if self.choices.gmail_sync:
            sources_enabled.append("Gmail")
        if self.choices.telegram_sync:
            sources_enabled.append("Telegram")

        if sources_enabled:
            click.secho(f"  Sources: {', '.join(sources_enabled)}", fg="green")
        else:
            click.secho("  No sync sources enabled — you can add them later in config.yaml.", dim=True)
        click.echo()

    # --- Step: System Crons (personal only) ---

    def _step_crons(self) -> None:
        click.clear()
        click.secho(self._next_step("Background Jobs"), fg="cyan", bold=True)
        click.echo()
        click.secho(
            "Nerve runs background jobs on a schedule — like a personal\n"
            "staff working while you sleep. Some are always on (memory\n"
            "maintenance, session cleanup). Others are optional:",
            dim=True,
        )
        click.echo()

        enabled = []
        for cron in PRODUCTIVITY_CRONS:
            click.secho("  ┌─" + "─" * 52 + "┐", dim=True)
            click.secho(f"  │  {cron['name']:<52}│", bold=True)
            click.secho("  │" + " " * 53 + "│", dim=True)

            # Word-wrap description to fit in the box
            desc_lines = _wrap_text(cron["description"], width=51)
            for line in desc_lines:
                click.secho(f"  │  {line:<51}│", dim=True)

            if cron.get("requires"):
                click.secho("  │" + " " * 53 + "│", dim=True)
                req_text = f"Requires: {cron['requires']}"
                click.secho(f"  │  {req_text:<51}│", fg="yellow")

            click.secho("  │" + " " * 53 + "│", dim=True)
            click.secho(f"  │  Schedule: {cron['schedule']:<39}│", dim=True)
            click.secho("  └─" + "─" * 52 + "┘", dim=True)

            if click.confirm(f"  Enable {cron['name'].lower()}?", default=True):
                enabled.append(cron["id"])
            click.echo()

        self.choices.enabled_crons = enabled

        click.secho("  Summary:", bold=True)
        for cron in PRODUCTIVITY_CRONS:
            status = "✓ enabled" if cron["id"] in enabled else "  disabled"
            color = "green" if cron["id"] in enabled else None
            click.secho(f"    {status}  {cron['name']}", fg=color)
        click.secho("    ✓ always   Memory Maintenance (core)", fg="cyan")
        click.echo()

    # --- Step: Worker Setup (worker only) ---

    def _step_worker_setup(self) -> None:
        """Collect task description for worker mode."""
        click.clear()
        click.secho(self._next_step("Worker Setup"), fg="cyan", bold=True)
        click.echo()
        click.secho(
            "In worker mode, Nerve needs to know what to do. Describe\n"
            "your task — what to monitor, what to fix, what to report.\n\n"
            "Examples:\n"
            '  "Monitor CI for repo X and fix flaky tests"\n'
            '  "Review PRs in org/repo and suggest improvements"\n'
            '  "Watch production logs and alert on anomalies"',
            dim=True,
        )
        click.echo()
        self.choices.task_description = click.prompt("Describe your task")
        click.echo()
        click.secho(
            "  This will be saved to TASK.md in your workspace.\n\n"
            "  On first boot, a setup agent will run with full tool\n"
            "  access to research your task, create a structured\n"
            "  TASK.md, generate custom cron jobs and skills, then\n"
            "  notify you when setup is complete.",
            dim=True,
        )
        click.echo()

    # --- Step: External Agents (Codex, Claude Code, ...) ---

    def _step_external_agents(self) -> None:
        """Pick which external chat agents Nerve should configure end-to-end.

        Writes the agent's config file (TOML/JSON) with the local MCP
        endpoint URL + bearer JWT, plus an initial memory bundle
        (~/.codex/AGENTS.md, ~/.claude/CLAUDE.md). The periodic sync
        service keeps the memory bundle fresh; the config file is
        one-shot and the user is free to edit it later.
        """
        from nerve.external_agents.registry import AGENT_REGISTRY

        click.clear()
        click.secho(self._next_step("External Agents (Optional)"), fg="cyan", bold=True)
        click.echo()
        click.secho(
            "Nerve can configure external chat agents (Codex, Claude\n"
            "Code) to use it as their MCP server. This writes the\n"
            "agent's config file, sets up MCP auth, and renders an\n"
            "AGENTS.md/CLAUDE.md memory bundle from your workspace\n"
            "identity files (SOUL, USER, MEMORY, ...).\n\n"
            "The sync cron then keeps the memory bundle fresh as your\n"
            "workspace evolves. Config files are written once and\n"
            "never overwritten — edit them freely.",
            dim=True,
        )
        click.echo()

        available = list(AGENT_REGISTRY.values())
        if not available:
            click.secho("  No external agent integrations registered.", dim=True)
            return

        selected: list[str] = []
        for agent in available:
            version = agent.smoke_check()
            badge = ""
            if version:
                badge = click.style(" (installed)", fg="green")
            elif agent.cli_command:
                badge = click.style(f" (not on PATH — `{agent.cli_command}`)", dim=True)
            prompt = f"  Configure {agent.display_name}?{badge}"
            if click.confirm(prompt, default=bool(version)):
                selected.append(agent.name)

        self.choices.external_agents = selected
        if not selected:
            click.echo()
            click.secho("  -> No external agents selected.", dim=True)
            click.echo()
            return

        # Detect conflicts so the user picks a policy before apply.
        existing: list[tuple[str, Path]] = []
        for name in selected:
            agent = AGENT_REGISTRY[name]
            for p in agent.default_config_paths():
                if p.exists():
                    existing.append((name, p))

        if existing:
            click.echo()
            click.secho("  These files already exist and will be touched:", bold=True)
            for name, p in existing:
                click.secho(f"    [{name}] {p}", dim=True)
            click.echo()
            self.choices.external_agents_conflict_policy = click.prompt(
                "  Conflict policy",
                type=click.Choice(["backup", "skip", "merge"]),
                default="backup",
            )
        click.echo()
        click.secho(
            f"  -> {len(selected)} agent(s) selected: "
            f"{', '.join(self.choices.external_agents)}",
            fg="green",
        )
        click.echo()

    # --- Step: Review ---

    def _step_review(self) -> None:
        click.clear()
        click.secho("Review", fg="cyan", bold=True)
        click.echo()

        ws = str(self.choices.workspace_path)
        if self.choices.provider_type == "bedrock":
            api_status = f"Bedrock ✓ ({self.choices.aws_region})"
        elif self.choices.claude_oauth_token:
            api_status = "OAuth ✓"
        elif self.choices.use_proxy:
            api_status = "Proxy ✓"
        elif self.choices.anthropic_api_key:
            api_status = "API key ✓"
        else:
            api_status = "—"
        if self.choices.openai_api_key:
            api_status += "  OpenAI ✓"
        else:
            api_status += "  OpenAI —"

        if not self.choices.telegram_bot_token:
            tg_status = "not configured"
        elif self.choices.telegram_allowed_users:
            tg_status = "configured (you're authorized)"
        else:
            tg_status = "configured (pair after setup)"

        click.secho("  ┌──────────────────────────────────────────────┐", dim=True)
        click.secho("  │            Setup Summary                     │", bold=True)
        click.secho("  ├──────────────────────────────────────────────┤", dim=True)
        click.secho(f"  │  Deploy:     {self.choices.deployment:<33}│")
        click.secho(f"  │  Mode:       {self.choices.mode:<33}│")
        click.secho(f"  │  Workspace:  {ws:<33}│")
        click.secho(f"  │  API keys:   {api_status:<33}│")
        pw_status = "set" if self.choices.password else (
            "none (passwordless)" if self.choices.passwordless else "none"
        )
        click.secho(f"  │  Password:   {pw_status:<33}│")

        if self.choices.mode == "personal":
            click.secho(f"  │  Telegram:   {tg_status:<33}│")
            # Sources summary
            src_parts = []
            if self.choices.github_sync:
                src_parts.append("GitHub")
            if self.choices.gmail_sync:
                src_parts.append("Gmail")
            if self.choices.telegram_sync:
                src_parts.append("Telegram")
            src_str = ", ".join(src_parts) if src_parts else "none"
            click.secho(f"  │  Sources:    {src_str:<33}│")
            if self.choices.enabled_crons:
                cron_str = ", ".join(self.choices.enabled_crons)
                # Wrap if too long
                if len(cron_str) > 33:
                    lines = _wrap_text(cron_str, width=33)
                    click.secho(f"  │  Crons:      {lines[0]:<33}│")
                    for line in lines[1:]:
                        click.secho(f"  │             {line:<33}│")
                else:
                    click.secho(f"  │  Crons:      {cron_str:<33}│")
            else:
                click.secho("  │  Crons:      none                            │")
            if self.choices.user_name:
                click.secho(f"  │  User:       {self.choices.user_name:<33}│")
            click.secho(f"  │  Timezone:   {self.choices.timezone:<33}│")
        else:
            task_preview = self.choices.task_description[:30] + "..." if len(self.choices.task_description) > 33 else self.choices.task_description
            click.secho(f"  │  Task:       {task_preview:<33}│")
            click.secho("  │  Setup:      on first boot (agent session)     │")

        click.secho("  └──────────────────────────────────────────────┘", dim=True)
        click.echo()
        click.secho(
            "  This will create config.yaml, config.local.yaml (with\n"
            "  your API keys), workspace files, and cron configuration.",
            dim=True,
        )
        click.echo()

        if not click.confirm("  Apply this configuration?", default=True):
            if click.confirm("  Restart setup?", default=True):
                # Re-run the whole wizard from scratch
                _clear_init_state()
                self.choices = SetupChoices()
                if self._inside_docker:
                    self.choices.deployment = "docker"
                self._completed_steps = set()
                self._step_counter = 0
                self.run()
                raise SystemExit(0)
            else:
                click.secho("  Aborted — your answers are saved; resume with 'nerve init'.", fg="yellow")
                raise SystemExit(0)

    # --- Apply ---

    def _apply(self) -> None:
        click.echo()

        # 1. Create workspace from templates (TASK.md is written in step 5),
        # worded for the memory backend an existing configuration names.
        click.echo("  Creating workspace...", nl=False)
        ws_path = self._workspace_dir()
        self.choices.memory_backend = (
            self._memory_backend or existing_memory_backend(self.config_dir, ws_path)
        )
        created = initialize_workspace(
            ws_path, self.choices.mode, skip=WRITER_OWNED_TEMPLATES,
            memory_backend=self.choices.memory_backend,
        )
        click.secho(" ✓", fg="green")

        # 2. Install bundled skills
        click.echo("  Installing bundled skills...", nl=False)
        install_bundled_skills(ws_path, memory_backend=self.choices.memory_backend)
        click.secho(" ✓", fg="green")

        # 2b. Scaffold the git-syncable config subtree (workspace/config/)
        click.echo("  Creating config subtree...", nl=False)
        install_config_scaffold(ws_path)
        click.secho(" ✓", fg="green")

        # 3. Fill the name/timezone lines of USER.md if a name was given
        # (personal mode). Same gate as before; the substitution itself is the
        # shared pristine-guarded one, which also fixes the legacy
        # {{USER_NAME}} token never matching the template's {{NAME}}.
        if self.choices.mode == "personal" and self.choices.user_name:
            apply_profile_templates(
                ws_path, name=self.choices.user_name, timezone=self.choices.timezone,
            )

        # 4. Patch TOOLS.md with Docker environment layout
        if self._inside_docker:
            tools_md = ws_path / "TOOLS.md"
            if tools_md.exists():
                content = tools_md.read_text(encoding="utf-8")
                content += _DOCKER_TOOLS_SECTION
                tools_md.write_text(content, encoding="utf-8")

        # 5. Worker: TASK.md from the template with the description in place —
        #    the same writer the web Mode step uses, so every install path
        #    produces one file for the onboarding conversation to fill in.
        if self.choices.mode == "worker":
            apply_task_md(ws_path, self.choices.task_description)

        # 6. Back up existing configs before regenerating — re-running init
        # must never silently destroy hand edits (model tweaks, paired
        # Telegram users, ...).
        backed_up = []
        for existing in (
            self.config_dir / "config.yaml",
            self.config_dir / "config.local.yaml",
            workspace_settings_file(self._workspace_dir()),
        ):
            if not existing.exists() or not _has_config_content(existing):
                # install_config_scaffold has just created a comments-only
                # settings.yaml, and it lives in a git-tracked directory —
                # backing that up would leave a junk .bak in the repo on every
                # fresh install, and claim to have rescued something.
                continue
            shutil.copy2(existing, existing.with_suffix(existing.suffix + ".bak"))
            backed_up.append(f"{existing.name}.bak")
        if backed_up:
            click.echo(f"  Backed up existing config → {', '.join(backed_up)}")

        # 7. Write config.yaml (machine-local) + settings.yaml (portable)
        click.echo("  Writing config.yaml...", nl=False)
        self._write_config_yaml()
        click.secho(" ✓", fg="green")

        click.echo("  Writing workspace config/settings.yaml...", nl=False)
        self._write_workspace_settings()
        click.secho(" ✓", fg="green")

        # 8. Write config.local.yaml
        click.echo("  Writing config.local.yaml...", nl=False)
        self._write_config_local_yaml()
        click.secho(" ✓", fg="green")

        # 9. Create the machine-local state directory. Cron config no longer
        # lives here — it's in workspace/config/cron.
        click.echo(f"  Setting up {paths.home_label()}/...", nl=False)
        nerve_dir = paths.ensure_nerve_home()
        click.secho(" ✓", fg="green")

        # 10. Write cron jobs
        click.echo("  Configuring cron jobs...", nl=False)
        self._write_cron_jobs()
        click.secho(" ✓", fg="green")

        # 11. Configure external agents (Codex, Claude Code, ...) if any
        if self.choices.external_agents:
            click.echo("  Configuring external agents...", nl=False)
            try:
                self._apply_external_agents(ws_path)
                click.secho(" ✓", fg="green")
            except Exception as e:
                click.secho(f" ✗ {e}", fg="red")

        # 12. Build web UI (server mode only — Docker handles this in entrypoint)
        if not self._inside_docker:
            self._build_web_ui()

    # ---- External agents apply step --------------------------------

    def _apply_external_agents(self, workspace: Path) -> None:
        """Issue an ephemeral MCP token, write each selected agent's config, and
        record them in config.yaml so the sync service can keep their
        memory bundles fresh.

        Token strategy: per the user's decision in plan-3bc42e5f we
        reuse the existing single gateway JWT mechanism (no per-agent
        token table). Generated clients reference ``NERVE_MCP_TOKEN`` rather
        than storing the credential; users refresh it with
        ``nerve codex token``. The token is scoped to MCP and expires.
        """
        import asyncio

        from nerve.external_agents.registry import AGENT_REGISTRY
        from nerve.external_agents.writer import ConfigWriter

        token = self._issue_mcp_token()
        nerve_url = self._compute_nerve_mcp_url()
        self.choices.external_agents_token = token
        self.choices.external_agents_mcp_url = nerve_url

        writer = ConfigWriter(
            conflict_policy=self.choices.external_agents_conflict_policy,
        )

        async def _run_all():
            results = []
            for agent_name in self.choices.external_agents:
                agent = AGENT_REGISTRY.get(agent_name)
                if agent is None:
                    continue
                result = await agent.write_config(
                    nerve_url=nerve_url,
                    mcp_token=token,
                    workspace=workspace,
                    writer=writer,
                )
                results.append(result)
            return results

        results = asyncio.run(_run_all())

        # Persist the agents to config.yaml so the SyncService picks
        # them up on next start.
        self._record_external_agents_in_config_yaml(results)

    def _issue_mcp_token(self) -> str:
        """Issue an eight-hour, MCP-audience JWT for external agents.

        If no jwt_secret is configured yet (dev mode), returns a
        placeholder string that the user can replace later. The MCP
        endpoint's auth bypass covers dev mode so this still works
        end-to-end on a fresh install.
        """
        from nerve.gateway.auth import create_external_mcp_token

        secret = self._resolve_jwt_secret()
        if not secret:
            return "dev-mode-no-auth"

        return create_external_mcp_token(secret)

    def _resolve_jwt_secret(self) -> str:
        """Return the JWT secret we're about to write to config.local.yaml.

        The wizard generates the secret in ``_write_config_local_yaml``
        and stores it on ``self.choices``. If apply order changes in
        the future this method keeps the dependency explicit.
        """
        # The cache is not the usual path: _write_config_local_yaml puts the
        # secret straight into the dict it dumps and never sets
        # _jwt_secret_cache, so a normal `nerve init` falls through to the read
        # below. That read pins its encoding because the except clause around it
        # does not fail safe -- on a decode error it returns a newly generated
        # secret that is not on disk, the external-agent MCP token is signed with
        # that, and the daemon rejects every call the token makes.
        secret = getattr(self.choices, "_jwt_secret_cache", "")
        if secret:
            return secret
        # Read back from the just-written config.local.yaml
        local = self.config_dir / "config.local.yaml"
        if local.exists():
            try:
                data = yaml.safe_load(local.read_text(encoding="utf-8")) or {}
                secret = (data.get("auth") or {}).get("jwt_secret", "") or ""
            except Exception:
                secret = ""
        if not secret:
            # Generate one now and stash so subsequent calls match.
            secret = secrets.token_urlsafe(48)
            self.choices._jwt_secret_cache = secret  # type: ignore[attr-defined]
        return secret

    def _compute_nerve_mcp_url(self) -> str:
        """Return the MCP endpoint URL external agents should hit.

        Uses the gateway scheme/host/port and MCP path from the configuration
        just written by the wizard. The trailing slash matters.

        Reads the merged layers rather than config.yaml alone. gateway.host and
        .port are written to the tracked settings, so reading only the machine
        file would build the URL from the declared defaults and ignore a port the
        operator had set.
        """
        raw: dict = {}
        try:
            from nerve.config import _read_config_sources

            raw = _read_config_sources(self.config_dir) or {}
        except Exception:
            # A half-written or unloadable config must not sink `nerve init`;
            # the per-key fallbacks below still produce a usable local URL.
            raw = {}
        gateway = raw.get("gateway") or {}
        ssl = gateway.get("ssl") or {}
        scheme = "https" if ssl.get("cert") and ssl.get("key") else "http"
        host = str(gateway.get("host") or "127.0.0.1")
        if host in {"0.0.0.0", "::", "[::]"}:
            host = "localhost"
        port = int(gateway.get("port") or 8900)
        endpoint = raw.get("mcp_endpoint") or {}
        mcp_path = "/" + str(endpoint.get("path") or "/mcp/v1").strip("/")
        return f"{scheme}://{host}:{port}{mcp_path}/"

    def _record_external_agents_in_config_yaml(self, results) -> None:
        """Record the external_agents block in the freshly-written config.yaml.

        Through the shared writer, so the file keeps its header and is
        republished atomically.
        """
        path = self.config_dir / "config.yaml"
        if not path.exists():
            return
        try:
            merge_external_agents(
                self.config_dir,
                targets=[r.agent for r in results],
                conflict_policy=self.choices.external_agents_conflict_policy,
            )
        except Exception as e:
            click.secho(
                f"  Warning: could not record external_agents in config.yaml: {e}",
                fg="yellow",
            )

    def _build_web_ui(self) -> None:
        """Build the web UI if not already built."""
        import subprocess

        web_dir = self.config_dir / "web"
        dist_dir = web_dir / "dist"

        if not web_dir.exists():
            # Not in the source tree (e.g. pip-installed) — skip
            return

        if dist_dir.exists():
            click.echo("  Web UI already built — skipping")
            return

        if not shutil.which("node"):
            click.secho(
                "  ⚠ Node.js not found — web UI not built.\n"
                "    Install Node.js 22.12+ and run: cd web && npm ci && npm run build",
                fg="yellow",
            )
            return

        click.echo("  Building web UI...", nl=False)
        try:
            # Install dependencies
            subprocess.run(
                ["npm", "ci", "--quiet"],
                cwd=str(web_dir),
                capture_output=True,
                check=True,
            )
            # Build
            subprocess.run(
                ["npm", "run", "build"],
                cwd=str(web_dir),
                capture_output=True,
                check=True,
            )
            click.secho(" ✓", fg="green")
        except subprocess.CalledProcessError as e:
            click.secho(" ✗", fg="red")
            stderr = e.stderr.decode() if e.stderr else ""
            if stderr:
                # Show last few lines of error
                lines = stderr.strip().splitlines()[-5:]
                for line in lines:
                    click.secho(f"    {line}", dim=True)
            click.secho(
                "    You can build manually: cd web && npm ci && npm run build",
                fg="yellow",
            )

    def _workspace_dir(self) -> Path:
        """The workspace path, expanded the same way the config loader does."""
        return workspace_dir(self.choices)

    @staticmethod
    def _leaf_paths(d: dict[str, Any], prefix: str = "") -> dict[str, Any]:
        """Flatten a nested dict to ``{"a.b": value}``. Lists are leaves."""
        return leaf_paths(d, prefix)

    def _build_config_layers(
        self,
    ) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
        """Split the wizard's answers into (machine-local, portable, shadowed)."""
        return build_config_layers(self.choices)

    def _write_config_yaml(self) -> None:
        """Write the machine-local base config.yaml."""
        write_config_yaml(self.choices, self.config_dir)

    def _write_workspace_settings(self) -> None:
        """Write the portable half into ``<workspace>/config/settings.yaml``.

        The writer decides what to merge and reports what it did; the words
        below are this wizard's, because it is the half that talks to a
        terminal. A file that is not YAML, or not a mapping, is left exactly
        as it was — the operator is told rather than having a hand-written
        settings file replaced by generated one.
        """
        outcome = write_workspace_settings(self.choices)
        if outcome.status == "invalid_yaml":
            click.secho(
                f"\n    Warning: {outcome.path} is not valid YAML"
                f" ({outcome.detail}) — leaving it alone.",
                fg="yellow",
            )
            return
        if outcome.status == "not_a_mapping":
            click.secho(
                f"\n    Warning: {outcome.path} is not a mapping —"
                " leaving it alone.",
                fg="yellow",
            )
            return
        if not outcome.wrote:
            return

        # safe_dump cannot round-trip comments, and this file is meant to be
        # read by other people in a PR. Say so rather than quietly deleting
        # someone's rationale.
        if outcome.comments_lost:
            click.secho(
                f"\n    Note: comments in {outcome.path.name} are not preserved"
                " when it is regenerated (the previous file is in"
                f" {outcome.path.name}.bak).",
                fg="yellow",
            )

        for label, items in (("added", outcome.added), ("updated", outcome.changed),
                             ("removed", outcome.removed)):
            if items:
                click.secho(f"\n    {label}: {', '.join(items)}", dim=True)

    def _write_config_local_yaml(self) -> None:
        """Write config.local.yaml with secrets.

        If the file cannot be made owner-only, nothing is written and setup
        stops.
        """
        try:
            write_config_local_yaml(self.choices, self.config_dir)
        except paths.InsecureFileError as e:
            raise click.ClickException(
                f"Setup stopped: {e} Nothing was written to "
                f"{self.config_dir / 'config.local.yaml'}."
            ) from e

    def _write_cron_jobs(self) -> None:
        """Write system crons to system.yaml and scaffold jobs.yaml for user crons."""
        outcome = write_cron_jobs(self.choices)
        if outcome.migrated_from is not None:
            click.echo(
                f"\n    Migrated custom crons from {outcome.migrated_from} to"
                f" {outcome.jobs_file}",
            )

    # --- Preflight ---

    def _preflight(self) -> None:
        """Validate the configuration that was just written with real calls.

        Catches bad API keys, unavailable Bedrock models (wrong region /
        no model access), and broken Telegram tokens at setup time instead
        of at the first message. Failures are informative, never fatal.
        """
        click.echo()
        click.secho("  Preflight checks:", bold=True)
        failures: list[str] = []

        # --- Claude API ---
        if self.choices.use_proxy:
            click.secho(
                "    – Claude API: skipped (proxy mode — verified at first start)",
                dim=True,
            )
        elif self.choices.claude_oauth_token:
            click.secho(
                "    – Claude API: skipped (OAuth token — verified on first use)",
                dim=True,
            )
        elif self.choices.provider_type == "bedrock" or self.choices.anthropic_api_key:
            click.echo("    · Claude API: testing...", nl=False)
            try:
                from nerve.cli import _check_api_connectivity
                from nerve.config import load_config as _load_config

                cfg = _load_config(self.config_dir)
                ok, detail = _check_api_connectivity(cfg)
            except Exception as e:  # never let preflight crash the wizard
                ok, detail = False, str(e)
            if ok:
                click.secho(f" ✓ {detail}", fg="green")
            else:
                click.secho(f" ✗ {detail}", fg="red")
                failures.append("Claude API")
                if self.choices.provider_type == "bedrock":
                    click.secho(
                        "      Check Claude model access for your region in the AWS\n"
                        "      Bedrock console. If this model isn't offered in your\n"
                        "      geography, edit agent.model in config.yaml.",
                        dim=True,
                    )
        else:
            click.secho("    – Claude API: no credentials configured", dim=True)

        # --- Telegram ---
        if self.choices.telegram_bot_token:
            click.echo("    · Telegram bot: testing...", nl=False)
            ok, detail = _telegram_get_me(self.choices.telegram_bot_token)
            if ok:
                click.secho(f" ✓ @{detail}", fg="green")
            else:
                click.secho(f" ✗ {detail}", fg="red")
                failures.append("Telegram")

        # --- OpenAI (optional embeddings) ---
        if self.choices.openai_api_key:
            click.echo("    · OpenAI API: testing...", nl=False)
            ok, detail = _check_openai_key(self.choices.openai_api_key)
            if ok:
                click.secho(f" ✓ {detail}", fg="green")
            else:
                click.secho(f" ✗ {detail}", fg="red")
                failures.append("OpenAI")

        if failures:
            click.echo()
            click.secho(
                f"  ⚠ {len(failures)} check(s) failed: {', '.join(failures)}.\n"
                "    Nerve will still start — fix the values in config.local.yaml\n"
                "    (or config.yaml) and re-verify with 'nerve doctor'.",
                fg="yellow",
            )

    # --- Done ---

    def _done(self) -> None:
        click.echo()
        click.secho("  ✅ Nerve is configured!", fg="green", bold=True)
        click.echo()

        click.secho("  Next steps:", bold=True)
        if self._inside_docker:
            click.echo("    nerve start              Start the container")
            click.echo("    nerve stop               Stop the container")
            click.echo("    nerve logs               Follow logs")
            click.echo("    nerve status             Container status")
        else:
            click.echo("    nerve start              Start the server")
            click.echo("    nerve start -f           Start in foreground (see logs)")
            click.echo("    nerve doctor             Verify everything is set up")
        click.echo("    http://localhost:8900     Open the web UI")
        click.echo()
        ws = str(self._workspace_dir())
        click.secho(f"  Your workspace: {ws}", bold=True)
        click.echo("    Edit SOUL.md to customize Nerve's personality")
        click.echo("    Edit USER.md to tell Nerve about yourself")
        click.echo()
        if self.choices.telegram_bot_token and not self.choices.telegram_allowed_users:
            click.secho("  Telegram pairing:", bold=True)
            click.echo("    1. Start Nerve, then run: nerve pair")
            click.echo("    2. In a private chat, send the bot:  /pair <code>")
            click.echo("    (until paired, the bot ignores all DMs)")
            click.echo()
        click.secho(
            "  Tip: Nerve learns from every conversation. The more\n"
            "  you interact, the more useful it becomes.",
            dim=True,
        )
        click.echo()


# --- The slim host-side setup (default `nerve init`) ---------------------------
#
# Only what the browser cannot decide: how Nerve runs on this machine, where
# its files live, how the gateway listens. Everything else — mode, model,
# profile, channels — is decided on the setup page the daemon serves. The
# answers are checkpointed after every prompt (same file the full wizard
# uses), the writes go through the shared writer, and nothing here starts a
# process: the CLI command does that, so this stays testable without a daemon.

SLIM_PORT_RANGE = range(8901, 9000)
_SLIM_STEP_KEYS = ("slim:deployment", "slim:workspace", "slim:network")
_API_KEY_SOURCES = ("ANTHROPIC_API_KEY env var", "macOS Keychain (API key)")


@dataclass
class SlimFlags:
    """Scriptable answers for the slim flow; ``None`` means ask (or default)."""

    deployment: str | None = None
    workspace: str | None = None
    port: int | None = None
    bind: str | None = None          # "localhost" | "lan"
    yes: bool = False
    start: bool = True
    browser: bool | None = None
    clipboard: bool = True
    # The memory backend the skeleton is worded for (``--memory-backend``);
    # None reads it from an existing configuration.
    memory_backend: str | None = None


@dataclass
class SlimResult:
    choices: SetupChoices
    outcome: SlimOutcome
    host_workspace: Path
    bind: str
    web_ui: str                      # "present" | "built" | "skipped" | "failed" | "not-in-tree"
    docker_files_created: list[str]


_CONNECT_PROBE_TIMEOUT = 0.25


def port_is_free(port: int, host: str = "127.0.0.1") -> bool:
    """Whether the gateway could listen on ``host:port`` right now. Never raises.

    A plain ``bind`` is the first word, not the last: for up to a minute after
    a listener closes its connections the port is still held by sockets in
    ``TIME_WAIT``, and a plain bind answers ``EADDRINUSE`` although nothing
    listens — exactly the moment someone re-runs ``nerve init`` after stopping
    the previous daemon. The server itself binds with ``SO_REUSEADDR`` (what
    uvicorn sets), which steps over ``TIME_WAIT`` but not over a listener. (On
    Linux that step also needs the earlier socket to have had ``SO_REUSEADDR``,
    which the gateway's always does; BSD kernels ask only the new socket.)

    So on ``EADDRINUSE`` the probe asks the two questions that tell the cases
    apart: does anything *accept* a connection on the port (a listener — on
    any interface, since a wildcard listener answers on loopback too), and
    does a bind with the server's own ``SO_REUSEADDR`` semantics succeed? Only
    "nobody accepts" *and* "the server's bind would work" counts as free; a
    listener on another interface alone still fails the second bind.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind((host, int(port)))
        return True
    except OSError as exc:
        if exc.errno != errno.EADDRINUSE:
            return False
    target = "127.0.0.1" if host in ("", "0.0.0.0") else host
    try:
        with socket.create_connection((target, int(port)), timeout=_CONNECT_PROBE_TIMEOUT):
            return False  # something accepted: a live listener
    except ConnectionRefusedError:
        pass  # nobody listens on this side; the bind failed on TIME_WAIT alone
    except OSError:
        return False  # a timeout or a filtered port: treat as busy
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind((host, int(port)))
        return True
    except OSError:
        return False


def _probe_hosts(bind: str) -> tuple[str, ...]:
    return ("127.0.0.1",) if bind == "localhost" else ("127.0.0.1", "0.0.0.0")


def propose_port(preferred: int, bind: str) -> tuple[int, bool]:
    """``(port, preferred_was_busy)``: the preferred port, or the first free one after it."""
    hosts = _probe_hosts(bind)
    if all(port_is_free(preferred, h) for h in hosts):
        return preferred, False
    for candidate in SLIM_PORT_RANGE:
        if candidate != preferred and all(port_is_free(candidate, h) for h in hosts):
            return candidate, True
    return preferred, True


def _web_dir(config_dir: Path) -> Path | None:
    """The ``web/`` directory the gateway serves ``dist`` from.

    Resolved the way ``server.py`` resolves ``web/dist``: next to the ``nerve``
    package (the install tree), with the config directory as a fallback for a
    checkout used as its own config dir. ``None`` when neither exists.
    """
    import nerve

    for candidate in (Path(nerve.__file__).resolve().parent.parent / "web", Path(config_dir) / "web"):
        if candidate.exists():
            return candidate
    return None


def _ensure_web_ui(config_dir: Path) -> str:
    """Build ``web/dist`` if it is missing and Node is available.

    Returns ``present``, ``built``, ``skipped`` (no Node), ``failed`` or
    ``not-in-tree``. A failure is a warning: the gateway serves a fallback
    page at ``/setup`` until the UI is built.
    """
    web_dir = _web_dir(config_dir)
    if web_dir is None:
        return "not-in-tree"
    if (web_dir / "dist").exists():
        return "present"
    if not shutil.which("node") or not shutil.which("npm"):
        return "skipped"
    try:
        subprocess.run(["npm", "ci", "--quiet"], cwd=str(web_dir), capture_output=True, check=True, timeout=900)
        subprocess.run(["npm", "run", "build"], cwd=str(web_dir), capture_output=True, check=True, timeout=900)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
        return "failed"
    return "built"


class SlimSetup:
    """The default ``nerve init``: deployment, paths, network — then the browser."""

    def __init__(self, config_dir: Path, flags: SlimFlags | None = None):
        self.config_dir = Path(config_dir)
        self.flags = flags or SlimFlags()
        self.choices = SetupChoices()
        self.bind = "localhost"
        self._host_workspace = Path("~/nerve-workspace")
        self._completed: set[str] = set()
        # Steps that printed a header and asked; the rest are summarised in
        # one "Using:" line so a flag-driven run is not a list of empty headers.
        self._prompted: set[str] = set()

    # -- prompts ---------------------------------------------------------------

    def _ask(self, label: str, default: str, *, choices: list[str] | None = None) -> str:
        if self.flags.yes:
            return default
        kwargs: dict[str, Any] = {"default": default}
        if choices:
            # The label already lists the options ("Choose [server/docker]");
            # click would otherwise print them a second time in parentheses.
            kwargs["type"] = click.Choice(choices, case_sensitive=False)
            kwargs["show_choices"] = False
        return str(click.prompt(label, **kwargs))

    def _checkpoint(self, key: str) -> None:
        self._completed.add(key)
        _save_init_state(self.choices, self._completed)

    def _resume(self) -> None:
        state = _load_init_state()
        if not state:
            return
        completed = set(state.get("completed") or [])
        if not completed & set(_SLIM_STEP_KEYS):
            return
        try:
            saved = _choices_from_dict(state.get("choices") or {})
        except (TypeError, ValueError):
            _clear_init_state()
            return
        # Only the answers this flow owns are taken from the checkpoint.
        self.choices.deployment = saved.deployment
        self.choices.workspace_path = saved.workspace_path
        self.choices.gateway_host = saved.gateway_host
        self.choices.gateway_port = saved.gateway_port
        self._host_workspace = (
            Path(str(state.get("slim_host_workspace") or saved.workspace_path))
        )
        self._completed = completed & set(_SLIM_STEP_KEYS)
        click.secho("  Resuming the answers saved by the previous run.", dim=True)

    def _step_deployment(self) -> None:
        if self.flags.deployment:
            self.choices.deployment = self.flags.deployment.lower()
        elif "slim:deployment" not in self._completed and not self.flags.yes:
            self._prompted.add("deployment")
            click.secho("  Step 1 of 3 · How do you want to run Nerve?", bold=True)
            click.echo("    server   Run directly on this machine. Recommended on a server or your own laptop.")
            click.echo("    docker   Run in a container. Isolated, easy to remove. Needs Docker.")
            self.choices.deployment = self._ask(
                "  Choose [server/docker]", "server", choices=["server", "docker"],
            ).lower()
        elif "slim:deployment" not in self._completed:
            self.choices.deployment = "server"
        if self.choices.deployment not in ("server", "docker"):
            raise click.ClickException("--deployment must be server or docker")
        self._checkpoint("slim:deployment")

    def _step_paths(self) -> None:
        docker = self.choices.deployment == "docker"
        if self.flags.workspace:
            raw = self.flags.workspace
        elif "slim:workspace" in self._completed:
            raw = str(self._host_workspace)
        elif self.flags.yes:
            raw = "~/nerve-workspace"
        else:
            self._prompted.add("paths")
            click.echo()
            click.secho("  Step 2 of 3 · Where things live", bold=True)
            click.echo(f"    Config files   {self.config_dir}   (config.yaml, config.local.yaml)")
            click.echo(
                f"    State          {paths.nerve_home()}   (database, logs; set NERVE_HOME "
                "before running nerve to move it)"
            )
            label = (
                "  Workspace on this computer (mounted into the container)" if docker
                else "  Workspace — the agent's files: memory, skills, settings"
            )
            raw = self._ask(label, "~/nerve-workspace")
        self._host_workspace = Path(raw.strip() or "~/nerve-workspace")
        # config.yaml records the container path for a docker install; the
        # host path is where the tracked settings and the skeleton are written.
        self.choices.workspace_path = Path(_DOCKER_WORKSPACE) if docker else self._host_workspace
        self._checkpoint("slim:workspace")

    def _step_network(self) -> None:
        bind = (self.flags.bind or "").lower() or None
        resumed = "slim:network" in self._completed
        if bind is None and resumed:
            bind = "localhost" if self.choices.gateway_host == "127.0.0.1" else "lan"
        ask_port = self.flags.port is None and not self.flags.yes
        ask_bind = bind is None and not self.flags.yes
        if ask_port or ask_bind:
            self._prompted.add("network")
            click.echo()
            click.secho("  Step 3 of 3 · Network", bold=True)
        probe_bind = bind or "lan"
        hosts = _probe_hosts(probe_bind)
        if self.flags.port is not None:
            # An explicit port is probed like a typed one: `/health` turning
            # green on a busy port would be another program's answer.
            port = int(self.flags.port)
            if not 1 <= port <= 65535:
                raise click.ClickException("--port must be a number between 1 and 65535")
            if not all(port_is_free(port, h) for h in hosts):
                raise click.ClickException(
                    f"Port {port} is already in use by another program; pick another with --port"
                )
        else:
            preferred = int(self.choices.gateway_port if resumed else 8900)
            proposed, busy = propose_port(preferred, probe_bind)
            if busy:
                click.echo(f"  Port {preferred} is already in use by another program. {proposed} is free.")
            while True:
                answer = self._ask("  Gateway port", str(proposed))
                try:
                    port = int(answer)
                except ValueError:
                    port = 0
                if not 1 <= port <= 65535:
                    click.secho("  A port is a number between 1 and 65535.", fg="yellow")
                    continue
                if all(port_is_free(port, h) for h in hosts):
                    break
                # Re-validated: a typed port is a choice, not a fact.
                proposed, _busy = propose_port(port, probe_bind)
                click.secho(
                    f"  Port {port} is already in use by another program. {proposed} is free.", fg="yellow",
                )
                if self.flags.yes:
                    port = proposed
                    break
        if bind is None:
            click.echo("  Who should be able to open the web UI?")
            click.echo("    localhost   Only this computer. Open it here, or through an SSH tunnel from elsewhere.")
            click.echo("    lan         Other devices on your network too. You will set a password on the setup page.")
            bind = self._ask("  Reachable from", "localhost", choices=["localhost", "lan"]).lower()
        if bind not in ("localhost", "lan"):
            raise click.ClickException("--bind must be localhost or lan")
        self.bind = bind
        self.choices.gateway_port = port
        # Inside a container the gateway listens on every interface; the
        # compose publish rule limits exposure to the chosen bind.
        self.choices.gateway_host = (
            "0.0.0.0" if (bind == "lan" or self.choices.deployment == "docker") else "127.0.0.1"
        )
        self._checkpoint("slim:network")

    def _step_docker_credentials(self) -> None:
        """2b: forward the host's Claude Code login and gh token to the container.

        Both are the operator's *personal* credentials, and both land in the
        instance's secret file — so each is forwarded only after the same
        confirmation (``--yes`` accepts), and each forward is said out loud.
        The values themselves are never printed.
        """
        token, source, _debug = _resolve_claude_credential()
        if token:
            forward = True
            if not self.flags.yes:
                forward = click.confirm(
                    f"  Found a Claude Code login in this machine's keychain ({source}). "
                    "Forward it to the container?", default=True,
                )
            if forward:
                if source in _API_KEY_SOURCES:
                    self.choices.anthropic_api_key = token
                    key = "anthropic_api_key"
                else:
                    self.choices.claude_oauth_token = token
                    key = "claude_oauth_token"
                click.secho(
                    f"    ✓ Forwarding the Claude Code login ({source}) as {key} in config.local.yaml",
                    fg="green",
                )
        gh_token, gh_source = _resolve_gh_token()
        if gh_token:
            forward = True
            if not self.flags.yes:
                forward = click.confirm(
                    f"  Found a GitHub login ({gh_source}). Forward it to the container?",
                    default=True,
                )
            if forward:
                self.choices.github_token = gh_token
                click.secho(
                    f"    ✓ Forwarding the GitHub token ({gh_source}) as github_token in config.local.yaml",
                    fg="green",
                )

    # -- run -------------------------------------------------------------------

    def run(self) -> SlimResult:
        click.echo()
        click.secho("  Nerve setup", bold=True)
        click.echo(
            "  Most of the setup happens in your browser. This terminal only decides what the\n"
            "  browser cannot: how Nerve runs on this machine and where it keeps its files.\n"
            "  Ctrl+C is safe; answers are saved."
        )
        click.echo()
        self._resume()
        try:
            self._step_deployment()
            self._step_paths()
            self._step_network()
            if self.choices.deployment == "docker":
                self._step_docker_credentials()
        except (KeyboardInterrupt, click.Abort):
            click.echo()
            click.secho("  Setup interrupted — your answers are saved.", fg="yellow")
            click.secho("  Resume anytime with: nerve init", dim=True)
            raise SystemExit(130)

        docker = self.choices.deployment == "docker"
        host_ws = _expand_workspace(str(self._host_workspace))
        docker_created: list[str] = []
        if self._prompted != {"deployment", "paths", "network"}:
            # Whatever was not asked is still said, once, in one line.
            click.echo(
                f"  Using: {self.choices.deployment} · workspace {self._host_workspace} · "
                f"port {self.choices.gateway_port} · {self.bind}"
            )
        click.echo()
        self.config_dir.mkdir(parents=True, exist_ok=True)
        if docker:
            docker_created = _ensure_docker_files(
                self.config_dir, str(self._host_workspace),
                port=self.choices.gateway_port, bind=self.bind,
            )
        self.choices.memory_backend = (
            self.flags.memory_backend or existing_memory_backend(self.config_dir, host_ws)
        )
        outcome = write_slim_install(
            self.choices, self.config_dir, host_workspace=host_ws if docker else None,
        )
        click.secho(f"    ✓ Created workspace skeleton in {host_ws} (config/, skills/)", fg="green")
        click.secho("    ✓ Wrote config.yaml and config/settings.yaml", fg="green")
        if outcome.jwt_created:
            click.secho("    ✓ Wrote config.local.yaml (owner-only)", fg="green")
        else:
            click.secho("    ✓ Kept config.local.yaml (signing secret unchanged)", fg="green")

        web_ui = "not-in-tree"
        if not docker:
            web_dir = _web_dir(self.config_dir)
            if web_dir is not None and not (web_dir / "dist").exists() and shutil.which("node"):
                click.echo("    Building the web UI (first time only, about a minute)…")
            web_ui = _ensure_web_ui(self.config_dir)
            if web_ui == "present":
                click.secho("    ✓ Web UI already built", fg="green")
            elif web_ui == "built":
                click.secho("    ✓ Web UI built", fg="green")
            elif web_ui == "not-in-tree":
                click.secho(
                    "    ⚠ No web/ directory next to this installation — the setup page will "
                    "say how to build the UI (nerve upgrade, or cd web && npm ci && npm run build)",
                    fg="yellow",
                )
            elif web_ui == "skipped":
                click.secho(
                    "    ⚠ Node.js not found — the web UI was not built. The setup page shows "
                    "how to build it; install Node.js 22.12+ and run: cd web && npm ci && npm run build",
                    fg="yellow",
                )
            elif web_ui == "failed":
                click.secho(
                    "    ⚠ The web UI build failed; the setup page will say so. Build it with: "
                    "cd web && npm ci && npm run build",
                    fg="yellow",
                )
        _clear_init_state()
        return SlimResult(
            choices=self.choices, outcome=outcome, host_workspace=host_ws, bind=self.bind,
            web_ui=web_ui, docker_files_created=docker_created,
        )


# --- Non-interactive mode ---


def run_non_interactive(config_dir: Path, *, memory_backend: str | None = None) -> SetupChoices:
    """Non-interactive setup using environment variables. For Docker.

    ``memory_backend`` (``nerve init --memory-backend``) words the workspace
    for that memory backend."""
    choices = SetupChoices()

    # Provider detection — check before API key requirements.
    provider = os.environ.get("NERVE_PROVIDER", "anthropic")
    if provider == "bedrock":
        choices.provider_type = "bedrock"
        choices.aws_region = os.environ.get(
            "NERVE_AWS_REGION", os.environ.get("AWS_REGION", "us-east-1"),
        )
        choices.aws_profile = os.environ.get(
            "NERVE_AWS_PROFILE", os.environ.get("AWS_PROFILE", ""),
        )
        # Bedrock uses IAM — no Anthropic API key needed
    else:
        # API auth: OAuth token, API key, or proxy mode.
        # Follows priority waterfall — first match wins.
        use_proxy = os.environ.get("NERVE_USE_PROXY", "") == "1"
        choices.use_proxy = use_proxy

        claude_oauth_token = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", "")
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")

        if claude_oauth_token:
            choices.claude_oauth_token = claude_oauth_token
        if api_key:
            choices.anthropic_api_key = api_key

        if not use_proxy and not api_key and not claude_oauth_token:
            raise click.ClickException(
                "ANTHROPIC_API_KEY or CLAUDE_CODE_OAUTH_TOKEN environment variable is required "
                "for non-interactive setup (or set NERVE_USE_PROXY=1 to use CLIProxyAPI, "
                "or set NERVE_PROVIDER=bedrock for AWS Bedrock)"
            )

    # GitHub token (from host extraction or env var)
    gh_token = os.environ.get("GH_TOKEN", "")
    if gh_token:
        choices.github_token = gh_token

    # Auto-detect Docker via env var
    is_docker = os.environ.get("NERVE_DOCKER", "") == "1"
    choices.deployment = "docker" if is_docker else "server"

    # Optional
    choices.mode = os.environ.get("NERVE_MODE", "personal")
    choices.openai_api_key = os.environ.get("OPENAI_API_KEY", "")
    default_ws = _DOCKER_WORKSPACE if is_docker else "~/nerve-workspace"
    choices.workspace_path = Path(os.environ.get("NERVE_WORKSPACE", default_ws))
    choices.timezone = os.environ.get("NERVE_TIMEZONE", "America/New_York")
    choices.telegram_bot_token = os.environ.get("NERVE_TELEGRAM_BOT_TOKEN", "")
    # Comma-separated Telegram user IDs allowed to DM the bot. Without this
    # the bot starts in pairing mode (authorize via `nerve pair`).
    allowed_raw = os.environ.get("NERVE_TELEGRAM_ALLOWED_USERS", "")
    if allowed_raw.strip():
        try:
            choices.telegram_allowed_users = [
                int(u.strip()) for u in allowed_raw.split(",") if u.strip()
            ]
        except ValueError:
            raise click.ClickException(
                f"NERVE_TELEGRAM_ALLOWED_USERS must be comma-separated numeric "
                f"user IDs, got: {allowed_raw!r}"
            )
    choices.password = os.environ.get("NERVE_PASSWORD", "")
    passwordless_raw = os.environ.get("NERVE_PASSWORDLESS", "").strip()
    if passwordless_raw not in ("", "0", "1"):
        raise click.ClickException(
            f"NERVE_PASSWORDLESS must be 1 or 0, got: {passwordless_raw!r}"
        )
    choices.passwordless = passwordless_raw == "1"
    if choices.password and choices.passwordless:
        raise click.ClickException(
            "Set NERVE_PASSWORD or NERVE_PASSWORDLESS=1, not both"
        )
    if choices.passwordless:
        click.secho(PASSWORDLESS_WARNING, fg="yellow")

    # Sources — auto-detect from available CLIs
    if choices.mode == "personal":
        if shutil.which("gh"):
            choices.github_sync = True
        if shutil.which("gog"):
            gmail_accounts = os.environ.get("NERVE_GMAIL_ACCOUNTS", "")
            if gmail_accounts:
                choices.gmail_sync = True
                choices.gmail_accounts = [a.strip() for a in gmail_accounts.split(",") if a.strip()]
        tg_api_id = os.environ.get("NERVE_TELEGRAM_API_ID", "")
        tg_api_hash = os.environ.get("NERVE_TELEGRAM_API_HASH", "")
        if tg_api_id and tg_api_hash:
            try:
                choices.telegram_api_id = int(tg_api_id)
                choices.telegram_api_hash = tg_api_hash
                choices.telegram_sync = True
            except ValueError:
                pass

    # The mode's default optional crons — the set the web Mode step writes too.
    # Personal: inbox-processor and task-planner. Worker: the two skill jobs;
    # its task-planner stays off until the onboarding conversation enables it.
    if choices.mode in DEFAULT_CRONS:
        choices.enabled_crons = list(DEFAULT_CRONS[choices.mode])

    # External agents — comma-separated list ("codex,claude-code") and
    # optional conflict policy. Validated against AGENT_REGISTRY so an
    # unknown name aborts setup rather than silently dropping the agent.
    external = (os.environ.get("NERVE_EXTERNAL_AGENTS", "") or "").strip()
    if external and choices.mode == "personal":
        from nerve.external_agents.registry import AGENT_REGISTRY
        names = [n.strip() for n in external.split(",") if n.strip()]
        for n in names:
            if n not in AGENT_REGISTRY:
                raise click.ClickException(
                    f"Unknown external agent: {n!r}. Known: "
                    f"{', '.join(AGENT_REGISTRY.keys())}"
                )
        choices.external_agents = names
        choices.external_agents_conflict_policy = os.environ.get(
            "NERVE_EXTERNAL_AGENTS_CONFLICT", "backup",
        )

    # Worker task description (setup agent runs on first boot, not during init)
    if choices.mode == "worker":
        choices.task_description = os.environ.get("NERVE_TASK", "")

    wizard = SetupWizard(config_dir, inside_docker=is_docker, memory_backend=memory_backend)
    wizard.choices = choices

    click.echo("Running non-interactive setup...")
    wizard._apply()
    click.echo("Setup complete.")

    return choices


# --- Detection ---


def is_fresh_install(config_dir: Path) -> bool:
    """Check if this is a fresh install (no config.local.yaml)."""
    return not (config_dir / "config.local.yaml").exists()


# --- Utilities ---


def _wrap_text(text: str, width: int = 51) -> list[str]:
    """Simple word-wrap for box formatting."""
    words = text.split()
    lines: list[str] = []
    current = ""
    for word in words:
        if current and len(current) + 1 + len(word) > width:
            lines.append(current)
            current = word
        elif current:
            current += " " + word
        else:
            current = word
    if current:
        lines.append(current)
    return lines or [""]


def _has_config_content(path: Path) -> bool:
    """True if the file holds at least one key (not just comments/blank)."""
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return True  # can't tell — err towards keeping a backup
    return bool(data)


# --- Docker file templates ---


def _compose_host_state_dir() -> str:
    """Host-side path to mount as the container's state dir.

    Uses the raw ``NERVE_HOME`` value rather than the expanded one: compose
    expands ``~`` itself, so echoing what the operator wrote keeps the
    generated file portable between machines.
    """
    return os.environ.get(paths.NERVE_HOME_ENV, "").strip() or "~/.nerve"

_DOCKERFILE_HEAD = """
FROM python:3.13-slim

RUN apt-get update && apt-get install -y --no-install-recommends \\
    curl git gpg && rm -rf /var/lib/apt/lists/*

# Install Node.js 22 for web UI build
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \\
    && apt-get install -y nodejs && rm -rf /var/lib/apt/lists/*

# Install GitHub CLI
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \\
    | gpg --dearmor -o /usr/share/keyrings/githubcli-archive-keyring.gpg \\
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \\
    > /etc/apt/sources.list.d/github-cli.list \\
    && apt-get update && apt-get install -y gh && rm -rf /var/lib/apt/lists/*

# Install gog (Google Workspace CLI) — Go binary from GitHub releases
RUN GOG_VERSION=0.11.0 \\
    && ARCH=$(dpkg --print-architecture) \\
    && curl -fsSL "https://github.com/steipete/gogcli/releases/download/v${GOG_VERSION}/gogcli_${GOG_VERSION}_linux_${ARCH}.tar.gz" \\
    | tar xz -C /usr/local/bin gog
""" + f"""
RUN mkdir -p {_DOCKER_NERVE_HOME} {_DOCKER_WORKSPACE}

ENV NERVE_DOCKER=1
ENV NERVE_HOME={_DOCKER_NERVE_HOME}
ENV NERVE_WORKSPACE={_DOCKER_WORKSPACE}
""" + """
WORKDIR /nerve

# uv, pinned to the same version CI uses. Dependency versions come from
# uv.lock, so this image installs exactly what CI tested rather than
# re-resolving pyproject's bounds at build time.
COPY --from=ghcr.io/astral-sh/uv:0.12.0 /uv /usr/local/bin/uv

# The project environment lives OUTSIDE /nerve, which is a bind mount at
# runtime: a .venv under /nerve would be shadowed by the host's checkout, and a
# host .venv may not even be Linux-compatible. Putting it in /opt keeps the
# image's environment intact regardless of what the host mounts.
ENV UV_PROJECT_ENVIRONMENT=/opt/nerve-venv
ENV PATH=/opt/nerve-venv/bin:$PATH

# Pre-install dependencies for layer caching. Only pyproject.toml + uv.lock are
# copied, so this layer is reused until dependencies actually change.
# --no-install-project because the source tree arrives at runtime via the mount.
COPY pyproject.toml uv.lock /tmp/nerve-deps/
RUN cd /tmp/nerve-deps \\
    && uv sync --locked --no-install-project --no-dev \\
    && rm -rf /tmp/nerve-deps

"""

_DOCKERFILE_TAIL = """
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
"""


def _build_dockerfile(port: int = 8900) -> str:
    """The Dockerfile, exposing and health-checking the chosen gateway port.

    The default reproduces the previous template byte for byte.
    """
    return (
        _DOCKERFILE_HEAD
        + f"EXPOSE {int(port)}\n\n"
        + "HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \\\n"
        + f"    CMD curl -f http://localhost:{int(port)}/health || exit 1\n"
        + _DOCKERFILE_TAIL
    )


_DOCKERFILE_TEMPLATE = _build_dockerfile()

def _build_docker_compose(
    workspace_path: str = "~/nerve-workspace",
    extra_mounts: list[str] | None = None,
    *,
    port: int = 8900,
    bind: str = "lan",
) -> str:
    """Build docker-compose.yml content with host bind-mounts.

    Args:
        workspace_path: Host path for the workspace (e.g. ~/nerve-workspace).
        extra_mounts: Additional host:container mount pairs (e.g. ["~/code:/code"]).
        port: The gateway port, published on the host under the same number.
        bind: ``"localhost"`` publishes on 127.0.0.1 only; ``"lan"`` (the
            default, and the previous behaviour) on every host interface.
    """
    # Required mounts (always present)
    # ~/.nerve/claude:/root/.claude persists Claude Code's conversation
    # .jsonl files across container restarts. Without this mount the files
    # are wiped on every `docker compose down/up` and the Nerve DB's stale
    # sdk_session_id rows fail every --resume with "No conversation found"
    # exit 1. Siloed under ~/.nerve so the agent's CLI is isolated from
    # the host user's personal ~/.claude (where macOS stores OAuth tokens).
    # The host side follows NERVE_HOME too. Deriving only the container side
    # would mount the default ~/.nerve into an instance the operator had
    # deliberately relocated, giving the host and the container two different
    # databases with no indication anything was wrong.
    host_state = _compose_host_state_dir()
    volumes = [
        ".:/nerve",
        f"{host_state}:{_DOCKER_NERVE_HOME}",
        f"{host_state}/claude:/root/.claude",
        f"{workspace_path}:{_DOCKER_WORKSPACE}",
    ]

    # Optional auth mounts — only include if the host directory exists.
    # Docker would create missing dirs as root-owned empties, which
    # confuses the tools and pollutes the host filesystem.
    _optional_mounts = [
        ("~/.config/gh", "/root/.config/gh", "gh CLI auth"),
        ("~/.config/gog", "/root/.config/gog", "gog CLI auth"),
    ]
    for host_path, container_path, _label in _optional_mounts:
        expanded = os.path.expanduser(host_path)
        if os.path.isdir(expanded):
            volumes.append(f"{host_path}:{container_path}")

    if extra_mounts:
        volumes.extend(extra_mounts)

    # Build YAML by hand to keep formatting clean
    vol_lines = "\n".join(f"      - {v}" for v in volumes)

    prefix = "127.0.0.1:" if bind == "localhost" else ""
    return f"""services:
  nerve:
    build: .
    ports:
      - "{prefix}{int(port)}:{int(port)}"
    volumes:
{vol_lines}
    restart: unless-stopped
    stdin_open: true
    tty: true
    env_file:
      - path: .env
        required: false
"""

_DOCKER_ENTRYPOINT_TEMPLATE = """#!/bin/bash
set -e

cd /nerve

# Install Nerve into the image's environment from uv.lock, so a container gets
# the dependency set CI tested rather than whatever PyPI resolves today.
#   --locked:  refuse a uv.lock that no longer matches pyproject.toml, instead
#              of silently re-resolving (and rewriting the mounted checkout).
#   --inexact: leave anything the image or the user added in place; only the
#              locked set is reconciled.
# The dependency layer is already baked into the image, so in the normal case
# this only installs the project itself and returns in well under a second.
uv sync --locked --inexact

# Build web UI if not already built
if [ ! -d "web/dist" ]; then
    echo "Building web UI..."
    cd web && npm ci --quiet && npm run build && cd ..
fi

# --- Credential resolution (priority waterfall) ---
# Export credentials from config.local.yaml so tools (claude CLI, gh CLI)
# can authenticate inside Docker. macOS stores tokens in the Keychain
# which Docker can't access — the bootstrap wizard extracts them during
# `nerve init` and stores them here.

# Claude: prefer OAuth token, fall back to API key
if [ -z "$CLAUDE_CODE_OAUTH_TOKEN" ] && [ -f config.local.yaml ]; then
    _token=$(python3 -c "import yaml; print(yaml.safe_load(open('config.local.yaml')).get('claude_oauth_token',''))" 2>/dev/null)
    [ -n "$_token" ] && export CLAUDE_CODE_OAUTH_TOKEN="$_token"
fi

if [ -z "$ANTHROPIC_API_KEY" ] && [ -f config.local.yaml ]; then
    _key=$(python3 -c "import yaml; print(yaml.safe_load(open('config.local.yaml')).get('anthropic_api_key',''))" 2>/dev/null)
    [ -n "$_key" ] && export ANTHROPIC_API_KEY="$_key"
fi

# GitHub CLI auth
if [ -z "$GH_TOKEN" ] && [ -f config.local.yaml ]; then
    _gh=$(python3 -c "import yaml; print(yaml.safe_load(open('config.local.yaml')).get('github_token',''))" 2>/dev/null)
    [ -n "$_gh" ] && export GH_TOKEN="$_gh"
fi

# Ensure the persisted Claude Code state dir exists and is writable
# before any tool that touches /root/.claude runs. The bind mount in
# docker-compose creates it as a host-owned empty dir on first boot;
# we need it owned by root with 0700 so the CLI can drop its config
# file and projects/ tree there without ENOENT or EACCES.
mkdir -p /root/.claude
chmod 700 /root/.claude

# Clean up stale PID file from previous container runs. The Dockerfile sets
# NERVE_HOME; the fallback keeps this working if the entrypoint is reused
# somewhere that doesn't.
rm -f "${NERVE_HOME:-$HOME/.nerve}/nerve.pid"

# If no arguments, default to init + start
if [ $# -eq 0 ]; then
    nerve init --if-needed --non-interactive
    exec nerve start -f
else
    exec "$@"
fi
"""

_DOCKERIGNORE_TEMPLATE = """
# Python
__pycache__/
*.py[cod]
*$py.class
*.egg-info/
dist/
build/
.eggs/
*.egg
.venv/
venv/

# Node
web/node_modules/
web/dist/

# IDE
.vscode/
.idea/
*.swp
*.swo

# Runtime data
*.db
*.db-journal
*.log
*.pid

# Config (secrets)
config.local.yaml
.env

# OS
.DS_Store
Thumbs.db

# Git
.git/
"""
