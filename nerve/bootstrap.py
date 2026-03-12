"""First-run bootstrap wizard — interactive CLI that guides through initial setup.

Each step explains what the component does before asking for configuration.
All choices are collected in memory; nothing is written until the final apply step.
Ctrl+C at any point leaves the system untouched.
"""

from __future__ import annotations

import os
import secrets
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import click
import yaml

from nerve.workspace import initialize_workspace


# --- Cron definitions for the wizard ---

# Core crons are always enabled and not presented for selection.
CORE_CRONS = [
    {
        "id": "memory-maintenance",
        "schedule": "0 5 * * *",
        "description": "Daily memory cleanup — dedup, prune stale entries, improve wording",
        "session_mode": "isolated",
        "model": "",
        "prompt": (
            "You are running a daily memory maintenance job. Work completely silently — do not output any text, only think.\n\n"
            "Do the following:\n\n"
            "## Phase 1: Gather Yesterday's Data\n\n"
            "1. Use memory_records_by_date(date=yesterday, updated=true, limit=200) to get ALL records created or updated yesterday.\n"
            "2. Optionally: use conversation_history(date=yesterday) for additional event context.\n\n"
            "## Phase 2: Evaluate Each Record\n\n"
            "For each record from yesterday, evaluate and act:\n"
            "- **Exact duplicates**: Same fact already stored elsewhere → delete the worse copy\n"
            "- **Category-redundant**: Adds nothing beyond category summary → delete\n"
            "- **Stale/completed**: No longer true → delete\n"
            "- **Generic knowledge**: Textbook facts not personal to the user → delete\n"
            "- **Meta-noise**: Observations about the memory system itself → delete\n"
            "- **Improvable**: Poorly worded or could be more useful → update via memory_update\n\n"
            "## Phase 3: Category Review\n\n"
            "If yesterday's memories revealed new important context, check whether category summaries need updating.\n\n"
            "Rules:\n"
            "- Never delete entries about people, relationships, or preferences unless exact duplicates\n"
            "- Never delete actionable/pending items\n"
            "- Updating is better than deleting\n"
            "- When in doubt, keep the memory\n"
            "- Do NOT log or memorize anything about this maintenance run\n"
        ),
    },
]

# Productivity crons the user can enable/disable.
PRODUCTIVITY_CRONS = [
    {
        "id": "inbox-processor",
        "name": "Inbox Processor",
        "schedule": "*/30 * * * *",
        "description": "Polls your connected sources (email, GitHub, Telegram) every 30 minutes. Creates tasks for actionable items, memorizes important facts, and sends you notifications for urgent things.",
        "requires": "At least one sync source connected",
        "session_mode": "persistent",
        "context_rotate_hours": 24,
        "reminder_mode": True,
        "prompt": (
            "Process the sync inbox by calling poll_all_sources(consumer=\"inbox\").\n\n"
            "If there are new messages, review them and take appropriate action:\n"
            "- **Create tasks** (via task_create) for items requiring follow-up\n"
            "- **Memorize** important facts (via memorize) worth remembering\n"
            "- **Ignore** routine notifications, spam, or low-signal items\n\n"
            "Cross-source deduplication: if multiple sources report the same event, treat as ONE.\n\n"
            "**Notifications — use them!**\n"
            "- Use `notify` for urgent/high-priority items\n"
            "- Use `ask_user` when unsure\n"
            "- Do NOT notify for routine items\n\n"
            "Be selective. If no new messages, reply \"No new messages.\"\n"
        ),
    },
    {
        "id": "task-planner",
        "name": "Task Planner",
        "schedule": "0 */4 * * *",
        "description": "Every 4 hours, reviews your open tasks and proposes implementation plans. Plans go through an approval flow — nothing is executed without your OK.",
        "requires": None,
        "session_mode": "persistent",
        "context_rotate_hours": 168,
        "reminder_mode": False,
        "prompt": (
            "You are a proactive planning agent. Your job is to find a task worth working on and produce an implementation plan.\n\n"
            "1. Use task_list to browse open tasks\n"
            "2. Use plan_list to see which tasks already have plans — skip those\n"
            "3. Pick ONE task and explore the relevant codebase\n"
            "4. Call plan_propose(task_id, content) with your plan\n\n"
            "If all tasks have plans or none are actionable, say so and stop.\n\n"
            "After proposing a plan, use `notify` to alert the user.\n"
        ),
    },
    {
        "id": "skill-extractor",
        "name": "Skill Extractor",
        "schedule": "0 */12 * * *",
        "description": "Every 12 hours, analyzes your recent activity to detect repeated workflows. When it finds a pattern, it proposes a reusable skill for your review.",
        "requires": None,
        "session_mode": "persistent",
        "context_rotate_hours": 168,
        "reminder_mode": False,
        "prompt": (
            "You are a skill extraction agent. Identify repeated workflows from recent activity and propose new skills.\n\n"
            "1. Recall recent behavior patterns and events\n"
            "2. Check existing skills to avoid duplicates\n"
            "3. Look for repeated tool sequences, domain knowledge clusters, and reusable patterns\n"
            "4. For each candidate (max 2): create a task and propose a plan with the full SKILL.md\n\n"
            "If no candidates found, say so and stop.\n"
            "After proposing, use `notify` to alert the user.\n"
        ),
    },
    {
        "id": "skill-reviser",
        "name": "Skill Reviser",
        "schedule": "0 3 * * 0",
        "description": "Weekly review of existing skills — checks if instructions are still accurate, complete, and well-written. Proposes fixes through the approval flow.",
        "requires": None,
        "session_mode": "persistent",
        "context_rotate_hours": 168,
        "reminder_mode": False,
        "prompt": (
            "You are a skill revision agent. Review existing skills and propose improvements.\n\n"
            "1. Load all skills and their content\n"
            "2. Check accuracy (outdated paths, commands, URLs)\n"
            "3. Check completeness (missing steps, known gotchas)\n"
            "4. Check quality (clear descriptions, good trigger phrases)\n"
            "5. For skills needing changes (max 3): create task + propose plan with updated SKILL.md\n\n"
            "If all skills look good, say so and stop.\n"
            "After proposing, use `notify` to alert the user.\n"
        ),
    },
]

# Default memory categories for a fresh install.
# Generic enough for any user — they can customize in config.yaml later.
_PERSONAL_MEMORY_CATEGORIES = [
    {"name": "personal_info", "description": "Identity, contact details, timezone, background"},
    {"name": "preferences", "description": "Communication style, tool preferences, how things should be done"},
    {"name": "relationships", "description": "People, dynamics, contact context"},
    {"name": "work", "description": "Job, projects, PRs, code reviews, meetings"},
    {"name": "infrastructure", "description": "Servers, deployments, CI/CD, system ops"},
    {"name": "finances", "description": "Accounts, payments, subscriptions, budgets"},
    {"name": "tasks_deadlines", "description": "Active tasks, deadlines, pending follow-ups"},
    {"name": "conversations", "description": "Key things said, promises, follow-ups"},
    {"name": "agent_ops", "description": "Operational lessons, memory design, prompt tuning"},
    {"name": "people", "description": "Information and facts about people"},
]

_WORKER_MEMORY_CATEGORIES = [
    {"name": "task_domain", "description": "Domain-specific knowledge: CI systems, APIs, database schemas, repo structure"},
    {"name": "patterns", "description": "Recurring patterns: common failure modes, root causes, known flaky tests, seasonal issues"},
    {"name": "procedures", "description": "How to do things: reproduction steps, debug workflows, fix templates that worked"},
    {"name": "decisions", "description": "Past decisions and outcomes: what was tried, what worked, why approach X over Y"},
    {"name": "approvals", "description": "What got approved/rejected, approval preferences, risk thresholds"},
    {"name": "contacts", "description": "People involved: who owns what, who to notify, escalation paths"},
    {"name": "infrastructure", "description": "Systems, endpoints, service dependencies, deployment details"},
    {"name": "agent_ops", "description": "Operational lessons about the worker itself: tool gotchas, performance observations"},
]


@dataclass
class SetupChoices:
    """Collected user choices — nothing is written until apply()."""

    deployment: str = "server"  # "server" or "docker"
    mode: str = "personal"
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    use_proxy: bool = False  # Use CLIProxyAPI instead of direct API key
    workspace_path: Path = field(default_factory=lambda: Path("~/nerve-workspace"))
    timezone: str = "America/New_York"
    user_name: str = ""
    telegram_bot_token: str = ""
    password: str = ""  # plaintext during wizard, hashed at write time
    enabled_crons: list[str] = field(default_factory=list)
    # sync sources
    github_sync: bool = False
    gmail_sync: bool = False
    gmail_accounts: list[str] = field(default_factory=list)
    telegram_sync: bool = False
    telegram_api_id: int = 0
    telegram_api_hash: str = ""
    # worker-specific
    task_description: str = ""


class SetupWizard:
    """Interactive first-run setup wizard."""

    def __init__(self, config_dir: Path, inside_docker: bool = False):
        self.config_dir = config_dir
        self.choices = SetupChoices()
        self._inside_docker = inside_docker
        self._step_counter = 0
        if inside_docker:
            self.choices.deployment = "docker"

    def _next_step(self, label: str) -> str:
        """Return a formatted step header with auto-incrementing number."""
        self._step_counter += 1
        return f"Step {self._step_counter}: {label}"

    def run(self) -> SetupChoices:
        """Run the full interactive wizard. Returns choices (nothing written yet)."""
        self._welcome()
        if not self._inside_docker:
            self._step_deployment()
            if self.choices.deployment == "docker":
                self._launch_docker()
                return self.choices  # Never reached — execvp replaces process
        self._step_mode()
        self._step_api_keys()
        self._step_workspace()
        self._step_password()
        if self.choices.mode == "personal":
            self._step_identity()
            self._step_channels()
            self._step_sources()
            self._step_crons()
        else:
            self._step_worker_setup()
        self._step_review()
        self._apply()
        self._done()
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

        # Run the wizard inside the container (interactive)
        click.echo("  Starting container...\n")
        os.execvp("docker", [
            "docker", "compose",
            "-f", str(self.config_dir / "docker-compose.yml"),
            "run", "--rm",
            "--service-ports",
            "nerve",
            "nerve", "init", "--inside-docker",
        ])
        # execvp replaces this process — we never return here

    def _ensure_docker_files(self) -> None:
        """Generate Dockerfile, docker-compose.yml, entrypoint, and .dockerignore."""
        # docker-compose.yml is generated dynamically (host paths, extra mounts)
        compose_content = _build_docker_compose(
            workspace_path=str(self.choices.workspace_path),
        )

        files = {
            "Dockerfile": _DOCKERFILE_TEMPLATE,
            "docker-compose.yml": compose_content,
            "docker-entrypoint.sh": _DOCKER_ENTRYPOINT_TEMPLATE,
            ".dockerignore": _DOCKERIGNORE_TEMPLATE,
        }
        for filename, content in files.items():
            filepath = self.config_dir / filename
            if filepath.exists():
                click.echo(f"  {filename} already exists — skipping")
                continue
            filepath.write_text(content.lstrip("\n"))
            if filename == "docker-entrypoint.sh":
                try:
                    os.chmod(filepath, 0o755)
                except OSError:
                    pass
            click.echo(f"  Created {filename}")

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

    def _step_api_keys(self) -> None:
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
        click.echo()

        auth_mode = click.prompt("Choose", type=click.Choice(["1", "2"]), default="1")

        if auth_mode == "2":
            self._step_proxy_setup()
        else:
            self._step_api_key_direct()

        # OpenAI key (optional, for embeddings — applies to both modes)
        click.echo()
        click.secho(
            "Optionally, an OpenAI key enables better memory search\n"
            "(text-embedding-3-small for vector embeddings). Nerve works\n"
            "without it but recall quality improves significantly.",
            dim=True,
        )
        click.echo()
        openai_key = click.prompt("OpenAI API key (Enter to skip)", default="", hide_input=True)
        if openai_key:
            self.choices.openai_api_key = openai_key

        click.echo()
        click.secho("  ✓ API configuration complete", fg="green")
        click.echo()

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
            click.secho(
                "\n  You can authenticate later by running:\n"
                "    ~/.nerve/bin/cli-proxy-api --claude-login --no-browser \\\n"
                "      --config ~/.nerve/cli-proxy-config.yaml",
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
        default_ws = "/root/nerve-workspace" if self._inside_docker else "~/nerve-workspace"
        ws = click.prompt("Workspace path", default=default_ws)
        self.choices.workspace_path = Path(ws)
        click.echo()
        click.secho(
            "  Nerve also stores databases, logs, and session data in\n"
            "  ~/.nerve/ — this is separate from your workspace.",
            dim=True,
        )
        click.echo()

    # --- Step: Password ---

    def _step_password(self) -> None:
        click.clear()
        click.secho(self._next_step("Web UI Password"), fg="cyan", bold=True)
        click.echo()
        click.secho(
            "The web UI at localhost:8900 requires a password.\n"
            "Set one now, or press Enter to skip (dev mode — no auth).",
            dim=True,
        )
        click.echo()

        while True:
            pw = click.prompt("Password (Enter to skip)", default="", hide_input=True)
            if not pw:
                click.echo()
                click.secho("  → Skipping — running in dev mode (no password).", fg="yellow")
                click.secho("    You can set one later in config.local.yaml.", dim=True)
                break
            pw2 = click.prompt("Confirm password", hide_input=True)
            if pw == pw2:
                self.choices.password = pw
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
            token = click.prompt("  Bot token (from @BotFather)")
            self.choices.telegram_bot_token = token
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

    # --- Step: Review ---

    def _step_review(self) -> None:
        click.clear()
        click.secho("Review", fg="cyan", bold=True)
        click.echo()

        ws = str(self.choices.workspace_path)
        if self.choices.use_proxy:
            api_status = "Proxy ✓"
        else:
            api_status = "Anthropic ✓"
        if self.choices.openai_api_key:
            api_status += "  OpenAI ✓"
        else:
            api_status += "  OpenAI —"

        tg_status = "configured" if self.choices.telegram_bot_token else "not configured"

        click.secho("  ┌──────────────────────────────────────────────┐", dim=True)
        click.secho("  │            Setup Summary                     │", bold=True)
        click.secho("  ├──────────────────────────────────────────────┤", dim=True)
        click.secho(f"  │  Deploy:     {self.choices.deployment:<33}│")
        click.secho(f"  │  Mode:       {self.choices.mode:<33}│")
        click.secho(f"  │  Workspace:  {ws:<33}│")
        click.secho(f"  │  API keys:   {api_status:<33}│")
        pw_status = "set" if self.choices.password else "none (dev mode)"
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
                # Re-run the whole wizard
                self.choices = SetupChoices()
                self.run()
                raise SystemExit(0)
            else:
                click.secho("  Aborted.", fg="yellow")
                raise SystemExit(0)

    # --- Apply ---

    def _apply(self) -> None:
        click.echo()

        # 1. Create workspace from templates
        click.echo("  Creating workspace...", nl=False)
        ws_path = Path(os.path.expanduser(str(self.choices.workspace_path)))
        created = initialize_workspace(ws_path, self.choices.mode)
        click.secho(" ✓", fg="green")

        # 2. Patch USER.md with name/timezone if provided (personal mode)
        if self.choices.mode == "personal" and self.choices.user_name:
            user_md = ws_path / "USER.md"
            if user_md.exists():
                content = user_md.read_text(encoding="utf-8")
                content = content.replace("{{USER_NAME}}", self.choices.user_name)
                content = content.replace("{{TIMEZONE}}", self.choices.timezone)
                user_md.write_text(content, encoding="utf-8")

        # 3. Write TASK.md for worker mode
        if self.choices.mode == "worker" and self.choices.task_description:
            task_md = ws_path / "TASK.md"
            task_md.write_text(
                f"# Task\n\n{self.choices.task_description}\n",
                encoding="utf-8",
            )

        # 4. Write config.yaml
        click.echo("  Writing config.yaml...", nl=False)
        self._write_config_yaml()
        click.secho(" ✓", fg="green")

        # 5. Write config.local.yaml
        click.echo("  Writing config.local.yaml...", nl=False)
        self._write_config_local_yaml()
        click.secho(" ✓", fg="green")

        # 6. Create ~/.nerve directory structure
        click.echo("  Setting up ~/.nerve/...", nl=False)
        nerve_dir = Path("~/.nerve").expanduser()
        nerve_dir.mkdir(parents=True, exist_ok=True)
        (nerve_dir / "cron").mkdir(parents=True, exist_ok=True)
        click.secho(" ✓", fg="green")

        # 7. Write cron jobs
        click.echo("  Configuring cron jobs...", nl=False)
        self._write_cron_jobs()
        click.secho(" ✓", fg="green")

        # 8. Build web UI (server mode only — Docker handles this in entrypoint)
        if not self._inside_docker:
            self._build_web_ui()

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
                "    Install Node.js 18+ and run: cd web && npm ci && npm run build",
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

    def _write_config_yaml(self) -> None:
        """Write the base config.yaml."""
        ws = str(self.choices.workspace_path)
        tz = self.choices.timezone

        config: dict[str, Any] = {
            "workspace": ws,
            "timezone": tz,
            "deployment": self.choices.deployment,
            "agent": {
                "model": "claude-opus-4-6",
                "cron_model": "claude-sonnet-4-6",
                "max_turns": 50,
                "max_concurrent": 4,
                "thinking": "max",
                "effort": "max",
                "context_1m": True,
            },
            "gateway": {
                "host": "0.0.0.0",
                "port": 8900,
            },
            "quiet_start": "02:00",
            "quiet_end": "08:00",
            "memory": {
                "recall_model": "claude-sonnet-4-6",
                "memorize_model": "claude-sonnet-4-6",
                "fast_model": "claude-haiku-4-5-20251001",
                "embed_model": "text-embedding-3-small",
                "categories": (
                    _PERSONAL_MEMORY_CATEGORIES if self.choices.mode == "personal"
                    else _WORKER_MEMORY_CATEGORIES
                ),
            },
            "cron": {
                "system_file": "~/.nerve/cron/system.yaml",
                "jobs_file": "~/.nerve/cron/jobs.yaml",
            },
            "sessions": {
                "sticky_period_minutes": 120,
                "archive_after_days": 30,
                "max_sessions": 500,
                "memorize_interval_minutes": 30,
            },
        }

        if self.choices.mode == "personal":
            config["telegram"] = {
                "enabled": bool(self.choices.telegram_bot_token),
                "dm_policy": "pairing",
                "stream_mode": "partial",
            }
            config["sync"] = {
                "telegram": {"enabled": self.choices.telegram_sync},
                "gmail": {
                    "enabled": self.choices.gmail_sync,
                    "accounts": self.choices.gmail_accounts,
                },
                "github": {"enabled": self.choices.github_sync},
                "github_events": {"enabled": self.choices.github_sync},
            }

        if self.choices.use_proxy:
            config["proxy"] = {
                "enabled": True,
                "port": 8317,
            }

        if self.choices.deployment == "docker":
            config["docker"] = {
                "extra_mounts": [],  # e.g. ["~/code:/code", "~/projects:/projects"]
            }

        config_path = self.config_dir / "config.yaml"
        with open(config_path, "w") as f:
            f.write("# Nerve — Configuration\n")
            f.write("# Edit this file to customize Nerve's behavior.\n")
            f.write("# Secrets (API keys, tokens) go in config.local.yaml.\n\n")
            yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

    def _write_config_local_yaml(self) -> None:
        """Write config.local.yaml with secrets."""
        local: dict[str, Any] = {}

        if self.choices.anthropic_api_key:
            local["anthropic_api_key"] = self.choices.anthropic_api_key

        if self.choices.openai_api_key:
            local["openai_api_key"] = self.choices.openai_api_key

        if self.choices.telegram_bot_token:
            local["telegram"] = {
                "bot_token": self.choices.telegram_bot_token,
            }

        # Sync credentials (secrets — go in local config)
        if self.choices.telegram_sync and self.choices.telegram_api_id:
            local.setdefault("sync", {})["telegram"] = {
                "api_id": self.choices.telegram_api_id,
                "api_hash": self.choices.telegram_api_hash,
            }

        # Auth: JWT secret + optional password hash
        auth: dict[str, str] = {
            "jwt_secret": secrets.token_hex(32),
        }
        if self.choices.password:
            import bcrypt
            hashed = bcrypt.hashpw(
                self.choices.password.encode("utf-8"),
                bcrypt.gensalt(),
            ).decode("utf-8")
            auth["password_hash"] = hashed
        local["auth"] = auth

        local_path = self.config_dir / "config.local.yaml"
        with open(local_path, "w") as f:
            f.write("# Nerve — Secrets (gitignored)\n")
            f.write("# API keys, tokens, and other sensitive configuration.\n\n")
            yaml.safe_dump(local, f, default_flow_style=False, sort_keys=False)

        # Set restrictive permissions on the secrets file
        try:
            os.chmod(local_path, 0o600)
        except OSError:
            pass  # Best-effort on platforms that don't support chmod

    def _write_cron_jobs(self) -> None:
        """Write system crons to system.yaml and scaffold jobs.yaml for user crons."""
        jobs: list[dict[str, Any]] = []

        # Core crons (always enabled)
        for cron in CORE_CRONS:
            jobs.append({
                "id": cron["id"],
                "schedule": cron["schedule"],
                "prompt": cron["prompt"],
                "description": cron["description"],
                "model": cron.get("model", ""),
                "session_mode": cron.get("session_mode", "isolated"),
                "enabled": True,
            })

        if self.choices.mode == "personal":
            # Productivity crons (personal mode)
            for cron in PRODUCTIVITY_CRONS:
                enabled = cron["id"] in self.choices.enabled_crons
                job: dict[str, Any] = {
                    "id": cron["id"],
                    "schedule": cron["schedule"],
                    "prompt": cron["prompt"],
                    "description": cron["description"],
                    "model": cron.get("model", ""),
                    "session_mode": cron.get("session_mode", "isolated"),
                    "enabled": enabled,
                }
                if cron.get("context_rotate_hours"):
                    job["context_rotate_hours"] = cron["context_rotate_hours"]
                if cron.get("reminder_mode"):
                    job["reminder_mode"] = cron["reminder_mode"]
                jobs.append(job)
        # Worker mode: no productivity crons — the worker-setup onboarding
        # session runs from AgentEngine.initialize() on first boot instead.

        # Write system crons (managed by nerve init, safe to regenerate)
        system_file = Path("~/.nerve/cron/system.yaml").expanduser()
        system_file.parent.mkdir(parents=True, exist_ok=True)

        with open(system_file, "w") as f:
            f.write("# Nerve — System Cron Jobs\n")
            f.write("# Managed by 'nerve init'. Safe to re-generate.\n")
            f.write("# To add custom crons, use jobs.yaml instead.\n\n")
            yaml.safe_dump({"jobs": jobs}, f, default_flow_style=False, sort_keys=False)

        # Create empty jobs.yaml scaffold if it doesn't exist
        jobs_file = Path("~/.nerve/cron/jobs.yaml").expanduser()
        if not jobs_file.exists():
            with open(jobs_file, "w") as f:
                f.write("# Nerve — Custom Cron Jobs\n")
                f.write("# Add your own cron jobs here. Nerve will never overwrite this file.\n")
                f.write("# Format is the same as system.yaml — see it for examples.\n\n")
                f.write("jobs: []\n")

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
        ws = os.path.expanduser(str(self.choices.workspace_path))
        click.secho(f"  Your workspace: {ws}", bold=True)
        click.echo("    Edit SOUL.md to customize Nerve's personality")
        click.echo("    Edit USER.md to tell Nerve about yourself")
        click.echo()
        click.secho(
            "  Tip: Nerve learns from every conversation. The more\n"
            "  you interact, the more useful it becomes.",
            dim=True,
        )
        click.echo()


# --- Non-interactive mode ---


def run_non_interactive(config_dir: Path) -> SetupChoices:
    """Non-interactive setup using environment variables. For Docker."""
    choices = SetupChoices()

    # API auth: either API key or proxy mode
    use_proxy = os.environ.get("NERVE_USE_PROXY", "") == "1"
    choices.use_proxy = use_proxy

    if use_proxy:
        # Proxy mode — API key not required.
        choices.anthropic_api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    else:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise click.ClickException(
                "ANTHROPIC_API_KEY environment variable is required for non-interactive setup "
                "(or set NERVE_USE_PROXY=1 to use CLIProxyAPI instead)"
            )
        choices.anthropic_api_key = api_key

    # Auto-detect Docker via env var
    is_docker = os.environ.get("NERVE_DOCKER", "") == "1"
    choices.deployment = "docker" if is_docker else "server"

    # Optional
    choices.mode = os.environ.get("NERVE_MODE", "personal")
    choices.openai_api_key = os.environ.get("OPENAI_API_KEY", "")
    default_ws = "/root/nerve-workspace" if is_docker else "~/nerve-workspace"
    choices.workspace_path = Path(os.environ.get("NERVE_WORKSPACE", default_ws))
    choices.timezone = os.environ.get("NERVE_TIMEZONE", "America/New_York")
    choices.telegram_bot_token = os.environ.get("NERVE_TELEGRAM_BOT_TOKEN", "")
    choices.password = os.environ.get("NERVE_PASSWORD", "")

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

    # In non-interactive personal mode, enable all productivity crons by default
    if choices.mode == "personal":
        choices.enabled_crons = ["inbox-processor", "task-planner"]
    # Worker mode: no productivity crons by default

    # Worker task description (setup agent runs on first boot, not during init)
    if choices.mode == "worker":
        choices.task_description = os.environ.get("NERVE_TASK", "")

    wizard = SetupWizard(config_dir, inside_docker=is_docker)
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


# --- Docker file templates ---

_DOCKERFILE_TEMPLATE = """
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

RUN mkdir -p /root/.nerve /root/nerve-workspace

ENV NERVE_DOCKER=1

WORKDIR /nerve

# Pre-install Python dependencies for caching
COPY pyproject.toml /tmp/pyproject.toml
RUN python3 -c "import tomllib,pathlib; pathlib.Path('/tmp/requirements.txt').write_text('\\n'.join(tomllib.load(open('/tmp/pyproject.toml','rb'))['project']['dependencies']))" \\
    && pip install --no-cache-dir -r /tmp/requirements.txt \\
    && rm /tmp/pyproject.toml /tmp/requirements.txt

EXPOSE 8900

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \\
    CMD curl -f http://localhost:8900/health || exit 1

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
"""

def _build_docker_compose(
    workspace_path: str = "~/nerve-workspace",
    extra_mounts: list[str] | None = None,
) -> str:
    """Build docker-compose.yml content with host bind-mounts.

    Args:
        workspace_path: Host path for the workspace (e.g. ~/nerve-workspace).
        extra_mounts: Additional host:container mount pairs (e.g. ["~/code:/code"]).
    """
    # Required mounts (always present)
    volumes = [
        ".:/nerve",
        "~/.nerve:/root/.nerve",
        f"{workspace_path}:/root/nerve-workspace",
    ]

    # Optional auth mounts — only include if the host directory exists.
    # Docker would create missing dirs as root-owned empties, which
    # confuses the tools and pollutes the host filesystem.
    # Note: ~/.claude is NOT mounted — macOS stores OAuth tokens in the
    # system Keychain, not on disk. The entrypoint exports ANTHROPIC_API_KEY
    # from config.local.yaml instead, which the claude CLI picks up.
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

    return f"""services:
  nerve:
    build: .
    ports:
      - "8900:8900"
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

# Install the package in editable mode (fast if already installed)
pip install -e . --quiet 2>/dev/null

# Build web UI if not already built
if [ ! -d "web/dist" ]; then
    echo "Building web UI..."
    cd web && npm ci --quiet && npm run build && cd ..
fi

# Export API keys from config.local.yaml so the claude CLI (Agent SDK)
# can authenticate without OAuth. macOS stores OAuth tokens in the
# system Keychain which Docker can't access.
if [ -z "$ANTHROPIC_API_KEY" ] && [ -f config.local.yaml ]; then
    _key=$(python3 -c "import yaml; print(yaml.safe_load(open('config.local.yaml')).get('anthropic_api_key',''))" 2>/dev/null)
    [ -n "$_key" ] && export ANTHROPIC_API_KEY="$_key"
fi

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
