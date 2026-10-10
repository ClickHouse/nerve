"""What setup writes, separated from who asked for it.

``nerve init`` collects answers at a terminal; the web wizard collects them in
a browser. Both have to produce the same configuration, and two independent
implementations of "setup" drift — which in setup means installs that cannot
be reproduced. So the decisions live in one data carrier (:class:`SetupChoices`)
and the writing lives here, callable from the CLI wizard and from an HTTP
handler alike.

Three layers, and which one an answer belongs to is the whole design:

* ``config.yaml`` — machine-local, non-secret. This box's workspace path,
  deployment style, local credential *handles*.
* ``<workspace>/config/settings.yaml`` — git-tracked and portable. Shared
  behaviour: timezone, models, gateway host/port, which sources sync.
* ``config.local.yaml`` — machine-local secrets, owner-only from its first
  byte (:func:`nerve.paths.write_private_text`, which refuses rather than
  writing a file other users could read).

A key appears in exactly one of the first two. ``config.yaml`` shadows
``settings.yaml``, so writing a portable value to both would make the tracked
copy dead weight — edit it and nothing happens.

**Nothing in this module prints.** The installer's console output stayed in
:mod:`nerve.bootstrap`, which renders the outcome objects returned here; a
writer that echoed would either go to a daemon's stdout or have to grow a
"quiet" flag, and both are worse than returning what happened.

Three entry shapes:

* ``write_*`` — what ``nerve init`` does: this run's answers own the file.
* ``merge_*`` — what the web wizard does: one step's keys are merged into
  whatever is already on disk. A running install's ``config.local.yaml``
  already holds the signing secret every live session is signed with, so the
  wizard must never regenerate the file (see :func:`merge_private_paths`).
* ``apply_*`` — workspace files (mode templates, ``TASK.md``, the profile
  lines in ``USER.md``/``IDENTITY.md``), written only where they are still
  pristine.
"""

from __future__ import annotations

import copy
import dataclasses
import os
import secrets
import shutil
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from nerve import paths
from nerve.config import _expand_path, _interpolate_str, workspace_settings_file
from nerve.workspace import (
    initialize_workspace,
    install_bundled_skills,
    install_config_scaffold,
)

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
        "display_name": "Inbox processor",
        "schedule": "*/30 * * * *",
        "description": "Polls your connected sources (email, GitHub, Telegram) every 30 minutes. Creates tasks for actionable items, memorizes important facts, and sends you notifications for urgent things.",
        "requires": "At least one sync source connected",
        "session_mode": "persistent",
        "context_rotate_hours": 24,
        "reminder_mode": True,
        # Skip idle polls: only wake when the inbox actually has new messages.
        # No "sources" list → any source the "inbox" consumer tracks.
        "run_if": [{"type": "messages", "consumer": "inbox"}],
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
        "display_name": "Task planner",
        "schedule": "0 */4 * * *",
        "description": "Every 4 hours, reviews your open tasks and proposes implementation plans. Plans go through an approval flow — nothing is executed without your OK.",
        "requires": None,
        "session_mode": "persistent",
        "context_rotate_hours": 168,
        "reminder_mode": False,
        # Only fire when there is actually something to plan.
        "run_if": [{"type": "tasks", "status": "pending"}],
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
        "display_name": "Skill extractor",
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
        "display_name": "Skill reviser",
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

# The optional crons each mode switches on when nobody chose otherwise: what
# ``nerve init --non-interactive`` writes, and what the web Mode step writes
# into a fresh ``system.yaml``.
DEFAULT_CRONS: dict[str, list[str]] = {
    "personal": ["inbox-processor", "task-planner"],
    "worker": ["skill-reviser", "skill-extractor", "task-planner"],
}

# The optional crons a worker install carries at all.
_WORKER_CRONS = ("skill-reviser", "skill-extractor", "task-planner")

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

# Container-side locations, referenced by the Dockerfile, the compose mounts and
# the agent-facing docs below so they can't drift apart. The image sets both as
# environment variables rather than relying on the root user's $HOME happening
# to be /root — see nerve.paths.nerve_home() for how NERVE_HOME is consumed.
_DOCKER_NERVE_HOME = "/root/.nerve"
_DOCKER_WORKSPACE = "/root/nerve-workspace"

_DOCKER_TOOLS_SECTION = f"""
## Docker Environment

You are running inside a Docker container. Key paths:

| Path | Contents | Writable | Notes |
|------|----------|----------|-------|
| `/nerve` | Nerve source code **and config** | ✓ (bind mount) | `pyproject.toml`, `nerve/` package, `web/` — the full repo. The config directory resolves to the working directory, so `config.yaml` and `config.local.yaml` live here. |
| `{_DOCKER_NERVE_HOME}` | Machine-local state (`$NERVE_HOME`) | ✓ (bind mount) | Databases, logs, PID, caches. Never synced. |
| `{_DOCKER_WORKSPACE}` | Your workspace (`$NERVE_WORKSPACE`) | ✓ (bind mount) | AGENTS.md, SOUL.md, MEMORY.md, skills, and `config/` — where you live. |
| `{_DOCKER_WORKSPACE}/config` | Shareable config | ✓ (bind mount) | `settings.yaml` and `cron/` — the git-syncable layer. |

### Working with Nerve source

- The Nerve package is installed in editable mode, from `uv.lock`, by the
  entrypoint (`uv sync --locked --inexact`). Dependency versions therefore match
  what CI tested; rebuilding an unchanged commit gets the same set.
- The Python environment is `/opt/nerve-venv`, deliberately outside `/nerve` —
  that path is a bind mount, so a `.venv` under it would be shadowed by the
  host's checkout.
- After modifying Nerve source code, changes take effect immediately for Python.
- After changing dependencies in `pyproject.toml`, run `uv lock` on the host and
  restart the container; the entrypoint refuses a lock that no longer matches.
- If you modify the web UI (`/nerve/web/`), rebuild with: `cd /nerve/web && npm run build`
- Config files live in `/nerve/`, NOT in `{_DOCKER_NERVE_HOME}/` — the config
  directory is the working directory the daemon was started from.
- Cron jobs live in `{_DOCKER_WORKSPACE}/config/cron/` (`jobs.yaml`, `system.yaml`,
  `gates/`), not under `$NERVE_HOME` — they are part of the syncable config.

### Credentials

Docker can't access the host's macOS Keychain. Credentials are extracted during
`nerve init` and stored in `/nerve/config.local.yaml` (alongside `config.yaml`).
The entrypoint reads that file and exports them as environment variables
(`CLAUDE_CODE_OAUTH_TOKEN`, `ANTHROPIC_API_KEY`, `GH_TOKEN`).
"""


@dataclass
class SetupChoices:
    """Collected user choices — nothing is written until apply()."""

    deployment: str = "server"  # "server" or "docker"
    mode: str = "personal"
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    use_proxy: bool = False  # Use CLIProxyAPI instead of direct API key
    # Provider
    provider_type: str = "anthropic"  # "anthropic" | "bedrock"
    aws_region: str = ""
    aws_profile: str = ""
    workspace_path: Path = field(default_factory=lambda: Path("~/nerve-workspace"))
    timezone: str = "America/New_York"
    user_name: str = ""
    telegram_bot_token: str = ""
    # Telegram user IDs authorized to DM the bot. Empty = pair after setup
    # via `nerve pair` + /pair <code> in a private chat.
    telegram_allowed_users: list[int] = field(default_factory=list)
    # ``None`` derives the switch from the token (what `nerve init` does); the
    # web Channels step sets it explicitly so a token can be kept while the
    # bot is switched off, and so a worker install can carry the key at all.
    telegram_enabled: bool | None = None
    password: str = ""  # plaintext during wizard, hashed at write time
    # The operator chose a passwordless installation. Without a password and
    # without this, setup stays incomplete until the setup-token claim.
    passwordless: bool = False
    enabled_crons: list[str] = field(default_factory=list)
    # sync sources
    github_sync: bool = False
    gmail_sync: bool = False
    gmail_accounts: list[str] = field(default_factory=list)
    telegram_sync: bool = False
    telegram_api_id: int = 0
    telegram_api_hash: str = ""
    # docker credential forwarding
    claude_oauth_token: str = ""  # OAuth token (from keychain/credentials.json/manual)
    github_token: str = ""  # GitHub PAT (from gh auth token/env)
    # worker-specific
    task_description: str = ""
    # external agents (Codex, Claude Code, ...)
    external_agents: list[str] = field(default_factory=list)
    external_agents_conflict_policy: str = "backup"   # "backup" | "skip" | "merge"
    external_agents_mcp_url: str = ""                  # e.g. "https://localhost:8900/mcp/v1/"
    external_agents_token: str = ""                    # bearer JWT (one-shot at bootstrap)
    # Model picker (web Provider step). Empty keeps the installer's defaults.
    agent_model: str = ""
    agent_effort: str = ""
    # Quiet hours (web Profile step). Empty keeps the installer's defaults.
    quiet_start: str = ""
    quiet_end: str = ""
    # Slack (web Channels step). ``None`` leaves the key out entirely.
    slack_bot_token: str = ""
    slack_app_token: str = ""
    slack_enabled: bool | None = None
    slack_allow_users: list[str] | None = None
    slack_allow_direct_messages: bool | None = None
    slack_allow_channels: list[str] | None = None
    # Where the gateway listens. The installer's defaults; the slim `nerve
    # init` asks.
    gateway_host: str = "0.0.0.0"
    gateway_port: int = 8900


def bedrock_geo_prefix(region: str) -> str:
    """Map an AWS region to its Bedrock cross-region inference-profile prefix.

    Bedrock inference profiles are geography-scoped: ``us.``, ``eu.`` and
    ``apac.``. Writing a ``us.`` model ID for an ``eu-*`` region yields an
    instant 400 ("The provided model identifier is invalid").
    """
    region = (region or "").lower()
    if region.startswith("eu-"):
        return "eu"
    if region.startswith(("ap-", "au-")):
        return "apac"
    # us-*, ca-*, sa-*, mx-* and unknown regions route via the US profile
    return "us"


def expand_workspace(raw: str) -> Path:
    """Expand a workspace path exactly as nerve.config resolves it.

    Delegates to ``_expand_path`` rather than repeating it. The previous
    implementation reversed the order (``expanduser`` before ``expandvars``) and
    did not strip, and both mattered: ``expanduser`` only expands a *leading*
    ``~``, so a value carrying leading whitespace — a ``NERVE_WORKSPACE`` with a
    trailing newline, say — was not expanded at all, and the wizard wrote
    settings.yaml to a directory the loader never reads.

    Blank means unset, as it does for every other path setting, so it falls back
    to the same default the loader uses.
    """
    if "${" in raw:
        raw = _interpolate_str(raw, [])
    return _expand_path(raw) or paths.default_workspace()


def workspace_dir(choices: SetupChoices) -> Path:
    """The workspace path, expanded the same way the config loader does.

    nerve/config.py resolves `workspace` with ${VAR} interpolation *and*
    expandvars/expanduser. Expanding differently here would put
    settings.yaml somewhere the loader never looks — the wizard would
    report success and every portable setting would be silently lost.
    """
    return expand_workspace(str(choices.workspace_path))


_MISSING = object()

# Regenerated verbatim on every write; anything below it is user content, so
# the comment-loss warning only fires for comments the operator added.
_SETTINGS_HEADER = """\
# Nerve — Shared configuration
# Git-tracked and portable: this is the layer that syncs between machines and
# the one lockdown mode trusts. Machine-specific values belong in config.yaml,
# which overrides this file. Secrets belong in config.local.yaml or behind an
# ${ENV_VAR} reference — never here.
#
# `nerve init` owns the keys it generates and rewrites them on re-run. Keys it
# does not generate are left alone.

"""
_SETTINGS_HEADER_LINES = _SETTINGS_HEADER.count("\n")

CONFIG_YAML_HEADER = (
    "# Nerve — Machine-local configuration\n"
    "# Settings specific to this box: workspace location,\n"
    "# bind address, deployment style, local credentials handles.\n"
    "# Shared behaviour lives in <workspace>/config/settings.yaml,\n"
    "# which this file overrides. Secrets go in config.local.yaml.\n\n"
)

_CONFIG_LOCAL_HEADER = (
    "# Nerve — Secrets (gitignored)\n"
    "# API keys, tokens, and other sensitive configuration.\n\n"
)


def leaf_paths(d: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten a nested dict to ``{"a.b": value}``. Lists are leaves."""
    out: dict[str, Any] = {}
    for key, value in d.items():
        path = f"{prefix}{key}"
        if isinstance(value, dict):
            out.update(leaf_paths(value, f"{path}."))
        else:
            out[path] = value
    return out


def set_leaf(d: dict[str, Any], path: str, value: Any) -> None:
    *parents, last = path.split(".")
    node = d
    for part in parents:
        child = node.get(part)
        if not isinstance(child, dict):
            child = {}
            node[part] = child
        node = child
    node[last] = value


def del_leaf(d: dict[str, Any], path: str) -> bool:
    """Remove ``path``, pruning any dict it leaves empty. True if removed."""
    *parents, last = path.split(".")
    chain: list[tuple[dict[str, Any], str]] = []
    node = d
    for part in parents:
        child = node.get(part)
        if not isinstance(child, dict):
            return False
        chain.append((node, part))
        node = child
    if last not in node:
        return False
    del node[last]
    for parent, key in reversed(chain):
        if not parent[key]:
            del parent[key]
    return True


# Keys that describe *this box* and must never travel with the workspace
# repo. Everything else the wizard decides is shared behaviour and belongs
# in the tracked settings layer, so `nerve config sync` and lockdown mean
# something on a default install instead of being no-ops.
#
# A key must appear in exactly one of the two dicts below. config.yaml
# shadows settings.yaml, so writing a portable value to both would make
# the tracked copy dead weight -- edit it and nothing happens.


def build_config_layers(
    choices: SetupChoices,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    """Split the wizard's answers into (machine-local, portable, shadowed).

    ``shadowed`` lists dotted paths this run must *delete* from a
    pre-existing settings.yaml rather than merely omit. The tracked file is
    merge-preserving, so a key the wizard stops emitting would otherwise
    remain at its old value. The case this covers is switching an install
    away from Bedrock: ``provider.aws_region`` has to be removed, or the
    tracked file keeps naming a region the box no longer uses.
    """
    machine: dict[str, Any] = {
        "workspace": str(choices.workspace_path),
        "deployment": choices.deployment,
    }
    shadowed: list[str] = []
    portable: dict[str, Any] = {
        "timezone": choices.timezone,
        # Shared, not machine-local: these describe the deployment, and a
        # fleet needs one place to set them. The wizard only ever wrote the
        # declared defaults here, so while they lived in config.yaml the
        # tracked layer could not state them at all. A box needing a
        # different port still overrides in config.yaml.
        #
        # host/port only. gateway.ssl.cert and .key are local filesystem
        # paths and stay machine-local.
        "gateway": {
            "host": choices.gateway_host,
            "port": int(choices.gateway_port),
        },
        "agent": {
            "model": "claude-opus-5",
            "cron_model": "claude-sonnet-4-6",
            "max_turns": 50,
            "max_concurrent": 32,
            "thinking": "max",
            "effort": "max",
            "context_1m": True,
        },
        "quiet_start": choices.quiet_start or "02:00",
        "quiet_end": choices.quiet_end or "08:00",
        "memory": {
            "recall_model": "claude-sonnet-4-6",
            "memorize_model": "claude-sonnet-4-6",
            "fast_model": "claude-haiku-4-5-20251001",
            "embed_model": "text-embedding-3-small",
            "categories": (
                _PERSONAL_MEMORY_CATEGORIES if choices.mode == "personal"
                else _WORKER_MEMORY_CATEGORIES
            ),
        },
        # Cron config lives in workspace/config/cron (git-syncable); the
        # loader resolves it from the workspace automatically, so no explicit
        # cron paths are written here.
        "sessions": {
            "sticky_period_minutes": 120,
            "archive_after_days": 30,
            "max_sessions": 500,
            "memorize_interval_minutes": 30,
        },
    }

    if choices.mode == "personal":
        # dm_policy/stream_mode are shared behaviour; `enabled` is not --
        # it is derived from whether *this* machine was given a bot token,
        # and the token itself lives in config.local.yaml.
        machine["telegram"] = {"enabled": bool(choices.telegram_bot_token)}
        portable["telegram"] = {
            "dm_policy": "pairing",
            "stream_mode": "partial",
        }
        portable["sync"] = {
            "telegram": {"enabled": choices.telegram_sync},
            "gmail": {"enabled": choices.gmail_sync},
            "github": {"enabled": choices.github_sync},
            "github_events": {"enabled": choices.github_sync},
            # Disabled by default — requires an explicit list of repos to watch.
            "github_repos": {"enabled": False, "repos": []},
        }
        # Which sources to sync is shared policy; *whose* mailboxes is
        # not. These are personal addresses in a file the docs tell you to
        # commit, and they are per-person rather than per-team.
        machine.setdefault("sync", {})["gmail"] = {
            "accounts": choices.gmail_accounts
        }
    # The web Channels step states the switch outright, in either mode.
    if choices.telegram_enabled is not None:
        machine.setdefault("telegram", {})["enabled"] = bool(choices.telegram_enabled)

    # Provider and region are shared; only the AWS profile is a local
    # credential handle. While the whole block was machine-local, a locked
    # box never read it and fell back to the declared default, becoming an
    # `anthropic` instance and then failing for a missing API key that lived
    # in config.local.yaml, which lockdown also ignores.
    portable["provider"] = {"type": choices.provider_type}
    if choices.aws_profile:
        machine["provider"] = {"aws_profile": choices.aws_profile}
    if choices.provider_type == "bedrock":
        portable["provider"]["aws_region"] = choices.aws_region
        # The prefix is geography-scoped (us./eu./apac.) and must match the
        # configured region or every call 400s. Since the region is now in
        # the tracked layer, these belong there too.
        geo = bedrock_geo_prefix(choices.aws_region)
        for section, key, value in (
            ("agent", "model", f"{geo}.anthropic.claude-opus-5"),
            ("agent", "cron_model", f"{geo}.anthropic.claude-sonnet-4-6"),
            ("agent", "title_model", f"{geo}.anthropic.claude-haiku-4-5-20251001-v1:0"),
            ("memory", "recall_model", f"{geo}.anthropic.claude-sonnet-4-6"),
            ("memory", "memorize_model", f"{geo}.anthropic.claude-sonnet-4-6"),
            ("memory", "fast_model", f"{geo}.anthropic.claude-haiku-4-5-20251001-v1:0"),
        ):
            portable[section][key] = value
    else:
        # Switching away from Bedrock. The non-prefixed model names in the
        # base dict above already overwrite the geo-prefixed ones, so only
        # the region needs deleting.
        shadowed.append("provider.aws_region")

    # The model picker's answer, applied after the Bedrock block so a chosen
    # (already geo-prefixed) id is what lands rather than the installer's.
    if choices.agent_model:
        portable["agent"]["model"] = choices.agent_model
    if choices.agent_effort:
        portable["agent"]["effort"] = choices.agent_effort

    # Slack: the switch is this box's, the grants are shared policy.
    if choices.slack_enabled is not None:
        machine["slack"] = {"enabled": bool(choices.slack_enabled)}
    slack_portable: dict[str, Any] = {}
    if choices.slack_allow_users is not None:
        slack_portable["allow_users"] = list(choices.slack_allow_users)
    if choices.slack_allow_direct_messages is not None:
        slack_portable["allow_direct_messages"] = bool(choices.slack_allow_direct_messages)
    if choices.slack_allow_channels is not None:
        slack_portable["allow_channels"] = list(choices.slack_allow_channels)
    if slack_portable:
        portable["slack"] = slack_portable

    if choices.use_proxy:
        # A local helper process on a local port.
        machine["proxy"] = {"enabled": True, "port": 8317}

    if choices.deployment == "docker":
        machine["docker"] = {
            "extra_mounts": [],  # e.g. ["~/code:/code", "~/projects:/projects"]
        }

    return machine, portable, shadowed


def build_config_local(choices: SetupChoices) -> dict[str, Any]:
    """The secrets ``config.local.yaml`` carries, from this run's answers.

    Everything except ``auth``: the signing secret is generated by
    :func:`write_config_local_yaml` (a fresh install) and must never be
    regenerated afterwards, since every live session is signed with the one
    already on disk.
    """
    local: dict[str, Any] = {}

    if choices.anthropic_api_key:
        local["anthropic_api_key"] = choices.anthropic_api_key

    if choices.claude_oauth_token:
        local["claude_oauth_token"] = choices.claude_oauth_token

    if choices.github_token:
        local["github_token"] = choices.github_token

    if choices.openai_api_key:
        local["openai_api_key"] = choices.openai_api_key

    if choices.telegram_bot_token:
        local["telegram"] = {
            "bot_token": choices.telegram_bot_token,
        }
        if choices.telegram_allowed_users:
            local["telegram"]["allowed_users"] = list(
                choices.telegram_allowed_users
            )

    if choices.slack_bot_token or choices.slack_app_token:
        slack: dict[str, Any] = {}
        if choices.slack_bot_token:
            slack["bot_token"] = choices.slack_bot_token
        if choices.slack_app_token:
            slack["app_token"] = choices.slack_app_token
        local["slack"] = slack

    # Sync credentials (secrets — go in local config).
    #
    # Keyed off the credential, not off the switch. Whether the source is
    # *enabled* is a separate value in a separate layer, and dropping a
    # credential because the switch is off means somebody who supplies one
    # before turning the source on is told it was saved and finds nothing
    # there. `nerve init` never collects one without the switch, so its output
    # is unchanged — the goldens say so.
    if choices.telegram_api_id:
        local.setdefault("sync", {})["telegram"] = {
            "api_id": choices.telegram_api_id,
            "api_hash": choices.telegram_api_hash,
        }
    elif choices.telegram_api_hash:
        # A hash supplied before its id (the web step saves the two fields
        # independently): kept, so the person is not told it was saved and
        # finds nothing there. `nerve init` never collects one alone.
        local.setdefault("sync", {})["telegram"] = {"api_hash": choices.telegram_api_hash}

    return local


def write_config_yaml(choices: SetupChoices, config_dir: Path) -> Path:
    """Write the machine-local base config.yaml.

    Published atomically (temporary beside the destination, then renamed), so
    a failure part way never leaves a truncated ``config.yaml`` — the bytes
    are the same as before; only the way they land changed.
    """
    machine, _portable, _shadowed = build_config_layers(choices)
    config_path = config_dir / "config.yaml"
    # encoding is pinned on every config read and write in this module, to
    # match nerve.config, which pins it on the way in. Under an ASCII default
    # encoding an unpinned open() raises on the em-dash in these headers, at
    # the first write, before any user value is involved. A user value cannot
    # trigger it: safe_dump escapes non-ASCII to \xNN, so the dumped body is
    # always ASCII.
    publish_text(
        config_path,
        CONFIG_YAML_HEADER
        + yaml.safe_dump(machine, default_flow_style=False, sort_keys=False),
    )
    return config_path


@dataclass(frozen=True)
class SettingsOutcome:
    """What a write to the tracked ``settings.yaml`` did.

    Returned rather than printed: the installer renders it at a terminal and
    the web wizard puts it in a response. ``status`` is one of ``written``,
    ``unchanged``, ``invalid_yaml`` or ``not_a_mapping`` — the last two mean
    the file was left exactly as it was.
    """

    status: str
    path: Path
    added: tuple[str, ...] = ()
    changed: tuple[str, ...] = ()
    removed: tuple[str, ...] = ()
    # A file that already had comments below the generated header: safe_dump
    # cannot round-trip them, and this file is meant to be read by other
    # people in a pull request.
    comments_lost: bool = False
    detail: str = ""

    @property
    def wrote(self) -> bool:
        return self.status == "written"


def write_workspace_settings(choices: SetupChoices) -> SettingsOutcome:
    """Write the portable half into ``<workspace>/config/settings.yaml``.

    Ownership model: the wizard owns the keys it generates and rewrites
    them from this run's answers, exactly as it does for config.yaml.
    Anything else in the file -- a team policy key, a hand-tuned setting
    the wizard never emits -- is preserved untouched.

    The alternative ("existing always wins") sounds safer for a file that
    may be shared through git, but it means re-running init after changing
    an answer silently does nothing: the wizard prompts for a timezone,
    prints a tick, and discards the answer because the key already exists.
    Overwriting is recoverable -- there is a .bak, and the file is in a
    repo where the diff is visible before anyone commits it.
    """
    _machine, portable, shadowed = build_config_layers(choices)
    return _merge_settings(
        workspace_settings_file(workspace_dir(choices)),
        leaf_paths(portable),
        shadowed,
    )


def merge_settings_paths(
    workspace: Path, updates: dict[str, Any], shadowed: list[str] | None = None,
) -> SettingsOutcome:
    """Merge selected dotted paths into the tracked settings file.

    What the web wizard writes, and the reason it is a different entry point
    from :func:`write_workspace_settings`: a wizard step owns a handful of
    keys (a timezone, which sources sync), while the installer owns the whole
    generated set. Rewriting all of them from a step's partial answers would
    reset an established install's models and memory categories to the
    installer's defaults.
    """
    return _merge_settings(
        workspace_settings_file(workspace), dict(updates), list(shadowed or []),
    )


def merge_settings_defaults(
    workspace: Path, leaf_values: dict[str, Any],
) -> SettingsOutcome:
    """Fill the given dotted paths into the tracked settings only where absent.

    The Mode step's writer: a terminal-configured install, or one whose
    settings arrived from a config repository, keeps every value it already
    states. Only the leaves that are missing are added.
    """
    settings_path = workspace_settings_file(workspace)
    existing: dict[str, Any] = {}
    if settings_path.exists():
        try:
            loaded = yaml.safe_load(settings_path.read_text(encoding="utf-8"))
        except yaml.YAMLError as e:
            return SettingsOutcome(status="invalid_yaml", path=settings_path, detail=str(e))
        if loaded is not None and not isinstance(loaded, dict):
            return SettingsOutcome(status="not_a_mapping", path=settings_path)
        existing = loaded or {}
    present = leaf_paths(existing)
    # A present *section* also counts: ``agent: {}`` written by hand is a
    # statement, and filling leaves under it is the one case where a
    # missing leaf is wanted. Only whole-leaf absence is filled.
    missing = {path: value for path, value in leaf_values.items() if path not in present}
    if not missing:
        return SettingsOutcome(status="unchanged", path=settings_path)
    return _merge_settings(settings_path, missing, [])


def settings_problem(workspace: Path) -> str:
    """Why the tracked settings file could not be merged into, or ``""``.

    Asked *before* a step writes anything, so a request that touches more than
    one file does not publish the first and then discover the second is
    unusable — which answers with an error for work that half happened.
    """
    path = workspace_settings_file(workspace)
    if not path.exists():
        return ""
    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as e:
        return f"it is not valid YAML: {e}"
    if loaded is not None and not isinstance(loaded, dict):
        return "it is not a mapping"
    return ""


def _merge_settings(
    settings_path: Path, updates: dict[str, Any], shadowed: list[str],
) -> SettingsOutcome:
    settings_path.parent.mkdir(parents=True, exist_ok=True)

    raw = ""
    existing: dict[str, Any] = {}
    if settings_path.exists():
        raw = settings_path.read_text(encoding="utf-8")
        try:
            loaded = yaml.safe_load(raw)
        except yaml.YAMLError as e:
            return SettingsOutcome(
                status="invalid_yaml", path=settings_path, detail=str(e),
            )
        if loaded is not None and not isinstance(loaded, dict):
            return SettingsOutcome(status="not_a_mapping", path=settings_path)
        existing = loaded or {}

    merged = copy.deepcopy(existing)
    # Diff against the file as it was on disk, not against `merged` while
    # it is being mutated. The two are equivalent -- no portable leaf path
    # is a prefix of another, so a set_leaf can only touch descendants of
    # its own path -- but flattening once says what is meant and drops the
    # per-key re-walk.
    before_paths = leaf_paths(existing)
    added, changed, removed = [], [], []
    for path, value in updates.items():
        before = before_paths.get(path, _MISSING)
        if before is _MISSING:
            added.append(path)
        elif before != value:
            changed.append(f"{path}: {before!r} → {value!r}")
        set_leaf(merged, path, value)
    for path in shadowed:
        if del_leaf(merged, path):
            removed.append(path)

    if merged == existing:
        return SettingsOutcome(status="unchanged", path=settings_path)

    comments_lost = bool(existing) and any(
        line.lstrip().startswith("#")
        for line in raw.splitlines()[_SETTINGS_HEADER_LINES:]
    )

    # Rendered in full, then published by rename. Dumping straight into the
    # destination truncates it first, so a failure part way through — a dump
    # that raises on an unserialisable value, a full disk — leaves the tracked
    # settings of a working install as however much of the new file got
    # written. That file is the one an operator shares through git and the one
    # the mandatory restart reads.
    publish_text(settings_path, _SETTINGS_HEADER + yaml.safe_dump(
        merged, default_flow_style=False, sort_keys=False,
    ))

    return SettingsOutcome(
        status="written",
        path=settings_path,
        added=tuple(added),
        changed=tuple(changed),
        removed=tuple(removed),
        comments_lost=comments_lost,
    )


def write_config_local_yaml(choices: SetupChoices, config_dir: Path) -> Path:
    """Write config.local.yaml with secrets.

    Raises :class:`nerve.paths.InsecureFileError` — with nothing written — if
    the file cannot be created owner-only. The caller turns that into its own
    failure; the installer exits non-zero.
    """
    local = build_config_local(choices)

    # Auth: JWT secret + optional password hash
    auth: dict[str, str] = {
        "jwt_secret": secrets.token_hex(32),
    }
    if choices.password:
        import bcrypt
        hashed = bcrypt.hashpw(
            choices.password.encode("utf-8"),
            bcrypt.gensalt(),
        ).decode("utf-8")
        auth["password_hash"] = hashed
    local["auth"] = auth

    # The file holds the signing secret, the password hash and the API keys,
    # so it is created owner-only. If the filesystem cannot keep it private,
    # nothing is written and setup stops.
    local_path = config_dir / "config.local.yaml"
    paths.write_private_text(
        local_path,
        _CONFIG_LOCAL_HEADER
        + yaml.safe_dump(local, default_flow_style=False, sort_keys=False),
    )
    return local_path


def leading_comment(text: str) -> str:
    """The comment block a machine-local config file opens with.

    ``safe_dump`` cannot round-trip comments, so a read-modify-write would
    otherwise drop the header explaining what the file is for.
    """
    kept: list[str] = []
    for line in text.splitlines(keepends=True):
        if line.strip() and not line.lstrip().startswith("#"):
            break
        kept.append(line)
    return "".join(kept)


def _merged_yaml(path: Path, updates: dict[str, Any], header: str) -> str:
    """The text a merge produces: what is on disk, with these paths set.

    Shared by both writers below, so "what the merge means" is decided once
    and only "how privately it is published" differs between them.
    """
    original = path.read_text(encoding="utf-8") if path.exists() else ""
    data: dict[str, Any] = {}
    if original.strip():
        loaded = yaml.safe_load(original)
        if loaded is not None and not isinstance(loaded, dict):
            raise ValueError(f"{path} is not a mapping; refusing to rewrite it")
        data = loaded or {}

    for dotted, value in updates.items():
        if value is None:
            del_leaf(data, dotted)
        else:
            set_leaf(data, dotted, value)

    opening = leading_comment(original) if original else header
    return opening + yaml.safe_dump(data, default_flow_style=False, sort_keys=False)


def publish_text(path: Path, text: str) -> None:
    """Write ``text`` to ``path`` atomically, keeping its mode and owner.

    Temporary beside the destination, then renamed over it: a reader never
    sees half a file, and a writer that dies half way leaves the previous
    version rather than a truncated one. The destination's mode and owner are
    carried onto the temporary *before* the rename, so a container running as
    root over a bind mount does not leave the host's own user locked out of a
    file it has to read.

    For files that carry no credential. :func:`nerve.paths.write_private_text`
    is the one for files that do: it forces ``0600`` and refuses rather than
    writing something other users could read.
    """
    path = Path(path)
    existing_stat = path.stat() if path.exists() else None
    tmp = path.with_name(path.name + ".tmp")
    tmp.unlink(missing_ok=True)
    fd = os.open(
        tmp,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        # Created narrow and widened to the destination's own mode below, so
        # the window in between is never *wider* than what the file ends at.
        0o600,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", closefd=False) as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        if existing_stat is not None:
            os.fchmod(fd, stat.S_IMODE(existing_stat.st_mode))
            if hasattr(os, "fchown") and os.getuid() == 0:
                # Only root can give a file away, and only root is in the
                # position that needs to: the container writing a file the
                # host's non-root user owns.
                try:
                    os.fchown(fd, existing_stat.st_uid, existing_stat.st_gid)
                except OSError:  # pragma: no cover - best effort
                    pass
        else:
            os.fchmod(fd, 0o644 & ~_current_umask())
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    finally:
        os.close(fd)


def merge_machine_paths(
    path: Path, updates: dict[str, Any], *, header: str = "",
) -> None:
    """Merge dotted paths into a machine-local file that carries no secret.

    ``config.yaml`` holds this box's workspace path, its deployment style and
    which features are on — nothing anyone must not read, and the file an
    operator edits by hand. So it is republished **at the mode and ownership it
    already had**, not forced to ``0600``.

    That distinction is not cosmetic. In the Docker deployment the container
    runs as root over a bind-mounted checkout, so a wizard write that published
    a fresh root-owned ``0600`` inode would leave the host's own non-root
    ``nerve`` unable to read the file it needs to recognise a Docker install at
    all. Forced ``0600`` is for the file that holds credentials, and
    :func:`merge_private_paths` is where that lives.

    Atomic all the same: written to a temporary beside the destination and
    renamed over it, so a reader never sees half a file, and the destination's
    mode and owner are carried onto the temporary before the rename rather than
    applied to the published name afterwards.
    """
    publish_text(path, _merged_yaml(path, updates, header or CONFIG_YAML_HEADER))


def _current_umask() -> int:
    """The process umask, read without leaving it changed."""
    current = os.umask(0o022)
    os.umask(current)
    return current


def merge_private_paths(
    path: Path, updates: dict[str, Any], *, header: str = "",
) -> None:
    """Merge dotted paths into a machine-local file, owner-only.

    The read-modify-write the web wizard needs, and the only way it is allowed
    to touch ``config.local.yaml``: regenerating the file would mint a new
    ``auth.jwt_secret`` and sign every live session out — including the one
    the wizard is being driven from, which then cannot come back after the
    restart it ends with.

    ``updates`` maps dotted paths to values; a value of ``None`` removes the
    path. The file's leading comment block is preserved, everything else in
    it is left exactly as it was, and the write goes through
    :func:`nerve.paths.write_private_text`, which raises
    :class:`nerve.paths.InsecureFileError` with nothing written rather than
    leaving credentials in a file other users can read.

    ``header`` is used only when the file does not exist yet, so a file
    created here opens with the same explanation the installer's would.
    """
    paths.write_private_text(
        path, _merged_yaml(path, updates, header or _CONFIG_LOCAL_HEADER),
    )


@dataclass(frozen=True)
class CronOutcome:
    """What writing the cron files did.

    ``migrated_from`` is set when a legacy ``jobs.yaml`` was copied into the
    workspace instead of a blank scaffold being written — the one thing the
    caller has to tell the operator about.
    """

    system_file: Path
    jobs_file: Path
    migrated_from: Path | None = None


def write_cron_jobs(choices: SetupChoices) -> CronOutcome:
    """Write system crons to system.yaml and scaffold jobs.yaml for user crons."""
    jobs = build_cron_jobs(choices)

    # Cron config lives in the git-syncable workspace/config/cron subtree.
    # Via workspace_dir for the reason spelled out there: expanding only
    # `~` here sent a `workspace: ${VAR}` install's jobs to a literal
    # "./${VAR}/config/cron" beside the process CWD, where nothing loads
    # them, while settings.yaml landed correctly and the wizard reported
    # success.
    cron_dir = workspace_dir(choices) / "config" / "cron"
    cron_dir.mkdir(parents=True, exist_ok=True)
    (cron_dir / "gates").mkdir(parents=True, exist_ok=True)

    # Write system crons (managed by nerve init, safe to regenerate).
    # Published atomically: the scheduler loads this file, and a dump that
    # fails half way must leave the previous version rather than a truncated
    # one. Same bytes as before.
    system_file = cron_dir / "system.yaml"
    publish_text(
        system_file,
        _CRON_FILE_HEADER
        + yaml.safe_dump({"jobs": jobs}, default_flow_style=False, sort_keys=False),
    )

    # Create jobs.yaml scaffold if it doesn't exist. If this is an upgrade
    # from a legacy install, preserve the user's existing custom crons by
    # copying the legacy jobs.yaml rather than writing a blank placeholder
    # (a blank one here would shadow the legacy jobs — see _resolve_cron_dir).
    jobs_file = cron_dir / "jobs.yaml"
    migrated_from: Path | None = None
    if not jobs_file.exists():
        legacy_jobs = paths.cron_dir() / "jobs.yaml"
        if legacy_jobs.exists():
            shutil.copy2(legacy_jobs, jobs_file)
            migrated_from = legacy_jobs
        else:
            with open(jobs_file, "w", encoding="utf-8") as f:
                f.write("# Nerve — Custom Cron Jobs\n")
                f.write("# Add your own cron jobs here. Nerve will never overwrite this file.\n")
                f.write("# Format is the same as system.yaml — see it for examples.\n\n")
                f.write("jobs: []\n")

    return CronOutcome(
        system_file=system_file, jobs_file=jobs_file, migrated_from=migrated_from,
    )


def build_cron_jobs(choices: SetupChoices) -> list[dict[str, Any]]:
    """The ``system.yaml`` job list this install's answers produce."""
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

    if choices.mode == "personal":
        # Productivity crons (personal mode)
        for cron in PRODUCTIVITY_CRONS:
            enabled = cron["id"] in choices.enabled_crons
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
            if cron.get("run_if"):
                job["run_if"] = cron["run_if"]
            jobs.append(job)
    elif choices.mode == "worker":
        # Workers get skill crons — they create skills during onboarding
        # and those skills should be maintained automatically.
        # Other crons (task-planner, etc.) can be added during onboarding.
        for cron in PRODUCTIVITY_CRONS:
            if cron["id"] not in _WORKER_CRONS:
                continue
            enabled = cron["id"] in choices.enabled_crons
            job = {
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
            if cron.get("run_if"):
                job["run_if"] = cron["run_if"]
            jobs.append(job)

    return jobs


def optional_cron_ids_for(mode: str) -> tuple[str, ...]:
    """The optional cron ids a ``system.yaml`` written for ``mode`` carries."""
    if mode == "worker":
        return tuple(c["id"] for c in PRODUCTIVITY_CRONS if c["id"] in _WORKER_CRONS)
    return tuple(c["id"] for c in PRODUCTIVITY_CRONS)


# --- Turning the installer's optional crons on and off ------------------------
#
# The web wizard cannot regenerate ``system.yaml`` the way `nerve init` does:
# the job list depends on whether this is a personal or a worker install, and
# that is a wizard answer no running instance records. So the web path flips
# ``enabled`` on the jobs the installer already wrote and touches nothing else
# — which is also the only change a checklist step should be making to a file
# an operator may have edited.

_CRON_FILE_HEADER = (
    "# Nerve — System Cron Jobs\n"
    "# Managed by 'nerve init'. Safe to re-generate.\n"
    "# To add custom crons, use jobs.yaml instead.\n\n"
)

# Ids the wizard is allowed to toggle: the optional ones. The core crons are
# always on and are not offered (a checklist that can switch memory
# maintenance off is a settings editor).
OPTIONAL_CRON_IDS = tuple(cron["id"] for cron in PRODUCTIVITY_CRONS)


@dataclass(frozen=True)
class CronToggle:
    """One optional cron, as the checklist offers it."""

    id: str
    name: str
    description: str
    enabled: bool
    schedule: str = ""
    requires: str | None = None


@dataclass(frozen=True)
class CronToggleOutcome:
    status: str          # written | unchanged | missing | unreadable
    path: Path
    enabled: tuple[str, ...] = ()
    disabled: tuple[str, ...] = ()
    detail: str = ""


@dataclass(frozen=True)
class CronTogglePlan:
    """A cron change worked out in full, before anything is written.

    Every way this can fail — a missing file, YAML that does not parse, a
    document that is not a job list — is discovered here, so a caller can find
    out whether the change is possible *before* it publishes anything else.
    :meth:`publish` is the part that has no failure mode worth reporting
    separately: one atomic rename.
    """

    path: Path
    text: str | None
    enabled: tuple[str, ...] = ()
    disabled: tuple[str, ...] = ()
    problem: str = ""

    @property
    def ok(self) -> bool:
        return not self.problem

    @property
    def changes_anything(self) -> bool:
        return self.text is not None

    def publish(self) -> CronToggleOutcome:
        if self.problem:
            return CronToggleOutcome(
                status="missing" if self.problem == "missing" else "unreadable",
                path=self.path, detail=self.problem,
            )
        if self.text is None:
            return CronToggleOutcome(
                status="unchanged", path=self.path,
                enabled=self.enabled, disabled=self.disabled,
            )
        publish_text(self.path, self.text)
        return CronToggleOutcome(
            status="written", path=self.path,
            enabled=self.enabled, disabled=self.disabled,
        )


def system_cron_file(workspace: Path) -> Path:
    return Path(workspace) / "config" / "cron" / "system.yaml"


def _read_system_crons(workspace: Path) -> tuple[dict[str, Any] | None, str, str]:
    """``(document, raw_text, problem)`` for the generated cron file."""
    path = system_cron_file(workspace)
    if not path.exists():
        return None, "", "missing"
    raw = path.read_text(encoding="utf-8")
    try:
        loaded = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        return None, raw, f"unreadable: {e}"
    if loaded is not None and not isinstance(loaded, dict):
        return None, raw, "unreadable: not a mapping"
    document = loaded or {}
    if not isinstance(document.get("jobs"), list):
        return None, raw, "unreadable: no job list"
    return document, raw, ""


def list_optional_crons(workspace: Path) -> list[CronToggle]:
    """The optional crons this install actually has, and whether each is on.

    Empty when the file is missing or unreadable: the checklist then says the
    step has nothing to offer rather than inventing jobs that were never
    written.
    """
    document, _raw, problem = _read_system_crons(workspace)
    if problem or document is None:
        return []
    present = {
        str(job.get("id")): bool(job.get("enabled"))
        for job in document["jobs"]
        if isinstance(job, dict) and job.get("id")
    }
    known = {cron["id"]: cron for cron in PRODUCTIVITY_CRONS}
    return [
        CronToggle(
            id=cron_id,
            name=str(known[cron_id].get("display_name") or known[cron_id].get("name") or cron_id),
            description=str(known[cron_id].get("description") or ""),
            enabled=present[cron_id],
            schedule=str(known[cron_id].get("schedule") or ""),
            requires=known[cron_id].get("requires"),
        )
        for cron_id in OPTIONAL_CRON_IDS
        if cron_id in present
    ]


def plan_optional_crons(workspace: Path, enabled_ids: set[str]) -> CronTogglePlan:
    """Work out the cron change without touching anything.

    Split from the publication so a caller writing to several places can find
    out that the cron file is unreadable *before* it writes the first of them
    — a step that reports failure after changing configuration is a step whose
    error message is wrong.
    """
    path = system_cron_file(workspace)
    document, raw, problem = _read_system_crons(workspace)
    if problem or document is None:
        return CronTogglePlan(path=path, text=None, problem=problem or "unreadable")

    enabled, disabled = [], []
    changed = False
    for job in document["jobs"]:
        if not isinstance(job, dict):
            continue
        job_id = str(job.get("id") or "")
        if job_id not in OPTIONAL_CRON_IDS:
            continue
        wanted = job_id in enabled_ids
        (enabled if wanted else disabled).append(job_id)
        if bool(job.get("enabled")) != wanted:
            job["enabled"] = wanted
            changed = True

    if not changed:
        return CronTogglePlan(
            path=path, text=None,
            enabled=tuple(enabled), disabled=tuple(disabled),
        )

    header = leading_comment(raw) or _CRON_FILE_HEADER
    body = yaml.safe_dump(document, default_flow_style=False, sort_keys=False)
    return CronTogglePlan(
        path=path, text=header + body,
        enabled=tuple(enabled), disabled=tuple(disabled),
    )


def set_optional_crons(workspace: Path, enabled_ids: set[str]) -> CronToggleOutcome:
    """Enable exactly ``enabled_ids`` among the optional crons; leave the rest.

    Idempotent, and re-enterable: it is the state of the list that is set, not
    a delta applied to it. Jobs the wizard does not own — the core crons, and
    anything an operator added — keep whatever they say. Plan and publish in
    one call, for a caller with nothing else to coordinate.
    """
    return plan_optional_crons(workspace, enabled_ids).publish()


_WEEKDAYS = ("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")


def describe_cron(expr: str) -> str:
    """A short English rendering of the cron expressions the catalogue uses.

    ``*/N * * * *`` → "every N minutes"; ``0 */N * * *`` → "every N hours";
    ``0 H * * *`` → "daily at HH:00"; ``0 H * * D`` → "<weekday>s at HH:00";
    anything else is returned as it is.
    """
    parts = (expr or "").split()
    if len(parts) != 5:
        return expr
    minute, hour, dom, month, dow = parts
    if minute.startswith("*/") and hour == "*" and (dom, month, dow) == ("*", "*", "*"):
        return f"every {minute[2:]} minutes"
    if minute == "0" and hour.startswith("*/") and (dom, month, dow) == ("*", "*", "*"):
        return f"every {hour[2:]} hours"
    if minute == "0" and hour.isdigit() and (dom, month) == ("*", "*"):
        if dow == "*":
            return f"daily at {int(hour):02d}:00"
        if dow.isdigit() and 0 <= int(dow) <= 7:
            return f"{_WEEKDAYS[int(dow) % 7]}s at {int(hour):02d}:00"
    return expr


# --- What the workspace says about itself -------------------------------------

_PERSONAL_SOUL_HEADER = "# SOUL.md - Who You Are"
_WORKER_SOUL_HEADER = "# SOUL.md — Worker Identity"


def workspace_mode(workspace: Path) -> str | None:
    """``"personal"``, ``"worker"`` or ``None`` when no mode was chosen yet.

    Derived from the files, never stored: ``SOUL.md`` missing means no mode;
    its first line decides for untouched templates; the other template files
    break ties when somebody edited the header.
    """
    ws = Path(workspace)
    soul = ws / "SOUL.md"
    if not soul.exists():
        return None
    try:
        with soul.open(encoding="utf-8") as handle:
            first = handle.readline().strip()
    except OSError:
        first = ""
    if first == _WORKER_SOUL_HEADER:
        return "worker"
    if first == _PERSONAL_SOUL_HEADER:
        return "personal"
    if (ws / "IDENTITY.md").exists() or (ws / "USER.md").exists():
        return "personal"
    if (ws / "TASK.md").exists():
        return "worker"
    return "personal"


# --- The slim install (`nerve init` without the mode) -------------------------


@dataclass(frozen=True)
class SlimOutcome:
    """What the slim host-side install wrote."""

    files_written: tuple[str, ...]
    jwt_created: bool


def write_slim_install(
    choices: SetupChoices, config_dir: Path, *, host_workspace: Path | None = None,
) -> SlimOutcome:
    """Write the machine-local minimum and the mode-independent skeleton.

    Exactly what the browser cannot decide: where the files live, how the
    gateway listens, and the signing secret. **No** mode templates, **no**
    ``system.yaml``, **no** timezone/agent/memory defaults — the web Mode and
    About-you steps own those. Re-runnable: the merge writers leave every key
    they do not name alone, and ``config.local.yaml`` is written only when it
    does not exist yet, so a re-run never rotates ``auth.jwt_secret``.

    ``host_workspace`` is the Docker case: ``config.yaml`` records the
    container's workspace path, while the tracked settings and the skeleton
    are written under the host directory that is bind-mounted there.
    """
    config_dir = Path(config_dir)
    config_dir.mkdir(parents=True, exist_ok=True)
    workspace = Path(host_workspace) if host_workspace is not None else workspace_dir(choices)
    written: list[str] = []
    docker = choices.deployment == "docker"

    # W: directories, scaffold, bundled skills, cron gates dir.
    workspace.mkdir(parents=True, exist_ok=True)
    install_config_scaffold(workspace)
    install_bundled_skills(workspace)
    (workspace / "config" / "cron" / "gates").mkdir(parents=True, exist_ok=True)

    # M: this box.
    machine: dict[str, Any] = {
        "workspace": str(choices.workspace_path),
        "deployment": choices.deployment,
        "telegram.enabled": False,
        "sync.gmail.accounts": [],
    }
    if docker:
        machine["docker.extra_mounts"] = []
        # On the host every `nerve` command loads config with the container
        # workspace path, cannot find the tracked settings and would fall back
        # to port 8900 — so the published port is stated here too. Inside the
        # container the config dir is the same bind-mounted file, so M and S
        # agree.
        machine["gateway.host"] = choices.gateway_host
        machine["gateway.port"] = int(choices.gateway_port)
    merge_machine_paths(config_dir / "config.yaml", machine, header=CONFIG_YAML_HEADER)
    written.append("config.yaml")

    # S: the deployment's shared shape, sources explicitly off.
    merge_settings_paths(workspace, {
        "gateway.host": choices.gateway_host,
        "gateway.port": int(choices.gateway_port),
        "sync.telegram.enabled": False,
        "sync.gmail.enabled": False,
        "sync.github.enabled": False,
        "sync.github_events.enabled": False,
        "sync.github_repos.enabled": False,
        "sync.github_repos.repos": [],
    })
    written.append("config/settings.yaml")

    # L: only if absent (the signing secret is generated once), otherwise
    # only the harvested docker credentials are merged in.
    local_path = config_dir / "config.local.yaml"
    secrets_only = SetupChoices(
        anthropic_api_key=choices.anthropic_api_key,
        claude_oauth_token=choices.claude_oauth_token,
        github_token=choices.github_token,
    )
    jwt_created = False
    if not local_path.exists():
        write_config_local_yaml(secrets_only, config_dir)
        jwt_created = True
        written.append("config.local.yaml")
    else:
        harvested = leaf_paths(build_config_local(secrets_only))
        if harvested:
            merge_private_paths(local_path, harvested)
            written.append("config.local.yaml")

    return SlimOutcome(files_written=tuple(written), jwt_created=jwt_created)


# --- The Mode step ------------------------------------------------------------


class ModeRefused(RuntimeError):
    """The Mode step cannot write under lockdown; the message says why."""


@dataclass(frozen=True)
class ModeOutcome:
    created: tuple[str, ...]
    task_written: bool
    settings_defaults_added: tuple[str, ...]
    cron_written: bool


_TASK_HEADER = "# Task\n"


def _task_is_placeholder(path: Path) -> bool:
    """A ``TASK.md`` the onboarding session has not rewritten yet."""
    if not path.exists():
        return True
    text = path.read_text(encoding="utf-8").strip()
    return text.startswith(_TASK_HEADER.strip()) and "## Mission" not in text


def apply_mode(
    workspace: Path,
    mode: str,
    task_description: str = "",
    *,
    in_container: bool = False,
    locked: bool = False,
) -> ModeOutcome:
    """Choose the mode: templates, ``TASK.md``, settings defaults, ``system.yaml``.

    Never overwrites: :func:`initialize_workspace` copies only missing
    templates, ``TASK.md`` is written only while it is still the installer's
    placeholder, settings leaves are filled only where absent, and
    ``system.yaml`` is written only when missing.

    Under ``locked`` the reviewed files (templates, settings, cron) are
    refused with :class:`ModeRefused` before anything is written; when the
    templates are already present the one remaining effect — a worker's
    ``TASK.md`` — is still applied, because ``TASK.md`` is runtime state the
    agent itself maintains.
    """
    if mode not in ("personal", "worker"):
        raise ValueError(f"mode must be personal or worker, got {mode!r}")
    ws = Path(workspace)
    task_path = ws / "TASK.md"

    if locked:
        if workspace_mode(ws) is None:
            raise ModeRefused(
                "Configuration is managed from the config repository on this "
                "instance; changes arrive by PR and sync. The mode is defined by "
                "the workspace templates (SOUL.md and friends), which are reviewed "
                "root files here — add them from the config repository."
            )
        task_written = False
        if mode == "worker" and task_description and _task_is_placeholder(task_path):
            publish_text(task_path, f"{_TASK_HEADER}\n{task_description.strip()}\n")
            task_written = True
        return ModeOutcome(
            created=(), task_written=task_written,
            settings_defaults_added=(), cron_written=False,
        )

    ws.mkdir(parents=True, exist_ok=True)
    created = tuple(initialize_workspace(ws, mode))
    install_bundled_skills(ws)
    install_config_scaffold(ws)

    task_written = False
    if mode == "worker" and task_description and _task_is_placeholder(task_path):
        publish_text(task_path, f"{_TASK_HEADER}\n{task_description.strip()}\n")
        task_written = True

    if in_container:
        tools_md = ws / "TOOLS.md"
        if tools_md.exists():
            content = tools_md.read_text(encoding="utf-8")
            if "## Docker Environment" not in content:
                publish_text(tools_md, content + _DOCKER_TOOLS_SECTION)

    # Settings: the mode decides the memory categories (set, where absent) and
    # the rest of the installer's defaults are filled only where absent.
    categories = (
        _PERSONAL_MEMORY_CATEGORIES if mode == "personal" else _WORKER_MEMORY_CATEGORIES
    )
    _machine, portable, _shadowed = build_config_layers(SetupChoices(mode=mode))
    defaults: dict[str, Any] = {"memory.categories": categories}
    for section in ("agent", "sessions"):
        for key, value in portable[section].items():
            defaults[f"{section}.{key}"] = value
    for key in ("recall_model", "memorize_model", "fast_model", "embed_model"):
        defaults[f"memory.{key}"] = portable["memory"][key]
    defaults["quiet_start"] = portable["quiet_start"]
    defaults["quiet_end"] = portable["quiet_end"]
    defaults["provider.type"] = portable["provider"]["type"]
    if mode == "personal":
        defaults["telegram.dm_policy"] = "pairing"
        defaults["telegram.stream_mode"] = "partial"
    outcome = merge_settings_defaults(ws, defaults)
    added = tuple(outcome.added) if outcome.wrote else ()

    # Cron: the catalogue for this mode, with the default set on — only when
    # the file is not there yet; an operator-edited file is theirs.
    cron_written = False
    if not system_cron_file(ws).exists():
        write_cron_jobs(SetupChoices(
            mode=mode, workspace_path=ws, enabled_crons=list(DEFAULT_CRONS[mode]),
        ))
        cron_written = True

    return ModeOutcome(
        created=created, task_written=task_written,
        settings_defaults_added=added, cron_written=cron_written,
    )


# --- The profile step: pristine-guarded template lines ------------------------

# Exact template lines (trailing whitespace stripped) and the key each one
# answers for. A line that no longer reads like this was edited by a person
# and is left alone — the point of the guard.
_USER_PRISTINE: dict[str, tuple[str, ...]] = {
    "name": ("- **Name:** {{NAME}}", "- **Name:** {{USER_NAME}}"),
    "timezone": ("- **Timezone:** {{TIMEZONE}}",),
    "language": ("- **Language:** *(preferred response language — don't assume, observe or ask)*",),
    "quiet_hours": ("- **Quiet hours:** *(when not to ping unless urgent)*",),
    "work": ("**Work:**",),
}
_IDENTITY_PRISTINE: dict[str, tuple[str, ...]] = {
    "name": ("- **Name:** *(your name here)*",),
    "emoji": ("- **Emoji:** *(optional — pick one that feels like you)*",),
}


def _pristine_lines(path: Path, table: dict[str, tuple[str, ...]]) -> dict[str, bool]:
    """Which template lines in ``path`` are still untouched (missing file → all False)."""
    if not path.exists():
        return {key: False for key in table}
    try:
        lines = [line.rstrip() for line in path.read_text(encoding="utf-8").splitlines()]
    except OSError:
        return {key: False for key in table}
    return {
        key: any(line in candidates for line in lines)
        for key, candidates in table.items()
    }


def user_md_pristine(workspace: Path) -> dict[str, bool]:
    return _pristine_lines(Path(workspace) / "USER.md", _USER_PRISTINE)


def identity_md_pristine(workspace: Path) -> dict[str, bool]:
    return _pristine_lines(Path(workspace) / "IDENTITY.md", _IDENTITY_PRISTINE)


def _substitute(path: Path, replacements: dict[tuple[str, ...], str]) -> list[str]:
    """Replace whole template lines, once each, and publish. Returns what changed."""
    if not path.exists():
        return []
    original = path.read_text(encoding="utf-8")
    lines = original.splitlines(keepends=True)
    changed: list[str] = []
    for index, raw in enumerate(lines):
        stripped = raw.rstrip("\r\n").rstrip()
        for candidates, replacement in replacements.items():
            if stripped in candidates:
                ending = raw[len(raw.rstrip("\r\n")):]
                lines[index] = replacement + ending
                changed.append(replacement)
                break
    if changed:
        publish_text(path, "".join(lines))
    return changed


def apply_profile_templates(
    workspace: Path,
    *,
    name: str | None = None,
    timezone: str | None = None,
    language: str | None = None,
    quiet: tuple[str, str] | None = None,
    agent_name: str | None = None,
    agent_emoji: str | None = None,
    about: str | None = None,
) -> list[str]:
    """Fill the profile lines of ``USER.md``/``IDENTITY.md`` while pristine.

    Exact-line substitution: a line is replaced only while it still reads as
    the template wrote it, so a second person — or the agent, which maintains
    these files — never has an edit clobbered. ``None`` arguments are skipped.
    Returns the lines it changed.
    """
    ws = Path(workspace)
    # Defence in depth for every caller (the API validates first; the
    # terminal wizard and future callers come through here too): each
    # replacement is one line of a file loaded into the agent's prompt, so a
    # line break would plant arbitrary prompt lines under a display name.
    for label, value in (
        ("name", name), ("timezone", timezone), ("language", language),
        ("agent_name", agent_name), ("agent_emoji", agent_emoji), ("about", about),
        *((f"quiet[{i}]", part) for i, part in enumerate(quiet or ())),
    ):
        if value and ("\r" in value or "\n" in value):
            raise ValueError(f"{label} must be a single line (no line breaks)")
    changed: list[str] = []
    user: dict[tuple[str, ...], str] = {}
    if name:
        user[_USER_PRISTINE["name"]] = f"- **Name:** {name}"
    if timezone:
        user[_USER_PRISTINE["timezone"]] = f"- **Timezone:** {timezone}"
    if language:
        user[_USER_PRISTINE["language"]] = f"- **Language:** {language}"
    if quiet and quiet[0] and quiet[1]:
        tz = f" ({timezone})" if timezone else ""
        user[_USER_PRISTINE["quiet_hours"]] = (
            f"- **Quiet hours:** {quiet[0]}–{quiet[1]}{tz}"
        )
    if about:
        user[_USER_PRISTINE["work"]] = f"**Work:** {about}"
    if user:
        changed.extend(_substitute(ws / "USER.md", user))

    identity: dict[tuple[str, ...], str] = {}
    if agent_name:
        identity[_IDENTITY_PRISTINE["name"]] = f"- **Name:** {agent_name}"
    if agent_emoji:
        identity[_IDENTITY_PRISTINE["emoji"]] = f"- **Emoji:** {agent_emoji}"
    if identity:
        changed.extend(_substitute(ws / "IDENTITY.md", identity))
    return changed


# --- External agents ----------------------------------------------------------


def merge_external_agents(
    config_dir: Path, *, targets: list[str], conflict_policy: str = "backup",
) -> None:
    """Record the configured external agents in ``config.yaml``.

    Through :func:`merge_machine_paths`, so the file keeps its header and is
    republished atomically — the previous ``write_text(yaml.safe_dump(...))``
    dropped both.
    """
    merge_machine_paths(Path(config_dir) / "config.yaml", {
        "external_agents.enabled": True,
        "external_agents.sync_interval_minutes": 15,
        "external_agents.conflict_policy": conflict_policy,
        "external_agents.targets": [{"name": name, "enabled": True} for name in targets],
        "mcp_endpoint.enabled": True,
        "mcp_endpoint.path": "/mcp/v1",
    }, header=CONFIG_YAML_HEADER)


def mcp_url(config) -> str:
    """The MCP endpoint URL external agents should hit, from a loaded config.

    The trailing slash matters. A wildcard bind is not an address a client can
    connect to, so it becomes ``localhost``.
    """
    ssl = config.gateway.ssl
    scheme = "https" if getattr(ssl, "enabled", False) else "http"
    host = str(config.gateway.host or "127.0.0.1")
    if host in {"0.0.0.0", "::", "[::]", ""}:
        host = "localhost"
    port = int(config.gateway.port or 8900)
    mcp_path = "/" + str(config.mcp_endpoint.path or "/mcp/v1").strip("/")
    return f"{scheme}://{host}:{port}{mcp_path}/"


# --- From a loaded config back to choices (no secrets) ------------------------


def choices_from_config(config) -> SetupChoices:
    """The :class:`SetupChoices` a running instance currently embodies.

    Every web PUT starts here, overrides the fields the request names, and
    selects the paths it owns out of :func:`build_config_layers` — which is
    what makes "omitted = untouched" hold for derived keys such as
    ``sync.github_events.enabled``. **No secret** is carried: the wizard
    writes credentials and never reads them back.
    """
    ws = Path(config.workspace)
    return SetupChoices(
        deployment=str(getattr(config, "deployment", "server") or "server"),
        mode=workspace_mode(ws) or "personal",
        use_proxy=bool(config.proxy.enabled),
        provider_type=str(config.provider.type or "anthropic"),
        aws_region=str(config.provider.aws_region or ""),
        aws_profile=str(config.provider.aws_profile or ""),
        workspace_path=ws,
        timezone=str(config.timezone),
        telegram_allowed_users=[
            int(u) for u in config.telegram.allowed_users if isinstance(u, int)
        ],
        telegram_enabled=bool(config.telegram.enabled),
        github_sync=bool(config.sync.github.enabled),
        gmail_sync=bool(config.sync.gmail.enabled),
        gmail_accounts=list(config.sync.gmail.accounts or []),
        telegram_sync=bool(config.sync.telegram.enabled),
        external_agents=[t.name for t in config.external_agents.targets],
        external_agents_conflict_policy=str(config.external_agents.conflict_policy),
        agent_model=str(config.agent.model or ""),
        agent_effort=str(config.agent.effort or ""),
        quiet_start=str(config.quiet_start or ""),
        quiet_end=str(config.quiet_end or ""),
        slack_enabled=bool(config.slack.enabled),
        slack_allow_users=list(config.slack.allow_users or []),
        slack_allow_direct_messages=bool(config.slack.allow_direct_messages),
        slack_allow_channels=list(config.slack.allow_channels or []),
        gateway_host=str(config.gateway.host),
        gateway_port=int(config.gateway.port),
    )


def replace_choices(choices: SetupChoices, **overrides: Any) -> SetupChoices:
    """A copy of ``choices`` with the named fields replaced."""
    return dataclasses.replace(choices, **overrides)
