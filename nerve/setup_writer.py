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

Two entry shapes:

* ``write_*`` — what ``nerve init`` does: this run's answers own the file.
* ``merge_*`` — what the web wizard does: one step's keys are merged into
  whatever is already on disk. A running install's ``config.local.yaml``
  already holds the signing secret every live session is signed with, so the
  wizard must never regenerate the file (see :func:`merge_private_paths`).
"""

from __future__ import annotations

import copy
import secrets
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from nerve import paths
from nerve.config import _expand_path, _interpolate_str, workspace_settings_file

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
    # Provider
    provider_type: str = "anthropic"  # "anthropic" | "bedrock"
    aws_region: str = ""
    aws_profile: str = ""
    workspace_path: Path = field(default_factory=lambda: Path("~/nerve-workspace"))
    timezone: str = "America/New_York"
    user_name: str = ""
    telegram_bot_token: str = ""
    # Telegram user IDs authorized to DM the bot. Empty = pair after setup
    # via `nerve pair` + /pair <code>.
    telegram_allowed_users: list[int] = field(default_factory=list)
    password: str = ""  # plaintext during wizard, hashed at write time
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
            "host": "0.0.0.0",
            "port": 8900,
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
        "quiet_start": "02:00",
        "quiet_end": "08:00",
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

    # Sync credentials (secrets — go in local config)
    if choices.telegram_sync and choices.telegram_api_id:
        local.setdefault("sync", {})["telegram"] = {
            "api_id": choices.telegram_api_id,
            "api_hash": choices.telegram_api_hash,
        }

    return local


def write_config_yaml(choices: SetupChoices, config_dir: Path) -> Path:
    """Write the machine-local base config.yaml."""
    machine, _portable, _shadowed = build_config_layers(choices)
    config_path = config_dir / "config.yaml"
    # encoding is pinned on every config read and write in this module, to
    # match nerve.config, which pins it on the way in. Under an ASCII default
    # encoding an unpinned open() raises on the em-dash in these headers, at
    # the first write, before any user value is involved. A user value cannot
    # trigger it: safe_dump escapes non-ASCII to \xNN, so the dumped body is
    # always ASCII.
    with open(config_path, "w", encoding="utf-8") as f:
        f.write(CONFIG_YAML_HEADER)
        yaml.safe_dump(machine, f, default_flow_style=False, sort_keys=False)
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

    with open(settings_path, "w", encoding="utf-8") as f:
        f.write(_SETTINGS_HEADER)
        yaml.safe_dump(merged, f, default_flow_style=False, sort_keys=False)

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

    # The file holds the signing secret, the password hash and every API key
    # collected above, so it is *created* owner-only rather than written and
    # chmod'ed afterwards — which left all of that readable for the moment
    # in between. If the filesystem will not keep it private, nothing is
    # written and setup fails: an instance whose secrets every local user
    # can read is not a successful install, and it would start happily.
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

    opening = leading_comment(original) if original else (header or _CONFIG_LOCAL_HEADER)
    paths.write_private_text(
        path,
        opening + yaml.safe_dump(data, default_flow_style=False, sort_keys=False),
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

    # Write system crons (managed by nerve init, safe to regenerate)
    system_file = cron_dir / "system.yaml"

    with open(system_file, "w", encoding="utf-8") as f:
        f.write("# Nerve — System Cron Jobs\n")
        f.write("# Managed by 'nerve init'. Safe to re-generate.\n")
        f.write("# To add custom crons, use jobs.yaml instead.\n\n")
        yaml.safe_dump({"jobs": jobs}, f, default_flow_style=False, sort_keys=False)

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
        _WORKER_CRONS = ("skill-reviser", "skill-extractor", "task-planner")
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


@dataclass(frozen=True)
class CronToggleOutcome:
    status: str          # written | unchanged | missing | unreadable
    path: Path
    enabled: tuple[str, ...] = ()
    disabled: tuple[str, ...] = ()
    detail: str = ""


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
            name=str(known[cron_id].get("name") or cron_id),
            description=str(known[cron_id].get("description") or ""),
            enabled=present[cron_id],
        )
        for cron_id in OPTIONAL_CRON_IDS
        if cron_id in present
    ]


def set_optional_crons(workspace: Path, enabled_ids: set[str]) -> CronToggleOutcome:
    """Enable exactly ``enabled_ids`` among the optional crons; leave the rest.

    Idempotent, and re-enterable: it is the state of the list that is set, not
    a delta applied to it. Jobs the wizard does not own — the core crons, and
    anything an operator added — keep whatever they say.
    """
    path = system_cron_file(workspace)
    document, raw, problem = _read_system_crons(workspace)
    if problem or document is None:
        return CronToggleOutcome(
            status="missing" if problem == "missing" else "unreadable",
            path=path,
            detail=problem,
        )

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
        return CronToggleOutcome(
            status="unchanged", path=path,
            enabled=tuple(enabled), disabled=tuple(disabled),
        )

    header = leading_comment(raw) or _CRON_FILE_HEADER
    with open(path, "w", encoding="utf-8") as f:
        f.write(header)
        yaml.safe_dump(document, f, default_flow_style=False, sort_keys=False)

    return CronToggleOutcome(
        status="written", path=path,
        enabled=tuple(enabled), disabled=tuple(disabled),
    )
