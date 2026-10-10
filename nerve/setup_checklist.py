"""The setup checklist, derived — never stored.

A step is ``done`` because a fact on disk, in the tracked settings or in the
database says so, so an install set up at the terminal shows the same
checklist as one set up in a browser, and an instance provisioned from the
environment or a config repository only shows what is genuinely left to
decide. The only stored decisions are *skipped* and *answered*
(:mod:`nerve.setup_state`).

Everything here is pure: :class:`Facts` carries what the caller already
resolved (the config object, the state file, whether the instance is
claimed, whether a Telegram channel is live), and the only I/O is reading the
workspace files the facts point at. The HTTP route and ``nerve doctor`` both
feed it, which is what keeps the two in agreement.

**No local-account fact is consulted** except for the ``claim`` step, which
exists only in local mode. The profile step's name comes from the actor the
caller resolved (an account's display name, or the gateway's actor header);
its status comes from the configuration and the workspace files.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from nerve.config import NerveConfig
from nerve.setup_state import SetupState
from nerve.setup_writer import (
    CORE_CRONS,
    DEFAULT_CRONS,
    PRODUCTIVITY_CRONS,
    describe_cron,
    identity_md_pristine,
    list_optional_crons,
    optional_cron_ids_for,
    system_cron_file,
    user_md_pristine,
    workspace_mode,
)

STEP_CLAIM = "claim"
STEP_MODE = "mode"
STEP_PROVIDER = "provider"
STEP_PROFILE = "profile"
STEP_MEMORY = "memory"
STEP_CHANNELS = "channels"
STEP_SOURCES = "sources"
STEP_AUTOMATION = "automation"
STEP_AGENTS = "agents"

STATUS_DONE = "done"
STATUS_PENDING = "pending"
STATUS_SKIPPED = "skipped"
STATUS_BLOCKED = "blocked"

# Why a step is ``blocked``. ``sequencing``: it waits for the Mode step and
# becomes actionable the moment a mode is chosen — so it is still work left to
# do and is counted in ``steps_left`` (otherwise the number would *rise* as
# the person progresses: choosing a mode turns four blocked steps pending).
# ``lockdown``: nothing on this page can change it, so it is never counted
# (C13) and ``finished`` can become true around it.
BLOCKED_SEQUENCING = "sequencing"
BLOCKED_LOCKDOWN = "lockdown"

# (id, wire title, modes, required, can_skip). Order is the order offered.
STEP_CATALOGUE: tuple[tuple[str, str, tuple[str, ...], bool, bool], ...] = (
    (STEP_CLAIM, "Claim", ("personal", "worker"), True, False),
    (STEP_MODE, "Mode", ("personal", "worker"), True, False),
    (STEP_PROVIDER, "Model", ("personal", "worker"), True, False),
    (STEP_PROFILE, "About you", ("personal", "worker"), False, True),
    (STEP_MEMORY, "Memory", ("personal", "worker"), False, True),
    (STEP_CHANNELS, "Channels", ("personal", "worker"), False, True),
    (STEP_SOURCES, "Sources", ("personal",), False, True),
    (STEP_AUTOMATION, "Automation", ("personal", "worker"), False, True),
    (STEP_AGENTS, "Agents", ("personal",), False, True),
)
STEP_IDS = tuple(step[0] for step in STEP_CATALOGUE)
SKIPPABLE_STEPS = frozenset(step[0] for step in STEP_CATALOGUE if step[4])
REQUIRED_STEPS = frozenset(step[0] for step in STEP_CATALOGUE if step[3])

# The wording a locked instance answers with. The same sentence everywhere, so
# the page can show one Lock badge per step with the reason as helper text.
LOCKDOWN_TEXT = (
    "Configuration is managed from the config repository on this instance; "
    "changes arrive by PR and sync."
)
_LOCKDOWN_CONFIG_STEP = LOCKDOWN_TEXT + " Configure this step from the config repository."
_LOCKDOWN_MODE = (
    LOCKDOWN_TEXT + " The mode is defined by the workspace templates (SOUL.md and "
    "friends), which are reviewed root files here — add them from the config "
    "repository."
)
_LOCKDOWN_PROFILE_EXTERNAL = (
    LOCKDOWN_TEXT + " Your name comes from your organisation's sign-in; USER.md is "
    "managed from the config repository."
)
MODE_LOCK_REASON = (
    "Mode was chosen when the workspace was created. Changing it means starting a "
    "new workspace: stop Nerve, point workspace: in config.yaml at an empty "
    "directory, and run setup again."
)
_CHOOSE_MODE_FIRST_PROFILE = (
    "Choose a mode first — the files this step fills in depend on it"
)
_CHOOSE_MODE_FIRST = "Choose a mode first"
_CHOOSE_MODE_FIRST_AUTOMATION = (
    "Choose a mode first — the job catalogue is created then"
)
PAIRING_AFTER_RESTART = (
    "The bot starts with this token after the restart at the end of setup. "
    "Pairing is offered there."
)

# What `nerve init` writes when nobody chose one (nerve/config.py's own
# default). A timezone that differs from it is an answer somebody gave.
DEFAULT_TIMEZONE = NerveConfig().timezone
DEFAULT_QUIET = (NerveConfig().quiet_start, NerveConfig().quiet_end)

# The persisted-config subset of each step's detail. Volatile fields (a test
# result, whether the bot is live, pairing counts, onboarding progress) are
# excluded so a reload on `visibilitychange` never remounts a form someone is
# typing into. ``name[]`` maps over a list.
ETAG_FIELDS: dict[str, tuple[str, ...]] = {
    STEP_CLAIM: ("username", "passwordless"),
    STEP_MODE: ("mode", "templates_present", "task_present"),
    STEP_PROVIDER: (
        "type", "credential", "anthropic_api_key", "claude_oauth_token",
        "proxy_enabled", "aws_region", "aws_profile", "model", "effort", "cron_model",
    ),
    STEP_PROFILE: (
        "name", "name_source", "display_name_editable", "writable_fields", "timezone",
        "timezone_is_default", "quiet_start", "quiet_end", "quiet_is_default",
        "language", "agent_name", "agent_emoji", "about_set", "user_md_pristine",
        "identity_md_pristine",
    ),
    STEP_MEMORY: ("openai_api_key", "embeddings_chosen", "embed_model"),
    STEP_CHANNELS: (
        "telegram.enabled", "telegram.bot_token", "telegram.dm_policy",
        "slack.enabled", "slack.mode", "slack.bot_token", "slack.app_token",
        "slack.allow_users", "slack.allow_direct_messages", "slack.allow_channels",
    ),
    STEP_SOURCES: (
        "github.enabled", "gmail.enabled", "gmail.accounts", "telegram.enabled",
        "telegram.api_id", "telegram.api_hash",
    ),
    STEP_AUTOMATION: ("file_present", "optional[].id", "optional[].enabled"),
    STEP_AGENTS: ("enabled", "conflict_policy"),
}

_PERSONAL_TEMPLATES = ("SOUL.md", "IDENTITY.md", "USER.md", "AGENTS.md", "TOOLS.md", "MEMORY.md")
_WORKER_TEMPLATES = ("SOUL.md", "AGENTS.md", "TOOLS.md", "MEMORY.md")


@dataclass(frozen=True)
class Facts:
    """Everything the derivation needs, resolved by the caller."""

    config: NerveConfig
    workspace: Path
    state: SetupState
    external_auth: bool = False
    #: Local mode only; external callers pass ``False`` and never compute it.
    unclaimed: bool = False
    lockdown: bool = False
    #: The lockdown text, or "no config dir", or ``None`` when writable.
    writable_problem: str | None = None
    #: ``config_reload.restart_pending()``; ``nerve doctor`` passes ``[]``.
    restart_pending: list[str] = field(default_factory=list)
    #: A TelegramChannel is registered, started, and runs the live token.
    telegram_live: bool = False
    #: ST ``boot_errors["telegram"]`` for the current boot, token scrubbed.
    telegram_start_error: str | None = None
    #: ANTHROPIC_API_KEY or CLAUDE_CODE_OAUTH_TOKEN in the gateway's environment.
    env_credential: bool = False
    #: The environment carries only an OAuth token (no API key).
    env_oauth_only: bool = False
    #: ``not_applicable`` | ``pending`` | ``running`` | ``ended`` | ``done``;
    #: ``ended`` = the onboarding session stopped without writing ``## Mission``
    #: (resolved by the API, which can ask the engine; doctor never says it).
    onboarding: str = "not_applicable"
    onboarding_session_id: str | None = None
    #: The caller's name: an account's display name, or the actor header's.
    display_name: str | None = None
    #: Live channel → process cache → ST → None (resolved by the caller).
    bot_username: str | None = None
    #: Local mode: the claimed account, for the claim step's detail.
    account_username: str | None = None
    account_passwordless: bool = False
    #: The last successful provider test in this process, if any.
    provider_last_test: dict[str, Any] | None = None
    #: Agent ids the registry knows (``codex``, ``claude-code``).
    available_agents: tuple[str, ...] = ()


@dataclass
class Step:
    id: str
    title: str
    status: str
    required: bool
    can_skip: bool
    blocked_reason: str | None
    read_only: bool
    read_only_reason: str | None
    etag: str
    summary: str
    files: list[str]
    detail: dict[str, Any]
    #: ``sequencing`` | ``lockdown`` while ``status == blocked``, else ``None``.
    blocked_kind: str | None = None

    @property
    def counts_as_left(self) -> bool:
        """Pending, or blocked only until the Mode step is answered."""
        return self.status == STATUS_PENDING or (
            self.status == STATUS_BLOCKED and self.blocked_kind == BLOCKED_SEQUENCING
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "status": self.status,
            "required": self.required,
            "can_skip": self.can_skip,
            "blocked_reason": self.blocked_reason,
            "blocked_kind": self.blocked_kind,
            "read_only": self.read_only,
            "read_only_reason": self.read_only_reason,
            "etag": self.etag,
            "summary": self.summary,
            "files": list(self.files),
            "detail": self.detail,
        }


# --------------------------------------------------------------------------- #
#  Small pure helpers                                                           #
# --------------------------------------------------------------------------- #


def mask(value: Any) -> dict[str, Any]:
    """``{"configured": bool, "last4": str | None}`` for a secret.

    ``last4`` only for *string* secrets of twelve characters or more — never
    for an integer such as ``sync.telegram.api_id`` (an int's last digits say
    too much about too short a value) and never for a short string.
    """
    if value is None or value == "" or value == 0:
        return {"configured": False, "last4": None}
    if isinstance(value, bool):
        return {"configured": bool(value), "last4": None}
    if not isinstance(value, str):
        return {"configured": True, "last4": None}
    text = str(value)
    return {"configured": True, "last4": text[-4:] if len(text) >= 12 else None}


def _pick(detail: Any, path: str) -> Any:
    """``detail`` at a dotted path; ``name[]`` maps over a list."""
    node = detail
    for part in path.split("."):
        if part.endswith("[]"):
            key = part[:-2]
            items = node.get(key) if isinstance(node, dict) else None
            if not isinstance(items, list):
                return None
            rest = path.split(".", path.split(".").index(part) + 1)
            remainder = ".".join(rest[1:]) if len(rest) > 1 else ""
            return [_pick(item, remainder) if remainder else item for item in items]
        if not isinstance(node, dict):
            return None
        node = node.get(part)
    return node


def etag(step_id: str, detail: dict[str, Any]) -> str:
    """First 12 hex of a SHA-1 over the persisted-config subset of ``detail``."""
    fields = ETAG_FIELDS.get(step_id, ())
    subset = {name: _pick(detail, name) for name in fields}
    blob = json.dumps(subset, sort_keys=True, default=str, ensure_ascii=True)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()[:12]


def files_for(step_id: str, mode: str | None) -> list[str]:
    """The display paths a step writes, for the Review table."""
    ws = "workspace/"
    if step_id == STEP_CLAIM:
        return []
    if step_id == STEP_MODE:
        templates = _WORKER_TEMPLATES if mode == "worker" else _PERSONAL_TEMPLATES
        files = [ws + name for name in templates]
        if mode == "worker":
            files.append(ws + "TASK.md")
        return files + [ws + "config/settings.yaml", ws + "config/cron/system.yaml"]
    if step_id == STEP_PROVIDER:
        return ["config.local.yaml", ws + "config/settings.yaml", "config.yaml"]
    if step_id == STEP_PROFILE:
        files = [ws + "config/settings.yaml"]
        if mode != "worker":
            files += [ws + "USER.md", ws + "IDENTITY.md"]
        return files
    if step_id == STEP_MEMORY:
        return ["config.local.yaml"]
    if step_id == STEP_CHANNELS:
        return ["config.local.yaml", "config.yaml", ws + "config/settings.yaml"]
    if step_id == STEP_SOURCES:
        return [ws + "config/settings.yaml", "config.yaml", "config.local.yaml"]
    if step_id == STEP_AUTOMATION:
        return [ws + "config/cron/system.yaml"]
    if step_id == STEP_AGENTS:
        return ["config.yaml"]
    return []


def steps_for_mode(mode: str | None, *, external_auth: bool) -> list[tuple[str, str, bool, bool]]:
    """``(id, title, required, can_skip)`` in order, for this mode and auth mode."""
    out = []
    for step_id, title, modes, required, can_skip in STEP_CATALOGUE:
        if step_id == STEP_CLAIM and external_auth:
            continue
        if mode is not None and mode not in modes:
            continue
        if step_id == STEP_PROFILE and mode == "worker":
            title = "Operator"
        out.append((step_id, title, required, can_skip))
    return out


def _read_lines(path: Path) -> list[str]:
    try:
        return [line.rstrip() for line in path.read_text(encoding="utf-8").splitlines()]
    except OSError:
        return []


def _line_value(lines: list[str], prefix: str) -> str | None:
    """The value after ``prefix`` on its line, unless it is still a placeholder."""
    for line in lines:
        if line.startswith(prefix):
            value = line[len(prefix):].strip()
            if not value or value.startswith("*(") or "{{" in value:
                return None
            return value
    return None


def onboarding_from_task(workspace: Path, mode: str | None) -> str:
    """``not_applicable`` | ``pending`` | ``done`` from ``TASK.md`` alone.

    The ``running`` state needs the session table and is the caller's to add
    (:func:`nerve.gateway.routes.setup` does; ``nerve doctor`` does not).
    """
    if mode != "worker":
        return "not_applicable"
    task = Path(workspace) / "TASK.md"
    if not task.exists():
        return "pending"
    try:
        text = task.read_text(encoding="utf-8").strip()
    except OSError:
        return "pending"
    if text.startswith("# Task") and "## Mission" not in text:
        return "pending"
    return "done"


def _system_yaml_jobs(workspace: Path) -> list[dict[str, Any]] | None:
    path = system_cron_file(workspace)
    if not path.exists():
        return None
    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return []
    if not isinstance(loaded, dict) or not isinstance(loaded.get("jobs"), list):
        return []
    return [job for job in loaded["jobs"] if isinstance(job, dict)]


# --------------------------------------------------------------------------- #
#  Per-step derivation                                                          #
# --------------------------------------------------------------------------- #


def _provider_credential(facts: Facts) -> str:
    config = facts.config
    if config.provider.is_bedrock:
        return "bedrock"
    if config.proxy.enabled:
        return "proxy"
    if config.anthropic_api_key:
        return "api_key"
    if config.claude_oauth_token:
        return "oauth_token"
    if facts.env_credential:
        return "environment"
    return "none"


def _provider_detail(facts: Facts) -> tuple[dict[str, Any], str]:
    config = facts.config
    credential = _provider_credential(facts)
    if credential == "bedrock":
        kind = "bedrock"
    elif credential == "oauth_token" or (credential == "environment" and facts.env_oauth_only):
        kind = "oauth"
    else:
        kind = "anthropic"
    warning = None
    if credential == "oauth_token" or (credential == "environment" and facts.env_oauth_only):
        warning = "oauth_background_jobs"
    detail = {
        "type": kind,
        "credential": credential,
        "anthropic_api_key": mask(config.anthropic_api_key),
        "claude_oauth_token": mask(config.claude_oauth_token),
        "proxy_enabled": bool(config.proxy.enabled),
        "aws_region": config.provider.aws_region or "",
        "aws_profile": config.provider.aws_profile or "",
        "model": config.agent.model,
        "effort": config.agent.effort,
        "cron_model": config.agent.cron_model,
        "warning": warning,
        "last_test": facts.provider_last_test,
    }
    labels = {
        "bedrock": f"AWS Bedrock ({config.provider.aws_region or 'default region'})",
        "proxy": "Claude subscription via the local proxy",
        "api_key": "Anthropic API key"
        + (f" (…{detail['anthropic_api_key']['last4']})" if detail["anthropic_api_key"]["last4"] else ""),
        "oauth_token": "Claude subscription token",
        "environment": "Credential from the environment",
        "none": "",
    }
    summary = labels[credential]
    if summary:
        summary += f" · {config.agent.model} · effort {config.agent.effort}"
    return detail, summary


def _profile_detail(facts: Facts, mode: str | None) -> tuple[dict[str, Any], str]:
    config = facts.config
    ws = facts.workspace
    personal = mode == "personal"
    user_lines = _read_lines(ws / "USER.md") if personal else []
    identity_lines = _read_lines(ws / "IDENTITY.md") if personal else []
    user_pristine = user_md_pristine(ws) if personal else {
        "name": False, "timezone": False, "language": False, "quiet_hours": False, "work": False,
    }
    identity_pristine = identity_md_pristine(ws) if personal else {"name": False, "emoji": False}
    writable_fields = None
    if facts.lockdown and not facts.external_auth:
        writable_fields = ["name"]
    # In external mode the step's value is what the instance holds (USER.md
    # is shared by everyone), not the requesting person's header name — which
    # would make a server-side step read differently per person and let the
    # second person's Continue overwrite the first one's saved name. The
    # header name is the prefill only while USER.md's name line is pristine;
    # it is still exposed as ``actor_name`` so the page can say who is signed in.
    name = facts.display_name
    name_source = "actor_header" if facts.external_auth else "account"
    if facts.external_auth and personal and not user_pristine.get("name", True):
        saved = _line_value(user_lines, "- **Name:**")
        if saved:
            name, name_source = saved, "user_md"
    detail = {
        "name": name,
        "name_source": name_source,
        "actor_name": facts.display_name,
        "display_name_editable": not facts.external_auth,
        "writable_fields": writable_fields,
        "timezone": config.timezone,
        "timezone_is_default": config.timezone == DEFAULT_TIMEZONE,
        "quiet_start": config.quiet_start,
        "quiet_end": config.quiet_end,
        "quiet_is_default": (config.quiet_start, config.quiet_end) == DEFAULT_QUIET,
        "language": _line_value(user_lines, "- **Language:**"),
        "agent_name": _line_value(identity_lines, "- **Name:**"),
        "agent_emoji": _line_value(identity_lines, "- **Emoji:**"),
        "about_set": bool(personal and not user_pristine.get("work", False)
                          and _line_value(user_lines, "**Work:**") is not None),
        "user_md_pristine": user_pristine,
        "identity_md_pristine": identity_pristine,
    }
    summary = config.timezone
    if name:
        summary = f"{name} · {config.timezone}"
    return detail, summary


def _channels_detail(facts: Facts) -> tuple[dict[str, Any], str]:
    config = facts.config
    tg = config.telegram
    slack = config.slack
    token_set = bool(tg.bot_token)
    if facts.lockdown:
        pairing_available, reason = False, LOCKDOWN_TEXT
    elif not token_set:
        pairing_available, reason = False, "Add a bot token first."
    elif tg.dm_policy != "pairing":
        pairing_available, reason = False, "Telegram DM policy is not 'pairing'."
    elif not facts.telegram_live:
        pairing_available, reason = False, PAIRING_AFTER_RESTART
    else:
        pairing_available, reason = True, None
    detail = {
        "telegram": {
            "enabled": bool(tg.enabled),
            "bot_token": mask(tg.bot_token),
            "bot_username": facts.bot_username,
            "live": bool(facts.telegram_live),
            "start_error": facts.telegram_start_error,
            "dm_policy": tg.dm_policy,
            "allowed_users_count": len(tg.allowed_users or []),
            "pairing_available": pairing_available,
            "pairing_unavailable_reason": reason,
        },
        "slack": {
            "enabled": bool(slack.enabled),
            "mode": slack.mode,
            "bot_token": mask(slack.bot_token),
            "app_token": mask(slack.app_token),
            "allow_users": list(slack.allow_users or []),
            "allow_direct_messages": bool(slack.allow_direct_messages),
            "allow_channels": list(slack.allow_channels or []),
        },
    }
    parts = []
    if token_set:
        name = f"@{facts.bot_username}" if facts.bot_username else "bot"
        paired = (
            f"{len(tg.allowed_users)} paired" if tg.allowed_users else "not paired yet"
        )
        if not facts.telegram_live and facts.restart_pending:
            paired += " (restart pending)"
        parts.append(f"Telegram {name} · {paired}")
    if slack.bot_token and slack.app_token:
        parts.append("Slack" + (" (hosted)" if slack.mode == "hosted" else ""))
    elif slack.mode == "hosted":
        parts.append("Slack managed by the gateway")
    return detail, " · ".join(parts)


def _sources_detail(facts: Facts) -> tuple[dict[str, Any], str]:
    sync = facts.config.sync
    detail = {
        "github": {"enabled": bool(sync.github.enabled)},
        "gmail": {"enabled": bool(sync.gmail.enabled), "accounts": list(sync.gmail.accounts or [])},
        "telegram": {
            "enabled": bool(sync.telegram.enabled),
            "api_id": mask(sync.telegram.api_id),
            "api_hash": mask(sync.telegram.api_hash),
        },
    }
    on = [name for name, flag in (
        ("GitHub", sync.github.enabled), ("Gmail", sync.gmail.enabled),
        ("Telegram", sync.telegram.enabled),
    ) if flag]
    if not on and STEP_SOURCES in facts.state.answered:
        return detail, "None"  # a deliberate "none of these" beside the Done badge
    return detail, ", ".join(on)


def _automation_detail(facts: Facts, mode: str | None) -> tuple[dict[str, Any], str, bool]:
    """``(detail, summary, differs_from_default)``."""
    ws = facts.workspace
    jobs = _system_yaml_jobs(ws)
    file_present = jobs is not None
    defaults = set(DEFAULT_CRONS.get(mode or "personal", []))
    toggles = list_optional_crons(ws) if file_present else []
    sync = facts.config.sync
    any_source = bool(sync.github.enabled or sync.gmail.enabled or sync.telegram.enabled)
    optional = []
    differs = False
    for toggle in toggles:
        default = toggle.id in defaults
        if toggle.enabled != default:
            differs = True
        optional.append({
            "id": toggle.id,
            "name": toggle.name,
            "description": toggle.description,
            "requires": toggle.requires,
            "requirement_met": (any_source if toggle.requires else True),
            "schedule": toggle.schedule,
            "schedule_text": describe_cron(toggle.schedule),
            "enabled": toggle.enabled,
            "default": default,
        })
    core = [{
        "id": c["id"], "name": "Memory maintenance" if c["id"] == "memory-maintenance" else c["id"],
        "description": c["description"], "schedule": c["schedule"],
        "schedule_text": describe_cron(c["schedule"]),
    } for c in CORE_CRONS]
    detail = {
        "file_present": file_present,
        "timezone": facts.config.timezone,
        "core": core,
        "optional": optional,
    }
    summary = ""
    if file_present:
        on = sum(1 for o in optional if o["enabled"])
        summary = f"{on} of {len(optional)} optional jobs on"
    return detail, summary, differs


def _agents_detail(facts: Facts) -> tuple[dict[str, Any], str]:
    ext = facts.config.external_agents
    enabled = [t.name for t in ext.targets if t.enabled]
    detail = {
        "enabled": enabled,
        "conflict_policy": ext.conflict_policy,
        "available": list(facts.available_agents),
    }
    if not enabled and STEP_AGENTS in facts.state.answered:
        return detail, "None chosen"
    return detail, ", ".join(enabled)


def _mode_detail(facts: Facts, mode: str | None) -> tuple[dict[str, Any], str]:
    ws = facts.workspace
    templates = _WORKER_TEMPLATES if mode == "worker" else _PERSONAL_TEMPLATES
    present = [name for name in templates if (ws / name).exists()]
    detail = {
        "mode": mode,
        "templates_present": present,
        "task_present": (ws / "TASK.md").exists(),
        "onboarding": facts.onboarding,
        "onboarding_session_id": facts.onboarding_session_id,
    }
    summary = {"personal": "Personal assistant", "worker": "Worker"}.get(mode or "", "")
    return detail, summary


def _claim_detail(facts: Facts) -> tuple[dict[str, Any], str]:
    detail = {
        "username": facts.account_username,
        "passwordless": bool(facts.account_passwordless),
    }
    if facts.unclaimed:
        return detail, ""
    who = facts.account_username or "owner"
    return detail, f"{who} · {'passwordless' if facts.account_passwordless else 'password set'}"


# --------------------------------------------------------------------------- #
#  The derivation                                                              #
# --------------------------------------------------------------------------- #


def _skipped_or_pending(step_id: str, facts: Facts) -> str:
    return STATUS_SKIPPED if step_id in facts.state.skipped else STATUS_PENDING


def derive(facts: Facts) -> list[Step]:
    """Every step for this instance, with status, reason, detail and etag."""
    config = facts.config
    ws = facts.workspace
    mode = workspace_mode(ws)
    answered = facts.state.answered
    locked = facts.lockdown
    steps: list[Step] = []

    def _lockdown(reason: str) -> tuple[str, str, str]:
        return STATUS_BLOCKED, reason, BLOCKED_LOCKDOWN

    def _until_mode(reason: str) -> tuple[str, str, str]:
        # Waiting for the Mode step is work left to do — unless the instance
        # is locked, where the templates arrive from the repository and
        # nothing on the page can answer Mode either.
        return STATUS_BLOCKED, reason, (BLOCKED_LOCKDOWN if locked else BLOCKED_SEQUENCING)

    for step_id, title, required, can_skip in steps_for_mode(mode, external_auth=facts.external_auth):
        blocked_reason: str | None = None
        blocked_kind: str | None = None
        read_only = False
        read_only_reason: str | None = None
        detail: dict[str, Any]
        summary: str

        if step_id == STEP_CLAIM:
            detail, summary = _claim_detail(facts)
            status = STATUS_PENDING if facts.unclaimed else STATUS_DONE

        elif step_id == STEP_MODE:
            detail, summary = _mode_detail(facts, mode)
            if mode is not None:
                status = STATUS_DONE
                read_only, read_only_reason = True, MODE_LOCK_REASON
            elif locked:
                status, blocked_reason, blocked_kind = _lockdown(_LOCKDOWN_MODE)
            else:
                status = STATUS_PENDING

        elif step_id == STEP_PROVIDER:
            detail, summary = _provider_detail(facts)
            done = bool(
                config.effective_api_key or config.provider.is_bedrock
                or config.proxy.enabled or config.claude_oauth_token
                or facts.env_credential or (STEP_PROVIDER in answered)
            )
            if done:
                status = STATUS_DONE
            elif locked:
                status, blocked_reason, blocked_kind = _lockdown(_LOCKDOWN_CONFIG_STEP)
            else:
                status = STATUS_PENDING

        elif step_id == STEP_PROFILE:
            detail, summary = _profile_detail(facts, mode)
            name_filled = mode == "personal" and (ws / "USER.md").exists() and not detail[
                "user_md_pristine"
            ]["name"]
            done = bool(
                STEP_PROFILE in answered
                or config.timezone != DEFAULT_TIMEZONE
                or name_filled
            )
            if done:
                status = STATUS_DONE
            elif locked and facts.external_auth:
                status, blocked_reason, blocked_kind = _lockdown(_LOCKDOWN_PROFILE_EXTERNAL)
            elif mode is None:
                status, blocked_reason, blocked_kind = _until_mode(_CHOOSE_MODE_FIRST_PROFILE)
            else:
                status = _skipped_or_pending(step_id, facts)

        elif step_id == STEP_MEMORY:
            key = mask(config.openai_api_key)
            detail = {
                "openai_api_key": key,
                "embeddings_chosen": bool(config.openai_api_key),
                "embed_model": config.memory.embed_model or "text-embedding-3-small",
            }
            if config.openai_api_key:
                summary = f"OpenAI key (…{key['last4']}) · vector search on" if key["last4"] else "OpenAI key · vector search on"
            elif STEP_MEMORY in answered:
                summary = "LLM-ranked recall (chosen)"
            else:
                summary = ""
            done = bool(config.openai_api_key or STEP_MEMORY in answered)
            if done:
                status = STATUS_DONE
            elif locked:
                status, blocked_reason, blocked_kind = _lockdown(_LOCKDOWN_CONFIG_STEP)
            else:
                status = _skipped_or_pending(step_id, facts)

        elif step_id == STEP_CHANNELS:
            detail, summary = _channels_detail(facts)
            done = bool(
                config.telegram.bot_token or (config.slack.bot_token and config.slack.app_token)
            )
            if done:
                status = STATUS_DONE
            elif locked:
                status, blocked_reason, blocked_kind = _lockdown(_LOCKDOWN_CONFIG_STEP)
            else:
                status = _skipped_or_pending(step_id, facts)

        elif step_id == STEP_SOURCES:
            detail, summary = _sources_detail(facts)
            sync = config.sync
            done = bool(
                sync.github.enabled or sync.gmail.enabled or sync.telegram.enabled
                or STEP_SOURCES in answered
            )
            if done:
                status = STATUS_DONE
            elif locked:
                status, blocked_reason, blocked_kind = _lockdown(_LOCKDOWN_CONFIG_STEP)
            elif mode is None:
                status, blocked_reason, blocked_kind = _until_mode(_CHOOSE_MODE_FIRST)
            else:
                status = _skipped_or_pending(step_id, facts)

        elif step_id == STEP_AUTOMATION:
            detail, summary, differs = _automation_detail(facts, mode)
            done = bool(STEP_AUTOMATION in answered or (detail["file_present"] and differs))
            if done:
                status = STATUS_DONE
            elif locked:
                status, blocked_reason, blocked_kind = _lockdown(_LOCKDOWN_CONFIG_STEP)
            elif not detail["file_present"]:
                status, blocked_reason, blocked_kind = _until_mode(_CHOOSE_MODE_FIRST_AUTOMATION)
            else:
                status = _skipped_or_pending(step_id, facts)

        elif step_id == STEP_AGENTS:
            detail, summary = _agents_detail(facts)
            done = bool(config.external_agents.targets or STEP_AGENTS in answered)
            if done:
                status = STATUS_DONE
            elif locked:
                status, blocked_reason, blocked_kind = _lockdown(_LOCKDOWN_CONFIG_STEP)
            elif mode is None:
                status, blocked_reason, blocked_kind = _until_mode(_CHOOSE_MODE_FIRST)
            else:
                status = _skipped_or_pending(step_id, facts)

        else:  # pragma: no cover - the catalogue is closed
            continue

        # Under lockdown a done config step is read-only (Change refused); a
        # blocked step is read-only too so the page shows one Lock badge.
        if locked and step_id != STEP_CLAIM and status == STATUS_DONE and not (
            step_id == STEP_PROFILE and not facts.external_auth
        ):
            if not read_only:
                read_only, read_only_reason = True, LOCKDOWN_TEXT
        if status == STATUS_BLOCKED:
            read_only = True
            read_only_reason = read_only_reason or blocked_reason

        steps.append(Step(
            id=step_id, title=title, status=status, required=required,
            can_skip=can_skip, blocked_reason=blocked_reason, read_only=read_only,
            read_only_reason=read_only_reason, etag=etag(step_id, detail),
            summary=summary, files=files_for(step_id, mode), detail=detail,
            blocked_kind=blocked_kind if status == STATUS_BLOCKED else None,
        ))
    return steps


def summarize(facts: Facts) -> dict[str, Any]:
    """The ``SetupState`` body minus what only the gateway knows
    (``generation``, ``restart_mode``, ``restart_message``, ``warning``)."""
    steps = derive(facts)
    debts = dict(facts.state.debts)
    restart_reasons = list(facts.restart_pending) + [
        f"{name}: {text}" for name, text in sorted(debts.items())
    ]
    restart_pending = bool(restart_reasons)
    finished = (
        all(
            s.status in (STATUS_DONE, STATUS_BLOCKED)
            if s.required else s.status in (STATUS_DONE, STATUS_SKIPPED, STATUS_BLOCKED)
            for s in steps
        )
        and not restart_pending
    )
    return {
        "auth_mode": "external" if facts.external_auth else "local",
        "mode": workspace_mode(facts.workspace),
        "lockdown": bool(facts.lockdown),
        "writable": facts.writable_problem is None,
        "read_only_reason": facts.writable_problem,
        "restart_pending": restart_pending,
        "restart_reasons": restart_reasons,
        "finished": finished,
        # Pending steps plus the ones waiting only for the Mode step — never
        # a lockdown-blocked one (C13) — so the count only ever goes down.
        "steps_left": sum(1 for s in steps if s.counts_as_left),
        "steps": [s.as_dict() for s in steps],
    }
