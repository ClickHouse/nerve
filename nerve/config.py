"""YAML config loader with local overrides.

Loads config.yaml (committed) and merges config.local.yaml (gitignored secrets) on top.
Supports ~ expansion in paths and environment variable references.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from nerve.houseofagents.config import HouseOfAgentsConfig

import yaml

logger = logging.getLogger(__name__)


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base, returning a new dict."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _expand_path(p: str | None) -> Path | None:
    if p is None:
        return None
    return Path(os.path.expanduser(os.path.expandvars(str(p))))


@dataclass
class SSLConfig:
    cert: Path | None = None
    key: Path | None = None

    @classmethod
    def from_dict(cls, d: dict) -> SSLConfig:
        return cls(cert=_expand_path(d.get("cert")), key=_expand_path(d.get("key")))

    @property
    def enabled(self) -> bool:
        return self.cert is not None and self.key is not None


@dataclass
class GatewayConfig:
    host: str = "0.0.0.0"
    port: int = 8900
    ssl: SSLConfig = field(default_factory=SSLConfig)

    @classmethod
    def from_dict(cls, d: dict) -> GatewayConfig:
        return cls(
            host=d.get("host", "0.0.0.0"),
            port=d.get("port", 8900),
            ssl=SSLConfig.from_dict(d.get("ssl", {})),
        )


@dataclass
class ProviderConfig:
    """LLM provider configuration — controls how Nerve connects to Claude.

    Supported types:
      - "anthropic" (default): Direct Anthropic API or Claude Code proxy.
      - "bedrock": AWS Bedrock. Uses IAM role on EC2/ECS/EKS automatically;
        outside AWS, configure credentials via AWS CLI, env vars, or explicit keys.
    """

    type: str = "anthropic"             # "anthropic" | "bedrock"
    aws_region: str = ""                # Bedrock region (falls back to us-east-1)
    aws_profile: str = ""               # AWS SSO profile name (optional)
    aws_access_key_id: str = ""         # Explicit creds (optional — IAM role preferred)
    aws_secret_access_key: str = ""     # Explicit creds (optional)

    @property
    def is_bedrock(self) -> bool:
        return self.type == "bedrock"

    @classmethod
    def from_dict(cls, d: dict) -> ProviderConfig:
        return cls(
            type=d.get("type", "anthropic"),
            aws_region=d.get("aws_region", ""),
            aws_profile=d.get("aws_profile", ""),
            aws_access_key_id=d.get("aws_access_key_id", ""),
            aws_secret_access_key=d.get("aws_secret_access_key", ""),
        )


@dataclass
class PromptRewriteConfig:
    """First-prompt rewrite — refine the opening message of a new chat.

    When enabled, the web UI offers a toggle in the composer of a new
    (empty) chat. With the toggle on, the first prompt is rewritten and
    shown to the user for approval before anything is sent.
    `enabled` here is the server-side master switch: it controls whether
    the feature is offered at all (the per-user toggle lives in the UI).

    The rewrite defaults to the main chat model (`agent.model`) — the
    rewrite shapes the whole conversation, so quality wins over speed
    here. It runs once per chat and the preview shows progress, so the
    extra latency is acceptable. Set `model` to a fast model (e.g. the
    title model) to trade quality for speed/cost.
    """

    enabled: bool = True
    model: str = ""              # empty → falls back to agent.model
    max_tokens: int = 1024
    timeout_seconds: float = 45.0

    @classmethod
    def from_dict(cls, d: dict) -> PromptRewriteConfig:
        return cls(
            enabled=bool(d.get("enabled", True)),
            model=d.get("model", ""),
            max_tokens=int(d.get("max_tokens", 1024)),
            timeout_seconds=float(d.get("timeout_seconds", 45.0)),
        )


@dataclass
class AgentConfig:
    model: str = "claude-opus-4-8"
    cron_model: str = "claude-sonnet-4-6"
    title_model: str = "claude-haiku-4-5-20251001"  # Session title generation
    max_turns: int = 100
    max_concurrent: int = 4
    thinking: str = "max"       # max, high, medium, low, disabled, adaptive, or number (budget_tokens)
    effort: str = "max"         # max, xhigh, high, medium, low
    context_1m: bool = True     # Enable 1M context window beta
    # Hung-CLI detection: max idle time between SDK messages on a single
    # turn before the engine treats the subprocess as dead and falls into
    # the existing CLI-crash retry path.  Set to 0 to disable (legacy
    # behaviour: turns can hang forever).  900s comfortably covers a 10-min
    # Bash tool call plus SDK round-trips while still catching real hangs.
    cli_idle_timeout_seconds: int = 900
    prompt_rewrite: PromptRewriteConfig = field(default_factory=PromptRewriteConfig)

    @classmethod
    def from_dict(cls, d: dict) -> AgentConfig:
        return cls(
            model=d.get("model", "claude-opus-4-8"),
            cron_model=d.get("cron_model", "claude-sonnet-4-6"),
            title_model=d.get("title_model", "claude-haiku-4-5-20251001"),
            max_turns=d.get("max_turns", 100),
            max_concurrent=d.get("max_concurrent", 4),
            thinking=str(d.get("thinking", "max")),
            effort=str(d.get("effort", "max")),
            context_1m=d.get("context_1m", True),
            cli_idle_timeout_seconds=int(d.get("cli_idle_timeout_seconds", 900)),
            prompt_rewrite=PromptRewriteConfig.from_dict(d.get("prompt_rewrite") or {}),
        )


@dataclass
class TelegramConfig:
    enabled: bool = True
    bot_token: str = ""
    allowed_users: list[int] = field(default_factory=list)
    stream_mode: str = "partial"
    # DM authorization policy:
    #   "pairing" (default) — unknown users may pair with a one-time code
    #                         (`nerve pair`); everyone else is rejected.
    #   "open"              — anyone can talk to the bot. Dangerous: full
    #                         agent access for any Telegram user. A warning
    #                         is logged at startup.
    dm_policy: str = "pairing"

    @classmethod
    def from_dict(cls, d: dict) -> TelegramConfig:
        dm_policy = d.get("dm_policy", "pairing")
        if dm_policy not in ("pairing", "open"):
            logger.warning(
                "telegram.dm_policy %r is not one of ('pairing', 'open') — "
                "falling back to 'pairing'",
                dm_policy,
            )
            dm_policy = "pairing"
        return cls(
            enabled=d.get("enabled", True),
            bot_token=d.get("bot_token", ""),
            allowed_users=[int(u) for u in d.get("allowed_users", []) or []],
            stream_mode=d.get("stream_mode", "partial"),
            dm_policy=dm_policy,
        )


@dataclass
class TelegramSyncConfig:
    enabled: bool = True
    api_id: int = 0
    api_hash: str = ""
    monitored_folders: list[str] = field(default_factory=list)
    exclude_chats: list[int] = field(default_factory=list)
    schedule: str = "*/5 * * * *"
    processor: str = "agent"
    batch_size: int = 50
    prompt_hint: str = ""
    model: str = ""
    condense: bool = False

    @classmethod
    def from_dict(cls, d: dict) -> TelegramSyncConfig:
        return cls(
            enabled=d.get("enabled", True),
            api_id=d.get("api_id", 0),
            api_hash=d.get("api_hash", ""),
            monitored_folders=d.get("monitored_folders", []),
            exclude_chats=d.get("exclude_chats", []),
            schedule=d.get("schedule", "*/5 * * * *"),
            processor=d.get("processor", "agent"),
            batch_size=d.get("batch_size", 50),
            prompt_hint=d.get("prompt_hint", ""),
            model=d.get("model", ""),
            condense=d.get("condense", False),
        )


@dataclass
class GmailSyncConfig:
    enabled: bool = True
    accounts: list[str] = field(default_factory=list)
    schedule: str = "*/15 * * * *"
    keyring_password: str = ""
    processor: str = "agent"
    batch_size: int = 20  # Lower default — each message needs a separate get call
    prompt_hint: str = ""
    model: str = ""
    condense: bool = False
    condense_prompt: str = ""  # Custom prompt for LLM condensation (overrides default)

    @classmethod
    def from_dict(cls, d: dict) -> GmailSyncConfig:
        return cls(
            enabled=d.get("enabled", True),
            accounts=d.get("accounts", []),
            schedule=d.get("schedule", "*/15 * * * *"),
            keyring_password=d.get("keyring_password", ""),
            processor=d.get("processor", "agent"),
            batch_size=d.get("batch_size", 20),
            prompt_hint=d.get("prompt_hint", ""),
            model=d.get("model", ""),
            condense=d.get("condense", False),
            condense_prompt=d.get("condense_prompt", ""),
        )


@dataclass
class GitHubSyncConfig:
    enabled: bool = True
    schedule: str = "*/15 * * * *"
    processor: str = "agent"
    batch_size: int = 30
    prompt_hint: str = ""
    model: str = ""
    condense: bool = False
    # Inbox guardrails — limit which repos reach the inbox (matched on the
    # notification's repo full_name, e.g. "ClickHouse/nerve"). Both support
    # case-insensitive globs. allow_repos is an allowlist (empty = all repos
    # pass); deny_repos is a denylist and takes precedence over allow_repos.
    allow_repos: list[str] = field(default_factory=list)
    deny_repos: list[str] = field(default_factory=list)
    # Actor guardrails — limit which GitHub logins can land a notification in
    # the inbox, matched on the "actors" metadata key (every login involved in
    # the notification: issue/PR author, assignees, comment & review authors).
    # Same semantics as allow_repos/deny_repos — case-insensitive globs, deny
    # wins, and a non-empty allow_actors is fail-closed (a notification with no
    # matching actor is dropped before it reaches the inbox). Empty = all pass.
    allow_actors: list[str] = field(default_factory=list)
    deny_actors: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> GitHubSyncConfig:
        return cls(
            enabled=d.get("enabled", True),
            schedule=d.get("schedule", "*/15 * * * *"),
            processor=d.get("processor", "agent"),
            batch_size=d.get("batch_size", 30),
            prompt_hint=d.get("prompt_hint", ""),
            model=d.get("model", ""),
            condense=d.get("condense", False),
            allow_repos=d.get("allow_repos", []),
            deny_repos=d.get("deny_repos", []),
            allow_actors=d.get("allow_actors", []),
            deny_actors=d.get("deny_actors", []),
        )


@dataclass
class GitHubEventsSyncConfig:
    """Config for GitHub Events source (user's own activity feed)."""
    enabled: bool = False
    schedule: str = "*/15 * * * *"
    repos: list[str] = field(default_factory=list)  # empty = all repos
    username: str = ""  # auto-detect from gh auth if empty
    batch_size: int = 50
    condense: bool = False
    processor: str = "agent"
    prompt_hint: str = ""
    model: str = ""

    @classmethod
    def from_dict(cls, d: dict) -> GitHubEventsSyncConfig:
        return cls(
            enabled=d.get("enabled", False),
            schedule=d.get("schedule", "*/15 * * * *"),
            repos=d.get("repos", []),
            username=d.get("username", ""),
            batch_size=d.get("batch_size", 50),
            condense=d.get("condense", False),
            processor=d.get("processor", "agent"),
            prompt_hint=d.get("prompt_hint", ""),
            model=d.get("model", ""),
        )


@dataclass
class GitHubReposSyncConfig:
    """Config for the GitHub Repos source (monitor watched repos for new issues/PRs).

    Unlike ``github`` (notifications) and ``github_events`` (your own activity),
    this source watches an explicit set of repositories for newly-created issues
    and pull requests. ``repos`` is required — an empty list makes the source a
    no-op.
    """
    enabled: bool = False
    schedule: str = "*/15 * * * *"
    repos: list[str] = field(default_factory=list)  # required; empty = no-op
    batch_size: int = 50
    condense: bool = False
    processor: str = "agent"
    prompt_hint: str = ""
    model: str = ""

    @classmethod
    def from_dict(cls, d: dict) -> GitHubReposSyncConfig:
        return cls(
            enabled=d.get("enabled", False),
            schedule=d.get("schedule", "*/15 * * * *"),
            repos=d.get("repos", []),
            batch_size=d.get("batch_size", 50),
            condense=d.get("condense", False),
            processor=d.get("processor", "agent"),
            prompt_hint=d.get("prompt_hint", ""),
            model=d.get("model", ""),
        )


@dataclass
class CodexOriginConfig:
    """A single Codex thread sync origin.

    Origins represent the transport over which we receive Codex thread
    items — a local rollout directory, a remote app-server, or the
    OpenAI cloud Codex API.
    """

    id: str = "local"
    type: str = "local_rollout"           # local_rollout | app_server | cloud
    enabled: bool = True
    # local_rollout fields
    path: str = "~/.codex/sessions"
    archive_path: str = "~/.codex/archived_sessions"
    poll_interval_seconds: float = 2.0    # How often to scan for new content
    # app_server fields
    transport: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict) -> CodexOriginConfig:
        return cls(
            id=d.get("id", "local"),
            type=d.get("type", "local_rollout"),
            enabled=bool(d.get("enabled", True)),
            path=d.get("path", "~/.codex/sessions"),
            archive_path=d.get("archive_path", "~/.codex/archived_sessions"),
            poll_interval_seconds=float(d.get("poll_interval_seconds", 2.0)),
            transport=d.get("transport", {}),
        )


@dataclass
class CodexWorkspaceFilterConfig:
    """Decides which Codex threads to sync based on ``session_meta.cwd``.

    ``mode``:
      * ``nerve_workspace`` (default) — only threads whose cwd matches
        Nerve's configured workspace.
      * ``explicit`` — only threads whose cwd matches one of
        ``explicit_paths``.
      * ``any`` — sync every thread, regardless of cwd. Not recommended
        unless you really want every Codex session on the box.
    """

    mode: str = "nerve_workspace"
    explicit_paths: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> CodexWorkspaceFilterConfig:
        return cls(
            mode=str(d.get("mode", "nerve_workspace")),
            explicit_paths=list(d.get("explicit_paths", [])),
        )


@dataclass
class CodexSyncConfig:
    """Sync configuration for Codex threads.

    Disabled by default — flip ``enabled=true`` in config.local.yaml once
    the workspace filter is verified to behave as expected on your box.
    """

    enabled: bool = False
    workspace_filter: CodexWorkspaceFilterConfig = field(
        default_factory=CodexWorkspaceFilterConfig,
    )
    origins: list[CodexOriginConfig] = field(default_factory=list)
    store_encrypted_reasoning: bool = True

    @classmethod
    def from_dict(cls, d: dict) -> CodexSyncConfig:
        raw_origins = d.get("origins", [])
        origins = [
            CodexOriginConfig.from_dict(o)
            for o in raw_origins
            if isinstance(o, dict)
        ]
        return cls(
            enabled=bool(d.get("enabled", False)),
            workspace_filter=CodexWorkspaceFilterConfig.from_dict(
                d.get("workspace_filter", {}),
            ),
            origins=origins,
            store_encrypted_reasoning=bool(d.get("store_encrypted_reasoning", True)),
        )


@dataclass
class SyncConfig:
    telegram: TelegramSyncConfig = field(default_factory=TelegramSyncConfig)
    gmail: GmailSyncConfig = field(default_factory=GmailSyncConfig)
    github: GitHubSyncConfig = field(default_factory=GitHubSyncConfig)
    github_events: GitHubEventsSyncConfig = field(default_factory=GitHubEventsSyncConfig)
    github_repos: GitHubReposSyncConfig = field(default_factory=GitHubReposSyncConfig)
    codex: CodexSyncConfig = field(default_factory=CodexSyncConfig)
    message_ttl_days: int = 7           # How long to keep source messages in the inbox
    consumer_cursor_ttl_days: int = 2   # Consumer cursors expire after N days of inactivity

    @classmethod
    def from_dict(cls, d: dict) -> SyncConfig:
        return cls(
            telegram=TelegramSyncConfig.from_dict(d.get("telegram", {})),
            gmail=GmailSyncConfig.from_dict(d.get("gmail", {})),
            github=GitHubSyncConfig.from_dict(d.get("github", {})),
            github_events=GitHubEventsSyncConfig.from_dict(d.get("github_events", {})),
            github_repos=GitHubReposSyncConfig.from_dict(d.get("github_repos", {})),
            codex=CodexSyncConfig.from_dict(d.get("codex", {})),
            message_ttl_days=d.get("message_ttl_days", 7),
            consumer_cursor_ttl_days=d.get("consumer_cursor_ttl_days", 2),
        )


@dataclass
class MemoryCategoryConfig:
    name: str
    description: str

    @classmethod
    def from_dict(cls, d: dict) -> MemoryCategoryConfig:
        return cls(name=d["name"], description=d.get("description", ""))


@dataclass
class MemoryConfig:
    recall_model: str = "claude-sonnet-4-6"  # Recall routing
    memorize_model: str = "claude-sonnet-4-6"  # Extraction & preprocessing
    fast_model: str = "claude-haiku-4-5-20251001"  # Category summaries, date resolution
    embed_model: str = ""
    sqlite_dsn: str = ""
    semantic_dedup_threshold: float = 0.85  # Cosine similarity threshold for semantic dedup
    knowledge_filter: bool = False  # Post-extraction LLM filter for generic knowledge (extra API call)
    categories: list[MemoryCategoryConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> MemoryConfig:
        default_dsn = f"sqlite:///{Path('~/.nerve/memu.sqlite').expanduser()}"
        raw_cats = d.get("categories", [])
        categories = [MemoryCategoryConfig.from_dict(c) for c in raw_cats]
        return cls(
            recall_model=d.get("recall_model", "claude-sonnet-4-6"),
            memorize_model=d.get("memorize_model", "claude-sonnet-4-6"),
            fast_model=d.get("fast_model", "claude-haiku-4-5-20251001"),
            embed_model=d.get("embed_model", ""),
            sqlite_dsn=d.get("sqlite_dsn", default_dsn),
            semantic_dedup_threshold=float(d.get("semantic_dedup_threshold", 0.85)),
            knowledge_filter=bool(d.get("knowledge_filter", False)),
            categories=categories,
        )


@dataclass
class CronConfig:
    jobs_file: Path = field(default_factory=lambda: Path("~/.nerve/cron/jobs.yaml"))
    system_file: Path = field(default_factory=lambda: Path("~/.nerve/cron/system.yaml"))

    @classmethod
    def from_dict(cls, d: dict) -> CronConfig:
        return cls(
            jobs_file=_expand_path(d.get("jobs_file", "~/.nerve/cron/jobs.yaml")) or Path("~/.nerve/cron/jobs.yaml"),
            system_file=_expand_path(d.get("system_file", "~/.nerve/cron/system.yaml")) or Path("~/.nerve/cron/system.yaml"),
        )


@dataclass
class BackupConfig:
    """Scheduled backup of Nerve state to a local directory.

    Opt-in: set ``target_dir`` to an external mount or a synced directory
    (the off-box copy is what protects against a disk failure) and flip
    ``enabled`` on. A bundle is a single ``nerve-backup-<host>-<ts>.tar.zst``
    file produced by :mod:`nerve.backup`. The scheduled task notifies on
    failure (silent backups that fail are worse than none).
    """

    enabled: bool = False            # opt-in; set target_dir first
    target_dir: str = ""             # e.g. /mnt/backup/nerve or a synced dir
    interval_hours: int = 24
    retention_count: int = 7
    include_workspace: bool = True
    workspace_excludes: list[str] = field(default_factory=list)  # extra globs
    notify_on_failure: bool = True   # high-priority notify
    notify_on_success: bool = False  # low-priority digest line

    @classmethod
    def from_dict(cls, d: dict) -> BackupConfig:
        return cls(
            enabled=bool(d.get("enabled", False)),
            target_dir=d.get("target_dir", ""),
            interval_hours=int(d.get("interval_hours", 24)),
            retention_count=int(d.get("retention_count", 7)),
            include_workspace=bool(d.get("include_workspace", True)),
            workspace_excludes=list(d.get("workspace_excludes", []) or []),
            notify_on_failure=bool(d.get("notify_on_failure", True)),
            notify_on_success=bool(d.get("notify_on_success", False)),
        )


@dataclass
class SessionsConfig:
    archive_after_days: int = 30
    max_sessions: int = 500
    cron_session_mode: str = "per_run"  # "per_run" or "reuse"
    memorize_interval_minutes: int = 30  # Background memorization sweep interval
    sticky_period_minutes: int = 120  # Reuse session if active within this window
    client_idle_timeout_minutes: int = 60  # Auto-disconnect clients idle longer than this (0 = disabled)

    @classmethod
    def from_dict(cls, d: dict) -> SessionsConfig:
        return cls(
            archive_after_days=d.get("archive_after_days", 30),
            max_sessions=d.get("max_sessions", 500),
            cron_session_mode=d.get("cron_session_mode", "per_run"),
            memorize_interval_minutes=d.get("memorize_interval_minutes", 30),
            sticky_period_minutes=d.get("sticky_period_minutes", 120),
            client_idle_timeout_minutes=d.get("client_idle_timeout_minutes", 60),
        )


@dataclass
class RetentionConfig:
    """Opt-in nerve.db retention: message compaction + telemetry pruning.

    Disabled by default so an upstream merge mutates no existing user's data;
    the operator opts in locally. When enabled, a background pass every
    ``interval_hours`` drops the verbose ``blocks``/``thinking`` JSON of old,
    already-memorized, non-starred, non-active messages (keeping ``content``),
    prunes append-only telemetry + file snapshots older than
    ``retention_days``, and checkpoints the WAL. The file is only shrunk by the
    explicit ``nerve db vacuum`` command (VACUUM takes a write lock).

    ``retention_full_days`` is the message-compaction window (default 30);
    ``retention_days`` is the telemetry/snapshot window (default 90). Both
    ints are clamped ``>= 1``.
    """

    enabled: bool = False
    retention_days: int = 90
    retention_full_days: int = 30
    interval_hours: int = 24

    @classmethod
    def from_dict(cls, d: dict) -> RetentionConfig:
        return cls(
            enabled=bool(d.get("enabled", False)),
            retention_days=max(1, int(d.get("retention_days", 90))),
            retention_full_days=max(1, int(d.get("retention_full_days", 30))),
            interval_hours=max(1, int(d.get("interval_hours", 24))),
        )


@dataclass
class AuthConfig:
    password_hash: str = ""
    jwt_secret: str = ""

    @classmethod
    def from_dict(cls, d: dict) -> AuthConfig:
        return cls(
            password_hash=d.get("password_hash", ""),
            jwt_secret=d.get("jwt_secret", ""),
        )


@dataclass
class NotificationsConfig:
    """Async notification delivery settings."""
    channels: list[str] = field(default_factory=lambda: ["web", "telegram"])
    telegram_chat_id: int | None = None       # Target chat; falls back to first allowed_user
    default_expiry_hours: int = 48            # Auto-expire unanswered questions
    priority_prefixes: dict[str, str] = field(default_factory=lambda: {
        "high": "⚠️ ",
        "urgent": "🚨 ",
    })

    @classmethod
    def from_dict(cls, d: dict) -> NotificationsConfig:
        return cls(
            channels=d.get("channels", ["web", "telegram"]),
            telegram_chat_id=d.get("telegram_chat_id"),
            default_expiry_hours=d.get("default_expiry_hours", 48),
            priority_prefixes=d.get("priority_prefixes", {
                "high": "⚠️ ",
                "urgent": "🚨 ",
            }),
        )


@dataclass
class ChannelsConfig:
    """Global channel settings."""

    @classmethod
    def from_dict(cls, d: dict) -> ChannelsConfig:
        return cls()


@dataclass
class DockerConfig:
    """Docker deployment settings."""

    extra_mounts: list[str] = field(default_factory=list)  # e.g. ["~/code:/code"]

    @classmethod
    def from_dict(cls, d: dict) -> DockerConfig:
        return cls(
            extra_mounts=d.get("extra_mounts", []),
        )


@dataclass
class ProxyConfig:
    """CLIProxyAPI — optional local proxy for routing API calls through Claude Code OAuth."""

    enabled: bool = False
    port: int = 8317
    host: str = "127.0.0.1"
    binary_path: Path = field(default_factory=lambda: Path("~/.nerve/bin/cli-proxy-api"))
    auth_dir: Path = field(default_factory=lambda: Path("~/.nerve/cli-proxy-auth"))
    api_key: str = "sk-nerve-local-proxy"   # local-only auth between Nerve and the proxy
    log_file: Path = field(default_factory=lambda: Path("~/.nerve/proxy.log"))

    @classmethod
    def from_dict(cls, d: dict) -> ProxyConfig:
        return cls(
            enabled=d.get("enabled", False),
            port=d.get("port", 8317),
            host=d.get("host", "127.0.0.1"),
            binary_path=_expand_path(d.get("binary_path", "~/.nerve/bin/cli-proxy-api")) or Path("~/.nerve/bin/cli-proxy-api"),
            auth_dir=_expand_path(d.get("auth_dir", "~/.nerve/cli-proxy-auth")) or Path("~/.nerve/cli-proxy-auth"),
            api_key=d.get("api_key", "sk-nerve-local-proxy"),
            log_file=_expand_path(d.get("log_file", "~/.nerve/proxy.log")) or Path("~/.nerve/proxy.log"),
        )


@dataclass
class McpEndpointConfig:
    """Nerve's own MCP server endpoint (Nerve-as-MCP-server).

    Exposes the Nerve tool registry to external MCP clients (Codex,
    Claude Code, Cursor) over Streamable HTTP, mounted at ``path`` inside
    the gateway. Off by default; flip ``enabled=true`` in config.local.yaml
    to advertise the endpoint. Authenticates with the existing JWT
    (``config.auth.jwt_secret``) — same token mechanism as the web UI.

    Not to be confused with :class:`McpServerConfig`, which configures
    *external* MCP servers that Nerve connects to as a client.
    """

    enabled: bool = False
    path: str = "/mcp/v1"
    include_hoa: bool = False   # Expose HouseOfAgents tools to external clients

    @classmethod
    def from_dict(cls, d: dict) -> McpEndpointConfig:
        return cls(
            enabled=bool(d.get("enabled", False)),
            path=str(d.get("path", "/mcp/v1")),
            include_hoa=bool(d.get("include_hoa", False)),
        )


@dataclass
class ExternalAgentTargetConfig:
    """One configured external agent (Codex, Claude Code, ...).

    Populated by the bootstrap wizard's ``_step_external_agents`` step
    and read by :class:`nerve.external_agents.sync_service.SyncService`
    every interval to keep the agent's memory files in sync with the
    workspace identity files.

    ``token`` is the bearer JWT the agent uses to authenticate against
    the local MCP endpoint. It's written into the agent's config once
    at bootstrap (e.g. into ``~/.codex/config.toml``); the sync service
    does NOT rewrite the config file, only the memory bundle.
    """

    name: str                                  # registry key: "codex" | "claude-code" | ...
    enabled: bool = True
    token: str = ""                            # bearer JWT for MCP auth (informational only)

    @classmethod
    def from_dict(cls, d: dict) -> ExternalAgentTargetConfig:
        return cls(
            name=str(d.get("name", "")),
            enabled=bool(d.get("enabled", True)),
            token=str(d.get("token", "")),
        )

    def to_dict(self) -> dict:
        return {"name": self.name, "enabled": self.enabled, "token": self.token}


@dataclass
class ExternalAgentsConfig:
    """Configuration for the external-agents bootstrap + sync subsystem.

    The bootstrap wizard writes one :class:`ExternalAgentTargetConfig`
    per agent selected, plus the global conflict policy chosen for
    pre-existing files. The sync service iterates ``targets`` every
    ``sync_interval_minutes`` and re-renders that agent's memory
    bundle when any source file changes.

    ``conflict_policy`` controls how :class:`nerve.external_agents.writer.ConfigWriter`
    handles paths that already exist when the wizard's apply step runs:
    ``backup`` (default) saves a ``.nerve-backup-<ts>`` copy then
    overwrites; ``skip`` leaves the existing file alone; ``merge`` is
    only meaningful for JSON files (used by Claude Code's settings.json).
    """

    enabled: bool = True
    sync_interval_minutes: int = 15
    conflict_policy: str = "backup"            # "backup" | "skip" | "merge"
    targets: list[ExternalAgentTargetConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> ExternalAgentsConfig:
        raw_targets = d.get("targets", [])
        targets: list[ExternalAgentTargetConfig] = []
        if isinstance(raw_targets, list):
            for raw in raw_targets:
                if isinstance(raw, dict) and raw.get("name"):
                    targets.append(ExternalAgentTargetConfig.from_dict(raw))
        return cls(
            enabled=bool(d.get("enabled", True)),
            sync_interval_minutes=int(d.get("sync_interval_minutes", 15)),
            conflict_policy=str(d.get("conflict_policy", "backup")),
            targets=targets,
        )


@dataclass
class McpServerConfig:
    """External MCP server configuration.

    Supports stdio (command + args + env), SSE (url + headers),
    and HTTP (url + headers) transports.  Dict-based YAML format
    allows _deep_merge to correctly overlay secrets from config.local.yaml.
    """

    name: str
    type: str = "stdio"                                    # stdio | sse | http
    enabled: bool = True
    # stdio fields
    command: str = ""
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    # sse / http fields
    url: str = ""
    headers: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, name: str, d: dict) -> McpServerConfig:
        return cls(
            name=name,
            type=d.get("type", "stdio"),
            enabled=d.get("enabled", True),
            command=d.get("command", ""),
            args=d.get("args", []),
            env=d.get("env", {}),
            url=d.get("url", ""),
            headers=d.get("headers", {}),
        )

    def to_sdk_config(self) -> dict:
        """Convert to Claude Agent SDK McpServerConfig dict."""
        if self.type == "stdio":
            cfg: dict = {"command": self.command}
            if self.args:
                cfg["args"] = self.args
            if self.env:
                cfg["env"] = self.env
            return cfg
        elif self.type in ("sse", "http"):
            cfg = {"type": self.type, "url": self.url}
            if self.headers:
                cfg["headers"] = self.headers
            return cfg
        raise ValueError(f"Unknown MCP server type: {self.type}")


def _parse_mcp_servers(d: dict) -> list[McpServerConfig]:
    """Parse the mcp_servers dict from merged YAML config."""
    raw = d.get("mcp_servers", {})
    if not isinstance(raw, dict):
        return []
    return [McpServerConfig.from_dict(name, cfg) for name, cfg in raw.items()
            if isinstance(cfg, dict)]


def _get_enabled_claude_code_plugins(
    claude_dir: Path | None = None,
) -> list[tuple[str, Path]]:
    """Find enabled Claude Code plugin directories.

    Returns list of (plugin_key, plugin_dir) tuples for each enabled plugin
    that has a cached installation with .mcp.json.
    """
    if claude_dir is None:
        claude_dir = Path.home() / ".claude"

    settings_path = claude_dir / "settings.json"
    if not settings_path.exists():
        return []

    try:
        settings = json.loads(settings_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.debug("Could not read Claude Code settings: %s", e)
        return []

    enabled_plugins: dict = settings.get("enabledPlugins", {})
    if not isinstance(enabled_plugins, dict):
        return []

    plugins_dir = claude_dir / "plugins"
    result: list[tuple[str, Path]] = []

    for plugin_key, is_enabled in enabled_plugins.items():
        if not is_enabled:
            continue

        # Key format: "name@marketplace"
        parts = plugin_key.split("@", 1)
        if len(parts) != 2:
            logger.debug("Skipping malformed plugin key: %s", plugin_key)
            continue
        name, marketplace = parts

        plugin_dir = _find_plugin_dir(plugins_dir, marketplace, name)
        if plugin_dir is None:
            logger.debug("No plugin dir found for %s", plugin_key)
            continue

        result.append((plugin_key, plugin_dir))

    return result


def load_claude_code_plugins(
    claude_dir: Path | None = None,
) -> list[dict[str, str]]:
    """Return SDK-compatible plugin configs for enabled Claude Code plugins.

    Each entry is ``{"type": "local", "path": "<dir>"}`` suitable for
    ``ClaudeAgentOptions.plugins``.
    """
    plugins = _get_enabled_claude_code_plugins(claude_dir)
    result: list[dict[str, str]] = []
    for plugin_key, plugin_dir in plugins:
        logger.debug("Claude Code plugin %s → %s", plugin_key, plugin_dir)
        result.append({"type": "local", "path": str(plugin_dir)})
    return result


def _find_plugin_dir(
    plugins_dir: Path, marketplace: str, name: str,
) -> Path | None:
    """Locate the directory of a Claude Code plugin.

    Checks cache/ (installed plugins with versioned dirs) first,
    then falls back to marketplaces/ (external plugin definitions).
    """
    # Cache: ~/.claude/plugins/cache/<marketplace>/<name>/<version>/
    cache_dir = plugins_dir / "cache" / marketplace / name
    if cache_dir.is_dir():
        versions = sorted(
            (d for d in cache_dir.iterdir() if d.is_dir()),
            key=lambda d: d.name,
            reverse=True,
        )
        for v in versions:
            if (v / ".mcp.json").exists():
                return v

    # Marketplace: external_plugins/<name>/
    ext_dir = plugins_dir / "marketplaces" / marketplace / "external_plugins" / name
    if (ext_dir / ".mcp.json").exists():
        return ext_dir

    # Marketplace: plugins/<name>/
    plugin_dir = plugins_dir / "marketplaces" / marketplace / "plugins" / name
    if (plugin_dir / ".mcp.json").exists():
        return plugin_dir

    return None


_DEFAULT_LANGFUSE_REDACT_PATTERNS: tuple[str, ...] = (
    r"sk-ant-[A-Za-z0-9_\-]{20,}",
    r"pk-lf-[A-Za-z0-9_\-]{20,}",
    r"sk-lf-[A-Za-z0-9_\-]{20,}",
    r"\$2[aby]\$\d{2}\$[./A-Za-z0-9]{53}",
)


@dataclass
class LangfuseConfig:
    """Langfuse observability — optional. Activated by setting both keys.

    With ``public_key`` and ``secret_key`` configured Nerve traces the agent
    loop and memU LLM calls into the Langfuse project pointed at by ``host``.
    Empty keys = no-op, zero overhead, no SDK calls.
    """

    public_key: str = ""
    secret_key: str = ""
    host: str = "https://cloud.langfuse.com"
    redact_patterns: list[str] = field(
        default_factory=lambda: list(_DEFAULT_LANGFUSE_REDACT_PATTERNS),
    )

    @classmethod
    def from_dict(cls, d: dict) -> "LangfuseConfig":
        return cls(
            public_key=d.get("public_key", ""),
            secret_key=d.get("secret_key", ""),
            host=d.get("host", "https://cloud.langfuse.com"),
            redact_patterns=list(
                d.get("redact_patterns", _DEFAULT_LANGFUSE_REDACT_PATTERNS),
            ),
        )


@dataclass
class XmemoryConfig:
    """xmemory.ai structured memory — optional, runs alongside memU.

    Activated only when both ``api_key`` (the bearer token) and
    ``instance_id`` are set. When active, the ``memorize`` tool dual-writes
    to xmemory (async) and ``memory_recall`` appends xmemory's synthesized
    answer to the memU results. The memorization sweep stays memU-only.

    Empty keys = no-op, zero overhead, no SDK calls. The instance and its
    schema are created out of band (by the operator) on xmemory's side.
    """

    api_key: str = ""
    instance_id: str = ""
    api_url: str = "https://api.xmemory.ai"
    extraction_logic: str = "deep"  # "deep" (default) or "fast"
    timeout: float = 60.0

    @property
    def enabled(self) -> bool:
        """True only when both the token and an instance are configured."""
        return bool(self.api_key and self.instance_id)

    @classmethod
    def from_dict(cls, d: dict) -> "XmemoryConfig":
        return cls(
            api_key=d.get("api_key", ""),
            instance_id=d.get("instance_id", ""),
            api_url=d.get("api_url", "https://api.xmemory.ai"),
            extraction_logic=d.get("extraction_logic", "deep"),
            timeout=float(d.get("timeout", 60.0)),
        )


@dataclass
class NerveConfig:
    workspace: Path = field(default_factory=lambda: Path("~/nerve-workspace"))
    timezone: str = "America/New_York"
    deployment: str = "server"            # "server" or "docker"
    quiet_start: str = "02:00"            # HH:MM — start of quiet period (local timezone)
    quiet_end: str = "08:00"              # HH:MM — end of quiet period (local timezone)
    provider: ProviderConfig = field(default_factory=ProviderConfig)
    gateway: GatewayConfig = field(default_factory=GatewayConfig)
    agent: AgentConfig = field(default_factory=AgentConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    sync: SyncConfig = field(default_factory=SyncConfig)
    memory: MemoryConfig = field(default_factory=MemoryConfig)
    cron: CronConfig = field(default_factory=CronConfig)
    backup: BackupConfig = field(default_factory=BackupConfig)
    sessions: SessionsConfig = field(default_factory=SessionsConfig)
    retention: RetentionConfig = field(default_factory=RetentionConfig)
    auth: AuthConfig = field(default_factory=AuthConfig)
    channels: ChannelsConfig = field(default_factory=ChannelsConfig)
    notifications: NotificationsConfig = field(default_factory=NotificationsConfig)
    docker: DockerConfig = field(default_factory=DockerConfig)
    proxy: ProxyConfig = field(default_factory=ProxyConfig)
    houseofagents: HouseOfAgentsConfig = field(default_factory=HouseOfAgentsConfig)
    langfuse: LangfuseConfig = field(default_factory=LangfuseConfig)
    xmemory: XmemoryConfig = field(default_factory=XmemoryConfig)
    mcp_endpoint: McpEndpointConfig = field(default_factory=McpEndpointConfig)
    mcp_servers: list[McpServerConfig] = field(default_factory=list)
    external_agents: ExternalAgentsConfig = field(default_factory=ExternalAgentsConfig)

    # API keys (from config.local.yaml)
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    brave_search_api_key: str = ""

    # Where this config was loaded from (set by load_config, not a YAML key).
    # Used by anything that needs to write back (e.g. Telegram pairing
    # persisting allowed_users to config.local.yaml).
    config_dir: Path = field(default_factory=Path.cwd)

    @property
    def anthropic_api_base_url(self) -> str:
        """Effective Anthropic API base URL — proxy or direct."""
        if self.provider.is_bedrock:
            return ""  # Bedrock doesn't use Anthropic base URL
        if self.proxy.enabled:
            return f"http://{self.proxy.host}:{self.proxy.port}/v1/"
        return "https://api.anthropic.com/v1/"

    @property
    def effective_api_key(self) -> str:
        """Effective API key — proxy's local key or real Anthropic key."""
        if self.provider.is_bedrock:
            return ""  # Bedrock uses IAM, not API keys
        if self.proxy.enabled:
            return self.proxy.api_key
        return self.anthropic_api_key

    def create_anthropic_client(self, timeout: float = 60.0) -> Any:
        """Create an Anthropic client based on the configured provider.

        Returns AnthropicBedrock when provider is "bedrock", otherwise
        a standard Anthropic client using the effective API key and base URL.
        """
        import anthropic

        if self.provider.is_bedrock:
            from anthropic import AnthropicBedrock
            kwargs: dict[str, Any] = {"timeout": timeout}
            if self.provider.aws_region:
                kwargs["aws_region"] = self.provider.aws_region
            if self.provider.aws_profile:
                kwargs["aws_profile"] = self.provider.aws_profile
            if self.provider.aws_access_key_id:
                kwargs["aws_access_key"] = self.provider.aws_access_key_id
                kwargs["aws_secret_key"] = self.provider.aws_secret_access_key
            return AnthropicBedrock(**kwargs)

        # Default: direct Anthropic API (or proxy)
        base_url = self.anthropic_api_base_url.rstrip("/")
        if base_url.endswith("/v1"):
            base_url = base_url[:-3]
        return anthropic.Anthropic(
            api_key=self.effective_api_key,
            base_url=base_url,
            timeout=timeout,
        )

    def create_async_anthropic_client(self, timeout: float = 60.0) -> Any:
        """Create an async Anthropic client based on the configured provider.

        Returns AsyncAnthropicBedrock when provider is "bedrock", otherwise
        a standard AsyncAnthropic client.
        """
        import anthropic

        if self.provider.is_bedrock:
            from anthropic import AsyncAnthropicBedrock
            kwargs: dict[str, Any] = {"timeout": timeout}
            if self.provider.aws_region:
                kwargs["aws_region"] = self.provider.aws_region
            if self.provider.aws_profile:
                kwargs["aws_profile"] = self.provider.aws_profile
            if self.provider.aws_access_key_id:
                kwargs["aws_access_key"] = self.provider.aws_access_key_id
                kwargs["aws_secret_key"] = self.provider.aws_secret_access_key
            return AsyncAnthropicBedrock(**kwargs)

        base_url = self.anthropic_api_base_url.rstrip("/")
        if base_url.endswith("/v1"):
            base_url = base_url[:-3]
        return anthropic.AsyncAnthropic(
            api_key=self.effective_api_key,
            base_url=base_url,
            timeout=timeout,
        )

    @classmethod
    def from_dict(cls, d: dict) -> NerveConfig:
        return cls(
            workspace=_expand_path(d.get("workspace", "~/nerve-workspace")) or Path("~/nerve-workspace"),
            timezone=d.get("timezone", "America/New_York"),
            deployment=d.get("deployment", "server"),
            quiet_start=d.get("quiet_start", "02:00"),
            quiet_end=d.get("quiet_end", "08:00"),
            provider=ProviderConfig.from_dict(d.get("provider", {})),
            gateway=GatewayConfig.from_dict(d.get("gateway", {})),
            agent=AgentConfig.from_dict(d.get("agent", {})),
            telegram=TelegramConfig.from_dict(d.get("telegram", {})),
            sync=SyncConfig.from_dict(d.get("sync", {})),
            memory=MemoryConfig.from_dict(d.get("memory", {})),
            cron=CronConfig.from_dict(d.get("cron", {})),
            backup=BackupConfig.from_dict(d.get("backup", {})),
            sessions=SessionsConfig.from_dict(d.get("sessions", {})),
            retention=RetentionConfig.from_dict(d.get("retention", {})),
            auth=AuthConfig.from_dict(d.get("auth", {})),
            channels=ChannelsConfig.from_dict(d.get("channels", {})),
            notifications=NotificationsConfig.from_dict(d.get("notifications", {})),
            docker=DockerConfig.from_dict(d.get("docker", {})),
            proxy=ProxyConfig.from_dict(d.get("proxy", {})),
            houseofagents=HouseOfAgentsConfig.from_dict(d.get("houseofagents", {})),
            langfuse=LangfuseConfig.from_dict(d.get("langfuse", {})),
            xmemory=XmemoryConfig.from_dict(d.get("xmemory", {})),
            mcp_endpoint=McpEndpointConfig.from_dict(d.get("mcp_endpoint", {})),
            mcp_servers=_parse_mcp_servers(d),
            external_agents=ExternalAgentsConfig.from_dict(d.get("external_agents", {})),
            anthropic_api_key=d.get("anthropic_api_key", ""),
            openai_api_key=d.get("openai_api_key", ""),
            brave_search_api_key=d.get("brave_search_api_key", ""),
        )


def load_mcp_servers(config_dir: Path | None = None) -> list[McpServerConfig]:
    """Re-read MCP server configs from YAML files.

    Called per session creation and on reload to pick up config changes
    without restarting Nerve.

    Note: Claude Code plugin MCPs are handled separately via the SDK
    ``plugins`` field (--plugin-dir), not through this function.
    """
    if config_dir is None:
        config_dir = Path.cwd()

    base_path = config_dir / "config.yaml"
    local_path = config_dir / "config.local.yaml"

    base: dict[str, Any] = {}
    if base_path.exists():
        with open(base_path) as f:
            base = yaml.safe_load(f) or {}

    local: dict[str, Any] = {}
    if local_path.exists():
        with open(local_path) as f:
            local = yaml.safe_load(f) or {}

    merged = _deep_merge(base, local)
    return _parse_mcp_servers(merged)


# --- Config directory resolution ---
#
# Nerve commands used to be CWD-sensitive: running `nerve start` from any
# directory other than the install dir silently loaded an empty config and
# reported "fresh install".  Resolution now follows a waterfall so commands
# work from anywhere:
#
#   1. Explicit --config-dir / -c flag
#   2. NERVE_CONFIG_DIR environment variable
#   3. Current directory, if it contains config.yaml or config.local.yaml
#      (preserves the dev workflow of running nerve from a checkout)
#   4. The pointer file ~/.nerve/config_dir (written by `nerve init` and on
#      daemon start), if it names a directory that still has config files
#   5. Current directory (fresh-install fallback)

CONFIG_POINTER_FILE = Path("~/.nerve/config_dir")


def _has_config_files(directory: Path) -> bool:
    """True if the directory contains config.yaml or config.local.yaml."""
    try:
        return (directory / "config.yaml").exists() or (
            directory / "config.local.yaml"
        ).exists()
    except OSError:
        return False


def read_config_pointer() -> Path | None:
    """Read the persisted config directory pointer. None if absent/invalid."""
    try:
        raw = CONFIG_POINTER_FILE.expanduser().read_text(encoding="utf-8").strip()
    except (FileNotFoundError, OSError):
        return None
    if not raw:
        return None
    p = Path(raw)
    return p if p.is_dir() else None


def write_config_pointer(config_dir: Path) -> None:
    """Persist the config directory so future commands find it from any CWD.

    Written by `nerve init` (after a successful apply) and on daemon start.
    Best-effort: failure to write must never break the caller.
    """
    try:
        pointer = CONFIG_POINTER_FILE.expanduser()
        pointer.parent.mkdir(parents=True, exist_ok=True)
        pointer.write_text(str(Path(config_dir).expanduser().resolve()), encoding="utf-8")
    except OSError as e:
        logger.warning("Could not write config pointer %s: %s", CONFIG_POINTER_FILE, e)


def resolve_config_dir(explicit: str | Path | None = None) -> tuple[Path, str]:
    """Resolve the effective config directory.

    Returns (directory, source) where source is one of:
    "flag", "env", "cwd", "pointer", "default".
    """
    if explicit is not None:
        return Path(explicit).expanduser(), "flag"

    env_dir = os.environ.get("NERVE_CONFIG_DIR", "").strip()
    if env_dir:
        return Path(env_dir).expanduser(), "env"

    cwd = Path.cwd()
    if _has_config_files(cwd):
        return cwd, "cwd"

    pointer = read_config_pointer()
    if pointer is not None and _has_config_files(pointer):
        return pointer, "pointer"

    return cwd, "default"


def load_config(config_dir: Path | None = None) -> NerveConfig:
    """Load config from config.yaml + config.local.yaml in the given directory.

    If config_dir is None, the directory is resolved via the waterfall in
    :func:`resolve_config_dir` (flag/env/cwd/pointer), so commands behave the
    same regardless of the caller's working directory.
    """
    if config_dir is None:
        config_dir, _source = resolve_config_dir()

    base_path = config_dir / "config.yaml"
    local_path = config_dir / "config.local.yaml"

    base: dict[str, Any] = {}
    if base_path.exists():
        with open(base_path) as f:
            base = yaml.safe_load(f) or {}

    local: dict[str, Any] = {}
    if local_path.exists():
        with open(local_path) as f:
            local = yaml.safe_load(f) or {}

    merged = _deep_merge(base, local)

    # Surface typos and stale keys instead of silently ignoring them.
    for warning in validate_config_keys(merged):
        logger.warning("config: %s", warning)

    config = NerveConfig.from_dict(merged)
    config.config_dir = Path(config_dir)
    return config


# --- Unknown-key validation ---

# YAML keys that are intentionally not dataclass fields — keyed by dotted
# prefix ("" is the top level). claude_oauth_token / github_token are read
# from config.local.yaml by the Docker entrypoint, not by NerveConfig.
_EXTRA_ALLOWED_KEYS: dict[str, set[str]] = {
    "": {"claude_oauth_token", "github_token"},
}

# Subtrees we don't descend into: free-form mappings or lists of mappings
# whose schema isn't a nested dataclass.
_OPAQUE_PREFIXES = {
    "mcp_servers",
    "memory.categories",
    "external_agents.targets",
    "docker.extra_mounts",
    "langfuse.redact_patterns",
}


def validate_config_keys(merged: dict) -> list[str]:
    """Compare a merged config dict against the NerveConfig dataclass tree.

    Returns human-readable warnings for keys that no dataclass field will
    ever read (typos, removed options). Warning-only by design — unknown
    keys must not break startup (forward/backward compatibility).
    """
    import dataclasses

    warnings: list[str] = []

    def _walk(d: dict, cls: type, prefix: str) -> None:
        field_map = {f.name: f for f in dataclasses.fields(cls)}
        allowed_extra = _EXTRA_ALLOWED_KEYS.get(prefix, set())
        for key, value in d.items():
            dotted = f"{prefix}.{key}" if prefix else key
            if key not in field_map:
                if key in allowed_extra:
                    continue
                warnings.append(
                    f"unknown key '{dotted}' — it is ignored (typo or removed option?)"
                )
                continue
            if dotted in _OPAQUE_PREFIXES:
                continue
            # Descend into nested dataclasses only
            ftype = field_map[key].type
            nested = _resolve_dataclass(ftype)
            if nested is not None and isinstance(value, dict):
                _walk(value, nested, dotted)

    def _resolve_dataclass(ftype: Any) -> type | None:
        """Map a (possibly string) field annotation to a dataclass type."""
        if isinstance(ftype, type) and dataclasses.is_dataclass(ftype):
            return ftype
        if isinstance(ftype, str):
            candidate = globals().get(ftype)
            if candidate is None and ftype == "HouseOfAgentsConfig":
                candidate = HouseOfAgentsConfig
            if isinstance(candidate, type) and dataclasses.is_dataclass(candidate):
                return candidate
        return None

    _walk(merged, NerveConfig, "")
    return warnings


# --- Write-back helpers ---


def append_telegram_allowed_user(config_dir: Path, user_id: int) -> bool:
    """Append a Telegram user ID to telegram.allowed_users in config.local.yaml.

    Used by the pairing flow. Reads, merges, and rewrites the local config
    (config.local.yaml is generated — comment loss is acceptable there).
    Returns True if the file was updated (False if the ID was already present).
    """
    local_path = Path(config_dir) / "config.local.yaml"
    data: dict[str, Any] = {}
    if local_path.exists():
        try:
            data = yaml.safe_load(local_path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as e:
            logger.error("Cannot parse %s to persist pairing: %s", local_path, e)
            return False

    telegram = data.setdefault("telegram", {})
    users = telegram.setdefault("allowed_users", [])
    if user_id in users:
        return False
    users.append(user_id)

    with open(local_path, "w", encoding="utf-8") as f:
        f.write("# Nerve — Secrets (gitignored)\n")
        f.write("# API keys, tokens, and other sensitive configuration.\n\n")
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
    try:
        os.chmod(local_path, 0o600)
    except OSError:
        pass
    logger.info("Persisted Telegram user %d to %s", user_id, local_path)
    return True


# Singleton config instance, loaded lazily
_config: NerveConfig | None = None


def get_config() -> NerveConfig:
    """Get the global config instance. Loads from CWD on first call."""
    global _config
    if _config is None:
        _config = load_config()
    return _config


def set_config(config: NerveConfig) -> None:
    """Override the global config (for testing or CLI-driven loading)."""
    global _config
    _config = config
