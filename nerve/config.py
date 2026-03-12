"""YAML config loader with local overrides.

Loads config.yaml (committed) and merges config.local.yaml (gitignored secrets) on top.
Supports ~ expansion in paths and environment variable references.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


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
class AgentConfig:
    model: str = "claude-opus-4-6"
    cron_model: str = "claude-sonnet-4-6"
    max_turns: int = 50
    max_concurrent: int = 4
    thinking: str = "max"       # max, high, medium, low, disabled, adaptive, or number (budget_tokens)
    effort: str = "max"         # max, high, medium, low
    context_1m: bool = True     # Enable 1M context window beta

    @classmethod
    def from_dict(cls, d: dict) -> AgentConfig:
        return cls(
            model=d.get("model", "claude-opus-4-6"),
            cron_model=d.get("cron_model", "claude-sonnet-4-6"),
            max_turns=d.get("max_turns", 50),
            max_concurrent=d.get("max_concurrent", 4),
            thinking=str(d.get("thinking", "max")),
            effort=str(d.get("effort", "max")),
            context_1m=d.get("context_1m", True),
        )


@dataclass
class TelegramConfig:
    enabled: bool = True
    bot_token: str = ""
    allowed_users: list[int] = field(default_factory=list)
    stream_mode: str = "partial"

    @classmethod
    def from_dict(cls, d: dict) -> TelegramConfig:
        return cls(
            enabled=d.get("enabled", True),
            bot_token=d.get("bot_token", ""),
            allowed_users=d.get("allowed_users", []),
            stream_mode=d.get("stream_mode", "partial"),
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
class SyncConfig:
    telegram: TelegramSyncConfig = field(default_factory=TelegramSyncConfig)
    gmail: GmailSyncConfig = field(default_factory=GmailSyncConfig)
    github: GitHubSyncConfig = field(default_factory=GitHubSyncConfig)
    github_events: GitHubEventsSyncConfig = field(default_factory=GitHubEventsSyncConfig)
    message_ttl_days: int = 7           # How long to keep source messages in the inbox
    consumer_cursor_ttl_days: int = 2   # Consumer cursors expire after N days of inactivity

    @classmethod
    def from_dict(cls, d: dict) -> SyncConfig:
        return cls(
            telegram=TelegramSyncConfig.from_dict(d.get("telegram", {})),
            gmail=GmailSyncConfig.from_dict(d.get("gmail", {})),
            github=GitHubSyncConfig.from_dict(d.get("github", {})),
            github_events=GitHubEventsSyncConfig.from_dict(d.get("github_events", {})),
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
    embed_model: str = "text-embedding-3-small"
    sqlite_dsn: str = ""
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
            embed_model=d.get("embed_model", "text-embedding-3-small"),
            sqlite_dsn=d.get("sqlite_dsn", default_dsn),
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

    @classmethod
    def from_dict(cls, d: dict) -> NotificationsConfig:
        return cls(
            channels=d.get("channels", ["web", "telegram"]),
            telegram_chat_id=d.get("telegram_chat_id"),
            default_expiry_hours=d.get("default_expiry_hours", 48),
        )


@dataclass
class ChannelsConfig:
    """Global channel settings."""

    @classmethod
    def from_dict(cls, d: dict) -> ChannelsConfig:
        return cls()


@dataclass
class NerveConfig:
    workspace: Path = field(default_factory=lambda: Path("~/nerve-workspace"))
    timezone: str = "America/New_York"
    deployment: str = "server"            # "server" or "docker"
    quiet_start: str = "02:00"            # HH:MM — start of quiet period (local timezone)
    quiet_end: str = "08:00"              # HH:MM — end of quiet period (local timezone)
    gateway: GatewayConfig = field(default_factory=GatewayConfig)
    agent: AgentConfig = field(default_factory=AgentConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    sync: SyncConfig = field(default_factory=SyncConfig)
    memory: MemoryConfig = field(default_factory=MemoryConfig)
    cron: CronConfig = field(default_factory=CronConfig)
    sessions: SessionsConfig = field(default_factory=SessionsConfig)
    auth: AuthConfig = field(default_factory=AuthConfig)
    channels: ChannelsConfig = field(default_factory=ChannelsConfig)
    notifications: NotificationsConfig = field(default_factory=NotificationsConfig)

    # API keys (from config.local.yaml)
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    brave_search_api_key: str = ""

    @classmethod
    def from_dict(cls, d: dict) -> NerveConfig:
        return cls(
            workspace=_expand_path(d.get("workspace", "~/nerve-workspace")) or Path("~/nerve-workspace"),
            timezone=d.get("timezone", "America/New_York"),
            deployment=d.get("deployment", "server"),
            quiet_start=d.get("quiet_start", "02:00"),
            quiet_end=d.get("quiet_end", "08:00"),
            gateway=GatewayConfig.from_dict(d.get("gateway", {})),
            agent=AgentConfig.from_dict(d.get("agent", {})),
            telegram=TelegramConfig.from_dict(d.get("telegram", {})),
            sync=SyncConfig.from_dict(d.get("sync", {})),
            memory=MemoryConfig.from_dict(d.get("memory", {})),
            cron=CronConfig.from_dict(d.get("cron", {})),
            sessions=SessionsConfig.from_dict(d.get("sessions", {})),
            auth=AuthConfig.from_dict(d.get("auth", {})),
            channels=ChannelsConfig.from_dict(d.get("channels", {})),
            notifications=NotificationsConfig.from_dict(d.get("notifications", {})),
            anthropic_api_key=d.get("anthropic_api_key", ""),
            openai_api_key=d.get("openai_api_key", ""),
            brave_search_api_key=d.get("brave_search_api_key", ""),
        )


def load_config(config_dir: Path | None = None) -> NerveConfig:
    """Load config from config.yaml + config.local.yaml in the given directory.

    If config_dir is None, uses the current working directory.
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
    return NerveConfig.from_dict(merged)


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
