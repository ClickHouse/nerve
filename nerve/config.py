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
class AgentConfig:
    model: str = "claude-opus-4-7"
    cron_model: str = "claude-sonnet-4-6"
    title_model: str = "claude-haiku-4-5-20251001"  # Session title generation
    max_turns: int = 100
    max_concurrent: int = 4
    thinking: str = "max"       # max, high, medium, low, disabled, adaptive, or number (budget_tokens)
    effort: str = "max"         # max, xhigh, high, medium, low
    context_1m: bool = True     # Enable 1M context window beta

    @classmethod
    def from_dict(cls, d: dict) -> AgentConfig:
        return cls(
            model=d.get("model", "claude-opus-4-7"),
            cron_model=d.get("cron_model", "claude-sonnet-4-6"),
            title_model=d.get("title_model", "claude-haiku-4-5-20251001"),
            max_turns=d.get("max_turns", 100),
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
    sessions: SessionsConfig = field(default_factory=SessionsConfig)
    auth: AuthConfig = field(default_factory=AuthConfig)
    channels: ChannelsConfig = field(default_factory=ChannelsConfig)
    notifications: NotificationsConfig = field(default_factory=NotificationsConfig)
    docker: DockerConfig = field(default_factory=DockerConfig)
    proxy: ProxyConfig = field(default_factory=ProxyConfig)
    houseofagents: HouseOfAgentsConfig = field(default_factory=HouseOfAgentsConfig)
    langfuse: LangfuseConfig = field(default_factory=LangfuseConfig)
    mcp_servers: list[McpServerConfig] = field(default_factory=list)

    # API keys (from config.local.yaml)
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    brave_search_api_key: str = ""

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
            sessions=SessionsConfig.from_dict(d.get("sessions", {})),
            auth=AuthConfig.from_dict(d.get("auth", {})),
            channels=ChannelsConfig.from_dict(d.get("channels", {})),
            notifications=NotificationsConfig.from_dict(d.get("notifications", {})),
            docker=DockerConfig.from_dict(d.get("docker", {})),
            proxy=ProxyConfig.from_dict(d.get("proxy", {})),
            houseofagents=HouseOfAgentsConfig.from_dict(d.get("houseofagents", {})),
            langfuse=LangfuseConfig.from_dict(d.get("langfuse", {})),
            mcp_servers=_parse_mcp_servers(d),
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
