# Configuration Reference

Nerve uses two YAML config files:
- `config.yaml` — Template settings (version controlled)
- `config.local.yaml` — Secrets and personal overrides (gitignored)

Values in `config.local.yaml` are deep-merged on top of `config.yaml`.

## Core

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `workspace` | path | `~/nerve-workspace` | Path to workspace directory |
| `timezone` | string | `America/New_York` | Local timezone for scheduling |
| `deployment` | string | `server` | `server` (bare metal) or `docker`. Set during `nerve init`; determines whether CLI commands run directly or proxy to `docker compose`. |

> **Note:** The _mode_ (personal vs worker) is not a config field — it's determined at `nerve init` time and expressed through which workspace templates, cron jobs, and memory categories are active. There's no `mode` key in config.

## Agent

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `agent.model` | string | `claude-opus-4-6` | Primary model for conversations |
| `agent.cron_model` | string | `claude-sonnet-4-6` | Model for cron jobs (cheaper) |
| `agent.max_turns` | int | `50` | Max agentic turns per request |
| `agent.max_concurrent` | int | `4` | Max concurrent agent sessions |

**Note:** The engine uses a `can_use_tool` callback (not `bypassPermissions`) so that interactive tools (`AskUserQuestion`, `ExitPlanMode`, `EnterPlanMode`) can pause mid-turn for user input. All other tools are auto-approved. See [sdk-sessions.md](sdk-sessions.md#permissions--interactive-tools) for details.

## Gateway

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `gateway.host` | string | `0.0.0.0` | Bind address |
| `gateway.port` | int | `8900` | Port number |
| `gateway.ssl.cert` | path | - | SSL certificate path |
| `gateway.ssl.key` | path | - | SSL private key path |

## Telegram

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `telegram.enabled` | bool | `true` | Enable Telegram bot |
| `telegram.bot_token` | string | - | Bot token from @BotFather |
| `telegram.dm_policy` | string | `pairing` | `open` or `pairing` |
| `telegram.stream_mode` | string | `partial` | `partial` (edit msgs) or `full` |

## Quiet Hours

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `quiet_start` | string | `02:00` | HH:MM — start of quiet period (local timezone) |
| `quiet_end` | string | `08:00` | HH:MM — end of quiet period (local timezone) |

## Sources (sync)

Sources pull data from external services on a schedule. See [sources.md](sources.md) for full details.

**Common fields** (available on all sources):

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sync.<source>.enabled` | bool | `true` | Enable/disable this source |
| `sync.<source>.schedule` | cron/interval | varies | Fetch frequency (crontab or interval like `2h`) |
| `sync.<source>.processor` | string | `agent` | `agent` (LLM review), `memorize` (direct memU), `notify` (channel forward), `none` |
| `sync.<source>.batch_size` | int | `50` | Max records per fetch cycle |
| `sync.<source>.prompt_hint` | string | `""` | Extra instructions for the agent prompt |
| `sync.<source>.model` | string | `""` | Override model (empty = `agent.cron_model`) |
| `sync.<source>.condense` | bool | `false` | LLM-condense long records via `memory.fast_model` before processing |

**Telegram-specific:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sync.telegram.api_id` | int | - | Telethon API ID (from my.telegram.org) |
| `sync.telegram.api_hash` | string | - | Telethon API hash |
| `sync.telegram.schedule` | cron | `*/5 * * * *` | Fetch frequency |
| `sync.telegram.exclude_chats` | list[int] | `[]` | Chat IDs to skip |
| `sync.telegram.monitored_folders` | list | `[]` | Telegram folder names to filter |

**Gmail-specific:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sync.gmail.accounts` | list | `[]` | Gmail accounts to sync |
| `sync.gmail.schedule` | cron | `*/15 * * * *` | Fetch frequency |
| `sync.gmail.keyring_password` | string | - | gog keyring password |

**GitHub-specific:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sync.github.schedule` | cron | `*/15 * * * *` | Fetch frequency |

## Memory (memU)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `memory.recall_model` | string | `claude-sonnet-4-6` | Model for recall routing |
| `memory.memorize_model` | string | `claude-sonnet-4-6` | Model for extraction & preprocessing |
| `memory.fast_model` | string | `claude-haiku-4-5-20251001` | Model for categorization, date resolution, knowledge filtering |
| `memory.embed_model` | string | `text-embedding-3-small` | Embedding model |
| `memory.semantic_dedup_threshold` | float | `0.85` | Cosine similarity threshold for semantic deduplication (0 to disable) |
| `memory.knowledge_filter` | bool | `false` | Post-extraction LLM filter that deletes generic knowledge items (extra Haiku API call per memorize) |
| `memory.categories` | list | `[]` | Seed categories — each entry has `name` and `description` fields. Used for semantic routing when memorizing and recalling facts. `nerve init` populates mode-appropriate defaults (personal: relationships, finances, health, etc.; worker: patterns, procedures, approvals, etc.). |

## Docker

Configuration for Docker deployment. Only relevant when `deployment: docker`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `docker.extra_mounts` | list[string] | `[]` | Additional host:container mount pairs to add to `docker-compose.yml`. Example: `["~/code:/code", "~/projects:/projects"]` |

The core Docker mounts (source code, `~/.nerve`, workspace) are always included. GitHub CLI (`~/.config/gh`) and Gmail CLI (`~/.config/gog`) auth directories are mounted automatically if they exist on the host.

## MCP Servers

External MCP servers can be added via config without code changes or restarts. The agent picks up new servers on the next session creation, or immediately via the "Reload" button in the UI / `mcp_reload` tool.

Config uses a **dict format** so `_deep_merge` correctly overlays secrets from `config.local.yaml`:

```yaml
# config.yaml — server definitions
mcp_servers:
  filesystem:
    type: stdio
    command: npx
    args: ["-y", "@modelcontextprotocol/server-filesystem", "/data"]

  remote-api:
    type: http
    url: https://mcp.example.com/v1
```

```yaml
# config.local.yaml — secrets merge on top
mcp_servers:
  remote-api:
    headers:
      Authorization: "Bearer sk-secret-token"
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mcp_servers.<name>.type` | string | `stdio` | Transport: `stdio`, `sse`, or `http` |
| `mcp_servers.<name>.enabled` | bool | `true` | Enable/disable this server |
| `mcp_servers.<name>.command` | string | - | Command to run (stdio only) |
| `mcp_servers.<name>.args` | list | `[]` | Command arguments (stdio only) |
| `mcp_servers.<name>.env` | dict | `{}` | Environment variables (stdio only) |
| `mcp_servers.<name>.url` | string | - | Server URL (sse/http only) |
| `mcp_servers.<name>.headers` | dict | `{}` | HTTP headers (sse/http only) |

The built-in `nerve` server (SDK type, in-process) is always present and cannot be overridden.

## Auth

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `auth.password_hash` | string | - | bcrypt hash for login |
| `auth.jwt_secret` | string | - | JWT signing secret |

## API Keys (config.local.yaml)

| Key | Type | Description |
|-----|------|-------------|
| `anthropic_api_key` | string | Anthropic API key (agent + memU chat). Not required when proxy is enabled. |
| `openai_api_key` | string | OpenAI API key (memU embeddings only) |
| `brave_search_api_key` | string | Brave Search API key (optional) |

## Proxy (CLIProxyAPI)

Optional local proxy that routes Anthropic API calls through Claude Code's OAuth authentication instead of a direct API key. When enabled, the API key is not required — all API calls go through the proxy at `localhost`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `proxy.enabled` | bool | `false` | Enable CLIProxyAPI proxy |
| `proxy.port` | int | `8317` | Proxy listen port |
| `proxy.host` | string | `127.0.0.1` | Proxy bind address |
| `proxy.binary_path` | path | `~/.nerve/bin/cli-proxy-api` | Path to CLIProxyAPI binary (auto-downloaded if missing) |
| `proxy.auth_dir` | path | `~/.nerve/cli-proxy-auth` | Directory for OAuth token storage |
| `proxy.api_key` | string | `sk-nerve-local-proxy` | Local auth key between Nerve and the proxy |
| `proxy.log_file` | path | `~/.nerve/proxy.log` | Proxy log file |

**Setup:**
```bash
# During nerve init, choose "Claude Code proxy" at the API configuration step.
# Or enable manually:
```

```yaml
# config.yaml
proxy:
  enabled: true
  port: 8317
```

```bash
# Authenticate with Claude (one-time):
~/.nerve/bin/cli-proxy-api --claude-login --no-browser \
  --config ~/.nerve/cli-proxy-config.yaml
```

The proxy binary is automatically downloaded from [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) on first start if not present. OAuth tokens are refreshed automatically.

## Sessions

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sessions.archive_after_days` | int | `30` | Auto-archive idle/stopped sessions older than this |
| `sessions.max_sessions` | int | `500` | Max active (non-archived) sessions before cleanup |
| `sessions.cron_session_mode` | string | `per_run` | `per_run` (unique session per cron run) or `reuse` (shared session per job) |

## Cron

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `cron.system_file` | path | `~/.nerve/cron/system.yaml` | System cron jobs (managed by `nerve init`) |
| `cron.jobs_file` | path | `~/.nerve/cron/jobs.yaml` | User-defined custom cron jobs |
