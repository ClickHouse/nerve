# Setup Guide

## Quick Start

The fastest way to get Nerve running:

```bash
git clone https://github.com/ClickHouse/nerve.git nerve
cd nerve
uv sync                       # creates .venv from uv.lock
source .venv/bin/activate     # puts `nerve` on PATH
cd web && npm install && npm run build && cd ..
nerve init                    # Interactive wizard — handles everything
nerve start
```

`uv sync` creates `.venv` but does not activate it, so `nerve` is not on `PATH` until
you do. If you'd rather not activate, prefix commands with `uv run` instead —
`uv run nerve init`.

The `nerve init` wizard walks you through deployment, mode selection, API keys, workspace setup, and cron configuration. Nothing is written until you confirm.

## Prerequisites

### Server deployment
- Python 3.13+
- Node.js 22.12+ (for web UI build)
- Anthropic API key **or** Claude subscription (via CLIProxyAPI proxy)

### Docker deployment
- Docker with Compose V2 (`docker compose`)
- Anthropic API key **or** Claude subscription (via CLIProxyAPI proxy)

## Installation

### Option A: Server (bare metal)

```bash
git clone https://github.com/ClickHouse/nerve.git nerve
cd nerve

# Create .venv and install Nerve at the locked versions
uv sync
source .venv/bin/activate

# Build web UI
cd web && npm install && npm run build && cd ..

# Run the setup wizard
nerve init
```

`uv sync` installs the exact versions in `uv.lock` — see
[Dependency versions](#dependency-versions) for how that is maintained.

### Option B: Docker

```bash
git clone https://github.com/ClickHouse/nerve.git nerve
cd nerve
uv sync                    # Needed to run the wizard on the host
uv run nerve init          # Choose "docker" at the deployment step
```

The wizard handles everything: generates Dockerfile + docker-compose.yml, builds the image, starts the container, and continues setup inside it. You never write Docker files manually.

**What happens under the hood:**
1. `nerve init` asks "How do you want to run Nerve?" → choose `docker`
2. Generates `Dockerfile`, `docker-compose.yml`, `docker-entrypoint.sh`, `.dockerignore`
3. Runs `docker compose build`
4. Runs `docker compose run nerve nerve init --inside-docker` (seamless transition)
5. The rest of the wizard (mode, API keys, workspace, crons) continues inside the container
6. After setup, Nerve starts automatically inside the container

**Subsequent starts:**
```bash
docker compose up        # Start
docker compose up -d     # Start in background
docker compose down      # Stop
docker compose logs -f   # Follow logs
```

**Non-interactive Docker setup** (CI / automation):
```bash
cp .env.example .env
# Edit .env with your ANTHROPIC_API_KEY (or set NERVE_USE_PROXY=1)
docker compose up
```

The entrypoint runs `nerve init --if-needed --non-interactive` before starting, using environment variables from `.env`.

**Using CLIProxyAPI instead of an API key:**
Set `NERVE_USE_PROXY=1` in your environment (no `ANTHROPIC_API_KEY` required). The proxy authenticates via Claude Code's OAuth — requires a Claude Max/Pro subscription at claude.ai. See [config.md](config.md#proxy-cliproxyapi) for details.

**Volumes:**
| Mount | Purpose |
|-------|---------|
| `.:/nerve` | Application code (bind mount) |
| `nerve-data:/root/.nerve` | Databases, logs, PID, sessions |
| `nerve-workspace:/root/nerve-workspace` | Workspace files (SOUL.md, tasks, skills, `config/`) |

The container paths are not conventions — the generated Dockerfile sets
`NERVE_HOME=/root/.nerve` and `NERVE_WORKSPACE=/root/nerve-workspace`
explicitly. Point them elsewhere and the mounts must follow.

## Unattended installation

For cloud-init, Ansible, image builds and other places with no terminal, run the installer with `--non-interactive` (or `NERVE_NON_INTERACTIVE=1`). It implies `NERVE_YES=1`, never prompts, and finishes with `nerve init --non-interactive`, which reads its configuration from the environment.

```bash
curl -fsSL https://raw.githubusercontent.com/ClickHouse/nerve/main/install.sh \
  | NERVE_NON_INTERACTIVE=1 \
    NERVE_INSTALL_DIR=/home/agent/nerve \
    NERVE_MODE=worker \
    NERVE_WORKSPACE=/home/agent/nerve-workspace \
    NERVE_TIMEZONE=UTC \
    NERVE_PROVIDER=bedrock \
    NERVE_AWS_REGION=eu-central-1 \
    bash
```

Dependency installation adapts to the environment: as root (a plain `ubuntu:24.04` container, for example) packages are installed directly with no `sudo` needed, and on Debian and Ubuntu `DEBIAN_FRONTEND=noninteractive` is set so `tzdata` and friends cannot open a debconf prompt. As a non-root user, `sudo -n` is used, so a missing password rule fails immediately instead of waiting.

The daemon is not started: unattended installs are normally followed by a service manager that owns the process (see [systemd Service](#systemd-service-optional)). Set `NERVE_START=1` to start it from the installer instead.

Anything the setup wizard would ask comes from the environment. Authentication is required — set `NERVE_PROVIDER=bedrock` (IAM, no key), or `ANTHROPIC_API_KEY`, `CLAUDE_CODE_OAUTH_TOKEN` or `NERVE_USE_PROXY=1`. Everything else has a default. `NERVE_PASSWORD` is read once here and stored only as a bcrypt hash in `config.local.yaml`, so it does not need to stay in the environment afterwards.

| Variable | Default | Purpose |
|---|---|---|
| `NERVE_MODE` | `personal` | `personal` or `worker` |
| `NERVE_WORKSPACE` | `~/nerve-workspace` | Workspace directory |
| `NERVE_TIMEZONE` | `America/New_York` | Schedule timezone |
| `NERVE_PROVIDER` | `anthropic` | `anthropic` or `bedrock` |
| `NERVE_AWS_REGION` | `$AWS_REGION`, else `us-east-1` | Bedrock region; sets the model geo-prefix |
| `NERVE_PASSWORD` | unset | Web UI password; unset means no authentication |
| `NERVE_TASK` | unset | Worker mode task description |
| `NERVE_EXTERNAL_AGENTS` | unset | Personal mode, e.g. `codex,claude-code` |
| `GH_TOKEN` | unset | GitHub integration |

A re-run over an existing install keeps the configuration and only upgrades the code.

## Re-running `nerve init`

You can re-run `nerve init` at any time — it's safe on existing installations.

**What gets overwritten:**
- `config.yaml` and `config.local.yaml` — regenerated from your choices
- `<workspace>/config/cron/system.yaml` — regenerated (picks up new built-in cron prompts from Nerve updates)

**What's preserved:**
- All workspace files (`SOUL.md`, `IDENTITY.md`, `USER.md`, `MEMORY.md`, skills, tasks, etc.)
- `<workspace>/config/cron/jobs.yaml` — your custom crons are never touched
- `<workspace>/config/settings.yaml` — only the keys `nerve init` generates are rewritten; anything else you added stays
- `~/.nerve/nerve.db` and `~/.nerve/memu.sqlite` — databases are preserved

When you run `nerve init` on an existing install, it prompts: *"Nerve is already configured. Re-run setup?"* The `--if-needed` flag skips setup entirely if already configured (useful in Docker entrypoints).

## Docker Credential Forwarding

When deploying via Docker, `nerve init` needs to pass authentication credentials from the host into the container. It resolves credentials using a priority waterfall:

1. **macOS Keychain — `Claude Code-credentials`** — extracts OAuth access token from the JSON stored by Claude Code
2. **macOS Keychain — `Claude Code`** — raw API key
3. **`CLAUDE_CODE_OAUTH_TOKEN` env var**
4. **`~/.claude/.credentials.json` file** — where Linux stores Claude credentials
5. **`ANTHROPIC_API_KEY` env var**

The first match wins. The extracted credential is passed to `docker compose run` as an environment variable, then written into `config.local.yaml` inside the container during setup.

> **Note:** The `~/.claude` directory is NOT mounted into the container. Instead, credentials are resolved on the host and injected via environment variables. This avoids file permission issues and macOS Keychain access from within Docker.

## Manual Configuration

The wizard handles all of this automatically, but you can also configure manually:

```bash
# Create secrets file (gitignored)
cat > config.local.yaml << 'EOF'
anthropic_api_key: sk-ant-...
openai_api_key: sk-...           # Optional — enables vector-based memory search

telegram:
  bot_token: "123456:ABC..."

auth:
  password_hash: "$2b$12$..."    # Generate below
  jwt_secret: "..."              # Generate below
  jwt_expiry_hours: 720          # Optional — web-session idle timeout (default 30 days)
EOF
```

`jwt_expiry_hours` is an **idle** timeout, not a cap on a working session: the
gateway re-mints the token whenever a request arrives past half its lifetime,
so a tab in continuous use is never logged out. Only a tab left untouched for
the whole window comes back to a password prompt. Lower it if the browser is
somewhere you don't fully trust.

### Generate auth credentials

```bash
# Password hash
python -c "import bcrypt; print(bcrypt.hashpw(b'yourpassword', bcrypt.gensalt()).decode())"

# JWT secret
python -c "import secrets; print(secrets.token_hex(32))"
```

## First Run

```bash
nerve doctor             # Verify everything is set up
nerve start              # Start the server
# Open http://localhost:8900
```

## HTTPS Setup

```bash
# Install mkcert
sudo apt install libnss3-tools
curl -L https://github.com/FiloSottile/mkcert/releases/download/v1.4.4/mkcert-v1.4.4-linux-arm64 -o mkcert
chmod +x mkcert && sudo mv mkcert /usr/local/bin/

# Create certificates
mkdir -p ~/.nerve/certs
mkcert -install
mkcert -cert-file ~/.nerve/certs/cert.pem -key-file ~/.nerve/certs/key.pem \
  localhost 127.0.0.1 "$(hostname)" "$(hostname).local"
```

Update `config.yaml`:
```yaml
gateway:
  ssl:
    cert: ~/.nerve/certs/cert.pem
    key: ~/.nerve/certs/key.pem
```

### Trust CA on Mac (for remote access)

```bash
# On Pi: copy the CA cert
cat "$(mkcert -CAROOT)/rootCA.pem"

# On Mac: save to file and trust
sudo security add-trusted-cert -d -r trustRoot \
  -k /Library/Keychains/System.keychain rootCA.pem
```

## Running Nerve

### Daemon Mode (recommended)

Nerve has built-in daemon management. No systemd required for basic usage.

```bash
nerve start           # Start as background daemon
nerve stop            # Stop the daemon (graceful, 15s timeout)
nerve restart         # Stop + start
nerve status          # Show PID, memory, uptime
nerve status -f       # Show status then tail logs
nerve logs            # Tail the daemon log

nerve start -f        # Run in foreground (for debugging)
```

**PID file:** `~/.nerve/nerve.pid`
**Log file:** `~/.nerve/nerve.log`

### systemd Service (optional)

For auto-start on boot, create `/etc/systemd/system/nerve.service`:

```ini
[Unit]
Description=Nerve Personal AI Assistant
After=network.target

[Service]
Type=simple
User=YOUR_USER
WorkingDirectory=/home/YOUR_USER/nerve
Environment=PATH=/home/YOUR_USER/nerve/.venv/bin:/usr/local/bin:/usr/bin:/bin
ExecStart=/home/YOUR_USER/nerve/.venv/bin/nerve start --foreground
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Note: Use `--foreground` with systemd since it manages the process lifecycle.

```bash
sudo systemctl daemon-reload
sudo systemctl enable nerve
sudo systemctl start nerve

# Check status
sudo systemctl status nerve
journalctl -u nerve -f
```

## Dependency versions

`uv.lock` pins the exact version of every dependency, transitive ones included.
`uv sync` installs from it, so the same commit resolves to the same versions
whenever and wherever you install. That is why `uv sync` is the documented install
rather than `uv pip install -e .`.

Two honest limits on "reproducible": the lock is universal but its entries carry
platform and Python markers, so different platforms legitimately get different
*files* for the same pinned versions; and artifacts are not vendored, so an
air-gapped install still needs a populated uv cache or a local wheelhouse.

```bash
uv sync                    # runtime dependencies
uv sync --extra test       # ...plus the test extra
```

`uv sync` creates and manages `.venv` itself, installs Nerve editable, and
removes anything not in the lock — so the environment matches the lock exactly
rather than accumulating leftovers.

> **`uv pip install -e .` does not read `uv.lock`.** uv's pip-compatible layer has
> no lockfile awareness, so that command resolves against the bounds in
> `pyproject.toml` and can install different versions. It still works, and it is
> what you want when deliberately testing against current upstream — but it is not
> a reproducible install.

### Upgrading an installation that predates the lockfile

`nerve upgrade` runs the updater code that is **already loaded in memory**, then
pulls. So the first upgrade across the commit that introduced `uv.lock` still uses
the old, unpinned installer — the lock only takes effect from the *second* upgrade
onward. Two consequences worth knowing before you run it:

- That first upgrade resolves dependencies fresh, so it can pick up something newer than the lock intends.
- If you are on Python 3.12, it will **fail after `git pull` has already advanced the checkout**, because the new `pyproject.toml` requires 3.13+. You are left with new source and an old environment.

Either is straightforward to recover from — do the install step yourself, on 3.13+:

```bash
cd <your nerve checkout>
uv sync --locked --inexact
nerve restart
```

Subsequent `nerve upgrade` runs install from the lock automatically.

### Changing a dependency

Edit `pyproject.toml`, then relock and commit `uv.lock` alongside it:

```bash
uv lock
```

CI runs `uv sync --locked`, which fails if `uv.lock` and `pyproject.toml` have
drifted apart — so a dependency change without a relock is caught rather than
silently re-resolved.

Relocking preserves existing pins, so it won't sweep in unrelated upgrades. To
move one deliberately:

```bash
uv lock --upgrade-package mcp    # one dependency
uv lock --upgrade                # everything
```

### How CI enforces this

`ci.yml` installs with `uv sync --locked`, which does two jobs at once:

- A PR is never broken by an upstream release that has nothing to do with it — the versions come from the lock.
- A dependency change cannot merge without a relock. `--locked` fails if `uv.lock` disagrees with `pyproject.toml` for *any* reason: a dependency added, removed, or simply re-bounded. Since the relock then moves the pins, the versions CI tests are always the ones the change actually selects.

That second property is why no separate unpinned CI job is needed. Widen a bound and
the lock is invalidated; relock and the pin moves; CI tests the moved pin.

### What is and isn't covered

| Path | Installs from `uv.lock`? |
|---|---|
| `uv sync` (quick start, server install, `install.sh`) | yes |
| `nerve upgrade` | yes — `uv sync --locked --inexact` when a `uv.lock` is present |
| CI (`ci.yml`) | yes — `uv sync --locked` |
| Docker | yes — the image installs from the lock at build time and the entrypoint syncs the project |
| `uv pip install -e .` / plain `pip install -e .` | **no** — resolves from `pyproject.toml` bounds |

Docker keeps its environment at `/opt/nerve-venv`, outside the `/nerve` bind mount,
because a `.venv` under `/nerve` would be shadowed by the host's checkout (and a
host-created one may not be Linux-compatible).

> **Existing Docker deployments need one manual step.** `nerve init` does not
> overwrite Docker files that already exist, so an install generated before this
> change keeps its old pip-based `Dockerfile` and entrypoint. To pick up the locked
> build, delete them and regenerate:
>
> ```bash
> rm Dockerfile docker-entrypoint.sh
> nerve init                       # choose "docker" again; regenerates both
> docker compose build --no-cache
> ```
>
> Keep `docker-compose.yml` — it is unchanged. Check any local edits you made to
> the old files before deleting them.

`nerve upgrade` uses `--inexact`, so it won't uninstall optional extras you added
yourself, and `--frozen`, so it never rewrites `uv.lock` as a side effect of
upgrading. If uv is missing, or the checkout has no `uv.lock`, it falls back to the
previous `pip install -e .` behaviour.

## Troubleshooting

### Database Schema Issues

Nerve auto-migrates the SQLite database on startup. If a migration fails or the schema gets out of sync, you can inspect and fix it manually.

**Check current schema version:**
```bash
sqlite3 ~/.nerve/nerve.db "SELECT version FROM schema_version"
```

**Verify sessions table columns:**
```bash
sqlite3 ~/.nerve/nerve.db "PRAGMA table_info(sessions)"
```

Expected columns (as of V3): `id`, `title`, `created_at`, `updated_at`, `source`, `metadata`, `status`, `sdk_session_id`, `parent_session_id`, `forked_from_message`, `connected_at`, `last_activity_at`, `archived_at`, `message_count`, `total_cost_usd`, `last_memorized_at`.

**Add a missing column manually:**
```bash
sqlite3 ~/.nerve/nerve.db "ALTER TABLE sessions ADD COLUMN last_memorized_at TEXT"
```

After any manual schema fix, restart Nerve: `nerve restart`.
