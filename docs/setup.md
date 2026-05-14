# Setup Guide

## Quick Start

The fastest way to get Nerve running:

```bash
git clone https://github.com/ClickHouse/nerve.git nerve
cd nerve
pip install -e .       # or: uv pip install -e .
cd web && npm install && npm run build && cd ..
nerve init             # Interactive wizard — handles everything
nerve start
```

The `nerve init` wizard walks you through deployment, mode selection, API keys, workspace setup, and cron configuration. Nothing is written until you confirm.

## Prerequisites

### Server deployment
- Python 3.12+
- Node.js 18+ (for web UI build)
- Anthropic API key **or** Claude subscription (via CLIProxyAPI proxy)

### Docker deployment
- Docker with Compose V2 (`docker compose`)
- Anthropic API key **or** Claude subscription (via CLIProxyAPI proxy)

## Installation

### Option A: Server (bare metal)

```bash
git clone https://github.com/ClickHouse/nerve.git nerve
cd nerve

# Create virtual environment
uv venv
source .venv/bin/activate

# Install Nerve
uv pip install -e .

# Build web UI
cd web && npm install && npm run build && cd ..

# Run the setup wizard
nerve init
```

### Option B: Docker

```bash
git clone https://github.com/ClickHouse/nerve.git nerve
cd nerve
pip install -e .   # Needed to run the wizard on the host
nerve init         # Choose "docker" at the deployment step
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
| `nerve-workspace:/root/nerve-workspace` | Workspace files (SOUL.md, tasks, skills) |

## Re-running `nerve init`

You can re-run `nerve init` at any time — it's safe on existing installations.

**What gets overwritten:**
- `config.yaml` and `config.local.yaml` — regenerated from your choices
- `~/.nerve/cron/system.yaml` — regenerated (picks up new built-in cron prompts from Nerve updates)

**What's preserved:**
- All workspace files (`SOUL.md`, `IDENTITY.md`, `USER.md`, `MEMORY.md`, skills, tasks, etc.)
- `~/.nerve/cron/jobs.yaml` — your custom crons are never touched
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
EOF
```

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
