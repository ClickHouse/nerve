# Migration from OpenClaw

> **Legacy guide.** This is for users migrating from OpenClaw (the predecessor project). New installations should start with `nerve init` — see the [Setup Guide](setup.md).

## Overview

Nerve replaces OpenClaw. This guide covers migrating workspace files, cron jobs, tasks, and memory from the existing OpenClaw setup.

## Workspace Files

Copy identity and memory files from your OpenClaw workspace:

```bash
OPENCLAW_WS=~/clawd
NERVE_WS=~/nerve-workspace

mkdir -p $NERVE_WS/memory/tasks/{active,done}

# Identity files
cp $OPENCLAW_WS/SOUL.md $NERVE_WS/
cp $OPENCLAW_WS/IDENTITY.md $NERVE_WS/
cp $OPENCLAW_WS/USER.md $NERVE_WS/
cp $OPENCLAW_WS/AGENTS.md $NERVE_WS/
cp $OPENCLAW_WS/TOOLS.md $NERVE_WS/
cp $OPENCLAW_WS/MEMORY.md $NERVE_WS/

# Memory files
cp -r $OPENCLAW_WS/memory/* $NERVE_WS/memory/
```

## Cron Jobs

Convert OpenClaw's `~/.openclaw/cron/jobs.json` to Nerve's YAML format:

```bash
mkdir -p ~/.nerve/cron
```

Create `~/.nerve/cron/jobs.yaml` from your existing jobs. The format changes from:

```json
{
  "id": "morning-telegram",
  "cron": "30 11 * * *",
  "payload": "...",
  "model": "claude-sonnet-4-6"
}
```

To:

```yaml
jobs:
  - id: morning-telegram
    schedule: "30 11 * * *"
    prompt: "..."
    model: claude-sonnet-4-6
    target: telegram
    enabled: true
```

## Tasks

If tasks are in `workspace/memory/tasks/`, they'll be auto-indexed on first run. The markdown format is compatible. The SQLite index replaces `index.json`.

## Config

Map OpenClaw config to Nerve:

| OpenClaw (openclaw.json) | Nerve (config.yaml + config.local.yaml) |
|---|---|
| `agent.model` | `agent.model` |
| `agent.fallback` | N/A (single model) |
| `channels.telegram.token` | `telegram.bot_token` |
| `hooks.*` | `hooks` (simplified) |
| `gateway.port` | `gateway.port` |

## What Changes

| Feature | OpenClaw | Nerve |
|---------|----------|-------|
| Runtime | Node.js | Python |
| Agent | Custom API wrapper | Claude Agent SDK |
| Tools | Custom implementations | SDK built-in + MCP |
| Config | JSON | YAML |
| Database | File-based JSON | SQLite |
| Sync | MCP servers | Sources layer (cursor-based, agent-processed) |
| Web UI | None | React |
| Plugins | 40+ plugins | Direct implementations |
| Memory search | None | memU |

## Telegram MCP

The existing Telegram MCP server at `~/telegram-mcp` can continue running alongside Nerve. Nerve's sources layer uses Telethon directly for message ingestion, while the MCP server provides tools for the agent to interact with Telegram (send messages, read chats, etc.).
