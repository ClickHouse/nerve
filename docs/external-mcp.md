# External MCP server

Nerve can expose its full tool registry to external MCP clients (Codex,
Claude Code, Cursor, custom agents) over **Streamable HTTP**. Every tool
the in-process agent uses — `task_create`, `memory_recall`, `notify`,
`plan_propose`, `skill_get`, and so on — is reachable from outside the
process, attributed to a Nerve "satellite session" so external tool
calls show up in the UI alongside native ones.

## Enabling

Add to `config.yaml` (or override in `config.local.yaml`):

```yaml
mcp_endpoint:
  enabled: true
  path: /mcp/v1          # default
  include_hoa: false     # set true to expose hoa_* tools
```

Restart Nerve. The endpoint is mounted at `https://<host>:<port><path>/`.

## Authentication

The endpoint reuses Nerve's existing JWT — same one the web UI uses.
Issue a token via `POST /api/auth/login` and present it on every MCP
request as either:

* `Authorization: Bearer <jwt>` header, **or**
* `?token=<jwt>` query parameter (handy for clients that don't let you
  set headers, e.g. some Codex configs).

If `auth.jwt_secret` is empty (dev mode), the endpoint accepts all
requests — mirrors the gateway's existing dev-mode behaviour.

## Configuring Codex CLI

In Codex's MCP config (`~/.codex/config.toml` or your Codex instance's
equivalent):

```toml
[[mcp_servers]]
name = "nerve"
url  = "https://your-nerve-host:8900/mcp/v1/?token=<your-jwt>"
```

Codex will discover Nerve's tools at `initialize` time and surface them
in the conversation like any other MCP toolset.

## Satellite sessions

Every MCP connection creates a row in `sessions` with
`source = "external"` and a `metadata` JSON carrying the client name
and MCP session id. The id format is
`external:<client_name>:<mcp_session_id>` (or
`external:<client_name>:<client_session_id>` if the client supplies a
stable id). The session shows up in the Nerve UI session list, and
every tool call is recorded in `session_events` as an
`external_tool_call` event for audit and diagnostics.

External `ask_user` calls are **fire-and-forget**: the user is notified
on Telegram / web as usual, but Nerve does **not** try to inject the
answer back into the external client's conversation (Codex owns its
own thread). If the external agent really needs blocking input, use
the client's native input mechanism instead of `ask_user`.

## Out of scope (for now)

* **stdio transport** — only HTTP is shipped. A stdio↔HTTP proxy can
  be added later if a Codex CLI flow demands it.
* **SSE resumability** — Nerve doesn't expose long-blocking tools, so
  there's nothing useful to resume across a network blip. The endpoint
  runs with no `EventStore`.
* **Per-client scopes / token revocation** — single JWT, full access.
  A future revision can add per-token scopes if multi-tenant or fine-
  grained access becomes a real need.
* **AGENTS.md sync** — keeping `~/.codex/AGENTS.md` etc. in lockstep
  with Nerve's memory files. Tracked separately.
