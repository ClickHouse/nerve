# Accounts and actor identity

Nerve stores login state separately from attribution identity.

| Table | Purpose |
|---|---|
| `accounts` | Local login state |
| `actor_refs` | Stable IDs and display names for people and the system |
| `instance_secrets` | Machine-local secrets, including a generated session-signing key |

Each human account has one actor. The account ID identifies the login record;
the actor ID identifies the person and remains stable when account state
changes. One Nerve database belongs to one agent installation. Tenant and
membership management belong outside Nerve.

## Bootstrap

A migration creates one system actor for autonomous work. When no account
exists, the gateway creates the first human account at startup. `nerve init`
creates it during setup, with the display name that you enter; a
non-interactive setup leaves the name empty. Other CLI commands do not create
accounts.

An account's `credential_source` is `config` for `auth.password_hash`, `local`
for a hash stored on the account, or `none` for passwordless access. Bootstrap
keeps `config` and `none` accounts aligned with configuration but never changes
a local credential.

## Session-signing secret

Nerve uses `auth.jwt_secret` when configured. Otherwise it generates a secret
and stores it in `instance_secrets`. Authentication fails closed until startup
has selected a secret.

The selected secret does not change on configuration reload. Adding or changing
`auth.jwt_secret` takes effect after restart and retires the stored secret.
If you remove it later, Nerve generates a new secret; the retired one does not
come back.

The database and its directory must be private to the owner. Nerve repairs the
modes when it can and refuses to open the database when another user could
have changed it. See [State-file permissions](config.md#state-file-permissions).

## Sessions and the actor on the request

A signature proves a token was minted by this instance. It does not say who is
holding it. So every token says what it is in a `typ` claim, and **every
authenticated request resolves that against the database, on the request
itself**:

| Token | `sub` | `typ` | Acts as |
|---|---|---|---|
| a login session | the account's id | `session` | that account's actor |
| the instance's own calls — `nerve reload`, starting or stopping a session from the CLI, the agent calling its own API | `agent-system` | `system` | the agent's system principal |
| MCP credentials for the agent's own subprocesses and their Ultracode workers | `backend-agent` | `system`, with `aud: nerve-mcp` | the agent's system principal |
| MCP credentials for a client you launched — `nerve codex token`, and the one the installer prints | `external-agent-mcp` | `system`, with `aud: nerve-mcp` | the agent's system principal |

Tokens are opaque to the browser; the claims above are an implementation
detail and will change again.

`agent-system`, `backend-agent` and `external-agent-mcp` are **labels, not
identity keys**. They say which minter issued the credential, and nothing looks
them up: the system principal is read from the database, so those strings never
have to match anything stored. A login session's `sub` is the one that is an
identifier, and it is an account id.

MCP credentials issued before this version carry the audience but no `typ`.
They keep working until they expire, because the audience is what the resolver
reads first.

Two consequences worth knowing:

- **Disabling an account takes effect at its next request, not retroactively.**
  A token issued before the change is still signed and unexpired, so the
  account row is the only thing that can stop it — and it does, at every door:
  HTTP, a new WebSocket, and the MCP endpoint.
- **A WebSocket's identity is fixed when it connects.** A socket stays open for
  hours, and re-reading identity mid-stream would attribute a message sent now
  differently from one sent a minute ago. Disabling, renaming or adding an
  account leaves open sockets exactly as they were and applies to the next
  connection.

Autonomous work — cron jobs, channel traffic, background agents, and the
instance talking to itself — acts as the **system principal** rather than as
whoever happens to have an account. That keeps "the agent did this" and "a
person asked for this" apart, and keeps it true after an account is renamed or
removed.

### Sessions that predate this version

Browsers hold 30-day session tokens issued before accounts existed. They name
no account, so they cannot say who they are — but they still verify, and
logging every open tab out on upgrade would be a poor trade. So:

- with **exactly one account**, such a token resolves to that account, and the
  reply carries a proper per-account token in the `X-Nerve-Token` header, which
  the browser stores. One request per tab and the old shape is gone;
- with **two or more accounts** it is refused (`401`) rather than resolved to
  whichever account sorts first: it names nobody in particular, and a guess
  would file one person's work under another's name. In practice the install
  that creates its second account makes its old tabs log in again at that
  moment, which is correct and explainable.

The acceptance is temporary and is removed in a later release. Nothing mints
that shape any more.

## Backup and restore

A normal backup includes `nerve.db`, so it preserves account and actor IDs and
the generated signing secret. `--no-secrets` removes stored secrets and causes a
new signing key to be generated after restore. Backup and restore keep these
files owner-only; see [State-file permissions](config.md#state-file-permissions).
