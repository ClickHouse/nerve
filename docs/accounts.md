# Accounts and actor identity

Nerve stores login state separately from attribution identity:

| Table | Purpose |
|---|---|
| `actor_refs` | Stable human and system actor IDs plus display names |
| `accounts` | Local login state linked one-to-one to a human actor |
| `instance_secrets` | Machine-local state secrets, including a generated session-signing key |

An account ID and its actor ID are deliberately different. Sessions and
messages can retain the actor ID as permanent attribution while account login
state changes independently. One Nerve database represents one agent
installation; tenant, membership and grant relationships belong outside Nerve.

## Bootstrap

Migration v047 creates exactly one system actor. The database validates and
caches that ID whenever it opens, and refuses to start if the singleton is
corrupt. The row cannot be duplicated, deleted, or changed into a human actor.
Autonomous work is attributed to this actor.

After migration, bootstrap creates a human actor and the first account only
when the account table is empty. The check and inserts run under one immediate
transaction, so concurrent starts still create one account. Existing accounts,
including disabled ones, are never recreated. The interactive installer uses
the name entered during setup; a headless install leaves the display name
empty.

`credential_source` records where the account password is held:

| Value | Meaning |
|---|---|
| `config` | `auth.password_hash`; the hash is not copied into the database |
| `none` | Passwordless |
| `local` | A hash on the account row, used by account management |

While the first account remains on `config` or `none`, bootstrap keeps that
value aligned with configuration. It never changes a `local` credential.

## Session-signing secret

`auth.jwt_secret` is used when configured. Otherwise first start generates a
secret in `instance_secrets`. There is no missing-secret authentication bypass:
HTTP, WebSocket, MCP, and worker-token authentication all fail closed until a
secret is pinned at startup.

The startup pin remains in force across configuration reloads. A newly
configured secret retires the database-held key using SQLite secure deletion;
removing the configured value later generates a fresh key instead of reviving
the retired one. If a database file was readable by other users, a stored key
is treated as compromised, deleted, and unpinned before replacement.

The database and its directory are protected by the state-file policy described
in [Configuration](config.md): writable or uninspectable state is refused
before opening, repairable read exposure is tightened and verified, and every
production database opener follows the same migration/bootstrap path.

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

`nerve.db` is included in backups, so account and actor IDs and the signing
secret survive a normal restore. `--no-secrets` removes `instance_secrets` from
the snapshot, causing a fresh key to be generated on next start. Backup staging,
archives, and restored database files retain the owner-only and hostile-filesystem
protections documented in [Setup](setup.md).
