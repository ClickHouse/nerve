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

## Request identity

Nerve resolves every authenticated request to either a human actor or the
installation's system actor.

| Credential | Acts as |
|---|---|
| Login session | The account's human actor |
| Nerve CLI and internal API token | The system actor |
| Backend and external MCP token | The system actor |

Disabled accounts are rejected on their next HTTP or MCP request and on new
WebSocket connections. An existing WebSocket keeps the identity it received at
connection time. Autonomous work, including cron jobs and background agents,
uses the system actor rather than a human account.

Browser sessions created before account-based tokens are accepted only while
one account exists. The next authenticated response replaces them through the
`X-Nerve-Token` header. They are rejected after a second account is added.

## Backup and restore

A normal backup includes `nerve.db`, so it preserves account and actor IDs and
the generated signing secret. `--no-secrets` removes stored secrets and causes a
new signing key to be generated after restore. Backup and restore keep these
files owner-only; see [State-file permissions](config.md#state-file-permissions).
