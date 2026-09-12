# Accounts and actor identity

Nerve uses local accounts for login and stable actor IDs for attribution.

| Table | Purpose |
|---|---|
| `accounts` | Usernames, passwords, and account status |
| `actor_refs` | Stable IDs and display names for people and the system |
| `instance_secrets` | Machine-local secrets, including a generated session-signing key |

Each human account has one actor. The account ID identifies the login record;
the actor ID identifies the person and remains stable when the account is
renamed. One Nerve database belongs to one agent installation. Tenant and
membership management belong outside Nerve.

## Account management

Every account has the same permissions. Any signed-in account can list, create,
rename, disable, and re-enable accounts. Adding someone therefore allows them to
disable your account. The system principal cannot manage accounts.

Account management follows these rules:

- Accounts are disabled, not deleted. Disabled rows remain for attribution and
  to prevent an installation from returning to single-account login rules.
- The last enabled account cannot be disabled.
- You can change only your own password. If the account already has a password,
  you must provide it.
- Before adding a second account, the existing account must have a username and
  the installation must have a password.

### Usernames

Usernames are trimmed, converted to lowercase, and compared case-insensitively.

| Rule | Value |
|---|---|
| Length | 2–32 characters |
| Format | `^[a-z0-9][a-z0-9._-]{1,31}$` |
| Reserved | `user`, `admin`, `system`, `nerve`, `agent-system`, `backend-agent`, `external-agent-mcp`, `root`, `me` |

A username is a login name, not an identity. Renaming it does not change the
actor ID or existing attribution. The display name is separate and may be
empty.

## Login

`GET /api/auth/status` tells clients which fields to request:

| `login` value | State | Required fields |
|---|---|---|
| `none` | One account with no password | None; any password is accepted |
| `password` | One account with a password | Password |
| `username_password` | Two or more accounts | Username and password |

A passwordless installation is safe only when access to the gateway is already
restricted, such as by binding it to loopback. Set a password before exposing
the gateway to other users or networks.

Passwords must be non-empty and no longer than 72 UTF-8 bytes. Nerve stores
passwords as bcrypt hashes. Unknown usernames and incorrect passwords return the
same response and use the same timing protections.

Sessions created before per-account login are accepted only while exactly one
account exists. They are replaced with an account session on the next request.
Creating a second account requires all users to sign in with a username and
password.

## Request identity

Nerve resolves every authenticated request to either a human actor or the
installation's system actor.

| Credential | Acts as |
|---|---|
| Login session | The account's human actor |
| Nerve CLI and internal API token | The system actor |
| Backend and external MCP token | The system actor |

Disabling an account blocks its next HTTP or MCP request and any new WebSocket
connection. An existing WebSocket keeps the identity it received when it
connected. Autonomous work, including cron jobs and background agents, uses the
system actor rather than a human account.

## Attribution

Sessions store who created them in `created_by_actor_id`. User messages store
who supplied the content in `actor_id`. Both columns contain stable actor IDs;
clients resolve display names through `GET /api/actors`.

| Event | Stored actor |
|---|---|
| A person creates a session or sends a message | That person's actor |
| Nerve creates a session or prompt | The system actor |
| Telegram, Slack, or MCP input without a local account mapping | The system actor |
| Assistant and tool output | None |

Existing records remain unattributed. Nerve does not infer identity from channel
or source metadata. Account and session administration actions are not yet
recorded as audit events.

## Session-signing secret

Nerve uses `auth.jwt_secret` when configured. Otherwise it generates a secret
on first start and stores it in `nerve.db`. The active secret is fixed for the
life of the process, so changing `auth.jwt_secret` requires a restart. Rotating
the secret signs users out.

Nerve refuses authentication until a signing secret and identity store are
available. The database files must be private to the owner; see
[State-file permissions](config.md#state-file-permissions).

## Upgrade compatibility

Existing installations keep their password when account management is enabled.
`auth.password_hash` is deprecated and retained only for compatibility; manage
passwords from the Accounts page.

Do not downgrade in place after enabling account management. Older versions do
not understand account credentials and may treat the installation as
passwordless. Restore a backup taken before the upgrade instead.

## Backup and restore

A normal backup includes `nerve.db`, so it preserves accounts, actor IDs,
password hashes, and the generated signing secret. Treat the archive as a
secret.

`--no-secrets` removes account passwords and the signing secret from the database
snapshot, omits `config.local.yaml`, and replaces credential values in workspace
configuration with environment-variable placeholders. It fails if a
configuration file cannot be parsed safely.

After restoring a `--no-secrets` backup:

- an installation with one account is passwordless until a password is set;
- an installation with multiple accounts cannot accept a login until a password
  is supplied through `auth.password_hash` or a backup with secrets is restored.
