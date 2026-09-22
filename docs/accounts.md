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

A successful password change advances that account's session epoch. Every
older HTTP token becomes stale and every open WebSocket is best-effort closed;
the response carries a replacement token for the tab that proved the current
password. This is the incident-response path for revoking a copied session.

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
| `setup` | Setup is not complete | None; login is refused, only the setup claim is permitted |
| `none` | One account with no password, chosen at setup | None; any password is accepted |
| `password` | One account with a password | Password |
| `username_password` | Two or more accounts | Username and password |

Setup state belongs to the installation. Passwords belong to accounts. The two
are independent:

| Setup complete | Password | State |
|---|---|---|
| No | No | Only the setup claim is permitted |
| Yes | No | Passwordless by choice, one account |
| Yes | Yes | Password-protected |

A password always completes setup. Nerve records completion in the
`instance_setup` table of `nerve.db`.

A passwordless installation is safe only when access to the gateway is already
restricted. Anyone who can reach it is the owner and can set the first password.
A password cannot be removed.

### Choosing passwordless

An installation becomes passwordless only by an explicit choice:

- `nerve init` asks for a password. An empty answer asks you to confirm
  "Keep this installation passwordless".
- `nerve init --non-interactive` reads `NERVE_PASSWORDLESS=1`. It refuses
  `NERVE_PASSWORD` and `NERVE_PASSWORDLESS=1` together. With neither, setup
  stays incomplete, and you complete it in the browser.
- The browser setup page has a "Keep this installation passwordless" option.

The installer reads these values once. Setup completion is persistent, so
changing the environment later has no effect.

### Claiming the first account

When setup is not complete, open `/setup` and enter the setup token shown by
`nerve status`. Then either set the first account's username and password, or
keep the installation passwordless. The display name is optional. Send the token
only in the JSON request body. For a remote claim, use HTTPS or a protected
tunnel.

Until the claim succeeds, `POST /api/setup/claim` is the only permitted account
write and login is refused. The claim updates the account, records setup as
complete, and increments the session epoch in one transaction. Earlier sessions
are invalidated, their open WebSockets are closed, and the response returns the
replacement token. The setup token is then invalidated.

See [Setup](setup.md#claiming-an-instance-from-a-browser).

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
connection. Open WebSockets re-check the account on each frame and close when
the account is no longer authorized. Autonomous work, including cron jobs and
background agents, uses the system actor rather than a human account.

## Attribution

Sessions store who created them in `created_by_actor_id`. User messages store
who supplied the content in `actor_id`. Both columns contain stable actor IDs;
clients resolve display names through `GET /api/actors`.

| Event | Stored actor |
|---|---|
| A person creates a session or sends a message | That person's actor |
| Nerve creates a session or prompt, including MCP and Codex sessions | The system actor |
| Telegram, Slack, or imported Codex input | None until a local identity mapping exists |
| Assistant and tool output | None |

Existing records remain unattributed. Nerve does not infer identity from channel
or source metadata. Account and session administration actions are not yet
recorded as audit events. Missing attribution means legacy history, unidentified
external input, or assistant/tool output; autonomous work uses the system actor.

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

An existing installation without a password has setup complete after the
upgrade, so it stays passwordless. An installation is existing when its
database has chat sessions from before accounts.

Do not downgrade in place after enabling account management. Older versions do
not understand account credentials and may treat the installation as
passwordless. Restore a backup taken before the upgrade instead.

## Backup and restore

A normal backup includes `nerve.db`, so it preserves accounts, actor IDs,
password hashes, and the generated signing secret. Treat the archive as a
secret.

`--no-secrets` removes account passwords, the signing secret and the setup
completion record from the database snapshot, omits `config.local.yaml`, and
replaces credential values in workspace configuration with environment-variable
placeholders. It fails if a configuration file cannot be parsed safely.

After restoring a `--no-secrets` backup:

- an installation with one account requires setup again, with a new setup
  token;
- an installation with multiple accounts cannot accept a login until a password
  is supplied through `auth.password_hash` or a backup with secrets is restored.
