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

## Backup and restore

`nerve.db` is included in backups, so account and actor IDs and the signing
secret survive a normal restore. `--no-secrets` removes `instance_secrets` from
the snapshot, causing a fresh key to be generated on next start. Backup staging,
archives, and restored database files retain the owner-only and hostile-filesystem
protections documented in [Setup](setup.md).
