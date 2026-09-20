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

The database contains one system actor for autonomous work. It also creates the
first human account when no account exists. Interactive setup uses the supplied
display name; headless setup leaves it empty.

An account's `credential_source` is `config` for `auth.password_hash`, `local`
for a hash stored on the account, or `none` for passwordless access. Bootstrap
keeps `config` and `none` accounts aligned with configuration but never changes
a local credential.

## Session-signing secret

Nerve uses `auth.jwt_secret` when configured. Otherwise it generates a secret
in `instance_secrets` on first start. Authentication fails closed until startup
has selected a secret.

The selected secret does not change on configuration reload. Adding or changing
`auth.jwt_secret` takes effect after restart and retires the stored secret.
Removing it later generates a new secret instead of restoring the retired one.

The database and its directory must satisfy the state-file permissions described
in [Configuration](config.md). Nerve refuses unsafe state that it cannot repair.

## Backup and restore

A normal backup includes `nerve.db`, so it preserves account and actor IDs and
the generated signing secret. `--no-secrets` removes stored secrets and causes a
new signing key to be generated after restore. Backup and restore files retain
the owner-only protections described in [Setup](setup.md).
