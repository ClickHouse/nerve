# Accounts and identity

Nerve keeps two things apart that are easy to conflate: **who a request is
attributed to** and **how a person logs in**. This page describes the local
identity model, the bootstrap that creates it on first start, where the login
credential lives, and where the session-signing secret comes from.

Today every install has exactly one account, created automatically. Nothing on
this page asks you to do anything; it explains what is there so the log lines,
the `nerve migrate --dry-run` output and the tables in `nerve.db` make sense.

## Identity mode

`auth.mode` selects how the instance learns who is making a request. The only
value this version implements is `local`: local accounts plus a session token,
with the decision made in process. It is deliberately an explicit setting rather
than something inferred from which credentials happen to be present, and it is
startup-only — it never follows a config reload.

It is also **machine-local**: it is read from `config.yaml` or
`config.local.yaml` on the box, or from `NERVE_AUTH_MODE` in the environment
(which wins over both), and never from the tracked `workspace/config/settings.yaml`.
A value there is ignored with a warning, so a configuration push or a workspace
sync can never change how an instance authenticates — nor crash it. The mode is
resolved from the machine layers *independently of lockdown*: a locked box still
reads it from its own `config.yaml`/`config.local.yaml` (or `NERVE_AUTH_MODE`),
so flipping the tracked `lockdown` flag — which otherwise drops the machine
layers — cannot reset the mode either. Any other value is a hard error at
startup and in `nerve config validate`.

The `auth` section as a whole is read carefully, because it is the one section
whose *disappearance* weakens an instance rather than resetting it to a
default. An `auth:` that is present but not a mapping (`auth: something`) is a
hard error rather than being read as "no authentication"; an `auth:` with
nothing under it is an empty overlay, so a bare line in a higher-precedence
file leaves the password hash and signing secret in the file below it exactly
where they were. Both rules apply per file, before the layers are merged, and
`nerve config validate` reports what startup would.

## The two tables

| Table | Holds | Exists in |
|---|---|---|
| `actor_refs` | attribution identity: a stable id, a `kind` (`human` or `system`), a display name, an email and a profile version | every mode |
| `accounts` | local login state: an optional username, where the credential lives, whether the account is enabled | `local` mode only |

The **actor** is the identity. Its id is what sessions and messages will
reference; the display name and email are presentation snapshots (renaming
bumps `profile_version` and rewrites nothing that was attributed before). In
local mode `email` is always empty — Nerve does not need anyone's email; the
login identifier, when one is needed, is a username.

Every install also carries the rows a hosted deployment would provision around
them, so a later move is a data question rather than a redesign: one **tenant**,
one **agent** with its **system principal** (an `actor_refs` row of kind
`system` — the identity that cron jobs, channel traffic and other autonomous
work act as), the owner's **membership** in the tenant and a recorded
**owner grant** on the agent. The grant confers nothing locally: every local
account has full permissions. Ids are UUIDs.

## The bootstrap

On start, after the schema migration, Nerve checks `nerve.db` and — only while
the `accounts` table is empty — creates the local owner: the actor, the account,
the membership and the grant, alongside the tenant and agent. Repeated starts
find the same rows; the ids never change. A disabled account is still a row, so
disabling one is durable: bootstrap never recreates what an operator removed.

The account's `credential_source` says where its password is:

| Value | Meaning |
|---|---|
| `config` | `auth.password_hash` in your configuration. Nothing is copied, no file is rewritten; a lockdown install whose hash is an `${ENV_VAR}` reference keeps working unchanged |
| `none` | passwordless |
| `local` | a hash stored on the account row (not produced by this version; reserved for account management) |

An existing install with a password gets `config`; a passwordless one, and a
fresh install, gets `none`. The username stays empty — password-only login
remains valid while exactly one account exists, so nothing needs one yet. The
display name is the answer you gave `nerve init` at "Your name": the installer
creates the account itself, in the same run, because nothing it writes carries
that answer. A headless (`--non-interactive`) install collects no name, so its
owner is unnamed until something sets one.

While the account is on `config` or `none`, the value is re-derived from
configuration at every start: add `auth.password_hash` to a passwordless install
and the row moves to `config`; remove it and the row moves back. A row that has
moved to `local` is never touched by this.

`nerve migrate --dry-run` shows what the bootstrap would do before it happens;
`nerve start`, `nerve upgrade` and `nerve migrate` report what it did. The
gateway repeats the check on every start, so an install is never served without
an account.

## Passwordless

With no `auth.password_hash`, every caller who can reach the gateway logs in
with any password and acts as the owner. That is the intended behaviour for a
private, loopback-bound install and a real exposure on anything else: Nerve does
**not** change the bind address or refuse to start over it. Set a password
before exposing the gateway beyond the machine.

## The signing secret

Session tokens are signed with `auth.jwt_secret` when it is configured. When it
is not, the first start generates a secret and keeps it in `nerve.db`
(`instance_secrets`) — machine-local state, never written into a config file.
There is no unauthenticated mode: the old behaviour, where an empty secret made
the gateway accept every request and the login route mint tokens signed with a
fixed string, is gone, and until a secret is in force every request — HTTP, the
WebSocket handshake and the MCP endpoint — is refused, locked or not.

- **The secret is pinned at startup.** Whichever value is in force when the
  gateway starts — configured or generated — stays in force for the life of the
  process. A config reload or workspace sync that changes or removes
  `auth.jwt_secret` is reported as needing a restart and changes nothing live:
  removing the key does not reopen the instance, and rotating it does not swap
  the key under live sessions half-way. The next restart applies it.
- A configured `auth.jwt_secret` wins over the stored one at startup. Setting it
  on an install that had generated one rotates the secret at the next restart;
  every open tab logs in again once.
- Under lockdown the same applies: a locked box without a configured secret
  generates one rather than refusing every request. Supplying it from the fleet
  configuration lets you rotate it centrally.
- **A configured secret retires the stored one.** The first start with
  `auth.jwt_secret` set deletes the database-held secret (securely, so it does
  not linger in freed pages). If the key is later removed from configuration,
  the next start generates a *fresh* secret rather than reviving the old one:
  tokens signed with a retired key never become valid again, which is what
  rotation is for.
- **A file another user could have changed is not opened at all.** `nerve.db`
  carries the accounts and may carry the signing secret, so every open — the
  gateway, each CLI command, the installer — applies the same policy before
  anything else happens. It first *inspects*: if the state directory or any
  database file (`nerve.db`, `-wal`, `-shm`) is group- or world-writable, or its
  mode cannot be read, Nerve refuses to open it — no migration, no repair —
  because another user could have altered the contents, and fixing the mode
  would only hide that. The message names the file, its mode and the remedy:
  `chmod 700` the directory, `chmod 600` the files, then start again. Nerve
  creates its own state directory `0700`, so a fresh install never sees this;
  a `~/.nerve` created by an older version under a permissive umask (`0775` is
  what a `002` umask leaves) is refused once, and that one `chmod 700` is the
  acknowledgement.
- **A generated secret only lives in a file this user alone can read.** A file
  that is merely *readable* by others is repaired — directory `0700`, files
  `0600` — and the result re-checked, never assumed from `chmod` succeeding. A
  stored signing secret that was readable before the repair is treated as
  copied and retired: on disk, and in memory too if this process had it pinned,
  so nothing accepts tokens signed with it while the replacement is generated
  (every open tab logs in again once). On a filesystem that cannot represent
  modes the check fails, and Nerve will not keep a secret there: with
  `auth.jwt_secret` configured it starts and logs an error naming the file and
  its mode (nothing secret is stored in the database in that case); without one
  it refuses to start, and the message gives the two ways out — fix the
  permissions, or set `auth.jwt_secret` in `config.local.yaml` or the
  environment. A restore holds itself to the same rule: the destination
  directory is made and verified `0700` first, `nerve.db` is written through a
  temporary *created* `0600` (verified before a byte is copied) and renamed into
  place, and the restore aborts rather than continue if either cannot be
  guaranteed. `config.local.yaml` — the password hash and the machine-local
  secrets — is installed the same way, and a restore that cannot install it
  fails rather than quietly leave the instance without a configured password.
- **A backup bundle is as sensitive as what it holds.** It carries `nerve.db`
  and, unless `--no-secrets` was used, `config.local.yaml`, so it is created
  `0600` before a byte is written to it rather than at whatever the umask
  gives, and written through that same open file rather than by reopening its
  name — which is what stops anyone who can write to the backup directory from
  redirecting the bundle to a file of their own. A filesystem that cannot keep
  it private refuses a backup that would carry secrets, and warns when
  `--no-secrets` means it does not.
- **A file that would hold your secrets in the clear is not written.** The
  wizard's `config.local.yaml` — API keys, password hash, signing secret — and
  its `init-state.json` checkpoint are created owner-only the same way: the
  mode is set on creation and checked on the open file *before* anything is
  written. If the filesystem will not honour it (a share or a volume without
  Unix permissions), `nerve init` stops with nothing written rather than
  leaving those readable by everyone with an account on the machine; put the
  configuration on a filesystem with permissions, or keep the secrets in the
  environment and reference them as `${VAR}`. Adding a Telegram user rewrites
  the same file, and reports the pairing as not saved rather than republishing
  its contents.
- **Every command opens the database the same way.** `nerve sync`, `nerve cron`,
  `nerve db prune`, `nerve db vacuum` and `nerve workflow list|status` open
  `nerve.db` exactly as the gateway does — the policy above, the migrations,
  then the identity bootstrap — so the first command run after an upgrade
  leaves the same owner account, system principal and signing secret `nerve
  start` would. `nerve migrate --dry-run` inspects without opening.
- The CLI (`nerve reload`, `nerve codex token`) reads the stored secret from
  `nerve.db`, so it authenticates to the daemon on the same box without any
  configuration. With no secret anywhere — the daemon has never started — it
  refuses rather than sending a request the gateway would reject.
- To rotate a generated secret, set `auth.jwt_secret`, or delete the row
  (`DELETE FROM instance_secrets WHERE name = 'jwt_secret'`), and restart.

## Backups

`nerve.db` is part of every backup, so a restore brings back the same actor,
account, tenant and agent ids and the same signing secret. A `--no-secrets`
bundle empties `instance_secrets` in the snapshot, exactly as it omits
`config.local.yaml`; the restored instance generates a fresh secret on first
start.
