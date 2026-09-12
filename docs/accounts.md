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
| `local` | a bcrypt hash stored on the account row. Where every account's password lives |
| `none` | passwordless |
| `config` | `auth.password_hash` in your configuration. Transitional — see [Moving off the configuration password](#moving-off-the-configuration-password), which every install does automatically at the first start on this version |

An existing install with a password gets `config` and is then moved to `local`
in the same start; a passwordless one, and a fresh install, gets `none`. The username stays empty — password-only login
remains valid while exactly one account exists, so nothing needs one yet. The
display name is the answer you gave `nerve init` at "Your name": the installer
creates the account itself, in the same run, because nothing it writes carries
that answer. A headless (`--non-interactive`) install collects no name, so its
owner is unnamed until something sets one.

While the account is on `config` or `none`, the value is re-derived from
configuration at every start: add `auth.password_hash` to a passwordless install
and the row moves to `config` — and straight on to `local`, with the hash copied
onto the row. A row on `local` is never touched by this.

`nerve migrate --dry-run` shows what the bootstrap would do before it happens;
`nerve start`, `nerve upgrade` and `nerve migrate` report what it did. The
gateway repeats the check on every start, so an install is never served without
an account.

## Passwordless

With no password anywhere — none on the account row and no `auth.password_hash`
— every caller who can reach the gateway logs in with any password and acts as
the owner. That is the intended behaviour for a private, loopback-bound install
and a real exposure on anything else: Nerve does **not** change the bind address
or refuse to start over it. Set a password before exposing the gateway beyond
the machine; the accounts screen is where.

**Passwordless is bounded to one account.** With two accounts it is not a weaker
login, it is an unanswerable question: nothing distinguishes the callers, so
every one of them would be whoever the code picked. So a second account cannot
be created while the instance is passwordless — the create is refused with a
`409` saying to set a password first, rather than startup being refused, which
would break the upgrade promise in an unrelated way.

## Account management

Every account has full permissions. Any account can list, create, rename,
disable and re-enable accounts, and any account can use the agent. There is no
role model and no account-management bit.

The consequence is worth stating plainly rather than discovering later:
**adding a person gives them the power to remove you.** That is acceptable for
a trusted team and it is the chosen property, not an oversight. The accounts
screen says so on the form.

The only guard is **the last enabled account cannot be disabled**, which is what
stops an install locking everybody out. Everything else is allowed right up to
that point.

Two more refusals, both about the *instance* rather than about the request, and
both reported as `409` with what to do first:

- a second account cannot be created while the instance is passwordless (above);
- a second account cannot be created while an existing account has no username,
  because an account without one cannot be signed in to once a username is
  required.

Setting a password is own-account only. Nobody can set anyone else's — and an
account that already has one must supply it, so a stolen session token is not on
its own enough to take the account over. The account that has *no* password yet
is the single exception, which is also the state the whole screen exists to end.

### Usernames

A username is a **lookup key, not an identity**. The identity is the actor id,
which never moves: renaming an account changes what it signs in as and what is
displayed, and rewrites nothing that was attributed to it.

| Rule | Value |
|---|---|
| Pattern | `^[a-z0-9][a-z0-9._-]{1,31}$` after trimming and lower-casing |
| Length | 2–32 characters |
| Case | stored lower-cased; uniqueness is case-insensitive, so `Alice` and `alice` are one name |
| Reserved | `user`, `admin`, `system`, `nerve`, `agent-system`, `backend-agent`, `external-agent-mcp`, `root`, `me` |

ASCII only, deliberately: SQLite's case-insensitive collation folds ASCII and
nothing else, so a character set with no non-ASCII letters in it leaves no room
for a look-alike to sit beside an existing name. `user` is reserved because the
session tokens issued before per-account logins use that exact string as their
subject (see [Sessions that predate this version](#sessions-that-predate-this-version));
`agent-system`, `backend-agent` and `external-agent-mcp` are the other token
subjects; `me` is a URL path segment; the rest read as an authority this model
does not have.

### There is no way to delete an account

Deliberate. Removal is **disablement**, and the row stays forever — it is the
tombstone.

The reason is the grandfather clause. Sessions issued before per-account logins
carry `sub: "user"` and resolve to the sole account *while exactly one account
exists*. If an install that had two accounts could go back to one, every such
token sitting in a browser would start resolving again — to whichever account
happened to remain, which is somebody else's. Keeping the row makes the account
count monotone, so "exactly one account has ever existed" is a one-way door: the
moment a second account is created, grandfathered tokens, password-only login
and passwordless access are all off, permanently.

It also matches the attribution rule: `actor_refs` rows are never deleted
either, because the sessions and messages a later release attributes will point
at them for good.

## Logging in

Login takes a password and, once it is needed, a username.

| Accounts | What the form collects | Why |
|---|---|---|
| one, passwordless | nothing (any password is accepted) | there is nothing to ask |
| one, with a password | a password; a username is accepted but not needed | the account an upgrade created has no username, so requiring one would lock the install out |
| two or more | a username and a password | a password alone names nobody |

`GET /api/auth/status` says which of the three applies, so the browser shows the
right fields without guessing (see [the API reference](api.md)). Existing users
keep typing just a password and notice nothing.

The same descriptor carries `setup_pending`, which is true while the one account
has no password — the state a headless install lands in. Giving that account a
username does not clear it: a named account with no password admits exactly as
many people as an unnamed one, so only setting a password does.

Wrong username and wrong password give the same answer — a `401` reading
`Invalid username or password` — and they take the same **time**, which takes
more doing than the same words:

- a username that names nobody still costs a full password comparison, against a
  fixed decoy hash, so "did any hashing happen" says nothing;
- an *empty* password is compared like any other rather than refused early,
  because returning immediately for one was a probe that needed no password at
  all;
- and every refusal waits out a common response budget before answering, so how
  long a particular account's hash takes to check says nothing either. See the
  cost policy below for why that varies at all.

A **disabled** account is refused with a message saying so, but only after its
password has checked out, so that answer reaches the person who knew the
password and nobody else.

### Password hashes and their cost

Passwords are stored as bcrypt hashes at a fixed work factor (currently 12).
Hashes are *accepted* at any work factor, because a password copied off an
upgrading install's configuration carries whatever produced it, possibly years
ago, and refusing it would lock that install out.

A hash at any other cost is **replaced at the current one the next time its
owner logs in successfully**. It happens silently, on a login that has already
been accepted, and it does not change anybody's password — the hash is
recomputed from the password that was just typed. An install therefore converges
on the current cost without anybody being asked to do anything, and the response
budget above is what keeps the assorted costs from being visible in the meantime.

Two exceptions, both deliberate: an account whose credential still lives in
configuration is left alone (moving it is the startup migration's job, not a
side effect of somebody logging in), and so is a password longer than bcrypt's
72 bytes, which is accepted by truncation on an old hash and could not be
re-hashed without storing something its owner does not type.

**Creating the second account is the moment three things change**, all at once
and all for the same reason:

1. sessions issued before per-account logins stop resolving;
2. password-only login stops being unambiguous, so a username is required;
3. passwordless access is refused.

In practice the install that adds its second person makes its open tabs sign in
again at that moment, which is correct and explainable.

## Moving off the configuration password

`auth.password_hash` in configuration was always a transitional home for the
credential: it is what an upgrading install had, and copying it at the time
would have made that upgrade irreversible. Every install leaves it at the first
start on this version.

At startup, for every account still on `credential_source = 'config'`, the
configured bcrypt hash is **copied** onto the account row and the row moves to
`local`. A copy, not a re-hash — **nobody's password changes** and every open
session stays valid. `nerve migrate --dry-run` shows it before it happens, and
`nerve start`, `nerve upgrade` and `nerve init` report it after.

What happens to the now-dead configuration value depends on the install:

- **Ordinary install.** The key is removed from this box's own `config.yaml`
  and `config.local.yaml`. Both are machine-local and gitignored. The rewrite
  goes through the same owner-only writer everything credential-bearing does:
  created `0600`, the mode confirmed on the open file before a byte is written,
  and nothing written at all if the filesystem will not honour it (in which case
  the value is left alone and a warning says to remove it by hand). Comments at
  the top of the file are kept; comments further down are not.
- **Lockdown install.** Nothing is written. Configuration is fleet-managed and
  the value may be an `${ENV_VAR}` reference the next push reasserts. A startup
  warning names the file and the key and says plainly that **the fleet-managed
  value no longer authenticates anybody** — a fleet that rotates the password
  through configuration after this point would otherwise believe it had changed
  a credential when it had not.
- **A value in the tracked `workspace/config/settings.yaml`** is reported the
  same way on any install: it is shared configuration, not this box's to
  rewrite.

A configured `auth.password_hash` that no account reads is logged once at every
start, and `nerve doctor` says the same thing. That is the kind of thing an
operator otherwise debugs for an hour.

`config` stays a *readable* credential source for one release, so a downgrade to
the previous version still authenticates against `auth.password_hash` if it is
still there. The enum value is removed in the release after this one; nothing
creates a `config` row any more.

### Rollback

**Do not downgrade in place after this release. Restore a backup taken before
the upgrade instead.** Older code knows nothing about account rows, so:

- **a downgrade silently strips access from any second account.** There is one
  credential again — the configured one — and it belongs to whoever is left;
- worse, on an ordinary install `auth.password_hash` has been *removed* from
  configuration by then, and older code reads a config with no password hash as
  **passwordless**: it admits every caller who can reach the gateway. The
  migration says this in a warning at the moment it removes the key.

A lockdown install is the one case that downgrades cleanly, because its
configuration was never rewritten.

Documented rather than prevented: there is no way to make old code understand
rows it has never heard of.

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
account, tenant and agent ids and the same signing secret.

A `--no-secrets` bundle carries **no credential at all**, which now means two
things inside `nerve.db` as well as the files it omits:

- `instance_secrets` is emptied in the snapshot, so the restored instance
  generates a fresh signing secret on its first start;
- every account's password hash is emptied too, and those rows move to
  `credential_source = 'none'`.

The live database is never touched; the snapshot is edited after it is taken and
before it is checksummed, and SQLite overwrites the freed pages rather than
merely unlinking them.

The consequence is worth knowing before you restore one: such a bundle omits
`config.local.yaml` as well, so `auth.password_hash` does not come back either.
With **one** account the restored instance is passwordless, which is the
ordinary first-run state — set a password. With **two or more**, nobody can sign
in until a password is configured (`auth.password_hash` applies to every account
that has none, and the next start copies it onto each row) or a bundle *with*
secrets is restored. That is the honest consequence of restoring a backup that
deliberately carries no credential.
