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

`auth.jwt_secret` is used when configured. Otherwise first start generates a
secret in `instance_secrets`. There is no missing-secret authentication bypass:
HTTP, WebSocket, MCP, and worker-token authentication all fail closed until a
secret is pinned at startup.

The startup pin remains in force across configuration reloads. A newly
configured secret retires the database-held key using SQLite secure deletion;
removing the configured value later generates a fresh key instead of reviving
the retired one. If a database file was readable by other users, a stored key
is treated as compromised, deleted, and unpinned before replacement.

With no password anywhere — none on the account row and no `auth.password_hash`
— every caller who can reach the gateway logs in with any password and acts as
the owner. That is the intended behaviour for a private, loopback-bound install
and a real exposure on anything else: Nerve does **not** change the bind address
or refuse to start over it. Set a password before exposing the gateway beyond
the machine.

**How that first password is set: the setup wizard, and only there.** An
install with one account and no password is *unclaimed*, and the wizard at
`/setup` is what ends that state — it names and secures the account the install
already has, in one transaction, and signs the browser in with the password it
set. It is guarded by a **setup token** printed in the server log, which a
caller whose socket peer is loopback does not need, because being on the machine
is proof enough. See [Setup](setup.md#claiming-an-instance-from-a-browser).

`PUT /api/accounts/me/password` needs no current password on an account that
has none — which is exactly this state — so while the instance is unclaimed it
refuses with a `409` pointing at the claim endpoint. There is one door, and it
is the guarded one. Once the instance has been claimed, the accounts screen is
where passwords are changed as usual.

**Claiming also ends every session that came before it.** A passwordless
install hands an ordinary account session to everybody who reaches it, and
those tokens name the same account and have thirty days left — so securing the
account would otherwise secure only the *next* caller. Each account carries a
**session epoch** (`accounts.session_epoch`, added by `v049`): every session
token records the epoch it was minted under, the account row is compared
against it on the same read that already checks whether the account is
disabled, and the claim bumps the row inside the transaction that sets the
password. The token the claim returns is minted at the new epoch, so the
browser doing the claiming is the one session that survives.

A token with no epoch at all reads as 0 — that is what a token minted before
the column existed carries, and what a grandfathered `sub: "user"` session
carries. An account that has never been claimed is also at 0, so an upgrade
logs nobody out; a claim moves the account to 1 and all of them stop. Nothing
else moves it: a password change does not, because that endpoint hands back no
token and would sign the person changing their password out of the tab they
changed it in.

**Open WebSockets end too.** A socket authenticates once, at accept, and is
then held for hours — so the claim closes every open connection whose account
has moved on (policy code `1008`), and every inbound frame is re-checked
against the account row before anything is done with it. Both halves are
needed: closing is what stops a stale socket *receiving* the owner's
transcript without ever speaking, and the per-frame check is what stops it
*acting* if the close never reached it.

The connection's **actor is never rewritten** — a socket that may no longer act
is closed, not re-pointed, because re-pointing it would attribute the next
message on it to somebody who did not send it. Messages already attributed to
the earlier actor stay as they are.

Disablement gets the same treatment for free: it used to take effect at the
account's next *request* and never on an open socket, and now the socket ends
at its next frame as well.

One limit worth knowing: the epoch is per *account*. It is the right shape for
"end every session on this account" and no shape at all for "sign out this one
device", which nothing here offers.

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

A standalone passwordless install opens the accounts screen, where its owner can
set a username and password. The browser derives that first-run route directly
from `login: "none"`.

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

The response budget is **calibrated from the highest work factor in use** —
every account's own hash and the configured `auth.password_hash` — rather than
from the current policy cost. Reacting to a slow comparison after making it
would be one probe too late: that request has already taken four times as long
as an unknown username did, and that is the whole of an enumeration.

It is recomputed on **every** login, not once, because what it depends on
changes under a running gateway: an account still reading `auth.password_hash`
picks up a configuration reload immediately, at whatever work factor the new
value carries. Only the per-comparison measurement is cached — that is a
property of the machine — so a recalculation costs an exponent and no extra
query. It comes *down* as well as up: when the last odd work factor is re-hashed
at the policy cost, the next login stops paying for it.

An operator who stores a very high work factor makes every *failed* login that
slow for as long as the account keeps it, which is the honest price of hiding
it. **Above about half a minute per comparison the padding stops pretending:**
any work factor is still *accepted* — refusing to verify a hash would lock out
the install that carries it, which is the one thing this release promises not to
do — but a comparison slower than that ceiling takes longer than the budget
however long the budget is, so such an account is distinguishable by timing. It
is also an account nobody can sign into in a reasonable time, so the remedy is
to fix the work factor rather than to keep padding for it.

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
- **A WebSocket's identity and authority are fixed when it connects.** Renaming
  does not rewrite it, and disabling the account takes effect when that socket
  reconnects. HTTP, MCP, and new WebSocket connections re-check the account
  immediately.

Autonomous work — cron jobs, channel traffic, background agents, and the
instance talking to itself — acts as the **system principal** rather than as
whoever happens to have an account. That keeps "the agent did this" and "a
person asked for this" apart, and keeps it true after an account is renamed or
removed.

### Sessions that predate this version

Browsers may hold 30-day session tokens issued before accounts existed. They
name no account, so they are handled narrowly:

- with **exactly one account**, such a token resolves to that account, and the
  reply carries a proper per-account token in the `X-Nerve-Token` header, which
  the browser stores. One request per tab and the old shape is gone;
- with **two or more accounts** it is refused (`401`) rather than resolved to
  whichever account sorts first; those tabs must log in again.

The acceptance is temporary and is removed in a later release. Nothing mints
that shape any more.

## What gets attributed, and what does not

Two nullable columns hold the answer, and both hold an **actor id** — never a
name:

| Column | Means |
|---|---|
| `sessions.created_by_actor_id` | who caused this session to exist |
| `messages.actor_id` | whose input this message records |

A name is a snapshot. Storing one would freeze it the moment somebody is
renamed, so the id is what is stored and the name is looked up when something
is displayed (`GET /api/actors`). Renaming an account changes every label and
rewrites nothing.

**Who ends up on a row:**

| The row | The actor |
|---|---|
| A session you created — from the sidebar, a fork, an approved plan | you |
| A message you typed — in the browser, over the WebSocket, through `/api/chat`, or composed and deferred with "run later" | you |
| A session or a prompt the instance produced for itself — a cron generation, a scheduled wakeup, a webhook, a workflow leg, a plan's implementation prompt, a relayed notification answer | the agent's system principal |
| A Telegram or Slack message, or imported Codex human input | `NULL` until a provider-person mapping exists |
| An MCP satellite session, or a Codex session the sync creates | the system principal |
| Anything the assistant or a tool produced, including Nerve's own status lines written in the assistant's voice | nobody — the column stays `NULL` |

The dividing line for a message is **who supplied the content**. A person
clicking "approve" on a plan gets the implementation session, because they
caused it; the implementation prompt itself is the instance's, because nobody
typed it. Recording the person on the *earlier* action — the approval, the
schedule, the task edit — is a separate row of the attribution map and is not
implemented yet.

Assistant and tool rows stay unattributed on purpose. Their authorship is
`role`, and turning "the model answered" into "a person wrote this" would be
false. Linking a turn back to whoever prompted it is a separate column
(`caused_by_actor_id`) that this release does not add.

**External people stay unidentified.** Telegram, Slack, and imported Codex
messages do not carry a Nerve account identity. Their `actor_id` is `NULL`
until a provider-person mapping exists; transport metadata remains provenance.

**History from before this version says nothing about who wrote it**, and that
is deliberate. Both columns are `NULL` on every pre-existing row, nothing is
backfilled, and no actor is inferred from a session's `source` or a message's
`channel`: those are legacy provenance strings, not identity bindings, and
turning them into attribution would invent an audit trail that never existed.
Anything reading these columns has to treat `NULL` as "not recorded" rather
than as an error.

**A stored id always resolves.** Both columns reference `actor_refs(id)`, so an
id that names nobody cannot be written — attribution nobody can look up would
render as a blank name. That is safe precisely because actor rows are never
deleted and accounts are tombstoned rather than removed (above). `NULL` is
exempt, so unattributed history is unaffected.

**`NULL` means attribution was not recorded.** That includes legacy history,
unidentified external people, and assistant/tool output. Autonomous Nerve work
uses the migration-guaranteed system actor cached by its database.

**Ids are permanent.** If an install later moves to an external identity
provider, work done before the move keeps the local actor ids it has now and
new work gets the provider's, with nothing merged and no history rewritten. The
same person can legitimately appear as two actors, and both are real.

What is *not* attributed yet, and is known to be missing: renaming, archiving,
deleting, stopping or resuming a session; task and schedule changes; who
answered a notification; and who requested or decided a workflow or a review.
Those are the remaining rows of the attribution map.

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

`nerve.db` is part of every backup, so a restore brings back the same actor and
account IDs and the same signing secret.

A `--no-secrets` bundle carries **no credential at all**, which takes three
things beyond omitting the obvious files:

- `instance_secrets` is emptied in the snapshot of `nerve.db`, so the restored
  instance generates a fresh signing secret on its first start;
- every account's password hash is emptied too, and those rows move to
  `credential_source = 'none'`;
- and the workspace's `config/*.yaml` is **rewritten** on the way into the
  bundle, with credential-shaped values replaced by `${ENV_VAR}` placeholders.
  That last one is not hypothetical: the startup migration deliberately leaves
  `auth.password_hash` alone when it is in a tracked or fleet-managed file (see
  above), so a live verifier can be sitting in exactly the file the bundle
  otherwise copies verbatim. A cron job's `env:` block goes the same way.

A `--no-secrets` backup **refuses** rather than guessing if one of those
configuration files cannot be parsed: "there is nothing to rewrite" and "I could
not look" are different answers, and only the first is compatible with promising
the bundle carries no credential. Fix the file, or take the backup with secrets
and keep it as private as the instance itself.

Every account comes back **passwordless**, including one still on the
transitional `config` source. Its credential lives in `config.local.yaml`, which
this bundle omits, so leaving it on `config` would restore an account that can
neither authenticate nor be recognised as passwordless — the one state with no
way out of it, and reachable simply by backing up between an upgrade and the
first start that migrates.

The live database and the real config files are never touched; the snapshot is
edited after it is taken and before it is checksummed, SQLite overwrites the
freed pages rather than merely unlinking them, and the rewritten config is
staged through the same owner-only, create-then-verify path everything else
credential-bearing uses.

The consequence is worth knowing before you restore one: such a bundle omits
`config.local.yaml` as well, so `auth.password_hash` does not come back either.
With **one** account the restored instance is passwordless, which is the
ordinary first-run state — set a password. With **two or more**, nobody can sign
in until a password is configured (`auth.password_hash` applies to every account
that has none, and the next start copies it onto each row) or a bundle *with*
secrets is restored. That is the honest consequence of restoring a backup that
deliberately carries no credential.
