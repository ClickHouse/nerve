# Accounts and actor identity

Nerve uses local accounts for login and stable actor IDs for attribution. In
[external mode](#external-mode), a gateway names the person behind each request
and Nerve does not use local accounts.

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

- The default `nerve init` asks nothing about accounts: it prints the setup
  token and leaves the choice to the browser setup page, whose first step
  has a "Keep this installation passwordless" option.
- `nerve init --terminal` asks for a password. An empty answer asks you to
  confirm "Keep this installation passwordless".
- `nerve init --non-interactive` reads `NERVE_PASSWORDLESS=1`. It refuses
  `NERVE_PASSWORD` and `NERVE_PASSWORDLESS=1` together. With neither, setup
  stays incomplete, and you complete it in the browser.

The installer reads these values once. Setup completion is persistent, so
changing the environment later has no effect.

### Claiming the first account

While setup is not complete, login is refused. Open `/setup`, enter the setup
token that `nerve init` printed (`nerve status` shows it again), and set a
username and password or keep the installation passwordless. The claim
updates the existing account, records setup as complete and deletes the
token, in one transaction. It is the first step of the setup wizard: on
success you are signed in and the wizard continues with the Mode step. See
[Setup](setup.md#the-setup-token).

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
connection, and closes its open WebSocket connections. Autonomous work, including cron jobs and background agents, uses the
system actor rather than a human account.

### External mode

With `NERVE_AUTH_MODE=external`, a gateway in front of Nerve signs people in
and names the person behind each request. See
[Authentication mode](config.md#authentication-mode).

| Credential | Acts as |
|---|---|
| `X-Nerve-Actor-Context` header | The human actor that the header names |
| Login session | Refused with `401` |
| Nerve CLI and internal API token | The system actor |
| Backend and external MCP token | The system actor |

The header is a compact JWS. Nerve reads `principal_id` and
`profile.display_name` from its payload. It does not check the signature or
any other claim. `principal_id` must be a UUID, and Nerve uses its lowercase
form as the actor ID. `profile.display_name` must be a string, `null`, or
absent. A header that Nerve cannot read, a header that names the system actor,
and a request with more than one header give `401`. When the header is
present, Nerve does not read `Authorization`, the `nerve_token` cookie or
`?token=`. The gateway also sends the header on the WebSocket upgrade request.
The connection keeps the actor of that request until it closes.

Nerve adds an `actor_refs` row of kind `human` when it sees a principal for the
first time, and writes the display name again when it changes. The row has no
`accounts` row. Nerve does not check the access of the person; the gateway
does.

In external mode:

- `POST /api/auth/login` returns `404`. The account routes (`/api/accounts`)
  and the setup claim (`/api/setup/claim`) are not available. The setup
  checklist (`/api/setup`, the `/setup` page) is: any person the gateway
  names may run it, and it never reads local-account state. See
  [Instances with external authentication](setup.md#instances-with-external-authentication).
- Startup creates no account and deletes a setup token from an earlier local
  mode. It still makes the signing secret, because system and MCP tokens need
  it.
- Accounts and history from an earlier local mode stay in `nerve.db`.
- Each model request names the principal that caused the work, in the
  `X-Nerve-Inference-Principal` header: the session's cause, or the system
  actor for Nerve's own model calls. See
  [Inference principal header](config.md#inference-principal-header).

> **Warning:** Nerve trusts the header. Use external mode only when the
> gateway is the only caller that can reach Nerve. A caller that can send the
> header can act as any person, also as the actor of an account from an earlier
> local mode.

## Attribution

Nerve can record two actor IDs for a change:

- The **actor** made the change: a person through the web UI, the REST API or
  the browser WebSocket, or the system actor when the agent or Nerve did it (a
  tool call, a cron job, any internal process).
- The **cause** is the principal on whose behalf the change was made. For a
  change that a person made directly, the cause is that person. For work by
  the agent, the cause is the cause of its session.

These columns store actor IDs:

| Column | What it records |
|---|---|
| `sessions.created_by_actor_id` | Who created the session |
| `sessions.caused_by_actor_id` | The session's cause: on whose behalf all agent work in the session runs |
| `messages.actor_id` | Who supplied the content of a user message |
| `task_events.actor_id`, `task_events.caused_by_actor_id` | Actor and cause of a task status change |
| `task_status_events.actor_id`, `task_status_events.caused_by_actor_id` | Actor and cause of a change to the set of task statuses |
| `session_events.actor_id`, `session_events.caused_by_actor_id` | Actor and cause of a session change (see [Session changes](#session-changes)) |
| `session_deletions.actor_id`, `session_deletions.caused_by_actor_id` | Actor and cause of a session delete |

`nerve/db/actor_columns.py` lists every attribution column. Clients resolve
names through `GET /api/actors`, which returns every actor that these columns
can name: the display name, or the username when none is set. The web UI shows
the system actor with a person as cause as "Nerve for <person>".

### The session's cause

Every session has one cause. Nerve stores it when it creates the session and
never changes it:

| Session | Cause |
|---|---|
| A person creates or forks a session (web UI, REST API, browser WebSocket) | That person |
| A cron job or a hook session | The system actor |
| A plan implementation session after an approval through the REST API | The person who approved |
| A plan implementation session after the agent approved the plan | The cause of the session in which the agent approved it |
| A planner session that a revision request creates | The person who asked for the revision |
| A workflow leg session | The system actor |
| A Telegram, Slack or other channel session, or an imported Codex session | None until a local identity mapping exists |
| An external MCP client's session | None |

Everything the agent does in a session (a tool call, a task it changes, a plan
it approves, a session it starts) records the system actor as actor and the
session's cause as cause. A change that a person makes directly records that
person as both. `messages.actor_id` keeps naming who wrote each message; there
is no cause per message or per turn.

This is an approximation in two places:

- In a session that several people use, the agent's work in every turn is
  attributed to the person who started the session, also in turns that
  another person sent.
- A deferred message ("run later") or a wakeup that runs in an existing session
  is attributed to that session's cause.

In external mode, the model requests of a session name the session's cause
in the same way, so the same approximation applies to model usage. See
[Inference principal header](config.md#inference-principal-header).

| Work | Actor | Cause |
|---|---|---|
| A person sends a message, or changes a task through the web UI or the REST API | The person | The person |
| The agent works in a session (tool calls, task changes, sessions it starts) | The system actor | The session's cause |
| Nerve sets a session title (the placeholder and the generated title) | The system actor | The session's cause |
| An automatic archive, the archive of a Codex session by the sync | The system actor | The system actor |
| Telegram, Slack, or imported Codex input, and channel commands (`/new`, `/stop`, `/star`) | None until a local identity mapping exists | None |
| Assistant and tool output | None | None |

Existing records remain unattributed: Nerve does not backfill these columns.
Nerve does not infer identity from channel or source metadata. The free-text
origin fields, such as `task_events.actor`, stay as they are; they record where
a change came from, not who made it. Account administration actions are not
yet recorded as audit events. Missing attribution means legacy history,
unidentified external input, or assistant/tool output; autonomous work uses the
system actor.

### Session changes

`session_events` records the changes to a session with their actor and cause:

| Event | Change | Details |
|---|---|---|
| `created` | The session is created; also a resume (`reason: "resume"`) | `source`, `parent`, or `reason` |
| `title_changed` | The title changes | `title`, `previous`; `reason` is `placeholder` or `generated` for a title that Nerve sets for a turn, `cron_rotation` for a rotated cron session |
| `starred`, `unstarred` | The session is starred or unstarred | |
| `model_changed` | The model changes | `model`, `previous` |
| `parent_changed` | The session moves under another session, or back to the top level | `parent_session_id`, `previous` |
| `archived`, `unarchived` | The session is archived or restored | `cascade_root` for a session that an archive of an ancestor archives; `reason` for an automatic archive (`idle_interactive`, `stale`, `overflow`, `codex_thread_archived`) |
| `stop_requested` | A stop reaches a running session | |
| `external_tool_call` | An external MCP client calls a tool | The tool call |

A change through the web UI, the REST API or the browser WebSocket records the
person as actor and cause. A change that has no effect records no event: a
field that keeps its value, an archive of an archived session (also in a
cascade), a restore of a session that is not archived. Of two people who make
the same change at the same time, only the one whose change has an effect is
recorded.
Events that record the state of the runtime (`started`, `idle`, `error`, and a
`stopped` status after a cancel) have no actor and no cause.

A delete removes the session's events. Nerve writes a row to
`session_deletions` (the session ID, its last title, the time, the actor and
the cause) in the same transaction as the delete. A later delete of a session
with the same ID replaces that row.

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
