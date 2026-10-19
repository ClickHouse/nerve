# API Reference

## REST API

Unless noted otherwise, endpoints require a JWT in the `Authorization: Bearer
<token>` header or `nerve_token` cookie.

### Auth

#### `POST /api/auth/login`
Log in and receive a session token. Authentication is not required.

```json
Request:  { "password": "...", "username": "alice" }
Response: { "token": "eyJ..." }
```

`username` is optional when one account exists and required when there are two
or more. Use `GET /api/auth/status` to determine which fields to show. On a
passwordless installation, any password is accepted for the sole account.

Treat the returned token as opaque.

| Outcome | Response |
|---|---|
| Invalid credentials or missing required username | `401` `Invalid username or password` |
| Valid credentials for a disabled account | `401` |
| Setup is not complete (`login` is `setup`) | `409` |
| Authentication is not ready | `503` |
| External mode (`mode` is `external`) | `404` |

#### Authenticated requests

Send the token as `Authorization: Bearer <jwt>`, as the `nerve_token` cookie,
or as `?token=` (for `<img src>` and downloads, which cannot set headers).

Every request resolves its token to an account actor or the system principal.
Unknown and disabled accounts fail with `401`; unavailable startup identity
state fails with `503`. See [Accounts and identity](accounts.md) for token
types and legacy-session compatibility. In external mode, the gateway names the
person in the `X-Nerve-Actor-Context` header and session tokens are refused; see
[External mode](accounts.md#external-mode).

When a session is more than halfway to expiry, the response includes a refreshed
token in `X-Nerve-Token`. Replace the current token with it. The header also
upgrades sessions created before per-account login and is exposed through CORS.

#### `GET /api/auth/status`
Return the authentication mode and the required login fields. Authentication is
not required.

```json
Response: {
  "mode": "local",
  "auth_required": true,
  "login": "password"
}
```

| Field | Meaning |
|---|---|
| `mode` | `local` or `external`; see [Authentication mode](config.md#authentication-mode) |
| `login` | `setup`, `none`, `password`, or `username_password` |
| `auth_required` | Compatibility field; equivalent to `login != "none"` |

`setup` means setup is not complete: login is refused and only
`POST /api/setup/claim` is permitted. `none` means the operator chose a
passwordless installation.

The response does not expose usernames or the account count. Before
authentication is ready, it returns the fail-closed `username_password` state.

In `external` mode, the gateway signs people in and local login is not
available. The response does not read local login state and always returns
`auth_required: true` and `login: "username_password"`. Use `mode` to decide
whether to show a login form.

#### `GET /api/auth/check`
Verify current authentication.

```json
Response: { "authenticated": true }
```

#### `GET /api/auth/me`
Return the actor this credential acts as, and its local account.

```json
Response: {
  "actor": { "id": "…", "kind": "human", "display_name": "Alice", "username": "alice" },
  "account": { "id": "…", "actor_id": "…", "username": "alice", "…": "…" }
}
```

`actor` has the shape of one `GET /api/actors` row. `account` has the shape of
`GET /api/accounts/me`, or is `null` when the credential has no account, such as
the system principal's. The web UI compares message and session authors with
`actor.id`.

### Accounts

Every signed-in account can manage accounts. System credentials cannot. Account
responses never include password hashes or their storage location.

An account is:

```json
{
  "id": "…",
  "actor_id": "…",
  "username": "alice",
  "display_name": "Alice",
  "enabled": true,
  "has_password": true,
  "created_at": "…"
}
```

`id` identifies the account. `actor_id` is the stable attribution ID and does
not change when the username changes.

| Endpoint | Does |
|---|---|
| `GET /api/accounts` | List accounts, oldest first; includes disabled accounts |
| `GET /api/accounts/me` | Return the signed-in account |
| `POST /api/accounts` | Create an account from `{username, password, display_name?}` |
| `PATCH /api/accounts/{id}` | Update `{username?, display_name?}` |
| `POST /api/accounts/{id}/disable` | Disable an account; idempotent |
| `POST /api/accounts/{id}/enable` | Enable an account; idempotent |
| `PUT /api/accounts/me/password` | Change the signed-in account's password using `{current_password?, new_password}` |

Failures:

| Response | When |
|---|---|
| `400` | Invalid username or password longer than 72 UTF-8 bytes |
| `403` | System credential, or current password not supplied or incorrect |
| `404` | Account not found |
| `409` | Username or account-state conflict; the response explains the conflict |

Disabling an account affects its next request and closes its open WebSockets.

### Setup

The browser setup wizard (see [Setup](setup.md#finishing-setup-in-the-browser)).
Two groups of endpoints:

- **The claim** — `POST /api/setup/claim`. Registered in local mode only. The
  one unauthenticated write in the product, guarded by the setup token.
- **The checklist** — everything else under `/api/setup/*` plus
  `POST /api/system/restart`. Registered in **both** authentication modes.
  Every route requires a signed-in *person*: a system or MCP token is refused
  with `403` (`Setup is driven by a person, not by a system or MCP token.`).
  In local mode every checklist route answers `409` while the instance is
  unclaimed; in external mode no local-account state is read at all.

Common behaviour of the checklist routes:

| Response | When |
|---|---|
| `401` | no or invalid credential |
| `403` | a system or MCP token |
| `409` | local mode, instance unclaimed (`Setup of this instance is not complete; claim it first.`); a step that writes configuration under lockdown or with no machine-local config directory (the message says why); a state conflict named in the message (mode already chosen, no bot token, Slack managed by the gateway, …) |
| `400` | a field was refused: `{"detail": "<sentence>", "field": "<json key>"}`, so a form can attach the message to its input |
| `429` | a server-side check (test, preflight, detection) while three are already running: `Another check is running; try again in a moment.` |
| `500` | a write failed; the message names what landed (`Landed so far: config.local.yaml`) |

Secrets never leave the server: a stored secret is reported as
`{"configured": true, "last4": "ab12"}` (`last4` only for string secrets of
twelve characters or more, never for an integer such as a Telegram API id).
No setup response sets a cookie, and no error message contains a secret value.
Nor does any response carry an absolute server path: files are named by the
display paths the Review table uses (`config.local.yaml`, `config.yaml`,
`workspace/config/settings.yaml`, `workspace/config/cron/system.yaml`,
`setup-state.json in the state directory`), and a failing write reports the
error text without the filename; the full paths go to the server log.

| Endpoint | no credential | system/MCP token | person, unclaimed (local) | person, claimed | external actor | lockdown |
|---|---|---|---|---|---|---|
| `POST /api/setup/claim` | `403` bad token / `200` | same | `200` | `409` | `404` | `200` |
| `GET /api/setup` | `401` | `403` | `409` | `200` | `200` | `200` |
| `PUT /api/setup/*`, skip/unskip, finish | `401` | `403` | `409` | `200` | `200` | `409` for steps that write configuration (they read as `blocked`), except `mode` (a worker's `TASK.md`), `profile` `{name}` (local), skip/unskip, finish |
| tests, preflights, detect, models, environment, host-login, pairing `GET` | `401` | `403` | `409` | `200` | `200` | `200` |
| pairing `POST` | `401` | `403` | `409` | `200`/`409` | `200`/`409` | `409` |
| `POST /api/system/restart` | `401` | `403` | `409` | `200` | `200` | `200` |

#### `POST /api/setup/claim`

Complete setup of the one account on an install whose `login` status is
`setup`. This is the only unauthenticated account write; every request must
include the persisted setup token. Not registered in external mode (`404`).

```json
Request: {
  "username": "alice",
  "password": "…",
  "setup_token": "…",
  "display_name": "Alice"
}
Response: { "token": "eyJ…" }
```

To keep the installation passwordless, send `"passwordless": true` and no
`password`. `username` is then optional:

```json
Request: { "passwordless": true, "setup_token": "…" }
```

Send exactly one of `password` and `"passwordless": true`. `display_name` is
optional. The response token is the client session minted after the claim;
the actor and account are read through `GET /api/auth/me`, and names through
the actor directory.

The setup token is read locally with `nerve status`, sent only in the JSON
body, and invalidated after success. It never appears in a URL, response,
server log, or browser storage. Use HTTPS or a protected tunnel when claiming
remotely.

The claim and the setup completion record are one database transaction. Of
concurrent claimants exactly one can win.

| Response | When |
|---|---|
| `400` | both or neither of `password` and `"passwordless": true`; a password claim without a username; the username is malformed/reserved; or the password exceeds bcrypt's 72-byte limit |
| `403` | the setup token is wrong or no stored token can match it; a concurrent loser may see this after the winner retires the token |
| `409` | setup is already complete, including a request that passed token validation before a concurrent winner, or a configured password |
| `422` | the setup token is missing/empty, the password is empty, or a field has the wrong type |
| `503` | no signing secret, or identity startup is incomplete |

#### `GET /api/setup`

The checklist, in one read. Pure: it reads configuration, workspace files,
the database and the state file, and never shells out or calls the network.

```json
Response: {
  "generation": "6f1a0c2b9e4d7a31",
  "auth_mode": "local",
  "mode": "personal",
  "lockdown": false,
  "writable": true,
  "read_only_reason": null,
  "restart_pending": true,
  "restart_reasons": ["timezone: 'America/New_York' → 'Europe/Berlin'", "openai_api_key: embeddings start after a restart"],
  "restart_mode": "restart",
  "restart_message": "Restarting. This page reconnects on its own.",
  "restart_command": null,
  "finished": false,
  "steps_left": 2,
  "warning": null,
  "steps": [
    { "id": "claim", "title": "Claim", "status": "done", "required": true, "can_skip": false,
      "blocked_reason": null, "blocked_kind": null, "read_only": false, "read_only_reason": null, "etag": "0a1b2c3d4e5f",
      "summary": "alice · password set", "files": [],
      "detail": { "username": "alice", "passwordless": false } },
    { "id": "mode", "title": "Mode", "status": "done", "required": true, "can_skip": false,
      "blocked_reason": null, "blocked_kind": null, "read_only": true,
      "read_only_reason": "Mode was chosen when the workspace was created. …",
      "etag": "…", "summary": "Personal assistant",
      "files": ["workspace/SOUL.md", "workspace/IDENTITY.md", "…", "workspace/config/cron/system.yaml"],
      "detail": { "mode": "personal", "templates_present": ["SOUL.md", "IDENTITY.md", "USER.md", "AGENTS.md", "TOOLS.md", "MEMORY.md"],
                  "task_present": false, "onboarding": "not_applicable", "onboarding_session_id": null } },
    { "id": "provider", "title": "Model", "status": "done", "required": true, "can_skip": false,
      "blocked_reason": null, "read_only": false, "read_only_reason": null, "etag": "…",
      "summary": "Anthropic API key (…ab12) · claude-opus-5-5 · effort xhigh",
      "files": ["config.local.yaml", "workspace/config/settings.yaml", "config.yaml"],
      "detail": { "type": "anthropic", "credential": "api_key", "credential_source": "file",
                  "anthropic_api_key": { "configured": true, "last4": "ab12", "source": "file" },
                  "claude_oauth_token": { "configured": false, "last4": null, "source": null },
                  "proxy_enabled": false, "aws_region": "", "aws_profile": "",
                  "model": "claude-opus-5-5", "effort": "xhigh", "cron_model": "claude-sonnet-5-5",
                  "warning": null, "last_test": { "ok": true, "code": "ok", "detail": "claude-haiku-5-5 answered", "checked_at": "2026-10-10T09:12:03Z" } } },
    { "id": "profile", "title": "About you", "status": "pending", "required": false, "can_skip": true,
      "blocked_reason": null, "read_only": false, "read_only_reason": null, "etag": "…",
      "summary": "Alice · Europe/Berlin", "files": ["workspace/config/settings.yaml", "workspace/USER.md", "workspace/IDENTITY.md"],
      "detail": { "name": "Alice", "name_source": "account", "display_name_editable": true, "writable_fields": null,
                  "timezone": "Europe/Berlin", "timezone_is_default": false,
                  "quiet_enabled": true, "quiet_start": "02:00", "quiet_end": "08:00", "quiet_is_default": true,
                  "language": null, "agent_name": null, "agent_emoji": null, "about_set": false,
                  "user_md_pristine": { "name": true, "timezone": false, "language": true, "quiet_hours": true, "work": true },
                  "identity_md_pristine": { "name": true, "emoji": true } } },
    { "id": "memory", "…": "…" },
    { "id": "channels", "…": "…" },
    { "id": "sources", "…": "…" },
    { "id": "automation", "…": "…" },
    { "id": "agents", "…": "…" }
  ]
}
```

| Field | Meaning |
|---|---|
| `generation` | this process's boot id; the same value `GET /health` reports, used to notice a restart |
| `auth_mode` | `local` or `external`. In external mode the `claim` step is absent |
| `mode` | `personal`, `worker`, or `null` before the Mode step. Derived from the workspace files, never stored |
| `lockdown`, `writable`, `read_only_reason` | whether configuration can be written here, and why not |
| `restart_pending`, `restart_reasons` | restart-only keys whose on-disk value differs from the one the running process started with, plus anything else waiting on a restart (embeddings after an OpenAI key change, a cron reload that failed). Computed by comparison, so a reverted value disappears and the list is empty after a restart |
| `restart_mode`, `restart_message`, `restart_command` | what `POST /api/system/restart` *would* answer now (`restart`, `reload` or `unsupported`), its sentence, and the operator's command as data (`systemctl restart <unit>`, `systemctl --user restart <unit>` under a systemd user manager, `docker compose up -d`, or the value of `NERVE_RESTART_COMMAND`; `null` when there is none) — so the page shows the command before anyone clicks and never has to parse it out of the sentence |
| `finished` | every required step is `done` or `blocked`, every other step is `done`, `skipped` or `blocked`, and nothing waits on a restart |
| `steps_left` | the number of `pending` steps plus the steps blocked only because the Mode step has not been answered yet (`blocked_kind: "sequencing"`) — they are still work left to do, so the count only ever goes down as a person progresses. Steps blocked by lockdown (`blocked_kind: "lockdown"`) are never counted |
| `warning` | set when configuration was written but the state file could not be saved or a reload reported errors (`Saved. Reload reported: cron: error: …`) |

Each step carries `id`, a short `title`, a `status` (`done`, `pending`,
`skipped`, `blocked`), `required`, `can_skip`, `blocked_reason`, `blocked_kind`
(`sequencing` while the step waits for the Mode step, `lockdown` when
configuration is managed from a repository; `null` unless `blocked`),
`read_only` with `read_only_reason`, an `etag` (a hash of the persisted part
of `detail`; forms remount when it changes, and never for a test result or a
pairing count), a one-line `summary`, the display paths it writes (`files`)
and a `detail` object with the current values, secrets masked.

A step is `done` because a fact says so — `mode` when `SOUL.md` exists;
`provider` when a credential is configured (including one from the gateway's
environment, reported as `"credential": "environment"`); `profile` when the
time zone differs from the default or `USER.md` carries a name; `channels`
when a Telegram or Slack token is stored; `sources`/`agents`/`automation`
when something is enabled or the step was answered — so an instance set up at
the terminal or from the environment shows the same list. `done` beats
`skipped`; `blocked` beats `skipped`. Steps are `blocked` before the Mode step
when they depend on the mode (`blocked_kind: "sequencing"` — counted in
`steps_left`, since choosing a mode makes them actionable), and under lockdown
when they would write configuration (`blocked_kind: "lockdown"` — never
counted; on a locked instance whose templates have not arrived yet, the steps
waiting for Mode read `lockdown` too, because Mode itself cannot be answered
there). Steps: `claim` (local only), `mode`, `provider`, `profile` (titled
`Operator` for a worker), `memory`, `channels`, `sources` (personal),
`automation`, `agents` (personal).

`detail.provider.credential` is `bedrock`, `proxy`, `api_key`, `oauth_token`,
`environment` or `none`; `detail.provider.warning` is `oauth_background_jobs`
when the only credential is a subscription token (memory, titles and cron
need an API key). `detail.channels.telegram.start_error` carries the scrubbed
reason when the bot failed to start at this boot. In external mode
`detail.profile.name_source` is `actor_header` and `display_name_editable` is
`false`.

`detail.profile.quiet_enabled` is `true` when both quiet times are set; both
empty means no quiet hours. `quiet_is_default` compares with the mode's
default: `02:00`–`08:00` for a personal install, none for a worker. Each job in
`detail.automation.optional` carries `id`, `name`, `description`, `schedule`
and `schedule_text`, `requires` and `requirement_met`, `enabled`, and
`default_enabled`: whether the mode's default set switches it on (`false` for
a worker's `task-planner`, which the onboarding session decides on; `default`
is the older name for the same value).

**Where a value came from.** Every masked secret is
`{"configured": bool, "last4": str | null, "source": str | null}`. `source`
says where a configured value came from — `file` (`config.local.yaml` or
`config.yaml` on this machine; the page may replace or remove it),
`environment` (a variable Nerve reads directly — `ANTHROPIC_API_KEY`,
`CLAUDE_CODE_OAUTH_TOKEN`, `GH_TOKEN` — or a `${VAR}` reference in any
configuration layer, judged on the raw files before interpolation),
`config-repo` (the tracked `settings.yaml` under lockdown) or `gateway`
(held by the shared channel gateway, `slack.mode: hosted`); `null` when
nothing is configured. A value with a `source` other than `file` is
configured and managed: the page renders it read-only, and a `PUT` that would
replace or remove it answers `409 {"detail", "field"}`. The same enum
appears as `detail.provider.credential_source` (the active credential's
origin; `null` for Bedrock, the proxy or none), `detail.sources.github.source`
(a GitHub token Nerve forwards to the CLI; `null` when only the CLI's own
login exists) and `detail.sources.gmail.source` (the address list). A
credential that exists only in the gateway's environment reads
`{"configured": true, "last4": null, "source": "environment"}` with
`credential: "environment"`.

`detail.channels.slack.gateway` is `{"streams": n, "connected": bool,
"last_event_at": iso | null}` when `slack.mode` is `hosted` — what the stream
manager reports about the gateway's streams (`connected` once a stream has
negotiated; `last_event_at` the last frame received, heartbeats included) —
and `null` in socket mode. A hosted Slack makes the channels step `done`; its
tokens read `source: "gateway"` and `enabled` is the only accepted field.
`detail.sources.telegram.session` is `{"present": bool, "authorized": true |
false | null, "user": {"id", "username", "first_name"} | null}` for the
Telegram message sync: `present` is the session file, `authorized` what the
last sign-in or session check established (`null` when a file exists but
nothing has verified it in this install — the session endpoint below answers
live), and the summary reads `Telegram sync · signed in as @name` or
`Telegram sync · not signed in`. Both `gateway` and `session` are volatile
and outside the step's `etag`.

#### `PUT /api/setup/{step}`

One per step: `mode`, `provider`, `profile`, `memory`, `channels`, `sources`,
`automation`, `agents`. **PATCH semantics**: an omitted field is left exactly
as it is; `null` removes a secret or optional value; a blank secret is omitted
(never written); the same value twice changes no file and creates no restart
debt. Each returns the whole `GET /api/setup` body, so one round trip both
writes and refreshes. Writes go through the same writer as `nerve init`,
merged into what is on disk, never regenerating a file; `config.local.yaml`
stays owner-only. After every write the configuration is hot-reloaded. One
mutation runs at a time.

```json
PUT /api/setup/mode        { "mode": "worker", "task_description": "Watch the nightly build for example/repo and propose fixes before changing anything." }
PUT /api/setup/provider    { "type": "anthropic", "anthropic_api_key": "sk-ant-REPLACE", "model": "claude-opus-5-5", "effort": "high" }
PUT /api/setup/provider    { "type": "oauth", "claude_oauth_token": "sk-ant-oat01-REPLACE" }
PUT /api/setup/provider    { "type": "host_login" }
PUT /api/setup/provider    { "type": "bedrock", "aws_region": "eu-central-1", "aws_profile": "nerve" }
PUT /api/setup/provider    { "model": "claude-sonnet-4-6", "effort": "medium" }
PUT /api/setup/profile     { "name": "Alice", "timezone": "Europe/Berlin", "quiet_enabled": true, "quiet_start": "22:00", "quiet_end": "07:00",
                             "language": "English", "agent_name": "Nerve", "agent_emoji": "⚡", "about": "I run a two-person design studio." }
PUT /api/setup/profile     { "quiet_enabled": false }
PUT /api/setup/memory      { "openai_api_key": "sk-REPLACE" }
PUT /api/setup/memory      { "openai_api_key": null, "embeddings": false }
PUT /api/setup/channels    { "telegram": { "bot_token": "123456:REPLACE_REPLACE_REPLACE_REPLACE_REPLACE", "enabled": true } }
PUT /api/setup/channels    { "telegram": { "allowed_users_add": [123456789] } }
PUT /api/setup/channels    { "slack": { "bot_token": "xoxb-REPLACE", "app_token": "xapp-REPLACE", "enabled": true,
                                        "allow_users": ["U0123456789"], "allow_direct_messages": true, "allow_channels": [] } }
PUT /api/setup/sources     { "github": true, "gmail": true, "gmail_accounts": ["alice@example.com"],
                             "telegram": true, "telegram_api_id": 12345, "telegram_api_hash": "REPLACE" }
PUT /api/setup/automation  { "crons": ["inbox-processor", "task-planner"] }
PUT /api/setup/agents      { "agents": ["codex"], "conflict_policy": "backup" }
```

| Step | Rules |
|---|---|
| `mode` | `mode` is `personal` or `worker`. Once chosen, another mode is `409` with the read-only reason; the same mode again is a no-op (`200`, nothing rewritten). A worker needs `task_description` (20–4000 characters → `400`, `field: task_description`). Once `TASK.md` exists, the same mode with a **different** description is `409` too — the task is chosen once, and changed by editing `TASK.md` (or by telling the worker); only a worker whose templates exist but whose `TASK.md` is still absent gets it written by a later PUT. Writes the templates, a worker's `TASK.md`, mode defaults in `settings.yaml` where absent, and `config/cron/system.yaml` with the mode's default jobs if missing |
| `provider` | `type` is `anthropic`, `oauth`, `host_login` or `bedrock`, optional when only `model`/`effort` change. `anthropic_api_key` must start with `sk-ant-`; `aws_region` must look like a region; `effort` is `low`, `medium`, `high`, `xhigh` or `max`; `model` must be in the catalogue for these credentials or be a Claude id. `host_login` is accepted only when `GET /api/setup/provider/host-login` finds one (`409` otherwise; `429` while other checks are running) and copies that credential into `config.local.yaml`; the value is never returned. In external authentication mode `host_login` is `409` unless the operator set `NERVE_SETUP_HOST_LOGIN=1` (see the host-login endpoint). Switching the credential type deletes the other secret; leaving Bedrock drops the region-scoped model ids. A credential whose `source` is not `file` (provisioned from the environment or the configuration repository) is neither replaced nor removed from here: `409` with `field: anthropic_api_key` / `claude_oauth_token` (`host_login` too); a `model`/`effort`-only save still works. The model catalogue and the host-login detection run before the write lock is taken, so other people's reads and saves never wait on them. No catalogue reset after a save: the cache is keyed by the credentials' fingerprint, so a new key finds the catalogue the provider test warmed for it |
| `profile` | `timezone` must be an IANA name; `quiet_start`/`quiet_end` are `HH:MM`; `name` ≤ 80, `agent_name` ≤ 40, `language` ≤ 40, `about` ≤ 600 characters. Every field becomes one line of a file the agent reads: `name`, `language`, `agent_name`, `agent_emoji` refuse control characters and line breaks (`400` with the `field`), and `about` is folded to a single line. A `null` time zone or quiet time is treated as omitted (nothing written, no decision recorded). `quiet_enabled` switches quiet hours: `false` writes both times empty (no quiet hours; the personal `USER.md` line reads `none`) and ignores any times sent with it; `true` needs both times (`400` naming the missing or invalid one), or neither, which keeps the window configured now or else uses `02:00`–`08:00`; omitted or `null` changes nothing, except that times sent alone switch quiet hours on with those times (the other time is kept, or set to its `02:00`–`08:00` default when there was none). Writes `timezone`/`quiet_*` to `settings.yaml`, the still-pristine template lines of `USER.md`/`IDENTITY.md` (personal), and — local mode only — the display name on your account. In external mode the name goes to `USER.md` alone; no account is touched. Under lockdown (local) only `name` is accepted; any other key is `409` with that `field` and the lockdown reason (a read-only state, not a validation error) |
| `memory` | `openai_api_key` must start with `sk-`. Adding, changing or removing it records that embeddings start after a restart. `{"openai_api_key": null}` alone removes the key; `"embeddings": false` records "continue without" as a decision. A key whose `source` is not `file` is `409` `field: openai_api_key` |
| `channels` | A Telegram `bot_token` must look like one from BotFather, and when the channel would be enabled it is **verified against Telegram before anything is written**: rejected → `400` `field: bot_token` `Telegram rejected this token: …`; unreachable → `400` `The server could not reach api.telegram.org to verify the token; nothing was saved. Try again.` The verification runs *before* the write lock is taken (a probe can take 10–20 s and must not stall other people's reads and saves); inside the lock the decision is recomputed against the configuration as it is then, and a token that was not the one verified — the stored token changed meanwhile — is verified again. `enabled` defaults to true when a token is given; `"bot_token": null` removes the token and disables the channel; `enabled: true` without a token is `409`. `allowed_users` replaces the allow-list, `allowed_users_add` appends; not both. Slack tokens need the `xoxb-`/`xapp-` prefixes, `enabled` needs both, and they are verified the same way (`auth_test` and a Socket Mode connection). With `slack.mode: hosted` only `enabled` is accepted (`409` for tokens). A token whose `source` is not `file` (environment, configuration repository) is `409` with `field: bot_token` / `app_token`, before any verification runs |
| `sources` | personal mode only (`409` for a worker). `gmail: true` needs at least one address in `gmail_accounts`; `telegram: true` needs `telegram_api_id` (positive) and `telegram_api_hash`. The two Telegram credentials follow the secret rules independently — omitted keeps, blank is omitted, `null` removes — and a hash without an id (new or stored) is `400` `field: telegram_api_id` rather than being dropped. A credential or address list whose `source` is not `file` is `409` with `field: telegram_api_id` / `telegram_api_hash` / `gmail_accounts`. Sending all three booleans records the decision, so "none of these" counts as answered. The credentials alone do not make the Telegram sync work — see the sign-in endpoints below |
| `automation` | `crons` is the full set of enabled optional job ids; an unknown id is `400` `field: crons`. Always records the decision; the scheduler reloads |
| `agents` | personal mode only. `agents` names from the registry (`codex`, `claude-code`); `conflict_policy` is `backup`, `skip` or `merge`. Writes each agent's config file (per-agent failures are reported in `warning`, the others still apply) and `external_agents.*`/`mcp_endpoint.enabled` in `config.yaml`. `[]` is a decision too |

#### `POST /api/setup/steps/{id}/skip` · `POST /api/setup/steps/{id}/unskip`

Remember that an optional step was declined, or put it back. `400` for a
required or unknown step, and for a step that does not exist on this instance
(a worker has no `sources` or `agents` step: `The sources step does not apply
to this instance.`). Skipping a step that is already `done` changes nothing:
`200` with the unchanged checklist, and nothing is recorded, so the state file
holds only meaningful decisions. Allowed under lockdown (it is runtime state,
not configuration). Returns the whole checklist. A skipped step that later
becomes true reads `done`; writing the step clears the skip.

#### `POST /api/setup/provider/test`

Test a candidate provider configuration without saving it. Same body as
`PUT /api/setup/provider`; `{}` tests the saved configuration. One
`max_tokens: 1` call on the server, 20 s limit.

```json
Response: { "ok": true,  "checked": "api", "code": "ok", "detail": "claude-haiku-5-5 answered", "hint": null, "latency_ms": 812 }
Response: { "ok": false, "checked": "api", "code": "invalid_key", "detail": "Anthropic rejected this key (401 authentication_error).",
            "hint": "Keys start with sk-ant-. Check for a missing last character or a revoked key.", "latency_ms": 640 }
Response: { "ok": false, "checked": "api", "code": "network", "detail": "The server could not reach api.anthropic.com (connection refused).",
            "hint": "This runs on the server, not in your browser. Check its internet access or proxy settings.", "latency_ms": null }
Response: { "ok": true,  "checked": "skipped", "code": "ok", "detail": "Subscription tokens are verified on first use; the Anthropic API cannot test them.",
            "hint": "Background jobs (memory, titles, cron) also need an API key; add one later in Memory.", "latency_ms": null }
```

`code` is `ok`, `invalid_key`, `model_access`, `network`, `timeout` or
`unknown`. The last test — success **or failure** — is remembered as
`detail.provider.last_test` = `{"ok", "code", "detail", "checked_at"}` for the
life of the process, so the Review and Ready screens can say that the last
connection test failed; `detail` carries no secret. It is forgotten when the
saved credential changes (a key written or removed, a type switch, a Bedrock
region or profile); a model/effort-only change keeps it. It is not part of the
step's `etag`, so remembering it never remounts a form.

#### `GET /api/setup/provider/host-login`

Whether the gateway host has a Claude Code login the Model step can copy.
Only the masked source label leaves the server.

```json
Response: { "available": true,  "kind": "oauth", "source": "~/.claude/.credentials.json", "reason": null, "lookup": "found" }
Response: { "available": false, "kind": null, "source": null, "reason": "No ~/.claude/.credentials.json; no Claude Code entry in the Keychain.", "lookup": "none" }
Response: { "available": false, "kind": null, "source": null, "reason": "No ~/.claude/.credentials.json; the Keychain is not readable from a background process (it would need to ask on the server's screen). On the server, nerve init --terminal runs the same detection interactively and can use the login; or paste an API key here.", "lookup": "unreadable" }
```

Evaluated at most once per process (plus once more with `?refresh=1`),
non-prompting sources first (environment variables, the credentials file) and
the macOS Keychain last, because a Keychain lookup can raise a prompt on the
server's desktop. Always `available: false` inside a container.

**External authentication mode.** The login found on the gateway host is the
personal credential of whoever runs the server, and in this mode every person
the gateway signs in may run setup. So the option is not offered there — the
endpoint answers `available: false` with `lookup: "none"` and a `reason` that
says so, **without looking the login up** (the answer alone would reveal that
it exists and where), and `PUT /api/setup/provider {"type": "host_login"}`
and the matching `POST …/provider/test` answer `409`. The operator can allow
it explicitly with `NERVE_SETUP_HOST_LOGIN=1` in the service environment;
the `reason` names the variable.

#### `GET /api/setup/models`

```json
Response: {
  "models": [{ "id": "claude-opus-5-5", "label": "Claude Opus 5.5", "family": "opus", "recommended": true },
             { "id": "claude-haiku-5-5", "label": "Claude Haiku 5.5", "family": "haiku", "recommended": false },
             { "id": "claude-sonnet-5-5", "label": "Claude Sonnet 5.5", "family": "sonnet", "recommended": false },
             { "id": "claude-fable-5-1", "label": "Claude Fable 5.1", "family": "fable", "recommended": false },
             { "id": "claude-opus-5", "label": "Claude Opus 5", "family": "opus", "recommended": false }],
  "current": { "model": "claude-opus-5-5", "effort": "xhigh" },
  "efforts": ["max", "xhigh", "high", "medium", "low"],
  "discovered": true,
  "default_model": "claude-opus-5-5"
}
```

The configured model comes first, then the catalogue as the Anthropic Models
API orders it (newest first), nothing else; without discovery the built-in
list applies. `family` is `fable`, `opus`, `sonnet`, `haiku` or `other`
(`fable` is checked first); `recommended` marks the configured model;
`default_model` is Nerve's own default (`nerve.config.DEFAULT_MODEL`), so the
page can say which entry it is even when this instance was changed to another
model. On Bedrock the ids carry the geo prefix and `discovered` is `false`.

The catalogue is cached per credential fingerprint (provider, base URL, key
tail). A successful `POST /api/setup/provider/test` with an API key fetches
the catalogue for that candidate key in the background, keyed by its
fingerprint, so the picker is complete the moment the key is saved; a failure
recorded while no key was configured is never served for a configuration
that now carries one.

#### Telegram message sync: `GET /api/setup/sources/telegram/session` and the sign-in

The Telethon source reads the person's own Telegram account and needs an
authorised user session on disk (`NERVE_HOME/telegram_sync.session`); the
API id and hash alone do nothing. These routes run the same sign-in the
terminal command `nerve setup-telegram` runs interactively. All of them need
a signed-in person and a claimed instance (local mode); the `POST`s also need
the sources step to be actionable — `409` for a worker instance, under
lockdown, or before the Mode step. One sign-in is pending per process, in
memory only, for ten minutes; a new start cancels the previous one.

```json
GET  /api/setup/sources/telegram/session
Response: { "configured": true, "session_present": true, "authorized": true,
            "user": { "id": 424242, "username": "alice_example", "first_name": "Alice" },
            "pending": null, "phone_masked": null, "reason": null }
Response: { "configured": true, "session_present": false, "authorized": false, "user": null,
            "pending": null, "phone_masked": null, "reason": "no_session" }
Response: { "configured": true, "session_present": true, "authorized": null, "user": null,
            "pending": null, "phone_masked": null, "reason": "busy" }
Response: { "configured": true, "session_present": true, "authorized": false, "user": null,
            "pending": "code", "phone_masked": "+1••••••••23", "reason": null }

POST /api/setup/sources/telegram/session/start     { "phone": "+1 555 010 0123" }
Response: { "pending": "code", "phone_masked": "+1••••••••23", "expires_in": 600 }
POST /api/setup/sources/telegram/session/code      { "code": "73146" }
POST /api/setup/sources/telegram/session/password  { "password": "…" }
POST /api/setup/sources/telegram/session/cancel    {}
POST /api/setup/sources/telegram/session/logout    {}
```

`configured` is whether the API id and hash are saved. The `GET` opens a
short-lived client on the session file (five seconds at most), never while
a sign-in is pending; `reason` is a short machine string — `busy` (the file
is locked by a running sync; `authorized` is then `null` rather than an
error), `not_configured`, `no_session`, `unreachable`, `telethon_missing` —
or `null`. `code`, `password`, `cancel` and `logout` answer the `GET` shape:
when Telegram asks for the two-step password, `code` answers with
`pending: "password"` and `authorized: false`; on success `authorized: true`
with `user`. `logout` calls `log_out()` on the saved session and removes the
file. Errors: `400 {"detail", "field"}` with `field` `phone`, `code` or
`password`; `429 {"detail", "retry_after"}` (seconds) for a Telegram flood
wait; `409 {"detail"}` when nothing is configured, no sign-in is in progress,
the code expired, or the session file is in use; `502 {"detail"}` when
Telegram cannot be reached. The phone appears only masked; the code, the
password and Telegram's hash never appear in a response, a log line or the
state file. A successful sign-in makes the session file owner-only and notes
who is signed in (`id`, `username`, `first_name`) in the state file, which
is what `detail.sources.telegram.session` and `nerve doctor` report.

#### `POST /api/setup/memory/test` · `POST /api/setup/channels/telegram/test` · `POST /api/setup/channels/slack/test`

Server-side checks of a candidate secret (or, with an empty body, the saved
one). Nothing is written. A successful Telegram test remembers the bot's
username in memory for that token only; it reaches the state file — and
`detail.channels.telegram.bot_username`, the pairing `link` — when
`PUT /api/setup/channels` saves that same token, so the page never names a
bot the instance is not configured with. Removing the token clears the name.

```json
POST /api/setup/memory/test             { "openai_api_key": "sk-REPLACE" }
Response: { "ok": false, "detail": "OpenAI rejected the key (401).", "hint": "Check the key in the OpenAI dashboard." }

POST /api/setup/channels/telegram/test  { "bot_token": "123456:REPLACE…" }
Response: { "ok": true,  "bot_username": "example_assistant_bot", "detail": null, "hint": null }
Response: { "ok": false, "bot_username": null, "detail": "Telegram rejected this token: Unauthorized", "hint": "In Telegram open @BotFather → /mybots → API Token and paste it again; a token that was revoked or re-issued answers Unauthorized." }

POST /api/setup/channels/slack/test     { "bot_token": "xoxb-…", "app_token": "xapp-…" }
Response: { "ok": true, "team": "Example Team", "bot_user": "nerve", "detail": null }
```

#### `GET /api/setup/channels/telegram/pairing` · `POST /api/setup/channels/telegram/pairing`

`GET` reports whether in-app pairing is possible right now. Pure; never
carries the code. `allowed_users_count` is re-read from `config.local.yaml`
on every call, because the bot's `/pair` handler appends there — polling it
is how the page notices that your message arrived.

```json
Response: { "available": false, "reason": "The bot starts with this token after the restart at the end of setup. Pairing is offered there.",
            "live": false, "bot_username": "example_assistant_bot", "link": "https://t.me/example_assistant_bot",
            "allowed_users_count": 0, "code_active": false, "expires_at": null }
Response: { "available": true, "reason": null, "live": true, "bot_username": "example_assistant_bot",
            "link": "https://t.me/example_assistant_bot", "allowed_users_count": 1, "code_active": true, "expires_at": "2026-10-10T10:12:03Z" }
```

`POST` issues the pairing code (one hour, five attempts, single use — the
same code `nerve pair` prints) and is the only response that carries it:

```json
Response: { "code": "483920", "expires_at": "2026-10-10T10:12:03Z", "bot_username": "example_assistant_bot",
            "link": "https://t.me/example_assistant_bot", "allowed_users_count": 0 }
```

`409` when there is no bot token, when `telegram.dm_policy` is not
`pairing`, when the bot is not running with the saved token yet (`Restart,
then pair.`), and under lockdown. `link` is `null` when no username is known
from any source.

#### `GET /api/setup/sources/preflight` · `GET /api/setup/agents/detect` · `GET /api/setup/environment`

Server-side detection for the Sources, Agents and Review steps. Each
subprocess runs with a timeout; the results are cached for 60 s.

```json
GET /api/setup/sources/preflight
Response: { "github": { "cli": true, "authenticated": true, "login": "alice", "hint": null },
            "gmail": { "cli": false, "hint": "Install gog, then run gog gmail setup <account> on the server." },
            "telegram": { "credentials_configured": false },
            "checked_at": "2026-10-10T09:12:03Z" }

GET /api/setup/agents/detect
Response: { "agents": [{ "id": "codex", "label": "Codex", "installed": true, "version": "codex-cli 0.40.0",
                         "config_paths": ["~/.codex/config.toml"], "config_exists": true, "enabled": false }] }

GET /api/setup/environment
Response: { "checks": [{ "id": "claude_cli", "label": "Claude Code CLI", "status": "ok", "detail": "2.1.0", "hint": null },
                       { "id": "node", "label": "Node.js", "status": "warn", "detail": "v20.19.0", "hint": "Nerve builds its web UI with Node 22.12 or newer." },
                       { "id": "gh", "…": "…" }, { "id": "gog", "…": "…" }, { "id": "disk", "…": "…" },
                       { "id": "web_ui", "…": "…" }, { "id": "python", "…": "…" }, { "id": "timezone", "…": "…" }],
            "served": { "scheme": "http", "host_is_loopback": true, "client_is_loopback": true, "https_hint": false },
            "bind_host": "127.0.0.1", "port": 8900, "in_container": false, "supervision": "none",
            "passwordless": false, "restart_mode": "restart", "restart_message": "Restarting. This page reconnects on its own.",
            "command": null,
            "checked_at": "2026-10-10T09:12:03Z" }
```

A check's `status` is `ok`, `warn` or `missing`. `served.https_hint` is true
when the page is served over plain HTTP and either end is not loopback; it is
advisory only and never an authorisation input. `supervision` is `none`,
`systemd`, `docker` or `supervisor` (an operator-declared supervisor via
`NERVE_SUPERVISOR=exit|refuse` on a host that is neither a container nor
systemd-managed); `passwordless` is `null` in external mode. `restart_mode`,
`restart_message` and `command` are the same three values `GET /api/setup`
reports as `restart_mode`, `restart_message` and `restart_command`.

#### `POST /api/setup/finish`

Body `{}`. Reloads the configuration, recomputes the checklist and, for a
worker whose `TASK.md` still carries the onboarding marker (the comment on its
second line, deleted by the onboarding session when the file is complete), a
provider is configured and nothing waits on a restart, starts the onboarding
session. Idempotent; allowed under lockdown (it writes nothing).

```json
Response: { "state": { "…": "the GET /api/setup body" }, "action": "reloaded",         "onboarding": { "status": "running", "session_id": "cron:worker-onboarding:…" } }
Response: { "state": { "…": "the GET /api/setup body" }, "action": "restart_required", "onboarding": { "status": "pending", "session_id": null } }
```

`onboarding.status` (and `mode.detail.onboarding` in `GET /api/setup`) is
`not_applicable`, `pending`, `running`, `ended` or `done`. `ended` means the
onboarding session exists but the engine is no longer driving it while
`TASK.md` still carries the onboarding marker — it stopped before completing
the file (a placeholder key, a crash); the session id is carried so the page
can link to it instead of waiting. Calling `finish` again schedules the session anew; the
ended row stays in the table, and a session the engine is driving always wins
over an ended one (among several ended ones the newest is named), so every
tab reads `running` with the new id.

#### `POST /api/system/restart`

Restart the daemon — what `nerve restart` does, asked for over HTTP. Body
`{}`. The handler first waits for any save in flight to finish, so nothing
is mid-write when the process is signalled.

```json
Response: { "mode": "reload",      "generation": "6f1a0c2b9e4d7a31", "message": "Nothing needed a restart; the configuration was reloaded.", "command": null, "timeout_hint_seconds": 0 }
Response: { "mode": "restart",     "generation": "6f1a0c2b9e4d7a31", "message": "Restarting. This page reconnects on its own.", "command": null, "timeout_hint_seconds": 120 }
Response: { "mode": "restart",     "generation": "6f1a0c2b9e4d7a31", "message": "The container exits and Docker starts it again (restart: unless-stopped). If you stopped it with docker compose stop, run docker compose up -d.", "command": "docker compose up -d", "timeout_hint_seconds": 180 }
Response: { "mode": "unsupported", "generation": "6f1a0c2b9e4d7a31", "message": "This instance is managed by systemd. On the server run: systemctl restart nerve.service — this page reconnects by itself.", "command": "systemctl restart nerve.service", "timeout_hint_seconds": 300 }
```

| `mode` | When |
|---|---|
| `reload` | nothing needs a restart; the configuration was reloaded instead and no process was touched |
| `restart` | on a plain `nerve start` install a detached helper stops this process and starts a new one; inside the generated container the process exits and Compose restarts it. `NERVE_SUPERVISOR=exit` forces the latter |
| `unsupported` | under systemd (or `NERVE_SUPERVISOR=refuse`): nothing was started, and `command` is what the operator must run — under systemd `systemctl restart <unit>` with the unit read from the *leaf* of the process's cgroup (anything but a `.service` leaf, such as a transient scope, falls back to `nerve.service`), or `systemctl --user restart <unit>` when a systemd user manager (`user@<uid>.service`) started the process; otherwise the value of `NERVE_RESTART_COMMAND` from the service environment, or `null` when it is not set (the `message` then says to set it) |

`generation` is the boot id of the process that answered. **Poll `GET
/health` until it reports a different one** — the process being replaced
answers perfectly well while it shuts down, so waiting for any answer at all
accepts the old process. `timeout_hint_seconds` is how long a client should
wait before giving up.

| Response | When |
|---|---|
| `409` | a restart is already in progress (once per process) |
| `500` | the helper could not be started; the instance is still running and the message says to run `nerve restart` on the server |

Sessions survive the restart: the signing secret is pinned and persisted, and
nothing in the wizard rotates it. Allowed under lockdown, because a restart
writes no configuration.

### Actors

An actor is *who* something is attributed to: a person, or the agent's system
principal. Sessions, messages, task events, notifications, silences, plans,
workflow runs, review loops, review-loop decisions, wakeups and cron run logs
store an actor id, never a name, so this is where a name is looked up at
render time. A rename changes
every label and moves no stored row.

```json
{ "id": "…", "kind": "human", "display_name": "Alice", "username": "alice" }
```

| Endpoint | Does |
|---|---|
| `GET /api/actors` | `{ "actors": [...] }`, oldest first. One row per person plus the system principal, so a single call labels a whole list |

`kind` is `human` or `system`.

`username` is the login name of the account behind a human actor, or `null` for
the system principal and for an account that has not been given one. It is here
so a label can fall back to it when `display_name` is empty, and it is the same
name every signed-in account already sees on `/accounts`.

Otherwise this is an identity, not an account: nothing else from `accounts`
appears here (no `enabled`, no `has_password`, no credential state), and an
actor need not have an account at all — the system principal does not. A
disabled person's actor is still readable, because their history stays in the
UI after their access ends.

### Sessions

#### `GET /api/sessions?offset=0`
Sidebar feed: one page of conversations plus every starred session.

`sessions` is a single page of the conversation feed (page size = `sessions.sidebar_page_size`, default 50; `0` = unlimited). The window covers only non-archived, non-system (cron/hook), non-starred rows, so cron traffic can never displace conversations. On the first page (`offset=0`) all starred sessions are prepended in full and are never truncated; pass the returned `next_offset` back as `?offset=N` to load subsequent pages. `archived_count`/`system_count` are the collapsed-group badge counts, and `has_more`/`next_offset` drive the "…" load-more control.

```json
Response: {
  "sessions": [{ "id": "main", "title": "Main", "source": "system", "updated_at": "..." }],
  "archived_count": 12,
  "system_count": 3,
  "has_more": true,
  "next_offset": 50
}
```

#### `GET /api/sessions/archived?offset=0`
One page of archived **conversations** — system/cron sessions are excluded. Fetched only when the sidebar's Archived group is expanded.

```json
Response: { "sessions": [{ "id": "a1b2c3d4", "title": "Old chat", "status": "archived", "updated_at": "..." }], "has_more": false, "next_offset": 7 }
```

#### `GET /api/sessions/system?offset=0`
One page of live (non-archived) system/cron/hook sessions. Fetched only when the sidebar's System group is expanded.

```json
Response: { "sessions": [{ "id": "cron-1", "title": "task-heartbeat", "source": "system", "updated_at": "..." }], "has_more": false, "next_offset": 3 }
```

#### `POST /api/sessions/{id}/unarchive`
Restore an archived session to idle so it resurfaces at the top of the conversation feed. Returns 404 if the session doesn't exist.

```json
Response: { "unarchived": true }
```

#### `POST /api/sessions`
Create a new session.

```json
Request:  { "title": "My Session" }
Response: { "id": "a1b2c3d4", "title": "My Session", "source": "web", "created_by_actor_id": "…", "caused_by_actor_id": "…" }
```

`created_by_actor_id` is an actor id for [`GET /api/actors`](#actors), not an
account id. It is `null` for legacy rows and sessions caused by an unidentified
external person. Every session payload carries the field.

Session payloads also carry `caused_by_actor_id`: the session's cause, on
whose behalf all agent work in the session runs. Nerve sets it when it creates
the session and never changes it (an actor id, or `null` when unknown). See
[Attribution](accounts.md#attribution).

Session list rows carry `pending_wakeup_at`, the fire time of the session's
pending wakeup or deferred message (`null` when there is none), with
`pending_wakeup_created_by_actor_id` (who scheduled it: a person, or the
system actor when the agent did) and `pending_wakeup_caused_by_actor_id` (on
whose behalf it was scheduled; the turn that it fires runs for the session's
cause). The `session_running` WebSocket event carries the same three fields.

Rows also carry `is_running` (the orchestrator's turn is in flight),
`has_background_tasks` (sub-agents or background tasks are still live) and
`background_count` (how many). The two are independent: agents can run
between turns. See [Sub-agents and background tasks](agents.md).

#### `GET /api/sessions/{id}`
Get session details.

#### `GET /api/sessions/{id}/messages?limit=100`
Get messages for a session.

```json
Response: { "messages": [{ "id": 1, "role": "user", "content": "...", "channel": "web", "created_at": "...", "actor_id": "…" }] }
```

`actor_id` is who supplied the message: the signed-in person who typed it, or
the system principal for a prompt Nerve composed itself. It is `null` on
Slack, Telegram, and imported Codex human input until identity mappings exist;
on assistant/tool output; and on legacy history. `channel` stays transport
provenance and is never identity. See
[Accounts and identity](accounts.md) for what is and is not attributed.

#### `DELETE /api/sessions/{id}`
Delete a session (cannot delete "main"). Disconnects any active SDK client before deletion.
The delete removes the session's events and its agent records and
transcripts. Nerve records the delete in
`session_deletions` with the signed-in person as actor and cause (see
[Session changes](accounts.md#session-changes)).

#### `PATCH /api/sessions/{id}`
Update `title`, `starred`, `model` or `parent_session_id`. A `model` change
recreates the session's runtime, which would end its live background agents:
while any run it answers `409` with
`{"detail": {"code": "live_background_agents", "message": "…", "live": 2}}`.

#### `GET /api/sessions/{id}/status`
Session status with lifecycle info.

```json
Response: {
  "session_id": "a1b2c3d4",
  "status": "active",
  "is_running": true,
  "background_count": 2,
  "sdk_session_id": "8fbba4a4-...",
  "connected_at": "2026-02-27T12:00:00+00:00",
  "parent_session_id": null,
  "message_count": 42,
  "total_cost_usd": 0.0
}
```

#### `POST /api/sessions/fork`
Fork a session, optionally from a specific message point (`at_message_id` is
a numeric message row id as a string). The fork branches the source's
*native* conversation on its first turn (Claude `--fork-session` +
`--resume-session-at`; Codex `thread/fork` + `lastTurnId`) and is created
pre-populated with the source's messages up to the fork point, so the new
chat displays exactly the history the agent remembers.

Errors: `404` unknown source; `409` not forkable — the source has no native
conversation yet (no completed turn), or the anchor message has no native
turn mapping (sessions predating per-turn recording can only be forked
whole).

```json
Request:  { "source_session_id": "main", "at_message_id": "42", "title": "My Fork" }
Response: { "id": "fork-a1b2c3d4", "title": "My Fork", "source": "web", "status": "created", "parent_session_id": "main", "message_count": 12 }
```

#### `POST /api/sessions/{id}/resume`
Resume a stopped or idle session (must have a stored `sdk_session_id`).

```json
Response: { "id": "a1b2c3d4", "status": "created", "sdk_session_id": "..." }
```

#### `POST /api/sessions/{id}/archive?force=false`
Archive a session and its whole subtree of child sessions, deepest first
(soft delete, cannot archive "main"). Disconnects any active SDK client. A
session that cannot be archived is listed in `skipped` with the reason, and
its ancestors stay: `session is running` (a turn is in flight) or
`background agents running` (archiving would stop them). `?force=true`
archives those too; their agents end as `interrupted` (`archived`).
`archived` is `false` when nothing was archived.

```json
Response: { "archived": true, "archived_ids": ["a1b2c3d4"], "skipped": [{ "id": "e5f6a7b8", "reason": "background agents running" }] }
```

#### `GET /api/sessions/{id}/events?limit=50`
Get session lifecycle event log (newest first).

```json
Response: {
  "events": [
    { "id": 4, "session_id": "abc", "event_type": "title_changed", "details": { "title": "Launch plan", "previous": "abc" }, "actor_id": "…", "caused_by_actor_id": "…", "created_at": "..." },
    { "id": 3, "session_id": "abc", "event_type": "idle", "details": { "resumable": true }, "actor_id": null, "caused_by_actor_id": null, "created_at": "..." },
    { "id": 2, "session_id": "abc", "event_type": "started", "details": { "sdk_session_id": "..." }, "actor_id": null, "caused_by_actor_id": null, "created_at": "..." },
    { "id": 1, "session_id": "abc", "event_type": "created", "details": { "source": "web" }, "actor_id": "…", "caused_by_actor_id": "…", "created_at": "..." }
  ]
}
```

`actor_id` is who made the change and `caused_by_actor_id` is on whose behalf
(actor ids for [`GET /api/actors`](#actors)). Both are `null` for an event that
records the state of the runtime, for a change with no known person, and for
legacy rows. The event types and their details are in
[Session changes](accounts.md#session-changes).

### Modified Files

#### `GET /api/sessions/{id}/modified-files`
List files modified during a session with diff stats. Reads from `session_file_snapshots` table and compares against current file content on disk.

```json
Response: {
  "files": [
    { "path": "/home/user/project/foo.py", "short_path": "project/foo.py", "status": "modified", "stats": { "additions": 15, "deletions": 3 }, "created_at": "2026-03-04T07:00:00Z" }
  ],
  "summary": { "total_files": 1, "total_additions": 15, "total_deletions": 3 }
}
```

#### `GET /api/sessions/{id}/file-diff?path=...&context=4`
Compute a unified diff for a single file against its session baseline snapshot. Returns structured hunks with line numbers for GitHub PR-style rendering. For markdown files (`.md`, `.markdown`) the response also carries `markdown_content` — the post-change file content (original content for deleted files) used by the UI's rendered-preview toggle — plus `markdown_truncated` when it was cut at the diff line limit. Both are `null`/`false` for other file types.

```json
Response: {
  "path": "/home/user/project/foo.py",
  "short_path": "project/foo.py",
  "status": "modified",
  "binary": false,
  "stats": { "additions": 15, "deletions": 3 },
  "hunks": [
    {
      "old_start": 10, "old_count": 5, "new_start": 10, "new_count": 7, "header": "class Foo:",
      "lines": [
        { "type": "context", "content": "    def bar(self):", "old_line": 10, "new_line": 10 },
        { "type": "deletion", "content": "        return None", "old_line": 11 },
        { "type": "addition", "content": "        return 42", "new_line": 11 }
      ]
    }
  ],
  "truncated": false,
  "markdown_content": null,
  "markdown_truncated": false
}
```

### Chat

#### `POST /api/chat`
Send a message and get the complete response (non-streaming).

```json
Request:  { "message": "Hello", "session_id": "main", "client_msg_id": "4b6f0c1e-2a1d-4c8e-9a51-0e2b7c4d9f10" }
Response: { "response": "Hi there!", "session_id": "main" }
```

`client_msg_id` is optional: a retry with an id the session saw in the last
10 minutes runs nothing and answers with an empty `response`. While another
turn of the session runs, the message waits in the session's
[turn queue](#get-apisessionsidqueue) (source `api`).

For streaming, use the WebSocket endpoint.

### Agents

A session's sub-agents and background tasks, its stops and its turn queue.
See [Sub-agents and background tasks](agents.md) for what the statuses and
stops mean. Errors are `{"detail": {"code": "<code>", "message": "<text>"}}`
(some add keys); clients branch on `code`. Timestamps are UTC ISO-8601 with
`Z`. Every key below is always present (`null`, `0` or `[]` when empty).

**AgentView** (one record):

```json
{
  "id": "ag_3f9a1c2b7d4e", "session_id": "s-1234", "backend": "claude", "kind": "agent",
  "task_id": "a1b2c3d4e5f6a7b8c", "task_type": "local_agent", "spawn_tool_use_id": "toolu_01AGENT",
  "parent_agent_id": null, "depth": 1, "name": "build-api", "label": "build-api",
  "description": "Build the API", "agent_type": "general-purpose", "command": null,
  "background": true, "ambient": false, "status": "running", "end_reason": null, "error": null,
  "attention": null, "activity": "Bash: pytest -q", "summary": "Running the API test suite",
  "result_preview": null, "requested_model": "opus", "model": "<model-id>", "model_changed": null,
  "tokens": 38211, "tool_uses": 17, "cost_usd": 1.42, "runs": 1,
  "created_at": "2026-02-27T02:01:00.000Z", "started_at": "2026-02-27T02:01:01.000Z",
  "run_started_at": "2026-02-27T02:01:01.000Z", "last_activity_at": "2026-02-27T02:05:12.000Z",
  "ended_at": null, "stop_requested_at": null, "stopped_by_actor_id": null, "resumable": false,
  "controls": { "stop": true, "transcript": true, "output": false, "message": true },
  "workflow": null, "rev": 1772150400123456
}
```

- `kind`: `agent|workflow|shell|monitor|teammate|remote_agent|mcp_task|other`.
- `status`: `starting|running|stopping|completed|failed|stopped|interrupted|lost`.
- `end_reason`: `completed|error|spawn_failed|user|stop_all|turn_stop|orchestrator|unknown_stop|process_ended|gateway_restart|force_stop|model_switch|archived|vanished`, or `null`.
- `attention`: `needs_input|failed|interrupted|lost|stop_unconfirmed`, or `null`.
- `model_changed`: `{"from": "<model-a>", "to": "<model-b>", "at": "…"}` when the agent ran on more than one model.
- `cost_usd` is a live estimate. `rev` only grows (also across restarts): keep the view with the highest `rev`.
- `workflow` is the workflow progress tree for `kind: "workflow"`, else `null`.

**AgentSummary**: `{"total", "live", "starting", "running", "stopping", "completed", "failed", "stopped", "interrupted", "lost", "attention", "shells_live", "tokens", "cost_usd", "oldest_live_started_at"}` (housekeeping tasks excluded).

**AgentCapabilities**: `{"stop_task", "transcript", "output", "interrupt_spares_background", "quiet_after_minutes"}`. `interrupt_spares_background` is false when Stop would end background agents (the UI then asks first).

**QueueEntryView**: `{"queue_id": "q_9c1d2e3f", "client_msg_id": "4b6f…", "preview": "also run the e2e suite", "source": "web", "internal": false, "actor_id": "…", "created_at": "…"}`. Internal entries (scheduled wakeups and runs) show a fixed label as `preview`.

**AgentBrief**: `{"id", "label", "kind", "status", "started_at", "tokens"}`.

#### `GET /api/sessions/{id}/agents?limit=200`
The session's **AgentSnapshot**: every live agent plus the most recent ended
ones up to `limit` (at most 500), oldest first; the summary, the
capabilities and the turn queue. `404 session_not_found`.

```json
Response: { "session_id": "s-1234", "agents": [AgentView], "summary": AgentSummary,
            "capabilities": AgentCapabilities, "queue": [QueueEntryView],
            "generated_at": "2026-02-27T02:06:01.000Z" }
```

#### `GET /api/sessions/{id}/agents/{agent_id}`
One agent with its full brief and final report.

```json
Response: { "agent": AgentView, "prompt": "…", "result_text": "…" }
```

Errors: `404 agent_not_found`; `404 agent_in_other_session` with
`owner_session_id` (a fork shows its parent's spawn cards; their agents live
in the parent).

#### `GET /api/sessions/{id}/agents/{agent_id}/transcript?before_seq=&after_seq=&limit=200`
A page of the agent's transcript, ascending by `seq` (per agent, from 1, no
gaps): the tail (no cursor), the `limit` items below `before_seq`, or the
first `limit` items above `after_seq` (at most one cursor; `limit` at most
500). Items still waiting to be written are included.

```json
Response: {
  "agent_id": "ag_3f9a1c2b7d4e",
  "items": [
    { "seq": 41, "at": "…", "kind": "tool_call", "tool_use_id": "toolu_02CHILD", "tool": "Bash", "input": { "command": "pytest -q" }, "input_truncated": false, "child_agent_id": null },
    { "seq": 42, "at": "…", "kind": "tool_result", "tool_use_id": "toolu_02CHILD", "result": "212 passed, 1 failed", "is_error": false, "truncated": false, "full_size": 20 },
    { "seq": 43, "at": "…", "kind": "text", "text": "One test fails; fixing it." },
    { "seq": 44, "at": "…", "kind": "thinking", "text": "The fixture is stale." },
    { "seq": 45, "at": "…", "kind": "lifecycle", "event": "stopped", "run": 1, "reason": "user", "actor_id": "…" }
  ],
  "first_seq": 41, "last_seq": 45, "has_more_before": true
}
```

`lifecycle.event`: `started|resumed|stop_requested|completed|failed|stopped|interrupted|lost`.
Tool inputs and results are cut at `agent.subagents.transcript_item_max_bytes`
(`truncated`/`input_truncated` with `full_size` in bytes). Errors:
`400 bad_cursor` (both cursors, or a cursor that is not a non-negative
integer); `404 agent_not_found`.

#### `GET /api/sessions/{id}/agents/{agent_id}/output?tail_bytes=65536`
The tail of a background shell's or monitor's output file (`tail_bytes` at
most 262144). Served only from the CLI's per-user temp directory
(`claude-<uid>` under the temp dir or `/tmp`) and only as
`<task_id>.output`; anything else, or a file that is gone, answers
`available: false`.

```json
Response: { "agent_id": "ag_…", "text": "…", "size": 24, "truncated": false, "available": true }
```

Errors: `404 agent_not_found`; `409 not_a_shell` (not a shell or monitor).

#### `POST /api/sessions/{id}/agents/{agent_id}/stop`
Stop one agent through the runtime's per-task stop; with `cascade` (the
default) also the agents it started, deepest first. Takes no run lock and
leaves the turn and every other agent running. The records turn `stopping`
and settle as `stopped` (`user`) when the runtime confirms.

```json
Request:  { "cascade": true }
Response: { "agent": AgentView, "stopping": ["ag_…", "ag_3f9a1c2b7d4e"] }
```

`202` with the ids now stopping; `200` with `"stopping": []` when the agent
had ended already. Errors: `404 agent_not_found` (or
`agent_in_other_session`); `409 no_runtime` (the session's runtime is gone;
the record is settled as `interrupted`); `501 unsupported` (the runtime
cannot stop one agent: Codex).

#### `POST /api/sessions/{id}/stop`
Stop by scope. Records who asked.

```json
Request:  { "scope": "turn", "confirm_kill_background": false }
Response: { "scope": "turn", "turn": "stopped", "would_end": [], "shell_count": 0 }

Request:  { "scope": "all", "include_shells": true }
Response: { "scope": "all", "turn": "not_running", "stopping": ["ag_…"], "unsupported": [] }

Request:  { "scope": "force" }
Response: { "scope": "force", "turn": "killed", "ended": ["ag_…"] }
```

- `turn` (the default): the orchestrator's turn only; background agents keep
  running. `turn` is `stopped`, `not_running` (nothing was sent), `killed`
  (cancelled: only when no agent is live), `not_stopped` (it did not end in
  5 s and agents are live: `would_end` lists what a force stop would end) or
  `needs_confirmation` (this runtime cannot spare background agents; nothing
  was sent; repeat with `confirm_kill_background: true` to stop the turn and
  them).
- `all`: every live agent (shells and monitors unless `include_shells` is
  false), then the turn. `unsupported` lists agents the runtime cannot stop
  one by one.
- `force`: cancel the turn (or restart the runtime when no turn runs);
  everything in it ends.

Queued messages are never touched. Errors: `404 session_not_found`;
`422 bad_scope`.

#### `GET /api/sessions/{id}/queue`
Messages waiting for the session's current turn to end.

```json
Response: { "items": [QueueEntryView] }
```

`404 session_not_found`.

#### `DELETE /api/sessions/{id}/queue/{queue_id}`
Cancel one of your own queued messages before it runs. It runs nothing and
stores nothing; its text comes back.

```json
Response: { "cancelled": true, "content": "also run the e2e suite" }
```

Errors: `404 queue_entry_not_found`; `409 already_started` (it holds the
turn now); `403 not_owner` (someone else's message, a channel relay or an
internal trigger).

### Tasks

#### `GET /api/tasks?status=pending&tag=backend&sort=position`
List tasks. All filters optional. Statuses are configurable — see
`GET /api/task-statuses`; empty means all non-done. `sort` accepts `deadline`
(default), `updated_at`, `created_at`, or `position` (board order).

#### `GET /api/tasks/search?q=keyword&status=`
Full-text search on task titles and content (FTS5). Optional status filter.

```json
Response: { "tasks": [{ "id": "2026-03-01-fix-bug", "title": "Fix bug", "status": "pending", ... }] }
```

#### `GET /api/tasks/board?limit=100&tag=`
Every lane in one round trip — the board's only read. Returns the configured
statuses plus one page of each, ordered by `position`. `total` is the lane's
true count so a column can offer "+N more"; the `done` lane is capped tighter
than the rest since it grows without bound.

```json
Response: {
  "statuses": [{ "name": "pending", "label": "Pending", "color": "#...", ... }],
  "lanes": [{ "status": "pending", "total": 12, "tasks": [ ... ] }],
  "status_since": { "2026-03-01-fix-bug": "2026-03-02T10:00:00Z" }
}
```

#### `GET /api/tasks/tags?include_done=false`
Distinct tags with their task counts, most-used first (filter-bar facets).

```json
Response: { "tags": [{ "name": "backend", "count": 7 }] }
```

#### `POST /api/tasks`
Create a task. Returns the created row; **409** if the duplicate guard refuses
(retry with `confirm_duplicate: true` to override), **422** if `status` names a
status that does not exist — a retry cannot fix that one, so it is kept
distinct from the collision.

```json
Request:  { "title": "Fix bug", "content": "Details...", "deadline": "2026-03-01", "tags": "backend,urgent" }
Response: { "task": { "id": "2026-03-01-fix-bug", ... }, "message": "Task created: ..." }
409:      { "detail": { "reason": "duplicate", "duplicates": [ ... ], "message": "..." } }
422:      { "detail": { "reason": "invalid_status", "duplicates": [], "message": "..." } }
```

#### `GET /api/tasks/{id}`
Get task details including full markdown file content.

```json
Response: { "id": "2026-03-01-fix-bug", "title": "Fix bug", "status": "pending", "content": "# Fix bug\n\n...", ... }
```

#### `PATCH /api/tasks/{id}`
Update a task. All fields are optional. `content` replaces the full markdown
file; title and deadline are re-synced to SQLite. Returns the full updated row.

`deadline` and `tags` read by **presence**: omit the key to leave the field
alone, or send `""` to clear it. An invalid status is a 400 (previously
reported as a success that changed nothing).

```json
Request:  { "status": "done", "note": "Fixed in PR #123" }
Request:  { "content": "# Updated Title\n\n**Deadline:** 2026-03-15\n\nNew details..." }
Request:  { "deadline": "" }
Response: { "task": { ... }, "task_id": "2026-03-01-fix-bug", "updated": true }
```

#### `GET /api/tasks/{id}/events`
A task's status history, oldest first.

```json
Response: { "events": [
  { "id": 1, "task_id": "...", "from_status": null, "to_status": "pending", "actor": "web", "actor_id": "…", "caused_by_actor_id": "…", "created_at": "..." },
  { "id": 2, "task_id": "...", "from_status": "pending", "to_status": "in_progress", "actor": "impl-abc123", "actor_id": "…", "caused_by_actor_id": "…", "created_at": "..." }
] }
```

`actor` is the origin of the change (`web`, a session id, `system`,
`backfill`). `actor_id` is who made the change and `caused_by_actor_id` on
whose behalf, both actor ids for [`GET /api/actors`](#actors) and `null` on
rows from before they were recorded. See
[Status History](tasks.md#status-history).

#### `POST /api/tasks/{id}/move`
Place a task in a lane — the board's drag-and-drop write. Send *intent*
("between these two cards"), not a computed rank: the server resolves the
neighbours itself, so a stale client board can't corrupt the ordering.
`before_id` is the card that ends up directly above, `after_id` the one
directly below; omit both to append. Omit `status` to reorder in place.

Moving into or out of `done` moves the markdown file between `active/` and
`done/` as a side effect.

```json
Request:  { "status": "in_progress", "before_id": "2026-03-01-a", "after_id": "2026-03-01-b" }
Response: { "task": { "id": "...", "status": "in_progress", "position": 3072.0, ... } }
```

### Workflow Runs

Budget-capped multi-agent jobs. See [workflow-runs.md](workflow-runs.md) for
semantics (engines, budget metering, lifecycle, journals). On the wire,
`spec.prompt` is trimmed to 500 chars.

#### `GET /api/workflow-runs?status=&limit=50&offset=0`
List runs, newest first. `status`: `active` (pending+running), an exact status (`pending`, `running`, `done`, `failed`, `killed`, `budget_exhausted`), or empty for all.

```json
Response: {
  "runs": [{
    "id": "wfr-a1b2c3d4", "engine": "claude-workflow", "title": "Sample batch audit",
    "spec": { "prompt": "Audit samples/batch-07/ for ..." },
    "status": "running", "budget_usd": 12.0, "spent_usd": 3.42, "warned_at": null,
    "session_id": "workflow:wfr-a1b2c3d4", "journal_dir": "/home/alice/.nerve/workflow-runs/wfr-a1b2c3d4",
    "created_by": "session:main",
    "created_by_actor_id": "…", "caused_by_actor_id": "…",
    "killed_by_actor_id": null, "killed_caused_by_actor_id": null,
    "error": null, "result": null,
    "created_at": "...", "started_at": "...", "finished_at": null, "updated_at": "..."
  }],
  "total": 1
}
```

`created_by` is the origin text. `created_by_actor_id` and
`caused_by_actor_id` name who created the run and on whose behalf; the run's
session is created with that cause. `killed_by_actor_id` and
`killed_caused_by_actor_id` name who killed it. All four are `null` on runs
from before attribution, and the kill pair is `null` when the kill names no
person (for example a `/stop` from a chat channel). See
[accounts.md](accounts.md#attribution).

#### `POST /api/workflow-runs`
Start a run. `engine` is `claude-workflow` or `codex-ultracode`; `budget_usd` is required unless `workflows.allow_unbudgeted` is set. Optional: `title`, `model`, `effort`, `cwd` (must be an existing directory). Returns the created run immediately (`pending`, or `running` once dispatched); execution happens in the background. The signed-in person is recorded as the run's actor and cause.

```json
Request:  { "engine": "claude-workflow", "prompt": "Audit samples/batch-07/ for ...", "budget_usd": 12, "title": "Sample batch audit" }
Response: { "id": "wfr-a1b2c3d4", "status": "pending", ... }
```

#### `GET /api/workflow-runs/{id}`
Run detail (same shape as the list items).

#### `POST /api/workflow-runs/{id}/kill`
Terminate a run. Scoped strictly to the run's own session/subprocess; idempotent on already-terminal runs. The kill that ends the run records the signed-in person in `killed_by_actor_id` and `killed_caused_by_actor_id`.

```json
Request:  { "reason": "superseded" }
Response: { "id": "wfr-a1b2c3d4", "status": "killed", ... }
```

#### `GET /api/workflow-runs/{id}/journal`
Journal contents from `<runs_dir>/<run-id>/`: the `run.json` snapshot, the parsed `events.ndjson` lifecycle events (created, started, budget_warning, terminal status, enforced_stop), and `result.md` when the run finished.

```json
Response: {
  "run_json": { "id": "wfr-a1b2c3d4", ... },
  "events": [
    { "ts": "...", "run_id": "wfr-a1b2c3d4", "event": "created", "engine": "claude-workflow", "budget_usd": 12.0, "created_by": "session:main" },
    { "ts": "...", "run_id": "wfr-a1b2c3d4", "event": "started", "session_id": "workflow:wfr-a1b2c3d4", "backend": "claude", "model": "..." }
  ],
  "has_result": false,
  "result": ""
}
```

### Skills

#### `GET /api/skills`
List all skills with aggregated usage statistics.

```json
Response: {
  "skills": [{
    "id": "my-skill", "name": "my-skill", "description": "Query database...",
    "version": "1.0.0", "enabled": true, "total_invocations": 5, "success_count": 5,
    "avg_duration_ms": 12, "last_used": "2026-03-06T21:00:00"
  }]
}
```

#### `GET /api/skills/{id}`
Get full skill content, metadata, references, and usage stats.

#### `POST /api/skills`
Create a new skill.

```json
Request:  { "name": "code-review", "description": "This skill should be used when...", "content": "## Steps\n..." }
Response: { "id": "code-review", "name": "code-review", "created": true }
```

#### `PUT /api/skills/{id}`
Update a skill's SKILL.md content (full raw file including frontmatter).

```json
Request:  { "content": "---\nname: code-review\ndescription: ...\n---\n\n# Instructions\n..." }
Response: { "id": "code-review", "name": "code-review", "updated": true }
```

#### `DELETE /api/skills/{id}`
Delete a skill (removes directory and DB record).

#### `PATCH /api/skills/{id}/toggle`
Enable or disable a skill.

```json
Request:  { "enabled": false }
Response: { "id": "code-review", "enabled": false }
```

#### `GET /api/skills/{id}/usage?limit=50`
Get usage history and aggregate stats for a skill.

#### `GET /api/skills/stats`
Aggregate usage stats across all skills.

#### `POST /api/skills/sync`
Re-scan the `workspace/skills/` directory and sync to DB. Discovers new skills, removes deleted ones, preserves enabled state.

### MCP Servers

#### `GET /api/mcp-servers`
List all MCP servers with aggregated usage statistics.

```json
Response: {
  "servers": [{
    "name": "nerve", "type": "sdk", "enabled": true, "tool_count": 34,
    "total_invocations": 127, "success_count": 125, "avg_duration_ms": null,
    "last_used": "2026-03-14T19:00:00", "first_seen_at": "...", "last_seen_at": "..."
  }]
}
```

#### `GET /api/mcp-servers/{name}`
Get server detail including per-tool breakdown and recent usage.

```json
Response: {
  "name": "nerve", "type": "sdk", ...,
  "tools": [{ "tool_name": "task_list", "invocations": 42, "success_count": 42, "avg_duration_ms": null, "last_used": "..." }],
  "recent_usage": [{ "id": 1, "server_name": "nerve", "tool_name": "task_list", "session_id": "abc", "success": true, "created_at": "..." }]
}
```

#### `GET /api/mcp-servers/{name}/usage?limit=50`
Paginated usage history for a server.

#### `POST /api/mcp-servers/reload`
Re-read MCP server config from YAML files and refresh the in-memory cache. New sessions will use updated config.

```json
Response: { "reloaded": 2, "servers": [...] }
```

### Memory Files

#### `GET /api/memory/files`
List markdown files in workspace.

#### `GET /api/memory/file/{path}`
Read a memory file.

#### `PUT /api/memory/file/{path}`
Write a memory file.

```json
Request: { "content": "# Updated content..." }
```

### memU Semantic Memory

#### `GET /api/memory/memu`
Get memU categories, items, and indexed resources.

```json
Response: {
  "available": true,
  "categories": [{ "id": "...", "name": "preferences", "description": "...", "summary": "..." }],
  "items": [{ "id": "...", "memory_type": "profile", "summary": "User works at Acme Corp", "resource_id": "...", "created_at": "...", "happened_at": "..." }],
  "resources": [{ "id": "...", "url": "/path/to/file.md", "modality": "document", "caption": "...", "created_at": "..." }],
  "category_items": { "category_id": ["item_id_1", "item_id_2"] }
}
```

#### `POST /api/memory/memu/categories`
Create a new category.

```json
Request:  { "name": "travel", "description": "Travel plans and logistics" }
Response: { "name": "travel", "created": true }
```

#### `PATCH /api/memory/memu/categories/{id}`
Update a category's summary or description. Re-embeds the category after update.

```json
Request:  { "summary": "Updated summary text", "description": "New description" }
Response: { "id": "...", "updated": true }
```

#### `PATCH /api/memory/memu/items/{id}`
Update a memory item's content, type, or category assignments.

```json
Request:  { "content": "New text", "memory_type": "knowledge", "categories": ["work"] }
Response: { "id": "...", "updated": true }
```

#### `DELETE /api/memory/memu/items/{id}`
Delete a memory item.

```json
Response: { "id": "...", "deleted": true }
```

#### `GET /api/memory/memu/health`
memU service health metrics and operation stats.

```json
Response: {
  "initialized_at": "...", "service_available": true,
  "operations": { "recall": { "call_count": 5, "avg_duration_s": 0.8, "error_count": 0 }, ... },
  "in_flight": [],
  "database": { "total_items": 2924, "total_categories": 24, "db_size_mb": 132.97, "type_distribution": { "profile": 671, ... } }
}
```

#### `GET /api/memory/memu/audit?action=&target_type=&limit=100&offset=0`
Paginated audit log of memU mutations.

```json
Response: {
  "logs": [{ "id": 1, "timestamp": "...", "action": "item_deleted", "target_type": "item", "target_id": "abc123", "source": "agent_tool", "details": {} }],
  "offset": 0, "limit": 100
}
```

### Diagnostics

#### `GET /api/diagnostics`
System health and status, including task/FTS index health.

```json
Response: {
  "system": { "hostname": "...", "memory_mb": 65.2, "disk_free_gb": 180.5 },
  "tasks": { "total": 92, "active": 16, "done": 76, "fts_indexed": 92, "fts_ok": true },
  "sync": { "github": { "cursor": "...", "last_run": "...", "records_fetched": 3, "records_processed": 3, "error": null } },
  "recent_cron_logs": [...],
  "usage": {
    "last_7d": { ... }, "daily": [...], "by_model": [...], "by_source": [...],
    "by_cron_job": [ { "job_id": "daily-digest", "runs": 7, "sessions": 7, "turns": 9, "input_tokens": 120, "cache_read": 410000, "cache_creation": 95000, "output_tokens": 5400, "cost_usd": 1.84, "estimated_cost_usd": 0.0, ... } ]
  }
}
```

In every usage row, `input_tokens` counts only uncached input. Cache reads
(`cache_read`) and cache writes (`cache_creation`) are separate fields, so
the total input is the sum of the three.

`usage.by_cron_job` attributes each turn to the cron job that caused it:
turns in the job's own sessions (`cron:<job>`, `cron:<job>:<run>`), and
turns of workflow runs that the job started, either as a workflow-run job
or through a `workflow_run_start` call in one of its sessions (review-loop
legs and runs started from another workflow run are not attributed).
Workflow-run turns have source `workflow` in `by_source`, so the per-job
totals can be larger than the `cron` row there. `runs` counts the job's
runs in the same 7-day window that did agent work: runs linked to a cron
session, and workflow runs that began execution. Missed and skipped runs,
and workflow runs that were cancelled while still queued, are not counted,
so `cost_usd / runs` is the average cost per run. Jobs without usage are
omitted.

#### `GET /api/cron/logs?job_id=&limit=50&offset=0`
Get cron job execution logs, newest first. `limit` is clamped to 1–200;
combine with `offset` for pagination. Each log row carries the
`session_id` of the chat session the run executed in (null for source
runners and missed runs).

Each log row also carries `actor_id` (who started the run) and
`caused_by_actor_id` (on whose behalf). Both are the system actor for a
scheduled run, and the person for a run that a person started with
`POST /api/cron/jobs/{job_id}/trigger`. Both are `null` on older rows. See
[Attribution](accounts.md#attribution).

```json
Response: {
  "logs": [ { "id": 12, "job_id": "...", "status": "success", "session_id": "cron:...", "actor_id": "…", "caused_by_actor_id": "…", ... } ],
  "total": 234,
  "limit": 50,
  "offset": 0
}
```

#### `POST /api/cron/jobs/{job_id}/trigger`
Run a cron job or a source runner now, and wait until the run ends. The run
log records the signed-in person as actor and cause. A session that the run
creates (an isolated run, or a new generation of a persistent job) has the
person as its cause; a persistent job's current session keeps its cause. The
workflow run that a workflow-run job starts is the system actor's, for the
person. Returns 404 for an unknown job.

```json
Response: { "job_id": "morning-briefing", "triggered": true }
```

### Health

#### `GET /health`
No auth required.

```json
Response: { "status": "ok", "version": "0.1.0", "generation": "6f1a0c2b9e4d7a31" }
```

`generation` is a random value fixed when the process started. It is what
tells the new process from the old one still answering as it shuts down,
which is what makes `POST /api/system/restart` observable: the setup page
polls `/health` until the value changes. Deliberately opaque — an uptime or
a counter would tell an anonymous caller how long the box has been up.

## WebSocket Protocol

Connect to `ws[s]://host:port/ws?token=<jwt>` (the `nerve_token` cookie works
too). The token is resolved to an actor at admission. A credential that names
nobody is refused with close code `4001`. The actor stays fixed until the socket
reconnects. Disabling the account closes the socket with code `1008`.

Unlike REST, a WebSocket never hands back a refreshed token — it has no
response headers. The browser's ordinary REST traffic keeps the stored token
fresh.

### Client → Server

```typescript
// Send a chat message. client_msg_id (optional) identifies it: a resend
// with the same id runs once, and the queue entry and started turn carry it.
{ type: "message", content: "Hello", session_id: "main", file_ids?: [], model?: "…", client_msg_id?: "4b6f…" }

// Stop the session's turn; background agents keep running (records a
// stop_requested session event for the connection's person). The web UI
// stops over REST (POST /api/sessions/{id}/stop) to get the structured result.
{ type: "stop", session_id: "main" }

// Switch active session (drops this socket's agent subscriptions)
{ type: "switch_session", session_id: "abc123" }

// Follow one agent's live transcript (agent_event frames) / stop following.
// At most 8 per socket: subscribing to a 9th drops the oldest. An unknown
// agent, or one not in the active session, answers
// { type: "error", session_id, error: "agent_not_found" }. That error is
// about the subscription only, not the turn: the web UI ignores it.
{ type: "agent_subscribe", session_id: "s-1234", agent_id: "ag_3f9a1c2b7d4e" }
{ type: "agent_unsubscribe", session_id: "s-1234", agent_id: "ag_3f9a1c2b7d4e" }

// Fork a session
{ type: "fork", session_id: "main", at_message_id: "msg-42", title: "My Fork" }

// Resume a stopped/idle session (records a created session event with
// reason "resume" for the connection's person)
{ type: "resume", session_id: "abc123" }

// Answer an interactive tool (AskUserQuestion, plan approval)
{ type: "answer_interaction", session_id: "main", interaction_id: "uuid", result?: { … }, denied?: false, message?: "" }

// Keep-alive
{ type: "ping" }
```

### Server → Client

Session events go to the sockets bound to that session; the client drops
those for another session. Events marked *global* go to every socket.

```typescript
// Streaming token, thinking, tool calls and results of the orchestrator's
// turn. Sub-agent work never appears here (see agent_event).
{ type: "token", session_id: "main", content: "Hello" }
{ type: "thinking", session_id: "main", content: "Let me check..." }
{ type: "tool_use", session_id: "main", tool: "Read", input: { file_path: "..." }, tool_use_id: "toolu_..." }
{ type: "tool_result", session_id: "main", tool_use_id: "toolu_...", result: "...", is_error: false }
// Live output of a running command (Codex)
{ type: "tool_output", session_id: "main", tool_use_id: "toolu_...", content: "..." }

// Turn complete (context usage)
{ type: "done", session_id: "main", usage?: { input_tokens: 1234, output_tokens: 567, cache_read_input_tokens: 890, cache_creation_input_tokens: 0 }, max_context_tokens?: 1048576, num_turns?: 1 }

// Turn stopped by a user / error
{ type: "stopped", session_id: "main" }
{ type: "error", session_id: "main", error: "..." }

// An autonomous turn starts (the runtime resumed after background work
// settled). triggers names the agents that settled since the last turn
// (at most 5; more counts the rest); both are absent when nothing did.
{ type: "auto_turn", session_id: "main", triggers?: [{ agent_id: "ag_…", label: "build-chart", kind: "agent", status: "completed" }], more?: 0 }

// A turn fired by a scheduled wakeup starts
{ type: "wakeup", session_id: "main" }

// Another client of this session sent a message (the sender sees its own
// optimistically; actor_id is who sent it, matching the stored row)
{ type: "user_message", session_id: "main", content: "Hello", blocks: null, actor_id: "…", client_msg_id: "4b6f…" | null }

// Sent on every bind (connect and switch_session): running state, lifecycle
// status, the agent snapshot, and the turn's buffered events when a turn runs
{ type: "session_status", session_id: "abc123", is_running: true, status: "active", buffered_events?: [...], agents: AgentSnapshot }
{ type: "session_switched", session_id: "abc123" }

// Turn boundary (global): is_running is the orchestrator's turn only;
// has_background_tasks / background_count are live agents
{ type: "session_running", session_id: "abc123", is_running: false, pending_wakeup_at: null, pending_wakeup_created_by_actor_id: null, pending_wakeup_caused_by_actor_id: null, has_background_tasks: true, background_count: 2 }

// Agents (see "Agents" above for the shapes)
// Changed records, coalesced per session: creation, status and attention changes go out after 50 ms,
// progress updates at most once a second (every two seconds above 20 live agents); never buffered.
// Keep the view with the highest rev per agent.
{ type: "agent_update", session_id: "s-1234", agents: [AgentView], summary: AgentSummary }
// One transcript item, only to sockets that subscribed to the agent
// (channel agent:<session_id>:<agent_id>). A seq gap means items were
// dropped under load: refetch with after_seq.
{ type: "agent_event", session_id: "s-1234", agent_id: "ag_3f9a1c2b7d4e", item: AgentEventItem }
// Live-agent counts changed between turns (global, debounced 1 s)
{ type: "session_agents", session_id: "s-1234", background_count: 5, has_background_tasks: true }
// The turn queue: the full queue, plus what just left it (never buffered)
{ type: "queue_update", session_id: "s-1234", queue: [QueueEntryView], removed?: [{ queue_id: "q_9c1d2e3f" | null, client_msg_id: "4b6f…" | null, reason: "started" | "cancelled" | "duplicate" }] }
// Kept for one release for tabs from before the agent roster (ignored by
// the current UI): status is running | done | failed
{ type: "background_tasks_update", session_id: "s-1234", tasks: [{ task_id: "a1b2…", label: "build-api", tool: "Agent", status: "running" }] }

// Workflow progress tree of a Workflow tool call
{ type: "workflow_progress", session_id: "main", tool_use_id: "toolu_...", workflow: { name: "…", status: "running", summary?: "…", phases: [...], agents: [...], totalTokens: 0, totalToolCalls: 0, agentCount: 0 } }

// Session title updated (AI-generated)
{ type: "session_updated", session_id: "abc123", title: "Italy Vacation Planning" }

// Session forked
{ type: "session_forked", source_id: "main", fork_id: "fork-a1b2c3d4", title: "My Fork" }

// Session resumed
{ type: "session_resumed", session_id: "abc123" }

// Plan file updated (Write/Edit to .claude/plans/, also by a sub-agent)
{ type: "plan_update", session_id: "main", content: "# Plan\n..." }

// The serving model changed (a capacity fallback and back)
{ type: "model_changed", session_id: "main", from_model: "…", to_model: "…", downgrade: true }

// Backend status (Codex rate limits, retryable errors)
{ type: "backend_status", session_id: "main", subtype: "codex_rate_limits" | "codex_status" | "codex_error", data: { … } }

// Workflow run created / status or spend changed (global; session_id is the
// run's own session, null before dispatch)
{ type: "workflow_run_update", session_id: "workflow:wfr-a1b2c3d4", run: { id: "wfr-a1b2c3d4", status: "running", spent_usd: 3.42, budget_usd: 12.0, ... } }

// Review loop changed (global)
{ type: "review_loop_update", session_id: "…", loop: { … }, message?: { … } }

// Task board row changed (global; session_id is null)
{ type: "task_updated", session_id: null, event: "created" | "updated" | "moved" | "done", task: { … } }

// File modified by the agent or a sub-agent (Edit/Write/NotebookEdit succeeded)
{ type: "file_changed", session_id: "main", path: "/home/user/project/foo.py", operation: "edit", tool_use_id: "toolu_..." }

// Interactive tool waiting for user input (AskUserQuestion, ExitPlanMode,
// EnterPlanMode, Codex approvals). agent names the sub-agent that asked;
// null for the orchestrator. agent_id is null when no record is known.
{ type: "interaction", session_id: "main", interaction_id: "uuid", interaction_type: "question" | "plan_exit" | "plan_enter" | "command_approval" | "file_approval" | "permission_approval", tool_name: "AskUserQuestion", tool_input: { ... }, agent: { agent_id: "ag_…" | null, task_id: "a1b2…", label: "build-api" } | null }
// ... and settled (any client answered, or it was cancelled)
{ type: "interaction_resolved", session_id: "main", interaction_id: "uuid" }
// A session waits for input, or no longer does (global)
{ type: "session_awaiting_input", session_id: "main", awaiting: true }

// Notifications (global)
{ type: "notification", notification_id: "…", notification_type: "notify" | "question" | …, session_id: "…", title: "…", body: "…", priority: "normal", options: [...] | null }
{ type: "notification_answered", notification_id: "…", session_id: "…", answer: "…", answered_by: "…", actor_id: "…" }
{ type: "notification_expired", notification_id: "…", session_id: "…", notification_type: "question", title: "…" }
// An answer was relayed into the session as a message
{ type: "answer_injected", session_id: "…", notification_id: "…", title: "…", answer: "…", answered_by: "…", content: "…", actor_id: "…" }

// Keep-alive response
{ type: "pong" }
```
