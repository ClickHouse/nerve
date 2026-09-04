# Configuration Reference

Nerve merges configuration from up to three layers, lowest precedence first:

1. `workspace/config/settings.yaml`: shareable, git-tracked settings that live
   inside the workspace. This is the layer a remote config repo syncs.
2. `config.yaml`: machine-local base settings.
3. `config.local.yaml`: machine-local secrets and personal overrides (gitignored).

Each layer is deep-merged over the one before it, so a machine can override a
shared setting locally. Lockdown mode drops both machine-local layers and leaves
the workspace as the only source of truth. With no `settings.yaml` present, only
`config.yaml` and `config.local.yaml` are read.

The workspace location is resolved from `config.yaml` (or the default
`~/nerve-workspace`) *before* `settings.yaml` is read, so a `workspace:` key
inside `settings.yaml` is ignored.

## Which layer a key belongs in

The first two layers divide as below. `nerve init` writes its answers this way,
and migration splits a legacy `config.yaml` on the same table:

| Layer | Gets |
|-------|------|
| `config.yaml` | `workspace`, `deployment`, `provider.aws_profile`, `gateway.ssl.*`, `proxy`, `docker`, `telegram.enabled`, `slack.enabled`, `sync.gmail.accounts`, `external_agents`, `mcp_endpoint`, `workflows.runs_dir` |
| `settings.yaml` | `timezone`, `gateway.host`/`port`, `provider.type`/`aws_region` (incl. the region-scoped Bedrock model IDs), `agent.*`, `memory.*`, `sessions.*`, `sync.*`, the rest of `workflows.*` (the budget caps and cadence), `houseofagents.*`, quiet hours, `telegram.dm_policy`/`stream_mode` |

The test is whether the value would be wrong on another machine: filesystem
paths, credential handles, whose mailboxes this person syncs, which agent
binaries this box has paired. Which provider the deployment uses and which port
it serves on are not in that category, so they are shared. Lockdown makes this
concrete: it drops `config.yaml`, so a key belongs there only if its declared
default is an acceptable answer on every locked box.

Write each key to one layer only. `config.yaml` shadows `settings.yaml`, so a
shared value repeated in both leaves the tracked copy with no effect.

Re-running `nerve init` regenerates `config.yaml` and `config.local.yaml`
wholesale. `settings.yaml` may be shared, so the wizard rewrites only the keys in
the table above and leaves the rest of the file alone; it prints what it added,
updated and removed. Each file is copied to `*.bak` first unless it is empty or
comments-only. Two things to expect: `settings.yaml` is rewritten with
`yaml.safe_dump`, which drops comments whenever anything changed (the previous
file is in `settings.yaml.bak`), and changing an answer overwrites whatever
someone else had under those keys, so review the diff before committing.

Unknown keys are ignored but logged as warnings at startup (and shown by
`nerve doctor`) so typos don't fail silently.

Keys typed `path` below expand `~` and any environment variables that are set.
A blank value (`runs_dir:` with nothing after it, or `""`) means *unset*, so the
documented default applies. That includes `gateway.ssl.cert`/`key`: blank means
TLS is off, not TLS with an empty certificate path.

## Environment Variable References

Any string value in any of the three layers may reference an environment
variable, so secrets can come from the environment (or a secret store) instead of
being written into a file:

```yaml
anthropic_api_key: ${ANTHROPIC_API_KEY}     # required: load fails if unset
gateway:
  host: ${BIND_HOST:-127.0.0.1}             # optional: default when unset or empty
```

| Form | Behavior |
|------|----------|
| `${VAR}` | Required. Loading fails with an error listing every unresolved variable. Only an *unset* variable is an error; `VAR=""` resolves to an empty string. |
| `${VAR:-default}` | Optional. Uses `default` when `VAR` is unset or empty (shell `:-` semantics). |
| `$$` | A literal `$`, so `$${X}` yields the text `${X}`. |

Only the braced `${...}` form is interpolated. A bare `$` is never touched, so
bcrypt `password_hash` values (`$2b$...`), jwt secrets and connection strings
are safe as written. Interpolation runs once, after all three layers are merged,
so any of them may use references.

Resolved values arrive as strings and are converted back to the field's declared
type, including `int | None`, `list[int]` and `list[str]`. So `port: ${PORT}` and
`enabled: ${FEATURE}` behave the same as literal YAML values.

- Booleans accept `true/false`, `1/0`, `yes/no`, `on/off`, `y/n`, `t/f`
  (case-insensitive). `enabled: ${FLAG}` with `FLAG=false` is **off**: the
  string is parsed, not tested for truthiness. An empty value (`FLAG=`) and a
  bare `enabled:` are also off, so blanking a variable reliably disables a
  feature.
- On a list field, **one reference is one element.** `accounts: ${GMAIL}` with
  `GMAIL=a@example.com` yields `["a@example.com"]`. Numeric lists
  (`list[int]`) additionally split on commas, because a comma cannot occur
  inside a number — so `exclude_chats: ${SKIP}` with `SKIP=-100,-200` yields two
  ids. String lists are never split, since a comma is legal inside the value:
  the default `langfuse.redact_patterns` are regexes containing `{20,}`. To set
  several strings, write a YAML list.
- An unrecognized value is logged with the field that owns it, and that field
  keeps its documented default. Integers are parsed with `int()`, so `"1.5"`
  and `"1e3"` are rejected rather than truncated.
- [`lockdown`](#lockdown-remote-only-read-only) is the exception: its default is
  "unprotected", so an unreadable or empty value there is a hard error instead.
- Defaults are not re-scanned: `${A:-${B}}` yields the literal `${B}` when `A`
  is unset. Use a single reference instead.

## Validating Configuration

`nerve config validate` checks a whole config bundle and exits non-zero on any
error, so it can gate a config repo in CI:

```bash
nerve config validate                   # the active install's config
nerve config validate --workspace .     # a checked-out config repo
nerve config validate --portable-only   # ignore this machine's config.yaml layers
nerve config validate --strict-keys     # unknown keys become errors
nerve config validate --strict-env      # every ${ENV_VAR} must be set
nerve config validate --assume-lockdown # check the view a locked box loads
```

It runs even when the config cannot otherwise load, so a missing secret does not
stop it. It reports an error for:

- an unparseable or invalid cron file, a malformed `run_if` gate spec, or a bad
  spec for a built-in gate;
- backend and codex misconfiguration;
- a schedule the daemon would not run as written, in `cron/*.yaml` or in
  `sync.<source>.schedule`, named by job id. Either a 5-field crontab the
  scheduler rejects (`99 * * * *`), which at run time the daemon only refuses
  after the change has merged and synced, or a value that is neither a crontab
  nor an interval (`hourly`, `@daily`), which silently becomes a fixed 2-hour
  cadence. Write a crontab (`*/15 * * * *`) or an interval (`4h`, `30m`,
  `1h30m`, `90s`);
- a path setting written `.` or `./`: `workspace`, `cron.jobs_file`,
  `cron.system_file`, `cron.gate_plugins_dir`, `gateway.ssl.cert`,
  `gateway.ssl.key`, `proxy.binary_path`, `proxy.auth_dir`, `proxy.log_file` or
  `workflows.runs_dir`. A dot is a path like any other, so it aims the setting at
  whatever directory the daemon was started in. Write the path out, or omit the
  key to get the default — a blank value is *unset* (above) and is not an error.
  This matters most for `cron.gate_plugins_dir`, whose `.py` files the daemon
  imports and executes at startup and on every cron reload.

Validation never imports those gate plugins, because checking them would mean
running the bundle it is judging. A `run_if` entry naming a gate type that is not
built in is therefore reported as a warning: validation can confirm neither that
the type exists nor that the spec's fields are right. Gate plugins are code, so
test them as code.

Two checks are lenient by default, so that validating a live install does not cry
wolf:

| Flag | Default | With the flag |
|------|---------|---------------|
| `--strict-keys` | An unknown or misspelled key is a warning. Covers config keys and the fields of a built-in gate's `run_if` spec. | Unknown keys are errors. |
| `--strict-env` | An unset `${ENV_VAR}` is info, since CI has no secrets. | Every reference must resolve. |

Turn `--strict-keys` on in CI. A typo'd key is the most common config mistake and
the quietest, because nothing reads it.

`--portable-only` judges the tracked `<workspace>/config/settings.yaml` layer on
its own. Use it for a change headed to a shared repo: a local override can
otherwise mask an invalid shared value, and, more often, a broken local file can
fail a shared bundle that has nothing wrong with it. Cron is covered too. A
workspace carrying no jobs normally falls back to the machine-local
`~/.nerve/cron`, which is right for an un-migrated install but wrong here, so under
`--portable-only` only the repo's own `config/cron/` counts and anything skipped is
named in the report. Pass `--workspace` as well, because with the machine layers
dropped there is nothing left to read the workspace location from and it falls back
to the default tree. It fails outright if it opened no file under the workspace's
`config/`, so an empty directory, a `settings.yml` typo or a `settings.yaml` left
at the repo root cannot pass as clean. Every run names the layers it read, in
absolute paths:

```
[info] portable layer: /home/you/config-repo/config/settings.yaml
[info] machine-local layers (config.yaml, config.local.yaml) not read: validating the portable workspace config on its own
```

Without `--portable-only` the second line names the machine-local files that were
overlaid instead, and the directory they came from.

In CI, install nerve and run the same command:

```yaml
jobs:
  validate-config:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: astral-sh/setup-uv@v6
      # nerve is not published to PyPI: the bare name is an unrelated project.
      # `uv tool install` because the runner's system interpreter is externally
      # managed (PEP 668) and only the CLI is needed here.
      - run: uv tool install "nerve @ git+https://github.com/ClickHouse/nerve"
      # Add --assume-lockdown if any instance served by this repo is locked and
      # the flag comes from the environment. Without it CI checks the unlocked
      # view and the lockdown checks never run. See "Lockdown" below.
      - run: nerve config validate --workspace . --portable-only --strict-keys
```

Installing from the default branch keeps the validator at or ahead of your
instance, which is the safe direction for `--strict-keys`: a newer validator
knows every key your instance reads. A key the validator accepts but the instance
is too old to read shows up as a startup warning on the instance and in `nerve
doctor`. If a key is ever retired upstream, `--strict-keys` flags it here first;
drop the key, or install the ref you deploy (`nerve @ git+...@v1.2.3`) instead.

`nerve config init-repo` scaffolds this workflow, plus a
[gitleaks](https://github.com/gitleaks/gitleaks) scan ahead of it to catch a
credential pasted into a tracked file. See [Setting up the config
repo](#setting-up-the-config-repo) for the generated file and the end-to-end setup.

## Hot-Reload

Many config changes apply without a restart. Not all do, and a config file
changing on disk is not by itself a reload.

### What triggers a reload

A reload is always explicit. Two things cause one:

- **`nerve reload`**, or `POST /api/config/reload` directly. Re-reads all three
  layers and reloads every reloadable subsystem at once. This is the one to use
  after editing a file on the box.
- **A sync cycle that finds config this daemon is not running.** Applies exactly
  the same set, so a reviewed config PR takes effect on the instance without
  anyone logging in. `workspace_sync.interval_minutes` (default 1) bounds how long
  that takes. The loop compares the revision on disk against the one it last
  applied, not against what its own pull moved, so it also covers a HEAD that
  moved out of band (`nerve config sync`, a bare `git pull` in the workspace) and
  a previous cycle whose reload failed for one subsystem — that one is retried
  every cycle until it takes.

### What a reload applies

| Change | Reloaded? |
|--------|-----------|
| Cron jobs (`config/cron/*.yaml`) | ✅ |
| Custom cron gate plugins (new, edited *and* deleted `.py` files) | ✅ the registry is rebuilt from the directory |
| Cron file locations (`cron.jobs_file`, `system_file`, `gate_plugins_dir`) | ✅ re-read from the new config |
| Cron sources: `sync.telegram`, `.gmail`, `.github`, `.github_events`, `.github_repos`, `.message_ttl_days`, and each source's `schedule` | ✅ runners are rebuilt and rescheduled, all or nothing: a `schedule` the scheduler will not take refuses the source reload with the running sources on their old triggers, and reports the error rather than `ok`. **`sync.codex` is not one of these**; see the restart table |
| MCP servers (`mcp_servers`) | ✅ new sessions get the new set |
| Skills (`skills/`) | ✅ re-scanned |
| `lockdown` | ✅ the write guards and the layer stack both follow |
| Web gateway auth (`auth.*`) | ✅ read per request. Only the gateway's own auth: the MCP endpoint checks `/mcp/v1` against the `auth.jwt_secret` it was mounted with, so rotating that secret is half-hot (see the restart table). `auth.jwt_expiry_hours` governs tokens minted *after* the reload; already-issued tokens keep the window they were signed with until they next slide |
| `notifications.*` | ✅ read per notification |
| `workspace_sync.*` | ✅ from the next sync cycle |
| `retention.*`, `backup.*`, and the `sessions.*` the background loops read | ✅ from the next cycle of that loop |
| `external_agents.targets` (including each target's `enabled`), `.sync_interval_minutes`, `.conflict_policy` | ✅ from the next sweep, provided at least one target existed at startup (see the restart table) |
| `sessions.sticky_period_minutes` | ✅ |
| `telegram.dm_policy`, `.stream_mode` | ✅ read per update. Tightening `open` to `pairing` takes effect on the next message; `allowed_users` does not follow it (see the restart table) |
| `slack.*` | ✅ `enabled` starts or stops the channel; same-workspace token changes reconnect and roll back on failure; other changes apply to the next event |
| `workflows.*` and `workflows.review_loop.*` — budget caps, concurrency, the warning fraction, iteration and criteria caps, leg engines/models, the verifier sandbox | ✅ read per use, by loops and runs already in flight as well as new ones. The two `enabled` flags and the two loop cadences are the exceptions; see the restart table |
| `provider.*` and the API keys it selects (`aws_region`, `aws_profile`, `aws_access_key_id`, and the effective Anthropic key) | ✅ for sessions started **after** the reload. Each client's environment is built from the live reference when the session is created, by the same seam as `agent.*` below |
| **`agent.*` and `codex.*`**: backend choice and models (`agent.backend`, `agent.cron_model`, `agent.model`, `codex.model`, `codex.cron_model`), `max_turns`, `agent.effort`/`cron_effort` and `codex.effort_map`, `agent.thinking`, `agent.context_1m*`, `agent.background_agent_permissions`, `agent.agent_teams`, idle timeouts, cache TTL, `codex.sandbox`, `.approval_policy`, `.web_search`, `.extra_config`, `.tool_timeout_sec`, `.bin_path`, `.auth`/`.api_key`/`.api_key_env`, `.pricing`, `.min_version`/`.max_version`, `.ultracode.*` | ✅ for sessions and turns **started after** the reload. The engine and both backends resolve these through one live reference, so a key cannot be hot in one and frozen in the other |

All of that is reloaded together, and the response says what happened to each
piece: `POST /api/config/reload` returns `ok`, a per-subsystem `detail`, and an
`errors` map. A reload is best-effort by design, because a typo in `settings.yaml`
must not stop a valid cron edit from being applied, so `ok: false` with `detail`
showing four subsystems reloaded and one failed is a normal answer. Check `errors`
rather than reading the 200 as success.

### What still needs a restart

A reload compares the old and new config and reports any of these that changed, as
`restart_required` on `POST /api/config/reload` and as a log warning. Without that
report a reload returns success while the process keeps the old value, which
matters most for `gateway.host`/`port`: they live in `settings.yaml`, so the change
can arrive by workspace sync rather than by a local edit.

The check covers the unconditional entries below. The conditional ones (turning X
on, adding the first target, a session already running) depend on runtime state the
reload cannot inspect, and are documented here only.

| Change | Why |
|--------|-----|
| `gateway.host`, `.port`, `.ssl.*` | the socket is already bound |
| `timezone` | the cron scheduler and every trigger built from it carry the old zone |
| `agent.max_concurrent` | its semaphore cannot be resized under in-flight turns |
| `workspace` | the skill manager, the tool context, the memory bridges and each session's working directory all captured it at startup. Following it in one of them and not the others would be worse than not following it at all |
| `memory.*`, `xmemory.*` | the bridges hold the config they were constructed with |
| `codex.home_dir` | half-hot, which is why it is here: new sessions are handed the new `CODEX_HOME`, but the directory is only created when the backend is built, so nothing creates the new one. Change it and restart, rather than leaving sessions pointed somewhere that may not exist |
| `sync.codex.*` (`enabled`, every `origins[*]` field, `store_encrypted_reasoning`, `workspace_filter.*`) | Codex thread sync is a **different service** from the cron sources above, built once at startup with one polling worker per origin. Adding or editing an origin and reloading reports `ok` and ingests nothing |
| `langfuse.*` | set up before the engine, caching its host, redaction patterns and `LANGFUSE_*` environment exports in process globals |
| `telegram.enabled`, `.bot_token`, `.allowed_users` | the bot was built with that token, and the allow-list was copied into a set when it was built. Notification *delivery* does follow a reload, so after changing `allowed_users` the two can disagree until a restart. `dm_policy` and `stream_mode` are read per update and do follow a reload (see the table above) |
| `mcp_endpoint.*` | fixed when the app was created |
| `auth.jwt_secret` | half-hot: the web gateway reads it per request, so its own auth follows a reload, but the MCP endpoint captured it when the app was mounted and keeps checking `/mcp/v1` against the old secret. Rotating it moves one and not the other until a restart |
| `workflows.enabled`, `workflows.review_loop.enabled` | each service is created at startup and only when its flag is on. Turning one **off** does not stop the service already running, and turning it **on** creates nothing for a reload to reach |
| `workflows.poll_interval_seconds`, `workflows.review_loop.reconcile_interval_seconds` | both loops were handed their interval when they started. Everything else under `workflows.*` is read per use (see the table above) |
| `proxy.*` | the proxy process is started at startup, so turning it on, turning it off or moving its port needs one. The backend does read the proxy host and port per session, so those can point somewhere nothing is listening until you restart |
| Turning `ollama.enabled` **on** while the proxy is not already running | Ollama routes through the proxy as its translation layer, and that process only starts at startup. With the proxy already up, this is read per use and follows a reload |
| Turning `workspace_sync.enabled` or `retention.enabled` **on** | their loops are only created at startup, so there is nothing running to see the flag change. Turning either **off** is hot |
| `external_agents.enabled` (**both** directions) and adding the **first** target | the sweeper is only created when the flag is on *and* at least one target is configured; with none it is never created, so a first target added later reloads to `ok` and does nothing (`POST /api/external-agents/sync` answers 503). Once it exists, adding, removing and toggling targets is hot |
| A session that is **already running** | the agent process was spawned with the options in force at the time; the new ones apply to the next session |

### From the command line

```bash
nerve reload    # apply config edits to the running daemon
```

It calls the endpoint above on this box's own gateway and prints the per-subsystem
result. It exits non-zero on a partial reload, so it can gate a deploy step rather
than only informing a human. Anything in the restart table that changed is printed
as a warning: nothing failed, but the new value is not live yet.

With no daemon running there is nothing to reload and the command says so. Config
is read fresh at startup, so `nerve start` already picks the edit up.

It authenticates the way the gateway asks to be authenticated, which depends on
`auth.jwt_secret` in the config it just read:

- **Set** → it signs a token with it. If that is not the secret the running daemon
  started with, the gateway rejects the request and only a restart resolves it.
- **Empty, unlocked** → the gateway is in dev mode and does not ask for a token
  (`require_auth` runs open), so the call goes unauthenticated.
- **Empty, locked** → refused before anything is sent. A locked gateway never takes
  the open path, so no request from that shell can be authenticated. If the secret
  comes from `${ENV_VAR}`, export it in that shell too.

`auth.password_hash` is not an alternative here. It gates the browser login, which
is what mints a token from it; `require_auth` reads `auth.jwt_secret` alone, so a
password neither makes the endpoint ask for a token nor gives the CLI one to sign.

`POST /api/config/sync` runs the same reload but scores it differently, because it
answers a different question. Its `ok` is about the *merge*: true once the merged
config is loaded and in effect, false only when the daemon could not load it, in
which case the merge applied nothing at all. A subsystem that failed *after* the
config loaded leaves `ok: true`, because the merged settings really are live, and
shows up as `applied: false` with the reason in `reload_errors`. So the same skills
failure gives `ok: false` on `/api/config/reload` and `ok: true, applied: false` on
`/api/config/sync`. **On the sync endpoint, read `applied`, not `ok`**, unless what
you want to know is specifically whether the merge took.

`applied: false` on its own does not say why, since a sync with nothing to merge
reports it too. `status` is the field to branch on:

| `status` | Meaning |
|----------|---------|
| `applied` | merged, and every subsystem took it |
| `partial` | merged and the config loaded, but some subsystem did not take it (`reload_errors`) |
| `not-applied` | merged, and the daemon could not load the merged config — nothing is in effect |
| `up-to-date` | nothing to merge; this call applied nothing |

It describes *this call*. Whether the daemon is running the revision currently on
disk is a different question, answered by `GET /api/config/sync` below.

## Setting up the config repo

The workspace *is* the config repo: its root holds `config/` (settings, cron) and
`skills/`. To put it under review and sync it to an instance, turn the workspace
into a git repo with a GitHub remote, add CI validation, and optionally enable
lockdown.

**1. Scaffold the repo files.** From the instance, or anywhere the workspace is
checked out:

```bash
nerve config init-repo                       # scaffolds into the resolved workspace
nerve config init-repo --workspace ./ws      # or an explicit path; --dry-run to preview
```

This writes four files, never overwriting an existing one:

- `.github/workflows/validate-config.yml`: the CI check, shown below.
- `.gitignore`: keeps `config.local.yaml`, `.env`, `*.migrated`, databases and the
  like out of the shared repo. Secrets belong in the environment, referenced as
  `${ENV_VAR}`. It also excludes the agent's own runtime state — `MEMORY.md`,
  `TASK.md`, `memory/` — which every workspace has and which the instance
  rewrites for itself; the reviewed instruction files (`SOUL.md`, `IDENTITY.md`,
  `USER.md`, `AGENTS.md`, `TOOLS.md`) are tracked. **A workspace that is also a
  working directory may hold more than config**, so read `git status` before the
  `git add -A` below rather than after the push.
- `README.md`: a short explainer of the PR-based flow.
- `config/settings.yaml`: the commented portable settings scaffold, if the
  workspace has none. An instance's workspace already has one; a bare directory
  does not, and the CI check fails a repo with no `config/` at all rather than
  passing it.

**2. Create the GitHub repo and push.** The command prints these rather than
running them, since they need a repo name and auth:

```bash
cd <workspace>
git init && git add -A && git commit -m "Initial Nerve config"
gh repo create <org>/nerve-config --private --source=. --remote=origin --push
```

**3. CI validation.** The scaffolded workflow scans for committed secrets and
validates the bundle on every PR. It needs no secrets or tokens of its own, since
`ClickHouse/nerve` is public:

```yaml
name: validate-config

# Validates the Nerve config bundle on every PR so a broken change can't be
# merged and then synced onto a live instance. See docs/config.md.

on:
  pull_request:
  push:
    branches: [main]

permissions:
  contents: read

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5

      # Backstop for the one thing review is worst at spotting: a credential
      # pasted into a tracked file. Secrets belong in the environment and are
      # referenced from settings.yaml as ${ENV_VAR}. Runs before anything else
      # is fetched, so it scans this repo and nothing else, and needs no token
      # of its own. The image is used directly because the marketplace action
      # asks organizations for a license key.
      #
      # The tag is pinned because an argument renamed in a later gitleaks would
      # fail every config repo at once, with no pull request anywhere to explain
      # it. (From 8.19 this subcommand is spelled `gitleaks dir <path>`; bump the
      # tag and the args together.) A false positive on a high-entropy string is
      # silenced with a trailing `# gitleaks:allow`, or repo-wide in a
      # .gitleaks.toml.
      - name: Scan for committed secrets
        uses: docker://ghcr.io/gitleaks/gitleaks:v8.18.4
        with:
          args: detect --no-git --source=/github/workspace --redact

      # nerve is not published to PyPI, so this installs from git; the bare name
      # would fetch an unrelated project. Tracking the default branch keeps the
      # validator at or ahead of the instance, which is the safe direction for
      # --strict-keys below: a newer validator knows every key the instance
      # reads. To check against one release instead, append a ref (nerve@v1.2.3).
      #
      # `uv tool install`, not `uv pip install --system`: the runner's system
      # interpreter is externally managed (PEP 668), so --system is refused
      # outright ("The interpreter at /usr is externally managed"). Only the
      # `nerve` CLI is needed here, never an importable library, so a tool
      # install in its own environment is both the idiomatic spelling and
      # independent of what Python the runner image happens to ship.
      - uses: astral-sh/setup-uv@v6
      - run: uv tool install "nerve @ git+https://github.com/ClickHouse/nerve"

      # The config repo root IS the workspace (it holds config/ and skills/).
      #
      # --portable-only judges the tracked bundle on its own: no machine-local
      # config.yaml, and no falling back to a machine-local cron directory when
      # this repo carries no jobs. It fails outright if it opened no file under
      # config/ at all, so a layout mistake cannot pass as clean.
      # --strict-keys makes a misspelled key block the PR rather than warn.
      #
      # Unset ${ENV_VAR} secret refs are reported as info (CI has no secrets);
      # add --strict-env to require them. If any instance served by this repo is
      # locked through ${NERVE_LOCKDOWN}, add --assume-lockdown as well, or CI
      # only ever checks the unlocked view that instance never loads.
      - name: Validate config bundle
        run: nerve config validate --workspace . --portable-only --strict-keys
```

That is the file verbatim, comments included, because they explain each flag where
the person editing it will be looking. The command is the same one you can run
locally, so a local `nerve config validate --workspace . --portable-only
--strict-keys` gives the verdict CI will. Note that the *config* repo is private;
only nerve itself is public.

`init-repo` never overwrites an existing workflow, so a repo scaffolded earlier
keeps whatever it has; compare it against the file above after a nerve upgrade.

Gate plugins need nothing installed here: validation never loads them (see
[Validating Configuration](#validating-configuration)), so their imports are never
resolved in CI.

**4. Point the instance at the remote and enable sync.** The workspace is already
a git clone of the repo, so the remote and credentials come from git itself. Turn
on periodic pulls in `workspace/config/settings.yaml`:

```yaml
workspace_sync:
  enabled: true
  branch: main
  interval_minutes: 5
```

**5. Optionally lock it down.** Once secrets are in the environment as `${ENV_VAR}`
refs, set `lockdown: true` in `settings.yaml` so the instance only ever runs the
reviewed, merged remote. See [Lockdown](#lockdown-remote-only-read-only).

From here the loop is: open a PR, CI validates, review and merge, the instance
syncs and reloads. The agent proposes its own changes the same way, through the
`nerve-workspace` skill.

## Git-Backed Workspace Sync

The workspace can be a git repository whose remote is a shared **config repo**.
Config changes are proposed as PRs, reviewed and merged there; the instance pulls
the merged result and reloads, with no restart and no editing on the box.

```bash
nerve config sync                 # git pull --ff-only the workspace, then validate
nerve config sync --branch main
nerve config sync --no-validate
nerve config sync --no-strict-env # tolerate ${VAR}s your shell doesn't have
```

Enable periodic pulls in the daemon (opt-in):

```yaml
workspace_sync:
  enabled: true          # off by default
  branch: main           # empty = current tracking branch
  interval_minutes: 1    # also the upper bound on how stale a box can be
  validate: true         # validate the pulled bundle before applying
  strict_env: true       # unset required ${VAR} in the bundle blocks the merge
```

Each sync is fetch, then validate, then fast-forward merge. The fetched bundle is
validated in a throwaway git worktree and the live working tree is fast-forwarded
only if that passes, so an invalid bundle never lands on disk and there is nothing
for a later reload or restart to pick up (`POST /api/config/sync` returns 400 and
leaves the workspace untouched). A pull that changed something runs the same reload
as `nerve reload`, so see [Hot-Reload](#hot-reload) for what that covers; the
response names which subsystems took the merged change (`applied`, `reload`,
`reload_errors`). A merge whose config the daemon then refuses to load has applied
nothing and reports `ok: false`. CI on the PR is still the first line of defense.
The remote and credentials come from git itself, so configure `git remote` and auth
in the workspace as usual.

**Keep the reviewed files clean.** Sync refuses to merge while the workspace's
reviewed files have local changes: an edited or deleted tracked file, a staged
change, or an untracked file. That is `<workspace>/config/`, `<workspace>/skills/`
and the root instruction files (`AGENTS.md`, `SOUL.md`, `IDENTITY.md`, `USER.md`,
`TOOLS.md`) — the same set lockdown's write guard refuses. Validation judges a clean
checkout of the fetched commit, but the merge lands in your working tree, and
`--ff-only` only refuses when the incoming commit touches the same path. Anything
else would survive the merge unchecked, leaving a bundle on disk that is not the one
that passed. The case that matters most is an untracked `config/cron/gates/*.py`:
the daemon imports and runs gate plugins and validation never loads them, so a box
meant to run only reviewed config would be running local code. Commit, discard or
push local edits; the failure message names the paths. So a skill the agent created
at runtime, or a locally edited `SOUL.md`, blocks the merge until it is committed or
dropped. Files matched by `.gitignore` in there are warnings rather than refusals,
since the shared repo can never carry them.

**A blocked sync says so.** Left to a log line it is invisible: the instance keeps
answering normally while every later config change stops arriving, indefinitely.
So the first cycle that refuses sends a notification through the usual channels —
naming the paths and what clears them — and only that first one, not one per
cycle. Recovery sends a second, low-priority, when the merge goes through again.

The state behind those is queryable rather than log-only. `nerve doctor` reports
it, and `GET /api/config/sync` returns the daemon's record of the last cycle:

```json
{
  "enabled": true, "checked": true, "ok": false,
  "applied_rev": "a1b2c3d4...", "fetched_rev": "e5f6a7b8...",
  "blocked": true, "blocked_paths": ["?? skills/backdoor/SKILL.md"],
  "reload_errors": {}, "message": "fetched e5f6a7b8 but ...",
  "checked_at": "2026-07-30T12:00:00+00:00"
}
```

`applied_rev` is what this daemon has actually applied everywhere, which is not
the same as what is checked out: a subsystem that refused the merged config, or a
HEAD moved out of band, leaves the two different and the loop retries until they
agree. `checked: false` means this process has no answer (sync off, or no cycle
yet) rather than that all is well. `nerve doctor` runs the clean-tree check
itself, so it answers from a shell where the daemon's own record is not visible.

Sync validates more strictly than CI: an unset required `${VAR}` blocks the merge.
CI has no secrets, so it reports those as info, but the daemon does have them, and a
bundle with an unresolved required variable is one it will refuse to load on its
next restart. If a shared change adds a `${VAR}` this box legitimately does not set,
use `workspace_sync.strict_env: false` rather than letting every sync fail.
`nerve config sync` runs in your shell, which may not carry the daemon's environment
(systemd `EnvironmentFile`, docker `--env-file`), so pass `--no-strict-env` there
for a one-off. Warnings never block a merge: an unrecognized cron gate type, an
unknown key, or a skipped validation.

`workspace_sync` changes apply from the next cycle, once a reload has run. The sync
loop reads the current config object every cycle rather than a copy taken at
startup, so an edit to `branch`, `interval_minutes`, `validate` or `strict_env`
reaches it as soon as something replaces that object: a sync that merged a change,
or `POST /api/config/reload` after you edited the file yourself. Turning `enabled`
**on** still needs a restart, because the sync task is only created at startup.
Turning it off is picked up like any other value.

## Lockdown (remote-only, read-only)

Lockdown guarantees that the configuration an instance runs came from the tracked
workspace repo, and that the runtime cannot change it. The machine-local layers are
dropped, the write paths refuse, and config changes arrive only as a reviewed,
merged commit that sync pulls in.

It is a config-integrity control, not a sandbox. It raises the bar against the
agent rewriting its own configuration; it does not confine the agent. Read
[What lockdown does not cover](#what-lockdown-does-not-cover) before relying on
it.

### Before you turn it on

**Clone the config repo as the workspace first.** A locked instance takes config
changes only as a merged commit that sync pulls in, so a workspace with no git
remote has nowhere to receive one from and refuses to start. `git remote -v` in the
workspace is the check.

**Move any required secrets into the environment first.** `config.local.yaml` is
ignored when locked, so a secret that lives only there stops being read, and the
feature depending on it breaks on the next restart. Supply each one as `${ENV_VAR}`
referenced from `settings.yaml` before you lock the box. The usual ones:
`auth.jwt_secret`, `auth.password_hash`, `telegram.bot_token`,
`slack.bot_token`/`slack.app_token`,
`anthropic_api_key`/`openai_api_key`, `xmemory.api_key`.

`auth.jwt_secret` is the one to get right. A locked instance that ends up without
it does not fall back to the unauthenticated dev mode an unlocked box would — the
gateway refuses every request with a 503 and websockets are declined — so the box
comes up unusable rather than open. Note also that a `${VAR}` left unresolved
survives as its literal text, which is a perfectly usable signing key and one
published in the config repo, so check that the variable is actually set on the box.

### Turning it on

Set `lockdown: true` in the tracked `workspace/config/settings.yaml`, so the
remote is the authority:

```yaml
# workspace/config/settings.yaml
lockdown: true
```

The value may be an environment reference, `lockdown: ${NERVE_LOCKDOWN}`, so one
shared repo can serve a fleet where only some boxes are locked. Unlike every other
boolean, this one is not lenient: a value that is neither true nor false is a hard
error and the instance refuses to start, because the only default it could fall
back to is "unlocked". An empty value counts as unreadable, so
`${NERVE_LOCKDOWN}` with the variable unset or blank is refused rather than read
as off. Write `${NERVE_LOCKDOWN:-false}` when you want an explicit unlocked
default. `nerve config validate` reports the same error, so a bad value fails the
PR instead of the box.

### Better: set it in the environment

Setting the flag in `settings.yaml` alone leaves a way around it. Which
`settings.yaml` gets read is decided by `workspace:` in the machine-local
`config.yaml`, so editing that one line repoints the instance at a tree that never
mentions lockdown — the flag reads false and the machine-local layers come back,
`auth.jwt_secret` included.

Closing that means putting both values in the environment, set where the service is
defined. That is the one place neither a config edit nor the agent can reach:

```ini
# /etc/systemd/system/nerve.service
[Service]
Environment=NERVE_LOCKDOWN=1
Environment=NERVE_WORKSPACE=/srv/nerve-workspace
```

```bash
docker run -e NERVE_LOCKDOWN=1 -e NERVE_WORKSPACE=/root/nerve-workspace ...
```

Set both, not just one. `NERVE_LOCKDOWN` without `NERVE_WORKSPACE` is refused at
startup: it would lock the instance onto whatever tree `config.yaml` happens to
name, which is worse than not locking it at all. Once `NERVE_WORKSPACE` is set,
`workspace:` in `config.yaml` is ignored.

Two things to know about `NERVE_LOCKDOWN`:

- **It can only lock, never unlock.** The instance is locked if the variable says so
  *or* the tracked `settings.yaml` does. `NERVE_LOCKDOWN=false`, empty, and unset
  all mean the environment expresses no opinion; none of them force an unlock. So no
  file arriving later can undo it, and equally you cannot use the environment to
  escape a locked tracked config — unlocking takes a merged change. A value that is
  neither true nor false is refused at startup rather than read as no opinion.
- **It is the same switch as `lockdown: ${NERVE_LOCKDOWN}`.** With the variable set
  you need not mention `lockdown` in the tracked file at all, and a fleet repo that
  already writes `${NERVE_LOCKDOWN:-false}` gets the same protection, along with the
  `NERVE_WORKSPACE` requirement on the boxes where it resolves true.

### When locked

- **Config is remote-only.** Only `workspace/config/` and `${ENV_VAR}` are used.
  The machine-local `config.yaml` and `config.local.yaml` and the legacy
  `~/.nerve/cron` are ignored, and secrets come from the environment.
- **The lever is remote-owned.** Lockdown is read only from the tracked
  `settings.yaml`, so a local edit to `config.yaml` or `config.local.yaml` cannot
  unlock the instance or fake-lock it. With `NERVE_WORKSPACE` set, neither can
  repointing `workspace:`.
- **What lockdown protects is the workspace's *reviewed surface*.** That is
  `config/`, `skills/`, and the root instruction files `AGENTS.md`, `SOUL.md`,
  `IDENTITY.md`, `USER.md` and `TOOLS.md`. `config/` is the declarative half; a
  `SKILL.md` is model-invocable text with its own `allowed-tools` frontmatter that
  the skill index picks up on the next reload, and the root files are the system
  prompt the agent starts every turn from. `memory/`, `tasks/`, `TASK.md` and
  ordinary workspace files are outside it and stay writable.
- **Runtime edits to the reviewed surface are blocked.** Creating, updating,
  deleting or toggling a skill, Telegram pairing, writing a workspace file that
  lands in that surface, and other config mutations fail with a "locked" error
  (HTTP 403). Change config by opening a PR against the workspace repo and letting
  sync apply the merge. The rest of the workspace is unaffected: it is also the
  agent's working directory, and lockdown covers what the box was reviewed to run
  rather than the whole box.
- **The agent's own `Write`/`Edit` are refused across that surface.** Every
  non-interactive tool is otherwise auto-approved, so without this the ordinary way
  an agent edits a file would never meet the guards above — and the skill
  endpoints' 403 would be beside the point, since the index picks up whatever is in
  `skills/` on the next reload however it got there. The refusal names the PR flow,
  so a capable agent routes to it instead of retrying. Writes elsewhere (memory,
  task files, scratch files) are unaffected. `Bash` is not covered; see below.
- **The workspace must be a git repository with a remote.** Every local change to
  the reviewed surface is refused, so a merged commit that sync pulls in is the only
  way a config change can arrive. A workspace that is not a repository, or is one
  with no remote, has no route at all: the instance would keep the configuration it
  happens to hold, and nothing would report it. A locked instance without one
  refuses to start rather than running unlocked — removing a remote is a
  machine-local change, and a machine-local change does not unlock a box. Any remote
  counts, and `workspace_sync.enabled: false` is not part of this: sync follows the
  branch's own upstream when `workspace_sync.branch` is unset, and `nerve config
  sync` is a manual route that works with the periodic loop off.
- **The reviewed subtrees must really be in the workspace.** `<workspace>/config`
  and `<workspace>/skills` have to resolve inside `<workspace>`, and each has to be
  a real directory rather than a symlink. If one is a symlink out, nothing under it
  is part of the reviewed repo, `settings.yaml` included. If it is a symlink to a
  sibling inside the workspace, git does not descend into it, so nothing under it is
  tracked and sync reports the workspace clean whatever it holds. Either way the
  instance refuses to start. Where the workspace itself lives stays a machine-local
  decision, symlink included. The root instruction files get no such startup check:
  a locked instance whose `SOUL.md` is a symlink starts. What covers that case is
  the write guard, which resolves the link and refuses its target too, so the file
  the prompt is built from is not writable under another name.
- **`settings.yaml` must be inside that subtree.** `<workspace>/config/settings.yaml`
  has to resolve inside `<workspace>/config/`, so a symlink there cannot source the
  lockdown flag, the auth secret and the cron paths from a file no reviewer saw — an
  ordinary workspace file the agent can write included. A link to another file
  *within* the subtree is fine; both ends are tracked. The write guard refuses the
  path `config/settings.yaml` by name as well as by where it resolves, so a symlink
  cannot turn a reviewed name into an allowed write.
- **Cron cannot be pointed out of the workspace.** `cron.jobs_file`,
  `cron.system_file` and `cron.gate_plugins_dir` must resolve inside
  `<workspace>/config/`. One that does not is ignored, with a warning, in favour of
  the in-workspace default. `..`, absolute paths and symlinks out of the tree are
  all caught, because the resolved path is what is checked. This matters most for
  `gate_plugins_dir`, whose `.py` files the daemon imports: without the check, a
  pure-YAML change to a reviewed file would be enough to get on-disk code executed.
  When the in-workspace default is itself what escapes (`config/cron`, or
  `config/cron/gates` committed as a symlink, which needs no config key at all)
  there is nothing contained left to fall back to and the instance refuses to
  start.
- **Sync is stricter about local files.** [Workspace
  sync](#git-backed-workspace-sync) already refuses to merge when the reviewed
  surface has local changes; on a locked instance a `.gitignore`d file in there is
  a refusal too rather than a warning, and so is a submodule, which a validation
  checkout never initializes and a fast-forward never updates. An ignored
  `config/cron/gates/*.py` is local code the daemon runs, invisible to both the
  reviewer and the validator.

Run `nerve config validate --workspace .` before merging; it validates the locked
view when `lockdown: true`, which is where the checks above are decided. It also
names the machine-local keys the bundle strands, so a key that quietly falls back
to its default on every locked box is visible in the PR rather than at boot. The
remote check is the one that judges the checkout the command runs in rather than
the bundle: `actions/checkout` sets `origin`, but a run against an unpacked
tarball reports it missing.

**If the flag comes from the environment, add `--assume-lockdown` in CI.** With
`lockdown: ${NERVE_LOCKDOWN:-false}` the validator resolves false wherever the
variable is unset, which is every CI runner, so it checks the unlocked view and
none of the checks above have anything to fire on — the only machine to find out is
the locked one, at boot. `--assume-lockdown` forces the locked view. A plain run
warns when it sees an env-controlled flag resolve to false, so the gap is at least
visible.

**Flipping the flag needs the config reloaded.** A sync does that, and so does
`POST /api/config/reload` — both run the same reload. The write guards, gateway
authentication and the sync loop pick the change up immediately; cron jobs and gates
already built follow on the next cron reload, and anything captured at startup needs
a restart. A hand edit on the box refreshes nothing, and under lockdown it is
ignored anyway. If a sync merges a config the daemon then cannot load, it reports
`ok: false` with `apply_error` rather than claiming the change took.

### What lockdown does not cover

Lockdown is a config-integrity control, not a sandbox and not a permission system.

**The agent can still reach the files through the shell.** `Bash` is auto-approved
like every other non-interactive tool, so refusing `Write` and `Edit` closes the
path an agent takes by default and by accident, not the one it takes deliberately.
Nerve does not filter command strings — `sh -c`, redirection, `python -c` and every
editor defeat such a filter, and a filter that looks like a boundary without being
one is worse than a documented gap. If your threat model includes a deliberately
hostile agent, run the daemon as a user that cannot write the workspace repo, or
mount the reviewed subtrees read-only.

**The codex backend is weaker still.** Codex's sandbox is a mode, not a path list,
so "everything except the reviewed surface" cannot be expressed to it. Nerve
declines a codex file change landing in that surface, but only through codex's
approval request, and the shipped defaults (`sandbox: danger-full-access`,
`approval_policy: never`) never ask. Treat the `Write`/`Edit` protection above as
absent there unless you have configured approvals.

**Operator CLI commands are out of scope.** `nerve init`, `nerve migrate` and
`nerve restore` write config without consulting the flag: they are how an operator
sets a box up or repairs one, and anyone who can run them can already edit the
files directly.

## Migrating an Existing Install

Installs from before the workspace-config layout are migrated automatically and
idempotently on `nerve upgrade` and daemon start. To run it by hand:

```bash
nerve migrate --dry-run   # show what would change
nerve migrate             # apply
```

Nothing is deleted and the effective configuration does not change; values are
only relocated. Originals are renamed to `*.migrated` breadcrumbs, and an existing
breadcrumb is never overwritten.

- `config.yaml` is split rather than copied. Shareable keys go to the git-tracked
  `workspace/config/settings.yaml`; secret values go to machine-local
  `config.local.yaml`, replaced with `${ENV_VAR}` placeholders; the machine-local
  keys from the table above are rewritten into `config.yaml`, so a certificate
  path or an AWS profile handle never travels with the workspace repo. The split
  follows the table at whatever depth a key is listed, so `gateway.ssl.*` stays
  local while `gateway.host`/`port` move across. Migration prints which keys it
  kept. The `workspace` path is the exception that stays in `config.local.yaml`:
  it is written before anything is consumed, so an interrupted migration cannot
  leave the instance unable to find its workspace.
- The legacy `~/.nerve/cron/*` moves to `workspace/config/cron/*`, including any
  `prompts/` referenced by `prompt_file`.
- `config.local.yaml`, the rewritten `config.yaml` and the breadcrumb can all hold
  plaintext credentials, so all three are written `0600`. `settings.yaml` is
  written normally, so a restrictive `umask` is honored.

A value is treated as secret when any of these hold:

- The key name looks like one: `*api_key*`, `*api_hash*`, `api_id`, `*token*`,
  `*secret*`, `password*`, `jwt`, `authorization`, `bearer`, `oauth`,
  `*access_key*`, `*private_key*`, `dsn`, `pw`, `pat`, `session_string`,
  `webhook_url`. A public identifier is not a secret: `client_id` is left alone,
  `client_secret` is not.
- The value's shape is a credential whatever the key is called: `sk-…`, `ghp_…`,
  `xox…`, `user:password@host`, `?token=…`, `Bearer …`. This fires only when the
  whole value is the credential, so a category description that mentions
  `postgres://user:pass@host` stays put.
- It sits inside an `env` or `headers` block, including inside lists, where MCP
  `headers` entries live.
- It is the argument after a flag that names a credential, so both
  `args: ["--token=abc123"]` and the split `args: ["--token", "abc123"]` are
  caught. The split form is what `npx` and `uvx` MCP servers use, and the value
  on its own is unrecognizable.

If one item in a list is a secret, the whole list moves, because a merge replaces
a list rather than combining it element-wise. Migration reports this, and the copy
left in `settings.yaml` has no further effect.

Secret detection is best-effort, so **review `workspace/config/settings.yaml`
before committing it to a shared repo**, then run `nerve config validate` to
confirm the bundle is well-formed. Migration also lists any value it left in the
tracked file that still looks like a credential, for you to judge.

Migration only runs on a pre-refactor `config.yaml`, meaning one that holds
shareable settings rather than only this box's. A `config.yaml` written by
`nerve init` — or left behind by an earlier migration — holds only machine-local
keys (workspace, certificate paths, provider handles), so it is left in place even
when the workspace has no `settings.yaml`. Migration is also a no-op once
`settings.yaml` carries real keys; the `nerve init` scaffold is all comments and
counts as empty. A `config.yaml` with shareable keys sitting next to a populated
`settings.yaml` is reported, because it overrides the tracked file, and you move
those keys across by hand.

## Config Directory Resolution

`nerve` commands locate the config directory via a waterfall, so they work
from any working directory:

1. `--config-dir` / `-c` flag
2. `NERVE_CONFIG_DIR` environment variable
3. The current directory, if it contains `config.yaml` or `config.local.yaml`
4. The pointer file `~/.nerve/config_dir` (written by `nerve init` and on
   daemon start)
5. The current directory (fresh-install fallback)

`nerve doctor` reports which directory was used and how it was found.

## Core

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `workspace` | path | `~/nerve-workspace` | Path to workspace directory |
| `timezone` | string | `America/New_York` | Local timezone for scheduling |
| `lockdown` | bool | `false` | [Remote-only, read-only mode](#lockdown-remote-only-read-only). Honored only in `workspace/config/settings.yaml`; an unreadable value is an error, not a default. |
| `deployment` | string | `server` | `server` (bare metal) or `docker`. Set during `nerve init`; determines whether CLI commands run directly or proxy to `docker compose`. |

> **Note:** The _mode_ (personal vs worker) is not a config field — it's determined at `nerve init` time and expressed through which workspace templates, cron jobs, and memory categories are active. There's no `mode` key in config.

## Agent

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `agent.model` | string | `claude-opus-5` | Primary model for conversations |
| `agent.cron_model` | string | `claude-sonnet-4-6` | Model for cron jobs (cheaper) |
| `agent.models` | list | `[]` | Claude models offered in the composer's model picker. `agent.model` always leads; entries extend the list. Empty → the discovered catalog (see `agent.model_discovery`), else a built-in current-generation list (opus / sonnet / haiku); on Bedrock only configured models are offered |
| `agent.model_discovery` | bool | `true` | Ask the Anthropic Models API (`GET /v1/models`) which models the configured credentials can reach, and offer those in the picker — so a newly released model needs no config edit. Primed at gateway startup, cached in-process (6h) and refreshed in the background. Best-effort: ignored when `agent.models` is set, on Bedrock (the Bedrock client has no Models API), without an API key, or when the API is unreachable — the built-in list applies |
| `agent.model_aliases` | map | `{opus: claude-opus-5}` | Alias → model ID remapping for the CLI (emitted as `ANTHROPIC_DEFAULT_<ALIAS>_MODEL` env vars). Aliases (`opus`, `sonnet`, `haiku`, `fable`) used in Agent/Workflow tool model options, skill frontmatter, and cron overrides resolve to the mapped ID. Entries merge over the built-in `opus → claude-opus-5` default (not applied on Bedrock — set geo-prefixed IDs explicitly there); `""` unsets an alias |
| `agent.max_turns` | int | `50` | Max agentic turns per request |
| `agent.max_concurrent` | int | `32` | Max concurrent agent sessions |
| `agent.cache_ttl` | string | `"5m"` | Prompt-cache write TTL policy: `5m` (status quo), `1h` (always request the 1-hour TTL), or `auto` (per session at client-build time: sparse-cadence sessions — persistent crons, wakeup loops, spaced chats — get `1h`; dense sessions stay on `5m`). Per-cron-job override via `cache_ttl` in jobs.yaml. See `nerve/agent/cache_policy.py` |
| `agent.cache_ttl_excluded_models` | list | `[]` | Model-name substrings that never request the 1h TTL |
| `agent.agent_teams` | bool | `true` | Set `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1` for the CLI subprocess, which registers the `SendMessage` tool. The Agent tool advertises `SendMessage` for resuming a sub-agent whether or not the flag is set, so with it off the model reaches for a tool that does not exist. Nerve loads no settings files (`setting_sources=[]`), so the env dict is the flag's only route in. Teammates stay opt-in per turn and cost a full context window each; the CLI cannot restore in-process teammates when a session's client is recycled (idle timeout, restart, crash retry) |
| `agent.prompt_rewrite.enabled` | bool | `true` | Offer the first-prompt rewrite feature in the web UI (per-user toggle lives in the composer) |
| `agent.prompt_rewrite.model` | string | `""` | Model for prompt rewriting (empty = `agent.model`, the chat model) |
| `agent.prompt_rewrite.max_tokens` | int | `1024` | Max tokens for the rewritten prompt |
| `agent.prompt_rewrite.timeout_seconds` | float | `45.0` | Rewrite API call timeout |

**Prompt rewrite:** when the ✨ toggle in the composer is on, the first prompt of a new chat is rewritten by a fast model to better express intent. The result is previewed (editable) and only sent after explicit approval — the user can always send the original instead. Trivial or already-clear prompts are sent unchanged without a preview.

**Note:** The engine uses a `can_use_tool` callback (not `bypassPermissions`) so that interactive tools (`AskUserQuestion`, `ExitPlanMode`, `EnterPlanMode`) can pause mid-turn for user input. All other tools are auto-approved. See [sdk-sessions.md](sdk-sessions.md#permissions--interactive-tools) for details.

## Agent Backends (claude / codex)

Nerve can run sessions on two agent runtimes. The backend is selected per
NEW session and is **sticky**: it's stamped into `sessions.backend` at first
client build and always wins over config afterwards, so flipping the
defaults never crosses an existing conversation (or its wakeups) onto a
runtime that can't resume it. See `docs/plans/codex-backend.md`.

```yaml
agent:
  backend: claude          # claude | codex — new interactive sessions
  cron_backend: null       # null → backend; new cron/hook sessions only

codex:                     # active when a codex backend is selected
  bin_path: codex          # tested: >= 0.144.1 and < 0.145.0
  min_version: 0.144.1
  max_version: 0.145.0
  home_dir: ~/.nerve/codex # isolated CODEX_HOME (auth, config, sessions)
  model: gpt-5.6-sol
  cron_model: null         # null → model
  auth: chatgpt            # chatgpt | api_key
  api_key: null            # config.local.yaml; or api_key_env: OPENAI_API_KEY
  sandbox: danger-full-access   # read-only | workspace-write | danger-full-access
  approval_policy: never        # never | on-request | untrusted
  web_search: true
  tool_timeout_sec: 3600        # nerve MCP calls may block on ask_user
  turn_idle_timeout_seconds: null  # null → agent.cli_idle_timeout_seconds
  pricing:                      # $/1M tokens — cost is None for unlisted models
    gpt-5.6-sol: {input: 5.0, cached_input: 0.5, output: 30.0}
  extra_config: {}              # arbitrary codex -c key=value passthrough
  ultracode:                    # optional managed third-party orchestrator
    enabled: false
    auto_install: true
    repository: https://github.com/just-every/plugin-ultracode.git
    revision: 9dde0086e983413016bf62ab96ba6bb17b599fae
    version: 0.3.0+codex.20260601143116
    dashboard: false            # authenticated read-only Nerve UI
    ui: false                   # detached upstream server; keep disabled
    default_transport: exec
    max_concurrency: 2          # hard cap, even if a workflow asks for more
    default_token_budget: 250000 # default and maximum per workflow
    max_agents: 8               # lifetime worker cap per workflow
```

`codex.ultracode.dashboard` exposes run journals in Nerve's authenticated UI.
It does not start Ultracode's detached dashboard process. Keep
`codex.ultracode.ui: false`: the upstream process serves unauthenticated
mutation and execution endpoints and is not safe to expose through Nerve.

Setup for `auth: chatgpt`: run `CODEX_HOME=~/.nerve/codex codex login` once,
then `nerve codex doctor` to verify CLI version, authentication, the live
model list, protocol, and managed plugin state before flipping any backend
default. Codex sessions reach Nerve tools through a dedicated plaintext ASGI
listener bound to an ephemeral `127.0.0.1` port. Its bearer token is scoped to
the owning session, expires after eight hours, and exists only in the spawned
process environment. `mcp_endpoint.enabled` must stay on; the public gateway
mount and the loopback listener share the same authenticated MCP manager.

External/user-launched Codex uses `bearer_token_env_var = "NERVE_MCP_TOKEN"`
instead of storing a credential in TOML. Refresh it with:

```bash
export NERVE_MCP_TOKEN="$(nerve codex token)"
```

Billing follows the effective account reported by `account/read` (and preflight
flags a mismatch with `codex.auth`). With ChatGPT authentication, token counts and rate/credit events are retained,
but `cost_usd` is null: any API-price calculation is stored separately as an
`api_equivalent_estimate`. API-key sessions use `cost_basis: api_billed` when a
known price is available.

Ultracode is installed only into the isolated Nerve Codex home at the exact
configured revision. A hash-verified Nerve policy overlay hard-enforces the
configured concurrency, token, lifetime-agent, and dashboard caps even when a
workflow requests looser values. Autonomous marketplace updates and its
dashboard are off by default. Workers inherit stable MCP definitions through a Nerve wrapper,
exchange the parent credential for two-hour worker-scoped tokens, report usage
back into the parent turn, and run read-only unless a workflow explicitly asks
for a writable sandbox. `GET /api/codex/status` exposes preflight state and
non-terminal journals available for recovery.

Notes: prompt-cache TTL policy, Claude Code plugins, and Langfuse tracing are
claude-only. PDF attachments are surfaced to Codex as explicit path/context
notes rather than silently dropped. With the
default `approval_policy: never` + full-access sandbox, codex sessions
behave like claude's auto-approved tools; tightening the policy surfaces
Approve/Decline cards in the web UI.

## Gateway

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `gateway.host` | string | `0.0.0.0` | Bind address |
| `gateway.port` | int | `8900` | Port number |
| `gateway.ssl.cert` | path | - | SSL certificate path |
| `gateway.ssl.key` | path | - | SSL private key path |

### Favicon

Drop `favicon.svg`, `favicon.png` or `favicon.ico` into `workspace/config/` and
the gateway serves it at `/favicon.ico`. There is no setting: the file is tracked
config like anything else under `config/`, so a config repo carries it to every
instance that syncs, and a fleet can be told apart by its browser tabs. Nothing
is copied into the web bundle — the file is read per request, so one that arrives
by sync appears without a restart, and removing it goes back to the browser
default.

Use one. If several are present the first of `.svg`, `.png`, `.ico` wins.

A symlink is followed only while it stays inside `workspace/config/`. Git tracks
symlinks, and `/favicon.ico` is served without authentication because a browser
asks for it before anyone logs in — so a `config/favicon.png` pointing at
`/etc/shadow` would otherwise be readable by anyone who can reach the port. One
pointing at another file under `config/` is fine.

An SVG goes out under `Content-Security-Policy: default-src 'none'` — with
`img-src data:` and `style-src 'unsafe-inline'` so ordinary icons still render —
and every format gets `X-Content-Type-Options: nosniff`. Navigating straight to
`/favicon.ico` makes an SVG a same-origin *document* rather than an image, and a
`<script>` inside one would otherwise run there and could read the session token
out of `localStorage`. Reviewing an icon for embedded script is not a reasonable
thing to ask, so the policy does it instead.

An agent can propose an SVG through `propose_config_change`, since proposals
carry text. A `.png` or `.ico` has to be committed by a human.

## Telegram

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `telegram.enabled` | bool | `true` | Enable Telegram bot |
| `telegram.bot_token` | string | - | Bot token from @BotFather |
| `telegram.dm_policy` | string | `pairing` | `pairing` (allowlist + one-time pairing codes) or `open` (anyone — dangerous) |
| `telegram.allowed_users` | list[int] | `[]` | User IDs allowed to use the bot in private and group chats |
| `telegram.stream_mode` | string | `partial` | `partial` (edit msgs) or `full` |

### Pairing

With `dm_policy: pairing` (the default), the bot only talks to users in
`allowed_users` and rejects everyone else. To authorize a user without
editing config files:

1. Run `nerve pair` on the server — it prints a one-time 6-digit code
   (valid 1 hour). On a fresh install with no `allowed_users`, a code is
   also generated automatically at startup and printed to the log.
2. In a private chat, send the bot `/pair <code>` from the Telegram account to
   authorize.
3. The user ID is appended to `telegram.allowed_users` in
   `config.local.yaml` and takes effect immediately.

An unauthorized `/start` gets a reply with the sender's numeric ID and pairing
instructions (rate-limited). Other unauthorized messages cannot start a live
channel turn. Matching group messages may still go to the independent channel
source.

### Channel source

Group traffic can feed the source inbox, where `poll_source`, `read_source`,
and the `messages` cron gate read it. Nothing here starts an agent turn. It
is a grant of its own, independent of `allowed_users`: see
[sources.md](sources.md#chat-channels-slack-telegram) for the routing rules
and the threat model.

```yaml
telegram:
  source:
    enabled: true
    allow_chats: ["-1001234567890"]
    deny_senders: ["12345"]
    schedule: "*/5 * * * *"
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `telegram.source.enabled` | bool | `false` | Feed the inbox from watched chats |
| `telegram.source.allow_chats` | list[str] | `[]` | Chats to collect. **Empty means none** |
| `telegram.source.deny_chats` | list[str] | `[]` | Never collect these |
| `telegram.source.allow_senders` | list[str] | `[]` | Restrict to these senders |
| `telegram.source.deny_senders` | list[str] | `[]` | Skip these senders |
| `telegram.source.include_handled_messages` | bool | `false` | Also collect messages accepted for live routing |
| `telegram.source.schedule` | cron/interval | `*/5 * * * *` | Drain cadence |
| `telegram.source.batch_size` | int | `50` | Records per drain |
| `telegram.source.condense` | bool | `false` | LLM-condense long messages |
| `telegram.source.max_stored_messages` | int | `10000` | Buffer trim target for all Telegram chats together; checked every 100 writes |

Reaches the inbox as the source `telegram:observed` — distinct from the
`telegram` sync source, which is the Telethon pull from your *user* account.
The keys say *chats* rather than *channels* because on Telegram a channel is
a specific entity type distinct from a group.

**Setup: the bot must receive group messages.** With privacy mode enabled, a
non-admin bot does not receive ordinary group messages. Make it an admin or
disable privacy mode with BotFather (`/setprivacy` → Disable). See Telegram's
[privacy-mode documentation](https://core.telegram.org/bots/faq#what-messages-will-my-bot-get).

**What the lists match.** Only numeric IDs may grant.

| List | Matches | Example |
|---|---|---|
| `allow_chats` | numeric chat ID **only** | `-1001234567890` |
| `deny_chats` | chat ID, `@username`, or title | `-1001234567890`, `ops-room` |
| `allow_senders` | numeric user ID **only** | `42` |
| `deny_senders` | user ID, `@username`, or profile name | `42`, `mallory` |

A group's title is set by whoever runs it and a `@username` is claimable and
movable, so treating either as grantable would let anyone create a group
named `ops-room` and walk into a grant meant for someone else's. Both stay
deny-eligible, where a spoofable name can only subtract access.

**Never collected:** private chats, the bot's own messages, other bots, and
`/pair` commands.

## Slack

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `slack.enabled` | bool | see below | Enable Slack |
| `slack.bot_token` | string | - | Bot User OAuth Token (`xoxb-…`) |
| `slack.app_token` | string | - | App-Level Token for Socket Mode (`xapp-…`) |
| `slack.allow_users` | list[str] | `[]` | Allowed senders |
| `slack.deny_users` | list[str] | `[]` | Blocked senders |
| `slack.allow_direct_messages` | bool | `false` | Allow DMs; sender rules still apply |
| `slack.allow_channels` | list[str] | `[]` | Allowed shared conversations |
| `slack.deny_channels` | list[str] | `[]` | Blocked shared conversations |
| `slack.stream_mode` | string | `partial` | `partial` (edit one message) or `full` |
| `slack.commands` | list[str] | see below | Enabled `/nerve` subcommands |

Put both tokens in `config.local.yaml`. Reloading can start or stop Slack, and
same-workspace token changes reconnect with rollback on failure. Credentials
for another workspace require a restart. Guardrails, commands, and message
behavior apply to the next event.

Slack runs when `enabled: true`. If the key is omitted, it runs only when both
tokens are present; under lockdown, `enabled: true` is always required. An
explicit `true` also makes `nerve doctor` report missing tokens.

### Setting up the Slack app

Socket Mode means the bot dials out to Slack, so Nerve needs no public URL.

1. Create an app at <https://api.slack.com/apps> from the manifest below.
2. Under **Basic Information → App-Level Tokens**, create a
   `connections:write` token for `slack.app_token` (`xapp-…`).
3. Install the app and copy its Bot User OAuth Token to `slack.bot_token`
   (`xoxb-…`).
4. Under **App Home → Show Tabs**, enable **Messages Tab** and **Allow users
   to send Slash commands and messages**. The manifest cannot set this; without
   it DMs are read-only.
5. Configure at least one access grant (see below), then restart Nerve.

```yaml
display_information:
  name: Nerve
features:
  bot_user:
    display_name: Nerve
    always_online: true
  slash_commands:
    - command: /nerve
      description: Control the Nerve agent
      usage_hint: sessions | new | stop | star | reply | doctor
oauth_config:
  scopes:
    bot:
      - app_mentions:read
      - channels:history
      - channels:read
      - chat:write
      - commands
      - files:read
      - files:write
      - groups:history
      - groups:read
      - im:history
      - im:read
      - mpim:history
      - mpim:read
      - reactions:read
      - reactions:write
      - users:read
      - users:read.email
settings:
  event_subscriptions:
    bot_events:
      - app_mention
      - message.channels
      - message.groups
      - message.im
      - message.mpim
      - reaction_added
  interactivity:
    is_enabled: true
  socket_mode_enabled: true
```

Rules that use only raw IDs do not need `users:read`, `users:read.email`, or
`channels:read`. Names need the corresponding read scope; email rules need
`users:read.email`. If Slack omits identity data needed by a rule, Nerve
refuses the message and logs why.

### Guardrails

Patterns match Slack IDs (`U0123ABC`, `C0456DEF`), handles, emails, or
channel names. Matching is case-insensitive and supports globs. Use raw IDs
to avoid name lookups.

A member edits their own display name and full name, so an `allow_users`
rule never grants on those. Write user grants against the member ID, the
handle, or the email. `deny_users` does match display and full names,
because refusing on more names than a grant may rest on is always safe. A
grant that matches only a self-set name is refused and logged, so the rule
does not fail silently.

```yaml
slack:
  allow_users: ["U0123ABC", "alex.soffronow"]
  deny_users: ["*-bot"]
  allow_direct_messages: true
  allow_channels: ["eng-*"]
  deny_channels: ["*-social"]
```

- Deny rules always win.
- A non-empty allow list restricts that dimension; an empty one allows
  anything not denied.
- Sender and conversation rules are independent. `allow_users` alone permits
  those users in any non-denied shared channel; `allow_channels` alone permits
  any non-denied user in those channels.
- DMs also require `allow_direct_messages: true`. With no `allow_users`, that
  permits any non-denied member who can DM the bot.
- With no `allow_users`, `allow_channels`, or DM grant, Nerve refuses everyone.
  Deny rules alone never enable access.
- If a required name lookup fails or omits data, Nerve refuses the message.

### Channel source

Channel traffic can feed the source inbox, where `poll_source`, `read_source`,
and the `messages` cron gate read it. Nothing here starts an agent turn. It is
a grant of its own, independent of the access rules above: see
[sources.md](sources.md#chat-channels-slack-telegram) for the routing rules
and the threat model.

```yaml
slack:
  source:
    enabled: true
    allow_channels: ["C0123ABCD", "eng-*"]
    deny_channels: ["*-social"]
    deny_senders: ["*-bot"]
    schedule: "*/5 * * * *"     # how often the buffer drains into the inbox
    batch_size: 50
    max_stored_messages: 10000  # all Slack channels together
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `slack.source.enabled` | bool | `false` | Feed the inbox from watched channels |
| `slack.source.allow_channels` | list[str] | `[]` | Channels to collect. **Empty means none** |
| `slack.source.deny_channels` | list[str] | `[]` | Never collect these |
| `slack.source.allow_senders` | list[str] | `[]` | Restrict to these senders |
| `slack.source.deny_senders` | list[str] | `[]` | Skip these senders |
| `slack.source.include_handled_messages` | bool | `false` | Also collect messages accepted for live routing |
| `slack.source.schedule` | cron/interval | `*/5 * * * *` | Drain cadence |
| `slack.source.batch_size` | int | `50` | Records per drain |
| `slack.source.condense` | bool | `false` | LLM-condense long messages |
| `slack.source.max_stored_messages` | int | `10000` | Buffer trim target for all Slack channels together; checked every 100 writes |

Reaches the inbox as the source `slack:observed`.

**What the lists match.** Case-insensitive, `*` / `?` globs, and a leading `#`
or `@` is stripped so a name pasted from Slack works as-is. Deny always wins;
a non-empty allow list must match.

| List | Matches | Example |
|---|---|---|
| `allow_channels` / `deny_channels` | channel ID, or channel name | `C0123ABCD`, `eng-backend`, `eng-*` |
| `allow_senders` | member ID, handle, or email | `U0456DEFG`, `alice`, `*@example.com` |
| `deny_senders` | same, **plus** display and real names | `*-bot`, `Alice Smith` |

A display name is edited by its owner, so it can only ever deny — the handle
and email are workspace-assigned, so they may grant.

**Never collected:** DMs, group DMs, the bot's own posts, other apps, and
join/leave noise. A group DM arrives as `channel_type="mpim"` on a `G` id, and
a `G` whose type cannot be established is skipped rather than guessed at.

**Prefer IDs for a busy channel.** Names and globs use cached Slack API lookups
(`conversations.info` or `users.info`). An ID skips the lookup, but only in
uppercase: `beckyjones` is a legal handle and `c0456def` a legal channel name,
so a lowercase pattern is resolved rather than assumed. It still matches. It
just costs the lookup. A channel the grant does not name is refused before any
sender is resolved.

### Message behavior

- In an allowed DM, the bot answers every message.
- In a shared channel, it answers mentions and threads where it already has a
  session.
- Each shared-channel thread has its own session and all replies stay there;
  shared channels never have a channel-wide session.
- Messages another app wrote itself are ignored, so two agents in one channel
  cannot answer each other without end. A person posting through an
  integration still reaches the agent, because they keep their own user ID.
- `/nerve` responses are ephemeral.
- `stream_mode: partial` posts a `⏳` placeholder and edits it as tokens and
  tool labels arrive, then posts the finished reply as a new message and
  deletes the placeholder. It is a new message because Slack does not notify
  on an edit. `full` skips all of that and posts once at the end.
- Prefer `full` in a busy shared channel, where the placeholder is visible to
  everyone, or under rate-limit pressure: Slack meters `chat.update` per
  conversation, so every thread streaming in one channel shares a single edit
  every 1.2s and the rest are dropped. Only replies to inbound messages
  stream; cron output and notifications are a single post either way.

### Commands

`slack.commands` controls slash commands only; chat and notification buttons
are unaffected.

```yaml
slack:
  commands: []                    # disable /nerve
  commands: [reply]               # enable only reply
  commands: [sessions, new, stop] # enable this exact set
  commands: [all]                 # enable every subcommand
```

Omitting the key enables `new`, `stop`, `star`, `unstar`, and `reply`.
`doctor`, `restart`, and `sessions` are opt-in: the first two expose host
operations, while `sessions` can reach sessions outside Slack and is not
scoped to the caller. Unknown command names are ignored with a warning;
`/nerve help` shows the enabled set.

Slack slash-command payloads have no thread ID. In a shared channel, `new` and
`sessions` therefore refuse, while `stop`, `star`, and `unstar` select among
that channel's active thread sessions. Commands work normally in DMs.

### Notifications

Question and approval cards go to Slack by default.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `notifications.channels` | list | `[web, telegram, slack]` | Where `notify`, `ask_user`, and `propose_action` deliver |
| `notifications.slack_channel_id` | string | `""` | Target channel ID; defaults to the first literal ID in `slack.allow_channels` |

`notifications.channels` replaces the default rather than adding to it, so
list every transport you want. A name nothing delivers to is skipped with a
warning. Slack in the list costs nothing while Slack is off.

Names and globs are not resolved for the `slack_channel_id` fallback. Without
a literal channel ID, delivery is skipped with a warning.

### Outbound Slack message tool

The `send_channel_message` tool posts to a given conversation. That makes it usable
from a cron run with no conversation attached.

It is **off by default**: `slack.allow_outbound: true` enables the capability,
and `slack.allow_channels` then bounds where it may go. The target must be a literal conversation id.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `slack.allow_outbound` | bool | `false` | Let an agent post to a conversation it names |

While no channel is both enabled and outbound-enabled, the tool is not offered
to the agent at all. The check reads live config per session, so a reload adds
or removes it.

Three differences from the inbound policy:

- **`slack.allow_users` has no effect.** It says who may drive the
  agent, not where the agent may broadcast.
- **Unsolicited DMs are refused**, even with `slack.allow_direct_messages`.
  **Group DMs count.** A `G` id is either a legacy private channel or a
  multi-person DM, and only `conversations.info` can tell them apart, so a
  `G` target costs one cached lookup; a lookup that cannot answer is refused
  rather than guessed.

## Quiet Hours

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `quiet_start` | string | `02:00` | HH:MM — start of quiet period (local timezone) |
| `quiet_end` | string | `08:00` | HH:MM — end of quiet period (local timezone) |

## Sources (sync)

Most sources pull data from external services on a schedule. See
[sources.md](sources.md) for full details.

Chat channels also feed the inbox, but are configured on the channel — see
`slack.source.*` and `telegram.source.*` above. For each message delivered by
the channel transport, matching messages are written to a local buffer.
`ChannelSource` drains that buffer; it does not poll the chat service. Its
`schedule` is a drain cadence, not a poll interval.

**Common pull-source fields** (under `sync.<source>`; availability varies):

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sync.<source>.enabled` | bool | `true` | Enable/disable this source |
| `sync.<source>.schedule` | cron/interval | varies | Fetch frequency (crontab or interval like `2h`) |
| `sync.<source>.processor` | string | `agent` | `agent` (LLM review), `memorize` (direct memU), `notify` (channel forward), `none` |
| `sync.<source>.batch_size` | int | `50` | Max records per fetch cycle |
| `sync.<source>.prompt_hint` | string | `""` | Extra instructions for the agent prompt |
| `sync.<source>.model` | string | `""` | Override model (empty = `agent.cron_model`) |
| `sync.<source>.condense` | bool | `false` | LLM-condense long records via `memory.fast_model` before processing |

**Telegram-specific:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sync.telegram.api_id` | int | - | Telethon API ID (from my.telegram.org) |
| `sync.telegram.api_hash` | string | - | Telethon API hash |
| `sync.telegram.schedule` | cron | `*/5 * * * *` | Fetch frequency |
| `sync.telegram.exclude_chats` | list[int] | `[]` | Chat IDs to skip |
| `sync.telegram.monitored_folders` | list | `[]` | Telegram folder names to filter |

**Gmail-specific:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sync.gmail.accounts` | list | `[]` | Gmail accounts to sync |
| `sync.gmail.schedule` | cron | `*/15 * * * *` | Fetch frequency |
| `sync.gmail.keyring_password` | string | - | gog keyring password |

**IMAP-specific** (see [sources.md](sources.md) for the image pass):

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sync.imap.enabled` | bool | `false` | Enable IMAP mailboxes |
| `sync.imap.accounts` | list | `[]` | Mailboxes: `host`, `username`, `label`, `port`, `mailbox` |
| `sync.imap.passwords` | dict | `{}` | Password per username — keep in `config.local.yaml` |
| `sync.imap.schedule` | cron | `*/30 * * * *` | Fetch frequency |
| `sync.imap.initial_lookback_days` | int | `1` | `SINCE` window on first run |
| `sync.imap.match.sender_contains` | list | `[]` | Substrings matched against `From:` |
| `sync.imap.match.attachment_contains` | list | `[]` | Substrings matched against an image's Content-ID / filename |
| `sync.imap.match.only_matched` | bool | `false` | Drop everything that did not match |
| `sync.imap.vision.enabled` | bool | `false` | Run a multimodal pass over a matched message's image |
| `sync.imap.vision.model` | string | *(empty)* | Falls back to `memory.fast_model` |
| `sync.imap.vision.prompt` | string | *(empty)* | What to ask about the image — required when vision is enabled |
| `sync.imap.vision.answer_key` | string | *(empty)* | Label the prompt asks the model to emit; empty = first non-empty line |

**GitHub-specific:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sync.github.schedule` | cron | `*/15 * * * *` | Fetch frequency |

## Memory (memU)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `memory.recall_model` | string | `claude-sonnet-4-6` | Model for recall routing |
| `memory.memorize_model` | string | `claude-sonnet-4-6` | Model for extraction & preprocessing |
| `memory.fast_model` | string | `claude-haiku-4-5-20251001` | Model for categorization, date resolution, knowledge filtering |
| `memory.embed_model` | string | *(empty)* | Embedding model (only used when `openai_api_key` is set, e.g. `text-embedding-3-small`) |
| `memory.semantic_dedup_threshold` | float | `0.85` | Cosine similarity threshold for semantic deduplication (0 to disable) |
| `memory.knowledge_filter` | bool | `false` | Post-extraction LLM filter that deletes generic knowledge items (extra Haiku API call per memorize) |
| `memory.categories` | list | `[]` | Seed categories — each entry has `name` and `description` fields. Used for semantic routing when memorizing and recalling facts. `nerve init` populates mode-appropriate defaults (personal: relationships, finances, health, etc.; worker: patterns, procedures, approvals, etc.). |

## xmemory (optional, alongside memU)

[xmemory.ai](https://xmemory.ai) is an optional schema-backed memory layer that runs **alongside** memU — it never replaces it. Activated only when both `xmemory.api_key` and `xmemory.instance_id` are set (put them in `config.local.yaml`); otherwise it is completely inert (no SDK calls, zero overhead). The instance and its schema are created out of band on xmemory's side.

When active:
- The `memorize` tool **dual-writes**: memU (as always) plus an async `write_async` to xmemory. Failures on the xmemory side never fail the tool.
- `memory_recall` appends xmemory's read result (serialized as JSON) to memU's N items, run concurrently so the dual lookup is one round-trip. Read behavior is controlled via `xmemory.read_mode` (defaults to `single-answer`).
- The memorization **sweep** (session-close / cron) stays memU-only by default — it does not go through the `memorize` tool handler. Opting in via `xmemory.index_conversations` mirrors each swept message window to xmemory as a text-only transcript (role + content only — no thinking, no tool blocks/results), chunked and written with fast extraction, fire-and-forget alongside the memU pass.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `xmemory.api_key` | string | *(empty)* | xmemory bearer token (invite-only). Secret → `config.local.yaml`. |
| `xmemory.instance_id` | string | *(empty)* | The xmemory instance to bind. Both this and `api_key` are required to activate. |
| `xmemory.api_url` | string | `https://api.xmemory.ai` | API base URL. |
| `xmemory.extraction_logic` | string | `deep` | Write extraction mode for `memorize`-tool facts: `deep` (accurate) or `fast` (high-volume). Sweep transcripts always use `fast`. |
| `xmemory.read_mode` | string | `single-answer` | Read mode for recall, whose result is appended as JSON: `single-answer` (synthesized answer envelope), `raw-tables` (table columns + rows), or `xresponse` (objects + relations). |
| `xmemory.timeout` | float | `60.0` | Per-request timeout in seconds. |
| `xmemory.index_conversations` | bool | `false` | Mirror the memorization sweep's session transcripts (text only) to xmemory. Best-effort: a failed write is logged, never retried (the sweep watermark is memU's). Off by default — full transcripts leave the machine only when explicitly enabled. |

## Docker

Configuration for Docker deployment. Only relevant when `deployment: docker`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `docker.extra_mounts` | list[string] | `[]` | Additional host:container mount pairs to add to `docker-compose.yml`. Example: `["~/code:/code", "~/projects:/projects"]` |

The core Docker mounts (source code, `~/.nerve`, workspace) are always included. GitHub CLI (`~/.config/gh`) and Gmail CLI (`~/.config/gog`) auth directories are mounted automatically if they exist on the host.

## MCP Servers

External MCP servers can be added via config without code changes or restarts. The agent picks up new servers on the next session creation, or immediately via the "Reload" button in the UI / `mcp_reload` tool.

Config uses a **dict format** so `_deep_merge` correctly overlays secrets from `config.local.yaml`:

```yaml
# config.yaml — server definitions
mcp_servers:
  filesystem:
    type: stdio
    command: npx
    args: ["-y", "@modelcontextprotocol/server-filesystem", "/data"]

  remote-api:
    type: http
    url: https://mcp.example.com/v1
```

```yaml
# config.local.yaml — secrets merge on top
mcp_servers:
  remote-api:
    headers:
      Authorization: "Bearer sk-secret-token"
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mcp_servers.<name>.type` | string | `stdio` | Transport: `stdio`, `sse`, or `http` |
| `mcp_servers.<name>.enabled` | bool | `true` | Enable/disable this server |
| `mcp_servers.<name>.command` | string | - | Command to run (stdio only) |
| `mcp_servers.<name>.args` | list | `[]` | Command arguments (stdio only) |
| `mcp_servers.<name>.env` | dict | `{}` | Environment variables (stdio only) |
| `mcp_servers.<name>.url` | string | - | Server URL (sse/http only) |
| `mcp_servers.<name>.headers` | dict | `{}` | HTTP headers (sse/http only) |

The built-in `nerve` server (SDK type, in-process) is always present and cannot be overridden.

### Claude Code Plugins

Nerve automatically discovers MCP servers from Claude Code's enabled plugins. Any plugin enabled in `~/.claude/settings.json` is loaded via the SDK's `--plugin-dir` flag, so the CLI handles OAuth, credentials, and plugin lifecycle natively.

- **No config needed** — just enable a plugin in Claude Code and restart Nerve.
- **OAuth works** — the CLI uses cached tokens from `~/.claude/.credentials.json`.
- **Auto-registered in UI** — plugin MCP servers appear in the MCP Servers page on first tool invocation (type: `plugin`).
- **No conflicts** — Nerve-configured MCPs (from `config.yaml`) and Claude Code plugin MCPs coexist; they use separate mechanisms (`--mcp-config` vs `--plugin-dir`).

## Auth

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `auth.password_hash` | string | - | bcrypt hash for login |
| `auth.jwt_secret` | string | - | JWT signing secret |

## API Keys (config.local.yaml)

| Key | Type | Description |
|-----|------|-------------|
| `anthropic_api_key` | string | Anthropic API key (agent + memU chat). Not required when proxy is enabled. |
| `openai_api_key` | string | OpenAI API key (optional — enables vector-based memory search via embeddings; without it, LLM-based recall is used) |
| `brave_search_api_key` | string | Brave Search API key (optional) |

## Proxy (CLIProxyAPI)

Optional local proxy that routes Anthropic API calls through Claude Code's OAuth authentication instead of a direct API key. When enabled, the API key is not required — all API calls go through the proxy at `localhost`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `proxy.enabled` | bool | `false` | Enable CLIProxyAPI proxy |
| `proxy.port` | int | `8317` | Proxy listen port |
| `proxy.host` | string | `127.0.0.1` | Proxy bind address |
| `proxy.binary_path` | path | `~/.nerve/bin/cli-proxy-api` | Path to CLIProxyAPI binary (auto-downloaded if missing) |
| `proxy.auth_dir` | path | `~/.nerve/cli-proxy-auth` | Directory for OAuth token storage |
| `proxy.api_key` | string | `sk-nerve-local-proxy` | Local auth key between Nerve and the proxy |
| `proxy.log_file` | path | `~/.nerve/proxy.log` | Proxy log file |

**Setup:**
```bash
# During nerve init, choose "Claude Code proxy" at the API configuration step.
# Or enable manually:
```

```yaml
# config.yaml
proxy:
  enabled: true
  port: 8317
```

```bash
# Authenticate with Claude (one-time):
~/.nerve/bin/cli-proxy-api --claude-login --no-browser \
  --config ~/.nerve/cli-proxy-config.yaml
```

The proxy binary is automatically downloaded from [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) on first start if not present. OAuth tokens are refreshed automatically.

## Sessions

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sessions.archive_after_days` | int | `30` | Auto-archive idle/stopped sessions older than this |
| `sessions.interactive_archive_after_hours` | int | `0` | Auto-close interactive (web/telegram/…) sessions after this many idle hours (`0` = disabled; opt-in). Cron/persistent sessions are unaffected. |
| `sessions.max_sessions` | int | `500` | Max active (non-archived) sessions before cleanup |
| `sessions.cron_session_mode` | string | `per_run` | `per_run` (unique session per cron run) or `reuse` (shared session per job) |
| `sessions.sidebar_page_size` | int | `50` | Rows per sidebar request: caps the conversation feed and sizes one lazy Archived/System page. `0` = unlimited (a group loads in a single request). Starred sessions are exempt and always returned in full. |

**Starred sessions are exempt from all auto-archival.** A session starred via
the star toggle (web sidebar, or the Telegram `/sessions` list / `/star`) is
never auto-closed: it is skipped by the idle cutoff and the
`archive_after_days` backstop, and is off-budget for `max_sessions` — neither
counted toward the cap nor evicted. It stays resumable until explicitly
unstarred, archived, or deleted.

## Retention

Opt-in `nerve.db` maintenance. Disabled by default. When enabled, a background
pass every `interval_hours` drops the verbose `blocks`/`thinking` JSON of old,
already-memorized messages (keeping the rendered `content`), prunes append-only
telemetry and file snapshots older than `retention_days`, and checkpoints the
WAL. This frees space inside the database but does not shrink the file on disk;
run `nerve db vacuum` once (with the daemon stopped) to reclaim it.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `retention.enabled` | bool | `false` | Master switch for the background retention pass |
| `retention.retention_full_days` | int | `30` | Compact `blocks`/`thinking` of memorized messages older than this |
| `retention.retention_days` | int | `90` | Prune telemetry and file snapshots older than this |
| `retention.interval_hours` | int | `24` | How often the background pass runs |

Manual commands (run regardless of `enabled`):

- `nerve db prune [--dry-run]` runs one pass immediately. `--dry-run` reports
  what would change without mutating.
- `nerve db vacuum` rewrites the file to reclaim freed pages. It takes a write
  lock, so stop the daemon first.

## Cron

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `cron.system_file` | path | `<workspace>/config/cron/system.yaml` (falls back to `~/.nerve/cron/system.yaml` for un-migrated installs) | System cron jobs (managed by `nerve init`) |
| `cron.jobs_file` | path | `<workspace>/config/cron/jobs.yaml` (falls back to `~/.nerve/cron/jobs.yaml`) | User-defined custom cron jobs |
| `cron.gate_plugins_dir` | path | `<workspace>/config/cron/gates` (falls back to `~/.nerve/cron/gates`) | Drop-in custom gate plugin directory |

## Workflow Runs

Budget-capped multi-agent jobs (Claude harness `Workflow` tool or Codex
Ultracode) in dedicated tracked sessions. Nerve meters real dollar spend from
its own usage accounting, warns at `warn_fraction`, and terminates the run at
100% of budget — the kill is scoped to the run's own session/subprocess. Each
run keeps a journal under `runs_dir` (`<run-id>/{run.json,events.ndjson,result.md}`).
Runs do not survive a daemon restart: a startup recovery pass marks orphaned
active runs `failed` and notifies. See [workflow-runs.md](workflow-runs.md).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `workflows.enabled` | bool | `true` | Master switch — service, MCP tools, and API |
| `workflows.runs_dir` | path | `~/.nerve/workflow-runs` | Root for per-run journal directories |
| `workflows.poll_interval_seconds` | int | `60` | Budget monitor cadence — spend is re-metered (recorded turn costs + live in-flight estimate) every interval (min 5s) |
| `workflows.warn_fraction` | float | `0.8` | Fraction of `budget_usd` at which the one-time warning notification fires |
| `workflows.kill_grace_seconds` | int | `30` | After the graceful stop at 100% budget, how long to wait before force-discarding the session's client (kills its subprocess) |
| `workflows.max_concurrent_runs` | int | `2` | Runs dispatched concurrently; excess queues in status `pending`. Each running workflow occupies one `agent.max_concurrent` slot for its whole turn — keep this well below that limit |
| `workflows.allow_unbudgeted` | bool | `false` | Permit starting runs without `budget_usd`. Budget enforcement is the point of this surface, so off by default |
