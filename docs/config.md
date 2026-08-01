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
| `config.yaml` | `workspace`, `deployment`, `provider.aws_profile`, `gateway.ssl.*`, `proxy`, `docker`, `telegram.enabled`, `sync.gmail.accounts`, `external_agents`, `mcp_endpoint`, `workflows.runs_dir` |
| `settings.yaml` | `timezone`, `gateway.host`/`port`, `provider.type`/`aws_region` (incl. the region-scoped Bedrock model IDs), `agent.*`, `memory.*`, `sessions.*`, `sync.*`, `houseofagents.*`, quiet hours, `telegram.dm_policy`/`stream_mode` |

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
fail a shared bundle that has nothing wrong with it. Pass `--workspace` with it,
because with the machine layers dropped there is nothing left to read the
workspace location from and it falls back to the default tree. It fails outright
if it opened no file under the workspace's `config/`, so an empty directory, a
`settings.yml` typo or a `settings.yaml` left at the repo root cannot pass as
clean. Every run names the layers it read, in absolute paths:

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
      - run: uv pip install --system "nerve @ git+https://github.com/ClickHouse/nerve"
      - run: nerve config validate --workspace . --portable-only --strict-keys
      # Add --assume-lockdown if any instance served by this repo is locked and
      # the flag comes from the environment. Without it CI checks the unlocked
      # view and the lockdown checks never run. See "Lockdown" below.
```

Installing from the default branch keeps the validator at or ahead of your
instance, which is the safe direction for `--strict-keys`: a newer validator
knows every key your instance reads. A key the validator accepts but the instance
is too old to read shows up as a startup warning on the instance and in `nerve
doctor`. If a key is ever retired upstream, `--strict-keys` flags it here first;
drop the key, or install the ref you deploy (`nerve @ git+...@v1.2.3`) instead.

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
leaves the workspace untouched). A pull that changed something reloads cron and MCP
config, so the merged change takes effect immediately. CI on the PR is still the
first line of defense. The remote and credentials come from git itself, so
configure `git remote` and auth in the workspace as usual.

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

Sync validates more strictly than CI: an unset required `${VAR}` blocks the merge.
CI has no secrets, so it reports those as info, but the daemon does have them, and a
bundle with an unresolved required variable is one it will refuse to load on its
next restart. If a shared change adds a `${VAR}` this box legitimately does not set,
use `workspace_sync.strict_env: false` rather than letting every sync fail.
`nerve config sync` runs in your shell, which may not carry the daemon's environment
(systemd `EnvironmentFile`, docker `--env-file`), so pass `--no-strict-env` there
for a one-off. Warnings never block a merge: an unrecognized cron gate type, an
unknown key, or a skipped validation.

`workspace_sync` changes need a daemon restart. The sync loop reads the current
config object every cycle, so it adds no staleness of its own, but nothing refreshes
that object while the process runs. Turning `enabled` on needs a restart in any
case, because the sync task is only created at startup.

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
`POST /api/config/reload`. The write guards, gateway authentication and the sync
loop pick the change up immediately; cron jobs and gates already built follow on the
next cron reload, and anything captured at startup needs a restart. A hand edit on
the box refreshes nothing, and under lockdown it is ignored anyway. If a sync merges
a config the daemon then cannot load, it reports `ok: false` with `apply_error`
rather than claiming the change took.

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
| `agent.models` | list | `[]` | Claude models offered in the composer's model picker. `agent.model` always leads; entries extend the list. Empty → a built-in current-generation list (opus / sonnet / haiku) on the direct Anthropic API; on Bedrock only configured models are offered |
| `agent.model_aliases` | map | `{opus: claude-opus-5}` | Alias → model ID remapping for the CLI (emitted as `ANTHROPIC_DEFAULT_<ALIAS>_MODEL` env vars). Aliases (`opus`, `sonnet`, `haiku`, `fable`) used in Agent/Workflow tool model options, skill frontmatter, and cron overrides resolve to the mapped ID. Entries merge over the built-in `opus → claude-opus-5` default (not applied on Bedrock — set geo-prefixed IDs explicitly there); `""` unsets an alias |
| `agent.max_turns` | int | `50` | Max agentic turns per request |
| `agent.max_concurrent` | int | `32` | Max concurrent agent sessions |
| `agent.cache_ttl` | string | `"5m"` | Prompt-cache write TTL policy: `5m` (status quo), `1h` (always request the 1-hour TTL), or `auto` (per session at client-build time: sparse-cadence sessions — persistent crons, wakeup loops, spaced chats — get `1h`; dense sessions stay on `5m`). Per-cron-job override via `cache_ttl` in jobs.yaml. See `nerve/agent/cache_policy.py` |
| `agent.cache_ttl_excluded_models` | list | `[]` | Model-name substrings that never request the 1h TTL |
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

## Telegram

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `telegram.enabled` | bool | `true` | Enable Telegram bot |
| `telegram.bot_token` | string | - | Bot token from @BotFather |
| `telegram.dm_policy` | string | `pairing` | `pairing` (allowlist + one-time pairing codes) or `open` (anyone — dangerous) |
| `telegram.allowed_users` | list[int] | `[]` | Telegram user IDs allowed to DM the bot |
| `telegram.stream_mode` | string | `partial` | `partial` (edit msgs) or `full` |

### Pairing

With `dm_policy: pairing` (the default), the bot only talks to users in
`allowed_users` and rejects everyone else. To authorize a user without
editing config files:

1. Run `nerve pair` on the server — it prints a one-time 6-digit code
   (valid 1 hour). On a fresh install with no `allowed_users`, a code is
   also generated automatically at startup and printed to the log.
2. Send the bot `/pair <code>` from the Telegram account to authorize.
3. The user ID is appended to `telegram.allowed_users` in
   `config.local.yaml` and takes effect immediately.

An unauthorized `/start` gets a reply with the sender's numeric ID and
pairing instructions (rate-limited); all other messages from unauthorized
users are ignored.

## Quiet Hours

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `quiet_start` | string | `02:00` | HH:MM — start of quiet period (local timezone) |
| `quiet_end` | string | `08:00` | HH:MM — end of quiet period (local timezone) |

## Sources (sync)

Sources pull data from external services on a schedule. See [sources.md](sources.md) for full details.

**Common fields** (available on all sources):

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
- The memorization **sweep** (session-close / cron) stays memU-only — it does not go through the `memorize` tool handler.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `xmemory.api_key` | string | *(empty)* | xmemory bearer token (invite-only). Secret → `config.local.yaml`. |
| `xmemory.instance_id` | string | *(empty)* | The xmemory instance to bind. Both this and `api_key` are required to activate. |
| `xmemory.api_url` | string | `https://api.xmemory.ai` | API base URL. |
| `xmemory.extraction_logic` | string | `deep` | Write extraction mode: `deep` (accurate) or `fast` (high-volume). |
| `xmemory.read_mode` | string | `single-answer` | Read mode for recall, whose result is appended as JSON: `single-answer` (synthesized answer envelope), `raw-tables` (table columns + rows), or `xresponse` (objects + relations). |
| `xmemory.timeout` | float | `60.0` | Per-request timeout in seconds. |

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
