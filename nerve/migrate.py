"""One-time migration of legacy config into the git-syncable workspace subtree.

Moves an existing install from the pre-refactor layout:

    <config_dir>/config.yaml            (shareable + secrets, gitignored)
    <config_dir>/config.local.yaml      (secrets, gitignored)
    ~/.nerve/cron/{jobs,system}.yaml    (cron config)

to the workspace-centric layout:

    <workspace>/config/settings.yaml    (shareable, git-tracked — SCRUBBED)
    <config_dir>/config.yaml            (machine-local base — REWRITTEN)
    <config_dir>/config.local.yaml      (secrets, machine-local)
    <workspace>/config/cron/*           (cron config, git-tracked)

Design (confirmed with the user):

* **Auto on upgrade / daemon start**, via :func:`maybe_migrate` — idempotent,
  a no-op once migrated. Also exposed as ``nerve migrate [--dry-run]``.
* **Only a legacy monolith is migrated.** A ``config.yaml`` holding nothing but
  the keys the split layout keeps machine-local is left exactly where it is,
  however empty the workspace's ``settings.yaml`` looks — see
  :func:`_has_portable_content`.
* **Copy + keep as backup** — originals are never deleted; ``config.yaml`` and
  the legacy cron files are renamed to ``*.migrated`` breadcrumbs, and the new
  location wins. An existing breadcrumb is never overwritten.
* **Split, don't copy** — a legacy ``config.yaml`` holds both halves, so the keys
  :data:`_MACHINE_LOCAL_PATHS` names are rewritten into a fresh ``config.yaml``
  and never reach the tracked file. A certificate path, an AWS profile handle or
  a mount list is right on exactly one box; syncing it to the rest of a fleet
  points them all at something that isn't there.
* **Auto-scrub secrets** — before writing the *tracked* ``settings.yaml``, secret
  values are moved into machine-local ``config.local.yaml`` and replaced with
  ``${ENV_VAR}`` placeholders. Scrubbed: values under secret-looking keys
  (see :data:`_SECRET_KEY_RE`), values whose *shape* is a credential whatever the
  key is called (``sk-…``, ``ghp_…``, ``user:pass@host``, ``?token=…``), *every*
  value inside ``env`` / ``headers`` mappings (where arbitrarily-named secrets
  live, e.g. MCP ``Authorization`` headers), and secrets nested inside lists.
  Migration prints exactly what it moved, plus anything left behind that still
  looks credential-shaped; heuristics can't be exhaustive, so **review
  settings.yaml before committing**.

Never destructive and never raises out of :func:`maybe_migrate` (best-effort on
startup).

**Isolating a migration.** Three roots are read and written, and only two of
them are obvious from the call:

* ``config_dir`` — the first argument.
* the workspace — the ``workspace`` argument; when omitted it is resolved from
  the machine-local config files, falling back to ``paths.default_workspace()``.
* the legacy cron directory — the ``legacy_cron_dir`` argument; when omitted it
  is ``paths.cron_dir()``, i.e. under ``NERVE_HOME``.

Passing ``workspace=`` alone therefore does **not** sandbox anything: the cron
half still reads, copies and *renames* files under the real state directory.
Callers that must not touch the machine — tests, tooling, a dry run against
someone else's tree — have to pass ``legacy_cron_dir=`` as well (or set
``NERVE_HOME``).
"""

from __future__ import annotations

import logging
import os
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from nerve import paths
from nerve.config import (
    _deep_merge,
    _expand_path,
    _is_within,
    _read_yaml_mapping,
    workspace_config_dir,
    workspace_settings_file,
)
from nerve.utils.fs import atomic_write_text

logger = logging.getLogger(__name__)

# Anything holding a credential is owner-only, including the breadcrumb copy of
# the pre-migration config — it still has every secret in plaintext.
_SECRET_FILE_MODE = 0o600

# Leaf key names whose values are treated as secrets and scrubbed out of the
# tracked settings file. Matched against the *normalized* key (see
# :func:`_normalize_key`), and every alternative has to cover whole
# underscore-separated runs: a substring match would read ``max_tokens`` (a
# size) as a token and ``client_idle_timeout_minutes`` (a duration) as a client
# id. Keys ending in "_env" hold an env-var *name* — a reference, not a secret —
# and are left alone.
#
# ``client_id`` is deliberately absent: an OAuth client id is public by design,
# and scrubbing it turns a shareable value into a required ``${VAR}`` that no
# other machine can resolve. ``client_secret`` is caught by ``secret``.
_SECRET_KEY_RE = re.compile(
    r"(?:^|_)(?:"
    r"api_?key|api_?hash|api_?id|access_?key|private_?key|secret_?key"
    r"|secret|token|password|passwd|passphrase|credentials?|jwt|authorization"
    r"|bearer|oauth|pw|pat|dsn"
    r"|session_?(?:string|token|secret|id)"
    r"|webhook_?url"
    r")(?:_|$)"
)

# Numbers are credentials far less often than strings are — a config is full of
# sizes, ports and timeouts under names that brush against the list above. So a
# non-string leaf is only scrubbed when the key *ends* in one of these, which
# keeps ``telegram.api_id`` (half of a Telegram credential pair, and an int)
# while leaving ``max_tokens`` and ``default_token_budget`` in place.
_NUMERIC_SECRET_KEY_RE = re.compile(
    r"(?:^|_)(?:api_?id|app_?id|api_?key|api_?hash|access_?key|secret|token"
    r"|password|passwd|pw|pat)$"
)

# Values whose *shape* is a credential, whatever the key is called. This is the
# half a key-name list can never do: a key pasted into an ``args`` list, a token
# in a URL's query string, a password inside a DSN. The lookbehind stops the
# provider prefixes from matching mid-word (``task-management-system`` is not an
# ``sk-`` key).
_SECRET_VALUE_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:"
    r"sk-[A-Za-z0-9_-]{20,}"                        # OpenAI / Anthropic-style keys
    r"|gh[pousr]_[A-Za-z0-9]{20,}"                  # GitHub tokens
    r"|xox[abposr]-[A-Za-z0-9-]{12,}"               # Slack tokens
    r"|AKIA[0-9A-Z]{12,}"                           # AWS access key ids
    r"|eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\."   # JWTs
    r")"
)
# Shapes that only count when the *whole value* is the thing, never when it is
# prose that happens to mention one. A category description reading "connect
# with postgres://user:pass@host" is documentation, and scrubbing it would put a
# required ``${VAR}`` where a sentence used to be. So these are only consulted
# for values with no whitespace in them.
_SECRET_VALUE_OPAQUE_RE = re.compile(
    r"://[^\s/@:]+:[^\s/@]+@"                       # user:password@host
    r"|://[A-Za-z0-9]{16,}@"                        # opaque userinfo (Sentry-style DSNs)
    r"|[?&][a-z_-]*(?:token|key|secret|password|auth)=[^&\s]{8,}"  # credential in a query string
    r"|--?[a-z-]*(?:api[_-]?key|token|secret|password)=\S{8,}",    # ...or on a command line
    re.IGNORECASE,
)
_BEARER_VALUE_RE = re.compile(r"bearer\s+[A-Za-z0-9._~+/-]{12,}", re.IGNORECASE)

# A command-line flag that names a credential, as its own argv item. The
# ``--flag=value`` form above is a single string and can be matched on shape, but
# ``["--token", "tok_abc…"]`` splits the name off the value, and the value on its
# own looks like nothing: no vendor prefix, no separator, whatever length the
# vendor picked. So the *flag* is what gets recognized and the item behind it is
# scrubbed for its position rather than its content. This is the form ``npx`` and
# ``uvx`` MCP servers are configured with.
_SECRET_FLAG_RE = re.compile(
    r"^--?[a-z0-9-]*(?:api[_-]?key|token|secret|password|passwd|auth)$", re.IGNORECASE
)

# A value that is *nothing but* one ``${VAR}`` / ``${VAR:-default}`` reference is
# already scrubbed. A value that merely contains one is not: a single reference
# anywhere used to grant the whole leaf immunity, so
# ``token: "ghp_real${SUFFIX}"`` sailed into the tracked file.
_FULL_ENV_REF_RE = re.compile(r"^\$\{[^}]*\}$")

# Left-behind values that still look like a credential: one opaque run of at
# least 24 alphanumerics (hashes, base64 blobs, random keys). Deliberately does
# not match hyphenated words, which is what separates a real key from
# ``claude-haiku-4-5-20251001`` or ``inbox-processor-daily-digest``.
_OPAQUE_VALUE_RE = re.compile(r"^[A-Za-z0-9]{24,}={0,2}$")

# Whole mappings whose *every* scalar value is treated as sensitive, regardless
# of the inner key names — this is where arbitrarily-named secrets live (MCP
# server headers like ``Authorization`` and env blocks like ``GH_PAT``).
_SENSITIVE_SUBTREE_KEYS = {"env", "headers"}

# Secret-*keyed* values that are NOT actually sensitive and should stay in the
# shared settings file (dotted paths). ``proxy.api_key`` is a fixed local-loopback
# token; scrubbing it would break the proxy under lockdown (no config.local.yaml).
# ``memory.sqlite_dsn`` is a local file DSN — it matches "dsn" but carries no
# credential.
_SCRUB_EXCLUDE_PATHS = {"proxy.api_key", "memory.sqlite_dsn"}

# Config paths that ``nerve init`` deliberately keeps out of the shareable file:
# a credential handle, a certificate path, whose mailboxes this person syncs,
# which agent binaries this box has paired. Prefixes match their whole subtree.
#
# Migration uses these twice.
#
# They tell the two shapes of ``config.yaml`` apart: a legacy monolith holds
# everything — timezone, secrets, agent behaviour — and belongs in the tracked
# layer, while a post-split ``config.yaml`` holds *only* these, so if nothing else
# is in there, there is nothing to migrate (see :func:`_has_portable_content`).
#
# And they are what a monolith is split *on*: the machine half is rewritten back
# into ``config.yaml`` rather than copied into a file the docs say to commit (see
# :func:`_partition_machine_local`).
#
# Entries are scoped to the part that is genuinely local. Listing a whole subtree
# here keeps every key under it out of the tracked layer, which is why ``gateway``
# and ``provider`` were wrong as whole subtrees: a legacy monolith's bind port and
# provider type stayed in config.yaml, which lockdown does not read.
#
# The list is the ``config.yaml`` row of the layer table in ``docs/config.md``,
# and tests check it from three sides: one parses that table and fails on any
# disagreement (in either direction, and on a path that resolves to no real
# setting), one drives the wizard and fails if it routes a path here that this
# does not cover, one fails if this claims a path the wizard shares.
_MACHINE_LOCAL_PATHS = frozenset({
    "workspace",
    "deployment",
    # Certificate and key paths into this box's filesystem. host/port describe
    # the deployment and belong in the tracked layer.
    "gateway.ssl",
    # Names an entry in one machine's AWS credentials file. provider.type and
    # .aws_region are shared, and the geo-scoped model ids go with the region.
    "provider.aws_profile",
    "proxy",
    "docker",
    "telegram.enabled",
    "sync.gmail.accounts",
    # Written into config.yaml after the fact, once the wizard has paired the
    # external agents this box runs. The gateway also rewrites it on every
    # enable/disable, so it must not live in a git-tracked file.
    "external_agents",
    "mcp_endpoint",
    # Only the journal location, not the rest of the section: the budget caps
    # and cadence are policy worth reviewing and sharing, while this is one
    # box's runtime directory. Publishing it would point every instance at a
    # path that exists on exactly one of them.
    "workflows.runs_dir",
})


# Cron settings naming a *file or directory* that :func:`_migrate_cron` is about
# to move. They are dropped rather than rewritten: the whole point of the
# workspace layout is that cron config resolves to ``<workspace>/config/cron``
# (see :func:`nerve.config._resolve_cron_dir`), and a rewritten absolute path
# would be a machine-local location baked into the file the docs say to commit —
# wrong on the next box to sync it, and exactly what the config repo exists to
# avoid.
_MIGRATED_CRON_PATH_KEYS = ("jobs_file", "system_file", "gate_plugins_dir")


@dataclass
class MigrationReport:
    dry_run: bool = False
    migrated_config: bool = False
    migrated_cron: bool = False
    actions: list[str] = field(default_factory=list)
    secrets_moved: list[str] = field(default_factory=list)
    # Dotted paths withheld from the tracked file and rewritten into
    # config.yaml. Reported for the same reason the scrubbed secrets are: the
    # operator has to be able to see which half of their config went where.
    machine_local_kept: list[str] = field(default_factory=list)
    # Dotted paths left in the tracked file whose value still looks like a
    # credential. Nothing was done about them — they are for the operator to
    # look at before committing.
    suspect_values: list[str] = field(default_factory=list)
    # States worth telling the operator about that migration itself can't fix.
    warnings: list[str] = field(default_factory=list)
    # Set when migration raised partway. Whatever is in ``actions`` still
    # happened: the write order is chosen so an interruption leaves a working,
    # retryable install, not a half-written one.
    error: str | None = None

    @property
    def did_anything(self) -> bool:
        return self.migrated_config or self.migrated_cron


def _env_name(path: tuple[str, ...]) -> str:
    """Derive an ENV_VAR name from a dotted config path (auth.jwt_secret →
    AUTH_JWT_SECRET)."""
    joined = "_".join(path)
    return re.sub(r"[^A-Za-z0-9]+", "_", joined).strip("_").upper()


def _normalize_key(key) -> str:
    """``apiKey`` / ``API-KEY`` / ``api.key`` → ``api_key``.

    Config written by hand (and MCP server blocks copied from vendor docs) uses
    every spelling; normalizing once means the pattern lists only have to know
    about one.
    """
    camel_split = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(key))
    return re.sub(r"[^a-z0-9]+", "_", camel_split.lower()).strip("_")


def _value_looks_secret(value: str) -> bool:
    if _SECRET_VALUE_RE.search(value) or _BEARER_VALUE_RE.search(value):
        return True
    return not any(c.isspace() for c in value) and bool(_SECRET_VALUE_OPAQUE_RE.search(value))


def _is_cli_flag(item) -> bool:
    """True for an argv item that is a flag name rather than a value."""
    return isinstance(item, str) and item.startswith("-")


def _is_secret_leaf(key, value, path: tuple[str, ...], force: bool) -> bool:
    """True if this leaf should be moved out of the tracked file.

    ``force`` marks a leaf inside a subtree that is sensitive by definition
    (``env`` / ``headers``), where the key names are the user's own and tell us
    nothing.
    """
    # Scalars only. A bool is never a credential, and containers are walked by
    # the caller.
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return False
    if isinstance(value, str) and (not value or _FULL_ENV_REF_RE.match(value)):
        return False
    if ".".join(path) in _SCRUB_EXCLUDE_PATHS:
        return False
    norm = _normalize_key(key)
    if force:
        return True
    if norm.endswith("_env"):
        return False  # holds an env-var name, not a secret
    if isinstance(value, str):
        return bool(_SECRET_KEY_RE.search(norm)) or _value_looks_secret(value)
    return bool(_NUMERIC_SECRET_KEY_RE.search(norm))


def _scrub_secrets(
    data: dict, path: tuple[str, ...] = (), force: bool = False
) -> tuple[dict, dict, list[str]]:
    """Split a config dict into (tracked, secrets, moved_paths).

    ``tracked`` is safe to commit (secret leaf values replaced with
    ``${ENV_VAR}``). ``secrets`` is the parallel structure holding the real
    values, to be merged into config.local.yaml. ``force`` scrubs every scalar
    leaf regardless of key name (used inside sensitive subtrees like env/headers).
    """
    tracked: dict = {}
    secrets: dict = {}
    moved: list[str] = []
    for key, value in data.items():
        p = path + (str(key),)
        child_force = force or (_normalize_key(key) in _SENSITIVE_SUBTREE_KEYS)
        if isinstance(value, dict):
            t, s, m = _scrub_secrets(value, p, force=child_force)
            tracked[key] = t
            if s:
                secrets[key] = s
            moved.extend(m)
        elif isinstance(value, list):
            t_list, has_secret = [], False
            after_secret_flag = False
            for i, item in enumerate(value):
                item_path = p + (str(i),)
                if isinstance(item, dict):
                    t, s, m = _scrub_secrets(item, item_path, force=child_force)
                    t_list.append(t)
                    if s or m:
                        has_secret = True
                    moved.extend(m)
                    after_secret_flag = False
                    continue
                # A scalar list item has no key of its own, so it is judged by
                # the list's key, by its own shape, or by the item in front of
                # it — which is how ``headers: ["Authorization: Bearer …"]``,
                # ``args: ["--api-key=…"]`` and ``args: ["--token", "…"]`` are
                # all caught.
                positional = after_secret_flag and not _is_cli_flag(item)
                if _is_secret_leaf(key, item, item_path, child_force or positional):
                    t_list.append("${" + _env_name(item_path) + "}")
                    has_secret = True
                    moved.append(".".join(item_path))
                    after_secret_flag = False
                else:
                    t_list.append(item)
                    # Only a flag arms the next item. Another flag does not:
                    # ``--token --verbose`` is a malformed command line, not a
                    # token whose value is ``--verbose``.
                    after_secret_flag = isinstance(item, str) and bool(
                        _SECRET_FLAG_RE.match(item)
                    )
            tracked[key] = t_list
            if has_secret:
                # The whole real list goes to the overlay, not just the secret
                # items. Merging replaces a list rather than combining it
                # element-wise, so there is no way for the overlay to supply
                # item 3 and let the tracked file keep items 1 and 2 — it is all
                # or nothing, and the tracked copy is inert from here on.
                # :func:`_relocated_lists` surfaces that so it isn't a surprise.
                secrets[key] = value
        elif _is_secret_leaf(key, value, p, force):
            tracked[key] = "${" + _env_name(p) + "}"
            secrets[key] = value
            moved.append(".".join(p))
        else:
            tracked[key] = value
    return tracked, secrets, moved


def _suspect_values(data, path: tuple[str, ...] = ()) -> list[str]:
    """Dotted paths of tracked values that still look credential-shaped.

    Nothing is moved on this signal — it is too weak to act on and too strong to
    swallow. Reporting it is the difference between "scrubbed 3 secrets" (which
    a user reads as "and that was all of them") and a prompt to look at the four
    opaque strings the key-name rules had no opinion about.
    """
    out: list[str] = []
    items = data.items() if isinstance(data, dict) else enumerate(data)
    for key, value in items:
        p = path + (str(key),)
        if isinstance(value, (dict, list)):
            out.extend(_suspect_values(value, p))
        elif isinstance(value, str) and _OPAQUE_VALUE_RE.match(value):
            if any(c.isdigit() for c in value) and any(c.isalpha() for c in value):
                out.append(".".join(p))
    return out


def _relocated_lists(secrets: dict, path: tuple[str, ...] = ()) -> list[str]:
    """Dotted paths of lists moved to the overlay whole because one item was a
    secret. The copy left in the tracked file no longer has any effect."""
    out: list[str] = []
    for key, value in secrets.items():
        p = path + (str(key),)
        if isinstance(value, list):
            out.append(".".join(p))
        elif isinstance(value, dict):
            out.extend(_relocated_lists(value, p))
    return out


def _leaf_paths(data, path: tuple[str, ...] = ()) -> list[str]:
    """Dotted paths of every value in a config mapping. Lists count as leaves —
    the split layout routes a whole list one way or the other."""
    out: list[str] = []
    for key, value in data.items():
        p = path + (str(key),)
        if isinstance(value, dict) and value:
            out.extend(_leaf_paths(value, p))
        else:
            out.append(".".join(p))
    return out


def _is_machine_local(dotted: str) -> bool:
    return any(
        dotted == known or dotted.startswith(known + ".") for known in _MACHINE_LOCAL_PATHS
    )


def _partition_machine_local(data: dict, path: tuple[str, ...] = ()) -> tuple[dict, dict]:
    """Split a config mapping into (portable, machine_local).

    The split happens at whatever depth the path is listed at, not at the top
    level: ``gateway.ssl`` takes the ``ssl`` subtree and leaves ``gateway.host``
    and ``gateway.port`` behind. Moving whole top-level keys instead would drag
    the bind address off to the machine layer along with the certificate paths,
    and lockdown never reads that layer.

    A subtree emptied by the split is dropped rather than left behind as
    ``gateway: {}``. An empty mapping in the source is preserved as written —
    there is nothing under it to be machine-local.
    """
    portable: dict = {}
    machine: dict = {}
    for key, value in data.items():
        p = path + (str(key),)
        if _is_machine_local(".".join(p)):
            machine[key] = value
        elif isinstance(value, dict) and value:
            sub_portable, sub_machine = _partition_machine_local(value, p)
            if sub_portable:
                portable[key] = sub_portable
            if sub_machine:
                machine[key] = sub_machine
        else:
            portable[key] = value
    return portable, machine


def _has_portable_content(raw: dict) -> bool:
    """True if ``config.yaml`` holds anything the shareable layer should own.

    The positive test for "this is a legacy monolith". Emptiness of
    ``settings.yaml`` can't answer it: a workspace loses its settings file by
    being repointed, moved, emptied, or interrupted mid-init, and in every one of
    those cases the ``config.yaml`` sitting next to it is the deliberately
    machine-local half — the last thing that should be copied into a file the
    docs tell you to commit and push.
    """
    return any(not _is_machine_local(p) for p in _leaf_paths(raw))


def _resolve_workspace(config_dir: Path) -> Path:
    machine = _deep_merge(
        _read_yaml_mapping(config_dir / "config.yaml"),
        _read_yaml_mapping(config_dir / "config.local.yaml"),
    )
    return _expand_path(machine.get("workspace")) or paths.default_workspace()


def _breadcrumb_path(original: Path) -> Path:
    """A free ``*.migrated`` name next to ``original``.

    ``Path.rename`` silently overwrites on POSIX (and raises on Windows), and the
    cron half of the migration can run more than once — so a fixed suffix could
    destroy the only surviving copy of an earlier original.
    """
    candidate = original.with_name(original.name + ".migrated")
    n = 1
    while candidate.exists():
        candidate = original.with_name(f"{original.name}.migrated.{n}")
        n += 1
    return candidate


def _restrict(path: Path) -> None:
    """Make a file owner-only, best-effort (some filesystems have no modes)."""
    try:
        os.chmod(path, _SECRET_FILE_MODE)
    except OSError as e:
        logger.warning("Could not restrict permissions on %s: %s", path, e)


def migrate(
    config_dir: Path,
    workspace: Path | None = None,
    dry_run: bool = False,
    legacy_cron_dir: Path | None = None,
    report: MigrationReport | None = None,
) -> MigrationReport:
    """Perform the migration for ``config_dir``. Idempotent; safe to re-run.

    ``workspace`` defaults to the one the machine-local config names, and
    ``legacy_cron_dir`` to ``paths.cron_dir()``. Both have to be supplied to
    confine the migration to a given tree — see the module docstring.

    Pass ``report`` to keep hold of it if this raises. Migration is not one
    transaction: the config half can commit and the cron half then fail on a
    directory it cannot write, and a caller that only sees the exception has no
    way to know the files already moved.
    """
    config_dir = Path(config_dir)
    workspace = Path(workspace) if workspace is not None else _resolve_workspace(config_dir)
    legacy_cron = Path(legacy_cron_dir) if legacy_cron_dir is not None else paths.cron_dir()
    report = MigrationReport(dry_run=dry_run) if report is None else report

    _migrate_config_yaml(config_dir, workspace, legacy_cron, report)
    _migrate_cron(workspace, legacy_cron, report)
    return report


def _settings_has_content(settings: Path) -> bool:
    """True if the tracked settings file already carries configuration.

    Existence is not the test: ``nerve init`` scaffolds a comments-only
    ``settings.yaml``, which parses to an empty mapping. Treating that as
    "already migrated" made migration a permanent no-op for every install
    created after the scaffold shipped.

    A file that won't parse — or parses to something that isn't a mapping —
    counts as content: migration must never overwrite what it cannot read.
    """
    if not settings.exists():
        return False
    try:
        return bool(_read_yaml_mapping(settings, strict=True))
    except Exception:  # noqa: BLE001 — unreadable is "leave it alone"
        return True


def _drop_migrated_cron_paths(
    raw: dict, legacy_cron: Path, report: MigrationReport
) -> None:
    """Drop cron path settings that point into the directory being migrated.

    A legacy monolith that spelled its cron locations out —

    .. code-block:: yaml

        cron:
          system_file: ~/.nerve/cron/system.yaml
          jobs_file: ~/.nerve/cron/jobs.yaml

    — is naming files that :func:`_migrate_cron` moves into the workspace,
    leaving ``*.migrated`` breadcrumbs behind. Carried across unchanged, those
    keys outlive the files they name and the daemon starts with **no jobs at
    all**: nothing raises, because an absent cron file is a normal state for an
    install that has none, so the only symptom is a log line nobody is watching
    for and every scheduled job silently not running.

    Dropping them restores the default, which is the workspace cron directory
    the files just landed in. Only paths *inside* ``legacy_cron`` are dropped: a
    pointer somewhere else is a deliberate choice about a location this
    migration does not touch, and stays exactly as it was.

    Note this is decided per key rather than from whether the copy went ahead.
    When a workspace file already shadows a legacy one, :func:`_migrate_cron`
    leaves the legacy copy in place and warns that the workspace's wins — so the
    pointer is stale in that case too, just for the other reason.
    """
    cron = raw.get("cron")
    if not isinstance(cron, dict):
        return
    for key in _MIGRATED_CRON_PATH_KEYS:
        configured = _expand_path(cron.get(key))
        if configured is None or not _is_within(configured, legacy_cron):
            continue
        cron.pop(key)
        report.actions.append(
            f"dropped cron.{key} ({configured}): it named a file this migration "
            f"moved, so cron now resolves to the workspace config/cron/"
        )
    if not cron:
        raw.pop("cron", None)


def _migrate_config_yaml(
    config_dir: Path, workspace: Path, legacy_cron: Path, report: MigrationReport
) -> None:
    config_yaml = config_dir / "config.yaml"
    settings = workspace_settings_file(workspace)

    if not config_yaml.exists():
        return

    raw = _read_yaml_mapping(config_yaml)

    if _settings_has_content(settings):
        # Already on the split layout. If config.yaml still carries shareable
        # keys they silently mask the tracked file, which is also what an
        # interrupted migration leaves behind — the two states are
        # indistinguishable on disk, so say so rather than guess.
        if _has_portable_content(raw):
            report.warnings.append(
                f"{config_yaml} is still present and overrides {settings}; "
                "move any shared settings across and remove it"
            )
        return

    if not _has_portable_content(raw):
        return  # a machine-local config.yaml from the split layout, not a legacy one

    # The workspace location is machine-local — it must not live in the tracked
    # settings file (circular). It is the one machine-local key that does not go
    # back into config.yaml with the rest: config.local.yaml is written before
    # anything is consumed, so no interruption can lose it. Losing it is the only
    # unrecoverable outcome here — the instance would resolve the *default*
    # workspace and read no settings.yaml at all.
    ws_value = raw.pop("workspace", None)

    # ``lockdown`` is honored only in the tracked settings file — the machine
    # layers deliberately ignore it so that a local edit cannot unlock or
    # fake-lock an instance (see :func:`nerve.config._load_layers`). Copying it
    # across would promote a flag that has never had any effect into the one file
    # where it is authoritative, and locking drops the machine layers entirely,
    # including the config.local.yaml this migration just moved the secrets into:
    # the next start would fail on an unresolved ${VAR} it used to resolve. So it
    # is dropped, which preserves what the box does today, and reported.
    if raw.pop("lockdown", None) is not None:
        report.warnings.append(
            f"dropped 'lockdown' from {config_yaml}: it was being ignored there and "
            f"is honored only in {settings}, where it also stops the machine-local "
            "layers from being read. Set it there deliberately to lock this instance"
        )

    # Before the split, so a stale pointer cannot land in either half.
    _drop_migrated_cron_paths(raw, legacy_cron, report)

    # Split before scrubbing, so a machine-local value never reaches the tracked
    # file even as a ${VAR} placeholder. The machine half is left unscrubbed:
    # config.yaml is machine-local and gitignored, exactly like the overlay the
    # placeholders would point at, so scrubbing it would only add indirection —
    # which is why it is written owner-only below.
    portable, machine_local = _partition_machine_local(raw)

    tracked, secrets, moved = _scrub_secrets(portable)

    # Everything bound for the machine-local overlay: scrubbed secrets + the
    # workspace path (not a secret, but machine-specific).
    local_additions = dict(secrets)
    if ws_value is not None:
        local_additions["workspace"] = ws_value

    local_path = config_dir / "config.local.yaml"
    backup = _breadcrumb_path(config_yaml)
    kept = _leaf_paths(machine_local)

    report.migrated_config = True
    report.secrets_moved.extend(moved)
    report.machine_local_kept.extend(kept)
    report.suspect_values.extend(_suspect_values(tracked))
    if local_additions:
        report.actions.append(f"moved secrets + workspace path into {local_path}")
    report.actions.append(f"config.yaml → {settings} (scrubbed {len(moved)} secret(s))")
    if kept:
        report.actions.append(f"kept {len(kept)} machine-local key(s) in {config_yaml}")
    report.actions.append(f"config.yaml → {backup} (backup)")
    for dotted in _relocated_lists(secrets):
        report.warnings.append(
            f"{dotted} contained a secret, so the whole list moved to "
            f"{local_path.name} — a list can't be half-overridden, and the copy "
            "left in settings.yaml no longer has any effect"
        )

    if report.dry_run:
        return

    # Order matters, and every write is atomic (temp file + rename), so an
    # interruption at any point leaves a working install:
    #
    # 1. the local overlay first, so the tracked file never references secrets
    #    that exist nowhere on this machine;
    # 2. the tracked settings file;
    # 3. rename config.yaml away;
    # 4. write the machine-local half back to config.yaml.
    #
    # Stopping between 2 and 3 leaves config.yaml still shadowing an already
    # complete settings.yaml — the instance keeps working, and re-running is a
    # no-op. Renaming earlier would invert that: a crash before the settings
    # file existed would take every non-secret setting out of the live config
    # with no way to retry.
    #
    # Steps 3 and 4 cannot be one operation — they are the same path — so an
    # interruption between them leaves the machine half only in the breadcrumb,
    # to be copied back by hand. That window holds nothing that stops the
    # instance from loading: the workspace path went to config.local.yaml at
    # step 1.
    if local_additions:
        existing_local = _read_yaml_mapping(local_path)
        # Existing local values win — never clobber a value the operator already
        # placed there.
        merged_local = _deep_merge(local_additions, existing_local)
        atomic_write_text(
            local_path,
            "# Nerve — machine-local secrets & overrides (gitignored).\n\n"
            + yaml.safe_dump(merged_local, default_flow_style=False, sort_keys=False),
            mode=_SECRET_FILE_MODE,
        )

    atomic_write_text(
        settings,
        "# Nerve — shareable workspace configuration (migrated).\n"
        "# Secrets were moved to config.local.yaml and replaced with\n"
        "# ${ENV_VAR} placeholders. Safe to commit.\n\n"
        + yaml.safe_dump(tracked, default_flow_style=False, sort_keys=False),
        # The shareable file gets whatever mode an ordinary write would have
        # produced. Forcing it open would override a restrictive umask on a file
        # that scrubbing is not guaranteed to have emptied of credentials.
        mode=None,
    )

    # Rename the original as a breadcrumb so it no longer overrides settings.yaml.
    # It keeps every secret in plaintext — unscrubbed, unlike the file we just
    # locked down — so tighten it first, then move it.
    _restrict(config_yaml)
    config_yaml.rename(backup)

    if machine_local:
        # Owner-only, unlike the tracked file: this half is unscrubbed, and the
        # subtrees that land in it are where a paired agent's token or a local
        # service credential lives.
        atomic_write_text(
            config_yaml,
            "# Nerve — machine-local configuration (migrated).\n"
            "# The half of the old config.yaml that describes this box:\n"
            "# filesystem paths, credential handles, what this machine has\n"
            "# paired. Not for a shared repo. Shareable settings are in\n"
            "# <workspace>/config/settings.yaml, which this file overrides.\n\n"
            + yaml.safe_dump(machine_local, default_flow_style=False, sort_keys=False),
            mode=_SECRET_FILE_MODE,
        )


def _migrate_cron(workspace: Path, legacy: Path, report: MigrationReport) -> None:
    """Copy the legacy cron directory into the workspace, one file at a time.

    Per file rather than all-or-nothing, because the two sides legitimately
    overlap: ``nerve init`` writes ``system.yaml`` and an empty ``gates/`` into
    the workspace while copying only ``jobs.yaml`` across (see
    :mod:`nerve.bootstrap`). A directory-level "the workspace already has cron
    config" test therefore skipped the remainder permanently, and the legacy
    directory stops being consulted as soon as the workspace has job files (see
    :func:`nerve.config._resolve_cron_dir`) — so ``gates/*.py`` and any
    ``prompts/`` a job names by relative path were stranded where nothing reads
    them, and custom gates quietly stopped loading. Losing a gate is not a soft
    failure either: an unknown gate ``type`` takes the whole job with it.

    A file already in the workspace is never overwritten; that copy is the
    reviewed one. The exception worth reporting is a legacy *job* file that loses
    to one already in place, because it is real cron config that now runs
    nowhere and migration cannot merge two sets of jobs.
    """
    if not legacy.is_dir():
        return
    ws_cron = workspace_config_dir(workspace) / "cron"
    job_files = ("system.yaml", "jobs.yaml")

    copy: list[Path] = []
    shadowed: list[Path] = []
    for src in sorted(legacy.rglob("*")):
        if src.is_dir():
            continue
        rel = src.relative_to(legacy)
        if any(".migrated" in Path(part).suffixes for part in rel.parts):
            continue  # a breadcrumb from an earlier pass, not cron content
        if not (ws_cron / rel).exists():
            copy.append(rel)
        elif rel.parent == Path(".") and rel.name in job_files:
            shadowed.append(rel)

    # Reported whether or not anything else moves, the way a leftover config.yaml
    # is: nothing here can fix it, and the jobs in it are not running.
    for rel in shadowed:
        report.warnings.append(
            f"{legacy / rel} still holds cron jobs, but {ws_cron / rel} is already "
            "present and wins; move across what you need and remove it"
        )
    if not copy:
        return

    report.migrated_cron = True
    report.actions.append(
        f"cron {legacy}/* → {ws_cron}/ ({len(copy)} file(s), originals kept as *.migrated)"
    )

    if report.dry_run:
        return

    for rel in copy:
        dst = ws_cron / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(legacy / rel, dst)
    # Breadcrumb only the job files that actually moved, so the legacy directory
    # stops reading as "has jobs". Renaming one that lost to a workspace copy
    # would hide the operator's only copy of it.
    for name in job_files:
        if Path(name) in copy:
            (legacy / name).rename(_breadcrumb_path(legacy / name))


def is_migrated(
    config_dir: Path,
    workspace: Path | None = None,
    legacy_cron_dir: Path | None = None,
) -> bool:
    """True if there is nothing left to migrate for ``config_dir``."""
    return not migrate(
        config_dir, workspace=workspace, dry_run=True, legacy_cron_dir=legacy_cron_dir
    ).did_anything


def maybe_migrate(
    config_dir: Path,
    workspace: Path | None = None,
    legacy_cron_dir: Path | None = None,
) -> MigrationReport | None:
    """Run migration if needed. Best-effort: never raises (called on startup).

    Returns the report if migration ran, else None. A failure partway still
    returns what was applied, rather than reading as "nothing happened": the
    config half commits before the cron half runs, so an exception can leave the
    tracked settings file written, the secrets relocated and ``config.yaml``
    renamed away. Callers need that — ``nerve start`` has to reload the config it
    is holding, and ``nerve upgrade`` has to print the review prompt for a
    tracked file that now exists.
    """
    report = MigrationReport(dry_run=False)
    try:
        migrate(
            config_dir,
            workspace=workspace,
            dry_run=False,
            legacy_cron_dir=legacy_cron_dir,
            report=report,
        )
    except Exception as e:  # noqa: BLE001 — must never break upgrade/startup
        report.error = str(e)
        if report.did_anything:
            logger.warning(
                "Config migration failed partway and is half-applied (%s). Already "
                "done: %s",
                e,
                "; ".join(report.actions),
            )
        else:
            logger.warning("Config migration skipped due to error: %s", e)
    if report.did_anything:
        if not report.error:
            logger.info(
                "Migrated config to the workspace layout: %s",
                "; ".join(report.actions),
            )
        if report.suspect_values:
            logger.warning(
                "Migration left %d value(s) in the tracked settings file that look "
                "like credentials — review before committing: %s",
                len(report.suspect_values),
                ", ".join(report.suspect_values),
            )
    for warning in report.warnings:
        logger.warning("Config migration: %s", warning)
    return report
