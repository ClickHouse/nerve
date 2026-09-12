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

**Identity bootstrap.** This module also owns the configuration-aware half of
the local accounts migration (:func:`bootstrap_identity`). The schema
migration (v047) creates empty tables and reads no configuration; whether the
one bootstrapped account starts from ``auth.password_hash`` or is passwordless,
whether that hash is then copied onto the account row and taken out of the
configuration files (:func:`_migrate_config_credentials`), and whether a JWT
signing secret has to be generated because ``auth.jwt_secret`` is unset, are
configuration questions, so they are decided here, after the schema is current. It runs from the CLI through
:func:`migrate` (so ``nerve migrate --dry-run`` shows it before it happens) and
authoritatively from the gateway at startup. Under ``NERVE_HOME`` — the fourth
root — since that is where ``nerve.db`` lives.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import secrets
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from nerve import paths
from nerve.config import (
    NerveConfig,
    _deep_merge,
    _expand_path,
    _is_within,
    _read_yaml_mapping,
    load_config,
    workspace_config_dir,
    workspace_settings_file,
)
from nerve.utils.fs import atomic_write_text

if TYPE_CHECKING:
    from nerve.db import Database

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
    # The identity mode is decided on the box (or where its service is
    # defined, via NERVE_AUTH_MODE), never by the file a configuration push
    # delivers: the loader ignores it in the tracked layer outright (see
    # nerve.config._drop_tracked_auth_mode), so a legacy monolith's value has
    # to stay in config.yaml or it stops having any effect.
    "auth.mode",
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
    # Identity bootstrap (see :func:`bootstrap_identity`). Kept apart from the
    # file-layout fields above: it changes rows in nerve.db, not files, and the
    # callers that reload config after a layout migration have nothing to
    # reload for it. Phrased in the present tense on a dry run ("create ...")
    # and the past tense otherwise ("created ..."), so the CLI's "would" prefix
    # reads correctly.
    identity_actions: list[str] = field(default_factory=list)
    bootstrapped_account: bool = False
    updated_credential_source: bool = False
    generated_jwt_secret: bool = False
    # A database-held signing secret was (or would be) deleted because a
    # configured auth.jwt_secret supersedes it.
    retired_stored_secret: bool = False
    # An account's credential moved off `config` onto the row itself — the
    # configured hash copied across, not re-hashed (see
    # :func:`_migrate_config_credentials`).
    migrated_config_credential: bool = False
    # `auth.password_hash` was (or would be) removed from the machine-local
    # configuration afterwards, because nothing reads it any more.
    scrubbed_config_password: bool = False

    @property
    def did_anything(self) -> bool:
        """Whether the *file layout* migration did (or would do) anything."""
        return self.migrated_config or self.migrated_cron

    @property
    def did_bootstrap(self) -> bool:
        """Whether the identity bootstrap did (or would do) anything."""
        return (
            self.bootstrapped_account
            or self.updated_credential_source
            or self.generated_jwt_secret
            or self.retired_stored_secret
            or self.migrated_config_credential
            or self.scrubbed_config_password
        )


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
    config: NerveConfig | None = None,
) -> MigrationReport:
    """Perform the migration for ``config_dir``. Idempotent; safe to re-run.

    ``workspace`` defaults to the one the machine-local config names, and
    ``legacy_cron_dir`` to ``paths.cron_dir()``. Both have to be supplied to
    confine the migration to a given tree — see the module docstring.

    Pass ``report`` to keep hold of it if this raises. Migration is not one
    transaction: the config half can commit and the cron half then fail on a
    directory it cannot write, and a caller that only sees the exception has no
    way to know the files already moved.

    ``config`` is the loaded configuration the identity bootstrap reads; when
    omitted (or when the layout half just moved files) it is loaded from
    ``config_dir``.
    """
    config_dir = Path(config_dir)
    workspace = Path(workspace) if workspace is not None else _resolve_workspace(config_dir)
    legacy_cron = Path(legacy_cron_dir) if legacy_cron_dir is not None else paths.cron_dir()
    report = MigrationReport(dry_run=dry_run) if report is None else report

    _migrate_config_yaml(config_dir, workspace, legacy_cron, report)
    _migrate_cron(workspace, legacy_cron, report)
    _bootstrap_identity_sync(config_dir, config, report)
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
    """True if there is nothing left to migrate in the *file layout* of
    ``config_dir``. The identity bootstrap is not part of this answer: it is
    re-checked on every start and never blocks anything."""
    return not migrate(
        config_dir, workspace=workspace, dry_run=True, legacy_cron_dir=legacy_cron_dir
    ).did_anything


def maybe_migrate(
    config_dir: Path,
    workspace: Path | None = None,
    legacy_cron_dir: Path | None = None,
    config: NerveConfig | None = None,
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
            config=config,
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
    if report.identity_actions:
        logger.info("Identity bootstrap: %s", "; ".join(report.identity_actions))
    for warning in report.warnings:
        logger.warning("Config migration: %s", warning)
    return report


# --------------------------------------------------------------------------- #
#  Identity bootstrap                                                          #
# --------------------------------------------------------------------------- #
#
# Step 2 of the local multi-user migration order ("bootstrap authority"), after
# the schema expand of v047. Creates the one local owner account, the rows the
# architecture wants around it (tenant, agent, system principal, membership,
# grant — see nerve/db/accounts.py) and, when configuration supplies no
# auth.jwt_secret, the signing secret the gateway runs with.
#
# Rules it keeps:
#
# * Acts on the accounts table only while it is empty. A disabled account is a
#   row, so it is never re-created and disablement stays durable.
# * Copies no credential. A configured password stays where it is and the row
#   says credential_source='config'; nothing in any config file is rewritten,
#   which is what keeps a lockdown install with `${NERVE_PASSWORD_HASH}` working.
# * Idempotent across restarts: the same rows and ids are found each time, which
#   backup and restore rely on.


def _credential_source_for(config: NerveConfig) -> str:
    """Where the bootstrapped account's credential lives, from configuration.

    ``config`` while ``auth.password_hash`` is set — the hash is used from
    there, never copied — and ``none`` (passwordless) otherwise. A fresh
    install is the passwordless shape until a password is set. ``local`` (a
    hash on the account row) is never chosen here: only an explicit password
    change moves an account there.
    """
    return "config" if config.auth.password_hash else "none"


def _account_action(source: str, dry_run: bool) -> str:
    verb = "create" if dry_run else "created"
    return (
        f"{verb} the local owner account in nerve.db "
        f"(credential_source={source}, no username yet)"
    )


def _mirror_action(current: str, expected: str, dry_run: bool) -> str:
    why = (
        "auth.password_hash is now configured"
        if expected == "config"
        else "auth.password_hash is no longer configured"
    )
    return (
        f"set credential_source {current} → {expected} on the local owner account "
        f"({why})"
    )


# --------------------------------------------------------------------------- #
#  Off the configuration credential (3.5)                                      #
# --------------------------------------------------------------------------- #
#
# `credential_source = 'config'` was always transitional: it is the shape PR 1
# gave an upgrading install so that nothing was copied and no file was
# rewritten, which is what made PR 1 reversible. This is where every install
# leaves it, because this is the first release with somewhere to put an
# account-owned credential and a UI to manage it.
#
# The hash is already bcrypt, so this is a *copy* and not a re-hash: nobody's
# password changes, and every open session stays valid. Copy first, scrub
# second — the other order locks everybody out if the copy fails.

# Only the machine-local layers are ever rewritten. The tracked settings file is
# shared configuration (possibly under version control and possibly delivered by
# a fleet), so a value there is reported, never edited.
_MACHINE_CONFIG_FILES = ("config.yaml", "config.local.yaml")


def _declared_password_hash(path: Path) -> bool:
    """Whether this file's own ``auth`` section sets a non-empty password hash.

    Read per file rather than from the merged config: the merged view cannot say
    *where* the value came from, and the answer decides whether this is a file
    to rewrite, a file to warn about, or neither.
    """
    if not path.is_file():
        return False
    try:
        raw = _read_yaml_mapping(path)
    except Exception:  # noqa: BLE001 — a broken file fails startup elsewhere, loudly
        return False
    auth = raw.get("auth")
    return isinstance(auth, dict) and bool(auth.get("password_hash"))


def _leading_comment(text: str) -> str:
    """The file's opening comment block, so a rewrite keeps its header."""
    kept: list[str] = []
    for line in text.splitlines():
        if line.strip() and not line.lstrip().startswith("#"):
            break
        kept.append(line)
    return "\n".join(kept).rstrip() + "\n\n" if kept else ""


def _strip_password_hash(path: Path) -> None:
    """Remove ``auth.password_hash`` from a machine-local config file.

    Written through :func:`nerve.paths.write_private_text`: created
    ``O_CREAT|O_EXCL`` at 0600, the mode confirmed on the descriptor before the
    first byte, written through that descriptor and proved to still be the same
    file before it is renamed into place. The file keeps the signing secret and
    every API key the wizard collected, so it must not exist at a wider mode for
    even the moment a write-then-chmod would leave.

    Raises :class:`nerve.paths.InsecureFileError` with nothing written if that
    cannot be guaranteed; the caller reports it and leaves the value alone.
    """
    original = path.read_text(encoding="utf-8")
    raw = _read_yaml_mapping(path)
    auth = raw.get("auth")
    if not isinstance(auth, dict):  # pragma: no cover - guarded by the caller
        return
    auth.pop("password_hash", None)
    if not auth:
        # An empty mapping would be an empty overlay rather than an eraser, so
        # either shape is safe here; dropping the key keeps the file tidy.
        raw.pop("auth", None)
    paths.write_private_text(
        path,
        _leading_comment(original)
        + yaml.safe_dump(raw, default_flow_style=False, sort_keys=False),
    )


def _copy_action(count: int, dry_run: bool) -> str:
    verb = "copy" if dry_run else "copied"
    which = "the account" if count == 1 else f"{count} accounts"
    return (
        f"{verb} auth.password_hash onto {which} in nerve.db and set "
        "credential_source=local (the hash is copied, not re-hashed — no "
        "password changes)"
    )


def _scrub_password_action(scrubbed: list[Path], dry_run: bool) -> str:
    verb = "remove" if dry_run else "removed"
    where = ", ".join(str(p) for p in scrubbed)
    return f"{verb} auth.password_hash from {where} — nothing reads it any more"


def _dead_password_warning(remaining: list[Path], *, locked: bool) -> str:
    where = ", ".join(str(p) for p in remaining)
    lead = (
        "this instance is in lockdown, so its configuration is fleet-managed and "
        "was not rewritten"
        if locked
        else "this value is in shared configuration, which is not rewritten here"
    )
    return (
        f"auth.password_hash is still set in {where}, but {lead}. "
        "It no longer authenticates anybody: every account now carries its own "
        "password hash in nerve.db, and changing the configured value has no "
        "effect. Remove the key, and change passwords through the accounts "
        "screen instead."
    )


async def _migrate_config_credentials(
    db: "Database", config: NerveConfig, report: MigrationReport, *,
    dry_run: bool, pending_config_rows: int = 0,
) -> None:
    """Move every account off ``credential_source = 'config'`` (3.5).

    ``pending_config_rows`` counts accounts a *dry run* says would be created on
    ``config`` — they are not in the table yet, and a dry run that showed the
    account being created and then said nothing about its credential would
    describe a state the real run never passes through.
    """
    if not config.auth.password_hash:
        # Nothing to copy. A row still on `config` with no configured hash is
        # brought back to `none` by the mirror, which has already run.
        return
    stragglers = [
        account for account in await db.list_accounts()
        if account["credential_source"] == "config"
    ]
    if not stragglers and not pending_config_rows:
        return

    report.migrated_config_credential = True
    report.identity_actions.append(
        _copy_action(len(stragglers) + pending_config_rows, dry_run)
    )
    if not dry_run:
        for account in stragglers:
            await db.set_account_credential(
                account["id"],
                credential_source="local",
                credential=config.auth.password_hash,
            )
        logger.info(
            "Identity: %d account(s) moved off the configured password onto their "
            "own credential in nerve.db. The hash was copied, so nobody's "
            "password changed.", len(stragglers),
        )
    _retire_config_password(config, report, dry_run=dry_run)


def _retire_config_password(
    config: NerveConfig, report: MigrationReport, *, dry_run: bool,
) -> list[Path]:
    """Take the now-dead ``auth.password_hash`` out of configuration, or say why not.

    Two environments, deliberately different:

    * **ordinary install** — the key is removed from this box's own
      ``config.yaml`` / ``config.local.yaml``. Both are machine-local and
      gitignored; leaving it in either would keep a credential on disk that
      nothing reads and that an operator would later edit expecting an effect.
    * **lockdown** — nothing is written. Configuration is fleet-managed and the
      value may be an ``${ENV_VAR}`` reference the next push reasserts. A
      warning names the file and the key and says plainly that the fleet-managed
      value no longer authenticates anybody, because a fleet that rotates the
      password through configuration after this point would otherwise believe it
      had changed a credential when it had not.

    A value in the *tracked* settings file is reported the same way on any
    install: it is shared configuration, not this box's to rewrite.

    Returns the files that still declare the key afterwards — empty when the
    value is gone for good.
    """
    tracked = workspace_settings_file(config.workspace)
    remaining = [tracked] if _declared_password_hash(tracked) else []
    machine = [
        config.config_dir / name
        for name in _MACHINE_CONFIG_FILES
        if _declared_password_hash(config.config_dir / name)
    ]

    if config.lockdown:
        # Under lockdown the machine layers are not even read, so whatever is in
        # them is already inert; report every file that still carries the key.
        remaining = remaining + machine
        machine = []

    scrubbed: list[Path] = []
    for path in machine:
        if dry_run:
            scrubbed.append(path)
            continue
        try:
            _strip_password_hash(path)
        except (paths.InsecureFileError, OSError) as e:
            # Nothing was written. The credential is already on the account row,
            # so the configured value is inert either way — report it rather than
            # stopping a start over a tidy-up.
            report.warnings.append(
                f"auth.password_hash could not be removed from {path} ({e}); it no "
                "longer authenticates anybody, so remove it by hand"
            )
            logger.warning(
                "Identity: auth.password_hash could not be removed from %s (%s). "
                "It no longer authenticates anybody.", path, e,
            )
            remaining.append(path)
            continue
        scrubbed.append(path)

    if scrubbed:
        report.scrubbed_config_password = True
        report.identity_actions.append(_scrub_password_action(scrubbed, dry_run))
        # Said at the moment it becomes true, because it is the one consequence
        # an operator cannot see from the outside: older code knows nothing
        # about account rows, so it reads a config with no password_hash as a
        # passwordless install and admits every caller.
        report.warnings.append(
            f"auth.password_hash {'is about to be' if dry_run else 'has been'} "
            "removed from configuration now that the account carries its own. A "
            "downgrade to a previous release would find no configured password and "
            "treat this instance as passwordless, which admits every caller — "
            "restore a backup taken before the upgrade instead of downgrading in "
            "place."
        )
        if not dry_run and not remaining:
            # Keep this process's view of the world in step with the file it just
            # rewrote: a config object that still shows a password hash would
            # make /api/auth/status and the login route read a credential that
            # exists nowhere, until the next restart reloaded it away.
            config.auth.password_hash = ""

    if remaining:
        warning = _dead_password_warning(remaining, locked=config.lockdown)
        report.warnings.append(warning)
        if not dry_run:
            logger.warning("Identity: %s", warning)
    return remaining


def _warn_if_password_hash_is_dead(
    db_sources: list[str], report: MigrationReport, *, hash_present: bool, dry_run: bool,
) -> None:
    """Spec 1.3: say once at startup when a configured hash does nothing.

    Reached when the key is still set — a lockdown install, a value in shared
    configuration, or one an operator added back later — and no account reads it
    because every one of them carries its own credential. An hour of debugging,
    prevented by one log line.
    """
    if not hash_present or "config" in db_sources:
        return
    message = (
        "auth.password_hash is set but no account uses it: every account has its "
        "own password (credential_source=local) or none at all. Editing the "
        "configured value has no effect; change passwords on the accounts screen."
    )
    if message not in report.warnings:
        report.warnings.append(message)
    if not dry_run:
        logger.warning("Identity: %s", message)


def _secret_action(dry_run: bool) -> str:
    verb = "generate" if dry_run else "generated"
    return (
        f"{verb} a JWT signing secret into nerve.db — auth.jwt_secret is not "
        "configured (set it to override)"
    )


def _retire_action(dry_run: bool) -> str:
    verb = "retire" if dry_run else "retired"
    return (
        f"{verb} the database-held signing secret from nerve.db "
        "(auth.jwt_secret is configured and supersedes it)"
    )


# The exception lives with the policy that raises it first (Database.connect
# refuses writable/uninspectable state before opening); the bootstrap raises the
# same class for the confidentiality case. Re-exported here, with the round-2
# name kept as an alias, so callers keep importing it from this module.
from nerve.db.base import InsecureStateStorage  # noqa: E402

InsecureSecretStorage = InsecureStateStorage


def _refuse_insecure_secret_storage(db: "Database", config: NerveConfig, *, log: bool) -> bool:
    """Decide what unsecured state storage means, and stop startup when unsafe.

    Three cases, in order of severity:

    * **Writable or uninspectable** database files or directory → always fatal,
      regardless of ``auth.jwt_secret``. Another user could replace ``nerve.db``
      or rewrite the accounts, actors and history later PRs trust; a configured
      JWT protects none of that. Raises :class:`InsecureStateStorage`.
    * **Readable** database files, no configured ``auth.jwt_secret`` → fatal: a
      generated secret would sit in a file other users can read. Raises. Any
      key that *was* stored while the file was readable has already been retired
      on connect (it is compromised), so re-securing the file is necessary but,
      on its own, does not un-leak the old key — the message says so.
    * **Readable** database files, ``auth.jwt_secret`` configured → the gateway
      starts (nothing secret is kept in the database), but an error names the
      files so the operator fixes them; the database still holds accounts and
      history worth protecting.

    Returns whether the storage is unsecured (readable-with-configured-secret
    is the only non-raising unsecured case). Secured → ``False``, no output.
    """
    perms = getattr(db, "state_permissions", None)
    if perms is None or perms.secured:
        return False

    if perms.writable or perms.uninspectable:
        raise InsecureStateStorage(
            "Refusing to run against database state other users can modify or that "
            f"cannot be inspected ({'; '.join(perms.integrity_hazards)}; expected "
            f"directory 0700, files 0600). Another user could replace the database "
            f"or plant accounts, which a configured auth.jwt_secret does not "
            f"protect against. Fix the permissions — chmod 0700 {db.db_path.parent} "
            f"and chmod 0600 {db.db_path} (with its -wal/-shm sidecars) — or move "
            f"the state directory to a filesystem that supports Unix modes."
        )

    listed = "; ".join(perms.readable_hazards)
    if config.auth.jwt_secret:
        if log:
            logger.error(
                "Database files are readable by other users (%s; expected 0600). "
                "auth.jwt_secret is configured, so no signing secret is kept in the "
                "database and the gateway starts — but fix the permissions "
                "(chmod 0600 %s and its -wal/-shm sidecars): the database still holds "
                "accounts and history.",
                listed, db.db_path,
            )
        return True
    raise InsecureStateStorage(
        f"Refusing to keep a signing secret in a database other users can read "
        f"({listed}; expected 0600). Any secret stored while the file was readable "
        f"has already been retired as compromised, so re-securing the file is "
        f"necessary but does not restore the old key. Either fix the permissions "
        f"(chmod 0600 {db.db_path} and its -wal/-shm sidecars) so a fresh secret can "
        f"be generated safely, or set auth.jwt_secret in config.local.yaml or the "
        f"environment, in which case nothing secret is stored in the database. "
        f"Existing sessions must re-authenticate."
    )


async def bootstrap_identity(
    db: "Database",
    config: NerveConfig,
    *,
    report: MigrationReport | None = None,
    dry_run: bool = False,
    display_name: str | None = None,
) -> MigrationReport:
    """Create the local owner account and signing secret if they do not exist.

    The configuration-aware step of the accounts migration. Runs after the
    schema is current (``db`` is connected, so v047 has applied) and before
    anything mints or verifies a token. Idempotent and cheap on a re-run:
    finds the same rows, changes nothing that exists, and returns the same
    ids. Only ``accounts`` being empty creates an account (1.5 of the
    sequence); ``display_name`` is applied to that new owner only. The
    interactive installer passes the name it collected ("Your name") — it
    runs this in-process before exiting, since nothing it writes carries the
    answer — while the gateway's own pass at startup has none to give, so an
    owner it creates is unnamed until renamed.

    One thing is re-derived on every run: while the owner's
    ``credential_source`` is ``config`` or ``none`` — both meaning "the
    credential is whatever configuration says" — it is kept in step with
    whether ``auth.password_hash`` is set. Nothing is created, enabled or
    disabled by that; a row that has moved to ``local`` (a password of its
    own) is never touched. It closes the gap between ``nerve start`` running
    this before the first-run wizard and the wizard then writing a password.

    Reports through ``report.identity_actions`` and the ``bootstrapped_account``
    / ``updated_credential_source`` / ``generated_jwt_secret`` flags;
    ``dry_run`` reports and writes nothing.
    """
    report = MigrationReport(dry_run=dry_run) if report is None else report
    source = _credential_source_for(config)
    # Accounts a dry run says would be created on the transitional value, so 3.5
    # can report what it would then do with them (see _migrate_config_credentials).
    pending_config_rows = 0

    if dry_run:
        if await db.count_accounts() == 0:
            report.bootstrapped_account = True
            report.identity_actions.append(_account_action(source, dry_run=True))
            pending_config_rows = 1 if source == "config" else 0
    else:
        # Before anything is written: a database other users can read may not
        # receive a generated secret, and if that is what this run would have
        # to do, it stops here rather than after creating the account.
        _refuse_insecure_secret_storage(db, config, log=False)
        # Reported from what the transaction actually did, not from a count
        # taken before it: two bootstraps racing — a `nerve migrate` beside a
        # starting daemon — both read zero accounts, but only the one that
        # wins BEGIN IMMEDIATE creates the owner, and only it may say so.
        identity = await db.bootstrap_local_identity(
            credential_source=source, display_name=display_name,
        )
        if "owner" in identity.created:
            report.bootstrapped_account = True
            report.identity_actions.append(_account_action(source, dry_run=False))
            logger.info(
                "Identity bootstrap: local tenant %s, agent %s, owner account %s "
                "(actor %s, credential_source=%s), system principal %s",
                identity.tenant_id, identity.agent_id, identity.owner_account_id,
                identity.owner_actor_id, source, identity.system_actor_id,
            )

    # The mirror runs after the transaction whoever won it, so a caller that
    # lost the race with a different configuration snapshot still brings the
    # row in line with its own. For the winner it is a no-op: the account it
    # just created already carries ``source``.
    for account in await db.list_accounts():
        current = account["credential_source"]
        if current == "local" or current == source:
            continue
        report.updated_credential_source = True
        report.identity_actions.append(_mirror_action(current, source, dry_run))
        if not dry_run:
            await db.set_account_credential(account["id"], credential_source=source)

    # Then straight off `config` again (3.5). The order matters in one
    # direction: the mirror may have *just* put a row on `config` (a passwordless
    # install that gained auth.password_hash), and this is what finishes the job
    # in the same start rather than leaving a transitional state behind.
    #
    # It also settles the passwordless guard before anything evaluates it: an
    # install whose password lives in configuration is `local` by the time the
    # gateway serves, so "this instance is passwordless" is read off a row that
    # already tells the truth.
    await _migrate_config_credentials(
        db, config, report, dry_run=dry_run, pending_config_rows=pending_config_rows,
    )
    # Read the hash *after* that ran: an ordinary install has had it removed
    # from configuration and from this object, so there is nothing stale to
    # warn about; a lockdown install still has it, and that is the whole point
    # of the warning.
    _warn_if_password_hash_is_dead(
        [account["credential_source"] for account in await db.list_accounts()]
        + (["local"] * pending_config_rows),
        report,
        hash_present=bool(config.auth.password_hash) and not (
            dry_run and report.scrubbed_config_password
        ),
        dry_run=dry_run,
    )

    await ensure_jwt_secret(db, config, report=report, dry_run=dry_run)
    return report


async def ensure_jwt_secret(
    db: "Database",
    config: NerveConfig,
    *,
    report: MigrationReport | None = None,
    dry_run: bool = False,
) -> str:
    """Make sure a JWT signing secret exists, and pin it for this process.

    ``auth.jwt_secret`` in configuration is used as-is when set, so an upgrade
    keeps every live session — and it *retires* any secret the database still
    holds from a time it was not configured: a superseded key that stayed on
    disk would come back into force the day the configured one was removed,
    which is what key rotation exists to rule out. Otherwise the secret kept
    in ``nerve.db`` (``instance_secrets``) is used, and generated first if
    there is none — once, never rotated here, never written into a config
    file. A database that other users can read never receives one (see
    :func:`_refuse_insecure_secret_storage`).

    The value in force is pinned via :func:`nerve.gateway.auth.pin_jwt_secret`,
    so every consumer of :func:`~nerve.gateway.auth.effective_jwt_secret` sees
    it and keeps seeing it across config reloads; the first pin in a process
    wins, so a later call with a changed configuration returns what is pinned.
    Returns the secret in force (``""`` on a dry run that would generate).
    """
    from nerve.db.accounts import JWT_SECRET_NAME
    from nerve.gateway.auth import pin_jwt_secret, pinned_jwt_secret

    report = MigrationReport(dry_run=dry_run) if report is None else report
    if not dry_run:
        _refuse_insecure_secret_storage(db, config, log=True)
    stored = await db.get_instance_secret(JWT_SECRET_NAME)

    if config.auth.jwt_secret:
        if stored is not None:
            report.retired_stored_secret = True
            report.identity_actions.append(_retire_action(dry_run))
            if not dry_run:
                await db.delete_instance_secret(JWT_SECRET_NAME)
                logger.info(
                    "Retired the database-held signing secret: auth.jwt_secret is "
                    "configured and supersedes it",
                )
        secret = config.auth.jwt_secret
    elif stored:
        secret = stored
    else:
        report.generated_jwt_secret = True
        report.identity_actions.append(_secret_action(dry_run))
        if dry_run:
            return ""
        stored = await db.ensure_instance_secret(JWT_SECRET_NAME, secrets.token_hex(32))
        secret = stored
        logger.info(
            "Generated a JWT signing secret and stored it in nerve.db "
            "(auth.jwt_secret is not configured)",
        )

    if dry_run:
        return secret
    pin_jwt_secret(secret)
    return pinned_jwt_secret()


def _bootstrap_identity_sync(
    config_dir: Path, config: NerveConfig | None, report: MigrationReport,
) -> None:
    """The identity bootstrap from a synchronous caller (the CLI).

    Skipped on a fresh install — no ``config.local.yaml`` yet — because what
    the first-run wizard is about to write (a password, a secret) is what
    shapes the account, and because ``nerve start -f`` on a docker install
    runs the wizard on the host and then hands over to the container, so a
    bootstrap here would leave a stray host-side ``nerve.db``. The gateway
    runs the bootstrap at startup in every case; this pass exists so
    ``nerve migrate --dry-run`` can show it first and ``nerve start`` /
    ``nerve upgrade`` report it.

    A dry run inspects ``nerve.db`` read-only and never creates it; the real
    run opens its own short-lived connection (which also applies the schema
    migration, as any CLI command opening the database does).
    """
    from nerve.bootstrap import is_fresh_install

    if is_fresh_install(config_dir):
        return
    if config is None or report.migrated_config:
        # After the layout half moved files, the caller's object is stale in
        # general; the auth values it needs did not move, but reloading is
        # cheap and removes the question.
        try:
            config = load_config(config_dir)
        except Exception as e:  # noqa: BLE001 — a broken config fails startup on its own
            report.warnings.append(
                f"identity bootstrap skipped: the config could not be loaded ({e}); "
                "the gateway runs it at startup"
            )
            return

    db_path = paths.db_path()
    if report.dry_run:
        _inspect_identity(config, db_path, report)
        return

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass  # the normal case: a plain CLI process
    else:
        report.warnings.append(
            "identity bootstrap skipped: called from inside a running event loop; "
            "the gateway performs it at startup"
        )
        return
    asyncio.run(_bootstrap_with_own_connection(config, db_path, report))


def _inspect_identity(config: NerveConfig, db_path: Path, report: MigrationReport) -> None:
    """Dry-run counterpart of :func:`bootstrap_identity`: read-only, and a
    missing database is reported as "would create" rather than created."""
    from nerve.db.accounts import (
        JWT_SECRET_NAME,
        count_accounts_readonly,
        list_credential_sources_readonly,
        read_instance_secret,
    )

    source = _credential_source_for(config)
    count = count_accounts_readonly(db_path)
    # What every account's credential_source would be after the bootstrap and
    # the mirror, which is what 3.5 then acts on.
    if not count:  # None (no database / pre-v047 schema) or zero rows
        report.bootstrapped_account = True
        report.identity_actions.append(_account_action(source, dry_run=True))
        settled = [source]
    else:
        settled = []
        for current in list_credential_sources_readonly(db_path) or []:
            if current == "local" or current == source:
                settled.append(current)
                continue
            report.updated_credential_source = True
            report.identity_actions.append(_mirror_action(current, source, dry_run=True))
            settled.append(source)

    on_config = [current for current in settled if current == "config"]
    hash_present = bool(config.auth.password_hash)
    if hash_present and on_config:
        report.migrated_config_credential = True
        report.identity_actions.append(_copy_action(len(on_config), dry_run=True))
        # A dry run changes nothing, so ask what *would* be left rather than
        # reading the config object back.
        hash_present = bool(_retire_config_password(config, report, dry_run=True))
        settled = ["local" if current == "config" else current for current in settled]
    _warn_if_password_hash_is_dead(
        settled, report, hash_present=hash_present, dry_run=True,
    )

    stored = bool(read_instance_secret(db_path, JWT_SECRET_NAME))
    if config.auth.jwt_secret:
        if stored:
            report.retired_stored_secret = True
            report.identity_actions.append(_retire_action(dry_run=True))
    elif not stored:
        report.generated_jwt_secret = True
        report.identity_actions.append(_secret_action(dry_run=True))


def bootstrap_identity_sync(
    config: NerveConfig, *, display_name: str | None = None,
) -> MigrationReport:
    """The identity bootstrap from synchronous code, on a fresh connection.

    For the installer: ``nerve init`` has the owner's name in hand only while
    it runs, so it creates the owner here rather than leaving that to the
    gateway's first start, which would create it unnamed. Unlike the CLI
    migration pass this does not skip a "fresh" install — the installer has
    just written the configuration — and it never dry-runs. Raises on
    failure; the caller decides how loudly to say so, since the gateway
    repeats the bootstrap at first start regardless.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError(
            "bootstrap_identity_sync() cannot run inside an event loop; "
            "await bootstrap_identity() instead"
        )
    report = MigrationReport()
    asyncio.run(
        _bootstrap_with_own_connection(config, paths.db_path(), report, display_name)
    )
    return report


async def _bootstrap_with_own_connection(
    config: NerveConfig,
    db_path: Path,
    report: MigrationReport,
    display_name: str | None = None,
) -> None:
    from nerve.db import Database

    db = Database(db_path, workspace=config.workspace)
    await db.connect()
    try:
        await bootstrap_identity(db, config, report=report, display_name=display_name)
    finally:
        await db.close()


async def open_production_db(
    config: NerveConfig,
    *,
    db_path: Path | None = None,
    display_name: str | None = None,
) -> Database:
    """The one way production code opens the state database.

    ``Database.connect`` (state-file policy, then migrations) followed by the
    configuration-aware identity bootstrap — accounts, system principal,
    signing secret — exactly what the gateway does at startup. Every CLI
    command that opens the database goes through here, so a maintenance
    command that happens to be the first thing run after an upgrade leaves
    the same state ``nerve start`` would, and refuses the same insecure state.
    Read-only inspection paths (``nerve migrate --dry-run``, the installer's
    pre-checks) never open a ``Database`` and are unaffected.

    Returns the open database; the caller closes it. On any failure after
    the connection is open, the connection is closed before the error
    propagates.
    """
    from nerve.db import Database

    db = Database(db_path or paths.db_path(), workspace=config.workspace)
    await db.connect()
    try:
        report = await bootstrap_identity(db, config, display_name=display_name)
    except BaseException:
        await db.close()
        raise
    for action in report.identity_actions:
        logger.info("Identity bootstrap: %s", action)
    return db
