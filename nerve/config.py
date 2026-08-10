"""YAML config loader with local overrides.

Loads config.yaml (committed) and merges config.local.yaml (gitignored secrets) on top.
Supports ~ expansion in paths and environment variable references.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from nerve import paths
from nerve.coerce import FALSY, TRUTHY
from nerve.coerce import as_bool as _as_bool
from nerve.coerce import coerced as _coerced
from nerve.coerce import lenient_int as _lenient_int

import yaml

logger = logging.getLogger(__name__)

# Runtime queue of session ids to auto-resume after ``nerve restart --resume``.
# The CLI appends ids here (synchronously, before triggering the restart) and
# the fresh daemon drains it on startup. Kept next to the other machine-local
# runtime state (pid/log/db) and independent of ``-c`` so writer and reader
# always agree — which is why it goes through the path provider rather than a
# literal home: with NERVE_HOME set, a hardcoded path would have the CLI write
# one file and the daemon read another.
RESUME_QUEUE_FILE = paths.nerve_path("resume-after-restart")


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base, returning a new dict."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _expand_path(p: str | None) -> Path | None:
    """Expand a configured path; a blank value means "not set" (``None``).

    Blank has to mean unset because ``Path("")`` is ``Path(".")``, which is
    *truthy*. Callers spell their defaults as ``_expand_path(...) or
    <default>``, so a key written ``runs_dir: ""`` — or left as a bare
    ``runs_dir:`` with a stray space after it — would otherwise sail past the
    fallback and quietly mean "the directory the daemon was started in": run
    journals, proxy auth material, and the gate-plugin directory whose ``*.py``
    files get imported and executed, all landing somewhere nobody chose, with
    nothing logged to say so. ``gateway.ssl`` is sharper still because it has no
    fallback at all — ``enabled`` is "cert and key are both set", so a blank
    cert would switch TLS *on* with ``.`` as the certificate file. Blank there
    means TLS off.

    Expansion covers ``~`` and environment variables that are set, and the
    blank check is applied to what comes back out as well: a variable that is
    set but *empty* (``NERVE_RUNS_DIR=`` in a unit file or a compose env block)
    expands to nothing and would land right back on ``Path(".")``. An unset
    ``$VAR`` is a different case — ``expandvars`` leaves it in the string
    verbatim, which yields a literal, obviously-wrong path instead of another
    silent "unset", and that is the more useful failure. Stripping happens
    before ``expanduser`` because ``expanduser`` only expands a leading ``~``,
    so ``" ~/runs"`` would otherwise keep its tilde.
    """
    if p is None:
        return None
    expanded = os.path.expanduser(os.path.expandvars(str(p).strip())).strip()
    if not expanded:
        return None
    return Path(expanded)


def _setting_str(value: object, default: str = "") -> str:
    """The same "blank means unset" rule for path settings that stay ``str``.

    A few settings are declared ``str`` rather than ``Path`` because something
    downstream expands them instead, so they never reach :func:`_expand_path`
    and need the rule spelled out again. Writing it once keeps the two from
    drifting: the ``or <default>`` idiom on its own catches ``""`` and a missing
    key but not ``"  "``, which is truthy and survives to become a directory
    named for a space, created relative to wherever the process happened to
    start. Where there is no default, ``""`` *is* the unset value and callers
    gate on it, so whitespace would read as deliberately configured.

    Non-strings are stringified rather than rejected, matching the surrounding
    ``str(d.get(...))`` calls; a wrong *type* is a separate concern from a blank
    value, and it surfaces as an obviously-wrong path rather than a silent one.
    """
    return str(value or "").strip() or default


def _str_list(value: object, *, clean: bool = False) -> object:
    """Copy a ``list[str]`` field's value, or hand a non-list straight through.

    ``list("a@b.com")`` is nineteen single-character entries, and a builder that
    widens the value itself gets there before ``@_coerced`` can intervene — at
    which point nothing downstream can tell the result from a genuine list. So a
    non-list is returned untouched for :func:`nerve.coerce.coerce_scalars` to
    wrap as one element, which is what a ``${VAR}`` reference on a list field
    means.

    ``clean`` additionally strips each entry and drops the blanks, for the fields
    that were already doing that.
    """
    if not isinstance(value, list):
        return value
    if clean:
        return [s for s in (str(v or "").strip() for v in value) if s]
    return list(value)
class ConfigError(ValueError):
    """Raised when configuration cannot be loaded (e.g. an unresolved
    required ``${ENV_VAR}`` reference)."""


# Matches an escaped ``$$`` (literal dollar) or a ``${...}`` reference.
# We deliberately only interpolate the *braced* form so that values which
# legitimately contain a bare ``$`` — bcrypt ``password_hash`` (``$2b$..``),
# jwt secrets, connection strings — are never touched.
_ENV_REF_RE = re.compile(r"\$\$|\$\{([^}]*)\}")


def _interpolate_str(value: str, missing: list[str]) -> str:
    """Resolve ``${VAR}`` / ``${VAR:-default}`` references in a single string.

    * ``${VAR}``            — required; if unset, the name is appended to
      ``missing`` and the reference is left intact (the caller raises).
    * ``${VAR:-default}``   — use ``default`` when VAR is unset *or* empty
      (shell ``:-`` semantics).
    * ``$$``                — an escaped literal ``$`` (so ``$${X}`` yields the
      literal text ``${X}``).
    """

    def _replace(match: re.Match[str]) -> str:
        if match.group(0) == "$$":
            return "$"
        expr = match.group(1)
        if ":-" in expr:
            name, default = expr.split(":-", 1)
            name = name.strip()
            resolved = os.environ.get(name)
            return resolved if resolved else default
        name = expr.strip()
        if not name:
            # `${}` — almost certainly a typo. Leave it intact rather than
            # emitting a confusing "missing variable: ''" error.
            return match.group(0)
        if name in os.environ:
            return os.environ[name]
        missing.append(name)
        return match.group(0)

    return _ENV_REF_RE.sub(_replace, value)


def _interpolate_env(obj: Any, missing: list[str]) -> Any:
    """Recursively resolve ``${ENV_VAR}`` references in all string values of a
    merged config structure. Non-string leaves are returned unchanged."""
    if isinstance(obj, dict):
        return {k: _interpolate_env(v, missing) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interpolate_env(v, missing) for v in obj]
    if isinstance(obj, str):
        return _interpolate_str(obj, missing)
    return obj


def _resolve_env_refs(merged: dict[str, Any]) -> dict[str, Any]:
    """Interpolate ``${ENV_VAR}`` references across a merged config dict,
    raising :class:`ConfigError` listing every unresolved required variable."""
    missing: list[str] = []
    resolved = _interpolate_env(merged, missing)
    if missing:
        names = ", ".join(sorted(set(missing)))
        raise ConfigError(
            f"Unresolved required environment variable(s) in config: {names}. "
            "Set them in the environment, or use ${VAR:-default} to supply a "
            "fallback."
        )
    return resolved


# --- Multi-source config resolution ----------------------------------------- #
#
# Configuration is assembled from up to three layers, lowest precedence first:
#
#   1. workspace/config/settings.yaml  — shareable, git-tracked settings (the
#      portable surface synced from a remote workspace repo)
#   2. config.yaml                     — machine-local base (the historical file)
#   3. config.local.yaml               — machine-local secrets / overrides
#
# ${ENV_VAR} interpolation is applied once at the end. This keeps existing
# single-file installs working unchanged (layer 1 absent → prior behavior) while
# letting shared settings live in the workspace.


def _read_yaml_mapping(path: Path, *, strict: bool = False) -> dict[str, Any]:
    """Read a YAML file into a dict; ``{}`` if absent or empty.

    ``strict`` controls what happens when the file parses to something that
    isn't a mapping — a list, a bare string, a number. Without it the file is
    ignored with a warning, which is a bad failure mode for a config layer:
    every key it was supposed to supply silently reverts to whatever the lower
    layers said, and validation reports the bundle as fine. A truncated write,
    a `yq` mishap, or a merge conflict resolved into a sequence all produce
    exactly this shape.

    An *empty* file (or one that is nothing but comments) still yields ``{}``
    in both modes — the shipped ``settings.yaml`` scaffold is all comments and
    parses to ``None``, so treating that as an error would break every fresh
    install.

    Every way this can fail is raised as :class:`ConfigError` rather than
    escaping as a parser or OS exception, so the callers that route around a
    broken config (``nerve doctor``, ``config validate``, the reload paths) can
    report it instead of showing a traceback. That includes the file being
    unreadable: a mode-600 file owned by another user, or one deleted between
    the ``exists()`` check and the open, is a config problem like any other and
    deserves the same one-line message naming the path and the reason.

    The content is decoded as UTF-8 rather than in whatever encoding the locale
    implies. The daemon usually runs under a service manager with ``LC_ALL=C``,
    where the interpreter's default is ASCII unless it manages to coerce the
    locale to C.UTF-8 — so on a box that lacks that locale, a settings file with
    an accent in a name or a prompt hint refuses to load under the service
    manager and loads fine from an interactive shell, which is a miserable thing
    to debug. YAML is UTF-8 by spec, so pinning it is also the correct reading
    of the file.
    """
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(f"Failed to parse {path}: {e}") from e
    except (OSError, UnicodeDecodeError) as e:
        # A file that exists but cannot be read — mode 000, a directory in its
        # place, a dead mount — or one whose bytes are not the encoding we pinned.
        # Same promise as a parse failure: the callers that route around a broken
        # config report it, they don't show a traceback.
        raise ConfigError(f"Cannot read {path}: {e}") from e
    if data is None:
        return {}
    if not isinstance(data, dict):
        if strict:
            raise ConfigError(
                f"{path}: root must be a mapping of config keys, got "
                f"{type(data).__name__}"
            )
        logger.warning("config: %s is not a mapping — ignoring", path)
        return {}
    return data


def workspace_config_dir(workspace: Path) -> Path:
    """The git-syncable config subtree inside a workspace (``<ws>/config``)."""
    return Path(workspace) / "config"


# The reviewed surface of a workspace: what a locked instance takes as
# instruction rather than as data, and therefore what may only arrive as a
# reviewed, merged commit.
#
# It is not one directory because the layout is not one directory. ``config/``
# is the declarative half. ``skills/`` is executable policy in all but name: a
# SKILL.md is model-invocable text carrying its own ``allowed-tools``
# frontmatter, and ``SkillManager.discover`` indexes whatever is on disk at
# startup and on every reload without consulting lockdown, so a file dropped
# there is live on the next sync. The skill endpoints already refuse under
# lockdown; refusing the endpoint and not the file it would have written is not
# a refusal. The root instruction files are the system prompt itself — the
# standing orders every turn starts from.
#
# Not a discovery list, and deliberately not derived from one:
# ``nerve.agent.prompts.PROMPT_FILES`` also names TASK.md, which is runtime state
# the agent maintains and has to go on being able to write.
REVIEWED_DIRS = ("config", "skills")
REVIEWED_ROOT_FILES = frozenset({
    "AGENTS.md", "SOUL.md", "IDENTITY.md", "USER.md", "TOOLS.md",
})


def workspace_reviewed_paths(workspace: Path) -> list[Path]:
    """Every member of the reviewed surface of ``workspace``, subtrees first.

    One list so the write guard and sync's dirty-state check ask about the same
    files. They answer different questions — may this be written, has this been
    edited locally — and when the two disagreed about what the surface *is*, a
    skill the reviewer never saw was both writable and invisible to sync.
    """
    ws = Path(workspace)
    return [ws / name for name in REVIEWED_DIRS] + [
        ws / name for name in sorted(REVIEWED_ROOT_FILES)
    ]


def workspace_settings_file(workspace: Path) -> Path:
    """Shareable settings file inside a workspace (``<ws>/config/settings.yaml``)."""
    return workspace_config_dir(workspace) / "settings.yaml"


# Filenames served at /favicon.ico when one is present in the tracked config
# subtree, best format first. Only one should exist; the order decides it if
# several do.
#
# A convention rather than a setting. The file has to travel with the config repo
# to be worth anything — a fleet pointed at one repo is exactly the case where
# telling the instances apart in a tab bar matters — and the config subtree is
# already synced, already reviewed, and already the surface a proposal may touch.
# A path setting would add a second thing to keep in step with the file, and a
# machine-local path in shared settings is the mistake `gateway.ssl.cert` is
# documented as: a box without that file. There is nothing here to misconfigure.
_FAVICON_FILES: tuple[tuple[str, str], ...] = (
    ("favicon.svg", "image/svg+xml"),
    ("favicon.png", "image/png"),
    ("favicon.ico", "image/x-icon"),
)

# Sent with the favicon, and the reason SVG can be on the list above.
#
# An SVG fetched through ``<link rel="icon">`` is an image and scripts in it do
# not run, but the same URL *navigated to* is a same-origin document, and there
# they do — the standard stored-XSS shape for uploaded SVG. The token this UI
# authenticates with lives in ``localStorage``, so a script reaching that origin
# reaches the session.
#
# What makes it worth a header rather than a note: an agent may propose a favicon
# (SVG is text, so it fits through ``propose_config_change``), and the effect
# classifier there judges by what a file causes the daemon to *run*, which is
# nothing — so a reviewer is shown a graphic with no notice attached. Reviewing
# an icon for embedded script is not a thing to ask of them.
#
# ``default-src 'none'`` stops script and every outbound request; ``img-src
# data:`` keeps an embedded raster working, since design tools emit them; and
# ``style-src 'unsafe-inline'`` keeps ordinary SVG styling working. ``nosniff``
# is for the raster formats: it stops a ``favicon.png`` that is really HTML from
# being re-interpreted as HTML.
FAVICON_RESPONSE_HEADERS: dict[str, str] = {
    "Content-Security-Policy": (
        "default-src 'none'; img-src data:; style-src 'unsafe-inline'"
    ),
    "X-Content-Type-Options": "nosniff",
}


def workspace_favicon(workspace: Path) -> tuple[Path, str] | None:
    """The tracked favicon and its content type, or ``None`` if there is none.

    Answered per call rather than cached at start-up, so a favicon that arrives
    by sync — or is simply dropped in — is served without a restart.

    The candidate is resolved and required to stay inside the config subtree,
    which matters more here than the feature's size suggests. Git tracks
    symlinks, so a config repo can carry ``config/favicon.png ->
    /etc/shadow``; the route that serves this is unauthenticated by design,
    since a browser asks for a favicon before anyone logs in. Following the link
    would turn "set your own favicon" into an unauthenticated read of any file
    the daemon can open, and a reviewer skimming a config PR would see a
    plausible filename.
    """
    config_root = workspace_config_dir(workspace)
    for name, content_type in _FAVICON_FILES:
        candidate = config_root / name
        if not candidate.is_file():
            continue
        if not _is_within(candidate, config_root):
            logger.warning(
                "Ignoring %s: it resolves outside the tracked config subtree %s",
                candidate, config_root,
            )
            continue
        return candidate, content_type
    return None


def _load_workspace_settings(workspace: Path) -> dict[str, Any]:
    """Load ``workspace/config/settings.yaml``.

    The ``workspace`` key is stripped: the workspace location is resolved from
    the machine-local config *before* this file is read, so honoring a
    ``workspace`` value here would be circular.

    Read strictly. This is the layer that arrives from a shared repo, where
    nobody is watching the daemon's log — a file of the wrong shape has to
    fail loudly rather than evaporate into ``{}``.
    """
    data = _read_yaml_mapping(workspace_settings_file(workspace), strict=True)
    if "workspace" in data:
        logger.warning(
            "config: ignoring 'workspace' key in workspace/config/settings.yaml "
            "(the workspace location must come from config.yaml or the default)"
        )
        data = {k: v for k, v in data.items() if k != "workspace"}
    return data


# The literal an unresolved ``${VAR}`` reference leaves behind. Interpolation is
# a string substitution, so a reference nothing resolved is still sitting in the
# value — which is how a "present" secret can turn out to be nothing of the kind.
_UNRESOLVED_REF = "${"

# The two variables that anchor lockdown outside every file on the box. Set in
# the service definition (systemd ``Environment=``, docker ``-e``), which is the
# one place neither the agent nor a config edit reaches.
LOCKDOWN_ANCHOR_ENV = "NERVE_LOCKDOWN"
WORKSPACE_ANCHOR_ENV = "NERVE_WORKSPACE"


def lockdown_anchor() -> bool:
    """Whether the environment forces this instance into lockdown.

    Hardening the flag's *value* does nothing about which file supplies it, and
    that file is chosen by ``workspace:`` in the machine-local ``config.yaml``.
    One local edit repoints the workspace at a tree whose ``settings.yaml`` says
    nothing, lockdown evaluates to false, the machine layers come back, and with
    them ``auth.jwt_secret``. Every guarantee in this module rests on a file the
    thing being guarded against can rewrite. The anchor is the way out: an
    environment variable set by whatever starts the daemon is outside the config
    tree, outside the workspace, and outside anything the agent can edit.

    **The anchor can only lock.** The effective state is this OR the tracked
    flag, so ``NERVE_LOCKDOWN=false`` and an unset variable both mean "the
    environment has no opinion", never "force unlocked". Monotonicity is the
    property worth having: once an operator has anchored a box, nothing that
    arrives in a file can take it back — including a file the agent wrote. It
    also means the anchor cannot be used to *escape* a locked tracked config;
    unlocking still takes a reviewed, merged change.

    An empty value is "no opinion" rather than an error, unlike an empty
    ``lockdown:`` in the tracked file. ``Environment=NERVE_LOCKDOWN=`` and a
    bare ``docker -e NERVE_LOCKDOWN`` both produce it, and under the OR above it
    can only ever fail to *add* a restriction — the tracked flag still decides.
    An unreadable value is refused, because the only other reading is "no
    opinion", which would silently discard an operator's intent to lock.
    """
    raw = os.environ.get(LOCKDOWN_ANCHOR_ENV)
    if raw is None:
        return False
    text = raw.strip().lower()
    if not text or text in FALSY:
        return False
    if text in TRUTHY:
        return True
    raise ConfigError(
        f"{LOCKDOWN_ANCHOR_ENV}={raw!r} is neither true nor false. Accepted "
        f"spellings are true/false, 1/0, yes/no, on/off (case-insensitive); "
        f"unset or empty means the environment has no opinion and the tracked "
        f"settings decide. Refused rather than assumed, because assuming would "
        f"mean discarding an instruction to lock this instance."
    )


def _anchored_workspace() -> Path:
    """The workspace an env-anchored instance must use.

    Anchoring the flag alone buys nothing. ``workspace:`` still comes from the
    machine-local ``config.yaml``, so an attacker who can write that file
    repoints the workspace and gets an instance that is locked *onto config they
    supplied* — strictly worse than an unlocked one, because it now treats that
    tree as its sole authority and stops reading anything else. The anchor has
    to cover both, so the two variables are a single deployment unit: whatever
    sets ``NERVE_LOCKDOWN`` sets ``NERVE_WORKSPACE`` beside it.

    Refused rather than defaulted. Falling back to ``~/nerve-workspace`` would
    quietly re-admit a path that is a machine-local decision, and defaulting is
    how an operator ends up believing an anchor holds when it does not.
    """
    raw = os.environ.get(WORKSPACE_ANCHOR_ENV, "").strip()
    if not raw:
        raise ConfigError(
            f"{LOCKDOWN_ANCHOR_ENV} anchors this instance in lockdown, but "
            f"{WORKSPACE_ANCHOR_ENV} is not set. Anchoring the flag alone is not "
            f"an anchor: the workspace — which selects the settings.yaml that is "
            f"then the only source of truth — would still come from the "
            f"machine-local config.yaml. Set both in the service definition "
            f"(systemd Environment=, docker -e)."
        )
    workspace = _expand_path(raw)
    if workspace is None or not workspace.is_absolute():
        raise ConfigError(
            f"{WORKSPACE_ANCHOR_ENV}={raw!r} must be an absolute path — a "
            f"relative one means a different directory in every process that "
            f"reads it."
        )
    return workspace


def _as_lockdown(value: Any) -> bool:
    """Parse the ``lockdown`` flag, refusing anything that isn't a plain yes/no.

    Every other boolean in this module is coerced *leniently*: a value nobody can
    parse falls back to the field's declared default, so one typo in a section
    that isn't even switched on cannot stop the daemon from booting. That trade
    is the wrong way round here, because this field's default is ``False``, and
    ``False`` is the position in which nothing else is protected — the
    machine-local layers come back, the legacy cron directory comes back, and
    runtime writes to tracked config are allowed again. Guessing "off" from a
    value we could not read would quietly undo every guarantee the flag exists to
    make, and it would do it on a box nobody is watching. So an unreadable value
    is a hard :class:`ConfigError` instead: the instance does not start, and
    ``nerve config validate`` reports it before the change is ever merged.

    ``None`` (a bare ``lockdown:`` in YAML, or the key absent) is off — that is
    the file declining to set the flag, which is what the default is for. An
    *empty string* is not: it arrives from an environment variable that exists
    and carries no value, and while blanking a variable is a perfectly good way
    to switch an ordinary feature off, the one switch that governs whether
    anything else is enforced must not be flipped by a value that says nothing.
    Spell a default-unlocked box ``${VAR:-false}``.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    shown = repr(value)
    if isinstance(value, str):
        # The flag decides which layers get merged, so it is read before the
        # single post-merge interpolation pass and has to resolve its own
        # reference — otherwise `lockdown: ${NERVE_LOCKDOWN}` is judged as the
        # literal string it still is, which is truthy whatever the variable says.
        resolved = _interpolate_str(value, []).strip()
        if resolved.lower() in TRUTHY:
            return True
        if resolved.lower() in FALSY:
            return False
        if resolved != value.strip():
            shown = f"{value!r} (which resolved to {resolved!r})"
        if _UNRESOLVED_REF in resolved:
            raise ConfigError(
                f"lockdown: {shown} names an environment variable that is not "
                f"set, so whether this instance is locked cannot be determined. "
                f"Set it, or write ${{VAR:-false}} to declare the unlocked "
                f"default explicitly."
            )
    raise ConfigError(
        f"lockdown must be true or false, got {shown}. Accepted spellings are "
        f"true/false, 1/0, yes/no, on/off (case-insensitive); an empty value is "
        f"not one of them. Refused rather than assumed, because assuming would "
        f"mean assuming 'unlocked' — the setting that drops every restriction "
        f"lockdown exists to impose."
    )


def _resolved(path: Path) -> Path:
    """``path`` with symlinks followed; the path itself if it cannot be resolved."""
    try:
        return path.resolve()
    except (OSError, RuntimeError, ValueError):
        return path


def _is_within(path: Path, root: Path) -> bool:
    """True if ``path`` resolves inside ``root``, symlinks followed.

    Both sides are resolved, so ``..`` segments, an absolute path elsewhere, a
    symlink pointing out of the tree, and a path inside the workspace but
    outside its ``config/`` subtree all answer False. An unreadable path (a
    symlink loop, a permission refusal partway up) answers False too: not being
    able to establish that something is contained is not containment.

    Note what resolving ``root`` too means: containment is judged *relative to
    wherever root actually is*. If ``root`` is itself a symlink out of the tree,
    everything under it is contained and this answers True — correctly, for the
    question it is asked. Whether ``root`` is the tree it claims to be is a
    separate question, and :func:`lockdown_workspace_problems` is where it is
    asked.
    """
    try:
        return path.resolve().is_relative_to(root.resolve())
    except (OSError, RuntimeError, ValueError):
        return False


def _is_lexically_within(path: Path, root: Path) -> bool:
    """True if ``path`` *names* a location under ``root``, symlinks not followed.

    The counterpart to :func:`_is_within`, and needed because the two disagree
    exactly where a symlink is. ``_is_within`` answers "where does this path end
    up", which is the right question for a path that reaches into the subtree
    from outside. It is the wrong question for a reviewed *name*: put a symlink
    at ``config/settings.yaml`` and the resolved path leaves the subtree, so a
    guard that only resolves concludes the write is none of its business and
    permits it — writing, through the link, the file the daemon then loads as
    tracked config.

    ``normpath`` collapses ``.`` and ``..`` textually, without touching the
    filesystem, which is the point: a lexical comparison a symlink can influence
    would answer the resolved question again. Collapsing ``..`` across a
    symlinked directory can name a location the OS would not, so this can
    over-refuse; callers pair it with :func:`_is_within` and only ever add
    refusals that way.
    """
    try:
        return Path(os.path.normpath(path)).is_relative_to(os.path.normpath(root))
    except (OSError, RuntimeError, ValueError):
        return False


def read_machine_layers(config_dir: Path) -> dict[str, Any]:
    """The machine-local overlay on its own, for diagnostics.

    Lockdown discards these layers, so reporting what was discarded needs a way
    to read them afterwards. Two small YAML reads, not memoized: a caller asking
    for this wants the current contents of the files.
    """
    return _deep_merge(
        _read_yaml_mapping(config_dir / "config.yaml"),
        _read_yaml_mapping(config_dir / "config.local.yaml"),
    )


def _read_config_sources(config_dir: Path) -> dict[str, Any]:
    """Merge all config layers for ``config_dir`` and resolve ${ENV_VAR} refs.

    Returns the fully-merged, env-interpolated config dict (untyped). Shared by
    :func:`load_config` and :func:`load_mcp_servers` so both see the same view.
    """
    base = _read_yaml_mapping(config_dir / "config.yaml")
    local = _read_yaml_mapping(config_dir / "config.local.yaml")

    machine = _deep_merge(base, local)
    # An env-anchored instance takes its workspace from the environment too, so
    # that the file which decides everything else cannot decide *which* file that
    # is. Without the anchor nothing changes: the workspace comes from the
    # machine-local config, which is where it has always come from and which is
    # the right place for a per-machine path.
    anchored = lockdown_anchor()
    if anchored:
        workspace = _anchored_workspace()
    else:
        # The workspace path comes from the machine-local config (or the
        # default), never from the tracked settings file — resolve it first.
        # Best-effort ${VAR}/${VAR:-default} interpolation is applied so an
        # env-based workspace path is honored when locating settings.yaml; an
        # unresolved required ref is left intact here and surfaces later in the
        # full _resolve_env_refs pass.
        ws_raw = machine.get("workspace")
        if isinstance(ws_raw, str) and "${" in ws_raw:
            ws_raw = _interpolate_str(ws_raw, [])
        workspace = _expand_path(ws_raw) or paths.default_workspace()

    ws_settings = _load_workspace_settings(workspace)

    # Lockdown is owned by the *tracked* settings file only, so a local edit to
    # config.yaml/config.local.yaml can't unlock (or fake-lock) an instance — the
    # remote is the authority. When locked, the machine layers are dropped
    # entirely: config comes only from workspace/config + ${ENV_VAR} (secrets from
    # the environment), never from local drop-in files.
    # OR, not override: the environment can add lockdown but never remove it.
    # The tracked flag is still evaluated (and still refuses an unreadable value)
    # so an anchored box doesn't quietly run a bundle nobody can parse.
    locked = _as_lockdown(ws_settings.get("lockdown")) or anchored
    if "lockdown" in machine:
        # Mirrors the warning for a `workspace` key in settings.yaml. Ignoring it
        # is the point of the feature, but ignoring it *in silence* leaves someone
        # who wrote it in the wrong file believing the box is locked when nothing
        # about it is, and no other signal would ever tell them.
        logger.warning(
            "config: ignoring 'lockdown' in config.yaml/config.local.yaml — the "
            "flag is honored only in workspace/config/settings.yaml, so that a "
            "local edit cannot unlock or fake-lock an instance. This instance is "
            "%s.", "LOCKED" if locked else "not locked",
        )
    if locked:
        merged = dict(ws_settings)
        merged["workspace"] = str(workspace)  # keep the machine-local workspace path
    else:
        # Precedence, lowest first: settings < config.yaml < config.local.yaml
        merged = _deep_merge(_deep_merge(ws_settings, base), local)
    # Authoritative: the effective lockdown flag matches the resolution decision.
    merged["lockdown"] = locked

    return _resolve_env_refs(merged)


@dataclass
class SSLConfig:
    cert: Path | None = None
    key: Path | None = None

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> SSLConfig:
        return cls(cert=_expand_path(d.get("cert")), key=_expand_path(d.get("key")))

    @property
    def enabled(self) -> bool:
        return self.cert is not None and self.key is not None


@dataclass
class GatewayConfig:
    host: str = "0.0.0.0"
    port: int = 8900
    ssl: SSLConfig = field(default_factory=SSLConfig)

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> GatewayConfig:
        return cls(
            host=d.get("host", "0.0.0.0"),
            port=d.get("port", 8900),
            ssl=SSLConfig.from_dict(d.get("ssl", {})),
        )


@dataclass
class ProviderConfig:
    """LLM provider configuration — controls how Nerve connects to Claude.

    Supported types:
      - "anthropic" (default): Direct Anthropic API or Claude Code proxy.
      - "bedrock": AWS Bedrock. Uses IAM role on EC2/ECS/EKS automatically;
        outside AWS, configure credentials via AWS CLI, env vars, or explicit keys.
    """

    type: str = "anthropic"             # "anthropic" | "bedrock"
    aws_region: str = ""                # Bedrock region (falls back to us-east-1)
    aws_profile: str = ""               # AWS SSO profile name (optional)
    aws_access_key_id: str = ""         # Explicit creds (optional — IAM role preferred)
    aws_secret_access_key: str = ""     # Explicit creds (optional)

    @property
    def is_bedrock(self) -> bool:
        return self.type == "bedrock"

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> ProviderConfig:
        return cls(
            type=d.get("type", "anthropic"),
            aws_region=d.get("aws_region", ""),
            aws_profile=d.get("aws_profile", ""),
            aws_access_key_id=d.get("aws_access_key_id", ""),
            aws_secret_access_key=d.get("aws_secret_access_key", ""),
        )


@dataclass
class PromptRewriteConfig:
    """First-prompt rewrite — refine the opening message of a new chat.

    When enabled, the web UI offers a toggle in the composer of a new
    (empty) chat. With the toggle on, the first prompt is rewritten and
    shown to the user for approval before anything is sent.
    `enabled` here is the server-side master switch: it controls whether
    the feature is offered at all (the per-user toggle lives in the UI).

    The rewrite defaults to the main chat model (`agent.model`) — the
    rewrite shapes the whole conversation, so quality wins over speed
    here. It runs once per chat and the preview shows progress, so the
    extra latency is acceptable. Set `model` to a fast model (e.g. the
    title model) to trade quality for speed/cost.
    """

    enabled: bool = True
    model: str = ""              # empty → falls back to agent.model
    max_tokens: int = 1024
    timeout_seconds: float = 45.0

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> PromptRewriteConfig:
        return cls(
            enabled=d.get("enabled", True),
            model=d.get("model", ""),
            max_tokens=d.get("max_tokens", 1024),
            timeout_seconds=d.get("timeout_seconds", 45.0),
        )


# Fallback for the composer's Claude model picker when ``agent.models`` is
# unset and live discovery is off or unavailable (see nerve/models_catalog.py)
# — the current-generation models Nerve itself defaults to elsewhere in this
# file. Bare Anthropic API IDs: they do not apply on Bedrock, where model IDs
# are region-prefixed.
DEFAULT_CLAUDE_MODELS: tuple[str, ...] = (
    "claude-opus-5",
    "claude-sonnet-4-6",
    "claude-haiku-4-5-20251001",
)


@dataclass
class AgentConfig:
    # Agent backend for NEW sessions: "claude" (Claude Agent SDK) or
    # "codex" (OpenAI Codex app-server; see CodexConfig). Existing
    # sessions are sticky — the backend they were created with is stored
    # in sessions.backend and always wins over this setting.
    backend: str = "claude"
    # Backend for NEW cron/hook sessions; empty → same as `backend`.
    # Wakeup turns fire on existing sessions and inherit their stored
    # backend — this only affects freshly minted cron/hook sessions.
    cron_backend: str = ""
    model: str = "claude-opus-5"
    cron_model: str = "claude-sonnet-4-6"
    title_model: str = "claude-haiku-4-5-20251001"  # Session title generation
    # Model-alias remapping for the Claude CLI subprocess: alias → model ID,
    # emitted as ANTHROPIC_DEFAULT_<ALIAS>_MODEL env vars so short aliases
    # ("opus" in Agent/Workflow tool calls, skill frontmatter, cron model
    # overrides) resolve to the mapped model. Supported aliases: opus,
    # sonnet, haiku, fable. Nerve defaults opus → claude-opus-5 on
    # non-Bedrock providers; entries here merge over that default, and an
    # empty value ("") unsets it (falls back to the CLI's built-in mapping).
    model_aliases: dict[str, str] = field(default_factory=dict)
    # Claude models selectable in the web composer's model picker
    # (GET /api/models). The configured `model` above is always offered
    # first; entries here extend the list (order-preserving, deduped).
    # Empty → the models the configured credentials actually expose, read
    # from the Anthropic Models API (see `model_discovery` below), falling
    # back to a built-in current-generation list (DEFAULT_CLAUDE_MODELS).
    # On Bedrock only the models named in config are offered (Bedrock IDs
    # are region-prefixed, so neither discovery nor the bare built-ins
    # resolve there).
    models: list[str] = field(default_factory=list)
    # Ask the Anthropic Models API (GET /v1/models) which models the
    # configured credentials can reach, and offer those in the picker, so a
    # newly released model needs no code change or config edit. Best-effort:
    # ignored when `models` above is set explicitly, on Bedrock, without an
    # API key, or when the API is unreachable — the built-in list applies.
    model_discovery: bool = True
    max_turns: int = 100
    max_concurrent: int = 32
    thinking: str = "max"       # max, high, medium, low, disabled, adaptive, or number (budget_tokens)
    effort: str = "max"         # max, xhigh, high, medium, low
    # Effort for cron- and hook-sourced turns (sensing / triage work). These
    # fire far more often than interactive sessions and rarely need Opus-tier
    # deliberation, so they default lower than `effort` above to cut token
    # spend. Applied by the claude backend when source is "cron" or "hook";
    # interactive sources (web, telegram, wakeup) keep the full `effort`.
    cron_effort: str = "medium"  # max, xhigh, high, medium, low
    context_1m: bool = True     # Enable 1M context window beta
    # Substrings of model names for which the context-1m beta header must NOT
    # be sent (some subscriptions reject the beta for specific models — e.g.
    # claude-sonnet-4-6 returns 400 "long context beta not yet available for
    # this subscription"). Match is case-insensitive substring on the resolved
    # model name. Empty list = send beta for all models when context_1m=True.
    context_1m_excluded_models: list[str] = field(default_factory=list)
    # Prompt-cache write TTL policy: "5m" (status quo — every write uses the
    # default 5-minute TTL), "1h" (always request the 1-hour TTL: writes cost
    # 2x base input instead of 1.25x but survive sparse turn cadences), or
    # "auto" (per session at client-build time — sparse-cadence sessions such
    # as persistent crons, wakeup loops and spaced conversations get 1h;
    # dense sessions stay on 5m). See nerve/agent/cache_policy.py.
    cache_ttl: str = "5m"
    # Substrings of model names that must never request the 1h cache TTL
    # (same matching semantics as context_1m_excluded_models).
    cache_ttl_excluded_models: list[str] = field(default_factory=list)
    # Hung-CLI detection: max idle time between SDK messages on a single
    # turn before the engine treats the subprocess as dead and falls into
    # the existing CLI-crash retry path.  Set to 0 to disable (legacy
    # behaviour: turns can hang forever).  900s comfortably covers a 10-min
    # Bash tool call plus SDK round-trips while still catching real hangs.
    cli_idle_timeout_seconds: int = 900
    # When True, background sub-agents (the Agent tool with run_in_background, or
    # background Bash) get the SAME auto-approved tool permissions as foreground
    # agents, via a PreToolUse hook that pre-approves all non-interactive tools.
    # Background tasks are detached and non-blocking, so the CLI never surfaces an
    # approval prompt for them — the can_use_tool callback is never invoked for
    # their nested Write/Edit/Bash calls, and the CLI denies them by default.
    # A PreToolUse hook DOES fire for those nested calls (it is a programmatic
    # callback, not a user prompt), so returning permissionDecision="allow" there
    # grants the permission. Set False to restore the CLI default (background
    # sub-agent writes denied; build/write agents must then run in foreground).
    background_agent_permissions: bool = True
    # Agent teams (CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS). The CLI gates the
    # `SendMessage` tool behind this flag, and without it the Agent tool still
    # advertises SendMessage in its own description and in every spawn result
    # ("use SendMessage with to: '<id>' to continue this agent") — so the model
    # is told to resume sub-agents with a tool that does not exist, tries, and
    # fails. Enabling it makes SendMessage real: sub-agents become resumable
    # with their context intact, and teammates can message each other.
    # Nerve loads no settings files (setting_sources=[]), so the flag can only
    # reach the CLI through the env dict built in the claude backend.
    # Teammates are opt-in per turn — the model still has to spawn them — and
    # they cost a full context window each. Note the CLI cannot restore
    # in-process teammates on resume: a session whose client is recycled (idle
    # timeout, restart, crash retry) comes back without them, and the lead may
    # message teammates that no longer exist. Set False to restore the CLI
    # default (no SendMessage, no teams).
    agent_teams: bool = True
    prompt_rewrite: PromptRewriteConfig = field(default_factory=PromptRewriteConfig)

    @property
    def resolved_cron_backend(self) -> str:
        """Backend used for new cron/hook sessions."""
        return self.cron_backend or self.backend

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> AgentConfig:
        return cls(
            backend=str(d.get("backend", "claude")).strip().lower(),
            cron_backend=str(d.get("cron_backend") or "").strip().lower(),
            model=d.get("model", "claude-opus-5"),
            cron_model=d.get("cron_model", "claude-sonnet-4-6"),
            title_model=d.get("title_model", "claude-haiku-4-5-20251001"),
            model_aliases={
                str(k): str(v or "")
                for k, v in (d.get("model_aliases") or {}).items()
            },
            models=_str_list(d.get("models"), clean=True),
            model_discovery=d.get("model_discovery", True),
            max_turns=d.get("max_turns", 100),
            max_concurrent=d.get("max_concurrent", 32),
            thinking=str(d.get("thinking", "max")),
            effort=str(d.get("effort", "max")),
            cron_effort=str(d.get("cron_effort", "medium")),
            context_1m=d.get("context_1m", True),
            context_1m_excluded_models=_str_list(
                d.get("context_1m_excluded_models")
            ),
            cache_ttl=str(d.get("cache_ttl", "5m")),
            cache_ttl_excluded_models=_str_list(
                d.get("cache_ttl_excluded_models")
            ),
            cli_idle_timeout_seconds=d.get("cli_idle_timeout_seconds", 900),
            background_agent_permissions=d.get("background_agent_permissions", True),
            agent_teams=d.get("agent_teams", True),
            prompt_rewrite=PromptRewriteConfig.from_dict(d.get("prompt_rewrite") or {}),
        )

    def context_1m_enabled_for(self, model: str | None) -> bool:
        """Whether the context-1m beta applies to *model* (or the default
        model when None).  False if globally disabled or if the model name
        matches any entry in ``context_1m_excluded_models``."""
        if not self.context_1m:
            return False
        resolved = (model or self.model).lower()
        return not any(
            tok and tok.lower() in resolved for tok in self.context_1m_excluded_models
        )


@dataclass
class TelegramConfig:
    enabled: bool = True
    bot_token: str = ""
    allowed_users: list[int] = field(default_factory=list)
    stream_mode: str = "partial"
    # DM authorization policy:
    #   "pairing" (default) — unknown users may pair with a one-time code
    #                         (`nerve pair`); everyone else is rejected.
    #   "open"              — anyone can talk to the bot. Dangerous: full
    #                         agent access for any Telegram user. A warning
    #                         is logged at startup.
    dm_policy: str = "pairing"

    @classmethod
    @_coerced
    def from_dict(cls, d: dict, locked: bool = False) -> TelegramConfig:
        dm_policy = d.get("dm_policy", "pairing")
        if dm_policy not in ("pairing", "open"):
            logger.warning(
                "telegram.dm_policy %r is not one of ('pairing', 'open') — "
                "falling back to 'pairing'",
                dm_policy,
            )
            dm_policy = "pairing"
        # Whether a given box answers Telegram DMs is a per-machine decision, so
        # `nerve init` writes this key to the machine-local config.yaml. Lockdown
        # drops that layer, which would leave the declared default — on —
        # deciding for it: a box where Telegram was switched off would start
        # serving DMs, with full agent access, the moment the shared settings
        # carried a token this machine's environment can resolve.
        #
        # So the fallback direction is chosen by lockdown, and parsed here rather
        # than left to @_coerced. The decorator falls back to the *declared*
        # default, which is True, so `enabled: ${TG_ON:-nope}` — an env-reference
        # spelling this file's own docs recommend for per-box fleet config — would
        # turn the bot on everywhere one bad value reached. Deliberately still
        # lenient rather than fatal: an unreadable value in one section must not
        # stop the daemon booting, and off is the safe end of the guess.
        enabled = (
            _as_bool(d["enabled"], not locked, label="TelegramConfig.enabled")
            if "enabled" in d
            else not locked
        )
        return cls(
            enabled=enabled,
            bot_token=d.get("bot_token", ""),
            # Deliberately uncast. The declared list[int] is converted after
            # construction, which logs an unconvertible entry and keeps it
            # verbatim; casting here raises instead, so one bad value in a
            # section that may not even be enabled would stop the daemon
            # booting. It also stopped a bare string being read character by
            # character — "123" became the three user IDs 1, 2 and 3.
            allowed_users=d.get("allowed_users") or [],
            stream_mode=d.get("stream_mode", "partial"),
            dm_policy=dm_policy,
        )


@dataclass
class TelegramSyncConfig:
    enabled: bool = True
    api_id: int = 0
    api_hash: str = ""
    monitored_folders: list[str] = field(default_factory=list)
    exclude_chats: list[int] = field(default_factory=list)
    schedule: str = "*/5 * * * *"
    processor: str = "agent"
    batch_size: int = 50
    prompt_hint: str = ""
    model: str = ""
    condense: bool = False

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> TelegramSyncConfig:
        return cls(
            enabled=d.get("enabled", True),
            api_id=d.get("api_id", 0),
            api_hash=d.get("api_hash", ""),
            monitored_folders=d.get("monitored_folders", []),
            exclude_chats=d.get("exclude_chats", []),
            schedule=d.get("schedule", "*/5 * * * *"),
            processor=d.get("processor", "agent"),
            batch_size=d.get("batch_size", 50),
            prompt_hint=d.get("prompt_hint", ""),
            model=d.get("model", ""),
            condense=d.get("condense", False),
        )


@dataclass
class GmailSyncConfig:
    enabled: bool = True
    accounts: list[str] = field(default_factory=list)
    schedule: str = "*/15 * * * *"
    keyring_password: str = ""
    processor: str = "agent"
    batch_size: int = 20  # Lower default — each message needs a separate get call
    prompt_hint: str = ""
    model: str = ""
    condense: bool = False
    condense_prompt: str = ""  # Custom prompt for LLM condensation (overrides default)

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> GmailSyncConfig:
        return cls(
            enabled=d.get("enabled", True),
            accounts=d.get("accounts", []),
            schedule=d.get("schedule", "*/15 * * * *"),
            keyring_password=d.get("keyring_password", ""),
            processor=d.get("processor", "agent"),
            batch_size=d.get("batch_size", 20),
            prompt_hint=d.get("prompt_hint", ""),
            model=d.get("model", ""),
            condense=d.get("condense", False),
            condense_prompt=d.get("condense_prompt", ""),
        )


@dataclass
class ImapMatchConfig:
    """Which IMAP messages are singled out for the optional image pass.

    Both lists are case-insensitive substrings and both default to empty, so
    an unconfigured source never singles anything out and stays a plain
    mailbox reader.
    """

    # Matched against the decoded From: header.
    sender_contains: list[str] = field(default_factory=list)
    # Matched against an inline image's Content-ID / filename. A hit also
    # picks that image over the merely largest one.
    attachment_contains: list[str] = field(default_factory=list)
    # Drop everything that did not match, turning the mailbox into a
    # single-purpose notifier instead of a second inbox.
    only_matched: bool = False

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> ImapMatchConfig:
        return cls(
            sender_contains=d.get("sender_contains", []),
            attachment_contains=d.get("attachment_contains", []),
            only_matched=d.get("only_matched", False),
        )


@dataclass
class ImapVisionConfig:
    """Multimodal pass over an inline image in matched messages.

    For mail whose payload is only legible in an image — a scan, a photo, a
    rendered document — the model reads it at ingest time so downstream
    consumers get plain text.

    ``prompt`` and ``answer_key`` are a matched pair: the prompt tells the
    model which label to emit, and the parser reads the line after exactly
    that label. Changing one without the other silently yields
    ``unknown_answer`` every time, so they live together here. Leave
    ``answer_key`` empty to take the first non-empty line of the answer.

    Wording templates accept ``{label} {answer} {vision} {subject} {sender}
    {date} {body}``; ``{answer}`` is the parsed line and ``{vision}`` the full
    model reply.
    """

    enabled: bool = False
    # Defaults to memory.fast_model when left empty.
    model: str = ""
    prompt: str = ""
    answer_key: str = ""
    unknown_answer: str = "unreadable"
    summary: str = "[{label}] {answer}"
    summary_unknown: str = "[{label}] {subject}"
    content: str = (
        "{vision}\n\n"
        "Subject: {subject}\n"
        "From: {sender}\n"
        "Date: {date}"
    )
    content_unknown: str = (
        "The image could not be read (missing or unreadable).\n\n"
        "Subject: {subject}\n"
        "From: {sender}\n"
        "Date: {date}"
    )

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> ImapVisionConfig:
        base = cls()
        return cls(
            enabled=d.get("enabled", base.enabled),
            model=str(d.get("model", base.model)),
            prompt=str(d.get("prompt", base.prompt)),
            answer_key=str(d.get("answer_key", base.answer_key)),
            unknown_answer=str(d.get("unknown_answer", base.unknown_answer)),
            summary=str(d.get("summary", base.summary)),
            summary_unknown=str(d.get("summary_unknown", base.summary_unknown)),
            content=str(d.get("content", base.content)),
            content_unknown=str(d.get("content_unknown", base.content_unknown)),
        )


@dataclass
class ImapAccountConfig:
    """One IMAP mailbox. The password lives in config.local.yaml under
    ``sync.imap.passwords[<username>]``, never here."""
    host: str
    username: str
    label: str
    port: int = 993
    mailbox: str = "INBOX"

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> ImapAccountConfig:
        # No key is required here: a half-written account entry must not stop
        # the daemon from starting. The registry skips an account whose host
        # or username is empty, the same way it skips one with no password.
        username = str(d.get("username", ""))
        return cls(
            host=str(d.get("host", "")),
            username=username,
            label=str(d.get("label") or username.split("@")[0]),
            port=d.get("port", 993),
            mailbox=str(d.get("mailbox", "INBOX")),
        )


@dataclass
class ImapSyncConfig:
    enabled: bool = False
    accounts: list[ImapAccountConfig] = field(default_factory=list)
    passwords: dict[str, str] = field(default_factory=dict)
    schedule: str = "*/30 * * * *"
    batch_size: int = 20
    initial_lookback_days: int = 1
    condense: bool = False
    condense_prompt: str = ""
    match: ImapMatchConfig = field(default_factory=ImapMatchConfig)
    vision: ImapVisionConfig = field(default_factory=ImapVisionConfig)

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> ImapSyncConfig:
        return cls(
            enabled=d.get("enabled", False),
            accounts=[ImapAccountConfig.from_dict(a) for a in d.get("accounts", [])],
            passwords=dict(d.get("passwords", {})),
            schedule=str(d.get("schedule", "*/30 * * * *")),
            batch_size=d.get("batch_size", 20),
            initial_lookback_days=d.get("initial_lookback_days", 1),
            condense=d.get("condense", False),
            condense_prompt=str(d.get("condense_prompt", "")),
            match=ImapMatchConfig.from_dict(d.get("match", {})),
            vision=ImapVisionConfig.from_dict(d.get("vision", {})),
        )


@dataclass
class GitHubSyncConfig:
    enabled: bool = True
    schedule: str = "*/15 * * * *"
    processor: str = "agent"
    batch_size: int = 30
    prompt_hint: str = ""
    model: str = ""
    condense: bool = False
    # Inbox guardrails — limit which repos reach the inbox (matched on the
    # notification's repo full_name, e.g. "ClickHouse/nerve"). Both support
    # case-insensitive globs. allow_repos is an allowlist (empty = all repos
    # pass); deny_repos is a denylist and takes precedence over allow_repos.
    allow_repos: list[str] = field(default_factory=list)
    deny_repos: list[str] = field(default_factory=list)
    # Actor guardrails — limit which GitHub logins can land a notification in
    # the inbox, matched on the "actors" metadata key (every login involved in
    # the notification: issue/PR author, assignees, comment & review authors).
    # Same semantics as allow_repos/deny_repos — case-insensitive globs, deny
    # wins, and a non-empty allow_actors is fail-closed (a notification with no
    # matching actor is dropped before it reaches the inbox). Empty = all pass.
    allow_actors: list[str] = field(default_factory=list)
    deny_actors: list[str] = field(default_factory=list)
    # Reason guardrails — limit which GitHub notification "reason" values reach
    # the inbox, matched on the "reason" metadata key (GitHub's own reason:
    # mention, author, review_requested, assign, team_mention, comment,
    # subscribed, ci_activity, state_change, ...). Same semantics — case-
    # insensitive globs, deny wins, non-empty allow is fail-closed. Denying
    # "comment"/"subscribed" drops follow-up churn on threads you merely
    # commented on or watch, while keeping mentions, review requests and
    # activity on your own PRs/issues. Empty = all reasons pass.
    allow_reasons: list[str] = field(default_factory=list)
    deny_reasons: list[str] = field(default_factory=list)
    # CI guardrail — drop workflow-run notifications for branches that aren't
    # yours, matched on the "ci_branch" metadata key (the branch parsed out of
    # a CheckSuite title; "" for every other notification). Denying
    # "main"/"master" keeps CI failures on your own PR branches while dropping
    # upstream-sync and scheduled runs on the default branch. deny only — an
    # allow list is fail-closed and would drop every non-CI notification, since
    # their ci_branch is empty.
    deny_ci_branches: list[str] = field(default_factory=list)

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> GitHubSyncConfig:
        return cls(
            enabled=d.get("enabled", True),
            schedule=d.get("schedule", "*/15 * * * *"),
            processor=d.get("processor", "agent"),
            batch_size=d.get("batch_size", 30),
            prompt_hint=d.get("prompt_hint", ""),
            model=d.get("model", ""),
            condense=d.get("condense", False),
            allow_repos=d.get("allow_repos", []),
            deny_repos=d.get("deny_repos", []),
            allow_actors=d.get("allow_actors", []),
            deny_actors=d.get("deny_actors", []),
            allow_reasons=d.get("allow_reasons", []),
            deny_reasons=d.get("deny_reasons", []),
            deny_ci_branches=d.get("deny_ci_branches", []),
        )


@dataclass
class GitHubEventsSyncConfig:
    """Config for GitHub Events source (user's own activity feed)."""
    enabled: bool = False
    schedule: str = "*/15 * * * *"
    repos: list[str] = field(default_factory=list)  # empty = all repos
    username: str = ""  # auto-detect from gh auth if empty
    batch_size: int = 50
    condense: bool = False
    processor: str = "agent"
    prompt_hint: str = ""
    model: str = ""

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> GitHubEventsSyncConfig:
        return cls(
            enabled=d.get("enabled", False),
            schedule=d.get("schedule", "*/15 * * * *"),
            repos=d.get("repos", []),
            username=d.get("username", ""),
            batch_size=d.get("batch_size", 50),
            condense=d.get("condense", False),
            processor=d.get("processor", "agent"),
            prompt_hint=d.get("prompt_hint", ""),
            model=d.get("model", ""),
        )


@dataclass
class GitHubReposSyncConfig:
    """Config for the GitHub Repos source (monitor watched repos for new issues/PRs).

    Unlike ``github`` (notifications) and ``github_events`` (your own activity),
    this source watches an explicit set of repositories for newly-created issues
    and pull requests. ``repos`` is required — an empty list makes the source a
    no-op.
    """
    enabled: bool = False
    schedule: str = "*/15 * * * *"
    repos: list[str] = field(default_factory=list)  # required; empty = no-op
    batch_size: int = 50
    condense: bool = False
    processor: str = "agent"
    prompt_hint: str = ""
    model: str = ""

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> GitHubReposSyncConfig:
        return cls(
            enabled=d.get("enabled", False),
            schedule=d.get("schedule", "*/15 * * * *"),
            repos=d.get("repos", []),
            batch_size=d.get("batch_size", 50),
            condense=d.get("condense", False),
            processor=d.get("processor", "agent"),
            prompt_hint=d.get("prompt_hint", ""),
            model=d.get("model", ""),
        )


@dataclass
class CodexOriginConfig:
    """A single Codex thread sync origin.

    Origins represent the transport over which we receive Codex thread
    items — a local rollout directory, a remote app-server, or the
    OpenAI cloud Codex API.
    """

    id: str = "local"
    type: str = "local_rollout"           # local_rollout | app_server | cloud
    enabled: bool = True
    # local_rollout fields
    path: str = "~/.codex/sessions"
    archive_path: str = "~/.codex/archived_sessions"
    poll_interval_seconds: float = 2.0    # How often to scan for new content
    # app_server fields
    transport: dict = field(default_factory=dict)

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> CodexOriginConfig:
        return cls(
            id=d.get("id", "local"),
            type=d.get("type", "local_rollout"),
            enabled=d.get("enabled", True),
            path=_setting_str(d.get("path"), "~/.codex/sessions"),
            archive_path=_setting_str(
                d.get("archive_path"), "~/.codex/archived_sessions"
            ),
            poll_interval_seconds=d.get("poll_interval_seconds", 2.0),
            transport=d.get("transport", {}),
        )


@dataclass
class CodexWorkspaceFilterConfig:
    """Decides which Codex threads to sync based on ``session_meta.cwd``.

    ``mode``:
      * ``nerve_workspace`` (default) — only threads whose cwd matches
        Nerve's configured workspace.
      * ``explicit`` — only threads whose cwd matches one of
        ``explicit_paths``.
      * ``any`` — sync every thread, regardless of cwd. Not recommended
        unless you really want every Codex session on the box.
    """

    mode: str = "nerve_workspace"
    explicit_paths: list[str] = field(default_factory=list)

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> CodexWorkspaceFilterConfig:
        return cls(
            mode=str(d.get("mode", "nerve_workspace")),
            explicit_paths=_str_list(d.get("explicit_paths")),
        )


@dataclass
class CodexSyncConfig:
    """Sync configuration for Codex threads.

    Disabled by default — flip ``enabled=true`` in config.local.yaml once
    the workspace filter is verified to behave as expected on your box.
    """

    enabled: bool = False
    workspace_filter: CodexWorkspaceFilterConfig = field(
        default_factory=CodexWorkspaceFilterConfig,
    )
    origins: list[CodexOriginConfig] = field(default_factory=list)
    store_encrypted_reasoning: bool = True

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> CodexSyncConfig:
        raw_origins = d.get("origins", [])
        origins = [
            CodexOriginConfig.from_dict(o)
            for o in raw_origins
            if isinstance(o, dict)
        ]
        return cls(
            enabled=d.get("enabled", False),
            workspace_filter=CodexWorkspaceFilterConfig.from_dict(
                d.get("workspace_filter", {}),
            ),
            origins=origins,
            store_encrypted_reasoning=d.get("store_encrypted_reasoning", True),
        )


@dataclass
class SyncConfig:
    telegram: TelegramSyncConfig = field(default_factory=TelegramSyncConfig)
    gmail: GmailSyncConfig = field(default_factory=GmailSyncConfig)
    imap: ImapSyncConfig = field(default_factory=ImapSyncConfig)
    github: GitHubSyncConfig = field(default_factory=GitHubSyncConfig)
    github_events: GitHubEventsSyncConfig = field(default_factory=GitHubEventsSyncConfig)
    github_repos: GitHubReposSyncConfig = field(default_factory=GitHubReposSyncConfig)
    codex: CodexSyncConfig = field(default_factory=CodexSyncConfig)
    message_ttl_days: int = 7           # How long to keep source messages in the inbox
    consumer_cursor_ttl_days: int = 2   # Consumer cursors expire after N days of inactivity

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> SyncConfig:
        return cls(
            telegram=TelegramSyncConfig.from_dict(d.get("telegram", {})),
            gmail=GmailSyncConfig.from_dict(d.get("gmail", {})),
            imap=ImapSyncConfig.from_dict(d.get("imap", {})),
            github=GitHubSyncConfig.from_dict(d.get("github", {})),
            github_events=GitHubEventsSyncConfig.from_dict(d.get("github_events", {})),
            github_repos=GitHubReposSyncConfig.from_dict(d.get("github_repos", {})),
            codex=CodexSyncConfig.from_dict(d.get("codex", {})),
            message_ttl_days=d.get("message_ttl_days", 7),
            consumer_cursor_ttl_days=d.get("consumer_cursor_ttl_days", 2),
        )


@dataclass
class MemoryCategoryConfig:
    name: str
    description: str

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> MemoryCategoryConfig:
        return cls(name=d["name"], description=d.get("description", ""))


@dataclass
class MemoryConfig:
    recall_model: str = "claude-sonnet-4-6"  # Recall routing
    memorize_model: str = "claude-sonnet-4-6"  # Extraction & preprocessing
    fast_model: str = "claude-haiku-4-5-20251001"  # Category summaries, date resolution
    embed_model: str = ""
    sqlite_dsn: str = ""
    semantic_dedup_threshold: float = 0.85  # Cosine similarity threshold for semantic dedup
    knowledge_filter: bool = False  # Post-extraction LLM filter for generic knowledge (extra API call)
    categories: list[MemoryCategoryConfig] = field(default_factory=list)

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> MemoryConfig:
        default_dsn = f"sqlite:///{paths.memu_sqlite()}"
        raw_cats = d.get("categories", [])
        categories = [MemoryCategoryConfig.from_dict(c) for c in raw_cats]
        return cls(
            recall_model=d.get("recall_model", "claude-sonnet-4-6"),
            memorize_model=d.get("memorize_model", "claude-sonnet-4-6"),
            fast_model=d.get("fast_model", "claude-haiku-4-5-20251001"),
            embed_model=d.get("embed_model", ""),
            sqlite_dsn=d.get("sqlite_dsn", default_dsn),
            semantic_dedup_threshold=d.get("semantic_dedup_threshold", 0.85),
            knowledge_filter=d.get("knowledge_filter", False),
            categories=categories,
        )


def _cron_dir_has_jobs(d: Path) -> bool:
    """True if a cron dir actually holds job definitions (jobs.yaml or system.yaml)."""
    return (d / "jobs.yaml").exists() or (d / "system.yaml").exists()


def _resolve_cron_dir(workspace: Path | None, locked: bool = False) -> Path:
    """Effective directory holding cron config (jobs/system/gates).

    Prefers the git-syncable ``workspace/config/cron`` so cron definitions live
    in the shared workspace. Resolution is **file-aware**: an empty
    ``workspace/config/cron`` (e.g. a git checkout with only a ``gates/``
    placeholder, or a partially-initialized workspace) must NOT silently shadow
    a legacy ``~/.nerve/cron`` that still holds real jobs. So:

      * If the workspace location has job files → use it.
      * Else, if the legacy location has job files → use it (un-migrated install).
      * Else → the workspace location (new install; may be about to be populated).

    Under ``locked`` (lockdown), the legacy ``~/.nerve/cron`` fallback is disabled
    entirely — cron config comes only from the workspace.
    """
    legacy = paths.cron_dir()
    if workspace is None:
        return legacy
    ws_cron = workspace_config_dir(workspace) / "cron"
    if locked:
        return ws_cron
    if _cron_dir_has_jobs(ws_cron):
        return ws_cron
    if _cron_dir_has_jobs(legacy):
        return legacy
    return ws_cron


def _locked_cron_path(configured: Path, default: Path, root: Path, key: str) -> Path:
    """Keep one locked cron path inside the tracked config subtree.

    A locked instance runs only what the reviewed workspace repo carries, and
    these three keys decide which files it reads — including, for
    ``gate_plugins_dir``, which ``.py`` files it imports and *executes* at
    start-up and on every cron reload. A tracked ``settings.yaml`` that pointed
    any of them elsewhere would hand the box job definitions and code that never
    went through review, using nothing but a YAML edit. So a path that escapes is
    dropped in favour of the in-workspace default and reported.

    Called only when locked. An unmigrated install is entitled to point cron at
    the legacy ``~/.nerve/cron``, and that is the whole reason the machine-local
    layers exist.

    The default is verified too, and the instance refuses to load when it also
    escapes. That is not a defensive afterthought — it is the case with no YAML
    key in it at all. ``<workspace>/config/cron/gates`` is a name a config repo
    can carry as a *symlink*, and git tracks symlinks, so a reviewed and merged
    bundle can point the default itself anywhere. Substituting the default there
    would hand back the exact path just rejected, log a warning that contradicts
    itself, and import whatever was on the other end.
    """
    if _is_within(configured, root):
        return configured
    if _is_within(default, root):
        logger.warning(
            "cron.%s (%s) resolves outside %s — ignoring it and using %s "
            "instead, because a locked instance takes its cron config, and its "
            "gate plugins, only from the tracked workspace",
            key, configured, root, default,
        )
        return default
    also_default = "" if configured == default else f" — as does the default {default}"
    raise ConfigError(
        f"lockdown: cron.{key} resolves outside the tracked config subtree "
        f"{root}: {configured} points at {_resolved(configured)}{also_default}. "
        f"A locked instance reads its cron config, and imports its gate plugins, "
        f"only from the reviewed workspace, and there is no contained path left "
        f"to fall back to."
    )


@dataclass
class CronConfig:
    # Bare defaults (no workspace context) point at the legacy machine-local
    # location; from_dict resolves the workspace-aware location when it can.
    jobs_file: Path = field(default_factory=lambda: paths.cron_dir() / "jobs.yaml")
    system_file: Path = field(default_factory=lambda: paths.cron_dir() / "system.yaml")
    # Directory scanned at startup for drop-in custom gate plugins (.py files
    # defining CronGate subclasses). See nerve/cron/gate_plugins.py.
    gate_plugins_dir: Path = field(default_factory=lambda: paths.cron_dir() / "gates")

    @classmethod
    @_coerced
    def from_dict(cls, d: dict, workspace: Path | None = None, locked: bool = False) -> CronConfig:
        base = _resolve_cron_dir(workspace, locked=locked)
        jobs_file = _expand_path(d.get("jobs_file")) or base / "jobs.yaml"
        system_file = _expand_path(d.get("system_file")) or base / "system.yaml"
        gate_plugins_dir = _expand_path(d.get("gate_plugins_dir")) or base / "gates"
        if locked and workspace is not None:
            root = workspace_config_dir(workspace)
            if not _is_within(base, root):
                # The subtree itself leaves the workspace — a symlinked
                # config/cron, say. No substitution fixes that, since every
                # default is derived from it, so refuse to run rather than read
                # cron config from a directory outside the reviewed tree.
                raise ConfigError(
                    f"lockdown: the cron directory {base} resolves outside the "
                    f"tracked config subtree {root}. A locked instance must read "
                    f"its cron config, and import its gate plugins, only from the "
                    f"reviewed workspace."
                )
            jobs_file = _locked_cron_path(jobs_file, base / "jobs.yaml", root, "jobs_file")
            system_file = _locked_cron_path(system_file, base / "system.yaml", root, "system_file")
            gate_plugins_dir = _locked_cron_path(
                gate_plugins_dir, base / "gates", root, "gate_plugins_dir",
            )
        return cls(
            jobs_file=jobs_file,
            system_file=system_file,
            gate_plugins_dir=gate_plugins_dir,
        )


@dataclass
class WorkspaceSyncConfig:
    """Git-backed sync of the workspace from a remote (the config repo).

    Opt-in. When enabled, the daemon periodically ``git pull --ff-only``s the
    workspace so config changes reviewed & merged via PR on the remote land on
    the instance and hot-reload. The remote/branch come from git itself; only the
    cadence and enablement live here.
    """

    enabled: bool = False
    branch: str = ""               # empty → git's default (current tracking branch)
    # How often the daemon pulls. This is also the upper bound on how long a
    # merged config change takes to reach the instance, since the pull is what
    # applies it, so it is deliberately short; a fetch that finds nothing new is
    # one round trip. Raise it for a large fleet pointed at one repo.
    interval_minutes: int = 1
    validate: bool = True          # validate the pulled bundle before applying
    # Treat an unset required ${VAR} in the pulled bundle as invalid. On by
    # default: the daemon refuses to load such a config, so merging it only
    # defers the failure to the next restart. Configurable because a shared repo
    # can introduce a ${VAR} that this particular box legitimately does not set,
    # and the alternative is a machine whose every sync fails until someone
    # notices.
    strict_env: bool = True

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> WorkspaceSyncConfig:
        return cls(
            enabled=d.get("enabled", False),
            branch=str(d.get("branch", "") or ""),
            interval_minutes=d.get("interval_minutes", 1),
            validate=d.get("validate", True),
            strict_env=d.get("strict_env", True),
        )


@dataclass
class BackupConfig:
    """Scheduled backup of Nerve state to a local directory.

    Opt-in: set ``target_dir`` to an external mount or a synced directory
    (the off-box copy is what protects against a disk failure) and flip
    ``enabled`` on. A bundle is a single ``nerve-backup-<host>-<ts>.tar.zst``
    file produced by :mod:`nerve.backup`. The scheduled task notifies on
    failure (silent backups that fail are worse than none).
    """

    enabled: bool = False            # opt-in; set target_dir first
    target_dir: str = ""             # e.g. /mnt/backup/nerve or a synced dir
    interval_hours: int = 24
    retention_count: int = 7
    include_workspace: bool = True
    workspace_excludes: list[str] = field(default_factory=list)  # extra globs
    notify_on_failure: bool = True   # high-priority notify
    notify_on_success: bool = False  # low-priority digest line

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> BackupConfig:
        return cls(
            enabled=d.get("enabled", False),
            target_dir=_setting_str(d.get("target_dir")),
            interval_hours=d.get("interval_hours", 24),
            retention_count=d.get("retention_count", 7),
            include_workspace=d.get("include_workspace", True),
            workspace_excludes=_str_list(d.get("workspace_excludes")),
            notify_on_failure=d.get("notify_on_failure", True),
            notify_on_success=d.get("notify_on_success", False),
        )


@dataclass
class ReviewLoopLegConfig:
    """Engine/model/effort defaults for one review-loop role."""

    engine: str = "claude-workflow"
    model: str = ""    # "" = backend default (agent.model / codex.model)
    effort: str = ""   # "" = source default

    @classmethod
    @_coerced
    def from_dict(cls, d: dict, default_engine: str) -> ReviewLoopLegConfig:
        engine = str(d.get("engine", default_engine))
        if engine not in ("claude-workflow", "codex-ultracode"):
            raise ValueError(
                "review_loop leg engine must be 'claude-workflow' or "
                f"'codex-ultracode', got {engine!r}"
            )
        return cls(
            engine=engine,
            model=d.get("model", ""),
            effort=d.get("effort", ""),
        )


@dataclass
class ReviewLoopConfig:
    """Deterministic implement→verify loops composed of workflow runs.

    An implementer leg works toward a Goal prompt; a verifier leg judges
    the workspace against Verifier criteria and returns a structured
    verdict; the server-side controller iterates until pass or caps
    (iterations, dollars, no-progress). See docs/review-loops.md.
    """

    enabled: bool = True
    implementer: ReviewLoopLegConfig = field(
        default_factory=lambda: ReviewLoopLegConfig(engine="claude-workflow"),
    )
    # Cross-vendor verification by default: a different model family has
    # decorrelated blind spots and no self-preference toward the
    # implementer's work. Falls back to claude/claude when codex is not
    # configured (checked at loop creation, not here).
    verifier: ReviewLoopLegConfig = field(
        default_factory=lambda: ReviewLoopLegConfig(engine="codex-ultracode"),
    )
    max_iterations: int = 3            # per-loop default; hard code ceiling 8
    default_budget_usd: float = 10.0
    min_leg_budget_usd: float = 0.5    # never start a leg with less than this
    verifier_reserve_fraction: float = 0.15  # held back so the last attempt is always verified
    criteria_adoption: str = "no"      # no | ask | auto (verifier-proposed criteria)
    max_new_criteria_per_iteration: int = 3
    max_discovered_criteria: int = 12
    discovery_grace_rounds: int = 2
    auto_reissue_implementer: bool = False  # restart recovery: re-issue interrupted implementer legs
    escalation_reproposals: int = 2    # re-file expired/dismissed decision cards this many times
    verifier_sandbox: str = "workspace-write"  # codex verifier legs (isolation vs test-writes)
    reconcile_interval_seconds: int = 600

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> ReviewLoopConfig:
        adoption = d.get("criteria_adoption", "no")
        if adoption is False:
            adoption = "no"  # YAML parses an unquoted `no` as boolean False
        # Normalized, not coerced: this one is a closed set, so the value is
        # lowered and checked here rather than left to the scalar coercer.
        adoption = str(adoption).lower()
        if adoption not in ("no", "ask", "auto"):
            raise ValueError(
                "review_loop.criteria_adoption must be 'no', 'ask' or "
                f"'auto', got {adoption!r}"
            )
        return cls(
            enabled=d.get("enabled", True),
            implementer=ReviewLoopLegConfig.from_dict(
                d.get("implementer", {}) or {}, "claude-workflow",
            ),
            verifier=ReviewLoopLegConfig.from_dict(
                d.get("verifier", {}) or {}, "codex-ultracode",
            ),
            max_iterations=d.get("max_iterations", 3),
            default_budget_usd=d.get("default_budget_usd", 10.0),
            min_leg_budget_usd=d.get("min_leg_budget_usd", 0.5),
            verifier_reserve_fraction=d.get("verifier_reserve_fraction", 0.15),
            criteria_adoption=adoption,
            max_new_criteria_per_iteration=d.get("max_new_criteria_per_iteration", 3),
            max_discovered_criteria=d.get("max_discovered_criteria", 12),
            discovery_grace_rounds=d.get("discovery_grace_rounds", 2),
            auto_reissue_implementer=d.get("auto_reissue_implementer", False),
            escalation_reproposals=d.get("escalation_reproposals", 2),
            verifier_sandbox=d.get("verifier_sandbox", "workspace-write"),
            reconcile_interval_seconds=d.get("reconcile_interval_seconds", 600),
        )


@dataclass
class WorkflowRunsConfig:
    """Budget-capped multi-agent workflow runs.

    A workflow run wraps a dedicated agent session (Claude harness
    Workflow tool or Codex Ultracode) in a dollar budget enforced from
    Nerve's own usage metering, with run-scoped kill and a durable
    journal directory under ``runs_dir``.
    """

    enabled: bool = True
    # Root for per-run journal dirs: <runs_dir>/<run-id>/{run.json,events.ndjson,result.md}
    runs_dir: Path = field(default_factory=lambda: paths.nerve_path("workflow-runs"))
    # Budget monitor cadence. Spend is re-metered (recorded turn costs +
    # live in-flight estimate) every interval.
    poll_interval_seconds: int = 60
    # Fraction of budget_usd at which a one-time warning notification fires.
    warn_fraction: float = 0.8
    # After a graceful stop request at 100% budget, how long to wait before
    # force-discarding the session's client (which kills its subprocess).
    kill_grace_seconds: int = 30
    # Runs dispatched concurrently; excess queues in status 'pending'.
    # Each running workflow occupies one agent.max_concurrent slot for the
    # duration of its turn — keep this well below that limit.
    max_concurrent_runs: int = 2
    # Whether workflow_run_start may launch runs without a budget. Budget
    # enforcement is the point of this surface, so default is False.
    allow_unbudgeted: bool = False
    # Review loops (implement→verify cycles) — nested feature config.
    review_loop: ReviewLoopConfig = field(default_factory=ReviewLoopConfig)

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> WorkflowRunsConfig:
        # Raw values through: an eager cast here happens before interpolated
        # strings are converted, and `bool("false")` is already True. That
        # matters most for allow_unbudgeted, whose whole job is to refuse
        # runs that would spend without a cap.
        return cls(
            enabled=d.get("enabled", True),
            runs_dir=_expand_path(d.get("runs_dir")) or paths.nerve_path("workflow-runs"),
            poll_interval_seconds=d.get("poll_interval_seconds", 60),
            warn_fraction=d.get("warn_fraction", 0.8),
            kill_grace_seconds=d.get("kill_grace_seconds", 30),
            max_concurrent_runs=d.get("max_concurrent_runs", 2),
            allow_unbudgeted=d.get("allow_unbudgeted", False),
            review_loop=ReviewLoopConfig.from_dict(d.get("review_loop", {}) or {}),
        )


@dataclass
class HouseOfAgentsConfig:
    """Deprecated — houseofagents was retired in favor of workflow runs.

    Kept only so existing ``config.yaml`` files with a ``houseofagents:``
    block keep parsing, and so ``enabled`` can gate visibility of the
    ``hoa_*`` deprecation stub tools (``nerve/agent/tools/handlers/hoa.py``).
    Every other legacy key (default_mode, default_agents, use_cli, ...) is
    ignored. Use ``workflows:`` / the ``workflow_run_*`` tools instead —
    see ``docs/workflow-runs.md``.
    """

    enabled: bool = False

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> HouseOfAgentsConfig:
        return cls(enabled=d.get("enabled", False))


@dataclass
class SessionsConfig:
    archive_after_days: int = 30
    interactive_archive_after_hours: int = 0  # Interactive (web/telegram/…) sessions auto-close after this many idle hours (0 = disabled; opt in via config). Starred sessions are exempt and never auto-close.
    max_sessions: int = 500
    cron_session_mode: str = "per_run"  # "per_run" or "reuse"
    memorize_interval_minutes: int = 30  # Background memorization sweep interval
    sticky_period_minutes: int = 120  # Reuse session if active within this window
    client_idle_timeout_minutes: int = 60  # Auto-disconnect clients idle longer than this (0 = disabled)
    star_project_hook: bool = False  # opt-in; fire an internal agent turn on star/unstar transition

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> SessionsConfig:
        return cls(
            archive_after_days=d.get("archive_after_days", 30),
            interactive_archive_after_hours=d.get("interactive_archive_after_hours", 0),
            max_sessions=d.get("max_sessions", 500),
            cron_session_mode=d.get("cron_session_mode", "per_run"),
            memorize_interval_minutes=d.get("memorize_interval_minutes", 30),
            sticky_period_minutes=d.get("sticky_period_minutes", 120),
            client_idle_timeout_minutes=d.get("client_idle_timeout_minutes", 60),
            star_project_hook=d.get("star_project_hook", False),
        )


@dataclass
class RetentionConfig:
    """Opt-in nerve.db retention: message compaction + telemetry pruning.

    Disabled by default so an upstream merge mutates no existing user's data;
    the operator opts in locally. When enabled, a background pass every
    ``interval_hours`` drops the verbose ``blocks``/``thinking`` JSON of old,
    already-memorized, non-starred, non-active messages (keeping ``content``),
    prunes append-only telemetry + file snapshots older than
    ``retention_days``, and checkpoints the WAL. The file is only shrunk by the
    explicit ``nerve db vacuum`` command (VACUUM takes a write lock).

    ``retention_full_days`` is the message-compaction window (default 30);
    ``retention_days`` is the telemetry/snapshot window (default 90). Both
    ints are clamped ``>= 1``.
    """

    enabled: bool = False
    retention_days: int = 90
    retention_full_days: int = 30
    interval_hours: int = 24

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> RetentionConfig:
        return cls(
            enabled=d.get("enabled", False),
            retention_days=max(1, _lenient_int(d.get("retention_days"), 90)),
            retention_full_days=max(1, _lenient_int(d.get("retention_full_days"), 30)),
            interval_hours=max(1, _lenient_int(d.get("interval_hours"), 24)),
        )


@dataclass
class AuthConfig:
    password_hash: str = ""
    jwt_secret: str = ""
    # Web-session lifetime. This is an *idle* timeout, not a cap on a working
    # session: the gateway slides the token forward on every authenticated
    # request (see gateway/auth.py), so an actively-used browser tab is never
    # logged out mid-work. Only a tab left untouched for the whole window
    # comes back to a password prompt.
    jwt_expiry_hours: int = 720  # 30 days

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> AuthConfig:
        return cls(
            password_hash=d.get("password_hash", ""),
            jwt_secret=d.get("jwt_secret", ""),
            jwt_expiry_hours=max(1, _lenient_int(d.get("jwt_expiry_hours"), 720)),
        )


@dataclass
class NotificationsConfig:
    """Async notification delivery settings."""
    channels: list[str] = field(default_factory=lambda: ["web", "telegram"])
    telegram_chat_id: int | None = None       # Target chat; falls back to first allowed_user
    default_expiry_hours: int = 48            # Auto-expire unanswered questions
    max_redeliveries: int = 3                 # Per-row cap on snooze/re-delivery cycles
    priority_prefixes: dict[str, str] = field(default_factory=lambda: {
        "high": "⚠️ ",
        "urgent": "🚨 ",
    })
    # Output language for <YYYY-MM-DD> / <dow:> placeholders rendered into
    # notification text: "en" (default), "ru", "de". Placeholder *parsing*
    # stays multilingual regardless, since the source a weekday was copied
    # from may be in any language. Unknown values fall back to English.
    date_locale: str = "en"

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> NotificationsConfig:
        return cls(
            channels=d.get("channels", ["web", "telegram"]),
            telegram_chat_id=d.get("telegram_chat_id"),
            default_expiry_hours=d.get("default_expiry_hours", 48),
            max_redeliveries=d.get("max_redeliveries", 3),
            priority_prefixes=d.get("priority_prefixes", {
                "high": "⚠️ ",
                "urgent": "🚨 ",
            }),
            date_locale=str(d.get("date_locale", "en")),
        )


@dataclass
class ChannelsConfig:
    """Global channel settings."""

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> ChannelsConfig:
        return cls()


@dataclass
class DockerConfig:
    """Docker deployment settings."""

    extra_mounts: list[str] = field(default_factory=list)  # e.g. ["~/code:/code"]

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> DockerConfig:
        return cls(
            extra_mounts=d.get("extra_mounts", []),
        )


@dataclass
class ProxyConfig:
    """CLIProxyAPI — optional local proxy for routing API calls through Claude Code OAuth."""

    enabled: bool = False
    port: int = 8317
    host: str = "127.0.0.1"
    binary_path: Path = field(default_factory=lambda: paths.nerve_path("bin", "cli-proxy-api"))
    auth_dir: Path = field(default_factory=lambda: paths.nerve_path("cli-proxy-auth"))
    api_key: str = "sk-nerve-local-proxy"   # local-only auth between Nerve and the proxy
    log_file: Path = field(default_factory=lambda: paths.nerve_path("proxy.log"))

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> ProxyConfig:
        return cls(
            enabled=d.get("enabled", False),
            port=d.get("port", 8317),
            host=d.get("host", "127.0.0.1"),
            binary_path=_expand_path(d.get("binary_path")) or paths.nerve_path("bin", "cli-proxy-api"),
            auth_dir=_expand_path(d.get("auth_dir")) or paths.nerve_path("cli-proxy-auth"),
            api_key=d.get("api_key", "sk-nerve-local-proxy"),
            log_file=_expand_path(d.get("log_file")) or paths.nerve_path("proxy.log"),
        )


@dataclass
class OllamaConfig:
    """Local Ollama server — exposes its models as selectable chat models.

    Ollama speaks an OpenAI-compatible API (``/v1``), not the Anthropic
    Messages API the Claude Agent SDK uses. So Ollama models are routed
    through the bundled CLIProxyAPI, which translates Anthropic ↔ OpenAI
    and is registered with Ollama as an ``openai-compatibility`` upstream.

    Requirement: this only takes effect when the proxy is also enabled
    (``proxy.enabled: true``) — the proxy is the translation layer. When
    ``enabled`` is true but the proxy is off, Ollama models are not offered
    (a warning is logged at startup).

    Models are auto-discovered at runtime from Ollama's native
    ``GET /api/tags`` endpoint, so whatever you have pulled locally shows
    up in the model picker with no extra config.
    """

    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 11434

    @property
    def base_url(self) -> str:
        """Native Ollama base URL (used for ``/api/tags`` discovery)."""
        return f"http://{self.host}:{self.port}"

    @property
    def openai_base_url(self) -> str:
        """OpenAI-compatible base URL (registered as a proxy upstream)."""
        return f"http://{self.host}:{self.port}/v1"

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> OllamaConfig:
        return cls(
            enabled=d.get("enabled", False),
            host=d.get("host", "127.0.0.1"),
            port=d.get("port", 11434),
        )


@dataclass
class McpEndpointConfig:
    """Nerve's own MCP server endpoint (Nerve-as-MCP-server).

    Exposes the Nerve tool registry to external MCP clients (Codex,
    Claude Code, Cursor) over Streamable HTTP, mounted at ``path`` inside
    the gateway. Off by default; flip ``enabled=true`` in config.local.yaml
    to advertise the endpoint. Authenticates with the existing JWT
    (``config.auth.jwt_secret``) — same token mechanism as the web UI.

    Not to be confused with :class:`McpServerConfig`, which configures
    *external* MCP servers that Nerve connects to as a client.
    """

    enabled: bool = False
    path: str = "/mcp/v1"
    include_hoa: bool = False   # Expose the deprecated hoa_* stub tools to external clients

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> McpEndpointConfig:
        return cls(
            enabled=d.get("enabled", False),
            path=str(d.get("path", "/mcp/v1")),
            include_hoa=d.get("include_hoa", False),
        )


@dataclass
class ExternalAgentTargetConfig:
    """One configured external agent (Codex, Claude Code, ...).

    Populated by the bootstrap wizard's ``_step_external_agents`` step
    and read by :class:`nerve.external_agents.sync_service.SyncService`
    every interval to keep the agent's memory files in sync with the
    workspace identity files.

    Bearer credentials are deliberately not persisted here. A legacy
    ``token`` field is accepted on read for compatibility, discarded, and
    omitted from every subsequent write.
    """

    name: str                                  # registry key: "codex" | "claude-code" | ...
    enabled: bool = True
    token: str = ""                            # deprecated; never persisted

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> ExternalAgentTargetConfig:
        return cls(
            name=str(d.get("name", "")),
            enabled=d.get("enabled", True),
            token="",
        )

    def to_dict(self) -> dict:
        return {"name": self.name, "enabled": self.enabled}


@dataclass
class ExternalAgentsConfig:
    """Configuration for the external-agents bootstrap + sync subsystem.

    The bootstrap wizard writes one :class:`ExternalAgentTargetConfig`
    per agent selected, plus the global conflict policy chosen for
    pre-existing files. The sync service iterates ``targets`` every
    ``sync_interval_minutes`` and re-renders that agent's memory
    bundle when any source file changes.

    ``conflict_policy`` controls how :class:`nerve.external_agents.writer.ConfigWriter`
    handles paths that already exist when the wizard's apply step runs:
    ``backup`` (default) saves a ``.nerve-backup-<ts>`` copy then
    overwrites; ``skip`` leaves the existing file alone; ``merge`` is
    only meaningful for JSON files (used by Claude Code's settings.json).
    """

    enabled: bool = True
    sync_interval_minutes: int = 15
    conflict_policy: str = "backup"            # "backup" | "skip" | "merge"
    targets: list[ExternalAgentTargetConfig] = field(default_factory=list)

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> ExternalAgentsConfig:
        raw_targets = d.get("targets", [])
        targets: list[ExternalAgentTargetConfig] = []
        if isinstance(raw_targets, list):
            for raw in raw_targets:
                if isinstance(raw, dict) and raw.get("name"):
                    targets.append(ExternalAgentTargetConfig.from_dict(raw))
        return cls(
            enabled=d.get("enabled", True),
            sync_interval_minutes=d.get("sync_interval_minutes", 15),
            conflict_policy=str(d.get("conflict_policy", "backup")),
            targets=targets,
        )


_CODEX_APPROVAL_POLICIES = ("never", "on-request", "untrusted")
_CODEX_SANDBOX_MODES = ("read-only", "workspace-write", "danger-full-access")

# $/1M tokens; cached input bills at the discounted rate. Config values
# under codex.pricing REPLACE entries per model key (dict deep-merge).
_DEFAULT_CODEX_PRICING: dict[str, dict[str, float]] = {
    "gpt-5.6-sol":    {"input": 5.0,  "cached_input": 0.5,  "output": 30.0},
    "gpt-5.6-terra":  {"input": 2.5,  "cached_input": 0.25, "output": 15.0},
    "gpt-5.6-luna":   {"input": 1.0,  "cached_input": 0.1,  "output": 6.0},
}


@dataclass
class UltracodeConfig:
    """Managed Ultracode plugin inside Nerve's isolated Codex home."""

    enabled: bool = False
    auto_install: bool = True
    repository: str = "https://github.com/just-every/plugin-ultracode.git"
    # Reviewed upstream revision. Upgrades are explicit config/code changes;
    # the plugin's own daily marketplace refresh is always disabled.
    revision: str = "9dde0086e983413016bf62ab96ba6bb17b599fae"
    version: str = "0.3.0+codex.20260601143116"
    # Expose completed and in-flight journals through Nerve's authenticated,
    # read-only dashboard.  This is deliberately separate from ``ui`` below:
    # upstream's detached Node server has unauthenticated mutation endpoints.
    dashboard: bool = False
    ui: bool = False
    default_transport: str = "exec"
    max_concurrency: int = 2
    default_token_budget: int = 250_000
    max_agents: int = 8

    @classmethod
    @_coerced
    def from_dict(cls, raw: dict | None) -> "UltracodeConfig":
        d = raw or {}
        return cls(
            enabled=d.get("enabled", False),
            auto_install=d.get("auto_install", True),
            repository=str(d.get("repository") or cls.repository),
            revision=str(d.get("revision") or cls.revision),
            version=str(d.get("version") or cls.version),
            dashboard=d.get("dashboard", False),
            ui=d.get("ui", False),
            default_transport=str(d.get("default_transport") or "exec"),
            max_concurrency=_lenient_int(d.get("max_concurrency"), 2),
            default_token_budget=_lenient_int(
                d.get("default_token_budget"), 250_000,
            ),
            max_agents=_lenient_int(d.get("max_agents"), 8),
        )

    def validate(self) -> list[str]:
        problems: list[str] = []
        if not re.fullmatch(r"[0-9a-f]{40}", self.revision):
            problems.append("codex.ultracode.revision must be a pinned 40-char git SHA")
        if self.default_transport not in ("exec", "app-server"):
            problems.append(
                "codex.ultracode.default_transport must be 'exec' or 'app-server'"
            )
        if not 1 <= self.max_concurrency <= 16:
            problems.append("codex.ultracode.max_concurrency must be in [1, 16]")
        if self.default_token_budget < 1:
            problems.append("codex.ultracode.default_token_budget must be positive")
        if not 1 <= self.max_agents <= 1000:
            problems.append("codex.ultracode.max_agents must be in [1, 1000]")
        return problems


@dataclass
class CodexConfig:
    """OpenAI Codex backend (``codex app-server``) settings.

    Active only when ``agent.backend`` / ``agent.cron_backend`` is
    "codex" (or a session was created on it). See
    docs/plans/codex-backend.md.
    """

    bin_path: str = "codex"                 # PATH-resolved codex binary
    min_version: str = "0.144.1"            # inclusive tested protocol range
    max_version: str = "0.145.0"            # exclusive
    home_dir: str = field(default_factory=lambda: str(paths.nerve_path("codex")))  # isolated CODEX_HOME (auth/config/sessions)
    model: str = "gpt-5.6-sol"
    cron_model: str = ""                    # empty → model
    auth: str = "chatgpt"                   # chatgpt | api_key
    api_key: str = ""                       # literal key (config.local.yaml)
    api_key_env: str = "OPENAI_API_KEY"     # env fallback when auth=api_key
    sandbox: str = "danger-full-access"     # read-only | workspace-write | danger-full-access
    approval_policy: str = "never"          # never | on-request | untrusted
    # nerve effort vocabulary -> codex reasoning effort string
    effort_map: dict[str, str] = field(default_factory=lambda: {
        "max": "ultra", "ultra": "ultra", "xhigh": "xhigh", "high": "high",
        "medium": "medium", "low": "low",
    })
    web_search: bool = True
    tool_timeout_sec: int = 3600            # nerve MCP calls may block on ask_user
    # Per-notification hang detection; 0/empty → agent.cli_idle_timeout_seconds
    turn_idle_timeout_seconds: int = 0
    pricing: dict[str, dict[str, float]] = field(
        default_factory=lambda: {k: dict(v) for k, v in _DEFAULT_CODEX_PRICING.items()},
    )
    # Arbitrary codex config-override passthrough (-c key=value at spawn)
    extra_config: dict[str, Any] = field(default_factory=dict)
    ultracode: UltracodeConfig = field(default_factory=UltracodeConfig)

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> "CodexConfig":
        pricing = {k: dict(v) for k, v in _DEFAULT_CODEX_PRICING.items()}
        raw_pricing = d.get("pricing") or {}
        if isinstance(raw_pricing, dict):
            for model_key, prices in raw_pricing.items():
                if not isinstance(prices, dict):
                    continue
                try:
                    pricing[str(model_key)] = {
                        str(k): float(v) for k, v in prices.items()
                    }
                except (TypeError, ValueError):
                    # Lenient here so a malformed INACTIVE codex section
                    # can't brick startup; the entry is dropped (cost
                    # records None) and flagged.
                    logger.warning(
                        "Ignoring malformed codex.pricing entry %r", model_key,
                    )
        effort_map = {
            "max": "ultra", "ultra": "ultra", "xhigh": "xhigh", "high": "high",
            "medium": "medium", "low": "low",
        }
        raw_effort = d.get("effort_map") or {}
        if isinstance(raw_effort, dict):
            effort_map.update({str(k): str(v) for k, v in raw_effort.items()})
        return cls(
            bin_path=str(d.get("bin_path", "codex")),
            min_version=str(d.get("min_version", "0.144.1")),
            max_version=str(d.get("max_version", "0.145.0")),
            home_dir=_setting_str(
                d.get("home_dir"), str(paths.nerve_path("codex"))
            ),
            model=str(d.get("model", "gpt-5.6-sol")),
            cron_model=str(d.get("cron_model") or ""),
            auth=str(d.get("auth", "chatgpt")).strip().lower(),
            api_key=str(d.get("api_key") or ""),
            api_key_env=str(d.get("api_key_env", "OPENAI_API_KEY")),
            sandbox=str(d.get("sandbox", "danger-full-access")),
            approval_policy=str(d.get("approval_policy", "never")),
            effort_map=effort_map,
            web_search=d.get("web_search", True),
            tool_timeout_sec=_lenient_int(d.get("tool_timeout_sec"), 3600),
            turn_idle_timeout_seconds=_lenient_int(
                d.get("turn_idle_timeout_seconds"), 0,
            ),
            pricing=pricing,
            extra_config=dict(d.get("extra_config") or {}),
            ultracode=UltracodeConfig.from_dict(d.get("ultracode")),
        )

    def validate(self) -> list[str]:
        """Config-load-time validation; returns human-readable problems."""
        problems: list[str] = []
        if self.auth not in ("chatgpt", "api_key"):
            problems.append(
                f"codex.auth must be 'chatgpt' or 'api_key', got {self.auth!r}"
            )
        if self.approval_policy not in _CODEX_APPROVAL_POLICIES:
            problems.append(
                f"codex.approval_policy must be one of "
                f"{_CODEX_APPROVAL_POLICIES}, got {self.approval_policy!r} "
                "(note: 'on-failure' is not accepted by the app-server v2 API)"
            )
        if self.sandbox not in _CODEX_SANDBOX_MODES:
            problems.append(
                f"codex.sandbox must be one of {_CODEX_SANDBOX_MODES}, "
                f"got {self.sandbox!r}"
            )
        problems.extend(self.ultracode.validate())
        return problems


@dataclass
class McpServerConfig:
    """External MCP server configuration.

    Supports stdio (command + args + env), SSE (url + headers),
    and HTTP (url + headers) transports.  Dict-based YAML format
    allows _deep_merge to correctly overlay secrets from config.local.yaml.
    """

    name: str
    type: str = "stdio"                                    # stdio | sse | http
    enabled: bool = True
    # stdio fields
    command: str = ""
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    # sse / http fields
    url: str = ""
    headers: dict[str, str] = field(default_factory=dict)

    @classmethod
    @_coerced
    def from_dict(cls, name: str, d: dict) -> McpServerConfig:
        return cls(
            name=name,
            type=d.get("type", "stdio"),
            enabled=d.get("enabled", True),
            command=d.get("command", ""),
            args=d.get("args", []),
            env=d.get("env", {}),
            url=d.get("url", ""),
            headers=d.get("headers", {}),
        )

    def to_sdk_config(self) -> dict:
        """Convert to Claude Agent SDK McpServerConfig dict."""
        if self.type == "stdio":
            cfg: dict = {"command": self.command}
            if self.args:
                cfg["args"] = self.args
            if self.env:
                cfg["env"] = self.env
            return cfg
        elif self.type in ("sse", "http"):
            cfg = {"type": self.type, "url": self.url}
            if self.headers:
                cfg["headers"] = self.headers
            return cfg
        raise ValueError(f"Unknown MCP server type: {self.type}")


def _parse_mcp_servers(d: dict) -> list[McpServerConfig]:
    """Parse the mcp_servers dict from merged YAML config."""
    raw = d.get("mcp_servers", {})
    if not isinstance(raw, dict):
        return []
    return [McpServerConfig.from_dict(name, cfg) for name, cfg in raw.items()
            if isinstance(cfg, dict)]


def _get_enabled_claude_code_plugins(
    claude_dir: Path | None = None,
) -> list[tuple[str, Path]]:
    """Find enabled Claude Code plugin directories.

    Returns list of (plugin_key, plugin_dir) tuples for each enabled plugin
    that has a cached installation with .mcp.json.
    """
    if claude_dir is None:
        claude_dir = Path.home() / ".claude"

    settings_path = claude_dir / "settings.json"
    if not settings_path.exists():
        return []

    try:
        settings = json.loads(settings_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.debug("Could not read Claude Code settings: %s", e)
        return []

    enabled_plugins: dict = settings.get("enabledPlugins", {})
    if not isinstance(enabled_plugins, dict):
        return []

    plugins_dir = claude_dir / "plugins"
    result: list[tuple[str, Path]] = []

    for plugin_key, is_enabled in enabled_plugins.items():
        if not is_enabled:
            continue

        # Key format: "name@marketplace"
        parts = plugin_key.split("@", 1)
        if len(parts) != 2:
            logger.debug("Skipping malformed plugin key: %s", plugin_key)
            continue
        name, marketplace = parts

        plugin_dir = _find_plugin_dir(plugins_dir, marketplace, name)
        if plugin_dir is None:
            logger.debug("No plugin dir found for %s", plugin_key)
            continue

        result.append((plugin_key, plugin_dir))

    return result


def load_claude_code_plugins(
    claude_dir: Path | None = None,
) -> list[dict[str, str]]:
    """Return SDK-compatible plugin configs for enabled Claude Code plugins.

    Each entry is ``{"type": "local", "path": "<dir>"}`` suitable for
    ``ClaudeAgentOptions.plugins``.
    """
    plugins = _get_enabled_claude_code_plugins(claude_dir)
    result: list[dict[str, str]] = []
    for plugin_key, plugin_dir in plugins:
        logger.debug("Claude Code plugin %s → %s", plugin_key, plugin_dir)
        result.append({"type": "local", "path": str(plugin_dir)})
    return result


def _find_plugin_dir(
    plugins_dir: Path, marketplace: str, name: str,
) -> Path | None:
    """Locate the directory of a Claude Code plugin.

    Checks cache/ (installed plugins with versioned dirs) first,
    then falls back to marketplaces/ (external plugin definitions).
    """
    # Cache: ~/.claude/plugins/cache/<marketplace>/<name>/<version>/
    cache_dir = plugins_dir / "cache" / marketplace / name
    if cache_dir.is_dir():
        versions = sorted(
            (d for d in cache_dir.iterdir() if d.is_dir()),
            key=lambda d: d.name,
            reverse=True,
        )
        for v in versions:
            if (v / ".mcp.json").exists():
                return v

    # Marketplace: external_plugins/<name>/
    ext_dir = plugins_dir / "marketplaces" / marketplace / "external_plugins" / name
    if (ext_dir / ".mcp.json").exists():
        return ext_dir

    # Marketplace: plugins/<name>/
    plugin_dir = plugins_dir / "marketplaces" / marketplace / "plugins" / name
    if (plugin_dir / ".mcp.json").exists():
        return plugin_dir

    return None


_DEFAULT_LANGFUSE_REDACT_PATTERNS: tuple[str, ...] = (
    r"sk-ant-[A-Za-z0-9_\-]{20,}",
    r"pk-lf-[A-Za-z0-9_\-]{20,}",
    r"sk-lf-[A-Za-z0-9_\-]{20,}",
    r"\$2[aby]\$\d{2}\$[./A-Za-z0-9]{53}",
)


@dataclass
class LangfuseConfig:
    """Langfuse observability — optional. Activated by setting both keys.

    With ``public_key`` and ``secret_key`` configured Nerve traces the agent
    loop and memU LLM calls into the Langfuse project pointed at by ``host``.
    Empty keys = no-op, zero overhead, no SDK calls.
    """

    public_key: str = ""
    secret_key: str = ""
    host: str = "https://cloud.langfuse.com"
    redact_patterns: list[str] = field(
        default_factory=lambda: list(_DEFAULT_LANGFUSE_REDACT_PATTERNS),
    )

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> "LangfuseConfig":
        # A bare ``redact_patterns:`` parses to None, which the shared list
        # handling reads as the empty list. That is the right answer for most
        # fields and the wrong one here, because it would turn secret redaction
        # off on a blank line. An explicit ``[]`` still means off. The default
        # is materialized as a ``list`` so the field arrives as its declared
        # type — handing the tuple through made ``coerce_scalars`` warn about
        # a "non-list config value" on every load of a config that never
        # mentions Langfuse.
        patterns = d.get("redact_patterns")
        if patterns is None:
            patterns = list(_DEFAULT_LANGFUSE_REDACT_PATTERNS)
        return cls(
            public_key=d.get("public_key", ""),
            secret_key=d.get("secret_key", ""),
            host=d.get("host", "https://cloud.langfuse.com"),
            redact_patterns=_str_list(patterns),
        )


@dataclass
class XmemoryConfig:
    """xmemory.ai structured memory — optional, runs alongside memU.

    Activated only when both ``api_key`` (the bearer token) and
    ``instance_id`` are set. When active, the ``memorize`` tool dual-writes
    to xmemory (async) and ``memory_recall`` appends xmemory's synthesized
    answer to the memU results. The memorization sweep stays memU-only
    unless ``index_conversations`` is additionally set: then every message
    window the sweep indexes into memU is also mirrored to xmemory as a
    text-only transcript (role + content only — thinking and tool
    blocks/results are never sent).

    Empty keys = no-op, zero overhead, no SDK calls. The instance and its
    schema are created out of band (by the operator) on xmemory's side.
    """

    api_key: str = ""
    instance_id: str = ""
    api_url: str = "https://api.xmemory.ai"
    extraction_logic: str = "deep"  # "deep" (default) or "fast"
    read_mode: str = "single-answer"  # "single-answer" | "raw-tables" | "xresponse"
    timeout: float = 60.0
    # Opt-in: mirror the memorization sweep's session transcripts to xmemory.
    # Off by default — full transcripts leave the machine only when the
    # operator explicitly enables it.
    index_conversations: bool = False

    @property
    def enabled(self) -> bool:
        """True only when both the token and an instance are configured."""
        return bool(self.api_key and self.instance_id)

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> "XmemoryConfig":
        return cls(
            api_key=d.get("api_key", ""),
            instance_id=d.get("instance_id", ""),
            api_url=d.get("api_url", "https://api.xmemory.ai"),
            extraction_logic=d.get("extraction_logic", "deep"),
            read_mode=d.get("read_mode", "single-answer"),
            timeout=d.get("timeout", 60.0),
            index_conversations=d.get("index_conversations", False),
        )


@dataclass
class NerveConfig:
    workspace: Path = field(default_factory=paths.default_workspace)
    timezone: str = "America/New_York"
    # Remote-only, read-only mode. When set (in workspace/config/settings.yaml —
    # the tracked file the remote controls), config comes ONLY from the workspace
    # + ${ENV_VAR}; machine config.yaml/config.local.yaml overrides and legacy
    # ~/.nerve/cron are ignored, and runtime edits to tracked config are blocked.
    lockdown: bool = False
    deployment: str = "server"            # "server" or "docker"
    quiet_start: str = "02:00"            # HH:MM — start of quiet period (local timezone)
    quiet_end: str = "08:00"              # HH:MM — end of quiet period (local timezone)
    provider: ProviderConfig = field(default_factory=ProviderConfig)
    gateway: GatewayConfig = field(default_factory=GatewayConfig)
    agent: AgentConfig = field(default_factory=AgentConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    sync: SyncConfig = field(default_factory=SyncConfig)
    memory: MemoryConfig = field(default_factory=MemoryConfig)
    cron: CronConfig = field(default_factory=CronConfig)
    workspace_sync: WorkspaceSyncConfig = field(default_factory=WorkspaceSyncConfig)
    backup: BackupConfig = field(default_factory=BackupConfig)
    sessions: SessionsConfig = field(default_factory=SessionsConfig)
    retention: RetentionConfig = field(default_factory=RetentionConfig)
    auth: AuthConfig = field(default_factory=AuthConfig)
    channels: ChannelsConfig = field(default_factory=ChannelsConfig)
    notifications: NotificationsConfig = field(default_factory=NotificationsConfig)
    docker: DockerConfig = field(default_factory=DockerConfig)
    proxy: ProxyConfig = field(default_factory=ProxyConfig)
    ollama: OllamaConfig = field(default_factory=OllamaConfig)
    codex: CodexConfig = field(default_factory=CodexConfig)
    workflows: WorkflowRunsConfig = field(default_factory=WorkflowRunsConfig)
    houseofagents: HouseOfAgentsConfig = field(default_factory=HouseOfAgentsConfig)
    langfuse: LangfuseConfig = field(default_factory=LangfuseConfig)
    xmemory: XmemoryConfig = field(default_factory=XmemoryConfig)
    mcp_endpoint: McpEndpointConfig = field(default_factory=McpEndpointConfig)
    mcp_servers: list[McpServerConfig] = field(default_factory=list)
    external_agents: ExternalAgentsConfig = field(default_factory=ExternalAgentsConfig)

    # API keys (from config.local.yaml)
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    brave_search_api_key: str = ""

    # Where this config was loaded from (set by load_config, not a YAML key).
    # Used by anything that needs to write back (e.g. Telegram pairing
    # persisting allowed_users to config.local.yaml).
    config_dir: Path = field(default_factory=Path.cwd)

    @property
    def anthropic_api_base_url(self) -> str:
        """Effective Anthropic API base URL — proxy or direct."""
        if self.provider.is_bedrock:
            return ""  # Bedrock doesn't use Anthropic base URL
        if self.proxy.enabled:
            return f"http://{self.proxy.host}:{self.proxy.port}/v1/"
        return "https://api.anthropic.com/v1/"

    @property
    def effective_api_key(self) -> str:
        """Effective API key — proxy's local key or real Anthropic key."""
        if self.provider.is_bedrock:
            return ""  # Bedrock uses IAM, not API keys
        if self.proxy.enabled:
            return self.proxy.api_key
        return self.anthropic_api_key

    @property
    def ollama_routable(self) -> bool:
        """True when Ollama models can actually be served.

        Requires both Ollama enabled and the proxy running (the proxy is
        the Anthropic↔OpenAI translation layer Ollama is reached through).
        """
        return self.ollama.enabled and self.proxy.enabled

    def selectable_claude_models(
        self, discovered: list[str] | None = None,
    ) -> list[str]:
        """Selectable Claude chat models for the composer's model picker.

        The configured default (``agent.model``) always leads; the rest come
        from the first source that has anything to say:

        1. ``agent.models`` — an explicit list always wins,
        2. *discovered* — what the Anthropic Models API reports the
           credentials can reach (see :mod:`nerve.models_catalog`),
        3. the built-in :data:`DEFAULT_CLAUDE_MODELS` list.

        Bedrock model IDs are region-prefixed, so neither discovery nor the
        bare built-ins apply there — Bedrock offers only configured models.
        """
        if self.agent.models:
            extras = list(self.agent.models)
        elif discovered:
            extras = list(discovered)
        elif self.provider.is_bedrock:
            extras = []
        else:
            extras = list(DEFAULT_CLAUDE_MODELS)

        ordered: list[str] = []
        for m in (self.agent.model, *extras):
            if m and m not in ordered:
                ordered.append(m)
        return ordered

    @property
    def claude_models(self) -> list[str]:
        """Config-only view of :meth:`selectable_claude_models` (no discovery)."""
        return self.selectable_claude_models()

    def create_anthropic_client(self, timeout: float = 60.0) -> Any:
        """Create an Anthropic client based on the configured provider.

        Returns AnthropicBedrock when provider is "bedrock", otherwise
        a standard Anthropic client using the effective API key and base URL.
        """
        import anthropic

        if self.provider.is_bedrock:
            from anthropic import AnthropicBedrock
            kwargs: dict[str, Any] = {"timeout": timeout}
            if self.provider.aws_region:
                kwargs["aws_region"] = self.provider.aws_region
            if self.provider.aws_profile:
                kwargs["aws_profile"] = self.provider.aws_profile
            if self.provider.aws_access_key_id:
                kwargs["aws_access_key"] = self.provider.aws_access_key_id
                kwargs["aws_secret_key"] = self.provider.aws_secret_access_key
            return AnthropicBedrock(**kwargs)

        # Default: direct Anthropic API (or proxy)
        base_url = self.anthropic_api_base_url.rstrip("/")
        if base_url.endswith("/v1"):
            base_url = base_url[:-3]
        return anthropic.Anthropic(
            api_key=self.effective_api_key,
            base_url=base_url,
            timeout=timeout,
        )

    def create_async_anthropic_client(self, timeout: float = 60.0) -> Any:
        """Create an async Anthropic client based on the configured provider.

        Returns AsyncAnthropicBedrock when provider is "bedrock", otherwise
        a standard AsyncAnthropic client.
        """
        import anthropic

        if self.provider.is_bedrock:
            from anthropic import AsyncAnthropicBedrock
            kwargs: dict[str, Any] = {"timeout": timeout}
            if self.provider.aws_region:
                kwargs["aws_region"] = self.provider.aws_region
            if self.provider.aws_profile:
                kwargs["aws_profile"] = self.provider.aws_profile
            if self.provider.aws_access_key_id:
                kwargs["aws_access_key"] = self.provider.aws_access_key_id
                kwargs["aws_secret_key"] = self.provider.aws_secret_access_key
            return AsyncAnthropicBedrock(**kwargs)

        base_url = self.anthropic_api_base_url.rstrip("/")
        if base_url.endswith("/v1"):
            base_url = base_url[:-3]
        return anthropic.AsyncAnthropic(
            api_key=self.effective_api_key,
            base_url=base_url,
            timeout=timeout,
        )

    _KNOWN_BACKENDS = ("claude", "codex")

    def _validate_backend_config(self) -> None:
        """Fail fast on unusable backend settings (called from from_dict).

        Unknown backend names are hard errors — a typo here would
        otherwise surface as a confusing per-session failure. Codex
        sub-config problems are hard errors only when a codex backend is
        actually selected; otherwise the section is inert.
        """
        for label, name in (
            ("agent.backend", self.agent.backend),
            ("agent.cron_backend", self.agent.resolved_cron_backend),
        ):
            if name not in self._KNOWN_BACKENDS:
                raise ValueError(
                    f"{label} must be one of {self._KNOWN_BACKENDS}, got {name!r}"
                )
        codex_selected = "codex" in (
            self.agent.backend, self.agent.resolved_cron_backend,
        )
        problems = self.codex.validate()
        if problems:
            if codex_selected:
                raise ValueError("; ".join(problems))
            for p in problems:
                logger.warning("Inactive codex config problem: %s", p)
        if codex_selected:
            if self.codex.model not in {
                k for k in self.codex.pricing
            } and not any(
                key.lower() in self.codex.model.lower()
                for key in self.codex.pricing
            ):
                logger.warning(
                    "codex.model %r has no codex.pricing entry — turn costs "
                    "will be recorded as unknown (tokens still tracked)",
                    self.codex.model,
                )
            if self.ollama.enabled:
                logger.warning(
                    "agent.backend=codex with ollama.enabled: Ollama models "
                    "cannot be served by the codex backend; sessions "
                    "explicitly selecting an Ollama model will fail",
                )

    @classmethod
    @_coerced
    def from_dict(cls, d: dict) -> NerveConfig:
        config = cls._build_from_dict(d)
        config._validate_backend_config()
        return config

    @classmethod
    def _build_from_dict(cls, d: dict) -> NerveConfig:
        # Resolve the workspace once so workspace-aware sub-configs (e.g. cron,
        # which now lives in workspace/config/cron) can be located relative to it.
        workspace = _expand_path(d.get("workspace")) or paths.default_workspace()
        # Parsed here rather than left to @_coerced, which would fall back to the
        # field's default — i.e. to "unlocked" — for a value it cannot read. It is
        # also needed *before* construction, because it changes how the cron paths
        # and the Telegram switch resolve. See :func:`_as_lockdown`.
        locked = _as_lockdown(d.get("lockdown"))
        return cls(
            workspace=workspace,
            timezone=d.get("timezone", "America/New_York"),
            lockdown=locked,
            deployment=d.get("deployment", "server"),
            quiet_start=d.get("quiet_start", "02:00"),
            quiet_end=d.get("quiet_end", "08:00"),
            provider=ProviderConfig.from_dict(d.get("provider", {})),
            gateway=GatewayConfig.from_dict(d.get("gateway", {})),
            agent=AgentConfig.from_dict(d.get("agent", {})),
            telegram=TelegramConfig.from_dict(d.get("telegram", {}), locked=locked),
            sync=SyncConfig.from_dict(d.get("sync", {})),
            memory=MemoryConfig.from_dict(d.get("memory", {})),
            cron=CronConfig.from_dict(d.get("cron", {}), workspace=workspace, locked=locked),
            workspace_sync=WorkspaceSyncConfig.from_dict(d.get("workspace_sync", {})),
            backup=BackupConfig.from_dict(d.get("backup", {})),
            sessions=SessionsConfig.from_dict(d.get("sessions", {})),
            retention=RetentionConfig.from_dict(d.get("retention", {})),
            auth=AuthConfig.from_dict(d.get("auth", {})),
            channels=ChannelsConfig.from_dict(d.get("channels", {})),
            notifications=NotificationsConfig.from_dict(d.get("notifications", {})),
            docker=DockerConfig.from_dict(d.get("docker", {})),
            proxy=ProxyConfig.from_dict(d.get("proxy", {})),
            ollama=OllamaConfig.from_dict(d.get("ollama", {})),
            codex=CodexConfig.from_dict(d.get("codex", {})),
            workflows=WorkflowRunsConfig.from_dict(d.get("workflows", {})),
            houseofagents=HouseOfAgentsConfig.from_dict(d.get("houseofagents", {})),
            langfuse=LangfuseConfig.from_dict(d.get("langfuse", {})),
            xmemory=XmemoryConfig.from_dict(d.get("xmemory", {})),
            mcp_endpoint=McpEndpointConfig.from_dict(d.get("mcp_endpoint", {})),
            mcp_servers=_parse_mcp_servers(d),
            external_agents=ExternalAgentsConfig.from_dict(d.get("external_agents", {})),
            anthropic_api_key=d.get("anthropic_api_key", ""),
            openai_api_key=d.get("openai_api_key", ""),
            brave_search_api_key=d.get("brave_search_api_key", ""),
        )


def load_mcp_servers(config_dir: Path | None = None) -> list[McpServerConfig]:
    """Re-read MCP server configs from YAML files.

    Called per session creation and on reload to pick up config changes
    without restarting Nerve.

    Note: Claude Code plugin MCPs are handled separately via the SDK
    ``plugins`` field (--plugin-dir), not through this function.
    """
    if config_dir is None:
        config_dir = Path.cwd()

    merged = _read_config_sources(config_dir)
    return _parse_mcp_servers(merged)


# --- Config directory resolution ---
#
# Nerve commands used to be CWD-sensitive: running `nerve start` from any
# directory other than the install dir silently loaded an empty config and
# reported "fresh install".  Resolution now follows a waterfall so commands
# work from anywhere:
#
#   1. Explicit --config-dir / -c flag
#   2. NERVE_CONFIG_DIR environment variable
#   3. Current directory, if it contains config.yaml or config.local.yaml
#      (preserves the dev workflow of running nerve from a checkout)
#   4. The pointer file ~/.nerve/config_dir (written by `nerve init` and on
#      daemon start), if it names a directory that still has config files
#   5. Current directory (fresh-install fallback)

def _has_config_files(directory: Path) -> bool:
    """True if the directory contains config.yaml or config.local.yaml."""
    try:
        return (directory / "config.yaml").exists() or (
            directory / "config.local.yaml"
        ).exists()
    except OSError:
        return False


def read_config_pointer() -> Path | None:
    """Read the persisted config directory pointer. None if absent/invalid."""
    try:
        raw = paths.config_pointer_file().read_text(encoding="utf-8").strip()
    except (FileNotFoundError, OSError):
        return None
    if not raw:
        return None
    p = Path(raw)
    return p if p.is_dir() else None


def write_config_pointer(config_dir: Path) -> None:
    """Persist the config directory so future commands find it from any CWD.

    Written by `nerve init` (after a successful apply) and on daemon start.
    Best-effort: failure to write must never break the caller.
    """
    pointer = paths.config_pointer_file()
    try:
        pointer.parent.mkdir(parents=True, exist_ok=True)
        pointer.write_text(str(Path(config_dir).expanduser().resolve()), encoding="utf-8")
    except OSError as e:
        logger.warning("Could not write config pointer %s: %s", pointer, e)


def resolve_config_dir(explicit: str | Path | None = None) -> tuple[Path, str]:
    """Resolve the effective config directory.

    Returns (directory, source) where source is one of:
    "flag", "env", "cwd", "pointer", "default".
    """
    if explicit is not None:
        return Path(explicit).expanduser(), "flag"

    env_dir = os.environ.get("NERVE_CONFIG_DIR", "").strip()
    if env_dir:
        return Path(env_dir).expanduser(), "env"

    cwd = Path.cwd()
    if _has_config_files(cwd):
        return cwd, "cwd"

    pointer = read_config_pointer()
    if pointer is not None and _has_config_files(pointer):
        return pointer, "pointer"

    return cwd, "default"


def load_config(config_dir: Path | None = None) -> NerveConfig:
    """Load and type the effective configuration.

    Merges (lowest→highest precedence) ``workspace/config/settings.yaml``
    (shareable, git-tracked), ``config.yaml`` (machine base), and
    ``config.local.yaml`` (machine secrets/overrides), then resolves
    ``${ENV_VAR}`` references. The workspace location is taken from the
    machine-local config (or the default) before the tracked settings file is
    read.

    If config_dir is None, the directory is resolved via the waterfall in
    :func:`resolve_config_dir` (flag/env/cwd/pointer), so commands behave the
    same regardless of the caller's working directory.
    """
    if config_dir is None:
        config_dir, _source = resolve_config_dir()

    # Assemble config from workspace/config/settings.yaml + config.yaml +
    # config.local.yaml (lowest→highest precedence) and resolve ${ENV_VAR} refs.
    merged = _read_config_sources(config_dir)

    # Surface typos and stale keys instead of silently ignoring them.
    for warning in validate_config_keys(merged):
        logger.warning("config: %s", warning)

    config = NerveConfig.from_dict(merged)
    config.config_dir = Path(config_dir)
    problems = lockdown_workspace_problems(config.workspace) if config.lockdown else []
    for problem in problems:
        # Fail closed: don't boot a locked instance that would take its "tracked"
        # config from somewhere the config repo never saw.
        raise ConfigError(problem)
    for note in lockdown_machine_local_notes(
        merged, read_machine_layers(config_dir) if config.lockdown else None
    ):
        logger.warning("config: %s", note)
    return config


def lockdown_workspace_problems(workspace: Path) -> list[str]:
    """Ways a locked instance's reviewed subtrees aren't the trees they claim.

    Everything else here judges a path against ``<workspace>/config`` or
    ``<workspace>/skills``. That settles whether a path stays inside a subtree,
    and says nothing about whether the subtree is the one the config repo was
    reviewed in. If ``config`` is a symlink out of the workspace, every path
    under it is contained relative to itself, every containment check passes, and
    the whole tracked layer — settings.yaml, the cron files, the gate plugins the
    daemon imports — comes from an untracked directory somewhere else on the box,
    with lockdown itself read from there and reporting that all is well. A
    redirected ``skills`` is the same arrangement one directory over: whatever is
    there gets indexed as model-invocable skills on the next reload.

    So each subtree is judged against the workspace instead, once, and a locked
    instance that fails does not start. The workspace *path* may still be
    anything, including a symlink: where a machine keeps its workspace is a
    machine-local decision and the one thing lockdown never took away.

    Two more ways the same claim fails, checked here for the same reason. A
    subtree can stay inside the workspace and still not be the reviewed tree —
    ``config`` as a symlink to a sibling directory — and ``settings.yaml`` alone
    can leave it while everything around it stays put.

    One problem here is not about a subtree at all: whether the workspace has a
    git remote. It belongs with these because it is the same claim from the other
    side — the reviewed surface is only reviewed if reviewed changes can reach it.
    See :func:`_workspace_remote_problem`.

    Every problem found is reported rather than the first: an operator fixing a
    layout wants the whole list, and the caller that refuses to boot only needs
    one of them.
    """
    problems: list[str] = []
    config_root = workspace_config_dir(workspace)
    for reviewed in (Path(workspace) / name for name in REVIEWED_DIRS):
        if not _is_within(reviewed, workspace):
            problems.append(
                f"lockdown is enabled but the reviewed subtree {reviewed} resolves "
                f"to {_resolved(reviewed)}, outside the workspace "
                f"{_resolved(workspace)} — so nothing it holds is part of the "
                f"reviewed repo. A locked instance reads {reviewed.name}/ only "
                f"from the workspace itself."
            )
        elif reviewed.is_symlink():
            # Contained, and still not the reviewed tree. git does not descend
            # into a symlinked directory, so `git status -- <ws>/config` reports
            # nothing whatever is under there: sync calls the workspace clean
            # while the daemon reads settings.yaml, and imports cron/gates/*.py,
            # from a directory the config repo has never seen a single file of.
            # A redirected skills/ is the same arrangement, with the skill index
            # reading whatever the link points at.
            problems.append(
                f"lockdown is enabled but the reviewed subtree {reviewed} is a "
                f"symlink to {_resolved(reviewed)}. git does not track anything "
                f"through a symlinked directory, so no file under it is part of "
                f"the reviewed repo and a sync would report the workspace clean "
                f"regardless of what it holds. A locked instance's "
                f"{reviewed.name}/ must be a real directory."
            )

    # The subtree can be the reviewed tree and this one file still not be part of
    # it. settings.yaml carries the lockdown flag itself, the auth secret and
    # every cron path, so a symlink here is the whole tracked layer arriving from
    # somewhere the reviewer never looked — including from an ordinary workspace
    # file the agent may write, which is why "still inside the workspace" is not
    # good enough. The write guard refuses the name; this refuses the instance,
    # because a file that is already in place was never written through the guard.
    settings = workspace_settings_file(workspace)
    if not _is_within(settings, config_root):
        problems.append(
            f"lockdown is enabled but the tracked settings file {settings} resolves "
            f"to {_resolved(settings)}, outside the tracked config subtree "
            f"{_resolved(config_root)} — so the file that states lockdown, and "
            f"everything else this instance runs on, is not part of the reviewed "
            f"repo. A locked instance reads its settings only from the workspace's "
            f"own config/ directory."
        )

    remote = _workspace_remote_problem(Path(workspace))
    if remote:
        problems.append(remote)
    return problems


def _workspace_remote_problem(workspace: Path) -> str | None:
    """Why this locked workspace has nowhere to receive reviewed config from.

    Lockdown refuses every local change to the reviewed surface and its refusals
    say so, naming the PR flow as the way to change config. That flow ends in a
    git pull: sync is the only route left in. A locked workspace that is not a
    repository, or is one with no remote, has no route at all — every local
    change is refused and no remote change can arrive, so the instance keeps the
    configuration it happens to hold and nothing on the box says why. Nothing
    else reports it, because every other check here judges a tree that is present
    and this one is about a tree that can never change.

    Refused rather than ignored, for the reason the other two rules exist:
    ``lockdown`` in config.yaml/config.local.yaml is not read, and
    ``NERVE_LOCKDOWN`` can lock but never unlock, both so that a machine-local
    change cannot turn lockdown off. ``git remote remove origin`` is a
    machine-local change. Honoring the flag only when a remote happens to be
    configured would make it one more way to unlock the box.

    Any remote counts. Which one to fetch is sync's decision, not this one's:
    with ``workspace_sync.branch`` unset it follows the current branch's own
    upstream, which need not be named ``origin``, so demanding that name would
    refuse a workspace that syncs today. Whether the periodic loop is enabled is
    not asked either — ``nerve config sync`` and ``POST /api/config/sync`` are
    manual routes that work on a locked box with the loop off, so a remote is a
    route regardless.

    Git being unusable is treated as no remote. The answer decides whether a
    locked instance starts, and "could not tell" is not "yes"; a box that cannot
    run git cannot sync either.
    """
    # Imported inside the function because sync_service imports this module. Its
    # git runner is the one the sync route itself uses: captured, timed out and
    # incapable of raising, which is what a call on the config load path needs.
    from nerve.sync_service import _git, is_git_repo

    if not is_git_repo(workspace):
        found = f"the workspace {workspace} is not a git repository"
        remedy = "Clone the config repo as the workspace"
    else:
        listed = _git(["remote"], workspace)
        if listed.returncode == 0 and listed.stdout.strip():
            return None
        if listed.returncode == 0:
            found = f"the workspace repository {workspace} has no git remote"
            remedy = "Add the config repo as a remote (git remote add origin <url>)"
        else:
            found = (
                f"git could not list the remotes of the workspace repository "
                f"{workspace}: {listed.stderr.strip() or listed.stdout.strip()}"
            )
            remedy = "Fix the error git reports"
    return (
        f"lockdown is enabled but {found}, so reviewed config has no way to reach "
        f"this instance. A locked instance refuses every local change to its "
        f"reviewed surface and takes changes only as a merged commit that sync "
        f"pulls in, so it would keep the config it currently holds for good. "
        f"{remedy}, or drop lockdown from the tracked settings."
    )


def _has_dotted(data: dict[str, Any], dotted: str) -> bool:
    """Whether ``dotted`` names a key present in ``data``.

    Presence, not truthiness: ``enabled: false`` and ``accounts: []`` are stated
    values, and reporting them as unstated would be wrong.
    """
    node: Any = data
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return False
        node = node[part]
    return True


def lockdown_machine_local_notes(
    merged: dict[str, Any], machine: dict[str, Any] | None = None
) -> list[str]:
    """Machine-local settings a locked bundle leaves for the defaults to decide.

    Lockdown reads only the tracked settings, so a key whose natural home is
    ``config.yaml`` has nowhere left to be said — and the declared default
    silently answers for it on every box in the fleet. That is invisible in the
    diff of the change that turned lockdown on, which is exactly when someone
    could still do something about it, so name it while the config is being
    loaded and while it is being validated.

    The per-box answer belongs in the tracked settings as an ``${ENV_VAR}``
    reference; that is the only channel a locked instance still has.

    ``machine`` is the dropped ``config.yaml``/``config.local.yaml`` overlay,
    which callers still hold at the point lockdown discards it. Without it this
    function can only report that a key is absent from the tracked settings, and
    most machine-local keys are legitimately absent with an acceptable default —
    so warning on all of them is noise. With it, the note fires only when a local
    file states something that lockdown is about to ignore.
    """
    if not _as_lockdown(merged.get("lockdown")):
        return []
    notes: list[str] = []

    if machine:
        # The list of machine-local paths lives next to the wizard split it
        # mirrors, and is guarded by tests in both directions. Imported inside the
        # function because nerve.migrate imports this module.
        from nerve.migrate import _is_machine_local, _leaf_paths

        # `workspace` is the one key lockdown keeps, so it is never dropped.
        dropped = sorted(
            path
            for path in _leaf_paths(machine)
            if path != "workspace" and not _has_dotted(merged, path)
        )
        shareable = [p for p in dropped if not _is_machine_local(p)]
        local_only = [p for p in dropped if _is_machine_local(p)]
        if shareable:
            notes.append(
                f"{', '.join(shareable)} are set in config.yaml/config.local.yaml"
                " and not in the tracked settings. Lockdown does not read those"
                " files, so this instance is using the declared defaults."
                " These values are shareable: move them to"
                " workspace/config/settings.yaml."
            )
        if local_only:
            notes.append(
                f"{', '.join(local_only)} are machine-local, so lockdown leaves"
                " them at their declared defaults. If a default is not the right"
                " answer for this box, state the key in the tracked settings as"
                " a ${VAR} reference."
            )

    telegram = merged.get("telegram")
    telegram = telegram if isinstance(telegram, dict) else {}
    if telegram.get("bot_token") and "enabled" not in telegram:
        notes.append(
            "workspace/config/settings.yaml sets telegram.bot_token but not "
            "telegram.enabled, and lockdown does not read config.yaml (where "
            "`nerve init` puts it) — so the Telegram bot stays OFF on this "
            "instance. Set telegram.enabled in the tracked settings, using "
            "${VAR} if the answer differs per machine."
        )
    return notes


# --- Unknown-key validation ---

# YAML keys that are intentionally not dataclass fields — keyed by dotted
# prefix ("" is the top level). claude_oauth_token / github_token are read
# from config.local.yaml by the Docker entrypoint, not by NerveConfig.
_EXTRA_ALLOWED_KEYS: dict[str, set[str]] = {
    "": {"claude_oauth_token", "github_token"},
}

# Subtrees we don't descend into: free-form mappings or lists of mappings
# whose schema isn't a nested dataclass.
_OPAQUE_PREFIXES = {
    "mcp_servers",
    "memory.categories",
    "external_agents.targets",
    "docker.extra_mounts",
    "langfuse.redact_patterns",
}


def validate_config_keys(merged: dict) -> list[str]:
    """Compare a merged config dict against the NerveConfig dataclass tree.

    Returns human-readable warnings for keys that no dataclass field will
    ever read (typos, removed options). Warning-only by design — unknown
    keys must not break startup (forward/backward compatibility).
    """
    import dataclasses

    warnings: list[str] = []

    def _walk(d: dict, cls: type, prefix: str) -> None:
        field_map = {f.name: f for f in dataclasses.fields(cls)}
        allowed_extra = _EXTRA_ALLOWED_KEYS.get(prefix, set())
        for key, value in d.items():
            dotted = f"{prefix}.{key}" if prefix else key
            if key not in field_map:
                if key in allowed_extra:
                    continue
                warnings.append(
                    f"unknown key '{dotted}' — it is ignored (typo or removed option?)"
                )
                continue
            if dotted in _OPAQUE_PREFIXES:
                continue
            # Descend into nested dataclasses only
            ftype = field_map[key].type
            nested = _resolve_dataclass(ftype)
            if nested is not None and isinstance(value, dict):
                _walk(value, nested, dotted)

    def _resolve_dataclass(ftype: Any) -> type | None:
        """Map a (possibly string) field annotation to a dataclass type."""
        if isinstance(ftype, type) and dataclasses.is_dataclass(ftype):
            return ftype
        if isinstance(ftype, str):
            candidate = globals().get(ftype)
            if isinstance(candidate, type) and dataclasses.is_dataclass(candidate):
                return candidate
        return None

    _walk(merged, NerveConfig, "")
    return warnings


# --- Write-back helpers ---


def append_telegram_allowed_user(config_dir: Path, user_id: int) -> bool:
    """Append a Telegram user ID to telegram.allowed_users in config.local.yaml.

    Used by the pairing flow. Reads, merges, and rewrites the local config
    (config.local.yaml is generated — comment loss is acceptable there).
    Returns True if the file was updated (False if the ID was already present).

    Blocked under lockdown: config.local.yaml isn't even loaded when locked, and
    tracked config is remote-only — pair by updating the workspace repo instead.
    """
    ensure_not_locked("add a Telegram user")
    local_path = Path(config_dir) / "config.local.yaml"
    data: dict[str, Any] = {}
    if local_path.exists():
        try:
            data = yaml.safe_load(local_path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as e:
            logger.error("Cannot parse %s to persist pairing: %s", local_path, e)
            return False

    telegram = data.setdefault("telegram", {})
    users = telegram.setdefault("allowed_users", [])
    if user_id in users:
        return False
    users.append(user_id)

    with open(local_path, "w", encoding="utf-8") as f:
        f.write("# Nerve — Secrets (gitignored)\n")
        f.write("# API keys, tokens, and other sensitive configuration.\n\n")
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
    try:
        os.chmod(local_path, 0o600)
    except OSError:
        pass
    logger.info("Persisted Telegram user %d to %s", user_id, local_path)
    return True


# Singleton config instance, loaded lazily
_config: NerveConfig | None = None


def get_config() -> NerveConfig:
    """Get the global config instance. Loads from CWD on first call."""
    global _config
    if _config is None:
        _config = load_config()
    return _config


def set_config(config: NerveConfig) -> None:
    """Override the global config (for testing or CLI-driven loading)."""
    global _config
    _config = config


class LockdownError(RuntimeError):
    """Raised when a runtime edit to tracked config is attempted under lockdown."""


def is_locked() -> bool:
    """True if the *loaded* config puts this instance in lockdown.

    A statement about the process, not about the box: with no config loaded there
    is no lockdown state and this answers False. That is a fail-open, and it is
    reachable — the CLI hands ``config=None`` to the commands that exist to repair
    a config which failed to load, and never calls :func:`set_config` on that
    path. Left as it is deliberately, because every caller of the guards below is
    a server-side write path that runs with a loaded config, and making this read
    files would put I/O behind a predicate called on every skill toggle to protect
    a case none of them are in.

    What that does mean is that a guard cannot be *added* by calling this from a
    command on the ``config=None`` path and expecting it to hold. Such a caller
    has to be given the flag explicitly.
    """
    return _config is not None and bool(_config.lockdown)


def ensure_not_locked(action: str = "modify configuration") -> None:
    """Raise :class:`LockdownError` if locked. Call before any runtime edit to
    tracked config (skills, cron, pairing, ...)."""
    if is_locked():
        raise LockdownError(
            f"Cannot {action}: this instance is in lockdown (remote-only, "
            "read-only). Change config via a PR to the workspace repo instead."
        )


def ensure_path_not_tracked_config(path: Path, action: str = "write") -> None:
    """Refuse, under lockdown, to touch a path in the workspace's reviewed surface.

    The other guards sit in front of a specific operation — creating a skill,
    persisting a pairing — and know what they are about to change. This one is
    for the endpoints that take a caller-supplied path anywhere under the
    workspace: whether they are editing tracked config is a property of the
    argument, not of the endpoint. Left unguarded, ``<workspace>/config/`` is
    reachable through them, which means both ``settings.yaml`` (including the
    ``lockdown`` flag itself) and ``cron/gates/*.py``, which the daemon imports
    and runs. Either one turns lockdown off from inside the box it is meant to be
    protecting. ``skills/`` and the root instruction files are reachable the same
    way and decide what the agent does next, so they are judged the same.

    A path outside that surface is none of lockdown's business — the workspace is
    also the agent's working directory, and normal files there are the point.
    """
    reason = tracked_config_write_refusal(path)
    if reason:
        raise LockdownError(f"Cannot {action} {path}: {reason}")


def tracked_config_write_refusal(path: Path | str) -> str | None:
    """Why a locked instance refuses to write ``path``, or ``None`` to allow it.

    The same rule as :func:`ensure_path_not_tracked_config`, phrased as a value
    rather than an exception, because the agent backends express a refusal by
    returning one — a raise there aborts the turn instead of telling the model
    what to do differently. The wording is aimed at the model for that reason:
    it names the alternative, so a capable agent routes to the review flow rather
    than retrying the same write with a different tool.

    A relative path is taken as relative to the workspace, which is the agent's
    working directory. That can in principle over-refuse for an agent that has
    changed directory into a subtree of its own with a ``config/`` in it; erring
    that way costs a clear message and a retry, and erring the other way is the
    hole this exists to close.

    Every member of :func:`workspace_reviewed_paths` is judged, not ``config/``
    alone. ``skills/`` and the root instruction files are refused by the named
    operations already — ``create_skill`` and friends — and leaving them writable
    here left the whole point of those refusals reachable by the plainest tool
    the agent has: write ``skills/x/SKILL.md``, wait for the reload that indexes
    it, and no 403 was ever involved.

    Both views of the path are checked, because a symlink is the case where they
    disagree and each direction of the disagreement is a way through. Resolving
    catches a path that arrives from outside and lands in the surface — a link in
    the workspace pointing at ``config/``, a directory link, a ``..`` climb. The
    lexical name catches the reverse: a symlink sitting *at* a reviewed path
    resolves elsewhere, and a guard that only resolved would decide the write was
    outside the surface and allow it, while the daemon goes on loading that name
    as tracked config.
    """
    if not is_locked():
        return None
    workspace = get_config().workspace
    target = Path(path)
    if not target.is_absolute():
        target = workspace / target
    for reviewed in workspace_reviewed_paths(workspace):
        if _is_within(target, reviewed) or _is_lexically_within(target, reviewed):
            return (
                f"this instance is in lockdown (remote-only, read-only) and "
                f"{target} is part of {reviewed}, which only a reviewed, merged "
                f"change to the workspace repo may alter. Open a pull request "
                f"against that repo instead; the instance will pick the change up "
                f"when it syncs. Memory, task files and the rest of the workspace "
                f"are still writable."
            )
    return None
