"""Central resolver for Nerve's machine-local runtime directories.

Historically, ``~/.nerve/...`` locations were hardcoded as ``Path("~/.nerve/x")``
literals scattered across a dozen modules (config dataclass defaults, the CLI,
the bootstrap wizard, cron gates, the DB, pairing, ...). That made the runtime
state directory impossible to relocate and easy to drift out of sync.

Everything that points at the machine-local state directory now funnels through
this module. Two consequences:

* The location can be overridden with the ``NERVE_HOME`` environment variable
  (defaults to ``~/.nerve``). Handy for tests and for running more than one
  instance on a box.
* There is a single, greppable inventory of what lives under the state dir.

IMPORTANT: this module is for *machine-local runtime state* (databases, tokens,
caches, downloaded binaries, PID/pointer files) — the stuff that is NOT
git-syncable. Git-syncable, shareable configuration lives in the workspace (see
``nerve/workspace.py`` and the ``config/`` subtree), not here.

Keep this module dependency-free (only ``os``/``pathlib``) so that
``nerve.config`` and everything else can import it without a cycle.
"""

from __future__ import annotations

import os
from pathlib import Path

# Environment override for the machine-local state directory.
NERVE_HOME_ENV = "NERVE_HOME"

# Default machine-local state directory name under the user's home.
_DEFAULT_DIRNAME = ".nerve"


def nerve_home() -> Path:
    """The machine-local runtime state directory (default ``~/.nerve``).

    Override with the ``NERVE_HOME`` env var. Returns an absolute, expanded,
    lexically normalized path; the directory is not created here.

    A relative override is anchored to the current working directory at call
    time, so callers always get something absolute. This matters because these
    paths are handed to subprocesses that run with a different ``cwd`` (git
    against the workspace, the proxy binary, the daemon) — a bare relative
    path would silently mean a different directory in each of them.

    Normalization applies to absolute overrides too: ``.`` segments, ``..``
    segments, and duplicate or trailing slashes are all collapsed (POSIX's
    special leading ``//`` aside), so ``/srv/a/../b`` comes back as
    ``/srv/b``. That is deliberate — one directory should mean one set of
    state-file paths no matter how the operator spelled it, otherwise two
    processes comparing paths as strings disagree about whether they share a
    state dir. Two things normalization is *not*:

    * It is not ``realpath``. Symlinks inside the override are left alone, so
      the result is stable but not canonical.
    * Because ``..`` is collapsed textually rather than by walking the tree,
      ``/srv/link/../b`` means ``/srv/b`` even when ``link`` points elsewhere
      — the same rule the shell's ``cd -L`` uses, not the kernel's.

    Two more caveats, since a relative ``NERVE_HOME`` is almost always a
    mistake:

    * The value is *not* cached. Two calls from different working directories
      still disagree, so a relative override is only stable for a process
      that never changes directory, and only shared between invocations
      launched from the same place.
    * ``os.getcwd()`` is canonicalized by the OS, so a relative override
      additionally picks up symlink resolution from the cwd prefix. An
      absolute override never does.
    """
    raw = os.environ.get(NERVE_HOME_ENV, "").strip()
    if raw:
        return Path(os.path.abspath(Path(raw).expanduser()))
    return Path.home() / _DEFAULT_DIRNAME


def nerve_path(*parts: str) -> Path:
    """A path under the machine-local state directory."""
    return nerve_home().joinpath(*parts)


# --- Rendering paths for operator-facing output ------------------------------- #
#
# Use these wherever the program prints a state-dir path back to the operator —
# wizard status lines, doctor findings, commands they are told to run. Writing
# ``~/.nerve`` as a literal in that output was correct before the location was
# overridable and is not now: on an instance that sets NERVE_HOME it names a
# directory Nerve does not use.
#
# Static help text is the exception. ``--help`` describes the command rather
# than this instance, so naming the default location there is accurate.


def home_label() -> str:
    """The state directory as it should appear in operator-facing output.

    ``~/.nerve`` when that is where the state dir resolves to, the absolute path
    when NERVE_HOME moves it. The default case stays short, and the overridden
    case says where the files actually are.

    Derived by comparing against the default rather than by testing whether the
    env var is set, so it agrees with :func:`nerve_home` even when an override
    is spelled some other way — ``NERVE_HOME=~/.nerve`` still prints ``~/.nerve``.
    """
    home = nerve_home()
    if home == Path.home() / _DEFAULT_DIRNAME:
        return f"~/{_DEFAULT_DIRNAME}"
    return str(home)


def path_label(*parts: str) -> str:
    """:func:`nerve_path` rendered for operator-facing output."""
    return str(Path(home_label(), *parts))


# --- Well-known files & subdirectories under the state dir ------------------- #
#
# These accessors are the canonical source of truth for each location. Prefer
# them over re-deriving paths so a future relocation only touches this file.


def config_pointer_file() -> Path:
    """Pointer file naming the active config directory (``~/.nerve/config_dir``)."""
    return nerve_path("config_dir")


def db_path() -> Path:
    """The main SQLite database (``~/.nerve/nerve.db``)."""
    return nerve_path("nerve.db")


def pid_file() -> Path:
    """The daemon PID file (``~/.nerve/nerve.pid``)."""
    return nerve_path("nerve.pid")


def log_file() -> Path:
    """The daemon log file (``~/.nerve/nerve.log``)."""
    return nerve_path("nerve.log")


def cache_dir() -> Path:
    """Scratch cache directory (gate state, prompt caches, ...)."""
    return nerve_path("cache")


def memu_sqlite() -> Path:
    """The memU memory index database (``~/.nerve/memu.sqlite``)."""
    return nerve_path("memu.sqlite")


def cron_dir() -> Path:
    """Legacy machine-local cron config directory (``~/.nerve/cron``).

    Kept for backward compatibility; new deployments store cron config in the
    workspace's ``config/cron`` subtree (see :mod:`nerve.config`).
    """
    return nerve_path("cron")


def default_workspace() -> Path:
    """Default git-syncable workspace root (``~/nerve-workspace``).

    This is *not* under the state dir — the workspace is the portable, shareable
    surface. Provided here so the default has a single definition; the effective
    workspace is configurable via ``config.workspace``.
    """
    return Path.home() / "nerve-workspace"
