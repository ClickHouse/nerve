"""Git-backed workspace sync.

The workspace is a git repository whose remote (on GitHub) is the shared config
repo. Config changes are proposed as PRs, reviewed, and merged there; this module
pulls the merged result onto the instance so it can hot-reload — no restart, no
hand-editing on the box.

``sync_workspace`` does a guarded ``git pull --ff-only`` and (optionally)
validates the pulled bundle. Applying the merged change is the caller's job: the
in-daemon periodic loop runs the unified reload itself — the config object and the
services holding their own reference, then cron jobs, cron sources, MCP servers
and skills — while the CLI only moves the files.

A running daemon does pick those up on its next cycle, because the loop compares
what is on disk against the revision it last applied rather than against what its
own pull moved: a HEAD that moved out of band (``nerve config sync``, a bare
``git pull``) is a revision this process has never applied, and so is one whose
reload failed for some subsystem. With no daemon running nothing applies them
until one starts, which reads config from disk anyway.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nerve.config_validate import ValidationResult

logger = logging.getLogger(__name__)

# Bound network git operations so a slow/hung remote can't stall the loop or a
# graceful shutdown indefinitely.
_GIT_TIMEOUT_SECONDS = 120

# Two callers drive syncs concurrently — the periodic loop and POST
# /api/config/sync — and git's own ref locks are per-process-pair, not per-caller.
# Overlapping runs produce "cannot lock ref 'HEAD'" reported as an ff-only merge
# failure, which blames fast-forwardability for what is really contention. Serialize
# instead: a manual sync waits for the cycle in flight and then does its own.
_sync_lock = threading.Lock()

# Prefix for the throwaway validation worktrees, so leftovers from a process that
# died mid-validation can be recognized and swept.
_TMP_WORKTREE_PREFIX = ".nerve-sync-"

# How old a leftover validation worktree must be before it is assumed abandoned.
# Generous on purpose: a sync in another process (`nerve config sync` while the
# daemon loop runs) owns a directory this sweep must not delete out from under it.
_ABANDONED_WORKTREE_AGE_SECONDS = 3600

# Cap on how many paths a diagnostic message lists before summarizing.
_MAX_LISTED_PATHS = 10

# Prefix for any git command whose output we quote paths out of. By default git
# C-escapes every non-ASCII byte in a path, so `config/sübmodül` reaches the
# operator as `"config/s\303\274bmod\303\274l"` — and naming the offending path is
# the entire point of the messages below. Shared so a third such call cannot
# quietly forget it.
_QUOTEPATH_OFF = ("-c", "core.quotepath=false")


@dataclass
class SyncResult:
    ok: bool
    changed: bool = False
    message: str = ""
    old_rev: str = ""
    new_rev: str = ""
    validation_errors: list[str] = field(default_factory=list)
    validation_warnings: list[str] = field(default_factory=list)
    # The reviewed-surface paths that made this sync refuse to merge. Also named
    # in ``message``, but a caller deciding whether the instance has just entered
    # the blocked state cannot get that out of prose.
    blocked_paths: list[str] = field(default_factory=list)


@dataclass
class SyncState:
    """What the periodic sync loop last did.

    The loop is the only thing that applies a merged change on its own, and its
    outcome used to go to the log and nowhere else: nothing could ask whether
    this instance was still taking config, so a sync that had been refusing for a
    week looked exactly like one that had nothing to do. ``nerve doctor`` and
    ``GET /api/config/sync`` read this.

    In memory, never persisted, for the same reason ``applied_rev`` is loop-local
    (see :func:`run_periodic_sync`): a restart re-reads config from disk and
    re-derives every field here.
    """

    # The revision this daemon has applied everywhere, as the loop tracks it.
    applied_rev: str = ""
    # The upstream revision the last cycle resolved. Different from
    # ``applied_rev`` means config is on disk, or waiting on the remote, that
    # this daemon is not running.
    fetched_rev: str = ""
    # Non-empty while sync is refusing to merge over local changes.
    blocked_paths: list[str] = field(default_factory=list)
    # Subsystems that did not take the last applied config; retried each cycle.
    reload_errors: dict[str, str] = field(default_factory=dict)
    ok: bool = True
    message: str = ""
    checked_at: float = 0.0


_last_sync: SyncState | None = None


def last_sync_state() -> SyncState | None:
    """The last sync cycle's outcome, or ``None`` if no loop has run here.

    ``None`` in every process that is not the daemon — the CLI, tests — and in
    the daemon before the loop starts or when sync is off, so a reader has to
    treat it as "not known here" rather than "nothing wrong".
    """
    return _last_sync


def _record_sync_state(state: SyncState) -> None:
    global _last_sync
    _last_sync = state


def _git(args: list[str], cwd: Path) -> subprocess.CompletedProcess:
    """Run a git command in ``cwd``; captured, never raises.

    A failed git invocation comes back as a non-zero ``CompletedProcess`` with
    the reason on stderr, whatever the cause: git exiting non-zero, git not
    being installed at all, ``cwd`` having been deleted underneath us, or the
    remote hanging past the timeout. Callers already branch on ``returncode``,
    and the one thing they must never have to handle is an exception — the
    whole module's contract is that a sync reports failure rather than raising.

    Output is decoded as UTF-8, leniently. git echoes bytes it was given (branch
    names, paths, config values, remote error text) and has no obligation to make
    them UTF-8, so a strict decode would turn someone else's mojibake into a crash
    here. The encoding is pinned rather than left to the locale because the daemon
    usually runs under a service manager with ``LC_ALL=C``, where the default would
    mangle a remote's perfectly valid UTF-8 error message into unreadable
    replacement characters — losing the one thing the operator needs.
    """
    try:
        return subprocess.run(
            ["git", *args], cwd=str(cwd), capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=_GIT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(
            args, 1, "", f"git command timed out after {_GIT_TIMEOUT_SECONDS}s",
        )
    except Exception as e:  # noqa: BLE001 — git missing, bad cwd, OS refusal…
        return subprocess.CompletedProcess(args, 1, "", f"could not run git: {e}")


def is_git_repo(path: Path) -> bool:
    return (Path(path) / ".git").exists()


def _rev(ref: str, cwd: Path) -> str:
    r = _git(["rev-parse", ref], cwd)
    return r.stdout.strip() if r.returncode == 0 else ""


def _head_rev(workspace) -> str:
    """``HEAD`` of ``workspace``, or ``""`` when it cannot be read.

    Coerces the raw configured value inside the guard, for the reason
    :func:`sync_workspace` spells out: this one runs before the periodic loop's
    per-cycle ``try``, so a ``workspace`` that is ``None`` or a list would kill
    the loop at start-up instead of failing a cycle.
    """
    try:
        return _rev("HEAD", Path(workspace))
    except Exception:  # noqa: BLE001 — see above
        return ""


def _reviewed_pathspec(workspace: Path) -> list[str]:
    """The git pathspecs for the reviewed surface of ``workspace``.

    The same list the write guard refuses, so the two cannot disagree about what
    a reviewed file is. A path this misses is one sync merges over without ever
    reporting that the live copy differs from the one that passed validation.
    """
    from nerve.config import workspace_reviewed_paths

    return [str(p) for p in workspace_reviewed_paths(workspace)]


def _local_config_divergence(
    workspace: Path, rev: str, locked: bool = False,
) -> tuple[list[str], list[str]]:
    """Local state in the reviewed surface that the validated rev does not contain.

    Validation runs against a clean detached checkout of ``rev``; the merge lands
    in the live working tree. ``git merge --ff-only`` only refuses when the
    incoming commit touches a path that is locally modified, so everything else
    survives the merge untouched — and was never part of what was checked. A
    locally edited or deleted tracked file, a staged-but-uncommitted change, a
    tracked file swapped for a symlink pointing out of the repo, an untracked
    file: each one makes the bundle on disk something other than the bundle that
    passed.

    The sharp end is ``cron/gates/*.py``. Those are imported and executed by the
    daemon, the validator deliberately never loads them, and an untracked one is
    invisible to the throwaway worktree — so a box whose whole premise is "only
    reviewed remote config runs here" would run local unreviewed code while every
    sync reported success. Refusing is the only honest answer: sync's guarantee is
    about what ends up on disk, and it cannot make that promise about a tree
    somebody else is also editing.

    Scoped to the reviewed surface on purpose. A nerve workspace is also the
    agent's working directory, so uncommitted notes and scratch files elsewhere
    in it are normal and none of sync's business — and where they *would* break
    the fast-forward, git says so itself.

    ``locked`` promotes ``.gitignore``d files inside the surface from a warning to
    a refusal. On an ordinary box those files are the operator's own, kept
    deliberately out of the shared repo, and refusing a merge over them would be
    sync passing judgement on what a machine may hold locally. A locked instance
    has already made that judgement: its stated contract is that only reviewed,
    merged remote config runs there, and an ignored ``cron/gates/*.py`` is local
    unreviewed code the daemon executes, invisible to both the reviewer and the
    validator. Merging on top of it would report success over a bundle that is
    not the one that passed.

    Returns ``(blocking, warnings)``.
    """
    blocking: list[str] = []
    warnings: list[str] = []
    pathspec = _reviewed_pathspec(workspace)

    status = _git([
        *_QUOTEPATH_OFF, "status", "--porcelain",
        "--untracked-files=all", "--ignored=matching", "--", *pathspec,
    ], workspace)
    if status.returncode != 0:
        # Fail closed: unable to establish that the tree is clean is not the
        # same as clean.
        return ([
            f"could not check the workspace for local changes: "
            f"{status.stderr.strip() or status.stdout.strip()}"
        ], warnings)

    for line in status.stdout.splitlines():
        if len(line) < 4:
            continue
        code, path = line[:2], line[3:]
        if code == "!!":
            # An ignored file inside the *reviewed surface* is a layout mistake —
            # it is config the shared repo can never carry and no reviewer will
            # ever see. Worth saying; on an unlocked box not worth refusing a
            # merge over, since refusing would be a policy decision about what
            # the machine is allowed to have locally rather than a statement
            # about whether the merge is sound. Under lockdown that policy
            # decision has already been made.
            if locked:
                blocking.append(f"!! {path} (gitignored)")
            else:
                warnings.append(
                    f"ignored file inside the reviewed surface: {path}"
                )
        else:
            blocking.append(f"{code.strip() or '??'} {path}")

    # A submodule inside the reviewed surface is a third case: `git worktree add`
    # does not initialize submodules, so validation saw an empty directory, and
    # the fast-forward leaves the live checkout on its old commit. Neither the
    # old contents nor the new ones were checked.
    #
    # ``locked`` promotes it for the same reason as an ignored file: unvalidated
    # content the daemon reads anyway is exactly what a locked box says cannot be
    # there, and the submodule case is the worse of the two, since the working
    # copy is whatever the box happens to have checked out and no remote change
    # ever moves it.
    tree = _git([*_QUOTEPATH_OFF, "ls-tree", "-r", rev, "--", *pathspec], workspace)
    if tree.returncode == 0:
        for line in tree.stdout.splitlines():
            if line.startswith("160000 "):
                sub = line.split("\t", 1)[-1]
                note = (
                    f"submodule {sub!r} in the reviewed surface was not validated "
                    f"(a validation checkout does not initialize submodules) and "
                    f"a fast-forward does not update it"
                )
                (blocking if locked else warnings).append(note)
    return blocking, warnings


def local_block_reasons(workspace, locked: bool = False) -> list[str]:
    """What in ``workspace`` would make the next sync refuse to merge, now.

    The same check :func:`_sync_workspace` runs, minus the fetch — so it answers
    from any process, which is what ``nerve doctor`` needs: the retained
    :class:`SyncState` only exists inside the daemon, and the shell is where an
    operator asks why config stopped arriving. It reports nothing when there is
    no repository to check, since sync is not the answer to that question.
    """
    try:
        ws = Path(workspace)
        if not is_git_repo(ws):
            return []
        blocking, _warnings = _local_config_divergence(ws, "HEAD", locked)
        return blocking
    except Exception as e:  # noqa: BLE001 — a diagnostic must not raise
        return [f"could not check the workspace for local changes: {e}"]


def _describe_paths(paths: list[str]) -> str:
    """Join paths for a message, summarizing once the list stops being readable."""
    if len(paths) <= _MAX_LISTED_PATHS:
        return ", ".join(paths)
    shown = ", ".join(paths[:_MAX_LISTED_PATHS])
    return f"{shown}, +{len(paths) - _MAX_LISTED_PATHS} more"


def _sweep_abandoned_worktrees(workspace: Path) -> None:
    """Drop validation worktrees left behind by a process that died mid-check.

    ``_validate_rev`` unregisters its worktree in a ``finally``, which covers
    every exception but not SIGKILL, a power loss, or a container stop. What
    survives is both a directory and a live entry in ``.git/worktrees``, so
    ``git worktree prune`` (which only forgets worktrees whose directory is
    already gone) will never clear it. On a five-minute sync cadence that grows
    without bound, and nothing else ever looks.

    Only clearly abandoned directories are touched — see
    ``_ABANDONED_WORKTREE_AGE_SECONDS``. Best-effort throughout: failing to tidy
    up is not a reason to fail a sync.
    """
    cutoff = time.time() - _ABANDONED_WORKTREE_AGE_SECONDS
    try:
        candidates = [
            p for p in workspace.parent.iterdir()
            if p.name.startswith(_TMP_WORKTREE_PREFIX) and p.is_dir()
            and p.stat().st_mtime < cutoff
        ]
    except OSError:
        return
    for stale in candidates:
        _git(["worktree", "remove", "--force", str(stale / "wt")], workspace)
        shutil.rmtree(stale, ignore_errors=True)
    if candidates:
        # Clears registrations whose directory the loop above just removed.
        _git(["worktree", "prune"], workspace)
        logger.info(
            "Workspace sync: removed %d abandoned validation worktree(s)",
            len(candidates),
        )


def _validate_rev(
    workspace: Path, rev: str, config_dir: Path, strict_env: bool = True,
) -> ValidationResult:
    """Validate the config bundle *at a fetched rev* without touching the live
    working tree, by checking it out into a throwaway git worktree.

    Returns a :class:`~nerve.config_validate.ValidationResult`. Anything that
    goes wrong on the way to a verdict — no room for the worktree, an
    unreadable file, a validator that raises — becomes an *error* in that
    result rather than an exception: sync fails closed, and never by crashing.

    ``strict_env`` is on by default because this asks a narrower question than
    ``nerve config validate`` does. CI is lenient about ``${VAR}`` references it
    has no secrets for; here the answer that matters is "can *this* process load
    the bundle", and this process is the daemon, with the daemon's environment.
    An unset required reference means ``load_config`` will raise on the next
    restart, so it has to block the merge.

    Key strictness goes the other way, and also on purpose: an unknown key is
    left a warning here, where the config repo's CI makes it an error. A key this
    nerve doesn't recognize is inert, and refusing an already-reviewed, merged
    change over one would strand the instance on a stale config for a spelling
    the next upgrade may well know.
    """
    from nerve.config_validate import ValidationResult, validate_config_bundle

    tmp = wt = None
    try:
        _sweep_abandoned_worktrees(workspace)
        # Keep the temp worktree on the same filesystem as the repo.
        tmp = Path(tempfile.mkdtemp(
            prefix=_TMP_WORKTREE_PREFIX, dir=str(workspace.parent),
        ))
        wt = tmp / "wt"
        add = _git(["worktree", "add", "--detach", str(wt), rev], workspace)
        if add.returncode != 0:
            return ValidationResult(
                errors=[
                    "could not create validation worktree: "
                    f"{add.stderr.strip() or add.stdout.strip()}"
                ]
            )
        return validate_config_bundle(
            config_dir, workspace_override=wt, strict_env=strict_env,
        )
    except Exception as e:  # noqa: BLE001 — an unreachable verdict is a failed one
        return ValidationResult(errors=[f"validation failed: {type(e).__name__}: {e}"])
    finally:
        if wt is not None:
            _git(["worktree", "remove", "--force", str(wt)], workspace)
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)


def sync_workspace(
    workspace: Path,
    config_dir: Path,
    branch: str = "",
    validate: bool = True,
    strict_env: bool = True,
    locked: bool = False,
) -> SyncResult:
    """Fetch the workspace remote, validate the fetched bundle, then ff-merge it.

    Crucially the live working tree is only fast-forwarded **after** validation
    passes, so an invalid bundle never lands on disk (nothing to be picked up by
    a later reload or the next restart). Never raises; returns a
    :class:`SyncResult`.

    That guarantee is about the tree the daemon will read, not merely about the
    commit that was checked, so a merge is also refused when the live reviewed
    files have local changes of their own — see
    :func:`_local_config_divergence`. ``changed`` reports whether the live tree
    actually moved; a refusal leaves it exactly where it was.

    ``strict_env`` treats an unset required ``${VAR}`` reference in the fetched
    bundle as invalid — the merge would otherwise leave a checkout the daemon
    refuses to load on its next restart. Turn it off only when running somewhere
    that legitimately lacks the daemon's environment, e.g. an operator's shell.

    ``locked`` tightens the local-changes check for an instance that has promised
    to run only reviewed remote config — see :func:`_local_config_divergence`.

    "Never raises" is the whole contract, not an aspiration: the HTTP route
    turns anything that escapes into a 500 and the daemon's loop would report a
    stack trace instead of a config problem. The guard here is deliberately
    broader than the failures currently known — every caller handles a
    ``SyncResult``, and none handles an exception. Callers should hand over the
    raw configured values and let this function coerce them: doing ``Path(...)``
    on the caller's side puts that conversion outside the guard, where a
    ``workspace`` that is ``None`` or a list (a merge conflict resolved badly)
    becomes the traceback the contract exists to prevent.
    """
    try:
        with _sync_lock:
            return _sync_workspace(
                Path(workspace), Path(config_dir), branch, validate, strict_env,
                locked,
            )
    except Exception as e:  # noqa: BLE001 — see the contract above
        logger.warning("Workspace sync failed unexpectedly: %s", e, exc_info=True)
        return SyncResult(ok=False, message=f"sync failed: {type(e).__name__}: {e}")


def _sync_workspace(
    workspace: Path,
    config_dir: Path,
    branch: str,
    validate: bool,
    strict_env: bool,
    locked: bool = False,
) -> SyncResult:
    """The fetch → validate → merge sequence. See :func:`sync_workspace`."""
    if not is_git_repo(workspace):
        return SyncResult(ok=False, message=f"{workspace} is not a git repository")

    old = _rev("HEAD", workspace)

    fetch = _git(["fetch", "origin", branch] if branch else ["fetch"], workspace)
    if fetch.returncode != 0:
        return SyncResult(
            ok=False, old_rev=old,
            message=f"git fetch failed: {fetch.stderr.strip() or fetch.stdout.strip()}",
        )

    target_ref = f"origin/{branch}" if branch else "@{u}"
    new = _rev(target_ref, workspace)
    if not new:
        return SyncResult(
            ok=False, old_rev=old,
            message=f"could not resolve upstream {target_ref!r} (no tracking branch?)",
        )

    if new == old:
        return SyncResult(ok=True, changed=False, old_rev=old, new_rev=new, message="up to date")

    blocking, warnings = _local_config_divergence(workspace, new, locked)
    if blocking:
        return SyncResult(
            ok=False, changed=False, old_rev=old, new_rev=new,
            validation_warnings=warnings, blocked_paths=blocking,
            message=(
                f"fetched {new[:8]} but the workspace's reviewed files have local "
                f"changes — not applying, because they would survive the "
                f"fast-forward without ever having been validated: "
                f"{_describe_paths(blocking)}. Commit, discard or push them."
            ),
        )

    if validate:
        report = _validate_rev(workspace, new, config_dir, strict_env)
        # Warnings ride along on the result even when the merge goes ahead.
        # Validation can only confirm what it is allowed to look at — it does
        # not load the bundle's gate plugins, and unknown keys are tolerated —
        # so "no errors" is not "nothing to know about". Applying a bundle whose
        # gate type nothing recognizes is how a cron job quietly starts running
        # unconditionally, and the only place that is visible is here.
        warnings += report.warnings
        if report.unresolved_env and not strict_env:
            # With strict_env on this is already an error and the merge is
            # refused. With it off the validator files it as *info*, which
            # nothing here propagates — so the gate would see the one thing that
            # predicts a failed post-merge reload and drop it on the floor. The
            # merge still goes ahead: switching strict_env off is exactly a
            # request to tolerate this. Saying so is not.
            warnings.append(
                f"the fetched bundle references unset environment variable(s): "
                f"{', '.join(report.unresolved_env)} — workspace_sync.strict_env "
                f"is off so the merge proceeds, but this daemon will refuse to "
                f"load the merged config"
            )
        if report.errors:
            return SyncResult(
                ok=False, changed=False, old_rev=old, new_rev=new,
                validation_errors=report.errors, validation_warnings=warnings,
                message=(
                    f"fetched {new[:8]} but the config bundle is INVALID — not "
                    f"applying ({len(report.errors)} error(s))"
                ),
            )
    merge = _git(["merge", "--ff-only", new], workspace)
    if merge.returncode != 0:
        return SyncResult(
            ok=False, old_rev=old, new_rev=new, validation_warnings=warnings,
            message=f"ff-only merge failed: {merge.stderr.strip() or merge.stdout.strip()}",
        )
    if not validate:
        # Reported on every merge, not once at start-up. Validation can be
        # switched off in ways that leave no trace — a config key, a CLI flag, an
        # env reference exported empty (an empty value means off) — and the
        # result is a fast-forward to whatever the remote happens to carry,
        # including a bundle the daemon will refuse to load. Each occurrence is
        # worth a line.
        warnings.append(
            "validation is disabled (workspace_sync.validate is off): the bundle "
            "now on disk has not been checked"
        )
    return SyncResult(
        ok=True, changed=True, old_rev=old, new_rev=new,
        validation_warnings=warnings,
        message=f"updated {old[:8]}→{new[:8]}",
    )


async def run_periodic_sync(config, engine, cron_service, stop_event: asyncio.Event) -> None:
    """Daemon loop: pull the workspace every ``interval_minutes`` and apply.

    On a successful, valid pull it runs the same unified reload as
    ``POST /api/config/reload`` — the process-wide config object and the services
    holding their own reference, then cron jobs, cron sources, MCP servers and
    skills — so the merged changes take effect without a restart. Best-effort: a
    failed cycle is logged and the loop continues.

    A cycle applies when the revision on disk is not the one this process has
    applied, which is a wider condition than "this pull merged something" and
    deliberately so — see ``applied_rev`` below.

    Every cycle's outcome is retained in a :class:`SyncState` and, when the merge
    starts being refused, announced once to the operator — see
    :func:`_record_cycle`.

    This loop is the only thing that applies a merged change on its own, so
    ``interval_minutes`` is the upper bound on how stale an instance can be.
    ``POST /api/config/sync`` pulls and applies what it merged on demand; it does
    not apply a revision that was already on disk when it ran, so the immediate
    equivalent for that is ``POST /api/config/reload``.

    Every cycle re-reads the process-wide config object rather than working from
    a reference captured before the loop, so the loop can never be the reason a
    setting is stuck. That object is replaced by any config reload — a sync that
    merged something, or ``POST /api/config/reload`` — after which ``branch``,
    ``validate``, ``strict_env``, ``interval_minutes``, the workspace location and
    turning sync off all apply from the following cycle. A file edited on the box
    does *not* reload itself, so a hand-edited ``config.yaml`` reaches this loop
    only once a reload is asked for. Turning sync *on* needs a restart regardless:
    this task is created at start-up only when sync is already enabled, and
    nothing creates it later.
    """
    from nerve.config import get_config
    from nerve.config_reload import reload_failures

    cfg = config.workspace_sync
    interval = max(1, cfg.interval_minutes) * 60
    enabled = True
    # The revision every subsystem in this process has taken. Not the same
    # question as what is checked out: HEAD moves without this loop (`nerve
    # config sync`, a bare `git pull` in the workspace) and a reload can fail for
    # one subsystem, and in both cases the config on disk is not the config the
    # daemon is running. Comparing against HEAD alone reports "up to date" for
    # exactly those two states and never applies them again.
    #
    # Loop-local and seeded from HEAD, because a restart reads config from disk
    # anyway: the state this would have to carry across one is exactly the state
    # a restart makes moot.
    applied_rev = _head_rev(config.workspace)
    logger.info(
        "Workspace sync enabled: pulling %s every %d min",
        config.workspace, cfg.interval_minutes,
    )
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            break  # stop_event set → exit
        except asyncio.TimeoutError:
            pass  # interval elapsed → run a sync
        # One guard around the whole cycle, applying included: a loop that dies
        # here never syncs again, and nothing restarts it.
        try:
            config = get_config()
            cfg = config.workspace_sync
            interval = max(1, cfg.interval_minutes) * 60
            if cfg.enabled != enabled:
                enabled = cfg.enabled
                logger.info(
                    "Workspace sync %s by config", "enabled" if enabled else "disabled",
                )
            if not enabled:
                continue
            result = await asyncio.to_thread(
                sync_workspace, config.workspace,
                config.config_dir or config.workspace,
                branch=cfg.branch, validate=cfg.validate, strict_env=cfg.strict_env,
                locked=config.lockdown,
            )
            for warning in result.validation_warnings:
                logger.warning("Workspace sync: config warning: %s", warning)
            failures: dict[str, str] = {}
            if not result.ok:
                logger.warning("Workspace sync: %s", result.message)
                for err in result.validation_errors:
                    logger.warning("  config error: %s", err)
            elif result.changed or (result.new_rev and result.new_rev != applied_rev):
                if result.changed:
                    logger.info("Workspace sync: %s — applying", result.message)
                else:
                    logger.info(
                        "Workspace sync: %s is on disk but this daemon last "
                        "applied %s — applying",
                        result.new_rev[:8], applied_rev[:8] or "nothing",
                    )
                summary = await _apply_sync(
                    engine, cron_service, config.config_dir or config.workspace,
                )
                failures = reload_failures(summary)
                if not failures:
                    # Advanced only when every subsystem took it. A subsystem
                    # that refused the new config leaves this behind, so the next
                    # cycle retries rather than the daemon settling on the one
                    # warning below and never mentioning it again.
                    applied_rev = result.new_rev
                # Both branches sit at WARNING beside the INFO line above, which
                # on its own reads as "applied".
                if "config" in failures:
                    # The merge landed but the daemon is still running the old
                    # config.
                    logger.warning(
                        "Workspace sync: merged %s but the new config could not "
                        "be loaded, so NOTHING was applied and this daemon is "
                        "still running the previous configuration: %s",
                        result.new_rev[:8], failures["config"],
                    )
                elif failures:
                    # Config loaded, so the merge is partly live — which is the
                    # state hardest to spot from the outside and so the one that
                    # has to be named rather than left to the summary line.
                    logger.warning(
                        "Workspace sync: merged %s and loaded the new config, but "
                        "%s did not take it — this daemon is running the merged "
                        "configuration only in part",
                        result.new_rev[:8],
                        "; ".join(f"{k} ({v})" for k, v in sorted(failures.items())),
                    )
            await _record_cycle(engine, result, applied_rev, failures)
        except Exception as e:  # noqa: BLE001 — never let the loop die
            logger.warning("Workspace sync cycle failed: %s", e)
            continue


async def _record_cycle(
    engine, result: SyncResult, applied_rev: str, failures: dict[str, str],
) -> None:
    """Retain what this cycle found, and notify when the box enters or leaves the
    blocked state.

    A refused merge is the failure with no other trace: the instance stays pinned
    to an old reviewed revision, every later config change stops arriving, and
    the only evidence is a WARNING repeating every cycle. On a locked box that
    defeats the deployment, and the check covers the whole reviewed surface
    (``config/``, ``skills/``, the root instruction files), so the agent writing
    a skill directly is enough to cause it.

    Notified on the transition rather than on the state, which is what the
    retained record buys: the state notified every cycle is 1,440 messages a day
    at the default cadence, all of them the same one.
    """
    previous = _last_sync
    blocked = list(result.blocked_paths)
    # The local check only runs on a cycle that resolved an upstream revision to
    # merge. A cycle that failed before that — no repository, a failed fetch, no
    # tracking branch — establishes nothing about the local tree, so the record
    # keeps what it had: clearing it would report a recovery nobody made and then
    # announce the same block again on the next cycle that gets through.
    checked_the_tree = bool(result.new_rev) and result.new_rev != result.old_rev
    if not blocked and not result.ok and not checked_the_tree:
        blocked = list(previous.blocked_paths) if previous else []
    _record_sync_state(SyncState(
        applied_rev=applied_rev,
        fetched_rev=result.new_rev,
        blocked_paths=blocked,
        reload_errors=dict(failures),
        ok=result.ok,
        message=result.message,
        checked_at=time.time(),
    ))
    was_blocked = bool(previous and previous.blocked_paths)
    if blocked and not was_blocked:
        await _notify(
            engine, "Workspace sync blocked",
            f"Config sync is refusing to merge: the reviewed files in the "
            f"workspace have local changes, and a fast-forward would keep them "
            f"without their ever having been reviewed or validated.\n\n"
            f"{_describe_paths(blocked)}\n\n"
            f"Until this clears, no merged config reaches this instance — it "
            f"stays on {applied_rev[:8] or 'the revision it started with'}. "
            f"Commit the changes, discard them, or re-propose them as a PR with "
            f"propose_config_change.",
            "high",
        )
    elif was_blocked and not blocked and result.ok:
        await _notify(
            engine, "Workspace sync unblocked",
            f"The reviewed files are clean again and config sync is merging. "
            f"This instance is on {applied_rev[:8] or result.new_rev[:8]}.",
            "low",
        )


async def _notify(engine, title: str, body: str, priority: str) -> None:
    """Send an operator notification through whatever the daemon has wired up.

    Same shape as every other in-daemon sender: the service hangs off the engine,
    and outside a live gateway (the CLI, tests) there is none, so the caller
    carries on silently. A delivery failure is logged and swallowed — sync's
    outcome does not depend on the news getting out.
    """
    svc = getattr(engine, "notification_service", None)
    if svc is None:
        return
    try:
        await svc.send_notification(
            session_id="system", title=title, body=body, priority=priority,
        )
    except Exception:  # noqa: BLE001 — see above
        logger.warning("Workspace sync notification failed", exc_info=True)


async def _apply_sync(engine, cron_service, config_dir) -> dict:
    """Hot-reload the subsystems affected by a workspace pull.

    Delegates to the unified reload so a synced pull applies the SAME set of
    subsystems as a manual ``POST /api/config/reload`` — the process config
    object, cron jobs, sources, MCP, and skills.

    Replacing the config object is what makes a synced settings change engage
    without a restart, for everything that reads it per use: the lockdown write
    guards (``is_locked``), gateway authentication, and — because it re-reads it
    every cycle — the sync loop itself. The services that captured the old object
    are re-pointed with it. What is *not*: cron jobs and gates already built,
    which follow on the next ``cron_service.reload()``, and anything a service
    derived from config when it was constructed, which follows on a restart.

    Returns the per-subsystem summary. It has to be *read*, not assumed: a merge
    that lands config the daemon cannot load has applied nothing however well the
    merge itself went, and a merge whose config loaded but whose cron reload
    failed is in effect only in part. The case that makes this concrete is a pull
    that turns ``lockdown`` on, where reporting a clean success tells the operator
    the box is locked while its write guards are still open.
    """
    from nerve.config_reload import reload_all, reload_failures

    summary = await reload_all(engine, cron_service, config_dir)
    if reload_failures(summary):
        logger.warning("Applied synced config, with failures: %s", summary)
    else:
        logger.info("Applied synced config: %s", summary)
    return summary
