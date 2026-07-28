"""Git-backed workspace sync.

The workspace is a git repository whose remote (on GitHub) is the shared config
repo. Config changes are proposed as PRs, reviewed, and merged there; this module
pulls the merged result onto the instance so it can hot-reload — no restart, no
hand-editing on the box.

``sync_workspace`` does a guarded ``git pull --ff-only`` and (optionally)
validates the pulled bundle. Applying the change (reloading cron / MCP) is the
caller's job: the in-daemon periodic loop reloads explicitly, while the CLI only
moves the files and leaves a running daemon to pick them up on its next cycle.
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


def _config_pathspec(workspace: Path) -> str:
    """The git pathspec for the portable config subtree of ``workspace``."""
    from nerve.config import workspace_config_dir

    return str(workspace_config_dir(workspace))


def _local_config_divergence(
    workspace: Path, rev: str, locked: bool = False,
) -> tuple[list[str], list[str]]:
    """Local state in the config subtree that the validated rev does not contain.

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

    Scoped to the config subtree on purpose. A nerve workspace is also the
    agent's working directory, so uncommitted notes and scratch files elsewhere
    in it are normal and none of sync's business — and where they *would* break
    the fast-forward, git says so itself.

    ``locked`` promotes ``.gitignore``d files inside the subtree from a warning to
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
    pathspec = _config_pathspec(workspace)

    status = _git([
        *_QUOTEPATH_OFF, "status", "--porcelain",
        "--untracked-files=all", "--ignored=matching", "--", pathspec,
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
            # An ignored file inside a *tracked config subtree* is a layout
            # mistake — it is config the shared repo can never carry and no
            # reviewer will ever see. Worth saying; on an unlocked box not worth
            # refusing a merge over, since refusing would be a policy decision
            # about what the machine is allowed to have locally rather than a
            # statement about whether the merge is sound. Under lockdown that
            # policy decision has already been made.
            if locked:
                blocking.append(f"!! {path} (gitignored)")
            else:
                warnings.append(
                    f"ignored file inside the tracked config subtree: {path}"
                )
        else:
            blocking.append(f"{code.strip() or '??'} {path}")

    # A submodule under the config subtree is a third case: `git worktree add`
    # does not initialize submodules, so validation saw an empty directory, and
    # the fast-forward leaves the live checkout on its old commit. Neither the
    # old contents nor the new ones were checked.
    tree = _git([*_QUOTEPATH_OFF, "ls-tree", "-r", rev, "--", pathspec], workspace)
    if tree.returncode == 0:
        for line in tree.stdout.splitlines():
            if line.startswith("160000 "):
                sub = line.split("\t", 1)[-1]
                warnings.append(
                    f"submodule {sub!r} in the config subtree was not validated "
                    f"(a validation checkout does not initialize submodules) and "
                    f"a fast-forward does not update it"
                )
    return blocking, warnings


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
    commit that was checked, so a merge is also refused when the live config
    subtree has local changes of its own — see
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
            validation_warnings=warnings,
            message=(
                f"fetched {new[:8]} but the workspace config subtree has local "
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

    On a successful, changed, valid pull it reloads MCP config and cron so the
    merged changes take effect without a restart. Best-effort — a failed cycle is
    logged and the loop continues.

    This loop is the only thing that applies a merged change on its own, so
    ``interval_minutes`` is the upper bound on how stale an instance can be.
    ``POST /api/config/sync`` does the same pull-and-apply on demand.

    Every cycle re-reads the process-wide config object rather than working from
    a reference captured before the loop, so the loop can never be the reason a
    setting is stuck. What refreshes that object is :func:`_apply_sync`, i.e. a
    sync that actually merged something — after which ``branch``, ``validate``,
    ``strict_env``, ``interval_minutes``, the workspace location and turning sync
    off all apply from the following cycle. Nothing *else* refreshes it, so a
    hand-edited ``config.yaml`` on the box still needs a restart. Turning sync
    *on* needs one regardless: this task is created at start-up only when sync is
    already enabled, and nothing creates it later.
    """
    from nerve.config import get_config

    cfg = config.workspace_sync
    interval = max(1, cfg.interval_minutes) * 60
    enabled = True
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
            if not result.ok:
                logger.warning("Workspace sync: %s", result.message)
                for err in result.validation_errors:
                    logger.warning("  config error: %s", err)
                continue
            if result.changed:
                logger.info("Workspace sync: %s — applying", result.message)
                apply_error = await _apply_sync(
                    engine, cron_service, config.config_dir or config.workspace,
                )
                if apply_error:
                    # The merge landed but the daemon is still running the old
                    # config. Logged at WARNING beside the INFO line above, which
                    # otherwise reads as "applied".
                    logger.warning(
                        "Workspace sync: merged %s but the new config could not "
                        "be loaded, so NOTHING was applied and this daemon is "
                        "still running the previous configuration: %s",
                        result.new_rev[:8], apply_error,
                    )
        except Exception as e:  # noqa: BLE001 — never let the loop die
            logger.warning("Workspace sync cycle failed: %s", e)
            continue


async def _apply_sync(engine, cron_service, config_dir) -> str | None:
    """Hot-reload the subsystems affected by a workspace pull.

    Re-reads the typed config and re-points the singleton, so a synced settings
    change engages here without a restart for everything that reads the singleton
    per use: the lockdown write guards (``is_locked``), gateway authentication,
    and — because it re-reads the singleton every cycle — the sync loop itself.
    ``cron_service.config`` and ``engine.config`` are re-pointed too. What is
    *not*: cron jobs and gates already built, which follow on the next
    ``cron_service.reload()``, and anything captured in a closure or a dataclass
    at start-up, which follows on a restart. This is also the only path that
    refreshes any of it — a hand edit to a config file on the box is not picked
    up, by design for a locked instance and by omission otherwise.

    Returns the config-reload error, if any. A merge that lands config the daemon
    cannot load has applied nothing, however well the merge itself went, and the
    caller must not report success for it: the case that makes this concrete is a
    pull that turns ``lockdown`` on, where reporting success tells the operator a
    box is locked while its write guards are still open.
    """
    from nerve.config import load_config, set_config

    config_error: str | None = None
    try:
        new_config = load_config(config_dir)
        set_config(new_config)
        if cron_service is not None:
            cron_service.config = new_config
        if engine is not None and hasattr(engine, "config"):
            engine.config = new_config
    except Exception as e:  # noqa: BLE001 — a bad reload must not kill the loop
        config_error = f"{type(e).__name__}: {e}"
        logger.warning("config reload after sync failed: %s", e, exc_info=True)
    if cron_service is not None:
        try:
            await cron_service.reload()
        except Exception as e:  # noqa: BLE001
            logger.warning("cron reload after sync failed: %s", e, exc_info=True)
    if engine is not None:
        try:
            await engine.reload_mcp_config()
        except Exception as e:  # noqa: BLE001
            logger.warning("MCP reload after sync failed: %s", e, exc_info=True)
    return config_error
