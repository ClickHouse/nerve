"""Git helpers for the code-review panel.

Enumerate the git worktrees of configured repo roots, list working-tree
changes against a base ref, and read file content at a ref or from the working
tree. Every path is confined to a configured repo root via
:func:`resolve_within_repos` (path-traversal / symlink-escape guard).

All functions here are synchronous ``subprocess`` wrappers — call them from
routes via ``asyncio.to_thread`` so git never blocks the event loop.
"""

from __future__ import annotations

import subprocess
from pathlib import Path


class RepoAccessError(Exception):
    """Raised for an invalid repo/worktree/path request (maps to HTTP 400)."""


def _git(cwd: Path, *args: str, timeout: int = 20) -> str:
    proc = subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        raise RepoAccessError(proc.stderr.strip() or f"git {' '.join(args)} failed")
    return proc.stdout


def list_worktrees_sync(root: Path) -> list[dict]:
    """Parse ``git worktree list --porcelain`` for one repo root."""
    out = _git(root, "worktree", "list", "--porcelain")
    worktrees: list[dict] = []
    cur: dict = {}
    for line in out.splitlines():
        if not line.strip():
            if cur:
                worktrees.append(cur)
                cur = {}
            continue
        key, _, val = line.partition(" ")
        if key == "worktree":
            cur["path"] = val
        elif key == "branch":
            cur["branch"] = val.replace("refs/heads/", "")
        elif key == "HEAD":
            cur["head"] = val[:12]
        elif key == "detached":
            cur["branch"] = "(detached)"
    if cur:
        worktrees.append(cur)
    return worktrees


def _all_worktree_paths(repos: list[str]) -> dict[Path, Path]:
    """Map every configured worktree's resolved path -> its owning repo root.

    Silently skips roots that aren't valid git repos so one bad config entry
    doesn't break the whole panel.
    """
    mapping: dict[Path, Path] = {}
    for r in repos:
        root = Path(r).expanduser().resolve()
        try:
            for w in list_worktrees_sync(root):
                mapping[Path(w["path"]).resolve()] = root
        except (RepoAccessError, subprocess.SubprocessError, OSError):
            continue
    return mapping


def resolve_within_repos(
    worktree: str,
    repos: list[str],
    path: str | None = None,
) -> tuple[Path, Path, Path | None]:
    """Validate a worktree (+ optional file path) against configured repos.

    Returns ``(worktree_path, repo_root, target_path_or_None)``.

    - ``worktree`` must be a real git worktree of one configured repo root.
    - ``path`` (repo-relative) must resolve to a location inside that worktree
      — after symlink resolution — so ``..`` and symlink escapes are rejected.
    """
    wt = Path(worktree).expanduser().resolve()
    valid = _all_worktree_paths(repos)
    root = valid.get(wt)
    if root is None:
        raise RepoAccessError(f"worktree is not part of a configured repo: {worktree}")

    if path is None:
        return wt, root, None

    candidate = Path(path)
    target = (candidate if candidate.is_absolute() else wt / candidate).resolve()
    if target != wt and not target.is_relative_to(wt):
        raise RepoAccessError("path escapes the worktree")
    return wt, root, target


def changed_files_sync(worktree: Path, base: str = "HEAD") -> list[dict]:
    """List files that differ between ``base`` and the working tree.

    Covers tracked changes (``git diff base``, i.e. staged + unstaged) plus
    untracked files. Each entry: ``{path, status, additions, deletions}``.
    """
    files: dict[str, dict] = {}

    name_status = _git(worktree, "diff", "--name-status", base, "--")
    for line in name_status.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        code = parts[0]
        if code.startswith("R") and len(parts) >= 3:  # rename: "R100\told\tnew"
            path, status = parts[2], "renamed"
        elif len(parts) >= 2:
            path = parts[1]
            status = {"A": "created", "M": "modified", "D": "deleted"}.get(code[0], "modified")
        else:
            continue
        files[path] = {"path": path, "status": status, "additions": 0, "deletions": 0}

    numstat = _git(worktree, "diff", "--numstat", base, "--")
    for line in numstat.splitlines():
        cols = line.split("\t")
        if len(cols) < 3:
            continue
        adds, dels, path = cols[0], cols[1], cols[2]
        entry = files.get(path)
        if entry is not None:
            entry["additions"] = 0 if adds == "-" else int(adds or 0)
            entry["deletions"] = 0 if dels == "-" else int(dels or 0)

    untracked = _git(worktree, "ls-files", "--others", "--exclude-standard")
    for path in untracked.splitlines():
        if path.strip():
            files.setdefault(path, {"path": path, "status": "created", "additions": 0, "deletions": 0})

    return sorted(files.values(), key=lambda f: f["path"])


def file_at_ref_sync(worktree: Path, ref: str, path: str) -> str | None:
    """Content of ``path`` at ``ref`` (repo-relative); None if absent there."""
    try:
        return _git(worktree, "show", f"{ref}:{path}")
    except RepoAccessError:
        return None


def read_working_file_sync(target: Path | None, max_bytes: int) -> str | None:
    """Read the working-tree file; None if missing; raise if too large/binary."""
    if target is None or not target.exists() or not target.is_file():
        return None
    if target.stat().st_size > max_bytes:
        raise RepoAccessError(f"file exceeds max_file_bytes ({max_bytes})")
    data = target.read_bytes()
    if b"\x00" in data[:8192]:
        raise RepoAccessError("binary file")
    return data.decode("utf-8", errors="replace")
