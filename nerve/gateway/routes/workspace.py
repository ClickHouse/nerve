"""Read-only browsing of the whole workspace, for the Files page.

Three routes, all read-only:

- ``GET /api/workspace/list?path=<dir>`` lists one directory. The tree loads a
  directory when it is expanded: a workspace can hold scratch directories with
  hundreds of thousands of files, which no single listing can carry.
- ``GET /api/workspace/file?path=<file>`` returns a file's text.
- ``GET /api/workspace/download?path=<file>`` sends the file itself, for the
  binary and oversized files the page does not show.

Writes stay on ``PUT /api/memory/file``, which takes memory markdown only. The
workspace holds files the daemon runs (git runs ``core.fsmonitor`` from
``.git/config``; the notification dispatcher runs ``scripts/``), so this module
adds no write path. Each entry says whether the memory route would accept a
save, so the UI offers an edit only where it works.

``files.exclude`` (see :class:`nerve.config.FilesConfig`) hides paths from the
listing and refuses them on read. A path is judged by where it resolves, so a
symlink cannot reach an excluded file or anything outside the workspace.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from fnmatch import fnmatchcase
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from nerve.config import FilesConfig, get_config, tracked_config_write_refusal
from nerve.gateway.auth import require_auth
from nerve.memory.manager import memory_file_path

router = APIRouter(dependencies=[Depends(require_auth)])

# Bytes sniffed for a NUL to call a file binary.
_SNIFF_BYTES = 8192


def is_excluded(rel_parts: tuple[str, ...], is_dir: bool, patterns: list[str]) -> bool:
    """Whether a workspace-relative path, or any directory above it, is excluded.

    ``rel_parts`` are the path's components from the workspace root. Every
    prefix is tested, so excluding ``tmp`` hides ``tmp/a/b.txt`` too.
    """
    for i in range(len(rel_parts)):
        name = rel_parts[i]
        sub = "/".join(rel_parts[: i + 1])
        component_is_dir = is_dir if i == len(rel_parts) - 1 else True
        for raw in patterns:
            pat = raw.strip()
            dir_only = pat.endswith("/")
            pat = pat.strip("/")
            if not pat or (dir_only and not component_is_dir):
                continue
            if "/" in pat:
                if fnmatchcase(sub, pat):
                    return True
            elif fnmatchcase(name, pat):
                return True
    return False


def resolve_workspace_path(workspace: Path, rel: str, patterns: list[str]) -> Path | None:
    """The path ``rel`` names inside the workspace, or None.

    None when it resolves outside the workspace, or when either the path as
    asked or the path it resolves to is excluded.
    """
    root = workspace.resolve()
    rel = (rel or "").strip()
    if rel.startswith("/"):
        return None
    try:
        asked = root / rel
        resolved = asked.resolve()
    except (OSError, RuntimeError, ValueError):
        return None
    if not resolved.is_relative_to(root):
        return None
    if resolved == root:
        return resolved
    try:
        is_dir = resolved.is_dir()
    except (OSError, ValueError):
        return None
    try:
        # Lexical, so ".." is folded without following symlinks.
        asked_parts = Path(os.path.normpath(asked)).relative_to(root).parts
    except ValueError:
        return None
    resolved_parts = resolved.relative_to(root).parts
    if is_excluded(asked_parts, is_dir, patterns) or is_excluded(resolved_parts, is_dir, patterns):
        return None
    return resolved


def _write_state(workspace: Path, rel: str, path: Path) -> tuple[bool, str | None]:
    """(read_only, reason) for a file: would PUT /api/memory/file save it?"""
    if memory_file_path(workspace, rel) is None:
        return True, "not_memory"
    if tracked_config_write_refusal(path) is not None:
        return True, "lockdown"
    return False, None


def _list_dir_sync(workspace: Path, rel: str, files_cfg: FilesConfig) -> dict:
    patterns = files_cfg.patterns
    root = workspace.resolve()
    directory = resolve_workspace_path(workspace, rel, patterns)
    if directory is None or not directory.is_dir():
        raise HTTPException(status_code=404, detail="Directory not found")
    # Children are named under the path as asked, so a symlinked directory
    # lists as itself rather than as its target.
    base = Path(rel.strip().strip("/")) if rel.strip().strip("/") else Path()

    entries: list[dict] = []
    try:
        scan = list(os.scandir(directory))
    except OSError as exc:
        raise HTTPException(status_code=403, detail=f"Cannot read directory: {exc.strerror}")

    for entry in scan:
        child_rel = (base / entry.name).as_posix()
        try:
            is_dir = entry.is_dir()  # follows a symlink
            target = Path(entry.path).resolve() if entry.is_symlink() else None
        except OSError:
            continue
        if target is not None and not target.is_relative_to(root):
            continue  # a symlink out of the workspace is not shown
        if is_excluded(Path(child_rel).parts, is_dir, patterns):
            continue
        if target is not None and target != root and is_excluded(
            target.relative_to(root).parts, is_dir, patterns,
        ):
            continue
        try:
            st = entry.stat()
        except OSError:
            continue
        item = {
            "name": entry.name,
            "path": child_rel,
            "type": "directory" if is_dir else "file",
            "modified": datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat(),
        }
        if not is_dir:
            read_only, reason = _write_state(workspace, child_rel, Path(entry.path))
            item.update(size=st.st_size, read_only=read_only, read_only_reason=reason)
        entries.append(item)

    entries.sort(key=lambda e: (e["type"] != "directory", e["name"].lower(), e["name"]))
    limit = max(1, files_cfg.max_entries)
    return {
        "path": base.as_posix() if base.parts else "",
        "entries": entries[:limit],
        "total": len(entries),
        "truncated": len(entries) > limit,
    }


def _read_file_sync(workspace: Path, rel: str, files_cfg: FilesConfig) -> dict:
    path = resolve_workspace_path(workspace, rel, files_cfg.patterns)
    if path is None or not path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    rel = rel.strip().strip("/")
    size = path.stat().st_size
    read_only, reason = _write_state(workspace, rel, path)
    result = {
        "path": rel,
        "name": Path(rel).name or path.name,
        "size": size,
        "content": "",
        "binary": False,
        "too_large": False,
        "read_only": read_only,
        "read_only_reason": reason,
    }
    if size > files_cfg.max_file_bytes:
        result["too_large"] = True
        return result
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise HTTPException(status_code=403, detail=f"Cannot read file: {exc.strerror}")
    if b"\x00" in data[:_SNIFF_BYTES]:
        result["binary"] = True
        return result
    try:
        result["content"] = data.decode("utf-8")
    except UnicodeDecodeError:
        result["binary"] = True
    return result


@router.get("/api/workspace/list")
async def list_workspace_dir(path: str = ""):
    """One directory of the workspace: its visible children, directories first."""
    config = get_config()
    return await asyncio.to_thread(_list_dir_sync, config.workspace, path, config.files)


@router.get("/api/workspace/file")
async def read_workspace_file(path: str):
    """A workspace file's text, or a flag that it is binary or too large."""
    config = get_config()
    return await asyncio.to_thread(_read_file_sync, config.workspace, path, config.files)


def _download_path_sync(workspace: Path, rel: str, files_cfg: FilesConfig) -> Path:
    path = resolve_workspace_path(workspace, rel, files_cfg.patterns)
    if path is None or not path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return path


@router.get("/api/workspace/download")
async def download_workspace_file(path: str):
    """A workspace file as an attachment: same rules as the read route, no size cap.

    The UI links here with the token in the query (``authUrl``), since a
    link cannot send a header.
    """
    config = get_config()
    resolved = await asyncio.to_thread(_download_path_sync, config.workspace, path, config.files)
    # The name as asked, not the symlink target's.
    name = Path(path.strip().strip("/")).name or resolved.name
    return FileResponse(
        path=str(resolved), media_type="application/octet-stream", filename=name,
    )
