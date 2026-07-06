"""Compute unified diffs for session file changes.

Primary approach: compare a stored snapshot (original content before agent
touched the file) against the current file on disk using Python's difflib.
Works without git — no external dependencies.

Optional optimization: use ``git diff`` when the file lives in a git repo.
"""

from __future__ import annotations

import difflib
import logging
import re
import subprocess
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Truncate diffs beyond this many output lines to keep payloads bounded.
MAX_DIFF_LINES = 2000

# Files with these suffixes get a rendered-markdown preview toggle in the UI;
# their diff response carries the preview source alongside the patch.
MARKDOWN_SUFFIXES = (".md", ".markdown")


def is_markdown_file(file_path: str) -> bool:
    """True when the path looks like a markdown document."""
    return file_path.lower().endswith(MARKDOWN_SUFFIXES)


# ------------------------------------------------------------------ #
#  Public API                                                          #
# ------------------------------------------------------------------ #

def compute_file_diff(
    original_content: str | None,
    current_content: str | None,
    file_path: str,
    context_lines: int = 3,
    workspace: str | None = None,
) -> dict[str, Any]:
    """Compute a structured unified diff between original and current content.

    Args:
        original_content: File content before the session touched it.
                          ``None`` means the file didn't exist (new file).
        current_content: Current file content on disk.
                         ``None`` means the file no longer exists (deleted).
        file_path: Absolute path — used only for display / path shortening.
        context_lines: Number of surrounding context lines per hunk.
        workspace: If provided, paths are shortened relative to this prefix.

    Returns:
        Structured diff dict ready for JSON serialization.
    """
    short = _shorten_path(file_path, workspace)

    # New file
    if original_content is None and current_content is not None:
        diff = _make_new_file_diff(current_content, file_path, short)

    # Deleted file
    elif current_content is None and original_content is not None:
        diff = _make_deleted_file_diff(original_content, file_path, short)

    # Both missing — shouldn't happen, treat as unchanged
    elif original_content is None and current_content is None:
        diff = _empty_diff(file_path, short, "unchanged")

    # Both exist — real diff
    elif original_content == current_content:
        diff = _empty_diff(file_path, short, "unchanged")

    else:
        assert original_content is not None and current_content is not None
        diff = _compute_difflib(original_content, current_content, file_path, short, context_lines)

    _attach_markdown_preview(diff, file_path, original_content, current_content)
    return diff


def compute_quick_stats(
    original_content: str | None,
    current_content: str | None,
) -> dict[str, int]:
    """Fast +/- counts without full hunk parsing."""
    if original_content is None and current_content is not None:
        return {"additions": current_content.count("\n") + 1, "deletions": 0}
    if current_content is None and original_content is not None:
        return {"additions": 0, "deletions": original_content.count("\n") + 1}
    if original_content is None and current_content is None:
        return {"additions": 0, "deletions": 0}
    assert original_content is not None and current_content is not None
    if original_content == current_content:
        return {"additions": 0, "deletions": 0}

    # Use difflib to count — cheaper than full parse
    adds = dels = 0
    for line in difflib.unified_diff(
        original_content.splitlines(keepends=True),
        current_content.splitlines(keepends=True),
        n=0,
    ):
        if line.startswith("+") and not line.startswith("+++"):
            adds += 1
        elif line.startswith("-") and not line.startswith("---"):
            dels += 1
    return {"additions": adds, "deletions": dels}


def shorten_path(file_path: str, workspace: str | None = None) -> str:
    """Public version of path shortener."""
    return _shorten_path(file_path, workspace)


# ------------------------------------------------------------------ #
#  Internal helpers                                                    #
# ------------------------------------------------------------------ #

def _attach_markdown_preview(
    diff: dict[str, Any],
    file_path: str,
    original_content: str | None,
    current_content: str | None,
) -> None:
    """Add the rendered-preview source for markdown files.

    ``markdown_content`` is the post-change content (what the file looks like
    now), falling back to the original for deleted files so the preview can
    still show what was removed. Always ``None`` for non-markdown files.
    Truncated at ``MAX_DIFF_LINES`` lines to keep payloads bounded, mirroring
    the patch truncation.
    """
    content: str | None = None
    truncated = False
    if is_markdown_file(file_path):
        content = current_content if current_content is not None else original_content
        if content is not None:
            lines = content.splitlines()
            if len(lines) > MAX_DIFF_LINES:
                content = "\n".join(lines[:MAX_DIFF_LINES])
                truncated = True
    diff["markdown_content"] = content
    diff["markdown_truncated"] = truncated


def _shorten_path(file_path: str, workspace: str | None = None) -> str:
    """Strip workspace prefix for display."""
    if workspace:
        ws = workspace.rstrip("/") + "/"
        if file_path.startswith(ws):
            return file_path[len(ws):]
    # Try common prefixes
    for prefix in ("/home/", "/root/", "/tmp/"):
        if file_path.startswith(prefix):
            parts = file_path[len(prefix):].split("/", 1)
            if len(parts) == 2:
                return parts[1]
    return file_path


def _compute_difflib(
    original: str,
    current: str,
    file_path: str,
    short_path: str,
    context_lines: int,
) -> dict[str, Any]:
    """Run difflib.unified_diff and parse into structured hunks."""
    orig_lines = original.splitlines(keepends=True)
    curr_lines = current.splitlines(keepends=True)

    raw = list(difflib.unified_diff(
        orig_lines,
        curr_lines,
        fromfile=f"a/{short_path}",
        tofile=f"b/{short_path}",
        n=context_lines,
    ))

    if not raw:
        return _empty_diff(file_path, short_path, "unchanged")

    return _parse_unified_diff(raw, file_path, short_path)


def _parse_unified_diff(
    lines: list[str],
    file_path: str,
    short_path: str,
) -> dict[str, Any]:
    """Parse unified diff lines into structured hunks with line numbers."""
    hunks: list[dict] = []
    current_hunk: dict | None = None
    additions = 0
    deletions = 0
    old_line = 0
    new_line = 0
    total_output = 0
    truncated = False

    for raw_line in lines:
        line = raw_line.rstrip("\n\r")

        # Skip diff header lines
        if line.startswith(("---", "+++")):
            continue

        # Hunk header
        m = re.match(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(.*)", line)
        if m:
            if current_hunk:
                hunks.append(current_hunk)
            old_line = int(m.group(1))
            new_line = int(m.group(3))
            current_hunk = {
                "old_start": old_line,
                "old_count": int(m.group(2) or 1),
                "new_start": new_line,
                "new_count": int(m.group(4) or 1),
                "header": m.group(5).strip(),
                "lines": [],
            }
            continue

        if current_hunk is None:
            continue

        total_output += 1
        if total_output > MAX_DIFF_LINES:
            truncated = True
            break

        if line.startswith("+"):
            current_hunk["lines"].append({
                "type": "addition",
                "content": line[1:],
                "new_line": new_line,
            })
            new_line += 1
            additions += 1
        elif line.startswith("-"):
            current_hunk["lines"].append({
                "type": "deletion",
                "content": line[1:],
                "old_line": old_line,
            })
            old_line += 1
            deletions += 1
        elif line.startswith(" "):
            current_hunk["lines"].append({
                "type": "context",
                "content": line[1:],
                "old_line": old_line,
                "new_line": new_line,
            })
            old_line += 1
            new_line += 1
        elif line.startswith("\\"):
            current_hunk["lines"].append({
                "type": "info",
                "content": line[2:].strip() if len(line) > 2 else "No newline at end of file",
            })

    if current_hunk:
        hunks.append(current_hunk)

    # Raw git-style patch string for the @pierre/diffs renderer. Built from the
    # same difflib output but truncated at whole-hunk boundaries so the emitted
    # patch is always valid.
    patch, patch_truncated = _build_patch(lines, short_path, MAX_DIFF_LINES)

    return {
        "path": file_path,
        "short_path": short_path,
        "status": "modified",
        "binary": False,
        "stats": {"additions": additions, "deletions": deletions},
        "hunks": hunks,
        "patch": patch,
        "truncated": truncated or patch_truncated,
    }


def _build_patch(
    raw_lines: list[str],
    short_path: str,
    max_content_lines: int,
) -> tuple[str, bool]:
    """Assemble a git-style unified-diff patch from raw difflib output.

    difflib's own ``---``/``+++`` header lines are regenerated as git-style
    headers (with a leading ``diff --git`` line) so the @pierre/diffs parser
    strips the ``a/``/``b/`` prefixes and detects the file correctly. Only
    whole hunks are emitted; truncation always lands on a hunk boundary so the
    result is a valid, parseable patch.

    Returns ``(patch_text, truncated)``.
    """
    hunks: list[list[str]] = []
    current: list[str] | None = None
    for raw in raw_lines:
        if raw.startswith(("---", "+++")):
            continue  # regenerated below
        if raw.startswith("@@"):
            current = [raw]
            hunks.append(current)
        elif current is not None:
            current.append(raw)

    if not hunks:
        return "", False

    out: list[str] = [
        f"diff --git a/{short_path} b/{short_path}\n",
        f"--- a/{short_path}\n",
        f"+++ b/{short_path}\n",
    ]
    total = 0
    truncated = False
    for hunk in hunks:
        body = hunk[1:]
        if total > 0 and total + len(body) > max_content_lines:
            truncated = True
            break
        for line in hunk:
            out.append(line if line.endswith("\n") else line + "\n")
        total += len(body)
    return "".join(out), truncated


def _new_file_patch(short_path: str, lines: list[str]) -> str:
    """git-style patch for a created file — every line is an addition."""
    if not lines:
        return ""
    out = [
        f"diff --git a/{short_path} b/{short_path}\n",
        "new file mode 100644\n",
        "--- /dev/null\n",
        f"+++ b/{short_path}\n",
        f"@@ -0,0 +1,{len(lines)} @@\n",
    ]
    out.extend(f"+{l}\n" for l in lines)
    return "".join(out)


def _deleted_file_patch(short_path: str, lines: list[str]) -> str:
    """git-style patch for a deleted file — every line is a deletion."""
    if not lines:
        return ""
    out = [
        f"diff --git a/{short_path} b/{short_path}\n",
        "deleted file mode 100644\n",
        f"--- a/{short_path}\n",
        "+++ /dev/null\n",
        f"@@ -1,{len(lines)} +0,0 @@\n",
    ]
    out.extend(f"-{l}\n" for l in lines)
    return "".join(out)


def _make_new_file_diff(
    content: str,
    file_path: str,
    short_path: str,
) -> dict[str, Any]:
    """Diff for a newly created file — all lines are additions."""
    lines = content.split("\n")
    truncated = len(lines) > MAX_DIFF_LINES
    if truncated:
        lines = lines[:MAX_DIFF_LINES]

    return {
        "path": file_path,
        "short_path": short_path,
        "status": "created",
        "binary": False,
        "stats": {"additions": len(lines), "deletions": 0},
        "hunks": [{
            "old_start": 0,
            "old_count": 0,
            "new_start": 1,
            "new_count": len(lines),
            "header": "",
            "lines": [
                {"type": "addition", "content": l, "new_line": i + 1}
                for i, l in enumerate(lines)
            ],
        }] if lines else [],
        "patch": _new_file_patch(short_path, lines),
        "truncated": truncated,
    }


def _make_deleted_file_diff(
    content: str,
    file_path: str,
    short_path: str,
) -> dict[str, Any]:
    """Diff for a deleted file — all lines are deletions."""
    lines = content.split("\n")
    truncated = len(lines) > MAX_DIFF_LINES
    if truncated:
        lines = lines[:MAX_DIFF_LINES]

    return {
        "path": file_path,
        "short_path": short_path,
        "status": "deleted",
        "binary": False,
        "stats": {"additions": 0, "deletions": len(lines)},
        "hunks": [{
            "old_start": 1,
            "old_count": len(lines),
            "new_start": 0,
            "new_count": 0,
            "header": "",
            "lines": [
                {"type": "deletion", "content": l, "old_line": i + 1}
                for i, l in enumerate(lines)
            ],
        }] if lines else [],
        "patch": _deleted_file_patch(short_path, lines),
        "truncated": truncated,
    }


def _empty_diff(
    file_path: str,
    short_path: str,
    status: str,
) -> dict[str, Any]:
    return {
        "path": file_path,
        "short_path": short_path,
        "status": status,
        "binary": False,
        "stats": {"additions": 0, "deletions": 0},
        "hunks": [],
        "patch": "",
        "truncated": False,
    }
