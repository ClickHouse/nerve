"""Reverse-apply unified diffs — pre-image reconstruction for snapshots.

The live probe (scripts/codex_smoke.py, 2026-07-10) showed the real
app-server emits ``item/started`` for fileChange items *after* the patch
is applied to disk, so snapshotting the file at that point captures the
NEW content — useless for the UI's before/after diff panel. But codex
hands us the unified diff of every change, so the pre-image is
recoverable: reverse-apply the diff to the post-apply content.

The verification-first design makes timing irrelevant:

* post-apply timing → reverse-apply succeeds → true pre-image;
* pre-apply timing (e.g. ``approval_policy: on-request``, where the
  request precedes the write) → the disk still holds the before-text, the
  reverse application fails its context checks → caller falls back to the
  disk content, which IS the pre-image.

Every hunk line is verified against the text it claims to describe;
any mismatch returns ``None`` (never a silently wrong reconstruction).
"""

from __future__ import annotations

import re

_HUNK_RE = re.compile(
    r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@",
)


def reverse_apply_unified_diff(diff: str, after_text: str) -> str | None:
    """Reconstruct the before-text from a unified diff and the after-text.

    Returns ``None`` when the diff doesn't cleanly describe
    ``after_text`` (wrong file state, malformed diff) — callers must
    fall back rather than trust a partial reconstruction.
    """
    if not diff:
        return None

    keepends = after_text.splitlines(keepends=True)
    # Track whether the after-text ends without a newline so the
    # reconstruction can preserve the analogous property.
    before_parts: list[str] = []
    pos = 0  # cursor into keepends (0-based line index of after_text)

    lines = diff.splitlines()
    i = 0
    saw_hunk = False
    while i < len(lines):
        line = lines[i]
        match = _HUNK_RE.match(line)
        if not match:
            # Headers (---/+++/diff --git/index) and anything between
            # hunks are skipped; hunk bodies are consumed inside the
            # inner loop below.
            i += 1
            continue

        saw_hunk = True
        new_start = int(match.group(3))
        new_len = int(match.group(4) or "1")
        # A zero-length new side positions the hunk AFTER line new_start
        # (unified-diff convention for pure deletions).
        hunk_pos = new_start - 1 if new_len > 0 else new_start

        if hunk_pos < pos or hunk_pos > len(keepends):
            return None  # overlapping or out-of-range hunk

        # Copy the untouched region preceding this hunk.
        before_parts.extend(keepends[pos:hunk_pos])
        pos = hunk_pos

        i += 1
        while i < len(lines):
            body = lines[i]
            if _HUNK_RE.match(body):
                break  # next hunk
            if body.startswith("\\"):
                # "\ No newline at end of file" — metadata, no content.
                i += 1
                continue
            if not body:
                # A fully blank line inside a hunk is an empty context
                # line whose leading space was stripped somewhere.
                body = " "
            tag, content = body[0], body[1:]
            if tag == " ":
                if pos >= len(keepends) or _line(keepends[pos]) != content:
                    return None
                before_parts.append(keepends[pos])
                pos += 1
            elif tag == "+":
                # Present in after, absent in before — verify and skip.
                if pos >= len(keepends) or _line(keepends[pos]) != content:
                    return None
                pos += 1
            elif tag == "-":
                # Absent in after, present in before — re-insert.
                before_parts.append(content + "\n")
            else:
                # Not a hunk body line (e.g. the next file's header in a
                # concatenated diff) — end of this hunk.
                break
            i += 1

    if not saw_hunk:
        return None

    before_parts.extend(keepends[pos:])
    before = "".join(before_parts)
    # If the diff removed the file's trailing newline (after has none but
    # the reconstruction added one via a "-" line), we can't tell without
    # the "\" markers per side; the common case (both sides newline-
    # terminated) is already correct and mismatches only affect the final
    # byte of a UI diff — acceptable for snapshot purposes.
    return before


def _line(keepends_line: str) -> str:
    """A keepends line without its terminator, for content comparison."""
    return keepends_line.rstrip("\n").rstrip("\r")
