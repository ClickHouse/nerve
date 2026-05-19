"""Local filesystem origin — tail Codex rollout JSONL files in-place.

Watches ``~/.codex/sessions/`` and ``~/.codex/archived_sessions/`` for
new or grown files. Each rollout starts with a ``session_meta`` line
carrying the thread's ``cwd``; reading just that one line is enough to
decide whether the rest of the file is in scope, so out-of-scope
rollouts cost us a single ``readline()`` and nothing more.

The implementation uses a simple poll-based scan (no inotify
dependency). On a Pi 5 with single-digit rollouts per workspace this
is comfortably under a millisecond per pass; if Codex ever produces
hundreds of in-flight rollouts we can swap in ``aionotify`` without
touching the parser/translator/ingester.

Cursor format (JSON):

  {
    "files": {"<absolute_path>": <byte_offset>, ...},
    "in_scope": ["<absolute_path>", ...],
    "out_of_scope": ["<absolute_path>", ...],
    "archived": ["<absolute_path>", ...]
  }

Cursors are persisted by the service via ``sync_cursors``, keyed on
``codex:<origin_id>``.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator

from nerve.sources.codex_threads.base import (
    CodexOrigin,
    ThreadEvent,
    WorkspaceFilter,
)
from nerve.sources.codex_threads.parser import parse_rollout_line

logger = logging.getLogger(__name__)


# Threshold below which we assume a file's "session_meta" line isn't
# fully flushed yet and back off rather than panic-skip the file.
_MIN_SESSION_META_BYTES = 32


class LocalRolloutOrigin(CodexOrigin):
    """Poll-based tail of local Codex rollout files."""

    def __init__(
        self,
        *,
        id: str,
        sessions_path: Path,
        archive_path: Path,
        workspace_filter: WorkspaceFilter,
        poll_interval_seconds: float = 2.0,
    ) -> None:
        self.id = id
        self.sessions_path = sessions_path.expanduser()
        self.archive_path = archive_path.expanduser()
        self.filter = workspace_filter
        self.poll_interval_seconds = max(0.25, poll_interval_seconds)

        # State (also serialized into the cursor)
        self._offsets: dict[Path, int] = {}
        self._in_scope: set[Path] = set()
        self._out_of_scope: set[Path] = set()
        self._archived_emitted: set[Path] = set()
        # Maps file -> last-known thread_id so we can rebuild events
        # after a partial-line backoff.
        self._thread_ids: dict[Path, str] = {}
        self._closed = False

    # ------------------------------------------------------------------
    # CodexOrigin protocol
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        for p in (self.sessions_path, self.archive_path):
            try:
                p.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                logger.warning("LocalRolloutOrigin: cannot create %s: %s", p, e)

    async def close(self) -> None:
        self._closed = True

    async def stream(
        self, cursor: str | None,
    ) -> AsyncIterator[ThreadEvent]:
        self._load_cursor(cursor)

        # Initial sweep: replay everything we know about. Each scan is
        # cheap (offsets cached) and lets a freshly-started Nerve catch
        # up on rollouts that grew while it was down.
        async for evt in self._scan_once():
            yield evt

        while not self._closed:
            try:
                await asyncio.sleep(self.poll_interval_seconds)
            except asyncio.CancelledError:
                break
            if self._closed:
                break
            async for evt in self._scan_once():
                yield evt

    def cursor(self) -> str:
        return json.dumps({
            "files": {str(k): v for k, v in self._offsets.items()},
            "in_scope": sorted(str(p) for p in self._in_scope),
            "out_of_scope": sorted(str(p) for p in self._out_of_scope),
            "archived": sorted(str(p) for p in self._archived_emitted),
        })

    # ------------------------------------------------------------------
    # Cursor load
    # ------------------------------------------------------------------

    def _load_cursor(self, cursor: str | None) -> None:
        if not cursor:
            return
        try:
            data = json.loads(cursor)
        except (TypeError, json.JSONDecodeError):
            logger.warning(
                "LocalRolloutOrigin %s: malformed cursor, starting fresh", self.id,
            )
            return
        self._offsets = {
            Path(k): int(v) for k, v in (data.get("files") or {}).items()
        }
        self._in_scope = {Path(p) for p in (data.get("in_scope") or [])}
        self._out_of_scope = {Path(p) for p in (data.get("out_of_scope") or [])}
        self._archived_emitted = {Path(p) for p in (data.get("archived") or [])}

    # ------------------------------------------------------------------
    # Filesystem scan
    # ------------------------------------------------------------------

    async def _scan_once(self) -> AsyncIterator[ThreadEvent]:
        """One scan pass: walk both dirs and tail each known file."""
        # Active sessions
        for path in _list_rollouts(self.sessions_path):
            async for evt in self._tail_file(path, archived=False):
                yield evt

        # Archived sessions
        for path in _list_rollouts(self.archive_path):
            async for evt in self._tail_file(path, archived=True):
                yield evt

    async def _tail_file(
        self, path: Path, *, archived: bool,
    ) -> AsyncIterator[ThreadEvent]:
        # Scope decision (cached)
        if path in self._out_of_scope:
            return
        if path not in self._in_scope:
            decided = await self._decide_scope(path)
            if decided is False:
                # Filter rejected — emit a no-op marker so the ingester
                # remembers and the cursor records it. Use the path
                # stem as a synthetic thread id (we never saw the real
                # one).
                thread_id = _path_to_thread_id(path) or path.stem
                yield ThreadEvent(
                    type="thread_out_of_scope",
                    thread_id=thread_id,
                    sequence=0,
                    timestamp=None,
                    payload={"path": str(path)},
                )
                return
            if decided is None:
                # First line not flushed yet — back off, try again
                # next pass.
                return

        thread_id = self._thread_ids.get(path) or _path_to_thread_id(path) or path.stem
        offset = self._offsets.get(path, 0)

        # Returns (final_offset, [(end_offset_of_line, event_or_None), ...]).
        # Even lines that produce no event advance the offset so we don't
        # re-parse them next pass.
        final_offset, scanned = await asyncio.to_thread(
            _read_jsonl_lines, path, offset, thread_id,
        )
        for end_offset, evt in scanned:
            # Advance the offset BEFORE yielding so a consumer that
            # stops mid-stream still has a correct cursor — the events
            # already yielded have been processed by the consumer; the
            # ones we never reached weren't.
            self._offsets[path] = end_offset
            if evt is None:
                continue
            if evt.type == "thread_in_scope":
                self._thread_ids[path] = evt.thread_id
            yield evt
        # If no parsed events but the offset advanced (e.g. malformed
        # JSON lines we skipped), still record the final position.
        if final_offset > self._offsets.get(path, 0):
            self._offsets[path] = final_offset

        # If the file lives in archived_sessions/, emit a one-time
        # archival sentinel once we've finished tailing it.
        if archived and path not in self._archived_emitted:
            self._archived_emitted.add(path)
            yield ThreadEvent(
                type="thread_archived",
                thread_id=self._thread_ids.get(path, thread_id),
                sequence=self._offsets.get(path, 0),
                timestamp=datetime.now(timezone.utc),
                payload={"path": str(path)},
            )

    async def _decide_scope(self, path: Path) -> bool | None:
        """Read only the first line to check the workspace filter.

        Returns ``True``/``False`` once a decision is reached, or
        ``None`` if the file is too short for ``session_meta`` to have
        been flushed yet (caller backs off and retries next pass).
        """
        try:
            data = await asyncio.to_thread(_read_first_line, path)
        except (OSError, ValueError) as e:
            logger.warning("LocalRolloutOrigin: cannot read %s: %s", path, e)
            self._out_of_scope.add(path)
            return False
        if data is None:
            return None
        try:
            obj = json.loads(data)
        except (TypeError, json.JSONDecodeError):
            logger.debug("LocalRolloutOrigin: %s first line not JSON", path)
            return None
        if obj.get("type") != "session_meta":
            logger.warning(
                "LocalRolloutOrigin: %s does not start with session_meta", path,
            )
            self._out_of_scope.add(path)
            return False
        payload = obj.get("payload") or {}
        cwd = payload.get("cwd") if isinstance(payload, dict) else None
        in_scope = self.filter.matches(cwd)
        if in_scope:
            self._in_scope.add(path)
            tid = payload.get("id", "") if isinstance(payload, dict) else ""
            if tid:
                self._thread_ids[path] = tid
            return True
        self._out_of_scope.add(path)
        return False


# ----------------------------------------------------------------------
# Module-level helpers (run in threads — they touch the filesystem)
# ----------------------------------------------------------------------

def _list_rollouts(root: Path) -> list[Path]:
    if not root.exists():
        return []
    try:
        return sorted(root.rglob("*.jsonl"))
    except OSError:
        return []


def _read_first_line(path: Path) -> str | None:
    """Read just enough bytes to recover the first JSONL line, if any."""
    try:
        size = path.stat().st_size
    except OSError:
        return None
    if size < _MIN_SESSION_META_BYTES:
        return None
    with path.open("rb") as f:
        # Allocate at most 64 KiB for the first line. Codex session_meta
        # is comfortably under 8 KiB on the Pi but the base_instructions
        # text can balloon if a future version adds documentation.
        chunk = f.read(65536)
    nl = chunk.find(b"\n")
    if nl < 0:
        # Header not yet flushed
        return None
    return chunk[:nl].decode("utf-8", errors="replace")


def _read_jsonl_lines(
    path: Path, offset: int, thread_id_hint: str,
) -> tuple[int, list[tuple[int, ThreadEvent | None]]]:
    """Read full lines starting at ``offset``.

    Returns ``(final_offset, [(end_offset, event_or_None), ...])``. The
    caller advances the persisted offset to ``end_offset`` BEFORE
    yielding ``event_or_None`` so a mid-stream cancellation leaves the
    cursor consistent with the events the consumer actually saw.

    Lines that don't end in ``\\n`` are skipped (partial tail) so the
    same offset is retried next pass once Codex flushes the newline.
    """
    scanned: list[tuple[int, ThreadEvent | None]] = []
    try:
        size = path.stat().st_size
    except OSError:
        return offset, scanned
    if size <= offset:
        return offset, scanned
    try:
        with path.open("rb") as f:
            f.seek(offset)
            buf = f.read(size - offset)
    except OSError as e:
        logger.warning("Cannot read %s at offset %d: %s", path, offset, e)
        return offset, scanned

    new_offset = offset
    for line in buf.splitlines(keepends=True):
        if not line.endswith((b"\n",)):
            # Partial — stop here, retry next pass.
            break
        line_str = line.decode("utf-8", errors="replace").rstrip("\n")
        new_offset += len(line)
        if not line_str:
            scanned.append((new_offset, None))
            continue
        try:
            raw = json.loads(line_str)
        except (TypeError, json.JSONDecodeError):
            logger.debug("Skipping malformed line in %s at offset %d", path, new_offset)
            scanned.append((new_offset, None))
            continue
        # Sequence = byte offset where this line ENDS, providing a
        # monotonic per-thread counter that's stable across restarts.
        evt = parse_rollout_line(
            raw,
            thread_id=thread_id_hint,
            sequence=new_offset,
        )
        scanned.append((new_offset, evt))
    return new_offset, scanned


def _path_to_thread_id(path: Path) -> str | None:
    """Try to extract a Codex thread UUID from a rollout filename.

    Filename pattern: ``rollout-<ISO-ts>-<thread-uuid>.jsonl``. Returns
    None if the filename doesn't match. The result is just a hint —
    the canonical thread id always comes from the in-file
    ``session_meta`` payload.
    """
    stem = path.stem  # rollout-<ISO-timestamp>-<thread-uuid>...
    if not stem.startswith("rollout-"):
        return None
    parts = stem.split("-")
    if len(parts) < 4:
        return None
    # The UUID is the LAST 5 hyphen-joined groups (UUID4 is 8-4-4-4-12)
    if len(parts) >= 5:
        candidate = "-".join(parts[-5:])
        if len(candidate) >= 32:
            return candidate
    return None
