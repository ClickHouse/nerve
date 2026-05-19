"""Core types for the Codex thread sync source.

Every concrete origin (``LocalRolloutOrigin``, ``AppServerOrigin``,
``CloudCodexOrigin``) speaks the same vocabulary defined here so the
ingester and translator stay origin-agnostic.

The structure mirrors the on-disk Codex rollout format with one
abstraction layer: an origin yields :class:`ThreadEvent`s tagged with
the canonical event type so the translator doesn't have to know whether
the bytes came from a JSONL file, a Unix socket, or a cloud API.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
    Literal,
    Protocol,
    runtime_checkable,
)

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Workspace filter — file-open-time scope decision
# ----------------------------------------------------------------------

@dataclass
class WorkspaceFilter:
    """Decides whether a thread is in scope based on ``session_meta.cwd``.

    Codex rollouts on a developer box can include sessions from many
    directories. Nerve only wants the ones run inside the configured
    workspace (or an explicit allowlist).

    The check happens at file-open time using a single line read — the
    first line of every rollout is ``session_meta`` which carries the
    cwd, so out-of-scope rollouts skip the rest of the file.
    """

    mode: Literal["nerve_workspace", "explicit", "any"] = "nerve_workspace"
    nerve_workspace_path: Path | None = None
    explicit_paths: list[Path] = field(default_factory=list)

    def matches(self, cwd: str | Path | None) -> bool:
        if self.mode == "any":
            return True
        if cwd is None:
            return False
        try:
            cwd_resolved = Path(cwd).expanduser().resolve()
        except (OSError, RuntimeError):
            logger.warning("WorkspaceFilter: cannot resolve cwd %r", cwd)
            return False
        if self.mode == "nerve_workspace":
            if self.nerve_workspace_path is None:
                return False
            try:
                ws = self.nerve_workspace_path.expanduser().resolve()
            except (OSError, RuntimeError):
                return False
            return cwd_resolved == ws
        if self.mode == "explicit":
            for p in self.explicit_paths:
                try:
                    if cwd_resolved == p.expanduser().resolve():
                        return True
                except (OSError, RuntimeError):
                    continue
            return False
        # Unknown mode — fail closed.
        logger.warning("WorkspaceFilter: unknown mode %r, denying", self.mode)
        return False


# ----------------------------------------------------------------------
# Session metadata (parsed from session_meta line)
# ----------------------------------------------------------------------

@dataclass
class SessionMeta:
    thread_id: str
    cwd: str | None
    originator: str = ""        # "codex_exec" | "codex_tui" | ...
    source: str = ""            # "exec" | "tui" | ...
    cli_version: str = ""
    model_provider: str = ""
    base_instructions: str = ""
    started_at: datetime | None = None

    @classmethod
    def from_payload(cls, payload: dict, ts: datetime | None) -> SessionMeta:
        bi = payload.get("base_instructions") or {}
        base = bi.get("text", "") if isinstance(bi, dict) else ""
        return cls(
            thread_id=payload.get("id", ""),
            cwd=payload.get("cwd"),
            originator=payload.get("originator", ""),
            source=payload.get("source", ""),
            cli_version=payload.get("cli_version", ""),
            model_provider=payload.get("model_provider", ""),
            base_instructions=base,
            started_at=ts,
        )


# ----------------------------------------------------------------------
# Event types
# ----------------------------------------------------------------------

ThreadEventType = Literal[
    "thread_in_scope",       # session_meta seen, cwd matches the filter
    "thread_out_of_scope",   # session_meta seen, cwd does NOT match
    "turn_started",
    "turn_completed",
    "user_message",
    "assistant_message",
    "reasoning",             # encrypted blob, placeholder rendered in UI
    "tool_call",             # function_call OR mcp_tool_call_begin/end
    "tool_result",           # function_call_output OR mcp_tool_call_end
    "thread_archived",       # file moved to archived_sessions/
]


@dataclass
class ThreadEvent:
    """A semantically-tagged event from an origin.

    ``sequence`` is a monotonic per-thread integer. For file origins
    this is the byte offset where the line ENDS (so the next read
    can resume from the same offset). For RPC origins it's a
    per-thread counter.
    """

    type: ThreadEventType
    thread_id: str
    sequence: int
    timestamp: datetime | None
    payload: dict[str, Any]


# ----------------------------------------------------------------------
# Origin protocol
# ----------------------------------------------------------------------

@runtime_checkable
class CodexOrigin(Protocol):
    """The contract every concrete origin honours.

    Implementations live in :mod:`nerve.sources.codex_threads.origins`.
    """

    id: str

    async def initialize(self) -> None:
        """Cheap setup — create directories, open sockets, etc."""
        ...

    async def close(self) -> None:
        """Cleanup hook. Always idempotent."""
        ...

    def stream(
        self,
        cursor: str | None,
    ) -> AsyncIterator[ThreadEvent]:
        """Yield events forever until cancelled.

        ``cursor`` is the opaque resume token from a prior run (typically
        a JSON blob). ``None`` means "start from the beginning of
        everything you know about" — origins decide whether that means
        replaying history or only future deltas.

        Implementations must return an async iterator. The service loop
        consumes events and calls :meth:`save_cursor` periodically.
        """
        ...

    def cursor(self) -> str:
        """Return the current resume token (called by the service)."""
        ...
