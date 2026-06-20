"""Interactive tool handler — pauses agent execution for user input.

Implements the `can_use_tool` callback for the Claude Agent SDK.
Interactive tools (AskUserQuestion, ExitPlanMode, EnterPlanMode) pause
the agent mid-turn, broadcast to the UI via WebSocket, and resume once
the user responds.

File-modifying tools (Edit, Write, NotebookEdit) trigger a pre-execution
file snapshot to capture original content for diff computation.

All other tools are auto-approved with zero overhead.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Coroutine
from uuid import uuid4

from claude_agent_sdk.types import (
    PermissionResult,
    PermissionResultAllow,
    PermissionResultDeny,
    ToolPermissionContext,
)

logger = logging.getLogger(__name__)

# Default timeout for interactive tool waits.
# Non-web sessions auto-deny upfront via `interactive_capable`, so this only
# guards web sessions where the user might step away mid-approval. Upstream
# uses 1h; I extended it to 24h after pending interactions kept timing out
# during long plan reads — keep that here.
INTERACTION_TIMEOUT = 24 * 60 * 60

# Tools that require user interaction before execution
INTERACTIVE_TOOLS = frozenset({
    "AskUserQuestion",
    "ExitPlanMode",
    "EnterPlanMode",
})

# Tools that modify files — trigger pre-execution snapshot
FILE_MODIFY_TOOLS = frozenset({
    "Edit",
    "Write",
    "NotebookEdit",
})

# Max file size to snapshot (1 MB)
_MAX_SNAPSHOT_SIZE = 1_024 * 1_024

# Type for async snapshot callback: fn(session_id, file_path, content)
SnapshotCallback = Callable[[str, str, str | None], Coroutine[Any, Any, None]]


@dataclass
class PendingInteraction:
    """A pending user interaction waiting for resolution."""
    interaction_id: str
    tool_name: str
    tool_input: dict[str, Any]
    event: asyncio.Event = field(default_factory=asyncio.Event)
    result: dict[str, Any] | None = None
    denied: bool = False
    deny_message: str = ""


class InteractiveToolHandler:
    """Per-session handler that intercepts interactive tool calls.

    Created for each session and registered with the SDK via can_use_tool.
    The WebSocket server routes user answers to the correct handler via
    the global registry.

    Also captures file content snapshots before file-modifying tools execute,
    enabling session-scoped diff computation.
    """

    def __init__(
        self,
        session_id: str,
        broadcast_fn,
        snapshot_fn: SnapshotCallback | None = None,
        interactive_capable: bool = True,
    ):
        """
        Args:
            session_id: The Nerve session this handler belongs to.
            broadcast_fn: async fn(session_id, message_dict) — the broadcaster.
            snapshot_fn: Optional async fn(session_id, file_path, content) — persists
                         original file content before modification for diff view.
            interactive_capable: Whether the session channel supports interactive
                                 tools (WebSocket UI). Non-interactive channels
                                 (Telegram, cron) auto-deny to prevent deadlocks.
        """
        self.session_id = session_id
        self._broadcast = broadcast_fn
        self._snapshot_fn = snapshot_fn
        self._interactive_capable = interactive_capable
        self._pending: dict[str, PendingInteraction] = {}
        self._captured_files: set[str] = set()  # paths already snapshotted this session

    async def can_use_tool(
        self,
        tool_name: str,
        tool_input: dict[str, Any],
        context: ToolPermissionContext,
    ) -> PermissionResult:
        """SDK permission callback.

        Captures file snapshots before file-modifying tools, then
        auto-approves non-interactive tools.
        """
        # Capture pre-execution file snapshot for diff tracking
        # (Also done via PreToolUse hook in engine.py as primary mechanism)
        if self._snapshot_fn and tool_name in FILE_MODIFY_TOOLS:
            file_path = tool_input.get("file_path") or tool_input.get("notebook_path")
            if file_path and file_path not in self._captured_files:
                self._captured_files.add(file_path)
                content = _read_file_safe(file_path)
                try:
                    await self._snapshot_fn(self.session_id, file_path, content)
                except Exception as e:
                    logger.warning("Failed to save file snapshot for %s: %s", file_path, e)

        if tool_name not in INTERACTIVE_TOOLS:
            return PermissionResultAllow()

        # Non-interactive channels: deny immediately to prevent deadlocks
        if not self._interactive_capable:
            deny_messages = {
                "AskUserQuestion": (
                    "AskUserQuestion is not available in this channel. "
                    "Use the Nerve `ask_user` tool to ask the user questions asynchronously."
                ),
                "EnterPlanMode": (
                    "Plan mode is not available in non-web sessions. "
                    "Proceed with implementation directly."
                ),
                "ExitPlanMode": (
                    "Plan mode is not available in non-web sessions."
                ),
            }
            logger.info(
                "Session %s: auto-denying %s (non-interactive channel)",
                self.session_id, tool_name,
            )
            return PermissionResultDeny(
                message=deny_messages.get(
                    tool_name,
                    f"{tool_name} is not available in this channel.",
                )
            )

        return await self._handle_interactive(tool_name, tool_input)

    async def _handle_interactive(
        self, tool_name: str, tool_input: dict[str, Any],
    ) -> PermissionResult:
        """Pause execution, broadcast to UI, wait for user response."""
        interaction_id = str(uuid4())
        pending = PendingInteraction(
            interaction_id=interaction_id,
            tool_name=tool_name,
            tool_input=tool_input,
        )
        self._pending[interaction_id] = pending

        # Broadcast to UI
        await self._broadcast(self.session_id, {
            "type": "interaction",
            "session_id": self.session_id,
            "interaction_id": interaction_id,
            "interaction_type": _interaction_type(tool_name),
            "tool_name": tool_name,
            "tool_input": tool_input,
        })
        # Tell every connected client this session is now waiting for input,
        # so the sidebar can show the "waiting" indicator (blue dot).
        await self._broadcast_awaiting()

        logger.info(
            "Session %s: waiting for user input on %s (interaction %s)",
            self.session_id, tool_name, interaction_id[:8],
        )

        try:
            try:
                await asyncio.wait_for(pending.event.wait(), timeout=INTERACTION_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning(
                    "Session %s: interaction %s timed out after %ds",
                    self.session_id, interaction_id[:8], INTERACTION_TIMEOUT,
                )
                return PermissionResultDeny(
                    message=f"No response received after {_humanize_seconds(INTERACTION_TIMEOUT)} — timed out.",
                )
            except asyncio.CancelledError:
                logger.info("Session %s: interaction %s cancelled", self.session_id, interaction_id[:8])
                return PermissionResultDeny(
                    message="Session stopped by user.",
                    interrupt=True,
                )

            if pending.denied:
                logger.info("Session %s: %s denied by user", self.session_id, tool_name)
                return PermissionResultDeny(message=pending.deny_message or "Declined by user.")

            # For AskUserQuestion: inject answers into the tool input
            if tool_name == "AskUserQuestion" and pending.result:
                updated = {**tool_input, "answers": pending.result}
                return PermissionResultAllow(updated_input=updated)

            # For ExitPlanMode/EnterPlanMode: just allow
            return PermissionResultAllow()
        finally:
            # Always drop the pending entry and refresh the waiting indicator,
            # regardless of how the wait resolved (answered, denied, timeout,
            # cancelled). has_pending then reflects any remaining interaction.
            self._pending.pop(interaction_id, None)
            await self._broadcast_awaiting()

    async def _broadcast_awaiting(self) -> None:
        """Broadcast this session's waiting-for-input state to all clients.

        Sent on the global channel so every connected client updates the
        sidebar indicator for this session, even when viewing another one.
        Best-effort: a broadcast failure must not break the interaction flow.
        """
        try:
            await self._broadcast("__global__", {
                "type": "session_awaiting_input",
                "session_id": self.session_id,
                "awaiting": self.has_pending,
            })
        except Exception as e:  # pragma: no cover - defensive
            logger.debug(
                "Failed to broadcast awaiting-input state for %s: %s",
                self.session_id, e,
            )

    def resolve(self, interaction_id: str, result: dict[str, Any] | None = None) -> bool:
        """Resolve a pending interaction with the user's answer.

        Returns True if the interaction was found and resolved.
        """
        pending = self._pending.get(interaction_id)
        if not pending:
            logger.warning("No pending interaction %s", interaction_id[:8])
            return False

        pending.result = result
        pending.denied = False
        pending.event.set()
        return True

    def deny(self, interaction_id: str, message: str = "") -> bool:
        """Deny/reject a pending interaction.

        Returns True if the interaction was found and denied.
        """
        pending = self._pending.get(interaction_id)
        if not pending:
            logger.warning("No pending interaction %s to deny", interaction_id[:8])
            return False

        pending.denied = True
        pending.deny_message = message
        pending.event.set()
        return True

    def cancel_all(self) -> None:
        """Cancel all pending interactions (e.g., on session stop)."""
        for pending in self._pending.values():
            if not pending.event.is_set():
                pending.denied = True
                pending.deny_message = "Session stopped."
                pending.event.set()
        self._pending.clear()

    @property
    def has_pending(self) -> bool:
        return len(self._pending) > 0


# ------------------------------------------------------------------ #
#  File snapshot helpers                                               #
# ------------------------------------------------------------------ #

def _read_file_safe(file_path: str) -> str | None:
    """Read file content for snapshotting. Returns None if file doesn't exist."""
    try:
        p = Path(file_path)
        if not p.is_file():
            return None
        if p.stat().st_size > _MAX_SNAPSHOT_SIZE:
            logger.debug("Skipping snapshot for %s: file too large", file_path)
            return None
        return p.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.warning("Failed to read file for snapshot %s: %s", file_path, e)
        return None


# ------------------------------------------------------------------ #
#  Global handler registry                                            #
# ------------------------------------------------------------------ #

_handlers: dict[str, InteractiveToolHandler] = {}


def register_handler(session_id: str, handler: InteractiveToolHandler) -> None:
    """Register a handler so the WebSocket server can route answers."""
    _handlers[session_id] = handler


def unregister_handler(session_id: str) -> None:
    """Remove a handler from the registry."""
    handler = _handlers.pop(session_id, None)
    if handler:
        handler.cancel_all()


def get_handler(session_id: str) -> InteractiveToolHandler | None:
    """Get the handler for a session."""
    return _handlers.get(session_id)


def get_awaiting_ids() -> set[str]:
    """Return session IDs currently paused waiting for user input.

    Mirrors ``SessionManager.get_running_ids`` for the interactive layer:
    the REST sessions list uses it so a freshly-loaded UI shows the
    "waiting for input" indicator without relying on the live broadcast.
    """
    return {sid for sid, handler in _handlers.items() if handler.has_pending}


def _interaction_type(tool_name: str) -> str:
    """Map tool name to a UI-friendly interaction type."""
    return {
        "AskUserQuestion": "question",
        "ExitPlanMode": "plan_exit",
        "EnterPlanMode": "plan_enter",
    }.get(tool_name, "unknown")


def _humanize_seconds(seconds: int) -> str:
    """Human-readable duration for timeout messages (e.g. '1 hour', '5 minutes')."""
    if seconds >= 3600 and seconds % 3600 == 0:
        hours = seconds // 3600
        return f"{hours} hour{'s' if hours != 1 else ''}"
    minutes = max(1, seconds // 60)
    return f"{minutes} minute{'s' if minutes != 1 else ''}"
