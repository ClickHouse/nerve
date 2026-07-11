"""Interactive tool handler — pauses agent execution for user input.

Backend-neutral pause/approve machinery. A backend adapter (e.g.
:class:`nerve.agent.backends.claude.ClaudeToolPermissions` for the SDK's
``can_use_tool`` callback, or the Codex approval-request handlers)
translates its runtime's permission surface into
:meth:`InteractiveToolHandler.request_interaction` /
:meth:`InteractiveToolHandler.request_approval` calls; the hub broadcasts
to the UI via WebSocket ``interaction`` events and resumes once the user
responds.

No agent-runtime types (``claude_agent_sdk`` / Codex protocol) may be
imported here — this module sits on the engine side of the backend seam.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Coroutine
from uuid import uuid4

logger = logging.getLogger(__name__)

# Default timeout for interactive tool waits (1 hour)
INTERACTION_TIMEOUT = 3600

# Claude CLI built-ins that require user interaction before execution.
# Defined here (not in the claude backend) so the neutral registry and
# tests can reference the set without importing backend modules.
INTERACTIVE_TOOLS = frozenset({
    "AskUserQuestion",
    "ExitPlanMode",
    "EnterPlanMode",
})

# Approval kinds surfaced by sandboxed backends (Codex approval requests).
APPROVAL_KINDS = frozenset({
    "command_approval",
    "file_approval",
    "permission_approval",
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


@dataclass
class InteractionOutcome:
    """How a pause resolved — the backend adapter maps this onto its
    runtime's permission vocabulary."""

    denied: bool = False
    cancelled: bool = False          # session stopped while waiting
    message: str = ""
    result: dict[str, Any] | None = None

    @property
    def approved(self) -> bool:
        return not self.denied and not self.cancelled


class InteractiveToolHandler:
    """Per-session hub that pauses turns for user input.

    Created for each session and registered in the global registry; the
    WebSocket server routes user answers to the correct hub. Backend
    adapters call :meth:`request_interaction` (interactive built-ins) or
    :meth:`request_approval` (sandbox approval requests) and translate
    the :class:`InteractionOutcome`.

    Also tracks which files were already snapshotted this session so
    permission adapters can capture pre-modification content exactly once
    (see :meth:`mark_snapshotted`).
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
            session_id: The Nerve session this hub belongs to.
            broadcast_fn: async fn(session_id, message_dict) — the broadcaster.
            snapshot_fn: Optional async fn(session_id, file_path, content) —
                         persists original file content before modification.
            interactive_capable: Whether the session channel supports
                                 interactive pauses (WebSocket UI).
                                 Non-interactive channels (Telegram, cron)
                                 auto-deny to prevent deadlocks.
        """
        self.session_id = session_id
        self._broadcast = broadcast_fn
        self.snapshot_fn = snapshot_fn
        self.interactive_capable = interactive_capable
        self._pending: dict[str, PendingInteraction] = {}
        self._captured_files: set[str] = set()  # paths snapshotted this session

    # -- snapshot bookkeeping ------------------------------------------- #

    def mark_snapshotted(self, file_path: str) -> bool:
        """Record *file_path* as snapshotted; True when newly recorded."""
        if file_path in self._captured_files:
            return False
        self._captured_files.add(file_path)
        return True

    # -- pause requests -------------------------------------------------- #

    async def request_interaction(
        self,
        tool_name: str,
        tool_input: dict[str, Any],
        timeout: float | None = None,
    ) -> InteractionOutcome:
        """Pause for an interactive built-in (question / plan approval)."""
        if not self.interactive_capable:
            logger.info(
                "Session %s: auto-denying %s (non-interactive channel)",
                self.session_id, tool_name,
            )
            return InteractionOutcome(
                denied=True,
                message=f"{tool_name} is not available in this channel.",
            )
        return await self._pause(
            tool_name, tool_input, _interaction_type(tool_name), timeout=timeout,
        )

    async def request_approval(
        self, kind: str, payload: dict[str, Any],
    ) -> InteractionOutcome:
        """Pause for a backend approval request (command / file change).

        ``kind`` must be one of :data:`APPROVAL_KINDS`; ``payload`` is the
        backend's request context (command, cwd, file changes, reason...)
        rendered by the UI's approval card.
        """
        if not self.interactive_capable:
            logger.info(
                "Session %s: auto-declining %s (non-interactive channel)",
                self.session_id, kind,
            )
            return InteractionOutcome(
                denied=True,
                message="Approval requests are not available in this channel.",
            )
        return await self._pause(kind, payload, kind)

    async def _pause(
        self,
        tool_name: str,
        tool_input: dict[str, Any],
        interaction_type: str,
        timeout: float | None = None,
    ) -> InteractionOutcome:
        """Broadcast to the UI, wait for the user's response."""
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
            "interaction_type": interaction_type,
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
                wait_timeout = INTERACTION_TIMEOUT if timeout is None else max(0.1, timeout)
                await asyncio.wait_for(pending.event.wait(), timeout=wait_timeout)
            except asyncio.TimeoutError:
                logger.warning(
                    "Session %s: interaction %s timed out after %ds",
                    self.session_id, interaction_id[:8], wait_timeout,
                )
                return InteractionOutcome(
                    denied=True,
                    message=(
                        f"No response received after "
                        f"{_humanize_seconds(wait_timeout)} — timed out."
                    ),
                )
            except asyncio.CancelledError:
                logger.info(
                    "Session %s: interaction %s cancelled",
                    self.session_id, interaction_id[:8],
                )
                return InteractionOutcome(
                    denied=True, cancelled=True,
                    message="Session stopped by user.",
                )

            if pending.denied:
                logger.info(
                    "Session %s: %s denied by user", self.session_id, tool_name,
                )
                return InteractionOutcome(
                    denied=True,
                    message=pending.deny_message or "Declined by user.",
                )

            return InteractionOutcome(result=pending.result)
        finally:
            # Always drop the pending entry and refresh the waiting indicator,
            # regardless of how the wait resolved (answered, denied, timeout,
            # cancelled). has_pending then reflects any remaining interaction.
            self._pending.pop(interaction_id, None)
            await self._broadcast_awaiting()
            # Tell every client this interaction is settled so parallel clients
            # clear their pending poll/plan prompt (the answering client cleared
            # it locally). Buffered for reconnect replay on the session channel.
            # Best-effort: a broadcast failure must not break the interaction flow.
            try:
                await self._broadcast(self.session_id, {
                    "type": "interaction_resolved",
                    "session_id": self.session_id,
                    "interaction_id": interaction_id,
                })
            except Exception as e:  # pragma: no cover - defensive
                logger.debug(
                    "Failed to broadcast interaction_resolved for %s: %s",
                    self.session_id, e,
                )

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
