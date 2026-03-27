"""Streaming bridge — routes SDK async messages to WebSocket and Telegram.

Broadcasts tokens, tool calls, thinking, and completion events to all
connected interfaces for a given session. Buffers events during active
runs for replay on client reconnect. Buffers are bounded to prevent
unbounded memory growth on long-running sessions.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine

logger = logging.getLogger(__name__)

# Type for broadcast callback: async fn(session_id, message_dict)
BroadcastCallback = Callable[[str, dict[str, Any]], Coroutine[Any, Any, None]]

# Default max events per session buffer
MAX_BUFFER_SIZE = 10_000


@dataclass
class StreamEvent:
    """A single streaming event."""
    type: str  # "token", "thinking", "tool_use", "tool_result", "done", "error"
    session_id: str
    content: Any = None
    tool: str | None = None
    tool_input: dict | None = None


class StreamBroadcaster:
    """Manages broadcast callbacks for streaming agent responses.

    Multiple interfaces (WebSocket clients, Telegram) can register callbacks
    for a session. All registered callbacks receive every streaming event.
    Also buffers events during active runs for replay on reconnect.

    Buffers are bounded: when a session exceeds max_buffer_size events,
    the oldest events are dropped to keep memory usage predictable.
    """

    def __init__(self, max_buffer_size: int = MAX_BUFFER_SIZE):
        # session_id -> list of (callback_id, callback)
        self._listeners: dict[str, list[tuple[str, BroadcastCallback]]] = {}
        self._lock = asyncio.Lock()
        # Per-session event buffer for reconnect replay
        self._session_buffers: dict[str, list[dict[str, Any]]] = {}
        self._max_buffer_size = max_buffer_size

    async def register(self, session_id: str, callback_id: str, callback: BroadcastCallback) -> None:
        """Register a broadcast listener for a session."""
        async with self._lock:
            if session_id not in self._listeners:
                self._listeners[session_id] = []
            self._listeners[session_id].append((callback_id, callback))
            logger.debug("Registered listener %s for session %s", callback_id, session_id)

    async def unregister(self, session_id: str, callback_id: str) -> None:
        """Remove a broadcast listener."""
        async with self._lock:
            if session_id in self._listeners:
                self._listeners[session_id] = [
                    (cid, cb) for cid, cb in self._listeners[session_id]
                    if cid != callback_id
                ]
                if not self._listeners[session_id]:
                    del self._listeners[session_id]

    async def broadcast(self, session_id: str, message: dict[str, Any]) -> None:
        """Send a message to all listeners of a session. Also buffers if active."""
        # Buffer events during active streaming
        if session_id in self._session_buffers:
            buf = self._session_buffers[session_id]
            buf.append(message)
            # Enforce buffer limit — drop oldest events
            if len(buf) > self._max_buffer_size:
                self._session_buffers[session_id] = buf[-self._max_buffer_size:]

        async with self._lock:
            listeners = list(self._listeners.get(session_id, []))

        for callback_id, callback in listeners:
            try:
                await callback(session_id, message)
            except Exception as e:
                logger.warning("Broadcast to %s failed: %s", callback_id, e)

    # --- Buffering for reconnect replay ---

    def start_buffering(self, session_id: str) -> None:
        """Start buffering events for a session (call at run start)."""
        self._session_buffers[session_id] = []

    def stop_buffering(self, session_id: str) -> list[dict[str, Any]]:
        """Stop buffering and return accumulated events."""
        return self._session_buffers.pop(session_id, [])

    def get_buffer(self, session_id: str) -> list[dict[str, Any]]:
        """Get buffered events for a running session (for replay)."""
        return list(self._session_buffers.get(session_id, []))

    def is_buffering(self, session_id: str) -> bool:
        """Check if a session is currently buffering (i.e., running)."""
        return session_id in self._session_buffers

    def get_buffer_stats(self) -> dict[str, int]:
        """Return {session_id: event_count} for all active buffers."""
        return {sid: len(buf) for sid, buf in self._session_buffers.items()}

    # --- Typed broadcast helpers ---

    async def broadcast_token(self, session_id: str, text: str, parent_tool_use_id: str | None = None) -> None:
        msg: dict[str, Any] = {"type": "token", "session_id": session_id, "content": text}
        if parent_tool_use_id:
            msg["parent_tool_use_id"] = parent_tool_use_id
        await self.broadcast(session_id, msg)

    async def broadcast_thinking(self, session_id: str, text: str, parent_tool_use_id: str | None = None) -> None:
        msg: dict[str, Any] = {"type": "thinking", "session_id": session_id, "content": text}
        if parent_tool_use_id:
            msg["parent_tool_use_id"] = parent_tool_use_id
        await self.broadcast(session_id, msg)

    async def broadcast_tool_use(self, session_id: str, tool_name: str, tool_input: dict, tool_use_id: str | None = None, parent_tool_use_id: str | None = None) -> None:
        msg: dict[str, Any] = {
            "type": "tool_use",
            "session_id": session_id,
            "tool": tool_name,
            "input": tool_input,
            "tool_use_id": tool_use_id,
        }
        if parent_tool_use_id:
            msg["parent_tool_use_id"] = parent_tool_use_id
        await self.broadcast(session_id, msg)

    async def broadcast_tool_result(self, session_id: str, result: str, tool_use_id: str | None = None, is_error: bool = False, parent_tool_use_id: str | None = None) -> None:
        msg: dict[str, Any] = {
            "type": "tool_result",
            "session_id": session_id,
            "tool_use_id": tool_use_id,
            "result": result,
            "is_error": is_error,
        }
        if parent_tool_use_id:
            msg["parent_tool_use_id"] = parent_tool_use_id
        await self.broadcast(session_id, msg)

    async def broadcast_done(self, session_id: str, usage: dict[str, Any] | None = None, max_context_tokens: int | None = None) -> None:
        msg: dict[str, Any] = {"type": "done", "session_id": session_id}
        if usage is not None:
            msg["usage"] = usage
        if max_context_tokens is not None:
            msg["max_context_tokens"] = max_context_tokens
        await self.broadcast(session_id, msg)

    async def broadcast_plan_update(self, session_id: str, content: str) -> None:
        await self.broadcast(session_id, {"type": "plan_update", "session_id": session_id, "content": content})

    async def broadcast_interaction(self, session_id: str, interaction_type: str, interaction_id: str, tool_name: str, tool_input: dict) -> None:
        await self.broadcast(session_id, {
            "type": "interaction",
            "session_id": session_id,
            "interaction_id": interaction_id,
            "interaction_type": interaction_type,
            "tool_name": tool_name,
            "tool_input": tool_input,
        })

    async def broadcast_subagent_start(
        self,
        session_id: str,
        tool_use_id: str,
        subagent_type: str,
        description: str,
        model: str | None = None,
    ) -> None:
        await self.broadcast(session_id, {
            "type": "subagent_start",
            "session_id": session_id,
            "tool_use_id": tool_use_id,
            "subagent_type": subagent_type,
            "description": description,
            "model": model,
        })

    async def broadcast_subagent_complete(
        self,
        session_id: str,
        tool_use_id: str,
        duration_ms: int,
        is_error: bool = False,
    ) -> None:
        await self.broadcast(session_id, {
            "type": "subagent_complete",
            "session_id": session_id,
            "tool_use_id": tool_use_id,
            "duration_ms": duration_ms,
            "is_error": is_error,
        })

    async def broadcast_error(self, session_id: str, error: str) -> None:
        await self.broadcast(session_id, {"type": "error", "session_id": session_id, "error": error})

    async def broadcast_file_changed(
        self,
        session_id: str,
        path: str,
        operation: str,
        tool_use_id: str,
    ) -> None:
        await self.broadcast(session_id, {
            "type": "file_changed",
            "session_id": session_id,
            "path": path,
            "operation": operation,
            "tool_use_id": tool_use_id,
        })

    async def broadcast_auto_resume_start(self, session_id: str) -> None:
        """Notify the frontend that a background-task auto-resume is starting.

        Broadcast on the *session* channel (not __global__) so it reaches
        the connected WebSocket listener and is buffered for replay.
        The frontend uses this to enter streaming mode before tokens arrive.
        """
        await self.broadcast(session_id, {
            "type": "auto_resume_start",
            "session_id": session_id,
        })


# Global broadcaster instance
broadcaster = StreamBroadcaster()
