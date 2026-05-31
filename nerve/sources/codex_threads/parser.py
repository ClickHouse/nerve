"""Parse Codex rollout JSONL lines into :class:`ThreadEvent` objects.

The rollout schema (from inspecting Codex 0.130.0 on the Pi):

  Outer types: ``session_meta``, ``turn_context``, ``response_item``,
  ``event_msg`` (plus ``compacted``/``state`` for older Codex versions).
  Each line has ``timestamp``, ``type``, ``payload``.

The parser prefers ``event_msg/*`` over ``response_item/*`` for user
input and final agent output — ``event_msg`` is Codex's deduplicated
UX-facing view and skips internals like the ``developer`` system block
and the auto-injected AGENTS.md ``user`` message.

Unknown types are dropped silently with a debug log so a future Codex
schema bump doesn't crash the sync — they'll just be skipped until the
parser learns about them.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from nerve.sources.codex_threads.base import ThreadEvent

logger = logging.getLogger(__name__)


def parse_timestamp(s: str | None) -> datetime | None:
    """Parse a Codex ISO 8601 timestamp (always trailing ``Z``)."""
    if not s:
        return None
    try:
        # ``fromisoformat`` only accepts ``Z`` from Python 3.11+; replace
        # explicitly so we don't depend on the runtime accepting it.
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        logger.debug("parse_timestamp: cannot parse %r", s)
        return None


def parse_rollout_line(
    raw: dict[str, Any],
    *,
    thread_id: str,
    sequence: int,
) -> ThreadEvent | None:
    """Convert one parsed rollout line into a :class:`ThreadEvent`.

    Returns ``None`` for lines we don't care about (developer messages,
    auto-injected AGENTS.md, token-count events, etc.). The caller MUST
    still advance the file offset even when this returns ``None``.
    """
    outer = raw.get("type")
    payload = raw.get("payload") or {}
    if not isinstance(payload, dict):
        return None
    inner = payload.get("type")
    ts = parse_timestamp(raw.get("timestamp"))

    # session_meta is treated specially — it's not a "real" event, but
    # the service uses it to create the satellite session. The thread_id
    # in the file is the canonical source; the caller's hint is ignored
    # if the meta carries one.
    if outer == "session_meta":
        # Caller decides scope via WorkspaceFilter; emit a placeholder
        # so the service can hand the payload to the ingester. The
        # 'thread_in_scope' / 'thread_out_of_scope' resolution happens
        # upstream in the origin.
        return ThreadEvent(
            type="thread_in_scope",       # Origin overwrites to OOS if needed
            thread_id=payload.get("id", thread_id),
            sequence=sequence,
            timestamp=ts,
            payload=payload,
        )

    if outer == "turn_context":
        tid = payload.get("turn_id", "")
        return ThreadEvent(
            type="turn_started",
            thread_id=thread_id,
            sequence=sequence,
            timestamp=ts,
            payload={"turn_id": tid, "context": payload},
        )

    if outer == "event_msg":
        if inner == "task_started":
            return ThreadEvent(
                type="turn_started",
                thread_id=thread_id,
                sequence=sequence,
                timestamp=ts,
                payload=payload,
            )
        if inner == "task_complete":
            return ThreadEvent(
                type="turn_completed",
                thread_id=thread_id,
                sequence=sequence,
                timestamp=ts,
                payload=payload,
            )
        if inner == "user_message":
            return ThreadEvent(
                type="user_message",
                thread_id=thread_id,
                sequence=sequence,
                timestamp=ts,
                payload=payload,
            )
        if inner == "agent_message":
            return ThreadEvent(
                type="assistant_message",
                thread_id=thread_id,
                sequence=sequence,
                timestamp=ts,
                payload=payload,
            )
        if inner == "mcp_tool_call_begin":
            return ThreadEvent(
                type="tool_call",
                thread_id=thread_id,
                sequence=sequence,
                timestamp=ts,
                payload=payload,
            )
        if inner == "mcp_tool_call_end":
            # End carries both the call (structured arguments) and the
            # result (Ok/Err), so the translator emits BOTH a tool_call
            # and a tool_result message. We tag the event as tool_result
            # and the translator handles fan-out.
            return ThreadEvent(
                type="tool_result",
                thread_id=thread_id,
                sequence=sequence,
                timestamp=ts,
                payload=payload,
            )
        if inner == "token_count":
            return None       # usage-only — not part of the transcript
        # Unknown event_msg type — skip but log so we notice schema
        # evolution during smoke tests.
        logger.debug(
            "parse_rollout_line: unknown event_msg/%s (thread=%s seq=%d)",
            inner, thread_id, sequence,
        )
        return None

    if outer == "response_item":
        if inner == "message":
            role = payload.get("role")
            if role == "user":
                # Skip: Codex auto-injects AGENTS.md as a user message;
                # the real user input lives in event_msg/user_message.
                return None
            if role == "developer":
                # Codex internal sandbox/skill instructions — skip.
                return None
            if role == "assistant":
                # Skip too: agent_message in event_msg is the canonical
                # one. response_item/message/assistant is the raw wire
                # form sent back to OpenAI and may include intermediate
                # phases we don't want duplicated.
                return None
            logger.debug("parse_rollout_line: response_item/message role=%r", role)
            return None
        if inner == "reasoning":
            return ThreadEvent(
                type="reasoning",
                thread_id=thread_id,
                sequence=sequence,
                timestamp=ts,
                payload=payload,
            )
        if inner == "function_call":
            return ThreadEvent(
                type="tool_call",
                thread_id=thread_id,
                sequence=sequence,
                timestamp=ts,
                payload=payload,
            )
        if inner == "function_call_output":
            return ThreadEvent(
                type="tool_result",
                thread_id=thread_id,
                sequence=sequence,
                timestamp=ts,
                payload=payload,
            )
        logger.debug("parse_rollout_line: unknown response_item/%s", inner)
        return None

    # Unknown outer type — Codex schema bump. Don't crash.
    logger.debug("parse_rollout_line: unknown outer type %r", outer)
    return None
