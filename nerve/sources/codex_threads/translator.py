"""Translate :class:`ThreadEvent`s into Nerve message blocks.

Each translator emits a list of ``StoredMessage`` records ready for
:class:`CodexIngester` to insert. The block format follows the
"combined" convention Nerve uses for native sessions (see
``nerve/agent/streaming.py``): tool calls and results are separate
messages with ``tool_call`` / ``tool_result`` blocks tagged by
``tool_use_id``.

External IDs are deterministic so the partial unique index added in
v028 deduplicates whether the same Codex item is seen via the file
sync or via the external MCP server:

  * user/assistant messages → ``msg:<event_id_or_seq>``
  * reasoning              → ``reasoning:<seq>``
  * tool calls/results     → ``tool_call:<call_id>``, ``tool_result:<call_id>``

The translator is tolerant: unknown fields land in ``external_metadata``
so a future schema bump only loses fidelity, not data.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from nerve.sources.codex_threads.base import ThreadEvent

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Output shape
# ----------------------------------------------------------------------

@dataclass
class StoredMessage:
    """Materialized message ready for the ingester.

    Mirrors the columns the ingester writes plus an ``external_id`` for
    idempotency.
    """

    role: str                      # "user" | "assistant" | "tool"
    external_id: str               # stable across runs
    content: str = ""              # plain text mirror (DB ``content`` col)
    blocks: list[dict] = field(default_factory=list)
    thinking: str | None = None
    created_at: datetime | None = None
    channel: str = "codex"
    external_metadata: dict = field(default_factory=dict)


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------

def translate_event(
    event: ThreadEvent,
    *,
    store_encrypted_reasoning: bool = True,
) -> list[StoredMessage]:
    """Translate a single :class:`ThreadEvent` into zero or more messages.

    Returns ``[]`` for events that are pure metadata (turn boundaries,
    in/out-of-scope sentinels, thread archive markers).
    """
    handler = _DISPATCH.get(event.type)
    if handler is None:
        return []
    try:
        return handler(event, store_encrypted_reasoning=store_encrypted_reasoning)
    except Exception:                  # pragma: no cover - defensive
        logger.exception(
            "translate_event: handler crashed for %s (seq=%d)",
            event.type, event.sequence,
        )
        return []


# ----------------------------------------------------------------------
# Individual handlers
# ----------------------------------------------------------------------

def _translate_user_message(
    event: ThreadEvent, *, store_encrypted_reasoning: bool,
) -> list[StoredMessage]:
    p = event.payload
    text = p.get("message", "")
    eid = p.get("event_id") or f"{event.thread_id}:user:{event.sequence}"
    blocks: list[dict] = []
    if text:
        blocks.append({"type": "text", "content": text})
    return [StoredMessage(
        role="user",
        external_id=f"msg:{eid}",
        content=text,
        blocks=blocks,
        created_at=event.timestamp,
        external_metadata={
            "images": p.get("images") or [],
            "local_images": p.get("local_images") or [],
            "text_elements": p.get("text_elements") or [],
        },
    )]


def _translate_assistant_message(
    event: ThreadEvent, *, store_encrypted_reasoning: bool,
) -> list[StoredMessage]:
    p = event.payload
    text = p.get("message", "")
    eid = p.get("event_id") or f"{event.thread_id}:assistant:{event.sequence}"
    blocks: list[dict] = []
    if text:
        blocks.append({"type": "text", "content": text})
    return [StoredMessage(
        role="assistant",
        external_id=f"msg:{eid}",
        content=text,
        blocks=blocks,
        created_at=event.timestamp,
        external_metadata={
            "phase": p.get("phase"),
            "memory_citation": p.get("memory_citation"),
        },
    )]


def _translate_reasoning(
    event: ThreadEvent, *, store_encrypted_reasoning: bool,
) -> list[StoredMessage]:
    """Reasoning blocks are encrypted by Codex.

    The plaintext never leaves the OpenAI side, so we store a
    placeholder ThinkingBlock plus (optionally) the encrypted blob in
    case a future Codex version exposes a decrypt path.
    """
    p = event.payload
    encrypted = p.get("encrypted_content") or ""
    summary_blocks = p.get("summary") or []
    placeholder = "(encrypted reasoning — Codex does not expose plaintext)"
    metadata: dict[str, Any] = {
        "encrypted_blob_length": len(encrypted),
        "summary": summary_blocks,
    }
    if store_encrypted_reasoning and encrypted:
        metadata["encrypted_content"] = encrypted
    return [StoredMessage(
        role="assistant",
        external_id=f"reasoning:{event.thread_id}:{event.sequence}",
        content="",
        blocks=[{"type": "thinking", "content": placeholder}],
        thinking=placeholder,
        created_at=event.timestamp,
        external_metadata=metadata,
    )]


def _translate_tool_call(
    event: ThreadEvent, *, store_encrypted_reasoning: bool,
) -> list[StoredMessage]:
    """Emit a tool_call message.

    Handles both flavours:

      * ``event_msg/mcp_tool_call_begin`` carries ``invocation: {server,
        tool, arguments}`` — structured.
      * ``response_item/function_call`` carries ``name`` + JSON-encoded
        ``arguments`` string — raw wire format.

    The deterministic ``external_id`` (``tool_call:<call_id>``) means
    the unique index drops duplicates whether the raw or structured
    form arrived first.
    """
    p = event.payload
    call_id = p.get("call_id") or ""
    if "invocation" in p and isinstance(p["invocation"], dict):
        inv = p["invocation"]
        server = inv.get("server", "")
        tool = inv.get("tool", "")
        tool_name = f"{server}.{tool}" if server else tool
        args = inv.get("arguments") or {}
        if not isinstance(args, dict):
            args = {"raw": args}
    else:
        tool_name = p.get("name", "")
        namespace = p.get("namespace", "")
        if namespace:
            tool_name = f"{namespace}{tool_name}"
        raw_args = p.get("arguments")
        args = _parse_tool_arguments(raw_args)

    block = {
        "type": "tool_call",
        "tool": tool_name,
        "input": args,
        "tool_use_id": call_id,
    }
    if not call_id:
        # Without a call_id we can't dedupe later. Fall back to
        # sequence so we at least don't crash, but log loud — this is
        # a Codex schema oddity worth knowing about.
        logger.warning(
            "tool_call without call_id (thread=%s seq=%d); "
            "using sequence-based external_id",
            event.thread_id, event.sequence,
        )
        call_id = f"seq-{event.thread_id}:{event.sequence}"
    return [StoredMessage(
        role="assistant",
        external_id=f"tool_call:{call_id}",
        content=tool_name,
        blocks=[block],
        created_at=event.timestamp,
    )]


def _translate_tool_result(
    event: ThreadEvent, *, store_encrypted_reasoning: bool,
) -> list[StoredMessage]:
    """Emit BOTH a tool_call message (if not already covered) and a
    tool_result message.

    ``mcp_tool_call_end`` carries the structured arguments AND the
    result, so when we see it the matching ``function_call`` row may
    already exist (from a prior ``response_item``) or may never appear
    (purely MCP-routed call). Emitting both is safe because the unique
    index dedupes on ``(session_id, tool_call:<call_id>)``.
    """
    p = event.payload
    call_id = p.get("call_id") or ""
    if not call_id:
        logger.warning(
            "tool_result without call_id (thread=%s seq=%d); skipping",
            event.thread_id, event.sequence,
        )
        return []

    messages: list[StoredMessage] = []

    # If the event carries an ``invocation`` we have enough to also
    # synthesize the tool_call. Idempotency is handled by the DB.
    if "invocation" in p and isinstance(p["invocation"], dict):
        inv = p["invocation"]
        server = inv.get("server", "")
        tool = inv.get("tool", "")
        tool_name = f"{server}.{tool}" if server else tool
        args = inv.get("arguments") or {}
        if not isinstance(args, dict):
            args = {"raw": args}
        messages.append(StoredMessage(
            role="assistant",
            external_id=f"tool_call:{call_id}",
            content=tool_name,
            blocks=[{
                "type": "tool_call",
                "tool": tool_name,
                "input": args,
                "tool_use_id": call_id,
            }],
            created_at=event.timestamp,
        ))

    if "result" in p and isinstance(p["result"], dict):
        # mcp_tool_call_end structured form
        if "Ok" in p["result"]:
            content = _stringify_result(p["result"]["Ok"])
            is_error = False
        else:
            content = _stringify_result(p["result"].get("Err", "unknown error"))
            is_error = True
    else:
        # response_item/function_call_output raw form
        raw = p.get("output", "")
        content = _strip_exec_header(raw) if isinstance(raw, str) else _stringify_result(raw)
        is_error = bool(p.get("is_error", False))

    messages.append(StoredMessage(
        role="tool",
        external_id=f"tool_result:{call_id}",
        content=content if isinstance(content, str) else json.dumps(content),
        blocks=[{
            "type": "tool_result",
            "tool_use_id": call_id,
            "result": content,
            "is_error": is_error,
        }],
        created_at=event.timestamp,
    ))
    return messages


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _parse_tool_arguments(raw: Any) -> dict:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return {"raw": raw}
        if isinstance(parsed, dict):
            return parsed
        return {"raw": parsed}
    return {"raw": raw}


def _stringify_result(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, (list, dict)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError):
            return str(value)
    return str(value)


def _strip_exec_header(output: str) -> str:
    """Codex ``exec_command`` output begins with a fixed header
    (``Chunk ID: ...\\nWall time: ...\\nProcess exited...\\nOutput:\\n``).
    Strip it for readability; if the header is missing return the
    original text untouched.
    """
    marker = "\nOutput:\n"
    if output.startswith(("Chunk ID:", "Wall time:")):
        idx = output.find(marker)
        if idx >= 0:
            return output[idx + len(marker):]
    return output


_DISPATCH = {
    "user_message":      _translate_user_message,
    "assistant_message": _translate_assistant_message,
    "reasoning":         _translate_reasoning,
    "tool_call":         _translate_tool_call,
    "tool_result":       _translate_tool_result,
}
