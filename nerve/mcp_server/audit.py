"""Record external tool calls into ``session_events`` for visibility.

External clients call tools without producing a conversation transcript
on Nerve's side, so satellite sessions would otherwise look like empty
rows in the DB. Writing one ``session_events`` row per call gives the
diagnostics views and the future per-session timeline UI something
concrete to show, and provides an audit trail for security review.

Args of long-running tools (``hoa_execute``, etc.) and big result blobs
are truncated to keep the event log compact; the unfiltered payload
lives in the broader conversation history if needed.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from nerve.agent.tools import ToolResult

if TYPE_CHECKING:
    from nerve.db import Database

logger = logging.getLogger(__name__)


_MAX_ARG_CHARS = 4_000
_MAX_RESULT_CHARS = 4_000


def _truncate(payload: str, limit: int) -> str:
    if len(payload) <= limit:
        return payload
    return payload[:limit] + f"... (truncated, {len(payload) - limit} chars)"


def _summarize_result(result: ToolResult) -> str:
    """Flatten a ToolResult into a single bounded string for the audit log."""
    chunks: list[str] = []
    for block in result.content:
        if isinstance(block, dict) and block.get("type") == "text":
            chunks.append(str(block.get("text", "")))
        else:
            chunks.append(repr(block))
    return _truncate("\n".join(chunks), _MAX_RESULT_CHARS)


def build_audit_writer(db: "Database"):
    """Build an async audit writer closure bound to a database handle.

    Returns a callable matching the ``AuditWriter`` protocol used by
    :func:`nerve.mcp_server.server.build_mcp_server`.
    """

    async def _write(
        session_id: str,
        tool_name: str,
        args: dict,
        result: ToolResult,
        duration_ms: float,
        is_error: bool,
    ) -> None:
        try:
            args_json = json.dumps(args, default=repr, ensure_ascii=False)
        except Exception:
            args_json = repr(args)
        details = {
            "tool": tool_name,
            "args": _truncate(args_json, _MAX_ARG_CHARS),
            "result": _summarize_result(result),
            "duration_ms": round(duration_ms, 2),
            "is_error": is_error,
        }
        try:
            await db.log_session_event(
                session_id=session_id,
                event_type="external_tool_call",
                details=details,
            )
        except Exception:
            logger.exception(
                "Failed to log external_tool_call for %s/%s",
                session_id, tool_name,
            )

    return _write
