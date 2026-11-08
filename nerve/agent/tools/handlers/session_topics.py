"""``session_open_topic`` / ``session_close_topic``: a session's Telegram topic.

What ``POST`` and ``DELETE /api/sessions/{id}/telegram-topic`` do, for an
agent: a run that starts a session for a piece of work can show it in a
Telegram topic of its own, and close the topic when the work is done. Both
default to the calling session. Offered only with Telegram configured and
``telegram.topics.enabled`` on (see ``config_excluded_tools``).
"""

from __future__ import annotations

import logging
from typing import Any

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec

logger = logging.getLogger(__name__)

_SESSION_ID = {
    "type": "string",
    "description": "The session. Empty: this session.",
    "default": "",
}

SESSION_OPEN_TOPIC_SCHEMA = {
    "type": "object",
    "properties": {
        "session_id": _SESSION_ID,
        "title": {
            "type": "string",
            "description": "The topic's name. Empty: the session's title.",
            "default": "",
        },
        "chat_id": {
            "type": "integer",
            "description": (
                "The Telegram chat to open the topic in. Leave it out for the "
                "notification chat (or the owner's DM)."
            ),
        },
    },
    "required": [],
}

SESSION_CLOSE_TOPIC_SCHEMA = {
    "type": "object",
    "properties": {
        "session_id": _SESSION_ID,
        "note": {
            "type": "string",
            "description": "A last message to post in the topic before it closes.",
            "default": "",
        },
    },
    "required": [],
}

_CLOSE_OUTCOMES = {
    "closed": "closed",
    "left_open": "unbound; Telegram cannot close a topic in a private chat, so it stays open",
    "gone": "unbound; the topic had been deleted already",
}


def _telegram(ctx: ToolContext) -> Any | None:
    """The Telegram channel when it is running, else ``None``."""
    engine = ctx.engine
    channel = engine.channel("telegram") if engine is not None and hasattr(engine, "channel") else None
    if channel is None or getattr(channel, "_app", None) is None:
        return None
    return channel


def _session_of(ctx: ToolContext, args: dict) -> str:
    return str(args.get("session_id") or "").strip() or ctx.session_id


async def session_open_topic_handler(ctx: ToolContext, args: dict) -> ToolResult:
    channel = _telegram(ctx)
    if channel is None:
        return ToolResult.text("Telegram is not running.", is_error=True)
    session_id = _session_of(ctx, args)
    chat_id = args.get("chat_id")
    try:
        result = await channel.open_session_topic(
            session_id,
            title=str(args.get("title") or "").strip() or None,
            chat_id=int(chat_id) if chat_id not in (None, "") else None,
        )
    except Exception as exc:
        logger.error("session_open_topic failed for %s: %s", session_id, exc)
        return ToolResult.text(f"Could not open a topic: {exc}", is_error=True)
    if result.channel_key is None:
        return ToolResult.text(
            f"No topic for session {session_id}: {result.error}", is_error=True,
        )
    what = (
        "Opened a new topic" if result.opened
        else "The session has a topic already; posted its card there"
    )
    return ToolResult.text(
        f"{what}: {result.channel_key} (session {session_id}).",
        structured={"channel_key": result.channel_key, "opened": result.opened},
    )


async def session_close_topic_handler(ctx: ToolContext, args: dict) -> ToolResult:
    channel = _telegram(ctx)
    if channel is None:
        return ToolResult.text("Telegram is not running.", is_error=True)
    session_id = _session_of(ctx, args)
    try:
        topics, error = await channel.close_session_topic(
            session_id, note=str(args.get("note") or "") or None,
        )
    except Exception as exc:
        logger.error("session_close_topic failed for %s: %s", session_id, exc)
        return ToolResult.text(f"Could not close the topic: {exc}", is_error=True)
    lines = [f"- {t.channel_key}: {_CLOSE_OUTCOMES.get(t.outcome, t.outcome)}" for t in topics]
    structured = {
        "closed": not error,
        "topics": [{"channel_key": t.channel_key, "outcome": t.outcome} for t in topics],
    }
    if error:
        text = "\n".join([f"Failed: {error}", *lines])
        return ToolResult.text(text, is_error=True, structured=structured)
    if not topics:
        return ToolResult.text(
            f"Session {session_id} has no Telegram topic; nothing to close.",
            structured=structured,
        )
    return ToolResult.text(
        "\n".join([f"Session {session_id} keeps running; its topics:", *lines]),
        structured=structured,
    )


SESSION_TOPIC_SPECS = [
    ToolSpec(
        name="session_open_topic",
        description=(
            "Show a session in a Telegram topic of its own, opening one if it "
            "has none. The topic starts with the session's catch-up card, and "
            "messages there go to the session. Idempotent: a session that has "
            "a topic already gets its card posted there again."
        ),
        input_schema=SESSION_OPEN_TOPIC_SCHEMA,
        handler=session_open_topic_handler,
    ),
    ToolSpec(
        name="session_close_topic",
        description=(
            "Close a session's Telegram topic: post an optional last note, "
            "close the topic, and unbind it from the session. The session is "
            "not stopped or archived. In a private chat Telegram cannot close "
            "topics, so the topic is only unbound there."
        ),
        input_schema=SESSION_CLOSE_TOPIC_SCHEMA,
        handler=session_close_topic_handler,
    ),
]
