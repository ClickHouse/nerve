"""Notification tool handlers — notify, ask_user, react, send_sticker, send_file.

All five tools need ``ctx.session_id`` so the channel router can deliver
to the correct chat (web, Telegram). The session_id arrives via
:class:`ToolContext`; there's no per-tool special-casing left.

``send_file`` enforces workspace containment via :py:meth:`Path.relative_to`
(path-aware) — a string prefix check would let sibling-prefix paths
slip past, e.g. workspace ``/srv/ws`` would accept ``/srv/ws-evil/secret.txt``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    ASK_USER_SCHEMA,
    NOTIFY_SCHEMA,
    REACT_SCHEMA,
    SEND_FILE_SCHEMA,
    SEND_STICKER_SCHEMA,
)

logger = logging.getLogger(__name__)


async def notify_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if not ctx.notification_service:
        return ToolResult.text("Notification service not available.")

    title = args.get("title", "")
    body = args.get("body", "")
    priority = args.get("priority", "normal")

    try:
        notification_id = await ctx.notification_service.send_notification(
            session_id=ctx.session_id,
            title=title,
            body=body,
            priority=priority,
        )
        return ToolResult.text(f"Notification sent: {notification_id}")
    except Exception as e:
        logger.error("notify tool failed: %s", e)
        return ToolResult.text(f"Failed to send notification: {e}")


async def ask_user_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if not ctx.notification_service:
        return ToolResult.text("Notification service not available.")

    title = args["title"]
    body = args.get("body", "")
    options_raw = args.get("options", "")
    # Parse options: accept JSON array, comma-separated string, or already-parsed list
    options: list[str] = []
    if isinstance(options_raw, list):
        options = [str(o).strip() for o in options_raw if str(o).strip()]
    elif isinstance(options_raw, str) and options_raw.strip():
        # Try JSON array first, fall back to comma-separated
        try:
            parsed = json.loads(options_raw)
            if isinstance(parsed, list):
                options = [str(o).strip() for o in parsed if str(o).strip()]
            else:
                options = [o.strip() for o in options_raw.split(",") if o.strip()]
        except (json.JSONDecodeError, ValueError):
            options = [o.strip() for o in options_raw.split(",") if o.strip()]
    priority = args.get("priority", "normal")

    try:
        result = await ctx.notification_service.ask_question(
            session_id=ctx.session_id,
            title=title,
            body=body,
            options=options if options else None,
            priority=priority,
        )

        nid = result["notification_id"]
        return ToolResult.text(
            f"Question sent ({nid}). The user's answer will be automatically "
            f"injected as a message in this session."
        )
    except Exception as e:
        logger.error("ask_user tool failed: %s", e)
        return ToolResult.text(f"Failed to ask question: {e}")


async def react_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if not ctx.engine:
        return ToolResult.text("Engine not available.")

    emoji = args["emoji"]

    try:
        success = await ctx.engine.router.set_reaction(ctx.session_id, emoji)
        if success:
            return ToolResult.text(f"Reaction set: {emoji}")
        return ToolResult.text(
            "Cannot set reaction: no message context or channel does not support reactions."
        )
    except Exception as e:
        logger.error("react tool failed: %s", e)
        return ToolResult.text(f"Failed to set reaction: {e}")


async def send_sticker_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if not ctx.engine:
        return ToolResult.text("Engine not available.")

    sticker = args["sticker"]

    try:
        success = await ctx.engine.router.send_sticker(ctx.session_id, sticker)
        if success:
            return ToolResult.text("Sticker sent.")
        return ToolResult.text(
            "Cannot send sticker: no message context or channel does not support stickers."
        )
    except Exception as e:
        logger.error("send_sticker tool failed: %s", e)
        return ToolResult.text(f"Failed to send sticker: {e}")


async def send_file_handler(ctx: ToolContext, args: dict) -> ToolResult:
    """Deliver a file via the channel router.

    Telegram → ``send_document``; web panel renders the persisted tool_call
    block as a download card. Falls back to a web-panel message when the
    bound channel cannot deliver files natively.

    Workspace containment is enforced via :py:meth:`Path.relative_to` —
    a string-prefix check would let ``/srv/ws-evil/secret.txt`` slip past
    a workspace of ``/srv/ws``.
    """
    file_path = args.get("file_path", "")
    if not file_path:
        return ToolResult.text("Error: file_path is required.")

    resolved = Path(file_path).resolve()
    if not resolved.is_file():
        return ToolResult.text(f"Error: file not found: {file_path}")

    if ctx.workspace:
        try:
            resolved.relative_to(ctx.workspace.resolve())
        except ValueError:
            return ToolResult.text("Error: file must be within the workspace.")

    filename = resolved.name
    file_size = resolved.stat().st_size

    delivered = False
    if ctx.engine is not None:
        try:
            active_channel = ctx.engine.get_active_channel(ctx.session_id)
            delivered = await ctx.engine.router.send_file(
                ctx.session_id, str(resolved), channel=active_channel,
            )
        except Exception as e:
            logger.error("send_file dispatch failed: %s", e)
            delivered = False

    if delivered:
        return ToolResult.text(f"Sent file: {filename} ({file_size:,} bytes)")

    return ToolResult.text(
        f"File ready: {filename} ({file_size:,} bytes). "
        "Native delivery not available on this channel — open the web panel to download."
    )


NOTIFY_SPEC = ToolSpec(
    name="notify",
    description=(
        "Send an async notification to the user. Fire-and-forget — does not wait for a response. "
        "Use for status updates, completion alerts, reminders, or any message that doesn't need a reply."
    ),
    input_schema=NOTIFY_SCHEMA,
    handler=notify_handler,
)

ASK_USER_SPEC = ToolSpec(
    name="ask_user",
    description=(
        "Ask the user a question via async notification. "
        "Returns immediately — when the user answers, their reply is "
        "automatically injected into this session. "
        "Use predefined options for quick answers (rendered as buttons), or the user can type a free-text reply."
    ),
    input_schema=ASK_USER_SCHEMA,
    handler=ask_user_handler,
)

REACT_SPEC = ToolSpec(
    name="react",
    description=(
        "Set an emoji reaction on the user's last message. "
        "Use to acknowledge messages, express emotions, or respond non-verbally. "
        "Works on channels that support reactions (e.g., Telegram)."
    ),
    input_schema=REACT_SCHEMA,
    handler=react_handler,
)

SEND_STICKER_SPEC = ToolSpec(
    name="send_sticker",
    description=(
        "Send a Telegram sticker to the current chat. "
        "Use the file_id received when a user sends you a sticker."
    ),
    input_schema=SEND_STICKER_SCHEMA,
    handler=send_sticker_handler,
)

SEND_FILE_SPEC = ToolSpec(
    name="send_file",
    description=(
        "Send a file to the user as a downloadable attachment in the chat. "
        "On Telegram the file is delivered as a document; on the web panel it appears as an inline "
        "download card. Use this when the user asks you to share, export, or send them a file."
    ),
    input_schema=SEND_FILE_SCHEMA,
    handler=send_file_handler,
)


NOTIFICATION_SPECS = [
    NOTIFY_SPEC,
    ASK_USER_SPEC,
    REACT_SPEC,
    SEND_STICKER_SPEC,
    SEND_FILE_SPEC,
]
