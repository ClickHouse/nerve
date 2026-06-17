"""Notification tool handlers — notify, ask_user, propose_action, react, send_sticker, send_file.

All six tools need ``ctx.session_id`` so the channel router can deliver
to the correct chat (web, Telegram). The session_id arrives via
:class:`ToolContext`; there's no per-tool special-casing left.

``propose_action`` files an ``approval``-kind notification whose answer
routes through a server-side dispatcher (``ctx.notification_service``)
instead of being injected back into the originating session.

``send_file`` enforces workspace containment via :py:meth:`Path.relative_to`
(path-aware) — a string prefix check would let sibling-prefix paths
slip past, e.g. workspace ``/srv/ws`` would accept ``/srv/ws-evil/secret.txt``.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    ASK_USER_SCHEMA,
    NOTIFICATION_SILENCE_SCHEMA,
    NOTIFY_SCHEMA,
    PROPOSE_ACTION_SCHEMA,
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
    force = bool(args.get("force", False))

    try:
        notification_id = await ctx.notification_service.send_notification(
            session_id=ctx.session_id,
            title=title,
            body=body,
            priority=priority,
            force=force,
        )
    except Exception as e:
        logger.error("notify tool failed: %s", e)
        return ToolResult.text(f"Failed to send notification: {e}")

    # Read the row back to learn the delivery outcome (normal / silenced /
    # force-delivered over a match). One indexed PK lookup on the
    # low-volume notify path; this keeps send_notification's ``-> str``
    # contract so cron and other callers stay untouched.
    status, meta = await _read_notify_outcome(
        ctx.notification_service, notification_id,
    )

    if status == "silenced":
        sil_id = meta.get("silenced_by", "?")
        reason = meta.get("silence_reason") or "(no reason recorded)"
        pattern = meta.get("silence_pattern", "")
        hits = meta.get("silence_hit_count")
        hits_str = f"   ({hits} hits)" if hits else ""
        return ToolResult.text(
            f"⚠ Notification {notification_id} was SILENCED and NOT delivered "
            f"(matched {sil_id}).\n"
            f"  reason:  {reason}\n"
            f"  pattern: {pattern}{hits_str}\n"
            "If you believe this match is INCORRECT and the alert genuinely "
            "needs to reach the user, re-send the same notification with "
            "force=true to bypass the silence."
        )

    if meta.get("force_sent_over_silence"):
        sil_id = meta["force_sent_over_silence"]
        count = meta.get("force_override_count")
        count_str = f"; override #{count} recorded" if count else ""
        return ToolResult.text(
            f"Notification sent: {notification_id} "
            f"(force-delivered over silence {sil_id}{count_str})."
        )

    return ToolResult.text(f"Notification sent: {notification_id}")


async def _read_notify_outcome(service, notification_id: str) -> tuple[str | None, dict]:
    """Fetch a notify row's status + parsed metadata for the result string.

    Defensive: any read/parse failure degrades to ``(None, {})`` so the
    handler still reports a plain "sent" rather than erroring.
    """
    try:
        row = await service.db.get_notification(notification_id)
    except Exception:
        return None, {}
    if not row:
        return None, {}
    raw = row.get("metadata")
    meta: dict = {}
    if isinstance(raw, dict):
        meta = raw
    elif isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                meta = parsed
        except (json.JSONDecodeError, ValueError):
            meta = {}
    return row.get("status"), meta


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


def _parse_action_options(raw) -> list[dict[str, str]] | None:
    """Parse the ``options`` arg for propose_action.

    Accepts:
    - a list of ``{"label": ..., "value": ...}`` dicts (passed through)
    - a list of strings (interpreted as ``label == value``)
    - a JSON-encoded string of either of the above
    - falsy / empty -> None (caller falls back to dispatcher defaults)
    """
    if raw is None or raw == "":
        return None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return None
    if not isinstance(raw, list) or not raw:
        return None
    out: list[dict[str, str]] = []
    for item in raw:
        if isinstance(item, dict):
            value = str(item.get("value", "")).strip()
            label = str(item.get("label", value)).strip()
            if not value:
                continue
            out.append({"label": label or value, "value": value})
        elif isinstance(item, str):
            v = item.strip()
            if v:
                out.append({"label": v, "value": v})
    return out or None


async def propose_action_handler(ctx: ToolContext, args: dict) -> ToolResult:
    """Ask the user to approve/decline/snooze a queued action.

    Unlike ask_user, the answer routes through a server-side dispatcher
    keyed by ``target_kind`` and acts on ``target_id`` directly — it is
    NOT injected back into this session.
    """
    if not ctx.notification_service:
        return ToolResult.text("Notification service not available.")

    target_kind = str(args.get("target_kind", "")).strip()
    target_id = str(args.get("target_id", "")).strip()
    title = str(args.get("title", "")).strip()
    if not target_kind or not target_id or not title:
        return ToolResult.text(
            "propose_action: target_kind, target_id, and title are required."
        )

    body = args.get("body", "")
    options = _parse_action_options(args.get("options"))
    priority = args.get("priority", "high")
    expires_at = args.get("expires_at") or None

    try:
        result = await ctx.notification_service.propose_action(
            session_id=ctx.session_id,
            target_kind=target_kind,
            target_id=target_id,
            title=title,
            body=body,
            options=options,
            priority=priority,
            expires_at=expires_at,
        )

        nid = result["notification_id"]
        return ToolResult.text(
            f"Approval requested ({nid}). When the user picks a button, "
            f"the {target_kind} dispatcher acts on {target_id}; the answer "
            f"is NOT injected back into this session."
        )
    except Exception as e:
        logger.error("propose_action tool failed: %s", e)
        return ToolResult.text(f"Failed to propose action: {e}")


async def notification_silence_handler(ctx: ToolContext, args: dict) -> ToolResult:
    """Manage deterministic notification silence rules (add / list / remove).

    Silences suppress known-benign ``notify`` classes at the service
    chokepoint: a match is persisted but not delivered, and the sending
    agent is told so it can force-send a wrong match. Create rules only
    on explicit user instruction or a recorded MEMORY ruling.
    """
    if not ctx.db:
        return ToolResult.text("Database not available.")

    op = str(args.get("op", "")).strip().lower()
    if op == "add":
        return await _silence_add(ctx, args)
    if op == "list":
        return await _silence_list(ctx)
    if op == "remove":
        return await _silence_remove(ctx, args)
    return ToolResult.text(
        "notification_silence: 'op' must be one of add, list, remove."
    )


async def _silence_add(ctx: ToolContext, args: dict) -> ToolResult:
    pattern = str(args.get("pattern", "")).strip()
    if not pattern:
        return ToolResult.text("notification_silence add: 'pattern' is required.")
    try:
        compiled = re.compile(pattern, re.IGNORECASE)
    except re.error as exc:
        return ToolResult.text(
            f"notification_silence add: invalid regex {pattern!r}: {exc}"
        )

    reason = str(args.get("reason", "")).strip()
    try:
        ttl_hours = float(args.get("ttl_hours", 0) or 0)
    except (TypeError, ValueError):
        ttl_hours = 0.0
    expires_at = None
    if ttl_hours > 0:
        expires_at = (
            datetime.now(timezone.utc) + timedelta(hours=ttl_hours)
        ).isoformat()

    silence_id = f"sil-{uuid.uuid4().hex[:8]}"
    await ctx.db.create_silence(
        silence_id=silence_id,
        pattern=pattern,
        reason=reason,
        created_by=ctx.session_id,
        expires_at=expires_at,
    )
    # Take effect immediately for the running service.
    if ctx.notification_service:
        ctx.notification_service.invalidate_silence_cache()

    example = str(args.get("example", "")).strip()
    example_line = ""
    if example:
        verdict = "WOULD match" if compiled.search(example) else "would NOT match"
        example_line = f"\nExample {example!r} {verdict} this pattern."

    ttl_str = "permanent" if not expires_at else f"expires in {ttl_hours:g}h"
    reason_str = f" — {reason}" if reason else ""
    return ToolResult.text(
        f"Silence created: {silence_id}  pattern={pattern}  ({ttl_str}){reason_str}\n"
        "Future 'notify' calls whose title+body match this pattern will be "
        "suppressed (persisted as silenced, not delivered)." + example_line
    )


async def _silence_list(ctx: ToolContext) -> ToolResult:
    rows = await ctx.db.list_silences()
    if not rows:
        return ToolResult.text("No active notification silences.")
    lines: list[str] = []
    for r in rows:
        expires_at = r.get("expires_at")
        expiry = (
            "permanent" if not expires_at
            else f"expires {str(expires_at)[:16].replace('T', ' ')}"
        )
        hits = r.get("hit_count") or 0
        overrides = r.get("override_count") or 0
        counters = f"{hits} hits"
        if overrides:
            counters += f", {overrides} override" + ("s" if overrides != 1 else "")
        reason = r.get("reason") or ""
        reason_str = f" — {reason}" if reason else ""
        flag = "  ⚠ over-broad? (force-overridden)" if overrides else ""
        lines.append(
            f"{r['id']}  {r['pattern']}  {r.get('action', 'silence')}  "
            f"{counters}  {expiry}{reason_str}{flag}"
        )
    return ToolResult.text(
        "Active notification silences:\n" + "\n".join(lines)
    )


async def _silence_remove(ctx: ToolContext, args: dict) -> ToolResult:
    silence_id = str(args.get("silence_id", "")).strip()
    if not silence_id:
        return ToolResult.text(
            "notification_silence remove: 'silence_id' is required."
        )
    ok = await ctx.db.delete_silence(silence_id)
    if not ok:
        return ToolResult.text(f"No silence found with id {silence_id}.")
    if ctx.notification_service:
        ctx.notification_service.invalidate_silence_cache()
    return ToolResult.text(f"Silence removed: {silence_id}.")


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

PROPOSE_ACTION_SPEC = ToolSpec(
    name="propose_action",
    description=(
        "Ask the user to approve, decline, or snooze a queued action. "
        "Unlike ask_user, the answer routes through a server-side dispatcher "
        "keyed by target_kind (e.g. 'mechanical-action') and acts on target_id "
        "directly. The answer is NOT injected back into this session. "
        "Use for queued mechanical actions, pending plans, or any binary "
        "decision the user owns and the agent has already prepared."
    ),
    input_schema=PROPOSE_ACTION_SCHEMA,
    handler=propose_action_handler,
)

NOTIFICATION_SILENCE_SPEC = ToolSpec(
    name="notification_silence",
    description=(
        "Manage notification silence rules — deterministic, server-side "
        "suppression of known-benign alert classes (the monitoring-system "
        "'silence' pattern). A matching 'notify' is persisted but NOT "
        "delivered; priority is never changed, and the sending agent is "
        "told it was silenced (with reason + pattern) so it can re-send "
        "with force=true if the match was wrong.\n"
        "ops: 'add' (pattern [required] + reason [+ ttl_hours, example]), "
        "'list' (active rules with hit/override counts — a non-zero "
        "override count flags an over-broad pattern), 'remove' (silence_id).\n"
        "CONTRACT: create a silence ONLY on explicit user instruction or a "
        "recorded MEMORY ruling that an alert class is benign, and always "
        "state the rule you created. Questions and approvals are never "
        "silenced."
    ),
    input_schema=NOTIFICATION_SILENCE_SCHEMA,
    handler=notification_silence_handler,
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
    PROPOSE_ACTION_SPEC,
    NOTIFICATION_SILENCE_SPEC,
    REACT_SPEC,
    SEND_STICKER_SPEC,
    SEND_FILE_SPEC,
]
