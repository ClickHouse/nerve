"""Notification service — centralized fanout, answer routing, and persistence.

Coordinates between MCP tools (agent-side), channels (delivery), and the
answer routing mechanism (user-side). Supports fire-and-forget notifications,
async questions with multi-channel delivery (web UI + Telegram), and
``approval``-kind notifications that route to a server-side dispatcher when
the user picks an inline option (see ``nerve.notifications.handlers``).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from nerve.notifications import handlers as _handlers

if TYPE_CHECKING:
    from nerve.agent.engine import AgentEngine
    from nerve.config import NerveConfig
    from nerve.db import Database

logger = logging.getLogger(__name__)


# Emoji decoration applied to the canonical approval decisions when we
# render their buttons. Keys are the option ``value`` strings; missing
# values fall back to the raw label.
_APPROVAL_EMOJIS: dict[str, str] = {
    "approve": "✅",       # white heavy check mark
    "decline": "❌",       # cross mark
    "snooze_24h": "\U0001F4A4",  # zzz
}


def _resolve_workspace(config: NerveConfig | None) -> Path | None:
    """Resolve the workspace directory.

    Mirrors ``handlers._resolve_workspace`` so the service can locate
    ``scripts/_mechanical_action.py`` for audit-log writes. Priority:
    ``$NERVE_WORKSPACE_PATH`` first (test override), then
    ``config.workspace``.
    """
    override = os.environ.get("NERVE_WORKSPACE_PATH")
    if override:
        return Path(override).expanduser()
    if config is not None and getattr(config, "workspace", None):
        return Path(config.workspace).expanduser()
    return None


def _load_mechanical_action_helper(workspace: Path):
    """Import the workspace's ``_mechanical_action`` helper by path.

    The helper is a workspace-side stdlib module, not part of the Nerve
    package, so we load it via ``importlib.util`` the same way the
    workspace scripts do. Cached on first load via a module-level dict
    so repeated approval answers do not re-spec the file each time.
    """
    cached = _HELPER_CACHE.get(str(workspace))
    if cached is not None:
        return cached

    import importlib.util

    helper_path = workspace / "scripts" / "_mechanical_action.py"
    if not helper_path.is_file():
        raise FileNotFoundError(
            f"mechanical-action helper not found at {helper_path}"
        )
    spec = importlib.util.spec_from_file_location(
        f"_mechanical_action__{abs(hash(str(workspace))) & 0xFFFFFF:06x}",
        helper_path,
    )
    if spec is None or spec.loader is None:
        raise ImportError(
            f"cannot build module spec for {helper_path}"
        )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _HELPER_CACHE[str(workspace)] = module
    return module


_HELPER_CACHE: dict[str, Any] = {}


class NotificationService:
    """Manages notification lifecycle: create, deliver, answer, route."""

    def __init__(self, config: NerveConfig, db: Database, engine: AgentEngine):
        self.config = config
        self.db = db
        self.engine = engine
        self._hide_session_label: set[str] = set()  # Session ID prefixes that suppress the label

    def hide_session_label_for(self, session_prefix: str) -> None:
        """Register a session ID (or prefix) that should not show the session label."""
        self._hide_session_label.add(session_prefix)

    def _should_show_session_label(self, session_id: str) -> bool:
        """Check whether the session label should be appended to this notification."""
        for prefix in self._hide_session_label:
            if session_id == prefix or session_id.startswith(prefix + ":"):
                return False
        return True

    # ------------------------------------------------------------------ #
    #  Core API (called by MCP tools)                                      #
    # ------------------------------------------------------------------ #

    async def send_notification(
        self,
        session_id: str,
        title: str,
        body: str = "",
        priority: str = "normal",
        channels: list[str] | None = None,
        silent: bool = False,
    ) -> str:
        """Fire-and-forget notification. Returns notification_id.

        Args:
            channels: Override default notification channels (e.g. ["telegram"]).
            silent: If True, deliver without sound (Telegram disable_notification).
        """
        notification_id = f"notif-{uuid.uuid4().hex[:8]}"

        await self.db.create_notification(
            notification_id=notification_id,
            session_id=session_id,
            type="notify",
            title=title,
            body=body,
            priority=priority,
        )

        await self._fanout(
            notification_id, session_id, "notify", title, body, priority,
            channels=channels, silent=silent,
        )

        return notification_id

    async def ask_question(
        self,
        session_id: str,
        title: str,
        body: str = "",
        options: list[str] | None = None,
        priority: str = "normal",
        expiry_hours: int | None = None,
    ) -> dict:
        """Pose a question to the user (always async).

        Returns immediately with notification_id. When the user answers,
        the answer is injected as a user message into the originating session.
        """
        notification_id = f"ask-{uuid.uuid4().hex[:8]}"
        hours = expiry_hours or self.config.notifications.default_expiry_hours
        expires_at = (
            datetime.now(timezone.utc) + timedelta(hours=hours)
        ).isoformat()

        await self.db.create_notification(
            notification_id=notification_id,
            session_id=session_id,
            type="question",
            title=title,
            body=body,
            priority=priority,
            options=options,
            expires_at=expires_at,
        )

        await self._fanout(
            notification_id, session_id, "question", title, body,
            priority, options=options,
        )

        return {"notification_id": notification_id, "status": "sent"}

    async def propose_action(
        self,
        session_id: str,
        target_kind: str,
        target_id: str,
        title: str,
        body: str = "",
        options: list[dict[str, str]] | None = None,
        priority: str = "high",
        expires_at: str | None = None,
        expiry_hours: int | None = None,
    ) -> dict:
        """File an actionable ``approval``-kind notification.

        ``target_kind`` + ``target_id`` route the user's answer through
        ``nerve.notifications.handlers`` instead of the question
        answer-injection path. ``options`` accepts a list of
        ``{"label": ..., "value": ...}`` dicts; when omitted, the
        dispatcher's canonical option set is used (Approve / Decline /
        Snooze 24h for the mechanical-action dispatcher).

        Returns ``{"notification_id": <id>, "status": "sent"}``.
        """
        notification_id = f"approval-{uuid.uuid4().hex[:8]}"

        # Resolve options. Default to the registered dispatcher's
        # canonical set when none was passed. Falling back to the
        # mechanical-action default keeps PR 1's only wired path
        # working without forcing the caller to recite the same triplet.
        if options is None:
            options = _handlers.default_approval_options()
        elif not options:
            raise ValueError("propose_action: options must not be empty")

        # Normalize: store as a list of value strings (matching the
        # existing question-kind contract) plus a parallel label map in
        # metadata so the Telegram + web layers can render the labels
        # without re-parsing options on every send.
        option_values = [str(opt["value"]) for opt in options]
        option_labels = {
            str(opt["value"]): str(opt.get("label", opt["value"]))
            for opt in options
        }

        if expires_at is None:
            hours = (
                expiry_hours
                or self.config.notifications.default_expiry_hours
            )
            expires_at = (
                datetime.now(timezone.utc) + timedelta(hours=hours)
            ).isoformat()

        await self.db.create_notification(
            notification_id=notification_id,
            session_id=session_id,
            type="approval",
            title=title,
            body=body,
            priority=priority,
            options=option_values,
            expires_at=expires_at,
            metadata={
                "target_kind": target_kind,
                "target_id": target_id,
                "option_labels": option_labels,
            },
            target_kind=target_kind,
            target_id=target_id,
        )

        await self._fanout(
            notification_id, session_id, "approval", title, body,
            priority, options=option_values, option_labels=option_labels,
        )

        return {"notification_id": notification_id, "status": "sent"}

    # ------------------------------------------------------------------ #
    #  Answer routing (called by REST API / Telegram callback)             #
    # ------------------------------------------------------------------ #

    async def handle_answer(
        self,
        notification_id: str,
        answer: str,
        answered_by: str,
    ) -> bool:
        """Process a user's answer to a question or approval.

        - For ``type=approval`` rows: look up the dispatcher in the
          handler registry, run it, audit-log the outcome, then flip
          the row's status. Snooze answers advance ``expires_at`` and
          keep the row pending so a later re-delivery tick can surface
          it again.
        - For ``type=question`` rows (legacy): persist the answer,
          inject it back into the originating session, broadcast.
        - Fire-and-forget ``type=notify`` rows do not flow through this
          method; the dismiss endpoint handles those.
        """
        notif = await self.db.get_notification(notification_id)
        if not notif or notif["status"] != "pending":
            return False

        if notif.get("type") == "approval":
            return await self._handle_approval_answer(
                notif, answer, answered_by,
            )

        success = await self.db.answer_notification(
            notification_id, answer, answered_by,
        )
        if not success:
            return False

        session_id = notif["session_id"]

        from nerve.agent.streaming import broadcaster

        # Inject answer as user message into the session
        injected_message = f"[Answer to: {notif['title']}]\n\n{answer}"

        # Broadcast the injected user message to the chat UI
        await broadcaster.broadcast(session_id, {
            "type": "answer_injected",
            "session_id": session_id,
            "notification_id": notification_id,
            "title": notif["title"],
            "answer": answer,
            "answered_by": answered_by,
            "content": injected_message,
        })

        try:
            if not self.engine.sessions.is_running(session_id):
                task = asyncio.create_task(
                    self.engine.run(
                        session_id=session_id,
                        user_message=injected_message,
                        source=f"notification:{answered_by}",
                        channel=answered_by,
                    )
                )
                task.add_done_callback(self._on_answer_task_done)
            else:
                logger.info(
                    "Session %s running — answer stored, not injected",
                    session_id,
                )
        except Exception as e:
            logger.error(
                "Failed to inject answer for %s into session %s: %s",
                notification_id, session_id, e,
            )

        # Broadcast answer event to web UI (notifications page)
        await broadcaster.broadcast("__global__", {
            "type": "notification_answered",
            "notification_id": notification_id,
            "session_id": session_id,
            "answer": answer,
            "answered_by": answered_by,
        })

        return True

    async def _handle_approval_answer(
        self,
        notif: dict[str, Any],
        answer: str,
        answered_by: str,
    ) -> bool:
        """Route an approval answer through the dispatcher registry."""
        notification_id = notif["id"]
        session_id = notif["session_id"]
        target_kind = notif.get("target_kind") or ""
        target_id = notif.get("target_id") or ""

        dispatcher = _handlers.get(target_kind) if target_kind else None
        if dispatcher is None:
            logger.warning(
                "approval %s has no dispatcher for target_kind=%r; "
                "marking answered without action",
                notification_id, target_kind,
            )
            result = _handlers.DispatchResult(
                ok=False,
                audit_event={
                    "event": "approval-acted",
                    "notification_id": notification_id,
                    "target_kind": target_kind,
                    "target_id": target_id,
                    "decision": answer,
                    "ok": False,
                    "error": (
                        f"no dispatcher registered for {target_kind!r}"
                    ),
                },
            )
        else:
            try:
                result = await asyncio.to_thread(
                    dispatcher, notif, target_id, answer, self.config,
                )
            except Exception as exc:  # defensive: never crash the route
                logger.exception(
                    "approval dispatch raised for %s (target=%s:%s, "
                    "decision=%s): %s",
                    notification_id, target_kind, target_id, answer, exc,
                )
                result = _handlers.DispatchResult(
                    ok=False,
                    audit_event={
                        "event": "approval-acted",
                        "notification_id": notification_id,
                        "target_kind": target_kind,
                        "target_id": target_id,
                        "decision": answer,
                        "ok": False,
                        "error": f"dispatcher raised: {exc}",
                    },
                )

        await self._append_approval_audit(result.audit_event)

        # Snooze keeps the row pending with a future expiry so a later
        # re-delivery tick (wired in PR 2) can surface it again.
        if result.snooze_until is not None and result.ok:
            await self.db.snooze_notification(
                notification_id, result.snooze_until,
            )
        else:
            await self.db.answer_notification(
                notification_id, answer, answered_by,
            )

        from nerve.agent.streaming import broadcaster
        broadcast_status = (
            "snoozed" if (result.snooze_until and result.ok) else "answered"
        )
        await broadcaster.broadcast("__global__", {
            "type": "notification_answered",
            "notification_id": notification_id,
            "session_id": session_id,
            "answer": answer,
            "answered_by": answered_by,
            "approval_status": broadcast_status,
            "dispatch_ok": result.ok,
        })

        return True

    async def _append_approval_audit(self, event: dict[str, Any]) -> None:
        """Append an ``approval-acted`` record to the mechanical-actions log.

        Uses the same audit log that the propose-mechanical-action
        primitive writes to (``~/.nerve/mechanical-actions/audit.jsonl``)
        so the proposal lifecycle (``proposed`` -> ``approval-acted``
        -> ``approved``/``declined``/``executed``) is visible in one
        place. The shared helper module
        ``scripts/_mechanical_action.py`` lives under the workspace, so
        we import it dynamically by path rather than as a real Python
        package.
        """
        workspace = _resolve_workspace(self.config)
        if workspace is None:
            logger.debug(
                "approval audit: no workspace configured; event=%s",
                event.get("event"),
            )
            return
        try:
            helper = _load_mechanical_action_helper(workspace)
        except Exception as exc:  # defensive: never crash the route
            logger.warning(
                "approval audit: cannot load helper at %s: %s",
                workspace, exc,
            )
            return

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        # Re-use the helper's append path. Use a tag the helper hasn't
        # whitelisted yet by patching VALID_EVENTS just for our event,
        # so the helper validation stays strict for everything else.
        record = {"ts": ts, **event}
        valid = getattr(helper, "VALID_EVENTS", None)
        if isinstance(valid, set) and "approval-acted" not in valid:
            valid.add("approval-acted")

        try:
            await asyncio.to_thread(helper.append_audit, record, None)
        except Exception as exc:
            logger.warning(
                "approval audit append failed: %s (event=%s)",
                exc, event.get("event"),
            )

    def _on_answer_task_done(self, task: asyncio.Task) -> None:
        """Log errors from answer injection tasks.

        Attached as a done_callback so exceptions from fire-and-forget
        asyncio.create_task() calls are surfaced instead of silently lost.
        """
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error("Answer injection task failed: %s", exc)

    async def handle_dismiss(self, notification_id: str) -> bool:
        """Dismiss a notification (no answer routing needed)."""
        notif = await self.db.get_notification(notification_id)
        if not notif or notif["status"] != "pending":
            return False

        await self.db.dismiss_notification(notification_id)
        return True

    # ------------------------------------------------------------------ #
    #  Fanout to channels                                                  #
    # ------------------------------------------------------------------ #

    async def _fanout(
        self,
        notification_id: str,
        session_id: str,
        notif_type: str,
        title: str,
        body: str,
        priority: str,
        options: list[str] | None = None,
        channels: list[str] | None = None,
        silent: bool = False,
        option_labels: dict[str, str] | None = None,
    ) -> None:
        """Deliver notification to all configured channels in parallel.

        ``option_labels`` is used by ``approval``-kind notifications: it
        maps the canonical option ``value`` (sent back on the callback
        as the answer string) to the human-facing label rendered on the
        button. ``None`` for the legacy ``question`` path, where the
        label and the value are identical.
        """
        target_channels = channels or self.config.notifications.channels

        async def _deliver(channel_name: str) -> str | None:
            """Deliver to a single channel, return name on success."""
            try:
                if channel_name == "web":
                    await self._deliver_web(
                        notification_id, session_id, notif_type,
                        title, body, priority, options,
                        option_labels=option_labels,
                    )
                    return "web"
                elif channel_name == "telegram":
                    msg_id = await self._deliver_telegram(
                        notification_id, session_id, notif_type,
                        title, body, priority, options,
                        silent=silent, option_labels=option_labels,
                    )
                    if msg_id:
                        await self.db.update_notification(
                            notification_id,
                            telegram_message_id=str(msg_id),
                        )
                    return "telegram"
            except Exception as e:
                logger.error(
                    "Failed to deliver %s to %s: %s",
                    notification_id, channel_name, e,
                )
            return None

        results = await asyncio.gather(
            *(_deliver(ch) for ch in target_channels),
            return_exceptions=True,
        )
        channels_delivered = [r for r in results if isinstance(r, str)]

        await self.db.update_notification(
            notification_id,
            channels_delivered=json.dumps(channels_delivered),
        )

    async def _deliver_web(
        self,
        notification_id: str,
        session_id: str,
        notif_type: str,
        title: str,
        body: str,
        priority: str,
        options: list[str] | None,
        option_labels: dict[str, str] | None = None,
    ) -> None:
        """Broadcast notification to web UI via the global broadcaster.

        For approval-kind rows we also include ``option_labels`` so the
        web NotificationCard can render readable button text while the
        button click still sends the canonical ``value`` back through
        the answer endpoint.
        """
        from nerve.agent.streaming import broadcaster
        message = {
            "type": "notification",
            "notification_id": notification_id,
            "notification_type": notif_type,
            "session_id": session_id,
            "title": title,
            "body": body,
            "priority": priority,
            "options": options,
        }
        if option_labels:
            message["option_labels"] = option_labels
        await broadcaster.broadcast("__global__", message)

    def _resolve_telegram_chat_id(self) -> int | None:
        """Resolve the Telegram chat ID for notification delivery."""
        chat_id = self.config.notifications.telegram_chat_id
        if chat_id:
            return chat_id
        allowed = self.config.telegram.allowed_users
        if allowed:
            return allowed[0]
        logger.warning("No telegram_chat_id configured for notifications")
        return None

    def _get_telegram_bot(self):
        """Get the Telegram bot instance, or None if unavailable."""
        channel = self.engine.router.get_channel("telegram")
        if not channel or not hasattr(channel, '_app') or channel._app is None:
            return None
        return channel._app.bot

    async def _deliver_telegram(
        self,
        notification_id: str,
        session_id: str,
        notif_type: str,
        title: str,
        body: str,
        priority: str,
        options: list[str] | None,
        silent: bool = False,
        option_labels: dict[str, str] | None = None,
    ) -> str | None:
        """Send notification to Telegram, with inline keyboard for questions/approvals."""
        bot = self._get_telegram_bot()
        if not bot:
            logger.warning("Telegram bot not available for notification %s", notification_id)
            return None

        chat_id = self._resolve_telegram_chat_id()
        if not chat_id:
            return None

        # Build message text
        priority_prefix = self.config.notifications.priority_prefixes.get(priority, "")
        if title:
            text = f"{priority_prefix}{title}"
            if body:
                text += f"\n\n{body}"
        else:
            text = body or ""
        if self._should_show_session_label(session_id):
            text += f"\n\nSession: {session_id}"

        if notif_type in ("question", "approval") and options:
            button_labels: list[tuple[str, str]] = []
            for value in options:
                if notif_type == "approval":
                    label = (
                        (option_labels or {}).get(value)
                        or value.replace("_", " ").title()
                    )
                    emoji = _APPROVAL_EMOJIS.get(value, "")
                    rendered = f"{emoji} {label}".strip() if emoji else label
                else:
                    rendered = value
                button_labels.append((rendered, value))
            return await self._send_telegram_inline(
                chat_id, notification_id, text, button_labels, silent=silent,
            )
        else:
            msg = await self._send_telegram_html(bot, chat_id, text, silent=silent)
            return str(msg.message_id)

    @staticmethod
    async def _send_telegram_html(
        bot: object,
        chat_id: int,
        text: str,
        *,
        reply_markup: object | None = None,
        silent: bool = False,
    ) -> object:
        """Send a message with markdown→HTML conversion and plain-text fallback."""
        from nerve.channels.telegram import _md_to_tg_html
        from telegram.constants import ParseMode

        html_text = _md_to_tg_html(text)
        try:
            return await bot.send_message(
                chat_id=chat_id, text=html_text,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
                disable_notification=silent,
            )
        except Exception:
            return await bot.send_message(
                chat_id=chat_id, text=text,
                reply_markup=reply_markup,
                disable_notification=silent,
            )

    async def _send_telegram_inline(
        self,
        chat_id: int,
        notification_id: str,
        text: str,
        options: list[str] | list[tuple[str, str]],
        silent: bool = False,
    ) -> str | None:
        """Send Telegram message with inline keyboard buttons.

        ``options`` accepts either a flat list of strings (legacy
        question kind: label == callback value) or a list of
        ``(label, value)`` tuples (approval kind: emoji-prefixed label,
        canonical value sent back on the callback).
        """
        bot = self._get_telegram_bot()
        if not bot:
            return None

        from telegram import InlineKeyboardButton, InlineKeyboardMarkup

        buttons = []
        for entry in options:
            if isinstance(entry, tuple):
                label, value = entry
            else:
                label = value = entry
            callback_data = f"notif:{notification_id}:{value}"
            # Telegram callback_data max 64 bytes — truncate option if needed
            if len(callback_data.encode("utf-8")) > 64:
                max_opt_len = 64 - len(f"notif:{notification_id}:".encode("utf-8"))
                truncated = value.encode("utf-8")[:max_opt_len].decode("utf-8", errors="ignore")
                callback_data = f"notif:{notification_id}:{truncated}"
            buttons.append([InlineKeyboardButton(label, callback_data=callback_data)])

        keyboard = InlineKeyboardMarkup(buttons)

        msg = await self._send_telegram_html(
            bot, chat_id, text, reply_markup=keyboard, silent=silent,
        )

        await self.db.update_notification(
            notification_id, telegram_chat_id=str(chat_id),
        )

        return str(msg.message_id)

    # ------------------------------------------------------------------ #
    #  Expiry (called by periodic background task)                         #
    # ------------------------------------------------------------------ #

    async def expire_stale(self) -> int:
        """Expire pending notifications past their expiry time."""
        return await self.db.expire_notifications()
