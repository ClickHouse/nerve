"""Notification service — centralized fanout, answer routing, and persistence.

Coordinates between MCP tools (agent-side), channels (delivery), and the
answer routing mechanism (user-side). Supports fire-and-forget notifications
and async questions with multi-channel delivery (web UI + Telegram).
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nerve.agent.engine import AgentEngine
    from nerve.config import NerveConfig
    from nerve.db import Database

logger = logging.getLogger(__name__)


class NotificationService:
    """Manages notification lifecycle: create, deliver, answer, route."""

    def __init__(self, config: NerveConfig, db: Database, engine: AgentEngine):
        self.config = config
        self.db = db
        self.engine = engine

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

    # ------------------------------------------------------------------ #
    #  Answer routing (called by REST API / Telegram callback)             #
    # ------------------------------------------------------------------ #

    async def handle_answer(
        self,
        notification_id: str,
        answer: str,
        answered_by: str,
    ) -> bool:
        """Process a user's answer to a question.

        1. Persist the answer in DB.
        2. Inject answer as user message in the session.
        3. Broadcast answer event to web UI.
        """
        notif = await self.db.get_notification(notification_id)
        if not notif or notif["status"] != "pending":
            return False

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
    ) -> None:
        """Deliver notification to all configured channels in parallel."""
        target_channels = channels or self.config.notifications.channels

        async def _deliver(channel_name: str) -> str | None:
            """Deliver to a single channel, return name on success."""
            try:
                if channel_name == "web":
                    await self._deliver_web(
                        notification_id, session_id, notif_type,
                        title, body, priority, options,
                    )
                    return "web"
                elif channel_name == "telegram":
                    msg_id = await self._deliver_telegram(
                        notification_id, session_id, notif_type,
                        title, body, priority, options,
                        silent=silent,
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
    ) -> None:
        """Broadcast notification to web UI via the global broadcaster."""
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
    ) -> str | None:
        """Send notification to Telegram, with inline keyboard for questions."""
        bot = self._get_telegram_bot()
        if not bot:
            logger.warning("Telegram bot not available for notification %s", notification_id)
            return None

        chat_id = self._resolve_telegram_chat_id()
        if not chat_id:
            return None

        # Build message text (plain text — avoids Markdown parse failures)
        priority_prefix = {"high": "\u26a0\ufe0f ", "urgent": "\U0001f6a8 "}.get(priority, "")
        text = f"{priority_prefix}{title}"
        if body:
            text += f"\n\n{body}"
        text += f"\n\nSession: {session_id}"

        if notif_type == "question" and options:
            return await self._send_telegram_inline(
                chat_id, notification_id, text, options, silent=silent,
            )
        else:
            msg = await bot.send_message(
                chat_id=chat_id, text=text,
                disable_notification=silent,
            )
            return str(msg.message_id)

    async def _send_telegram_inline(
        self,
        chat_id: int,
        notification_id: str,
        text: str,
        options: list[str],
        silent: bool = False,
    ) -> str | None:
        """Send Telegram message with inline keyboard buttons."""
        bot = self._get_telegram_bot()
        if not bot:
            return None

        from telegram import InlineKeyboardButton, InlineKeyboardMarkup

        buttons = []
        for option in options:
            callback_data = f"notif:{notification_id}:{option}"
            # Telegram callback_data max 64 bytes — truncate option if needed
            if len(callback_data.encode("utf-8")) > 64:
                max_opt_len = 64 - len(f"notif:{notification_id}:".encode("utf-8"))
                truncated = option.encode("utf-8")[:max_opt_len].decode("utf-8", errors="ignore")
                callback_data = f"notif:{notification_id}:{truncated}"
            buttons.append([InlineKeyboardButton(option, callback_data=callback_data)])

        keyboard = InlineKeyboardMarkup(buttons)

        msg = await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=keyboard,
            disable_notification=silent,
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
