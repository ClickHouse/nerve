"""Telegram bot channel — receive messages, run agent, respond.

Uses python-telegram-bot (v21+) for async Telegram bot communication.
Supports partial message streaming (edit-in-place) via the StreamAdapter.
Session management is delegated to ChannelRouter.
"""

from __future__ import annotations

import asyncio
import html as _html
import logging
import re
import socket
import time
from typing import Any, TYPE_CHECKING

from telegram import Update
from telegram.constants import ChatAction, ParseMode
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, MessageHandler, filters

from nerve.channels.base import (
    BaseChannel,
    ChannelCapability,
    ChannelConstraints,
    InboundMessage,
    OutboundMessage,
)
from nerve.config import NerveConfig

if TYPE_CHECKING:
    from nerve.channels.router import ChannelRouter

logger = logging.getLogger(__name__)

# Telegram message length limit
MAX_MSG_LEN = 4096
# Minimum interval between message edits (seconds) to avoid rate limits
EDIT_INTERVAL = 1.5
# Watchdog: check every 30s, log heartbeat every ~5 min
WATCHDOG_INTERVAL = 30
WATCHDOG_HEARTBEAT_EVERY = 10

# TCP keepalive: prevent NAT/firewall from silently dropping the
# long-poll connection.  These values tell the OS to send a keepalive
# probe after 60s idle, retry every 10s, give up after 3 failures.
_TCP_KEEPALIVE_OPTS = (
    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
    (socket.SOL_TCP, socket.TCP_KEEPIDLE, 60),
    (socket.SOL_TCP, socket.TCP_KEEPINTVL, 10),
    (socket.SOL_TCP, socket.TCP_KEEPCNT, 3),
)


def _md_to_tg_html(text: str) -> str:
    """Convert standard Markdown to Telegram-compatible HTML.

    Telegram's legacy ``ParseMode.MARKDOWN`` only supports ``*bold*``,
    but LLMs emit ``**bold**`` (standard Markdown).  This converts the
    common constructs to HTML so we can use ``ParseMode.HTML`` instead,
    which is more predictable and doesn't choke on special characters.

    Handles: ``**bold**``, ``*italic*``, `` `code` ``, code fences,
    and ``[text](url)``.  Unmatched markers pass through as-is.
    """
    protected: list[str] = []

    def _protect(replacement: str) -> str:
        idx = len(protected)
        protected.append(replacement)
        return f"\x00{idx}\x00"

    # -- protect constructs that contain chars we'd otherwise escape --

    # Code fences: ```lang\n...\n```
    def _fence(m: re.Match) -> str:
        return _protect(f"<pre>{_html.escape(m.group(2))}</pre>")
    text = re.sub(r"```(\w*)\n?(.*?)```", _fence, text, flags=re.DOTALL)

    # Inline code: `...`
    def _code(m: re.Match) -> str:
        return _protect(f"<code>{_html.escape(m.group(1))}</code>")
    text = re.sub(r"`([^`]+)`", _code, text)

    # Markdown links: [text](url)
    def _link(m: re.Match) -> str:
        label = _html.escape(m.group(1))
        url = m.group(2)
        return _protect(f'<a href="{url}">{label}</a>')
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", _link, text)

    # -- escape remaining HTML entities --
    text = _html.escape(text, quote=False)

    # -- inline formatting --
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"<i>\1</i>", text)

    # -- restore protected spans --
    for i, repl in enumerate(protected):
        text = text.replace(f"\x00{i}\x00", repl)

    return text


class TelegramChannel(BaseChannel):
    """Telegram bot channel.

    Handles the Telegram bot transport: polling, commands, auth.
    Delegates session management and agent execution to the ChannelRouter.
    """

    def __init__(self, config: NerveConfig, router: ChannelRouter):
        self.config = config
        self.router = router
        self._app: Application | None = None
        self._allowed_users: set[int] = set(config.telegram.allowed_users)
        self._notification_service = None  # Set after service is created
        self._watchdog_task: asyncio.Task | None = None
        self._stopping = False
        self._last_update_time: float = 0.0  # monotonic, set on any incoming update

    def set_notification_service(self, service) -> None:
        """Wire the notification service for callback query handling."""
        self._notification_service = service

    @property
    def name(self) -> str:
        return "telegram"

    @property
    def capabilities(self) -> ChannelCapability:
        caps = (
            ChannelCapability.SEND_TEXT
            | ChannelCapability.MARKDOWN
            | ChannelCapability.TYPING_INDICATOR
        )
        if self.config.telegram.stream_mode == "partial":
            caps |= ChannelCapability.STREAMING
        return caps

    @property
    def constraints(self) -> ChannelConstraints:
        return ChannelConstraints(
            max_message_length=MAX_MSG_LEN,
            min_edit_interval=EDIT_INTERVAL,
            supports_message_edit=True,
        )

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    def _build_application(self) -> Application:
        """Build a PTB Application with robust connection settings."""
        builder = (
            Application.builder()
            .token(self.config.telegram.bot_token)
            # TCP keepalive on the polling connection — prevents NAT/firewall
            # from silently dropping the long-poll connection
            .get_updates_socket_options(_TCP_KEEPALIVE_OPTS)
            # Separate connection pool for polling vs sending, so a stuck
            # outbound request can't starve the polling connection
            .get_updates_connection_pool_size(1)
            .get_updates_read_timeout(10)
            .get_updates_connect_timeout(10)
            .get_updates_pool_timeout(1.0)
        )
        app = builder.build()

        # Register handlers
        app.add_handler(CommandHandler("start", self._handle_start))
        app.add_handler(CommandHandler("session", self._handle_session))
        app.add_handler(CommandHandler("sessions", self._handle_sessions))
        app.add_handler(CommandHandler("new", self._handle_new_session))
        app.add_handler(CommandHandler("reply", self._handle_reply))
        app.add_handler(CallbackQueryHandler(self._handle_callback_query))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_message))
        app.add_error_handler(self._handle_error)

        return app

    async def start(self) -> None:
        """Start the Telegram bot."""
        if not self.config.telegram.bot_token:
            logger.warning("Telegram bot token not configured")
            return

        self._stopping = False
        self._app = self._build_application()

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling(
            drop_pending_updates=True,
            # Retry initial connection indefinitely — don't give up on
            # transient network errors during startup
            bootstrap_retries=-1,
        )
        self._last_update_time = time.monotonic()
        logger.info("Telegram bot started polling (with TCP keepalive)")

        # Launch watchdog for monitoring + recovery
        self._watchdog_task = asyncio.create_task(
            self._run_watchdog(), name="telegram-polling-watchdog",
        )

    async def stop(self) -> None:
        self._stopping = True
        if self._watchdog_task and not self._watchdog_task.done():
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
        if self._app:
            await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()

    # ------------------------------------------------------------------ #
    #  Watchdog — monitor both Updater and Application health              #
    # ------------------------------------------------------------------ #

    async def _run_watchdog(self) -> None:
        """Monitor Telegram bot health and log diagnostics.

        Checks BOTH the Updater (fetches updates from Telegram) AND the
        Application (processes updates from queue → handlers).  Previous
        versions only checked the Updater, missing cases where the
        Application's update-processing task crashed silently.
        """
        check_count = 0
        while not self._stopping:
            try:
                await asyncio.sleep(WATCHDOG_INTERVAL)
            except asyncio.CancelledError:
                break

            if self._app is None or self._stopping:
                break

            check_count += 1
            status = self._get_health_status()

            # Periodic heartbeat
            if check_count % WATCHDOG_HEARTBEAT_EVERY == 0:
                logger.info(
                    "Telegram watchdog: %s (check #%d, last_update=%s, queue=%d)",
                    status["summary"], check_count,
                    status["last_update_ago"], status["queue_size"],
                )

            if not status["healthy"]:
                logger.warning(
                    "Telegram bot unhealthy: %s (queue=%d, last_update=%s) — rebuilding",
                    status["summary"], status["queue_size"],
                    status["last_update_ago"],
                )
                try:
                    await self._rebuild()
                    logger.info("Telegram bot rebuilt successfully")
                except Exception as e:
                    logger.error("Telegram bot rebuild failed: %s", e, exc_info=True)

    def _get_health_status(self) -> dict:
        """Check health of both Updater and Application."""
        now = time.monotonic()
        stale_sec = now - self._last_update_time if self._last_update_time > 0 else 0
        last_ago = f"{stale_sec:.0f}s ago" if self._last_update_time > 0 else "never"

        result = {
            "healthy": True,
            "summary": "ok",
            "last_update_ago": last_ago,
            "queue_size": 0,
        }

        if not self._app:
            result.update(healthy=False, summary="no Application")
            return result

        # --- Check update queue ---
        # If updates pile up in the queue, the Application isn't consuming them
        try:
            result["queue_size"] = self._app.update_queue.qsize()
        except Exception:
            pass

        # --- Check Updater (the part that fetches from Telegram) ---
        updater = self._app.updater
        if not updater or not updater.running:
            result.update(healthy=False, summary="updater not running")
            return result

        polling_task: asyncio.Task | None = getattr(
            updater, "_Updater__polling_task", None,
        )
        if polling_task is not None and polling_task.done():
            exc = None
            try:
                exc = polling_task.exception()
            except (asyncio.CancelledError, asyncio.InvalidStateError):
                pass
            result.update(
                healthy=False,
                summary=f"polling task dead (exception: {exc})",
            )
            return result

        # --- Check Application (the part that processes updates → handlers) ---
        fetcher_task: asyncio.Task | None = getattr(
            self._app, "_Application__update_fetcher_task", None,
        )
        if fetcher_task is not None and fetcher_task.done():
            exc = None
            try:
                exc = fetcher_task.exception()
            except (asyncio.CancelledError, asyncio.InvalidStateError):
                pass
            result.update(
                healthy=False,
                summary=f"update fetcher task dead (exception: {exc})",
            )
            return result

        # --- Check for backed-up queue (Application alive but not consuming) ---
        if result["queue_size"] > 10:
            result.update(
                healthy=False,
                summary=f"update queue backed up ({result['queue_size']} pending)",
            )
            return result

        return result

    async def _rebuild(self) -> None:
        """Tear down and rebuild the entire PTB Application."""
        old_app = self._app
        self._app = self._build_application()

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling(
            drop_pending_updates=False,
            bootstrap_retries=-1,
        )
        self._last_update_time = time.monotonic()

        # Best-effort cleanup of the old app
        if old_app:
            try:
                await asyncio.wait_for(old_app.updater.stop(), timeout=5)
            except Exception:
                pass
            try:
                await asyncio.wait_for(old_app.stop(), timeout=5)
            except Exception:
                pass
            try:
                await asyncio.wait_for(old_app.shutdown(), timeout=5)
            except Exception:
                pass

    # ------------------------------------------------------------------ #
    #  Outbound: send complete message                                     #
    # ------------------------------------------------------------------ #

    async def send(self, message: OutboundMessage) -> None:
        """Send a message to a Telegram chat."""
        if self._app is None:
            return
        chat_id = int(message.target)
        text = message.text
        # Split long messages
        for i in range(0, len(text), MAX_MSG_LEN):
            chunk = text[i:i + MAX_MSG_LEN]
            html_chunk = _md_to_tg_html(chunk)
            try:
                await self._app.bot.send_message(
                    chat_id=chat_id,
                    text=html_chunk,
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                await self._app.bot.send_message(
                    chat_id=chat_id,
                    text=chunk,
                )

    def format_response(self, text: str) -> str:
        """Truncate for Telegram if needed."""
        if len(text) > MAX_MSG_LEN:
            return text[:MAX_MSG_LEN - 20] + "\n\n... (truncated)"
        return text

    # ------------------------------------------------------------------ #
    #  Streaming protocol                                                  #
    # ------------------------------------------------------------------ #

    async def send_placeholder(self, target: str, session_id: str) -> str | None:
        """Send a placeholder message for streaming. Returns message_id."""
        if self._app is None:
            return None
        chat_id = int(target)
        msg = await self._app.bot.send_message(chat_id=chat_id, text="...")
        return str(msg.message_id)

    async def edit_message(self, target: str, message_id: str, text: str) -> None:
        """Edit a previously sent message (for streaming updates)."""
        if self._app is None:
            return
        chat_id = int(target)
        html_text = _md_to_tg_html(text)
        try:
            await self._app.bot.edit_message_text(
                chat_id=chat_id,
                message_id=int(message_id),
                text=html_text,
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            exc_str = str(exc)
            if "message is not modified" in exc_str.lower():
                return  # Already up-to-date — don't clobber with plain text
            logger.warning("edit_message HTML failed: %s", exc)
            # Fallback: send without formatting if HTML parsing fails
            try:
                await self._app.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=int(message_id),
                    text=text,
                )
            except Exception:
                pass

    async def send_typing(self, target: str) -> None:
        """Show typing indicator."""
        if self._app is None:
            return
        await self._app.bot.send_chat_action(
            chat_id=int(target),
            action=ChatAction.TYPING,
        )

    # ------------------------------------------------------------------ #
    #  Auth                                                                #
    # ------------------------------------------------------------------ #

    def _is_authorized(self, user_id: int) -> bool:
        """Check if a user is in the allowed_users whitelist."""
        if not self._allowed_users:
            # No whitelist configured — block everyone (fail closed)
            logger.warning("No allowed_users configured — rejecting user %d", user_id)
            return False
        return user_id in self._allowed_users

    # ------------------------------------------------------------------ #
    #  Command handlers — delegate to router for session management        #
    # ------------------------------------------------------------------ #

    def _touch(self) -> None:
        """Record that we received an update from Telegram."""
        self._last_update_time = time.monotonic()

    async def _handle_start(self, update: Update, context: Any) -> None:
        """Handle /start command."""
        self._touch()
        user_id = update.effective_user.id
        if not self._is_authorized(user_id):
            logger.warning("Unauthorized /start from user %d", user_id)
            return
        await update.message.reply_text(
            "Connected to Nerve. Send me a message to start chatting."
        )
        logger.info("Telegram user authorized: %d", user_id)

    async def _handle_session(self, update: Update, context: Any) -> None:
        """Handle /session <id> — switch active session."""
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        chat_id = update.effective_chat.id
        channel_key = f"telegram:{chat_id}"

        args = context.args
        if not args:
            # Show current session
            current = await self.router.get_active_session(channel_key, source="telegram")
            await update.message.reply_text(
                f"Current session: `{current}`", parse_mode=ParseMode.MARKDOWN,
            )
            return

        session_id = args[0]
        try:
            await self.router.switch_session(channel_key, session_id)
            await update.message.reply_text(
                f"Switched to session: `{session_id}`", parse_mode=ParseMode.MARKDOWN,
            )
        except ValueError as e:
            await update.message.reply_text(str(e))

    async def _handle_sessions(self, update: Update, context: Any) -> None:
        """Handle /sessions — list sessions."""
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return

        sessions = await self.router.list_sessions(limit=20)
        if not sessions:
            await update.message.reply_text("No sessions.")
            return

        lines = []
        for s in sessions:
            lines.append(f"• `{s['id']}` — {s.get('title', 'untitled')}")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

    async def _handle_new_session(self, update: Update, context: Any) -> None:
        """Handle /new [title] — create and switch to a new session."""
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        chat_id = update.effective_chat.id

        title = " ".join(context.args) if context.args else None
        session_id = await self.router.create_session(
            f"telegram:{chat_id}", title=title, source="telegram",
        )
        await update.message.reply_text(
            f"New session: `{session_id}`" + (f" — {title}" if title else ""),
            parse_mode=ParseMode.MARKDOWN,
        )

    # ------------------------------------------------------------------ #
    #  Message handler — construct InboundMessage and delegate             #
    # ------------------------------------------------------------------ #

    async def _handle_message(self, update: Update, context: Any) -> None:
        """Handle incoming text messages — delegate to router."""
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        chat_id = update.effective_chat.id
        text = update.message.text
        logger.info(
            "Telegram message from %s: %s",
            chat_id, text[:80] + ("..." if len(text) > 80 else ""),
        )

        msg = InboundMessage(
            channel_name="telegram",
            channel_key=f"telegram:{chat_id}",
            sender_id=str(chat_id),
            text=text,
        )

        try:
            await self.router.handle_message(msg)
        except Exception as e:
            logger.error("Agent error for chat %s: %s", chat_id, e, exc_info=True)
            try:
                await update.message.reply_text(f"Error: {e}")
            except Exception:
                logger.error("Failed to send error reply to chat %s", chat_id)

    # ------------------------------------------------------------------ #
    #  Error handler                                                       #
    # ------------------------------------------------------------------ #

    async def _handle_error(self, update: object, context: Any) -> None:
        """Log errors from the Telegram bot polling/handler pipeline."""
        self._touch()
        logger.error(
            "Telegram update error: %s (update=%s)",
            context.error, update, exc_info=context.error,
        )

    # ------------------------------------------------------------------ #
    #  Notification callback handlers                                      #
    # ------------------------------------------------------------------ #

    async def _handle_callback_query(self, update: Update, context: Any) -> None:
        """Handle inline keyboard button presses for notification questions."""
        self._touch()
        query = update.callback_query
        if not query or not query.data:
            return

        if not self._is_authorized(query.from_user.id):
            await query.answer("Unauthorized", show_alert=True)
            return

        # Parse callback_data: "notif:{notification_id}:{answer}"
        parts = query.data.split(":", 2)
        if len(parts) < 3 or parts[0] != "notif":
            await query.answer()
            return

        notification_id = parts[1]
        answer = parts[2]

        if not self._notification_service:
            await query.answer("Service unavailable", show_alert=True)
            return

        success = await self._notification_service.handle_answer(
            notification_id=notification_id,
            answer=answer,
            answered_by="telegram",
        )

        if success:
            await query.answer(f"Answered: {answer}")
            try:
                original = query.message.text or ""
                await query.edit_message_text(
                    text=f"{original}\n\n\u2705 Answered: {answer}",
                    reply_markup=None,
                )
            except Exception:
                pass
        else:
            await query.answer("Already answered or expired", show_alert=True)

    async def _handle_reply(self, update: Update, context: Any) -> None:
        """Handle /reply <text> — answer the most recent pending question."""
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        if not context.args:
            await update.message.reply_text("Usage: /reply <your answer>")
            return
        if not self._notification_service:
            await update.message.reply_text("Notification service not available")
            return

        answer_text = " ".join(context.args)

        pending = await self._notification_service.db.list_notifications(
            status="pending", type="question", limit=1,
        )
        if not pending:
            await update.message.reply_text("No pending questions.")
            return

        notification_id = pending[0]["id"]
        success = await self._notification_service.handle_answer(
            notification_id=notification_id,
            answer=answer_text,
            answered_by="telegram",
        )

        if success:
            await update.message.reply_text(f"Answer recorded for: {pending[0]['title']}")
        else:
            await update.message.reply_text("Failed to record answer.")
