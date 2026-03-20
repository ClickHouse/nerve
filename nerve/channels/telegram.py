"""Telegram bot channel — receive messages, run agent, respond.

Uses python-telegram-bot (v21+) for async Telegram bot communication.
Supports partial message streaming (edit-in-place) via the StreamAdapter.
Session management is delegated to ChannelRouter.
"""

from __future__ import annotations

import asyncio
import logging
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
# Polling watchdog settings
WATCHDOG_INTERVAL = 30  # seconds between health checks
WATCHDOG_HEARTBEAT_INTERVAL = 10  # log heartbeat every N checks (~5 min)
STALE_THRESHOLD = 600  # seconds without any update before active probe
MAX_RESTART_ATTEMPTS = 10  # consecutive failures before entering cooldown
RESTART_BACKOFF_BASE = 5  # seconds, doubles each attempt, capped at 300s
COOLDOWN_INTERVAL = 300  # seconds to wait after exhausting restart attempts
PROBE_TIMEOUT = 10  # seconds for bot.get_me() active health check


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

    async def start(self) -> None:
        """Start the Telegram bot."""
        if not self.config.telegram.bot_token:
            logger.warning("Telegram bot token not configured")
            return

        self._stopping = False
        self._app = (
            Application.builder()
            .token(self.config.telegram.bot_token)
            .build()
        )

        # Register handlers
        self._app.add_handler(CommandHandler("start", self._handle_start))
        self._app.add_handler(CommandHandler("session", self._handle_session))
        self._app.add_handler(CommandHandler("sessions", self._handle_sessions))
        self._app.add_handler(CommandHandler("new", self._handle_new_session))
        self._app.add_handler(CommandHandler("reply", self._handle_reply))
        self._app.add_handler(CallbackQueryHandler(self._handle_callback_query))
        self._app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_message))
        self._app.add_error_handler(self._handle_error)

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling(drop_pending_updates=True)
        self._last_update_time = time.monotonic()
        logger.info("Telegram bot started polling")

        # Launch watchdog to auto-restart polling if it silently dies
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
    #  Polling watchdog — detect silent crashes and auto-restart           #
    # ------------------------------------------------------------------ #

    async def _run_watchdog(self) -> None:
        """Monitor polling health and auto-restart if it dies.

        Three layers of health checking:
        1. Task alive? — is the internal polling asyncio task still running
        2. Updater flag — does PTB think it's polling
        3. Active probe — can we actually reach Telegram's API (bot.get_me)

        The active probe fires when we haven't received any update in
        STALE_THRESHOLD seconds, catching cases where the task is alive
        but the connection is silently hung.

        Never gives up permanently — after exhausting rapid restart
        attempts, enters a cooldown period and then tries again.
        """
        restart_count = 0
        check_count = 0
        while not self._stopping:
            try:
                await asyncio.sleep(WATCHDOG_INTERVAL)
            except asyncio.CancelledError:
                break

            if self._app is None or self._stopping:
                break

            check_count += 1

            # Heartbeat log so we know the watchdog itself is alive
            if check_count % WATCHDOG_HEARTBEAT_INTERVAL == 0:
                elapsed = (
                    f"{time.monotonic() - self._last_update_time:.0f}s ago"
                    if self._last_update_time > 0
                    else "never"
                )
                logger.info(
                    "Telegram watchdog heartbeat: check #%d, "
                    "last_update=%s, restarts=%d",
                    check_count, elapsed, restart_count,
                )

            try:
                alive = await self._is_polling_healthy()
            except Exception as e:
                logger.error(
                    "Watchdog health check failed: %s", e, exc_info=True,
                )
                continue

            if alive:
                if restart_count > 0:
                    logger.info(
                        "Telegram polling healthy after %d restart(s)",
                        restart_count,
                    )
                restart_count = 0
                continue

            # Polling is dead or unresponsive
            restart_count += 1
            if restart_count > MAX_RESTART_ATTEMPTS:
                logger.warning(
                    "Telegram polling: exhausted %d restart attempts — "
                    "entering cooldown (%ds) before retrying",
                    MAX_RESTART_ATTEMPTS, COOLDOWN_INTERVAL,
                )
                try:
                    await asyncio.sleep(COOLDOWN_INTERVAL)
                except asyncio.CancelledError:
                    break
                if self._stopping:
                    break
                restart_count = 1
                logger.info("Telegram polling: cooldown over, resuming restart attempts")

            delay = min(RESTART_BACKOFF_BASE * (2 ** (restart_count - 1)), 300)
            logger.warning(
                "Telegram polling dead — restarting in %ds (attempt %d/%d)",
                delay, restart_count, MAX_RESTART_ATTEMPTS,
            )

            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                break

            if self._stopping:
                break

            try:
                await self._restart_polling()
                self._last_update_time = time.monotonic()
                logger.info(
                    "Telegram polling restarted successfully (attempt %d/%d)",
                    restart_count, MAX_RESTART_ATTEMPTS,
                )
            except Exception as e:
                logger.error(
                    "Telegram polling restart failed: %s", e, exc_info=True,
                )

    async def _is_polling_healthy(self) -> bool:
        """Multi-layer health check for Telegram polling.

        Layer 1: Is the updater flag set?
        Layer 2: Is the internal polling task still running?
        Layer 3: If stale (no updates for STALE_THRESHOLD), actively
                 probe the API with bot.get_me() to detect hung connections.
        """
        if not self._app or not self._app.updater:
            return False

        # Layer 1: updater thinks it's not running → definitely dead
        if not self._app.updater.running:
            logger.warning("Telegram watchdog: updater.running is False")
            return False

        # Layer 2: internal polling task crashed silently
        polling_task: asyncio.Task | None = getattr(
            self._app.updater, "_Updater__polling_task", None,
        )
        if polling_task is not None and polling_task.done():
            try:
                exc = polling_task.exception()
            except (asyncio.CancelledError, asyncio.InvalidStateError):
                exc = None
            logger.warning(
                "Telegram watchdog: polling task is dead (exception: %s)", exc,
            )
            return False

        # Layer 3: if no updates received recently, probe the API
        now = time.monotonic()
        if self._last_update_time > 0:
            stale_seconds = now - self._last_update_time
        else:
            # Never received an update — use time since start
            stale_seconds = now - self._last_update_time if self._last_update_time else STALE_THRESHOLD + 1

        if stale_seconds > STALE_THRESHOLD:
            logger.info(
                "Telegram watchdog: no updates for %.0fs, running active probe",
                stale_seconds,
            )
            try:
                me = await asyncio.wait_for(
                    self._app.bot.get_me(), timeout=PROBE_TIMEOUT,
                )
                if me:
                    # API is reachable — polling task might just have no messages
                    # That's fine, reset the timer to avoid spamming probes
                    logger.info(
                        "Telegram watchdog: active probe OK (bot: @%s)",
                        me.username,
                    )
                    self._last_update_time = now
                    return True
            except asyncio.TimeoutError:
                logger.warning(
                    "Telegram watchdog: active probe timed out after %ds",
                    PROBE_TIMEOUT,
                )
                return False
            except Exception as e:
                logger.warning(
                    "Telegram watchdog: active probe failed: %s", e,
                )
                return False

        return True

    async def _restart_polling(self) -> None:
        """Stop the updater cleanly, then start a fresh polling loop."""
        if self._app is None:
            return

        # stop() sets _running=False and cancels the dead task
        try:
            await self._app.updater.stop()
        except Exception as e:
            logger.debug("Updater stop during restart: %s", e)

        await asyncio.sleep(1)

        # Don't drop pending updates on restart — we might have missed some
        await self._app.updater.start_polling(drop_pending_updates=False)

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
            await self._app.bot.send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode=ParseMode.MARKDOWN,
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
        await self._app.bot.edit_message_text(
            chat_id=chat_id,
            message_id=int(message_id),
            text=text,
        )

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
