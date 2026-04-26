"""Telegram bot channel — receive messages, run agent, respond.

Uses python-telegram-bot (v21+) for async Telegram bot communication.
Supports partial message streaming (edit-in-place) via the StreamAdapter.
Session management is delegated to ChannelRouter.
"""

from __future__ import annotations

import asyncio
import base64
import collections
import io
import html as _html
import logging
import re
import socket
import subprocess
import sys
import time
import zipfile
from typing import Any, TYPE_CHECKING

from telegram import Update
from telegram.constants import ChatAction, ParseMode
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, MessageHandler, MessageReactionHandler, filters

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

    # Expandable blockquotes: <blockquote expandable>...</blockquote>
    def _expandable_bq(m: re.Match) -> str:
        inner = _md_to_tg_html(m.group(1))
        return _protect(f"<blockquote expandable>{inner}</blockquote>")
    text = re.sub(
        r"<blockquote\s+expandable>(.*?)</blockquote>",
        _expandable_bq, text, flags=re.DOTALL,
    )

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


def _format_forward_context(message: Any) -> str:
    """Extract forward origin info from a Telegram message.

    Returns a prefix string like:
        [Forwarded from Иван Иванов]
        [Forwarded from Новости]
        [Forwarded from hidden user "Аноним"]

    Returns empty string if the message is not forwarded.
    """
    origin = getattr(message, "forward_origin", None)
    if not origin:
        return ""

    origin_type = getattr(origin, "type", None)

    if origin_type == "user":
        user = getattr(origin, "sender_user", None)
        if user:
            name = getattr(user, "first_name", "") or ""
            last = getattr(user, "last_name", "") or ""
            full = f"{name} {last}".strip() or "unknown"
            username = getattr(user, "username", None)
            if username:
                return f'[Forwarded from {full} (@{username})]'
            return f'[Forwarded from {full}]'

    elif origin_type == "hidden_user":
        name = getattr(origin, "sender_user_name", None) or "unknown"
        return f'[Forwarded from hidden user "{name}"]'

    elif origin_type == "chat":
        chat = getattr(origin, "sender_chat", None)
        if chat:
            title = getattr(chat, "title", None) or "unknown chat"
            return f'[Forwarded from chat "{title}"]'

    elif origin_type == "channel":
        chat = getattr(origin, "chat", None)
        if chat:
            title = getattr(chat, "title", None) or "unknown channel"
            return f'[Forwarded from channel "{title}"]'

    return "[Forwarded message]"


def _format_reply_context(message: Any) -> str:
    """Extract reply-to context and quote from a Telegram message.

    Returns a prefix string like:
        [Reply to assistant: "original text here"]
        [Quoted: "selected portion"]

    Returns empty string if the message is not a reply.
    """
    reply = getattr(message, "reply_to_message", None)
    if not reply:
        return ""

    parts: list[str] = []

    # Determine sender label
    from_user = getattr(reply, "from_user", None)
    if from_user and getattr(from_user, "is_bot", False):
        sender = "assistant"
    elif from_user:
        sender = getattr(from_user, "first_name", None) or "user"
    else:
        sender = "user"

    # Original message text
    original = getattr(reply, "text", None) or getattr(reply, "caption", None) or ""
    if original:
        display = original if len(original) <= 500 else original[:500] + "…"
        parts.append(f'[Reply to {sender}: "{display}"]')
    else:
        parts.append(f"[Reply to {sender}'s message]")

    # Quote (manually selected text)
    quote = getattr(message, "quote", None)
    if quote:
        quote_text = getattr(quote, "text", None)
        if quote_text:
            parts.append(f'[Quoted: "{quote_text}"]')

    return "\n".join(parts)


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
        # Media group (album) collection: group_id -> list of updates
        self._media_groups: dict[str, list[Update]] = {}
        self._media_group_tasks: dict[str, asyncio.Task] = {}
        # Message cache for reaction context: message_id -> (chat_id, text_snippet)
        self._message_cache: collections.OrderedDict[int, tuple[int, str]] = (
            collections.OrderedDict()
        )
        self._message_cache_max = 200

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
            | ChannelCapability.REACTIONS
            | ChannelCapability.SEND_FILES
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
            # Process updates concurrently so /stop can interrupt a running
            # message handler instead of being queued behind it.
            .concurrent_updates(True)
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
        app.add_handler(CommandHandler("stop", self._handle_stop))
        app.add_handler(CommandHandler("restart", self._handle_restart))
        app.add_handler(CommandHandler("doctor", self._handle_doctor))
        app.add_handler(CommandHandler("reply", self._handle_reply))
        app.add_handler(CallbackQueryHandler(self._handle_callback_query))
        app.add_handler(MessageHandler(
            filters.TEXT | filters.PHOTO | filters.COMMAND | filters.Sticker.ALL | filters.Document.ALL,
            self._handle_message,
        ))
        app.add_handler(MessageReactionHandler(self._handle_reaction))
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
            # Explicitly request all update types — the default set excludes
            # message_reaction, and auto-detection only works via
            # Application.run_polling(), not Updater.start_polling().
            allowed_updates=Update.ALL_TYPES,
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
            allowed_updates=Update.ALL_TYPES,
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
                sent = await self._app.bot.send_message(
                    chat_id=chat_id,
                    text=html_chunk,
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                sent = await self._app.bot.send_message(
                    chat_id=chat_id,
                    text=chunk,
                )
            self._cache_message(sent.message_id, chat_id, chunk)

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
        msg = await self._app.bot.send_message(chat_id=chat_id, text="⏳")
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
        # Update cache with the latest text (streaming overwrites placeholder)
        self._cache_message(int(message_id), int(target), text)

    async def delete_message(self, target: str, message_id: str) -> None:
        """Delete a previously sent message."""
        if self._app is None:
            return
        try:
            await self._app.bot.delete_message(
                chat_id=int(target), message_id=int(message_id),
            )
        except Exception as exc:
            logger.warning("delete_message failed: %s", exc)

    async def send_typing(self, target: str) -> None:
        """Show typing indicator."""
        if self._app is None:
            return
        await self._app.bot.send_chat_action(
            chat_id=int(target),
            action=ChatAction.TYPING,
        )

    # ------------------------------------------------------------------ #
    #  Reactions: set emoji on a message                                    #
    # ------------------------------------------------------------------ #

    async def set_reaction(self, target: str, message_id: int, emoji: str) -> None:
        """Set an emoji reaction on a Telegram message."""
        if self._app is None:
            return
        await self._app.bot.set_message_reaction(
            chat_id=int(target),
            message_id=message_id,
            reaction=[emoji],
        )

    # ------------------------------------------------------------------ #
    #  Stickers: send a sticker by file_id                                 #
    # ------------------------------------------------------------------ #

    async def send_sticker(self, target: str, sticker: str) -> None:
        """Send a sticker to a Telegram chat by file_id."""
        if self._app is None:
            return
        await self._app.bot.send_sticker(
            chat_id=int(target),
            sticker=sticker,
        )

    # ------------------------------------------------------------------ #
    #  Files: deliver a workspace file as a Telegram document             #
    # ------------------------------------------------------------------ #

    async def send_file(self, target: str, file_path: str) -> bool:
        """Deliver a file to a Telegram chat as a document attachment.

        Size limits are enforced by the Telegram API itself (50 MiB on
        api.telegram.org, up to 2 GiB on a self-hosted Bot API server).
        We surface the API's verdict via the ``send_document`` exception
        path rather than hard-coding the cap client-side, so this code
        stays correct if the user runs against a local Bot API server.
        """
        if self._app is None:
            return False
        from pathlib import Path
        try:
            resolved = Path(file_path).resolve()
        except (OSError, RuntimeError) as e:
            logger.warning("send_file: failed to resolve %s: %s", file_path, e)
            return False
        if not resolved.exists() or not resolved.is_file():
            return False
        try:
            with open(resolved, "rb") as f:
                await self._app.bot.send_document(
                    chat_id=int(target),
                    document=f,
                    filename=resolved.name,
                )
            return True
        except Exception as e:
            logger.warning("send_file: send_document failed for %s: %s", target, e)
            return False

    # ------------------------------------------------------------------ #
    #  Message cache (for reaction context)                                #
    # ------------------------------------------------------------------ #

    def _cache_message(self, message_id: int, chat_id: int, text: str) -> None:
        """Store a message snippet in the LRU cache for reaction lookups."""
        snippet = text[:200] if text else ""
        if not snippet:
            return
        self._message_cache[message_id] = (chat_id, snippet)
        # Move to end (most recent) and evict oldest if over limit
        self._message_cache.move_to_end(message_id)
        while len(self._message_cache) > self._message_cache_max:
            self._message_cache.popitem(last=False)

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
        """Handle /new [title] — stop current session, create and switch to a new one."""
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        chat_id = update.effective_chat.id
        channel_key = f"telegram:{chat_id}"

        # Stop the current session before creating a new one
        prev = await self.router.get_last_session(channel_key)
        if prev:
            stopped = await self.router.engine.stop_session(prev)
            if stopped:
                await update.message.reply_text(
                    f"Stopped session `{prev}`.",
                    parse_mode=ParseMode.MARKDOWN,
                )

        title = " ".join(context.args) if context.args else None
        session_id = await self.router.create_session(
            channel_key, title=title, source="telegram",
        )
        await update.message.reply_text(
            f"New session: `{session_id}`" + (f" — {title}" if title else ""),
            parse_mode=ParseMode.MARKDOWN,
        )

    async def _handle_stop(self, update: Update, context: Any) -> None:
        """Handle /stop — stop the currently running session."""
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        chat_id = update.effective_chat.id
        channel_key = f"telegram:{chat_id}"

        session_id = await self.router.get_last_session(channel_key)
        if not session_id:
            await update.message.reply_text("No active session.")
            return

        stopped = await self.router.engine.stop_session(session_id)
        if stopped:
            await update.message.reply_text(
                f"Stopped session `{session_id}`.",
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await update.message.reply_text("Nothing running to stop.")

    async def _handle_restart(self, update: Update, context: Any) -> None:
        """Handle /restart — run ``nerve restart`` to restart the daemon."""
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        await update.message.reply_text("Restarting Nerve...")
        logger.info("Restart requested by Telegram user %d", update.effective_user.id)
        subprocess.Popen(
            [sys.executable, "-m", "nerve", "restart"],
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    async def _handle_doctor(self, update: Update, context: Any) -> None:
        """Handle /doctor — run health checks and return the report."""
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return
        from nerve.cli import doctor_report
        report = doctor_report(self.config)
        await update.message.reply_text(
            f"```\n{report}\n```",
            parse_mode=ParseMode.MARKDOWN,
        )

    # ------------------------------------------------------------------ #
    #  Message handler — construct InboundMessage and delegate             #
    # ------------------------------------------------------------------ #

    async def _extract_image(self, message: Any) -> dict[str, str] | None:
        """Download and base64-encode an image from a Telegram message."""
        if message.photo:
            # Telegram provides multiple resolutions; pick the largest
            photo = message.photo[-1]
            tg_file = await photo.get_file()
            data = await tg_file.download_as_bytearray()
            return {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": base64.b64encode(bytes(data)).decode("utf-8"),
            }
        return None

    async def _extract_sticker(
        self, message: Any,
    ) -> tuple[dict[str, str] | None, str]:
        """Extract sticker image and context text from a Telegram message.

        Returns (image_dict_or_None, context_text).
        Static stickers (.webp) are downloaded as full images.
        Animated/video stickers use their thumbnail instead.
        """
        sticker = getattr(message, "sticker", None)
        if not sticker:
            return None, ""

        emoji = sticker.emoji or ""
        set_name = sticker.set_name or ""
        file_id = sticker.file_id

        # Build human-readable context
        parts = []
        if emoji:
            parts.append(emoji)
        if set_name:
            parts.append(f'set "{set_name}"')
        context = f'[Sticker: {", ".join(parts)}, file_id: {file_id}]'

        # Download image — static stickers are WEBP; animated/video use thumbnail
        image: dict[str, str] | None = None
        try:
            if sticker.is_animated or sticker.is_video:
                thumb = sticker.thumbnail
                if thumb:
                    tg_file = await thumb.get_file()
                    data = await tg_file.download_as_bytearray()
                    image = {
                        "type": "base64",
                        "media_type": "image/webp",
                        "data": base64.b64encode(bytes(data)).decode("utf-8"),
                    }
            else:
                tg_file = await sticker.get_file()
                data = await tg_file.download_as_bytearray()
                image = {
                    "type": "base64",
                    "media_type": "image/webp",
                    "data": base64.b64encode(bytes(data)).decode("utf-8"),
                }
        except Exception as e:
            logger.warning("Failed to download sticker image: %s", e)

        return image, context

    # Known text-like MIME types (beyond text/*)
    _TEXT_MIME_EXTRAS: set[str] = {
        "application/json",
        "application/xml",
        "application/javascript",
        "application/x-yaml",
        "application/yaml",
        "application/toml",
        "application/x-sh",
        "application/x-python",
        "application/sql",
        "application/x-httpd-php",
        "application/xhtml+xml",
        "application/csv",
    }

    # Extensions treated as text when MIME type is missing or generic
    _TEXT_EXTENSIONS: set[str] = {
        ".txt", ".py", ".js", ".ts", ".jsx", ".tsx", ".json", ".yaml", ".yml",
        ".toml", ".xml", ".html", ".htm", ".css", ".scss", ".less",
        ".md", ".rst", ".csv", ".tsv", ".sql", ".sh", ".bash", ".zsh",
        ".rb", ".go", ".rs", ".java", ".kt", ".c", ".cpp", ".h", ".hpp",
        ".swift", ".lua", ".r", ".m", ".pl", ".php", ".env", ".ini", ".cfg",
        ".conf", ".log", ".diff", ".patch", ".vue", ".svelte",
    }

    _IMAGE_MIMES: set[str] = {"image/jpeg", "image/png", "image/gif", "image/webp"}

    _ARCHIVE_MIMES: set[str] = {"application/zip", "application/x-zip-compressed"}

    _IMAGE_EXT_TO_MIME: dict[str, str] = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".gif": "image/gif", ".webp": "image/webp",
    }

    _MAX_TEXT_SIZE: int = 512 * 1024       # 512 KB — inline text cap
    _MAX_DOWNLOAD_SIZE: int = 20_000_000   # ~20 MB — Telegram Bot API limit

    async def _extract_document(
        self, message: Any,
    ) -> tuple[list[dict[str, str]], str]:
        """Extract document content and context from a Telegram message.

        Returns (content_blocks, context_text).
        - Text files: blocks=[], context has file content.
        - Images sent as documents: blocks=[image_dict], context has metadata.
        - PDFs: blocks=[pdf_dict], context has metadata.
        - ZIP archives: blocks=[images/pdfs inside], context has metadata + text.
        - Other: blocks=[], context has metadata only.
        """
        doc = getattr(message, "document", None)
        if not doc:
            return [], ""

        file_name = doc.file_name or "unnamed"
        mime = doc.mime_type or ""
        size = doc.file_size or 0
        ext = ""
        if "." in file_name:
            ext = "." + file_name.rsplit(".", 1)[-1].lower()

        size_str = (
            f"{size / 1024:.0f} KB" if size < 1_000_000
            else f"{size / 1_000_000:.1f} MB"
        )
        meta_line = f"[Document: {file_name} ({size_str}, {mime or 'unknown type'})]"

        # -- Too large to download via Bot API --
        if size > self._MAX_DOWNLOAD_SIZE:
            return [], f"{meta_line}\n(File too large to download)"

        # -- Text-like files --
        is_text = (
            mime.startswith("text/")
            or mime in self._TEXT_MIME_EXTRAS
            or ext in self._TEXT_EXTENSIONS
        )
        if is_text:
            if size > self._MAX_TEXT_SIZE:
                return [], f"{meta_line}\n(Text file too large to display — {size_str})"
            try:
                tg_file = await doc.get_file()
                data = await tg_file.download_as_bytearray()
                content = bytes(data).decode("utf-8", errors="replace")
                return [], f"{meta_line}\n```\n{content}\n```"
            except Exception as e:
                logger.warning("Failed to download text document %s: %s", file_name, e)
                return [], meta_line

        # -- Image files sent as documents --
        if mime in self._IMAGE_MIMES:
            try:
                tg_file = await doc.get_file()
                data = await tg_file.download_as_bytearray()
                image = {
                    "type": "base64",
                    "media_type": mime,
                    "data": base64.b64encode(bytes(data)).decode("utf-8"),
                }
                return [image], meta_line
            except Exception as e:
                logger.warning("Failed to download image document %s: %s", file_name, e)
                return [], meta_line

        # -- PDF --
        if mime == "application/pdf" or ext == ".pdf":
            try:
                tg_file = await doc.get_file()
                data = await tg_file.download_as_bytearray()
                pdf_block = {
                    "type": "base64",
                    "media_type": "application/pdf",
                    "data": base64.b64encode(bytes(data)).decode("utf-8"),
                }
                return [pdf_block], meta_line
            except Exception as e:
                logger.warning("Failed to download PDF %s: %s", file_name, e)
                return [], meta_line

        # -- ZIP archives --
        if mime in self._ARCHIVE_MIMES or ext == ".zip":
            return await self._extract_zip(doc, file_name, meta_line)

        # -- Other binary files — metadata only --
        return [], meta_line

    async def _extract_zip(
        self, doc: Any, file_name: str, meta_line: str,
    ) -> tuple[list[dict[str, str]], str]:
        """Extract ZIP archive contents — text inline, images/PDFs as blocks."""
        try:
            tg_file = await doc.get_file()
            data = await tg_file.download_as_bytearray()
        except Exception as e:
            logger.warning("Failed to download ZIP %s: %s", file_name, e)
            return [], meta_line

        buf = io.BytesIO(bytes(data))
        if not zipfile.is_zipfile(buf):
            return [], f"{meta_line}\n(Invalid or corrupted ZIP archive)"
        buf.seek(0)

        blocks: list[dict[str, str]] = []
        parts: list[str] = [meta_line]

        try:
            with zipfile.ZipFile(buf) as zf:
                entries = [
                    i for i in zf.infolist()
                    if not i.is_dir() and not i.filename.startswith("__MACOSX/")
                ]
                parts.append(f"Archive contains {len(entries)} file(s):")

                total_text = 0
                for info in entries:
                    ename = info.filename
                    esize = info.file_size
                    eext = ""
                    if "." in ename.rsplit("/", 1)[-1]:
                        eext = "." + ename.rsplit(".", 1)[-1].lower()

                    is_text = eext in self._TEXT_EXTENSIONS
                    is_image = eext in self._IMAGE_EXT_TO_MIME
                    is_pdf = eext == ".pdf"

                    if is_text and total_text + esize <= self._MAX_TEXT_SIZE:
                        try:
                            raw = zf.read(info.filename)
                            total_text += len(raw)
                            text_content = raw.decode("utf-8", errors="replace")
                            parts.append(
                                f"--- {ename} ({esize} bytes) ---\n"
                                f"```\n{text_content}\n```"
                            )
                        except Exception:
                            parts.append(f"- {ename} ({esize} bytes) [read error]")
                    elif is_text:
                        parts.append(f"- {ename} ({esize} bytes) [text, too large to inline]")
                    elif is_image:
                        try:
                            raw = zf.read(info.filename)
                            img_mime = self._IMAGE_EXT_TO_MIME.get(eext, "image/png")
                            blocks.append({
                                "type": "base64",
                                "media_type": img_mime,
                                "data": base64.b64encode(raw).decode("utf-8"),
                            })
                            parts.append(f"- {ename} ({esize} bytes) [image]")
                        except Exception:
                            parts.append(f"- {ename} ({esize} bytes) [read error]")
                    elif is_pdf:
                        try:
                            raw = zf.read(info.filename)
                            blocks.append({
                                "type": "base64",
                                "media_type": "application/pdf",
                                "data": base64.b64encode(raw).decode("utf-8"),
                            })
                            parts.append(f"- {ename} ({esize} bytes) [PDF]")
                        except Exception:
                            parts.append(f"- {ename} ({esize} bytes) [read error]")
                    else:
                        parts.append(f"- {ename} ({esize} bytes)")
        except zipfile.BadZipFile:
            return [], f"{meta_line}\n(Invalid or corrupted ZIP archive)"
        except RuntimeError as e:
            # Password-protected archives
            return [], f"{meta_line}\n(Cannot extract: {e})"

        return blocks, "\n".join(parts)

    async def _handle_message(self, update: Update, context: Any) -> None:
        """Handle incoming text and photo messages — delegate to router."""
        self._touch()
        if not self._is_authorized(update.effective_user.id):
            return

        # Media group (album) — collect all parts before processing
        if update.message.media_group_id:
            await self._collect_media_group(update)
            return

        chat_id = update.effective_chat.id
        text = update.message.text or update.message.caption or ""

        # Cache for reaction lookups (raw text, before reply-context prefix)
        self._cache_message(update.message.message_id, chat_id, text)

        # Prepend forward origin (if forwarded message)
        forward_context = _format_forward_context(update.message)
        if forward_context:
            text = f"{forward_context}\n\n{text}" if text else forward_context

        # Prepend reply-to context and quote (if replying to a message)
        reply_context = _format_reply_context(update.message)
        if reply_context:
            text = f"{reply_context}\n\n{text}" if text else reply_context

        # Extract image if present
        images: list[dict[str, str]] = []
        image = await self._extract_image(update.message)
        if image:
            images.append(image)

        # Extract sticker if present
        sticker_image, sticker_context = await self._extract_sticker(update.message)
        if sticker_context:
            text = f"{sticker_context}\n\n{text}" if text else sticker_context
        if sticker_image:
            images.append(sticker_image)

        # Extract document/file if present
        doc_contents, doc_context = await self._extract_document(update.message)
        if doc_context:
            text = f"{doc_context}\n\n{text}" if text else doc_context
        if doc_contents:
            images.extend(doc_contents)

        # Extract attachments from replied-to message (images, documents, PDFs)
        reply_msg = getattr(update.message, "reply_to_message", None)
        if reply_msg:
            reply_image = await self._extract_image(reply_msg)
            if reply_image:
                images.append(reply_image)
            reply_doc_contents, reply_doc_ctx = await self._extract_document(reply_msg)
            if reply_doc_ctx:
                text = f"{reply_doc_ctx}\n\n{text}" if text else reply_doc_ctx
            if reply_doc_contents:
                images.extend(reply_doc_contents)

        if not text and not images:
            return

        logger.info(
            "Telegram message from %s: %s%s",
            chat_id,
            (text[:80] + ("..." if len(text) > 80 else "")) if text else "(no text)",
            f" [{len(images)} image(s)]" if images else "",
        )

        metadata: dict[str, Any] = {"message_id": update.message.message_id}
        if images:
            metadata["images"] = images

        msg = InboundMessage(
            channel_name="telegram",
            channel_key=f"telegram:{chat_id}",
            sender_id=str(chat_id),
            text=text,
            metadata=metadata,
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
    #  Reaction handler                                                    #
    # ------------------------------------------------------------------ #

    async def _handle_reaction(self, update: Update, context: Any) -> None:
        """Handle message reaction updates — forward as text to router."""
        self._touch()
        reaction_update = update.message_reaction
        if reaction_update is None:
            return

        user = reaction_update.user
        if user is None or not self._is_authorized(user.id):
            return

        chat_id = reaction_update.chat.id
        message_id = reaction_update.message_id

        # Extract new emoji reactions (standard + premium custom emoji)
        emojis = []
        custom_ids = []
        for r in reaction_update.new_reaction:
            emoji = getattr(r, "emoji", None)
            if emoji:
                emojis.append(emoji)
            else:
                custom_id = getattr(r, "custom_emoji_id", None)
                if custom_id:
                    custom_ids.append(custom_id)

        # Resolve premium custom emoji IDs to their base emoji
        if custom_ids:
            try:
                stickers = await self._app.bot.get_custom_emoji_stickers(custom_ids)
                for sticker in stickers:
                    emojis.append(sticker.emoji or f"[premium:{sticker.custom_emoji_id}]")
            except Exception as e:
                logger.warning("Failed to resolve custom emoji: %s", e)
                emojis.extend(f"[premium:{cid}]" for cid in custom_ids)

        if not emojis:
            # Reaction removed — ignore for now
            return

        emoji_str = " ".join(emojis)

        # Look up original message text from cache
        cached = self._message_cache.get(message_id)
        if cached:
            _, original_text = cached
            text = f'[Reaction: {emoji_str} on message: "{original_text}"]'
        else:
            text = f"[Reaction: {emoji_str}]"

        logger.info("Telegram reaction from %s: %s (msg %d)", chat_id, emoji_str, message_id)

        msg = InboundMessage(
            channel_name="telegram",
            channel_key=f"telegram:{chat_id}",
            sender_id=str(chat_id),
            text=text,
            metadata={},
        )

        try:
            await self.router.handle_message(msg)
        except Exception as e:
            logger.error("Agent error for reaction in chat %s: %s", chat_id, e, exc_info=True)

    # ------------------------------------------------------------------ #
    #  Media group (album) collection                                     #
    # ------------------------------------------------------------------ #

    async def _collect_media_group(self, update: Update) -> None:
        """Buffer a media-group message; schedule processing after a short delay."""
        group_id = update.message.media_group_id
        if group_id not in self._media_groups:
            self._media_groups[group_id] = []
        self._media_groups[group_id].append(update)

        # Reset the timer — wait for remaining album parts
        task = self._media_group_tasks.get(group_id)
        if task:
            task.cancel()
        self._media_group_tasks[group_id] = asyncio.create_task(
            self._process_media_group(group_id),
        )

    async def _process_media_group(self, group_id: str) -> None:
        """Wait briefly for all album parts, then send as one message."""
        await asyncio.sleep(0.5)

        updates = self._media_groups.pop(group_id, [])
        self._media_group_tasks.pop(group_id, None)
        if not updates:
            return

        chat_id = updates[0].effective_chat.id

        # Caption is usually on the first message only
        text = ""
        for u in updates:
            caption = u.message.text or u.message.caption or ""
            if caption:
                text = caption
                break

        # Prepend forward origin (album forward info is on the first message)
        forward_context = _format_forward_context(updates[0].message)
        if forward_context:
            text = f"{forward_context}\n\n{text}" if text else forward_context

        # Prepend reply-to context (album reply info is on the first message)
        reply_context = _format_reply_context(updates[0].message)
        if reply_context:
            text = f"{reply_context}\n\n{text}" if text else reply_context

        # Download all images
        images: list[dict[str, str]] = []
        for u in updates:
            image = await self._extract_image(u.message)
            if image:
                images.append(image)

        # Extract attachments from replied-to message (images, documents, PDFs)
        reply_msg = getattr(updates[0].message, "reply_to_message", None)
        if reply_msg:
            reply_image = await self._extract_image(reply_msg)
            if reply_image:
                images.append(reply_image)
            reply_doc_contents, reply_doc_ctx = await self._extract_document(reply_msg)
            if reply_doc_ctx:
                text = f"{reply_doc_ctx}\n\n{text}" if text else reply_doc_ctx
            if reply_doc_contents:
                images.extend(reply_doc_contents)

        if not text and not images:
            return

        logger.info(
            "Telegram media group from %s: %d image(s), caption: %s",
            chat_id, len(images),
            (text[:80] + "..." if len(text) > 80 else text) if text else "(none)",
        )

        metadata: dict[str, Any] = {"message_id": updates[0].message.message_id}
        if images:
            metadata["images"] = images

        msg = InboundMessage(
            channel_name="telegram",
            channel_key=f"telegram:{chat_id}",
            sender_id=str(chat_id),
            text=text,
            metadata=metadata,
        )

        try:
            await self.router.handle_message(msg)
        except Exception as e:
            logger.error("Agent error for chat %s: %s", chat_id, e, exc_info=True)
            try:
                await updates[0].message.reply_text(f"Error: {e}")
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
