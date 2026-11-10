"""Telegram topics for sessions that never asked for one (``telegram.topics.auto``).

With the switch on, a session from one of the configured sources gets a topic
in the home chat when a turn of it starts, and lets go of it when the session
is archived, deleted, or (a one-run cron session) its run ends. With
``mirror_turns`` each finished turn that did not come from Telegram is posted
into the topic, so the topic follows a conversation held in the web UI.

State lives in the session's metadata under ``telegram_topic_auto``::

    {"channel_key": "telegram:<chat>:<thread>", "opened_at": ..., "title": ...}
    {"error": "...", "failed_at": ...}            # Telegram refused: not retried
    {... , "closed_at": ..., "outcome": "left_open" | "closed" | "deleted" | "gone"}

The key is set once per session, so a session gets at most one topic this way:
a topic someone deleted or closed is not opened again, and only topics this
opened are closed by it. Everything runs in background tasks, one at a time
per session, so the turn never waits on Telegram.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from fnmatch import fnmatchcase
from typing import TYPE_CHECKING, Awaitable

from nerve.channels.session_events import (
    SESSION_ARCHIVED,
    SESSION_DELETED,
    TurnFinished,
    TurnStarted,
)

if TYPE_CHECKING:
    from nerve.channels.telegram import TelegramChannel
    from nerve.config import TelegramAutoTopicsConfig

logger = logging.getLogger(__name__)

MARKER = "telegram_topic_auto"

# The longest user message quoted above a mirrored answer.
_QUOTE_MAX = 600

_CLOSE_EVENT = {SESSION_ARCHIVED: "archive", SESSION_DELETED: "delete"}
_CLOSE_NOTE = {
    "archive": "Session archived. This topic no longer follows it.",
    "delete": "Session deleted. This topic no longer follows it.",
    "cron_done": "Run finished.",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _metadata(session: dict) -> dict:
    raw = session.get("metadata")
    if isinstance(raw, dict):
        return raw
    try:
        meta = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return {}
    return meta if isinstance(meta, dict) else {}


def is_one_run_cron(session: dict) -> bool:
    """A cron session made for one run (``cron:<job>:<run>``), not a persistent one."""
    sid = str(session.get("id") or "")
    return session.get("source") == "cron" and sid.startswith("cron:") and sid.count(":") >= 2


def _clip(text: str, limit: int) -> str:
    text = text.strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def mirror_text(event: TurnFinished, max_chars: int) -> str:
    """The message a finished turn becomes in its topic (markdown)."""
    parts: list[str] = []
    if event.user_message and event.user_message.strip():
        quoted = _clip(event.user_message, _QUOTE_MAX).replace("\n", "\n> ")
        parts.append(f"> **{event.channel or event.source}:** {quoted}")
    elif event.autonomous:
        parts.append("_Background continuation_")
    answer = event.text.strip()
    if len(answer) > max_chars:
        answer = answer[:max_chars].rstrip() + "\n\n_…cut here; the full answer is in the web UI._"
    if event.status == "failed":
        answer = f"⚠️ {answer}"
    parts.append(answer)
    return "\n\n".join(parts)


class AutoTopics:
    """The ``telegram.topics.auto`` behaviour of one Telegram channel."""

    def __init__(self, channel: TelegramChannel):
        self._channel = channel
        self._locks: dict[str, asyncio.Lock] = {}
        self._tasks: set[asyncio.Task] = set()

    # -- config ----------------------------------------------------------

    @property
    def _cfg(self) -> TelegramAutoTopicsConfig:
        return self._channel.config.telegram.topics.auto

    def _active(self) -> bool:
        topics = self._channel.config.telegram.topics
        return bool(topics.enabled and topics.auto.enabled and self._channel.is_running)

    def _eligible(self, session: dict) -> bool:
        cfg = self._cfg
        if session.get("status") == "archived":
            return False
        if session.get("source") not in cfg.sources:
            return False
        sid = str(session.get("id") or "")
        return not any(fnmatchcase(sid, pat) for pat in cfg.exclude)

    # -- plumbing --------------------------------------------------------

    def _spawn(self, session_id: str, work: Awaitable[None]) -> None:
        """Run *work* in the background, after earlier work for the session."""
        lock = self._locks.setdefault(session_id, asyncio.Lock())

        async def _run() -> None:
            try:
                async with lock:
                    await work
            except Exception as exc:
                logger.warning("Auto topic work for %s failed: %s", session_id, exc)

        task = asyncio.get_running_loop().create_task(_run(), name=f"auto-topic:{session_id}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def wait_idle(self) -> None:
        """Wait for the background work queued so far (tests, shutdown)."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    async def _session(self, session_id: str) -> dict | None:
        return await self._channel.router.get_session(session_id)

    async def _set_marker(self, session_id: str, value: dict) -> None:
        db = self._channel.router.engine.db
        await db.set_session_metadata_key(session_id, MARKER, value)

    async def _bound_topic(self, session_id: str, marker: dict) -> str | None:
        """The marker's topic if it is still bound to the session, else None."""
        key = marker.get("channel_key")
        if not key or marker.get("closed_at"):
            return None
        keys = await self._channel.router.channel_keys_for_session(session_id)
        return key if key in keys else None

    # -- hooks -----------------------------------------------------------

    def turn_started(self, event: TurnStarted) -> None:
        if self._active():
            self._spawn(event.session_id, self._open(event.session_id))

    def turn_finished(self, event: TurnFinished) -> None:
        if self._active():
            self._spawn(event.session_id, self._after_turn(event))

    async def session_ending(self, session_id: str, reason: str) -> None:
        event = _CLOSE_EVENT.get(reason)
        if event is None or not self._channel.is_running or event not in self._cfg.close_on:
            return
        lock = self._locks.setdefault(session_id, asyncio.Lock())
        async with lock:
            await self._close(session_id, event)

    # -- work ------------------------------------------------------------

    async def _open(self, session_id: str) -> None:
        session = await self._session(session_id)
        if not session or not self._eligible(session):
            return
        if MARKER in _metadata(session):
            return  # opened, refused or closed before: once per session
        keys = await self._channel.router.channel_keys_for_session(session_id)
        if any(k.startswith("telegram:") for k in keys):
            return  # it is on Telegram already (a Telegram session, or opened by hand)
        result = await self._channel.open_session_topic(session_id)
        if result.channel_key:
            await self._set_marker(session_id, {
                "channel_key": result.channel_key,
                "opened_at": _now(),
                "title": session.get("title"),
            })
            logger.info("Opened topic %s for session %s", result.channel_key, session_id)
        elif result.retry_after is not None:
            logger.info(
                "Rate limited opening a topic for %s; the next turn retries", session_id,
            )
        else:
            await self._set_marker(session_id, {"error": result.error, "failed_at": _now()})
            logger.info("No topic for session %s: %s", session_id, result.error)

    async def _after_turn(self, event: TurnFinished) -> None:
        session = await self._session(event.session_id)
        if not session:
            return
        marker = _metadata(session).get(MARKER)
        if not isinstance(marker, dict):
            return
        key = await self._bound_topic(event.session_id, marker)
        if key is None:
            return
        cfg = self._cfg
        if cfg.mirror_turns and event.channel != "telegram" and event.text.strip():
            await self._channel.post_in_topic(
                key, event.session_id, mirror_text(event, cfg.mirror_max_chars),
            )
        # A title generated before the topic existed: rename once it is known.
        title = session.get("title")
        if title and title != marker.get("title"):
            await self._channel.on_session_titled(event.session_id, title)
            marker = {**marker, "title": title}
            await self._set_marker(event.session_id, marker)
        if (
            "cron_done" in cfg.close_on
            and is_one_run_cron(session)
            and not event.work_pending
        ):
            await self._close(event.session_id, "cron_done", session=session)

    async def _close(self, session_id: str, event: str, session: dict | None = None) -> None:
        session = session or await self._session(session_id)
        if not session:
            return
        marker = _metadata(session).get(MARKER)
        if not isinstance(marker, dict):
            return
        key = await self._bound_topic(session_id, marker)
        if key is None:
            return
        outcome, error = await self._channel.let_go_of_topic(
            key, session_id, _CLOSE_NOTE[event],
            delete_in_private=self._cfg.private_chat_close == "delete",
        )
        if error:
            logger.warning("Could not let go of %s for %s: %s", key, session_id, error)
            return
        logger.info("Let go of topic %s for %s on %s: %s", key, session_id, event, outcome)
        if event != "delete":  # a deleted session's row is about to go
            await self._set_marker(session_id, {
                **marker, "closed_at": _now(), "outcome": outcome, "closed_on": event,
            })
