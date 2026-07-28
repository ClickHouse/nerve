"""Telegram source — pull-based update fetching via Telethon.

Uses Telegram's native update mechanism (PTS/QTS/date state) via
updates.getState() and updates.getDifference() for reliable incremental
message delivery. This is the correct way to ask Telegram "what's new
since I last checked" — no dialog iteration or message ID tracking needed.

Cursor semantics: JSON-encoded state {pts, qts, date, seq}.
On first run (cursor=None), calls getState() to establish baseline
and returns 0 records.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from nerve import paths
from nerve.sources.base import Source
from nerve.sources.models import FetchResult, SourceRecord

logger = logging.getLogger(__name__)


class TelegramSource(Source):
    """Telegram source using Telethon's updates.getDifference mechanism."""

    source_name = "telegram"

    def __init__(self, config: dict[str, Any]):
        self._config = config
        self._client = None

    def _session_path(self) -> str:
        """Where Telethon keeps the authenticated session, minus ``.session``.

        A blank ``session_path`` means unset. Blank is truthy, so without the
        strip the session would land in a file named for a space, *relative to
        the cwd* — which is how the interactive ``nerve sync telegram`` and a
        later cron run end up looking in two different places, leaving the sync
        insisting it is not authenticated with no hint as to why.
        """
        configured = str(self._config.get("session_path") or "").strip()
        return os.path.expanduser(
            configured or str(paths.nerve_path("telegram_sync"))
        )

    async def _ensure_client(self) -> bool:
        """Initialize Telethon client if not already connected."""
        if self._client is not None:
            return True

        try:
            from telethon import TelegramClient
        except ImportError:
            logger.error("Telethon not installed — cannot fetch Telegram")
            return False

        api_id = self._config.get("api_id")
        api_hash = self._config.get("api_hash")
        if not api_id or not api_hash:
            logger.warning("Telegram source: api_id/api_hash not configured")
            return False

        session_path = self._session_path()

        # Check if session file exists — Telethon requires interactive auth
        # on first use, which can't happen in a cron context.
        session_file = session_path + ".session"
        if not os.path.exists(session_file):
            logger.warning(
                "Telegram source: no session file at %s. "
                "Run `nerve sync telegram` interactively first to authenticate.",
                session_file,
            )
            return False

        self._client = TelegramClient(session_path, api_id, api_hash)
        await self._client.connect()

        if not await self._client.is_user_authorized():
            logger.warning(
                "Telegram source: session exists but not authorized. "
                "Run `nerve sync telegram` interactively to re-authenticate."
            )
            await self._client.disconnect()
            self._client = None
            return False

        logger.info("Telethon client connected for source fetch")
        return True

    async def fetch(self, cursor: str | None, limit: int = 100) -> FetchResult:
        """Fetch new messages using updates.getDifference.

        Cursor is a JSON-encoded Telegram state: {pts, qts, date, seq}.
        On first run (cursor=None): calls getState() to get current position,
        returns 0 records so the agent isn't flooded with history.
        """
        if not await self._ensure_client():
            return FetchResult(records=[], next_cursor=cursor)

        from telethon import functions, types

        exclude_chats = set(self._config.get("exclude_chats", []))

        try:
            # First run: establish baseline state
            if cursor is None:
                state = await self._client(functions.updates.GetStateRequest())
                new_cursor = _encode_state(state)
                logger.info(
                    "Telegram baseline established: pts=%d, qts=%d, date=%s, seq=%d",
                    state.pts, state.qts, state.date, state.seq,
                )
                return FetchResult(records=[], next_cursor=new_cursor, has_more=False)

            # Parse stored state
            saved = _decode_state(cursor)
            pts = saved["pts"]
            qts = saved["qts"]
            date = datetime.fromisoformat(saved["date"])

            # Ask Telegram: "what's new since this state?"
            diff = await self._client(functions.updates.GetDifferenceRequest(
                pts=pts,
                date=date,
                qts=qts,
                pts_total_limit=limit,
            ))

            # Handle the different response types
            if isinstance(diff, types.updates.DifferenceEmpty):
                # Nothing new — update date only
                new_state = {**saved, "date": diff.date.isoformat(), "seq": diff.seq}
                return FetchResult(
                    records=[],
                    next_cursor=json.dumps(new_state),
                    has_more=False,
                )

            if isinstance(diff, types.updates.DifferenceTooLong):
                # Gap too large — reset to the pts Telegram suggests
                logger.warning("Telegram difference too long, resetting to pts=%d", diff.pts)
                new_state = {**saved, "pts": diff.pts}
                return FetchResult(
                    records=[],
                    next_cursor=json.dumps(new_state),
                    has_more=False,
                )

            # Difference or DifferenceSlice — contains actual messages
            if isinstance(diff, types.updates.DifferenceSlice):
                new_state_obj = diff.intermediate_state
                has_more = True
            else:
                # types.updates.Difference — final batch
                new_state_obj = diff.state
                has_more = False

            # Build lookup maps for users and chats
            users_map = {u.id: u for u in diff.users}
            chats_map = {c.id: c for c in diff.chats}

            # Convert new messages to SourceRecords
            records: list[SourceRecord] = []
            for msg in diff.new_messages:
                # Skip non-text messages
                if not getattr(msg, "message", None):
                    continue

                # Resolve chat info
                chat_id = msg.peer_id
                chat_title, resolved_chat_id = _resolve_chat(chat_id, chats_map, users_map)

                # Skip excluded chats
                if resolved_chat_id in exclude_chats:
                    continue

                # Resolve sender name
                sender_name = _resolve_sender(msg, users_map)

                text_preview = msg.message[:80].replace("\n", " ")
                ts = msg.date.isoformat() if msg.date else datetime.now(timezone.utc).isoformat()

                records.append(SourceRecord(
                    id=f"{resolved_chat_id}:{msg.id}",
                    source="telegram",
                    record_type="telegram_message",
                    summary=f"[{chat_title}] {sender_name}: {text_preview}",
                    content=msg.message,
                    timestamp=ts,
                    metadata={
                        "chat_id": resolved_chat_id,
                        "chat_title": chat_title,
                        "sender_id": getattr(msg, "from_id", None) and msg.from_id.user_id
                            if hasattr(getattr(msg, "from_id", None) or object(), "user_id") else None,
                        "sender_name": sender_name,
                        "message_id": msg.id,
                    },
                ))

                if len(records) >= limit:
                    break

            new_cursor = _encode_state(new_state_obj)
            logger.info(
                "Telegram getDifference: %d messages, %d records after filtering, has_more=%s",
                len(diff.new_messages), len(records), has_more,
            )
            return FetchResult(records=records, next_cursor=new_cursor, has_more=has_more)

        except Exception as e:
            logger.error("Telegram fetch error: %s", e, exc_info=True)
            return FetchResult(records=[], next_cursor=cursor, has_more=False)

    async def close(self) -> None:
        if self._client:
            await self._client.disconnect()
            self._client = None


def _encode_state(state) -> str:
    """Encode a Telegram updates.State into a JSON cursor string."""
    return json.dumps({
        "pts": state.pts,
        "qts": state.qts,
        "date": state.date.isoformat() if hasattr(state.date, "isoformat") else str(state.date),
        "seq": state.seq,
    })


def _decode_state(cursor: str) -> dict:
    """Decode a JSON cursor string into state components."""
    return json.loads(cursor)


def _resolve_chat(peer_id, chats_map: dict, users_map: dict) -> tuple[str, int]:
    """Resolve a peer_id to (chat_title, chat_id)."""
    from telethon.tl import types as tl_types

    if isinstance(peer_id, tl_types.PeerUser):
        user = users_map.get(peer_id.user_id)
        if user:
            name = getattr(user, "first_name", "") or ""
            last = getattr(user, "last_name", "") or ""
            title = f"{name} {last}".strip() or "DM"
        else:
            title = "DM"
        return title, peer_id.user_id

    if isinstance(peer_id, tl_types.PeerChat):
        chat = chats_map.get(peer_id.chat_id)
        title = getattr(chat, "title", "Group") if chat else "Group"
        return title, peer_id.chat_id

    if isinstance(peer_id, tl_types.PeerChannel):
        channel = chats_map.get(peer_id.channel_id)
        title = getattr(channel, "title", "Channel") if channel else "Channel"
        return title, peer_id.channel_id

    return "Unknown", 0


def _resolve_sender(msg, users_map: dict) -> str:
    """Resolve sender name from a message's from_id."""
    from_id = getattr(msg, "from_id", None)
    if from_id is None:
        return ""

    from telethon.tl import types as tl_types

    if isinstance(from_id, tl_types.PeerUser):
        user = users_map.get(from_id.user_id)
        if user:
            name = getattr(user, "first_name", "") or ""
            last = getattr(user, "last_name", "") or ""
            return f"{name} {last}".strip()

    return ""
