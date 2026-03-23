"""Session data access methods."""

from __future__ import annotations

import json
from datetime import datetime, timezone


class SessionStore:
    """Mixin providing session CRUD and lifecycle operations."""

    async def create_session(
        self,
        session_id: str,
        title: str | None = None,
        source: str = "web",
        metadata: dict | None = None,
        status: str = "created",
        parent_session_id: str | None = None,
        forked_from_message: str | None = None,
    ) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            """INSERT OR IGNORE INTO sessions
               (id, title, source, metadata, status, parent_session_id,
                forked_from_message, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (session_id, title or session_id, source,
             json.dumps(metadata or {}), status,
             parent_session_id, forked_from_message, now, now),
        )
        await self.db.commit()
        return {
            "id": session_id, "title": title or session_id,
            "source": source, "status": status,
            "parent_session_id": parent_session_id,
        }

    async def get_session(self, session_id: str) -> dict | None:
        async with self.db.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def list_sessions(
        self, limit: int = 50, include_archived: bool = False,
    ) -> list[dict]:
        if include_archived:
            query = "SELECT * FROM sessions ORDER BY updated_at DESC LIMIT ?"
            params = (limit,)
        else:
            query = "SELECT * FROM sessions WHERE status != 'archived' ORDER BY updated_at DESC LIMIT ?"
            params = (limit,)
        async with self.db.execute(query, params) as cursor:
            return [dict(row) async for row in cursor]

    async def search_sessions(self, query: str, limit: int = 100) -> list[dict]:
        """Search sessions by title (LIKE match), across all non-archived sessions."""
        sql = (
            "SELECT * FROM sessions "
            "WHERE title LIKE ? AND status != 'archived' "
            "ORDER BY updated_at DESC LIMIT ?"
        )
        async with self.db.execute(sql, (f"%{query}%", limit)) as cursor:
            return [dict(row) async for row in cursor]

    async def touch_session(self, session_id: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            "UPDATE sessions SET updated_at = ? WHERE id = ?", (now, session_id)
        )
        await self.db.commit()

    async def update_session_title(self, session_id: str, title: str) -> None:
        await self.db.execute(
            "UPDATE sessions SET title = ? WHERE id = ?", (title, session_id)
        )
        await self.db.commit()

    async def delete_session(self, session_id: str) -> None:
        async with self._atomic():
            await self.db.execute("DELETE FROM session_file_snapshots WHERE session_id = ?", (session_id,))
            await self.db.execute("DELETE FROM session_events WHERE session_id = ?", (session_id,))
            await self.db.execute("DELETE FROM channel_sessions WHERE session_id = ?", (session_id,))
            await self.db.execute("DELETE FROM messages WHERE session_id = ?", (session_id,))
            await self.db.execute("DELETE FROM sessions WHERE id = ?", (session_id,))

    async def update_session_metadata(self, session_id: str, metadata: dict) -> None:
        """Update the metadata JSON for a session.

        Also syncs dedicated columns (sdk_session_id, connected_at) for
        backward compatibility with callers that still use the metadata blob.
        """
        await self.db.execute(
            "UPDATE sessions SET metadata = ? WHERE id = ?",
            (json.dumps(metadata), session_id),
        )
        # Sync dedicated columns from metadata
        col_sync: dict[str, str | None] = {}
        if "sdk_session_id" in metadata:
            col_sync["sdk_session_id"] = metadata["sdk_session_id"]
        if "connected_at" in metadata:
            col_sync["connected_at"] = metadata["connected_at"]
        if col_sync:
            await self.update_session_fields(session_id, col_sync)
        await self.db.commit()

    async def update_session_fields(self, session_id: str, fields: dict) -> None:
        """Update specific session columns atomically. Merges, doesn't replace."""
        allowed = {
            "status", "sdk_session_id", "connected_at", "last_activity_at",
            "archived_at", "title", "message_count", "total_cost_usd",
            "parent_session_id", "forked_from_message", "last_memorized_at",
            "starred",
        }
        set_clauses: list[str] = []
        params: list = []
        for key, value in fields.items():
            if key in allowed:
                set_clauses.append(f"{key} = ?")
                params.append(value)
        if not set_clauses:
            return
        set_clauses.append("updated_at = ?")
        params.append(datetime.now(timezone.utc).isoformat())
        params.append(session_id)
        await self.db.execute(
            f"UPDATE sessions SET {', '.join(set_clauses)} WHERE id = ?",
            tuple(params),
        )
        await self.db.commit()

    async def get_sessions_with_metadata_key(self, key: str) -> list[dict]:
        """Find sessions whose metadata JSON contains a specific key.

        Legacy method — prefer querying dedicated columns instead.
        """
        async with self.db.execute("SELECT * FROM sessions") as cursor:
            results = []
            async for row in cursor:
                d = dict(row)
                meta = json.loads(d.get("metadata", "{}") or "{}")
                if key in meta:
                    d["_parsed_metadata"] = meta
                    results.append(d)
            return results

    async def archive_session(self, old_id: str, new_id: str) -> None:
        """Rename a session (move messages to new ID)."""
        async with self._atomic():
            await self.db.execute("UPDATE session_events SET session_id = ? WHERE session_id = ?", (new_id, old_id))
            await self.db.execute("UPDATE messages SET session_id = ? WHERE session_id = ?", (new_id, old_id))
            await self.db.execute("UPDATE sessions SET id = ? WHERE id = ?", (new_id, old_id))

    # --- Session lifecycle operations (V3) ---

    async def log_session_event(
        self, session_id: str, event_type: str, details: dict | None = None,
    ) -> int:
        """Log a session lifecycle event."""
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.execute(
            "INSERT INTO session_events (session_id, event_type, details, created_at) VALUES (?, ?, ?, ?)",
            (session_id, event_type, json.dumps(details) if details else None, now),
        ) as cursor:
            event_id = cursor.lastrowid
        await self.db.commit()
        return event_id

    async def get_session_events(
        self, session_id: str, limit: int = 50,
    ) -> list[dict]:
        """Get lifecycle events for a session, newest first."""
        async with self.db.execute(
            "SELECT * FROM session_events WHERE session_id = ? ORDER BY created_at DESC LIMIT ?",
            (session_id, limit),
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        for row in rows:
            if row.get("details"):
                try:
                    row["details"] = json.loads(row["details"])
                except (json.JSONDecodeError, TypeError):
                    pass
        return rows

    # --- Channel session mapping (V3) ---

    async def get_channel_session(self, channel_key: str) -> dict | None:
        """Get the persisted session for a channel."""
        async with self.db.execute(
            "SELECT * FROM channel_sessions WHERE channel_key = ?", (channel_key,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def set_channel_session(self, channel_key: str, session_id: str) -> None:
        """Persist a channel-to-session mapping."""
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            "INSERT OR REPLACE INTO channel_sessions (channel_key, session_id, updated_at) VALUES (?, ?, ?)",
            (channel_key, session_id, now),
        )
        await self.db.commit()

    # --- Session cleanup queries (V3) ---

    async def get_sessions_by_status(self, statuses: list[str]) -> list[dict]:
        """Find sessions with any of the given statuses."""
        placeholders = ",".join("?" for _ in statuses)
        async with self.db.execute(
            f"SELECT * FROM sessions WHERE status IN ({placeholders})",
            tuple(statuses),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_stale_sessions(
        self, before_iso: str, exclude_ids: list[str] | None = None,
    ) -> list[dict]:
        """Get idle/stopped/error sessions not updated since before_iso."""
        excludes = exclude_ids or []
        if excludes:
            placeholders = ",".join("?" for _ in excludes)
            query = f"""
                SELECT * FROM sessions
                WHERE status IN ('idle', 'stopped', 'error')
                AND updated_at < ?
                AND id NOT IN ({placeholders})
            """
            params = (before_iso, *excludes)
        else:
            query = """
                SELECT * FROM sessions
                WHERE status IN ('idle', 'stopped', 'error')
                AND updated_at < ?
            """
            params = (before_iso,)
        async with self.db.execute(query, params) as cursor:
            return [dict(row) async for row in cursor]

    async def count_active_sessions(self) -> int:
        """Count non-archived sessions."""
        async with self.db.execute(
            "SELECT COUNT(*) FROM sessions WHERE status != 'archived'"
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_oldest_sessions(
        self, count: int, exclude_ids: list[str] | None = None,
    ) -> list[dict]:
        """Get the oldest non-active, non-archived sessions for cleanup."""
        excludes = exclude_ids or []
        if excludes:
            placeholders = ",".join("?" for _ in excludes)
            query = f"""
                SELECT * FROM sessions
                WHERE status NOT IN ('active', 'archived')
                AND id NOT IN ({placeholders})
                ORDER BY updated_at ASC LIMIT ?
            """
            params = (*excludes, count)
        else:
            query = """
                SELECT * FROM sessions
                WHERE status NOT IN ('active', 'archived')
                ORDER BY updated_at ASC LIMIT ?
            """
            params = (count,)
        async with self.db.execute(query, params) as cursor:
            return [dict(row) async for row in cursor]

    async def increment_message_count(self, session_id: str) -> None:
        """Atomically increment the message counter for a session."""
        await self.db.execute(
            "UPDATE sessions SET message_count = COALESCE(message_count, 0) + 1 WHERE id = ?",
            (session_id,),
        )
        await self.db.commit()

    async def get_sessions_needing_memorization(self) -> list[dict]:
        """Find non-archived sessions that have un-memorized messages.

        Returns sessions where:
        - status is not 'archived'
        - message_count > 0
        - last_memorized_at is NULL (never memorized) OR
          messages exist with created_at > last_memorized_at

        The ``last_memorized_at`` value is normalised in the comparison to
        match SQLite's ``CURRENT_TIMESTAMP`` format (``YYYY-MM-DD HH:MM:SS``)
        so the string comparison works correctly regardless of the stored
        format (ISO 8601 with ``T``/``Z`` or plain space-separated).
        """
        async with self.db.execute("""
            SELECT s.* FROM sessions s
            WHERE s.status != 'archived'
            AND s.message_count > 0
            AND (
                s.last_memorized_at IS NULL
                OR EXISTS (
                    SELECT 1 FROM messages m
                    WHERE m.session_id = s.id
                    AND m.created_at > SUBSTR(
                        REPLACE(REPLACE(s.last_memorized_at, 'T', ' '), 'Z', ''),
                        1, 19
                    )
                )
            )
        """) as cursor:
            return [dict(row) async for row in cursor]

    async def get_last_telegram_channel_key(self) -> str | None:
        """Get the most recently updated telegram:* channel key."""
        async with self.db.execute(
            "SELECT channel_key FROM channel_sessions WHERE channel_key LIKE 'telegram:%' ORDER BY updated_at DESC LIMIT 1",
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None
