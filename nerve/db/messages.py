"""Message data access methods."""

from __future__ import annotations

import json
from datetime import datetime, timezone


class MessageStore:
    """Mixin providing message CRUD and file snapshot operations."""

    async def add_message(
        self,
        session_id: str,
        role: str,
        content: str,
        channel: str | None = None,
        thinking: str | None = None,
        blocks: list | None = None,
    ) -> int:
        async with self._atomic():
            async with self.db.execute(
                """INSERT INTO messages (session_id, role, content, thinking, blocks, channel)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (session_id, role, content, thinking,
                 json.dumps(blocks) if blocks else None,
                 channel),
            ) as cursor:
                msg_id = cursor.lastrowid
            # Update session timestamp and message counter
            now = datetime.now(timezone.utc).isoformat()
            await self.db.execute(
                "UPDATE sessions SET updated_at = ?, message_count = COALESCE(message_count, 0) + 1 WHERE id = ?",
                (now, session_id),
            )
        return msg_id

    async def get_messages(
        self, session_id: str, limit: int = 500, offset: int = 0
    ) -> list[dict]:
        async with self.db.execute(
            "SELECT * FROM (SELECT * FROM messages WHERE session_id = ? ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?) ORDER BY created_at ASC, id ASC",
            (session_id, limit, offset),
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        for row in rows:
            if row.get("blocks"):
                row["blocks"] = json.loads(row["blocks"])
        return rows

    # --- File snapshot operations ---

    async def save_file_snapshot(
        self, session_id: str, file_path: str, content: str | None,
    ) -> None:
        """Save original file content before agent modification.

        Uses INSERT OR IGNORE so only the first touch per session+file is stored.
        """
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            """INSERT OR IGNORE INTO session_file_snapshots
               (session_id, file_path, original_content, created_at)
               VALUES (?, ?, ?, ?)""",
            (session_id, file_path, content, now),
        )
        await self.db.commit()

    async def get_file_snapshot(
        self, session_id: str, file_path: str,
    ) -> dict | None:
        """Retrieve original file snapshot for a specific file."""
        async with self.db.execute(
            "SELECT * FROM session_file_snapshots WHERE session_id = ? AND file_path = ?",
            (session_id, file_path),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_session_snapshots(self, session_id: str) -> list[dict]:
        """Get all file snapshots for a session."""
        async with self.db.execute(
            "SELECT session_id, file_path, created_at FROM session_file_snapshots "
            "WHERE session_id = ? ORDER BY created_at ASC",
            (session_id,),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def delete_session_snapshots(self, session_id: str) -> None:
        """Delete all file snapshots for a session."""
        await self.db.execute(
            "DELETE FROM session_file_snapshots WHERE session_id = ?",
            (session_id,),
        )
        await self.db.commit()

    async def count_messages(self, session_id: str) -> int:
        async with self.db.execute(
            "SELECT COUNT(*) FROM messages WHERE session_id = ?", (session_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_last_user_message_time(self) -> str | None:
        """Get the timestamp of the most recent user message across non-system sessions."""
        async with self.db.execute(
            """SELECT MAX(m.created_at) FROM messages m
               JOIN sessions s ON m.session_id = s.id
               WHERE m.role = 'user'
               AND s.id NOT LIKE 'cron:%'
               AND s.id NOT LIKE 'hb:%'
               AND s.id NOT LIKE 'hook:%'""",
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] else None
