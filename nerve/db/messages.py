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
        external_id: str | None = None,
        created_at: str | None = None,
    ) -> int:
        """Insert a message row. ``external_id`` enables idempotent ingest
        from external sources (Codex thread sync, MCP server).

        ``created_at`` lets external ingesters preserve original Codex
        timestamps. Defaults to ``CURRENT_TIMESTAMP`` for native callers.
        """
        async with self._atomic():
            if created_at is not None:
                async with self.db.execute(
                    """INSERT INTO messages
                         (session_id, role, content, thinking, blocks, channel,
                          external_id, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (session_id, role, content, thinking,
                     json.dumps(blocks) if blocks else None,
                     channel, external_id, created_at),
                ) as cursor:
                    msg_id = cursor.lastrowid
            else:
                async with self.db.execute(
                    """INSERT INTO messages
                         (session_id, role, content, thinking, blocks, channel, external_id)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (session_id, role, content, thinking,
                     json.dumps(blocks) if blocks else None,
                     channel, external_id),
                ) as cursor:
                    msg_id = cursor.lastrowid
            # Update session timestamp and message counter
            now = datetime.now(timezone.utc).isoformat()
            await self.db.execute(
                "UPDATE sessions SET updated_at = ?, message_count = COALESCE(message_count, 0) + 1 WHERE id = ?",
                (now, session_id),
            )
        return msg_id

    async def add_message_idempotent(
        self,
        session_id: str,
        role: str,
        content: str,
        external_id: str,
        channel: str | None = None,
        thinking: str | None = None,
        blocks: list | None = None,
        created_at: str | None = None,
    ) -> int | None:
        """Insert a message keyed on ``(session_id, external_id)``.

        Returns the new message id, or ``None`` if a row with the same
        ``external_id`` already exists for the session (no-op).

        Relies on the partial unique index added in v028 — callers MUST
        pass a non-empty ``external_id``. Use :meth:`add_message` for
        native inserts where idempotency isn't needed.
        """
        if not external_id:
            raise ValueError("add_message_idempotent requires non-empty external_id")
        async with self._atomic():
            ts = created_at or datetime.now(timezone.utc).isoformat()
            async with self.db.execute(
                """INSERT OR IGNORE INTO messages
                     (session_id, role, content, thinking, blocks, channel,
                      external_id, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (session_id, role, content, thinking,
                 json.dumps(blocks) if blocks else None,
                 channel, external_id, ts),
            ) as cursor:
                msg_id = cursor.lastrowid
                # rowcount==0 on IGNORE-skipped insert; lastrowid still
                # holds the previous insert's id, so we can't rely on it.
                if cursor.rowcount == 0:
                    return None
            now = datetime.now(timezone.utc).isoformat()
            await self.db.execute(
                "UPDATE sessions SET updated_at = ?, message_count = COALESCE(message_count, 0) + 1 WHERE id = ?",
                (now, session_id),
            )
        return msg_id

    async def message_exists_by_external_id(
        self, session_id: str, external_id: str,
    ) -> bool:
        """Check if a message with the given external_id already exists.

        Cheaper than attempting an insert when you only need a yes/no.
        """
        async with self.db.execute(
            "SELECT 1 FROM messages WHERE session_id = ? AND external_id = ? LIMIT 1",
            (session_id, external_id),
        ) as cursor:
            row = await cursor.fetchone()
            return row is not None

    async def merge_tool_result_into_call(
        self,
        session_id: str,
        tool_use_id: str,
        result: str | list | dict,
        is_error: bool,
    ) -> int | None:
        """Find the tool_call message for ``tool_use_id`` and attach the
        result + is_error fields to its block.

        The Nerve UI renders tool calls and results in a single combined
        ``tool_call`` block — call inputs + result text live side by side.
        External ingest paths see them as separate Codex events, so the
        ingester needs to fold the second event into the first message.

        Returns the message ``id`` that was updated, or ``None`` if no
        matching tool_call message exists (in which case the caller can
        fall back to inserting a tool-result-only row).
        """
        external_id = f"tool_call:{tool_use_id}"
        async with self.db.execute(
            "SELECT id, blocks FROM messages WHERE session_id = ? AND external_id = ? LIMIT 1",
            (session_id, external_id),
        ) as cursor:
            row = await cursor.fetchone()
        if not row:
            return None
        msg_id, blocks_json = row["id"], row["blocks"]
        if not blocks_json:
            return None
        try:
            blocks = json.loads(blocks_json)
        except (TypeError, json.JSONDecodeError):
            return None
        updated = False
        for b in blocks:
            if not isinstance(b, dict):
                continue
            if b.get("type") == "tool_call" and b.get("tool_use_id") == tool_use_id:
                b["result"] = result
                b["is_error"] = bool(is_error)
                updated = True
                break
        if not updated:
            return None
        await self._write(
            "UPDATE messages SET blocks = ? WHERE id = ?",
            (json.dumps(blocks), msg_id),
        )
        return msg_id

    async def merge_workflow_into_call(
        self,
        session_id: str,
        tool_use_id: str,
        workflow: dict,
    ) -> int | None:
        """Attach a dynamic-workflow progress snapshot to its ``Workflow``
        ``tool_call`` block so it survives reload.

        A workflow runs in the background and can settle *after* the turn
        that launched it was already persisted, so the snapshot is folded
        into the stored block out-of-band (keyed by ``tool_use_id``). Unlike
        :meth:`merge_tool_result_into_call`, normal Nerve turns store every
        block inside a single assistant message with no per-block
        ``external_id`` — so we locate the row by scanning recent messages
        whose ``blocks`` JSON mentions the id.

        Returns the message ``id`` that was updated, or ``None`` if no
        matching tool_call block exists.
        """
        async with self.db.execute(
            "SELECT id, blocks FROM messages "
            "WHERE session_id = ? AND blocks LIKE ? "
            "ORDER BY id DESC LIMIT 10",
            (session_id, f"%{tool_use_id}%"),
        ) as cursor:
            rows = await cursor.fetchall()
        for row in rows:
            blocks_json = row["blocks"]
            if not blocks_json:
                continue
            try:
                blocks = json.loads(blocks_json)
            except (TypeError, json.JSONDecodeError):
                continue
            updated = False
            for b in blocks:
                if not isinstance(b, dict):
                    continue
                if b.get("type") == "tool_call" and b.get("tool_use_id") == tool_use_id:
                    b["workflow"] = workflow
                    updated = True
                    break
            if updated:
                await self._write(
                    "UPDATE messages SET blocks = ? WHERE id = ?",
                    (json.dumps(blocks), row["id"]),
                )
                return row["id"]
        return None

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
        await self._write(
            """INSERT OR IGNORE INTO session_file_snapshots
               (session_id, file_path, original_content, created_at)
               VALUES (?, ?, ?, ?)""",
            (session_id, file_path, content, now),
        )

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
        await self._write(
            "DELETE FROM session_file_snapshots WHERE session_id = ?",
            (session_id,),
        )

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
