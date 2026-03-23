"""Notification data access methods."""

from __future__ import annotations

import json
from datetime import datetime, timezone


class NotificationStore:
    """Mixin providing notification CRUD operations."""

    async def create_notification(
        self,
        notification_id: str,
        session_id: str,
        type: str,
        title: str,
        body: str = "",
        priority: str = "normal",
        options: list[str] | None = None,
        expires_at: str | None = None,
        metadata: dict | None = None,
    ) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            """INSERT INTO notifications
               (id, session_id, type, title, body, priority, options, expires_at, metadata, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (notification_id, session_id, type, title, body, priority,
             json.dumps(options) if options else None,
             expires_at, json.dumps(metadata or {}), now),
        )
        await self.db.commit()
        return {"id": notification_id, "session_id": session_id, "type": type}

    async def get_notification(self, notification_id: str) -> dict | None:
        async with self.db.execute(
            """SELECT n.*, s.title AS session_title
               FROM notifications n
               LEFT JOIN sessions s ON n.session_id = s.id
               WHERE n.id = ?""",
            (notification_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def list_notifications(
        self, status: str | None = None, type: str | None = None,
        session_id: str | None = None, limit: int = 50,
        channel: str | None = None,
    ) -> list[dict]:
        conditions = []
        params: list = []
        if status:
            conditions.append("n.status = ?")
            params.append(status)
        if type:
            conditions.append("n.type = ?")
            params.append(type)
        if session_id:
            conditions.append("n.session_id = ?")
            params.append(session_id)
        if channel:
            # Filter: only show notifications delivered to this channel
            # channels_delivered is JSON like '["telegram"]' or '["web","telegram"]'
            conditions.append("(n.channels_delivered IS NULL OR n.channels_delivered LIKE ?)")
            params.append(f'%"{channel}"%')
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        async with self.db.execute(
            f"""SELECT n.*, s.title AS session_title
                FROM notifications n
                LEFT JOIN sessions s ON n.session_id = s.id
                {where}
                ORDER BY n.created_at DESC LIMIT ?""",
            tuple(params),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def answer_notification(
        self, notification_id: str, answer: str, answered_by: str,
    ) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        async with self._atomic():
            async with self.db.execute(
                "SELECT id FROM notifications WHERE id = ? AND status = 'pending'",
                (notification_id,),
            ) as cursor:
                if not await cursor.fetchone():
                    return False
            await self.db.execute(
                """UPDATE notifications
                   SET answer = ?, answered_by = ?, answered_at = ?, status = 'answered'
                   WHERE id = ?""",
                (answer, answered_by, now, notification_id),
            )
        return True

    async def dismiss_notification(self, notification_id: str) -> bool:
        async with self._atomic():
            async with self.db.execute(
                "SELECT id FROM notifications WHERE id = ? AND status = 'pending'",
                (notification_id,),
            ) as cursor:
                if not await cursor.fetchone():
                    return False
            await self.db.execute(
                "UPDATE notifications SET status = 'dismissed' WHERE id = ?",
                (notification_id,),
            )
        return True

    async def dismiss_all_notifications(self) -> int:
        """Dismiss all pending non-question notifications. Returns count dismissed."""
        cursor = await self.db.execute(
            "UPDATE notifications SET status = 'dismissed' WHERE status = 'pending' AND type = 'notify'",
        )
        await self.db.commit()
        return cursor.rowcount

    async def expire_notifications(self) -> int:
        now = datetime.now(timezone.utc).isoformat()
        cursor = await self.db.execute(
            """UPDATE notifications SET status = 'expired'
               WHERE status = 'pending' AND expires_at IS NOT NULL AND expires_at < ?""",
            (now,),
        )
        await self.db.commit()
        return cursor.rowcount

    async def count_pending_notifications(self, channel: str | None = None) -> int:
        sql = "SELECT COUNT(*) FROM notifications WHERE status = 'pending'"
        params: tuple = ()
        if channel:
            sql += ' AND (channels_delivered IS NULL OR channels_delivered LIKE ?)'
            params = (f'%"{channel}"%',)
        async with self.db.execute(sql, params) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def update_notification(self, notification_id: str, **fields) -> None:
        if not fields:
            return
        sets = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values())
        vals.append(notification_id)
        await self.db.execute(
            f"UPDATE notifications SET {sets} WHERE id = ?", tuple(vals),
        )
        await self.db.commit()
