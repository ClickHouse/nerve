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
        options: list | None = None,
        expires_at: str | None = None,
        metadata: dict | None = None,
        target_kind: str | None = None,
        target_id: str | None = None,
        status: str | None = None,
    ) -> dict:
        """Insert a notification row.

        ``type`` is one of ``notify`` (fire-and-forget), ``question``
        (ask_user / answer-injection), or ``approval`` (action-dispatch
        via the handler registry). ``target_kind`` and ``target_id`` are
        only populated for ``approval`` rows; left NULL otherwise so the
        legacy answer path stays untouched.

        ``status`` defaults to ``None`` → ``'pending'`` (identical to the
        column default, so existing callers are unchanged). The silence
        path passes ``status='silenced'`` to insert a suppressed row that
        is persisted for audit but never fanned out to channels.
        """
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            """INSERT INTO notifications
               (id, session_id, type, title, body, priority, status, options,
                expires_at, metadata, created_at, target_kind, target_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (notification_id, session_id, type, title, body, priority,
             status or "pending",
             json.dumps(options) if options else None,
             expires_at, json.dumps(metadata or {}), now,
             target_kind, target_id),
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

    async def snooze_notification(
        self, notification_id: str, new_expires_at: str,
    ) -> bool:
        """Push a pending notification's expiry forward.

        Used by the ``approval`` dispatcher when the user picks
        ``snooze_24h``: the row stays at status=pending so a later
        re-delivery tick (wired in PR 2) can surface it again, but the
        expiry advances so it does not get caught by ``expire_stale``
        in the meantime.

        Returns True on success, False if the row is not pending.
        """
        async with self._atomic():
            async with self.db.execute(
                "SELECT id FROM notifications WHERE id = ? AND status = 'pending'",
                (notification_id,),
            ) as cursor:
                if not await cursor.fetchone():
                    return False
            await self.db.execute(
                "UPDATE notifications SET expires_at = ? WHERE id = ?",
                (new_expires_at, notification_id),
            )
        return True

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

    # ------------------------------------------------------------------ #
    #  Notification silences (deterministic suppression rules)             #
    # ------------------------------------------------------------------ #

    async def create_silence(
        self,
        silence_id: str,
        pattern: str,
        reason: str = "",
        action: str = "silence",
        created_by: str = "",
        expires_at: str | None = None,
    ) -> dict:
        """Insert a silence rule.

        ``pattern`` is a case-insensitive regex the notification service
        matches against ``title + "\\n" + body``. ``expires_at`` NULL =
        permanent. Returns the freshly-inserted row.
        """
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            """INSERT INTO notification_silences
               (id, pattern, action, reason, created_by, created_at, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (silence_id, pattern, action, reason, created_by, now, expires_at),
        )
        await self.db.commit()
        row = await self.get_silence(silence_id)
        return row or {"id": silence_id, "pattern": pattern}

    async def get_silence(self, silence_id: str) -> dict | None:
        async with self.db.execute(
            "SELECT * FROM notification_silences WHERE id = ?", (silence_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def list_silences(self, include_disabled: bool = False) -> list[dict]:
        """Return silence rules, newest first.

        ``include_disabled=False`` (default) returns only enabled rules;
        ``True`` returns every row regardless of the ``enabled`` flag.
        """
        where = "" if include_disabled else "WHERE enabled = 1"
        async with self.db.execute(
            f"SELECT * FROM notification_silences {where} "
            "ORDER BY created_at DESC",
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_active_silences(self) -> list[dict]:
        """Return enabled, non-expired silence rules, oldest first.

        Oldest-first ordering gives the service a stable "first rule wins"
        precedence. Used to (re)build the in-memory matcher cache.
        """
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.execute(
            """SELECT * FROM notification_silences
               WHERE enabled = 1 AND (expires_at IS NULL OR expires_at > ?)
               ORDER BY created_at ASC""",
            (now,),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def delete_silence(self, silence_id: str) -> bool:
        """Hard-delete a silence rule. Returns False if it didn't exist."""
        async with self._atomic():
            async with self.db.execute(
                "SELECT id FROM notification_silences WHERE id = ?",
                (silence_id,),
            ) as cursor:
                if not await cursor.fetchone():
                    return False
            await self.db.execute(
                "DELETE FROM notification_silences WHERE id = ?", (silence_id,),
            )
        return True

    async def record_silence_hit(self, silence_id: str) -> int:
        """Bump ``hit_count`` + stamp ``last_hit_at``; return the new count.

        Called every time a silence suppresses a delivery, so the user can
        see how often a rule is firing.
        """
        now = datetime.now(timezone.utc).isoformat()
        async with self._atomic():
            await self.db.execute(
                """UPDATE notification_silences
                   SET hit_count = hit_count + 1, last_hit_at = ?
                   WHERE id = ?""",
                (now, silence_id),
            )
            async with self.db.execute(
                "SELECT hit_count FROM notification_silences WHERE id = ?",
                (silence_id,),
            ) as cursor:
                row = await cursor.fetchone()
        return row[0] if row else 0

    async def record_silence_override(self, silence_id: str) -> int:
        """Bump ``override_count`` + stamp ``last_override_at``; return count.

        Called when an agent force-sends a notification over a matching
        rule. A climbing override count is a false-match signal: the
        pattern is catching alerts that genuinely need to reach the user.
        """
        now = datetime.now(timezone.utc).isoformat()
        async with self._atomic():
            await self.db.execute(
                """UPDATE notification_silences
                   SET override_count = override_count + 1,
                       last_override_at = ?
                   WHERE id = ?""",
                (now, silence_id),
            )
            async with self.db.execute(
                "SELECT override_count FROM notification_silences WHERE id = ?",
                (silence_id,),
            ) as cursor:
                row = await cursor.fetchone()
        return row[0] if row else 0
