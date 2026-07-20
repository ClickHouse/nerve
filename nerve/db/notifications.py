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
        await self._write(
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
        result = await self._write(
            "UPDATE notifications SET status = 'dismissed' WHERE status = 'pending' AND type = 'notify'",
        )
        return result.rowcount

    async def expire_due_notifications(self) -> list[dict]:
        """Flip pending rows past their expiry to ``expired``.

        Returns the affected rows (as they were *before* the flip, with
        ``status`` already rewritten to ``'expired'`` in the returned
        dicts) so the service layer can report each expiry — inject a
        note into the asking session, audit-log approvals, gray the web
        card, edit the Telegram message. Select-then-update runs inside
        one transaction so a concurrent answer can't slip between the
        two statements.
        """
        now = datetime.now(timezone.utc).isoformat()
        async with self._atomic():
            async with self.db.execute(
                """SELECT * FROM notifications
                   WHERE status = 'pending'
                     AND expires_at IS NOT NULL AND expires_at < ?""",
                (now,),
            ) as cursor:
                rows = [dict(row) async for row in cursor]
            if rows:
                placeholders = ",".join("?" for _ in rows)
                await self.db.execute(
                    f"""UPDATE notifications SET status = 'expired'
                        WHERE id IN ({placeholders})""",
                    tuple(r["id"] for r in rows),
                )
        for r in rows:
            r["status"] = "expired"
        return rows

    async def expire_notification(self, notification_id: str) -> bool:
        """Flip a single pending row to ``expired``.

        Used by the re-delivery tick when a row hits the
        ``max_redeliveries`` cap: instead of another fanout, it expires
        (with reporting) even though ``expires_at`` may still be in the
        future. Returns False if the row is not pending.
        """
        async with self._atomic():
            async with self.db.execute(
                "SELECT id FROM notifications WHERE id = ? AND status = 'pending'",
                (notification_id,),
            ) as cursor:
                if not await cursor.fetchone():
                    return False
            await self.db.execute(
                "UPDATE notifications SET status = 'expired' WHERE id = ?",
                (notification_id,),
            )
        return True

    async def expire_pending_questions_for_session(self, session_id: str) -> int:
        """Expire a session's pending ``question`` notifications.

        Called when an idle session is auto-archived so the user isn't left
        with a phantom pending question on a closed session (and the periodic
        expiry pass has nothing to inject into the now-archived session).
        Returns the number of questions expired.
        """
        async with self._atomic():
            async with self.db.execute(
                """SELECT id FROM notifications
                   WHERE session_id = ? AND status = 'pending' AND type = 'question'""",
                (session_id,),
            ) as cursor:
                ids = [row[0] async for row in cursor]
            if ids:
                ph = ",".join("?" for _ in ids)
                await self.db.execute(
                    f"UPDATE notifications SET status = 'expired' WHERE id IN ({ph})",
                    tuple(ids),
                )
        return len(ids)

    async def snooze_notification(
        self, notification_id: str, redeliver_at: str, new_expires_at: str,
    ) -> bool:
        """Queue a pending notification for re-delivery.

        Used when the user picks ``snooze_24h`` on an approval: the row
        stays at status=pending, ``redeliver_at`` marks when the
        periodic maintenance tick should fan it out again (fresh
        Telegram card + web broadcast), and ``expires_at`` advances past
        the re-delivery time so the row cannot expire before it
        resurfaces. Re-snoozing after a re-delivery simply sets
        ``redeliver_at`` again — each snooze buys another cycle, up to
        ``config.notifications.max_redeliveries``.

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
                """UPDATE notifications SET redeliver_at = ?, expires_at = ?
                   WHERE id = ?""",
                (redeliver_at, new_expires_at, notification_id),
            )
        return True

    async def get_due_redeliveries(self) -> list[dict]:
        """Return pending rows whose ``redeliver_at`` has passed.

        Oldest-first so long-waiting rows resurface before fresher ones
        when several come due in the same sweep.
        """
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.execute(
            """SELECT * FROM notifications
               WHERE status = 'pending'
                 AND redeliver_at IS NOT NULL AND redeliver_at <= ?
               ORDER BY created_at ASC""",
            (now,),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def mark_notification_redelivered(
        self, notification_id: str, new_expires_at: str | None = None,
    ) -> bool:
        """Record one re-delivery: bump the count, clear ``redeliver_at``.

        ``new_expires_at`` (when given) restarts the expiry window so
        the fresh card gets a full answering window — without it, a row
        whose original expiry already passed would be expired by the
        very next pass of the same sweep that just re-delivered it.
        Returns False if the row is not pending.
        """
        async with self._atomic():
            async with self.db.execute(
                "SELECT id FROM notifications WHERE id = ? AND status = 'pending'",
                (notification_id,),
            ) as cursor:
                if not await cursor.fetchone():
                    return False
            if new_expires_at is not None:
                await self.db.execute(
                    """UPDATE notifications
                       SET redelivery_count = redelivery_count + 1,
                           redeliver_at = NULL, expires_at = ?
                       WHERE id = ?""",
                    (new_expires_at, notification_id),
                )
            else:
                await self.db.execute(
                    """UPDATE notifications
                       SET redelivery_count = redelivery_count + 1,
                           redeliver_at = NULL
                       WHERE id = ?""",
                    (notification_id,),
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
        await self._write(
            f"UPDATE notifications SET {sets} WHERE id = ?", tuple(vals),
        )

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
        await self._write(
            """INSERT INTO notification_silences
               (id, pattern, action, reason, created_by, created_at, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (silence_id, pattern, action, reason, created_by, now, expires_at),
        )
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
