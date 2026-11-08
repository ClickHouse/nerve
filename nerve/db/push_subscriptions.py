"""Web Push subscription data access (``push_subscriptions``, migration v068)."""

from __future__ import annotations

from datetime import datetime, timezone

#: The most browsers that one person can turn push on in.
MAX_PUSH_SUBSCRIPTIONS = 20

_SELECT = "SELECT * FROM push_subscriptions"
#: The rows that get push: not those of a disabled account. A person that
#: the gateway names in external mode has no ``accounts`` row, so their rows
#: get push. ``accounts.actor_id`` is NOT NULL, so ``NOT IN`` is safe.
_GETS_PUSH = "actor_id NOT IN (SELECT actor_id FROM accounts WHERE enabled = 0)"


class PushSubscriptionStore:
    """Mixin for the browsers that get the panel's push notifications."""

    async def save_push_subscription(
        self,
        *,
        actor_id: str,
        endpoint: str,
        p256dh: str,
        auth: str,
        level: str,
        user_agent: str,
    ) -> dict | None:
        """Add the browser at ``endpoint`` for ``actor_id``, or update its keys,
        level and user agent. A row of another person for the same endpoint
        (the same browser) is replaced. ``None`` when the row would be new and
        the person has :data:`MAX_PUSH_SUBSCRIPTIONS` rows already."""
        now = datetime.now(timezone.utc).isoformat()
        async with self._atomic():
            async with self.db.execute(
                "SELECT actor_id FROM push_subscriptions WHERE endpoint = ?", (endpoint,),
            ) as cursor:
                row = await cursor.fetchone()
            if row is None or row[0] != actor_id:
                async with self.db.execute(
                    "SELECT COUNT(*) FROM push_subscriptions WHERE actor_id = ?", (actor_id,),
                ) as cursor:
                    (count,) = await cursor.fetchone()
                if count >= MAX_PUSH_SUBSCRIPTIONS:
                    return None
                await self.db.execute("DELETE FROM push_subscriptions WHERE endpoint = ?", (endpoint,))
            await self.db.execute(
                """INSERT INTO push_subscriptions
                       (actor_id, endpoint, p256dh, auth, level, user_agent, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(endpoint) DO UPDATE SET
                       p256dh = excluded.p256dh, auth = excluded.auth,
                       level = excluded.level, user_agent = excluded.user_agent""",
                (actor_id, endpoint, p256dh, auth, level, user_agent, now),
            )
        async with self.db.execute(f"{_SELECT} WHERE endpoint = ?", (endpoint,)) as cursor:
            saved = await cursor.fetchone()
        return dict(saved) if saved else None

    async def list_push_subscriptions(self, actor_id: str | None = None) -> list[dict]:
        """The rows of one person; or, when ``actor_id`` is ``None``, the rows
        that get push (:data:`_GETS_PUSH`). Oldest first. The rows of a
        disabled account stay, so push starts again when the account is
        enabled again."""
        sql, params = (f"{_SELECT} WHERE {_GETS_PUSH} ORDER BY id", ()) if actor_id is None else (
            f"{_SELECT} WHERE actor_id = ? ORDER BY id", (actor_id,),
        )
        async with self.db.execute(sql, params) as cursor:
            return [dict(row) for row in await cursor.fetchall()]

    async def delete_push_subscription(self, subscription_id: int, actor_id: str | None = None) -> bool:
        """Delete a row; with ``actor_id``, only a row of that person. True when deleted."""
        if actor_id is None:
            result = await self._write("DELETE FROM push_subscriptions WHERE id = ?", (subscription_id,))
        else:
            result = await self._write(
                "DELETE FROM push_subscriptions WHERE id = ? AND actor_id = ?", (subscription_id, actor_id),
            )
        return result.rowcount > 0

    async def mark_push_success(self, subscription_id: int) -> None:
        await self._write(
            "UPDATE push_subscriptions SET last_success_at = ? WHERE id = ?",
            (datetime.now(timezone.utc).isoformat(), subscription_id),
        )
