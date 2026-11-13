"""What the widgets of a chat saved (``widget_states``, migration v073).

One row is one widget of one session that has saved something; see
``nerve/widget_state.py`` for what a widget and its state are, and for the
limits this store holds a chat to.
"""

from __future__ import annotations

from datetime import datetime, timezone

from nerve.widget_state import (
    MAX_CHAT_CHARS,
    MAX_CHAT_WIDGETS,
    WidgetStateError,
    decode_items,
    encode_items,
)


def _state(row) -> dict:
    return {
        "id": row["widget_id"],
        "items": decode_items(row["items"]),
        "size": row["size"],
        "updated_at": row["updated_at"],
    }


class WidgetStateStore:
    """Mixin for the saved state of widgets."""

    async def list_widget_states(self, session_id: str) -> list[dict]:
        """The widgets of a session that have saved something, the one that
        changed last first: ``id``, ``items``, ``size``, ``updated_at``."""
        async with self.db.execute(
            "SELECT widget_id, items, size, updated_at FROM widget_states "
            "WHERE session_id = ? ORDER BY updated_at DESC, widget_id",
            (session_id,),
        ) as cursor:
            rows = await cursor.fetchall()
        return [_state(row) for row in rows]

    async def save_widget_state(
        self, session_id: str, widget_id: str, items: list[list[str]], size: int,
    ) -> dict:
        """Keep ``items`` (checked, ``size`` characters) as the state of one
        widget, in the place of what it had. No items: the widget has no
        state any more, and no row.

        Raises :class:`~nerve.widget_state.WidgetStateError`: 404
        ``session_not_found``; 413 ``too_many_widgets`` when the row would be
        new and the session has :data:`MAX_CHAT_WIDGETS` already, ``chat_full``
        when the session's widgets together would keep more than
        :data:`MAX_CHAT_CHARS`. A refused write changes nothing.
        """
        now = datetime.now(timezone.utc).isoformat()
        async with self._atomic():
            async with self.db.execute(
                "SELECT 1 FROM sessions WHERE id = ?", (session_id,),
            ) as cursor:
                if await cursor.fetchone() is None:
                    raise WidgetStateError("session_not_found", 404)
            if not items:
                await self.db.execute(
                    "DELETE FROM widget_states WHERE session_id = ? AND widget_id = ?",
                    (session_id, widget_id),
                )
                return {"id": widget_id, "items": [], "size": 0, "updated_at": None}
            # What the other widgets of the session keep.
            async with self.db.execute(
                "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM widget_states "
                "WHERE session_id = ? AND widget_id != ?",
                (session_id, widget_id),
            ) as cursor:
                others, kept = await cursor.fetchone()
            if others >= MAX_CHAT_WIDGETS:
                async with self.db.execute(
                    "SELECT 1 FROM widget_states WHERE session_id = ? AND widget_id = ?",
                    (session_id, widget_id),
                ) as cursor:
                    if await cursor.fetchone() is None:
                        raise WidgetStateError("too_many_widgets", 413)
            if kept + size > MAX_CHAT_CHARS:
                raise WidgetStateError("chat_full", 413)
            await self.db.execute(
                """INSERT INTO widget_states (session_id, widget_id, items, size, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(session_id, widget_id) DO UPDATE SET
                       items = excluded.items, size = excluded.size,
                       updated_at = excluded.updated_at""",
                (session_id, widget_id, encode_items(items), size, now),
            )
        return {"id": widget_id, "items": items, "size": size, "updated_at": now}

    async def delete_widget_state(self, session_id: str, widget_id: str) -> bool:
        """Forget what one widget of a session saved. False when it had saved nothing."""
        result = await self._write(
            "DELETE FROM widget_states WHERE session_id = ? AND widget_id = ?",
            (session_id, widget_id),
        )
        return bool(result.rowcount)
