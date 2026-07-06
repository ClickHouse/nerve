"""Task status definition data access.

This manages the *configurable set* of statuses a task may hold — the
rows of the ``task_statuses`` table — which is distinct from a task's
*current* status value (stored on ``tasks.status`` and managed by
:class:`nerve.db.tasks.TaskStore`).
"""

from __future__ import annotations

import random
import re
from datetime import datetime, timezone

# Status names are slug-like so they're safe to store on tasks.status and
# render without escaping: lowercase alphanumerics + underscores, starting
# with an alphanumeric.
STATUS_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_]*$")

# Statuses with special semantics — protected from deletion (see migration
# v030 for the rationale). Kept here so handlers/routes share one source.
DEFAULT_STATUS = "pending"   # initial status assigned to new tasks
TERMINAL_STATUS = "done"     # task_done moves file to done/, hidden by default

# Curated palette for "random by default" colors — pleasant, distinct hues
# that read well on both light and dark themes.
_COLOR_PALETTE = [
    "#ef4444", "#f97316", "#f59e0b", "#eab308", "#84cc16", "#22c55e",
    "#10b981", "#14b8a6", "#06b6d4", "#3b82f6", "#6366f1", "#8b5cf6",
    "#a855f7", "#d946ef", "#ec4899", "#f43f5e",
]

_HEX_RE = re.compile(r"^#[0-9a-fA-F]{6}$")


def random_status_color() -> str:
    """Return a random color from the curated palette."""
    return random.choice(_COLOR_PALETTE)


def normalize_color(color: str | None) -> str:
    """Normalize a color to ``#rrggbb`` form, or pick a random one.

    Accepts ``#rgb``/``#rrggbb`` (with or without leading ``#``). Falls
    back to a random palette color when the input is empty or invalid so
    the UI always has a renderable hex value.
    """
    if not color:
        return random_status_color()
    c = color.strip()
    if not c.startswith("#"):
        c = "#" + c
    # Expand shorthand #rgb -> #rrggbb
    if re.fullmatch(r"#[0-9a-fA-F]{3}", c):
        c = "#" + "".join(ch * 2 for ch in c[1:])
    if not _HEX_RE.match(c):
        return random_status_color()
    return c.lower()


class TaskStatusStore:
    """Mixin providing CRUD for the configurable ``task_statuses`` table."""

    async def list_task_statuses(self) -> list[dict]:
        """Return all status definitions ordered by sort_order then name."""
        async with self.db.execute(
            "SELECT * FROM task_statuses ORDER BY sort_order ASC, name ASC"
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_task_status_def(self, name: str) -> dict | None:
        async with self.db.execute(
            "SELECT * FROM task_statuses WHERE name = ?", (name,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def task_status_names(self) -> list[str]:
        """Return the set of valid status names (ordered)."""
        async with self.db.execute(
            "SELECT name FROM task_statuses ORDER BY sort_order ASC, name ASC"
        ) as cursor:
            return [row[0] async for row in cursor]

    async def create_task_status(
        self,
        name: str,
        label: str,
        color: str | None = None,
        description: str = "",
        is_system: int = 0,
    ) -> dict:
        """Insert a new status definition. Caller validates name uniqueness."""
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.execute(
            "SELECT COALESCE(MAX(sort_order), -1) FROM task_statuses"
        ) as cur:
            next_order = (await cur.fetchone())[0] + 1
        async with self._atomic():
            await self.db.execute(
                """INSERT INTO task_statuses
                       (name, label, color, description, is_system, sort_order, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (name, label, normalize_color(color), description,
                 1 if is_system else 0, next_order, now),
            )
        return await self.get_task_status_def(name)  # type: ignore[return-value]

    async def update_task_status_def(
        self,
        name: str,
        *,
        label: str | None = None,
        color: str | None = None,
        description: str | None = None,
        sort_order: int | None = None,
    ) -> None:
        """Patch mutable fields of a status definition (never the name)."""
        sets: list[str] = []
        params: list = []
        if label is not None:
            sets.append("label = ?")
            params.append(label)
        if color is not None:
            sets.append("color = ?")
            params.append(normalize_color(color))
        if description is not None:
            sets.append("description = ?")
            params.append(description)
        if sort_order is not None:
            sets.append("sort_order = ?")
            params.append(sort_order)
        if not sets:
            return
        params.append(name)
        await self._write(
            f"UPDATE task_statuses SET {', '.join(sets)} WHERE name = ?",
            tuple(params),
        )

    async def delete_task_status_def(self, name: str) -> None:
        """Delete a status definition. Caller enforces protection rules."""
        await self._write("DELETE FROM task_statuses WHERE name = ?", (name,))

    async def count_tasks_with_status(self, name: str) -> int:
        """Count tasks currently set to the given status."""
        async with self.db.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = ?", (name,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0
