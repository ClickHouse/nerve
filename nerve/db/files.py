"""Uploaded file data access methods."""

from __future__ import annotations

import json
from datetime import datetime, timezone


class FileStore:
    """Mixin providing uploaded file CRUD operations."""

    async def save_uploaded_file(
        self,
        file_id: str,
        session_id: str,
        filename: str,
        media_type: str,
        file_type: str,
        file_size: int,
        disk_path: str,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            """INSERT INTO uploaded_files (id, session_id, filename, media_type, file_type, file_size, disk_path, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (file_id, session_id, filename, media_type, file_type, file_size, disk_path, now),
        )
        await self.db.commit()

    async def get_uploaded_file(self, file_id: str) -> dict | None:
        async with self.db.execute(
            "SELECT * FROM uploaded_files WHERE id = ?", (file_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_uploaded_files_by_ids(self, file_ids: list[str]) -> list[dict]:
        if not file_ids:
            return []
        placeholders = ",".join("?" for _ in file_ids)
        async with self.db.execute(
            f"SELECT * FROM uploaded_files WHERE id IN ({placeholders})",
            file_ids,
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def delete_uploaded_files(self, session_id: str) -> list[str]:
        """Delete all uploaded file records for a session. Returns disk paths for cleanup."""
        async with self.db.execute(
            "SELECT disk_path FROM uploaded_files WHERE session_id = ?",
            (session_id,),
        ) as cursor:
            paths = [row[0] async for row in cursor]
        await self.db.execute(
            "DELETE FROM uploaded_files WHERE session_id = ?",
            (session_id,),
        )
        await self.db.commit()
        return paths
