"""Code-review data access methods.

Backs the ``code_review`` panel: reviews own line-anchored threads, threads own
comments. IDs are generated here (short uuid hex) so callers stay simple. All
writes go through the shared ``_write`` / ``_atomic`` helpers on
:class:`nerve.db.base.Database`.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return uuid.uuid4().hex[:12]


class ReviewStore:
    """Mixin providing code-review CRUD operations."""

    # -- Reviews ------------------------------------------------------------

    async def create_review(
        self,
        *,
        repo_root: str,
        worktree: str,
        branch: str | None = None,
        base_ref: str = "HEAD",
        target_session_id: str | None = None,
        created_by: str = "human",
        title: str = "",
    ) -> dict:
        review_id = _new_id()
        await self._write(
            """INSERT INTO code_reviews
                 (id, title, repo_root, worktree, branch, base_ref,
                  target_session_id, created_by, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open')""",
            (review_id, title, repo_root, worktree, branch, base_ref,
             target_session_id, created_by),
        )
        review = await self.get_review(review_id)
        assert review is not None
        return review

    async def get_review(self, review_id: str) -> dict | None:
        async with self.db.execute(
            "SELECT * FROM code_reviews WHERE id = ?", (review_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def list_reviews(
        self, status: str | None = None, target_session_id: str | None = None, limit: int = 100,
    ) -> list[dict]:
        """List reviews (newest first) with open-thread counts, optionally
        filtered by status and/or the session they're attached to."""
        conditions = []
        params: list = []
        if status:
            conditions.append("r.status = ?")
            params.append(status)
        if target_session_id:
            conditions.append("r.target_session_id = ?")
            params.append(target_session_id)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        async with self.db.execute(
            f"""SELECT r.*,
                       (SELECT COUNT(*) FROM code_review_threads t
                         WHERE t.review_id = r.id) AS thread_count,
                       (SELECT COUNT(*) FROM code_review_threads t
                         WHERE t.review_id = r.id AND t.status = 'open') AS open_thread_count
                FROM code_reviews r
                {where}
                ORDER BY r.updated_at DESC, r.created_at DESC
                LIMIT ?""",
            tuple(params),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_review_full(self, review_id: str) -> dict | None:
        """Return the review with its threads, each carrying its comments."""
        review = await self.get_review(review_id)
        if review is None:
            return None
        threads = await self.list_threads(review_id)
        for thread in threads:
            thread["comments"] = await self.list_comments(thread["id"])
        review["threads"] = threads
        return review

    async def update_review(self, review_id: str, **fields) -> None:
        fields = {k: v for k, v in fields.items() if k in {"title", "status", "target_session_id"}}
        if not fields:
            return
        fields["updated_at"] = _now()
        sets = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values())
        vals.append(review_id)
        await self._write(f"UPDATE code_reviews SET {sets} WHERE id = ?", tuple(vals))

    async def delete_review(self, review_id: str) -> None:
        await self._write("DELETE FROM code_reviews WHERE id = ?", (review_id,))

    # -- Threads ------------------------------------------------------------

    async def add_thread(
        self,
        *,
        review_id: str,
        file_path: str,
        side: str = "new",
        line_start: int | None = None,
        line_end: int | None = None,
        anchor_snippet: str | None = None,
    ) -> dict:
        thread_id = _new_id()
        now = _now()
        async with self._atomic():
            await self.db.execute(
                """INSERT INTO code_review_threads
                     (id, review_id, file_path, side, line_start, line_end,
                      anchor_snippet, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'open')""",
                (thread_id, review_id, file_path, side, line_start, line_end, anchor_snippet),
            )
            await self.db.execute(
                "UPDATE code_reviews SET updated_at = ? WHERE id = ?", (now, review_id),
            )
        thread = await self.get_thread(thread_id)
        assert thread is not None
        return thread

    async def get_thread(self, thread_id: str) -> dict | None:
        async with self.db.execute(
            "SELECT * FROM code_review_threads WHERE id = ?", (thread_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def list_threads(self, review_id: str) -> list[dict]:
        async with self.db.execute(
            """SELECT * FROM code_review_threads
                WHERE review_id = ?
                ORDER BY created_at ASC""",
            (review_id,),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def set_thread_status(self, thread_id: str, status: str) -> None:
        await self._write(
            "UPDATE code_review_threads SET status = ?, updated_at = ? WHERE id = ?",
            (status, _now(), thread_id),
        )

    # -- Comments -----------------------------------------------------------

    async def add_comment(
        self,
        *,
        thread_id: str,
        author: str,
        body: str,
        thread_status: str | None = None,
        pending: bool = False,
    ) -> dict:
        """Append a comment, bump the thread + its review, and optionally set
        the thread status (e.g. 'answered' when the agent replies).

        ``pending=True`` stages a human *draft* comment that is NOT delivered to
        the target session until the review is submitted — see
        :meth:`list_pending_comments` and :meth:`mark_review_submitted`."""
        comment_id = _new_id()
        now = _now()
        async with self._atomic():
            await self.db.execute(
                """INSERT INTO code_review_comments (id, thread_id, author, body, pending)
                   VALUES (?, ?, ?, ?, ?)""",
                (comment_id, thread_id, author, body, 1 if pending else 0),
            )
            if thread_status is not None:
                await self.db.execute(
                    "UPDATE code_review_threads SET status = ?, updated_at = ? WHERE id = ?",
                    (thread_status, now, thread_id),
                )
            else:
                await self.db.execute(
                    "UPDATE code_review_threads SET updated_at = ? WHERE id = ?",
                    (now, thread_id),
                )
            await self.db.execute(
                """UPDATE code_reviews SET updated_at = ?
                    WHERE id = (SELECT review_id FROM code_review_threads WHERE id = ?)""",
                (now, thread_id),
            )
        async with self.db.execute(
            "SELECT * FROM code_review_comments WHERE id = ?", (comment_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else {"id": comment_id}

    async def list_comments(self, thread_id: str) -> list[dict]:
        async with self.db.execute(
            """SELECT * FROM code_review_comments
                WHERE thread_id = ?
                ORDER BY created_at ASC""",
            (thread_id,),
        ) as cursor:
            return [dict(row) async for row in cursor]

    # -- Batched review submission ------------------------------------------

    async def list_pending_comments(self, review_id: str) -> list[dict]:
        """Human draft comments not yet submitted, each with its thread anchor
        (file/side/line/snippet), ordered for a readable batch message."""
        async with self.db.execute(
            """SELECT c.id, c.thread_id, c.body, c.created_at,
                      t.file_path, t.side, t.line_start, t.line_end, t.anchor_snippet
               FROM code_review_comments c
               JOIN code_review_threads t ON c.thread_id = t.id
               WHERE t.review_id = ? AND c.pending = 1 AND c.author = 'human'
               ORDER BY t.file_path ASC, t.line_start ASC, c.created_at ASC""",
            (review_id,),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def mark_review_submitted(self, review_id: str) -> int:
        """Clear the pending flag on all of a review's staged comments.
        Returns how many comments were published."""
        result = await self._write(
            """UPDATE code_review_comments SET pending = 0
               WHERE pending = 1
                 AND thread_id IN (SELECT id FROM code_review_threads WHERE review_id = ?)""",
            (review_id,),
        )
        return result.rowcount
