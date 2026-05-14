"""Task data access methods."""

from __future__ import annotations

import re
from datetime import datetime, timezone


class TaskStore:
    """Mixin providing task CRUD, FTS search, and escalation operations."""

    async def upsert_task(
        self,
        task_id: str,
        file_path: str,
        title: str,
        status: str = "pending",
        source: str | None = None,
        source_url: str | None = None,
        deadline: str | None = None,
        tags: str = "",
        content: str = "",
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        async with self._atomic():
            await self.db.execute(
                """INSERT INTO tasks (id, file_path, title, status, source, source_url, deadline, tags, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                       file_path=excluded.file_path, title=excluded.title, status=excluded.status,
                       source=excluded.source, source_url=excluded.source_url,
                       deadline=excluded.deadline, tags=excluded.tags, updated_at=?""",
                (task_id, file_path, title, status, source, source_url, deadline, tags, now, now, now),
            )
            # Sync FTS index — include tags and slug so they're all searchable
            fts_content = f"{content} {tags.replace(',', ' ')}" if tags else content
            # Normalize slug: replace hyphens with spaces so "2026-03-10-distribution"
            # becomes searchable as individual words
            fts_slug = task_id.replace("-", " ")
            await self.db.execute("DELETE FROM tasks_fts WHERE task_id = ?", (fts_slug,))
            await self.db.execute(
                "INSERT INTO tasks_fts (task_id, title, content) VALUES (?, ?, ?)",
                (fts_slug, title, fts_content),
            )

    async def get_task(self, task_id: str) -> dict | None:
        async with self.db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def list_tasks(self, status: str | None = None, tag: str | None = None, limit: int = 100) -> list[dict]:
        conditions: list[str] = []
        params: list = []

        if status == "all":
            pass
        elif status:
            conditions.append("status = ?")
            params.append(status)
        else:
            conditions.append("status != 'done'")

        if tag:
            # Match exact tag in comma-separated list: ,tag, within ,tags,
            conditions.append("',' || tags || ',' LIKE ?")
            params.append(f"%,{tag.strip().lower()},%")

        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        params.append(limit)
        async with self.db.execute(
            f"SELECT * FROM tasks{where} ORDER BY deadline ASC NULLS LAST, created_at DESC LIMIT ?",
            tuple(params),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def update_task_status(self, task_id: str, status: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            "UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?",
            (status, now, task_id),
        )
        await self.db.commit()

    async def update_task_tags(self, task_id: str, tags: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            "UPDATE tasks SET tags = ?, updated_at = ? WHERE id = ?",
            (tags, now, task_id),
        )
        await self.db.commit()

    # ── FTS query building ───────────────────────────────────────────────

    # Characters that are FTS5 syntax or punctuation — replaced with spaces.
    _FTS_CLEAN_RE = re.compile(r'["\*\(\)\-:/\\#\.\,\;\'\[\]\{\}@!?\^~`]')

    # Common stop words filtered from search queries.
    _FTS_STOP_WORDS = frozenset({
        "a", "an", "the", "is", "at", "by", "on", "in", "to", "of", "for",
        "and", "or", "not", "it", "be", "as", "do", "if", "so", "no", "up",
        "my", "we", "he", "me",
    })

    @classmethod
    def _tokenize_query(cls, query: str) -> list[str]:
        """Clean and tokenize a raw search string into FTS-safe words."""
        clean = cls._FTS_CLEAN_RE.sub(" ", query)
        return [
            w for w in clean.split()
            if w.strip() and len(w) > 1 and w.lower() not in cls._FTS_STOP_WORDS
        ]

    @classmethod
    def _build_fts_query(cls, query: str, mode: str = "and") -> str:
        """Build an FTS5 MATCH expression with prefix matching.

        Each word is searched as both exact and prefix: (word OR word*)
        so "distrib" matches "distribution", "distributed", etc.

        Args:
            query: Raw search text.
            mode: 'and' — all terms must match (strict search).
                  'or'  — any term can match (permissive dedup).
        """
        words = cls._tokenize_query(query)
        if not words:
            return ""

        # Each term: exact OR prefix match.  FTS5 prefix syntax is word*
        terms = [f'("{w}" OR {w}*)' for w in words]

        joiner = " OR " if mode == "or" else " AND "
        return joiner.join(terms)

    # ── Status/tag filter helpers ────────────────────────────────────────

    @staticmethod
    def _apply_status_filter(
        conditions: list[str], params: list, status: str | None,
    ) -> None:
        """Append status filter clause to conditions/params in place."""
        if status == "all":
            pass
        elif status:
            conditions.append("t.status = ?")
            params.append(status)
        else:
            conditions.append("t.status != 'done'")

    @staticmethod
    def _apply_tag_filter(
        conditions: list[str], params: list, tag: str | None,
    ) -> None:
        """Append tag filter clause to conditions/params in place."""
        if tag:
            conditions.append("',' || t.tags || ',' LIKE ?")
            params.append(f"%,{tag.strip().lower()},%")

    # ── Search ───────────────────────────────────────────────────────────

    async def search_tasks(
        self, query: str, status: str | None = None, tag: str | None = None, limit: int = 20,
    ) -> list[dict]:
        """Flexible task search with multiple strategies and relevance ranking.

        Strategies (in priority order):
          1. Exact task ID match
          2. FTS5 prefix search ranked by BM25
          3. Task ID substring match (LIKE)
          4. Title substring fallback (LIKE)

        Results are merged and deduplicated — earlier strategies rank higher.
        """
        query = query.strip()
        if not query:
            return []

        seen: set[str] = set()
        results: list[dict] = []

        def _add(rows: list[dict]) -> None:
            for row in rows:
                tid = row["id"]
                if tid not in seen:
                    seen.add(tid)
                    results.append(row)

        # ── Strategy 1: exact task ID ────────────────────────────────────
        exact = await self.get_task(query)
        if exact and self._row_matches_filters(exact, status, tag):
            _add([exact])

        # ── Strategy 2: FTS5 prefix match + BM25 ranking ────────────────
        fts_query = self._build_fts_query(query)
        if fts_query:
            conditions: list[str] = []
            params: list = []
            self._apply_status_filter(conditions, params, status)
            self._apply_tag_filter(conditions, params, tag)

            where_extra = (" AND " + " AND ".join(conditions)) if conditions else ""
            fts_params = [fts_query] + params + [limit]

            async with self.db.execute(
                f"SELECT t.* FROM tasks t "
                f"JOIN tasks_fts f ON f.task_id = REPLACE(t.id, '-', ' ') "
                f"WHERE tasks_fts MATCH ?{where_extra} "
                f"ORDER BY f.rank LIMIT ?",
                tuple(fts_params),
            ) as cursor:
                _add([dict(row) async for row in cursor])

        # ── Strategy 3: task ID substring (slug search) ──────────────────
        if len(results) < limit:
            conditions = ["t.id LIKE ?"]
            params = [f"%{query.lower()}%"]
            self._apply_status_filter(conditions, params, status)
            self._apply_tag_filter(conditions, params, tag)
            remaining = limit - len(results)
            params.append(remaining)

            async with self.db.execute(
                f"SELECT t.* FROM tasks t WHERE {' AND '.join(conditions)} "
                f"ORDER BY t.updated_at DESC LIMIT ?",
                tuple(params),
            ) as cursor:
                _add([dict(row) async for row in cursor])

        # ── Strategy 4: title LIKE fallback ──────────────────────────────
        if len(results) < limit:
            conditions = ["lower(t.title) LIKE ?"]
            params = [f"%{query.lower()}%"]
            self._apply_status_filter(conditions, params, status)
            self._apply_tag_filter(conditions, params, tag)
            remaining = limit - len(results)
            params.append(remaining)

            async with self.db.execute(
                f"SELECT t.* FROM tasks t WHERE {' AND '.join(conditions)} "
                f"ORDER BY t.updated_at DESC LIMIT ?",
                tuple(params),
            ) as cursor:
                _add([dict(row) async for row in cursor])

        return results[:limit]

    @staticmethod
    def _row_matches_filters(row: dict, status: str | None, tag: str | None) -> bool:
        """Check if a single task row passes the status/tag filters."""
        if status == "all":
            pass
        elif status:
            if row.get("status") != status:
                return False
        else:
            if row.get("status") == "done":
                return False
        if tag:
            tags_csv = row.get("tags", "") or ""
            if tag.strip().lower() not in [t.strip().lower() for t in tags_csv.split(",")]:
                return False
        return True

    async def search_tasks_similar(
        self, query: str, limit: int = 10,
        rank_threshold: float | None = None,
    ) -> list[dict]:
        """Find tasks similar to query using OR semantics + FTS5 ranking.

        Unlike search_tasks (AND, strict), this uses OR (any word matches)
        and orders by BM25 relevance.  Designed for duplicate detection —
        searches all statuses including done.

        Args:
            rank_threshold: If set, only return matches with BM25 rank
                below this value (more negative = better match).  Filters
                out weak false-positives that share only generic words.
        """
        fts_query = self._build_fts_query(query, mode="or")
        if not fts_query:
            return []

        sql = (
            "SELECT t.* FROM tasks t "
            "JOIN tasks_fts f ON f.task_id = REPLACE(t.id, '-', ' ') "
            "WHERE tasks_fts MATCH ? "
        )
        params: list = [fts_query]

        if rank_threshold is not None:
            sql += "AND f.rank < ? "
            params.append(rank_threshold)

        sql += "ORDER BY f.rank LIMIT ?"
        params.append(limit)

        async with self.db.execute(sql, tuple(params)) as cursor:
            return [dict(row) async for row in cursor]

    async def find_tasks_by_source_url(
        self, source_url: str, limit: int = 10,
    ) -> list[dict]:
        """Find tasks with an exact source_url match (most reliable dedup)."""
        async with self.db.execute(
            "SELECT * FROM tasks WHERE source_url = ? ORDER BY updated_at DESC LIMIT ?",
            (source_url, limit),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def rebuild_fts(self) -> None:
        """Clear the FTS index. Caller must re-populate via upsert_task()."""
        await self.db.execute("DELETE FROM tasks_fts")
        await self.db.commit()

    async def update_task_escalation(
        self, task_id: str, level: int, reminded_at: str | None = None
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            "UPDATE tasks SET escalation_level = ?, last_reminded_at = ?, updated_at = ? WHERE id = ?",
            (level, reminded_at or now, now, task_id),
        )
        await self.db.commit()

    async def get_task_health_stats(self) -> dict:
        """Get task and FTS counts for diagnostics (replaces raw sqlite3 calls)."""
        try:
            async with self.db.execute("SELECT COUNT(*) FROM tasks") as cur:
                task_count = (await cur.fetchone())[0]
            async with self.db.execute("SELECT COUNT(*) FROM tasks_fts") as cur:
                fts_count = (await cur.fetchone())[0]
            async with self.db.execute("SELECT COUNT(*) FROM tasks WHERE status != 'done'") as cur:
                active_count = (await cur.fetchone())[0]
            async with self.db.execute("SELECT COUNT(*) FROM tasks WHERE status = 'done'") as cur:
                done_count = (await cur.fetchone())[0]
            return {
                "total": task_count,
                "active": active_count,
                "done": done_count,
                "fts_indexed": fts_count,
                "fts_ok": task_count == fts_count,
            }
        except Exception:
            return {}
