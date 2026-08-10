"""Task data access methods."""

from __future__ import annotations

import re
from datetime import datetime, timezone


# Spacing between task ranks (``tasks.position``). Wide enough that a
# midpoint insert has ~50 levels of headroom before float precision runs
# out, so renormalization is a theoretical path rather than a routine one.
POSITION_GAP = 1024.0

# Two neighbours closer than this can't yield a meaningful midpoint, so a
# move between them re-spaces the lane first.
_MIN_POSITION_GAP = 1e-6


class _Keep:
    """Type of the :data:`KEEP` sentinel."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "KEEP"


# Default for the preserve-on-omit columns of :meth:`TaskStore.upsert_task`.
# A separate sentinel is necessary because ``None`` is a real value there: it
# clears the column.
KEEP = _Keep()


def _resolve_kept(value, stored: dict | None, column: str, default):
    """Resolve one preserve-on-omit argument against the stored row.

    A stored NULL gives ``default``. A row written before its column existed
    can hold NULL, and ``tags`` must stay a ``str``.
    """
    if value is not KEEP:
        return value
    if stored is None:
        return default
    kept = stored[column]
    return default if kept is None else kept


class TaskStore:
    """Mixin providing task CRUD, FTS search, and escalation operations."""

    async def upsert_task(
        self,
        task_id: str,
        file_path: str,
        title: str,
        status: str = "pending",
        source: str | None | _Keep = KEEP,
        source_url: str | None | _Keep = KEEP,
        deadline: str | None | _Keep = KEEP,
        tags: str | _Keep = KEEP,
        content: str = "",
        position: float | None = None,
        actor: str = "system",
    ) -> None:
        """Insert or update a task row and its FTS entry.

        ``file_path``, ``title``, ``status`` and ``content`` are a full
        replace. Every caller rebuilds them from the markdown file.

        ``source``, ``source_url``, ``deadline``, ``tags`` and ``position``
        are preserve-on-omit. An omitted column keeps its stored value. To
        clear one, pass it: ``None`` for the nullable columns, ``""`` for
        ``tags``.

        These five columns need that rule because a markdown file does not
        always carry them. No file carries ``position``: board order lives
        only in this column. A file carries the other four only while it has
        the frontmatter for them. Under a full replace, every caller must
        read the row and pass all five back, and a caller that forgets one
        deletes user data. That happened twice. ``TaskManager.reindex``
        dropped ``tags`` after v018 added the column, and ``task_done``
        nulled the other four when it moved a file into done/.
        ``test_task_position.py``, ``test_task_reindex.py`` and
        ``test_task_completion.py`` hold the regression coverage.
        """
        now = datetime.now(timezone.utc).isoformat()
        async with self._atomic():
            # One read serves every column this method resolves before the
            # write. The statement below stays a plain full replace, and the
            # FTS text sees the tags that actually land in the row.
            stored = await self._stored_task_columns(task_id)
            source = _resolve_kept(source, stored, "source", None)
            source_url = _resolve_kept(source_url, stored, "source_url", None)
            deadline = _resolve_kept(deadline, stored, "deadline", None)
            tags = _resolve_kept(tags, stored, "tags", "")
            # A full-row upsert is one of the paths that can change status
            # (task_update with a note, the PATCH route saving content), so
            # it has to record transitions like the dedicated methods do.
            previous_status = stored["status"] if stored else None
            if position is None:
                position = await self._resolve_upsert_position(stored, status)
            await self.db.execute(
                """INSERT INTO tasks (id, file_path, title, status, source, source_url, deadline, tags, position, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                       file_path=excluded.file_path, title=excluded.title, status=excluded.status,
                       source=excluded.source, source_url=excluded.source_url,
                       deadline=excluded.deadline, tags=excluded.tags,
                       position=excluded.position, updated_at=?""",
                (task_id, file_path, title, status, source, source_url, deadline, tags, position, now, now, now),
            )
            # Sync FTS index — include tags and content so they're all searchable.
            # The join key is the RAW task_id; readers join on `f.task_id = t.id`.
            # The FTS5 tokenizer already splits the hyphenated slug into words
            # ("2026-03-10-distribution" → 2026, 03, 10, distribution), so slug
            # search works without rewriting the key — and the key stays joinable.
            await self._record_status_event(task_id, previous_status, status, actor)

            fts_content = f"{content} {tags.replace(',', ' ')}" if tags else content
            await self.db.execute("DELETE FROM tasks_fts WHERE task_id = ?", (task_id,))
            await self.db.execute(
                "INSERT INTO tasks_fts (task_id, title, content) VALUES (?, ?, ?)",
                (task_id, title, fts_content),
            )

    async def get_task(self, task_id: str) -> dict | None:
        async with self.db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    # ── Status history (task_events) ─────────────────────────────────────

    async def _record_status_event(
        self,
        task_id: str,
        from_status: str | None,
        to_status: str,
        actor: str,
    ) -> None:
        """Append a transition to ``task_events``, if it is one.

        Called from inside ``_atomic()`` by every path that can change a
        task's status, so the event and the status commit together — there
        is no window in which a task has moved but its history hasn't.

        A no-op transition writes nothing, and that one rule is what keeps
        ``TaskManager.reindex()`` from flooding the table: reindex rewrites
        every row with the status it already had, so from == to and nothing
        is recorded. The single case where reindex *does* change a status —
        resetting an orphaned row to match its directory — is a real
        correction and worth a row.
        """
        if from_status == to_status:
            return
        await self.db.execute(
            "INSERT INTO task_events (task_id, from_status, to_status, actor, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                task_id, from_status, to_status, actor or "system",
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    async def list_task_events(self, task_id: str, limit: int = 200) -> list[dict]:
        """A task's transitions, oldest first — the order a timeline reads."""
        async with self.db.execute(
            "SELECT * FROM task_events WHERE task_id = ? "
            "ORDER BY created_at ASC, id ASC LIMIT ?",
            (task_id, limit),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_status_entry_times(self, task_ids: list[str]) -> dict[str, str]:
        """When each task last *entered* its current status.

        The basis for card aging. Takes the newest event per task and keeps
        it only if it still matches the live status: a row whose status was
        last changed by a path predating this table, or by a direct DB edit,
        has no truthful entry time. Omitting it leaves the indicator silent,
        which beats inventing an age that reads as fact.
        """
        if not task_ids:
            return {}
        placeholders = ",".join("?" * len(task_ids))
        async with self.db.execute(
            f"SELECT task_id, to_status, MAX(created_at) FROM task_events "
            f"WHERE task_id IN ({placeholders}) GROUP BY task_id",
            tuple(task_ids),
        ) as cursor:
            newest = {row[0]: (row[1], row[2]) async for row in cursor}

        async with self.db.execute(
            f"SELECT id, status FROM tasks WHERE id IN ({placeholders})",
            tuple(task_ids),
        ) as cursor:
            live = {row[0]: row[1] async for row in cursor}

        return {
            task_id: entered_at
            for task_id, (to_status, entered_at) in newest.items()
            if live.get(task_id) == to_status
        }

    # ── Board ordering (tasks.position) ──────────────────────────────────
    #
    # Lanes sort by ``position ASC``, so a *lower* rank is *higher* in the
    # lane. Every helper below assumes it is running inside ``_atomic()``:
    # they read on the shared connection between the caller's writes, which
    # is only consistent while the caller holds the write lock.

    async def _lane_top_position(self, status: str) -> float:
        """A rank that lands above everything currently in ``status``."""
        async with self.db.execute(
            "SELECT MIN(position) FROM tasks WHERE status = ?", (status,),
        ) as cursor:
            lane_min = (await cursor.fetchone())[0]
        # Going negative is fine: this is an ordering key, not a count.
        return POSITION_GAP if lane_min is None else float(lane_min) - POSITION_GAP

    async def _stored_task_columns(self, task_id: str) -> dict | None:
        """The columns an upsert can preserve, or ``None`` for a new task."""
        async with self.db.execute(
            "SELECT status, position, source, source_url, deadline, tags "
            "FROM tasks WHERE id = ?",
            (task_id,),
        ) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row else None

    async def _resolve_upsert_position(
        self, stored: dict | None, status: str,
    ) -> float:
        """Rank for an upsert that supplied none: keep it, or mint one.

        A rank is preserved only *within* a lane. A row whose status is
        changing gets re-ranked to the top of its destination, because a
        rank carried across lanes lands the card at an arbitrary depth of a
        list it was never ordered against — the same rule
        :meth:`update_task_status` applies, kept here so it also holds for
        the callers that flip status through a full-row upsert instead
        (``task_update`` with a note, the PATCH route saving content).
        """
        if stored is not None and stored["status"] == status:
            return float(stored["position"])
        # Brand new task, or one changing lanes — surface it at the top,
        # where whoever just created or moved it is looking.
        return await self._lane_top_position(status)

    async def _neighbour_position(
        self, neighbour_id: str | None, lane: str, moving_id: str,
    ) -> float | None:
        """Rank of an anchor card, or None if it can't anchor this move.

        An anchor is ignored when it is missing, is the moved card itself,
        or has since left the lane — all of which happen routinely when a
        drag lands against a board the client rendered moments ago.
        """
        if not neighbour_id or neighbour_id == moving_id:
            return None
        async with self.db.execute(
            "SELECT position FROM tasks WHERE id = ? AND status = ?",
            (neighbour_id, lane),
        ) as cursor:
            row = await cursor.fetchone()
        return float(row[0]) if row else None

    async def _renormalize_lane(self, lane: str) -> None:
        """Re-space a lane at ``POSITION_GAP`` intervals, order preserved."""
        async with self.db.execute(
            "SELECT id FROM tasks WHERE status = ? "
            "ORDER BY position ASC, created_at DESC, id DESC",
            (lane,),
        ) as cursor:
            ids = [row[0] async for row in cursor]
        if ids:
            await self.db.executemany(
                "UPDATE tasks SET position = ? WHERE id = ?",
                [((i + 1) * POSITION_GAP, tid) for i, tid in enumerate(ids)],
            )

    async def _compute_move_position(
        self, task_id: str, lane: str,
        before_id: str | None, after_id: str | None,
    ) -> float:
        """Resolve "between these two cards" into a concrete rank."""
        for attempt in (0, 1):
            above = await self._neighbour_position(before_id, lane, task_id)
            below = await self._neighbour_position(after_id, lane, task_id)

            if above is not None and below is not None:
                gap = below - above
                if gap >= _MIN_POSITION_GAP:
                    return (above + below) / 2.0
                if gap >= 0 and attempt == 0:
                    # Ranks have converged after many midpoint inserts.
                    # Re-space once, then take a real midpoint on the retry.
                    await self._renormalize_lane(lane)
                    continue
                # gap < 0 means the anchors arrived swapped; re-spacing
                # preserves order so it can't help. Fall through to the tail.
            elif above is not None:
                return above + POSITION_GAP
            elif below is not None:
                return below - POSITION_GAP
            break

        # No usable anchor: append to the end of the lane.
        async with self.db.execute(
            "SELECT MAX(position) FROM tasks WHERE status = ? AND id != ?",
            (lane, task_id),
        ) as cursor:
            tail = (await cursor.fetchone())[0]
        return POSITION_GAP if tail is None else float(tail) + POSITION_GAP

    async def move_task(
        self,
        task_id: str,
        *,
        status: str | None = None,
        before_id: str | None = None,
        after_id: str | None = None,
        actor: str = "system",
    ) -> dict | None:
        """Place a task at an explicit slot in a lane; return the new row.

        The client sends *intent* — "put this between A and B" — instead of
        a computed rank. Lanes are paginated, so the client may not be
        holding the neighbours' true ranks, and resolving them server-side
        means two people dragging at once converge on a sane order rather
        than overwriting each other with stale absolute values.

        ``before_id`` is the card that ends up directly **above** the moved
        task, ``after_id`` the one directly **below**; either may be None
        for "top of lane" / "bottom of lane". Returns None if the task does
        not exist.

        Note this writes ``status`` directly rather than going through
        :meth:`update_task_status` — a move already carries an explicit
        rank, so it must not be re-ranked to the top of the lane on top of
        it. Callers are responsible for any *file* movement a status change
        implies (see ``task_done`` / ``task_reopen``).
        """
        async with self._atomic():
            async with self.db.execute(
                "SELECT status FROM tasks WHERE id = ?", (task_id,),
            ) as cursor:
                row = await cursor.fetchone()
            if row is None:
                return None

            lane = status or row[0]
            position = await self._compute_move_position(
                task_id, lane, before_id, after_id,
            )
            now = datetime.now(timezone.utc).isoformat()
            await self.db.execute(
                "UPDATE tasks SET status = ?, position = ?, updated_at = ? WHERE id = ?",
                (lane, position, now, task_id),
            )
            # A pure reorder leaves status alone, and _record_status_event
            # drops the no-op — so dragging within a lane doesn't pollute
            # the history with rows that say nothing changed.
            await self._record_status_event(task_id, row[0], lane, actor)
            async with self.db.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,),
            ) as cursor:
                moved = await cursor.fetchone()
            return dict(moved) if moved else None

    # Supported sort keys → ORDER BY clause. Keep deterministic with a
    # secondary key so equal timestamps don't flicker between pages.
    _SORT_CLAUSES = {
        "deadline": "deadline ASC NULLS LAST, created_at DESC, id DESC",
        "updated_at": "updated_at DESC, id DESC",
        "created_at": "created_at DESC, id DESC",
        # Board order. Rows predating v043's backfill (or written by a
        # caller that never set a rank) share position 0, so the tiebreak
        # keeps them in the list view's familiar newest-first order.
        "position": "position ASC, created_at DESC, id DESC",
    }

    async def list_tasks(
        self,
        status: str | None = None,
        tag: str | None = None,
        limit: int = 100,
        offset: int = 0,
        sort: str = "deadline",
    ) -> list[dict]:
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
        order_clause = self._SORT_CLAUSES.get(sort, self._SORT_CLAUSES["deadline"])
        params.extend([limit, offset])
        async with self.db.execute(
            f"SELECT * FROM tasks{where} ORDER BY {order_clause} LIMIT ? OFFSET ?",
            tuple(params),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def count_tasks(
        self,
        status: str | None = None,
        tag: str | list[str] | None = None,
    ) -> int:
        """Count tasks matching the given filters (without limit/offset).

        ``tag`` may be a single tag or a list of tags. A list is OR-matched
        (a task counts if it carries *any* of the tags) within one COUNT(*),
        so each matching task is counted at most once.
        """
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
            tags = [tag] if isinstance(tag, str) else list(tag)
            tag_clauses: list[str] = []
            for t in tags:
                t = str(t).strip().lower()
                if not t:
                    continue
                tag_clauses.append("',' || tags || ',' LIKE ?")
                params.append(f"%,{t},%")
            if tag_clauses:
                conditions.append("(" + " OR ".join(tag_clauses) + ")")

        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        async with self.db.execute(
            f"SELECT COUNT(*) FROM tasks{where}", tuple(params),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def update_task_status(
        self, task_id: str, status: str, actor: str = "system",
    ) -> None:
        """Move a task to another status, re-ranking it into the new lane.

        A rank only means anything relative to its own lane, so carrying
        the old one across a status change would drop the card at an
        arbitrary depth of its destination — the agent marking something
        in_progress would land it in the middle of the lane rather than
        somewhere a person would look. Same-lane calls skip the re-rank so
        a redundant update doesn't reshuffle the board.
        """
        now = datetime.now(timezone.utc).isoformat()
        async with self._atomic():
            async with self.db.execute(
                "SELECT status FROM tasks WHERE id = ?", (task_id,),
            ) as cursor:
                row = await cursor.fetchone()
            if row is None:
                return
            if row[0] == status:
                await self.db.execute(
                    "UPDATE tasks SET updated_at = ? WHERE id = ?", (now, task_id),
                )
                return
            position = await self._lane_top_position(status)
            await self.db.execute(
                "UPDATE tasks SET status = ?, position = ?, updated_at = ? WHERE id = ?",
                (status, position, now, task_id),
            )
            await self._record_status_event(task_id, row[0], status, actor)

    async def update_task_tags(self, task_id: str, tags: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self._write(
            "UPDATE tasks SET tags = ?, updated_at = ? WHERE id = ?",
            (tags, now, task_id),
        )

    async def count_tasks_by_status(self) -> dict[str, int]:
        """Task counts keyed by status — one query for every board lane."""
        async with self.db.execute(
            "SELECT status, COUNT(*) FROM tasks GROUP BY status",
        ) as cursor:
            return {row[0]: row[1] async for row in cursor}

    async def distinct_task_tags(self, include_done: bool = False) -> list[dict]:
        """Every distinct tag with its task count, most-used first.

        ``tags`` is a comma-separated TEXT column, so the split happens in
        Python rather than in a recursive CTE: the table is small (hundreds
        of rows), and the SQL version would be both slower to run and much
        harder to read for no benefit at this scale.
        """
        where = "" if include_done else " WHERE status != 'done'"
        counts: dict[str, int] = {}
        async with self.db.execute(f"SELECT tags FROM tasks{where}") as cursor:
            async for row in cursor:
                for raw in (row[0] or "").split(","):
                    tag = raw.strip().lower()
                    if tag:
                        counts[tag] = counts.get(tag, 0) + 1
        return [
            {"name": name, "count": count}
            for name, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
        ]

    # ── FTS query building ───────────────────────────────────────────────

    # Anything that is NOT a word character (unicode letters, digits,
    # underscore) is FTS5 syntax or punctuation — replaced with spaces.
    # Allowlist rather than blocklist: the old punctuation blocklist
    # missed characters like `$`, `+`, `=`, `&`, `%`, which slipped into
    # the MATCH expression as barewords and raised `fts5: syntax error`
    # (e.g. any query containing a dollar amount).  Every char `\w`
    # keeps is legal in an unquoted FTS5 bareword: ASCII alphanumerics,
    # underscore, and codepoints > 127.
    _FTS_CLEAN_RE = re.compile(r"[^\w]")

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
                f"JOIN tasks_fts f ON f.task_id = t.id "
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
            "JOIN tasks_fts f ON f.task_id = t.id "
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
        await self._write("DELETE FROM tasks_fts")

    async def update_task_escalation(
        self, task_id: str, level: int, reminded_at: str | None = None
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self._write(
            "UPDATE tasks SET escalation_level = ?, last_reminded_at = ?, updated_at = ? WHERE id = ?",
            (level, reminded_at or now, now, task_id),
        )

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
