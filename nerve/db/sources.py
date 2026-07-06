"""Source messages, sync cursors, consumer cursors, and source run log data access."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone


class SourceStore:
    """Mixin providing source inbox, sync cursors, consumer cursors, and run log operations."""

    # --- Sync cursor operations ---

    async def get_sync_cursor(self, source: str) -> str | None:
        async with self.db.execute(
            "SELECT cursor FROM sync_cursors WHERE source = ?", (source,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

    async def set_sync_cursor(self, source: str, cursor_value: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self.db.execute(
            "INSERT OR REPLACE INTO sync_cursors (source, cursor, updated_at) VALUES (?, ?, ?)",
            (source, cursor_value, now),
        )
        await self.db.commit()

    # --- Source run log operations ---

    async def log_source_run(
        self,
        source: str,
        records_fetched: int = 0,
        records_processed: int = 0,
        error: str | None = None,
        session_id: str | None = None,
    ) -> int:
        """Log a source run with stats."""
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.execute(
            "INSERT INTO source_run_log (source, ran_at, records_fetched, records_processed, error, session_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (source, now, records_fetched, records_processed, error, session_id),
        ) as cursor:
            log_id = cursor.lastrowid
        await self.db.commit()
        return log_id

    async def get_last_source_run(self, source: str) -> dict | None:
        """Get the most recent source run entry."""
        async with self.db.execute(
            "SELECT * FROM source_run_log WHERE source = ? ORDER BY ran_at DESC LIMIT 1",
            (source,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_source_run_stats(self, source: str, limit: int = 10) -> list[dict]:
        """Get recent source runs for diagnostics."""
        async with self.db.execute(
            "SELECT * FROM source_run_log WHERE source = ? ORDER BY ran_at DESC LIMIT ?",
            (source, limit),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_source_stats(self, hours: int = 24) -> dict[str, dict]:
        """Aggregate source_run_log stats per source for the last N hours."""
        async with self.db.execute(
            "SELECT source, COUNT(*) as runs, "
            "SUM(records_fetched) as fetched, SUM(records_processed) as processed, "
            "SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) as errors, "
            "MAX(ran_at) as last_run_at "
            "FROM source_run_log WHERE ran_at > datetime('now', ? || ' hours') "
            "GROUP BY source",
            (f"-{hours}",),
        ) as cursor:
            result = {}
            async for row in cursor:
                result[row[0]] = {
                    "runs": row[1],
                    "fetched": row[2] or 0,
                    "processed": row[3] or 0,
                    "errors": row[4] or 0,
                    "last_run_at": row[5],
                }
            return result

    async def get_source_run_log(
        self,
        source: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """Get source run history with session_id for linking."""
        if source:
            query = "SELECT * FROM source_run_log WHERE source = ? ORDER BY ran_at DESC LIMIT ?"
            params = (source, limit)
        else:
            query = "SELECT * FROM source_run_log ORDER BY ran_at DESC LIMIT ?"
            params = (limit,)
        async with self.db.execute(query, params) as cursor:
            return [dict(row) async for row in cursor]

    # --- Source messages inbox ---

    async def insert_source_messages(
        self,
        records: list,
        source: str,
        ttl_days: int = 7,
    ) -> int:
        """Bulk insert source records into the inbox. Returns count inserted.

        If a record with the same (source, id) already exists but has different
        metadata or content, the old record is deleted and re-inserted at a
        strictly-higher rowid. This ensures mutable sources (e.g. GitHub
        notifications whose thread gains a new comment, or whose ``reason``
        field changes from "author" to "mention") surface as new messages for
        consumer cursors that already read the old version.
        """
        import logging
        logger = logging.getLogger(__name__)
        now = datetime.now(timezone.utc)
        expires = (now + timedelta(days=ttl_days)).isoformat()
        now_iso = now.isoformat()
        inserted = 0
        async with self._atomic():
            for r in records:
                try:
                    new_metadata = json.dumps(r.metadata) if r.metadata else None

                    # Check if this record already exists
                    async with self.db.execute(
                        "SELECT metadata, content FROM source_messages "
                        "WHERE source = ? AND id = ?",
                        (source, r.id),
                    ) as cursor:
                        existing = await cursor.fetchone()

                    forced_rowid: int | None = None
                    if existing:
                        old_metadata, old_content = existing[0], existing[1]
                        if old_metadata == new_metadata and old_content == r.content:
                            # Nothing changed — skip silently
                            continue
                        # Metadata or content changed. Re-surface the update to
                        # consumer cursors (which poll `rowid > cursor_seq`) by
                        # re-inserting at a strictly-higher rowid.
                        #
                        # source_messages has PRIMARY KEY (source, id) and no
                        # AUTOINCREMENT, so a plain re-INSERT lands at the implicit
                        # rowid MAX(rowid)+1. If the row being replaced is itself
                        # the current MAX, deleting it first lowers the max and the
                        # re-insert REUSES the same rowid, leaving it <= a cursor
                        # already parked there, so the update is silently never
                        # re-delivered. Capture MAX(rowid)+1 BEFORE the delete
                        # (while the old row still counts toward the max) and insert
                        # at that explicit rowid so it is always above every prior
                        # rowid and every consumer cursor.
                        async with self.db.execute(
                            "SELECT COALESCE(MAX(rowid), 0) + 1 FROM source_messages"
                        ) as cursor:
                            forced_rowid = (await cursor.fetchone())[0]
                        await self.db.execute(
                            "DELETE FROM source_messages WHERE source = ? AND id = ?",
                            (source, r.id),
                        )
                        logger.info(
                            "Source message %s/%s updated (content/metadata changed): "
                            "re-inserting at rowid %s to re-surface for consumers",
                            source, r.id, forced_rowid,
                        )

                    if forced_rowid is not None:
                        await self.db.execute(
                            "INSERT INTO source_messages "
                            "(rowid, id, source, record_type, summary, content, raw_content, timestamp, metadata, created_at, expires_at) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (forced_rowid, r.id, source, r.record_type, r.summary, r.content,
                             getattr(r, 'raw_content', None),
                             r.timestamp, new_metadata,
                             now_iso, expires),
                        )
                    else:
                        await self.db.execute(
                            "INSERT INTO source_messages "
                            "(id, source, record_type, summary, content, raw_content, timestamp, metadata, created_at, expires_at) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (r.id, source, r.record_type, r.summary, r.content,
                             getattr(r, 'raw_content', None),
                             r.timestamp, new_metadata,
                             now_iso, expires),
                        )
                    inserted += 1
                except Exception as e:
                    logger.warning("Failed to insert source message %s: %s", r.id, e)
        return inserted

    async def update_source_messages_processed(
        self,
        source: str,
        ids: list[str],
        processed_map: dict[str, str],
    ) -> None:
        """Set processed_content on messages after condensation."""
        async with self._atomic():
            for msg_id in ids:
                content = processed_map.get(msg_id)
                if content is not None:
                    await self.db.execute(
                        "UPDATE source_messages SET processed_content = ? WHERE source = ? AND id = ?",
                        (content, source, msg_id),
                    )

    async def update_source_messages_session(
        self,
        source: str,
        ids: list[str],
        session_id: str,
    ) -> None:
        """Link messages to the cron session that processed them."""
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        await self.db.execute(
            f"UPDATE source_messages SET run_session_id = ? WHERE source = ? AND id IN ({placeholders})",
            (session_id, source, *ids),
        )
        await self.db.commit()

    # Normalize ISO 8601 timestamps for consistent sorting across sources.
    # Different sources use different suffixes: "+00:00", "Z", or none.
    # This expression strips the tz suffix so pure lexicographic ORDER BY works.
    _TS_SORT = "REPLACE(REPLACE(timestamp, '+00:00', ''), 'Z', '')"

    async def list_source_messages(
        self,
        source: str | None = None,
        limit: int = 50,
        before_ts: str | None = None,
        run_session_id: str | None = None,
    ) -> list[dict]:
        """Paginated list of source messages, newest first.

        Excludes processed_content for performance (use get_source_message for full detail).
        """
        conditions: list[str] = []
        params: list = []
        if source:
            conditions.append("source = ?")
            params.append(source)
        if run_session_id:
            conditions.append("run_session_id = ?")
            params.append(run_session_id)
        if before_ts:
            # Normalize the pagination cursor the same way as the sort key
            norm_before = before_ts.replace("+00:00", "").replace("Z", "")
            conditions.append(f"{self._TS_SORT} < ?")
            params.append(norm_before)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        params.append(limit + 1)  # fetch one extra to detect has_more
        async with self.db.execute(
            f"SELECT id, source, record_type, summary, timestamp, run_session_id, created_at "
            f"FROM source_messages {where} ORDER BY {self._TS_SORT} DESC LIMIT ?",
            tuple(params),
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        has_more = len(rows) > limit
        return rows[:limit], has_more

    async def get_source_message(self, source: str, msg_id: str) -> dict | None:
        """Get a single source message with full content and metadata."""
        async with self.db.execute(
            "SELECT * FROM source_messages WHERE source = ? AND id = ?",
            (source, msg_id),
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            d = dict(row)
            if d.get("metadata"):
                try:
                    d["metadata"] = json.loads(d["metadata"])
                except (json.JSONDecodeError, TypeError):
                    pass
            return d

    async def get_source_message_counts(self) -> dict[str, int]:
        """Get message counts per source."""
        async with self.db.execute(
            "SELECT source, COUNT(*) as cnt FROM source_messages GROUP BY source"
        ) as cursor:
            return {row[0]: row[1] async for row in cursor}

    async def get_source_messages_storage(self) -> dict[str, dict]:
        """Get storage stats per source: count and estimated bytes."""
        async with self.db.execute(
            "SELECT source, COUNT(*) as cnt, "
            "SUM(LENGTH(content) + COALESCE(LENGTH(processed_content), 0) + COALESCE(LENGTH(raw_content), 0)) as bytes "
            "FROM source_messages GROUP BY source"
        ) as cursor:
            result = {}
            async for row in cursor:
                result[row[0]] = {"count": row[1], "bytes": row[2] or 0}
            return result

    async def delete_source_messages(self, source: str | None = None) -> int:
        """Purge source messages. If source is None, purge all. Returns count deleted."""
        if source:
            async with self.db.execute(
                "SELECT COUNT(*) FROM source_messages WHERE source = ?", (source,)
            ) as cursor:
                count = (await cursor.fetchone())[0]
            await self.db.execute("DELETE FROM source_messages WHERE source = ?", (source,))
        else:
            async with self.db.execute("SELECT COUNT(*) FROM source_messages") as cursor:
                count = (await cursor.fetchone())[0]
            await self.db.execute("DELETE FROM source_messages")
        await self.db.commit()
        return count

    async def cleanup_expired_messages(self) -> int:
        """Delete source messages past their TTL. Returns count deleted."""
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.execute(
            "SELECT COUNT(*) FROM source_messages WHERE expires_at < ?", (now,)
        ) as cursor:
            count = (await cursor.fetchone())[0]
        if count > 0:
            await self.db.execute(
                "DELETE FROM source_messages WHERE expires_at < ?", (now,)
            )
            await self.db.commit()
        return count

    # --- Consumer cursors ---

    async def get_source_max_rowid(self, source: str) -> int:
        """Get current MAX(rowid) for a source. Returns 0 if no messages."""
        async with self.db.execute(
            "SELECT COALESCE(MAX(rowid), 0) FROM source_messages WHERE source = ?",
            (source,),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_consumer_cursor(self, consumer: str, source: str) -> int:
        """Get cursor position for a consumer+source pair.

        If no cursor exists or it has expired, initializes to current
        MAX(rowid) for the source. New consumers see only future messages.
        """
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.execute(
            "SELECT cursor_seq, expires_at FROM consumer_cursors WHERE consumer = ? AND source = ?",
            (consumer, source),
        ) as cursor:
            row = await cursor.fetchone()

        if row is not None:
            expires = row[1]
            if expires is None or expires > now:
                return row[0]
            # Expired — fall through to re-initialize

        # No cursor or expired: initialize to latest
        max_seq = await self.get_source_max_rowid(source)
        await self.set_consumer_cursor(consumer, source, max_seq)
        return max_seq

    async def set_consumer_cursor(
        self,
        consumer: str,
        source: str,
        cursor_seq: int,
        ttl_days: int = 2,
        session_id: str | None = None,
    ) -> None:
        """Advance cursor and refresh TTL. Links to session_id for UI tracking."""
        now = datetime.now(timezone.utc)
        expires = (now + timedelta(days=ttl_days)).isoformat()
        await self.db.execute(
            """INSERT INTO consumer_cursors (consumer, source, cursor_seq, session_id, updated_at, expires_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(consumer, source) DO UPDATE SET
                   cursor_seq=excluded.cursor_seq, session_id=COALESCE(excluded.session_id, consumer_cursors.session_id),
                   updated_at=excluded.updated_at, expires_at=excluded.expires_at""",
            (consumer, source, cursor_seq, session_id, now.isoformat(), expires),
        )
        await self.db.commit()

    async def list_consumer_cursors(self, consumer: str | None = None) -> list[dict]:
        """List active (non-expired) consumer cursors with unread counts."""
        now = datetime.now(timezone.utc).isoformat()
        conditions = ["(expires_at IS NULL OR expires_at > ?)"]
        params: list = [now]
        if consumer:
            conditions.append("consumer = ?")
            params.append(consumer)
        where = " AND ".join(conditions)

        async with self.db.execute(
            f"""SELECT cc.consumer, cc.source, cc.cursor_seq, cc.session_id,
                       cc.updated_at, cc.expires_at,
                       (SELECT COUNT(*) FROM source_messages sm
                        WHERE sm.source = cc.source AND sm.rowid > cc.cursor_seq) as unread
                FROM consumer_cursors cc
                WHERE {where}
                ORDER BY cc.consumer, cc.source""",
            tuple(params),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def read_source_messages_by_rowid(
        self,
        source: str,
        after_seq: int,
        limit: int = 50,
    ) -> list[dict]:
        """Read messages from one source with rowid > after_seq.

        Returns dicts with 'rowid' included for cursor advancement.
        Uses processed_content if available, falls back to content.
        """
        async with self.db.execute(
            """SELECT rowid, id, source, record_type, summary,
                      COALESCE(processed_content, content) as content,
                      timestamp, metadata, run_session_id, created_at
               FROM source_messages
               WHERE source = ? AND rowid > ?
               ORDER BY rowid ASC
               LIMIT ?""",
            (source, after_seq, limit),
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        for row in rows:
            if row.get("metadata"):
                try:
                    row["metadata"] = json.loads(row["metadata"])
                except (json.JSONDecodeError, TypeError):
                    pass
        return rows

    async def browse_source_messages(
        self,
        source: str,
        limit: int = 50,
        before_seq: int | None = None,
        after_seq: int | None = None,
    ) -> list[dict]:
        """Browse historical messages with manual cursor. No consumer state modified.

        before_seq: messages with rowid < X (paginate backwards, newest first)
        after_seq: messages with rowid > X (paginate forwards, oldest first)
        Neither: return most recent messages (newest first)
        """
        conditions = ["source = ?"]
        params: list = [source]

        if before_seq is not None:
            conditions.append("rowid < ?")
            params.append(before_seq)
            order = "DESC"
        elif after_seq is not None:
            conditions.append("rowid > ?")
            params.append(after_seq)
            order = "ASC"
        else:
            order = "DESC"

        where = " AND ".join(conditions)
        params.append(limit)

        async with self.db.execute(
            f"""SELECT rowid, id, source, record_type, summary,
                       COALESCE(processed_content, content) as content,
                       timestamp, metadata, run_session_id, created_at
                FROM source_messages
                WHERE {where}
                ORDER BY rowid {order}
                LIMIT ?""",
            tuple(params),
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        for row in rows:
            if row.get("metadata"):
                try:
                    row["metadata"] = json.loads(row["metadata"])
                except (json.JSONDecodeError, TypeError):
                    pass
        return rows

    async def get_known_source_names(self) -> set[str]:
        """Get distinct source names from sync_cursors and source_run_log."""
        names: set[str] = set()
        async with self.db.execute("SELECT DISTINCT source FROM sync_cursors") as cursor:
            async for row in cursor:
                names.add(row[0])
        async with self.db.execute("SELECT DISTINCT source FROM source_run_log") as cursor:
            async for row in cursor:
                names.add(row[0])
        return names

    async def cleanup_expired_consumer_cursors(self) -> int:
        """Delete expired consumer cursors. Returns count deleted."""
        now = datetime.now(timezone.utc).isoformat()
        async with self._atomic():
            async with self.db.execute(
                "SELECT COUNT(*) FROM consumer_cursors WHERE expires_at IS NOT NULL AND expires_at < ?",
                (now,),
            ) as cursor:
                count = (await cursor.fetchone())[0]
            if count > 0:
                await self.db.execute(
                    "DELETE FROM consumer_cursors WHERE expires_at IS NOT NULL AND expires_at < ?",
                    (now,),
                )
        return count
