"""Database maintenance: message compaction, telemetry pruning, reclaim.

Opt-in retention for ``nerve.db``. The dominant footprint is the verbose
machine-facing ``blocks``/``thinking`` JSON on old messages, which is safe to
drop once a message is in memU and no longer rendered live:

* memU extraction reads ``content``, not ``blocks``
  (``nerve/memory/memu_bridge.py``), gated by the per-session
  ``last_memorized_at`` watermark (``nerve/agent/engine.py``).
* SDK resume restores context from the ``.jsonl`` transcript, not DB blocks.
* The only remaining reader of ``blocks`` is UI rendering of an opened
  session, which falls back to the kept ``content`` text when blocks is NULL.

So compaction targets messages that are older than the full-retention window,
already past their session's memorize watermark, in a non-starred session, and
not the currently connected (``active``) session.

Telemetry tables and file snapshots are append-only and pruned by age.

Reclaim model: nulling columns and deleting rows frees pages to the SQLite
freelist (reused by later writes) but does not shrink the file.
``PRAGMA wal_checkpoint(TRUNCATE)`` truncates only the WAL. Only ``VACUUM``
rewrites and shrinks the main DB file, so it is exposed as an explicit,
operator-run step (it takes a write lock and cannot run in a transaction).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


# Append-only telemetry tables keyed by their timestamp column. memu_audit_log
# uses ``timestamp``; the others use ``created_at``. Names are static (never
# user input), so interpolating them into SQL is safe.
_TELEMETRY_TABLES: dict[str, str] = {
    "session_events": "created_at",
    "mcp_tool_usage": "created_at",
    "session_usage": "created_at",
    "memu_audit_log": "timestamp",
}

# Compaction predicate, shared by the count (dry-run) query and the UPDATE.
# Single ``?`` param: the full-retention cutoff. ``datetime(...)`` normalizes
# the mixed stored timestamp formats (space-separated vs ISO-8601) to UTC.
_COMPACT_WHERE = """
    (m.blocks IS NOT NULL OR m.thinking IS NOT NULL)
    AND datetime(m.created_at) < datetime(?)
    AND s.last_memorized_at IS NOT NULL
    AND datetime(m.created_at) < datetime(s.last_memorized_at)
    AND s.starred = 0
    AND s.status != 'active'
"""


class MaintenanceStore:
    """Mixin providing opt-in DB retention: compaction, pruning, reclaim."""

    @staticmethod
    def _cutoff(days: int) -> str:
        """UTC cutoff ``days`` ago as ``YYYY-MM-DD HH:MM:SS`` (clamped >= 1)."""
        days = max(1, int(days))
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        return cutoff.strftime("%Y-%m-%d %H:%M:%S")

    async def compact_messages(
        self, full_days: int, *, dry_run: bool = False,
    ) -> dict:
        """Drop ``blocks``/``thinking`` from old, memorized, inactive messages.

        Keeps ``content``, ``role``, ``created_at``, ``external_id``. Idempotent:
        rows already NULL on both columns are skipped, so a second pass is a
        no-op. ``content`` is never touched.

        Returns ``{"messages_compacted": int, "bytes_reclaimed": int}``.
        ``bytes_reclaimed`` is an estimate (``LENGTH`` of the TEXT JSON).
        """
        cutoff = self._cutoff(full_days)
        async with self.db.execute(
            f"""SELECT COUNT(*),
                       COALESCE(SUM(LENGTH(m.blocks)), 0)
                       + COALESCE(SUM(LENGTH(m.thinking)), 0)
                FROM messages m
                JOIN sessions s ON m.session_id = s.id
                WHERE {_COMPACT_WHERE}""",
            (cutoff,),
        ) as cursor:
            row = await cursor.fetchone()
        count = row[0] if row else 0
        reclaimed = (row[1] or 0) if row else 0

        if not dry_run and count:
            async with self._atomic():
                await self.db.execute(
                    f"""UPDATE messages
                        SET blocks = NULL, thinking = NULL
                        WHERE id IN (
                            SELECT m.id FROM messages m
                            JOIN sessions s ON m.session_id = s.id
                            WHERE {_COMPACT_WHERE}
                        )""",
                    (cutoff,),
                )

        return {"messages_compacted": count, "bytes_reclaimed": reclaimed}

    async def prune_telemetry(
        self, days: int, *, dry_run: bool = False,
    ) -> dict:
        """Delete telemetry rows older than ``days`` from the append-only tables.

        Returns ``{"telemetry_deleted": int, "by_table": {table: int, ...}}``.
        """
        cutoff = self._cutoff(days)
        by_table: dict[str, int] = {}
        total = 0
        for table, ts_col in _TELEMETRY_TABLES.items():
            async with self.db.execute(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE datetime({ts_col}) < datetime(?)",
                (cutoff,),
            ) as cursor:
                row = await cursor.fetchone()
            n = row[0] if row else 0
            by_table[table] = n
            total += n
            if not dry_run and n:
                async with self._atomic():
                    await self.db.execute(
                        f"DELETE FROM {table} "
                        f"WHERE datetime({ts_col}) < datetime(?)",
                        (cutoff,),
                    )
        return {"telemetry_deleted": total, "by_table": by_table}

    async def prune_file_snapshots(
        self, days: int, *, dry_run: bool = False,
    ) -> dict:
        """Delete ``session_file_snapshots`` rows older than ``days``.

        Returns ``{"snapshots_deleted": int, "bytes_reclaimed": int}``.
        """
        cutoff = self._cutoff(days)
        async with self.db.execute(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(original_content)), 0) "
            "FROM session_file_snapshots WHERE datetime(created_at) < datetime(?)",
            (cutoff,),
        ) as cursor:
            row = await cursor.fetchone()
        count = row[0] if row else 0
        reclaimed = (row[1] or 0) if row else 0

        if not dry_run and count:
            async with self._atomic():
                await self.db.execute(
                    "DELETE FROM session_file_snapshots "
                    "WHERE datetime(created_at) < datetime(?)",
                    (cutoff,),
                )
        return {"snapshots_deleted": count, "bytes_reclaimed": reclaimed}

    async def checkpoint(self) -> None:
        """Truncate the WAL after a prune pass (best-effort).

        Frees the WAL file; does not shrink the main DB (see :meth:`vacuum`).
        Runs under the write lock: an unlocked bare ``commit()`` here could
        prematurely flush another coroutine's in-flight transaction.
        """
        async with self._write_lock:
            await self._heal_leaked_txn()
            await self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    async def vacuum(self) -> None:
        """Rewrite the DB file to reclaim freelist pages (shrinks the file).

        Cannot run inside a transaction (``_heal_leaked_txn`` guarantees the
        connection is clean) and autocommits on completion. Serialized under
        the write lock; still an explicit operator step, never on the
        background loop. Run with the daemon stopped to avoid lock contention.
        """
        async with self._write_lock:
            await self._heal_leaked_txn()
            await self.db.execute("VACUUM")

    async def run_retention(
        self,
        *,
        retention_days: int,
        retention_full_days: int,
        dry_run: bool = False,
    ) -> dict:
        """Run a full retention pass: compact, prune telemetry + snapshots,
        then checkpoint the WAL. Does NOT VACUUM (file shrink is operator-run).

        Returns a merged report dict.
        """
        compacted = await self.compact_messages(
            retention_full_days, dry_run=dry_run,
        )
        telemetry = await self.prune_telemetry(retention_days, dry_run=dry_run)
        snapshots = await self.prune_file_snapshots(
            retention_days, dry_run=dry_run,
        )
        # Distinct keys: compaction and snapshot pruning both report
        # ``bytes_reclaimed`` in isolation, so namespace them here rather than
        # merging (which would clobber the larger compaction figure).
        report = {
            "dry_run": dry_run,
            "messages_compacted": compacted["messages_compacted"],
            "message_bytes_reclaimed": compacted["bytes_reclaimed"],
            "telemetry_deleted": telemetry["telemetry_deleted"],
            "by_table": telemetry["by_table"],
            "snapshots_deleted": snapshots["snapshots_deleted"],
            "snapshot_bytes_reclaimed": snapshots["bytes_reclaimed"],
        }
        # Only checkpoint when something actually changed on disk.
        if not dry_run and (
            compacted["messages_compacted"]
            or telemetry["telemetry_deleted"]
            or snapshots["snapshots_deleted"]
        ):
            await self.checkpoint()
        return report
