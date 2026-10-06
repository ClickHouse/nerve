"""Optional PostgreSQL operational database."""

from __future__ import annotations

import asyncio
import json
import hashlib
import re
import uuid
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path

from nerve.db.base import Database, StatePermissions, WriteResult
from nerve.db.postgres.connection import Connection


class PostgresDatabase(Database):
    persistent_task_content = True
    task_search_rank_threshold = None

    def __init__(self, dsn, *, tenant="default", workflow="default", workspace=None):
        if any(
            not isinstance(v, str) or not v or len(v) > 200 or "\x00" in v
            for v in (tenant, workflow)
        ):
            raise ValueError(
                "Tenant and workflow must be nonempty identifiers of at most 200 characters"
            )
        self._dsn = dsn
        self.scope = json.dumps([tenant, workflow], separators=(",", ":"))
        self.workspace = workspace
        self._active = ContextVar(f"postgres_transaction_{id(self)}", default=None)
        self._connected = False
        self._system_actor = None
        self.state_permissions = StatePermissions()
        self._write_lock = asyncio.Lock()
        self._db = Connection(self)

    @property
    def db(self):
        if not self._connected:
            raise RuntimeError("Database not connected")
        return self._db

    @asynccontextmanager
    async def connection(self):
        import psycopg

        if not self._connected:
            raise RuntimeError("Database not connected")
        active = self._active.get()
        if active is not None:
            if active[0] is not asyncio.current_task():
                raise RuntimeError(
                    "A transaction cannot be shared across concurrent tasks"
                )
            yield active[1]
            return
        async with await psycopg.AsyncConnection.connect(
            self._dsn, connect_timeout=5
        ) as conn:
            await conn.execute("SET LOCAL search_path = nerve_pg, public")
            await conn.execute("SET LOCAL statement_timeout = '30s'")
            await conn.execute("SET LOCAL lock_timeout = '10s'")
            await conn.execute(
                "SELECT set_config('nerve.scope', %s, true)", (self.scope,)
            )
            role = await (
                await conn.execute(
                    "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user"
                )
            ).fetchone()
            if role[0]:
                raise PermissionError("PostgreSQL runtime role must not bypass RLS")
            yield conn

    async def connect(self):
        self._connected = True
        try:
            async with self.connection() as conn:
                rows = await (
                    await conn.execute("SELECT version FROM storage_version")
                ).fetchall()
                if rows != [(1,)]:
                    raise RuntimeError(
                        "Unsupported PostgreSQL schema; apply owner migrations first"
                    )
            async with self._atomic():
                await self.db.execute(
                    "INSERT INTO actor_refs (id,kind,display_name,created_at) "
                    "SELECT ?, 'system', NULL, ? WHERE NOT EXISTS (SELECT 1 FROM actor_refs WHERE kind='system')",
                    (str(uuid.uuid4()), datetime.now(timezone.utc).isoformat()),
                )
                initialized = await self.db.execute(
                    "INSERT INTO scopes DEFAULT VALUES ON CONFLICT DO NOTHING"
                )
                from nerve.db.migrations.v030_task_statuses import _SEED

                for values in _SEED if initialized.rowcount else ():
                    await self.db.execute(
                        "INSERT OR IGNORE INTO task_statuses (name,label,color,description,is_system,sort_order) "
                        "VALUES (?,?,?,?,?,?)",
                        values,
                    )
            await self._cache_system_actor()
        except BaseException:
            self._connected = False
            raise

    async def close(self):
        self._connected = False
        self._system_actor = None

    @asynccontextmanager
    async def _atomic(self):
        if self._active.get() is not None:
            raise RuntimeError("Nested write transaction")
        async with self.connection() as conn:
            # Protect read/modify/write sequences across processes of this scope.
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (self.scope,)
            )
            token = self._active.set((asyncio.current_task(), conn))
            try:
                yield
            finally:
                self._active.reset(token)

    async def _write(self, sql, params=()):
        async with self._atomic():
            cursor = await self.db.execute(sql, params)
            return WriteResult(cursor.lastrowid, cursor.rowcount)

    async def checkpoint(self):
        pass  # PostgreSQL owns WAL checkpointing.

    async def vacuum(self):
        raise ValueError("PostgreSQL vacuum is managed by the database administrator")

    async def rebuild_fts(self):
        # Task content is durable here; startup must never truncate it.
        pass

    async def get_status_entry_times(self, task_ids):
        if not task_ids:
            return {}
        placeholders = ",".join("?" for _ in task_ids)
        async with self.db.execute(
            "SELECT e.task_id,e.to_status,e.created_at FROM task_events e "
            "JOIN tasks t ON t.id=e.task_id AND t.status=e.to_status "
            f"WHERE e.task_id IN ({placeholders}) AND e.id=(SELECT e2.id FROM task_events e2 "
            "WHERE e2.task_id=e.task_id ORDER BY e2.created_at DESC,e2.id DESC LIMIT 1)",
            task_ids,
        ) as cursor:
            return {r[0]: r[2] async for r in cursor}

    @staticmethod
    def _search_query(query, *, similar=False):
        words = re.findall(r"[^\W_]+", query, flags=re.UNICODE)
        return (" | " if similar else " & ").join("'" + word + "':*" for word in words)

    async def search_tasks(
        self, query, status=None, tag=None, limit=20, *, similar=False
    ):
        if not query.strip():
            return []
        tsquery = self._search_query(query, similar=similar)
        conditions = [
            "(t.id ILIKE ? OR t.title ILIKE ? OR f.document @@ to_tsquery('simple', ?))"
        ]
        params = [f"%{query}%", f"%{query}%", tsquery]
        self._apply_status_filter(conditions, params, status)
        self._apply_tag_filter(conditions, params, tag)
        params.extend([query, tsquery, limit])
        async with self.db.execute(
            "SELECT t.* FROM tasks t JOIN tasks_fts f ON f.task_id=t.id WHERE "
            + " AND ".join(conditions)
            + " ORDER BY (t.id=?) DESC, ts_rank(f.document,to_tsquery('simple',?)) DESC,t.id LIMIT ?",
            params,
        ) as cursor:
            return [dict(r) async for r in cursor]

    async def search_tasks_similar(self, query, limit=10, rank_threshold=None):
        if rank_threshold is not None:
            raise ValueError(
                "SQLite BM25 thresholds cannot be applied to PostgreSQL search"
            )
        return await self.search_tasks(query, status="all", limit=limit, similar=True)

    async def get_task(self, task_id):
        row = await super().get_task(task_id)
        if row and self.workspace:
            from nerve.config import ensure_path_not_tracked_config
            from nerve.utils.fs import atomic_write_text

            target = (Path(self.workspace) / row["file_path"]).resolve()
            root = (Path(self.workspace) / "memory" / "tasks").resolve()
            if not target.is_relative_to(root):
                raise ValueError("Task file must be within the workspace task cache")
            ensure_path_not_tracked_config(target, "write")
            target.parent.mkdir(parents=True, exist_ok=True)
            if (
                not target.is_file()
                or await asyncio.to_thread(target.read_text) != row["content"]
            ):
                await asyncio.to_thread(atomic_write_text, target, row["content"])
        return row

    async def reserve_task_id(self, task_id):
        result = await self._write(
            "INSERT INTO task_reservations (id) VALUES (?) ON CONFLICT DO NOTHING",
            (task_id,),
        )
        return result.rowcount == 1

    async def next_source_sequence(self):
        async with self.db.execute(
            "SELECT nextval('nerve_pg.source_messages_rowid_seq')"
        ) as cursor:
            return (await cursor.fetchone())[0]

    async def release_task_id(self, task_id):
        await self._write("DELETE FROM task_reservations WHERE id=?", (task_id,))

    async def save_uploaded_file(
        self, file_id, session_id, filename, media_type, file_type, file_size, disk_path
    ):
        content = await asyncio.to_thread(Path(disk_path).read_bytes)
        async with self._atomic():
            await self.db.execute(
                "INSERT INTO uploaded_files (id,session_id,filename,media_type,file_type,file_size,disk_path,created_at,content) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    file_id,
                    session_id,
                    filename,
                    media_type,
                    file_type,
                    len(content),
                    disk_path,
                    datetime.now(timezone.utc).isoformat(),
                    content,
                ),
            )

    async def _restore_upload(self, row):
        if row and self.workspace:
            content = row.pop("content")
            name = hashlib.sha256(row["id"].encode()).hexdigest()
            target = Path(self.workspace) / ".nerve-cache" / "uploads" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(target.write_bytes, content)
            row["disk_path"] = str(target)
        return row

    async def get_uploaded_file(self, file_id):
        return await self._restore_upload(await super().get_uploaded_file(file_id))

    async def get_uploaded_files_by_ids(self, file_ids):
        return [
            await self._restore_upload(row)
            for row in await super().get_uploaded_files_by_ids(file_ids)
        ]
