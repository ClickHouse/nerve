"""Execute the shared stores' parameterized SQL on PostgreSQL.

Dialect differences live here; values never participate in SQL rewriting.
Each standalone statement owns its transaction. Multi-statement store writes
use the connection supplied by PostgresDatabase._atomic().
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Mapping

_SERIAL_TABLES = {
    "cron_logs",
    "mcp_tool_usage",
    "memu_audit_log",
    "messages",
    "review_loop_attempts",
    "session_events",
    "session_usage",
    "session_wakeups",
    "skill_usage",
    "source_run_log",
}
_TOKENS = re.compile(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|--[^\n]*|/\*.*?\*/|\?", re.S)


def translate(sql):
    """Translate the finite SQL dialect used by the operational stores."""
    sql = sql.strip().rstrip(";")
    if sql in ("PRAGMA secure_delete=ON", "PRAGMA secure_delete=OFF"):
        return "SELECT 1", False
    if re.search(r"\b(?:PRAGMA|sqlite_master|sqlite_sequence)\b", sql, re.I):
        raise NotImplementedError("SQLite administration is unavailable on PostgreSQL")
    sql = re.sub(r"(\w+) = \? COLLATE NOCASE", r"lower(\1) = lower(?)", sql)
    sql = sql.replace(
        "CAST(strftime('%s', u.created_at) AS REAL)",
        "EXTRACT(EPOCH FROM u.created_at::timestamptz)",
    )
    sql = re.sub(r"\bLIKE\b", "ILIKE", sql)
    replace = re.match(r"INSERT OR REPLACE INTO (\w+)\s*\(([^)]+)\)", sql, re.I)
    if replace:
        keys = {
            "sync_cursors": "source",
            "channel_sessions": "channel_key",
            "consumer_cursors": "consumer, source",
        }
        table, columns = replace.groups()
        if table not in keys:
            raise NotImplementedError(
                f"No PostgreSQL replacement semantics for {table}"
            )
        sql = sql.replace("INSERT OR REPLACE", "INSERT", 1)
        changes = ", ".join(
            f"{c.strip()}=excluded.{c.strip()}"
            for c in columns.split(",")
            if c.strip() not in keys[table].replace(" ", "").split(",")
        )
        sql += f" ON CONFLICT ({keys[table]}) DO UPDATE SET {changes}"
    if re.match(r"INSERT OR IGNORE\b", sql, re.I):
        sql = re.sub(r"INSERT OR IGNORE", "INSERT", sql, count=1, flags=re.I)
        sql += " ON CONFLICT DO NOTHING"
    sql = re.sub(r"ON CONFLICT\s*\(", "ON CONFLICT (_scope, ", sql, flags=re.I)
    match = re.match(r"INSERT INTO (\w+)\b", sql, re.I)
    returning = bool(
        match and match[1] in _SERIAL_TABLES and "RETURNING" not in sql.upper()
    )
    if returning:
        sql += " RETURNING id"
    sql = sql.replace("%", "%%")
    sql = _TOKENS.sub(lambda m: "%s" if m[0] == "?" else m[0], sql)
    return sql, returning


class Row(Mapping):
    def __init__(self, columns, values):
        self._data = {
            key: value for key, value in zip(columns, values) if key != "_scope"
        }
        self._values = tuple(
            value for key, value in zip(columns, values) if key != "_scope"
        )

    def __getitem__(self, key):
        return self._values[key] if isinstance(key, (int, slice)) else self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)


class Cursor:
    def __init__(self, rows, *, rowcount=0, lastrowid=None):
        self.rows, self.rowcount, self.lastrowid = rows, rowcount, lastrowid
        self._position = 0

    async def fetchone(self):
        if self._position >= len(self.rows):
            return None
        row = self.rows[self._position]
        self._position += 1
        return row

    async def fetchall(self):
        rows = self.rows[self._position :]
        self._position = len(self.rows)
        return rows

    async def close(self):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row


class Execute:
    def __init__(self, owner, sql, params):
        self.owner, self.sql, self.params = owner, sql, params

    async def _run(self):
        import psycopg

        if self.sql.strip().upper() == "BEGIN IMMEDIATE":
            if self.owner._active.get() is None:
                raise RuntimeError(
                    "A write lock requires an enclosing atomic transaction"
                )
            return Cursor([])
        sql, returning = translate(self.sql)
        try:
            async with self.owner.connection() as conn:
                cursor = await conn.execute(sql, self.params)
                columns = (
                    [col.name for col in cursor.description]
                    if cursor.description
                    else []
                )
                rows = (
                    [Row(columns, row) for row in await cursor.fetchall()]
                    if columns
                    else []
                )
                return Cursor(
                    rows,
                    rowcount=cursor.rowcount,
                    lastrowid=rows[0][0] if returning and rows else None,
                )
        except psycopg.IntegrityError as exc:
            # The existing domain methods catch this portable constraint error.
            raise sqlite3.IntegrityError(str(exc)) from exc

    def __await__(self):
        return self._run().__await__()

    async def __aenter__(self):
        self.cursor = await self._run()
        return self.cursor

    async def __aexit__(self, *args):
        await self.cursor.close()


class Connection:
    def __init__(self, owner):
        self.owner = owner

    def execute(self, sql, params=()):
        return Execute(self.owner, sql, params)

    async def executemany(self, sql, params):
        rowcount = 0
        for values in params:
            result = await self.execute(sql, values)
            rowcount += result.rowcount
        return Cursor([], rowcount=rowcount)

    async def commit(self):
        if self.owner._active.get() is not None:
            raise RuntimeError("Commit belongs to the enclosing atomic transaction")

    async def rollback(self):
        if self.owner._active.get() is not None:
            raise RuntimeError("Rollback belongs to the enclosing atomic transaction")
