"""Scoped synchronous access for CLI inspection and memory bookkeeping."""

import json
from datetime import date, datetime
from decimal import Decimal

from nerve.db.postgres.connection import Row, translate


def _value(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


class SyncCursor:
    def __init__(self, cursor):
        self.rowcount = cursor.rowcount
        columns = [c.name for c in cursor.description] if cursor.description else []
        self._rows = (
            iter(Row(columns, [_value(v) for v in row]) for row in cursor.fetchall())
            if columns
            else iter(())
        )
        cursor.close()

    def fetchone(self):
        return next(self._rows, None)

    def fetchall(self):
        return list(self._rows)

    def __iter__(self):
        return self._rows


class SyncConnection:
    def __init__(self, config, *, memory=False):
        import psycopg

        self._conn = psycopg.connect(config.postgresql_dsn, connect_timeout=5)
        self.memory = memory
        try:
            role = self._conn.execute(
                "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user"
            ).fetchone()
            if role[0]:
                raise PermissionError("Runtime role must not bypass RLS")
            self._conn.execute("SET search_path = nerve_pg, public")
            self._conn.execute(
                "SELECT set_config('nerve.scope',%s,false)",
                (
                    json.dumps(
                        [config.tenant_id, config.workflow_id], separators=(",", ":")
                    ),
                ),
            )
            self._conn.execute("SET statement_timeout='30s'")
            self._conn.commit()
        except BaseException:
            self._conn.close()
            raise

    def execute(self, sql, params=()):
        if self.memory:
            for table in (
                "resources",
                "memory_items",
                "memory_categories",
                "category_items",
            ):
                sql = sql.replace(f"memu_{table}", table)
        sql, _ = translate(sql)
        return SyncCursor(self._conn.execute(sql, params))

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()
