"""Nerve database package.

Re-exports the public API so that ``from nerve.db import Database`` (and
friends) continues to work without changes to any importing module.
"""

from __future__ import annotations

from pathlib import Path

from nerve import paths
from nerve.db.base import SCHEMA_VERSION, Database

# Global database instance
_db: Database | None = None


async def get_db() -> Database:
    """Get the global database instance."""
    global _db
    if _db is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _db


async def init_db(db_path: Path | None = None, workspace: Path | None = None, *, config=None) -> Database:
    """Initialize the global database.

    Args:
        db_path: Path to the SQLite file. Defaults to ``~/.nerve/nerve.db``.
        workspace: Workspace root for resolving task file paths during FTS
            reseed. When omitted, the DB falls back to the DB's parent dir.

    The global is set only after ``connect()`` succeeds, so ``get_db()`` never
    returns a database that failed to open or migrate.
    """
    global _db
    if db_path is None:
        db_path = paths.db_path()
    db = create_database(config, db_path=db_path, workspace=workspace)
    await db.connect()
    _db = db
    return _db


async def close_db() -> None:
    """Close the global database."""
    global _db
    if _db:
        await _db.close()
        _db = None


__all__ = [
    "Database",
    "SCHEMA_VERSION",
    "get_db",
    "init_db",
    "close_db",
]


def create_database(config=None, *, db_path=None, workspace=None):
    """Select storage once from trusted configuration; never fall back on failure."""
    if config is not None and config.use_postgresql:
        if not config.postgresql_dsn:
            raise ValueError("PostgreSQL requires postgresql_dsn")
        from nerve.db.postgres import PostgresDatabase
        return PostgresDatabase(
            config.postgresql_dsn, tenant=config.tenant_id, workflow=config.workflow_id,
            workspace=workspace or config.workspace,
        )
    return Database(db_path or paths.db_path(), workspace=workspace)
