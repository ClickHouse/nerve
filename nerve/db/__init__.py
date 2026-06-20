"""Nerve database package.

Re-exports the public API so that ``from nerve.db import Database`` (and
friends) continues to work without changes to any importing module.
"""

from __future__ import annotations

from pathlib import Path

from nerve.db.base import SCHEMA_VERSION, Database

# Global database instance
_db: Database | None = None


async def get_db() -> Database:
    """Get the global database instance."""
    global _db
    if _db is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _db


async def init_db(db_path: Path | None = None, workspace: Path | None = None) -> Database:
    """Initialize the global database.

    Args:
        db_path: Path to the SQLite file. Defaults to ``~/.nerve/nerve.db``.
        workspace: Workspace root for resolving task file paths during FTS
            reseed. When omitted, the DB falls back to the DB's parent dir.
    """
    global _db
    if db_path is None:
        db_path = Path("~/.nerve/nerve.db").expanduser()
    _db = Database(db_path, workspace=workspace)
    await _db.connect()
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
