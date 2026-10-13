"""Migration runner — discovers and applies numbered migration files."""

from __future__ import annotations

import importlib
import logging
import os
import pkgutil
from pathlib import Path

import aiosqlite

logger = logging.getLogger(__name__)

# Set to "1" to run this code against a database whose schema is newer than
# the migrations it ships. Off by default: older code on a newer schema has no
# migration to apply and no idea what the newer columns mean, so it fails
# silently later instead of loudly now. Meant for a deliberate fallback to an
# older build, where staying up matters more and the risk is accepted.
ALLOW_NEWER_SCHEMA_ENV = "NERVE_ALLOW_NEWER_SCHEMA"


class SchemaNewerThanCodeError(RuntimeError):
    """The database was migrated by a newer Nerve than this one."""

    def __init__(self, database_version: int, code_version: int) -> None:
        self.database_version = database_version
        self.code_version = code_version
        super().__init__(
            f"the database is at schema version {database_version}, newer than the "
            f"version {code_version} this Nerve ships; refusing to start on a schema "
            f"it does not know. Run the Nerve that migrated it, restore the state "
            f"snapshot taken before that upgrade, or set {ALLOW_NEWER_SCHEMA_ENV}=1 "
            f"to accept the risk."
        )


def discover_migrations() -> list[tuple[int, str]]:
    """Scan the migrations package for vNNN_*.py files.

    Returns sorted list of (version, module_name) tuples.
    """
    migrations_dir = Path(__file__).parent
    results: list[tuple[int, str]] = []
    for info in pkgutil.iter_modules([str(migrations_dir)]):
        name = info.name
        if name.startswith("v") and "_" in name:
            try:
                version = int(name.split("_", 1)[0][1:])
                results.append((version, name))
            except ValueError:
                continue
    results.sort(key=lambda x: x[0])
    return results


async def get_current_version(db: aiosqlite.Connection) -> int:
    """Read the current schema version from the database."""
    try:
        async with db.execute("SELECT MAX(version) FROM schema_version") as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] else 0
    except Exception:
        return 0


async def run_migrations(db: aiosqlite.Connection) -> int:
    """Apply all pending migrations in order.

    Returns the final schema version after applying migrations.
    """
    current = await get_current_version(db)
    migrations = discover_migrations()

    code_version = migrations[-1][0] if migrations else 0
    if current > code_version:
        if os.environ.get(ALLOW_NEWER_SCHEMA_ENV, "").strip() != "1":
            raise SchemaNewerThanCodeError(current, code_version)
        logger.warning(
            "Database schema version %d is newer than this code's %d; continuing "
            "because %s=1 is set",
            current, code_version, ALLOW_NEWER_SCHEMA_ENV,
        )

    applied = 0
    for version, module_name in migrations:
        if current >= version:
            continue

        full_module = f"nerve.db.migrations.{module_name}"
        mod = importlib.import_module(full_module)

        if not hasattr(mod, "up"):
            logger.warning("Migration %s has no up() function, skipping", module_name)
            continue

        logger.info("Applying migration V%d (%s)...", version, module_name)
        try:
            await mod.up(db)
            await db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)",
                (version,),
            )
            await db.commit()
            applied += 1
            logger.info("Migration V%d applied successfully", version)
        except Exception:
            logger.exception("Migration V%d failed", version)
            raise

    final_version = await get_current_version(db)
    if applied > 0:
        logger.info(
            "Database migrated to schema version %d (%d migrations applied)",
            final_version, applied,
        )
    return final_version
