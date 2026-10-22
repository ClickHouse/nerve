"""Migration runner — discovers and applies numbered migration files.

Migrations come in two tracks. Each track is numbered on its own and recorded
in its own table, and the tracks are applied in this order:

* upstream: ``nerve/db/migrations/vNNN_*.py``, recorded in ``schema_version``;
* fork: ``nerve/db/migrations/fork/vNNN_*.py``, recorded in
  ``fork_schema_version``. Upstream ships none. A fork of Nerve adds its own
  migrations there, so a fork's number can never shadow an upstream one (see
  ``fork/README.md``).
"""

from __future__ import annotations

import importlib
import logging
import os
import pkgutil
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import aiosqlite

logger = logging.getLogger(__name__)

# Set to "1" to run this code against a database whose schema is newer than
# the migrations it ships. Off by default: older code on a newer schema has no
# migration to apply and no idea what the newer columns mean, so it fails
# silently later instead of loudly now. Meant for a deliberate fallback to an
# older build, where staying up matters more and the risk is accepted.
ALLOW_NEWER_SCHEMA_ENV = "NERVE_ALLOW_NEWER_SCHEMA"

#: Exit status of a process that refused a database newer than its code: the
#: gateway at start, and every CLI command that opens the database. It is
#: EX_DATAERR from sysexits.h. It differs from 1 (a failed command), 2 (a
#: usage error) and 3 (uvicorn's code for any other startup failure), so a
#: supervisor can tell this failure apart and stop restarting.
NEWER_SCHEMA_EXIT_CODE = 65


class SchemaNewerThanCodeError(RuntimeError):
    """The database was migrated by a newer Nerve than this one."""

    def __init__(
        self, database_version: int, code_version: int, track: str = "upstream",
    ) -> None:
        self.database_version = database_version
        self.code_version = code_version
        self.track = track
        super().__init__(
            f"the database is at {track} schema version {database_version}, newer "
            f"than the version {code_version} this Nerve ships; refusing to start on "
            f"a schema it does not know. Run the Nerve that migrated it, restore the "
            f"state snapshot taken before that upgrade, or set "
            f"{ALLOW_NEWER_SCHEMA_ENV}=1 to accept the risk."
        )


@dataclass(frozen=True)
class MigrationTrack:
    """A numbered sequence of migrations and the table that records it.

    ``directory`` holds the ``vNNN_*.py`` files, imported as modules of
    ``package``. ``table`` is interpolated into SQL, so it must be a
    constant in code, never input.
    """

    name: str
    package: str
    directory: Path
    table: str


UPSTREAM_TRACK = MigrationTrack(
    "upstream", "nerve.db.migrations", Path(__file__).parent, "schema_version",
)
FORK_TRACK = MigrationTrack(
    "fork", "nerve.db.migrations.fork", Path(__file__).parent / "fork",
    "fork_schema_version",
)
#: Every track, in the order they are applied.
TRACKS = (UPSTREAM_TRACK, FORK_TRACK)


def discover_migrations(track: MigrationTrack = UPSTREAM_TRACK) -> list[tuple[int, str]]:
    """Scan a track's directory for vNNN_*.py files.

    Returns sorted list of (version, module_name) tuples.
    """
    results: list[tuple[int, str]] = []
    for info in pkgutil.iter_modules([str(track.directory)]):
        name = info.name
        if name.startswith("v") and "_" in name:
            try:
                version = int(name.split("_", 1)[0][1:])
                results.append((version, name))
            except ValueError:
                continue
    results.sort(key=lambda x: x[0])
    return results


def code_version(track: MigrationTrack) -> int:
    """The highest migration a track ships (0 when it has none)."""
    migrations = discover_migrations(track)
    return migrations[-1][0] if migrations else 0


async def get_current_version(
    db: aiosqlite.Connection, track: MigrationTrack = UPSTREAM_TRACK,
) -> int:
    """Read a track's current schema version from the database (0 before its
    first migration)."""
    try:
        async with db.execute(f"SELECT MAX(version) FROM {track.table}") as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] else 0
    except Exception:
        return 0


async def get_fork_version(db: aiosqlite.Connection) -> int:
    """Read the fork track's schema version (0 when no fork migration has run)."""
    return await get_current_version(db, FORK_TRACK)


def newer_schema_allowed() -> bool:
    """Whether ``NERVE_ALLOW_NEWER_SCHEMA=1`` is set (only ``1`` counts)."""
    return os.environ.get(ALLOW_NEWER_SCHEMA_ENV, "").strip() == "1"


async def check_schema(
    db: aiosqlite.Connection, tracks: Sequence[MigrationTrack] = TRACKS,
) -> None:
    """Refuse a database that is newer than this code on any track.

    Raises :class:`SchemaNewerThanCodeError` for the first such track. With
    ``NERVE_ALLOW_NEWER_SCHEMA=1`` it logs a warning instead.
    """
    for track in tracks:
        current = await get_current_version(db, track)
        shipped = code_version(track)
        if current <= shipped:
            continue
        if not newer_schema_allowed():
            raise SchemaNewerThanCodeError(current, shipped, track.name)
        logger.warning(
            "Database %s schema version %d is newer than this code's %d; continuing "
            "because %s=1 is set",
            track.name, current, shipped, ALLOW_NEWER_SCHEMA_ENV,
        )


async def _apply_track(db: aiosqlite.Connection, track: MigrationTrack) -> None:
    current = await get_current_version(db, track)
    pending = [(v, name) for v, name in discover_migrations(track) if v > current]
    if not pending:
        return
    # The version table is created on first use. The upstream table also comes
    # from v001, with the same definition.
    await db.execute(
        f"CREATE TABLE IF NOT EXISTS {track.table} (version INTEGER PRIMARY KEY)"
    )

    applied = 0
    for version, module_name in pending:
        mod = importlib.import_module(f"{track.package}.{module_name}")

        if not hasattr(mod, "up"):
            logger.warning(
                "Skipping %s migration %s, which has no up() function",
                track.name, module_name,
            )
            continue

        logger.info("Applying %s migration V%d (%s)...", track.name, version, module_name)
        try:
            await mod.up(db)
            await db.execute(
                f"INSERT OR REPLACE INTO {track.table} (version) VALUES (?)",
                (version,),
            )
            await db.commit()
            applied += 1
            logger.info("Applied %s migration V%d", track.name, version)
        except Exception:
            logger.exception("Failed to apply %s migration V%d", track.name, version)
            raise

    if applied > 0:
        logger.info(
            "Database migrated to %s schema version %d (%d migrations applied)",
            track.name, await get_current_version(db, track), applied,
        )


async def run_migrations(
    db: aiosqlite.Connection, tracks: Sequence[MigrationTrack] = TRACKS,
) -> int:
    """Apply all pending migrations in order, track by track.

    Every track passes the newer-schema guard before any migration runs, so a
    refused database is left as it was. Within a track, versions at or below
    the database's current one are skipped.

    Returns the final version of the first track (by default the upstream
    schema version).
    """
    await check_schema(db, tracks)
    for track in tracks:
        await _apply_track(db, track)
    return await get_current_version(db, tracks[0])
