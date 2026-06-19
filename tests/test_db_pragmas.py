"""Tests for nerve.db connection pragmas (busy_timeout, synchronous, WAL).

These guard the concurrency tuning applied in ``Database.connect()`` — the
same PRAGMA set memU already uses — so that concurrent crons / CLI commands /
the scheduled backup queue politely instead of failing instantly with
"database is locked", and the per-commit fsync cost is dropped under WAL.
"""

import asyncio
import sqlite3

import pytest

from nerve.db import Database


@pytest.mark.asyncio
class TestConnectionPragmas:
    """The tuned pragmas are actually applied on connect()."""

    async def test_pragmas_applied(self, db: Database):
        async with db.db.execute("PRAGMA busy_timeout") as cur:
            assert (await cur.fetchone())[0] == 10000
        # synchronous: NORMAL == 1 (OFF=0, NORMAL=1, FULL=2, EXTRA=3)
        async with db.db.execute("PRAGMA synchronous") as cur:
            assert (await cur.fetchone())[0] == 1
        async with db.db.execute("PRAGMA journal_mode") as cur:
            assert (await cur.fetchone())[0].lower() == "wal"
        async with db.db.execute("PRAGMA foreign_keys") as cur:
            assert (await cur.fetchone())[0] == 1
        async with db.db.execute("PRAGMA temp_store") as cur:
            assert (await cur.fetchone())[0] == 2  # MEMORY
        async with db.db.execute("PRAGMA cache_size") as cur:
            assert (await cur.fetchone())[0] == -16000

    async def test_pragmas_overridable_before_connect(self, tmp_path):
        """A caller or test can tune ``_pragmas`` before connect()."""
        database = Database(tmp_path / "override.db")
        database._pragmas = {**database._pragmas, "busy_timeout": 0}
        await database.connect()
        try:
            async with database.db.execute("PRAGMA busy_timeout") as cur:
                assert (await cur.fetchone())[0] == 0
        finally:
            await database.close()


@pytest.mark.asyncio
class TestBusyTimeoutBehavior:
    """busy_timeout makes a second writer wait instead of erroring."""

    @staticmethod
    async def _hold_write_lock(database: Database) -> None:
        """Begin a write on ``database`` and leave it uncommitted.

        The first write of a deferred transaction acquires the WAL write lock;
        not committing keeps it held until the caller commits.
        """
        await database.db.execute(
            "CREATE TABLE IF NOT EXISTS _lock_probe (x INTEGER)"
        )
        await database.db.commit()
        await database.db.execute("INSERT INTO _lock_probe VALUES (1)")

    async def test_second_writer_waits_and_succeeds(self, tmp_path):
        db_path = tmp_path / "contended.db"
        writer = Database(db_path)
        waiter = Database(db_path)
        await writer.connect()
        await waiter.connect()
        try:
            await self._hold_write_lock(writer)

            async def _waiter_write() -> None:
                await waiter.db.execute("INSERT INTO _lock_probe VALUES (2)")
                await waiter.db.commit()

            task = asyncio.create_task(_waiter_write())
            # Give the waiter time to start blocking inside SQLite's busy
            # handler. With a non-zero busy_timeout it must still be pending
            # (waiting on the lock), not failed.
            await asyncio.sleep(0.3)
            assert not task.done(), "waiter should be blocked on the lock, not done"

            await writer.db.commit()  # release the write lock
            await asyncio.wait_for(task, timeout=5)  # waiter now proceeds

            async with waiter.db.execute(
                "SELECT COUNT(*) FROM _lock_probe"
            ) as cur:
                assert (await cur.fetchone())[0] == 2
        finally:
            await writer.close()
            await waiter.close()

    async def test_zero_timeout_raises_database_locked(self, tmp_path):
        """Control: with busy_timeout=0 the same contention raises immediately.

        Proves the positive test actually exercises the busy-timeout mechanism
        rather than passing for some unrelated reason.
        """
        db_path = tmp_path / "contended_zero.db"
        writer = Database(db_path)
        waiter = Database(db_path)
        waiter._pragmas = {**waiter._pragmas, "busy_timeout": 0}
        await writer.connect()
        await waiter.connect()
        try:
            await self._hold_write_lock(writer)
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                await waiter.db.execute("INSERT INTO _lock_probe VALUES (2)")
                await waiter.db.commit()
            await writer.db.commit()
        finally:
            await writer.close()
            await waiter.close()
