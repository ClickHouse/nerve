"""Transaction-safety tests for the shared-connection write path.

Covers the failure class behind the 2026-07-06 production wedge: an
abandoned open transaction on the shared aiosqlite connection pins a stale
WAL read snapshot, and once any other connection commits, every subsequent
write on the shared connection fails instantly with "database is locked"
(SQLITE_BUSY_SNAPSHOT — ``busy_timeout`` deliberately does not apply).
Nothing ever called ROLLBACK, so the daemon stayed wedged until restart.

The write layer now guarantees:
  * ``_atomic()`` / ``_write()`` commit on success, roll back on ANY error
    (including task cancellation) — the connection is never left inside an
    open transaction.
  * ``_heal_leaked_txn()`` recovers from a leaked transaction produced by
    any code path that bypasses the helpers.
  * Every writer serializes under ``_write_lock``, so a single-statement
    commit can never flush another coroutine's in-flight transaction.
"""

import asyncio
import sqlite3

import pytest

from nerve.db import Database


def _external_commit(db_path) -> None:
    """Commit a write from a second, independent connection.

    Simulates the external process (CLI command, helper script, backup)
    whose commit advances the WAL past the shared connection's snapshot.
    """
    ext = sqlite3.connect(str(db_path), timeout=5)
    try:
        ext.execute(
            "INSERT OR REPLACE INTO sync_cursors (source, cursor, updated_at) "
            "VALUES ('external-writer', 'x', 'now')"
        )
        ext.commit()
    finally:
        ext.close()


async def _poison_connection(db: Database) -> None:
    """Leave the shared connection inside an open txn with a pinned snapshot.

    Equivalent to a write task abandoned between the implicit BEGIN and the
    commit (the incident's entry path).
    """
    await db.db.execute("BEGIN")
    cur = await db.db.execute("SELECT COUNT(*) FROM sync_cursors")
    await cur.fetchone()
    await cur.close()
    assert db.db.in_transaction


@pytest.mark.asyncio
class TestPoisonedConnectionRecovery:
    """The incident reproduction: leaked txn + external commit."""

    async def test_write_heals_leaked_transaction(self, db: Database, caplog):
        await _poison_connection(db)
        _external_commit(db.db_path)

        # Pre-fix behavior: this raised OperationalError("database is locked")
        # in ~0ms, forever, despite busy_timeout=10s. The heal guard must
        # roll back the leaked txn and let the write proceed.
        await db.set_sync_cursor("healed", "42")

        assert not db.db.in_transaction
        assert await db.get_sync_cursor("healed") == "42"
        assert "Leaked open transaction" in caplog.text

    async def test_atomic_heals_leaked_transaction(self, db: Database):
        await _poison_connection(db)
        _external_commit(db.db_path)

        # Multi-statement path must recover the same way.
        await db.upsert_task("t-heal", "tasks/t-heal.md", "heal test")

        assert not db.db.in_transaction
        task = await db.get_task("t-heal")
        assert task is not None and task["title"] == "heal test"

    async def test_stale_snapshot_write_fails_without_heal(self, db: Database):
        """Documents the raw failure mode the guard protects against.

        A write attempted directly on the poisoned connection (bypassing
        the helpers) fails instantly — proving the wedge is real and that
        busy_timeout cannot save it. ROLLBACK is the only cure.
        """
        await _poison_connection(db)
        _external_commit(db.db_path)

        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            await db.db.execute(
                "INSERT OR REPLACE INTO sync_cursors (source, cursor, updated_at) "
                "VALUES ('doomed', 'x', 'now')"
            )

        await db.db.rollback()  # the cure
        await db.set_sync_cursor("recovered", "1")
        assert await db.get_sync_cursor("recovered") == "1"


@pytest.mark.asyncio
class TestAtomicRollback:
    """_atomic() must discard partial writes on error and stay clean."""

    async def test_body_error_rolls_back_partial_writes(self, db: Database):
        with pytest.raises(RuntimeError, match="boom"):
            async with db._atomic():
                await db.db.execute(
                    "INSERT INTO sync_cursors (source, cursor, updated_at) "
                    "VALUES ('partial', 'x', 'now')"
                )
                raise RuntimeError("boom")

        assert not db.db.in_transaction
        assert await db.get_sync_cursor("partial") is None
        # Connection remains fully usable.
        await db.set_sync_cursor("after-error", "7")
        assert await db.get_sync_cursor("after-error") == "7"

    async def test_failed_statement_rolls_back(self, db: Database):
        with pytest.raises(sqlite3.OperationalError):
            await db._write("INSERT INTO no_such_table VALUES (1)")
        assert not db.db.in_transaction
        await db.set_sync_cursor("after-bad-sql", "3")
        assert await db.get_sync_cursor("after-bad-sql") == "3"

    async def test_cancellation_mid_transaction_rolls_back(self, db: Database):
        """A cancelled write task must not leave an open transaction.

        This is the incident's most likely entry path: the SDK cancels an
        in-flight tool task between the implicit BEGIN and the commit.
        """
        entered = asyncio.Event()
        release = asyncio.Event()  # never set — the task parks here

        async def body():
            async with db._atomic():
                await db.db.execute(
                    "INSERT INTO sync_cursors (source, cursor, updated_at) "
                    "VALUES ('cancelled', 'x', 'now')"
                )
                entered.set()
                await release.wait()

        task = asyncio.create_task(body())
        await asyncio.wait_for(entered.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert not db.db.in_transaction
        assert await db.get_sync_cursor("cancelled") is None
        await db.set_sync_cursor("after-cancel", "9")
        assert await db.get_sync_cursor("after-cancel") == "9"


@pytest.mark.asyncio
class TestWriteSerialization:
    """All writers hold the lock: no premature commit, no interleaving."""

    async def test_single_writer_cannot_flush_inflight_transaction(
        self, db: Database,
    ):
        entered = asyncio.Event()
        proceed = asyncio.Event()

        async def failing_atomic():
            try:
                async with db._atomic():
                    await db.db.execute(
                        "INSERT INTO sync_cursors (source, cursor, updated_at) "
                        "VALUES ('inflight', 'x', 'now')"
                    )
                    entered.set()
                    await proceed.wait()
                    raise RuntimeError("abort transaction")
            except RuntimeError:
                pass

        atomic_task = asyncio.create_task(failing_atomic())
        await asyncio.wait_for(entered.wait(), timeout=5)

        # A single-statement writer fired mid-transaction must block on the
        # lock (pre-fix it ran immediately and its commit() flushed the
        # in-flight 'inflight' row).
        solo_task = asyncio.create_task(db.set_sync_cursor("solo", "v"))
        await asyncio.sleep(0.05)
        assert not solo_task.done(), "unlocked writer interleaved with _atomic"

        proceed.set()
        await atomic_task
        await asyncio.wait_for(solo_task, timeout=5)

        # The solo write landed; the aborted transaction's row did not.
        assert await db.get_sync_cursor("solo") == "v"
        assert await db.get_sync_cursor("inflight") is None

    async def test_concurrent_writers_smoke(self, db: Database):
        async def single(i: int):
            await db.set_sync_cursor(f"src-{i}", str(i))

        async def multi(i: int):
            await db.upsert_task(f"task-{i}", f"tasks/task-{i}.md", f"Task {i}")

        await asyncio.gather(
            *(single(i) for i in range(25)),
            *(multi(i) for i in range(25)),
        )

        assert not db.db.in_transaction
        for i in range(25):
            assert await db.get_sync_cursor(f"src-{i}") == str(i)
        assert await db.count_tasks(status="all") == 25


@pytest.mark.asyncio
class TestWriteResultPlumbing:
    """lastrowid/rowcount survive the migration to _write()."""

    async def test_lastrowid_via_log_session_event(self, db: Database):
        await db.create_session("s1")
        first = await db.log_session_event("s1", "created")
        second = await db.log_session_event("s1", "connected")
        assert isinstance(first, int) and isinstance(second, int)
        assert second > first

    async def test_rowcount_via_claim_wakeup(self, db: Database):
        await db.create_session("s2")
        wid = await db.add_wakeup("s2", "wake", "2999-01-01T00:00:00+00:00")
        assert await db.claim_wakeup(wid) is True
        assert await db.claim_wakeup(wid) is False  # already fired

    async def test_rowcount_via_cancel_wakeups(self, db: Database):
        await db.create_session("s3")
        await db.add_wakeup("s3", "wake", "2999-01-01T00:00:00+00:00")
        assert await db.cancel_wakeups_for_session("s3") == 1
        assert await db.cancel_wakeups_for_session("s3") == 0
