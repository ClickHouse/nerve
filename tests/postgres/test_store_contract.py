# ruff: noqa: F401 — imported test classes are collected by pytest.
"""Run the existing operational-store contract against PostgreSQL."""

from tests.test_db import (
    TestSessionCRUD,
    TestUpdateSessionFields,
    TestListSessions,
    TestSessionEvents,
    TestChannelSessions,
    TestMessages,
    TestCleanupQueries,
    TestMemorizationQuery,
    TestMetadataBackwardCompat,
    TestTaskFtsJoinRegression,
    TestTaskFtsReseed,
    TestTagParsing,
    TestPatchRouteTagCanonicalization,
    TestFrontmatterParsing,
    TestPlanUpdate,
    TestDiagnosticsHelpers,
    TestConsumerCursors,
    TestCronLogSessions,
    TestCronLogPagination,
    TestCronLogSessionBackfill,
    TestSetCronLogSession,
)


import pytest
from tests.test_db import TestTaskSearch as SQLiteTaskSearch


@pytest.mark.asyncio
class TestPostgresTaskSearch(SQLiteTaskSearch):
    async def test_fts_table_exists(self, db):
        async with db.db.execute("SELECT to_regclass('nerve_pg.tasks_fts')") as cursor:
            assert (await cursor.fetchone())[0] is not None

    async def test_rebuild_fts(self, db):
        await self._create_task(db, "t1", "Some task", content="Durable content")
        await db.rebuild_fts()
        assert len(await db.search_tasks("Durable")) == 1
