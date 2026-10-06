# ruff: noqa: F401 — imported test classes are collected by pytest.
"""Domain state transitions use the same contract with either backend."""

from tests.test_task_statuses import TestTaskStatusStore, TestReorderTaskStatuses
from tests.test_task_events import TestRecording, TestStatusEntryTimes
from tests.test_task_position import (
    TestNewTaskRanking,
    TestMoveTask,
    TestCrossLaneMove,
    TestRenormalization,
)
from tests.test_task_upsert_preserve import TestOmit, TestExplicitClear, TestSearchIndex
from tests.test_workflow_runs import TestWorkflowRunStore
from tests.test_review_loops import TestReviewLoopStore


from tests.test_db_retention import (
    TestCompaction,
    TestTelemetryPrune,
    TestFileSnapshotPrune,
    TestRunRetention,
)

import pytest
from tests.test_task_events import TestNoOpsAreNotRecorded as SQLiteNoOps


@pytest.mark.asyncio
class TestPostgresNoOps(SQLiteNoOps):
    async def test_reindex_records_an_orphan_correction(self, db, tmp_path):
        from nerve.tasks.manager import TaskManager

        task_file = tmp_path / "memory/tasks/done/t1.md"
        task_file.parent.mkdir(parents=True)
        task_file.write_text("# stale cached task")
        await db.upsert_task(
            "t1",
            "memory/tasks/active/t1.md",
            "Current",
            status="in_progress",
            content="# Current",
        )
        db.workspace = tmp_path
        await TaskManager(tmp_path, db).reindex()
        assert (await db.get_task("t1"))["status"] == "in_progress"
        assert (tmp_path / "memory/tasks/active/t1.md").read_text() == "# Current"
