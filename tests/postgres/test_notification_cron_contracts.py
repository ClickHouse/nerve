"""Shared plan and diagnostics contracts against the DML-only PostgreSQL role."""

import json
import sqlite3

import pytest

from tests.test_task_similarity import TestTitleIndexCache  # noqa: F401
from tests.test_plan_review_flow import TestNotification, broadcasts  # noqa: F401
from tests.test_plan_actions import TestApprovePlan  # noqa: F401
from tests.test_usage_by_cron_job import TestUsageByCronJob  # noqa: F401
from tests.test_cron_health import (  # noqa: F401
    test_hide_empty_leaves_out_only_successful_runs_with_no_records,
    test_the_counts_cover_seven_days_in_one_query,
    test_the_latest_run_of_each_job,
    test_the_cleanup_keeps_the_latest_finished_run_of_each_job,
)
from tests.test_telegram_reply_routing import (  # noqa: F401
    test_record_and_lookup_hit,
    test_lookup_unknown_message_is_none,
    test_lookup_requires_matching_chat,
    test_same_message_id_in_two_chats_do_not_collide,
    test_re_record_replaces_the_session,
    test_cleanup_keeps_fresh_routes_and_removes_old_ones,
)

pytestmark = pytest.mark.asyncio


async def test_plan_notifications_ignore_malformed_and_unrelated_metadata(db):
    metadata = [
        json.dumps({"task_id": "target", "plan_id": "p"}),
        json.dumps({"task_id": "other", "plan_id": "p"}),
        json.dumps({"task_id": "target"}),
        '{"task_id":',
        None,
        "null",
    ]
    for index, value in enumerate(metadata):
        await db.create_notification(str(index), "session", "notify", "Plan")
        await db._write(
            "UPDATE notifications SET metadata=? WHERE id=?", (value, str(index))
        )
    assert await db.dismiss_plan_notifications("target") == 1
    for index in range(len(metadata)):
        row = await db.get_notification(str(index))
        assert row["status"] == ("dismissed" if index == 0 else "pending")
    assert await db.dismiss_plan_notifications("target") == 0


@pytest.mark.parametrize(
    "pattern",
    [
        "cron:?*",
        "[0-9][0-9]-[0-9a-f]",
        "*",
        "?",
        "[abc]",
        "[^a]",
        "[]a]",
        "[",
        "[]",
        r"a\b",
        "a.b",
        "a+b",
        "a(b)",
        "a|b",
        "a{b}",
        "a$b",
        "a^b",
        "a]b",
    ],
)
async def test_glob_matches_sqlite_without_changing_bound_patterns(db, pattern):
    values = [
        None,
        "",
        "a",
        "A",
        "b",
        "]",
        "cron:one",
        "cron:",
        "cron:x:y",
        "CRON:one",
        "12-f",
        "12-F",
        "12-ff",
        "a\nb",
        "a\n",
        r"a\b",
        "a.b",
        "a+b",
        "a(b)",
        "a|b",
        "a{b}",
        "a$b",
        "a^b",
        "a]b",
        "é",
    ]
    with sqlite3.connect(":memory:") as sqlite:
        for value in values:
            expected = sqlite.execute(
                "SELECT ? GLOB ?, ? NOT GLOB ?", (value, pattern, value, pattern)
            ).fetchone()
            async with db.db.execute(
                "SELECT ? GLOB ?, ? NOT GLOB ?", (value, pattern, value, pattern)
            ) as cursor:
                actual = await cursor.fetchone()
            assert tuple(actual) == expected, (value, pattern)
