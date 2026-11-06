"""Contracts added since the initial PostgreSQL schema."""

import os
import sqlite3

import pytest

from nerve.config import NerveConfig
from nerve.db.accounts import inspect_system_actor, read_system_actor_id
from nerve.db.actor_columns import ACTOR_COLUMNS
from nerve.identity import Attribution
from tests.test_channel_observation import TestBuffer  # noqa: F401
from tests.test_task_revisions import (
    TestEveryWriteAdvancesIt as RevisionContract,
    TestConditionalUpsert as UpsertContract,
)  # noqa: F401
from tests.test_db_agents import (  # noqa: F401
    seeded,
    test_upsert_round_trips_every_column,
    test_upsert_keeps_identity_and_birth,
    test_foreign_keys_and_unique_task,
    test_stop_attribution_must_name_an_actor,
    test_events_cursors_and_idempotent_insert,
    test_write_agent_batch_is_one_transaction,
    test_list_session_agents_keeps_live_and_newest,
    test_delete_session_removes_records_and_items,
    test_mark_live_agents_interrupted,
    test_mark_agents_notified,
    test_telemetry_retention_prunes_old_transcript_items,
)


pytestmark = pytest.mark.asyncio


class TestEveryWriteAdvancesIt(RevisionContract):
    async def test_rows_start_at_one(self, db):
        await db.upsert_task("a", "a.md", "A")
        await db._write(
            "INSERT INTO tasks (id,file_path,title) VALUES ('b','b.md','B')"
        )
        assert (await db.get_task("a"))["revision"] == 1
        assert (await db.get_task("b"))["revision"] == 1


class TestConditionalUpsert(UpsertContract):
    async def test_a_row_deleted_since_the_read_stays_deleted(self, db):
        await db.upsert_task("t", "t.md", "T")
        await db._write("DELETE FROM tasks WHERE id='t'")
        assert not await db.upsert_task("t", "t.md", "T", expect_revision=1)
        assert await db.get_task("t") is None


@pytest.mark.asyncio
async def test_actor_columns_and_rekey_are_scoped_without_ddl(db, tmp_path):
    import json

    config = NerveConfig(
        use_postgresql=True,
        postgresql_dsn=os.environ["NERVE_TEST_POSTGRES_DSN"],
        workflow_id=json.loads(db.scope)[1],
    )
    assert set(ACTOR_COLUMNS) <= set(await db._actor_ref_columns())
    assert (
        read_system_actor_id(tmp_path / "absent.db", config=config)
        == db.system_actor_id
    )
    assert inspect_system_actor(tmp_path / "absent.db", "claimed", config=config) == (
        db.system_actor_id,
        None,
    )
    preview = await db._preview_system_actor_rekey("claimed")
    assert preview.actor_id == "claimed"
    assert (await db._rekey_system_actor("claimed")).changed
    assert not (await db._rekey_system_actor("second")).changed
    with pytest.raises(sqlite3.IntegrityError):
        await db._write("UPDATE actor_refs SET id='illegal' WHERE kind='system'")
    with pytest.raises(sqlite3.IntegrityError):
        await db.upsert_external_actor("claimed", "Person")
    assert read_system_actor_id(tmp_path / "absent.db", config=config) == "claimed"


@pytest.mark.asyncio
async def test_new_attribution_blocks_rekey(db):
    await db.set_onboarding(
        "requested", session_id=None, actor_id=db.system_actor_id, expect=(None, None)
    )
    result = await db._rekey_system_actor("claimed")
    assert not result.changed
    assert result.blocker == "onboarding.actor_id refers to the current ID"
    assert (await db.get_onboarding())["actor_id"] == db.system_actor_id


@pytest.mark.asyncio
async def test_rekey_rolls_back_claim_on_failure(db, monkeypatch):
    original = db.db.execute

    def execute(query, params=()):
        if query.startswith("UPDATE actor_refs SET id="):
            raise RuntimeError("injected write failure")
        return original(query, params)

    monkeypatch.setattr(db.db, "execute", execute)
    old_id = db.system_actor_id
    with pytest.raises(RuntimeError, match="injected write failure"):
        await db._rekey_system_actor("claimed")
    assert await db._get_instance_secret("system_principal_id") is None
    assert (await db.get_actor_ref(old_id))["kind"] == "system"


@pytest.mark.asyncio
async def test_comment_revision_and_transition_keep_content(db):
    await db.upsert_task("t", "memory/tasks/t.md", "Task", content="original")
    revision = (await db.get_task("t"))["revision"]
    comment = await db.add_task_comment(
        "t", "Review this", attribution=Attribution(db.system_actor_id, None)
    )
    row = await db.get_task("t")
    assert row["comment_count"] == 1 and row["revision"] == revision + 1
    assert not await db.transition_task(
        "t", "done", expect_revision=revision, content="stale"
    )
    assert (await db.get_task("t"))["content"] == "original"
    assert await db.transition_task(
        "t", "done", expect_revision=row["revision"], content="new searchable content"
    )
    assert (await db.get_task("t"))["content"] == "new searchable content"
    assert (await db.search_tasks("searchable", status="all"))[0]["id"] == "t"
    assert await db.delete_task_comment("t", comment["id"])
    assert (await db.get_task("t"))["comment_count"] == 0


async def test_latest_delivery_and_session_cause_survive_reconnect(db):
    from nerve.db.postgres import PostgresDatabase
    import json

    await db.create_session("s", source="web", actor=None, cause=db.system_actor_id)
    await db.create_notification("n", "s", "question", "Question")
    await db.record_notification_delivery("n", "slack", "first", "1")
    await db.record_notification_delivery("n", "slack", "second", "2")
    await db._write("UPDATE notification_deliveries SET delivered_at='2026-01-01'")
    assert (await db.get_latest_notification_delivery("n", "slack"))[
        "message_id"
    ] == "2"
    _, workflow = json.loads(db.scope)
    reopened = PostgresDatabase(
        os.environ["NERVE_TEST_POSTGRES_DSN"], workflow=workflow
    )
    await reopened.connect()
    try:
        assert reopened.session_cause("s") == db.system_actor_id
        assert (await reopened.get_latest_notification_delivery("n", "slack"))[
            "target"
        ] == "second"
    finally:
        await reopened.close()
