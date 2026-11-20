"""New operational stores keep their shared contracts and workflow isolation."""

import json
import os
import sqlite3
from types import SimpleNamespace

import pytest
import pytest_asyncio

from nerve.db.postgres import PostgresDatabase
from nerve.widget_state import WidgetStateError
from tests.test_widget_state import TestStore as WidgetContract, SID, OTHER
from tests.test_web_push import TestStore as PushContract

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def chats(db):
    await db.create_session(SID, actor=None)
    await db.create_session(OTHER, actor=None)
    return db


@pytest_asyncio.fixture
async def people(db):
    await db.upsert_external_actor("alice", "Alice")
    await db.upsert_external_actor("bob", "Bob")
    return SimpleNamespace(db=db, alice="alice", bob="bob")


class TestWidgets(WidgetContract):
    async def test_a_session_that_does_not_exist_keeps_nothing(self, chats):
        with pytest.raises(WidgetStateError) as refused:
            await chats.save_widget_state("missing", "widget", [["key", "value"]], 8)
        assert (refused.value.code, refused.value.status) == ("session_not_found", 404)
        with pytest.raises(sqlite3.IntegrityError):
            await chats._write(
                "INSERT INTO widget_states (session_id,widget_id,items,size,updated_at) "
                "VALUES ('missing','widget','[]',0,'now')"
            )

    # Owner migration coverage lives in test_upgrades; shared tests use runtime DML.
    test_the_migration_makes_the_table_and_can_run_again = None


class TestPush(PushContract):
    test_the_migration_makes_the_table_and_can_run_again = None


async def test_new_stores_are_scoped_and_survive_reconnect(db):
    async def populate(store, label):
        await store.create_session("same-session", actor=None)
        await store.upsert_external_actor("same-actor", label)
        await store.save_widget_state(
            "same-session", "widget", [["value", label]], len(label) + 5
        )
        await store.record_reply_route("telegram", "chat", "1", label)
        await store.save_push_subscription(
            actor_id="same-actor",
            endpoint="https://push.example/device",
            p256dh=label,
            auth="test",
            level="all",
            user_agent="test",
        )

    other = PostgresDatabase(
        os.environ["NERVE_TEST_POSTGRES_DSN"],
        workflow=json.loads(db.scope)[1] + "-other",
    )
    await other.connect()
    try:
        await populate(db, "first")
        assert await other.lookup_reply_route("telegram", "chat", "1") is None
        assert await other.list_widget_states("same-session") == []
        assert await other.list_push_subscriptions() == []
        await populate(other, "second")
        await db.close()
        await db.connect()
        assert (await db.list_widget_states("same-session"))[0]["items"] == [
            ["value", "first"]
        ]
        assert await db.lookup_reply_route("telegram", "chat", "1") == "first"
        assert (await db.list_push_subscriptions())[0]["p256dh"] == "first"
        await other.delete_session("same-session")
        assert await other.list_widget_states("same-session") == []
        assert len(await db.list_widget_states("same-session")) == 1
    finally:
        await other.close()
