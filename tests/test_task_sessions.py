"""Links between tasks and sessions: the creating session, the backfill, the API.

A task records the session that created it (``tasks.created_session_id``).
The task detail lists the sessions that created, implemented or updated the
task, and ``GET /api/sessions/{id}/tasks`` lists the tasks of a session.
These tests drive the real routes, engine, tool registry and database.
"""

from __future__ import annotations

import aiosqlite
import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from nerve.agent.engine import AgentEngine
from nerve.config import NerveConfig, set_config
from nerve.db import Database
from nerve.db.migrations import v066_task_created_session
from nerve.gateway.auth import create_session_token, hash_password, pin_jwt_secret
from nerve.gateway.routes import init_deps, register_all_routes

_SECRET = "test-secret-for-task-sessions-pad-32-bytes"


class _Install:
    def __init__(self, db: Database, engine: AgentEngine, account_id: str, actor_id: str):
        self.db = db
        self.engine = engine
        self.account_id = account_id
        self.actor_id = actor_id

    def headers(self) -> dict:
        return {"Authorization": f"Bearer {create_session_token(_SECRET, self.account_id)}"}

    def client(self) -> httpx.AsyncClient:
        app = FastAPI()
        app.include_router(register_all_routes())
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://nerve-test",
        )

    async def session(self, client: httpx.AsyncClient, title: str) -> str:
        response = await client.post("/api/sessions", headers=self.headers(), json={"title": title})
        assert response.status_code == 200, response.text
        return response.json()["id"]

    async def invoke(self, session_id: str, tool: str, args: dict):
        ctx = self.engine._build_tool_context(session_id)
        result = await self.engine.registry.invoke(tool, ctx, args)
        assert not result.is_error, result.text_content
        return result

    async def agent_task(self, session_id: str, title: str) -> str:
        result = await self.invoke(
            session_id, "task_create", {"title": title, "confirm_duplicate": True},
        )
        return result.structured["task_id"]

    async def rest_task(self, client: httpx.AsyncClient, title: str) -> str:
        response = await client.post(
            "/api/tasks", headers=self.headers(),
            json={"title": title, "confirm_duplicate": True},
        )
        assert response.status_code == 200, response.text
        return response.json()["task"]["id"]


@pytest_asyncio.fixture
async def install(tmp_path, open_identity_db, wire_identity_store):
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
        "auth": {"jwt_secret": _SECRET},
    })
    (config.workspace / "memory" / "tasks" / "active").mkdir(parents=True)
    set_config(config)
    pin_jwt_secret(_SECRET)
    database, identity = await open_identity_db(tmp_path / "nerve.db")
    wire_identity_store(database)
    engine = AgentEngine(config, database)
    init_deps(engine, database)
    await database.update_account_login(
        identity.owner_account_id, username="alice",
        credential=hash_password("correct-horse-battery-staple"),
    )
    try:
        yield _Install(database, engine, identity.owner_account_id, identity.owner_actor_id)
    finally:
        await database.close()
        set_config(NerveConfig())


# --------------------------------------------------------------------------- #
#  Migration                                                                   #
# --------------------------------------------------------------------------- #


async def _old_schema() -> aiosqlite.Connection:
    """The three tables the migration reads, as they are before it runs."""
    db = await aiosqlite.connect(":memory:")
    await db.executescript("""
        CREATE TABLE sessions (id TEXT PRIMARY KEY, title TEXT);
        CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT);
        CREATE TABLE task_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL,
            from_status TEXT, to_status TEXT NOT NULL,
            actor TEXT NOT NULL DEFAULT 'system', created_at TEXT NOT NULL
        );
    """)
    return db


async def _created_sessions(db: aiosqlite.Connection) -> dict[str, str | None]:
    async with db.execute("SELECT id, created_session_id FROM tasks ORDER BY id") as cursor:
        return {row[0]: row[1] async for row in cursor}


@pytest.mark.asyncio
async def test_the_backfill_takes_only_creation_actors_that_are_session_ids():
    db = await _old_schema()
    try:
        await db.executemany(
            "INSERT INTO sessions (id, title) VALUES (?, ?)",
            [("chat-1", "Chat one"), ("chat-2", "Chat two")],
        )
        # One task per kind of creation actor. Each creation event has a NULL
        # from_status. The actor ID of a person, the route and system origins
        # and the v044 seed rows are not session IDs. ``chat-gone`` is the ID
        # of a session that no longer exists.
        creators = {
            "t-session": "chat-1",
            "t-web": "web",
            "t-system": "system",
            "t-backfill": "backfill",
            "t-person": "actor-0f3e9a1c",
            "t-deleted": "chat-gone",
        }
        await db.executemany(
            "INSERT INTO tasks (id, title) VALUES (?, ?)",
            [(task_id, task_id) for task_id in [*creators, "t-updated"]],
        )
        events = [
            (task_id, None, "pending", actor, "2026-01-01T10:00:00+00:00")
            for task_id, actor in creators.items()
        ]
        # A session that only changed the status did not create the task.
        events += [
            ("t-updated", None, "pending", "web", "2026-01-01T10:00:00+00:00"),
            ("t-updated", "pending", "in_progress", "chat-2", "2026-01-01T11:00:00+00:00"),
        ]
        await db.executemany(
            "INSERT INTO task_events (task_id, from_status, to_status, actor, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            events,
        )

        await v066_task_created_session.up(db)

        assert await _created_sessions(db) == {
            "t-backfill": None,
            "t-deleted": None,
            "t-person": None,
            "t-session": "chat-1",
            "t-system": None,
            "t-updated": None,
            "t-web": None,
        }
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_the_backfill_reruns_without_change_and_keeps_a_stored_value():
    db = await _old_schema()
    try:
        await db.executemany(
            "INSERT INTO sessions (id, title) VALUES (?, ?)",
            [("chat-1", "Chat one"), ("chat-2", "Chat two")],
        )
        await db.executemany(
            "INSERT INTO tasks (id, title) VALUES (?, ?)", [("t-1", "One"), ("t-2", "Two")],
        )
        # Two creation events: the earliest one names the creator.
        await db.executemany(
            "INSERT INTO task_events (task_id, from_status, to_status, actor, created_at) "
            "VALUES (?, NULL, 'pending', ?, ?)",
            [
                ("t-1", "chat-2", "2026-01-02T10:00:00+00:00"),
                ("t-1", "chat-1", "2026-01-01T10:00:00+00:00"),
            ],
        )
        await v066_task_created_session.up(db)
        assert await _created_sessions(db) == {"t-1": "chat-1", "t-2": None}

        await db.execute("UPDATE tasks SET created_session_id = 'chat-2' WHERE id = 't-1'")
        await v066_task_created_session.up(db)
        assert await _created_sessions(db) == {"t-1": "chat-2", "t-2": None}
    finally:
        await db.close()


# --------------------------------------------------------------------------- #
#  The creating session                                                        #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_task_create_in_a_session_records_the_session(install):
    async with install.client() as client:
        creator = await install.session(client, "Plan the release")
        other = await install.session(client, "Weekly triage")
    task_id = await install.agent_task(creator, "Write the release notes")
    assert (await install.db.get_task(task_id))["created_session_id"] == creator

    # A change by another session keeps the creator.
    await install.invoke(other, "task_update", {"task_id": task_id, "status": "in_progress"})
    await install.invoke(other, "task_update", {"task_id": task_id, "note": "Started"})
    task = await install.db.get_task(task_id)
    assert task["created_session_id"] == creator
    assert task["created_session_title"] == "Plan the release"


@pytest.mark.asyncio
async def test_a_task_from_the_api_or_a_reindex_has_no_creating_session(install):
    async with install.client() as client:
        task_id = await install.rest_task(client, "Fix the login form")
    task = await install.db.get_task(task_id)
    assert task["created_session_id"] is None
    assert task["created_session_title"] is None

    await install.db.upsert_task(
        "t-reindexed", "memory/tasks/active/t-reindexed.md", "From disk", actor="system",
    )
    assert (await install.db.get_task("t-reindexed"))["created_session_id"] is None


@pytest.mark.asyncio
async def test_the_creating_session_title_falls_back_to_its_id_and_goes_with_it(install):
    async with install.client() as client:
        creator = await install.session(client, "Untitled for now")
    await install.db.update_session_fields(creator, {"title": ""})
    task_id = await install.agent_task(creator, "Rotate the API keys")
    assert (await install.db.get_task(task_id))["created_session_title"] == creator

    await install.db.delete_session(creator)
    task = await install.db.get_task(task_id)
    assert task["created_session_id"] == creator
    assert task["created_session_title"] is None


@pytest.mark.asyncio
async def test_the_list_search_and_board_reads_carry_the_session_title(install):
    async with install.client() as client:
        creator = await install.session(client, "Plan the release")
        await install.agent_task(creator, "Write the release notes")
        for path in ("/api/tasks", "/api/tasks/search?q=release"):
            response = await client.get(path, headers=install.headers())
            assert response.status_code == 200, response.text
            [task] = response.json()["tasks"]
            assert task["created_session_title"] == "Plan the release"
        response = await client.get("/api/tasks/board", headers=install.headers())
        cards = [t for lane in response.json()["lanes"] for t in lane["tasks"]]
        assert [t["created_session_title"] for t in cards] == ["Plan the release"]


# --------------------------------------------------------------------------- #
#  The sessions of a task, and the tasks of a session                          #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_task_detail_lists_its_sessions_once_each(install):
    async with install.client() as client:
        creator = await install.session(client, "Plan the release")
        updater = await install.session(client, "Weekly triage")
        gone = await install.session(client, "Deleted chat")
        task_id = await install.agent_task(creator, "Write the release notes")
        # The creator also changes the status: it stays "created".
        await install.invoke(creator, "task_update", {"task_id": task_id, "status": "in_progress"})
        await install.invoke(updater, "task_update", {"task_id": task_id, "status": "pending"})
        await install.invoke(gone, "task_update", {"task_id": task_id, "status": "in_progress"})
        await install.db.delete_session(gone)
        # A change through the API is no session.
        response = await client.patch(
            f"/api/tasks/{task_id}", headers=install.headers(), json={"status": "pending"},
        )
        assert response.status_code == 200, response.text
        impl = await install.session(client, "Implement: Write the release notes")
        await install.db.create_plan("plan-1", task_id, "Steps")
        await install.db.update_plan(
            "plan-1", status="implementing", impl_session_id=impl,
            reviewed_at="2026-01-01T10:00:00+00:00",
        )

        response = await client.get(f"/api/tasks/{task_id}", headers=install.headers())

    assert response.status_code == 200, response.text
    assert response.json()["sessions"] == [
        {"id": creator, "title": "Plan the release", "relation": "created"},
        {"id": impl, "title": "Implement: Write the release notes", "relation": "implemented"},
        {"id": updater, "title": "Weekly triage", "relation": "updated"},
    ]


@pytest.mark.asyncio
async def test_a_task_from_the_api_has_no_sessions(install):
    async with install.client() as client:
        task_id = await install.rest_task(client, "Fix the login form")
        response = await client.get(f"/api/tasks/{task_id}", headers=install.headers())
    assert response.json()["sessions"] == []


@pytest.mark.asyncio
async def test_the_session_tasks_route_lists_created_implemented_and_updated_tasks(install):
    async with install.client() as client:
        session = await install.session(client, "Plan the release")
        other = await install.session(client, "Weekly triage")
        created = await install.agent_task(session, "Write the release notes")
        updated = await install.agent_task(other, "Rotate the API keys")
        implemented = await install.rest_task(client, "Update the changelog")
        await install.agent_task(other, "Not touched by the session")
        await install.invoke(session, "task_update", {"task_id": updated, "status": "in_progress"})
        await install.db.create_plan("plan-1", implemented, "Steps")
        await install.db.update_plan("plan-1", status="implementing", impl_session_id=session)

        response = await client.get(f"/api/sessions/{session}/tasks", headers=install.headers())
        missing = await client.get("/api/sessions/no-such-session/tasks", headers=install.headers())

    assert response.status_code == 200, response.text
    relations = {t["id"]: t["relation"] for t in response.json()["tasks"]}
    assert relations == {created: "created", updated: "updated", implemented: "implemented"}
    assert missing.status_code == 404
