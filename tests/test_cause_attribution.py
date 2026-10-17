"""Actor and cause attribution: the session cause and task attribution.

A write records who made it (``actor_id``) and on whose behalf
(``caused_by_actor_id``). A person who changes something directly is both. A
tool call by the agent is the system actor's change, made for the cause of its
session (``sessions.caused_by_actor_id``), which is fixed when the session is
created. These tests drive the real routes, engine, tool registry and
database; only the model runtime is replaced, by a client factory that makes
tool calls the way the agent would inside a turn.
"""

from __future__ import annotations

import asyncio
import base64
import json
import threading
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from nerve.agent.backends import TurnCompleted
from nerve.agent.engine import AgentEngine
from nerve.agent.tools.handlers.plans import plan_approve_handler
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig, set_config
from nerve.db import Database
from nerve.db.actor_columns import ACTOR_COLUMNS
from nerve.gateway.auth import (
    ACTOR_CONTEXT_HEADER,
    AUTH_MODE_ENV,
    AUTH_MODE_EXTERNAL,
    create_session_token,
    hash_password,
    pin_auth_mode,
    pin_jwt_secret,
)
from nerve.gateway.routes import init_deps, register_all_routes
from nerve.identity import (
    CAUSE_FROM_ACTOR,
    UNATTRIBUTED,
    Actor,
    Attribution,
    actor_id_of,
    attribution_for,
    resolve_cause,
)
from nerve.mcp_server.http import build_ctx_resolver
from nerve.mcp_server.session import SatelliteSessionResolver
from nerve.migrate import bootstrap_identity

_SECRET = "test-secret-for-cause-attribution-pad-32b"
# One hash for both people: the tests sign in with tokens, and a hash per
# fixture is the slowest part of the setup.
_PASSWORD_HASH = hash_password("correct-horse-battery-staple")

# External-mode principals (synthetic).
ALICE_PRINCIPAL = "6f1c2a8e-3b4d-4e5f-8a9b-0c1d2e3f4a5b"
BOB_PRINCIPAL = "0a9b8c7d-6e5f-4a3b-9c2d-1e0f9a8b7c6d"

ALICE = Actor(actor_id="00000000-0000-4000-8000-0000000a11ce", kind="human")
SYSTEM = Actor(actor_id="00000000-0000-4000-8000-00000000515e", kind="system")


# --------------------------------------------------------------------------- #
#  The helper and the tool context                                            #
# --------------------------------------------------------------------------- #


class TestAttributionHelper:
    def test_the_cause_defaults_to_the_actor(self):
        assert attribution_for(ALICE) == Attribution(ALICE.actor_id, ALICE.actor_id)
        assert attribution_for(SYSTEM) == Attribution(SYSTEM.actor_id, SYSTEM.actor_id)
        assert attribution_for(None) == UNATTRIBUTED

    def test_an_explicit_cause_wins_and_none_stays_unknown(self):
        assert attribution_for(SYSTEM, ALICE) == Attribution(
            SYSTEM.actor_id, ALICE.actor_id,
        )
        assert attribution_for(SYSTEM, None) == Attribution(SYSTEM.actor_id, None)

    def test_a_cause_can_be_a_stored_id(self):
        assert resolve_cause(SYSTEM, ALICE.actor_id) == ALICE.actor_id
        assert resolve_cause(SYSTEM, CAUSE_FROM_ACTOR) == SYSTEM.actor_id

    @pytest.mark.parametrize("bad", ["", 42, object()])
    def test_a_reference_that_names_no_actor_is_refused(self, bad):
        with pytest.raises(ValueError):
            actor_id_of(bad)


class TestToolContextCause:
    def test_a_fixed_cause(self):
        ctx = ToolContext(session_id="s", actor=SYSTEM, cause=ALICE)
        assert ctx.cause_id == ALICE.actor_id
        assert ctx.attribution == Attribution(SYSTEM.actor_id, ALICE.actor_id)

    def test_a_callable_cause_is_read_at_the_time_of_the_call(self):
        current = {"cause": ALICE.actor_id}
        ctx = ToolContext(session_id="s", actor=SYSTEM, cause=lambda: current["cause"])
        assert ctx.cause_id == ALICE.actor_id
        current["cause"] = None
        assert ctx.attribution == Attribution(SYSTEM.actor_id, None)

    def test_a_context_without_identity_is_unattributed(self):
        assert ToolContext(session_id="s").attribution == UNATTRIBUTED


# --------------------------------------------------------------------------- #
#  Local-mode install with two people                                          #
# --------------------------------------------------------------------------- #


class _FakeRuntime:
    """A model runtime client that makes tool calls the way the agent does.

    The engine uses it through the client API (``start_turn``,
    ``receive_turn``, the idle-event calls). Each turn calls ``task_create``
    through the session's own tool context, as the Claude CLI calls the
    in-process MCP server, and then completes. ``before_tool_call`` runs
    first when it is set.
    """

    native_session_id = None

    def __init__(self, install: "_Install", session_id: str):
        self.install = install
        self.session_id = session_id
        self.before_tool_call = None

    def try_receive_idle_events(self):
        return None

    async def start_turn(self, turn_input) -> None:
        return None

    async def disconnect(self) -> None:
        return None

    async def receive_turn(self):
        if self.before_tool_call is not None:
            await self.before_tool_call()
        await self.install.agent_tool_call(self.session_id)
        yield TurnCompleted()


class _Install:
    def __init__(self, db: Database, engine: AgentEngine, config: NerveConfig):
        self.db = db
        self.engine = engine
        self.config = config
        self.people: dict[str, dict] = {}
        # One record per tool call of the fake runtime.
        self.turns: list[dict] = []
        self.runtimes: dict[str, _FakeRuntime] = {}

    @property
    def system_actor_id(self) -> str:
        return self.db.system_actor_id

    def actor_id(self, name: str) -> str:
        return self.people[name]["actor_id"]

    def headers(self, name: str) -> dict:
        token = create_session_token(_SECRET, self.people[name]["id"])
        return {"Authorization": f"Bearer {token}"}

    def app(self) -> FastAPI:
        app = FastAPI()
        app.include_router(register_all_routes())
        return app

    async def events(self, task_id: str) -> list[tuple]:
        return [
            (row["from_status"], row["to_status"], row["actor"],
             row["actor_id"], row["caused_by_actor_id"])
            for row in await self.db.list_task_events(task_id)
        ]

    async def user_rows(self, session_id: str) -> list[tuple]:
        return [
            (row["content"], row["actor_id"])
            for row in await self.db.get_messages(session_id)
            if row["role"] == "user"
        ]

    async def agent_tool_call(self, session_id: str) -> None:
        """The agent calls ``task_create`` in a turn of the session."""
        engine = self.engine
        cause = engine.session_cause(session_id)
        result = await engine.registry.invoke(
            "task_create", engine._build_tool_context(session_id),
            {"title": f"Made in {session_id} #{len(self.turns)}",
             "confirm_duplicate": True},
        )
        self.turns.append({
            "session_id": session_id,
            "cause": cause,
            "task_id": (result.structured or {}).get("task_id"),
        })

    def use_fake_runtime(self, monkeypatch) -> None:
        """Serve every session's turns from a :class:`_FakeRuntime`."""

        async def _client(session_id, source, model, **kwargs):
            if session_id not in self.runtimes:
                self.runtimes[session_id] = _FakeRuntime(self, session_id)
            return self.runtimes[session_id]

        monkeypatch.setattr(self.engine, "_get_or_create_client", _client)

    def runtime(self, session_id: str) -> _FakeRuntime:
        if session_id not in self.runtimes:
            self.runtimes[session_id] = _FakeRuntime(self, session_id)
        return self.runtimes[session_id]

    def turns_in(self, session_id: str) -> list[dict]:
        return [turn for turn in self.turns if turn["session_id"] == session_id]

    def turn_in(self, session_id: str) -> dict:
        (turn,) = self.turns_in(session_id)
        return turn

    async def session_cause_of(self, session_id: str) -> str | None:
        return (await self.db.get_session(session_id))["caused_by_actor_id"]


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
    installed = _Install(database, engine, config)
    await database.update_account_login(
        identity.owner_account_id, username="alice", credential=_PASSWORD_HASH,
    )
    installed.people["alice"] = {
        "id": identity.owner_account_id, "actor_id": identity.owner_actor_id,
    }
    bob = await database.create_managed_account(
        username="bob", credential=_PASSWORD_HASH, display_name="Bob",
    )
    installed.people["bob"] = bob
    try:
        yield installed
    finally:
        await database.close()
        set_config(NerveConfig())


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://nerve-test",
    )


async def _new_session(client, headers) -> str:
    return (await client.post("/api/sessions", headers=headers, json={})).json()["id"]


async def _chat(client, headers, session_id: str, message: str) -> None:
    response = await client.post(
        "/api/chat", headers=headers,
        json={"session_id": session_id, "message": message},
    )
    assert response.status_code == 200


# --------------------------------------------------------------------------- #
#  Schema                                                                      #
# --------------------------------------------------------------------------- #


async def _actor_ref_columns(db: Database) -> set[tuple[str, str]]:
    async with db.db.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ) as cursor:
        tables = [row[0] async for row in cursor]
    found = set()
    for table in tables:
        async with db.db.execute(f"PRAGMA foreign_key_list({table})") as cursor:
            for row in await cursor.fetchall():
                if row["table"] == "actor_refs":
                    found.add((table, row["from"]))
    return found


@pytest.mark.asyncio
async def test_the_registry_lists_every_attribution_column(install):
    assert len(set(ACTOR_COLUMNS)) == len(ACTOR_COLUMNS)
    in_schema = await _actor_ref_columns(install.db)
    # accounts.actor_id binds a login to its person; it is not attribution.
    assert in_schema - {("accounts", "actor_id")} == set(ACTOR_COLUMNS)


@pytest.mark.asyncio
async def test_v053_keeps_old_rows_null_and_is_idempotent(install):
    raw = install.db.db
    await raw.execute("ALTER TABLE sessions DROP COLUMN caused_by_actor_id")
    await raw.execute("ALTER TABLE task_events DROP COLUMN actor_id")
    await raw.execute("ALTER TABLE task_events DROP COLUMN caused_by_actor_id")
    await raw.execute(
        "INSERT INTO sessions (id, title, source, status, created_at, updated_at)"
        " VALUES ('old', 'Old chat', 'web', 'idle', '2020-01-01', '2020-01-01')"
    )
    await raw.execute(
        "INSERT INTO messages (session_id, role, content, created_at)"
        " VALUES ('old', 'user', 'from before', '2020-01-01')"
    )
    await raw.execute(
        "INSERT INTO task_events (task_id, from_status, to_status, actor, created_at)"
        " VALUES ('old-task', NULL, 'pending', 'web', '2020-01-01')"
    )
    await raw.commit()

    from nerve.db.migrations.v053_cause_attribution import up

    await up(raw)
    await up(raw)
    await raw.commit()
    assert await install.session_cause_of("old") is None
    assert await install.user_rows("old") == [("from before", None)]
    assert await install.events("old-task") == [(None, "pending", "web", None, None)]


# --------------------------------------------------------------------------- #
#  Task changes through REST                                                   #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_rest_task_changes_record_each_person(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    async with _client(install.app()) as client:
        created = await client.post(
            "/api/tasks", headers=install.headers("alice"),
            json={"title": "Write the report"},
        )
        assert created.status_code == 200
        task_id = created.json()["task"]["id"]
        moved = await client.post(
            f"/api/tasks/{task_id}/move", headers=install.headers("bob"),
            json={"status": "in_progress"},
        )
        assert moved.status_code == 200
        events = (
            await client.get(f"/api/tasks/{task_id}/events", headers=install.headers("bob"))
        ).json()["events"]

    assert [
        (e["to_status"], e["actor"], e["actor_id"], e["caused_by_actor_id"])
        for e in events
    ] == [
        ("pending", "web", alice, alice),
        ("in_progress", "web", bob, bob),
    ]


@pytest.mark.asyncio
async def test_rest_patch_with_content_records_one_change_for_the_person(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    async with _client(install.app()) as client:
        task_id = (await client.post(
            "/api/tasks", headers=install.headers("alice"),
            json={"title": "Edit and move"},
        )).json()["task"]["id"]
        patched = await client.patch(
            f"/api/tasks/{task_id}", headers=install.headers("bob"),
            json={"content": "# Edit and move\n\nNew body.\n", "status": "deferred"},
        )
        assert patched.status_code == 200
        finished = await client.patch(
            f"/api/tasks/{task_id}", headers=install.headers("alice"),
            json={"content": "# Edit and move\n\nBody again.\n", "status": "done"},
        )
        assert finished.status_code == 200
        assert (await install.db.get_task(task_id))["file_path"].startswith(
            "memory/tasks/done/",
        )
        reopened = await client.patch(
            f"/api/tasks/{task_id}", headers=install.headers("bob"),
            json={"content": "# Edit and move\n\nOnce more.\n", "status": "pending"},
        )
        assert reopened.status_code == 200

    assert await install.events(task_id) == [
        (None, "pending", "web", alice, alice),
        ("pending", "deferred", "web", bob, bob),
        ("deferred", "done", "web", alice, alice),
        ("done", "pending", "web", bob, bob),
    ]
    # Leaving done goes through the reopen path, so the file leaves done/.
    task = await install.db.get_task(task_id)
    assert task["file_path"].startswith("memory/tasks/active/")
    assert (install.config.workspace / task["file_path"]).is_file()


def _bob_moves_during_the_file_write(install, monkeypatch, module, task_id: str) -> None:
    """Bob moves the task to deferred while ``module`` writes the task file."""
    loop = asyncio.get_running_loop()
    real_write = module.write_task_file

    def write_while_bob_moves(path, content):
        real_write(path, content)
        asyncio.run_coroutine_threadsafe(
            install.db.move_task(
                task_id, status="deferred", actor="web",
                attribution=attribution_for(install.actor_id("bob")),
            ),
            loop,
        ).result(timeout=5)

    monkeypatch.setattr(module, "write_task_file", write_while_bob_moves)


async def _assert_bobs_move_stays(install, task_id: str) -> None:
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    assert await install.events(task_id) == [
        (None, "pending", "web", alice, alice),
        ("pending", "deferred", "web", bob, bob),
    ]
    assert (await install.db.get_task(task_id))["status"] == "deferred"


@pytest.mark.asyncio
@pytest.mark.parametrize("edit", [
    {"title": "Raced, renamed"},
    {"note": "a note"},
    {"deadline": "2026-12-01"},
    {"tags": "alpha"},
])
async def test_rest_patch_fields_keep_a_concurrent_move(install, monkeypatch, edit):
    from nerve.agent.tools.handlers import tasks as task_handlers

    async with _client(install.app()) as client:
        task_id = (await client.post(
            "/api/tasks", headers=install.headers("alice"), json={"title": "Raced"},
        )).json()["task"]["id"]
        _bob_moves_during_the_file_write(install, monkeypatch, task_handlers, task_id)
        response = await client.patch(
            f"/api/tasks/{task_id}", headers=install.headers("alice"), json=edit,
        )
    assert response.status_code == 200
    await _assert_bobs_move_stays(install, task_id)


@pytest.mark.asyncio
async def test_task_write_keeps_a_concurrent_move(install, monkeypatch):
    from nerve.agent.tools.handlers import tasks as task_handlers

    async with _client(install.app()) as client:
        task_id = (await client.post(
            "/api/tasks", headers=install.headers("alice"), json={"title": "Raced"},
        )).json()["task"]["id"]
    ctx = ToolContext(
        session_id="alices-turn", workspace=install.config.workspace,
        db=install.db, config=install.config, engine=install.engine,
        actor=install.db.system_actor, cause=install.actor_id("alice"),
    )
    await task_handlers.task_read_handler(ctx, {"task_id": task_id})
    _bob_moves_during_the_file_write(install, monkeypatch, task_handlers, task_id)
    result = await task_handlers.task_write_handler(
        ctx, {"task_id": task_id, "content": "# Raced\n\nWritten by the agent.\n"},
    )
    assert not result.is_error
    await _assert_bobs_move_stays(install, task_id)


@pytest.mark.asyncio
async def test_rest_patch_content_keeps_a_concurrent_move(install, monkeypatch):
    """A move that lands while PATCH writes the file is not undone.

    Alice saves new content while Bob moves the card. The content save must
    not write back the status that it read before Bob's move.
    """
    from nerve.gateway.routes import tasks as tasks_route

    async with _client(install.app()) as client:
        task_id = (await client.post(
            "/api/tasks", headers=install.headers("alice"), json={"title": "Raced"},
        )).json()["task"]["id"]
        _bob_moves_during_the_file_write(install, monkeypatch, tasks_route, task_id)
        response = await client.patch(
            f"/api/tasks/{task_id}", headers=install.headers("alice"),
            json={"content": "# Raced\n\nNew body.\n"},
        )
    assert response.status_code == 200
    await _assert_bobs_move_stays(install, task_id)
    task = await install.db.get_task(task_id)
    assert "New body." in (install.config.workspace / task["file_path"]).read_text()


def _pause_file_writes(monkeypatch, module) -> tuple[threading.Event, threading.Event]:
    """Make ``module``'s task file write wait until the test releases it."""
    entered, release = threading.Event(), threading.Event()
    real_write = module.write_task_file

    def paused_write(path, content):
        entered.set()
        assert release.wait(5), "the test never released the write"
        real_write(path, content)

    monkeypatch.setattr(module, "write_task_file", paused_write)
    return entered, release


async def _move_while_the_edit_writes(client, install, task_id, status, edit, entered, release):
    """Start ``edit``, move the task while its file write waits, then finish."""
    editing = asyncio.create_task(edit)
    assert await asyncio.to_thread(entered.wait, 5)
    moving = asyncio.create_task(client.post(
        f"/api/tasks/{task_id}/move", headers=install.headers("bob"),
        json={"status": status},
    ))
    await asyncio.sleep(0.2)
    # The move waits for the edit: both change the file and the row.
    assert not moving.done()
    release.set()
    return await editing, await moving


def _task_file(install, lane: str, task_id: str):
    return install.config.workspace / "memory" / "tasks" / lane / f"{task_id}.md"


@pytest.mark.asyncio
async def test_an_agent_edit_and_a_move_into_done_do_not_lose_the_edit(
    install, monkeypatch,
):
    from nerve.agent.tools.handlers import tasks as task_handlers

    async with _client(install.app()) as client:
        task_id = (await client.post(
            "/api/tasks", headers=install.headers("alice"), json={"title": "Raced"},
        )).json()["task"]["id"]
        ctx = ToolContext(
            session_id="alices-turn", workspace=install.config.workspace,
            db=install.db, config=install.config, engine=install.engine,
            actor=install.db.system_actor, cause=install.actor_id("alice"),
        )
        await task_handlers.task_read_handler(ctx, {"task_id": task_id})
        entered, release = _pause_file_writes(monkeypatch, task_handlers)
        written, moved = await _move_while_the_edit_writes(
            client, install, task_id, "done",
            task_handlers.task_write_handler(
                ctx, {"task_id": task_id, "content": "# Raced\n\nAgent edit.\n"},
            ),
            entered, release,
        )

    assert not written.is_error and moved.status_code == 200
    task = await install.db.get_task(task_id)
    assert task["status"] == "done"
    assert task["file_path"] == f"memory/tasks/done/{task_id}.md"
    assert "Agent edit." in _task_file(install, "done", task_id).read_text()
    assert not _task_file(install, "active", task_id).exists()


@pytest.mark.asyncio
async def test_a_rest_title_edit_and_a_move_into_done_do_not_lose_the_edit(
    install, monkeypatch,
):
    from nerve.agent.tools.handlers import tasks as task_handlers

    async with _client(install.app()) as client:
        task_id = (await client.post(
            "/api/tasks", headers=install.headers("alice"), json={"title": "Raced"},
        )).json()["task"]["id"]
        entered, release = _pause_file_writes(monkeypatch, task_handlers)
        patched, moved = await _move_while_the_edit_writes(
            client, install, task_id, "done",
            client.patch(
                f"/api/tasks/{task_id}", headers=install.headers("alice"),
                json={"title": "Raced and renamed"},
            ),
            entered, release,
        )

    assert patched.status_code == 200 and moved.status_code == 200
    task = await install.db.get_task(task_id)
    assert (task["status"], task["title"]) == ("done", "Raced and renamed")
    assert "# Raced and renamed" in _task_file(install, "done", task_id).read_text()
    assert not _task_file(install, "active", task_id).exists()


@pytest.mark.asyncio
async def test_a_rest_content_edit_and_a_move_out_of_done_do_not_lose_the_edit(
    install, monkeypatch,
):
    from nerve.gateway.routes import tasks as tasks_route

    async with _client(install.app()) as client:
        task_id = (await client.post(
            "/api/tasks", headers=install.headers("alice"), json={"title": "Raced"},
        )).json()["task"]["id"]
        await client.post(
            f"/api/tasks/{task_id}/move", headers=install.headers("alice"),
            json={"status": "done"},
        )
        entered, release = _pause_file_writes(monkeypatch, tasks_route)
        patched, moved = await _move_while_the_edit_writes(
            client, install, task_id, "pending",
            client.patch(
                f"/api/tasks/{task_id}", headers=install.headers("alice"),
                json={"content": "# Raced\n\nEdited while done.\n"},
            ),
            entered, release,
        )

    assert patched.status_code == 200 and moved.status_code == 200
    task = await install.db.get_task(task_id)
    assert task["status"] == "pending"
    assert task["file_path"] == f"memory/tasks/active/{task_id}.md"
    assert "Edited while done." in _task_file(install, "active", task_id).read_text()
    assert not _task_file(install, "done", task_id).exists()


@pytest.mark.asyncio
async def test_task_status_definition_changes_record_actor_and_cause(install):
    from nerve.agent.tools.handlers.tasks import task_status_create_handler

    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    async with _client(install.app()) as client:
        created = await client.post(
            "/api/task-statuses", headers=install.headers("bob"),
            json={"name": "review", "label": "Review", "color": "#123456"},
        )
        assert created.status_code == 200
        updated = await client.patch(
            "/api/task-statuses/review", headers=install.headers("alice"),
            json={"label": "In review"},
        )
        assert updated.status_code == 200
        reordered = await client.post(
            "/api/task-statuses/reorder", headers=install.headers("bob"),
            json={"names": ["review", "pending"]},
        )
        assert reordered.status_code == 200
        deleted = await client.delete(
            "/api/task-statuses/review", headers=install.headers("alice"),
        )
        assert deleted.status_code == 200

    # The agent adds a status in a turn that Alice started.
    ctx = ToolContext(
        session_id="alices-turn", db=install.db,
        actor=install.db.system_actor, cause=alice,
    )
    await task_status_create_handler(ctx, {"name": "blocked"})

    events = await install.db.list_task_status_events()
    assert [
        (e["change"], e["status_name"], e["actor_id"], e["caused_by_actor_id"])
        for e in events
    ] == [
        ("created", "review", bob, bob),
        ("updated", "review", alice, alice),
        ("reordered", None, bob, bob),
        ("deleted", "review", alice, alice),
        ("created", "blocked", install.system_actor_id, alice),
    ]
    assert json.loads(events[1]["detail"]) == {"label": "In review"}
    assert json.loads(events[2]["detail"])["order"][:2] == ["review", "pending"]


# --------------------------------------------------------------------------- #
#  The session cause: stored once, used by every agent action in the session  #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_person_creates_and_forks_sessions_for_themselves(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    async with _client(install.app()) as client:
        created = (
            await client.post("/api/sessions", headers=install.headers("alice"), json={})
        ).json()
        assert created["caused_by_actor_id"] == alice
        # REST chat without a session opens the API channel's session.
        await client.post(
            "/api/chat", headers=install.headers("bob"), json={"message": "hi"},
        )
        await install.db.update_session_fields(created["id"], {"sdk_session_id": "native-1"})
        fork = (await client.post(
            "/api/sessions/fork", headers=install.headers("bob"),
            json={"source_session_id": created["id"]},
        )).json()

    api_session = await install.engine.sessions.get_last_session("api:default")
    assert await install.session_cause_of(api_session) == bob
    assert fork["caused_by_actor_id"] == bob


@pytest.mark.asyncio
async def test_a_tool_change_in_alices_session_is_for_alice_whoever_sends_the_turn(
    install, monkeypatch,
):
    """A shared session: every turn's agent work is for the session's starter."""
    install.use_fake_runtime(monkeypatch)
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    async with _client(install.app()) as client:
        session_id = await _new_session(client, install.headers("alice"))
        await _chat(client, install.headers("alice"), session_id, "first")
        await _chat(client, install.headers("bob"), session_id, "second")

    assert [turn["cause"] for turn in install.turns_in(session_id)] == [alice, alice]
    for turn in install.turns_in(session_id):
        assert await install.events(turn["task_id"]) == [
            (None, "pending", session_id, install.system_actor_id, alice),
        ]
    # Each message still names who wrote it.
    assert await install.user_rows(session_id) == [("first", alice), ("second", bob)]
    assert install.engine.session_cause(session_id) == alice


@pytest.mark.asyncio
async def test_a_cron_sessions_tool_change_is_system_for_system(install, monkeypatch):
    install.use_fake_runtime(monkeypatch)
    await install.engine.run_cron(job_id="nightly", prompt="tidy the board")

    turn = install.turns[0]
    system = install.system_actor_id
    assert await install.session_cause_of(turn["session_id"]) == system
    assert turn["cause"] == system
    assert await install.events(turn["task_id"]) == [
        (None, "pending", turn["session_id"], system, system),
    ]


@pytest.mark.asyncio
async def test_concurrent_sessions_of_two_people_keep_their_causes(install, monkeypatch):
    install.use_fake_runtime(monkeypatch)
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    both_in_flight = asyncio.Barrier(2)

    async def _wait_for_the_other_turn():
        await asyncio.wait_for(both_in_flight.wait(), timeout=5)

    async with _client(install.app()) as client:
        hers = await _new_session(client, install.headers("alice"))
        his = await _new_session(client, install.headers("bob"))
        install.runtime(hers).before_tool_call = _wait_for_the_other_turn
        install.runtime(his).before_tool_call = _wait_for_the_other_turn
        await asyncio.gather(
            _chat(client, install.headers("alice"), hers, "from Alice"),
            _chat(client, install.headers("bob"), his, "from Bob"),
        )

    for session_id, person in ((hers, alice), (his, bob)):
        turn = install.turn_in(session_id)
        assert turn["cause"] == person
        assert await install.events(turn["task_id"]) == [
            (None, "pending", session_id, install.system_actor_id, person),
        ]


@pytest.mark.asyncio
async def test_channel_input_with_no_person_has_no_cause(install, monkeypatch):
    install.use_fake_runtime(monkeypatch)
    await install.engine.run(
        "chat-app", "hello from a chat app", source="telegram", actor=None,
    )

    turn = install.turn_in("chat-app")
    assert await install.session_cause_of("chat-app") is None
    assert turn["cause"] is None
    assert await install.events(turn["task_id"]) == [
        (None, "pending", "chat-app", install.system_actor_id, None),
    ]


@pytest.mark.asyncio
async def test_a_run_cause_applies_only_when_the_run_creates_the_session(
    install, monkeypatch,
):
    install.use_fake_runtime(monkeypatch)
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    system = install.db.system_actor
    await install.engine.run("leg", "start", source="workflow", actor=system, cause=alice)
    await install.engine.run("leg", "go on", source="workflow", actor=system, cause=bob)
    await install.engine.run("unknown", "start", source="workflow", actor=system, cause=None)

    assert await install.session_cause_of("leg") == alice
    assert [turn["cause"] for turn in install.turns_in("leg")] == [alice, alice]
    assert await install.session_cause_of("unknown") is None
    assert install.turn_in("unknown")["cause"] is None


@pytest.mark.asyncio
async def test_resume_after_restart_keeps_the_session_cause(
    install, monkeypatch, tmp_path,
):
    install.use_fake_runtime(monkeypatch)
    alice = install.actor_id("alice")
    async with _client(install.app()) as client:
        session_id = await _new_session(client, install.headers("alice"))
        await _chat(client, install.headers("bob"), session_id, "long job")
    await install.db.update_session_fields(session_id, {"sdk_session_id": "native-1"})
    # A restart: the new process has read no session row yet.
    install.engine._session_causes.clear()
    assert install.engine.session_cause(session_id) is None

    queue = tmp_path / "resume-after-restart"
    queue.write_text(f"{session_id}\n")
    with patch("nerve.agent.engine.RESUME_QUEUE_FILE", queue):
        assert await install.engine.resume_enrolled_sessions() == 1

    resumed = install.turns_in(session_id)[-1]
    assert resumed["cause"] == alice
    assert await install.events(resumed["task_id"]) == [
        (None, "pending", session_id, install.system_actor_id, alice),
    ]


@pytest.mark.asyncio
async def test_run_later_and_fork_keep_who_wrote_each_message(install):
    alice = install.actor_id("alice")
    async with _client(install.app()) as client:
        session_id = await _new_session(client, install.headers("alice"))
        await client.post(
            "/api/sessions/run-later", headers=install.headers("alice"),
            json={"session_id": session_id, "message": "later", "delay": "none"},
        )
        await install.db.update_session_fields(session_id, {"sdk_session_id": "native-1"})
        fork = await client.post(
            "/api/sessions/fork", headers=install.headers("bob"),
            json={"source_session_id": session_id},
        )

    assert await install.user_rows(session_id) == [("later", alice)]
    assert await install.user_rows(fork.json()["id"]) == [("later", alice)]


# --------------------------------------------------------------------------- #
#  Sessions that plans start                                                   #
# --------------------------------------------------------------------------- #


def _record_runs(install: _Install, monkeypatch) -> list[dict]:
    runs: list[dict] = []
    ran = asyncio.Event()

    async def _run(*args, **kwargs):
        runs.append(kwargs)
        ran.set()
        return ""

    monkeypatch.setattr(install.engine, "run", _run)
    install.ran = ran  # type: ignore[attr-defined]
    return runs


async def _pending_plan(install: _Install, suffix: str) -> str:
    task_id, plan_id = f"task-{suffix}", f"plan-{suffix}"
    task_file = install.config.workspace / f"{task_id}.md"
    task_file.write_text("# Plan task\n", encoding="utf-8")
    await install.db.upsert_task(
        task_id=task_id, file_path=task_file.name, title="Plan task",
        status="pending", content="# Plan task\n",
    )
    await install.db.create_plan(
        plan_id=plan_id, task_id=task_id, content="Ship it",
        session_id=f"planner-{suffix}", version=1,
    )
    return plan_id


@pytest.mark.asyncio
async def test_rest_plan_approval_starts_a_session_for_the_approver(install, monkeypatch):
    runs = _record_runs(install, monkeypatch)
    plan_id = await _pending_plan(install, "rest")
    async with _client(install.app()) as client:
        response = await client.post(
            f"/api/plans/{plan_id}/approve", headers=install.headers("bob"),
        )
    assert response.status_code == 200
    await asyncio.wait_for(install.ran.wait(), timeout=5)
    impl = response.json()["impl_session_id"]
    assert await install.session_cause_of(impl) == install.actor_id("bob")
    assert runs[0]["actor"] == install.db.system_actor


@pytest.mark.asyncio
async def test_tool_plan_approval_starts_a_session_for_the_approving_sessions_cause(
    install, monkeypatch,
):
    runs = _record_runs(install, monkeypatch)
    plan_id = await _pending_plan(install, "tool")
    alice = install.actor_id("alice")
    # The agent approves the plan in a session that Alice started.
    ctx = ToolContext(
        session_id="alices-session", workspace=install.config.workspace,
        db=install.db, config=install.config, engine=install.engine,
        actor=install.db.system_actor, cause=alice,
    )
    await plan_approve_handler(ctx, {"plan_id": plan_id})
    await asyncio.wait_for(install.ran.wait(), timeout=5)
    impl = (await install.db.get_plan(plan_id))["impl_session_id"]
    assert await install.session_cause_of(impl) == alice
    assert runs[0]["actor"] == install.db.system_actor


@pytest.mark.asyncio
async def test_a_revision_request_creates_the_planner_session_for_the_requester(
    install, monkeypatch,
):
    _record_runs(install, monkeypatch)
    plan_id = await _pending_plan(install, "revise")
    async with _client(install.app()) as client:
        response = await client.post(
            f"/api/plans/{plan_id}/revise", headers=install.headers("bob"),
            json={"feedback": "add tests"},
        )
    assert response.status_code == 200
    await asyncio.wait_for(install.ran.wait(), timeout=5)
    assert await install.session_cause_of("planner-revise") == install.actor_id("bob")


# --------------------------------------------------------------------------- #
#  The MCP endpoint                                                            #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_mcp_contexts_carry_the_system_actor_and_the_session_cause(
    install, monkeypatch,
):
    resolve = build_ctx_resolver(install.engine, SatelliteSessionResolver(install.db))
    rctx = SimpleNamespace(request=None, session=None)

    # A satellite session: its agent runs outside this instance.
    satellite = await resolve(rctx)
    assert await install.session_cause_of(satellite.session_id) is None
    assert satellite.attribution == Attribution(install.system_actor_id, None)

    # A Codex session that this instance runs, started by Alice.
    await install.engine.sessions.get_or_create(
        "codex-session", source="web",
        actor=Actor(actor_id=install.actor_id("alice"), kind="human"),
    )
    monkeypatch.setattr(
        "nerve.mcp_server.http._bound_identity_from_request",
        lambda config, rctx: ("codex-session", {}),
    )
    bound = await resolve(rctx)
    assert bound.attribution == Attribution(
        install.system_actor_id, install.actor_id("alice"),
    )


# --------------------------------------------------------------------------- #
#  The actor directory                                                         #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_actor_directory_names_every_id_in_every_registered_column(
    install, monkeypatch,
):
    install.use_fake_runtime(monkeypatch)
    async with _client(install.app()) as client:
        session_id = await _new_session(client, install.headers("alice"))
        await _chat(client, install.headers("bob"), session_id, "make a task")
        await client.post(
            "/api/tasks", headers=install.headers("alice"), json={"title": "Direct"},
        )
        directory = {
            row["id"]
            for row in (
                await client.get("/api/actors", headers=install.headers("alice"))
            ).json()["actors"]
        }

    referenced: dict[tuple[str, str], set[str]] = {}
    for table, column in ACTOR_COLUMNS:
        async with install.db.db.execute(
            f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL"
        ) as cursor:
            referenced[(table, column)] = {row[0] async for row in cursor}
        assert referenced[(table, column)] <= directory, (table, column)
    # The scenario fills each column that this change adds.
    for pair in (
        ("sessions", "caused_by_actor_id"),
        ("task_events", "actor_id"),
        ("task_events", "caused_by_actor_id"),
    ):
        assert referenced[pair], pair


# --------------------------------------------------------------------------- #
#  External mode                                                               #
# --------------------------------------------------------------------------- #


def _b64url(value: object) -> str:
    return base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=").decode()


def _as(principal_id: str, name: str) -> dict:
    payload = {"principal_id": principal_id, "profile": {"display_name": name}}
    token = _b64url({"alg": "ES256", "typ": "JWT"}) + "." + _b64url(payload) + ".x"
    return {ACTOR_CONTEXT_HEADER: token}


@pytest_asyncio.fixture
async def external_install(tmp_path, monkeypatch, wire_identity_store):
    monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
    pin_auth_mode(AUTH_MODE_EXTERNAL)
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
        "auth": {"jwt_secret": _SECRET},
    })
    (config.workspace / "memory" / "tasks" / "active").mkdir(parents=True)
    set_config(config)
    database = Database(tmp_path / "nerve.db")
    await database.connect()
    try:
        await bootstrap_identity(database, config)
        wire_identity_store(database)
        engine = AgentEngine(config, database)
        init_deps(engine, database)
        yield _Install(database, engine, config)
    finally:
        await database.close()
        set_config(NerveConfig())


@pytest.mark.asyncio
async def test_external_mode_records_principals_as_actor_and_cause(
    external_install, monkeypatch,
):
    install = external_install
    install.use_fake_runtime(monkeypatch)
    alice, bob = _as(ALICE_PRINCIPAL, "Alice"), _as(BOB_PRINCIPAL, "Bob")
    async with _client(install.app()) as client:
        task_id = (await client.post(
            "/api/tasks", headers=alice, json={"title": "Hosted task"},
        )).json()["task"]["id"]
        await client.post(
            f"/api/tasks/{task_id}/move", headers=bob, json={"status": "deferred"},
        )
        session_id = await _new_session(client, bob)
        await _chat(client, bob, session_id, "make a task")
        await _chat(client, alice, session_id, "and one more")

    assert await install.events(task_id) == [
        (None, "pending", "web", ALICE_PRINCIPAL, ALICE_PRINCIPAL),
        ("pending", "deferred", "web", BOB_PRINCIPAL, BOB_PRINCIPAL),
    ]
    assert await install.session_cause_of(session_id) == BOB_PRINCIPAL
    for turn in install.turns_in(session_id):
        assert turn["cause"] == BOB_PRINCIPAL
        assert await install.events(turn["task_id"]) == [
            (None, "pending", session_id, install.system_actor_id, BOB_PRINCIPAL),
        ]
