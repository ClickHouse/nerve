"""Actor and cause attribution: the turn cause plumbing and task attribution.

A write records who made it (``actor_id``) and on whose behalf
(``caused_by_actor_id``). A person who changes something directly is both. A
tool call by the agent is the system actor's change, made for the cause of the
turn it runs in. These tests drive the real routes, engine, tool registry and
database; only the model runtime is replaced, by a client factory that makes
tool calls the way the agent would inside the turn.
"""

from __future__ import annotations

import asyncio
import base64
import json
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

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


class _Install:
    def __init__(self, db: Database, engine: AgentEngine, config: NerveConfig):
        self.db = db
        self.engine = engine
        self.config = config
        self.people: dict[str, dict] = {}
        # Records of the model runtime stand-in, one per turn.
        self.turns: list[dict] = []

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
            (row["content"], row["actor_id"], row["caused_by_actor_id"])
            for row in await self.db.get_messages(session_id)
            if row["role"] == "user"
        ]

    def agent_creates_a_task_in_each_turn(self, monkeypatch, barrier: int = 0) -> None:
        """Replace the model runtime with an agent that calls ``task_create``.

        The stand-in runs where the real client is fetched, inside the turn,
        and calls the tool through the session's own tool context, like the
        Claude backend's in-process MCP server. With ``barrier`` > 0, each
        turn waits until that many turns are in flight before its tool call.
        """
        engine = self.engine
        arrived = 0
        all_in = asyncio.Event()

        async def _agent(session_id, source, model, **kwargs):
            nonlocal arrived
            cause_at_start = engine.session_cause(session_id)
            if barrier:
                arrived += 1
                if arrived >= barrier:
                    all_in.set()
                await asyncio.wait_for(all_in.wait(), timeout=5)
            ctx = engine._build_tool_context(session_id)
            result = await engine.registry.invoke(
                "task_create", ctx,
                {"title": f"Made in {session_id}", "confirm_duplicate": True},
            )
            self.turns.append({
                "session_id": session_id,
                "cause": cause_at_start,
                "task_id": (result.structured or {}).get("task_id"),
            })
            raise RuntimeError("no model in this test")

        monkeypatch.setattr(engine, "_get_or_create_client", _agent)

    def turn_in(self, session_id: str) -> dict:
        return next(turn for turn in self.turns if turn["session_id"] == session_id)


@pytest_asyncio.fixture
async def install(tmp_path, open_identity_db, wire_identity_store):
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
        "auth": {"jwt_secret": _SECRET},
        "sessions": {"star_project_hook": True},
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
    await raw.execute("ALTER TABLE messages DROP COLUMN caused_by_actor_id")
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
    assert await install.user_rows("old") == [("from before", None, None)]
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


@pytest.mark.asyncio
async def test_task_status_definition_changes_take_the_person(install, caplog):
    caplog.set_level("INFO", logger="nerve.gateway.routes.tasks")
    async with _client(install.app()) as client:
        response = await client.post(
            "/api/task-statuses", headers=install.headers("bob"),
            json={"name": "review"},
        )
    assert response.status_code == 200
    assert f"by actor {install.actor_id('bob')}" in caplog.text


# --------------------------------------------------------------------------- #
#  Turns: the cause on the user row, in the engine and on tool changes         #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_tool_change_inside_a_persons_turn_is_system_for_that_person(
    install, monkeypatch,
):
    install.agent_creates_a_task_in_each_turn(monkeypatch)
    bob = install.actor_id("bob")
    async with _client(install.app()) as client:
        session_id = await _new_session(client, install.headers("bob"))
        await _chat(client, install.headers("bob"), session_id, "make a task")

    turn = install.turn_in(session_id)
    assert turn["cause"] == bob
    assert await install.user_rows(session_id) == [("make a task", bob, bob)]
    assert await install.events(turn["task_id"]) == [
        (None, "pending", session_id, install.system_actor_id, bob),
    ]
    # Autonomous CLI turns between runs keep the last cause.
    assert install.engine.session_cause(session_id) == bob


@pytest.mark.asyncio
async def test_a_cron_turns_tool_change_is_system_for_system(install, monkeypatch):
    install.agent_creates_a_task_in_each_turn(monkeypatch)
    await install.engine.run_cron(job_id="nightly", prompt="tidy the board")

    turn = install.turns[0]
    system = install.system_actor_id
    assert turn["cause"] == system
    assert await install.user_rows(turn["session_id"]) == [
        ("tidy the board", system, system),
    ]
    assert await install.events(turn["task_id"]) == [
        (None, "pending", turn["session_id"], system, system),
    ]


@pytest.mark.asyncio
async def test_concurrent_turns_of_two_people_keep_their_causes(install, monkeypatch):
    install.agent_creates_a_task_in_each_turn(monkeypatch, barrier=2)
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    async with _client(install.app()) as client:
        hers = await _new_session(client, install.headers("alice"))
        his = await _new_session(client, install.headers("bob"))
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
async def test_input_with_no_person_has_no_cause(install, monkeypatch):
    install.agent_creates_a_task_in_each_turn(monkeypatch)
    await install.db.create_session("chat-app", source="telegram", actor=None)
    await install.engine.run(
        "chat-app", "hello from a chat app", source="telegram", actor=None,
    )

    turn = install.turn_in("chat-app")
    assert turn["cause"] is None
    assert await install.user_rows("chat-app") == [("hello from a chat app", None, None)]
    assert await install.events(turn["task_id"]) == [
        (None, "pending", "chat-app", install.system_actor_id, None),
    ]


@pytest.mark.asyncio
async def test_an_explicit_unknown_cause_is_not_replaced_by_the_actor(
    install, monkeypatch,
):
    install.agent_creates_a_task_in_each_turn(monkeypatch)
    await install.db.create_session("leg", source="workflow", actor=None)
    await install.engine.run(
        "leg", "continue", source="workflow",
        actor=install.db.system_actor, cause=None,
    )
    assert install.turn_in("leg")["cause"] is None
    assert await install.user_rows("leg") == [("continue", install.system_actor_id, None)]


@pytest.mark.asyncio
async def test_resume_after_restart_keeps_the_cause_of_the_interrupted_turn(
    install, monkeypatch, tmp_path,
):
    alice = install.actor_id("alice")
    async with _client(install.app()) as client:
        session_id = await _new_session(client, install.headers("alice"))
        monkeypatch.setattr(
            install.engine, "_get_or_create_client",
            _raise_no_model,
        )
        await _chat(client, install.headers("alice"), session_id, "long job")
    await install.db.update_session_fields(session_id, {"sdk_session_id": "native-1"})
    # A restart: the new process has no turn cause in memory.
    install.engine._turn_causes.clear()

    install.agent_creates_a_task_in_each_turn(monkeypatch)
    queue = tmp_path / "resume-after-restart"
    queue.write_text(f"{session_id}\n")
    with patch("nerve.agent.engine.RESUME_QUEUE_FILE", queue):
        assert await install.engine.resume_enrolled_sessions() == 1

    turn = install.turn_in(session_id)
    assert turn["cause"] == alice
    assert await install.events(turn["task_id"]) == [
        (None, "pending", session_id, install.system_actor_id, alice),
    ]


async def _raise_no_model(*args, **kwargs):
    raise RuntimeError("no model in this test")


@pytest.mark.asyncio
async def test_run_later_and_fork_keep_the_cause_on_user_rows(install):
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

    assert await install.user_rows(session_id) == [("later", alice, alice)]
    assert await install.user_rows(fork.json()["id"]) == [("later", alice, alice)]
    assert await install.db.latest_user_turn_cause(session_id) == alice


# --------------------------------------------------------------------------- #
#  Turns that the system actor runs for a person                               #
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
        session_id="proposal", version=1,
    )
    return plan_id


@pytest.mark.asyncio
async def test_rest_plan_approval_runs_for_the_approver(install, monkeypatch):
    runs = _record_runs(install, monkeypatch)
    plan_id = await _pending_plan(install, "rest")
    async with _client(install.app()) as client:
        response = await client.post(
            f"/api/plans/{plan_id}/approve", headers=install.headers("bob"),
        )
    assert response.status_code == 200
    await asyncio.wait_for(install.ran.wait(), timeout=5)
    assert runs[0]["actor"] == install.db.system_actor
    assert actor_id_of(runs[0]["cause"]) == install.actor_id("bob")


@pytest.mark.asyncio
async def test_tool_plan_approval_runs_for_the_cause_of_the_turn(install, monkeypatch):
    runs = _record_runs(install, monkeypatch)
    plan_id = await _pending_plan(install, "tool")
    alice = install.actor_id("alice")
    ctx = ToolContext(
        session_id="alices-turn", workspace=install.config.workspace,
        db=install.db, config=install.config, engine=install.engine,
        actor=install.db.system_actor, cause=lambda: alice,
    )
    await plan_approve_handler(ctx, {"plan_id": plan_id})
    await asyncio.wait_for(install.ran.wait(), timeout=5)
    assert runs[0]["actor"] == install.db.system_actor
    assert runs[0]["cause"] == alice


@pytest.mark.asyncio
async def test_rest_plan_revision_runs_for_the_requester(install, monkeypatch):
    runs = _record_runs(install, monkeypatch)
    plan_id = await _pending_plan(install, "revise")
    async with _client(install.app()) as client:
        response = await client.post(
            f"/api/plans/{plan_id}/revise", headers=install.headers("bob"),
            json={"feedback": "add tests"},
        )
    assert response.status_code == 200
    await asyncio.wait_for(install.ran.wait(), timeout=5)
    assert runs[0]["actor"] == install.db.system_actor
    assert actor_id_of(runs[0]["cause"]) == install.actor_id("bob")


@pytest.mark.asyncio
async def test_the_star_hook_runs_for_the_person_who_starred(install, monkeypatch):
    async with _client(install.app()) as client:
        session_id = await _new_session(client, install.headers("alice"))
        runs = _record_runs(install, monkeypatch)
        response = await client.patch(
            f"/api/sessions/{session_id}", headers=install.headers("bob"),
            json={"starred": 1},
        )
    assert response.status_code == 200
    await asyncio.wait_for(install.ran.wait(), timeout=5)
    assert runs[0]["actor"] == install.db.system_actor
    assert actor_id_of(runs[0]["cause"]) == install.actor_id("bob")


# --------------------------------------------------------------------------- #
#  The MCP endpoint                                                            #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_mcp_contexts_carry_the_system_actor_and_the_bound_turn_cause(
    install, monkeypatch,
):
    resolve = build_ctx_resolver(install.engine, SatelliteSessionResolver(install.db))
    rctx = SimpleNamespace(request=None, session=None)

    satellite = await resolve(rctx)
    assert satellite.attribution == Attribution(install.system_actor_id, None)

    monkeypatch.setattr(
        "nerve.mcp_server.http._bound_identity_from_request",
        lambda config, rctx: ("codex-session", {}),
    )
    bound = await resolve(rctx)
    install.engine._turn_causes["codex-session"] = install.actor_id("alice")
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
    install.agent_creates_a_task_in_each_turn(monkeypatch)
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
        ("messages", "caused_by_actor_id"),
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
    install.agent_creates_a_task_in_each_turn(monkeypatch)
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

    assert await install.events(task_id) == [
        (None, "pending", "web", ALICE_PRINCIPAL, ALICE_PRINCIPAL),
        ("pending", "deferred", "web", BOB_PRINCIPAL, BOB_PRINCIPAL),
    ]
    turn = install.turn_in(session_id)
    assert turn["cause"] == BOB_PRINCIPAL
    assert await install.events(turn["task_id"]) == [
        (None, "pending", session_id, install.system_actor_id, BOB_PRINCIPAL),
    ]
