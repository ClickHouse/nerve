"""Who starts, kills and decides workflow runs and review loops, and for whom.

A person who acts through REST is the actor and the cause. A workflow tool or
review-loop tool that the agent calls in a session is the system actor's
change, made for the session's cause. A run's session, and the session of
every leg of a loop, is created with the cause of the run, so the agent work
and the model requests in it name the person who started the work. These
tests drive the real
routes, workflow and review-loop services, notification service, engine, tool
registry and database. Only the model runtime is replaced, by a client that
completes each turn and makes a tool call where a test asks for one. Run
payloads that the service dispatches are executed by the test, one at a time,
on the test's event loop.
"""

from __future__ import annotations

import asyncio
import base64
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

import nerve.workflows as workflows_mod
from nerve.agent import streaming
from nerve.agent.backends import TurnCompleted
from nerve.agent.engine import AgentEngine
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
from nerve.gateway.routes._deps import set_notification_service
from nerve.identity import Actor, Attribution, attribution_for
from nerve.inference_principal import CUSTOM_HEADERS_ENV
from nerve.migrate import bootstrap_identity
from nerve.notifications import handlers as _handlers
from nerve.notifications.service import NotificationService
from nerve.workflows.review_loop import ReviewLoopService
from nerve.workflows.service import ENGINE_CLAUDE, WorkflowRunService

from tests.test_inference_principal import _principals, cli  # noqa: F401 - fixture

_SECRET = "test-secret-for-workflow-attribution-32ch"
_PASSWORD_HASH = hash_password("correct-horse-battery-staple")

# External-mode principal (synthetic).
CAROL_PRINCIPAL = "5e6f7a8b-9c0d-4e1f-8a2b-3c4d5e6f7a8b"

_NEW_COLUMNS = (
    ("workflow_runs", "created_by_actor_id"),
    ("workflow_runs", "caused_by_actor_id"),
    ("workflow_runs", "killed_by_actor_id"),
    ("workflow_runs", "killed_caused_by_actor_id"),
    ("review_loops", "created_by_actor_id"),
    ("review_loops", "caused_by_actor_id"),
    ("review_loops", "killed_by_actor_id"),
    ("review_loops", "killed_caused_by_actor_id"),
    ("review_loop_decisions", "actor_id"),
    ("review_loop_decisions", "caused_by_actor_id"),
)


class _FakeRuntime:
    """A model runtime client that completes every turn.

    It records the session's cause as the turn's tool calls see it. When a
    test asked for a tool
    call in the session, the turn makes it through the session's own tool
    context, as the Claude CLI calls the in-process MCP server.
    """

    native_session_id = None

    def __init__(self, install: "_Install", session_id: str):
        self.install = install
        self.session_id = session_id

    def try_receive_idle_events(self):
        return None

    async def receive_idle_events(self, timeout=None):
        return None

    async def start_turn(self, turn_input) -> None:
        return None

    async def interrupt(self) -> None:
        return None

    async def disconnect(self) -> None:
        return None

    async def receive_turn(self):
        engine = self.install.engine
        self.install.tool_call_causes.setdefault(self.session_id, []).append(
            engine.session_cause(self.session_id),
        )
        call = self.install.tool_calls.pop(self.session_id, None)
        if call is not None:
            tool, args = call
            result = await engine.registry.invoke(
                tool, engine._build_tool_context(self.session_id), args,
            )
            self.install.tool_results.setdefault(self.session_id, []).append(result)
        yield TurnCompleted()


class _Install:
    def __init__(self, db: Database, engine: AgentEngine, config: NerveConfig):
        self.db = db
        self.engine = engine
        self.config = config
        self.notifications = NotificationService(config, db, engine)
        engine.set_notification_service(self.notifications)
        set_notification_service(self.notifications)
        self.runs = WorkflowRunService(config, db, engine)
        self.loops = ReviewLoopService(lambda: config, db, engine, self.runs)
        self.people: dict[str, dict] = {}
        self.broadcasts: list[tuple[str, dict]] = []
        self.turns: list[dict] = []
        self.tool_call_causes: dict[str, list] = {}
        self.runtimes: dict[str, _FakeRuntime] = {}
        self.tool_calls: dict[str, tuple[str, dict]] = {}
        self.tool_results: dict[str, list] = {}
        # Run ids that the service dispatched; ``drain`` executes them.
        self.spawned: list[str] = []

    @property
    def system_actor_id(self) -> str:
        return self.db.system_actor_id

    def actor_id(self, name: str) -> str:
        return self.people[name]["actor_id"]

    def actor(self, name: str) -> Actor:
        return Actor(actor_id=self.actor_id(name), kind="human")

    def headers(self, name: str) -> dict:
        token = create_session_token(_SECRET, self.people[name]["id"])
        return {"Authorization": f"Bearer {token}"}

    def app(self) -> FastAPI:
        app = FastAPI()
        app.include_router(register_all_routes())
        return app

    def watch(self, monkeypatch, *, fake_runtime: bool = True) -> None:
        """Wire the services and record broadcasts, turns and dispatches.

        Without ``fake_runtime`` the engine builds its real clients (a test
        replaces the CLI underneath them)."""
        real_broadcast = streaming.broadcaster.broadcast

        async def _broadcast(channel: str, message: dict) -> None:
            self.broadcasts.append((channel, message))
            await real_broadcast(channel, message)

        monkeypatch.setattr(streaming.broadcaster, "broadcast", _broadcast)

        real_run = self.engine.run

        async def _run(*args, **kwargs):
            self.turns.append(kwargs)
            return await real_run(*args, **kwargs)

        monkeypatch.setattr(self.engine, "run", _run)

        async def _client(session_id, source, model, **kwargs):
            if session_id not in self.runtimes:
                self.runtimes[session_id] = _FakeRuntime(self, session_id)
            return self.runtimes[session_id]

        if fake_runtime:
            monkeypatch.setattr(self.engine, "_get_or_create_client", _client)
        self.engine.add_stop_listener(self.runs._on_session_stop)
        # The test executes dispatched payloads itself (see ``drain``). The
        # stop sequence after a kill is not attribution and does not run.
        monkeypatch.setattr(self.runs, "_spawn_execute", self.spawned.append)
        monkeypatch.setattr(self.runs, "_spawn_enforcement", lambda run_id: None)
        monkeypatch.setattr(workflows_mod, "_service", self.runs)
        monkeypatch.setattr(workflows_mod, "_review_loop_service", self.loops)
        monkeypatch.setitem(
            _handlers._DISPATCHERS, "review-loop", self.loops._dispatch_decision,
        )
        self.runs.add_completion_listener(self.loops._on_run_terminal)
        self.loops._worker_task = asyncio.create_task(self.loops._worker())

    async def close(self) -> None:
        await self.loops.stop()
        await self.drain()

    async def _settle(self) -> None:
        while self.runs._kill_tasks:
            await asyncio.gather(*list(self.runs._kill_tasks), return_exceptions=True)

    async def drain(self, count: int | None = None) -> list[str]:
        """Execute dispatched run payloads in their own task, in order.

        ``count`` limits how many; the default is until none is left.
        Returns the executed run ids.
        """
        done: list[str] = []
        while self.spawned and (count is None or len(done) < count):
            run_id = self.spawned.pop(0)
            await asyncio.create_task(self.runs._execute(run_id))
            await self._settle()
            done.append(run_id)
        return done

    async def wait_for(self, predicate: Callable[[], Any], timeout: float = 5.0) -> Any:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if asyncio.iscoroutine(value):
                value = await value
            if value:
                return value
            await asyncio.sleep(0.01)
        raise AssertionError("condition not reached in time")

    def events_of(self, kind: str) -> list[dict]:
        return [m for _, m in self.broadcasts if m.get("type") == kind]

    async def message_writers(self, session_id: str) -> list[str | None]:
        """Who wrote each user message of the session."""
        return [
            row["actor_id"]
            for row in await self.db.get_messages(session_id)
            if row["role"] == "user"
        ]

    async def created_event(self, session_id: str) -> tuple:
        (row,) = [
            r for r in await self.db.get_session_events(session_id, limit=200)
            if r["event_type"] == "created"
        ]
        return (row["actor_id"], row["caused_by_actor_id"])

    async def session_cause_of(self, session_id: str) -> str | None:
        """The session's stored cause: the cause of all agent work in it."""
        return (await self.db.get_session(session_id))["caused_by_actor_id"]

    async def run_attribution(self, run_id: str) -> tuple:
        run = await self.db.get_workflow_run(run_id)
        return (
            run["created_by_actor_id"], run["caused_by_actor_id"],
            run["killed_by_actor_id"], run["killed_caused_by_actor_id"],
        )

    async def loop_attribution(self, loop_id: str) -> tuple:
        loop = await self.db.get_review_loop(loop_id)
        return (
            loop["created_by_actor_id"], loop["caused_by_actor_id"],
            loop["killed_by_actor_id"], loop["killed_caused_by_actor_id"],
        )

    async def decisions(self, loop_id: str) -> list[tuple]:
        return [
            (d["decision"], d["applied"], d["decided_by"], d["actor_id"],
             d["caused_by_actor_id"])
            for d in await self.db.list_review_loop_decisions(loop_id)
        ]

    def agent_calls(self, session_id: str, tool: str, args: dict) -> list:
        """Make the agent call ``tool`` once, in the next turn of ``session_id``.

        Returns the list that receives the tool's result.
        """
        self.tool_calls[session_id] = (tool, args)
        return self.tool_results.setdefault(session_id, [])

    async def chat_session(self, session_id: str, person: str | None = None) -> None:
        """A web session; with ``person``, that person created it and is its
        cause. A title keeps the review-loop service from asking a model for
        one."""
        await self.db.create_session(
            session_id, title=f"{session_id} title", source="web",
            actor=self.actor(person) if person else None,
        )

    async def parked_loop(self, creator: str, session_id: str) -> tuple[str, str]:
        """A loop created by ``creator`` and parked for a decision.

        Returns the loop id and the id of its decision card.
        """
        await self.chat_session(session_id)
        loop = await self.loops.create_loop(
            goal="Write the report", verifier="- C1: the report exists",
            session_id=session_id, title="Report loop",
            attribution=attribution_for(self.actor(creator)), autostart=False,
        )
        await self.loops._escalate(
            loop, "max_iterations", "Iteration cap reached.", expect=("pending",),
        )
        card = await self.db.get_pending_approval_for_target("review-loop", loop["id"])
        assert card is not None
        return loop["id"], card["id"]


def _config(tmp_path) -> NerveConfig:
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
        "auth": {"jwt_secret": _SECRET},
        "notifications": {"channels": ["web"]},
    })
    (config.workspace / "memory" / "tasks" / "active").mkdir(parents=True)
    config.workflows.runs_dir = tmp_path / "runs"
    config.workflows.kill_grace_seconds = 0
    rl = config.workflows.review_loop
    rl.implementer.engine = ENGINE_CLAUDE
    rl.verifier.engine = ENGINE_CLAUDE  # no codex preflight
    rl.default_budget_usd = 5.0
    rl.min_leg_budget_usd = 0.05
    return config


@pytest_asyncio.fixture
async def install(tmp_path, open_identity_db, wire_identity_store, monkeypatch):
    config = _config(tmp_path)
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
    installed.people["bob"] = await database.create_managed_account(
        username="bob", credential=_PASSWORD_HASH, display_name="Bob",
    )
    installed.watch(monkeypatch)
    try:
        yield installed
    finally:
        await installed.close()
        await database.close()
        set_config(NerveConfig())


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://nerve-test",
    )


_RUN = {"engine": ENGINE_CLAUDE, "prompt": "Summarize the logs", "budget_usd": 2.0}


async def _rest_run(client: httpx.AsyncClient, headers: dict, **extra) -> dict:
    response = await client.post("/api/workflow-runs", headers=headers, json={**_RUN, **extra})
    assert response.status_code == 200, response.text
    return response.json()


# --------------------------------------------------------------------------- #
#  Schema                                                                      #
# --------------------------------------------------------------------------- #


def test_the_registry_names_the_new_columns():
    for pair in _NEW_COLUMNS:
        assert pair in ACTOR_COLUMNS, pair


@pytest.mark.asyncio
async def test_v056_keeps_old_rows_null_and_is_idempotent(install):
    raw = install.db.db
    for table, column in _NEW_COLUMNS:
        if table != "review_loop_decisions":
            await raw.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
    await raw.execute("DROP TABLE review_loop_decisions")
    await raw.execute(
        "INSERT INTO workflow_runs (id, engine, status, created_by, created_at,"
        " updated_at) VALUES ('wfr-old', 'claude-workflow', 'killed', 'api',"
        " '2020-01-01', '2020-01-01')"
    )
    await raw.execute(
        "INSERT INTO review_loops (id, status, goal_prompt, verifier_prompt,"
        " created_by, created_at, updated_at) VALUES ('rvl-old', 'killed', 'g',"
        " 'v', 'session:s', '2020-01-01', '2020-01-01')"
    )
    await raw.commit()

    from nerve.db.migrations.v056_workflow_attribution import up

    await up(raw)
    await up(raw)
    await raw.commit()
    assert (await install.db.get_workflow_run("wfr-old"))["created_by"] == "api"
    assert await install.run_attribution("wfr-old") == (None, None, None, None)
    assert await install.loop_attribution("rvl-old") == (None, None, None, None)
    assert await install.db.list_review_loop_decisions("rvl-old") == []


# --------------------------------------------------------------------------- #
#  Workflow runs                                                               #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_rest_run_records_the_person_and_its_session_is_for_them(install):
    alice, system = install.actor_id("alice"), install.system_actor_id
    async with _client(install.app()) as client:
        created = await _rest_run(client, install.headers("alice"), title="Logs")
    run_id = created["id"]
    session_id = f"workflow:{run_id}"
    assert created["created_by"] == "api"
    assert (created["created_by_actor_id"], created["caused_by_actor_id"]) == (alice, alice)

    assert await install.drain() == [run_id]
    run = await install.db.get_workflow_run(run_id)
    assert run["status"] == "done"
    # The turn runs as the system actor in a session for Alice; the turn
    # itself names no cause.
    assert install.turns[-1]["actor"] == install.db.system_actor
    assert "cause" not in install.turns[-1]
    assert await install.message_writers(session_id) == [system]
    assert await install.session_cause_of(session_id) == alice
    assert install.tool_call_causes[session_id] == [alice]
    assert await install.session_cause_of(session_id) == alice
    assert (await install.db.get_session(session_id))["created_by_actor_id"] == system
    # The session's created event: the system actor, for Alice.
    assert await install.created_event(session_id) == (system, alice)
    # The live update and the journal carry the IDs.
    update = install.events_of("workflow_run_update")[-1]["run"]
    assert (update["created_by_actor_id"], update["caused_by_actor_id"]) == (alice, alice)
    events = (Path(run["journal_dir"]) / "events.ndjson").read_text().splitlines()
    created_event = json.loads(events[0])
    assert (created_event["created_by_actor_id"], created_event["caused_by_actor_id"]) == (
        alice, alice,
    )


@pytest.mark.asyncio
async def test_a_run_without_a_cause_gets_a_session_without_one(install):
    run = await install.runs.start_run(ENGINE_CLAUDE, {"prompt": "p"}, 1.0)
    await install.drain()

    session_id = f"workflow:{run['id']}"
    assert "cause" not in install.turns[-1]
    assert await install.message_writers(session_id) == [install.system_actor_id]
    assert await install.session_cause_of(session_id) == None
    assert await install.session_cause_of(session_id) is None
    assert await install.created_event(session_id) == (install.system_actor_id, None)


@pytest.mark.asyncio
async def test_a_rest_kill_records_the_person(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    async with _client(install.app()) as client:
        run_id = (await _rest_run(client, install.headers("alice")))["id"]
        killed = await client.post(
            f"/api/workflow-runs/{run_id}/kill", headers=install.headers("bob"),
            json={"reason": "not needed"},
        )
        # A kill of a terminal run changes nothing, also not who killed it.
        again = await client.post(
            f"/api/workflow-runs/{run_id}/kill", headers=install.headers("alice"),
        )
    assert killed.status_code == 200 and again.status_code == 200
    body = killed.json()
    assert (body["status"], body["error"]) == ("killed", "not needed (by api)")
    assert (body["killed_by_actor_id"], body["killed_caused_by_actor_id"]) == (bob, bob)
    assert await install.run_attribution(run_id) == (alice, alice, bob, bob)


@pytest.mark.asyncio
async def test_a_stop_of_the_runs_session_records_who_stopped_it(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    async with _client(install.app()) as client:
        run_id = (await _rest_run(client, install.headers("alice")))["id"]
    assert (await install.db.get_workflow_run(run_id))["status"] == "running"

    # A stop of the session (the WebSocket stop of the web UI) is a kill.
    await install.engine.stop_session(
        f"workflow:{run_id}", attribution=attribution_for(install.actor("bob")),
    )

    run = await install.db.get_workflow_run(run_id)
    assert (run["status"], run["error"]) == ("killed", "session stopped")
    assert await install.run_attribution(run_id) == (alice, alice, bob, bob)


@pytest.mark.asyncio
async def test_a_run_started_by_the_tool_in_alices_session_is_for_alice(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    system = install.system_actor_id
    await install.chat_session("alices-chat", "alice")
    results = install.agent_calls(
        "alices-chat", "workflow_run_start",
        {"engine": ENGINE_CLAUDE, "prompt": "Sort the inbox", "budget_usd": 1.0},
    )
    await install.engine.run("alices-chat", "start a run", actor=install.actor("alice"))
    assert results and "created" in results[0].content[0]["text"]
    (run,) = await install.db.list_workflow_runs()
    assert run["created_by"] == "session:alices-chat"
    assert await install.run_attribution(run["id"]) == (system, alice, None, None)

    await install.drain()
    session_id = f"workflow:{run['id']}"
    assert await install.message_writers(session_id) == [system]
    assert await install.session_cause_of(session_id) == alice
    assert install.tool_call_causes[session_id] == [alice]
    # The origin pointer still nests the run's session under Alice's chat.
    assert (await install.db.get_session(session_id))["parent_session_id"] == "alices-chat"

    # Bob's turn kills a second run of Alice's: the system actor, for Bob.
    async with _client(install.app()) as client:
        second = (await _rest_run(client, install.headers("alice")))["id"]
    await install.chat_session("bobs-chat", "bob")
    install.agent_calls("bobs-chat", "workflow_run_kill", {"run_id": second})
    await install.engine.run("bobs-chat", "kill it", actor=install.actor("bob"))
    assert await install.run_attribution(second) == (alice, alice, system, bob)


# --------------------------------------------------------------------------- #
#  Review loops                                                                #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_loop_created_with_its_session_runs_every_leg_for_the_person(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    system = install.system_actor_id
    async with _client(install.app()) as client:
        created = await client.post(
            "/api/sessions", headers=install.headers("alice"),
            json={
                "title": "Loop chat",
                "review_loop": {"goal": "Write the report", "verifier": "- C1: it exists"},
            },
        )
    assert created.status_code == 200, created.text
    loop_id = created.json()["review_loop_id"]
    loop = await install.db.get_review_loop(loop_id)
    assert loop["created_by"] == f"session:{created.json()['id']}"
    assert await install.loop_attribution(loop_id) == (alice, alice, None, None)

    # The implementer leg: created by the system actor, for Alice.
    await install.wait_for(lambda: install.spawned)
    implementer = install.spawned[0]
    run = await install.db.get_workflow_run(implementer)
    assert run["created_by"] == f"review-loop:{loop_id}"
    assert await install.run_attribution(implementer) == (system, alice, None, None)
    await install.drain(count=1)
    assert await install.message_writers(f"workflow:{implementer}") == [system]
    assert await install.session_cause_of(f"workflow:{implementer}") == alice
    assert install.tool_call_causes[f"workflow:{implementer}"] == [alice]

    # The verifier leg is for Alice too. Bob kills the loop while it runs.
    await install.wait_for(lambda: install.spawned)
    verifier = install.spawned[0]
    assert await install.run_attribution(verifier) == (system, alice, None, None)
    async with _client(install.app()) as client:
        killed = await client.post(
            f"/api/review-loops/{loop_id}/kill", headers=install.headers("bob"),
            json={"reason": "wrong goal"},
        )
    assert killed.status_code == 200
    assert (killed.json()["killed_by_actor_id"], killed.json()["killed_caused_by_actor_id"]) == (
        bob, bob,
    )
    assert await install.loop_attribution(loop_id) == (alice, alice, bob, bob)
    assert await install.run_attribution(verifier) == (system, alice, bob, bob)


@pytest.mark.asyncio
async def test_a_loop_started_by_the_tool_in_alices_session_and_killed_in_bobs(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    system = install.system_actor_id
    await install.chat_session("alices-chat", "alice")
    results = install.agent_calls(
        "alices-chat", "review_loop_start",
        {"goal": "Tidy the notes", "verifier": "- C1: the notes are tidy"},
    )
    await install.engine.run("alices-chat", "start a loop", actor=install.actor("alice"))
    assert results and "created" in results[0].content[0]["text"]
    (loop,) = await install.db.list_review_loops()
    assert loop["created_by"] == "mcp:alices-chat"
    assert await install.loop_attribution(loop["id"]) == (system, alice, None, None)
    await install.wait_for(lambda: install.spawned)
    leg = install.spawned[0]
    assert await install.run_attribution(leg) == (system, alice, None, None)

    await install.chat_session("bobs-chat", "bob")
    install.agent_calls("bobs-chat", "review_loop_kill", {"loop_id": loop["id"]})
    await install.engine.run("bobs-chat", "stop the loop", actor=install.actor("bob"))
    assert await install.loop_attribution(loop["id"]) == (system, alice, system, bob)
    assert await install.run_attribution(leg) == (system, alice, system, bob)


@pytest.mark.asyncio
async def test_a_leg_killed_through_its_run_kills_the_loop_for_the_same_person(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    await install.chat_session("loop-chat")
    loop = await install.loops.create_loop(
        goal="Write the report", verifier="- C1: it exists", session_id="loop-chat",
        attribution=attribution_for(install.actor("alice")),
    )
    await install.wait_for(lambda: install.spawned)
    leg = install.spawned[0]

    async with _client(install.app()) as client:
        killed = await client.post(
            f"/api/workflow-runs/{leg}/kill", headers=install.headers("bob"),
        )
    assert killed.status_code == 200
    killed_loop = await install.wait_for(
        lambda: _loop_if(install, loop["id"], "killed"),
    )
    assert killed_loop["failure_reason"] == "implementer leg killed"
    assert await install.loop_attribution(loop["id"]) == (alice, alice, bob, bob)


@pytest.mark.asyncio
async def test_a_leg_issued_again_after_a_restart_keeps_the_loops_cause(install):
    alice, system = install.actor_id("alice"), install.system_actor_id
    await install.chat_session("loop-chat")
    loop = await install.loops.create_loop(
        goal="Write the report", verifier="- C1: it exists", session_id="loop-chat",
        attribution=attribution_for(install.actor("alice")), autostart=False,
    )
    # The crash window of the dispatch outbox: the attempt row exists, the
    # run row does not.
    await install.db.begin_leg_attempt(
        loop["id"], "implementing", ("pending",),
        iteration=1, role="implementer", run_id="wfr-again001",
    )

    await install.loops._recover()

    assert await install.run_attribution("wfr-again001") == (system, alice, None, None)
    await install.drain(count=1)
    assert await install.message_writers("workflow:wfr-again001") == [system]
    assert await install.session_cause_of("workflow:wfr-again001") == alice


async def _loop_if(install: _Install, loop_id: str, status: str) -> dict | None:
    loop = await install.db.get_review_loop(loop_id)
    return loop if loop and loop["status"] == status else None


# --------------------------------------------------------------------------- #
#  Review-loop decisions                                                       #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_rest_decision_records_the_person_and_resumes_for_the_loops_cause(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    system = install.system_actor_id
    loop_id, card_id = await install.parked_loop("alice", "loop-chat")

    async with _client(install.app()) as client:
        decided = await client.post(
            f"/api/review-loops/{loop_id}/decision", headers=install.headers("bob"),
            json={"decision": "iterate:2"},
        )
        detail = (await client.get(
            f"/api/review-loops/{loop_id}", headers=install.headers("alice"),
        )).json()
    assert decided.status_code == 200, decided.text

    assert await install.decisions(loop_id) == [("iterate:2", True, "api", bob, bob)]
    (shown,) = detail["decisions"]
    assert (shown["decision"], shown["actor_id"], shown["caused_by_actor_id"]) == (
        "iterate:2", bob, bob,
    )
    # The decision card closes in Bob's name.
    card = await install.db.get_notification(card_id)
    assert (card["status"], card["answer"], card["answered_by_actor_id"]) == (
        "answered", "decided:iterate:2", bob,
    )
    answered = [
        e for e in install.events_of("notification_answered")
        if e["notification_id"] == card_id
    ]
    assert answered[-1]["actor_id"] == bob
    # The resumed leg still runs for the loop's cause.
    (leg,) = install.spawned
    assert await install.run_attribution(leg) == (system, alice, None, None)


@pytest.mark.asyncio
async def test_a_card_decision_records_the_person_who_answered(install):
    alice = install.actor_id("alice")
    loop_id, card_id = await install.parked_loop("bob", "loop-chat")

    async with _client(install.app()) as client:
        answered = await client.post(
            f"/api/notifications/{card_id}/answer", headers=install.headers("alice"),
            json={"answer": "accept"},
        )
    assert answered.status_code == 200

    assert (await install.db.get_review_loop(loop_id))["status"] == "passed"
    assert await install.decisions(loop_id) == [
        ("accept", True, "approval-card", alice, alice),
    ]
    assert (await install.db.get_notification(card_id))["answered_by_actor_id"] == alice


@pytest.mark.asyncio
async def test_a_card_reminder_then_a_rest_decision_name_each_person(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    loop_id, card_id = await install.parked_loop("alice", "loop-chat")

    async with _client(install.app()) as client:
        reminded = await client.post(
            f"/api/notifications/{card_id}/answer", headers=install.headers("alice"),
            json={"answer": "remind_4h"},
        )
        assert reminded.status_code == 200
        # The snoozed card stays pending and names who snoozed it.
        card = await install.db.get_notification(card_id)
        assert (card["status"], card["answered_by_actor_id"]) == ("pending", alice)

        decided = await client.post(
            f"/api/review-loops/{loop_id}/decision", headers=install.headers("bob"),
            json={"decision": "accept"},
        )
    assert decided.status_code == 200

    assert await install.decisions(loop_id) == [
        ("remind_4h", True, "approval-card", alice, alice),
        ("accept", True, "api", bob, bob),
    ]
    # The decision closes the snoozed card in the decider's name.
    card = await install.db.get_notification(card_id)
    assert (card["status"], card["answer"], card["answered_by_actor_id"]) == (
        "answered", "decided:accept", bob,
    )


@pytest.mark.asyncio
async def test_a_reminder_that_loses_to_the_expiry_sweep_is_not_applied(
    install, monkeypatch,
):
    alice = install.actor_id("alice")
    loop_id, card_id = await install.parked_loop("alice", "loop-chat")
    real_snooze = install.db.snooze_notification
    rows_before_snooze: list = []

    async def sweep_first(notification_id, *args, **kwargs):
        # The barrier: the dispatcher has returned and the snooze write has
        # not run yet. The expiry sweep takes the claimed card here.
        rows_before_snooze.append(await install.db.list_review_loop_decisions(loop_id))
        await install.db.db.execute(
            "UPDATE notifications SET expires_at = '2000-01-01T00:00:00+00:00'"
            " WHERE id = ?", (notification_id,),
        )
        await install.db.db.commit()
        assert await install.notifications.expire_stale() == 1
        return await real_snooze(notification_id, *args, **kwargs)

    monkeypatch.setattr(install.db, "snooze_notification", sweep_first)
    async with _client(install.app()) as client:
        answered = await client.post(
            f"/api/notifications/{card_id}/answer", headers=install.headers("alice"),
            json={"answer": "remind_4h"},
        )
    assert answered.status_code == 200

    # Nothing is recorded before the snooze write, and the snooze that found
    # no pending card is recorded as not applied, with the reason.
    assert rows_before_snooze == [[]]
    assert (await install.db.get_notification(card_id))["status"] == "expired"
    assert await install.decisions(loop_id) == [
        ("remind_4h", False, "approval-card", alice, alice),
    ]
    (row,) = await install.db.list_review_loop_decisions(loop_id)
    assert row["message"] == (
        "the decision card was no longer pending, so it was not snoozed"
    )
    assert (await install.db.get_review_loop(loop_id))["status"] == "awaiting_user"


@pytest.mark.asyncio
async def test_a_channel_card_decision_records_no_person(install):
    loop_id, card_id = await install.parked_loop("alice", "loop-chat")
    await install.db.record_notification_delivery(
        card_id, "telegram", target="1001", message_id="7",
    )

    answered = await install.notifications.answer_delivered_notification(
        card_id, "abandon", channel="telegram", target="1001", actor="4242",
    )
    assert answered is not None

    assert (await install.db.get_review_loop(loop_id))["status"] == "failed"
    assert await install.decisions(loop_id) == [
        ("abandon", True, "approval-card", None, None),
    ]
    assert (await install.db.get_notification(card_id))["answered_by_actor_id"] is None


@pytest.mark.asyncio
async def test_applied_says_whether_the_decision_changed_the_loop(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    loop_id, _ = await install.parked_loop("alice", "loop-chat")

    async with _client(install.app()) as client:
        # A reminder snoozes the decision card; REST has no card to snooze.
        reminded = await client.post(
            f"/api/review-loops/{loop_id}/decision", headers=install.headers("bob"),
            json={"decision": "remind_4h"},
        )
        accepted = await client.post(
            f"/api/review-loops/{loop_id}/decision", headers=install.headers("alice"),
            json={"decision": "accept"},
        )
        late = await client.post(
            f"/api/review-loops/{loop_id}/decision", headers=install.headers("bob"),
            json={"decision": "abandon"},
        )
        unknown = await client.post(
            f"/api/review-loops/{loop_id}/decision", headers=install.headers("bob"),
            json={"decision": "shrug"},
        )
    assert (reminded.status_code, accepted.status_code) == (400, 200)
    assert accepted.json()["applied"] is True
    assert (late.status_code, unknown.status_code) == (409, 409)
    # A card reminder on a loop that is no longer parked changes nothing.
    card_reminder = await install.loops._dispatch_decision(
        {}, loop_id, "remind_4h", None, actor=install.actor("bob"),
    )
    assert not card_reminder.ok and card_reminder.snooze_until is None

    assert await install.decisions(loop_id) == [
        ("accept", True, "api", alice, alice),
        ("abandon", False, "api", bob, bob),
        ("remind_4h", False, "approval-card", bob, bob),
    ]
    rows = await install.db.list_review_loop_decisions(loop_id)
    assert rows[0]["message"] == "accepted as done"
    assert "is passed; decision recorded but not applied" in rows[1]["message"]


@pytest.mark.asyncio
async def test_a_grant_whose_leg_does_not_start_is_still_applied(install):
    bob = install.actor_id("bob")
    loop_id, _ = await install.parked_loop("alice", "loop-chat")
    # Too little money for any leg: the resume parks the loop again.
    install.config.workflows.review_loop.min_leg_budget_usd = 100.0

    async with _client(install.app()) as client:
        decided = await client.post(
            f"/api/review-loops/{loop_id}/decision", headers=install.headers("bob"),
            json={"decision": "budget:1"},
        )
    assert decided.status_code == 200, decided.text
    assert (decided.json()["ok"], decided.json()["applied"]) == (False, True)

    loop = await install.db.get_review_loop(loop_id)
    assert (loop["budget_usd"], loop["status"], loop["failure_reason"]) == (
        6.0, "awaiting_user", "budget",
    )
    assert await install.decisions(loop_id) == [("budget:1", True, "api", bob, bob)]
    (row,) = await install.db.list_review_loop_decisions(loop_id)
    assert row["message"] == "budget raised to $6.00 (+$1.00)"
    assert install.spawned == []


# A synthetic actor ID with no actor_refs row: its decision row cannot be
# stored, which makes the write of the decision history fail.
_NO_SUCH_ACTOR = Attribution(
    actor_id="00000000-0000-4000-8000-0000000000ff",
    caused_by_actor_id="00000000-0000-4000-8000-0000000000ff",
)


@pytest.mark.asyncio
@pytest.mark.parametrize("decision", ["accept", "abandon", "iterate:2", "budget:3"])
async def test_a_decision_that_cannot_be_recorded_changes_nothing(install, decision):
    loop_id, card_id = await install.parked_loop("alice", "loop-chat")
    before = await install.db.get_review_loop(loop_id)

    with pytest.raises(sqlite3.IntegrityError):
        await install.loops.handle_decision(
            loop_id, decision, decided_by="api", attribution=_NO_SUCH_ACTOR,
        )

    after = await install.db.get_review_loop(loop_id)
    for column in ("status", "failure_reason", "budget_usd", "max_iterations",
                   "ended_at", "updated_at"):
        assert after[column] == before[column], column
    assert await install.db.list_review_loop_decisions(loop_id) == []
    assert (await install.db.get_notification(card_id))["status"] == "pending"
    assert install.spawned == []


@pytest.mark.asyncio
async def test_a_late_decision_that_cannot_be_recorded_is_not_hidden(install):
    loop_id, _ = await install.parked_loop("alice", "loop-chat")
    await install.loops.handle_decision(loop_id, "accept", decided_by="api")

    with pytest.raises(sqlite3.IntegrityError):
        await install.loops.handle_decision(
            loop_id, "abandon", decided_by="api", attribution=_NO_SUCH_ACTOR,
        )
    assert [d[0] for d in await install.decisions(loop_id)] == ["accept"]


@pytest.mark.asyncio
async def test_a_decision_is_applied_only_to_a_parked_loop(install):
    await install.chat_session("loop-chat")
    loop = await install.loops.create_loop(
        goal="g", verifier="- C1: v", session_id="loop-chat", autostart=False,
    )
    who = attribution_for(install.actor("bob"))

    # The loop is pending, not parked: neither a change nor a reminder lands.
    assert not await install.db.apply_review_loop_decision(
        loop["id"], "iterate:2", fields={"max_iterations": 5}, attribution=who,
    )
    assert not await install.db.apply_review_loop_decision(
        loop["id"], "remind_4h", attribution=who,
    )
    assert (await install.db.get_review_loop(loop["id"]))["max_iterations"] == 3
    assert await install.db.list_review_loop_decisions(loop["id"]) == []

    await install.db.transition_review_loop(
        loop["id"], "awaiting_user", expect=("pending",),
    )
    assert await install.db.apply_review_loop_decision(
        loop["id"], "remind_4h", message="snoozed 4h", attribution=who,
    )
    assert await install.db.apply_review_loop_decision(
        loop["id"], "accept", to_status="passed", attribution=who,
    )
    done = await install.db.get_review_loop(loop["id"])
    assert done["status"] == "passed" and done["ended_at"]
    assert await install.decisions(loop["id"]) == [
        ("remind_4h", True, "", install.actor_id("bob"), install.actor_id("bob")),
        ("accept", True, "", install.actor_id("bob"), install.actor_id("bob")),
    ]


@pytest.mark.asyncio
async def test_a_new_park_supersedes_the_old_card_as_the_system_actor(install):
    loop_id, first_card = await install.parked_loop("alice", "loop-chat")
    await install.db.transition_review_loop(
        loop_id, "implementing", expect=("awaiting_user",),
    )
    loop = await install.db.get_review_loop(loop_id)

    await install.loops._escalate(
        loop, "budget", "Budget too low.", expect=("implementing",),
    )

    old = await install.db.get_notification(first_card)
    assert (old["answer"], old["answered_by_actor_id"]) == (
        "superseded", install.system_actor_id,
    )
    current = await install.db.get_pending_approval_for_target("review-loop", loop_id)
    assert current is not None and current["id"] != first_card


# --------------------------------------------------------------------------- #
#  The actor directory                                                         #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_actor_directory_names_every_id_in_the_new_columns(install):
    parked, _ = await install.parked_loop("alice", "parked-chat")
    await install.chat_session("killed-chat")
    killed_loop = await install.loops.create_loop(
        goal="g", verifier="- C1: v", session_id="killed-chat",
        attribution=attribution_for(install.actor("alice")), autostart=False,
    )
    async with _client(install.app()) as client:
        run_id = (await _rest_run(client, install.headers("alice")))["id"]
        await client.post(f"/api/workflow-runs/{run_id}/kill", headers=install.headers("bob"))
        await client.post(
            f"/api/review-loops/{killed_loop['id']}/kill", headers=install.headers("bob"),
        )
        await client.post(
            f"/api/review-loops/{parked}/decision", headers=install.headers("bob"),
            json={"decision": "abandon"},
        )
        directory = {
            row["id"]
            for row in (
                await client.get("/api/actors", headers=install.headers("bob"))
            ).json()["actors"]
        }

    for table, column in _NEW_COLUMNS:
        async with install.db.db.execute(
            f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL"
        ) as cursor:
            referenced = {row[0] async for row in cursor}
        assert referenced, (table, column)
        assert referenced <= directory, (table, column)


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
    config = _config(tmp_path)
    set_config(config)
    database = Database(tmp_path / "nerve.db")
    await database.connect()
    installed = None
    try:
        await bootstrap_identity(database, config)
        wire_identity_store(database)
        engine = AgentEngine(config, database)
        init_deps(engine, database)
        installed = _Install(database, engine, config)
        installed.watch(monkeypatch)
        yield installed
    finally:
        if installed is not None:
            await installed.close()
        await database.close()
        set_config(NerveConfig())


@pytest.mark.asyncio
async def test_external_mode_gives_the_session_the_principal(external_install):
    install = external_install
    carol = _as(CAROL_PRINCIPAL, "Carol")
    async with _client(install.app()) as client:
        created = await _rest_run(client, carol)
        await install.drain()
        second = await _rest_run(client, carol)
        killed = await client.post(f"/api/workflow-runs/{second['id']}/kill", headers=carol)
    assert killed.status_code == 200

    session_id = f"workflow:{created['id']}"
    assert (created["created_by_actor_id"], created["caused_by_actor_id"]) == (
        CAROL_PRINCIPAL, CAROL_PRINCIPAL,
    )
    assert await install.message_writers(session_id) == [install.system_actor_id]
    assert await install.session_cause_of(session_id) == CAROL_PRINCIPAL
    assert (await install.run_attribution(second["id"]))[2:] == (
        CAROL_PRINCIPAL, CAROL_PRINCIPAL,
    )


@pytest_asyncio.fixture
async def external_cli_install(tmp_path, monkeypatch, wire_identity_store, cli):  # noqa: F811
    """External mode with the real engine clients over T2's recording CLI."""
    monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
    monkeypatch.delenv(CUSTOM_HEADERS_ENV, raising=False)
    pin_auth_mode(AUTH_MODE_EXTERNAL)
    config = _config(tmp_path)
    config.agent.backend = "claude"
    set_config(config)
    database = Database(tmp_path / "nerve.db")
    await database.connect()
    installed = None
    try:
        await bootstrap_identity(database, config)
        wire_identity_store(database)
        engine = AgentEngine(config, database)
        init_deps(engine, database)
        installed = _Install(database, engine, config)
        installed.watch(monkeypatch, fake_runtime=False)
        yield installed, cli
    finally:
        if installed is not None:
            await installed.close()
            for session_id in list(installed.engine._idle_watchers):
                installed.engine._stop_idle_watcher(session_id)
        await database.close()
        set_config(NerveConfig())


@pytest.mark.asyncio
async def test_leg_sessions_name_the_cause_of_the_run_to_the_model(external_cli_install):
    install, processes = external_cli_install
    carol = _as(CAROL_PRINCIPAL, "Carol")
    system = install.system_actor_id
    async with _client(install.app()) as client:
        rest_run = (await _rest_run(client, carol))["id"]
        created = await client.post(
            "/api/sessions", headers=carol,
            json={
                "title": "Loop chat",
                "review_loop": {"goal": "Write the report", "verifier": "- C1: it exists"},
            },
        )
    assert created.status_code == 200, created.text
    await install.wait_for(lambda: len(install.spawned) >= 2)
    (loop,) = await install.db.list_review_loops()
    loop_leg = loop["current_run_id"]
    scheduled = await install.runs.start_run(
        ENGINE_CLAUDE, {"prompt": "nightly"}, 1.0,
        attribution=attribution_for(install.db.system_actor),
    )
    unknown = await install.runs.start_run(ENGINE_CLAUDE, {"prompt": "unknown"}, 1.0)
    await install.drain(count=4)

    by_session = {process.spec.session_id: process for process in processes}
    expected = {
        rest_run: CAROL_PRINCIPAL,      # a run that Carol started through REST
        loop_leg: CAROL_PRINCIPAL,      # a leg of the loop that Carol started
        scheduled["id"]: system,        # autonomous work: the system actor
        unknown["id"]: None,            # no cause: no header
    }
    for run_id, principal in expected.items():
        process = by_session[f"workflow:{run_id}"]
        assert process.spec.inference_principal == principal, run_id
        assert _principals(process.env) == ([principal] if principal else []), run_id
