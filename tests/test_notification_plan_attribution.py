"""Who answers or dismisses a notification, creates a silence, or reviews a plan.

A person who acts through the web UI or REST is the actor and the cause. A
plan tool or the silence tool that the agent calls inside a turn is the system
actor's change, made for the cause of that turn. An answer through a chat
channel names a provider user and no person, so it records no actor. These
tests drive the real routes, notification service, engine, tool registry and
database; only the model runtime is replaced, by a client that completes
each turn and makes a tool call where a test asks for one.
"""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import logging
import textwrap
from typing import Any

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from nerve.agent import streaming
from nerve.agent.backends import TurnCompleted
from nerve.agent.engine import AgentEngine
from nerve.agent.tools.handlers.plans import (
    plan_approve_handler,
    plan_decline_handler,
    plan_propose_handler,
    plan_revise_handler,
    plan_update_handler,
)
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
from nerve.gateway.routes._deps import set_notification_service
from nerve.identity import Actor
from nerve.migrate import bootstrap_identity
from nerve.notifications import handlers as _handlers
from nerve.notifications.service import NotificationService

from tests.test_notifications_actionable import (
    _MINIMAL_HELPER_SRC,
    read_audit_jsonl,
)

_SECRET = "test-secret-for-notification-attribution-32"
_PASSWORD_HASH = hash_password("correct-horse-battery-staple")

# External-mode principal (synthetic).
CAROL_PRINCIPAL = "3c4d5e6f-7a8b-4c9d-8e0f-1a2b3c4d5e6f"

_NEW_COLUMNS = (
    ("notifications", "answered_by_actor_id"),
    ("notifications", "dismissed_by_actor_id"),
    ("notification_silences", "created_by_actor_id"),
    ("notification_silences", "caused_by_actor_id"),
    ("plans", "reviewed_by_actor_id"),
    ("plans", "reviewed_caused_by_actor_id"),
    ("plans", "created_by_actor_id"),
    ("plans", "caused_by_actor_id"),
)


class _FakeRuntime:
    """A model runtime client that completes every turn.

    When a test asked for a tool call in the session, the turn makes it
    through the session's own tool context, as the Claude CLI calls the
    in-process MCP server, after the engine has started the turn.
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

    async def disconnect(self) -> None:
        return None

    async def receive_turn(self):
        gate = self.install.holds.get(self.session_id)
        if gate is not None:
            await gate.wait()
        call = self.install.tool_calls.pop(self.session_id, None)
        if call is not None:
            tool, args = call
            engine = self.install.engine
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
        self.people: dict[str, dict] = {}
        self.broadcasts: list[tuple[str, dict]] = []
        self.runs: list[dict] = []
        self._runs_done: dict[str, asyncio.Event] = {}
        self.runtimes: dict[str, _FakeRuntime] = {}
        self.tool_calls: dict[str, tuple[str, dict]] = {}
        self.tool_results: dict[str, list] = {}
        # A session whose turns wait for this event before they complete.
        self.holds: dict[str, asyncio.Event] = {}

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

    def watch(self, monkeypatch) -> None:
        """Record broadcasts and turns; a fake runtime serves the turns."""
        real_broadcast = streaming.broadcaster.broadcast

        async def _broadcast(channel: str, message: dict) -> None:
            self.broadcasts.append((channel, message))
            await real_broadcast(channel, message)

        monkeypatch.setattr(streaming.broadcaster, "broadcast", _broadcast)

        real_run = self.engine.run

        async def _run(*args, **kwargs):
            session_id = kwargs.get("session_id") or args[0]
            self.runs.append(kwargs)
            done = self._runs_done.setdefault(session_id, asyncio.Event())
            try:
                return await real_run(*args, **kwargs)
            finally:
                done.set()

        monkeypatch.setattr(self.engine, "run", _run)

        async def _client(session_id, source, model, **kwargs):
            if session_id not in self.runtimes:
                self.runtimes[session_id] = _FakeRuntime(self, session_id)
            return self.runtimes[session_id]

        monkeypatch.setattr(self.engine, "_get_or_create_client", _client)

    async def turn_done(self, session_id: str) -> None:
        done = self._runs_done.setdefault(session_id, asyncio.Event())
        await asyncio.wait_for(done.wait(), timeout=5)

    def events_of(self, kind: str) -> list[dict]:
        return [m for _, m in self.broadcasts if m.get("type") == kind]

    async def user_rows(self, session_id: str) -> list[tuple]:
        return [
            (row["content"], row["actor_id"])
            for row in await self.db.get_messages(session_id)
            if row["role"] == "user"
        ]

    async def task_events(self, task_id: str) -> list[tuple]:
        return [
            (row["to_status"], row["actor"], row["actor_id"], row["caused_by_actor_id"])
            for row in await self.db.list_task_events(task_id)
        ]

    async def plan_row(self, plan_id: str) -> dict:
        # A direct read: some tests replace ``db.get_plan`` with a stale one.
        async with self.db.db.execute(
            "SELECT * FROM plans WHERE id = ?", (plan_id,),
        ) as cursor:
            return dict(await cursor.fetchone())

    async def plan_reviewer(self, plan_id: str) -> tuple:
        plan = await self.plan_row(plan_id)
        return (plan["reviewed_by_actor_id"], plan["reviewed_caused_by_actor_id"])

    def impl_runs(self) -> list[dict]:
        return [run for run in self.runs if str(run.get("session_id", "")).startswith("impl-")]

    async def pending_plan(self, suffix: str) -> tuple[str, str]:
        task_id, plan_id = f"task-{suffix}", f"plan-{suffix}"
        task_file = self.config.workspace / f"{task_id}.md"
        task_file.write_text("# Plan task\n", encoding="utf-8")
        await self.db.upsert_task(
            task_id=task_id, file_path=task_file.name, title="Plan task",
            status="pending", content="# Plan task\n",
        )
        await self.db.create_plan(
            plan_id=plan_id, task_id=task_id, content="Ship it",
            session_id="proposal", version=1,
        )
        return task_id, plan_id

    def agent_calls(self, session_id: str, tool: str, args: dict) -> list:
        """Make the agent call ``tool`` once, in the next turn of ``session_id``.

        Returns the list that receives the tool's result.
        """
        self.tool_calls[session_id] = (tool, args)
        return self.tool_results.setdefault(session_id, [])

    async def session_cause_of(self, session_id: str) -> str | None:
        return (await self.db.get_session(session_id))["caused_by_actor_id"]


@pytest_asyncio.fixture
async def install(tmp_path, open_identity_db, wire_identity_store, monkeypatch):
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
        "auth": {"jwt_secret": _SECRET},
        "notifications": {"channels": ["web"]},
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
    installed.people["bob"] = await database.create_managed_account(
        username="bob", credential=_PASSWORD_HASH, display_name="Bob",
    )
    installed.watch(monkeypatch)
    try:
        yield installed
    finally:
        await database.close()
        set_config(NerveConfig())


@pytest.fixture
def audit_dir(tmp_path, monkeypatch):
    """A workspace with the audit helper and a stub ``mechanical-action.sh``."""
    workspace = tmp_path / "audit-ws"
    scripts = workspace / "scripts"
    scripts.mkdir(parents=True)
    (scripts / "_mechanical_action.py").write_text(_MINIMAL_HELPER_SRC)
    script = scripts / "mechanical-action.sh"
    script.write_text(textwrap.dedent("""\
        #!/usr/bin/env bash
        exit 0
    """))
    script.chmod(0o755)
    state = workspace / ".nerve" / "mechanical-actions"
    monkeypatch.setenv("NERVE_WORKSPACE_PATH", str(workspace))
    monkeypatch.setenv("NERVE_MECHANICAL_STATE_DIR", str(state))
    return state


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://nerve-test",
    )


def _register(monkeypatch, kind: str, fn) -> None:
    monkeypatch.setitem(_handlers._DISPATCHERS, kind, fn)


def _acted(**event: Any) -> _handlers.DispatchResult:
    return _handlers.DispatchResult(
        ok=True, audit_event={"event": "approval-acted", "ok": True, **event},
    )


# --------------------------------------------------------------------------- #
#  Schema                                                                      #
# --------------------------------------------------------------------------- #


def test_the_registry_names_the_new_columns():
    for pair in _NEW_COLUMNS:
        assert pair in ACTOR_COLUMNS, pair


@pytest.mark.asyncio
async def test_v055_keeps_old_rows_null_and_is_idempotent(install):
    raw = install.db.db
    for table, column in _NEW_COLUMNS:
        await raw.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
    await raw.execute(
        "INSERT INTO notifications (id, session_id, type, title, status, created_at,"
        " answered_by) VALUES ('old-n', 's', 'question', 'Old', 'answered',"
        " '2020-01-01', 'web')"
    )
    await raw.execute(
        "INSERT INTO notification_silences (id, pattern, created_by, created_at)"
        " VALUES ('old-s', 'noise', 'web', '2020-01-01')"
    )
    await raw.execute(
        "INSERT INTO plans (id, task_id, content, status) VALUES"
        " ('old-p', 't', 'Old plan', 'declined')"
    )
    await raw.commit()

    from nerve.db.migrations.v055_notification_plan_attribution import up

    await up(raw)
    await up(raw)
    await raw.commit()
    notif = await install.db.get_notification("old-n")
    assert (notif["answered_by"], notif["answered_by_actor_id"],
            notif["dismissed_by_actor_id"]) == ("web", None, None)
    silence = await install.db.get_silence("old-s")
    assert (silence["created_by"], silence["created_by_actor_id"],
            silence["caused_by_actor_id"]) == ("web", None, None)
    assert await install.plan_reviewer("old-p") == (None, None)


# --------------------------------------------------------------------------- #
#  Questions                                                                   #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_web_answer_records_the_person_and_relays_it_as_their_message(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    # Bob started the session that asks; Alice answers.
    await install.db.create_session("asker", source="web", actor=_actor(install, "bob"))
    asked = await install.notifications.ask_question("asker", title="Pick one")
    nid = asked["notification_id"]

    async with _client(install.app()) as client:
        response = await client.post(
            f"/api/notifications/{nid}/answer", headers=install.headers("alice"),
            json={"answer": "yes"},
        )
        listed = (await client.get(
            "/api/notifications", headers=install.headers("alice"),
        )).json()["notifications"]
    assert response.status_code == 200
    await install.turn_done("asker")

    row = await install.db.get_notification(nid)
    assert (row["answered_by"], row["answered_by_actor_id"]) == ("web", alice)
    assert next(n for n in listed if n["id"] == nid)["answered_by_actor_id"] == alice
    # The relayed message is Alice's; the agent's work in that turn has the
    # session's cause, Bob.
    assert await install.user_rows("asker") == [
        ("[Answer to: Pick one]\n\nyes", alice),
    ]
    assert await install.session_cause_of("asker") == bob
    assert install.events_of("notification_answered")[-1]["actor_id"] == alice
    injected = install.events_of("answer_injected")[-1]
    assert (injected["actor_id"], injected["caused_by_actor_id"]) == (alice, alice)


@pytest.mark.asyncio
async def test_a_channel_answer_keeps_the_provider_id_and_records_no_actor(install):
    await install.db.create_session("asker", source="web", actor=None)
    asked = await install.notifications.ask_question("asker", title="Pick one")
    nid = asked["notification_id"]
    await install.db.record_notification_delivery(
        nid, "telegram", target="1001", message_id="7",
    )

    answered = await install.notifications.answer_delivered_notification(
        nid, "no", channel="telegram", target="1001", actor="4242",
    )
    assert answered is not None
    await install.turn_done("asker")

    row = await install.db.get_notification(nid)
    assert (row["answered_by"], row["answered_by_actor_id"]) == ("telegram", None)
    assert '"answered_by_actor": "4242"' in row["metadata"]
    assert install.runs[-1]["actor"] is None
    assert await install.user_rows("asker") == [
        ("[Answer to: Pick one]\n\nno", None),
    ]
    assert install.events_of("notification_answered")[-1]["actor_id"] is None
    assert install.events_of("answer_injected")[-1]["actor_id"] is None


@pytest.mark.asyncio
async def test_an_expiry_note_is_the_system_actors_turn(install):
    await install.db.create_session("asker", source="web", actor=_actor(install, "bob"))
    await install.notifications.ask_question("asker", title="Too late")
    await install.db.db.execute(
        "UPDATE notifications SET expires_at = '2000-01-01T00:00:00+00:00'"
    )
    await install.db.db.commit()

    assert await install.notifications.expire_stale() == 1
    await install.turn_done("asker")
    assert install.runs[-1]["actor"] == install.db.system_actor
    # The note does not change the session's cause.
    assert install.engine.session_cause("asker") == install.actor_id("bob")


# --------------------------------------------------------------------------- #
#  Approvals                                                                   #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_web_approval_gives_the_dispatcher_the_person(
    install, monkeypatch, audit_dir,
):
    bob = install.actor_id("bob")
    seen: list = []

    def dispatch(notification, target_id, decision, config, *, actor):
        seen.append(actor)
        return _acted(target_kind="a3-sync", decision=decision)

    _register(monkeypatch, "a3-sync", dispatch)
    await install.db.create_session("proposer", source="web", actor=None)
    nid = (await install.notifications.propose_action(
        "proposer", target_kind="a3-sync", target_id="item-1", title="Run it?",
    ))["notification_id"]

    async with _client(install.app()) as client:
        response = await client.post(
            f"/api/notifications/{nid}/answer", headers=install.headers("bob"),
            json={"answer": "approve"},
        )
    assert response.status_code == 200

    assert [actor.actor_id for actor in seen] == [bob]
    row = await install.db.get_notification(nid)
    assert (row["status"], row["answered_by_actor_id"]) == ("answered", bob)
    acted = [r for r in read_audit_jsonl(audit_dir) if r["event"] == "approval-acted"]
    assert acted[-1]["answered_by"] == "web"
    assert acted[-1]["answered_by_actor_id"] == bob
    assert install.events_of("notification_answered")[-1]["actor_id"] == bob


def test_every_in_tree_dispatcher_takes_the_actor_keyword():
    from nerve.workflows.review_loop import ReviewLoopService

    for dispatcher in (
        _handlers.get("mechanical-action"), ReviewLoopService._dispatch_decision,
    ):
        param = inspect.signature(dispatcher).parameters["actor"]
        assert param.kind is inspect.Parameter.KEYWORD_ONLY, dispatcher


@pytest.mark.asyncio
async def test_the_mechanical_action_audit_names_the_person(install, audit_dir):
    alice = install.actor_id("alice")
    await install.db.create_session("proposer", source="web", actor=None)
    nid = (await install.notifications.propose_action(
        "proposer", target_kind="mechanical-action", target_id="prop-1",
        title="Archive old logs?",
    ))["notification_id"]

    async with _client(install.app()) as client:
        response = await client.post(
            f"/api/notifications/{nid}/answer", headers=install.headers("alice"),
            json={"answer": "decline"},
        )
    assert response.status_code == 200
    acted = [r for r in read_audit_jsonl(audit_dir) if r["event"] == "approval-acted"]
    assert acted[-1]["ok"] is True
    assert acted[-1]["target_kind"] == "mechanical-action"
    assert acted[-1]["answered_by_actor_id"] == alice


@pytest.mark.asyncio
async def test_a_web_snooze_records_who_deferred_the_approval(
    install, monkeypatch, audit_dir,
):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    seen: list = []

    def dispatch(notification, target_id, decision, config, *, actor):
        seen.append((decision, actor.actor_id if actor else None))
        result = _acted(target_kind="a3-snooze", decision=decision)
        if decision == "snooze_24h":
            result.snooze_until = "2999-01-01T00:00:00+00:00"
        return result

    _register(monkeypatch, "a3-snooze", dispatch)
    await install.db.create_session("proposer", source="web", actor=None)
    nid = (await install.notifications.propose_action(
        "proposer", target_kind="a3-snooze", target_id="item-3", title="Run it?",
    ))["notification_id"]

    async with _client(install.app()) as client:
        snoozed = await client.post(
            f"/api/notifications/{nid}/answer", headers=install.headers("bob"),
            json={"answer": "snooze_24h"},
        )
        row = await install.db.get_notification(nid)
        assert snoozed.status_code == 200
        assert (row["status"], row["answered_by_actor_id"]) == ("pending", bob)
        event = install.events_of("notification_answered")[-1]
        assert (event["approval_status"], event["actor_id"]) == ("snoozed", bob)

        # The card can still be decided; the decision names its own person.
        approved = await client.post(
            f"/api/notifications/{nid}/answer", headers=install.headers("alice"),
            json={"answer": "approve"},
        )
    assert approved.status_code == 200
    row = await install.db.get_notification(nid)
    assert (row["status"], row["answered_by_actor_id"]) == ("answered", alice)
    assert seen == [("snooze_24h", bob), ("approve", alice)]


@pytest.mark.asyncio
async def test_a_channel_approval_gives_the_dispatcher_no_actor(
    install, monkeypatch, audit_dir,
):
    seen: list = []

    async def dispatch(notification, target_id, decision, config, *, actor):
        seen.append(actor)
        return _acted(target_kind="a3-async", decision=decision)

    _register(monkeypatch, "a3-async", dispatch)
    await install.db.create_session("proposer", source="web", actor=None)
    nid = (await install.notifications.propose_action(
        "proposer", target_kind="a3-async", target_id="item-2", title="Run it?",
    ))["notification_id"]
    await install.db.record_notification_delivery(
        nid, "slack", target="C0FAKE01", message_id="1.0",
    )

    answered = await install.notifications.answer_delivered_notification(
        nid, "approve", channel="slack", target="C0FAKE01", actor="U0FAKE01",
    )
    assert answered is not None
    assert seen == [None]
    assert answered["answered_by_actor_id"] is None
    acted = [r for r in read_audit_jsonl(audit_dir) if r["event"] == "approval-acted"]
    assert acted[-1]["answered_by_actor"] == "U0FAKE01"
    assert "answered_by_actor_id" not in acted[-1]


# --------------------------------------------------------------------------- #
#  Dismissals                                                                  #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_dismiss_and_dismiss_all_record_each_person(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    await install.db.create_session("s", source="web", actor=None)
    first = await install.notifications.send_notification("s", title="One")
    second = await install.notifications.send_notification("s", title="Two")
    third = await install.notifications.send_notification("s", title="Three")

    async with _client(install.app()) as client:
        dismissed = await client.post(
            f"/api/notifications/{first}/dismiss", headers=install.headers("alice"),
        )
        all_dismissed = await client.post(
            "/api/notifications/dismiss-all", headers=install.headers("bob"),
        )
    assert dismissed.status_code == 200
    assert all_dismissed.json() == {"dismissed": 2}

    by = {
        nid: (await install.db.get_notification(nid))["dismissed_by_actor_id"]
        for nid in (first, second, third)
    }
    assert by == {first: alice, second: bob, third: bob}


# --------------------------------------------------------------------------- #
#  Silences                                                                    #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_web_silence_records_the_person(install, caplog):
    alice = install.actor_id("alice")
    async with _client(install.app()) as client:
        created = (await client.post(
            "/api/notifications/silences", headers=install.headers("alice"),
            json={"pattern": "disk usage", "reason": "known"},
        )).json()
        with caplog.at_level(logging.INFO, logger="nerve.gateway.routes.notifications"):
            removed = await client.delete(
                f"/api/notifications/silences/{created['id']}",
                headers=install.headers("bob"),
            )

    assert (created["created_by"], created["created_by_actor_id"],
            created["caused_by_actor_id"]) == ("web", alice, alice)
    assert removed.status_code == 200
    assert f"removed by actor {install.actor_id('bob')}" in caplog.text


@pytest.mark.asyncio
async def test_the_silence_tool_in_a_persons_turn_is_system_for_that_person(install):
    bob = install.actor_id("bob")
    results = install.agent_calls(
        "bobs-chat", "notification_silence",
        {"op": "add", "pattern": "nightly backup ok"},
    )
    await install.db.create_session("bobs-chat", source="web", actor=_actor(install, "bob"))
    await install.engine.run("bobs-chat", "silence the backup alerts", actor=_actor(install, "bob"))

    assert results and "Silence created" in results[0].content[0]["text"]
    (silence,) = await install.db.list_silences()
    assert (silence["created_by"], silence["created_by_actor_id"],
            silence["caused_by_actor_id"]) == ("bobs-chat", install.system_actor_id, bob)


def _actor(install: _Install, name: str) -> Actor:
    return Actor(actor_id=install.actor_id(name), kind="human")


# --------------------------------------------------------------------------- #
#  Plans through REST                                                          #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_rest_approval_records_the_approver_on_the_plan_and_the_task(install):
    bob = install.actor_id("bob")
    task_id, plan_id = await install.pending_plan("approve")
    async with _client(install.app()) as client:
        response = await client.post(
            f"/api/plans/{plan_id}/approve", headers=install.headers("bob"),
        )
        shown = (await client.get(
            f"/api/plans/{plan_id}", headers=install.headers("alice"),
        )).json()
    assert response.status_code == 200
    await install.turn_done(response.json()["impl_session_id"])

    assert await install.plan_reviewer(plan_id) == (bob, bob)
    assert (shown["reviewed_by_actor_id"], shown["reviewed_caused_by_actor_id"]) == (bob, bob)
    assert await install.task_events(task_id) == [
        ("pending", "system", None, None),
        ("in_progress", "web", bob, bob),
    ]


@pytest.mark.asyncio
async def test_rest_decline_records_the_person_on_the_plan_and_the_task(install):
    alice = install.actor_id("alice")
    task_id, plan_id = await install.pending_plan("decline")
    async with _client(install.app()) as client:
        response = await client.patch(
            f"/api/plans/{plan_id}", headers=install.headers("alice"),
            json={"status": "declined", "feedback": "not now"},
        )
    assert response.status_code == 200

    assert await install.plan_reviewer(plan_id) == (alice, alice)
    assert (await install.task_events(task_id))[-1] == ("done", "web", alice, alice)


@pytest.mark.asyncio
async def test_rest_revision_and_feedback_record_the_person(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    _, revised = await install.pending_plan("revise")
    _, commented = await install.pending_plan("comment")
    async with _client(install.app()) as client:
        response = await client.post(
            f"/api/plans/{revised}/revise", headers=install.headers("bob"),
            json={"feedback": "add tests"},
        )
        patched = await client.patch(
            f"/api/plans/{commented}", headers=install.headers("alice"),
            json={"feedback": "looks close"},
        )
    assert response.status_code == 200 and patched.status_code == 200
    await install.turn_done("proposal")

    assert await install.plan_reviewer(revised) == (bob, bob)
    assert await install.plan_reviewer(commented) == (alice, alice)


# --------------------------------------------------------------------------- #
#  Plans through tools                                                         #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_plan_approved_by_the_tool_in_alices_turn_is_system_for_alice(install):
    alice = install.actor_id("alice")
    system = install.system_actor_id
    task_id, plan_id = await install.pending_plan("tool-approve")
    results = install.agent_calls(
        "alices-chat", "plan_approve", {"plan_id": plan_id},
    )
    await install.db.create_session("alices-chat", source="web", actor=_actor(install, "alice"))
    await install.engine.run("alices-chat", "approve that plan", actor=_actor(install, "alice"))
    assert results and "approved" in results[0].content[0]["text"]
    impl_session_id = (await install.db.get_plan(plan_id))["impl_session_id"]
    await install.turn_done(impl_session_id)

    assert await install.plan_reviewer(plan_id) == (system, alice)
    assert (await install.task_events(task_id))[-1] == (
        "in_progress", "alices-chat", system, alice,
    )
    # The implementation turn runs as the system actor for Alice.
    assert install.runs[-1]["actor"] == install.db.system_actor
    assert await install.session_cause_of(impl_session_id) == alice
    assert await install.user_rows(impl_session_id) == [
        (install.runs[-1]["user_message"], system),
    ]


def _tool_ctx(install: _Install, session_id: str, cause: str | None) -> ToolContext:
    return ToolContext(
        session_id=session_id, workspace=install.config.workspace,
        db=install.db, config=install.config, engine=install.engine,
        actor=install.db.system_actor, cause=lambda: cause,
    )


@pytest.mark.asyncio
async def test_tool_decline_and_revision_are_system_for_the_turns_cause(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    system = install.system_actor_id
    task_id, declined = await install.pending_plan("tool-decline")
    _, revised = await install.pending_plan("tool-revise")

    await plan_decline_handler(_tool_ctx(install, "alices-chat", alice), {"plan_id": declined})
    await plan_revise_handler(
        _tool_ctx(install, "bobs-chat", bob), {"plan_id": revised, "feedback": "split it"},
    )
    await install.turn_done("proposal")

    assert await install.plan_reviewer(declined) == (system, alice)
    assert (await install.task_events(task_id))[-1] == ("done", "alices-chat", system, alice)
    assert await install.plan_reviewer(revised) == (system, bob)
    # The revision created the planner session for Bob.
    assert await install.session_cause_of("proposal") == bob


@pytest.mark.asyncio
async def test_a_plan_proposed_in_alices_turn_is_written_by_system_for_alice(install):
    alice = install.actor_id("alice")
    task_id, _ = await install.pending_plan("authorship")
    await install.db.update_plan("plan-authorship", status="declined")
    results = install.agent_calls(
        "alices-chat", "plan_propose", {"task_id": task_id, "content": "Do it this way"},
    )
    await install.db.create_session("alices-chat", source="web", actor=_actor(install, "alice"))
    await install.engine.run("alices-chat", "plan that task", actor=_actor(install, "alice"))
    assert results and "Plan proposed" in results[0].content[0]["text"]

    (proposed,) = [p for p in await install.db.get_plans_for_task(task_id) if p["version"] == 2]
    assert (proposed["created_by_actor_id"], proposed["caused_by_actor_id"]) == (
        install.system_actor_id, alice,
    )
    assert await install.plan_reviewer(proposed["id"]) == (None, None)


@pytest.mark.asyncio
async def test_a_new_version_records_its_writer_and_the_old_keeps_its_reviewer(install):
    bob = install.actor_id("bob")
    system = install.system_actor_id
    task_id, plan_id = await install.pending_plan("supersede")
    async with _client(install.app()) as client:
        await client.post(
            f"/api/plans/{plan_id}/revise", headers=install.headers("bob"),
            json={"feedback": "shorter"},
        )
    await install.turn_done("proposal")

    # The planner's revision turn runs for Bob and replaces its own plan.
    await plan_update_handler(
        _tool_ctx(install, "proposal", bob), {"plan_id": plan_id, "content": "Ship less"},
    )
    old = await install.plan_row(plan_id)
    assert old["status"] == "superseded"
    assert await install.plan_reviewer(plan_id) == (bob, bob)
    (new,) = [p for p in await install.db.get_plans_for_task(task_id) if p["parent_plan_id"] == plan_id]
    assert (new["status"], new["created_by_actor_id"], new["caused_by_actor_id"]) == (
        "pending", system, bob,
    )
    assert await install.plan_reviewer(new["id"]) == (None, None)


# --------------------------------------------------------------------------- #
#  Concurrent plan decisions                                                   #
# --------------------------------------------------------------------------- #


async def _stale_plan_reads(install: _Install, monkeypatch, plan_id: str) -> None:
    """Every later ``get_plan(plan_id)`` returns the row as it is now.

    A second decision that read the plan while it was still pending, before
    the first decision wrote, sees exactly this.
    """
    stale = await install.db.get_plan(plan_id)
    real = install.db.get_plan

    async def _get_plan(pid):
        return dict(stale) if pid == plan_id else await real(pid)

    monkeypatch.setattr(install.db, "get_plan", _get_plan)


@pytest.mark.asyncio
async def test_a_second_approval_from_a_stale_read_changes_nothing(install, monkeypatch):
    alice = install.actor_id("alice")
    task_id, plan_id = await install.pending_plan("race-approve")
    await _stale_plan_reads(install, monkeypatch, plan_id)
    async with _client(install.app()) as client:
        first = await client.post(
            f"/api/plans/{plan_id}/approve", headers=install.headers("alice"),
        )
        second = await client.post(
            f"/api/plans/{plan_id}/approve", headers=install.headers("bob"),
        )
    assert (first.status_code, second.status_code) == (200, 409)
    impl_session_id = first.json()["impl_session_id"]
    await install.turn_done(impl_session_id)

    plan = await install.plan_row(plan_id)
    assert (plan["status"], plan["impl_session_id"]) == ("implementing", impl_session_id)
    assert await install.plan_reviewer(plan_id) == (alice, alice)
    assert [run["session_id"] for run in install.impl_runs()] == [impl_session_id]
    assert [e[0] for e in await install.task_events(task_id)] == ["pending", "in_progress"]


@pytest.mark.asyncio
async def test_a_decline_or_revision_after_an_approval_changes_nothing(install, monkeypatch):
    alice = install.actor_id("alice")
    task_id, plan_id = await install.pending_plan("race-decline")
    await _stale_plan_reads(install, monkeypatch, plan_id)
    async with _client(install.app()) as client:
        approved = await client.post(
            f"/api/plans/{plan_id}/approve", headers=install.headers("alice"),
        )
        declined = await client.patch(
            f"/api/plans/{plan_id}", headers=install.headers("bob"),
            json={"status": "declined", "feedback": "too late"},
        )
        revised = await client.post(
            f"/api/plans/{plan_id}/revise", headers=install.headers("bob"),
            json={"feedback": "too late"},
        )
    assert approved.status_code == 200
    assert (declined.status_code, revised.status_code) == (409, 409)
    await install.turn_done(approved.json()["impl_session_id"])

    plan = await install.plan_row(plan_id)
    assert (plan["status"], plan["feedback"]) == ("implementing", None)
    assert await install.plan_reviewer(plan_id) == (alice, alice)
    assert [e[0] for e in await install.task_events(task_id)] == ["pending", "in_progress"]
    assert [run["session_id"] for run in install.runs if run["session_id"] == "proposal"] == []


@pytest.mark.asyncio
async def test_tool_decisions_after_an_approval_change_nothing(install, monkeypatch):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    task_id, plan_id = await install.pending_plan("race-tool")
    await _stale_plan_reads(install, monkeypatch, plan_id)
    async with _client(install.app()) as client:
        approved = await client.post(
            f"/api/plans/{plan_id}/approve", headers=install.headers("alice"),
        )
    await install.turn_done(approved.json()["impl_session_id"])

    ctx = _tool_ctx(install, "bobs-chat", bob)
    for handler, args in (
        (plan_approve_handler, {"plan_id": plan_id}),
        (plan_decline_handler, {"plan_id": plan_id}),
        (plan_revise_handler, {"plan_id": plan_id, "feedback": "again"}),
        (plan_update_handler, {"plan_id": plan_id, "content": "Ship more"}),
    ):
        result = await handler(ctx, args)
        assert "pending" in result.content[0]["text"], handler.__name__

    plan = await install.plan_row(plan_id)
    assert plan["status"] == "implementing"
    assert await install.plan_reviewer(plan_id) == (alice, alice)
    assert len(install.impl_runs()) == 1
    assert [e[0] for e in await install.task_events(task_id)] == ["pending", "in_progress"]
    assert [p["id"] for p in await install.db.get_plans_for_task(task_id)] == [plan_id]


@pytest.mark.asyncio
async def test_a_revision_excludes_every_other_decision_while_it_runs(
    install, monkeypatch,
):
    bob = install.actor_id("bob")
    task_id, plan_id = await install.pending_plan("race-revise")
    install.holds["proposal"] = asyncio.Event()
    await _stale_plan_reads(install, monkeypatch, plan_id)
    async with _client(install.app()) as client:
        revised = await client.post(
            f"/api/plans/{plan_id}/revise", headers=install.headers("bob"),
            json={"feedback": "add tests"},
        )
        assert revised.status_code == 200
        assert (await install.plan_row(plan_id))["status"] == "revising"
        # Each of these read the plan while it was still pending.
        approved = await client.post(
            f"/api/plans/{plan_id}/approve", headers=install.headers("alice"),
        )
        declined = await client.patch(
            f"/api/plans/{plan_id}", headers=install.headers("alice"),
            json={"status": "declined"},
        )
        revised_again = await client.post(
            f"/api/plans/{plan_id}/revise", headers=install.headers("alice"),
            json={"feedback": "no, remove tests"},
        )
    assert (approved.status_code, declined.status_code, revised_again.status_code) == (
        409, 409, 409,
    )
    ctx = _tool_ctx(install, "alices-chat", install.actor_id("alice"))
    for handler, args in (
        (plan_approve_handler, {"plan_id": plan_id}),
        (plan_decline_handler, {"plan_id": plan_id}),
        (plan_revise_handler, {"plan_id": plan_id, "feedback": "again"}),
    ):
        result = await handler(ctx, args)
        assert "pending" in result.content[0]["text"], handler.__name__

    plan = await install.plan_row(plan_id)
    assert (plan["status"], plan["feedback"]) == ("revising", "add tests")
    assert await install.plan_reviewer(plan_id) == (bob, bob)
    assert install.impl_runs() == []
    assert [e[0] for e in await install.task_events(task_id)] == ["pending"]
    assert [r["session_id"] for r in install.runs].count("proposal") == 1

    # The revision turn ends without a new version: the plan is open again.
    install.holds["proposal"].set()
    await install.turn_done("proposal")
    assert (await install.plan_row(plan_id))["status"] == "pending"


def _read_together(install: _Install, monkeypatch, plan_id: str, count: int) -> None:
    """The first ``count`` reads of ``plan_id`` return only when all have read.

    So every request sees the plan pending before any of them decides.
    """
    real = install.db.get_plan
    arrived = 0
    all_in = asyncio.Event()

    async def _get_plan(pid):
        nonlocal arrived
        row = await real(pid)
        if pid == plan_id and not all_in.is_set():
            arrived += 1
            if arrived >= count:
                all_in.set()
            await asyncio.wait_for(all_in.wait(), timeout=5)
        return row

    monkeypatch.setattr(install.db, "get_plan", _get_plan)


@pytest.mark.parametrize("other", ["approve", "decline", "revise"])
@pytest.mark.asyncio
async def test_a_revision_and_a_concurrent_decision_apply_once(install, monkeypatch, other):
    task_id, plan_id = await install.pending_plan(f"race-pair-{other}")
    install.holds["proposal"] = asyncio.Event()
    _read_together(install, monkeypatch, plan_id, 2)
    async with _client(install.app()) as client:
        if other == "approve":
            second = client.post(f"/api/plans/{plan_id}/approve", headers=install.headers("alice"))
        elif other == "decline":
            second = client.patch(
                f"/api/plans/{plan_id}", headers=install.headers("alice"),
                json={"status": "declined"},
            )
        else:
            second = client.post(
                f"/api/plans/{plan_id}/revise", headers=install.headers("alice"),
                json={"feedback": "longer"},
            )
        first = client.post(
            f"/api/plans/{plan_id}/revise", headers=install.headers("bob"),
            json={"feedback": "shorter"},
        )
        # The revision reads last, so it is the first to write after the
        # reads; the other decision then writes from the same pending read.
        responses = await asyncio.gather(second, first)
    codes = [r.status_code for r in responses]
    assert sorted(codes) == [200, 409], codes
    revision_turns = [r for r in install.runs if r["session_id"] == "proposal"]
    implementations = install.impl_runs()
    # One decision, so at most one kind of work started.
    assert len(revision_turns) + len(implementations) <= 1
    install.holds["proposal"].set()
    if revision_turns:
        await install.turn_done("proposal")
    for run in implementations:
        await install.turn_done(run["session_id"])


@pytest.mark.asyncio
async def test_concurrent_revisions_and_decisions_apply_once(install, monkeypatch):
    task_id, plan_id = await install.pending_plan("race-mixed")
    install.holds["proposal"] = asyncio.Event()
    _read_together(install, monkeypatch, plan_id, 4)
    async with _client(install.app()) as client:
        responses = await asyncio.gather(
            client.post(
                f"/api/plans/{plan_id}/revise", headers=install.headers("bob"),
                json={"feedback": "shorter"},
            ),
            client.post(f"/api/plans/{plan_id}/approve", headers=install.headers("alice")),
            client.patch(
                f"/api/plans/{plan_id}", headers=install.headers("alice"),
                json={"status": "declined"},
            ),
            client.post(
                f"/api/plans/{plan_id}/revise", headers=install.headers("alice"),
                json={"feedback": "longer"},
            ),
        )
    codes = [r.status_code for r in responses]
    assert sorted(codes) == [200, 409, 409, 409], codes
    status = (await install.plan_row(plan_id))["status"]
    winner = codes.index(200)
    assert status == ("revising", "implementing", "declined", "revising")[winner]
    revision_turns = [r for r in install.runs if r["session_id"] == "proposal"]
    assert len(revision_turns) == (1 if winner in (0, 3) else 0)
    assert len(install.impl_runs()) == (1 if winner == 1 else 0)
    install.holds["proposal"].set()
    if revision_turns:
        await install.turn_done("proposal")
    if winner == 1:
        await install.turn_done(responses[1].json()["impl_session_id"])


@pytest.mark.asyncio
async def test_the_planner_replaces_a_revising_plan_and_it_stays_replaced(install):
    bob = install.actor_id("bob")
    task_id, plan_id = await install.pending_plan("revise-update")
    install.agent_calls("proposal", "plan_update", {"plan_id": plan_id, "content": "Ship less"})
    async with _client(install.app()) as client:
        revised = await client.post(
            f"/api/plans/{plan_id}/revise", headers=install.headers("bob"),
            json={"feedback": "shorter"},
        )
    assert revised.status_code == 200
    await install.turn_done("proposal")

    assert (await install.plan_row(plan_id))["status"] == "superseded"
    (new,) = [p for p in await install.db.get_plans_for_task(task_id) if p["parent_plan_id"] == plan_id]
    assert (new["status"], new["created_by_actor_id"], new["caused_by_actor_id"]) == (
        "pending", install.system_actor_id, bob,
    )


@pytest.mark.asyncio
async def test_a_task_with_a_revising_plan_takes_no_new_proposal(install):
    task_id, plan_id = await install.pending_plan("revising-propose")
    await install.db.update_plan(plan_id, status="revising")
    result = await plan_propose_handler(
        _tool_ctx(install, "planner", None), {"task_id": task_id, "content": "Another"},
    )
    assert "already has a pending" in result.content[0]["text"]
    assert [p["id"] for p in await install.db.get_plans_for_task(task_id)] == [plan_id]


@pytest.mark.asyncio
async def test_startup_reopens_plans_left_revising(install):
    _, revising = await install.pending_plan("left-revising")
    _, declined = await install.pending_plan("left-declined")
    await install.db.update_plan(revising, status="revising")
    await install.db.update_plan(declined, status="declined")
    assert await install.db.reopen_revising_plans() == 1
    assert (await install.plan_row(revising))["status"] == "pending"
    assert (await install.plan_row(declined))["status"] == "declined"


@pytest.mark.asyncio
async def test_concurrent_approvals_start_one_implementation(install):
    task_id, plan_id = await install.pending_plan("race-gather")
    async with _client(install.app()) as client:
        responses = await asyncio.gather(*(
            client.post(f"/api/plans/{plan_id}/approve", headers=install.headers(name))
            for name in ("alice", "bob", "alice", "bob")
        ))
    winners = [r for r in responses if r.status_code == 200]
    assert len(winners) == 1
    assert sorted(r.status_code for r in responses) == [200, 409, 409, 409]
    await install.turn_done(winners[0].json()["impl_session_id"])
    assert len(install.impl_runs()) == 1


# --------------------------------------------------------------------------- #
#  The actor directory                                                         #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_actor_directory_names_every_id_in_the_new_columns(install):
    await install.db.create_session("s", source="web", actor=None)
    question = (await install.notifications.ask_question("s", title="Q"))["notification_id"]
    note = await install.notifications.send_notification("s", title="N")
    task_id, _ = await install.pending_plan("directory-old")
    await install.db.update_plan("plan-directory-old", status="declined")
    await plan_propose_handler(
        _tool_ctx(install, "planner", install.actor_id("bob")),
        {"task_id": task_id, "content": "Ship it"},
    )
    (plan_id,) = [
        p["id"] for p in await install.db.get_plans_for_task(task_id)
        if p["status"] == "pending"
    ]
    async with _client(install.app()) as client:
        await client.post(
            f"/api/notifications/{question}/answer", headers=install.headers("alice"),
            json={"answer": "ok"},
        )
        await client.post(
            f"/api/notifications/{note}/dismiss", headers=install.headers("bob"),
        )
        await client.post(
            "/api/notifications/silences", headers=install.headers("bob"),
            json={"pattern": "heartbeat"},
        )
        approved = await client.post(
            f"/api/plans/{plan_id}/approve", headers=install.headers("alice"),
        )
        directory = {
            row["id"]
            for row in (
                await client.get("/api/actors", headers=install.headers("bob"))
            ).json()["actors"]
        }
    await install.turn_done("s")
    await install.turn_done(approved.json()["impl_session_id"])

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
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
        "auth": {"jwt_secret": _SECRET},
        "notifications": {"channels": ["web"]},
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
        installed = _Install(database, engine, config)
        installed.watch(monkeypatch)
        yield installed
    finally:
        await database.close()
        set_config(NerveConfig())


@pytest.mark.asyncio
async def test_external_mode_records_the_principal(external_install):
    install = external_install
    carol = _as(CAROL_PRINCIPAL, "Carol")
    await install.db.create_session("asker", source="web", actor=None)
    nid = (await install.notifications.ask_question("asker", title="Go?"))["notification_id"]
    _, plan_id = await install.pending_plan("external")
    async with _client(install.app()) as client:
        answered = await client.post(
            f"/api/notifications/{nid}/answer", headers=carol, json={"answer": "go"},
        )
        approved = await client.post(f"/api/plans/{plan_id}/approve", headers=carol)
    assert answered.status_code == 200 and approved.status_code == 200
    await install.turn_done("asker")
    await install.turn_done(approved.json()["impl_session_id"])

    assert (await install.db.get_notification(nid))["answered_by_actor_id"] == CAROL_PRINCIPAL
    assert await install.user_rows("asker") == [
        ("[Answer to: Go?]\n\ngo", CAROL_PRINCIPAL),
    ]
    assert await install.session_cause_of(approved.json()["impl_session_id"]) == CAROL_PRINCIPAL
    assert await install.plan_reviewer(plan_id) == (CAROL_PRINCIPAL, CAROL_PRINCIPAL)
