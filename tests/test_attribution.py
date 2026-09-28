"""Integration coverage for persisted session and message attribution."""

from __future__ import annotations

import asyncio
import pathlib
import sqlite3
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from nerve.agent.engine import AgentEngine
from nerve.agent.streaming import broadcaster
from nerve.agent.tools.handlers.plans import plan_approve_handler
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig, set_config
from nerve.gateway.auth import create_session_token, hash_password, pin_jwt_secret
from nerve.gateway.routes import init_deps, register_all_routes
from nerve.gateway.server import create_app
from nerve.identity import Actor

_SECRET = "test-secret-for-attribution-padded-to-32b"
_ALICE_PASSWORD = "correct-horse-battery-staple"
_BOB_PASSWORD = "a-different-passphrase-entirely"


class _Install:
    def __init__(self, db, identity, engine, config):
        self.db = db
        self.identity = identity
        self.engine = engine
        self.config = config
        self.alice_account = identity.owner_account_id
        self.alice_actor = identity.owner_actor_id
        self.bob_account = ""
        self.bob_actor = ""

    async def add_bob(self) -> None:
        await self.db.update_account_login(
            self.alice_account,
            username="alice",
            credential=hash_password(_ALICE_PASSWORD),
        )
        await self.db.update_actor_profile(self.alice_actor, display_name="Alice")
        bob = await self.db.create_managed_account(
            username="bob",
            credential=hash_password(_BOB_PASSWORD),
            display_name="Bob",
        )
        self.bob_account = bob["id"]
        self.bob_actor = bob["actor_id"]

    @property
    def system_actor_id(self) -> str:
        return self.identity.system_actor_id

    def token(self, account_id: str) -> str:
        return create_session_token(_SECRET, account_id)

    @property
    def alice(self) -> dict:
        return {"Authorization": f"Bearer {self.token(self.alice_account)}"}

    @property
    def bob(self) -> dict:
        return {"Authorization": f"Bearer {self.token(self.bob_account)}"}

    def app(self) -> FastAPI:
        app = FastAPI()
        app.include_router(register_all_routes())
        return app

    async def creator_of(self, session_id: str) -> str | None:
        return (await self.db.get_session(session_id))["created_by_actor_id"]

    async def messages_in(self, session_id: str) -> list[dict]:
        return await self.db.get_messages(session_id)

    async def said_in(self, session_id: str) -> list[tuple[str, str | None]]:
        return [
            (row["content"], row["actor_id"])
            for row in await self.messages_in(session_id)
            if row["role"] == "user"
        ]


@pytest_asyncio.fixture
async def install(tmp_path, open_identity_db, wire_identity_store):
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
        "auth": {"jwt_secret": _SECRET},
    })
    set_config(config)
    pin_jwt_secret(_SECRET)
    database, identity = await open_identity_db(tmp_path / "nerve.db")
    wire_identity_store(database)
    engine = AgentEngine(config, database)
    init_deps(engine, database)
    installed = _Install(database, identity, engine, config)
    await installed.add_bob()
    try:
        yield installed
    finally:
        await database.close()
        set_config(NerveConfig())


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://nerve-test",
    )


def _no_model(engine: AgentEngine, monkeypatch) -> None:
    async def _refuse(*args, **kwargs):
        raise RuntimeError("no model in this test")

    monkeypatch.setattr(engine, "_get_or_create_client", _refuse)


async def _run_later(client, headers, session_id, message):
    return await client.post(
        "/api/sessions/run-later",
        headers=headers,
        json={"session_id": session_id, "message": message, "delay": "none"},
    )


async def _pending_plan(install: _Install, suffix: str) -> tuple[str, str]:
    task_id = f"task-{suffix}"
    plan_id = f"plan-{suffix}"
    install.config.workspace.mkdir(exist_ok=True)
    task_file = install.config.workspace / f"{task_id}.md"
    task_file.write_text("# Attribution plan\n", encoding="utf-8")
    await install.db.upsert_task(
        task_id=task_id, file_path=task_file.name, title="Attribution plan",
        status="pending", content="# Attribution plan\n",
    )
    await install.db.create_plan(
        plan_id=plan_id, task_id=task_id, content="Ship it",
        session_id="proposal", version=1,
    )
    return task_id, plan_id


def _finish_implementation_immediately(install: _Install, monkeypatch) -> None:
    async def _done(**kwargs):
        return None

    monkeypatch.setattr(install.engine, "run", _done)


@pytest.mark.asyncio
async def test_http_plan_approval_attributes_the_session_to_the_approver(
    install, monkeypatch,
):
    _, plan_id = await _pending_plan(install, "http")
    _finish_implementation_immediately(install, monkeypatch)

    async with _client(install.app()) as client:
        response = await client.post(
            f"/api/plans/{plan_id}/approve", headers=install.alice,
        )

    assert response.status_code == 200
    session = await install.db.get_session(response.json()["impl_session_id"])
    assert session["created_by_actor_id"] == install.alice_actor


@pytest.mark.asyncio
async def test_agent_plan_approval_attributes_the_session_to_nerve(
    install, monkeypatch,
):
    _, plan_id = await _pending_plan(install, "agent")
    _finish_implementation_immediately(install, monkeypatch)
    ctx = ToolContext(
        session_id="system", workspace=install.config.workspace,
        db=install.db, config=install.config, engine=install.engine,
    )

    await plan_approve_handler(ctx, {"plan_id": plan_id})

    plan = await install.db.get_plan(plan_id)
    session = await install.db.get_session(plan["impl_session_id"])
    assert session["created_by_actor_id"] == install.system_actor_id


@pytest.mark.asyncio
async def test_http_keeps_concurrent_people_distinct(install):
    """Exercise authentication, routes, and both persistence columns."""
    async with _client(install.app()) as client:
        hers, his = await asyncio.gather(
            client.post("/api/sessions", headers=install.alice, json={}),
            client.post("/api/sessions", headers=install.bob, json={}),
        )
        assert hers.json()["created_by_actor_id"] == install.alice_actor
        assert his.json()["created_by_actor_id"] == install.bob_actor

        shared = hers.json()["id"]
        alice_send, bob_send = await asyncio.gather(
            _run_later(client, install.alice, shared, "from Alice"),
            _run_later(client, install.bob, shared, "from Bob"),
        )
        assert alice_send.status_code == bob_send.status_code == 200

    assert sorted(await install.said_in(shared)) == sorted([
        ("from Alice", install.alice_actor),
        ("from Bob", install.bob_actor),
    ])
    assert [
        row["actor_id"] for row in await install.messages_in(shared)
        if row["role"] == "assistant"
    ] == [None, None]


@pytest.mark.asyncio
async def test_chat_and_read_apis_preserve_the_request_actor(
    install, monkeypatch,
):
    _no_model(install.engine, monkeypatch)
    async with _client(install.app()) as client:
        session_id = (
            await client.post("/api/sessions", headers=install.bob, json={})
        ).json()["id"]
        response = await client.post(
            "/api/chat",
            headers=install.bob,
            json={"session_id": session_id, "message": "hello"},
        )
        listed = (await client.get("/api/sessions", headers=install.alice)).json()
        messages = (
            await client.get(
                f"/api/sessions/{session_id}/messages", headers=install.alice,
            )
        ).json()["messages"]

    assert response.status_code == 200
    session = next(row for row in listed["sessions"] if row["id"] == session_id)
    assert session["created_by_actor_id"] == install.bob_actor
    assert [(row["role"], row["actor_id"]) for row in messages] == [
        ("user", install.bob_actor),
        ("assistant", None),
    ]


class _Socket:
    def __init__(self, token: str, frames: list[dict]):
        self.query_params = {"token": token}
        self.cookies = {}
        self.sent: list[dict] = []
        self._frames = list(frames)

    async def accept(self) -> None:
        return None

    async def close(self, code: int = 1000, reason: str = "") -> None:
        return None

    async def send_json(self, payload: dict) -> None:
        self.sent.append(payload)

    async def receive_json(self) -> dict:
        from starlette.websockets import WebSocketDisconnect

        if not self._frames:
            raise WebSocketDisconnect(1000)
        return self._frames.pop(0)


def _ws_endpoint():
    app = create_app()
    return next(
        route.endpoint
        for route in app.routes
        if getattr(route, "path", "") == "/ws"
    )


@pytest.mark.asyncio
async def test_websocket_connections_keep_their_actors(install, monkeypatch):
    _no_model(install.engine, monkeypatch)
    monkeypatch.setattr("nerve.gateway.server._engine", install.engine)
    endpoint = _ws_endpoint()
    session_id = "ws-shared"
    await install.db.create_session(session_id, source="web", actor=None)

    echoes: list[dict] = []
    await broadcaster.register(session_id, "listener", lambda _sid, msg: echoes.append(msg))
    try:
        await asyncio.gather(
            endpoint(_Socket(install.token(install.alice_account), [{
                "type": "message", "content": "from Alice", "session_id": session_id,
            }])),
            endpoint(_Socket(install.token(install.bob_account), [{
                "type": "message", "content": "from Bob", "session_id": session_id,
            }])),
        )
        await _wait_for_user_messages(install, session_id, 2)
    finally:
        await broadcaster.unregister(session_id, "listener")

    assert sorted(await install.said_in(session_id)) == sorted([
        ("from Alice", install.alice_actor),
        ("from Bob", install.bob_actor),
    ])
    assert {
        (msg["content"], msg["actor_id"])
        for msg in echoes if msg.get("type") == "user_message"
    } == {
        ("from Alice", install.alice_actor),
        ("from Bob", install.bob_actor),
    }


@pytest.mark.asyncio
async def test_websocket_created_session_belongs_to_the_connection(
    install, monkeypatch,
):
    monkeypatch.setattr("nerve.gateway.server._engine", install.engine)
    socket = _Socket(install.token(install.bob_account), [])
    await _ws_endpoint()(socket)
    switched = next(msg for msg in socket.sent if msg["type"] == "session_switched")
    assert await install.creator_of(switched["session_id"]) == install.bob_actor


async def _wait_for_user_messages(install, session_id: str, count: int) -> None:
    for _ in range(200):
        rows = await install.messages_in(session_id)
        if sum(row["role"] == "user" for row in rows) >= count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"{session_id} did not receive {count} user messages")


@pytest.mark.asyncio
async def test_autonomous_cron_uses_the_system_actor(install, monkeypatch):
    _no_model(install.engine, monkeypatch)
    await install.engine.run_cron(job_id="nightly", prompt="do the thing")
    session = next(
        row for row in await install.db.list_sessions(limit=50)
        if row["source"] == "cron"
    )
    assert session["created_by_actor_id"] == install.system_actor_id
    assert [(row["role"], row["actor_id"]) for row in await install.messages_in(session["id"])] == [
        ("user", install.system_actor_id),
        ("assistant", None),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("channel_name", ["slack", "telegram"])
async def test_unidentified_channel_human_stays_unattributed(
    install, monkeypatch, channel_name,
):
    _no_model(install.engine, monkeypatch)
    from nerve.channels.base import InboundMessage

    channel = SimpleNamespace(
        name=channel_name,
        capabilities=set(),
        format_response=lambda text: text,
    )
    router = install.engine.router
    router._channels[channel_name] = channel
    monkeypatch.setattr(router, "_setup_streaming", _noop)
    monkeypatch.setattr(router, "_teardown_streaming", _noop)
    monkeypatch.setattr(type(router), "BATCH_DEBOUNCE", 0)

    key = f"{channel_name}:1"
    await router.handle_message(InboundMessage(
        channel_name=channel_name,
        channel_key=key,
        sender_id="provider-person-1",
        text="hello from a chat app",
    ))
    session_id = await install.engine.sessions.get_last_session(key)
    assert await install.creator_of(session_id) is None
    assert await install.said_in(session_id) == [("hello from a chat app", None)]


async def _noop(*args, **kwargs):
    return None


class _SilentBroadcaster:
    async def broadcast(self, *args, **kwargs):
        return None


def _codex_thread(thread_id: str, cwd: str):
    from datetime import datetime, timezone

    from nerve.sources.codex_threads.base import ThreadEvent

    now = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)

    def event(type_: str, payload: dict, sequence: int) -> ThreadEvent:
        return ThreadEvent(
            type=type_,  # type: ignore[arg-type]
            thread_id=thread_id,
            sequence=sequence,
            timestamp=now,
            payload=payload,
        )

    return [
        event("thread_in_scope", {
            "id": thread_id,
            "cwd": cwd,
            "originator": "codex_exec",
            "cli_version": "0.130.0",
            "source": "exec",
        }, 1),
        event("user_message", {"message": "hi", "event_id": "e1"}, 2),
        event("assistant_message", {"message": "hello", "event_id": "e2"}, 3),
    ]


@pytest.mark.asyncio
async def test_codex_sync_session_is_system_but_imported_human_is_unknown(install):
    from nerve.sources.codex_threads.base import WorkspaceFilter
    from nerve.sources.codex_threads.ingester import CodexIngester

    workspace = str(install.config.workspace)
    ingester = CodexIngester(
        install.db,
        origin_id="origin-1",
        workspace_filter=WorkspaceFilter(
            mode="nerve_workspace",
            nerve_workspace_path=pathlib.Path(workspace),
        ),
        broadcaster=_SilentBroadcaster(),
    )
    for event in _codex_thread("thread-aaa", workspace):
        await ingester.ingest(event)

    session_id = "codex:thread-aaa"
    assert await install.creator_of(session_id) == install.system_actor_id
    assert [(row["role"], row["actor_id"]) for row in await install.messages_in(session_id)] == [
        ("user", None),
        ("assistant", None),
    ]


@pytest.mark.asyncio
async def test_bulk_actor_directory_is_current_and_account_private(install):
    async with _client(install.app()) as client:
        assert (await client.get("/api/actors")).status_code == 401
        response = await client.get("/api/actors", headers=install.alice)
        assert response.status_code == 200
        actors = {row["id"]: row for row in response.json()["actors"]}
        assert set(actors) == {
            install.alice_actor, install.bob_actor, install.system_actor_id,
        }
        assert actors[install.alice_actor] == {
            "id": install.alice_actor,
            "kind": "human",
            "display_name": "Alice",
        }

        session_id = (
            await client.post("/api/sessions", headers=install.bob, json={})
        ).json()["id"]
        await _run_later(client, install.bob, session_id, "before rename")
        await install.db.disable_account(install.bob_account)
        await install.db.update_actor_profile(install.bob_actor, display_name="Robert")
        refreshed = {
            row["id"]: row
            for row in (
                await client.get("/api/actors", headers=install.alice)
            ).json()["actors"]
        }

    assert refreshed[install.bob_actor]["display_name"] == "Robert"
    assert set(refreshed[install.bob_actor]) == {"id", "kind", "display_name"}
    assert await install.creator_of(session_id) == install.bob_actor
    assert await install.said_in(session_id) == [("before rename", install.bob_actor)]


@pytest.mark.asyncio
async def test_migration_keeps_existing_history_null_and_is_idempotent(install):
    raw = install.db.db
    await raw.execute("ALTER TABLE sessions DROP COLUMN created_by_actor_id")
    await raw.execute("ALTER TABLE messages DROP COLUMN actor_id")
    await raw.execute(
        "INSERT INTO sessions (id, title, source, status, created_at, updated_at)"
        " VALUES ('old', 'Old chat', 'web', 'idle', '2020-01-01', '2020-01-01')"
    )
    await raw.execute(
        "INSERT INTO messages (session_id, role, content, created_at)"
        " VALUES ('old', 'user', 'from before', '2020-01-01')"
    )
    await raw.commit()

    from nerve.db.migrations.v048_attribution import up

    await up(raw)
    await up(raw)
    await raw.commit()
    assert await install.creator_of("old") is None
    assert await install.said_in("old") == [("from before", None)]
    async with _client(install.app()) as client:
        sessions = (await client.get("/api/sessions", headers=install.alice)).json()
        messages = (
            await client.get("/api/sessions/old/messages", headers=install.alice)
        ).json()["messages"]
    old = next(row for row in sessions["sessions"] if row["id"] == "old")
    assert old["created_by_actor_id"] is None
    assert messages[0]["actor_id"] is None


@pytest.mark.asyncio
async def test_foreign_keys_and_write_once_exclude_false_attribution(install):
    ghost = Actor(actor_id="00000000-0000-4000-8000-0000000000ff", kind="human")
    with pytest.raises(sqlite3.IntegrityError):
        await install.db.create_session("ghost", actor=ghost)
    await install.db.create_session("ghost-message", actor=None)
    with pytest.raises(sqlite3.IntegrityError):
        await install.db.add_message("ghost-message", "user", "hi", actor=ghost)

    alice = Actor(
        actor_id=install.alice_actor,
        kind="human",
        account_id=install.alice_account,
    )
    bob = Actor(
        actor_id=install.bob_actor,
        kind="human",
        account_id=install.bob_account,
    )
    await install.db.create_session("repeat", title="Hers", actor=alice)
    repeated = await install.db.create_session("repeat", title="His", actor=bob)
    await install.db.update_session_fields(
        "repeat", {"created_by_actor_id": install.bob_actor},
    )
    assert repeated["created_by_actor_id"] == install.alice_actor
    assert repeated["title"] == "Hers"
    assert await install.creator_of("repeat") == install.alice_actor


@pytest.mark.asyncio
async def test_fork_records_the_forker_and_preserves_senders(install):
    async with _client(install.app()) as client:
        session_id = (
            await client.post("/api/sessions", headers=install.alice, json={})
        ).json()["id"]
        await _run_later(client, install.alice, session_id, "Alice wrote this")
        await install.db.update_session_fields(
            session_id, {"sdk_session_id": "native-1"},
        )
        fork = await client.post(
            "/api/sessions/fork",
            headers=install.bob,
            json={"source_session_id": session_id},
        )

    assert fork.status_code == 200
    fork_id = fork.json()["id"]
    assert await install.creator_of(fork_id) == install.bob_actor
    assert await install.said_in(fork_id) == [
        ("Alice wrote this", install.alice_actor),
    ]
