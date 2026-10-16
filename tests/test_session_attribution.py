"""Session change attribution: who changed a session, and on whose behalf.

A session event records the actor (``actor_id``) and the cause
(``caused_by_actor_id``) of the change. A person who changes a session through
REST or the browser WebSocket is both. A change by the agent or by Nerve is
the system actor's, for the cause of the turn that made it, or for the system
actor when Nerve does it on its own. A delete removes the session's events, so
``session_deletions`` records it. These tests drive the real routes, engine,
session manager and database; only the model runtime is replaced.
"""

from __future__ import annotations

import asyncio
import pathlib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from nerve.agent.backends import TurnCompleted
from nerve.agent.engine import AgentEngine
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig, set_config
from nerve.db import Database
from nerve.db.actor_columns import ACTOR_COLUMNS
from nerve.gateway.auth import create_session_token, hash_password, pin_jwt_secret
from nerve.gateway.routes import init_deps, register_all_routes
from nerve.gateway.server import create_app
from nerve.identity import Attribution
from nerve.mcp_server.audit import build_audit_writer

_SECRET = "test-secret-for-session-attribution-32b"
_PASSWORD_HASH = hash_password("correct-horse-battery-staple")

_NEW_COLUMNS = (
    ("session_events", "actor_id"),
    ("session_events", "caused_by_actor_id"),
    ("session_deletions", "actor_id"),
    ("session_deletions", "caused_by_actor_id"),
)


class _Runtime:
    """A model runtime client whose turns complete at once."""

    native_session_id = None

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
        yield TurnCompleted()


class _Install:
    def __init__(self, db: Database, engine: AgentEngine, config: NerveConfig):
        self.db = db
        self.engine = engine
        self.config = config
        self.people: dict[str, dict] = {}

    @property
    def system(self) -> str:
        return self.db.system_actor_id

    def actor_id(self, name: str) -> str:
        return self.people[name]["actor_id"]

    def token(self, name: str) -> str:
        return create_session_token(_SECRET, self.people[name]["id"])

    def headers(self, name: str) -> dict:
        return {"Authorization": f"Bearer {self.token(name)}"}

    def app(self) -> FastAPI:
        app = FastAPI()
        app.include_router(register_all_routes())
        return app

    async def events(self, session_id: str, *types: str) -> list[tuple]:
        """``(event_type, actor_id, caused_by_actor_id, details)``, oldest first."""
        rows = await self.db.get_session_events(session_id, limit=200)
        return [
            (row["event_type"], row["actor_id"], row["caused_by_actor_id"],
             row["details"])
            for row in sorted(rows, key=lambda row: row["id"])
            if not types or row["event_type"] in types
        ]

    def use_fake_runtime(self, monkeypatch) -> None:
        runtimes: dict[str, _Runtime] = {}

        async def _client(session_id, source, model, **kwargs):
            return runtimes.setdefault(session_id, _Runtime())

        monkeypatch.setattr(self.engine, "_get_or_create_client", _client)

    async def started(self, session_id: str) -> None:
        """Move a new session out of ``created``, as its first turn does."""
        await self.db.update_session_fields(
            session_id, {"status": "idle", "sdk_session_id": f"native-{session_id}"},
        )


@pytest_asyncio.fixture
async def install(tmp_path, open_identity_db, wire_identity_store):
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
        "auth": {"jwt_secret": _SECRET},
    })
    config.workspace.mkdir(parents=True)
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
    response = await client.post("/api/sessions", headers=headers, json={})
    assert response.status_code == 200
    return response.json()["id"]


# --------------------------------------------------------------------------- #
#  Schema                                                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_registry_lists_the_session_columns(install):
    for pair in _NEW_COLUMNS:
        assert pair in ACTOR_COLUMNS


@pytest.mark.asyncio
async def test_v054_keeps_old_rows_null_and_is_idempotent(install):
    raw = install.db.db
    await raw.execute("ALTER TABLE session_events DROP COLUMN actor_id")
    await raw.execute("ALTER TABLE session_events DROP COLUMN caused_by_actor_id")
    await raw.execute("DROP TABLE session_deletions")
    await raw.execute(
        "INSERT INTO sessions (id, title, source, status, created_at, updated_at)"
        " VALUES ('old', 'Old chat', 'web', 'idle', '2020-01-01', '2020-01-01')"
    )
    await raw.execute(
        "INSERT INTO session_events (session_id, event_type, created_at)"
        " VALUES ('old', 'archived', '2020-01-01')"
    )
    await raw.commit()

    from nerve.db.migrations.v054_session_attribution import up

    await up(raw)
    await up(raw)
    await raw.commit()
    assert await install.events("old") == [("archived", None, None, None)]
    assert await install.db.get_session_deletion("old") is None


# --------------------------------------------------------------------------- #
#  REST: two people change sessions                                           #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_rest_session_changes_record_each_person(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    async with _client(install.app()) as client:
        session_id = await _new_session(client, install.headers("alice"))
        parent_id = await _new_session(client, install.headers("bob"))
        await install.started(session_id)

        async def patch(name: str, body: dict) -> None:
            response = await client.patch(
                f"/api/sessions/{session_id}", headers=install.headers(name), json=body,
            )
            assert response.status_code == 200, response.text

        await patch("alice", {"title": "Plan the launch"})
        await patch("bob", {"starred": True})
        await patch("alice", {"model": "claude-test-model"})
        await patch("bob", {"parent_session_id": parent_id})
        await patch("alice", {"starred": False, "title": "Plan the launch"})
        await patch("bob", {"parent_session_id": None})
        events = (await client.get(
            f"/api/sessions/{session_id}/events", headers=install.headers("bob"),
        )).json()["events"]

    assert [
        (e["event_type"], e["actor_id"], e["caused_by_actor_id"])
        for e in sorted(events, key=lambda e: e["id"])
    ] == [
        ("created", alice, alice),
        ("title_changed", alice, alice),
        ("starred", bob, bob),
        ("model_changed", alice, alice),
        ("parent_changed", bob, bob),
        # The title in the same request keeps its value: no event.
        ("unstarred", alice, alice),
        ("parent_changed", bob, bob),
    ]
    details = {e["id"]: e["details"] for e in events}
    by_type = [
        (e["event_type"], details[e["id"]])
        for e in sorted(events, key=lambda e: e["id"])
    ]
    assert by_type[1] == ("title_changed", {"title": "Plan the launch", "previous": session_id})
    assert by_type[3][1]["model"] == "claude-test-model"
    assert by_type[4] == ("parent_changed", {"parent_session_id": parent_id, "previous": None})
    assert by_type[6] == ("parent_changed", {"parent_session_id": None, "previous": parent_id})


@pytest.mark.asyncio
async def test_rest_archive_unarchive_star_and_resume_record_the_person(install):
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    async with _client(install.app()) as client:
        root = await _new_session(client, install.headers("alice"))
        await install.started(root)
        await install.db.create_session(
            "child", source="web", parent_session_id=root,
            actor=install.db.system_actor,
        )
        response = await client.post(
            f"/api/sessions/{root}/archive", headers=install.headers("bob"),
        )
        assert sorted(response.json()["archived_ids"]) == sorted([root, "child"])
        response = await client.post(
            f"/api/sessions/{root}/unarchive", headers=install.headers("alice"),
        )
        assert response.status_code == 200
        await client.post(f"/api/sessions/{root}/archive", headers=install.headers("alice"))
        # Starring an archived session restores it first.
        await client.patch(
            f"/api/sessions/{root}", headers=install.headers("bob"), json={"starred": 1},
        )
        # An archive drops the native session; give it one to resume.
        await install.started(root)
        response = await client.post(
            f"/api/sessions/{root}/resume", headers=install.headers("alice"),
        )
        assert response.status_code == 200

    assert await install.events(root, "archived", "unarchived", "starred", "created") == [
        ("created", alice, alice, {"source": "web", "parent": None}),
        ("archived", bob, bob, None),
        ("unarchived", alice, alice, None),
        ("archived", alice, alice, None),
        ("unarchived", bob, bob, None),
        ("starred", bob, bob, None),
        ("created", alice, alice, {"reason": "resume"}),
    ]
    assert await install.events("child", "archived") == [
        ("archived", bob, bob, {"cascade_root": root}),
        ("archived", alice, alice, {"cascade_root": root}),
    ]


@pytest.mark.asyncio
async def test_rest_delete_records_the_person_in_a_tombstone(install):
    bob = install.actor_id("bob")
    async with _client(install.app()) as client:
        session_id = await _new_session(client, install.headers("alice"))
        await client.patch(
            f"/api/sessions/{session_id}", headers=install.headers("alice"),
            json={"title": "Old notes"},
        )
        response = await client.delete(
            f"/api/sessions/{session_id}", headers=install.headers("bob"),
        )
        assert response.json() == {"deleted": True}
        events = (await client.get(
            f"/api/sessions/{session_id}/events", headers=install.headers("bob"),
        )).json()["events"]

    assert events == []
    deletion = await install.db.get_session_deletion(session_id)
    assert deletion["title"] == "Old notes"
    assert (deletion["actor_id"], deletion["caused_by_actor_id"]) == (bob, bob)
    assert datetime.fromisoformat(deletion["deleted_at"]) <= datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_a_delete_and_its_tombstone_are_one_transaction(install):
    db = install.db
    await db.create_session("kept", title="Keep me", source="web", actor=None)
    await db.log_session_event("kept", "archived")

    # An actor that does not exist fails the tombstone, so the delete rolls back.
    with pytest.raises(Exception, match="FOREIGN KEY"):
        await db.delete_session(
            "kept", attribution=Attribution("00000000-0000-4000-8000-00000000dead", None),
        )
    assert (await db.get_session("kept"))["title"] == "Keep me"
    assert [e[0] for e in await install.events("kept")] == ["archived"]
    assert await db.get_session_deletion("kept") is None

    # No session, no tombstone.
    assert await db.delete_session("never-was") is False
    assert await db.get_session_deletion("never-was") is None

    # A later delete of a session with the same ID replaces the tombstone.
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    assert await db.delete_session("kept", attribution=Attribution(alice, alice))
    await db.create_session("kept", title="Again", source="web", actor=None)
    assert await db.delete_session("kept", attribution=Attribution(bob, bob))
    deletion = await db.get_session_deletion("kept")
    assert (deletion["title"], deletion["actor_id"]) == ("Again", bob)


# --------------------------------------------------------------------------- #
#  The browser WebSocket                                                       #
# --------------------------------------------------------------------------- #


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
        route.endpoint for route in app.routes if getattr(route, "path", "") == "/ws"
    )


@pytest.mark.asyncio
async def test_websocket_stop_and_resume_record_the_connections_person(
    install, monkeypatch,
):
    monkeypatch.setattr("nerve.gateway.server._engine", install.engine)
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    await install.db.create_session("ws-chat", source="web", actor=None)
    await install.started("ws-chat")
    # A live client, so the stop reaches the session.
    install.engine.sessions.set_client("ws-chat", _Runtime())

    endpoint = _ws_endpoint()
    alices = _Socket(install.token("alice"), [{"type": "stop", "session_id": "ws-chat"}])
    bobs = _Socket(install.token("bob"), [{"type": "resume", "session_id": "ws-chat"}])
    await endpoint(alices)
    await endpoint(bobs)

    assert not [m for m in alices.sent + bobs.sent if m.get("type") == "error"]
    assert await install.events("ws-chat", "stop_requested", "created") == [
        ("stop_requested", alice, alice, None),
        ("created", bob, bob, {"reason": "resume"}),
    ]


@pytest.mark.asyncio
async def test_a_stop_that_reaches_no_session_records_nothing(install, monkeypatch):
    monkeypatch.setattr("nerve.gateway.server._engine", install.engine)
    await install.db.create_session("idle-chat", source="web", actor=None)
    socket = _Socket(install.token("alice"), [{"type": "stop", "session_id": "idle-chat"}])
    await _ws_endpoint()(socket)
    assert any(m.get("type") == "error" for m in socket.sent)
    assert await install.events("idle-chat", "stop_requested") == []


# --------------------------------------------------------------------------- #
#  Channel commands                                                            #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_channel_session_commands_are_unattributed(install):
    router = install.engine.router
    # /new
    session_id = await router.create_session("telegram:1", source="telegram")
    install.engine.sessions.set_client(session_id, _Runtime())
    # /stop, /star, and the star toggle
    assert await router.stop_session(session_id)
    await router.set_session_starred(session_id, True)
    await router.toggle_session_starred(session_id)

    assert [
        (event, actor, cause)
        for event, actor, cause, _ in await install.events(session_id)
    ] == [
        ("created", None, None),
        ("stop_requested", None, None),
        ("starred", None, None),
        ("unstarred", None, None),
    ]


# --------------------------------------------------------------------------- #
#  Changes by the agent and by Nerve                                           #
# --------------------------------------------------------------------------- #


class _TitleModel:
    """The fast model that names a session. It answers when ``gate`` opens."""

    def __init__(self, title: str):
        self.title = title
        self.gate = asyncio.Event()
        self.asked = asyncio.Event()
        self.messages = self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc) -> None:
        return None

    async def create(self, **kwargs):
        self.asked.set()
        await asyncio.wait_for(self.gate.wait(), timeout=5)
        return SimpleNamespace(content=[SimpleNamespace(type="text", text=self.title)])


async def _wait_for_event(install: _Install, session_id: str, reason: str) -> None:
    for _ in range(500):
        for event in await install.events(session_id, "title_changed"):
            if (event[3] or {}).get("reason") == reason:
                return
        await asyncio.sleep(0.01)
    raise AssertionError(f"no {reason} title for {session_id}")


@pytest.mark.asyncio
async def test_a_generated_title_takes_the_cause_of_the_turn_that_named_it(
    install, monkeypatch,
):
    """Bob's turn starts before the title of Alice's first turn arrives.

    The title is still the system actor's change for Alice.
    """
    install.use_fake_runtime(monkeypatch)
    model = _TitleModel("Launch plan review")
    monkeypatch.setattr(install.config, "resolve_anthropic_api_key", lambda: "test-key")
    monkeypatch.setattr(
        install.config, "create_async_anthropic_client", lambda **kwargs: model,
    )
    alice = install.actor_id("alice")
    async with _client(install.app()) as client:
        session_id = await _new_session(client, install.headers("alice"))
        response = await client.post(
            "/api/chat", headers=install.headers("alice"),
            json={"session_id": session_id, "message": "Review the launch plan"},
        )
        assert response.status_code == 200
        await asyncio.wait_for(model.asked.wait(), timeout=5)
        response = await client.post(
            "/api/chat", headers=install.headers("bob"),
            json={"session_id": session_id, "message": "Add the budget"},
        )
        assert response.status_code == 200

    model.gate.set()
    await _wait_for_event(install, session_id, "generated")

    system = install.system
    assert await install.events(session_id, "title_changed") == [
        ("title_changed", system, alice, {
            "title": "Review the launch plan", "previous": session_id,
            "reason": "placeholder",
        }),
        ("title_changed", system, alice, {
            "title": "Launch plan review", "previous": "Review the launch plan",
            "reason": "generated",
        }),
    ]


@pytest.mark.asyncio
async def test_a_title_in_a_turn_with_no_person_has_no_cause(install, monkeypatch):
    install.use_fake_runtime(monkeypatch)
    # No model credential: the turn sets only the placeholder title.
    monkeypatch.setattr(install.config, "resolve_anthropic_api_key", lambda: "")
    await install.db.create_session("chat-app", source="telegram", actor=None)
    await install.engine.run("chat-app", "hello from a chat app", source="telegram", actor=None)
    assert await install.events("chat-app", "title_changed") == [
        ("title_changed", install.system, None, {
            "title": "hello from a chat app", "previous": "chat-app",
            "reason": "placeholder",
        }),
    ]


@pytest.mark.asyncio
async def test_automatic_archives_are_the_system_actors_own_work(install):
    db = install.db
    long_ago = "2020-01-01T00:00:00+00:00"
    # Older than the other recent job, newer than the 30-day backstop.
    days_ago = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    for session_id, source in (
        ("quiet-chat", "web"), ("old-job", "cron"), ("job-1", "cron"), ("job-2", "cron"),
    ):
        await db.create_session(session_id, source=source, actor=None)
    await db.db.execute("UPDATE sessions SET status = 'idle'")
    await db.db.execute(
        "UPDATE sessions SET updated_at = ? WHERE id IN ('quiet-chat', 'old-job')",
        (long_ago,),
    )
    await db.db.execute(
        "UPDATE sessions SET updated_at = ? WHERE id = 'job-1'", (days_ago,),
    )
    await db.db.commit()

    stats = await install.engine.sessions.run_cleanup(
        archive_after_days=30, max_sessions=1, interactive_archive_after_hours=1,
    )

    assert stats == {"archived_interactive": 1, "archived_stale": 1, "archived_overflow": 1}
    system = install.system
    assert await install.events("quiet-chat", "archived") == [
        ("archived", system, system, {"reason": "idle_interactive"}),
    ]
    assert await install.events("old-job", "archived") == [
        ("archived", system, system, {"reason": "stale"}),
    ]
    assert await install.events("job-1", "archived") == [
        ("archived", system, system, {"reason": "overflow"}),
    ]


class _SilentBroadcaster:
    async def broadcast(self, *args, **kwargs):
        return None


@pytest.mark.asyncio
async def test_a_codex_sync_archive_is_the_system_actors_own_work(install):
    from nerve.sources.codex_threads.base import ThreadEvent, WorkspaceFilter
    from nerve.sources.codex_threads.ingester import CodexIngester

    workspace = str(install.config.workspace)
    ingester = CodexIngester(
        install.db,
        origin_id="origin-1",
        workspace_filter=WorkspaceFilter(
            mode="nerve_workspace", nerve_workspace_path=pathlib.Path(workspace),
        ),
        broadcaster=_SilentBroadcaster(),
    )
    now = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    for sequence, (type_, payload) in enumerate((
        ("thread_in_scope", {"id": "thread-aaa", "cwd": workspace, "source": "exec"}),
        ("thread_archived", {}),
    )):
        await ingester.ingest(ThreadEvent(
            type=type_, thread_id="thread-aaa", sequence=sequence,
            timestamp=now, payload=payload,
        ))

    system = install.system
    assert (await install.db.get_session("codex:thread-aaa"))["status"] == "archived"
    assert await install.events("codex:thread-aaa") == [
        ("archived", system, system, {"reason": "codex_thread_archived"}),
    ]


@pytest.mark.asyncio
async def test_an_external_tool_call_records_the_context_attribution(install):
    db = install.db
    alice = install.actor_id("alice")
    await db.create_session("codex-session", source="external", actor=None)
    write = build_audit_writer(db)
    from nerve.agent.tools import ToolResult

    ctx = ToolContext(session_id="codex-session", actor=db.system_actor, cause=lambda: alice)
    await write(ctx, "task_list", {}, ToolResult.text("ok"), 1.0, False)
    await write("codex-session", "task_list", {}, ToolResult.text("ok"), 1.0, False)

    assert [
        (event, actor, cause)
        for event, actor, cause, _ in await install.events("codex-session")
    ] == [
        ("external_tool_call", install.system, alice),
        ("external_tool_call", None, None),
    ]


# --------------------------------------------------------------------------- #
#  The actor directory                                                         #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_actor_directory_names_every_id_in_the_session_columns(install):
    async with _client(install.app()) as client:
        session_id = await _new_session(client, install.headers("alice"))
        await client.patch(
            f"/api/sessions/{session_id}", headers=install.headers("bob"),
            json={"title": "Shared"},
        )
        await client.delete(f"/api/sessions/{session_id}", headers=install.headers("bob"))
        other = await _new_session(client, install.headers("alice"))
        await client.post(f"/api/sessions/{other}/archive", headers=install.headers("alice"))
        directory = {
            row["id"]
            for row in (
                await client.get("/api/actors", headers=install.headers("alice"))
            ).json()["actors"]
        }

    for table, column in _NEW_COLUMNS:
        async with install.db.db.execute(
            f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL"
        ) as cursor:
            referenced = {row[0] async for row in cursor}
        assert referenced, (table, column)
        assert referenced <= directory, (table, column)

