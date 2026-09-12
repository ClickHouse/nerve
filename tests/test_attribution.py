"""Who did it — attribution persisted on sessions and messages.

This is the delivery gate: two people sharing one agent, two local accounts, no
gateway, and every session and message they create stored under the right one
of them. The rest of the file is the boundaries of that claim — what the
instance's own work is attributed to, what is deliberately left unattributed,
and the property a "current user" global would quietly destroy.

The engine's client build is stubbed out in the turn tests: the model is the
only part of a turn these care nothing about, and everything before it — the
route, ``engine.run``, the session lookup, the message insert — is the real
code.
"""

from __future__ import annotations

import asyncio
import ast
import json
import pathlib
import sqlite3
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from nerve.agent.engine import AgentEngine
from nerve.agent.streaming import broadcaster
from nerve.config import AuthConfig, NerveConfig, set_config
from nerve.gateway.auth import create_session_token, hash_password, pin_jwt_secret
from nerve.gateway.routes import init_deps, register_all_routes
from nerve.gateway.server import create_app
from nerve.identity import Actor
from nerve.mcp_server.session import SatelliteSessionResolver

_SECRET = "test-secret-for-attribution-padded-to-32b"
_ALICE_PASSWORD = "correct-horse-battery-staple"
_BOB_PASSWORD = "a-different-passphrase-entirely"


# --------------------------------------------------------------------------- #
#  Fixtures                                                                    #
# --------------------------------------------------------------------------- #


class _Install:
    """One instance with two people on it, and a real engine behind the routes.

    ``secure_the_owner`` then ``create_managed_account`` is the only way to get
    to two accounts (a passwordless instance refuses the second), so it is what
    the gate runs on.
    """

    def __init__(self, db, identity, engine, config):
        self.db = db
        self.identity = identity
        self.engine = engine
        self.config = config
        self.alice_account = identity.owner_account_id
        self.alice_actor = identity.owner_actor_id
        self.bob_account = ""
        self.bob_actor = ""

    async def add_the_second_person(self) -> None:
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

    async def senders_in(self, session_id: str) -> list[tuple[str, str | None]]:
        """``(role, actor_id)`` for a session's messages, oldest first."""
        return [
            (m["role"], m["actor_id"])
            for m in await self.db.get_messages(session_id)
        ]


@pytest_asyncio.fixture
async def install(tmp_path, open_identity_db, wire_identity_store, monkeypatch):
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
    await installed.add_the_second_person()
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
    """Let a turn run for real right up to the model, then stop.

    ``_run_inner`` persists the user message *before* it builds a client, and
    catches a failed build into an error response, so a turn with no model
    still exercises the whole path this file is about: route or socket →
    ``engine.run`` → session lookup → message insert.
    """
    async def _refuse(*args, **kwargs):
        raise RuntimeError("no model in this test")

    monkeypatch.setattr(engine, "_get_or_create_client", _refuse)


async def _run_later(client, headers, session_id, message):
    """Persist a user message through a real route, with no model involved."""
    return await client.post(
        "/api/sessions/run-later",
        headers=headers,
        json={"session_id": session_id, "message": message, "delay": "none"},
    )


# --------------------------------------------------------------------------- #
#  The gate: two people, over HTTP                                             #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestTwoPeopleOverHttp:
    async def test_each_session_belongs_to_whoever_asked_for_it(self, install):
        async with _client(install.app()) as client:
            hers = await client.post("/api/sessions", headers=install.alice, json={})
            his = await client.post("/api/sessions", headers=install.bob, json={})

        assert hers.status_code == his.status_code == 200
        assert hers.json()["created_by_actor_id"] == install.alice_actor
        assert his.json()["created_by_actor_id"] == install.bob_actor
        assert install.alice_actor != install.bob_actor
        # ...and that is what is on disk, not only in the answer.
        assert await install.creator_of(hers.json()["id"]) == install.alice_actor
        assert await install.creator_of(his.json()["id"]) == install.bob_actor

    async def test_each_message_belongs_to_whoever_sent_it(self, install):
        """Two people sending into the *same* session: the session has one
        creator, the messages have two senders."""
        async with _client(install.app()) as client:
            shared = (
                await client.post("/api/sessions", headers=install.alice, json={})
            ).json()["id"]
            assert (await _run_later(client, install.alice, shared, "mine")).status_code == 200
            assert (await _run_later(client, install.bob, shared, "and mine")).status_code == 200

        assert await install.creator_of(shared) == install.alice_actor
        rows = await install.senders_in(shared)
        assert [(r, a) for r, a in rows if r == "user"] == [
            ("user", install.alice_actor),
            ("user", install.bob_actor),
        ]

    async def test_a_chat_turn_stores_the_person_who_typed_it(
        self, install, monkeypatch,
    ):
        """The whole way through the engine: POST /api/chat → engine.run →
        the persisted user row."""
        _no_model(install.engine, monkeypatch)
        async with _client(install.app()) as client:
            session_id = (
                await client.post("/api/sessions", headers=install.bob, json={})
            ).json()["id"]
            res = await client.post(
                "/api/chat",
                headers=install.bob,
                json={"session_id": session_id, "message": "hello"},
            )
        assert res.status_code == 200
        # The error row the failed turn writes is in the assistant's voice, so
        # it is unattributed like every other one.
        assert await install.senders_in(session_id) == [
            ("user", install.bob_actor),
            ("assistant", None),
        ]

    async def test_the_read_apis_carry_attribution(self, install):
        """PR 5 is frontend-only, so everything it needs is already published."""
        async with _client(install.app()) as client:
            session_id = (
                await client.post("/api/sessions", headers=install.alice, json={})
            ).json()["id"]
            await _run_later(client, install.alice, session_id, "hi")

            listed = (await client.get("/api/sessions", headers=install.bob)).json()
            messages = (
                await client.get(
                    f"/api/sessions/{session_id}/messages", headers=install.bob,
                )
            ).json()["messages"]

        row = next(s for s in listed["sessions"] if s["id"] == session_id)
        assert row["created_by_actor_id"] == install.alice_actor
        assert [m["actor_id"] for m in messages] == [install.alice_actor, None]

    async def test_one_persons_token_cannot_write_under_the_other(self, install):
        """The actor comes from the credential, not from anything the caller
        can put in the request body."""
        async with _client(install.app()) as client:
            res = await client.post(
                "/api/sessions",
                headers=install.bob,
                json={"title": "for alice", "source": "web"},
            )
        assert res.json()["created_by_actor_id"] == install.bob_actor


# --------------------------------------------------------------------------- #
#  The gate: two people, over the WebSocket                                    #
# --------------------------------------------------------------------------- #


class _Socket:
    """A WebSocket the real ``/ws`` endpoint can talk to.

    Scripted inbound frames, then a disconnect; everything sent back is kept so
    a test can read what the client would have seen.
    """

    def __init__(self, token: str, frames: list[dict]):
        self.query_params = {"token": token}
        self.cookies = {}
        self.accepted = False
        self.closed: tuple[int, str] | None = None
        self.sent: list[dict] = []
        self._frames = list(frames)

    async def accept(self) -> None:
        self.accepted = True

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closed = (code, reason)

    async def send_json(self, payload: dict) -> None:
        self.sent.append(payload)

    async def receive_json(self) -> dict:
        from starlette.websockets import WebSocketDisconnect

        if not self._frames:
            raise WebSocketDisconnect(1000)
        return self._frames.pop(0)


def _ws_endpoint():
    """The real ``/ws`` handler, pulled off the real app.

    It is defined inside ``create_app`` and closes over the module-level
    engine, so this is how a test reaches it without standing up a lifespan.
    """
    app = create_app()
    return next(r.endpoint for r in app.routes if getattr(r, "path", "") == "/ws")


@pytest.mark.asyncio
class TestTwoPeopleOverTheWebSocket:
    async def test_two_connections_store_two_senders(
        self, install, monkeypatch,
    ):
        """The property the socket has to have: its actor is fixed at accept,
        so two open connections write two different names into one session."""
        _no_model(install.engine, monkeypatch)
        monkeypatch.setattr("nerve.gateway.server._engine", install.engine)
        endpoint = _ws_endpoint()

        session_id = "ws-shared"
        await install.db.create_session(session_id, source="web", actor=None)

        for n, (account, text) in enumerate((
            (install.alice_account, "from her"),
            (install.bob_account, "from him"),
        ), start=1):
            await endpoint(_Socket(install.token(account), [{
                "type": "message", "content": text, "session_id": session_id,
            }]))
            await _wait_for_messages(install, session_id, n)

        assert [
            (r, a) for r, a in await install.senders_in(session_id) if r == "user"
        ] == [
            ("user", install.alice_actor),
            ("user", install.bob_actor),
        ]

    async def test_a_session_minted_at_connect_belongs_to_the_connection(
        self, install, monkeypatch,
    ):
        monkeypatch.setattr("nerve.gateway.server._engine", install.engine)
        endpoint = _ws_endpoint()

        socket = _Socket(install.token(install.bob_account), [])
        await endpoint(socket)

        switched = next(m for m in socket.sent if m["type"] == "session_switched")
        assert await install.creator_of(switched["session_id"]) == install.bob_actor

    async def test_the_live_echo_names_the_sender(self, install, monkeypatch):
        """A second tab renders the bubble before the row is readable, so the
        echo carries the id too — the same id the row gets."""
        _no_model(install.engine, monkeypatch)
        monkeypatch.setattr("nerve.gateway.server._engine", install.engine)
        endpoint = _ws_endpoint()

        seen: list[dict] = []
        session_id = "ws-echo"
        await install.db.create_session(session_id, source="web", actor=None)
        await broadcaster.register(session_id, "listener", lambda _s, m: seen.append(m))
        try:
            await endpoint(_Socket(install.token(install.alice_account), [{
                "type": "message", "content": "hi", "session_id": session_id,
            }]))
            await _wait_for_messages(install, session_id, 1)
        finally:
            await broadcaster.unregister(session_id, "listener")

        echo = next(m for m in seen if m.get("type") == "user_message")
        assert echo["actor_id"] == install.alice_actor
        assert [
            (r, a) for r, a in await install.senders_in(session_id) if r == "user"
        ] == [("user", install.alice_actor)]


async def _wait_for_messages(install, session_id: str, count: int) -> None:
    """Wait for the fire-and-forget turn the socket spawned to land its rows.

    Polled rather than slept on: the turn is a task, its write goes through a
    worker thread, and a fixed sleep is how a test like this becomes flaky.
    """
    for _ in range(200):
        rows = await install.senders_in(session_id)
        if len([r for r in rows if r[0] == "user"]) >= count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(
        f"{session_id} never reached {count} user message(s): "
        f"{await install.senders_in(session_id)}"
    )


# --------------------------------------------------------------------------- #
#  The instance's own work                                                     #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestAutonomousWorkIsTheSystemPrincipal:
    async def test_a_cron_run_belongs_to_the_agent(self, install, monkeypatch):
        _no_model(install.engine, monkeypatch)
        await install.engine.run_cron(job_id="nightly", prompt="do the thing")

        sessions = [
            s for s in await install.db.list_sessions(limit=50)
            if s["source"] == "cron"
        ]
        assert len(sessions) == 1
        assert sessions[0]["created_by_actor_id"] == install.system_actor_id
        assert await install.senders_in(sessions[0]["id"]) == [
            ("user", install.system_actor_id),
            ("assistant", None),
        ]

    async def test_a_channel_message_belongs_to_the_agent(
        self, install, monkeypatch,
    ):
        """Channel identity resolution is out of scope, and guessing a person
        from a chat id is the inference the RFC forbids — so the honest answer
        is that the instance received it."""
        _no_model(install.engine, monkeypatch)
        from nerve.channels.base import InboundMessage

        channel = SimpleNamespace(
            name="telegram",
            capabilities=set(),
            format_response=lambda t: t,
        )
        router = install.engine.router
        router._channels["telegram"] = channel
        monkeypatch.setattr(router, "_setup_streaming", _noop)
        monkeypatch.setattr(router, "_teardown_streaming", _noop)
        monkeypatch.setattr(type(router), "BATCH_DEBOUNCE", 0)

        await router.handle_message(InboundMessage(
            channel_name="telegram",
            channel_key="telegram:1",
            sender_id="1",
            text="hello from a chat app",
        ))

        session_id = await install.engine.sessions.get_last_session("telegram:1")
        assert await install.creator_of(session_id) == install.system_actor_id
        assert await install.senders_in(session_id) == [
            ("user", install.system_actor_id),
            ("assistant", None),
        ]

    async def test_an_mcp_satellite_belongs_to_the_agent(self, install):
        resolver = SatelliteSessionResolver(install.db)
        sid = await resolver.resolve(
            client_name="claude-code", mcp_session_id="mcp-abc123",
        )
        assert await install.creator_of(sid) == install.system_actor_id

    async def test_an_ingested_codex_thread_belongs_to_the_agent(self, install):
        """A thread typed into another program: Nerve sees a rollout file, not
        a login, so the user rows are the instance's — and the assistant rows
        are nobody's, exactly like a native turn's."""
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

        sid = "codex:thread-aaa"
        assert await install.creator_of(sid) == install.system_actor_id
        assert await install.senders_in(sid) == [
            ("user", install.system_actor_id),
            ("assistant", None),
        ]

    async def test_a_missing_system_principal_does_not_cancel_the_work(
        self, install, monkeypatch,
    ):
        """Attribution is metadata about the work, so it must never be able to
        stop the work. Unreachable in production — bootstrap runs before
        anything can serve — which is why it is asserted rather than assumed.
        """
        _no_model(install.engine, monkeypatch)

        async def _gone():
            return None

        monkeypatch.setattr(install.db, "get_system_principal", _gone)
        await install.engine.run_cron(job_id="orphan", prompt="still runs")

        sessions = [
            s for s in await install.db.list_sessions(limit=50)
            if s["source"] == "cron"
        ]
        assert len(sessions) == 1
        assert sessions[0]["created_by_actor_id"] is None
        assert await install.senders_in(sessions[0]["id"]) == [
            ("user", None), ("assistant", None),
        ]


async def _noop(*args, **kwargs):
    return None


class _SilentBroadcaster:
    async def broadcast(self, *args, **kwargs):
        return None


def _codex_thread(thread_id: str, cwd: str):
    """A minimal in-scope/user/assistant sequence for the ingester."""
    from datetime import datetime, timezone

    from nerve.sources.codex_threads.base import ThreadEvent

    now = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)

    def _event(type_: str, payload: dict, seq: int) -> ThreadEvent:
        return ThreadEvent(
            type=type_,                   # type: ignore[arg-type]
            thread_id=thread_id,
            sequence=seq,
            timestamp=now,
            payload=payload,
        )

    return [
        _event("thread_in_scope", {
            "id": thread_id, "cwd": cwd, "originator": "codex_exec",
            "cli_version": "0.130.0", "source": "exec",
        }, 1),
        _event("user_message", {"message": "hi", "event_id": "e1"}, 2),
        _event("assistant_message", {"message": "hello", "event_id": "e2"}, 3),
    ]


# --------------------------------------------------------------------------- #
#  Assistant and tool output keeps its own authorship                          #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestOutputIsUnattributed:
    async def test_the_synthetic_acknowledgement_is_nobodys(self, install):
        """Run-later writes two rows: the person's message, and an
        acknowledgement in the assistant's voice."""
        async with _client(install.app()) as client:
            session_id = (
                await client.post("/api/sessions", headers=install.alice, json={})
            ).json()["id"]
            await _run_later(client, install.alice, session_id, "later please")

        assert await install.senders_in(session_id) == [
            ("user", install.alice_actor),
            ("assistant", None),
        ]

    async def test_a_review_loop_milestone_is_nobodys(self, install):
        await install.db.create_session("obs", source="web", actor=None)
        await install.db.add_message(
            "obs", "assistant", "iteration 1 passed", channel="review-loop",
            actor=None,
        )
        assert await install.senders_in("obs") == [("assistant", None)]


class TestOutputIsUnattributedEverywhere:
    """The same rule, checked where no runtime test can reach."""

    def test_no_assistant_row_in_the_package_is_written_under_an_actor(self):
        """Structural, because the rule has to hold at call sites nothing in
        this file reaches: every ``add_message`` in ``nerve/`` whose role is
        the literal ``"assistant"`` passes ``actor=None``.
        """
        offenders = []
        for path in sorted(pathlib.Path("nerve").rglob("*.py")):
            tree = ast.parse(path.read_text(), str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                name = getattr(node.func, "attr", None)
                if name not in ("add_message", "add_message_idempotent"):
                    continue
                role = _literal_role(node)
                if role != "assistant":
                    continue
                actor = next(
                    (k.value for k in node.keywords if k.arg == "actor"), None,
                )
                if not isinstance(actor, ast.Constant) or actor.value is not None:
                    offenders.append(f"{path}:{node.lineno}")
        assert not offenders, f"assistant rows written under an actor: {offenders}"


def _literal_role(node: ast.Call) -> str | None:
    """The ``role`` argument of an ``add_message`` call, when it is a literal."""
    for keyword in node.keywords:
        if keyword.arg == "role" and isinstance(keyword.value, ast.Constant):
            return keyword.value.value
    if len(node.args) >= 2 and isinstance(node.args[1], ast.Constant):
        return node.args[1].value
    return None


# --------------------------------------------------------------------------- #
#  History that predates attribution                                           #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestHistoryPredatingAttribution:
    async def test_the_migration_backfills_nothing(self, install):
        """v048 is additive: an install that upgrades keeps its history, and
        its history keeps saying nothing about who wrote it. Synthesising an
        actor from a session's ``source`` would be inventing an audit trail.
        """
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
        await raw.commit()

        assert await install.creator_of("old") is None
        assert await install.senders_in("old") == [("user", None)]

    async def test_unattributed_rows_serialise_as_null(self, install):
        """PR 5 renders them as they are today, so they have to arrive as
        ``null`` rather than as a missing key or a 500."""
        await install.db.create_session("legacy", source="web", actor=None)
        await install.db.add_message("legacy", "user", "from before", actor=None)

        async with _client(install.app()) as client:
            listed = (await client.get("/api/sessions", headers=install.alice)).json()
            messages = (
                await client.get(
                    "/api/sessions/legacy/messages", headers=install.alice,
                )
            ).json()["messages"]

        row = next(s for s in listed["sessions"] if s["id"] == "legacy")
        assert "created_by_actor_id" in row and row["created_by_actor_id"] is None
        assert [m["actor_id"] for m in messages] == [None]

    async def test_the_migration_can_be_re_run(self, install):
        """Guarded rather than assumed: a re-run must be a no-op, not a
        duplicate-column failure."""
        from nerve.db.migrations.v048_attribution import up

        await up(install.db.db)
        await install.db.create_session("still-works", source="web", actor=None)
        assert await install.creator_of("still-works") is None


# --------------------------------------------------------------------------- #
#  No process-global actor (required by the RFC)                               #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestNoProcessGlobalActor:
    async def test_two_people_writing_at_the_same_moment_do_not_swap(
        self, install,
    ):
        """Both requests are held *inside* the session insert, between
        resolving the actor and writing the row, until the other arrives. Any
        state shared between them — a module global, a memo, a "current user" —
        shows up here as one answer where there should be two.
        """
        barrier = asyncio.Barrier(2)
        original = install.db.create_session

        async def _rendezvous(*args, **kwargs):
            await barrier.wait()
            return await original(*args, **kwargs)

        install.db.create_session = _rendezvous
        try:
            async with _client(install.app()) as client:
                hers, his = await asyncio.gather(
                    client.post("/api/sessions", headers=install.alice, json={}),
                    client.post("/api/sessions", headers=install.bob, json={}),
                )
        finally:
            install.db.create_session = original

        assert hers.json()["created_by_actor_id"] == install.alice_actor
        assert his.json()["created_by_actor_id"] == install.bob_actor

    async def test_two_people_sending_at_the_same_moment_do_not_swap(
        self, install,
    ):
        """The same overlap on the message path."""
        async with _client(install.app()) as client:
            shared = (
                await client.post("/api/sessions", headers=install.alice, json={})
            ).json()["id"]

            barrier = asyncio.Barrier(2)
            original = install.db.add_message

            async def _rendezvous(*args, **kwargs):
                if kwargs.get("actor") is not None:
                    await barrier.wait()
                return await original(*args, **kwargs)

            install.db.add_message = _rendezvous
            try:
                await asyncio.gather(
                    _run_later(client, install.alice, shared, "hers"),
                    _run_later(client, install.bob, shared, "his"),
                )
            finally:
                install.db.add_message = original

        senders = {a for r, a in await install.senders_in(shared) if r == "user"}
        assert senders == {install.alice_actor, install.bob_actor}

    async def test_two_people_resolved_at_the_same_moment_do_not_swap(
        self, install,
    ):
        """The other place two requests can be conflated: both are held inside
        the actor lookup together, and each must still persist itself. A cache
        keyed on nothing, or an actor stashed while resolving, gives both rows
        the same name here.
        """
        barrier = asyncio.Barrier(2)
        original = install.db.get_actor_ref

        async def _rendezvous(actor_id: str):
            row = await original(actor_id)
            await barrier.wait()
            return row

        install.db.get_actor_ref = _rendezvous
        try:
            async with _client(install.app()) as client:
                hers, his = await asyncio.gather(
                    client.post("/api/sessions", headers=install.alice, json={}),
                    client.post("/api/sessions", headers=install.bob, json={}),
                )
        finally:
            install.db.get_actor_ref = original

        assert await install.creator_of(hers.json()["id"]) == install.alice_actor
        assert await install.creator_of(his.json()["id"]) == install.bob_actor

    async def test_alternating_writes_are_never_served_from_a_previous_one(
        self, install,
    ):
        """The half a cache breaks: requests that do not overlap must still
        each be resolved. A memo of the last actor answers the second caller
        with the first caller's identity and this is what notices.
        """
        expected = [
            (install.alice, install.alice_actor),
            (install.bob, install.bob_actor),
            (install.alice, install.alice_actor),
            (install.bob, install.bob_actor),
            (install.alice, install.alice_actor),
        ]
        async with _client(install.app()) as client:
            for headers, actor_id in expected:
                res = await client.post("/api/sessions", headers=headers, json={})
                assert res.json()["created_by_actor_id"] == actor_id

    async def test_nothing_in_the_package_is_holding_an_actor(
        self, install, monkeypatch,
    ):
        """Structural, and independent of timing: after both people and the
        agent itself have written rows, no module — and no long-lived object a
        module is holding, which is where a cache would actually go — has an
        :class:`Actor` on it. An actor lives on a request, a connection or a
        call chain.
        """
        import sys

        _no_model(install.engine, monkeypatch)
        async with _client(install.app()) as client:
            await client.post("/api/sessions", headers=install.alice, json={})
            await client.post("/api/sessions", headers=install.bob, json={})
        await install.engine.run_cron(job_id="sweep", prompt="anything")

        holders: list[str] = []
        for name, module in list(sys.modules.items()):
            if module is None or not (name == "nerve" or name.startswith("nerve.")):
                continue
            for attribute, value in list(vars(module).items()):
                holders.extend(_actor_holders(f"{name}.{attribute}", value))
        assert not holders, f"actor state outside a request: {holders}"


def _actor_holders(where: str, value, *, max_depth: int = 3) -> list[str]:
    """Every :class:`Actor` reachable from a module attribute, with its path.

    Walks a few levels into the objects a module holds, which is the part that
    matters: an actor memoised on the engine or on the notification service
    would be invisible to a scan of module attributes alone, and those objects
    are reached *through* a module-level container rather than being one.
    Bounded by depth, by a visited set, and by only descending into instances
    of this package's own classes.
    """
    found: list[str] = []
    seen: set[int] = set()
    stack: list[tuple[str, object, int]] = [(where, value, 0)]
    while stack:
        path, item, depth = stack.pop()
        if isinstance(item, Actor):
            found.append(path)
            continue
        if depth >= max_depth:
            continue
        if isinstance(item, (list, tuple, set, frozenset)):
            stack.extend(
                (f"{path}[{i}]", element, depth + 1)
                for i, element in enumerate(item)
            )
            continue
        if isinstance(item, dict):
            stack.extend(
                (f"{path}[{key!r}]", element, depth + 1)
                for key, element in item.items()
            )
            continue
        if isinstance(item, type) or not hasattr(item, "__dict__"):
            continue
        if type(item).__module__.split(".")[0] != "nerve":
            continue
        if id(item) in seen:
            continue
        seen.add(id(item))
        stack.extend(
            (f"{path}.{attribute}", element, depth + 1)
            for attribute, element in list(vars(item).items())
        )
    return found


# --------------------------------------------------------------------------- #
#  Resolving an id to a name                                                   #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestTheActorLookup:
    async def test_it_lists_identities_and_nothing_else(self, install):
        async with _client(install.app()) as client:
            res = await client.get("/api/actors", headers=install.alice)

        assert res.status_code == 200
        actors = {a["id"]: a for a in res.json()["actors"]}
        assert set(actors) == {
            install.alice_actor, install.bob_actor, install.system_actor_id,
        }
        assert actors[install.alice_actor] == {
            "id": install.alice_actor,
            "kind": "human",
            "display_name": "Alice",
            "profile_version": actors[install.alice_actor]["profile_version"],
        }
        assert actors[install.system_actor_id]["kind"] == "system"

    async def test_no_account_state_leaks_through_it(self, install):
        """An account's username, whether it has a password, whether it is
        enabled — none of it is this endpoint's business, at any depth."""
        async with _client(install.app()) as client:
            body = (await client.get("/api/actors", headers=install.alice)).json()
            one = (
                await client.get(
                    f"/api/actors/{install.bob_actor}", headers=install.alice,
                )
            ).json()

        blob = json.dumps([body, one])
        for forbidden in (
            "username", "credential", "password", "enabled", "alice", "bob",
            "account", "email", "disabled_at",
        ):
            assert forbidden not in blob, f"{forbidden!r} leaked through /api/actors"

    async def test_an_unknown_id_is_a_404_not_an_invention(self, install):
        async with _client(install.app()) as client:
            res = await client.get(
                "/api/actors/00000000-0000-4000-8000-0000000000ff",
                headers=install.alice,
            )
        assert res.status_code == 404

    async def test_it_needs_a_credential(self, install):
        async with _client(install.app()) as client:
            assert (await client.get("/api/actors")).status_code == 401
            assert (
                await client.get(f"/api/actors/{install.alice_actor}")
            ).status_code == 401

    async def test_a_disabled_account_still_has_a_name_to_show(self, install):
        """History outlives access: a disabled person's rows keep rendering."""
        await install.db.disable_account(install.bob_account)
        async with _client(install.app()) as client:
            res = await client.get(
                f"/api/actors/{install.bob_actor}", headers=install.alice,
            )
        assert res.status_code == 200
        assert res.json()["display_name"] == "Bob"


@pytest.mark.asyncio
class TestRenamingChangesTheNameNotTheHistory:
    async def test_a_rename_moves_no_stored_row(self, install):
        async with _client(install.app()) as client:
            session_id = (
                await client.post("/api/sessions", headers=install.bob, json={})
            ).json()["id"]
            await _run_later(client, install.bob, session_id, "before the rename")

            renamed = await client.patch(
                f"/api/accounts/{install.bob_account}",
                headers=install.alice,
                json={"display_name": "Robert"},
            )
            assert renamed.status_code == 200

            actors = {
                a["id"]: a
                for a in (
                    await client.get("/api/actors", headers=install.alice)
                ).json()["actors"]
            }

        assert actors[install.bob_actor]["display_name"] == "Robert"
        # The stored ids did not move, which is the whole point of storing an
        # id rather than a name.
        assert await install.creator_of(session_id) == install.bob_actor
        assert [a for r, a in await install.senders_in(session_id) if r == "user"] == [
            install.bob_actor,
        ]


# --------------------------------------------------------------------------- #
#  The columns reference real actors                                           #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestStoredActorsAlwaysResolve:
    async def test_an_id_that_names_nobody_cannot_be_stored(self, install):
        """Attribution that resolves to nothing renders as a blank name, so
        the schema refuses it. Safe because actor rows are never deleted."""
        ghost = Actor(
            actor_id="00000000-0000-4000-8000-0000000000ff", kind="human",
        )
        with pytest.raises(sqlite3.IntegrityError):
            await install.db.create_session("ghost-session", actor=ghost)
        with pytest.raises(sqlite3.IntegrityError):
            await install.db.create_session("ghost-message", actor=None)
            await install.db.add_message("ghost-message", "user", "hi", actor=ghost)

    async def test_no_actor_at_all_is_always_allowed(self, install):
        """NULL is exempt from the reference, which is what lets history that
        predates attribution stay exactly as it was."""
        await install.db.create_session("nobody", actor=None)
        await install.db.add_message("nobody", "user", "hi", actor=None)
        assert await install.senders_in("nobody") == [("user", None)]


@pytest.mark.asyncio
class TestForkingKeepsTheOriginalSenders:
    async def test_a_fork_records_who_forked_and_who_spoke(self, install):
        """Forking someone else's chat copies their messages; it does not
        make them yours."""
        async with _client(install.app()) as client:
            session_id = (
                await client.post("/api/sessions", headers=install.alice, json={})
            ).json()["id"]
            await _run_later(client, install.alice, session_id, "hers")
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
        assert [a for r, a in await install.senders_in(fork_id) if r == "user"] == [
            install.alice_actor,
        ]
