"""External authentication mode (``NERVE_AUTH_MODE=external``).

The gateway names the person behind each request in the
``X-Nerve-Actor-Context`` header. Nerve decodes the payload, ignores the
signature, adds or renames the actor row, and acts as that person. Local
logins, local accounts and session tokens are not available. System and MCP
tokens still give the system actor.

The headers here are unsigned: ``b64url(header) + "." + b64url(payload) + ".x"``.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import functools
import json
import time
from types import SimpleNamespace

import httpx
import jwt
import pytest
import pytest_asyncio
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from nerve import paths
from nerve.agent.engine import AgentEngine
from nerve.agent.streaming import broadcaster
from nerve.config import ConfigError, NerveConfig, load_config, set_config
from nerve.db import Database
from nerve.db.accounts import inspect_bootstrap_state, read_instance_secret
from nerve.gateway import server
from nerve.gateway.auth import (
    ACTOR_CONTEXT_HEADER,
    AUTH_MODE_ENV,
    AUTH_MODE_EXTERNAL,
    AUTH_MODE_LOCAL,
    EXTERNAL_SESSION_DETAIL,
    JWT_ALGORITHM,
    SESSION_TOKEN_HEADER,
    ActorContextError,
    auth_mode,
    create_external_mcp_token,
    create_mcp_session_token,
    create_session_token,
    create_system_token,
    decode_actor_context,
    parse_auth_mode,
    pin_auth_mode,
    pinned_jwt_secret,
    require_auth,
)
from nerve.gateway.routes import init_deps, register_all_routes
from nerve.identity import Actor
from nerve.migrate import MigrationReport, bootstrap_identity

_SECRET = "test-secret-for-external-auth-padded-32b"
ALICE = "6f1c2a8e-3b4d-4e5f-8a9b-0c1d2e3f4a5b"
BOB = "0a9b8c7d-6e5f-4a3b-9c2d-1e0f9a8b7c6d"


# --------------------------------------------------------------------------- #
#  Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _b64url(value: object) -> str:
    raw = value if isinstance(value, bytes) else json.dumps(value).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def _context(principal_id: object, display_name: str | None = None, **claims) -> str:
    """An unsigned actor context header value."""
    payload = {"principal_id": principal_id, **claims}
    if display_name is not None:
        payload["profile"] = {"display_name": display_name}
    return _b64url({"alg": "ES256", "typ": "JWT"}) + "." + _b64url(payload) + ".x"


def _as(principal_id: str, display_name: str | None = None) -> dict:
    return {ACTOR_CONTEXT_HEADER: _context(principal_id, display_name)}


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://nerve-test",
    )


def _whoami_app() -> FastAPI:
    """The real dependency and the real slide middleware, as in the gateway."""
    app = FastAPI()

    @app.middleware("http")
    async def _slide_session_token(request: Request, call_next):
        response = await call_next(request)
        token = getattr(request.state, "refreshed_token", None)
        if token:
            response.headers[SESSION_TOKEN_HEADER] = token
        return response

    @app.get("/api/whoami")
    async def whoami(actor: Actor = Depends(require_auth)):
        return {
            "actor_id": actor.actor_id,
            "kind": actor.kind,
            "account_id": actor.account_id,
            "display_name": actor.display_name,
        }

    return app


def _route_paths(router) -> set[str]:
    """Every route path, including the routes of included routers."""
    found: set[str] = set()
    for route in router.routes:
        included = getattr(route, "original_router", None)
        if included is not None:
            found |= _route_paths(included)
        elif getattr(route, "path", ""):
            found.add(route.path)
    return found


def _legacy_token() -> str:
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    return jwt.encode(
        {"iat": now, "exp": now + timedelta(hours=1), "sub": "user"},
        _SECRET,
        algorithm=JWT_ALGORITHM,
    )


def _config(tmp_path) -> NerveConfig:
    return NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
        "auth": {"jwt_secret": _SECRET},
    })


@contextlib.asynccontextmanager
async def _no_lifespan(_app):
    yield


# --------------------------------------------------------------------------- #
#  Fixtures                                                                    #
# --------------------------------------------------------------------------- #


@pytest.fixture
def external(monkeypatch):
    """Run the test in external mode, as ``create_app()`` would pin it."""
    monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
    pin_auth_mode(AUTH_MODE_EXTERNAL)


class _Install:
    """One external-mode instance after its identity bootstrap."""

    def __init__(self, db: Database, engine: AgentEngine, report: MigrationReport):
        self.db = db
        self.engine = engine
        self.report = report

    @property
    def system_actor_id(self) -> str:
        return self.db.system_actor_id

    async def humans(self) -> list[dict]:
        return await self.db.list_actor_refs(kind="human")

    async def creator_of(self, session_id: str) -> str | None:
        return (await self.db.get_session(session_id))["created_by_actor_id"]

    async def said_in(self, session_id: str) -> list[tuple[str, str | None]]:
        return [
            (row["content"], row["actor_id"])
            for row in await self.db.get_messages(session_id)
            if row["role"] == "user"
        ]

    async def add_leftover_account(self) -> str:
        """An account left from local mode."""
        account = await self.db._bootstrap_first_account(credential_source="none")
        assert account.created
        return account.account_id


@pytest_asyncio.fixture
async def install(tmp_path, external, wire_identity_store):
    config = _config(tmp_path)
    set_config(config)
    database = Database(tmp_path / "nerve.db")
    await database.connect()
    try:
        report = await bootstrap_identity(database, config)
        wire_identity_store(database)
        engine = AgentEngine(config, database)
        init_deps(engine, database)
        yield _Install(database, engine, report)
    finally:
        await database.close()
        set_config(NerveConfig())


# --------------------------------------------------------------------------- #
#  The mode                                                                    #
# --------------------------------------------------------------------------- #


class TestMode:
    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_unset_or_blank_is_local(self, value):
        assert parse_auth_mode(value) == AUTH_MODE_LOCAL

    @pytest.mark.parametrize(
        ("value", "mode"),
        [("local", "local"), ("LOCAL", "local"), (" External ", "external")],
    )
    def test_known_modes_are_accepted_case_insensitively(self, value, mode):
        assert parse_auth_mode(value) == mode

    @pytest.mark.parametrize("value", ["hosted", "simple", "none", "externally"])
    def test_anything_else_is_refused_with_the_accepted_values_named(self, value):
        with pytest.raises(ConfigError) as ei:
            parse_auth_mode(value)
        message = str(ei.value)
        assert AUTH_MODE_ENV in message
        assert "'local'" in message and "'external'" in message
        assert repr(value) in message

    def test_an_unknown_value_stops_startup(self, monkeypatch):
        monkeypatch.setenv(AUTH_MODE_ENV, "hosted")
        with pytest.raises(ConfigError, match=AUTH_MODE_ENV):
            server.create_app()

    def test_startup_pins_the_mode_from_the_environment(self, monkeypatch):
        monkeypatch.setenv(AUTH_MODE_ENV, "external")
        server.create_app()
        monkeypatch.delenv(AUTH_MODE_ENV)
        assert auth_mode() == AUTH_MODE_EXTERNAL

    def test_before_startup_the_environment_decides(self, monkeypatch):
        assert auth_mode() == AUTH_MODE_LOCAL
        monkeypatch.setenv(AUTH_MODE_ENV, "external")
        assert auth_mode() == AUTH_MODE_EXTERNAL

    def test_a_pin_cannot_change_to_another_mode(self):
        pin_auth_mode(AUTH_MODE_EXTERNAL)
        pin_auth_mode(AUTH_MODE_EXTERNAL)
        with pytest.raises(ConfigError, match="already pinned"):
            pin_auth_mode(AUTH_MODE_LOCAL)
        assert auth_mode() == AUTH_MODE_EXTERNAL

    @pytest.mark.asyncio
    async def test_a_reload_does_not_change_the_mode(self, tmp_path, monkeypatch):
        from nerve.config_reload import reload_all

        config_dir, workspace = tmp_path / "cfg", tmp_path / "ws"
        config_dir.mkdir()
        (workspace / "config").mkdir(parents=True)
        (config_dir / "config.yaml").write_text(
            f"workspace: {workspace}\n", encoding="utf-8",
        )
        set_config(load_config(config_dir))
        try:
            monkeypatch.setenv(AUTH_MODE_ENV, "external")
            server.create_app()

            monkeypatch.setenv(AUTH_MODE_ENV, "local")
            (config_dir / "config.yaml").write_text(
                f"workspace: {workspace}\ntimezone: UTC\n", encoding="utf-8",
            )
            summary = await reload_all(None, None, config_dir)

            assert summary["config"] == "reloaded"
            assert auth_mode() == AUTH_MODE_EXTERNAL
            routes = _route_paths(register_all_routes())
            assert "/api/actors" in routes and "/api/accounts" not in routes
        finally:
            set_config(NerveConfig())


# --------------------------------------------------------------------------- #
#  Reading the header                                                          #
# --------------------------------------------------------------------------- #


class TestDecodeActorContext:
    def test_it_reads_the_principal_and_the_display_name(self):
        assert decode_actor_context(_context(ALICE, "Alice")) == (ALICE, "Alice")

    def test_an_upper_case_uuid_gives_the_canonical_string(self):
        assert decode_actor_context(_context(ALICE.upper(), "Alice")) == (ALICE, "Alice")

    def test_no_profile_or_no_name_gives_no_display_name(self):
        assert decode_actor_context(_context(ALICE)) == (ALICE, None)
        assert decode_actor_context(
            _context(ALICE, profile={"display_name": None}),
        ) == (ALICE, None)
        assert decode_actor_context(_context(ALICE, profile={})) == (ALICE, None)

    def test_the_signature_and_other_claims_are_ignored(self):
        value = _context(
            ALICE, "Alice", tenant_id="t-1", agent_id="a-1", aud="someone", exp=1,
        )
        header, payload, _signature = value.split(".")
        assert decode_actor_context(f"{header}.{payload}.") == (ALICE, "Alice")
        assert decode_actor_context(f"{header}.{payload}.anything") == (ALICE, "Alice")

    @pytest.mark.parametrize(
        "value",
        [
            "",
            "abc",
            "a.b",
            "a.b.c.d",
            "h.!!!.x",
            "h." + _b64url(b"not json") + ".x",
            "h." + _b64url(b"\xff\xfe\xfd") + ".x",
            "h." + _b64url(b"[" * 50_000) + ".x",
            "h." + _b64url([ALICE]) + ".x",
            "h." + _b64url({"sub": ALICE}) + ".x",
            _context(42),
            _context("alice"),
            _context(ALICE[:-1]),
            _context(ALICE, profile="Alice"),
            _context(ALICE, profile={"display_name": 42}),
        ],
        ids=[
            "empty", "one-part", "two-parts", "four-parts", "not-base64url",
            "not-json", "not-utf8", "nested-too-deep", "not-an-object",
            "no-principal", "number-principal",
            "name-principal", "short-uuid", "profile-not-object", "name-not-string",
        ],
    )
    def test_a_malformed_value_is_refused(self, value):
        with pytest.raises(ActorContextError):
            decode_actor_context(value)


# --------------------------------------------------------------------------- #
#  HTTP requests                                                               #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestHttpAuthentication:
    async def test_the_header_acts_as_the_person_it_names(self, install):
        async with _client(_whoami_app()) as client:
            res = await client.get("/api/whoami", headers=_as(ALICE, "Alice"))
        assert res.status_code == 200
        assert res.json() == {
            "actor_id": ALICE, "kind": "human", "account_id": None,
            "display_name": "Alice",
        }
        assert SESSION_TOKEN_HEADER not in res.headers

    async def test_no_header_and_no_token_is_refused(self, install):
        async with _client(_whoami_app()) as client:
            res = await client.get("/api/whoami")
        assert res.status_code == 401

    @pytest.mark.parametrize(
        "value", ["", "abc", "a.b.c.d", _context("alice"), _context(42)],
        ids=["empty", "one-part", "four-parts", "not-a-uuid", "number"],
    )
    async def test_a_malformed_header_is_refused(self, install, value):
        async with _client(_whoami_app()) as client:
            res = await client.get(
                "/api/whoami", headers={ACTOR_CONTEXT_HEADER: value},
            )
        assert res.status_code == 401
        assert await install.humans() == []

    async def test_an_upper_case_uuid_is_the_same_actor(self, install):
        async with _client(_whoami_app()) as client:
            lower = await client.get("/api/whoami", headers=_as(ALICE, "Alice"))
            upper = await client.get("/api/whoami", headers=_as(ALICE.upper(), "Alice"))
        assert lower.json()["actor_id"] == upper.json()["actor_id"] == ALICE
        assert [row["id"] for row in await install.humans()] == [ALICE]

    async def test_the_header_wins_over_every_token(self, install):
        """With the header present, Authorization, the cookie and ``?token=``
        are not read, even when they carry a valid system token."""
        system = create_system_token(_SECRET)
        async with _client(_whoami_app()) as client:
            client.cookies.set("nerve_token", system)
            res = await client.get(
                f"/api/whoami?token={system}",
                headers={**_bearer(system), **_as(ALICE, "Alice")},
            )
        assert res.status_code == 200
        assert res.json()["actor_id"] == ALICE
        assert res.json()["kind"] == "human"

    async def test_a_malformed_header_is_not_rescued_by_a_token(self, install):
        async with _client(_whoami_app()) as client:
            res = await client.get(
                "/api/whoami",
                headers={
                    **_bearer(create_system_token(_SECRET)),
                    ACTOR_CONTEXT_HEADER: "abc",
                },
            )
        assert res.status_code == 401

    async def test_who_am_i_reports_a_person_without_an_account(self, install):
        async with _client(server.create_app()) as client:
            res = await client.get("/api/auth/me", headers=_as(ALICE, "Alice"))
            check = await client.get("/api/auth/check", headers=_as(ALICE, "Alice"))
        assert res.status_code == 200
        assert res.json() == {
            "actor": {
                "id": ALICE, "kind": "human", "display_name": "Alice",
                "username": None,
            },
            "account": None,
        }
        assert check.json() == {"authenticated": True}

    async def test_the_header_means_nothing_in_local_mode(
        self, tmp_path, open_identity_db, wire_identity_store,
    ):
        """Without external mode the header is not read at all."""
        set_config(_config(tmp_path))
        try:
            from nerve.gateway.auth import pin_jwt_secret

            pin_jwt_secret(_SECRET)
            database, _identity = await open_identity_db(tmp_path / "nerve.db")
            wire_identity_store(database)
            try:
                async with _client(_whoami_app()) as client:
                    res = await client.get("/api/whoami", headers=_as(ALICE, "Alice"))
                assert res.status_code == 401
                assert await database.get_actor_ref(ALICE) is None
            finally:
                await database.close()
        finally:
            set_config(NerveConfig())


# --------------------------------------------------------------------------- #
#  The actor row                                                               #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestActorRows:
    async def test_first_sight_adds_the_row(self, install):
        assert await install.db.get_actor_ref(ALICE) is None
        async with _client(_whoami_app()) as client:
            assert (
                await client.get("/api/whoami", headers=_as(ALICE, "Alice"))
            ).status_code == 200
        row = await install.db.get_actor_ref(ALICE)
        assert (row["kind"], row["display_name"], row["username"]) == (
            "human", "Alice", None,
        )
        assert await install.db.count_accounts() == 0

    async def test_a_new_display_name_updates_the_row(self, install):
        async with _client(_whoami_app()) as client:
            await client.get("/api/whoami", headers=_as(ALICE, "Alice"))
            res = await client.get("/api/whoami", headers=_as(ALICE, "Alice Smith"))
            nameless = await client.get("/api/whoami", headers=_as(ALICE))
        assert res.json()["display_name"] == "Alice Smith"
        assert nameless.json()["display_name"] is None
        assert (await install.db.get_actor_ref(ALICE))["display_name"] is None
        assert len(await install.humans()) == 1

    async def test_an_unchanged_name_takes_no_write_lock(self, install, monkeypatch):
        writes: list[str] = []
        original = install.db._write

        async def _counting(sql, params=()):
            writes.append(sql)
            return await original(sql, params)

        monkeypatch.setattr(install.db, "_write", _counting)
        async with _client(_whoami_app()) as client:
            for _ in range(3):
                await client.get("/api/whoami", headers=_as(ALICE, "Alice"))
            assert len(writes) == 1
            await client.get("/api/whoami", headers=_as(ALICE, "Alice Smith"))
            assert len(writes) == 2
            await client.get("/api/whoami", headers=_as(BOB, "Bob"))
            assert len(writes) == 3

    async def test_the_system_actor_id_is_refused(self, install):
        """The schema trigger aborts the insert; the request gets 401, not 500."""
        system_id = install.system_actor_id
        before = await install.db.get_actor_ref(system_id)
        async with _client(_whoami_app()) as client:
            res = await client.get("/api/whoami", headers=_as(system_id, "Mallory"))
            again = await client.get("/api/whoami", headers=_as(system_id.upper()))
        assert res.status_code == again.status_code == 401
        assert await install.db.get_actor_ref(system_id) == before
        assert before["kind"] == "system"


# --------------------------------------------------------------------------- #
#  Two people at once                                                          #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestConcurrentPeople:
    @staticmethod
    def _rendezvous(install, monkeypatch) -> None:
        """Hold each request in authentication until a second one arrives, so
        the requests overlap. A shared "current actor" would then leak."""
        barrier = asyncio.Barrier(2)
        original = install.db.upsert_external_actor

        async def _both_here(actor_id, display_name):
            await asyncio.wait_for(barrier.wait(), timeout=5)
            await original(actor_id, display_name)

        monkeypatch.setattr(install.db, "upsert_external_actor", _both_here)

    async def test_concurrent_requests_each_see_their_own_actor(
        self, install, monkeypatch,
    ):
        self._rendezvous(install, monkeypatch)
        async with _client(_whoami_app()) as client:
            results = await asyncio.gather(*(
                client.get("/api/whoami", headers=_as(pid, name))
                for pid, name in [(ALICE, "Alice"), (BOB, "Bob")] * 3
            ))
        assert [res.json()["actor_id"] for res in results] == [ALICE, BOB] * 3
        assert [res.json()["display_name"] for res in results] == ["Alice", "Bob"] * 3

    async def test_sessions_and_messages_keep_their_people_apart(
        self, install, monkeypatch,
    ):
        self._rendezvous(install, monkeypatch)
        async with _client(server.create_app()) as client:
            hers, his = await asyncio.gather(
                client.post("/api/sessions", headers=_as(ALICE, "Alice"), json={}),
                client.post("/api/sessions", headers=_as(BOB, "Bob"), json={}),
            )
            assert hers.status_code == his.status_code == 200
            assert await install.creator_of(hers.json()["id"]) == ALICE
            assert await install.creator_of(his.json()["id"]) == BOB

            shared = hers.json()["id"]
            sends = await asyncio.gather(*(
                client.post(
                    "/api/sessions/run-later",
                    headers=_as(pid, name),
                    json={"session_id": shared, "message": f"from {name}", "delay": "none"},
                )
                for pid, name in [(ALICE, "Alice"), (BOB, "Bob")]
            ))
        assert [res.status_code for res in sends] == [200, 200]
        assert sorted(await install.said_in(shared)) == sorted([
            ("from Alice", ALICE), ("from Bob", BOB),
        ])

    async def test_alternating_requests_never_reuse_an_earlier_actor(self, install):
        expected = [
            (_as(ALICE, "Alice"), ALICE),
            (_as(BOB, "Bob"), BOB),
            (_bearer(create_system_token(_SECRET)), install.system_actor_id),
            (_as(ALICE, "Alice"), ALICE),
        ]
        async with _client(_whoami_app()) as client:
            for headers, actor_id in expected:
                res = await client.get("/api/whoami", headers=headers)
                assert res.status_code == 200
                assert res.json()["actor_id"] == actor_id


# --------------------------------------------------------------------------- #
#  Tokens                                                                      #
# --------------------------------------------------------------------------- #


class _RecordingManager:
    """Stands in for the MCP session manager; counts admitted requests."""

    def __init__(self):
        self.calls = 0

    async def handle_request(self, scope, receive, send):
        self.calls += 1
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json")],
        })
        await send({"type": "http.response.body", "body": b"{}", "more_body": False})


@pytest.mark.asyncio
class TestTokens:
    async def test_session_tokens_are_refused_on_rest(self, install):
        account_id = await install.add_leftover_account()
        async with _client(_whoami_app()) as client:
            session = await client.get(
                "/api/whoami", headers=_bearer(create_session_token(_SECRET, account_id)),
            )
            legacy = await client.get("/api/whoami", headers=_bearer(_legacy_token()))
        assert session.status_code == legacy.status_code == 401
        assert session.json()["detail"] == EXTERNAL_SESSION_DETAIL
        assert legacy.json()["detail"] == EXTERNAL_SESSION_DETAIL

    async def test_system_tokens_give_the_system_actor(self, install):
        async with _client(_whoami_app()) as client:
            res = await client.get(
                "/api/whoami", headers=_bearer(create_system_token(_SECRET)),
            )
        assert res.status_code == 200
        assert res.json()["actor_id"] == install.system_actor_id
        assert res.json()["kind"] == "system"

    def _mcp_app(self, manager) -> FastAPI:
        from nerve.config import McpEndpointConfig, get_config
        from nerve.mcp_server.http import mount_deferred

        config = get_config()
        config.mcp_endpoint = McpEndpointConfig(enabled=True, path="/mcp/v1")
        app = FastAPI()
        mount_deferred(app, config, lambda: manager)
        return app

    async def test_the_mcp_endpoint_refuses_session_tokens(self, install):
        account_id = await install.add_leftover_account()
        manager = _RecordingManager()
        async with _client(self._mcp_app(manager)) as client:
            session = await client.post(
                "/mcp/v1/",
                headers=_bearer(create_session_token(_SECRET, account_id)),
                content=b"{}",
            )
            legacy = await client.post(
                "/mcp/v1/", headers=_bearer(_legacy_token()), content=b"{}",
            )
        assert session.status_code == legacy.status_code == 401
        assert manager.calls == 0

    async def test_the_mcp_endpoint_admits_system_and_mcp_tokens(self, install):
        manager = _RecordingManager()
        tokens = [
            create_system_token(_SECRET),
            create_mcp_session_token(_SECRET, "engine-sess-1"),
            create_external_mcp_token(_SECRET),
        ]
        async with _client(self._mcp_app(manager)) as client:
            for token in tokens:
                res = await client.post("/mcp/v1/", headers=_bearer(token), content=b"{}")
                assert res.status_code == 200
        assert manager.calls == len(tokens)

    def _worker_app(self, monkeypatch) -> FastAPI:
        from nerve.config import get_config
        from nerve.gateway.routes import codex as codex_routes

        monkeypatch.setattr(
            codex_routes, "get_deps",
            lambda: SimpleNamespace(engine=SimpleNamespace(config=get_config())),
        )
        app = FastAPI()
        app.include_router(codex_routes.router)
        return app

    async def test_the_worker_token_route_refuses_session_tokens(
        self, install, monkeypatch,
    ):
        account_id = await install.add_leftover_account()
        worker = {"worker_id": "ultracode-0123456789abcdef"}
        async with _client(self._worker_app(monkeypatch)) as client:
            refused = await client.post(
                "/api/codex/worker-token",
                headers=_bearer(create_session_token(_SECRET, account_id)),
                json=worker,
            )
            exchanged = await client.post(
                "/api/codex/worker-token",
                headers=_bearer(create_mcp_session_token(_SECRET, "engine-sess-1")),
                json=worker,
            )
        assert refused.status_code == 401
        assert refused.json()["detail"] == EXTERNAL_SESSION_DETAIL
        assert exchanged.status_code == 200


# --------------------------------------------------------------------------- #
#  Routes                                                                      #
# --------------------------------------------------------------------------- #


class TestRoutes:
    def test_account_and_setup_routes_are_not_registered(self, external):
        paths_in_use = _route_paths(server.create_app())
        assert not any(path.startswith("/api/accounts") for path in paths_in_use)
        assert "/api/setup/claim" not in paths_in_use
        for kept in ("/api/auth/check", "/api/auth/me", "/api/auth/status", "/api/actors"):
            assert kept in paths_in_use

    def test_local_mode_registers_them(self):
        paths_in_use = _route_paths(server.create_app())
        assert "/api/accounts" in paths_in_use
        assert "/api/setup/claim" in paths_in_use

    @pytest.mark.asyncio
    async def test_login_answers_404(self, install):
        async with _client(server.create_app()) as client:
            with_body = await client.post("/api/auth/login", json={"password": "x"})
            no_body = await client.post("/api/auth/login", json={})
        assert with_body.status_code == no_body.status_code == 404

    @pytest.mark.asyncio
    async def test_status_reports_the_mode_without_reading_login_state(
        self, install, monkeypatch,
    ):
        async def _no_login_state():
            raise AssertionError("external mode must not read login state")

        monkeypatch.setattr(install.db, "login_state", _no_login_state)
        async with _client(server.create_app()) as client:
            res = await client.get("/api/auth/status")
        assert res.status_code == 200
        assert res.json() == {
            "mode": "external", "auth_required": True, "login": "username_password",
        }


# --------------------------------------------------------------------------- #
#  The WebSocket                                                               #
# --------------------------------------------------------------------------- #


class _RecordingEngine:
    """Enough engine for ``/ws``; records what each message runs as."""

    def __init__(self):
        self.session_actors: list[Actor] = []
        self.runs: list[dict] = []
        self.router = SimpleNamespace(get_last_session=self._no_session)
        self.sessions = SimpleNamespace(get_active_session=self._session)

    async def _no_session(self, *_args, **_kwargs):
        return None

    async def _session(self, *_args, actor=None, **_kwargs):
        self.session_actors.append(actor)
        return "ws-session"

    def is_session_running(self, *_args, **_kwargs):
        return False

    async def run(self, **kwargs):
        self.runs.append(kwargs)

    def register_task(self, *_args, **_kwargs):
        return None


async def _open_external_db(db_path, config) -> Database:
    database = Database(db_path)
    await database.connect()
    await bootstrap_identity(database, config)
    return database


@pytest.fixture
def ws_instance(tmp_path, external, wire_identity_store, monkeypatch):
    """The real ``/ws`` endpoint of ``create_app()`` in external mode."""
    config = _config(tmp_path)
    set_config(config)
    app = server.create_app()
    app.router.lifespan_context = _no_lifespan
    engine = _RecordingEngine()
    with TestClient(app) as client:
        database = client.portal.call(_open_external_db, tmp_path / "nerve.db", config)
        wire_identity_store(database)
        monkeypatch.setattr(server, "_engine", engine, raising=False)
        try:
            yield SimpleNamespace(client=client, db=database, engine=engine)
        finally:
            client.portal.call(database.close)
    set_config(NerveConfig())


def _wait_for(predicate, *, within: float = 5.0) -> None:
    deadline = time.monotonic() + within
    while not predicate():
        if time.monotonic() > deadline:
            pytest.fail("the condition did not become true in time")
        time.sleep(0.01)


def _close_code(client: TestClient, url: str, headers: dict | None = None) -> int:
    with pytest.raises(WebSocketDisconnect) as ei:
        with client.websocket_connect(url, headers=headers or {}) as socket:
            socket.receive_json()
    return ei.value.code


class TestWebSocket:
    def test_an_external_person_is_admitted_and_keeps_the_actor(self, ws_instance):
        client, engine = ws_instance.client, ws_instance.engine
        with client.websocket_connect("/ws", headers=_as(ALICE, "Alice")) as socket:
            assert socket.receive_json() == {
                "type": "session_switched", "session_id": "ws-session",
            }
            (connection, _socket), = server._live_sockets.values()
            assert connection.actor == Actor(
                actor_id=ALICE, kind="human", account_id=None, display_name="Alice",
            )
            assert engine.session_actors == [connection.actor]

            socket.send_json({"type": "message", "content": "one", "session_id": "ws-session"})
            _wait_for(lambda: len(engine.runs) == 1)

            # A later request renames the person. The open connection keeps the
            # actor it was admitted with.
            renamed = client.get("/api/auth/me", headers=_as(ALICE, "Alice Smith"))
            assert renamed.json()["actor"]["display_name"] == "Alice Smith"

            socket.send_json({"type": "message", "content": "two", "session_id": "ws-session"})
            _wait_for(lambda: len(engine.runs) == 2)
            socket.send_json({"type": "ping"})
            assert socket.receive_json() == {"type": "pong"}

        assert [run["actor"] for run in engine.runs] == [connection.actor] * 2
        assert [run["user_message"] for run in engine.runs] == ["one", "two"]
        assert server._live_sockets == {}

    def test_refused_upgrades_close_with_4001(self, ws_instance):
        client = ws_instance.client
        account_id = client.portal.call(
            functools.partial(ws_instance.db._bootstrap_first_account, credential_source="none"),
        ).account_id
        session = create_session_token(_SECRET, account_id)
        assert _close_code(client, "/ws") == 4001
        assert _close_code(client, f"/ws?token={session}") == 4001
        assert _close_code(client, f"/ws?token={_legacy_token()}") == 4001
        assert _close_code(client, "/ws", {ACTOR_CONTEXT_HEADER: "abc"}) == 4001
        assert _close_code(
            client, "/ws", _as(ws_instance.db.system_actor_id, "Mallory"),
        ) == 4001

    def test_a_system_token_is_still_admitted(self, ws_instance):
        token = create_system_token(_SECRET)
        with ws_instance.client.websocket_connect(f"/ws?token={token}") as socket:
            assert socket.receive_json()["type"] == "session_switched"
            (connection, _socket), = server._live_sockets.values()
            assert connection.actor.is_system


class _Socket:
    """The parts of a WebSocket that the ``/ws`` handler touches."""

    def __init__(self, headers: dict, frames: list[dict]):
        self.headers = headers
        self.query_params = {}
        self.cookies = {}
        self.sent: list[dict] = []
        self._frames = list(frames)

    async def accept(self) -> None:
        return None

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.sent.append({"closed": code})

    async def send_json(self, payload: dict) -> None:
        self.sent.append(payload)

    async def receive_json(self) -> dict:
        if not self._frames:
            raise WebSocketDisconnect(1000)
        return self._frames.pop(0)


def _ws_endpoint():
    app = server.create_app()
    return next(
        route.endpoint for route in app.routes if getattr(route, "path", "") == "/ws"
    )


async def _wait_for_user_messages(install, session_id: str, count: int) -> None:
    for _ in range(500):
        if len(await install.said_in(session_id)) >= count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"{session_id} did not get {count} user messages")


@pytest.mark.asyncio
class TestWebSocketAttribution:
    async def test_messages_are_attributed_to_each_connection(self, install, monkeypatch):
        async def _no_model(*_args, **_kwargs):
            raise RuntimeError("no model in this test")

        monkeypatch.setattr(install.engine, "_get_or_create_client", _no_model)
        monkeypatch.setattr(server, "_engine", install.engine)
        endpoint = _ws_endpoint()
        session_id = "ws-shared"
        await install.db.create_session(session_id, source="web", actor=None)

        echoes: list[dict] = []
        await broadcaster.register(
            session_id, "listener", lambda _sid, msg: echoes.append(msg),
        )
        try:
            await asyncio.gather(*(
                endpoint(_Socket(_as(pid, name), [{
                    "type": "message", "content": f"from {name}",
                    "session_id": session_id,
                }]))
                for pid, name in [(ALICE, "Alice"), (BOB, "Bob")]
            ))
            await _wait_for_user_messages(install, session_id, 2)
        finally:
            await broadcaster.unregister(session_id, "listener")

        assert sorted(await install.said_in(session_id)) == sorted([
            ("from Alice", ALICE), ("from Bob", BOB),
        ])
        assert {
            (msg["content"], msg["actor_id"])
            for msg in echoes if msg.get("type") == "user_message"
        } == {("from Alice", ALICE), ("from Bob", BOB)}

    async def test_a_session_created_over_the_socket_belongs_to_the_person(
        self, install, monkeypatch,
    ):
        monkeypatch.setattr(server, "_engine", install.engine)
        socket = _Socket(_as(BOB, "Bob"), [])
        await _ws_endpoint()(socket)
        switched = next(msg for msg in socket.sent if msg.get("type") == "session_switched")
        assert await install.creator_of(switched["session_id"]) == BOB


# --------------------------------------------------------------------------- #
#  Startup                                                                     #
# --------------------------------------------------------------------------- #


class _ProxyThatFails:
    """Stops the lifespan right after the identity and setup-token steps."""

    async def start(self):
        raise RuntimeError("stop after the identity steps")

    async def stop(self):
        return None


class TestStartup:
    @pytest.mark.asyncio
    async def test_the_bootstrap_creates_no_account(self, install):
        assert await install.db.count_accounts() == 0
        assert await install.humans() == []
        assert not install.report.bootstrapped_account
        assert pinned_jwt_secret() == _SECRET

    @pytest.mark.asyncio
    async def test_the_bootstrap_still_generates_a_signing_secret(self, tmp_path, external):
        database = Database(tmp_path / "nerve.db")
        await database.connect()
        try:
            report = await bootstrap_identity(database, NerveConfig())
            assert report.generated_jwt_secret and not report.bootstrapped_account
            assert await database.count_accounts() == 0
            assert pinned_jwt_secret()
        finally:
            await database.close()

    def test_nerve_init_creates_no_account(self, monkeypatch):
        """``nerve init`` does not start the gateway, so it reads the mode
        from the environment."""
        from nerve.migrate import bootstrap_identity_sync

        monkeypatch.setenv(AUTH_MODE_ENV, "external")
        report = bootstrap_identity_sync(
            NerveConfig(), display_name="Alice", passwordless=True,
        )
        assert not report.bootstrapped_account
        assert inspect_bootstrap_state(paths.db_path()) == ([], True)

    def test_the_identity_preview_reports_no_account(self, monkeypatch):
        from nerve.migrate import _inspect_identity

        monkeypatch.setenv(AUTH_MODE_ENV, "external")
        report = MigrationReport(dry_run=True)
        _inspect_identity(NerveConfig(), paths.db_path(), report)
        assert not report.bootstrapped_account
        assert report.generated_jwt_secret

    @pytest.mark.asyncio
    async def test_startup_deletes_a_setup_token_left_from_local_mode(
        self, tmp_path, monkeypatch,
    ):
        import nerve.proxy.service as proxy_module
        from nerve import setup_token

        database = Database(paths.db_path())
        await database.connect()
        try:
            assert await setup_token.ensure_setup_token(database, unclaimed=True)
        finally:
            await database.close()

        config = NerveConfig()
        config.workspace = tmp_path / "ws"
        config.workspace.mkdir()
        config.proxy.enabled = True
        config.mcp_endpoint.enabled = False
        set_config(config)
        try:
            monkeypatch.setenv(AUTH_MODE_ENV, "external")
            app = server.create_app()
            monkeypatch.setattr(proxy_module, "ProxyService", lambda cfg: _ProxyThatFails())
            with pytest.raises(RuntimeError, match="stop after the identity steps"):
                async with server.lifespan(app):
                    pass
        finally:
            set_config(NerveConfig())

        assert inspect_bootstrap_state(paths.db_path()) == ([], True)
        assert read_instance_secret(paths.db_path(), setup_token.SETUP_TOKEN_NAME) == ""

    def test_nerve_start_refuses_an_unknown_mode(self, tmp_path, monkeypatch):
        """The command stops before it starts a daemon that would stop at once."""
        from click.testing import CliRunner

        from nerve import cli

        TestDoctor._config(tmp_path)
        monkeypatch.setattr(
            cli, "_get_daemon_status",
            lambda: pytest.fail("start must stop before it looks for a daemon"),
        )
        monkeypatch.setenv(AUTH_MODE_ENV, "hosted")
        result = CliRunner().invoke(cli.main, ["-c", str(tmp_path / "cfg"), "start"])
        assert result.exit_code == 1
        assert f"{AUTH_MODE_ENV} must be one of" in result.output


class TestDoctor:
    @staticmethod
    def _config(tmp_path) -> NerveConfig:
        config_dir, workspace = tmp_path / "cfg", tmp_path / "ws"
        config_dir.mkdir()
        (workspace / "config").mkdir(parents=True)
        (config_dir / "config.yaml").write_text(
            f"workspace: {workspace}\n", encoding="utf-8",
        )
        (config_dir / "config.local.yaml").write_text("{}\n", encoding="utf-8")
        return load_config(config_dir)

    @pytest.mark.asyncio
    async def test_no_warning_about_a_missing_local_account(self, tmp_path, monkeypatch):
        from nerve.cli import doctor_report

        database = Database(paths.db_path())
        await database.connect()
        await database.close()
        config = self._config(tmp_path)

        assert "No local account" in doctor_report(config)
        monkeypatch.setenv(AUTH_MODE_ENV, "external")
        report = doctor_report(config)
        assert "No local account" not in report
        assert "Auth mode: external" in report

    def test_an_unknown_mode_is_an_error(self, tmp_path, monkeypatch):
        from nerve.cli import doctor_report

        monkeypatch.setenv(AUTH_MODE_ENV, "hosted")
        report = doctor_report(self._config(tmp_path))
        assert f"[ERR] {AUTH_MODE_ENV} must be one of" in report
