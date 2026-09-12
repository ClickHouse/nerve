"""The actor on the request.

Every authenticated ingress resolves *who* is calling, from the database, on
the request itself. This file is about that resolution and its failure modes:

* a session token resolves to its own account, and only while that account is
  enabled and exists;
* the credentials the instance mints for itself — the CLI, the agent calling
  its own API, backend agent subprocesses, MCP clients — resolve to the agent's
  system principal;
* a session minted before per-account logins existed resolves to the sole
  account while there is exactly one, and is refused once there are two;
* a WebSocket's actor is fixed at accept and does not drift;
* two people calling at the same moment each see themselves, which is the
  property a process-global "current user" would quietly destroy (0.7).
"""

from __future__ import annotations

import asyncio
import inspect

import httpx
import jwt
import pytest
import pytest_asyncio
from fastapi import Depends, FastAPI, Request

from nerve.config import AuthConfig, NerveConfig, set_config
from nerve.gateway.auth import (
    JWT_ALGORITHM,
    MCP_AUDIENCE,
    MCP_SESSION_CLAIM,
    MCP_WORKER_CLAIM,
    NO_IDENTITY_DETAIL,
    SESSION_TOKEN_HEADER,
    TOKEN_TYPE_CLAIM,
    TOKEN_TYPE_SESSION,
    create_external_mcp_token,
    create_mcp_session_token,
    create_session_token,
    create_system_token,
    identity_store,
    is_legacy_session_token,
    maybe_refresh_token,
    pin_jwt_secret,
    require_auth,
    resolve_actor_from_claims,
)
from nerve.identity import Actor, ActorResolutionError
from nerve.gateway.server import WebSocketConnection, _accept_websocket

_SECRET = "test-secret-for-request-actors-padded-32b"
_OTHER_SECRET = "another-secret-for-request-actors-padded32"


# --------------------------------------------------------------------------- #
#  Fixtures                                                                    #
# --------------------------------------------------------------------------- #


class _Install:
    """One bootstrapped instance, plus the accounts a test adds to it."""

    def __init__(self, db, identity):
        self.db = db
        self.identity = identity
        self.account_id = identity.owner_account_id
        self.actor_id = identity.owner_actor_id

    async def add_account(self, display_name: str) -> tuple[str, str]:
        """A second person. Returns ``(account_id, actor_id)``."""
        actor = await self.db.create_actor_ref(kind="human", display_name=display_name)
        account = await self.db.create_account(
            actor_id=actor["id"], credential_source="none",
        )
        return account["id"], actor["id"]

    def session_token(self, account_id: str | None = None, secret: str = _SECRET) -> str:
        return create_session_token(secret, account_id or self.account_id)

    def legacy_token(self, secret: str = _SECRET) -> str:
        """What a browser that logged in before this version is holding: the
        fixed subject, no type claim. Built by hand — nothing mints these now.
        """
        from datetime import datetime, timedelta, timezone

        now = datetime.now(timezone.utc)
        return jwt.encode(
            {"iat": now, "exp": now + timedelta(hours=720), "sub": "user"},
            secret,
            algorithm=JWT_ALGORITHM,
        )


def pre_typ_mcp_token(
    *, session_id: str | None = None, worker_id: str | None = None,
) -> str:
    """An MCP credential of the shape minted before ``typ`` existed.

    Backend subprocesses and clients started with ``nerve codex token`` are
    holding 8-hour tokens like this across the upgrade. Hand-minted, because
    nothing produces the shape any more: what keeps them working is the
    **audience**, which the resolver reads before ``typ``, and this is what
    stops a later reordering of that dispatch from cutting them off silently.
    Without ``session_id`` it is the external (satellite) shape.
    """
    from datetime import datetime, timedelta, timezone
    from uuid import uuid4

    now = datetime.now(timezone.utc)
    payload = {
        "iat": now,
        "exp": now + timedelta(hours=8),
        "jti": uuid4().hex,
        "sub": "backend-agent" if session_id else "external-agent-mcp",
        "aud": MCP_AUDIENCE,
    }
    if session_id:
        payload[MCP_SESSION_CLAIM] = session_id
    if worker_id:
        payload[MCP_WORKER_CLAIM] = worker_id
    return jwt.encode(payload, _SECRET, algorithm=JWT_ALGORITHM)


@pytest_asyncio.fixture
async def install(tmp_path, open_identity_db, wire_identity_store):
    """A running instance's identity: one account, one system principal."""
    set_config(NerveConfig(auth=AuthConfig(jwt_secret=_SECRET, jwt_expiry_hours=720)))
    pin_jwt_secret(_SECRET)
    database, identity = await open_identity_db(tmp_path / "nerve.db")
    wire_identity_store(database)
    try:
        yield _Install(database, identity)
    finally:
        await database.close()
        set_config(NerveConfig())


def _app() -> FastAPI:
    """A gateway-shaped app: the real dependency and the real slide middleware."""
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


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://nerve-test",
    )


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


class _Socket:
    """The parts of a WebSocket the accept path touches."""

    def __init__(self, token: str | None = None, cookie: str | None = None):
        self.query_params = {"token": token} if token else {}
        self.cookies = {"nerve_token": cookie} if cookie else {}
        self.accepted = False
        self.closed: tuple[int, str] | None = None

    async def accept(self) -> None:
        self.accepted = True

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closed = (code, reason)


# --------------------------------------------------------------------------- #
#  A session token resolves to its own account                                 #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestSessionTokens:
    async def test_a_session_resolves_to_its_account(self, install):
        await install.db.update_actor_profile(install.actor_id, display_name="Alice")
        async with _client(_app()) as client:
            res = await client.get(
                "/api/whoami", headers=_bearer(install.session_token()),
            )
        assert res.status_code == 200
        assert res.json() == {
            "actor_id": install.actor_id,
            "kind": "human",
            "account_id": install.account_id,
            "display_name": "Alice",
        }

    async def test_a_disabled_account_is_refused_at_the_next_request(self, install):
        """Disablement takes effect on the next check, not retroactively: the
        token is still signed and unexpired, so the account row is the only
        thing that can stop it."""
        token = install.session_token()
        app = _app()
        async with _client(app) as client:
            assert (await client.get("/api/whoami", headers=_bearer(token))).status_code == 200
            await install.db.set_account_enabled(install.account_id, False)
            refused = await client.get("/api/whoami", headers=_bearer(token))
            assert refused.status_code == 401
            assert "disabled" in refused.json()["detail"]
            # ...and re-enabling lets the same token back in.
            await install.db.set_account_enabled(install.account_id, True)
            assert (await client.get("/api/whoami", headers=_bearer(token))).status_code == 200

    async def test_an_unknown_account_is_refused(self, install):
        """A correctly signed token naming an account that is not there — a
        deleted account, or a token minted against another instance's data —
        resolves to nobody rather than to whoever is left."""
        token = create_session_token(_SECRET, "99999999-9999-4999-8999-999999999999")
        async with _client(_app()) as client:
            res = await client.get("/api/whoami", headers=_bearer(token))
        assert res.status_code == 401
        assert "no longer exists" in res.json()["detail"]

    async def test_the_display_name_is_a_snapshot_taken_per_request(self, install):
        """Renaming changes what later requests carry and nothing else: the
        actor id — what attribution stores — is untouched."""
        await install.db.update_actor_profile(install.actor_id, display_name="Alice")
        app, token = _app(), install.session_token()
        async with _client(app) as client:
            first = (await client.get("/api/whoami", headers=_bearer(token))).json()
            await install.db.update_actor_profile(install.actor_id, display_name="Alice B")
            second = (await client.get("/api/whoami", headers=_bearer(token))).json()
        assert first["display_name"] == "Alice"
        assert second["display_name"] == "Alice B"
        assert first["actor_id"] == second["actor_id"]

    async def test_a_token_signed_with_another_secret_is_refused(self, install):
        async with _client(_app()) as client:
            res = await client.get(
                "/api/whoami",
                headers=_bearer(install.session_token(secret=_OTHER_SECRET)),
            )
        assert res.status_code == 401


# --------------------------------------------------------------------------- #
#  The instance acting on its own behalf                                       #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestSystemPrincipal:
    async def test_a_system_token_resolves_to_the_system_principal(self, install):
        async with _client(_app()) as client:
            res = await client.get(
                "/api/whoami", headers=_bearer(create_system_token(_SECRET)),
            )
        assert res.status_code == 200
        body = res.json()
        assert body["kind"] == "system"
        assert body["account_id"] is None
        assert body["actor_id"] == install.identity.system_actor_id
        # And it is not a person: no account row points at it.
        assert await install.db.get_account_by_actor(body["actor_id"]) is None

    async def test_mcp_and_worker_credentials_resolve_to_the_system_principal(
        self, install,
    ):
        """Every shape the MCP endpoint accepts from the instance's own
        subprocesses, including the worker tokens Ultracode children
        exchange for."""
        system_actor_id = install.identity.system_actor_id
        for token in (
            create_mcp_session_token(_SECRET, "engine-sess-1"),
            create_mcp_session_token(
                _SECRET, "engine-sess-1", worker_id="ultracode-0123456789abcdef",
            ),
            create_external_mcp_token(_SECRET),
        ):
            claims = jwt.decode(
                token, _SECRET, algorithms=[JWT_ALGORITHM], audience=MCP_AUDIENCE,
            )
            actor = await resolve_actor_from_claims(install.db, claims)
            assert actor.actor_id == system_actor_id
            assert actor.kind == "system" and actor.account_id is None

    async def test_credentials_minted_before_typ_resolve_the_same_way(self, install):
        """The MCP tokens already in flight when this version starts carry the
        audience and no type claim. They resolve on the audience alone, which
        is what the resolver reads first."""
        system_actor_id = install.identity.system_actor_id
        for token in (
            pre_typ_mcp_token(session_id="engine-sess-1"),
            pre_typ_mcp_token(
                session_id="engine-sess-1", worker_id="ultracode-0123456789abcdef",
            ),
            pre_typ_mcp_token(),
        ):
            claims = jwt.decode(
                token, _SECRET, algorithms=[JWT_ALGORITHM], audience=MCP_AUDIENCE,
            )
            assert TOKEN_TYPE_CLAIM not in claims
            actor = await resolve_actor_from_claims(install.db, claims)
            assert actor.actor_id == system_actor_id
            assert actor.is_system and actor.account_id is None
            # Still not a web session, so still no sliding.
            assert maybe_refresh_token(claims, _SECRET, actor) is None

    async def test_an_mcp_credential_cannot_authenticate_a_web_route(self, install):
        """Audience-scoped tokens never pass ordinary web auth, so the system
        principal cannot arrive through the browser's door by that route."""
        async with _client(_app()) as client:
            res = await client.get(
                "/api/whoami",
                headers=_bearer(create_mcp_session_token(_SECRET, "engine-sess-1")),
            )
        assert res.status_code == 401

    async def test_system_resolution_needs_a_bootstrapped_instance(self, db):
        """Before bootstrap there is no principal to act as — refuse rather
        than invent one."""
        with pytest.raises(ActorResolutionError, match="system principal"):
            await resolve_actor_from_claims(
                db, jwt.decode(
                    create_system_token(_SECRET), _SECRET,
                    algorithms=[JWT_ALGORITHM],
                ),
            )


# --------------------------------------------------------------------------- #
#  The MCP endpoint and the worker-token exchange                              #
# --------------------------------------------------------------------------- #


class _RecordingManager:
    """Stands in for the MCP session manager; remembers the scope it saw."""

    def __init__(self):
        self.scopes = []

    async def handle_request(self, scope, receive, send):
        self.scopes.append(scope)
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json")],
        })
        await send({"type": "http.response.body", "body": b"{}", "more_body": False})


@pytest.mark.asyncio
class TestMcpEndpointResolvesAnActor:
    def _mounted(self, manager):
        from nerve.config import McpEndpointConfig, get_config
        from nerve.mcp_server.http import mount_deferred

        config = get_config()
        config.mcp_endpoint = McpEndpointConfig(enabled=True, path="/mcp/v1")
        app = FastAPI()
        mount_deferred(app, config, lambda: manager)
        return app

    async def test_a_backend_credential_arrives_as_the_system_principal(self, install):
        manager = _RecordingManager()
        async with _client(self._mounted(manager)) as client:
            res = await client.post(
                "/mcp/v1/",
                headers=_bearer(create_mcp_session_token(_SECRET, "engine-sess-1")),
                content=b"{}",
            )
        assert res.status_code == 200
        from nerve.mcp_server.http import MCP_ACTOR_SCOPE_KEY

        actor = manager.scopes[0][MCP_ACTOR_SCOPE_KEY]
        assert actor.actor_id == install.identity.system_actor_id
        assert actor.is_system

    async def test_a_credential_minted_before_typ_still_gets_in(self, install):
        """End to end at the door the backend subprocesses actually knock on."""
        manager = _RecordingManager()
        async with _client(self._mounted(manager)) as client:
            res = await client.post(
                "/mcp/v1/",
                headers=_bearer(pre_typ_mcp_token(session_id="engine-sess-1")),
                content=b"{}",
            )
        assert res.status_code == 200
        from nerve.mcp_server.http import MCP_ACTOR_SCOPE_KEY

        assert manager.scopes[0][MCP_ACTOR_SCOPE_KEY].actor_id == (
            install.identity.system_actor_id
        )

    async def test_a_persons_token_arrives_as_that_person(self, install):
        """A session token is what an external MCP client (Codex, Claude Code)
        presents after logging in through the web flow; it is that person, and
        the endpoint says so rather than flattening everyone to the agent."""
        manager = _RecordingManager()
        async with _client(self._mounted(manager)) as client:
            res = await client.post(
                "/mcp/v1/", headers=_bearer(install.session_token()), content=b"{}",
            )
        assert res.status_code == 200
        from nerve.mcp_server.http import MCP_ACTOR_SCOPE_KEY

        actor = manager.scopes[0][MCP_ACTOR_SCOPE_KEY]
        assert actor.account_id == install.account_id and actor.is_human

    async def test_a_disabled_account_is_refused_at_the_mcp_door(self, install):
        """What a signature check alone would never notice."""
        manager = _RecordingManager()
        app = self._mounted(manager)
        token = install.session_token()
        async with _client(app) as client:
            assert (
                await client.post("/mcp/v1/", headers=_bearer(token), content=b"{}")
            ).status_code == 200
            await install.db.set_account_enabled(install.account_id, False)
            refused = await client.post(
                "/mcp/v1/", headers=_bearer(token), content=b"{}",
            )
        assert refused.status_code == 401
        assert "disabled" in refused.json()["error"]
        assert len(manager.scopes) == 1  # the refused frame never reached it

    async def test_it_refuses_before_the_manager_exists(self, install):
        from nerve.config import McpEndpointConfig, get_config

        from nerve.mcp_server.http import mount_deferred

        config = get_config()
        config.mcp_endpoint = McpEndpointConfig(enabled=True, path="/mcp/v1")
        app = FastAPI()
        mount_deferred(app, config, lambda: None)
        async with _client(app) as client:
            res = await client.post(
                "/mcp/v1/", headers=_bearer(install.session_token()), content=b"{}",
            )
        assert res.status_code == 503


@pytest.mark.asyncio
class TestWorkerTokenExchange:
    def _app(self, install, monkeypatch):
        from types import SimpleNamespace

        from nerve.config import get_config
        from nerve.gateway.routes import codex as codex_routes

        monkeypatch.setattr(
            codex_routes, "get_deps",
            lambda: SimpleNamespace(engine=SimpleNamespace(config=get_config())),
        )
        app = FastAPI()
        app.include_router(codex_routes.router)
        return app

    async def test_a_parent_session_token_is_exchanged(self, install, monkeypatch):
        worker_id = "ultracode-0123456789abcdef"
        async with _client(self._app(install, monkeypatch)) as client:
            res = await client.post(
                "/api/codex/worker-token",
                headers=_bearer(create_mcp_session_token(_SECRET, "engine-sess-1")),
                json={"worker_id": worker_id},
            )
        assert res.status_code == 200
        assert res.json()["worker_id"] == worker_id

    async def test_a_parent_token_minted_before_typ_is_still_exchanged(
        self, install, monkeypatch,
    ):
        """An Ultracode run that started before the upgrade keeps being able to
        hand its children worker tokens."""
        worker_id = "ultracode-0123456789abcdef"
        async with _client(self._app(install, monkeypatch)) as client:
            res = await client.post(
                "/api/codex/worker-token",
                headers=_bearer(pre_typ_mcp_token(session_id="engine-sess-1")),
                json={"worker_id": worker_id},
            )
        assert res.status_code == 200
        assert res.json()["worker_id"] == worker_id

    async def test_a_disabled_account_cannot_exchange(self, install, monkeypatch):
        """The actor is resolved before the token's shape is judged, so a
        credential whose account is gone is refused as unauthenticated rather
        than as the wrong kind of token."""
        await install.db.set_account_enabled(install.account_id, False)
        async with _client(self._app(install, monkeypatch)) as client:
            res = await client.post(
                "/api/codex/worker-token",
                headers=_bearer(install.session_token()),
                json={"worker_id": "ultracode-0123456789abcdef"},
            )
        assert res.status_code == 401
        assert "disabled" in res.json()["detail"]


# --------------------------------------------------------------------------- #
#  The grandfather clause                                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestGrandfatheredSessions:
    async def test_one_account_resolves_and_the_token_is_upgraded(self, install):
        """An open tab keeps working, and stops being legacy on its first
        call: the reply carries a proper session token for the account it
        resolved to, over the header the frontend already absorbs."""
        legacy = install.legacy_token()
        assert is_legacy_session_token(
            jwt.decode(legacy, _SECRET, algorithms=[JWT_ALGORITHM]),
        )
        app = _app()
        async with _client(app) as client:
            res = await client.get("/api/whoami", headers=_bearer(legacy))
            assert res.status_code == 200
            assert res.json()["account_id"] == install.account_id

            upgraded = res.headers.get(SESSION_TOKEN_HEADER)
            assert upgraded and upgraded != legacy
            claims = jwt.decode(upgraded, _SECRET, algorithms=[JWT_ALGORITHM])
            assert claims["sub"] == install.account_id
            assert claims[TOKEN_TYPE_CLAIM] == TOKEN_TYPE_SESSION

            # The replacement authenticates, and is not itself upgraded again.
            again = await client.get("/api/whoami", headers=_bearer(upgraded))
            assert again.status_code == 200
            assert SESSION_TOKEN_HEADER not in again.headers

    async def test_two_accounts_refuse_it_rather_than_guess(self, install):
        """With a second account the old token names nobody in particular.
        Resolving it to whichever row sorts first would attribute one person's
        work to another, so those tabs re-login at that moment."""
        await install.add_account("Bob")
        app = _app()
        async with _client(app) as client:
            res = await client.get("/api/whoami", headers=_bearer(install.legacy_token()))
            assert res.status_code == 401
            assert SESSION_TOKEN_HEADER not in res.headers
            # The accounts' own session tokens are unaffected.
            ok = await client.get("/api/whoami", headers=_bearer(install.session_token()))
            assert ok.status_code == 200

    async def test_a_disabled_sole_account_is_still_refused(self, install):
        await install.db.set_account_enabled(install.account_id, False)
        async with _client(_app()) as client:
            res = await client.get("/api/whoami", headers=_bearer(install.legacy_token()))
        assert res.status_code == 401

    async def test_the_predicate_is_narrow(self):
        """Only the exact pre-account shape is grandfathered."""
        assert is_legacy_session_token({"sub": "user"})
        assert not is_legacy_session_token({"sub": "user", TOKEN_TYPE_CLAIM: "session"})
        assert not is_legacy_session_token({"sub": "user", "aud": MCP_AUDIENCE})
        assert not is_legacy_session_token({"sub": "someone-else"})
        assert not is_legacy_session_token({})


# --------------------------------------------------------------------------- #
#  Login                                                                       #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestLoginMintsAccountSessions:
    def _login_app(self) -> FastAPI:
        from nerve.gateway.routes.auth import router as auth_router

        app = _app()
        app.include_router(auth_router)
        return app

    async def test_passwordless_login_names_the_sole_account(self, install):
        async with _client(self._login_app()) as client:
            res = await client.post("/api/auth/login", json={"password": "anything"})
            assert res.status_code == 200
            token = res.json()["token"]
            claims = jwt.decode(token, _SECRET, algorithms=[JWT_ALGORITHM])
            assert claims["sub"] == install.account_id
            assert claims[TOKEN_TYPE_CLAIM] == TOKEN_TYPE_SESSION
            me = await client.get("/api/whoami", headers=_bearer(token))
            assert me.json()["actor_id"] == install.actor_id

    async def test_a_configured_password_still_names_the_sole_account(self, install):
        import bcrypt

        from nerve.config import get_config

        password = "correct horse battery staple"
        get_config().auth.password_hash = bcrypt.hashpw(
            password.encode(), bcrypt.gensalt(rounds=4),
        ).decode()
        async with _client(self._login_app()) as client:
            assert (
                await client.post("/api/auth/login", json={"password": "wrong"})
            ).status_code == 401
            res = await client.post("/api/auth/login", json={"password": password})
            assert res.status_code == 200
            claims = jwt.decode(res.json()["token"], _SECRET, algorithms=[JWT_ALGORITHM])
            assert claims["sub"] == install.account_id

    async def test_a_disabled_sole_account_cannot_log_in(self, install):
        await install.db.set_account_enabled(install.account_id, False)
        async with _client(self._login_app()) as client:
            res = await client.post("/api/auth/login", json={"password": ""})
        assert res.status_code == 401
        assert "disabled" in res.json()["detail"]


# --------------------------------------------------------------------------- #
#  Two people at once                                                          #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestNoProcessGlobalActor:
    async def test_concurrent_requests_each_see_their_own_actor(self, install):
        """Required by 0.7. Both requests are inside actor resolution at the
        same moment — a barrier in the middle of the database lookup holds the
        first one there until the second arrives — so anything shared between
        them (a module global, a cache, a "current user") shows up as one
        answer where there should be two.
        """
        bob_account, bob_actor = await install.add_account("Bob")
        await install.db.update_actor_profile(install.actor_id, display_name="Alice")

        barrier = asyncio.Barrier(2)
        original = install.db.get_actor_ref

        async def _rendezvous(actor_id: str):
            row = await original(actor_id)
            await barrier.wait()   # nobody leaves until both are here
            return await original(actor_id)

        install.db.get_actor_ref = _rendezvous
        try:
            app = _app()
            async with _client(app) as client:
                alice, bob = await asyncio.gather(
                    client.get("/api/whoami", headers=_bearer(install.session_token())),
                    client.get("/api/whoami", headers=_bearer(install.session_token(bob_account))),
                )
        finally:
            install.db.get_actor_ref = original

        assert alice.status_code == bob.status_code == 200
        assert alice.json() == {
            "actor_id": install.actor_id, "kind": "human",
            "account_id": install.account_id, "display_name": "Alice",
        }
        assert bob.json() == {
            "actor_id": bob_actor, "kind": "human",
            "account_id": bob_account, "display_name": "Bob",
        }

    async def test_alternating_requests_are_never_served_from_a_previous_one(
        self, install,
    ):
        """The other half of the same property, and the one a cache breaks:
        requests that do *not* overlap must still each be resolved. Anything
        remembered between them — a memo, a "last actor", a key-less cache —
        answers the second caller with the first caller's identity.
        """
        bob_account, bob_actor = await install.add_account("Bob")
        expected = [
            (install.session_token(), install.actor_id),
            (install.session_token(bob_account), bob_actor),
            (install.session_token(), install.actor_id),
            (install.session_token(bob_account), bob_actor),
            (create_system_token(_SECRET), install.identity.system_actor_id),
            (install.session_token(), install.actor_id),
        ]
        async with _client(_app()) as client:
            for token, actor_id in expected:
                res = await client.get("/api/whoami", headers=_bearer(token))
                assert res.status_code == 200
                assert res.json()["actor_id"] == actor_id

    async def test_no_module_keeps_an_actor_after_serving_requests(self, install):
        """Structural, and the check that does not depend on timing: after two
        people and the agent itself have been served, no module in the package
        is holding an :class:`Actor`. An actor lives on the request, the
        connection or the call chain — never on a module.
        """
        import sys

        bob_account, _ = await install.add_account("Bob")
        async with _client(_app()) as client:
            for token in (
                install.session_token(),
                install.session_token(bob_account),
                create_system_token(_SECRET),
            ):
                assert (
                    await client.get("/api/whoami", headers=_bearer(token))
                ).status_code == 200

        holders = []
        for name, module in list(sys.modules.items()):
            if module is None or not (name == "nerve" or name.startswith("nerve.")):
                continue
            for attribute, value in list(vars(module).items()):
                if isinstance(value, Actor):
                    holders.append(f"{name}.{attribute}")
                elif isinstance(value, (list, tuple, set, frozenset)):
                    if any(isinstance(item, Actor) for item in value):
                        holders.append(f"{name}.{attribute}")
                elif isinstance(value, dict):
                    if any(isinstance(item, Actor) for item in value.values()):
                        holders.append(f"{name}.{attribute}")
        assert not holders, f"module-level actor state: {holders}"

    async def test_a_system_and_a_human_call_do_not_bleed(self, install):
        """The same overlap, across the two kinds: autonomous work and a
        person's request in flight together."""
        barrier = asyncio.Barrier(2)
        original = install.db.get_actor_ref

        async def _rendezvous(actor_id: str):
            row = await original(actor_id)
            await barrier.wait()
            return row

        install.db.get_actor_ref = _rendezvous
        try:
            async with _client(_app()) as client:
                human, machine = await asyncio.gather(
                    client.get("/api/whoami", headers=_bearer(install.session_token())),
                    client.get("/api/whoami", headers=_bearer(create_system_token(_SECRET))),
                )
        finally:
            install.db.get_actor_ref = original

        assert human.json()["actor_id"] == install.actor_id
        assert machine.json()["actor_id"] == install.identity.system_actor_id


# --------------------------------------------------------------------------- #
#  The WebSocket's actor is fixed at accept                                    #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestWebSocketActorIsFixedAtAccept:
    async def test_accept_resolves_once_and_records_it(self, install):
        socket = _Socket(token=install.session_token())
        connection = await _accept_websocket(socket)
        assert isinstance(connection, WebSocketConnection)
        assert socket.accepted and socket.closed is None
        assert connection.actor.account_id == install.account_id
        assert connection.client_id

    async def test_a_refused_socket_is_closed_and_yields_nothing(self, install):
        await install.db.set_account_enabled(install.account_id, False)
        socket = _Socket(token=install.session_token())
        assert await _accept_websocket(socket) is None
        assert socket.closed == (4001, "Unauthorized")

        no_credential = _Socket()
        assert await _accept_websocket(no_credential) is None
        assert no_credential.closed == (4001, "Unauthorized")

    async def test_the_actor_does_not_drift_when_the_account_changes(self, install):
        """A socket is open for hours. Disabling the account, renaming it, and
        adding a second one all leave this connection exactly as it was — and
        all take effect on the *next* connection."""
        connection = await _accept_websocket(_Socket(token=install.session_token()))
        before = connection.actor

        await install.db.update_actor_profile(install.actor_id, display_name="Renamed")
        await install.add_account("Bob")
        await install.db.set_account_enabled(install.account_id, False)

        assert connection.actor == before
        assert connection.actor.display_name == before.display_name
        # The next connection re-resolves, and is refused.
        assert await _accept_websocket(_Socket(token=install.session_token())) is None

    async def test_a_grandfathered_socket_keeps_its_actor_when_a_second_arrives(
        self, install,
    ):
        connection = await _accept_websocket(_Socket(token=install.legacy_token()))
        assert connection is not None
        assert connection.actor.account_id == install.account_id

        await install.add_account("Bob")

        assert connection.actor.account_id == install.account_id
        assert await _accept_websocket(_Socket(token=install.legacy_token())) is None

    async def test_the_record_cannot_be_rewritten(self, install):
        import dataclasses

        connection = await _accept_websocket(_Socket(token=install.session_token()))
        with pytest.raises(dataclasses.FrozenInstanceError):
            connection.actor = Actor(actor_id="someone-else", kind="human")
        with pytest.raises(dataclasses.FrozenInstanceError):
            connection.client_id = "hijacked"

    async def test_a_cookie_is_accepted_like_the_query_parameter(self, install):
        connection = await _accept_websocket(_Socket(cookie=install.session_token()))
        assert connection is not None
        assert connection.actor.account_id == install.account_id

    async def test_the_endpoint_authenticates_exactly_once(self):
        """Structural, because the drift the spec rules out is a *second*
        resolution mid-stream: the handler must have no way to re-authenticate
        after the accept helper returns."""
        from nerve.gateway import server as gw

        source = inspect.getsource(gw)
        assert source.count("await authenticate_websocket(") == 1
        assert source.count("await _accept_websocket(") == 1
        accept = inspect.getsource(gw._accept_websocket)
        assert "authenticate_websocket(" in accept


# --------------------------------------------------------------------------- #
#  Fail closed                                                                 #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestFailsClosedWithoutAStore:
    async def test_http_refuses_when_no_database_is_wired(self, monkeypatch):
        """Before the lifespan wires the database there is nothing to resolve
        a verified token against. A signature alone is not an identity."""
        from nerve.gateway.routes import _deps as deps_module

        set_config(NerveConfig(auth=AuthConfig(jwt_secret=_SECRET)))
        pin_jwt_secret(_SECRET)
        monkeypatch.setattr(deps_module, "_deps", None)
        try:
            assert identity_store() is None
            async with _client(_app()) as client:
                res = await client.get(
                    "/api/whoami",
                    headers=_bearer(create_session_token(_SECRET, "any-account")),
                )
            assert res.status_code == 503
            assert res.json()["detail"] == NO_IDENTITY_DETAIL
        finally:
            set_config(NerveConfig())

    async def test_the_websocket_refuses_when_no_database_is_wired(self, monkeypatch):
        from nerve.gateway.routes import _deps as deps_module

        set_config(NerveConfig(auth=AuthConfig(jwt_secret=_SECRET)))
        pin_jwt_secret(_SECRET)
        monkeypatch.setattr(deps_module, "_deps", None)
        try:
            socket = _Socket(token=create_session_token(_SECRET, "any-account"))
            assert await _accept_websocket(socket) is None
            assert socket.closed == (4001, "Unauthorized")
        finally:
            set_config(NerveConfig())

    async def test_a_deps_container_without_a_database_is_not_a_store(
        self, monkeypatch,
    ):
        from nerve.gateway.routes import _deps as deps_module

        monkeypatch.setattr(
            deps_module, "_deps", deps_module.RouteDeps(engine=None, db=None),
        )
        assert identity_store() is None

    async def test_an_unrecognised_token_shape_names_nobody(self, install):
        """A token signed by this instance but carrying a type nothing mints
        resolves to no actor rather than to a default one."""
        with pytest.raises(ActorResolutionError):
            await resolve_actor_from_claims(
                install.db, {"sub": install.account_id, TOKEN_TYPE_CLAIM: "invented"},
            )
        with pytest.raises(ActorResolutionError):
            await resolve_actor_from_claims(install.db, {})
        with pytest.raises(ActorResolutionError):
            await resolve_actor_from_claims(
                install.db, {"sub": None, TOKEN_TYPE_CLAIM: TOKEN_TYPE_SESSION},
            )
