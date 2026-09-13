"""The signing secret is pinned at startup and resolved through one seam.

Covers 1.6 of the local multi-user sequence: a configured ``auth.jwt_secret``
keeps being used, a generated one is honoured by every consumer, the old
``"dev-secret"`` login path — mint a token signed with a literal string and
skip the password check whenever the secret was empty — is gone, and there is
no unauthenticated mode left anywhere: with no secret in force HTTP, the
WebSocket handshake and the MCP endpoint all refuse. The secret is
restart-only, so a reload that removes or changes the key leaves the pinned
one in force. Everything else about the routes behaves as before.
"""

from __future__ import annotations

import warnings

import bcrypt
import jwt
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from nerve.config import AuthConfig, NerveConfig, set_config, workspace_settings_file
from nerve.gateway.auth import (
    JWT_ALGORITHM,
    NO_SECRET_DETAIL,
    TOKEN_TYPE_CLAIM,
    TOKEN_TYPE_SESSION,
    TOKEN_TYPE_SYSTEM,
    authenticate_websocket,
    create_session_token,
    create_system_token,
    effective_jwt_secret,
    pin_jwt_secret,
    pinned_jwt_secret,
    require_auth,
    unpin_jwt_secret,
)
from nerve.gateway.routes.auth import router as auth_router
from nerve.identity import Actor
from nerve.mcp_server.auth import McpAuthError, authenticate_mcp

_CONFIGURED = "configured-secret-padded-to-thirty-two-bytes!!"
_GENERATED = "generated-secret-padded-to-thirty-two-bytes!!"
# A secret nothing was ever signed with. Padded, like the two above, so PyJWT's
# short-HMAC-key warning does not fire on tokens that exist only to be refused.
_FORGED = "forged-secret-padded-to-thirty-two-bytes!!!!"
_PASSWORD = "correct horse battery staple"
_HASH = bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt(rounds=4)).decode()


@pytest.fixture
def config():
    """Install a config; tests set the fields they care about."""
    cfg = NerveConfig(auth=AuthConfig())
    set_config(cfg)
    yield cfg
    set_config(NerveConfig())


@pytest.fixture
def client(config, tmp_path, open_identity_db, wire_identity_store):
    """The gateway's auth surface over a real, bootstrapped database.

    A verified token still has to name an account, so these tests need the
    identity rows first start creates. The database is opened inside the
    client's own event loop (``portal``) so the app and the fixture share one.
    ``client.account_id`` / ``client.actor_id`` are the ids that were created.
    """
    app = FastAPI()
    app.include_router(auth_router)

    @app.get("/api/thing")
    async def thing(actor: Actor = Depends(require_auth)):
        return {"ok": True, "actor_id": actor.actor_id, "account_id": actor.account_id}

    with TestClient(app) as c:
        database, identity = c.portal.call(open_identity_db, tmp_path / "nerve.db")
        wire_identity_store(database)
        c.account_id = identity.owner_account_id
        c.actor_id = identity.owner_actor_id
        try:
            yield c
        finally:
            c.portal.call(database.close)


def _claims(token: str, secret: str) -> dict:
    return jwt.decode(token, secret, algorithms=[JWT_ALGORITHM])


def _bearer(secret: str, account_id: str) -> dict:
    return {"Authorization": f"Bearer {create_session_token(secret, account_id)}"}


class TestEffectiveSecret:
    def test_pinned_secret_wins_over_whatever_config_says_now(self, config):
        pin_jwt_secret(_GENERATED)
        config.auth.jwt_secret = _CONFIGURED  # as a reload could make it
        assert effective_jwt_secret(config) == _GENERATED
        assert effective_jwt_secret() == _GENERATED  # via get_config()

    def test_config_is_the_fallback_before_anything_is_pinned(self, config):
        """A CLI process, the installer, tests: nothing pinned yet."""
        config.auth.jwt_secret = _CONFIGURED
        assert effective_jwt_secret(config) == _CONFIGURED

    def test_empty_when_neither(self, config):
        assert effective_jwt_secret(config) == ""

    def test_the_first_pin_wins_for_the_life_of_the_process(self, config, caplog):
        pin_jwt_secret(_CONFIGURED)
        pin_jwt_secret(_CONFIGURED)  # the same value again is a no-op
        pin_jwt_secret(_GENERATED)   # a different one is refused, loudly
        assert pinned_jwt_secret() == _CONFIGURED
        assert any("restart-only" in r.getMessage() for r in caplog.records)
        pin_jwt_secret("")  # nothing is never pinned
        assert pinned_jwt_secret() == _CONFIGURED

    def test_the_pin_is_cleared_between_tests(self):
        """The conftest fixture: a previous test's pin must not leak."""
        assert pinned_jwt_secret() == ""


class TestFailClosedWithoutASecret:
    """No secret in force — nothing pinned, nothing configured — means every
    door is shut, locked or not. There is no dev mode to fall into."""

    @pytest.mark.parametrize("lockdown", [False, True], ids=["unlocked", "locked"])
    def test_http_refuses(self, client, config, lockdown):
        config.lockdown = lockdown
        res = client.get("/api/thing")
        assert res.status_code == 503
        assert res.json()["detail"] == NO_SECRET_DETAIL
        # A token signed with anything at all changes nothing.
        assert client.get(
            "/api/thing", headers=_bearer(_FORGED, client.account_id),
        ).status_code == 503

    def test_login_refuses_instead_of_minting_a_dev_secret_token(self, client, config):
        config.auth.password_hash = _HASH
        res = client.post("/api/auth/login", json={"password": _PASSWORD})
        assert res.status_code == 503
        assert "signing secret" in res.json()["detail"]
        config.auth.password_hash = ""
        assert client.post("/api/auth/login", json={"password": ""}).status_code == 503

    @pytest.mark.asyncio
    @pytest.mark.parametrize("lockdown", [False, True], ids=["unlocked", "locked"])
    async def test_websocket_refuses(self, config, lockdown):
        config.lockdown = lockdown
        assert await authenticate_websocket(_Socket()) is None
        forged = create_system_token(_FORGED)
        assert await authenticate_websocket(_Socket(token=forged)) is None

    @pytest.mark.parametrize("lockdown", [False, True], ids=["unlocked", "locked"])
    def test_mcp_refuses(self, config, lockdown):
        config.lockdown = lockdown
        with pytest.raises(McpAuthError, match="No signing secret"):
            authenticate_mcp(_scope(), config)
        with pytest.raises(McpAuthError, match="No signing secret"):
            authenticate_mcp(_scope(create_system_token(_FORGED)), config)

    def test_backend_token_minting_yields_nothing(self, config):
        from nerve.agent.engine import AgentEngine

        class _Engine:
            pass

        stub = _Engine()
        stub.config = config
        assert AgentEngine._mint_mcp_session_token(stub, "sess-1") == ""


class TestLoginRoute:
    def test_password_checked_and_token_signed_with_the_configured_secret(self, client, config):
        config.auth.password_hash = _HASH
        config.auth.jwt_secret = _CONFIGURED
        pin_jwt_secret(_CONFIGURED)  # what startup does with a configured secret
        assert client.post("/api/auth/login", json={"password": "wrong"}).status_code == 401
        res = client.post("/api/auth/login", json={"password": _PASSWORD})
        assert res.status_code == 200
        claims = _claims(res.json()["token"], _CONFIGURED)
        assert claims["sub"] == client.account_id
        assert claims[TOKEN_TYPE_CLAIM] == TOKEN_TYPE_SESSION
        assert client.get("/api/auth/status").json() == {"auth_required": True}

    def test_password_checked_and_token_signed_with_the_generated_secret(self, client, config):
        """An install that never configured auth.jwt_secret used to skip the
        password check entirely. With the generated secret pinned the
        password is verified and the token is signed with that secret."""
        config.auth.password_hash = _HASH
        pin_jwt_secret(_GENERATED)
        assert client.post("/api/auth/login", json={"password": "wrong"}).status_code == 401
        res = client.post("/api/auth/login", json={"password": _PASSWORD})
        assert res.status_code == 200
        token = res.json()["token"]
        assert _claims(token, _GENERATED)["sub"] == client.account_id
        # ...and specifically not with the literal the old code used. Decoding
        # with a ten-byte key trips PyJWT's key-length warning; that is the
        # point of the check, not noise worth surfacing in the run.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with pytest.raises(jwt.InvalidSignatureError):
                _claims(token, "dev-secret")
        assert client.get("/api/auth/status").json() == {"auth_required": True}

    def test_passwordless_admits_any_password_with_a_real_secret(self, client, config):
        """0.5 / 0.7: a passwordless install stays passwordless; every admitted
        caller resolves to the single account. The token is a real one."""
        pin_jwt_secret(_GENERATED)
        assert client.get("/api/auth/status").json() == {"auth_required": False}
        res = client.post("/api/auth/login", json={"password": "anything at all"})
        assert res.status_code == 200
        token = res.json()["token"]
        assert _claims(token, _GENERATED)["sub"] == client.account_id
        assert client.get(
            "/api/auth/check", headers={"Authorization": f"Bearer {token}"},
        ).json() == {"authenticated": True}

    def test_passwordless_with_a_configured_secret_is_unchanged(self, client, config):
        config.auth.jwt_secret = _CONFIGURED
        pin_jwt_secret(_CONFIGURED)
        res = client.post("/api/auth/login", json={"password": ""})
        assert res.status_code == 200
        assert _claims(res.json()["token"], _CONFIGURED)["sub"] == client.account_id


class TestRequireAuthWithAPinnedSecret:
    def test_the_pinned_secret_gates_requests(self, client, config):
        pin_jwt_secret(_GENERATED)
        assert client.get("/api/thing").status_code == 401
        res = client.get("/api/thing", headers=_bearer(_GENERATED, client.account_id))
        assert res.status_code == 200
        assert res.json()["actor_id"] == client.actor_id
        assert client.get(
            "/api/thing", headers=_bearer(_FORGED, client.account_id),
        ).status_code == 401

    def test_a_configured_secret_that_config_no_longer_shows_still_gates(self, client, config):
        """The config object is what a reload replaces; the pin is what the
        gateway checks against."""
        pin_jwt_secret(_CONFIGURED)
        config.auth.jwt_secret = ""  # the reloaded config dropped the key
        assert client.get(
            "/api/thing", headers=_bearer(_CONFIGURED, client.account_id),
        ).status_code == 200
        assert client.get("/api/thing").status_code == 401  # still not open


class _Socket:
    def __init__(self, token: str | None = None, cookie: str | None = None):
        self.query_params = {"token": token} if token else {}
        self.cookies = {"nerve_token": cookie} if cookie else {}


def _scope(token: str | None = None) -> dict:
    headers = [(b"authorization", f"Bearer {token}".encode())] if token else []
    return {"type": "http", "headers": headers, "query_string": b""}


@pytest.mark.asyncio
class TestWebSocketWithAPinnedSecret:
    async def test_the_pinned_secret_is_what_the_socket_checks(
        self, config, tmp_path, open_identity_db, wire_identity_store,
    ):
        pin_jwt_secret(_GENERATED)
        database, identity = await open_identity_db(tmp_path / "nerve.db")
        wire_identity_store(database)
        try:
            account_id = identity.owner_account_id
            good = create_session_token(_GENERATED, account_id)
            assert await authenticate_websocket(_Socket()) is None
            accepted = await authenticate_websocket(_Socket(token=good))
            assert accepted is not None and accepted.account_id == account_id
            by_cookie = await authenticate_websocket(_Socket(cookie=good))
            assert by_cookie == accepted
            forged = create_session_token(_FORGED, account_id)
            assert await authenticate_websocket(_Socket(token=forged)) is None
        finally:
            await database.close()


class TestMcpWithAPinnedSecret:
    def test_the_pinned_secret_is_enforced(self, config):
        pin_jwt_secret(_GENERATED)
        with pytest.raises(McpAuthError, match="Missing token"):
            authenticate_mcp(_scope(), config)
        with pytest.raises(McpAuthError):
            authenticate_mcp(_scope(create_system_token(_FORGED)), config)
        payload = authenticate_mcp(_scope(create_system_token(_GENERATED)), config)
        assert payload[TOKEN_TYPE_CLAIM] == TOKEN_TYPE_SYSTEM

    def test_engine_mints_backend_tokens_with_the_pinned_secret(self, config):
        from nerve.agent.engine import AgentEngine
        from nerve.gateway.auth import MCP_AUDIENCE, MCP_SESSION_CLAIM

        class _Engine:
            pass

        stub = _Engine()
        stub.config = config
        pin_jwt_secret(_GENERATED)
        token = AgentEngine._mint_mcp_session_token(stub, "sess-1")
        claims = jwt.decode(
            token, _GENERATED, algorithms=[JWT_ALGORITHM], audience=MCP_AUDIENCE,
        )
        assert claims[MCP_SESSION_CLAIM] == "sess-1"


# --------------------------------------------------------------------------- #
#  Reload cannot change or clear the secret                                    #
# --------------------------------------------------------------------------- #


def _write_install(tmp_path, local_yaml: str):
    config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
    config_dir.mkdir(exist_ok=True)
    (ws / "config").mkdir(parents=True, exist_ok=True)
    workspace_settings_file(ws).write_text("timezone: UTC\n", encoding="utf-8")
    (config_dir / "config.yaml").write_text(f"workspace: {ws}\n", encoding="utf-8")
    (config_dir / "config.local.yaml").write_text(local_yaml, encoding="utf-8")
    return config_dir


@pytest.mark.asyncio
class TestReloadKeepsThePinnedSecret:
    """A real ``reload_all`` against files on disk: the edit that removes or
    rotates ``auth.jwt_secret`` is reported as needing a restart and changes
    nothing about which secret is in force."""

    async def _start(self, tmp_path, open_identity_db, wire_identity_store):
        from nerve.config import load_config
        from nerve.migrate import ensure_jwt_secret

        config_dir = _write_install(tmp_path, f"auth:\n  jwt_secret: {_CONFIGURED}\n")
        config = load_config(config_dir)
        set_config(config)
        # What the lifespan does: bootstrap the local identity, then pin the
        # effective secret. Both go through the real functions.
        database, identity = await open_identity_db(tmp_path / "nerve.db")
        assert await ensure_jwt_secret(database, config) == _CONFIGURED
        wire_identity_store(database)
        assert pinned_jwt_secret() == _CONFIGURED
        return config_dir, database, identity.owner_account_id

    async def test_removing_the_key_does_not_reopen_the_gateway(
        self, tmp_path, open_identity_db, wire_identity_store,
    ):
        from nerve.config import get_config
        from nerve.config_reload import reload_all

        config_dir, database, account_id = await self._start(
            tmp_path, open_identity_db, wire_identity_store,
        )
        try:
            (config_dir / "config.local.yaml").write_text("{}\n", encoding="utf-8")

            summary = await reload_all(None, None, config_dir)

            assert summary["config"] == "reloaded"
            assert "auth.jwt_secret" in summary["restart_required"]
            assert get_config().auth.jwt_secret == ""      # the config object did change
            assert effective_jwt_secret() == _CONFIGURED    # the secret in force did not

            app = FastAPI()

            @app.get("/api/thing")
            async def thing(actor: Actor = Depends(require_auth)):
                return {"ok": True}

            with TestClient(app) as client:
                assert client.get("/api/thing").status_code == 401  # not open
                assert client.get(
                    "/api/thing", headers=_bearer(_CONFIGURED, account_id),
                ).status_code == 200
            live = create_session_token(_CONFIGURED, account_id)
            assert await authenticate_websocket(_Socket(token=live)) is not None
            assert await authenticate_websocket(_Socket()) is None
            assert authenticate_mcp(
                _scope(create_system_token(_CONFIGURED)), get_config(),
            )[TOKEN_TYPE_CLAIM] == TOKEN_TYPE_SYSTEM
            with pytest.raises(McpAuthError):
                authenticate_mcp(_scope(), get_config())
        finally:
            await database.close()
        set_config(NerveConfig())

    async def test_rotating_the_key_waits_for_a_restart(
        self, tmp_path, open_identity_db, wire_identity_store,
    ):
        from nerve.config import get_config
        from nerve.config_reload import reload_all

        config_dir, database, account_id = await self._start(
            tmp_path, open_identity_db, wire_identity_store,
        )
        try:
            (config_dir / "config.local.yaml").write_text(
                f"auth:\n  jwt_secret: {_GENERATED}\n", encoding="utf-8",
            )

            summary = await reload_all(None, None, config_dir)

            assert "auth.jwt_secret" in summary["restart_required"]
            assert get_config().auth.jwt_secret == _GENERATED
            assert effective_jwt_secret() == _CONFIGURED
            app = FastAPI()

            @app.get("/api/thing")
            async def thing(actor: Actor = Depends(require_auth)):
                return {"ok": True}

            with TestClient(app) as client:
                assert client.get(
                    "/api/thing", headers=_bearer(_CONFIGURED, account_id),
                ).status_code == 200
                assert client.get(
                    "/api/thing", headers=_bearer(_GENERATED, account_id),
                ).status_code == 401

            # "Restart": the pin is gone, startup pins the new configured value.
            unpin_jwt_secret()
            from nerve.migrate import ensure_jwt_secret

            assert await ensure_jwt_secret(database, get_config()) == _GENERATED
            assert effective_jwt_secret() == _GENERATED
        finally:
            await database.close()
        set_config(NerveConfig())
