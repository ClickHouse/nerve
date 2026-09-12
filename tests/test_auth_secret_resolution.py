"""The signing secret is resolved, not read: ``auth.jwt_secret`` when set,
else the database-held secret the bootstrap published to the process.

Covers 1.6 of the local multi-user sequence: an existing secret keeps being
used, a generated one is honoured by every consumer, and the old
``"dev-secret"`` login path — mint a token signed with a literal string and
skip the password check whenever the secret was empty — is gone. Everything
else about the routes behaves as before.
"""

from __future__ import annotations

import bcrypt
import jwt
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from nerve.config import AuthConfig, NerveConfig, set_config
from nerve.gateway.auth import (
    JWT_ALGORITHM,
    authenticate_websocket,
    create_token,
    effective_jwt_secret,
    require_auth,
    set_stored_jwt_secret,
    stored_jwt_secret,
)
from nerve.gateway.routes.auth import router as auth_router
from nerve.mcp_server.auth import McpAuthError, authenticate_mcp

_CONFIGURED = "configured-secret-padded-to-thirty-two-bytes!!"
_STORED = "stored-secret-padded-to-thirty-two-bytes!!!!"
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
def client(config):
    app = FastAPI()
    app.include_router(auth_router)

    @app.get("/api/thing")
    async def thing(user: dict = Depends(require_auth)):
        return {"ok": True}

    with TestClient(app) as c:
        yield c


def _claims(token: str, secret: str) -> dict:
    return jwt.decode(token, secret, algorithms=[JWT_ALGORITHM])


class TestEffectiveSecret:
    def test_configured_secret_wins(self, config):
        config.auth.jwt_secret = _CONFIGURED
        set_stored_jwt_secret(_STORED)
        assert effective_jwt_secret(config) == _CONFIGURED
        assert effective_jwt_secret() == _CONFIGURED  # via get_config()

    def test_stored_secret_when_config_has_none(self, config):
        set_stored_jwt_secret(_STORED)
        assert effective_jwt_secret(config) == _STORED
        assert stored_jwt_secret() == _STORED

    def test_empty_when_neither(self, config):
        assert effective_jwt_secret(config) == ""

    def test_holder_is_reset_between_tests(self):
        """The conftest fixture: a previous test's stored secret must not leak."""
        assert stored_jwt_secret() == ""


class TestLoginRoute:
    def test_password_checked_and_token_signed_with_configured_secret(self, client, config):
        config.auth.password_hash = _HASH
        config.auth.jwt_secret = _CONFIGURED
        assert client.post("/api/auth/login", json={"password": "wrong"}).status_code == 401
        res = client.post("/api/auth/login", json={"password": _PASSWORD})
        assert res.status_code == 200
        assert _claims(res.json()["token"], _CONFIGURED)["sub"] == "user"
        assert client.get("/api/auth/status").json() == {"auth_required": True}

    def test_password_checked_and_token_signed_with_stored_secret(self, client, config):
        """An install that never configured auth.jwt_secret used to skip the
        password check entirely. With the generated secret in force the
        password is verified and the token is signed with that secret."""
        config.auth.password_hash = _HASH
        set_stored_jwt_secret(_STORED)
        assert client.post("/api/auth/login", json={"password": "wrong"}).status_code == 401
        res = client.post("/api/auth/login", json={"password": _PASSWORD})
        assert res.status_code == 200
        token = res.json()["token"]
        assert _claims(token, _STORED)["sub"] == "user"
        with pytest.raises(jwt.InvalidSignatureError):
            _claims(token, "dev-secret")
        assert client.get("/api/auth/status").json() == {"auth_required": True}

    def test_no_secret_anywhere_refuses_instead_of_dev_secret(self, client, config):
        """The dead path: no secret in config, none stored. It used to answer
        with a token signed by the literal string "dev-secret"."""
        config.auth.password_hash = _HASH
        res = client.post("/api/auth/login", json={"password": _PASSWORD})
        assert res.status_code == 503
        assert "signing secret" in res.json()["detail"]
        # ...and the same for a passwordless install.
        config.auth.password_hash = ""
        assert client.post("/api/auth/login", json={"password": ""}).status_code == 503

    def test_passwordless_admits_any_password_with_a_real_secret(self, client, config):
        """0.5 / 0.7: a passwordless install stays passwordless; every admitted
        caller resolves to the single account. The token is a real one."""
        set_stored_jwt_secret(_STORED)
        assert client.get("/api/auth/status").json() == {"auth_required": False}
        res = client.post("/api/auth/login", json={"password": "anything at all"})
        assert res.status_code == 200
        token = res.json()["token"]
        assert _claims(token, _STORED)["sub"] == "user"
        assert client.get(
            "/api/auth/check", headers={"Authorization": f"Bearer {token}"},
        ).json() == {"authenticated": True}

    def test_passwordless_with_configured_secret_is_unchanged(self, client, config):
        config.auth.jwt_secret = _CONFIGURED
        res = client.post("/api/auth/login", json={"password": ""})
        assert res.status_code == 200
        assert _claims(res.json()["token"], _CONFIGURED)["sub"] == "user"


class TestRequireAuthWithStoredSecret:
    def test_stored_secret_gates_requests(self, client, config):
        set_stored_jwt_secret(_STORED)
        assert client.get("/api/thing").status_code == 401
        good = create_token(_STORED)
        assert client.get(
            "/api/thing", headers={"Authorization": f"Bearer {good}"},
        ).status_code == 200
        forged = create_token("dev-secret")
        assert client.get(
            "/api/thing", headers={"Authorization": f"Bearer {forged}"},
        ).status_code == 401

    def test_configured_secret_still_wins_over_stored(self, client, config):
        config.auth.jwt_secret = _CONFIGURED
        set_stored_jwt_secret(_STORED)
        assert client.get(
            "/api/thing", headers={"Authorization": f"Bearer {create_token(_CONFIGURED)}"},
        ).status_code == 200
        assert client.get(
            "/api/thing", headers={"Authorization": f"Bearer {create_token(_STORED)}"},
        ).status_code == 401

    def test_no_secret_anywhere_keeps_the_pre_bootstrap_open_path(self, client, config):
        """Unreachable in a served instance (bootstrap always leaves a secret);
        kept so the harness tests that never bootstrap keep working."""
        assert client.get("/api/thing").status_code == 200

    def test_locked_instance_with_no_secret_anywhere_fails_closed(self, client, config):
        config.lockdown = True
        assert client.get("/api/thing").status_code == 503


class _Socket:
    def __init__(self, token: str | None = None, cookie: str | None = None):
        self.query_params = {"token": token} if token else {}
        self.cookies = {"nerve_token": cookie} if cookie else {}


@pytest.mark.asyncio
class TestWebSocketWithStoredSecret:
    async def test_stored_secret_is_what_the_socket_checks(self, config):
        set_stored_jwt_secret(_STORED)
        assert await authenticate_websocket(_Socket()) is False
        assert await authenticate_websocket(_Socket(token=create_token(_STORED))) is True
        assert await authenticate_websocket(_Socket(cookie=create_token(_STORED))) is True
        assert await authenticate_websocket(_Socket(token=create_token("dev-secret"))) is False


class TestMcpWithStoredSecret:
    def _scope(self, token: str | None = None) -> dict:
        headers = [(b"authorization", f"Bearer {token}".encode())] if token else []
        return {"type": "http", "headers": headers, "query_string": b""}

    def test_stored_secret_is_enforced(self, config):
        set_stored_jwt_secret(_STORED)
        with pytest.raises(McpAuthError, match="Missing token"):
            authenticate_mcp(self._scope(), config)
        with pytest.raises(McpAuthError):
            authenticate_mcp(self._scope(create_token("dev-secret")), config)
        payload = authenticate_mcp(self._scope(create_token(_STORED)), config)
        assert payload["sub"] == "user"

    def test_engine_mints_backend_tokens_with_the_stored_secret(self, config):
        from nerve.agent.engine import AgentEngine
        from nerve.gateway.auth import MCP_AUDIENCE, MCP_SESSION_CLAIM

        class _Engine:
            pass

        stub = _Engine()
        stub.config = config
        # No secret anywhere: no token (the endpoint runs open pre-bootstrap).
        assert AgentEngine._mint_mcp_session_token(stub, "sess-1") == ""
        set_stored_jwt_secret(_STORED)
        token = AgentEngine._mint_mcp_session_token(stub, "sess-1")
        claims = jwt.decode(
            token, _STORED, algorithms=[JWT_ALGORITHM], audience=MCP_AUDIENCE,
        )
        assert claims[MCP_SESSION_CLAIM] == "sess-1"
