"""Session-bound MCP tokens: minting, decoding, and ctx binding.

Backend-managed agent subprocesses (codex) reach nerve tools over the
gateway's Streamable HTTP MCP endpoint with a token carrying
``aud=nerve-mcp`` + ``nerve_session_id``; their tool calls must bind to
the REAL engine session, while ordinary tokens keep satellite
attribution and web-UI auth stays closed to scoped tokens.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from nerve.gateway.auth import (
    MCP_AUDIENCE,
    MCP_SESSION_CLAIM,
    MCP_WORKER_CLAIM,
    TOKEN_TYPE_CLAIM,
    TOKEN_TYPE_SESSION,
    TOKEN_TYPE_SYSTEM,
    create_mcp_session_token,
    create_session_token,
    decode_token,
)
from nerve.mcp_server.auth import (
    McpAuthError,
    authenticate_mcp,
    bound_session_id,
    decode_mcp_token,
)

# 32+ bytes so PyJWT's short-key warning stays out of the suite's output.
SECRET = "test-secret-for-mcp-binding-padded-to-32b"
# A person's account id, as a session token would carry it.
ACCOUNT = "55555555-5555-4555-8555-555555555555"


def _scope(token: str | None) -> dict:
    headers = []
    if token is not None:
        headers.append((b"authorization", f"Bearer {token}".encode()))
    return {"type": "http", "headers": headers, "query_string": b""}


class TestTokenShapes:
    def test_session_token_carries_claims_and_short_expiry(self):
        token = create_mcp_session_token(SECRET, "sess-42")
        payload = decode_mcp_token(token, SECRET)
        assert payload["aud"] == MCP_AUDIENCE
        assert payload[MCP_SESSION_CLAIM] == "sess-42"
        assert payload["exp"] - payload["iat"] == 8 * 60 * 60
        assert payload["jti"]

    def test_plain_gateway_token_still_decodes(self):
        token = create_session_token(SECRET, ACCOUNT)
        payload = decode_mcp_token(token, SECRET)
        assert payload["sub"] == ACCOUNT
        assert payload[TOKEN_TYPE_CLAIM] == TOKEN_TYPE_SESSION
        assert bound_session_id(payload) is None  # satellite attribution

    def test_mcp_tokens_say_what_they_are(self):
        """Every token this version mints carries ``typ``. The MCP shapes are
        the instance acting on its own behalf, so they say ``system`` — which
        is also why they never slide."""
        bound = decode_mcp_token(create_mcp_session_token(SECRET, "sess-42"), SECRET)
        assert bound[TOKEN_TYPE_CLAIM] == TOKEN_TYPE_SYSTEM
        from nerve.gateway.auth import create_external_mcp_token

        external = decode_mcp_token(create_external_mcp_token(SECRET), SECRET)
        assert external[TOKEN_TYPE_CLAIM] == TOKEN_TYPE_SYSTEM
        assert MCP_SESSION_CLAIM not in external

    def test_scoped_token_rejected_by_ordinary_web_auth(self):
        """A session-bound token must never pass the web-UI decode path —
        PyJWT rejects aud-carrying tokens unless the audience is requested."""
        token = create_mcp_session_token(SECRET, "sess-42")
        with pytest.raises(HTTPException):
            decode_token(token, SECRET)

    def test_wrong_secret_rejected(self):
        token = create_mcp_session_token(SECRET, "sess-42")
        with pytest.raises(HTTPException):
            decode_mcp_token(token, "another-secret-padded-to-32-bytes!!!")

    def test_bound_session_id_requires_audience(self):
        """The binding is read from the audience and the session claim — never
        from the subject, which is an account id now and said nothing about
        binding even when it was the string ``user``."""
        assert bound_session_id(None) is None
        assert bound_session_id(
            {"sub": ACCOUNT, TOKEN_TYPE_CLAIM: TOKEN_TYPE_SESSION},
        ) is None
        assert bound_session_id({"sub": "user"}) is None  # the legacy shape
        assert bound_session_id({
            "aud": MCP_AUDIENCE, MCP_SESSION_CLAIM: "s9",
        }) == "s9"
        assert bound_session_id({"aud": MCP_AUDIENCE}) is None


class TestAuthenticateMcp:
    def _config(self, tmp_path, secret: str):
        from nerve.config import NerveConfig
        cfg = NerveConfig.from_dict({"workspace": str(tmp_path)})
        cfg.auth.jwt_secret = secret
        return cfg

    def test_accepts_both_token_shapes(self, tmp_path):
        cfg = self._config(tmp_path, SECRET)
        plain = authenticate_mcp(_scope(create_session_token(SECRET, ACCOUNT)), cfg)
        assert plain["sub"] == ACCOUNT
        scoped = authenticate_mcp(
            _scope(create_mcp_session_token(SECRET, "sess-1")), cfg,
        )
        assert scoped[MCP_SESSION_CLAIM] == "sess-1"

    def test_missing_and_garbage_tokens_rejected(self, tmp_path):
        cfg = self._config(tmp_path, SECRET)
        with pytest.raises(McpAuthError):
            authenticate_mcp(_scope(None), cfg)
        with pytest.raises(McpAuthError):
            authenticate_mcp(_scope("garbage"), cfg)

    def test_no_secret_in_force_fails_closed(self, tmp_path):
        """There is no dev mode: nothing configured and nothing pinned means
        the endpoint refuses, with or without a token."""
        cfg = self._config(tmp_path, "")
        with pytest.raises(McpAuthError, match="No signing secret"):
            authenticate_mcp(_scope(None), cfg)
        with pytest.raises(McpAuthError, match="No signing secret"):
            authenticate_mcp(_scope("garbage"), cfg)


class TestCtxBinding:
    @pytest.mark.asyncio
    async def test_bound_token_binds_real_session(self, tmp_path, monkeypatch):
        """A request carrying the session claim resolves ToolContext to the
        engine session; without it the satellite resolver is used."""
        from types import SimpleNamespace

        from nerve.mcp_server import http as mcp_http
        from nerve.config import NerveConfig

        cfg = NerveConfig.from_dict({"workspace": str(tmp_path)})
        cfg.auth.jwt_secret = SECRET

        token = create_mcp_session_token(SECRET, "engine-sess-7")

        class _Headers(dict):
            def get(self, key, default=None):
                return super().get(key.lower(), default)

        fake_request = SimpleNamespace(
            headers=_Headers({"authorization": f"Bearer {token}"}),
            query_params={},
        )
        fake_rctx = SimpleNamespace(request=fake_request, session=None)
        assert mcp_http._bound_session_from_request(cfg, fake_rctx) == "engine-sess-7"

        # Plain token → no binding (satellite path).
        fake_request.headers = _Headers(
            {"authorization": f"Bearer {create_session_token(SECRET, ACCOUNT)}"},
        )
        assert mcp_http._bound_session_from_request(cfg, fake_rctx) is None

        # No request context → no binding.
        assert mcp_http._bound_session_from_request(cfg, None) is None

    @pytest.mark.asyncio
    async def test_worker_token_adds_runtime_attribution(self, tmp_path):
        from types import SimpleNamespace

        from nerve.config import NerveConfig
        from nerve.mcp_server import http as mcp_http

        cfg = NerveConfig.from_dict({"workspace": str(tmp_path)})
        cfg.auth.jwt_secret = SECRET
        worker_id = "ultracode-0123456789abcdef"
        token = create_mcp_session_token(
            SECRET, "engine-sess-8", worker_id=worker_id,
        )

        class _Headers(dict):
            def get(self, key, default=None):
                return super().get(key.lower(), default)

        fake_request = SimpleNamespace(
            headers=_Headers({"authorization": f"Bearer {token}"}),
            query_params={},
        )
        session_id, runtime = mcp_http._bound_identity_from_request(
            cfg, SimpleNamespace(request=fake_request, session=None),
        )
        assert session_id == "engine-sess-8"
        assert runtime == {"worker_id": worker_id, "runtime": "ultracode"}
        payload = decode_mcp_token(token, SECRET)
        assert payload[MCP_WORKER_CLAIM] == worker_id

    @pytest.mark.asyncio
    async def test_dev_mode_never_binds(self, tmp_path):
        from nerve.mcp_server import http as mcp_http
        from nerve.config import NerveConfig

        cfg = NerveConfig.from_dict({"workspace": str(tmp_path)})
        cfg.auth.jwt_secret = ""
        assert mcp_http._bound_session_from_request(cfg, None) is None
