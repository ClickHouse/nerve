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
    create_mcp_session_token,
    create_token,
    decode_token,
)
from nerve.mcp_server.auth import (
    McpAuthError,
    authenticate_mcp,
    bound_session_id,
    decode_mcp_token,
)

SECRET = "test-secret-1234"


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
        token = create_token(SECRET)
        payload = decode_mcp_token(token, SECRET)
        assert payload["sub"] == "user"
        assert bound_session_id(payload) is None  # satellite attribution

    def test_scoped_token_rejected_by_ordinary_web_auth(self):
        """A session-bound token must never pass the web-UI decode path —
        PyJWT rejects aud-carrying tokens unless the audience is requested."""
        token = create_mcp_session_token(SECRET, "sess-42")
        with pytest.raises(HTTPException):
            decode_token(token, SECRET)

    def test_wrong_secret_rejected(self):
        token = create_mcp_session_token(SECRET, "sess-42")
        with pytest.raises(HTTPException):
            decode_mcp_token(token, "other-secret")

    def test_bound_session_id_requires_audience(self):
        assert bound_session_id(None) is None
        assert bound_session_id({"sub": "user"}) is None
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
        plain = authenticate_mcp(_scope(create_token(SECRET)), cfg)
        assert plain["sub"] == "user"
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

    def test_dev_mode_bypass(self, tmp_path):
        cfg = self._config(tmp_path, "")
        assert authenticate_mcp(_scope(None), cfg) is None


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
        cv_token = mcp_http.request_ctx.set(fake_rctx)
        try:
            assert mcp_http._bound_session_from_request(cfg) == "engine-sess-7"

            # Plain token → no binding (satellite path).
            fake_request.headers = _Headers(
                {"authorization": f"Bearer {create_token(SECRET)}"},
            )
            assert mcp_http._bound_session_from_request(cfg) is None
        finally:
            mcp_http.request_ctx.reset(cv_token)

        # No request context set → no binding.
        assert mcp_http._bound_session_from_request(cfg) is None

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
        cv_token = mcp_http.request_ctx.set(
            SimpleNamespace(request=fake_request, session=None),
        )
        try:
            session_id, runtime = mcp_http._bound_identity_from_request(cfg)
        finally:
            mcp_http.request_ctx.reset(cv_token)
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
        assert mcp_http._bound_session_from_request(cfg) is None
