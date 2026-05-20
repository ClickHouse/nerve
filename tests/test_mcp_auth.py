"""Tests for the external MCP auth wrapper.

Exercises :func:`nerve.mcp_server.auth.authenticate_mcp` directly: it
should pass valid bearer tokens, reject missing/invalid ones, accept
the ``?token=`` query-param form, and bypass entirely when no
``jwt_secret`` is configured (dev mode).
"""

from __future__ import annotations

import pytest

from nerve.config import AuthConfig, NerveConfig
from nerve.gateway.auth import create_token
from nerve.mcp_server.auth import McpAuthError, authenticate_mcp


_JWT_SECRET = "test-secret-for-mcp-auth"


def _scope(
    *,
    headers: list[tuple[bytes, bytes]] | None = None,
    query_string: bytes = b"",
) -> dict:
    return {
        "type": "http",
        "headers": headers or [],
        "query_string": query_string,
    }


@pytest.fixture
def auth_config() -> NerveConfig:
    return NerveConfig(auth=AuthConfig(jwt_secret=_JWT_SECRET))


@pytest.fixture
def dev_config() -> NerveConfig:
    return NerveConfig(auth=AuthConfig(jwt_secret=""))


def test_dev_mode_bypasses_auth(dev_config):
    # No headers, no query — dev mode should still pass.
    result = authenticate_mcp(_scope(), dev_config)
    assert result is None


def test_missing_token_raises(auth_config):
    with pytest.raises(McpAuthError, match="Missing token"):
        authenticate_mcp(_scope(), auth_config)


def test_invalid_bearer_token_raises(auth_config):
    headers = [(b"authorization", b"Bearer not-a-real-jwt")]
    with pytest.raises(McpAuthError):
        authenticate_mcp(_scope(headers=headers), auth_config)


def test_valid_bearer_token_passes(auth_config):
    token = create_token(_JWT_SECRET)
    headers = [(b"authorization", f"Bearer {token}".encode("ascii"))]
    payload = authenticate_mcp(_scope(headers=headers), auth_config)
    assert payload is not None
    assert payload["sub"] == "user"


def test_query_param_token_passes(auth_config):
    token = create_token(_JWT_SECRET)
    qs = f"token={token}".encode("ascii")
    payload = authenticate_mcp(_scope(query_string=qs), auth_config)
    assert payload is not None


def test_invalid_query_param_token_raises(auth_config):
    qs = b"token=bogus"
    with pytest.raises(McpAuthError):
        authenticate_mcp(_scope(query_string=qs), auth_config)


def test_authorization_header_case_insensitive(auth_config):
    """Header names should be matched case-insensitively per HTTP spec."""
    token = create_token(_JWT_SECRET)
    headers = [(b"Authorization", f"Bearer {token}".encode("ascii"))]
    payload = authenticate_mcp(_scope(headers=headers), auth_config)
    assert payload is not None


def test_header_takes_precedence_over_query(auth_config):
    """If both header and query are supplied, the header wins (and the
    bogus query is never consulted)."""
    token = create_token(_JWT_SECRET)
    headers = [(b"authorization", f"Bearer {token}".encode("ascii"))]
    qs = b"token=bogus"
    payload = authenticate_mcp(_scope(headers=headers, query_string=qs), auth_config)
    assert payload is not None
