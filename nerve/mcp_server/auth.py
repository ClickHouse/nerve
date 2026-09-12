"""Authenticate external MCP requests against the gateway JWT.

The external MCP endpoint reuses Nerve's existing JWT secret (see
:func:`nerve.gateway.auth.effective_jwt_secret`) and the same token
mechanism the web UI uses — no separate credential store, no per-client
token table. A client (Codex, Claude Code, etc.) presents the JWT it
received from ``POST /api/auth/login`` either as an
``Authorization: Bearer <jwt>`` header or as a ``?token=<jwt>`` query
parameter.

There is no unauthenticated mode. The secret is pinned at startup by the
identity bootstrap (a configured ``auth.jwt_secret``, else one generated into
the database); until that has happened no secret is in force and every request
is refused, the same way the web gateway refuses.
"""

from __future__ import annotations

import logging
from urllib.parse import parse_qs

from fastapi import HTTPException
from starlette.types import Scope

from nerve.config import NerveConfig
from nerve.gateway.auth import (
    MCP_AUDIENCE,
    MCP_SESSION_CLAIM,
    NO_SECRET_DETAIL,
    decode_token,
    effective_jwt_secret,
)

logger = logging.getLogger(__name__)


class McpAuthError(Exception):
    """Raised when MCP authentication fails.

    Distinct from :class:`HTTPException` so the caller decides whether
    to send a 401 ASGI response or wrap differently.
    """


def _extract_token_from_scope(scope: Scope) -> str:
    """Pull the JWT out of an ASGI scope (Authorization header or query)."""
    # Authorization header (case-insensitive search across the raw header list)
    for raw_name, raw_value in scope.get("headers", []):
        if raw_name.lower() == b"authorization":
            value = raw_value.decode("latin-1")
            if value.lower().startswith("bearer "):
                return value[7:].strip()
            return value.strip()

    # ?token= query parameter — Codex CLI's HTTP MCP config supports this
    # via the URL itself, no header munging required.
    qs = scope.get("query_string", b"").decode("latin-1")
    if qs:
        token = parse_qs(qs).get("token", [""])[0]
        if token:
            return token

    return ""


def decode_mcp_token(token: str, jwt_secret: str) -> dict:
    """Decode a token for the MCP endpoint.

    Two token shapes are accepted:

    * ordinary gateway tokens (no ``aud``) — external clients (Codex CLI,
      Claude Code) that logged in via the web-UI flow; their tool calls
      attribute to satellite sessions.
    * session-bound tokens (``aud=nerve-mcp`` + ``nerve_session_id``) —
      minted by :func:`nerve.gateway.auth.create_mcp_session_token` for
      backend-managed agent subprocesses; their tool calls bind to the
      real engine session.

    PyJWT rejects an ``aud``-carrying token unless the audience is
    requested, and rejects an aud-less token when one is — hence the
    two-step decode.
    """
    try:
        return decode_token(token, jwt_secret)
    except HTTPException:
        pass
    # Not a plain token — try the MCP-audience shape (raises on failure).
    return decode_token(token, jwt_secret, audience=MCP_AUDIENCE)


def authenticate_mcp(scope: Scope, config: NerveConfig) -> dict:
    """Validate the JWT on an incoming MCP request.

    Returns the decoded JWT payload. Raises :class:`McpAuthError` on a
    missing or invalid token — and when no signing secret is in force at
    all, which only happens before startup has pinned one: the endpoint
    fails closed rather than open, like ``require_auth``.
    """
    secret = effective_jwt_secret(config)
    if not secret:
        raise McpAuthError(NO_SECRET_DETAIL)

    token = _extract_token_from_scope(scope)
    if not token:
        raise McpAuthError("Missing token")

    try:
        return decode_mcp_token(token, secret)
    except HTTPException as e:
        # decode_token raises FastAPI HTTPException; translate so the
        # caller doesn't need to import fastapi.
        raise McpAuthError(e.detail or "Invalid token")


def bound_session_id(payload: dict | None) -> str | None:
    """The engine session a decoded MCP token is bound to (or ``None``).

    Read from the audience and the session claim, never from the subject: a
    web session's ``sub`` is an account id and said nothing about binding even
    back when it was the fixed string ``user``. Only ``aud=nerve-mcp`` tokens
    carry the claim; a session token, a system token and a ``None`` payload all
    return ``None`` → satellite attribution.
    """
    if not payload:
        return None
    if payload.get("aud") != MCP_AUDIENCE:
        return None
    session_id = payload.get(MCP_SESSION_CLAIM)
    return str(session_id) if session_id else None
