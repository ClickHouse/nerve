"""Authenticate external MCP requests against the gateway JWT.

The external MCP endpoint reuses Nerve's existing JWT secret
(``config.auth.jwt_secret``) and the same token mechanism the web UI
uses — no separate credential store, no per-client token table. A
client (Codex, Claude Code, etc.) presents the JWT it received from
``POST /api/auth/login`` either as an ``Authorization: Bearer <jwt>``
header or as a ``?token=<jwt>`` query parameter.

When ``config.auth.jwt_secret`` is empty (Nerve's "dev mode") the
gateway accepts all requests without auth; this module mirrors that
behaviour so a fresh install can be smoke-tested locally without
fiddling with credentials.
"""

from __future__ import annotations

import logging
from urllib.parse import parse_qs

from fastapi import HTTPException
from starlette.types import Scope

from nerve.config import NerveConfig
from nerve.gateway.auth import decode_token

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


def authenticate_mcp(scope: Scope, config: NerveConfig) -> dict | None:
    """Validate the JWT on an incoming MCP request.

    Returns the decoded JWT payload on success, ``None`` in dev mode
    (no jwt_secret configured), and raises :class:`McpAuthError` on
    auth failure.
    """
    if not config.auth.jwt_secret:
        # Dev mode — matches gateway.auth.require_auth's bypass.
        return None

    token = _extract_token_from_scope(scope)
    if not token:
        raise McpAuthError("Missing token")

    try:
        return decode_token(token, config.auth.jwt_secret)
    except HTTPException as e:
        # decode_token raises FastAPI HTTPException; translate so the
        # caller doesn't need to import fastapi.
        raise McpAuthError(e.detail or "Invalid token")
