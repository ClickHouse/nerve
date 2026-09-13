"""JWT authentication for the gateway.

Password login, HS256 session tokens, bcrypt hashing.

**The signing secret.** Tokens are signed with :func:`effective_jwt_secret`,
which returns the secret *pinned* at startup by the identity bootstrap:
``auth.jwt_secret`` when configuration supplied one, otherwise the secret the
bootstrap generated on first start and keeps in the database
(``instance_secrets``). Every consumer — this module, the login route, the
external MCP endpoint, the CLI — must go through that function rather than
read ``config.auth.jwt_secret`` directly: the config object is rebuilt on every
reload, and the secret is restart-only. With no secret in force every check
fails closed; there is no unauthenticated mode.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import bcrypt
import jwt
from fastapi import Depends, HTTPException, Request, WebSocket

from nerve.config import NerveConfig, get_config

logger = logging.getLogger(__name__)

JWT_ALGORITHM = "HS256"

# Fallback web-session lifetime, used only when the config can't be read.
# The real value is ``auth.jwt_expiry_hours`` (default 720h / 30 days).
#
# Session tokens *slide*: ``require_auth`` re-mints one that is past
# REFRESH_AFTER_RATIO of its lifetime and the gateway hands the fresh token
# back on the response, so continuous use never expires. The configured
# window is therefore an idle timeout — the old fixed 24h constant logged
# you out mid-work exactly one day after login no matter what you were doing.
DEFAULT_JWT_EXPIRY_HOURS = 720

# Re-mint once a token is this far into its lifetime. At 0.5 an active
# session is refreshed about every half-window (so it never dies), while a
# fresh token costs no crypto on the vast majority of requests.
REFRESH_AFTER_RATIO = 0.5

# Response header carrying a slid session token back to the browser.
SESSION_TOKEN_HEADER = "X-Nerve-Token"

# What every fail-closed check says when no signing secret is in force. Shared
# so the HTTP, MCP and worker-token paths agree, and so a test can match it.
NO_SECRET_DETAIL = "No signing secret is in force; the gateway has not completed startup"

# Audience claim on session-bound MCP tokens (see create_mcp_session_token).
MCP_AUDIENCE = "nerve-mcp"
# Claim carrying the bound nerve session id on MCP tokens.
MCP_SESSION_CLAIM = "nerve_session_id"
MCP_WORKER_CLAIM = "nerve_worker_id"


def verify_password(plain: str, hashed: str) -> bool:
    """Verify a plaintext password against a bcrypt hash."""
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


def session_expiry_hours() -> int:
    """Configured web-session lifetime, in hours (never below 1)."""
    try:
        hours = int(get_config().auth.jwt_expiry_hours)
    except Exception:  # config unreadable (very early boot / tests)
        hours = DEFAULT_JWT_EXPIRY_HOURS
    return max(1, hours)


# The signing secret in force for this process. Fixed once at startup by the
# identity bootstrap (nerve.migrate.ensure_jwt_secret): auth.jwt_secret when
# configuration supplied one, else the secret kept in the database. Pinned here
# rather than read from the config object per request because the secret is
# restart-only: a reload rebuilds that object from disk, and an edit that
# removed or changed the key must neither reopen the instance nor swap the key
# under live sessions. `restart_required` reports such a change; the next
# restart applies it.
_pinned_jwt_secret: str = ""


def pin_jwt_secret(secret: str) -> None:
    """Fix the signing secret for the rest of this process's life.

    The first pin wins. A later call offering a *different* value is ignored
    with a warning rather than honoured, because a swap after startup is
    exactly what pinning exists to rule out; the same value is a no-op, which
    is what the CLI pass followed by the gateway's own bootstrap produces.
    """
    global _pinned_jwt_secret
    secret = secret or ""
    if not secret:
        return
    if _pinned_jwt_secret and _pinned_jwt_secret != secret:
        logger.warning(
            "A signing secret is already pinned for this process; a different one "
            "was offered and ignored. The secret is restart-only: restart to change it.",
        )
        return
    _pinned_jwt_secret = secret


def unpin_jwt_secret() -> None:
    """Forget the pinned secret. For tests, which are each their own "process";
    a running daemon never does this."""
    global _pinned_jwt_secret
    _pinned_jwt_secret = ""


def pinned_jwt_secret() -> str:
    """The secret pinned to this process, or ``""`` before startup pinned one."""
    return _pinned_jwt_secret


def effective_jwt_secret(config: NerveConfig | None = None) -> str:
    """The secret tokens are signed and verified with.

    Once startup has pinned one, that — whatever the config object says by
    now. Before that (a CLI process, the installer, tests) it is
    ``auth.jwt_secret`` from the given configuration, or nothing; every
    consumer treats nothing as fail-closed, so an instance that has not
    completed startup refuses rather than admits.
    """
    if _pinned_jwt_secret:
        return _pinned_jwt_secret
    cfg = config if config is not None else get_config()
    return cfg.auth.jwt_secret or ""


def create_token(jwt_secret: str, expiry_hours: int | None = None) -> str:
    """Create a web-session JWT token."""
    hours = max(1, int(expiry_hours)) if expiry_hours else session_expiry_hours()
    now = datetime.now(timezone.utc)
    payload = {
        "exp": now + timedelta(hours=hours),
        "iat": now,
        "sub": "user",
    }
    return jwt.encode(payload, jwt_secret, algorithm=JWT_ALGORITHM)


def maybe_refresh_token(payload: dict, jwt_secret: str) -> str | None:
    """Re-mint a session token that is past its refresh threshold.

    Sliding expiry: each authenticated request carries the session further
    into the future, so a tab in continuous use never hits the wall. Returns
    ``None`` while the token is still fresh — the common case, and the reason
    this costs nothing on most requests.

    Only ordinary web-session tokens slide. Audience-scoped tokens (MCP
    session/worker credentials) are minted per-process with deliberately
    short TTLs and must expire on schedule.
    """
    if payload.get("aud") or payload.get("sub") != "user":
        return None
    iat, exp = payload.get("iat"), payload.get("exp")
    if not isinstance(iat, (int, float)) or not isinstance(exp, (int, float)):
        return None
    lifetime = exp - iat
    if lifetime <= 0:
        return None
    age = datetime.now(timezone.utc).timestamp() - iat
    if age < lifetime * REFRESH_AFTER_RATIO:
        return None
    return create_token(jwt_secret)


def create_mcp_session_token(
    jwt_secret: str,
    session_id: str,
    *,
    ttl_seconds: int = 8 * 60 * 60,
    worker_id: str | None = None,
) -> str:
    """Mint a session-bound MCP token for a backend-managed agent process.

    Carries ``aud=nerve-mcp`` + the bound session id so the external MCP
    endpoint attributes every tool call to the real engine session
    (instead of a satellite). The token is deliberately short-lived.
    Backend clients are normally
    idle-swept within an hour and receive a fresh token when recreated;
    Ultracode children exchange the parent token for still-shorter worker
    tokens so calls can be attributed without persisting secrets.
    """
    now = datetime.now(timezone.utc)
    payload = {
        "iat": now,
        "exp": now + timedelta(seconds=max(60, int(ttl_seconds))),
        "jti": uuid4().hex,
        "sub": "backend-agent",
        "aud": MCP_AUDIENCE,
        MCP_SESSION_CLAIM: session_id,
    }
    if worker_id:
        payload[MCP_WORKER_CLAIM] = worker_id
    return jwt.encode(payload, jwt_secret, algorithm=JWT_ALGORITHM)


def create_external_mcp_token(
    jwt_secret: str,
    *,
    ttl_seconds: int = 8 * 60 * 60,
) -> str:
    """Mint a short-lived MCP-only token for a user-launched client.

    Unlike backend session tokens this intentionally has no bound Nerve
    session; the MCP resolver creates/reuses a satellite session. The MCP
    audience prevents this credential from authenticating to ordinary web
    routes.
    """
    now = datetime.now(timezone.utc)
    payload = {
        "iat": now,
        "exp": now + timedelta(seconds=max(60, int(ttl_seconds))),
        "jti": uuid4().hex,
        "sub": "external-agent-mcp",
        "aud": MCP_AUDIENCE,
    }
    return jwt.encode(payload, jwt_secret, algorithm=JWT_ALGORITHM)


def decode_token(
    token: str, jwt_secret: str, audience: str | None = None,
) -> dict:
    """Decode and validate a JWT token.

    ``audience=None`` (the default) accepts only aud-less tokens — PyJWT
    rejects any token carrying an ``aud`` claim unless the caller
    verifies it, so audience-scoped tokens (MCP session tokens) never
    pass ordinary web-UI auth by accident. Callers that accept scoped
    tokens pass the expected ``audience`` explicitly.
    """
    try:
        return jwt.decode(
            token, jwt_secret, algorithms=[JWT_ALGORITHM], audience=audience,
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


def get_token_from_request(request: Request) -> str:
    """Extract JWT token from cookie, Authorization header, or query param."""
    # Try cookie first
    token = request.cookies.get("nerve_token")
    if token:
        return token

    # Try Authorization header
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:]

    # Try query parameter (for <img src> and <a download> that can't set headers)
    token = request.query_params.get("token")
    if token:
        return token

    raise HTTPException(status_code=401, detail="Not authenticated")


async def require_auth(request: Request) -> dict:
    """FastAPI dependency: require valid authentication."""
    secret = effective_jwt_secret(get_config())
    if not secret:
        # Fail closed. Startup pins a secret before the gateway serves — the
        # configured one, or one generated into the database — so this is only
        # reachable before startup has completed. An empty secret must never
        # mean an open instance, locked or not: inferring "no auth" from a
        # missing credential is the class of bug this seam exists to end.
        raise HTTPException(status_code=503, detail=NO_SECRET_DETAIL)

    token = get_token_from_request(request)
    payload = decode_token(token, secret)
    # Slide the session forward. Stashed on request.state rather than returned
    # so every existing caller of this dependency is unaffected; the gateway's
    # http middleware picks it up and emits SESSION_TOKEN_HEADER.
    refreshed = maybe_refresh_token(payload, secret)
    if refreshed:
        request.state.refreshed_token = refreshed
    return payload


async def authenticate_websocket(websocket: WebSocket) -> bool:
    """Validate WebSocket authentication.

    Checks token from query parameter or first message.
    Returns True if authenticated, False otherwise.
    """
    secret = effective_jwt_secret(get_config())
    if not secret:
        return False  # fail closed, locked or not — see require_auth

    # Check query parameter
    token = websocket.query_params.get("token")
    if token:
        try:
            decode_token(token, secret)
            return True
        except HTTPException:
            return False

    # Check cookie
    token = websocket.cookies.get("nerve_token")
    if token:
        try:
            decode_token(token, secret)
            return True
        except HTTPException:
            return False

    return False
