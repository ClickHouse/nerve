"""Gateway password and token authentication.

Signing uses the secret pinned at startup. Authentication fails closed until a
secret is available.
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

# Fallback used only when auth.jwt_expiry_hours cannot be read.
DEFAULT_JWT_EXPIRY_HOURS = 720

# Session expiry acts as an idle timeout by refreshing active sessions.
REFRESH_AFTER_RATIO = 0.5

SESSION_TOKEN_HEADER = "X-Nerve-Token"

NO_SECRET_DETAIL = "No signing secret is in force; the gateway has not completed startup"

MCP_AUDIENCE = "nerve-mcp"
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


# Keep signing stable across configuration reloads. A restart applies changes.
_pinned_jwt_secret: str = ""


def pin_jwt_secret(secret: str) -> None:
    """Pin the first non-empty signing secret for this process."""
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
    """Clear the pinned secret for tests."""
    global _pinned_jwt_secret
    _pinned_jwt_secret = ""


def pinned_jwt_secret() -> str:
    """Return the process signing secret, or ``""`` before startup."""
    return _pinned_jwt_secret


def effective_jwt_secret(config: NerveConfig | None = None) -> str:
    """Return the pinned secret, or the configured secret before startup."""
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
    """Refresh a browser session after its refresh threshold."""
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
    """Mint a short-lived MCP token bound to a Nerve session."""
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
    """Mint an MCP-only token without a bound Nerve session."""
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
    """Decode a JWT for the expected audience.

    The default accepts only tokens without an audience, keeping MCP tokens out
    of web authentication.
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
        raise HTTPException(status_code=503, detail=NO_SECRET_DETAIL)

    token = get_token_from_request(request)
    payload = decode_token(token, secret)
    # Middleware emits this without changing route response models.
    refreshed = maybe_refresh_token(payload, secret)
    if refreshed:
        request.state.refreshed_token = refreshed
    return payload


async def authenticate_websocket(websocket: WebSocket) -> bool:
    """Validate WebSocket authentication."""
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
