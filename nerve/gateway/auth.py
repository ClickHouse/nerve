"""Gateway password and token authentication.

Signing uses the secret pinned at startup. Verified tokens are resolved against
the database on every request; a valid signature alone is not an identity.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING
from uuid import uuid4

import bcrypt
import jwt
from fastapi import Depends, HTTPException, Request, WebSocket

from nerve.config import NerveConfig, get_config
from nerve.identity import (
    Actor,
    ActorResolutionError,
    actor_for_account,
    actor_for_sole_account,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from nerve.db import Database

logger = logging.getLogger(__name__)

JWT_ALGORITHM = "HS256"

# Fallback used only when auth.jwt_expiry_hours cannot be read.
DEFAULT_JWT_EXPIRY_HOURS = 720

# Session expiry acts as an idle timeout by refreshing active sessions.
REFRESH_AFTER_RATIO = 0.5

SESSION_TOKEN_HEADER = "X-Nerve-Token"

NO_SECRET_DETAIL = "No signing secret is in force; the gateway has not completed startup"

NO_IDENTITY_DETAIL = (
    "Identity storage is not available; the gateway has not completed startup"
)

MCP_AUDIENCE = "nerve-mcp"
MCP_SESSION_CLAIM = "nerve_session_id"
MCP_WORKER_CLAIM = "nerve_worker_id"

TOKEN_TYPE_CLAIM = "typ"
# Claiming an installation increments the account epoch and invalidates tokens
# issued while it was passwordless. Missing claims use the initial epoch, 0.
SESSION_EPOCH_CLAIM = "sep"
TOKEN_TYPE_SESSION = "session"
TOKEN_TYPE_SYSTEM = "system"

# Label only; the system actor is resolved from the database.
SYSTEM_SUBJECT = "agent-system"

# Pre-account browser sessions are accepted only for a sole account and are
# replaced on first use.
LEGACY_SUBJECT = "user"


# bcrypt accepts at most 72 bytes. Validate before calling it.
PASSWORD_MAX_BYTES = 72

# Work factor for new hashes. Existing hashes are accepted and upgraded after a
# successful login.
BCRYPT_COST = 12


class PasswordTooLongError(ValueError):
    """The password exceeds bcrypt's byte limit."""


def password_length_problem(plain: str) -> str | None:
    """Return a validation error for a password, or ``None``."""
    if not plain:
        return "A password is required"
    size = len(plain.encode("utf-8"))
    if size > PASSWORD_MAX_BYTES:
        return (
            f"That password is {size} bytes long; the maximum is "
            f"{PASSWORD_MAX_BYTES} bytes. Note that this is bytes rather than "
            "characters — accented letters and emoji cost two to four each."
        )
    return None


def hash_password(plain: str) -> str:
    """Hash a valid password for storage.

    Raises :class:`PasswordTooLongError` instead of truncating overlong input.
    """
    problem = password_length_problem(plain)
    if problem:
        raise PasswordTooLongError(problem)
    return bcrypt.hashpw(
        plain.encode("utf-8"), bcrypt.gensalt(rounds=BCRYPT_COST),
    ).decode("utf-8")


def bcrypt_cost(hashed: str) -> int | None:
    """Return a bcrypt hash's work factor, or ``None`` if invalid."""
    parts = (hashed or "").split("$")
    if len(parts) < 4 or not parts[1].startswith("2"):
        return None
    try:
        return int(parts[2])
    except ValueError:
        return None


def needs_rehash(hashed: str) -> bool:
    """Return whether a valid hash uses a different work factor."""
    cost = bcrypt_cost(hashed)
    return cost is not None and cost != BCRYPT_COST


def source_authenticates(credential_source: str, *, configured_password: bool) -> bool:
    """Return whether the account has a local or configured credential."""
    return credential_source == "local" or configured_password


def verify_password(plain: str, hashed: str) -> bool:
    """Verify a password, returning ``False`` for malformed hashes.

    Candidates are truncated to 72 bytes to preserve hashes created by bcrypt
    versions that silently truncated overlong passwords. New hashes reject such
    passwords in :func:`hash_password`.
    """
    if not hashed:
        return False
    try:
        return bcrypt.checkpw(
            (plain or "").encode("utf-8")[:PASSWORD_MAX_BYTES], hashed.encode("utf-8"),
        )
    except (ValueError, TypeError):
        return False


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


def create_session_token(
    jwt_secret: str,
    account_id: str,
    expiry_hours: int | None = None,
    *,
    session_epoch: int = 0,
) -> str:
    """Create a web-session JWT for one local account.

    ``sub`` is the account id, so every request the browser makes afterwards
    says *which* person is making it. ``typ`` says what the token is, which is
    what the refresh gate and the MCP endpoint read — never the subject string.

    ``session_epoch`` is the account's epoch at the moment of minting, and the
    caller passes the value it just read from the row. It defaults to 0 rather
    than being required because 0 is where every account starts and what a
    token minted before the column existed reads as; a caller that mints for an
    account which has been claimed **must** pass the row's value, or the token
    it hands out is stale on arrival.
    """
    if not account_id:
        raise ValueError("a session token must name an account")
    hours = max(1, int(expiry_hours)) if expiry_hours else session_expiry_hours()
    now = datetime.now(timezone.utc)
    payload = {
        "exp": now + timedelta(hours=hours),
        "iat": now,
        "sub": account_id,
        TOKEN_TYPE_CLAIM: TOKEN_TYPE_SESSION,
        SESSION_EPOCH_CLAIM: int(session_epoch or 0),
    }
    return jwt.encode(payload, jwt_secret, algorithm=JWT_ALGORITHM)


def create_system_token(jwt_secret: str, *, ttl_seconds: int = 3600) -> str:
    """Mint a short-lived token for the instance acting on its own behalf."""
    now = datetime.now(timezone.utc)
    payload = {
        "iat": now,
        "exp": now + timedelta(seconds=max(60, int(ttl_seconds))),
        "jti": uuid4().hex,
        "sub": SYSTEM_SUBJECT,
        TOKEN_TYPE_CLAIM: TOKEN_TYPE_SYSTEM,
    }
    return jwt.encode(payload, jwt_secret, algorithm=JWT_ALGORITHM)


def is_legacy_session_token(payload: dict) -> bool:
    """Match the exact aud-less, pre-``typ`` browser-session shape."""
    return (
        not payload.get("aud")
        and payload.get(TOKEN_TYPE_CLAIM) is None
        and payload.get("sub") == LEGACY_SUBJECT
    )


def maybe_refresh_token(payload: dict, jwt_secret: str) -> str | None:
    """Refresh an account session while preserving its session epoch."""
    if payload.get("aud") or payload.get(TOKEN_TYPE_CLAIM) != TOKEN_TYPE_SESSION:
        return None
    account_id = payload.get("sub")
    if not account_id:
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
    return create_session_token(
        jwt_secret, account_id,
        session_epoch=session_epoch_of(payload),
    )


def session_epoch_of(claims: dict) -> int:
    """The epoch a session token was minted under. Absent or unusable reads 0.

    Said once so the three readers — the slide, the resolution check and the
    tests — cannot disagree about what a missing claim means.
    """
    raw = claims.get(SESSION_EPOCH_CLAIM)
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return 0
    return int(raw)


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
        TOKEN_TYPE_CLAIM: TOKEN_TYPE_SYSTEM,
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
        TOKEN_TYPE_CLAIM: TOKEN_TYPE_SYSTEM,
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


def identity_store() -> "Database | None":
    """Return the request database, or ``None`` before startup completes."""
    from nerve.gateway.routes._deps import get_deps

    try:
        deps = get_deps()
    except RuntimeError:
        return None
    return getattr(deps, "db", None)


async def resolve_actor_from_claims(store: "Database", claims: dict) -> Actor:
    """Resolve verified token claims to their current actor."""
    if claims.get("aud") == MCP_AUDIENCE:
        return store.system_actor

    token_type = claims.get(TOKEN_TYPE_CLAIM)
    if token_type == TOKEN_TYPE_SYSTEM:
        return store.system_actor
    if token_type == TOKEN_TYPE_SESSION:
        return await actor_for_account(
            store, claims.get("sub"), session_epoch=session_epoch_of(claims),
        )
    if is_legacy_session_token(claims):
        return await actor_for_sole_account(store, session_epoch=0)

    raise ActorResolutionError("This credential names no actor")


async def require_auth(request: Request) -> Actor:
    """Authenticate an HTTP request and return its request-local actor."""
    secret = effective_jwt_secret(get_config())
    if not secret:
        raise HTTPException(status_code=503, detail=NO_SECRET_DETAIL)

    token = get_token_from_request(request)
    payload = decode_token(token, secret)

    store = identity_store()
    if store is None:
        raise HTTPException(status_code=503, detail=NO_IDENTITY_DETAIL)
    try:
        actor = await resolve_actor_from_claims(store, payload)
    except ActorResolutionError as e:
        raise HTTPException(status_code=401, detail=str(e)) from e

    # Middleware emits this without changing route response models.
    if is_legacy_session_token(payload) and actor.account_id:
        request.state.refreshed_token = create_session_token(secret, actor.account_id)
    else:
        refreshed = maybe_refresh_token(payload, secret)
        if refreshed:
            request.state.refreshed_token = refreshed
    return actor


async def authenticate_websocket(websocket: WebSocket) -> Actor | None:
    """Resolve a WebSocket actor when the connection is admitted."""
    secret = effective_jwt_secret(get_config())
    if not secret:
        return None  # fail closed, as require_auth does

    token = websocket.query_params.get("token") or websocket.cookies.get("nerve_token")
    if not token:
        return None
    try:
        payload = decode_token(token, secret)
    except HTTPException:
        return None

    store = identity_store()
    if store is None:
        return None
    try:
        return await resolve_actor_from_claims(store, payload)
    except ActorResolutionError as e:
        logger.info("WebSocket refused: %s", e)
        return None
