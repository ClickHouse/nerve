"""Gateway password and token authentication.

Signing uses the secret pinned at startup. Verified tokens are resolved against
the database on every request; a valid signature alone is not an identity.

The authentication mode is pinned at startup from ``NERVE_AUTH_MODE``. In
``external`` mode, the gateway names the person behind a request in the
``X-Nerve-Actor-Context`` header (see :func:`decode_actor_context`), and local
logins and session tokens are not accepted.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import bcrypt
import jwt
from fastapi import Depends, HTTPException, Request, WebSocket

from nerve.config import ConfigError, NerveConfig, get_config
from nerve.identity import (
    ACTOR_KIND_HUMAN,
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


# How this instance learns who makes a request. ``local``: local accounts and
# session tokens. ``external``: the gateway names the person in the actor
# context header. Only the environment sets the mode, and only at startup.
AUTH_MODE_ENV = "NERVE_AUTH_MODE"
AUTH_MODE_LOCAL = "local"
AUTH_MODE_EXTERNAL = "external"
AUTH_MODES = (AUTH_MODE_LOCAL, AUTH_MODE_EXTERNAL)


def parse_auth_mode(value: str | None) -> str:
    """Parse an authentication mode, and refuse a mode that does not exist.

    Unset or blank is ``local``. An unknown value is an error and does not
    fall back to ``local``: an operator who asks for one mode must not get a
    different mode without notice.
    """
    if value is None:
        return AUTH_MODE_LOCAL
    text = str(value).strip().lower()
    if not text:
        return AUTH_MODE_LOCAL
    if text in AUTH_MODES:
        return text
    accepted = ", ".join(repr(mode) for mode in AUTH_MODES)
    raise ConfigError(
        f"{AUTH_MODE_ENV} must be one of {accepted}, got {value!r}. "
        f"Unset {AUTH_MODE_ENV} to run in local mode."
    )


def auth_mode_from_env() -> str:
    """Read and parse ``NERVE_AUTH_MODE``."""
    return parse_auth_mode(os.environ.get(AUTH_MODE_ENV))


# Pinned once by create_app(). A configuration reload does not read it.
_pinned_auth_mode: str | None = None


def pin_auth_mode(mode: str) -> None:
    """Pin the authentication mode for this process.

    A second pin of the same mode has no effect. A different mode raises
    :class:`ConfigError`, because the mode changes only with a restart.
    """
    global _pinned_auth_mode
    if mode not in AUTH_MODES:
        raise ValueError(f"unknown authentication mode {mode!r}")
    if _pinned_auth_mode is not None and _pinned_auth_mode != mode:
        raise ConfigError(
            f"The authentication mode {_pinned_auth_mode!r} is already pinned for "
            f"this process; {mode!r} was offered. Restart to change {AUTH_MODE_ENV}."
        )
    _pinned_auth_mode = mode


def unpin_auth_mode() -> None:
    """Clear the pinned authentication mode for tests."""
    global _pinned_auth_mode
    _pinned_auth_mode = None


def auth_mode() -> str:
    """Return the pinned mode, or the mode in the environment before startup.

    CLI commands such as ``nerve init`` and ``nerve doctor`` do not start the
    gateway, so they read the environment.
    """
    if _pinned_auth_mode is not None:
        return _pinned_auth_mode
    return auth_mode_from_env()


def is_external_mode() -> bool:
    """Whether the gateway names the person behind each request."""
    return auth_mode() == AUTH_MODE_EXTERNAL


def create_session_token(
    jwt_secret: str, account_id: str, expiry_hours: int | None = None,
) -> str:
    """Create a typed web-session JWT whose subject is an account id."""
    if not account_id:
        raise ValueError("a session token must name an account")
    hours = max(1, int(expiry_hours)) if expiry_hours else session_expiry_hours()
    now = datetime.now(timezone.utc)
    payload = {
        "exp": now + timedelta(hours=hours),
        "iat": now,
        "sub": account_id,
        TOKEN_TYPE_CLAIM: TOKEN_TYPE_SESSION,
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
    """Refresh an account session after its refresh threshold."""
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
    return create_session_token(jwt_secret, account_id)


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


EXTERNAL_SESSION_DETAIL = (
    "Session tokens are not accepted in external mode; the gateway names the "
    "person behind each request"
)


async def resolve_actor_from_claims(store: "Database", claims: dict) -> Actor:
    """Resolve verified token claims to their current actor.

    In external mode, session tokens name no actor: only system and MCP
    tokens are accepted, and they give the system actor.
    """
    if claims.get("aud") == MCP_AUDIENCE:
        return store.system_actor

    token_type = claims.get(TOKEN_TYPE_CLAIM)
    if token_type == TOKEN_TYPE_SYSTEM:
        return store.system_actor
    is_session = token_type == TOKEN_TYPE_SESSION or is_legacy_session_token(claims)
    if is_session and is_external_mode():
        raise ActorResolutionError(EXTERNAL_SESSION_DETAIL)
    if token_type == TOKEN_TYPE_SESSION:
        return await actor_for_account(store, claims.get("sub"))
    if is_legacy_session_token(claims):
        return await actor_for_sole_account(store)

    raise ActorResolutionError("This credential names no actor")


# In external mode, the gateway sends the person behind a request in this
# header, as a compact JWS.
ACTOR_CONTEXT_HEADER = "X-Nerve-Actor-Context"

_BASE64URL = re.compile(r"[A-Za-z0-9_-]*")


class ActorContextError(ValueError):
    """The actor context header cannot be read."""


def decode_actor_context(value: str) -> tuple[str, str | None]:
    """Return the principal ID and display name in an actor context header.

    The value is a compact JWS. This function decodes the payload and does
    not check the signature, so it trusts every caller that can reach Nerve.
    External mode needs the gateway to be the only caller. Every reader of
    the header calls this function.

    The principal ID is returned as a canonical lower-case UUID string.
    Raises :class:`ActorContextError` when the value cannot be read.
    """
    parts = value.split(".")
    if len(parts) != 3:
        raise ActorContextError("The actor context is not a compact JWS")
    segment = parts[1]
    if not _BASE64URL.fullmatch(segment):
        raise ActorContextError("The actor context payload is not base64url")
    try:
        claims = json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4)))
    except (binascii.Error, ValueError, RecursionError) as e:
        # RecursionError: JSON that is nested too deeply to parse.
        raise ActorContextError("The actor context payload is not base64url JSON") from e
    if not isinstance(claims, dict):
        raise ActorContextError("The actor context payload is not a JSON object")

    principal = claims.get("principal_id")
    if not isinstance(principal, str):
        raise ActorContextError("The actor context names no principal")
    try:
        principal_id = str(UUID(principal))
    except ValueError as e:
        raise ActorContextError("The actor context principal is not a UUID") from e

    profile = claims.get("profile")
    if profile is None:
        return principal_id, None
    if not isinstance(profile, dict):
        raise ActorContextError("The actor context profile is not a JSON object")
    display_name = profile.get("display_name")
    if display_name is not None and not isinstance(display_name, str):
        raise ActorContextError("The actor context display name is not a string")
    return principal_id, display_name


async def resolve_external_actor(store: "Database", value: str) -> Actor:
    """Resolve an actor context header to a human actor.

    Adds the ``actor_refs`` row on first sight and writes a changed display
    name, before the request can write anything that refers to the actor.
    The actor has no local account.
    """
    try:
        actor_id, display_name = decode_actor_context(value)
    except ActorContextError as e:
        raise ActorResolutionError(str(e)) from e
    try:
        await store.upsert_external_actor(actor_id, display_name)
    except sqlite3.IntegrityError as e:
        # A schema trigger refuses the system actor's ID.
        raise ActorResolutionError(
            "The actor context names an actor that cannot act as a person"
        ) from e
    return Actor(
        actor_id=actor_id,
        kind=ACTOR_KIND_HUMAN,
        account_id=None,
        display_name=display_name,
    )


async def require_auth(request: Request) -> Actor:
    """Authenticate an HTTP request and return its request-local actor.

    In external mode, a request with the actor context header acts as the
    person it names, and its tokens and cookies are ignored. A request
    without the header can authenticate only with a system or MCP token.
    """
    secret = effective_jwt_secret(get_config())
    if not secret:
        raise HTTPException(status_code=503, detail=NO_SECRET_DETAIL)

    external = is_external_mode()
    context = request.headers.get(ACTOR_CONTEXT_HEADER) if external else None
    if context is not None:
        store = identity_store()
        if store is None:
            raise HTTPException(status_code=503, detail=NO_IDENTITY_DETAIL)
        try:
            return await resolve_external_actor(store, context)
        except ActorResolutionError as e:
            raise HTTPException(status_code=401, detail=str(e)) from e

    token = get_token_from_request(request)
    payload = decode_token(token, secret)

    store = identity_store()
    if store is None:
        raise HTTPException(status_code=503, detail=NO_IDENTITY_DETAIL)
    try:
        actor = await resolve_actor_from_claims(store, payload)
    except ActorResolutionError as e:
        raise HTTPException(status_code=401, detail=str(e)) from e

    if external:
        # External mode accepts no session tokens, so it has none to refresh.
        return actor

    # Middleware emits this without changing route response models.
    if is_legacy_session_token(payload) and actor.account_id:
        request.state.refreshed_token = create_session_token(secret, actor.account_id)
    else:
        refreshed = maybe_refresh_token(payload, secret)
        if refreshed:
            request.state.refreshed_token = refreshed
    return actor


async def authenticate_websocket(websocket: WebSocket) -> Actor | None:
    """Resolve a WebSocket actor when the connection is admitted.

    In external mode, the gateway signs the upgrade request, so the actor
    context header names the person, as for HTTP requests.
    """
    secret = effective_jwt_secret(get_config())
    if not secret:
        return None  # fail closed, as require_auth does

    if is_external_mode():
        context = websocket.headers.get(ACTOR_CONTEXT_HEADER)
        if context is not None:
            store = identity_store()
            if store is None:
                return None
            try:
                return await resolve_external_actor(store, context)
            except ActorResolutionError as e:
                logger.info("WebSocket refused: %s", e)
                return None

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
