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
TOKEN_TYPE_SESSION = "session"
TOKEN_TYPE_SYSTEM = "system"

# Label only; the system actor is resolved from the database.
SYSTEM_SUBJECT = "agent-system"

# Pre-account browser sessions are accepted only for a sole account and are
# replaced on first use.
LEGACY_SUBJECT = "user"


# bcrypt hashes at most this many *bytes* of a password. Up to and including
# 4.x the library silently ignored the rest; 5.0 raises instead, which turns a
# long or emoji-laden password into a 500 unless something upstream says no
# first. That something is here, once, rather than at each caller.
PASSWORD_MAX_BYTES = 72

# The bcrypt work factor every hash this release writes is created at.
#
# Pinned rather than left to ``bcrypt.gensalt()``'s default so that the cost is
# this project's decision and not the library's to change under us — and so
# there is one number to point at when the policy is discussed.
#
# **Cost policy.** Hashes are *stored* at this cost. Hashes are *accepted* at
# any cost, because a configured password copied onto an account row by the
# startup migration carries whatever work factor produced it, possibly years
# ago, and refusing it would lock out an upgrading install. A stored hash at any
# other cost is re-hashed at this one the next time its owner logs in
# successfully (see :func:`needs_rehash`), so an install converges without
# anybody being asked to do anything. Until it does, failed logins are padded to
# a common response budget, because otherwise the comparison time says which
# work factor an account uses and therefore that the account exists.
BCRYPT_COST = 12


class PasswordTooLongError(ValueError):
    """A password longer than bcrypt will hash. Ingress turns it into a 4xx."""


def password_length_problem(plain: str) -> str | None:
    """Why this password cannot be stored, or ``None``.

    Measured in **UTF-8 bytes, not characters**: nineteen emoji are nineteen
    characters and seventy-six bytes, and it is the bytes bcrypt counts. Said in
    the message too, because "too long" on a password a user can see is twelve
    characters long is not a usable error.
    """
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
    """bcrypt-hash a password for storage on an account row.

    The same construction the installer uses for ``auth.password_hash``
    (:mod:`nerve.bootstrap`), at bcrypt's default cost, so a hash produced here
    and one produced there are interchangeable — which is what lets PR 3's
    startup migration *copy* the configured hash onto the account row instead of
    re-hashing it and changing somebody's password.

    Raises :class:`PasswordTooLongError` rather than letting bcrypt's own
    ``ValueError`` escape as a 500. The ingress that collects a password checks
    :func:`password_length_problem` first and answers `400`; this is the
    backstop for anything that does not (a later wizard, a script), and it
    refuses rather than truncating, because a stored credential whose last bytes
    were silently dropped is a password that is not the one its owner set.
    """
    problem = password_length_problem(plain)
    if problem:
        raise PasswordTooLongError(problem)
    return bcrypt.hashpw(
        plain.encode("utf-8"), bcrypt.gensalt(rounds=BCRYPT_COST),
    ).decode("utf-8")


def bcrypt_cost(hashed: str) -> int | None:
    """The work factor a bcrypt hash was produced at, or ``None``.

    Read out of the modular-crypt prefix (``$2b$12$...``) rather than by
    hashing anything. ``None`` for a string that is not a bcrypt hash — a
    configured value an operator typed by hand, say.
    """
    parts = (hashed or "").split("$")
    if len(parts) < 4 or not parts[1].startswith("2"):
        return None
    try:
        return int(parts[2])
    except ValueError:
        return None


def needs_rehash(hashed: str) -> bool:
    """Whether this stored hash should be replaced at the current cost.

    True when the work factor differs from :data:`BCRYPT_COST` — lower (an old
    or hand-made hash, which is weaker than the policy) or higher (slower than
    the policy, and the thing that makes a failed login against it take a
    distinguishable amount of time). An unparseable hash is left alone: there is
    nothing to compare, and re-hashing it would need a password it never
    accepted.
    """
    cost = bcrypt_cost(hashed)
    return cost is not None and cost != BCRYPT_COST


def source_authenticates(credential_source: str, *, configured_password: bool) -> bool:
    """Whether an account on this ``credential_source`` can be logged into.

    The one statement of where a credential lives, so that nothing else has to
    restate it and get it subtly different: ``nerve doctor`` used to call an
    account passwordless whenever its row said ``none``, and then tell the
    operator that their ``auth.password_hash`` did nothing — while the login
    route was still honouring it, and removing it would have opened the
    instance.

    ``local`` carries its own hash. **Both** ``config`` and ``none`` read
    ``auth.password_hash``: the startup mirror keeps the row in step with that
    key, but it only runs at startup, and a reload that adds a password has to
    take effect before the next restart re-derives the row.
    """
    return credential_source == "local" or configured_password


def verify_password(plain: str, hashed: str) -> bool:
    """Verify a plaintext password against a bcrypt hash.

    ``False`` — never an exception — when ``hashed`` is not a bcrypt hash at
    all. An operator can put anything in ``auth.password_hash``, and a
    credential that cannot be parsed must read as "does not match" rather than
    as a 500 that tells the caller their guess was interesting.

    **An empty candidate is compared, not refused.** Returning early for an
    empty password made an unknown-username probe return in microseconds while a
    known one paid for a full comparison — username enumeration with an empty
    string — and it rejected the unusual but previously valid case of a
    configured hash *of* an empty password, which an upgrade has to keep
    honouring. Only a missing *hash* short-circuits, because there is then
    nothing to compare against at all.

    **An over-long candidate is truncated to bcrypt's 72 bytes rather than
    refused**, which is the asymmetry with :func:`hash_password` and is
    deliberate. Every hash that reaches this function from before bcrypt 5 was
    made from the first 72 bytes of whatever was typed, because that is what the
    library did; refusing the full password now would lock out anybody whose
    password is longer than that and who could log in yesterday — and this
    release's whole premise is that an upgrade preserves authentication. It
    weakens nothing: the bytes past 72 were already not part of that hash, and
    no hash this release *creates* can have any, since hashing refuses them.
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


async def resolve_actor_from_claims(store: "Database", claims: dict) -> Actor:
    """Resolve verified token claims to their current actor."""
    if claims.get("aud") == MCP_AUDIENCE:
        return store.system_actor

    token_type = claims.get(TOKEN_TYPE_CLAIM)
    if token_type == TOKEN_TYPE_SYSTEM:
        return store.system_actor
    if token_type == TOKEN_TYPE_SESSION:
        return await actor_for_account(store, claims.get("sub"))
    if is_legacy_session_token(claims):
        return await actor_for_sole_account(store)

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
