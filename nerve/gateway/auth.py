"""JWT authentication for the gateway.

Password login, HS256 session tokens, bcrypt hashing, and the resolution of a
verified token into the :class:`~nerve.identity.Actor` the request acts as.

**The signing secret.** Tokens are signed with :func:`effective_jwt_secret`,
which returns the secret *pinned* at startup by the identity bootstrap:
``auth.jwt_secret`` when configuration supplied one, otherwise the secret the
bootstrap generated on first start and keeps in the database
(``instance_secrets``). Every consumer — this module, the login route, the
external MCP endpoint, the CLI — must go through that function rather than
read ``config.auth.jwt_secret`` directly: the config object is rebuilt on every
reload, and the secret is restart-only. With no secret in force every check
fails closed; there is no unauthenticated mode.

**The actor.** A good signature says the token was minted here; it does not say
who is holding it. Every token therefore carries a ``typ`` claim naming what it
is, and every ingress resolves that claim against the database *on the
request* — see :func:`resolve_actor_from_claims`. Nothing is cached and nothing
is stored on a module global: one agent serves several people at once, so a
process-wide "current user" is how one person's work ends up under another's
name.
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
    system_actor,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from nerve.db import Database

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

# The same, for the other half of an authenticated request: a verified token
# still has to be resolved to an account, and until the lifespan has wired the
# database there is nothing to resolve it against. Fail closed, like the above.
NO_IDENTITY_DETAIL = (
    "Identity storage is not available; the gateway has not completed startup"
)

# Audience claim on session-bound MCP tokens (see create_mcp_session_token).
MCP_AUDIENCE = "nerve-mcp"
# Claim carrying the bound nerve session id on MCP tokens.
MCP_SESSION_CLAIM = "nerve_session_id"
MCP_WORKER_CLAIM = "nerve_worker_id"

# Claim naming what a token is. Present on everything this version mints; its
# absence is what identifies a token minted before per-account sessions existed
# (see LEGACY_SUBJECT). Authorization does not vary by type — every account has
# full permissions (0.4) — but *attribution* does, and so does sliding.
TOKEN_TYPE_CLAIM = "typ"
# A person's web session. ``sub`` is the account id; these slide (see
# maybe_refresh_token).
TOKEN_TYPE_SESSION = "session"
# The instance acting on its own behalf: the CLI talking to its daemon, the
# agent calling its own API, backend agent subprocesses. Resolves to the agent
# system principal (0.6). Never slides — each one is minted for a single use.
TOKEN_TYPE_SYSTEM = "system"

# Subject of a system token. A label, not a lookup key: the system principal is
# read from the database, so this string never has to match anything stored.
SYSTEM_SUBJECT = "agent-system"

# Subject of the web-session tokens minted *before* this version, which sit in
# browsers' localStorage with 30-day expiries. They still verify — same secret,
# same expiry — so they are accepted and resolved to the sole account while
# exactly one exists, and upgraded in place on first use (see require_auth).
# This is the ONLY place the subject string is given a meaning; a later
# contract PR deletes this constant and is_legacy_session_token with it.
LEGACY_SUBJECT = "user"


# bcrypt hashes at most this many *bytes* of a password. Up to and including
# 4.x the library silently ignored the rest; 5.0 raises instead, which turns a
# long or emoji-laden password into a 500 unless something upstream says no
# first. That something is here, once, rather than at each caller.
PASSWORD_MAX_BYTES = 72


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
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    """Verify a plaintext password against a bcrypt hash.

    ``False`` — never an exception — when ``hashed`` is not a bcrypt hash at
    all. An operator can put anything in ``auth.password_hash``, and a
    credential that cannot be parsed must read as "does not match" rather than
    as a 500 that tells the caller their guess was interesting.

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
    if not plain or not hashed:
        return False
    try:
        return bcrypt.checkpw(
            plain.encode("utf-8")[:PASSWORD_MAX_BYTES], hashed.encode("utf-8"),
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


def create_session_token(
    jwt_secret: str, account_id: str, expiry_hours: int | None = None,
) -> str:
    """Create a web-session JWT for one local account.

    ``sub`` is the account id, so every request the browser makes afterwards
    says *which* person is making it. ``typ`` says what the token is, which is
    what the refresh gate and the MCP endpoint read — never the subject string.
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
    }
    return jwt.encode(payload, jwt_secret, algorithm=JWT_ALGORITHM)


def create_system_token(jwt_secret: str, *, ttl_seconds: int = 3600) -> str:
    """Mint a token for the instance acting on its own behalf.

    Used where there is no person to name: the CLI calling its own daemon over
    HTTP (``nerve reload``, session start/stop), and the agent calling its own
    REST API in-process. Holding the signing secret is already proof of being
    the instance, so the credential does not stand in for anybody — it resolves
    to the agent system principal, which is what autonomous work is attributed
    to (0.6).

    Short-lived by default: each one is minted for a single call and nothing
    stores it, so there is no reason to hand out a 30-day credential.
    """
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
    """Whether these claims are a web session minted before this version.

    The grandfather clause, in one predicate: an aud-less token with no ``typ``
    and the old fixed subject. Everything that has to know about the legacy
    shape asks this, so removing the clause later is deleting one function and
    its callers rather than hunting for ``== "user"`` comparisons.
    """
    return (
        not payload.get("aud")
        and payload.get(TOKEN_TYPE_CLAIM) is None
        and payload.get("sub") == LEGACY_SUBJECT
    )


def maybe_refresh_token(
    payload: dict, jwt_secret: str, actor: Actor | None = None,
) -> str | None:
    """Re-mint a session token that is past its refresh threshold.

    Sliding expiry: each authenticated request carries the session further
    into the future, so a tab in continuous use never hits the wall. Returns
    ``None`` while the token is still fresh — the common case, and the reason
    this costs nothing on most requests.

    Only ordinary web-session tokens slide, and the gate is the ``typ`` claim.
    It used to be ``sub != "user"``, which stopped meaning "not a web session"
    the moment ``sub`` became an account id: read as a subject it would either
    have stopped sliding every web session or started sliding MCP credentials,
    which are minted per-process with deliberately short TTLs and must expire
    on schedule. System tokens do not slide either, for the same reason.

    The replacement is minted for ``actor``'s account rather than for whatever
    the old token said, so a slid session cannot outlive the account it names.
    """
    if payload.get("aud") or payload.get(TOKEN_TYPE_CLAIM) != TOKEN_TYPE_SESSION:
        return None
    account_id = actor.account_id if actor is not None else payload.get("sub")
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
        TOKEN_TYPE_CLAIM: TOKEN_TYPE_SYSTEM,
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


def identity_store() -> "Database | None":
    """The database the request path resolves actors against, or ``None``.

    One seam, and it is the one the routes already use: the lifespan wires the
    live :class:`~nerve.db.Database` into the route dependency container before
    the app serves, so by the time any request arrives it is there. ``None``
    means the gateway has not finished starting (or a unit test built an app
    without one), and every caller treats that as fail-closed rather than as
    permission to skip the check.

    Imported inside the function: the routes package imports this module, so a
    module-level import of it here would be a cycle.
    """
    from nerve.gateway.routes._deps import get_deps

    try:
        deps = get_deps()
    except RuntimeError:
        return None
    return getattr(deps, "db", None)


async def resolve_actor_from_claims(store: "Database", claims: dict) -> Actor:
    """The actor a verified token acts as — looked up, never inferred.

    Dispatch is on ``typ`` (and ``aud`` for MCP credentials), so the subject is
    only ever *read as data* — an account id to look up. Each branch hits the
    database on every call: an account disabled or deleted a moment ago must
    stop working on the next request, which cannot happen if an actor is cached
    anywhere.

    Raises :class:`~nerve.identity.ActorResolutionError` when the credential
    verifies but names nobody this instance can act for.
    """
    if claims.get("aud") == MCP_AUDIENCE:
        # MCP and backend-agent credentials. Minted by this instance for its
        # own subprocesses and for clients the operator launched; they act as
        # the agent, not as a person (0.6).
        return await system_actor(store)

    token_type = claims.get(TOKEN_TYPE_CLAIM)
    if token_type == TOKEN_TYPE_SYSTEM:
        return await system_actor(store)
    if token_type == TOKEN_TYPE_SESSION:
        return await actor_for_account(store, claims.get("sub"))
    if is_legacy_session_token(claims):
        # Grandfathered: a tab that logged in before per-account sessions
        # existed. Bounded to the single-account case — with two accounts the
        # token names nobody in particular and actor_for_sole_account refuses.
        return await actor_for_sole_account(store)

    raise ActorResolutionError("This credential names no actor")


async def require_auth(request: Request) -> Actor:
    """FastAPI dependency: require valid authentication, and say who it is.

    Returns the :class:`~nerve.identity.Actor` the request acts as. Routes
    declare it purely as a gate today; from PR 4 it is what session and message
    rows are attributed to.
    """
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

    store = identity_store()
    if store is None:
        # A good signature is not an identity. With no store there is nothing
        # to resolve it against, so refuse rather than admit an actor-less
        # request.
        raise HTTPException(status_code=503, detail=NO_IDENTITY_DETAIL)
    try:
        actor = await resolve_actor_from_claims(store, payload)
    except ActorResolutionError as e:
        raise HTTPException(status_code=401, detail=str(e)) from e

    # Hand a fresh token back where one is due. Stashed on request.state rather
    # than returned so the response shape of every route is unchanged; the
    # gateway's http middleware picks it up and emits SESSION_TOKEN_HEADER.
    if is_legacy_session_token(payload) and actor.account_id:
        # Upgrade rather than slide: the tab keeps working and stops carrying
        # the legacy shape after its first call, so the grandfather clause
        # drains itself instead of lingering for 30 days. (The sole account
        # always has an id; without one there is simply nothing to upgrade to,
        # and the request is served on the token it came with.)
        request.state.refreshed_token = create_session_token(secret, actor.account_id)
    else:
        refreshed = maybe_refresh_token(payload, secret, actor)
        if refreshed:
            request.state.refreshed_token = refreshed
    return actor


async def authenticate_websocket(websocket: WebSocket) -> Actor | None:
    """Validate WebSocket authentication and resolve the connection's actor.

    Returns the :class:`~nerve.identity.Actor` on success and ``None`` on any
    failure. The caller fixes the returned value on the connection for its
    whole life: a socket is open for hours, and re-reading identity mid-stream
    would let a message sent now be attributed differently from one sent a
    minute ago.

    A slid token is deliberately *not* handed back here — a WebSocket has no
    response headers to put one on, and the browser's REST traffic keeps the
    stored token fresh.
    """
    secret = effective_jwt_secret(get_config())
    if not secret:
        return None  # fail closed, locked or not — see require_auth

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
