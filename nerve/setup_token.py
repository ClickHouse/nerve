"""Who may claim an unclaimed instance.

A fresh install has one account with no password (0.5): until somebody claims
it, every caller who can reach the gateway is admitted as the owner. The web
wizard's first step ends that state, and this module is the guard on it.

Two ways to prove you are allowed to:

* **You are on the machine.** The request's *socket peer address* is loopback.
  That is at least as strong as reading a token out of the machine's own log,
  and it is what makes the common case — someone installing Nerve on their own
  box and opening a browser on it — need no token at all.
* **You hold the setup token.** Generated on first start while the instance is
  unclaimed, written to the server log (so `nerve logs` and `docker logs` carry
  it) and to `nerve status`, kept in ``instance_secrets`` so a restart does not
  invalidate it, and deleted the moment the account is claimed.

Three things this deliberately does not do:

* **It never reads a header.** Not ``X-Forwarded-For``, not ``Forwarded``, not
  any equivalent. Nerve has no forwarding-header handling anywhere, and a
  caller-supplied header that can turn a remote request into a local one
  defeats the entire guard. The peer address comes from the ASGI scope, which
  the server fills in from the socket.
* **It never looks at the bind address.** ``gateway.host`` defaults to
  ``0.0.0.0``, so a laptop install listens on every interface while its
  operator browses from loopback. Checking the bind address would demand a
  token in the most common local case and prove nothing in the others.
* **It does not decide *what* a claim does.** The transaction that names and
  secures the account is :meth:`nerve.db.accounts.AccountStore.claim_sole_account`,
  which asserts its own precondition; this module only answers "may this
  caller try".

One honest limitation, documented in ``docs/setup.md``: a reverse proxy on the
same host makes every request's peer loopback, which silently makes the token
optional. ``auth.setup_token_required: true`` forces it for those deployments.

**The one place a header can reach the peer address, and why it is still
safe.** uvicorn runs ``ProxyHeadersMiddleware`` by default, which rewrites
``scope["client"]`` from ``X-Forwarded-For`` — but only when the immediate peer
is already trusted (``127.0.0.1``). A remote caller's header is ignored
outright, so the rewrite can never turn a remote request into a local one; and
where it does fire, the peer was loopback anyway. In the deployment this module
cannot otherwise see through — a proxy on this host — a proxy that forwards the
real client address makes the guard *stricter* rather than weaker, because the
peer becomes that remote address. Both directions are pinned by tests.
"""

from __future__ import annotations

import ipaddress
import logging
import secrets
from typing import Any

logger = logging.getLogger(__name__)

# The row in ``instance_secrets``. State, not configuration: it must survive a
# restart (a headless operator reads it out of the log and comes back later),
# must never land in a tracked config file, and is scrubbed from a
# ``--no-secrets`` backup with everything else in that table.
SETUP_TOKEN_NAME = "setup_token"

# 32 url-safe characters. Long enough that guessing is not a strategy, short
# enough to retype from a terminal into a browser on another machine.
_TOKEN_BYTES = 24

# Compared against when no token is stored, so a request that supplies one
# costs the same whether or not there is anything to match. Built once, at
# import: it is not a credential, it is a constant-time stand-in for one.
_DECOY_TOKEN = secrets.token_urlsafe(_TOKEN_BYTES)


async def instance_is_unclaimed(db, config) -> bool:
    """Whether the sole account still has no credential anywhere.

    The predicate the whole guard turns on, and the same one
    ``claim_sole_account`` enforces inside its transaction: exactly one
    account, no hash on its row and none in configuration. Read from the
    accounts table rather than from the published ``/api/auth/status``
    descriptor, so the guard cannot be weakened by a change to what that
    endpoint chooses to say.

    Imported inside the function because the routes package imports this
    module; one definition of "passwordless" rather than two that can drift.
    """
    from nerve.gateway.routes.accounts import instance_is_passwordless

    state = await db.login_state()
    return instance_is_passwordless(state, config)


async def ensure_setup_token(db, *, unclaimed: bool) -> str | None:
    """The token in force for an unclaimed instance, generating one if needed.

    Returns ``None`` — and *deletes* any stored token — once the instance is
    claimed, so a token read from an old log line stops working the moment it
    stops being needed. Idempotent: an unclaimed instance that restarts keeps
    the token it already published (``ensure_instance_secret`` returns the
    stored value), because an operator who wrote it down must not have to go
    looking for a new one.
    """
    if not unclaimed:
        if await db.get_instance_secret(SETUP_TOKEN_NAME) is not None:
            await db.delete_instance_secret(SETUP_TOKEN_NAME)
            logger.info("Setup token invalidated: this instance has been claimed")
        return None
    return await db.ensure_instance_secret(
        SETUP_TOKEN_NAME, secrets.token_urlsafe(_TOKEN_BYTES),
    )


async def stored_setup_token(db) -> str:
    """The token in force, or ``""``. Never generates one."""
    return await db.get_instance_secret(SETUP_TOKEN_NAME) or ""


async def invalidate_setup_token(db) -> bool:
    """Drop the stored token. True if there was one.

    Called after a successful claim: the token's only job is done, and a
    credential left in the log of a machine anyone can read is a credential
    somebody will eventually use.
    """
    return await db.delete_instance_secret(SETUP_TOKEN_NAME)


def peer_host(scope: dict[str, Any] | None) -> str | None:
    """The socket peer address of a request, from the ASGI scope.

    ``scope["client"]`` is filled in by the server from the accepted
    connection. ``None`` when the transport does not report one, which is
    treated as "not local" everywhere below — fail closed.
    """
    if not scope:
        return None
    client = scope.get("client")
    if not client:
        return None
    host = client[0] if isinstance(client, (tuple, list)) else None
    return str(host) if host else None


def is_loopback_peer(host: str | None) -> bool:
    """Whether an address proves the caller is on this machine.

    ``127.0.0.0/8``, ``::1`` and IPv4-mapped loopback (``::ffff:127.0.0.1``,
    which is what a dual-stack listener reports for a v4 loopback connection —
    a string comparison against ``"127.0.0.1"`` misses it and would demand a
    token from a caller who is on the machine).

    A hostname is not an address and is refused: ``request.client.host`` is
    always an address, and anything else reaching here did not come from a
    socket.
    """
    if not host:
        return False
    candidate = host.strip()
    if candidate.startswith("[") and candidate.endswith("]"):
        candidate = candidate[1:-1]
    # A link-local v6 address carries a zone id (fe80::1%eth0) that
    # ip_address() will not parse. Dropping it cannot turn a non-loopback
    # address into a loopback one.
    candidate = candidate.split("%", 1)[0]
    try:
        address = ipaddress.ip_address(candidate)
    except ValueError:
        return False
    mapped = getattr(address, "ipv4_mapped", None)
    if mapped is not None:
        address = mapped
    return address.is_loopback


def token_is_required(host: str | None, config) -> bool:
    """Whether this caller must present the setup token.

    Always, when ``auth.setup_token_required`` is on — the switch that exists
    for deployments where a proxy on the same host makes every peer look
    local. Otherwise, whenever the peer is not loopback.
    """
    if getattr(config.auth, "setup_token_required", False):
        return True
    return not is_loopback_peer(host)


def token_accepted(supplied: str | None, stored: str) -> bool:
    """Constant-time comparison, with a constant-time "nothing stored" too.

    A missing or empty stored token can never be matched — an instance with no
    token in force refuses every token rather than accepting the empty one —
    and the comparison still happens, so the refusal costs the same either way.
    """
    candidate = supplied or ""
    reference = stored or _DECOY_TOKEN
    matched = secrets.compare_digest(candidate, reference)
    return bool(stored) and matched


def announce(token: str | None, *, host: str, port: int) -> None:
    """Put the token where the operator will find it: the server log.

    A headless install — the Docker case the token exists for — has no
    terminal to prompt at, so this line in ``nerve logs`` / ``docker logs`` is
    the delivery channel. Logged at WARNING because an unclaimed instance is
    admitting everybody who can reach it, which is worth the level.
    """
    if not token:
        return
    logger.warning(
        "This instance has no password yet: anyone who can reach it is the "
        "owner. Finish setup at http://%s:%s/setup — a browser on this machine "
        "needs nothing more, one on another machine needs this setup token: %s",
        "localhost" if host in {"0.0.0.0", "::", "[::]", ""} else host,
        port,
        token,
    )
