"""Persist and validate the mandatory first-account setup token.

The token is generated once while the sole account is passwordless, stored in
``instance_secrets`` across restarts, and deleted as soon as the claim
commits. It is accepted only in the claim request body: never in a URL,
response, log entry, or browser storage.
"""

from __future__ import annotations

import secrets

SETUP_TOKEN_NAME = "setup_token"
_TOKEN_BYTES = 24
_DECOY_TOKEN = secrets.token_urlsafe(_TOKEN_BYTES)


async def instance_is_unclaimed(db, config) -> bool:
    """Whether exactly one account still has no credential anywhere."""
    from nerve.gateway.routes.accounts import instance_is_passwordless

    return instance_is_passwordless(await db.login_state(), config)


async def ensure_setup_token(db, *, unclaimed: bool) -> str | None:
    """Return the persisted token while unclaimed, deleting it otherwise."""
    if not unclaimed:
        await db._delete_instance_secret(SETUP_TOKEN_NAME)
        return None
    return await db._ensure_instance_secret(
        SETUP_TOKEN_NAME, secrets.token_urlsafe(_TOKEN_BYTES),
    )


async def stored_setup_token(db) -> str:
    """Return the token in force, or an empty string. Never generate one."""
    return await db._get_instance_secret(SETUP_TOKEN_NAME) or ""


def token_accepted(supplied: str | None, stored: str) -> bool:
    """Compare in constant time, including when no token is stored."""
    # ``compare_digest`` rejects non-ASCII ``str`` inputs. Encode explicitly so
    # arbitrary JSON strings take the ordinary refusal path instead of turning
    # an unauthenticated bad token into a 500. ``surrogatepass`` also covers a
    # JSON parser handing us an escaped lone surrogate.
    candidate = (supplied or "").encode("utf-8", "surrogatepass")
    reference = (stored or _DECOY_TOKEN).encode("utf-8", "surrogatepass")
    matched = secrets.compare_digest(candidate, reference)
    return bool(candidate) and bool(stored) and matched
