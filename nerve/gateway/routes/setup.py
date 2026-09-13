"""Secure first-account claim.

This is intentionally not an administration wizard. The sole unauthenticated
write names and secures the account created at install time. Every request must
carry the persisted setup token in its JSON body.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from nerve.config import get_config
from nerve.db.accounts import AccountError, NotClaimableError, UsernameTakenError
from nerve.gateway.auth import (
    NO_IDENTITY_DETAIL,
    create_session_token,
    effective_jwt_secret,
    hash_password,
    identity_store,
    password_length_problem,
)
from nerve.setup_token import (
    SETUP_TOKEN_NAME,
    instance_is_unclaimed,
    stored_setup_token,
    token_accepted,
)

logger = logging.getLogger(__name__)
router = APIRouter()

_GUARD_REFUSED = (
    "A valid setup token is required. Run 'nerve status' on the server to "
    "read the current token."
)


class ClaimRequest(BaseModel):
    username: str
    password: str = Field(min_length=1)
    setup_token: str = Field(min_length=1)
    display_name: str | None = None


class ClaimResponse(BaseModel):
    """The client session created by a successful claim."""

    token: str


@router.post("/api/setup/claim", response_model=ClaimResponse)
async def claim(req: ClaimRequest):
    """Atomically name and secure the sole account, then sign the client in."""
    config = get_config()
    secret = effective_jwt_secret(config)
    store = identity_store()
    if store is None:
        raise HTTPException(status_code=503, detail=NO_IDENTITY_DETAIL)
    if not secret:
        raise HTTPException(
            status_code=503,
            detail="No session signing secret is available; restart the gateway "
                   "or set auth.jwt_secret.",
        )

    # Validate before reading claimability. A caller without the credential
    # learns nothing here, and an absent stored value still takes the same
    # constant-time comparison path against the module decoy.
    stored = await stored_setup_token(store)
    if not token_accepted(req.setup_token, stored):
        raise HTTPException(status_code=403, detail=_GUARD_REFUSED)

    problem = password_length_problem(req.password)
    if problem:
        raise HTTPException(status_code=400, detail=problem)
    credential = hash_password(req.password)

    if not await instance_is_unclaimed(store, config):
        raise HTTPException(
            status_code=409,
            detail="This instance has already been claimed. Sign in instead.",
        )

    try:
        account = await store.claim_sole_account(
            username=req.username,
            credential=credential,
            display_name=(req.display_name or "").strip() or None,
            invalidate_secret_name=SETUP_TOKEN_NAME,
        )
    except NotClaimableError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except UsernameTakenError as e:  # pragma: no cover - one account exists
        raise HTTPException(status_code=409, detail=str(e)) from e
    except AccountError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    # Closing is best-effort. Per-frame epoch checks independently prevent a
    # stale socket from acting if proactive closure misses or fails.
    try:
        from nerve.gateway.server import close_revoked_sockets

        await close_revoked_sockets()
    except Exception as e:  # noqa: BLE001 - the claim has already committed
        logger.warning("Claim: open sockets could not be closed: %s", e)

    logger.info(
        "Instance claimed: account %s now has a password; pre-claim sessions "
        "were revoked",
        account["id"],
    )
    return ClaimResponse(token=create_session_token(
        secret,
        account["id"],
        session_epoch=account.get("session_epoch") or 0,
    ))
