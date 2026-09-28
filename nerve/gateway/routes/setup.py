"""First-account claim, the one unauthenticated write.

It gives the install-time account a username and password, or records the
passwordless choice. Every request must carry the setup token.
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
    """Exactly one of ``password`` and ``passwordless: true``.

    ``username`` is required with a password and optional without one.
    """

    username: str | None = None
    password: str | None = Field(default=None, min_length=1)
    passwordless: bool = False
    setup_token: str = Field(min_length=1)
    display_name: str | None = None


class ClaimResponse(BaseModel):
    """The client session created by a successful claim."""

    token: str


@router.post("/api/setup/claim", response_model=ClaimResponse)
async def claim(req: ClaimRequest):
    """Atomically complete setup of the sole account, then sign the client in."""
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

    # Check the token first, so a caller without it learns nothing.
    stored = await stored_setup_token(store)
    if not token_accepted(req.setup_token, stored):
        raise HTTPException(status_code=403, detail=_GUARD_REFUSED)

    if (req.password is None) != req.passwordless:
        raise HTTPException(
            status_code=400,
            detail="Supply a password, or set passwordless to true to keep "
                   "this installation passwordless. Not both.",
        )
    credential = None
    if req.password is not None:
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

    logger.info(
        "Instance claimed: account %s %s",
        account["id"],
        "now has a password" if credential else "stays passwordless by choice",
    )
    return ClaimResponse(token=create_session_token(secret, account["id"]))
