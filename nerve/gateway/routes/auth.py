"""Authentication routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi import HTTPException
from pydantic import BaseModel

from nerve.config import get_config
from nerve.gateway.auth import (
    NO_IDENTITY_DETAIL,
    create_session_token,
    effective_jwt_secret,
    identity_store,
    require_auth,
    verify_password,
)
from nerve.identity import Actor, ActorResolutionError, actor_for_sole_account

router = APIRouter()


class LoginRequest(BaseModel):
    password: str


class LoginResponse(BaseModel):
    token: str


@router.post("/api/auth/login", response_model=LoginResponse)
async def login(req: LoginRequest):
    config = get_config()
    secret = effective_jwt_secret(config)
    if not secret:
        # Startup normally generates or loads the signing secret.
        raise HTTPException(
            status_code=503,
            detail="No session signing secret is available yet; restart the "
            "gateway so one is generated, or set auth.jwt_secret.",
        )

    store = identity_store()
    if store is None:
        # Nothing to mint a token *for*. A session token names an account, and
        # without the database there is no account to name.
        raise HTTPException(status_code=503, detail=NO_IDENTITY_DETAIL)

    if config.auth.password_hash:
        if not verify_password(req.password, config.auth.password_hash):
            raise HTTPException(status_code=401, detail="Invalid password")
    # A sole passwordless account accepts any password.

    # Password-only login names nobody, so it is valid exactly while there is
    # only one account it could mean — the same bound passwordless access has
    # (0.5), and the reason an upgrading install with no username can still log
    # in. PR 3 adds the username and the multi-account form; until then a
    # second account is refused at creation, so this cannot be reached with
    # two. The account's own state (disabled) is checked on the way to its
    # actor, so a disabled account cannot log in either.
    try:
        actor: Actor = await actor_for_sole_account(store)
    except ActorResolutionError as e:
        raise HTTPException(status_code=401, detail=str(e)) from e

    return LoginResponse(token=create_session_token(secret, actor.account_id))


@router.get("/api/auth/status")
async def auth_status():
    """Return whether authentication is required (password configured)."""
    config = get_config()
    return {
        "auth_required": bool(config.auth.password_hash and effective_jwt_secret(config)),
    }


@router.get("/api/auth/check")
async def check_auth(actor: Actor = Depends(require_auth)):
    return {"authenticated": True}
