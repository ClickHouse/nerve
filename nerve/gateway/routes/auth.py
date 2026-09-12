"""Authentication routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi import HTTPException
from pydantic import BaseModel

from nerve.config import get_config
from nerve.gateway.auth import (
    create_token,
    effective_jwt_secret,
    require_auth,
    verify_password,
)

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
        # No secret in configuration and none stored yet. The identity bootstrap
        # generates one before the gateway serves, so this only fires when it
        # has not run. It used to mint a token signed with the literal string
        # "dev-secret" — and skip the password check while at it — which made
        # a missing secret an open instance. Refuse instead.
        raise HTTPException(
            status_code=503,
            detail="No session signing secret is available yet; restart the "
            "gateway so one is generated, or set auth.jwt_secret.",
        )

    if config.auth.password_hash:
        if not verify_password(req.password, config.auth.password_hash):
            raise HTTPException(status_code=401, detail="Invalid password")
    # Otherwise passwordless: every admitted caller resolves to the single
    # local account, so any password is accepted. Valid only while exactly one
    # account exists; creating a second one requires setting a password first.

    return LoginResponse(token=create_token(secret))


@router.get("/api/auth/status")
async def auth_status():
    """Return whether authentication is required (password configured)."""
    config = get_config()
    return {
        "auth_required": bool(config.auth.password_hash and effective_jwt_secret(config)),
    }


@router.get("/api/auth/check")
async def check_auth(user: dict = Depends(require_auth)):
    return {"authenticated": True}
