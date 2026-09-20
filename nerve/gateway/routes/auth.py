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
        # Startup normally generates or loads the signing secret.
        raise HTTPException(
            status_code=503,
            detail="No session signing secret is available yet; restart the "
            "gateway so one is generated, or set auth.jwt_secret.",
        )

    if config.auth.password_hash:
        if not verify_password(req.password, config.auth.password_hash):
            raise HTTPException(status_code=401, detail="Invalid password")
    # A sole passwordless account accepts any password.

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
