"""JWT authentication for the gateway.

Single-user system: password-only login, JWT tokens, bcrypt hashing.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt
from fastapi import Depends, HTTPException, Request, WebSocket

from nerve.config import get_config

logger = logging.getLogger(__name__)

JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = 24


def verify_password(plain: str, hashed: str) -> bool:
    """Verify a plaintext password against a bcrypt hash."""
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


def create_token(jwt_secret: str) -> str:
    """Create a JWT token."""
    payload = {
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS),
        "iat": datetime.now(timezone.utc),
        "sub": "user",
    }
    return jwt.encode(payload, jwt_secret, algorithm=JWT_ALGORITHM)


def decode_token(token: str, jwt_secret: str) -> dict:
    """Decode and validate a JWT token."""
    try:
        return jwt.decode(token, jwt_secret, algorithms=[JWT_ALGORITHM])
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


async def require_auth(request: Request) -> dict:
    """FastAPI dependency: require valid authentication."""
    config = get_config()
    if not config.auth.jwt_secret:
        # Auth not configured — allow access (development mode)
        return {"sub": "user"}

    token = get_token_from_request(request)
    return decode_token(token, config.auth.jwt_secret)


async def authenticate_websocket(websocket: WebSocket) -> bool:
    """Validate WebSocket authentication.

    Checks token from query parameter or first message.
    Returns True if authenticated, False otherwise.
    """
    config = get_config()
    if not config.auth.jwt_secret:
        return True  # Dev mode

    # Check query parameter
    token = websocket.query_params.get("token")
    if token:
        try:
            decode_token(token, config.auth.jwt_secret)
            return True
        except HTTPException:
            return False

    # Check cookie
    token = websocket.cookies.get("nerve_token")
    if token:
        try:
            decode_token(token, config.auth.jwt_secret)
            return True
        except HTTPException:
            return False

    return False
