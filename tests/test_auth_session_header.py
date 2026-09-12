"""End-to-end test for the slid-token response header.

``require_auth`` stashes a refreshed token on ``request.state`` and an http
middleware turns that into ``X-Nerve-Token`` on the way out. That hand-off
crosses Starlette's middleware boundary, so it is asserted against a real ASGI
round-trip rather than reasoned about: if the header ever stops coming back,
every browser tab silently returns to being logged out on a fixed timer.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import jwt
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from nerve.config import AuthConfig, NerveConfig, set_config
from nerve.gateway.auth import (
    JWT_ALGORITHM,
    SESSION_TOKEN_HEADER,
    TOKEN_TYPE_CLAIM,
    TOKEN_TYPE_SESSION,
    create_session_token,
    maybe_refresh_token,
    require_auth,
)
from nerve.identity import Actor

_SECRET = "test-secret-for-session-header-padded-32"


@pytest.fixture
def client(tmp_path, open_identity_db, wire_identity_store):
    """Minimal app wired exactly like the gateway: dependency + middleware.

    Against a real bootstrapped database, because the dependency now resolves
    the token's account before it decides whether to slide it.
    ``client.account_id`` is the account the tokens below name.
    """
    set_config(NerveConfig(auth=AuthConfig(jwt_secret=_SECRET, jwt_expiry_hours=720)))
    app = FastAPI()

    from fastapi import Request

    @app.middleware("http")
    async def _slide_session_token(request: Request, call_next):
        response = await call_next(request)
        token = getattr(request.state, "refreshed_token", None)
        if token:
            response.headers[SESSION_TOKEN_HEADER] = token
        return response

    @app.get("/api/thing")
    async def thing(actor: Actor = Depends(require_auth)):
        return {"ok": True}

    with TestClient(app) as c:
        database, identity = c.portal.call(open_identity_db, tmp_path / "nerve.db")
        wire_identity_store(database)
        c.account_id = identity.owner_account_id
        try:
            yield c
        finally:
            c.portal.call(database.close)
    set_config(NerveConfig())


def _token(account_id: str, *, age_hours: float, lifetime_hours: int = 720) -> str:
    """A session token minted ``age_hours`` ago — what the browser is holding."""
    iat = datetime.now(timezone.utc) - timedelta(hours=age_hours)
    return jwt.encode(
        {
            "iat": iat,
            "exp": iat + timedelta(hours=lifetime_hours),
            "sub": account_id,
            TOKEN_TYPE_CLAIM: TOKEN_TYPE_SESSION,
        },
        _SECRET,
        algorithm=JWT_ALGORITHM,
    )


def test_fresh_token_gets_no_refresh_header(client):
    fresh = create_session_token(_SECRET, client.account_id)
    res = client.get("/api/thing", headers={"Authorization": f"Bearer {fresh}"})
    assert res.status_code == 200
    assert SESSION_TOKEN_HEADER not in res.headers


def test_half_spent_token_comes_back_refreshed(client):
    stale = _token(client.account_id, age_hours=400)
    res = client.get("/api/thing", headers={"Authorization": f"Bearer {stale}"})
    assert res.status_code == 200

    fresh = res.headers.get(SESSION_TOKEN_HEADER)
    assert fresh, "an aging session must be handed a replacement token"
    assert fresh != stale

    decoded = jwt.decode(fresh, _SECRET, algorithms=[JWT_ALGORITHM])
    old = jwt.decode(stale, _SECRET, algorithms=[JWT_ALGORITHM])
    assert decoded["exp"] > old["exp"]
    # The replacement still names the same account, and is still a session.
    assert decoded["sub"] == client.account_id
    assert decoded[TOKEN_TYPE_CLAIM] == TOKEN_TYPE_SESSION
    # And it must itself authenticate.
    assert client.get(
        "/api/thing", headers={"Authorization": f"Bearer {fresh}"},
    ).status_code == 200


def test_expired_token_is_rejected(client):
    # Older than its own 720h window.
    dead = _token(client.account_id, age_hours=800)
    res = client.get("/api/thing", headers={"Authorization": f"Bearer {dead}"})
    assert res.status_code == 401
    assert SESSION_TOKEN_HEADER not in res.headers


def test_refresh_is_not_triggered_by_an_unauthenticated_call(client):
    res = client.get("/api/thing")
    assert res.status_code == 401
    assert SESSION_TOKEN_HEADER not in res.headers


def test_query_param_token_also_slides(client):
    """`<img src>`/downloads authenticate via ?token= and must slide too."""
    stale = _token(client.account_id, age_hours=400)
    res = client.get(f"/api/thing?token={stale}")
    assert res.status_code == 200
    assert res.headers.get(SESSION_TOKEN_HEADER)


def test_maybe_refresh_agrees_with_the_route(client):
    """Guard against the dependency and the helper drifting apart."""
    stale = _token(client.account_id, age_hours=400)
    payload = jwt.decode(stale, _SECRET, algorithms=[JWT_ALGORITHM])
    assert maybe_refresh_token(payload, _SECRET) is not None
