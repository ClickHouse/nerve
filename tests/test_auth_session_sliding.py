"""Tests for sliding web-session tokens.

The gateway used to mint a fixed 24h token with no refresh path, so an
actively-used browser tab was logged out exactly one day after login — mid
work, with no warning. Session tokens now carry a configurable lifetime
(``auth.jwt_expiry_hours``) and are re-minted once past half of it, which
turns the window into an idle timeout.

Covers: the configured TTL is honoured, a fresh token is left alone, a
half-spent one is renewed, and audience-scoped (MCP) tokens — which are
deliberately short-lived — never slide.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import jwt
import pytest

from nerve.config import AuthConfig, NerveConfig, set_config
from nerve.gateway.auth import (
    JWT_ALGORITHM,
    MCP_AUDIENCE,
    create_mcp_session_token,
    create_token,
    maybe_refresh_token,
    session_expiry_hours,
)

_SECRET = "test-secret-for-session-sliding-padded-to-32-bytes"


def _decode(token: str) -> dict:
    return jwt.decode(
        token, _SECRET, algorithms=[JWT_ALGORITHM],
        options={"verify_aud": False},
    )


def _aged_payload(*, lifetime_hours: int, age_hours: float) -> dict:
    """A session payload minted ``age_hours`` ago with the given lifetime."""
    iat = datetime.now(timezone.utc) - timedelta(hours=age_hours)
    return {
        "iat": iat.timestamp(),
        "exp": (iat + timedelta(hours=lifetime_hours)).timestamp(),
        "sub": "user",
    }


@pytest.fixture
def config_720h():
    set_config(NerveConfig(auth=AuthConfig(jwt_secret=_SECRET, jwt_expiry_hours=720)))
    yield
    set_config(NerveConfig())


def test_expiry_hours_follows_config(config_720h):
    assert session_expiry_hours() == 720


def test_config_expiry_is_floored_at_one_hour():
    # A nonsense value must not mint an already-dead token.
    set_config(NerveConfig(auth=AuthConfig(jwt_secret=_SECRET, jwt_expiry_hours=0)))
    try:
        assert session_expiry_hours() == 1
    finally:
        set_config(NerveConfig())


def test_token_lifetime_matches_configured_window(config_720h):
    payload = _decode(create_token(_SECRET))
    lifetime_hours = (payload["exp"] - payload["iat"]) / 3600
    assert lifetime_hours == pytest.approx(720, abs=0.01)


def test_explicit_expiry_overrides_config(config_720h):
    payload = _decode(create_token(_SECRET, expiry_hours=6))
    lifetime_hours = (payload["exp"] - payload["iat"]) / 3600
    assert lifetime_hours == pytest.approx(6, abs=0.01)


def test_fresh_token_is_not_refreshed(config_720h):
    # Well inside the first half of its life — no new token, no crypto.
    payload = _aged_payload(lifetime_hours=720, age_hours=1)
    assert maybe_refresh_token(payload, _SECRET) is None


def test_token_past_half_life_is_refreshed(config_720h):
    payload = _aged_payload(lifetime_hours=720, age_hours=400)
    refreshed = maybe_refresh_token(payload, _SECRET)
    assert refreshed is not None
    # The renewed token starts a fresh full window, so continuous use never
    # reaches the wall.
    new_payload = _decode(refreshed)
    assert new_payload["exp"] > payload["exp"]
    assert new_payload["sub"] == "user"


def test_continuous_use_keeps_pushing_the_deadline_out(config_720h):
    """Every refresh moves the wall further away than the token it replaced.

    Chained over a working life this is what makes the window an *idle*
    timeout: each request past half-life buys another full window, so the
    deadline only ever arrives for a tab that stops asking.
    """
    for _ in range(12):
        aged = _aged_payload(lifetime_hours=720, age_hours=400)
        refreshed = maybe_refresh_token(aged, _SECRET)
        assert refreshed is not None, "an active session must keep sliding"
        assert _decode(refreshed)["exp"] > aged["exp"]


def test_mcp_tokens_do_not_slide(config_720h):
    """Audience-scoped credentials are short-lived on purpose."""
    token = create_mcp_session_token(_SECRET, "sess1234", ttl_seconds=60)
    payload = jwt.decode(
        token, _SECRET, algorithms=[JWT_ALGORITHM], audience=MCP_AUDIENCE,
    )
    assert maybe_refresh_token(payload, _SECRET) is None


def test_malformed_payload_is_not_refreshed(config_720h):
    assert maybe_refresh_token({}, _SECRET) is None
    assert maybe_refresh_token({"sub": "user"}, _SECRET) is None
    # exp before iat — degenerate, must not be treated as "past half life".
    now = datetime.now(timezone.utc).timestamp()
    assert maybe_refresh_token(
        {"sub": "user", "iat": now, "exp": now - 10}, _SECRET,
    ) is None
