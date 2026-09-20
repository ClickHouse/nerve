"""Tests for sliding web-session tokens.

The gateway used to mint a fixed 24h token with no refresh path, so an
actively-used browser tab was logged out exactly one day after login — mid
work, with no warning. Session tokens now carry a configurable lifetime
(``auth.jwt_expiry_hours``) and are re-minted once past half of it, which
turns the window into an idle timeout.

Covers: the configured TTL is honoured, a fresh token is left alone, a
half-spent one is renewed, and short-lived audience-scoped MCP tokens never
slide. What decides is the ``typ`` claim: the
gate used to read ``sub``, which stopped meaning anything the moment ``sub``
became an account id.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import jwt
import pytest

from nerve.config import AuthConfig, NerveConfig, set_config
from nerve.gateway.auth import (
    JWT_ALGORITHM,
    MCP_AUDIENCE,
    TOKEN_TYPE_CLAIM,
    TOKEN_TYPE_SESSION,
    create_mcp_session_token,
    create_session_token,
    create_system_token,
    maybe_refresh_token,
    session_expiry_hours,
)

_SECRET = "test-secret-for-session-sliding-padded-to-32-bytes"
_ACCOUNT = "11111111-1111-4111-8111-111111111111"


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
        "sub": _ACCOUNT,
        TOKEN_TYPE_CLAIM: TOKEN_TYPE_SESSION,
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
    payload = _decode(create_session_token(_SECRET, _ACCOUNT))
    lifetime_hours = (payload["exp"] - payload["iat"]) / 3600
    assert lifetime_hours == pytest.approx(720, abs=0.01)


def test_explicit_expiry_overrides_config(config_720h):
    payload = _decode(create_session_token(_SECRET, _ACCOUNT, expiry_hours=6))
    lifetime_hours = (payload["exp"] - payload["iat"]) / 3600
    assert lifetime_hours == pytest.approx(6, abs=0.01)


def test_a_session_token_names_its_account(config_720h):
    """``sub`` is the account id and ``typ`` says what the token is. Both are
    what the request path reads — nothing infers a person from the subject."""
    payload = _decode(create_session_token(_SECRET, _ACCOUNT))
    assert payload["sub"] == _ACCOUNT
    assert payload[TOKEN_TYPE_CLAIM] == TOKEN_TYPE_SESSION
    with pytest.raises(ValueError):
        create_session_token(_SECRET, "")


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
    assert new_payload["sub"] == _ACCOUNT
    assert new_payload[TOKEN_TYPE_CLAIM] == TOKEN_TYPE_SESSION


def test_mcp_tokens_do_not_slide(config_720h):
    """Audience-scoped credentials are short-lived on purpose."""
    token = create_mcp_session_token(_SECRET, "sess1234", ttl_seconds=60)
    payload = jwt.decode(
        token, _SECRET, algorithms=[JWT_ALGORITHM], audience=MCP_AUDIENCE,
    )
    assert maybe_refresh_token(payload, _SECRET) is None


def test_system_tokens_do_not_slide(config_720h):
    """The CLI and the agent's own API calls mint one per request; there is
    nothing to keep alive, and nothing reads the response header."""
    payload = _decode(create_system_token(_SECRET, ttl_seconds=60))
    assert maybe_refresh_token(payload, _SECRET) is None
    # Even well past half its life.
    aged = dict(payload)
    aged["iat"] = datetime.now(timezone.utc).timestamp() - 3600
    assert maybe_refresh_token(aged, _SECRET) is None


def test_legacy_tokens_do_not_slide_here(config_720h):
    """A pre-account token carries no ``typ``, so the refresh gate passes on
    it. ``require_auth`` upgrades it instead — see test_request_actor.py."""
    iat = datetime.now(timezone.utc) - timedelta(hours=400)
    legacy = {
        "iat": iat.timestamp(),
        "exp": (iat + timedelta(hours=720)).timestamp(),
        "sub": "user",
    }
    assert maybe_refresh_token(legacy, _SECRET) is None


def test_malformed_payload_is_not_refreshed(config_720h):
    assert maybe_refresh_token({}, _SECRET) is None
    assert maybe_refresh_token(
        {"sub": _ACCOUNT, TOKEN_TYPE_CLAIM: TOKEN_TYPE_SESSION}, _SECRET,
    ) is None
    # exp before iat — degenerate, must not be treated as "past half life".
    now = datetime.now(timezone.utc).timestamp()
    assert maybe_refresh_token(
        {"sub": _ACCOUNT, TOKEN_TYPE_CLAIM: TOKEN_TYPE_SESSION,
         "iat": now, "exp": now - 10},
        _SECRET,
    ) is None
    # A session claim with no account anywhere mints nothing.
    assert maybe_refresh_token(
        _aged_payload(lifetime_hours=720, age_hours=400) | {"sub": ""}, _SECRET,
    ) is None
