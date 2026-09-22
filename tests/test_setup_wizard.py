"""Mandatory-token first-account claim and session cutover."""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import jwt
import pytest
import pytest_asyncio
from fastapi import FastAPI

from nerve import setup_token
from nerve.config import AuthConfig, NerveConfig, set_config
from nerve.db.accounts import JWT_SECRET_NAME
from nerve.gateway.auth import (
    JWT_ALGORITHM,
    TOKEN_TYPE_CLAIM,
    TOKEN_TYPE_SESSION,
    create_session_token,
    hash_password,
    pin_jwt_secret,
    verify_password,
)
from nerve.gateway.routes import accounts as accounts_routes
from nerve.gateway.routes import auth as auth_routes
from nerve.gateway.routes import setup as setup_routes
from nerve.gateway import server

_SECRET = "test-secret-for-the-setup-claim-padded-32b"
_PASSWORD = "correct-horse-battery-staple"
_LOCAL = ("127.0.0.1", 41000)
_REMOTE = ("203.0.113.7", 41000)


@pytest.mark.asyncio
async def test_validation_errors_never_echo_the_setup_token():
    """A malformed claim response must not reflect its credential input."""
    token = "live-setup-token-that-must-not-appear"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=server.create_app()),
        base_url="http://nerve-test",
    ) as http:
        response = await http.post("/api/setup/claim", json={
            "password": _PASSWORD,
            "passwordless": "not-a-boolean",
            "setup_token": token,
        })
    assert response.status_code == 422
    assert token not in response.text
    assert all("input" not in error for error in response.json()["detail"])


def _legacy_token(secret: str = _SECRET) -> str:
    now = datetime.now(timezone.utc)
    return jwt.encode(
        {"iat": now, "exp": now + timedelta(hours=720), "sub": "user"},
        secret,
        algorithm=JWT_ALGORITHM,
    )


def _pre_epoch_session_token(account_id: str, secret: str = _SECRET) -> str:
    """A typed per-account token minted before the ``sep`` claim existed."""
    now = datetime.now(timezone.utc)
    return jwt.encode(
        {
            "iat": now,
            "exp": now + timedelta(hours=720),
            "sub": account_id,
            TOKEN_TYPE_CLAIM: TOKEN_TYPE_SESSION,
        },
        secret,
        algorithm=JWT_ALGORITHM,
    )


class _FakeSocket:
    def __init__(self, token: str):
        self.query_params = {"token": token}
        self.cookies: dict[str, str] = {}


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(setup_routes.router)
    app.include_router(accounts_routes.router)
    app.include_router(auth_routes.router)
    return app


def _client(
    app: FastAPI, *, token: str = "", client=_LOCAL,
) -> httpx.AsyncClient:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=client),
        base_url="http://nerve-test",
        headers=headers,
    )


class _Install:
    def __init__(self, db, identity, token: str):
        self.db = db
        self.identity = identity
        self.owner_id = identity.owner_account_id
        self.setup_token = token
        self.app = _app()

    async def owner_actor_id(self) -> str:
        account = await self.db.get_account(self.owner_id)
        return account["actor_id"]

    def session_token(self, account_id: str | None = None, epoch: int = 0) -> str:
        return create_session_token(
            _SECRET,
            account_id or self.owner_id,
            session_epoch=epoch,
        )


@pytest_asyncio.fixture
async def install(tmp_path, open_identity_db, wire_identity_store):
    config_dir = tmp_path / "config"
    workspace = tmp_path / "workspace"
    config_dir.mkdir()
    workspace.mkdir()
    set_config(NerveConfig(
        auth=AuthConfig(jwt_secret=_SECRET),
        config_dir=config_dir,
        workspace=workspace,
    ))
    pin_jwt_secret(_SECRET)
    database, identity = await open_identity_db(tmp_path / "nerve.db")
    wire_identity_store(database)
    token = await setup_token.ensure_setup_token(database, unclaimed=True)
    assert token
    try:
        yield _Install(database, identity, token)
    finally:
        await database.close()
        set_config(NerveConfig())


_UNSET = object()


async def _claim(
    install: _Install,
    *,
    username: str = "alice",
    password: str = _PASSWORD,
    display_name: str | None = None,
    token: str | None | object = _UNSET,
    client=_LOCAL,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    body = {"username": username, "password": password}
    supplied = install.setup_token if token is _UNSET else token
    if supplied is not None:
        body["setup_token"] = supplied
    if display_name is not None:
        body["display_name"] = display_name
    async with _client(install.app, client=client) as http:
        return await http.post(
            "/api/setup/claim",
            json=body,
            headers=headers or {},
        )


@pytest.mark.asyncio
class TestClaim:
    async def test_every_request_requires_a_nonempty_token(self, install):
        missing = await _claim(install, token=None)
        empty = await _claim(install, token="")
        wrong = await _claim(install, token="not-the-token")
        non_ascii = await _claim(install, token="é")
        assert missing.status_code == empty.status_code == 422
        assert wrong.status_code == non_ascii.status_code == 403
        account = await install.db.get_account(install.owner_id)
        assert account["credential_source"] == "none"

    async def test_valid_token_claims_from_any_peer_or_page(self, install):
        response = await _claim(
            install,
            client=_REMOTE,
            headers={
                "Origin": "https://elsewhere.invalid",
                "Host": "rebound.invalid",
                "Sec-Fetch-Site": "cross-site",
                "X-Forwarded-For": "127.0.0.1",
            },
        )
        assert response.status_code == 200, response.text
        assert set(response.json()) == {"token"}
        account = await install.db.get_account(install.owner_id)
        assert account["username"] == "alice"
        assert verify_password(_PASSWORD, account["credential"])

    async def test_the_returned_client_session_uses_canonical_me(self, install):
        token = (await _claim(install)).json()["token"]
        async with _client(install.app, token=token) as http:
            checked = await http.get("/api/auth/check")
            me = await http.get("/api/accounts/me")
        assert checked.status_code == 200
        assert me.status_code == 200
        assert me.json()["username"] == "alice"

    async def test_optional_display_name_keeps_the_existing_identity(self, install):
        actor_id = await install.owner_actor_id()
        response = await _claim(install, display_name="  Alice Example  ")
        assert response.status_code == 200
        account = await install.db.get_account(install.owner_id)
        actor = await install.db.get_actor_ref(actor_id)
        assert account["actor_id"] == actor_id
        assert actor["display_name"] == "Alice Example"
        assert await install.db.count_accounts() == 1

    async def test_token_is_invalidated_and_never_returned_or_logged(
        self, install, caplog,
    ):
        token = install.setup_token
        with caplog.at_level("INFO"):
            response = await _claim(install)
        assert response.status_code == 200
        assert await setup_token.stored_setup_token(install.db) == ""
        assert token not in response.text
        assert token not in caplog.text

    async def test_token_invalidation_rolls_the_claim_back(self, install):
        """Roll back the account claim if setup-token deletion fails."""
        await install.db.db.execute(
            """CREATE TRIGGER refuse_setup_token_delete
                 BEFORE DELETE ON instance_secrets
                   WHEN OLD.name = 'setup_token'
                 BEGIN
                   SELECT RAISE(ABORT, 'injected setup-token delete failure');
                 END"""
        )
        await install.db.db.commit()

        with pytest.raises(sqlite3.IntegrityError, match="injected setup-token"):
            await install.db.claim_sole_account(
                username="alice",
                credential=hash_password(_PASSWORD),
                invalidate_secret_name=setup_token.SETUP_TOKEN_NAME,
            )

        account = await install.db.get_account(install.owner_id)
        assert account["credential_source"] == "none"
        assert account["username"] is None
        assert await setup_token.stored_setup_token(install.db) == install.setup_token

    async def test_second_claim_is_refused_without_changing_the_owner(self, install):
        assert (await _claim(install)).status_code == 200
        second = await _claim(install, username="bob")
        # The invalidated credential is checked before claimability.
        assert second.status_code == 403
        account = await install.db.get_account(install.owner_id)
        assert account["username"] == "alice"
        assert verify_password(_PASSWORD, account["credential"])

    async def test_two_claims_have_exactly_one_winner(self, install):
        body = {
            "username": "alice",
            "password": _PASSWORD,
            "setup_token": install.setup_token,
        }
        other = {
            "username": "bob",
            "password": "a-different-passphrase",
            "setup_token": install.setup_token,
        }
        async with _client(install.app) as http:
            first, second = await asyncio.gather(
                http.post("/api/setup/claim", json=body),
                http.post("/api/setup/claim", json=other),
            )
        statuses = [first.status_code, second.status_code]
        assert statuses.count(200) == 1
        # The loser either saw the claimed row inside the CAS transaction, or
        # arrived after that transaction atomically retired the bearer.
        assert next(code for code in statuses if code != 200) in {403, 409}
        account = await install.db.get_account(install.owner_id)
        winner = "alice" if first.status_code == 200 else "bob"
        assert account["username"] == winner

    async def test_password_and_username_validation_leave_it_unclaimed(self, install):
        too_long = await _claim(install, password="x" * 200)
        bad_name = await _claim(install, username="admin")
        assert too_long.status_code == 400
        assert "72" in too_long.json()["detail"]
        assert "bytes" in too_long.json()["detail"]
        assert bad_name.status_code == 400
        assert await setup_token.instance_is_unclaimed(
            install.db,
            NerveConfig(auth=AuthConfig(jwt_secret=_SECRET)),
        )

    async def test_configured_password_cannot_be_claimed(self, install):
        set_config(NerveConfig(
            auth=AuthConfig(
                jwt_secret=_SECRET,
                password_hash=hash_password(_PASSWORD),
            ),
        ))
        response = await _claim(install)
        assert response.status_code == 409
        account = await install.db.get_account(install.owner_id)
        assert account["credential_source"] == "none"

    async def test_missing_stored_token_fails_closed(self, install):
        # Exceptional raw state: production only deletes through claim or the
        # startup lifecycle, both of which are covered separately.
        await install.db._delete_instance_secret(setup_token.SETUP_TOKEN_NAME)
        response = await _claim(install)
        assert response.status_code == 403

    async def test_socket_cleanup_is_best_effort(self, install, monkeypatch):
        import nerve.gateway.server as server

        async def explode():
            raise RuntimeError("already gone")

        monkeypatch.setattr(server, "close_revoked_sockets", explode)
        response = await _claim(install)
        assert response.status_code == 200
        assert await setup_token.stored_setup_token(install.db) == ""


@pytest.mark.asyncio
class TestSessionEpoch:
    async def test_preclaim_http_and_new_websocket_sessions_are_revoked(
        self, install,
    ):
        before = install.session_token()
        from nerve.gateway.auth import authenticate_websocket

        assert await authenticate_websocket(_FakeSocket(before)) is not None
        claimed = (await _claim(install)).json()["token"]

        async with _client(install.app, token=before) as http:
            assert (await http.get("/api/auth/check")).status_code == 401
        assert await authenticate_websocket(_FakeSocket(before)) is None
        async with _client(install.app, token=claimed) as http:
            assert (await http.get("/api/accounts/me")).status_code == 200

    async def test_a_token_from_any_other_epoch_is_refused(self, install):
        # Reject tokens minted against older or future account state, including
        # after a database restore or rollback.
        future = install.session_token(epoch=1)
        async with _client(install.app, token=future) as http:
            assert (await http.get("/api/auth/check")).status_code == 401

    async def test_legacy_tokens_survive_upgrade_then_die_at_claim(self, install):
        legacy = _legacy_token()
        async with _client(install.app, token=legacy) as http:
            assert (await http.get("/api/auth/check")).status_code == 200
        await _claim(install)
        async with _client(install.app, token=legacy) as http:
            assert (await http.get("/api/auth/check")).status_code == 401

    async def test_pre_epoch_account_tokens_survive_upgrade_then_die_at_claim(
        self, install,
    ):
        token = _pre_epoch_session_token(install.owner_id)
        async with _client(install.app, token=token) as http:
            assert (await http.get("/api/auth/check")).status_code == 200
        await _claim(install)
        async with _client(install.app, token=token) as http:
            assert (await http.get("/api/auth/check")).status_code == 401

    async def test_epoch_moves_on_claim_and_password_change(self, install):
        async def epoch() -> int:
            account = await install.db.get_account(install.owner_id)
            return int(account["session_epoch"])

        assert await epoch() == 0
        claimed = await _claim(install)
        assert claimed.status_code == 200
        assert await epoch() == 1

        async with _client(
            install.app,
            token=claimed.json()["token"],
        ) as http:
            changed = await http.put("/api/accounts/me/password", json={
                "current_password": _PASSWORD,
                "new_password": "a-third-passphrase",
            })
        assert changed.status_code == 200
        assert await epoch() == 2


@pytest.mark.asyncio
class TestStaleHttpWrites:
    async def _claim_at(
        self, install, monkeypatch, seam: str, **claim_kwargs,
    ):
        fired: list[bool] = []
        original = getattr(install.db, seam)

        async def claim_then(*args, **kwargs):
            if not fired:
                fired.append(True)
                response = await _claim(install, **claim_kwargs)
                assert response.status_code == 200, response.text
            return await original(*args, **kwargs)

        monkeypatch.setattr(install.db, seam, claim_then)
        return fired

    async def test_password_change_cannot_outlive_claim(
        self, install, monkeypatch,
    ):
        visitor = install.session_token()
        fired = await self._claim_at(install, monkeypatch, "login_state")
        async with _client(install.app, token=visitor) as http:
            response = await http.put(
                "/api/accounts/me/password",
                json={"new_password": "taken-over"},
            )
        assert fired
        assert response.status_code in (401, 409)
        account = await install.db.get_account(install.owner_id)
        assert verify_password(_PASSWORD, account["credential"])
        assert not verify_password("taken-over", account["credential"])

    async def test_account_creation_cannot_outlive_claim(
        self, install, monkeypatch,
    ):
        visitor = install.session_token()
        fired = await self._claim_at(
            install, monkeypatch, "create_managed_account",
        )
        async with _client(install.app, token=visitor) as http:
            response = await http.post("/api/accounts", json={
                "username": "mallory",
                "password": "a-second-way-in",
            })
        assert fired
        assert response.status_code in (401, 409)
        assert await install.db.get_account_by_username("mallory") is None

    async def test_disable_cannot_outlive_claim(self, install, monkeypatch):
        visitor = install.session_token()
        fired = await self._claim_at(install, monkeypatch, "disable_account")
        async with _client(install.app, token=visitor) as http:
            response = await http.post(
                f"/api/accounts/{install.owner_id}/disable",
            )
        assert fired
        assert response.status_code in (401, 409)
        assert (await install.db.get_account(install.owner_id))["enabled"] is True

    async def test_display_name_patch_cannot_outlive_claim(
        self, install, monkeypatch,
    ):
        visitor = install.session_token()
        fired = await self._claim_at(
            install,
            monkeypatch,
            "login_state",
            display_name="Alice Example",
        )
        async with _client(install.app, token=visitor) as http:
            response = await http.patch(
                f"/api/accounts/{install.owner_id}",
                json={"display_name": "Mallory"},
            )
        assert fired
        assert response.status_code in (401, 409)
        actor = await install.db.get_actor_ref(await install.owner_actor_id())
        assert actor["display_name"] == "Alice Example"


@pytest.mark.asyncio
class TestOnlyClaimSetsTheFirstPassword:
    async def test_password_endpoint_refuses_while_unclaimed(self, install):
        async with _client(
            install.app,
            token=install.session_token(),
        ) as http:
            response = await http.put(
                "/api/accounts/me/password",
                json={"new_password": "taken-over"},
            )
        assert response.status_code == 409
        assert "/api/setup/claim" in response.json()["detail"]
        assert not (await install.db.get_account(install.owner_id))["credential"]

    async def test_it_requires_the_current_password_after_claim(self, install):
        claimed = (await _claim(install)).json()["token"]
        async with _client(install.app, token=claimed) as http:
            refused = await http.put(
                "/api/accounts/me/password",
                json={"new_password": "next-one"},
            )
            accepted = await http.put("/api/accounts/me/password", json={
                "current_password": _PASSWORD,
                "new_password": "next-one-please",
            })
        assert refused.status_code == 403
        assert accepted.status_code == 200
        account = await install.db.get_account(install.owner_id)
        assert verify_password("next-one-please", account["credential"])

    async def test_claim_does_not_rotate_the_signing_secret(self, install):
        before = await install.db._get_instance_secret(JWT_SECRET_NAME)
        response = await _claim(install)
        after = await install.db._get_instance_secret(JWT_SECRET_NAME)
        assert response.status_code == 200
        assert after == before


async def _passwordless_claim(install: _Install, **extra) -> httpx.Response:
    body = {"passwordless": True, "setup_token": install.setup_token, **extra}
    async with _client(install.app) as http:
        return await http.post("/api/setup/claim", json=body)


async def _status(install: _Install) -> dict:
    async with _client(install.app) as http:
        return (await http.get("/api/auth/status")).json()


@pytest.mark.asyncio
class TestSetupState:
    async def test_setup_required_refuses_login_and_reports_setup(self, install):
        assert await _status(install) == {"auth_required": True, "login": "setup"}
        async with _client(install.app) as http:
            response = await http.post("/api/auth/login", json={"password": ""})
        assert response.status_code == 409
        assert "setup token" in response.json()["detail"]

    async def test_passwordless_claim_completes_setup_without_a_password(
        self, install,
    ):
        response = await _passwordless_claim(install)
        assert response.status_code == 200, response.text
        claimed = response.json()["token"]

        account = await install.db.get_account(install.owner_id)
        assert account["credential_source"] == "none"
        assert account["username"] is None
        assert account["session_epoch"] == 1
        assert await install.db.setup_completed()
        assert await setup_token.stored_setup_token(install.db) == ""
        assert await _status(install) == {"auth_required": False, "login": "none"}

        async with _client(install.app, token=claimed) as http:
            assert (await http.get("/api/accounts/me")).status_code == 200
        async with _client(install.app) as http:
            login = await http.post("/api/auth/login", json={"password": ""})
        assert login.status_code == 200

    async def test_passwordless_claim_may_set_a_username(self, install):
        response = await _passwordless_claim(install, username="Alice")
        assert response.status_code == 200, response.text
        account = await install.db.get_account(install.owner_id)
        assert account["username"] == "alice"

    @pytest.mark.parametrize("body", [
        {"passwordless": True, "password": _PASSWORD},
        {"username": "alice"},
    ])
    async def test_claim_needs_exactly_one_of_password_and_passwordless(
        self, install, body,
    ):
        body = {**body, "setup_token": install.setup_token}
        async with _client(install.app) as http:
            response = await http.post("/api/setup/claim", json=body)
        assert response.status_code == 400
        assert not await install.db.setup_completed()
        assert await setup_token.stored_setup_token(install.db) == install.setup_token

    async def test_a_passwordless_setup_cannot_be_claimed_again(self, install):
        assert (await _passwordless_claim(install)).status_code == 200
        # The token is gone, so a replay fails on the token check.
        assert (await _passwordless_claim(install)).status_code == 403
        # Even with a token back in place, the store refuses the claim.
        await install.db._ensure_instance_secret(
            setup_token.SETUP_TOKEN_NAME, install.setup_token,
        )
        assert (await _claim(install)).status_code == 409
        account = await install.db.get_account(install.owner_id)
        assert account["credential_source"] == "none"

    async def test_a_password_can_be_added_after_a_passwordless_setup(
        self, install,
    ):
        claimed = (await _passwordless_claim(install)).json()["token"]
        async with _client(install.app, token=claimed) as http:
            response = await http.put(
                "/api/accounts/me/password", json={"new_password": _PASSWORD},
            )
        assert response.status_code == 200, response.text
        assert await _status(install) == {"auth_required": True, "login": "password"}

    async def test_password_claim_records_setup_complete(self, install):
        assert (await _claim(install)).status_code == 200
        assert await install.db.setup_completed()

    async def test_a_configured_password_means_setup_is_not_required(
        self, install,
    ):
        set_config(NerveConfig(auth=AuthConfig(
            jwt_secret=_SECRET, password_hash=hash_password(_PASSWORD),
        )))
        assert await _status(install) == {"auth_required": True, "login": "password"}


@pytest.mark.asyncio
class TestInstallerPasswordlessChoice:
    async def test_it_records_setup_and_deletes_the_token(self, install):
        assert await install.db.complete_passwordless_setup(
            invalidate_secret_name=setup_token.SETUP_TOKEN_NAME,
        )
        assert await install.db.setup_completed()
        assert await setup_token.stored_setup_token(install.db) == ""

    async def test_it_does_nothing_to_an_account_with_a_password(self, install):
        assert (await _claim(install)).status_code == 200
        await install.db.db.execute("DELETE FROM instance_setup")
        await install.db.db.commit()
        assert not await install.db.complete_passwordless_setup()
        assert not await install.db.setup_completed()
        account = await install.db.get_account(install.owner_id)
        assert verify_password(_PASSWORD, account["credential"])
