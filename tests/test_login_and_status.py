"""Logging in with a username, and what an anonymous caller is told.

Three rules are bounded by "exactly one account exists": grandfathered
``sub: "user"`` tokens, password-only login, and passwordless access. They read
one predicate, so this file checks them together — in particular at the moment
a second account is created, which is when all three change at once.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import jwt
import pytest
import pytest_asyncio
from fastapi import FastAPI

from nerve.config import AuthConfig, NerveConfig, set_config
from nerve.gateway.auth import (
    JWT_ALGORITHM,
    hash_password,
    pin_jwt_secret,
)
from nerve.gateway.routes import accounts as accounts_routes
from nerve.gateway.routes import auth as auth_routes

_SECRET = "test-secret-for-login-and-status-padded32b"
_PASSWORD = "correct-horse-battery-staple"
_OTHER_PASSWORD = "a-different-passphrase-entirely"


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(auth_routes.router)
    app.include_router(accounts_routes.router)
    return app


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://nerve-test",
    )


def _legacy_token(secret: str = _SECRET) -> str:
    """What a browser that logged in before per-account sessions is holding."""
    now = datetime.now(timezone.utc)
    return jwt.encode(
        {"iat": now, "exp": now + timedelta(hours=720), "sub": "user"},
        secret, algorithm=JWT_ALGORITHM,
    )


class _Install:
    def __init__(self, db, identity, app):
        self.db = db
        self.identity = identity
        self.app = app
        self.owner_id = identity.owner_account_id

    async def secure_the_owner(self, username="alice", password=_PASSWORD):
        await self.db.update_account_login(
            self.owner_id, username=username, credential=hash_password(password),
        )

    async def add_account(self, username="bob", password=_OTHER_PASSWORD):
        return await self.db.create_managed_account(
            username=username, credential=hash_password(password),
        )


@pytest_asyncio.fixture
async def install(tmp_path, open_identity_db, wire_identity_store):
    set_config(NerveConfig(auth=AuthConfig(jwt_secret=_SECRET)))
    pin_jwt_secret(_SECRET)
    database, identity = await open_identity_db(tmp_path / "nerve.db")
    wire_identity_store(database)
    try:
        yield _Install(database, identity, _app())
    finally:
        await database.close()
        set_config(NerveConfig())


# --------------------------------------------------------------------------- #
#  One account: a username is optional                                         #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestSingleAccountLogin:
    async def test_password_only_login_still_works_after_an_upgrade(self, install):
        """The account an upgrade creates has no username. Demanding one here
        would lock out every install that upgrades, so password-only login is
        valid while exactly one account exists."""
        await install.db.set_account_credential(
            install.owner_id, credential_source="local",
            credential=hash_password(_PASSWORD),
        )
        async with _client(install.app) as client:
            good = await client.post("/api/auth/login", json={"password": _PASSWORD})
            bad = await client.post("/api/auth/login", json={"password": "nope"})
        assert good.status_code == 200 and good.json()["token"]
        assert bad.status_code == 401

    async def test_a_configured_password_still_authenticates(self, install):
        """``credential_source = 'config'`` stays readable for one release, so a
        downgrade to code that only knows ``auth.password_hash`` keeps working."""
        set_config(NerveConfig(auth=AuthConfig(
            jwt_secret=_SECRET, password_hash=hash_password(_PASSWORD),
        )))
        await install.db.set_account_credential(
            install.owner_id, credential_source="config",
        )
        async with _client(install.app) as client:
            assert (await client.post(
                "/api/auth/login", json={"password": _PASSWORD},
            )).status_code == 200
            assert (await client.post(
                "/api/auth/login", json={"password": "nope"},
            )).status_code == 401

    async def test_passwordless_admits_anything(self, install):
        async with _client(install.app) as client:
            response = await client.post(
                "/api/auth/login", json={"password": "anything at all"},
            )
        assert response.status_code == 200

    async def test_a_username_may_be_supplied_once_the_account_has_one(self, install):
        await install.secure_the_owner("alice")
        async with _client(install.app) as client:
            named = await client.post(
                "/api/auth/login", json={"username": "ALICE", "password": _PASSWORD},
            )
            unnamed = await client.post("/api/auth/login", json={"password": _PASSWORD})
            wrong_name = await client.post(
                "/api/auth/login", json={"username": "bob", "password": _PASSWORD},
            )
        assert named.status_code == 200
        assert unnamed.status_code == 200
        assert wrong_name.status_code == 401

    async def test_a_disabled_sole_account_cannot_log_in(self, install):
        await install.secure_the_owner()
        await install.db.set_account_enabled(install.owner_id, False)
        async with _client(install.app) as client:
            response = await client.post(
                "/api/auth/login", json={"username": "alice", "password": _PASSWORD},
            )
        assert response.status_code == 401
        assert "disabled" in response.json()["detail"].lower()


# --------------------------------------------------------------------------- #
#  Two accounts: a username is required, and nothing is enumerable             #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestMultiAccountLogin:
    @pytest_asyncio.fixture(autouse=True)
    async def _two_accounts(self, install):
        await install.secure_the_owner("alice", _PASSWORD)
        self.bob = await install.add_account("bob", _OTHER_PASSWORD)

    async def test_each_account_gets_its_own_token(self, install):
        async with _client(install.app) as client:
            alice = await client.post(
                "/api/auth/login", json={"username": "alice", "password": _PASSWORD},
            )
            bob = await client.post(
                "/api/auth/login",
                json={"username": "bob", "password": _OTHER_PASSWORD},
            )
        assert alice.status_code == bob.status_code == 200
        claims = [
            jwt.decode(r.json()["token"], _SECRET, algorithms=[JWT_ALGORITHM])
            for r in (alice, bob)
        ]
        assert claims[0]["sub"] == install.owner_id
        assert claims[1]["sub"] == self.bob["id"]

    async def test_a_password_alone_no_longer_names_anybody(self, install):
        async with _client(install.app) as client:
            response = await client.post("/api/auth/login", json={"password": _PASSWORD})
        assert response.status_code == 401

    async def test_one_accounts_password_does_not_work_for_another(self, install):
        async with _client(install.app) as client:
            response = await client.post(
                "/api/auth/login",
                json={"username": "bob", "password": _PASSWORD},
            )
        assert response.status_code == 401

    async def test_an_unknown_username_costs_the_same_comparison_as_a_known_one(
        self, install,
    ):
        """One bcrypt comparison either way, on every request including the
        first one a process serves. The decoy is a constant precisely so that
        the first unknown-username attempt does not also pay for a hash."""
        from nerve.gateway.routes import auth as auth_routes

        calls: list[str] = []
        real = auth_routes.verify_password

        def counting(plain, hashed):
            calls.append(hashed)
            return real(plain, hashed)

        auth_routes.verify_password = counting
        try:
            async with _client(install.app) as client:
                await client.post(
                    "/api/auth/login",
                    json={"username": "nobody-here", "password": _PASSWORD},
                )
                unknown = list(calls)
                calls.clear()
                await client.post(
                    "/api/auth/login",
                    json={"username": "alice", "password": "wrong"},
                )
                known = list(calls)
        finally:
            auth_routes.verify_password = real

        assert len(unknown) == len(known) == 1
        assert unknown[0] == auth_routes._DECOY_HASH
        # ...and the decoy really is a usable bcrypt hash, or the comparison it
        # is there to pay for would be skipped.
        assert auth_routes._DECOY_HASH.startswith("$2b$12$")
        assert real("anything at all", auth_routes._DECOY_HASH) is False

    async def test_wrong_username_and_wrong_password_are_indistinguishable(self, install):
        async with _client(install.app) as client:
            no_such_user = await client.post(
                "/api/auth/login",
                json={"username": "nobody-here", "password": _PASSWORD},
            )
            wrong_password = await client.post(
                "/api/auth/login",
                json={"username": "alice", "password": "not-the-password"},
            )
            no_username = await client.post(
                "/api/auth/login", json={"password": _PASSWORD},
            )
        assert no_such_user.status_code == wrong_password.status_code == 401
        assert no_such_user.json() == wrong_password.json() == no_username.json()
        assert "username or password" in no_such_user.json()["detail"].lower()
        # ...and the message names neither of them.
        for response in (no_such_user, wrong_password):
            body = response.text.lower()
            assert "alice" not in body and "nobody-here" not in body

    async def test_a_disabled_account_is_refused_after_its_password_checks_out(
        self, install,
    ):
        """Order matters: the credential is verified first, so the "disabled"
        answer only ever reaches somebody who already knew the password."""
        await install.db.set_account_enabled(self.bob["id"], False)
        async with _client(install.app) as client:
            right = await client.post(
                "/api/auth/login",
                json={"username": "bob", "password": _OTHER_PASSWORD},
            )
            wrong = await client.post(
                "/api/auth/login", json={"username": "bob", "password": "guess"},
            )
        assert right.status_code == 401
        assert "disabled" in right.json()["detail"].lower()
        assert wrong.json()["detail"] == "Invalid username or password"

    async def test_an_account_left_without_a_password_cannot_log_in(self, install):
        """Passwordless is bounded to one account. With two, an account with no
        credential authenticates nobody rather than everybody — the state a
        restored `--no-secrets` bundle leaves behind."""
        await install.db.set_account_credential(
            self.bob["id"], credential_source="none",
        )
        async with _client(install.app) as client:
            for body in (
                {"username": "bob", "password": ""},
                {"username": "bob", "password": "anything"},
            ):
                response = await client.post("/api/auth/login", json=body)
                assert response.status_code == 401, body


# --------------------------------------------------------------------------- #
#  The second account changes three things at once                             #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestTheSecondAccountIsTheTurningPoint:
    async def test_all_three_relaxations_end_together(self, install):
        await install.secure_the_owner("alice")
        legacy = _legacy_token()
        headers = {"Authorization": f"Bearer {legacy}"}

        async with _client(install.app) as client:
            # Before: a legacy tab works, a password alone logs in.
            assert (await client.get(
                "/api/auth/check", headers=headers,
            )).status_code == 200
            assert (await client.post(
                "/api/auth/login", json={"password": _PASSWORD},
            )).status_code == 200
            before = (await client.get("/api/auth/status")).json()
            assert before["login"] == "password"
            assert before["multiple_accounts"] is False

            await install.add_account("bob")

            # After: the same token names nobody, and so does a bare password.
            assert (await client.get(
                "/api/auth/check", headers=headers,
            )).status_code == 401
            assert (await client.post(
                "/api/auth/login", json={"password": _PASSWORD},
            )).status_code == 401
            after = (await client.get("/api/auth/status")).json()
            assert after["login"] == "username_password"
            assert after["multiple_accounts"] is True

    async def test_disabling_the_second_account_does_not_bring_them_back(self, install):
        """The row is the tombstone: the relaxations are keyed on how many
        accounts *exist*, so this is a one-way door."""
        await install.secure_the_owner("alice")
        bob = await install.add_account("bob")
        await install.db.disable_account(bob["id"])
        async with _client(install.app) as client:
            assert (await client.post(
                "/api/auth/login", json={"password": _PASSWORD},
            )).status_code == 401
            assert (await client.get(
                "/api/auth/check",
                headers={"Authorization": f"Bearer {_legacy_token()}"},
            )).status_code == 401
            assert (await client.get("/api/auth/status")).json()["login"] == (
                "username_password"
            )


# --------------------------------------------------------------------------- #
#  The status descriptor                                                       #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestStatusDescriptor:
    async def test_setup_pending(self, install):
        """One account, no password, no username: what a fresh headless install
        looks like, and the state PR 6's wizard claims."""
        async with _client(install.app) as client:
            body = (await client.get("/api/auth/status")).json()
        assert body == {
            "auth_required": False, "mode": "local", "login": "none",
            "setup_pending": True, "multiple_accounts": False,
        }

    async def test_naming_the_account_does_not_end_setup(self, install):
        """The accounts screen can set a username on its own. Doing that first
        must not stop the instance reporting as unsecured — it still admits
        every caller, which is the state the wizard exists to end."""
        await install.db.set_account_username(install.owner_id, "alice")
        async with _client(install.app) as client:
            body = (await client.get("/api/auth/status")).json()
        assert body["login"] == "none"
        assert body["setup_pending"] is True

    async def test_only_a_password_ends_setup(self, install):
        await install.db.set_account_username(install.owner_id, "alice")
        await install.db.update_account_login(
            install.owner_id, credential=hash_password(_PASSWORD),
        )
        async with _client(install.app) as client:
            body = (await client.get("/api/auth/status")).json()
        assert body["login"] == "password"
        assert body["setup_pending"] is False

    async def test_password_only(self, install):
        await install.secure_the_owner("alice")
        async with _client(install.app) as client:
            body = (await client.get("/api/auth/status")).json()
        assert body == {
            "auth_required": True, "mode": "local", "login": "password",
            "setup_pending": False, "multiple_accounts": False,
        }

    async def test_username_and_password(self, install):
        await install.secure_the_owner("alice")
        await install.add_account("bob")
        async with _client(install.app) as client:
            body = (await client.get("/api/auth/status")).json()
        assert body == {
            "auth_required": True, "mode": "local", "login": "username_password",
            "setup_pending": False, "multiple_accounts": True,
        }

    async def test_a_configured_password_is_not_passwordless(self, install):
        """The row still says `none` — a reload added the hash and no restart
        has re-derived it yet — but the instance is not passwordless, and the
        descriptor must not tell a browser to log itself in with nothing."""
        set_config(NerveConfig(auth=AuthConfig(
            jwt_secret=_SECRET, password_hash=hash_password(_PASSWORD),
        )))
        async with _client(install.app) as client:
            body = (await client.get("/api/auth/status")).json()
            assert body["login"] == "password"
            assert body["setup_pending"] is False
            # ...and the two agree: the empty password a `none` descriptor
            # would have invited is refused.
            assert (await client.post(
                "/api/auth/login", json={"password": ""},
            )).status_code == 401
            assert (await client.post(
                "/api/auth/login", json={"password": _PASSWORD},
            )).status_code == 200

    async def test_it_names_nobody(self, install):
        await install.secure_the_owner("alice")
        await install.add_account("bob")
        async with _client(install.app) as client:
            body = (await client.get("/api/auth/status")).text.lower()
        assert "alice" not in body and "bob" not in body
        # ...and does not publish how many people work here.
        assert "2" not in body

    async def test_needs_no_token(self, install):
        async with _client(install.app) as client:
            assert (await client.get("/api/auth/status")).status_code == 200

    async def test_fails_closed_with_no_signing_secret(self, install, monkeypatch):
        from nerve.gateway import auth as gw_auth

        monkeypatch.setattr(gw_auth, "_pinned_jwt_secret", "")
        set_config(NerveConfig(auth=AuthConfig(jwt_secret="")))
        async with _client(install.app) as client:
            body = (await client.get("/api/auth/status")).json()
        assert body["login"] == "username_password"
        assert body["auth_required"] is True
        assert body["setup_pending"] is False

    async def test_fails_closed_with_no_identity_store(self, install, monkeypatch):
        from nerve.gateway.routes import _deps as deps_module

        monkeypatch.setattr(deps_module, "_deps", None)
        async with _client(install.app) as client:
            body = (await client.get("/api/auth/status")).json()
            login = await client.post("/api/auth/login", json={"password": ""})
        assert body["login"] == "username_password"
        assert body["auth_required"] is True
        assert login.status_code == 503
