"""Logging in with a username, and what an anonymous caller is told.

Three rules are bounded by "exactly one account exists": grandfathered
``sub: "user"`` tokens, password-only login, and passwordless access. They read
one predicate, so this file checks them together — in particular at the moment
a second account is created, which is when all three change at once.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import bcrypt
import httpx
import jwt
import pytest
import pytest_asyncio
from fastapi import FastAPI

from nerve.config import AuthConfig, NerveConfig, set_config
from nerve.gateway.auth import (
    BCRYPT_COST,
    JWT_ALGORITHM,
    bcrypt_cost,
    hash_password,
    pin_jwt_secret,
    verify_password,
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

    async def set_credential(
        self,
        account_id: str,
        *,
        credential_source: str,
        credential: str | None = None,
    ) -> None:
        """Seed a credential shape without exposing a fixture-only DAL method."""
        await self.db._write(
            "UPDATE accounts SET credential_source = ?, credential = ? WHERE id = ?",
            (credential_source, credential, account_id),
        )

    async def set_enabled(self, account_id: str, enabled: bool) -> None:
        """Create disabled test state without bypassing the production guard."""
        await self.db._write(
            "UPDATE accounts SET enabled = ? WHERE id = ?",
            (1 if enabled else 0, account_id),
        )


@pytest.fixture(autouse=True)
def _fast_failures(monkeypatch):
    """Skip timing padding except in tests that explicitly reset it."""
    monkeypatch.setattr(auth_routes, "_FAILURE_BUDGET_FLOOR_SECONDS", 0.0)
    auth_routes._failure_budget = 0.0
    auth_routes._calibrated_budget = 0.0
    auth_routes._policy_comparison_seconds = 0.0
    yield
    auth_routes._failure_budget = None
    auth_routes._calibrated_budget = None
    auth_routes._policy_comparison_seconds = None


def _reset_timing() -> None:
    auth_routes._failure_budget = None
    auth_routes._calibrated_budget = None
    auth_routes._policy_comparison_seconds = None


def _fixed_timing(monkeypatch, budget: float) -> None:
    def prepare(_config, _accounts):
        auth_routes._failure_budget = budget
        auth_routes._calibrated_budget = budget
        return budget

    monkeypatch.setattr(auth_routes, "prepare_login_timing", prepare)


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
        await install.set_credential(
            install.owner_id, credential_source="local",
            credential=hash_password(_PASSWORD),
        )
        async with _client(install.app) as client:
            good = await client.post("/api/auth/login", json={"password": _PASSWORD})
            bad = await client.post("/api/auth/login", json={"password": "nope"})
        assert good.status_code == 200 and good.json()["token"]
        assert bad.status_code == 401

    async def test_a_configured_password_still_authenticates(self, install):
        """A transitional ``config`` account reads ``auth.password_hash``."""
        set_config(NerveConfig(auth=AuthConfig(
            jwt_secret=_SECRET, password_hash=hash_password(_PASSWORD),
        )))
        await install.set_credential(
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
        await install.set_enabled(install.owner_id, False)
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
        self, install, monkeypatch,
    ):
        """One bcrypt comparison either way, on every request including the
        first one a process serves. The decoy is a constant precisely so that
        the first unknown-username attempt does not also pay for a hash."""
        from nerve.gateway.auth import verify_password
        from nerve.gateway.routes import auth as auth_routes

        counted = _CountedComparisons(monkeypatch)
        async with _client(install.app) as client:
            await client.post(
                "/api/auth/login",
                json={"username": "nobody-here", "password": _PASSWORD},
            )
            unknown = counted.take()
            await client.post(
                "/api/auth/login", json={"username": "alice", "password": "wrong"},
            )
            known = counted.take()

        # Real hashing, once, on both paths — and with the same plaintext, so
        # the two comparisons are the same piece of work.
        assert unknown == known == [_PASSWORD.encode()] or (
            unknown == [_PASSWORD.encode()] and known == ["wrong".encode()]
        )
        assert len(unknown) == len(known) == 1
        # ...and the decoy really is a usable bcrypt hash, or the comparison it
        # is there to pay for would be skipped.
        assert auth_routes._DECOY_HASH.startswith("$2b$12$")
        assert verify_password("anything at all", auth_routes._DECOY_HASH) is False

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
        await install.set_enabled(self.bob["id"], False)
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
        await install.set_credential(
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
    async def test_passwordless(self, install):
        async with _client(install.app) as client:
            body = (await client.get("/api/auth/status")).json()
        assert body == {"auth_required": False, "login": "none"}

    async def test_naming_the_account_stays_passwordless(self, install):
        """The accounts screen can set a username on its own. Doing that first
        must not stop the instance reporting as unsecured — it still admits
        every caller, which is the state the wizard exists to end."""
        await install.db.update_account_login(install.owner_id, username="alice")
        async with _client(install.app) as client:
            body = (await client.get("/api/auth/status")).json()
        assert body == {"auth_required": False, "login": "none"}

    async def test_only_a_password_ends_passwordless_login(self, install):
        await install.db.update_account_login(install.owner_id, username="alice")
        await install.db.update_account_login(
            install.owner_id, credential=hash_password(_PASSWORD),
        )
        async with _client(install.app) as client:
            body = (await client.get("/api/auth/status")).json()
        assert body == {"auth_required": True, "login": "password"}

    async def test_password_only(self, install):
        await install.secure_the_owner("alice")
        async with _client(install.app) as client:
            body = (await client.get("/api/auth/status")).json()
        assert body == {"auth_required": True, "login": "password"}

    async def test_username_and_password(self, install):
        await install.secure_the_owner("alice")
        await install.add_account("bob")
        async with _client(install.app) as client:
            body = (await client.get("/api/auth/status")).json()
        assert body == {"auth_required": True, "login": "username_password"}

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

    async def test_fails_closed_with_no_identity_store(self, install, monkeypatch):
        from nerve.gateway.routes import _deps as deps_module

        monkeypatch.setattr(deps_module, "_deps", None)
        async with _client(install.app) as client:
            body = (await client.get("/api/auth/status")).json()
            login = await client.post("/api/auth/login", json={"password": ""})
        assert body["login"] == "username_password"
        assert body["auth_required"] is True
        assert login.status_code == 503


# --------------------------------------------------------------------------- #
#  What a failed login costs, and what it therefore does not say               #
# --------------------------------------------------------------------------- #


class _CountedComparisons:
    """Count the bcrypt comparisons a block of work actually performs.

    Counting calls to ``verify_password`` does not establish equivalent work: it
    is the helper that used to return early, so it was called either way and the
    count was identical while one path did a quarter of a second of hashing and
    the other did none. ``bcrypt.checkpw`` is where the work is, so that is what
    is counted, along with the plaintext each comparison was actually handed.
    """

    def __init__(self, monkeypatch):
        from nerve.gateway import auth as gw_auth

        self.calls: list[bytes] = []
        real = gw_auth.bcrypt.checkpw

        def counting(password: bytes, hashed: bytes) -> bool:
            self.calls.append(password)
            return real(password, hashed)

        monkeypatch.setattr(gw_auth.bcrypt, "checkpw", counting)

    def take(self) -> list[bytes]:
        taken, self.calls = list(self.calls), []
        return taken


def _cheap_hash(password: str, rounds: int = 4) -> str:
    """Create a legacy hash with a lower work factor than current policy."""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=rounds)).decode()


@pytest.mark.asyncio
class TestFailedLoginsCostTheSame:
    async def test_an_empty_password_is_compared_not_short_circuited(
        self, install, monkeypatch,
    ):
        """Returning early for an empty password made an unknown username answer
        in microseconds and a known one pay for a full comparison — enumeration
        with an empty string."""
        await install.secure_the_owner("alice")
        counted = _CountedComparisons(monkeypatch)

        async with _client(install.app) as client:
            known = await client.post(
                "/api/auth/login", json={"username": "alice", "password": ""},
            )
            for_known = counted.take()
            unknown = await client.post(
                "/api/auth/login", json={"username": "nobody-here", "password": ""},
            )
            for_unknown = counted.take()

        assert known.status_code == unknown.status_code == 401
        assert known.json() == unknown.json()
        # One real bcrypt comparison either way — and the empty string reached
        # it, rather than being refused on the way to it.
        assert for_known == for_unknown == [b""]

    async def test_a_configured_empty_password_still_authenticates(self, install):
        """Existing empty-password hashes still verify. New empty passwords are
        rejected, so this only preserves access to legacy credentials."""
        empty = bcrypt.hashpw(b"", bcrypt.gensalt(rounds=4)).decode()
        await install.set_credential(
            install.owner_id, credential_source="local", credential=empty,
        )
        async with _client(install.app) as client:
            good = await client.post("/api/auth/login", json={"password": ""})
            bad = await client.post("/api/auth/login", json={"password": "x"})
        assert good.status_code == 200, good.text
        assert bad.status_code == 401

    @pytest.mark.parametrize("rounds", [4, 12])
    async def test_failures_take_the_budget_whatever_the_stored_cost(
        self, install, rounds, monkeypatch,
    ):
        """The decoy equalises *whether* a comparison happens. The budget
        equalises how long one takes — otherwise a cost-4 account fails in a
        millisecond while an unknown username takes a quarter of a second, and
        the difference is the account's existence."""
        await install.db.update_account_login(install.owner_id, username="alice")
        await install.set_credential(
            install.owner_id, credential_source="local",
            credential=_cheap_hash(_PASSWORD, rounds),
        )
        budget = 0.4
        _fixed_timing(monkeypatch, budget)

        async def elapsed(body) -> float:
            async with _client(install.app) as client:
                started = time.monotonic()
                response = await client.post("/api/auth/login", json=body)
                assert response.status_code == 401
                return time.monotonic() - started

        known = await elapsed({"username": "alice", "password": "wrong"})
        unknown = await elapsed({"username": "nobody-here", "password": "wrong"})

        # Both wait out the budget...
        assert known >= budget * 0.9, known
        assert unknown >= budget * 0.9, unknown
        # ...and neither runs away with it. The generous ceiling is the decoy
        # comparison the unknown path still performs inside the window.
        assert known < budget + 1.5, known
        assert unknown < budget + 1.5, unknown

    async def test_the_budget_covers_a_slower_hash_than_the_policy(
        self, install, monkeypatch,
    ):
        """From the other direction: a hash *more* expensive than the budget
        would stand out by being slower. One observation raises the floor."""
        _fixed_timing(monkeypatch, 0.01)
        await install.db.update_account_login(install.owner_id, username="alice")
        await install.set_credential(
            install.owner_id, credential_source="local",
            credential=_cheap_hash(_PASSWORD, rounds=10),
        )
        async with _client(install.app) as client:
            await client.post(
                "/api/auth/login", json={"username": "alice", "password": "wrong"},
            )
            raised = auth_routes._failure_budget
            started = time.monotonic()
            await client.post(
                "/api/auth/login",
                json={"username": "nobody-here", "password": "wrong"},
            )
            unknown = time.monotonic() - started
        assert raised > 0.01, raised
        assert unknown >= raised * 0.9, (unknown, raised)

    async def test_the_very_first_probe_covers_a_slower_stored_hash(self, install):
        """Reacting to a slow comparison *after* making it is too late: that
        request already took four times as long as an unknown username did, and
        one probe is all an enumeration needs. The budget is calibrated from the
        slowest cost the accounts actually carry, before the first login is
        served — and the unknown username is asked first here, so nothing has
        had a chance to observe the slow hash."""
        from nerve.config import get_config

        _reset_timing()
        await install.db.update_account_login(install.owner_id, username="alice")
        slow = _cheap_hash(_PASSWORD, rounds=13)
        await install.set_credential(
            install.owner_id, credential_source="local", credential=slow,
        )

        # Calibration, with nothing having compared against the slow hash yet —
        # so anything it knows, it knows from reading the accounts.
        budget = auth_routes.prepare_login_timing(
            get_config(), await install.db.list_accounts(),
        )
        started = time.monotonic()
        verify_password("wrong", slow)
        one_slow_comparison = time.monotonic() - started
        assert budget >= one_slow_comparison, (budget, one_slow_comparison)

        async def elapsed(body) -> float:
            async with _client(install.app) as client:
                started_at = time.monotonic()
                response = await client.post("/api/auth/login", json=body)
                assert response.status_code == 401
                return time.monotonic() - started_at

        # Unknown first, so the known one cannot be the thing that taught the
        # budget how slow this account is.
        unknown = await elapsed({"username": "nobody-here", "password": "wrong"})
        known = await elapsed({"username": "alice", "password": "wrong"})

        assert unknown >= budget * 0.9, (unknown, budget)
        assert known >= budget * 0.9, (known, budget)
        assert abs(known - unknown) < budget * 0.5, (unknown, known, budget)

    async def test_a_slower_hash_introduced_after_calibration_is_covered(
        self, install,
    ):
        """A `none` or `config` row verifies against auth.password_hash, which a
        configuration *reload* can replace at any moment with a hash at any work
        factor. A budget worked out once at first login goes stale the instant
        that happens, and stays stale until some known-user probe raises the
        reactive mark — which is the same one-probe-too-late this is supposed to
        have ended. So it is recomputed every login."""
        _reset_timing()
        await install.db.update_account_login(install.owner_id, username="alice")

        async def probe(body) -> float:
            async with _client(install.app) as client:
                started = time.monotonic()
                response = await client.post("/api/auth/login", json=body)
                assert response.status_code == 401
                return time.monotonic() - started

        # Calibrated on an ordinary install, and warm.
        await probe({"username": "nobody-here", "password": "wrong"})
        settled = auth_routes._failure_budget
        assert settled is not None

        # Now the configured hash is swapped for a much slower one, with no
        # restart — exactly what a reload does.
        set_config(NerveConfig(auth=AuthConfig(
            jwt_secret=_SECRET, password_hash=_cheap_hash(_PASSWORD, rounds=14),
        )))

        # Unknown first, so nothing has had the chance to *observe* the new cost.
        unknown = await probe({"username": "nobody-here", "password": "wrong"})
        known = await probe({"username": "alice", "password": "wrong"})

        assert auth_routes._failure_budget > settled, auth_routes._failure_budget
        assert unknown >= known * 0.75, (unknown, known)
        assert known >= unknown * 0.75, (unknown, known)

    async def test_the_budget_comes_back_down_when_the_slow_hash_goes(self, install):
        """It is recomputed, not ratcheted: an install that converges on the
        policy cost stops paying for the one account that had not."""
        _reset_timing()
        set_config(NerveConfig(auth=AuthConfig(
            jwt_secret=_SECRET, password_hash=_cheap_hash(_PASSWORD, rounds=14),
        )))
        async with _client(install.app) as client:
            await client.post("/api/auth/login", json={"password": "wrong"})
        slow = auth_routes._failure_budget

        set_config(NerveConfig(auth=AuthConfig(jwt_secret=_SECRET)))
        await install.db.update_account_login(
            install.owner_id, credential=hash_password(_PASSWORD),
        )
        async with _client(install.app) as client:
            await client.post("/api/auth/login", json={"password": "wrong"})

        assert auth_routes._failure_budget < slow, (auth_routes._failure_budget, slow)

    async def test_calibration_reads_the_configured_hash_too(self, install):
        """`config` and `none` rows verify against auth.password_hash, so its
        work factor counts as much as any stored on a row."""
        cheap = _cheap_hash(_PASSWORD, rounds=4)
        assert auth_routes._slowest_stored_cost([], cheap) == BCRYPT_COST
        assert auth_routes._slowest_stored_cost(
            [], _cheap_hash(_PASSWORD, rounds=14),
        ) == 14
        assert auth_routes._slowest_stored_cost(
            [{"credential_source": "local", "credential": cheap}], "",
        ) == BCRYPT_COST
        assert auth_routes._slowest_stored_cost(
            [{"credential_source": "local", "credential": _cheap_hash(_PASSWORD, 13)},
             {"credential_source": "local", "credential": hash_password(_PASSWORD)}], "",
        ) == 13
        # A `config` row's own column is NULL and says nothing about cost.
        assert auth_routes._slowest_stored_cost(
            [{"credential_source": "config", "credential": None}], "",
        ) == BCRYPT_COST

    async def test_the_budget_is_measured_outside_a_request(self, install):
        """The measurement costs a comparison. Paid inside a request's own timed
        window, it would make the first failure of a process stand out from
        every later one — the same leak, moved."""
        _reset_timing()
        async with _client(install.app) as client:
            await install.secure_the_owner("alice")
            first = time.monotonic()
            await client.post(
                "/api/auth/login", json={"username": "nobody", "password": "x"},
            )
            first = time.monotonic() - first
            second = time.monotonic()
            await client.post(
                "/api/auth/login", json={"username": "nobody", "password": "x"},
            )
            second = time.monotonic() - second
        budget = auth_routes._failure_budget
        assert budget is not None
        # The first request pays for the measurement *before* its clock starts,
        # so what is left is one budget — the same as every request after it.
        assert first < budget * 3, (first, budget)
        assert second >= budget * 0.9, (second, budget)


@pytest.mark.asyncio
class TestHashesConvergeOnThePolicyCost:
    async def test_a_successful_login_upgrades_an_older_work_factor(self, install):
        await install.db.update_account_login(install.owner_id, username="alice")
        legacy = _cheap_hash(_PASSWORD, rounds=4)
        await install.set_credential(
            install.owner_id, credential_source="local", credential=legacy,
        )
        assert bcrypt_cost(legacy) == 4

        async with _client(install.app) as client:
            ok = await client.post(
                "/api/auth/login", json={"username": "alice", "password": _PASSWORD},
            )
            assert ok.status_code == 200

            row = await install.db.get_account(install.owner_id)
            assert row["credential"] != legacy
            assert bcrypt_cost(row["credential"]) == BCRYPT_COST
            # ...and the password is still the password.
            again = await client.post(
                "/api/auth/login", json={"username": "alice", "password": _PASSWORD},
            )
            assert again.status_code == 200
            assert (await install.db.get_account(
                install.owner_id,
            ))["credential"] == row["credential"]          # nothing churns after

    async def test_a_hash_already_at_the_policy_cost_is_left_alone(self, install):
        await install.secure_the_owner("alice")
        before = (await install.db.get_account(install.owner_id))["credential"]
        async with _client(install.app) as client:
            assert (await client.post(
                "/api/auth/login", json={"username": "alice", "password": _PASSWORD},
            )).status_code == 200
        assert (await install.db.get_account(
            install.owner_id,
        ))["credential"] == before

    async def test_a_configured_credential_is_not_moved_onto_the_row_by_a_login(
        self, install,
    ):
        """Moving an account off the configured password is the startup
        migration's job. A login doing it as a side effect would take an install
        off `config` at a moment nothing reported."""
        set_config(NerveConfig(auth=AuthConfig(
            jwt_secret=_SECRET, password_hash=_cheap_hash(_PASSWORD, rounds=4),
        )))
        await install.set_credential(
            install.owner_id, credential_source="config",
        )
        async with _client(install.app) as client:
            assert (await client.post(
                "/api/auth/login", json={"password": _PASSWORD},
            )).status_code == 200
        row = await install.db.get_account(install.owner_id)
        assert row["credential_source"] == "config"
        assert row["credential"] is None

    async def test_a_failed_upgrade_does_not_fail_the_login(self, install, monkeypatch):
        await install.db.update_account_login(install.owner_id, username="alice")
        await install.set_credential(
            install.owner_id, credential_source="local",
            credential=_cheap_hash(_PASSWORD, rounds=4),
        )

        async def boom(*args, **kwargs):
            raise RuntimeError("database is locked")

        before = (await install.db.get_account(install.owner_id))["credential"]
        # The method the re-hash actually calls — a compare-and-swap since the
        # unconditional write could revert a concurrent password change.
        monkeypatch.setattr(install.db, "replace_credential_if_unchanged", boom)
        async with _client(install.app) as client:
            response = await client.post(
                "/api/auth/login", json={"username": "alice", "password": _PASSWORD},
            )
        assert response.status_code == 200
        assert (await install.db.get_account(
            install.owner_id,
        ))["credential"] == before

    async def test_a_concurrent_password_change_is_not_reverted(self, install):
        """The credential is read, compared against, and only then replaced. A
        password changed in between must stand: writing unconditionally would
        put the old one back and leave whoever knew it still able to log in."""
        await install.db.update_account_login(install.owner_id, username="alice")
        stale = _cheap_hash(_PASSWORD, rounds=4)
        await install.set_credential(
            install.owner_id, credential_source="local", credential=stale,
        )
        account = await install.db.get_account(install.owner_id)

        # ...the owner changes their password from another tab...
        await install.db.update_account_login(
            install.owner_id, credential=hash_password(_OTHER_PASSWORD),
        )

        # ...and the in-flight login finishes, holding the row it read first.
        await auth_routes._maybe_upgrade_hash(install.db, account, _PASSWORD)

        async with _client(install.app) as client:
            new_one = await client.post(
                "/api/auth/login",
                json={"username": "alice", "password": _OTHER_PASSWORD},
            )
            old_one = await client.post(
                "/api/auth/login", json={"username": "alice", "password": _PASSWORD},
            )
        assert new_one.status_code == 200, new_one.text
        assert old_one.status_code == 401

    async def test_the_swap_is_conditioned_on_the_source_too(self, install):
        """A row that has moved off its own credential in the meantime is left
        alone rather than dragged back to `local`."""
        await install.set_credential(
            install.owner_id, credential_source="local",
            credential=_cheap_hash(_PASSWORD, rounds=4),
        )
        account = await install.db.get_account(install.owner_id)
        await install.set_credential(
            install.owner_id, credential_source="config",
        )

        await auth_routes._maybe_upgrade_hash(install.db, account, _PASSWORD)

        row = await install.db.get_account(install.owner_id)
        assert row["credential_source"] == "config"
        assert row["credential"] is None

    async def test_an_over_long_legacy_password_is_not_re_hashed(self, install):
        """verify_password accepted it by truncating. Re-hashing would refuse,
        and hashing the truncation would store a different password from the one
        its owner types."""
        long_password = "p" * 100
        legacy = bcrypt.hashpw(
            long_password.encode()[:72], bcrypt.gensalt(rounds=4),
        ).decode()
        await install.db.update_account_login(install.owner_id, username="alice")
        await install.set_credential(
            install.owner_id, credential_source="local", credential=legacy,
        )
        async with _client(install.app) as client:
            assert (await client.post(
                "/api/auth/login",
                json={"username": "alice", "password": long_password},
            )).status_code == 200
        assert (await install.db.get_account(install.owner_id))["credential"] == legacy
