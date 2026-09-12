"""The account-management API.

Every account may manage accounts (0.4), so these tests are mostly about the
three things that are *not* allowed — a second account while the instance is
passwordless, a second account while the first has no username, and disabling
the last enabled one — plus the rule that no response ever carries a
credential.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from nerve.config import AuthConfig, NerveConfig, set_config
from nerve.gateway.auth import (
    create_session_token,
    create_system_token,
    hash_password,
    pin_jwt_secret,
    verify_password,
)
from nerve.gateway.routes import accounts as accounts_routes
from nerve.gateway.routes import auth as auth_routes

_SECRET = "test-secret-for-account-management-pad-32b"

# Obviously synthetic, and a real bcrypt hash so verify_password can be asked
# about it: this is the password the fixtures log in with.
_PASSWORD = "correct-horse-battery-staple"


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(auth_routes.router)
    app.include_router(accounts_routes.router)
    return app


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://nerve-test",
    )


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


class _Install:
    def __init__(self, db, identity, app):
        self.db = db
        self.identity = identity
        self.app = app
        self.owner_id = identity.owner_account_id

    def token(self, account_id: str | None = None) -> str:
        return create_session_token(_SECRET, account_id or self.owner_id)

    def headers(self, account_id: str | None = None) -> dict:
        return _bearer(self.token(account_id))

    async def secure_the_owner(self, username: str = "alice") -> None:
        """Give the bootstrapped account a username and a password, which is
        what an install has to do before it can add anybody."""
        await self.db.update_account_login(
            self.owner_id, username=username, credential=hash_password(_PASSWORD),
        )


@pytest.fixture(autouse=True)
def _fast_failures():
    """Failed logins are padded to a common response budget (see
    ``nerve.gateway.routes.auth._failure_budget_seconds``). Nothing in this file
    is about the padding, and paying it per refusal would add minutes."""
    from nerve.gateway.routes import auth as auth_routes

    auth_routes._set_failure_budget(0.0)
    yield
    auth_routes._set_failure_budget(None)


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
#  Nothing leaks a credential                                                  #
# --------------------------------------------------------------------------- #

# What an account response is allowed to contain. Asserted field-for-field
# rather than "credential not in response": a column added to the table later
# must not be able to arrive in a response unnoticed.
_ACCOUNT_FIELDS = {
    "id", "actor_id", "username", "display_name", "enabled", "has_password",
    "created_at", "updated_at", "disabled_at", "is_self",
}


def _assert_no_credential(payload) -> None:
    """Walk a response and refuse anything credential-shaped, at any depth."""
    if isinstance(payload, dict):
        for key, value in payload.items():
            assert key not in ("credential", "credential_source", "password"), key
            assert not (isinstance(value, str) and value.startswith("$2b$")), key
            _assert_no_credential(value)
    elif isinstance(payload, list):
        for item in payload:
            _assert_no_credential(item)


@pytest.mark.asyncio
class TestNoCredentialEverLeaves:
    async def test_every_endpoint(self, install: _Install):
        await install.secure_the_owner()
        async with _client(install.app) as client:
            created = await client.post(
                "/api/accounts",
                json={"username": "bob", "password": _PASSWORD, "display_name": "Bob"},
                headers=install.headers(),
            )
            assert created.status_code == 201
            bob = created.json()["id"]

            responses = [
                created,
                await client.get("/api/accounts", headers=install.headers()),
                await client.get("/api/accounts/me", headers=install.headers()),
                await client.patch(
                    f"/api/accounts/{bob}", json={"username": "bobby"},
                    headers=install.headers(),
                ),
                await client.post(
                    f"/api/accounts/{bob}/disable", headers=install.headers(),
                ),
                await client.post(
                    f"/api/accounts/{bob}/enable", headers=install.headers(),
                ),
                await client.put(
                    "/api/accounts/me/password",
                    json={"current_password": _PASSWORD, "new_password": "a-new-one"},
                    headers=install.headers(),
                ),
            ]
        for response in responses:
            assert response.status_code in (200, 201), response.text
            _assert_no_credential(response.json())

        listed = responses[1].json()["accounts"]
        assert len(listed) == 2
        for row in listed:
            assert set(row) == _ACCOUNT_FIELDS

    async def test_the_actor_id_is_published_and_is_not_the_account_id(
        self, install: _Install,
    ):
        """Attribution is written against the actor id, so a UI showing who did
        something has to be able to look one up. It is a different column from
        the account id, and it is the one that outlives a rename."""
        async with _client(install.app) as client:
            me = (await client.get("/api/accounts/me", headers=install.headers())).json()
        row = await install.db.get_account(install.owner_id)
        assert me["actor_id"] == row["actor_id"]
        assert me["actor_id"] != me["id"]

        await install.db.update_account_login(install.owner_id, username="renamed")
        async with _client(install.app) as client:
            after = (await client.get(
                "/api/accounts/me", headers=install.headers(),
            )).json()
        assert after["actor_id"] == me["actor_id"]

    async def test_has_password_stands_in_for_the_credential(self, install: _Install):
        async with _client(install.app) as client:
            before = await client.get("/api/accounts/me", headers=install.headers())
            assert before.json()["has_password"] is False
            await client.put(
                "/api/accounts/me/password",
                json={"new_password": _PASSWORD}, headers=install.headers(),
            )
            after = await client.get("/api/accounts/me", headers=install.headers())
            assert after.json()["has_password"] is True


# --------------------------------------------------------------------------- #
#  Any account may manage accounts (0.4)                                       #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestEveryAccountMayManageAccounts:
    async def test_the_account_you_created_can_disable_you(self, install: _Install):
        """The stated property of 0.4, tested rather than assumed: adding a
        person gives them the power to remove you."""
        await install.secure_the_owner()
        async with _client(install.app) as client:
            created = await client.post(
                "/api/accounts", json={"username": "bob", "password": _PASSWORD},
                headers=install.headers(),
            )
            bob = created.json()["id"]

            disabled = await client.post(
                f"/api/accounts/{install.owner_id}/disable",
                headers=install.headers(bob),
            )
            assert disabled.status_code == 200
            assert disabled.json()["enabled"] is False

            # ...and the newcomer can list and create just the same.
            assert (await client.get(
                "/api/accounts", headers=install.headers(bob),
            )).status_code == 200
            assert (await client.post(
                "/api/accounts", json={"username": "carol", "password": _PASSWORD},
                headers=install.headers(bob),
            )).status_code == 201

    async def test_the_system_principal_may_not(self, install: _Install):
        """``require_auth`` admits the credential the instance mints for itself.
        Account administration is a human act, so that credential is refused —
        an agent talked into calling its own API cannot make itself a login."""
        await install.secure_the_owner()
        system = create_system_token(_SECRET)
        async with _client(install.app) as client:
            for method, path, body in [
                ("get", "/api/accounts", None),
                ("get", "/api/accounts/me", None),
                ("post", "/api/accounts", {"username": "mallory", "password": _PASSWORD}),
                ("post", f"/api/accounts/{install.owner_id}/disable", None),
                ("post", f"/api/accounts/{install.owner_id}/enable", None),
                ("put", "/api/accounts/me/password", {"new_password": _PASSWORD}),
                ("patch", f"/api/accounts/{install.owner_id}", {"username": "eve"}),
            ]:
                response = await getattr(client, method)(
                    path, headers=_bearer(system), **({"json": body} if body else {}),
                )
                assert response.status_code == 403, (method, path, response.text)
        assert await install.db.count_accounts() == 1

    async def test_no_token_no_access(self, install: _Install):
        async with _client(install.app) as client:
            assert (await client.get("/api/accounts")).status_code == 401


# --------------------------------------------------------------------------- #
#  The guards                                                                  #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestPasswordlessGuard:
    async def test_a_second_account_is_refused_while_passwordless(self, install: _Install):
        async with _client(install.app) as client:
            response = await client.post(
                "/api/accounts", json={"username": "bob", "password": _PASSWORD},
                headers=install.headers(),
            )
        assert response.status_code == 409
        assert "password" in response.json()["detail"].lower()
        assert await install.db.count_accounts() == 1

    async def test_setting_a_password_and_a_username_unblocks_it(self, install: _Install):
        async with _client(install.app) as client:
            assert (await client.post(
                "/api/accounts", json={"username": "bob", "password": _PASSWORD},
                headers=install.headers(),
            )).status_code == 409

            # 1. the first account sets its own password...
            assert (await client.put(
                "/api/accounts/me/password", json={"new_password": _PASSWORD},
                headers=install.headers(),
            )).status_code == 200
            # 2. ...and is still refused, because it has no username yet.
            blocked = await client.post(
                "/api/accounts", json={"username": "bob", "password": _PASSWORD},
                headers=install.headers(),
            )
            assert blocked.status_code == 409
            assert "username" in blocked.json()["detail"].lower()
            # 3. name it, and the second account can exist.
            assert (await client.patch(
                f"/api/accounts/{install.owner_id}", json={"username": "alice"},
                headers=install.headers(),
            )).status_code == 200
            assert (await client.post(
                "/api/accounts", json={"username": "bob", "password": _PASSWORD},
                headers=install.headers(),
            )).status_code == 201
        assert await install.db.count_accounts() == 2


@pytest.mark.asyncio
class TestLastAccountGuard:
    async def test_the_only_account_cannot_disable_itself(self, install: _Install):
        async with _client(install.app) as client:
            response = await client.post(
                f"/api/accounts/{install.owner_id}/disable", headers=install.headers(),
            )
        assert response.status_code == 409
        assert (await install.db.get_account(install.owner_id))["enabled"] is True

    async def test_two_concurrent_disables_cannot_both_succeed(self, install: _Install):
        await install.secure_the_owner()
        async with _client(install.app) as client:
            bob = (await client.post(
                "/api/accounts", json={"username": "bob", "password": _PASSWORD},
                headers=install.headers(),
            )).json()["id"]

            first, second = await asyncio.gather(
                client.post(
                    f"/api/accounts/{install.owner_id}/disable",
                    headers=install.headers(bob),
                ),
                client.post(
                    f"/api/accounts/{bob}/disable", headers=install.headers(),
                ),
            )
        codes = sorted([first.status_code, second.status_code])
        assert codes == [200, 409], (first.text, second.text)
        assert await install.db.count_accounts(enabled_only=True) == 1

    async def test_disable_and_enable_repeat_without_changing_anything(
        self, install: _Install,
    ):
        await install.secure_the_owner()
        async with _client(install.app) as client:
            bob = (await client.post(
                "/api/accounts", json={"username": "bob", "password": _PASSWORD},
                headers=install.headers(),
            )).json()["id"]

            once = await client.post(f"/api/accounts/{bob}/disable", headers=install.headers())
            twice = await client.post(f"/api/accounts/{bob}/disable", headers=install.headers())
            assert once.json() == twice.json()

            back = await client.post(f"/api/accounts/{bob}/enable", headers=install.headers())
            again = await client.post(f"/api/accounts/{bob}/enable", headers=install.headers())
            assert back.json() == again.json()
            assert back.json()["enabled"] is True
            assert back.json()["disabled_at"] is None

    async def test_unknown_account(self, install: _Install):
        async with _client(install.app) as client:
            for path in ("/api/accounts/nope/disable", "/api/accounts/nope/enable"):
                assert (await client.post(path, headers=install.headers())).status_code == 404
            assert (await client.patch(
                "/api/accounts/nope", json={"username": "bob"}, headers=install.headers(),
            )).status_code == 404


@pytest.mark.asyncio
class TestDisablementTakesEffectAtTheNextRequest:
    async def test_not_retroactively(self, install: _Install):
        """The API half of the rule PR 2 proved on the request path: a token
        minted before the change keeps verifying, and the account row is what
        stops it — on the *next* call, not the one in flight."""
        await install.secure_the_owner()
        async with _client(install.app) as client:
            bob = (await client.post(
                "/api/accounts", json={"username": "bob", "password": _PASSWORD},
                headers=install.headers(),
            )).json()["id"]
            bob_headers = install.headers(bob)

            assert (await client.get("/api/accounts", headers=bob_headers)).status_code == 200
            await client.post(f"/api/accounts/{bob}/disable", headers=install.headers())

            refused = await client.get("/api/accounts", headers=bob_headers)
            assert refused.status_code == 401
            assert "disabled" in refused.json()["detail"].lower()

            # Re-enabling restores the very same token: nothing was revoked.
            await client.post(f"/api/accounts/{bob}/enable", headers=install.headers())
            assert (await client.get("/api/accounts", headers=bob_headers)).status_code == 200


# --------------------------------------------------------------------------- #
#  Usernames                                                                   #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestUsernamesThroughTheApi:
    @pytest.mark.parametrize("username,status", [
        ("bob", 201),
        ("BOB", 201),
        # Reserved and malformed names are 400: they describe the request.
        # The three 409s below describe the *instance* — a name already taken.
        ("user", 400),              # reserved: the legacy token subject
        ("admin", 400),
        ("me", 400),
        ("b", 400),                 # too short
        ("no spaces", 400),
        ("-bob", 400),
        ("bob@example", 400),
        ("alice", 409),             # taken by the owner, case-insensitively
        ("ALICE", 409),
    ])
    async def test_creation_applies_the_rules(
        self, install: _Install, username, status,
    ):
        await install.secure_the_owner("alice")
        async with _client(install.app) as client:
            response = await client.post(
                "/api/accounts", json={"username": username, "password": _PASSWORD},
                headers=install.headers(),
            )
        assert response.status_code == status, response.text
        if status == 201:
            assert response.json()["username"] == username.lower()

    async def test_renaming_keeps_the_identity(self, install: _Install):
        """0.7: a username is a lookup key. Renaming moves nothing stored."""
        await install.secure_the_owner("alice")
        actor_before = (await install.db.get_account(install.owner_id))["actor_id"]
        async with _client(install.app) as client:
            renamed = await client.patch(
                f"/api/accounts/{install.owner_id}",
                json={"username": "alice2", "display_name": "Alice Two"},
                headers=install.headers(),
            )
        assert renamed.status_code == 200
        assert renamed.json()["username"] == "alice2"
        assert renamed.json()["display_name"] == "Alice Two"
        account = await install.db.get_account(install.owner_id)
        assert account["actor_id"] == actor_before
        assert account["id"] == install.owner_id
        # The old session token still works: identity did not move.
        async with _client(install.app) as client:
            assert (await client.get(
                "/api/accounts", headers=install.headers(),
            )).status_code == 200


# --------------------------------------------------------------------------- #
#  Own password                                                                #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestOwnPassword:
    async def test_a_first_password_needs_no_current_one(self, install: _Install):
        async with _client(install.app) as client:
            response = await client.put(
                "/api/accounts/me/password", json={"new_password": _PASSWORD},
                headers=install.headers(),
            )
        assert response.status_code == 200
        account = await install.db.get_account(install.owner_id)
        assert account["credential_source"] == "local"
        assert verify_password(_PASSWORD, account["credential"])

    async def test_changing_one_requires_the_current_one(self, install: _Install):
        await install.secure_the_owner()
        async with _client(install.app) as client:
            missing = await client.put(
                "/api/accounts/me/password", json={"new_password": "another-one"},
                headers=install.headers(),
            )
            wrong = await client.put(
                "/api/accounts/me/password",
                json={"current_password": "not-it", "new_password": "another-one"},
                headers=install.headers(),
            )
            right = await client.put(
                "/api/accounts/me/password",
                json={"current_password": _PASSWORD, "new_password": "another-one"},
                headers=install.headers(),
            )
        assert missing.status_code == 403
        assert wrong.status_code == 403
        assert right.status_code == 200
        account = await install.db.get_account(install.owner_id)
        assert verify_password("another-one", account["credential"])

    async def test_a_configured_password_counts_as_the_current_one(self, install: _Install):
        """A ``config``-source account has a password — in configuration — so it
        has to prove it like anyone else, and setting a new one moves the
        account onto its own credential."""
        configured = hash_password(_PASSWORD)
        set_config(NerveConfig(auth=AuthConfig(
            jwt_secret=_SECRET, password_hash=configured,
        )))
        await install.db.set_account_credential(
            install.owner_id, credential_source="config",
        )
        async with _client(install.app) as client:
            assert (await client.put(
                "/api/accounts/me/password", json={"new_password": "another-one"},
                headers=install.headers(),
            )).status_code == 403
            assert (await client.put(
                "/api/accounts/me/password",
                json={"current_password": _PASSWORD, "new_password": "another-one"},
                headers=install.headers(),
            )).status_code == 200
        account = await install.db.get_account(install.owner_id)
        assert account["credential_source"] == "local"
        assert verify_password("another-one", account["credential"])

    async def test_there_is_no_way_to_set_someone_elses(self, install: _Install):
        """Only own-password change exists. Resetting a colleague's password is
        deliberately not a thing any account can do."""
        await install.secure_the_owner()
        async with _client(install.app) as client:
            bob = (await client.post(
                "/api/accounts", json={"username": "bob", "password": _PASSWORD},
                headers=install.headers(),
            )).json()["id"]
            for path in (
                f"/api/accounts/{bob}/password",
                f"/api/accounts/{install.owner_id}/password",
            ):
                response = await client.put(
                    path, json={"new_password": "x"}, headers=install.headers(),
                )
                assert response.status_code in (404, 405), path
            # ...and the owner's own change does not touch bob's credential.
            before = (await install.db.get_account(bob))["credential"]
            await client.put(
                "/api/accounts/me/password",
                json={"current_password": _PASSWORD, "new_password": "another-one"},
                headers=install.headers(),
            )
            assert (await install.db.get_account(bob))["credential"] == before

    async def test_an_omitted_current_password_is_not_an_empty_one(
        self, install: _Install,
    ):
        """Omitting it is "I am not claiming to know it". Sending "" is a claim
        that the current password is the empty string — which a credential made
        before this release can legitimately be, so it is compared."""
        import bcrypt

        empty = bcrypt.hashpw(b"", bcrypt.gensalt(rounds=4)).decode()
        await install.db.set_account_credential(
            install.owner_id, credential_source="local", credential=empty,
        )
        async with _client(install.app) as client:
            omitted = await client.put(
                "/api/accounts/me/password", json={"new_password": "a-new-one"},
                headers=install.headers(),
            )
            assert omitted.status_code == 403

            supplied = await client.put(
                "/api/accounts/me/password",
                json={"current_password": "", "new_password": "a-new-one"},
                headers=install.headers(),
            )
        assert supplied.status_code == 200, supplied.text
        assert verify_password(
            "a-new-one", (await install.db.get_account(install.owner_id))["credential"],
        )

    async def test_an_empty_current_password_is_still_wrong_when_it_is_wrong(
        self, install: _Install,
    ):
        await install.secure_the_owner()
        async with _client(install.app) as client:
            response = await client.put(
                "/api/accounts/me/password",
                json={"current_password": "", "new_password": "a-new-one"},
                headers=install.headers(),
            )
        assert response.status_code == 403

    async def test_an_empty_new_password_is_refused(self, install: _Install):
        async with _client(install.app) as client:
            response = await client.put(
                "/api/accounts/me/password", json={"new_password": ""},
                headers=install.headers(),
            )
        assert response.status_code == 422


# --------------------------------------------------------------------------- #
#  Structural: what the route surface promises                                 #
# --------------------------------------------------------------------------- #


class TestTheRouteSurface:
    """Checked against the modules rather than through a client, so a route
    added later without a gate fails here rather than in production."""

    @staticmethod
    def _endpoints():
        import inspect

        from nerve.gateway.routes import (
            accounts, auth, codex, config, cron, diagnostics, external_agents,
            files, mcp_servers, memory, models, notifications, plans,
            prompt_rewrite, review_loops, sessions, skills, sources, tasks,
            workflow_runs,
        )

        modules = [
            accounts, auth, codex, config, cron, diagnostics, external_agents,
            files, mcp_servers, memory, models, notifications, plans,
            prompt_rewrite, review_loops, sessions, skills, sources, tasks,
            workflow_runs,
        ]
        for module in modules:
            for route in module.router.routes:
                endpoint = getattr(route, "endpoint", None)
                if endpoint is None:
                    continue
                gates = [
                    p.default.dependency.__name__
                    for p in inspect.signature(endpoint).parameters.values()
                    if getattr(p.default, "dependency", None) is not None
                ]
                yield sorted(route.methods), str(route.path), gates

    def test_only_three_api_endpoints_are_unauthenticated(self):
        """Login and status are the doors themselves; the worker-token exchange
        authenticates through the MCP path instead. Anything else appearing
        here is a hole."""
        open_endpoints = {
            (tuple(methods), path)
            for methods, path, gates in self._endpoints()
            if path.startswith("/api")
            and "require_auth" not in gates
            and "require_account" not in gates
        }
        assert open_endpoints == {
            (("POST",), "/api/auth/login"),
            (("GET",), "/api/auth/status"),
            (("POST",), "/api/codex/worker-token"),
        }

    def test_every_account_endpoint_requires_a_human_account(self):
        account_endpoints = [
            (tuple(methods), path, gates)
            for methods, path, gates in self._endpoints()
            if path.startswith("/api/accounts")
        ]
        assert len(account_endpoints) == 7
        for methods, path, gates in account_endpoints:
            assert gates == ["require_account"], (methods, path, gates)

    def test_there_is_no_endpoint_that_deletes_an_account(self):
        """Removal is disablement; the row is the tombstone that keeps a
        grandfathered token from resolving to the wrong account."""
        for methods, path, _ in self._endpoints():
            if path.startswith("/api/accounts"):
                assert "DELETE" not in methods, path


# --------------------------------------------------------------------------- #
#  How long a password may be                                                  #
# --------------------------------------------------------------------------- #

# bcrypt hashes at most 72 *bytes*, and version 5 raises rather than ignoring
# the rest. Both boundaries that write a password have to say so themselves, or
# a long passphrase is a 500.
_AT_THE_LIMIT = "p" * 72
_OVER_THE_LIMIT = "p" * 73
# Nineteen emoji: nineteen characters, seventy-six bytes. The case that makes
# "measure characters" wrong rather than merely imprecise.
_MULTIBYTE_OVER = "\U0001F600" * 19
# Eighteen is seventy-two bytes exactly.
_MULTIBYTE_AT_THE_LIMIT = "\U0001F600" * 18


@pytest.mark.asyncio
class TestPasswordLength:
    @pytest.mark.parametrize("password,status", [
        (_AT_THE_LIMIT, 201),
        (_MULTIBYTE_AT_THE_LIMIT, 201),
        (_OVER_THE_LIMIT, 400),
        (_MULTIBYTE_OVER, 400),
    ])
    async def test_creating_an_account(self, install: _Install, password, status):
        await install.secure_the_owner()
        async with _client(install.app) as client:
            response = await client.post(
                "/api/accounts", json={"username": "bob", "password": password},
                headers=install.headers(),
            )
        assert response.status_code == status, response.text
        if status == 400:
            detail = response.json()["detail"]
            assert "bytes" in detail
            assert "72" in detail

    @pytest.mark.parametrize("password,status", [
        (_AT_THE_LIMIT, 200),
        (_MULTIBYTE_AT_THE_LIMIT, 200),
        (_OVER_THE_LIMIT, 400),
        (_MULTIBYTE_OVER, 400),
    ])
    async def test_changing_your_own_password(self, install: _Install, password, status):
        async with _client(install.app) as client:
            response = await client.put(
                "/api/accounts/me/password", json={"new_password": password},
                headers=install.headers(),
            )
        assert response.status_code == status, response.text

    async def test_a_password_at_the_limit_still_logs_in(self, install: _Install):
        async with _client(install.app) as client:
            assert (await client.put(
                "/api/accounts/me/password", json={"new_password": _AT_THE_LIMIT},
                headers=install.headers(),
            )).status_code == 200
            assert (await client.post(
                "/api/auth/login", json={"password": _AT_THE_LIMIT},
            )).status_code == 200

    async def test_an_over_long_guess_is_refused_rather_than_crashing(
        self, install: _Install,
    ):
        """The login route verifies rather than hashes, so it sees over-long
        input too — and must answer, not raise."""
        await install.secure_the_owner()
        async with _client(install.app) as client:
            response = await client.post(
                "/api/auth/login",
                json={"username": "alice", "password": _OVER_THE_LIMIT},
            )
        assert response.status_code == 401
        assert response.json()["detail"] == "Invalid username or password"

    async def test_a_password_hashed_before_bcrypt_5_still_verifies(self):
        """bcrypt used to ignore everything past 72 bytes, so a hash from an
        older install was made from the first 72 of whatever was typed.
        Refusing the full password now would lock out somebody who could log in
        yesterday, which is the one thing this release promises not to do."""
        import bcrypt

        from nerve.gateway.auth import verify_password

        legacy = bcrypt.hashpw(
            _OVER_THE_LIMIT.encode()[:72], bcrypt.gensalt(rounds=4),
        ).decode()
        assert verify_password(_OVER_THE_LIMIT, legacy) is True
        assert verify_password("something else entirely", legacy) is False

    async def test_the_hashing_helper_refuses_defensively(self):
        """For the callers that are not HTTP — a later wizard, a script. It
        refuses rather than truncating: a stored credential whose last bytes
        were dropped is not the password its owner set."""
        from nerve.gateway.auth import PasswordTooLongError, hash_password

        with pytest.raises(PasswordTooLongError):
            hash_password(_OVER_THE_LIMIT)
        with pytest.raises(PasswordTooLongError):
            hash_password(_MULTIBYTE_OVER)
        with pytest.raises(ValueError):
            hash_password("")
        assert hash_password(_AT_THE_LIMIT)
