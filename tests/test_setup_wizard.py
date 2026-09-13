"""The first-run wizard: who may claim an instance, and what the checklist writes.

The security-relevant half is the claim guard, so most of this file is about
the ways a caller might try to get past it: a forged header, an IPv6 spelling
of loopback that a string comparison would miss, a second claim racing the
first, and the password endpoint that needs no current password on exactly the
account this exists to secure.

The rest is the checklist: every step idempotent, skippable and re-enterable,
nothing written under lockdown, and nothing anywhere that rotates the signing
secret — because the wizard ends in a restart and the browser has to come back
still signed in.
"""

from __future__ import annotations

import asyncio
import os
import stat
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import jwt
import pytest
import pytest_asyncio
import yaml
from fastapi import FastAPI

from nerve import boot, setup_state, setup_token
from nerve.config import AuthConfig, NerveConfig, set_config
from nerve.db.accounts import JWT_SECRET_NAME
from nerve.gateway.auth import (
    JWT_ALGORITHM,
    create_session_token,
    hash_password,
    pin_jwt_secret,
    verify_password,
)
from nerve.gateway.routes import accounts as accounts_routes
from nerve.gateway.routes import auth as auth_routes
from nerve.gateway.routes import setup as setup_routes
from nerve.setup_writer import (
    SetupChoices,
    write_config_local_yaml,
    write_config_yaml,
    write_cron_jobs,
    write_workspace_settings,
)

_SECRET = "test-secret-for-the-setup-wizard-padded-32b"
_PASSWORD = "correct-horse-battery-staple"
_LOOPBACK = ("127.0.0.1", 41000)
_REMOTE = ("203.0.113.7", 41000)     # TEST-NET-3, never routable
_ANTHROPIC_KEY = "anthropic-key-placeholder"
_TELEGRAM_TOKEN = "0000000000:telegram-bot-token-placeholder"


def _legacy_token(secret: str = _SECRET) -> str:
    """What a browser that logged in before per-account sessions is holding."""
    now = datetime.now(timezone.utc)
    return jwt.encode(
        {"iat": now, "exp": now + timedelta(hours=720), "sub": "user"},
        secret, algorithm=JWT_ALGORITHM,
    )


class _FakeSocket:
    """Enough of a WebSocket for ``authenticate_websocket``: a token and no cookies."""

    def __init__(self, token: str):
        self.query_params = {"token": token}
        self.cookies: dict[str, str] = {}


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(setup_routes.router)
    app.include_router(accounts_routes.router)
    app.include_router(auth_routes.router)
    return app


def _client(app: FastAPI, *, client=_LOOPBACK, token: str = "") -> httpx.AsyncClient:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=client),
        base_url="http://nerve-test",
        headers=headers,
    )


class _Install:
    """A fresh install: real files from the installer's own writers, a real db."""

    def __init__(self, db, identity, app, config_dir: Path, workspace: Path):
        self.db = db
        self.identity = identity
        self.app = app
        self.config_dir = config_dir
        self.workspace = workspace
        self.owner_id = identity.owner_account_id

    @property
    def config_local(self) -> Path:
        return self.config_dir / "config.local.yaml"

    @property
    def config_yaml(self) -> Path:
        return self.config_dir / "config.yaml"

    @property
    def settings(self) -> Path:
        return self.workspace / "config" / "settings.yaml"

    @property
    def system_crons(self) -> Path:
        return self.workspace / "config" / "cron" / "system.yaml"

    def secrets(self) -> dict:
        return yaml.safe_load(self.config_local.read_text(encoding="utf-8")) or {}

    def tracked(self) -> dict:
        return yaml.safe_load(self.settings.read_text(encoding="utf-8")) or {}

    def machine(self) -> dict:
        return yaml.safe_load(self.config_yaml.read_text(encoding="utf-8")) or {}

    async def token(self) -> str:
        """The setup token, as first start would have generated it."""
        return await setup_token.ensure_setup_token(self.db, unclaimed=True)

    async def owner_actor_id(self) -> str:
        account = await self.db.get_account(self.owner_id)
        return account["actor_id"]

    def session_token(self, account_id: str | None = None) -> str:
        return create_session_token(_SECRET, account_id or self.owner_id)

    def restarted(self, **kwargs) -> NerveConfig:
        """Model what a restart does: a new process, with a new config.

        The generation matters as much as the config does. Everything the
        checklist notes down about work in flight is scoped to the process
        that noted it — a secret is recorded as "present" rather than as
        itself, so nothing else can tell a key that is in force from one
        pasted over it — and a new generation is what retires those notes.
        """
        boot.BOOT_ID = secrets.token_hex(8)
        return self.reconfigure(**kwargs)

    def reconfigure(self, **kwargs) -> NerveConfig:
        config = NerveConfig(
            auth=AuthConfig(jwt_secret=_SECRET, **kwargs.pop("auth", {})),
            config_dir=self.config_dir,
            workspace=self.workspace,
            **kwargs,
        )
        set_config(config)
        return config


@pytest.fixture(autouse=True)
def no_ambient_credential(monkeypatch):
    """Answer the provider step from the instance, not from this shell.

    A Docker install is handed its credential in the environment, so the step
    reads it — which means a developer who exports `ANTHROPIC_API_KEY` gets a
    different checklist from one who does not. The tests say which world they
    are in.
    """
    for name in ("ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN"):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(autouse=True)
def process_generation(monkeypatch):
    """One generation per test, restored afterwards.

    ``boot.BOOT_ID`` is a module global set at import, which in production is
    the daemon starting. ``_Install.restarted()`` moves it; this puts it back.
    """
    monkeypatch.setattr(boot, "BOOT_ID", secrets.token_hex(8))


@pytest_asyncio.fixture
async def install(tmp_path, open_identity_db, wire_identity_store):
    """An unclaimed install with real configuration files on disk.

    The files come from :mod:`nerve.setup_writer`, so what the wizard merges
    into is exactly what `nerve init` would have left behind.
    """
    config_dir = tmp_path / "config"
    workspace = tmp_path / "workspace"
    config_dir.mkdir(parents=True)
    choices = SetupChoices(workspace_path=workspace, timezone="UTC")
    write_config_yaml(choices, config_dir)
    write_workspace_settings(choices)
    write_config_local_yaml(choices, config_dir)
    write_cron_jobs(choices)

    set_config(NerveConfig(
        auth=AuthConfig(jwt_secret=_SECRET),
        config_dir=config_dir,
        workspace=workspace,
    ))
    pin_jwt_secret(_SECRET)
    database, identity = await open_identity_db(tmp_path / "nerve.db")
    wire_identity_store(database)
    try:
        yield _Install(database, identity, _app(), config_dir, workspace)
    finally:
        await database.close()
        set_config(NerveConfig())


async def _claim(
    install: _Install, *, client=_LOOPBACK, body: dict | None = None,
    headers: dict | None = None,
) -> httpx.Response:
    payload = {"username": "alice", "password": _PASSWORD}
    payload.update(body or {})
    async with _client(install.app, client=client) as http:
        return await http.post("/api/setup/claim", json=payload, headers=headers or {})


# --------------------------------------------------------------------------- #
#  Claiming                                                                    #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestClaimingFromThisMachine:
    async def test_a_loopback_caller_claims_without_a_token(self, install):
        response = await _claim(install)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["username"] == "alice"
        account = await install.db.get_account(install.owner_id)
        assert account["username"] == "alice"
        assert verify_password(_PASSWORD, account["credential"])

    async def test_the_session_it_returns_works(self, install):
        token = (await _claim(install)).json()["token"]
        async with _client(install.app, token=token) as http:
            checked = await http.get("/api/auth/check")
            me = await http.get("/api/auth/me")
        assert checked.status_code == 200
        assert me.status_code == 200
        assert me.json()["username"] == "alice"

    async def test_it_names_the_account_that_was_already_there(self, install):
        """Not "create": the actor every earlier row points at must not move."""
        before = await install.owner_actor_id()
        response = await _claim(install, body={"display_name": "Alice Example"})
        assert response.status_code == 200
        after = await install.owner_actor_id()
        assert after == before
        assert response.json()["actor_id"] == before
        ref = await install.db.get_actor_ref(before)
        assert ref["display_name"] == "Alice Example"
        assert await install.db.count_accounts() == 1

    async def test_the_token_is_invalidated(self, install):
        await install.token()
        assert (await _claim(install)).status_code == 200
        assert await setup_token.stored_setup_token(install.db) == ""

    async def test_a_second_claim_is_refused(self, install):
        assert (await _claim(install)).status_code == 200
        second = await _claim(install, body={"username": "bob"})
        assert second.status_code == 409
        account = await install.db.get_account(install.owner_id)
        assert account["username"] == "alice"
        assert verify_password(_PASSWORD, account["credential"])

    async def test_two_claims_at_once_leave_exactly_one_winner(self, install):
        async with _client(install.app) as http:
            first, second = await asyncio.gather(
                http.post("/api/setup/claim", json={
                    "username": "alice", "password": _PASSWORD,
                }),
                http.post("/api/setup/claim", json={
                    "username": "bob", "password": "a-different-passphrase",
                }),
            )
        codes = sorted([first.status_code, second.status_code])
        assert codes == [200, 409], (first.text, second.text)
        account = await install.db.get_account(install.owner_id)
        winner = "alice" if first.status_code == 200 else "bob"
        assert account["username"] == winner

    async def test_a_password_bcrypt_cannot_hold_is_a_400(self, install):
        response = await _claim(install, body={"password": "x" * 200})
        assert response.status_code == 400
        # Said in bytes, like the accounts routes say it: "too long" is not
        # actionable on a password whose length the person can see.
        assert "72" in response.json()["detail"]
        assert "bytes" in response.json()["detail"]
        assert await setup_token.instance_is_unclaimed(
            install.db, install.reconfigure(),
        ) is True

    @pytest.mark.parametrize("username", ["user", "A B", "x", "admin"])
    async def test_a_username_the_rules_refuse_is_a_400(self, install, username):
        response = await _claim(install, body={"username": username})
        assert response.status_code == 400

    async def test_the_response_never_carries_the_setup_token(self, install):
        token = await install.token()
        response = await _claim(install)
        assert token not in response.text

    async def test_a_configured_password_means_the_instance_is_not_unclaimed(
        self, install,
    ):
        """PR 3's D3 window: the row says ``none``, configuration says
        otherwise, and configuration is what authenticates until the next
        restart re-derives the row.

        The claim reads the shared predicate rather than the row, so it cannot
        be used to take over an install whose password arrived by a config
        reload — which the row-only precondition inside ``claim_sole_account``
        would happily allow.
        """
        install.reconfigure(auth={"password_hash": hash_password(_PASSWORD)})
        response = await _claim(install)
        assert response.status_code == 409, response.text
        account = await install.db.get_account(install.owner_id)
        assert account["username"] is None
        assert not account["credential"]

    async def test_the_password_endpoint_is_closed_in_that_window_too(self, install):
        """...and the side door stays shut for the same reason: with a
        configured password the account is not credential-less, so changing it
        needs the current one."""
        install.reconfigure(auth={"password_hash": hash_password(_PASSWORD)})
        async with _client(install.app, token=install.session_token()) as http:
            response = await http.put(
                "/api/accounts/me/password", json={"new_password": "taken-over"},
            )
        assert response.status_code == 403
        assert not (await install.db.get_account(install.owner_id))["credential"]


@pytest.mark.asyncio
class TestClaimingFromSomewhereElse:
    async def test_a_remote_caller_without_a_token_is_refused(self, install):
        await install.token()
        response = await _claim(install, client=_REMOTE)
        assert response.status_code == 403
        assert await setup_token.instance_is_unclaimed(
            install.db, install.reconfigure(),
        ) is True

    async def test_a_wrong_token_is_refused_in_the_same_words(self, install):
        await install.token()
        missing = await _claim(install, client=_REMOTE)
        wrong = await _claim(
            install, client=_REMOTE, body={"setup_token": "not-the-token"},
        )
        assert missing.status_code == wrong.status_code == 403
        assert missing.json() == wrong.json()

    async def test_the_right_token_lets_a_remote_caller_in(self, install):
        token = await install.token()
        response = await _claim(
            install, client=_REMOTE, body={"setup_token": token},
        )
        assert response.status_code == 200, response.text
        account = await install.db.get_account(install.owner_id)
        assert account["username"] == "alice"

    @pytest.mark.parametrize("headers", [
        {"X-Forwarded-For": "127.0.0.1"},
        {"X-Forwarded-For": "127.0.0.1, 203.0.113.7"},
        {"Forwarded": "for=127.0.0.1;proto=http"},
        {"X-Real-IP": "::1"},
        {"Host": "localhost"},
    ])
    async def test_a_forwarded_header_cannot_make_a_caller_local(
        self, install, headers,
    ):
        await install.token()
        response = await _claim(install, client=_REMOTE, headers=headers)
        assert response.status_code == 403, headers

    async def test_uvicorn_own_proxy_header_handling_cannot_relax_the_guard(
        self, install,
    ):
        """The one place a header *can* reach the peer address, checked.

        Nerve's listener disables that middleware (see
        ``test_setup_token.py``), so in production it is not in the path at
        all. Driven through it here anyway, because "we turned it off" and
        "it could not hurt us if it were on" are different claims and the
        second one is the one that survives somebody turning it back on.
        """
        from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

        await install.token()
        fronted = ProxyHeadersMiddleware(install.app)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=fronted, client=_REMOTE),
            base_url="http://nerve-test",
        ) as http:
            response = await http.post(
                "/api/setup/claim",
                json={"username": "alice", "password": _PASSWORD},
                headers={"X-Forwarded-For": "127.0.0.1"},
            )
        assert response.status_code == 403, response.text

    async def test_a_proxy_that_forwards_the_real_client_makes_it_stricter(
        self, install,
    ):
        """And the same middleware in the deployment the switch exists for.

        With a reverse proxy on this host the peer is loopback and the guard
        would let the claim through — the documented limitation, and why
        ``auth.setup_token_required`` exists. *If* that middleware were in the
        path and the proxy forwarded the real client, the token would be
        demanded after all. Nerve's own listener does not run it, so this is
        the behaviour of the guard rather than a property anybody should
        depend on.
        """
        from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

        token = await install.token()
        fronted = ProxyHeadersMiddleware(install.app)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=fronted, client=_LOOPBACK),
            base_url="http://nerve-test",
        ) as http:
            refused = await http.post(
                "/api/setup/claim",
                json={"username": "alice", "password": _PASSWORD},
                headers={"X-Forwarded-For": "203.0.113.7"},
            )
            accepted = await http.post(
                "/api/setup/claim",
                json={
                    "username": "alice", "password": _PASSWORD,
                    "setup_token": token,
                },
                headers={"X-Forwarded-For": "203.0.113.7"},
            )
        assert refused.status_code == 403, refused.text
        assert accepted.status_code == 200, accepted.text

    async def test_with_no_token_stored_a_remote_caller_can_never_claim(self, install):
        """The guard fails closed: nothing to match means nothing matches."""
        assert await setup_token.stored_setup_token(install.db) == ""
        for supplied in ("", "guess"):
            response = await _claim(
                install, client=_REMOTE, body={"setup_token": supplied},
            )
            assert response.status_code == 403

    async def test_the_switch_forces_a_token_from_a_local_caller_too(self, install):
        install.reconfigure(auth={"setup_token_required": True})
        token = await install.token()
        refused = await _claim(install)
        assert refused.status_code == 403
        accepted = await _claim(install, body={"setup_token": token})
        assert accepted.status_code == 200

    async def test_the_guard_is_judged_before_the_instance_state(self, install):
        """A refused caller learns nothing about the instance here.

        After a claim, a remote caller with no token still gets the guard's
        403 rather than "already claimed" — which is public anyway, but the
        ordering is what keeps the endpoint from answering questions for
        callers who have not passed the door.
        """
        await install.token()
        assert (await _claim(install)).status_code == 200
        response = await _claim(install, client=_REMOTE, body={"username": "bob"})
        assert response.status_code == 403


# --------------------------------------------------------------------------- #
#  What the claim does to the sessions that came before it                     #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestClaimingEndsTheSessionsBeforeIt:
    """The point of step one, and the thing it did not do in round 1.

    A passwordless install hands a session to everybody who reaches it. Those
    tokens name the account the claim secures, are signed with the same secret
    and have thirty days left, so unless the claim ends them it secures the
    *next* caller and leaves every previous one with owner authority — which is
    the window the claim exists to close.
    """

    async def test_a_session_from_before_the_claim_is_refused_after_it(
        self, install,
    ):
        # What any visitor gets on a passwordless install: an ordinary login.
        async with _client(install.app) as http:
            before = (await http.post(
                "/api/auth/login", json={"password": "anything at all"},
            )).json()["token"]
        async with _client(install.app, token=before) as http:
            assert (await http.get("/api/auth/check")).status_code == 200

        assert (await _claim(install)).status_code == 200

        async with _client(install.app, token=before) as http:
            after = await http.get("/api/auth/check")
        assert after.status_code == 401, after.text

    async def test_the_claimer_keeps_working(self, install):
        claimed = (await _claim(install)).json()["token"]
        async with _client(install.app, token=claimed) as http:
            assert (await http.get("/api/auth/check")).status_code == 200
            assert (await http.get("/api/setup")).status_code == 200

    async def test_a_websocket_holding_one_is_refused_too(self, install):
        """The other door into the same identity."""
        from nerve.gateway.auth import authenticate_websocket

        async with _client(install.app) as http:
            before = (await http.post(
                "/api/auth/login", json={"password": "anything at all"},
            )).json()["token"]

        socket = _FakeSocket(before)
        assert await authenticate_websocket(socket) is not None

        assert (await _claim(install)).status_code == 200
        assert await authenticate_websocket(_FakeSocket(before)) is None

    async def test_a_legacy_token_dies_with_them(self, install):
        """A tab from before per-account sessions carries no epoch at all, so
        it reads as 0 and stops the moment the account is claimed — which is
        right: it was minted while the instance admitted everybody."""
        async with _client(install.app, token=_legacy_token()) as http:
            assert (await http.get("/api/auth/check")).status_code == 200
        assert (await _claim(install)).status_code == 200
        async with _client(install.app, token=_legacy_token()) as http:
            assert (await http.get("/api/auth/check")).status_code == 401

    async def test_an_upgrade_logs_nobody_out(self, install):
        """The epoch starts at 0 and a token minted before the column existed
        carries none, which reads as 0. An install that has never been claimed
        therefore keeps its sessions across the upgrade; only a claim ends
        them."""
        token = create_session_token(_SECRET, install.owner_id)  # no epoch claim
        async with _client(install.app, token=token) as http:
            assert (await http.get("/api/auth/check")).status_code == 200

    async def test_a_restart_does_not_end_a_session(self, claimed):
        """The epoch lives on the account row, not in the process: the wizard
        ends in a restart and the browser has to come back signed in."""
        async with _http(claimed) as http:
            assert (await http.get("/api/auth/check")).status_code == 200
        claimed.restarted()
        async with _http(claimed) as http:
            assert (await http.get("/api/auth/check")).status_code == 200

    async def test_the_epoch_moves_once_and_only_on_a_claim(self, install):
        async def epoch() -> int:
            account = await install.db.get_account(install.owner_id)
            return int(account["session_epoch"])

        assert await epoch() == 0
        assert (await _claim(install)).status_code == 200
        assert await epoch() == 1

        # A refused second claim does not move it, and neither does an
        # ordinary login or a password change.
        assert (await _claim(install, body={"username": "bob"})).status_code == 409
        async with _client(install.app, token=install.session_token()) as http:
            await http.put("/api/accounts/me/password", json={
                "current_password": _PASSWORD, "new_password": "a-third-one",
            })
        assert await epoch() == 1

    async def test_a_session_for_another_account_is_unaffected(self, claimed):
        """The epoch is per account. Claiming one instance's account must not
        reach into anybody else's session — there is only one account here, so
        this pins the column rather than a global."""
        second = await claimed.db.create_managed_account(
            username="bob", credential=hash_password(_PASSWORD),
        )
        token = create_session_token(
            _SECRET, second["id"],
            session_epoch=int(second["session_epoch"] or 0),
        )
        async with _client(claimed.app, token=token) as http:
            assert (await http.get("/api/auth/check")).status_code == 200


# --------------------------------------------------------------------------- #
#  The side door                                                               #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestThePasswordEndpointIsNotASecondDoor:
    async def test_it_refuses_while_the_instance_is_unclaimed(self, install):
        """The one endpoint that needs no current password is the one an
        unclaimed install would hand to anybody."""
        async with _client(install.app, token=install.session_token()) as http:
            response = await http.put(
                "/api/accounts/me/password", json={"new_password": "taken-over"},
            )
        assert response.status_code == 409
        assert "/api/setup/claim" in response.json()["detail"]
        account = await install.db.get_account(install.owner_id)
        assert not account["credential"]

    async def test_it_works_again_once_the_account_is_claimed(self, install):
        token = (await _claim(install)).json()["token"]
        async with _client(install.app, token=token) as http:
            without = await http.put(
                "/api/accounts/me/password", json={"new_password": "next-one"},
            )
            with_current = await http.put("/api/accounts/me/password", json={
                "current_password": _PASSWORD, "new_password": "next-one-please",
            })
        assert without.status_code == 403      # the current password is required
        assert with_current.status_code == 200
        account = await install.db.get_account(install.owner_id)
        assert verify_password("next-one-please", account["credential"])

    async def test_the_claim_is_the_only_unauthenticated_write(self, install):
        """Every other setup endpoint needs a session."""
        async with _client(install.app) as http:
            for method, path in (
                ("get", "/api/setup"),
                ("put", "/api/setup/provider"),
                ("put", "/api/setup/profile"),
                ("put", "/api/setup/channels"),
                ("put", "/api/setup/automation"),
                ("post", "/api/setup/steps/provider/skip"),
                ("post", "/api/system/restart"),
                ("get", "/api/auth/me"),
            ):
                call = getattr(http, method)
                response = await call(path, json={}) if method != "get" else await call(path)
                assert response.status_code == 401, (path, response.status_code)


# --------------------------------------------------------------------------- #
#  The checklist                                                               #
# --------------------------------------------------------------------------- #


@pytest_asyncio.fixture
async def claimed(install):
    """An install past step one, with a session token for its owner."""
    response = await _claim(install)
    assert response.status_code == 200
    install.token_for_owner = response.json()["token"]
    return install


def _http(install, **kwargs) -> httpx.AsyncClient:
    return _client(install.app, token=install.token_for_owner, **kwargs)


@pytest.mark.asyncio
class TestTheChecklist:
    async def test_the_account_step_is_the_only_required_one(self, claimed):
        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        required = [s["id"] for s in state["steps"] if s["required"]]
        assert required == ["account"]
        assert state["setup_pending"] is False
        account = next(s for s in state["steps"] if s["id"] == "account")
        assert account["status"] == "done"
        assert account["can_skip"] is False

    async def test_an_unclaimed_instance_says_so(self, install):
        async with _client(install.app, token=install.session_token()) as http:
            state = (await http.get("/api/setup")).json()
        assert state["setup_pending"] is True
        account = next(s for s in state["steps"] if s["id"] == "account")
        assert account["status"] == "pending"
        assert state["finished"] is False

    async def test_a_provider_key_is_written_privately_and_only_there(self, claimed):
        async with _http(claimed) as http:
            response = await http.put(
                "/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY},
            )
        assert response.status_code == 200, response.text
        assert claimed.secrets()["anthropic_api_key"] == _ANTHROPIC_KEY
        assert _ANTHROPIC_KEY not in claimed.settings.read_text(encoding="utf-8")
        assert _ANTHROPIC_KEY not in claimed.config_yaml.read_text(encoding="utf-8")
        mode = stat.S_IMODE(claimed.config_local.stat().st_mode)
        assert mode == 0o600, f"{mode:04o}"

    async def test_writing_a_step_twice_changes_nothing_the_second_time(self, claimed):
        async with _http(claimed) as http:
            await http.put("/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY})
            once = claimed.config_local.read_text(encoding="utf-8")
            second = await http.put(
                "/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY},
            )
        assert second.status_code == 200
        assert claimed.config_local.read_text(encoding="utf-8") == once

    async def test_an_empty_provider_step_is_refused_rather_than_written(self, claimed):
        async with _http(claimed) as http:
            response = await http.put("/api/setup/provider", json={})
        assert response.status_code == 400
        assert "anthropic_api_key" not in claimed.secrets()

    async def test_the_profile_step_writes_the_timezone_to_the_tracked_layer(
        self, claimed,
    ):
        async with _http(claimed) as http:
            response = await http.put(
                "/api/setup/profile", json={"timezone": "Europe/Berlin"},
            )
        assert response.status_code == 200, response.text
        assert claimed.tracked()["timezone"] == "Europe/Berlin"
        # The wizard writes a portable value to exactly one layer.
        assert "timezone" not in claimed.machine()

    async def test_the_profile_step_renames_the_existing_actor(self, claimed):
        before = await claimed.owner_actor_id()
        async with _http(claimed) as http:
            response = await http.put(
                "/api/setup/profile", json={"display_name": "Alice Example"},
            )
        assert response.status_code == 200
        assert await claimed.owner_actor_id() == before
        ref = await claimed.db.get_actor_ref(before)
        assert ref["display_name"] == "Alice Example"
        assert len(await claimed.db.list_actor_refs(kind="human")) == 1

    async def test_a_time_zone_this_machine_does_not_know_is_refused(self, claimed):
        async with _http(claimed) as http:
            response = await http.put(
                "/api/setup/profile", json={"timezone": "Mars/Olympus_Mons"},
            )
        assert response.status_code == 400
        assert claimed.tracked()["timezone"] == "UTC"

    async def test_the_channel_step_splits_the_token_from_the_switch(self, claimed):
        async with _http(claimed) as http:
            response = await http.put("/api/setup/channels", json={
                "telegram_bot_token": _TELEGRAM_TOKEN,
                "telegram_allowed_users": [4242],
            })
        assert response.status_code == 200, response.text
        assert claimed.secrets()["telegram"]["bot_token"] == _TELEGRAM_TOKEN
        assert claimed.secrets()["telegram"]["allowed_users"] == [4242]
        assert claimed.machine()["telegram"]["enabled"] is True
        assert _TELEGRAM_TOKEN not in claimed.settings.read_text(encoding="utf-8")

    async def test_the_machine_file_keeps_its_mode_and_owner(self, claimed):
        """`config.yaml` carries no secret and an operator edits it by hand.

        In Docker the container is root over a bind-mounted checkout, so a
        wizard write that published a fresh root-owned 0600 inode would leave
        the host's own non-root CLI unable to read the file it needs to
        recognise a Docker install at all. The private writer is for the file
        that holds credentials.
        """
        claimed.config_yaml.chmod(0o644)
        before = claimed.config_yaml.stat()
        async with _http(claimed) as http:
            response = await http.put("/api/setup/channels", json={
                "telegram_bot_token": _TELEGRAM_TOKEN,
            })
        assert response.status_code == 200, response.text
        after = claimed.config_yaml.stat()
        assert stat.S_IMODE(after.st_mode) == 0o644
        assert (after.st_uid, after.st_gid) == (before.st_uid, before.st_gid)
        assert claimed.machine()["telegram"]["enabled"] is True
        # ...while the file that does hold a credential stays owner-only.
        assert stat.S_IMODE(claimed.config_local.stat().st_mode) == 0o600

    async def test_a_machine_write_is_atomic_and_leaves_no_temporary(self, claimed):
        async with _http(claimed) as http:
            await http.put("/api/setup/channels", json={
                "telegram_bot_token": _TELEGRAM_TOKEN,
            })
        assert not (claimed.config_dir / "config.yaml.tmp").exists()
        assert claimed.machine()["workspace"], "the rest of the file survived"

    async def test_the_automation_step_toggles_the_crons_the_installer_wrote(
        self, claimed,
    ):
        before = claimed.tracked()["sync"]["gmail"]
        async with _http(claimed) as http:
            response = await http.put("/api/setup/automation", json={
                "crons": ["inbox-processor"], "github": True,
            })
        assert response.status_code == 200, response.text
        jobs = {
            job["id"]: job["enabled"]
            for job in yaml.safe_load(
                claimed.system_crons.read_text(encoding="utf-8"),
            )["jobs"]
        }
        assert jobs["inbox-processor"] is True
        assert jobs["task-planner"] is False
        assert jobs["memory-maintenance"] is True, "a core cron is never touched"
        assert claimed.tracked()["sync"]["github"]["enabled"] is True
        # Gmail was not mentioned, so it is exactly what the installer left.
        assert claimed.tracked()["sync"]["gmail"] == before

    async def test_re_entering_it_changes_only_what_was_asked_for(self, claimed):
        """The blocker: a step entered again to turn one cron on used to
        switch off every sync source configured anywhere else, and clear the
        Gmail addresses with them. Omitted means untouched."""
        async with _http(claimed) as http:
            await http.put("/api/setup/automation", json={
                "github": True, "gmail": True,
                "gmail_accounts": ["alice@example.invalid"],
                "crons": [],
            })
            settled = claimed.tracked()
            machine = claimed.machine()

            await http.put("/api/setup/automation", json={"crons": ["inbox-processor"]})

        assert claimed.tracked() == settled, "a cron change rewrote the sync settings"
        assert claimed.machine() == machine, "a cron change cleared the mailboxes"
        jobs = {
            job["id"]: job["enabled"]
            for job in yaml.safe_load(
                claimed.system_crons.read_text(encoding="utf-8"),
            )["jobs"]
        }
        assert jobs["inbox-processor"] is True

    async def test_the_automation_step_is_re_enterable(self, claimed):
        async with _http(claimed) as http:
            await http.put("/api/setup/automation", json={"crons": ["inbox-processor"]})
            response = await http.put(
                "/api/setup/automation", json={"crons": ["task-planner"]},
            )
        assert response.status_code == 200
        jobs = {
            job["id"]: job["enabled"]
            for job in yaml.safe_load(
                claimed.system_crons.read_text(encoding="utf-8"),
            )["jobs"]
        }
        assert jobs["inbox-processor"] is False
        assert jobs["task-planner"] is True

    async def test_the_crons_on_offer_come_from_the_file(self, claimed):
        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        offered = {c["id"] for c in state["crons"]}
        assert offered == {
            "inbox-processor", "task-planner", "skill-extractor", "skill-reviser",
        }
        assert all(c["description"] for c in state["crons"])

    async def test_a_step_can_be_skipped_and_re_entered(self, claimed):
        async with _http(claimed) as http:
            skipped = (await http.post("/api/setup/steps/channels/skip")).json()
            assert _status_of(skipped, "channels") == "skipped"

            unskipped = (await http.post("/api/setup/steps/channels/unskip")).json()
            assert _status_of(unskipped, "channels") == "pending"

            await http.post("/api/setup/steps/channels/skip")
            written = (await http.put("/api/setup/channels", json={
                "telegram_bot_token": _TELEGRAM_TOKEN,
            })).json()
        assert _status_of(written, "channels") == "done"

    async def test_the_required_step_cannot_be_skipped(self, claimed):
        async with _http(claimed) as http:
            response = await http.post("/api/setup/steps/account/skip")
        assert response.status_code == 400

    async def test_an_unknown_step_is_a_400(self, claimed):
        async with _http(claimed) as http:
            response = await http.post("/api/setup/steps/nonsense/skip")
        assert response.status_code == 400

    async def test_skipping_survives_a_restart(self, claimed):
        """The one thing that cannot be derived is the one thing that is stored."""
        async with _http(claimed) as http:
            await http.post("/api/setup/steps/channels/skip")
        assert setup_state.load_state().skipped == {"channels"}
        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        assert _status_of(state, "channels") == "skipped"

    async def test_a_restart_is_reported_as_pending_until_it_happens(self, claimed):
        async with _http(claimed) as http:
            state = (await http.put(
                "/api/setup/profile", json={"timezone": "Europe/Berlin"},
            )).json()
        assert state["restart_pending"] is True
        assert state["restart_pending_paths"] == ["timezone"]
        assert state["finished"] is False

        # What a restart does: the process comes back with the written value.
        claimed.restarted(timezone="Europe/Berlin")
        async with _http(claimed) as http:
            after = (await http.get("/api/setup")).json()
        assert after["restart_pending"] is False
        assert after["restart_pending_paths"] == []

    async def test_replacing_a_credential_that_is_already_live_is_pending_too(
        self, claimed,
    ):
        """The case the value comparison alone cannot see.

        A secret is recorded as "present", never as itself, so a key pasted
        over an existing one reads as present either way while only the old one
        is in force. Without this the checklist would say nothing is waiting on
        a restart and the new key would sit on disk unused.
        """
        claimed.reconfigure(anthropic_api_key="the-key-this-process-started-with")
        async with _http(claimed) as http:
            state = (await http.put(
                "/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY},
            )).json()
        assert state["restart_pending"] is True
        assert state["restart_pending_paths"] == ["anthropic_api_key"]
        assert claimed.secrets()["anthropic_api_key"] == _ANTHROPIC_KEY

    async def test_a_credential_written_by_an_earlier_process_is_not_pending(
        self, claimed,
    ):
        """...and it clears itself at the restart, rather than nagging forever."""
        async with _http(claimed) as http:
            await http.put(
                "/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY},
            )
        claimed.restarted(anthropic_api_key=_ANTHROPIC_KEY)
        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        assert state["restart_pending"] is False

    async def test_a_step_stops_being_done_when_its_reason_stops_being_true(
        self, claimed,
    ):
        """A note the wizard left is not evidence forever.

        The provider step is "done" because a credential is configured. Once a
        later process has started, the instance is the witness — so a
        credential removed by hand afterwards leaves the step to do again,
        rather than green because of something the wizard wrote down months
        ago.
        """
        async with _http(claimed) as http:
            saved = (await http.put(
                "/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY},
            )).json()
        assert _status_of(saved, "provider") == "done"

        # A restart, and the operator has since taken the key back out.
        claimed.restarted()
        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        assert _status_of(state, "provider") == "pending"
        assert state["restart_pending"] is False, (
            "a restart that has happened must stop being reported as pending"
        )

    async def test_a_skip_is_a_decision_and_survives(self, claimed):
        """What is retired is what the instance can answer for itself. "I do
        not want Telegram" is not one of those."""
        async with _http(claimed) as http:
            await http.post("/api/setup/steps/channels/skip")
        claimed.restarted()
        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        assert _status_of(state, "channels") == "skipped"

    async def test_the_automation_answer_survives_too(self, claimed):
        """Which crons an operator wanted is a decision nothing else records,
        so it is not transitional."""
        async with _http(claimed) as http:
            await http.put("/api/setup/automation", json={"crons": ["inbox-processor"]})
        claimed.restarted()
        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        assert _status_of(state, "automation") == "done"

    async def test_finished_once_everything_is_answered(self, claimed):
        async with _http(claimed) as http:
            await http.put("/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY})
            await http.put("/api/setup/profile", json={"display_name": "Alice Example"})
            await http.post("/api/setup/steps/channels/skip")
            state = (await http.post("/api/setup/steps/automation/skip")).json()
        # The provider key is the one path still waiting for a restart.
        assert state["restart_pending"] is True
        claimed.restarted(anthropic_api_key=_ANTHROPIC_KEY)
        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        assert state["restart_pending"] is False
        assert state["finished"] is True


@pytest.mark.asyncio
class TestOneMutationAtATime:
    """Two tabs, or one tab's form and another's skip.

    Every step reads the checklist's notes, changes them and writes them back,
    so without a lock each request saves over a snapshot taken before the
    other — and a skip disappears because a provider save that started first
    finished last.
    """

    async def test_concurrent_steps_all_survive(self, claimed):
        async with _http(claimed) as http:
            responses = await asyncio.gather(
                http.put("/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY}),
                http.post("/api/setup/steps/channels/skip"),
                http.put("/api/setup/profile", json={"display_name": "Alice Example"}),
                http.put("/api/setup/automation", json={"crons": []}),
            )
        assert [r.status_code for r in responses] == [200, 200, 200, 200]

        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        assert _status_of(state, "provider") == "done"
        assert _status_of(state, "channels") == "skipped"
        assert _status_of(state, "profile") == "done"
        assert _status_of(state, "automation") == "done"

    async def test_the_notes_on_disk_agree_with_the_answer(self, claimed):
        async with _http(claimed) as http:
            await asyncio.gather(*[
                http.post(f"/api/setup/steps/{step}/skip")
                for step in ("provider", "channels", "automation")
            ])
        assert setup_state.load_state().skipped == {
            "provider", "channels", "automation",
        }


@pytest.mark.asyncio
class TestNothingHalfLands:
    """A step that writes to more than one place either does all of it or
    answers with an error for work it has not done."""

    async def test_an_unusable_settings_file_stops_the_step_before_the_rename(
        self, claimed,
    ):
        claimed.settings.write_text("- this is a list\n", encoding="utf-8")
        async with _http(claimed) as http:
            response = await http.put("/api/setup/profile", json={
                "timezone": "Europe/Berlin", "display_name": "Alice Example",
            })
        assert response.status_code == 409
        assert "settings" in response.json()["detail"]
        ref = await claimed.db.get_actor_ref(await claimed.owner_actor_id())
        assert ref["display_name"] is None, "the rename landed anyway"

    async def test_a_refused_step_is_not_recorded_as_done(self, claimed):
        claimed.settings.write_text("- this is a list\n", encoding="utf-8")
        async with _http(claimed) as http:
            assert (await http.put(
                "/api/setup/automation", json={"github": True, "crons": ["inbox-processor"]},
            )).status_code == 409
        assert "automation" not in setup_state.load_state().done
        jobs = yaml.safe_load(claimed.system_crons.read_text(encoding="utf-8"))["jobs"]
        assert all(
            not job["enabled"] for job in jobs if job["id"] == "inbox-processor"
        ), "the cron file was published for a step that failed"


@pytest.mark.asyncio
class TestCronChangesAreAppliedOrOwned:
    """A rewritten cron file the scheduler has not re-read is a change that
    has not happened — and a checklist saying "nothing is waiting" over it is
    the one wrong answer that matters, because then nobody restarts."""

    async def test_an_unreachable_scheduler_becomes_restart_debt(self, claimed):
        async with _http(claimed) as http:
            state = (await http.put(
                "/api/setup/automation", json={"crons": ["inbox-processor"]},
            )).json()
        assert state["restart_pending"] is True
        assert any("scheduler" in reason for reason in state["restart_pending_reasons"])
        assert state["finished"] is False

    async def test_a_live_scheduler_is_reloaded_and_owes_nothing(
        self, claimed, monkeypatch,
    ):
        reloads = []

        class _Cron:
            async def reload(self):
                reloads.append(True)
                return {"jobs": 1}

        import nerve.gateway.server as server_module

        monkeypatch.setattr(server_module, "_cron_service", _Cron(), raising=False)
        async with _http(claimed) as http:
            state = (await http.put(
                "/api/setup/automation", json={"crons": ["inbox-processor"]},
            )).json()
        assert reloads, "the scheduler was never told"
        assert state["restart_pending_reasons"] == []

    async def test_a_reload_that_fails_is_debt_rather_than_an_error(
        self, claimed, monkeypatch,
    ):
        class _Cron:
            async def reload(self):
                raise RuntimeError("no scheduler today")

        import nerve.gateway.server as server_module

        monkeypatch.setattr(server_module, "_cron_service", _Cron(), raising=False)
        async with _http(claimed) as http:
            state = (await http.put(
                "/api/setup/automation", json={"crons": ["inbox-processor"]},
            )).json()
        # The file was written; only the applying failed, and the checklist
        # says so rather than pretending either way.
        assert state["restart_pending"] is True
        assert state["restart_pending_reasons"]

    async def test_a_selection_that_changes_nothing_owes_nothing(
        self, claimed, monkeypatch,
    ):
        async with _http(claimed) as http:
            state = (await http.put(
                "/api/setup/automation", json={"crons": []},
            )).json()
        assert state["restart_pending_reasons"] == []


@pytest.mark.asyncio
class TestAWhitespaceTokenIsNoToken:
    async def test_it_is_refused_rather_than_marked_done(self, claimed):
        async with _http(claimed) as http:
            response = await http.put(
                "/api/setup/channels", json={"telegram_bot_token": "   "},
            )
        assert response.status_code == 400
        assert "channels" not in setup_state.load_state().done
        assert "telegram" not in claimed.secrets()

    async def test_the_allow_list_is_left_alone_when_it_is_not_given(self, claimed):
        """Pairing is how people are added to it; a setup step must not clear
        what pairing built."""
        async with _http(claimed) as http:
            await http.put("/api/setup/channels", json={
                "telegram_bot_token": _TELEGRAM_TOKEN,
                "telegram_allowed_users": [4242],
            })
            await http.put("/api/setup/channels", json={
                "telegram_bot_token": "0000000000:a-replacement-placeholder",
            })
        assert claimed.secrets()["telegram"]["allowed_users"] == [4242]


def _status_of(state: dict, step: str) -> str:
    return next(s["status"] for s in state["steps"] if s["id"] == step)


# --------------------------------------------------------------------------- #
#  Lockdown                                                                    #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestLockdown:
    async def test_the_checklist_says_it_is_read_only(self, claimed):
        claimed.reconfigure(lockdown=True)
        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        assert state["lockdown"] is True
        assert state["writable"] is False
        assert "lockdown" in state["read_only_reason"].lower()

    async def test_no_step_writes_anything(self, claimed):
        before = {
            path: path.read_text(encoding="utf-8")
            for path in (
                claimed.config_local, claimed.config_yaml,
                claimed.settings, claimed.system_crons,
            )
        }
        claimed.reconfigure(lockdown=True)
        async with _http(claimed) as http:
            for path, body in (
                ("/api/setup/provider", {"anthropic_api_key": _ANTHROPIC_KEY}),
                ("/api/setup/profile", {"timezone": "Europe/Berlin"}),
                ("/api/setup/channels", {"telegram_bot_token": _TELEGRAM_TOKEN}),
                ("/api/setup/automation", {"crons": ["inbox-processor"]}),
            ):
                response = await http.put(path, json=body)
                assert response.status_code == 409, (path, response.text)
        for path, text in before.items():
            assert path.read_text(encoding="utf-8") == text, path

    async def test_the_account_can_still_be_claimed(self, install):
        """Deliberate: the claim writes to nerve.db, not to configuration.

        A fleet-managed install that could never be claimed would stay open to
        everyone who can reach it, forever.
        """
        install.reconfigure(lockdown=True)
        response = await _claim(install)
        assert response.status_code == 200, response.text

    async def test_a_request_for_both_halves_lands_neither(self, claimed):
        """The name is a database write and the zone is a file write.

        A request that asked for both must not land the name and then answer
        with the failure of the other — that is a committed write reported as
        a failure, and the form it leaves behind primes a retry that can only
        conflict with what already happened.
        """
        claimed.reconfigure(lockdown=True)
        async with _http(claimed) as http:
            response = await http.put("/api/setup/profile", json={
                "timezone": "Europe/Berlin", "display_name": "Alice Example",
            })
        assert response.status_code == 409
        ref = await claimed.db.get_actor_ref(await claimed.owner_actor_id())
        assert ref["display_name"] is None
        assert claimed.tracked()["timezone"] == "UTC"

    async def test_a_display_name_is_still_allowed(self, claimed):
        """It is a database write too — and the one thing the wizard can
        usefully do on a fleet-managed box."""
        claimed.reconfigure(lockdown=True)
        async with _http(claimed) as http:
            response = await http.put(
                "/api/setup/profile", json={"display_name": "Alice Example"},
            )
        assert response.status_code == 200, response.text
        ref = await claimed.db.get_actor_ref(await claimed.owner_actor_id())
        assert ref["display_name"] == "Alice Example"


# --------------------------------------------------------------------------- #
#  The signing secret, and the restart the wizard ends with                    #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestTheSigningSecretIsNeverRotated:
    async def test_no_step_touches_the_auth_section(self, claimed):
        before = claimed.secrets()["auth"]
        async with _http(claimed) as http:
            await http.put("/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY})
            await http.put("/api/setup/profile", json={"timezone": "Europe/Berlin"})
            await http.put("/api/setup/channels", json={
                "telegram_bot_token": _TELEGRAM_TOKEN,
            })
            await http.put("/api/setup/automation", json={"crons": []})
        assert claimed.secrets()["auth"] == before
        assert claimed.secrets()["auth"]["jwt_secret"]

    async def test_the_claim_leaves_a_stored_secret_alone(self, install):
        """The reconnect after the restart depends on this: the token in
        localStorage has to still verify."""
        await install.db.ensure_instance_secret(JWT_SECRET_NAME, "a-stored-signing-secret-32-bytes!!")
        stored = await install.db.get_instance_secret(JWT_SECRET_NAME)
        token = (await _claim(install)).json()["token"]
        assert await install.db.get_instance_secret(JWT_SECRET_NAME) == stored
        async with _client(install.app, token=token) as http:
            assert (await http.get("/api/auth/check")).status_code == 200

    async def test_a_session_issued_before_the_steps_still_works_after_them(
        self, claimed,
    ):
        async with _http(claimed) as http:
            await http.put("/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY})
            # Same token, after every write: nothing re-keyed the instance.
            assert (await http.get("/api/auth/check")).status_code == 200


@pytest.mark.asyncio
class TestTheRestartStep:
    async def test_it_needs_a_session(self, install):
        async with _client(install.app) as http:
            assert (await http.post("/api/system/restart")).status_code == 401

    async def test_it_uses_the_same_mechanism_the_cli_does(self, claimed, monkeypatch):
        calls = []

        def _fake_restart(config_dir, **kwargs):
            calls.append((Path(config_dir), kwargs))
            return setup_routes.daemon.RestartOutcome(
                method="helper", message="ok", old_pid=kwargs.get("old_pid"),
            )

        monkeypatch.setattr(setup_routes.daemon, "restart_daemon", _fake_restart)
        monkeypatch.setattr(setup_routes, "_restart_requested", False)
        async with _http(claimed) as http:
            response = await http.post("/api/system/restart")
        assert response.status_code == 200, response.text
        assert response.json()["restarting"] is True
        assert calls, "the helper was not started"
        config_dir, kwargs = calls[0]
        assert config_dir == claimed.config_dir
        assert kwargs["old_pid"] == os.getpid(), "this process is the daemon"
        assert kwargs["delay_seconds"] > 0, (
            "the helper must hold off until this response is on the wire"
        )

    async def test_it_says_which_process_answered(self, claimed, monkeypatch):
        """The client waits for a *different* generation. The old process
        answers /health perfectly well while it shuts down, so 'anybody home?'
        accepts the process being replaced."""
        monkeypatch.setattr(
            setup_routes.daemon, "restart_daemon",
            lambda config_dir, **kwargs: setup_routes.daemon.RestartOutcome(
                method="helper", message="ok", old_pid=1,
            ),
        )
        monkeypatch.setattr(setup_routes, "_restart_requested", False)
        async with _http(claimed) as http:
            body = (await http.post("/api/system/restart")).json()
        assert body["boot"] == setup_routes.boot.boot_id()
        assert body["boot"]

    async def test_a_restart_that_cannot_start_is_reported_as_a_failure(
        self, claimed, monkeypatch,
    ):
        """Whether one was *begun* is knowable here, and a page told
        'restarting' for a helper that never existed waits for a process that
        is never coming."""
        def _boom(config_dir, **kwargs):
            raise OSError("no such directory")

        monkeypatch.setattr(setup_routes.daemon, "restart_daemon", _boom)
        monkeypatch.setattr(setup_routes, "_restart_requested", False)
        async with _http(claimed) as http:
            response = await http.post("/api/system/restart")
        assert response.status_code == 500
        assert "no such directory" in response.json()["detail"]
        assert "still running" in response.json()["detail"]

    async def test_a_second_restart_is_refused_rather_than_spawning_another(
        self, claimed, monkeypatch,
    ):
        """Two helpers race over the same pid and the same pid file."""
        calls = []
        monkeypatch.setattr(
            setup_routes.daemon, "restart_daemon",
            lambda config_dir, **kwargs: (
                calls.append(kwargs),
                setup_routes.daemon.RestartOutcome(
                    method="helper", message="ok", old_pid=1,
                ),
            )[1],
        )
        monkeypatch.setattr(setup_routes, "_restart_requested", False)
        async with _http(claimed) as http:
            first = await http.post("/api/system/restart")
            second = await http.post("/api/system/restart")
        assert first.status_code == 200
        assert second.status_code == 409
        assert len(calls) == 1

    async def test_concurrent_restarts_start_exactly_one_helper(
        self, claimed, monkeypatch,
    ):
        calls = []
        monkeypatch.setattr(
            setup_routes.daemon, "restart_daemon",
            lambda config_dir, **kwargs: (
                calls.append(kwargs),
                setup_routes.daemon.RestartOutcome(
                    method="helper", message="ok", old_pid=1,
                ),
            )[1],
        )
        monkeypatch.setattr(setup_routes, "_restart_requested", False)
        async with _http(claimed) as http:
            responses = await asyncio.gather(*[
                http.post("/api/system/restart") for _ in range(4)
            ])
        assert sorted(r.status_code for r in responses) == [200, 409, 409, 409]
        assert len(calls) == 1


# --------------------------------------------------------------------------- #
#  /api/auth/me                                                                #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestWhoAmI:
    async def test_it_says_who_the_caller_is(self, claimed):
        async with _http(claimed) as http:
            body = (await http.get("/api/auth/me")).json()
        assert body["username"] == "alice"
        assert body["kind"] == "human"
        assert body["account_id"] == claimed.owner_id
        assert body["actor_id"] == await claimed.owner_actor_id()

    async def test_it_carries_no_credential(self, claimed):
        await claimed.db.update_account_login(
            claimed.owner_id, credential=hash_password(_PASSWORD),
        )
        async with _http(claimed) as http:
            response = await http.get("/api/auth/me")
        assert set(response.json()) == {
            "actor_id", "account_id", "username", "display_name", "kind",
        }
        assert "$2b$" not in response.text
        assert "credential" not in response.text
