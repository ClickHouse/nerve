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
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
import yaml
from fastapi import FastAPI

from nerve import setup_state, setup_token
from nerve.config import AuthConfig, NerveConfig, set_config
from nerve.db.accounts import JWT_SECRET_NAME
from nerve.gateway.auth import (
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

        The process clock matters because a *secret* is recorded as "present"
        rather than as itself, so "was this written after the daemon started"
        is the only way to tell a key that is in force from one that replaced
        it and is waiting.
        """
        setup_state.PROCESS_STARTED = datetime.now(timezone.utc)
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
def process_clock(monkeypatch):
    """Start each test with a process older than every write it makes.

    ``setup_state.PROCESS_STARTED`` is a module global set at import, which in
    production is the daemon starting. ``_Install.restarted()`` moves it; this
    puts it back.
    """
    monkeypatch.setattr(
        setup_state, "PROCESS_STARTED", datetime.now(timezone.utc),
    )


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

        Nerve's guard reads ``scope["client"]`` and no header — but uvicorn
        runs ``ProxyHeadersMiddleware`` by default, which rewrites that value
        from ``X-Forwarded-For`` when the immediate peer is trusted
        (``127.0.0.1``). So the real question is whether that rewrite can ever
        turn a remote caller into a local one, and the answer has to be no:
        the rewrite only happens for a peer that was *already* loopback, and
        for a remote peer the header is ignored outright.

        This drives the endpoint through the real middleware rather than
        reasoning about it.
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

        With a reverse proxy on this host, the peer is loopback and the guard
        would let the claim through — that is the documented limitation. When
        the proxy passes the real client on, uvicorn replaces the peer with it
        and the token is demanded after all. Worth pinning, because it is the
        direction that could otherwise regress silently.
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

    async def test_the_automation_step_toggles_the_crons_the_installer_wrote(
        self, claimed,
    ):
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
        assert claimed.tracked()["sync"]["gmail"]["enabled"] is False

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
        async with _http(claimed) as http:
            response = await http.post("/api/system/restart")
        assert response.status_code == 200, response.text
        assert response.json()["restarting"] is True
        assert calls, "the background task did not run"
        config_dir, kwargs = calls[0]
        assert config_dir == claimed.config_dir
        assert kwargs["old_pid"] == os.getpid(), "this process is the daemon"

    async def test_a_restart_that_cannot_start_does_not_break_the_response(
        self, claimed, monkeypatch,
    ):
        def _boom(config_dir, **kwargs):
            raise OSError("no")

        monkeypatch.setattr(setup_routes.daemon, "restart_daemon", _boom)
        async with _http(claimed) as http:
            response = await http.post("/api/system/restart")
        assert response.status_code == 200


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
