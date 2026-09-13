"""The post-claim setup checklist: what each step writes, and what it refuses.

Step one — the token-guarded claim — is in test_setup_wizard.py. Everything
after it is here: the checklist is *derived* rather than remembered, so an
install set up at the terminal shows the same list as one set up in a browser;
every step is skippable and re-enterable; one mutation runs at a time; nothing
half-lands; and nothing anywhere rotates the signing secret, because the
checklist ends in a restart and the browser has to come back signed in.
"""

from __future__ import annotations

import asyncio
import json
import secrets
import stat
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
import yaml
from fastapi import FastAPI

from nerve import boot, setup_state, setup_token
from nerve import setup_writer as setup_writer_module
from nerve.config import AuthConfig, NerveConfig, TelegramConfig, set_config
from nerve.gateway.auth import create_session_token, pin_jwt_secret
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

_SECRET = "test-secret-for-the-setup-checklist-pad32b"
_PASSWORD = "correct-horse-battery-staple"
_PEER = ("127.0.0.1", 41000)
_ANTHROPIC_KEY = "anthropic-key-placeholder"
_OPENAI_KEY = "openai-key-placeholder"
_TELEGRAM_TOKEN = "0000000000:telegram-bot-token-placeholder"
_TELEGRAM_API_HASH = "telegram-api-hash-placeholder"


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(setup_routes.router)
    app.include_router(accounts_routes.router)
    app.include_router(auth_routes.router)
    return app


def _client(app: FastAPI, *, token: str = "", client=_PEER) -> httpx.AsyncClient:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=client),
        base_url="http://nerve-test",
        headers=headers,
    )


def _status_of(state: dict, step: str) -> str:
    return next(s["status"] for s in state["steps"] if s["id"] == step)


class _Install:
    """A fresh install: real files from the installer's own writers, a real db."""

    def __init__(self, db, identity, config_dir: Path, workspace: Path, token: str):
        self.db = db
        self.identity = identity
        self.app = _app()
        self.config_dir = config_dir
        self.workspace = workspace
        self.owner_id = identity.owner_account_id
        self.setup_token = token
        self.token_for_owner = ""

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

    async def owner_actor_id(self) -> str:
        account = await self.db.get_account(self.owner_id)
        return account["actor_id"]

    def session_token(self, account_id: str | None = None, epoch: int = 0) -> str:
        return create_session_token(
            _SECRET, account_id or self.owner_id, session_epoch=epoch,
        )

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

    The files come from :mod:`nerve.setup_writer`, so what the checklist merges
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
    token = await setup_token.ensure_setup_token(database, unclaimed=True)
    assert token
    try:
        yield _Install(database, identity, config_dir, workspace, token)
    finally:
        await database.close()
        set_config(NerveConfig())


async def _claim(install: _Install, *, body: dict | None = None) -> httpx.Response:
    payload = {
        "username": "alice",
        "password": _PASSWORD,
        "setup_token": install.setup_token,
    }
    payload.update(body or {})
    async with _client(install.app) as http:
        return await http.post("/api/setup/claim", json=payload)


@pytest_asyncio.fixture
async def claimed(install):
    """An install past step one, with a session token for its owner."""
    response = await _claim(install)
    assert response.status_code == 200, response.text
    install.token_for_owner = response.json()["token"]
    return install


def _http(install: _Install, **kwargs) -> httpx.AsyncClient:
    return _client(install.app, token=install.token_for_owner, **kwargs)


# --------------------------------------------------------------------------- #
#  Nothing but the claim, until the claim                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestAVisitorCanOnlyClaim:
    """Rule one of the cutover, for the setup endpoints.

    A passwordless install mints a real session for any password, so
    `require_account` admits the very visitor the setup token exists to keep
    out. Every write except the claim therefore has to refuse until the
    instance has been claimed — otherwise "account first" means only that the
    account is first among the things a stranger may do.
    """

    MUTATIONS = [
        ("put", "/api/setup/provider", {"anthropic_api_key": _ANTHROPIC_KEY}),
        ("put", "/api/setup/profile", {"timezone": "Europe/Berlin"}),
        ("put", "/api/setup/profile", {"display_name": "Mallory"}),
        ("put", "/api/setup/channels", {"telegram_bot_token": _TELEGRAM_TOKEN}),
        ("put", "/api/setup/automation", {"crons": ["inbox-processor"]}),
        ("post", "/api/setup/steps/channels/skip", None),
        ("post", "/api/setup/steps/channels/unskip", None),
    ]

    @pytest.mark.parametrize("method,path,body", MUTATIONS)
    async def test_a_visitor_session_writes_nothing(
        self, install, method, path, body,
    ):
        before = {
            p: p.read_text(encoding="utf-8")
            for p in (
                install.config_local, install.config_yaml,
                install.settings, install.system_crons,
            )
        }
        async with _client(install.app, token=install.session_token()) as http:
            call = getattr(http, method)
            response = await (call(path, json=body) if body is not None else call(path))

        assert response.status_code == 409, (path, response.text)
        assert "/api/setup/claim" in response.json()["detail"]
        for path_on_disk, text in before.items():
            assert path_on_disk.read_text(encoding="utf-8") == text, path_on_disk
        assert setup_state.load_state().done == set()
        assert setup_state.load_state().skipped == set()

    async def test_reading_the_checklist_is_still_allowed(self, install):
        """The screen has to render for the person about to claim it."""
        async with _client(install.app, token=install.session_token()) as http:
            response = await http.get("/api/setup")
        assert response.status_code == 200
        assert response.json()["setup_pending"] is True

    async def test_everything_works_once_it_has_been_claimed(self, claimed):
        """The refusal is about the instance's state, not about the endpoints:
        every one of them works the moment step one is done."""
        async with _http(claimed) as http:
            for method, path, body in self.MUTATIONS:
                call = getattr(http, method)
                response = await (
                    call(path, json=body) if body is not None else call(path)
                )
                assert response.status_code == 200, (path, response.text)


@pytest.mark.asyncio
class TestAnInFlightRequestCannotOutliveTheClaim:
    """The claim is a cutover, not a door that swings shut behind the last
    caller through it.

    A passwordless install admits everybody, so a visitor's request can be
    *authorised* a moment before the claim and land a moment after it. A file
    write has no transaction to carry its epoch into, so the setup steps read
    it again as late as they can — inside the mutation lock, immediately before
    the write.
    """

    SETUP_MUTATIONS = [
        ("put", "/api/setup/provider", {"anthropic_api_key": _ANTHROPIC_KEY}),
        ("put", "/api/setup/profile", {"timezone": "Europe/Berlin"}),
        ("put", "/api/setup/profile", {"display_name": "Mallory"}),
        ("put", "/api/setup/channels", {"telegram_bot_token": _TELEGRAM_TOKEN}),
        ("put", "/api/setup/automation", {"crons": ["inbox-processor"]}),
        ("post", "/api/setup/steps/channels/skip", None),
        ("post", "/api/setup/steps/channels/unskip", None),
    ]

    @pytest.mark.parametrize("method,path,body", SETUP_MUTATIONS)
    async def test_a_setup_write_admitted_before_the_claim_is_refused(
        self, install, monkeypatch, method, path, body,
    ):
        """"The instance is claimed now" is not the same question as "you were
        allowed to ask".

        The claim commits *after* this request's credential was accepted and
        before its write — the only window that matters, since a request that
        starts later is refused at the door by the epoch on its token. Without
        the revalidation the handler sees a claimed instance, decides the
        caller is signed in, and writes as the person it just locked out.
        """
        visitor = install.session_token()
        before = {
            p: p.read_text(encoding="utf-8")
            for p in (
                install.config_local, install.config_yaml,
                install.settings, install.system_crons,
            )
        }

        fired: list[bool] = []
        original = install.db.login_state

        async def _claim_in_the_window(*args, **kwargs):
            # Guarded before awaiting: the claim reads this too.
            if not fired:
                fired.append(True)
                response = await _claim(install)
                assert response.status_code == 200, response.text
            return await original(*args, **kwargs)

        monkeypatch.setattr(install.db, "login_state", _claim_in_the_window)

        async with _client(install.app, token=visitor) as http:
            call = getattr(http, method)
            answered = await (
                call(path, json=body) if body is not None else call(path)
            )
        assert fired, "the claim never ran inside the window"
        # 409 rather than 401, and from the revalidation rather than the door:
        # `require_auth` admitted this request before the claim committed, so
        # the only thing left to refuse it is the epoch read inside the lock.
        assert answered.status_code == 409, (path, answered.text)
        assert "claimed after your session started" in answered.json()["detail"]
        for path_on_disk, text in before.items():
            assert path_on_disk.read_text(encoding="utf-8") == text, path_on_disk


# --------------------------------------------------------------------------- #
#  The checklist                                                               #
# --------------------------------------------------------------------------- #


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
            await http.put(
                "/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY},
            )
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
        # A portable value goes to exactly one layer.
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
        write that published a fresh root-owned 0600 inode would leave the
        host's own non-root CLI unable to read the file it needs to recognise
        a Docker install at all. The private writer is for the file that holds
        credentials.
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
        """A step entered again to turn one cron on must not switch off the
        sync sources configured elsewhere, nor clear the Gmail addresses with
        them. Omitted means untouched."""
        async with _http(claimed) as http:
            await http.put("/api/setup/automation", json={
                "github": True, "gmail": True,
                "gmail_accounts": ["alice@example.invalid"],
                "crons": [],
            })
            settled = claimed.tracked()
            machine = claimed.machine()

            await http.put(
                "/api/setup/automation", json={"crons": ["inbox-processor"]},
            )

        assert claimed.tracked() == settled, "a cron change rewrote the sync settings"
        assert claimed.machine() == machine, "a cron change cleared the mailboxes"
        jobs = {
            job["id"]: job["enabled"]
            for job in yaml.safe_load(
                claimed.system_crons.read_text(encoding="utf-8"),
            )["jobs"]
        }
        assert jobs["inbox-processor"] is True

    async def test_telegram_sync_and_its_credentials(self, claimed):
        """The source's credentials are not the bot token: the bot is how Nerve
        talks *as* you, these are how it reads your own messages."""
        async with _http(claimed) as http:
            response = await http.put("/api/setup/automation", json={
                "telegram": True,
                "telegram_api_id": 1234567,
                "telegram_api_hash": _TELEGRAM_API_HASH,
            })
        assert response.status_code == 200, response.text
        assert claimed.tracked()["sync"]["telegram"]["enabled"] is True
        stored = claimed.secrets()["sync"]["telegram"]
        assert stored["api_id"] == 1234567
        assert stored["api_hash"] == _TELEGRAM_API_HASH
        # ...and it is a secret, so it is in the private file only.
        assert _TELEGRAM_API_HASH not in claimed.settings.read_text(encoding="utf-8")

    async def test_credentials_supplied_before_the_source_is_on_are_kept(
        self, claimed,
    ):
        """Keyed off the credential, not off the switch: somebody who pastes
        credentials with the source still off must not be told it saved and
        find nothing there."""
        async with _http(claimed) as http:
            response = await http.put("/api/setup/automation", json={
                "telegram_api_id": 7654321,
                "telegram_api_hash": _TELEGRAM_API_HASH,
            })
        assert response.status_code == 200, response.text
        assert claimed.secrets()["sync"]["telegram"]["api_id"] == 7654321

    async def test_one_credential_does_not_erase_the_other(self, claimed):
        async with _http(claimed) as http:
            await http.put("/api/setup/automation", json={
                "telegram_api_id": 1234567,
                "telegram_api_hash": _TELEGRAM_API_HASH,
            })
            await http.put("/api/setup/automation", json={
                "telegram_api_id": 7654321,
            })
        stored = claimed.secrets()["sync"]["telegram"]
        assert stored["api_id"] == 7654321
        assert stored["api_hash"] == _TELEGRAM_API_HASH, (
            "supplying one credential erased the other"
        )

    async def test_the_automation_step_is_re_enterable(self, claimed):
        async with _http(claimed) as http:
            await http.put(
                "/api/setup/automation", json={"crons": ["inbox-processor"]},
            )
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

    async def test_it_carries_the_values_a_form_has_to_open_on(self, claimed):
        """A form that opens on defaults submits defaults."""
        config = claimed.reconfigure(
            timezone="Europe/Berlin", anthropic_api_key=_ANTHROPIC_KEY,
        )
        async with _http(claimed) as http:
            values = (await http.get("/api/setup")).json()["values"]
        assert values["timezone"] == "Europe/Berlin"
        assert values["has_anthropic_key"] is True
        assert values["has_openai_key"] is False
        # Whatever the live config says, not a default this screen invented.
        assert values["sync_github"] is config.sync.github.enabled

    async def test_it_reports_a_secret_as_present_and_never_repeats_it(
        self, claimed,
    ):
        async with _http(claimed) as http:
            await http.put("/api/setup/channels", json={
                "telegram_bot_token": _TELEGRAM_TOKEN,
            })
            body = (await http.get("/api/setup")).text
        assert _TELEGRAM_TOKEN not in body
        assert _ANTHROPIC_KEY not in body

        # ...and once the restart has made it live it is reported as present,
        # which is the whole of what a form needs, and still never as itself.
        claimed.restarted(telegram=TelegramConfig(bot_token=_TELEGRAM_TOKEN))
        async with _http(claimed) as http:
            live = await http.get("/api/setup")
        assert live.json()["values"]["has_telegram_token"] is True
        assert _TELEGRAM_TOKEN not in live.text

    async def test_it_names_the_command_that_applies_what_it_wrote(self, claimed):
        """There is no restart endpoint, so the answer has to carry the words
        an operator types on the server."""
        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        assert state["restart_command"] == "nerve restart"

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
        """The case a value comparison alone cannot see.

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
        """A note the checklist left is not evidence forever.

        The provider step is "done" because a credential is configured. Once a
        later process has started, the instance is the witness — so a
        credential removed by hand afterwards leaves the step to do again,
        rather than green because of something written down months ago.
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
            await http.put(
                "/api/setup/automation", json={"crons": ["inbox-processor"]},
            )
        claimed.restarted()
        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        assert _status_of(state, "automation") == "done"

    async def test_an_openai_only_answer_survives_the_restart(self, claimed):
        """The step accepts an OpenAI key on its own, so the instance has to
        recognise one on its own — otherwise a valid answer goes back to "to
        do" at exactly the restart the checklist tells you to perform."""
        async with _http(claimed) as http:
            saved = (await http.put(
                "/api/setup/provider", json={"openai_api_key": _OPENAI_KEY},
            )).json()
        assert _status_of(saved, "provider") == "done"

        claimed.restarted(openai_api_key=_OPENAI_KEY)
        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        assert _status_of(state, "provider") == "done"
        assert "OpenAI" in next(
            s["detail"] for s in state["steps"] if s["id"] == "provider"
        )

    async def test_a_timezone_only_answer_survives_the_restart(self, claimed):
        """Nothing else records that somebody answered the profile step: no
        display name was set, and the marker is retired with the process that
        wrote it. A timezone that is not the installer's default is the
        answer, and it is on disk."""
        async with _http(claimed) as http:
            saved = (await http.put(
                "/api/setup/profile", json={"timezone": "Europe/Berlin"},
            )).json()
        assert _status_of(saved, "profile") == "done"

        claimed.restarted(timezone="Europe/Berlin")
        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        assert _status_of(state, "profile") == "done"
        assert state["restart_pending"] is False

    async def test_the_installers_own_default_is_not_an_answer(self, claimed):
        """...and the other direction: an install nobody has touched must not
        report the profile step as done because the default exists."""
        claimed.restarted(timezone=setup_routes.DEFAULT_TIMEZONE)
        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        assert _status_of(state, "profile") == "pending"

    async def test_an_answered_checklist_stays_finished_across_a_restart(
        self, claimed,
    ):
        """The whole point of the two above, end to end: the checklist tells
        you to restart, so the restart must not undo the checklist."""
        async with _http(claimed) as http:
            await http.put(
                "/api/setup/provider", json={"openai_api_key": _OPENAI_KEY},
            )
            await http.put("/api/setup/profile", json={"timezone": "Europe/Berlin"})
            await http.post("/api/setup/steps/channels/skip")
            await http.post("/api/setup/steps/automation/skip")

        claimed.restarted(openai_api_key=_OPENAI_KEY, timezone="Europe/Berlin")
        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        assert state["restart_pending"] is False
        assert state["finished"] is True, [
            (s["id"], s["status"]) for s in state["steps"]
        ]

    async def test_finished_once_everything_is_answered(self, claimed):
        async with _http(claimed) as http:
            await http.put(
                "/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY},
            )
            await http.put(
                "/api/setup/profile", json={"display_name": "Alice Example"},
            )
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
class TestTheChecklistIsReadAsTheCaller:
    """The profile step edits the person asking, not "the owner".

    An install that has grown a second account has no single owner, so a read
    that answered with the first account's name would open Bob's form on
    Alice's — and the first thing he saved would be her name back onto himself.
    """

    async def _add_bob(self, claimed, *, display_name="Bob Example") -> str:
        async with _http(claimed) as http:
            created = await http.post("/api/accounts", json={
                "username": "bob",
                "password": "another-passphrase",
                "display_name": display_name,
            })
        assert created.status_code == 201, created.text
        return created.json()["id"]

    async def test_bob_reads_his_own_name_and_not_alices(self, claimed):
        bob = await self._add_bob(claimed)
        async with _http(claimed) as http:
            renamed = await http.put(
                "/api/setup/profile", json={"display_name": "Alice Example"},
            )
        assert renamed.status_code == 200, renamed.text

        async with _client(claimed.app, token=claimed.session_token(bob)) as http:
            state = (await http.get("/api/setup")).json()
        assert state["values"]["display_name"] == "Bob Example"
        assert "Alice Example" not in str(state)

    async def test_a_save_by_alice_does_not_move_bobs_name(self, claimed):
        bob = await self._add_bob(claimed)

        async with _client(claimed.app, token=claimed.session_token(bob)) as http:
            before = (await http.get("/api/setup")).json()["values"]["display_name"]

        async with _http(claimed) as http:
            saved = (await http.put(
                "/api/setup/profile", json={"display_name": "Alice Renamed"},
            )).json()
        assert saved["values"]["display_name"] == "Alice Renamed"

        async with _client(claimed.app, token=claimed.session_token(bob)) as http:
            after = (await http.get("/api/setup")).json()["values"]["display_name"]
        assert before == after == "Bob Example"

    async def test_alices_progress_and_skips_do_not_answer_bobs_checklist(
        self, claimed,
    ):
        bob = await self._add_bob(claimed, display_name=None)
        async with _http(claimed) as http:
            profile = await http.put(
                "/api/setup/profile", json={"display_name": "Alice Example"},
            )
            skipped = await http.post("/api/setup/steps/channels/skip")
        assert profile.status_code == 200, profile.text
        assert skipped.status_code == 200, skipped.text
        assert _status_of(skipped.json(), "profile") == "done"
        assert _status_of(skipped.json(), "channels") == "skipped"

        async with _client(claimed.app, token=claimed.session_token(bob)) as http:
            state = (await http.get("/api/setup")).json()
        assert state["values"]["display_name"] is None
        assert _status_of(state, "profile") == "pending"
        assert _status_of(state, "channels") == "pending"

    async def test_concurrent_accounts_keep_both_sets_of_decisions(self, claimed):
        bob = await self._add_bob(claimed, display_name=None)
        async with (
            _http(claimed) as alice_http,
            _client(claimed.app, token=claimed.session_token(bob)) as bob_http,
        ):
            alice_skip, bob_skip = await asyncio.gather(
                alice_http.post("/api/setup/steps/channels/skip"),
                bob_http.post("/api/setup/steps/provider/skip"),
            )
        assert alice_skip.status_code == 200, alice_skip.text
        assert bob_skip.status_code == 200, bob_skip.text

        async with _http(claimed) as http:
            alice = (await http.get("/api/setup")).json()
        async with _client(claimed.app, token=claimed.session_token(bob)) as http:
            bob_state = (await http.get("/api/setup")).json()
        assert _status_of(alice, "channels") == "skipped"
        assert _status_of(alice, "provider") == "pending"
        assert _status_of(bob_state, "provider") == "skipped"
        assert _status_of(bob_state, "channels") == "pending"

    async def test_ambiguous_legacy_decisions_are_not_given_to_bob(self, claimed):
        bob = await self._add_bob(claimed, display_name=None)
        state_path = setup_state.state_file()
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps({
            "version": 2,
            "skipped": ["channels"],
            "done": ["profile"],
            "answered": ["profile"],
            "applied": {},
            "debts": [],
            "boot": boot.boot_id(),
        }), encoding="utf-8")

        async with _client(claimed.app, token=claimed.session_token(bob)) as http:
            state = (await http.get("/api/setup")).json()
        assert _status_of(state, "profile") == "pending"
        assert _status_of(state, "channels") == "pending"

        migrated = json.loads(
            state_path.read_text(encoding="utf-8"),
        )
        assert migrated["version"] == 3
        assert migrated["accounts"][bob]["skipped"] == []


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
                http.put(
                    "/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY},
                ),
                http.post("/api/setup/steps/channels/skip"),
                http.put(
                    "/api/setup/profile", json={"display_name": "Alice Example"},
                ),
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
class TestMutationsAreActuallySerialised:
    """`asyncio.gather` proves nothing on its own — the requests may simply not
    overlap. These force the interleaving: one handler is held open *after* it
    has read the checklist, and another runs to completion inside that window.
    """

    async def _blocked_at(self, claimed, monkeypatch, release: asyncio.Event,
                          started: asyncio.Event):
        """Hold the profile step between its read of the state and its save."""
        db = claimed.db
        original = db.update_actor_profile

        async def _slow(*args, **kwargs):
            started.set()
            await release.wait()
            return await original(*args, **kwargs)

        monkeypatch.setattr(db, "update_actor_profile", _slow)

    async def test_a_skip_during_a_profile_write_is_not_overwritten(
        self, claimed, monkeypatch,
    ):
        release, started = asyncio.Event(), asyncio.Event()
        await self._blocked_at(claimed, monkeypatch, release, started)

        async with _http(claimed) as http:
            profile = asyncio.create_task(http.put(
                "/api/setup/profile", json={"display_name": "Alice Example"},
            ))
            await asyncio.wait_for(started.wait(), timeout=5)

            # Inside the window: the profile handler has read the checklist and
            # not yet written it back.
            skip = asyncio.create_task(http.post("/api/setup/steps/channels/skip"))
            await asyncio.sleep(0.05)
            release.set()
            profile_response, skip_response = await asyncio.gather(profile, skip)

        assert profile_response.status_code == 200, profile_response.text
        assert skip_response.status_code == 200, skip_response.text
        assert setup_state.load_state().skipped == {"channels"}, (
            "the profile step saved a snapshot taken before the skip"
        )
        assert _status_of(skip_response.json(), "profile") == "done"

    async def test_an_unskip_during_an_automation_write_is_not_overwritten(
        self, claimed, monkeypatch,
    ):
        async with _http(claimed) as http:
            await http.post("/api/setup/steps/channels/skip")

        release = asyncio.Event()

        async def _blocked_publish(plan):
            # Held open between the step's read of the checklist and its save.
            await release.wait()
            return ()

        monkeypatch.setattr(setup_routes, "_publish_cron_plan", _blocked_publish)

        async with _http(claimed) as http:
            automation = asyncio.create_task(http.put(
                "/api/setup/automation", json={"crons": ["inbox-processor"]},
            ))
            await asyncio.sleep(0.05)
            unskip = asyncio.create_task(
                http.post("/api/setup/steps/channels/unskip"),
            )
            await asyncio.sleep(0.05)
            release.set()
            automation_response, unskip_response = await asyncio.gather(
                automation, unskip,
            )

        assert automation_response.status_code == 200, automation_response.text
        assert unskip_response.status_code == 200, unskip_response.text
        assert setup_state.load_state().skipped == set(), (
            "the automation step saved a snapshot taken before the unskip"
        )
        assert "automation" in setup_state.load_state().done

    async def test_every_mutating_handler_holds_the_lock(self):
        """Structural: the lock is only a rule if every handler follows it.

        Checked against the source because the failure it prevents — one
        handler quietly written without it — is invisible in any test that does
        not force an interleaving.
        """
        import inspect

        source = inspect.getsource(setup_routes)
        handlers = [
            "set_provider", "set_profile", "set_channels", "set_automation",
            "skip_step", "unskip_step", "get_setup",
        ]
        for name in handlers:
            body = inspect.getsource(getattr(setup_routes, name))
            assert '_loop_lock("state")' in body, name
        # ...and nothing mutates the notes outside one.
        assert source.count("setup_state.save_state(") == 4, (
            "a new call site for save_state: check it runs under the lock"
        )


@pytest.mark.asyncio
class TestAChoiceThatWasNotSavedIsNotReportedAsSaved:
    """`save_state` can fail — a state directory that cannot be written
    owner-only, a full disk — and it returns False rather than raising, so a
    caller that ignored it answered 200 to a skip that reached no disk and came
    back at the next read."""

    @staticmethod
    def _failing_save(monkeypatch):
        monkeypatch.setattr(setup_state, "save_state", lambda state: False)

    async def test_a_skip_that_could_not_be_saved_is_an_error(
        self, claimed, monkeypatch,
    ):
        self._failing_save(monkeypatch)
        async with _http(claimed) as http:
            response = await http.post("/api/setup/steps/channels/skip")
        assert response.status_code == 500
        assert "not remembered" in response.json()["detail"]
        assert "Nothing else changed" in response.json()["detail"]

    async def test_an_unskip_that_could_not_be_saved_is_an_error(
        self, claimed, monkeypatch,
    ):
        self._failing_save(monkeypatch)
        async with _http(claimed) as http:
            response = await http.post("/api/setup/steps/channels/unskip")
        assert response.status_code == 500

    async def test_a_step_whose_configuration_landed_says_what_landed(
        self, claimed, monkeypatch,
    ):
        """The other case, and it is not a failure: the credential *is* on
        disk. Answering 500 would invite a retry of a write that already
        happened; answering a plain 200 would hide that the checklist has
        forgotten it."""
        self._failing_save(monkeypatch)
        async with _http(claimed) as http:
            response = await http.put(
                "/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY},
            )
        assert response.status_code == 200
        body = response.json()
        assert body["warning"]
        assert "configuration was written" in body["warning"]
        assert "provider" in body["warning"]
        # And it really did land.
        assert claimed.secrets()["anthropic_api_key"] == _ANTHROPIC_KEY

    async def test_nothing_is_warned_about_when_the_save_works(self, claimed):
        async with _http(claimed) as http:
            body = (await http.put(
                "/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY},
            )).json()
        assert body["warning"] is None


@pytest.mark.asyncio
class TestReadingAndWritingDoNotRaceEachOther:
    """Reading the checklist can write: the first read after a restart retires
    what the previous process left behind. That write has to be inside the same
    lock the steps take, or it lands on top of one."""

    async def test_a_read_racing_every_step_loses_nothing(self, claimed):
        async with _http(claimed) as http:
            await http.put(
                "/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY},
            )
        claimed.restarted(anthropic_api_key=_ANTHROPIC_KEY)

        async with _http(claimed) as http:
            results = await asyncio.gather(
                http.get("/api/setup"),
                http.post("/api/setup/steps/channels/skip"),
                http.get("/api/setup"),
                http.put("/api/setup/automation", json={"crons": []}),
                http.get("/api/setup"),
            )
        assert {r.status_code for r in results} == {200}

        async with _http(claimed) as http:
            final = (await http.get("/api/setup")).json()
        assert _status_of(final, "channels") == "skipped"
        assert _status_of(final, "automation") == "done"
        on_disk = setup_state.load_state()
        assert on_disk.skipped == {"channels"}
        assert "automation" in on_disk.done


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
            assert (await http.put("/api/setup/automation", json={
                "github": True, "crons": ["inbox-processor"],
            })).status_code == 409
        assert "automation" not in setup_state.load_state().done
        jobs = yaml.safe_load(claimed.system_crons.read_text(encoding="utf-8"))["jobs"]
        assert all(
            not job["enabled"] for job in jobs if job["id"] == "inbox-processor"
        ), "the cron file was published for a step that failed"


@pytest.mark.asyncio
class TestAFailureSaysWhatLanded:
    """Where two resources cannot be written as one, the answer has to say
    which of them changed. "Nothing was changed" is only allowed when it is
    true."""

    async def test_an_unreadable_cron_file_stops_before_the_sync_write(
        self, claimed,
    ):
        """The cron file is parsed before anything is published, so a step
        that cannot finish has not started."""
        before = claimed.tracked()
        claimed.system_crons.write_text(
            "jobs: [this is not a list of jobs\n", encoding="utf-8",
        )
        async with _http(claimed) as http:
            response = await http.put("/api/setup/automation", json={
                "github": True, "crons": ["inbox-processor"],
            })
        assert response.status_code == 409
        assert "nothing was changed" in response.json()["detail"].lower()
        assert claimed.tracked() == before, (
            "the sync settings were written by a step that reported failure"
        )
        assert "automation" not in setup_state.load_state().done

    async def test_a_database_failure_after_the_timezone_says_the_timezone_landed(
        self, claimed, monkeypatch,
    ):
        async def _broken(*args, **kwargs):
            raise RuntimeError("disk is on fire")

        monkeypatch.setattr(claimed.db, "update_actor_profile", _broken)
        async with _http(claimed) as http:
            response = await http.put("/api/setup/profile", json={
                "timezone": "Europe/Berlin", "display_name": "Alice Example",
            })
        assert response.status_code == 500
        detail = response.json()["detail"]
        assert "time zone was saved" in detail
        assert "display name could not be saved" in detail
        # And it is true: the file changed, and the checklist knows it owes a
        # restart for it rather than having lost that with the failure.
        assert claimed.tracked()["timezone"] == "Europe/Berlin"
        state = setup_state.load_state()
        assert state.applied.get("timezone") == "Europe/Berlin"

    async def test_a_name_only_failure_says_nothing_was_written(
        self, claimed, monkeypatch,
    ):
        async def _broken(*args, **kwargs):
            raise RuntimeError("disk is on fire")

        monkeypatch.setattr(claimed.db, "update_actor_profile", _broken)
        async with _http(claimed) as http:
            response = await http.put(
                "/api/setup/profile", json={"display_name": "Alice Example"},
            )
        assert response.status_code == 500
        assert "Nothing was written" in response.json()["detail"]

    async def test_a_publication_failure_after_the_sync_write_says_so(
        self, claimed, monkeypatch,
    ):
        """The rename itself failing, after the sync settings have landed."""
        real = setup_writer_module.publish_text

        def _explode_on_the_cron_file(path, text):
            # Only the last target fails, which is the case: the sync settings
            # have already been published by the time the cron file is renamed.
            if path.name == "system.yaml":
                raise OSError("read-only file system")
            return real(path, text)

        monkeypatch.setattr(
            setup_writer_module, "publish_text", _explode_on_the_cron_file,
        )
        async with _http(claimed) as http:
            response = await http.put("/api/setup/automation", json={
                "github": True, "crons": ["inbox-processor"],
            })
        assert response.status_code == 500
        detail = response.json()["detail"]
        assert "sync settings were saved" in detail
        assert "crons are unchanged" in detail

        # What landed is on disk and the checklist knows it owes a restart for
        # it — but the step is *not* done, and there is no cron debt for a file
        # that was never written.
        assert claimed.tracked()["sync"]["github"]["enabled"] is True
        state = setup_state.load_state()
        assert state.applied.get("sync.github.enabled") is True
        assert "automation" not in state.done
        assert state.debts == set(), (
            "a debt was recorded for a cron file that was never published"
        )

        async with _http(claimed) as http:
            after = (await http.get("/api/setup")).json()
        assert _status_of(after, "automation") == "pending"
        assert after["finished"] is False

        # And the restart does not launder it: automation's completion is a
        # durable note, so a step marked done here would have stayed done
        # while the cron file never changed.
        claimed.restarted()
        async with _http(claimed) as http:
            restarted = (await http.get("/api/setup")).json()
        assert _status_of(restarted, "automation") == "pending"
        assert restarted["finished"] is False

    async def test_a_failed_name_only_profile_leaves_no_marker(
        self, claimed, monkeypatch,
    ):
        """The step is recorded *after* the write it depends on, so a failed
        rename cannot count towards `finished`."""
        async def _broken(*args, **kwargs):
            raise RuntimeError("disk is on fire")

        monkeypatch.setattr(claimed.db, "update_actor_profile", _broken)
        async with _http(claimed) as http:
            response = await http.put(
                "/api/setup/profile", json={"display_name": "Alice Example"},
            )
        assert response.status_code == 500
        state = setup_state.load_state()
        assert "profile" not in state.done
        assert "profile" not in state.answered

    async def test_a_successful_rename_comes_back_renamed(self, claimed):
        """The actor this request resolved with is immutable and carries the
        old name; rendering from it makes a saved rename look unsaved, and
        leaves the form's Save button lit over a change that landed."""
        async with _http(claimed) as http:
            body = (await http.put(
                "/api/setup/profile", json={"display_name": "Alice Example"},
            )).json()
        assert body["values"]["display_name"] == "Alice Example"
        assert "Alice Example" in next(
            s["detail"] for s in body["steps"] if s["id"] == "profile"
        )

    async def test_choosing_the_default_timezone_is_still_an_answer(self, claimed):
        """Setting a non-default zone back to UTC is a decision, and nothing
        on disk can tell it from never having been asked — so it is written
        down, and it survives the restart."""
        async with _http(claimed) as http:
            await http.put("/api/setup/profile", json={"timezone": "Europe/Berlin"})
            saved = (await http.put(
                "/api/setup/profile",
                json={"timezone": setup_routes.DEFAULT_TIMEZONE},
            )).json()
        assert _status_of(saved, "profile") == "done"

        claimed.restarted(timezone=setup_routes.DEFAULT_TIMEZONE)
        async with _http(claimed) as http:
            state = (await http.get("/api/setup")).json()
        assert _status_of(state, "profile") == "done"
        assert state["restart_pending"] is False

    async def test_the_cron_file_is_published_atomically(self, claimed):
        async with _http(claimed) as http:
            await http.put(
                "/api/setup/automation", json={"crons": ["inbox-processor"]},
            )
        assert not claimed.system_crons.with_name("system.yaml.tmp").exists()
        jobs = yaml.safe_load(claimed.system_crons.read_text(encoding="utf-8"))["jobs"]
        assert any(j["id"] == "memory-maintenance" for j in jobs), (
            "the rest of the file survived the rewrite"
        )


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
        assert any(
            "scheduler" in reason for reason in state["restart_pending_reasons"]
        )
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

    async def test_a_selection_that_changes_nothing_owes_nothing(self, claimed):
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
        """It is a database write too — and the one thing the checklist can
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
#  The signing secret                                                          #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestTheSigningSecretIsNeverRotated:
    """The checklist ends in a restart, so the browser has to come back still
    signed in: nothing below may re-key the instance."""

    async def test_no_step_touches_the_auth_section(self, claimed):
        before = claimed.secrets()["auth"]
        async with _http(claimed) as http:
            await http.put(
                "/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY},
            )
            await http.put("/api/setup/profile", json={"timezone": "Europe/Berlin"})
            await http.put("/api/setup/channels", json={
                "telegram_bot_token": _TELEGRAM_TOKEN,
            })
            await http.put("/api/setup/automation", json={"crons": []})
        assert claimed.secrets()["auth"] == before
        assert claimed.secrets()["auth"]["jwt_secret"]

    async def test_a_session_issued_before_the_steps_still_works_after_them(
        self, claimed,
    ):
        async with _http(claimed) as http:
            await http.put(
                "/api/setup/provider", json={"anthropic_api_key": _ANTHROPIC_KEY},
            )
            # Same token, after every write: nothing re-keyed the instance.
            assert (await http.get("/api/auth/check")).status_code == 200
