"""The Telegram message sync's sign-in, driven against a fake Telethon client.

The source needs an authorised *user* session on disk; API credentials alone
do nothing. These tests pin the state machine (start → code → optional
password → signed in), its refusals (bad phone, wrong code, expired code,
flood wait, a session file locked by a running sync), cancel and sign-out,
and that the phone, the code, the password and Telegram's hash never reach a
log line, the state file or a response (the phone only masked). The HTTP
side — guards, the GET shape on the wire, the checklist detail — is in
``tests/test_setup_api.py::TestTelegramSession``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from telethon import errors as tg_errors

from nerve import cli, paths, setup_state
from nerve import setup_telegram_login as login
from nerve.config import load_config
from nerve.setup_writer import CONFIG_YAML_HEADER, SetupChoices, merge_machine_paths, merge_private_paths, write_slim_install

PHONE = "+1 (555) 010-0123"
PHONE_NORMALISED = "+15550100123"
CODE = "73146"
PASSWORD = "two-step-REPLACE"
HASH = "phone-code-hash-REPLACE"
ME = SimpleNamespace(id=424242, username="alice_example", first_name="Alice")


class FakeClient:
    """Scripted stand-in for ``telethon.TelegramClient`` on the session file."""

    def __init__(self, api_id: int, api_hash: str, *, script: dict):
        self.api_id, self.api_hash = api_id, api_hash
        self.script = script
        self.calls: list = []
        self.connected = False
        self.disconnected = 0

    async def connect(self):
        self.calls.append("connect")
        if "connect" in self.script:
            raise self.script["connect"]
        self.connected = True
        # Telethon creates the sqlite session on the first connection.
        login.session_file().parent.mkdir(parents=True, exist_ok=True)
        login.session_file().touch()

    async def is_user_authorized(self):
        self.calls.append("is_user_authorized")
        return bool(self.script.get("authorized"))

    async def get_me(self):
        self.calls.append("get_me")
        return self.script.get("me", ME)

    async def send_code_request(self, phone):
        self.calls.append(("send_code", phone))
        if "send_code" in self.script:
            raise self.script["send_code"]
        return SimpleNamespace(phone_code_hash=HASH)

    async def sign_in(self, phone=None, code=None, *, password=None, phone_code_hash=None):
        self.calls.append(("sign_in", phone, code, password, phone_code_hash))
        if password is not None:
            if password != self.script.get("password", PASSWORD):
                raise tg_errors.PasswordHashInvalidError(request=None)
        else:
            if code != self.script.get("code", CODE):
                raise self.script.get("bad_code", tg_errors.PhoneCodeInvalidError(request=None))
            if self.script.get("two_step"):
                raise tg_errors.SessionPasswordNeededError(request=None)
        self.script["authorized"] = True
        return self.script.get("me", ME)

    async def log_out(self):
        self.calls.append("log_out")
        if "log_out" in self.script:
            raise self.script["log_out"]
        self.script["authorized"] = False
        login.session_file().unlink(missing_ok=True)
        return True

    async def disconnect(self):
        self.calls.append("disconnect")
        self.connected = False
        self.disconnected += 1


class Harness:
    def __init__(self, script: dict | None = None):
        self.script = script if script is not None else {}
        self.clients: list[FakeClient] = []
        self.now = 1000.0
        self.login = login.TelegramLogin(client_factory=self._factory, clock=lambda: self.now)
        self.config = SimpleNamespace(sync=SimpleNamespace(telegram=SimpleNamespace(api_id=1234567, api_hash="REPLACE_HASH_VALUE")))

    def _factory(self, api_id, api_hash):
        client = FakeClient(api_id, api_hash, script=self.script)
        self.clients.append(client)
        return client


@pytest.fixture
def harness(tmp_path, monkeypatch):
    return Harness()


def run(coro):
    return asyncio.run(coro)


class TestStatus:
    def test_not_configured(self, harness):
        harness.config.sync.telegram.api_hash = ""
        assert run(harness.login.status(harness.config)) == {
            "configured": False, "session_present": False, "authorized": False, "user": None,
            "pending": None, "phone_masked": None, "reason": "not_configured",
        }
        assert harness.clients == [], "no client is opened without credentials"

    def test_no_session_file(self, harness):
        status = run(harness.login.status(harness.config))
        assert status["configured"] is True and status["session_present"] is False
        assert status["authorized"] is False and status["reason"] == "no_session"
        assert harness.clients == []

    def test_an_authorised_session_names_the_user(self, harness):
        login.session_file().parent.mkdir(parents=True, exist_ok=True)
        login.session_file().touch()
        harness.script["authorized"] = True
        status = run(harness.login.status(harness.config))
        assert status["authorized"] is True
        assert status["user"] == {"id": 424242, "username": "alice_example", "first_name": "Alice"}
        assert status["reason"] is None
        assert harness.clients[0].calls == ["connect", "is_user_authorized", "get_me", "disconnect"]

    def test_a_locked_session_file_is_busy_not_an_error(self, harness):
        login.session_file().parent.mkdir(parents=True, exist_ok=True)
        login.session_file().touch()
        harness.script["connect"] = sqlite3.OperationalError("database is locked")
        status = run(harness.login.status(harness.config))
        assert status["authorized"] is None and status["reason"] == "busy"
        assert status["session_present"] is True
        assert harness.clients[0].disconnected == 1

    def test_an_unreachable_telegram_is_reported_not_raised(self, harness):
        login.session_file().parent.mkdir(parents=True, exist_ok=True)
        login.session_file().touch()
        harness.script["connect"] = ConnectionError("no route")
        status = run(harness.login.status(harness.config))
        assert status["authorized"] is None and status["reason"] == "unreachable"


class TestSignIn:
    def test_start_code_password_then_signed_in(self, harness, caplog):
        caplog.set_level(logging.DEBUG)
        harness.script["two_step"] = True
        started = run(harness.login.start(harness.config, PHONE))
        assert started == {"pending": "code", "phone_masked": "+1••••••••23", "expires_in": 600}
        client = harness.clients[0]
        assert client.calls == ["connect", ("send_code", PHONE_NORMALISED)]
        pending = run(harness.login.status(harness.config))
        assert pending["pending"] == "code" and pending["authorized"] is False
        assert pending["phone_masked"] == "+1••••••••23" and pending["session_present"] is True
        assert len(harness.clients) == 1, "no probe while a login is pending"

        wrong = pytest.raises(login.LoginError, run, harness.login.code(harness.config, "00000"))
        assert wrong.value.status_code == 400 and wrong.value.field == "code"

        asked = run(harness.login.code(harness.config, " 731 46 "))
        assert asked["pending"] == "password" and asked["authorized"] is False
        assert client.calls[-1] == ("sign_in", PHONE_NORMALISED, CODE, None, HASH)

        stage = pytest.raises(login.LoginError, run, harness.login.code(harness.config, CODE))
        assert stage.value.status_code == 409 and "password" in stage.value.detail

        bad = pytest.raises(login.LoginError, run, harness.login.password(harness.config, "nope"))
        assert bad.value.status_code == 400 and bad.value.field == "password"

        done = run(harness.login.password(harness.config, PASSWORD))
        assert done["authorized"] is True and done["pending"] is None
        assert done["user"] == {"id": 424242, "username": "alice_example", "first_name": "Alice"}
        assert done["session_present"] is True and done["configured"] is True
        assert harness.login.pending is None and client.disconnected == 1
        assert stat.S_IMODE(login.session_file().stat().st_mode) == 0o600

        text = caplog.text + json.dumps([started, pending, asked, done])
        for secret in (PHONE_NORMALISED, "5550100123", CODE, PASSWORD, HASH, "REPLACE_HASH_VALUE"):
            assert secret not in text, secret

    def test_without_two_step_the_code_signs_in(self, harness):
        run(harness.login.start(harness.config, PHONE))
        done = run(harness.login.code(harness.config, CODE))
        assert done["authorized"] is True and done["user"]["username"] == "alice_example"

    def test_a_new_start_cancels_the_previous_login(self, harness):
        run(harness.login.start(harness.config, PHONE))
        first = harness.clients[0]
        run(harness.login.start(harness.config, "+44 20 7946 0000"))
        assert first.disconnected == 1 and len(harness.clients) == 2
        assert harness.login.pending.phone == "+442079460000"

    def test_cancel_drops_the_login_and_touches_no_file_state(self, harness):
        run(harness.login.start(harness.config, PHONE))
        status = run(harness.login.cancel(harness.config))
        assert harness.login.pending is None and harness.clients[0].disconnected == 1
        assert status["pending"] is None
        nothing = pytest.raises(login.LoginError, run, harness.login.code(harness.config, CODE))
        assert nothing.value.status_code == 409

    def test_the_code_expires_after_ten_minutes(self, harness):
        run(harness.login.start(harness.config, PHONE))
        harness.now += 599
        assert harness.login.pending is not None
        harness.now += 2
        expired = pytest.raises(login.LoginError, run, harness.login.code(harness.config, CODE))
        assert expired.value.status_code == 409 and "expired" in expired.value.detail
        assert harness.login.pending is None and harness.clients[0].disconnected == 1
        status = run(harness.login.status(harness.config))
        assert status["pending"] is None

    def test_an_expired_code_from_telegram_drops_the_login(self, harness):
        harness.script["bad_code"] = tg_errors.PhoneCodeExpiredError(request=None)
        run(harness.login.start(harness.config, PHONE))
        expired = pytest.raises(login.LoginError, run, harness.login.code(harness.config, "11111"))
        assert expired.value.status_code == 400 and expired.value.field == "code"
        assert harness.login.pending is None

    def test_a_flood_wait_is_a_429_with_retry_after(self, harness):
        harness.script["send_code"] = tg_errors.FloodWaitError(request=None, capture=42)
        flood = pytest.raises(login.LoginError, run, harness.login.start(harness.config, PHONE))
        assert flood.value.status_code == 429 and flood.value.retry_after == 42
        assert "42 seconds" in flood.value.detail
        assert harness.login.pending is None and harness.clients[0].disconnected == 1

    def test_a_locked_session_file_refuses_the_start(self, harness):
        harness.script["connect"] = sqlite3.OperationalError("database is locked")
        busy = pytest.raises(login.LoginError, run, harness.login.start(harness.config, PHONE))
        assert busy.value.status_code == 409 and "running sync" in busy.value.detail

    @pytest.mark.parametrize("phone", ["", "alice", "+1", "12345", "+1 555 0100 123 4567 890"])
    def test_a_phone_that_is_not_a_number_is_a_400_on_the_field(self, harness, phone):
        bad = pytest.raises(login.LoginError, run, harness.login.start(harness.config, phone))
        assert bad.value.status_code == 400 and bad.value.field == "phone"
        assert harness.clients == []

    def test_telegram_rejecting_the_number_is_a_400_on_the_field(self, harness):
        harness.script["send_code"] = tg_errors.PhoneNumberInvalidError(request=None)
        bad = pytest.raises(login.LoginError, run, harness.login.start(harness.config, PHONE))
        assert bad.value.status_code == 400 and bad.value.field == "phone"

    def test_not_configured_is_a_409(self, harness):
        harness.config.sync.telegram.api_id = 0
        refused = pytest.raises(login.LoginError, run, harness.login.start(harness.config, PHONE))
        assert refused.value.status_code == 409

    def test_telethon_missing_is_a_409_and_a_status_reason(self, harness):
        def _missing(api_id, api_hash):
            raise ImportError("No module named telethon")

        harness.login.client_factory = _missing
        refused = pytest.raises(login.LoginError, run, harness.login.start(harness.config, PHONE))
        assert refused.value.status_code == 409 and "Telethon" in refused.value.detail
        login.session_file().parent.mkdir(parents=True, exist_ok=True)
        login.session_file().touch()
        assert run(harness.login.status(harness.config))["reason"] == "telethon_missing"


class TestLogout:
    def test_logout_revokes_and_removes_the_session(self, harness):
        run(harness.login.start(harness.config, PHONE))
        run(harness.login.code(harness.config, CODE))
        assert login.session_present()
        status = run(harness.login.logout(harness.config))
        assert status["session_present"] is False and status["authorized"] is False
        assert status["reason"] == "no_session"
        assert not login.session_present()
        assert harness.clients[-1].calls == ["connect", "log_out", "disconnect"]

    def test_logout_while_unreachable_still_forgets_the_session_here(self, harness):
        login.session_file().parent.mkdir(parents=True, exist_ok=True)
        login.session_file().touch()
        harness.script["log_out"] = ConnectionError("offline")
        status = run(harness.login.logout(harness.config))
        assert status["session_present"] is False and not login.session_present()

    def test_logout_keeps_a_session_a_running_sync_holds(self, harness):
        login.session_file().parent.mkdir(parents=True, exist_ok=True)
        login.session_file().touch()
        harness.script["connect"] = sqlite3.OperationalError("database is locked")
        busy = pytest.raises(login.LoginError, run, harness.login.logout(harness.config))
        assert busy.value.status_code == 409 and login.session_present()


class TestHelpers:
    def test_mask_phone(self):
        assert login.mask_phone("+15550100123") == "+1••••••••23"
        assert login.mask_phone("+44 20 7946 0000") == "+4•••••••••00"
        assert login.mask_phone("12") == "+••"

    def test_classify_knows_the_telethon_families(self):
        assert login.classify(tg_errors.FloodWaitError(request=None, capture=3)) == "flood"
        assert login.classify(tg_errors.SessionPasswordNeededError(request=None)) == "password_needed"
        assert login.classify(tg_errors.PhoneCodeInvalidError(request=None)) == "bad_code"
        assert login.classify(tg_errors.PhoneCodeExpiredError(request=None)) == "expired_code"
        assert login.classify(tg_errors.PasswordHashInvalidError(request=None)) == "bad_password"
        assert login.classify(tg_errors.PhoneNumberBannedError(request=None)) == "bad_phone"
        assert login.classify(sqlite3.OperationalError("database is locked")) == "busy"
        assert login.classify(asyncio.TimeoutError()) == "unreachable"
        assert login.classify(RuntimeError("x")) == "other"

    def test_the_session_path_is_the_one_the_sync_source_uses(self):
        from nerve.sources.telegram import TelegramSource

        source = TelegramSource(config={"api_id": 1, "api_hash": "x"})
        assert source._session_path() == str(login.session_base())
        assert str(login.session_file()) == source._session_path() + ".session"


class TestDoctor:
    """``nerve doctor`` warns about an enabled sync without a signed-in
    session and names both routes; the sync source's own warning does too."""

    def _config_dir(self, tmp_path: Path, *, creds: bool = True) -> Path:
        config_dir = tmp_path / "config"
        ws = tmp_path / "workspace"
        write_slim_install(
            SetupChoices(deployment="server", workspace_path=ws, gateway_host="127.0.0.1", gateway_port=8976),
            config_dir,
        )
        settings = yaml.safe_load((ws / "config" / "settings.yaml").read_text(encoding="utf-8")) or {}
        settings.setdefault("sync", {}).setdefault("telegram", {})["enabled"] = True
        (ws / "config" / "settings.yaml").write_text(yaml.safe_dump(settings, sort_keys=False), encoding="utf-8")
        if creds:
            merge_private_paths(config_dir / "config.local.yaml", {
                "sync.telegram.api_id": 1234567, "sync.telegram.api_hash": "REPLACE_HASH_VALUE",
            })
        return config_dir

    def test_enabled_without_a_session_warns_and_names_both_routes(self, tmp_path):
        report = cli.doctor_report(load_config(self._config_dir(tmp_path)))
        assert "[OK] Telegram sync: API credentials configured" in report
        assert "[WARN] Telegram sync: no signed-in session at" in report
        assert "telegram_sync.session" in report
        assert "setup page (Sources → Telegram)" in report and "nerve setup-telegram" in report
        assert "REPLACE_HASH_VALUE" not in report

    def test_a_signed_in_session_reads_ok(self, tmp_path):
        config_dir = self._config_dir(tmp_path)
        login.session_file().parent.mkdir(parents=True, exist_ok=True)
        login.session_file().touch()
        state = setup_state.load_state()
        state.telegram_sync_user = {"id": 424242, "username": "alice_example", "first_name": "Alice"}
        setup_state.save_state(state)
        report = cli.doctor_report(load_config(config_dir))
        assert "[OK] Telegram sync: signed in as @alice_example" in report
        assert "[WARN] Telegram sync" not in report

    def test_a_session_file_nobody_verified_warns(self, tmp_path):
        config_dir = self._config_dir(tmp_path)
        login.session_file().parent.mkdir(parents=True, exist_ok=True)
        login.session_file().touch()
        report = cli.doctor_report(load_config(config_dir))
        assert "[WARN] Telegram sync: a session file exists at" in report
        assert "no sign-in has been verified" in report and "nerve setup-telegram" in report

    def test_enabled_without_credentials_points_at_the_page(self, tmp_path):
        report = cli.doctor_report(load_config(self._config_dir(tmp_path, creds=False)))
        assert "[WARN] Telegram sync: enabled but sync.telegram.api_id/api_hash are not set" in report
        assert "setup page (Sources → Telegram)" in report

    def test_disabled_is_one_quiet_line(self, tmp_path):
        config_dir = tmp_path / "config"
        write_slim_install(
            SetupChoices(deployment="server", workspace_path=tmp_path / "ws", gateway_host="127.0.0.1", gateway_port=8976),
            config_dir,
        )
        report = cli.doctor_report(load_config(config_dir))
        assert "[--] Telegram sync disabled" in report

    def test_the_source_warning_names_both_routes(self, caplog):
        from nerve.sources.telegram import TelegramSource

        caplog.set_level(logging.WARNING)
        source = TelegramSource(config={"api_id": 1234567, "api_hash": "REPLACE_HASH_VALUE"})
        assert run(source._ensure_client()) is False
        assert "setup page (Sources → Telegram)" in caplog.text and "nerve setup-telegram" in caplog.text
        assert "REPLACE_HASH_VALUE" not in caplog.text
