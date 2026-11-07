"""Web Push: the subscriptions, the ``webpush`` channel and the app manifest.

No test reaches a push service: :class:`FakePushService` stands in for the
HTTP session. It keeps each request, so the tests decrypt the payload with
the browser's keys and check the VAPID token, as a browser and a push
service would.
"""

from __future__ import annotations

import base64
import json
import os
import sqlite3
import struct
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import aiosqlite
import http_ece
import httpx
import jwt
import pytest
import pytest_asyncio
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi import FastAPI
from py_vapid import Vapid

from nerve.config import NerveConfig, NotificationsConfig, set_config
from nerve.db.migrations import v068_push_subscriptions
from nerve.db.push_subscriptions import MAX_PUSH_SUBSCRIPTIONS
from nerve.gateway.auth import create_session_token, create_system_token, hash_password, pin_jwt_secret
from nerve.gateway.routes import register_all_routes
from nerve.gateway.routes._deps import RouteDeps
from nerve.notifications import webpush
from nerve.notifications.service import NotificationService

_SECRET = "test-secret-for-web-push-pad-to-32-bytes"
_PASSWORD_HASH = hash_password("correct-horse-battery-staple")
FCM = "https://fcm.googleapis.com/fcm/send/device-one"
MOZILLA = "https://updates.push.services.mozilla.com/wpush/v2/device-two"


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


class Browser:
    """The keys that a browser makes when it subscribes."""

    def __init__(self, endpoint: str = FCM):
        self.endpoint = endpoint
        self.private_key = ec.generate_private_key(ec.SECP256R1())
        public = self.private_key.public_key().public_bytes(
            serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint,
        )
        self.auth_secret = os.urandom(16)
        self.keys = {"p256dh": _b64url(public), "auth": _b64url(self.auth_secret)}

    def json(self, level: str = "important") -> dict:
        return {"endpoint": self.endpoint, "keys": self.keys, "level": level}

    def decrypt(self, body: bytes) -> dict:
        plain = http_ece.decrypt(body, private_key=self.private_key, auth_secret=self.auth_secret)
        return json.loads(plain)


class FakePushService:
    """The HTTP session of :func:`webpush.deliver`: it keeps each request and
    answers with the status set for the endpoint (201 by default), or raises
    the exception set for it."""

    def __init__(self):
        self.requests: list[dict] = []
        self.answers: dict[str, int | Exception] = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, timeout=None, data=None, headers=None):
        self.requests.append({"url": url, "data": data, "headers": dict(headers or {}), "timeout": timeout})
        answer = self.answers.get(url, 201)
        if isinstance(answer, Exception):
            raise answer

        async def text():
            return ""
        return SimpleNamespace(status=answer, reason="", text=text)


@pytest.fixture
def push_service(monkeypatch) -> FakePushService:
    service = FakePushService()
    monkeypatch.setattr(webpush, "_http_session", lambda: service)
    return service


@pytest_asyncio.fixture
async def people(tmp_path, open_identity_db):
    """A database with two people: ``alice`` (the owner) and ``bob``."""
    database, identity = await open_identity_db(tmp_path / "nerve.db")
    await database.update_account_login(
        identity.owner_account_id, username="alice", credential=_PASSWORD_HASH,
    )
    bob = await database.create_managed_account(
        username="bob", credential=_PASSWORD_HASH, display_name="Bob",
    )
    try:
        yield SimpleNamespace(
            db=database, alice=identity.owner_actor_id, bob=bob["actor_id"],
            alice_account=identity.owner_account_id, bob_account=bob["id"],
        )
    finally:
        await database.close()


async def _subscribe(db, actor_id: str, browser: Browser, level: str = "important") -> dict:
    return await db.save_push_subscription(
        actor_id=actor_id, endpoint=browser.endpoint, level=level, user_agent="Firefox", **browser.keys,
    )


# --------------------------------------------------------------------------- #
#  Schema and store                                                            #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestStore:
    async def test_the_migration_makes_the_table_and_can_run_again(self, tmp_path):
        async with aiosqlite.connect(tmp_path / "raw.db") as conn:
            await conn.execute("CREATE TABLE actor_refs (id TEXT PRIMARY KEY)")
            await v068_push_subscriptions.up(conn)
            await v068_push_subscriptions.up(conn)
            async with conn.execute("PRAGMA table_info(push_subscriptions)") as cursor:
                columns = {row[1] for row in await cursor.fetchall()}
        assert columns == {
            "id", "actor_id", "endpoint", "p256dh", "auth", "level", "user_agent",
            "created_at", "last_success_at",
        }

    async def test_a_second_save_updates_the_row_of_the_browser(self, people):
        browser = Browser()
        first = await _subscribe(people.db, people.alice, browser)
        browser.keys["auth"] = _b64url(os.urandom(16))
        second = await _subscribe(people.db, people.alice, browser, level="all")
        assert second["id"] == first["id"]
        assert (second["auth"], second["level"]) == (browser.keys["auth"], "all")
        assert len(await people.db.list_push_subscriptions()) == 1

    async def test_the_browser_moves_to_the_person_who_subscribes_last(self, people):
        browser = Browser()
        await _subscribe(people.db, people.alice, browser)
        await _subscribe(people.db, people.bob, browser)
        assert await people.db.list_push_subscriptions(people.alice) == []
        (row,) = await people.db.list_push_subscriptions(people.bob)
        assert row["endpoint"] == FCM

    async def test_a_person_has_at_most_the_maximum_of_browsers(self, people):
        for n in range(MAX_PUSH_SUBSCRIPTIONS):
            assert await _subscribe(people.db, people.alice, Browser(f"{FCM}-{n}"))
        assert await _subscribe(people.db, people.alice, Browser(f"{FCM}-extra")) is None
        # A browser that has a row already can still change it.
        assert await _subscribe(people.db, people.alice, Browser(f"{FCM}-0"), level="all")

    async def test_a_person_deletes_only_their_own_rows(self, people):
        row = await _subscribe(people.db, people.alice, Browser())
        assert not await people.db.delete_push_subscription(row["id"], people.bob)
        assert await people.db.delete_push_subscription(row["id"], people.alice)
        assert await people.db.list_push_subscriptions() == []

    async def test_the_actor_must_exist(self, people):
        with pytest.raises(sqlite3.IntegrityError):
            await _subscribe(people.db, "no-such-actor", Browser())


# --------------------------------------------------------------------------- #
#  Endpoint and key checks                                                     #
# --------------------------------------------------------------------------- #


class TestChecks:
    @pytest.mark.parametrize("endpoint", [
        FCM,
        MOZILLA,
        "https://web.push.apple.com/QGuQyavXutnMH8",
        "https://wns2-par02p.notify.windows.com/w/?token=BQYAAAB",
        "https://fcm.googleapis.com:443/wp/abc",
    ])
    def test_the_push_services_of_the_browsers_are_accepted(self, endpoint):
        assert webpush.check_endpoint(endpoint) == endpoint

    @pytest.mark.parametrize("endpoint", [
        "http://fcm.googleapis.com/fcm/send/x",
        "https://localhost/api/push",
        "https://127.0.0.1/x",
        "https://10.0.0.5/x",
        "https://[::1]/x",
        "https://example.com/push",
        "https://fcm.googleapis.com.example.com/x",
        "https://evilpush.apple.com/x",
        "https://fcm.googleapis.com:8443/x",
        "https://user:pw@fcm.googleapis.com/x",
        "https://fcm.googleapis.com:99999/x",
        "javascript:alert(1)",
        "",
        "https://fcm.googleapis.com/" + "a" * webpush.MAX_ENDPOINT_CHARS,
    ])
    def test_any_other_address_is_refused(self, endpoint):
        with pytest.raises(ValueError):
            webpush.check_endpoint(endpoint)

    def test_a_key_must_decode_to_its_size(self):
        keys = Browser().keys
        assert webpush.check_key(keys["p256dh"], webpush.P256DH_BYTES)
        assert webpush.check_key(keys["auth"], webpush.AUTH_BYTES)
        for bad in (keys["auth"], "not base64 !", ""):
            with pytest.raises(ValueError):
                webpush.check_key(bad, webpush.P256DH_BYTES)


# --------------------------------------------------------------------------- #
#  VAPID key and payload                                                       #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestVapid:
    async def test_the_key_is_made_once_and_kept_with_the_instance_secrets(self, people):
        assert await people.db._get_instance_secret(webpush.VAPID_SECRET_NAME) is None
        first = await webpush.public_key(people.db)
        stored = await people.db._get_instance_secret(webpush.VAPID_SECRET_NAME)
        assert stored and stored not in first
        assert await webpush.public_key(people.db) == first
        assert len(base64.urlsafe_b64decode(first + "=")) == webpush.P256DH_BYTES


def test_the_payload_has_the_agent_name_the_first_lines_and_the_tag():
    body = "Line one\n\nLine two\nLine three\nLine four"
    data = json.loads(webpush.payload("Ada", "ask-1234", "Deploy now?", body, silent=True))
    assert data == {
        "title": "Ada: Deploy now?",
        "body": "Line one\nLine two\nLine three",
        "tag": "ask-1234",
        "url": "/notifications",
        "silent": True,
    }
    long = json.loads(webpush.payload("Ada", "n", "t" * 500, "b" * 500, silent=False))
    assert len(long["title"]) == webpush.TITLE_CHARS
    assert len(long["body"]) == webpush.BODY_CHARS


# --------------------------------------------------------------------------- #
#  Delivery                                                                    #
# --------------------------------------------------------------------------- #


def _config(tmp_path: Path, **notifications) -> NerveConfig:
    workspace = tmp_path / "ws"
    workspace.mkdir(exist_ok=True)
    config = NerveConfig.from_dict({"workspace": str(workspace)})
    config.notifications = NotificationsConfig(channels=["web"], **notifications)
    config.quiet_start = config.quiet_end = ""
    return config


async def _deliver(people, config, **overrides) -> bool:
    item = {
        "notification_id": "ask-1234", "notif_type": "question", "title": "Deploy now?",
        "body": "The build passed.", "priority": "normal", "silent": False, **overrides,
    }
    return await webpush.deliver(people.db, config, **item)


@pytest.mark.asyncio
class TestDeliver:
    async def test_the_browser_can_decrypt_the_payload_and_the_token_is_signed(
        self, people, push_service, tmp_path,
    ):
        browser = Browser()
        await _subscribe(people.db, people.alice, browser)
        config = _config(tmp_path)
        (config.workspace / "IDENTITY.md").write_text("- **Name:** Ada\n")

        assert await _deliver(people, config, priority="urgent", silent=True)

        (request,) = push_service.requests
        assert request["url"] == FCM
        assert browser.decrypt(request["data"]) == {
            "title": "Ada: Deploy now?", "body": "The build passed.", "tag": "ask-1234",
            "url": "/notifications", "silent": True,
        }
        headers = request["headers"]
        assert (headers["ttl"], headers["urgency"]) == (str(webpush.PUSH_TTL_S), "high")
        assert headers["content-encoding"] == "aes128gcm"
        assert request["timeout"].total == webpush.PUSH_TIMEOUT_S
        authorization = headers["authorization"]
        assert Vapid.verify(authorization)
        token = dict(part.split("=", 1) for part in authorization.split(" ", 1)[1].split(","))
        assert token["k"] == await webpush.public_key(people.db)
        claims = jwt.decode(token["t"], options={"verify_signature": False})
        assert (claims["sub"], claims["aud"]) == (webpush.VAPID_SUBJECT, "https://fcm.googleapis.com")
        (row,) = await people.db.list_push_subscriptions()
        assert row["last_success_at"]

    async def test_the_title_names_nerve_when_the_agent_has_no_name(self, people, push_service, tmp_path, monkeypatch):
        monkeypatch.delenv("NERVE_DISPLAY_NAME", raising=False)
        browser = Browser()
        await _subscribe(people.db, people.alice, browser)
        await _deliver(people, _config(tmp_path))
        assert browser.decrypt(push_service.requests[0]["data"])["title"] == "Nerve: Deploy now?"

    async def test_each_browser_gets_the_items_of_its_level(self, people, push_service, tmp_path):
        everything, important = Browser(FCM), Browser(MOZILLA)
        await _subscribe(people.db, people.alice, everything, level="all")
        await _subscribe(people.db, people.bob, important, level="important")
        config = _config(tmp_path)

        assert await _deliver(people, config, notif_type="notify", priority="normal")
        assert [r["url"] for r in push_service.requests] == [FCM]
        push_service.requests.clear()
        assert await _deliver(people, config, notif_type="notify", priority="high")
        assert sorted(r["url"] for r in push_service.requests) == sorted([FCM, MOZILLA])

    @pytest.mark.parametrize("status", [404, 410])
    async def test_a_gone_subscription_is_deleted(self, people, push_service, tmp_path, status):
        await _subscribe(people.db, people.alice, Browser())
        push_service.answers[FCM] = status
        assert not await _deliver(people, _config(tmp_path))
        assert await people.db.list_push_subscriptions() == []

    @pytest.mark.parametrize("answer", [500, 429, 403, OSError("connection refused")])
    async def test_any_other_failure_keeps_the_subscription(self, people, push_service, tmp_path, answer, caplog):
        await _subscribe(people.db, people.alice, Browser())
        push_service.answers[FCM] = answer
        assert not await _deliver(people, _config(tmp_path))
        (row,) = await people.db.list_push_subscriptions()
        assert row["last_success_at"] is None
        assert "device-one" not in caplog.text

    async def test_one_failure_does_not_stop_the_other_browsers(self, people, push_service, tmp_path):
        await _subscribe(people.db, people.alice, Browser(FCM))
        await _subscribe(people.db, people.alice, Browser(MOZILLA))
        push_service.answers[FCM] = 410
        assert await _deliver(people, _config(tmp_path))
        (row,) = await people.db.list_push_subscriptions()
        assert row["endpoint"] == MOZILLA

    async def test_nothing_is_sent_without_subscriptions(self, people, push_service, tmp_path):
        assert not await _deliver(people, _config(tmp_path))
        assert push_service.requests == []
        assert await people.db._get_instance_secret(webpush.VAPID_SECRET_NAME) is None


@pytest.fixture
def broadcasts(monkeypatch) -> list[dict]:
    captured: list[dict] = []

    class _FakeBroadcaster:
        async def broadcast(self, _channel: str, message: dict) -> None:
            captured.append(message)

    from nerve.agent import streaming
    monkeypatch.setattr(streaming, "broadcaster", _FakeBroadcaster())
    return captured


@pytest.mark.asyncio
class TestFanout:
    async def _service(self, people, tmp_path, **notifications) -> NotificationService:
        await people.db.create_session("s1", actor=None)
        return NotificationService(_config(tmp_path, **notifications), people.db, MagicMock())

    async def test_a_notification_goes_to_the_subscribed_browsers(
        self, people, push_service, tmp_path, broadcasts,
    ):
        browser = Browser()
        await _subscribe(people.db, people.alice, browser, level="all")
        service = await self._service(people, tmp_path)

        notification_id = await service.send_notification("s1", "Build finished", "All green")

        assert browser.decrypt(push_service.requests[0]["data"])["tag"] == notification_id
        row = await people.db.get_notification(notification_id)
        assert json.loads(row["channels_delivered"]) == ["web", "webpush"]

    async def test_a_question_goes_to_a_browser_at_important(self, people, push_service, tmp_path, broadcasts):
        await _subscribe(people.db, people.alice, Browser(), level="important")
        service = await self._service(people, tmp_path)
        await service.send_notification("s1", "Build finished")
        assert push_service.requests == []
        await service.ask_question("s1", "Deploy now?", options=["Yes", "No"])
        assert len(push_service.requests) == 1

    async def test_the_channel_level_applies_too(self, people, push_service, tmp_path, broadcasts):
        await _subscribe(people.db, people.alice, Browser(), level="all")
        service = await self._service(people, tmp_path, levels={"webpush": "important"})
        await service.send_notification("s1", "Build finished")
        assert push_service.requests == []

    async def test_a_caller_that_names_the_channels_gets_no_push(self, people, push_service, tmp_path, broadcasts):
        await _subscribe(people.db, people.alice, Browser(), level="all")
        service = await self._service(people, tmp_path)
        await service.send_notification("s1", "Build finished", channels=["web"])
        assert push_service.requests == []

    async def test_during_quiet_hours_the_push_is_silent(
        self, people, push_service, tmp_path, broadcasts, monkeypatch,
    ):
        from nerve.notifications import quiet_hours

        monkeypatch.setattr(quiet_hours, "in_quiet_hours", lambda config, now=None: True)
        browser = Browser()
        await _subscribe(people.db, people.alice, browser, level="all")
        service = await self._service(people, tmp_path)
        await service.send_notification("s1", "Build finished")
        await service.send_notification("s1", "Disk full", priority="urgent")
        silent = [browser.decrypt(r["data"])["silent"] for r in push_service.requests]
        assert silent == [True, False]


# --------------------------------------------------------------------------- #
#  Routes                                                                      #
# --------------------------------------------------------------------------- #


@pytest_asyncio.fixture
async def api(people, tmp_path, monkeypatch):
    from nerve.gateway.routes import _deps as deps_module

    workspace = tmp_path / "ws"
    (workspace / "config").mkdir(parents=True)
    config = NerveConfig.from_dict({"workspace": str(workspace), "auth": {"jwt_secret": _SECRET}})
    set_config(config)
    pin_jwt_secret(_SECRET)
    monkeypatch.setattr(deps_module, "_deps", RouteDeps(engine=None, db=people.db))
    monkeypatch.delenv("NERVE_DISPLAY_NAME", raising=False)
    app = FastAPI()
    app.include_router(register_all_routes())

    def client(account: str | None = None, token: str | None = None) -> httpx.AsyncClient:
        if account:
            token = create_session_token(_SECRET, account)
        headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Firefox/140.0"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://nerve-test", headers=headers,
        )

    try:
        yield SimpleNamespace(client=client, workspace=workspace, **vars(people))
    finally:
        set_config(NerveConfig())


URL = "/api/push/subscriptions"


@pytest.mark.asyncio
class TestRoutes:
    async def test_a_person_turns_push_on_and_sees_only_their_own_browsers(self, api):
        await _subscribe(api.db, api.bob, Browser(MOZILLA))
        async with api.client(api.alice_account) as http:
            saved = await http.post(URL, json=Browser().json(level="all"))
            assert saved.status_code == 200, saved.text
            listed = (await http.get(URL)).json()
        assert saved.json()["user_agent"].endswith("Firefox/140.0")
        assert listed["public_key"] == await webpush.public_key(api.db)
        (device,) = listed["subscriptions"]
        assert device == saved.json()
        assert set(device) == {"id", "endpoint", "level", "user_agent", "created_at", "last_success_at"}
        assert device["level"] == "all"

    @pytest.mark.parametrize("body", [
        {"endpoint": "http://fcm.googleapis.com/fcm/send/x"},
        {"endpoint": "https://127.0.0.1/api/push"},
        {"endpoint": "https://nerve-test/api/push/subscriptions"},
        {"keys": {"p256dh": "AAAA", "auth": "AAAA"}},
    ])
    async def test_a_bad_subscription_is_refused_and_nothing_is_stored(self, api, body):
        async with api.client(api.alice_account) as http:
            response = await http.post(URL, json={**Browser().json(), **body})
        assert response.status_code == 400
        assert await api.db.list_push_subscriptions() == []

    async def test_the_maximum_of_browsers_is_409(self, api):
        for n in range(MAX_PUSH_SUBSCRIPTIONS):
            await _subscribe(api.db, api.alice, Browser(f"{FCM}-{n}"))
        async with api.client(api.alice_account) as http:
            assert (await http.post(URL, json=Browser().json())).status_code == 409

    async def test_a_person_removes_only_their_own_browsers(self, api):
        bobs = await _subscribe(api.db, api.bob, Browser(MOZILLA))
        alices = await _subscribe(api.db, api.alice, Browser(FCM))
        async with api.client(api.alice_account) as http:
            assert (await http.delete(f"{URL}/{bobs['id']}")).status_code == 404
            assert (await http.delete(f"{URL}/{alices['id']}")).status_code == 200
        assert [r["endpoint"] for r in await api.db.list_push_subscriptions()] == [MOZILLA]

    async def test_a_token_is_required_and_must_be_a_person(self, api):
        async with api.client() as http:
            assert (await http.get(URL)).status_code == 401
        async with api.client(token=create_system_token(_SECRET)) as http:
            assert (await http.get(URL)).status_code == 403
            assert (await http.post(URL, json=Browser().json())).status_code == 403


def _png(size: int) -> bytes:
    return b"\x89PNG\r\n\x1a\n" + struct.pack(">I", 13) + b"IHDR" + struct.pack(">II", size, size) + b"\x08\x06\x00\x00\x00"


@pytest.mark.asyncio
class TestManifest:
    async def _get(self, api) -> dict:
        async with api.client() as http:
            response = await http.get("/api/manifest.webmanifest")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("application/manifest+json")
        return response.json()

    async def test_the_app_has_the_agent_name_and_needs_no_sign_in(self, api):
        (api.workspace / "IDENTITY.md").write_text("# Identity\n- **Name:** Ada\n")
        body = await self._get(api)
        assert (body["name"], body["short_name"]) == ("Ada", "Ada")
        assert (body["display"], body["start_url"], body["scope"]) == ("standalone", "/", "/")
        assert [icon["sizes"] for icon in body["icons"]] == ["192x192", "512x512", "512x512"]

    async def test_the_hosted_name_and_then_nerve(self, api, monkeypatch):
        assert (await self._get(api))["name"] == "Nerve"
        monkeypatch.setenv("NERVE_DISPLAY_NAME", "Support bot")
        assert (await self._get(api))["name"] == "Support bot"

    async def test_an_svg_or_a_large_png_favicon_is_the_icon(self, api):
        favicon = api.workspace / "config" / "favicon.png"
        favicon.write_bytes(_png(512))
        assert (await self._get(api))["icons"] == [
            {"src": "/favicon.ico", "sizes": "512x512", "type": "image/png"},
        ]
        favicon.write_bytes(_png(64))
        assert (await self._get(api))["icons"][0]["src"] == "/nerve-icon-192.png"
        (api.workspace / "config" / "favicon.svg").write_text("<svg xmlns='http://www.w3.org/2000/svg'/>")
        assert (await self._get(api))["icons"] == [
            {"src": "/favicon.ico", "sizes": "any", "type": "image/svg+xml"},
        ]
