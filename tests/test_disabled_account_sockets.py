"""Disabling an account closes its open WebSockets, through the real ``/ws``."""

from __future__ import annotations

import contextlib
import queue
import threading
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from nerve.config import AuthConfig, NerveConfig, set_config
from nerve.gateway import server
from nerve.gateway.auth import create_session_token, pin_jwt_secret

_SECRET = "test-secret-for-disabled-account-sockets"
_HASH = "$2b$12$" + "x" * 53
_SESSION = "a-session-id"


class _FakeEngine:
    """Enough engine for the socket handler."""

    def __init__(self, db):
        self.db = db
        self.router = SimpleNamespace(get_last_session=self._session)
        self.sessions = SimpleNamespace(get_active_session=self._session)

    async def _session(self, *_args, **_kwargs):
        return _SESSION

    def is_session_running(self, *_args, **_kwargs):
        return False


@contextlib.asynccontextmanager
async def _no_lifespan(_app):
    yield


async def _two_accounts(db, owner_id: str) -> str:
    await db.update_account_login(owner_id, username="alice", credential=_HASH)
    bob = await db.create_managed_account(username="bob", credential=_HASH)
    return bob["id"]


@pytest.fixture
def instance(tmp_path, open_identity_db, wire_identity_store, monkeypatch):
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
    app = server.create_app()
    app.router.lifespan_context = _no_lifespan
    with TestClient(app) as client:
        database, identity = client.portal.call(open_identity_db, tmp_path / "nerve.db")
        wire_identity_store(database)
        monkeypatch.setattr(server, "_engine", _FakeEngine(database), raising=False)
        owner_id = identity.owner_account_id
        bob_id = client.portal.call(_two_accounts, database, owner_id)
        try:
            yield SimpleNamespace(
                client=client, db=database, owner_id=owner_id, bob_id=bob_id,
            )
        finally:
            client.portal.call(database.close)
    set_config(NerveConfig())


def _connect(instance, account_id: str):
    token = create_session_token(_SECRET, account_id)
    return instance.client.websocket_connect(f"/ws?token={token}")


def _close_code(socket, *, within: float = 5.0) -> int:
    """Wait for the server to close ``socket`` and return the close code."""
    outcome: queue.Queue = queue.Queue(maxsize=1)

    def _read() -> None:
        try:
            outcome.put(("message", socket.receive_json()))
        except WebSocketDisconnect as disconnect:
            outcome.put(("closed", disconnect.code))
        except Exception as e:  # noqa: BLE001 - reported below
            outcome.put(("error", e))

    threading.Thread(target=_read, daemon=True).start()
    try:
        kind, value = outcome.get(timeout=within)
    except queue.Empty:
        pytest.fail(f"the socket was still open after {within}s")
    if kind != "closed":
        pytest.fail(f"expected a close, got {kind}: {value!r}")
    return value


def test_disabling_an_account_closes_only_its_sockets(instance):
    with _connect(instance, instance.bob_id) as bob, \
            _connect(instance, instance.owner_id) as owner:
        assert bob.receive_json()["type"] == "session_switched"
        assert owner.receive_json()["type"] == "session_switched"

        token = create_session_token(_SECRET, instance.owner_id)
        response = instance.client.post(
            f"/api/accounts/{instance.bob_id}/disable",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 200, response.text

        assert _close_code(bob) == server.WS_ACCOUNT_DISABLED_CODE
        owner.send_json({"type": "ping"})
        assert owner.receive_json() == {"type": "pong"}
    assert server._live_sockets == {}


def test_a_disable_during_the_handshake_closes_the_socket(instance, monkeypatch):
    """The disable commits after authentication and before registration."""
    authenticate = server.authenticate_websocket

    async def _then_disable(websocket):
        actor = await authenticate(websocket)
        await instance.db.disable_account(instance.bob_id)
        return actor

    monkeypatch.setattr(server, "authenticate_websocket", _then_disable)
    with _connect(instance, instance.bob_id) as bob:
        assert _close_code(bob) == server.WS_ACCOUNT_DISABLED_CODE
    assert server._live_sockets == {}


def test_a_closed_socket_leaves_the_registry(instance):
    with _connect(instance, instance.bob_id) as bob:
        bob.receive_json()
        assert len(server._live_sockets) == 1
    assert server._live_sockets == {}


def test_a_failed_handshake_leaves_the_registry(instance, monkeypatch):
    async def _broken(*_args, **_kwargs):
        raise RuntimeError("no session")

    monkeypatch.setattr(server._engine.router, "get_last_session", _broken)
    try:
        with _connect(instance, instance.bob_id):
            pass
    except Exception:  # noqa: BLE001 - the handshake failing is the case here
        pass
    assert server._live_sockets == {}
