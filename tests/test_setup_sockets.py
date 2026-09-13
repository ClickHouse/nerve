"""A live WebSocket held across the claim, driven through the real app.

The claim exists to end the authority a passwordless install handed out. An
HTTP token from before it is refused by the session epoch, and a *new* socket
is refused at the handshake — but a socket that was already open is the case
that matters most, because it is both the one an attacker would already have
and the one that goes on receiving the owner's transcript without ever sending
anything.

So this test holds a real connection: it opens one over the real ``/ws``
endpoint before the claim, claims over HTTP, and then asks what that
connection can still do.

Sync rather than ``async``, because ``TestClient`` runs the app on its own
portal loop and a WebSocket needs that loop to outlive the request that opened
it. The database is opened with ``asyncio.run`` and used from both; aiosqlite
takes the running loop per call rather than pinning one, and nothing here uses
it from two loops at once.
"""

from __future__ import annotations

import asyncio
import queue
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from nerve import setup_token
from nerve.config import AuthConfig, NerveConfig, set_config
from nerve.db import Database
from nerve.gateway import server
from nerve.gateway.auth import pin_jwt_secret

_SECRET = "test-secret-for-the-socket-claim-tests-32b"
_PASSWORD = "correct-horse-battery-staple"
_SESSION = "a-session-id"


class _FakeEngine:
    """Enough engine for the socket handler: it is not what is under test."""

    def __init__(self, db):
        self.db = db
        self.router = SimpleNamespace(
            get_last_session=self._last_session,
            switch_session=self._noop,
        )
        self.sessions = SimpleNamespace(get_active_session=self._active_session)
        self.ran: list[str] = []

    async def _last_session(self, *_args, **_kwargs):
        return _SESSION

    async def _active_session(self, *_args, **_kwargs):
        return _SESSION

    async def _noop(self, *_args, **_kwargs):
        return None

    def is_session_running(self, *_args, **_kwargs):
        return False

    async def run(self, **kwargs):
        self.ran.append(kwargs.get("user_message", ""))

    def register_task(self, *_args, **_kwargs):
        return None

    async def stop_session(self, *_args, **_kwargs):
        return False


@pytest.fixture
def instance(tmp_path, wire_identity_store, monkeypatch):
    """A real app over a real database, on a passwordless install."""
    config_dir = tmp_path / "config"
    workspace = tmp_path / "workspace"
    config_dir.mkdir(parents=True)
    workspace.mkdir(parents=True)
    set_config(NerveConfig(
        auth=AuthConfig(jwt_secret=_SECRET),
        config_dir=config_dir,
        workspace=workspace,
    ))
    pin_jwt_secret(_SECRET)

    async def _open():
        database = Database(Path(tmp_path / "nerve.db"))
        await database.connect()
        identity = await database.bootstrap_local_identity(credential_source="none")
        # What first start would have published while the instance is unclaimed.
        token = await setup_token.ensure_setup_token(database, unclaimed=True)
        return database, identity, token

    database, identity, token = asyncio.run(_open())
    wire_identity_store(database)
    monkeypatch.setattr(server, "_engine", _FakeEngine(database), raising=False)

    client = TestClient(server.create_app())
    try:
        yield SimpleNamespace(
            client=client,
            db=database,
            setup_token=token,
            owner_id=identity.owner_account_id,
        )
    finally:
        asyncio.run(database.close())
        set_config(NerveConfig())


def _expect_closed(socket, *, within: float = 5.0) -> int:
    """The close code this socket is about to receive, or a failure.

    Read on a **daemon** thread with a deadline. ``receive_json`` blocks
    forever on a socket that is not closed, so a test asserting a close has to
    be able to give up — otherwise the run hangs when the behaviour under test
    is missing, which is the one moment the result has to be readable. The
    thread is left behind on purpose: it is waiting on a queue nobody will
    fill, and joining it is exactly the wait being avoided.
    """
    outcome: queue.Queue = queue.Queue(maxsize=1)

    def _read() -> None:
        try:
            outcome.put(("message", socket.receive_json()))
        except WebSocketDisconnect as disconnect:
            outcome.put(("closed", disconnect.code))
        except Exception as e:  # noqa: BLE001 - reported, not raised in a thread
            outcome.put(("error", e))

    threading.Thread(target=_read, daemon=True).start()
    try:
        kind, value = outcome.get(timeout=within)
    except queue.Empty:
        pytest.fail(
            f"the socket was still open after {within}s; it should have been "
            "closed"
        )
    if kind == "closed":
        return value
    pytest.fail(f"expected a close, got {kind}: {value!r}")


def _passwordless_session(instance) -> str:
    """What any visitor gets on an install nobody has claimed."""
    response = instance.client.post(
        "/api/auth/login", json={"password": "anything at all"},
    )
    assert response.status_code == 200, response.text
    return response.json()["token"]


def _claim(instance) -> str:
    """Claim over HTTP, from a peer TestClient reports as ``testclient`` —
    which is not loopback, so this goes through the setup token, exactly as a
    claim from another machine does."""
    response = instance.client.post("/api/setup/claim", json={
        "username": "alice",
        "password": _PASSWORD,
        "setup_token": instance.setup_token,
    })
    assert response.status_code == 200, response.text
    return response.json()["token"]


class TestASocketHeldAcrossTheClaim:
    def test_it_is_closed_and_can_neither_send_nor_receive(self, instance):
        visitor = _passwordless_session(instance)

        with instance.client.websocket_connect(f"/ws?token={visitor}") as socket:
            # Live before the claim: the handler tells a fresh listener which
            # session it is bound to.
            assert socket.receive_json()["type"] == "session_switched"

            claimed = _claim(instance)

            # The claim closed it rather than waiting to be spoken to — a
            # socket that is only checked when it *sends* keeps receiving the
            # owner's transcript in the meantime. The registry is emptied by
            # the same pass, so it answers without waiting for anything.
            assert server._live_sockets == {}, (
                "the claim left a passwordless socket open"
            )
            assert _expect_closed(socket) == server.WS_REVOKED_CODE

        # ...while the browser that did the claiming connects perfectly well.
        with instance.client.websocket_connect(f"/ws?token={claimed}") as fresh:
            assert fresh.receive_json()["type"] == "session_switched"
            fresh.send_json({"type": "ping"})
            assert fresh.receive_json() == {"type": "pong"}

    def test_a_frame_from_a_stale_socket_is_refused_even_if_the_close_missed_it(
        self, instance, monkeypatch,
    ):
        """The other half, tested on its own.

        Closing on claim is what stops a stale socket *receiving*. The
        per-frame check is what stops it *acting* — and it has to hold even
        when the close did not happen: the connection could have been accepted
        by another process, or the close could have failed.
        """
        visitor = _passwordless_session(instance)

        with instance.client.websocket_connect(f"/ws?token={visitor}") as socket:
            assert socket.receive_json()["type"] == "session_switched"

            async def _no_close():
                return 0

            monkeypatch.setattr(server, "close_revoked_sockets", _no_close)
            _claim(instance)

            # Still open — nothing closed it — and the very next frame is
            # refused rather than executed.
            socket.send_json({"type": "ping"})
            assert _expect_closed(socket) == server.WS_REVOKED_CODE

    def test_a_message_from_a_stale_socket_never_reaches_the_engine(
        self, instance, monkeypatch,
    ):
        """The frame is refused *before* anything is done with it."""
        visitor = _passwordless_session(instance)

        with instance.client.websocket_connect(f"/ws?token={visitor}") as socket:
            socket.receive_json()

            async def _no_close():
                return 0

            monkeypatch.setattr(server, "close_revoked_sockets", _no_close)
            _claim(instance)

            socket.send_json({"type": "message", "content": "act as the owner"})
            _expect_closed(socket)

        assert server._engine.ran == [], "a revoked socket ran an agent turn"

    def test_an_unclaimed_instance_keeps_serving_its_sockets(self, instance):
        """The check is "has this account moved on", not "is this suspicious":
        an install nobody has claimed still works exactly as before."""
        visitor = _passwordless_session(instance)
        with instance.client.websocket_connect(f"/ws?token={visitor}") as socket:
            assert socket.receive_json()["type"] == "session_switched"
            socket.send_json({"type": "ping"})
            assert socket.receive_json() == {"type": "pong"}

    def test_a_disabled_account_loses_its_socket_at_the_next_frame(self, instance):
        """The same gate, for free: disablement used to take effect on the
        next *request* and never on an open socket."""
        claimed = _claim(instance)
        with instance.client.websocket_connect(f"/ws?token={claimed}") as socket:
            assert socket.receive_json()["type"] == "session_switched"

            second = asyncio.run(instance.db.create_managed_account(
                username="bob", credential="$2b$12$" + "x" * 53,
            ))
            assert second
            asyncio.run(instance.db.disable_account(instance.owner_id))

            socket.send_json({"type": "ping"})
            assert _expect_closed(socket) == server.WS_REVOKED_CODE

    def test_the_registry_does_not_leak_connections(self, instance):
        visitor = _passwordless_session(instance)
        with instance.client.websocket_connect(f"/ws?token={visitor}") as socket:
            socket.receive_json()
            assert len(server._live_sockets) == 1
        # Closed by the client: the handler's cleanup drops it.
        assert server._live_sockets == {}
