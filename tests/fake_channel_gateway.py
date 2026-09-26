"""A channel gateway stand-in for hosted channel tests.

``FakeChannelGateway`` signs workload identity tokens with a test ES256 key
and serves the JWK Set through an httpx transport. It opens WebSocket streams
to a Nerve endpoint, negotiates, advertises capabilities, and serves inbox
pages from an in-memory inbox while it records every read and
acknowledgement. Switches on the gateway simulate lost frames and outages.

``NerveServer`` runs an ASGI app on a loopback port with uvicorn, so the
tests exercise real HTTP upgrades and real status codes.

The frame and event templates follow the gateway's version 1 channel
contract samples.
"""

from __future__ import annotations

import asyncio
import copy
import json
import time
import uuid
from typing import Any

import httpx
import jwt
import uvicorn
from cryptography.hazmat.primitives.asymmetric import ec
from jwt.algorithms import ECAlgorithm
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

TENANT_ID = "6f1d3a2c-0b4e-4f7a-9c1d-2e5b8a3f7c04"
AGENT_ID = "b28c5e91-7d4a-4c3b-8f61-0a9e2d4b6c17"
CONNECTION_ID = "3d9a4f2e-1c7b-4a85-9e60-2f8b7c1d5a43"
AGENT_AUTHOR = "U_FIXTURE_AGENT"
ISSUER = "https://cp.test/workload-identity"
JWKS_URL = "https://cp.test/workload-identity/jwks.json"
AUDIENCE = "nerve-channel"
# An upper bound for waits. A loaded machine can be slow; a passing run is not.
EVENT_TIMEOUT = 15.0


# The gateway's sample capabilities of a Slack connection.
CAPABILITIES: dict[str, Any] = {
    "self": {"id": AGENT_AUTHOR, "kind": "bot", "display_name": "Nerve"},
    "operations": [
        "send", "origin_reply", "edit", "delete", "reaction", "interaction", "file_read",
        "history_read", "file_send", "command_response", "typing", "conversation_lookup",
        "author_lookup",
    ],
    "limits": {
        "in_flight_operations": 16,
        "operation_deadline_millis": 30000,
        "text_characters": 4000,
        "edit_interval_millis": 1200,
        "file_bytes": 16777216,
    },
}

# The gateway's sample Slack message event, without its delivery and content.
MESSAGE_EVENT: dict[str, Any] = {
    "event_id": "EvFixtureMessage001",
    "message_revision_id": "revision:1700000000.000100:0",
    "tenant_id": TENANT_ID,
    "agent_id": AGENT_ID,
    "connection_id": CONNECTION_ID,
    "provider": "slack",
    "issuer": "T_FIXTURE_WORKSPACE",
    "installation_id": "A_FIXTURE_INSTALLATION",
    "kind": "message",
    "conversation": {"id": "C_FIXTURE_CHANNEL", "kind": "channel", "display_name": "deployments"},
    "message": {"id": "1700000000.000100"},
    "author": {"id": "U_FIXTURE_MEMBER", "kind": "human", "display_name": "Example Member"},
    "admission": {
        "purpose": "invoke",
        "invoke": {
            "resolved_principal_id": "1c4a8f30-95d2-4b7e-a6f1-38c0e7b52a49",
            "access_projection_reference": 14,
        },
    },
    "occurred_at": "2026-09-16T10:00:00.100Z",
    "extensions": [
        {"kind": "slack.channel_type", "value": "channel"},
        {"kind": "slack.event_type", "value": "message"},
    ],
}


def gateway_negotiation(**limits: int) -> dict[str, Any]:
    receive_limits = {
        "frame_bytes": 262144,
        "transfer_bytes": 16777216,
        "memory_bytes": 67108864,
        "in_flight_requests": 256,
    }
    receive_limits.update(limits)
    return {
        "preferred_version": "1",
        "supported_versions": ["1"],
        "delivery_modes": ["pull"],
        "receive_limits": receive_limits,
    }


def capabilities(self_id: str = AGENT_AUTHOR) -> dict[str, Any]:
    body = copy.deepcopy(CAPABILITIES)
    body["self"]["id"] = self_id
    return body


def message_event(
    *,
    purpose: str = "invoke",
    text: str = "hello",
    conversation: str = "C_FIXTURE_CHANNEL",
    conversation_kind: str = "channel",
    message_id: str = "1700000100.000200",
    thread: str | None = None,
    mention_agent: bool = False,
    author: str = "U_FIXTURE_MEMBER",
    event_id: str | None = None,
    connection_id: str = CONNECTION_ID,
    kind: str = "message",
) -> dict[str, Any]:
    """A message event in the shape of the contract samples, without a delivery."""
    event = copy.deepcopy(MESSAGE_EVENT)
    event["kind"] = kind
    event["event_id"] = event_id or f"ev-{purpose}-{conversation}-{message_id}-{kind}"
    event["message_revision_id"] = f"revision:{message_id}:0"
    event["connection_id"] = connection_id
    event["conversation"] = {"id": conversation, "kind": conversation_kind, "display_name": "deployments"}
    event["message"] = {"id": message_id}
    event["author"] = {"id": author, "kind": "human", "display_name": "Example Member"}
    if thread is not None:
        event["thread"] = {"id": thread}
    content: list[dict[str, Any]] = []
    if mention_agent:
        content.append({"kind": "reference", "reference": {
            "kind": "mention", "mention_kind": "user", "id": AGENT_AUTHOR, "label": "Nerve",
        }})
        text = " " + text
    content.append({"kind": "text", "text": {"format": "markdown", "body": text}})
    event["content"] = content
    if purpose == "observe":
        event["admission"] = {"purpose": "observe", "observe": {"provenance": {
            "provider": event["provider"],
            "issuer": event["issuer"],
            "installation_id": event["installation_id"],
        }}}
    return event


class FakeChannelGateway:
    """Tokens, a JWK Set, streams, and an inbox, for one tenant and agent."""

    def __init__(
        self,
        *,
        tenant_id: str = TENANT_ID,
        agent_id: str = AGENT_ID,
        issuer: str = ISSUER,
        jwks_url: str = JWKS_URL,
    ) -> None:
        self.tenant_id = tenant_id
        self.agent_id = agent_id
        self.issuer = issuer
        self.jwks_url = jwks_url
        self.keys: dict[str, ec.EllipticCurvePrivateKey] = {}
        self.published: list[str] = []
        self.jwks_fetches = 0
        self.kid = self.add_key("gateway-test-key-1")
        # The inbox: event dicts in store order, each with its delivery.
        self.inbox: list[dict[str, Any]] = []
        self.acknowledged: dict[str, str] = {}
        self.reads: list[dict[str, Any]] = []
        self.acks: list[list[dict[str, Any]]] = []
        self.streams: list[FakeStream] = []
        self._next_inbox_id = 100
        # Switches. Each counter applies to that many requests, then resets.
        self.hold_reads = False
        self.unavailable_reads = 0
        self.unavailable_acks = 0
        self.forget_acks = 0          # the ack is lost: not stored, no result
        self.drop_ack_results = 0     # the ack is stored, its result is lost
        self.read_changed = asyncio.Event()
        self.ack_changed = asyncio.Event()

    # ------------------------------------------------------------------ #
    #  Tokens and keys                                                     #
    # ------------------------------------------------------------------ #

    def add_key(self, kid: str, *, publish: bool = True) -> str:
        self.keys[kid] = ec.generate_private_key(ec.SECP256R1())
        if publish:
            self.published.append(kid)
        return kid

    def jwks(self) -> dict[str, Any]:
        keys = []
        for kid in self.published:
            entry = json.loads(ECAlgorithm.to_jwk(self.keys[kid].public_key()))
            entry.update({"kid": kid, "alg": "ES256", "use": "sig"})
            keys.append(entry)
        return {"keys": keys}

    def transport(self) -> httpx.MockTransport:
        """An httpx transport that serves the JWK Set and counts fetches."""

        def handle(request: httpx.Request) -> httpx.Response:
            if str(request.url) != self.jwks_url:
                return httpx.Response(404)
            self.jwks_fetches += 1
            return httpx.Response(200, json=self.jwks())

        return httpx.MockTransport(handle)

    def token(self, *, kid: str | None = None, algorithm: str = "ES256", **claims: Any) -> str:
        """A workload identity token. A claim set to ``None`` is left out."""
        now = int(time.time())
        payload: dict[str, Any] = {
            "iss": self.issuer,
            "sub": f"tenants/{self.tenant_id}/agents/{self.agent_id}",
            "aud": AUDIENCE,
            "iat": now,
            "nbf": now,
            "exp": now + 300,
            "jti": uuid.uuid4().hex,
            "tenant_id": self.tenant_id,
            "agent_id": self.agent_id,
        }
        payload.update(claims)
        payload = {name: value for name, value in payload.items() if value is not None}
        kid = kid or self.kid
        key = self.keys.get(kid) or self.keys[self.kid]
        return jwt.encode(payload, key, algorithm=algorithm, headers={"kid": kid})

    # ------------------------------------------------------------------ #
    #  Inbox                                                               #
    # ------------------------------------------------------------------ #

    def store(self, event: dict[str, Any]) -> str:
        """Add a row to the inbox and return its inbox ID."""
        self._next_inbox_id += 1
        inbox_id = str(self._next_inbox_id)
        row = copy.deepcopy(event)
        row["delivery"] = {"inbox_id": inbox_id, "received_at": "2026-09-16T10:05:00.240Z"}
        self.inbox.append(row)
        return inbox_id

    def unacknowledged(self) -> list[dict[str, Any]]:
        return [row for row in self.inbox if row["delivery"]["inbox_id"] not in self.acknowledged]

    def page(self, maximum_events: int) -> dict[str, Any]:
        events = self.unacknowledged()[:maximum_events]
        return {"outcome": "succeeded", "events": events} if events else {"outcome": "succeeded"}

    def outcome(self, inbox_id: str) -> str | None:
        return self.acknowledged.get(inbox_id)

    async def wait_for(self, predicate, timeout: float = EVENT_TIMEOUT) -> None:
        """Wait until *predicate* holds, checking after each read and ack."""
        deadline = asyncio.get_running_loop().time() + timeout
        while not predicate():
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise AssertionError("timed out waiting for the gateway state")
            try:
                await asyncio.wait_for(self._any_change(), min(remaining, 0.05))
            except TimeoutError:
                pass

    async def _any_change(self) -> None:
        read = asyncio.ensure_future(self.read_changed.wait())
        ack = asyncio.ensure_future(self.ack_changed.wait())
        try:
            await asyncio.wait({read, ack}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            read.cancel()
            ack.cancel()
        self.read_changed.clear()
        self.ack_changed.clear()

    async def serve(self, stream: FakeStream, frame: dict[str, Any]) -> None:
        """Answer one inbox request the way the gateway would."""
        kind = frame["kind"]
        request_id = frame["request_id"]
        if kind == "inbox_read":
            self.reads.append({"stream": stream, "request_id": request_id, **frame["payload"]["inbox_read"]})
            self.read_changed.set()
            if self.hold_reads:
                stream.held.append(frame)
                return
            if self.unavailable_reads:
                self.unavailable_reads -= 1
                await stream.respond(request_id, "inbox_read_result", {
                    "outcome": "unavailable", "reason_code": "overloaded",
                })
                return
            page = self.page(frame["payload"]["inbox_read"]["maximum_events"])
            self.reads[-1]["served"] = [row["delivery"]["inbox_id"] for row in page.get("events", [])]
            await stream.respond(request_id, "inbox_read_result", page)
        elif kind == "inbox_ack":
            items = frame["payload"]["inbox_ack"]["items"]
            if self.forget_acks:
                self.forget_acks -= 1
                self.acks.append([{**item, "lost": True} for item in items])
                self.ack_changed.set()
                return
            if self.unavailable_acks:
                self.unavailable_acks -= 1
                self.acks.append([{**item, "unavailable": True} for item in items])
                self.ack_changed.set()
                await stream.respond(request_id, "inbox_ack_result", {
                    "outcome": "unavailable", "reason_code": "maintenance",
                })
                return
            self.acks.append(items)
            for item in items:
                self.acknowledged.setdefault(item["inbox_id"], item["outcome"])
            self.ack_changed.set()
            if self.drop_ack_results:
                self.drop_ack_results -= 1
                return
            await stream.respond(request_id, "inbox_ack_result", {"outcome": "succeeded"})

    # ------------------------------------------------------------------ #
    #  Streams                                                             #
    # ------------------------------------------------------------------ #

    async def open_stream(
        self,
        url: str,
        *,
        token: str | None = None,
        headers: dict[str, str] | list[tuple[str, str]] | None = None,
        negotiate: bool = True,
        advertise: bool = True,
        auto_serve: bool = True,
    ) -> FakeStream:
        """Open a stream, then negotiate and advertise capabilities.

        Raises ``websockets.exceptions.InvalidStatus`` when Nerve refuses the
        upgrade.
        """
        if headers is None:
            headers = {"Authorization": f"Bearer {token or self.token()}"}
        websocket = await connect(url, additional_headers=headers, open_timeout=EVENT_TIMEOUT)
        stream = FakeStream(self, websocket, auto_serve=auto_serve)
        self.streams.append(stream)
        stream.start()
        if negotiate:
            await stream.negotiate()
            if advertise:
                await stream.advertise()
        return stream

    async def close_all(self) -> None:
        for stream in self.streams:
            await stream.close()


class FakeStream:
    """One gateway replica's stream to Nerve."""

    def __init__(self, gateway: FakeChannelGateway, websocket: ClientConnection, *, auto_serve: bool) -> None:
        self.gateway = gateway
        self.websocket = websocket
        self.auto_serve = auto_serve
        self.frames: list[dict[str, Any]] = []
        self.requests: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.held: list[dict[str, Any]] = []
        self.nerve_negotiation = asyncio.Event()
        self.closed = asyncio.Event()
        self.close_code: int | None = None
        self.close_reason = ""
        self._next_id = 0
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._read())

    def _request_id(self) -> str:
        self._next_id += 1
        return f"g-{self._next_id}"

    async def _read(self) -> None:
        try:
            async for text in self.websocket:
                frame = json.loads(text)
                self.frames.append(frame)
                if frame["kind"] == "negotiation":
                    self.nerve_negotiation.set()
                elif frame["kind"] in ("inbox_read", "inbox_ack"):
                    if self.auto_serve:
                        await self.gateway.serve(self, frame)
                    else:
                        await self.requests.put(frame)
        except ConnectionClosed:
            pass
        finally:
            self.close_code = self.websocket.close_code
            self.close_reason = self.websocket.close_reason or ""
            self.closed.set()

    async def send(self, kind: str, body: dict[str, Any], *, connection_id: str | None = None) -> str:
        request_id = self._request_id()
        frame: dict[str, Any] = {"version": "1", "kind": kind, "request_id": request_id}
        if connection_id is not None:
            frame["connection_id"] = connection_id
        frame["payload"] = {kind: body}
        await self.websocket.send(json.dumps(frame))
        return request_id

    async def send_raw(self, text: str) -> None:
        await self.websocket.send(text)

    async def respond(self, correlation_id: str, kind: str, body: dict[str, Any]) -> None:
        frame = {"version": "1", "kind": kind, "correlation_id": correlation_id, "payload": {kind: body}}
        try:
            await self.websocket.send(json.dumps(frame))
        except ConnectionClosed:
            pass

    async def negotiate(self, **limits: int) -> None:
        await self.send("negotiation", gateway_negotiation(**limits))
        await asyncio.wait_for(self.nerve_negotiation.wait(), EVENT_TIMEOUT)

    async def advertise(self, *, connection_id: str = CONNECTION_ID, self_id: str = AGENT_AUTHOR) -> None:
        await self.send("capabilities", capabilities(self_id), connection_id=connection_id)

    async def nudge(self, purpose: str | None = None) -> None:
        await self.send("nudge", {"purpose": purpose} if purpose else {})

    async def drain(self) -> None:
        await self.send("drain", {
            "reason": "rollout",
            "initiated_at": "2026-09-16T10:30:00Z",
            "deadline": "2026-09-16T10:31:00Z",
        })

    async def next_request(self, kind: str, timeout: float = EVENT_TIMEOUT) -> dict[str, Any]:
        while True:
            frame = await asyncio.wait_for(self.requests.get(), timeout)
            if frame["kind"] == kind:
                return frame

    def sent(self, kind: str) -> list[dict[str, Any]]:
        """Frames of *kind* that Nerve sent on this stream."""
        return [frame for frame in self.frames if frame["kind"] == kind]

    async def close(self) -> None:
        await self.websocket.close()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, EVENT_TIMEOUT)
            except (TimeoutError, asyncio.CancelledError):
                pass


class NerveServer:
    """Serve an ASGI app on a free loopback port for the length of a test."""

    def __init__(self, app: Any) -> None:
        self._server = uvicorn.Server(uvicorn.Config(
            app, host="127.0.0.1", port=0, lifespan="off", log_level="warning",
        ))
        self._task: asyncio.Task | None = None
        self.port = 0

    async def __aenter__(self) -> NerveServer:
        self._task = asyncio.create_task(self._server.serve())
        deadline = asyncio.get_running_loop().time() + EVENT_TIMEOUT
        while not self._server.started:
            if self._task.done():
                self._task.result()
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError("uvicorn did not start")
            await asyncio.sleep(0.01)
        self.port = self._server.servers[0].sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc: object) -> None:
        self._server.should_exit = True
        if self._task is not None:
            await asyncio.wait_for(self._task, EVENT_TIMEOUT * 2)

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def stream_url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/_internal/channel/v1/stream"
