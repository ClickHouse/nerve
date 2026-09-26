"""A channel gateway stand-in for hosted channel tests.

``FakeChannelGateway`` signs stream tokens with a test ES256 key and writes
the public JWK Set to a key file for Nerve. It opens WebSocket streams
to a Nerve endpoint, negotiates, advertises capabilities, and serves inbox
pages from an in-memory inbox while it records every read and
acknowledgement. Switches on the gateway simulate lost frames and outages.
It records every operation and answers it with the next scripted reply for
its kind, or with success. It serves ``file_read`` from ``files``, and it
answers a ``file_send`` after the final chunk and keeps it in ``uploads``.

``NerveServer`` runs an ASGI app on a loopback port with uvicorn, so the
tests exercise real HTTP upgrades and real status codes.

The frame and event templates follow the gateway's version 1 channel
contract samples.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import json
import secrets
import time
from pathlib import Path
from typing import Any

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
ISSUER = "nerve-gateway"
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


def capabilities(
    self_id: str = AGENT_AUTHOR, *, operations: list[str] | None = None, **limits: int,
) -> dict[str, Any]:
    """The sample capabilities, with other operations or limits if given."""
    body = copy.deepcopy(CAPABILITIES)
    body["self"]["id"] = self_id
    if operations is not None:
        body["operations"] = operations
    body["limits"].update(limits)
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
    """Tokens, a key file, streams, and an inbox, for one tenant and agent.

    With ``jwks_file``, every change to the published keys rewrites that file.
    """

    def __init__(
        self,
        *,
        tenant_id: str = TENANT_ID,
        agent_id: str = AGENT_ID,
        issuer: str = ISSUER,
        jwks_file: Path | None = None,
    ) -> None:
        self.tenant_id = tenant_id
        self.agent_id = agent_id
        self.issuer = issuer
        self.jwks_file = jwks_file
        self.keys: dict[str, ec.EllipticCurvePrivateKey] = {}
        self.published: list[str] = []
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
        # Every operation: {"stream", "request_id", "connection_id", "kind",
        # "operation", "at"}, where "at" is the event loop time of arrival.
        self.operations: list[dict[str, Any]] = []
        # kind -> replies used in order: a result body without "kind",
        # "hold" (no result), or "close" (close the stream, no result).
        self.operation_replies: dict[str, list[Any]] = {}
        self.operation_changed = asyncio.Event()
        self._next_message = 0
        # attachment ID -> its bytes, served by file_read.
        self.files: dict[str, bytes] = {}
        # Completed uploads: {"request_id", "file", "data"}.
        self.uploads: list[dict[str, Any]] = []
        # Largest chunk that file_read sends.
        self.chunk_bytes = 64 * 1024

    # ------------------------------------------------------------------ #
    #  Tokens and keys                                                     #
    # ------------------------------------------------------------------ #

    def add_key(self, kid: str, *, publish: bool = True) -> str:
        self.keys[kid] = ec.generate_private_key(ec.SECP256R1())
        if publish:
            self.published.append(kid)
            self.write_jwks()
        return kid

    def unpublish(self, kid: str) -> None:
        self.published.remove(kid)
        self.write_jwks()

    def write_jwks(self) -> None:
        """Write the public keys to the key file, as the local stack does for Nerve."""
        if self.jwks_file is not None:
            self.jwks_file.write_text(json.dumps(self.jwks()), encoding="utf-8")

    def jwks(self) -> dict[str, Any]:
        keys = []
        for kid in self.published:
            entry = json.loads(ECAlgorithm.to_jwk(self.keys[kid].public_key()))
            entry.update({"kid": kid, "alg": "ES256", "use": "sig"})
            keys.append(entry)
        return {"keys": keys}

    def token(self, *, kid: str | None = None, algorithm: str = "ES256", **claims: Any) -> str:
        """A stream token. A claim set to ``None`` is left out."""
        now = int(time.time())
        payload: dict[str, Any] = {
            "iss": self.issuer,
            "sub": f"tenants/{self.tenant_id}/agents/{self.agent_id}",
            "aud": AUDIENCE,
            "iat": now,
            "nbf": now,
            "exp": now + 300,
            "jti": secrets.token_urlsafe(16),
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
        events = (self.read_changed, self.ack_changed, self.operation_changed)
        waits = {asyncio.ensure_future(event.wait()) for event in events}
        try:
            await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for wait in waits:
                wait.cancel()
        for event in events:
            event.clear()

    # ------------------------------------------------------------------ #
    #  Operations                                                          #
    # ------------------------------------------------------------------ #

    def script(self, kind: str, *replies: Any) -> None:
        """Queue replies for the next operations of *kind*."""
        self.operation_replies.setdefault(kind, []).extend(replies)

    def sent_operations(self, kind: str) -> list[dict[str, Any]]:
        """The payloads of the operations of *kind*, in arrival order."""
        return [record["operation"][kind] for record in self.operations if record["kind"] == kind]

    def new_target(self, destination: dict[str, Any]) -> dict[str, Any]:
        """A new message in *destination*, as a send result names it."""
        self._next_message += 1
        target = copy.deepcopy(destination)
        target["message"] = {"id": f"1700009000.{self._next_message:06d}"}
        return target

    async def serve_operation(self, stream: FakeStream, frame: dict[str, Any]) -> None:
        """Record one operation and answer it with its next scripted reply."""
        operation = frame["payload"]["operation"]
        kind = operation["kind"]
        self.operations.append({
            "stream": stream,
            "request_id": frame["request_id"],
            "connection_id": frame["connection_id"],
            "kind": kind,
            "operation": operation,
            "at": asyncio.get_running_loop().time(),
        })
        self.operation_changed.set()
        replies = self.operation_replies.get(kind)
        reply = replies.pop(0) if replies else {"outcome": "succeeded"}
        if reply == "hold":
            stream.held.append(frame)
            return
        if reply == "close":
            await stream.websocket.close()
            return
        if kind == "file_send" and reply.get("outcome") == "succeeded":
            # Answered after the final chunk, as the gateway does.
            stream.uploads[frame["request_id"]] = {"frame": frame, "reply": reply, "data": bytearray()}
            return
        reply = dict(reply)
        data = reply.pop("data", None)
        body = {"kind": kind, **reply}
        if body["outcome"] == "succeeded" and kind == "send" and "target" not in body:
            body["target"] = self.new_target(operation["send"]["destination"])
        if body["outcome"] == "succeeded" and kind == "file_read":
            read = operation["file_read"]
            if data is None:
                content = self.files[read["target"]["attachment"]["id"]]
                data = content[read["offset_bytes"]:read["offset_bytes"] + read["length_bytes"]]
            body.setdefault("transfer", {
                "transfer_id": f"t-{frame['request_id']}",
                "kind": "file_bytes",
                "serialization": "raw",
                "total_bytes": len(data),
            })
        await stream.respond(
            frame["request_id"], "operation_result", body, connection_id=frame["connection_id"],
        )
        if data is not None and body["outcome"] == "succeeded":
            await stream.send_chunks(
                frame["request_id"], frame["connection_id"], body["transfer"]["transfer_id"], data,
            )

    async def receive_chunk(self, stream: FakeStream, frame: dict[str, Any]) -> None:
        """Collect one upload chunk, and answer the upload after the final one."""
        upload = stream.uploads.get(frame["correlation_id"])
        if upload is None:
            return
        chunk = frame["payload"]["transfer"]
        upload["data"] += base64.b64decode(chunk["data"])
        if not chunk["final"]:
            return
        del stream.uploads[frame["correlation_id"]]
        request = upload["frame"]
        self.uploads.append({
            "request_id": request["request_id"],
            "file": request["payload"]["operation"]["file_send"]["file"],
            "data": bytes(upload["data"]),
        })
        self.operation_changed.set()
        await stream.respond(
            request["request_id"], "operation_result", {"kind": "file_send", **upload["reply"]},
            connection_id=request["connection_id"],
        )

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
        # request ID -> an upload whose final chunk has not arrived.
        self.uploads: dict[str, dict[str, Any]] = {}
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
                elif frame["kind"] == "operation":
                    # Served also without auto_serve; a "hold" reply keeps
                    # an operation for the test to answer.
                    await self.gateway.serve_operation(self, frame)
                elif frame["kind"] == "transfer":
                    await self.gateway.receive_chunk(self, frame)
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

    async def respond(
        self, correlation_id: str, kind: str, body: dict[str, Any], *, connection_id: str | None = None,
    ) -> None:
        frame: dict[str, Any] = {"version": "1", "kind": kind, "correlation_id": correlation_id}
        if connection_id is not None:
            frame["connection_id"] = connection_id
        frame["payload"] = {kind: body}
        try:
            await self.websocket.send(json.dumps(frame))
        except ConnectionClosed:
            pass

    async def send_chunks(
        self, correlation_id: str, connection_id: str, transfer_id: str, data: bytes,
    ) -> None:
        """Send *data* as the chunks of a read transfer."""
        size = self.gateway.chunk_bytes
        for offset in range(0, len(data), size):
            piece = data[offset:offset + size]
            await self.respond(correlation_id, "transfer", {
                "transfer_id": transfer_id,
                "offset": offset,
                "total_bytes": len(data),
                "data": base64.b64encode(piece).decode("ascii"),
                "final": offset + len(piece) == len(data),
            }, connection_id=connection_id)

    async def negotiate(self, **limits: int) -> None:
        await self.send("negotiation", gateway_negotiation(**limits))
        await asyncio.wait_for(self.nerve_negotiation.wait(), EVENT_TIMEOUT)

    async def advertise(
        self,
        *,
        connection_id: str = CONNECTION_ID,
        self_id: str = AGENT_AUTHOR,
        operations: list[str] | None = None,
        **limits: int,
    ) -> None:
        await self.send(
            "capabilities", capabilities(self_id, operations=operations, **limits),
            connection_id=connection_id,
        )

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
