"""A local stand-in for Slack — the Web API and the Socket Mode gateway.

Socket Mode is just a WebSocket the bot dials out to, and the URL for it
comes from ``apps.connections.open`` on the Web API. Point a
:class:`slack_sdk.web.async_client.AsyncWebClient` at a different
``base_url`` and both halves are ours, so a Slack integration test needs no
workspace, no tokens, and no network.

Usage::

    async with FakeSlack() as slack:
        channel = SlackChannel(lambda: cfg, router)
        with slack.patch_client(monkeypatch):
            await channel.start()
        await slack.push_event({"type": "message", ...})
        await slack.wait_for("chat.postMessage")

What it deliberately does not do: rate limits, pagination, scope
enforcement, or Block Kit validation. It answers the calls this channel
makes, records them, and lets a test push envelopes at the bot.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

from aiohttp import web


class FakeSlack:
    """An aiohttp app serving the Slack endpoints SlackChannel calls."""

    def __init__(self, bot_user_id: str = "U0BOT") -> None:
        self.bot_user_id = bot_user_id
        # Every Web API call, in order: (method_name, parsed_body).
        self.calls: list[tuple[str, dict[str, Any]]] = []
        # Envelope ids the bot acked over the socket.
        self.acks: list[str] = []
        # Names returned by users.info / conversations.info, set by the test.
        self.users: dict[str, dict[str, Any]] = {}
        self.conversations: dict[str, dict[str, Any]] = {}
        # Methods that should answer with an error instead of ok.
        self.errors: dict[str, str] = {}

        self._ws: web.WebSocketResponse | None = None
        self._connected = asyncio.Event()
        self._runner: web.AppRunner | None = None
        self._port = 0
        self._ts = 1_000_000.0

    # -- lifecycle ------------------------------------------------------ #

    async def __aenter__(self) -> FakeSlack:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def start(self) -> str:
        """Bind to a free port and start serving. Returns the base URL."""
        app = web.Application()
        app.router.add_get("/link", self._handle_socket)
        # slack_sdk sends read methods (users.info, conversations.info) as GET
        # and writes as POST, so both verbs reach the same dispatcher.
        app.router.add_post("/api/{method}", self._handle_api)
        app.router.add_get("/api/{method}", self._handle_api)

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        self._port = site._server.sockets[0].getsockname()[1]
        return self.base_url

    async def stop(self) -> None:
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._runner is not None:
            await self._runner.cleanup()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self._port}"

    def patch_client(self, monkeypatch) -> None:
        """Point slack_sdk's AsyncWebClient at this server.

        SlackChannel imports AsyncWebClient inside ``start()``, so replacing
        the module attribute is enough — and the Socket Mode client reuses
        that same web client to look up its WebSocket URL.
        """
        import functools

        import slack_sdk.web.async_client as module

        monkeypatch.setattr(
            module,
            "AsyncWebClient",
            functools.partial(module.AsyncWebClient, base_url=f"{self.base_url}/api/"),
        )

    # -- the socket ----------------------------------------------------- #

    async def _handle_socket(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(autoping=True)
        await ws.prepare(request)
        self._ws = ws
        await ws.send_str(json.dumps({"type": "hello", "num_connections": 1}))
        self._connected.set()

        async for message in ws:
            if message.type is not web.WSMsgType.TEXT:
                continue
            try:
                payload = json.loads(message.data)
            except json.JSONDecodeError:
                continue
            envelope_id = payload.get("envelope_id")
            if envelope_id:
                self.acks.append(envelope_id)
        return ws

    async def wait_connected(self, timeout: float = 5.0) -> None:
        """Block until the bot has opened its socket."""
        await asyncio.wait_for(self._connected.wait(), timeout)

    async def push(self, envelope_type: str, payload: dict[str, Any]) -> str:
        """Push one Socket Mode envelope at the bot. Returns its envelope id."""
        await self.wait_connected()
        assert self._ws is not None
        envelope_id = str(uuid.uuid4())
        await self._ws.send_str(json.dumps({
            "type": envelope_type,
            "envelope_id": envelope_id,
            "payload": payload,
            "accepts_response_payload": False,
        }))
        return envelope_id

    async def push_event(self, event: dict[str, Any]) -> str:
        """Push an Events API event (the common case)."""
        return await self.push("events_api", {"event": event})

    # -- the Web API ---------------------------------------------------- #

    async def _handle_api(self, request: web.Request) -> web.Response:
        method = request.match_info["method"]
        body = await self._parse_body(request)
        self.calls.append((method, body))

        if method in self.errors:
            return web.json_response({"ok": False, "error": self.errors[method]})

        handler = getattr(self, f"_api_{method.replace('.', '_')}", None)
        if handler is None:
            return web.json_response({"ok": True})
        return web.json_response(handler(body))

    @staticmethod
    async def _parse_body(request: web.Request) -> dict[str, Any]:
        """Read a call's arguments from the query string, a form, or JSON."""
        if request.method == "GET":
            items: Any = request.query.items()
        elif request.content_type == "application/json":
            return await request.json()
        else:
            items = (await request.post()).items()
        parsed: dict[str, Any] = {}
        for key, value in items:
            text = str(value)
            if text.startswith(("[", "{")):
                try:
                    parsed[key] = json.loads(text)
                    continue
                except json.JSONDecodeError:
                    pass
            parsed[key] = text
        return parsed

    def _next_ts(self) -> str:
        self._ts += 1
        return f"{self._ts:.6f}"

    def _api_auth_test(self, body: dict) -> dict:
        return {
            "ok": True,
            "user_id": self.bot_user_id,
            "user": "nerve",
            "team": "T0FAKE",
        }

    def _api_apps_connections_open(self, body: dict) -> dict:
        return {"ok": True, "url": f"ws://127.0.0.1:{self._port}/link"}

    def _api_chat_postMessage(self, body: dict) -> dict:
        return {"ok": True, "ts": self._next_ts(), "channel": body.get("channel")}

    def _api_chat_postEphemeral(self, body: dict) -> dict:
        return {"ok": True, "message_ts": self._next_ts()}

    def _api_chat_update(self, body: dict) -> dict:
        return {"ok": True, "ts": body.get("ts"), "channel": body.get("channel")}

    def _api_users_info(self, body: dict) -> dict:
        user_id = body.get("user", "")
        return {
            "ok": True,
            "user": self.users.get(user_id, {"id": user_id, "name": user_id, "profile": {}}),
        }

    def _api_conversations_info(self, body: dict) -> dict:
        channel_id = body.get("channel", "")
        return {
            "ok": True,
            "channel": self.conversations.get(
                channel_id, {"id": channel_id, "name": channel_id},
            ),
        }

    # -- assertions ----------------------------------------------------- #

    def calls_to(self, method: str) -> list[dict[str, Any]]:
        """Every recorded body for one API method, in order."""
        return [body for name, body in self.calls if name == method]

    async def wait_for(
        self, method: str, count: int = 1, timeout: float = 5.0,
    ) -> list[dict[str, Any]]:
        """Wait until *method* has been called *count* times, then return the bodies.

        Events are dispatched off the ack path, so a test that asserts
        immediately after pushing would race the handler.
        """
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            bodies = self.calls_to(method)
            if len(bodies) >= count:
                return bodies
            await asyncio.sleep(0.01)
        raise AssertionError(
            f"{method} was called {len(self.calls_to(method))} times, "
            f"expected {count}. Calls seen: {[n for n, _ in self.calls]}",
        )

    async def settle(self, delay: float = 0.15) -> None:
        """Give the bot time to finish dispatching before asserting a negative."""
        await asyncio.sleep(delay)
