"""Shared setup for the live Slack tests.

These talk to a real Slack workspace. They exist to settle the questions
:mod:`tests.fake_slack` structurally cannot: the fake answers whatever this
code asks it, so it confirms the client is self-consistent, not that its
beliefs about Slack are true.

Credentials come from the environment and every test skips when they are
absent, so the ordinary suite and CI on a fork are unaffected. See
:data:`SETUP` for what to provide.

Two tiers:

* **Outbound** needs the bot token, the app token, and a channel id. It
  covers everything Nerve *sends* — Block Kit validation, emoji short names,
  message splitting, streaming edits, uploads.
* **Inbound** additionally needs a user token, used to post as a human so a
  real event travels Slack → Socket Mode → the channel → the router. Without
  it there is no way to originate a message the bot will react to.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

import pytest

SETUP = """\
Live Slack tests need a scratch workspace and these environment variables.

Required (outbound):
  NERVE_SLACK_TEST_BOT_TOKEN   xoxb-…  Bot User OAuth Token
  NERVE_SLACK_TEST_APP_TOKEN   xapp-…  App-Level Token, connections:write
  NERVE_SLACK_TEST_CHANNEL     C…      A channel the bot has been invited to

Also required for the inbound tests:
  NERVE_SLACK_TEST_USER_TOKEN  xoxp-…  User OAuth Token for a human account in
                                       that workspace. Tests post as this user
                                       so the bot receives a genuine event.
                                       Needs chat:write and im:write.

Optional:
  NERVE_SLACK_TEST_BOT_TOKEN_NO_EMAIL
                               xoxb-…  A second bot token installed WITHOUT
                                       users:read.email. Proves Slack answers
                                       users.info successfully while omitting
                                       the email — the premise the deny-list
                                       fail-closed rule rests on.

The bot app needs the scopes from the manifest in docs/config.md, and its
App Home "Messages Tab" must be on with "Allow users to send Slash commands
and messages" ticked — otherwise the DM conversation is read-only and Slack
refuses the direct-message test with restricted_action_read_only_channel.

Use a throwaway workspace: these tests post, edit, react, and upload.
"""

BOT_TOKEN = os.environ.get("NERVE_SLACK_TEST_BOT_TOKEN", "")
APP_TOKEN = os.environ.get("NERVE_SLACK_TEST_APP_TOKEN", "")
TEST_CHANNEL = os.environ.get("NERVE_SLACK_TEST_CHANNEL", "")
USER_TOKEN = os.environ.get("NERVE_SLACK_TEST_USER_TOKEN", "")
NO_EMAIL_BOT_TOKEN = os.environ.get("NERVE_SLACK_TEST_BOT_TOKEN_NO_EMAIL", "")

HAVE_OUTBOUND = bool(BOT_TOKEN and APP_TOKEN and TEST_CHANNEL)
HAVE_INBOUND = HAVE_OUTBOUND and bool(USER_TOKEN)

requires_outbound = pytest.mark.skipif(
    not HAVE_OUTBOUND,
    reason="live Slack outbound creds not set (see tests/slack_live.py SETUP)",
)
requires_inbound = pytest.mark.skipif(
    not HAVE_INBOUND,
    reason="NERVE_SLACK_TEST_USER_TOKEN not set (see tests/slack_live.py SETUP)",
)
requires_no_email_token = pytest.mark.skipif(
    not NO_EMAIL_BOT_TOKEN,
    reason="NERVE_SLACK_TEST_BOT_TOKEN_NO_EMAIL not set",
)

# How long to wait for an event to travel Slack → Socket Mode → the router.
# Slack is usually well under a second; the ceiling is for a slow round trip
# rather than an expected wait, since every helper polls.
EVENT_TIMEOUT = 20.0

# How long to let a closed Socket Mode connection disappear before the
# next test opens one. Slack gives each event to exactly one of an app's
# connections, so an overlap silently steals events from the new test.
SOCKET_DRAIN_SECONDS = 2.5

# After connecting, wait until the socket has been this quiet before treating
# the next arrival as the test's own. Slack retries events that had no listener
# on a fixed schedule, so a test posting amid a retry burst can otherwise read
# someone else's exhaust.
SOCKET_QUIET_SECONDS = 1.5

# After the envelope has reached the channel, how long to let dispatch run
# before calling a refusal a refusal. Dispatch starts off the ack path, so
# this covers the policy's own Slack lookups rather than network delivery,
# which the arrival wait has already accounted for.
REFUSAL_SETTLE_SECONDS = 3.0

# The runner's clock minus Slack's, measured from a real Slack timestamp.
# Event ages are the difference between a stamp Slack wrote and a reading of
# the local clock, which are two different clocks. A runner that has drifted
# ahead of Slack by more than EVENT_TIMEOUT would call every fresh event
# stale and drop it, and on the refusal tests that reads as a guardrail
# doing its job. WSL2 across a host suspend drifts by far more than that, so
# the offset is measured rather than assumed.
_clock_offset: float | None = None


def note_slack_timestamp(slack_ts: Any) -> None:
    """Calibrate the local clock against one Slack-stamped timestamp."""
    global _clock_offset
    try:
        _clock_offset = time.time() - float(slack_ts)
    except (TypeError, ValueError):
        return


def event_age_seconds(event_time: Any) -> float | None:
    """Age of an event on Slack's clock, or None if it cannot be told.

    None means the harness has no calibration or no usable stamp. Callers
    treat that as "not stale": a stale event that slips through only makes a
    test wait for a marker it will not match, while wrongly dropping a fresh
    one hides the behavior under test.
    """
    if _clock_offset is None:
        return None
    try:
        return (time.time() - _clock_offset) - float(event_time)
    except (TypeError, ValueError):
        return None


def live_diagnostic(event: str, **fields: Any) -> None:
    """Emit one machine-readable, credential-free live-test diagnostic."""
    payload = {
        "event": event,
        "pid": os.getpid(),
        "time": round(time.time(), 3),
        **fields,
    }
    print(f"SLACK_LIVE {json.dumps(payload, sort_keys=True)}", flush=True)


@dataclass
class SocketDiagnostics:
    """Observe a live-test socket without changing its acknowledgement path."""

    label: str
    started_at: float = field(default_factory=time.monotonic)
    connections: int = 0
    envelopes: int = 0
    fresh: int = 0
    retry_attempts: Counter[int] = field(default_factory=Counter)
    event_types: Counter[str] = field(default_factory=Counter)
    delayed_envelopes: int = 0
    max_event_age_seconds: float = 0.0
    # Text of every envelope the harness handed to the channel, and of every
    # one it dropped first. A refusal test needs the difference: an event the
    # harness dropped as stale never met the policy, so a quiet router says
    # nothing about the guardrail.
    forwarded_texts: list[str] = field(default_factory=list)
    dropped_texts: list[str] = field(default_factory=list)
    _clients: int = 0

    def emit(self, event: str, **fields: Any) -> None:
        live_diagnostic(event, socket=self.label, **fields)

    @staticmethod
    def _text_of(req) -> str:
        payload = req.payload or {}
        return ((payload.get("event") or {}).get("text")) or ""

    def note_forwarded(self, req) -> None:
        self.forwarded_texts.append(self._text_of(req))

    def note_dropped(self, req) -> None:
        self.dropped_texts.append(self._text_of(req))

    def forwarded(self, marker: str) -> bool:
        """Whether an envelope carrying *marker* reached the channel."""
        return any(marker in text for text in self.forwarded_texts)

    def dropped(self, marker: str) -> bool:
        return any(marker in text for text in self.dropped_texts)

    def attach(self, socket) -> None:
        """Attach passive listeners before *socket* connects."""
        self._clients += 1
        client_number = self._clients

        async def observe_message(_client, message, _raw):
            kind = message.get("type")
            if kind == "hello":
                self.connections += 1
                self.emit(
                    "socket_hello",
                    client=client_number,
                    num_connections=message.get("num_connections"),
                    host=(message.get("debug_info") or {}).get("host"),
                )
        async def observe_request(_client, req):
            self.envelopes += 1
            payload = req.payload or {}
            slack_event = payload.get("event") or {}
            event_type = slack_event.get("type") or "none"
            subtype = slack_event.get("subtype")
            kind = f"{req.type}/{event_type}"
            if subtype:
                kind += f"/{subtype}"
            self.event_types[kind] += 1

            attempt = req.retry_attempt or 0
            if attempt:
                self.retry_attempts[attempt] += 1
            else:
                self.fresh += 1

            event_age = None
            timestamp_source = "event_time"
            event_ts = payload.get("event_time")
            if event_ts is None:
                timestamp_source = "event_ts"
                event_ts = slack_event.get("event_ts")
            if event_ts is None:
                timestamp_source = "message_ts"
                event_ts = slack_event.get("ts")
            try:
                event_age = max(0.0, time.time() - float(event_ts))
                self.max_event_age_seconds = max(
                    self.max_event_age_seconds, event_age,
                )
                if event_age > 5.0:
                    self.delayed_envelopes += 1
            except (TypeError, ValueError):
                pass

            if attempt:
                self.emit(
                    "retry_envelope",
                    attempt=attempt,
                    reason=req.retry_reason,
                    request_type=req.type,
                    event_type=event_type,
                    event_subtype=subtype,
                    event_age_seconds=(
                        round(event_age, 3) if event_age is not None else None
                    ),
                )
            elif event_age is not None and event_age > 5.0:
                self.emit(
                    "delayed_unmarked_envelope",
                    request_type=req.type,
                    event_type=event_type,
                    event_subtype=subtype,
                    event_age_seconds=round(event_age, 3),
                    timestamp_source=timestamp_source,
                )

        socket.message_listeners.append(observe_message)
        socket.socket_mode_request_listeners.append(observe_request)
        socket._live_diagnostics = self
        socket._live_diagnostics_request_listener = observe_request
        self.emit("socket_client_built", client=client_number)

    def emit_summary(self) -> None:
        self.emit(
            "socket_summary",
            duration_seconds=round(time.monotonic() - self.started_at, 3),
            clients=self._clients,
            connections=self.connections,
            envelopes=self.envelopes,
            fresh=self.fresh,
            retried=sum(self.retry_attempts.values()),
            retry_attempts=dict(sorted(self.retry_attempts.items())),
            event_types=dict(sorted(self.event_types.items())),
            delayed_envelopes=self.delayed_envelopes,
            max_event_age_seconds=round(self.max_event_age_seconds, 3),
        )



def unique_marker() -> str:
    """A token no other test or earlier run will carry."""
    return f"nvz{uuid.uuid4().hex[:10]}"


def direct_message_guardrails(user_id: str) -> dict[str, object]:
    """The explicit access settings every live DM contract must use."""
    return {
        "allow_users": [user_id],
        "allow_direct_messages": True,
    }


def make_client(token: str):
    """A Web API client with 429 retry, so a slow test does not go flaky."""
    from slack_sdk.http_retry.builtin_async_handlers import (
        AsyncRateLimitErrorRetryHandler,
    )
    from slack_sdk.web.async_client import AsyncWebClient

    client = AsyncWebClient(token=token)
    client.retry_handlers.append(AsyncRateLimitErrorRetryHandler(max_retry_count=5))
    return client


@dataclass
class Posted:
    """What the test created, so it can be cleaned up afterwards.

    An upload is not a message: chat.delete cannot remove it, so a file
    needs its own id recorded and files.delete to take it away. Without
    that, every run leaves another copy in the scratch channel for good.
    """

    bot: list[tuple[str, str]] = field(default_factory=list)   # (channel, ts)
    user: list[tuple[str, str]] = field(default_factory=list)
    files: list[str] = field(default_factory=list)             # file ids

    def note_bot(self, channel: str, ts: str | None) -> None:
        if ts:
            self.bot.append((channel, ts))

    def note_user(self, channel: str, ts: str | None) -> None:
        if ts:
            self.user.append((channel, ts))

    def note_file(self, file_id: str | None) -> None:
        if file_id:
            self.files.append(file_id)


class RecordingRouter:
    """A ChannelRouter stand-in that records what the channel hands it.

    The live tests are about the transport and the guardrails, so the engine
    is not involved: this captures each InboundMessage and optionally posts a
    reply through the channel, which is what the real router's stream adapter
    would end up doing.
    """

    def __init__(self, reply_text: str | None = None):
        self.messages: list[Any] = []
        self.reply_text = reply_text
        self.channel: Any = None
        self._sessions: dict[str, str] = {}
        self._arrived = asyncio.Event()

    async def handle_message(self, msg: Any) -> str:
        self._sessions.setdefault(msg.channel_key, f"s{len(self._sessions)}")
        if self.reply_text and self.channel is not None:
            from nerve.channels.base import OutboundMessage

            await self.channel.send(
                OutboundMessage(target=msg.sender_id, text=self.reply_text),
            )
        # Recorded last, so a test woken by wait_for_message can rely on the
        # reply already being posted. Recording first let a test read the
        # thread before the bot had answered it.
        self.messages.append(msg)
        self._arrived.set()
        return self.reply_text or "ok"

    async def get_last_session(self, channel_key: str) -> str | None:
        return self._sessions.get(channel_key)

    def _matching(self, marker: str) -> list[Any]:
        return [m for m in self.messages if marker in (m.text or "")]

    async def wait_for_message(
        self, marker: str, timeout: float = EVENT_TIMEOUT,
    ) -> Any:
        """Block until a message carrying *marker* is routed, and return it.

        Tests match on their own marker rather than on "something arrived".
        A live workspace is not a clean room: Slack redelivers an envelope it
        thinks went unacked, so a run that was interrupted can push an old
        message into a later run and make an unrelated test fail.
        """
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            found = self._matching(marker)
            if found:
                return found[-1]
            await asyncio.sleep(0.1)
        raise AssertionError(
            f"no inbound message carrying {marker!r} reached the router "
            f"within {timeout}s (saw {[m.text for m in self.messages]})",
        )

    async def expect_no_message(
        self, marker: str, channel, timeout: float = EVENT_TIMEOUT,
    ) -> None:
        """Assert the event met the policy and was refused by it.

        Waiting and finding an empty router proves little on its own. An
        event Slack never delivered, one it handed to another connection,
        and one the harness dropped as stale all look the same from here. So
        this first waits for the envelope to reach the channel, and only
        then asserts nothing came out the far side.
        """
        diagnostics = getattr(channel._client, "_live_diagnostics", None)
        assert diagnostics is not None, (
            "the live channel has no diagnostics, so a refusal cannot be "
            "told apart from an event that never arrived"
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if diagnostics.forwarded(marker):
                break
            await asyncio.sleep(0.1)
        else:
            dropped = diagnostics.dropped(marker)
            raise AssertionError(
                f"no envelope carrying {marker!r} reached the channel within "
                f"{timeout}s, so the guardrail was never exercised "
                f"({'the harness dropped it as stale' if dropped else 'Slack never delivered it'})",
            )

        # The envelope is in. Dispatch runs off the ack path, so give it room
        # before concluding the policy stopped it.
        await asyncio.sleep(REFUSAL_SETTLE_SECONDS)
        found = self._matching(marker)
        assert not found, (
            f"expected the message to be refused, but the router received "
            f"{found[-1].text!r}"
        )


async def wait_until_quiet(
    channel, quiet_for: float = SOCKET_QUIET_SECONDS, timeout: float = 120.0,
) -> None:
    """Block until no envelope has arrived for *quiet_for* seconds.

    Adaptive where a fixed sleep is not: instant on a clean socket, and
    patient while scheduled retries are arriving. The timeout is generous
    because a retry burst is exactly when this matters, and returning early
    leaves it streaming — which is how a test came to read a message posted
    by the one before it.

    Raises rather than returning quietly on timeout. Giving up silently
    turns a socket that never settles into a confusing assertion three
    tests later.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    started = loop.time()
    diagnostics = getattr(channel._client, "_live_diagnostics", None)
    while loop.time() < deadline:
        if time.monotonic() - channel._last_event_time >= quiet_for:
            if diagnostics:
                diagnostics.emit(
                    "socket_quiet",
                    quiet_for_seconds=quiet_for,
                    waited_seconds=round(loop.time() - started, 3),
                )
            return
        await asyncio.sleep(0.2)
    if diagnostics:
        diagnostics.emit(
            "socket_quiet_timeout",
            quiet_for_seconds=quiet_for,
            waited_seconds=round(loop.time() - started, 3),
        )
    raise AssertionError(
        f"the Slack socket never went quiet for {quiet_for}s within "
        f"{timeout}s — scheduled retries are still arriving",
    )


async def start_event_sink(
    bot_token: str = BOT_TOKEN,
    app_token: str = APP_TOKEN,
    diagnostics_label: str | None = None,
):
    """Open a Socket Mode client that acknowledges and discards every envelope.

    Slack retries events that have no listener, and a run of the outbound live
    tests produces dozens of them.  Those retries used to make the next run's
    inbound socket intermittently deaf to fresh messages.  Keeping this client
    beside the outbound tests prevents that retry schedule at its source.
    """
    from slack_sdk.socket_mode.aiohttp import SocketModeClient
    from slack_sdk.socket_mode.response import SocketModeResponse

    socket = SocketModeClient(
        app_token=app_token,
        web_client=make_client(bot_token),
        auto_reconnect_enabled=True,
    )

    async def acknowledge(client, req):
        await client.send_socket_mode_response(
            SocketModeResponse(envelope_id=req.envelope_id),
        )

    socket.socket_mode_request_listeners.append(acknowledge)
    if diagnostics_label:
        SocketDiagnostics(diagnostics_label).attach(socket)
    await socket.connect()
    diagnostics = getattr(socket, "_live_diagnostics", None)
    if diagnostics:
        diagnostics.emit("socket_connect_returned")
    return socket


async def wait_until_receiving(
    socket, timeout: float = 180.0, web_client=None,
    probe_interval: float = 15.0,
) -> None:
    """Block until Slack is delivering *fresh* events to this connection.

    Slack does not hold undelivered events in a queue; it retries them on a
    schedule — immediately, then again at +60s and ~+5min, each marked with
    a ``retry_attempt``. While retries are outstanding a newly opened socket
    is deaf to new events for roughly 20-30 seconds: anything posted in that
    window misses the first attempts and only comes back a minute later.

    So readiness cannot be "an envelope arrived". A retried envelope is at
    least a minute old and often belongs to an earlier run, and accepting
    one declares the socket ready while it is still black-holing. This posts
    a probe and waits for *that message's own ts*, unretried, re-probing
    until it lands.  It also waits for the probe's deletion event, making the
    round trip a teardown fence: once this returns, it has not left its own
    final mutation unacknowledged behind it.
    """
    client = web_client or socket.web_client
    loop = asyncio.get_running_loop()
    started = loop.time()
    deadline = started + timeout
    diagnostics = getattr(socket, "_live_diagnostics", None)
    seen_any: set[str] = set()
    seen_fresh: set[str] = set()
    seen_fresh_deletes: set[str] = set()
    arrivals: dict[str, tuple[float, int, str | None]] = {}
    deletion_arrivals: dict[str, float] = {}
    probe_posted: dict[str, float] = {}
    probes: list[str] = []
    deleted: set[str] = set()
    deletion_sent: dict[str, float] = {}
    completed = False

    if diagnostics:
        diagnostics.emit("delivery_barrier_started", timeout_seconds=timeout)

    # Wrap the listener list rather than channel._on_request: attribute
    # access builds a fresh bound method each time, so an identity check
    # against one never matches and the swap silently does nothing.
    listeners = socket.socket_mode_request_listeners
    installed = list(listeners)

    async def watch(sock, req):
        payload = req.payload or {}
        event = payload.get("event") or {}
        if event.get("ts"):
            seen_any.add(event["ts"])
            arrivals.setdefault(
                event["ts"],
                (loop.time(), req.retry_attempt or 0, req.retry_reason),
            )
        if not req.retry_attempt:
            if event.get("ts"):
                seen_fresh.add(event["ts"])
            if event.get("deleted_ts"):
                seen_fresh_deletes.add(event["deleted_ts"])
                deletion_arrivals.setdefault(event["deleted_ts"], loop.time())
        for fn in installed:
            await fn(sock, req)

    listeners[:] = [watch]
    try:
        while loop.time() < deadline:
            post_started = loop.time()
            probe = await client.chat_postMessage(
                channel=TEST_CHANNEL, text="nerve socket readiness probe",
            )
            ts = probe["ts"]
            # A message ts is Slack's own clock reading, which is what the
            # staleness cutoff has to measure against.
            note_slack_timestamp(ts)
            probes.append(ts)
            probe_posted[ts] = post_started
            if diagnostics:
                diagnostics.emit(
                    "probe_posted",
                    probe=len(probes),
                    clock_offset_seconds=round(_clock_offset or 0.0, 3),
                )
            settle = min(deadline, loop.time() + probe_interval)
            while loop.time() < settle:
                if ts in seen_fresh:
                    break
                await asyncio.sleep(0.2)
            if ts in seen_fresh:
                break
            if diagnostics:
                arrived = arrivals.get(ts)
                diagnostics.emit(
                    "probe_missed",
                    probe=len(probes),
                    waited_seconds=round(loop.time() - probe_posted[ts], 3),
                    arrived=arrived is not None,
                    retry_attempt=arrived[1] if arrived else None,
                    retry_reason=arrived[2] if arrived else None,
                )
        else:
            raise AssertionError(
                f"Slack was still not delivering fresh events after {timeout}s. "
                "A backlog of retries from an earlier run keeps a new socket "
                "deaf for 20-30s; longer than that suggests something else.",
            )

        # A failed probe has already missed Slack's immediate attempts and is
        # scheduled to come back at +60s.  Closing while that retry is pending
        # would make the readiness check itself poison the next run.  Keep the
        # socket open until every probe has arrived and been acknowledged.
        while loop.time() < deadline and not set(probes) <= seen_any:
            await asyncio.sleep(0.2)
        missing = set(probes) - seen_any
        if missing:
            raise AssertionError(
                f"Slack became ready, but {len(missing)} readiness probe(s) "
                f"were still awaiting retry after {timeout}s",
            )

        if diagnostics:
            for number, probe_ts in enumerate(probes, start=1):
                arrived_at, retry_attempt, retry_reason = arrivals[probe_ts]
                diagnostics.emit(
                    "probe_arrived",
                    probe=number,
                    latency_seconds=round(
                        arrived_at - probe_posted[probe_ts], 3,
                    ),
                    retry_attempt=retry_attempt,
                    retry_reason=retry_reason,
                )

        for probe_ts in probes:
            deletion_sent[probe_ts] = loop.time()
            await client.chat_delete(channel=TEST_CHANNEL, ts=probe_ts)
            deleted.add(probe_ts)

        # Do not close a socket immediately after deleting the fence: the Web
        # API response wins the race with its Socket Mode event.  That race
        # used to create one last retry after otherwise-clean fixture teardown.
        while loop.time() < deadline and not deleted <= seen_fresh_deletes:
            await asyncio.sleep(0.2)
        missing = deleted - seen_fresh_deletes
        if missing:
            raise AssertionError(
                f"Slack did not deliver {len(missing)} readiness-probe "
                f"deletion event(s) within {timeout}s",
            )
        completed = True
        if diagnostics:
            deletion_latencies = [
                max(0.0, deletion_arrivals[probe_ts] - deletion_sent[probe_ts])
                for probe_ts in deleted
            ]
            diagnostics.emit(
                "delivery_barrier_complete",
                duration_seconds=round(loop.time() - started, 3),
                probes=len(probes),
                missed_probes=max(0, len(probes) - 1),
                max_deletion_latency_seconds=round(
                    max(deletion_latencies, default=0.0), 3,
                ),
            )
        return
    finally:
        for probe_ts in set(probes) - deleted:
            try:
                await client.chat_delete(channel=TEST_CHANNEL, ts=probe_ts)
            except Exception:
                pass
        listeners[:] = installed
        if diagnostics and not completed:
            diagnostics.emit(
                "delivery_barrier_failed",
                duration_seconds=round(loop.time() - started, 3),
                probes=len(probes),
                probes_arrived=len(set(probes) & seen_any),
                deletions_arrived=len(set(probes) & seen_fresh_deletes),
            )


def ignore_stale_events(channel) -> None:
    """Make *channel* skip retries and events too old for the current test.

    Tests only ever wait for a message they just posted. A retried envelope,
    or any Events API callback older than ``EVENT_TIMEOUT``, therefore belongs
    to an earlier test or run. Slack has also delivered old callbacks without
    ``retry_attempt`` metadata, so checking the documented top-level
    ``event_time`` closes the hole that checking retry metadata alone leaves.

    Production does the opposite on purpose: a retry is how a message
    survives a restart, so the channel must handle it there. This is a
    test-harness concern only.
    """
    from slack_sdk.socket_mode.response import SocketModeResponse

    listeners = channel._client.socket_mode_request_listeners
    installed = list(listeners)

    async def drop_stale(sock, req):
        payload = req.payload or {}
        age = event_age_seconds(payload.get("event_time"))
        too_old = age is not None and age > EVENT_TIMEOUT

        diagnostics = getattr(sock, "_live_diagnostics", None)
        if req.retry_attempt or too_old:
            await sock.send_socket_mode_response(
                SocketModeResponse(envelope_id=req.envelope_id),
            )
            observer = getattr(
                sock, "_live_diagnostics_request_listener", None,
            )
            if observer:
                await observer(sock, req)
            if diagnostics:
                diagnostics.note_dropped(req)
            return
        if diagnostics:
            diagnostics.note_forwarded(req)
        for fn in installed:
            await fn(sock, req)

    listeners[:] = [drop_stale]


def build_channel(
    router: RecordingRouter,
    diagnostics_label: str | None = None,
    **slack_kwargs,
):
    """A SlackChannel wired to the live workspace with the given guardrails."""
    from nerve.channels.slack import SlackChannel
    from nerve.config import NerveConfig, SlackConfig

    cfg = NerveConfig()
    cfg.slack = SlackConfig(
        enabled=True,
        bot_token=BOT_TOKEN,
        app_token=APP_TOKEN,
        **slack_kwargs,
    )
    channel = SlackChannel(cfg, router)
    router.channel = channel
    if diagnostics_label:
        diagnostics = SocketDiagnostics(diagnostics_label)
        build_socket_client = channel._build_socket_client

        def build_instrumented_socket(
            *, app_token: str | None = None, web_client=None,
        ):
            socket = build_socket_client(
                app_token=app_token,
                web_client=web_client,
            )
            diagnostics.attach(socket)
            return socket

        channel._build_socket_client = build_instrumented_socket
        channel._live_diagnostics = diagnostics
    return channel, cfg


def build_outbound_channel(**slack_kwargs):
    """A SlackChannel that can post live, with no Socket Mode connection.

    Addressed delivery never reads an inbound event, so opening a socket
    would only take a share of this app's envelopes away from whichever
    test is waiting on one. The web client is real, which is the point:
    ``authorize_outbound`` resolves conversation names through
    ``conversations.info`` and the answer is Slack's, not a fixture's.

    ``allow_outbound`` defaults on so each test states only the policy it is
    about; the switch itself is unit-tested.
    """
    from nerve.channels.slack import SlackChannel
    from nerve.config import NerveConfig, SlackConfig

    slack_kwargs.setdefault("allow_outbound", True)
    cfg = NerveConfig()
    cfg.slack = SlackConfig(
        enabled=True,
        bot_token=BOT_TOKEN,
        app_token=APP_TOKEN,
        **slack_kwargs,
    )
    channel = SlackChannel(cfg, RecordingRouter())
    channel._web = make_client(BOT_TOKEN)
    channel._state = "running"
    return channel
