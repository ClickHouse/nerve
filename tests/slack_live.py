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
import os
import uuid
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

# After connecting, wait until the socket has been this quiet before
# treating the next arrival as the test's own. Slack queues events that
# happened while nothing was listening and delivers the backlog to the
# connection that turns up, so a test posting straight after a handshake
# can be reading someone else's exhaust.
SOCKET_QUIET_SECONDS = 1.5


def unique_marker() -> str:
    """A token no other test or earlier run will carry."""
    return f"nvz{uuid.uuid4().hex[:10]}"


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
    """Messages the test created, so they can be cleaned up afterwards."""

    bot: list[tuple[str, str]] = field(default_factory=list)   # (channel, ts)
    user: list[tuple[str, str]] = field(default_factory=list)

    def note_bot(self, channel: str, ts: str | None) -> None:
        if ts:
            self.bot.append((channel, ts))

    def note_user(self, channel: str, ts: str | None) -> None:
        if ts:
            self.user.append((channel, ts))


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
        self.messages.append(msg)
        self._sessions.setdefault(msg.channel_key, f"s{len(self._sessions)}")
        self._arrived.set()
        if self.reply_text and self.channel is not None:
            from nerve.channels.base import OutboundMessage

            await self.channel.send(
                OutboundMessage(target=msg.sender_id, text=self.reply_text),
            )
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
        self, marker: str, settle: float = 6.0,
    ) -> None:
        """Assert nothing carrying *marker* is routed within *settle* seconds.

        The guardrail tests turn on this: a real Slack event reaches the
        socket and must go no further.
        """
        await asyncio.sleep(settle)
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
    patient when Slack is replaying a backlog. The timeout is generous
    because a backlog is exactly when this matters, and returning early
    leaves the replay still streaming — which is how a test came to read a
    message posted by the one before it.

    Raises rather than returning quietly on timeout. Giving up silently
    turns a socket that never settles into a confusing assertion three
    tests later.
    """
    import time

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if time.monotonic() - channel._last_event_time >= quiet_for:
            return
        await asyncio.sleep(0.2)
    raise AssertionError(
        f"the Slack socket never went quiet for {quiet_for}s within "
        f"{timeout}s — a backlog is still draining",
    )


async def wait_until_receiving(channel, timeout: float = 60.0) -> None:
    """Block until Slack is actually routing events to this connection.

    A completed handshake is not the same as being the connection Slack
    delivers to; there is a window after connecting where events go
    elsewhere or are held. Quiet does not distinguish "nothing is happening"
    from "nothing is reaching us", so this proves the path instead: post a
    message as the bot and wait for the channel to see *any* envelope.

    The probe is the bot's own message, which the channel filters out before
    the router — but ``_on_request`` still stamps ``_last_event_time``, so
    the stamp moving is the proof, and no test router is disturbed.
    """
    client = make_client(BOT_TOKEN)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while loop.time() < deadline:
        before = channel._last_event_time
        probe = await client.chat_postMessage(
            channel=TEST_CHANNEL, text="nerve socket readiness probe",
        )
        settle = loop.time() + 10.0
        while loop.time() < settle:
            if channel._last_event_time > before:
                try:
                    await client.chat_delete(
                        channel=TEST_CHANNEL, ts=probe["ts"],
                    )
                except Exception:
                    pass
                return
            await asyncio.sleep(0.2)
        try:
            await client.chat_delete(channel=TEST_CHANNEL, ts=probe["ts"])
        except Exception:
            pass

    raise AssertionError(
        f"Slack never routed an event to this connection within {timeout}s",
    )


def build_channel(router: RecordingRouter, **slack_kwargs):
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
    channel = SlackChannel(lambda: cfg, router)  # type: ignore[arg-type]
    router.channel = channel
    return channel, cfg
