"""Target-addressed delivery — who may post where, unprompted.

Every other outbound path answers a person who wrote in first, so the
destination comes from their message. Here the agent names it, which makes
the destination the thing that has to be authorized. The policy seam is
``BaseChannel.authorize_outbound``: the router asks, the channel decides.

Covers the Slack write policy, the default refusal every other channel
inherits, the router guard on ``deliver``/``deliver_addressed``, and the
``send_channel_message`` handler that reports a refusal's reason.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.tools.handlers.notifications import send_channel_message_handler
from nerve.agent.tools.registry import ToolContext
from nerve.channels.access import Decision
from nerve.channels.base import BaseChannel, ChannelCapability
from nerve.channels.router import ChannelRouter
from nerve.channels.slack import SlackChannel
from nerve.config import NerveConfig, SlackConfig

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------- #
#  Doubles                                                                #
# ---------------------------------------------------------------------- #


class _PlainChannel(BaseChannel):
    """A channel that never overrode ``authorize_outbound``."""

    def __init__(self, name: str = "plain"):
        self._name = name
        self.sent: list[tuple[str, str]] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def capabilities(self) -> ChannelCapability:
        return ChannelCapability.SEND_TEXT

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, message) -> None:
        self.sent.append((message.target, message.text))


def _slack(**slack_kwargs) -> SlackChannel:
    """A Slack channel with a stub transport, ready to authorize.

    ``allow_outbound`` defaults on here so each test states only the policy
    it is about; the switch itself is covered by TestOutboundSwitch.
    """
    slack_kwargs.setdefault("allow_outbound", True)
    cfg = NerveConfig()
    cfg.slack = SlackConfig(
        enabled=True,
        bot_token="xoxb-test",
        app_token="xapp-test",
        **slack_kwargs,
    )
    channel = SlackChannel(cfg, router=MagicMock())
    channel._web = MagicMock()
    channel._web.chat_postMessage = AsyncMock(return_value={"ts": "1.1"})
    channel._web.conversations_info = AsyncMock(
        return_value={"channel": {"name": "general"}},
    )
    channel._state = "running"
    return channel


# ---------------------------------------------------------------------- #
#  Slack write policy                                                     #
# ---------------------------------------------------------------------- #


class TestSlackAuthorizeOutbound:
    async def test_an_allowed_conversation_is_approved(self):
        channel = _slack(allow_channels=["C0123ABCD"])

        verdict = await channel.authorize_outbound("C0123ABCD")

        assert verdict.allowed

    async def test_a_thread_target_is_approved_on_its_conversation(self):
        # The thread ts addresses a reply inside a conversation already
        # granted; splitting it off would refuse the allowed channel.
        channel = _slack(allow_channels=["C0123ABCD"])

        verdict = await channel.authorize_outbound("C0123ABCD:1700000000.000100")

        assert verdict.allowed

    async def test_a_conversation_off_the_allow_list_is_refused(self):
        channel = _slack(allow_channels=["C0123ABCD"])

        verdict = await channel.authorize_outbound("C0999ZZZZ")

        assert not verdict.allowed
        assert "not approved" in verdict.reason

    async def test_a_denied_conversation_is_refused(self, caplog):
        channel = _slack(allow_channels=["*"], deny_channels=["C0999ZZZZ"])

        with caplog.at_level("INFO"):
            verdict = await channel.authorize_outbound("C0999ZZZZ")

        assert not verdict.allowed
        # Coarse to the agent, specific to the log.
        assert "C0999ZZZZ" not in verdict.reason
        assert "deny pattern" in caplog.text

    async def test_a_group_dm_is_refused(self):
        # A `G` is ambiguous: legacy private channel or multi-person DM.
        # Refusing only `D` would let a group DM in through the door
        # marked "no unsolicited DMs".
        channel = _slack(allow_channels=["*"])
        channel._web.conversations_info = AsyncMock(
            return_value={"channel": {"is_mpim": True}},
        )

        verdict = await channel.authorize_outbound("G0123ABCD")

        assert not verdict.allowed
        assert "direct message" in verdict.reason

    async def test_a_private_channel_is_allowed(self):
        # The other half of the same ambiguity: a real `G` private channel
        # must still work.
        channel = _slack(allow_channels=["G0123ABCD"])
        channel._web.conversations_info = AsyncMock(
            return_value={"channel": {"name": "private-eng", "is_mpim": False}},
        )

        verdict = await channel.authorize_outbound("G0123ABCD")

        assert verdict.allowed

    async def test_an_unknowable_conversation_kind_fails_closed(self):
        channel = _slack(allow_channels=["*"])
        channel._web.conversations_info = AsyncMock(side_effect=RuntimeError("boom"))

        verdict = await channel.authorize_outbound("G0123ABCD")

        assert not verdict.allowed
        assert "could not establish" in verdict.reason

    async def test_a_conversation_name_is_not_a_target(self):
        # Targets are ids. Accepting a name would make the destination
        # depend on a lookup the caller does not control.
        channel = _slack(allow_channels=["general"])

        verdict = await channel.authorize_outbound("general")

        assert not verdict.allowed
        assert "not a name" in verdict.reason

    async def test_a_malformed_id_is_refused(self):
        channel = _slack(allow_channels=["*"])

        verdict = await channel.authorize_outbound("Cx")

        assert not verdict.allowed

    async def test_a_refusal_does_not_name_the_pattern_or_channel(self):
        # The reason goes back to the agent, which may repeat it into a
        # chat. The detail belongs in the log, not the reply.
        channel = _slack(allow_channels=["*"], deny_channels=["secret-*"])
        channel._web.conversations_info = AsyncMock(
            return_value={"channel": {"name": "secret-payroll"}},
        )

        verdict = await channel.authorize_outbound("C0123ABCD")

        assert not verdict.allowed
        assert "secret-payroll" not in verdict.reason
        assert "secret-*" not in verdict.reason

    async def test_a_direct_message_is_refused(self):
        # An inbound DM comes from someone who chose to write. An outbound
        # one does not, and allow_direct_messages never authorized a
        # recipient the agent picks for itself.
        channel = _slack(allow_channels=["*"], allow_direct_messages=True)

        verdict = await channel.authorize_outbound("D0123ABCD")

        assert not verdict.allowed
        assert "direct message" in verdict.reason

    async def test_an_unresolvable_name_is_refused_when_a_deny_list_needs_one(
        self, caplog,
    ):
        # conversations.info failing leaves the identity incomplete, and a
        # deny list cannot be checked against a name nobody could read.
        channel = _slack(allow_channels=["*"], deny_channels=["secrets"])
        channel._web.conversations_info = AsyncMock(side_effect=RuntimeError("boom"))

        with caplog.at_level("INFO"):
            verdict = await channel.authorize_outbound("C0123ABCD")

        assert not verdict.allowed
        assert "could not be fully identified" in caplog.text

    async def test_a_name_grant_matches_the_resolved_conversation(self):
        channel = _slack(allow_channels=["general"])

        verdict = await channel.authorize_outbound("C0123ABCD")

        assert verdict.allowed
        channel._web.conversations_info.assert_awaited_once()

    async def test_no_allow_channels_refuses_everything(self):
        # The empty PatternGate allows all comers, which is right for an
        # inbound check that already ran the user gate and wrong here:
        # there is no sender to have vetted. Without an explicit grant
        # there is no approved destination at all.
        channel = _slack(allow_users=["U0123ABCD"])

        verdict = await channel.authorize_outbound("C0123ABCD")

        assert not verdict.allowed
        assert "slack.allow_channels" in verdict.reason

    async def test_an_allowed_user_does_not_grant_a_channel(self):
        # allow_users says who may drive the agent, not where it may
        # broadcast. Reading it as a write grant would hand every
        # conversation the bot sits in to a cron run.
        channel = _slack(allow_users=["*"], deny_channels=["C0999ZZZZ"])

        verdict = await channel.authorize_outbound("C0123ABCD")

        assert not verdict.allowed

    async def test_a_refusal_costs_no_slack_api_call(self):
        channel = _slack()

        await channel.authorize_outbound("C0123ABCD")

        channel._web.conversations_info.assert_not_awaited()

    async def test_a_user_id_is_not_a_conversation(self):
        channel = _slack(allow_channels=["*"])

        verdict = await channel.authorize_outbound("U0123ABCD")

        assert not verdict.allowed
        assert "not a Slack conversation id" in verdict.reason

    async def test_an_empty_target_is_refused(self):
        channel = _slack(allow_channels=["*"])

        verdict = await channel.authorize_outbound("")

        assert not verdict.allowed
        assert "no Slack conversation id" in verdict.reason


class TestOutboundSwitch:
    async def test_addressed_delivery_is_off_by_default(self):
        # allow_channels is set by nearly every Slack deployment for inbound
        # access. Deriving writes from it alone would hand every cron run a
        # megaphone into those channels the moment this shipped.
        channel = _slack(allow_channels=["C0123ABCD"], allow_outbound=False)

        verdict = await channel.authorize_outbound("C0123ABCD")

        assert not verdict.allowed
        assert "slack.allow_outbound" in verdict.reason

    async def test_the_switch_does_not_widen_where_writes_may_go(self):
        # On, but the conversation is still not granted: the switch enables
        # the capability, allow_channels still bounds it.
        channel = _slack(allow_channels=["C0123ABCD"], allow_outbound=True)

        verdict = await channel.authorize_outbound("C0999ZZZZ")

        assert not verdict.allowed

    async def test_the_switch_alone_grants_nothing(self):
        channel = _slack(allow_outbound=True)

        verdict = await channel.authorize_outbound("C0123ABCD")

        assert not verdict.allowed
        assert "slack.allow_channels" in verdict.reason


class TestDefaultRefusal:
    async def test_a_channel_without_an_override_refuses(self):
        verdict = await _PlainChannel().authorize_outbound("anything")

        assert not verdict.allowed
        assert "addressed delivery" in verdict.reason


# ---------------------------------------------------------------------- #
#  Router guard                                                           #
# ---------------------------------------------------------------------- #


class TestRouterDeliver:
    async def test_an_approved_target_receives_the_message(self):
        router = ChannelRouter(MagicMock())
        channel = _slack(allow_channels=["C0123ABCD"])
        router.register(channel)

        verdict = await router.deliver_addressed("slack", "C0123ABCD", "hello")

        assert verdict.allowed
        channel._web.chat_postMessage.assert_awaited_once()
        assert channel._web.chat_postMessage.await_args.kwargs["channel"] == "C0123ABCD"

    async def test_the_caller_target_is_used_verbatim(self):
        # The whole point of this path: a session's last inbound message
        # must never redirect a delivery the caller addressed itself.
        router = ChannelRouter(MagicMock())
        channel = _slack(allow_channels=["C0123ABCD"])
        router.register(channel)
        router._message_context["s1"] = {
            "channel_name": "slack",
            "target": "C0999ZZZZ",
            "message_id": "1.0",
        }

        await router.deliver_addressed("slack", "C0123ABCD", "hello", "s1")

        assert channel._web.chat_postMessage.await_args.kwargs["channel"] == "C0123ABCD"

    async def test_a_refused_target_is_not_sent_to(self):
        router = ChannelRouter(MagicMock())
        channel = _slack(allow_channels=["C0123ABCD"])
        router.register(channel)

        verdict = await router.deliver_addressed("slack", "C0999ZZZZ", "hello")

        assert not verdict.allowed
        channel._web.chat_postMessage.assert_not_awaited()

    async def test_deliver_refuses_a_channel_that_never_opted_in(self):
        router = ChannelRouter(MagicMock())
        channel = _PlainChannel(name="plain")
        router.register(channel)

        await router.deliver("plain", "somewhere", "hello")

        assert channel.sent == []

    async def test_an_unknown_channel_is_refused(self):
        router = ChannelRouter(MagicMock())

        verdict = await router.deliver_addressed("nope", "C0123ABCD", "hello")

        assert not verdict.allowed
        assert "unknown channel" in verdict.reason

    async def test_a_transport_failure_is_not_reported_as_a_refusal(self):
        # "the policy said no" and "Slack was down" are different answers,
        # and only one of them is worth changing the config over.
        router = ChannelRouter(MagicMock())
        channel = _slack(allow_channels=["C0123ABCD"])
        channel._web.chat_postMessage = AsyncMock(side_effect=RuntimeError("boom"))
        router.register(channel)

        with pytest.raises(RuntimeError):
            await router.deliver_addressed("slack", "C0123ABCD", "hello")


# ---------------------------------------------------------------------- #
#  Tool handler                                                           #
# ---------------------------------------------------------------------- #


def _ctx(decision) -> ToolContext:
    engine = MagicMock()
    engine.router.deliver_addressed = AsyncMock(return_value=decision)
    return ToolContext(session_id="s1", engine=engine)


class TestSendChannelMessageHandler:
    async def test_a_delivered_message_is_confirmed(self):
        ctx = _ctx(Decision(True, "ok"))

        result = await send_channel_message_handler(
            ctx, {"channel": "slack", "target": "C0123ABCD", "text": "hi"},
        )

        assert "sent" in result.content[0]["text"].lower()
        ctx.engine.router.deliver_addressed.assert_awaited_once_with(
            "slack", "C0123ABCD", "hi", session_id="s1",
        )

    async def test_a_refusal_reports_the_reason(self):
        ctx = _ctx(Decision(False, "channel general (C1) is not on the allow list"))

        result = await send_channel_message_handler(
            ctx, {"channel": "slack", "target": "C0123ABCD", "text": "hi"},
        )

        text = result.content[0]["text"]
        assert "Refused" in text
        assert "not on the allow list" in text
        assert not result.is_error

    async def test_a_transport_failure_is_reported(self):
        engine = MagicMock()
        engine.router.deliver_addressed = AsyncMock(side_effect=RuntimeError("boom"))
        ctx = ToolContext(session_id="s1", engine=engine)

        result = await send_channel_message_handler(
            ctx, {"channel": "slack", "target": "C0123ABCD", "text": "hi"},
        )

        assert "Failed" in result.content[0]["text"]

    @pytest.mark.parametrize(
        "args,missing",
        [
            ({"channel": "", "target": "C1", "text": "hi"}, "channel"),
            ({"channel": "slack", "target": "  ", "text": "hi"}, "target"),
            ({"channel": "slack", "target": "C1", "text": "  "}, "text"),
        ],
    )
    async def test_a_missing_field_is_named(self, args, missing):
        ctx = _ctx(Decision(True, "ok"))

        result = await send_channel_message_handler(ctx, args)

        assert missing in result.content[0]["text"]
        ctx.engine.router.deliver_addressed.assert_not_awaited()

    async def test_no_engine_is_reported(self):
        result = await send_channel_message_handler(
            ToolContext(session_id="s1"),
            {"channel": "slack", "target": "C1", "text": "hi"},
        )

        assert "Engine not available" in result.content[0]["text"]
