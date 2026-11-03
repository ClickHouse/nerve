"""The hosted chat providers: the providers that a hosted channel can serve.

Each :class:`HostedChatProvider` in :data:`HOSTED_CHAT_PROVIDERS` names a
provider, says when the config puts it in hosted mode, reads its switch, and
gives the local settings of its channel. The runtime builds one
:class:`~nerve.channels.hosted.channel.HostedChannel` for each provider that
the config puts in hosted mode, in table order. A provider that the config
does not put in hosted mode has no channel, and the inbox reader rejects its
events.

A provider needs no Slack-shaped config section. A setting that its config
does not have gets the default of :class:`HostedChannelSettings`.

A provider that is switched off does one of two things with its events. The
gateway keeps the events of every provider in one ordered inbox, so the two
choices are to hold all of them or to refuse those of the provider:

- With ``hold_while_off``, its channel takes no events, and the inbox reader
  stops reading until a reload switches the provider on. The events wait in
  the gateway inbox, also those of the other providers. Slack does this.
- Without it, which is the default, its channel rejects each of its events,
  and the reader continues with the events of the other providers. Email
  does this.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from nerve.channels.hosted.contract.model import EMAIL_PROVIDER

if TYPE_CHECKING:
    from nerve.config import ChannelSourceConfig, NerveConfig


# Answers stream into a placeholder message (``partial``) or are posted when
# they are complete (``full``).
STREAM_MODE_PARTIAL = "partial"


@dataclass(frozen=True)
class HostedChannelSettings:
    """The local settings of one hosted chat channel.

    ``stream_mode`` ``partial`` streams an answer into a placeholder message,
    and ``full`` posts it when it is complete. ``allow_outbound`` lets the
    agent post to a conversation that it names, which must also pass
    ``allow_channels`` and ``deny_channels`` when they are set. ``source`` is
    the grant of observed messages; without it, observations are rejected.
    """

    stream_mode: str = STREAM_MODE_PARTIAL
    allow_outbound: bool = False
    allow_channels: tuple[str, ...] = ()
    deny_channels: tuple[str, ...] = ()
    source: ChannelSourceConfig | None = None


def _no_settings(config: NerveConfig) -> HostedChannelSettings:
    return HostedChannelSettings()


def _no_options() -> dict[str, Any]:
    return {}


@dataclass(frozen=True)
class HostedChatProvider:
    """One chat provider that a hosted channel can serve.

    ``name`` is the provider token of its events and connections, and the
    name of its channel. ``hosted`` says whether the config puts the
    provider in hosted mode; the runtime reads it once, at startup.
    ``switched_on`` and ``settings`` are read for each use, so they follow a
    reload. ``hold_while_off`` says what the channel does with events while
    the provider is switched off (see the module docstring).
    ``channel_options`` gives the provider's presentation rules, as keyword
    arguments of :class:`~nerve.channels.hosted.channel.HostedChannel`:
    ``is_id``, ``reaction_names`` and ``button_style``. A provider without
    them gets the channel's defaults. ``subscribes`` says that the gateway
    takes the provider's events only for an agent whose ``subscriptions``
    frame names the provider. While such a provider is switched on, the
    frame carries an entry with its name and no filters; while it is off,
    the frame leaves it out, and the gateway refuses its events before it
    stores them.
    """

    name: str
    hosted: Callable[[NerveConfig], bool]
    switched_on: Callable[[NerveConfig], bool]
    settings: Callable[[NerveConfig], HostedChannelSettings] = _no_settings
    hold_while_off: bool = False
    channel_options: Callable[[], dict[str, Any]] = _no_options
    subscribes: bool = False


# ---------------------------------------------------------------------- #
#  Slack                                                                  #
# ---------------------------------------------------------------------- #


def _slack_hosted(config: NerveConfig) -> bool:
    return config.slack.mode == "hosted"


def _slack_switched_on(config: NerveConfig) -> bool:
    return bool(config.slack.enabled and config.slack.mode == "hosted")


def _slack_settings(config: NerveConfig) -> HostedChannelSettings:
    slack = config.slack
    return HostedChannelSettings(
        stream_mode=slack.stream_mode,
        allow_outbound=slack.allow_outbound,
        allow_channels=tuple(slack.allow_channels or ()),
        deny_channels=tuple(slack.deny_channels or ()),
        source=slack.source,
    )


def _slack_channel_options() -> dict[str, Any]:
    """Slack IDs, short reaction names, and the button styles of notification cards.

    The config does not choose the conversation of a card: the gateway posts
    it in the agent's notification channel, which an admin sets in the
    control plane.
    """
    from nerve.channels.slack import is_slack_id
    from nerve.channels.slack_presentation import approval_style, slack_emoji_by_name

    return {
        "is_id": is_slack_id,
        "reaction_names": slack_emoji_by_name(),
        "button_style": approval_style,
    }


SLACK = HostedChatProvider(
    name="slack",
    hosted=_slack_hosted,
    switched_on=_slack_switched_on,
    settings=_slack_settings,
    hold_while_off=True,
    channel_options=_slack_channel_options,
)


# ---------------------------------------------------------------------- #
#  Email                                                                  #
# ---------------------------------------------------------------------- #


def email_hosted(config: NerveConfig) -> bool:
    """Whether the config gives Nerve an email channel.

    Email has no mode of its own: the channel gateway is the only way that
    mail arrives, so usable ``channels.hosted`` settings give the channel,
    also while ``email.enabled`` is false. A reload can then switch email on
    without a restart.
    """
    return not config.channels.hosted.problems()


def _email_switched_on(config: NerveConfig) -> bool:
    return bool(config.email.enabled)


def email_warning(config: NerveConfig) -> str | None:
    """Why ``email.enabled`` gives no email channel, or ``None``.

    ``None`` also while email is switched off. Nerve logs the text at
    startup, and ``nerve doctor`` shows it.
    """
    if not config.email.enabled:
        return None
    problems = config.channels.hosted.problems()
    if not problems:
        return None
    return (
        "email.enabled is set, but mail comes only through the channel gateway and the "
        f"channels.hosted settings are not usable ({'; '.join(problems)}); no email channel "
        "is registered"
    )


# The mail that members of the agent send to its address. Each mail is a
# direct conversation of its own sender and thread. The gateway announces the
# connection's behaviour: Nerve posts no answer, and the conversation keeps
# its session. Email has no settings of its own, refuses its events while it
# is switched off, and the gateway takes mail only for a subscribed agent.
EMAIL = HostedChatProvider(
    name=EMAIL_PROVIDER,
    hosted=email_hosted,
    switched_on=_email_switched_on,
    subscribes=True,
)

# Every hosted chat provider, in the order of their channels.
HOSTED_CHAT_PROVIDERS: tuple[HostedChatProvider, ...] = (SLACK, EMAIL)


def hosted_chat_providers(config: NerveConfig) -> list[HostedChatProvider]:
    """The providers that *config* puts in hosted mode, in table order.

    A provider that is switched off is included: its channel reads the
    switch for each event.
    """
    return [provider for provider in HOSTED_CHAT_PROVIDERS if provider.hosted(config)]


__all__ = [
    "EMAIL",
    "HOSTED_CHAT_PROVIDERS",
    "SLACK",
    "STREAM_MODE_PARTIAL",
    "HostedChannelSettings",
    "HostedChatProvider",
    "email_hosted",
    "email_warning",
    "hosted_chat_providers",
]
