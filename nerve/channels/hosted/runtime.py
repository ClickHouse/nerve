"""Build and run hosted channels and hosted sources from the process config.

The runtime owns the token verifier, the stream manager, the operation
runner, the inbox reader, one :class:`HostedChannel` for each chat provider in
hosted mode, and one :class:`HostedSourceConsumer` for each hosted source. The
hosted settings and the provider modes are read once at startup; a change
needs a restart. A reload reads the gateway key file again and publishes the
source subscriptions again. The source grant of each chat provider is read
per event, so it follows a reload.
"""

from __future__ import annotations

import functools
import logging
import uuid
from typing import TYPE_CHECKING, Any, Callable

from nerve.channels.hosted.auth import GatewayTokenVerifier
from nerve.channels.hosted.channel import HostedChannel
from nerve.channels.hosted.contract import (
    MAX_FRAME_BYTES,
    MAX_TRANSFER_BYTES,
    StreamLimits,
    Subscriptions,
)
from nerve.channels.hosted.intake import EventConsumer, InboxReader, ReaderSettings
from nerve.channels.hosted.manager import StreamManager
from nerve.channels.hosted.operations import OperationRunner, OperationTiming
from nerve.channels.hosted.sources import (
    HostedSourceConsumer,
    hosted_sources,
    subscriptions_for,
)
from nerve.channels.hosted.stream import ChannelStream, StreamTiming

if TYPE_CHECKING:
    from starlette.websockets import WebSocket

    from nerve.channels.router import ChannelRouter
    from nerve.config import NerveConfig
    from nerve.db import Database

logger = logging.getLogger(__name__)

# What Nerve accepts from the gateway on every stream. The whole protocol
# frame keeps every admitted event deliverable, and the transfer limit is the
# largest file a read can return.
RECEIVE_LIMITS = StreamLimits(
    frame_bytes=MAX_FRAME_BYTES,
    transfer_bytes=MAX_TRANSFER_BYTES,
    memory_bytes=2 * MAX_TRANSFER_BYTES,
    in_flight_requests=64,
)


def hosted_channel_providers(config: NerveConfig) -> list[str]:
    """The chat providers whose traffic comes from the gateway.

    A provider that is switched off is included. Its channel reads
    ``enabled`` for each event, so intake stays paused until a reload
    switches the provider on.
    """
    return ["slack"] if config.slack.mode == "hosted" else []


def hosted_providers(config: NerveConfig) -> list[str]:
    """Every provider whose traffic comes from the gateway: chat, then sources.

    A hosted source that is switched off is included too: the gateway is
    told that the agent subscribes to nothing, and the records already on
    their way are kept for when it is switched on.
    """
    return hosted_channel_providers(config) + hosted_sources(config)


class HostedChannelRuntime:
    """Everything hosted mode needs, started and stopped as one unit.

    ``db`` is where hosted sources buffer their records; without it, the
    router's engine database is used.
    """

    def __init__(
        self,
        config: NerveConfig,
        router: ChannelRouter,
        config_getter: Callable[[], NerveConfig],
        *,
        stream_timing: StreamTiming = StreamTiming(),
        reader_settings: ReaderSettings = ReaderSettings(),
        operation_timing: OperationTiming = OperationTiming(),
        notification_service: Any | None = None,
        db: Database | None = None,
    ) -> None:
        settings = config.channels.hosted
        problems = settings.problems()
        if problems:
            raise ValueError("; ".join(problems))
        providers = hosted_channel_providers(config)
        source_providers = hosted_sources(config)
        if not providers and not source_providers:
            raise ValueError("no provider is in hosted mode")
        if db is None:
            db = getattr(getattr(router, "engine", None), "db", None)
        if source_providers and db is None:
            raise ValueError("hosted sources need a database to buffer their records")
        self.router = router
        self._config = config_getter
        tenant_id = uuid.UUID(settings.tenant_id)
        agent_id = uuid.UUID(settings.agent_id)
        self.verifier = GatewayTokenVerifier(
            key_file=settings.gateway_jwks_file,
            issuer=settings.issuer,
            audience=settings.audience,
            tenant_id=tenant_id,
            agent_id=agent_id,
        )
        self.streams = StreamManager(
            verifier=self.verifier,
            receive_limits=RECEIVE_LIMITS,
            max_streams=settings.max_streams,
            timing=stream_timing,
            on_ready=self._stream_ready,
            on_nudge=self._stream_nudged,
            subscriptions=self.subscriptions if source_providers else None,
        )
        self.operations = OperationRunner(self.streams, operation_timing)
        self.channels: dict[str, HostedChannel] = {
            provider: HostedChannel(
                provider, router, config_getter,
                is_id=_identifier_test(provider),
                operations=self.operations,
                reaction_names=_reaction_names(provider),
                notifications=notification_service,
                **_notification_rules(provider),
            )
            for provider in providers
        }
        # Source consumers read events but are not channels: they stay out of
        # the router, so nothing routes a reply or a notification to them.
        self.sources: dict[str, HostedSourceConsumer] = {
            provider: HostedSourceConsumer(provider, db, config_getter)
            for provider in source_providers
        }
        consumers: dict[str, EventConsumer] = {**self.channels, **self.sources}
        self.reader = InboxReader(self.streams, consumers, settings=reader_settings)
        self._registered: list[HostedChannel] = []

    async def start(self) -> None:
        """Register the channels and start reading once a stream opens."""
        for channel in self.channels.values():
            if self.router.get_channel(channel.name) is not None:
                raise RuntimeError(f"a {channel.name} channel is already registered")
        for channel in self.channels.values():
            self.router.register(channel)
            self._registered.append(channel)
            await channel.start()
        self.reader.start()
        logger.info(
            "Hosted providers ready for gateway streams: %s",
            ", ".join([*self.channels, *(f"{name} (source)" for name in self.sources)]),
        )

    async def stop(self) -> None:
        """Drain and close the streams, then stop the channels and sources."""
        await self.streams.drain()
        await self.reader.stop()
        await self.streams.close()
        for source in self.sources.values():
            await source.stop()
        for channel in self._registered:
            try:
                await channel.stop()
            finally:
                self.router.unregister(channel)
        self._registered.clear()

    def reload_keys(self) -> str:
        """Read the gateway key file again, for a config reload.

        Raises :class:`~nerve.channels.hosted.auth.KeyFileError` and keeps the
        current keys when the file cannot be used.
        """
        return f"{self.verifier.reload_keys()} gateway key(s)"

    def subscriptions(self) -> Subscriptions | None:
        """The ``subscriptions`` payload from the current config.

        ``None`` without hosted sources: such a runtime sends no frame, so a
        gateway that predates the frame never receives one.
        """
        if not self.sources:
            return None
        return subscriptions_for(self._config(), self.sources)

    async def republish_subscriptions(self) -> str | None:
        """Send the subscriptions again on every open stream, after a reload.

        ``None`` without hosted sources, otherwise a summary for the reload.
        """
        if not self.sources:
            return None
        sent = await self.streams.publish_subscriptions()
        return f"published on {sent} stream(s)"

    def set_delivery_listener(self, listener: Callable[[str], Any] | None) -> None:
        """Call *listener* with the provider name whenever a source record is stored."""
        for source in self.sources.values():
            source.set_delivery_listener(listener)

    async def serve(self, websocket: WebSocket) -> None:
        """Handle one upgrade at the stream endpoint."""
        await self.streams.serve(websocket)

    def _stream_ready(self, stream: ChannelStream) -> None:
        self.reader.read_now()

    def _stream_nudged(self, stream: ChannelStream, purpose: str) -> None:
        self.reader.nudge(purpose)


def _identifier_test(provider: str) -> Callable[[str], bool] | None:
    """How a provider tells a literal ID from a name in a source pattern."""
    if provider == "slack":
        from nerve.channels.slack import is_slack_id

        return is_slack_id
    return None


def _notification_rules(provider: str) -> dict[str, Any]:
    """How a provider chooses its notification conversation and button styles.

    A config that names no Slack conversation is not a problem in hosted
    mode, because the gateway can choose one, so that case logs no warning.
    """
    if provider == "slack":
        from nerve.channels.slack import notification_target
        from nerve.channels.slack_presentation import approval_style

        return {
            "notification_target": functools.partial(notification_target, warn_unset=False),
            "button_style": approval_style,
        }
    return {}


def _reaction_names(provider: str) -> dict[str, str]:
    """A provider's short reaction names and their emoji."""
    if provider == "slack":
        from nerve.channels.slack_presentation import slack_emoji_by_name

        return slack_emoji_by_name()
    return {}


__all__ = [
    "RECEIVE_LIMITS",
    "HostedChannelRuntime",
    "hosted_channel_providers",
    "hosted_providers",
]
