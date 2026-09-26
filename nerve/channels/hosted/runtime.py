"""Build and run hosted channels from the process config.

The runtime owns the token verifier, the stream manager, the inbox reader,
and one :class:`HostedChannel` for each provider in hosted mode. The hosted
settings and the provider modes are read once at startup; a change needs a
restart. The source grant of each provider is read per event, so it follows
a reload.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Callable

from nerve.channels.hosted.auth import WorkloadTokenVerifier
from nerve.channels.hosted.channel import HostedChannel
from nerve.channels.hosted.contract import (
    MAX_FRAME_BYTES,
    MAX_TRANSFER_BYTES,
    Capabilities,
    StreamLimits,
)
from nerve.channels.hosted.intake import InboxReader, ReaderSettings
from nerve.channels.hosted.manager import StreamManager
from nerve.channels.hosted.stream import ChannelStream, StreamTiming

if TYPE_CHECKING:
    import httpx
    from starlette.websockets import WebSocket

    from nerve.channels.router import ChannelRouter
    from nerve.config import NerveConfig

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


def hosted_providers(config: NerveConfig) -> list[str]:
    """The providers whose traffic comes from the gateway."""
    return ["slack"] if config.slack.enabled and config.slack.mode == "hosted" else []


class HostedChannelRuntime:
    """Everything hosted mode needs, started and stopped as one unit."""

    def __init__(
        self,
        config: NerveConfig,
        router: ChannelRouter,
        config_getter: Callable[[], NerveConfig],
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        stream_timing: StreamTiming = StreamTiming(),
        reader_settings: ReaderSettings = ReaderSettings(),
    ) -> None:
        settings = config.channels.hosted
        problems = settings.problems()
        if problems:
            raise ValueError("; ".join(problems))
        providers = hosted_providers(config)
        if not providers:
            raise ValueError("no provider is in hosted mode")
        self.router = router
        tenant_id = uuid.UUID(settings.tenant_id)
        agent_id = uuid.UUID(settings.agent_id)
        self.verifier = WorkloadTokenVerifier(
            issuer=settings.issuer,
            jwks_url=settings.jwks_url,
            audience=settings.audience,
            tenant_id=tenant_id,
            agent_id=agent_id,
            transport=transport,
        )
        self.channels: dict[str, HostedChannel] = {
            provider: HostedChannel(
                provider, router, config_getter, is_id=_identifier_test(provider),
            )
            for provider in providers
        }
        self.streams = StreamManager(
            verifier=self.verifier,
            receive_limits=RECEIVE_LIMITS,
            max_streams=settings.max_streams,
            timing=stream_timing,
            on_ready=self._stream_ready,
            on_nudge=self._stream_nudged,
            on_capabilities=self._stream_capabilities,
        )
        self.reader = InboxReader(self.streams, dict(self.channels), settings=reader_settings)
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
            "Hosted channels ready for gateway streams: %s", ", ".join(self.channels),
        )

    async def stop(self) -> None:
        """Drain and close the streams, then stop the channels."""
        await self.streams.drain()
        await self.reader.stop()
        await self.streams.close()
        for channel in self._registered:
            try:
                await channel.stop()
            finally:
                self.router.unregister(channel)
        self._registered.clear()

    async def serve(self, websocket: WebSocket) -> None:
        """Handle one upgrade at the stream endpoint."""
        await self.streams.serve(websocket)

    def _stream_ready(self, stream: ChannelStream) -> None:
        self.reader.read_now()

    def _stream_nudged(self, stream: ChannelStream, purpose: str) -> None:
        self.reader.nudge(purpose)

    def _stream_capabilities(
        self, stream: ChannelStream, connection_id: uuid.UUID, capabilities: Capabilities,
    ) -> None:
        for channel in self.channels.values():
            channel.note_capabilities(connection_id, capabilities)
        self.reader.capabilities_changed()


def _identifier_test(provider: str) -> Callable[[str], bool] | None:
    """How a provider tells a literal ID from a name in a source pattern."""
    if provider == "slack":
        from nerve.channels.slack import is_slack_id

        return is_slack_id
    return None


__all__ = ["RECEIVE_LIMITS", "HostedChannelRuntime", "hosted_providers"]
