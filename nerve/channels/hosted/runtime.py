"""Build and run hosted channels from the process config.

The runtime owns the token verifier, the stream manager, the operation
runner, the inbox reader, and one :class:`HostedChannel` for each provider in
hosted mode. The hosted settings and the provider modes are read once at
startup; a change needs a restart. A reload reads the gateway key file again.
The source grant of each provider is read per event, so it follows a reload.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any, Callable

from nerve.channels.hosted.auth import GatewayTokenVerifier
from nerve.channels.hosted.channel import HostedChannel
from nerve.channels.hosted.contract import (
    MAX_FRAME_BYTES,
    MAX_TRANSFER_BYTES,
    StreamLimits,
)
from nerve.channels.hosted.intake import InboxReader, ReaderSettings
from nerve.channels.hosted.manager import StreamManager
from nerve.channels.hosted.operations import OperationRunner, OperationTiming
from nerve.channels.hosted.stream import ChannelStream, StreamTiming

if TYPE_CHECKING:
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
    """The providers whose traffic comes from the gateway.

    A provider that is switched off is included. Its channel reads
    ``enabled`` for each event, so intake stays paused until a reload
    switches the provider on.
    """
    return ["slack"] if config.slack.mode == "hosted" else []


class HostedChannelRuntime:
    """Everything hosted mode needs, started and stopped as one unit."""

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

    def reload_keys(self) -> str:
        """Read the gateway key file again, for a config reload.

        Raises :class:`~nerve.channels.hosted.auth.KeyFileError` and keeps the
        current keys when the file cannot be used.
        """
        return f"{self.verifier.reload_keys()} gateway key(s)"

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
    """How a provider chooses its notification conversation and button styles."""
    if provider == "slack":
        from nerve.channels.slack import notification_target
        from nerve.channels.slack_presentation import approval_style

        return {"notification_target": notification_target, "button_style": approval_style}
    return {}


def _reaction_names(provider: str) -> dict[str, str]:
    """A provider's short reaction names and their emoji."""
    if provider == "slack":
        from nerve.channels.slack_presentation import slack_emoji_by_name

        return slack_emoji_by_name()
    return {}


__all__ = ["RECEIVE_LIMITS", "HostedChannelRuntime", "hosted_providers"]
