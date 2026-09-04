"""Serialized lifecycle ownership for the Slack channel."""

from __future__ import annotations

import asyncio
import copy
import logging
from typing import TYPE_CHECKING

from nerve.channels.slack import (
    WATCHDOG_HEARTBEAT_EVERY,
    WATCHDOG_INTERVAL,
    SlackChannel,
)

if TYPE_CHECKING:
    from nerve.channels.router import ChannelRouter
    from nerve.config import NerveConfig
    from nerve.notifications.service import NotificationService

logger = logging.getLogger(__name__)


class SlackRuntimeError(RuntimeError):
    """A sanitized Slack transition failure safe for logs and HTTP output."""


def _secrets_of(*configs: NerveConfig | None) -> tuple[str, ...]:
    """Every token that must not appear in a message handed to a caller."""
    return tuple(
        secret
        for config in configs
        if config is not None
        for secret in (config.slack.bot_token, config.slack.app_token)
        if secret
    )


def _redact(detail: str, secrets: tuple[str, ...]) -> str:
    for secret in secrets:
        detail = detail.replace(secret, "<redacted>")
    return detail


class SlackRuntime:
    """Own one Slack instance and serialize every lifecycle transition."""

    name = "slack"

    def __init__(
        self,
        router: ChannelRouter,
        notification_service: NotificationService | None = None,
    ) -> None:
        self.router = router
        self.notification_service = notification_service
        self._lock = asyncio.Lock()
        self._channel: SlackChannel | None = None
        self._active_config: NerveConfig | None = None
        self._watchdog_task: asyncio.Task | None = None
        self._closed = False

    @property
    def channel(self) -> SlackChannel | None:
        return self._channel

    @property
    def active_config(self) -> NerveConfig | None:
        return self._active_config

    async def reconcile(self, config: NerveConfig) -> str | None:
        """Atomically reconcile one desired process config generation."""
        async with self._lock:
            if self._closed:
                # The process is shutting down. The workspace sync loop is
                # given a bounded wait to finish the cycle it is in, and its
                # git phase runs in a worker thread past cancellation, so a
                # reload can still arrive here. Starting a socket now leaves
                # one open past exit.
                return "shutting down"
            # Captured before any transition, because stopping clears the
            # active generation and the tokens it held are exactly the ones
            # a failure in that transition can name.
            secrets = _secrets_of(self._active_config, config)
            try:
                return await self._reconcile_locked(copy.deepcopy(config))
            except (SlackRuntimeError, asyncio.CancelledError):
                raise
            except Exception as error:
                raise SlackRuntimeError(
                    _redact(str(error) or type(error).__name__, secrets),
                ) from error

    async def _reconcile_locked(self, desired: NerveConfig) -> str | None:
        """Move to *desired*, assuming the lifecycle lock is held."""
        if not desired.slack.enabled:
            if self._channel is None:
                return None
            await self._stop_locked(drain=True)
            return "disabled"

        if not desired.slack.bot_token or not desired.slack.app_token:
            raise RuntimeError("Slack needs both bot_token and app_token")

        if self._channel is None:
            return await self._enable_locked(desired)

        channel = self._channel
        if self.router.get_channel(self.name) is not channel:
            raise SlackRuntimeError(
                "the Slack runtime no longer owns the registered channel",
            )
        if not channel.is_available:
            await self._stop_locked(drain=False)
            return await self._enable_locked(desired)

        slack = desired.slack
        if channel.needs_credential_reload(slack.bot_token, slack.app_token):
            try:
                await channel.reload_credentials(desired)
            except Exception:
                if not channel.is_available:
                    await self._stop_locked(drain=False)
                raise
            self._active_config = desired
            return "credentials reloaded"

        channel.apply_config(desired)
        self._active_config = desired
        return None

    async def shutdown(self) -> None:
        """Stop Slack for good; no later reconcile may bring it back."""
        async with self._lock:
            self._closed = True
            if self._channel is not None:
                await self._stop_locked(drain=False)

    async def _enable_locked(self, desired: NerveConfig) -> str:
        existing = self.router.get_channel(self.name)
        if existing is not None:
            raise SlackRuntimeError(
                "a Slack channel exists outside the runtime lifecycle",
            )

        candidate = SlackChannel(desired, self.router)
        candidate.set_notification_service(self.notification_service)
        self._channel = candidate
        self.router.register(candidate)
        try:
            await candidate.start()
        except (Exception, asyncio.CancelledError) as error:
            try:
                await candidate.stop()
            except Exception:
                logger.debug(
                    "Slack cleanup after failed enable raised",
                    exc_info=True,
                )
            finally:
                self.router.unregister(candidate)
                self._channel = None
                self._active_config = None
            if isinstance(error, asyncio.CancelledError):
                raise
            raise

        self._active_config = desired
        self._watchdog_task = asyncio.create_task(
            self._watchdog(candidate),
            name="slack-socket-watchdog",
        )
        return "enabled"

    async def _stop_locked(self, *, drain: bool) -> None:
        channel = self._channel
        if channel is None:
            return
        await self._cancel_watchdog_locked()
        try:
            await channel.stop(drain=drain)
        finally:
            self.router.unregister(channel)
            if self._channel is channel:
                self._channel = None
                self._active_config = None

    async def _cancel_watchdog_locked(self) -> None:
        task = self._watchdog_task
        self._watchdog_task = None
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _watchdog(self, channel: SlackChannel) -> None:
        check_count = 0
        while True:
            await asyncio.sleep(WATCHDOG_INTERVAL)
            # The lifecycle lock covers the decision only. Repair runs
            # outside it, because rebuild_transport waits on a socket close
            # and a connect, and holding the lock across that made a config
            # reload and a shutdown wait on an unreachable Slack. The
            # channel's own transport lock still serializes repair against
            # credential rotation.
            async with self._lock:
                if self._channel is not channel or not channel.is_available:
                    return
                check_count += 1
                connected = await channel.transport_connected()
                if check_count % WATCHDOG_HEARTBEAT_EVERY == 0:
                    logger.info(
                        "Slack watchdog: %s (check #%d, last event %.0fs ago)",
                        "connected" if connected else "disconnected",
                        check_count,
                        channel.seconds_since_last_event,
                    )
            if connected:
                continue
            logger.warning("Slack socket is down, rebuilding")
            try:
                await channel.rebuild_transport()
            except Exception as error:
                logger.error(
                    "Slack reconnect failed: %s",
                    _redact(
                        str(error) or type(error).__name__,
                        _secrets_of(self._active_config),
                    ),
                )
            else:
                logger.info("Slack socket reconnected")
