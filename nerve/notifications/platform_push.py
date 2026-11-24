"""Web Push in external mode: the platform delivers it.

In external mode the gateway in front of Nerve keeps the browsers of the
members, decides at send time who gets a push, and sends it. The runtime in
the VM needs no VAPID key, no subscription routes and no access to a push
service. The ``webpush`` branch of the notification fan-out calls
:func:`hand_over`, which sends the notification with the ``platform_notify``
operation on a hosted channel connection that advertises it.

The payload holds what a push shows, cut as :mod:`nerve.notifications.webpush`
cuts it: the title, the first lines of the body, the agent name, and the
silent flag after quiet hours. The gateway applies its own display limits
again and builds the push.

The hand-over does not wait for a stream and does not retry. No connection,
or an outcome other than ``succeeded``, gives no delivery. The logs name the
notification and the outcome, never the text.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from nerve.channels.hosted.contract import PLATFORM_NOTIFY, PlatformNotifyOperation, Rejected
from nerve.channels.hosted.contract.model import MAX_PLATFORM_AGENT_NAME_BYTES, valid_platform_token
from nerve.channels.hosted.operations import OperationFailed
from nerve.notifications.webpush import TITLE_CHARS, short_body
from nerve.onboarding import read_agent_name

if TYPE_CHECKING:
    from nerve.config import NerveConfig

logger = logging.getLogger(__name__)

#: The priority of a notification whose priority is not a contract token.
DEFAULT_PRIORITY = "normal"
#: The outcome and reason of the gateway when no browser wants the notification.
NO_RECIPIENT = ("forbidden", "invalid_target")


def _utf8(text: str) -> str:
    """*text* as valid UTF-8: each lone surrogate becomes ``?``."""
    return text.encode("utf-8", "replace").decode("utf-8")


def _cut_bytes(text: str, maximum: int) -> str:
    """*text* cut to at most *maximum* UTF-8 bytes, on a character boundary."""
    return _utf8(text).encode("utf-8")[:maximum].decode("utf-8", "ignore")


def payload(
    *,
    notification_id: str,
    notif_type: str,
    title: str,
    body: str,
    priority: str,
    silent: bool,
    agent_name: str,
) -> PlatformNotifyOperation:
    """The ``platform_notify`` payload of one notification, cut as a push shows it."""
    return PlatformNotifyOperation(
        notification_id=notification_id,
        type=notif_type,
        title=_utf8(title[:TITLE_CHARS]),
        body=_utf8(short_body(body)),
        priority=priority if valid_platform_token(priority) else DEFAULT_PRIORITY,
        silent=silent,
        agent_name=_cut_bytes(agent_name, MAX_PLATFORM_AGENT_NAME_BYTES),
    )


async def hand_over(
    runtime: Any,
    config: NerveConfig,
    *,
    notification_id: str,
    notif_type: str,
    title: str,
    body: str,
    priority: str,
    silent: bool,
) -> bool:
    """Hand one notification over to the platform. True when the gateway took it.

    *runtime* is the hosted channel runtime, or ``None`` when Nerve has none.
    """
    operations = getattr(runtime, "operations", None)
    connections = sorted(operations.streams.connections(PLATFORM_NOTIFY)) if operations else []
    if not connections:
        logger.debug(
            "No connection advertises %s; notification %s gets no push", PLATFORM_NOTIFY, notification_id,
        )
        return False
    agent_name = await asyncio.to_thread(read_agent_name, Path(config.workspace))
    operation = payload(
        notification_id=notification_id, notif_type=notif_type, title=title, body=body,
        priority=priority, silent=silent, agent_name=agent_name or "",
    )
    try:
        # A value that the gateway refuses would close the stream.
        operation.validate()
        await operations.perform(
            connections[0], PLATFORM_NOTIFY, operation, rate_limit_retries=0, stream_wait=0,
        )
    except Rejected as error:
        logger.info("Notification %s gets no push: %s", notification_id, error.reason)
        return False
    except OperationFailed as error:
        quiet = (error.outcome, error.reason_code) == NO_RECIPIENT
        (logger.debug if quiet else logger.info)(
            "Notification %s was not handed over for push: %s", notification_id, error.detailed,
        )
        return False
    return True


__all__ = ["DEFAULT_PRIORITY", "hand_over", "payload"]
