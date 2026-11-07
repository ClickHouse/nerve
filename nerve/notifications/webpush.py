"""Web Push: the ``webpush`` channel of the notification fan-out.

The panel's service worker (``web/public/sw.js``) shows each push as a system
notification, also when no panel tab is open. This module holds all the code
that talks to the push services of the browsers: the instance's VAPID key, the
check of a subscription, the payload and the send. A sender outside the
agent's machine can replace :func:`deliver` and keep the rest of the fan-out.

The push services are those of the browsers (:data:`PUSH_SERVICE_HOSTS`,
:data:`PUSH_SERVICE_DOMAINS`). The instance must reach them: on Nerve Cloud an
agent in restricted internet mode cannot, so push works only in open mode.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import aiohttp
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from py_vapid import Vapid
from pywebpush import WebPushException, webpush_async

from nerve.config import level_wants
from nerve.onboarding import read_agent_name

if TYPE_CHECKING:
    from nerve.config import NerveConfig
    from nerve.db import Database

logger = logging.getLogger(__name__)

PUSH_CHANNEL = "webpush"

#: The ``instance_secrets`` row of the VAPID private key (raw, base64url).
VAPID_SECRET_NAME = "vapid_private_key"
#: The contact of the sender in each VAPID token (RFC 8292 ``sub``). Push
#: services require a ``mailto:`` or ``https:`` URI.
VAPID_SUBJECT = "https://github.com/ClickHouse/nerve"

#: The push services of Chrome and most Chromium browsers, and of Firefox.
PUSH_SERVICE_HOSTS = frozenset({"fcm.googleapis.com", "updates.push.services.mozilla.com"})
#: The push services of Safari and of Edge, by domain.
PUSH_SERVICE_DOMAINS = (".push.apple.com", ".notify.windows.com")
MAX_ENDPOINT_CHARS = 2048
#: Decoded sizes of the browser's keys: an uncompressed P-256 point, and the auth secret.
P256DH_BYTES = 65
AUTH_BYTES = 16

#: How long a push service keeps a notification for a browser that is offline.
PUSH_TTL_S = 24 * 3600
PUSH_TIMEOUT_S = 10
#: A push service answers these when the browser removed the subscription.
GONE_STATUSES = frozenset({404, 410})

#: The panel page that a click on the notification opens.
OPEN_PATH = "/notifications"
TITLE_CHARS = 120
BODY_LINES = 3
BODY_CHARS = 180
DEFAULT_AGENT_NAME = "Nerve"
_URGENT_PRIORITIES = frozenset({"high", "urgent"})


def check_endpoint(endpoint: str) -> str:
    """The endpoint when it is an https URL of a known push service, else
    :class:`ValueError`. Only those hosts, on the default port: a
    subscription can never make the instance send to itself or to a private
    address."""
    if len(endpoint) > MAX_ENDPOINT_CHARS:
        raise ValueError("The push endpoint is too long.")
    try:
        url = urlsplit(endpoint)
        port = url.port
    except ValueError:
        raise ValueError("The push endpoint is not a valid URL.") from None
    host = (url.hostname or "").lower()
    if url.scheme != "https" or url.username or url.password or port not in (None, 443):
        raise ValueError("The push endpoint must be an https URL.")
    if host not in PUSH_SERVICE_HOSTS and not host.endswith(PUSH_SERVICE_DOMAINS):
        raise ValueError("The push service of this browser is not supported.")
    return endpoint


def check_key(value: str, size: int) -> str:
    """A browser key in base64url that decodes to ``size`` bytes, else :class:`ValueError`."""
    try:
        decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (binascii.Error, ValueError):
        decoded = b""
    if len(decoded) != size:
        raise ValueError("The browser sent a key that is not valid.")
    return value


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


async def vapid_key(db: Database) -> Vapid:
    """The instance's VAPID key. Made at first use and kept in
    ``instance_secrets``, like the other secrets of the instance."""
    raw = await db._get_instance_secret(VAPID_SECRET_NAME)
    if raw is None:
        scalar = ec.generate_private_key(ec.SECP256R1()).private_numbers().private_value
        raw = await db._ensure_instance_secret(VAPID_SECRET_NAME, _b64url(scalar.to_bytes(32, "big")))
    key = Vapid.from_raw(raw.encode("ascii"))
    # py_vapid takes an https contact only without a path; RFC 8292 lets
    # ``sub`` be any https URI.
    key.conf = {"no-strict": True}
    return key


async def public_key(db: Database) -> str:
    """The VAPID public key that a browser subscribes with (base64url, uncompressed point)."""
    key = (await vapid_key(db)).public_key
    return _b64url(key.public_bytes(serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint))


def payload(agent: str, notification_id: str, title: str, body: str, silent: bool) -> str:
    """The JSON that the service worker shows: ``<agent>: <title>``, the
    first lines of the body, the notification id as tag, and the page to open."""
    lines = "\n".join(line for line in body.splitlines() if line.strip())
    short = "\n".join(lines.splitlines()[:BODY_LINES])
    return json.dumps({
        "title": f"{agent}: {title}"[:TITLE_CHARS],
        "body": short[:BODY_CHARS],
        "tag": notification_id,
        "url": OPEN_PATH,
        "silent": silent,
    })


async def deliver(
    db: Database,
    config: NerveConfig,
    *,
    notification_id: str,
    notif_type: str,
    title: str,
    body: str,
    priority: str,
    silent: bool,
) -> bool:
    """Push one notification to every subscribed browser whose level wants
    it. True when at least one push service took it. A subscription that
    the push service reports gone is deleted."""
    subscriptions = [
        row for row in await db.list_push_subscriptions()
        if level_wants(row["level"], notif_type, priority)
    ]
    if not subscriptions:
        return False
    vapid = await vapid_key(db)
    agent = await asyncio.to_thread(read_agent_name, Path(config.workspace)) or DEFAULT_AGENT_NAME
    data = payload(agent, notification_id, title, body, silent)
    urgency = "high" if priority in _URGENT_PRIORITIES else "normal"
    async with _http_session() as session:
        sent = await asyncio.gather(*(
            _send(db, session, row, data, vapid, urgency) for row in subscriptions
        ))
    return any(sent)


def _http_session() -> aiohttp.ClientSession:
    # trust_env: use the proxy of the environment, as the HTTP clients of
    # the other channels do.
    return aiohttp.ClientSession(trust_env=True)


async def _send(
    db: Database, session: aiohttp.ClientSession, row: dict, data: str, vapid: Vapid, urgency: str,
) -> bool:
    info = {"endpoint": row["endpoint"], "keys": {"p256dh": row["p256dh"], "auth": row["auth"]}}
    try:
        await webpush_async(
            info, data,
            vapid_private_key=vapid,
            vapid_claims={"sub": VAPID_SUBJECT},
            ttl=PUSH_TTL_S,
            headers={"Urgency": urgency},
            timeout=aiohttp.ClientTimeout(total=PUSH_TIMEOUT_S),
            aiohttp_session=session,
        )
    except WebPushException as e:
        # The log names the row, never its endpoint: the endpoint is a capability URL.
        if e.status_code in GONE_STATUSES:
            await db.delete_push_subscription(row["id"])
            logger.info("Push subscription %s is gone (%s); deleted it", row["id"], e.status_code)
        else:
            logger.warning("Push to subscription %s failed with status %s", row["id"], e.status_code)
        return False
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
        logger.warning("Push to subscription %s failed: %s", row["id"], type(e).__name__)
        return False
    await db.mark_push_success(row["id"])
    return True
