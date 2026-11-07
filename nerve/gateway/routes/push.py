"""The panel as an installable app, and its Web Push subscriptions.

``GET /api/manifest.webmanifest`` names the app after the agent. It needs no
sign-in, because the browser fetches it without the panel's token. The
subscription routes let a person turn push on in a browser, and see and
remove the browsers of that person. Delivery is in
:mod:`nerve.notifications.webpush`.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from nerve.config import NOTIFY_LEVEL_IMPORTANT, get_config
from nerve.db.push_subscriptions import MAX_PUSH_SUBSCRIPTIONS
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps
from nerve.identity import Actor
from nerve.notifications import webpush
from nerve.onboarding import read_agent_name

router = APIRouter()

MANIFEST_MEDIA_TYPE = "application/manifest+json"
#: The background of the dark theme: the start screen of the installed app.
APP_BACKGROUND = "#1f1f1c"
#: Nerve's icons in the web bundle (``web/public``): the 192 and 512 px PNGs
#: that installing needs.
NERVE_ICONS = (
    {"src": "/nerve-icon-192.png", "sizes": "192x192", "type": "image/png"},
    {"src": "/nerve-icon-512.png", "sizes": "512x512", "type": "image/png"},
    {"src": "/nerve-icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
)
USER_AGENT_CHARS = 300

_PERSON_ONLY = "Push notifications are for a person, not for a system or MCP token."
_TOO_MANY = f"Push is on in {MAX_PUSH_SUBSCRIPTIONS} browsers already. Remove one first."


@router.get("/api/manifest.webmanifest", include_in_schema=False)
async def manifest():
    workspace = Path(get_config().workspace)
    name = await asyncio.to_thread(read_agent_name, workspace) or webpush.DEFAULT_AGENT_NAME
    body = {
        "id": "/",
        "name": name,
        "short_name": name,
        "start_url": "/",
        "scope": "/",
        "display": "standalone",
        "background_color": APP_BACKGROUND,
        "theme_color": APP_BACKGROUND,
        "icons": NERVE_ICONS,
    }
    return JSONResponse(body, media_type=MANIFEST_MEDIA_TYPE, headers={"Cache-Control": "no-cache"})


async def _require_person(actor: Actor = Depends(require_auth)) -> Actor:
    if not actor.is_human:
        raise HTTPException(status_code=403, detail=_PERSON_ONLY)
    return actor


class PushKeys(BaseModel):
    p256dh: str
    auth: str


class PushSubscriptionBody(BaseModel):
    """What ``PushSubscription.toJSON()`` gives in the browser, and the level of its alerts."""

    endpoint: str
    keys: PushKeys
    level: Literal["all", "important"] = NOTIFY_LEVEL_IMPORTANT


def _device(row: dict) -> dict:
    return {key: row[key] for key in ("id", "endpoint", "level", "user_agent", "created_at", "last_success_at")}


@router.get("/api/push/subscriptions")
async def list_subscriptions(actor: Actor = Depends(_require_person)):
    """The VAPID public key, and the browsers of this person."""
    db = get_deps().db
    return {
        "public_key": await webpush.public_key(db),
        "subscriptions": [_device(row) for row in await db.list_push_subscriptions(actor.actor_id)],
    }


@router.post("/api/push/subscriptions")
async def save_subscription(
    body: PushSubscriptionBody, request: Request, actor: Actor = Depends(_require_person),
):
    """Turn push on for this browser, or change its keys or level."""
    try:
        endpoint = webpush.check_endpoint(body.endpoint)
        p256dh = webpush.check_p256dh(body.keys.p256dh)
        auth = webpush.check_key(body.keys.auth, webpush.AUTH_BYTES)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from None
    row = await get_deps().db.save_push_subscription(
        actor_id=actor.actor_id, endpoint=endpoint, p256dh=p256dh, auth=auth, level=body.level,
        user_agent=request.headers.get("user-agent", "")[:USER_AGENT_CHARS],
    )
    if row is None:
        raise HTTPException(status_code=409, detail=_TOO_MANY)
    return _device(row)


@router.delete("/api/push/subscriptions/{subscription_id}")
async def delete_subscription(subscription_id: int, actor: Actor = Depends(_require_person)):
    """Turn push off for one browser of this person."""
    if not await get_deps().db.delete_push_subscription(subscription_id, actor.actor_id):
        raise HTTPException(status_code=404, detail="This browser does not get push notifications.")
    return {"deleted": True}
