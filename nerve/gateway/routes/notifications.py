"""Notification routes."""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps

router = APIRouter()


class NotificationAnswerRequest(BaseModel):
    answer: str


class SilenceCreateRequest(BaseModel):
    pattern: str
    reason: str = ""
    ttl_hours: float = 0


@router.get("/api/notifications")
async def list_notifications(
    status: str = "",
    type: str = "",
    session_id: str = "",
    limit: int = 50,
    user: dict = Depends(require_auth),
):
    deps = get_deps()
    notifications = await deps.db.list_notifications(
        status=status or None,
        type=type or None,
        session_id=session_id or None,
        limit=min(limit, 200),
        channel="web",
    )
    pending_count = await deps.db.count_pending_notifications(channel="web")
    return {"notifications": notifications, "pending_count": pending_count}


# --------------------------------------------------------------------- #
#  Silences (deterministic suppression rules)                            #
#                                                                        #
#  Declared BEFORE the ``/{notification_id}`` route so the literal       #
#  ``/silences`` path is not captured as a notification id.              #
# --------------------------------------------------------------------- #


@router.get("/api/notifications/silences")
async def list_silences(
    include_disabled: bool = False,
    user: dict = Depends(require_auth),
):
    deps = get_deps()
    silences = await deps.db.list_silences(include_disabled=include_disabled)
    return {"silences": silences}


@router.post("/api/notifications/silences")
async def create_silence(
    req: SilenceCreateRequest,
    user: dict = Depends(require_auth),
):
    pattern = (req.pattern or "").strip()
    if not pattern:
        raise HTTPException(status_code=400, detail="pattern is required")
    try:
        re.compile(pattern, re.IGNORECASE)
    except re.error as exc:
        raise HTTPException(status_code=400, detail=f"invalid regex: {exc}")

    expires_at = None
    if req.ttl_hours and req.ttl_hours > 0:
        expires_at = (
            datetime.now(timezone.utc) + timedelta(hours=req.ttl_hours)
        ).isoformat()

    deps = get_deps()
    silence_id = f"sil-{uuid.uuid4().hex[:8]}"
    row = await deps.db.create_silence(
        silence_id=silence_id,
        pattern=pattern,
        reason=(req.reason or "").strip(),
        created_by="web",
        expires_at=expires_at,
    )
    if deps.notification_service:
        deps.notification_service.invalidate_silence_cache()
    return row


@router.delete("/api/notifications/silences/{silence_id}")
async def delete_silence(silence_id: str, user: dict = Depends(require_auth)):
    deps = get_deps()
    ok = await deps.db.delete_silence(silence_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Silence not found")
    if deps.notification_service:
        deps.notification_service.invalidate_silence_cache()
    return {"silence_id": silence_id, "deleted": True}


@router.get("/api/notifications/{notification_id}")
async def get_notification(notification_id: str, user: dict = Depends(require_auth)):
    deps = get_deps()
    notif = await deps.db.get_notification(notification_id)
    if not notif:
        raise HTTPException(status_code=404, detail="Notification not found")
    return notif


@router.post("/api/notifications/{notification_id}/answer")
async def answer_notification(
    notification_id: str,
    req: NotificationAnswerRequest,
    user: dict = Depends(require_auth),
):
    deps = get_deps()
    if not deps.notification_service:
        raise HTTPException(status_code=503, detail="Notification service not available")
    success = await deps.notification_service.handle_answer(
        notification_id=notification_id,
        answer=req.answer,
        answered_by="web",
    )
    if not success:
        raise HTTPException(status_code=409, detail="Notification already answered or not found")
    return {"notification_id": notification_id, "answered": True}


@router.post("/api/notifications/{notification_id}/dismiss")
async def dismiss_notification(
    notification_id: str,
    user: dict = Depends(require_auth),
):
    deps = get_deps()
    if not deps.notification_service:
        raise HTTPException(status_code=503, detail="Notification service not available")
    success = await deps.notification_service.handle_dismiss(notification_id)
    if not success:
        raise HTTPException(status_code=409, detail="Notification not pending")
    return {"notification_id": notification_id, "dismissed": True}


@router.post("/api/notifications/dismiss-all")
async def dismiss_all_notifications(user: dict = Depends(require_auth)):
    deps = get_deps()
    count = await deps.db.dismiss_all_notifications()
    return {"dismissed": count}
