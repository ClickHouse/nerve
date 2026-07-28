"""Review loop routes — inspect, decide, kill implement→verify loops.

Thin HTTP veneer over :class:`nerve.workflows.review_loop.ReviewLoopService`;
all lifecycle logic lives in the service. Loop creation happens through
POST /api/sessions (``review_loop`` field) or the ``review_loop_start``
MCP tool — a loop is always attached to an observer session.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from nerve.gateway.auth import require_auth

logger = logging.getLogger(__name__)

router = APIRouter()


class ReviewLoopKillRequest(BaseModel):
    reason: str = ""


class ReviewLoopDecisionRequest(BaseModel):
    # accept | abandon | iterate:N | adopt_and_continue | remind_4h
    decision: str


def _service():
    from nerve.workflows import get_review_loop_service

    service = get_review_loop_service()
    if service is None:
        raise HTTPException(status_code=503, detail="review loops disabled")
    return service


@router.get("/api/review-loops")
async def list_review_loops(
    status: str = "",
    limit: int = 50,
    user: dict = Depends(require_auth),
):
    """List loops (newest first). ``status``: 'open', an exact status, or empty."""
    service = _service()
    loops = await service.list_loops(status=status or None, limit=max(1, min(limit, 200)))
    return {"loops": [service.public_loop(lp) for lp in loops]}


@router.get("/api/review-loops/{loop_id}")
async def get_review_loop(loop_id: str, user: dict = Depends(require_auth)):
    """Full loop detail: state, criteria, and the attempt ledger with
    verdicts — everything the loop card renders."""
    service = _service()
    loop = await service.get_loop(loop_id)
    if loop is None:
        raise HTTPException(status_code=404, detail="Review loop not found")
    attempts = await service.list_attempts(loop_id)
    return {"loop": loop, "attempts": attempts}


_STATE_MAX_BYTES = 256 * 1024


@router.get("/api/review-loops/{loop_id}/state")
async def get_review_loop_state(loop_id: str, user: dict = Depends(require_auth)):
    """The implementer's handoff file (STATE.md) — the loop's primary
    artifact. Path is derived server-side from the loop row (never from
    the client), read bounded (tail)."""
    service = _service()
    loop = await service.get_loop(loop_id)
    if loop is None:
        raise HTTPException(status_code=404, detail="Review loop not found")
    path = service.state_file_path(loop)
    exists = False
    truncated = False
    content = ""
    try:
        if path.is_file():
            exists = True
            size = path.stat().st_size
            with path.open("rb") as f:
                if size > _STATE_MAX_BYTES:
                    truncated = True
                    f.seek(size - _STATE_MAX_BYTES)
                content = f.read(_STATE_MAX_BYTES).decode("utf-8", errors="replace")
    except OSError:
        exists = False
        content = ""
    return {
        "exists": exists,
        "truncated": truncated,
        "content": content,
        "path": str(path),
    }


@router.post("/api/review-loops/{loop_id}/kill")
async def kill_review_loop(
    loop_id: str,
    req: ReviewLoopKillRequest = ReviewLoopKillRequest(),
    user: dict = Depends(require_auth),
):
    service = _service()
    from nerve.workflows.review_loop import ReviewLoopError

    try:
        loop = await service.kill_loop(loop_id, reason=req.reason, killed_by="api")
    except ReviewLoopError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return service.public_loop(loop)


@router.post("/api/review-loops/{loop_id}/decision")
async def decide_review_loop(
    loop_id: str,
    req: ReviewLoopDecisionRequest,
    user: dict = Depends(require_auth),
):
    """Apply a decision to a parked loop — the same handler the approval
    card's dispatcher awaits, so the card is never the only path."""
    service = _service()
    result = await service.handle_decision(loop_id, req.decision, decided_by="api")
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result.get("message", "not applied"))
    return result
