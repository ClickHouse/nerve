"""Plan routes."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from nerve.agent.plan_service import (
    PlanNotFound,
    PlanNotPending,
    TaskNotFound,
    approve_plan,
    decline_plan,
    request_plan_revision,
)
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps

logger = logging.getLogger(__name__)

router = APIRouter()


class PlanUpdateRequest(BaseModel):
    status: str = ""        # decline
    feedback: str = ""


class PlanReviseRequest(BaseModel):
    feedback: str


@router.get("/api/plans")
async def list_plans(status: str = "", task_id: str = "", user: dict = Depends(require_auth)):
    deps = get_deps()
    plans = await deps.db.list_plans(
        status=status or None,
        task_id=task_id or None,
    )
    return {"plans": plans}


@router.get("/api/plans/{plan_id}")
async def get_plan(plan_id: str, user: dict = Depends(require_auth)):
    deps = get_deps()
    plan = await deps.db.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    return plan


@router.patch("/api/plans/{plan_id}")
async def update_plan(plan_id: str, req: PlanUpdateRequest, user: dict = Depends(require_auth)):
    deps = get_deps()

    # Decline is the only status transition the UI drives through PATCH.
    # Delegate to the shared service so the WebUI, MCP tool, and Telegram
    # all mark the plan declined and close the task identically.
    if req.status == "declined":
        try:
            await decline_plan(
                db=deps.db, engine=deps.engine,
                plan_id=plan_id, feedback=req.feedback,
            )
        except PlanNotFound:
            raise HTTPException(status_code=404, detail="Plan not found")
        except TaskNotFound:
            raise HTTPException(status_code=404, detail="Task not found")
        except PlanNotPending as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return {"plan_id": plan_id, "updated": True}

    # Generic field update (e.g. feedback-only, or a non-decline status).
    plan = await deps.db.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")

    fields = {}
    if req.status:
        fields["status"] = req.status
        fields["reviewed_at"] = datetime.now(timezone.utc).isoformat()
    if req.feedback:
        fields["feedback"] = req.feedback

    if fields:
        await deps.db.update_plan(plan_id, **fields)

    return {"plan_id": plan_id, "updated": True}


@router.post("/api/plans/{plan_id}/revise")
async def revise_plan(plan_id: str, req: PlanReviseRequest, user: dict = Depends(require_auth)):
    """Send revision feedback to the persistent planner session.

    Thin wrapper around ``request_plan_revision`` — the shared helper
    handles validation, persistence, and dispatch. Errors are mapped to
    HTTP status codes so the UI can surface meaningful messages instead
    of silently dropping non-pending revision attempts.
    """
    if not req.feedback.strip():
        raise HTTPException(status_code=400, detail="Feedback is required")

    deps = get_deps()
    try:
        result = await request_plan_revision(
            db=deps.db,
            engine=deps.engine,
            plan_id=plan_id,
            feedback=req.feedback,
        )
    except PlanNotFound:
        raise HTTPException(status_code=404, detail="Plan not found")
    except TaskNotFound:
        raise HTTPException(status_code=404, detail="Task not found")
    except PlanNotPending as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return result


@router.post("/api/plans/{plan_id}/approve")
async def approve_plan_route(
    plan_id: str,
    user: dict = Depends(require_auth),
):
    """Approve a plan and spawn an implementation session.

    Thin wrapper around the shared ``approve_plan`` service so the WebUI,
    MCP tool, and Telegram all spawn identically-briefed implementation
    sessions. Helper exceptions map to HTTP status codes.
    """
    deps = get_deps()
    try:
        result = await approve_plan(
            db=deps.db, engine=deps.engine, plan_id=plan_id,
        )
    except PlanNotFound:
        raise HTTPException(status_code=404, detail="Plan not found")
    except TaskNotFound:
        raise HTTPException(status_code=404, detail="Task not found")
    except PlanNotPending as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return {
        "plan_id": result["plan_id"],
        "impl_session_id": result["impl_session_id"],
    }


@router.get("/api/tasks/{task_id}/plans")
async def get_task_plans(task_id: str, user: dict = Depends(require_auth)):
    deps = get_deps()
    plans = await deps.db.get_plans_for_task(task_id)
    return {"plans": plans}
