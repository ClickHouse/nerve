"""Plan routes."""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from nerve.config import get_config
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps

logger = logging.getLogger(__name__)

router = APIRouter()


class PlanUpdateRequest(BaseModel):
    status: str = ""        # decline
    feedback: str = ""


class PlanReviseRequest(BaseModel):
    feedback: str


class PlanApproveRequest(BaseModel):
    runtime: str = "default"       # "default" | "houseofagents"
    hoa_mode: str = ""             # relay | swarm | pipeline
    hoa_agents: list[str] = []
    hoa_pipeline_id: str = ""


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

    # Write task note on decline
    if req.status == "declined":
        from nerve.agent.tools import task_update as task_update_tool
        feedback_suffix = ""
        if req.feedback:
            feedback_suffix = f" — {req.feedback[:80]}{'...' if len(req.feedback) > 80 else ''}"
        await task_update_tool.handler({
            "task_id": plan["task_id"],
            "note": f"Plan declined: {plan_id}{feedback_suffix}",
        })

    return {"plan_id": plan_id, "updated": True}


@router.post("/api/plans/{plan_id}/revise")
async def revise_plan(plan_id: str, req: PlanReviseRequest, user: dict = Depends(require_auth)):
    """Send revision feedback to the persistent planner session."""
    deps = get_deps()
    plan = await deps.db.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    task = await deps.db.get_task(plan["task_id"])
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    # Store feedback on the plan
    await deps.db.update_plan(plan_id, feedback=req.feedback)

    # Write task note
    from nerve.agent.tools import task_update as task_update_tool
    feedback_summary = req.feedback[:80] + "..." if len(req.feedback) > 80 else req.feedback
    await task_update_tool.handler({
        "task_id": plan["task_id"],
        "note": f"Revision requested for {plan_id}: {feedback_summary}",
    })

    # Send revision request to the persistent planner session
    feedback_prompt = (
        f'Revise plan {plan_id} for task "{task["title"]}" based on this feedback:\n\n'
        f"{req.feedback}\n\n"
        f"Explore the codebase again if needed, then call "
        f'plan_propose(task_id="{plan["task_id"]}", content="...") with the revised plan.'
    )

    session_id = plan.get("session_id") or "cron:task-planner"
    # Ensure the session exists — route feedback to the original proposer
    await deps.engine.sessions.get_or_create(
        session_id, title=f"Cron: {session_id.split(':')[-1]}" if session_id.startswith("cron:") else session_id, source="cron",
    )
    asyncio.create_task(
        deps.engine.run(session_id=session_id, user_message=feedback_prompt, source="cron")
    )
    return {"plan_id": plan_id, "status": "revision_requested"}


@router.post("/api/plans/{plan_id}/approve")
async def approve_plan(
    plan_id: str,
    req: PlanApproveRequest = PlanApproveRequest(),
    user: dict = Depends(require_auth),
):
    """Approve a plan and spawn an implementation session."""
    deps = get_deps()
    plan = await deps.db.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")

    # Guard: only pending plans can be approved
    if plan["status"] != "pending":
        raise HTTPException(
            status_code=409,
            detail=f"Plan is '{plan['status']}', only 'pending' plans can be approved",
        )

    task = await deps.db.get_task(plan["task_id"])
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    now = datetime.now(timezone.utc).isoformat()
    plan_type = plan.get("plan_type", "generic")

    # Mark plan as implementing immediately (prevents double-approve)
    await deps.db.update_plan(plan_id, status="implementing", reviewed_at=now)

    # Create implementation session (visible in Chat UI)
    impl_session_id = f"impl-{str(uuid.uuid4())[:8]}"
    await deps.engine.sessions.get_or_create(
        impl_session_id, title=f"Implement: {task['title']}", source="web",
    )
    await deps.db.update_plan(plan_id, impl_session_id=impl_session_id)

    # Update task status + note
    from nerve.agent.tools import task_update as task_update_tool
    await task_update_tool.handler({
        "task_id": plan["task_id"],
        "status": "in_progress",
        "note": f"Plan approved — implementation started (session: {impl_session_id})",
    })

    # Read task file content for the implementation prompt
    config = get_config()
    task_content = ""
    if task.get("file_path"):
        task_file = config.workspace / task["file_path"]
        if task_file.exists():
            task_content = task_file.read_text(encoding="utf-8")

    # Build implementation prompt — skill-aware
    if plan_type in ("skill-create", "skill-update"):
        prompt = (
            f"You are implementing an approved plan for a skill task.\n\n"
            f"## Task: {task['title']}\n\n"
            f"### Task Content\n{task_content}\n\n"
            f"## Approved Plan\n{plan['content']}\n\n"
            f"## Instructions\n"
        )
        if plan_type == "skill-create":
            prompt += (
                "The plan contains a skill specification. "
                "Use the `skill_create` tool to create the skill. "
                "Extract the name, description, and content from the plan. "
                "If the plan contains a full SKILL.md with frontmatter, parse out the name and description "
                "from the frontmatter and use the body as the content.\n"
            )
        else:
            prompt += (
                "The plan contains a skill revision. "
                "Use the `skill_update` tool to update the existing skill. "
                "Pass the skill ID (directory name) as the name parameter and the full SKILL.md content "
                "(frontmatter + body).\n"
            )
        prompt += (
            "\nAfter the skill is created/updated, mark the task as done using "
            "`task_done` with a note describing what was done.\n"
        )
    else:
        prompt = (
            f"You are implementing an approved plan for a task.\n\n"
            f"## Task: {task['title']}\n\n"
            f"### Task Content\n{task_content}\n\n"
            f"## Approved Plan\n{plan['content']}\n\n"
            f"## Instructions\n"
            f"Follow the plan step by step. You have full tool access.\n"
            f"After implementation, verify your changes work correctly.\n"
            f"If you encounter issues not covered by the plan, use your judgment or ask the user.\n"
        )

    # Augment prompt with houseofagents instructions when selected
    if req.runtime == "houseofagents":
        hoa_instructions = (
            "\n## Execution Runtime: houseofagents (Multi-Agent)\n"
            "Use the `hoa_execute` tool to run the implementation with multi-agent collaboration.\n"
            "This orchestrates multiple AI agents in relay/swarm/pipeline mode for higher quality output.\n\n"
        )
        hoa_mode = req.hoa_mode or "relay"
        hoa_instructions += f"**Mode:** {hoa_mode}\n"
        if req.hoa_agents:
            hoa_instructions += f"**Agents:** {', '.join(req.hoa_agents)}\n"
        if req.hoa_pipeline_id:
            hoa_instructions += f"**Pipeline:** {req.hoa_pipeline_id}\n"
        hoa_instructions += (
            "\nPass the full plan content as the prompt to `hoa_execute`. "
            "After it completes, review the output carefully, verify changes work correctly, "
            "run tests if applicable, commit changes, and mark the task as done.\n"
        )
        prompt += hoa_instructions

    # Spawn implementation in background with error handling
    async def _run_impl():
        try:
            await deps.engine.run(
                session_id=impl_session_id, user_message=prompt, source="web",
            )
        except Exception:
            logger.exception("Implementation session %s failed", impl_session_id)
            try:
                await deps.db.update_plan(plan_id, status="failed")
            except Exception:
                logger.exception("Failed to mark plan %s as failed", plan_id)

    asyncio.create_task(_run_impl())

    return {"plan_id": plan_id, "impl_session_id": impl_session_id}


@router.get("/api/tasks/{task_id}/plans")
async def get_task_plans(task_id: str, user: dict = Depends(require_auth)):
    deps = get_deps()
    plans = await deps.db.get_plans_for_task(task_id)
    return {"plans": plans}
