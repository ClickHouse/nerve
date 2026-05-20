"""Plan tool handlers — plan_propose, plan_update, plan_list, plan_read,
plan_approve, plan_decline, plan_revise.

``plan_propose`` and ``plan_update`` attribute the change to the current
session (via ``ctx.session_id``), which is how plan_service.py routes
revisions back to the proposer. Cross-domain calls into task handlers
are direct function imports (per design decision); no registry round-trip.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    PLAN_APPROVE_SCHEMA,
    PLAN_DECLINE_SCHEMA,
    PLAN_LIST_SCHEMA,
    PLAN_PROPOSE_SCHEMA,
    PLAN_READ_SCHEMA,
    PLAN_REVISE_SCHEMA,
    PLAN_UPDATE_SCHEMA,
)

# Direct cross-domain imports — handlers in this file can call task
# handlers without going through the registry. This is the chosen pattern
# for intra-package coupling (see plan-a217db3c "Cross-handler calls").
from nerve.agent.tools.handlers.tasks import (
    task_done_handler,
    task_update_handler,
)

logger = logging.getLogger(__name__)


async def plan_propose_handler(ctx: ToolContext, args: dict) -> ToolResult:
    task_id = args["task_id"]
    content = args["content"]
    plan_type = (args.get("plan_type", "") or "").strip()

    if not ctx.db:
        return ToolResult.text("Database not available.")

    task = await ctx.db.get_task(task_id)
    if not task:
        return ToolResult.text(f"Task not found: {task_id}")

    # Auto-detect plan_type from task source if not explicitly provided
    if not plan_type:
        task_source = task.get("source", "")
        if task_source == "skill-extractor":
            plan_type = "skill-create"
        elif task_source == "skill-reviser":
            plan_type = "skill-update"
        else:
            plan_type = "generic"

    existing = await ctx.db.get_pending_plan_task_ids()
    if task_id in existing:
        return ToolResult.text(
            f"Task {task_id} already has a pending or implementing plan. Skip it."
        )

    existing_plans = await ctx.db.get_plans_for_task(task_id)
    version = max((p.get("version", 0) for p in existing_plans), default=0) + 1

    # Supersede prior pending plan if any
    for p in existing_plans:
        if p.get("status") == "pending":
            await ctx.db.update_plan(p["id"], status="superseded")

    plan_id = f"plan-{str(uuid.uuid4())[:8]}"

    await ctx.db.create_plan(
        plan_id=plan_id,
        task_id=task_id,
        content=content,
        session_id=ctx.session_id,  # Attribution: which agent proposed this plan
        model="",
        version=version,
        plan_type=plan_type,
    )

    # Note on the task
    await task_update_handler(ctx, {
        "task_id": task_id,
        "note": f"Plan proposed: {plan_id} (v{version})",
    })

    return ToolResult.text(
        f"Plan proposed: {plan_id} (v{version}) for task '{task['title']}'. "
        "Awaiting human review."
    )


async def plan_update_handler(ctx: ToolContext, args: dict) -> ToolResult:
    """Supersede a pending plan with a new version.

    Creates a new plan record (v+1) linked to the old one via
    ``parent_plan_id``, marks the old plan ``superseded``, and optionally
    stores feedback on the old plan explaining why it was replaced. Used
    instead of the decline→propose dance when refining your own plan.
    """
    plan_id = args["plan_id"]
    content = args["content"]
    feedback = (args.get("feedback", "") or "").strip()

    if not ctx.db:
        return ToolResult.text("Database not available.")

    old_plan = await ctx.db.get_plan(plan_id)
    if not old_plan:
        return ToolResult.text(f"Plan not found: {plan_id}")

    if old_plan["status"] != "pending":
        return ToolResult.text(
            f"Plan is '{old_plan['status']}' — only pending plans can be updated."
        )

    update_fields: dict = {"status": "superseded"}
    if feedback:
        update_fields["feedback"] = feedback
    await ctx.db.update_plan(plan_id, **update_fields)

    new_plan_id = f"plan-{str(uuid.uuid4())[:8]}"
    new_version = int(old_plan.get("version", 1)) + 1
    await ctx.db.create_plan(
        plan_id=new_plan_id,
        task_id=old_plan["task_id"],
        content=content,
        session_id=ctx.session_id,
        model="",
        version=new_version,
        parent_plan_id=plan_id,
        plan_type=old_plan.get("plan_type", "generic"),
    )

    note = f"Plan updated: {plan_id} → {new_plan_id} (v{new_version})"
    if feedback:
        note += f" — {feedback}"
    await task_update_handler(ctx, {
        "task_id": old_plan["task_id"],
        "note": note,
    })

    return ToolResult.text(
        f"Plan {plan_id} superseded by {new_plan_id} (v{new_version}). "
        "The new version is pending review."
    )


async def plan_list_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if not ctx.db:
        return ToolResult.text("Database not available.")

    raw_status = (args.get("status", "") or "").strip().lower()

    if raw_status:
        plans = await ctx.db.list_plans(status=raw_status)
    else:
        pending = await ctx.db.list_plans(status="pending")
        implementing = await ctx.db.list_plans(status="implementing")
        plans = pending + implementing

    if not plans:
        return ToolResult.text("No plans found.")

    lines = []
    for p in plans:
        task_title = p.get("task_title", p.get("task_id", "?"))
        lines.append(
            f"- [{p['status']}] {task_title} — plan {p['id']} v{p['version']} ({p['created_at'][:10]})"
        )

    return ToolResult.text(f"Found {len(plans)} plan(s):\n" + "\n".join(lines))


async def plan_read_handler(ctx: ToolContext, args: dict) -> ToolResult:
    plan_id = args["plan_id"]

    if not ctx.db:
        return ToolResult.text("Database not available.")

    plan = await ctx.db.get_plan(plan_id)
    if not plan:
        return ToolResult.text(f"Plan not found: {plan_id}")

    task_title = plan.get("task_title", plan.get("task_id", "?"))
    header = (
        f"**Plan {plan['id']}** v{plan['version']} [{plan['status']}]\n"
        f"Task: {task_title} ({plan['task_id']})\n"
        f"Type: {plan.get('plan_type', 'generic')} | Created: {plan['created_at'][:10]}"
    )
    if plan.get("feedback"):
        header += f"\nFeedback: {plan['feedback']}"
    if plan.get("impl_session_id"):
        header += f"\nImpl session: {plan['impl_session_id']}"

    return ToolResult.text(f"{header}\n\n---\n\n{plan['content']}")


async def plan_approve_handler(ctx: ToolContext, args: dict) -> ToolResult:
    plan_id = args["plan_id"]

    if not ctx.db:
        return ToolResult.text("Database not available.")
    if not ctx.engine:
        return ToolResult.text("Engine not available — cannot spawn implementation session.")

    plan = await ctx.db.get_plan(plan_id)
    if not plan:
        return ToolResult.text(f"Plan not found: {plan_id}")

    if plan["status"] != "pending":
        return ToolResult.text(
            f"Plan is '{plan['status']}' — only pending plans can be approved."
        )

    task = await ctx.db.get_task(plan["task_id"])
    if not task:
        return ToolResult.text(f"Task not found: {plan['task_id']}")

    now = datetime.now(timezone.utc).isoformat()
    plan_type = plan.get("plan_type", "generic")

    # Mark as implementing (prevents double-approve)
    await ctx.db.update_plan(plan_id, status="implementing", reviewed_at=now)

    impl_session_id = f"impl-{str(uuid.uuid4())[:8]}"
    await ctx.engine.sessions.get_or_create(
        impl_session_id, title=f"Implement: {task['title']}", source="web",
    )
    await ctx.db.update_plan(plan_id, impl_session_id=impl_session_id)

    # Update task status — cross-domain call into tasks handler
    await task_update_handler(ctx, {
        "task_id": plan["task_id"],
        "status": "in_progress",
        "note": f"Plan approved — implementation started (session: {impl_session_id})",
    })

    task_content = ""
    if task.get("file_path") and ctx.config:
        task_file = ctx.config.workspace / task["file_path"]
        if task_file.exists():
            task_content = task_file.read_text(encoding="utf-8")

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

    engine = ctx.engine
    db = ctx.db

    async def _run_impl():
        try:
            await engine.run(
                session_id=impl_session_id, user_message=prompt, source="web",
            )
        except Exception:
            logger.exception("Implementation session %s failed", impl_session_id)
            try:
                await db.update_plan(plan_id, status="failed")
            except Exception:
                logger.exception("Failed to mark plan %s as failed", plan_id)

    asyncio.create_task(_run_impl())

    return ToolResult.text(
        f"Plan {plan_id} approved. Implementation session started: {impl_session_id}"
    )


async def plan_decline_handler(ctx: ToolContext, args: dict) -> ToolResult:
    plan_id = args["plan_id"]
    feedback = (args.get("feedback", "") or "").strip()

    if not ctx.db:
        return ToolResult.text("Database not available.")

    plan = await ctx.db.get_plan(plan_id)
    if not plan:
        return ToolResult.text(f"Plan not found: {plan_id}")

    if plan["status"] != "pending":
        return ToolResult.text(
            f"Plan is '{plan['status']}' — only pending plans can be declined."
        )

    now = datetime.now(timezone.utc).isoformat()
    fields: dict = {"status": "declined", "reviewed_at": now}
    if feedback:
        fields["feedback"] = feedback
    await ctx.db.update_plan(plan_id, **fields)

    if feedback:
        note = f"Plan {plan_id} declined — {feedback}"
    else:
        note = f"Related plan {plan_id} was closed without a specified reason"
    await task_done_handler(ctx, {
        "task_id": plan["task_id"],
        "note": note,
    })

    return ToolResult.text(
        f"Plan {plan_id} declined and task moved to done."
        + (f" Feedback: {feedback}" if feedback else "")
    )


async def plan_revise_handler(ctx: ToolContext, args: dict) -> ToolResult:
    """Wrapper around the shared ``plan_service.request_plan_revision`` helper.

    The helper holds the actual validation + dispatch logic shared between
    the MCP tool surface and the HTTP route. Exceptions are mapped here to
    user-facing text; the HTTP layer maps them to status codes.
    """
    from nerve.agent.plan_service import (
        PlanNotFound,
        PlanNotPending,
        TaskNotFound,
        request_plan_revision,
    )

    plan_id = args["plan_id"]
    feedback = (args.get("feedback", "") or "").strip()

    if not ctx.db:
        return ToolResult.text("Database not available.")
    if not ctx.engine:
        return ToolResult.text("Engine not available — cannot send revision to planner.")
    if not feedback:
        return ToolResult.text("Feedback is required for revision requests.")

    try:
        result = await request_plan_revision(
            db=ctx.db, engine=ctx.engine, plan_id=plan_id, feedback=feedback,
        )
    except PlanNotFound:
        return ToolResult.text(f"Plan not found: {plan_id}")
    except TaskNotFound:
        return ToolResult.text("Task not found for this plan.")
    except PlanNotPending as exc:
        return ToolResult.text(str(exc))

    return ToolResult.text(
        f"Revision requested for {result['plan_id']}. "
        f"Feedback sent to planner session ({result['session_id']})."
    )


PLAN_PROPOSE_SPEC = ToolSpec(
    name="plan_propose",
    description="Propose an implementation plan for a task. The plan will be reviewed and approved by the user asynchronously — it is NOT executed immediately. Use this when you have analyzed a task and want to suggest how to implement it.",
    input_schema=PLAN_PROPOSE_SCHEMA,
    handler=plan_propose_handler,
)

PLAN_UPDATE_SPEC = ToolSpec(
    name="plan_update",
    description="Update a pending plan with revised content. Creates a new version (v+1), marks the old version as superseded, and links them via parent_plan_id. Prefer this over plan_decline + plan_propose when you're refining your own plan based on feedback — the task stays open and the version history is preserved.",
    input_schema=PLAN_UPDATE_SCHEMA,
    handler=plan_update_handler,
)

PLAN_LIST_SPEC = ToolSpec(
    name="plan_list",
    description="List existing plans. Use this to check which tasks already have pending plans before proposing new ones.",
    input_schema=PLAN_LIST_SCHEMA,
    handler=plan_list_handler,
)

PLAN_READ_SPEC = ToolSpec(
    name="plan_read",
    description="Read the full content of a plan. Use this to review a plan's details before approving, declining, or revising it.",
    input_schema=PLAN_READ_SCHEMA,
    handler=plan_read_handler,
)

PLAN_APPROVE_SPEC = ToolSpec(
    name="plan_approve",
    description="Approve a pending plan and spawn an implementation session. Use when the user approves a proposed plan.",
    input_schema=PLAN_APPROVE_SCHEMA,
    handler=plan_approve_handler,
)

PLAN_DECLINE_SPEC = ToolSpec(
    name="plan_decline",
    description="Decline a pending plan. Optionally provide feedback explaining why.",
    input_schema=PLAN_DECLINE_SCHEMA,
    handler=plan_decline_handler,
)

PLAN_REVISE_SPEC = ToolSpec(
    name="plan_revise",
    description="Request revision of a pending plan. Sends feedback to the planner session which will propose a new version.",
    input_schema=PLAN_REVISE_SCHEMA,
    handler=plan_revise_handler,
)


PLAN_SPECS = [
    PLAN_PROPOSE_SPEC,
    PLAN_UPDATE_SPEC,
    PLAN_LIST_SPEC,
    PLAN_READ_SPEC,
    PLAN_APPROVE_SPEC,
    PLAN_DECLINE_SPEC,
    PLAN_REVISE_SPEC,
]
