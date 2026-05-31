"""Shared plan-revision dispatch logic.

Both the HTTP route ``/api/plans/{plan_id}/revise`` and the MCP tool
``plan_revise`` need to do the same thing: validate the plan, persist
feedback, write a task note, and dispatch a revision prompt to the
planner session. The prompt instructs the planner to call ``plan_update``
(in-place revision), not ``plan_propose`` — the latter refuses when a
pending plan already exists for the task, which is precisely the
situation here.

Keeping this in one place prevents the two surfaces from drifting
apart again. The HTTP route translates the exceptions raised here into
HTTP status codes; the MCP tool translates them into user-facing text.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nerve.agent.engine import AgentEngine
    from nerve.db import Database

logger = logging.getLogger(__name__)


_FALLBACK_PLANNER_SESSION = "cron:task-planner"


# The prompt template lives here so future tweaks (model selection,
# wording, calling conventions) happen in exactly one place.
_REVISION_PROMPT_TEMPLATE = (
    'Revise plan {plan_id} for task "{task_title}" based on this feedback:\n\n'
    "{feedback}\n\n"
    "Explore the codebase again if needed, then call "
    'plan_update(plan_id="{plan_id}", content="...", feedback="<short summary>") '
    "with the revised plan."
)


class PlanNotFound(Exception):
    """The plan_id does not exist."""


class TaskNotFound(Exception):
    """The plan exists but its task is missing."""


class PlanNotPending(Exception):
    """The plan exists but is not in 'pending' status — cannot be revised."""


async def request_plan_revision(
    db: "Database",
    engine: "AgentEngine",
    plan_id: str,
    feedback: str,
) -> dict:
    """Persist revision feedback and dispatch a revision prompt to the planner.

    Args:
        db: Database instance (used for plan/task lookups + updates).
        engine: AgentEngine instance (used to ensure session + dispatch run).
        plan_id: The pending plan to revise.
        feedback: Free-text feedback explaining what should change.

    Returns:
        ``{"plan_id", "task_id", "session_id", "status"}`` on success.

    Raises:
        PlanNotFound: No plan with that ID.
        TaskNotFound: The plan's task no longer exists.
        PlanNotPending: The plan is declined/superseded/implementing/etc.

    Behavior contract:
        - Stores ``feedback`` on the plan (status untouched — old plan
          stays ``pending`` until the planner calls ``plan_update``).
        - Writes a ``Revision requested for {plan_id}`` note to the task.
        - Dispatches ``engine.run()`` on the original proposer session
          (``plan.session_id``), falling back to ``cron:task-planner``
          if the proposer is unknown/deleted.
        - The planner is instructed to call
          ``plan_update(plan_id=..., content=..., feedback=<summary>)``
          so version history stays linked.
    """
    # Import here to avoid a circular import: tool handlers import
    # plan_service indirectly via the engine wiring.
    from dataclasses import replace
    from nerve.agent.tools import _legacy_ctx
    from nerve.agent.tools.handlers.tasks import task_update_handler

    feedback = feedback.strip()
    if not feedback:
        # Treat empty feedback as a programmer error — both callers
        # validate this upstream, but defend the helper anyway.
        raise ValueError("Feedback is required for revision requests.")

    plan = await db.get_plan(plan_id)
    if not plan:
        raise PlanNotFound(f"Plan not found: {plan_id}")

    if plan["status"] != "pending":
        raise PlanNotPending(
            f"Plan is '{plan['status']}' — only pending plans can be revised."
        )

    task = await db.get_task(plan["task_id"])
    if not task:
        raise TaskNotFound(f"Task not found for plan {plan_id}: {plan['task_id']}")

    # 1. Store feedback on the plan (status stays pending until planner
    #    supersedes it via plan_update).
    await db.update_plan(plan_id, feedback=feedback)

    # 2. Write a task note so the revision request is visible in the
    #    task's history. Build a ``ToolContext`` from the legacy module
    #    globals (which ``engine.initialize`` / ``init_tools`` set up)
    #    and override the db + engine fields with the ones explicitly
    #    handed to this helper — tests pass a FakeEngine that doesn't
    #    expose ``.config``, so we can't read workspace off ``engine``
    #    directly.
    feedback_summary = feedback[:80] + "..." if len(feedback) > 80 else feedback
    task_ctx = replace(_legacy_ctx("system"), db=db, engine=engine)
    await task_update_handler(task_ctx, {
        "task_id": plan["task_id"],
        "note": f"Revision requested for {plan_id}: {feedback_summary}",
    })

    # 3. Build the revision prompt from the shared template.
    prompt = _REVISION_PROMPT_TEMPLATE.format(
        plan_id=plan_id,
        task_title=task["title"],
        feedback=feedback,
    )

    # 4. Route the prompt back to the original proposer session.
    #    Fall back to the cron planner if the plan was created without
    #    a session attribution (older rows, manual inserts, etc.).
    session_id = plan.get("session_id") or _FALLBACK_PLANNER_SESSION
    session_title = (
        f"Cron: {session_id.split(':')[-1]}"
        if session_id.startswith("cron:")
        else session_id
    )
    await engine.sessions.get_or_create(
        session_id, title=session_title, source="cron",
    )
    asyncio.create_task(
        engine.run(session_id=session_id, user_message=prompt, source="cron")
    )

    logger.info(
        "Revision dispatched: plan=%s task=%s session=%s",
        plan_id, plan["task_id"], session_id,
    )

    return {
        "plan_id": plan_id,
        "task_id": plan["task_id"],
        "session_id": session_id,
        "status": "revision_requested",
    }
