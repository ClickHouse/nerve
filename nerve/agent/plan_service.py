"""Shared plan review actions (approve / decline / revise).

Every plan decision has several surfaces that must behave identically:
the HTTP routes under ``/api/plans/*`` (WebUI), the MCP ``plan_*`` tools
(agents), and the Telegram ``/plans`` command (chat). Rather than let the
approve/decline/revise logic drift apart across three copies, it lives
here once and each surface is a thin adapter:

- ``approve_plan`` — mark implementing, spawn an implementation session,
  flip the task to in_progress, and dispatch the build prompt.
- ``decline_plan`` — mark declined and close the task as done.
- ``request_plan_revision`` — persist feedback and dispatch a
  ``plan_update`` prompt to the original proposer session (not
  ``plan_propose``, which refuses when a pending plan already exists).

Each surface translates the exceptions raised here into its own idiom:
HTTP → status codes, MCP/Telegram → user-facing text.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
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


def _build_impl_prompt(
    task: dict, task_content: str, plan_type: str, plan_content: str,
) -> str:
    """Build the implementation prompt handed to a freshly-spawned impl session.

    ``skill-create`` / ``skill-update`` tasks get tool-specific instructions
    (call ``skill_create`` / ``skill_update``); everything else gets the
    generic "follow the plan step by step" prompt. Kept here so the WebUI,
    MCP, and Telegram approve paths all spawn identically-briefed sessions.
    """
    if plan_type in ("skill-create", "skill-update"):
        prompt = (
            f"You are implementing an approved plan for a skill task.\n\n"
            f"## Task: {task['title']}\n\n"
            f"### Task Content\n{task_content}\n\n"
            f"## Approved Plan\n{plan_content}\n\n"
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
        return prompt

    return (
        f"You are implementing an approved plan for a task.\n\n"
        f"## Task: {task['title']}\n\n"
        f"### Task Content\n{task_content}\n\n"
        f"## Approved Plan\n{plan_content}\n\n"
        f"## Instructions\n"
        f"Follow the plan step by step. You have full tool access.\n"
        f"After implementation, verify your changes work correctly.\n"
        f"If you encounter issues not covered by the plan, use your judgment or ask the user.\n"
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


async def approve_plan(
    db: "Database",
    engine: "AgentEngine",
    plan_id: str,
) -> dict:
    """Approve a pending plan and spawn its implementation session.

    Args:
        db: Database instance (plan/task lookups + updates).
        engine: AgentEngine (session creation + run dispatch).
        plan_id: The pending plan to approve.

    Returns:
        ``{"plan_id", "task_id", "impl_session_id"}`` on success.

    Raises:
        PlanNotFound: No plan with that ID.
        PlanNotPending: The plan is not ``pending`` (guards double-approve).
        TaskNotFound: The plan's task no longer exists.

    Behavior contract:
        - Flips the plan to ``implementing`` up front so a concurrent
          approve can't spawn a second session.
        - Creates ``impl-<uuid>`` and stores it on the plan.
        - Moves the task to ``in_progress`` with an audit note.
        - Dispatches ``engine.run()`` in the background with the build
          prompt; registers the task with the engine (when supported) so
          ``/stop`` can cancel a stuck implementation.
    """
    from dataclasses import replace
    from nerve.agent.tools import _legacy_ctx
    from nerve.agent.tools.handlers.tasks import task_update_handler

    plan = await db.get_plan(plan_id)
    if not plan:
        raise PlanNotFound(f"Plan not found: {plan_id}")

    if plan["status"] != "pending":
        raise PlanNotPending(
            f"Plan is '{plan['status']}' — only pending plans can be approved."
        )

    task = await db.get_task(plan["task_id"])
    if not task:
        raise TaskNotFound(f"Task not found for plan {plan_id}: {plan['task_id']}")

    now = datetime.now(timezone.utc).isoformat()
    plan_type = plan.get("plan_type", "generic")

    # Mark implementing immediately (prevents a double-approve race).
    await db.update_plan(plan_id, status="implementing", reviewed_at=now)

    impl_session_id = f"impl-{str(uuid.uuid4())[:8]}"
    await engine.sessions.get_or_create(
        impl_session_id, title=f"Implement: {task['title']}", source="web",
    )
    await db.update_plan(plan_id, impl_session_id=impl_session_id)

    # Move the task to in_progress with an audit note. Uses the legacy
    # ToolContext (db/engine overridden with the handed-in instances) — the
    # same pattern request_plan_revision relies on, so tests with a
    # config-less FakeEngine keep working.
    task_ctx = replace(_legacy_ctx("system"), db=db, engine=engine)
    await task_update_handler(task_ctx, {
        "task_id": plan["task_id"],
        "status": "in_progress",
        "note": f"Plan approved — implementation started (session: {impl_session_id})",
    })

    # Read the task file for the implementation prompt. Resolve against the
    # legacy ToolContext workspace (init_tools sets it to config.workspace),
    # the same field request_plan_revision relies on — best-effort.
    task_content = ""
    workspace = getattr(task_ctx, "workspace", None)
    if task.get("file_path") and workspace:
        task_file = workspace / task["file_path"]
        if task_file.exists():
            task_content = await asyncio.to_thread(
                task_file.read_text, encoding="utf-8",
            )

    prompt = _build_impl_prompt(task, task_content, plan_type, plan["content"])

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

    impl_task = asyncio.create_task(_run_impl())
    # Register with the engine so a manual /stop can cancel a stuck impl
    # session. FakeEngine (tests) has no register_task — guard for it.
    register = getattr(engine, "register_task", None)
    if register:
        register(impl_session_id, impl_task)

    logger.info(
        "Plan approved: plan=%s task=%s impl=%s",
        plan_id, plan["task_id"], impl_session_id,
    )

    return {
        "plan_id": plan_id,
        "task_id": plan["task_id"],
        "impl_session_id": impl_session_id,
    }


async def decline_plan(
    db: "Database",
    engine: "AgentEngine",
    plan_id: str,
    feedback: str = "",
) -> dict:
    """Decline a pending plan and close its task as done.

    Args:
        db: Database instance.
        engine: AgentEngine (only used to build the task-handler context).
        plan_id: The pending plan to decline.
        feedback: Optional free-text reason, recorded on plan + task note.

    Returns:
        ``{"plan_id", "task_id", "status": "declined", "feedback"}``.

    Raises:
        PlanNotFound: No plan with that ID.
        PlanNotPending: The plan is not ``pending``.
        TaskNotFound: The plan's task no longer exists.
    """
    from dataclasses import replace
    from nerve.agent.tools import _legacy_ctx
    from nerve.agent.tools.handlers.tasks import task_done_handler

    feedback = (feedback or "").strip()

    plan = await db.get_plan(plan_id)
    if not plan:
        raise PlanNotFound(f"Plan not found: {plan_id}")

    if plan["status"] != "pending":
        raise PlanNotPending(
            f"Plan is '{plan['status']}' — only pending plans can be declined."
        )

    task = await db.get_task(plan["task_id"])
    if not task:
        raise TaskNotFound(f"Task not found for plan {plan_id}: {plan['task_id']}")

    now = datetime.now(timezone.utc).isoformat()
    fields: dict = {"status": "declined", "reviewed_at": now}
    if feedback:
        fields["feedback"] = feedback
    await db.update_plan(plan_id, **fields)

    if feedback:
        note = f"Plan {plan_id} declined — {feedback}"
    else:
        note = f"Related plan {plan_id} was closed without a specified reason"
    task_ctx = replace(_legacy_ctx("system"), db=db, engine=engine)
    await task_done_handler(task_ctx, {
        "task_id": plan["task_id"],
        "note": note,
    })

    logger.info(
        "Plan declined: plan=%s task=%s", plan_id, plan["task_id"],
    )

    return {
        "plan_id": plan_id,
        "task_id": plan["task_id"],
        "status": "declined",
        "feedback": feedback,
    }
