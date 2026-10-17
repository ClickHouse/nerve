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
from typing import TYPE_CHECKING, Awaitable

from nerve.identity import attribution_for

if TYPE_CHECKING:
    from nerve.agent.engine import AgentEngine
    from nerve.db import Database
    from nerve.identity import ActorOrId

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
    *,
    actor: "ActorOrId | None",
    cause: "ActorOrId | None",
) -> dict:
    """Persist revision feedback and dispatch a revision prompt to the planner.

    Args:
        db: Database instance (used for plan/task lookups + updates).
        engine: AgentEngine instance (used to ensure session + dispatch run).
        plan_id: The pending plan to revise.
        feedback: Free-text feedback explaining what should change.
        actor: Who asks for the revision: the person for the HTTP route,
            the system actor for the tool. Recorded with ``cause`` as the
            plan's reviewer.
        cause: On whose behalf the revision is asked for: the person for
            the HTTP route, the calling session's cause for the tool. It is
            the reviewer's cause, and the cause of the planner session if
            this call creates it; an existing planner session keeps its own
            cause.

    Returns:
        ``{"plan_id", "task_id", "session_id", "status"}`` on success.

    Raises:
        PlanNotFound: No plan with that ID.
        TaskNotFound: The plan's task no longer exists.
        PlanNotPending: The plan is declined/superseded/implementing/etc.

    Behavior contract:
        - Stores ``feedback`` on the plan, ``actor`` + ``cause`` as its
          reviewer, and moves it from ``pending`` to ``revising`` in one
          compare-and-update. While it is ``revising``, approve, decline
          and another revision refuse. The planner's ``plan_update``
          supersedes it; if the revision turn ends without that, the plan
          is ``pending`` again.
        - Writes a ``Revision requested for {plan_id}`` note to the task.
        - Dispatches ``engine.run()`` on the original proposer session
          (``plan.session_id``), falling back to ``cron:task-planner``
          if the proposer is unknown/deleted.
        - The planner is instructed to call
          ``plan_update(plan_id=..., content=..., feedback=<summary>)``
          so version history stays linked.
    """
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

    # 1. Store feedback on the plan and move it to ``revising``. The request
    #    is a review decision: it applies only while the plan is pending, and
    #    while the plan is ``revising`` no approval, decline or second
    #    revision applies. The planner's plan_update supersedes it; the end
    #    of the revision turn makes a plan that it did not replace pending
    #    again.
    if not await db.compare_and_update_plan(
        plan_id, expect_status="pending", status="revising",
        feedback=feedback, attribution=attribution_for(actor, cause),
    ):
        raise PlanNotPending(
            f"Plan {plan_id} is no longer pending; another decision was made first."
        )
    try:
        session_id, prompt = await _prepare_revision(
            db, engine, plan, task, feedback, cause,
        )
    except BaseException:
        await _reopen_after_revision(db, plan_id)
        raise
    asyncio.create_task(
        _run_revision_turn(db, engine, plan_id, session_id, prompt, cause),
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


async def _prepare_revision(
    db: "Database",
    engine: "AgentEngine",
    plan: dict,
    task: dict,
    feedback: str,
    cause: "ActorOrId | None",
) -> tuple[str, str]:
    """Write the task note and return the planner session and the prompt."""
    # Import here to avoid a circular import: tool handlers import
    # plan_service indirectly via the engine wiring.
    from dataclasses import replace
    from nerve.agent.tools import _legacy_ctx
    from nerve.agent.tools.handlers.tasks import task_update_handler

    plan_id = plan["id"]

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
    # A planner session that does not exist yet is created by the system
    # actor for the person who asked for the revision.
    await engine.sessions.get_or_create(
        session_id, title=session_title, source="cron",
        actor=engine.db.system_actor, cause=cause,
    )
    return session_id, prompt


async def _run_revision_turn(
    db: "Database",
    engine: "AgentEngine",
    plan_id: str,
    session_id: str,
    prompt: str,
    cause: "ActorOrId | None",
) -> None:
    """Run the revision turn, then reopen the plan if it was not replaced."""
    try:
        await engine.run(
            session_id=session_id, user_message=prompt, source="cron",
            # The revision prompt is built from a template here, not typed by
            # the person who asked for the revision.
            actor=engine.db.system_actor,
        )
    except Exception:
        logger.exception("Revision turn for plan %s failed", plan_id)
    finally:
        await _reopen_after_revision(db, plan_id)


def _resume_enrolled_sessions() -> set[str]:
    """The session IDs that resume-after-restart will continue at startup.

    Reads the queue file without consuming it; the engine reads and deletes
    it when it resumes the sessions.
    """
    from nerve.agent import engine as engine_module

    try:
        raw = engine_module.RESUME_QUEUE_FILE.read_text()
    except OSError:
        return set()
    return {line.strip() for line in raw.splitlines() if line.strip()}


async def recover_revising_plans(db: "Database") -> list[str]:
    """At startup, reopen the plans whose revision turn no longer exists.

    Call it before the engine resumes sessions. A plan is ``revising`` only
    while its revision turn runs. A stop ends that turn, unless
    resume-after-restart continues its session: such a plan stays
    ``revising`` and :func:`finish_revisions_after_resume` reopens it
    when the resume ends, if it is still ``revising`` then. Every other
    ``revising`` plan is ``pending`` again.

    Returns the IDs of the plans that stay ``revising``.
    """
    resumed = _resume_enrolled_sessions()
    held: list[str] = []
    for plan in await db.list_plans(status="revising", limit=1_000_000):
        if (plan.get("session_id") or _FALLBACK_PLANNER_SESSION) in resumed:
            held.append(plan["id"])
        else:
            await _reopen_after_revision(db, plan["id"])
    return held


async def finish_revisions_after_resume(
    resume: "Awaitable[int]", db: "Database", held_plan_ids: list[str],
) -> int:
    """Await the resume of the enrolled sessions, then reopen the held plans.

    ``resume`` is ``engine.resume_enrolled_sessions()``; ``held_plan_ids``
    comes from :func:`recover_revising_plans`. The resume ends after every
    resumed turn ends, so a held plan that is still ``revising`` then got no
    new version and is ``pending`` again.
    """
    try:
        return await resume
    finally:
        for plan_id in held_plan_ids:
            await _reopen_after_revision(db, plan_id)


async def _reopen_after_revision(db: "Database", plan_id: str) -> None:
    """Make a plan that is still ``revising`` pending, so it can be decided."""
    try:
        if await db.compare_and_update_plan(
            plan_id, expect_status="revising", status="pending",
        ):
            logger.info(
                "Plan %s is pending again: the revision ended without a new version",
                plan_id,
            )
    except Exception:
        logger.exception("Could not reopen plan %s after its revision", plan_id)
