"""Shared plan-revision dispatch and restart-recovery logic.

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

Plans do not survive a daemon restart: ``status='implementing'`` asserts
an in-process implementation obligation, but that obligation is a bare
``asyncio`` task, so a SIGKILL/OOM/restart destroys it and leaves the row
claiming a run that no longer exists. :func:`recover_orphaned_plans` is
the startup reconciliation pass for exactly that, mirroring the one
workflow runs already have.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from nerve.config import RESUME_QUEUE_FILE

if TYPE_CHECKING:
    from nerve.agent.engine import AgentEngine
    from nerve.db import Database

logger = logging.getLogger(__name__)


_FALLBACK_PLANNER_SESSION = "cron:task-planner"

# Reason a restart-reconciled plan reads ``failed``. ``plans`` has no error
# column (and this change deliberately adds no migration), so the reason lives
# in the log line and the notification body.
_PLAN_RESTART_ERROR = "interrupted by nerve restart"


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


# --------------------------------------------------------------------------- #
#  Restart recovery                                                            #
# --------------------------------------------------------------------------- #


def _enrolled_resume_session_ids() -> set[str] | None:
    """Session ids ``nerve restart --resume`` enrolled, or None if unreadable.

    Parsed exactly as :meth:`AgentEngine.resume_enrolled_sessions` parses the
    same file, so the two readers can never disagree about what is enrolled.

    Never drains the file: the engine's resume task is the sole drainer and it
    runs later in startup, so consuming it here would silently cancel every
    enrolled resume.

    The two error cases go opposite ways. A missing file is a definite answer
    from the sole writer (the CLI appends *before* triggering the restart):
    nothing was enrolled, so return an empty set. An ``OSError`` is not an
    answer, so return None and let the caller skip the sweep -- our read and
    the engine's happen at different instants, so a transient error here can
    succeed there and resume a session whose plan we would have failed.
    """
    try:
        raw = RESUME_QUEUE_FILE.read_text()
    except FileNotFoundError:
        return set()
    except OSError as e:
        logger.error(
            "Plan recovery: could not read resume queue %s: %s", RESUME_QUEUE_FILE, e,
        )
        return None
    return {sid for line in raw.splitlines() if (sid := line.strip())}


async def _resume_eligible(db: "Database", session_id: str) -> bool:
    """Whether the engine would actually resume ``session_id``.

    Re-evaluates the four skip predicates in
    :meth:`AgentEngine.resume_enrolled_sessions` (missing / archived /
    satellite / no SDK session to resume). Enrollment alone is not evidence of
    a live obligation: a session the engine will skip never resumes, so
    sparing its plan would wedge the plan for nothing.

    All four are pure reads of one ``sessions`` row, so this costs one lookup
    per enrolled id and adds no ordering constraint.
    """
    from nerve.agent.sessions import SessionStatus

    session = await db.get_session(session_id)
    if not session:
        return False
    if session.get("status") == SessionStatus.ARCHIVED.value:
        return False
    if session.get("source") == "external":
        return False
    if not session.get("sdk_session_id"):
        return False
    return True


async def recover_orphaned_plans(db: "Database", notification_service=None) -> int:
    """Reconcile plans orphaned by a daemon restart; return how many flipped.

    A plan in ``implementing`` asserts a live in-process implementation run.
    The only things that can move it off that status are the ``asyncio`` task
    approval spawned and a later ``task_done``, so a restart leaves the row
    asserting an obligation nobody holds: re-approval is refused (both
    surfaces gate on ``pending``) and ``plan_propose`` is refused forever for
    that task, with no agent tool or UI flow able to recover it.

    Each orphan is CASed to ``failed`` -- the status both approval surfaces
    already write when the implementation run does not complete -- which
    unblocks ``plan_propose`` so the recovery route is "propose v+1" rather
    than blindly replaying a stale plan over partial effects.

    Plans whose implementation session is enrolled for resume and still
    eligible are left ``implementing``. That exclusion is correctness, not
    caution: a resumed session completes through ``task_done``, which closes
    only an ``implementing`` plan, so sweeping it would leave the resumed
    session unable to ever close its own plan -- and would immediately let a
    duplicate plan be proposed while it is still working.

    Must run before the cron service starts: cron's catch-up can dispatch a
    planner run whose ``plan_propose`` would read a stale ``implementing`` row
    and permanently skip the task.

    A plan reconciled to ``failed`` is not re-adopted if its session is later
    resumed (the task still completes; only the plan's label reads ``failed``).
    Distinguishing a restart-swept plan from a legitimately failed one needs
    durable recovery provenance, i.e. a schema change this change avoids.
    """
    enrolled = _enrolled_resume_session_ids()
    if enrolled is None:
        # Fail closed: leave every plan alone rather than risk failing one
        # whose session the engine is about to resume. Costs exactly the
        # status quo -- plans stay ``implementing`` and the next restart
        # retries.
        logger.error(
            "Plan recovery skipped: resume queue unreadable. Plans stay "
            "'implementing'; the next restart retries.",
        )
        return 0

    orphaned = await db.get_implementing_plans()
    flipped: list[str] = []
    for plan in orphaned:
        sid = plan.get("impl_session_id")
        # Cheap membership test first, so the common case (nothing enrolled)
        # never pays for a session lookup. A NULL owner can never match.
        if sid and sid in enrolled and await _resume_eligible(db, sid):
            logger.info(
                "Plan recovery: plan %s left 'implementing' -- its session %s is "
                "enrolled for resume", plan["id"], sid,
            )
            continue
        if await db.fail_orphaned_plan(plan["id"]):
            flipped.append(plan["id"])

    if flipped:
        logger.info(
            "Plan recovery: %d plan(s) marked failed (%s): %s",
            len(flipped), _PLAN_RESTART_ERROR, ", ".join(flipped),
        )
        if notification_service is not None:
            # Keyed on what we actually flipped, not on what we read: a plan
            # that legitimately left ``implementing`` between the read and the
            # write (a concurrent ``task_done``) must not be reported as
            # interrupted.
            try:
                await notification_service.send_notification(
                    session_id="system",
                    title="Plans interrupted by restart",
                    body=(
                        f"{len(flipped)} plan(s) were marked failed after a Nerve "
                        f"restart ({_PLAN_RESTART_ERROR}): {', '.join(flipped)}. "
                        "Their implementation sessions did not survive the "
                        "restart, so the work may be partially done. Review the "
                        "task, then propose a fresh plan version if it is still "
                        "needed."
                    ),
                    priority="high",
                )
            except Exception:
                # A notification failure must not abort recovery -- the rows are
                # already reconciled, which is the part that matters.
                logger.exception("Plan recovery: notification failed")

    return len(flipped)
