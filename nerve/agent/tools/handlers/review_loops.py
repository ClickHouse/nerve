"""Review loop tools — start/status/kill implement→verify loops.

A review loop alternates a budget-capped implementer workflow run (works
toward a Goal prompt) with an independent verifier workflow run (judges
the workspace against Verifier criteria, returns a structured verdict)
until the criteria pass or a cap fires. The loop is driven by a
deterministic server-side controller; see docs/review-loops.md.

Same restriction semantics as workflow runs: external MCP satellites and
workflow-run sessions may inspect loops but not start/kill them.
"""

from __future__ import annotations

import json
import logging

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec

logger = logging.getLogger(__name__)

REVIEW_LOOP_START_SCHEMA = {
    "type": "object",
    "properties": {
        "goal": {
            "type": "string",
            "description": (
                "The implementation objective — what the implementer leg "
                "must accomplish. Written for an autonomous agent."
            ),
        },
        "verifier": {
            "type": "string",
            "description": (
                "The completion criteria the verifier leg judges the "
                "workspace against. Bullet lines become individual pinned "
                "criteria; prefer executable, observable checks."
            ),
        },
        "budget_usd": {
            "type": "number",
            "description": (
                "Total loop budget in dollars (all legs; finite, > 0). "
                "Defaults to workflows.review_loop.default_budget_usd."
            ),
        },
        "title": {
            "type": "string",
            "description": "Short human-readable label.",
            "default": "",
        },
        "cwd": {
            "type": "string",
            "description": (
                "Shared workspace for all legs (created if missing). "
                "Defaults to this session's cwd, then the global workspace."
            ),
            "default": "",
        },
        "max_iterations": {
            "type": "integer",
            "description": "Implementer iterations before parking for a decision (default 3, ceiling 8).",
        },
        "criteria_adoption": {
            "type": "string",
            "enum": ["no", "ask", "auto"],
            "description": (
                "What happens to verifier-proposed criteria: 'no' = advisory "
                "only (default; fixed contract), 'ask' = adoption needs a "
                "user decision, 'auto' = auto-adopt with caps (research/"
                "audit goals where completeness is discovered)."
            ),
        },
        "implementer_engine": {
            "type": "string",
            "enum": ["claude-workflow", "codex-ultracode"],
            "description": "Implementer leg engine (default from config).",
        },
        "implementer_model": {"type": "string", "description": "Implementer model override.", "default": ""},
        "implementer_effort": {"type": "string", "description": "Implementer effort override.", "default": ""},
        "verifier_engine": {
            "type": "string",
            "enum": ["claude-workflow", "codex-ultracode"],
            "description": "Verifier leg engine (default from config — cross-vendor codex).",
        },
        "verifier_model": {"type": "string", "description": "Verifier model override.", "default": ""},
        "verifier_effort": {"type": "string", "description": "Verifier effort override.", "default": ""},
    },
    "required": ["goal", "verifier"],
}

REVIEW_LOOP_STATUS_SCHEMA = {
    "type": "object",
    "properties": {
        "loop_id": {
            "type": "string",
            "description": "Review loop id (rvl-xxxxxxxx).",
            "default": "",
        },
        "session_id": {
            "type": "string",
            "description": (
                "Look the loop up by its observer session instead of by id "
                "(defaults to the current session when both are empty)."
            ),
            "default": "",
        },
    },
    "required": [],
}

REVIEW_LOOP_KILL_SCHEMA = {
    "type": "object",
    "properties": {
        "loop_id": {"type": "string", "description": "Review loop id (rvl-xxxxxxxx)."},
        "reason": {"type": "string", "description": "Why (recorded on the loop).", "default": ""},
    },
    "required": ["loop_id"],
}

REVIEW_LOOP_LIST_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {
            "type": "string",
            "description": (
                "Filter: 'open' (running + parked), one of pending/"
                "implementing/verifying/awaiting_user/passed/failed/killed, "
                "or empty for all."
            ),
            "default": "",
        },
        "limit": {"type": "integer", "description": "Max loops to return.", "default": 20},
    },
    "required": [],
}


def _get_service(ctx: ToolContext):
    from nerve.workflows import get_review_loop_service

    if ctx.db is None or ctx.engine is None:
        return None, "review loops unavailable: engine not wired"
    service = get_review_loop_service()
    if service is None:
        return None, (
            "review loops are disabled (workflows.review_loop.enabled: "
            "false) or the service has not started"
        )
    return service, ""


async def _reject_restricted(ctx: ToolContext) -> ToolResult | None:
    """Mirror workflow-run gating: no external satellites (loops are
    engine-owned) and no workflow sessions (budget containment)."""
    try:
        session = await ctx.db.get_session(ctx.session_id)
    except Exception:  # noqa: BLE001
        session = None
    source = (session or {}).get("source")
    if source == "external":
        return ToolResult.text(
            "review_loop_start/kill are not available for external client "
            "sessions. Use review_loop_list/status to inspect.",
            is_error=True,
        )
    if source == "workflow":
        return ToolResult.text(
            "workflow-run sessions cannot start or kill review loops — a "
            "run's budget must bound all spend it causes.",
            is_error=True,
        )
    return None


def _format_loop(loop: dict) -> str:
    line = (
        f"{loop['id']} [{loop['status']}] {loop.get('title') or ''} — "
        f"iteration {loop.get('iteration')}/{loop.get('max_iterations')}, "
        f"spent ${float(loop.get('spent_usd') or 0):.2f} / "
        f"${float(loop.get('budget_usd') or 0):.2f}"
    )
    if loop.get("failure_reason"):
        line += f" — {loop['failure_reason']}"
    if loop.get("session_id"):
        line += f" — session {loop['session_id']}"
    return line


async def review_loop_start_handler(ctx: ToolContext, args: dict) -> ToolResult:
    service, reason = _get_service(ctx)
    if service is None:
        return ToolResult.text(reason, is_error=True)
    rejected = await _reject_restricted(ctx)
    if rejected:
        return rejected
    from nerve.workflows.review_loop import ReviewLoopError

    cwd = str(args.get("cwd") or "")
    if not cwd:
        try:
            session = await ctx.db.get_session(ctx.session_id)
            cwd = str((session or {}).get("cwd") or "")
        except Exception:  # noqa: BLE001
            cwd = ""

    def _leg(prefix: str) -> dict | None:
        leg = {
            "engine": args.get(f"{prefix}_engine") or "",
            "model": args.get(f"{prefix}_model") or "",
            "effort": args.get(f"{prefix}_effort") or "",
        }
        return {k: v for k, v in leg.items() if v} or None

    try:
        loop = await service.create_loop(
            goal=str(args.get("goal") or ""),
            verifier=str(args.get("verifier") or ""),
            session_id=ctx.session_id,
            cwd=cwd or None,
            title=str(args.get("title") or ""),
            budget_usd=args.get("budget_usd"),
            max_iterations=args.get("max_iterations"),
            criteria_adoption=args.get("criteria_adoption"),
            implementer=_leg("implementer"),
            verifier_leg=_leg("verifier"),
            created_by=f"mcp:{ctx.session_id}",
        )
    except ReviewLoopError as e:
        return ToolResult.text(str(e), is_error=True)
    except Exception as e:  # noqa: BLE001
        logger.exception("review_loop_start failed")
        return ToolResult.text(f"review_loop_start failed: {e}", is_error=True)
    crit = "\n".join(f"  {c['id']}: {c['statement']}" for c in loop["criteria"])
    return ToolResult.text(
        f"Review loop {loop['id']} created ({loop['status']}). Budget "
        f"${loop['budget_usd']:.2f}, max {loop['max_iterations']} iterations, "
        f"implementer {loop['implementer']['engine']}, verifier "
        f"{loop['verifier']['engine']}, cwd {loop.get('cwd')}.\n"
        f"Pinned criteria:\n{crit}\n"
        f"This session is the loop's observer — iteration milestones will "
        f"appear here. Check progress with review_loop_status(loop_id="
        f"\"{loop['id']}\")."
    )


async def review_loop_status_handler(ctx: ToolContext, args: dict) -> ToolResult:
    service, reason = _get_service(ctx)
    if service is None:
        return ToolResult.text(reason, is_error=True)
    loop_id = str(args.get("loop_id") or "")
    if loop_id:
        loop = await service.get_loop(loop_id)
    else:
        session_id = str(args.get("session_id") or "") or ctx.session_id
        loop = await service.get_loop_for_session(session_id)
    if loop is None:
        return ToolResult.text("no matching review loop found", is_error=True)
    attempts = await service.list_attempts(loop["id"])
    ledger = [
        {
            "iteration": a["iteration"], "role": a["role"],
            "attempt": a["attempt_no"], "run_id": a["run_id"],
            "status": a["status"], "spend_usd": a["spend_usd"],
            "verdict": (a.get("verdict") or {}).get("verdict"),
        }
        for a in attempts
    ]
    payload = {"loop": service.public_loop(loop), "attempts": ledger}
    return ToolResult.text(
        _format_loop(loop) + "\n\n" + json.dumps(payload, indent=2, default=str)
    )


async def review_loop_kill_handler(ctx: ToolContext, args: dict) -> ToolResult:
    service, reason = _get_service(ctx)
    if service is None:
        return ToolResult.text(reason, is_error=True)
    rejected = await _reject_restricted(ctx)
    if rejected:
        return rejected
    from nerve.workflows.review_loop import ReviewLoopError

    try:
        loop = await service.kill_loop(
            str(args.get("loop_id") or ""),
            reason=str(args.get("reason") or ""),
            killed_by=f"session:{ctx.session_id}",
        )
    except ReviewLoopError as e:
        return ToolResult.text(str(e), is_error=True)
    return ToolResult.text(
        f"Review loop {loop['id']} is now '{loop['status']}'. Its current "
        "leg was killed; other runs are unaffected."
    )


async def review_loop_list_handler(ctx: ToolContext, args: dict) -> ToolResult:
    service, reason = _get_service(ctx)
    if service is None:
        return ToolResult.text(reason, is_error=True)
    status = str(args.get("status") or "") or None
    limit = max(1, min(int(args.get("limit") or 20), 100))
    loops = await service.list_loops(status=status, limit=limit)
    if not loops:
        return ToolResult.text(
            "No review loops" + (f" with status '{status}'" if status else "") + "."
        )
    lines = [_format_loop(lp) for lp in loops]
    return ToolResult.text(f"{len(loops)} review loop(s):\n" + "\n".join(lines))


REVIEW_LOOP_SPECS = [
    ToolSpec(
        name="review_loop_start",
        description=(
            "Start a review loop: an implementer agent (workflow run) works "
            "toward a Goal prompt; an independent verifier agent judges the "
            "workspace against Verifier criteria with a structured, "
            "evidence-backed verdict; the server iterates until the criteria "
            "pass or caps fire (iterations, dollar budget, no-progress). "
            "Cross-vendor legs supported (e.g. Claude implements, Codex "
            "verifies). The current session becomes the loop's observer."
        ),
        input_schema=REVIEW_LOOP_START_SCHEMA,
        handler=review_loop_start_handler,
    ),
    ToolSpec(
        name="review_loop_status",
        description=(
            "Get a review loop's state: status, iteration, criteria "
            "statuses, per-leg attempt ledger with verdicts and spend. Look "
            "up by loop_id or by observer session."
        ),
        input_schema=REVIEW_LOOP_STATUS_SCHEMA,
        handler=review_loop_status_handler,
    ),
    ToolSpec(
        name="review_loop_kill",
        description=(
            "Kill a review loop and its currently-running leg. Scoped to "
            "the loop's own runs."
        ),
        input_schema=REVIEW_LOOP_KILL_SCHEMA,
        handler=review_loop_kill_handler,
    ),
    ToolSpec(
        name="review_loop_list",
        description="List review loops with status, iteration, and spend.",
        input_schema=REVIEW_LOOP_LIST_SCHEMA,
        handler=review_loop_list_handler,
    ),
]
