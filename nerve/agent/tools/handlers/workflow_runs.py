"""Workflow run tools — start/status/kill/list budget-capped multi-agent jobs.

A workflow run executes in a dedicated engine-owned session (Claude
harness Workflow tool or Codex Ultracode) with a hard dollar budget
metered from Nerve's own usage accounting, run-scoped kill, and a
journal under ``workflows.runs_dir``. See :mod:`nerve.workflows`.

External MCP satellites may inspect runs but not start/kill them — runs
execute and are killed through the engine, which makes no sense to drive
from a session the engine has never owned (mirrors ``schedule_wakeup``).
"""

from __future__ import annotations

import json
import logging

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec

logger = logging.getLogger(__name__)

WORKFLOW_RUN_START_SCHEMA = {
    "type": "object",
    "properties": {
        "engine": {
            "type": "string",
            "enum": ["claude-workflow", "codex-ultracode"],
            "description": (
                "Execution engine: 'claude-workflow' (Claude session using "
                "the harness Workflow tool) or 'codex-ultracode' (Codex "
                "session using Ultracode multi-agent runs)."
            ),
        },
        "prompt": {
            "type": "string",
            "description": (
                "The task for the run. Written as instructions for an "
                "autonomous orchestrating agent."
            ),
        },
        "budget_usd": {
            "type": "number",
            "description": (
                "Hard dollar budget (finite, > 0). Spend is metered from "
                "Nerve's usage accounting; the run is warned at 80% and "
                "stopped at 100%. Pass 0 to explicitly request an "
                "unbudgeted run — honored only when "
                "workflows.allow_unbudgeted is enabled."
            ),
        },
        "title": {
            "type": "string",
            "description": "Short human-readable label for the run.",
            "default": "",
        },
        "model": {
            "type": "string",
            "description": "Model override (defaults to the backend's configured model).",
            "default": "",
        },
        "effort": {
            "type": "string",
            "description": "Reasoning effort override (low/medium/high/xhigh/max).",
            "default": "",
        },
        "cwd": {
            "type": "string",
            "description": "Working directory for the run's session (must exist).",
            "default": "",
        },
    },
    "required": ["engine", "prompt", "budget_usd"],
}

WORKFLOW_RUN_STATUS_SCHEMA = {
    "type": "object",
    "properties": {
        "run_id": {
            "type": "string",
            "description": "Workflow run id (wfr-xxxxxxxx).",
        },
    },
    "required": ["run_id"],
}

WORKFLOW_RUN_KILL_SCHEMA = {
    "type": "object",
    "properties": {
        "run_id": {
            "type": "string",
            "description": "Workflow run id (wfr-xxxxxxxx).",
        },
        "reason": {
            "type": "string",
            "description": "Why the run is being killed (recorded on the run).",
            "default": "",
        },
    },
    "required": ["run_id"],
}

WORKFLOW_RUN_LIST_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {
            "type": "string",
            "description": (
                "Filter: 'active' (pending+running), one of "
                "pending/running/done/failed/killed/budget_exhausted, "
                "or empty for all."
            ),
            "default": "",
        },
        "limit": {
            "type": "integer",
            "description": "Max runs to return.",
            "default": 20,
        },
    },
    "required": [],
}


def _get_service(ctx: ToolContext):
    """Resolve the workflow run service, or None with a reason string."""
    from nerve.workflows import get_workflow_run_service

    if ctx.db is None or ctx.engine is None:
        return None, "workflow runs unavailable: engine not wired"
    service = get_workflow_run_service()
    if service is None:
        return None, (
            "workflow runs are disabled (workflows.enabled: false) or the "
            "service has not started"
        )
    return service, ""


async def _reject_restricted(ctx: ToolContext) -> ToolResult | None:
    """Gate start/kill: reject external MCP satellites (runs are owned by
    the Nerve engine) AND workflow-run sessions themselves (a run spawning
    further independently-budgeted runs would let it spend past its own
    cap; killing a parent doesn't cascade)."""
    try:
        session = await ctx.db.get_session(ctx.session_id)
    except Exception:  # noqa: BLE001
        session = None
    source = (session or {}).get("source")
    if source == "external":
        return ToolResult.text(
            "workflow_run_start/kill are not available for external client "
            "sessions — runs are owned by the Nerve engine. Use "
            "workflow_run_list/status to inspect.",
            is_error=True,
        )
    if source == "workflow":
        return ToolResult.text(
            "workflow runs cannot start or kill other workflow runs — a "
            "run's budget must bound all spend it causes. Use the Workflow "
            "tool / Ultracode for in-run parallelism instead.",
            is_error=True,
        )
    return None


def _format_run(run: dict, service) -> str:
    budget = run.get("budget_usd")
    budget_txt = f"${budget:.2f}" if budget else "none"
    spent = float(run.get("spent_usd") or 0.0)
    line = (
        f"{run['id']} [{run['status']}] {run.get('title') or run['engine']} — "
        f"spent ${spent:.2f} / budget {budget_txt}"
    )
    if run.get("session_id"):
        line += f" — session {run['session_id']}"
    if run.get("error"):
        line += f" — {run['error']}"
    return line


async def workflow_run_start_handler(ctx: ToolContext, args: dict) -> ToolResult:
    service, reason = _get_service(ctx)
    if service is None:
        return ToolResult.text(reason, is_error=True)
    rejected = await _reject_restricted(ctx)
    if rejected:
        return rejected
    from nerve.workflows.service import WorkflowRunError

    spec = {
        "prompt": args.get("prompt") or "",
        "model": args.get("model") or "",
        "effort": args.get("effort") or "",
        "cwd": args.get("cwd") or "",
    }
    try:
        run = await service.start_run(
            engine_kind=str(args.get("engine") or ""),
            spec=spec,
            budget_usd=args.get("budget_usd"),
            title=str(args.get("title") or ""),
            created_by=f"session:{ctx.session_id}",
        )
    except WorkflowRunError as e:
        return ToolResult.text(str(e), is_error=True)
    except Exception as e:  # noqa: BLE001
        logger.exception("workflow_run_start failed")
        return ToolResult.text(f"workflow_run_start failed: {e}", is_error=True)
    budget = run.get("budget_usd")
    budget_txt = f"${budget:.2f}" if budget else "UNBUDGETED"
    return ToolResult.text(
        f"Workflow run {run['id']} created ({run['status']}). Engine: "
        f"{run['engine']}, budget: {budget_txt}. Journal: "
        f"{run.get('journal_dir')}. Check progress with "
        f"workflow_run_status(run_id=\"{run['id']}\")."
    )


async def workflow_run_status_handler(ctx: ToolContext, args: dict) -> ToolResult:
    service, reason = _get_service(ctx)
    if service is None:
        return ToolResult.text(reason, is_error=True)
    run = await service.get_run(str(args.get("run_id") or ""))
    if run is None:
        return ToolResult.text(
            f"no such workflow run: {args.get('run_id')}", is_error=True,
        )
    payload = service.public_run(run)
    return ToolResult.text(
        _format_run(run, service) + "\n\n" + json.dumps(payload, indent=2, default=str)
    )


async def workflow_run_kill_handler(ctx: ToolContext, args: dict) -> ToolResult:
    service, reason = _get_service(ctx)
    if service is None:
        return ToolResult.text(reason, is_error=True)
    rejected = await _reject_restricted(ctx)
    if rejected:
        return rejected
    from nerve.workflows.service import WorkflowRunError

    try:
        run = await service.kill_run(
            str(args.get("run_id") or ""),
            reason=str(args.get("reason") or ""),
            killed_by=f"session:{ctx.session_id}",
        )
    except WorkflowRunError as e:
        return ToolResult.text(str(e), is_error=True)
    return ToolResult.text(
        f"Workflow run {run['id']} is now '{run['status']}'. "
        "Kill is scoped to this run's own session — concurrent runs are "
        "unaffected."
    )


async def workflow_run_list_handler(ctx: ToolContext, args: dict) -> ToolResult:
    service, reason = _get_service(ctx)
    if service is None:
        return ToolResult.text(reason, is_error=True)
    status = str(args.get("status") or "") or None
    limit = max(1, min(int(args.get("limit") or 20), 100))
    runs = await service.list_runs(status=status, limit=limit)
    if not runs:
        return ToolResult.text("No workflow runs" + (f" with status '{status}'" if status else "") + ".")
    lines = [_format_run(r, service) for r in runs]
    return ToolResult.text(f"{len(runs)} workflow run(s):\n" + "\n".join(lines))


WORKFLOW_RUN_SPECS = [
    ToolSpec(
        name="workflow_run_start",
        description=(
            "Start a budget-capped multi-agent workflow run (Claude Workflow "
            "or Codex Ultracode) in its own tracked session. Nerve meters "
            "real dollar spend, warns at 80%, and kills the run at 100%. "
            "Returns the run id immediately; the run executes in the "
            "background."
        ),
        input_schema=WORKFLOW_RUN_START_SCHEMA,
        handler=workflow_run_start_handler,
    ),
    ToolSpec(
        name="workflow_run_status",
        description=(
            "Get a workflow run's status, metered spend vs budget, session, "
            "journal location, and result/error."
        ),
        input_schema=WORKFLOW_RUN_STATUS_SCHEMA,
        handler=workflow_run_status_handler,
    ),
    ToolSpec(
        name="workflow_run_kill",
        description=(
            "Kill a workflow run. Termination is scoped to the run's own "
            "session/subprocess — other runs are never affected."
        ),
        input_schema=WORKFLOW_RUN_KILL_SCHEMA,
        handler=workflow_run_kill_handler,
    ),
    ToolSpec(
        name="workflow_run_list",
        description=(
            "List workflow runs with status and spend-vs-budget. Filter by "
            "status ('active', 'running', 'done', ...)."
        ),
        input_schema=WORKFLOW_RUN_LIST_SCHEMA,
        handler=workflow_run_list_handler,
    ),
]
