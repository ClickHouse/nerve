"""Deprecation stubs for the retired houseofagents (HoA) toolset.

houseofagents was retired in favor of workflow runs (``nerve/workflows/``,
the ``workflow_run_*`` tools, ``docs/workflow-runs.md``). These stubs keep
the three tool names (``hoa_status``, ``hoa_list_pipelines``,
``hoa_execute``) registered so stale clients, saved prompts, and old
sessions that still reference them get a helpful pointer instead of an
"unknown tool" error. They perform no work and import nothing from the
removed ``nerve.houseofagents`` package.

Visibility is still gated by ``config.houseofagents.enabled`` /
``include_hoa`` exactly as before (see ``ToolRegistry.list`` and the
session/MCP server builders), so most installs never see them.

Temporary: keep for two releases after the retirement, then delete this
module together with the ``include_hoa`` gating plumbing.
"""

from __future__ import annotations

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec

_EMPTY_SCHEMA = {
    "type": "object",
    "properties": {},
    "required": [],
}

_RETIREMENT_NOTE = (
    "houseofagents was retired. Use workflow_run_start(engine='claude-workflow' "
    "or 'codex-ultracode', prompt=..., budget_usd=...) instead — budget-capped "
    "multi-agent runs with journals, live spend tracking, and run-scoped kill. "
    "See docs/workflow-runs.md."
)


async def hoa_status_handler(ctx: ToolContext, args: dict) -> ToolResult:
    return ToolResult.text(
        "houseofagents was retired and is no longer installed. Multi-agent "
        "execution now runs as workflow runs — check workflow_run_list / "
        "workflow_run_status, and start new runs with workflow_run_start. "
        "See docs/workflow-runs.md."
    )


async def hoa_list_pipelines_handler(ctx: ToolContext, args: dict) -> ToolResult:
    return ToolResult.text(
        "houseofagents pipelines were retired along with houseofagents. "
        "Use workflow_run_start for multi-agent runs instead. "
        "See docs/workflow-runs.md."
    )


async def hoa_execute_handler(ctx: ToolContext, args: dict) -> ToolResult:
    return ToolResult.text(_RETIREMENT_NOTE)


HOA_STATUS_SPEC = ToolSpec(
    name="hoa_status",
    description=(
        "[DEPRECATED] houseofagents was retired — returns a pointer to the "
        "workflow_run_* tools (see docs/workflow-runs.md)."
    ),
    input_schema=_EMPTY_SCHEMA,
    handler=hoa_status_handler,
)

HOA_LIST_PIPELINES_SPEC = ToolSpec(
    name="hoa_list_pipelines",
    description=(
        "[DEPRECATED] houseofagents was retired — returns a pointer to the "
        "workflow_run_* tools (see docs/workflow-runs.md)."
    ),
    input_schema=_EMPTY_SCHEMA,
    handler=hoa_list_pipelines_handler,
)

HOA_EXECUTE_SPEC = ToolSpec(
    name="hoa_execute",
    description=(
        "[DEPRECATED] houseofagents was retired — use workflow_run_start for "
        "budget-capped multi-agent runs (see docs/workflow-runs.md)."
    ),
    input_schema=_EMPTY_SCHEMA,
    handler=hoa_execute_handler,
)


HOA_SPECS = [
    HOA_STATUS_SPEC,
    HOA_LIST_PIPELINES_SPEC,
    HOA_EXECUTE_SPEC,
]
