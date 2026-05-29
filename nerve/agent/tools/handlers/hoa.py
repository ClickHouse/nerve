"""HouseOfAgents tool handlers — hoa_status, hoa_list_pipelines, hoa_execute.

These are only registered into a session's MCP server when
``config.houseofagents.enabled`` is true — see the ``include_hoa`` flag
on :func:`build_session_mcp_server`. ``hoa_execute`` needs the session_id
so its streaming progress events route to the right session.
"""

from __future__ import annotations

import json
import logging

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    HOA_EXECUTE_SCHEMA,
    HOA_LIST_PIPELINES_SCHEMA,
    HOA_STATUS_SCHEMA,
)

logger = logging.getLogger(__name__)


def _format_hoa_event_log(events: list[dict]) -> str:
    """Format HoA progress events into a readable log for the tool result."""
    if not events:
        return ""
    lines = ["## Execution Log"]
    for ev in events:
        event_type = ev.get("event", "")
        label = ev.get("label", "")
        agent = ev.get("agent", "")
        message = ev.get("message", "")
        iteration = ev.get("iteration")
        loop_pass = ev.get("loop_pass")

        if event_type == "run_info":
            mode = ev.get("mode", "?")
            agents = ev.get("agents", [])
            lines.append(
                f"- **Run started** — mode: {mode}, "
                f"agents: {', '.join(agents) if agents else 'from pipeline'}"
            )
        elif event_type == "block_started":
            suffix = f" (iter {iteration})" if iteration and iteration > 1 else ""
            loop_suffix = f" [loop {loop_pass}]" if loop_pass and loop_pass > 0 else ""
            lines.append(f"- **{label or 'Block'}** started{suffix}{loop_suffix} → {agent}")
        elif event_type == "block_finished":
            lines.append(f"- **{label or 'Block'}** finished → {agent}")
        elif event_type == "block_skipped":
            lines.append(f"- **{label or 'Block'}** skipped")
        elif event_type == "iteration_complete":
            lines.append(f"- Iteration {iteration} complete")
        elif event_type == "all_done":
            lines.append(f"- **All done**")
        elif event_type == "error":
            lines.append(f"- ❌ Error: {message}")
        # Skip verbose block_log / run_dir events from the summary

    return "\n".join(lines) if len(lines) > 1 else ""


async def hoa_status_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if not ctx.config or not ctx.config.houseofagents.enabled:
        return ToolResult.text(
            "houseofagents: disabled (set houseofagents.enabled: true in config.yaml)"
        )
    from nerve.houseofagents import get_hoa_service
    svc = get_hoa_service()
    available = svc.is_available()
    version = await svc.get_version() if available else None
    status = "available" if available else "not installed (will install on first use)"
    text = f"houseofagents: {status}"
    if version:
        text += f"\nVersion: {version}"
    text += f"\nDefault mode: {ctx.config.houseofagents.default_mode}"
    text += f"\nDefault agents: {', '.join(ctx.config.houseofagents.default_agents)}"
    return ToolResult.text(text)


async def hoa_list_pipelines_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if not ctx.config or not ctx.config.houseofagents.enabled:
        return ToolResult.text("houseofagents is not enabled.")
    from nerve.houseofagents.pipelines import PipelineManager
    pm = PipelineManager(ctx.config.houseofagents.pipelines_dir)
    pipelines = pm.list_pipelines()
    if not pipelines:
        return ToolResult.text("No pipelines configured.")
    lines = [f"- **{p['id']}**: {p['description']}" for p in pipelines]
    return ToolResult.text("Available pipelines:\n" + "\n".join(lines))


async def hoa_execute_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if not ctx.config or not ctx.config.houseofagents.enabled:
        return ToolResult.text(
            "houseofagents is not enabled. "
            "Set houseofagents.enabled: true in config.yaml to use multi-agent execution."
        )
    from nerve.houseofagents import get_hoa_service
    from nerve.houseofagents.runner import HoARunner

    runner = HoARunner(get_hoa_service())

    agents_str = args.get("agents", "")
    agents = [a.strip() for a in agents_str.split(",") if a.strip()] if agents_str else None

    pipeline_file = None
    pipeline_id = args.get("pipeline_id", "")
    if pipeline_id:
        from nerve.houseofagents.pipelines import PipelineManager
        pm = PipelineManager(ctx.config.houseofagents.pipelines_dir)
        pipeline_file = pm.get_path(pipeline_id)
        if not pipeline_file:
            return ToolResult.text(
                f"Pipeline '{pipeline_id}' not found. "
                "Use hoa_list_pipelines to see available pipelines."
            )

    result = await runner.execute(
        prompt=args["prompt"],
        mode=args.get("mode", ctx.config.houseofagents.default_mode),
        agents=agents,
        iterations=args.get("iterations", ctx.config.houseofagents.default_iterations),
        pipeline_file=pipeline_file,
        session_id=ctx.session_id,
    )

    event_log = _format_hoa_event_log(result.events)

    if result.success:
        output_parts = []
        if result.output_dir:
            output_parts.append(f"Output directory: {result.output_dir}")
        if result.stdout_json:
            output_parts.append(json.dumps(result.stdout_json, indent=2))
        elif result.stdout_raw:
            output_parts.append(result.stdout_raw)
        if event_log:
            output_parts.append(event_log)
        return ToolResult.text(
            "\n\n".join(output_parts) if output_parts else "Completed successfully."
        )

    parts = [f"houseofagents exited with code {result.exit_code}"]
    if event_log:
        parts.append(event_log)
    parts.append(f"stderr:\n{result.stderr_log[:2000]}")
    return ToolResult.text("\n\n".join(parts))


HOA_STATUS_SPEC = ToolSpec(
    name="hoa_status",
    description=(
        "Check houseofagents multi-agent runtime availability and version. "
        "Returns whether houseofagents is enabled, installed, and its version."
    ),
    input_schema=HOA_STATUS_SCHEMA,
    handler=hoa_status_handler,
)

HOA_LIST_PIPELINES_SPEC = ToolSpec(
    name="hoa_list_pipelines",
    description="List available houseofagents pipeline configurations.",
    input_schema=HOA_LIST_PIPELINES_SCHEMA,
    handler=hoa_list_pipelines_handler,
)

HOA_EXECUTE_SPEC = ToolSpec(
    name="hoa_execute",
    description=(
        "Execute a multi-agent workflow using houseofagents. "
        "Orchestrates multiple AI agents (Claude, OpenAI, Gemini) in relay, swarm, or pipeline mode. "
        "Progress streams to the UI in real-time. Returns the combined result. "
        "Use this for complex implementations that benefit from multi-agent review and iteration. "
        "Only available when houseofagents is enabled in config."
    ),
    input_schema=HOA_EXECUTE_SCHEMA,
    handler=hoa_execute_handler,
)


HOA_SPECS = [
    HOA_STATUS_SPEC,
    HOA_LIST_PIPELINES_SPEC,
    HOA_EXECUTE_SPEC,
]
