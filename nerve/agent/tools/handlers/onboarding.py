"""Onboarding tool handler: onboarding_done.

The onboarding conversation (:mod:`nerve.onboarding`) ends when the agent
calls ``onboarding_done``. Only the session of that conversation can call it.
"""

from __future__ import annotations

from nerve import onboarding
from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import ONBOARDING_DONE_SCHEMA


async def onboarding_done_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if not ctx.db or not ctx.workspace:
        return ToolResult.text("Database or workspace not available.", is_error=True)
    done = await onboarding.complete(
        ctx.db, ctx.workspace, ctx.session_id, actor_id=ctx.attribution.actor_id,
    )
    if not done:
        return ToolResult.text(
            "Nothing changed: this session is not an open onboarding conversation.",
            is_error=True,
        )
    return ToolResult.text(
        "Onboarding is done. The web UI no longer asks the person to come back to it."
    )


ONBOARDING_DONE_SPEC = ToolSpec(
    name="onboarding_done",
    description=(
        "Mark the onboarding conversation done. Call it once, in the onboarding "
        "session, after the person confirmed the setup and you applied and summarized it."
    ),
    input_schema=ONBOARDING_DONE_SCHEMA,
    handler=onboarding_done_handler,
)

ONBOARDING_SPECS = [ONBOARDING_DONE_SPEC]
