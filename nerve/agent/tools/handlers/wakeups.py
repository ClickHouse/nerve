"""``schedule_wakeup`` — registry equivalent of the Claude CLI built-in.

The Claude backend gets ScheduleWakeup as a CLI built-in tool (recorded
via a PostToolUse hook); backends without built-ins (Codex) expose this
registry tool instead, which persists the same wakeup rows the
cron-service sweep fires via ``engine.run(..., source="wakeup")``.

The Claude backend EXCLUDES this tool from its per-session MCP server
(``ClaudeBackend.excluded_tools``) so the two paths never coexist in one
session. Satellite sessions (external Codex/Claude Code clients over the
HTTP MCP endpoint) are rejected in the handler: the wakeup sweep runs
sessions through the engine, which makes no sense for a session the
engine has never owned.
"""

from __future__ import annotations

import logging

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec

logger = logging.getLogger(__name__)

SCHEDULE_WAKEUP_SCHEMA = {
    "type": "object",
    "properties": {
        "delaySeconds": {
            "type": "number",
            "description": (
                "Seconds from now to wake this session up. "
                "Clamped to [60, 3600]."
            ),
        },
        "prompt": {
            "type": "string",
            "description": (
                "The prompt to re-inject into this session when the wakeup "
                "fires. Describe what future-you should check or continue."
            ),
        },
        "reason": {
            "type": "string",
            "description": (
                "One short sentence explaining the chosen delay "
                "(shown to the user)."
            ),
            "default": "",
        },
    },
    "required": ["delaySeconds", "prompt"],
}


async def schedule_wakeup_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if ctx.db is None or ctx.engine is None:
        return ToolResult.text(
            "schedule_wakeup is unavailable: engine not wired", is_error=True,
        )

    # Wakeups fire through engine.run() on this exact session — reject
    # sessions the engine doesn't own (external MCP satellites).
    try:
        session = await ctx.db.get_session(ctx.session_id)
    except Exception:
        session = None
    if session and session.get("source") == "external":
        return ToolResult.text(
            "schedule_wakeup is not available for external client sessions — "
            "it can only wake sessions owned by the Nerve engine.",
            is_error=True,
        )

    wakeup_id = await ctx.engine._record_wakeup(ctx.db, ctx.session_id, args)
    if wakeup_id is None:
        return ToolResult.text(
            "No wakeup scheduled: a non-empty `prompt` is required.",
            is_error=True,
        )
    fire_at = ctx.engine._wakeup_fire_at(args.get("delaySeconds"))
    return ToolResult.text(
        f"Wakeup #{wakeup_id} scheduled (~{fire_at} UTC). This session will "
        "be re-invoked with your prompt then. Omit further scheduling to "
        "stop the loop."
    )


WAKEUP_SPECS = [
    ToolSpec(
        name="schedule_wakeup",
        description=(
            "Schedule this session to wake up again after a delay (60–3600s) "
            "with a prompt for your future self. Use for monitoring loops and "
            "deferred follow-ups. The wakeup re-invokes THIS session."
        ),
        input_schema=SCHEDULE_WAKEUP_SCHEMA,
        handler=schedule_wakeup_handler,
    ),
]
