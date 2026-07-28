"""Tool handler: propose_config_change — open a PR against the workspace repo.

This is how the agent changes its own configuration (skills, cron, settings)
when it can't edit the live workspace directly — always under lockdown, and the
recommended reviewed path otherwise. See the `nerve-workspace` skill.
"""

from __future__ import annotations

import asyncio
import logging
import time

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import PROPOSE_CONFIG_CHANGE_SCHEMA

logger = logging.getLogger(__name__)


async def propose_config_change_handler(ctx: ToolContext, args: dict) -> ToolResult:
    from pathlib import Path

    from nerve.config_pr import propose_config_change

    config = ctx.config
    if config is None:
        return ToolResult.text("Config not available.", is_error=True)

    workspace = Path(config.workspace)
    config_dir = Path(config.config_dir) if config.config_dir else workspace
    title = args["title"]
    body = args.get("body", "")
    changes = args["changes"]

    try:
        result = await asyncio.to_thread(
            propose_config_change,
            workspace, config_dir, title, body, changes, int(time.time()),
        )
    except Exception as e:  # noqa: BLE001
        logger.error("propose_config_change failed: %s", e)
        return ToolResult.text(f"Could not open PR: {e}", is_error=True)

    if result.ok:
        text = f"Opened PR on branch `{result.branch}`:\n{result.pr_url}"
        if result.code_paths:
            # Validation never runs or parses a bundle's own code, so the
            # reviewer is the only check on it. Say it here too: the tool only
            # recognizes the effects it knows about, and the agent knows what it
            # actually intended.
            listed = ", ".join(f"`{p}`" for p in result.code_paths)
            text += (
                f"\n\nThis PR changes what runs on the instance ({listed}) and the "
                "PR body says so. Nothing validates it — repeat it in your own "
                "words when you report the PR."
            )
        return ToolResult.text(text)
    if result.validation_errors:
        errs = "\n".join(f"- {e}" for e in result.validation_errors)
        return ToolResult.text(
            f"Change is invalid — no PR opened. Fix these and retry:\n{errs}",
            is_error=True,
        )
    return ToolResult.text(f"Could not open PR: {result.message}", is_error=True)


PROPOSE_CONFIG_CHANGE_SPEC = ToolSpec(
    name="propose_config_change",
    description=(
        "Propose a change to your own configuration (skills, cron jobs, settings) "
        "by opening a pull request against the workspace git repo, for human "
        "review. Use this — never edit tracked config files directly — when the "
        "instance is locked (remote-only), or whenever a reviewed change is "
        "wanted. Provide the FULL new content of each file you want to change; "
        "paths are relative to the workspace root (e.g. 'config/cron/jobs.yaml') "
        "and must be reviewed configuration — 'config/…', 'skills/…', or a "
        "workspace-root instruction file. The change is validated before the PR "
        "is opened; an invalid change, or any path outside that surface, is "
        "rejected with the reasons to fix and no PR is opened."
    ),
    input_schema=PROPOSE_CONFIG_CHANGE_SCHEMA,
    handler=propose_config_change_handler,
)


CONFIG_PR_SPECS = [PROPOSE_CONFIG_CHANGE_SPEC]
