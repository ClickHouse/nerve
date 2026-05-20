"""Skill tool handlers — skill_list, skill_get, skill_read_reference,
skill_run_script, skill_create, skill_update.
"""

from __future__ import annotations

import logging
import time

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    SKILL_CREATE_SCHEMA,
    SKILL_GET_SCHEMA,
    SKILL_LIST_SCHEMA,
    SKILL_READ_REFERENCE_SCHEMA,
    SKILL_RUN_SCRIPT_SCHEMA,
    SKILL_UPDATE_SCHEMA,
)

logger = logging.getLogger(__name__)


async def skill_list_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if not ctx.skill_manager:
        return ToolResult.text("Skills system not available.")

    try:
        summaries = await ctx.skill_manager.get_enabled_summaries()
        if not summaries:
            return ToolResult.text("No skills available.")

        lines = [f"**{len(summaries)} skill(s) available:**\n"]
        for s in summaries:
            lines.append(f"- **{s['name']}** (`{s['id']}`): {s['description'][:200]}")

        return ToolResult.text("\n".join(lines))
    except Exception as e:
        logger.error("skill_list failed: %s", e)
        return ToolResult.text(f"Error listing skills: {e}")


async def skill_get_handler(ctx: ToolContext, args: dict) -> ToolResult:
    skill_id = args["name"]

    if not ctx.skill_manager:
        return ToolResult.text("Skills system not available.")

    try:
        start = time.monotonic()
        skill = await ctx.skill_manager.get_skill(skill_id)
        duration_ms = int((time.monotonic() - start) * 1000)

        if not skill:
            return ToolResult.text(f"Skill not found: {skill_id}")

        await ctx.skill_manager.record_usage(
            skill_id=skill_id, invoked_by="model", duration_ms=duration_ms, success=True,
        )

        parts = [f"# Skill: {skill.name} (v{skill.version})\n"]
        parts.append(skill.content)

        if skill.has_references:
            refs = await ctx.skill_manager.list_references(skill_id)
            if refs:
                parts.append(f"\n**References available** (use `skill_read_reference` to load):")
                for r in refs:
                    parts.append(f"  - `{r}`")

        if skill.has_scripts:
            scripts_dir = ctx.skill_manager.skills_dir / skill_id / "scripts"
            scripts = sorted(
                str(f.relative_to(scripts_dir))
                for f in scripts_dir.rglob("*")
                if f.is_file()
            )
            if scripts:
                parts.append(f"\n**Scripts available** (use `skill_run_script` to execute):")
                for s in scripts:
                    parts.append(f"  - `{s}`")

        return ToolResult.text("\n".join(parts))
    except Exception as e:
        logger.error("skill_get failed: %s", e)
        if ctx.skill_manager:
            try:
                await ctx.skill_manager.record_usage(
                    skill_id=skill_id, invoked_by="model", success=False, error=str(e),
                )
            except Exception:
                pass
        return ToolResult.text(f"Error loading skill: {e}")


async def skill_read_reference_handler(ctx: ToolContext, args: dict) -> ToolResult:
    skill_id = args["name"]
    rel_path = args["path"]

    if not ctx.skill_manager:
        return ToolResult.text("Skills system not available.")

    try:
        content = await ctx.skill_manager.read_reference(skill_id, rel_path)
        if content is None:
            return ToolResult.text(f"Reference not found: {skill_id}/{rel_path}")
        return ToolResult.text(content)
    except Exception as e:
        return ToolResult.text(f"Error reading reference: {e}")


async def skill_run_script_handler(ctx: ToolContext, args: dict) -> ToolResult:
    skill_id = args["name"]
    rel_path = args["path"]
    script_args = args.get("args", "")

    if not ctx.skill_manager:
        return ToolResult.text("Skills system not available.")

    try:
        output = await ctx.skill_manager.run_script(skill_id, rel_path, script_args)
        return ToolResult.text(output)
    except Exception as e:
        return ToolResult.text(f"Error running script: {e}")


async def skill_create_handler(ctx: ToolContext, args: dict) -> ToolResult:
    name = args["name"]
    description = args["description"]
    content = args.get("content", "")

    if not ctx.skill_manager:
        return ToolResult.text("Skills system not available.")

    if not name or not description:
        return ToolResult.text("Both name and description are required.")

    try:
        meta = await ctx.skill_manager.create_skill(
            name=name, description=description, content=content,
        )
        await ctx.skill_manager.record_usage(
            skill_id=meta.id, invoked_by="model", success=True,
        )
        return ToolResult.text(
            f"Skill created: **{meta.name}** (`{meta.id}`)\n"
            f"Path: {ctx.skill_manager.skills_dir / meta.id}/SKILL.md"
        )
    except Exception as e:
        logger.error("skill_create failed: %s", e)
        return ToolResult.text(f"Error creating skill: {e}")


async def skill_update_handler(ctx: ToolContext, args: dict) -> ToolResult:
    skill_id = args["name"]
    content = args["content"]

    if not ctx.skill_manager:
        return ToolResult.text("Skills system not available.")

    try:
        meta = await ctx.skill_manager.update_skill(skill_id, content)
        if not meta:
            return ToolResult.text(f"Skill not found: {skill_id}")

        await ctx.skill_manager.record_usage(
            skill_id=skill_id, invoked_by="model", success=True,
        )
        return ToolResult.text(
            f"Skill updated: **{meta.name}** (`{meta.id}`) v{meta.version}"
        )
    except Exception as e:
        logger.error("skill_update failed: %s", e)
        return ToolResult.text(f"Error updating skill: {e}")


SKILL_LIST_SPEC = ToolSpec(
    name="skill_list",
    description="List all available skills with their descriptions. Use this to discover what skills are available before loading one.",
    input_schema=SKILL_LIST_SCHEMA,
    handler=skill_list_handler,
)

SKILL_GET_SPEC = ToolSpec(
    name="skill_get",
    description="Load the full content of a skill's SKILL.md instructions. Use this when you need to follow a skill's workflow.",
    input_schema=SKILL_GET_SCHEMA,
    handler=skill_get_handler,
)

SKILL_READ_REFERENCE_SPEC = ToolSpec(
    name="skill_read_reference",
    description="Read a reference file from a skill's references/ directory. Load only when you need specific documentation.",
    input_schema=SKILL_READ_REFERENCE_SCHEMA,
    handler=skill_read_reference_handler,
)

SKILL_RUN_SCRIPT_SPEC = ToolSpec(
    name="skill_run_script",
    description="Execute a script from a skill's scripts/ directory. Scripts run with a 30s timeout.",
    input_schema=SKILL_RUN_SCRIPT_SCHEMA,
    handler=skill_run_script_handler,
)

SKILL_CREATE_SPEC = ToolSpec(
    name="skill_create",
    description=(
        "Create a new skill. Use this to codify a reusable workflow, procedure, or domain knowledge "
        "into a skill that persists across sessions.\n\n"
        "When to create a skill:\n"
        "- You notice a multi-step workflow being repeated across sessions\n"
        "- The user asks you to 'remember how to do X' for a procedural task\n"
        "- You've built up domain-specific knowledge that future sessions would need\n"
        "- A complex task would benefit from step-by-step instructions\n\n"
        "The skill is written as a SKILL.md file with YAML frontmatter (name, description) "
        "and a markdown body containing instructions. Write the description in third person "
        "with specific trigger phrases."
    ),
    input_schema=SKILL_CREATE_SCHEMA,
    handler=skill_create_handler,
)

SKILL_UPDATE_SPEC = ToolSpec(
    name="skill_update",
    description=(
        "Update an existing skill's SKILL.md content. Use this to refine, fix, or extend a skill "
        "based on new knowledge or after discovering the current instructions are incomplete.\n\n"
        "The content parameter should be the FULL SKILL.md file including the YAML frontmatter "
        "(--- delimited block with name and description) and the markdown body."
    ),
    input_schema=SKILL_UPDATE_SCHEMA,
    handler=skill_update_handler,
)


SKILL_SPECS = [
    SKILL_LIST_SPEC,
    SKILL_GET_SPEC,
    SKILL_READ_REFERENCE_SPEC,
    SKILL_RUN_SCRIPT_SPEC,
    SKILL_CREATE_SPEC,
    SKILL_UPDATE_SPEC,
]
