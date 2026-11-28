"""Artifact tool handler: publish_artifact.

The tool publishes the bundle of a workspace path to the platform
(``nerve/artifacts/publish.py``). It exists only in external mode with
``artifacts.publish`` on (``config_excluded_tools``). The Publish button of
the Preview tab runs this same handler through the registry
(``nerve/gateway/routes/artifacts.py``), with the person as the actor and
the cause.

The result text has the viewer address, the version and who can see it;
the web chat shows the viewer from it. ``structured`` (for the route only)
has the same values. Neither has an upload link or the ID of a person.
"""

from __future__ import annotations

import logging
from typing import Any

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import PUBLISH_ARTIFACT_SCHEMA
from nerve.artifacts.publish import (
    PUBLISH_TOOL,
    PublishError,
    publish_artifact,
    publish_settings,
    result_text,
)
from nerve.config import get_config

logger = logging.getLogger(__name__)


async def publish_artifact_handler(ctx: ToolContext, args: dict) -> ToolResult:
    """Publish ``args["path"]`` with ``args["title"]`` (and ``artifact_id``
    for a new version)."""
    servers = ctx.engine.current_mcp_servers() if ctx.engine is not None else []
    # Pinned at the start of the gateway with the switch (publish_settings).
    settings = publish_settings(ctx.config if ctx.config is not None else get_config())
    try:
        published = await publish_artifact(
            workspace=ctx.workspace or "",
            servers=servers,
            path=args.get("path"),
            title=args.get("title"),
            artifact_id=args.get("artifact_id"),
            cause_id=ctx.cause_id,
            upload_origins=settings.upload_origins,
        )
    except PublishError as e:
        logger.info("Artifact publish refused (%s)", e.code)
        error: dict[str, Any] = {"code": e.code, "message": e.message}
        if e.bundle:
            error["bundle"] = True
        return ToolResult.text(f"Not published: {e.message}", is_error=True, structured={"error": error})
    return ToolResult.text(
        result_text(published),
        structured={
            "artifact_id": published.artifact_id,
            "version": published.version,
            "viewer_url": published.viewer_url,
            "expires_at": published.expires_at,
            "title": published.title,
            "changed_after": published.changed_after,
        },
    )


PUBLISH_ARTIFACT_SPEC = ToolSpec(
    name=PUBLISH_TOOL,
    description=(
        "Publish a page, a slide deck, a Markdown or CSV file, or an image of the workspace "
        "as an artifact on the platform, for the members of this agent. A folder with an "
        "index.html, an index.html, or another .html file sends that page and the files it "
        "uses; a .md or .csv file becomes a page that shows it; an image is shown as it is. "
        "Pages must use relative addresses for their own files and load nothing from the "
        "network. The result has the viewer address to give to people. A member can then "
        "share the version with everyone in the tenant in the viewer; you cannot. To change "
        "a published artifact, publish again with its artifact_id: that makes a new version "
        "at the same address."
    ),
    input_schema=PUBLISH_ARTIFACT_SCHEMA,
    handler=publish_artifact_handler,
)

ARTIFACT_SPECS = [PUBLISH_ARTIFACT_SPEC]
