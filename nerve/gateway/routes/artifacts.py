"""Artifact routes: the Publish button of the Preview tab.

``POST /api/artifacts/publish`` publishes the bundle of a workspace path
for the person who clicks. It runs the handler of the tool
``publish_artifact`` through the registry, with the person as the actor
and the cause, so the button and the tool have one code path
(``nerve/artifacts/publish.py``).

The router is registered only in external mode with ``artifacts.publish``
on; the web UI learns of it from ``GET /api/auth/me`` (``artifacts``).
Errors are ``{"detail": {"code": "<code>", "message": "<sentence>"}}``.
No answer has an upload link.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from nerve.artifacts.publish import (
    INVALID_ARGUMENTS,
    PLATFORM_BUSY,
    PLATFORM_ERROR,
    PLATFORM_UNAVAILABLE,
    PUBLISH_TOOL,
    SERVER_MISSING,
    SERVER_REFUSED,
    UPLOAD_FAILED,
    UPLOAD_ORIGIN_REFUSED,
    UPLOAD_ORIGINS_MISSING,
)
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import build_route_tool_context, get_tool_registry
from nerve.identity import Actor

router = APIRouter()

# The HTTP status of a publish that did not end, by its code. A refusal of
# the bundle is always 422 (also its ``not_found``: a path that does not
# exist); 404 ``not_found`` is the platform's word for an unknown artifact.
# Every other code (a refusal of the platform) is 422.
_STATUS = {
    INVALID_ARGUMENTS: 400,
    SERVER_MISSING: 409,
    SERVER_REFUSED: 409,
    PLATFORM_BUSY: 503,
    PLATFORM_UNAVAILABLE: 503,
    PLATFORM_ERROR: 502,
    UPLOAD_FAILED: 502,
    UPLOAD_ORIGIN_REFUSED: 502,
    UPLOAD_ORIGINS_MISSING: 409,
    "not_found": 404,
}


class PublishBody(BaseModel):
    path: str
    title: str
    artifact_id: str | None = None


@router.post("/api/artifacts/publish")
async def publish(body: PublishBody, actor: Actor = Depends(require_auth)):
    """Publish the bundle of ``path`` with ``title`` (a new version of
    ``artifact_id`` when it is given). The answer: ``artifact_id``,
    ``version``, ``viewer_url``, ``expires_at``, ``title`` and
    ``changed_after`` (a file changed after the publish read it: the
    version has the bytes that were read)."""
    spec = get_tool_registry().get(PUBLISH_TOOL)
    if spec is None:  # pragma: no cover - the registry always has the tool
        raise HTTPException(status_code=404, detail={"code": "not_found", "message": "Publish is off."})
    args = {"path": body.path, "title": body.title}
    if body.artifact_id:
        args["artifact_id"] = body.artifact_id
    result = await spec.handler(build_route_tool_context(actor=actor), args)
    structured = result.structured or {}
    if result.is_error:
        error = structured.get("error") or {}
        code = str(error.get("code") or PLATFORM_ERROR)
        message = str(error.get("message") or "The publish did not end.")
        status = 422 if error.get("bundle") is True else _STATUS.get(code, 422)
        raise HTTPException(status_code=status, detail={"code": code, "message": message})
    return structured
