"""Codex operator/preflight and worker-token endpoints."""

from __future__ import annotations

import re

from fastapi import APIRouter, Depends, HTTPException, Request

from nerve.agent.backends.codex.ultracode import (
    installation_status,
    list_dashboard_runs,
    read_dashboard_run,
    recoverable_runs,
)
from nerve.gateway.auth import (
    MCP_WORKER_CLAIM,
    create_mcp_session_token,
    require_auth,
)
from nerve.gateway.routes._deps import get_deps
from nerve.mcp_server.auth import McpAuthError, authenticate_mcp, bound_session_id

router = APIRouter()
_WORKER_ID_RE = re.compile(r"^ultracode-[a-f0-9]{16}$")


def _dashboard_deps():
    deps = get_deps()
    ultracode = deps.engine.config.codex.ultracode
    if not (ultracode.enabled and ultracode.dashboard):
        # A 404 lets old/current frontends feature-detect this optional page
        # without advertising a disabled operator surface.
        raise HTTPException(status_code=404, detail="Ultracode dashboard is disabled")
    return deps


@router.post("/api/codex/worker-token")
async def mint_worker_token(request: Request):
    """Exchange a parent session token for a worker-scoped two-hour token."""
    deps = get_deps()
    body = await request.json()
    worker_id = str(body.get("worker_id") or "")
    if not _WORKER_ID_RE.fullmatch(worker_id):
        raise HTTPException(status_code=400, detail="Invalid Ultracode worker id")
    secret = deps.engine.config.auth.jwt_secret
    if not secret:
        # Development mode has no credential to exchange. Keep workers
        # functional; tool calls follow the endpoint's existing unauthenticated
        # satellite attribution policy.
        return {"token": "", "worker_id": worker_id, "expires_in": 0}
    try:
        payload = authenticate_mcp(request.scope, deps.engine.config)
    except McpAuthError as e:
        raise HTTPException(status_code=401, detail=str(e)) from e
    session_id = bound_session_id(payload)
    if not session_id or (payload or {}).get(MCP_WORKER_CLAIM):
        raise HTTPException(status_code=403, detail="Parent MCP session token required")
    token = create_mcp_session_token(
        secret, session_id, ttl_seconds=2 * 60 * 60, worker_id=worker_id,
    )
    return {"token": token, "worker_id": worker_id, "expires_in": 7200}


@router.get("/api/codex/status")
async def codex_status(user: dict = Depends(require_auth)):
    deps = get_deps()
    backend = deps.engine._backends.get("codex")
    preflight = await backend.preflight() if backend is not None else {
        "available": False, "reason": "Codex backend is not registered",
    }
    return {
        "preflight": preflight,
        "ultracode": installation_status(deps.engine.config),
        "recoverable_runs": recoverable_runs(deps.engine.config),
    }


@router.get("/api/codex/ultracode/dashboard")
async def ultracode_dashboard_status(user: dict = Depends(require_auth)):
    deps = _dashboard_deps()
    status = installation_status(deps.engine.config)
    return {
        "enabled": True,
        "installed": bool(status.get("installed")),
        "version": status.get("version"),
        "read_only": True,
        # Explicitly distinguish this safe renderer from upstream's detached
        # unauthenticated mutation server.
        "upstream_ui": False,
    }


@router.get("/api/codex/ultracode/runs")
async def ultracode_dashboard_runs(
    limit: int = 50,
    user: dict = Depends(require_auth),
):
    deps = _dashboard_deps()
    return {"runs": list_dashboard_runs(deps.engine.config, limit=limit)}


@router.get("/api/codex/ultracode/runs/{workflow_id}")
async def ultracode_dashboard_run(
    workflow_id: str,
    user: dict = Depends(require_auth),
):
    deps = _dashboard_deps()
    run = read_dashboard_run(deps.engine.config, workflow_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Ultracode run not found")
    return {"run": run}
