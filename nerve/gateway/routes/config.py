"""Config / workspace-sync routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps

router = APIRouter()


@router.post("/api/config/sync")
async def sync_workspace_route(user: dict = Depends(require_auth)):
    """Pull the workspace from its git remote and apply changes (cron + MCP).

    Fast-forward only; validates the pulled bundle before applying.
    """
    import asyncio

    from nerve.config import get_config
    from nerve.gateway.server import _cron_service
    from nerve.sync_service import _apply_sync, sync_workspace

    config = get_config()
    # Hand over the raw configured values: sync_workspace coerces them inside its
    # own never-raises guard, so a `workspace` that is null or a list reports a
    # 400 rather than a 500 with a stack trace.
    result = await asyncio.to_thread(
        sync_workspace,
        config.workspace,
        config.config_dir or config.workspace,
        branch=config.workspace_sync.branch,
        validate=config.workspace_sync.validate,
        strict_env=config.workspace_sync.strict_env,
    )
    if not result.ok:
        raise HTTPException(
            status_code=400,
            detail={
                "message": result.message,
                "errors": result.validation_errors,
                "warnings": result.validation_warnings,
            },
        )
    if result.changed:
        deps = get_deps()
        await _apply_sync(deps.engine, _cron_service)
    return {
        "ok": True,
        "changed": result.changed,
        "message": result.message,
        "old_rev": result.old_rev,
        "new_rev": result.new_rev,
        "warnings": result.validation_warnings,
    }
