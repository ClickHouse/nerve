"""Config / workspace-sync routes."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException

from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps

router = APIRouter()


@router.post("/api/config/reload")
async def reload_config_route(user: dict = Depends(require_auth)):
    """Re-read config and hot-reload cron, sources, MCP, and skills (no restart).

    Apply config edits made on this box without restarting. Some settings
    (gateway host/port, provider/auth, agent backend) still require a restart —
    ``docs/config.md`` has the full list.

    ``ok`` is derived from what the subsystems actually reported, and ``errors``
    names the ones that failed. A reload is best-effort by design: an invalid
    ``settings.yaml`` must not stop a valid cron edit from being applied, so the
    other subsystems still run and the response is still 200 — but it says so
    rather than reporting a success it did not earn.

    ``restart_required`` lists config that changed but cannot take effect until
    the process restarts (see ``docs/config.md``). It is a separate field rather
    than a line in ``detail`` because a caller reading ``ok`` alone would treat it
    as fully applied. It is not in ``errors`` and does not affect ``ok``: nothing
    failed, and the reload applied everything it can.
    """
    from nerve.config import get_config
    from nerve.config_reload import reload_all, reload_failures
    from nerve.gateway.server import _cron_service

    config = get_config()
    deps = get_deps()
    config_dir = Path(config.config_dir) if config.config_dir else Path(config.workspace)
    summary = await reload_all(deps.engine, _cron_service, config_dir)
    failures = reload_failures(summary)
    return {
        "ok": not failures,
        "detail": summary,
        "errors": failures,
        "restart_required": summary.get("restart_required", ""),
    }


@router.post("/api/config/sync")
async def sync_workspace_route(user: dict = Depends(require_auth)):
    """Pull the workspace from its git remote and apply what it merged.

    Fast-forward only; validates the pulled bundle before applying. Applying runs
    the unified reload: the config object and the services holding their own
    reference, then cron jobs, cron sources, MCP servers and skills.

    ``ok`` is scored differently here than on ``/api/config/reload``, which runs
    the same reload: it answers "did the merge take", so it stays true when the
    merged config loaded and only some later subsystem stumbled. That case is
    ``applied: false`` with the reason in ``reload_errors``. Callers who want
    "did everything apply" should read ``applied``.
    """
    import asyncio

    from nerve.config import get_config
    from nerve.config_reload import reload_failures
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
        locked=config.lockdown,
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
    summary: dict = {}
    if result.changed:
        deps = get_deps()
        config_dir = Path(config.config_dir) if config.config_dir else Path(config.workspace)
        summary = await _apply_sync(deps.engine, _cron_service, config_dir)
    failures = reload_failures(summary)
    # The merge is a real state change, so this is not a 4xx — but a merge whose
    # config the daemon then refused to load has applied nothing, and reporting
    # ok for it would tell an operator who just enabled lockdown that the box is
    # locked while its write guards are still open. A subsystem that failed after
    # the config loaded is less severe (the merged settings *are* in effect) but
    # still leaves the daemon only partly on the new config, so it keeps `ok` and
    # clears `applied` rather than being folded into either.
    apply_error = failures.get("config")
    return {
        "ok": apply_error is None,
        "changed": result.changed,
        "applied": result.changed and not failures,
        "apply_error": apply_error,
        "reload": summary,
        "reload_errors": failures,
        "message": result.message,
        "old_rev": result.old_rev,
        "new_rev": result.new_rev,
        "warnings": result.validation_warnings,
    }
