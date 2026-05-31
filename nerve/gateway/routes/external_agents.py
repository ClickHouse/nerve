"""REST routes for external-agent configuration + sync.

Endpoints:

- ``GET /api/external-agents`` — list configured agents with status
- ``POST /api/external-agents/sync`` — manually trigger a sync sweep
- ``POST /api/external-agents/{name}/disable`` — pause sync for one agent
- ``POST /api/external-agents/{name}/enable`` — resume sync
- ``DELETE /api/external-agents/{name}`` — remove an agent target

Token revocation is a no-op in v1 because we reuse the single gateway
JWT; the endpoint is exposed anyway so the UI can surface "revoke"
once per-agent tokens land in a follow-up.
"""

from __future__ import annotations

import logging
from typing import Any

import yaml
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from nerve.config import get_config, set_config
from nerve.external_agents.registry import AGENT_REGISTRY
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/external-agents", tags=["external-agents"])


class ToggleResponse(BaseModel):
    name: str
    enabled: bool


@router.get("")
async def list_external_agents(user: dict = Depends(require_auth)) -> dict[str, Any]:
    """Return per-agent status + the registry of available agents.

    The frontend uses ``available`` to render the "Add agent" picker
    (so it can show unconfigured agents alongside the configured ones)
    and ``configured`` to render the live status table.
    """
    config = get_config()
    deps = get_deps()
    sync = deps.external_agents_sync

    available: list[dict] = []
    for agent in AGENT_REGISTRY.values():
        version = agent.smoke_check()
        available.append({
            "name": agent.name,
            "display_name": agent.display_name,
            "cli_command": agent.cli_command,
            "cli_installed": version is not None,
            "cli_version": version,
            "config_paths": [str(p) for p in agent.default_config_paths()],
        })

    status: dict[str, Any] = {}
    if sync is not None:
        status = sync.status_for_api()

    configured: list[dict] = []
    for target in config.external_agents.targets:
        agent_status = status.get(target.name, {})
        configured.append({
            "name": target.name,
            "enabled": target.enabled,
            **agent_status,
        })

    return {
        "enabled": config.external_agents.enabled,
        "sync_interval_minutes": config.external_agents.sync_interval_minutes,
        "conflict_policy": config.external_agents.conflict_policy,
        "available": available,
        "configured": configured,
    }


@router.post("/sync")
async def trigger_sync(user: dict = Depends(require_auth)) -> dict[str, Any]:
    """Run one sync sweep right now (instead of waiting for the timer).

    Returns the fresh status map so the UI can refresh without an extra
    GET round-trip.
    """
    deps = get_deps()
    sync = deps.external_agents_sync
    if sync is None:
        raise HTTPException(
            status_code=503,
            detail="External-agents sync service is not running.",
        )
    try:
        await sync.run_once()
    except Exception as e:
        logger.exception("Manual sync sweep failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Sync failed: {e}")
    return {"status": "ok", "agents": sync.status_for_api()}


@router.post("/{name}/disable", response_model=ToggleResponse)
async def disable_agent(name: str, user: dict = Depends(require_auth)) -> ToggleResponse:
    """Pause sync for ``name`` without removing it from config."""
    return _toggle_agent(name, enabled=False)


@router.post("/{name}/enable", response_model=ToggleResponse)
async def enable_agent(name: str, user: dict = Depends(require_auth)) -> ToggleResponse:
    """Re-enable a previously paused agent."""
    return _toggle_agent(name, enabled=True)


@router.delete("/{name}")
async def remove_agent(name: str, user: dict = Depends(require_auth)) -> dict[str, str]:
    """Remove ``name`` from the configured targets list.

    Does NOT delete the agent's config files or memory bundle — the
    user can re-add later without re-bootstrapping. To wipe the files
    too, they should delete them manually.
    """
    config = get_config()
    before = len(config.external_agents.targets)
    config.external_agents.targets = [
        t for t in config.external_agents.targets if t.name != name
    ]
    if len(config.external_agents.targets) == before:
        raise HTTPException(status_code=404, detail=f"Agent {name!r} not configured")

    _persist_external_agents_yaml(config)
    set_config(config)
    return {"status": "removed", "name": name}


def _toggle_agent(name: str, *, enabled: bool) -> ToggleResponse:
    config = get_config()
    target = next(
        (t for t in config.external_agents.targets if t.name == name),
        None,
    )
    if target is None:
        raise HTTPException(status_code=404, detail=f"Agent {name!r} not configured")
    target.enabled = enabled
    _persist_external_agents_yaml(config)
    set_config(config)
    return ToggleResponse(name=name, enabled=enabled)


def _persist_external_agents_yaml(config) -> None:
    """Write the external_agents block back to config.yaml.

    We persist to the *committed* config.yaml (not local) because the
    target list is plumbing, not a secret — keeping it in the
    user-visible config lets the user audit and edit it directly.
    """
    from pathlib import Path

    # Reuse the same config dir resolution the bootstrap wizard uses.
    # The path stamp lives in ``~/.nerve/config_dir`` after the daemon
    # starts via ``nerve start``.
    config_dir_marker = Path("~/.nerve/config_dir").expanduser()
    if config_dir_marker.exists():
        try:
            config_dir = Path(config_dir_marker.read_text().strip())
        except OSError:
            config_dir = Path.cwd()
    else:
        config_dir = Path.cwd()

    yaml_path = config_dir / "config.yaml"
    raw: dict[str, Any] = {}
    if yaml_path.exists():
        with open(yaml_path) as f:
            raw = yaml.safe_load(f) or {}

    raw["external_agents"] = {
        "enabled": config.external_agents.enabled,
        "sync_interval_minutes": config.external_agents.sync_interval_minutes,
        "conflict_policy": config.external_agents.conflict_policy,
        "targets": [t.to_dict() for t in config.external_agents.targets],
    }

    yaml_path.write_text(yaml.safe_dump(raw, sort_keys=False))
