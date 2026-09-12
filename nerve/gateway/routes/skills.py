"""Skill routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps
from nerve.identity import Actor

router = APIRouter()


class SkillCreateRequest(BaseModel):
    name: str
    description: str
    content: str = ""
    version: str = "1.0.0"


class SkillUpdateRequest(BaseModel):
    content: str


class SkillToggleRequest(BaseModel):
    enabled: bool


def _require_skill_manager():
    deps = get_deps()
    if not deps.engine or not hasattr(deps.engine, '_skill_manager') or not deps.engine._skill_manager:
        raise HTTPException(status_code=503, detail="Skills system not available")
    return deps.engine._skill_manager


@router.get("/api/skills")
async def list_skills(actor: Actor = Depends(require_auth)):
    """List all skills with usage stats."""
    deps = get_deps()
    skills = await deps.db.get_all_skills_with_stats()
    return {"skills": skills}


@router.get("/api/skills/stats")
async def skill_stats(actor: Actor = Depends(require_auth)):
    """Aggregate usage stats across all skills."""
    deps = get_deps()
    stats = await deps.db.get_skill_stats()
    return {"stats": stats}


@router.post("/api/skills/sync")
async def sync_skills(actor: Actor = Depends(require_auth)):
    """Re-scan filesystem and sync skills to DB."""
    mgr = _require_skill_manager()
    skills = await mgr.discover()
    return {"synced": len(skills), "skills": [{"id": s.id, "name": s.name} for s in skills]}


@router.get("/api/skills/{skill_id}")
async def get_skill_detail(skill_id: str, actor: Actor = Depends(require_auth)):
    """Get full skill content + metadata + usage stats."""
    deps = get_deps()
    mgr = _require_skill_manager()
    skill = await mgr.get_skill(skill_id)
    if not skill:
        raise HTTPException(status_code=404, detail="Skill not found")

    db_row = await deps.db.get_skill_row(skill_id)
    stats = await deps.db.get_skill_stats(skill_id)
    usage = await deps.db.get_skill_usage(skill_id, limit=20)
    refs = await mgr.list_references(skill_id)

    return {
        "id": skill.id,
        "name": skill.name,
        "description": skill.description,
        "version": skill.version,
        "enabled": db_row.get("enabled", True) if db_row else True,
        "user_invocable": skill.user_invocable,
        "model_invocable": skill.model_invocable,
        "allowed_tools": skill.allowed_tools,
        "has_references": skill.has_references,
        "has_scripts": skill.has_scripts,
        "has_assets": skill.has_assets,
        "content": skill.content,
        "raw": skill.raw,
        "references": refs,
        "stats": stats[0] if stats else {"total_invocations": 0, "success_count": 0, "avg_duration_ms": None, "last_used": None},
        "recent_usage": usage,
        "created_at": db_row.get("created_at") if db_row else None,
        "updated_at": db_row.get("updated_at") if db_row else None,
    }


@router.post("/api/skills")
async def create_skill(req: SkillCreateRequest, actor: Actor = Depends(require_auth)):
    """Create a new skill."""
    mgr = _require_skill_manager()
    skill = await mgr.create_skill(
        name=req.name, description=req.description,
        content=req.content, version=req.version,
    )
    return {"id": skill.id, "name": skill.name, "created": True}


@router.put("/api/skills/{skill_id}")
async def update_skill(skill_id: str, req: SkillUpdateRequest, actor: Actor = Depends(require_auth)):
    """Update a skill's SKILL.md content."""
    mgr = _require_skill_manager()
    skill = await mgr.update_skill(skill_id, req.content)
    if not skill:
        raise HTTPException(status_code=404, detail="Skill not found")
    return {"id": skill.id, "name": skill.name, "updated": True}


@router.delete("/api/skills/{skill_id}")
async def delete_skill(skill_id: str, actor: Actor = Depends(require_auth)):
    """Delete a skill."""
    mgr = _require_skill_manager()
    await mgr.delete_skill(skill_id)
    return {"id": skill_id, "deleted": True}


@router.patch("/api/skills/{skill_id}/toggle")
async def toggle_skill(skill_id: str, req: SkillToggleRequest, actor: Actor = Depends(require_auth)):
    """Enable or disable a skill."""
    mgr = _require_skill_manager()
    success = await mgr.toggle_skill(skill_id, req.enabled)
    if not success:
        raise HTTPException(status_code=404, detail="Skill not found")
    return {"id": skill_id, "enabled": req.enabled}


@router.get("/api/skills/{skill_id}/usage")
async def get_skill_usage(skill_id: str, limit: int = 50, actor: Actor = Depends(require_auth)):
    """Get usage history for a skill."""
    deps = get_deps()
    usage = await deps.db.get_skill_usage(skill_id, limit=min(limit, 200))
    stats = await deps.db.get_skill_stats(skill_id)
    return {
        "usage": usage,
        "stats": stats[0] if stats else {"total_invocations": 0, "success_count": 0, "avg_duration_ms": None, "last_used": None},
    }
