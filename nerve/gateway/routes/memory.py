"""Memory file and memU routes."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel

from nerve.config import get_config
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps

logger = logging.getLogger(__name__)

router = APIRouter()


# --- Memory files ---

def _list_memory_files_sync(workspace: Path) -> list[dict]:
    """Walk + stat memory markdown files.  Sync — call via asyncio.to_thread."""
    files = []
    memory_dir = workspace / "memory"
    if memory_dir.exists():
        for f in sorted(memory_dir.rglob("*.md")):
            rel = f.relative_to(workspace)
            st = f.stat()
            files.append({
                "path": str(rel),
                "name": f.name,
                "size": st.st_size,
                "modified": datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat(),
            })

    # Also include root-level md files
    for f in sorted(workspace.glob("*.md")):
        st = f.stat()
        files.append({
            "path": f.name,
            "name": f.name,
            "size": st.st_size,
            "modified": datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat(),
        })

    return files


@router.get("/api/memory/files")
async def list_memory_files(user: dict = Depends(require_auth)):
    """List markdown files in workspace memory directory."""
    config = get_config()
    files = await asyncio.to_thread(_list_memory_files_sync, config.workspace)
    return {"files": files}


@router.get("/api/memory/file/{file_path:path}")
async def read_memory_file(file_path: str, user: dict = Depends(require_auth)):
    config = get_config()
    full_path = config.workspace / file_path
    # Prevent path traversal
    try:
        full_path.resolve().relative_to(config.workspace.resolve())
    except ValueError:
        raise HTTPException(status_code=403, detail="Access denied")
    if not full_path.exists() or not full_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    content = await asyncio.to_thread(full_path.read_text, encoding="utf-8")
    return {"path": file_path, "content": content}


class FileWriteRequest(BaseModel):
    content: str


@router.put("/api/memory/file/{file_path:path}")
async def write_memory_file(file_path: str, req: FileWriteRequest, user: dict = Depends(require_auth)):
    config = get_config()
    full_path = config.workspace / file_path
    try:
        full_path.resolve().relative_to(config.workspace.resolve())
    except ValueError:
        raise HTTPException(status_code=403, detail="Access denied")

    def _write() -> None:
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text(req.content, encoding="utf-8")

    await asyncio.to_thread(_write)
    return {"path": file_path, "saved": True}


# --- memU semantic memory ---

def _require_memu():
    deps = get_deps()
    if not deps.engine or not hasattr(deps.engine, '_memory_bridge') or not deps.engine._memory_bridge or not deps.engine._memory_bridge.available:
        raise HTTPException(status_code=503, detail="Memory service not available")
    return deps.engine._memory_bridge


def _read_memu_snapshot_sync(db_path: str) -> str:
    """Read the full memU snapshot and JSON-encode it.

    Sync — call via asyncio.to_thread.  With 20K+ items this is a multi-MB
    payload; both the table scans AND the json.dumps are deliberately done
    in the worker thread so the event loop never pays for them.
    """
    db = sqlite3.connect(db_path, timeout=10)
    db.row_factory = sqlite3.Row
    try:
        categories = []
        for row in db.execute("SELECT id, name, description, summary FROM memu_memory_categories ORDER BY name"):
            categories.append({
                "id": row["id"],
                "name": row["name"],
                "description": row["description"],
                "summary": row["summary"],
            })

        items = []
        for row in db.execute("SELECT id, memory_type, summary, resource_id, created_at, happened_at FROM memu_memory_items ORDER BY created_at DESC"):
            items.append({
                "id": row["id"],
                "memory_type": row["memory_type"],
                "summary": row["summary"],
                "resource_id": row["resource_id"],
                "created_at": row["created_at"],
                "happened_at": row["happened_at"],
            })

        resources = []
        for row in db.execute("SELECT id, url, modality, caption, created_at FROM memu_resources ORDER BY created_at DESC"):
            resources.append({
                "id": row["id"],
                "url": row["url"],
                "modality": row["modality"],
                "caption": row["caption"],
                "created_at": row["created_at"],
            })

        # Category-item links
        cat_items: dict[str, list[str]] = {}
        for row in db.execute("SELECT category_id, item_id FROM memu_category_items"):
            cat_items.setdefault(row["category_id"], []).append(row["item_id"])
    finally:
        db.close()

    return json.dumps({
        "available": True,
        "categories": categories,
        "items": items,
        "resources": resources,
        "category_items": cat_items,
    })


@router.get("/api/memory/memu")
async def get_memu_data(user: dict = Depends(require_auth)):
    """Get memU categories and items for the memory UI."""
    deps = get_deps()
    if not deps.engine or not hasattr(deps.engine, '_memory_bridge') or not deps.engine._memory_bridge or not deps.engine._memory_bridge.available:
        return {"available": False, "categories": [], "items": [], "resources": []}

    config = get_config()
    dsn = config.memory.sqlite_dsn
    # Extract file path from sqlite:///path
    db_path = dsn.replace("sqlite:///", "")

    try:
        payload = await asyncio.to_thread(_read_memu_snapshot_sync, db_path)
        return Response(content=payload, media_type="application/json")
    except Exception as e:
        logger.error("Failed to read memU data: %s", e)
        return {"available": False, "categories": [], "items": [], "resources": [], "error": str(e)}


class CategoryCreateRequest(BaseModel):
    name: str
    description: str = ""


class ItemUpdateRequest(BaseModel):
    content: str | None = None
    memory_type: str | None = None
    categories: list[str] | None = None


class CategoryUpdateRequest(BaseModel):
    summary: str | None = None
    description: str | None = None


@router.post("/api/memory/memu/categories")
async def create_memu_category(req: CategoryCreateRequest, user: dict = Depends(require_auth)):
    """Create a new memU memory category at runtime."""
    bridge = _require_memu()
    success = await bridge.create_category(req.name, req.description, source="web_ui")
    if not success:
        raise HTTPException(status_code=500, detail="Failed to create category")
    return {"name": req.name, "created": True}


@router.patch("/api/memory/memu/items/{item_id}")
async def update_memu_item(item_id: str, req: ItemUpdateRequest, user: dict = Depends(require_auth)):
    """Update a memU memory item's content, type, or categories."""
    bridge = _require_memu()
    if req.content is None and req.memory_type is None and req.categories is None:
        raise HTTPException(status_code=400, detail="Nothing to update")
    success = await bridge.update_item(
        memory_id=item_id,
        content=req.content,
        memory_type=req.memory_type,
        categories=req.categories,
        source="web_ui",
    )
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update item")
    return {"id": item_id, "updated": True}


@router.delete("/api/memory/memu/items/{item_id}")
async def delete_memu_item(item_id: str, user: dict = Depends(require_auth)):
    """Delete a memU memory item."""
    bridge = _require_memu()
    success = await bridge.delete_item(memory_id=item_id, source="web_ui")
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete item")
    return {"id": item_id, "deleted": True}


@router.patch("/api/memory/memu/categories/{category_id}")
async def update_memu_category(category_id: str, req: CategoryUpdateRequest, user: dict = Depends(require_auth)):
    """Update a memU category's summary or description (re-embeds)."""
    bridge = _require_memu()
    if req.summary is None and req.description is None:
        raise HTTPException(status_code=400, detail="Nothing to update")
    success = await bridge.update_category(
        category_id=category_id, summary=req.summary, description=req.description,
        source="web_ui",
    )
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update category")
    return {"id": category_id, "updated": True}


@router.get("/api/memory/memu/audit")
async def get_memu_audit_log(
    action: str = "", target_type: str = "", limit: int = 100, offset: int = 0,
    user: dict = Depends(require_auth),
):
    """Paginated audit log for memU operations."""
    deps = get_deps()
    logs = await deps.db.get_audit_logs(
        action=action or None, target_type=target_type or None,
        limit=min(limit, 500), offset=offset,
    )
    return {"logs": logs, "offset": offset, "limit": limit}


@router.get("/api/memory/memu/health")
async def memu_health(user: dict = Depends(require_auth)):
    """memU memory service health and metrics."""
    deps = get_deps()
    if not deps.engine or not deps.engine._memory_bridge:
        return {"service_available": False}
    return await deps.engine._memory_bridge.get_health()
