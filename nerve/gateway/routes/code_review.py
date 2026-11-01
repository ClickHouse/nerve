"""Authenticated Code Review board and immutable version history."""

import json
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query

from nerve.code_review import get_service
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps

router = APIRouter(dependencies=[Depends(require_auth)])


@router.get("/api/code-review/status")
async def status():
    service = get_service()
    if service:
        return await service.status()
    return {"board_name": get_deps().engine.config.code_review.board_name,
            "running": False, "worker_available": False, "profiles": []}


@router.post("/api/code-review/profiles/{profile_id}/pause")
async def pause(profile_id: str):
    service = get_service()
    if not service:
        raise HTTPException(503, "Code Review controller is unavailable")
    try:
        await service.pause(profile_id)
    except KeyError:
        raise HTTPException(404, "Unknown review profile") from None
    return await service.status()


@router.post("/api/code-review/profiles/{profile_id}/resume")
async def resume(profile_id: str):
    service = get_service()
    if not service:
        raise HTTPException(503, "Code Review controller is unavailable")
    try:
        await service.resume(profile_id)
    except KeyError:
        raise HTTPException(404, "Unknown review profile") from None
    except ValueError as error:
        raise HTTPException(409, str(error)) from None
    return await service.status()


@router.get("/api/code-review/board")
async def board(column: Literal["active", "archive"] = "active", profile: str = "",
                limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0)):
    db = get_deps().db
    clause = "state='open'" if column == "active" else "state!='open'"
    args = []
    if profile:
        clause += " AND profile=?"
        args.append(profile)
    async with db._read_snapshot():
        total = await db.cr_one(f"SELECT COUNT(*) total FROM code_review_prs WHERE {clause}", args)
        rows = await db.cr_rows(
            f"SELECT * FROM code_review_prs WHERE {clause} ORDER BY content_updated_at DESC,id LIMIT ? OFFSET ?",
            [*args, limit, offset],
        )
    return {"items": rows, "total": total["total"], "offset": offset}


@router.get("/api/code-review/prs/{pr_id}")
async def detail(pr_id: str, limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0)):
    db = get_deps().db
    pr = await db.cr_one("SELECT * FROM code_review_prs WHERE id=?", (pr_id,))
    if not pr:
        raise HTTPException(404, "Review PR not found")
    versions = await db.cr_rows(
        """SELECT v.id,v.head,v.base,v.status,v.created_at,v.updated_at,v.error,
        COALESCE((SELECT SUM(a.spent) FROM code_review_attempts a WHERE a.version_id=v.id),0) spent_usd
        FROM code_review_versions v WHERE pr_id=? ORDER BY created_at DESC,id LIMIT ? OFFSET ?""", (pr_id, limit, offset),
    )
    count = await db.cr_one("SELECT COUNT(*) total FROM code_review_versions WHERE pr_id=?", (pr_id,))
    return {"pr": pr, "versions": versions, "total": count["total"]}


@router.get("/api/code-review/prs/{pr_id}/versions/{version_id}")
async def version(pr_id: str, version_id: str):
    deps = get_deps()
    row = await deps.db.cr_one("SELECT * FROM code_review_versions WHERE id=? AND pr_id=?", (version_id, pr_id))
    if not row:
        raise HTTPException(404, "Review version not found")
    attempts = await deps.db.cr_rows("SELECT * FROM code_review_attempts WHERE version_id=? ORDER BY created_at", (version_id,))
    root = (Path(deps.engine.config.workspace) / "memory" / "code-reviews").resolve()
    reports = []
    for attempt in attempts:
        if not attempt["artifact_path"]:
            continue
        path = (root / attempt["artifact_path"]).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise HTTPException(409, "Review artifact is unavailable")
        report = json.loads(path.read_text())
        if attempt["artifact_path"] == row["artifact_path"]:
            report["admission"] = row["status"]
            report["error"] = row["error"]
        report["spent_usd"] = attempt["spent"]
        report["uncertain_usd"] = attempt["uncertain"]
        reports.append(report)
    return {"version": {**row, "snapshot": json.loads(row["snapshot"])}, "attempts": attempts, "reports": reports}
