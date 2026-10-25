"""File upload and download routes."""

from __future__ import annotations

import asyncio
import base64
import logging
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse

from nerve.config import get_config
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_auth)])

MAX_FILE_SIZE = 20 * 1024 * 1024  # 20 MB per file
MAX_TOTAL_SIZE = 50 * 1024 * 1024  # 50 MB per request

IMAGE_TYPES = {"image/png", "image/jpeg", "image/gif", "image/webp"}
PDF_TYPES = {"application/pdf"}


def _classify_file(media_type: str) -> str:
    """Classify an uploaded file as image, pdf, or text."""
    if media_type in IMAGE_TYPES:
        return "image"
    if media_type in PDF_TYPES:
        return "pdf"
    return "text"


def _uploads_dir() -> Path:
    """Return the uploads root directory."""
    config = get_config()
    return config.workspace / ".uploads"


def session_upload_dir(session_id: str) -> Path | None:
    """The directory for a session's uploads, or None if the id cannot name one.

    The session id comes from the client: ``/api/chat`` and the WebSocket
    create a session with any id that they get. Joined onto the uploads root
    as it is, ``..`` segments or an absolute path name a directory anywhere on
    the disk. The directory must resolve inside the uploads root.
    """
    root = _uploads_dir().resolve()
    try:
        candidate = (root / session_id).resolve()
    except (OSError, RuntimeError, ValueError):
        # A name that is too long, a symlink loop, a NUL byte: no directory.
        return None
    if candidate == root or not candidate.is_relative_to(root):
        return None
    return candidate


@router.post("/api/files/upload")
async def upload_files(
    files: list[UploadFile] = File(...),
    session_id: str = Form(...),
):
    """Upload one or more files, store on disk and track in DB."""
    deps = get_deps()

    # Validate session exists
    session = await deps.db.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    upload_dir = session_upload_dir(session_id)
    if upload_dir is None:
        raise HTTPException(
            status_code=400,
            detail="This session id cannot name an upload directory",
        )

    # Read all files and validate sizes
    file_data: list[tuple[UploadFile, bytes]] = []
    total_size = 0
    for f in files:
        data = await f.read()
        if len(data) > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=413,
                detail=f"File '{f.filename}' exceeds {MAX_FILE_SIZE // (1024*1024)}MB limit",
            )
        total_size += len(data)
        if total_size > MAX_TOTAL_SIZE:
            raise HTTPException(
                status_code=413,
                detail=f"Total upload size exceeds {MAX_TOTAL_SIZE // (1024*1024)}MB limit",
            )
        file_data.append((f, data))

    # Store files on disk and in DB
    upload_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for f, data in file_data:
        file_id = uuid.uuid4().hex[:16]
        filename = f.filename or "unnamed"
        # Sanitize filename
        filename = Path(filename).name  # strip directory components
        media_type = f.content_type or "application/octet-stream"
        file_type = _classify_file(media_type)

        disk_path = upload_dir / f"{file_id}_{filename}"
        await asyncio.to_thread(disk_path.write_bytes, data)

        await deps.db.save_uploaded_file(
            file_id=file_id,
            session_id=session_id,
            filename=filename,
            media_type=media_type,
            file_type=file_type,
            file_size=len(data),
            disk_path=str(disk_path),
        )

        results.append({
            "id": file_id,
            "filename": filename,
            "media_type": media_type,
            "file_type": file_type,
            "size": len(data),
        })

    return {"files": results}


@router.get("/api/files/uploads/{file_id}")
async def get_uploaded_file(
    file_id: str,
):
    """Serve an uploaded file by its ID (for image display in chat history)."""
    deps = get_deps()
    record = await deps.db.get_uploaded_file(file_id)
    if not record:
        raise HTTPException(status_code=404, detail="File not found")

    disk_path = Path(record["disk_path"])
    if not disk_path.exists():
        raise HTTPException(status_code=404, detail="File not found on disk")

    return FileResponse(
        path=str(disk_path),
        media_type=record["media_type"],
        filename=record["filename"],
    )


@router.get("/api/files/download")
async def download_file(
    path: str,
):
    """Download a workspace file by absolute path."""
    config = get_config()
    workspace_root = config.workspace.resolve()

    resolved = Path(path).resolve()
    if not resolved.is_relative_to(workspace_root):
        raise HTTPException(status_code=403, detail="Access denied: path outside workspace")

    if not resolved.exists() or not resolved.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    import mimetypes
    media_type, _ = mimetypes.guess_type(str(resolved))

    return FileResponse(
        path=str(resolved),
        media_type=media_type or "application/octet-stream",
        filename=resolved.name,
        headers={"Content-Disposition": f'attachment; filename="{resolved.name}"'},
    )
