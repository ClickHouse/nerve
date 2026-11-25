"""Preview routes: what the Preview tab of the web UI reads (see
``docs/web-ui.md``, "Preview").

The tab shows the bundle of a workspace path (``nerve/artifacts/bundle.py``).
It reads the description of the bundle, then the bytes of the files that
the page uses, and builds one document from them in the browser. That
document runs in the sandbox frame, never as a page of this address.

``path`` is the path of the bundle: relative to the workspace, or an
absolute path below it. ``file`` is the path of one file, relative to the
root of the bundle.

Errors are ``{"detail": {"code": "<code>", "message": "<sentence>"}}``.
While ``artifacts.preview`` is off, the bundle and file routes answer 404
``preview_disabled``.
"""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response

from nerve.artifacts.bundle import (
    MIB,
    Bundle,
    BundleError,
    BundleRefusal,
    describe_bundle,
    read_bundle_file,
)
from nerve.config import get_config
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps

router = APIRouter(dependencies=[Depends(require_auth)])

# The files of one preview together, and so also one file. The web UI shows
# a message for a preview above it.
PREVIEW_MAX_BYTES = 10 * MIB

# A file of a bundle is data for the web UI, never a page of this address:
# opened directly, it is a download, and it runs nothing.
FILE_HEADERS = {
    "Content-Disposition": "attachment",
    "Content-Security-Policy": "sandbox; default-src 'none'",
    "X-Content-Type-Options": "nosniff",
    "Cache-Control": "no-store",
}

_STATUS = {
    BundleRefusal.NOT_FOUND: 404,
    BundleRefusal.OUTSIDE_WORKSPACE: 403,
}


def _serving() -> bool:
    try:
        return bool(get_deps().preview_serving)
    except RuntimeError:  # before startup
        return False


def _require_enabled() -> None:
    if not _serving():
        raise HTTPException(status_code=404, detail={"code": "preview_disabled", "message": "Preview is off."})


def _refusal(e: BundleError) -> HTTPException:
    return HTTPException(status_code=_STATUS.get(e.code, 422), detail={"code": e.code.value, "message": e.message})


async def _describe(path: str) -> Bundle:
    try:
        return await asyncio.to_thread(describe_bundle, get_config().workspace, path)
    except BundleError as e:
        raise _refusal(e) from None


def bundle_view(bundle: Bundle) -> dict[str, Any]:
    """The description of a bundle as the API answers it."""
    return {
        "path": bundle.path,
        "root": bundle.root,
        "root_path": str(bundle.root_path),
        "kind": bundle.kind.value,
        "entry": bundle.entry,
        "files": [{"path": f.path, "size": f.size, "type": f.media_type} for f in bundle.files],
        "size": bundle.size,
        "fingerprint": bundle.fingerprint,
        "modified": bundle.modified_ns // 1_000_000,
    }


@router.get("/api/preview/config")
async def preview_config():
    """Whether this instance shows the Preview tab, and the most bytes that
    the files of one preview may have."""
    return {"enabled": _serving(), "max_bytes": PREVIEW_MAX_BYTES}


@router.get("/api/preview/bundle")
async def preview_bundle(path: str = ""):
    """The description of the bundle of ``path``: its entry, its files with
    size and type, and a fingerprint that changes when a file changes."""
    _require_enabled()
    return bundle_view(await _describe(path))


@router.get("/api/preview/file")
async def preview_file(path: str = "", file: str = ""):
    """The bytes of the file ``file`` of the bundle of ``path``. Only a file
    of the bundle's description, of at most ``PREVIEW_MAX_BYTES``."""
    _require_enabled()
    bundle = await _describe(path)
    found = bundle.file(file)
    if found is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "not_in_bundle", "message": "The file is not part of the bundle."},
        )
    if found.size > PREVIEW_MAX_BYTES:
        raise HTTPException(
            status_code=413,
            detail={"code": "too_large", "message": f"The file is larger than {PREVIEW_MAX_BYTES // MIB} MiB."},
        )
    try:
        data = await asyncio.to_thread(read_bundle_file, bundle, file, PREVIEW_MAX_BYTES)
    except BundleError as e:
        if e.code is BundleRefusal.FILE_TOO_LARGE:
            raise HTTPException(status_code=413, detail={"code": "too_large", "message": e.message}) from None
        raise _refusal(e) from None
    return Response(data, media_type=found.media_type, headers=FILE_HEADERS)
