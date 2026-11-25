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
The router is registered only with ``artifacts.preview`` on; the web UI
learns of it from ``GET /api/auth/me`` (``preview``). Until the gateway has
decided at startup that it serves previews, the routes answer 404
``preview_disabled``.

A file read does not walk the bundle again. The bundle route keeps the
description it answered, for its path, in a small cache
(``DescriptionCache``). The file route takes the fingerprint of that
description and uses the kept description when it is the same; otherwise it
describes the bundle once more. Each open still goes through
:func:`~nerve.artifacts.bundle.open_bundle_file`: no link followed on any
name, a regular file, within the size limit.
"""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
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


class DescriptionCache:
    """The newest description of a few bundles, by workspace and path.

    A description is kept for at most ``max_age`` seconds. It is used only
    for a request that names its fingerprint, so a file read takes the
    membership of the description that the web UI is building from.
    """

    def __init__(self, size: int = 16, max_age: float = 60.0) -> None:
        self.size = size
        self.max_age = max_age
        self._items: OrderedDict[tuple[str, str], tuple[float, Bundle]] = OrderedDict()

    def get(self, workspace: str, path: str, fingerprint: str) -> Bundle | None:
        item = self._items.get((workspace, path))
        if item is None:
            return None
        kept_at, bundle = item
        if time.monotonic() - kept_at > self.max_age or bundle.fingerprint != fingerprint:
            return None
        self._items.move_to_end((workspace, path))
        return bundle

    def put(self, workspace: str, path: str, bundle: Bundle) -> None:
        self._items[(workspace, path)] = (time.monotonic(), bundle)
        self._items.move_to_end((workspace, path))
        while len(self._items) > self.size:
            self._items.popitem(last=False)


descriptions = DescriptionCache()


async def _describe(path: str) -> Bundle:
    workspace = str(get_config().workspace)
    try:
        bundle = await asyncio.to_thread(describe_bundle, workspace, path)
    except BundleError as e:
        raise _refusal(e) from None
    descriptions.put(workspace, path, bundle)
    return bundle


async def _kept_or_described(path: str, fingerprint: str) -> Bundle:
    kept = descriptions.get(str(get_config().workspace), path, fingerprint) if fingerprint else None
    return kept if kept is not None else await _describe(path)


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


@router.get("/api/preview/bundle")
async def preview_bundle(path: str = ""):
    """The description of the bundle of ``path``: its entry, its files with
    size and type, and a fingerprint that changes when a file changes."""
    _require_enabled()
    return bundle_view(await _describe(path))


@router.get("/api/preview/file")
async def preview_file(path: str = "", file: str = "", fingerprint: str = ""):
    """The bytes of the file ``file`` of the bundle of ``path``. Only a file
    of the bundle's description, of at most ``PREVIEW_MAX_BYTES``.
    ``fingerprint`` names the description that the bundle route answered;
    when it is the kept one, the bundle is not described again."""
    _require_enabled()
    bundle = await _kept_or_described(path, fingerprint)
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
