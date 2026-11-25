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

A file read never walks the bundle. The bundle route keeps each
description it answered in a small cache (``DescriptionCache``), by path
and fingerprint, so two states of one path can be kept at the same time.
The file route takes the fingerprint of the description that the web UI
builds from. When that description is not kept (too old, pushed out, never
answered), or when the file on disk is not the described file any more, the
answer is 409 ``stale_description``: the web UI reads the description again
and starts the build again. Each open goes through
:func:`~nerve.artifacts.bundle.open_bundle_file`: no link followed on any
name, a regular file, the same size, times, inode and device as described.
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


# How many descriptions the file route keeps. One entry is the description
# of one bundle state: the path, size, media type, times, inode and device
# of each of its files (at most 1000), so at most a few hundred KiB.
MAX_KEPT_DESCRIPTIONS = 64
# A kept description goes when it was not used for this long.
KEPT_DESCRIPTION_IDLE_SECONDS = 60.0


class DescriptionCache:
    """The descriptions that the bundle route answered last, by workspace,
    path and fingerprint: at most ``size`` of them. A description goes when
    it was not used for ``max_age`` seconds (each successful file read uses
    it again), or when a new one needs its place and it is the one used
    least recently. Two viewers that build from two states of one path each
    find their own. With more than ``size`` previews built at the same time
    a build can lose its description and start again."""

    def __init__(
        self, size: int = MAX_KEPT_DESCRIPTIONS, max_age: float = KEPT_DESCRIPTION_IDLE_SECONDS,
    ) -> None:
        self.size = size
        self.max_age = max_age
        self._items: OrderedDict[tuple[str, str, str], tuple[float, Bundle]] = OrderedDict()

    def get(self, workspace: str, path: str, fingerprint: str) -> Bundle | None:
        key = (workspace, path, fingerprint)
        item = self._items.get(key)
        if item is None:
            return None
        kept_at, bundle = item
        if time.monotonic() - kept_at > self.max_age:
            del self._items[key]
            return None
        self._items.move_to_end(key)
        return bundle

    def touch(self, workspace: str, path: str, fingerprint: str) -> None:
        """The description was used: its ``max_age`` starts again."""
        key = (workspace, path, fingerprint)
        item = self._items.get(key)
        if item is not None:
            self._items[key] = (time.monotonic(), item[1])
            self._items.move_to_end(key)

    def put(self, workspace: str, path: str, bundle: Bundle) -> None:
        key = (workspace, path, bundle.fingerprint)
        self._items[key] = (time.monotonic(), bundle)
        self._items.move_to_end(key)
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


STALE_DESCRIPTION = "stale_description"


def _stale() -> HTTPException:
    return HTTPException(
        status_code=409,
        detail={
            "code": STALE_DESCRIPTION,
            "message": "The files changed after the description. Read the description again.",
        },
    )


def _kept(path: str, fingerprint: str) -> Bundle:
    kept = descriptions.get(str(get_config().workspace), path, fingerprint) if fingerprint else None
    if kept is None:
        raise _stale()
    return kept


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
    """The bytes of the file ``file`` of the bundle of ``path``, for the
    description with ``fingerprint`` that the bundle route answered. Only a
    file of that description, of at most ``PREVIEW_MAX_BYTES``, and only
    while it is the described file: else 409 ``stale_description``."""
    _require_enabled()
    bundle = _kept(path, fingerprint)
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
        # No more than the described size: a write during the read is a change too.
        data = await asyncio.to_thread(read_bundle_file, bundle, file, found.size)
    except BundleError as e:
        if e.code in (BundleRefusal.CHANGED, BundleRefusal.FILE_TOO_LARGE):
            raise _stale() from None
        raise _refusal(e) from None
    if len(data) != found.size:
        raise _stale()
    descriptions.touch(str(get_config().workspace), path, fingerprint)
    return Response(data, media_type=found.media_type, headers=FILE_HEADERS)
