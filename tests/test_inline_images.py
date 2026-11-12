"""Raster images open in a browser tab; everything else still downloads.

Starlette's ``FileResponse`` makes any file with a ``filename`` an
attachment, so a click on an image in the chat saved it to disk instead of
showing it. A raster image is now sent inline. An SVG or an HTML file stays an
attachment: opened as a document of the panel's origin, a script in it could
read the session token.
"""

from __future__ import annotations

import types

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import nerve.config as cfg
import nerve.gateway.routes._deps as deps_mod
from nerve.config import FilesConfig, NerveConfig
from nerve.gateway.auth import require_auth

_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16
_SVG = b'<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>'


def _disposition(r) -> str:
    return r.headers["content-disposition"].split(";")[0].strip()


@pytest.fixture
def ws(tmp_path, monkeypatch):
    ws = tmp_path / "ws"
    ws.mkdir()
    monkeypatch.setattr(cfg, "_config", NerveConfig(workspace=ws, files=FilesConfig()))
    return ws


class _FakeDb:
    def __init__(self, rows):
        self.rows = rows

    async def get_uploaded_file(self, file_id):
        return self.rows.get(file_id)


@pytest.fixture
def uploads(ws, monkeypatch):
    from nerve.gateway.routes.files import router

    store = ws / ".uploads" / "s1"
    store.mkdir(parents=True)
    rows = {}
    for fid, name, media_type, data in [
        ("png", "shot.png", "image/png", _PNG),
        ("svg", "chart.svg", "image/svg+xml", _SVG),
        ("html", "page.html", "text/html", b"<script>alert(1)</script>"),
        # A client may declare any type: the bytes are HTML, the type says PNG.
        ("liar", "x.png", "image/png", b"<html><script>alert(1)</script></html>"),
    ]:
        path = store / f"{fid}_{name}"
        path.write_bytes(data)
        rows[fid] = {"disk_path": str(path), "media_type": media_type, "filename": name}
    monkeypatch.setattr(deps_mod, "_deps", types.SimpleNamespace(db=_FakeDb(rows)))
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[require_auth] = lambda: None
    return TestClient(app)


class TestUploadedFile:
    def test_a_raster_image_is_inline(self, uploads):
        r = uploads.get("/api/files/uploads/png")
        assert r.status_code == 200 and r.content == _PNG
        assert r.headers["content-type"] == "image/png"
        assert _disposition(r) == "inline"
        assert 'filename="shot.png"' in r.headers["content-disposition"]
        assert r.headers["x-content-type-options"] == "nosniff"
        assert "sandbox" in r.headers["content-security-policy"]
        assert "default-src 'none'" in r.headers["content-security-policy"]

    def test_download_makes_an_image_an_attachment(self, uploads):
        r = uploads.get("/api/files/uploads/png", params={"download": "1"})
        assert _disposition(r) == "attachment"

    @pytest.mark.parametrize("fid", ["svg", "html"])
    def test_script_capable_types_stay_attachments(self, uploads, fid):
        r = uploads.get(f"/api/files/uploads/{fid}")
        assert r.status_code == 200
        assert _disposition(r) == "attachment"
        assert r.headers["x-content-type-options"] == "nosniff"

    def test_html_declared_as_png_cannot_run(self, uploads):
        # Inline, but as image/png with nosniff and a sandbox: no document, no script.
        r = uploads.get("/api/files/uploads/liar")
        assert r.headers["content-type"] == "image/png"
        assert r.headers["x-content-type-options"] == "nosniff"
        assert "sandbox" in r.headers["content-security-policy"]

    def test_unknown_id_is_404(self, uploads):
        assert uploads.get("/api/files/uploads/nope").status_code == 404


@pytest.fixture
def workspace_client(ws):
    from nerve.gateway.routes import workspace as ws_routes

    (ws / "shots").mkdir()
    (ws / "shots" / "a.png").write_bytes(_PNG)
    (ws / "shots" / "b.JPG").write_bytes(b"\xff\xd8\xff")
    (ws / "shots" / "c.svg").write_bytes(_SVG)
    (ws / "shots" / "d.bin").write_bytes(b"\x00\x01")
    app = FastAPI()
    app.include_router(ws_routes.router)
    app.dependency_overrides[require_auth] = lambda: None
    return TestClient(app)


class TestWorkspaceDownload:
    @pytest.mark.parametrize("name,media_type", [("a.png", "image/png"), ("b.JPG", "image/jpeg")])
    def test_inline_serves_a_raster_image_in_place(self, workspace_client, name, media_type):
        r = workspace_client.get("/api/workspace/download", params={"path": f"shots/{name}", "inline": "1"})
        assert r.status_code == 200
        assert r.headers["content-type"] == media_type
        assert _disposition(r) == "inline"
        assert "sandbox" in r.headers["content-security-policy"]

    @pytest.mark.parametrize("name", ["c.svg", "d.bin"])
    def test_inline_leaves_other_files_attachments(self, workspace_client, name):
        r = workspace_client.get("/api/workspace/download", params={"path": f"shots/{name}", "inline": "1"})
        assert r.status_code == 200
        assert r.headers["content-type"] == "application/octet-stream"
        assert _disposition(r) == "attachment"

    def test_without_inline_an_image_still_downloads(self, workspace_client):
        r = workspace_client.get("/api/workspace/download", params={"path": "shots/a.png"})
        assert r.headers["content-type"] == "application/octet-stream"
        assert _disposition(r) == "attachment"

    def test_inline_keeps_the_read_rules(self, workspace_client):
        for p in ("../x.png", "/etc/passwd", "shots/missing.png"):
            r = workspace_client.get("/api/workspace/download", params={"path": p, "inline": "1"})
            assert r.status_code == 404, p
