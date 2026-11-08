"""``GET /api/files/download`` sends a workspace file as an attachment.

The chat shows an image that the agent sends in an ``<img>`` element, where
an SVG file runs no script. The same URL opened in a browser tab must not
show the SVG as a document of the panel's origin, where a script in it could
read the session token from ``localStorage``. ``Content-Disposition:
attachment`` makes the browser save the file.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import nerve.config as cfg
from nerve.config import NerveConfig

_SVG = '<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>\n'


@pytest.fixture
def ws(tmp_path, monkeypatch):
    ws = tmp_path / "ws"
    ws.mkdir()
    monkeypatch.setattr(cfg, "_config", NerveConfig(workspace=ws))
    return ws


@pytest.fixture
def client(ws, bypass_auth):
    from nerve.gateway.routes.files import router

    app = FastAPI()
    app.include_router(router)
    bypass_auth(app)
    return TestClient(app)


def test_an_svg_file_is_an_attachment(client, ws):
    (ws / "shots").mkdir()
    (ws / "shots" / "chart.svg").write_text(_SVG)

    r = client.get("/api/files/download", params={"path": str(ws / "shots" / "chart.svg")})

    assert r.status_code == 200
    assert r.headers["content-type"].startswith("image/svg+xml")
    disposition = r.headers["content-disposition"]
    assert disposition.split(";")[0].strip() == "attachment"
    assert 'filename="chart.svg"' in disposition
    assert r.text == _SVG
