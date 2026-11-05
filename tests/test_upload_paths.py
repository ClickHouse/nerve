"""A session's uploads stay inside the uploads directory.

``/api/chat`` and the WebSocket create a session with any id that the client
sends, and the upload route names the session's upload directory after that
id. Pinned at both levels: the directory rule on its own, and the routes that
create and remove the directory.
"""

from __future__ import annotations

import asyncio
import types

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import nerve.config as cfg
import nerve.gateway.routes._deps as deps_mod
from nerve.config import NerveConfig
from nerve.gateway.routes.files import session_upload_dir


@pytest.fixture
def ws(tmp_path, monkeypatch):
    ws = tmp_path / "ws"
    ws.mkdir()
    monkeypatch.setattr(cfg, "_config", NerveConfig(workspace=ws))
    return ws


class TestSessionUploadDir:
    @pytest.mark.parametrize("session_id", ["a1b2c3d4", "telegram:42", "web/default"])
    def test_an_id_inside_the_uploads_directory(self, ws, session_id):
        assert session_upload_dir(session_id) == (ws / ".uploads" / session_id).resolve()

    @pytest.mark.parametrize("session_id", [
        "..", "../x", "../../x", "a/../../x", "a/..", ".", "", "x\x00y",
    ])
    def test_an_id_that_leaves_the_uploads_directory(self, ws, session_id):
        assert session_upload_dir(session_id) is None

    def test_an_absolute_id_is_not_joined(self, ws, tmp_path):
        # Path("/ws/.uploads") / "/x" is Path("/x"): the left side is dropped.
        assert session_upload_dir(str(tmp_path / "elsewhere")) is None
        assert session_upload_dir("/etc") is None

    def test_a_symlink_out_of_the_uploads_directory(self, ws, tmp_path):
        (ws / ".uploads").mkdir()
        (ws / ".uploads" / "link").symlink_to(tmp_path)
        assert session_upload_dir("link") is None


class _FakeDb:
    """The two calls that the upload route makes, over a dict."""

    def __init__(self):
        self.saved: list[dict] = []

    async def get_session(self, session_id):
        # The client creates the session first, through /api/chat.
        return {"id": session_id}

    async def save_uploaded_file(self, **row):
        self.saved.append(row)

    async def delete_uploaded_files(self, session_id):
        return []


@pytest.fixture
def client(ws, monkeypatch):
    from nerve.gateway.auth import require_auth
    from nerve.gateway.routes.files import router

    db = _FakeDb()
    monkeypatch.setattr(deps_mod, "_deps", types.SimpleNamespace(db=db))
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[require_auth] = lambda: None
    return TestClient(app), db


def _upload(client, session_id):
    return client.post(
        "/api/files/upload",
        data={"session_id": session_id},
        files={"files": ("notes.txt", b"hello\n", "text/plain")},
    )


class TestUploadRoute:
    def test_a_file_is_stored_under_the_session_directory(self, client, ws):
        test_client, db = client
        r = _upload(test_client, "a1b2c3d4")
        assert r.status_code == 200
        [row] = db.saved
        stored = ws / ".uploads" / "a1b2c3d4"
        assert row["disk_path"].startswith(str(stored.resolve()))
        assert [p.name.split("_", 1)[1] for p in stored.iterdir()] == ["notes.txt"]

    @pytest.mark.parametrize("make_id", [
        lambda tmp: str(tmp / "elsewhere"),
        lambda tmp: "../../elsewhere",
    ], ids=["absolute", "dot-dot"])
    def test_an_id_that_leaves_the_uploads_directory_writes_nothing(
        self, client, ws, tmp_path, make_id,
    ):
        test_client, db = client
        r = _upload(test_client, make_id(tmp_path))
        assert r.status_code == 400
        assert db.saved == []
        assert not (tmp_path / "elsewhere").exists()


class TestSessionDeleteCleanup:
    def test_the_empty_session_directory_is_removed(self, ws):
        from nerve.gateway.routes.sessions import _cleanup_uploaded_files

        (ws / ".uploads" / "a1b2c3d4").mkdir(parents=True)
        asyncio.run(_cleanup_uploaded_files(_FakeDb(), "a1b2c3d4"))
        assert not (ws / ".uploads" / "a1b2c3d4").exists()

    def test_a_directory_outside_the_uploads_directory_is_kept(self, ws):
        from nerve.gateway.routes.sessions import _cleanup_uploaded_files

        (ws / ".uploads").mkdir()
        (ws / "empty").mkdir()
        asyncio.run(_cleanup_uploaded_files(_FakeDb(), "../empty"))
        assert (ws / "empty").is_dir()
