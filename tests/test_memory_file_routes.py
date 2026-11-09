"""The memory file routes read and write memory files and nothing else.

The routes take the path from the request. The workspace also holds files that
the daemon runs: git runs ``core.fsmonitor`` from ``.git/config``, and the
notification dispatcher runs ``scripts/``. Pinned at both levels: the path
rule on its own, and the routes as mounted, behind Starlette's path decoding.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import nerve.config as cfg
from nerve.config import NerveConfig
from nerve.memory.manager import MemoryManager, memory_file_path

_GIT_CONFIG = "[core]\n\tbare = false\n"


@pytest.fixture
def ws(tmp_path):
    """A workspace with memory files and files that the daemon runs."""
    ws = tmp_path / "ws"
    (ws / "memory" / "notes").mkdir(parents=True)
    (ws / ".git").mkdir()
    (ws / "scripts").mkdir()
    (ws / "MEMORY.md").write_text("# Memory\n")
    (ws / "memory" / "notes" / "a.md").write_text("a\n")
    (ws / ".git" / "config").write_text(_GIT_CONFIG)
    (tmp_path / "outside.md").write_text("outside\n")
    return ws


class TestMemoryFilePath:
    @pytest.mark.parametrize("path", [
        "MEMORY.md", "TASK.md", "memory/notes/a.md", "memory/new/b.md",
        "memory/../MEMORY.md",
    ])
    def test_markdown_at_the_root_or_under_memory(self, ws, path):
        assert memory_file_path(ws, path) == (ws / path).resolve()

    @pytest.mark.parametrize("path", [
        ".git/config", ".git/hooks/post-checkout",
        "scripts/_mechanical_action.py", "scripts/mechanical-action.sh",
        "config/settings.yaml", "skills/x/SKILL.md", "docs/notes.md",
        "memory/notes.txt", "NOTES.MD", "memory", "", ".",
    ])
    def test_other_workspace_files_are_not_memory_files(self, ws, path):
        assert memory_file_path(ws, path) is None

    def test_a_path_out_of_the_workspace_is_not_a_memory_file(self, ws, tmp_path):
        assert memory_file_path(ws, "../outside.md") is None
        assert memory_file_path(ws, "memory/../../outside.md") is None
        # Path("/ws") / "/x" is Path("/x"): the left side is dropped.
        assert memory_file_path(ws, str(tmp_path / "outside.md")) is None

    def test_a_symlink_is_judged_by_the_file_it_names(self, ws):
        (ws / "NOTES.md").symlink_to(ws / ".git" / "config")
        (ws / "memory" / "run.md").symlink_to(ws / "scripts" / "x.py")
        (ws / "TODAY.md").symlink_to(ws / "memory" / "notes" / "a.md")
        assert memory_file_path(ws, "NOTES.md") is None
        assert memory_file_path(ws, "memory/run.md") is None
        assert memory_file_path(ws, "TODAY.md") == (ws / "memory" / "notes" / "a.md").resolve()

    def test_a_nul_byte_is_not_an_error(self, ws):
        assert memory_file_path(ws, "MEMORY.md\x00") is None

    def test_the_memory_manager_uses_the_same_rule(self, ws, monkeypatch):
        monkeypatch.setattr(cfg, "_config", NerveConfig(workspace=ws))
        mgr = MemoryManager(ws)
        assert mgr.read_file(".git/config") is None
        assert mgr.write_file(".git/config", "[core]\n") is False
        assert mgr.write_file("scripts/x.py", "print()\n") is False
        assert (ws / ".git" / "config").read_text() == _GIT_CONFIG
        assert not (ws / "scripts" / "x.py").exists()
        assert mgr.write_file("memory/b.md", "b\n") is True
        assert mgr.read_file("memory/b.md") == "b\n"


@pytest.fixture(params=[True, False], ids=["locked", "unlocked"])
def client(request, ws, monkeypatch):
    """The memory router as the gateway mounts it, with auth satisfied."""
    from nerve.gateway.auth import require_auth
    from nerve.gateway.routes.memory import router

    monkeypatch.setattr(
        cfg, "_config", NerveConfig(lockdown=request.param, workspace=ws),
    )
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[require_auth] = lambda: None
    return TestClient(app)


class TestMemoryFileRoutes:
    @pytest.mark.parametrize("path", [
        ".git/config", "scripts/_mechanical_action.py",
        "scripts/mechanical-action.sh", "docs/notes.md",
    ])
    def test_a_file_the_daemon_runs_is_not_written(self, client, ws, path):
        r = client.put(f"/api/memory/file/{path}", json={"content": "x\n"})
        assert r.status_code == 403
        assert "memory/" in r.json()["detail"]
        if path == ".git/config":
            assert (ws / ".git" / "config").read_text() == _GIT_CONFIG
        else:
            assert not (ws / path).exists()

    def test_a_file_outside_the_memory_files_is_not_read(self, client):
        assert client.get("/api/memory/file/.git/config").status_code == 403

    @pytest.mark.parametrize("path", ["%2e%2e/outside.md", "%2Fetc%2Fhosts", "MEMORY.md%00"])
    def test_an_encoded_path_out_of_the_memory_files_is_refused(self, client, ws, tmp_path, path):
        assert client.get(f"/api/memory/file/{path}").status_code == 403
        r = client.put(f"/api/memory/file/{path}", json={"content": "x\n"})
        assert r.status_code == 403
        assert (tmp_path / "outside.md").read_text() == "outside\n"

    def test_a_memory_file_is_written_and_read(self, client, ws):
        r = client.put("/api/memory/file/memory/new/b.md", json={"content": "b\n"})
        assert r.status_code == 200
        assert (ws / "memory" / "new" / "b.md").read_text() == "b\n"
        r = client.get("/api/memory/file/memory/new/b.md")
        assert r.status_code == 200
        assert r.json()["content"] == "b\n"

    def test_a_missing_memory_file_is_not_found(self, client):
        assert client.get("/api/memory/file/memory/none.md").status_code == 404
