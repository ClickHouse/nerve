"""The workspace routes list and read the whole workspace, and write nothing.

Pinned: the exclusion patterns (default and configured, on listing and on
read), symlinks judged by where they lead, the per-directory cap, binary and
large files, and which files the UI may offer to edit.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import nerve.config as cfg
from nerve.config import FilesConfig, NerveConfig
from nerve.gateway.auth import require_auth
from nerve.gateway.routes import workspace as ws_routes
from nerve.gateway.routes.workspace import is_excluded, resolve_workspace_path


@pytest.fixture
def ws(tmp_path):
    ws = tmp_path / "ws"
    (ws / "memory").mkdir(parents=True)
    (ws / "drafts" / "deep").mkdir(parents=True)
    (ws / ".git").mkdir()
    (ws / "node_modules" / "x").mkdir(parents=True)
    (ws / "tmp" / "big").mkdir(parents=True)
    (ws / "MEMORY.md").write_text("# Memory\n")
    (ws / "memory" / "a.md").write_text("a\n")
    (ws / "drafts" / "plan.md").write_text("plan\n")
    (ws / "drafts" / "deep" / "notes.txt").write_text("notes\n")
    (ws / "drafts" / "scratch.tmp").write_text("tmp\n")
    (ws / ".git" / "config").write_text("[core]\n")
    (ws / "tmp" / "big" / "f.txt").write_text("f\n")
    (ws / "blob.bin").write_bytes(b"\x00\x01\x02")
    (ws / "latin1.txt").write_bytes("caf\xe9".encode("latin-1"))
    (tmp_path / "outside.txt").write_text("outside\n")
    return ws


def _client(ws, monkeypatch, **files) -> TestClient:
    monkeypatch.setattr(cfg, "_config", NerveConfig(workspace=ws, files=FilesConfig(**files)))
    app = FastAPI()
    app.include_router(ws_routes.router)
    app.dependency_overrides[require_auth] = lambda: None
    return TestClient(app)


def _names(client, path=""):
    r = client.get("/api/workspace/list", params={"path": path})
    assert r.status_code == 200, r.text
    return [e["name"] for e in r.json()["entries"]]


class TestIsExcluded:
    @pytest.mark.parametrize("parts,is_dir,pats,expected", [
        (("tmp",), True, ["tmp"], True),
        (("tmp", "a", "b.txt"), False, ["tmp"], True),          # under an excluded dir
        (("drafts", "tmp"), True, ["tmp"], True),              # bare name: any depth
        (("drafts", "x.log"), False, ["*.log"], True),
        (("repos", "big"), True, ["repos/big"], True),          # with "/": from the root
        (("x", "repos", "big"), True, ["repos/big"], False),
        (("build",), False, ["build/"], False),                 # trailing "/": dirs only
        (("build",), True, ["build/"], True),
        (("build", "f"), False, ["build/"], True),
        (("Tmp",), True, ["tmp"], False),                       # case-sensitive
        (("drafts",), True, [], False),
    ])
    def test_patterns(self, parts, is_dir, pats, expected):
        assert is_excluded(parts, is_dir, pats) is expected


class TestResolve:
    def test_inside_and_outside(self, ws):
        pats = FilesConfig().patterns
        assert resolve_workspace_path(ws, "drafts/plan.md", pats) == (ws / "drafts" / "plan.md").resolve()
        assert resolve_workspace_path(ws, "", pats) == ws.resolve()
        assert resolve_workspace_path(ws, "../outside.txt", pats) is None
        assert resolve_workspace_path(ws, "/etc/passwd", pats) is None
        assert resolve_workspace_path(ws, ".git/config", pats) is None
        assert resolve_workspace_path(ws, "MEMORY.md\x00", pats) is None

    def test_a_symlink_is_judged_by_where_it_leads(self, ws, tmp_path):
        (ws / "out").symlink_to(tmp_path / "outside.txt")
        (ws / "gitcfg").symlink_to(ws / ".git" / "config")
        (ws / "plan").symlink_to(ws / "drafts" / "plan.md")
        pats = FilesConfig().patterns
        assert resolve_workspace_path(ws, "out", pats) is None
        assert resolve_workspace_path(ws, "gitcfg", pats) is None
        assert resolve_workspace_path(ws, "plan", pats) == (ws / "drafts" / "plan.md").resolve()


class TestListRoute:
    def test_root_lists_directories_first_and_hides_the_defaults(self, ws, monkeypatch):
        client = _client(ws, monkeypatch)
        names = _names(client)
        assert names == ["drafts", "memory", "tmp", "blob.bin", "latin1.txt", "MEMORY.md"]
        assert ".git" not in names and "node_modules" not in names

    def test_configured_exclusions_hide_and_refuse(self, ws, monkeypatch):
        client = _client(ws, monkeypatch, exclude=["tmp", "*.tmp", "drafts/deep"])
        assert "tmp" not in _names(client)
        assert _names(client, "drafts") == ["plan.md"]
        assert client.get("/api/workspace/list", params={"path": "tmp"}).status_code == 404
        assert client.get("/api/workspace/list", params={"path": "tmp/big"}).status_code == 404
        r = client.get("/api/workspace/file", params={"path": "drafts/deep/notes.txt"})
        assert r.status_code == 404

    def test_default_excludes_can_be_switched_off(self, ws, monkeypatch):
        client = _client(ws, monkeypatch, default_excludes=False)
        assert ".git" in _names(client)

    def test_a_subdirectory_names_its_children_by_full_path(self, ws, monkeypatch):
        client = _client(ws, monkeypatch)
        body = client.get("/api/workspace/list", params={"path": "drafts"}).json()
        assert body["path"] == "drafts"
        assert [e["path"] for e in body["entries"]] == [
            "drafts/deep", "drafts/plan.md", "drafts/scratch.tmp",
        ]

    def test_only_memory_markdown_is_editable(self, ws, monkeypatch):
        client = _client(ws, monkeypatch)
        root = {e["name"]: e for e in client.get("/api/workspace/list").json()["entries"]}
        assert root["MEMORY.md"]["read_only"] is False
        assert root["latin1.txt"]["read_only"] is True
        assert root["latin1.txt"]["read_only_reason"] == "not_memory"
        assert "read_only" not in root["drafts"]  # directories carry no write state
        drafts = {e["name"]: e for e in client.get(
            "/api/workspace/list", params={"path": "drafts"}).json()["entries"]}
        assert drafts["plan.md"]["read_only"] is True  # markdown, but not a memory file

    def test_the_cap_cuts_a_big_directory(self, ws, monkeypatch):
        for i in range(5):
            (ws / "tmp" / f"f{i}.txt").write_text("x")
        client = _client(ws, monkeypatch, max_entries=3)
        body = client.get("/api/workspace/list", params={"path": "tmp"}).json()
        assert len(body["entries"]) == 3
        assert body["truncated"] is True and body["total"] == 6

    def test_a_symlink_out_of_the_workspace_is_not_listed(self, ws, monkeypatch, tmp_path):
        (ws / "out").symlink_to(tmp_path)
        (ws / "gitdir").symlink_to(ws / ".git")
        client = _client(ws, monkeypatch)
        names = _names(client)
        assert "out" not in names and "gitdir" not in names

    def test_outside_paths_are_404(self, ws, monkeypatch):
        client = _client(ws, monkeypatch)
        for p in ("..", "../..", "/tmp", "drafts/plan.md"):
            assert client.get("/api/workspace/list", params={"path": p}).status_code == 404


class TestFileRoute:
    def test_reads_text(self, ws, monkeypatch):
        client = _client(ws, monkeypatch)
        body = client.get("/api/workspace/file", params={"path": "drafts/deep/notes.txt"}).json()
        assert body["content"] == "notes\n"
        assert body["read_only"] is True and body["binary"] is False

    def test_memory_file_is_editable(self, ws, monkeypatch):
        client = _client(ws, monkeypatch)
        body = client.get("/api/workspace/file", params={"path": "memory/a.md"}).json()
        assert body["read_only"] is False and body["read_only_reason"] is None

    def test_binary_and_non_utf8_are_flagged_without_content(self, ws, monkeypatch):
        client = _client(ws, monkeypatch)
        for p in ("blob.bin", "latin1.txt"):
            body = client.get("/api/workspace/file", params={"path": p}).json()
            assert body["binary"] is True and body["content"] == ""

    def test_a_large_file_is_flagged_without_content(self, ws, monkeypatch):
        (ws / "big.txt").write_text("x" * 100)
        client = _client(ws, monkeypatch, max_file_bytes=10)
        body = client.get("/api/workspace/file", params={"path": "big.txt"}).json()
        assert body["too_large"] is True and body["content"] == "" and body["size"] == 100

    def test_refusals(self, ws, monkeypatch):
        client = _client(ws, monkeypatch)
        for p in ("../outside.txt", ".git/config", "drafts", "missing.txt", "node_modules/x"):
            assert client.get("/api/workspace/file", params={"path": p}).status_code == 404

    def test_there_is_no_write_route(self, ws, monkeypatch):
        client = _client(ws, monkeypatch)
        for method in ("put", "post", "delete"):
            r = getattr(client, method)("/api/workspace/file", params={"path": "drafts/plan.md"})
            assert r.status_code == 405
        assert (ws / "drafts" / "plan.md").read_text() == "plan\n"


class TestDownloadRoute:
    def test_sends_the_file_as_an_attachment(self, ws, monkeypatch):
        client = _client(ws, monkeypatch, max_file_bytes=1)  # no size cap here
        r = client.get("/api/workspace/download", params={"path": "blob.bin"})
        assert r.status_code == 200
        assert r.content == b"\x00\x01\x02"
        assert r.headers["content-type"] == "application/octet-stream"
        assert 'attachment; filename="blob.bin"' in r.headers["content-disposition"]

    def test_a_symlink_downloads_under_its_own_name(self, ws, monkeypatch):
        (ws / "plan-link.md").symlink_to(ws / "drafts" / "plan.md")
        client = _client(ws, monkeypatch)
        r = client.get("/api/workspace/download", params={"path": "plan-link.md"})
        assert r.status_code == 200 and r.content == b"plan\n"
        assert 'filename="plan-link.md"' in r.headers["content-disposition"]

    def test_refusals(self, ws, monkeypatch, tmp_path):
        (ws / "out").symlink_to(tmp_path / "outside.txt")
        client = _client(ws, monkeypatch, exclude=["tmp"])
        for p in ("../outside.txt", "/etc/passwd", ".git/config", "tmp/big/f.txt",
                  "out", "drafts", "missing.bin"):
            r = client.get("/api/workspace/download", params={"path": p})
            assert r.status_code == 404, p


class TestConfig:
    def test_from_dict(self):
        c = FilesConfig.from_dict({"exclude": ["repos", " tmp ", ""], "max_entries": "50",
                                   "default_excludes": "false"})
        assert c.exclude == ["repos", "tmp"]
        assert c.max_entries == 50
        assert c.default_excludes is False
        assert c.patterns == ["repos", "tmp"]

    def test_a_single_string_is_one_pattern(self):
        assert FilesConfig.from_dict({"exclude": "repos"}).exclude == ["repos"]

    def test_defaults_come_first_and_are_not_repeated(self):
        c = FilesConfig.from_dict({"exclude": [".git", "repos"]})
        assert c.patterns[0] == ".git" and c.patterns.count(".git") == 1 and c.patterns[-1] == "repos"

    def test_the_key_validates(self):
        assert cfg.validate_config_keys({"files": {"exclude": ["x"], "max_file_bytes": 1}}) == []
        assert cfg.validate_config_keys({"files": {"exclud": ["x"]}}) != []
