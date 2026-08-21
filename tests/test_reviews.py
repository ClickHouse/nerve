"""Tests for the code-review panel: ReviewStore, git helpers, and routes."""

from __future__ import annotations

import asyncio
import subprocess
from types import SimpleNamespace

import pytest
import pytest_asyncio

from nerve.db import Database
from nerve.gateway import gitreview


# --------------------------------------------------------------------------- #
#  Helpers                                                                     #
# --------------------------------------------------------------------------- #

def _git(cwd, *args):
    subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True)


def _make_repo(path):
    """A git repo with a committed file, an uncommitted modification, and an
    untracked file."""
    path.mkdir(parents=True, exist_ok=True)
    _git(path, "init", "-q")
    _git(path, "config", "user.email", "t@example.com")
    _git(path, "config", "user.name", "Test")
    (path / "a.txt").write_text("line1\nline2\nline3\n")
    (path / "keep.txt").write_text("unchanged\n")
    _git(path, "add", "-A")
    _git(path, "commit", "-q", "-m", "init")
    (path / "a.txt").write_text("line1\nCHANGED\nline3\nline4\n")   # modified
    (path / "new.txt").write_text("brand new\n")                    # untracked
    return path


class FakeEngine:
    """Records engine.run() calls (the comment-inject path)."""

    def __init__(self):
        self.runs: list[dict] = []
        self.run_event = asyncio.Event()

    async def run(self, *, session_id, user_message, source, channel):
        self.runs.append({
            "session_id": session_id, "user_message": user_message,
            "source": source, "channel": channel,
        })
        self.run_event.set()
        return "ok"

    def is_session_running(self, session_id):  # pragma: no cover - unused here
        return False


# --------------------------------------------------------------------------- #
#  ReviewStore                                                                 #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
class TestReviewStore:
    async def test_migration_applied(self, db: Database):
        from nerve.db.base import SCHEMA_VERSION
        assert SCHEMA_VERSION >= 40

    async def test_review_thread_comment_lifecycle(self, db: Database):
        review = await db.create_review(
            repo_root="/r", worktree="/r/wt", branch="feature",
            base_ref="HEAD", created_by="agent", title="my review",
            target_session_id="sess1234",
        )
        rid = review["id"]

        thread = await db.add_thread(
            review_id=rid, file_path="a.txt", side="new",
            line_start=2, line_end=3, anchor_snippet="CHANGED",
        )
        await db.add_comment(thread_id=thread["id"], author="human", body="why?")
        await db.add_comment(thread_id=thread["id"], author="agent", body="because",
                             thread_status="answered")

        full = await db.get_review_full(rid)
        assert full["title"] == "my review"
        assert len(full["threads"]) == 1
        t = full["threads"][0]
        assert t["status"] == "answered"
        assert t["line_start"] == 2 and t["line_end"] == 3
        assert [c["author"] for c in t["comments"]] == ["human", "agent"]

        listed = await db.list_reviews()
        assert listed[0]["thread_count"] == 1
        assert listed[0]["open_thread_count"] == 0   # thread is 'answered', not 'open'

        await db.set_thread_status(t["id"], "resolved")
        assert (await db.get_thread(t["id"]))["status"] == "resolved"

    async def test_status_filter_and_delete(self, db: Database):
        r = await db.create_review(repo_root="/r", worktree="/r/wt")
        await db.update_review(r["id"], status="resolved")
        assert [x["id"] for x in await db.list_reviews(status="open")] == []
        assert [x["id"] for x in await db.list_reviews(status="resolved")] == [r["id"]]
        await db.delete_review(r["id"])
        assert await db.get_review(r["id"]) is None

    async def test_filter_by_session(self, db: Database):
        a = await db.create_review(repo_root="/r", worktree="/r/wt", target_session_id="sessAAAA")
        b = await db.create_review(repo_root="/r", worktree="/r/wt2", target_session_id="sessBBBB")
        assert {x["id"] for x in await db.list_reviews(target_session_id="sessAAAA")} == {a["id"]}
        assert {x["id"] for x in await db.list_reviews(target_session_id="sessBBBB")} == {b["id"]}
        assert {x["id"] for x in await db.list_reviews()} == {a["id"], b["id"]}


# --------------------------------------------------------------------------- #
#  Git helpers                                                                 #
# --------------------------------------------------------------------------- #

class TestGitReview:
    def test_changed_files(self, tmp_path):
        repo = _make_repo(tmp_path / "repo")
        by_path = {f["path"]: f for f in gitreview.changed_files_sync(repo, "HEAD")}
        assert by_path["a.txt"]["status"] == "modified"
        assert by_path["a.txt"]["additions"] >= 1 and by_path["a.txt"]["deletions"] >= 1
        assert by_path["new.txt"]["status"] == "created"
        assert "keep.txt" not in by_path

    def test_file_at_ref_and_working(self, tmp_path):
        repo = _make_repo(tmp_path / "repo")
        _wt, _root, target = gitreview.resolve_within_repos(str(repo), [str(repo)], "a.txt")
        original = gitreview.file_at_ref_sync(repo, "HEAD", "a.txt")
        current = gitreview.read_working_file_sync(target, 1_000_000)
        assert original == "line1\nline2\nline3\n"
        assert current == "line1\nCHANGED\nline3\nline4\n"
        # A file that doesn't exist at HEAD (new file) → None original
        assert gitreview.file_at_ref_sync(repo, "HEAD", "new.txt") is None

    def test_resolve_rejects_traversal_and_foreign_worktree(self, tmp_path):
        repo = _make_repo(tmp_path / "repo")
        # traversal escape
        with pytest.raises(gitreview.RepoAccessError):
            gitreview.resolve_within_repos(str(repo), [str(repo)], "../../etc/passwd")
        # a path not registered as a worktree of any configured repo
        outside = tmp_path / "outside"
        outside.mkdir()
        with pytest.raises(gitreview.RepoAccessError):
            gitreview.resolve_within_repos(str(outside), [str(repo)], None)

    def test_read_working_file_limits(self, tmp_path):
        repo = _make_repo(tmp_path / "repo")
        _wt, _root, target = gitreview.resolve_within_repos(str(repo), [str(repo)], "a.txt")
        with pytest.raises(gitreview.RepoAccessError):
            gitreview.read_working_file_sync(target, 2)   # smaller than the file


# --------------------------------------------------------------------------- #
#  Routes (TestClient + fake engine, auth bypassed)                            #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
class TestReviewRoutes:
    @pytest_asyncio.fixture
    async def app_setup(self, db: Database, tmp_path):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        import nerve.config as cfg_mod
        from nerve.config import NerveConfig
        from nerve.gateway.routes._deps import init_deps
        from nerve.gateway.routes.reviews import router as reviews_router

        repo = _make_repo(tmp_path / "repo")

        cfg = NerveConfig()
        cfg.workspace = tmp_path
        cfg.auth.jwt_secret = ""                 # require_auth becomes a no-op
        cfg.code_review.enabled = True
        cfg.code_review.repos = [str(repo)]
        cfg_mod._config = cfg

        engine = FakeEngine()
        init_deps(engine=engine, db=db)  # type: ignore[arg-type]

        app = FastAPI()
        app.include_router(reviews_router)
        client = TestClient(app)

        yield SimpleNamespace(client=client, engine=engine, db=db, repo=repo, cfg=cfg)

        cfg_mod._config = None

    async def test_repos_and_changed(self, app_setup):
        c = app_setup.client
        repos = c.get("/api/review/repos").json()["repos"]
        assert len(repos) == 1
        assert any(str(app_setup.repo) == wt["path"] or str(app_setup.repo.resolve()) == wt["path"]
                   for wt in repos[0]["worktrees"])

        changed = c.get("/api/review/changed", params={"worktree": str(app_setup.repo)}).json()
        names = {f["path"] for f in changed["files"]}
        assert "a.txt" in names and "new.txt" in names

    async def test_diff_and_file(self, app_setup):
        c = app_setup.client
        d = c.get("/api/review/diff",
                  params={"worktree": str(app_setup.repo), "path": "a.txt"}).json()
        assert d["status"] == "modified"
        assert d["patch"]
        assert any(ln["type"] == "addition" for h in d["hunks"] for ln in h["lines"])

        f = c.get("/api/review/file",
                  params={"worktree": str(app_setup.repo), "path": "a.txt"}).json()
        assert f["binary"] is False and "CHANGED" in f["content"]

    async def test_path_traversal_returns_400(self, app_setup):
        r = app_setup.client.get(
            "/api/review/diff",
            params={"worktree": str(app_setup.repo), "path": "../../../etc/passwd"},
        )
        assert r.status_code == 400

    async def test_disabled_returns_404(self, app_setup):
        app_setup.cfg.code_review.enabled = False
        try:
            assert app_setup.client.get("/api/review/repos").status_code == 404
        finally:
            app_setup.cfg.code_review.enabled = True

    async def test_human_comments_stage_pending_without_injecting(self, app_setup):
        c, engine = app_setup.client, app_setup.engine
        review = c.post("/api/reviews", json={
            "worktree": str(app_setup.repo), "title": "t", "created_by": "agent",
            "target_session_id": "sess-tgt",
        }).json()
        rid = review["id"]
        assert review["branch"]           # derived from the worktree

        # Human line comment → staged pending, NO turn injected.
        resp = c.post(f"/api/reviews/{rid}/threads", json={
            "file_path": "a.txt", "side": "new", "line_start": 2, "line_end": 2,
            "anchor_snippet": "CHANGED", "body": "why change this?", "author": "human",
        })
        assert resp.status_code == 200
        await asyncio.sleep(0)            # let any stray task run — there should be none
        assert engine.runs == []

        full = c.get(f"/api/reviews/{rid}").json()
        cmt = full["threads"][0]["comments"][0]
        assert cmt["author"] == "human" and cmt["pending"] == 1

    async def test_submit_delivers_whole_review_in_one_turn(self, app_setup):
        c, engine = app_setup.client, app_setup.engine
        review = c.post("/api/reviews", json={
            "worktree": str(app_setup.repo), "title": "t", "created_by": "agent",
            "target_session_id": "sess-tgt",
        }).json()
        rid = review["id"]

        # Stage two comments across two files — nothing delivered yet.
        c.post(f"/api/reviews/{rid}/threads", json={
            "file_path": "a.txt", "side": "new", "line_start": 2,
            "anchor_snippet": "CHANGED", "body": "rename this", "author": "human"})
        c.post(f"/api/reviews/{rid}/threads", json={
            "file_path": "new.txt", "side": "new", "line_start": 1,
            "anchor_snippet": "brand new", "body": "needs a test", "author": "human"})
        assert engine.runs == []

        # Submit once → exactly ONE turn carrying the summary + both comments.
        r = c.post(f"/api/reviews/{rid}/submit", json={"summary": "looks close, two nits"})
        assert r.status_code == 200 and r.json()["submitted"] == 2
        assert r.json()["target_session_id"] == "sess-tgt"

        await asyncio.wait_for(engine.run_event.wait(), timeout=2.0)
        assert len(engine.runs) == 1
        msg = engine.runs[0]["user_message"]
        assert engine.runs[0]["session_id"] == "sess-tgt"
        assert "looks close, two nits" in msg
        assert "rename this" in msg and "needs a test" in msg
        assert "a.txt:2" in msg and "new.txt:1" in msg

        # Pending cleared; a re-submit with nothing staged sends no new turn.
        full = c.get(f"/api/reviews/{rid}").json()
        assert all(cm["pending"] == 0 for t in full["threads"] for cm in t["comments"])
        engine.run_event.clear()
        assert c.post(f"/api/reviews/{rid}/submit", json={"summary": ""}).json()["submitted"] == 0
        await asyncio.sleep(0)
        assert len(engine.runs) == 1     # unchanged

    async def test_agent_reply_marks_answered_without_injecting(self, app_setup):
        c, engine = app_setup.client, app_setup.engine
        rid = c.post("/api/reviews", json={
            "worktree": str(app_setup.repo), "title": "t", "target_session_id": "sess-tgt",
        }).json()["id"]
        thr = c.post(f"/api/reviews/{rid}/threads", json={
            "file_path": "a.txt", "side": "new", "line_start": 2,
            "anchor_snippet": "CHANGED", "body": "q?", "author": "human"}).json()["thread"]
        r = c.post(f"/api/reviews/{rid}/threads/{thr['id']}/comments",
                   json={"body": "because Y", "author": "agent"})
        assert r.status_code == 200
        th = c.get(f"/api/reviews/{rid}").json()["threads"][0]
        assert th["status"] == "answered"
        assert [cm["author"] for cm in th["comments"]] == ["human", "agent"]
        await asyncio.sleep(0)
        assert engine.runs == []         # neither staging nor the agent reply injected
