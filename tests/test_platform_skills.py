"""Platform skills: read-only skills in ``<config_dir>/skills``.

The operator or the hosting platform writes them next to ``platform.md``.
Nerve reads them besides ``<workspace>/skills``, gives the platform's skill
precedence for an id that is in both, and refuses to change them.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest

import nerve.config as cfg
from nerve.agent.tools.handlers.skills import (
    skill_create_handler,
    skill_get_handler,
    skill_list_handler,
    skill_read_reference_handler,
    skill_run_script_handler,
    skill_update_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig
from nerve.skills.manager import (
    PLATFORM_SKILL_READ_ONLY,
    SOURCE_PLATFORM,
    SOURCE_WORKSPACE,
    PlatformSkillError,
    SkillManager,
)


def _skill(root: Path, skill_id: str, *, name: str | None = None,
           description: str = "A test skill.", body: str = "Do the thing.") -> Path:
    skill_dir = root / skill_id
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(
        f"---\nname: {name or skill_id}\ndescription: {description}\n---\n\n{body}\n",
        encoding="utf-8",
    )
    return skill_dir


@pytest.fixture
def unlocked(monkeypatch):
    monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=False))


@pytest.fixture
def dirs(tmp_path, unlocked):
    workspace = tmp_path / "ws"
    config_dir = tmp_path / "cfg"
    (workspace / "skills").mkdir(parents=True)
    (config_dir / "skills").mkdir(parents=True)
    return SimpleNamespace(
        workspace=workspace,
        config_dir=config_dir,
        ws_skills=workspace / "skills",
        platform_skills=config_dir / "skills",
    )


@pytest.fixture
def mgr(dirs, db):
    return SkillManager(dirs.workspace, db, config_dir=dirs.config_dir)


def _ctx(mgr: SkillManager) -> ToolContext:
    return ToolContext(session_id="test-session", skill_manager=mgr)


def _text(result) -> str:
    return result.content[0]["text"]


class TestDiscovery:
    @pytest.mark.asyncio
    async def test_discovers_skills_from_both_directories(self, dirs, mgr, db):
        _skill(dirs.ws_skills, "alpha")
        _skill(dirs.platform_skills, "beta")

        found = {meta.id: meta.source for meta in await mgr.discover()}

        assert found == {"alpha": SOURCE_WORKSPACE, "beta": SOURCE_PLATFORM}
        assert {row["id"] for row in await db.list_skills()} == {"alpha", "beta"}
        assert mgr.source("alpha") == SOURCE_WORKSPACE
        assert mgr.source("beta") == SOURCE_PLATFORM
        beta = await mgr.get_skill("beta")
        assert beta.source == SOURCE_PLATFORM
        assert "Do the thing." in beta.content

    @pytest.mark.asyncio
    async def test_a_missing_platform_directory_is_not_an_error(self, tmp_path, db, unlocked):
        workspace = tmp_path / "ws"
        _skill(workspace / "skills", "alpha")
        mgr = SkillManager(workspace, db, config_dir=tmp_path / "no-such-dir")

        found = await mgr.discover()

        assert [(m.id, m.source) for m in found] == [("alpha", SOURCE_WORKSPACE)]

    @pytest.mark.asyncio
    async def test_without_a_config_directory_only_workspace_skills(self, tmp_path, db, unlocked):
        workspace = tmp_path / "ws"
        _skill(workspace / "skills", "alpha")
        mgr = SkillManager(workspace, db)

        assert [m.id for m in await mgr.discover()] == ["alpha"]
        assert mgr.platform_dir is None

    @pytest.mark.asyncio
    async def test_the_platform_skill_is_in_effect_for_an_id_in_both(self, dirs, mgr, db, caplog):
        _skill(dirs.ws_skills, "shared", description="Workspace copy.", body="workspace body")
        _skill(dirs.platform_skills, "shared", description="Platform copy.", body="platform body")

        with caplog.at_level(logging.WARNING, logger="nerve.skills.manager"):
            found = await mgr.discover()

        assert [(m.id, m.source, m.description) for m in found] == [
            ("shared", SOURCE_PLATFORM, "Platform copy."),
        ]
        row = await db.get_skill_row("shared")
        assert row["description"] == "Platform copy."
        skill = await mgr.get_skill("shared")
        assert skill.source == SOURCE_PLATFORM
        assert skill.content == "platform body"
        assert any(
            "shared" in r.getMessage() and "platform skill is in effect" in r.getMessage()
            for r in caplog.records
        )

    @pytest.mark.asyncio
    async def test_a_skill_the_platform_removes_leaves_the_index(self, dirs, mgr, db):
        skill_dir = _skill(dirs.platform_skills, "beta")
        await mgr.discover()
        assert await db.get_skill_row("beta") is not None

        (skill_dir / "SKILL.md").unlink()
        await mgr.discover()

        assert await db.get_skill_row("beta") is None
        assert "beta" not in mgr._cache, "the system prompt's skill list reads the cache"

    @pytest.mark.asyncio
    async def test_a_platform_directory_that_is_not_a_skill_id_is_skipped(self, dirs, mgr):
        _skill(dirs.platform_skills, "not a valid id")
        _skill(dirs.platform_skills, "beta")

        assert [m.id for m in await mgr.discover()] == ["beta"]

    @pytest.mark.asyncio
    async def test_a_config_directory_that_is_the_workspace_has_no_platform_skills(
        self, tmp_path, db, unlocked,
    ):
        workspace = tmp_path / "ws"
        _skill(workspace / "skills", "alpha")
        mgr = SkillManager(workspace, db, config_dir=workspace)

        found = await mgr.discover()

        assert [(m.id, m.source) for m in found] == [("alpha", SOURCE_WORKSPACE)]
        assert await mgr.update_skill("alpha", "---\nname: alpha\n---\nnew\n") is not None

    @pytest.mark.asyncio
    async def test_a_locked_instance_does_not_read_platform_skills(
        self, dirs, mgr, monkeypatch, caplog,
    ):
        _skill(dirs.ws_skills, "alpha")
        _skill(dirs.platform_skills, "beta")
        _skill(dirs.platform_skills, "alpha", body="platform body")
        await mgr.discover()
        assert mgr.source("beta") == SOURCE_PLATFORM
        # A reload that turns lockdown on.
        monkeypatch.setattr(cfg, "_config", NerveConfig(lockdown=True))

        with caplog.at_level(logging.INFO, logger="nerve.skills.manager"):
            found = await mgr.discover()

        assert [(m.id, m.source) for m in found] == [("alpha", SOURCE_WORKSPACE)]
        assert set(mgr._cache) == {"alpha"}
        assert mgr._cache["alpha"].source == SOURCE_WORKSPACE
        assert mgr.source("alpha") == SOURCE_WORKSPACE
        assert (await mgr.get_skill("alpha")).content == "Do the thing."
        assert await mgr.get_skill("beta") is None
        assert any("lockdown" in r.getMessage() for r in caplog.records)



_AS_ROOT = hasattr(os, "geteuid") and os.geteuid() == 0


@pytest.mark.skipif(_AS_ROOT, reason="root can read a directory with mode 000")
class TestUnreadablePlatformDirectory:
    """Another party owns the platform directory. A mode that Nerve cannot
    read gives no platform skills there and does not break workspace skills."""

    @pytest.mark.asyncio
    async def test_an_unreadable_platform_directory_leaves_workspace_skills_working(
        self, dirs, mgr, caplog,
    ):
        _skill(dirs.ws_skills, "alpha")
        _skill(dirs.platform_skills, "beta")
        dirs.platform_skills.chmod(0)
        try:
            with caplog.at_level(logging.ERROR, logger="nerve.skills.manager"):
                found = await mgr.discover()

            assert [(m.id, m.source) for m in found] == [("alpha", SOURCE_WORKSPACE)]
            assert mgr.source("alpha") == SOURCE_WORKSPACE
            assert mgr.source("beta") == SOURCE_WORKSPACE
            assert (await mgr.get_skill("alpha")).content == "Do the thing."
            assert [s["id"] for s in await mgr.get_enabled_summaries()] == ["alpha"]
            assert await mgr.update_skill("alpha", "---\nname: alpha\n---\nnew\n") is not None
            assert any("platform skills" in r.getMessage() for r in caplog.records)
        finally:
            dirs.platform_skills.chmod(0o755)

    @pytest.mark.asyncio
    async def test_one_unreadable_platform_skill_does_not_stop_the_scan(
        self, dirs, mgr, caplog,
    ):
        _skill(dirs.ws_skills, "alpha")
        _skill(dirs.ws_skills, "shared", body="workspace body")
        _skill(dirs.platform_skills, "beta")
        broken = _skill(dirs.platform_skills, "shared", body="platform body")
        broken.chmod(0)
        try:
            with caplog.at_level(logging.ERROR, logger="nerve.skills.manager"):
                found = await mgr.discover()

            assert [(m.id, m.source) for m in found] == [
                ("alpha", SOURCE_WORKSPACE),
                ("beta", SOURCE_PLATFORM),
                ("shared", SOURCE_WORKSPACE),
            ]
            # The skill in effect for the id agrees with the scan.
            assert mgr.source("shared") == SOURCE_WORKSPACE
            assert (await mgr.get_skill("shared")).content == "workspace body"
            assert any(
                "shared" in r.getMessage() and r.levelno == logging.ERROR
                for r in caplog.records
            )
        finally:
            broken.chmod(0o755)


class TestReadOnly:
    @pytest.mark.asyncio
    async def test_create_refuses_an_id_the_platform_provides(self, dirs, mgr):
        _skill(dirs.platform_skills, "beta")
        await mgr.discover()

        with pytest.raises(PlatformSkillError, match="provided by the platform"):
            await mgr.create_skill("Beta", "A copy.")

        assert not (dirs.ws_skills / "beta").exists()

    @pytest.mark.asyncio
    async def test_a_platform_skill_can_be_disabled_and_enabled(self, dirs, mgr, db):
        _skill(dirs.platform_skills, "beta")
        await mgr.discover()

        assert await mgr.toggle_skill("beta", False) is True
        assert not (await db.get_skill_row("beta"))["enabled"]
        assert await mgr.get_enabled_summaries() == []

        await mgr.discover()
        assert not (await db.get_skill_row("beta"))["enabled"], "kept across a rescan"

        assert await mgr.toggle_skill("beta", True) is True
        assert [s["id"] for s in await mgr.get_enabled_summaries()] == ["beta"]

    @pytest.mark.asyncio
    async def test_workspace_skills_stay_writable(self, dirs, mgr):
        _skill(dirs.ws_skills, "alpha")
        _skill(dirs.platform_skills, "beta")
        await mgr.discover()

        meta = await mgr.update_skill("alpha", "---\nname: alpha\ndescription: New.\n---\nnew\n")

        assert meta is not None and meta.source == SOURCE_WORKSPACE
        assert await mgr.delete_skill("alpha") is True
        assert not (dirs.ws_skills / "alpha").exists()


class TestReferencesAndScripts:
    @pytest.mark.asyncio
    async def test_a_reference_path_cannot_leave_the_platform_skill(self, dirs, mgr):
        skill_dir = _skill(dirs.platform_skills, "beta")
        (skill_dir / "references").mkdir()
        (dirs.config_dir / "config.local.yaml").write_text("outside: the skill\n", encoding="utf-8")
        await mgr.discover()

        assert await mgr.read_reference("beta", "../../../config.local.yaml") is None
        assert await mgr.read_reference("beta", str(dirs.config_dir / "config.local.yaml")) is None

    @pytest.mark.asyncio
    async def test_scripts_run_from_the_platform_directory(self, dirs, mgr):
        skill_dir = _skill(dirs.platform_skills, "beta")
        (skill_dir / "scripts").mkdir()
        (skill_dir / "scripts" / "where.sh").write_text("pwd\n", encoding="utf-8")
        (dirs.config_dir / "outside.sh").write_text("echo escaped\n", encoding="utf-8")
        await mgr.discover()

        output = await mgr.run_script("beta", "where.sh")
        assert Path(output.strip()).resolve() == skill_dir.resolve()
        assert await mgr.run_script("beta", "../../../outside.sh") == (
            "Error: path traversal detected"
        )


class TestTools:
    @pytest.mark.asyncio
    async def test_skill_list_marks_platform_skills(self, dirs, mgr):
        _skill(dirs.ws_skills, "alpha", description="Workspace skill.")
        _skill(dirs.platform_skills, "beta", description="Platform skill.")
        await mgr.discover()

        text = _text(await skill_list_handler(_ctx(mgr), {}))

        assert "- **alpha** (`alpha`): Workspace skill." in text
        assert "- **beta** (`beta`): [platform, read only] Platform skill." in text

    @pytest.mark.asyncio
    async def test_skill_get_names_the_source_and_platform_files(self, dirs, mgr):
        skill_dir = _skill(dirs.platform_skills, "beta")
        (skill_dir / "references").mkdir()
        (skill_dir / "references" / "guide.md").write_text("g", encoding="utf-8")
        (skill_dir / "scripts").mkdir()
        (skill_dir / "scripts" / "run.sh").write_text("echo hi\n", encoding="utf-8")
        _skill(dirs.ws_skills, "alpha")
        await mgr.discover()

        beta = _text(await skill_get_handler(_ctx(mgr), {"name": "beta"}))
        alpha = _text(await skill_get_handler(_ctx(mgr), {"name": "alpha"}))

        assert "Source: platform (read only)" in beta
        assert "`guide.md`" in beta and "`run.sh`" in beta
        assert "Source: workspace" in alpha

    @pytest.mark.asyncio
    async def test_reference_and_script_tools_use_the_platform_directory(self, dirs, mgr):
        skill_dir = _skill(dirs.platform_skills, "beta")
        (skill_dir / "references").mkdir()
        (skill_dir / "references" / "guide.md").write_text("platform guide", encoding="utf-8")
        (skill_dir / "scripts").mkdir()
        (skill_dir / "scripts" / "hello.sh").write_text("echo hello\n", encoding="utf-8")
        await mgr.discover()

        ref = await skill_read_reference_handler(_ctx(mgr), {"name": "beta", "path": "guide.md"})
        out = await skill_run_script_handler(_ctx(mgr), {"name": "beta", "path": "hello.sh"})

        assert _text(ref) == "platform guide"
        assert _text(out).strip() == "hello"

    @pytest.mark.asyncio
    async def test_skill_update_refuses_a_platform_skill(self, dirs, mgr):
        skill_dir = _skill(dirs.platform_skills, "beta")
        before = (skill_dir / "SKILL.md").read_text(encoding="utf-8")
        await mgr.discover()

        result = await skill_update_handler(
            _ctx(mgr), {"name": "beta", "content": "---\nname: beta\n---\nchanged\n"},
        )

        assert PLATFORM_SKILL_READ_ONLY in _text(result)
        assert (skill_dir / "SKILL.md").read_text(encoding="utf-8") == before

    @pytest.mark.asyncio
    async def test_skill_create_refuses_a_platform_id(self, dirs, mgr):
        _skill(dirs.platform_skills, "beta")
        await mgr.discover()

        result = await skill_create_handler(
            _ctx(mgr), {"name": "beta", "description": "A copy."},
        )

        assert "A skill with the id 'beta' is provided by the platform" in _text(result)
        assert not (dirs.ws_skills / "beta").exists()


class _NoMemory:
    available = False

    def __init__(self, *args, **kwargs):
        pass

    async def initialize(self):
        return False

    async def aclose(self):
        pass


class TestEngine:
    @pytest.mark.asyncio
    async def test_the_engine_reads_platform_skills_from_its_config_directory(
        self, dirs, db,
    ):
        from nerve.agent.engine import AgentEngine

        _skill(dirs.ws_skills, "alpha")
        _skill(dirs.platform_skills, "beta")
        config = NerveConfig.from_dict({"workspace": str(dirs.workspace)})
        config.config_dir = dirs.config_dir
        engine = AgentEngine(config, db)

        with patch("nerve.memory.memu_bridge.MemUBridge", _NoMemory), \
             patch("nerve.memory.xmemory_bridge.XmemoryBridge", _NoMemory), \
             patch("nerve.config.load_claude_code_plugins"):
            await engine.initialize()
        try:
            mgr = engine._skill_manager
            assert mgr.source("beta") == SOURCE_PLATFORM
            assert {s["id"] for s in await mgr.get_enabled_summaries()} == {"alpha", "beta"}
        finally:
            await engine.shutdown()


@asynccontextmanager
async def _no_lifespan(_app):
    yield


class TestRoutes:
    @pytest.fixture
    def client(self, dirs, mgr, db, monkeypatch, bypass_auth):
        from nerve.gateway import server
        from nerve.gateway.routes import _deps as deps_module

        config = NerveConfig(lockdown=False)
        config.workspace = dirs.workspace
        monkeypatch.setattr(cfg, "_config", config)
        monkeypatch.setattr(
            deps_module, "_deps",
            deps_module.RouteDeps(engine=SimpleNamespace(_skill_manager=mgr), db=db),
        )
        monkeypatch.setattr(server, "lifespan", _no_lifespan)
        app = bypass_auth(server.create_app())
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://nerve-test",
        )

    @pytest.mark.asyncio
    async def test_payloads_carry_the_source(self, dirs, mgr, client):
        _skill(dirs.ws_skills, "alpha")
        _skill(dirs.platform_skills, "beta")
        await mgr.discover()

        async with client as http:
            listed = (await http.get("/api/skills")).json()["skills"]
            detail = (await http.get("/api/skills/beta")).json()
            synced = (await http.post("/api/skills/sync")).json()["skills"]

        assert {s["id"]: s["source"] for s in listed} == {
            "alpha": SOURCE_WORKSPACE, "beta": SOURCE_PLATFORM,
        }
        assert detail["source"] == SOURCE_PLATFORM
        assert {s["id"]: s["source"] for s in synced} == {
            "alpha": SOURCE_WORKSPACE, "beta": SOURCE_PLATFORM,
        }

    @pytest.mark.asyncio
    async def test_update_delete_and_create_refuse_a_platform_skill(self, dirs, mgr, client):
        skill_dir = _skill(dirs.platform_skills, "beta")
        before = (skill_dir / "SKILL.md").read_text(encoding="utf-8")
        await mgr.discover()

        async with client as http:
            put = await http.put("/api/skills/beta", json={"content": "changed"})
            delete = await http.delete("/api/skills/beta")
            post = await http.post(
                "/api/skills", json={"name": "beta", "description": "A copy."},
            )

        assert put.status_code == 403
        assert put.json()["detail"] == PLATFORM_SKILL_READ_ONLY
        assert delete.status_code == 403
        assert delete.json()["detail"] == PLATFORM_SKILL_READ_ONLY
        assert post.status_code == 403
        assert "provided by the platform" in post.json()["detail"]
        assert (skill_dir / "SKILL.md").read_text(encoding="utf-8") == before
        assert not (dirs.ws_skills / "beta").exists()

    @pytest.mark.asyncio
    async def test_toggle_works_for_a_platform_skill(self, dirs, mgr, db, client):
        _skill(dirs.platform_skills, "beta")
        await mgr.discover()

        async with client as http:
            resp = await http.patch("/api/skills/beta/toggle", json={"enabled": False})

        assert resp.status_code == 200
        assert not (await db.get_skill_row("beta"))["enabled"]
