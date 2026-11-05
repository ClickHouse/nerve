"""Per-session reasoning effort.

A session keeps its effort pick in ``metadata["effort"]`` (None = Default).
The engine builds the client with it (a per-run override first, then the
session's pick, then the source default), rebuilds the client when the pick
changes between turns (deferred while agents run, like a model switch), and
records what the runtime reports each agent runs at. The routes take it on
create and PATCH and expose it as a top-level ``effort``; forks copy it; a
stale metadata write cannot undo it.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import pytest_asyncio

from nerve.agent.backends import events as ev
from nerve.agent.backends.base import BackendCapabilities, SessionSpec
from nerve.agent.backends.claude import ClaudeBackend
from nerve.agent.effort import (
    EFFORT_LEVELS,
    effort_from_metadata,
    normalize_effort,
    session_effort,
)
from nerve.agent.engine import AgentEngine
from nerve.agent.sessions import SessionManager
from nerve.config import NerveConfig
from nerve.db import Database

from tests.actor_rows import ensure_actor_row
from tests.conftest import TEST_ACTOR

SID = "s-eff1"


# --------------------------------------------------------------------------- #
#  The vocabulary                                                              #
# --------------------------------------------------------------------------- #


class TestNormalize:
    def test_levels_pass_in_any_case(self):
        assert normalize_effort("medium") == "medium"
        assert normalize_effort("  HIGH ") == "high"
        assert [normalize_effort(lv) for lv in EFFORT_LEVELS] == list(EFFORT_LEVELS)

    def test_empty_means_default(self):
        assert normalize_effort(None) is None
        assert normalize_effort("") is None
        assert normalize_effort("   ") is None

    @pytest.mark.parametrize("bad", ["ultra", "med", "hgh", 3, ["high"], {"level": "high"}])
    def test_anything_else_is_refused(self, bad):
        with pytest.raises(ValueError):
            normalize_effort(bad)

    def test_reading_metadata(self):
        assert effort_from_metadata({"effort": "low"}) == "low"
        assert effort_from_metadata('{"effort": "max"}') == "max"
        assert effort_from_metadata({"effort": "ultra"}) is None
        assert effort_from_metadata("not json") is None
        assert effort_from_metadata(None) is None
        assert session_effort({"metadata": '{"effort": "xhigh"}'}) == "xhigh"
        assert session_effort({"metadata": None}) is None
        assert session_effort(None) is None


# --------------------------------------------------------------------------- #
#  Storage                                                                     #
# --------------------------------------------------------------------------- #


async def _meta(db: Database, sid: str) -> dict:
    return json.loads((await db.get_session(sid))["metadata"] or "{}")


@pytest.mark.asyncio
class TestStorage:
    async def test_set_change_and_clear_record_events(self, db: Database):
        await db.create_session(SID, metadata={"keep": 1}, actor=None)

        assert await db.set_session_effort(SID, "medium") is True
        assert await db.set_session_effort(SID, "max") is True
        assert await db.set_session_effort(SID, None) is True
        meta = await _meta(db, SID)
        assert "effort" not in meta and meta["keep"] == 1

        events = [
            e["details"] for e in await db.get_session_events(SID)
            if e["event_type"] == "effort_changed"
        ]
        # Newest first.
        assert events == [
            {"effort": None, "previous": "max"},
            {"effort": "max", "previous": "medium"},
            {"effort": "medium", "previous": None},
        ]

    async def test_same_value_writes_nothing(self, db: Database):
        await db.create_session(SID, metadata={"effort": "high"}, actor=None)
        assert await db.set_session_effort(SID, "high") is False
        assert await db.set_session_effort("no-such-session", "high") is False
        assert not [
            e for e in await db.get_session_events(SID)
            if e["event_type"] == "effort_changed"
        ]

    async def test_a_stale_metadata_write_keeps_the_pick(self, db: Database):
        await db.create_session(SID, metadata={"cache_ttl": "5m"}, actor=None)
        stale = await _meta(db, SID)          # read before the change...
        await db.set_session_effort(SID, "low")
        stale["preamble_sent"] = True
        await db.update_session_metadata(SID, stale)   # ...written after it
        meta = await _meta(db, SID)
        assert meta == {"cache_ttl": "5m", "preamble_sent": True, "effort": "low"}

        # Nor can a metadata write set or clear it.
        await db.update_session_metadata(SID, {"effort": "max"})
        assert effort_from_metadata(await _meta(db, SID)) == "low"
        await db.set_session_effort(SID, None)
        await db.update_session_metadata(SID, {"effort": "max", "x": 1})
        assert await _meta(db, SID) == {"x": 1}

    async def test_a_fork_runs_at_its_sources_effort(self, db: Database):
        sm = SessionManager(db)
        await sm.get_or_create("src", source="web", actor=None)
        await db.update_session_fields("src", {"sdk_session_id": "native-src"})
        await db.set_session_effort("src", "medium")
        fork = await sm.fork_session("src", actor=None)
        assert session_effort(await db.get_session(fork["id"])) == "medium"

        await sm.get_or_create("plain", source="web", actor=None)
        await db.update_session_fields("plain", {"sdk_session_id": "native-plain"})
        fork2 = await sm.fork_session("plain", actor=None)
        assert session_effort(await db.get_session(fork2["id"])) is None


# --------------------------------------------------------------------------- #
#  Engine: which effort a client is built with, and rebuilds                   #
# --------------------------------------------------------------------------- #


class _Client:
    native_session_id = "native-1"

    def __init__(self, spec: SessionSpec) -> None:
        self.spec = spec
        self.model = spec.model or "model-a"
        self._alive = True

    def is_alive(self) -> bool:
        return self._alive

    async def interrupt(self, *, cancel_queued: bool = False) -> None:
        pass

    async def disconnect(self, timeout: float = 5.0) -> None:
        pass

    def try_receive_idle_events(self):
        return None

    def buffer_used(self) -> int:
        return 0


class _StubBackend:
    name = "claude"
    capabilities = BackendCapabilities(
        cost_is_cumulative=False, supports_idle_stream=False,
        supports_cache_ttl=False, interactive_builtins=False,
        supports_task_stop=True, supports_agent_transcripts=True,
    )

    def __init__(self) -> None:
        self.built: list[_Client] = []

    def default_model(self, source):
        return "model-a"

    def excluded_tools(self):
        return set()

    def validate_resume_target(self, native_id, cwd):
        return True

    async def create_client(self, spec):
        client = _Client(spec)
        self.built.append(client)
        return client


def _engine(db: Database, tmp_path: Path) -> tuple[AgentEngine, _StubBackend]:
    workspace = tmp_path / "ws"
    workspace.mkdir(parents=True, exist_ok=True)
    cfg = NerveConfig.from_dict({
        "workspace": str(workspace),
        "agent": {"backend": "claude", "effort": "max", "cron_effort": "low"},
    })
    engine = AgentEngine(cfg, db)
    backend = engine._backends["claude"] = _StubBackend()
    return engine, backend


async def _live_agent(engine: AgentEngine, task_id: str = "a1b2c3d4e5f6a7b8c"):
    await engine.agents.on_system_event(SID, ev.SystemEvent("task_started", {
        "task_id": task_id, "task_type": "local_agent", "description": "build-api",
        "is_backgrounded": True,
    }))
    return next(
        r for r in engine.agents._sessions[SID].records.values() if r.task_id == task_id
    )


@pytest.mark.asyncio
class TestEngine:
    async def test_default_is_the_source_default(self, db, tmp_path):
        engine, backend = _engine(db, tmp_path)
        await engine.sessions.get_or_create(SID, source="web", backend="claude", actor=None)
        await engine.sessions.get_or_create("cron-1", source="cron", backend="claude", actor=None)
        web = await engine._get_or_create_client(SID, "web", None)
        cron = await engine._get_or_create_client("cron-1", "cron", None)
        assert (web.spec.effort, cron.spec.effort) == ("max", "low")

    async def test_the_session_pick_beats_the_source_default(self, db, tmp_path):
        engine, backend = _engine(db, tmp_path)
        await engine.sessions.get_or_create(
            SID, source="web", backend="claude", metadata={"effort": "medium"}, actor=None,
        )
        await engine.sessions.get_or_create(
            "cron-1", source="cron", backend="claude", metadata={"effort": "high"}, actor=None,
        )
        web = await engine._get_or_create_client(SID, "web", None)
        cron = await engine._get_or_create_client("cron-1", "cron", None)
        assert (web.spec.effort, cron.spec.effort) == ("medium", "high")

    async def test_a_per_run_override_beats_the_session_pick(self, db, tmp_path):
        engine, backend = _engine(db, tmp_path)
        await engine.sessions.get_or_create(
            SID, source="workflow", backend="claude", metadata={"effort": "medium"}, actor=None,
        )
        client = await engine._get_or_create_client(
            SID, "workflow", None, effort_override="xhigh",
        )
        assert client.spec.effort == "xhigh"

    async def test_a_changed_pick_rebuilds_the_client(self, db, tmp_path):
        engine, backend = _engine(db, tmp_path)
        await engine.sessions.get_or_create(SID, source="web", backend="claude", actor=None)
        first = await engine._get_or_create_client(SID, "web", None)
        assert first.spec.effort == "max"
        # Same pick: the client is reused, also when a turn brings an override.
        assert await engine._get_or_create_client(SID, "web", None) is first
        assert await engine._get_or_create_client(
            SID, "web", None, effort_override="low",
        ) is first

        await db.set_session_effort(SID, "medium")
        second = await engine._get_or_create_client(SID, "web", None)
        assert second is not first and second.spec.effort == "medium"
        # The rebuilt client resumes the same conversation.
        assert second.spec.resume_native_id == first.spec.resume_native_id
        assert await engine._get_or_create_client(SID, "web", None) is second

        await db.set_session_effort(SID, None)   # back to Default
        third = await engine._get_or_create_client(SID, "web", None)
        assert third is not second and third.spec.effort == "max"
        assert len(backend.built) == 3

    async def test_an_effort_switch_waits_while_agents_run(self, db, tmp_path):
        engine, backend = _engine(db, tmp_path)
        await engine.sessions.get_or_create(SID, source="web", backend="claude", actor=None)
        first = await engine._get_or_create_client(SID, "web", None)
        rec = await _live_agent(engine)

        await db.set_session_effort(SID, "medium")
        kept = await engine._get_or_create_client(SID, "web", None)
        assert kept is first and len(backend.built) == 1
        assert rec.status == "running"

        await engine.agents.on_system_event(SID, ev.SystemEvent(
            "task_notification", {"task_id": rec.task_id, "status": "completed"},
        ))
        switched = await engine._get_or_create_client(SID, "web", None)
        assert switched is not first and switched.spec.effort == "medium"

    async def test_effort_switch_is_a_runtime_end_for_agent_records(self, db, tmp_path):
        engine, _ = _engine(db, tmp_path)
        await engine.sessions.get_or_create(SID, source="web", backend="claude", actor=None)
        await engine._get_or_create_client(SID, "web", None)
        rec = await _live_agent(engine)
        engine.sessions.remove_client(SID, reason="effort_switch")
        assert (rec.status, rec.end_reason) == ("interrupted", "process_ended")

    async def test_observed_efforts_become_session_events(self, db, tmp_path):
        engine, backend = _engine(db, tmp_path)
        await engine.sessions.get_or_create(SID, source="web", backend="claude", actor=None)
        client = await engine._get_or_create_client(SID, "web", None)
        await client.spec.observe_effort(SID, {
            "agent_id": "agent-1", "agent_type": "general-purpose", "effort": "max",
        })
        observed = [
            e for e in await db.get_session_events(SID)
            if e["event_type"] == "effort_observed"
        ]
        assert [e["details"] for e in observed] == [
            {"agent_id": "agent-1", "agent_type": "general-purpose", "effort": "max"},
        ]
        assert observed[0]["actor_id"] is None   # runtime state: unattributed


# --------------------------------------------------------------------------- #
#  Claude backend: the flag on the CLI and the effort hook                     #
# --------------------------------------------------------------------------- #


def _claude_backend(tmp_path: Path) -> ClaudeBackend:
    cfg = NerveConfig.from_dict({"workspace": str(tmp_path)})
    return ClaudeBackend(SimpleNamespace(
        config=lambda: cfg, claude_plugins=lambda: [], managed_mcp=False,
    ))


def _spec(tmp_path: Path, model: str, effort: str, **kw) -> SessionSpec:
    return SessionSpec(
        session_id="sess-eff", source="web", model=model, effort=effort,
        system_prompt="p", cwd=str(tmp_path), **kw,
    )


class TestClaudeBackend:
    def test_the_session_effort_reaches_the_cli_command(self, tmp_path):
        from claude_agent_sdk._internal.transport.subprocess_cli import (
            SubprocessCLITransport,
        )

        backend = _claude_backend(tmp_path)
        with patch.object(backend, "_build_mcp_servers", return_value={}), \
             patch.object(backend, "_build_hooks", return_value={}):
            options = backend._build_options(_spec(tmp_path, "claude-opus-5-5", "medium"))
        assert options.effort == "medium"
        options.cli_path = "/nonexistent/claude"
        argv = SubprocessCLITransport(prompt="hi", options=options)._build_command()
        i = argv.index("--effort")
        assert argv[i + 1] == "medium"

    def test_supported_efforts(self):
        assert ClaudeBackend.supported_efforts("claude-sonnet-4-6") == ("low", "medium", "high")
        assert ClaudeBackend.supported_efforts("claude-opus-5-5") == EFFORT_LEVELS
        assert ClaudeBackend.supported_efforts("some-future-model") == EFFORT_LEVELS
        assert ClaudeBackend.supported_efforts(None) == EFFORT_LEVELS
        # The cap still uses the same table.
        assert ClaudeBackend._effective_effort("max", "claude-sonnet-4-6") == "high"
        assert ClaudeBackend._effective_effort("medium", "claude-sonnet-4-6") == "medium"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("permissions", [True, False])
    async def test_the_hook_reports_each_agent_once(self, permissions):
        seen: list[tuple[str, dict]] = []

        async def observe(sid, details):
            seen.append((sid, details))

        config = SimpleNamespace(agent=SimpleNamespace(
            background_agent_permissions=permissions,
            cli_max_message_bytes=64 * 1024 * 1024,
        ))
        backend = ClaudeBackend(SimpleNamespace(config=lambda: config))
        spec = SessionSpec(
            session_id="sess-obs", source="web", model="m", effort="medium",
            system_prompt="", cwd="/tmp", observe_effort=observe,
        )
        hooks = backend._build_hooks(spec)
        catch_all = [m for m in hooks["PreToolUse"] if m.matcher is None]
        assert len(catch_all) == 1   # one round trip per tool call, not two
        hook = catch_all[0].hooks[0]

        main = {"tool_name": "Bash", "effort": {"level": "medium"}}
        sub = {
            "tool_name": "Grep", "agent_id": "a1", "agent_type": "Explore",
            "effort": {"level": "medium"},
        }
        for hook_input in (main, main, sub, sub, {"tool_name": "Read"}):
            await hook(hook_input, "tid", None)
        assert seen == [
            ("sess-obs", {"agent_id": None, "agent_type": None, "effort": "medium"}),
            ("sess-obs", {"agent_id": "a1", "agent_type": "Explore", "effort": "medium"}),
        ]

    @pytest.mark.asyncio
    async def test_a_failing_observer_never_blocks_the_tool(self):
        async def boom(sid, details):
            raise RuntimeError("db down")

        config = SimpleNamespace(agent=SimpleNamespace(
            background_agent_permissions=True, cli_max_message_bytes=64 * 1024 * 1024,
        ))
        backend = ClaudeBackend(SimpleNamespace(config=lambda: config))
        spec = SessionSpec(
            session_id="sess-obs", source="web", model="m", effort="medium",
            system_prompt="", cwd="/tmp", observe_effort=boom,
        )
        hook = next(
            m for m in backend._build_hooks(spec)["PreToolUse"] if m.matcher is None
        ).hooks[0]
        out = await hook({"tool_name": "Bash", "effort": {"level": "low"}}, "tid", None)
        assert out["hookSpecificOutput"]["permissionDecision"] == "allow"


# --------------------------------------------------------------------------- #
#  Routes                                                                      #
# --------------------------------------------------------------------------- #


class _RouteBackend:
    def default_model(self, source):
        return "claude-opus-5-5"


@pytest.mark.asyncio
class TestRoutes:
    @pytest_asyncio.fixture
    async def setup(self, db: Database, bypass_auth, tmp_path):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        import nerve.config as cfg_mod
        from nerve.gateway.routes._deps import init_deps
        from nerve.gateway.routes.models import router as models_router
        from nerve.gateway.routes.sessions import router as sessions_router

        cfg = NerveConfig.from_dict({
            "workspace": str(tmp_path),
            "agent": {"effort": "max", "model_discovery": False},
        })
        cfg_mod._config = cfg
        await ensure_actor_row(db, TEST_ACTOR)
        sm = SessionManager(db)
        live = {"n": 0}
        engine = SimpleNamespace(
            config=cfg, sessions=sm, _backends={"claude": _RouteBackend()},
            has_live_background_tasks=lambda sid: False,
            agents=SimpleNamespace(
                has_live=lambda sid: live["n"] > 0, live_count=lambda sid: live["n"],
            ),
        )
        init_deps(engine=engine, db=db)  # type: ignore[arg-type]
        app = FastAPI()
        app.include_router(sessions_router)
        app.include_router(models_router)
        bypass_auth(app)
        yield SimpleNamespace(client=TestClient(app), db=db, live=live)
        cfg_mod._config = None

    async def _events(self, db, sid):
        return [
            e for e in await db.get_session_events(sid)
            if e["event_type"] == "effort_changed"
        ]

    async def test_create_with_an_effort(self, setup):
        r = setup.client.post("/api/sessions", json={"effort": "MEDIUM"})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["effort"] == "medium"
        assert session_effort(await setup.db.get_session(body["id"])) == "medium"
        # GET one and the feed carry it too.
        assert setup.client.get(f"/api/sessions/{body['id']}").json()["effort"] == "medium"
        feed = setup.client.get("/api/sessions").json()["sessions"]
        assert next(s for s in feed if s["id"] == body["id"])["effort"] == "medium"

    async def test_create_without_an_effort_is_default(self, setup):
        body = setup.client.post("/api/sessions", json={}).json()
        assert body["effort"] is None
        assert "effort" not in json.loads(
            (await setup.db.get_session(body["id"]))["metadata"] or "{}",
        )

    async def test_create_refuses_an_unknown_level(self, setup):
        r = setup.client.post("/api/sessions", json={"effort": "ultra"})
        assert r.status_code == 400
        assert "Unknown effort" in r.json()["detail"]
        assert setup.client.get("/api/sessions").json()["sessions"] == []

    async def test_patch_sets_changes_and_clears(self, setup):
        sid = setup.client.post("/api/sessions", json={}).json()["id"]

        r = setup.client.patch(f"/api/sessions/{sid}", json={"effort": "high"})
        assert r.status_code == 200, r.text
        assert r.json()["effort"] == "high"
        # Unchanged: 200, no event.
        r = setup.client.patch(f"/api/sessions/{sid}", json={"effort": "high"})
        assert r.status_code == 200 and r.json()["effort"] == "high"
        r = setup.client.patch(f"/api/sessions/{sid}", json={"effort": None})
        assert r.status_code == 200 and r.json()["effort"] is None
        r = setup.client.patch(f"/api/sessions/{sid}", json={"effort": "low", "title": "t"})
        assert r.status_code == 200
        assert (r.json()["effort"], r.json()["title"]) == ("low", "t")

        events = await self._events(setup.db, sid)
        assert [e["details"] for e in events] == [
            {"effort": "low", "previous": None},
            {"effort": None, "previous": "high"},
            {"effort": "high", "previous": None},
        ]
        assert all(e["actor_id"] == TEST_ACTOR.actor_id for e in events)

    async def test_patch_refuses_an_unknown_level(self, setup):
        sid = setup.client.post("/api/sessions", json={"effort": "low"}).json()["id"]
        r = setup.client.patch(f"/api/sessions/{sid}", json={"effort": "extreme"})
        assert r.status_code == 400
        assert session_effort(await setup.db.get_session(sid)) == "low"

    async def test_patch_refuses_a_switch_while_agents_run(self, setup):
        sid = setup.client.post("/api/sessions", json={"effort": "low"}).json()["id"]
        setup.live["n"] = 2
        r = setup.client.patch(f"/api/sessions/{sid}", json={"effort": "max"})
        assert r.status_code == 409
        detail = r.json()["detail"]
        assert (detail["code"], detail["live"]) == ("live_background_agents", 2)
        assert "effort" in detail["message"]
        # Re-sending the current value is not a switch.
        assert setup.client.patch(
            f"/api/sessions/{sid}", json={"effort": "low"},
        ).status_code == 200
        assert session_effort(await setup.db.get_session(sid)) == "low"

    async def test_models_list_the_effort_levels(self, setup):
        body = setup.client.get("/api/models").json()
        assert body["effort_default"] == "max"
        levels = body["effort_levels"]
        anthropic = [m["id"] for m in body["models"] if m["provider"] == "anthropic"]
        assert anthropic and set(levels) == set(anthropic)
        for model_id in anthropic:
            assert levels[model_id] == list(ClaudeBackend.supported_efforts(model_id))
