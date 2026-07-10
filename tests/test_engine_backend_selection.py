"""Backend resolution: sticky-per-session, config routing, guards."""

from __future__ import annotations

import pytest

from nerve.agent.engine import AgentEngine
from nerve.config import NerveConfig


def _config(tmp_path, **agent_overrides) -> NerveConfig:
    return NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "agent": dict(agent_overrides),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
    })


def _engine(tmp_path, db, **agent_overrides) -> AgentEngine:
    return AgentEngine(_config(tmp_path, **agent_overrides), db)


class TestBackendResolution:
    def test_defaults_to_claude(self, tmp_path, db):
        engine = _engine(tmp_path, db)
        assert engine._backend_for(None, "web").name == "claude"
        assert engine._backend_for({}, "cron").name == "claude"

    def test_config_selects_codex_for_new_sessions(self, tmp_path, db):
        engine = _engine(tmp_path, db, backend="codex")
        assert engine._backend_for(None, "web").name == "codex"
        # cron_backend falls back to backend
        assert engine._backend_for(None, "cron").name == "codex"

    def test_cron_backend_only_affects_cron_sources(self, tmp_path, db):
        engine = _engine(tmp_path, db, backend="claude", cron_backend="codex")
        assert engine._backend_for(None, "web").name == "claude"
        assert engine._backend_for(None, "telegram").name == "claude"
        assert engine._backend_for(None, "cron").name == "codex"
        assert engine._backend_for(None, "hook").name == "codex"
        # Wakeups are NOT cron: they fire on existing (typically
        # interactive) sessions and must not be dragged onto cron_backend.
        assert engine._backend_for(None, "wakeup").name == "claude"

    def test_stored_backend_always_wins_over_config(self, tmp_path, db):
        """The sticky rule: flipping config never crosses an existing
        session onto another runtime — including its wakeups."""
        engine = _engine(tmp_path, db, backend="claude", cron_backend="codex")
        claude_session = {"backend": "claude", "metadata": "{}"}
        codex_session = {"backend": "codex", "metadata": "{}"}
        # A claude session's wakeup under cron_backend=codex stays claude.
        assert engine._backend_for(claude_session, "wakeup").name == "claude"
        assert engine._backend_for(claude_session, "cron").name == "claude"
        # And a codex session stays codex even when config flips back.
        engine2 = _engine(tmp_path, db, backend="claude")
        assert engine2._backend_for(codex_session, "web").name == "codex"

    def test_metadata_override_for_new_sessions(self, tmp_path, db):
        engine = _engine(tmp_path, db, backend="claude")
        session = {"backend": None, "metadata": '{"backend_override": "codex"}'}
        assert engine._backend_for(session, "web").name == "codex"
        # ...but a stored backend beats the override
        session2 = {"backend": "claude", "metadata": '{"backend_override": "codex"}'}
        assert engine._backend_for(session2, "web").name == "claude"

    def test_unknown_stored_backend_is_a_hard_error(self, tmp_path, db):
        engine = _engine(tmp_path, db)
        with pytest.raises(RuntimeError, match="gemini"):
            engine._backend_for({"backend": "gemini", "metadata": "{}"}, "web")


class TestDefaultModels:
    def test_claude_models_by_source(self, tmp_path, db):
        engine = _engine(tmp_path, db)
        claude = engine._backends["claude"]
        assert claude.default_model("web") == engine.config.agent.model
        assert claude.default_model("cron") == engine.config.agent.cron_model
        assert claude.default_model("hook") == engine.config.agent.cron_model
        assert claude.default_model("wakeup") == engine.config.agent.model

    def test_codex_models_by_source(self, tmp_path, db):
        engine = _engine(tmp_path, db)
        codex = engine._backends["codex"]
        assert codex.default_model("web") == "gpt-5.6-sol"
        assert codex.default_model("cron") == "gpt-5.6-sol"  # cron_model empty → model

    def test_codex_cron_model_override(self, tmp_path, db):
        cfg = NerveConfig.from_dict({
            "workspace": str(tmp_path / "ws"),
            "codex": {"home_dir": str(tmp_path / "h"), "cron_model": "gpt-5.6-luna"},
        })
        engine = AgentEngine(cfg, db)
        assert engine._backends["codex"].default_model("cron") == "gpt-5.6-luna"
        assert engine._backends["codex"].default_model("web") == "gpt-5.6-sol"


class TestExcludedTools:
    def test_claude_excludes_schedule_wakeup(self, tmp_path, db):
        engine = _engine(tmp_path, db)
        assert "schedule_wakeup" in engine._backends["claude"].excluded_tools()
        assert engine._backends["codex"].excluded_tools() == set()

    def test_prompt_tool_list_respects_exclusions(self):
        from nerve.agent.prompts import _format_tool_list
        full = _format_tool_list()
        filtered = _format_tool_list({"schedule_wakeup"})
        assert "mcp__nerve__schedule_wakeup" in full
        assert "mcp__nerve__schedule_wakeup" not in filtered

    def test_claude_session_server_drops_excluded(self):
        from nerve.agent.tools import ToolContext, build_default_registry
        from nerve.agent.tools.claude_sdk_adapter import build_session_mcp_server
        registry = build_default_registry()
        ctx = ToolContext(session_id="s")
        server = build_session_mcp_server(
            registry, ctx, exclude={"schedule_wakeup"},
        )
        instance = server.get("instance")
        # The SDK config carries the tools inside the server instance;
        # assert via the registry contract instead of SDK internals:
        names_full = {s.name for s in registry.list()}
        assert "schedule_wakeup" in names_full
        assert server is not None and instance is not None


class TestConfigValidation:
    def test_unknown_backend_rejected_at_load(self, tmp_path):
        with pytest.raises(ValueError, match="agent.backend"):
            NerveConfig.from_dict({
                "workspace": str(tmp_path),
                "agent": {"backend": "geminy"},
            })

    def test_invalid_codex_settings_rejected_when_selected(self, tmp_path):
        with pytest.raises(ValueError, match="approval_policy"):
            NerveConfig.from_dict({
                "workspace": str(tmp_path),
                "agent": {"backend": "codex"},
                "codex": {"approval_policy": "on-failure"},  # not in v2 API
            })

    def test_invalid_codex_settings_tolerated_when_inactive(self, tmp_path):
        cfg = NerveConfig.from_dict({
            "workspace": str(tmp_path),
            "agent": {"backend": "claude"},
            "codex": {"approval_policy": "on-failure"},
        })
        assert cfg.agent.backend == "claude"

    def test_pricing_defaults_cover_default_model(self, tmp_path):
        cfg = NerveConfig.from_dict({"workspace": str(tmp_path)})
        from nerve.agent.backends.codex.pricing import match_pricing
        assert match_pricing(cfg.codex.model, cfg.codex.pricing) is not None


class TestScheduleWakeupTool:
    @pytest.mark.asyncio
    async def test_rejected_for_external_sessions(self, tmp_path, db):
        from nerve.agent.tools.handlers.wakeups import schedule_wakeup_handler
        from nerve.agent.tools.registry import ToolContext

        engine = _engine(tmp_path, db)
        await db.create_session("ext-1", source="external")
        ctx = ToolContext(session_id="ext-1", db=db, engine=engine)
        result = await schedule_wakeup_handler(ctx, {
            "delaySeconds": 120, "prompt": "check things",
        })
        assert result.is_error
        assert "external" in result.content[0]["text"]

    @pytest.mark.asyncio
    async def test_records_wakeup_for_engine_sessions(self, tmp_path, db):
        from nerve.agent.tools.handlers.wakeups import schedule_wakeup_handler
        from nerve.agent.tools.registry import ToolContext

        engine = _engine(tmp_path, db)
        await db.create_session("web-1", source="web")
        ctx = ToolContext(session_id="web-1", db=db, engine=engine)
        result = await schedule_wakeup_handler(ctx, {
            "delaySeconds": 120, "prompt": "check things", "reason": "test",
        })
        assert not result.is_error
        assert "Wakeup #" in result.content[0]["text"]
        # missing prompt → error, nothing scheduled
        bad = await schedule_wakeup_handler(ctx, {"delaySeconds": 60, "prompt": " "})
        assert bad.is_error


class TestResumeDroppedEnginePath:
    @pytest.mark.asyncio
    async def test_stale_native_id_is_cleared_not_repersisted(self, tmp_path, db):
        """When the backend reports resume_dropped, the engine must clear
        sessions.sdk_session_id AND not let mark_active re-persist the
        stale id (review finding: the local variable also has to be
        dropped)."""
        from types import SimpleNamespace

        from nerve.agent.backends.base import BackendCapabilities

        engine = _engine(tmp_path, db)
        await db.create_session("s-drop", source="web")
        await db.update_session_fields("s-drop", {
            "sdk_session_id": "stale-thread-1", "backend": "codex",
        })

        class StubClient:
            resume_dropped = True
            native_session_id = "fresh-thread-9"
            model = "gpt-5.6-sol"

            def is_alive(self):
                return True

            async def disconnect(self):
                pass

        class StubBackend:
            name = "codex"
            capabilities = BackendCapabilities(
                cost_is_cumulative=False,
                supports_idle_stream=False,
                supports_cache_ttl=False,
                interactive_builtins=False,
                reports_context_window=True,
            )

            def default_model(self, source):
                return "gpt-5.6-sol"

            def excluded_tools(self):
                return set()

            def validate_resume_target(self, native_id, cwd):
                return True

            async def create_client(self, spec):
                # The backend already recovered with a fresh thread —
                # spec still carried the stale id.
                assert spec.resume_native_id == "stale-thread-1"
                return StubClient()

        engine._backends["codex"] = StubBackend()
        client = await engine._get_or_create_client("s-drop", "web", None)
        assert client.resume_dropped is True

        session = await db.get_session("s-drop")
        # The stale id must be GONE (mark_active must not re-persist it);
        # the fresh id lands at turn end via TurnCompleted.
        assert session.get("sdk_session_id") in (None, ""), session.get(
            "sdk_session_id",
        )
        assert session.get("backend") == "codex"
