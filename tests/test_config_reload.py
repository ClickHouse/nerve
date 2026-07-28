"""Tests for unified config hot-reload."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

import nerve.sources.registry as registry
from nerve.config_reload import reload_all, reload_failures
from nerve.cron.service import CronService


def _fake_runner(job_id, source_name):
    return SimpleNamespace(
        job_id=job_id,
        source=SimpleNamespace(source_name=source_name),
        set_notification_service=lambda *a, **k: None,
    )


@pytest_asyncio.fixture
async def svc():
    config = MagicMock()
    config.timezone = "UTC"
    config.sync.gmail.schedule = "5m"
    config.sync.github.schedule = "10m"
    db = AsyncMock()
    db.get_last_successful_cron_run = AsyncMock(return_value=None)
    service = CronService(config, AsyncMock(), db)
    service.scheduler.start(paused=True)
    try:
        yield service
    finally:
        service.scheduler.shutdown(wait=False)


class TestReloadSources:
    @pytest.mark.asyncio
    async def test_reschedules_new_removes_old(self, svc, monkeypatch):
        # Initially one source scheduled.
        monkeypatch.setattr(
            registry, "build_source_runners",
            lambda config, db: [_fake_runner("source:gmail", "gmail")],
        )
        svc._register_source_runners()
        assert svc.scheduler.get_job("source:gmail") is not None

        # Config now has a different source — reload swaps them.
        monkeypatch.setattr(
            registry, "build_source_runners",
            lambda config, db: [_fake_runner("source:github", "github")],
        )
        result = await svc.reload_sources()
        assert svc.scheduler.get_job("source:github") is not None
        assert svc.scheduler.get_job("source:gmail") is None
        assert result["removed"] == ["source:gmail"]
        assert result["sources"] == ["source:github"]

    @pytest.mark.asyncio
    async def test_build_failure_keeps_old_sources(self, svc, monkeypatch):
        """A build failure during reload must NOT silently unschedule sources."""
        monkeypatch.setattr(
            registry, "build_source_runners",
            lambda config, db: [_fake_runner("source:gmail", "gmail")],
        )
        svc._register_source_runners()
        assert svc.scheduler.get_job("source:gmail") is not None

        def _boom(config, db):
            raise RuntimeError("bad source config")

        monkeypatch.setattr(registry, "build_source_runners", _boom)
        with pytest.raises(RuntimeError):
            await svc.reload_sources()
        # Old source still scheduled — built new BEFORE removing old.
        assert svc.scheduler.get_job("source:gmail") is not None

    @pytest.mark.asyncio
    async def test_notification_service_rewired_on_reload(self, svc, monkeypatch):
        wired = []
        runner = SimpleNamespace(
            job_id="source:gmail",
            source=SimpleNamespace(source_name="gmail"),
            set_notification_service=lambda ns: wired.append(ns),
        )
        monkeypatch.setattr(registry, "build_source_runners", lambda config, db: [runner])
        svc.notification_service = object()
        await svc.reload_sources()
        assert wired == [svc.notification_service]


class TestReloadAll:
    @pytest.mark.asyncio
    async def test_summary_covers_all_subsystems(self, tmp_path):
        cron = MagicMock()
        cron.reload = AsyncMock(return_value={"added": [], "removed": [], "updated": [], "enabled": 0})
        cron.reload_sources = AsyncMock(return_value={"sources": [], "removed": []})
        engine = MagicMock()
        engine.reload_mcp_config = AsyncMock(return_value=[])
        engine._skill_manager = MagicMock()
        engine._skill_manager.discover = AsyncMock(return_value=[1, 2])

        summary = await reload_all(engine, cron, tmp_path)
        assert summary["config"] == "reloaded"
        assert summary["cron"]["enabled"] == 0
        assert summary["sources"] == {"sources": [], "removed": []}
        assert summary["mcp"] == "0 server(s)"
        assert summary["skills"] == "2 discovered"
        cron.reload.assert_awaited_once()
        cron.reload_sources.assert_awaited_once()
        engine.reload_mcp_config.assert_awaited_once()
        engine._skill_manager.discover.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_per_subsystem_error_isolated(self, tmp_path):
        cron = MagicMock()
        cron.reload = AsyncMock(side_effect=RuntimeError("boom"))
        cron.reload_sources = AsyncMock(return_value={"sources": [], "removed": []})
        engine = MagicMock()
        engine.reload_mcp_config = AsyncMock(return_value=[])
        engine._skill_manager = MagicMock()
        engine._skill_manager.discover = AsyncMock(return_value=[])

        summary = await reload_all(engine, cron, tmp_path)
        assert summary["cron"].startswith("error:")
        # Other subsystems still ran despite cron failing.
        assert summary["mcp"] == "0 server(s)"
        assert summary["skills"] == "0 discovered"

    @pytest.mark.asyncio
    async def test_no_engine_no_cron(self, tmp_path):
        summary = await reload_all(None, None, tmp_path)
        assert summary["config"] == "reloaded"
        assert "cron" not in summary and "mcp" not in summary

    @pytest.mark.asyncio
    async def test_config_load_error_reported_not_applied(self, tmp_path, monkeypatch):
        import nerve.config as cfgmod

        def _raise(config_dir=None):
            raise cfgmod.ConfigError("locked but no jwt_secret")

        monkeypatch.setattr(cfgmod, "load_config", _raise)
        summary = await reload_all(None, None, tmp_path)
        assert summary["config"].startswith("error:")
        assert "jwt_secret" in summary["config"]

    @pytest.mark.asyncio
    async def test_reloads_cron_jobs_and_sources_together(self, tmp_path):
        """Both, on every reload: a caller has no way to ask for one and not the
        other, so a jobs edit and a sources edit can never be applied apart."""
        cron = MagicMock()
        cron.reload = AsyncMock(return_value={"added": [], "removed": []})
        cron.reload_sources = AsyncMock(return_value={"sources": [], "removed": []})
        summary = await reload_all(None, cron, tmp_path)
        cron.reload.assert_awaited_once()
        cron.reload_sources.assert_awaited_once()
        assert "cron" in summary and "sources" in summary


class TestSourceReloadScope:
    def test_sync_codex_is_not_a_cron_source(self):
        """``sync.codex`` sits on the same dataclass as the cron sources but
        belongs to the Codex thread-sync service, which is built once at
        start-up. A reload rebuilds cron source runners and touches none of it,
        so the hot-reload table must not say "``sync.*``" — an operator who added
        a Codex origin would read ``ok: true`` and get no ingestion.
        """
        from nerve.config import NerveConfig
        from nerve.sources.registry import build_source_runners

        off = NerveConfig()
        off.sync.codex.enabled = False
        on = NerveConfig()
        on.sync.codex.enabled = True

        before = [r.job_id for r in build_source_runners(off, db=None)]
        after = [r.job_id for r in build_source_runners(on, db=None)]
        assert after == before  # turning it on changes nothing here
        assert "source:codex" not in after


class TestReloadFailures:
    """The summary is a report, and ``reload_failures`` is how a caller reads it
    instead of assuming it."""

    def test_only_marked_strings_count_as_failures(self):
        summary = {
            "config": "reloaded",
            "cron": {"added": [], "enabled": 2},          # a dict is not an error
            "mcp": "0 server(s)",
            "skills": "error: skills dir vanished",
        }
        assert reload_failures(summary) == {"skills": "skills dir vanished"}

    def test_empty_for_a_clean_reload(self):
        assert reload_failures({"config": "reloaded", "mcp": "1 server(s)"}) == {}


def _write_config(config_dir, workspace, body=""):
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "config.yaml").write_text(
        f"workspace: {workspace}\n{body}", encoding="utf-8",
    )


class TestConfigObjectIsReplaced:
    """Drives the real ``nerve.config`` singleton rather than a stand-in.

    The point of these is that ``get_config()`` only loads when its global is
    ``None``, so for most of this daemon's life the loaded config is immortal.
    A test that patches ``get_config`` would pass whether or not a reload
    actually replaces it, which is exactly how the staleness went unnoticed.
    """

    @pytest.mark.asyncio
    async def test_reload_picks_up_an_edited_file(self, tmp_path, monkeypatch):
        import nerve.config as cfgmod

        config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
        ws.mkdir()
        _write_config(config_dir, ws, "timezone: UTC\n")
        monkeypatch.setattr(cfgmod, "_config", cfgmod.load_config(config_dir))
        started_with = cfgmod.get_config()
        assert started_with.timezone == "UTC"

        _write_config(config_dir, ws, "timezone: Europe/Berlin\n")
        # Reading it again without a reload still gives the start-up object.
        assert cfgmod.get_config() is started_with

        summary = await reload_all(None, None, config_dir)
        assert summary["config"] == "reloaded"
        assert cfgmod.get_config() is not started_with
        assert cfgmod.get_config().timezone == "Europe/Berlin"

    @pytest.mark.asyncio
    async def test_a_changed_port_is_reported_as_needing_a_restart(
        self, tmp_path, monkeypatch,
    ):
        """The socket is already bound, so the reload reports rather than applies.

        Previously the summary listed only what was applied, so a port change was
        indistinguishable from no change. That was hard to hit while host/port
        were machine-local and needed a local edit; they are in the tracked
        settings now, so the change can arrive by workspace sync.
        """
        import nerve.config as cfgmod

        config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
        ws.mkdir()
        _write_config(config_dir, ws, "gateway:\n  port: 8900\n")
        monkeypatch.setattr(cfgmod, "_config", cfgmod.load_config(config_dir))

        _write_config(config_dir, ws, "gateway:\n  port: 9100\n")
        summary = await reload_all(None, None, config_dir)

        assert summary["config"] == "reloaded"
        assert "gateway.port" in summary["restart_required"]
        assert "8900" in summary["restart_required"]
        assert "9100" in summary["restart_required"]
        # Nothing failed: the reload did everything it can do.
        assert reload_failures(summary) == {}

    @pytest.mark.asyncio
    async def test_a_hot_reloadable_change_claims_no_restart(
        self, tmp_path, monkeypatch,
    ):
        """A hot-reloadable change must leave the field absent, so that its
        presence carries information rather than firing on every reload."""
        import nerve.config as cfgmod

        config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
        ws.mkdir()
        _write_config(config_dir, ws, "agent:\n  max_turns: 50\n")
        monkeypatch.setattr(cfgmod, "_config", cfgmod.load_config(config_dir))

        _write_config(config_dir, ws, "agent:\n  max_turns: 80\n")
        summary = await reload_all(None, None, config_dir)

        assert summary["config"] == "reloaded"
        assert "restart_required" not in summary

    @pytest.mark.asyncio
    async def test_a_failed_load_leaves_the_previous_config_in_place(
        self, tmp_path, monkeypatch,
    ):
        """Continuing on stale config is the deliberate choice; silently serving
        a half-built one would not be."""
        import nerve.config as cfgmod

        config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
        ws.mkdir()
        _write_config(config_dir, ws, "timezone: UTC\n")
        monkeypatch.setattr(cfgmod, "_config", cfgmod.load_config(config_dir))
        started_with = cfgmod.get_config()

        (config_dir / "config.yaml").write_text("timezone: [oops\n", encoding="utf-8")
        summary = await reload_all(None, None, config_dir)
        assert "config" in reload_failures(summary)
        assert cfgmod.get_config() is started_with

    @pytest.mark.asyncio
    async def test_the_agent_backends_follow_the_engine(self, tmp_path, monkeypatch):
        """The seam the whole exercise exists to close.

        The engine and its backends both answer questions about ``agent.*`` and
        ``codex.*``. When the backends cached the start-up object, a reload moved
        the engine and left them behind — so the context bar computed a 200k
        budget off the engine while the Claude backend kept sending the 1M header
        on every new session, and the reload reported ``ok`` with no errors. The
        codex backend cached one level deeper still (``config.codex``), so
        re-pointing ``.config`` would not have been enough either.
        """
        import nerve.config as cfgmod
        from nerve.agent.engine import AgentEngine

        config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
        ws.mkdir()
        _write_config(config_dir, ws, (
            "agent:\n  context_1m: true\ncodex:\n  sandbox: danger-full-access\n"
        ))
        monkeypatch.setattr(cfgmod, "_config", cfgmod.load_config(config_dir))
        engine = AgentEngine(cfgmod.get_config(), db=None)
        claude, codex = engine._backends["claude"], engine._backends["codex"]
        assert claude.config.agent.context_1m_enabled_for(None) is True
        assert codex.codex.sandbox == "danger-full-access"

        _write_config(config_dir, ws, (
            "agent:\n  context_1m: false\ncodex:\n  sandbox: read-only\n"
        ))
        await reload_all(engine, None, config_dir)

        new = cfgmod.get_config()
        assert engine.config is new
        assert claude.config is new and codex.config is new
        # The engine's answer and the backend's answer are the same answer.
        assert engine.config.agent.context_1m_enabled_for(None) is False
        assert claude.config.agent.context_1m_enabled_for(None) is False
        assert codex.codex.sandbox == "read-only"

    @pytest.mark.asyncio
    async def test_backend_choice_is_not_split_across_the_engine(
        self, tmp_path, monkeypatch,
    ):
        """Backend resolution reads ``agent.backend`` off the live engine config,
        but the session manager holds its own default for the rows it creates.
        Both have to move, or new sessions route by the old default while every
        other read reports the new one.
        """
        import nerve.config as cfgmod
        from nerve.agent.engine import AgentEngine

        config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
        ws.mkdir()
        _write_config(config_dir, ws, (
            "agent:\n  backend: claude\n  model: model-old\n"
            "sessions:\n  sticky_period_minutes: 120\n"
        ))
        monkeypatch.setattr(cfgmod, "_config", cfgmod.load_config(config_dir))
        engine = AgentEngine(cfgmod.get_config(), db=None)
        assert engine.sessions.default_backend == "claude"
        cwd_at_startup = engine.sessions.default_cwd

        _write_config(config_dir, ws, (
            "agent:\n  backend: codex\n  model: model-new\n"
            "sessions:\n  sticky_period_minutes: 30\n"
        ))
        await reload_all(engine, None, config_dir)

        assert engine.config.agent.backend == "codex"
        assert engine.sessions.default_backend == "codex"
        assert engine.sessions.cron_backend == "codex"
        assert engine.sessions.backend_models["claude"] == "model-new"
        assert engine.sessions.sticky_period_minutes == 30
        # workspace stays put: the skill manager, tool context and memory bridges
        # all captured it, so following it here alone would split the daemon.
        assert engine.sessions.default_cwd == cwd_at_startup

    @pytest.mark.asyncio
    async def test_services_holding_the_old_object_are_re_pointed(
        self, tmp_path, monkeypatch,
    ):
        """A daemon half on the new config and half on the old is worse than one
        uniformly stale: nothing tells you which half you are looking at. So the
        services that kept a reference get handed the new object too.
        """
        import nerve.config as cfgmod

        config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
        ws.mkdir()
        _write_config(config_dir, ws)
        old = cfgmod.load_config(config_dir)
        monkeypatch.setattr(cfgmod, "_config", old)

        notifications = SimpleNamespace(config=old)
        engine = SimpleNamespace(config=old, notification_service=notifications)
        cron = SimpleNamespace(config=old)

        await reload_all(engine, cron, config_dir)

        new = cfgmod.get_config()
        assert new is not old
        assert engine.config is new
        assert cron.config is new
        assert notifications.config is new

    @pytest.mark.asyncio
    async def test_external_agents_sweeper_is_re_pointed(self, tmp_path, monkeypatch):
        """The add/remove/toggle routes edit the process config *in place*, so a
        sweeper left on the previous object would keep rendering the old target
        list while every toggle reported success.
        """
        import nerve.config as cfgmod
        from nerve.external_agents.sync_service import SyncService
        from nerve.gateway.routes import _deps

        config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
        ws.mkdir()
        _write_config(config_dir, ws)
        old = cfgmod.load_config(config_dir)
        monkeypatch.setattr(cfgmod, "_config", old)

        sweeper = SyncService(old)
        monkeypatch.setattr(
            _deps, "_deps",
            _deps.RouteDeps(engine=None, db=None, external_agents_sync=sweeper),
        )

        await reload_all(None, None, config_dir)
        assert sweeper._config is cfgmod.get_config()
        assert sweeper._config is not old

    @pytest.mark.asyncio
    async def test_workflow_run_service_is_re_pointed(self, tmp_path, monkeypatch):
        """Budget enforcement is the point of this service, so it must not be
        the half of the process still reading the old ceiling. It reads through
        its own reference at use, so re-pointing is all it takes.
        """
        import nerve.config as cfgmod
        import nerve.config_reload as reload_mod

        config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
        ws.mkdir()
        _write_config(config_dir, ws)
        old = cfgmod.load_config(config_dir)
        monkeypatch.setattr(cfgmod, "_config", old)

        service = SimpleNamespace(config=old)
        monkeypatch.setattr(reload_mod, "_workflow_run_service", lambda: service)

        await reload_all(None, None, config_dir)
        assert service.config is cfgmod.get_config()
        assert service.config is not old

    @pytest.mark.asyncio
    async def test_no_workflow_run_service_is_not_a_failure(self, tmp_path, monkeypatch):
        """The CLI and the tests have no gateway, so the lookup returning None
        is the normal case, not something to report."""
        import nerve.config as cfgmod
        import nerve.config_reload as reload_mod

        config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
        ws.mkdir()
        _write_config(config_dir, ws)
        monkeypatch.setattr(cfgmod, "_config", cfgmod.load_config(config_dir))
        monkeypatch.setattr(reload_mod, "_workflow_run_service", lambda: None)

        summary = await reload_all(None, None, config_dir)
        assert "workflow run service" not in summary.get("services", "")

    @pytest.mark.asyncio
    async def test_a_holder_that_refuses_the_new_config_is_reported(
        self, tmp_path, monkeypatch,
    ):
        """Loading the config and failing to hand it on is its own outcome — the
        one where the daemon really is running two configurations at once."""
        import nerve.config as cfgmod

        config_dir, ws = tmp_path / "cfg", tmp_path / "ws"
        ws.mkdir()
        _write_config(config_dir, ws)
        monkeypatch.setattr(cfgmod, "_config", cfgmod.load_config(config_dir))

        class Stubborn:
            @property
            def config(self):
                return None  # no setter: assignment raises

        summary = await reload_all(None, Stubborn(), config_dir)
        assert summary["config"] == "reloaded"  # the load itself was fine
        assert "services" in reload_failures(summary)
        assert "cron service" in summary["services"]


class TestReloadRoute:
    @pytest.mark.asyncio
    async def test_route_returns_summary(self, tmp_path, monkeypatch):
        import nerve.gateway.server as srv
        import nerve.gateway.routes.config as route_mod

        fake_cfg = SimpleNamespace(config_dir=str(tmp_path), workspace=str(tmp_path))
        monkeypatch.setattr("nerve.config.get_config", lambda: fake_cfg)
        monkeypatch.setattr(srv, "_cron_service", None, raising=False)
        monkeypatch.setattr(route_mod, "get_deps", lambda: SimpleNamespace(engine=None))
        monkeypatch.setattr(
            "nerve.config_reload.reload_all",
            AsyncMock(return_value={"config": "reloaded"}),
        )
        result = await route_mod.reload_config_route(user={})
        assert result["ok"] is True
        assert result["detail"] == {"config": "reloaded"}
        assert result["errors"] == {}

    @pytest.mark.asyncio
    async def test_route_does_not_claim_a_reload_that_failed(self, tmp_path, monkeypatch):
        """The route used to answer ``reloaded: True`` whatever came back, so a
        settings.yaml the daemon could not load was a 200 with the reason buried
        in a free-text field.
        """
        import nerve.gateway.server as srv
        import nerve.gateway.routes.config as route_mod

        fake_cfg = SimpleNamespace(config_dir=str(tmp_path), workspace=str(tmp_path))
        monkeypatch.setattr("nerve.config.get_config", lambda: fake_cfg)
        monkeypatch.setattr(srv, "_cron_service", None, raising=False)
        monkeypatch.setattr(route_mod, "get_deps", lambda: SimpleNamespace(engine=None))
        monkeypatch.setattr(
            "nerve.config_reload.reload_all",
            AsyncMock(return_value={
                "config": "error: bad yaml", "cron": {"enabled": 3},
            }),
        )
        result = await route_mod.reload_config_route(user={})
        assert result["ok"] is False
        assert result["errors"] == {"config": "bad yaml"}
        # The rest still ran — best-effort is the point, and the detail shows it.
        assert result["detail"]["cron"] == {"enabled": 3}
