"""A startup that fails leaves nothing running.

The lifespan's shutdown half runs only after the ``yield``. These tests check
that:

* the database opens and the identity bootstraps before anything starts;
* when a later step fails, everything already started is stopped in reverse
  order. The proxy matters most: its detached subprocess keeps the port.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.config import NerveConfig


class _Recorder:
    """A service that counts its starts and stops."""

    def __init__(self, fail: bool = False):
        self.starts = 0
        self.stops = 0
        self.fail = fail

    async def start(self):
        self.starts += 1
        if self.fail:
            raise RuntimeError("no port for you")

    async def stop(self):
        self.stops += 1


@pytest.fixture
def harness(tmp_path, monkeypatch):
    """Drive the real lifespan with fakes for everything it starts."""
    import nerve.config as config_module
    import nerve.proxy.service as proxy_module
    from nerve.gateway import server as gw

    config = NerveConfig()
    config.workspace = tmp_path / "workspace"
    config.workspace.mkdir(parents=True, exist_ok=True)
    config.anthropic_api_key = ""
    config.telegram.enabled = False
    config.proxy.enabled = True
    config.mcp_endpoint.enabled = False
    config.agent.model_discovery = False
    config.external_agents.enabled = False
    config.workspace_sync.enabled = False
    config_module._config = config

    proxy = _Recorder()
    monkeypatch.setattr(proxy_module, "ProxyService", lambda cfg: proxy)

    engine = MagicMock()
    engine.initialize = AsyncMock()
    engine.shutdown = AsyncMock()
    engine.registry = MagicMock()
    engine.router = MagicMock()
    engine.set_notification_service = MagicMock()
    engine.resume_enrolled_sessions = AsyncMock(return_value=0)
    monkeypatch.setattr(gw, "AgentEngine", lambda cfg, db: engine)
    monkeypatch.setattr(gw, "init_langfuse", lambda cfg: None)
    monkeypatch.setattr(gw, "init_deps", lambda *a, **k: None)

    opened: list[str] = []
    closed: list[str] = []

    async def _init_db(*a, **k):
        opened.append("db")
        return MagicMock(
            cleanup_expired_messages=AsyncMock(return_value=0),
            login_state=AsyncMock(
                return_value=MagicMock(passwordless=False),
            ),
            _delete_instance_secret=AsyncMock(return_value=False),
        )

    async def _close_db():
        closed.append("db")

    monkeypatch.setattr(gw, "init_db", _init_db)
    monkeypatch.setattr(gw, "close_db", _close_db)
    monkeypatch.setattr(
        "nerve.migrate.bootstrap_identity",
        AsyncMock(return_value=MagicMock(identity_actions=[])),
    )
    monkeypatch.setattr(
        "nerve.notifications.service.NotificationService",
        lambda *a, **k: MagicMock(
            send_notification=AsyncMock(), hide_session_label_for=MagicMock(),
        ),
    )
    monkeypatch.setattr(gw, "set_notification_service", lambda *a, **k: None)

    yield {
        "server": gw, "proxy": proxy, "engine": engine,
        "opened": opened, "closed": closed, "config": config,
    }

    gw._engine = None
    gw._cron_service = None
    config_module._config = None


@pytest.mark.asyncio
async def test_a_refused_database_never_starts_the_proxy(harness, monkeypatch):
    """The database opens first, so a state-file refusal leaves nothing to
    stop."""
    from nerve.db.base import InsecureStateStorage

    async def refuse(*a, **k):
        raise InsecureStateStorage("Refusing to open nerve.db: 0666")

    monkeypatch.setattr(harness["server"], "init_db", refuse)

    with pytest.raises(InsecureStateStorage):
        async with harness["server"].lifespan(MagicMock()):
            pass  # pragma: no cover - startup must not reach the body

    assert harness["proxy"].starts == 0
    assert harness["proxy"].stops == 0
    assert harness["engine"].initialize.await_count == 0


@pytest.mark.asyncio
async def test_a_failure_after_the_proxy_stops_it_again(harness, monkeypatch):
    """When the engine fails, everything already started is stopped in
    reverse order, including the proxy."""
    harness["engine"].initialize.side_effect = RuntimeError("engine exploded")

    with pytest.raises(RuntimeError, match="engine exploded"):
        async with harness["server"].lifespan(MagicMock()):
            pass  # pragma: no cover

    assert harness["proxy"].starts == 1 and harness["proxy"].stops == 1
    assert harness["closed"] == ["db"]  # the database was closed too
    # The failed engine is shut down too: initialize() may already have
    # started memU's non-daemon thread, which keeps the process alive.
    assert harness["engine"].shutdown.await_count == 1


@pytest.mark.asyncio
async def test_cancellation_while_the_proxy_is_starting_still_stops_it(harness):
    """The subprocess exists while ``start()`` polls for health. A
    cancellation during that poll still stops it."""
    started = asyncio.Event()

    async def start_then_hang():
        harness["proxy"].starts += 1
        started.set()
        await asyncio.Event().wait()  # the health poll that never completes

    harness["proxy"].start = start_then_hang

    async def _run():
        async with harness["server"].lifespan(MagicMock()):
            pass  # pragma: no cover

    task = asyncio.create_task(_run())
    await asyncio.wait_for(started.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert harness["proxy"].starts == 1 and harness["proxy"].stops == 1
    assert harness["closed"] == ["db"]


@pytest.mark.asyncio
async def test_cancellation_inside_initialize_still_shuts_the_engine_down(harness):
    """``initialize()`` starts memU's thread partway through, so a
    cancellation inside it still reaches ``shutdown()``."""
    started = asyncio.Event()

    async def initialize_then_hang():
        started.set()  # stands in for "memU's thread is up"
        await asyncio.Event().wait()

    harness["engine"].initialize.side_effect = initialize_then_hang

    async def _run():
        async with harness["server"].lifespan(MagicMock()):
            pass  # pragma: no cover

    task = asyncio.create_task(_run())
    await asyncio.wait_for(started.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert harness["engine"].shutdown.await_count == 1
    assert harness["proxy"].stops == 1
    assert harness["closed"] == ["db"]


@pytest.mark.asyncio
async def test_one_cancelled_cleanup_does_not_abandon_the_rest(harness, monkeypatch):
    """A cancelled stop does not skip the cleanups registered before it."""
    harness["engine"].initialize.side_effect = RuntimeError("engine exploded")

    async def cancelled_stop():
        raise asyncio.CancelledError()

    monkeypatch.setattr(harness["proxy"], "stop", cancelled_stop)

    with pytest.raises(RuntimeError, match="engine exploded"):
        async with harness["server"].lifespan(MagicMock()):
            pass  # pragma: no cover

    # The proxy's stop raised; the database, registered before it, closed.
    assert harness["closed"] == ["db"]


@pytest.mark.asyncio
async def test_a_failing_stop_does_not_hide_the_startup_failure(harness, monkeypatch):
    """A stop that raises is logged, and the startup failure propagates."""
    import logging

    harness["engine"].initialize.side_effect = RuntimeError("engine exploded")

    async def angry_stop():
        harness["proxy"].stops += 1
        raise RuntimeError("stop failed too")

    monkeypatch.setattr(harness["proxy"], "stop", angry_stop)

    with pytest.raises(RuntimeError, match="engine exploded"):
        async with harness["server"].lifespan(MagicMock()):
            pass  # pragma: no cover

    assert harness["proxy"].stops == 1


@pytest.mark.asyncio
async def test_a_successful_startup_still_stops_everything_at_shutdown(harness):
    """The ordinary path is unchanged: start, serve, then the shutdown half."""
    async with harness["server"].lifespan(MagicMock()):
        assert harness["proxy"].starts == 1 and harness["proxy"].stops == 0

    assert harness["proxy"].stops == 1
    assert harness["closed"] == ["db"]
    assert harness["engine"].shutdown.await_count == 1


@pytest.mark.asyncio
async def test_a_failure_after_the_hosted_channels_start_stops_them(harness, monkeypatch):
    """Hosted channels that started are stopped when a later step fails."""
    import nerve.channels.hosted.runtime as hosted_module

    hosted = _Recorder()
    monkeypatch.setattr(hosted_module, "hosted_providers", lambda cfg: ["slack"])
    monkeypatch.setattr(hosted_module, "HostedChannelRuntime", lambda *a, **k: hosted)
    monkeypatch.setattr(harness["server"], "_hosted_channels", None)
    harness["engine"].resume_enrolled_sessions = MagicMock(
        side_effect=RuntimeError("late failure"),
    )

    with pytest.raises(RuntimeError, match="late failure"):
        async with harness["server"].lifespan(MagicMock()):
            pass  # pragma: no cover

    assert hosted.starts == 1 and hosted.stops == 1


@pytest.mark.asyncio
async def test_hosted_source_deliveries_are_handed_to_the_cron_service(harness, monkeypatch):
    """With GitHub hosted, the runtime's source consumer calls the cron
    service's delivery hook, which drains the records and runs the jobs
    gated on them."""
    import nerve.cron.service as cron_module
    from nerve.config import GitHubSyncConfig, HostedChannelsConfig
    from tests.fake_channel_gateway import FakeChannelGateway
    from tests.test_hosted_channel_config import HOSTED

    class Cron:
        def __init__(self, config, engine, db):
            self._jobs = []
            self.notification_service = None

        async def start(self):
            pass

        async def stop(self):
            pass

        async def on_source_delivered(self, source):
            pass

    gateway = FakeChannelGateway(jwks_file=harness["config"].workspace.parent / "gateway-jwks.json")
    harness["config"].sync.github = GitHubSyncConfig(mode="hosted")
    harness["config"].channels.hosted = HostedChannelsConfig.from_dict(
        {**HOSTED, "gateway_jwks_file": str(gateway.jwks_file)},
    )
    monkeypatch.setattr(cron_module, "CronService", Cron)
    monkeypatch.setattr(harness["server"], "_hosted_channels", None)

    async with harness["server"].lifespan(MagicMock()):
        runtime = harness["server"]._hosted_channels
        cron = harness["server"]._cron_service
        # GitHub is hosted as a whole: the repository watch comes with it.
        assert runtime is not None and set(runtime.sources) == {"github", "github_repos"}
        assert isinstance(cron, Cron)
        assert runtime.sources["github"]._listener == cron.on_source_delivered
        assert runtime.sources["github_repos"]._listener == cron.on_source_delivered


@pytest.mark.asyncio
async def test_hosted_channels_start_while_slack_is_switched_off(harness, monkeypatch):
    """A reload can switch hosted Slack on only if its runtime exists."""
    import nerve.channels.hosted.runtime as hosted_module

    hosted = _Recorder()
    monkeypatch.setattr(hosted_module, "HostedChannelRuntime", lambda *a, **k: hosted)
    monkeypatch.setattr(harness["server"], "_hosted_channels", None)
    harness["config"].slack.mode = "hosted"
    harness["config"].slack.enabled = False

    async with harness["server"].lifespan(MagicMock()):
        assert hosted.starts == 1
        assert harness["server"]._hosted_channels is hosted

    assert hosted.stops == 1


class _Reporter(_Recorder):
    """An activity reporter that records how the lifespan drives it."""

    def __init__(self, config, engine, db, cron_service, *, connection_count=None, fail=False):
        super().__init__(fail=fail)
        self.cron_service = cron_service
        self.connection_count = connection_count
        self.watched: list = []

    async def start(self):
        await super().start()
        return True

    def watch_workflow_runs(self, service):
        self.watched.append(service)


@pytest.fixture
def reporter(harness, monkeypatch):
    """The reporter the lifespan builds, replaced by a recorder."""
    import nerve.platform.activity as activity_module

    built: list[_Reporter] = []

    def build(*args, **kwargs):
        built.append(_Reporter(*args, **kwargs))
        return built[-1]

    monkeypatch.setattr(activity_module, "ActivityReporter", build)
    monkeypatch.setattr(harness["server"], "_activity_reporter", None)
    return built


@pytest.mark.asyncio
async def test_a_reporter_that_fails_to_start_never_stops_the_gateway(harness, monkeypatch):
    import nerve.platform.activity as activity_module

    def explode(*_args, **_kwargs):
        return _Reporter(None, None, None, None, fail=True)

    monkeypatch.setattr(activity_module, "ActivityReporter", explode)
    monkeypatch.setattr(harness["server"], "_activity_reporter", None)

    async with harness["server"].lifespan(MagicMock()):
        assert harness["server"]._activity_reporter is None

    assert harness["closed"] == ["db"]
    assert harness["engine"].shutdown.await_count == 1


@pytest.mark.asyncio
async def test_the_reporter_runs_for_the_life_of_the_gateway(harness, reporter, monkeypatch):
    """Started with the client connection count and the workflow-run service,
    stopped at shutdown."""
    import nerve.workflows as workflows_module

    runs = _Recorder()
    monkeypatch.setattr(workflows_module, "init_workflow_run_service", lambda *a, **k: runs)
    monkeypatch.setattr(harness["server"], "_workflow_run_service", None)

    async with harness["server"].lifespan(MagicMock()):
        (built,) = reporter
        assert harness["server"]._activity_reporter is built
        assert built.starts == 1 and built.stops == 0
        assert built.watched == [runs]
        harness["server"]._live_sockets["c1"] = (MagicMock(), MagicMock())
        try:
            assert built.connection_count() == 1
        finally:
            harness["server"]._live_sockets.pop("c1")

    assert built.stops == 1
    assert harness["server"]._activity_reporter is None


@pytest.mark.asyncio
async def test_a_failure_after_the_reporter_started_stops_it(harness, reporter):
    harness["engine"].resume_enrolled_sessions = MagicMock(
        side_effect=RuntimeError("late failure"),
    )

    with pytest.raises(RuntimeError, match="late failure"):
        async with harness["server"].lifespan(MagicMock()):
            pass  # pragma: no cover

    (built,) = reporter
    assert built.starts == 1 and built.stops == 1


@pytest.mark.asyncio
async def test_the_gateway_asks_for_the_onboarding_conversation(harness, monkeypatch):
    """The gateway opens a requested onboarding conversation at start-up;
    one-shot CLI engines run initialize() only and never do."""
    import nerve.onboarding as onboarding

    start_on_boot = AsyncMock(return_value=None)
    monkeypatch.setattr(onboarding, "start_on_boot", start_on_boot)
    async with harness["server"].lifespan(MagicMock()):
        await asyncio.sleep(0)
    start_on_boot.assert_awaited_once_with(harness["engine"], harness["config"].workspace)
