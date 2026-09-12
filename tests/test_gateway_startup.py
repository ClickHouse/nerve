"""A startup that fails leaves nothing running behind it.

The lifespan's shutdown half only runs after the ``yield``, so anything that
raised before it used to leak whatever had already started. The proxy is the
one that hurts: :meth:`ProxyService.start` detaches its subprocess into its own
process group, so an orphan keeps the port and the operator has to hunt it down
before the next start (F28). The database now has a failure mode that reaches
exactly this path — ``Database.connect`` refuses state other users can write to.

Two guarantees are pinned here:

* the database is opened and the identity bootstrapped **before** anything is
  started, so the likeliest failure costs nothing to clean up;
* whatever *is* up when a later step fails gets stopped, in reverse order.
"""

from __future__ import annotations

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
        return MagicMock(cleanup_expired_messages=AsyncMock(return_value=0))

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
    """The database comes first, so the failure the state-file policy produces
    costs nothing: there is nothing started to leave behind."""
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
    """And when something later fails — here the engine — everything already
    started is stopped, in reverse: no orphaned proxy holding its port."""
    harness["engine"].initialize.side_effect = RuntimeError("engine exploded")

    with pytest.raises(RuntimeError, match="engine exploded"):
        async with harness["server"].lifespan(MagicMock()):
            pass  # pragma: no cover

    assert harness["proxy"].starts == 1 and harness["proxy"].stops == 1
    assert harness["closed"] == ["db"]  # the database was closed too
    assert harness["engine"].shutdown.await_count == 0  # it never came up


@pytest.mark.asyncio
async def test_a_failing_stop_does_not_hide_the_startup_failure(harness, monkeypatch):
    """The unwind is best-effort per resource: a stop that raises is logged and
    the original failure is what propagates."""
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
