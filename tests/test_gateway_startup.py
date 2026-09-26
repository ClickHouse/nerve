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
