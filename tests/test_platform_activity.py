"""The activity reporter and the platform's local activity socket.

A fake socket — an HTTP/1.1 server on a Unix socket, in the test's event loop —
answers like the real one and records every request, so each test reads exactly
what the reporter sent.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import os
import shutil
import tempfile
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest
import pytest_asyncio

import nerve.config
from nerve.agent.sessions import SessionManager
from nerve.config import NerveConfig, PlatformConfig
from nerve.platform import activity
from nerve.platform.activity import (
    LEASE_BACKGROUND,
    LEASE_BACKUP,
    LEASE_CONFIG_SYNC,
    LEASE_MEMORIZE,
    ActivityReporter,
    activity_socket_present,
    keep_awake,
    next_task_name,
)

# --------------------------------------------------------------------------- #
#  The fake socket                                                            #
# --------------------------------------------------------------------------- #


@dataclass
class Recorded:
    method: str
    path: str
    body: Any
    at: float


class FakeActivitySocket:
    """Answers like the platform's local activity socket; records requests."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.requests: list[Recorded] = []
        # "METHOD /path" -> status to answer instead of 200.
        self.status: dict[str, int] = {}
        # Body keys answered with 400, as a socket that predates them does.
        self.unknown_fields: set[str] = set()
        # "METHOD /path" -> seconds to wait before answering.
        self.delays: dict[str, float] = {}
        self._server: asyncio.AbstractServer | None = None
        self._connections: set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        self._server = await asyncio.start_unix_server(self._serve, path=self.path)

    async def close(self) -> None:
        """Stop listening and drop open connections, as a restarting socket does."""
        if self._server is not None:
            self._server.close()
            for writer in list(self._connections):
                writer.close()
            await self._server.wait_closed()
            self._server = None
        if os.path.exists(self.path):
            os.unlink(self.path)

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        loop = asyncio.get_running_loop()
        self._connections.add(writer)
        try:
            while True:
                line = await reader.readline()
                if not line:
                    return
                method, target, _version = line.decode("latin-1").split(" ", 2)
                headers: dict[str, str] = {}
                while True:
                    header = await reader.readline()
                    if header in (b"\r\n", b"\n", b""):
                        break
                    name, _, value = header.decode("latin-1").partition(":")
                    headers[name.strip().lower()] = value.strip()
                length = int(headers.get("content-length") or 0)
                raw = await reader.readexactly(length) if length else b""
                body = json.loads(raw) if raw else None
                self.requests.append(Recorded(method, target, body, loop.time()))
                delay = self.delays.get(f"{method} {target}")
                if delay:
                    await asyncio.sleep(delay)
                status, payload = self._answer(method, target, body)
                data = json.dumps(payload).encode()
                writer.write(
                    f"HTTP/1.1 {status} Answer\r\nContent-Type: application/json\r\n"
                    f"Content-Length: {len(data)}\r\n\r\n".encode() + data
                )
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            self._connections.discard(writer)
            writer.close()

    def _answer(self, method: str, target: str, body: Any) -> tuple[int, dict]:
        status = self.status.get(f"{method} {target}")
        if status is not None and status >= 400:
            return status, {"error": "invalid_request"}
        if isinstance(body, dict) and self.unknown_fields & body.keys():
            return 400, {"error": "invalid_request"}
        if method == "POST" and target == "/v1/leases":
            return 200, {"name": body["name"], "expiresAt": "2030-01-01T00:00:00Z"}
        return 200, {"ok": True}

    # -- reading what was sent ------------------------------------------ #

    def reports(self) -> list[dict]:
        return [r.body for r in self.requests if (r.method, r.path) == ("POST", "/v1/activity")]

    def report_times(self) -> list[float]:
        return [r.at for r in self.requests if (r.method, r.path) == ("POST", "/v1/activity")]

    def lease_calls(self) -> list[tuple[str, str, Any]]:
        return [(r.method, r.path, r.body) for r in self.requests if r.path.startswith("/v1/leases")]

    async def wait_for(self, predicate: Callable[[], bool], timeout: float = 3.0) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not predicate():
            if loop.time() > deadline:
                raise AssertionError(f"timed out; requests so far: {self.requests}")
            await asyncio.sleep(0.01)


# --------------------------------------------------------------------------- #
#  Doubles for what the reporter reads                                        #
# --------------------------------------------------------------------------- #


class FakeAgents:
    """The agent registry's live view: whether any background agent lives."""

    def __init__(self) -> None:
        self.alive = False
        self.listeners: list[Callable[[], None]] = []

    def any_live(self) -> bool:
        return self.alive

    def add_live_listener(self, callback: Callable[[], None]) -> None:
        self.listeners.append(callback)

    def remove_live_listener(self, callback: Callable[[], None]) -> None:
        self.listeners.remove(callback)

    def set_alive(self, alive: bool) -> None:
        self.alive = alive
        for callback in self.listeners:
            callback()


class FakeEngine:
    def __init__(self, agents: FakeAgents | None = None) -> None:
        # The real running-turn bookkeeping, listeners included.
        self.sessions = SessionManager(db=None)
        if agents is not None:
            self.agents = agents


class FakeDb:
    def __init__(self) -> None:
        self.runs: list[dict] = []

    async def get_active_workflow_runs(self) -> list[dict]:
        return list(self.runs)


@dataclass(frozen=True)
class Tick:
    at: datetime
    name: str
    kind: str = "cron"


class FakeCron:
    def __init__(self, tick: Tick | None = None) -> None:
        self.tick = tick
        self.listeners: list[Callable[[], Any]] = []
        self.fail = False

    async def next_scheduled(self, now: datetime | None = None, horizon: timedelta | None = None):
        if self.fail:
            raise RuntimeError("forecast broke")
        return self.tick

    def add_schedule_listener(self, callback: Callable[[], Any]) -> None:
        self.listeners.append(callback)

    def changed(self) -> None:
        for callback in self.listeners:
            callback()


class FakeWorkflowRuns:
    def __init__(self) -> None:
        self.listeners: list[Callable] = []

    def add_completion_listener(self, callback: Callable) -> None:
        self.listeners.append(callback)

    def remove_completion_listener(self, callback: Callable) -> None:
        self.listeners.remove(callback)


@pytest.fixture
def socket_path():
    # A Unix socket path must stay short (about 104 bytes on macOS), which a
    # pytest tmp_path does not.
    directory = tempfile.mkdtemp(prefix="nact-", dir="/tmp" if os.path.isdir("/tmp") else None)
    yield os.path.join(directory, "activity.sock")
    shutil.rmtree(directory, ignore_errors=True)


@pytest_asyncio.fixture
async def fake_socket(socket_path):
    server = FakeActivitySocket(socket_path)
    await server.start()
    yield server
    await server.close()


def make_reporter(
    socket_path: str,
    *,
    engine: FakeEngine | None = None,
    db: FakeDb | None = None,
    cron: Any = None,
    connections: Callable[[], int] | None = None,
    interval: float = 300,
    lease_ttl: int = 120,
) -> ActivityReporter:
    config = NerveConfig()
    # Built directly: from_dict would hold the interval to its 5 s floor.
    config.platform = PlatformConfig(
        activity_socket=socket_path,
        activity_interval_seconds=interval,
        lease_ttl_seconds=lease_ttl,
    )
    return ActivityReporter(
        config, engine or FakeEngine(), db or FakeDb(), cron,
        connection_count=connections,
    )


@pytest_asyncio.fixture
async def reporters():
    """Stops every reporter a test started, even when the test fails."""
    started: list[ActivityReporter] = []
    yield started
    for reporter in started:
        await reporter.stop()


async def started(reporters: list, reporter: ActivityReporter) -> ActivityReporter:
    assert await reporter.start() is True
    reporters.append(reporter)
    return reporter


# --------------------------------------------------------------------------- #
#  Inert unless the socket exists                                             #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_absent_socket_leaves_the_reporter_inert(socket_path, caplog):
    engine = FakeEngine()
    reporter = make_reporter(socket_path, engine=engine, cron=FakeCron())

    with caplog.at_level(logging.INFO, logger="nerve.platform.activity"):
        assert await reporter.start() is False

    assert not reporter.active
    assert engine.sessions._running_listeners == ()  # nothing subscribed
    assert any("is not present" in r.getMessage() for r in caplog.records)
    reporter.notify()  # a no-op, not an error
    engine.sessions.mark_running("s1")
    with keep_awake(LEASE_BACKUP):
        pass
    await reporter.stop()


@pytest.mark.asyncio
async def test_an_empty_socket_setting_switches_reporting_off(caplog):
    reporter = make_reporter("")
    with caplog.at_level(logging.INFO, logger="nerve.platform.activity"):
        assert await reporter.start() is False
    assert any("reporting is off" in r.getMessage() for r in caplog.records)
    await reporter.stop()


def test_a_file_that_is_not_a_socket_is_not_present(socket_path):
    with open(socket_path, "w") as fh:
        fh.write("not a socket")
    assert activity_socket_present(socket_path) is False
    assert activity_socket_present("") is False
    assert activity_socket_present(socket_path + ".missing") is False


@pytest.mark.asyncio
async def test_a_listening_socket_is_present(fake_socket):
    assert activity_socket_present(fake_socket.path) is True


# --------------------------------------------------------------------------- #
#  The activity report                                                        #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_first_report_goes_out_at_start(fake_socket, reporters, caplog):
    with caplog.at_level(logging.INFO, logger="nerve.platform.activity"):
        await started(reporters, make_reporter(fake_socket.path))
    await fake_socket.wait_for(lambda: fake_socket.reports())

    assert fake_socket.reports()[0] == {
        "runningTurns": 0,
        "activeSessions": 0,
        "nextScheduledAt": None,
        "nextScheduledName": None,
    }
    assert any(
        "Reporting activity to the platform's local activity socket" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_running_turns_follow_session_turns(fake_socket, reporters):
    engine = FakeEngine()
    await started(reporters, make_reporter(fake_socket.path, engine=engine))
    await fake_socket.wait_for(lambda: fake_socket.reports())

    def last_turns() -> int:
        return fake_socket.reports()[-1]["runningTurns"]

    engine.sessions.mark_running("web:one")
    await fake_socket.wait_for(lambda: last_turns() == 1)
    engine.sessions.mark_running("cron:two")
    await fake_socket.wait_for(lambda: last_turns() == 2)
    engine.sessions.mark_not_running("web:one")
    engine.sessions.mark_not_running("cron:two")
    await fake_socket.wait_for(lambda: last_turns() == 0)


@pytest.mark.asyncio
async def test_workflow_runs_count_once_each(fake_socket, reporters):
    """A queued run counts; a running run counts once, through its session's
    turn while one runs and on its own while it drains background work."""
    engine, db = FakeEngine(), FakeDb()
    reporter = await started(reporters, make_reporter(fake_socket.path, engine=engine, db=db))
    await fake_socket.wait_for(lambda: fake_socket.reports())

    db.runs = [
        {"id": "wfr-queued", "status": "pending", "session_id": None},
        {"id": "wfr-turn", "status": "running", "session_id": "workflow:wfr-turn"},
        {"id": "wfr-drain", "status": "running", "session_id": "workflow:wfr-drain"},
    ]
    engine.sessions.mark_running("workflow:wfr-turn")
    engine.sessions.mark_running("web:main")
    # web:main + wfr-turn (its session) + wfr-queued + wfr-drain
    await fake_socket.wait_for(lambda: fake_socket.reports()[-1]["runningTurns"] == 4)

    # A run that finishes is reported through the completion listener.
    runs = FakeWorkflowRuns()
    reporter.watch_workflow_runs(runs)
    db.runs = db.runs[:2]
    await runs.listeners[0]({"id": "wfr-drain", "status": "done"})
    await fake_socket.wait_for(lambda: fake_socket.reports()[-1]["runningTurns"] == 3)

    await reporter.stop()
    assert runs.listeners == []  # unsubscribed at stop


@pytest.mark.asyncio
async def test_active_sessions_is_the_client_connection_count(fake_socket, reporters):
    connections: list[str] = []
    reporter = await started(
        reporters, make_reporter(fake_socket.path, connections=lambda: len(connections)),
    )
    await fake_socket.wait_for(lambda: fake_socket.reports())

    connections.extend(["a", "b"])
    reporter.notify()
    await fake_socket.wait_for(lambda: fake_socket.reports()[-1]["activeSessions"] == 2)
    connections.clear()
    reporter.notify()
    await fake_socket.wait_for(lambda: fake_socket.reports()[-1]["activeSessions"] == 0)


@pytest.mark.asyncio
async def test_next_scheduled_task_is_reported_in_utc(fake_socket, reporters):
    eastern = timezone(timedelta(hours=-4))
    cron = FakeCron(Tick(datetime(2026, 10, 21, 23, 0, 0, 750000, tzinfo=eastern), "memory-maintenance"))
    await started(reporters, make_reporter(fake_socket.path, cron=cron))
    await fake_socket.wait_for(lambda: fake_socket.reports())

    report = fake_socket.reports()[0]
    assert report["nextScheduledAt"] == "2026-10-22T03:00:00Z"
    assert report["nextScheduledName"] == "memory-maintenance"

    # The cron service says its schedule changed: a new report follows.
    cron.tick = Tick(datetime(2026, 10, 22, 5, 30, tzinfo=UTC), "wakeup:0f3a9c2e", "wakeup")
    cron.changed()
    await fake_socket.wait_for(
        lambda: fake_socket.reports()[-1]["nextScheduledName"] == "wakeup:0f3a9c2e",
    )
    assert fake_socket.reports()[-1]["nextScheduledAt"] == "2026-10-22T05:30:00Z"

    cron.tick = None
    cron.changed()
    await fake_socket.wait_for(lambda: fake_socket.reports()[-1]["nextScheduledAt"] is None)
    assert fake_socket.reports()[-1]["nextScheduledName"] is None


@pytest.mark.asyncio
async def test_a_failing_forecast_keeps_the_last_known_task(fake_socket, reporters):
    cron = FakeCron(Tick(datetime(2026, 10, 22, 3, 0, tzinfo=UTC), "digest"))
    reporter = await started(reporters, make_reporter(fake_socket.path, cron=cron))
    await fake_socket.wait_for(lambda: fake_socket.reports())

    cron.fail = True
    reporter.notify()
    await fake_socket.wait_for(lambda: len(fake_socket.reports()) >= 2)
    assert fake_socket.reports()[-1]["nextScheduledName"] == "digest"


def test_next_task_names_are_made_acceptable():
    assert next_task_name("source:gmail") == "source:gmail"
    assert next_task_name("daily digest/ü") == "daily-digest--"
    assert next_task_name("x" * 80) == "x" * 64
    assert next_task_name("") is None
    assert next_task_name(None) is None


@pytest.mark.asyncio
async def test_no_cron_service_means_nothing_scheduled(fake_socket, reporters):
    await started(reporters, make_reporter(fake_socket.path, cron=None))
    await fake_socket.wait_for(lambda: fake_socket.reports())
    assert fake_socket.reports()[0]["nextScheduledAt"] is None


# --------------------------------------------------------------------------- #
#  Promptness and coalescing                                                  #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_turn_start_is_reported_promptly(fake_socket, reporters):
    engine = FakeEngine()
    await started(reporters, make_reporter(fake_socket.path, engine=engine, interval=300))
    await fake_socket.wait_for(lambda: fake_socket.reports())
    await asyncio.sleep(0.3)  # past the coalescing window of the first report

    loop = asyncio.get_running_loop()
    began = loop.time()
    engine.sessions.mark_running("web:main")
    await fake_socket.wait_for(lambda: fake_socket.reports()[-1]["runningTurns"] == 1)
    assert fake_socket.report_times()[-1] - began < 0.2


@pytest.mark.asyncio
async def test_a_burst_of_changes_is_coalesced(fake_socket, reporters):
    engine = FakeEngine()
    await started(reporters, make_reporter(fake_socket.path, engine=engine, interval=300))
    await fake_socket.wait_for(lambda: fake_socket.reports())

    for i in range(40):
        engine.sessions.mark_running(f"s{i}")
        await asyncio.sleep(0.01)
    await fake_socket.wait_for(lambda: fake_socket.reports()[-1]["runningTurns"] == 40)
    await asyncio.sleep(0.3)

    times = fake_socket.report_times()
    gaps = [b - a for a, b in itertools.pairwise(times)]
    assert min(gaps) >= activity.REPORT_MIN_SPACING_SECONDS - 0.02
    assert len(times) <= 5  # ~0.4 s of changes, one report per 250 ms at most


@pytest.mark.asyncio
async def test_a_turn_inside_one_window_is_still_reported(fake_socket, reporters):
    engine = FakeEngine()
    await started(reporters, make_reporter(fake_socket.path, engine=engine, interval=300))
    await fake_socket.wait_for(lambda: fake_socket.reports())

    # Both inside the coalescing window that follows the first report.
    engine.sessions.mark_running("cron:quick")
    engine.sessions.mark_not_running("cron:quick")

    await fake_socket.wait_for(lambda: len(fake_socket.reports()) >= 3)
    turns = [report["runningTurns"] for report in fake_socket.reports()]
    assert turns[:3] == [0, 1, 0]


@pytest.mark.asyncio
async def test_reports_repeat_every_interval(fake_socket, reporters):
    await started(reporters, make_reporter(fake_socket.path, interval=0.3))
    await fake_socket.wait_for(lambda: len(fake_socket.reports()) >= 3, timeout=3)


# --------------------------------------------------------------------------- #
#  Leases                                                                     #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_lease_is_taken_renewed_and_released(fake_socket, reporters):
    await started(reporters, make_reporter(fake_socket.path, lease_ttl=1))
    await fake_socket.wait_for(lambda: fake_socket.reports())

    with keep_awake(LEASE_BACKUP):
        await fake_socket.wait_for(lambda: fake_socket.lease_calls())
        assert fake_socket.lease_calls()[0] == (
            "POST", "/v1/leases", {"name": "nerve-backup", "ttlSeconds": 1},
        )
        # Renewed every half lifetime while held.
        await fake_socket.wait_for(lambda: len(fake_socket.lease_calls()) >= 3, timeout=3)
        assert all(call[0] == "POST" for call in fake_socket.lease_calls())

    await fake_socket.wait_for(lambda: fake_socket.lease_calls()[-1][0] == "DELETE")
    assert fake_socket.lease_calls()[-1][:2] == ("DELETE", "/v1/leases/nerve-backup")


@pytest.mark.asyncio
async def test_holders_of_one_name_share_one_lease(fake_socket, reporters):
    await started(reporters, make_reporter(fake_socket.path))
    await fake_socket.wait_for(lambda: fake_socket.reports())

    outer = keep_awake(LEASE_MEMORIZE)
    inner = keep_awake(LEASE_MEMORIZE)
    outer.__enter__()
    await fake_socket.wait_for(lambda: fake_socket.lease_calls())
    inner.__enter__()
    outer.__exit__(None, None, None)
    await asyncio.sleep(0.4)
    assert [call[0] for call in fake_socket.lease_calls()] == ["POST"]
    inner.__exit__(None, None, None)
    await fake_socket.wait_for(lambda: fake_socket.lease_calls()[-1][0] == "DELETE")


@pytest.mark.asyncio
async def test_a_lease_can_be_held_from_a_worker_thread(fake_socket, reporters):
    await started(reporters, make_reporter(fake_socket.path))
    await fake_socket.wait_for(lambda: fake_socket.reports())
    release = threading.Event()

    def work() -> None:
        with keep_awake(LEASE_CONFIG_SYNC):
            release.wait(5)

    worker = asyncio.create_task(asyncio.to_thread(work))
    await fake_socket.wait_for(
        lambda: ("POST", "/v1/leases", {"name": "nerve-config-sync", "ttlSeconds": 120})
        in fake_socket.lease_calls(),
    )
    release.set()
    await worker
    await fake_socket.wait_for(lambda: fake_socket.lease_calls()[-1][0] == "DELETE")


@pytest.mark.asyncio
async def test_background_agents_hold_their_lease(fake_socket, reporters):
    agents = FakeAgents()
    reporter = await started(
        reporters, make_reporter(fake_socket.path, engine=FakeEngine(agents)),
    )
    await fake_socket.wait_for(lambda: fake_socket.reports())
    reports = len(fake_socket.reports())
    assert fake_socket.lease_calls() == []

    agents.set_alive(True)
    await fake_socket.wait_for(lambda: fake_socket.lease_calls())
    assert fake_socket.lease_calls()[0][2] == {"name": LEASE_BACKGROUND, "ttlSeconds": 120}

    agents.set_alive(False)
    await fake_socket.wait_for(lambda: fake_socket.lease_calls()[-1][0] == "DELETE")
    assert fake_socket.lease_calls()[-1][1] == "/v1/leases/nerve-background"
    # A lease change alone costs no activity report.
    assert len(fake_socket.reports()) == reports

    await reporter.stop()
    assert agents.listeners == []


@pytest.mark.asyncio
async def test_a_pause_renews_leases_at_the_next_wake(fake_socket, reporters, monkeypatch):
    """After the machine was paused the loop clock has hardly moved, but the
    platform's copy of a lease expired by the wall clock."""
    reporter = await started(reporters, make_reporter(fake_socket.path, lease_ttl=120))
    await fake_socket.wait_for(lambda: fake_socket.reports())
    with keep_awake(LEASE_BACKUP):
        await fake_socket.wait_for(lambda: len(fake_socket.lease_calls()) == 1)
        reporter.notify()
        await fake_socket.wait_for(lambda: len(fake_socket.reports()) >= 2)
        assert len(fake_socket.lease_calls()) == 1  # not due by either clock

        wall = activity._wall_time
        monkeypatch.setattr(activity, "_wall_time", lambda: wall() + 3 * 3600)
        reporter.notify()
        await fake_socket.wait_for(lambda: len(fake_socket.lease_calls()) == 2)
        assert fake_socket.lease_calls()[1] == (
            "POST", "/v1/leases", {"name": "nerve-backup", "ttlSeconds": 120},
        )


def test_keep_awake_refuses_a_name_the_socket_would_refuse():
    with pytest.raises(ValueError), keep_awake("has space"):
        pass
    with pytest.raises(ValueError), keep_awake("x" * 65):
        pass
    assert activity.held_leases() == set()


# --------------------------------------------------------------------------- #
#  Failures never raise                                                       #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_400_is_logged_once_and_never_raised(fake_socket, reporters, caplog):
    fake_socket.status["POST /v1/activity"] = 400
    fake_socket.status["POST /v1/leases"] = 400
    engine = FakeEngine()
    with caplog.at_level(logging.DEBUG, logger="nerve.platform.activity"):
        reporter = await started(reporters, make_reporter(fake_socket.path, engine=engine))
        await fake_socket.wait_for(lambda: fake_socket.reports())
        with keep_awake(LEASE_BACKUP):
            engine.sessions.mark_running("web:main")
            await fake_socket.wait_for(lambda: len(fake_socket.reports()) >= 2)
            await fake_socket.wait_for(lambda: fake_socket.lease_calls())
        engine.sessions.mark_not_running("web:main")
        await fake_socket.wait_for(lambda: len(fake_socket.reports()) >= 3)

    assert reporter.active
    info = [r for r in caplog.records if r.levelno == logging.INFO and "failed" in r.getMessage()]
    assert len([r for r in info if "activity report" in r.getMessage()]) == 1
    assert len([r for r in info if "keep-awake lease" in r.getMessage()]) == 1
    assert "HTTP 400 invalid_request" in info[0].getMessage()


@pytest.mark.asyncio
async def test_a_socket_that_refuses_the_task_name_gets_reports_without_it(fake_socket, reporters, caplog):
    fake_socket.unknown_fields = {"nextScheduledName"}
    cron = FakeCron(Tick(datetime(2026, 10, 22, 3, 0, tzinfo=UTC), "digest"))
    with caplog.at_level(logging.INFO, logger="nerve.platform.activity"):
        reporter = await started(reporters, make_reporter(fake_socket.path, cron=cron))
        await fake_socket.wait_for(lambda: len(fake_socket.reports()) >= 2)

        first, retry = fake_socket.reports()[:2]
        assert "nextScheduledName" in first
        assert retry == {
            "runningTurns": 0, "activeSessions": 0, "nextScheduledAt": "2026-10-22T03:00:00Z",
        }
        reporter.notify()
        await fake_socket.wait_for(lambda: len(fake_socket.reports()) >= 3)
        assert "nextScheduledName" not in fake_socket.reports()[2]
        # Logged once the retry's answer is read, which the third report follows.
        assert any("reporting without it" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_the_socket_going_away_and_coming_back(fake_socket, reporters, caplog):
    engine = FakeEngine()
    with caplog.at_level(logging.INFO, logger="nerve.platform.activity"):
        reporter = await started(reporters, make_reporter(fake_socket.path, engine=engine))
        await fake_socket.wait_for(lambda: fake_socket.reports())

        await fake_socket.close()
        engine.sessions.mark_running("web:main")
        await asyncio.sleep(0.4)
        assert reporter.active
        assert any("activity report failed" in r.getMessage() for r in caplog.records)

        before = len(fake_socket.reports())
        await fake_socket.start()
        engine.sessions.mark_not_running("web:main")
        await fake_socket.wait_for(lambda: len(fake_socket.reports()) > before)
        assert fake_socket.reports()[-1]["runningTurns"] == 0
        # Logged once the answer is read, just after the request was recorded.
        await fake_socket.wait_for(
            lambda: any("works again" in r.getMessage() for r in caplog.records),
        )


# --------------------------------------------------------------------------- #
#  Stop                                                                       #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_stop_releases_leases_and_reports_nothing_running(fake_socket):
    engine = FakeEngine()
    cron = FakeCron(Tick(datetime(2026, 10, 22, 3, 0, tzinfo=UTC), "digest"))
    reporter = make_reporter(fake_socket.path, engine=engine, cron=cron)
    assert await reporter.start()
    engine.sessions.mark_running("web:main")
    with keep_awake(LEASE_BACKUP):
        await fake_socket.wait_for(lambda: fake_socket.lease_calls())
        await reporter.stop()

    assert fake_socket.lease_calls()[-1][:2] == ("DELETE", "/v1/leases/nerve-backup")
    assert fake_socket.reports()[-1] == {
        "runningTurns": 0,
        "activeSessions": 0,
        "nextScheduledAt": "2026-10-22T03:00:00Z",
        "nextScheduledName": "digest",
    }
    assert not reporter.active
    assert engine.sessions._running_listeners == ()
    assert cron.listeners  # the fake has no remove; the listener is a no-op now
    count = len(fake_socket.requests)
    engine.sessions.mark_not_running("web:main")
    cron.changed()
    await asyncio.sleep(0.3)
    assert len(fake_socket.requests) == count
    await reporter.stop()  # idempotent


@pytest.mark.asyncio
async def test_stop_releases_a_lease_whose_request_is_still_in_flight(fake_socket):
    fake_socket.delays["POST /v1/leases"] = 0.5
    reporter = make_reporter(fake_socket.path)
    assert await reporter.start()
    with keep_awake(LEASE_CONFIG_SYNC):
        await fake_socket.wait_for(lambda: fake_socket.lease_calls())
        await reporter.stop()  # interrupts the upsert while it waits for its answer
    assert [call[:2] for call in fake_socket.lease_calls()] == [
        ("POST", "/v1/leases"), ("DELETE", "/v1/leases/nerve-config-sync"),
    ]


@pytest.mark.asyncio
async def test_a_failed_lease_request_is_retried_not_hammered(fake_socket, reporters, monkeypatch):
    monkeypatch.setattr(activity, "LEASE_RETRY_SECONDS", 0.3)
    fake_socket.status["POST /v1/leases"] = 500
    engine = FakeEngine()
    await started(reporters, make_reporter(fake_socket.path, engine=engine))
    await fake_socket.wait_for(lambda: fake_socket.reports())
    with keep_awake(LEASE_BACKUP):
        await fake_socket.wait_for(lambda: len(fake_socket.lease_calls()) >= 2, timeout=3)
        first, second = [r.at for r in fake_socket.requests if r.path == "/v1/leases"][:2]
        assert second - first >= 0.25
        fake_socket.status.clear()
        await fake_socket.wait_for(lambda: len(fake_socket.lease_calls()) >= 3, timeout=3)
    await fake_socket.wait_for(lambda: fake_socket.lease_calls()[-1][0] == "DELETE")


# --------------------------------------------------------------------------- #
#  Configuration                                                              #
# --------------------------------------------------------------------------- #

# Read at import, before any fixture runs: conftest points the default at
# nothing so that no test reaches a real socket.
_DEFAULT_SOCKET_AT_IMPORT = nerve.config.DEFAULT_PLATFORM_ACTIVITY_SOCKET
_DEFAULT_SOCKET = "/run/nerve/guestd.sock"


def test_the_default_socket_is_the_well_known_path():
    assert _DEFAULT_SOCKET_AT_IMPORT == _DEFAULT_SOCKET


def test_platform_defaults(monkeypatch):
    monkeypatch.setattr(nerve.config, "DEFAULT_PLATFORM_ACTIVITY_SOCKET", _DEFAULT_SOCKET)
    for platform in (
        PlatformConfig(),
        PlatformConfig.from_dict({}),
        NerveConfig().platform,
        NerveConfig.from_dict({}).platform,
        NerveConfig.from_dict({"platform": None}).platform,
    ):
        assert platform.activity_socket == _DEFAULT_SOCKET
        assert platform.activity_interval_seconds == 30
        assert platform.lease_ttl_seconds == 120


def test_an_empty_socket_setting_stays_empty(monkeypatch):
    monkeypatch.setattr(nerve.config, "DEFAULT_PLATFORM_ACTIVITY_SOCKET", _DEFAULT_SOCKET)
    for value in ("", "   ", None):
        assert PlatformConfig.from_dict({"activity_socket": value}).activity_socket == ""
    home = PlatformConfig.from_dict({"activity_socket": "~/activity.sock"})
    assert home.activity_socket == os.path.expanduser("~/activity.sock")


def test_platform_numbers_are_held_inside_their_bounds(caplog):
    with caplog.at_level(logging.WARNING, logger="nerve.config"):
        low = PlatformConfig.from_dict({"activity_interval_seconds": 1, "lease_ttl_seconds": 2})
        high = PlatformConfig.from_dict(
            {"activity_interval_seconds": 9000, "lease_ttl_seconds": 99999},
        )
    assert (low.activity_interval_seconds, low.lease_ttl_seconds) == (5, 10)
    assert (high.activity_interval_seconds, high.lease_ttl_seconds) == (300, 3600)
    assert any("platform.lease_ttl_seconds is 99999" in r.getMessage() for r in caplog.records)

    from_env = PlatformConfig.from_dict(
        {"activity_interval_seconds": "60", "lease_ttl_seconds": "soon"},
    )
    assert (from_env.activity_interval_seconds, from_env.lease_ttl_seconds) == (60, 120)


def test_changing_the_platform_section_needs_a_restart():
    from nerve.config_reload import restart_required

    before = NerveConfig()
    after = NerveConfig()
    after.platform = PlatformConfig(activity_socket="/tmp/elsewhere.sock")
    assert restart_required(before, after) == ["platform: changed"]


# --------------------------------------------------------------------------- #
#  Turn bookkeeping                                                           #
# --------------------------------------------------------------------------- #


def test_session_manager_counts_turns_and_tells_listeners():
    sessions = SessionManager(db=None)
    seen: list[tuple[str, bool]] = []

    def listener(session_id: str, running: bool) -> None:
        seen.append((session_id, running))

    sessions.add_running_listener(listener)
    sessions.add_running_listener(listener)  # once is enough
    sessions.mark_running("web:a")
    sessions.mark_running("web:a")  # already running: no news
    sessions.mark_running("cron:b")
    assert sessions.running_count() == 2
    sessions.mark_not_running("web:a")
    sessions.mark_not_running("web:a")
    sessions.mark_not_running("never-ran")
    assert seen == [("web:a", True), ("cron:b", True), ("web:a", False)]
    assert sessions.running_count() == 1

    sessions.remove_running_listener(listener)
    sessions.mark_not_running("cron:b")
    assert len(seen) == 3 and sessions.running_count() == 0


def test_a_failing_running_listener_never_reaches_the_turn(caplog):
    sessions = SessionManager(db=None)
    heard: list[str] = []

    def broken(_session_id: str, _running: bool) -> None:
        raise RuntimeError("listener broke")

    sessions.add_running_listener(broken)
    sessions.add_running_listener(lambda session_id, _running: heard.append(session_id))
    with caplog.at_level(logging.ERROR, logger="nerve.agent.sessions"):
        sessions.mark_running("web:a")
    assert sessions.is_running("web:a") and heard == ["web:a"]
    assert any("running listener failed" in r.getMessage() for r in caplog.records)


# --------------------------------------------------------------------------- #
#  nerve doctor                                                               #
# --------------------------------------------------------------------------- #


@pytest.fixture
def listening(socket_path):
    """A bound Unix socket at ``socket_path`` (nothing needs to answer)."""
    import socket

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(socket_path)
    server.listen(1)
    yield socket_path
    server.close()


def _config_with_socket(path: str) -> NerveConfig:
    config = NerveConfig()
    config.platform = PlatformConfig(activity_socket=path)
    return config


def test_doctor_line_says_whether_the_socket_is_present(socket_path):
    from nerve.cli import _platform_activity_line

    assert _platform_activity_line(_config_with_socket("")) == (
        "[--] Platform activity socket: not present "
        "(platform.activity_socket is empty, reporting is off)"
    )
    assert _platform_activity_line(_config_with_socket(socket_path)) == (
        f"[--] Platform activity socket: not present ({socket_path})"
    )


def test_doctor_report_shows_a_present_socket(tmp_path, listening):
    from nerve.cli import doctor_report
    from nerve.config import load_config

    config_dir, workspace = tmp_path / "cfg", tmp_path / "ws"
    config_dir.mkdir()
    (workspace / "config").mkdir(parents=True)
    (config_dir / "config.yaml").write_text(
        f"workspace: {workspace}\nplatform:\n  activity_socket: {listening}\n",
        encoding="utf-8",
    )
    (config_dir / "config.local.yaml").write_text("{}\n", encoding="utf-8")
    report = doctor_report(load_config(config_dir))
    assert f"[OK] Platform activity socket: present ({listening})" in report
