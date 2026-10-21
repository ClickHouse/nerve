"""Activity reports to the platform's local activity socket.

A platform that parks idle machines, and wakes them for scheduled work, has to
know from inside the machine whether Nerve is busy and when it next has work.
Such a platform can offer a local activity socket: HTTP/1.1 on a Unix socket,
named by ``platform.activity_socket``. When that socket exists at startup,
:class:`ActivityReporter` keeps it current. Without it, the reporter does
nothing.

What is sent:

* ``POST /v1/activity`` with ``runningTurns`` (sessions running a turn, plus
  workflow runs that are queued or still running without one; review-loop
  legs are workflow runs), ``activeSessions`` (open web client connections),
  and ``nextScheduledAt``/``nextScheduledName`` (the next scheduled task that
  will actually run, as the cron service forecasts it). Sent when something
  changes (a turn starts or ends, a client connects or leaves, the schedule
  changes, a workflow run finishes), at most once every 250 ms, and every
  ``platform.activity_interval_seconds``.
* Keep-awake leases for background work that is not a turn
  (``POST /v1/leases``, renewed every half ``platform.lease_ttl_seconds``,
  ``DELETE /v1/leases/<name>`` when the work ends). Work takes one with
  :func:`keep_awake`; :data:`LEASE_BACKGROUND` is held while any session has
  a live background agent.

Every request is best effort: a failure is logged (at INFO the first time,
then at DEBUG at most every few minutes) and never reaches the work it
reports on.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import os
import re
import stat
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx

from nerve.utils.aio import stop_background_task

if TYPE_CHECKING:
    from nerve.config import NerveConfig

logger = logging.getLogger(__name__)

ACTIVITY_ROUTE = "/v1/activity"
LEASES_ROUTE = "/v1/leases"

# Keep-awake lease names. The socket takes ``[A-Za-z0-9._-]{1,64}``.
LEASE_MEMORIZE = "nerve-memorize"
LEASE_CONFIG_SYNC = "nerve-config-sync"
LEASE_BACKUP = "nerve-backup"
LEASE_BACKGROUND = "nerve-background"
LEASE_NAME_PATTERN = re.compile(r"[A-Za-z0-9._-]{1,64}")

# What the socket takes as the next task's name.
NEXT_TASK_NAME_MAX_BYTES = 64
_NEXT_TASK_NAME_REFUSED = re.compile(r"[^A-Za-z0-9._:-]")

# Activity reports are coalesced: a burst of changes costs one post.
REPORT_MIN_SPACING_SECONDS = 0.25
# The socket is local; a request that takes longer is treated as failed.
REQUEST_TIMEOUT_SECONDS = 2.0
# Bound on asking the cron service for the next task.
SCHEDULE_TIMEOUT_SECONDS = 5.0
# Bound on releasing leases and the last report at shutdown.
STOP_TIMEOUT_SECONDS = 3.0
# A lease request that failed is tried again after this long (or half the
# lease lifetime, if that is shorter).
LEASE_RETRY_SECONDS = 5.0
# A failure that keeps happening is logged at DEBUG at most this often.
REPEAT_FAILURE_LOG_SECONDS = 300.0

# Only a short error code from the socket's answer reaches the log.
_ERROR_CODE = re.compile(r"[a-z_]{1,40}")
# The wall clock, for lease lifetimes (see _Lease).
_wall_time = time.time
# The key a socket that predates it refuses (see _post_activity).
_TASK_NAME_KEY = "nextScheduledName"


def activity_socket_present(path: str) -> bool:
    """True when *path* names an existing Unix socket."""
    if not path:
        return False
    try:
        return stat.S_ISSOCK(os.stat(path).st_mode)
    except OSError:
        return False


# --------------------------------------------------------------------------- #
#  Keep-awake leases                                                          #
# --------------------------------------------------------------------------- #


class _KeepAwake:
    """How many pieces of work hold each lease name, process-wide.

    Thread-safe: work that runs in a worker thread may hold a lease too.
    Listeners hear about a name being taken or dropped, not about a second
    holder of a name already held.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[str, int] = {}
        self._listeners: tuple[Callable[[], None], ...] = ()

    def held(self) -> set[str]:
        with self._lock:
            return set(self._counts)

    def add_listener(self, listener: Callable[[], None]) -> None:
        with self._lock:
            if listener not in self._listeners:
                self._listeners = (*self._listeners, listener)

    def remove_listener(self, listener: Callable[[], None]) -> None:
        with self._lock:
            self._listeners = tuple(item for item in self._listeners if item != listener)

    def _change(self, name: str, delta: int) -> None:
        with self._lock:
            before = self._counts.get(name, 0)
            after = before + delta
            if after > 0:
                self._counts[name] = after
            else:
                self._counts.pop(name, None)
            listeners = self._listeners if (before > 0) != (after > 0) else ()
        for listener in listeners:
            try:
                listener()
            except Exception:  # a listener never breaks the work
                logger.exception("keep-awake listener failed for %s", name)

    @contextlib.contextmanager
    def hold(self, name: str) -> Iterator[None]:
        if not LEASE_NAME_PATTERN.fullmatch(name):
            raise ValueError(f"invalid keep-awake lease name: {name!r}")
        self._change(name, 1)
        try:
            yield
        finally:
            self._change(name, -1)


_keep_awake = _KeepAwake()


def keep_awake(name: str) -> contextlib.AbstractContextManager[None]:
    """Hold the keep-awake lease *name* while the ``with`` block runs.

    For background work that is not a turn and must not be cut short by the
    platform parking the machine: memorization, a workspace sync, a backup.
    Concurrent and nested holders of one name share one lease. Works from any
    thread. Without an activity socket it only counts; it never blocks, and it
    raises only for a name the socket would refuse (a programming error).
    """
    return _keep_awake.hold(name)


def held_leases() -> set[str]:
    """The lease names :func:`keep_awake` holds right now."""
    return _keep_awake.held()


# --------------------------------------------------------------------------- #
#  Reporter                                                                   #
# --------------------------------------------------------------------------- #


class _FailureLog:
    """Logs a kind of request that fails: at INFO the first time, then at
    DEBUG at most every :data:`REPEAT_FAILURE_LOG_SECONDS`, and at INFO
    again once it works."""

    def __init__(self, what: str) -> None:
        self._what = what
        self._failing = False
        self._last_logged = -math.inf

    def failed(self, detail: str) -> None:
        now = time.monotonic()
        if not self._failing:
            self._failing = True
            self._last_logged = now
            logger.info(
                "Platform activity socket: %s failed (%s); repeats are logged "
                "at debug level", self._what, detail,
            )
        elif now - self._last_logged >= REPEAT_FAILURE_LOG_SECONDS:
            self._last_logged = now
            logger.debug("Platform activity socket: %s failed (%s)", self._what, detail)

    def succeeded(self) -> None:
        if self._failing:
            self._failing = False
            logger.info("Platform activity socket: %s works again", self._what)


def _rfc3339(at: datetime) -> str:
    """UTC, second precision. A naive time is taken as UTC."""
    if at.tzinfo is None:
        at = at.replace(tzinfo=UTC)
    return at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def next_task_name(name: object) -> str | None:
    """*name* in the form the socket takes, or ``None`` when nothing is left.

    Characters outside ``[A-Za-z0-9._:-]`` become ``-`` (so the result is
    ASCII and its length is its size in bytes), cut at 64.
    """
    if not isinstance(name, str):
        return None
    cleaned = _NEXT_TASK_NAME_REFUSED.sub("-", name)[:NEXT_TASK_NAME_MAX_BYTES]
    return cleaned or None


@dataclass
class _Lease:
    """A lease the socket may hold. Recorded when first asked for, so that a
    stop that interrupts the request still releases it.

    The next request (renewal or retry) is due at ``due`` by the loop clock or
    at ``due_wall`` by the wall clock, whichever comes first: after the
    machine was paused, the loop clock has hardly moved but the platform's
    copy of the lease expired by the wall clock.
    """

    due: float
    due_wall: float
    renewed_at: float = -math.inf  # wall time of the last successful upsert
    releasing: bool = False  # no longer wanted; a release failed and is retried


def _error_detail(response: httpx.Response) -> str:
    """``HTTP <status>`` and the socket's error code, if it sent a short one."""
    detail = f"HTTP {response.status_code}"
    try:
        code = response.json().get("error")
    except Exception:  # noqa: BLE001 — any body that is not the error shape
        code = None
    if isinstance(code, str) and _ERROR_CODE.fullmatch(code):
        detail += f" {code}"
    return detail


class ActivityReporter:
    """Keeps the platform's local activity socket current.

    Inert unless ``config.platform.activity_socket`` names an existing socket
    when :meth:`start` runs. Reads ``engine.sessions`` (running turns),
    ``engine.agents`` (live background agents), ``db`` (active workflow
    runs) and ``cron_service`` (the next scheduled task; ``None`` when the
    scheduler did not start, and nothing is then reported as scheduled).
    ``connection_count`` returns the number of open web client connections.
    """

    def __init__(
        self,
        config: NerveConfig,
        engine: Any,
        db: Any,
        cron_service: Any,
        *,
        connection_count: Callable[[], int] | None = None,
    ) -> None:
        platform = config.platform
        self.socket_path = platform.activity_socket
        self.interval = float(platform.activity_interval_seconds)
        self.lease_ttl = int(platform.lease_ttl_seconds)
        self._engine = engine
        self._db = db
        self._cron = cron_service
        self._connection_count = connection_count

        self._client: httpx.AsyncClient | None = None
        self._task: asyncio.Task | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._wake: asyncio.Event | None = None
        self._stopped = False
        self._undo: list[Callable[[], None]] = []

        # Something an activity report shows changed since the last one.
        self._report_wanted = False
        # A turn started since the last report. A turn that starts and ends
        # inside one coalescing window is still reported as running once.
        self._turn_started = False
        # The cron service's last answer, kept for when it fails.
        self._next_tick: tuple[datetime, str] | None = None
        # Leases the socket may hold, by name.
        self._leases: dict[str, _Lease] = {}
        self._last_post = -math.inf
        # Whether the socket takes nextScheduledName (see _post_activity).
        self._send_task_name = True

        self._activity_failures = _FailureLog("activity report")
        self._lease_failures = _FailureLog("keep-awake lease")
        self._schedule_failures = _FailureLog("next scheduled task")
        self._loop_failures = _FailureLog("reporting")

    @property
    def active(self) -> bool:
        """True while reports are being sent."""
        return self._task is not None

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                          #
    # ------------------------------------------------------------------ #

    async def start(self) -> bool:
        """Start reporting if the socket exists. Returns whether it did.

        Logs one INFO line either way.
        """
        if self._stopped or self._task is not None:
            return self._task is not None
        path = self.socket_path
        if not path:
            logger.info(
                "Platform activity reporting is off (platform.activity_socket is empty)",
            )
            return False
        if not activity_socket_present(path):
            logger.info(
                "Platform activity socket %s is not present; activity reporting is off",
                path,
            )
            return False

        self._loop = asyncio.get_running_loop()
        self._wake = asyncio.Event()
        self._client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=path),
            base_url="http://localhost",
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        self._subscribe()
        self._report_wanted = True  # the first report goes out at once
        self._task = asyncio.create_task(self._run(), name="platform-activity-reporter")
        logger.info(
            "Reporting activity to the platform's local activity socket at %s "
            "(every %ds, leases for %ds)", path, int(self.interval), self.lease_ttl,
        )
        return True

    async def stop(self) -> None:
        """Stop reporting: release held leases and send a last report.

        The last report says nothing runs and nobody is connected, which is
        true once the gateway is gone, and keeps the next scheduled task.
        Bounded by :data:`STOP_TIMEOUT_SECONDS`; never raises.
        """
        if self._stopped:
            return
        self._stopped = True
        for undo in reversed(self._undo):
            try:
                undo()
            except Exception:  # shutdown goes on regardless
                logger.debug("activity reporter: unsubscribe failed", exc_info=True)
        self._undo.clear()
        task, self._task = self._task, None
        if task is not None:
            await stop_background_task(task, None, "Activity reporter", timeout=1.0)
        if self._client is None:
            return
        try:
            await asyncio.wait_for(self._last_words(), STOP_TIMEOUT_SECONDS)
        except Exception:  # includes the timeout
            logger.debug("activity reporter: last report failed", exc_info=True)
        finally:
            client, self._client = self._client, None
            with contextlib.suppress(Exception):
                await client.aclose()

    def notify(self) -> None:
        """Ask for an activity report soon (coalesced).

        Safe from any thread; never raises; does nothing while inactive.
        """
        self._wake_up(report=True)

    def watch_workflow_runs(self, service: Any) -> None:
        """Report when a workflow run reaches a terminal status.

        A run's start needs no hook: its turn starts a session turn, which is
        reported. Does nothing while the reporter is inactive.
        """
        if not self.active or service is None:
            return
        add = getattr(service, "add_completion_listener", None)
        if add is None:
            return

        async def _finished(_run: Any) -> None:
            self.notify()

        add(_finished)
        remove = getattr(service, "remove_completion_listener", None)
        if remove is not None:
            self._undo.append(lambda: remove(_finished))

    # ------------------------------------------------------------------ #
    #  Triggers                                                           #
    # ------------------------------------------------------------------ #

    def _wake_up(self, *, report: bool) -> None:
        loop, wake = self._loop, self._wake
        if loop is None or wake is None or self._stopped:
            return

        def _set() -> None:
            if report:
                self._report_wanted = True
            wake.set()

        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        try:
            if running is loop:
                _set()
            else:
                loop.call_soon_threadsafe(_set)
        except RuntimeError:  # the loop is closed: nothing left to report to
            pass

    def _leases_changed(self) -> None:
        """A lease may be wanted or unwanted now; no activity report needed."""
        self._wake_up(report=False)

    def _subscribe(self) -> None:
        sessions = getattr(self._engine, "sessions", None)
        if hasattr(sessions, "add_running_listener"):
            sessions.add_running_listener(self._on_running_changed)
            self._undo.append(lambda: sessions.remove_running_listener(self._on_running_changed))

        agents = getattr(self._engine, "agents", None)
        if hasattr(agents, "add_live_listener"):
            agents.add_live_listener(self._leases_changed)
            self._undo.append(lambda: agents.remove_live_listener(self._leases_changed))

        _keep_awake.add_listener(self._leases_changed)
        self._undo.append(lambda: _keep_awake.remove_listener(self._leases_changed))

        if self._cron is not None and _add_schedule_listener(self._cron, self.notify):
            remove = getattr(self._cron, "remove_schedule_listener", None)
            if remove is not None:
                self._undo.append(lambda: remove(self.notify))

    def _on_running_changed(self, _session_id: str, running: bool) -> None:
        if running:
            self._turn_started = True
        self.notify()

    # ------------------------------------------------------------------ #
    #  The loop                                                           #
    # ------------------------------------------------------------------ #

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        wake = self._wake
        assert wake is not None
        next_report = loop.time()
        while True:
            await self._wait(wake, min(next_report, self._next_lease_request()) - loop.time())
            wake.clear()
            try:
                await self._sync_leases()
                if self._report_wanted or loop.time() >= next_report:
                    spacing = self._last_post + REPORT_MIN_SPACING_SECONDS - loop.time()
                    if spacing > 0:
                        # Changes that arrive meanwhile join this report.
                        await asyncio.sleep(spacing)
                    self._report_wanted = False
                    await self._post_activity(await self._snapshot())
                    next_report = loop.time() + self.interval
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — the loop outlives any one report
                self._loop_failures.failed(f"{type(e).__name__}: {e}")
                next_report = loop.time() + self.interval

    @staticmethod
    async def _wait(wake: asyncio.Event, timeout: float) -> None:
        if wake.is_set() or timeout <= 0:
            return
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(wake.wait(), timeout)

    # ------------------------------------------------------------------ #
    #  Activity                                                           #
    # ------------------------------------------------------------------ #

    async def _snapshot(self) -> dict[str, Any]:
        """The activity report to send now."""
        turn_started, self._turn_started = self._turn_started, False
        running = await self._running_turns()
        if turn_started and running == 0:
            # The turn is already over: report it once, then the truth.
            running = 1
            self.notify()
        tick = await self._next_scheduled()
        return self._body(running, self._active_sessions(), tick)

    @staticmethod
    def _body(
        running: int, sessions: int, tick: tuple[datetime, str] | None,
    ) -> dict[str, Any]:
        return {
            "runningTurns": running,
            "activeSessions": sessions,
            "nextScheduledAt": _rfc3339(tick[0]) if tick else None,
            _TASK_NAME_KEY: next_task_name(tick[1]) if tick else None,
        }

    async def _post_activity(self, body: dict[str, Any]) -> bool:
        if not self._send_task_name:
            body = {key: value for key, value in body.items() if key != _TASK_NAME_KEY}
        self._last_post = asyncio.get_running_loop().time()
        response = await self._request("POST", ACTIVITY_ROUTE, body, self._activity_failures)
        if response is None:
            return False
        if response.status_code == 400 and _TASK_NAME_KEY in body:
            # A socket that predates the task's name refuses the unknown field,
            # and with it the whole report. Send the report without the name,
            # and keep leaving the name out if that is what it takes.
            retry = {key: value for key, value in body.items() if key != _TASK_NAME_KEY}
            second = await self._request("POST", ACTIVITY_ROUTE, retry, None)
            if second is not None and second.is_success:
                self._send_task_name = False
                self._activity_failures.succeeded()
                logger.info(
                    "Platform activity socket refused the next task's name "
                    "(%s); reporting without it", _error_detail(response),
                )
                return True
        if response.is_success:
            self._activity_failures.succeeded()
            return True
        self._activity_failures.failed(_error_detail(response))
        return False

    async def _running_turns(self) -> int:
        sessions = getattr(self._engine, "sessions", None)
        count = sessions.running_count() if sessions is not None else 0
        try:
            runs = await self._db.get_active_workflow_runs()
        except Exception as e:  # noqa: BLE001 — a report goes out without them
            logger.debug("activity reporter: workflow runs unavailable: %s", e)
            runs = []
        for run in runs:
            session_id = run.get("session_id")
            if session_id and sessions is not None and sessions.is_running(session_id):
                continue  # its turn is counted already
            count += 1  # queued, or still draining background work
        return count

    def _active_sessions(self) -> int:
        counter = self._connection_count
        if counter is None:
            return 0
        try:
            return max(0, int(counter()))
        except Exception as e:  # noqa: BLE001
            logger.debug("activity reporter: connection count unavailable: %s", e)
            return 0

    async def _next_scheduled(self) -> tuple[datetime, str] | None:
        if self._cron is None:
            return None
        try:
            tick = await asyncio.wait_for(
                _next_scheduled(self._cron), SCHEDULE_TIMEOUT_SECONDS,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — keep the last known answer
            self._schedule_failures.failed(f"{type(e).__name__}: {e}")
            return self._next_tick
        self._schedule_failures.succeeded()
        self._next_tick = None if tick is None else (tick.at, tick.name)
        return self._next_tick

    # ------------------------------------------------------------------ #
    #  Leases                                                             #
    # ------------------------------------------------------------------ #

    def _wanted_leases(self) -> set[str]:
        wanted = held_leases()
        agents = getattr(self._engine, "agents", None)
        if hasattr(agents, "any_live"):
            try:
                if agents.any_live():
                    wanted.add(LEASE_BACKGROUND)
            except Exception as e:  # noqa: BLE001
                logger.debug("activity reporter: background agent check failed: %s", e)
        return wanted

    def _next_lease_request(self) -> float:
        return min((lease.due for lease in self._leases.values()), default=math.inf)

    @staticmethod
    def _schedule(lease: _Lease, delay: float) -> None:
        lease.due = asyncio.get_running_loop().time() + delay
        lease.due_wall = _wall_time() + delay

    @staticmethod
    def _request_due(lease: _Lease) -> bool:
        return asyncio.get_running_loop().time() >= lease.due or _wall_time() >= lease.due_wall

    def _retry_delay(self) -> float:
        return min(LEASE_RETRY_SECONDS, self.lease_ttl / 2)

    async def _sync_leases(self) -> None:
        wanted = self._wanted_leases()
        for name in sorted(wanted):
            lease = self._leases.get(name)
            if lease is None:
                lease = self._leases[name] = _Lease(
                    due=asyncio.get_running_loop().time(), due_wall=_wall_time(),
                )
            elif lease.releasing:
                lease.releasing = False  # wanted again before it was released
            elif not self._request_due(lease):
                continue
            response = await self._request(
                "POST", LEASES_ROUTE, {"name": name, "ttlSeconds": self.lease_ttl},
                self._lease_failures,
            )
            if response is not None and response.is_success:
                lease.renewed_at = _wall_time()
                self._schedule(lease, self.lease_ttl / 2)
                self._lease_failures.succeeded()
                continue
            if response is not None:
                self._lease_failures.failed(f"{name}: {_error_detail(response)}")
            self._schedule(lease, self._retry_delay())
        for name in sorted(set(self._leases) - wanted):
            lease = self._leases[name]
            if not lease.releasing or self._request_due(lease):
                await self._release(name)

    async def _release(self, name: str) -> None:
        lease = self._leases.get(name)
        if lease is None:
            return
        response = await self._request(
            "DELETE", f"{LEASES_ROUTE}/{name}", None, self._lease_failures,
        )
        if response is not None and (response.is_success or response.status_code == 404):
            self._leases.pop(name, None)
            self._lease_failures.succeeded()
            return
        if response is not None:
            self._lease_failures.failed(f"releasing {name}: {_error_detail(response)}")
        if _wall_time() - lease.renewed_at >= self.lease_ttl:
            # The platform's copy has expired by now, or was never confirmed
            # (one that was stored all the same expires on its own).
            self._leases.pop(name, None)
        else:
            lease.releasing = True
            self._schedule(lease, self._retry_delay())

    # ------------------------------------------------------------------ #
    #  Transport                                                          #
    # ------------------------------------------------------------------ #

    async def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None,
        failures: _FailureLog | None,
    ) -> httpx.Response | None:
        """Send one request. ``None`` (and a logged failure) when no answer came."""
        client = self._client
        if client is None:
            return None
        try:
            return await client.request(method, path, json=body)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — best effort, by design
            if failures is not None:
                failures.failed(f"{type(e).__name__}: {e}")
            return None

    async def _last_words(self) -> None:
        for name in sorted(self._leases):
            await self._release(name)
        spacing = self._last_post + REPORT_MIN_SPACING_SECONDS - asyncio.get_running_loop().time()
        if spacing > 0:
            await asyncio.sleep(spacing)
        await self._post_activity(self._body(0, 0, self._next_tick))


# --------------------------------------------------------------------------- #
#  Cron service seam                                                          #
# --------------------------------------------------------------------------- #


async def _next_scheduled(cron_service: Any) -> Any:
    """The cron service's next scheduled tick (``at``, ``name``), or ``None``."""
    method = getattr(cron_service, "next_scheduled", None)
    if method is None:
        return None
    return await method()


def _add_schedule_listener(cron_service: Any, listener: Callable[[], None]) -> bool:
    """Ask the cron service to call *listener* when its schedule changes."""
    add = getattr(cron_service, "add_schedule_listener", None)
    if add is None:
        return False
    add(listener)
    return True
