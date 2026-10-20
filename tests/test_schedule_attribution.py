"""Schedules: who schedules a wakeup or starts a cron run, and for whom.

A wakeup row records who scheduled it and its cause: a person who defers a
message with "Run later", or the system actor and the session's cause when
the agent calls ``ScheduleWakeup`` / ``schedule_wakeup``. A wakeup fires in
its session, so its turn runs for the session's cause. A cron run log records
who started the run: the system actor for a scheduled run, the person who
used "Run now". A run that a person started creates its session with that
person as the cause.

The tests drive the real routes, cron service, engine, tool registry and
database. The model runtime is the tool-calling stand-in of
``tests/test_cause_attribution.py``, or a stand-in for the Claude CLI process
where the CLI's own ScheduleWakeup hook is under test.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

from nerve.agent.backends import claude as claude_backend
from nerve.agent.backends.claude import translate_message
from nerve.cron.jobs import CronJob
from nerve.cron.service import CronService
from nerve.db.actor_columns import ACTOR_COLUMNS
from nerve.identity import Attribution
from nerve.inference_principal import CUSTOM_HEADERS_ENV, INFERENCE_PRINCIPAL_HEADER
from tests.test_cause_attribution import (  # noqa: F401 (fixtures)
    ALICE_PRINCIPAL,
    BOB_PRINCIPAL,
    _as,
    _chat,
    _client,
    _Install,
    _new_session,
    external_install,
    install,
)

_SCHEDULE_COLUMNS = (
    ("session_wakeups", "created_by_actor_id"),
    ("session_wakeups", "caused_by_actor_id"),
    ("cron_logs", "actor_id"),
    ("cron_logs", "caused_by_actor_id"),
)


def _past() -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()


class _OneSecond(datetime):
    """Every cron run ID starts in the same second."""

    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


@pytest.fixture
def same_second(monkeypatch):
    """Cron runs and generations that start in one second."""
    monkeypatch.setattr("nerve.agent.sessions.datetime", _OneSecond)


async def _wakeups(install: _Install, session_id: str) -> list[tuple]:
    async with install.db.db.execute(
        "SELECT prompt, status, created_by_actor_id, caused_by_actor_id"
        " FROM session_wakeups WHERE session_id = ? ORDER BY id",
        (session_id,),
    ) as cursor:
        return [tuple(row) async for row in cursor]


async def _cron_logs(install: _Install) -> list[tuple]:
    async with install.db.db.execute(
        "SELECT job_id, status, actor_id, caused_by_actor_id FROM cron_logs ORDER BY id"
    ) as cursor:
        return [tuple(row) async for row in cursor]


async def _fire_due_wakeups(install: _Install) -> CronService:
    """Make every pending wakeup due, run one sweep and wait for its turns."""
    await install.db._write(
        "UPDATE session_wakeups SET fire_at = ? WHERE status = 'pending'", (_past(),),
    )
    service = CronService(install.config, install.engine, install.db)
    before = asyncio.all_tasks()
    await service._sweep_wakeups()
    fired = asyncio.all_tasks() - before
    assert fired, "the sweep fired no wakeup"
    await asyncio.wait_for(asyncio.gather(*fired), timeout=10)
    return service


def _schedule_in_turn(install: _Install, session_id: str, prompt: str):
    """A tool call that the agent makes in the session's current turn."""

    async def _call() -> None:
        engine = install.engine
        result = await engine.registry.invoke(
            "schedule_wakeup", engine._build_tool_context(session_id),
            {"delaySeconds": 120, "prompt": prompt},
        )
        assert not result.is_error, result

    return _call


# --------------------------------------------------------------------------- #
#  Schema                                                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_v057_keeps_old_rows_null_and_is_idempotent(install):
    raw = install.db.db
    for table, column in _SCHEDULE_COLUMNS:
        await raw.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
    await raw.execute(
        "INSERT INTO sessions (id, title, source, status, created_at, updated_at)"
        " VALUES ('old', 'Old chat', 'web', 'idle', '2020-01-01', '2020-01-01')"
    )
    await raw.execute(
        "INSERT INTO session_wakeups (session_id, prompt, fire_at)"
        " VALUES ('old', 'from before', '2020-01-01T00:00:00+00:00')"
    )
    await raw.execute("INSERT INTO cron_logs (job_id) VALUES ('old-job')")
    await raw.commit()

    from nerve.db.migrations.v057_schedule_attribution import up

    await up(raw)
    await up(raw)
    await raw.commit()
    assert await _wakeups(install, "old") == [("from before", "pending", None, None)]
    assert await _cron_logs(install) == [("old-job", None, None, None)]
    assert set(_SCHEDULE_COLUMNS) <= set(ACTOR_COLUMNS)


# --------------------------------------------------------------------------- #
#  Run later                                                                   #
# --------------------------------------------------------------------------- #


async def _run_later(client, headers, session_id: str, message: str) -> None:
    response = await client.post(
        "/api/sessions/run-later", headers=headers,
        json={"session_id": session_id, "message": message, "delay": "30m"},
    )
    assert response.status_code == 200, response.text
    assert response.json()["scheduled"] is True


@pytest.mark.asyncio
async def test_a_run_later_fires_as_the_system_actor_for_the_person(
    install, monkeypatch,
):
    install.use_fake_runtime(monkeypatch)
    alice = install.actor_id("alice")
    system = install.system_actor_id
    async with _client(install.app()) as client:
        session_id = await _new_session(client, install.headers("alice"))
        await _run_later(client, install.headers("alice"), session_id, "summarize the week")

    assert await _wakeups(install, session_id) == [
        ("summarize the week", "pending", alice, alice),
    ]

    await _fire_due_wakeups(install)

    turn = install.turn_in(session_id)
    assert turn["cause"] == alice
    assert await install.events(turn["task_id"]) == [
        (None, "pending", session_id, system, alice),
    ]
    assert await install.session_cause_of(session_id) == alice
    # The person's own message is the only user row; the delivery is internal.
    assert await install.user_rows(session_id) == [("summarize the week", alice)]
    assert await _wakeups(install, session_id) == [
        ("summarize the week", "fired", alice, alice),
    ]


@pytest.mark.asyncio
async def test_a_run_later_in_another_persons_session_fires_for_the_session_cause(
    install, monkeypatch,
):
    """Bob defers a message in Alice's session.

    The wakeup row names Bob, who scheduled it. It fires in Alice's session,
    whose turns run for Alice (the session cause).
    """
    install.use_fake_runtime(monkeypatch)
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    system = install.system_actor_id
    async with _client(install.app()) as client:
        session_id = await _new_session(client, install.headers("alice"))
        await _run_later(client, install.headers("bob"), session_id, "deferred work")

    assert await _wakeups(install, session_id) == [
        ("deferred work", "pending", bob, bob),
    ]

    await _fire_due_wakeups(install)

    fired = install.turn_in(session_id)
    assert fired["cause"] == alice
    assert await install.events(fired["task_id"]) == [
        (None, "pending", session_id, system, alice),
    ]


# --------------------------------------------------------------------------- #
#  Wakeups that the agent sets                                                 #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_wakeup_that_the_agent_sets_runs_for_the_sessions_cause(
    install, monkeypatch,
):
    """Bob's session: the agent sets a wakeup in a turn that Alice sends.

    The wakeup records the system actor and Bob, the session cause, and it
    fires for Bob.
    """
    install.use_fake_runtime(monkeypatch)
    bob = install.actor_id("bob")
    system = install.system_actor_id
    async with _client(install.app()) as client:
        session_id = await _new_session(client, install.headers("bob"))
        runtime = install.runtime(session_id)
        runtime.before_tool_call = _schedule_in_turn(install, session_id, "check the build")
        await _chat(client, install.headers("alice"), session_id, "watch the build")
        runtime.before_tool_call = None

    assert await _wakeups(install, session_id) == [
        ("check the build", "pending", system, bob),
    ]

    await _fire_due_wakeups(install)

    fired = install.turns_in(session_id)[-1]
    assert fired["cause"] == bob
    assert await install.events(fired["task_id"]) == [
        (None, "pending", session_id, system, bob),
    ]


@pytest.mark.asyncio
async def test_a_wakeup_set_in_a_turn_with_no_cause_fires_with_no_cause(
    install, monkeypatch,
):
    install.use_fake_runtime(monkeypatch)
    system = install.system_actor_id
    await install.db.create_session("chat-app", source="telegram", actor=None)
    install.runtime("chat-app").before_tool_call = _schedule_in_turn(
        install, "chat-app", "ask again later",
    )
    await install.engine.run(
        "chat-app", "remind me", source="telegram", actor=None,
    )
    install.runtime("chat-app").before_tool_call = None
    assert await _wakeups(install, "chat-app") == [
        ("ask again later", "pending", system, None),
    ]

    await _fire_due_wakeups(install)

    fired = install.turns_in("chat-app")[-1]
    assert fired["cause"] is None
    assert await install.events(fired["task_id"]) == [
        (None, "pending", "chat-app", system, None),
    ]
    assert await install.session_cause_of("chat-app") is None


class _FakeCli:
    """Stands in for one Claude CLI process (``ClaudeClient``).

    A turn calls the ScheduleWakeup PostToolUse hook from the options that
    the backend built, when ``schedule`` holds a prompt, as the CLI does after
    its built-in tool runs. Then it makes a tool call through the session's
    tool context and completes.
    """

    started: list[_FakeCli] = []
    install: _Install | None = None

    def __init__(self, spec, options, **_kwargs):
        self.spec = spec
        self.options = options
        self.schedule: str | None = None
        self.started.append(self)

    @property
    def native_session_id(self):
        return "native-cli"

    async def connect(self):
        pass

    async def start_turn(self, turn):
        pass

    async def receive_turn(self):
        if self.schedule:
            tool_input = {"delaySeconds": 300, "prompt": self.schedule}
            self.schedule = None
            for matcher in self.options.hooks["PostToolUse"]:
                if matcher.matcher == "ScheduleWakeup":
                    for hook in matcher.hooks:
                        await hook(
                            {"tool_name": "ScheduleWakeup", "tool_input": tool_input},
                            "toolu-1", {},
                        )
        await self.install.agent_tool_call(self.spec.session_id)
        for msg in (
            AssistantMessage(content=[TextBlock(text="ok")], model="claude-test"),
            ResultMessage(
                subtype="success", duration_ms=1, duration_api_ms=1,
                is_error=False, num_turns=1, session_id="native-cli",
                total_cost_usd=0.0, usage={"input_tokens": 1},
            ),
        ):
            for event in translate_message(msg):
                yield event

    async def interrupt(self):
        pass

    def is_alive(self):
        return True

    async def disconnect(self, timeout: float = 5.0):
        pass

    def try_receive_idle_events(self):
        return None

    async def receive_idle_events(self, timeout):
        await asyncio.sleep(min(timeout or 0, 0.05))
        raise asyncio.TimeoutError

    def buffer_used(self):
        return 0


@pytest.fixture
def cli(monkeypatch) -> list[_FakeCli]:
    started: list[_FakeCli] = []
    monkeypatch.setattr(_FakeCli, "started", started)
    monkeypatch.setattr(claude_backend, "ClaudeClient", _FakeCli)
    monkeypatch.setattr(
        claude_backend.ClaudeBackend, "validate_resume_target",
        lambda self, native_id, cwd: True,
    )
    return started


async def _stop_idle_watchers(install: _Install) -> None:
    for session_id in list(install.engine._idle_watchers):
        install.engine._stop_idle_watcher(session_id)


@pytest.mark.asyncio
async def test_a_cli_wakeup_records_the_sessions_cause(
    install, cli, monkeypatch,
):
    """The Claude CLI's built-in ScheduleWakeup, captured by its hook.

    Bob sends the turn in Alice's session; the wakeup and its turn are
    Alice's, the session cause.
    """
    monkeypatch.setattr(_FakeCli, "install", install)
    alice = install.actor_id("alice")
    system = install.system_actor_id
    try:
        async with _client(install.app()) as client:
            session_id = await _new_session(client, install.headers("alice"))
            await _chat(client, install.headers("alice"), session_id, "hello")
            (process,) = cli
            process.schedule = "look at the queue again"
            await _chat(client, install.headers("bob"), session_id, "watch the queue")

        assert await _wakeups(install, session_id) == [
            ("look at the queue again", "pending", system, alice),
        ]

        await _fire_due_wakeups(install)

        assert [turn["cause"] for turn in install.turns_in(session_id)] == [
            alice, alice, alice,
        ]
    finally:
        await _stop_idle_watchers(install)


# --------------------------------------------------------------------------- #
#  Cron runs                                                                   #
# --------------------------------------------------------------------------- #


def _cron_service(install: _Install, *jobs: CronJob) -> CronService:
    service = CronService(install.config, install.engine, install.db)
    service._jobs = list(jobs)
    return service


@pytest.mark.asyncio
async def test_a_scheduled_cron_run_is_the_systems_for_the_system(install, monkeypatch):
    install.use_fake_runtime(monkeypatch)
    system = install.system_actor_id
    job = CronJob(id="nightly", schedule="1h", prompt="tidy the board")
    service = _cron_service(install, job)

    # The scheduler calls the wrapper with the job only.
    await service._run_job_wrapper(job)

    assert await _cron_logs(install) == [("nightly", "success", system, system)]
    turn = install.turns[0]
    assert turn["cause"] == system
    assert await install.user_rows(turn["session_id"]) == [("tidy the board", system)]
    assert await install.session_cause_of(turn["session_id"]) == system
    assert await install.events(turn["task_id"]) == [
        (None, "pending", turn["session_id"], system, system),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("session_mode", ["isolated", "persistent"])
async def test_run_now_through_rest_runs_as_the_system_for_the_person(
    install, monkeypatch, session_mode,
):
    install.use_fake_runtime(monkeypatch)
    alice = install.actor_id("alice")
    system = install.system_actor_id
    job = CronJob(
        id="nightly", schedule="1h", prompt="tidy the board", session_mode=session_mode,
    )
    monkeypatch.setattr("nerve.gateway.server._cron_service", _cron_service(install, job))

    async with _client(install.app()) as client:
        response = await client.post(
            "/api/cron/jobs/nightly/trigger", headers=install.headers("alice"),
        )
        assert response.status_code == 200, response.text
        logs = (await client.get(
            "/api/cron/logs", headers=install.headers("bob"),
        )).json()["logs"]

    assert [(log["actor_id"], log["caused_by_actor_id"]) for log in logs] == [
        (alice, alice),
    ]
    turn = install.turns[0]
    assert turn["cause"] == alice
    assert await install.user_rows(turn["session_id"]) == [("tidy the board", system)]
    assert await install.events(turn["task_id"]) == [
        (None, "pending", turn["session_id"], system, alice),
    ]
    assert await install.session_cause_of(turn["session_id"]) == alice


@pytest.mark.asyncio
async def test_a_persistent_generation_that_a_run_now_creates_keeps_the_persons_cause(
    install, monkeypatch, same_second,
):
    """Alice's "Run now" starts a persistent job's first session.

    The session's cause is Alice. A later scheduled run reuses the session
    and keeps that cause (a documented approximation). When the session
    rotates, its retitle is Nerve's change for Alice, and the next session
    is the system actor's.
    """
    install.use_fake_runtime(monkeypatch)
    alice = install.actor_id("alice")
    system = install.system_actor_id
    job = CronJob(
        id="digest", schedule="1h", prompt="write the digest",
        session_mode="persistent", context_rotate_hours=1,
    )
    service = _cron_service(install, job)
    monkeypatch.setattr("nerve.gateway.server._cron_service", service)

    async with _client(install.app()) as client:
        response = await client.post(
            "/api/cron/jobs/digest/trigger", headers=install.headers("alice"),
        )
    assert response.status_code == 200, response.text
    first = install.turns[0]["session_id"]
    assert await install.session_cause_of(first) == alice

    await service._run_job_wrapper(job)
    assert (install.turns[1]["session_id"], install.turns[1]["cause"]) == (first, alice)

    # The session ages past the rotation window. The next generation starts
    # in the same second as the first one, and still gets its own session.
    await install.db.update_session_fields(first, {
        "connected_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
    })
    await service._run_job_wrapper(job)

    second = install.turns[2]["session_id"]
    assert second != first
    assert await install.session_cause_of(second) == system
    assert install.turns[2]["cause"] == system
    async with install.db.db.execute(
        "SELECT details, actor_id, caused_by_actor_id FROM session_events"
        " WHERE session_id = ? AND event_type = 'title_changed'",
        (first,),
    ) as cursor:
        retitles = [
            (json.loads(row[0]).get("reason"), row[1], row[2])
            async for row in cursor
        ]
    assert ("cron_rotation", system, alice) in retitles
    assert await _cron_logs(install) == [
        ("digest", "success", alice, alice),
        ("digest", "success", system, system),
        ("digest", "success", system, system),
    ]


@pytest.mark.asyncio
async def test_an_immediate_rotation_starts_a_new_session(
    install, monkeypatch, same_second,
):
    """A manual rotation right after the first run, in the same second, starts
    a new session for the system actor; the first one keeps Alice."""
    install.use_fake_runtime(monkeypatch)
    alice = install.actor_id("alice")
    job = CronJob(
        id="digest", schedule="1h", prompt="write the digest", session_mode="persistent",
    )
    service = _cron_service(install, job)
    monkeypatch.setattr("nerve.gateway.server._cron_service", service)

    async with _client(install.app()) as client:
        await client.post("/api/cron/jobs/digest/trigger", headers=install.headers("alice"))
        rotated = (await client.post(
            "/api/cron/jobs/digest/rotate", headers=install.headers("alice"),
        )).json()

    first = install.turns[0]["session_id"]
    assert rotated["rotated"] is True
    assert rotated["old_session_id"] == first
    assert rotated["new_session_id"] != first
    assert rotated["new_session_id"].startswith("cron:digest:20260102-030405-")
    assert await install.session_cause_of(first) == alice
    assert await install.session_cause_of(rotated["new_session_id"]) == (
        install.system_actor_id
    )


@pytest.mark.asyncio
async def test_run_now_of_a_workflow_job_starts_the_run_for_the_person(
    install, monkeypatch,
):
    """A job that declares a workflow run starts it for the person who used
    "Run now"; a scheduled run of the same job starts it for the system."""
    import nerve.workflows

    alice = install.actor_id("alice")
    system = install.system_actor_id
    started: list[dict] = []

    async def _start_run(**kwargs):
        started.append(kwargs)
        return {"id": f"wfr-{len(started)}", "budget_usd": 1.0}

    monkeypatch.setattr(
        nerve.workflows, "_service", SimpleNamespace(start_run=_start_run, db=install.db),
    )
    job = CronJob(
        id="audit", schedule="1h",
        workflow={"engine": "claude-workflow", "prompt": "audit", "budget_usd": 1.0},
    )
    service = _cron_service(install, job)
    monkeypatch.setattr("nerve.gateway.server._cron_service", service)

    async with _client(install.app()) as client:
        response = await client.post(
            "/api/cron/jobs/audit/trigger", headers=install.headers("alice"),
        )
    assert response.status_code == 200, response.text
    await service._run_job_wrapper(job)

    assert [run["attribution"] for run in started] == [
        Attribution(system, alice),
        Attribution(system, system),
    ]
    assert await _cron_logs(install) == [
        ("audit", "success", alice, alice),
        ("audit", "success", system, system),
    ]


@pytest.mark.asyncio
async def test_run_now_of_a_source_records_the_person(install, monkeypatch):
    bob = install.actor_id("bob")

    async def _run():
        return SimpleNamespace(records_ingested=2, records_dropped=0, error=None)

    runner = SimpleNamespace(
        job_id="source:fake", source=SimpleNamespace(source_name="fake"), run=_run,
    )
    service = _cron_service(install)
    service._source_runners = [runner]
    monkeypatch.setattr("nerve.gateway.server._cron_service", service)

    async with _client(install.app()) as client:
        response = await client.post(
            "/api/cron/jobs/source:fake/trigger", headers=install.headers("bob"),
        )
    assert response.status_code == 200, response.text

    # A scheduled run of the same source.
    await service._run_source_wrapper(runner)

    system = install.system_actor_id
    assert await _cron_logs(install) == [
        ("source:fake", "success", bob, bob),
        ("source:fake", "success", system, system),
    ]


@pytest.mark.asyncio
async def test_a_missed_run_is_the_systems(install):
    service = _cron_service(install)
    await service._record_missed_run("nightly", datetime.now(timezone.utc))

    system = install.system_actor_id
    assert await _cron_logs(install) == [("nightly", "missed", system, system)]


@pytest.mark.asyncio
async def test_the_cli_run_of_a_job_is_the_systems(install, monkeypatch):
    """``nerve cron <id>`` runs the job with no attribution argument."""
    install.use_fake_runtime(monkeypatch)
    job = CronJob(id="nightly", schedule="1h", prompt="tidy the board")
    await _cron_service(install, job).run_job("nightly")

    system = install.system_actor_id
    assert await _cron_logs(install) == [("nightly", "success", system, system)]
    assert install.turns[0]["cause"] == system


# --------------------------------------------------------------------------- #
#  Payloads                                                                    #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_session_list_names_who_scheduled_the_pending_wakeup(
    install, monkeypatch,
):
    install.use_fake_runtime(monkeypatch)
    alice, bob = install.actor_id("alice"), install.actor_id("bob")
    system = install.system_actor_id
    async with _client(install.app()) as client:
        deferred = await _new_session(client, install.headers("alice"))
        await _run_later(client, install.headers("alice"), deferred, "later")
        looping = await _new_session(client, install.headers("alice"))
        install.runtime(looping).before_tool_call = _schedule_in_turn(
            install, looping, "next tick",
        )
        await _chat(client, install.headers("bob"), looping, "loop on this")
        plain = await _new_session(client, install.headers("bob"))
        rows = {
            row["id"]: row
            for row in (
                await client.get("/api/sessions", headers=install.headers("bob"))
            ).json()["sessions"]
        }

    def _scheduled_by(session_id: str) -> tuple:
        row = rows[session_id]
        return (
            row["pending_wakeup_at"] is not None,
            row["pending_wakeup_created_by_actor_id"],
            row["pending_wakeup_caused_by_actor_id"],
        )

    assert _scheduled_by(deferred) == (True, alice, alice)
    # The agent set it in a turn that Bob sent; Alice is the session cause.
    assert _scheduled_by(looping) == (True, system, alice)
    assert _scheduled_by(plain) == (False, None, None)


@pytest.mark.asyncio
async def test_the_session_running_event_names_who_scheduled_the_wakeup(
    install, monkeypatch,
):
    from nerve.agent import engine as engine_module

    bob = install.actor_id("bob")
    await install.db.create_session("s1", source="web", actor=None)
    await install.db.add_wakeup(
        "s1", prompt="tick", fire_at=_past(),
        attribution=Attribution(install.system_actor_id, bob),
    )
    events: list[dict] = []

    async def _broadcast(channel, message):
        events.append(message)

    monkeypatch.setattr(engine_module.broadcaster, "broadcast", _broadcast)
    await install.engine._broadcast_session_running("s1", False)

    (event,) = events
    assert event["type"] == "session_running"
    assert event["pending_wakeup_at"] is not None
    assert event["pending_wakeup_created_by_actor_id"] == install.system_actor_id
    assert event["pending_wakeup_caused_by_actor_id"] == bob


@pytest.mark.asyncio
async def test_the_actor_directory_names_every_scheduler_and_cause(
    install, monkeypatch,
):
    install.use_fake_runtime(monkeypatch)
    job = CronJob(id="nightly", schedule="1h", prompt="tidy the board")
    monkeypatch.setattr("nerve.gateway.server._cron_service", _cron_service(install, job))
    async with _client(install.app()) as client:
        session_id = await _new_session(client, install.headers("alice"))
        await _run_later(client, install.headers("alice"), session_id, "later")
        await client.post("/api/cron/jobs/nightly/trigger", headers=install.headers("bob"))
        directory = {
            row["id"]
            for row in (
                await client.get("/api/actors", headers=install.headers("alice"))
            ).json()["actors"]
        }

    for table, column in _SCHEDULE_COLUMNS:
        async with install.db.db.execute(
            f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL"
        ) as cursor:
            referenced = {row[0] async for row in cursor}
        assert referenced, (table, column)
        assert referenced <= directory, (table, column)


# --------------------------------------------------------------------------- #
#  External mode                                                               #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_external_mode_run_later_and_run_now_name_the_principal(
    external_install, monkeypatch,
):
    install = external_install
    install.use_fake_runtime(monkeypatch)
    alice = _as(ALICE_PRINCIPAL, "Alice")
    job = CronJob(id="nightly", schedule="1h", prompt="tidy the board")
    monkeypatch.setattr("nerve.gateway.server._cron_service", _cron_service(install, job))
    async with _client(install.app()) as client:
        session_id = await _new_session(client, alice)
        await _run_later(client, alice, session_id, "summarize the week")
        await client.post("/api/cron/jobs/nightly/trigger", headers=alice)

    assert await _wakeups(install, session_id) == [
        ("summarize the week", "pending", ALICE_PRINCIPAL, ALICE_PRINCIPAL),
    ]
    assert await _cron_logs(install) == [
        ("nightly", "success", ALICE_PRINCIPAL, ALICE_PRINCIPAL),
    ]

    await _fire_due_wakeups(install)

    assert install.turn_in(session_id)["cause"] == ALICE_PRINCIPAL
    assert {turn["cause"] for turn in install.turns} == {ALICE_PRINCIPAL}


def _named_principals(process: _FakeCli) -> list[str]:
    """The principal headers that the CLI process sends on model requests."""
    named = []
    for line in process.options.env.get(CUSTOM_HEADERS_ENV, "").splitlines():
        name, _, value = line.partition(":")
        if name.strip().lower() == INFERENCE_PRINCIPAL_HEADER.lower():
            named.append(value.strip())
    return named


@pytest.mark.asyncio
async def test_external_mode_wakeup_turns_name_the_session_cause_to_the_model_api(
    external_install, cli, monkeypatch,
):
    """A fired wakeup in Alice's session names Alice on its model requests.

    Bob sends the turn in which the agent sets a wakeup, and Bob defers a
    message in another of Alice's sessions. Both fire in Alice's sessions,
    whose CLI processes name Alice, the session cause.
    """
    install = external_install
    monkeypatch.setattr(_FakeCli, "install", install)
    monkeypatch.delenv(CUSTOM_HEADERS_ENV, raising=False)
    alice, bob = _as(ALICE_PRINCIPAL, "Alice"), _as(BOB_PRINCIPAL, "Bob")
    try:
        async with _client(install.app()) as client:
            looping = await _new_session(client, alice)
            await _chat(client, alice, looping, "hello")
            cli[-1].schedule = "check the build"
            await _chat(client, bob, looping, "watch the build")
            deferred = await _new_session(client, alice)
            await _run_later(client, bob, deferred, "summarize the week")

        await _fire_due_wakeups(install)

        assert {turn["cause"] for turn in install.turns} == {ALICE_PRINCIPAL}
        for session_id in (looping, deferred):
            processes = [p for p in cli if p.spec.session_id == session_id]
            assert [_named_principals(p) for p in processes] == [[ALICE_PRINCIPAL]]
    finally:
        await _stop_idle_watchers(install)


@pytest.mark.asyncio
async def test_external_mode_a_run_now_session_names_the_person(
    external_install, cli, monkeypatch,
):
    """Bob's "Run now" creates the run's session for Bob, so its CLI process
    names Bob on its model requests."""
    install = external_install
    monkeypatch.setattr(_FakeCli, "install", install)
    monkeypatch.delenv(CUSTOM_HEADERS_ENV, raising=False)
    job = CronJob(id="nightly", schedule="1h", prompt="tidy the board")
    monkeypatch.setattr("nerve.gateway.server._cron_service", _cron_service(install, job))
    try:
        async with _client(install.app()) as client:
            response = await client.post(
                "/api/cron/jobs/nightly/trigger", headers=_as(BOB_PRINCIPAL, "Bob"),
            )
        assert response.status_code == 200, response.text

        (process,) = cli
        assert process.spec.session_id.startswith("cron:nightly:")
        assert _named_principals(process) == [BOB_PRINCIPAL]
    finally:
        await _stop_idle_watchers(install)


@pytest.mark.asyncio
async def test_external_mode_concurrent_run_nows_get_their_own_sessions(
    external_install, cli, monkeypatch, same_second,
):
    """Alice and Bob use "Run now" on one job at the same time.

    Both runs start in the same second. Each gets its own session, with its
    person as cause, and its CLI process names that person.
    """
    install = external_install
    monkeypatch.setattr(_FakeCli, "install", install)
    monkeypatch.delenv(CUSTOM_HEADERS_ENV, raising=False)
    job = CronJob(id="nightly", schedule="1h", prompt="tidy the board")
    monkeypatch.setattr("nerve.gateway.server._cron_service", _cron_service(install, job))
    alice, bob = _as(ALICE_PRINCIPAL, "Alice"), _as(BOB_PRINCIPAL, "Bob")
    try:
        async with _client(install.app()) as client:
            responses = await asyncio.gather(
                client.post("/api/cron/jobs/nightly/trigger", headers=alice),
                client.post("/api/cron/jobs/nightly/trigger", headers=bob),
            )
        assert [r.status_code for r in responses] == [200, 200]

        sessions = {turn["cause"]: turn["session_id"] for turn in install.turns}
        assert set(sessions) == {ALICE_PRINCIPAL, BOB_PRINCIPAL}
        assert sessions[ALICE_PRINCIPAL] != sessions[BOB_PRINCIPAL]
        for principal, session_id in sessions.items():
            assert session_id.startswith("cron:nightly:20260102-030405-")
            assert await install.session_cause_of(session_id) == principal
            (process,) = [p for p in cli if p.spec.session_id == session_id]
            assert _named_principals(process) == [principal]
        assert sorted(await _cron_logs(install)) == sorted([
            ("nightly", "success", ALICE_PRINCIPAL, ALICE_PRINCIPAL),
            ("nightly", "success", BOB_PRINCIPAL, BOB_PRINCIPAL),
        ])
    finally:
        await _stop_idle_watchers(install)
