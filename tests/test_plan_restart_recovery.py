"""Tests for the plan restart-recovery pass (``recover_orphaned_plans``).

``plans.status = 'implementing'`` asserts a live in-process implementation run,
but that run is a bare ``asyncio`` task, so a SIGKILL/OOM/daemon restart leaves
the row asserting an obligation nobody holds: re-approval is refused (both
surfaces gate on ``pending``) and ``plan_propose`` is refused forever for that
task. These tests pin the startup pass that reconciles such rows to ``failed``,
mirroring the recovery pass workflow runs already have.

The one piece of apparent extra machinery -- the resume-queue exclusion -- is
correctness, not caution: ``nerve restart --resume <impl-sid>`` is a supported
operator action, and a resumed implementation session completes through
``task_done``, which closes only an ``implementing`` plan. Sweeping such a plan
would leave the resumed session unable to close it. T10-T13 and T15 pin that.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# --------------------------------------------------------------------------- #
#  Helpers                                                                     #
# --------------------------------------------------------------------------- #


async def _recover(db, notification_service=None) -> int:
    """Call the pass under test.

    IMPORTANT: The import is inside the function on purpose. A module-level import of a
    symbol this change ADDS would make the whole module a collection error when
    the file is run against the pre-change tree, which would hide the two
    behavioural witnesses (``test_lifespan_recovers_an_orphaned_plan`` and
    ``test_recovery_runs_before_cron_starts``): those must fail on plan state,
    not on an ImportError.
    """
    from nerve.agent.plan_service import recover_orphaned_plans

    return await recover_orphaned_plans(db, notification_service)


async def _seed_plan(
    db,
    plan_id: str,
    status: str = "implementing",
    impl_session_id: str | None = None,
    task_id: str = "task-1",
) -> None:
    """Create a plan row in an arbitrary status, with an optional owner session.

    ``create_plan`` always writes ``pending`` and never sets ``impl_session_id``,
    so both are applied afterwards via ``update_plan`` (which is exactly how the
    approval surfaces reach ``implementing`` too).
    """
    await db.create_plan(plan_id, task_id, f"content of {plan_id}")
    fields: dict = {"status": status}
    if impl_session_id is not None:
        fields["impl_session_id"] = impl_session_id
    await db.update_plan(plan_id, **fields)


async def _seed_resumable_session(db, session_id: str) -> None:
    """A session the engine's resume pass would accept: present, not archived,
    not a satellite, with an SDK session to resume."""
    await db.create_session(session_id, source="web", status="running")
    await db.update_session_metadata(session_id, {"sdk_session_id": f"sdk-{session_id}"})


def _notif_stub() -> MagicMock:
    return MagicMock(send_notification=AsyncMock(return_value="notif-1"))


def _queue(monkeypatch, tmp_path, *session_ids: str, raw: str | None = None):
    """Point ``plan_service.RESUME_QUEUE_FILE`` at a temp file and fill it.

    IMPORTANT: Patching the module global is what matters, not ``NERVE_HOME``:
    ``RESUME_QUEUE_FILE`` is evaluated at ``nerve.config`` import time, so an
    env var set inside a test lands too late and the test would read the live
    daemon's queue.
    """
    q = tmp_path / "resume-queue"
    if raw is not None:
        q.write_text(raw)
    elif session_ids:
        q.write_text("".join(f"{sid}\n" for sid in session_ids))
    monkeypatch.setattr("nerve.agent.plan_service.RESUME_QUEUE_FILE", q)
    return q


def _no_queue(monkeypatch, tmp_path):
    """Point the queue at a path that does not exist (nothing was enrolled)."""
    q = tmp_path / "absent-queue"
    monkeypatch.setattr("nerve.agent.plan_service.RESUME_QUEUE_FILE", q)
    return q


# --------------------------------------------------------------------------- #
#  Store level                                                                 #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_get_implementing_plans_is_unlimited_and_ordered(db):
    """T1: every ``implementing`` row, oldest first, with no limit.

    The 150-row fixture is the discriminator against
    ``list_plans(status="implementing")``, whose ``limit=100`` would silently
    under-recover a larger backlog.
    """
    for i in range(150):
        await _seed_plan(db, f"plan-impl-{i:03d}")
    for other in ("pending", "done", "declined", "superseded", "failed"):
        await _seed_plan(db, f"plan-{other}", status=other)

    rows = await db.get_implementing_plans()

    assert len(rows) == 150
    assert {r["status"] for r in rows} == {"implementing"}
    assert [r["id"] for r in rows] == sorted(r["id"] for r in rows)


@pytest.mark.asyncio
async def test_fail_orphaned_plan_cas_only_flips_implementing(db):
    """T2: True + flip for ``implementing``; False + byte-unchanged otherwise."""
    await _seed_plan(db, "plan-impl")
    assert await db.fail_orphaned_plan("plan-impl") is True
    assert (await db.get_plan("plan-impl"))["status"] == "failed"

    for other in ("pending", "done", "declined", "superseded", "failed"):
        pid = f"plan-{other}"
        await _seed_plan(db, pid, status=other)
        before = await db.get_plan(pid)
        assert await db.fail_orphaned_plan(pid) is False
        assert await db.get_plan(pid) == before


@pytest.mark.asyncio
async def test_fail_orphaned_plan_flips_a_null_owner_row(db):
    """T3: a NULL ``impl_session_id`` row still flips.

    This is the test that discriminates the status-only CAS from an
    owner-keyed one: ``PATCH /api/plans/{id}`` takes an arbitrary status with
    no whitelist, so it can create ``implementing`` with no owner, and
    ``AND impl_session_id = ?`` bound to NULL matches nothing.
    """
    await _seed_plan(db, "plan-noowner")
    assert (await db.get_plan("plan-noowner"))["impl_session_id"] is None

    assert await db.fail_orphaned_plan("plan-noowner") is True
    assert (await db.get_plan("plan-noowner"))["status"] == "failed"


# --------------------------------------------------------------------------- #
#  Helper level                                                                #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_recovery_flips_implementing_to_failed_only(db, monkeypatch, tmp_path):
    """T4: exactly ``failed`` (the literal), other statuses untouched, count returned."""
    _no_queue(monkeypatch, tmp_path)
    await _seed_plan(db, "plan-a")
    await _seed_plan(db, "plan-b")
    survivors = {}
    for other in ("pending", "done", "declined", "superseded", "failed"):
        pid = f"plan-{other}"
        await _seed_plan(db, pid, status=other)
        survivors[pid] = other

    flipped = await _recover(db, _notif_stub())

    assert flipped == 2
    assert (await db.get_plan("plan-a"))["status"] == "failed"
    assert (await db.get_plan("plan-b"))["status"] == "failed"
    for pid, status in survivors.items():
        assert (await db.get_plan(pid))["status"] == status


@pytest.mark.asyncio
async def test_recovery_sends_exactly_one_notification(db, monkeypatch, tmp_path):
    """T5: one high-priority system notification naming every flipped id."""
    _no_queue(monkeypatch, tmp_path)
    await _seed_plan(db, "plan-a")
    await _seed_plan(db, "plan-b")
    notif = _notif_stub()

    await _recover(db, notif)

    assert notif.send_notification.await_count == 1
    kwargs = notif.send_notification.await_args.kwargs
    assert kwargs["session_id"] == "system"
    assert kwargs["priority"] == "high"
    assert "plan-a" in kwargs["body"] and "plan-b" in kwargs["body"]


@pytest.mark.asyncio
async def test_recovery_is_silent_with_nothing_to_do(db, monkeypatch, tmp_path):
    """T5b: no ``implementing`` rows means no notification at all."""
    _no_queue(monkeypatch, tmp_path)
    await _seed_plan(db, "plan-pending", status="pending")
    notif = _notif_stub()

    assert await _recover(db, notif) == 0
    assert notif.send_notification.await_count == 0


@pytest.mark.asyncio
async def test_recovery_ignores_a_plan_that_left_implementing_mid_sweep(
    db, monkeypatch, tmp_path,
):
    """T6: a row leaving ``implementing`` between read and write is not claimed.

    Pins notify-on-flipped rather than notify-on-orphaned: with only this plan
    orphaned, a pass that keyed its notification off the pre-CAS list would
    alert about a plan that legitimately completed.
    """
    _no_queue(monkeypatch, tmp_path)
    await _seed_plan(db, "plan-racer")
    notif = _notif_stub()

    real_cas = db.fail_orphaned_plan

    async def racing_cas(plan_id: str) -> bool:
        # A concurrent ``task_done`` lands after the read, before our write.
        await db.update_plan(plan_id, status="done")
        return await real_cas(plan_id)

    with patch.object(db, "fail_orphaned_plan", side_effect=racing_cas):
        flipped = await _recover(db, notif)

    assert flipped == 0
    assert (await db.get_plan("plan-racer"))["status"] == "done"
    assert notif.send_notification.await_count == 0


@pytest.mark.asyncio
async def test_recovery_survives_a_failing_notification(db, monkeypatch, tmp_path):
    """T7: rows are already reconciled, so a notification error cannot abort."""
    _no_queue(monkeypatch, tmp_path)
    await _seed_plan(db, "plan-a")
    await _seed_plan(db, "plan-b")
    notif = MagicMock(send_notification=AsyncMock(side_effect=RuntimeError("boom")))

    assert await _recover(db, notif) == 2
    assert (await db.get_plan("plan-a"))["status"] == "failed"
    assert (await db.get_plan("plan-b"))["status"] == "failed"


# --------------------------------------------------------------------------- #
#  Resume-queue exclusion                                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_enrolled_and_eligible_plan_is_spared(db, monkeypatch, tmp_path):
    """T10: the enrolled plan stays ``implementing``, an unenrolled one fails.

    Both in ONE sweep, so this discriminates per-plan exclusion from an
    all-or-nothing skip.
    """
    await _seed_resumable_session(db, "impl-live")
    _queue(monkeypatch, tmp_path, "impl-live")
    await _seed_plan(db, "plan-resumed", impl_session_id="impl-live")
    await _seed_plan(db, "plan-orphan", impl_session_id="impl-dead")
    notif = _notif_stub()

    flipped = await _recover(db, notif)

    assert flipped == 1
    assert (await db.get_plan("plan-resumed"))["status"] == "implementing"
    assert (await db.get_plan("plan-orphan"))["status"] == "failed"
    body = notif.send_notification.await_args.kwargs["body"]
    assert "plan-orphan" in body and "plan-resumed" not in body
    # The COUNT in the body must also be what was flipped, not what was read:
    # this sweep reads 2 orphans and flips 1, so a body built from the pre-CAS
    # list would announce "2 plan(s)" while naming only one.
    assert body.startswith("1 plan(s)"), body


@pytest.mark.asyncio
async def test_eligibility_preflight_covers_every_skip_class(db, monkeypatch, tmp_path):
    """T15: one enrolled plan per engine skip class is still swept.

    ``resume_enrolled_sessions`` skips missing / archived / satellite /
    no-SDK-session ids, and a skipped session never resumes -- so sparing its
    plan would wedge the plan for nothing. All four classes plus an eligible
    control in ONE sweep: a single-class fixture could not tell a full
    preflight from one checking only that predicate.
    """
    # 1. session row absent entirely (no create_session at all)
    await _seed_plan(db, "plan-missing", impl_session_id="impl-missing")
    # 2. archived
    await _seed_resumable_session(db, "impl-archived")
    await db.update_session_fields("impl-archived", {"status": "archived"})
    await _seed_plan(db, "plan-archived", impl_session_id="impl-archived")
    # 3. satellite (source="external")
    await db.create_session("impl-external", source="external", status="running")
    await db.update_session_metadata("impl-external", {"sdk_session_id": "sdk-ext"})
    await _seed_plan(db, "plan-external", impl_session_id="impl-external")
    # 4. no SDK session to resume
    await db.create_session("impl-nosdk", source="web", status="running")
    await _seed_plan(db, "plan-nosdk", impl_session_id="impl-nosdk")
    # control: fully eligible
    await _seed_resumable_session(db, "impl-ok")
    await _seed_plan(db, "plan-ok", impl_session_id="impl-ok")

    _queue(
        monkeypatch, tmp_path,
        "impl-missing", "impl-archived", "impl-external", "impl-nosdk", "impl-ok",
    )

    flipped = await _recover(db, _notif_stub())

    assert flipped == 4
    for pid in ("plan-missing", "plan-archived", "plan-external", "plan-nosdk"):
        assert (await db.get_plan(pid))["status"] == "failed", pid
    assert (await db.get_plan("plan-ok"))["status"] == "implementing"


def test_queue_parse_matches_the_engine(monkeypatch, tmp_path):
    """T11: byte-parity with ``AgentEngine.resume_enrolled_sessions``' parse.

    Asserted against the engine's own expression rather than a hand-copied
    list, so the two readers cannot drift about what counts as enrolled.
    """
    # The internal-whitespace id is load-bearing: without it a
    # whitespace-splitting parse (``raw.split()``) yields the identical set on
    # blank lines / padding / duplicates alone, so the fixture could not tell
    # the two apart. With it, splitting shatters one id into three tokens.
    raw = "impl-a\n\n  impl-b  \nimpl-a\n\t\n impl-c\nimpl d with space\n"
    _queue(monkeypatch, tmp_path, raw=raw)

    # The engine's parse, verbatim (engine.py resume_enrolled_sessions).
    seen: set[str] = set()
    ids: list[str] = []
    for line in raw.splitlines():
        sid = line.strip()
        if sid and sid not in seen:
            seen.add(sid)
            ids.append(sid)

    from nerve.agent.plan_service import _enrolled_resume_session_ids

    assert _enrolled_resume_session_ids() == set(ids)


@pytest.mark.asyncio
async def test_recovery_does_not_consume_the_queue(db, monkeypatch, tmp_path):
    """T12: the queue is READ, never drained.

    The engine's resume task is the sole drainer and runs later in startup;
    consuming the file here would silently cancel every enrolled resume.
    """
    await _seed_resumable_session(db, "impl-live")
    q = _queue(monkeypatch, tmp_path, "impl-live")
    before = q.read_bytes()
    await _seed_plan(db, "plan-resumed", impl_session_id="impl-live")

    await _recover(db, _notif_stub())

    assert q.exists()
    assert q.read_bytes() == before


@pytest.mark.asyncio
async def test_missing_queue_sweeps_but_unreadable_queue_does_not(
    db, monkeypatch, tmp_path,
):
    """T13: the two error cases go OPPOSITE ways.

    A missing file is a definite answer from the sole writer (the CLI appends
    before triggering the restart), so sweep. An ``OSError`` is not an answer:
    our read and the engine's happen at different instants, so a transient
    error here can succeed there and resume a session whose plan we would have
    failed -- hence fail closed. Both arms assert on plan state, since a
    single-arm test cannot discriminate the two directions.
    """
    # Arm 1: missing queue -> sweep.
    _no_queue(monkeypatch, tmp_path)
    await _seed_plan(db, "plan-a")
    assert await _recover(db, _notif_stub()) == 1
    assert (await db.get_plan("plan-a"))["status"] == "failed"

    # Arm 2: unreadable queue -> sweep nothing, notify nothing.
    await _seed_plan(db, "plan-b")
    unreadable = tmp_path / "unreadable-queue"
    unreadable.write_text("impl-whatever\n")
    monkeypatch.setattr(
        "nerve.agent.plan_service.RESUME_QUEUE_FILE", unreadable,
    )
    notif = _notif_stub()
    with patch.object(
        type(unreadable), "read_text", side_effect=OSError("EIO"),
    ):
        assert await _recover(db, notif) == 0
    assert (await db.get_plan("plan-b"))["status"] == "implementing"
    assert notif.send_notification.await_count == 0


@pytest.mark.asyncio
async def test_an_already_failed_plan_is_not_reclaimed(db, monkeypatch, tmp_path):
    """T16: a ``failed`` + enrolled + eligible plan is left ``failed``.

    IMPORTANT: This documents an ACCEPTED trade, not a bug. An earlier daemon can sweep
    a plan before its session is enrolled (the daemon publishes its PID before
    startup runs), so a later daemon can resume that session with the plan
    already ``failed``. The pass deliberately does NOT reclaim it: the task
    still completes via ``task_done`` and ``plan_propose`` stays unblocked, so
    only the plan's label differs. Re-adopting it would need durable recovery
    provenance -- a legitimately failed plan also keeps its
    ``impl_session_id``, and ``PATCH /api/plans/{id}`` can set ``failed`` with
    no whitelist -- i.e. a schema change this change avoids. Do not "fix" this
    into a reclaim without solving that first.
    """
    await _seed_resumable_session(db, "impl-live")
    _queue(monkeypatch, tmp_path, "impl-live")
    await _seed_plan(db, "plan-swept", status="failed", impl_session_id="impl-live")
    notif = _notif_stub()

    assert await _recover(db, notif) == 0
    assert (await db.get_plan("plan-swept"))["status"] == "failed"
    assert notif.send_notification.await_count == 0


# --------------------------------------------------------------------------- #
#  Wiring: the pass is reachable from daemon startup, and correctly ordered     #
# --------------------------------------------------------------------------- #


@pytest.fixture
def lifespan_app(tmp_path, monkeypatch):
    """Drive the real gateway ``lifespan`` against a temp DB.

    Follows ``tests/test_mcp_http_integration.py``: config built in-process,
    ``init_db`` patched to a temp path, heavy components stubbed. Yields
    ``(app_factory, db_holder, cron_stub, engine_stub)`` so a test can seed rows
    before startup, inspect them after, and re-stub the engine method whose
    ordering against recovery is load-bearing (the resume-queue drainer).
    """
    import nerve.config as config_module
    from nerve.config import NerveConfig
    from nerve.gateway import server as gw_server

    config = NerveConfig()
    config.workspace = tmp_path / "workspace"
    config.workspace.mkdir(parents=True, exist_ok=True)
    config.anthropic_api_key = ""
    config.telegram.enabled = False
    config.mcp_endpoint.enabled = False
    config.agent.model_discovery = False
    config_module._config = config

    fake_engine = MagicMock()
    fake_engine.config = config
    fake_engine.db = None
    fake_engine._memory_bridge = None
    fake_engine._skill_manager = None
    fake_engine.notification_service = None
    fake_engine.set_notification_service = MagicMock()
    fake_engine.shutdown = AsyncMock()
    fake_engine.initialize = AsyncMock()
    fake_engine.router = MagicMock()
    fake_engine.run = AsyncMock()
    fake_engine.is_session_running = MagicMock(return_value=False)
    fake_engine.sessions = MagicMock()
    fake_engine.sessions.run_cleanup = AsyncMock(return_value={})
    fake_engine.run_memorization_sweep = AsyncMock(return_value={})
    fake_engine.run_idle_client_sweep = AsyncMock()
    fake_engine.resume_enrolled_sessions = AsyncMock(return_value=0)
    from nerve.agent.tools import build_default_registry

    fake_engine.registry = build_default_registry()

    db_path = tmp_path / "lifespan.db"
    holder: dict = {}

    async def _init_db_patched(*args, **kwargs):
        from nerve.db import Database

        d = Database(db_path)
        await d.connect()
        fake_engine.db = d
        holder["db"] = d
        return d

    async def _close_db_patched():
        if fake_engine.db is not None:
            await fake_engine.db.close()

    notif_stub = MagicMock(
        send_notification=AsyncMock(return_value="notif-stub"),
        expire_stale=AsyncMock(return_value=0),
        hide_session_label_for=MagicMock(),
    )
    cron_stub = MagicMock(start=AsyncMock(), stop=AsyncMock(),
                          _source_runners=[], _jobs=[])

    def _build():
        return gw_server.create_app()

    with patch("nerve.gateway.server.AgentEngine", return_value=fake_engine), \
         patch("nerve.gateway.server.init_db", side_effect=_init_db_patched), \
         patch("nerve.gateway.server.close_db", side_effect=_close_db_patched), \
         patch("nerve.notifications.service.NotificationService",
               return_value=notif_stub), \
         patch("nerve.gateway.server.init_langfuse"), \
         patch("nerve.cron.service.CronService", return_value=cron_stub):
        yield _build, holder, cron_stub, fake_engine

    gw_server._engine = None
    gw_server._mcp_manager = None
    config_module._config = None


def _seed_via_sqlite(db_path, plan_id: str, impl_session_id: str | None = None) -> None:
    """Insert an ``implementing`` plan directly, before the app starts.

    Synchronous on purpose: the lifespan tests are sync (``TestClient``), so
    they cannot await the async store, and the row must exist before startup.

    ``impl_session_id`` defaults to NULL, which is what an unowned orphan looks
    like. Pass it to build a plan the resume exclusion can actually spare: the
    exclusion tests ``sid and sid in enrolled``, so a NULL owner is swept however
    the queue reads.
    """
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO plans (id, task_id, impl_session_id, status, content, "
            "created_at) VALUES (?, ?, ?, 'implementing', 'c', "
            "'2026-01-01T00:00:00Z')",
            (plan_id, "task-1", impl_session_id),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_session_via_sqlite(db_path, session_id: str) -> None:
    """Insert a resume-eligible session row directly, before the app starts.

    Eligible = present, not archived, not a satellite, with an SDK session to
    resume: the four predicates ``AgentEngine.resume_enrolled_sessions`` skips
    on, which ``_resume_eligible`` re-evaluates. Sync for the same reason as
    ``_seed_via_sqlite``.
    """
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO sessions (id, source, status, sdk_session_id, created_at) "
            "VALUES (?, 'web', 'running', ?, '2026-01-01T00:00:00Z')",
            (session_id, f"sdk-{session_id}"),
        )
        conn.commit()
    finally:
        conn.close()


def _plan_status(db_path, plan_id: str) -> str | None:
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT status FROM plans WHERE id = ?", (plan_id,)
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def test_lifespan_recovers_an_orphaned_plan(lifespan_app, tmp_path, monkeypatch):
    """T8: the pass is actually reached from daemon startup.

    IMPORTANT: Imports no new symbol and asserts only DB state, which is what makes its
    base-arm failure behavioural (the plan is still ``implementing``) rather
    than an ``ImportError`` -- and makes it the only test that can observe the
    lifespan call being deleted.
    """
    from fastapi.testclient import TestClient

    build, holder, _cron, _engine = lifespan_app
    db_path = tmp_path / "lifespan.db"
    # First start materializes the schema; seed and restart to get an orphan
    # that predates startup.
    with TestClient(build()):
        pass
    _seed_via_sqlite(db_path, "plan-orphan")
    assert _plan_status(db_path, "plan-orphan") == "implementing"

    with TestClient(build()):
        pass

    assert _plan_status(db_path, "plan-orphan") == "failed"


def test_recovery_runs_before_cron_starts(lifespan_app, tmp_path):
    """T14: cron OBSERVES the plan already reconciled.

    IMPORTANT: Asserted on what cron sees, not on wall-clock order or source line
    numbers: a source-order assertion would pass against a version that awaits
    something else in between. Ordering is load-bearing -- cron's catch-up can
    dispatch a planner run whose ``plan_propose`` reads a stale
    ``implementing`` row and permanently skips the task.
    """
    from fastapi.testclient import TestClient

    build, holder, cron_stub, _engine = lifespan_app
    db_path = tmp_path / "lifespan.db"
    with TestClient(build()):
        pass
    _seed_via_sqlite(db_path, "plan-orphan")

    observed: dict = {}

    async def _record_then_start():
        observed["status"] = _plan_status(db_path, "plan-orphan")

    cron_stub.start = AsyncMock(side_effect=_record_then_start)

    with TestClient(build()):
        pass

    assert observed["status"] == "failed"


def test_a_raising_recovery_does_not_block_startup(lifespan_app, tmp_path):
    """T9: recovery is not startup-critical, so a failure must not take it down."""
    from fastapi.testclient import TestClient

    build, _holder, cron_stub, _engine = lifespan_app
    db_path = tmp_path / "lifespan.db"
    with TestClient(build()):
        pass
    _seed_via_sqlite(db_path, "plan-orphan")

    with patch(
        "nerve.agent.plan_service.recover_orphaned_plans",
        side_effect=RuntimeError("boom"),
    ):
        with TestClient(build()) as client:
            resp = client.get("/health")

    assert resp.status_code == 200
    # The pass raised, so the row is untouched -- startup still completed.
    assert _plan_status(db_path, "plan-orphan") == "implementing"


def test_recovery_reads_the_resume_queue_before_the_engine_drains_it(
    lifespan_app, tmp_path, monkeypatch,
):
    """T18: the enrolled plan survives real startup, so recovery read the queue first.

    IMPORTANT: The exclusion is only sound while recovery's read precedes the engine's
    drain -- ``resume_enrolled_sessions`` unlinks the queue up front, so a
    recovery pass running after it sees no queue at all and sweeps EVERY
    enrolled plan. Nothing in the source pins that ordering, and without this
    test hoisting the drain above recovery keeps every other test green.

    Asserted on plan STATE with a stub that really unlinks, not on call order or
    source line numbers: an order assertion would pass against a version that
    awaits something else in between. Same shape as ``T14``, which pins the
    other load-bearing ordering (cron) through an observer stub.
    """
    from fastapi.testclient import TestClient

    build, _holder, _cron, engine_stub = lifespan_app
    db_path = tmp_path / "lifespan.db"
    # First start materializes the schema; seed and restart to get an orphan
    # that predates startup.
    with TestClient(build()):
        pass

    # The owner session must be seeded too, and must be eligible: the exclusion
    # spares a plan only if its session would really be resumed.
    _seed_via_sqlite(db_path, "plan-resumed", impl_session_id="impl-live")
    _seed_session_via_sqlite(db_path, "impl-live")

    queue = tmp_path / "resume-queue"
    queue.write_text("impl-live\n")
    monkeypatch.setattr("nerve.agent.plan_service.RESUME_QUEUE_FILE", queue)

    drained: dict = {}

    async def _draining_resume() -> int:
        # What the real engine does: read, then unlink up front.
        drained["existed"] = queue.exists()
        queue.unlink(missing_ok=True)
        return 0

    engine_stub.resume_enrolled_sessions = AsyncMock(side_effect=_draining_resume)

    with TestClient(build()):
        pass

    assert _plan_status(db_path, "plan-resumed") == "implementing"
    # Anti-vacuity: without this, a stub that never ran would leave the plan
    # ``implementing`` for the wrong reason and the test would pass even against
    # a deleted exclusion.
    assert drained.get("existed") is True
