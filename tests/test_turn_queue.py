"""The server-visible turn queue and ``client_msg_id``.

A message sent while a turn runs waits for the run lock; while it waits it is
listed in ``queue_update`` (every client sees it), its sender can cancel it,
and Stop never touches it: it runs after the stopped turn. The queue runs in
its order, and a reorder changes that order.
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from nerve.agent.agents.errors import AlreadyStarted, NotOwner, QueueEntryNotFound
from nerve.agent.backends import events as ev
from nerve.agent.engine import AgentEngine
from nerve.agent.streaming import broadcaster
from nerve.config import NerveConfig
from nerve.identity import Actor

SID = "s-queue"
# Another person: never writes anything, so it need not exist in the store.
BOB = Actor(actor_id="act_bob", kind="human", account_id="acc_bob")


class _Client:
    """A runtime whose turns run until interrupted or released."""

    native_session_id = "native-q"
    model = "claude-test"

    def __init__(self) -> None:
        self.prompts: list[str] = []
        self.calls: list[tuple] = []
        self._turn_end: asyncio.Event | None = None

    @property
    def interrupt_spares_background(self) -> bool:
        return True

    @property
    def supports_task_stop(self) -> bool:
        return True

    async def stop_task(self, task_id: str) -> None:
        self.calls.append(("stop_task", task_id))

    async def interrupt(self, *, cancel_queued: bool = False) -> None:
        self.calls.append(("interrupt", cancel_queued))
        self.release()

    def release(self) -> None:
        if self._turn_end is not None:
            self._turn_end.set()

    async def wait_turns(self, n: int) -> None:
        async def _until():
            while len(self.prompts) < n:
                await asyncio.sleep(0.01)
        await asyncio.wait_for(_until(), timeout=5)

    async def start_turn(self, turn) -> None:
        self.prompts.append(turn.text.split("\n\n<system-reminder>")[0])

    async def receive_turn(self):
        end = self._turn_end = asyncio.Event()
        await end.wait()
        yield ev.TurnCompleted(native_session_id="native-q")

    def is_alive(self) -> bool:
        return True

    async def disconnect(self, timeout: float = 5.0) -> None:
        pass

    def try_receive_idle_events(self):
        return None

    def buffer_used(self) -> int:
        return 0


async def _engine(db, tmp_path, client: _Client) -> AgentEngine:
    workspace = tmp_path / "ws"
    workspace.mkdir(parents=True, exist_ok=True)
    cfg = NerveConfig.from_dict({"workspace": str(workspace), "agent": {"backend": "claude"}})
    engine = AgentEngine(cfg, db)
    engine._STOP_WAIT_SECONDS = 0.5
    engine._STOP_ALL_SETTLE_SECONDS = 0.2
    await engine.sessions.get_or_create(SID, title="t", source="web", backend="claude", actor=None)

    async def _get_client(sid, source, model, **_kw):
        engine.sessions.set_client(sid, client)
        return client

    engine._get_or_create_client = _get_client
    return engine


@contextlib.asynccontextmanager
async def _listening():
    """Collect what the session channel broadcasts."""
    seen: list[dict] = []

    async def _cb(_sid, message):
        seen.append(message)

    await broadcaster.register(SID, "test-queue", _cb)
    try:
        yield seen
    finally:
        await broadcaster.unregister(SID, "test-queue")


def _queue_updates(seen: list[dict]) -> list[dict]:
    return [m for m in seen if m.get("type") == "queue_update"]


async def _wait_queued(engine: AgentEngine, n: int) -> None:
    async def _until():
        while len(engine.queue_view(SID)) < n:
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_until(), timeout=5)


async def _finish(*tasks: asyncio.Task) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
        with contextlib.suppress(BaseException):
            await task


async def _drain(client: _Client, *tasks: asyncio.Task) -> None:
    """End every turn until the tasks are done (test cleanup)."""
    async def _until():
        while not all(t.done() for t in tasks):
            client.release()
            await asyncio.sleep(0.02)
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(_until(), timeout=5)
    await _finish(*tasks)


async def _user_messages(db) -> list[str]:
    rows = await db.get_messages(SID)
    return [r["content"] for r in rows if r["role"] == "user"]


@pytest.mark.asyncio
async def test_a_waiting_run_is_listed_and_leaves_when_it_starts(db, tmp_path):
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    async with _listening() as seen:
        first = asyncio.create_task(engine.run(SID, "first", actor=alice, client_msg_id="m1"))
        await client.wait_turns(1)
        queued = asyncio.create_task(engine.run(
            SID, "also run the e2e suite", actor=alice,
            client_msg_id="m2", origin="web",
        ))
        await _wait_queued(engine, 1)
        try:
            (view,) = engine.queue_view(SID)
            assert set(view) == {
                "queue_id", "client_msg_id", "preview", "source", "internal",
                "actor_id", "created_at",
            }
            assert view["queue_id"].startswith("q_") and len(view["queue_id"]) == 10
            assert (view["client_msg_id"], view["preview"], view["source"]) == (
                "m2", "also run the e2e suite", "web",
            )
            assert (view["internal"], view["actor_id"]) == (False, alice.actor_id)
            assert view["created_at"].endswith("Z")
            listed = _queue_updates(seen)[-1]
            assert listed == {"type": "queue_update", "session_id": SID, "queue": [view]}
            # The snapshot carries the queue too.
            assert (await engine.agent_snapshot(SID))["queue"] == [view]

            client.release()
            assert await asyncio.wait_for(first, timeout=5) == ""
            await client.wait_turns(2)
            started = _queue_updates(seen)[-1]
            assert started["queue"] == []
            assert started["removed"] == [{
                "queue_id": view["queue_id"], "client_msg_id": "m2", "reason": "started",
            }]
            # The first run never queued, but its sender still learns it started.
            assert {
                "queue_id": None, "client_msg_id": "m1", "reason": "started",
            } in [r for u in _queue_updates(seen) for r in u.get("removed", [])]
            assert engine.sessions.turn_task(SID) is queued
            # Never part of the turn replay buffer.
            assert not [m for m in broadcaster.get_buffer(SID) if m.get("type") == "queue_update"]
        finally:
            client.release()
            await _finish(first, queued)


@pytest.mark.asyncio
async def test_cancelling_a_queued_message_runs_and_persists_nothing(db, tmp_path):
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    async with _listening() as seen:
        first = asyncio.create_task(engine.run(SID, "first", actor=alice))
        await client.wait_turns(1)
        queued = asyncio.create_task(engine.run(SID, "never mind", actor=alice, client_msg_id="m2"))
        await _wait_queued(engine, 1)
        try:
            (view,) = engine.queue_view(SID)
            entry = engine.cancel_queued_turn(SID, view["queue_id"], actor=alice)
            assert entry.content == "never mind"
            # Idempotent while the cancel lands.
            assert engine.cancel_queued_turn(SID, view["queue_id"], actor=alice) is entry
            assert await asyncio.wait_for(queued, timeout=5) == ""
            assert not queued.cancelled()
            assert queued.cancelling() == 0          # the task carries on normally
            assert engine.queue_view(SID) == []
            update = _queue_updates(seen)[-1]
            assert update["removed"] == [{
                "queue_id": view["queue_id"], "client_msg_id": "m2", "reason": "cancelled",
            }]
            client.release()
            await asyncio.wait_for(first, timeout=5)
            await asyncio.sleep(0.05)
            assert client.prompts == ["first"]       # it never ran
            assert await _user_messages(db) == ["first"]   # nor was it stored
            with pytest.raises(QueueEntryNotFound):
                engine.cancel_queued_turn(SID, view["queue_id"], actor=alice)
        finally:
            client.release()
            await _finish(first, queued)


@pytest.mark.asyncio
async def test_a_cancelled_api_relay_returns_nothing_to_its_handler(db, tmp_path):
    """The queued run is awaited inside a handler (``POST /api/chat``): the
    handler gets ``""`` and keeps running normally."""
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    first = asyncio.create_task(engine.run(SID, "first", actor=alice))
    await client.wait_turns(1)

    async def _handler():
        out = await engine.run(SID, "relay", actor=alice, origin="api")
        await asyncio.sleep(0)       # a later await is not cancelled
        return f"handled:{out}"

    handler = asyncio.create_task(_handler())
    await _wait_queued(engine, 1)
    try:
        (view,) = engine.queue_view(SID)
        assert view["source"] == "api"
        engine.cancel_queued_turn(SID, view["queue_id"], actor=alice)
        assert await asyncio.wait_for(handler, timeout=5) == "handled:"
    finally:
        client.release()
        await _finish(first, handler)


@pytest.mark.asyncio
async def test_only_the_sender_can_cancel(db, tmp_path):
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    first = asyncio.create_task(engine.run(SID, "first", actor=alice))
    await client.wait_turns(1)
    mine = asyncio.create_task(engine.run(SID, "alice's", actor=alice))
    relay = asyncio.create_task(engine.run(SID, "from a channel", actor=None, origin="telegram"))
    await _wait_queued(engine, 2)
    try:
        alices, relays = engine.queue_view(SID)
        with pytest.raises(NotOwner) as other:
            engine.cancel_queued_turn(SID, alices["queue_id"], actor=BOB)
        assert other.value.status == 403
        # A relay (no person) and internal turns can never be cancelled.
        with pytest.raises(NotOwner):
            engine.cancel_queued_turn(SID, relays["queue_id"], actor=alice)
        assert len(engine.queue_view(SID)) == 2
    finally:
        await _drain(client, first, mine, relay)


@pytest.mark.asyncio
async def test_a_started_message_cannot_be_cancelled(db, tmp_path):
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    first = asyncio.create_task(engine.run(SID, "first", actor=alice))
    await client.wait_turns(1)
    second = asyncio.create_task(engine.run(SID, "second", actor=alice))
    await _wait_queued(engine, 1)
    (view,) = engine.queue_view(SID)
    try:
        client.release()
        await client.wait_turns(2)
        with pytest.raises(AlreadyStarted) as started:
            engine.cancel_queued_turn(SID, view["queue_id"], actor=alice)
        assert started.value.status == 409
        assert not second.done()             # the running turn is untouched
    finally:
        client.release()
        await _finish(first, second)


@pytest.mark.asyncio
async def test_a_repeated_client_msg_id_runs_once(db, tmp_path):
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    async with _listening() as seen:
        first = asyncio.create_task(engine.run(SID, "hello", actor=alice, client_msg_id="m1"))
        await client.wait_turns(1)
        try:
            # The client retried after its socket dropped.
            assert await engine.run(SID, "hello", actor=alice, client_msg_id="m1") == ""
            assert _queue_updates(seen)[-1]["removed"] == [{
                "queue_id": None, "client_msg_id": "m1", "reason": "duplicate",
            }]
            assert engine.queue_view(SID) == []
            client.release()
            await asyncio.wait_for(first, timeout=5)
            assert client.prompts == ["hello"]
            assert await _user_messages(db) == ["hello"]
        finally:
            client.release()
            await _finish(first)


@pytest.mark.asyncio
async def test_internal_entries_never_show_their_prompt(db, tmp_path):
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    first = asyncio.create_task(engine.run(SID, "first", actor=alice))
    await client.wait_turns(1)
    tasks = [
        asyncio.create_task(engine.run(SID, "secret wakeup prompt", source="wakeup", internal=True, actor=None)),
        asyncio.create_task(engine.run(SID, "secret cron prompt", source="cron", internal=True, actor=None)),
        asyncio.create_task(engine.run(SID, "secret trigger", internal=True, actor=None)),
    ]
    await _wait_queued(engine, 3)
    try:
        previews = [(v["preview"], v["internal"]) for v in engine.queue_view(SID)]
        assert previews == [
            ("Scheduled wakeup", True), ("Scheduled run", True), ("Background trigger", True),
        ]
    finally:
        await _drain(client, first, *tasks)


@pytest.mark.asyncio
async def test_stop_ends_the_turn_and_the_queued_message_runs_next(db, tmp_path):
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    first = asyncio.create_task(engine.run(SID, "first", actor=alice, client_msg_id="m1"))
    await client.wait_turns(1)
    queued = asyncio.create_task(engine.run(SID, "next", actor=alice, client_msg_id="m2"))
    await _wait_queued(engine, 1)
    try:
        result = await engine.stop_turn(SID)
        assert result["turn"] == "stopped"
        await asyncio.wait_for(first, timeout=5)
        await client.wait_turns(2)
        assert client.prompts == ["first", "next"]
        assert not queued.cancelled()
        assert engine.sessions.turn_task(SID) is queued
    finally:
        client.release()
        await _finish(first, queued)


@pytest.mark.asyncio
async def test_a_relay_holding_the_lock_is_the_stop_target(db, tmp_path):
    """A turn started by ``POST /api/chat`` (awaited in its handler, never
    registered) is what Stop reaches, not a WebSocket message waiting
    behind it — even one registered the legacy way."""
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)

    async def _relay():
        return await engine.run(SID, "relay", actor=alice, origin="api")

    relay = asyncio.create_task(_relay())
    await client.wait_turns(1)
    ws_message = asyncio.create_task(engine.run(SID, "from the web", actor=alice, origin="web"))
    engine.register_task(SID, ws_message)
    await _wait_queued(engine, 1)
    try:
        assert engine.sessions.turn_task(SID) is relay
        result = await engine.stop_turn(SID)
        assert result["turn"] == "stopped"
        assert await asyncio.wait_for(relay, timeout=5) == ""
        await client.wait_turns(2)
        assert client.prompts == ["relay", "from the web"]
        assert not ws_message.cancelled()
    finally:
        client.release()
        await _finish(relay, ws_message)


@pytest.mark.asyncio
async def test_stop_all_leaves_queued_messages(db, tmp_path):
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    first = asyncio.create_task(engine.run(SID, "first", actor=alice))
    await client.wait_turns(1)
    queued = asyncio.create_task(engine.run(SID, "next", actor=alice))
    await _wait_queued(engine, 1)
    try:
        result = await engine.stop_all(SID)
        assert result["turn"] == "stopped"
        await client.wait_turns(2)
        assert client.prompts == ["first", "next"]
    finally:
        client.release()
        await _finish(first, queued)


@pytest.mark.asyncio
async def test_the_handover_instant_is_safe(db, tmp_path):
    """The lock goes to the first queued run before that run resumes. In
    that instant a new run waits behind it, listed; a reorder or a cancel
    request cannot reach it any more."""
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    lock = engine._session_locks.setdefault(SID, asyncio.Lock())
    await lock.acquire()                          # a turn holds the lock
    async with _listening() as seen:
        b = asyncio.create_task(engine.run(SID, "b", actor=alice, client_msg_id="mb"))
        c = asyncio.create_task(engine.run(SID, "c", actor=alice, client_msg_id="mc"))
        await _wait_queued(engine, 2)
        b_id, c_id = (v["queue_id"] for v in engine.queue_view(SID))
        engine._pass_run_lock(SID, lock)          # the turn ends: b gets the lock
        late = asyncio.create_task(engine.run(SID, "late", actor=alice, client_msg_id="ml"))
        try:
            assert lock.locked()
            assert [v["queue_id"] for v in await engine.reorder_queue(SID, [c_id, b_id])] == [c_id]
            with pytest.raises(AlreadyStarted):
                engine.cancel_queued_turn(SID, b_id, actor=alice)
            await client.wait_turns(1)
            listed = {
                e["client_msg_id"] for u in _queue_updates(seen) for e in u["queue"]
            }
            assert listed == {"mb", "mc", "ml"}
            await _drain(client, b, c, late)
            assert client.prompts == ["b", "c", "late"]
        finally:
            await _drain(client, b, c, late)


async def _queue_three(engine: AgentEngine, client: _Client, actor) -> tuple[asyncio.Task, list[asyncio.Task]]:
    """A running turn "first" and the runs "b", "c", "d" queued behind it."""
    first = asyncio.create_task(engine.run(SID, "first", actor=actor))
    await client.wait_turns(1)
    queued = []
    for text in ("b", "c", "d"):
        queued.append(asyncio.create_task(engine.run(SID, text, actor=actor)))
        await _wait_queued(engine, len(queued))
    return first, queued


def _ids(view: list[dict]) -> list[str]:
    return [v["queue_id"] for v in view]


@pytest.mark.asyncio
async def test_the_new_order_is_the_run_order(db, tmp_path):
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    async with _listening() as seen:
        first, queued = await _queue_three(engine, client, alice)
        try:
            b, c, d = _ids(engine.queue_view(SID))
            view = await engine.reorder_queue(SID, [d, b, c])
            assert _ids(view) == [d, b, c]
            assert engine.queue_view(SID) == view
            # Every client gets the new order.
            assert _queue_updates(seen)[-1] == {
                "type": "queue_update", "session_id": SID, "queue": view,
            }
            await _drain(client, first, *queued)
            assert client.prompts == ["first", "d", "b", "c"]
            assert await _user_messages(db) == ["first", "d", "b", "c"]
        finally:
            await _drain(client, first, *queued)


@pytest.mark.asyncio
async def test_two_quick_sends_keep_their_order(db, tmp_path):
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    # Both in the same loop step: on an idle session, then behind a turn.
    tasks = [asyncio.create_task(engine.run(SID, t, actor=alice)) for t in ("a", "b")]
    await client.wait_turns(1)
    tasks += [asyncio.create_task(engine.run(SID, t, actor=alice)) for t in ("c", "d")]
    await _wait_queued(engine, 3)
    try:
        assert [v["preview"] for v in engine.queue_view(SID)] == ["b", "c", "d"]
        await _drain(client, *tasks)
        assert client.prompts == ["a", "b", "c", "d"]
    finally:
        await _drain(client, *tasks)


@pytest.mark.asyncio
async def test_a_reorder_moves_only_what_is_still_queued(db, tmp_path):
    """Ids that left the queue are skipped; entries the list does not name
    keep their order after the named ones."""
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    first, queued = await _queue_three(engine, client, alice)
    try:
        b, c, d = _ids(engine.queue_view(SID))
        assert _ids(await engine.reorder_queue(SID, ["q_00000000", d, d])) == [d, b, c]
        assert _ids(await engine.reorder_queue(SID, [])) == [d, b, c]
    finally:
        await _drain(client, first, *queued)


@pytest.mark.asyncio
async def test_a_reorder_racing_the_next_start_is_safe(db, tmp_path):
    """The client still shows "b" queued when "b" has already started."""
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    first, (b_task, c_task, d_task) = await _queue_three(engine, client, alice)
    try:
        b, c, d = _ids(engine.queue_view(SID))
        client.release()
        await client.wait_turns(2)
        assert _ids(await engine.reorder_queue(SID, [d, c, b])) == [d, c]
        assert engine.sessions.turn_task(SID) is b_task
        await _drain(client, first, b_task, c_task, d_task)
        assert client.prompts == ["first", "b", "d", "c"]
    finally:
        await _drain(client, first, b_task, c_task, d_task)


@pytest.mark.asyncio
async def test_cancel_still_works_after_a_reorder(db, tmp_path):
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    first, (b_task, c_task, d_task) = await _queue_three(engine, client, alice)
    try:
        b, c, d = _ids(engine.queue_view(SID))
        await engine.reorder_queue(SID, [d, c, b])
        engine.cancel_queued_turn(SID, c, actor=alice)
        assert await asyncio.wait_for(c_task, timeout=5) == ""
        assert _ids(engine.queue_view(SID)) == [d, b]
        await _drain(client, first, b_task, d_task)
        assert client.prompts == ["first", "d", "b"]
        assert await _user_messages(db) == ["first", "d", "b"]
    finally:
        await _drain(client, first, b_task, c_task, d_task)


@pytest.mark.asyncio
async def test_stop_after_a_reorder_runs_the_new_first_next(db, tmp_path):
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    first, (b_task, c_task, d_task) = await _queue_three(engine, client, alice)
    try:
        b, c, d = _ids(engine.queue_view(SID))
        await engine.reorder_queue(SID, [c, b, d])
        assert (await engine.stop_turn(SID))["turn"] == "stopped"
        await asyncio.wait_for(first, timeout=5)
        await client.wait_turns(2)
        assert client.prompts == ["first", "c"]
        assert engine.sessions.turn_task(SID) is c_task
        assert _ids(engine.queue_view(SID)) == [b, d]
    finally:
        await _drain(client, first, b_task, c_task, d_task)


@pytest.mark.asyncio
async def test_a_run_cancelled_as_it_gets_the_lock_passes_it_on(db, tmp_path):
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    lock = engine._session_locks.setdefault(SID, asyncio.Lock())
    await lock.acquire()
    b = asyncio.create_task(engine.run(SID, "b", actor=alice))
    c = asyncio.create_task(engine.run(SID, "c", actor=alice))
    await _wait_queued(engine, 2)
    try:
        engine._pass_run_lock(SID, lock)          # b gets the lock ...
        b.cancel()                                # ... and is cancelled before it resumes
        with pytest.raises(asyncio.CancelledError):
            await b
        await client.wait_turns(1)
        assert client.prompts == ["c"]
    finally:
        await _drain(client, b, c)


@pytest.mark.asyncio
async def test_a_run_cancelled_while_it_waits_never_gets_the_lock(db, tmp_path):
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    lock = engine._session_locks.setdefault(SID, asyncio.Lock())
    await lock.acquire()
    b = asyncio.create_task(engine.run(SID, "b", actor=alice))
    c = asyncio.create_task(engine.run(SID, "c", actor=alice))
    await _wait_queued(engine, 2)
    try:
        b.cancel()                                # b's run has not cleaned up yet ...
        engine._pass_run_lock(SID, lock)          # ... when the turn ends
        with pytest.raises(asyncio.CancelledError):
            await b
        await client.wait_turns(1)
        assert client.prompts == ["c"]
    finally:
        await _drain(client, b, c)


@pytest.mark.asyncio
async def test_work_that_holds_the_lock_between_turns_passes_it_to_the_queue(db, tmp_path):
    """An idle drain or a client reap holds the run lock; a run queued
    behind it starts when it ends."""
    alice = db.system_actor
    client = _Client()
    engine = await _engine(db, tmp_path, client)
    lock = engine._session_locks.setdefault(SID, asyncio.Lock())
    async with engine._holding_run_lock(SID, lock):
        queued = asyncio.create_task(engine.run(SID, "b", actor=alice))
        await _wait_queued(engine, 1)
    try:
        await client.wait_turns(1)
        assert client.prompts == ["b"]
    finally:
        await _drain(client, queued)
