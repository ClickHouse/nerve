"""A fork branches from the source stored on its row until its first
completed turn (issue #673).

``SessionManager.fork_session`` stores the parent's native id and the fork
turn on the fork row. Every client build of the fork reads them, so the
fork branches from the same point whatever happens before its first
completed turn: a crash-retry, a Stop, a status change, or a parent whose
own conversation pointer is gone. The fork row never takes the parent's
native id as its own.

The backend here is a fake that records each ``SessionSpec``. Its clients
report native ids as the real ones do: a Claude fork has no id until the
CLI sends one, a Codex fork has its new thread id at once.
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest
from claude_agent_sdk import ClaudeAgentOptions

from nerve.agent.backends.base import (
    BackendCapabilities,
    ForkSourceMissingError,
    SessionSpec,
    TransportDiedError,
)
from nerve.agent.backends.claude import ClaudeClient
from nerve.agent.backends.events import TextDelta, TurnCompleted
from nerve.agent.engine import AgentEngine
from nerve.config import NerveConfig

PARENT_NATIVE = "sdk-parent"
FORK_OWN = "sdk-fork-own"


class _Client:
    def __init__(self, spec: SessionSpec, mode: str, *, codex: bool, n: int):
        self.spec = spec
        self.mode = mode
        self.model = "claude-test"
        self.n = n
        self.entered = asyncio.Event()
        self.resume_dropped = False
        if spec.fork:
            # Codex: thread/fork returns the new thread. Claude: no id
            # until the CLI's first message.
            self._native = f"thread-fork-{n}" if codex else None
        else:
            self._native = spec.resume_native_id

    @property
    def native_session_id(self):
        return self._native

    async def connect(self):
        pass

    async def start_turn(self, turn):
        pass

    def _own_id(self) -> str:
        if self.spec.fork:
            return self._native or FORK_OWN
        return self.spec.resume_native_id or f"sdk-fresh-{self.n}"

    async def receive_turn(self):
        self.entered.set()
        if self.mode == "crash":
            raise TransportDiedError("the runtime ended the turn without a result")
        if self.mode == "hang":
            await asyncio.Event().wait()
        # The CLI's first message brings the fork's own id.
        self._native = self._own_id()
        yield TextDelta(text=f"reply {self.n}")
        if self.mode == "content-hang":
            await asyncio.Event().wait()
        if self.mode == "content-crash":
            raise TransportDiedError("the runtime died after the first message")
        yield TurnCompleted(
            native_session_id=self._native, native_turn_id=f"turn-{self.n}",
            total_cost_usd=0.0,
        )

    async def interrupt(self, **kw):
        pass

    def is_alive(self):
        return True

    async def disconnect(self, timeout: float = 5.0):
        pass

    def try_receive_idle_events(self):
        return None

    def buffer_used(self):
        return 0


class _Backend:
    name = "claude"

    def __init__(self, modes, *, codex: bool = False):
        self.capabilities = BackendCapabilities(
            supports_idle_stream=False, supports_cache_ttl=False,
        )
        self.modes = list(modes)
        self.codex = codex
        self.specs: list[SessionSpec] = []
        self.clients: list[_Client] = []
        # Native ids whose conversation is gone on this host.
        self.missing: set[str] = set()
        # Fork sources the runtime itself cannot find (Codex thread/read).
        self.refuse_fork_of: set[str] = set()
        # Resume targets that the runtime finds missing only when it
        # connects (Codex): it starts a fresh thread and says so.
        self.drop_resume_of: set[str] = set()

    def default_model(self, source):
        return "claude-test"

    def excluded_tools(self):
        return set()

    def validate_resume_target(self, native_id, cwd):
        return native_id not in self.missing

    async def create_client(self, spec):
        self.specs.append(spec)
        if spec.fork and spec.resume_native_id in self.refuse_fork_of:
            raise ForkSourceMissingError(
                f"Cannot fork missing Codex thread {spec.resume_native_id!r}",
            )
        mode = self.modes.pop(0) if self.modes else "ok"
        if not spec.fork and spec.resume_native_id in self.drop_resume_of:
            spec = SessionSpec(**{**vars(spec), "resume_native_id": None})
            client = _Client(spec, mode, codex=self.codex, n=len(self.clients))
            client.resume_dropped = True
        else:
            client = _Client(spec, mode, codex=self.codex, n=len(self.clients))
        self.clients.append(client)
        return client


async def _setup(tmp_path, db, modes, *, codex: bool = False, at_turn: bool = False):
    """An engine with a parent that has one completed turn, and a fork of it
    made by ``fork_session``."""
    ws = tmp_path / "ws"
    ws.mkdir(exist_ok=True)
    cfg = NerveConfig.from_dict({"workspace": str(ws), "agent": {"backend": "claude"}})
    engine = AgentEngine(cfg, db)
    backend = _Backend(modes, codex=codex)
    engine._backends["claude"] = backend
    await engine.sessions.get_or_create(
        "parent", title="p", source="web", backend="claude", actor=None,
    )
    await db.update_session_fields("parent", {"sdk_session_id": PARENT_NATIVE})
    first = await db.add_message("parent", "user", "first question", actor=None)
    await db.add_message(
        "parent", "assistant", "first answer", native_turn_id="p-turn-1", actor=None,
    )
    await db.add_message("parent", "user", "second question", actor=None)
    await db.add_message(
        "parent", "assistant", "second answer", native_turn_id="p-turn-2", actor=None,
    )
    fork = await engine.fork_session(
        "parent", str(first) if at_turn else None, None, actor=None,
    )
    return engine, backend, fork["id"]


async def _drop_client(engine: AgentEngine, session_id: str) -> None:
    """End the live client, as an idle reap does: the next turn builds anew."""
    client = engine.sessions.remove_client(session_id, reason="idle_timeout")
    if client is not None:
        await client.disconnect()


async def _run(engine, session_id, text="hello"):
    return await engine.run(session_id, text, source="web", channel="web", actor=None)


async def _stop_when_entered(engine, backend, session_id, client_index):
    task = asyncio.create_task(_run(engine, session_id, "first message"))
    for _ in range(300):
        if len(backend.clients) > client_index and backend.clients[client_index].entered.is_set():
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("the turn did not start")
    await asyncio.sleep(0.05)
    task.cancel()
    with contextlib.suppress(BaseException):  # run() returns after a cancel
        await task


# --------------------------------------------------------------------------- #
#  fork_session and the migration                                              #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_fork_session_stores_the_source_and_the_fork_turn(tmp_path, db):
    engine, _backend, fork_id = await _setup(tmp_path, db, [], at_turn=True)
    row = await db.get_session(fork_id)
    assert row["fork_source_native_id"] == PARENT_NATIVE
    # The user message maps to the turn that answers it, which also bounds
    # the copied messages.
    assert row["fork_source_turn_id"] == "p-turn-1"
    assert row["sdk_session_id"] is None
    contents = [m["content"] for m in await db.get_messages(fork_id)]
    assert contents == ["first question", "first answer"]

    # A whole fork branches at the parent's last completed turn: a turn that
    # the parent completes later is neither copied nor in the native fork.
    whole = await engine.fork_session("parent", None, None, actor=None)
    row = await db.get_session(whole["id"])
    assert row["fork_source_native_id"] == PARENT_NATIVE
    assert row["fork_source_turn_id"] == "p-turn-2"
    await db.add_message("parent", "user", "later question", actor=None)
    await db.add_message(
        "parent", "assistant", "later answer", native_turn_id="p-turn-3", actor=None,
    )
    await _run(engine, whole["id"])
    spec = engine._backends["claude"].specs[-1]
    assert (spec.fork, spec.resume_native_id, spec.fork_last_turn_id) == (
        True, PARENT_NATIVE, "p-turn-2",
    )
    contents = [m["content"] for m in await db.get_messages(whole["id"])]
    assert "later question" not in contents and "later answer" not in contents


@pytest.mark.asyncio
async def test_a_whole_fork_of_history_without_turn_mapping_takes_everything(tmp_path, db):
    engine, _backend, _fork_id = await _setup(tmp_path, db, [])
    await engine.sessions.get_or_create(
        "old", title="o", source="web", backend="claude", actor=None,
    )
    await db.update_session_fields("old", {"sdk_session_id": "sdk-old"})
    await db.add_message("old", "user", "q", actor=None)
    await db.add_message("old", "assistant", "a", actor=None)
    fork = await engine.fork_session("old", None, None, actor=None)
    row = await db.get_session(fork["id"])
    assert (row["fork_source_native_id"], row["fork_source_turn_id"]) == ("sdk-old", None)
    assert fork["message_count"] == 2


async def _started(db, session_id: str, native_id: str | None) -> None:
    await db.log_session_event(session_id, "started", {"sdk_session_id": native_id})


@pytest.mark.asyncio
async def test_v072_gives_a_source_to_forks_that_never_had_their_own_conversation(db):
    from nerve.db.migrations.v072_session_fork_source import up

    await db.create_session("parent", actor=None)
    await db.update_session_fields("parent", {"sdk_session_id": PARENT_NATIVE})
    await _started(db, "parent", PARENT_NATIVE)
    anchor = await db.add_message("parent", "user", "q", actor=None)
    await db.add_message("parent", "assistant", "a", native_turn_id="p-turn-1", actor=None)

    async def fork(fork_id: str, parent: str = "parent", **kw) -> None:
        await db.create_session(fork_id, parent_session_id=parent, actor=None, **kw)
        await db.log_session_event(fork_id, "created", {"parent": parent})

    # Not run yet, whole and at a message.
    await fork("fork-whole")
    await fork("fork-at", forked_from_message=str(anchor))
    # Archived and unarchived before its first turn: idle, no own id.
    await fork("fork-unarchived")
    await db.update_session_fields("fork-unarchived", {"status": "idle"})
    # A first turn that did not complete bound it to the parent's id.
    await fork("fork-bound")
    await _started(db, "fork-bound", PARENT_NATIVE)
    await db.update_session_fields(
        "fork-bound", {"sdk_session_id": PARENT_NATIVE, "status": "idle"},
    )
    # Its parent was archived (native id cleared) after the fork.
    await db.create_session("archived-parent", actor=None)
    await _started(db, "archived-parent", "sdk-old-parent")
    await fork("fork-of-archived", parent="archived-parent")
    await _started(db, "archived-parent", "sdk-after-the-fork")
    # Completed a turn in its own conversation, with or without the id now.
    await fork("fork-ran")
    await _started(db, "fork-ran", PARENT_NATIVE)
    await _started(db, "fork-ran", "sdk-ran")
    await db.update_session_fields("fork-ran", {"sdk_session_id": "sdk-ran", "status": "idle"})
    await fork("fork-ran-then-archived")
    await _started(db, "fork-ran-then-archived", PARENT_NATIVE)
    await _started(db, "fork-ran-then-archived", "sdk-ran-2")
    # Its first build did not fork (status had changed): a fresh conversation.
    await fork("fork-fresh")
    await _started(db, "fork-fresh", None)
    await _started(db, "fork-fresh", "sdk-fresh")
    # Not forks: a nested session and a workflow leg.
    await db.create_session("nested", parent_session_id="parent", actor=None)
    await db.create_session(
        "workflow:wfr-x", source="workflow", parent_session_id="parent", actor=None,
    )
    # An interrupted run: only the first column was added.
    await db.db.execute("ALTER TABLE sessions DROP COLUMN fork_source_native_id")
    await db.db.execute("ALTER TABLE sessions DROP COLUMN fork_source_turn_id")
    await db.db.execute("ALTER TABLE sessions ADD COLUMN fork_source_native_id TEXT")

    await up(db.db)
    await up(db.db)

    expected = {
        "fork-whole": (PARENT_NATIVE, None, None),
        "fork-at": (PARENT_NATIVE, "p-turn-1", None),
        "fork-unarchived": (PARENT_NATIVE, None, None),
        "fork-bound": (PARENT_NATIVE, None, None),
        "fork-of-archived": ("sdk-old-parent", None, None),
        "fork-ran": (None, None, "sdk-ran"),
        "fork-ran-then-archived": (None, None, None),
        "fork-fresh": (None, None, None),
        "nested": (None, None, None),
        "workflow:wfr-x": (None, None, None),
        "parent": (None, None, PARENT_NATIVE),
    }
    for session_id, want in expected.items():
        row = await db.get_session(session_id)
        got = (row["fork_source_native_id"], row["fork_source_turn_id"], row["sdk_session_id"])
        assert got == want, session_id


# --------------------------------------------------------------------------- #
#  The first turn                                                              #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_first_turn_forks_and_then_the_fork_resumes_its_own_conversation(tmp_path, db):
    engine, backend, fork_id = await _setup(tmp_path, db, ["ok", "ok"], at_turn=True)

    await _run(engine, fork_id)
    first = backend.specs[0]
    assert (first.fork, first.resume_native_id, first.fork_last_turn_id) == (
        True, PARENT_NATIVE, "p-turn-1",
    )
    row = await db.get_session(fork_id)
    assert row["sdk_session_id"] == FORK_OWN
    assert row["fork_source_native_id"] is None
    assert row["fork_source_turn_id"] is None

    await _drop_client(engine, fork_id)
    await _run(engine, fork_id, "second message")
    second = backend.specs[1]
    assert (second.fork, second.resume_native_id, second.fork_last_turn_id) == (
        False, FORK_OWN, None,
    )


@pytest.mark.asyncio
async def test_claude_fork_client_has_no_native_id_before_the_cli_sends_one():
    def _client(fork: bool) -> ClaudeClient:
        spec = SessionSpec(
            session_id="s", source="web", model="claude-test", effort="high",
            system_prompt="x", cwd="/tmp", resume_native_id=PARENT_NATIVE, fork=fork,
        )
        return ClaudeClient(spec, ClaudeAgentOptions(model="claude-test"))

    assert _client(fork=True).native_session_id is None
    assert _client(fork=False).native_session_id == PARENT_NATIVE


@pytest.mark.asyncio
async def test_a_crash_retry_forks_again_and_never_stores_the_parent_id(tmp_path, db):
    engine, backend, fork_id = await _setup(tmp_path, db, ["crash", "ok"])

    await _run(engine, fork_id)
    first, retry = backend.specs
    assert (first.fork, first.resume_native_id) == (True, PARENT_NATIVE)
    assert (retry.fork, retry.resume_native_id) == (True, PARENT_NATIVE)
    assert (await db.get_session(fork_id))["sdk_session_id"] == FORK_OWN


@pytest.mark.asyncio
async def test_a_first_turn_that_fails_keeps_the_fork_source(tmp_path, db):
    engine, backend, fork_id = await _setup(tmp_path, db, ["crash", "crash", "ok"])

    out = await _run(engine, fork_id)
    assert out.startswith("Agent error:")
    row = await db.get_session(fork_id)
    assert row["sdk_session_id"] is None
    assert row["fork_source_native_id"] == PARENT_NATIVE

    await _run(engine, fork_id, "again")
    assert (backend.specs[2].fork, backend.specs[2].resume_native_id) == (True, PARENT_NATIVE)


@pytest.mark.asyncio
async def test_an_error_after_the_first_message_keeps_the_fork_source(tmp_path, db):
    """The id that the error path takes from the live client is kept for the
    resume, but only a completed turn ends the fork source."""
    engine, backend, fork_id = await _setup(tmp_path, db, ["content-crash", "ok"])

    out = await _run(engine, fork_id)
    assert out.startswith("Agent error:")
    row = await db.get_session(fork_id)
    assert row["sdk_session_id"] == FORK_OWN
    assert row["fork_source_native_id"] == PARENT_NATIVE

    backend.missing.add(FORK_OWN)
    await _run(engine, fork_id, "again")
    assert (backend.specs[1].fork, backend.specs[1].resume_native_id) == (True, PARENT_NATIVE)


@pytest.mark.asyncio
async def test_a_stop_before_the_first_message_keeps_the_fork_source(tmp_path, db):
    engine, backend, fork_id = await _setup(tmp_path, db, ["hang", "ok"])

    # The client build does not write the parent's id into the fork row.
    task = asyncio.create_task(_run(engine, fork_id, "first message"))
    for _ in range(300):
        if backend.clients and backend.clients[0].entered.is_set():
            break
        await asyncio.sleep(0.01)
    assert (await db.get_session(fork_id))["sdk_session_id"] is None
    task.cancel()
    with contextlib.suppress(BaseException):
        await task

    row = await db.get_session(fork_id)
    assert row["sdk_session_id"] is None
    assert row["fork_source_native_id"] == PARENT_NATIVE

    await _run(engine, fork_id, "second message")
    assert (backend.specs[1].fork, backend.specs[1].resume_native_id) == (True, PARENT_NATIVE)


@pytest.mark.asyncio
async def test_a_stop_after_the_first_message_resumes_the_forks_own_conversation(tmp_path, db):
    engine, backend, fork_id = await _setup(tmp_path, db, ["content-hang", "ok"])

    await _stop_when_entered(engine, backend, fork_id, 0)
    row = await db.get_session(fork_id)
    assert row["sdk_session_id"] == FORK_OWN
    # Kept until a turn completes in the fork's own conversation.
    assert row["fork_source_native_id"] == PARENT_NATIVE

    await _run(engine, fork_id, "go on")
    assert (backend.specs[1].fork, backend.specs[1].resume_native_id) == (False, FORK_OWN)
    assert (await db.get_session(fork_id))["fork_source_native_id"] is None


@pytest.mark.asyncio
async def test_a_missing_own_conversation_forks_again_from_the_source(tmp_path, db):
    engine, backend, fork_id = await _setup(tmp_path, db, ["content-hang", "ok"])
    await _stop_when_entered(engine, backend, fork_id, 0)
    assert (await db.get_session(fork_id))["sdk_session_id"] == FORK_OWN

    # The runtime died before it wrote the fork's transcript.
    backend.missing.add(FORK_OWN)
    await _run(engine, fork_id, "go on")
    assert (backend.specs[1].fork, backend.specs[1].resume_native_id) == (True, PARENT_NATIVE)


@pytest.mark.asyncio
async def test_a_codex_fork_stopped_before_content_resumes_its_new_thread(tmp_path, db):
    engine, backend, fork_id = await _setup(tmp_path, db, ["hang", "ok"], codex=True)

    await _stop_when_entered(engine, backend, fork_id, 0)
    assert (await db.get_session(fork_id))["sdk_session_id"] == "thread-fork-0"

    await _run(engine, fork_id, "go on")
    assert (backend.specs[1].fork, backend.specs[1].resume_native_id) == (False, "thread-fork-0")


@pytest.mark.asyncio
async def test_a_codex_fork_whose_new_thread_is_gone_forks_again(tmp_path, db):
    """Codex finds a missing thread only when it connects, and then starts a
    fresh one: for a fork, the engine branches from the source instead."""
    engine, backend, fork_id = await _setup(tmp_path, db, ["hang", "ok", "ok"], codex=True)
    await _stop_when_entered(engine, backend, fork_id, 0)
    assert (await db.get_session(fork_id))["sdk_session_id"] == "thread-fork-0"

    backend.drop_resume_of.add("thread-fork-0")
    await _run(engine, fork_id, "go on")
    dropped, refork = backend.specs[1:]
    assert (dropped.fork, dropped.resume_native_id) == (False, "thread-fork-0")
    assert (refork.fork, refork.resume_native_id) == (True, PARENT_NATIVE)
    row = await db.get_session(fork_id)
    assert row["sdk_session_id"] == "thread-fork-2"
    assert row["fork_source_native_id"] is None


@pytest.mark.asyncio
async def test_a_row_that_holds_the_source_id_still_forks(tmp_path, db):
    """Defense against a runtime that reports the parent's id for a fork."""
    engine, backend, fork_id = await _setup(tmp_path, db, ["ok"])
    await db.update_session_fields(fork_id, {"sdk_session_id": PARENT_NATIVE})

    await _run(engine, fork_id)
    assert (backend.specs[0].fork, backend.specs[0].resume_native_id) == (True, PARENT_NATIVE)


# --------------------------------------------------------------------------- #
#  Status changes before the first turn                                        #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_an_unarchived_fork_still_forks_from_its_source(tmp_path, db):
    engine, backend, fork_id = await _setup(tmp_path, db, ["ok"])

    # Archiving the parent archives the nested fork and clears the parent's
    # native id. The person brings the fork back.
    result = await engine.sessions.archive_session_cascade("parent")
    assert set(result["archived"]) == {"parent", fork_id}
    assert (await db.get_session("parent"))["sdk_session_id"] is None
    assert await engine.sessions.unarchive_session(fork_id)
    assert (await db.get_session(fork_id))["status"] == "idle"

    await _run(engine, fork_id)
    assert (backend.specs[0].fork, backend.specs[0].resume_native_id) == (True, PARENT_NATIVE)


@pytest.mark.asyncio
async def test_resume_session_does_not_fork_a_fork_again(tmp_path, db):
    engine, backend, fork_id = await _setup(tmp_path, db, ["ok", "ok"])
    await _run(engine, fork_id)
    await _drop_client(engine, fork_id)
    await engine.sessions.mark_stopped(fork_id)

    resumed = await engine.resume_session(fork_id)
    assert resumed["status"] == "created"
    await _run(engine, fork_id, "second message")
    assert (backend.specs[1].fork, backend.specs[1].resume_native_id) == (False, FORK_OWN)


@pytest.mark.asyncio
async def test_resume_session_leaves_a_running_session_active(tmp_path, db):
    engine, _backend, fork_id = await _setup(tmp_path, db, ["ok"])
    await _run(engine, fork_id)
    assert (await db.get_session(fork_id))["status"] == "active"

    resumed = await engine.resume_session(fork_id)
    assert resumed["status"] == "active"


@pytest.mark.asyncio
async def test_only_fork_session_makes_a_fork(tmp_path, db):
    """A workflow leg, or a session nested under another one, has a parent
    but no fork source: its first turn starts a new conversation."""
    engine, backend, _fork_id = await _setup(tmp_path, db, ["ok", "ok"])
    await engine.sessions.get_or_create(
        "workflow:wfr-x", title="t", source="workflow", backend="claude", actor=None,
    )
    await db.update_session_fields("workflow:wfr-x", {"parent_session_id": "parent"})
    await engine.sessions.get_or_create(
        "nested", title="t", source="web", backend="claude", actor=None,
    )
    await db.update_session_fields("nested", {"parent_session_id": "parent"})

    await engine.run("workflow:wfr-x", "go", source="workflow", channel="web", actor=None)
    await _run(engine, "nested", "go")
    for spec in backend.specs:
        assert (spec.fork, spec.resume_native_id) == (False, None)


# --------------------------------------------------------------------------- #
#  A missing source                                                            #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
@pytest.mark.parametrize("found_by", ["engine", "runtime"])
async def test_a_missing_source_fails_once_and_says_so(tmp_path, db, found_by):
    engine, backend, fork_id = await _setup(tmp_path, db, ["ok"])
    if found_by == "engine":
        backend.missing.add(PARENT_NATIVE)  # Claude: no transcript file
    else:
        backend.refuse_fork_of.add(PARENT_NATIVE)  # Codex: thread/read fails

    out = await _run(engine, fork_id)
    assert out.startswith("Agent error:")
    assert "no longer exists" in out
    assert "next message starts a new conversation" in out
    row = await db.get_session(fork_id)
    assert row["fork_source_native_id"] is None
    assert row["fork_source_turn_id"] is None

    await _run(engine, fork_id, "again")
    last = backend.specs[-1]
    assert (last.fork, last.resume_native_id) == (False, None)


@pytest.mark.asyncio
async def test_a_fork_moved_under_another_session_still_forks(tmp_path, db):
    """The parent link is for the sidebar: moving a fork that has not run
    under a session of another backend does not change what it forks."""
    engine, backend, fork_id = await _setup(tmp_path, db, ["ok"])
    await engine.sessions.get_or_create(
        "other", title="o", source="web", backend="codex", actor=None,
    )
    await db.update_session_fields(fork_id, {"parent_session_id": "other"})

    await _run(engine, fork_id)
    assert (backend.specs[0].fork, backend.specs[0].resume_native_id) == (True, PARENT_NATIVE)
