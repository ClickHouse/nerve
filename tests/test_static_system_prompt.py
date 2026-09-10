"""Static system prompt + per-session preamble, end to end in the engine.

Anthropic prompt caching is exact-prefix, so the appended system prompt is
shared between sessions only when it is byte-identical. The per-session
parts — session id, pre-recalled memories — therefore travel in a
``<session-context>`` block at the top of the FIRST user message of each
native conversation. These tests drive the real ``_get_or_create_client``
and ``_run_inner`` with a stub backend and pin that delivery contract.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

from nerve.agent.backends.base import BackendCapabilities, TransportDiedError
from nerve.agent.backends.claude import translate_message
from nerve.agent.engine import AgentEngine
from nerve.agent.prompts import SESSION_CONTEXT_TAG
from nerve.config import NerveConfig

OPEN = f"<{SESSION_CONTEXT_TAG}>"
CLOSE = f"</{SESSION_CONTEXT_TAG}>"


class _RecordingClient:
    """AgentClient stub: records every ``start_turn`` text; each turn answers
    one text block and a result carrying this client's native id."""

    def __init__(self, native_id: str, die_on_start: bool = False):
        self._native_id = native_id
        self._die_on_start = die_on_start
        self.model = "claude-test"
        self.turns: list[str] = []

    @property
    def native_session_id(self) -> str:
        return self._native_id

    async def connect(self) -> None:
        pass

    async def start_turn(self, turn) -> None:
        if self._die_on_start:
            self._die_on_start = False
            raise TransportDiedError("runtime died in the query phase")
        self.turns.append(turn.text)

    async def receive_turn(self):
        msgs = [
            AssistantMessage(content=[TextBlock(text="ok")], model="claude-test"),
            ResultMessage(
                subtype="success", duration_ms=1, duration_api_ms=1,
                is_error=False, num_turns=1, session_id=self._native_id,
                total_cost_usd=0.01, usage={"input_tokens": 1},
            ),
        ]
        for msg in msgs:
            for event in translate_message(msg):
                yield event

    async def interrupt(self) -> None:
        pass

    def is_alive(self) -> bool:
        return True

    async def disconnect(self, timeout: float = 5.0) -> None:
        pass

    def try_receive_idle_events(self):
        return None

    def buffer_used(self) -> int:
        return 0


class _StubBackend:
    name = "claude"
    capabilities = BackendCapabilities(
        supports_idle_stream=False, supports_cache_ttl=False,
    )

    def __init__(self):
        self.specs = []        # SessionSpec per client build
        self.clients = []      # _RecordingClient per client build
        self.die_on_first_start_turn = False

    def default_model(self, source):
        return "claude-test"

    def excluded_tools(self):
        return set()

    def validate_resume_target(self, native_id, cwd):
        return True

    async def create_client(self, spec):
        self.specs.append(spec)
        client = _RecordingClient(
            f"native-{len(self.clients)}",
            die_on_start=self.die_on_first_start_turn and not self.clients,
        )
        self.clients.append(client)
        return client


def _engine(tmp_path, db, **agent_overrides) -> tuple[AgentEngine, _StubBackend]:
    ws = tmp_path / "ws"
    ws.mkdir(exist_ok=True)
    cfg = NerveConfig.from_dict({
        "workspace": str(ws),
        "agent": {"backend": "claude", **agent_overrides},
    })
    engine = AgentEngine(cfg, db)
    backend = _StubBackend()
    engine._backends["claude"] = backend
    return engine, backend


def _bridge(memories: list[str]) -> MagicMock:
    bridge = MagicMock(available=True)
    bridge.recall = AsyncMock(return_value=[{"summary": m} for m in memories])
    return bridge


@pytest.mark.asyncio
async def test_system_prompt_identical_across_sessions_with_different_recall(
    tmp_path, db,
):
    """Two sessions, different ids, different frozen recall → the same
    system-prompt bytes; the per-session parts are in each first message."""
    engine, backend = _engine(tmp_path, db)
    first = ["alpha prior 1a", "alpha prior 1b"]
    second = ["beta prior 2a"]

    engine._memory_bridge = _bridge(first)
    await engine.run("s-one", "hello", source="web", channel="web")
    engine._memory_bridge = _bridge(second)
    await engine.run("s-two", "hello", source="web", channel="web")

    prompt_one, prompt_two = (s.system_prompt for s in backend.specs)
    assert prompt_one == prompt_two
    for leaked in ("s-one", "s-two", *first, *second):
        assert leaked not in prompt_one, leaked

    turn_one = backend.clients[0].turns[0]
    turn_two = backend.clients[1].turns[0]
    assert "- **Session ID:** s-one" in turn_one
    assert all(f"- {m}" in turn_one for m in first)
    assert not any(m in turn_one for m in second)
    assert "- **Session ID:** s-two" in turn_two
    assert all(f"- {m}" in turn_two for m in second)


@pytest.mark.asyncio
async def test_first_turn_carries_preamble_once_and_later_turns_do_not(
    tmp_path, db,
):
    engine, backend = _engine(tmp_path, db)
    sid = "s-first"
    await engine.run(sid, "hello", source="web", channel="web")
    await engine.run(sid, "and again", source="web", channel="web")

    (client,) = backend.clients
    first, second = client.turns
    # Exactly one block, leading, ahead of the user's text; the trailing
    # per-turn time reminder still follows the user text.
    assert first.startswith(OPEN)
    assert first.count(OPEN) == 1 and first.count(CLOSE) == 1
    assert first.index(CLOSE) < first.index("hello")
    assert f"- **Session ID:** {sid}" in first
    assert "- **Source:** web" in first
    assert first.index("hello") < first.index("<system-reminder>Current time:")
    # Later turns on the same conversation do not repeat it.
    assert OPEN not in second and sid not in second
    assert second.startswith("and again")

    # The shared system prompt carries no session id ...
    assert sid not in backend.specs[0].system_prompt
    # ... and the persisted user messages stay clean (UI text).
    rows = await db.get_messages(sid)
    assert [r["content"] for r in rows if r["role"] == "user"] == [
        "hello", "and again",
    ]
    # Delivery marker and native id persisted; nothing left pending.
    row = await db.get_session(sid)
    assert json.loads(row["metadata"] or "{}").get("preamble_sent") is True
    assert row["sdk_session_id"] == "native-0"
    assert sid not in engine._pending_preambles

    # A client rebuild that RESUMES the transcript must not repeat it.
    engine.sessions.remove_client(sid)
    await engine.run(sid, "third", source="web", channel="web")
    assert len(backend.clients) == 2
    assert backend.specs[1].resume_native_id == "native-0"
    (third,) = backend.clients[1].turns
    assert OPEN not in third and sid not in third


@pytest.mark.asyncio
async def test_pre_existing_resumable_session_gets_preamble_once(tmp_path, db):
    """A session created before static mode (resumable, no marker) had its
    id in the system prompt; it must receive the block once, then never."""
    engine, backend = _engine(tmp_path, db)
    sid = "s-legacy"
    await db.create_session(sid, source="web", backend="claude")
    await db.update_session_fields(sid, {"sdk_session_id": "native-old"})

    await engine.run(sid, "resume me", source="web", channel="web")
    assert backend.specs[0].resume_native_id == "native-old"
    (first,) = backend.clients[0].turns
    assert first.startswith(OPEN)
    assert f"- **Session ID:** {sid}" in first

    engine.sessions.remove_client(sid)
    await engine.run(sid, "once more", source="web", channel="web")
    (again,) = backend.clients[1].turns
    assert OPEN not in again and sid not in again


@pytest.mark.asyncio
async def test_crash_retry_on_first_turn_sends_exactly_one_block(tmp_path, db):
    """The query-phase crash retry rebuilds the client on a fresh
    transcript; the retried first message carries one block, not two."""
    engine, backend = _engine(tmp_path, db)
    backend.die_on_first_start_turn = True
    sid = "s-retry"
    out = await engine.run(sid, "hello", source="web", channel="web")
    assert out == "ok"
    assert len(backend.clients) == 2
    assert backend.clients[0].turns == []
    (retried,) = backend.clients[1].turns
    assert retried.startswith(OPEN)
    assert retried.count(OPEN) == 1
    assert f"- **Session ID:** {sid}" in retried
    assert sid not in engine._pending_preambles


@pytest.mark.asyncio
async def test_flag_off_restores_inline_session_context(tmp_path, db):
    """``agent.static_system_prompt: false`` → legacy shape: id and recall
    in the system prompt, no block in the first message."""
    engine, backend = _engine(tmp_path, db, static_system_prompt=False)
    engine._memory_bridge = _bridge(["legacy prior 77"])
    sid = "s-inline"
    await engine.run(sid, "hello", source="web", channel="web")

    prompt = backend.specs[0].system_prompt
    assert f"- **Session ID:** {sid}" in prompt
    assert "# Recalled Memories\n\n- legacy prior 77" in prompt
    (first,) = backend.clients[0].turns
    assert OPEN not in first
    assert first.startswith("hello")
    assert engine._pending_preambles == {}
    meta = json.loads((await db.get_session(sid))["metadata"] or "{}")
    assert "preamble_sent" not in meta
