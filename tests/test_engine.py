"""Tests for nerve.agent.engine and nerve.agent.backends.claude — pure
helpers (no SDK subprocess)."""

import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock, UserMessage

from nerve.agent.backends import events as ev
from nerve.agent.backends.base import SessionSpec, TransportDiedError
from nerve.agent.backends.claude import ClaudeBackend, ClaudeClient, translate_message
from nerve.agent.engine import AgentEngine, _TurnState, _model_family
from nerve.config import AgentConfig, NerveConfig


@pytest.mark.parametrize(
    "value, model, expected",
    [
        # Fable 5 (Mythos-class) supports the full effort ladder
        ("max",    "claude-fable-5",            "max"),
        ("xhigh",  "claude-fable-5",            "xhigh"),
        ("low",    "claude-fable-5",            "low"),
        # Opus 5 supports the full ladder (verified via Models API 2026-07-28)
        ("max",    "claude-opus-5",             "max"),
        ("xhigh",  "claude-opus-5",             "xhigh"),
        ("xhigh",  "claude-opus-5-20260720",    "xhigh"),
        ("max",    "us.anthropic.claude-opus-5", "max"),
        # Sonnet 5 supports the full ladder (unlike Sonnet 4.6's high cap)
        ("max",    "claude-sonnet-5",           "max"),
        ("xhigh",  "claude-sonnet-5",           "xhigh"),
        # Opus 4.8 supports every level (same ladder as 4.7)
        ("max",    "claude-opus-4-8",           "max"),
        ("xhigh",  "claude-opus-4-8",           "xhigh"),
        ("high",   "claude-opus-4-8",           "high"),
        # Bedrock dateless ID resolves via substring match
        ("max",    "us.anthropic.claude-opus-4-8", "max"),
        # Opus 4.7 still resolves correctly for legacy configs
        ("max",    "claude-opus-4-7",           "max"),
        ("xhigh",  "claude-opus-4-7",           "xhigh"),
        ("high",   "claude-opus-4-7",           "high"),
        # Dated alias resolves via substring match
        ("max",    "claude-opus-4-7-20260416",  "max"),
        # Opus 4.6: max OK, xhigh caps to high (not registered)
        ("max",    "claude-opus-4-6",           "max"),
        ("xhigh",  "claude-opus-4-6",           "high"),
        # Sonnet 4.6 tops out at high
        ("max",    "claude-sonnet-4-6",         "high"),
        ("xhigh",  "claude-sonnet-4-6",         "high"),
        ("high",   "claude-sonnet-4-6",         "high"),
        ("medium", "claude-sonnet-4-6",         "medium"),
        ("low",    "claude-sonnet-4-6",         "low"),
        # Unknown models (including Haiku which uses budget_tokens, not levels)
        # pass through unchanged — capping is a no-op for non-level-based thinking
        ("max",    "claude-haiku-4-5-20251001", "max"),
        ("max",    "some-future-model",         "max"),
        ("max",    None,                        "max"),
        ("max",    "",                          "max"),
        # Invalid effort string → None (same as the pre-existing behaviour)
        ("invalid", "claude-opus-4-7",          None),
        ("",        "claude-sonnet-4-6",        None),
    ],
)
def test_effective_effort(value, model, expected):
    assert ClaudeBackend._effective_effort(value, model) == expected


def test_effective_effort_model_default_none():
    # Signature symmetry with _parse_thinking_config
    assert ClaudeBackend._effective_effort("max") == "max"


@pytest.mark.parametrize(
    "source, expected",
    [
        # Interactive sources keep the full interactive effort.
        ("web",      "max"),
        ("telegram", "max"),
        ("wakeup",   "max"),
        ("api",      "max"),
        # Cron and hook turns drop to cron_effort.
        ("cron",     "medium"),
        ("hook",     "medium"),
    ],
)
def test_base_effort_for_source(source, expected):
    assert AgentEngine._base_effort_for_source(source, "max", "medium") == expected


def test_base_effort_for_source_then_capped():
    # A cron turn at the default cron_effort stays "medium" after the model-cap
    # pass on Sonnet 4.6 (which tops out at "high", so medium is unaffected).
    base = AgentEngine._base_effort_for_source("cron", "max", "medium")
    assert ClaudeBackend._effective_effort(base, "claude-sonnet-4-6") == "medium"
    # Sonnet 5 is not in the cap table, so cron_effort passes through unchanged.
    assert ClaudeBackend._effective_effort(base, "claude-sonnet-5") == "medium"
    # An interactive turn keeps "max", which caps to "high" on Sonnet 4.6.
    base = AgentEngine._base_effort_for_source("web", "max", "medium")
    assert ClaudeBackend._effective_effort(base, "claude-sonnet-4-6") == "high"
    # A cron turn whose cron_effort is left at "max" still caps to the model max.
    base = AgentEngine._base_effort_for_source("cron", "max", "max")
    assert ClaudeBackend._effective_effort(base, "claude-sonnet-4-6") == "high"


def test_agent_config_cron_effort_default_and_override():
    # Default is medium when unset.
    assert AgentConfig.from_dict({}).cron_effort == "medium"
    # Explicit value is respected, and interactive effort stays independent.
    cfg = AgentConfig.from_dict({"cron_effort": "low"})
    assert cfg.cron_effort == "low"
    assert cfg.effort == "max"


def test_claude_system_prompt_excludes_codex_runbook_policy(tmp_path):
    """Codex-only runbook semantics must never alter Claude's prompt."""
    cfg = NerveConfig.from_dict({"workspace": str(tmp_path)})
    backend = ClaudeBackend(SimpleNamespace(
        config=lambda: cfg,
        claude_plugins=lambda: [],
    ))
    marker = "exact claude system prompt"
    spec = SessionSpec(
        session_id="claude-prompt-isolation",
        source="web",
        model=cfg.agent.model,
        effort="high",
        system_prompt=marker,
        cwd=str(tmp_path),
    )

    with patch.object(backend, "_build_mcp_servers", return_value={}), \
         patch.object(backend, "_build_hooks", return_value={}):
        options = backend._build_options(spec)

    # The prompt travels by file (never argv), so the exactness check is on
    # what landed on disk.
    rendered = Path(options.system_prompt["path"]).read_text(encoding="utf-8")
    assert rendered == marker
    assert "Nerve runbooks" not in rendered
    assert "Codex-native skills" not in rendered


# ---------------------------------------------------------------------------
# ClaudeClient.receive_turn — per-message idle timeout (hung-CLI detection)
# ---------------------------------------------------------------------------


class _StubClient:
    """Minimal SDK-shaped client whose receive_response yields a fixed list.

    If ``hang`` is True, the generator sleeps after yielding all real
    messages instead of exiting cleanly — simulating a CLI that streams
    initial output then goes silent forever.

    Tracks whether ``aclose`` was called on the returned generator so the
    timeout path can assert cleanup.
    """

    def __init__(self, messages, hang=False, hang_seconds=10.0):
        self._messages = messages
        self._hang = hang
        self._hang_seconds = hang_seconds
        self.aclose_calls = 0

    def receive_response(self):
        outer = self

        async def _gen():
            try:
                for msg in outer._messages:
                    yield msg
                if outer._hang:
                    await asyncio.sleep(outer._hang_seconds)
            finally:
                outer.aclose_calls += 1

        return _gen()


def _turn_client(sdk: _StubClient, session_id: str, idle_timeout: float) -> ClaudeClient:
    """A ClaudeClient wired to a stub SDK (no subprocess)."""
    client = ClaudeClient.__new__(ClaudeClient)
    client._sdk = sdk
    client._spec = SessionSpec(
        session_id=session_id, source="web", model="m", effort="high",
        system_prompt="", cwd="/tmp", idle_timeout=idle_timeout,
    )
    client._native_session_id = None
    return client


def _text_msg(text: str) -> AssistantMessage:
    return AssistantMessage(content=[TextBlock(text=text)], model="claude-test")


def _translated(messages: list) -> list:
    """The normalized events the given SDK messages translate into."""
    return [event for m in messages for event in translate_message(m)]


def _result_msg(session_id: str = "sdk-1") -> ResultMessage:
    """A terminal ResultMessage: translates to one TurnCompleted."""
    return ResultMessage(
        subtype="success", duration_ms=1, duration_api_ms=1,
        is_error=False, num_turns=1, session_id=session_id,
        total_cost_usd=0.5, usage={"input_tokens": 1},
    )


@pytest.mark.asyncio
async def test_receive_turn_yields_events_normally():
    """Fast SDK stream completes without timing out.

    The stream ends with a ResultMessage: that terminal event is what
    makes the turn complete, so ``receive_turn`` returns rather than
    reporting the exhausted stream as a transport death.
    """
    messages = [_text_msg("a"), _text_msg("b"), _text_msg("c"), _result_msg()]
    sdk = _StubClient(messages)
    client = _turn_client(sdk, "sess-1", idle_timeout=5.0)
    seen = []
    async for event in client.receive_turn():
        seen.append(event)
    assert seen == _translated(messages)
    # Generator was closed cleanly when it ran to completion.
    assert sdk.aclose_calls == 1


@pytest.mark.asyncio
async def test_receive_turn_raises_on_idle_timeout():
    """If the SDK goes silent past idle_timeout, raise TimeoutError."""
    # Yields one message, then hangs long enough to trip a 50ms timeout.
    messages = [_text_msg("a")]
    sdk = _StubClient(messages, hang=True, hang_seconds=2.0)
    client = _turn_client(sdk, "sess-2", idle_timeout=0.05)
    seen = []
    with pytest.raises(asyncio.TimeoutError):
        async for event in client.receive_turn():
            seen.append(event)
    # The first message's events arrived before the hang.
    assert seen == _translated(messages)
    # The underlying iterator was closed before the exception propagated.
    assert sdk.aclose_calls == 1


@pytest.mark.asyncio
async def test_receive_turn_captures_fork_anchor_uuid():
    """The turn's TurnCompleted carries the last MAIN-CHAIN transcript-entry
    uuid as native_turn_id — the ``resume_session_at`` fork anchor. Sidechain
    (subagent) entries and uuid-less messages must not win."""
    messages = [
        AssistantMessage(content=[TextBlock(text="a")], model="m", uuid="uuid-1"),
        # Sidechain entry arrives later but is not a valid truncation point.
        AssistantMessage(
            content=[TextBlock(text="sub")], model="m",
            uuid="uuid-side", parent_tool_use_id="tool-1",
        ),
        AssistantMessage(content=[TextBlock(text="b")], model="m", uuid="uuid-2"),
        # End-turn tool shape: the kept turn can close on a user entry.
        UserMessage(content=[], uuid="uuid-3"),
        _result_msg(),
    ]
    sdk = _StubClient(messages)
    client = _turn_client(sdk, "sess-fork", idle_timeout=5.0)
    completed = [
        e async for e in client.receive_turn()
        if isinstance(e, ev.TurnCompleted)
    ]
    assert len(completed) == 1
    assert completed[0].native_turn_id == "uuid-3"


@pytest.mark.asyncio
async def test_start_turn_resets_fork_anchor():
    """A new turn must not inherit the previous turn's anchor uuid."""

    class _QueryStub:
        async def query(self, *_a, **_k):
            return None

    client = _turn_client(_StubClient([]), "sess-reset", idle_timeout=5.0)
    client._last_entry_uuid = "stale-uuid"
    client._sdk = _QueryStub()
    from nerve.agent.backends.base import TurnInput
    await client.start_turn(TurnInput(text="hi"))
    assert client._last_entry_uuid is None


def test_claude_options_pass_resume_session_at_only_for_forks(tmp_path):
    """fork_last_turn_id maps to resume_session_at, gated on spec.fork —
    without fork_session the flag would truncate the ORIGINAL session."""
    cfg = NerveConfig.from_dict({"workspace": str(tmp_path)})
    backend = ClaudeBackend(SimpleNamespace(
        config=lambda: cfg,
        claude_plugins=lambda: [],
    ))
    base = dict(
        session_id="fork-opts", source="web", model=cfg.agent.model,
        effort="high", system_prompt="p", cwd=str(tmp_path),
        resume_native_id="native-1", fork_last_turn_id="uuid-9",
    )
    with patch.object(backend, "_build_mcp_servers", return_value={}), \
         patch.object(backend, "_build_hooks", return_value={}):
        forked = backend._build_options(SessionSpec(**base, fork=True))
        plain = backend._build_options(SessionSpec(**base, fork=False))
    assert forked.fork_session is True
    assert forked.resume_session_at == "uuid-9"
    assert plain.resume_session_at is None


def test_claude_options_set_stdout_buffer_from_config(tmp_path):
    """The SDK's 1 MiB per-line default aborts the turn on any large image
    tool result; the configured cap must reach ClaudeAgentOptions."""
    def _opts(cfg):
        backend = ClaudeBackend(SimpleNamespace(
            config=lambda: cfg,
            claude_plugins=lambda: [],
        ))
        spec = SessionSpec(
            session_id="buf-opts", source="web", model=cfg.agent.model,
            effort="high", system_prompt="p", cwd=str(tmp_path),
        )
        with patch.object(backend, "_build_mcp_servers", return_value={}), \
             patch.object(backend, "_build_hooks", return_value={}):
            return backend._build_options(spec)

    default = NerveConfig.from_dict({"workspace": str(tmp_path)})
    assert default.agent.cli_max_message_bytes == 64 * 1024 * 1024
    assert _opts(default).max_buffer_size == 64 * 1024 * 1024

    custom = NerveConfig.from_dict({
        "workspace": str(tmp_path),
        "agent": {"cli_max_message_bytes": 8 * 1024 * 1024},
    })
    assert _opts(custom).max_buffer_size == 8 * 1024 * 1024


@pytest.mark.asyncio
async def test_receive_turn_disabled_when_timeout_zero():
    """idle_timeout <= 0 disables the timeout (legacy behaviour)."""
    # Hangs forever after 1 message.  Without a timeout we'd wait forever;
    # to verify "disabled" we wrap the whole call in our own short outer
    # timeout and assert that's what fired (not the inner one).
    messages = [_text_msg("a")]
    sdk = _StubClient(messages, hang=True, hang_seconds=10.0)
    client = _turn_client(sdk, "sess-3", idle_timeout=0)
    seen = []
    with pytest.raises(asyncio.TimeoutError):
        async with asyncio.timeout(0.1):
            async for event in client.receive_turn():
                seen.append(event)
    assert seen == _translated(messages)
    # Outer-cancel still triggers the finally block → aclose() runs.
    assert sdk.aclose_calls == 1


@pytest.mark.asyncio
async def test_receive_turn_raises_on_empty_stream():
    """An exhausted stream with no result is a dead runtime, not a turn.

    Previously this returned silently with no events, which the engine's
    receive loop could not tell apart from a completed turn. base.py
    requires every turn to end in a terminal event, so a pre-result EOF
    now raises. The stream can only end this way after the subprocess
    exited zero (a non-zero exit raises inside the SDK), so the
    transport really is gone.
    """
    sdk = _StubClient([])
    client = _turn_client(sdk, "sess-4", idle_timeout=5.0)
    seen = []
    with pytest.raises(TransportDiedError):
        async for event in client.receive_turn():
            seen.append(event)
    assert seen == []
    # Kept from the test this replaces. It counts the stub generator's own
    # finally, which runs on exhaustion regardless of receive_turn's cleanup;
    # test_receive_turn_closes_the_iterator_when_eof_raises pins that.
    assert sdk.aclose_calls == 1


@pytest.mark.asyncio
async def test_receive_turn_raises_when_stream_ends_after_content():
    """A stream that streams text then dies without a result also raises."""
    messages = [_text_msg("half an answer")]
    sdk = _StubClient(messages)
    client = _turn_client(sdk, "sess-4b", idle_timeout=5.0)
    seen = []
    with pytest.raises(TransportDiedError):
        async for event in client.receive_turn():
            seen.append(event)
    # The partial content arrived before the death was reported.
    assert seen == _translated(messages)
    assert sdk.aclose_calls == 1


@pytest.mark.asyncio
async def test_receive_turn_completes_on_result_without_raising():
    """A stream ending in a ResultMessage yields exactly one TurnCompleted."""
    messages = [_text_msg("a"), _result_msg("sdk-xyz")]
    sdk = _StubClient(messages)
    client = _turn_client(sdk, "sess-4c", idle_timeout=5.0)
    seen = []
    async for event in client.receive_turn():
        seen.append(event)
    terminal = [e for e in seen if isinstance(e, ev.TurnCompleted)]
    assert len(terminal) == 1
    assert terminal[0].status == "completed"
    assert sdk.aclose_calls == 1


@pytest.mark.asyncio
async def test_receive_turn_idle_timeout_is_not_a_transport_death():
    """A hung (but live) CLI must stay distinguishable from a dead one.

    A caller that handles only TransportDiedError must not swallow an
    idle timeout, so assert the timeout is NOT a TransportDiedError.
    """
    sdk = _StubClient([_text_msg("a")], hang=True, hang_seconds=2.0)
    client = _turn_client(sdk, "sess-4d", idle_timeout=0.05)
    with pytest.raises(asyncio.TimeoutError) as excinfo:
        async for _event in client.receive_turn():
            pass
    assert not isinstance(excinfo.value, TransportDiedError)


@pytest.mark.asyncio
async def test_receive_turn_propagates_other_errors_unchanged():
    """A real error from the stream is not relabelled a transport death."""

    class _BoomClient:
        def __init__(self):
            self.aclose_calls = 0

        def receive_response(self):
            outer = self

            async def _gen():
                try:
                    raise RuntimeError("boom from the stream")
                    yield  # pragma: no cover - unreachable, makes this a generator
                finally:
                    outer.aclose_calls += 1

            return _gen()

    sdk = _BoomClient()
    client = _turn_client(sdk, "sess-4e", idle_timeout=5.0)
    with pytest.raises(RuntimeError, match="boom from the stream"):
        async for _event in client.receive_turn():
            pass
    assert sdk.aclose_calls == 1


class _AcloseCountingClient:
    """Counts explicit ``aclose()`` calls, not the generator's own finally.

    ``_StubClient.aclose_calls`` counts its generator's ``finally``, which
    also runs on plain exhaustion and on cancellation, so it stays 1 even
    with ``receive_turn``'s own ``finally`` deleted and cannot pin the
    cleanup. Delegating through a wrapper counts only the real call.
    """

    def __init__(self, messages):
        self._messages = messages
        self.explicit_aclose = 0

    def receive_response(self):
        outer = self

        async def _gen():
            for msg in outer._messages:
                yield msg

        class _Counting:
            def __init__(self, inner):
                self._inner = inner

            def __anext__(self):
                return self._inner.__anext__()

            async def aclose(self):
                outer.explicit_aclose += 1
                return await self._inner.aclose()

        return _Counting(_gen())


@pytest.mark.asyncio
async def test_receive_turn_closes_the_iterator_when_eof_raises():
    """The EOF raise still closes the SDK iterator exactly once."""
    sdk = _AcloseCountingClient([])
    client = _turn_client(sdk, "sess-4f", idle_timeout=5.0)
    with pytest.raises(TransportDiedError):
        async for _event in client.receive_turn():
            pass
    assert sdk.explicit_aclose == 1


@pytest.mark.asyncio
async def test_receive_turn_closes_the_iterator_on_a_completed_turn():
    """The normal path closes it exactly once too, via the same finally."""
    sdk = _AcloseCountingClient([_text_msg("a"), _result_msg()])
    client = _turn_client(sdk, "sess-4g", idle_timeout=5.0)
    async for _event in client.receive_turn():
        pass
    assert sdk.explicit_aclose == 1


# ---------------------------------------------------------------------------
# Engine receive loop: what a pre-result EOF does to a turn.
#
# These drive the REAL retry loop (engine.py) with a scripted client, so they
# pin both sides of its "retry only if no content arrived yet" gate.
# ---------------------------------------------------------------------------


class _ScriptedClient:
    """Minimal AgentClient whose receive_turn follows a script.

    Script items: ``("text", str)``, ``("result", session_id)``,
    ``("raise",)`` -> TransportDiedError, as a pre-result EOF produces.
    """

    def __init__(self, script: list[tuple], label: str):
        self._script = script
        self.label = label
        self.model = "claude-test"
        self.disconnect_calls = 0

    @property
    def native_session_id(self) -> str:
        return f"native-{self.label}"

    async def connect(self) -> None:
        pass

    async def start_turn(self, turn) -> None:
        pass

    async def receive_turn(self):
        for item in self._script:
            if item[0] == "raise":
                raise TransportDiedError("claude runtime ended the turn without a result")
            msg = _text_msg(item[1]) if item[0] == "text" else _result_msg(item[1])
            for event in translate_message(msg):
                yield event

    async def interrupt(self) -> None:
        pass

    def is_alive(self) -> bool:
        return True

    async def disconnect(self, timeout: float = 5.0) -> None:
        self.disconnect_calls += 1

    # Sync, like the real client's: the idle drain calls it without await.
    def try_receive_idle_events(self):
        return None

    def buffer_used(self) -> int:
        return 0


async def _engine_with_scripted_clients(db, tmp_path, session_id, scripts):
    """An AgentEngine that serves one _ScriptedClient per script, in order."""
    workspace = tmp_path / f"ws-{session_id}"
    workspace.mkdir(parents=True, exist_ok=True)
    cfg = NerveConfig.from_dict({
        "workspace": str(workspace), "agent": {"backend": "claude"},
    })
    engine = AgentEngine(cfg, db)
    await engine.sessions.get_or_create(
        session_id, title="t", source="web", backend="claude",
    )
    made: list[_ScriptedClient] = []

    async def _fake_get_or_create(sid, source, model, **kw):
        script = scripts[min(len(made), len(scripts) - 1)]
        client = _ScriptedClient(script, f"c{len(made)}")
        made.append(client)
        engine.sessions.set_client(sid, client)
        return client

    engine._get_or_create_client = _fake_get_or_create
    return engine, made


@pytest.mark.asyncio
async def test_engine_retries_fresh_client_when_eof_precedes_content(db, tmp_path):
    """No content yet: the dead client is replaced and the turn retried.

    This is the pre-existing hung/dead-client path: the EOF raise simply
    routes into it instead of being read as an empty success.
    """
    engine, made = await _engine_with_scripted_clients(
        db, tmp_path, "eof-no-content",
        [[("raise",)], [("text", "recovered"), ("result", "sdk-ok")]],
    )
    out = await engine.run("eof-no-content", "hello", source="web", channel="web")
    assert out == "recovered"
    # Exactly one retry: the dead client was disconnected, the fresh one kept.
    assert [c.label for c in made] == ["c0", "c1"]
    assert made[0].disconnect_calls == 1
    assert made[1].disconnect_calls == 0


@pytest.mark.asyncio
async def test_engine_fails_turn_when_eof_follows_content(db, tmp_path):
    """Content already streamed: the turn fails instead of retrying.

    Retrying would duplicate output, so the engine re-raises to its error
    handler. Two consequences are asserted deliberately: the live view and
    the return value carry the error while the stored row keeps the partial
    text in ``blocks``, and the native session id survives for resume.
    """
    engine, made = await _engine_with_scripted_clients(
        db, tmp_path, "eof-after-content",
        [[("text", "half an answer"), ("raise",)]],
    )
    out = await engine.run("eof-after-content", "hello", source="web", channel="web")
    # No retry: the return value carries the error, not the partial text.
    assert [c.label for c in made] == ["c0"]
    assert out.startswith("Agent error:")
    assert "half an answer" not in out
    # Resume is preserved: the id is read off the client before removal.
    row = await db.get_session("eof-after-content")
    assert row["sdk_session_id"] == "native-c0"
    # The dead client is replaced, not returned to the pool: this is what
    # makes a spent stream unreusable.
    assert made[0].disconnect_calls == 1
    assert engine.sessions.get_client("eof-after-content") is None
    # The durable split: the error is the message content, while the partial
    # text survives in `blocks` (the error path never clears ordered_blocks).
    rows = await db.get_messages("eof-after-content")
    asst = [r for r in rows if r["role"] == "assistant"][-1]
    assert asst["content"].startswith("Agent error:")
    assert json.dumps(asst["blocks"] or []).count("half an answer") == 1


@pytest.mark.asyncio
async def test_workflow_session_never_forks_from_parent(db, tmp_path):
    """A source=='workflow' leg carries parent_session_id for sidebar nesting
    only — its first turn must NOT fork the parent's native session (that would
    contaminate the fresh orchestration prompt, and cross-backend it raises).
    A non-workflow child with identical setup still forks: source is the only
    discriminator."""
    engine, _made = await _engine_with_scripted_clients(
        db, tmp_path, "seed", [[("text", "ok"), ("result", "sdk-ok")]],
    )
    # Parent with a resumable native session — the fork target.
    await engine.sessions.get_or_create(
        "parent", title="t", source="web", backend="claude",
    )
    await db.update_session_fields("parent", {"sdk_session_id": "sdk-parent"})

    forks: dict[str, str | None] = {}
    served = engine._get_or_create_client

    async def _cap(sid, source, model, **kw):
        forks[sid] = kw.get("fork_from")
        return await served(sid, source, model, **kw)

    engine._get_or_create_client = _cap

    # Workflow leg nested under the parent, still in 'created' state.
    await engine.sessions.get_or_create(
        "workflow:wfr-x", title="t", source="workflow", backend="claude",
    )
    await db.update_session_fields("workflow:wfr-x", {"parent_session_id": "parent"})
    await engine.run("workflow:wfr-x", "go", source="workflow", channel="web")
    assert forks["workflow:wfr-x"] is None

    # Control: a non-workflow child with the same setup DOES fork.
    await engine.sessions.get_or_create(
        "web-child", title="t", source="web", backend="claude",
    )
    await db.update_session_fields("web-child", {"parent_session_id": "parent"})
    await engine.run("web-child", "go", source="web", channel="web")
    assert forks["web-child"] == "sdk-parent"


# ---------------------------------------------------------------------------
# ClaudeBackend.validate_resume_target
# ---------------------------------------------------------------------------

def _make_backend() -> ClaudeBackend:
    """Minimal ClaudeBackend (validate_resume_target reads no config)."""
    return ClaudeBackend(SimpleNamespace(config=lambda: SimpleNamespace()))


class TestValidateResumeTarget:
    WORKSPACE = "/root/nerve-workspace"

    def test_returns_true_when_file_present(self):
        backend = _make_backend()
        with patch("nerve.agent.backends.claude.os.path.isfile", return_value=True):
            assert backend.validate_resume_target(
                "some-session-id", self.WORKSPACE,
            ) is True

    def test_returns_false_when_file_missing(self):
        backend = _make_backend()
        with patch("nerve.agent.backends.claude.os.path.isfile", return_value=False):
            assert backend.validate_resume_target(
                "some-session-id", self.WORKSPACE,
            ) is False

    def test_fail_open_on_exception(self):
        """Any unexpected error returns True rather than crashing the turn."""
        backend = _make_backend()
        with patch(
            "nerve.agent.backends.claude.os.path.isfile",
            side_effect=OSError("denied"),
        ):
            assert backend.validate_resume_target(
                "some-session-id", self.WORKSPACE,
            ) is True

    def test_path_encodes_workspace_slashes(self):
        """'/' in the workspace path are replaced with '-' in the projects subdir."""
        backend = _make_backend()
        captured: dict = {}

        def _capture(path: str) -> bool:
            captured["path"] = path
            return True

        # Pin realpath to identity so this test exercises only the
        # slash-encoding, independent of any host symlinks.  Symlink
        # resolution itself is covered by the symlinked-workspace tests.
        with patch("nerve.agent.backends.claude.os.path.realpath",
                   side_effect=lambda p: p), \
             patch("nerve.agent.backends.claude.os.path.isfile",
                   side_effect=_capture):
            backend.validate_resume_target("sid-abc", "/root/nerve-workspace")

        assert "-root-nerve-workspace" in captured["path"]
        assert "sid-abc.jsonl" in captured["path"]

    def test_path_ends_with_jsonl(self):
        """The constructed path always ends with <session_id>.jsonl."""
        backend = _make_backend()
        captured: dict = {}

        def _capture(path: str) -> bool:
            captured["path"] = path
            return True

        with patch("nerve.agent.backends.claude.os.path.isfile",
                   side_effect=_capture):
            backend.validate_resume_target("myid", "/workspace")

        assert captured["path"].endswith("myid.jsonl")

    def test_symlinked_workspace_checks_realpath(self, tmp_path, monkeypatch):
        """A symlinked workspace finds its history under the *resolved*
        (realpath) directory, matching where the CLI actually writes it.

        Regression test for resume-history loss on the Docker deployment,
        where config.workspace (/root/nerve-workspace) is a symlink and
        the guard previously checked the unresolved-path-encoded dir,
        which never exists, then wiped the stored sdk_session_id.
        """
        real_ws = tmp_path / "real-workspace"
        real_ws.mkdir()
        link_ws = tmp_path / "linked-workspace"
        link_ws.symlink_to(real_ws)

        fake_home = tmp_path / "home"
        monkeypatch.setenv("HOME", str(fake_home))

        sid = "11111111-2222-3333-4444-555555555555"
        encoded = os.path.realpath(str(link_ws)).replace("/", "-")
        proj_dir = fake_home / ".claude" / "projects" / encoded
        proj_dir.mkdir(parents=True)
        (proj_dir / f"{sid}.jsonl").write_text("{}")

        backend = _make_backend()
        assert backend.validate_resume_target(sid, str(link_ws)) is True

    def test_symlinked_workspace_missing_returns_false(self, tmp_path, monkeypatch):
        """Symlinked workspace, but the .jsonl genuinely does not exist:
        the guard returns False so the caller starts a fresh conversation."""
        real_ws = tmp_path / "real-workspace"
        real_ws.mkdir()
        link_ws = tmp_path / "linked-workspace"
        link_ws.symlink_to(real_ws)

        fake_home = tmp_path / "home"
        monkeypatch.setenv("HOME", str(fake_home))

        encoded = os.path.realpath(str(link_ws)).replace("/", "-")
        (fake_home / ".claude" / "projects" / encoded).mkdir(parents=True)

        backend = _make_backend()
        assert backend.validate_resume_target("does-not-exist", str(link_ws)) is False

    def test_falls_back_to_unresolved_path(self, tmp_path, monkeypatch):
        """If history lives under the unresolved (symlink) encoding, the
        fallback still finds it (non-symlinked layouts, or a future CLI
        that stops resolving the cwd)."""
        real_ws = tmp_path / "real-workspace"
        real_ws.mkdir()
        link_ws = tmp_path / "linked-workspace"
        link_ws.symlink_to(real_ws)

        fake_home = tmp_path / "home"
        monkeypatch.setenv("HOME", str(fake_home))

        sid = "abcd"
        encoded_unresolved = str(link_ws).replace("/", "-")
        proj_dir = fake_home / ".claude" / "projects" / encoded_unresolved
        proj_dir.mkdir(parents=True)
        (proj_dir / f"{sid}.jsonl").write_text("{}")

        backend = _make_backend()
        assert backend.validate_resume_target(sid, str(link_ws)) is True


# ---------------------------------------------------------------------------
# ClaudeBackend._build_hooks — background-agent permission parity
# ---------------------------------------------------------------------------

def _make_hook_backend(
    background_agent_permissions: bool,
    cli_max_message_bytes: int = 64 * 1024 * 1024,
) -> ClaudeBackend:
    """Minimal backend stub for exercising _build_hooks's PreToolUse wiring."""
    config = SimpleNamespace(
        agent=SimpleNamespace(
            background_agent_permissions=background_agent_permissions,
            cli_max_message_bytes=cli_max_message_bytes,
        ),
    )
    return ClaudeBackend(SimpleNamespace(config=lambda: config))


def _hook_spec(session_id: str) -> SessionSpec:
    return SessionSpec(
        session_id=session_id, source="web", model="m", effort="high",
        system_prompt="", cwd="/tmp",
    )


def _catch_all_grant_hook(hooks: dict):
    """Return the catch-all (matcher=None) PreToolUse hook callback, or None."""
    for matcher in hooks.get("PreToolUse", []):
        if matcher.matcher is None:
            return matcher.hooks[0]
    return None


def _read_validator_hook(backend: ClaudeBackend):
    for matcher in backend._build_hooks(_hook_spec("sess-img"))["PreToolUse"]:
        if matcher.matcher == "Read":
            return matcher.hooks[0]
    raise AssertionError("Read image validator hook not registered")


def _fake_png(tmp_path: Path, size: int) -> str:
    png = tmp_path / "shot.png"
    png.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\0" * size)
    return str(png)


class TestReadHookTransportBound:
    """A valid image can still be too big for the SDK transport: its result
    is one stream-json line, and a line over max_buffer_size aborts the
    whole turn. The Read validator refuses it up front instead."""

    @pytest.mark.asyncio
    async def test_denies_image_that_cannot_fit_the_configured_cap(self, tmp_path):
        # 600 KB → ~820 KB of base64, shipped twice per line → > 1 MiB.
        path = _fake_png(tmp_path, 600 * 1024)
        hook = _read_validator_hook(
            _make_hook_backend(True, cli_max_message_bytes=1024 * 1024),
        )
        out = await hook(
            {"tool_name": "Read", "tool_input": {"file_path": path}}, "tid", None,
        )
        spec = out["hookSpecificOutput"]
        assert spec.get("permissionDecision") == "deny"
        assert "cli_max_message_bytes" in spec["permissionDecisionReason"]

    @pytest.mark.asyncio
    async def test_allows_the_same_image_under_the_default_cap(self, tmp_path):
        path = _fake_png(tmp_path, 600 * 1024)
        hook = _read_validator_hook(_make_hook_backend(True))
        out = await hook(
            {"tool_name": "Read", "tool_input": {"file_path": path}}, "tid", None,
        )
        assert "permissionDecision" not in out["hookSpecificOutput"]


class TestBuildHooksBackgroundPermissions:
    """The catch-all PreToolUse hook gives background sub-agents (whose nested
    tool calls never reach can_use_tool) the same permissions as foreground."""

    @pytest.mark.asyncio
    async def test_grants_non_interactive_tools_when_enabled(self):
        backend = _make_hook_backend(True)
        hooks = backend._build_hooks(_hook_spec("sess-x"))
        grant = _catch_all_grant_hook(hooks)
        assert grant is not None, "permission-grant hook should be registered"

        # Permission-requiring, non-interactive tools are pre-approved so a
        # detached background sub-agent can run them without a prompt.
        for tool in ("Bash", "Write", "Edit", "NotebookEdit", "Glob",
                     "mcp__some_server__write_thing"):
            out = await grant({"tool_name": tool}, "tid", None)
            spec = out["hookSpecificOutput"]
            assert spec.get("permissionDecision") == "allow", tool

    @pytest.mark.asyncio
    async def test_defers_interactive_but_grants_read_when_enabled(self):
        backend = _make_hook_backend(True)
        grant = _catch_all_grant_hook(backend._build_hooks(_hook_spec("sess-x")))

        # Interactive tools defer to can_use_tool (pause / inject / deny):
        # the hook must NOT pre-decide them, or the web pause-for-input breaks.
        for tool in ("AskUserQuestion", "ExitPlanMode", "EnterPlanMode"):
            out = await grant({"tool_name": tool}, "tid", None)
            assert "permissionDecision" not in out["hookSpecificOutput"], tool

        # Read IS parity-allowed: can_use_tool never fires for detached
        # background agents, so excluding Read only broke their
        # out-of-cwd reads (Edit/Write worked while Read was denied).
        # The image validator still runs and its deny wins over this allow.
        out = await grant({"tool_name": "Read"}, "tid", None)
        assert out["hookSpecificOutput"].get("permissionDecision") == "allow"

    @pytest.mark.asyncio
    async def test_no_grant_hook_when_disabled(self):
        backend = _make_hook_backend(False)
        hooks = backend._build_hooks(_hook_spec("sess-y"))
        assert _catch_all_grant_hook(hooks) is None
        # Snapshot + image-validator hooks stay registered regardless.
        matchers = {m.matcher for m in hooks["PreToolUse"]}
        assert "Edit|Write|NotebookEdit" in matchers
        assert "Read" in matchers


# ---------------------------------------------------------------------------
# _model_family — serving-model identifier normalization
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "model, expected",
    [
        # Bare alias passes through
        ("claude-fable-5",                    "claude-fable-5"),
        ("claude-opus-4-8",                   "claude-opus-4-8"),
        # Dated release ids collapse onto the alias
        ("claude-fable-5-20260601",           "claude-fable-5"),
        ("claude-opus-4-8-20260115",          "claude-opus-4-8"),
        # Bedrock inference-profile spellings
        ("us.anthropic.claude-fable-5-20260601-v1:0", "claude-fable-5"),
        ("global.anthropic.claude-fable-5",   "claude-fable-5"),
        # Context-window suffix
        ("claude-sonnet-4-5[1m]",             "claude-sonnet-4-5"),
        # "-latest" alias
        ("claude-haiku-4-5-latest",           "claude-haiku-4-5"),
        # Case / whitespace robustness
        ("  Claude-Fable-5-20260601 ",        "claude-fable-5"),
        # Version-looking tails that are NOT dates stay intact
        ("claude-opus-4",                     "claude-opus-4"),
        ("claude-3-5-sonnet-20241022",        "claude-3-5-sonnet"),
    ],
)
def test_model_family(model, expected):
    assert _model_family(model) == expected


def test_model_family_distinguishes_real_changes():
    # The pair that matters: a frontier model downgrading to the prior tier
    # must NOT normalize to the same family.
    assert _model_family("claude-fable-5-20260601") != _model_family(
        "claude-opus-4-8-20260115",
    )


# ---------------------------------------------------------------------------
# _track_serving_model — downgrade / recovery detection via
# translate_message + _process_agent_event (the shared user-run +
# autonomous-turn path)
# ---------------------------------------------------------------------------


def _make_model_engine(configured: str | None = "claude-fable-5") -> AgentEngine:
    """Minimal AgentEngine stub for serving-model tracking tests."""
    engine = AgentEngine.__new__(AgentEngine)
    engine._session_models = {"s1": configured} if configured else {}
    engine._observed_models = {}
    engine._workflows = {}
    return engine


def _assistant(model: str, parent_tool_use_id: str | None = None) -> AssistantMessage:
    return AssistantMessage(
        content=[], model=model, parent_tool_use_id=parent_tool_use_id,
    )


async def _process_sdk_message(engine: AgentEngine, session_id, message, st) -> bool:
    """Feed one SDK message through the live pipeline: translate it into
    normalized events, dispatch each to the engine. Returns True when the
    message completed the turn (TurnCompleted event)."""
    done = False
    for event in translate_message(message):
        done = await engine._process_agent_event(session_id, event, st)
    return done


class TestServingModelTracking:
    @pytest.mark.asyncio
    async def test_first_message_downgrade_fires_event(self):
        engine = _make_model_engine()
        st = _TurnState()
        with patch("nerve.agent.engine.broadcaster") as bc:
            bc.broadcast_model_changed = AsyncMock()
            await _process_sdk_message(
                engine, "s1", _assistant("claude-opus-4-8-20260115"), st,
            )
        assert st.last_model == "claude-opus-4-8-20260115"
        assert st.ordered_blocks == [{
            "type": "model_change",
            "from": "claude-fable-5",
            "to": "claude-opus-4-8-20260115",
            "downgrade": True,
        }]
        bc.broadcast_model_changed.assert_awaited_once_with(
            "s1",
            from_model="claude-fable-5",
            to_model="claude-opus-4-8-20260115",
            downgrade=True,
        )

    @pytest.mark.asyncio
    async def test_same_family_dated_id_is_not_a_change(self):
        engine = _make_model_engine()
        st = _TurnState()
        with patch("nerve.agent.engine.broadcaster") as bc:
            bc.broadcast_model_changed = AsyncMock()
            await _process_sdk_message(
                engine, "s1", _assistant("claude-fable-5-20260601"), st,
            )
        assert st.ordered_blocks == []
        bc.broadcast_model_changed.assert_not_awaited()
        # Baseline still updated for subsequent comparisons
        assert engine._observed_models["s1"] == "claude-fable-5-20260601"

    @pytest.mark.asyncio
    async def test_recovery_back_to_configured_is_not_downgrade(self):
        engine = _make_model_engine()
        engine._observed_models["s1"] = "claude-opus-4-8-20260115"
        st = _TurnState()
        with patch("nerve.agent.engine.broadcaster") as bc:
            bc.broadcast_model_changed = AsyncMock()
            await _process_sdk_message(
                engine, "s1", _assistant("claude-fable-5-20260601"), st,
            )
        assert st.ordered_blocks == [{
            "type": "model_change",
            "from": "claude-opus-4-8-20260115",
            "to": "claude-fable-5-20260601",
            "downgrade": False,
        }]

    @pytest.mark.asyncio
    async def test_mid_session_transition_uses_observed_baseline(self):
        engine = _make_model_engine()
        st = _TurnState()
        with patch("nerve.agent.engine.broadcaster") as bc:
            bc.broadcast_model_changed = AsyncMock()
            await _process_sdk_message(
                engine, "s1", _assistant("claude-fable-5-20260601"), st,
            )
            await _process_sdk_message(
                engine, "s1", _assistant("claude-opus-4-8-20260115"), st,
            )
        # Only the second message fires; "from" is the observed dated id,
        # not the configured alias.
        assert len(st.ordered_blocks) == 1
        assert st.ordered_blocks[0]["from"] == "claude-fable-5-20260601"
        assert st.ordered_blocks[0]["downgrade"] is True

    @pytest.mark.asyncio
    async def test_subagent_messages_are_ignored(self):
        engine = _make_model_engine()
        st = _TurnState()
        with patch("nerve.agent.engine.broadcaster") as bc:
            bc.broadcast_model_changed = AsyncMock()
            await _process_sdk_message(
                engine, "s1",
                _assistant("claude-haiku-4-5", parent_tool_use_id="tu_1"),
                st,
            )
        # Sub-agents legitimately run other models — no event, no baseline
        # pollution, and no cost-attribution model override.
        assert st.last_model is None
        assert st.ordered_blocks == []
        assert "s1" not in engine._observed_models
        bc.broadcast_model_changed.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_configured_model_first_message_is_quiet(self):
        engine = _make_model_engine(configured=None)
        st = _TurnState()
        with patch("nerve.agent.engine.broadcaster") as bc:
            bc.broadcast_model_changed = AsyncMock()
            await _process_sdk_message(
                engine, "s1", _assistant("claude-opus-4-8-20260115"), st,
            )
        # Nothing to compare against — record the baseline silently.
        assert st.ordered_blocks == []
        bc.broadcast_model_changed.assert_not_awaited()
        assert engine._observed_models["s1"] == "claude-opus-4-8-20260115"
