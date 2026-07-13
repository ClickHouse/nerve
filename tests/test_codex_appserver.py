"""CodexBackend / CodexAppServerClient integration tests.

Driven against ``tests/fixtures/fake_codex_appserver.py`` — a scripted
stdio JSON-RPC subprocess mimicking codex-cli 0.144.1's app-server
surface. Everything runs offline.

The crown-jewel test is ``test_approval_does_not_block_stream``: the
fake emits an approval REQUEST and then keeps streaming deltas that must
arrive while the approval is still pending. The official beta Python SDK
fails exactly this (its reader thread dispatches server requests
synchronously); nerve's asyncio client must not.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from nerve.agent.backends import BackendDeps, SessionSpec, TransportDiedError
from nerve.agent.backends import events as ev
from nerve.agent.backends.codex import CodexBackend
from nerve.agent.backends.base import TurnInput
from nerve.agent.interactive import InteractiveToolHandler
from nerve.agent.interactive import InteractionOutcome
from nerve.config import NerveConfig

FAKE_BIN = str(Path(__file__).parent / "fixtures" / "fake_codex_appserver.py")


def _config(tmp_path: Path, **codex_overrides) -> NerveConfig:
    cfg = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {
            "bin_path": FAKE_BIN,
            "home_dir": str(tmp_path / "codex-home"),
            "model": "gpt-5.6-sol",
            **codex_overrides,
        },
    })
    (tmp_path / "ws").mkdir(parents=True, exist_ok=True)
    return cfg


def _deps(cfg: NerveConfig, *, gateway_port: int = 8900) -> BackendDeps:
    return BackendDeps(
        config=cfg,
        db=None,
        registry=None,
        tool_ctx_factory=lambda sid: None,
        external_mcp_servers=lambda: [],
        gateway_port=lambda: gateway_port,
        mint_session_token=lambda sid: f"tok-{sid}",
    )


class _Broadcasts:
    def __init__(self):
        self.messages: list[tuple[str, dict]] = []

    async def __call__(self, session_id: str, message: dict) -> None:
        self.messages.append((session_id, message))


def _spec(cfg: NerveConfig, *, interactive: bool = True, **kw) -> SessionSpec:
    hub = InteractiveToolHandler(
        session_id=kw.get("session_id", "s1"),
        broadcast_fn=_Broadcasts(),
        interactive_capable=interactive,
    )
    defaults = dict(
        session_id="s1", source="web", model=None, effort="high",
        system_prompt="You are Nerve.", cwd=str(cfg.workspace),
        resume_native_id=None, fork=False, interactive=hub,
        snapshot=None, record_wakeup=None, idle_timeout=15.0,
    )
    defaults.update(kw)
    return SessionSpec(**defaults)


async def _collect_turn(client) -> list:
    events = []
    async for event in client.receive_turn():
        events.append(event)
    return events


def _mode(monkeypatch, mode: str) -> None:
    monkeypatch.setenv("FAKE_CODEX_MODE", mode)


@pytest.mark.asyncio
async def test_basic_turn_streams_and_completes(tmp_path, monkeypatch):
    _mode(monkeypatch, "basic")
    cfg = _config(tmp_path)
    backend = CodexBackend(_deps(cfg))
    client = await backend.create_client(_spec(cfg))
    try:
        assert client.native_session_id == "th_fake_1"  # known at thread/start
        await client.start_turn(TurnInput(text="hello"))
        events = await _collect_turn(client)

        texts = [e.text for e in events if isinstance(e, ev.TextDelta)]
        assert "".join(texts) == "Hello "
        assert any(isinstance(e, ev.ThinkingDelta) for e in events)
        assert isinstance(events[0], ev.ModelObserved)
        assert events[0].model == "gpt-5.6-sol"

        done = events[-1]
        assert isinstance(done, ev.TurnCompleted)
        assert done.status == "completed"
        assert done.native_session_id == "th_fake_1"
        assert done.duration_ms == 1234
        assert done.context_window == 272000
        # OpenAI inputTokens INCLUDES cached — normalized split is disjoint.
        assert done.usage.input_tokens == 200
        assert done.usage.cache_read_tokens == 1000
        assert done.usage.output_tokens == 50
        # ChatGPT auth is subscription/credit based, not API-billed USD.
        # Keep the token-price equivalent separate and explicitly labelled.
        # 200*5 + 1000*0.5 + 50*30 = 1000+500+1500 = 3000 per 1M → $0.003
        assert done.total_cost_usd is None
        assert done.estimated_cost_usd == pytest.approx(0.003)
        assert done.cost_basis == "api_equivalent_estimate"
        # per-turn usage lands anthropic-shaped for the engine
        shaped = done.usage.to_anthropic_shape()
        assert shaped["input_tokens"] == 200
        assert shaped["cache_read_input_tokens"] == 1000
        assert shaped["cache_creation_input_tokens"] == 0
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_effective_api_key_auth_controls_billing(tmp_path, monkeypatch):
    _mode(monkeypatch, "account_api_key")
    cfg = _config(tmp_path, auth="chatgpt")  # deliberately mismatched config
    backend = CodexBackend(_deps(cfg))
    client = await backend.create_client(_spec(cfg))
    try:
        await client.start_turn(TurnInput(text="hello"))
        done = (await _collect_turn(client))[-1]
        assert isinstance(done, ev.TurnCompleted)
        assert done.total_cost_usd == pytest.approx(0.003)
        assert done.estimated_cost_usd is None
        assert done.cost_basis == "api_billed"
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_config_overrides_carry_mcp_bridge(tmp_path, monkeypatch):
    _mode(monkeypatch, "basic")
    cfg = _config(tmp_path)
    backend = CodexBackend(_deps(cfg))
    overrides = backend.build_config_overrides(_spec(cfg))
    joined = "\n".join(overrides)
    assert 'mcp_servers.nerve.url="http://127.0.0.1:8900/mcp/v1/"' in joined
    assert 'mcp_servers.nerve.bearer_token_env_var="NERVE_MCP_TOKEN"' in joined
    assert "mcp_servers.nerve.required=true" in joined
    assert "mcp_servers.nerve.startup_timeout_sec=30" in joined
    assert "project_doc_max_bytes=0" in joined

    client = await backend.create_client(_spec(cfg))
    try:
        # The fake mirrors argv config overrides + env back at initialize;
        # spawn env must carry the session token + isolated CODEX_HOME.
        env = backend.build_env(_spec(cfg))
        assert env["NERVE_MCP_TOKEN"] == "tok-s1"
        assert env["CODEX_HOME"] == str(tmp_path / "codex-home")
    finally:
        await client.disconnect()


def test_config_overrides_use_runtime_loopback_port(tmp_path):
    cfg = _config(tmp_path)
    backend = CodexBackend(_deps(cfg, gateway_port=49152))
    overrides = backend.build_config_overrides(_spec(cfg))
    assert (
        'mcp_servers.nerve.url="http://127.0.0.1:49152/mcp/v1/"'
        in overrides
    )


def test_notification_backlog_fails_transport_instead_of_dropping(monkeypatch):
    from types import SimpleNamespace

    from nerve.agent.backends.codex import appserver as appserver_mod

    async def handler(method, params):
        return {}

    client = appserver_mod.CodexAppServerClient(
        bin_path="codex", cwd="/tmp", env={}, server_request_handler=handler,
    )
    client.notifications = asyncio.Queue(maxsize=2)
    client._proc = SimpleNamespace(pid=None)
    monkeypatch.setattr(appserver_mod, "_MAX_NOTIFICATION_BACKLOG", 2)
    client._dispatch({"jsonrpc": "2.0", "method": "one", "params": {}})
    client._dispatch({"jsonrpc": "2.0", "method": "two", "params": {}})
    client._dispatch({"jsonrpc": "2.0", "method": "turn/completed", "params": {}})
    assert client._closed is True
    assert client.notifications.qsize() == 1
    assert client.notifications.get_nowait()["method"] == "__transport_died__"


@pytest.mark.asyncio
async def test_tools_map_to_claude_vocabulary(tmp_path, monkeypatch):
    _mode(monkeypatch, "tools")
    cfg = _config(tmp_path)
    snapshots: list[tuple[str, str]] = []

    async def snapshot(sid, path, content):
        snapshots.append((sid, path))

    backend = CodexBackend(_deps(cfg))
    client = await backend.create_client(_spec(cfg, snapshot=snapshot))
    try:
        await client.start_turn(TurnInput(text="do things"))
        events = await _collect_turn(client)

        uses = [e for e in events if isinstance(e, ev.ToolUse)]
        results = [e for e in events if isinstance(e, ev.ToolResult)]
        by_name = {u.name for u in uses}
        assert "Bash" in by_name
        assert "Edit" in by_name
        assert "mcp__nerve__memorize" in by_name

        bash = next(u for u in uses if u.name == "Bash")
        assert bash.input["command"] == "echo hi"
        bash_result = next(r for r in results if r.tool_use_id == bash.tool_use_id)
        assert "hi" in bash_result.content
        assert bash_result.is_error is False

        # Multi-file fileChange fans out: one ToolUse/ToolResult per file.
        edits = [u for u in uses if u.name == "Edit"]
        assert {u.input["file_path"] for u in edits} == {
            "/tmp/fake_a.txt", "/tmp/fake_b.txt",
        }
        # PatchChangeKind objects normalize to plain strings.
        assert {u.input["kind"] for u in edits} == {"update", "add"}
        edit_ids = {u.tool_use_id for u in edits}
        edit_results = [r for r in results if r.tool_use_id in edit_ids]
        assert len(edit_results) == 2
        assert any("-a" in (r.content or "") for r in edit_results)

        # Pre-apply snapshots captured for every changed path.
        assert {p for _, p in snapshots} == {"/tmp/fake_a.txt", "/tmp/fake_b.txt"}

        mcp = next(u for u in uses if u.name == "mcp__nerve__memorize")
        assert mcp.input == {"content": "x"}
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_approval_does_not_block_stream(tmp_path, monkeypatch):
    """Deltas emitted while an approval is pending MUST reach the client
    before the approval resolves — the reader can never block on user
    input (the official SDK's deadlock)."""
    _mode(monkeypatch, "approval")
    cfg = _config(tmp_path, approval_policy="on-request")

    delta_seen_before_answer = asyncio.Event()
    answered = asyncio.Event()

    class Hub(InteractiveToolHandler):
        async def request_approval(self, kind, payload):
            # Wait until interleaved deltas prove the stream is alive,
            # then approve.
            await asyncio.wait_for(delta_seen_before_answer.wait(), timeout=10)
            answered.set()
            from nerve.agent.interactive import InteractionOutcome
            assert kind == "command_approval"
            assert payload.get("itemId") == "c1"
            return InteractionOutcome()  # approved

    hub = Hub("s1", _Broadcasts(), interactive_capable=True)
    backend = CodexBackend(_deps(cfg))
    client = await backend.create_client(_spec(cfg, interactive=hub))
    # replace the spec hub (SessionSpec is frozen into the client at build)
    client._spec.interactive = hub
    try:
        await client.start_turn(TurnInput(text="run something"))
        text = ""
        async for event in client.receive_turn():
            if isinstance(event, ev.TextDelta):
                text += event.text
                if "pending" in text and not answered.is_set():
                    delta_seen_before_answer.set()
            if isinstance(event, ev.TurnCompleted):
                break
        assert "streaming while pending" in text
        assert "decision=accept" in text
        assert answered.is_set()
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_approval_declined_on_noninteractive_source(tmp_path, monkeypatch):
    _mode(monkeypatch, "approval")
    cfg = _config(tmp_path, approval_policy="on-request")
    backend = CodexBackend(_deps(cfg))
    client = await backend.create_client(_spec(cfg, interactive=False))
    try:
        await client.start_turn(TurnInput(text="run"))
        events = await _collect_turn(client)
        text = "".join(e.text for e in events if isinstance(e, ev.TextDelta))
        assert "decision=decline" in text  # auto-declined, turn still completed
        assert isinstance(events[-1], ev.TurnCompleted)
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_structured_and_freeform_user_input_bridge(tmp_path):
    cfg = _config(tmp_path)
    captured = {}

    class Hub:
        async def request_interaction(self, tool, payload, timeout=None):
            captured.update(tool=tool, payload=payload, timeout=timeout)
            return InteractionOutcome(result={"name": "Ada", "mode": "Fast"})

    spec = _spec(cfg)
    spec.interactive = Hub()
    from nerve.agent.backends.codex.backend import CodexClient
    client = CodexClient(CodexBackend(_deps(cfg)), spec)
    response = await client._request_user_input({
        "autoResolutionMs": 5000,
        "questions": [
            {
                "id": "name", "header": "Name", "question": "Your name?",
                "isOther": True, "isSecret": False, "options": None,
            },
            {
                "id": "mode", "header": "Mode", "question": "Choose mode",
                "options": [{"label": "Fast", "description": "quick"}],
            },
        ],
    })
    assert captured["payload"]["outOfBand"] is True
    assert captured["payload"]["questions"][0]["freeText"] is True
    assert captured["timeout"] == 5
    assert response == {
        "answers": {
            "name": {"answers": ["Ada"]},
            "mode": {"answers": ["Fast"]},
        },
    }


@pytest.mark.asyncio
async def test_mcp_form_elicitation_preserves_types(tmp_path):
    cfg = _config(tmp_path)

    class Hub:
        async def request_interaction(self, tool, payload, timeout=None):
            assert payload["outOfBand"] is True
            assert payload["questions"][3]["multiSelect"] is True
            return InteractionOutcome(result={
                "enabled": "true", "count": "3", "ratio": "1.5",
                "tags": "a, b", "note": "hello",
            })

    spec = _spec(cfg)
    spec.interactive = Hub()
    from nerve.agent.backends.codex.backend import CodexClient
    client = CodexClient(CodexBackend(_deps(cfg)), spec)
    response = await client._request_mcp_elicitation({
        "mode": "form",
        "message": "Configure",
        "requestedSchema": {
            "type": "object",
            "required": ["enabled", "count"],
            "properties": {
                "enabled": {"type": "boolean"},
                "count": {"type": "integer"},
                "ratio": {"type": "number"},
                "tags": {"type": "array", "items": {"type": "string", "enum": ["a", "b"]}},
                "note": {"type": "string"},
            },
        },
    })
    assert response == {"action": "accept", "content": {
        "enabled": True, "count": 3, "ratio": 1.5,
        "tags": ["a", "b"], "note": "hello",
    }}


@pytest.mark.asyncio
async def test_resume_miss_falls_back_to_fresh_thread(tmp_path, monkeypatch):
    _mode(monkeypatch, "resume_fail")
    cfg = _config(tmp_path)
    backend = CodexBackend(_deps(cfg))
    client = await backend.create_client(
        _spec(cfg, resume_native_id="th_stale_123"),
    )
    try:
        assert client.resume_dropped is True
        assert client.native_session_id == "th_fake_1"  # fresh thread
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_resume_auth_error_never_starts_fresh_thread(tmp_path, monkeypatch):
    _mode(monkeypatch, "resume_auth_fail")
    cfg = _config(tmp_path)
    backend = CodexBackend(_deps(cfg))
    with pytest.raises(Exception, match="authentication expired"):
        await backend.create_client(_spec(cfg, resume_native_id="th_existing"))


@pytest.mark.asyncio
async def test_fork_uses_selected_native_turn(tmp_path, monkeypatch):
    _mode(monkeypatch, "basic")
    cfg = _config(tmp_path)
    backend = CodexBackend(_deps(cfg))
    client = await backend.create_client(_spec(
        cfg,
        resume_native_id="th_parent",
        fork=True,
        fork_last_turn_id="turn_selected",
    ))
    try:
        assert client.native_session_id == "fork:turn_selected"
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_interrupt_terminates_receive_turn(tmp_path, monkeypatch):
    _mode(monkeypatch, "interrupt")
    cfg = _config(tmp_path)
    backend = CodexBackend(_deps(cfg))
    client = await backend.create_client(_spec(cfg))
    try:
        await client.start_turn(TurnInput(text="loop forever"))

        async def _interrupt_soon():
            await asyncio.sleep(0.2)
            await client.interrupt()

        interrupter = asyncio.create_task(_interrupt_soon())
        events = await asyncio.wait_for(_collect_turn(client), timeout=10)
        await interrupter
        done = events[-1]
        assert isinstance(done, ev.TurnCompleted)
        assert done.status == "interrupted"  # graceful /stop wait works
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_failed_turn_completes_with_error(tmp_path, monkeypatch):
    _mode(monkeypatch, "failed_turn")
    cfg = _config(tmp_path)
    backend = CodexBackend(_deps(cfg))
    client = await backend.create_client(_spec(cfg))
    try:
        await client.start_turn(TurnInput(text="explode"))
        events = await _collect_turn(client)
        done = events[-1]
        assert isinstance(done, ev.TurnCompleted)
        assert done.status == "failed"
        assert "model exploded" in (done.error or "")
        # non-retryable error surfaced as a system event too
        assert any(
            isinstance(e, ev.SystemEvent) and e.subtype == "codex_error"
            for e in events
        )
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_transport_death_mid_turn_raises(tmp_path, monkeypatch):
    _mode(monkeypatch, "die_mid_turn")
    cfg = _config(tmp_path)
    backend = CodexBackend(_deps(cfg))
    client = await backend.create_client(_spec(cfg))
    try:
        await client.start_turn(TurnInput(text="die"))
        with pytest.raises(TransportDiedError):
            await asyncio.wait_for(_collect_turn(client), timeout=10)
        assert client.is_alive() is False
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_image_inputs_convert_to_data_urls(tmp_path, monkeypatch):
    _mode(monkeypatch, "basic")
    cfg = _config(tmp_path)
    backend = CodexBackend(_deps(cfg))
    client = await backend.create_client(_spec(cfg))
    try:
        import base64
        png = base64.b64encode(b"\x89PNG\r\n\x1a\n" + b"0" * 16).decode()
        items = client._build_input_items(TurnInput(
            text="look",
            images=[
                {"type": "base64", "media_type": "image/png", "data": png},
                {"path": "/tmp/pic.png"},
                {"type": "text_file", "filename": "notes.txt", "content": "hi"},
                {"type": "base64", "media_type": "application/pdf", "data": "aGk="},
            ],
        ))
        types = [i["type"] for i in items]
        assert types[0] == "text"
        assert "image" in types and "localImage" in types
        text_item = items[0]["text"]
        assert "look" in text_item
        assert "Attached: notes.txt" in text_item          # text file inlined
        assert "PDF attachment could not be delivered" in text_item  # explicit degradation
        image = next(i for i in items if i["type"] == "image")
        assert image["url"].startswith("data:image/png;base64,")
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_idle_stream_is_absent(tmp_path, monkeypatch):
    _mode(monkeypatch, "basic")
    cfg = _config(tmp_path)
    backend = CodexBackend(_deps(cfg))
    assert backend.capabilities.supports_idle_stream is False
    assert backend.capabilities.cost_is_cumulative is False
    assert backend.capabilities.supports_cache_ttl is False
    client = await backend.create_client(_spec(cfg))
    try:
        assert client.try_receive_idle_events() is None
        assert await client.receive_idle_events(0.1) is None
        assert client.buffer_used() == 0
    finally:
        await client.disconnect()


# -- 64 KiB StreamReader line-limit regression (large MCP responses) ----- #


@pytest.mark.asyncio
async def test_read_jsonl_line_tolerates_lines_beyond_limit():
    """The tolerant reader accumulates across LimitOverrunError and
    mirrors readline() semantics (newline kept, partial at EOF, b"" at
    clean EOF) — plain readline() raises ValueError past the limit."""
    from nerve.agent.backends.codex.appserver import _read_jsonl_line

    reader = asyncio.StreamReader(limit=64)
    big = b"x" * 100_000
    reader.feed_data(big + b"\n" + b'{"ok":1}\n' + b"tail-no-newline")
    reader.feed_eof()

    assert await _read_jsonl_line(reader, "stdout") == big + b"\n"
    assert await _read_jsonl_line(reader, "stdout") == b'{"ok":1}\n'
    assert await _read_jsonl_line(reader, "stdout") == b"tail-no-newline"
    assert await _read_jsonl_line(reader, "stdout") == b""


@pytest.mark.asyncio
async def test_read_jsonl_line_caps_runaway_lines(monkeypatch):
    """Past _MAX_LINE_BYTES the line is drained and fails explicitly.

    Framing remains intact for diagnostics/recovery, but callers cannot
    mistake a silently dropped lifecycle message for a healthy transport.
    """
    from nerve.agent.backends.codex import appserver as appserver_mod

    monkeypatch.setattr(appserver_mod, "_MAX_LINE_BYTES", 256)
    reader = asyncio.StreamReader(limit=64)
    reader.feed_data(b"y" * 5000 + b"\n" + b'{"ok":1}\n')
    reader.feed_eof()

    with pytest.raises(TransportDiedError, match="exceeded 256 bytes"):
        await appserver_mod._read_jsonl_line(reader, "stdout")
    assert await appserver_mod._read_jsonl_line(reader, "stdout") == b'{"ok":1}\n'


@pytest.mark.asyncio
async def test_large_mcp_result_survives_transport(tmp_path, monkeypatch):
    """A ~2 MiB MCP tool result on one JSONL line used to raise
    ValueError in the reader loop (asyncio default 64 KiB line limit)
    and fail the whole turn with TransportDiedError."""
    _mode(monkeypatch, "big_line")
    cfg = _config(tmp_path)
    backend = CodexBackend(_deps(cfg))
    client = await backend.create_client(_spec(cfg))
    try:
        await client.start_turn(TurnInput(text="fetch big"))
        events = await asyncio.wait_for(_collect_turn(client), timeout=20)

        results = [e for e in events if isinstance(e, ev.ToolResult)]
        assert any(len(r.content or "") >= 2_000_000 for r in results)
        done = events[-1]
        assert isinstance(done, ev.TurnCompleted)
        assert done.status == "completed"
        assert client.is_alive()
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_oversized_stderr_line_keeps_draining(tmp_path, monkeypatch):
    """A >limit stderr line used to kill the stderr loop silently —
    stderr then filled and the subprocess could wedge on its next
    write. The loop must survive and keep the (clamped) tail."""
    _mode(monkeypatch, "big_stderr")
    cfg = _config(tmp_path)
    backend = CodexBackend(_deps(cfg))
    client = await backend.create_client(_spec(cfg))
    try:
        await client.start_turn(TurnInput(text="log a lot"))
        events = await asyncio.wait_for(_collect_turn(client), timeout=20)

        done = events[-1]
        assert isinstance(done, ev.TurnCompleted)
        assert done.status == "completed"
        assert client.is_alive()

        # The huge line landed in the tail, clamped per line.
        for _ in range(60):  # stderr drains concurrently — poll briefly
            tail = client._transport._stderr_tail
            if any(t.startswith("EEEE") for t in tail):
                break
            await asyncio.sleep(0.05)
        assert any(t.startswith("EEEE") for t in tail)
        assert all(len(t) <= 500 for t in tail)
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_reader_death_marks_client_dead(tmp_path, monkeypatch):
    """stdout EOF while the process is still running: is_alive() used to
    stay True (deaf zombie — every request written fine, then timed out
    at request_timeout). The engine health check must see it dead."""
    _mode(monkeypatch, "close_stdout_mid_turn")
    cfg = _config(tmp_path)
    backend = CodexBackend(_deps(cfg))
    client = await backend.create_client(_spec(cfg))
    try:
        await client.start_turn(TurnInput(text="go deaf"))
        with pytest.raises(TransportDiedError):
            await asyncio.wait_for(_collect_turn(client), timeout=10)

        assert client._transport._proc.returncode is None  # process lives
        assert client.is_alive() is False                  # client is dead
    finally:
        await client.disconnect()
