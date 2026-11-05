"""The Claude transport seam (``nerve/agent/backends/claude_transport.py``).

The CLI decides at its first ``initialize`` whether an interrupt spares
background agents (``perTaskStopAffordance``). The Python SDK cannot send that
flag, so :class:`NerveCLITransport` patches the frame. These tests pin the
private SDK seams it relies on, and check that a missing seam degrades to
``interrupt_spares_background == False`` (Stop then asks) instead of silently
killing agents.
"""

from __future__ import annotations

import asyncio
import json
import sys
from types import SimpleNamespace

import pytest
from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    TextBlock,
    ToolResultBlock,
    UserMessage,
)
from claude_agent_sdk._errors import CLIConnectionError
from claude_agent_sdk._internal.query import Query
from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport
from claude_agent_sdk.types import _configure_can_use_tool

from nerve.agent.backends import events as ev
from nerve.agent.backends.base import SessionSpec, TransportDiedError
from nerve.agent.backends.claude import ClaudeBackend, ClaudeClient, translate_message
from nerve.agent.backends.claude_transport import NerveCLITransport, _never_yields
from nerve.config import SubagentsConfig

EXTRAS = {"perTaskStopAffordance": True, "agentProgressSummaries": True}


class _Stdin:
    """Captures what the transport writes to the CLI's stdin."""

    def __init__(self, on_send=None) -> None:
        self.sent: list[str] = []
        self._on_send = on_send

    async def send(self, data: str) -> None:
        self.sent.append(data)
        if self._on_send is not None:
            self._on_send(data)


def _ready_transport(extras=EXTRAS, on_send=None) -> tuple[NerveCLITransport, _Stdin]:
    transport = NerveCLITransport(
        prompt=_never_yields(), options=ClaudeAgentOptions(), init_extras=extras,
    )
    stdin = _Stdin(on_send)
    transport._ready = True
    transport._stdin_stream = stdin
    return transport, stdin


def _init_frame(**request) -> str:
    return json.dumps({
        "type": "control_request",
        "request_id": "req_1_abcd",
        "request": {"subtype": "initialize", "hooks": None, **request},
    }) + "\n"


def _spec() -> SessionSpec:
    return SessionSpec(
        session_id="s-1234", source="web", model="m", effort="high",
        system_prompt="", cwd="/tmp",
    )


async def _allow(*_args, **_kwargs):  # a can_use_tool callback
    return None


# --------------------------------------------------------------------------- #
#  NerveCLITransport.write                                                     #
# --------------------------------------------------------------------------- #


def test_the_transport_is_the_sdks_subprocess_transport():
    assert issubclass(NerveCLITransport, SubprocessCLITransport)


@pytest.mark.asyncio
async def test_only_the_first_initialize_is_patched():
    transport, stdin = _ready_transport()
    first = _init_frame(forwardSubagentText=True)
    second = _init_frame()

    await transport.write(first)
    await transport.write(second)

    patched = json.loads(stdin.sent[0])
    assert patched["request"]["perTaskStopAffordance"] is True
    assert patched["request"]["agentProgressSummaries"] is True
    # The SDK's own fields survive.
    assert patched["request"]["forwardSubagentText"] is True
    assert patched["request_id"] == "req_1_abcd"
    assert stdin.sent[0].endswith("\n")
    assert transport.init_patched is True
    # The CLI honours the flags only on the first initialize: later ones go
    # out exactly as written.
    assert stdin.sent[1] == second


@pytest.mark.asyncio
async def test_setdefault_keeps_an_explicit_value():
    transport, stdin = _ready_transport()
    await transport.write(_init_frame(perTaskStopAffordance=False))
    request = json.loads(stdin.sent[0])["request"]
    assert request["perTaskStopAffordance"] is False
    assert request["agentProgressSummaries"] is True


@pytest.mark.asyncio
async def test_other_frames_pass_through_byte_for_byte():
    transport, stdin = _ready_transport()
    user = json.dumps({
        "type": "user",
        "message": {"role": "user", "content": "say \"initialize\" please"},
        "parent_tool_use_id": None,
    }) + "\n"
    interrupt = json.dumps({
        "type": "control_request", "request_id": "req_2",
        "request": {"subtype": "interrupt"},
    }) + "\n"
    # Both substrings, but not an initialize request.
    lookalike = json.dumps({
        "type": "control_response", "note": "control_request initialize",
        "response": {"subtype": "initialize"},
    }) + "\n"
    for frame in (user, interrupt, lookalike):
        await transport.write(frame)
    assert stdin.sent == [user, interrupt, lookalike]
    assert transport.init_patched is False


@pytest.mark.asyncio
async def test_an_unparseable_frame_passes_through_unpatched():
    transport, stdin = _ready_transport()
    broken = '{"type": "control_request", "request": {"subtype": "initialize"'
    await transport.write(broken)
    assert stdin.sent == [broken]
    assert transport.init_patched is False


@pytest.mark.asyncio
async def test_the_sdks_initialize_goes_through_the_patch():
    """Drive the real ``Query.initialize``: the frame it writes must be the
    one the transport patches (the seam this whole feature relies on)."""
    holder: dict = {}

    def _answer(data: str) -> None:
        msg = json.loads(data)
        rid = msg["request_id"]
        query = holder["query"]
        query.pending_control_results[rid] = {"response": {"commands": []}}
        query.pending_control_responses[rid].set()

    transport, stdin = _ready_transport(on_send=_answer)
    query = Query(transport=transport, is_streaming_mode=True, forward_subagent_text=True)
    holder["query"] = query
    await asyncio.wait_for(query.initialize(), timeout=5)

    request = json.loads(stdin.sent[0])["request"]
    assert request["subtype"] == "initialize"
    assert request["perTaskStopAffordance"] is True
    assert request["agentProgressSummaries"] is True
    assert request["forwardSubagentText"] is True
    assert transport.init_patched is True


# --------------------------------------------------------------------------- #
#  ClaudeClient wiring                                                         #
# --------------------------------------------------------------------------- #


def test_client_builds_the_transport_with_stdio_and_the_sdk_with_the_original():
    options = ClaudeAgentOptions(can_use_tool=_allow)
    client = ClaudeClient(_spec(), options, subagents=SubagentsConfig())

    transport = client._transport
    assert isinstance(transport, NerveCLITransport)
    assert transport.init_extras == EXTRAS
    # The transport carries the configured copy (the CLI flag that routes
    # permission prompts over stdio) ...
    assert transport._options.permission_prompt_tool_name == "stdio"
    # ... the SDK client the original: it configures it again at connect,
    # and a pre-configured copy would raise there.
    assert client._sdk.options is options
    assert options.permission_prompt_tool_name is None
    assert client._sdk._custom_transport is transport
    _configure_can_use_tool(client._sdk.options)  # does not raise
    with pytest.raises(ValueError):
        _configure_can_use_tool(transport._options)


def test_each_flag_is_sent_only_when_enabled():
    client = ClaudeClient(
        _spec(), ClaudeAgentOptions(),
        subagents=SubagentsConfig(per_task_stop_affordance=True, progress_summaries=False),
    )
    assert client._transport.init_extras == {"perTaskStopAffordance": True}
    client = ClaudeClient(
        _spec(), ClaudeAgentOptions(),
        subagents=SubagentsConfig(per_task_stop_affordance=False, progress_summaries=True),
    )
    assert client._transport.init_extras == {"agentProgressSummaries": True}


def test_flags_off_means_no_custom_transport():
    client = ClaudeClient(
        _spec(), ClaudeAgentOptions(),
        subagents=SubagentsConfig(per_task_stop_affordance=False, progress_summaries=False),
    )
    assert client._transport is None
    assert client._sdk._custom_transport is None
    assert client.interrupt_spares_background is False


def test_default_subagents_config_declares_both_flags():
    client = ClaudeClient(_spec(), ClaudeAgentOptions())
    assert client._transport.init_extras == EXTRAS


@pytest.mark.asyncio
async def test_backend_passes_the_configured_subagents(monkeypatch):
    config = SimpleNamespace(agent=SimpleNamespace(
        subagents=SubagentsConfig(per_task_stop_affordance=False, progress_summaries=False),
    ))
    backend = ClaudeBackend.__new__(ClaudeBackend)
    backend._deps = SimpleNamespace(config=lambda: config)
    monkeypatch.setattr(backend, "_build_options", lambda spec: ClaudeAgentOptions())
    built: list[ClaudeClient] = []

    async def _connect(self):
        built.append(self)

    monkeypatch.setattr(ClaudeClient, "connect", _connect)
    await backend.create_client(_spec())
    assert built and built[0]._transport is None


@pytest.mark.asyncio
async def test_interrupt_spares_background_reflects_the_patch():
    client = ClaudeClient(_spec(), ClaudeAgentOptions(), subagents=SubagentsConfig())
    # Built, not yet initialized: nothing proves the CLI got the flag.
    assert client.interrupt_spares_background is False
    client._transport._ready = True
    client._transport._stdin_stream = _Stdin()
    await client._transport.write(_init_frame())
    assert client.interrupt_spares_background is True


@pytest.mark.asyncio
async def test_a_patch_without_the_affordance_does_not_spare():
    client = ClaudeClient(
        _spec(), ClaudeAgentOptions(),
        subagents=SubagentsConfig(per_task_stop_affordance=False, progress_summaries=True),
    )
    client._transport._ready = True
    client._transport._stdin_stream = _Stdin()
    await client._transport.write(_init_frame())
    assert client._transport.init_patched is True
    assert client.interrupt_spares_background is False


@pytest.mark.asyncio
async def test_a_frame_the_patch_never_matches_degrades():
    """The SDK changed its initialize frame: nothing is patched, and the
    client says so (Stop asks before it would end background agents)."""
    client = ClaudeClient(_spec(), ClaudeAgentOptions(), subagents=SubagentsConfig())
    client._transport._ready = True
    client._transport._stdin_stream = _Stdin()
    await client._transport.write(json.dumps({
        "type": "control_request", "request_id": "r",
        "request": {"subtype": "init"},
    }) + "\n")
    assert client._transport.init_patched is False
    assert client.interrupt_spares_background is False


def test_a_missing_seam_degrades_to_no_affordance(monkeypatch):
    """The transport module cannot be imported (the SDK moved its private
    transport): the client still works, without the affordance."""
    monkeypatch.setitem(sys.modules, "nerve.agent.backends.claude_transport", None)
    client = ClaudeClient(_spec(), ClaudeAgentOptions(), subagents=SubagentsConfig())
    assert client._transport is None
    assert client._sdk._custom_transport is None
    assert client.interrupt_spares_background is False
    assert client.supports_task_stop is True


def test_a_moved_sdk_transport_module_degrades(monkeypatch):
    """The SDK's private transport module is gone: importing the patch fails
    and the client runs on the default transport, without the affordance."""
    monkeypatch.setitem(
        sys.modules, "claude_agent_sdk._internal.transport.subprocess_cli", None,
    )
    monkeypatch.delitem(sys.modules, "nerve.agent.backends.claude_transport")
    client = ClaudeClient(_spec(), ClaudeAgentOptions(), subagents=SubagentsConfig())
    assert client._transport is None
    assert client.interrupt_spares_background is False
    assert "nerve.agent.backends.claude_transport" not in sys.modules


def test_a_transport_that_fails_to_build_degrades(monkeypatch):
    import nerve.agent.backends.claude_transport as module

    def _boom(*_args, **_kwargs):
        raise TypeError("signature changed")

    monkeypatch.setattr(module, "NerveCLITransport", _boom)
    client = ClaudeClient(_spec(), ClaudeAgentOptions(), subagents=SubagentsConfig())
    assert client._transport is None
    assert client.interrupt_spares_background is False


def test_test_doubles_built_with_new_read_no_transport():
    client = ClaudeClient.__new__(ClaudeClient)
    assert client._transport is None
    assert client.interrupt_spares_background is False


# --------------------------------------------------------------------------- #
#  stop_task and interrupt                                                     #
# --------------------------------------------------------------------------- #


class _FakeSDK:
    def __init__(self, *, stop_error: Exception | None = None, stop_delay: float = 0.0):
        self.stopped: list[str] = []
        self.interrupts = 0
        self.control: list[dict] = []
        self._stop_error = stop_error
        self._stop_delay = stop_delay
        sdk = self

        class _Query:
            async def _send_control_request(self, request, timeout=60.0):
                sdk.control.append(request)
                return {}

        self._query = _Query()

    async def stop_task(self, task_id: str) -> None:
        if self._stop_delay:
            await asyncio.sleep(self._stop_delay)
        if self._stop_error is not None:
            raise self._stop_error
        self.stopped.append(task_id)

    async def interrupt(self) -> None:
        self.interrupts += 1


def _client_with(sdk: _FakeSDK) -> ClaudeClient:
    client = ClaudeClient.__new__(ClaudeClient)
    client._spec = _spec()
    client._sdk = sdk
    return client


@pytest.mark.asyncio
async def test_stop_task_sends_the_control_request():
    sdk = _FakeSDK()
    await _client_with(sdk).stop_task("a1b2c3d4e5f6a7b8c")
    assert sdk.stopped == ["a1b2c3d4e5f6a7b8c"]


@pytest.mark.asyncio
async def test_stop_task_on_a_dead_transport_raises_transport_died():
    sdk = _FakeSDK(stop_error=CLIConnectionError("Not connected"))
    with pytest.raises(TransportDiedError):
        await _client_with(sdk).stop_task("a1b2c3d4e5f6a7b8c")


@pytest.mark.asyncio
async def test_stop_task_times_out(monkeypatch):
    import nerve.agent.backends.claude as claude_module

    monkeypatch.setattr(claude_module, "STOP_TASK_TIMEOUT", 0.05)
    sdk = _FakeSDK(stop_delay=1.0)
    with pytest.raises(asyncio.TimeoutError):
        await _client_with(sdk).stop_task("a1b2c3d4e5f6a7b8c")


@pytest.mark.asyncio
async def test_interrupt_plain_and_with_cancel_queued():
    sdk = _FakeSDK()
    client = _client_with(sdk)
    await client.interrupt()
    assert (sdk.interrupts, sdk.control) == (1, [])
    await client.interrupt(cancel_queued=True)
    assert sdk.interrupts == 1
    assert sdk.control == [{"subtype": "interrupt", "cancel_queued": True}]


@pytest.mark.asyncio
async def test_interrupt_cancel_queued_without_the_seam_falls_back():
    sdk = _FakeSDK()
    sdk._query = None
    await _client_with(sdk).interrupt(cancel_queued=True)
    assert sdk.interrupts == 1


def test_claude_capabilities_declare_task_stop_and_transcripts():
    caps = ClaudeBackend.capabilities
    assert caps.supports_task_stop is True
    assert caps.supports_agent_transcripts is True


# --------------------------------------------------------------------------- #
#  Translation                                                                 #
# --------------------------------------------------------------------------- #


def test_sidechain_usage_becomes_subagent_usage():
    message = AssistantMessage(
        content=[TextBlock(text="working")], model="model-x",
        parent_tool_use_id="toolu_PARENT", message_id="msg_1",
        usage={"input_tokens": 3, "output_tokens": 4},
    )
    events = translate_message(message)
    usage = [e for e in events if isinstance(e, ev.SubagentUsage)]
    assert usage == [ev.SubagentUsage(
        parent_tool_use_id="toolu_PARENT", message_id="msg_1", model="model-x",
        usage={"input_tokens": 3, "output_tokens": 4},
    )]
    text = [e for e in events if isinstance(e, ev.TextDelta)]
    assert text and text[0].parent_tool_use_id == "toolu_PARENT"


def test_a_single_tool_result_carries_the_structured_result():
    structured = {"status": "async_launched", "agentId": "a1b2c3d4e5f6a7b8c"}
    message = UserMessage(
        content=[ToolResultBlock(tool_use_id="toolu_A", content="Async agent launched")],
        tool_use_result=structured,
    )
    (result,) = [e for e in translate_message(message) if isinstance(e, ev.ToolResult)]
    assert result.structured == structured


def test_two_tool_results_carry_no_structured_result():
    message = UserMessage(
        content=[
            ToolResultBlock(tool_use_id="toolu_A", content="one"),
            ToolResultBlock(tool_use_id="toolu_B", content="two"),
        ],
        tool_use_result={"status": "completed"},
    )
    results = [e for e in translate_message(message) if isinstance(e, ev.ToolResult)]
    assert [r.structured for r in results] == [None, None]


def test_subagent_started_carries_its_parent():
    from claude_agent_sdk import ToolUseBlock

    message = AssistantMessage(
        content=[ToolUseBlock(id="toolu_CHILD", name="Agent", input={
            "description": "nested", "subagent_type": "general-purpose",
        })],
        model="model-x", parent_tool_use_id="toolu_PARENT",
    )
    started = [e for e in translate_message(message) if isinstance(e, ev.SubagentStarted)]
    assert started and started[0].parent_tool_use_id == "toolu_PARENT"
