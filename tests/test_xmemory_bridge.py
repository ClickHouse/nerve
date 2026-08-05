"""Tests for the optional xmemory.ai memory layer.

xmemory runs *alongside* memU, never replacing it:
* ``memorize`` dual-writes (memU + xmemory async),
* ``memory_recall`` appends xmemory's read output to memU's hits, and
* with ``index_conversations`` opted in, the memorization sweep mirrors
  its text-only session transcripts to xmemory.

These tests lock in four contracts: (1) the bridge is inert unless both a
token and an instance_id are configured, (2) every xmemory failure is
isolated so memU recall/memorize still works, (3) the handlers combine
both sources without regressing the memU-only output shape, and (4)
transcript mirroring is opt-in, text-only (no thinking / tool blocks),
chunked, and best-effort.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.engine import AgentEngine
from nerve.agent.tools.handlers.memory import (
    memorize_handler,
    memory_recall_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig, XmemoryConfig
from nerve.memory.xmemory_bridge import (
    XmemoryBridge,
    _serialize_read_payload,
    _transcript_chunks,
    _transcript_lines,
)


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #


def test_config_enabled_requires_both_keys() -> None:
    assert XmemoryConfig().enabled is False
    assert XmemoryConfig(api_key="tok").enabled is False
    assert XmemoryConfig(instance_id="inst_1").enabled is False
    assert XmemoryConfig(api_key="tok", instance_id="inst_1").enabled is True


def test_config_from_dict_defaults_and_overrides() -> None:
    c = XmemoryConfig.from_dict({})
    assert c.api_key == "" and c.instance_id == ""
    assert c.api_url == "https://api.xmemory.ai"
    assert c.extraction_logic == "deep"
    assert c.read_mode == "single-answer"
    assert c.timeout == 60.0
    assert c.index_conversations is False  # transcripts are strictly opt-in

    c2 = XmemoryConfig.from_dict({
        "api_key": "tok",
        "instance_id": "inst_1",
        "api_url": "https://example.test",
        "extraction_logic": "fast",
        "read_mode": "raw-tables",
        "timeout": 30,
        "index_conversations": True,
    })
    assert c2.enabled and c2.api_url == "https://example.test"
    assert c2.extraction_logic == "fast" and c2.read_mode == "raw-tables"
    assert c2.timeout == 30.0
    assert c2.index_conversations is True


def test_nerveconfig_wires_xmemory_block() -> None:
    nc = NerveConfig.from_dict({"xmemory": {"api_key": "t", "instance_id": "i"}})
    assert nc.xmemory.enabled is True
    # Absent block → inert, never None.
    assert NerveConfig.from_dict({}).xmemory.enabled is False


# --------------------------------------------------------------------------- #
# Pure helper: read-payload serialization
# --------------------------------------------------------------------------- #


def test_serialize_prefers_reader_results() -> None:
    """When the server decomposes the query, the per-sub-query results win, and
    the SDK's Pydantic models are unwrapped to plain dicts for JSON."""
    from xmemory import ReadResult, TaggedReaderResult

    result = ReadResult(
        trace_id="t",
        reader_result="combined back-compat answer",
        reader_results=[
            TaggedReaderResult(sub_query="Who leads sales?", reader_result={"answer": "Ann"}),
            TaggedReaderResult(sub_query="What is churn?", reader_result="", error="no data"),
        ],
    )
    parsed = json.loads(_serialize_read_payload(result))
    assert [p["sub_query"] for p in parsed] == ["Who leads sales?", "What is churn?"]
    assert parsed[0]["reader_result"] == {"answer": "Ann"}
    assert parsed[1]["error"] == "no data"


def test_serialize_falls_back_to_reader_result() -> None:
    """No decomposition (empty ``reader_results``) → serialize the combined
    ``reader_result``, intact. The bridge never reaches for an ``answer`` key:
    raw-tables is columns+rows, xresponse is objects+relations, and both are
    passed through whole."""
    raw_tables = {
        "columns": ["question", "answer"],
        "rows": [["refund window?", "30 days"], ["SLA?", "99.9%"]],
    }
    assert json.loads(
        _serialize_read_payload(SimpleNamespace(reader_result=raw_tables, reader_results=[]))
    ) == raw_tables

    xresponse = {"objects": [{"type": "Person", "name": "Ann"}], "relations": []}
    assert json.loads(
        _serialize_read_payload(SimpleNamespace(reader_result=xresponse, reader_results=[]))
    ) == xresponse


def test_serialize_string_payload_passes_through_unquoted() -> None:
    assert _serialize_read_payload(
        SimpleNamespace(reader_result="  a plain answer  ", reader_results=[])
    ) == "a plain answer"


def test_serialize_empty_payload_is_none() -> None:
    assert _serialize_read_payload(
        SimpleNamespace(reader_result=None, reader_results=[])
    ) is None
    assert _serialize_read_payload(
        SimpleNamespace(reader_result="", reader_results=[])
    ) is None


def test_serialize_preserves_non_ascii() -> None:
    """JSON rendering must not escape non-ASCII into \\uXXXX noise."""
    out = _serialize_read_payload(
        SimpleNamespace(reader_result={"city": "Zürich"}, reader_results=[])
    )
    assert "Zürich" in out


# --------------------------------------------------------------------------- #
# Bridge — disabled / missing-package paths
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_bridge_inert_when_unconfigured() -> None:
    bridge = XmemoryBridge(XmemoryConfig())
    await bridge.initialize()
    assert bridge.available is False
    assert await bridge.recall_answer("q") is None
    assert await bridge.memorize("knowledge: x") is False
    await bridge.aclose()  # idempotent / safe on a never-initialized bridge


@pytest.mark.asyncio
async def test_bridge_disabled_when_package_missing(monkeypatch) -> None:
    # Simulate `import xmemory` raising ImportError even though it's installed.
    monkeypatch.setitem(sys.modules, "xmemory", None)
    bridge = XmemoryBridge(XmemoryConfig(api_key="t", instance_id="i"))
    await bridge.initialize()
    assert bridge.available is False


# --------------------------------------------------------------------------- #
# Bridge — enabled path (real client construction, mocked instance handle)
# --------------------------------------------------------------------------- #


async def _enabled_bridge(
    extraction_logic: str = "deep",
    read_mode: str = "single-answer",  # mirrors the production default
    index_conversations: bool = False,  # mirrors the production default
) -> XmemoryBridge:
    """Build a bridge bound to a (fake-token) real client, then mock the
    instance handle so reads/writes never hit the network."""
    cfg = XmemoryConfig(
        api_key="tok",
        instance_id="inst_1",
        extraction_logic=extraction_logic,
        read_mode=read_mode,
        index_conversations=index_conversations,
    )
    bridge = XmemoryBridge(cfg)
    await bridge.initialize()  # client + .instance() are network-free
    assert bridge.available
    bridge._instance = AsyncMock()
    return bridge


@pytest.mark.asyncio
async def test_recall_answer_serializes_single_answer() -> None:
    bridge = await _enabled_bridge(read_mode="single-answer")
    bridge._instance.read = AsyncMock(
        return_value=SimpleNamespace(reader_result={"answer": "alice@acme.com"})
    )
    ans = await bridge.recall_answer("What is Alice's email?")
    assert json.loads(ans) == {"answer": "alice@acme.com"}
    # Uses SINGLE_ANSWER read mode.
    _, kwargs = bridge._instance.read.call_args
    assert kwargs["read_mode"] == bridge._ReadMode.SINGLE_ANSWER
    await bridge.aclose()


@pytest.mark.asyncio
async def test_recall_defaults_to_single_answer_read_mode() -> None:
    """An unconfigured ``read_mode`` reads in single-answer mode."""
    bridge = XmemoryBridge(XmemoryConfig(api_key="tok", instance_id="inst_1"))
    await bridge.initialize()
    bridge._instance = AsyncMock()
    bridge._instance.read = AsyncMock(
        return_value=SimpleNamespace(reader_result={"answer": "HQ is in Berlin."})
    )
    assert json.loads(await bridge.recall_answer("Where is HQ?")) == {
        "answer": "HQ is in Berlin.",
    }
    _, kwargs = bridge._instance.read.call_args
    assert kwargs["read_mode"] == bridge._ReadMode.SINGLE_ANSWER
    await bridge.aclose()


@pytest.mark.asyncio
async def test_recall_answer_honors_raw_tables_read_mode() -> None:
    bridge = await _enabled_bridge(read_mode="raw-tables")
    table = {"columns": ["k"], "rows": [["v"]]}
    bridge._instance.read = AsyncMock(
        return_value=SimpleNamespace(reader_result=table)
    )
    ans = await bridge.recall_answer("Show rows")
    assert json.loads(ans) == table  # columns and rows both survive
    _, kwargs = bridge._instance.read.call_args
    assert kwargs["read_mode"] == bridge._ReadMode.RAW_TABLES
    await bridge.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("configured", "expected_attr"),
    [
        ("raw_tables", "RAW_TABLES"),   # underscore alias still accepted
        ("RAW-TABLES", "RAW_TABLES"),   # case-insensitive
        ("  xresponse ", "XRESPONSE"),  # surrounding whitespace tolerated
        ("nonsense", "SINGLE_ANSWER"),  # unknown → default
        ("", "SINGLE_ANSWER"),          # empty → default
    ],
)
async def test_read_mode_resolution(configured: str, expected_attr: str) -> None:
    bridge = await _enabled_bridge(read_mode=configured)
    assert bridge._read_mode() == getattr(bridge._ReadMode, expected_attr)
    await bridge.aclose()


@pytest.mark.asyncio
async def test_recall_answer_isolates_errors() -> None:
    bridge = await _enabled_bridge()
    bridge._instance.read = AsyncMock(side_effect=RuntimeError("xmem down"))
    assert await bridge.recall_answer("q") is None  # never propagates
    await bridge.aclose()


@pytest.mark.asyncio
async def test_memorize_writes_async_with_configured_logic() -> None:
    bridge = await _enabled_bridge(extraction_logic="deep")
    bridge._instance.write_async = AsyncMock(return_value=SimpleNamespace(write_id="w1"))
    assert await bridge.memorize("knowledge: the sky is blue") is True
    args, kwargs = bridge._instance.write_async.call_args
    assert args[0] == "knowledge: the sky is blue"
    assert kwargs["extraction_logic"] == bridge._ExtractionLogic.DEEP
    await bridge.aclose()


@pytest.mark.asyncio
async def test_memorize_honors_fast_extraction_logic() -> None:
    bridge = await _enabled_bridge(extraction_logic="fast")
    bridge._instance.write_async = AsyncMock(return_value=SimpleNamespace(write_id="w1"))
    await bridge.memorize("event: launched")
    _, kwargs = bridge._instance.write_async.call_args
    assert kwargs["extraction_logic"] == bridge._ExtractionLogic.FAST
    await bridge.aclose()


@pytest.mark.asyncio
async def test_memorize_isolates_errors() -> None:
    bridge = await _enabled_bridge()
    bridge._instance.write_async = AsyncMock(side_effect=RuntimeError("boom"))
    assert await bridge.memorize("knowledge: x") is False
    await bridge.aclose()


# --------------------------------------------------------------------------- #
# Handlers — dual recall / dual write
# --------------------------------------------------------------------------- #


def _ctx(*, memu, xmem) -> ToolContext:
    return ToolContext(
        session_id="s-1",
        workspace=Path("/tmp/ws"),
        db=None,
        memory_bridge=memu,
        xmemory_bridge=xmem,
        config=None,
    )


def _memu_recall(items):
    memu = MagicMock()
    memu.available = True
    memu.recall = AsyncMock(return_value=items)
    return memu


@pytest.mark.asyncio
async def test_recall_handler_combines_memu_and_xmemory() -> None:
    memu = _memu_recall([
        {"id": "i1", "type": "profile", "summary": "Alice lives in Metropolis"},
    ])
    xmem = MagicMock()
    xmem.available = True
    xmem.recall_answer = AsyncMock(return_value="Alice's email is alice@acme.com")

    result = await memory_recall_handler(_ctx(memu=memu, xmem=xmem), {"query": "alice"})
    text = result.content[0]["text"]

    assert "[memU]" in text
    assert "Alice lives in Metropolis" in text
    assert "[xmemory]" in text
    assert "alice@acme.com" in text
    xmem.recall_answer.assert_awaited_once_with("alice")


@pytest.mark.asyncio
async def test_recall_handler_xmemory_answer_without_memu_hits() -> None:
    memu = _memu_recall([])  # memU returns nothing
    xmem = MagicMock()
    xmem.available = True
    xmem.recall_answer = AsyncMock(return_value="Synthesized from the graph.")

    result = await memory_recall_handler(_ctx(memu=memu, xmem=xmem), {"query": "q"})
    text = result.content[0]["text"]
    assert "No relevant memories found" in text  # memU part
    assert "Synthesized from the graph." in text  # xmemory part
    assert "[xmemory]" in text


@pytest.mark.asyncio
async def test_recall_handler_preserves_memu_only_shape_when_xmemory_disabled() -> None:
    memu = _memu_recall([
        {"id": "i1", "type": "profile", "summary": "Alice lives in Metropolis"},
    ])
    # xmemory bridge absent entirely.
    result = await memory_recall_handler(_ctx(memu=memu, xmem=None), {"query": "x"})
    text = result.content[0]["text"]
    assert "Recalled 1 memories" in text
    assert "[memU]" not in text  # original format, no source labels
    assert "[xmemory]" not in text


@pytest.mark.asyncio
async def test_recall_handler_no_xmemory_section_when_answer_empty() -> None:
    memu = _memu_recall([
        {"id": "i1", "type": "profile", "summary": "fact"},
    ])
    xmem = MagicMock()
    xmem.available = True
    xmem.recall_answer = AsyncMock(return_value=None)  # xmemory found nothing
    result = await memory_recall_handler(_ctx(memu=memu, xmem=xmem), {"query": "x"})
    text = result.content[0]["text"]
    assert "Recalled 1 memories" in text
    assert "[xmemory]" not in text  # no empty section


@pytest.mark.asyncio
async def test_recall_handler_surfaces_xmemory_when_memu_errors() -> None:
    memu = MagicMock()
    memu.available = True
    memu.recall = AsyncMock(side_effect=RuntimeError("db down"))
    xmem = MagicMock()
    xmem.available = True
    xmem.recall_answer = AsyncMock(return_value="still answerable")

    result = await memory_recall_handler(_ctx(memu=memu, xmem=xmem), {"query": "x"})
    text = result.content[0]["text"]
    assert "Memory recall error" in text
    assert "still answerable" in text


@pytest.mark.asyncio
async def test_recall_handler_skips_xmemory_when_bridge_unavailable() -> None:
    memu = _memu_recall([{"id": "i1", "type": "profile", "summary": "fact"}])
    xmem = MagicMock()
    xmem.available = False  # configured object present but not ready
    xmem.recall_answer = AsyncMock(return_value="should not be called")

    result = await memory_recall_handler(_ctx(memu=memu, xmem=xmem), {"query": "x"})
    text = result.content[0]["text"]
    assert "[xmemory]" not in text
    xmem.recall_answer.assert_not_called()


@pytest.mark.asyncio
async def test_memorize_handler_dual_writes(monkeypatch, tmp_path) -> None:
    # Keep the manual-memorize file write inside the test sandbox.
    monkeypatch.setenv("HOME", str(tmp_path))

    memu = MagicMock()
    memu.available = True
    memu.memorize_file = AsyncMock(return_value=True)
    xmem = MagicMock()
    xmem.available = True
    xmem.memorize = AsyncMock(return_value=True)

    result = await memorize_handler(
        _ctx(memu=memu, xmem=xmem),
        {"content": "the sky is blue", "memory_type": "knowledge"},
    )
    text = result.content[0]["text"]
    assert "Memorized: the sky is blue" in text
    assert "(+ xmemory)" in text
    xmem.memorize.assert_awaited_once()
    assert xmem.memorize.call_args.args[0] == "knowledge: the sky is blue"


@pytest.mark.asyncio
async def test_memorize_handler_memu_only_when_xmemory_disabled(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    memu = MagicMock()
    memu.available = True
    memu.memorize_file = AsyncMock(return_value=True)

    result = await memorize_handler(
        _ctx(memu=memu, xmem=None),
        {"content": "the sky is blue", "memory_type": "knowledge"},
    )
    text = result.content[0]["text"]
    assert text == "Memorized: the sky is blue"  # no xmemory suffix


@pytest.mark.asyncio
async def test_memorize_handler_succeeds_even_if_xmemory_write_fails(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    memu = MagicMock()
    memu.available = True
    memu.memorize_file = AsyncMock(return_value=True)
    xmem = MagicMock()
    xmem.available = True
    xmem.memorize = AsyncMock(return_value=False)  # xmemory enqueue failed

    result = await memorize_handler(
        _ctx(memu=memu, xmem=xmem),
        {"content": "fact", "memory_type": "knowledge"},
    )
    text = result.content[0]["text"]
    assert "Memorized: fact" in text
    assert "(+ xmemory)" not in text  # memU still succeeded, xmemory silently skipped


# --------------------------------------------------------------------------- #
# Transcript helpers — text-only flattening and chunking
# --------------------------------------------------------------------------- #


def test_transcript_lines_are_text_only() -> None:
    """Only role + content (+ timestamp) survive — the same contract as the
    memU sweep. Thinking and tool blocks/results must never be included."""
    msgs = [
        {
            "role": "user",
            "content": "hello",
            "created_at": "2026-01-01 00:00:00",
            "thinking": "PRIVATE-REASONING",
            "blocks": [{"type": "tool_result", "text": "RAW-TOOL-DUMP"}],
        },
        {"role": "assistant", "content": ""},          # empty → skipped
        {"role": "assistant", "content": "hi there"},  # no timestamp → bare role
    ]
    lines = _transcript_lines(msgs)
    assert lines == [
        "[2026-01-01 00:00:00] user: hello",
        "assistant: hi there",
    ]
    joined = "\n".join(lines)
    assert "PRIVATE-REASONING" not in joined
    assert "RAW-TOOL-DUMP" not in joined


def test_transcript_chunks_single_chunk_header() -> None:
    chunks = _transcript_chunks("s-1", [{"role": "user", "content": "hello"}])
    assert len(chunks) == 1
    header = chunks[0].splitlines()[0]
    assert header == "Conversation transcript (session s-1):"
    assert "user: hello" in chunks[0]


def test_transcript_chunks_split_at_message_boundaries() -> None:
    msgs = [{"role": "user", "content": f"m{i} " + "x" * 30} for i in range(10)]
    chunks = _transcript_chunks("s-1", msgs, chunk_bytes=80)
    assert len(chunks) > 1
    total = len(chunks)
    for i, chunk in enumerate(chunks, start=1):
        header, body = chunk.split("\n", 1)
        assert header == f"Conversation transcript (session s-1, part {i}/{total}):"
        assert len(body.encode("utf-8")) <= 80  # body respects the budget
    combined = "\n".join(c.split("\n", 1)[1] for c in chunks)
    for i in range(10):
        assert f"m{i} " in combined  # every message survives, exactly once each


def test_transcript_chunks_hard_split_oversized_message() -> None:
    """A single message larger than the budget is split rather than dropped."""
    msgs = [{"role": "user", "content": "A" * 200}]
    chunks = _transcript_chunks("s-1", msgs, chunk_bytes=80)
    assert len(chunks) >= 3
    rejoined = "".join(c.split("\n", 1)[1] for c in chunks)
    assert "A" * 200 in rejoined  # nothing lost (ASCII → no boundary drops)


def test_transcript_chunks_empty_transcript() -> None:
    assert _transcript_chunks("s-1", []) == []
    assert _transcript_chunks("s-1", [{"role": "user", "content": ""}]) == []


# --------------------------------------------------------------------------- #
# Bridge — conversation mirroring (opt-in, FAST extraction, best-effort)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_memorize_conversation_requires_opt_in() -> None:
    """An available bridge without ``index_conversations`` never writes."""
    bridge = await _enabled_bridge()  # index_conversations defaults to False
    bridge._instance.write_async = AsyncMock()
    assert bridge.indexes_conversations is False
    sent = await bridge.memorize_conversation(
        "s-1", [{"role": "user", "content": "hello"}],
    )
    assert sent == 0
    bridge._instance.write_async.assert_not_called()
    await bridge.aclose()


@pytest.mark.asyncio
async def test_memorize_conversation_noop_when_bridge_unavailable() -> None:
    """Opt-in without credentials stays inert (no SDK calls, no errors)."""
    bridge = XmemoryBridge(XmemoryConfig(index_conversations=True))  # no keys
    await bridge.initialize()
    assert bridge.indexes_conversations is False
    assert await bridge.memorize_conversation(
        "s-1", [{"role": "user", "content": "hello"}],
    ) == 0


@pytest.mark.asyncio
async def test_memorize_conversation_sends_text_only_with_fast_extraction() -> None:
    """Transcripts always use FAST extraction — even when the configured
    ``extraction_logic`` (which governs the memorize tool) is ``deep``."""
    bridge = await _enabled_bridge(extraction_logic="deep", index_conversations=True)
    bridge._instance.write_async = AsyncMock(return_value=SimpleNamespace(write_id="w1"))
    msgs = [
        {
            "role": "user",
            "content": "hello",
            "created_at": "2026-01-01 00:00:00",
            "thinking": "PRIVATE-REASONING",
            "blocks": [{"type": "tool_result", "text": "RAW-TOOL-DUMP"}],
        },
        {"role": "assistant", "content": "hi!"},
    ]
    assert await bridge.memorize_conversation("s-1", msgs) == 1
    args, kwargs = bridge._instance.write_async.call_args
    payload = args[0]
    assert payload.startswith("Conversation transcript (session s-1)")
    assert "[2026-01-01 00:00:00] user: hello" in payload
    assert "assistant: hi!" in payload
    assert "PRIVATE-REASONING" not in payload
    assert "RAW-TOOL-DUMP" not in payload
    assert kwargs["extraction_logic"] == bridge._ExtractionLogic.FAST
    await bridge.aclose()


@pytest.mark.asyncio
async def test_memorize_conversation_chunks_large_transcripts(monkeypatch) -> None:
    monkeypatch.setattr("nerve.memory.xmemory_bridge._TRANSCRIPT_CHUNK_BYTES", 64)
    bridge = await _enabled_bridge(index_conversations=True)
    bridge._instance.write_async = AsyncMock(return_value=SimpleNamespace(write_id="w"))
    msgs = [
        {"role": "user", "content": f"message number {i} padded " + "x" * 20}
        for i in range(6)
    ]
    sent = await bridge.memorize_conversation("s-1", msgs)
    assert sent > 1
    assert sent == bridge._instance.write_async.await_count
    payloads = [c.args[0] for c in bridge._instance.write_async.call_args_list]
    for payload in payloads:
        assert payload.splitlines()[0].startswith(
            "Conversation transcript (session s-1, part ",
        )
    combined = "\n".join(payloads)
    for i in range(6):
        assert f"message number {i} " in combined
    await bridge.aclose()


@pytest.mark.asyncio
async def test_memorize_conversation_stops_on_first_failure(monkeypatch) -> None:
    """Best-effort: a failed chunk abandons the rest instead of hammering a
    down service — and never raises into the caller."""
    monkeypatch.setattr("nerve.memory.xmemory_bridge._TRANSCRIPT_CHUNK_BYTES", 64)
    bridge = await _enabled_bridge(index_conversations=True)
    bridge._instance.write_async = AsyncMock(
        side_effect=[
            SimpleNamespace(write_id="w1"),
            RuntimeError("quota exceeded"),
            SimpleNamespace(write_id="w3"),
        ],
    )
    msgs = [
        {"role": "user", "content": f"chunk filler {i} " + "y" * 40}
        for i in range(8)
    ]
    sent = await bridge.memorize_conversation("s-1", msgs)
    assert sent == 1  # first chunk enqueued…
    assert bridge._instance.write_async.await_count == 2  # …second failed, rest abandoned
    await bridge.aclose()


@pytest.mark.asyncio
async def test_memorize_conversation_empty_transcript_is_noop() -> None:
    bridge = await _enabled_bridge(index_conversations=True)
    bridge._instance.write_async = AsyncMock()
    assert await bridge.memorize_conversation("s-1", []) == 0
    assert await bridge.memorize_conversation(
        "s-1", [{"role": "assistant", "content": ""}],
    ) == 0
    bridge._instance.write_async.assert_not_called()
    await bridge.aclose()


# --------------------------------------------------------------------------- #
# Engine — the sweep mirrors its memU window to xmemory
# --------------------------------------------------------------------------- #


def _bare_engine(xmem, memu=None, db=None) -> AgentEngine:
    """AgentEngine with only the attributes the memorize paths touch."""
    engine = AgentEngine.__new__(AgentEngine)
    engine._xmemory_bridge = xmem
    engine._memory_bridge = memu
    engine._memorize_bg_tasks = set()
    engine.db = db
    return engine


async def _drain_bg_tasks(engine: AgentEngine) -> None:
    while engine._memorize_bg_tasks:
        await asyncio.gather(
            *list(engine._memorize_bg_tasks), return_exceptions=True,
        )
        await asyncio.sleep(0)  # let done-callbacks run and discard


def _xmem_mirror(opted_in: bool = True) -> MagicMock:
    xmem = MagicMock()
    xmem.indexes_conversations = opted_in
    xmem.memorize_conversation = AsyncMock(return_value=1)
    return xmem


def test_schedule_xmemory_transcript_inert_without_bridge() -> None:
    engine = _bare_engine(xmem=None)
    engine.schedule_xmemory_transcript("s-1", [{"role": "user", "content": "x"}])
    assert engine._memorize_bg_tasks == set()  # nothing scheduled, no crash


@pytest.mark.asyncio
async def test_schedule_xmemory_transcript_inert_without_opt_in() -> None:
    xmem = _xmem_mirror(opted_in=False)
    engine = _bare_engine(xmem=xmem)
    engine.schedule_xmemory_transcript("s-1", [{"role": "user", "content": "x"}])
    assert engine._memorize_bg_tasks == set()
    xmem.memorize_conversation.assert_not_called()


@pytest.mark.asyncio
async def test_schedule_xmemory_transcript_fires_bridge_write() -> None:
    xmem = _xmem_mirror()
    engine = _bare_engine(xmem=xmem)
    msgs = [{"role": "user", "content": "hello", "created_at": "2026-01-01 00:00:00"}]
    engine.schedule_xmemory_transcript("s-1", msgs)
    assert len(engine._memorize_bg_tasks) == 1
    await _drain_bg_tasks(engine)
    xmem.memorize_conversation.assert_awaited_once_with("s-1", msgs)
    assert engine._memorize_bg_tasks == set()  # done-callback cleaned up


@pytest.mark.asyncio
async def test_schedule_xmemory_transcript_isolates_task_failure() -> None:
    """A crashing mirror task is logged by the done-callback, never raised."""
    xmem = _xmem_mirror()
    xmem.memorize_conversation = AsyncMock(side_effect=RuntimeError("boom"))
    engine = _bare_engine(xmem=xmem)
    engine.schedule_xmemory_transcript("s-1", [{"role": "user", "content": "x"}])
    await _drain_bg_tasks(engine)  # must not raise
    assert engine._memorize_bg_tasks == set()


@pytest.mark.asyncio
async def test_incremental_sweep_mirrors_new_messages_to_xmemory() -> None:
    """The periodic sweep sends the exact memU window to xmemory too."""
    old = {"role": "user", "content": "old", "created_at": "2026-01-01 00:00:00"}
    new_user = {"role": "user", "content": "newer", "created_at": "2026-01-02 10:00:00"}
    new_asst = {"role": "assistant", "content": "reply", "created_at": "2026-01-02 10:00:05"}

    db = MagicMock()
    db.get_session = AsyncMock(
        return_value={"id": "s-1", "last_memorized_at": "2026-01-01 12:00:00"},
    )
    db.get_messages = AsyncMock(return_value=[old, new_user, new_asst])
    db.update_session_fields = AsyncMock()

    memu = MagicMock()
    memu.available = True
    memu.memorize_conversation = AsyncMock(return_value=True)

    xmem = _xmem_mirror()
    engine = _bare_engine(xmem=xmem, memu=memu, db=db)

    count = await engine._memorize_incremental("s-1")
    assert count == 2  # only the post-watermark window

    await _drain_bg_tasks(engine)
    memu.memorize_conversation.assert_awaited_once_with("s-1", [new_user, new_asst])
    xmem.memorize_conversation.assert_awaited_once_with("s-1", [new_user, new_asst])
    db.update_session_fields.assert_awaited_once_with(
        "s-1", {"last_memorized_at": "2026-01-02 10:00:05"},
    )


@pytest.mark.asyncio
async def test_incremental_sweep_memu_only_when_not_opted_in() -> None:
    db = MagicMock()
    db.get_session = AsyncMock(return_value={"id": "s-1", "last_memorized_at": None})
    db.get_messages = AsyncMock(
        return_value=[{"role": "user", "content": "hi", "created_at": "2026-01-02 10:00:00"}],
    )
    db.update_session_fields = AsyncMock()

    memu = MagicMock()
    memu.available = True
    memu.memorize_conversation = AsyncMock(return_value=True)

    xmem = _xmem_mirror(opted_in=False)
    engine = _bare_engine(xmem=xmem, memu=memu, db=db)

    assert await engine._memorize_incremental("s-1") == 1
    await _drain_bg_tasks(engine)
    memu.memorize_conversation.assert_awaited_once()
    xmem.memorize_conversation.assert_not_called()
