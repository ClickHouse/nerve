"""Tests for the optional xmemory.ai memory layer.

xmemory runs *alongside* memU, never replacing it:
* ``memorize`` dual-writes (memU + xmemory async), and
* ``memory_recall`` appends xmemory's read output to memU's hits.

These tests lock in three contracts: (1) the bridge is inert unless both a
token and an instance_id are configured, (2) every xmemory failure is
isolated so memU recall/memorize still works, and (3) the handlers combine
both sources without regressing the memU-only output shape.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.tools.handlers.memory import (
    memorize_handler,
    memory_recall_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig, XmemoryConfig
from nerve.memory.xmemory_bridge import XmemoryBridge, _extract_answer


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

    c2 = XmemoryConfig.from_dict({
        "api_key": "tok",
        "instance_id": "inst_1",
        "api_url": "https://example.test",
        "extraction_logic": "fast",
        "read_mode": "raw-tables",
        "timeout": 30,
    })
    assert c2.enabled and c2.api_url == "https://example.test"
    assert c2.extraction_logic == "fast" and c2.read_mode == "raw-tables"
    assert c2.timeout == 30.0


def test_nerveconfig_wires_xmemory_block() -> None:
    nc = NerveConfig.from_dict({"xmemory": {"api_key": "t", "instance_id": "i"}})
    assert nc.xmemory.enabled is True
    # Absent block → inert, never None.
    assert NerveConfig.from_dict({}).xmemory.enabled is False


# --------------------------------------------------------------------------- #
# Pure helper: answer extraction
# --------------------------------------------------------------------------- #


def test_extract_answer_shapes() -> None:
    assert _extract_answer(SimpleNamespace(reader_result={"answer": "  hi "})) == "hi"
    assert _extract_answer(SimpleNamespace(reader_result={"answer": ""})) is None
    assert _extract_answer(SimpleNamespace(reader_result={})) is None
    assert _extract_answer(SimpleNamespace(reader_result="plain")) == "plain"
    assert _extract_answer(SimpleNamespace(reader_result=SimpleNamespace(answer="x"))) == "x"


def test_extract_answer_labels_each_sub_query() -> None:
    """Decomposed answers keep the sub-question they answer — otherwise the
    agent cannot tell which answer belongs to which part of the query."""
    assert _extract_answer(
        SimpleNamespace(
            reader_result="Who leads sales? -> Ann\nWhere is HQ? -> Berlin",
            reader_results=[
                SimpleNamespace(
                    sub_query="Who leads sales?",
                    reader_result={"answer": "Ann"},
                    error=None,
                ),
                SimpleNamespace(
                    sub_query="Where is HQ?",
                    reader_result={"answer": "Berlin"},
                    error=None,
                ),
            ],
        )
    ) == "Who leads sales?: Ann\nWhere is HQ?: Berlin"


def test_extract_answer_surfaces_sub_query_error() -> None:
    """A sub-query the server could not answer is reported, not dropped."""
    assert _extract_answer(
        SimpleNamespace(
            reader_result="Ann leads sales.",
            reader_results=[
                SimpleNamespace(
                    sub_query="Who leads sales?",
                    reader_result={"answer": "Ann"},
                    error=None,
                ),
                SimpleNamespace(
                    sub_query="What is churn?", reader_result="", error="no data",
                ),
            ],
        )
    ) == "Who leads sales?: Ann\nWhat is churn?: (unavailable: no data)"


def test_extract_answer_falls_back_when_no_sub_query_answers() -> None:
    """Every sub-query empty and error-free → fall back to the combined
    ``reader_result`` rather than discarding a real answer."""
    assert _extract_answer(
        SimpleNamespace(
            reader_result="Partial: HQ is Berlin.",
            reader_results=[
                SimpleNamespace(sub_query="q1", reader_result="", error=None),
                SimpleNamespace(sub_query="q2", reader_result=None, error=None),
            ],
        )
    ) == "Partial: HQ is Berlin."


def test_extract_answer_does_not_mine_answer_keys_from_table_rows() -> None:
    """Under raw-tables the payload is data rows. A row with an ``answer``
    column must not be mistaken for the synthesized answer."""
    rows = [
        {"question": "What is our refund window?", "answer": "30 days"},
        {"question": "What is our SLA?", "answer": "99.9%"},
    ]
    text = _extract_answer(
        SimpleNamespace(reader_result=rows, reader_results=[]), single_answer=False,
    )
    assert text is not None
    # Whole rows survive — questions included, not just the 'answer' cells.
    assert "refund window" in text and "30 days" in text


def test_extract_answer_preserves_non_ascii() -> None:
    """JSON rendering must not escape non-ASCII into \\uXXXX noise."""
    text = _extract_answer(
        SimpleNamespace(reader_result=[{"city": "Zürich"}], reader_results=[]),
        single_answer=False,
    )
    assert text is not None and "Zürich" in text


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
) -> XmemoryBridge:
    """Build a bridge bound to a (fake-token) real client, then mock the
    instance handle so reads/writes never hit the network."""
    cfg = XmemoryConfig(
        api_key="tok",
        instance_id="inst_1",
        extraction_logic=extraction_logic,
        read_mode=read_mode,
    )
    bridge = XmemoryBridge(cfg)
    await bridge.initialize()  # client + .instance() are network-free
    assert bridge.available
    bridge._instance = AsyncMock()
    return bridge


@pytest.mark.asyncio
async def test_recall_answer_returns_single_answer() -> None:
    bridge = await _enabled_bridge(read_mode="single-answer")
    bridge._instance.read = AsyncMock(
        return_value=SimpleNamespace(reader_result={"answer": "alice@acme.com"})
    )
    ans = await bridge.recall_answer("What is Alice's email?")
    assert ans == "alice@acme.com"
    # Uses SINGLE_ANSWER read mode.
    _, kwargs = bridge._instance.read.call_args
    assert kwargs["read_mode"] == bridge._ReadMode.SINGLE_ANSWER
    await bridge.aclose()


@pytest.mark.asyncio
async def test_recall_defaults_to_single_answer() -> None:
    """An unconfigured ``read_mode`` synthesizes an answer, not raw rows."""
    bridge = XmemoryBridge(XmemoryConfig(api_key="tok", instance_id="inst_1"))
    await bridge.initialize()
    bridge._instance = AsyncMock()
    bridge._instance.read = AsyncMock(
        return_value=SimpleNamespace(reader_result={"answer": "HQ is in Berlin."})
    )
    assert await bridge.recall_answer("Where is HQ?") == "HQ is in Berlin."
    _, kwargs = bridge._instance.read.call_args
    assert kwargs["read_mode"] == bridge._ReadMode.SINGLE_ANSWER
    await bridge.aclose()


@pytest.mark.asyncio
async def test_recall_answer_honors_raw_tables_read_mode() -> None:
    bridge = await _enabled_bridge(read_mode="raw-tables")
    bridge._instance.read = AsyncMock(
        return_value=SimpleNamespace(reader_result={"rows": [{"k": "v"}]})
    )
    ans = await bridge.recall_answer("Show rows")
    assert ans is not None
    assert '"rows"' in ans
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
