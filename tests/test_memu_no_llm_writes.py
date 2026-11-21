"""memory.extraction: none — memory without a model: facts stored as written,
conversations and files as embedded chunks, update/delete without memU's
category-summary step, session skip list, and listings that keep chunks
apart from facts. The integration tests run memU's real SQLite storage with
a fake fastembed model and assert that no language model is ever called."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from nerve.config import MemoryConfig, NerveConfig
from nerve.memory.memu_bridge import (
    _chunk_conversation,
    _chunk_document,
    _parse_event_date,
    _RefusingLLMClient,
    _session_of_url,
    _split_long,
)

# --- config ----------------------------------------------------------------


def test_config_reads_extraction_and_skip_sessions() -> None:
    cfg = MemoryConfig.from_dict({"extraction": " NONE ", "skip_sessions": ["cron:nightly-report*", " ", "x"]})
    assert cfg.extraction == "none"
    assert cfg.skip_sessions == ["cron:nightly-report*", "x"]
    assert MemoryConfig.from_dict({}).extraction == "llm"
    assert MemoryConfig.from_dict({}).skip_sessions == []


@pytest.mark.parametrize(
    ("memory", "message"),
    [
        ({"extraction": "regex"}, "memory.extraction must be one of"),
        ({"extraction": "none"}, "needs an embedder"),
    ],
)
def test_config_refuses_bad_extraction(memory, message) -> None:
    config = NerveConfig()
    config.memory = MemoryConfig.from_dict(memory)
    with pytest.raises(ValueError, match=message):
        config._validate_memory_backend()


def test_config_accepts_no_llm_with_a_local_embedder(monkeypatch) -> None:
    # fastembed is an optional extra; CI does not install it.
    import nerve.config as config_module

    monkeypatch.setattr(config_module, "_fastembed_installed", lambda: True)
    config = NerveConfig()
    config.memory = MemoryConfig.from_dict({"extraction": "none", "embedder": "local"})
    config._validate_memory_backend()


# --- chunking, dates -----------------------------------------------------------


def test_split_long_keeps_pieces_bounded_and_overlapping() -> None:
    text = " ".join(f"word{i}" for i in range(1000))
    pieces = _split_long(text, size=300, overlap=50)
    assert len(pieces) > 1
    assert all(len(p) <= 300 for p in pieces)
    assert pieces[0].split()[-1] in pieces[1]  # the border word appears again
    assert _split_long("short") == ["short"]
    assert _split_long("   ") == []


def test_chunk_conversation_packs_messages_and_keeps_first_timestamps() -> None:
    entries = [
        {"role": "user", "content": "first question", "created_at": "2026-11-03T03:00:00Z"},
        {"role": "assistant", "content": "a" * 1400, "created_at": "2026-11-03T03:01:00Z"},
        {"role": "user", "content": "", "created_at": "2026-11-03T03:02:00Z"},
        {"role": "user", "content": "second question", "created_at": "2026-11-03T03:03:00Z"},
    ]
    chunks = _chunk_conversation(entries)
    assert chunks[0][0].startswith("user: first question")
    assert chunks[0][1] == "2026-11-03T03:00:00Z"
    assert all(len(text) <= 1500 for text, _ in chunks)
    assert chunks[-1][0].endswith("user: second question")
    assert _chunk_conversation([{"role": "user", "content": ""}]) == []


def test_chunk_document_cuts_at_blank_lines() -> None:
    text = "\n\n".join(f"paragraph {i} " + "x" * 400 for i in range(10))
    chunks = _chunk_document(text)
    assert len(chunks) > 1
    assert all(len(c) <= 1500 for c in chunks)
    assert chunks[0].startswith("paragraph 0")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("On November 3, 2026, the user merged PR 4100", "2026-11-03"),
        ("Deployed 2026-10-28 at 18:13", "2026-10-28"),
        ("Pushed 0c1d2e3f on Nov 2", "2026-11-02"),
        ("Posted on 1 November about Olympus Mons", "2026-11-01"),
        ("Reviewed it yesterday", "2026-11-04"),
        ("Merged today, after the call", "2026-11-05"),
        ("He may have pushed it", None),
        ("Nothing dated here", None),
        ("Due Feb 30, then March 2", "2026-03-02"),
    ],
)
def test_parse_event_date(text, expected) -> None:
    reference = datetime(2026, 11, 5, 9, 0, tzinfo=timezone.utc)
    assert _parse_event_date(text, reference) == expected


def test_parse_event_date_puts_a_yearless_future_date_in_the_past_year() -> None:
    assert _parse_event_date("Booked on Dec 30", datetime(2026, 1, 2, tzinfo=timezone.utc)) == "2025-12-30"


def test_session_of_url() -> None:
    assert _session_of_url("/x/session-3f9a1c07-1793676027.json") == "3f9a1c07"
    assert _session_of_url("/x/session-cron:nightly-report:20260101-000000-9b0d2e51-1793704860.json") == (
        "cron:nightly-report:20260101-000000-9b0d2e51"
    )
    assert _session_of_url("/x/memorize-1.txt") == ""


@pytest.mark.asyncio
async def test_refusing_client_refuses() -> None:
    client = _RefusingLLMClient("fast")
    for call in (client.chat("hi"), client.summarize("x"), client.embed(["x"])):
        with pytest.raises(RuntimeError, match="may not call a language model"):
            await call


# --- integration: memU's real SQLite storage, fake embedding model ---------------


def test_no_llm_memory_end_to_end(tmp_path) -> None:
    """The whole no-LLM flow on memU's real storage, in a fresh interpreter
    (memU registers its tables in process-global SQLAlchemy metadata, so a
    second real memU service in one process cannot start). See
    _memu_no_llm_e2e.py for the checks."""
    import os
    import subprocess

    env = {**os.environ, "NERVE_HOME": str(tmp_path / "home")}
    proc = subprocess.run(
        [sys.executable, str(Path(__file__).with_name("_memu_no_llm_e2e.py")), str(tmp_path)],
        capture_output=True, text=True, timeout=300, env=env,
    )
    assert proc.returncode == 0 and proc.stdout.strip().endswith("ok"), proc.stdout[-3000:] + proc.stderr[-3000:]
