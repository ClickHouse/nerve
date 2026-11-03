"""End-to-end run of memory.extraction: none on memU's real SQLite storage,
with a fake fastembed model; run by test_memu_no_llm_writes.py in a fresh
interpreter (memU registers its tables in process-global SQLAlchemy
metadata, so a second real memU service in one process cannot start).

Usage: python _memu_no_llm_e2e.py <tmp dir>   (exit 0 = every check passed)
"""

from __future__ import annotations

import asyncio
import hashlib
import sqlite3
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from nerve.agent.tools.handlers.memory import (
    conversation_history_handler,
    memorize_handler,
    memory_records_by_date_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.config import MemoryConfig, NerveConfig
from nerve.memory.memu_bridge import MemUBridge, _RefusingLLMClient


def _bag_of_words(text: str, dim: int = 384) -> np.ndarray:
    vec = np.zeros(dim, dtype=np.float32)
    for word in text.lower().split():
        vec[int(hashlib.md5(word.encode()).hexdigest(), 16) % dim] += 1.0
    return vec


class _FakeTextEmbedding:
    def __init__(self, model_name: str, cache_dir: str, threads: int) -> None:
        pass

    def embed(self, documents, batch_size: int = 256):
        for doc in documents:
            yield _bag_of_words(doc)


def _rows(db_path: Path, sql: str, params: tuple = ()) -> list[tuple]:
    db = sqlite3.connect(db_path)
    try:
        return db.execute(sql, params).fetchall()
    finally:
        db.close()


async def main(tmp_path: Path) -> None:
    module = types.ModuleType("fastembed")
    module.TextEmbedding = _FakeTextEmbedding
    sys.modules["fastembed"] = module
    refused: list[str] = []
    original = _RefusingLLMClient._refuse

    def _spy(self):
        refused.append(self.profile)
        return original(self)

    _RefusingLLMClient._refuse = _spy

    db_path = tmp_path / "memu.sqlite"
    config = NerveConfig()
    config.memory = MemoryConfig.from_dict({
        "sqlite_dsn": f"sqlite:///{db_path}",
        "embedder": "local",
        "extraction": "none",
        "skip_sessions": ["cron:nightly-report*"],
        "categories": [
            {"name": "work", "description": "work projects pull requests code reviews merged PR"},
            {"name": "personal", "description": "personal errands family home"},
        ],
    })
    config.anthropic_api_key = "test-key"
    config.timezone = "Australia/Sydney"
    bridge = MemUBridge(config)
    assert await bridge.initialize()

    async def _await(task):
        return await task

    if bridge._embed_backfill_task is not None:
        await bridge._submit(_await(bridge._embed_backfill_task))
    ctx = ToolContext(session_id="s-1", workspace=Path("/tmp/ws"), db=None, memory_bridge=bridge, config=config)
    try:
        await _checks(bridge, db_path, ctx, refused)
    finally:
        await bridge.shutdown()

async def _checks(bridge, db_path, ctx, refused) -> None:
    assert all(
        isinstance(bridge._service._llm_clients[name], _RefusingLLMClient)
        for name in bridge._service.llm_profiles.profiles if name != "embedding"
    )

    # A fact through the tool: stored as written, dated, put in a category.
    result = await memorize_handler(ctx, {
        "content": "On November 2, 2026, the user merged PR 4100 after code review", "memory_type": "event",
    })
    assert result.content[0]["text"].startswith("Memorized:")
    (item_id, summary, happened), = _rows(
        db_path, "SELECT id, summary, happened_at FROM memu_memory_items WHERE memory_type = 'event'",
    )
    assert summary == "On November 2, 2026, the user merged PR 4100 after code review"
    assert happened == "2026-11-02"
    linked = _rows(
        db_path,
        "SELECT c.name FROM memu_category_items ci JOIN memu_memory_categories c ON c.id = ci.category_id "
        "WHERE ci.item_id = ?", (item_id,),
    )
    assert linked == [("work",)]

    # The same fact again is reinforced, not stored twice.
    assert await bridge.memorize_fact("On November 2, 2026, the user merged PR 4100 after code review", "event")
    assert len(_rows(db_path, "SELECT id FROM memu_memory_items WHERE memory_type = 'event'")) == 1

    # An explicit date wins over the text; a bad type or date is refused.
    assert await bridge.memorize_fact("Release notes went out", "event", date="2026-10-31")
    assert _rows(db_path, "SELECT happened_at FROM memu_memory_items WHERE summary = 'Release notes went out'") == [
        ("2026-10-31",)
    ]
    bad = await memorize_handler(ctx, {"content": "x", "memory_type": "event", "date": "31/10/2026"})
    assert bad.content[0]["text"].startswith("Error: date must be YYYY-MM-DD")
    bad = await memorize_handler(ctx, {"content": "x", "memory_type": "gossip"})
    assert "memory_type must be one of" in bad.content[0]["text"]

    # A conversation: embedded chunks, local timestamps, first message as caption.
    messages = [
        {"role": "user", "content": "what is the status of the parser fix PR 4242",
         "created_at": "2026-11-03T03:19:00+00:00"},
        {"role": "assistant", "content": "PR 4242 is green and waits for review",
         "created_at": "2026-11-03T03:20:00+00:00"},
    ]
    assert await bridge.memorize_conversation("3f9a1c07", messages)
    chunks = _rows(
        db_path,
        "SELECT i.summary, i.happened_at, r.caption FROM memu_memory_items i "
        "JOIN memu_resources r ON r.id = i.resource_id WHERE i.memory_type = 'conversation'",
    )
    assert len(chunks) == 1
    text, happened, caption = chunks[0]
    assert text.startswith("user: what is the status of the parser fix")
    assert happened == "2026-11-03 14:19:00"  # Australia/Sydney
    assert caption == "what is the status of the parser fix PR 4242"

    # A skipped session is handled without storing anything.
    before = _rows(db_path, "SELECT count(*) FROM memu_memory_items")[0][0]
    assert await bridge.memorize_conversation("cron:nightly-report:20261103-1", messages)
    assert _rows(db_path, "SELECT count(*) FROM memu_memory_items")[0][0] == before

    # Recall: the fact, and the chunk labelled with its session.
    out = await bridge.recall("parser fix PR 4242 status", limit=5, category_limit=0)
    labelled = [m["summary"] for m in out if m["type"] == "conversation"]
    assert labelled and labelled[0].startswith("(2026-11-03 14:19, session 3f9a1c07) user: what is the status")
    out = await bridge.recall("merged PR 4100", limit=5, category_limit=0)
    assert out[0]["id"] == item_id

    # The date listing: facts, plus one line for the session — not the chunk.
    history = (await conversation_history_handler(ctx, {"date": "2026-11-02", "end_date": "2026-11-03"}))
    text = history.content[0]["text"]
    assert "merged PR 4100" in text
    assert "session 3f9a1c07: what is the status of the parser fix PR 4242" in text
    assert "user: what is the status" not in text
    records = await memory_records_by_date_handler(
        ctx, {"date": datetime.now(timezone.utc).date().isoformat()},
    )
    assert "[conversation]" not in records.content[0]["text"]

    # Update and delete without a model.
    assert await bridge.update_item(item_id, content="The user merged PR 4100 on November 2, 2026")
    assert _rows(db_path, "SELECT summary FROM memu_memory_items WHERE id = ?", (item_id,)) == [
        ("The user merged PR 4100 on November 2, 2026",)
    ]
    assert await bridge.delete_item(item_id)
    assert _rows(db_path, "SELECT count(*) FROM memu_memory_items WHERE id = ?", (item_id,)) == [(0,)]
    assert _rows(db_path, "SELECT count(*) FROM memu_category_items WHERE item_id = ?", (item_id,)) == [(0,)]

    # A workspace file: document chunks.
    doc = db_path.parent / "NOTES.md"
    doc.write_text("# Notes\n\nThe deploy runs on Tuesdays.\n\nBackups live in S3.", encoding="utf-8")
    assert await bridge.memorize_file(str(doc), modality="document")
    assert _rows(db_path, "SELECT count(*) FROM memu_memory_items WHERE memory_type = 'document'") == [(1,)]

    assert refused == []  # no model was ever asked


if __name__ == "__main__":
    asyncio.run(main(Path(sys.argv[1])))
    print("ok")
