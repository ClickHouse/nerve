"""Local embeddings (``memory.embedder: local``), hybrid recall, the FTS5
keyword index, the background embedding pass, and recall failures that are
reported instead of read as an empty memory."""

from __future__ import annotations

import json
import sqlite3
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pytest

from nerve.agent.tools.handlers.memory import memory_recall_handler
from nerve.agent.tools.registry import ToolContext
from nerve.config import LOCAL_EMBED_MODELS, MemoryConfig, NerveConfig
from nerve.memory.backend import MemoryBackendUnavailable, MemoryRecallFailed
from nerve.memory.memu_bridge import (
    _FTS_TABLE,
    MemUBridge,
    _as_the_user,
    _fts_match_expr,
    _fuse_recall_hits,
    _learn_user_name_terms,
    _LocalEmbeddingClient,
)

# --- helpers ---------------------------------------------------------------


def _make_config(tmp_path: Path, **memory: object) -> NerveConfig:
    config = NerveConfig()
    config.memory = MemoryConfig(sqlite_dsn=f"sqlite:///{tmp_path / 'memu.sqlite'}", **memory)
    config.anthropic_api_key = "test-key"
    return config


def _items_table(db_path: Path, rows: list[tuple[str, str]] = ()) -> None:
    db = sqlite3.connect(db_path)
    db.execute(
        "CREATE TABLE memu_memory_items (id TEXT PRIMARY KEY, summary TEXT NOT NULL, "
        "embedding_json TEXT, updated_at TEXT)"
    )
    db.executemany(
        "INSERT INTO memu_memory_items (id, summary, updated_at) VALUES (?, ?, '2026-01-01T00:00:00')",
        rows,
    )
    db.commit()
    db.close()


def _fts_rows(db_path: Path) -> dict[str, str]:
    db = sqlite3.connect(db_path)
    rows = dict(db.execute(f"SELECT item_id, summary FROM {_FTS_TABLE}").fetchall())
    db.close()
    return rows


class _FakeTextEmbedding:
    """Stands in for fastembed.TextEmbedding: a vector from the text's
    length and first letter, and a record of every call."""

    loads = 0
    calls: list[tuple[list[str], int]] = []

    def __init__(self, model_name: str, cache_dir: str, threads: int) -> None:
        type(self).loads += 1
        self.model_name = model_name

    def embed(self, documents: list[str], batch_size: int = 256):
        type(self).calls.append((list(documents), batch_size))
        for doc in documents:
            yield np.array([len(doc), ord(doc[0]), 1.0, 2.0], dtype=np.float32)


@pytest.fixture
def fake_fastembed(monkeypatch):
    _FakeTextEmbedding.loads = 0
    _FakeTextEmbedding.calls = []
    module = types.ModuleType("fastembed")
    module.TextEmbedding = _FakeTextEmbedding
    monkeypatch.setitem(sys.modules, "fastembed", module)
    return _FakeTextEmbedding


def _ctx(bridge) -> ToolContext:
    return ToolContext(session_id="s-1", workspace=Path("/tmp/ws"), db=None, memory_bridge=bridge, config=None)


# --- config ----------------------------------------------------------------


def test_config_reads_embedder_and_local_model() -> None:
    cfg = MemoryConfig.from_dict({"embedder": " Local ", "local_model": "BGE-Base"})
    assert cfg.embedder == "local"
    assert cfg.local_model == "bge-base"
    assert cfg.local_model_id == LOCAL_EMBED_MODELS["bge-base"][0]


def test_config_defaults_keep_the_old_rule() -> None:
    cfg = MemoryConfig.from_dict({})
    assert cfg.embedder == ""
    assert cfg.local_model == "bge-small"
    assert cfg.embedder_kind("sk-key") == "openai"
    assert cfg.embedder_kind("") == "none"
    assert MemoryConfig.from_dict({"embedder": "local"}).embedder_kind("sk-key") == "local"
    assert MemoryConfig.from_dict({"embedder": "none"}).embedder_kind("sk-key") == "none"


@pytest.mark.parametrize(
    ("memory", "openai_key", "message"),
    [
        ({"embedder": "ollama"}, "", "memory.embedder must be one of"),
        ({"embedder": "local", "local_model": "e5-huge"}, "", "memory.local_model must be one of"),
        ({"embedder": "openai"}, "", "needs openai_api_key"),
    ],
)
def test_config_refuses_bad_embedder_settings(memory, openai_key, message) -> None:
    config = NerveConfig()
    config.memory = MemoryConfig.from_dict(memory)
    config.openai_api_key = openai_key
    with pytest.raises(ValueError, match=message):
        config._validate_memory_backend()


def test_dedup_threshold_is_stricter_for_a_local_model_unless_set() -> None:
    assert MemoryConfig.from_dict({}).dedup_threshold("openai") == 0.85
    assert MemoryConfig.from_dict({}).dedup_threshold("local") == 0.96
    assert MemoryConfig.from_dict({"semantic_dedup_threshold": 0.9}).dedup_threshold("local") == 0.9
    assert MemoryConfig.from_dict({"semantic_dedup_threshold": 0}).dedup_threshold("local") == 0


def test_values_differ_tells_sibling_facts_apart() -> None:
    from nerve.memory.memu_bridge import _values_differ

    assert _values_differ("3 failures, retry blocked", "4 failures, retry blocked")
    assert _values_differ(
        "Cherry-pick PR #71918 targets release/26.6", "Cherry-pick PR #71917 targets release/26.4",
    )
    assert _values_differ("set up on September 28, 2026", "set up on September 15, 2026")
    assert _values_differ("check at 15:00 AEDT", "check at 00:00 AEDT")
    assert not _values_differ(
        "The user manages @youniversed, posting awe and wonder", "The user posts awe content to @youniversed",
    )
    assert not _values_differ("AEDT is a timezone", "AEDT is a timezone abbreviation")


def test_config_ignores_local_model_on_a_loom_instance() -> None:
    config = NerveConfig()
    config.memory = MemoryConfig.from_dict({"backend": "loom", "embedder": "local", "local_model": "nope"})
    config._validate_memory_backend()  # does not raise


# --- keyword query, user name, fusion --------------------------------------


def test_fts_match_expr_quotes_words_and_drops_noise() -> None:
    assert _fts_match_expr("what makes Alice angry", {"alice"}) == '"angry"'
    assert _fts_match_expr("#4100 and 0c1d2e3f") == '"4100" OR "0c1d2e3f"'
    assert _fts_match_expr("what is the") == ""


def test_fts_match_expr_is_always_valid_fts5() -> None:
    db = sqlite3.connect(":memory:")
    db.execute("CREATE VIRTUAL TABLE f USING fts5(s)")
    db.execute("INSERT INTO f VALUES ('a NEAR b \"quoted\" (x:y) -minus star*')")
    expr = _fts_match_expr('he said "quoted" (x:y) -minus star* NEAR AND OR')
    assert db.execute("SELECT count(*) FROM f WHERE f MATCH ?", (expr,)).fetchone()[0] == 1


def test_learn_user_name_needs_two_statements() -> None:
    summaries = [
        "The user's name is Alice",
        "The user is named Alice Smith",
        "The user is named Alice Smith",
        "The user's name is Bob",
        "Alice likes terse answers",
        None,
    ]
    assert _learn_user_name_terms(summaries) == {"alice", "smith"}


def test_as_the_user_rewrites_the_name_only() -> None:
    names = frozenset({"alice", "smith"})
    assert _as_the_user("what makes Alice angry", names) == "what makes the user angry"
    assert _as_the_user("Alice Smith's PRs", names) == "the user's PRs"
    assert _as_the_user("aliceville news", names) == "aliceville news"
    assert _as_the_user("parser fix PR", names) == "parser fix PR"
    assert _as_the_user("what makes Alice angry", frozenset()) == "what makes Alice angry"


def _item(age_days: float = 0, reinforced: int = 1) -> SimpleNamespace:
    stamp = (datetime(2026, 11, 3, tzinfo=timezone.utc) - timedelta(days=age_days)).isoformat()
    return SimpleNamespace(extra={"reinforcement_count": reinforced}, updated_at=stamp, created_at=stamp)


def test_fusion_prefers_rows_found_by_several_legs() -> None:
    items = {i: _item() for i in "abcd"}
    now = datetime(2026, 11, 3, tzinfo=timezone.utc)
    fused = _fuse_recall_hits([["a", "b", "c"], ["c", "d"]], items, now=now)
    assert [i for i, _ in fused][0] == "c"
    assert {i for i, _ in fused} == {"a", "b", "c", "d"}


def test_fusion_drops_unknown_ids_and_breaks_ties_by_recency_and_reinforcement() -> None:
    now = datetime(2026, 11, 3, tzinfo=timezone.utc)
    items = {"old": _item(age_days=720), "new": _item(age_days=1), "again": _item(age_days=720, reinforced=8)}
    # Same rank in mirrored legs, so only the tie-breakers differ.
    fused = dict(_fuse_recall_hits([["old", "new", "ghost"], ["new", "old"]], items, now=now))
    assert "ghost" not in fused
    assert fused["new"] > fused["old"]
    fused = dict(_fuse_recall_hits([["old"], ["again"]], items, now=now))
    assert fused["again"] > fused["old"]


# --- local embedding client ------------------------------------------------


@pytest.mark.asyncio
async def test_local_client_keeps_order_and_normalizes(tmp_path, fake_fastembed) -> None:
    client = _LocalEmbeddingClient(model_id="m", dim=4, cache_dir=str(tmp_path / "models"), threads=1)
    texts = ["ccc long text", "", "a", "bb"]
    out = await client.embed(texts)
    assert len(out) == 4
    assert out[1] == [0.0, 0.0, 0.0, 0.0]  # empty input: zero vector, never matches
    for text, vec in zip(texts, out, strict=True):
        if text:
            raw = np.array([len(text), ord(text[0]), 1.0, 2.0])
            np.testing.assert_allclose(vec, raw / np.linalg.norm(raw), rtol=1e-6)
    (docs, batch_size), = fake_fastembed.calls
    assert docs == ["a", "bb", "ccc long text"]  # shortest first, empties skipped
    assert batch_size == _LocalEmbeddingClient._BATCH_SIZE == 4
    await client.embed(["x"])
    assert fake_fastembed.loads == 1  # loaded once


@pytest.mark.asyncio
async def test_local_client_does_not_chat(tmp_path) -> None:
    client = _LocalEmbeddingClient(model_id="m", dim=4, cache_dir=str(tmp_path))
    with pytest.raises(NotImplementedError):
        await client.chat("hi")


# --- FTS5 index --------------------------------------------------------------


def test_fts_index_fills_and_follows_the_items_table(tmp_path) -> None:
    db_path = tmp_path / "memu.sqlite"
    _items_table(db_path, [("i1", "Parser fix is PR 4242"), ("i2", "The user likes tea")])
    dsn = f"sqlite:///{db_path}"
    MemUBridge._ensure_fts_index(dsn)
    assert _fts_rows(db_path) == {"i1": "Parser fix is PR 4242", "i2": "The user likes tea"}

    db = sqlite3.connect(db_path)
    db.execute("INSERT INTO memu_memory_items (id, summary) VALUES ('i3', '0c1d2e3f adds a test')")
    db.execute("UPDATE memu_memory_items SET summary = 'The user likes coffee' WHERE id = 'i2'")
    db.execute("UPDATE memu_memory_items SET embedding_json = '[1]' WHERE id = 'i1'")
    db.execute("DELETE FROM memu_memory_items WHERE id = 'i1'")
    db.commit()
    db.close()
    MemUBridge._ensure_fts_index(dsn)  # idempotent: no rebuild, no duplicates
    assert _fts_rows(db_path) == {"i2": "The user likes coffee", "i3": "0c1d2e3f adds a test"}


def test_fts_search_ranks_and_survives_a_missing_index(tmp_path) -> None:
    db_path = tmp_path / "memu.sqlite"
    _items_table(db_path, [
        ("i1", "Parser fix is PR 4242"),
        ("i2", "Parser parser flaky shards on build 8051"),
        ("i3", "The user likes tea"),
    ])
    bridge = MemUBridge(_make_config(tmp_path))
    assert bridge._fts_search("parser flaky", 10) == []  # no index yet
    MemUBridge._ensure_fts_index(f"sqlite:///{db_path}")
    hits = bridge._fts_search("parser flaky", 10)
    assert hits[0] == "i2" and set(hits) == {"i1", "i2"}
    assert bridge._fts_search("what is the", 10) == []


# --- hybrid recall -------------------------------------------------------------


class _Repo:
    def __init__(self, items: dict) -> None:
        self.items = items


def _hybrid_bridge(tmp_path: Path, summaries: dict[str, str], vectors: dict[str, list[float]]) -> MemUBridge:
    db_path = tmp_path / "memu.sqlite"
    _items_table(db_path, list(summaries.items()))
    MemUBridge._ensure_fts_index(f"sqlite:///{db_path}")
    bridge = MemUBridge(_make_config(tmp_path, embedder="local"))
    bridge._available = True
    bridge._service = MagicMock()
    stamp = datetime.now(timezone.utc).isoformat()
    items = {
        item_id: SimpleNamespace(
            id=item_id, summary=text, memory_type="knowledge", extra={}, created_at=stamp,
            updated_at=stamp, embedding=np.asarray(vectors[item_id], dtype=np.float32),
        )
        for item_id, text in summaries.items()
    }
    bridge._service.database.memory_item_repo = _Repo(items)
    bridge._service.database.memory_category_repo.categories = {
        "c1": SimpleNamespace(id="c1", name="work", description="Work and PRs", summary="", embedding=np.array([1.0, 0, 0])),
        "c2": SimpleNamespace(id="c2", name="people", description="People", summary="", embedding=np.array([0, 1.0, 0])),
    }
    return bridge


@pytest.mark.asyncio
async def test_hybrid_recall_fuses_vectors_and_keywords(tmp_path) -> None:
    summaries = {
        "i1": "PR 4242 fixes the parser slowdown",
        "i2": "The user collaborates with alesapin on reviews",
        "i3": "0c1d2e3f adds test 05335",
    }
    vectors = {"i1": [1.0, 0, 0], "i2": [0, 1.0, 0], "i3": [0, 0, 1.0]}
    bridge = _hybrid_bridge(tmp_path, summaries, vectors)
    embed = AsyncMock(return_value=[[0.9, 0.1, 0.0]])
    bridge._service._get_llm_client.return_value = SimpleNamespace(embed=embed)

    out = await bridge.recall("0c1d2e3f", limit=2, category_limit=1)

    items = [m for m in out if m["type"] != "category"]
    cats = [m for m in out if m["type"] == "category"]
    # i3 only matches by keyword, i1 only by vector: both make the cut.
    assert {m["id"] for m in items} == {"i1", "i3"}
    assert cats == [{"id": "cat:c1", "type": "category", "name": "work", "summary": "Work and PRs"}]
    bridge._service.retrieve.assert_not_called()  # memU's LLM retrieve is not used


@pytest.mark.asyncio
async def test_hybrid_recall_reads_the_users_name_as_the_user(tmp_path) -> None:
    summaries = {
        "n1": "The user's name is Alice",
        "n2": "The user's name is Alice",
        "f1": "The user becomes frustrated by unverified claims",
    }
    vectors = {"n1": [1.0, 0, 0], "n2": [1.0, 0, 0], "f1": [0, 1.0, 0]}
    bridge = _hybrid_bridge(tmp_path, summaries, vectors)
    embed = AsyncMock(return_value=[[1.0, 0, 0], [0, 1.0, 0]])
    bridge._service._get_llm_client.return_value = SimpleNamespace(embed=embed)

    out = await bridge.recall("what makes Alice angry", limit=3, category_limit=0)

    embed.assert_awaited_once_with(["what makes Alice angry", "what makes the user angry"])
    assert "f1" in {m["id"] for m in out}


# --- recall failures -----------------------------------------------------------


def _llm_bridge(tmp_path: Path) -> MemUBridge:
    bridge = MemUBridge(_make_config(tmp_path))
    bridge._available = True
    bridge._service = MagicMock()
    return bridge


@pytest.mark.asyncio
async def test_a_failed_recall_is_not_an_empty_memory(tmp_path) -> None:
    bridge = _llm_bridge(tmp_path)
    bridge._service.retrieve = AsyncMock(side_effect=ValueError("prompt is too long: 326040 tokens > 200000"))
    with pytest.raises(MemoryRecallFailed, match="prompt is too long"):
        await bridge.recall("anything")
    assert bridge.metrics.ops["recall"].error_count == 1


@pytest.mark.asyncio
async def test_an_outage_is_still_reported_as_an_outage(tmp_path, monkeypatch) -> None:
    import nerve.memory.memu_bridge as mb

    bridge = _llm_bridge(tmp_path)
    bridge._service.retrieve = AsyncMock(side_effect=RuntimeError("503 overloaded"))
    monkeypatch.setattr(mb, "_is_transient_llm_error", lambda exc: True)
    with pytest.raises(MemoryBackendUnavailable) as caught:
        await bridge.recall("anything")
    assert not isinstance(caught.value, MemoryRecallFailed)


@pytest.mark.asyncio
async def test_memory_recall_tool_names_a_failed_recall(tmp_path) -> None:
    bridge = _llm_bridge(tmp_path)
    bridge._service.retrieve = AsyncMock(side_effect=ValueError("prompt is too long"))
    result = await memory_recall_handler(_ctx(bridge), {"query": "x"})
    text = result.content[0]["text"]
    assert "MEMORY RECALL FAILED" in text
    assert "No relevant memories found" not in text


# --- background embedding pass -------------------------------------------------


@pytest.mark.asyncio
async def test_backfill_embeds_missing_rows_without_touching_updated_at(tmp_path, fake_fastembed) -> None:
    db_path = tmp_path / "memu.sqlite"
    _items_table(db_path, [("i1", "first fact"), ("i2", "second, longer fact")])
    db = sqlite3.connect(db_path)
    db.execute(
        "CREATE TABLE memu_memory_categories (id TEXT PRIMARY KEY, name TEXT, description TEXT, "
        "summary TEXT, embedding_json TEXT, updated_at TEXT)"
    )
    db.execute("INSERT INTO memu_memory_categories VALUES ('c1', 'work', 'Work', '', NULL, '2026-01-01T00:00:00')")
    db.commit()
    db.close()

    bridge = MemUBridge(_make_config(tmp_path, embedder="local"))
    bridge._local_embedder = _LocalEmbeddingClient(model_id="m", dim=4, cache_dir=str(tmp_path), threads=1)
    bridge._service = MagicMock()
    item_repo = SimpleNamespace(
        _memory_item_model=SimpleNamespace(__tablename__="memu_memory_items"),
        items={
            "i1": SimpleNamespace(summary="first fact", memory_type="knowledge", embedding=None),
            "i2": SimpleNamespace(summary="second, longer fact", memory_type="event", embedding=None),
        },
    )
    cat_repo = SimpleNamespace(
        _memory_category_model=SimpleNamespace(__tablename__="memu_memory_categories"),
        categories={"c1": SimpleNamespace(name="work", description="Work", summary="", embedding=None)},
    )
    bridge._service.database.memory_item_repo = item_repo
    bridge._service.database.memory_category_repo = cat_repo

    await bridge._backfill_embeddings()

    assert bridge._embed_backfill["state"] == "done"
    assert bridge._embed_backfill["done"] == 3
    db = sqlite3.connect(db_path)
    rows = dict(db.execute("SELECT id, embedding_json FROM memu_memory_items").fetchall())
    stamps = {r[0] for r in db.execute("SELECT updated_at FROM memu_memory_items")}
    cat_json = db.execute("SELECT embedding_json FROM memu_memory_categories").fetchone()[0]
    db.close()
    assert all(len(json.loads(v)) == 4 for v in rows.values())
    assert stamps == {"2026-01-01T00:00:00"}
    assert len(json.loads(cat_json)) == 4
    assert item_repo.items["i1"].embedding is not None
    assert cat_repo.categories["c1"].embedding is not None
    hits = item_repo._nerve_vec_index.search(np.asarray(json.loads(rows["i2"])), 1)
    assert hits[0][0] == "i2"


@pytest.mark.asyncio
async def test_backfill_redoes_vectors_from_another_model(tmp_path, fake_fastembed) -> None:
    db_path = tmp_path / "memu.sqlite"
    _items_table(db_path, [("i1", "same width, other model"), ("i2", "other width")])
    bridge = MemUBridge(_make_config(tmp_path, embedder="local"))
    bridge._local_embedder = _LocalEmbeddingClient(model_id="m", dim=4, cache_dir=str(tmp_path), threads=1)
    bridge._embedding_model_marker("text-embedding-3-small")
    bridge._service = MagicMock()
    item_repo = SimpleNamespace(
        _memory_item_model=SimpleNamespace(__tablename__="memu_memory_items"),
        items={
            "i1": SimpleNamespace(summary="same width, other model", memory_type="knowledge", embedding=np.ones(4)),
            "i2": SimpleNamespace(summary="other width", memory_type="knowledge", embedding=np.ones(1536)),
        },
    )
    bridge._service.database.memory_item_repo = item_repo
    bridge._service.database.memory_category_repo.categories = {}

    await bridge._backfill_embeddings()

    assert bridge._embed_backfill["done"] == 2
    assert bridge._embedding_model_marker() == bridge.config.memory.local_model_id
    assert all(len(item.embedding) == 4 and not np.allclose(item.embedding, 1) for item in item_repo.items.values())

    # Recorded model now matches: a restart re-embeds nothing.
    fake_fastembed.calls.clear()
    await bridge._backfill_embeddings()
    assert bridge._embed_backfill["state"] == "done" and fake_fastembed.calls == []


@pytest.mark.asyncio
async def test_backfill_with_nothing_missing_is_a_no_op(tmp_path) -> None:
    bridge = MemUBridge(_make_config(tmp_path, embedder="local"))
    bridge._service = MagicMock()
    bridge._service.database.memory_item_repo.items = {
        "i1": SimpleNamespace(summary="x", memory_type="knowledge", embedding=np.ones(4)),
    }
    bridge._service.database.memory_category_repo.categories = {}
    await bridge._backfill_embeddings()
    assert bridge._embed_backfill == {"state": "done", "missing": 0}
    bridge._service._get_llm_client.assert_not_called()


def test_embedder_kind_drives_has_embeddings(tmp_path) -> None:
    assert MemUBridge(_make_config(tmp_path, embedder="local"))._has_embeddings
    assert not MemUBridge(_make_config(tmp_path))._has_embeddings
    keyed = _make_config(tmp_path)
    keyed.openai_api_key = "sk-test"
    assert MemUBridge(keyed)._has_embeddings


def test_hybrid_recall_needs_an_explicit_embedder(tmp_path) -> None:
    # An install with openai_api_key and no memory.embedder keeps memU's own
    # recall and gets no startup backfill: nothing changes for it.
    keyed = _make_config(tmp_path)
    keyed.openai_api_key = "sk-test"
    assert not MemUBridge(keyed)._hybrid_recall
    explicit = _make_config(tmp_path, embedder="openai")
    explicit.openai_api_key = "sk-test"
    assert MemUBridge(explicit)._hybrid_recall
    assert MemUBridge(_make_config(tmp_path, embedder="local"))._hybrid_recall
    assert not MemUBridge(_make_config(tmp_path, embedder="none"))._hybrid_recall
    # extraction: none stores transcript chunks, which need the hybrid path
    # (labels, the chunk cap): it goes by the embedder in effect.
    no_llm = _make_config(tmp_path, extraction="none")
    no_llm.openai_api_key = "sk-test"
    assert MemUBridge(no_llm)._hybrid_recall
    assert not MemUBridge(_make_config(tmp_path, extraction="none"))._hybrid_recall


def test_config_refuses_a_local_embedder_without_fastembed(monkeypatch) -> None:
    import nerve.config as config_module

    monkeypatch.setattr(config_module, "_fastembed_installed", lambda: False)
    config = NerveConfig()
    config.memory = MemoryConfig.from_dict({"embedder": "local"})
    with pytest.raises(ValueError, match="local-embeddings"):
        config._validate_memory_backend()
    monkeypatch.setattr(config_module, "_fastembed_installed", lambda: True)
    config._validate_memory_backend()
