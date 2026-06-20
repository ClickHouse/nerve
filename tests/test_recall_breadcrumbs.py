"""Tests for recall category breadcrumbs + memory_expand_category drill-down.

memU categories are rolled-up topic *documents* (often 5–20KB). recall must
surface them as short navigable breadcrumbs, never dump the document — that
blows past the harness tool-output limit. These tests lock in that contract
and the drill-down path that replaces the lost detail.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.tools.handlers.memory import (
    _clip_to_budget,
    memory_expand_category_handler,
    memory_recall_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.config import MemoryConfig, NerveConfig
from nerve.memory.memu_bridge import MemUBridge, _category_breadcrumb


def _make_config(tmp_path: Path) -> NerveConfig:
    config = NerveConfig()
    config.memory = MemoryConfig(sqlite_dsn=f"sqlite:///{tmp_path / 'memu.sqlite'}")
    config.anthropic_api_key = "test-key"
    return config


def _stub_bridge(config: NerveConfig) -> MemUBridge:
    """A MemUBridge marked available with a mockable _service."""
    bridge = MemUBridge(config)
    bridge._available = True
    bridge._service = MagicMock()
    return bridge


def _ctx(bridge) -> ToolContext:
    return ToolContext(
        session_id="s-1",
        workspace=Path("/tmp/ws"),
        db=None,
        memory_bridge=bridge,
        config=None,
    )


# --- pure helpers ----------------------------------------------------------


def test_breadcrumb_prefers_description() -> None:
    crumb = _category_breadcrumb(
        name="preferences",
        description="Communication style and tool preferences",
        summary="# preferences\n\n## A\n- huge\n" + ("x" * 5000),
    )
    assert crumb == "Communication style and tool preferences"
    assert len(crumb) < 200


def test_breadcrumb_falls_back_to_first_summary_line() -> None:
    crumb = _category_breadcrumb(
        name="agent_ops",
        description="",
        summary="# agent_ops\n\n- First real fact [ref:abc123]\n- second",
    )
    # header '#' skipped, first bullet used, [ref:] stripped
    assert "First real fact" in crumb
    assert "ref:" not in crumb
    assert "#" not in crumb


def test_breadcrumb_truncates_long_text() -> None:
    crumb = _category_breadcrumb("c", "y" * 500, "")
    assert len(crumb) <= 200
    assert crumb.endswith("…")


def test_clip_to_budget_passes_small_text() -> None:
    assert _clip_to_budget("hello", max_bytes=100) == "hello"


def test_clip_to_budget_truncates_large_text() -> None:
    out = _clip_to_budget("a" * 50_000, max_bytes=1000)
    assert len(out.encode("utf-8")) <= 1000 + 64
    assert "truncated" in out


# --- bridge.recall() -------------------------------------------------------


@pytest.mark.asyncio
async def test_recall_categories_become_breadcrumbs(tmp_path) -> None:
    config = _make_config(tmp_path)
    bridge = _stub_bridge(config)

    fat_summary = "# preferences\n\n" + ("- a giant bullet\n" * 2000)  # ~30KB
    bridge._service.retrieve = AsyncMock(return_value={
        "items": [
            {"id": "i1", "memory_type": "profile", "summary": "Alice lives in Metropolis"},
            {"id": "i2", "memory_type": "behavior", "summary": "Prefers dark mode"},
        ],
        "categories": [
            {
                "id": "c1",
                "name": "preferences",
                "description": "How things should be done",
                "summary": fat_summary,
            },
        ],
    })

    out = await bridge.recall("prefs", limit=10, category_limit=5)

    items = [m for m in out if m["type"] != "category"]
    cats = [m for m in out if m["type"] == "category"]
    assert len(items) == 2
    assert items[0]["summary"] == "Alice lives in Metropolis"  # full content kept
    assert len(cats) == 1
    cat = cats[0]
    assert cat["id"] == "cat:c1"
    assert cat["name"] == "preferences"
    assert cat["summary"] == "How things should be done"  # breadcrumb, not the doc
    # The fat document must not leak anywhere into the result.
    assert "giant bullet" not in repr(out)


@pytest.mark.asyncio
async def test_recall_caps_items_and_categories(tmp_path) -> None:
    config = _make_config(tmp_path)
    bridge = _stub_bridge(config)
    bridge._service.retrieve = AsyncMock(return_value={
        "items": [
            {"id": f"i{n}", "memory_type": "knowledge", "summary": f"fact {n}"}
            for n in range(20)
        ],
        "categories": [
            {"id": f"c{n}", "name": f"cat{n}", "description": f"desc {n}", "summary": "x"}
            for n in range(20)
        ],
    })

    out = await bridge.recall("q", limit=3, category_limit=2)
    items = [m for m in out if m["type"] != "category"]
    cats = [m for m in out if m["type"] == "category"]
    assert len(items) == 3
    assert len(cats) == 2
    # items come before categories
    assert out[0]["type"] != "category"


# --- bridge.expand_category() ---------------------------------------------


def _create_category_schema(db_path: str) -> None:
    db = sqlite3.connect(db_path)
    db.executescript(
        """
        CREATE TABLE memu_memory_items (
            id TEXT PRIMARY KEY, memory_type TEXT, summary TEXT,
            created_at TEXT, updated_at TEXT
        );
        CREATE TABLE memu_memory_categories (
            id TEXT PRIMARY KEY, name TEXT, description TEXT, summary TEXT
        );
        CREATE TABLE memu_category_items (
            id TEXT PRIMARY KEY, item_id TEXT, category_id TEXT
        );
        """
    )
    db.commit()
    db.close()


def _seed_category(db_path: str) -> None:
    db = sqlite3.connect(db_path)
    db.execute(
        "INSERT INTO memu_memory_categories (id, name, description) VALUES (?,?,?)",
        ("cat-pref", "preferences", "How things should be done"),
    )
    rows = [
        ("it-1", "profile", "Likes the color teal", "2026-06-01 10:00:00"),
        ("it-2", "behavior", "Prefers dark mode", "2026-06-02 10:00:00"),
        ("it-3", "profile", "Drinks black coffee", "2026-05-30 10:00:00"),
    ]
    for iid, mt, summ, upd in rows:
        db.execute(
            "INSERT INTO memu_memory_items (id, memory_type, summary, created_at, updated_at) "
            "VALUES (?,?,?,?,?)",
            (iid, mt, summ, upd, upd),
        )
        db.execute(
            "INSERT INTO memu_category_items (id, item_id, category_id) VALUES (?,?,?)",
            (f"link-{iid}", iid, "cat-pref"),
        )
    db.commit()
    db.close()


@pytest.mark.asyncio
async def test_expand_category_returns_recent_items(tmp_path) -> None:
    config = _make_config(tmp_path)
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
    _create_category_schema(db_path)
    _seed_category(db_path)

    bridge = _stub_bridge(config)
    result = await bridge.expand_category("cat:cat-pref", limit=2)

    assert result["name"] == "preferences"
    assert result["total"] == 3
    assert len(result["items"]) == 2
    # most-recent-first: it-2 (Jun 2) then it-1 (Jun 1)
    assert [i["id"] for i in result["items"]] == ["it-2", "it-1"]


@pytest.mark.asyncio
async def test_expand_category_keyword_filter(tmp_path) -> None:
    config = _make_config(tmp_path)
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
    _create_category_schema(db_path)
    _seed_category(db_path)

    bridge = _stub_bridge(config)
    result = await bridge.expand_category("cat-pref", query="coffee", limit=10)
    assert [i["id"] for i in result["items"]] == ["it-3"]


@pytest.mark.asyncio
async def test_expand_category_unknown_id(tmp_path) -> None:
    config = _make_config(tmp_path)
    db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
    _create_category_schema(db_path)
    _seed_category(db_path)

    bridge = _stub_bridge(config)
    result = await bridge.expand_category("cat:nope")
    assert result["name"] is None
    assert result["items"] == []


# --- handlers --------------------------------------------------------------


@pytest.mark.asyncio
async def test_recall_handler_renders_two_sections(tmp_path) -> None:
    bridge = MagicMock()
    bridge.available = True
    bridge.recall = AsyncMock(return_value=[
        {"id": "i1", "type": "profile", "summary": "Alice lives in Metropolis"},
        {"id": "cat:c1", "type": "category", "name": "preferences",
         "summary": "How things should be done"},
    ])
    result = await memory_recall_handler(_ctx(bridge), {"query": "x"})
    text = result.content[0]["text"]
    assert "Recalled 1 memories" in text
    assert "1 related topics" in text
    assert "Alice lives in Metropolis" in text
    assert "memory_expand_category" in text
    assert "cat:c1" in text


@pytest.mark.asyncio
async def test_recall_handler_passes_category_limit(tmp_path) -> None:
    bridge = MagicMock()
    bridge.available = True
    bridge.recall = AsyncMock(return_value=[])
    await memory_recall_handler(_ctx(bridge), {"query": "x", "category_limit": 2})
    _, kwargs = bridge.recall.call_args
    assert kwargs["category_limit"] == 2


@pytest.mark.asyncio
async def test_expand_category_handler(tmp_path) -> None:
    bridge = MagicMock()
    bridge.available = True
    bridge.expand_category = AsyncMock(return_value={
        "name": "preferences",
        "total": 5,
        "items": [{"id": "it-1", "type": "profile", "summary": "Likes the color teal"}],
    })
    result = await memory_expand_category_handler(
        _ctx(bridge), {"category_id": "cat:cat-pref"}
    )
    text = result.content[0]["text"]
    assert "preferences" in text
    assert "1 of 5" in text
    assert "Likes the color teal" in text


@pytest.mark.asyncio
async def test_expand_category_handler_requires_id(tmp_path) -> None:
    bridge = MagicMock()
    bridge.available = True
    result = await memory_expand_category_handler(_ctx(bridge), {"category_id": ""})
    assert result.is_error
