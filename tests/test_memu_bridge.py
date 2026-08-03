"""Tests for nerve.memory.memu_bridge — event date resolution & knowledge filtering."""

import asyncio
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest
import pytest_asyncio

from nerve.config import MemoryConfig, NerveConfig
from nerve.memory.memu_bridge import (
    MemoryBackendUnavailable,
    MemUBridge,
    _KNOWLEDGE_CUSTOM_RULES,
    _KNOWLEDGE_CUSTOM_EXAMPLES,
    _SEMANTIC_DEDUP_THRESHOLD,
    _is_sqlite_locked_error,
)


def _make_config(tmp_path: Path) -> NerveConfig:
    """Create a minimal NerveConfig pointing at a temp SQLite DB."""
    db_path = tmp_path / "memu.sqlite"
    config = NerveConfig()
    config.memory = MemoryConfig(
        sqlite_dsn=f"sqlite:///{db_path}",
    )
    config.anthropic_api_key = "test-key"
    return config


def _create_memu_schema(db_path: str) -> None:
    """Create the minimal memu tables needed for date resolution tests."""
    db = sqlite3.connect(db_path)
    db.execute("""
        CREATE TABLE IF NOT EXISTS memu_memory_items (
            id TEXT PRIMARY KEY,
            resource_id TEXT,
            memory_type TEXT NOT NULL,
            summary TEXT NOT NULL,
            embedding_json TEXT,
            happened_at TEXT,
            extra TEXT DEFAULT '{}',
            created_at TEXT,
            updated_at TEXT
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS memu_resources (
            id TEXT PRIMARY KEY,
            url TEXT,
            modality TEXT,
            local_path TEXT,
            caption TEXT,
            embedding_json TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    db.commit()
    db.close()


def _insert_items(db_path: str, items: list[dict]) -> None:
    """Insert test memory items into the DB."""
    db = sqlite3.connect(db_path)
    for item in items:
        db.execute(
            "INSERT INTO memu_memory_items (id, resource_id, memory_type, summary, happened_at, extra, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                item["id"],
                item.get("resource_id", "res-1"),
                item["memory_type"],
                item["summary"],
                item.get("happened_at"),
                json.dumps(item.get("extra", {})),
                item.get("created_at"),
            ),
        )
    db.commit()
    db.close()


def _read_items(db_path: str) -> dict[str, dict]:
    """Read all items from DB as a dict keyed by id."""
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    rows = db.execute("SELECT * FROM memu_memory_items").fetchall()
    db.close()
    return {r["id"]: dict(r) for r in rows}


class TestResolveEventDatesSync:
    """Test _resolve_event_dates_sync with a real SQLite DB."""

    def test_events_get_llm_resolved_dates(self, tmp_path):
        config = _make_config(tmp_path)
        db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
        _create_memu_schema(db_path)
        _insert_items(db_path, [
            {"id": "evt-1", "memory_type": "event", "summary": "The user went hiking on February 5, 2026"},
            {"id": "evt-2", "memory_type": "event", "summary": "On Feb 10, the user scheduled a dentist appointment for March 15, 2026"},
        ])

        bridge = MemUBridge(config)
        fake_llm_result = {"evt-1": "2026-02-05", "evt-2": "2026-02-10"}

        with patch.object(bridge, "_resolve_dates_via_llm", return_value=fake_llm_result):
            bridge._resolve_event_dates_sync("2026-02-10T14:00:00")

        items = _read_items(db_path)
        assert items["evt-1"]["happened_at"] == "2026-02-05"
        assert items["evt-2"]["happened_at"] == "2026-02-10"

    def test_non_events_stay_null(self, tmp_path):
        config = _make_config(tmp_path)
        db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
        _create_memu_schema(db_path)
        _insert_items(db_path, [
            {"id": "prof-1", "memory_type": "profile", "summary": "The user works at Acme Corp"},
            {"id": "know-1", "memory_type": "knowledge", "summary": "PostgreSQL supports UPSERT operations"},
            {"id": "beh-1", "memory_type": "behavior", "summary": "The user prefers dark mode"},
        ])

        bridge = MemUBridge(config)
        bridge._resolve_event_dates_sync("2026-02-27T10:00:00")

        items = _read_items(db_path)
        assert items["prof-1"]["happened_at"] is None
        assert items["know-1"]["happened_at"] is None
        assert items["beh-1"]["happened_at"] is None

    def test_mentioned_at_set_on_all_items(self, tmp_path):
        config = _make_config(tmp_path)
        db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
        _create_memu_schema(db_path)
        _insert_items(db_path, [
            {"id": "evt-1", "memory_type": "event", "summary": "User went hiking on Feb 5"},
            {"id": "prof-1", "memory_type": "profile", "summary": "The user works at Acme Corp"},
        ])

        bridge = MemUBridge(config)
        conv_ts = "2026-02-27T10:00:00"

        with patch.object(bridge, "_resolve_dates_via_llm", return_value={"evt-1": "2026-02-05"}):
            bridge._resolve_event_dates_sync(conv_ts)

        items = _read_items(db_path)
        for item in items.values():
            extra = json.loads(item["extra"])
            # Code stores date-only (converted to user's local timezone)
            assert extra["mentioned_at"] == "2026-02-27"

    def test_llm_failure_falls_back_to_conversation_date(self, tmp_path):
        config = _make_config(tmp_path)
        db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
        _create_memu_schema(db_path)
        _insert_items(db_path, [
            {"id": "evt-1", "memory_type": "event", "summary": "Some event"},
        ])

        bridge = MemUBridge(config)
        conv_ts = "2026-02-27T10:00:00"

        with patch.object(bridge, "_resolve_dates_via_llm", side_effect=Exception("API error")):
            bridge._resolve_event_dates_sync(conv_ts)

        items = _read_items(db_path)
        # Falls back to conv_date (date-only) when LLM fails
        assert items["evt-1"]["happened_at"] == "2026-02-27"

    def test_skips_items_that_already_have_happened_at(self, tmp_path):
        config = _make_config(tmp_path)
        db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
        _create_memu_schema(db_path)
        _insert_items(db_path, [
            {"id": "evt-old", "memory_type": "event", "summary": "Already dated", "happened_at": "2026-01-01"},
            {"id": "evt-new", "memory_type": "event", "summary": "Needs dating"},
        ])

        bridge = MemUBridge(config)

        with patch.object(bridge, "_resolve_dates_via_llm", return_value={"evt-new": "2026-02-15"}) as mock_llm:
            bridge._resolve_event_dates_sync("2026-02-27T10:00:00")

        items = _read_items(db_path)
        # Old item untouched
        assert items["evt-old"]["happened_at"] == "2026-01-01"
        # New item resolved
        assert items["evt-new"]["happened_at"] == "2026-02-15"
        # LLM only called with the new item
        call_args = mock_llm.call_args[0]
        assert len(call_args[0]) == 1
        assert call_args[0][0][0] == "evt-new"

    def test_no_items_is_noop(self, tmp_path):
        config = _make_config(tmp_path)
        db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
        _create_memu_schema(db_path)

        bridge = MemUBridge(config)
        # Should not raise
        bridge._resolve_event_dates_sync("2026-02-27T10:00:00")

    def test_preserves_existing_extra_fields(self, tmp_path):
        config = _make_config(tmp_path)
        db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
        _create_memu_schema(db_path)
        _insert_items(db_path, [
            {
                "id": "evt-1",
                "memory_type": "event",
                "summary": "Some event",
                "extra": {"content_hash": "abc123", "reinforcement_count": 3},
            },
        ])

        bridge = MemUBridge(config)

        with patch.object(bridge, "_resolve_dates_via_llm", return_value={"evt-1": "2026-02-05"}):
            bridge._resolve_event_dates_sync("2026-02-27T10:00:00")

        items = _read_items(db_path)
        extra = json.loads(items["evt-1"]["extra"])
        assert extra["content_hash"] == "abc123"
        assert extra["reinforcement_count"] == 3
        assert extra["mentioned_at"] == "2026-02-27"

    def test_sweep_skips_old_and_already_stamped_items(self, tmp_path):
        """The sweep is scoped: items outside the recency window and items
        already carrying ``mentioned_at`` are never rewritten. This is the
        fix for the unbounded whole-corpus rewrite that held the SQLite
        write lock for minutes per conversation on large databases."""
        config = _make_config(tmp_path)
        db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
        _create_memu_schema(db_path)
        _insert_items(db_path, [
            # Ancient item (outside the 24h window) — must NOT be touched.
            {"id": "old-1", "memory_type": "profile", "summary": "Old fact",
             "created_at": "2020-01-01 00:00:00.000000"},
            # Already stamped — must NOT be re-stamped.
            {"id": "stamped-1", "memory_type": "knowledge", "summary": "Known fact",
             "extra": {"mentioned_at": "2026-01-15"}},
            # Fresh, unstamped — must be stamped.
            {"id": "fresh-1", "memory_type": "behavior", "summary": "New habit"},
        ])

        bridge = MemUBridge(config)
        bridge._resolve_event_dates_sync("2026-02-27T10:00:00")

        items = _read_items(db_path)
        assert "mentioned_at" not in json.loads(items["old-1"]["extra"])
        assert json.loads(items["stamped-1"]["extra"])["mentioned_at"] == "2026-01-15"
        assert json.loads(items["fresh-1"]["extra"])["mentioned_at"] == "2026-02-27"

    def test_sweep_is_idempotent(self, tmp_path):
        """Running the sweep twice keeps the FIRST mentioned_at stamp."""
        config = _make_config(tmp_path)
        db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
        _create_memu_schema(db_path)
        _insert_items(db_path, [
            {"id": "prof-1", "memory_type": "profile", "summary": "A fact"},
        ])

        bridge = MemUBridge(config)
        bridge._resolve_event_dates_sync("2026-02-27T10:00:00")
        bridge._resolve_event_dates_sync("2026-03-05T10:00:00")

        items = _read_items(db_path)
        assert json.loads(items["prof-1"]["extra"])["mentioned_at"] == "2026-02-27"

    def test_sweep_commits_in_batches(self, tmp_path, monkeypatch):
        """All rows are stamped even when the batch size forces multiple
        commits mid-sweep."""
        config = _make_config(tmp_path)
        db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
        _create_memu_schema(db_path)
        _insert_items(db_path, [
            {"id": f"item-{i}", "memory_type": "knowledge", "summary": f"Fact {i}"}
            for i in range(5)
        ])

        bridge = MemUBridge(config)
        monkeypatch.setattr(bridge, "_DATE_SWEEP_COMMIT_EVERY", 2)
        bridge._resolve_event_dates_sync("2026-02-27T10:00:00")

        items = _read_items(db_path)
        assert len(items) == 5
        for item in items.values():
            assert json.loads(item["extra"])["mentioned_at"] == "2026-02-27"

    def test_sweep_row_cap_takes_newest(self, tmp_path, monkeypatch):
        """When the cap is hit, only the newest rows are processed."""
        config = _make_config(tmp_path)
        db_path = config.memory.sqlite_dsn.replace("sqlite:///", "")
        _create_memu_schema(db_path)
        now = datetime.now(timezone.utc)
        _insert_items(db_path, [
            {"id": "newest", "memory_type": "knowledge", "summary": "n",
             "created_at": now.strftime("%Y-%m-%d %H:%M:%S.%f")},
            {"id": "newer", "memory_type": "knowledge", "summary": "n",
             "created_at": (now - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S.%f")},
            {"id": "oldest", "memory_type": "knowledge", "summary": "n",
             "created_at": (now - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S.%f")},
        ])

        bridge = MemUBridge(config)
        monkeypatch.setattr(bridge, "_DATE_SWEEP_MAX_ROWS", 2)
        bridge._resolve_event_dates_sync("2026-02-27T10:00:00")

        items = _read_items(db_path)
        assert "mentioned_at" in json.loads(items["newest"]["extra"])
        assert "mentioned_at" in json.loads(items["newer"]["extra"])
        assert "mentioned_at" not in json.loads(items["oldest"]["extra"])


def _mock_anthropic(response_text: str) -> tuple[MagicMock, MagicMock]:
    """Create a mock anthropic module and client that returns the given text.

    Returns (mock_module, mock_client_instance) so tests can inspect calls.
    """
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text=response_text)]
    mock_client_cls = MagicMock()
    mock_client_cls.return_value.messages.create.return_value = mock_response
    mock_module = MagicMock()
    mock_module.Anthropic = mock_client_cls
    return mock_module, mock_client_cls


class TestResolveDatesViaLlm:
    """Test _resolve_dates_via_llm response parsing."""

    def test_parses_valid_json_response(self, tmp_path):
        config = _make_config(tmp_path)
        bridge = MemUBridge(config)

        items = [
            ("id-1", "User went hiking on February 5, 2026"),
            ("id-2", "On Feb 10, user scheduled dentist for March 15"),
            ("id-3", "User's team previously completed a project"),
        ]

        mock_mod, _ = _mock_anthropic(
            '[{"happened_at": "2026-02-05"}, {"happened_at": "2026-02-10"}, {"happened_at": null}]'
        )
        with patch.dict("sys.modules", {"anthropic": mock_mod}):
            result = bridge._resolve_dates_via_llm(items, "2026-02-10")

        assert result == {"id-1": "2026-02-05", "id-2": "2026-02-10", "id-3": None}

    def test_parses_json_with_surrounding_text(self, tmp_path):
        config = _make_config(tmp_path)
        bridge = MemUBridge(config)

        items = [("id-1", "Some event")]

        mock_mod, _ = _mock_anthropic(
            'Here is the result:\n[{"happened_at": "2026-03-01"}]\nDone.'
        )
        with patch.dict("sys.modules", {"anthropic": mock_mod}):
            result = bridge._resolve_dates_via_llm(items, "2026-02-10")

        assert result == {"id-1": "2026-03-01"}

    def test_returns_empty_on_unparseable_response(self, tmp_path):
        config = _make_config(tmp_path)
        bridge = MemUBridge(config)

        items = [("id-1", "Some event")]

        mock_mod, _ = _mock_anthropic("I cannot process this request.")
        with patch.dict("sys.modules", {"anthropic": mock_mod}):
            result = bridge._resolve_dates_via_llm(items, "2026-02-10")

        assert result == {}

    def test_uses_fast_model_from_config(self, tmp_path):
        config = _make_config(tmp_path)
        config.memory.fast_model = "claude-haiku-4-5-20251001"
        bridge = MemUBridge(config)

        items = [("id-1", "Some event")]

        mock_mod, mock_client_cls = _mock_anthropic('[{"happened_at": "2026-01-01"}]')
        with patch.dict("sys.modules", {"anthropic": mock_mod}):
            bridge._resolve_dates_via_llm(items, "2026-02-10")

        call_kwargs = mock_client_cls.return_value.messages.create.call_args[1]
        assert call_kwargs["model"] == "claude-haiku-4-5-20251001"


class TestConfigMemoryModels:
    """Test that all memory model fields load correctly."""

    def test_defaults(self):
        config = MemoryConfig()
        assert config.recall_model == "claude-sonnet-4-6"
        assert config.memorize_model == "claude-sonnet-4-6"
        assert config.fast_model == "claude-haiku-4-5-20251001"
        assert config.embed_model == ""

    def test_from_dict(self):
        config = MemoryConfig.from_dict({
            "recall_model": "claude-opus-4-8",
            "memorize_model": "claude-sonnet-4-6",
            "fast_model": "claude-haiku-4-5-20251001",
            "embed_model": "text-embedding-3-large",
        })
        assert config.recall_model == "claude-opus-4-8"
        assert config.memorize_model == "claude-sonnet-4-6"
        assert config.fast_model == "claude-haiku-4-5-20251001"
        assert config.embed_model == "text-embedding-3-large"

    def test_from_dict_uses_defaults(self):
        config = MemoryConfig.from_dict({})
        assert config.recall_model == "claude-sonnet-4-6"
        assert config.memorize_model == "claude-sonnet-4-6"

    def test_semantic_dedup_threshold_default(self):
        config = MemoryConfig()
        assert config.semantic_dedup_threshold == 0.85

    def test_semantic_dedup_threshold_from_dict(self):
        config = MemoryConfig.from_dict({"semantic_dedup_threshold": 0.9})
        assert config.semantic_dedup_threshold == 0.9

    def test_knowledge_filter_default_false(self):
        config = MemoryConfig()
        assert config.knowledge_filter is False

    def test_knowledge_filter_from_dict(self):
        config = MemoryConfig.from_dict({"knowledge_filter": True})
        assert config.knowledge_filter is True

    def test_knowledge_filter_from_dict_default(self):
        config = MemoryConfig.from_dict({})
        assert config.knowledge_filter is False

    def test_semantic_dedup_threshold_from_dict_default(self):
        config = MemoryConfig.from_dict({})
        assert config.semantic_dedup_threshold == 0.85


class TestKnowledgeCustomPrompts:
    """Test that custom knowledge extraction prompts are defined correctly."""

    def test_knowledge_rules_exist_and_contain_relevance_filter(self):
        assert len(_KNOWLEDGE_CUSTOM_RULES) > 100
        assert "textbook" in _KNOWLEDGE_CUSTOM_RULES.lower()
        assert "MUST NOT extract" in _KNOWLEDGE_CUSTOM_RULES
        assert "SHOULD extract" in _KNOWLEDGE_CUSTOM_RULES

    def test_knowledge_rules_forbid_general_knowledge(self):
        for term in [
            "standard library",
            "Common CS concepts",
            "Standard DevOps",
            "popular libraries",
        ]:
            assert term in _KNOWLEDGE_CUSTOM_RULES, f"Missing forbidden category: {term}"

    def test_knowledge_rules_allow_project_specific(self):
        for term in [
            "Architecture decisions or conventions",
            "Non-obvious gotchas",
            "Custom tool behavior",
            "CI/CD issues",
            "monitoring data",
        ]:
            assert term in _KNOWLEDGE_CUSTOM_RULES, f"Missing allowed category: {term}"

    def test_knowledge_examples_include_positive_and_negative(self):
        assert "NOT extracted" in _KNOWLEDGE_CUSTOM_EXAMPLES
        assert "EXTRACTED" in _KNOWLEDGE_CUSTOM_EXAMPLES
        # Should have an empty-result example
        assert "empty result is correct" in _KNOWLEDGE_CUSTOM_EXAMPLES.lower()

    def test_knowledge_examples_show_bcrypt_as_generic(self):
        assert "bcrypt" in _KNOWLEDGE_CUSTOM_EXAMPLES
        assert "json.dumps" in _KNOWLEDGE_CUSTOM_EXAMPLES


class TestCallKnowledgeFilterSync:
    """Test _call_knowledge_filter_sync response parsing."""

    def test_parses_valid_json_array(self, tmp_path):
        config = _make_config(tmp_path)
        bridge = MemUBridge(config)

        mock_mod, _ = _mock_anthropic("[0, 2, 4]")
        with patch.dict("sys.modules", {"anthropic": mock_mod}):
            result = bridge._call_knowledge_filter_sync("claude-haiku-4-5-20251001", "test prompt")

        assert result == [0, 2, 4]

    def test_parses_empty_array(self, tmp_path):
        config = _make_config(tmp_path)
        bridge = MemUBridge(config)

        mock_mod, _ = _mock_anthropic("[]")
        with patch.dict("sys.modules", {"anthropic": mock_mod}):
            result = bridge._call_knowledge_filter_sync("claude-haiku-4-5-20251001", "test prompt")

        assert result == []

    def test_parses_json_with_surrounding_text(self, tmp_path):
        config = _make_config(tmp_path)
        bridge = MemUBridge(config)

        mock_mod, _ = _mock_anthropic("Here are the generic indices:\n[1, 3]\nDone.")
        with patch.dict("sys.modules", {"anthropic": mock_mod}):
            result = bridge._call_knowledge_filter_sync("claude-haiku-4-5-20251001", "test prompt")

        assert result == [1, 3]

    def test_returns_empty_on_unparseable_response(self, tmp_path):
        config = _make_config(tmp_path)
        bridge = MemUBridge(config)

        mock_mod, _ = _mock_anthropic("I cannot determine which items are generic.")
        with patch.dict("sys.modules", {"anthropic": mock_mod}):
            result = bridge._call_knowledge_filter_sync("claude-haiku-4-5-20251001", "test prompt")

        assert result == []

    def test_uses_provided_model(self, tmp_path):
        config = _make_config(tmp_path)
        bridge = MemUBridge(config)

        mock_mod, mock_client_cls = _mock_anthropic("[]")
        with patch.dict("sys.modules", {"anthropic": mock_mod}):
            bridge._call_knowledge_filter_sync("claude-haiku-4-5-20251001", "test prompt")

        call_kwargs = mock_client_cls.return_value.messages.create.call_args[1]
        assert call_kwargs["model"] == "claude-haiku-4-5-20251001"


@pytest.mark.asyncio
class TestFilterKnowledgeItems:
    """Test _filter_knowledge_items async method."""

    async def test_deletes_items_flagged_by_filter(self, tmp_path):
        config = _make_config(tmp_path)
        bridge = MemUBridge(config)
        bridge._available = True
        bridge._service = MagicMock()

        items = [
            {"id": "k1", "memory_type": "knowledge", "summary": "bcrypt is for password hashing"},
            {"id": "k2", "memory_type": "knowledge", "summary": "Nerve uses monkey-patching in memu_bridge"},
            {"id": "k3", "memory_type": "knowledge", "summary": "json.dumps doesn't handle numpy"},
        ]

        with patch.object(bridge, "_call_knowledge_filter_sync", return_value=[0, 2]):
            bridge.delete_item = AsyncMock(return_value=True)
            await bridge._filter_knowledge_items(items)

        assert bridge.delete_item.call_count == 2
        deleted_ids = [call.args[0] for call in bridge.delete_item.call_args_list]
        assert "k1" in deleted_ids
        assert "k3" in deleted_ids
        assert "k2" not in deleted_ids

    async def test_no_deletions_when_filter_returns_empty(self, tmp_path):
        config = _make_config(tmp_path)
        bridge = MemUBridge(config)
        bridge._available = True
        bridge._service = MagicMock()

        items = [
            {"id": "k1", "memory_type": "knowledge", "summary": "Nerve-specific architecture fact"},
        ]

        with patch.object(bridge, "_call_knowledge_filter_sync", return_value=[]):
            bridge.delete_item = AsyncMock(return_value=True)
            await bridge._filter_knowledge_items(items)

        bridge.delete_item.assert_not_called()

    async def test_skips_when_not_available(self, tmp_path):
        config = _make_config(tmp_path)
        bridge = MemUBridge(config)
        bridge._available = False

        items = [{"id": "k1", "memory_type": "knowledge", "summary": "something"}]

        with patch.object(bridge, "_call_knowledge_filter_sync") as mock_filter:
            bridge.delete_item = AsyncMock()
            await bridge._filter_knowledge_items(items)

        mock_filter.assert_not_called()
        bridge.delete_item.assert_not_called()

    async def test_handles_filter_failure_gracefully(self, tmp_path):
        config = _make_config(tmp_path)
        bridge = MemUBridge(config)
        bridge._available = True
        bridge._service = MagicMock()

        items = [{"id": "k1", "memory_type": "knowledge", "summary": "something"}]

        with patch.object(bridge, "_call_knowledge_filter_sync", side_effect=Exception("API down")):
            bridge.delete_item = AsyncMock()
            # Should not raise
            await bridge._filter_knowledge_items(items)

        bridge.delete_item.assert_not_called()

    async def test_handles_out_of_range_indices(self, tmp_path):
        config = _make_config(tmp_path)
        bridge = MemUBridge(config)
        bridge._available = True
        bridge._service = MagicMock()

        items = [
            {"id": "k1", "memory_type": "knowledge", "summary": "item one"},
        ]

        # Filter returns index 5 which is out of range — should be ignored
        with patch.object(bridge, "_call_knowledge_filter_sync", return_value=[0, 5, -1]):
            bridge.delete_item = AsyncMock(return_value=True)
            await bridge._filter_knowledge_items(items)

        # Only index 0 is valid
        assert bridge.delete_item.call_count == 1
        assert bridge.delete_item.call_args_list[0].args[0] == "k1"


class TestSemanticDedupThreshold:
    """Test semantic dedup threshold module-level variable."""

    def test_default_threshold_value(self):
        assert _SEMANTIC_DEDUP_THRESHOLD == 0.85

    def test_threshold_set_from_config(self, tmp_path):
        """Verify initialize() sets the module-level threshold from config."""
        import nerve.memory.memu_bridge as bridge_mod

        config = _make_config(tmp_path)
        config.memory.semantic_dedup_threshold = 0.92
        bridge = MemUBridge(config)

        original = bridge_mod._SEMANTIC_DEDUP_THRESHOLD
        try:
            # Simulate what initialize() does: set global before patching
            bridge_mod._SEMANTIC_DEDUP_THRESHOLD = config.memory.semantic_dedup_threshold
            assert bridge_mod._SEMANTIC_DEDUP_THRESHOLD == 0.92
        finally:
            bridge_mod._SEMANTIC_DEDUP_THRESHOLD = original


class TestSanitizeMemuDatetimes:
    """Test _sanitize_memu_datetimes fixes non-string datetime values."""

    def _create_memu_db(self, db_path: Path) -> None:
        """Create a minimal memu_memory_items table with test data."""
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE memu_memory_items (
                id VARCHAR PRIMARY KEY,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL,
                resource_id VARCHAR,
                memory_type VARCHAR NOT NULL,
                summary TEXT NOT NULL,
                happened_at DATETIME,
                extra JSON,
                embedding_json TEXT,
                user_id VARCHAR
            )
        """)
        # Good row: text datetime
        conn.execute(
            "INSERT INTO memu_memory_items (id, updated_at, memory_type, summary, happened_at) "
            "VALUES ('good-1', '2026-04-09', 'event', 'Normal event', '2026-03-15 10:00:00')"
        )
        # Good row: NULL happened_at
        conn.execute(
            "INSERT INTO memu_memory_items (id, updated_at, memory_type, summary, happened_at) "
            "VALUES ('good-2', '2026-04-09', 'knowledge', 'Some fact', NULL)"
        )
        # Bad row: integer happened_at
        conn.execute(
            "INSERT INTO memu_memory_items (id, updated_at, memory_type, summary, happened_at) "
            "VALUES ('bad-int', '2026-04-09', 'event', 'Tax year 2025', 2025)"
        )
        # Bad row: another integer
        conn.execute(
            "INSERT INTO memu_memory_items (id, updated_at, memory_type, summary, happened_at) "
            "VALUES ('bad-int2', '2026-04-09', 'event', 'Year 2024', 2024)"
        )
        conn.commit()
        conn.close()

    def test_fixes_integer_happened_at(self, tmp_path):
        db_path = tmp_path / "memu.sqlite"
        self._create_memu_db(db_path)

        MemUBridge._sanitize_memu_datetimes(f"sqlite:///{db_path}")

        conn = sqlite3.connect(str(db_path))
        # Integer rows should now be text
        row = conn.execute(
            "SELECT happened_at, typeof(happened_at) FROM memu_memory_items WHERE id = 'bad-int'"
        ).fetchone()
        assert row[1] == "text"
        assert row[0] == "2025-01-01 00:00:00"

        row2 = conn.execute(
            "SELECT happened_at, typeof(happened_at) FROM memu_memory_items WHERE id = 'bad-int2'"
        ).fetchone()
        assert row2[1] == "text"
        assert row2[0] == "2024-01-01 00:00:00"
        conn.close()

    def test_preserves_valid_datetimes(self, tmp_path):
        db_path = tmp_path / "memu.sqlite"
        self._create_memu_db(db_path)

        MemUBridge._sanitize_memu_datetimes(f"sqlite:///{db_path}")

        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT happened_at FROM memu_memory_items WHERE id = 'good-1'"
        ).fetchone()
        assert row[0] == "2026-03-15 10:00:00"

        row2 = conn.execute(
            "SELECT happened_at FROM memu_memory_items WHERE id = 'good-2'"
        ).fetchone()
        assert row2[0] is None
        conn.close()

    def test_no_crash_on_missing_db(self, tmp_path):
        """Should handle missing DB gracefully."""
        MemUBridge._sanitize_memu_datetimes(f"sqlite:///{tmp_path / 'nonexistent.sqlite'}")
        # No exception raised


class TestValidateDateValue:
    """Test _validate_date_value rejects bad LLM date outputs."""

    def test_valid_date_string(self):
        assert MemUBridge._validate_date_value("2025-03-15") == "2025-03-15"

    def test_valid_datetime_string(self):
        assert MemUBridge._validate_date_value("2025-03-15 10:30:00") == "2025-03-15 10:30:00"

    def test_none_passthrough(self):
        assert MemUBridge._validate_date_value(None) is None

    def test_bare_integer_year(self):
        assert MemUBridge._validate_date_value(2025) == "2025-01-01"

    def test_bare_float_year(self):
        assert MemUBridge._validate_date_value(2025.0) == "2025-01-01"

    def test_out_of_range_integer(self):
        assert MemUBridge._validate_date_value(99) is None
        assert MemUBridge._validate_date_value(3000) is None

    def test_empty_string(self):
        assert MemUBridge._validate_date_value("") is None
        assert MemUBridge._validate_date_value("  ") is None

    def test_garbage_string(self):
        assert MemUBridge._validate_date_value("not a date") is None

    def test_non_string_non_number(self):
        assert MemUBridge._validate_date_value([2025]) is None
        assert MemUBridge._validate_date_value({"year": 2025}) is None


# ---------------------------------------------------------------------------
# _VectorIndex — persistent incremental matrix index
# ---------------------------------------------------------------------------

class _FakeItem:
    def __init__(self, memory_type: str, embedding):
        self.memory_type = memory_type
        self.embedding = embedding


class TestVectorIndex:
    def _items(self):
        import numpy as np
        return {
            "a": _FakeItem("event", np.array([1.0, 0.0, 0.0], dtype="float32")),
            "b": _FakeItem("profile", np.array([0.0, 1.0, 0.0], dtype="float32")),
            "c": _FakeItem("event", np.array([0.0, 0.0, 1.0], dtype="float32")),
            "no-vec": _FakeItem("event", None),
        }

    def test_build_and_search(self):
        from nerve.memory.memu_bridge import _VectorIndex
        idx = _VectorIndex()
        idx.build(self._items())
        assert idx.count == 3  # embedding-less item excluded
        hits = idx.search([1.0, 0.0, 0.0], k=2)
        assert hits[0][0] == "a"
        assert hits[0][1] == pytest.approx(1.0, abs=1e-4)

    def test_type_filtered_search(self):
        from nerve.memory.memu_bridge import _VectorIndex
        idx = _VectorIndex()
        idx.build(self._items())
        # Without filter, "b" wins for [0,1,0]; with event filter it must not.
        hits = idx.search([0.0, 1.0, 0.0], k=1, memory_type="event")
        assert hits
        assert hits[0][0] in ("a", "c")
        # No matches for unknown type
        assert idx.search([0.0, 1.0, 0.0], k=1, memory_type="nope") == []

    def test_upsert_appends_and_updates(self):
        from nerve.memory.memu_bridge import _VectorIndex
        idx = _VectorIndex()
        idx.build(self._items())
        idx.upsert("d", "knowledge", [0.5, 0.5, 0.0])
        assert idx.count == 4
        hits = idx.search([0.5, 0.5, 0.0], k=1)
        assert hits[0][0] == "d"
        # Update in place — no growth
        idx.upsert("d", "knowledge", [0.0, 0.0, 1.0])
        assert idx.count == 4
        hits = idx.search([0.0, 0.0, 1.0], k=1)
        assert hits[0][0] in ("c", "d")

    def test_remove_swaps_last_row(self):
        from nerve.memory.memu_bridge import _VectorIndex
        idx = _VectorIndex()
        idx.build(self._items())
        idx.remove("a")
        assert idx.count == 2
        assert "a" not in idx.id_to_row
        # Remaining vectors still searchable
        hits = idx.search([0.0, 1.0, 0.0], k=1)
        assert hits[0][0] == "b"
        # Removing a missing id is a no-op
        idx.remove("missing")
        assert idx.count == 2

    def test_empty_index(self):
        from nerve.memory.memu_bridge import _VectorIndex
        idx = _VectorIndex()
        idx.build({})
        assert idx.count == 0
        assert idx.search([1.0, 0.0, 0.0], k=5) == []
        # Upsert into an empty index bootstraps the dimension
        idx.upsert("x", "event", [1.0, 0.0])
        assert idx.count == 1
        assert idx.search([1.0, 0.0], k=1)[0][0] == "x"

    def test_capacity_growth(self):
        import numpy as np
        from nerve.memory.memu_bridge import _VectorIndex
        idx = _VectorIndex()
        idx.build({})
        rng = np.random.default_rng(42)
        for i in range(600):  # exceeds initial 256 capacity
            idx.upsert(f"item-{i}", "event", rng.random(8).astype("float32"))
        assert idx.count == 600
        assert idx.capacity >= 600

    def test_vec_index_for_rebuilds_on_drift(self):
        from nerve.memory.memu_bridge import _vec_index_for

        class _Repo:
            pass

        repo = _Repo()
        repo.items = self._items()
        idx = _vec_index_for(repo)
        assert idx.count == 3
        # Mutate items WITHOUT going through the hooks — drift detected
        import numpy as np
        repo.items["e"] = _FakeItem("event", np.array([1.0, 1.0, 0.0], dtype="float32"))
        idx2 = _vec_index_for(repo)
        assert idx2.count == 4


# ---------------------------------------------------------------------------
# Dedicated memU event loop (_submit / shutdown)
# ---------------------------------------------------------------------------

class TestMemuLoop:
    @pytest.mark.asyncio
    async def test_submit_inline_fallback_without_loop(self, tmp_path):
        """Bridges that never ran initialize() execute coroutines inline."""
        bridge = MemUBridge(_make_config(tmp_path))

        async def _probe():
            return asyncio.get_running_loop()

        loop = await bridge._submit(_probe())
        assert loop is asyncio.get_running_loop()

    @pytest.mark.asyncio
    async def test_submit_runs_on_memu_loop(self, tmp_path):
        """After _start_memu_loop, coroutines run on the dedicated loop."""
        bridge = MemUBridge(_make_config(tmp_path))
        bridge._start_memu_loop()
        try:
            async def _probe():
                return asyncio.get_running_loop()

            loop = await bridge._submit(_probe())
            assert loop is bridge._memu_loop
            assert loop is not asyncio.get_running_loop()

            # Exceptions propagate back to the caller
            async def _boom():
                raise ValueError("kaput")

            with pytest.raises(ValueError, match="kaput"):
                await bridge._submit(_boom())
        finally:
            await bridge.shutdown()
        assert bridge._memu_loop is None

    @pytest.mark.asyncio
    async def test_submit_timeout_cancels_memu_task(self, tmp_path):
        """wait_for around _submit cancels the coroutine on the memU loop."""
        bridge = MemUBridge(_make_config(tmp_path))
        bridge._start_memu_loop()
        cancelled = asyncio.Event()
        main_loop = asyncio.get_running_loop()

        async def _hang():
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                main_loop.call_soon_threadsafe(cancelled.set)
                raise

        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(bridge._submit(_hang()), timeout=0.2)
            await asyncio.wait_for(cancelled.wait(), timeout=5)
        finally:
            await bridge.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_idempotent(self, tmp_path):
        bridge = MemUBridge(_make_config(tmp_path))
        await bridge.shutdown()  # never started — must not raise
        bridge._start_memu_loop()
        await bridge.shutdown()
        await bridge.shutdown()  # double shutdown is fine


# ---------------------------------------------------------------------------
# SQLite pragmas
# ---------------------------------------------------------------------------

class TestSqlitePragmas:
    def test_wal_conversion(self, tmp_path):
        db_path = tmp_path / "memu.sqlite"
        _create_memu_schema(str(db_path))
        assert sqlite3.connect(db_path).execute("PRAGMA journal_mode").fetchone()[0] == "delete"
        MemUBridge._setup_sqlite_pragmas(f"sqlite:///{db_path}")
        assert sqlite3.connect(db_path).execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        # Idempotent
        MemUBridge._setup_sqlite_pragmas(f"sqlite:///{db_path}")
        assert sqlite3.connect(db_path).execute("PRAGMA journal_mode").fetchone()[0] == "wal"

    def test_missing_db_does_not_raise(self, tmp_path):
        MemUBridge._setup_sqlite_pragmas(f"sqlite:///{tmp_path}/nope/missing.sqlite")


# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# memU write-path integrity (Fixes 7-correction, 8, 9, 10)
# ---------------------------------------------------------------------------


_MEMU_MODELS_CACHE = {}


def _memu_models():
    """Build the SQLAlchemy models ONCE per process.

    nerve's Fix 6 (_patched_get_models) clears memu's _MODEL_CACHE on every
    call, so a second SQLiteStore(dsn=...) that rebuilds them raises
    ArgumentError("Column object 'url' already assigned"). Sharing one build
    across stores is what lets these tests use several isolated stores.
    """
    if "models" in _MEMU_MODELS_CACHE:
        return _MEMU_MODELS_CACHE["models"]

    import memu.app.service  # noqa: F401 - initialize the package graph
    import memu.database.sqlite.schema as schema_mod
    from memu.app.crud import CRUDMixin
    from memu.database.sqlite.repositories.memory_item_repo import (
        SQLiteMemoryItemRepo as Repo,
    )

    # Building the models needs the patches applied (Fix 6 renames memu's own
    # "sqlite_*" tables, which SQLite reserves), but this helper must leave
    # global state exactly as it found it: leaking a patched Repo out of a
    # module-level helper is how one test silently breaks its neighbours.
    saved_repo = {n: Repo.__dict__.get(n) for n in _PATCHED_ITEM_REPO_ATTRS}
    saved_handler = CRUDMixin.__dict__.get("_patch_update_memory_item")
    saved_stash = CRUDMixin.__dict__.get("_nerve_memu_update_handler")
    try:
        MemUBridge._patch_sqlite_bugs()
        # Resolve through the MODULE, never a from-import: Fix 6 replaces this
        # attribute.
        models = schema_mod.get_sqlite_sqlalchemy_models(scope_model=None)
    finally:
        _restore_attrs(Repo, saved_repo)
        _restore_attrs(CRUDMixin, {
            "_patch_update_memory_item": saved_handler,
            "_nerve_memu_update_handler": saved_stash,
        })

    _MEMU_MODELS_CACHE["models"] = models
    return models


def _restore_attrs(cls, saved):
    """Put class attributes back, deleting those that did not exist before."""
    for name, value in saved.items():
        if value is None:
            if name in cls.__dict__:
                delattr(cls, name)
        else:
            setattr(cls, name, value)


def _content_hash(summary, memory_type):
    """memu's compute_content_hash, imported safely.

    ``from memu.database.models import ...`` FIRST triggers a circular import
    inside memu itself (database/__init__ -> factory -> app/__init__ -> service
    -> factory), so memu.app.service must be imported before it. Going through
    this helper is what lets a single test in these classes run alone.
    """
    import memu.app.service  # noqa: F401
    from memu.database.models import compute_content_hash

    return compute_content_hash(summary, memory_type)


# Every memu-py class attribute the patches reassign, so each test can restore
# global state and never leak into another test (or another suite: a module that
# mutates shared globals at import time breaks its neighbours invisibly, so all
# patching happens INSIDE tests).
class _NoRowSessions:
    """Session manager whose queries return no row.

    Lets a bare ``object.__new__(Repo)`` stub satisfy the hash-refresh wrapper's
    row lookup: with no row there is nothing to recompute a hash from, so it
    delegates straight through - which is what the forwarding test measures.
    """

    class _Session:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def exec(self, *args, **kwargs):
            return self

        def first(self):
            return None

    def session(self):
        return self._Session()


_PATCHED_ITEM_REPO_ATTRS = (
    "update_item", "delete_item", "clear_items", "list_items",
    "create_item", "create_item_reinforce", "vector_search_items",
    "_nerve_memu_update_item", "_nerve_memu_delete_item",
)


class _MemuPatchFixture:
    """Isolated memU stores over a temp file with _patch_sqlite_bugs() applied."""

    def __init__(self, tmp_path):
        self.models = _memu_models()
        self.path = str(tmp_path / "memu.sqlite")
        self._stores = []
        self.saved_item_repo = {}
        self.saved_handler = None

    def __enter__(self):
        import memu.app.service  # noqa: F401
        from memu.app.crud import CRUDMixin
        from memu.database.sqlite.repositories.memory_item_repo import (
            SQLiteMemoryItemRepo as Repo,
        )

        self.Repo = Repo
        self.CRUDMixin = CRUDMixin
        self.saved_item_repo = {
            n: Repo.__dict__.get(n) for n in _PATCHED_ITEM_REPO_ATTRS
        }
        self.saved_handler = CRUDMixin.__dict__.get("_patch_update_memory_item")
        self.saved_handler_stash = CRUDMixin.__dict__.get("_nerve_memu_update_handler")
        MemUBridge._patch_sqlite_bugs()
        return self

    def __exit__(self, *exc):
        for store in self._stores:
            try:
                store.close()
            except Exception:
                pass
        _restore_attrs(self.Repo, self.saved_item_repo)
        _restore_attrs(self.CRUDMixin, {
            "_patch_update_memory_item": self.saved_handler,
            "_nerve_memu_update_handler": self.saved_handler_stash,
        })
        return False

    def store(self):
        """A fresh store over the same file (a simulated process restart)."""
        from memu.database.sqlite.sqlite import SQLiteStore

        store = SQLiteStore(dsn=f"sqlite:///{self.path}", sqla_models=self.models)
        self._stores.append(store)
        return store

    def setup(self, n_categories=2):
        store = self.store()
        cats = {}
        for name in ("procedures", "patterns", "decisions")[:n_categories]:
            cat = store.memory_category_repo.get_or_create_category(
                name=name, description="d", embedding=None, user_data={},
            )
            cats[name] = cat.id
        return store, cats

    def add_item(self, store, summary="a fact", memory_type="knowledge", embedding=None):
        return store.memory_item_repo.create_item_reinforce(
            resource_id=None, memory_type=memory_type, summary=summary,
            embedding=embedding, user_data={},
        )

    def link_all(self, store, item_id, cats):
        for cid in cats.values():
            store.category_item_repo.link_item_category(item_id, cid, user_data={})

    def raw_extra(self, item_id):
        row = sqlite3.connect(self.path).execute(
            "SELECT extra FROM memu_memory_items WHERE id = ?", (item_id,),
        ).fetchone()
        return json.loads(row[0]) if row and row[0] else {}

    def raw_count(self, sql, *params):
        return sqlite3.connect(self.path).execute(sql, params).fetchone()[0]

    def run_update(self, store, cats, item_id, *, content=None, memory_type=None,
                   categories=None, ctx=None):
        """Drive the REAL memU update workflow handler (no mock of it)."""
        class _Ctx:
            def __init__(self, mapping):
                self.category_name_to_id = dict(mapping)
                self.category_ids = list(mapping.values())

        class _Embed:
            async def embed(self, payload):
                return [None]

        svc = object.__new__(self.CRUDMixin)
        svc._get_step_embedding_client = lambda step_ctx: _Embed()
        state = {
            "memory_id": item_id,
            "memory_payload": {"content": content, "type": memory_type,
                               "categories": categories},
            "ctx": ctx if ctx is not None else _Ctx(cats),
            "store": store,
            "user": {},
        }
        return asyncio.run(
            self.CRUDMixin._patch_update_memory_item(svc, state, None),
        )

    def ctx(self, mapping):
        class _Ctx:
            def __init__(self, m):
                self.category_name_to_id = dict(m)
                self.category_ids = list(m.values())

        return _Ctx(mapping)


class TestIndexedUpdateItemForwarding:
    """Regression: the _indexed_update_item monkeypatch must forward item_id
    by keyword.

    memu-py 1.4.0's ``SQLiteMemoryItemRepo.update_item`` is keyword-only
    (``def update_item(self, *, item_id, ...)``). The vector-index wrapper used
    to forward ``item_id`` positionally, which raised "takes 1 positional
    argument but 2 positional arguments (and 3 keyword-only arguments) were
    given" — silently turning every ``memory_update`` tool call into
    "Failed to update memory". ``delete_item(self, item_id)`` is NOT keyword
    only, which is why deletes kept working and masked the bug.
    """

    def test_update_item_is_keyword_only_in_memu(self):
        import inspect
        import memu.app.service  # noqa: F401 — initialize package graph first
        from memu.database.sqlite.repositories.memory_item_repo import (
            SQLiteMemoryItemRepo as Repo,
        )

        kind = inspect.signature(Repo.update_item).parameters["item_id"].kind
        assert kind is inspect.Parameter.KEYWORD_ONLY, (
            "memu update_item contract changed — revisit _indexed_update_item"
        )

    def test_wrapper_forwards_item_id_as_keyword(self):
        import memu.app.service  # noqa: F401 — initialize package graph first
        from memu.database.sqlite.repositories.memory_item_repo import (
            SQLiteMemoryItemRepo as Repo,
        )

        calls: list[str] = []

        def spy_update(self, *, item_id, memory_type=None, summary=None,
                       embedding=None, extra=None, tool_record=None):
            calls.append(item_id)
            return "spy-result"

        # Snapshot every item-repo attribute _patch_sqlite_bugs() reassigns so
        # the test restores global state and does not leak into other tests.
        # The _nerve_memu_* stashes matter as much as the methods: leaving one
        # behind hands the NEXT test this spy as "memU's own implementation".
        names = _PATCHED_ITEM_REPO_ATTRS
        saved = {n: Repo.__dict__.get(n) for n in names}
        # Fix 8 installs on CRUDMixin, not on Repo, so those two attributes leak
        # out of this class unless they are snapshotted here as well.
        from memu.app.crud import CRUDMixin

        saved_crud = {
            n: CRUDMixin.__dict__.get(n)
            for n in ("_patch_update_memory_item", "_nerve_memu_update_handler")
        }

        Repo.update_item = spy_update
        try:
            # Returns None on success (the body falls through); the observable
            # effect is that update_item gets wrapped in front of our spy.
            MemUBridge._patch_sqlite_bugs()
            assert Repo.update_item is not spy_update

            stub = object.__new__(Repo)  # no _nerve_vec_index → index hook skipped
            # The hash-refresh wrapper reads the item's current row, so the stub
            # needs the two attributes that read uses.  Returning no row makes it
            # skip the refresh and delegate, which is what this test measures.
            stub._memory_item_model = _memu_models().MemoryItem
            stub._sessions = _NoRowSessions()
            # Exactly how memu's crud layer calls it (all keyword) — used to raise.
            result = Repo.update_item(
                stub, item_id="mem-123", memory_type=None,
                summary="updated", embedding=None,
            )
            assert result == "spy-result"
            assert calls == ["mem-123"]
        finally:
            for name, fn in saved.items():
                if fn is None:
                    if name in Repo.__dict__:
                        delattr(Repo, name)
                else:
                    setattr(Repo, name, fn)
            _restore_attrs(CRUDMixin, saved_crud)


class TestSqliteLockedClassifier:
    """_is_sqlite_locked_error truth table."""

    def test_raw_sqlite_message(self):
        assert _is_sqlite_locked_error(sqlite3.OperationalError("database is locked"))

    def test_sqlalchemy_wrapped_message(self):
        assert _is_sqlite_locked_error(
            Exception("(sqlite3.OperationalError) database is locked\n[SQL: UPDATE ...]")
        )

    def test_table_locked_variant(self):
        assert _is_sqlite_locked_error(Exception("database table is locked: memu_memory_items"))

    def test_unrelated_errors_not_matched(self):
        assert not _is_sqlite_locked_error(Exception("no such table: foo"))
        assert not _is_sqlite_locked_error(Exception("Error code: 429 - rate limited"))


def _make_lock_test_bridge(tmp_path: Path) -> MemUBridge:
    """Bridge wired for memorize_file tests: available, mocked service,
    zero retry delay, no memU loop (inline _submit)."""
    config = _make_config(tmp_path)
    bridge = MemUBridge(config)
    bridge._available = True
    bridge._service = MagicMock()
    bridge._MEMORIZE_RETRY_DELAY = 0
    return bridge


_LOCKED_ERR = "(sqlite3.OperationalError) database is locked"


class TestMemorizeFileLockRetry:
    """memorize_file retries SQLite lock contention instead of failing the
    write outright (the "memU refused" failure mode under fleet load)."""

    @pytest.mark.asyncio
    async def test_lock_then_success_returns_true(self, tmp_path):
        bridge = _make_lock_test_bridge(tmp_path)
        bridge._service.memorize = AsyncMock(
            side_effect=[Exception(_LOCKED_ERR), {"items": []}],
        )

        target = tmp_path / "note.txt"
        target.write_text("knowledge: a fact")
        ok = await bridge.memorize_file(str(target))

        assert ok is True
        assert bridge._service.memorize.await_count == 2

    @pytest.mark.asyncio
    async def test_lock_exhausted_raises_backend_unavailable(self, tmp_path):
        bridge = _make_lock_test_bridge(tmp_path)
        bridge._service.memorize = AsyncMock(side_effect=Exception(_LOCKED_ERR))

        target = tmp_path / "note.txt"
        target.write_text("knowledge: a fact")
        with pytest.raises(MemoryBackendUnavailable, match="write-lock contention"):
            await bridge.memorize_file(str(target))

        # Initial attempt + _MEMORIZE_MAX_RETRIES retries
        assert bridge._service.memorize.await_count == bridge._MEMORIZE_MAX_RETRIES + 1

    @pytest.mark.asyncio
    async def test_non_transient_error_still_fails_fast(self, tmp_path):
        bridge = _make_lock_test_bridge(tmp_path)
        bridge._service.memorize = AsyncMock(side_effect=ValueError("bad payload"))

        target = tmp_path / "note.txt"
        target.write_text("knowledge: a fact")
        ok = await bridge.memorize_file(str(target))

        assert ok is False
        assert bridge._service.memorize.await_count == 1

    @pytest.mark.asyncio
    async def test_transient_llm_error_still_raises_backend_unavailable(self, tmp_path):
        bridge = _make_lock_test_bridge(tmp_path)
        bridge._service.memorize = AsyncMock(
            side_effect=Exception("Error code: 429 - guardrail text units per second limit exceeded"),
        )

        target = tmp_path / "note.txt"
        target.write_text("knowledge: a fact")
        with pytest.raises(MemoryBackendUnavailable, match="unavailable"):
            await bridge.memorize_file(str(target))

        assert bridge._service.memorize.await_count == 1


class TestUpdatePreservesCategories:
    """Fix 8: a content-only memory_update must not unlink every category.

    memU has ONE sentinel for two meanings: _patch_update_memory_item maps a
    missing ``categories`` to ``[]`` (_map_category_names_to_ids returns [] for
    a falsy list), so ``cats_to_remove`` becomes the item's ENTIRE current set.
    Measured on the live store before the fix: 154 of 154 items ever updated
    without a categories argument held ZERO category links.
    """

    def test_content_only_update_keeps_links_and_rows(self, tmp_path):
        with _MemuPatchFixture(tmp_path) as fx:
            store, cats = fx.setup(2)
            item = fx.add_item(store)
            fx.link_all(store, item.id, cats)
            before = {r.id for r in store.category_item_repo.get_item_categories(item.id)}

            out = fx.run_update(store, cats, item.id, content="a corrected fact")

            after = {r.id for r in store.category_item_repo.get_item_categories(item.id)}
            assert len(after) == 2
            # Same relation row ids: preserved, not deleted and re-created.
            assert after == before
            # The summary-patch step must see "updated", never "discarded":
            # (old, None) renders as "This memory content is discarded" and
            # would make the LLM drop an item whose link still exists.
            assert sorted(out["category_updates"].values()) == [
                ("a fact", "a corrected fact"),
            ] * 2

    def test_unpatched_handler_loses_every_link(self, tmp_path):
        """The control arm: without Fix 8 the same call unlinks everything."""
        with _MemuPatchFixture(tmp_path) as fx:
            # Use memU's own handler from the stash, not fx.saved_handler:
            # a previous test in this process may already have patched the class,
            # so the snapshot is not guaranteed to be the pristine original.
            fx.CRUDMixin._patch_update_memory_item = (
                fx.CRUDMixin._nerve_memu_update_handler
            )

            store, cats = fx.setup(2)
            item = fx.add_item(store)
            fx.link_all(store, item.id, cats)

            out = fx.run_update(store, cats, item.id, content="a corrected fact")

            assert store.category_item_repo.get_item_categories(item.id) == []
            assert sorted(out["category_updates"].values()) == [("a fact", None)] * 2

    def test_explicit_list_still_replaces(self, tmp_path):
        with _MemuPatchFixture(tmp_path) as fx:
            store, cats = fx.setup(2)
            item = fx.add_item(store)
            fx.link_all(store, item.id, cats)

            fx.run_update(store, cats, item.id, content="y", categories=["patterns"])

            links = [r.category_id for r in
                     store.category_item_repo.get_item_categories(item.id)]
            assert links == [cats["patterns"]]

    def test_explicit_empty_list_still_clears(self, tmp_path):
        with _MemuPatchFixture(tmp_path) as fx:
            store, cats = fx.setup(2)
            item = fx.add_item(store)
            fx.link_all(store, item.id, cats)

            fx.run_update(store, cats, item.id, content="y", categories=[])

            assert store.category_item_repo.get_item_categories(item.id) == []

    def test_three_links_preserved(self, tmp_path):
        with _MemuPatchFixture(tmp_path) as fx:
            store, cats = fx.setup(3)
            item = fx.add_item(store)
            fx.link_all(store, item.id, cats)
            before = {r.id for r in store.category_item_repo.get_item_categories(item.id)}

            out = fx.run_update(store, cats, item.id, content="revised")

            after = {r.id for r in store.category_item_repo.get_item_categories(item.id)}
            assert len(after) == 3
            assert after == before
            assert set(out["category_updates"].values()) == {("a fact", "revised")}

    def test_incomplete_map_raises_and_changes_nothing(self, tmp_path):
        """Fail closed: refusing the update beats silently dropping links.

        ctx.category_name_to_id is rebuilt from every DB category on bridge
        init, so this should be unreachable in nerve; the raise is a tripwire.
        """
        with _MemuPatchFixture(tmp_path) as fx:
            store, cats = fx.setup(3)
            item = fx.add_item(store)
            fx.link_all(store, item.id, cats)
            partial = {k: v for k, v in cats.items() if k != "decisions"}
            ctx = fx.ctx(partial)
            snapshot = dict(ctx.category_name_to_id)

            with pytest.raises(ValueError, match="do not round-trip"):
                fx.run_update(store, cats, item.id, content="revised", ctx=ctx)

            assert len(store.category_item_repo.get_item_categories(item.id)) == 3
            # Service-lifetime state (shared with memorize) must not be mutated.
            assert ctx.category_name_to_id == snapshot
            assert fx.raw_count(
                "SELECT count(*) FROM memu_memory_items WHERE id = ? AND summary = ?",
                item.id, "a fact",
            ) == 1

    def test_item_with_no_links_is_a_noop(self, tmp_path):
        with _MemuPatchFixture(tmp_path) as fx:
            store, cats = fx.setup(2)
            item = fx.add_item(store)

            out = fx.run_update(store, cats, item.id, content="y")

            assert store.category_item_repo.get_item_categories(item.id) == []
            assert out["category_updates"] == {}

    def test_type_only_update_keeps_links(self, tmp_path):
        """Preservation must not depend on `content` being supplied.

        bridge.update_item allows a type-only change (memory_type set, content
        None) from both the tool handler and the web route, and that shape
        reaches memU's diff exactly like a content-only one.
        """
        with _MemuPatchFixture(tmp_path) as fx:
            store, cats = fx.setup(2)
            item = fx.add_item(store)
            fx.link_all(store, item.id, cats)
            before = {r.id for r in store.category_item_repo.get_item_categories(item.id)}
            assert len(before) == 2

            fx.run_update(store, cats, item.id, memory_type="profile")

            after = {r.id for r in store.category_item_repo.get_item_categories(item.id)}
            assert after == before
            assert fx.raw_count(
                "SELECT count(*) FROM memu_category_items WHERE item_id = ?", item.id,
            ) == 2
            # The update must not have been a no-op, or the assertion above is free.
            assert fx.raw_count(
                "SELECT count(*) FROM memu_memory_items WHERE id = ? AND memory_type = ?",
                item.id, "profile",
            ) == 1


class TestUpdateRefreshesContentHash:
    """Fix 9: update_item must refresh extra.content_hash.

    create_item_reinforce dedups on json_extract(extra,'$.content_hash'), but
    update_item rewrites summary/memory_type without touching it, so an updated
    item keeps its OLD text's hash. Measured before the fix: 149 of 149 updated
    items were hash-stale against a 400/400 fresh never-updated baseline.
    """

    def test_hash_matches_new_summary(self, tmp_path):
        with _MemuPatchFixture(tmp_path) as fx:
            store, _ = fx.setup(1)
            item = fx.add_item(store, summary="original text")

            store.memory_item_repo.update_item(item_id=item.id, summary="corrected text")

            assert fx.raw_extra(item.id)["content_hash"] == _content_hash(
                "corrected text", "knowledge",
            )

    def test_hash_refreshed_after_a_restart(self, tmp_path):
        """The discriminating arm: the refresh must read the DB ROW.

        get_item()/list_items() build MemoryItem WITHOUT extra=, so a cached
        item has extra == {}. A cache-based implementation finds no
        content_hash here and refreshes nothing, while still passing the
        create-path test above.
        """
        with _MemuPatchFixture(tmp_path) as fx:
            store, _ = fx.setup(1)
            item = fx.add_item(store, summary="original text")
            store.close()

            restarted = fx.store()
            restarted.memory_item_repo.list_items()
            assert restarted.memory_item_repo.items[item.id].extra == {}

            restarted.memory_item_repo.update_item(
                item_id=item.id, summary="corrected text",
            )

            assert fx.raw_extra(item.id)["content_hash"] == _content_hash(
                "corrected text", "knowledge",
            )

    def test_unpatched_update_leaves_the_hash_stale(self, tmp_path):
        """Control arm: memU's own update_item keeps the old text's hash."""
        with _MemuPatchFixture(tmp_path) as fx:
            store, _ = fx.setup(1)
            item = fx.add_item(store, summary="original text")
            memu_update = fx.Repo._nerve_memu_update_item

            memu_update(store.memory_item_repo, item_id=item.id, summary="corrected text")

            assert fx.raw_extra(item.id)["content_hash"] == _content_hash(
                "original text", "knowledge",
            )

    def test_type_change_alone_recomputes_from_the_row(self, tmp_path):
        with _MemuPatchFixture(tmp_path) as fx:
            store, _ = fx.setup(1)
            item = fx.add_item(store, summary="the text")

            store.memory_item_repo.update_item(item_id=item.id, memory_type="behavior")

            assert fx.raw_extra(item.id)["content_hash"] == _content_hash(
                "the text", "behavior",
            )

    def test_summary_and_type_changed_together_hash_from_both(self, tmp_path):
        """The combined shape. Changing summary and memory_type in one call must
        hash from BOTH new values: an implementation that keeps the old type
        whenever a summary is supplied satisfies the summary-only and type-only
        cases above while hashing a combined update wrongly.
        """
        with _MemuPatchFixture(tmp_path) as fx:
            store, _ = fx.setup(1)
            item = fx.add_item(store, summary="orig", memory_type="knowledge")

            store.memory_item_repo.update_item(
                item_id=item.id, summary="new text", memory_type="behavior",
            )

            assert fx.raw_extra(item.id)["content_hash"] == _content_hash(
                "new text", "behavior",
            )
            # Neither half may have been a no-op, or the assertion is free.
            assert fx.raw_count(
                "SELECT count(*) FROM memu_memory_items"
                " WHERE id = ? AND summary = ? AND memory_type = ?",
                item.id, "new text", "behavior",
            ) == 1

    def test_a_racing_type_change_is_not_hashed_from_the_argument(self, tmp_path):
        """The type half of the same property as the summary case below.

        Taking the type from the ARGUMENT rather than from the row passes every
        non-racing case (after the delegation commits they are equal), yet
        produces a hash for a type the row no longer holds - and the conditional
        write cannot catch it, because that predicate binds the row's own value.
        Only reading BOTH halves off the row is correct.
        """
        with _MemuPatchFixture(tmp_path) as fx:
            store, _ = fx.setup(1)
            item = fx.add_item(store, summary="orig", memory_type="knowledge")
            repo = store.memory_item_repo

            sessions = repo._sessions
            original_session = sessions.session
            raced = []

            def racing_session():
                session = original_session()
                real_commit = session.commit

                def _commit():
                    real_commit()
                    if raced:
                        return
                    conn = sqlite3.connect(fx.path)
                    landed = conn.execute(
                        "SELECT memory_type FROM memu_memory_items WHERE id = ?",
                        (item.id,),
                    ).fetchone()
                    if landed and landed[0] == "behavior":
                        conn.execute(
                            "UPDATE memu_memory_items SET memory_type = ? WHERE id = ?",
                            ("profile", item.id),
                        )
                        conn.commit()
                        raced.append(True)
                    conn.close()

                session.commit = _commit
                return session

            sessions.session = racing_session
            try:
                repo.update_item(item_id=item.id, memory_type="behavior")
            finally:
                sessions.session = original_session

            assert raced == [True]
            final_type = sqlite3.connect(fx.path).execute(
                "SELECT memory_type FROM memu_memory_items WHERE id = ?", (item.id,),
            ).fetchone()[0]
            assert final_type == "profile"

            stored = fx.raw_extra(item.id)["content_hash"]
            assert stored in (
                _content_hash("orig", final_type),
                _content_hash("orig", "knowledge"),
            )
            assert stored != _content_hash("orig", "behavior")

    def test_a_writer_outside_the_memu_loop_cannot_leave_a_stale_hash(self, tmp_path):
        """The hash must come from the row the write LEFT BEHIND.

        A pre-read snapshot closes its session before memU's write transaction,
        so a writer outside the memU loop thread (the date sweep runs on
        _blocking_pool with its own connection; `nerve memory` is a second
        process) can land in between and leave a hash of text the row does not
        hold. Here a second connection rewrites the summary the instant memU's
        write commits - which is AFTER the pre-read snapshot but BEFORE a
        post-write read, so the hook is shape-agnostic and lands in the window
        either implementation exposes.
        """
        with _MemuPatchFixture(tmp_path) as fx:
            store, _ = fx.setup(1)
            item = fx.add_item(store, summary="orig")
            repo = store.memory_item_repo

            sessions = repo._sessions
            original_session = sessions.session
            raced = []

            def racing_session():
                session = original_session()
                real_commit = session.commit

                def _commit():
                    real_commit()
                    if raced:
                        return
                    conn = sqlite3.connect(fx.path)
                    landed = conn.execute(
                        "SELECT summary FROM memu_memory_items WHERE id = ?",
                        (item.id,),
                    ).fetchone()
                    if landed and landed[0] == "my new text":
                        conn.execute(
                            "UPDATE memu_memory_items SET summary = ? WHERE id = ?",
                            ("text from the other writer", item.id),
                        )
                        conn.commit()
                        raced.append(True)
                    conn.close()

                session.commit = _commit
                return session

            sessions.session = racing_session
            try:
                repo.update_item(item_id=item.id, summary="my new text")
            finally:
                sessions.session = original_session

            # The race really happened, or the test proves nothing.
            assert raced == [True]
            final_summary = sqlite3.connect(fx.path).execute(
                "SELECT summary FROM memu_memory_items WHERE id = ?", (item.id,),
            ).fetchone()[0]
            assert final_summary == "text from the other writer"

            stored = fx.raw_extra(item.id)["content_hash"]
            # Never a hash of text the row does not hold: either it is
            # consistent with the row's final summary, or it was left alone.
            assert stored in (
                _content_hash(final_summary, "knowledge"),
                _content_hash("orig", "knowledge"),
            )
            assert stored != _content_hash("my new text", "knowledge")

    def test_a_declined_writeback_leaves_the_cache_agreeing_with_the_row(self, tmp_path):
        """When the conditional write does not land, the cache must not claim it did.

        The writeback is a no-op if another writer moved summary/memory_type
        between the read and the write. Stamping the refreshed hash into
        repo.items anyway would leave recall serving a hash the row does not
        hold - the same cache/DB divergence class as the delete failure paths.
        """
        with _MemuPatchFixture(tmp_path) as fx:
            store, _ = fx.setup(1)
            item = fx.add_item(store, summary="orig")
            repo = store.memory_item_repo

            sessions = repo._sessions
            original_session = sessions.session
            raced = []

            def racing_session():
                session = original_session()
                real_execute = session.execute

                def _execute(stmt, *args, **kwargs):
                    # Only the conditional hash write; memU's own update_item
                    # goes through exec/add/commit, so this lands in exactly the
                    # read -> write window.
                    if "json_set" in str(stmt) and not raced:
                        raced.append(True)
                        conn = sqlite3.connect(fx.path)
                        conn.execute(
                            "UPDATE memu_memory_items SET summary = ? WHERE id = ?",
                            ("text from the other writer", item.id),
                        )
                        conn.commit()
                        conn.close()
                    return real_execute(stmt, *args, **kwargs)

                session.execute = _execute
                return session

            sessions.session = racing_session
            try:
                result = repo.update_item(item_id=item.id, summary="my new text")
            finally:
                sessions.session = original_session

            assert raced == [True]
            row_hash = fx.raw_extra(item.id)["content_hash"]
            # Nothing was written, so the row keeps the hash it already had.
            assert row_hash == _content_hash("orig", "knowledge")
            # ... and neither the cache nor the returned item may disagree.
            assert repo.items[item.id].extra["content_hash"] == row_hash
            assert result.extra["content_hash"] == row_hash

    def test_item_without_a_hash_is_not_enrolled(self, tmp_path):
        """Adding a hash where there was none is a behaviour change, not a fix.

        The cache and the returned item must not be enrolled either: memorize()
        and recall read them, so a hash present only in memory is still a hash
        the store never agreed to.
        """
        with _MemuPatchFixture(tmp_path) as fx:
            store, _ = fx.setup(1)
            item = fx.add_item(store, summary="t")
            db = sqlite3.connect(fx.path)
            db.execute(
                "UPDATE memu_memory_items SET extra = ? WHERE id = ?",
                (json.dumps({"reinforcement_count": 1}), item.id),
            )
            db.commit()
            db.close()

            restarted = fx.store().memory_item_repo
            result = restarted.update_item(item_id=item.id, summary="t2")

            assert "content_hash" not in fx.raw_extra(item.id)
            assert "content_hash" not in (result.extra or {})
            assert "content_hash" not in (restarted.items[item.id].extra or {})

    def test_a_present_but_empty_hash_is_not_enrolled(self, tmp_path):
        """The only input where the guard and the SQL predicate disagree.

        `json_extract(extra, '$.content_hash') IS NOT NULL` is TRUE for an empty
        string, so the SQL alone would enroll a row whose hash is present but
        blank. The Python guard is what keeps "no usable hash" out of hash-dedup,
        and this is the case that proves it is load-bearing rather than a
        duplicate of the SQL.
        """
        with _MemuPatchFixture(tmp_path) as fx:
            store, _ = fx.setup(1)
            item = fx.add_item(store, summary="t")
            extra = fx.raw_extra(item.id)
            extra["content_hash"] = ""
            db = sqlite3.connect(fx.path)
            db.execute(
                "UPDATE memu_memory_items SET extra = ? WHERE id = ?",
                (json.dumps(extra), item.id),
            )
            db.commit()
            db.close()

            restarted = fx.store().memory_item_repo
            result = restarted.update_item(item_id=item.id, summary="t2")

            assert fx.raw_extra(item.id)["content_hash"] == ""
            assert (result.extra or {}).get("content_hash") == ""

    def test_salience_fields_survive_the_extra_merge(self, tmp_path):
        with _MemuPatchFixture(tmp_path) as fx:
            store, _ = fx.setup(1)
            item = fx.add_item(store, summary="t")
            fx.add_item(store, summary="t")  # exact-hash reinforce -> rc = 2
            before = fx.raw_extra(item.id)
            assert before["reinforcement_count"] == 2

            store.memory_item_repo.update_item(item_id=item.id, summary="t2")

            after = fx.raw_extra(item.id)
            assert after["reinforcement_count"] == 2
            assert after["last_reinforced_at"] == before["last_reinforced_at"]
            assert after["content_hash"] == _content_hash("t2", "knowledge")
            # The cache and the returned item are read by memorize()/recall, so
            # they must carry the SAME merged extra - not just the new hash.
            assert store.memory_item_repo.items[item.id].extra == after

    def test_a_callers_own_extra_is_not_dropped(self, tmp_path):
        """The refreshed hash must be MERGED into the caller's extra, not
        substituted for it: update_item(summary=..., extra={"ref_id": ...}) must
        keep ref_id, which list_items_by_ref_ids filters on.
        """
        with _MemuPatchFixture(tmp_path) as fx:
            store, _ = fx.setup(1)
            item = fx.add_item(store, summary="t")

            store.memory_item_repo.update_item(
                item_id=item.id, summary="t2", extra={"ref_id": "abc"},
            )

            after = fx.raw_extra(item.id)
            assert after["ref_id"] == "abc"
            assert after["content_hash"] == _content_hash("t2", "knowledge")

    def test_stale_hash_reinforces_the_corrected_row(self, tmp_path):
        """The user-visible consequence, both directions in one test."""
        with _MemuPatchFixture(tmp_path) as fx:
            store, _ = fx.setup(1)
            item = fx.add_item(store, summary="original text")
            memu_update = fx.Repo._nerve_memu_update_item

            # Unpatched: correcting the wording, then re-memorizing the OLD
            # text, reinforces the corrected row under its new summary.
            memu_update(store.memory_item_repo, item_id=item.id, summary="corrected text")
            again = fx.add_item(store, summary="original text")
            assert again.id == item.id
            assert fx.raw_count("SELECT count(*) FROM memu_memory_items") == 1
            assert again.summary == "corrected text"

        with _MemuPatchFixture(tmp_path / "b") as fx:
            (tmp_path / "b").mkdir()
            store, _ = fx.setup(1)
            item = fx.add_item(store, summary="original text")

            # Patched: the same sequence recognises the old text as different.
            store.memory_item_repo.update_item(item_id=item.id, summary="corrected text")
            again = fx.add_item(store, summary="original text")
            assert again.id != item.id
            assert fx.raw_count("SELECT count(*) FROM memu_memory_items") == 2


class TestReinforceWritebackReadsTheRow:
    """Fix 7 correction: the semantic-dedup writeback must seed ``extra`` from
    the ROW inside its transaction, not from the item cache.

    Read paths build MemoryItem without extra=, so a cache entry is {} in any
    process that did not itself create the item; writing that back replaced the
    row's whole extra with just the two salience keys. Measured before the fix:
    all 6,473 rows with no content_hash carried reinforcement_count > 1, none
    carried rc == 1, and the store holds 0 items of type "tool" (the only
    create path that legitimately writes no hash).
    """

    @staticmethod
    def _similar_pair():
        return (
            np.array([1.0, 0.0, 0.0], dtype=np.float32),
            np.array([1.0, 0.001, 0.0], dtype=np.float32),
        )

    def _cold_cache_reinforce(self, fx, extra_writer=None):
        first, second = self._similar_pair()
        store, _ = fx.setup(1)
        item = fx.add_item(store, summary="cold cache subject", embedding=first)
        if extra_writer is not None:
            extra_writer(item.id)
        store.close()

        # Simulated restart: a fresh store whose cache is filled by a READ.
        restarted = fx.store()
        restarted.memory_item_repo.list_items()
        assert restarted.memory_item_repo.items[item.id].extra == {}

        restarted.memory_item_repo.create_item_reinforce(
            resource_id=None, memory_type="knowledge",
            summary="cold cache subject!!", embedding=second, user_data={},
        )
        return item, restarted

    def test_content_hash_survives_a_cold_cache_reinforce(self, tmp_path):
        with _MemuPatchFixture(tmp_path) as fx:
            item, _ = self._cold_cache_reinforce(fx)

            after = fx.raw_extra(item.id)
            assert "content_hash" in after
            assert after["reinforcement_count"] == 2

    def test_a_third_party_writers_key_survives(self, tmp_path):
        """The arm that distinguishes this from hydrating the read paths.

        _resolve_event_dates_sync adds extra.mentioned_at with raw SQL and never
        refreshes the cache, so hydrating reads cannot keep the cache canonical:
        with read-path hydration installed, content_hash survived here but
        mentioned_at was still destroyed. Reading the row inside the write
        transaction is correct for every writer, present and future.
        """
        with _MemuPatchFixture(tmp_path) as fx:
            def write_mentioned_at(item_id):
                db = sqlite3.connect(fx.path)
                extra = fx.raw_extra(item_id)
                extra["mentioned_at"] = "2026-01-01"
                db.execute(
                    "UPDATE memu_memory_items SET extra = ? WHERE id = ?",
                    (json.dumps(extra), item_id),
                )
                db.commit()
                db.close()

            item, _ = self._cold_cache_reinforce(fx, write_mentioned_at)

            after = fx.raw_extra(item.id)
            assert after["mentioned_at"] == "2026-01-01"
            assert "content_hash" in after

    def test_cache_matches_the_row_afterwards(self, tmp_path):
        with _MemuPatchFixture(tmp_path) as fx:
            item, restarted = self._cold_cache_reinforce(fx)

            assert restarted.memory_item_repo.items[item.id].extra == fx.raw_extra(item.id)

    def test_reinforce_return_value_still_signals_rc_gt_1(self, tmp_path):
        """memorize() skips category linking when the returned item's
        extra.reinforcement_count > 1. The correction changes what that extra
        holds, so pin the DECISION: if it flipped, a reinforced item would stop
        getting its category links, i.e. a new orphan source.
        """
        first, second = self._similar_pair()
        with _MemuPatchFixture(tmp_path) as fx:
            store, _ = fx.setup(1)
            fresh = fx.add_item(store, summary="subject", embedding=first)
            assert fresh.extra.get("reinforcement_count", 1) == 1
            store.close()

            restarted = fx.store()
            restarted.memory_item_repo.list_items()
            reinforced = restarted.memory_item_repo.create_item_reinforce(
                resource_id=None, memory_type="knowledge", summary="subject!!",
                embedding=second, user_data={},
            )

            assert reinforced.extra.get("reinforcement_count", 1) > 1

    def test_read_paths_still_omit_extra(self, tmp_path):
        """Pin the deliberate scope boundary: the read-path hydration gap is
        DOCUMENTED, not fixed here. With the writeback reading the row, an
        empty cached extra is no longer destructive, so hydration is a separate
        change. If a future PR hydrates the reads, this test should be updated,
        not silently kept passing by accident.
        """
        with _MemuPatchFixture(tmp_path) as fx:
            store, _ = fx.setup(1)
            item = fx.add_item(store, summary="t")
            store.close()

            restarted = fx.store()
            assert restarted.memory_item_repo.get_item(item.id).extra == {}
            restarted.memory_item_repo.items.clear()
            assert restarted.memory_item_repo.list_items()[item.id].extra == {}


class TestCascadeDeleteItem:
    """Fix 10: delete_item must not leave the item's category relations behind.

    There is no FK and no ON DELETE CASCADE on memu_category_items, and no
    layer owns the dependent rows: _patch_delete_memory_item reads the item's
    categories only to build category_updates, then deletes the item row.
    Measured before the fix: 6,455 dangling relations, every one of the 5,611
    distinct dangling item_ids present in the item_deleted audit log. They also
    inflate memory_expand_category's reported total (which counts relations
    while listing through a JOIN) by 3.5-4.4 percent on every category.
    """

    def test_relations_and_caches_are_removed(self, tmp_path):
        with _MemuPatchFixture(tmp_path) as fx:
            store, cats = fx.setup(2)
            item = fx.add_item(store, summary="doomed")
            fx.link_all(store, item.id, cats)

            store.memory_item_repo.delete_item(item.id)

            assert fx.raw_count(
                "SELECT count(*) FROM memu_category_items WHERE item_id = ?", item.id,
            ) == 0
            assert fx.raw_count(
                "SELECT count(*) FROM memu_memory_items WHERE id = ?", item.id,
            ) == 0
            assert item.id not in store.memory_item_repo.items
            # DatabaseState.relations is read directly by memu's retrieve path.
            assert [r for r in store.category_item_repo.relations
                    if r.item_id == item.id] == []

    def test_unpatched_delete_orphans_the_relations(self, tmp_path):
        with _MemuPatchFixture(tmp_path) as fx:
            memu_delete = fx.Repo._nerve_memu_delete_item

            store, cats = fx.setup(2)
            item = fx.add_item(store, summary="doomed")
            fx.link_all(store, item.id, cats)

            memu_delete(store.memory_item_repo, item.id)

            assert fx.raw_count(
                "SELECT count(*) FROM memu_memory_items WHERE id = ?", item.id,
            ) == 0
            assert fx.raw_count(
                "SELECT count(*) FROM memu_category_items WHERE item_id = ?", item.id,
            ) == 2

    def test_missing_id_and_unlinked_item_are_noops(self, tmp_path):
        with _MemuPatchFixture(tmp_path) as fx:
            store, _ = fx.setup(1)
            store.memory_item_repo.delete_item("no-such-id")
            item = fx.add_item(store, summary="t")
            store.memory_item_repo.delete_item(item.id)
            assert fx.raw_count("SELECT count(*) FROM memu_memory_items") == 0

    def test_a_failed_item_delete_rolls_the_relations_back(self, tmp_path):
        """Atomicity. Deleting relations in their own transaction and then
        delegating would leave a SURVIVING item stripped of its links on a
        failure - strictly worse than the dangling rows it set out to fix.
        SQLiteSessionManager.session() returns a fresh Session per call, so one
        session for both deletes is the only way to get this.

        The cache assertions matter as much as the DB ones: Fix 5 serves
        self.items unfiltered, so evicting before (or despite) a rolled-back
        transaction makes the SURVIVING row invisible to recall.
        """
        with _MemuPatchFixture(tmp_path) as fx:
            store, cats = fx.setup(2)
            item = fx.add_item(store, summary="t")
            fx.link_all(store, item.id, cats)
            # A bystander keeps the cache non-empty so Fix 5 serves the CACHE:
            # with an empty cache list_items() falls through to the DB and the
            # cache assertion below would be free.
            other = fx.add_item(store, summary="bystander")
            store.category_item_repo.link_item_category(
                other.id, next(iter(cats.values())), user_data={},
            )

            class _Boom(Exception):
                pass

            sessions = store.memory_item_repo._sessions
            original_session = sessions.session

            def failing_session():
                session = original_session()
                def _raise(_obj):
                    raise _Boom("forced")
                session.delete = _raise
                return session

            sessions.session = failing_session
            try:
                with pytest.raises(_Boom):
                    store.memory_item_repo.delete_item(item.id)
            finally:
                sessions.session = original_session

            assert fx.raw_count(
                "SELECT count(*) FROM memu_category_items WHERE item_id = ?", item.id,
            ) == 2
            assert fx.raw_count(
                "SELECT count(*) FROM memu_memory_items WHERE id = ?", item.id,
            ) == 1
            # The caches must match the rolled-back DB, or the surviving row is
            # unreachable through recall.
            assert item.id in store.memory_item_repo.items
            assert len([r for r in store.category_item_repo.relations
                        if r.item_id == item.id]) == 2
            assert item.id in store.memory_item_repo.list_items()

    def test_a_failed_relations_delete_rolls_the_item_back(self, tmp_path):
        """The other direction, and the one that pins the ordering.

        session.delete (the ITEM row) is the first statement to raise in BOTH
        the one-transaction form and an item-first split, so the sibling case
        above cannot see a split.  Failing only the RELATIONS delete can: under
        a split the item is already committed and gone, leaving 2 dangling rows.
        """
        with _MemuPatchFixture(tmp_path) as fx:
            store, cats = fx.setup(2)
            item = fx.add_item(store, summary="t")
            fx.link_all(store, item.id, cats)
            # As in the sibling case: a bystander keeps the cache non-empty so
            # the list_items() assertion exercises Fix 5's cached path.  It is
            # created BEFORE the interceptor is installed, and `raised` below
            # PROVES the extra item did not change which statement raises rather
            # than assuming the statement-text predicate is unaffected.
            other = fx.add_item(store, summary="bystander")
            store.category_item_repo.link_item_category(
                other.id, next(iter(cats.values())), user_data={},
            )

            class _Boom(Exception):
                pass

            sessions = store.memory_item_repo._sessions
            original_session = sessions.session
            raised = []

            def failing_session():
                session = original_session()
                real_exec = session.exec

                def _exec(stmt, *args, **kwargs):
                    text = str(stmt).lower().strip()
                    # Only the relations DELETE; everything else must really run
                    # or the row lookup fails and the test proves nothing.
                    if text.startswith("delete") and "category_items" in text:
                        raised.append(text)
                        raise _Boom("forced")
                    return real_exec(stmt, *args, **kwargs)

                session.exec = _exec
                return session

            sessions.session = failing_session
            try:
                with pytest.raises(_Boom):
                    store.memory_item_repo.delete_item(item.id)
            finally:
                sessions.session = original_session

            # Exactly one statement raised, and it was the relations DELETE:
            # the bystander did not move the failure point.
            assert len(raised) == 1
            assert "category_items" in raised[0]

            # BOTH rolled back: the item survives with its links intact.
            assert fx.raw_count(
                "SELECT count(*) FROM memu_memory_items WHERE id = ?", item.id,
            ) == 1
            assert fx.raw_count(
                "SELECT count(*) FROM memu_category_items WHERE item_id = ?", item.id,
            ) == 2
            # ... and so must the caches, or recall cannot see the survivor.
            assert item.id in store.memory_item_repo.items
            assert len([r for r in store.category_item_repo.relations
                        if r.item_id == item.id]) == 2
            assert item.id in store.memory_item_repo.list_items()

    def test_cache_is_evicted_when_the_row_is_already_gone(self, tmp_path):
        """The row can vanish underneath us (another process deleted it).

        Returning early on a missing row would keep the id in self.items, and
        Fix 5 serves that cache unfiltered, so list_items() would go on
        returning a deleted item while Fix 3 removed it from the vector index.
        memU's own delete_item pops the cache unconditionally.
        """
        with _MemuPatchFixture(tmp_path) as fx:
            store, cats = fx.setup(1)
            item = fx.add_item(store, summary="doomed")
            fx.link_all(store, item.id, cats)
            assert item.id in store.memory_item_repo.items

            # Another connection removes the row underneath us.
            conn = sqlite3.connect(fx.path)
            conn.execute("DELETE FROM memu_memory_items WHERE id = ?", (item.id,))
            conn.commit()
            conn.close()

            store.memory_item_repo.delete_item(item.id)

            assert item.id not in store.memory_item_repo.items
            assert [r for r in store.category_item_repo.relations
                    if r.item_id == item.id] == []
            # list_items() returns the cache dict itself (keyed by id).
            assert item.id not in store.memory_item_repo.list_items()

    def test_fix_3_index_hook_stays_outermost(self, tmp_path):
        """Fix 10 REPLACES the base implementation, so it must be installed
        BEFORE Fix 3 (the opposite order from the wrapper-style fixes) or the
        vector-index remove() hook would be buried and stop firing.
        """
        with _MemuPatchFixture(tmp_path) as fx:
            assert fx.Repo.delete_item.__qualname__.endswith("_indexed_delete_item")

            store, _ = fx.setup(1)
            item = fx.add_item(
                store, summary="t", embedding=np.array([1.0, 0.0], dtype=np.float32),
            )
            removed = []

            class _Index:
                dirty = False
                seen_items_len = 0

                def remove(self, item_id):
                    removed.append(item_id)

            store.memory_item_repo._nerve_vec_index = _Index()
            store.memory_item_repo.delete_item(item.id)

            assert removed == [item.id]


class TestWritePathPatchStructure:
    """Structural tripwires for the assumptions the three fixes rest on."""

    def test_patching_twice_does_not_stack(self, tmp_path):
        def chain(fn):
            names = []
            while fn is not None and hasattr(fn, "__closure__"):
                names.append(fn.__qualname__.rsplit(".", 1)[-1])
                nxt = None
                for cell in (fn.__closure__ or ()):
                    value = cell.cell_contents
                    if callable(value) and getattr(value, "__qualname__", "").endswith(
                        ("update_item", "delete_item"),
                    ):
                        nxt = value
                        break
                fn = nxt
            return names

        with _MemuPatchFixture(tmp_path) as fx:
            first_update = chain(fx.Repo.update_item)
            first_delete = chain(fx.Repo.delete_item)
            handler = fx.CRUDMixin._patch_update_memory_item

            MemUBridge._patch_sqlite_bugs()
            MemUBridge._patch_sqlite_bugs()

            assert chain(fx.Repo.update_item) == first_update
            assert chain(fx.Repo.delete_item) == first_delete
            assert fx.CRUDMixin._patch_update_memory_item is handler

    def test_the_handler_the_service_resolves_is_the_one_patched(self, tmp_path):
        """Fix 8 patches a class attribute, and PipelineManager captures the
        BOUND method during MemoryService.__init__ - which runs AFTER
        _patch_sqlite_bugs() in _initialize_impl, so the patch is picked up.
        """
        with _MemuPatchFixture(tmp_path):
            from memu.app.crud import CRUDMixin
            from memu.app.service import MemoryService

            assert (MemoryService._patch_update_memory_item
                    is CRUDMixin._patch_update_memory_item)

    def test_map_category_names_to_ids_copies_agree(self):
        """_map_category_names_to_ids exists in BOTH CRUDMixin and
        MemorizeMixin, and the MRO resolves MemorizeMixin's copy. Fix 8 calls it
        late-bound via self. so it follows the MRO; this pins that the two
        copies cannot silently diverge.
        """
        import inspect

        import memu.app.service  # noqa: F401
        from memu.app.crud import CRUDMixin
        from memu.app.memorize import MemorizeMixin
        from memu.app.service import MemoryService

        assert (MemoryService._map_category_names_to_ids
                is MemorizeMixin._map_category_names_to_ids)
        assert (inspect.getsource(CRUDMixin._map_category_names_to_ids)
                == inspect.getsource(MemorizeMixin._map_category_names_to_ids))

    def test_patch_mixin_duplicates_are_dead_code(self):
        """memu.app.patch.PatchMixin holds byte-equivalent duplicates of both
        workflow handlers. Nothing inherits or instantiates it, so it needs no
        patch - assert that, so a future memU version wiring it up fails here.
        """
        import memu.app.patch as patch_mod
        from memu.app.service import MemoryService

        assert patch_mod.PatchMixin.__subclasses__() == []
        assert patch_mod.PatchMixin not in MemoryService.__mro__

    def test_clear_items_remains_unreachable_from_nerve(self):
        """clear_items also leaves dangling relations, but its only memU caller
        is CRUDMixin.clear_memory, which nerve never calls. Fixing bulk-delete
        semantics is a separate concern; this pins the exemption so it cannot
        rot silently.
        """
        from pathlib import Path

        package_root = Path(nerve_memory_bridge_file()).parents[1]
        assert package_root.name == "nerve", package_root
        hits = sorted(
            str(path.relative_to(package_root))
            for path in package_root.rglob("*.py")
            if "clear_memory" in path.read_text(encoding="utf-8", errors="ignore")
        )
        assert hits == []

    def test_salience_ranking_branch_is_unreachable_today(self):
        """_fast_vector_search's salience branch reads extra from the cache.
        It is not a live defect either way, because ranking defaults to
        "similarity" and nerve never sets it - assert that rather than claiming
        this PR fixes or regresses salience ranking.
        """
        from memu.app.settings import RetrieveItemConfig

        assert RetrieveItemConfig().ranking == "similarity"


def nerve_memory_bridge_file():
    from nerve.memory import memu_bridge

    return memu_bridge.__file__
