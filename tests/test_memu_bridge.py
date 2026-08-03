"""Tests for nerve.memory.memu_bridge — event date resolution & knowledge filtering."""

import asyncio
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from nerve.config import MemoryCategoryConfig, MemoryConfig, NerveConfig
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

        # Snapshot the item-repo methods _patch_sqlite_bugs() reassigns so the
        # test restores global state and does not leak into other tests.
        names = (
            "update_item", "delete_item", "clear_items", "list_items",
            "create_item", "create_item_reinforce", "vector_search_items",
        )
        saved = {n: Repo.__dict__.get(n) for n in names}

        Repo.update_item = spy_update
        try:
            # Returns None on success (the body falls through); the observable
            # effect is that update_item gets wrapped in front of our spy.
            MemUBridge._patch_sqlite_bugs()
            assert Repo.update_item is not spy_update

            stub = object.__new__(Repo)  # no _nerve_vec_index → index hook skipped
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


# ---------------------------------------------------------------------------
# The advertised category view (_rebuild_category_view)
# ---------------------------------------------------------------------------

# memU's SQLModel table classes are process-global: building a second set raises
# "Column object 'url' already assigned to Table 'memu_resources'".  Build them
# once and inject them, so each test still gets a real store on a fresh file.
_CATEGORY_SQLA: dict[str, object] = {}


def _category_store(db_path: Path):
    """A genuine memU SQLite store, safe to build repeatedly in one process."""
    import memu.app.service  # noqa: F401 - first, else the patcher hits a circular import

    MemUBridge._patch_sqlite_bugs()  # required before the first store is built
    from memu.database.sqlite.schema import get_sqlite_sqlalchemy_models
    from memu.database.sqlite.sqlite import SQLiteStore
    from pydantic import BaseModel

    class _Scope(BaseModel):
        pass

    if "models" not in _CATEGORY_SQLA:
        _CATEGORY_SQLA["scope"] = _Scope
        _CATEGORY_SQLA["models"] = get_sqlite_sqlalchemy_models(scope_model=_Scope)
    return SQLiteStore(
        dsn=f"sqlite:///{db_path}",
        scope_model=_CATEGORY_SQLA["scope"],
        sqla_models=_CATEGORY_SQLA["models"],
    )


class _ViewService:
    """Stand-in for MemoryService's category state over a REAL store.

    Mirrors service.py exactly: ``category_configs`` is a *copy* of
    ``memorize_config.memory_categories``, and the prompt is memU's own
    formatter, so a run against unfixed code shows its true behaviour.
    """

    def __init__(self, store, effective):
        from memu.app.memorize import MemorizeMixin
        from memu.app.service import CategoryConfig

        self.memorize_config = MagicMock()
        self.memorize_config.memory_categories = [
            CategoryConfig(name=n, description=d) for n, d in effective
        ]
        self.category_configs = list(self.memorize_config.memory_categories)
        self.category_config_map = {c.name: c for c in self.category_configs}
        self._format_categories_for_prompt = (
            lambda cfgs: MemorizeMixin._format_categories_for_prompt(self, cfgs)
        )
        self._category_prompt_str = self._format_categories_for_prompt(self.category_configs)
        self.database = store
        self._context = SimpleNamespace(
            category_ids=[], category_name_to_id={}, categories_ready=False,
        )

    def _get_context(self):
        return self._context


def _view_bridge(tmp_path, effective, *, configured=None, rows=(), db_name="memu.sqlite"):
    """A bridge wired for the view helper only: stub service, real store, real rows."""
    config = _make_config(tmp_path)
    config.memory.categories = [
        MemoryCategoryConfig(name=n, description=d)
        for n, d in (configured if configured is not None else effective)
    ]
    bridge = MemUBridge(config, audit_db=None)
    store = _category_store(tmp_path / db_name)
    for name, desc in rows:
        store.memory_category_repo.get_or_create_category(
            name=name, description=desc, embedding=[], user_data={},
        )
    bridge._service = _ViewService(store, effective)
    # The rows above were created through the same store the service holds, so
    # they are already in its cache -- what _initialize_impl's hydration does.
    return bridge


def _advertised(bridge):
    return [c.name for c in bridge._service.category_configs]


def _prompt_lines(bridge):
    return [ln for ln in bridge._service._category_prompt_str.splitlines() if ln.strip()]


_VIEW_INIT_PROBE = """
import asyncio, json, sqlite3, sys
from pathlib import Path
import memu.app.service  # first: avoids a circular import in the patcher
from nerve.config import MemoryCategoryConfig, MemoryConfig, NerveConfig
from nerve.memory.memu_bridge import MemUBridge

async def main():
    tmp = Path(sys.argv[1]); configured = json.loads(sys.argv[2]); mode = sys.argv[3]
    db = tmp / "memu.sqlite"
    cfg = NerveConfig()
    cfg.memory = MemoryConfig(
        sqlite_dsn=f"sqlite:///{db}",
        categories=[MemoryCategoryConfig(name=n, description=d) for n, d in configured],
    )
    cfg.anthropic_api_key = "test-key"
    if mode == "preseed":
        # A previous process: nerve's own init, then web-UI-style creates.
        bridge = MemUBridge(cfg, audit_db=None)
        ok = await bridge.initialize()
        for name, desc in json.loads(sys.argv[4]):
            if not any(name == n for n, _ in configured):
                await bridge.create_category(name, desc, source="web_ui")
        await bridge.shutdown()
        print("PROBE_JSON " + json.dumps({"initialize": ok}))
        return
    MemUBridge._patch_sqlite_bugs()
    import memu.database.sqlite.repositories.memory_category_repo as m
    if mode == "fail-load":
        def _boom(self, where=None):
            raise RuntimeError("list_categories exploded")
        m.SQLiteMemoryCategoryRepo.list_categories = _boom
    else:
        # Record busy_timeout as seen BY EACH category read, in order.  A
        # timeout read after init cannot discriminate the pragma ordering,
        # because dispose() fixes every later connection; only the connection
        # the read itself uses can.
        _timeouts = []
        _real_list = m.SQLiteMemoryCategoryRepo.list_categories
        def _watched(self, where=None):
            try:
                with self._sessions.engine.connect() as conn:
                    _timeouts.append(conn.exec_driver_sql("PRAGMA busy_timeout").scalar())
            except Exception as exc:
                _timeouts.append("error: %s" % exc)
            return _real_list(self, where)
        m.SQLiteMemoryCategoryRepo.list_categories = _watched
    bridge = MemUBridge(cfg, audit_db=None)
    ok = await bridge.initialize()
    out = {"initialize": ok, "available": bridge._available,
           "service_available": bridge._metrics.service_available}
    if mode != "fail-load":
        out["busy_timeout_per_category_read"] = _timeouts
    if bridge._service is not None:
        svc = bridge._service
        out["advertised"] = [c.name for c in svc.category_configs]
        out["prompt_lines"] = [l for l in svc._category_prompt_str.splitlines() if l.strip()]
        out["descs"] = {c.name: c.description for c in svc.category_configs}
        await bridge.shutdown()
    con = sqlite3.connect(db)
    try:
        out["file_rows"] = sorted(r[0] for r in
                                  con.execute("select name from memu_memory_categories"))
    except Exception:
        out["file_rows"] = []
    con.close()
    print("PROBE_JSON " + json.dumps(out))

asyncio.run(main())
"""


def _run_view_init(tmp_path, configured=(), mode="normal", preseed=()):
    """Run full MemUBridge.initialize() runs in subprocesses and report the view.

    Out of process because memU allows exactly one MemoryService per
    interpreter; the preseed is its own process precisely so it is a genuine
    "previous run" rather than a warm cache in this one.
    """
    import subprocess

    root = str(Path(__file__).resolve().parent.parent)
    conf = json.dumps([list(c) for c in configured])

    def _run(*args):
        proc = subprocess.run(
            [sys.executable, "-c", _VIEW_INIT_PROBE, str(tmp_path), conf, *args],
            capture_output=True, text=True, timeout=300, cwd=root,
        )
        line = next((ln for ln in proc.stdout.splitlines()
                     if ln.startswith("PROBE_JSON ")), None)
        assert line is not None, (
            f"probe produced no report\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
        return json.loads(line[len("PROBE_JSON "):])

    if preseed:
        assert _run("preseed", json.dumps([list(p) for p in preseed]))["initialize"] is True
    return _run(mode)


class TestRebuildCategoryView:
    """``category_configs`` is the ONLY set memU advertises to the memorize LLM.

    nerve filled it from config once and then only ever appended, so a category
    persisted by an earlier process was never advertised (it could not receive
    new memories) while a configured one was advertised twice.
    """

    def test_a_persisted_runtime_category_is_advertised(self, tmp_path):
        """Defect A: a web-created category must survive a restart."""
        bridge = _view_bridge(
            tmp_path, [("procedures", "cfg desc")],
            rows=[("procedures", "cfg desc"), ("my_topic", "user made this")],
        )

        bridge._rebuild_category_view()

        assert _advertised(bridge) == ["procedures", "my_topic"]
        assert "- my_topic: user made this" in _prompt_lines(bridge)
        assert bridge._service.category_config_map["my_topic"].description == "user made this"

    def test_the_config_description_wins_over_the_persisted_row(self, tmp_path):
        """Precedence: get_or_create_category ignores description on a name hit,
        so a row keeps its original text forever.  Letting the row win would
        make a config.yaml edit stop reaching the prompt."""
        bridge = _view_bridge(
            tmp_path, [("procedures", "NEW config desc")],
            rows=[("procedures", "OLD db desc")],
        )

        bridge._rebuild_category_view()

        assert _prompt_lines(bridge) == ["- procedures: NEW config desc"]
        assert bridge._service.category_config_map["procedures"].description == "NEW config desc"

    def test_rebuilding_repeatedly_does_not_grow_the_advertised_set(self, tmp_path):
        """Defect B: idempotence is what replaces an is-it-present predicate."""
        bridge = _view_bridge(
            tmp_path, [("procedures", "p")], rows=[("procedures", "p"), ("my_topic", "u")],
        )

        for _ in range(5):
            bridge._rebuild_category_view()

        assert _advertised(bridge) == ["procedures", "my_topic"]
        assert len(_prompt_lines(bridge)) == 2

    def test_a_case_variant_row_is_not_dropped(self, tmp_path):
        """Names are compared EXACTLY, as memU stores and matches them.

        The base schema has a non-unique name index and get_or_create_category
        matches exactly, so ``procedures`` and ``PROCEDURES`` are distinct rows;
        deduping on a normalized name would silently omit one of them.
        """
        bridge = _view_bridge(
            tmp_path, [("procedures", "cfg")],
            rows=[("procedures", "cfg"), ("PROCEDURES", "upper")],
        )

        bridge._rebuild_category_view()

        assert _advertised(bridge) == ["procedures", "PROCEDURES"]
        assert len(_prompt_lines(bridge)) == 2

    def test_the_persisted_tail_is_name_sorted(self, tmp_path):
        """list_categories issues no ORDER BY, so without a sort the prompt
        would vary run to run; the configured prefix keeps its config order."""
        bridge = _view_bridge(
            tmp_path, [("zzz_configured", "c"), ("aaa_configured", "c")],
            rows=[("zeta", "z"), ("alpha", "a"), ("mid", "m")],
        )

        bridge._rebuild_category_view()

        assert _advertised(bridge) == [
            "zzz_configured", "aaa_configured", "alpha", "mid", "zeta",
        ]

    def test_an_empty_nerve_config_keeps_memus_own_defaults(self, tmp_path):
        """The baseline is memU's EFFECTIVE list, not nerve's config.

        With no configured categories memU substitutes its own defaults and
        advertises them; reading nerve's (empty) config here would wipe them and
        leave the LLM with 'No categories provided.'
        """
        defaults = [(f"memu_default_{i}", f"d{i}") for i in range(10)]
        bridge = _view_bridge(tmp_path, defaults, configured=[])

        bridge._rebuild_category_view()

        assert _advertised(bridge) == [n for n, _ in defaults]
        assert len(_prompt_lines(bridge)) == 10

    def test_an_empty_nerve_config_also_advertises_a_persisted_row(self, tmp_path):
        """Both properties on that path: memU's defaults AND the persisted row."""
        defaults = [(f"memu_default_{i}", f"d{i}") for i in range(10)]
        bridge = _view_bridge(tmp_path, defaults, configured=[], rows=[("my_topic", "u")])

        bridge._rebuild_category_view()

        assert _advertised(bridge) == [n for n, _ in defaults] + ["my_topic"]
        assert len(_prompt_lines(bridge)) == 11

    def test_a_configured_deployment_gets_no_extra_defaults(self, tmp_path):
        """Reverse control: never 'just always use memU's 10 defaults'."""
        bridge = _view_bridge(
            tmp_path, [("task_domain", "d"), ("patterns", "p")], rows=[("my_topic", "u")],
        )

        bridge._rebuild_category_view()

        assert _advertised(bridge) == ["task_domain", "patterns", "my_topic"]

    def test_a_persisted_row_never_becomes_part_of_the_baseline(self, tmp_path):
        """The baseline is memU's immutable effective list, never the view's own
        output: promoting a row into it would make a row that later leaves the
        DB impossible to drop from the prompt."""
        bridge = _view_bridge(tmp_path, [("procedures", "p")], rows=[("my_topic", "u")])
        repo = bridge._service.database.memory_category_repo

        for _ in range(3):
            bridge._rebuild_category_view()
        assert "my_topic" in _advertised(bridge)

        repo.categories.clear()
        bridge._rebuild_category_view()

        assert _advertised(bridge) == ["procedures"]


class TestCreateCategoryKeepsTheViewIdempotent:
    """A repeated create on an existing name used to append forever: nothing
    rebuilt the view after init, and get_or_create_category returns the
    existing row rather than failing."""

    @pytest.mark.asyncio
    async def test_repeated_create_does_not_grow_the_advertised_set(self, tmp_path):
        bridge = _view_bridge(tmp_path, [("procedures", "p")], rows=[("procedures", "p")])
        bridge._audit = AsyncMock()
        repo = bridge._service.database.memory_category_repo

        for _ in range(3):
            assert await bridge._create_category_impl("procedures", "p") is True

        assert _advertised(bridge) == ["procedures"]
        assert len(_prompt_lines(bridge)) == 1
        assert len(repo.list_categories()) == 1

    @pytest.mark.asyncio
    async def test_repeated_create_does_not_duplicate_the_context_id(self, tmp_path):
        """``ctx.category_ids`` is guarded on the ID, NOT on config presence:
        the two invariants are independent (see the reverse-direction test)."""
        bridge = _view_bridge(tmp_path, [("procedures", "p")], rows=[("procedures", "p")])
        bridge._audit = AsyncMock()
        ctx = bridge._service._get_context()

        for _ in range(3):
            await bridge._create_category_impl("procedures", "p")

        assert len(ctx.category_ids) == 1
        assert len(ctx.category_ids) == len(set(ctx.category_ids))

    @pytest.mark.asyncio
    async def test_the_context_id_is_appended_when_only_the_name_is_known(self, tmp_path):
        """Reverse direction, and the arm that rejects sharing R1's predicate:
        on first boot the name is already in ``category_configs`` (memU put it
        there) while its ID is not yet in ``ctx`` -- so a config-presence gate
        would wrongly suppress a needed append."""
        bridge = _view_bridge(tmp_path, [("procedures", "p")], rows=[("procedures", "p")])
        bridge._audit = AsyncMock()
        ctx = bridge._service._get_context()
        assert "procedures" in _advertised(bridge)
        assert ctx.category_ids == []

        await bridge._create_category_impl("procedures", "p")

        assert len(ctx.category_ids) == 1

    @pytest.mark.asyncio
    async def test_a_genuinely_new_category_still_reaches_the_prompt(self, tmp_path):
        """Control against over-suppression: a real create must still land."""
        bridge = _view_bridge(tmp_path, [("procedures", "p")], rows=[("procedures", "p")])
        bridge._audit = AsyncMock()
        ctx = bridge._service._get_context()
        repo = bridge._service.database.memory_category_repo

        assert await bridge._create_category_impl("brand_new", "fresh") is True

        assert _advertised(bridge) == ["procedures", "brand_new"]
        assert "- brand_new: fresh" in _prompt_lines(bridge)
        assert len(_prompt_lines(bridge)) == 2
        assert sorted(c.name for c in repo.list_categories().values()) == [
            "brand_new", "procedures",
        ]
        new_id = next(c.id for c in repo.categories.values() if c.name == "brand_new")
        assert new_id in ctx.category_ids

    @pytest.mark.asyncio
    async def test_the_view_rebuild_makes_no_embedding_call(self, tmp_path):
        """memU's own category producer is dead here (nerve sets
        categories_ready=True and nothing ever sets it False), so surfacing
        persisted rows costs zero embedding calls.  Asserted, not assumed: a
        change that revived that producer would add N API calls per restart."""
        bridge = _view_bridge(tmp_path, [("procedures", "p")], rows=[("my_topic", "u")])
        embed = AsyncMock(side_effect=AssertionError("unexpected embedding call"))
        bridge._service._get_llm_client = lambda _p: SimpleNamespace(embed=embed)

        bridge._rebuild_category_view()

        assert "my_topic" in _advertised(bridge)
        assert embed.await_count == 0


class TestUpdateCategoryRefreshesTheView:
    """``_update_category_impl`` wrote the row and re-embedded but left the
    advertised description stale for the rest of the process."""

    @pytest.mark.asyncio
    async def test_a_description_edit_reaches_the_prompt(self, tmp_path):
        bridge = _view_bridge(
            tmp_path, [("procedures", "cfg desc")],
            rows=[("procedures", "cfg desc"), ("my_topic", "original")],
        )
        bridge._audit = AsyncMock()
        bridge._rebuild_category_view()
        repo = bridge._service.database.memory_category_repo
        cid = next(c.id for c in repo.categories.values() if c.name == "my_topic")

        assert await bridge._update_category_impl(cid, description="EDITED") is True

        assert "- my_topic: EDITED" in _prompt_lines(bridge)
        assert bridge._service.category_config_map["my_topic"].description == "EDITED"

    @pytest.mark.asyncio
    async def test_an_edit_does_not_override_a_configured_description(self, tmp_path):
        """The precedence half, end to end: config still wins after a UI edit."""
        bridge = _view_bridge(
            tmp_path, [("procedures", "cfg desc")], rows=[("procedures", "cfg desc")],
        )
        bridge._audit = AsyncMock()
        repo = bridge._service.database.memory_category_repo
        cid = next(c.id for c in repo.categories.values() if c.name == "procedures")

        assert await bridge._update_category_impl(cid, description="EDITED") is True

        assert _prompt_lines(bridge) == ["- procedures: cfg desc"]
        assert repo.categories[cid].description == "EDITED"


class TestInitializeHydratesPersistedCategories:
    """End-to-end over a real MemoryService: the view must reflect the DB, and
    the load must happen on a tuned connection before availability is published."""

    def test_a_category_from_an_earlier_process_is_advertised(self, tmp_path):
        report = _run_view_init(
            tmp_path, configured=[("procedures", "cfg desc")],
            preseed=[("my_topic", "user made this")],
        )

        assert report["initialize"] is True
        assert report["file_rows"] == ["my_topic", "procedures"]
        assert report["advertised"] == ["procedures", "my_topic"]
        assert "- my_topic: user made this" in report["prompt_lines"]

    def test_a_first_boot_does_not_advertise_a_configured_category_twice(self, tmp_path):
        report = _run_view_init(tmp_path, configured=[("procedures", "p")])

        assert report["initialize"] is True
        assert report["advertised"] == ["procedures"]
        assert report["prompt_lines"] == ["- procedures: p"]

    def test_an_empty_config_advertises_defaults_and_persisted_rows(self, tmp_path):
        """The injection-free discriminator for the early hydration: on this path
        nothing else ever reads the category table (``_ensure_categories``
        early-returns), so without it the persisted row is invisible."""
        report = _run_view_init(tmp_path, configured=[], preseed=[("my_topic", "u")])

        assert report["initialize"] is True
        assert report["file_rows"] == ["my_topic"]
        assert len(report["advertised"]) == 11
        assert report["advertised"][-1] == "my_topic"
        assert report["descs"]["my_topic"] == "u"

    def test_the_load_runs_on_a_tuned_connection(self, tmp_path):
        """The pragmas are attached before the load, not after: dispose() fixes
        future connections but cannot re-run a completed read, so the load would
        otherwise run without the 30s busy_timeout that stops
        'database is locked' under concurrent writers.

        The observable has to be the timeout seen by the FIRST category read.
        A timeout read after init passes either way (dispose() already recycled
        that connection), so it would not discriminate the ordering at all.
        """
        report = _run_view_init(
            tmp_path, configured=[("procedures", "p")], preseed=[("my_topic", "u")],
        )

        seen = report["busy_timeout_per_category_read"]
        assert seen, "no category read was observed"
        assert seen[0] == 30000, f"the load ran untuned: {seen}"
        assert set(seen) == {30000}
        assert "my_topic" in report["advertised"]

    def test_availability_is_not_published_when_the_load_fails(self, tmp_path):
        """The load precedes the availability flags, so a failure cannot leave
        the bridge advertising a service it could not initialize."""
        report = _run_view_init(tmp_path, configured=[("procedures", "p")], mode="fail-load")

        assert report["initialize"] is False
        assert report["available"] is False
        assert report["service_available"] is False
        assert report["file_rows"] == []
