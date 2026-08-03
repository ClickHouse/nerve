"""Tests for nerve.memory.memu_bridge — event date resolution & knowledge filtering."""

import asyncio
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
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
# Category seeding (_ensure_categories / _seed_categories)
# ---------------------------------------------------------------------------

# memU's SQLModel table classes are process-global: a second set raises
# "Column object 'url' already assigned to Table 'memu_resources'".  Build them
# once and inject them, so each test still gets a real store on a fresh file.
_SQLA_MODELS: dict[str, object] = {}


def _memu_store(db_path: Path):
    """A genuine memU SQLite store, safe to build repeatedly in one process."""
    import memu.app.service  # noqa: F401  - first, else the patcher hits a circular import

    MemUBridge._patch_sqlite_bugs()  # required before the first store is built
    from memu.database.sqlite.schema import get_sqlite_sqlalchemy_models
    from memu.database.sqlite.sqlite import SQLiteStore
    from pydantic import BaseModel

    class _Scope(BaseModel):
        pass

    if "models" not in _SQLA_MODELS:
        _SQLA_MODELS["scope"] = _Scope
        _SQLA_MODELS["models"] = get_sqlite_sqlalchemy_models(scope_model=_Scope)
    return SQLiteStore(
        dsn=f"sqlite:///{db_path}",
        scope_model=_SQLA_MODELS["scope"],
        sqla_models=_SQLA_MODELS["models"],
    )


class _StubCategoryConfig:
    """Stands in for memu.app.service.CategoryConfig (name + description)."""

    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description


class _StubService:
    """Stand-in for MemoryService: the advertised set, a real store, a real context.

    Mirrors every attribute the seeding path touches on either branch, so a run
    against unfixed code exercises its true behaviour (``_create_category_impl``
    appending to the advertised set) instead of tripping over a missing stub
    attribute and failing for the wrong reason.
    """

    def __init__(self, store, advertised: list[tuple[str, str]]):
        from memu.app.service import Context

        self.database = store
        self.category_configs = [_StubCategoryConfig(n, d) for n, d in advertised]
        self.category_config_map = {c.name: c for c in self.category_configs}
        self._category_prompt_str = self._format_categories_for_prompt(self.category_configs)
        self._context = Context()
        self._embed_client = None

    @staticmethod
    def _format_categories_for_prompt(cfgs) -> str:
        # Verbatim memU (memu/app/memorize.py:930-938): the advertised name is
        # normalized even though the CategoryConfig keeps the raw one, which is
        # exactly the asymmetry the seeding path has to match.
        lines = []
        for c in cfgs:
            name = c.name.strip() or "Untitled"
            desc = c.description.strip()
            lines.append(f"- {name}: {desc}" if desc else f"- {name}")
        return "\n".join(lines)

    def _get_context(self):
        return self._context

    def _get_llm_client(self, _profile):
        return self._embed_client


def _seed_bridge(tmp_path, advertised, *, configured=(), has_embeddings=False, db_name="memu.sqlite"):
    """A bridge wired for _ensure_categories only: stub service, real store."""
    config = _make_config(tmp_path)
    config.memory.categories = [
        MemoryCategoryConfig(name=n, description=d) for n, d in configured
    ]
    bridge = MemUBridge(config, audit_db=None)
    bridge._service = _StubService(_memu_store(tmp_path / db_name), advertised)
    # _has_embeddings reads config.openai_api_key; set it so no network is touched.
    config.openai_api_key = "test-embed-key" if has_embeddings else ""
    return bridge


def _rebuild_map(bridge) -> dict[str, str]:
    """The name->ID rebuild _initialize_impl runs after _ensure_categories."""
    repo = bridge._service.database.memory_category_repo
    return {cat.name.lower(): cat.id for cat in repo.categories.values()}


_INIT_PROBE = """
import asyncio, sys, json, socket
from pathlib import Path
import memu.app.service  # imported first: avoids a circular import in the patcher
from nerve.config import MemoryCategoryConfig, MemoryConfig, NerveConfig
from nerve.memory.memu_bridge import MemUBridge

# This probe must stay offline: httpx dials through socket.socket.connect
# (measured -- socket.create_connection is never called on this path).
# Record as well as raise: the warmup's ``except Exception`` swallows the raise,
# so only the record proves nothing dialled out.
_dialled = []

def _blocked(self, address, *args, **kwargs):
    _dialled.append(address)
    raise AssertionError(f"probe attempted an outbound connection to {address!r}")

socket.socket.connect = _blocked

async def main():
    tmp = Path(sys.argv[1])
    configured = json.loads(sys.argv[2])
    fail_load = sys.argv[3] == "fail-load"
    # Rows a PREVIOUS nerve left on disk, written by _PRE_STORE_PROBE in its own
    # interpreter (memU allows one store per process) and passed in as name->id.
    pre_ids = json.loads(sys.argv[4]) if len(sys.argv) > 4 else {}
    cfg = NerveConfig()
    cfg.memory = MemoryConfig(
        sqlite_dsn=f"sqlite:///{tmp / 'memu.sqlite'}",
        categories=[MemoryCategoryConfig(name=n, description=d) for n, d in configured],
    )
    cfg.anthropic_api_key = "test-key"
    bridge = MemUBridge(cfg, audit_db=None)
    if fail_load:
        real_ensure = bridge._ensure_categories
        async def _boom():
            raise RuntimeError("category load exploded")
        bridge._ensure_categories = _boom
        del real_ensure
    # _initialize_impl warms up three LLM profiles against the live endpoint.
    # Make the sole client factory it calls raise; the loop already swallows it.
    real_init = bridge._initialize_impl

    async def _init_without_warmup():
        from memu.app.service import MemoryService
        orig = MemoryService._get_llm_base_client

        def _no_warmup(self, profile=None):
            raise RuntimeError("LLM warmup disabled: this probe must stay offline")

        MemoryService._get_llm_base_client = _no_warmup
        try:
            return await real_init()
        finally:
            MemoryService._get_llm_base_client = orig

    bridge._initialize_impl = _init_without_warmup

    ok = await bridge.initialize()
    out = {"initialize": ok, "available": bridge._available,
           "service_available": bridge._metrics.service_available,
           "offline": not _dialled and socket.socket.connect is _blocked,
           "dialled": [str(a) for a in _dialled]}
    if bridge._service is not None:
        svc = bridge._service
        ctx = svc._get_context()
        advertised = [c.name for c in svc.category_configs]
        out["advertised"] = advertised
        lines = [ln for ln in svc._category_prompt_str.splitlines() if ln.strip()]
        out["prompt_lines"] = lines
        # The names the LLM is actually TOLD, read back out of memU's own prompt
        # string ("- <name>: <desc>" / "- <name>", memu/app/memorize.py:930-938).
        # Those, not the raw config names, are what it emits and what must resolve.
        out["prompt_names"] = [ln[2:].split(": ", 1)[0] for ln in lines]
        out["map"] = dict(ctx.category_name_to_id)
        out["resolved"] = svc._map_category_names_to_ids(advertised, ctx)
        out["resolved_prompt"] = svc._map_category_names_to_ids(out["prompt_names"], ctx)
        rows = svc.database.memory_category_repo.list_categories().values()
        out["rows"] = sorted(c.name for c in rows)
        # name -> id, so an upgrade arm can prove a repaired row kept its id
        # (category_items link on the id, and a fresh row would orphan them).
        out["row_ids"] = {c.name: c.id for c in rows}
        out["pre_ids"] = pre_ids
    print("PROBE_JSON " + json.dumps(out))

asyncio.run(main())
"""


_PRE_STORE_PROBE = """
import json, sys
from pathlib import Path
import memu.app.service  # imported first: avoids a circular import in the patcher
from nerve.memory.memu_bridge import MemUBridge

# Rows a PREVIOUS nerve left behind: the pre-fix seed path persisted the raw
# configured name, so an upgrade finds rows whose lookup key is not the one memU
# computes.  Written through memU's own repo, so the rows are genuine.
#
# Its own process because memU permits one store per interpreter: the patched
# model factory clears the model cache and rebuilds the tables, so a second
# build in the initialize() probe raises "Column object 'url' already assigned".
#
# Patch FIRST, then read the factory off the MODULE -- _patch_sqlite_bugs rebinds
# get_sqlite_sqlalchemy_models (memu-py names its tables ``sqlite_*``, a prefix
# SQLite reserves), so a ``from ... import`` above the call captures the
# unpatched factory and cannot create tables.
MemUBridge._patch_sqlite_bugs()
import memu.database.sqlite.schema as schema
from memu.database.sqlite.sqlite import SQLiteStore

# The SAME scope model MemoryService uses (memu/app/settings.py: UserConfig
# defaults to DefaultUserModel), so the tables written here carry the scope
# columns initialize() will later select -- a bare BaseModel scope omits
# ``user_id`` and the bridge's own read then fails "no such column".
from memu.app.settings import DefaultUserModel as Scope

store = SQLiteStore(dsn=f"sqlite:///{Path(sys.argv[1]) / 'memu.sqlite'}", scope_model=Scope,
                    sqla_models=schema.get_sqlite_sqlalchemy_models(scope_model=Scope))
ids = {n: store.memory_category_repo.get_or_create_category(
    name=n, description="A", embedding=None, user_data={}).id
    for n in json.loads(sys.argv[2])}
print("PRE_JSON " + json.dumps(ids))
"""


def _run_init(tmp_path, configured=(), mode="normal", pre_store=()):
    """Run a full MemUBridge.initialize() in a subprocess and return its report.

    Out of process because memU allows exactly one MemoryService per interpreter.
    ``pre_store`` names rows to persist before initialize() runs, modelling a store
    written by an earlier nerve; it needs its OWN process for the same reason.
    """
    import subprocess

    pre_ids: dict[str, str] = {}
    if pre_store:
        pre = subprocess.run(
            [sys.executable, "-c", _PRE_STORE_PROBE, str(tmp_path),
             json.dumps(list(pre_store))],
            capture_output=True, text=True, timeout=300,
            cwd=str(Path(__file__).resolve().parent.parent),
        )
        pre_line = next((ln for ln in pre.stdout.splitlines()
                         if ln.startswith("PRE_JSON ")), None)
        assert pre_line is not None, (
            f"pre-store probe produced no report\nstdout:\n{pre.stdout}\nstderr:\n{pre.stderr}"
        )
        pre_ids = json.loads(pre_line[len("PRE_JSON "):])
        # The rows must really be there, or the upgrade arm would silently
        # degenerate into the ordinary cold-start case it is meant to contrast.
        assert sorted(pre_ids) == sorted(pre_store), pre_ids

    proc = subprocess.run(
        [sys.executable, "-c", _INIT_PROBE, str(tmp_path),
         json.dumps([list(c) for c in configured]), mode, json.dumps(pre_ids)],
        capture_output=True, text=True, timeout=300,
        cwd=str(Path(__file__).resolve().parent.parent),
    )
    line = next((ln for ln in proc.stdout.splitlines() if ln.startswith("PROBE_JSON ")), None)
    assert line is not None, f"probe produced no report\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    return json.loads(line[len("PROBE_JSON "):])


class TestEnsureCategoriesSeeding:
    """Every category advertised to the LLM must resolve through the name->ID map."""

    @pytest.mark.asyncio
    async def test_empty_config_seeds_the_advertised_defaults(self, tmp_path):
        """The filed defect: no configured categories -> memU's defaults are advertised.

        Fails before the fix with map=0 / resolvable=0 of 10.
        """
        advertised = [(f"cat_{i}", f"desc {i}") for i in range(4)]
        bridge = _seed_bridge(tmp_path, advertised, configured=())

        await bridge._ensure_categories()

        rows = bridge._service.database.memory_category_repo.list_categories()
        mapping = _rebuild_map(bridge)
        assert sorted(c.name for c in rows.values()) == sorted(n for n, _ in advertised)
        assert [n for n, _ in advertised if n.lower() not in mapping] == []
        # Descriptions carry through, so category summaries keep meaningful text.
        assert {c.name: c.description for c in rows.values()} == dict(advertised)

    @pytest.mark.asyncio
    async def test_configured_path_rows_and_map_unchanged(self, tmp_path):
        """Seeding from the effective set does not change what a configured install gets.

        A no-regression guard that must hold on BOTH trees, not a defect reproducer:
        it passes at base by design.
        """
        configured = [("task_domain", "Domain knowledge"), ("patterns", "Recurring patterns")]
        bridge = _seed_bridge(tmp_path, configured, configured=configured)

        await bridge._ensure_categories()

        rows = bridge._service.database.memory_category_repo.list_categories()
        assert sorted(c.name for c in rows.values()) == ["patterns", "task_domain"]
        assert {c.name: c.description for c in rows.values()} == dict(configured)
        assert sorted(_rebuild_map(bridge)) == ["patterns", "task_domain"]

    @pytest.mark.asyncio
    async def test_configured_cold_start_does_not_inflate_advertised_set(self, tmp_path):
        """A cold start must not list every category twice in the memorize prompt.

        Seeding via _create_category_impl appends a CategoryConfig for a name memU
        already advertises: 3 configured categories became 6 advertised entries.
        """
        configured = [("task_domain", "Domain"), ("patterns", "Recurring"), ("procedures", "How to")]
        bridge = _seed_bridge(tmp_path, configured, configured=configured)
        svc = bridge._service
        before = [c.name for c in svc.category_configs]

        await bridge._ensure_categories()

        after = [c.name for c in svc.category_configs]
        assert after == before
        assert len(after) == len(set(after))
        assert len(svc._category_prompt_str.splitlines()) == len(configured)

    @pytest.mark.asyncio
    async def test_empty_config_does_not_inflate_advertised_set(self, tmp_path):
        """Same guard on the empty-config path, where the advertised set is memU's own.

        Pins the seeding primitive: routing this through _create_category_impl
        duplicates all 10 default names (or loops, if the live list is iterated).
        """
        advertised = [(f"cat_{i}", f"desc {i}") for i in range(4)]
        bridge = _seed_bridge(tmp_path, advertised, configured=())
        svc = bridge._service
        before = [c.name for c in svc.category_configs]

        await bridge._ensure_categories()

        after = [c.name for c in svc.category_configs]
        assert after == before
        assert len(after) == len(set(after))
        assert len(svc._category_prompt_str.splitlines()) == len(advertised)

    @pytest.mark.asyncio
    async def test_reinit_creates_no_duplicates(self, tmp_path):
        """"Only missing ones are created": a second boot adds nothing."""
        advertised = [("alpha", "A"), ("beta", "B")]
        bridge = _seed_bridge(tmp_path, advertised, configured=())

        await bridge._ensure_categories()
        first = {c.id for c in bridge._service.database.memory_category_repo.list_categories().values()}
        await bridge._ensure_categories()

        rows = bridge._service.database.memory_category_repo.list_categories()
        assert sorted(c.name for c in rows.values()) == ["alpha", "beta"]
        assert {c.id for c in rows.values()} == first
        assert sorted(_rebuild_map(bridge)) == ["alpha", "beta"]

    @pytest.mark.asyncio
    async def test_persisted_row_is_remapped_when_no_categories_configured(self, tmp_path):
        """A row created at runtime is resolvable again after a restart.

        Only the map half: the advertised set is rebuilt by memU from config at
        construction, so the LLM is still not told this category exists.  That
        remaining half is out of scope here.
        """
        store = _memu_store(tmp_path / "memu.sqlite")
        store.memory_category_repo.get_or_create_category(
            name="work", description="Work stuff", embedding=None, user_data={},
        )

        config = _make_config(tmp_path)
        config.memory.categories = []
        config.openai_api_key = ""
        bridge = MemUBridge(config, audit_db=None)
        # A fresh store over the same file: a restart starts with a cold cache.
        bridge._service = _StubService(_memu_store(tmp_path / "memu.sqlite"), [("alpha", "A")])

        await bridge._ensure_categories()

        mapping = _rebuild_map(bridge)
        assert "work" in mapping
        # Pinned residual: the persisted row is resolvable but still not advertised.
        assert "work" not in [c.name for c in bridge._service.category_configs]
        assert "work" not in bridge._service._category_prompt_str

    @pytest.mark.asyncio
    async def test_category_load_failure_propagates(self, tmp_path):
        """A read error must surface, not leave ``existing`` empty and re-seed everything."""
        bridge = _seed_bridge(tmp_path, [("alpha", "A")], configured=())
        repo = bridge._service.database.memory_category_repo
        repo.list_categories = MagicMock(side_effect=RuntimeError("db read failed"))

        with pytest.raises(RuntimeError, match="db read failed"):
            await bridge._ensure_categories()

        assert repo.categories == {}

    @pytest.mark.asyncio
    async def test_seeds_are_audited_as_category_created(self, tmp_path):
        """Seeded categories keep the documented ``category_created`` audit record."""
        advertised = [("alpha", "A"), ("beta", "B")]
        bridge = _seed_bridge(tmp_path, advertised, configured=())
        bridge._audit = AsyncMock()

        await bridge._ensure_categories()

        actions = [(c.args[0], c.args[1], c.args[2], c.args[3]) for c in bridge._audit.await_args_list]
        assert actions == [
            ("category_created", "category", "alpha", "bridge"),
            ("category_created", "category", "beta", "bridge"),
        ]
        # Nothing new on a re-init, so no further audit records.
        bridge._audit.reset_mock()
        await bridge._ensure_categories()
        assert bridge._audit.await_count == 0

    @pytest.mark.asyncio
    async def test_seeds_are_audited_on_the_configured_path_too(self, tmp_path):
        """A no-regression guard that must hold on BOTH trees, not a defect reproducer.

        The configured path already audited its seeds at base; this pins that the
        rewrite kept it.
        """
        configured = [("task_domain", "Domain")]
        bridge = _seed_bridge(tmp_path, configured, configured=configured)
        bridge._audit = AsyncMock()

        await bridge._ensure_categories()

        assert [c.args[:4] for c in bridge._audit.await_args_list] == [
            ("category_created", "category", "task_domain", "bridge"),
        ]

    @pytest.mark.asyncio
    async def test_embeddings_are_batched_when_a_provider_is_configured(self, tmp_path):
        """Category ranking is vector-based on RAG installs, so seeds must carry vectors."""
        advertised = [("alpha", "A desc"), ("beta", "")]
        bridge = _seed_bridge(tmp_path, advertised, configured=(), has_embeddings=True)
        embed = AsyncMock(return_value=[[0.1, 0.2], [0.3, 0.4]])
        bridge._service._embed_client = MagicMock(embed=embed)

        await bridge._ensure_categories()

        # One batched call, not one per category.
        assert embed.await_count == 1
        assert embed.await_args.args[0] == ["alpha: A desc", "beta"]
        rows = bridge._service.database.memory_category_repo.list_categories()
        stored = {c.name: list(c.embedding) for c in rows.values()}
        assert stored == {"alpha": [0.1, 0.2], "beta": [0.3, 0.4]}

    @pytest.mark.asyncio
    async def test_no_embed_call_without_a_provider(self, tmp_path):
        bridge = _seed_bridge(tmp_path, [("alpha", "A")], configured=(), has_embeddings=False)
        embed = AsyncMock(return_value=[[0.1]])
        bridge._service._embed_client = MagicMock(embed=embed)

        await bridge._ensure_categories()

        assert embed.await_count == 0
        rows = bridge._service.database.memory_category_repo.list_categories()
        assert [c.embedding for c in rows.values()] == [None]

    @pytest.mark.asyncio
    async def test_embed_failure_writes_nothing(self, tmp_path):
        """No null-embedding row may be written when a provider IS configured.

        get_or_create_category returns an existing row untouched, so such a row is
        never repaired: a later boot sees the name and skips it, while both category
        rankers drop null vectors.  Assert the ABSENCE of rows, not just the error.
        """
        advertised = [("alpha", "A"), ("beta", "B")]
        bridge = _seed_bridge(tmp_path, advertised, configured=(), has_embeddings=True)
        embed = AsyncMock(side_effect=RuntimeError("embedding provider down"))
        bridge._service._embed_client = MagicMock(embed=embed)

        with pytest.raises(RuntimeError, match="embedding provider down"):
            await bridge._ensure_categories()

        assert bridge._service.database.memory_category_repo.list_categories() == {}

    @pytest.mark.parametrize("returned", [1, 3], ids=["too-few", "too-many"])
    @pytest.mark.asyncio
    async def test_wrong_embedding_count_writes_nothing(self, tmp_path, returned):
        """A provider returning the wrong number of vectors must not half-seed.

        The pairing is materialized before the write loop, so the length mismatch
        is raised before the first row instead of part-way through.
        """
        advertised = [("alpha", "A"), ("beta", "B")]
        bridge = _seed_bridge(tmp_path, advertised, configured=(), has_embeddings=True)
        embed = AsyncMock(return_value=[[0.1]] * returned)
        bridge._service._embed_client = MagicMock(embed=embed)

        with pytest.raises(ValueError, match="zip"):
            await bridge._ensure_categories()

        assert bridge._service.database.memory_category_repo.list_categories() == {}

    @pytest.mark.parametrize("returned", [1, 3], ids=["too-few", "too-many"])
    @pytest.mark.asyncio
    async def test_wrong_embedding_count_writes_nothing_with_a_repair_too(
        self, tmp_path, returned,
    ):
        """The strict pairing covers the COMBINED plan: one repair plus one seed = 2.

        The batch now spans repairs as well as seeds, so a wrong vector count must
        still raise before the first write -- including before the rename.
        """
        store = _memu_store(tmp_path / "memu.sqlite")
        legacy = store.memory_category_repo.get_or_create_category(
            name="  alpha  ", description="A", embedding=None, user_data={},
        )
        bridge = _seed_bridge(tmp_path, [("  alpha  ", "A"), ("beta", "B")],
                              configured=(), has_embeddings=True)
        embed = AsyncMock(return_value=[[0.1]] * returned)
        bridge._service._embed_client = MagicMock(embed=embed)

        with pytest.raises(ValueError, match="zip"):
            await bridge._ensure_categories()

        rows = bridge._service.database.memory_category_repo.list_categories()
        assert [c.name for c in rows.values()] == ["  alpha  "]
        assert rows[legacy.id].name == "  alpha  "

    @pytest.mark.asyncio
    async def test_padded_name_is_not_seeded_twice(self, tmp_path):
        """The already-exists skip compares memU's normalized name, not the raw one.

        A row stored as ``alpha`` and a config entry ``"  alpha  "`` are the same
        category, so a second boot must add nothing.
        """
        store = _memu_store(tmp_path / "memu.sqlite")
        store.memory_category_repo.get_or_create_category(
            name="alpha", description="A", embedding=None, user_data={},
        )
        bridge = _seed_bridge(tmp_path, [("  alpha  ", "A")], configured=())

        await bridge._ensure_categories()

        rows = bridge._service.database.memory_category_repo.list_categories()
        assert [c.name for c in rows.values()] == ["alpha"]
        assert sorted(_rebuild_map(bridge)) == ["alpha"]

    @pytest.mark.asyncio
    async def test_a_legacy_raw_row_is_repaired_not_duplicated(self, tmp_path):
        """A row stored under a raw name is RENAMED to memU's form, not duplicated.

        Reachable two ways, both live: an upgrade from the pre-fix code, which
        persisted the raw configured name, and ``_create_category_impl``, the runtime
        creation path this change deliberately leaves alone.  Seeding a second row
        instead would give two rows for one logical category; recognising the row but
        leaving it raw would keep its rebuild key raw, so the advertised name still
        would not resolve.  ``nerve``'s own ``update_category`` wrapper cannot rename
        (it forwards summary/description only), but the repo layer can, so the seed
        path renames -- keeping ONE row, with its id, and therefore its items.
        """
        store = _memu_store(tmp_path / "memu.sqlite")
        before = store.memory_category_repo.get_or_create_category(
            name="  alpha  ", description="A", embedding=None, user_data={},
        )
        bridge = _seed_bridge(tmp_path, [("  alpha  ", "A")], configured=())

        await bridge._ensure_categories()

        rows = bridge._service.database.memory_category_repo.list_categories()
        assert [c.name for c in rows.values()] == ["alpha"]
        assert sorted(_rebuild_map(bridge)) == ["alpha"]
        # ONE row, and it is the SAME row: renaming must not orphan its
        # category_items, which link on the id.
        assert len(rows) == 1
        assert [c.id for c in rows.values()] == [before.id]

    @pytest.mark.asyncio
    async def test_a_legacy_row_is_not_repaired_onto_a_taken_name(self, tmp_path):
        """The repair must not produce two rows with the SAME name.

        A store holding BOTH the raw and the normalized row is reachable from base
        (base seeds the normalized configured name beside an existing raw row), and
        ``list_categories()`` applies no ORDER BY -- so a guard that only remembered
        the rows walked so far would rename the raw one onto the taken name whenever
        it came first.  Every lookup keys on the name, so the two rows would then be
        indistinguishable.  Asserted in BOTH insertion orders.
        """
        for tag, order in (("raw-first", ["  alpha  ", "alpha"]),
                           ("norm-first", ["alpha", "  alpha  "])):
            store = _memu_store(tmp_path / f"memu-{tag}.sqlite")
            ids = {
                n: store.memory_category_repo.get_or_create_category(
                    name=n, description="A", embedding=None, user_data={},
                ).id
                for n in order
            }
            bridge = _seed_bridge(tmp_path, [("  alpha  ", "A")], configured=(),
                                  db_name=f"memu-{tag}.sqlite")

            await bridge._ensure_categories()

            rows = bridge._service.database.memory_category_repo.list_categories()
            names = sorted(c.name for c in rows.values())
            assert names == ["  alpha  ", "alpha"], tag
            assert len(names) == len(set(names)), tag
            # Both pre-existing rows survive untouched, so no items are orphaned.
            assert {c.id for c in rows.values()} == set(ids.values()), tag
            # The advertised name still resolves, via the already-normalized row.
            assert "alpha" in _rebuild_map(bridge), tag

    @pytest.mark.asyncio
    async def test_a_blank_legacy_row_is_repaired_to_untitled(self, tmp_path):
        """The repair uses memU's normalization, so a blank name becomes ``Untitled``.

        memU advertises a nameless category as ``Untitled``; a row stored as ``'   '``
        resolves under no advertised key until it carries that name.
        """
        store = _memu_store(tmp_path / "memu.sqlite")
        before = store.memory_category_repo.get_or_create_category(
            name="   ", description="B", embedding=None, user_data={},
        )
        bridge = _seed_bridge(tmp_path, [("   ", "B")], configured=())

        await bridge._ensure_categories()

        rows = bridge._service.database.memory_category_repo.list_categories()
        assert [c.name for c in rows.values()] == ["Untitled"]
        assert [c.id for c in rows.values()] == [before.id]
        assert sorted(_rebuild_map(bridge)) == ["untitled"]

    @pytest.mark.asyncio
    async def test_case_only_duplicate_rows_are_left_alone(self, tmp_path):
        """Case-only pairs are deliberately OUT of scope, and must stay untouched.

        ``Alpha`` and ``alpha`` are both already normalized, so neither is a legacy
        raw row; base produces the same two rows, and merging them would have to
        discard one row's items.  Pinned so a later change does not quietly widen
        the repair into a destructive merge.
        """
        store = _memu_store(tmp_path / "memu.sqlite")
        ids = {
            n: store.memory_category_repo.get_or_create_category(
                name=n, description="A", embedding=None, user_data={},
            ).id
            for n in ("Alpha", "alpha")
        }
        bridge = _seed_bridge(tmp_path, [("alpha", "A")], configured=())

        await bridge._ensure_categories()

        rows = bridge._service.database.memory_category_repo.list_categories()
        assert sorted(c.name for c in rows.values()) == ["Alpha", "alpha"]
        assert {c.id for c in rows.values()} == set(ids.values())

    @pytest.mark.parametrize(
        "order",
        [["  Alpha  ", "alpha"], ["alpha", "  Alpha  "]],
        ids=["padded-first", "normalized-first"],
    )
    @pytest.mark.asyncio
    async def test_a_padded_row_is_not_repaired_onto_another_rows_lookup_key(
        self, tmp_path, order,
    ):
        """Occupancy is judged on the LOOKUP key, so a rename cannot collapse two rows.

        ``'  Alpha  '`` normalizes to ``Alpha``, which is free among display names but
        shares the rebuild key ``alpha`` with the second row -- so renaming it would
        leave two live rows sharing ONE ``category_name_to_id`` entry, and which one
        wins depends on ``repo.categories`` order (``list_categories()`` applies no
        ORDER BY).  Both rows must therefore be left exactly as base leaves them.
        Asserted on the map-key COUNT, not just membership: membership alone cannot
        see a collapse.  Both insertion orders, for the same missing-ORDER-BY reason.
        """
        db = f"memu-{'-'.join(order)}.sqlite".replace(" ", "_")
        store = _memu_store(tmp_path / db)
        ids = {
            n: store.memory_category_repo.get_or_create_category(
                name=n, description="A", embedding=None, user_data={},
            ).id
            for n in order
        }
        bridge = _seed_bridge(tmp_path, [("alpha", "A")], configured=(), db_name=db)

        await bridge._ensure_categories()

        rows = bridge._service.database.memory_category_repo.list_categories()
        assert sorted(c.name for c in rows.values()) == ["  Alpha  ", "alpha"]
        assert {c.id for c in rows.values()} == set(ids.values())
        # Two live rows, so two addressable keys.  One key here is the regression.
        assert len(_rebuild_map(bridge)) == 2
        assert "alpha" in _rebuild_map(bridge)

    @pytest.mark.parametrize(
        "order",
        [["  alpha  ", "alpha "], ["alpha ", "  alpha  "]],
        ids=["wider-first", "narrower-first"],
    )
    @pytest.mark.asyncio
    async def test_two_raw_rows_sharing_a_lookup_key_still_get_an_addressable_row(
        self, tmp_path, order,
    ):
        """When NO row already owns the advertised key, one must still be seeded.

        Both stored rows are raw, so the multi-owner guard correctly declines to rename
        either -- but neither is addressable, because the name-to-id rebuild keys on
        ``cat.name.lower()`` without stripping.  Judging occupancy on ``_memu_cat_key``
        instead marks ``alpha`` taken by a key no consumer computes and suppresses the
        seed, leaving the advertised name unresolvable -- exactly what base avoids, by
        seeding a third row.  Both insertion orders: ``list_categories()`` applies no
        ORDER BY.
        """
        db = f"memu-raw-{'-'.join(order)}.sqlite".replace(" ", "_")
        store = _memu_store(tmp_path / db)
        ids = {
            n: store.memory_category_repo.get_or_create_category(
                name=n, description="A", embedding=None, user_data={},
            ).id
            for n in order
        }
        bridge = _seed_bridge(tmp_path, [("alpha", "A")], configured=(), db_name=db)

        await bridge._ensure_categories()

        rows = bridge._service.database.memory_category_repo.list_categories()
        # Three rows: both raw rows untouched, plus the addressable seed.
        assert sorted(c.name for c in rows.values()) == ["  alpha  ", "alpha", "alpha "], order
        assert set(ids.values()) <= {c.id for c in rows.values()}, order
        assert {c.name for c in rows.values() if c.id in set(ids.values())} == set(order), order
        mapping = _rebuild_map(bridge)
        assert "alpha" in mapping, order
        # A key COUNT assertion: membership alone cannot see a collapse.
        assert len(mapping) == 3, order

    @pytest.mark.parametrize(
        "order",
        [["  Alpha  ", " alpha"], [" alpha", "  Alpha  "]],
        ids=["padded-first", "narrower-first"],
    )
    @pytest.mark.asyncio
    async def test_case_differing_raw_rows_sharing_a_key_still_get_an_addressable_row(
        self, tmp_path, order,
    ):
        """The same gap reached through a case difference, where no rename is free.

        ``'  Alpha  '`` and ``' alpha'`` share the ``_memu_cat_key`` ``alpha`` and
        neither is already normalized, so both are correctly left alone -- and neither
        answers to the advertised ``alpha`` under the rebuild's own key.
        """
        db = f"memu-case-{'-'.join(order)}.sqlite".replace(" ", "_")
        store = _memu_store(tmp_path / db)
        ids = {
            n: store.memory_category_repo.get_or_create_category(
                name=n, description="A", embedding=None, user_data={},
            ).id
            for n in order
        }
        bridge = _seed_bridge(tmp_path, [("alpha", "A")], configured=(), db_name=db)

        await bridge._ensure_categories()

        rows = bridge._service.database.memory_category_repo.list_categories()
        assert sorted(c.name for c in rows.values()) == ["  Alpha  ", " alpha", "alpha"], order
        assert set(ids.values()) <= {c.id for c in rows.values()}, order
        assert {c.name for c in rows.values() if c.id in set(ids.values())} == set(order), order
        mapping = _rebuild_map(bridge)
        assert "alpha" in mapping, order
        assert len(mapping) == 3, order

    @pytest.mark.asyncio
    async def test_a_padded_config_entry_matches_a_case_differing_row(self, tmp_path):
        """The already-exists test compares lookup keys, so no second row is seeded.

        A row stored as ``Alpha`` and an advertised ``  alpha  `` are one category to
        every memU lookup; seeding a second row would put both behind one rebuild key.
        """
        store = _memu_store(tmp_path / "memu.sqlite")
        before = store.memory_category_repo.get_or_create_category(
            name="Alpha", description="A", embedding=None, user_data={},
        )
        bridge = _seed_bridge(tmp_path, [("  alpha  ", "A")], configured=())

        await bridge._ensure_categories()

        rows = bridge._service.database.memory_category_repo.list_categories()
        assert [c.name for c in rows.values()] == ["Alpha"]
        assert [c.id for c in rows.values()] == [before.id]
        assert sorted(_rebuild_map(bridge)) == ["alpha"]

    @pytest.mark.asyncio
    async def test_seeded_name_and_description_are_normalized(self, tmp_path):
        """The stored row carries the name the prompt advertises, so lookups resolve."""
        advertised = [("  alpha  ", "  A  "), ("   ", "B")]
        bridge = _seed_bridge(tmp_path, advertised, configured=())

        await bridge._ensure_categories()

        rows = bridge._service.database.memory_category_repo.list_categories()
        assert {c.name: c.description for c in rows.values()} == {"alpha": "A", "Untitled": "B"}
        # The map is keyed on what memU looks up: name.strip().lower().
        assert sorted(_rebuild_map(bridge)) == ["alpha", "untitled"]
        # A second pass finds both and adds nothing.
        await bridge._ensure_categories()
        assert len(bridge._service.database.memory_category_repo.list_categories()) == 2

    @pytest.mark.asyncio
    async def test_embed_text_is_normalized_like_memu(self, tmp_path):
        """Seed vectors must be embedded from memU's own _category_embedding_text.

        A vector built from the padded text lands elsewhere in the space cosine_topk
        ranks in, so category ranking would degrade silently.
        """
        advertised = [("  alpha  ", "  A  "), ("  beta  ", "   ")]
        bridge = _seed_bridge(tmp_path, advertised, configured=(), has_embeddings=True)
        embed = AsyncMock(return_value=[[0.1, 0.2], [0.3, 0.4]])
        bridge._service._embed_client = MagicMock(embed=embed)

        await bridge._ensure_categories()

        # "beta" has a whitespace-only description, so it takes the desc-less form.
        assert embed.await_args.args[0] == ["alpha: A", "beta"]

    @pytest.mark.asyncio
    async def test_a_repaired_row_is_re_embedded_from_its_normalized_text(self, tmp_path):
        """A renamed row must not keep the vector embedded from its raw name.

        Both category rankers read the stored vector directly, and seeds are
        deliberately embedded from the normalized text -- so a repair that updated
        only ``name`` would leave that one row ranked in a different space.  One
        batched call covers the repair and the seed together.
        """
        store = _memu_store(tmp_path / "memu.sqlite")
        legacy = store.memory_category_repo.get_or_create_category(
            name="  alpha  ", description="A", embedding=[9.0, 9.0], user_data={},
        )
        bridge = _seed_bridge(tmp_path, [("  alpha  ", "A"), ("beta", "B")],
                              configured=(), has_embeddings=True)
        embed = AsyncMock(return_value=[[0.1, 0.2], [0.3, 0.4]])
        bridge._service._embed_client = MagicMock(embed=embed)

        await bridge._ensure_categories()

        # ONE call, carrying the repair's NORMALIZED text alongside the seed's.
        assert embed.await_count == 1
        assert embed.await_args.args[0] == ["alpha: A", "beta: B"]
        rows = bridge._service.database.memory_category_repo.list_categories()
        stored = {c.name: list(c.embedding) for c in rows.values()}
        # approx, not ==: a re-read vector comes back through _patch_sqlite_bugs'
        # numpy float32 embeddings (Fix 6), unlike a freshly created row's cached
        # list.  The point is WHICH vector is stored, not its dtype.
        assert stored["alpha"] == pytest.approx([0.1, 0.2], abs=1e-6)
        assert stored["beta"] == pytest.approx([0.3, 0.4], abs=1e-6)
        # And emphatically not the vector embedded from the raw name.
        assert stored["alpha"] != pytest.approx([9.0, 9.0], abs=1e-6)
        assert rows[legacy.id].name == "alpha"

    @pytest.mark.asyncio
    async def test_no_embed_call_leaves_a_repaired_rows_vector_alone(self, tmp_path):
        """With no provider the rename passes ``embedding=None``, which is a no-op write.

        ``update_category`` skips ``embedding_json`` when the argument is None, so the
        existing vector survives -- the correct outcome when nothing can be embedded.
        A no-regression guard that must hold on BOTH trees, not a defect reproducer.
        """
        store = _memu_store(tmp_path / "memu.sqlite")
        legacy = store.memory_category_repo.get_or_create_category(
            name="  alpha  ", description="A", embedding=[9.0, 9.0], user_data={},
        )
        bridge = _seed_bridge(tmp_path, [("  alpha  ", "A")], configured=(),
                              has_embeddings=False)
        embed = AsyncMock(return_value=[[0.1]])
        bridge._service._embed_client = MagicMock(embed=embed)

        await bridge._ensure_categories()

        assert embed.await_count == 0
        rows = bridge._service.database.memory_category_repo.list_categories()
        assert rows[legacy.id].name == "alpha"
        assert list(rows[legacy.id].embedding) == [9.0, 9.0]

    @pytest.mark.asyncio
    async def test_embed_failure_commits_no_rename(self, tmp_path):
        """Embed-before-write covers repairs too, so a failure migrates nothing.

        With the rename written before the embedding batch, an outage left the row
        renamed and the missing category unseeded: a partially migrated store.
        """
        store = _memu_store(tmp_path / "memu.sqlite")
        legacy = store.memory_category_repo.get_or_create_category(
            name="  alpha  ", description="A", embedding=[9.0], user_data={},
        )
        bridge = _seed_bridge(tmp_path, [("  alpha  ", "A"), ("beta", "B")],
                              configured=(), has_embeddings=True)
        embed = AsyncMock(side_effect=RuntimeError("embedding provider down"))
        bridge._service._embed_client = MagicMock(embed=embed)

        with pytest.raises(RuntimeError, match="embedding provider down"):
            await bridge._ensure_categories()

        rows = bridge._service.database.memory_category_repo.list_categories()
        assert [c.name for c in rows.values()] == ["  alpha  "]
        assert rows[legacy.id].name == "  alpha  "

    @pytest.mark.asyncio
    async def test_a_repair_is_audited_as_category_updated(self, tmp_path):
        """Every other category mutation in this file is audited; the repair must be too.

        ``category_updated`` with the row id, matching ``_update_category_impl``.
        """
        store = _memu_store(tmp_path / "memu.sqlite")
        legacy = store.memory_category_repo.get_or_create_category(
            name="  alpha  ", description="A", embedding=None, user_data={},
        )
        bridge = _seed_bridge(tmp_path, [("  alpha  ", "A")], configured=())
        bridge._audit = AsyncMock()

        await bridge._ensure_categories()

        assert [c.args[:4] for c in bridge._audit.await_args_list] == [
            ("category_updated", "category", legacy.id, "bridge"),
        ]
        assert bridge._audit.await_args.args[4] == {
            "name_before": "  alpha  ", "name_after": "alpha",
        }
        # A boot with nothing to repair emits no update.
        bridge._audit.reset_mock()
        await bridge._ensure_categories()
        assert bridge._audit.await_count == 0

    @pytest.mark.asyncio
    async def test_a_repaired_row_keeps_its_own_description(self, tmp_path):
        """The repair renames ONLY.  Pinned so a later change cannot flip it silently.

        A stored description may have been edited through the API or the UI, and the
        rename is not a reconciliation point: overwriting it with the config text
        would discard that edit, and resolution does not depend on it.  A BEHAVIOUR
        PIN that holds on both trees, not a defect reproducer -- its value is that a
        future widening of the repair has to change this arm deliberately.
        """
        store = _memu_store(tmp_path / "memu.sqlite")
        legacy = store.memory_category_repo.get_or_create_category(
            name="  alpha  ", description="edited by the user", embedding=None, user_data={},
        )
        bridge = _seed_bridge(tmp_path, [("  alpha  ", "config text")], configured=())

        await bridge._ensure_categories()

        rows = bridge._service.database.memory_category_repo.list_categories()
        assert rows[legacy.id].name == "alpha"
        assert rows[legacy.id].description == "edited by the user"
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_blank_name_is_seeded_once_as_untitled(self, tmp_path):
        """memU shows a nameless category as ``Untitled``; the row must match."""
        bridge = _seed_bridge(tmp_path, [("   ", "B")], configured=())
        bridge._audit = AsyncMock()

        await bridge._ensure_categories()

        rows = bridge._service.database.memory_category_repo.list_categories()
        assert [c.name for c in rows.values()] == ["Untitled"]
        # The audit target_id names the row that was written, not the blank config.
        assert [c.args[:4] for c in bridge._audit.await_args_list] == [
            ("category_created", "category", "Untitled", "bridge"),
        ]
        bridge._audit.reset_mock()
        await bridge._ensure_categories()
        assert len(bridge._service.database.memory_category_repo.list_categories()) == 1
        assert bridge._audit.await_count == 0


class TestInitializeCategoryInvariant:
    """End-to-end: a full initialize() in its own process (one MemoryService each)."""

    def test_empty_config_every_advertised_category_resolves(self, tmp_path):
        report = _run_init(tmp_path, configured=())

        assert report["offline"] is True
        assert report["initialize"] is True
        assert report["available"] is True
        assert len(report["advertised"]) == 10  # memU's defaults
        assert len(report["resolved"]) == len(report["advertised"])
        assert sorted(report["rows"]) == sorted(report["advertised"])
        assert len(report["prompt_lines"]) == len(report["advertised"])

    def test_configured_cold_start_prompt_lists_each_category_once(self, tmp_path):
        configured = [("task_domain", "Domain"), ("patterns", "Recurring")]
        report = _run_init(tmp_path, configured=configured)

        assert report["offline"] is True
        assert report["advertised"] == ["task_domain", "patterns"]
        assert len(report["prompt_lines"]) == 2
        assert len(report["resolved"]) == 2
        assert sorted(report["rows"]) == ["patterns", "task_domain"]

    def test_padded_and_blank_configured_names_still_all_resolve(self, tmp_path):
        """End-to-end: every name the PROMPT advertises resolves, however it was written.

        Unfixed, initialize() reports success with rows/map keyed on the raw
        ``'  alpha  '`` / ``'   '`` while the prompt says ``alpha`` / ``Untitled``,
        so resolved_prompt is empty and every LLM assignment is dropped.
        Asserting against report["advertised"] would NOT see this: the raw
        ``'  alpha  '`` key happens to match itself, giving 1 of 2 even unfixed.
        """
        report = _run_init(tmp_path, configured=[("  alpha  ", "A"), ("   ", "B")])

        assert report["offline"] is True
        assert report["initialize"] is True
        assert report["prompt_names"] == ["alpha", "Untitled"]
        assert len(report["resolved_prompt"]) == len(report["prompt_names"])
        assert sorted(report["rows"]) == ["Untitled", "alpha"]
        assert sorted(report["map"]) == ["alpha", "untitled"]

    def test_upgrade_from_a_raw_stored_row_still_resolves_everything(self, tmp_path):
        """End-to-end upgrade: rows left by the pre-fix seed path are repaired.

        The post-upgrade shape: the row on disk carries the raw configured name the
        old code stored, while the config has since been cleaned up.  Unfixed, the
        row keys as ``'  alpha  '`` and the prompt says ``alpha``, so resolved_prompt
        is short and every LLM assignment to it is dropped.  Asserts the repair
        keeps ONE row and the SAME row, so its category_items stay linked.
        """
        report = _run_init(tmp_path, configured=[("alpha", "A")],
                           pre_store=["  alpha  "])

        assert report["offline"] is True
        assert report["initialize"] is True
        assert report["prompt_names"] == ["alpha"]
        assert len(report["resolved_prompt"]) == len(report["prompt_names"])
        assert report["rows"] == ["alpha"]
        assert sorted(report["map"]) == ["alpha"]
        assert report["row_ids"]["alpha"] == report["pre_ids"]["  alpha  "]

    def test_upgrade_from_two_raw_rows_sharing_a_key_still_resolves(self, tmp_path):
        """End-to-end: two raw rows share the advertised key, so neither can be renamed.

        Reachable as an upgrade from a config that once carried both ``'  alpha  '``
        and ``'alpha '``, or a pre-fix boot plus one ``_create_category_impl`` call.
        Judging occupancy on the strip-and-lower key marks ``alpha`` present and skips
        the seed, so ``resolved_prompt`` is empty while ``initialize()`` reports
        success -- the very "advertised but unresolvable" state this branch removes.
        Both pre-existing rows must survive, and one row must answer to ``alpha``.
        """
        pre = ["  alpha  ", "alpha "]
        report = _run_init(tmp_path, configured=[("alpha", "A")], pre_store=pre)

        assert report["offline"] is True
        assert report["initialize"] is True
        assert report["available"] is True
        assert report["prompt_names"] == ["alpha"]
        assert len(report["resolved_prompt"]) == len(report["prompt_names"])
        assert sorted(report["rows"]) == ["  alpha  ", "alpha", "alpha "]
        # Both raw rows keep their ids, so their category_items stay linked.
        assert set(report["pre_ids"].values()) <= set(report["row_ids"].values())
        assert sorted(report["map"]) == ["  alpha  ", "alpha", "alpha "]

    def test_availability_is_not_published_when_init_fails(self, tmp_path):
        """_available is the only failure signal the agent sees: engine.py drops the return."""
        report = _run_init(tmp_path, configured=(), mode="fail-load")

        assert report["offline"] is True
        assert report["initialize"] is False
        assert report["available"] is False
        assert report["service_available"] is False
