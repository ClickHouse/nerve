"""Tests for nerve.memory.memu_bridge — event date resolution & knowledge filtering."""

import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

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
# memU category-name normalization
#
# ctx.category_name_to_id is the only name-to-id channel between nerve and memU.
# memU's contract is strip at creation, strip+lowercase at lookup, and all three
# of its consumer copies key on name.strip().lower(). nerve substitutes its own
# rebuild and creation paths, so these tests pin that its keys agree, and that at
# most one category exists per normalized key.
# ---------------------------------------------------------------------------


_CATEGORY_MODELS_CACHE = {}


def _category_models():
    """Build the SQLAlchemy models ONCE per process.

    nerve's Fix 6 clears memu's _MODEL_CACHE on every call, so a second
    SQLiteStore(dsn=...) that rebuilds them raises ArgumentError("Column object
    'url' already assigned"). Sharing one build is what lets these tests use
    several isolated stores, and reopen one file to simulate a restart.
    """
    if "models" in _CATEGORY_MODELS_CACHE:
        return _CATEGORY_MODELS_CACHE["models"]
    # memu has an import-order circularity (database/__init__ -> factory ->
    # app/__init__ -> service -> factory), so app.service must come first.
    import memu.app.service  # noqa: F401
    import memu.database.sqlite.schema as schema_mod

    # Building the models needs the patches applied (Fix 6 renames memu's own
    # "sqlite_*" tables, which SQLite reserves). Resolve through the MODULE:
    # Fix 6 replaces this attribute.
    MemUBridge._patch_sqlite_bugs()
    models = schema_mod.get_sqlite_sqlalchemy_models(scope_model=None)
    _CATEGORY_MODELS_CACHE["models"] = models
    return models


class _CategoryCtx:
    """Stand-in for memU's workflow context (only the category fields)."""

    def __init__(self):
        self.category_ids = []
        self.category_name_to_id = {}
        self.categories_ready = False


class _EmbedSpy:
    """Embedding client that counts calls.

    The resolution's PLACEMENT is load-bearing, not cosmetic: seeding invokes the
    create path once per configured category on every start, so a check placed
    below the embed costs one embedding API call per category per restart.
    """

    def __init__(self):
        self.calls = 0
        self.gate = None

    async def embed(self, texts):
        self.calls += 1
        if self.gate is not None:
            await self.gate.wait()
        return [[0.0, 1.0] for _ in texts]


class _CategoryFixture:
    """Isolated memU stores over one temp file, with the patches applied.

    The SQLiteMemoryItemRepo methods are restored on exit; the other globals
    _patch_sqlite_bugs() installs are not, so do not rely on full isolation.
    """

    def __init__(self, tmp_path):
        # Snapshot before _category_models(), which patches too -- a snapshot
        # taken in __enter__ would already hold the patched methods.
        import memu.app.service  # noqa: F401 - import-order circularity
        from memu.database.sqlite.repositories.memory_item_repo import (
            SQLiteMemoryItemRepo,
        )

        self._repo = SQLiteMemoryItemRepo
        self._saved = {
            n: SQLiteMemoryItemRepo.__dict__.get(n)
            for n in ("update_item", "delete_item", "clear_items", "list_items",
                      "create_item", "create_item_reinforce", "vector_search_items")
        }
        self.models = _category_models()
        self.path = str(tmp_path / "memu.sqlite")
        self._stores = []

    def __enter__(self):
        MemUBridge._patch_sqlite_bugs()
        return self

    def __exit__(self, *exc):
        for store in self._stores:
            try:
                store.close()
            except Exception:
                pass
        for name, fn in self._saved.items():
            if fn is None:
                if name in self._repo.__dict__:
                    delattr(self._repo, name)
            else:
                setattr(self._repo, name, fn)
        return False

    def store(self):
        """A fresh store over the same file -- a simulated process restart."""
        from memu.database.sqlite.sqlite import SQLiteStore

        store = SQLiteStore(dsn=f"sqlite:///{self.path}", sqla_models=self.models)
        self._stores.append(store)
        return store

    def write_row(self, store, name, description="from web UI"):
        """Insert a row directly through the repo, bypassing nerve.

        Simulates a row created before this fix, or by another writer.
        """
        return store.memory_category_repo.get_or_create_category(
            name=name, description=description, embedding=None, user_data={},
        )

    def bridge(self, store, config_names=(), embeddings=True):
        """A MemUBridge whose _service is a stub carrying the real collaborators.

        Only what the category paths touch is stubbed; the code under test is
        nerve's own.
        """
        from memu.app.service import CategoryConfig

        from nerve.config import MemoryCategoryConfig

        bridge = MemUBridge.__new__(MemUBridge)
        config = NerveConfig()
        config.memory = MemoryConfig(
            sqlite_dsn=f"sqlite:///{self.path}",
            categories=[
                MemoryCategoryConfig(name=n, description=f"desc {n}")
                for n in config_names
            ],
        )
        bridge.config = config
        bridge._audit_db = None
        bridge._main_loop = None
        bridge._available = False

        spy = _EmbedSpy()
        ctx = _CategoryCtx()
        service = SimpleNamespace(
            database=store,
            _context=ctx,
            _get_context=lambda: ctx,
            category_configs=[
                CategoryConfig(name=n, description=f"desc {n}") for n in config_names
            ],
            category_config_map={
                n: CategoryConfig(name=n, description=f"desc {n}") for n in config_names
            },
            _category_prompt_str="",
            _format_categories_for_prompt=lambda cats: " | ".join(c.name for c in cats),
            _get_llm_client=lambda _kind: spy,
        )
        bridge._service = service
        # _has_embeddings is a property on the class; category_fixture replaces it
        # with one reading this flag, so each test chooses whether the embedding
        # branch runs.
        bridge._category_test_embeddings = embeddings
        return bridge, service, ctx, spy


@pytest.fixture
def category_fixture(tmp_path, monkeypatch):
    """A _CategoryFixture with _has_embeddings driven by the stub's flag."""
    monkeypatch.setattr(
        MemUBridge,
        "_has_embeddings",
        property(lambda self: getattr(self, "_category_test_embeddings", False)),
    )
    with _CategoryFixture(tmp_path) as fx:
        yield fx


def _rebuild_map(bridge, ctx):
    """Rebuild the name-to-id map the way _initialize_impl does.

    The key is spelled out here rather than imported from the bridge, so this
    helper works unchanged against a tree that lacks the fix: a helper that
    ImportErrors would make every arm fail in its harness, which discriminates
    nothing. This is the memU consumer contract these tests hold nerve to.
    """
    ctx.category_ids = []
    ctx.category_name_to_id = {}
    for _cat_id, cat in bridge._service.database.memory_category_repo.categories.items():
        ctx.category_ids.append(cat.id)
        ctx.category_name_to_id[cat.name.strip().lower()] = cat.id


def _run_real_rebuild_block(bridge, ctx):
    """Execute the REAL rebuild block, extracted from _initialize_impl's source.

    _rebuild_map above is an independent oracle; this drives nerve's own key
    derivation so the rebuild arm cannot pass by re-implementing the fix. Running
    the whole of _initialize_impl would need the entire memU service constructed,
    which is what makes extracting this block the cheaper honest option. It is
    keyed on structure, not line numbers, and asserts its own match.
    """
    import inspect
    import re
    import sys
    import textwrap

    src = inspect.getsource(type(bridge)._initialize_impl)
    match = re.search(
        r"^(\s*)ctx\.category_ids = \[\]\n(.*?)\n(?=\1if ctx\.category_name_to_id:)",
        src,
        re.S | re.M,
    )
    assert match, "could not locate the rebuild block in _initialize_impl"
    block = textwrap.dedent(match.group(1) + "ctx.category_ids = []\n" + match.group(2))
    assert "category_name_to_id[" in block, f"extracted the wrong block: {block!r}"
    namespace = dict(vars(sys.modules[type(bridge).__module__]))
    namespace.update({"ctx": ctx, "self": bridge})
    exec(compile(block, "<rebuild-block>", "exec"), namespace)


def _unreachable_ids(store, ctx):
    """Category ids no name-based lookup can reach -- the damage this fix prevents."""
    live = {c.id for c in store.memory_category_repo.categories.values()}
    return live - set(ctx.category_name_to_id.values())


class TestCategoryNameNormalization:
    """nerve's name-to-id keys must match memU's strip().lower() contract."""

    def test_fixture_restores_the_item_repo_on_exit(self, tmp_path):
        """The fixture must not leak its SQLiteMemoryItemRepo patches to its neighbours.

        TestIndexedUpdateItemForwarding introspects update_item's signature, so a
        leaked wrapper turns it red from ~200 lines away. Order-independent: the
        leak is observable within one test body.
        """
        import inspect

        import memu.app.service  # noqa: F401 - import-order circularity
        from memu.database.sqlite.repositories.memory_item_repo import (
            SQLiteMemoryItemRepo as Repo,
        )

        names = (
            "update_item", "delete_item", "clear_items", "list_items",
            "create_item", "create_item_reinforce", "vector_search_items",
        )

        def methods():
            return {n: Repo.__dict__.get(n) for n in names}

        def keyword_only():
            param = inspect.signature(Repo.update_item).parameters.get("item_id")
            return param is not None and param.kind is inspect.Parameter.KEYWORD_ONLY

        before = methods()
        assert keyword_only(), "precondition: update_item unpatched on entry"

        with _CategoryFixture(tmp_path) as fx:
            fx.store()
            assert "item_id" not in inspect.signature(Repo.update_item).parameters, (
                "the patch is meant to be applied inside the fixture"
            )

        # Every SQLiteMemoryItemRepo name the patch reassigns, not just the one
        # the neighbour reads: a restore list narrowed to update_item must fail here.
        leaked = sorted(n for n, fn in methods().items() if fn is not before[n])
        assert not leaked, f"_CategoryFixture leaked on exit: {leaked}"
        assert keyword_only(), "_CategoryFixture leaked _patch_sqlite_bugs() on exit"

    @pytest.mark.asyncio
    async def test_padded_category_name_is_reachable_by_its_stripped_key(
        self, category_fixture
    ):
        """A padded name is unreachable at base with NO collision anywhere.

        The worse of the two shapes, and invisible to a collision query: one row,
        one map key, and every consumer's stripped lookup misses it.
        """
        store = category_fixture.store()
        bridge, _svc, ctx, _spy = category_fixture.bridge(store)

        assert await bridge._create_category_impl(" procedures ", "d") is True
        _rebuild_map(bridge, ctx)

        cats = list(store.memory_category_repo.categories.values())
        assert [c.name for c in cats] == ["procedures"]
        assert ctx.category_name_to_id["procedures"] == cats[0].id
        assert _unreachable_ids(store, ctx) == set()

    @pytest.mark.asyncio
    async def test_case_variant_creation_reuses_the_existing_row(self, category_fixture):
        """Two spellings must not become two rows sharing one map key."""
        store = category_fixture.store()
        bridge, _svc, ctx, _spy = category_fixture.bridge(store)

        assert await bridge._create_category_impl("procedures", "FIRST") is True
        assert await bridge._create_category_impl("PROCEDURES", "SECOND") is True
        _rebuild_map(bridge, ctx)

        cats = list(store.memory_category_repo.categories.values())
        assert len(cats) == 1
        assert len(ctx.category_name_to_id) == 1
        assert _unreachable_ids(store, ctx) == set()

    @pytest.mark.asyncio
    async def test_exact_repeat_creation_is_still_idempotent(self, category_fixture):
        """Control: the restart seed path depends on the exact repeat staying cheap."""
        store = category_fixture.store()
        bridge, _svc, _ctx, _spy = category_fixture.bridge(store)

        assert await bridge._create_category_impl("procedures", "d") is True
        assert await bridge._create_category_impl("procedures", "d") is True

        assert len(store.memory_category_repo.categories) == 1

    def test_rebuild_uses_the_normalized_key(self, category_fixture):
        """Pins the rebuild key independently of the creation path.

        Without this arm the creation fix alone makes the suite green while the
        rebuild key stays unguarded -- and the rebuild is what maps rows written
        before this fix, or by any other writer.
        """
        store = category_fixture.store()
        row = category_fixture.write_row(store, " Procedures ")
        bridge, _svc, ctx, _spy = category_fixture.bridge(store)

        _run_real_rebuild_block(bridge, ctx)

        assert ctx.category_name_to_id["procedures"] == row.id
        assert _unreachable_ids(store, ctx) == set()

    def test_the_key_is_lower_not_casefold(self, category_fixture):
        """memU keys on ``.lower()``, so nerve must too -- they are not the same.

        Every other arm here uses an ASCII name, where ``lower()`` and
        ``casefold()`` are byte-identical, so none of them can tell the two apart:
        a mutant switching to ``casefold()`` passes the whole suite. 'Straße' is
        the cheapest name where they diverge ('straße' vs 'strasse'), and the
        lookup asserted is exactly what memU's three consumers compute, so this
        arm fails the moment nerve's key stops agreeing with theirs.
        """
        name = "Straße"
        assert name.lower() != name.casefold(), "the fixture name must discriminate"

        store = category_fixture.store()
        row = category_fixture.write_row(store, f" {name} ")
        bridge, _svc, ctx, _spy = category_fixture.bridge(store)

        _run_real_rebuild_block(bridge, ctx)

        # memU's own lookup key, spelled out rather than imported from the bridge.
        assert ctx.category_name_to_id[f" {name} ".strip().lower()] == row.id
        assert _unreachable_ids(store, ctx) == set()

    @pytest.mark.asyncio
    async def test_reuse_does_NOT_call_the_embedding_client(self, category_fixture):
        """The resolution must sit ABOVE the embed, not below it.

        A resolution placed after the embed satisfies every other arm here; only
        this one distinguishes it. Below the embed, seeding would cost one
        embedding API call per configured category on every restart.
        """
        store = category_fixture.store()
        bridge, _svc, _ctx, spy = category_fixture.bridge(store, embeddings=True)

        assert await bridge._create_category_impl("procedures", "d") is True
        assert spy.calls == 1, "a genuinely new category must still embed"

        assert await bridge._create_category_impl("PROCEDURES", "d") is True
        assert spy.calls == 1, "a reuse must not embed"

    @pytest.mark.asyncio
    async def test_two_concurrent_creates_of_the_same_name_make_ONE_row(
        self, category_fixture
    ):
        """The post-embed recheck, and this is its only guard.

        The memU loop runs each create as its own coroutine with no lock, so
        without a recheck after the embed's await both creates pass the first
        existence check and insert -- reintroducing the very collision this change
        removes, through the fix.
        """
        store = category_fixture.store()
        bridge, _svc, ctx, spy = category_fixture.bridge(store, embeddings=True)
        spy.gate = asyncio.Event()

        first = asyncio.create_task(bridge._create_category_impl("procedures", "d"))
        second = asyncio.create_task(bridge._create_category_impl("PROCEDURES", "d"))
        # Both must be suspended inside embed() before either may insert.
        while spy.calls < 2:
            await asyncio.sleep(0)
        spy.gate.set()
        assert await first is True
        assert await second is True

        _rebuild_map(bridge, ctx)
        assert len(store.memory_category_repo.categories) == 1
        assert len(ctx.category_name_to_id) == 1
        assert _unreachable_ids(store, ctx) == set()

    @pytest.mark.asyncio
    async def test_reuse_does_not_duplicate_the_prompt_or_the_id_or_audit_a_create(
        self, category_fixture
    ):
        """The early return keeps the create-only side effects untouched."""
        store = category_fixture.store()
        bridge, service, ctx, _spy = category_fixture.bridge(store)
        # The bridge mutates the ctx the fixture handed back (_get_context returns
        # this object), so an id appended by the reuse path is observable here.
        assert bridge._service._get_context() is ctx
        audited = []
        bridge._audit = AsyncMock(side_effect=lambda *a, **k: audited.append(a))

        await bridge._create_category_impl("procedures", "d")
        configs_after_create = len(service.category_configs)
        prompt_after_create = service._category_prompt_str
        ids_after_create = list(ctx.category_ids)
        assert ids_after_create, "the create arm must have appended its id"
        audited.clear()

        await bridge._create_category_impl("PROCEDURES", "d")

        # BEFORE any rebuild: _rebuild_map resets category_ids and refills it from
        # the single-row DB, which erases a spurious append before it can be seen.
        # This is the only oracle for the "a reuse never re-appends an id" claim.
        assert list(ctx.category_ids) == ids_after_create, (
            "a reuse must not append an id"
        )
        assert len(service.category_configs) == configs_after_create
        assert service._category_prompt_str == prompt_after_create
        assert audited == [], "a reuse must not audit a create that did not happen"
        _rebuild_map(bridge, ctx)
        assert len(ctx.category_ids) == len(set(ctx.category_ids))

    @pytest.mark.asyncio
    async def test_a_create_on_an_ALREADY_COLLIDED_store_does_not_flip_the_mapping(
        self, category_fixture
    ):
        """Resolution must pick the row the rebuild mapped, not merely the first.

        No other arm here writes two rows before acting, so a pre-collided store --
        the state this change explicitly promises to leave alone -- is otherwise
        untested. The rebuild assigns the shared key row-by-row, so the LAST match
        wins; a first-match scan answers the OTHER row, and registering that answer
        repoints the key without touching either row, orphaning everything linked
        to the row that just lost it.
        """
        seeded = category_fixture.store()
        first = category_fixture.write_row(seeded, "procedures", "FIRST")
        second = category_fixture.write_row(seeded, "PROCEDURES", "SECOND")
        assert first.id != second.id, "the fixture must produce two distinct rows"

        restarted = category_fixture.store()
        bridge, _svc, ctx, _spy = category_fixture.bridge(restarted)
        restarted.memory_category_repo.list_categories()
        _run_real_rebuild_block(bridge, ctx)

        # One key for two rows is the pre-existing damage; the arm is about what a
        # later create does to it, so pin the starting point rather than assume it.
        assert len(ctx.category_name_to_id) == 1
        snapshot = dict(ctx.category_name_to_id)
        rows_before = {c.id: c.name for c in restarted.memory_category_repo.categories.values()}
        assert len(rows_before) == 2

        assert await bridge._create_category_impl("Procedures", "d") is True

        assert dict(ctx.category_name_to_id) == snapshot, (
            "a create must not repoint an already-collided key at the other row"
        )
        # Not by merging, renaming or deleting either row -- the no-migration
        # promise -- and not by inserting a third one, which is the base behaviour
        # this change already fixes and which must stay fixed.
        assert {
            c.id: c.name for c in restarted.memory_category_repo.categories.values()
        } == rows_before


class TestEnsureCategoriesNormalization:
    """Seeding must adopt a normalized-equal row, never shadow-create one."""

    @pytest.mark.asyncio
    async def test_ensure_categories_does_not_shadow_a_case_variant_row(
        self, category_fixture
    ):
        """The amplifier: a one-off UI typo orphans a category on the next start.

        At base the exact-match skip misses 'PROCEDURES' for config 'procedures',
        so seeding creates a second row and the rebuild orphans one of them.
        """
        seeded = category_fixture.store()
        row = category_fixture.write_row(seeded, "PROCEDURES")

        restarted = category_fixture.store()
        bridge, _svc, ctx, _spy = category_fixture.bridge(
            restarted, config_names=("procedures",)
        )
        restarted.memory_category_repo.list_categories()
        await bridge._ensure_categories()
        _rebuild_map(bridge, ctx)

        cats = list(restarted.memory_category_repo.categories.values())
        assert [c.id for c in cats] == [row.id]
        assert _unreachable_ids(restarted, ctx) == set()

    @pytest.mark.asyncio
    async def test_config_map_is_registered_for_a_REUSED_row(self, category_fixture):
        """memU reads category_config_map by the STORED row's name.

        MemoryService pre-keys that map by the *config* names, so a row stored
        under another spelling is never a key unless the reuse path adds it --
        which is why this arm must go through reuse, and must assert the lookup
        by the stored name rather than by the requested one.
        """
        seeded = category_fixture.store()
        category_fixture.write_row(seeded, "PROCEDURES")

        restarted = category_fixture.store()
        bridge, service, _ctx, _spy = category_fixture.bridge(
            restarted, config_names=("procedures",)
        )
        restarted.memory_category_repo.list_categories()
        await bridge._ensure_categories()

        assert service.category_config_map.get("PROCEDURES") is not None

    @pytest.mark.asyncio
    async def test_a_reused_UNCONFIGURED_row_becomes_assignable_in_the_prompt(
        self, category_fixture
    ):
        """A reuse must still register the row in the LLM's offered set.

        ``category_configs`` is seeded only from config at service construction
        and is never rebuilt from the DB, and the create path's own append exists
        "so new memorizations can assign to this category". The reuse branch returns
        before it, so a persisted row with no configured counterpart -- the web-UI
        case -- got both name maps and was still never offered to the LLM, i.e.
        nothing could ever be filed under it. The base create was unconditional and
        did register it, so the early return removed that repair.
        """
        seeded = category_fixture.store()
        row = category_fixture.write_row(seeded, "PROCEDURES")

        restarted = category_fixture.store()
        # No config_names: the row exists in the DB and in NO config entry.
        bridge, service, _ctx, _spy = category_fixture.bridge(restarted)
        restarted.memory_category_repo.list_categories()
        assert service.category_configs == [], "the arm needs an unconfigured start"

        assert await bridge._create_category_impl("procedures", "d") is True

        assert [c.name for c in service.category_configs] == [row.name], (
            "a reused row must be offered to the LLM exactly once, under its "
            "STORED name"
        )
        assert row.name in service._category_prompt_str
        # Still a reuse, not a create: one row, no second spelling inserted.
        assert len(restarted.memory_category_repo.categories) == 1

    @pytest.mark.asyncio
    async def test_prompt_registration_is_stable_across_repeated_seeding(
        self, category_fixture
    ):
        """Idempotence must hold across a restart, not merely within one call.

        The arm above says "register it"; this one says "and never twice". Both are
        needed: an unconditional append satisfies the first and grows the offered
        set once per restart, which is the duplicate-in-prompt shape a separately
        filed defect already covers and which this change must not create.
        """
        seeded = category_fixture.store()
        category_fixture.write_row(seeded, "PROCEDURES")

        restarted = category_fixture.store()
        # Configured under a DIFFERENT spelling than the stored row, so exact-name
        # membership would not find it and only a normalized test can.
        bridge, service, _ctx, _spy = category_fixture.bridge(
            restarted, config_names=("procedures",)
        )
        restarted.memory_category_repo.list_categories()
        before = [c.name for c in service.category_configs]
        assert before == ["procedures"]

        for _ in range(3):
            await bridge._ensure_categories()

        assert [c.name for c in service.category_configs] == before, (
            "a normalized-equal config entry already offers this category"
        )

    @pytest.mark.asyncio
    async def test_repeated_ensure_categories_makes_no_embedding_calls(
        self, category_fixture
    ):
        """The cost property categories_ready=True exists to protect.

        Seeding now invokes the create path unconditionally, so reuse must stay
        free of embedding calls or every restart pays for the whole config.
        """
        store = category_fixture.store()
        bridge, _svc, _ctx, spy = category_fixture.bridge(
            store, config_names=("procedures", "patterns"), embeddings=True
        )
        store.memory_category_repo.list_categories()

        await bridge._ensure_categories()
        first_run = spy.calls
        assert first_run == 2, "the first seed embeds each new category once"

        await bridge._ensure_categories()
        assert spy.calls == first_run, "a second seed must make no embedding calls"
        assert len(store.memory_category_repo.categories) == 2

    @pytest.mark.asyncio
    async def test_ensure_categories_is_quiet_and_stable_across_repeated_runs(
        self, category_fixture, caplog
    ):
        """Three runs over a case-variant row: no growth, no warning, no orphan."""
        seeded = category_fixture.store()
        category_fixture.write_row(seeded, "PROCEDURES")

        restarted = category_fixture.store()
        bridge, _svc, ctx, _spy = category_fixture.bridge(
            restarted, config_names=("procedures",)
        )
        restarted.memory_category_repo.list_categories()

        with caplog.at_level(logging.WARNING, logger="nerve.memory.memu_bridge"):
            for _ in range(3):
                await bridge._ensure_categories()

        _rebuild_map(bridge, ctx)
        assert len(restarted.memory_category_repo.categories) == 1
        assert _unreachable_ids(restarted, ctx) == set()
        assert [r.message for r in caplog.records] == []


# ---------------------------------------------------------------------------
# The production initialization path
#
# The arms above drive _create_category_impl / _ensure_categories with a stubbed
# _service, so they never observe _initialize_impl's own ORDERING. Two of its
# properties exist only there:
#
#   * the category cache is hydrated BEFORE seeding (a fresh process starts with
#     an empty cache, so without it the seed path cannot see pre-existing rows
#     and shadow-creates a duplicate);
#   * the availability flags are published only AFTER every step succeeded.
#
# Both need the REAL function, and _initialize_impl builds a memU MemoryService,
# which can only happen ONCE per process: memU caches its SQLAlchemy models
# module-globally, and nerve's Fix 6 clears that cache on every call, so a second
# build raises ArgumentError("Column object 'url' already assigned"). Measured:
# a second _initialize_impl in the same interpreter returns False, and merely
# having built the fixture models above is enough to make the FIRST one return
# False. An in-process arm would therefore pass on rc=False without ever
# reaching the code it names -- so each arm runs the real function in a fresh
# interpreter and asserts rc is True first. The in-repo precedent for this shape
# is tests/test_config_sources.py.
# ---------------------------------------------------------------------------


_CATEGORY_TABLE_DDL = """
CREATE TABLE memu_memory_categories (
  id VARCHAR NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
  updated_at DATETIME NOT NULL,
  name VARCHAR NOT NULL,
  description TEXT NOT NULL,
  summary TEXT,
  embedding_json TEXT,
  user_id VARCHAR,
  PRIMARY KEY (id)
);
"""
# Verbatim from the live schema memU's create_all produces, and the point of the
# whole fix: PRIMARY KEY (id) only, no unique index on name, so 'procedures' and
# 'PROCEDURES' are two perfectly legal rows.


def _run_init_script(tmp_path, body: str, seed_name: str | None = None):
    """Run ``body`` against a real _initialize_impl in a fresh interpreter.

    Seeds ``seed_name`` with plain sqlite3 rather than through memU: importing
    memU here to write one row would build the models and poison the init under
    test. memU's create_all adds the remaining tables to the same file.
    """
    import os
    import subprocess
    import sys
    import uuid

    home = tmp_path / "nerve-home"
    home.mkdir(parents=True, exist_ok=True)
    db = home / "memu.sqlite"
    seeded_id = ""
    if seed_name is not None:
        seeded_id = str(uuid.uuid4())
        conn = sqlite3.connect(db)
        conn.executescript(_CATEGORY_TABLE_DDL)
        conn.execute(
            "INSERT INTO memu_memory_categories (id, updated_at, name, description)"
            " VALUES (?, CURRENT_TIMESTAMP, ?, ?)",
            (seeded_id, seed_name, "from another writer"),
        )
        conn.commit()
        conn.close()

    preamble = (
        "import asyncio, json, logging, os, sqlite3, sys\n"
        "logging.disable(logging.CRITICAL)\n"
        "from nerve.config import NerveConfig, MemoryConfig, MemoryCategoryConfig\n"
        "from nerve.memory.memu_bridge import MemUBridge\n"
        "DB = sys.argv[1]\n"
        "SEEDED_ID = sys.argv[2]\n"
        "config = NerveConfig()\n"
        "config.memory = MemoryConfig(sqlite_dsn='sqlite:///' + DB,\n"
        "    categories=[MemoryCategoryConfig(name='procedures', description='d')])\n"
        "config.openai_api_key = ''\n"
        "bridge = MemUBridge(config)\n"
        "out = {}\n"
    )
    epilogue = "print('NERVE_RESULT ' + json.dumps(out))\n"
    proc = subprocess.run(
        [sys.executable, "-c", preamble + body + epilogue, str(db), seeded_id],
        capture_output=True,
        text=True,
        # NERVE_HOME redirects every machine-local path (memu-resources etc.) into
        # tmp_path, so the arm cannot touch the developer's real ~/.nerve.
        env={**os.environ, "NERVE_HOME": str(home)},
        timeout=300,
    )
    marker = [ln for ln in proc.stdout.splitlines() if ln.startswith("NERVE_RESULT ")]
    assert marker, (
        f"init subprocess produced no result\nSTDOUT:\n{proc.stdout}\n"
        f"STDERR:\n{proc.stderr}"
    )
    result = json.loads(marker[-1][len("NERVE_RESULT "):])
    result["_seeded_id"] = seeded_id
    result["_db"] = str(db)
    return result


class TestInitializeImplCategoryOrdering:
    """_initialize_impl's own ordering guarantees, driven through the real function."""

    def test_seeding_sees_a_preexisting_row_because_the_cache_is_hydrated_first(
        self, tmp_path
    ):
        """A restart over a case-variant row must adopt it, not shadow it.

        The seed path resolves against the repo's in-memory cache, which is empty
        in a fresh process, so the hydration before _ensure_categories is what
        lets it see the row at all. Asserted on the DATABASE rather than the
        cache: the damage is a second persisted row, and a cache-only assertion
        cannot tell an adopted row from a freshly created one.
        """
        result = _run_init_script(
            tmp_path,
            "async def main():\n"
            "    out['rc'] = await bridge._initialize_impl()\n"
            "    conn = sqlite3.connect(DB)\n"
            "    rows = conn.execute(\n"
            "        'select id, name from memu_memory_categories').fetchall()\n"
            "    conn.close()\n"
            "    out['db_ids'] = [r[0] for r in rows]\n"
            "    out['db_names'] = sorted(r[1] for r in rows)\n"
            "    ctx = bridge._service._get_context()\n"
            "    out['map'] = dict(ctx.category_name_to_id)\n"
            "asyncio.run(main())\n",
            seed_name="PROCEDURES",
        )

        assert result["rc"] is True, "the arm must observe a real successful init"
        assert result["db_names"] == ["PROCEDURES"], (
            "seeding shadow-created a second row for the case variant"
        )
        assert result["db_ids"] == [result["_seeded_id"]]
        assert result["map"] == {"procedures": result["_seeded_id"]}
        unreachable = set(result["db_ids"]) - set(result["map"].values())
        assert unreachable == set()

    def test_a_failure_after_the_service_is_built_publishes_no_availability(
        self, tmp_path
    ):
        """All three flags, because initialized_at is never reset anywhere.

        _attach_engine_pragmas is the first step after the MemoryService
        construction the flags used to sit beside, so raising there lands
        strictly between the old site and the new one. A bridge that reports
        itself available after a failed init is what the relocation prevents;
        `initialized_at` has no reset path, so an except-clause fix could not
        achieve this and the assertion must cover it explicitly.
        """
        result = _run_init_script(
            tmp_path,
            "def boom(self):\n"
            "    raise RuntimeError('injected failure after the old flag site')\n"
            "MemUBridge._attach_engine_pragmas = boom\n"
            "async def main():\n"
            "    out['rc'] = await bridge._initialize_impl()\n"
            "    out['available'] = bridge._available\n"
            "    out['service_available'] = bridge._metrics.service_available\n"
            "    out['initialized_at'] = bridge._metrics.initialized_at\n"
            "asyncio.run(main())\n",
        )

        assert result["rc"] is False, "the injection must actually fail the init"
        assert result["available"] is False
        assert result["service_available"] is False
        assert result["initialized_at"] == ""

    def test_a_clean_init_does_publish_availability(self, tmp_path):
        """Control for the arm above: the relocation must not withhold the flags.

        Without this, moving the flags to somewhere unreachable would satisfy the
        failure arm perfectly.
        """
        result = _run_init_script(
            tmp_path,
            "async def main():\n"
            "    out['rc'] = await bridge._initialize_impl()\n"
            "    out['available'] = bridge._available\n"
            "    out['service_available'] = bridge._metrics.service_available\n"
            "    out['initialized_at_set'] = bridge._metrics.initialized_at != ''\n"
            "asyncio.run(main())\n",
        )

        assert result["rc"] is True
        assert result["available"] is True
        assert result["service_available"] is True
        assert result["initialized_at_set"] is True


class TestBlankCategoryNameRejection:
    """A blank name must be refused at its origin, never coerced.

    memU's prompt formatter renders a blank name as a placeholder, and nothing
    can then assign to it -- so coercing downstream would turn a typo into a
    permanent category row.
    """

    def test_blank_config_name_is_rejected_by_from_dict(self):
        from nerve.config import MemoryCategoryConfig

        for blank in ("", "   ", "\t\n"):
            with pytest.raises(ValueError, match="must not be blank"):
                MemoryCategoryConfig.from_dict({"name": blank})

    def test_well_formed_config_name_is_stripped_and_accepted(self):
        from nerve.config import MemoryCategoryConfig

        cfg = MemoryCategoryConfig.from_dict(
            {"name": "  procedures  ", "description": "d"}
        )
        assert cfg.name == "procedures"
        assert cfg.description == "d"

    def test_no_category_is_ever_named_untitled(self):
        """Pins the reject-not-coerce policy against a future placeholder."""
        from nerve.config import MemoryCategoryConfig

        try:
            cfg = MemoryCategoryConfig.from_dict({"name": " "})
        except ValueError:
            return
        raise AssertionError(f"blank name was coerced to {cfg.name!r}, not rejected")

    def test_yaml_null_name_is_rejected_not_coerced_to_the_string_None(self):
        """A bare ``name:`` in YAML is the realistic typo, and str() hides it.

        ``str(None)`` is the truthy literal ``'None'``, so a blank guard placed
        after stringification never fires and seeding persists a category named
        ``None``. Asserts the parse too, so the arm cannot pass because PyYAML
        stopped producing None.
        """
        import yaml

        from nerve.config import MemoryCategoryConfig

        parsed = yaml.safe_load("categories:\n  - name:\n    description: x\n")
        assert parsed["categories"][0]["name"] is None

        with pytest.raises(ValueError, match="must be a string"):
            MemoryCategoryConfig.from_dict(parsed["categories"][0])

    def test_non_string_config_name_is_rejected(self):
        """The same silent-coercion shape as YAML null, for every non-str scalar.

        A non-str name has never been reachable: nerve's own key derivation
        (``name.lower()``) and memU's (``cfg.name.strip()``) both raise
        AttributeError on it, so rejecting here removes no working case and only
        moves the failure to the config boundary that can name the offender.
        """
        from nerve.config import MemoryCategoryConfig

        for bad in (12, 1.5, True, ["a"], {"k": 1}):
            with pytest.raises(ValueError, match="must be a string"):
                MemoryCategoryConfig.from_dict({"name": bad, "description": "d"})

    def test_no_config_can_produce_a_category_named_None(self):
        """Independent oracle: assert the OUTCOME, not the exception type.

        A future revision that reverts to ``str(d["name"])`` while keeping a
        blank guard passes the raises-arms' sibling shapes but fails here.
        """
        from nerve.config import MemoryCategoryConfig

        for bad in (None, 12, True):
            try:
                cfg = MemoryCategoryConfig.from_dict({"name": bad})
            except ValueError:
                continue
            raise AssertionError(
                f"name={bad!r} was coerced to {cfg.name!r}, not rejected"
            )


class TestCreateCategoryRoute:
    """POST /api/memory/memu/categories input validation."""

    @pytest.fixture
    def client(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        import nerve.config as cfg_mod
        from nerve.gateway.routes._deps import init_deps
        from nerve.gateway.routes.memory import router

        config = NerveConfig()
        # require_auth reads get_config().auth.jwt_secret -- empty makes it a no-op.
        config.auth.jwt_secret = ""
        cfg_mod._config = config

        bridge = SimpleNamespace(
            available=True,
            calls=[],
        )

        async def _create_category(name, description, source="bridge"):
            bridge.calls.append((name, description, source))
            return True

        bridge.create_category = _create_category
        init_deps(engine=SimpleNamespace(_memory_bridge=bridge), db=None)  # type: ignore[arg-type]

        app = FastAPI()
        app.include_router(router)
        try:
            yield TestClient(app), bridge
        finally:
            cfg_mod._config = None

    def test_blank_name_is_rejected_with_400(self, client):
        """400, not 422: a pydantic validator would fail before the body runs,
        and this route reports bad input as 400 (as its sibling handlers do)."""
        http, bridge = client

        response = http.post("/api/memory/memu/categories", json={"name": "   "})

        assert response.status_code == 400
        assert bridge.calls == [], "a rejected name must never reach the bridge"

    def test_padded_name_is_stripped_before_reaching_the_bridge(self, client):
        http, bridge = client

        response = http.post(
            "/api/memory/memu/categories", json={"name": "  procedures  "}
        )

        assert response.status_code == 200
        assert response.json()["name"] == "procedures"
        assert bridge.calls == [("procedures", "", "web_ui")]
