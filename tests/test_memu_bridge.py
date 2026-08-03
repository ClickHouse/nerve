"""Tests for nerve.memory.memu_bridge — event date resolution & knowledge filtering."""

import asyncio
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import openai
import pytest
import pytest_asyncio

from nerve.agent.tools.handlers.memory import memory_recall_handler
from nerve.agent.tools.registry import ToolContext
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


def _make_recall_bridge(tmp_path: Path) -> MemUBridge:
    """Bridge wired for recall() tests: available, mocked service, no memU loop
    (so _submit awaits inline)."""
    bridge = MemUBridge(_make_config(tmp_path))
    bridge._available = True
    bridge._service = MagicMock()
    return bridge


def _recall_ctx(bridge) -> ToolContext:
    return ToolContext(
        session_id="s-recall",
        workspace=Path("/tmp/ws"),
        db=None,
        memory_bridge=bridge,
        config=None,
    )


_REQ = httpx.Request("POST", "http://embeddings.invalid/v1/embeddings")


def _conn_error(cause: BaseException | None = None) -> openai.APIConnectionError:
    """The exception the OpenAI SDK raises when the call never got a response."""
    exc = openai.APIConnectionError(request=_REQ)
    if cause is not None:
        exc.__cause__ = cause
    return exc


class TestRecallTransportFailureClassification:
    """recall() must never report a FAILURE as "no results".

    A transport-level LLM failure carries no HTTP status, so
    _is_transient_llm_error cannot match it by construction; before this class
    existed such a failure returned [] and the tool layer rendered it as
    "Recalled 0 memories" -- a confident wrong answer. An empty list from
    recall() now means retrieval genuinely returned nothing.
    """

    # --- infrastructure failures => MemoryBackendUnavailable ---------------

    @pytest.mark.asyncio
    async def test_t1_connection_error_raises_backend_unavailable(self, tmp_path):
        bridge = _make_recall_bridge(tmp_path)
        bridge._service.retrieve = AsyncMock(side_effect=_conn_error())

        with pytest.raises(MemoryBackendUnavailable, match="unavailable"):
            await bridge.recall("q")

    @pytest.mark.asyncio
    async def test_t2_closed_transport_raises_backend_unavailable(self, tmp_path):
        """The reset window: _reset_llm_clients_impl closed the transport under
        an in-flight call."""
        bridge = _make_recall_bridge(tmp_path)
        bridge._service.retrieve = AsyncMock(side_effect=_conn_error(
            RuntimeError("Cannot send a request, as the client has been closed."),
        ))

        with pytest.raises(MemoryBackendUnavailable, match="unavailable"):
            await bridge.recall("q")

    @pytest.mark.asyncio
    async def test_t3_refused_connection_raises_backend_unavailable(self, tmp_path):
        """The shape the SDK actually produces for a refused connection -- the
        27+ production occurrences. A BARE httpx.ConnectError is deliberately
        NOT classified (no carrier: nerve sets client_backend="sdk"), so this
        arm uses the wrapped form the SDK really raises."""
        bridge = _make_recall_bridge(tmp_path)
        bridge._service.retrieve = AsyncMock(side_effect=_conn_error(
            httpx.ConnectError("[Errno 111] Connection refused"),
        ))

        with pytest.raises(MemoryBackendUnavailable, match="unavailable"):
            await bridge.recall("q")

    @pytest.mark.asyncio
    async def test_t7_anthropic_connection_error_raises_backend_unavailable(self, tmp_path):
        """The Bedrock family. anthropic.APIConnectionError is NOT a subclass of
        openai's, so a single-SDK predicate would miss nerve's own
        _BedrockLLMClient -- this arm is what pins two-family coverage."""
        anthropic = pytest.importorskip("anthropic")
        assert not issubclass(
            anthropic.APIConnectionError, openai.APIConnectionError,
        ), "families are expected to be disjoint; the predicate needs both arms"

        bridge = _make_recall_bridge(tmp_path)
        bridge._service.retrieve = AsyncMock(
            side_effect=anthropic.APIConnectionError(request=_REQ),
        )

        with pytest.raises(MemoryBackendUnavailable, match="unavailable"):
            await bridge.recall("q")

    @pytest.mark.asyncio
    async def test_t10_plain_asyncio_timeout_raises_backend_unavailable(self, tmp_path):
        """asyncio.wait_for expiry is builtins.TimeoutError, matched by none of
        the SDK predicates. _instrument_llm_timeouts wraps .chat() in
        asyncio.wait_for for the "fast" profile, which is the profile recall's
        LLM ranker uses, so this is reachable on recall's own path."""
        assert asyncio.TimeoutError is TimeoutError

        bridge = _make_recall_bridge(tmp_path)
        bridge._service.retrieve = AsyncMock(side_effect=TimeoutError("timed out"))

        with pytest.raises(MemoryBackendUnavailable, match="unavailable"):
            await bridge.recall("q")

    @pytest.mark.asyncio
    async def test_t11_api_timeout_error_raises_backend_unavailable(self, tmp_path):
        """APITimeoutError is a SUBCLASS of APIConnectionError, which is why the
        arm does not need to call _is_llm_timeout. If a future SDK reparents it,
        this arm fails loudly instead of silently losing coverage."""
        assert issubclass(openai.APITimeoutError, openai.APIConnectionError)

        bridge = _make_recall_bridge(tmp_path)
        bridge._service.retrieve = AsyncMock(
            side_effect=openai.APITimeoutError(request=_REQ),
        )

        with pytest.raises(MemoryBackendUnavailable, match="unavailable"):
            await bridge.recall("q")

    # --- everything else propagates unchanged -----------------------------

    @pytest.mark.asyncio
    async def test_t4_logic_error_re_raises_as_itself(self, tmp_path):
        """A logic error is neither disguised as a backend outage nor as an
        empty result: it propagates to the caller's generic error path.

        NOTE this deliberately INVERTS the memorize_file control shape
        (test_non_transient_error_still_fails_fast asserts ok is False):
        memorize_file returns a bool, recall returns results, and [] is a
        meaningful value there."""
        bridge = _make_recall_bridge(tmp_path)
        bridge._service.retrieve = AsyncMock(side_effect=ValueError("bad payload"))

        with pytest.raises(ValueError, match="bad payload"):
            await bridge.recall("q")

    @pytest.mark.asyncio
    async def test_t14_genuine_miss_still_returns_empty(self, tmp_path):
        """The arm that keeps the inversion honest: [] must still be reachable
        for a successful retrieval with no hits, else every miss would look
        like an outage."""
        bridge = _make_recall_bridge(tmp_path)
        bridge._service.retrieve = AsyncMock(
            return_value={"items": [], "categories": []},
        )

        assert await bridge.recall("q") == []
        assert bridge._service.retrieve.await_count == 1, (
            "retrieval was skipped: [] would not prove a genuine miss"
        )
        assert bridge._service.retrieve.await_args.kwargs["queries"] == [
            {"role": "user", "content": "q"},
        ]

    # --- the uninitialized-bridge guard -----------------------------------

    @pytest.mark.asyncio
    async def test_t12_uninitialized_bridge_raises_backend_unavailable(self, tmp_path):
        """The same lie by a different route: a bridge that never initialized
        used to answer [] with no exception involved at all."""
        bridge = MemUBridge(_make_config(tmp_path))
        bridge._available = False

        with pytest.raises(MemoryBackendUnavailable, match="not initialized"):
            await bridge.recall("q")

    # --- the rendered artifact (what the agent actually reads) -------------

    @pytest.mark.asyncio
    async def test_t6_handler_renders_unavailable_not_a_count(self, tmp_path):
        bridge = _make_recall_bridge(tmp_path)
        bridge._service.retrieve = AsyncMock(side_effect=_conn_error())

        result = await memory_recall_handler(_recall_ctx(bridge), {"query": "q"})
        text = result.content[0]["text"]

        assert "MEMORY RECALL UNAVAILABLE" in text
        assert "Recalled 0" not in text
        assert "No relevant memories found" not in text

    @pytest.mark.asyncio
    async def test_t8_handler_message_claims_no_false_cause(self, tmp_path):
        """The message must describe the OUTCOME only. Every phrase below was
        false for some case the same arm covers: "transient" (a misconfigured
        endpoint is permanent), "proxy/auth error" and "BACKEND DOWN" (a
        self-closed local transport leaves the remote blameless), "did not
        respond" / "never reached" (429/5xx ARE responses, and a read failure
        can happen mid-response)."""
        bridge = _make_recall_bridge(tmp_path)
        bridge._service.retrieve = AsyncMock(side_effect=_conn_error())

        result = await memory_recall_handler(_recall_ctx(bridge), {"query": "q"})
        text = result.content[0]["text"]

        for false_claim in (
            "transient proxy/auth error",
            "BACKEND DOWN",
            "did not respond",
            "never reached",
            "memory is down",
        ):
            assert false_claim not in text, f"false cause claim in message: {false_claim}"
        assert "MEMORY RECALL UNAVAILABLE" in text
        assert "NOT an empty result" in text

    @pytest.mark.asyncio
    async def test_t9_rate_limit_still_renders_unavailable(self, tmp_path):
        """The PRE-EXISTING 429 arm. Pins that rewording the message did not
        break the case it already had to cover, and that a 429 -- which IS a
        response -- is not mis-described by the new text."""
        bridge = _make_recall_bridge(tmp_path)
        bridge._service.retrieve = AsyncMock(side_effect=openai.RateLimitError(
            "Error code: 429 - rate limit exceeded",
            response=httpx.Response(429, request=_REQ),
            body=None,
        ))

        result = await memory_recall_handler(_recall_ctx(bridge), {"query": "q"})
        text = result.content[0]["text"]

        assert "MEMORY RECALL UNAVAILABLE" in text
        assert "Recalled 0" not in text

    @pytest.mark.asyncio
    async def test_t13_uninitialized_bridge_renders_unavailable(self, tmp_path):
        """End-to-end artifact for T12: the guard's raise must reach the agent
        as an unavailability notice, not as "No relevant memories found."."""
        bridge = MemUBridge(_make_config(tmp_path))
        bridge._available = False
        # `available` is a read-only property; the handler gates only on the bridge
        # being present (handlers/memory.py:67), so the guard is what fires here.

        result = await memory_recall_handler(_recall_ctx(bridge), {"query": "q"})
        text = result.content[0]["text"]

        assert "MEMORY RECALL UNAVAILABLE" in text
        assert "No relevant memories found" not in text

    @pytest.mark.asyncio
    async def test_t15_session_context_surfaces_the_error(self, tmp_path):
        """The second production caller. It has its own broad `except`
        (handlers/memory.py:253), so a raising recall must render the error text
        rather than the pre-existing "(none -- fresh topic or empty memU)"."""
        from nerve.agent.tools.handlers.memory import session_context_handler

        bridge = _make_recall_bridge(tmp_path)
        bridge._service.retrieve = AsyncMock(side_effect=_conn_error())
        # session_context gates on `.available`, which is a read-only property
        # backed by _available (already True via _make_recall_bridge).
        ctx = _recall_ctx(bridge)

        result = await session_context_handler(ctx, {"topic": "t", "include_skills": False})
        text = result.content[0]["text"]

        assert "recall error" in text
        assert "fresh topic or empty memU" not in text

    # --- the caller that must not crash -----------------------------------

    @pytest.mark.asyncio
    async def test_t5_pre_recall_caller_does_not_freeze_a_failure(self, tmp_path, db):
        """Drives the REAL engine caller (`_get_or_create_client`), not a copy.

        `meta_updates` is set only on the success path, so a failed recall is NOT
        frozen into session metadata -- `engine.py:1140` replays a frozen value on
        every rebuild of the session. Session creation must also survive."""
        from nerve.agent.backends.base import BackendCapabilities
        from nerve.agent.engine import AgentEngine

        cfg = _make_config(tmp_path)
        cfg.workspace = tmp_path / "ws"
        engine = AgentEngine(cfg, db)
        await db.create_session("s-prerecall", source="web")

        bridge = _make_recall_bridge(tmp_path)
        bridge._service.retrieve = AsyncMock(side_effect=_conn_error())
        engine._memory_bridge = bridge

        class _StubClient:
            resume_dropped = False
            native_session_id = None
            model = "claude-opus-5"

            def is_alive(self):
                return True

            async def disconnect(self):
                pass

        class _StubBackend:
            name = "claude"
            capabilities = BackendCapabilities(
                cost_is_cumulative=False, supports_idle_stream=False,
                supports_cache_ttl=False, interactive_builtins=False,
                reports_context_window=True,
            )

            def default_model(self, source):
                return "claude-opus-5"

            def excluded_tools(self):
                return set()

            def validate_resume_target(self, native_id, cwd):
                return True

            async def create_client(self, spec):
                return _StubClient()

        engine._backends["claude"] = _StubBackend()

        client = await engine._get_or_create_client("s-prerecall", "web", None)
        assert client is not None, "session creation must not break"

        assert bridge._service.retrieve.await_count == 1, (
            "pre-recall never ran: the test would pass even if the branch were skipped"
        )
        session = await db.get_session("s-prerecall")
        meta = json.loads(session.get("metadata") or "{}")
        assert "recalled_memories" not in meta, f"failed recall was FROZEN: {meta}"
