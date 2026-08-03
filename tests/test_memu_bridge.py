"""Tests for nerve.memory.memu_bridge — event date resolution & knowledge filtering."""

import asyncio
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
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
# Embedding-call boundedness (_instrument_embedding_timeout)
# ---------------------------------------------------------------------------


class _HangingEmbedClient:
    """Base-client stand-in whose .embed() never completes.

    Awaits an Event that is never set, so it hangs without touching a real
    endpoint and without a sleep the test has to outwait.
    """

    def __init__(self):
        self.client = MagicMock()          # stands in for AsyncOpenAI
        self.client.timeout = "untouched"
        self.client.max_retries = 7        # sentinel: must survive (T7)
        self.gate = asyncio.Event()
        self.embed_calls = 0

    async def embed(self, inputs, *args, **kwargs):
        self.embed_calls += 1
        await self.gate.wait()             # never set
        return ([[0.0]], None)             # pragma: no cover - unreachable

    async def chat(self, prompt, **kwargs):  # pragma: no cover - not exercised
        return "ok"


class _FastEmbedClient(_HangingEmbedClient):
    """Same shape, but .embed() returns memU's real 2-tuple immediately."""

    async def embed(self, inputs, *args, **kwargs):
        self.embed_calls += 1
        return ([[0.1, 0.2]], {"raw": 1})


# Wall-clock ceiling for the hang arms. Well above the 0.25s bound they
# assert, so a passing arm never waits on it.
_ARM_CEILING = 5.0


async def _bounded(awaitable):
    """Await ``awaitable``, converting a HANG into a distinguishable failure.

    Without this an un-bounded embed (e.g. a mutant that deletes the wait_for)
    would hang the arm forever instead of failing it. The escape raises
    ``AssertionError``, never ``TimeoutError``, so it can never be mistaken
    for the bound firing - i.e. it cannot make T1/T2 pass vacuously.
    """
    task = asyncio.ensure_future(awaitable)
    done, _ = await asyncio.wait({task}, timeout=_ARM_CEILING)
    if not done:
        task.cancel()
        raise AssertionError(
            f"embed did not complete within {_ARM_CEILING}s - the call is NOT bounded",
        )
    return task.result()


def _make_embed_bridge(tmp_path, client, *, has_embeddings=True):
    """Bridge with a mocked service handing out ``client`` for "embedding"."""
    config = _make_config(tmp_path)
    if has_embeddings:
        config.openai_api_key = "test-embed-key"
    bridge = MemUBridge(config)
    bridge._available = True
    bridge._service = MagicMock()
    bridge._service._get_llm_base_client = MagicMock(return_value=client)
    bridge._LLM_CALL_TIMEOUT = 0.25       # keep the arms fast
    return bridge


class TestEmbeddingCallTimeout:
    """The "embedding" LLM profile must be bounded like every other call."""

    @pytest.mark.asyncio
    async def test_t1_hanging_embed_is_bounded(self, tmp_path):
        """T1: a hung embed raises TimeoutError instead of stalling."""
        client = _HangingEmbedClient()
        bridge = _make_embed_bridge(tmp_path, client)

        bridge._instrument_embedding_timeout()

        t0 = asyncio.get_running_loop().time()
        with pytest.raises(asyncio.TimeoutError):
            await _bounded(client.embed(["query"]))
        elapsed = asyncio.get_running_loop().time() - t0
        assert elapsed < _ARM_CEILING, f"took {elapsed:.2f}s - not bounded"
        assert client.embed_calls == 1

    @pytest.mark.asyncio
    async def test_t2_bound_fires_through_the_real_memu_wrapper(self, tmp_path):
        """T2: the bound survives memU's LLMClientWrapper delegation.

        This pins the load-bearing mechanism: the wrapper resolves
        ``self._client.embed`` at CALL time, so patching the base client's
        instance attribute is observed even by a wrapper built earlier.
        """
        from memu.llm.wrapper import LLMClientWrapper, LLMInterceptorRegistry

        client = _HangingEmbedClient()
        bridge = _make_embed_bridge(tmp_path, client)
        # built BEFORE patching - the delegation is resolved at call time
        wrapper = LLMClientWrapper(client, registry=LLMInterceptorRegistry())

        bridge._instrument_embedding_timeout()

        with pytest.raises(asyncio.TimeoutError):
            await _bounded(wrapper.embed(["query"]))
        assert client.embed_calls == 1

    @pytest.mark.asyncio
    async def test_t3_return_shape_is_preserved(self, tmp_path):
        """T3: the 2-tuple embed contract is untouched by the wrapper."""
        from memu.llm.wrapper import LLMClientWrapper, LLMInterceptorRegistry

        client = _FastEmbedClient()
        bridge = _make_embed_bridge(tmp_path, client)
        bridge._instrument_embedding_timeout()

        result = await client.embed(["q"])
        assert isinstance(result, tuple) and len(result) == 2
        vecs, raw = result
        assert vecs[0] == [0.1, 0.2]
        assert raw is not None

        # ...and through the wrapper, which unpacks to the vectors.
        wrapper = LLMClientWrapper(client, registry=LLMInterceptorRegistry())
        via_wrapper = await wrapper.embed(["q"])
        assert via_wrapper[0] == [0.1, 0.2]

    @pytest.mark.asyncio
    async def test_t4_instrumenting_twice_does_not_double_wrap(self, tmp_path):
        """T4: the sentinel makes repeat instrumentation idempotent."""
        client = _FastEmbedClient()
        bridge = _make_embed_bridge(tmp_path, client)

        bridge._instrument_embedding_timeout()
        wrapped_once = client.embed
        bridge._instrument_embedding_timeout()

        assert client.embed is wrapped_once, "embed was wrapped twice"
        await client.embed(["q"])
        assert client.embed_calls == 1, "original reached more than once per call"

    @pytest.mark.asyncio
    async def test_t5_no_provider_install_is_still_bounded(self, tmp_path):
        """T5: a no-provider install must be bounded, not skipped.

        memU synthesizes "embedding" from "default" whenever the caller omits
        it (memu/app/settings.py:286), so the profile exists even with no
        openai_api_key and its embed calls are reachable through memU's update
        workflow. Gating on ``_has_embeddings`` would leave them unbounded.
        """
        client = _FastEmbedClient()
        bridge = _make_embed_bridge(tmp_path, client, has_embeddings=False)

        assert bridge._has_embeddings is False, "fixture is not a no-provider install"

        bridge._instrument_embedding_timeout()

        bridge._service._get_llm_base_client.assert_called_once_with("embedding")
        assert getattr(client.embed, "_nerve_timeout_wrapped", False), \
            "a no-provider install was left with an unbounded embed"
        assert client.client.timeout != "untouched", "Layer 1 timeout was not set"

    @pytest.mark.asyncio
    async def test_t6_instrument_llm_timeouts_covers_the_embedding_profile(self, tmp_path):
        """T6: regression arm for the enumeration that silently rots.

        Asserted behaviourally (the method IS invoked), not by grepping the
        ("memorize","fast","default") tuple.
        """
        client = _FastEmbedClient()
        bridge = _make_embed_bridge(tmp_path, client)
        bridge._service._get_llm_base_client = MagicMock(side_effect=KeyError("chat"))

        with patch.object(bridge, "_instrument_embedding_timeout") as spy:
            bridge._instrument_llm_timeouts()

        spy.assert_called_once()

    @pytest.mark.asyncio
    async def test_t7_sdk_max_retries_is_left_alone(self, tmp_path):
        """T7: Layer 1 sets timeout but must NOT copy chat's max_retries=0.

        Embeddings have no compensating retry ladder, so dropping SDK retries
        would make a transient 429 fail immediately.
        """
        client = _HangingEmbedClient()
        bridge = _make_embed_bridge(tmp_path, client)
        before = client.client.max_retries

        bridge._instrument_embedding_timeout()

        assert client.client.max_retries == before == 7
        assert client.client.timeout != "untouched", "Layer 1 timeout was not set"

    def test_t8_startup_call_precedes_availability_and_seeding(self):
        """T8: ordering + availability-window guard, by source order.

        Cannot run ``_initialize_impl`` (it builds a real MemoryService, and
        only one may exist per process), so assert on its source text.
        """
        import inspect

        src = inspect.getsource(MemUBridge._initialize_impl)
        i_bound = src.index("self._instrument_embedding_timeout()")
        i_avail = src.index("self._available = True")
        i_seed = src.index("await self._ensure_categories()")

        assert i_bound < i_avail, "availability is published before the bound is installed"
        assert i_avail < i_seed, "unexpected ordering: seeding moved above availability"
        assert i_bound < i_seed, "category seeding embeds before the bound is installed"

    @pytest.mark.asyncio
    async def test_t9_reset_evicts_the_embedding_client_but_never_warms_it(self, tmp_path):
        """T9: (d) recycles the transport that actually timed out.

        Also pins the negative: the post-reset warmup loop must NOT touch
        "embedding", since warming it means a real billed request per reset.
        """
        config = _make_config(tmp_path)
        config.openai_api_key = "test-embed-key"
        bridge = MemUBridge(config)
        bridge._available = True
        bridge._service = MagicMock()

        clients = {p: MagicMock() for p in ("memorize", "fast", "default", "embedding")}
        for c in clients.values():
            c.client._client.aclose = AsyncMock()

        # One ordered timeline, so "was it evicted", "was re-instrumentation
        # after the eviction" and "did the WARMUP loop touch it" are three
        # independent reads rather than one conflated lookup count. A bare
        # lookup list cannot separate the warmup loop from the
        # re-instrumentation call, which legitimately resolves "embedding".
        timeline: list[tuple[str, str]] = []

        class _RecordingClients(dict):
            def __delitem__(self, key):
                timeline.append(("evict", key))
                super().__delitem__(key)

        cache = _RecordingClients(clients)
        bridge._service._llm_clients = cache

        def _factory(profile):
            """Stand in for memU's _get_llm_base_client: cache, else create.

            Writing the fresh object back into ``cache`` (not the original
            ``clients`` dict) is what makes the post-reset assertions observe
            production behaviour rather than a stale copy. The fresh object is
            a real client stand-in, NOT a MagicMock: a MagicMock auto-creates
            ``embed._nerve_timeout_wrapped`` as a truthy child mock, so the
            "is it wrapped" assertion below could never fail against one.
            """
            timeline.append(("lookup", profile))
            if profile in cache:
                return cache[profile]
            fresh = _FastEmbedClient()
            fresh.client._client.aclose = AsyncMock()
            cache[profile] = fresh
            return fresh

        bridge._service._get_llm_base_client = MagicMock(side_effect=_factory)
        bridge._probe_api_health = AsyncMock(return_value="ok")
        before_embedding = clients["embedding"]

        real_instrument = bridge._instrument_llm_timeouts

        def _tracking_instrument():
            timeline.append(("instrument", "begin"))
            try:
                return real_instrument()
            finally:
                timeline.append(("instrument", "end"))

        bridge._instrument_llm_timeouts = _tracking_instrument

        await bridge._reset_llm_clients_impl()

        assert ("evict", "embedding") in timeline, \
            "the embedding client was not evicted on reset"

        # Production truth: the re-instrumentation that follows the eviction
        # re-resolves the profile through the factory, so the cache is
        # repopulated with a FRESH, wrapped client - it does not stay empty.
        assert "embedding" in bridge._service._llm_clients, \
            "the embedding client was not re-created after the eviction"
        after_embedding = bridge._service._llm_clients["embedding"]
        assert after_embedding is not before_embedding, \
            "the evicted client was reused instead of re-created"
        assert getattr(after_embedding.embed, "_nerve_timeout_wrapped", False), \
            "the fresh embedding client was left unbounded"

        i_begin = timeline.index(("instrument", "begin"))
        i_end = timeline.index(("instrument", "end"))
        last_evict = max(i for i, ev in enumerate(timeline) if ev[0] == "evict")
        assert last_evict < i_begin, \
            "re-instrumentation ran before eviction - the fresh client would not be wrapped"

        # Everything after instrumentation ends is the warmup loop.
        warmed = [p for kind, p in timeline[i_end + 1:] if kind == "lookup"]
        assert warmed == ["memorize", "fast"], f"unexpected warmup set: {warmed}"
        assert "embedding" not in warmed, \
            "the post-reset warmup issued an embeddings request"

    @pytest.mark.asyncio
    async def test_t10_startup_instrumentation_fails_closed(self, tmp_path):
        """T10: the method must NOT swallow its own failure.

        The startup call site relies on propagation to leave the bridge
        unavailable; an internal try/except would silently reintroduce an
        unbounded client behind ``_available = True``.
        """
        client = _FastEmbedClient()
        bridge = _make_embed_bridge(tmp_path, client)
        bridge._service._get_llm_base_client = MagicMock(
            side_effect=RuntimeError("profile exploded"),
        )

        with pytest.raises(RuntimeError, match="profile exploded"):
            bridge._instrument_embedding_timeout()

    @pytest.mark.asyncio
    async def test_t11_reset_path_stays_best_effort(self, tmp_path):
        """T11: an embedding failure cannot break chat re-instrumentation."""
        client = _FastEmbedClient()
        bridge = _make_embed_bridge(tmp_path, client)

        chat_clients = {}
        originals = {}

        def _by_profile(profile):
            if profile == "embedding":
                raise RuntimeError("embedding profile exploded")
            # A concrete fake, not a MagicMock: a mock auto-creates
            # ``chat._nerve_timeout_wrapped`` as a truthy child, which makes
            # production skip wrapping at its sentinel guard AND makes the
            # assertion below pass anyway.
            c = _FastEmbedClient()
            originals[profile] = c.chat
            chat_clients[profile] = c
            return c

        bridge._service._get_llm_base_client = MagicMock(side_effect=_by_profile)

        bridge._instrument_llm_timeouts()          # must NOT raise

        assert set(chat_clients) == {"memorize", "fast", "default"}
        for profile, c in chat_clients.items():
            assert not getattr(originals[profile], "_nerve_timeout_wrapped", False), \
                "fixture pre-carried the sentinel, so the assertion below is vacuous"
            assert c.chat is not originals[profile], \
                f"chat profile {profile} method was never replaced"
            assert getattr(c.chat, "_nerve_timeout_wrapped", False), \
                f"chat profile {profile} was left un-instrumented"

    @pytest.mark.asyncio
    async def test_t12_recall_timeout_is_backend_down_not_empty(self, tmp_path):
        """T12: a bounded-but-failed recall must not answer "no memories".

        Both layers: asyncio.TimeoutError (our wait_for) and
        openai.APITimeoutError (the httpx transport).
        """
        from openai import APITimeoutError
        import httpx

        for exc in (
            asyncio.TimeoutError(),
            APITimeoutError(request=httpx.Request("POST", "http://embeddings.invalid")),
        ):
            config = _make_config(tmp_path)
            bridge = MemUBridge(config)
            bridge._available = True
            bridge._service = MagicMock()
            bridge._service.retrieve = MagicMock(side_effect=exc)

            with pytest.raises(MemoryBackendUnavailable, match="unavailable"):
                await bridge.recall("anything")

    @pytest.mark.asyncio
    async def test_t13_recall_still_returns_empty_for_logic_errors(self, tmp_path):
        """T13: (e) must not over-reach - a genuine error still yields []."""
        config = _make_config(tmp_path)
        bridge = MemUBridge(config)
        bridge._available = True
        bridge._service = MagicMock()
        bridge._service.retrieve = MagicMock(side_effect=ValueError("bad payload"))

        assert await bridge.recall("anything") == []

    @pytest.mark.asyncio
    async def test_t14_no_key_update_path_is_bounded(self, tmp_path):
        """T14: the no-provider update workflow is bounded end to end.

        memU's update workflow embeds changed content through the "embedding"
        profile (memu/app/crud.py:431 declares embed_llm_profile), reached from
        nerve via update_item. Nothing on that route imposes a timeout, so the
        instrumentation is the only bound - and it must apply with no
        openai_api_key, which is the install shape that used to be skipped.
        """
        from memu.app.crud import CRUDMixin
        from memu.app.service import MemoryService
        from memu.llm.wrapper import LLMClientWrapper, LLMInterceptorRegistry

        client = _HangingEmbedClient()
        bridge = _make_embed_bridge(tmp_path, client, has_embeddings=False)
        # built BEFORE instrumentation, exactly as memU builds it at startup
        wrapper = LLMClientWrapper(client, registry=LLMInterceptorRegistry())

        bridge._instrument_embedding_timeout()

        with pytest.raises(asyncio.TimeoutError):
            await _bounded(wrapper.embed(["a changed fact"]))
        assert client.embed_calls == 1

        # Link 1: nerve still forwards the content memU embeds.
        bridge._service.update_memory_item = AsyncMock(return_value={})
        bridge._audit = AsyncMock()
        assert await bridge.update_item("id-1", content="a changed fact") is True
        kwargs = bridge._service.update_memory_item.await_args.kwargs
        assert kwargs["memory_content"] == "a changed fact", \
            "update_item no longer forwards the content memU would embed"

        # Link 2: memU's update step still embeds on the bounded profile.
        # Declaration only, so no MemoryService.__init__ and no per-process cost.
        svc = object.__new__(MemoryService)
        steps = CRUDMixin._build_update_memory_item_workflow(svc)
        step = next(s for s in steps if s.step_id == "update_memory_item")
        profile = MemoryService._llm_profile_from_context(
            {"step_config": step.config}, task="embedding",
        )
        assert profile == "embedding", \
            f"memU's update step no longer embeds on the bounded profile: {profile}"

    def test_t15_startup_instrumentation_failure_leaves_the_bridge_unavailable(self, tmp_path):
        """T15: edit (c) is uncaught, so a failure there must fail closed.

        Runs the REAL _initialize_impl in a fresh interpreter: only one
        MemoryService may exist per process (a second raises ArgumentError on
        a re-declared column), which is why T8 settles for source order. A
        call-site try/except would leave the bridge available with an
        unbounded client, and no in-process arm can observe that.
        """
        import os
        import subprocess
        import sys

        script = (
            "import asyncio, json, sqlite3, sys\n"
            "from unittest.mock import AsyncMock\n"
            "from nerve.config import MemoryCategoryConfig, MemoryConfig, NerveConfig\n"
            "from nerve.memory import memu_bridge as mb\n"
            "db = sys.argv[1]\n"
            "if sys.argv[2] == 'break':\n"
            "    def _boom(self):\n"
            "        raise RuntimeError('instrumentation exploded')\n"
            "    mb.MemUBridge._instrument_embedding_timeout = _boom\n"
            "async def main():\n"
            "    c = NerveConfig()\n"
            "    c.memory = MemoryConfig(sqlite_dsn='sqlite:///' + db)\n"
            "    c.memory.categories = [MemoryCategoryConfig(name='probes', description='d')]\n"
            "    c.anthropic_api_key = 'k'\n"
            # unroutable local port, so the warmup never reaches a real endpoint
            "    c.proxy.enabled = True\n"
            "    c.proxy.host = '127.0.0.1'\n"
            "    c.proxy.port = 1\n"
            "    b = mb.MemUBridge(c)\n"
            "    b._audit = AsyncMock()\n"
            "    rc = await b.initialize()\n"
            "    rows = -1\n"
            "    try:\n"
            "        con = sqlite3.connect(db)\n"
            "        rows = list(con.execute("
            "'select count(*) from memu_memory_categories'))[0][0]\n"
            "    except sqlite3.OperationalError:\n"
            "        rows = 0\n"          # table never created at all
            "    print('NERVE_RESULT ' + json.dumps("
            "{'rc': bool(rc), 'available': bool(b.available), 'rows': rows}))\n"
            "    await b.shutdown()\n"
            "asyncio.run(main())\n"
        )

        def run(mode):
            db = tmp_path / f"memu-{mode}.sqlite"
            proc = subprocess.run(
                [sys.executable, "-c", script, str(db), mode],
                capture_output=True, text=True, timeout=300,
                cwd=str(Path(__file__).resolve().parent.parent),
                env={**os.environ, "NERVE_HOME": str(tmp_path / f"home-{mode}")},
            )
            marker = [ln for ln in proc.stdout.splitlines()
                      if ln.startswith("NERVE_RESULT ")]
            assert marker, (
                f"no result marker from the {mode} run\n"
                f"stdout tail:\n{proc.stdout[-2000:]}\n"
                f"stderr tail:\n{proc.stderr[-2000:]}"
            )
            return json.loads(marker[-1][len("NERVE_RESULT "):])

        # Control first: a later False must not be the fixture failing for an
        # unrelated reason.
        ok = run("intact")
        assert ok["rc"] is True, f"control startup failed: {ok}"
        assert ok["available"] is True
        assert ok["rows"] >= 1, "control persisted no category, so rows==0 proves nothing"

        broken = run("break")
        assert broken["rc"] is False, \
            "startup succeeded despite the instrumentation raising - it is caught somewhere"
        assert broken["available"] is False, \
            "the bridge advertised itself as usable with an unbounded embedding client"
        assert broken["rows"] == 0, \
            "a category row was persisted after the bound failed to install"
