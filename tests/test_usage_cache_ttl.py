"""Tests for the 5m/1h ephemeral cache split (migration v027).

Covers the path from the raw Anthropic API usage shape through the
``UsageStore`` persistence layer and back out via the aggregate queries
and the cost estimator.
"""

from __future__ import annotations

import pytest

from nerve.db import Database
from nerve.db.usage import (
    DEFAULT_PRICING,
    MODEL_PRICING,
    estimate_cost_from_totals,
    estimate_turn_cost,
    extract_cache_ttl_split,
)


# ---------------------------------------------------------------------------
# Schema: v027 columns exist and default to 0
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestSchema:
    async def test_v027_columns_present(self, db: Database):
        async with db.db.execute("PRAGMA table_info(session_usage)") as cur:
            cols = {row[1] async for row in cur}
        assert "cache_creation_5m_input_tokens" in cols
        assert "cache_creation_1h_input_tokens" in cols

    async def test_v027_columns_default_zero(self, db: Database):
        # Schema was rolled forward by the fixture — the new columns
        # must be addable to existing rows with a 0 default.
        await db.create_session("sess-default", source="web")
        await db.record_turn_usage(
            session_id="sess-default",
            input_tokens=100,
            output_tokens=50,
            cache_creation=500,
            cache_read=2000,
            max_context=200_000,
        )
        async with db.db.execute(
            "SELECT cache_creation_5m_input_tokens, cache_creation_1h_input_tokens "
            "FROM session_usage WHERE session_id = ?",
            ("sess-default",),
        ) as cur:
            row = await cur.fetchone()
        assert row is not None
        assert row[0] == 0
        assert row[1] == 0


# ---------------------------------------------------------------------------
# extract_cache_ttl_split: parse the API usage dict
# ---------------------------------------------------------------------------


class TestExtractSplit:
    def test_returns_zeros_when_field_missing(self):
        assert extract_cache_ttl_split({}) == (0, 0)
        assert extract_cache_ttl_split({"input_tokens": 10}) == (0, 0)

    def test_returns_zeros_when_field_not_a_dict(self):
        # Defensive: API has historically returned ints or nulls for
        # cache_creation in older responses.
        assert extract_cache_ttl_split({"cache_creation": None}) == (0, 0)
        assert extract_cache_ttl_split({"cache_creation": 42}) == (0, 0)

    def test_parses_split(self):
        usage = {
            "cache_creation": {
                "ephemeral_5m_input_tokens": 148,
                "ephemeral_1h_input_tokens": 100,
            },
        }
        assert extract_cache_ttl_split(usage) == (148, 100)

    def test_missing_keys_default_to_zero(self):
        usage = {"cache_creation": {"ephemeral_5m_input_tokens": 50}}
        assert extract_cache_ttl_split(usage) == (50, 0)


# ---------------------------------------------------------------------------
# Persistence: record_turn_usage stores the split, queries return it
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestPersistence:
    async def test_record_persists_split(self, db: Database):
        await db.create_session("sess-split", source="web")
        await db.record_turn_usage(
            session_id="sess-split",
            input_tokens=200,
            output_tokens=80,
            cache_creation=300,
            cache_read=1500,
            cache_creation_5m=200,
            cache_creation_1h=100,
            max_context=200_000,
            model="claude-opus-4-7",
        )
        async with db.db.execute(
            "SELECT cache_creation_5m_input_tokens, cache_creation_1h_input_tokens "
            "FROM session_usage WHERE session_id = ?",
            ("sess-split",),
        ) as cur:
            row = await cur.fetchone()
        assert row is not None
        assert row[0] == 200
        assert row[1] == 100

    async def test_session_totals_include_split(self, db: Database):
        await db.create_session("sess-totals", source="web")
        await db.record_turn_usage(
            session_id="sess-totals",
            input_tokens=200, output_tokens=80,
            cache_creation=300, cache_read=1500,
            cache_creation_5m=200, cache_creation_1h=100,
            max_context=200_000,
        )
        await db.record_turn_usage(
            session_id="sess-totals",
            input_tokens=50, output_tokens=20,
            cache_creation=80, cache_read=500,
            cache_creation_5m=30, cache_creation_1h=50,
            max_context=200_000,
        )
        totals = await db.get_session_usage_totals("sess-totals")
        assert totals["total_cache_creation_5m"] == 230
        assert totals["total_cache_creation_1h"] == 150
        # Aggregate must still match the sum of the legacy column.
        assert totals["total_cache_creation"] == 380

    async def test_session_totals_zero_when_empty(self, db: Database):
        # Empty-session response should include the new keys so callers
        # can index them unconditionally.
        totals = await db.get_session_usage_totals("nonexistent")
        assert totals["total_cache_creation_5m"] == 0
        assert totals["total_cache_creation_1h"] == 0


# ---------------------------------------------------------------------------
# Pricing: 5m and 1h have distinct rates
# ---------------------------------------------------------------------------


class TestPricing:
    def test_pricing_tuple_has_six_fields(self):
        # Every entry must carry input, output, cache_read,
        # cache_write_5m, cache_write_1h, web_search.
        for name, tup in MODEL_PRICING.items():
            assert len(tup) == 6, f"{name} pricing wrong arity: {tup}"
        assert len(DEFAULT_PRICING) == 6

    def test_1h_more_expensive_than_5m(self):
        for name, tup in MODEL_PRICING.items():
            _, _, _, c5m, c1h, _ = tup
            assert c1h > c5m, f"{name}: 1h write should cost more than 5m"

    def test_opus_47_rates(self):
        # Anchor: Opus 4.7 specifically — base $5/MTok input.
        # 5m write = 1.25x = $6.25/MTok, 1h write = 2.00x = $10.00/MTok.
        from nerve.db.usage import _get_pricing
        p_in, _, _, c5m, c1h, _ = _get_pricing("claude-opus-4-7")
        assert p_in == 5
        assert c5m == 6.25
        assert c1h == 10.00


class TestEstimateCost:
    def test_falls_back_to_5m_when_split_absent(self):
        # Legacy path: no cache_creation field → bill the aggregate at
        # the 5-minute rate. Confirms backward compatibility for
        # pre-v027 rows.
        cost = estimate_turn_cost(
            {
                "input_tokens": 0, "output_tokens": 0,
                "cache_read_input_tokens": 0,
                "cache_creation_input_tokens": 1_000_000,
            },
            model="claude-opus-4-7",
        )
        # 1M tokens * $6.25/MTok = $6.25
        assert cost == 6.25

    def test_uses_split_when_present(self):
        # 0.5M @ 5m ($6.25/M) + 0.5M @ 1h ($10/M) for Opus 4.7
        cost = estimate_turn_cost(
            {
                "input_tokens": 0, "output_tokens": 0,
                "cache_read_input_tokens": 0,
                "cache_creation_input_tokens": 1_000_000,
                "cache_creation": {
                    "ephemeral_5m_input_tokens": 500_000,
                    "ephemeral_1h_input_tokens": 500_000,
                },
            },
            model="claude-opus-4-7",
        )
        # 500_000 * $6.25/M + 500_000 * $10/M = 3.125 + 5.000 = $8.125
        assert cost == 8.125

    def test_pure_1h_cache_write(self):
        cost = estimate_turn_cost(
            {
                "input_tokens": 0, "output_tokens": 0,
                "cache_read_input_tokens": 0,
                "cache_creation_input_tokens": 1_000_000,
                "cache_creation": {
                    "ephemeral_5m_input_tokens": 0,
                    "ephemeral_1h_input_tokens": 1_000_000,
                },
            },
            model="claude-opus-4-7",
        )
        # 1M @ $10/M = $10
        assert cost == 10.0

    def test_totals_estimator_honors_split(self):
        cost = estimate_cost_from_totals(
            {
                "total_input": 0, "total_output": 0,
                "total_cache_read": 0,
                "total_cache_creation": 1_000_000,
                "total_cache_creation_5m": 500_000,
                "total_cache_creation_1h": 500_000,
            },
            model="claude-opus-4-7",
        )
        assert cost == 8.125

    def test_totals_estimator_falls_back_without_split(self):
        # Pre-v027 aggregate without the split keys.
        cost = estimate_cost_from_totals(
            {
                "total_input": 0, "total_output": 0,
                "total_cache_read": 0,
                "total_cache_creation": 1_000_000,
            },
            model="claude-opus-4-7",
        )
        assert cost == 6.25
