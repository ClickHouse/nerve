"""Tests for reset-aware per-turn cost accounting.

The SDK's ``ResultMessage.total_cost_usd`` is cumulative per CLI client
process.  This subsystem has produced two accounting incidents: treating
the cumulative as per-turn (over-count, fixed by v024) and clamping the
delta with ``max(delta, 0)`` (under-count on every client recycle —
recorded $0 for the first turn of each new client, i.e. every persistent
cron run).  These tests pin the reset-aware semantics
(``nerve.db.usage.compute_turn_cost``), the engine's baseline reset on
client creation, the v036 backfill migration, and the FTS5 query
sanitization that broke while filing the incident.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from nerve.db import Database
from nerve.db.migrations.v036_backfill_zero_cost_turns import up as v036_up
from nerve.db.usage import compute_turn_cost, estimate_turn_cost

# A usage dict with real token traffic (estimate well above the backstop
# threshold on any pricing tier).
_BUSY_USAGE = {
    "input_tokens": 1_000,
    "output_tokens": 500,
    "cache_read_input_tokens": 200_000,
    "cache_creation_input_tokens": 20_000,
}


# ---------------------------------------------------------------------------
# compute_turn_cost: reset-aware delta semantics
# ---------------------------------------------------------------------------


class TestComputeTurnCost:
    def test_monotonic_increase_is_exact_delta(self):
        cost, source = compute_turn_cost(5.2, 5.0, _BUSY_USAGE, model="claude-fable-5")
        assert cost == pytest.approx(0.2)
        assert source == "sdk_delta"

    def test_fresh_session_uses_full_cumulative(self):
        cost, source = compute_turn_cost(0.3, 0, _BUSY_USAGE, model="claude-fable-5")
        assert cost == pytest.approx(0.3)
        # prev=0 is the fresh-session baseline: plain delta, no reset.
        assert source == "sdk_delta"

    def test_counter_reset_attributes_new_cumulative(self):
        # Client recycled: cumulative dropped from 5.0 to 0.3.  The old
        # max(delta, 0) clamp recorded $0 here — the core of the bug.
        cost, source = compute_turn_cost(0.3, 5.0, _BUSY_USAGE, model="claude-fable-5")
        assert cost == pytest.approx(0.3)
        assert source == "sdk_reset"

    def test_no_sdk_cost_falls_back_to_estimate(self):
        cost, source = compute_turn_cost(None, 5.0, _BUSY_USAGE, model="claude-fable-5")
        assert cost == estimate_turn_cost(_BUSY_USAGE, model="claude-fable-5")
        assert cost > 0
        assert source == "estimate"

    def test_zero_cost_with_traffic_uses_estimate_backstop(self):
        # Counter stuck: same cumulative as before despite real tokens.
        cost, source = compute_turn_cost(5.0, 5.0, _BUSY_USAGE, model="claude-fable-5")
        assert cost == estimate_turn_cost(_BUSY_USAGE, model="claude-fable-5")
        assert cost > 0
        assert source == "estimate_backstop"

    def test_reset_to_zero_with_traffic_uses_estimate_backstop(self):
        # Recycled client that reports 0.0 cumulative on its first turn
        # (e.g. a provider that does not price usage).
        cost, source = compute_turn_cost(0.0, 5.0, _BUSY_USAGE, model="claude-fable-5")
        assert cost == estimate_turn_cost(_BUSY_USAGE, model="claude-fable-5")
        assert source == "estimate_backstop"

    def test_genuinely_free_turn_stays_zero(self):
        # No token traffic → the zero is real, no false-positive backstop.
        cost, source = compute_turn_cost(5.0, 5.0, {}, model="claude-fable-5")
        assert cost == 0
        assert source == "sdk_delta"

    def test_tiny_turn_below_threshold_keeps_sdk_zero(self):
        # A handful of tokens estimates below the 0.0005 backstop
        # threshold — trust the SDK's zero.
        tiny = {"input_tokens": 10, "output_tokens": 5}
        cost, source = compute_turn_cost(5.0, 5.0, tiny, model="claude-haiku-4-5")
        assert cost == 0
        assert source == "sdk_delta"

    def test_reset_with_nonzero_first_turn_not_backstopped(self):
        # Reset with a real first-turn cost: use the SDK number, not the
        # estimate, even if they differ.
        cost, source = compute_turn_cost(0.003, 9.9, _BUSY_USAGE, model="claude-fable-5")
        assert cost == pytest.approx(0.003)
        assert source == "sdk_reset"

    def test_none_usage_is_safe(self):
        cost, source = compute_turn_cost(None, 0, None, model=None)
        assert cost == 0
        assert source == "estimate"


# ---------------------------------------------------------------------------
# Engine: baseline zeroed when a new CLI client is created
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestResetCostBaseline:
    async def _shim(self, db: Database):
        return SimpleNamespace(db=db)

    async def test_baseline_zeroed(self, db: Database):
        from nerve.agent.engine import AgentEngine

        await db.create_session("sess-base", source="web")
        await db.update_session_metadata(
            "sess-base", {"_sdk_cumulative_cost": 4.2, "other_key": "kept"},
        )

        await AgentEngine._reset_cost_baseline(await self._shim(db), "sess-base")

        session = await db.get_session("sess-base")
        meta = json.loads(session["metadata"])
        assert meta["_sdk_cumulative_cost"] == 0
        # Sibling metadata keys survive the rewrite.
        assert meta["other_key"] == "kept"

    async def test_noop_when_baseline_absent(self, db: Database):
        from nerve.agent.engine import AgentEngine

        await db.create_session("sess-nobase", source="web")
        await db.update_session_metadata("sess-nobase", {"other_key": "kept"})

        await AgentEngine._reset_cost_baseline(await self._shim(db), "sess-nobase")

        meta = json.loads((await db.get_session("sess-nobase"))["metadata"])
        assert meta == {"other_key": "kept"}

    async def test_missing_session_is_safe(self, db: Database):
        from nerve.agent.engine import AgentEngine

        await AgentEngine._reset_cost_baseline(await self._shim(db), "no-such-session")


# ---------------------------------------------------------------------------
# v036 migration: backfill zero-cost turns from token counts
# ---------------------------------------------------------------------------

# Seed rows for the "repaired" session.  Costs recompute from token
# counts with model pricing — cross-checked against estimate_turn_cost.
_ROW_SPLIT = {
    "input_tokens": 1_000,
    "output_tokens": 500,
    "cache_read_input_tokens": 100_000,
    "cache_creation_input_tokens": 20_000,
    "cache_creation": {
        "ephemeral_5m_input_tokens": 15_000,
        "ephemeral_1h_input_tokens": 5_000,
    },
}
_ROW_AGGREGATE = {
    "input_tokens": 2_000,
    "output_tokens": 1_000,
    "cache_read_input_tokens": 50_000,
    "cache_creation_input_tokens": 10_000,
    "server_tool_use": {"web_search_requests": 3},
}


async def _fetch_costs(db: Database, session_id: str) -> list[float]:
    async with db.db.execute(
        "SELECT cost_usd FROM session_usage WHERE session_id = ? ORDER BY rowid",
        (session_id,),
    ) as cur:
        return [row[0] async for row in cur]


async def _session_total(db: Database, session_id: str) -> float:
    session = await db.get_session(session_id)
    return session["total_cost_usd"] or 0


@pytest.mark.asyncio
class TestBackfillMigration:
    async def _seed(self, db: Database) -> None:
        # Session A: two swallowed turns (one with the 5m/1h split, one
        # aggregate-only + web searches) plus one correctly-priced turn.
        await db.create_session("sess-a", source="cron")
        await db.record_turn_usage(
            session_id="sess-a",
            input_tokens=_ROW_SPLIT["input_tokens"],
            output_tokens=_ROW_SPLIT["output_tokens"],
            cache_creation=_ROW_SPLIT["cache_creation_input_tokens"],
            cache_read=_ROW_SPLIT["cache_read_input_tokens"],
            cache_creation_5m=15_000,
            cache_creation_1h=5_000,
            max_context=200_000,
            model="claude-fable-5",
            cost_usd=0,
        )
        await db.record_turn_usage(
            session_id="sess-a",
            input_tokens=_ROW_AGGREGATE["input_tokens"],
            output_tokens=_ROW_AGGREGATE["output_tokens"],
            cache_creation=_ROW_AGGREGATE["cache_creation_input_tokens"],
            cache_read=_ROW_AGGREGATE["cache_read_input_tokens"],
            max_context=200_000,
            model="claude-opus-4-7",
            cost_usd=0,
            web_search_requests=3,
        )
        await db.record_turn_usage(
            session_id="sess-a",
            input_tokens=100, output_tokens=100,
            cache_creation=0, cache_read=0,
            max_context=200_000,
            model="claude-fable-5",
            cost_usd=1.0,
        )
        # Stored total only ever saw the correctly-priced turn.
        await db.update_session_fields("sess-a", {"total_cost_usd": 1.0})

        # Session B: a zero-cost row with zero tokens — NOT a target.
        await db.create_session("sess-b", source="web")
        await db.record_turn_usage(
            session_id="sess-b",
            input_tokens=0, output_tokens=0,
            cache_creation=0, cache_read=0,
            max_context=200_000,
            cost_usd=0,
        )
        await db.update_session_fields("sess-b", {"total_cost_usd": 0})

        # Session C: swallowed turn with unknown model → default pricing.
        await db.create_session("sess-c", source="telegram")
        await db.record_turn_usage(
            session_id="sess-c",
            input_tokens=1_000_000, output_tokens=0,
            cache_creation=0, cache_read=0,
            max_context=200_000,
            model=None,
            cost_usd=0,
        )
        await db.update_session_fields("sess-c", {"total_cost_usd": 0})

        # Session D: total accumulated from usage rows that telemetry
        # pruning has since deleted.  The migration must NOT re-sum this
        # to zero — pruned history is legitimate spend.
        await db.create_session("sess-d", source="web")
        await db.update_session_fields("sess-d", {"total_cost_usd": 10.0})

    async def test_backfill_recomputes_from_tokens(self, db: Database):
        await self._seed(db)
        await v036_up(db.db)

        costs = await _fetch_costs(db, "sess-a")
        expected_split = estimate_turn_cost(_ROW_SPLIT, model="claude-fable-5")
        expected_agg = estimate_turn_cost(_ROW_AGGREGATE, model="claude-opus-4-7")
        assert costs[0] == pytest.approx(expected_split)
        assert costs[1] == pytest.approx(expected_agg)
        assert costs[0] > 0 and costs[1] > 0
        # The correctly-priced row is untouched.
        assert costs[2] == pytest.approx(1.0)

    async def test_backfill_uses_default_pricing_for_unknown_model(self, db: Database):
        await self._seed(db)
        await v036_up(db.db)

        costs = await _fetch_costs(db, "sess-c")
        # 1M fresh input tokens at the default tier ($5/MTok).
        assert costs[0] == pytest.approx(5.0)

    async def test_zero_token_rows_untouched(self, db: Database):
        await self._seed(db)
        await v036_up(db.db)

        costs = await _fetch_costs(db, "sess-b")
        assert costs == [0]

    async def test_session_totals_add_recovered_delta(self, db: Database):
        await self._seed(db)
        await v036_up(db.db)

        expected_split = estimate_turn_cost(_ROW_SPLIT, model="claude-fable-5")
        expected_agg = estimate_turn_cost(_ROW_AGGREGATE, model="claude-opus-4-7")
        assert await _session_total(db, "sess-a") == pytest.approx(
            1.0 + expected_split + expected_agg,
        )
        assert await _session_total(db, "sess-c") == pytest.approx(5.0)
        assert await _session_total(db, "sess-b") == 0
        # Totals now match the per-turn sum for sessions with full history.
        totals_a = await db.get_session_usage_totals("sess-a")
        assert totals_a["total_cost_usd"] == pytest.approx(
            await _session_total(db, "sess-a"),
        )

    async def test_pruned_session_total_preserved(self, db: Database):
        await self._seed(db)
        await v036_up(db.db)

        assert await _session_total(db, "sess-d") == pytest.approx(10.0)

    async def test_rerun_is_idempotent(self, db: Database):
        await self._seed(db)
        await v036_up(db.db)

        costs_first = {
            sid: await _fetch_costs(db, sid)
            for sid in ("sess-a", "sess-b", "sess-c")
        }
        totals_first = {
            sid: await _session_total(db, sid)
            for sid in ("sess-a", "sess-b", "sess-c", "sess-d")
        }

        await v036_up(db.db)

        for sid, costs in costs_first.items():
            assert await _fetch_costs(db, sid) == costs
        for sid, total in totals_first.items():
            assert await _session_total(db, sid) == pytest.approx(total)

    async def test_empty_db_is_noop(self, db: Database):
        await v036_up(db.db)


# ---------------------------------------------------------------------------
# FTS5 query sanitization (the dup-check crash found while filing this)
# ---------------------------------------------------------------------------


class TestFtsQueryBuilding:
    def test_dollar_sign_stripped_from_fts_query(self):
        q = Database._build_fts_query("cost recorded as $0 per turn")
        assert q  # real terms survive
        assert "$" not in q

    def test_operator_characters_stripped(self):
        q = Database._build_fts_query("a+b=c & 100% <weird> | $money NEAR near")
        assert q
        for ch in "+=&%<>|$":
            assert ch not in q

    def test_unicode_words_preserved(self):
        words = Database._tokenize_query("стоимость сессии $55")
        assert "стоимость" in words
        assert "сессии" in words


@pytest.mark.asyncio
class TestFtsSanitization:
    async def _seed_task(self, db: Database) -> None:
        await db.upsert_task(
            task_id="2026-01-01-turn-cost-recorded-as-zero",
            file_path="tasks/2026-01-01-turn-cost-recorded-as-zero.md",
            title="Turn cost recorded as $0 after client recycle",
            content="Cost counter resets swallow the first turn (about $100).",
            tags="accounting,cost",
        )

    async def test_search_tasks_with_dollar_query(self, db: Database):
        await self._seed_task(db)
        # Previously raised `fts5: syntax error near "$"`.
        results = await db.search_tasks("cost recorded as $0")
        assert any(t["id"] == "2026-01-01-turn-cost-recorded-as-zero" for t in results)

    async def test_search_tasks_similar_with_special_chars(self, db: Database):
        await self._seed_task(db)
        # The duplicate-check path used by task_create.
        results = await db.search_tasks_similar(
            "Per-turn cost silently recorded as $0 — max(delta,0) clamp "
            "swallows ~$9.99 & 100% of cron runs",
        )
        assert any(t["id"] == "2026-01-01-turn-cost-recorded-as-zero" for t in results)

    async def test_search_all_punctuation_query_is_safe(self, db: Database):
        await self._seed_task(db)
        # Nothing but FTS operators/punctuation → no crash, graceful result.
        await db.search_tasks("$$$ +++ ===")
        await db.search_tasks_similar("$$$ +++ ===")
