"""Token usage tracking data access methods."""

from __future__ import annotations


# ---------------------------------------------------------------------------
# Model pricing (per 1M tokens, USD)
# ---------------------------------------------------------------------------
# Mirrors Claude Code's modelCost.ts pricing tiers.
# Key: canonical model short-name substring → (input, output, cache_read, cache_write, web_search_per_req)
MODEL_PRICING: dict[str, tuple[float, float, float, float, float]] = {
    "opus-4-7":   (5, 25, 0.50, 6.25, 0.01),      # Opus 4.7 standard
    "opus-4-6":   (5, 25, 0.50, 6.25, 0.01),      # Opus 4.6 standard
    "opus-4-5":   (5, 25, 0.50, 6.25, 0.01),      # Opus 4.5
    "opus-4-1":   (15, 75, 1.50, 18.75, 0.01),     # Opus 4.1
    "opus-4":     (15, 75, 1.50, 18.75, 0.01),     # Opus 4
    "sonnet-4":   (3, 15, 0.30, 3.75, 0.01),       # Sonnet 4.x
    "haiku-4-5":  (1, 5, 0.10, 1.25, 0.01),        # Haiku 4.5
    "haiku-3-5":  (0.8, 4, 0.08, 1.0, 0.01),       # Haiku 3.5
}

# Default fallback when model is unknown or None
DEFAULT_PRICING = (5, 25, 0.50, 6.25, 0.01)  # Opus 4.6 standard (most common)


def _get_pricing(model: str | None) -> tuple[float, float, float, float, float]:
    """Resolve model string to pricing tier."""
    if not model:
        return DEFAULT_PRICING
    m = model.lower()
    # Check from most specific to least specific
    for key, pricing in MODEL_PRICING.items():
        if key in m:
            return pricing
    return DEFAULT_PRICING


class UsageStore:
    """Mixin providing per-turn token usage persistence and aggregate queries."""

    async def record_turn_usage(
        self,
        session_id: str,
        input_tokens: int,
        output_tokens: int,
        cache_creation: int,
        cache_read: int,
        max_context: int,
        *,
        model: str | None = None,
        cost_usd: float | None = None,
        duration_ms: int | None = None,
        duration_api_ms: int | None = None,
        num_turns: int = 1,
        web_search_requests: int = 0,
        web_fetch_requests: int = 0,
    ) -> None:
        """Record token usage for a single agent turn."""
        await self.db.execute(
            "INSERT INTO session_usage "
            "(session_id, input_tokens, output_tokens, "
            "cache_creation_input_tokens, cache_read_input_tokens, "
            "max_context_tokens, model, cost_usd, duration_ms, "
            "duration_api_ms, num_turns, web_search_requests, web_fetch_requests) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                session_id, input_tokens, output_tokens,
                cache_creation, cache_read, max_context,
                model, cost_usd, duration_ms,
                duration_api_ms, num_turns,
                web_search_requests, web_fetch_requests,
            ),
        )
        await self.db.commit()

    async def get_session_usage_totals(self, session_id: str) -> dict:
        """Aggregate token usage for a session."""
        async with self.db.execute(
            """
            SELECT
                COUNT(*) as turns,
                COALESCE(SUM(input_tokens), 0) as total_input,
                COALESCE(SUM(output_tokens), 0) as total_output,
                COALESCE(SUM(cache_creation_input_tokens), 0) as total_cache_creation,
                COALESCE(SUM(cache_read_input_tokens), 0) as total_cache_read,
                MAX(max_context_tokens) as max_context,
                COALESCE(SUM(cost_usd), 0) as total_cost_usd,
                COALESCE(SUM(web_search_requests), 0) as total_web_searches,
                COALESCE(SUM(web_fetch_requests), 0) as total_web_fetches
            FROM session_usage WHERE session_id = ?
            """,
            (session_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if not row or row[0] == 0:
                return {"turns": 0, "total_input": 0, "total_output": 0,
                        "total_cache_creation": 0, "total_cache_read": 0,
                        "max_context": 0, "total_cost_usd": 0,
                        "total_web_searches": 0, "total_web_fetches": 0}
            return dict(row)

    async def get_usage_by_period(self, days: int = 7) -> list[dict]:
        """Daily aggregated usage for the past N days."""
        async with self.db.execute(
            """
            SELECT
                DATE(created_at) as date,
                COUNT(*) as turns,
                SUM(input_tokens) as input_tokens,
                SUM(output_tokens) as output_tokens,
                SUM(cache_creation_input_tokens) as cache_creation,
                SUM(cache_read_input_tokens) as cache_read,
                COALESCE(SUM(cost_usd), 0) as cost_usd,
                COALESCE(SUM(web_search_requests), 0) as web_searches
            FROM session_usage
            WHERE created_at >= DATE('now', ?)
            GROUP BY DATE(created_at)
            ORDER BY date DESC
            """,
            (f"-{days} days",),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_usage_by_source(self, days: int = 7) -> list[dict]:
        """Usage aggregated by session source (web, cron, telegram, etc.)."""
        async with self.db.execute(
            """
            SELECT
                s.source,
                COUNT(DISTINCT u.session_id) as sessions,
                COUNT(*) as turns,
                SUM(u.input_tokens) as input_tokens,
                SUM(u.output_tokens) as output_tokens,
                SUM(u.cache_creation_input_tokens) as cache_creation,
                SUM(u.cache_read_input_tokens) as cache_read,
                COALESCE(SUM(u.cost_usd), 0) as cost_usd,
                COALESCE(SUM(u.web_search_requests), 0) as web_searches
            FROM session_usage u
            JOIN sessions s ON s.id = u.session_id
            WHERE u.created_at >= DATE('now', ?)
            GROUP BY s.source
            ORDER BY COALESCE(SUM(u.cost_usd), 0) DESC
            """,
            (f"-{days} days",),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_usage_by_model(self, days: int = 7) -> list[dict]:
        """Usage aggregated by model for the past N days."""
        async with self.db.execute(
            """
            SELECT
                COALESCE(model, 'unknown') as model,
                COUNT(*) as turns,
                SUM(input_tokens) as input_tokens,
                SUM(output_tokens) as output_tokens,
                SUM(cache_creation_input_tokens) as cache_creation,
                SUM(cache_read_input_tokens) as cache_read,
                COALESCE(SUM(cost_usd), 0) as cost_usd,
                COALESCE(SUM(web_search_requests), 0) as web_searches
            FROM session_usage
            WHERE created_at >= DATE('now', ?)
            GROUP BY COALESCE(model, 'unknown')
            ORDER BY COALESCE(SUM(cost_usd), 0) DESC
            """,
            (f"-{days} days",),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_cache_hit_rate(
        self, session_id: str | None = None, days: int = 7,
    ) -> dict:
        """Calculate cache hit rate: cache_read / total_input."""
        if session_id:
            query = """
                SELECT
                    COALESCE(SUM(cache_read_input_tokens), 0) as total_read,
                    COALESCE(SUM(cache_creation_input_tokens), 0) as total_creation,
                    COALESCE(SUM(input_tokens), 0) as total_input
                FROM session_usage WHERE session_id = ?
            """
            params: tuple = (session_id,)
        else:
            query = """
                SELECT
                    COALESCE(SUM(cache_read_input_tokens), 0) as total_read,
                    COALESCE(SUM(cache_creation_input_tokens), 0) as total_creation,
                    COALESCE(SUM(input_tokens), 0) as total_input
                FROM session_usage WHERE created_at >= DATE('now', ?)
            """
            params = (f"-{days} days",)

        async with self.db.execute(query, params) as cursor:
            row = await cursor.fetchone()
            total_read = row[0] or 0
            total_creation = row[1] or 0
            fresh_input = row[2] or 0
            # SDK's input_tokens is only non-cached input; real total includes all three
            total_all_input = fresh_input + total_read + total_creation
            rate = total_read / total_all_input if total_all_input > 0 else 0.0
            return {
                "rate": round(rate, 4),
                "total_read": total_read,
                "total_creation": total_creation,
                "total_input": total_all_input,
            }

    async def get_usage_summary(self, days: int = 7) -> dict:
        """High-level usage summary for the past N days.

        Uses SDK-provided cost_usd when available; falls back to estimation
        for legacy rows that predate v021 (cost_usd is NULL).
        """
        async with self.db.execute(
            """
            SELECT
                COUNT(*) as turns,
                COUNT(DISTINCT session_id) as sessions,
                COALESCE(SUM(input_tokens), 0) as total_input,
                COALESCE(SUM(output_tokens), 0) as total_output,
                COALESCE(SUM(cache_read_input_tokens), 0) as total_cache_read,
                COALESCE(SUM(cache_creation_input_tokens), 0) as total_cache_creation,
                COALESCE(SUM(cost_usd), 0) as total_cost_usd,
                COALESCE(SUM(web_search_requests), 0) as total_web_searches
            FROM session_usage
            WHERE created_at >= DATE('now', ?)
            """,
            (f"-{days} days",),
        ) as cursor:
            row = await cursor.fetchone()
            if not row or row[0] == 0:
                return {"turns": 0, "sessions": 0, "total_input": 0,
                        "total_output": 0, "total_cache_read": 0,
                        "total_cache_creation": 0, "total_cost_usd": 0,
                        "total_web_searches": 0}
            d = dict(row)
            # If SDK cost is available, use it; otherwise fall back to estimate
            if not d["total_cost_usd"]:
                d["total_cost_usd"] = round(estimate_cost_from_totals(d), 4)
            else:
                d["total_cost_usd"] = round(d["total_cost_usd"], 4)
            return d

    async def delete_session_usage(self, session_id: str) -> None:
        """Delete all usage records for a session (cascade on delete)."""
        await self.db.execute(
            "DELETE FROM session_usage WHERE session_id = ?", (session_id,),
        )
        await self.db.commit()


def estimate_turn_cost(usage: dict, model: str | None = None) -> float:
    """Estimate USD cost from a single turn's token counts.

    Uses model-specific pricing when model is provided, otherwise defaults
    to Opus 4.6 standard pricing.

    NOTE: The SDK's ``input_tokens`` is already the *non-cached* portion.
    Total input = input_tokens + cache_read + cache_creation.
    """
    pricing = _get_pricing(model)
    p_input, p_output, p_cache_read, p_cache_write, p_web_search = pricing

    fresh_input = usage.get("input_tokens", 0)
    output_t = usage.get("output_tokens", 0)
    cache_read = usage.get("cache_read_input_tokens", 0)
    cache_create = usage.get("cache_creation_input_tokens", 0)
    server_tool = usage.get("server_tool_use") or {}
    web_searches = server_tool.get("web_search_requests", 0)

    cost = (
        fresh_input * p_input / 1_000_000
        + cache_read * p_cache_read / 1_000_000
        + cache_create * p_cache_write / 1_000_000
        + output_t * p_output / 1_000_000
        + web_searches * p_web_search
    )
    return round(cost, 6)


def estimate_cost_from_totals(totals: dict, model: str | None = None) -> float:
    """Estimate USD cost from aggregated totals."""
    return estimate_turn_cost({
        "input_tokens": totals.get("total_input", 0),
        "output_tokens": totals.get("total_output", 0),
        "cache_read_input_tokens": totals.get("total_cache_read", 0),
        "cache_creation_input_tokens": totals.get("total_cache_creation", 0),
    }, model=model)
