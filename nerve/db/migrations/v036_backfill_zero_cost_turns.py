"""V36: Backfill per-turn costs swallowed by SDK client recycling.

The Claude Agent SDK's ``ResultMessage.total_cost_usd`` is *cumulative*
per CLI client process.  The engine derives per-turn cost by diffing
consecutive cumulative values, persisting the last seen value in session
metadata.  The metadata survives client recycling but the counter does
not: every recycle (idle sweep, oneshot cron teardown, restart, model
switch) starts a fresh CLI process whose cumulative begins near zero —
*below* the persisted high-water mark.  The old ``max(delta, 0)`` clamp
turned that negative diff into $0, silently swallowing the first turn on
every new client.  Persistent crons tear the client down after every
run, so every cron turn recorded $0.

(V24 fixed the inverse bug — treating the cumulative as per-turn, a
massive over-count.  The delta logic introduced then caused this
under-count.  The engine now uses reset-aware deltas via
``nerve.db.usage.compute_turn_cost``; this migration repairs the rows
written while the clamp was live.)

Fix:
1. Recompute ``session_usage.cost_usd`` from token counts (same math as
   ``nerve.db.usage.estimate_turn_cost``, honoring the 5m/1h cache-write
   split when present) for rows that recorded zero cost despite nonzero
   token traffic.
2. Add each session's recovered amount to ``sessions.total_cost_usd``.
   NOTE: deliberately additive, NOT a re-sum of ``session_usage`` —
   telemetry pruning (``prune_telemetry``) deletes old usage rows while
   keeping the session row, so a blanket re-sum would erase the pruned
   turns' legitimate accumulated cost.  The zero-cost target rows
   contributed exactly $0 to the stored totals, so adding their
   recomputed cost is exact.

Idempotent: rerunning finds no remaining zero-cost rows with token
traffic (rows whose recompute legitimately rounds to zero recompute to
the same zero and contribute a zero delta).
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

# Model pricing (per 1M tokens, USD):
# (input, output, cache_read, cache_write_5m, cache_write_1h, web_search_per_req)
# Snapshot of nerve/db/usage.py MODEL_PRICING at migration time.
_PRICING: dict[str, tuple[float, float, float, float, float, float]] = {
    "fable-5":    (10,  50, 1.00, 12.50, 20.00, 0.01),
    "opus-4-8":   (5,   25, 0.50,  6.25, 10.00, 0.01),
    "opus-4-7":   (5,   25, 0.50,  6.25, 10.00, 0.01),
    "opus-4-6":   (5,   25, 0.50,  6.25, 10.00, 0.01),
    "opus-4-5":   (5,   25, 0.50,  6.25, 10.00, 0.01),
    "opus-4-1":   (15,  75, 1.50, 18.75, 30.00, 0.01),
    "opus-4":     (15,  75, 1.50, 18.75, 30.00, 0.01),
    "sonnet-4":   (3,   15, 0.30,  3.75,  6.00, 0.01),
    "haiku-4-5":  (1,    5, 0.10,  1.25,  2.00, 0.01),
    "haiku-3-5":  (0.8,  4, 0.08,  1.00,  1.60, 0.01),
}
_DEFAULT = (5, 25, 0.50, 6.25, 10.00, 0.01)  # Opus 4.x standard fallback

# A row is a backfill target when it recorded no cost despite real
# token traffic.
_TARGET_WHERE = (
    "COALESCE(cost_usd, 0) = 0 "
    "AND COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0) "
    "  + COALESCE(cache_read_input_tokens, 0) "
    "  + COALESCE(cache_creation_input_tokens, 0) > 0"
)


def _pricing_for(model: str | None) -> tuple[float, float, float, float, float, float]:
    if not model:
        return _DEFAULT
    m = model.lower()
    for key, p in _PRICING.items():
        if key in m:
            return p
    return _DEFAULT


async def up(db: aiosqlite.Connection) -> None:
    # -- Step 0: snapshot the target rows -----------------------------------
    # After the UPDATE the repaired rows are indistinguishable from rows
    # that always had a cost, so capture their rowids (and sessions) first.
    await db.execute("DROP TABLE IF EXISTS _v036_targets")
    await db.execute(
        f"""
        CREATE TEMP TABLE _v036_targets AS
        SELECT rowid AS rid, session_id
        FROM session_usage
        WHERE {_TARGET_WHERE}
        """
    )
    async with db.execute("SELECT COUNT(*) FROM _v036_targets") as cur:
        target_count = (await cur.fetchone())[0]

    if not target_count:
        await db.execute("DROP TABLE IF EXISTS _v036_targets")
        await db.commit()
        logger.info("V36 migration: no zero-cost turns with token traffic — nothing to backfill")
        return

    # -- Step 1: recompute cost_usd from token counts, per model ------------
    # SQLite can't substring-match models in a CASE, so resolve pricing
    # per distinct model and bulk-update (the v024 pattern).  Cache
    # writes honor the 5m/1h TTL split when recorded; legacy rows
    # without the split bill the aggregate at the 5m rate — identical
    # to estimate_turn_cost.
    async with db.execute(
        "SELECT DISTINCT model FROM session_usage "
        "WHERE rowid IN (SELECT rid FROM _v036_targets)"
    ) as cur:
        models = [row[0] for row in await cur.fetchall()]

    for model in models:
        p_in, p_out, p_cr, p_c5m, p_c1h, p_ws = _pricing_for(model)
        if model is None:
            model_where = "model IS NULL"
            params: tuple = ()
        else:
            model_where = "model = ?"
            params = (model,)

        await db.execute(
            f"""
            UPDATE session_usage
            SET cost_usd = ROUND(
                COALESCE(input_tokens, 0)            * {p_in}  / 1000000.0
              + COALESCE(output_tokens, 0)           * {p_out} / 1000000.0
              + COALESCE(cache_read_input_tokens, 0) * {p_cr}  / 1000000.0
              + CASE
                  WHEN COALESCE(cache_creation_5m_input_tokens, 0)
                     + COALESCE(cache_creation_1h_input_tokens, 0) > 0
                  THEN COALESCE(cache_creation_5m_input_tokens, 0) * {p_c5m} / 1000000.0
                     + COALESCE(cache_creation_1h_input_tokens, 0) * {p_c1h} / 1000000.0
                  ELSE COALESCE(cache_creation_input_tokens, 0) * {p_c5m} / 1000000.0
                END
              + COALESCE(web_search_requests, 0) * {p_ws},
              6
            )
            WHERE rowid IN (SELECT rid FROM _v036_targets) AND {model_where}
            """,
            params,
        )

    # -- Step 2: add the recovered cost to sessions.total_cost_usd ----------
    # The target rows contributed $0 to the stored session totals, so the
    # recovered per-session sum is exactly the correction to add.
    async with db.execute(
        """
        SELECT t.session_id, COALESCE(SUM(u.cost_usd), 0) AS recovered
        FROM _v036_targets t
        JOIN session_usage u ON u.rowid = t.rid
        GROUP BY t.session_id
        HAVING recovered > 0
        """
    ) as cur:
        per_session = await cur.fetchall()

    total_recovered = 0.0
    for session_id, recovered in per_session:
        total_recovered += recovered
        await db.execute(
            "UPDATE sessions "
            "SET total_cost_usd = ROUND(COALESCE(total_cost_usd, 0) + ?, 6) "
            "WHERE id = ?",
            (recovered, session_id),
        )

    await db.execute("DROP TABLE IF EXISTS _v036_targets")
    await db.commit()
    logger.info(
        "V36 migration: backfilled %d zero-cost turns from token counts "
        "(%.2f USD recovered across %d sessions)",
        target_count, total_recovered, len(per_session),
    )
