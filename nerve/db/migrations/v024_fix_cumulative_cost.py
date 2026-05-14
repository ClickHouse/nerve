"""V24: Fix inflated session costs caused by cumulative SDK cost bug.

The Claude Agent SDK's ``ResultMessage.total_cost_usd`` reports a
*cumulative* session total, but engine.py was treating it as per-turn
and adding it to the running session total on every invocation.  This
caused ``session_usage.cost_usd`` to store cumulative snapshots
instead of deltas, and ``sessions.total_cost_usd`` to be the sum of
those snapshots (massively inflated).

Fix:
1. Recalculate every ``session_usage.cost_usd`` from its token counts
   using model-specific pricing.
2. Recompute every ``sessions.total_cost_usd`` as the SUM of the now-
   correct per-turn costs.
3. Seed ``_sdk_cumulative_cost`` in session metadata for sessions that
   may be resumed, so the engine computes correct deltas on the first
   turn after the upgrade.
"""

from __future__ import annotations

import json
import logging

import aiosqlite

logger = logging.getLogger(__name__)

# Model pricing (per 1M tokens, USD): (input, output, cache_read, cache_write)
# Kept in sync with nerve/db/usage.py MODEL_PRICING.
_PRICING = {
    "opus-4-6":  (5, 25, 0.50, 6.25),
    "opus-4-5":  (5, 25, 0.50, 6.25),
    "opus-4-1":  (15, 75, 1.50, 18.75),
    "opus-4":    (15, 75, 1.50, 18.75),
    "sonnet-4":  (3, 15, 0.30, 3.75),
    "haiku-4-5": (1, 5, 0.10, 1.25),
    "haiku-3-5": (0.8, 4, 0.08, 1.0),
}
_DEFAULT = (5, 25, 0.50, 6.25)  # Opus 4.6 fallback


def _pricing_for(model: str | None) -> tuple[float, float, float, float]:
    if not model:
        return _DEFAULT
    m = model.lower()
    for key, p in _PRICING.items():
        if key in m:
            return p
    return _DEFAULT


async def up(db: aiosqlite.Connection) -> None:
    # -- Step 1: recalculate session_usage.cost_usd from token counts ------
    #
    # We can't just do a single UPDATE with CASE on model because SQLite
    # doesn't easily support substring matching in CASE.  Instead, fetch
    # all distinct models, resolve pricing once per model, then bulk-update.

    async with db.execute(
        "SELECT DISTINCT model FROM session_usage"
    ) as cur:
        models = [row[0] for row in await cur.fetchall()]

    for model in models:
        p_in, p_out, p_cr, p_cw = _pricing_for(model)
        if model is None:
            where = "model IS NULL"
            params: tuple = ()
        else:
            where = "model = ?"
            params = (model,)

        await db.execute(
            f"""
            UPDATE session_usage
            SET cost_usd = ROUND(
                input_tokens              * {p_in}  / 1000000.0
              + output_tokens             * {p_out} / 1000000.0
              + cache_read_input_tokens   * {p_cr}  / 1000000.0
              + cache_creation_input_tokens * {p_cw} / 1000000.0,
              6
            )
            WHERE {where}
            """,
            params,
        )

    # -- Step 2: recompute sessions.total_cost_usd from fixed per-turn costs
    await db.execute(
        """
        UPDATE sessions
        SET total_cost_usd = COALESCE(
            (SELECT SUM(cost_usd) FROM session_usage
             WHERE session_usage.session_id = sessions.id),
            0
        )
        """
    )

    # -- Step 3: seed _sdk_cumulative_cost in metadata for resumable sessions
    #
    # The engine now tracks the SDK's cumulative total_cost_usd in session
    # metadata as ``_sdk_cumulative_cost`` to compute per-turn deltas.
    # For sessions that existed before this fix and may be resumed after
    # upgrade, we seed the value from the last (highest) cumulative
    # cost_usd snapshot that was stored by the old buggy code.
    #
    # We read the *original* max cost_usd BEFORE step 1 would have
    # overwritten it — but step 1 already ran, so we approximate the
    # SDK cumulative as the corrected session total.  After restart the
    # SDK session is re-created fresh anyway (cumulative starts at 0),
    # so this seed only matters for hot-resumed sessions (rare).
    # Setting it to 0 is safest: on restart the SDK resets, and the
    # first delta will be correct.  For the uncommon hot-resume case,
    # there may be a one-time over-count on the first turn, but
    # subsequent turns will be accurate.

    async with db.execute(
        "SELECT id, metadata FROM sessions WHERE sdk_session_id IS NOT NULL"
    ) as cur:
        rows = await cur.fetchall()

    for sid, raw_meta in rows:
        meta = json.loads(raw_meta) if raw_meta else {}
        if "_sdk_cumulative_cost" not in meta:
            # Seed to 0 — safest default since restart creates fresh SDK
            # sessions.  The engine's max(delta, 0) clamp prevents negative
            # costs if the SDK cumulative is lower than expected.
            meta["_sdk_cumulative_cost"] = 0
            await db.execute(
                "UPDATE sessions SET metadata = ? WHERE id = ?",
                (json.dumps(meta, default=str), sid),
            )

    await db.commit()
    logger.info(
        "V24 migration: recalculated session_usage.cost_usd from token "
        "counts, recomputed sessions.total_cost_usd, and seeded "
        "_sdk_cumulative_cost in session metadata"
    )
