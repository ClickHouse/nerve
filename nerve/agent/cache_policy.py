"""Cadence-aware prompt-cache TTL policy.

Anthropic prompt caching has two write TTLs: 5 minutes (1.25x base input)
and 1 hour (2.0x base input); reads are 0.1x. A cache write is therefore
~12.5x the price of a read of the same tokens, so sessions whose turn
cadence exceeds 5 minutes by design (persistent crons, ScheduleWakeup
monitoring loops, spaced web conversations) re-buy their entire context
on every turn under the default TTL.

This module decides, per SDK-client build, which TTL a session should
request. The Claude Code CLI has native support via env vars:

- ``ENABLE_PROMPT_CACHING_1H=1`` — request the 1h TTL (API-key auth;
  the CLI adds the ``extended-cache-ttl-2025-04-11`` beta itself).
- ``ENABLE_PROMPT_CACHING_1H_BEDROCK=1`` — same, for Bedrock.
- ``FORCE_PROMPT_CACHING_5M=1`` — upstream kill switch (not set here).

Policy modes (``agent.cache_ttl`` in config, or a per-session override
in session metadata):

- ``"5m"``   — status quo, never request the beta.
- ``"1h"``   — always request it (minus excluded models).
- ``"auto"`` — per session at client-build time:
    1. observed cadence wins: median of the session's recent turn gaps
       in (5min, 1h] → 1h; any other observed cadence → 5m (gaps beyond
       the TTL expire either way, so the 2x write premium buys nothing);
    2. no history: wakeup-driven turns and persistent-mode cron sessions
       → 1h (the canonical sparse-cadence cases), everything else → 5m.

The 1h TTL only pays off if the prompt bytes are identical across turns
— which in Nerve holds *within* an SDK-client lifetime, and across
client rebuilds only if the system prompt is byte-stable (see the
``Current date`` + frozen-recall changes in prompts.py / engine.py).
"""

from __future__ import annotations

import logging
import statistics
from typing import Any, Iterable

from nerve.db.usage import _get_pricing

logger = logging.getLogger(__name__)

FIVE_MIN_S = 300.0
ONE_HOUR_S = 3600.0

# How many recent turn gaps to consider for the cadence heuristic.
CADENCE_WINDOW = 12

VALID_TTL_MODES = ("5m", "1h", "auto")


def cache_ttl_env(ttl: str, is_bedrock: bool = False) -> dict[str, str]:
    """Env vars for the CLI subprocess implementing the resolved TTL.

    ``"5m"`` returns an empty dict — the CLI default is already 5m and we
    deliberately do NOT set ``FORCE_PROMPT_CACHING_5M`` (that would also
    override a claude.ai-subscriber allowlist upstream).
    """
    if ttl != "1h":
        return {}
    env = {"ENABLE_PROMPT_CACHING_1H": "1"}
    if is_bedrock:
        env["ENABLE_PROMPT_CACHING_1H_BEDROCK"] = "1"
    return env


async def resolve_cache_ttl(
    agent_config: Any,
    db: Any,
    session_id: str,
    source: str,
    model: str | None,
    session_meta: dict | None = None,
    is_claude_model: bool = True,
) -> str:
    """Resolve the cache TTL ("5m" | "1h") for a session's next client.

    ``session_meta`` is the parsed session metadata dict; recognised keys:

    - ``cache_ttl_override`` — per-session mode override (e.g. from a
      cron job's ``cache_ttl`` in jobs.yaml). Same values as the config.
    - ``cron_session_mode`` — "persistent" | "isolated", written by the
      cron runners; used as the no-history prior for cron sessions.
    """
    if not is_claude_model:
        return "5m"  # Ollama/OpenAI-translated models have no Anthropic cache

    meta = session_meta or {}
    mode = meta.get("cache_ttl_override") or getattr(
        agent_config, "cache_ttl", "5m",
    )
    if mode not in VALID_TTL_MODES:
        logger.warning(
            "Unknown cache_ttl mode %r for session %s — falling back to 5m",
            mode, session_id,
        )
        return "5m"
    if mode == "5m":
        return "5m"

    # Model exclusion applies to both "1h" and "auto".
    resolved = (model or getattr(agent_config, "model", "") or "").lower()
    excluded = getattr(agent_config, "cache_ttl_excluded_models", []) or []
    if any(tok and tok.lower() in resolved for tok in excluded):
        return "5m"

    if mode == "1h":
        return "1h"

    # --- auto: observed cadence first, source priors on no data
    gaps: list[float] = []
    try:
        gaps = await get_recent_turn_gaps(db, session_id, CADENCE_WINDOW)
    except Exception as e:  # never fail a client build over the heuristic
        logger.warning(
            "cache_ttl cadence query failed for %s: %s", session_id, e,
        )

    if gaps:
        med = statistics.median(gaps)
        return "1h" if FIVE_MIN_S < med <= ONE_HOUR_S else "5m"

    if source == "wakeup":
        return "1h"
    if source == "cron" and meta.get("cron_session_mode") == "persistent":
        return "1h"
    return "5m"


async def get_recent_turn_gaps(
    db: Any, session_id: str, window: int = CADENCE_WINDOW,
) -> list[float]:
    """Seconds between the session's most recent turns (indexed query)."""
    async with db.db.execute(
        """
        SELECT CAST(strftime('%s', created_at) AS REAL)
        FROM session_usage WHERE session_id = ?
        ORDER BY id DESC LIMIT ?
        """,
        (session_id, window + 1),
    ) as cursor:
        ts = [row[0] async for row in cursor if row[0] is not None]
    ts.reverse()  # chronological
    return [b - a for a, b in zip(ts, ts[1:])]


# ---------------------------------------------------------------------------
# Live counterfactual: what would the observed traffic have cost on 5m?
# ---------------------------------------------------------------------------

def estimate_live_ttl_delta(
    turns: Iterable[tuple],
    ttl_threshold: float = ONE_HOUR_S,
) -> dict:
    """Estimate savings of observed (possibly 1h-cached) traffic vs a
    pure-5m baseline.

    ``turns`` are chronological rows for ONE session:
    ``(ts_epoch, model, input_tokens, cache_read, write_5m, write_1h)``.

    Model: a turn whose gap from the previous turn is in (5min, 1h] and
    whose predecessor wrote 1h-TTL cache benefited from the extended TTL
    — under 5m its first-iteration prefix read would have been a re-write
    at 1.25x. The warm-prefix size is tracked from creation tokens
    (reads multi-count across agentic-loop iterations, creations don't).
    Turns are also charged the 1h-vs-5m write premium they actually paid.

    Returns ``{"actual": $, "baseline_5m": $, "savings": $}`` where
    positive savings mean the 1h TTL is paying off.
    """
    actual = 0.0
    baseline = 0.0
    warm_prefix = 0
    prev_ts: float | None = None
    prev_model: str | None = None
    prev_wrote_1h = False

    for ts, model, inp, reads, w5m, w1h in turns:
        p_in, _o, p_read, p_c5m, p_c1h, _w = _get_pricing(model)
        creation = (w5m or 0) + (w1h or 0)
        gap = None if prev_ts is None else ts - prev_ts
        cold_boundary = (
            gap is None or model != prev_model or gap > ttl_threshold
        )

        actual += (
            inp * p_in + reads * p_read + w5m * p_c5m + w1h * p_c1h
        ) / 1_000_000

        # Baseline: same turn under a pure-5m policy.
        converted = 0
        if (
            not cold_boundary
            and gap is not None
            and gap > FIVE_MIN_S
            and prev_wrote_1h
        ):
            # This read survived only thanks to the 1h TTL; under 5m the
            # prefix would have been re-written once at the 5m rate.
            converted = min(reads, warm_prefix)
        baseline += (
            inp * p_in
            + (reads - converted) * p_read
            + converted * p_c5m
            + (w5m + w1h) * p_c5m
        ) / 1_000_000

        # Warm-prefix bookkeeping (observed world).
        if cold_boundary or (gap is not None and gap > FIVE_MIN_S and not prev_wrote_1h):
            warm_prefix = creation
        else:
            warm_prefix += creation
        prev_ts = ts
        prev_model = model
        prev_wrote_1h = w1h > 0

    return {
        "actual": round(actual, 4),
        "baseline_5m": round(baseline, 4),
        "savings": round(baseline - actual, 4),
    }


def build_ttl_report(rows: list[tuple]) -> dict:
    """Aggregate the live 1h-vs-5m estimate per source for diagnostics.

    ``rows`` come from ``UsageStore.get_cache_ttl_turn_rows``:
    ``(session_id, source, ts, model, input_tokens, reads, w5m, w1h)``,
    ordered by session then time.

    Guardrail: a source whose 1h traffic is estimated to cost *more* than
    the 5m baseline (the auto policy misclassified its cadence) lands in
    ``regressions`` and is logged at WARNING — manual revert is one
    config line (``agent.cache_ttl``) or a per-job ``cache_ttl: "5m"``.
    """
    by_session: dict[str, tuple[str, list[tuple]]] = {}
    for sid, source, ts, model, inp, reads, w5m, w1h in rows:
        if ts is None:
            continue
        by_session.setdefault(sid, (source, []))[1].append(
            (ts, model, inp or 0, reads or 0, w5m or 0, w1h or 0),
        )

    per_source: dict[str, dict] = {}
    total = {"actual": 0.0, "baseline_5m": 0.0, "savings": 0.0}
    for _sid, (source, turns) in by_session.items():
        est = estimate_live_ttl_delta(turns)
        agg = per_source.setdefault(
            source, {"actual": 0.0, "baseline_5m": 0.0, "savings": 0.0},
        )
        for k in agg:
            agg[k] = round(agg[k] + est[k], 4)
            total[k] = round(total[k] + est[k], 4)

    regressions = [
        src for src, agg in per_source.items() if agg["savings"] < -0.5
    ]
    for src in regressions:
        logger.warning(
            "cache_ttl guardrail: 1h caching for source %r cost "
            "$%.2f MORE than the 5m baseline over the window — the auto "
            "policy may be misclassifying its cadence (revert via "
            "agent.cache_ttl or a per-job cache_ttl override)",
            src, -per_source[src]["savings"],
        )

    return {
        "by_source": per_source,
        "total": total,
        "regressions": regressions,
    }
