"""Codex (OpenAI) turn pricing.

The app-server reports token usage but never a USD figure, so cost is
computed here from the config-driven price table
(``codex.pricing: {model_substring: {input, cached_input, output}}``,
$/1M tokens).

Semantics: OpenAI's ``inputTokens`` INCLUDES ``cachedInputTokens`` (the
cached subset bills at the discounted rate). The backend normalizes that
into nerve's Anthropic-style split — ``input_tokens`` = full-price
tokens only, ``cache_read_tokens`` = the cached subset — *before* this
module runs, so:

    cost = input*in + cache_read*cached_in + output*out

Unknown model → ``None``, never estimated: a wrong cost is worse than a
missing one (the token counts are still recorded).
"""

from __future__ import annotations

from nerve.agent.backends.events import NormalizedUsage


def match_pricing(
    model: str | None, table: dict[str, dict[str, float]],
) -> dict[str, float] | None:
    """Longest-substring match of *model* against the price table keys.

    Mirrors the matching style of ``nerve.db.usage.MODEL_PRICING`` so
    dated/suffixed aliases resolve to their family entry.
    """
    if not model or not table:
        return None
    m = model.lower()
    best_key = ""
    for key in table:
        k = key.lower()
        if k and k in m and len(k) > len(best_key):
            best_key = key
    return table.get(best_key) if best_key else None


def compute_cost(
    model: str | None,
    usage: NormalizedUsage | None,
    table: dict[str, dict[str, float]],
) -> float | None:
    """Per-turn USD cost, or ``None`` when the model has no table entry."""
    if usage is None:
        return None
    prices = match_pricing(model, table)
    if prices is None:
        return None
    per_m = 1_000_000
    in_rate = float(prices.get("input") or 0.0)
    cached_rate = float(prices.get("cached_input") or 0.0)
    out_rate = float(prices.get("output") or 0.0)
    return (
        usage.input_tokens * in_rate
        + usage.cache_read_tokens * cached_rate
        + usage.output_tokens * out_rate
    ) / per_m
