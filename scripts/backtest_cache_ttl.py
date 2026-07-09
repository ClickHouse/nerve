#!/usr/bin/env python3
"""Backtest prompt-cache TTL policies against historical session_usage data.

Simulates three cache-TTL policies over real per-turn token traffic and
reports the weekly USD delta of each vs the status quo:

- ``5m``   — status quo: every cache write uses the default 5-minute TTL.
- ``1h``   — every session requests the 1-hour TTL (writes 2.0x base
  input instead of 1.25x, but turns arriving within an hour of the
  previous turn read the prefix at 0.1x instead of re-writing it).
- ``auto`` — per-session: 1h iff the session's median turn gap falls in
  (5min, 1h] (sparse cadence that actually benefits), else 5m.

Simulation model (per session, turns ordered by created_at):

The observed data was produced under the 5m policy, so the observed
``cache_creation`` of a turn whose gap from the previous turn exceeds
5 minutes is (mostly) a *full re-write* of the accumulated prefix.
Under a 1h TTL that prefix would still be warm, so the re-written
portion converts into cache reads.  The warm-prefix size after turn i
is estimated with a running counter ``C``:

    C = creation[i]              if turn i looked cold (gap > 5m / first
                                 turn / model switch)
    C = C + creation[i]          if turn i looked warm (gap <= 5m)

(``cache_read`` sums the prefix once per agentic-loop iteration inside a
run, so it multi-counts and is NOT a usable prefix estimate; creation
tokens are written once and are.)

For a turn with gap in (5min, 1h] the 1h simulation converts
``min(creation[i], C_prev)`` write-tokens into read-tokens.  Gaps > 1h
are cold under both policies (and in Nerve the SDK client is recycled at
the 60-min idle sweep, changing the system-prompt bytes anyway, so >1h
continuity would not materialize even if the TTL allowed it).

Output-token and web-search costs are identical across policies and are
excluded everywhere.

Usage:
    python3 scripts/backtest_cache_ttl.py [--db ~/.nerve/nerve.db] \
        [--days 28] [--ttl-threshold 3600] [--top 20]
"""

from __future__ import annotations

import argparse
import sqlite3
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path

# Reuse the real pricing table — no invented prices.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from nerve.db.usage import _get_pricing  # noqa: E402

FIVE_MIN = 300.0
ONE_HOUR = 3600.0


@dataclass
class Turn:
    """One session_usage row (input side only)."""

    ts: float  # unix epoch seconds
    model: str | None
    input_tokens: int
    cache_creation: int
    cache_read: int


@dataclass
class SimResult:
    cost_5m: float
    cost_1h: float
    converted_tokens: int  # write-tokens that became reads under 1h


def _input_cost(
    pricing: tuple, fresh: int, reads: int, w5m: int, w1h: int,
) -> float:
    p_in, _p_out, p_read, p_c5m, p_c1h, _p_ws = pricing
    return (
        fresh * p_in + reads * p_read + w5m * p_c5m + w1h * p_c1h
    ) / 1_000_000


def simulate_session(
    turns: list[Turn], ttl_threshold: float = ONE_HOUR,
) -> SimResult:
    """Simulate 5m (status quo) and 1h policies over one session's turns.

    Returns input-side costs only (output/web-search excluded — identical
    across policies).
    """
    cost_5m = 0.0
    cost_1h = 0.0
    converted_total = 0
    warm_prefix = 0  # C: estimated cached-prefix tokens after prev turn
    prev_ts: float | None = None
    prev_model: str | None = None

    for t in turns:
        pricing = _get_pricing(t.model)
        gap = None if prev_ts is None else t.ts - prev_ts
        model_switch = prev_model is not None and t.model != prev_model

        # --- status quo: everything observed was a 5m write
        cost_5m += _input_cost(
            pricing, t.input_tokens, t.cache_read, t.cache_creation, 0,
        )

        # --- 1h policy counterfactual
        if gap is None or model_switch or gap > ttl_threshold:
            # Cold under 1h too: same token flows, writes at the 1h rate.
            converted = 0
        elif gap <= FIVE_MIN:
            # Warm under both policies: same token flows.
            converted = 0
        else:
            # Cold observed (5m expired) but warm under 1h: the re-written
            # prefix converts into reads.
            converted = min(t.cache_creation, warm_prefix)
        cost_1h += _input_cost(
            pricing,
            t.input_tokens,
            t.cache_read + converted,
            0,
            t.cache_creation - converted,
        )
        converted_total += converted

        # --- update the observed-world warm-prefix estimate
        if gap is None or model_switch or gap > FIVE_MIN:
            warm_prefix = t.cache_creation  # full re-write observed
        else:
            warm_prefix += t.cache_creation  # incremental suffix write

        prev_ts = t.ts
        prev_model = t.model

    return SimResult(cost_5m, cost_1h, converted_total)


def auto_policy_for_session(
    turns: list[Turn], ttl_threshold: float = ONE_HOUR,
) -> str:
    """Cadence heuristic: 1h iff the median turn gap is in (5min, ttl].

    Sessions with <2 turns (no gaps) stay on 5m — a lone write at 2x can
    only lose money.  Median gaps > ttl also stay on 5m: the prefix dies
    before the next turn either way, so the 2x write premium buys nothing.
    """
    if len(turns) < 2:
        return "5m"
    gaps = [b.ts - a.ts for a, b in zip(turns, turns[1:])]
    med = statistics.median(gaps)
    return "1h" if FIVE_MIN < med <= ttl_threshold else "5m"


def load_sessions(
    db_path: str, days: int,
) -> dict[str, tuple[str, list[Turn]]]:
    """Load per-turn usage rows grouped by session: {sid: (source, turns)}."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT u.session_id, COALESCE(s.source, 'unknown'),
                   CAST(strftime('%s', u.created_at) AS REAL),
                   u.model, u.input_tokens,
                   u.cache_creation_input_tokens, u.cache_read_input_tokens
            FROM session_usage u
            LEFT JOIN sessions s ON s.id = u.session_id
            WHERE u.created_at >= DATETIME('now', ?)
            ORDER BY u.session_id, u.created_at, u.id
            """,
            (f"-{days} days",),
        ).fetchall()
    finally:
        conn.close()

    sessions: dict[str, tuple[str, list[Turn]]] = {}
    for sid, source, ts, model, inp, cc, cr in rows:
        if ts is None:
            continue
        entry = sessions.setdefault(sid, (source, []))
        entry[1].append(Turn(ts, model, inp or 0, cc or 0, cr or 0))
    return sessions


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", default=str(Path.home() / ".nerve" / "nerve.db"))
    ap.add_argument("--days", type=int, default=28)
    ap.add_argument("--ttl-threshold", type=float, default=ONE_HOUR,
                    help="Effective 1h-hit window in seconds (default 3600)")
    ap.add_argument("--top", type=int, default=20)
    args = ap.parse_args()

    sessions = load_sessions(args.db, args.days)
    if not sessions:
        print("No usage rows in window.")
        return

    weeks = args.days / 7.0
    per_source: dict[str, dict[str, float]] = {}
    per_session: list[tuple[float, str, str, int, str]] = []
    tot = {"5m": 0.0, "1h": 0.0, "auto": 0.0, "converted": 0}
    losers_1h = 0.0

    for sid, (source, turns) in sessions.items():
        sim = simulate_session(turns, args.ttl_threshold)
        policy = auto_policy_for_session(turns, args.ttl_threshold)
        cost_auto = sim.cost_1h if policy == "1h" else sim.cost_5m

        agg = per_source.setdefault(
            source, {"5m": 0.0, "1h": 0.0, "auto": 0.0, "sessions": 0,
                     "auto_1h_sessions": 0},
        )
        agg["5m"] += sim.cost_5m
        agg["1h"] += sim.cost_1h
        agg["auto"] += cost_auto
        agg["sessions"] += 1
        agg["auto_1h_sessions"] += policy == "1h"

        tot["5m"] += sim.cost_5m
        tot["1h"] += sim.cost_1h
        tot["auto"] += cost_auto
        tot["converted"] += sim.converted_tokens
        if sim.cost_1h > sim.cost_5m:
            losers_1h += sim.cost_1h - sim.cost_5m

        per_session.append(
            (sim.cost_5m - cost_auto, sid, source, len(turns), policy),
        )

    print(f"Backtest window: {args.days} days "
          f"({len(sessions)} sessions, "
          f"{sum(len(t) for _, t in sessions.values())} turns)\n")

    hdr = (f"{'source':<10} {'sess':>5} {'auto=1h':>7} "
           f"{'5m $/wk':>9} {'1h $/wk':>9} {'auto $/wk':>9} "
           f"{'Δ1h/wk':>8} {'Δauto/wk':>9}")
    print(hdr)
    print("-" * len(hdr))
    for source, a in sorted(per_source.items(), key=lambda kv: -kv[1]["5m"]):
        print(f"{source:<10} {a['sessions']:>5} {a['auto_1h_sessions']:>7} "
              f"{a['5m'] / weeks:>9.2f} {a['1h'] / weeks:>9.2f} "
              f"{a['auto'] / weeks:>9.2f} "
              f"{(a['5m'] - a['1h']) / weeks:>+8.2f} "
              f"{(a['5m'] - a['auto']) / weeks:>+9.2f}")
    print("-" * len(hdr))
    print(f"{'TOTAL':<10} {len(sessions):>5} {'':>7} "
          f"{tot['5m'] / weeks:>9.2f} {tot['1h'] / weeks:>9.2f} "
          f"{tot['auto'] / weeks:>9.2f} "
          f"{(tot['5m'] - tot['1h']) / weeks:>+8.2f} "
          f"{(tot['5m'] - tot['auto']) / weeks:>+9.2f}")

    print(f"\nWrite-tokens converted to reads under 1h: "
          f"{tot['converted'] / 1e6:.1f}M over {args.days}d")
    print(f"Blanket-1h losses on dense/one-shot sessions: "
          f"${losers_1h / weeks:.2f}/wk (avoided by auto)")

    print(f"\nTop {args.top} sessions by auto-policy savings (window total):")
    per_session.sort(reverse=True)
    for delta, sid, source, n, policy in per_session[: args.top]:
        print(f"  {delta:>+8.2f} USD  {sid:<44} {source:<9} "
              f"turns={n:<4} auto={policy}")

    gate = (tot["5m"] - tot["auto"]) / weeks
    print(f"\nGate: auto saves ${gate:.2f}/week "
          f"({'PASS ≥ $25' if gate >= 25 else 'FAIL < $25'})")


if __name__ == "__main__":
    main()
