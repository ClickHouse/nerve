"""Tests for nerve.agent.cache_policy — cadence-aware prompt-cache TTL."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from nerve.agent.backends.claude import ClaudeBackend
from nerve.agent.cache_policy import (
    build_ttl_report,
    cache_ttl_env,
    estimate_live_ttl_delta,
    resolve_cache_ttl,
)
from nerve.config import AgentConfig

# ---------------------------------------------------------------------------
# The backtest simulator (scripts/) shares the gap→conversion model; import
# it from the script file so the hand-computed cases below pin both.
# ---------------------------------------------------------------------------
_spec = importlib.util.spec_from_file_location(
    "backtest_cache_ttl",
    Path(__file__).resolve().parent.parent / "scripts" / "backtest_cache_ttl.py",
)
backtest = importlib.util.module_from_spec(_spec)
import sys  # noqa: E402

sys.modules["backtest_cache_ttl"] = backtest  # dataclasses need the module registered
_spec.loader.exec_module(backtest)


def _cfg(**kw) -> AgentConfig:
    return AgentConfig(**kw)


def _resolve(agent_cfg, *, source="web", model="claude-fable-5",
             meta=None, gaps=None, is_claude=True, gaps_raise=False):
    """Run resolve_cache_ttl with the cadence query stubbed out."""
    import asyncio

    mock = AsyncMock(return_value=gaps or [])
    if gaps_raise:
        mock.side_effect = RuntimeError("db down")
    with patch("nerve.agent.cache_policy.get_recent_turn_gaps", mock):
        return asyncio.run(
            resolve_cache_ttl(
                agent_cfg, db=object(), session_id="s1", source=source,
                model=model, session_meta=meta, is_claude_model=is_claude,
            )
        )


# ---------------------------------------------------------------------------
# cache_ttl_env
# ---------------------------------------------------------------------------

def test_env_5m_is_empty():
    assert cache_ttl_env("5m") == {}
    assert cache_ttl_env("5m", is_bedrock=True) == {}


def test_env_1h_sets_cli_flag():
    assert cache_ttl_env("1h") == {"ENABLE_PROMPT_CACHING_1H": "1"}


def test_env_1h_bedrock_sets_both_flags():
    env = cache_ttl_env("1h", is_bedrock=True)
    assert env["ENABLE_PROMPT_CACHING_1H"] == "1"
    assert env["ENABLE_PROMPT_CACHING_1H_BEDROCK"] == "1"


# ---------------------------------------------------------------------------
# resolve_cache_ttl — mode / exclusion / prior matrix
# ---------------------------------------------------------------------------

class TestResolveModes:
    def test_default_config_is_5m(self):
        assert _cfg().cache_ttl == "5m"

    def test_mode_5m_never_requests(self):
        cfg = _cfg(cache_ttl="5m")
        for source in ("web", "cron", "wakeup", "telegram"):
            assert _resolve(cfg, source=source, gaps=[1800, 1800]) == "5m"

    def test_mode_1h_always_requests(self):
        cfg = _cfg(cache_ttl="1h")
        assert _resolve(cfg, source="web") == "1h"
        assert _resolve(cfg, source="cron") == "1h"

    def test_mode_1h_respects_model_exclusion(self):
        cfg = _cfg(cache_ttl="1h",
                   cache_ttl_excluded_models=["sonnet-4-6"])
        assert _resolve(cfg, model="claude-sonnet-4-6") == "5m"
        assert _resolve(cfg, model="claude-fable-5") == "1h"

    def test_auto_respects_model_exclusion(self):
        cfg = _cfg(cache_ttl="auto", cache_ttl_excluded_models=["haiku"])
        assert _resolve(cfg, model="claude-haiku-4-5",
                        gaps=[1800, 1800]) == "5m"

    def test_non_claude_model_never_requests(self):
        cfg = _cfg(cache_ttl="1h")
        assert _resolve(cfg, model="qwen3:32b", is_claude=False) == "5m"

    def test_invalid_mode_falls_back_to_5m(self):
        cfg = _cfg(cache_ttl="2h")
        assert _resolve(cfg, gaps=[1800, 1800]) == "5m"

    def test_metadata_override_beats_config(self):
        cfg = _cfg(cache_ttl="5m")
        assert _resolve(cfg, meta={"cache_ttl_override": "1h"}) == "1h"
        cfg = _cfg(cache_ttl="1h")
        assert _resolve(cfg, meta={"cache_ttl_override": "5m"}) == "5m"


class TestResolveAutoCadence:
    def test_sparse_median_gets_1h(self):
        cfg = _cfg(cache_ttl="auto")
        # 30-minute cadence — the canonical sparse session.
        assert _resolve(cfg, source="web", gaps=[1800, 1750, 1900]) == "1h"

    def test_dense_median_stays_5m_even_for_cron(self):
        cfg = _cfg(cache_ttl="auto")
        assert _resolve(
            cfg, source="cron", gaps=[30, 60, 45],
            meta={"cron_session_mode": "persistent"},
        ) == "5m"

    def test_median_beyond_ttl_stays_5m(self):
        # Gaps > 1h expire either way — the 2x write premium buys nothing.
        cfg = _cfg(cache_ttl="auto")
        assert _resolve(cfg, source="web", gaps=[7200, 8000]) == "5m"

    def test_no_history_web_defaults_5m(self):
        cfg = _cfg(cache_ttl="auto")
        assert _resolve(cfg, source="web") == "5m"
        assert _resolve(cfg, source="telegram") == "5m"

    def test_no_history_wakeup_defaults_1h(self):
        cfg = _cfg(cache_ttl="auto")
        assert _resolve(cfg, source="wakeup") == "1h"

    def test_no_history_persistent_cron_defaults_1h(self):
        cfg = _cfg(cache_ttl="auto")
        assert _resolve(
            cfg, source="cron", meta={"cron_session_mode": "persistent"},
        ) == "1h"

    def test_no_history_isolated_cron_defaults_5m(self):
        cfg = _cfg(cache_ttl="auto")
        assert _resolve(
            cfg, source="cron", meta={"cron_session_mode": "isolated"},
        ) == "5m"
        assert _resolve(cfg, source="cron") == "5m"

    def test_cadence_query_failure_falls_back_to_priors(self):
        cfg = _cfg(cache_ttl="auto")
        assert _resolve(cfg, source="wakeup", gaps_raise=True) == "1h"
        assert _resolve(cfg, source="web", gaps_raise=True) == "5m"


# ---------------------------------------------------------------------------
# Backend env wiring — the switch reaches the CLI subprocess iff resolved 1h
# ---------------------------------------------------------------------------

def _make_env_backend(is_bedrock: bool = False) -> ClaudeBackend:
    config = SimpleNamespace(
        provider=SimpleNamespace(
            is_bedrock=is_bedrock, aws_region="", aws_profile="",
            aws_access_key_id="", aws_secret_access_key="",
        ),
        proxy=SimpleNamespace(enabled=False, host="", port=0),
        effective_api_key="",
    )
    return ClaudeBackend(SimpleNamespace(config=config))


def test_build_env_5m_has_no_cache_flag():
    env = _make_env_backend()._build_env(cache_ttl="5m")
    assert "ENABLE_PROMPT_CACHING_1H" not in env
    assert "FORCE_PROMPT_CACHING_5M" not in env  # never force upstream off


def test_build_env_1h_sets_cache_flag():
    env = _make_env_backend()._build_env(cache_ttl="1h")
    assert env["ENABLE_PROMPT_CACHING_1H"] == "1"


def test_build_env_1h_bedrock_sets_bedrock_flag():
    env = _make_env_backend(is_bedrock=True)._build_env(cache_ttl="1h")
    assert env["ENABLE_PROMPT_CACHING_1H"] == "1"
    assert env["ENABLE_PROMPT_CACHING_1H_BEDROCK"] == "1"


def test_build_env_default_is_5m():
    env = _make_env_backend()._build_env()
    assert "ENABLE_PROMPT_CACHING_1H" not in env


# ---------------------------------------------------------------------------
# Backtest simulator — hand-computed costs (fable pricing: input 10,
# read 1.0, write5m 12.5, write1h 20 per 1M tokens)
# ---------------------------------------------------------------------------

FABLE = "claude-fable-5"


def _turn(ts, cc, cr=0, inp=0):
    return backtest.Turn(ts=ts, model=FABLE, input_tokens=inp,
                         cache_creation=cc, cache_read=cr)


class TestBacktestSimulator:
    def test_sparse_session_converts_prefix(self):
        # t=0: cold write 100k. t=30min: observed cold re-write of 110k
        # (prefix 100k + 10k new); under 1h the 100k prefix reads instead.
        sim = backtest.simulate_session([
            _turn(0, 100_000), _turn(1800, 110_000),
        ])
        assert sim.cost_5m == pytest.approx(210_000 * 12.5 / 1e6)   # 2.625
        assert sim.cost_1h == pytest.approx(
            100_000 * 20 / 1e6            # turn 1 cold write @ 1h
            + 100_000 * 1.0 / 1e6         # converted prefix read
            + 10_000 * 20 / 1e6           # genuinely-new suffix write
        )                                  # = 2.3
        assert sim.converted_tokens == 100_000

    def test_dense_session_pays_write_premium(self):
        # 60s gap: warm under both policies; 1h only raises write price.
        sim = backtest.simulate_session([
            _turn(0, 100_000), _turn(60, 5_000, cr=100_000),
        ])
        assert sim.converted_tokens == 0
        assert sim.cost_5m == pytest.approx(
            105_000 * 12.5 / 1e6 + 100_000 * 1.0 / 1e6,
        )
        assert sim.cost_1h == pytest.approx(
            105_000 * 20 / 1e6 + 100_000 * 1.0 / 1e6,
        )
        assert sim.cost_1h > sim.cost_5m

    def test_gap_beyond_ttl_is_cold_for_both(self):
        sim = backtest.simulate_session([
            _turn(0, 100_000), _turn(7200, 120_000),
        ])
        assert sim.converted_tokens == 0
        assert sim.cost_5m == pytest.approx(220_000 * 12.5 / 1e6)
        assert sim.cost_1h == pytest.approx(220_000 * 20 / 1e6)

    def test_model_switch_breaks_conversion(self):
        turns = [_turn(0, 100_000), _turn(1800, 110_000)]
        turns[1].model = "claude-haiku-4-5"
        sim = backtest.simulate_session(turns)
        assert sim.converted_tokens == 0

    def test_conversion_capped_by_warm_prefix(self):
        # Second write smaller than tracked prefix (compaction): convert
        # only what was actually re-written.
        sim = backtest.simulate_session([
            _turn(0, 100_000), _turn(1800, 40_000),
        ])
        assert sim.converted_tokens == 40_000

    def test_auto_policy_thresholds(self):
        pol = backtest.auto_policy_for_session
        assert pol([_turn(0, 1)]) == "5m"                       # no gaps
        assert pol([_turn(0, 1), _turn(60, 1)]) == "5m"         # dense
        assert pol([_turn(0, 1), _turn(1800, 1)]) == "1h"       # sparse
        assert pol([_turn(0, 1), _turn(7200, 1)]) == "5m"       # beyond TTL


# ---------------------------------------------------------------------------
# Live counterfactual (diagnostics guardrail input)
# ---------------------------------------------------------------------------

class TestLiveTtlDelta:
    def test_1h_traffic_savings_hand_computed(self):
        # (ts, model, input, reads, w5m, w1h)
        rows = [
            (0, FABLE, 0, 0, 0, 100_000),
            (1800, FABLE, 0, 100_000, 0, 10_000),
        ]
        est = estimate_live_ttl_delta(rows)
        assert est["actual"] == pytest.approx(
            100_000 * 20 / 1e6 + 100_000 * 1.0 / 1e6 + 10_000 * 20 / 1e6,
        )  # 2.3
        assert est["baseline_5m"] == pytest.approx(
            100_000 * 12.5 / 1e6            # turn 1 write @ 5m
            + 100_000 * 12.5 / 1e6          # converted read → 5m re-write
            + 10_000 * 12.5 / 1e6           # suffix write @ 5m
        )  # 2.625
        assert est["savings"] == pytest.approx(0.325)

    def test_pure_5m_traffic_has_zero_delta(self):
        rows = [
            (0, FABLE, 0, 0, 100_000, 0),
            (1800, FABLE, 0, 50_000, 120_000, 0),
        ]
        est = estimate_live_ttl_delta(rows)
        assert est["savings"] == pytest.approx(0.0)

    def test_report_flags_regression(self):
        # A lone 1h write with no follow-up costs the 2x premium for
        # nothing → negative savings → guardrail flags the source.
        rows = [
            ("s1", "cron", 0.0, FABLE, 0, 0, 0, 1_000_000),
        ]
        report = build_ttl_report(rows)
        assert report["by_source"]["cron"]["savings"] < 0
        assert "cron" in report["regressions"]

    def test_report_aggregates_by_source(self):
        rows = [
            ("s1", "web", 0.0, FABLE, 0, 0, 0, 100_000),
            ("s1", "web", 1800.0, FABLE, 0, 100_000, 0, 10_000),
            ("s2", "cron", 0.0, FABLE, 0, 0, 50_000, 0),
        ]
        report = build_ttl_report(rows)
        assert report["by_source"]["web"]["savings"] == pytest.approx(0.325)
        assert report["by_source"]["cron"]["savings"] == pytest.approx(0.0)
        assert report["total"]["savings"] == pytest.approx(0.325)
        assert report["regressions"] == []
