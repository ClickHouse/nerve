"""Tests for engine option helpers — OAuth-conditional thinking/effort cap.

Regression for the issue where every cron run failed with
``API Error: 400 level "max" not supported, valid levels: low, medium, high``
because the global ``effort=max`` / ``thinking=max`` settings were applied
to cron sessions running on ``cron_model`` (Sonnet) under Claude OAuth,
which caps non-flagship models at ``high``.

The fix downgrades ``thinking`` and ``effort`` to ``high`` for cron and
hook sessions **only when OAuth is in use** (``config.proxy.enabled``).
API users keep ``max`` for every session, and interactive sessions
(web/Telegram/Discord/...) keep ``max`` even under OAuth — only the
narrow OAuth+cron path is touched.
"""

from __future__ import annotations

import pytest

from nerve.agent.engine import _select_thinking_effort
from nerve.config import AgentConfig, NerveConfig, ProxyConfig


def _make_config(
    *,
    thinking: str = "max",
    effort: str = "max",
    proxy_enabled: bool = False,
) -> NerveConfig:
    """Build a minimal NerveConfig for testing _select_thinking_effort.

    Only the ``agent`` and ``proxy`` fields are read by the helper; the
    rest can stay at their defaults.
    """
    return NerveConfig(
        agent=AgentConfig(thinking=thinking, effort=effort),
        proxy=ProxyConfig(enabled=proxy_enabled),
    )


class TestSelectThinkingEffort:
    """``_select_thinking_effort`` downgrades only when OAuth + cron/hook."""

    # ------------------------------------------------------------------ #
    #  OAuth on, cron-like source: must downgrade max -> high            #
    # ------------------------------------------------------------------ #

    @pytest.mark.parametrize("source", ["cron", "hook"])
    def test_oauth_caps_cron_max_to_high(self, source: str):
        """The exact bug being fixed: OAuth + cron + max -> 'high'."""
        config = _make_config(thinking="max", effort="max", proxy_enabled=True)
        assert _select_thinking_effort(config, source) == ("high", "high")

    @pytest.mark.parametrize("source", ["cron", "hook"])
    def test_oauth_does_not_upgrade_lower_values(self, source: str):
        """The cap is a max-only cap, not a forced value. Lower settings pass through."""
        config = _make_config(
            thinking="medium", effort="low", proxy_enabled=True,
        )
        assert _select_thinking_effort(config, source) == ("medium", "low")

    @pytest.mark.parametrize("source", ["cron", "hook"])
    def test_oauth_caps_individually(self, source: str):
        """Each knob is capped independently if it's at 'max'."""
        config = _make_config(
            thinking="max", effort="medium", proxy_enabled=True,
        )
        assert _select_thinking_effort(config, source) == ("high", "medium")

        config = _make_config(
            thinking="medium", effort="max", proxy_enabled=True,
        )
        assert _select_thinking_effort(config, source) == ("medium", "high")

    # ------------------------------------------------------------------ #
    #  OAuth on, interactive source: must NOT downgrade                  #
    # ------------------------------------------------------------------ #

    @pytest.mark.parametrize("source", ["web", "telegram", "discord", "api", ""])
    def test_oauth_does_not_downgrade_interactive_sources(self, source: str):
        """Interactive sessions run on agent.model (Opus by default) which
        accepts max under OAuth. Don't touch them."""
        config = _make_config(thinking="max", effort="max", proxy_enabled=True)
        assert _select_thinking_effort(config, source) == ("max", "max")

    # ------------------------------------------------------------------ #
    #  OAuth off (API key): never downgrade — Artem's requirement        #
    # ------------------------------------------------------------------ #

    @pytest.mark.parametrize("source", ["cron", "hook", "web", "telegram", ""])
    def test_api_users_never_downgraded(self, source: str):
        """Without the local proxy (i.e. user has a real Anthropic API key),
        every source keeps the configured value. This is exactly what Artem
        asked for on ClickHouse/nerve#129 — don't change behavior for API
        users."""
        config = _make_config(thinking="max", effort="max", proxy_enabled=False)
        assert _select_thinking_effort(config, source) == ("max", "max")

    @pytest.mark.parametrize("source", ["cron", "hook"])
    def test_api_users_keep_custom_values_for_cron(self, source: str):
        """API users can pick any value for cron sessions too — no special
        treatment."""
        config = _make_config(
            thinking="medium", effort="low", proxy_enabled=False,
        )
        assert _select_thinking_effort(config, source) == ("medium", "low")

    # ------------------------------------------------------------------ #
    #  Defaults sanity check                                             #
    # ------------------------------------------------------------------ #

    def test_defaults_match_documented_values(self):
        """Defaults: max/max for everyone; no special cron knobs anymore."""
        cfg = AgentConfig()
        assert cfg.thinking == "max"
        assert cfg.effort == "max"
        # The cron_thinking / cron_effort knobs were removed — keep them
        # gone so we don't reintroduce the unconditional downgrade.
        assert not hasattr(cfg, "cron_thinking")
        assert not hasattr(cfg, "cron_effort")


class TestAgentConfigFromDict:
    """``AgentConfig.from_dict`` no longer reads cron_thinking/cron_effort."""

    def test_from_dict_ignores_legacy_cron_keys(self):
        """Old configs with cron_thinking/cron_effort load cleanly — the
        keys are silently ignored. Backwards-compatible: nothing crashes,
        the keys just don't do anything anymore (their behavior moved to
        the engine's OAuth check)."""
        cfg = AgentConfig.from_dict({
            "thinking": "max",
            "effort": "max",
            "cron_thinking": "high",   # legacy, ignored
            "cron_effort": "high",     # legacy, ignored
        })
        assert cfg.thinking == "max"
        assert cfg.effort == "max"

    def test_from_dict_empty_uses_defaults(self):
        cfg = AgentConfig.from_dict({})
        assert cfg.thinking == "max"
        assert cfg.effort == "max"
