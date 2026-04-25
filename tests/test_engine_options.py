"""Tests for engine option helpers — thinking/effort selection per source.

Regression for the issue where every cron run failed with
``API Error: 400 level "max" not supported, valid levels: low, medium, high``
because the global ``effort=max`` / ``thinking=max`` settings were applied
to cron sessions that run on ``cron_model`` (Sonnet) under Claude OAuth,
which caps non-flagship models at ``high``.

The fix introduces dedicated ``agent.cron_thinking`` / ``agent.cron_effort``
fields and a ``_select_thinking_effort`` helper that picks the right pair
based on ``source`` (``cron`` / ``hook`` get the cron overrides, everything
else keeps the main settings).
"""

from __future__ import annotations

import pytest

from nerve.agent.engine import _select_thinking_effort
from nerve.config import AgentConfig


@pytest.fixture
def agent_config() -> AgentConfig:
    """Default AgentConfig — main = max, cron = high."""
    return AgentConfig()


class TestSelectThinkingEffort:
    """``_select_thinking_effort`` routes settings by session source."""

    def test_defaults_match_documented_values(self, agent_config: AgentConfig):
        """Sanity check: the defaults are the ones the docs promise."""
        assert agent_config.thinking == "max"
        assert agent_config.effort == "max"
        assert agent_config.cron_thinking == "high"
        assert agent_config.cron_effort == "high"

    @pytest.mark.parametrize("source", ["web", "telegram", "discord", "api", ""])
    def test_interactive_sources_use_main_settings(
        self, agent_config: AgentConfig, source: str,
    ):
        """Anything that isn't cron/hook keeps the main thinking/effort."""
        thinking, effort = _select_thinking_effort(agent_config, source)
        assert thinking == "max"
        assert effort == "max"

    @pytest.mark.parametrize("source", ["cron", "hook"])
    def test_cron_and_hook_use_cron_overrides(
        self, agent_config: AgentConfig, source: str,
    ):
        """Cron and hook sessions must use ``cron_thinking`` / ``cron_effort``.

        This is the regression — the original code always passed
        ``effort=max`` regardless of source, blocking every cron run.
        """
        thinking, effort = _select_thinking_effort(agent_config, source)
        assert thinking == "high"
        assert effort == "high"

    def test_overrides_propagate_from_config(self):
        """Custom values in AgentConfig are honored, not overwritten."""
        config = AgentConfig(
            thinking="medium",
            effort="medium",
            cron_thinking="low",
            cron_effort="low",
        )
        assert _select_thinking_effort(config, "web") == ("medium", "medium")
        assert _select_thinking_effort(config, "cron") == ("low", "low")
        assert _select_thinking_effort(config, "hook") == ("low", "low")

    def test_main_and_cron_can_differ_independently(self):
        """Cron settings don't have to mirror main settings."""
        config = AgentConfig(
            thinking="max", effort="max",
            cron_thinking="medium", cron_effort="low",
        )
        thinking_main, effort_main = _select_thinking_effort(config, "web")
        thinking_cron, effort_cron = _select_thinking_effort(config, "cron")
        assert (thinking_main, effort_main) == ("max", "max")
        assert (thinking_cron, effort_cron) == ("medium", "low")


class TestAgentConfigFromDict:
    """``AgentConfig.from_dict`` accepts the new cron_* fields."""

    def test_omitted_cron_fields_default_to_high(self):
        """Configs predating this fix still load — defaults kick in."""
        config = AgentConfig.from_dict({})
        assert config.cron_thinking == "high"
        assert config.cron_effort == "high"

    def test_cron_fields_are_loaded_from_dict(self):
        config = AgentConfig.from_dict({
            "cron_thinking": "medium",
            "cron_effort": "low",
        })
        assert config.cron_thinking == "medium"
        assert config.cron_effort == "low"

    def test_main_settings_unaffected_by_cron_fields(self):
        config = AgentConfig.from_dict({
            "thinking": "max",
            "effort": "max",
            "cron_thinking": "low",
            "cron_effort": "low",
        })
        assert config.thinking == "max"
        assert config.effort == "max"
        assert config.cron_thinking == "low"
        assert config.cron_effort == "low"
