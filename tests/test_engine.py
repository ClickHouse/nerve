"""Tests for nerve.agent.engine — pure helpers (no SDK state)."""

import pytest

from nerve.agent.engine import AgentEngine


@pytest.mark.parametrize(
    "value, model, expected",
    [
        # Opus 4.7 supports every level
        ("max",    "claude-opus-4-7",           "max"),
        ("xhigh",  "claude-opus-4-7",           "xhigh"),
        ("high",   "claude-opus-4-7",           "high"),
        # Dated alias resolves via substring match
        ("max",    "claude-opus-4-7-20260416",  "max"),
        # Opus 4.6: max OK, xhigh caps to high (not registered)
        ("max",    "claude-opus-4-6",           "max"),
        ("xhigh",  "claude-opus-4-6",           "high"),
        # Sonnet 4.6 tops out at high
        ("max",    "claude-sonnet-4-6",         "high"),
        ("xhigh",  "claude-sonnet-4-6",         "high"),
        ("high",   "claude-sonnet-4-6",         "high"),
        ("medium", "claude-sonnet-4-6",         "medium"),
        ("low",    "claude-sonnet-4-6",         "low"),
        # Unknown models (including Haiku which uses budget_tokens, not levels)
        # pass through unchanged — capping is a no-op for non-level-based thinking
        ("max",    "claude-haiku-4-5-20251001", "max"),
        ("max",    "some-future-model",         "max"),
        ("max",    None,                        "max"),
        ("max",    "",                          "max"),
        # Invalid effort string → None (same as the pre-existing behaviour)
        ("invalid", "claude-opus-4-7",          None),
        ("",        "claude-sonnet-4-6",        None),
    ],
)
def test_effective_effort(value, model, expected):
    assert AgentEngine._effective_effort(value, model) == expected


def test_effective_effort_model_default_none():
    # Signature symmetry with _parse_thinking_config
    assert AgentEngine._effective_effort("max") == "max"
