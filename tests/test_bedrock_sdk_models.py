"""`nerve doctor` warns about Bedrock models that Nerve's own calls cannot use.

Nerve's own model calls (titles, memory, source condensing, image
description, prompt rewrite) go through the Anthropic SDK. It sends API
version 2026-10-21, which Bedrock accepts only for Claude 4.7 and later
models. The check reads the version from the model ID. An ID that it cannot
read gets no warning.
"""

from __future__ import annotations

import pytest

from nerve.config import BEDROCK_SDK_MIN_CLAUDE_VERSION, NerveConfig, claude_model_version


@pytest.mark.parametrize("model, version", [
    ("claude-opus-5-5", (5, 5)),
    ("claude-fable-5", (5, 0)),
    ("claude-opus-4-7", (4, 7)),
    ("claude-sonnet-4-6", (4, 6)),
    ("claude-sonnet-4-5-20250929", (4, 5)),
    ("claude-opus-4-1-20250805", (4, 1)),
    ("claude-opus-4-20250514", (4, 0)),
    ("claude-3-5-haiku-20241022", (3, 5)),
    ("claude-3-haiku-20240307", (3, 0)),
    ("claude-opus-5-5[1m]", (5, 5)),
    ("claude-haiku-4-5-latest", (4, 5)),
    ("  Claude-Sonnet-4-5 ", (4, 5)),
    # Bedrock IDs: region prefix, version suffix, ARN of a system profile.
    ("us.anthropic.claude-sonnet-4-5-20250929-v1:0", (4, 5)),
    ("global.anthropic.claude-haiku-4-5-20251001-v1:0", (4, 5)),
    ("eu.anthropic.claude-opus-4-7", (4, 7)),
    ("apac.anthropic.claude-sonnet-4-20250514-v1:0", (4, 0)),
    ("anthropic.claude-3-5-sonnet-20240620-v1:0", (3, 5)),
    ("us.anthropic.claude-haiku-5-5", (5, 5)),
    ("arn:aws:bedrock:us-east-1:123456789012:inference-profile/"
     "us.anthropic.claude-sonnet-4-5-20250929-v1:0", (4, 5)),
    # Bedrock IDs with a context window.
    ("anthropic.claude-3-sonnet-20240229-v1:0:28k", (3, 0)),
    ("us.anthropic.claude-3-haiku-20240307-v1:0:48k", (3, 0)),
    # The first IDs, on the API and on Bedrock.
    ("claude-2.1", (2, 1)),
    ("claude-2.0", (2, 0)),
    ("claude-instant-1.2", (1, 2)),
    ("anthropic.claude-v2:1", (2, 1)),
    ("anthropic.claude-v2", (2, 0)),
    ("anthropic.claude-v1", (1, 0)),
    ("anthropic.claude-instant-v1", (1, 0)),
    ("anthropic.claude-v2:1:200k", (2, 1)),
    # IDs that name no readable version.
    ("arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/abc123", None),
    ("claude", None),
    ("haiku", None),
    ("gateway-small", None),
    ("", None),
])
def test_claude_model_version(model, version):
    assert claude_model_version(model) == version


def test_the_minimum_is_claude_4_7():
    assert BEDROCK_SDK_MIN_CLAUDE_VERSION == (4, 7)
    assert claude_model_version("claude-opus-4-7") >= BEDROCK_SDK_MIN_CLAUDE_VERSION
    assert claude_model_version("claude-sonnet-4-6") < BEDROCK_SDK_MIN_CLAUDE_VERSION


def _bedrock(**sections) -> NerveConfig:
    return NerveConfig.from_dict({
        "provider": {"type": "bedrock", "aws_region": "us-east-1"}, **sections,
    })


def test_the_sdk_models_of_a_default_config():
    labels = [label for label, _ in NerveConfig.from_dict({}).anthropic_sdk_models()]
    assert labels == [
        "agent.title_model", "memory.recall_model", "memory.memorize_model",
        "memory.fast_model", "agent.model (prompt rewrite)",
    ]


def test_the_sdk_models_follow_the_settings():
    config = NerveConfig.from_dict({
        "agent": {"prompt_rewrite": {"model": "rewrite-model"}},
        "memory": {"backend": "loom"},
        "sync": {"imap": {"enabled": True, "vision": {"enabled": True, "model": "vision-model"}}},
    })
    assert config.anthropic_sdk_models() == [
        ("agent.title_model", config.agent.title_model),
        ("memory.fast_model", config.memory.fast_model),
        ("agent.prompt_rewrite.model", "rewrite-model"),
        ("sync.imap.vision.model", "vision-model"),
    ]
    off = NerveConfig.from_dict({"agent": {"prompt_rewrite": {"enabled": False}}})
    assert "agent.model (prompt rewrite)" not in dict(off.anthropic_sdk_models())


def _sdk_warnings(config: NerveConfig) -> list[str]:
    from nerve.cli import doctor_report

    return [
        line.strip() for line in doctor_report(config).splitlines()
        if "older than Claude 4.7" in line
    ]


def test_the_doctor_warns_for_each_older_model_on_bedrock():
    old_title = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
    old_rewrite = "us.anthropic.claude-sonnet-4-6"
    warnings = _sdk_warnings(_bedrock(
        agent={"title_model": old_title, "prompt_rewrite": {"model": old_rewrite}},
        memory={"fast_model": "us.anthropic.claude-haiku-5-5"},
    ))
    assert len(warnings) == 2
    assert warnings[0].startswith(f"[WARN] agent.title_model '{old_title}' is older than Claude 4.7.")
    assert warnings[1].startswith(f"[WARN] agent.prompt_rewrite.model '{old_rewrite}'")
    assert all("set Claude 4.7 or later" in w for w in warnings)


@pytest.mark.parametrize("model", [
    "anthropic.claude-v2:1",
    "anthropic.claude-instant-v1",
    "anthropic.claude-3-haiku-20240307-v1:0:48k",
])
def test_the_doctor_warns_for_the_first_bedrock_models(model):
    warnings = _sdk_warnings(_bedrock(memory={"fast_model": model}))
    assert len(warnings) == 1
    assert warnings[0].startswith(f"[WARN] memory.fast_model '{model}' is older than Claude 4.7.")


def test_the_doctor_does_not_warn_for_new_or_unreadable_models():
    arn = "arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/abc123"
    assert _sdk_warnings(_bedrock()) == []
    assert _sdk_warnings(_bedrock(
        agent={"title_model": arn, "model": "us.anthropic.claude-opus-4-7"},
        memory={"fast_model": "global.anthropic.claude-opus-4-8"},
    )) == []


def test_the_doctor_does_not_warn_outside_bedrock():
    """The Claude API accepts the new API version for every model."""
    assert _sdk_warnings(NerveConfig.from_dict({
        "anthropic_api_key": "sk-ant-test-key-not-real",
        "agent": {"title_model": "claude-haiku-4-5-20251001"},
    })) == []
