"""The default Claude models are spelled in exactly one place.

``nerve/config.py`` carries a constants block (``DEFAULT_MODEL`` and friends,
``DEFAULT_CLAUDE_MODELS``); every other site — the setup writer, the Bedrock
geo-prefixed ids, the Claude CLI's ``opus`` alias, the terminal wizard's
example line, the picker's fallback — references those names. This file is
what makes the next model update a one-line edit: a model-id literal anywhere
else under ``nerve/`` fails with its ``file:line``.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from nerve import config as config_module
from nerve.config import (
    DEFAULT_CLAUDE_MODELS,
    DEFAULT_CRON_MODEL,
    DEFAULT_EFFORT,
    DEFAULT_FALLBACK_MODEL,
    DEFAULT_FAST_MODEL,
    DEFAULT_MEMORIZE_MODEL,
    DEFAULT_MODEL,
    DEFAULT_RECALL_MODEL,
    DEFAULT_TITLE_MODEL,
    AgentConfig,
    MemoryConfig,
    NerveConfig,
)

ROOT = Path(__file__).resolve().parents[1]

# Any Claude model id: ``claude-<family>-<digit>…``. Broader than the three
# ids the defaults used to be, on purpose — a new literal of *any* generation
# outside the constants block is the mistake this guards against.
_MODEL_LITERAL = re.compile(r"claude-(opus|sonnet|haiku|fable)-\d")

# The one file allowed to spell them.
_ALLOWED = {ROOT / "nerve" / "config.py"}


def test_no_default_model_literal_outside_the_constants_block():
    offenders: list[str] = []
    for path in sorted((ROOT / "nerve").rglob("*.py")):
        if path in _ALLOWED:
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if _MODEL_LITERAL.search(line):
                offenders.append(f"{path.relative_to(ROOT)}:{number}: {line.strip()}")
    assert not offenders, (
        "A Claude model id is spelled outside the constants block at the top of "
        "nerve/config.py. Reference DEFAULT_MODEL / DEFAULT_CRON_MODEL / … instead, so "
        "the next default-model update is one edit:\n  " + "\n  ".join(offenders)
    )


def test_the_constants_agree_with_each_other():
    assert DEFAULT_MODEL == "claude-opus-5-5"
    assert DEFAULT_CLAUDE_MODELS[0] == DEFAULT_MODEL
    assert DEFAULT_CLAUDE_MODELS[1] == DEFAULT_FALLBACK_MODEL
    assert len(set(DEFAULT_CLAUDE_MODELS)) == len(DEFAULT_CLAUDE_MODELS)
    for model_id in DEFAULT_CLAUDE_MODELS:
        assert _MODEL_LITERAL.match(model_id), model_id
    # The dataclass defaults are the constants, parsed and declared alike.
    assert AgentConfig().model == AgentConfig.from_dict({}).model == DEFAULT_MODEL
    assert AgentConfig().cron_model == AgentConfig.from_dict({}).cron_model == DEFAULT_CRON_MODEL
    assert AgentConfig().title_model == AgentConfig.from_dict({}).title_model == DEFAULT_TITLE_MODEL
    # The reasoning effort travels with the models: one constant, declared and parsed alike.
    assert DEFAULT_EFFORT == "xhigh"
    assert AgentConfig().effort == AgentConfig.from_dict({}).effort == DEFAULT_EFFORT
    memory = MemoryConfig.from_dict({})
    assert (memory.recall_model, memory.memorize_model, memory.fast_model) == (
        DEFAULT_RECALL_MODEL, DEFAULT_MEMORIZE_MODEL, DEFAULT_FAST_MODEL,
    )
    assert NerveConfig().agent.model == DEFAULT_MODEL
    assert NerveConfig().selectable_claude_models()[0] == DEFAULT_MODEL


def test_the_constants_block_sits_at_the_top_of_config_py():
    """The block is the first thing after the imports — where the next person
    updating the default will look — and the only assignment of each name."""
    text = (ROOT / "nerve" / "config.py").read_text(encoding="utf-8")
    first_def = text.index("\ndef ")
    for name in (
        "DEFAULT_MODEL", "DEFAULT_CRON_MODEL", "DEFAULT_TITLE_MODEL", "DEFAULT_RECALL_MODEL",
        "DEFAULT_MEMORIZE_MODEL", "DEFAULT_FAST_MODEL", "DEFAULT_FALLBACK_MODEL", "DEFAULT_EFFORT",
        "DEFAULT_CLAUDE_MODELS",
    ):
        assignments = re.findall(rf"^{name}\b[^=\n]*=", text, flags=re.MULTILINE)
        assert len(assignments) == 1, (name, assignments)
        assert text.index(f"\n{name}") < first_def, f"{name} is assigned below the first def"
    assert getattr(config_module, "DEFAULT_MODEL") == DEFAULT_MODEL


def test_the_writer_and_the_cli_alias_use_the_constants():
    from nerve.setup_writer import SetupChoices, bedrock_geo_prefix, build_config_layers

    _machine, portable, _shadowed = build_config_layers(SetupChoices())
    assert portable["agent"] == {
        "model": DEFAULT_MODEL, "cron_model": DEFAULT_CRON_MODEL,
        "max_turns": 50, "max_concurrent": 32, "thinking": "max", "effort": DEFAULT_EFFORT, "context_1m": True,
    }
    assert (portable["memory"]["recall_model"], portable["memory"]["memorize_model"],
            portable["memory"]["fast_model"]) == (DEFAULT_RECALL_MODEL, DEFAULT_MEMORIZE_MODEL, DEFAULT_FAST_MODEL)
    _machine, bedrock, _shadowed = build_config_layers(
        SetupChoices(provider_type="bedrock", aws_region="eu-west-1"),
    )
    geo = bedrock_geo_prefix("eu-west-1")
    assert bedrock["agent"]["model"] == f"{geo}.anthropic.{DEFAULT_MODEL}"
    assert bedrock["agent"]["cron_model"] == f"{geo}.anthropic.{DEFAULT_CRON_MODEL}"
    assert bedrock["agent"]["title_model"] == f"{geo}.anthropic.{DEFAULT_TITLE_MODEL}-v1:0"
    assert bedrock["memory"]["fast_model"] == f"{geo}.anthropic.{DEFAULT_FAST_MODEL}-v1:0"

    # The Claude CLI's built-in `opus` alias is the default model.
    from types import SimpleNamespace

    from nerve.agent.backends.claude import ClaudeBackend

    config = SimpleNamespace(
        provider=SimpleNamespace(
            is_bedrock=False, aws_region="", aws_profile="",
            aws_access_key_id="", aws_secret_access_key="",
        ),
        proxy=SimpleNamespace(enabled=False, host="", port=0),
        anthropic_base_url="",
        effective_api_key="",
        agent=SimpleNamespace(model_aliases={}, agent_teams=True),
    )
    env = ClaudeBackend(SimpleNamespace(config=lambda: config))._build_env()
    assert env["ANTHROPIC_DEFAULT_OPUS_MODEL"] == DEFAULT_MODEL


def test_the_examples_and_templates_carry_the_default():
    """Regenerated text, not constants — but it must say the same thing."""
    example = yaml.safe_load((ROOT / "config.example.yaml").read_text(encoding="utf-8"))
    assert example["agent"]["model"] == DEFAULT_MODEL
    assert example["agent"]["cron_model"] == DEFAULT_CRON_MODEL
    assert example["agent"]["title_model"] == DEFAULT_TITLE_MODEL
    template = (ROOT / "nerve" / "templates" / "config" / "settings.yaml").read_text(encoding="utf-8")
    assert f"#   model: {DEFAULT_MODEL}" in template
    for golden in ("personal", "unattended-personal", "unattended-worker"):
        data = yaml.safe_load(
            (ROOT / "tests" / "fixtures" / "setup_writer" / golden / "settings.yaml.golden").read_text(encoding="utf-8"),
        )
        assert data["agent"]["model"] == DEFAULT_MODEL, golden
        assert data["agent"]["effort"] == DEFAULT_EFFORT, golden
    worker = yaml.safe_load(
        (ROOT / "tests" / "fixtures" / "setup_writer" / "worker" / "settings.yaml.golden").read_text(encoding="utf-8"),
    )
    assert worker["agent"]["model"] == f"eu.anthropic.{DEFAULT_MODEL}"
    config_doc = (ROOT / "docs" / "config.md").read_text(encoding="utf-8")
    assert f"| `agent.model` | string | `{DEFAULT_MODEL}` |" in config_doc
