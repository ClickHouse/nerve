"""Codex notification→event mapping + pricing unit tests (no subprocess)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from nerve.agent.backends import events as ev
from nerve.agent.backends.base import SessionSpec
from nerve.agent.backends.codex.backend import CodexBackend, CodexClient
from nerve.agent.backends.codex.pricing import compute_cost, match_pricing
from nerve.config import NerveConfig


def _client(tmp_path, **codex_overrides) -> CodexClient:
    cfg = NerveConfig.from_dict({
        "workspace": str(tmp_path),
        "codex": {"home_dir": str(tmp_path / "home"), **codex_overrides},
    })
    deps = SimpleNamespace(
        config=cfg,
        external_mcp_servers=lambda: [],
        gateway_port=lambda: None,
        mint_session_token=None,
        tool_ctx_factory=lambda sid: None,
        registry=None,
        db=None,
    )
    backend = CodexBackend(deps)
    spec = SessionSpec(
        session_id="s1", source="web", model=None, effort="high",
        system_prompt="", cwd=str(tmp_path),
    )
    return CodexClient(backend, spec)


@pytest.mark.asyncio
async def test_unknown_notifications_are_tolerated(tmp_path):
    client = _client(tmp_path)
    assert await client._map_notification("some/future/thing", {"x": 1}) == []
    assert await client._map_notification("", {}) == []


@pytest.mark.asyncio
async def test_stale_turn_usage_is_scoped(tmp_path):
    client = _client(tmp_path)
    events = await client._map_notification("thread/tokenUsage/updated", {
        "threadId": "t", "turnId": "turn_1",
        "tokenUsage": {
            "last": {"inputTokens": 10, "cachedInputTokens": 4,
                     "outputTokens": 2, "reasoningOutputTokens": 0,
                     "totalTokens": 12},
            "modelContextWindow": 400000,
        },
    })
    assert events == []  # retained, not emitted
    done = client._map_turn_completed({
        "turn": {"id": "turn_1", "status": "completed", "durationMs": 5},
    })
    assert done.usage.input_tokens == 6      # 10 - 4 cached (disjoint split)
    assert done.usage.cache_read_tokens == 4
    assert done.context_window == 400000


@pytest.mark.asyncio
async def test_model_rerouted_updates_serving_model(tmp_path):
    client = _client(tmp_path)
    events = await client._map_notification("model/rerouted", {"model": "gpt-5.6-terra"})
    assert events == [ev.ModelObserved(model="gpt-5.6-terra")]
    done = client._map_turn_completed({"turn": {"id": "x", "status": "completed"}})
    assert done.model == "gpt-5.6-terra"


@pytest.mark.asyncio
async def test_command_exit_code_marks_error(tmp_path):
    client = _client(tmp_path)
    await client._map_notification("item/started", {"item": {
        "id": "c9", "type": "commandExecution", "command": ["false"],
    }})
    events = client._map_item_completed({
        "id": "c9", "type": "commandExecution",
        "command": ["false"], "aggregatedOutput": "", "exitCode": 3,
    })
    assert len(events) == 1
    assert events[0].is_error is True
    assert "exit code 3" in events[0].content


def test_turn_status_fallbacks(tmp_path):
    client = _client(tmp_path)
    weird = client._map_turn_completed({"turn": {"id": "x", "status": "inProgress"}})
    assert weird.status == "completed"  # defensive downgrade, logged
    failed = client._map_turn_completed({
        "turn": {"id": "x", "status": "failed", "error": {"message": "boom"}},
    })
    assert failed.status == "failed" and failed.error == "boom"


def test_pricing_matches_longest_substring():
    table = {
        "gpt-5.6": {"input": 1.0, "cached_input": 0.1, "output": 2.0},
        "gpt-5.6-sol": {"input": 5.0, "cached_input": 0.5, "output": 30.0},
    }
    assert match_pricing("gpt-5.6-sol-20260709", table)["input"] == 5.0
    assert match_pricing("gpt-5.6-luna", table)["input"] == 1.0
    assert match_pricing("o5-mini", table) is None
    assert match_pricing(None, table) is None


def test_cost_none_for_unknown_model_never_estimated():
    usage = ev.NormalizedUsage(
        input_tokens=1_000_000, output_tokens=1_000_000,
        cache_read_tokens=1_000_000,
    )
    assert compute_cost("mystery-model", usage, {"gpt-5.6": {
        "input": 1.0, "cached_input": 0.1, "output": 2.0,
    }}) is None
    assert compute_cost("gpt-5.6", None, {"gpt-5.6": {"input": 1.0}}) is None
    got = compute_cost("gpt-5.6", usage, {"gpt-5.6": {
        "input": 1.0, "cached_input": 0.1, "output": 2.0,
    }})
    assert got == pytest.approx(1.0 + 0.1 + 2.0)


def test_normalized_usage_anthropic_shape_contract():
    # Codex-shaped usage synthesizes the canonical keys + keeps raw.
    u = ev.NormalizedUsage(
        input_tokens=6, output_tokens=2, cache_read_tokens=4,
        cache_creation_tokens=0, raw={"last": {"inputTokens": 10}},
    )
    shaped = u.to_anthropic_shape()
    assert shaped["input_tokens"] == 6
    assert shaped["cache_read_input_tokens"] == 4
    assert shaped["cache_creation_input_tokens"] == 0
    assert shaped["_raw"] == {"last": {"inputTokens": 10}}

    # Claude-shaped usage passes through byte-identical (cache-TTL split
    # readers depend on nested cache_creation.ephemeral_* surviving).
    native = {
        "input_tokens": 100, "output_tokens": 5,
        "cache_read_input_tokens": 7, "cache_creation_input_tokens": 3,
        "cache_creation": {"ephemeral_5m_input_tokens": 3},
        "server_tool_use": {"web_search_requests": 1},
    }
    u2 = ev.NormalizedUsage.from_anthropic(native)
    assert u2.to_anthropic_shape() is native
    assert u2.input_tokens == 100 and u2.cache_read_tokens == 7


def test_effort_mapping_and_defaults(tmp_path):
    client = _client(tmp_path, effort_map={"max": "xhigh", "low": "minimal"})
    backend = client._backend
    assert backend.map_effort("max") == "xhigh"
    assert backend.map_effort("low") == "minimal"
    assert backend.map_effort("high") == "high"     # default map preserved
    assert backend.map_effort("unknown") is None    # omitted from turn/start


def test_backend_notes_appended_to_developer_instructions(tmp_path):
    client = _client(tmp_path)
    params = client._backend.thread_params(client._spec)
    assert params["developerInstructions"].startswith("")
    assert "schedule_wakeup" in params["developerInstructions"]
    assert params["approvalPolicy"] == "never"
    assert params["sandbox"] == "danger-full-access"
