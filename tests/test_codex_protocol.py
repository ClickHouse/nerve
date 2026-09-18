"""Codex notification→event mapping + pricing unit tests (no subprocess)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from nerve.agent.backends import events as ev
from nerve.agent.backends.base import SessionSpec
from nerve.agent.backends.codex.backend import (
    CodexBackend,
    CodexClient,
    clamp_effort,
)
from nerve.agent.backends.codex.pricing import compute_cost, match_pricing
from nerve.config import _DEFAULT_CODEX_PRICING, CodexConfig, NerveConfig


def _client(tmp_path, **codex_overrides) -> CodexClient:
    cfg = NerveConfig.from_dict({
        "workspace": str(tmp_path),
        "codex": {"home_dir": str(tmp_path / "home"), **codex_overrides},
    })
    deps = SimpleNamespace(
        config=lambda: cfg,
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
    # Schema shape: {fromModel, toModel, reason, threadId, turnId}
    events = await client._map_notification("model/rerouted", {
        "fromModel": "gpt-5.6-sol", "toModel": "gpt-5.6-terra",
        "threadId": "t", "turnId": "x", "reason": "capacity",
    })
    assert events == [ev.ModelObserved(model="gpt-5.6-terra")]
    done = client._map_turn_completed({"turn": {"id": "x", "status": "completed"}})
    assert done.model == "gpt-5.6-terra"


@pytest.mark.asyncio
async def test_command_exit_code_marks_error(tmp_path):
    client = _client(tmp_path)
    await client._map_notification("item/started", {"item": {
        "id": "c9", "type": "commandExecution", "command": ["false"],
    }})
    events = await client._map_item_completed({
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


def test_default_pricing_covers_gpt_6_astra():
    assert _DEFAULT_CODEX_PRICING["gpt-6-astra"] == {
        "input": 10.0, "cached_input": 1.0, "output": 50.0,
    }
    usage = ev.NormalizedUsage(
        input_tokens=1_000_000, output_tokens=1_000_000,
        cache_read_tokens=1_000_000,
    )
    assert compute_cost("gpt-6-astra", usage, _DEFAULT_CODEX_PRICING) == (
        pytest.approx(10.0 + 1.0 + 50.0)
    )


def test_clamp_effort_uses_the_catalog_as_a_ceiling():
    full = ["low", "medium", "high", "xhigh", "max", "ultra"]
    assert clamp_effort("ultra", full) == "ultra"
    assert clamp_effort("ultra", ["low", "medium", "high"]) == "high"
    assert clamp_effort("max", ["low", "medium", "high"]) == "high"
    assert clamp_effort("medium", ["low", "high"]) == "low"
    assert clamp_effort("low", ["medium", "high"]) == "low"   # nothing lower: as asked
    assert clamp_effort("ultra", []) == "ultra"               # unknown catalog: advisory
    assert clamp_effort("turbo", ["low"]) == "turbo"          # outside the vocabulary
    assert clamp_effort(None, full) is None


def test_catalog_offers_visible_plus_configured_hidden(tmp_path):
    entries = [
        {"id": "gpt-5.6-sol", "hidden": False, "supportedReasoningEfforts": [
            {"reasoningEffort": "low"}, {"reasoningEffort": "ultra"},
        ]},
        {"id": "gpt-6-astra", "hidden": True, "supportedReasoningEfforts": [
            {"reasoningEffort": "max"},
        ]},
        {"id": "internal-preview", "hidden": True},
        {"model": "legacy-shape"},          # id-less shape still identifies
        {"garbage": True},                  # no identifier: ignored
    ]
    backend = _client(tmp_path)._backend    # default codex.models: gpt-6-astra
    cat = backend.catalog(entries)
    assert cat["listed"] == [
        "gpt-5.6-sol", "gpt-6-astra", "internal-preview", "legacy-shape",
    ]
    assert cat["offered"] == ["gpt-5.6-sol", "gpt-6-astra", "legacy-shape"]
    assert cat["hidden"] == ["gpt-6-astra", "internal-preview"]
    assert cat["allowed"] == cat["listed"]
    assert cat["efforts"]["gpt-5.6-sol"] == ["low", "ultra"]
    assert cat["efforts"]["internal-preview"] == []

    # A configured model the catalog does not serve is allowed, never offered.
    backend = _client(tmp_path, models=["gpt-6-astra", "gpt-7-preview"])._backend
    cat = backend.catalog(entries[:1])
    assert cat["offered"] == ["gpt-5.6-sol"]
    assert cat["allowed"] == ["gpt-5.6-sol", "gpt-6-astra", "gpt-7-preview"]
    # models: [] opts out of every hidden entry.
    cat = _client(tmp_path, models=[])._backend.catalog(entries)
    assert cat["offered"] == ["gpt-5.6-sol", "legacy-shape"]
    # An empty catalog lists nothing — callers then skip model validation.
    assert backend.catalog([])["listed"] == []


def test_codex_models_config_parsing():
    assert CodexConfig.from_dict({}).models == ["gpt-6-astra"]
    assert CodexConfig.from_dict({"models": None}).models == ["gpt-6-astra"]
    assert CodexConfig.from_dict({"models": []}).models == []
    assert CodexConfig.from_dict({"models": "gpt-6-astra"}).models == ["gpt-6-astra"]
    assert CodexConfig.from_dict({"models": ["a", "a", " b ", ""]}).models == ["a", "b"]
    assert CodexConfig.from_dict({"models": 42}).models == ["gpt-6-astra"]


def test_backend_notes_appended_to_developer_instructions(tmp_path):
    client = _client(tmp_path)
    client._spec.system_prompt = "base system prompt"
    params = client._backend.thread_params(client._spec)
    instructions = params["developerInstructions"]
    flat_instructions = " ".join(instructions.split())
    assert instructions.startswith("base system prompt")
    assert "schedule_wakeup" in instructions
    assert "Nerve runbooks, not Codex-native skills" in flat_instructions
    assert "later user turns without calling `skill_get` again" in flat_instructions
    assert "resume of the same native thread" in flat_instructions
    assert "independent child must load its own copy once" in flat_instructions
    assert "context compaction" in flat_instructions
    assert "Native Codex skills keep their normal" in flat_instructions
    assert params["approvalPolicy"] == "never"
    assert params["sandbox"] == "danger-full-access"


# --------------------------------------------------------------------------- #
# Per-turn usage: cumulative-total delta (not `last`).                         #
#                                                                              #
# The app-server emits one thread/tokenUsage/updated per API response within a #
# turn. `total` is the monotonic per-thread cumulative counter; `last` is only #
# the most recent single response. A turn's usage is the delta of `total`      #
# across the turn — reading `last` alone drops every response but the last of  #
# a multi-step (tool-calling) turn and undercounts it.                         #
# --------------------------------------------------------------------------- #


def _tok(inp, cached, out):
    return {"inputTokens": inp, "cachedInputTokens": cached, "outputTokens": out}


async def _feed_usage(client, total, last, window=None):
    """Deliver one thread/tokenUsage/updated notification."""
    payload = {"total": total, "last": last}
    if window is not None:
        payload["modelContextWindow"] = window
    assert await client._map_notification(
        "thread/tokenUsage/updated", {"tokenUsage": payload},
    ) == []  # retained for the turn, never emitted as an event


def _complete(client, status="completed", error=None):
    turn = {"id": "t", "status": status}
    if error is not None:
        turn["error"] = {"message": error}
    return client._map_turn_completed({"turn": turn})


@pytest.mark.asyncio
async def test_single_step_turn_uses_total_delta(tmp_path):
    # Fresh thread, one response: total == last, so the turn-start baseline is
    # zero and the turn equals the full total (cached split out disjoint).
    client = _client(tmp_path)
    await _feed_usage(client, _tok(100, 20, 10), _tok(100, 20, 10))
    done = _complete(client)
    assert done.usage.input_tokens == 80        # 100 - 20 cached
    assert done.usage.cache_read_tokens == 20
    assert done.usage.output_tokens == 10


@pytest.mark.asyncio
async def test_multi_step_turn_uses_total_not_last(tmp_path):
    # Two responses in one turn. The final `last` (200/50/60) must NOT be the
    # recorded usage — the turn is the cumulative total delta (300/50/100).
    client = _client(tmp_path)
    await _feed_usage(client, _tok(100, 0, 40), _tok(100, 0, 40))   # response 1
    await _feed_usage(client, _tok(300, 50, 100), _tok(200, 50, 60))  # response 2
    done = _complete(client)
    assert done.usage.output_tokens == 100      # not 60 (final `last`)
    assert done.usage.input_tokens == 250       # 300 - 50 cached, not 200-based
    assert done.usage.cache_read_tokens == 50


@pytest.mark.asyncio
async def test_second_turn_deltas_off_first_turn_end(tmp_path):
    # Turn 2's baseline is derived from the live stream (total - last of its
    # first notification), which equals turn 1's ending total — so turn 2 counts
    # only its own increment, never the accumulated thread history.
    client = _client(tmp_path)
    await _feed_usage(client, _tok(100, 0, 40), _tok(100, 0, 40))
    await _feed_usage(client, _tok(300, 50, 100), _tok(200, 50, 60))
    _complete(client)                                   # turn 1 ended at 300/50/100
    client._reset_turn_usage_accounting()               # what start_turn() does
    await _feed_usage(client, _tok(450, 80, 150), _tok(150, 30, 50))
    done = _complete(client)
    assert done.usage.output_tokens == 50               # 150 - 100
    assert done.usage.input_tokens == 120               # (450-300) - (80-50)
    assert done.usage.cache_read_tokens == 30           # 80 - 50


@pytest.mark.asyncio
async def test_resume_does_not_overcount_full_thread_total(tmp_path):
    # Resume/reconnect: a fresh client object (baseline None) attaches to a
    # native thread whose `total` continues at ~66M and does NOT reset. The
    # first post-resume turn must count only its own response, NOT the whole
    # accumulated history. The baseline is recovered from (total - last) of the
    # first notification, so the delta is tiny even though `total` is huge.
    client = _client(tmp_path)
    await _feed_usage(
        client,
        _tok(66_000_000, 64_000_000, 300_000),   # continuing thread total
        _tok(132_000, 130_000, 40),               # this turn's single response
    )
    done = _complete(client)
    assert done.usage.input_tokens == 2_000       # 132_000 - 130_000, NOT ~2M
    assert done.usage.cache_read_tokens == 130_000
    assert done.usage.output_tokens == 40
    # Guard against a regression to "no baseline => full total" (would be ~2M).
    assert done.usage.input_tokens < 1_000_000


@pytest.mark.asyncio
async def test_fallback_to_last_when_total_absent(tmp_path):
    # Legacy app-server sends only `last` (no cumulative `total`): preserve the
    # previous single-response behaviour rather than dropping usage.
    client = _client(tmp_path)
    assert await client._map_notification("thread/tokenUsage/updated", {
        "tokenUsage": {"last": _tok(10, 4, 2), "modelContextWindow": 400_000},
    }) == []
    assert client._turn_total_base is None        # nothing to anchor a delta to
    done = _complete(client)
    assert done.usage.input_tokens == 6           # 10 - 4 cached
    assert done.usage.cache_read_tokens == 4
    assert done.usage.output_tokens == 2
    assert done.context_window == 400_000


@pytest.mark.asyncio
async def test_reset_turn_accounting_clears_state(tmp_path):
    # start_turn()'s reset must clear the baseline and the native-child marker
    # so nothing bleeds from the previous turn.
    client = _client(tmp_path)
    await client._map_notification("item/started", {"item": {
        "id": "c1", "type": "collabAgentToolCall", "tool": "Agent", "prompt": "go",
    }})
    await _feed_usage(client, _tok(100, 0, 10), _tok(100, 0, 10))
    assert client._turn_total_base is not None and client._turn_has_native_child
    client._reset_turn_usage_accounting()
    assert client._turn_usage is None
    assert client._turn_total_base is None
    assert client._turn_has_native_child is False


@pytest.mark.asyncio
async def test_error_turn_still_records_partial_usage(tmp_path):
    client = _client(tmp_path)
    await _feed_usage(client, _tok(200, 0, 50), _tok(200, 0, 50))
    done = _complete(client, status="failed", error="boom")
    assert done.status == "failed" and done.error == "boom"
    assert done.usage.input_tokens == 200
    assert done.usage.output_tokens == 50


@pytest.mark.asyncio
async def test_native_child_flags_cost_lower_bound(tmp_path):
    # A native collaboration child ran: its separate-thread tokens are not yet
    # attributed here, so the turn's cost must be marked a lower bound.
    client = _client(tmp_path)
    await client._map_notification("item/started", {"item": {
        "id": "c1", "type": "collabAgentToolCall", "tool": "Researcher",
        "prompt": "investigate",
    }})
    await _feed_usage(client, _tok(500, 100, 60), _tok(500, 100, 60))
    done = _complete(client)
    assert done.usage.input_tokens == 400              # parent still counted
    assert done.usage.raw.get("cost_is_lower_bound") is True
    assert done.usage.raw.get("native_children_unattributed") is True


@pytest.mark.asyncio
async def test_native_child_only_turn_records_lower_bound(tmp_path):
    # Turn completes with a native child but no parent tokenUsage notification:
    # a zero-parent usage is still recorded, carrying the lower-bound marker so
    # the child cost is not silently dropped.
    client = _client(tmp_path)
    await client._map_notification("item/started", {"item": {
        "id": "c1", "type": "collabAgentToolCall", "tool": "Agent", "prompt": "x",
    }})
    done = _complete(client)
    assert done.usage is not None
    assert done.usage.input_tokens == 0
    assert done.usage.raw.get("cost_is_lower_bound") is True


@pytest.mark.asyncio
async def test_dropped_mid_turn_notification_self_heals(tmp_path):
    # If a mid-turn notification is lost, the cumulative `total` on the NEXT
    # notification still includes the missing response, so the total-delta
    # recovers it — a sum-of-`last` scheme would have undercounted.
    client = _client(tmp_path)
    await _feed_usage(client, _tok(100, 0, 30), _tok(100, 0, 30))  # response 1
    # (response 2's notification dropped — never delivered)
    await _feed_usage(client, _tok(600, 0, 130), _tok(300, 0, 60))  # response 3
    done = _complete(client)
    assert done.usage.input_tokens == 600      # full cumulative, incl. the gap
    assert done.usage.output_tokens == 130
