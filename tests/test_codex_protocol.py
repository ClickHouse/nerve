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
# Per-turn usage = the sum of the increases in the cumulative `total`.          #
# `total` is the per-thread cumulative counter, `last` a single response. The   #
# baseline is the latest `total` on the client, or `total - last` of the first  #
# notification when the client has none.                                        #
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
    # Turn 2 is counted from the total at the end of turn 1, so it counts only
    # its own increment, never the accumulated thread history.
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
    # Resume/reconnect without the usage replay: a fresh client object
    # (baseline None) attaches to a native thread whose `total` continues at
    # ~66M and does NOT reset. The first post-resume turn must count only its
    # own response, NOT the whole accumulated history. The baseline is recovered from (total - last) of the
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
    # Only `last` present (no cumulative `total`): usage comes from that
    # single response.
    client = _client(tmp_path)
    assert await client._map_notification("thread/tokenUsage/updated", {
        "tokenUsage": {"last": _tok(10, 4, 2), "modelContextWindow": 400_000},
    }) == []
    done = _complete(client)
    assert done.usage.input_tokens == 6           # 10 - 4 cached
    assert done.usage.cache_read_tokens == 4
    assert done.usage.output_tokens == 2
    assert done.context_window == 400_000


@pytest.mark.asyncio
async def test_error_turn_still_records_partial_usage(tmp_path):
    client = _client(tmp_path)
    await _feed_usage(client, _tok(200, 0, 50), _tok(200, 0, 50))
    done = _complete(client, status="failed", error="boom")
    assert done.status == "failed" and done.error == "boom"
    assert done.usage.input_tokens == 200
    assert done.usage.output_tokens == 50


@pytest.mark.asyncio
async def test_num_turns_counts_model_responses(tmp_path):
    # num_turns counts the notifications that increase `total`. A resend of the
    # previous counts (a retry after a failed request) and a compaction (same
    # `total`, `last` input 0) are not model responses.
    client = _client(tmp_path)
    await _feed_usage(client, _tok(100, 0, 40), _tok(100, 0, 40))
    await _feed_usage(client, _tok(300, 50, 100), _tok(200, 50, 60))
    assert _complete(client).num_turns == 2
    client._reset_turn_usage_accounting()
    await _feed_usage(client, _tok(300, 50, 100), _tok(200, 50, 60))  # resend
    await _feed_usage(client, _tok(450, 80, 150), _tok(150, 30, 50))
    await _feed_usage(client, _tok(450, 80, 150), _tok(0, 0, 0))      # compaction
    assert _complete(client).num_turns == 1
    # No tokenUsage at all still reports at least one call.
    client._reset_turn_usage_accounting()
    assert _complete(client).num_turns == 1


@pytest.mark.asyncio
async def test_context_tokens_is_the_last_response_input(tmp_path):
    # The context grows over a tool loop, so the context bar needs the input of
    # the last call (cached included), not the mean over the turn's calls.
    client = _client(tmp_path)
    await _feed_usage(client, _tok(100_000, 0, 500), _tok(100_000, 0, 500),
                      window=272_000)
    await _feed_usage(client, _tok(240_000, 90_000, 1_000),
                      _tok(140_000, 90_000, 500))
    await _feed_usage(client, _tok(420_000, 220_000, 1_500),
                      _tok(180_000, 130_000, 500))
    done = _complete(client)
    assert done.context_tokens == 180_000   # the mean would be 140k
    assert done.context_window == 272_000


@pytest.mark.asyncio
async def test_carried_baseline_survives_a_stale_first_notification(tmp_path):
    # A rate-limit-then-retry can emit the turn's first tokenUsage carrying the
    # PREVIOUS turn's total+last (no response happened yet). The carried
    # previous-turn total must be the baseline, not (total - last) of that stale
    # notification, or the previous turn's last response is counted twice.
    client = _client(tmp_path)
    await _feed_usage(client, _tok(150_000, 140_000, 2_000),
                      _tok(150_000, 140_000, 2_000))
    _complete(client)                                    # turn 1 total = 150k/140k/2k
    client._reset_turn_usage_accounting()
    # Stale first notification for turn 2: previous turn's total AND last.
    await _feed_usage(client, _tok(150_000, 140_000, 2_000),
                      _tok(150_000, 140_000, 2_000))
    # Turn 2's real (only) response lands.
    await _feed_usage(client, _tok(310_000, 290_000, 3_000),
                      _tok(160_000, 150_000, 1_000))
    done = _complete(client)
    assert done.usage.input_tokens == 10_000       # 160k - 150k cached, NOT 20k
    assert done.usage.cache_read_tokens == 150_000  # NOT 290k
    assert done.usage.output_tokens == 1_000        # NOT 3k


@pytest.mark.asyncio
async def test_failure_before_first_response_records_zero(tmp_path):
    # Turn 2 fails before any real response, leaving only a stale notification
    # carrying turn 1's total. Against the carried baseline the delta is zero —
    # turn 1's last response is not re-counted.
    client = _client(tmp_path)
    await _feed_usage(client, _tok(100_000, 90_000, 5_000),
                      _tok(100_000, 90_000, 5_000))
    _complete(client)
    client._reset_turn_usage_accounting()
    await _feed_usage(client, _tok(100_000, 90_000, 5_000),
                      _tok(100_000, 90_000, 5_000))   # stale only
    done = _complete(client, status="failed", error="rate limit")
    assert done.usage.input_tokens == 0
    assert done.usage.cache_read_tokens == 0
    assert done.usage.output_tokens == 0


@pytest.mark.asyncio
async def test_context_window_exceeded_reset_rebaselines_to_zero(tmp_path):
    # On ContextWindowExceeded codex-rs zeroes `total`, and later responses
    # re-accumulate from 0. The reset adds nothing to the failing turn, and the
    # next turn counts from 0, not against the old peak, which would record 0
    # for the rest of the thread.
    client = _client(tmp_path)
    await _feed_usage(client, _tok(200_000, 150_000, 10_000),
                      _tok(200_000, 150_000, 10_000))
    _complete(client)                                    # baseline = 200k/150k/10k
    client._reset_turn_usage_accounting()
    await _feed_usage(client, _tok(0, 0, 0), _tok(0, 0, 0))   # counters zeroed
    done = _complete(client, status="failed", error="context window exceeded")
    assert done.usage.input_tokens == 0
    assert done.usage.cache_read_tokens == 0
    assert done.usage.output_tokens == 0
    # Next turn re-accumulates from 0; the baseline is the reset (0).
    client._reset_turn_usage_accounting()
    await _feed_usage(client, _tok(10_000, 5_000, 500),
                      _tok(10_000, 5_000, 500))
    done2 = _complete(client)
    assert done2.usage.input_tokens == 5_000        # 10k - 5k cached
    assert done2.usage.cache_read_tokens == 5_000
    assert done2.usage.output_tokens == 500


@pytest.mark.asyncio
async def test_context_window_exceeded_mid_turn_keeps_earlier_responses(tmp_path):
    # Two responses, then the next request exceeds the context window and codex
    # sets `total` to 0. The two responses are still the turn's usage.
    client = _client(tmp_path)
    await _feed_usage(client, _tok(1_000_000, 900_000, 20_000),
                      _tok(1_000_000, 900_000, 20_000))
    _complete(client)
    client._reset_turn_usage_accounting()
    await _feed_usage(client, _tok(1_250_000, 1_140_000, 21_000),
                      _tok(250_000, 240_000, 1_000))
    await _feed_usage(client, _tok(1_510_000, 1_390_000, 22_500),
                      _tok(260_000, 250_000, 1_500))
    await _feed_usage(client, _tok(0, 0, 0), _tok(0, 0, 0))   # counters zeroed
    done = _complete(client, status="failed", error="context window exceeded")
    assert done.usage.input_tokens == 20_000        # 510k - 490k cached
    assert done.usage.cache_read_tokens == 490_000
    assert done.usage.output_tokens == 2_500


@pytest.mark.asyncio
async def test_notification_from_another_turn_keeps_the_baseline(tmp_path):
    # A late notification of an earlier turn arrives when the client already
    # has a baseline. It is dropped, and the next notification of the turn
    # counts its tokens, so they are counted once.
    client = _client(tmp_path)
    await _feed_usage(client, _tok(100, 0, 10), _tok(100, 0, 10))
    _complete(client)
    client._reset_turn_usage_accounting()
    client._take_usage_baseline({
        "method": "thread/tokenUsage/updated",
        "params": {"turnId": "old", "tokenUsage": {
            "total": _tok(300, 0, 30), "last": _tok(200, 0, 20),
        }},
    })
    await _feed_usage(client, _tok(700, 0, 70), _tok(400, 0, 40))
    done = _complete(client)
    assert done.usage.input_tokens == 600       # the late response and this one
    assert done.usage.output_tokens == 60
