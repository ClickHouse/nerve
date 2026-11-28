"""Tests for per-turn cost accounting from the SDK's cumulative counter.

The SDK's ``ResultMessage.total_cost_usd`` is cumulative.  This subsystem
has produced three accounting incidents: treating the cumulative as
per-turn (over-count, fixed by v024); clamping the delta with
``max(delta, 0)`` (under-count: $0 for the first turn of each new
client); and a baseline of zero for every new client (over-count: a
process that resumes a conversation counts from the total that an
earlier process of it saved, so the first turn of each new client paid
for that total again).  These tests pin the delta
semantics (``nerve.db.usage.compute_turn_cost``), the baseline that the
engine sets when it creates a client, the v036 backfill migration, and
the FTS5 query sanitization that broke while filing the second incident.
"""

from __future__ import annotations

import json
import os
import re
from types import SimpleNamespace

import pytest

from nerve.agent.backends import claude as claude_backend
from nerve.agent.backends.base import BackendCapabilities, SessionSpec
from nerve.agent.backends.claude import ClaudeBackend
from nerve.agent.backends.events import NormalizedUsage, TextDelta, TurnCompleted
from nerve.agent.engine import AgentEngine
from nerve.config import NerveConfig
from nerve.db import Database
from nerve.db.migrations.v036_backfill_zero_cost_turns import up as v036_up
from nerve.db.usage import compute_turn_cost, estimate_turn_cost

# A usage dict with real token traffic (estimate well above the backstop
# threshold on any pricing tier).
_BUSY_USAGE = {
    "input_tokens": 1_000,
    "output_tokens": 500,
    "cache_read_input_tokens": 200_000,
    "cache_creation_input_tokens": 20_000,
}


# ---------------------------------------------------------------------------
# compute_turn_cost: reset-aware delta semantics
# ---------------------------------------------------------------------------


class TestComputeTurnCost:
    def test_monotonic_increase_is_exact_delta(self):
        cost, source = compute_turn_cost(5.2, 5.0, _BUSY_USAGE, model="claude-fable-5")
        assert cost == pytest.approx(0.2)
        assert source == "sdk_delta"

    def test_fresh_session_uses_full_cumulative(self):
        cost, source = compute_turn_cost(0.3, 0, _BUSY_USAGE, model="claude-fable-5")
        assert cost == pytest.approx(0.3)
        # prev=0 is the fresh-session baseline: plain delta, no reset.
        assert source == "sdk_delta"

    def test_counter_below_baseline_attributes_new_cumulative(self):
        # The new process did not count from the baseline: cumulative
        # dropped from 5.0 to 0.3.  The old max(delta, 0) clamp recorded
        # $0 here.
        cost, source = compute_turn_cost(0.3, 5.0, _BUSY_USAGE, model="claude-fable-5")
        assert cost == pytest.approx(0.3)
        assert source == "sdk_reset"

    def test_no_sdk_cost_falls_back_to_estimate(self):
        cost, source = compute_turn_cost(None, 5.0, _BUSY_USAGE, model="claude-fable-5")
        assert cost == estimate_turn_cost(_BUSY_USAGE, model="claude-fable-5")
        assert cost > 0
        assert source == "estimate"

    def test_zero_cost_with_traffic_uses_estimate_backstop(self):
        # Counter stuck: same cumulative as before despite real tokens.
        cost, source = compute_turn_cost(5.0, 5.0, _BUSY_USAGE, model="claude-fable-5")
        assert cost == estimate_turn_cost(_BUSY_USAGE, model="claude-fable-5")
        assert cost > 0
        assert source == "estimate_backstop"

    def test_reset_to_zero_with_traffic_uses_estimate_backstop(self):
        # Recycled client that reports 0.0 cumulative on its first turn
        # (e.g. a provider that does not price usage).
        cost, source = compute_turn_cost(0.0, 5.0, _BUSY_USAGE, model="claude-fable-5")
        assert cost == estimate_turn_cost(_BUSY_USAGE, model="claude-fable-5")
        assert source == "estimate_backstop"

    def test_genuinely_free_turn_stays_zero(self):
        # No token traffic → the zero is real, no false-positive backstop.
        cost, source = compute_turn_cost(5.0, 5.0, {}, model="claude-fable-5")
        assert cost == 0
        assert source == "sdk_delta"

    def test_tiny_turn_below_threshold_keeps_sdk_zero(self):
        # A handful of tokens estimates below the 0.0005 backstop
        # threshold — trust the SDK's zero.
        tiny = {"input_tokens": 10, "output_tokens": 5}
        cost, source = compute_turn_cost(5.0, 5.0, tiny, model="claude-haiku-4-5")
        assert cost == 0
        assert source == "sdk_delta"

    def test_reset_with_nonzero_first_turn_not_backstopped(self):
        # Reset with a real first-turn cost: use the SDK number, not the
        # estimate, even if they differ.
        cost, source = compute_turn_cost(0.003, 9.9, _BUSY_USAGE, model="claude-fable-5")
        assert cost == pytest.approx(0.003)
        assert source == "sdk_reset"

    def test_none_usage_is_safe(self):
        cost, source = compute_turn_cost(None, 0, None, model=None)
        assert cost == 0
        assert source == "estimate"


# ---------------------------------------------------------------------------
# Engine: baseline set when a new CLI client is created
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestSetCostBaseline:
    async def _shim(self, db: Database):
        return SimpleNamespace(db=db)

    async def _baseline_after(
        self, db: Database, before: dict, start: float, *, same_conversation: bool = False,
    ) -> dict:
        await db.create_session("sess-base", source="web", actor=None)
        await db.update_session_metadata("sess-base", before)
        await AgentEngine._set_cost_baseline(
            await self._shim(db), "sess-base", start,
            same_conversation=same_conversation,
        )
        return json.loads((await db.get_session("sess-base"))["metadata"])

    async def test_baseline_set_to_the_counter_start(self, db: Database):
        # A fork, or a new conversation: the stored value is of no use.
        meta = await self._baseline_after(
            db, {"_sdk_cumulative_cost": 4.2, "other_key": "kept"}, 3.5,
        )
        assert meta["_sdk_cumulative_cost"] == 3.5
        # Sibling metadata keys survive the rewrite.
        assert meta["other_key"] == "kept"

    async def test_baseline_zeroed_for_a_counter_that_starts_at_zero(self, db: Database):
        meta = await self._baseline_after(db, {"_sdk_cumulative_cost": 4.2}, 0.0)
        assert meta["_sdk_cumulative_cost"] == 0

    async def test_noop_when_baseline_absent_and_start_is_zero(self, db: Database):
        meta = await self._baseline_after(db, {"other_key": "kept"}, 0.0)
        assert meta == {"other_key": "kept"}

    async def test_resumed_conversation_takes_a_lower_start(self, db: Database):
        # The previous process was killed: it saved less than the engine saw.
        meta = await self._baseline_after(
            db, {"_sdk_cumulative_cost": 4.2}, 3.5, same_conversation=True,
        )
        assert meta["_sdk_cumulative_cost"] == 3.5

    async def test_resumed_conversation_keeps_a_lower_stored_value(self, db: Database):
        # The previous process spent after the last result the engine saw.
        meta = await self._baseline_after(
            db, {"_sdk_cumulative_cost": 3.5}, 4.2, same_conversation=True,
        )
        assert meta["_sdk_cumulative_cost"] == 3.5

    async def test_resumed_conversation_without_a_stored_value_takes_the_start(self, db: Database):
        meta = await self._baseline_after(db, {}, 3.5, same_conversation=True)
        assert meta["_sdk_cumulative_cost"] == 3.5

    async def test_missing_session_is_safe(self, db: Database):
        await AgentEngine._set_cost_baseline(await self._shim(db), "no-such-session", 1.0)


# ---------------------------------------------------------------------------
# Claude backend: where the counter of a new process starts
# ---------------------------------------------------------------------------


def _backend_with_history(tmp_path, monkeypatch, text: str) -> tuple[ClaudeBackend, str]:
    """A ClaudeBackend on a host where the CLI's history of the
    conversation ``conv-1`` is *text*. Returns it with the cwd."""
    ws = tmp_path / "ws"
    ws.mkdir()
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    project = home / ".claude" / "projects" / re.sub(
        r"[^a-zA-Z0-9]", "-", os.path.realpath(str(ws)),
    )
    project.mkdir(parents=True)
    (project / "conv-1.jsonl").write_text(text)
    backend = ClaudeBackend(SimpleNamespace(config=lambda: SimpleNamespace()))
    return backend, str(ws)


def _lines(*entries: dict) -> str:
    return "".join(json.dumps(entry) + "\n" for entry in entries)


class TestCostCounterStart:
    def test_the_last_saved_total_is_the_start(self, tmp_path, monkeypatch):
        backend, cwd = _backend_with_history(tmp_path, monkeypatch, _lines(
            {"type": "user", "message": {"content": "one"}},
            {"type": "cost-state", "totalCostUSD": 1.5},
            {"type": "user", "message": {"content": "two"}},
            {"type": "cost-state", "totalCostUSD": 2.25},
            {"type": "last-prompt"},
        ))
        assert backend.cost_counter_start("conv-1", cwd) == 2.25

    def test_a_history_without_a_total_starts_at_zero(self, tmp_path, monkeypatch):
        backend, cwd = _backend_with_history(tmp_path, monkeypatch, _lines(
            {"type": "user", "message": {"content": "one"}},
        ))
        assert backend.cost_counter_start("conv-1", cwd) == 0.0
        # A conversation without a history on this host.
        assert backend.cost_counter_start("conv-2", cwd) == 0.0

    def test_only_a_cost_entry_with_a_total_counts(self, tmp_path, monkeypatch):
        text = _lines(
            {"type": "cost-state", "totalCostUSD": 1.5},
            # Text that names the entry type, and an entry that holds one.
            {"type": "assistant", "message": {
                "content": 'see the "cost-state" entry {"type":"cost-state","totalCostUSD":9}',
            }},
            {"type": "attachment", "attachment": {"type": "cost-state", "totalCostUSD": 9}},
            {"type": "cost-state", "totalCostUSD": "much"},
            {"type": "cost-state", "totalCostUSD": -1},
            {"type": "cost-state"},
        ) + '{"type":"cost-state","totalCostUSD":7'  # a line that is not complete
        backend, cwd = _backend_with_history(tmp_path, monkeypatch, text)
        assert backend.cost_counter_start("conv-1", cwd) == 1.5

    def test_a_total_far_from_the_end_is_found(self, tmp_path, monkeypatch):
        # Read in small pieces: lines that span several of them come after
        # the total, as they do after a process that was killed.
        monkeypatch.setattr(claude_backend, "_HISTORY_CHUNK", 64)
        backend, cwd = _backend_with_history(tmp_path, monkeypatch, _lines(
            {"type": "user", "message": {"content": "one"}},
            {"type": "cost-state", "totalCostUSD": 3.75},
            *[{"type": "assistant", "message": {"content": "x" * size}}
              for size in (10, 500, 63, 64, 65, 1)],
        ))
        assert backend.cost_counter_start("conv-1", cwd) == 3.75

    def test_a_history_that_cannot_be_read_starts_at_zero(self, tmp_path, monkeypatch):
        backend, cwd = _backend_with_history(tmp_path, monkeypatch, "")

        def _denied(path):
            raise OSError("denied")

        monkeypatch.setattr(claude_backend, "_last_saved_cost", _denied)
        assert backend.cost_counter_start("conv-1", cwd) == 0.0


# ---------------------------------------------------------------------------
# Engine: the cost of a turn across clients of one conversation
# ---------------------------------------------------------------------------


class _CountingClient:
    """A runtime process whose cost counter works as the CLI's does: it
    counts from what the conversation it resumes or forks from has saved,
    and it saves its own total when it ends normally (``end_normally``:
    the gateway stops, and the input of the process closes). ``disconnect``
    kills the process, as the Claude backend's does, and saves nothing."""

    def __init__(self, backend: "_CountingBackend", spec: SessionSpec, n: int):
        self.backend = backend
        self.model = "claude-test"
        self.resume_dropped = False
        # A fork gets its own id with its first message.
        self._native = None if spec.fork else spec.resume_native_id
        self._own = (
            f"native-fork-{n}" if spec.fork
            else spec.resume_native_id or f"native-{n}"
        )
        self.counter = backend.saved.get(spec.resume_native_id, 0.0)
        self.turns = 0

    @property
    def native_session_id(self):
        return self._native

    async def connect(self):
        pass

    async def start_turn(self, turn):
        pass

    async def receive_turn(self):
        self._native = self._own
        self.turns += 1
        self.counter += self.backend.turn_costs.pop(0)
        yield TextDelta(text="reply")
        yield TurnCompleted(
            native_session_id=self._native,
            native_turn_id=f"{self._own}-turn-{self.turns}",
            usage=NormalizedUsage.from_anthropic(
                {"input_tokens": 10, "output_tokens": 5},
            ),
            total_cost_usd=self.counter,
        )

    async def interrupt(self, **kw):
        pass

    def is_alive(self):
        return True

    def end_normally(self):
        self.backend.saved[self._own] = self.counter

    async def disconnect(self, timeout: float = 5.0):
        pass

    def try_receive_idle_events(self):
        return None

    def buffer_used(self):
        return 0


class _CountingBackend:
    name = "claude"

    def __init__(self, turn_costs: list[float]):
        self.capabilities = BackendCapabilities(
            supports_idle_stream=False, supports_cache_ttl=False,
        )
        # The cost of each turn, in the order the turns run.
        self.turn_costs = list(turn_costs)
        # What the runtime keeps per conversation: the total that its last
        # process saved when it ended normally.
        self.saved: dict[str, float] = {}
        self.clients: list[_CountingClient] = []

    def default_model(self, source):
        return "claude-test"

    def excluded_tools(self):
        return set()

    def validate_resume_target(self, native_id, cwd):
        return True

    def cost_counter_start(self, native_id, cwd):
        return self.saved.get(native_id, 0.0)

    async def create_client(self, spec):
        client = _CountingClient(self, spec, len(self.clients))
        self.clients.append(client)
        return client


async def _engine(tmp_path, db: Database, turn_costs: list[float]):
    """An engine with the session ``chat`` on a counting backend."""
    ws = tmp_path / "ws"
    ws.mkdir(exist_ok=True)
    cfg = NerveConfig.from_dict({"workspace": str(ws), "agent": {"backend": "claude"}})
    engine = AgentEngine(cfg, db)
    backend = _CountingBackend(turn_costs)
    engine._backends["claude"] = backend
    await engine.sessions.get_or_create(
        "chat", title="c", source="web", backend="claude", actor=None,
    )
    return engine, backend


async def _turn(engine: AgentEngine, session_id: str = "chat") -> None:
    await engine.run(session_id, "hello", source="web", channel="web", actor=None)


async def _end_client(engine: AgentEngine, session_id: str = "chat", *, normally: bool) -> None:
    """End the live client: the next turn builds anew. *normally* is the
    end at a gateway stop, which saves the total. The other end is the
    kill of an idle reap, which saves nothing."""
    client = engine.sessions.remove_client(session_id, reason="idle_timeout")
    if normally:
        client.end_normally()
    await client.disconnect()


@pytest.mark.asyncio
class TestTurnCostAcrossClients:
    async def test_a_client_after_a_kill_counts_from_zero(self, tmp_path, db: Database):
        engine, backend = await _engine(tmp_path, db, [0.30, 0.20])
        await _turn(engine)
        # Nothing was saved, so the next process counts from zero, below
        # the last value the engine saw.
        await _end_client(engine, normally=False)
        await _turn(engine)

        assert [c.counter for c in backend.clients] == pytest.approx([0.30, 0.20])
        assert await _fetch_costs(db, "chat") == pytest.approx([0.30, 0.20])

    async def test_a_client_after_a_normal_end_charges_each_turn_once(self, tmp_path, db: Database):
        engine, backend = await _engine(tmp_path, db, [0.30, 0.20, 0.05])
        await _turn(engine)
        await _end_client(engine, normally=True)
        # The new process counts from 0.30: it reports 0.50, then 0.55.
        await _turn(engine)
        await _turn(engine)

        assert [c.counter for c in backend.clients] == pytest.approx([0.30, 0.55])
        assert await _fetch_costs(db, "chat") == pytest.approx([0.30, 0.20, 0.05])
        assert await _session_total(db, "chat") == pytest.approx(0.55)

    async def test_each_later_client_counts_from_the_saved_total(self, tmp_path, db: Database):
        engine, backend = await _engine(tmp_path, db, [0.30, 0.20, 0.10])
        await _turn(engine)
        await _end_client(engine, normally=True)
        await _turn(engine)
        # Killed at 0.50: the conversation still holds 0.30, and the next
        # process counts from there, below the last value the engine saw.
        await _end_client(engine, normally=False)
        await _turn(engine)

        assert [c.counter for c in backend.clients] == pytest.approx([0.30, 0.50, 0.40])
        assert await _fetch_costs(db, "chat") == pytest.approx([0.30, 0.20, 0.10])

    async def test_spend_after_the_last_result_goes_to_the_next_turn(self, tmp_path, db: Database):
        engine, backend = await _engine(tmp_path, db, [0.30, 0.05])
        await _turn(engine)
        # The process spends 0.07 that no result reports (a turn that was
        # stopped), and saves 0.37 when it ends.
        backend.clients[0].counter += 0.07
        await _end_client(engine, normally=True)
        await _turn(engine)

        assert [c.counter for c in backend.clients] == pytest.approx([0.37, 0.42])
        assert await _fetch_costs(db, "chat") == pytest.approx([0.30, 0.12])
        assert await _session_total(db, "chat") == pytest.approx(0.42)

    async def test_a_fork_does_not_pay_for_its_source(self, tmp_path, db: Database):
        engine, backend = await _engine(tmp_path, db, [0.30, 0.20, 0.05])
        await _turn(engine)
        await _end_client(engine, normally=True)
        fork = await engine.fork_session("chat", None, None, actor=None)
        # The fork's process counts from the 0.30 of its source.
        await _turn(engine, fork["id"])
        await _end_client(engine, fork["id"], normally=True)
        await _turn(engine, fork["id"])

        assert [c.counter for c in backend.clients] == pytest.approx([0.30, 0.50, 0.55])
        assert await _fetch_costs(db, fork["id"]) == pytest.approx([0.20, 0.05])
        assert await _fetch_costs(db, "chat") == pytest.approx([0.30])

    async def test_a_fork_of_a_source_that_saved_nothing_counts_from_zero(self, tmp_path, db: Database):
        engine, backend = await _engine(tmp_path, db, [0.30, 0.20])
        await _turn(engine)
        # The source's process has not ended, so it has saved nothing.
        fork = await engine.fork_session("chat", None, None, actor=None)
        await _turn(engine, fork["id"])

        assert [c.counter for c in backend.clients] == pytest.approx([0.30, 0.20])
        assert await _fetch_costs(db, fork["id"]) == pytest.approx([0.20])


# ---------------------------------------------------------------------------
# v036 migration: backfill zero-cost turns from token counts
# ---------------------------------------------------------------------------

# Seed rows for the "repaired" session.  Costs recompute from token
# counts with model pricing — cross-checked against estimate_turn_cost.
_ROW_SPLIT = {
    "input_tokens": 1_000,
    "output_tokens": 500,
    "cache_read_input_tokens": 100_000,
    "cache_creation_input_tokens": 20_000,
    "cache_creation": {
        "ephemeral_5m_input_tokens": 15_000,
        "ephemeral_1h_input_tokens": 5_000,
    },
}
_ROW_AGGREGATE = {
    "input_tokens": 2_000,
    "output_tokens": 1_000,
    "cache_read_input_tokens": 50_000,
    "cache_creation_input_tokens": 10_000,
    "server_tool_use": {"web_search_requests": 3},
}


async def _fetch_costs(db: Database, session_id: str) -> list[float]:
    async with db.db.execute(
        "SELECT cost_usd FROM session_usage WHERE session_id = ? ORDER BY rowid",
        (session_id,),
    ) as cur:
        return [row[0] async for row in cur]


async def _session_total(db: Database, session_id: str) -> float:
    session = await db.get_session(session_id)
    return session["total_cost_usd"] or 0


@pytest.mark.asyncio
class TestBackfillMigration:
    async def _seed(self, db: Database) -> None:
        # Session A: two swallowed turns (one with the 5m/1h split, one
        # aggregate-only + web searches) plus one correctly-priced turn.
        await db.create_session("sess-a", source="cron", actor=None)
        await db.record_turn_usage(
            session_id="sess-a",
            input_tokens=_ROW_SPLIT["input_tokens"],
            output_tokens=_ROW_SPLIT["output_tokens"],
            cache_creation=_ROW_SPLIT["cache_creation_input_tokens"],
            cache_read=_ROW_SPLIT["cache_read_input_tokens"],
            cache_creation_5m=15_000,
            cache_creation_1h=5_000,
            max_context=200_000,
            model="claude-fable-5",
            cost_usd=0,
        )
        await db.record_turn_usage(
            session_id="sess-a",
            input_tokens=_ROW_AGGREGATE["input_tokens"],
            output_tokens=_ROW_AGGREGATE["output_tokens"],
            cache_creation=_ROW_AGGREGATE["cache_creation_input_tokens"],
            cache_read=_ROW_AGGREGATE["cache_read_input_tokens"],
            max_context=200_000,
            model="claude-opus-4-7",
            cost_usd=0,
            web_search_requests=3,
        )
        await db.record_turn_usage(
            session_id="sess-a",
            input_tokens=100, output_tokens=100,
            cache_creation=0, cache_read=0,
            max_context=200_000,
            model="claude-fable-5",
            cost_usd=1.0,
        )
        # Stored total only ever saw the correctly-priced turn.
        await db.update_session_fields("sess-a", {"total_cost_usd": 1.0})

        # Session B: a zero-cost row with zero tokens — NOT a target.
        await db.create_session("sess-b", source="web", actor=None)
        await db.record_turn_usage(
            session_id="sess-b",
            input_tokens=0, output_tokens=0,
            cache_creation=0, cache_read=0,
            max_context=200_000,
            cost_usd=0,
        )
        await db.update_session_fields("sess-b", {"total_cost_usd": 0})

        # Session C: swallowed turn with unknown model → default pricing.
        await db.create_session("sess-c", source="telegram", actor=None)
        await db.record_turn_usage(
            session_id="sess-c",
            input_tokens=1_000_000, output_tokens=0,
            cache_creation=0, cache_read=0,
            max_context=200_000,
            model=None,
            cost_usd=0,
        )
        await db.update_session_fields("sess-c", {"total_cost_usd": 0})

        # Session D: total accumulated from usage rows that telemetry
        # pruning has since deleted.  The migration must NOT re-sum this
        # to zero — pruned history is legitimate spend.
        await db.create_session("sess-d", source="web", actor=None)
        await db.update_session_fields("sess-d", {"total_cost_usd": 10.0})

    async def test_backfill_recomputes_from_tokens(self, db: Database):
        await self._seed(db)
        await v036_up(db.db)

        costs = await _fetch_costs(db, "sess-a")
        expected_split = estimate_turn_cost(_ROW_SPLIT, model="claude-fable-5")
        expected_agg = estimate_turn_cost(_ROW_AGGREGATE, model="claude-opus-4-7")
        assert costs[0] == pytest.approx(expected_split)
        assert costs[1] == pytest.approx(expected_agg)
        assert costs[0] > 0 and costs[1] > 0
        # The correctly-priced row is untouched.
        assert costs[2] == pytest.approx(1.0)

    async def test_backfill_uses_default_pricing_for_unknown_model(self, db: Database):
        await self._seed(db)
        await v036_up(db.db)

        costs = await _fetch_costs(db, "sess-c")
        # 1M fresh input tokens at the default tier ($5/MTok).
        assert costs[0] == pytest.approx(5.0)

    async def test_zero_token_rows_untouched(self, db: Database):
        await self._seed(db)
        await v036_up(db.db)

        costs = await _fetch_costs(db, "sess-b")
        assert costs == [0]

    async def test_session_totals_add_recovered_delta(self, db: Database):
        await self._seed(db)
        await v036_up(db.db)

        expected_split = estimate_turn_cost(_ROW_SPLIT, model="claude-fable-5")
        expected_agg = estimate_turn_cost(_ROW_AGGREGATE, model="claude-opus-4-7")
        assert await _session_total(db, "sess-a") == pytest.approx(
            1.0 + expected_split + expected_agg,
        )
        assert await _session_total(db, "sess-c") == pytest.approx(5.0)
        assert await _session_total(db, "sess-b") == 0
        # Totals now match the per-turn sum for sessions with full history.
        totals_a = await db.get_session_usage_totals("sess-a")
        assert totals_a["total_cost_usd"] == pytest.approx(
            await _session_total(db, "sess-a"),
        )

    async def test_pruned_session_total_preserved(self, db: Database):
        await self._seed(db)
        await v036_up(db.db)

        assert await _session_total(db, "sess-d") == pytest.approx(10.0)

    async def test_rerun_is_idempotent(self, db: Database):
        await self._seed(db)
        await v036_up(db.db)

        costs_first = {
            sid: await _fetch_costs(db, sid)
            for sid in ("sess-a", "sess-b", "sess-c")
        }
        totals_first = {
            sid: await _session_total(db, sid)
            for sid in ("sess-a", "sess-b", "sess-c", "sess-d")
        }

        await v036_up(db.db)

        for sid, costs in costs_first.items():
            assert await _fetch_costs(db, sid) == costs
        for sid, total in totals_first.items():
            assert await _session_total(db, sid) == pytest.approx(total)

    async def test_empty_db_is_noop(self, db: Database):
        await v036_up(db.db)


# ---------------------------------------------------------------------------
# FTS5 query sanitization (the dup-check crash found while filing this)
# ---------------------------------------------------------------------------


class TestFtsQueryBuilding:
    def test_dollar_sign_stripped_from_fts_query(self):
        q = Database._build_fts_query("cost recorded as $0 per turn")
        assert q  # real terms survive
        assert "$" not in q

    def test_operator_characters_stripped(self):
        q = Database._build_fts_query("a+b=c & 100% <weird> | $money NEAR near")
        assert q
        for ch in "+=&%<>|$":
            assert ch not in q

    def test_unicode_words_preserved(self):
        words = Database._tokenize_query("стоимость сессии $55")
        assert "стоимость" in words
        assert "сессии" in words


@pytest.mark.asyncio
class TestFtsSanitization:
    async def _seed_task(self, db: Database) -> None:
        await db.upsert_task(
            task_id="2026-01-01-turn-cost-recorded-as-zero",
            file_path="tasks/2026-01-01-turn-cost-recorded-as-zero.md",
            title="Turn cost recorded as $0 after client recycle",
            content="Cost counter resets swallow the first turn (about $100).",
            tags="accounting,cost",
        )

    async def test_search_tasks_with_dollar_query(self, db: Database):
        await self._seed_task(db)
        # Previously raised `fts5: syntax error near "$"`.
        results = await db.search_tasks("cost recorded as $0")
        assert any(t["id"] == "2026-01-01-turn-cost-recorded-as-zero" for t in results)

    async def test_search_tasks_similar_with_special_chars(self, db: Database):
        await self._seed_task(db)
        # The duplicate-check path used by task_create.
        results = await db.search_tasks_similar(
            "Per-turn cost silently recorded as $0 — max(delta,0) clamp "
            "swallows ~$9.99 & 100% of cron runs",
        )
        assert any(t["id"] == "2026-01-01-turn-cost-recorded-as-zero" for t in results)

    async def test_search_all_punctuation_query_is_safe(self, db: Database):
        await self._seed_task(db)
        # Nothing but FTS operators/punctuation → no crash, graceful result.
        await db.search_tasks("$$$ +++ ===")
        await db.search_tasks_similar("$$$ +++ ===")
