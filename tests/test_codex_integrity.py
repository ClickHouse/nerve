"""Regression tests for Codex session identity and billing invariants."""

from __future__ import annotations

from pathlib import Path

import pytest

from nerve.agent.engine import AgentEngine
from nerve.agent.sessions import SessionManager
from nerve.config import NerveConfig
from nerve.db.migrations.v039_codex_integrity import up as apply_v039

pytestmark = pytest.mark.asyncio


async def test_v039_backfills_null_backend_to_claude(db):
    await db.create_session("legacy", backend="claude")
    await db._write("UPDATE sessions SET backend = NULL WHERE id = ?", ("legacy",))
    await apply_v039(db.db)
    await db.db.commit()
    row = await db.get_session("legacy")
    assert row["backend"] == "claude"


async def test_legacy_null_row_never_uses_changed_global_default(db, tmp_path):
    await db.create_session("legacy", backend="claude")
    await db._write("UPDATE sessions SET backend = NULL WHERE id = ?", ("legacy",))
    cfg = NerveConfig.from_dict({
        "workspace": str(tmp_path),
        "agent": {"backend": "codex"},
        "codex": {"home_dir": str(tmp_path / "codex")},
    })
    engine = AgentEngine(cfg, db)
    row = await db.get_session("legacy")
    assert engine._backend_for(row, "web").name == "claude"


async def test_fork_inherits_backend_model_and_cwd(db, tmp_path):
    manager = SessionManager(db)
    await db.create_session(
        "parent", backend="codex", model="gpt-test", cwd=str(tmp_path),
    )
    fork = await manager.fork_session("parent", at_message_id="7")
    row = await db.get_session(fork["id"])
    assert row["backend"] == "codex"
    assert row["model"] == "gpt-test"
    assert row["cwd"] == str(tmp_path)
    assert row["forked_from_message"] == "7"


async def test_native_turn_lookup_and_thread_mapping(db):
    await db.create_session("s1", backend="codex")
    user_id = await db.add_message("s1", "user", "question")
    assistant_id = await db.add_message(
        "s1", "assistant", "answer", native_turn_id="turn-1",
    )
    assert await db.get_native_turn_at_message("s1", user_id) == "turn-1"
    assert await db.get_native_turn_at_message("s1", assistant_id) == "turn-1"
    await db.bind_native_thread("codex", "thread-1", "s1")
    assert await db.get_session_for_native_thread("codex", "thread-1") == "s1"


async def test_codex_engine_run_persists_reloadable_output(
    db, tmp_path, monkeypatch,
):
    """A streamed Codex turn must survive a fresh history read."""
    fake_bin = Path(__file__).parent / "fixtures" / "fake_codex_appserver.py"
    workspace = tmp_path / "ws"
    workspace.mkdir()
    monkeypatch.setenv("FAKE_CODEX_MODE", "basic")
    cfg = NerveConfig.from_dict({
        "workspace": str(workspace),
        "agent": {"backend": "codex"},
        "codex": {
            "bin_path": str(fake_bin),
            "home_dir": str(tmp_path / "codex-home"),
            "model": "gpt-5.6-sol",
        },
    })
    engine = AgentEngine(cfg, db)
    session_id = "codex-persist-e2e"
    await engine.sessions.get_or_create(
        session_id,
        title="Persistence test",
        source="web",
        backend="codex",
    )

    try:
        response = await engine.run(
            session_id, "hello", source="web", channel="web",
        )
        assert response == "Hello "

        # This is the durable read behind GET /api/sessions/{id}/messages.
        history = await db.get_messages(session_id)
        assert [(row["role"], row["content"]) for row in history] == [
            ("user", "hello"),
            ("assistant", "Hello "),
        ]
        assert history[-1]["native_turn_id"] == "turn_1_1"
        assert history[-1]["blocks"] == [
            {"type": "thinking", "content": "thinking..."},
            {"type": "text", "content": "Hello "},
        ]
    finally:
        await engine.shutdown()


async def test_chatgpt_estimate_is_not_billed_cost(db):
    await db.create_session("s1", backend="codex")
    await db.record_turn_usage(
        "s1", 10, 5, 0, 0, 100,
        model="gpt-test",
        cost_usd=None,
        cost_basis="api_equivalent_estimate",
        estimated_cost_usd=0.123,
    )
    async with db.db.execute(
        "SELECT cost_usd, cost_basis, estimated_cost_usd "
        "FROM session_usage WHERE session_id = 's1'"
    ) as cursor:
        row = await cursor.fetchone()
    assert row["cost_usd"] is None
    assert row["cost_basis"] == "api_equivalent_estimate"
    assert row["estimated_cost_usd"] == 0.123
    totals = await db.get_session_usage_totals("s1")
    assert totals["total_cost_usd"] == 0
    assert totals["total_estimated_cost_usd"] == pytest.approx(0.123)
    summary = await db.get_usage_summary(7)
    assert summary["total_cost_usd"] == 0
    assert summary["total_estimated_cost_usd"] == pytest.approx(0.123)
