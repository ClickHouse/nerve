"""The summary of a plan that waits for the person's decision.

The summarizer (``nerve.agent.plan_summary``) against a fake Messages API
client, and the engine's wiring: an ``ExitPlanMode`` pause gets one
``plan_summary`` event on the session channel, buffered for reconnect replay
like the ``interaction`` event, and never holds the pause up.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from nerve.agent import engine as engine_module
from nerve.agent import plan_summary
from nerve.agent.engine import AgentEngine
from nerve.agent.interactive import InteractiveToolHandler
from nerve.agent.plan_summary import (
    MAX_PLAN_CHARS,
    MAX_TOKENS,
    SUMMARY_TIMEOUT,
    parse_bullets,
    summarize_plan,
)
from nerve.agent.streaming import broadcaster
from nerve.config import NerveConfig

PLAN = (
    "# Setup proposal — review pull requests in org/repo-a\n"
    "## Jobs\n"
    "| pr-scan | */30 * * * * | tasks tagged pr-review | draft comments |\n"
    "## Approval boundaries\n"
    "Review comments are drafts until you approve them.\n"
)

ANSWER = (
    "Here is the summary:\n"
    "- Scans org/repo-a for new pull requests every 30 minutes.\n"
    "\n"
    "* Drafts review comments; you approve each one before it is posted.\n"
    "• Needs read access to org/repo-a and an account to act as.\n"
    "1. Clones the repository into the workspace; nothing else changes on its own.\n"
)

ANSWER_BULLETS = [
    "Scans org/repo-a for new pull requests every 30 minutes.",
    "Drafts review comments; you approve each one before it is posted.",
    "Needs read access to org/repo-a and an account to act as.",
    "Clones the repository into the workspace; nothing else changes on its own.",
]


# --------------------------------------------------------------------------- #
#  A fake Messages API client                                                  #
# --------------------------------------------------------------------------- #


class _Messages:
    def __init__(self, answer: str = ANSWER, *, stop_reason: str = "end_turn",
                 error: Exception | None = None, delay: float = 0.0) -> None:
        self.answer = answer
        self.stop_reason = stop_reason
        self.error = error
        self.delay = delay
        self.requests: list[dict] = []

    async def create(self, **kwargs):
        self.requests.append(kwargs)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error is not None:
            raise self.error
        # A 5.5 model thinks first: the text is not the first block.
        return SimpleNamespace(
            content=[
                SimpleNamespace(type="thinking", thinking=""),
                SimpleNamespace(type="text", text=self.answer),
            ],
            stop_reason=self.stop_reason,
        )


class _Client:
    def __init__(self, messages: _Messages) -> None:
        self.messages = messages
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc) -> bool:
        self.closed = True
        return False


def _config(messages: _Messages | None = None, *, credential: bool = True,
            api_key: str = "test-key", bedrock: bool = False, proxy: bool = False,
            factory_error: Exception | None = None):
    """A config whose client factory hands out a fake client; returns the
    config, the client and the factory's calls."""
    client = _Client(messages or _Messages())
    made: list[dict] = []

    def factory(**kwargs):
        made.append(kwargs)
        if factory_error is not None:
            raise factory_error
        return client

    cfg = SimpleNamespace(
        agent=SimpleNamespace(title_model="claude-haiku-5-5"),
        provider=SimpleNamespace(is_bedrock=bedrock),
        proxy=SimpleNamespace(enabled=proxy),
        credential_available=lambda: credential,
        resolve_anthropic_api_key=lambda: api_key,
        create_async_anthropic_client=factory,
    )
    return cfg, client, made


# --------------------------------------------------------------------------- #
#  The summarizer                                                              #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_answer_becomes_bullets_in_one_cheap_call():
    messages = _Messages()
    cfg, client, made = _config(messages)

    assert await summarize_plan(cfg, PLAN) == ANSWER_BULLETS

    # One attempt with the title model, bounded by the summary timeout.
    assert made == [{"timeout": SUMMARY_TIMEOUT, "max_retries": 0}]
    (request,) = messages.requests
    assert request["model"] == "claude-haiku-5-5"
    assert request["max_tokens"] == MAX_TOKENS
    for rule in ("3 to 6 bullets", "at most 25 words", "how often", "on its own",
                 "asks the person about first", "needs from the person",
                 "unusual or risky", 'starting with "- "'):
        assert rule in request["system"]
    assert request["messages"] == [
        {"role": "user", "content": f"<plan>\n{PLAN.strip()}\n</plan>"},
    ]
    assert client.closed


@pytest.mark.parametrize("text, expected", [
    ("- one\n* two\n• three\n1. four\n2) five", ["one", "two", "three", "four", "five"]),
    # An introduction, blank lines, a rule and a bold line are no bullets;
    # a nested bullet is one; an empty bullet is dropped.
    ("Summary:\n\n- one\n  - nested two\n---\n**Bold line**\n- \n-", ["one", "nested two"]),
    # A heading mark is dropped; an issue number is not a heading.
    ("- ## Jobs run every hour\n- #547 waits for review", ["Jobs run every hour", "#547 waits for review"]),
    ("-no space\n1.no space\n- spaced   out \t words ", ["spaced out words"]),
    ("", []),
])
def test_bullet_lines_are_found_and_cleaned(text, expected):
    assert parse_bullets(text) == expected


def test_at_most_six_bullets_of_at_most_25_words():
    words = [f"w{i}," if i == 24 else f"w{i}" for i in range(40)]
    long_bullet = " ".join(words)
    exact = " ".join(f"x{i}" for i in range(25))
    bullets = parse_bullets("\n".join([f"- {exact}"] + [f"- {long_bullet}"] * 8))

    assert len(bullets) == 6
    assert bullets[0] == exact  # 25 words: kept whole
    assert bullets[1] == " ".join(f"w{i}" for i in range(25)) + "…"
    assert all(len(b.split()) <= 25 for b in bullets)


@pytest.mark.asyncio
async def test_an_answer_cut_at_max_tokens_loses_its_last_line():
    answer = "- One whole bullet.\n- Another whole bullet.\n- A third that stops mid"
    cfg, _client, _made = _config(_Messages(answer, stop_reason="max_tokens"))
    assert await summarize_plan(cfg, PLAN) == ["One whole bullet.", "Another whole bullet."]


@pytest.mark.asyncio
@pytest.mark.parametrize("answer", ["- Only one bullet.", "A paragraph instead of bullets.", ""])
async def test_fewer_than_two_bullets_is_no_summary(answer):
    cfg, _client, _made = _config(_Messages(answer))
    assert await summarize_plan(cfg, PLAN) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kwargs", [
    {"credential": False, "api_key": ""},
    # A subscription token runs the Claude CLI; Nerve's own client cannot use it.
    {"credential": True, "api_key": ""},
])
async def test_no_usable_credential_is_no_summary_and_no_call(kwargs):
    cfg, _client, made = _config(**kwargs)
    assert await summarize_plan(cfg, PLAN) is None
    assert made == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kwargs", [{"bedrock": True}, {"proxy": True}])
async def test_bedrock_and_the_proxy_need_no_api_key(kwargs):
    cfg, _client, made = _config(api_key="", **kwargs)
    assert await summarize_plan(cfg, PLAN) == ANSWER_BULLETS
    assert len(made) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("plan", ["", "  \n\t", None, 7])
async def test_a_blank_plan_never_reaches_the_model(plan):
    cfg, _client, made = _config()
    assert await summarize_plan(cfg, plan) is None
    assert made == []


@pytest.mark.asyncio
async def test_a_declined_answer_is_no_summary_whatever_it_says():
    cfg, _client, _made = _config(_Messages(ANSWER, stop_reason="refusal"))
    assert await summarize_plan(cfg, PLAN) is None


@pytest.mark.asyncio
async def test_a_failed_call_is_no_summary():
    cfg, client, _made = _config(_Messages(error=RuntimeError("upstream refused")))
    assert await summarize_plan(cfg, PLAN) is None
    assert client.closed


@pytest.mark.asyncio
async def test_a_client_that_cannot_be_made_is_no_summary():
    cfg, _client, made = _config(factory_error=ValueError("no region"))
    assert await summarize_plan(cfg, PLAN) is None
    assert len(made) == 1


@pytest.mark.asyncio
async def test_a_slow_answer_is_no_summary(monkeypatch):
    monkeypatch.setattr(plan_summary, "SUMMARY_TIMEOUT", 0.05)
    cfg, client, made = _config(_Messages(delay=5))
    loop = asyncio.get_running_loop()
    started = loop.time()

    assert await summarize_plan(cfg, PLAN) is None

    assert loop.time() - started < 2
    assert made == [{"timeout": 0.05, "max_retries": 0}]
    assert client.closed  # the timeout cancelled the call and closed the client


@pytest.mark.asyncio
async def test_a_long_plan_is_cut_before_it_is_sent():
    messages = _Messages()
    cfg, _client, _made = _config(messages)
    await summarize_plan(cfg, "# Plan\n" + "x" * (3 * MAX_PLAN_CHARS))
    sent = messages.requests[0]["messages"][0]["content"]
    assert len(sent) < MAX_PLAN_CHARS + 100
    assert sent.endswith("[The rest of the plan is cut off.]\n</plan>")


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["ok", "failed", "timeout", "no credential"])
async def test_one_log_line_per_summary_and_never_the_plan(caplog, monkeypatch, outcome):
    monkeypatch.setattr(plan_summary, "SUMMARY_TIMEOUT", 0.05)
    messages = {
        "ok": _Messages(),
        "failed": _Messages(error=RuntimeError("upstream refused")),
        "timeout": _Messages(delay=5),
        "no credential": _Messages(),
    }[outcome]
    cfg, _client, _made = _config(messages, credential=outcome != "no credential")
    plan = PLAN + "\nA-LINE-THAT-MUST-NOT-BE-LOGGED\n"

    with caplog.at_level(logging.DEBUG, logger="nerve.agent.plan_summary"):
        await summarize_plan(cfg, plan, label="session s-1 (interaction abcd1234)")

    lines = [r for r in caplog.records
             if r.name == "nerve.agent.plan_summary" and r.levelno >= logging.INFO]
    assert len(lines) == 1
    message = lines[0].getMessage()
    assert message.startswith("Plan summary for session s-1 (interaction abcd1234)")
    assert ("4 bullets" in message) if outcome == "ok" else ("unavailable" in message)
    assert "A-LINE-THAT-MUST-NOT-BE-LOGGED" not in caplog.text


# --------------------------------------------------------------------------- #
#  The engine: a plan pause gets a plan_summary event                          #
# --------------------------------------------------------------------------- #


def _engine(db, tmp_path) -> AgentEngine:
    workspace = tmp_path / "ws"
    workspace.mkdir(parents=True, exist_ok=True)
    cfg = NerveConfig.from_dict({"workspace": str(workspace), "agent": {"backend": "claude"}})
    return AgentEngine(cfg, db)


async def _until(predicate, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("the condition never held")
        await asyncio.sleep(0.005)


@contextlib.asynccontextmanager
async def _listening(session_id: str):
    """What a web client bound to the session receives."""
    got: list[dict] = []

    async def _collect(_sid: str, message: dict) -> None:
        got.append(message)

    await broadcaster.register(session_id, "test-plan-summary", _collect)
    try:
        yield got
    finally:
        await broadcaster.unregister(session_id, "test-plan-summary")


async def _plan_pause(engine: AgentEngine, session_id: str, tool_input: dict):
    """An ``ExitPlanMode`` pause on a hub wired as the engine wires it (the
    same broadcaster for the pause and the summary)."""
    hub = InteractiveToolHandler(session_id, broadcaster.broadcast)
    engine._wire_interaction_hooks(session_id, hub)
    pause = asyncio.create_task(hub.request_interaction("ExitPlanMode", tool_input))
    await _until(lambda: hub.has_pending)
    return hub, pause


def _of_type(messages: list[dict], kind: str) -> list[dict]:
    return [m for m in messages if m.get("type") == kind]


@pytest.mark.asyncio
async def test_a_plan_pause_broadcasts_its_summary(db, tmp_path, monkeypatch):
    engine = _engine(db, tmp_path)
    asked: list[tuple] = []

    async def fake_summarize(config, plan_text, *, label=""):
        asked.append((config, plan_text, label))
        return list(ANSWER_BULLETS)

    monkeypatch.setattr(engine_module, "summarize_plan", fake_summarize)
    sid = "s-summary-ok"
    tool_input = {"plan": PLAN, "planFilePath": "/home/someone/.claude/plans/quiet-blue-river.md"}

    async with _listening(sid) as got:
        hub, pause = await _plan_pause(engine, sid, tool_input)
        await _until(lambda: _of_type(got, "plan_summary"))
        (interaction,) = _of_type(got, "interaction")
        (summary,) = _of_type(got, "plan_summary")
        iid = interaction["interaction_id"]

        assert summary == {
            "type": "plan_summary",
            "session_id": sid,
            "interaction_id": iid,
            "bullets": ANSWER_BULLETS,
            "status": "ok",
        }
        assert got.index(summary) > got.index(interaction)
        (call,) = asked
        assert call[0] is engine.config
        assert call[1:] == (PLAN, f"session {sid} (interaction {iid[:8]})")
        # The summary never decides anything: the pause still waits.
        assert hub.pending_interaction_types() == ["plan_exit"] and not pause.done()

        hub.resolve(iid, None)
        assert (await asyncio.wait_for(pause, 5)).approved
    assert not engine._plan_summary_tasks


@pytest.mark.asyncio
async def test_without_a_credential_the_summary_is_unavailable(db, tmp_path):
    # The real summarizer: no key in the config, none in the environment.
    engine = _engine(db, tmp_path)
    assert not engine.config.credential_available()
    sid = "s-summary-no-credential"

    async with _listening(sid) as got:
        hub, pause = await _plan_pause(engine, sid, {"plan": PLAN})
        await _until(lambda: _of_type(got, "plan_summary"))
        (interaction,) = _of_type(got, "interaction")
        assert _of_type(got, "plan_summary") == [{
            "type": "plan_summary",
            "session_id": sid,
            "interaction_id": interaction["interaction_id"],
            "bullets": [],
            "status": "unavailable",
        }]
        hub.cancel_all()
        await pause


@pytest.mark.asyncio
async def test_a_summarizer_error_is_still_an_unavailable_event(db, tmp_path, monkeypatch):
    engine = _engine(db, tmp_path)

    async def broken(*_args, **_kwargs):
        raise RuntimeError("summarizer bug")

    monkeypatch.setattr(engine_module, "summarize_plan", broken)
    sid = "s-summary-error"

    async with _listening(sid) as got:
        hub, pause = await _plan_pause(engine, sid, {"plan": PLAN})
        await _until(lambda: _of_type(got, "plan_summary"))
        (summary,) = _of_type(got, "plan_summary")
        assert (summary["status"], summary["bullets"]) == ("unavailable", [])
        assert hub.has_pending
        hub.cancel_all()
        await pause


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_input", [{}, {"plan": None}, {"plan": 7}, {"plan": "  \n"}])
async def test_a_pause_without_plan_text_is_unavailable_at_once(db, tmp_path, monkeypatch, tool_input):
    engine = _engine(db, tmp_path)
    seen: list[str] = []
    real = engine_module.summarize_plan

    async def spy(config, plan_text, **kwargs):
        seen.append(plan_text)
        return await real(config, plan_text, **kwargs)

    monkeypatch.setattr(engine_module, "summarize_plan", spy)
    sid = "s-summary-blank"

    async with _listening(sid) as got:
        hub, pause = await _plan_pause(engine, sid, tool_input)
        await _until(lambda: _of_type(got, "plan_summary"))
        (summary,) = _of_type(got, "plan_summary")
        assert (summary["status"], summary["bullets"]) == ("unavailable", [])
        # Only text reaches the summarizer, which stops before any client.
        assert seen == [tool_input.get("plan") if isinstance(tool_input.get("plan"), str) else ""]
        hub.cancel_all()
        await pause


@pytest.mark.asyncio
async def test_a_question_gets_no_summary(db, tmp_path, monkeypatch):
    engine = _engine(db, tmp_path)
    summarize = AsyncMock(return_value=["One.", "Two."])
    monkeypatch.setattr(engine_module, "summarize_plan", summarize)
    sid = "s-summary-question"

    async with _listening(sid) as got:
        hub = InteractiveToolHandler(sid, broadcaster.broadcast)
        engine._wire_interaction_hooks(sid, hub)
        pause = asyncio.create_task(hub.request_interaction("AskUserQuestion", {"questions": []}))
        await _until(lambda: _of_type(got, "interaction"))
        await asyncio.sleep(0.05)

        assert not engine._plan_summary_tasks
        summarize.assert_not_awaited()
        assert not _of_type(got, "plan_summary")
        hub.cancel_all()
        await pause


@pytest.mark.asyncio
async def test_the_summary_is_replayed_to_a_client_that_reconnects(db, tmp_path, monkeypatch):
    """A turn that runs buffers its session events for a client that binds
    later (``session_status.buffered_events``): the summary is among them,
    after the interaction it belongs to."""
    engine = _engine(db, tmp_path)
    monkeypatch.setattr(
        engine_module, "summarize_plan", AsyncMock(return_value=["One.", "Two.", "Three."]),
    )
    sid = "s-summary-replay"
    broadcaster.start_buffering(sid)
    try:
        hub, pause = await _plan_pause(engine, sid, {"plan": PLAN})
        await _until(lambda: _of_type(broadcaster.get_buffer(sid), "plan_summary"))
        buffered = broadcaster.get_buffer(sid)

        assert [m["type"] for m in buffered] == ["interaction", "plan_summary"]
        assert buffered[1]["interaction_id"] == buffered[0]["interaction_id"]
        assert buffered[1]["bullets"] == ["One.", "Two.", "Three."]
        hub.cancel_all()
        await pause
    finally:
        broadcaster.stop_buffering(sid)


@pytest.mark.asyncio
async def test_shutdown_cancels_a_summary_in_flight(db, tmp_path, monkeypatch):
    engine = _engine(db, tmp_path)
    started = asyncio.Event()

    async def endless(*_args, **_kwargs):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(engine_module, "summarize_plan", endless)
    sid = "s-summary-shutdown"

    async with _listening(sid) as got:
        hub, pause = await _plan_pause(engine, sid, {"plan": PLAN})
        await asyncio.wait_for(started.wait(), 5)
        (task,) = engine._plan_summary_tasks
        hub.cancel_all()
        await pause

        await engine.shutdown()

        assert task.cancelled()
        assert not engine._plan_summary_tasks
        assert not _of_type(got, "plan_summary")
