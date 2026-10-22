"""Tests for the interactive tool handler: timeout, waiting-input indicator,
plan mode (entering never pauses; ExitPlanMode waits for the decision) and
what a session waits for.

The hub itself is backend-neutral; the SDK ``can_use_tool`` surface lives in
``ClaudeToolPermissions``, which translates the hub's ``InteractionOutcome``
into ``PermissionResultAllow``/``PermissionResultDeny``.
"""

from __future__ import annotations

import asyncio

import pytest
from claude_agent_sdk.types import PermissionResultAllow, PermissionResultDeny

from nerve.agent.backends.claude import ClaudeToolPermissions
from nerve.agent.interactive import (
    INTERACTION_TIMEOUT,
    InteractiveToolHandler,
    _humanize_seconds,
    get_awaiting_ids,
    register_handler,
    unregister_handler,
)


def test_interaction_timeout_is_one_hour():
    """AskUserQuestion / plan-mode waits give the user a full hour to respond."""
    assert INTERACTION_TIMEOUT == 3600


@pytest.mark.parametrize(
    "seconds,expected",
    [
        (3600, "1 hour"),
        (7200, "2 hours"),
        (300, "5 minutes"),
        (60, "1 minute"),
        (90, "1 minute"),  # floor division -> 1
        (5400, "90 minutes"),  # not a whole number of hours
    ],
)
def test_humanize_seconds(seconds, expected):
    assert _humanize_seconds(seconds) == expected


async def _wait_until_pending(handler: InteractiveToolHandler) -> None:
    """Spin briefly until the handler registers its pending interaction."""
    for _ in range(200):
        if handler.has_pending:
            return
        await asyncio.sleep(0.005)
    raise AssertionError("handler never became pending")


@pytest.mark.asyncio
async def test_awaiting_input_broadcast_and_registry():
    """An interactive wait sets the registry flag and broadcasts awaiting=true,
    then clears both once the user answers."""
    messages: list[tuple[str, dict]] = []

    async def broadcast(channel: str, msg: dict) -> None:
        messages.append((channel, msg))

    handler = InteractiveToolHandler("sess-await", broadcast, interactive_capable=True)
    register_handler("sess-await", handler)
    try:
        task = asyncio.create_task(
            ClaudeToolPermissions(handler).can_use_tool(
                "AskUserQuestion", {"questions": []}, None,
            )
        )
        await _wait_until_pending(handler)

        # Registry reports the session as waiting for input.
        assert "sess-await" in get_awaiting_ids()

        # A global awaiting=true broadcast was emitted.
        awaiting = [m for (_c, m) in messages if m["type"] == "session_awaiting_input"]
        assert awaiting and awaiting[-1]["awaiting"] is True
        assert any(c == "__global__" for (c, m) in messages if m["type"] == "session_awaiting_input")

        # Resolve with an answer.
        interaction = next(m for (_c, m) in messages if m["type"] == "interaction")
        assert handler.resolve(interaction["interaction_id"], {"q": "a"})
        result = await task

        assert isinstance(result, PermissionResultAllow)
        assert result.updated_input == {"questions": [], "answers": {"q": "a"}}

        # Waiting state cleared in registry and via a final awaiting=false broadcast.
        assert not handler.has_pending
        assert "sess-await" not in get_awaiting_ids()
        awaiting = [m for (_c, m) in messages if m["type"] == "session_awaiting_input"]
        assert awaiting[-1]["awaiting"] is False
    finally:
        unregister_handler("sess-await")


@pytest.mark.asyncio
async def test_resolve_broadcasts_interaction_resolved():
    """Resolving emits a session-scoped interaction_resolved event so parallel
    clients clear their pending poll/plan prompt instead of re-prompting."""
    messages: list[tuple[str, dict]] = []

    async def broadcast(channel: str, msg: dict) -> None:
        messages.append((channel, msg))

    handler = InteractiveToolHandler("sess-resolved", broadcast, interactive_capable=True)
    register_handler("sess-resolved", handler)
    try:
        task = asyncio.create_task(
            ClaudeToolPermissions(handler).can_use_tool(
                "AskUserQuestion", {"questions": []}, None,
            )
        )
        await _wait_until_pending(handler)
        interaction = next(m for (_c, m) in messages if m["type"] == "interaction")

        assert handler.resolve(interaction["interaction_id"], {"q": "a"})
        await task

        resolved = [(c, m) for (c, m) in messages if m["type"] == "interaction_resolved"]
        assert resolved, "expected an interaction_resolved broadcast"
        channel, msg = resolved[-1]
        assert channel == "sess-resolved"
        assert msg["session_id"] == "sess-resolved"
        assert msg["interaction_id"] == interaction["interaction_id"]
    finally:
        unregister_handler("sess-resolved")


@pytest.mark.asyncio
async def test_cancel_all_clears_awaiting():
    """Stopping a session mid-wait denies the interaction and clears the flag."""
    messages: list[tuple[str, dict]] = []

    async def broadcast(channel: str, msg: dict) -> None:
        messages.append((channel, msg))

    handler = InteractiveToolHandler("sess-cancel", broadcast, interactive_capable=True)
    register_handler("sess-cancel", handler)
    try:
        task = asyncio.create_task(
            ClaudeToolPermissions(handler).can_use_tool("ExitPlanMode", {}, None)
        )
        await _wait_until_pending(handler)
        assert "sess-cancel" in get_awaiting_ids()

        handler.cancel_all()
        result = await task

        assert isinstance(result, PermissionResultDeny)
        assert not handler.has_pending
        assert "sess-cancel" not in get_awaiting_ids()
        awaiting = [m for (_c, m) in messages if m["type"] == "session_awaiting_input"]
        assert awaiting[-1]["awaiting"] is False
    finally:
        unregister_handler("sess-cancel")


# ------------------------------------------------------------------ #
#  Plan mode: entering never pauses, the decision is ExitPlanMode's    #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_enter_plan_mode_is_allowed_without_a_pause():
    """Entering plan mode only restricts the agent: on an interactive channel
    it is allowed at once, with no card, no waiting indicator."""
    messages: list[tuple[str, dict]] = []

    async def broadcast(channel: str, msg: dict) -> None:
        messages.append((channel, msg))

    handler = InteractiveToolHandler("sess-enter", broadcast, interactive_capable=True)
    register_handler("sess-enter", handler)
    try:
        result = await asyncio.wait_for(
            ClaudeToolPermissions(handler).can_use_tool("EnterPlanMode", {}, None), timeout=1,
        )
        assert isinstance(result, PermissionResultAllow)
        assert result.updated_input is None
        assert messages == []
        assert not handler.has_pending and handler.pending_interaction_types() == []
        assert "sess-enter" not in get_awaiting_ids()
    finally:
        unregister_handler("sess-enter")


@pytest.mark.asyncio
async def test_plan_mode_is_still_denied_on_a_non_interactive_channel():
    async def broadcast(channel: str, msg: dict) -> None:  # pragma: no cover - never called
        raise AssertionError("a non-interactive channel never broadcasts a pause")

    handler = InteractiveToolHandler("sess-cron", broadcast, interactive_capable=False)
    permissions = ClaudeToolPermissions(handler)
    enter = await permissions.can_use_tool("EnterPlanMode", {}, None)
    exit_ = await permissions.can_use_tool("ExitPlanMode", {"plan": "x"}, None)
    assert isinstance(enter, PermissionResultDeny) and "Plan mode is not available" in enter.message
    assert isinstance(exit_, PermissionResultDeny) and "Plan mode is not available" in exit_.message


@pytest.mark.asyncio
async def test_exit_plan_mode_pauses_and_a_change_request_carries_its_words():
    """ExitPlanMode waits for the person's decision; "Request changes" denies
    with their words, which reach the agent as the refusal's message."""
    messages: list[tuple[str, dict]] = []

    async def broadcast(channel: str, msg: dict) -> None:
        messages.append((channel, msg))

    handler = InteractiveToolHandler("sess-plan", broadcast, interactive_capable=True)
    register_handler("sess-plan", handler)
    try:
        task = asyncio.create_task(
            ClaudeToolPermissions(handler).can_use_tool("ExitPlanMode", {}, None)
        )
        await _wait_until_pending(handler)
        interaction = next(m for (_c, m) in messages if m["type"] == "interaction")
        assert (interaction["interaction_type"], interaction["tool_name"]) == ("plan_exit", "ExitPlanMode")
        assert handler.pending_interaction_types() == ["plan_exit"]

        words = "Scan every 15 minutes, and leave org/repo-b out."
        assert handler.deny(interaction["interaction_id"], words)
        result = await task
        assert isinstance(result, PermissionResultDeny)
        assert result.message == words and not result.interrupt
        assert handler.pending_interaction_types() == []
    finally:
        unregister_handler("sess-plan")


@pytest.mark.asyncio
async def test_pending_interaction_types_name_what_waits_oldest_first():
    async def broadcast(channel: str, msg: dict) -> None:
        pass

    handler = InteractiveToolHandler("sess-types", broadcast, interactive_capable=True)
    assert handler.pending_interaction_types() == []
    question = asyncio.create_task(handler.request_interaction("AskUserQuestion", {"questions": []}))
    await _wait_until_pending(handler)
    approval = asyncio.create_task(handler.request_approval("command_approval", {"command": "ls"}))
    for _ in range(200):
        if len(handler.pending_interaction_types()) == 2:
            break
        await asyncio.sleep(0.005)
    assert handler.pending_interaction_types() == ["question", "command_approval"]

    handler.cancel_all()
    assert (await question).cancelled is False and (await question).denied
    assert (await approval).denied
    assert handler.pending_interaction_types() == []


PLAN_INPUT = {
    "plan": "# Setup proposal — review PRs in org/repo-a\n## Jobs\n| pr-scan | */30 * * * * |\n",
    "planFilePath": "/home/someone/.claude/plans/quiet-blue-river.md",
}


async def _decide_plan(answer=None, *, deny: str | None = None) -> tuple[dict, object]:
    """Pause on ExitPlanMode with the CLI's input (``plan``, ``planFilePath``),
    answer it like the web UI does, return the broadcast and the decision."""
    messages: list[tuple[str, dict]] = []

    async def broadcast(channel: str, msg: dict) -> None:
        messages.append((channel, msg))

    handler = InteractiveToolHandler("sess-decide", broadcast, interactive_capable=True)
    register_handler("sess-decide", handler)
    try:
        task = asyncio.create_task(
            ClaudeToolPermissions(handler).can_use_tool("ExitPlanMode", dict(PLAN_INPUT), None)
        )
        await _wait_until_pending(handler)
        interaction = next(m for (_c, m) in messages if m["type"] == "interaction")
        if deny is not None:
            handler.deny(interaction["interaction_id"], deny)
        else:
            handler.resolve(interaction["interaction_id"], answer)
        return interaction, await task
    finally:
        unregister_handler("sess-decide")


@pytest.mark.asyncio
async def test_the_plan_pause_carries_the_clis_input_unchanged():
    """The CLI puts the plan file's text into ExitPlanMode's input; the
    interaction event hands it to the UI as is, so the card can show it."""
    interaction, result = await _decide_plan(None)
    assert interaction["tool_input"] == PLAN_INPUT
    assert isinstance(result, PermissionResultAllow) and result.updated_input is None


@pytest.mark.asyncio
async def test_a_plan_approved_with_the_persons_edits_goes_back_edited():
    edited = PLAN_INPUT["plan"].replace("*/30", "*/15")
    _interaction, result = await _decide_plan({"plan": edited})
    assert isinstance(result, PermissionResultAllow)
    assert result.updated_input == {**PLAN_INPUT, "plan": edited}


@pytest.mark.asyncio
@pytest.mark.parametrize("answer", [{}, {"plan": "  \n"}, {"plan": 3}, {"other": "x"}])
async def test_anything_but_an_edited_plan_is_a_plain_approval(answer):
    _interaction, result = await _decide_plan(answer)
    assert isinstance(result, PermissionResultAllow) and result.updated_input is None
