"""What ``/stop`` answers in the chat channels.

``/stop`` stops the turn only; background agents keep running. When the
runtime cannot spare its agents the turn is left running (``needs_confirmation``)
and the answer says why instead of "nothing running".
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from nerve.channels.router import ChannelRouter, turn_stop_note
from nerve.channels.telegram import TelegramChannel

_RESULT = {"scope": "turn", "turn": "stopped", "would_end": [], "shell_count": 0}
_BRIEF = {"id": "ag_000000000001", "label": "build-api", "kind": "agent",
          "status": "running", "started_at": None, "tokens": 0}


def test_a_stopped_turn_or_an_idle_session_needs_no_note():
    for turn in ("stopped", "killed", "not_running"):
        assert turn_stop_note({**_RESULT, "turn": turn}, "s-1234") is None


def test_a_runtime_that_cannot_spare_its_agents_says_so():
    one = turn_stop_note({**_RESULT, "turn": "needs_confirmation", "would_end": [_BRIEF]}, "s-1234")
    assert one == (
        "Couldn't stop `s-1234` without stopping its 1 background agent. "
        "Stop it from the web UI."
    )
    two = turn_stop_note(
        {**_RESULT, "turn": "needs_confirmation", "would_end": [_BRIEF, _BRIEF]}, "s-1234",
    )
    assert "its 2 background agents. Stop them from the web UI." in two


def test_a_turn_that_did_not_stop_points_to_force_stop():
    note = turn_stop_note({**_RESULT, "turn": "not_stopped"}, "s-1234")
    assert "didn't stop within 5 s" in note and "Force stop" in note


@pytest.mark.asyncio
async def test_the_router_returns_the_engines_turn_result():
    engine = SimpleNamespace(stop_turn=AsyncMock(return_value=_RESULT))
    router = ChannelRouter(engine)
    assert await router.stop_turn("s-1234") == _RESULT
    engine.stop_turn.assert_awaited_once()
    assert engine.stop_turn.await_args.args == ("s-1234",)


def _telegram(result):
    ch = TelegramChannel.__new__(TelegramChannel)
    ch._touch = lambda: None
    ch._is_authorized = lambda _uid: True
    ch.router = SimpleNamespace(
        get_last_session=AsyncMock(return_value="s-1234"),
        stop_turn=AsyncMock(return_value=result),
    )
    return ch


async def _telegram_says(result) -> str:
    ch = _telegram(result)
    reply = AsyncMock()
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=4242),
        effective_chat=SimpleNamespace(id=4242),
        message=SimpleNamespace(reply_text=reply),
    )
    await ch._handle_stop(update, SimpleNamespace(args=[]))
    ch.router.stop_turn.assert_awaited_once_with("s-1234")
    return reply.await_args.args[0]


@pytest.mark.asyncio
async def test_telegram_stop_answers():
    assert await _telegram_says(_RESULT) == "Stopped session `s-1234`."
    assert await _telegram_says({**_RESULT, "turn": "killed"}) == "Stopped session `s-1234`."
    assert await _telegram_says({**_RESULT, "turn": "not_running"}) == "Nothing running to stop."
    degraded = await _telegram_says({**_RESULT, "turn": "needs_confirmation", "would_end": [_BRIEF]})
    assert degraded.startswith("Couldn't stop `s-1234` without stopping its 1 background agent.")
    wedged = await _telegram_says({**_RESULT, "turn": "not_stopped"})
    assert "didn't stop within 5 s" in wedged
