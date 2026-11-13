"""A literal ``\\n`` a model writes into notify / ask_user text becomes a line break."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.tools.handlers.notifications import (
    _unescape_newlines,
    ask_user_handler,
    notify_handler,
    propose_action_handler,
)
from nerve.agent.tools.registry import ToolContext


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("card X\\n\\nIs alice a colleague?", "card X\n\nIs alice a colleague?"),
        ("a\\r\\nb", "a\nb"),
        ("already\nfine", "already\nfine"),
        ("mixed\nreal\\nescaped", "mixed\nreal\nescaped"),
        ("rejects `\\r` and `\\n` in headers", "rejects `\\r` and `\\n` in headers"),
        ("before\\n`code \\n`\\nafter", "before\n`code \\n`\nafter"),
        ("x\\n```\nprint('a\\n')\n```\\ny", "x\n```\nprint('a\\n')\n```\ny"),
        ("", ""),
        (None, None),
    ],
)
def test_unescape_newlines(raw, expected):
    assert _unescape_newlines(raw) == expected


def _ctx() -> tuple[ToolContext, MagicMock]:
    svc = MagicMock()
    svc.send_notification = AsyncMock(return_value="notif-1")
    svc.ask_question = AsyncMock(return_value={"notification_id": "ask-1"})
    svc.propose_action = AsyncMock(return_value={"notification_id": "act-1"})
    svc.db.get_notification = AsyncMock(return_value=None)
    return ToolContext(session_id="s1", notification_service=svc), svc


@pytest.mark.asyncio
async def test_notify_unescapes_title_and_body():
    ctx, svc = _ctx()
    await notify_handler(ctx, {"title": "T\\nU", "body": "a\\n\\nb"})
    kw = svc.send_notification.await_args.kwargs
    assert kw["title"] == "T\nU"
    assert kw["body"] == "a\n\nb"


@pytest.mark.asyncio
async def test_ask_user_unescapes_body():
    ctx, svc = _ctx()
    await ask_user_handler(ctx, {"title": "Q", "body": "card X\\n\\nIs alice a colleague?"})
    assert svc.ask_question.await_args.kwargs["body"] == "card X\n\nIs alice a colleague?"


@pytest.mark.asyncio
async def test_propose_action_unescapes_body():
    ctx, svc = _ctx()
    await propose_action_handler(
        ctx, {"target_kind": "k", "target_id": "1", "title": "T", "body": "a\\nb"},
    )
    assert svc.propose_action.await_args.kwargs["body"] == "a\nb"
