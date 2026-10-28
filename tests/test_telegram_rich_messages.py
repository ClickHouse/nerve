"""Answers and plain notifications go out as Telegram rich messages.

A rich message (Bot API 10.1) renders GitHub-flavoured Markdown and holds
32,768 characters. Nerve sends the answer's Markdown through
``sendRichMessage``, adjusted so it reads the way a chat message does: lines
stay apart, ``$`` opens no formula, and a ``<`` that starts no rendered tag
stays visible. A message Telegram refuses goes out as HTML, cut into 4,096
character messages between blocks, as before.
"""

from __future__ import annotations

import random
import re
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from telegram import Chat, Message, User
from telegram.constants import ParseMode
from telegram.error import BadRequest, EndPointNotFound, RetryAfter

from nerve.channels.base import OutboundMessage
from nerve.channels.telegram import (
    RICH_MSG_LEN,
    TelegramChannel,
    _format_reply_context,
    _markdown_blocks,
    _rich_markdown,
    _rich_plain_text,
    _split_markdown,
    _weight,
)
from nerve.config import NerveConfig, NotificationsConfig
from nerve.notifications.service import NotificationService
from tests.actor_rows import ensure_system_principal

CHAT = -1001234567890
TOPIC = 42


class _Bot:
    """Records Bot API calls; ``fail`` queues an exception per method."""

    defaults = None

    def __init__(self):
        self.calls: list[tuple[str, dict, dict]] = []
        self.fail: dict[str, list[Exception]] = {}
        self.bot = SimpleNamespace(has_topics_enabled=False)
        self._next_id = 100

    def _sent(self, name: str, params: dict, options: dict):
        self.calls.append((name, params, options))
        if self.fail.get(name):
            raise self.fail[name].pop(0)
        self._next_id += 1
        return SimpleNamespace(message_id=self._next_id)

    async def do_api_request(self, endpoint, api_kwargs=None, return_type=None, **kwargs):
        return self._sent(
            endpoint, dict(api_kwargs or {}), {"return_type": return_type, **kwargs},
        )

    async def send_message(self, **kwargs):
        return self._sent("send_message", kwargs, {})

    def names(self) -> list[str]:
        return [name for name, _, _ in self.calls]

    def named(self, name: str) -> list[dict]:
        return [params for called, params, _ in self.calls if called == name]


def _channel(*, rich: bool = True) -> TelegramChannel:
    cfg = NerveConfig()
    cfg.telegram.rich_messages = rich
    ch = TelegramChannel(lambda: cfg, router=MagicMock())
    ch._app = SimpleNamespace(bot=_Bot())
    return ch


# --------------------------------------------------------------------------- #
#  The Markdown a rich message gets                                            #
# --------------------------------------------------------------------------- #


def test_a_tag_telegram_does_not_render_stays_visible():
    # Telegram drops an unknown tag with its name: this would read
    # "Use std::vector or ".
    assert _rich_markdown("Use std::vector<int> or <branch>") == (
        "Use std::vector&lt;int> or &lt;branch>"
    )


def test_tags_an_answer_uses_and_bracketed_links_are_kept():
    text = (
        "<details><summary>More</summary>\n\n"
        "Body with <u>underline</u>, <sup>2</sup> and <https://t.me/x>\n\n"
        "</details>"
    )
    assert _rich_markdown(text) == text


def test_a_dollar_sign_opens_no_formula():
    assert _rich_markdown("costs $5 and $10") == r"costs \$5 and \$10"
    assert _rich_markdown(r"already escaped \$3") == r"already escaped \$3"


def test_code_goes_out_as_written():
    text = "Run `a<b> $x` now\n\n```cpp\nstd::vector<int> v;  // $5\n\nv.clear();\n```"
    assert _rich_markdown(text) == text


def test_the_lines_of_a_paragraph_stay_apart():
    assert _rich_markdown("one\ntwo\n**Label:** three") == (
        "one  \ntwo  \n**Label:** three"
    )


def test_no_hard_break_ends_a_block_or_a_heading():
    assert _rich_markdown("# Title\ntext\n\nnext") == "# Title\ntext\n\nnext"
    assert _rich_markdown("list:\n- a\n- b") == "list:  \n- a  \n- b"


def test_a_table_next_to_text_is_set_apart():
    text = "Intro:\n| k | v |\n|---|---|\n| x | 1 |\nAfter"
    assert _rich_markdown(text) == (
        "Intro:\n\n| k | v |\n|---|---|\n| x | 1 |\n\nAfter"
    )


def test_a_table_is_escaped_but_gets_no_hard_breaks():
    text = "| k | v |\n|---|---|\n| a | $1 <b2> |\n| b | `<ok>` |"
    assert _rich_markdown(text) == (
        "| k | v |\n|---|---|\n| a | \\$1 &lt;b2> |\n| b | `<ok>` |"
    )


def test_a_rule_is_not_mistaken_for_a_table():
    # "---" alone is a rule (or a heading's underline): the lines after it
    # are no table rows, and keep their hard breaks.
    assert _rich_markdown("one\n---\ntwo\nthree") == "one  \n---\ntwo  \nthree"


# --------------------------------------------------------------------------- #
#  Splitting between blocks                                                    #
# --------------------------------------------------------------------------- #


def test_text_that_fits_is_sent_whole():
    assert _split_markdown("a\n\n\n\nb", 100) == ["a\n\n\n\nb"]
    assert _split_markdown("", 100) == []


def test_parts_end_between_blocks():
    a, b, c = "a" * 40, "b" * 40, "c" * 40
    assert _split_markdown(f"{a}\n\n{b}\n\n{c}", 90) == [f"{a}\n\n{b}", c]


def test_a_blank_line_inside_a_code_block_does_not_end_the_block():
    text = "```\nx\n\ny\n```"
    assert _markdown_blocks(f"para\n\n{text}") == ["para", text]


def test_a_long_code_block_is_closed_and_reopened_with_its_language():
    lines = [f"line_{i} = {i}" for i in range(30)]
    parts = _split_markdown("```python\n" + "\n".join(lines) + "\n```", 80)
    assert len(parts) > 1
    inner: list[str] = []
    for part in parts:
        assert len(part) <= 80
        assert part.startswith("```python\n") and part.endswith("\n```")
        inner.extend(part.split("\n")[1:-1])
    assert inner == lines


def test_a_part_holds_at_most_max_blocks():
    text = "\n".join(f"- item {i}" for i in range(10))
    parts = _split_markdown(text, 10_000, max_blocks=4)
    assert all(_weight(part) <= 4 for part in parts)
    assert "\n".join(parts).split("\n") == text.split("\n")


def test_a_long_code_block_counts_as_one_block():
    # Telegram counts a code block as one block, so a long log or diff that
    # fits in one rich message is not split for its line count.
    code = "```\n" + "\n".join(f"line {i}" for i in range(500)) + "\n```"
    assert _split_markdown(code, RICH_MSG_LEN, max_blocks=200) == [code]


def test_a_line_longer_than_a_part_is_cut():
    parts = _split_markdown("x" * 250, 100)
    assert all(len(part) <= 100 for part in parts)
    assert "".join(parts) == "x" * 250


def test_every_part_fits_and_closes_its_code_blocks():
    rng = random.Random(7)
    blocks = []
    for _ in range(80):
        kind = rng.choice(("paragraph", "code", "list"))
        if kind == "paragraph":
            blocks.append(" ".join(f"word{rng.randint(0, 99)}" for _ in range(rng.randint(1, 60))))
        elif kind == "code":
            body = "\n".join(f"x = {j}" for j in range(rng.randint(1, 40)))
            blocks.append(f"```py\n{body}\n```")
        else:
            blocks.append("\n".join(f"- item {j}" for j in range(rng.randint(1, 12))))
    text = "\n\n".join(blocks)
    for limit, max_blocks in ((120, 0), (400, 10), (4096, 0), (RICH_MSG_LEN, 40)):
        for part in _split_markdown(text, limit, max_blocks):
            assert len(part) <= limit
            assert part.count("```") % 2 == 0
            if max_blocks:
                assert _weight(part) <= max_blocks


# --------------------------------------------------------------------------- #
#  Reading a rich message back                                                 #
# --------------------------------------------------------------------------- #

# The shape Telegram returns for a sent rich message, from a live send.
_RICH = {
    "blocks": [
        {"type": "heading", "text": "Done", "size": 2},
        {"type": "paragraph", "text": ["Ran ", {"type": "code", "text": "make"}, "."]},
        {"type": "table", "cells": [
            [{"text": "k", "is_header": True}, {"text": "v", "is_header": True}],
            [{"text": "x"}, {"text": [{"type": "bold", "text": "1"}]}],
        ]},
        {"type": "list", "items": [
            {"label": "•", "blocks": [{"type": "paragraph", "text": "first"}],
             "has_checkbox": True, "is_checked": True},
        ]},
        {"type": "pre", "text": "print(1)", "language": "python"},
        {"type": "mathematical_expression", "expression": "x^2"},
        {"type": "details", "summary": "More", "blocks": [
            {"type": "paragraph", "text": "hidden"},
        ]},
    ],
}


def test_the_text_of_a_rich_message():
    message = SimpleNamespace(api_kwargs={"rich_message": _RICH})
    assert _rich_plain_text(message) == (
        "Done\nRan make.\nk | v\nx | 1\nfirst\nprint(1)\nx^2\nMore\nhidden"
    )
    assert _rich_plain_text(SimpleNamespace(api_kwargs={})) == ""
    assert _rich_plain_text(MagicMock()) == ""


def test_a_reply_to_a_rich_message_quotes_its_text():
    # python-telegram-bot keeps the field it does not know in api_kwargs.
    answer = Message.de_json({
        "message_id": 5,
        "date": 1_800_000_000,
        "chat": {"id": 1, "type": "private"},
        "from": {"id": 999, "is_bot": True, "first_name": "Nerve"},
        "rich_message": _RICH,
    }, None)
    reply = Message(
        message_id=6,
        date=datetime.now(timezone.utc),
        chat=Chat(id=1, type="private"),
        from_user=User(id=7, first_name="Seva", is_bot=False),
        text="why?",
        reply_to_message=answer,
    )
    assert _format_reply_context(reply).startswith('[Reply to assistant: "Done\nRan make.')


# --------------------------------------------------------------------------- #
#  Sending an answer                                                           #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_an_answer_goes_out_as_one_rich_message_in_its_topic():
    ch = _channel()
    bot = ch._app.bot
    text = "# Done\n| k | v |\n|---|---|\n| x | $1 |"

    await ch.send(OutboundMessage(target=f"{CHAT}:{TOPIC}", text=text))

    ((name, params, options),) = bot.calls
    assert name == "sendRichMessage"
    assert params == {
        "chat_id": CHAT,
        "message_thread_id": TOPIC,
        "rich_message": {
            "markdown": "# Done\n\n| k | v |\n|---|---|\n| x | \\$1 |",
        },
    }
    assert options["return_type"] is Message
    # The cache keeps what was sent: a rich message has no text to read back.
    assert ch._message_cache[(CHAT, bot._next_id)] == (TOPIC, text)


@pytest.mark.asyncio
async def test_an_answer_to_the_chat_carries_no_topic():
    ch = _channel()
    await ch.send(OutboundMessage(target=str(CHAT), text="hi"))
    (params,) = ch._app.bot.named("sendRichMessage")
    assert "message_thread_id" not in params


@pytest.mark.asyncio
async def test_a_refused_rich_message_goes_out_as_html():
    ch = _channel()
    bot = ch._app.bot
    bot.fail["sendRichMessage"] = [BadRequest("Rich_message_table_cols_too_many")]

    await ch.send(OutboundMessage(target=f"{CHAT}:{TOPIC}", text="**hi**"))

    assert bot.names() == ["sendRichMessage", "send_message"]
    (html,) = bot.named("send_message")
    assert html["text"] == "<b>hi</b>"
    assert html["parse_mode"] == ParseMode.HTML
    assert html["message_thread_id"] == TOPIC


@pytest.mark.asyncio
async def test_a_server_without_rich_messages_turns_them_off():
    ch = _channel()
    bot = ch._app.bot
    bot.fail["sendRichMessage"] = [
        EndPointNotFound("Endpoint 'sendRichMessage' not found in Bot API"),
    ]

    await ch.send(OutboundMessage(target=str(CHAT), text="one"))
    await ch.send(OutboundMessage(target=str(CHAT), text="two"))

    assert bot.names() == ["sendRichMessage", "send_message", "send_message"]


@pytest.mark.asyncio
async def test_a_rate_limited_rich_message_is_sent_again_after_the_wait():
    ch = _channel()
    bot = ch._app.bot
    bot.fail["sendRichMessage"] = [RetryAfter(0)]

    await ch.send(OutboundMessage(target=str(CHAT), text="answer"))

    assert bot.names() == ["sendRichMessage", "sendRichMessage"]


@pytest.mark.asyncio
async def test_a_long_answer_is_split_between_blocks():
    ch = _channel()
    paragraph = "x" * 20_000
    assert 2 * len(paragraph) > RICH_MSG_LEN

    await ch.send(OutboundMessage(target=str(CHAT), text=f"{paragraph}\n\n{paragraph}"))

    sent = [p["rich_message"]["markdown"] for p in ch._app.bot.named("sendRichMessage")]
    assert sent == [paragraph, paragraph]


@pytest.mark.asyncio
async def test_with_rich_messages_off_html_parts_keep_code_blocks_whole():
    ch = _channel(rich=False)
    bot = ch._app.bot
    code = "```py\n" + "\n".join(f"value = {i}" for i in range(1000)) + "\n```"

    await ch.send(OutboundMessage(target=str(CHAT), text=f"intro\n\n{code}"))

    sends = bot.named("send_message")
    assert not bot.named("sendRichMessage")
    assert len(sends) > 1
    assert all(html["parse_mode"] == ParseMode.HTML for html in sends)
    # Each part closes and reopens the code block, so every line of it
    # renders as code; cut at a fixed length, the middle parts had none.
    in_code = [
        line
        for html in sends
        for block in re.findall(r"<pre>(.*?)</pre>", html["text"], re.DOTALL)
        for line in block.split("\n")
        if line.startswith("value = ")
    ]
    assert len(in_code) == 1000


# --------------------------------------------------------------------------- #
#  Notifications                                                               #
# --------------------------------------------------------------------------- #


@pytest_asyncio.fixture
async def db(db):  # noqa: F811 - the conftest database, with an identity
    await ensure_system_principal(db)
    return db


@pytest.fixture
def broadcasts(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    captured: list[dict] = []

    class _FakeBroadcaster:
        async def broadcast(self, _channel: str, message: dict) -> None:
            captured.append(message)

    from nerve.agent import streaming
    monkeypatch.setattr(streaming, "broadcaster", _FakeBroadcaster())
    return captured


def _service(db, tmp_path, channel) -> NotificationService:
    config = NerveConfig()
    config.workspace = tmp_path
    config.notifications = NotificationsConfig(
        channels=["telegram"], telegram_chat_id=1001,
    )
    engine = MagicMock()
    engine.router.get_channel.return_value = channel
    return NotificationService(config, db, engine)


@pytest.mark.asyncio
async def test_a_plain_notification_is_a_rich_message(db, tmp_path, broadcasts):
    ch = _channel()
    service = _service(db, tmp_path, ch)

    await service.send_notification(
        "s1", title="Build finished", body="| job | state |\n|---|---|\n| tsan | ok |",
    )

    bot = ch._app.bot
    assert bot.names() == ["sendRichMessage"]
    (params,) = bot.named("sendRichMessage")
    assert params["chat_id"] == 1001
    assert "Build finished" in params["rich_message"]["markdown"]
    assert "| tsan | ok |" in params["rich_message"]["markdown"]


@pytest.mark.asyncio
async def test_a_notification_with_a_button_stays_html(db, tmp_path, broadcasts):
    ch = _channel()
    service = _service(db, tmp_path, ch)

    await service.send_notification(
        "s1", title="Plan ready", metadata={"plan_id": "plan-abc", "task_id": "t1"},
    )

    bot = ch._app.bot
    assert bot.names() == ["send_message"]
    assert bot.named("send_message")[0]["reply_markup"] is not None


@pytest.mark.asyncio
async def test_a_refused_rich_notification_goes_out_as_html(db, tmp_path, broadcasts):
    ch = _channel()
    ch._app.bot.fail["sendRichMessage"] = [BadRequest("Rich_message_markdown_invalid")]
    service = _service(db, tmp_path, ch)

    await service.send_notification("s1", title="Build finished")

    assert ch._app.bot.names() == ["sendRichMessage", "send_message"]


@pytest.mark.asyncio
async def test_notifications_follow_the_switch(db, tmp_path, broadcasts):
    ch = _channel(rich=False)
    service = _service(db, tmp_path, ch)

    await service.send_notification("s1", title="Build finished")

    assert ch._app.bot.names() == ["send_message"]
