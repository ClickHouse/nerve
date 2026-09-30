"""Telegram question buttons answer with the full option text.

Telegram caps a button's callback_data at 64 bytes. A question button
carries its option's index (``notifopt:<id>:<index>``) and the tap handler
maps it back to the stored option, so an option of any length survives the
round trip. Approval buttons keep their short canonical value
(``notif:<id>:<value>``).

Question buttons sent before option indexes carried the option text, cut to
fit. The handler maps such a payload back to the one option it was cut from,
and marks it ``[truncated]`` when several options share the cut.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from nerve.channels.telegram import TelegramChannel, _fit_toast
from nerve.config import NerveConfig, NotificationsConfig
from nerve.notifications.service import NotificationService

from tests.actor_rows import ensure_system_principal

USER_ID = 4242
CHAT_ID = 1001

# 61 bytes: more than the 45 that a "notif:ask-xxxxxxxx:" payload leaves.
LONG_OPTION = "Keep both, and file the other two as follow-ups for next week"


@pytest_asyncio.fixture
async def db(db):  # noqa: F811 — the conftest database, with an identity
    """The conftest database after local bootstrap, plus session ``s1``."""
    await ensure_system_principal(db)
    await db.create_session("s1", actor=None)
    return db


@pytest.fixture
def bot() -> MagicMock:
    bot = MagicMock()
    bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=77))
    return bot


@pytest.fixture
def engine(bot: MagicMock) -> MagicMock:
    engine = MagicMock()
    engine.run = AsyncMock()
    telegram = MagicMock()
    telegram._app.bot = bot
    engine.router.get_channel.return_value = telegram
    return engine


@pytest.fixture
def service(tmp_path, monkeypatch, db, engine) -> NotificationService:
    # Keep the approval audit log away from any real workspace.
    monkeypatch.setenv("NERVE_WORKSPACE_PATH", str(tmp_path))
    config = NerveConfig()
    config.workspace = tmp_path
    config.notifications = NotificationsConfig(
        channels=["telegram"], telegram_chat_id=CHAT_ID,
    )
    return NotificationService(config, db, engine)


@pytest.fixture
def channel(service: NotificationService) -> TelegramChannel:
    config = NerveConfig()
    config.telegram.allowed_users = [USER_ID]
    channel = TelegramChannel(lambda: config, MagicMock())
    channel.set_notification_service(service)
    return channel


@pytest.fixture(autouse=True)
def broadcasts(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    """Capture broadcaster messages instead of hitting any WebSocket."""
    captured: list[dict] = []

    class _FakeBroadcaster:
        async def broadcast(self, _channel: str, message: dict) -> None:
            captured.append(message)

    from nerve.agent import streaming
    monkeypatch.setattr(streaming, "broadcaster", _FakeBroadcaster())
    return captured


def _buttons(bot: MagicMock) -> list:
    """The inline buttons of the last message the bot sent."""
    markup = bot.send_message.await_args.kwargs["reply_markup"]
    return [button for row in markup.inline_keyboard for button in row]


def _old_payload(notification_id: str, option: str) -> str:
    """What a question button carried before option indexes: the text, cut."""
    room = 64 - len(f"notif:{notification_id}:".encode("utf-8"))
    return option.encode("utf-8")[:room].decode("utf-8", errors="ignore")


def _tap(data: str):
    """A callback-query update for a tap on a button carrying ``data``."""
    query = SimpleNamespace(
        data=data,
        from_user=SimpleNamespace(id=USER_ID),
        message=SimpleNamespace(text="Which one?"),
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
    )
    return SimpleNamespace(callback_query=query), query


async def _ask(service: NotificationService, options: list[str]) -> str:
    result = await service.ask_question("s1", "Which one?", options=options)
    return result["notification_id"]


# ----------------------------------------------------------------------
#  Buttons
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_question_buttons_carry_the_option_index(service, bot):
    nid = await _ask(service, ["No", LONG_OPTION])

    buttons = _buttons(bot)
    assert [b.text for b in buttons] == ["No", LONG_OPTION]
    assert [b.callback_data for b in buttons] == [
        f"notifopt:{nid}:0", f"notifopt:{nid}:1",
    ]
    assert all(len(b.callback_data.encode("utf-8")) <= 64 for b in buttons)


@pytest.mark.asyncio
async def test_approval_buttons_keep_their_canonical_value(service, bot):
    result = await service.propose_action(
        session_id="s1", target_kind="test-kind", target_id="t-1",
        title="Ship it?",
    )
    nid = result["notification_id"]

    assert [b.callback_data for b in _buttons(bot)] == [
        f"notif:{nid}:approve", f"notif:{nid}:decline", f"notif:{nid}:snooze_24h",
    ]


# ----------------------------------------------------------------------
#  Taps
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_long_option_round_trips_through_its_index(
    service, channel, db, bot, engine,
):
    nid = await _ask(service, ["No", LONG_OPTION])
    assert _old_payload(nid, LONG_OPTION) != LONG_OPTION  # would have been cut

    update, query = _tap(_buttons(bot)[1].callback_data)
    await channel._handle_callback_query(update, None)

    notif = await db.get_notification(nid)
    assert notif["status"] == "answered"
    assert notif["answer"] == LONG_OPTION
    injected = engine.run.call_args.kwargs["user_message"]
    assert injected.endswith(f"\n\n{LONG_OPTION}")
    query.answer.assert_awaited_once_with(f"Answered: {LONG_OPTION}")
    edited = query.edit_message_text.await_args.kwargs["text"]
    assert edited.endswith(f"Answered: {LONG_OPTION}")
    await asyncio.sleep(0)  # let the fire-and-forget injection task finish


@pytest.mark.asyncio
async def test_approval_tap_answers_with_the_canonical_value(
    service, channel, db, bot,
):
    result = await service.propose_action(
        session_id="s1", target_kind="test-kind", target_id="t-1",
        title="Ship it?",
    )
    nid = result["notification_id"]

    update, _query = _tap(_buttons(bot)[1].callback_data)
    await channel._handle_callback_query(update, None)

    notif = await db.get_notification(nid)
    assert notif["status"] == "answered"
    assert notif["answer"] == "decline"


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", ["2", "-1", "x"])
async def test_unknown_option_index_is_rejected(
    service, channel, db, engine, payload,
):
    nid = await _ask(service, ["Yes", "No"])

    update, query = _tap(f"notifopt:{nid}:{payload}")
    await channel._handle_callback_query(update, None)

    query.answer.assert_awaited_once_with(
        "Already answered or expired", show_alert=True,
    )
    assert (await db.get_notification(nid))["status"] == "pending"
    engine.run.assert_not_called()


@pytest.mark.asyncio
async def test_option_index_of_a_missing_row_is_rejected(channel, engine):
    update, query = _tap("notifopt:ask-00000000:0")
    await channel._handle_callback_query(update, None)

    query.answer.assert_awaited_once_with(
        "Already answered or expired", show_alert=True,
    )
    engine.run.assert_not_called()


@pytest.mark.asyncio
async def test_long_answer_toast_is_capped_but_the_card_keeps_it(
    service, channel, db, bot,
):
    option = "Option " + "y" * 300
    nid = await _ask(service, [option, "No"])

    update, query = _tap(_buttons(bot)[0].callback_data)
    await channel._handle_callback_query(update, None)

    assert (await db.get_notification(nid))["answer"] == option
    toast = query.answer.await_args.args[0]
    assert len(toast) == 200
    assert toast == f"Answered: {option}"[:199] + "…"
    edited = query.edit_message_text.await_args.kwargs["text"]
    assert edited.endswith(f"Answered: {option}")


# ----------------------------------------------------------------------
#  Buttons sent before option indexes
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_old_cut_payload_resolves_to_the_full_option(
    service, channel, db, engine,
):
    nid = await _ask(service, [LONG_OPTION, "No"])

    update, _query = _tap(f"notif:{nid}:{_old_payload(nid, LONG_OPTION)}")
    await channel._handle_callback_query(update, None)

    assert (await db.get_notification(nid))["answer"] == LONG_OPTION
    assert engine.run.call_args.kwargs["user_message"].endswith(LONG_OPTION)
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_old_cut_inside_a_multibyte_character_resolves(
    service, channel, db,
):
    room = 64 - len("notif:ask-00000000:".encode("utf-8"))
    # The cut lands inside "é", so the old payload is one byte short of the room.
    option = "x" * (room - 1) + "éclair, with more text after it"
    nid = await _ask(service, [option, "No"])
    payload = _old_payload(nid, option)
    assert len(payload.encode("utf-8")) == room - 1

    update, _query = _tap(f"notif:{nid}:{payload}")
    await channel._handle_callback_query(update, None)

    assert (await db.get_notification(nid))["answer"] == option


@pytest.mark.asyncio
async def test_old_short_payload_is_the_option_itself(service, channel, db):
    nid = await _ask(service, ["Yes", "No"])

    update, _query = _tap(f"notif:{nid}:No")
    await channel._handle_callback_query(update, None)

    assert (await db.get_notification(nid))["answer"] == "No"


@pytest.mark.asyncio
async def test_ambiguous_old_cut_payload_is_flagged(service, channel, db, engine):
    first, second = f"{LONG_OPTION} (first)", f"{LONG_OPTION} (second)"
    nid = await _ask(service, [first, second])
    payload = _old_payload(nid, first)
    assert payload == _old_payload(nid, second)

    update, _query = _tap(f"notif:{nid}:{payload}")
    await channel._handle_callback_query(update, None)

    flagged = f"[truncated] {payload}"
    assert (await db.get_notification(nid))["answer"] == flagged
    assert engine.run.call_args.kwargs["user_message"].endswith(flagged)
    await asyncio.sleep(0)


# ----------------------------------------------------------------------
#  Typed answers are stored as typed
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reply_command_answer_is_never_expanded(service, channel, db):
    nid = await _ask(service, [LONG_OPTION, "No"])
    typed = _old_payload(nid, LONG_OPTION)
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=USER_ID),
        message=SimpleNamespace(reply_text=AsyncMock()),
    )

    await channel._handle_reply(update, SimpleNamespace(args=typed.split(" ")))

    assert (await db.get_notification(nid))["answer"] == typed
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_web_answer_is_never_expanded(service, db):
    nid = await _ask(service, [LONG_OPTION, "No"])
    typed = _old_payload(nid, LONG_OPTION)

    assert await service.handle_answer(nid, typed, "web")

    assert (await db.get_notification(nid))["answer"] == typed
    await asyncio.sleep(0)


# ----------------------------------------------------------------------
#  Toast fitting
# ----------------------------------------------------------------------


def test_fit_toast_keeps_text_that_fits():
    assert _fit_toast("Answered: Yes") == "Answered: Yes"
    assert _fit_toast("a" * 200) == "a" * 200


def test_fit_toast_cuts_long_text_with_an_ellipsis():
    assert _fit_toast("a" * 201) == "a" * 199 + "…"


def test_fit_toast_counts_an_emoji_as_two_units():
    toast = _fit_toast("\U0001F680" * 150)  # 150 code points, 300 UTF-16 units
    assert toast == "\U0001F680" * 99 + "…"
    assert len(toast.encode("utf-16-le")) // 2 <= 200
