"""Replies to notifications in a hosted channel.

A message that replies to a notification card goes to the session that raised
the notification, and the answer goes to the thread of the reply. The harness
is the one of ``test_hosted_channel``: the fake gateway against the real
stream endpoint, with a real router, session manager, database, and
notification service, and a stand-in for the agent turn.

Covered here: a reply in the thread under a card that is a message of the
notification conversation; a reply under a copy of a card in a direct
conversation, whose event names the card in ``reply_to``; the conversation
that keeps its own session; replies that answer no notification; a session
that is archived or deleted, before the reply and between the check and the
turn; a lookup that fails; and the lookups of the database and the
notification service on their own.
"""

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio

from nerve.agent.engine import SessionClosedError
from nerve.gateway.auth import AUTH_MODE_ENV, AUTH_MODE_EXTERNAL, pin_auth_mode

from tests.fake_channel_gateway import NOTIFICATION_CONVERSATION, message_event
from tests.test_hosted_channel import Hosted, Turns, start_hosted, stop_hosted
from tests.test_hosted_channel_actor import BOB, invoked_by, person
from tests.test_hosted_channel_behaviour import LONG_AGO, StreamingTurns, turn_of
from tests.test_hosted_channel_outbound import FIRST_MESSAGE_ID, NOTIFY_OPERATIONS, body, notify_stream

pytestmark = pytest.mark.asyncio

# The session that raises the notifications.
RAISED = "s-raised"
MEMBER_DM = "D0MEMBER"
# The thread under a copy of a card in the member's direct conversation.
COPY_THREAD = "1700000800.000100"
REPLY_ID = "1700000900.000200"
# What a notify result names when the gateway posts the card as copies in
# other conversations: no message of a conversation that Nerve sees events of.
STAND_IN = {"conversation": {"id": "card-copies"}, "message": {"id": "card-0001"}}


@pytest.fixture
def external(monkeypatch):
    """Run the test in external mode, as ``create_app()`` would pin it."""
    monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
    pin_auth_mode(AUTH_MODE_EXTERNAL)


@pytest_asyncio.fixture
async def hosted(tmp_path, db, monkeypatch):
    harness = await start_hosted(tmp_path, db, monkeypatch, notifications=True)
    await db.create_session(RAISED, source="cron", actor=None)
    try:
        yield harness
    finally:
        await stop_hosted(harness)


async def card(hosted: Hosted, *, copies: bool = False) -> tuple[str, str]:
    """Raise a notification in ``RAISED``; return the conversation and message ID of its delivery."""
    if copies:
        hosted.gateway.script("notify", {"outcome": "succeeded", "target": STAND_IN})
    notification_id = await hosted.notifications.send_notification(session_id=RAISED, title="Deploy finished")
    delivery = await hosted.db.get_latest_notification_delivery(notification_id, "slack")
    return delivery["target"], str(delivery["message_id"])


def thread_reply(conversation: str, thread: str, **changes: Any) -> dict[str, Any]:
    """A message in *thread* of a channel."""
    values: dict[str, Any] = {"text": "roll it back", "message_id": REPLY_ID}
    values.update(changes)
    return message_event(conversation=conversation, thread=thread, **values)


def direct_message(**changes: Any) -> dict[str, Any]:
    """A message in the member's direct conversation."""
    values: dict[str, Any] = {"text": "roll it back", "message_id": REPLY_ID}
    values.update(changes)
    return message_event(conversation=MEMBER_DM, conversation_kind="direct", **values)


def copy_reply(card_id: str | None, **changes: Any) -> dict[str, Any]:
    """A message in the thread under a copy of the card, which the gateway names in ``reply_to``."""
    return direct_message(thread=COPY_THREAD, reply_to=card_id, **changes)


async def deliver(hosted: Hosted, stream, event: dict[str, Any]) -> dict[str, Any]:
    """Deliver *event* and return the turn that it started."""
    before = len(hosted.turns.runs)
    inbox_id = hosted.gateway.store(event)
    await stream.nudge("invoke")
    assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
    await hosted.turns.wait_for(before + 1)
    return hosted.turns.runs[-1]


async def session_of(hosted: Hosted, channel_key: str) -> str | None:
    mapping = await hosted.db.get_channel_session(channel_key)
    return mapping["session_id"] if mapping else None


def in_thread(conversation: str, thread: str) -> dict[str, Any]:
    return {"conversation": {"id": conversation}, "thread": {"id": thread}}


async def answers(hosted: Hosted, turns: int = 1) -> list[tuple[str, dict[str, Any]]]:
    """The text and the destination of each message of *turns* streamed answers.

    A streamed answer is a placeholder, the answer, and the delete of the
    placeholder, which comes last.
    """
    await hosted.gateway.wait_for(lambda: len(hosted.gateway.sent_operations("delete")) >= turns)
    return [(body(send), send["destination"]) for send in hosted.gateway.sent_operations("send")]


# ---------------------------------------------------------------------- #
#  A card that is a message of the notification conversation              #
# ---------------------------------------------------------------------- #


class TestReplyUnderACard:
    async def test_a_thread_reply_goes_to_the_session_that_raised_the_notification(self, hosted):
        stream = await notify_stream(hosted)
        conversation, card_id = await card(hosted)
        assert (conversation, card_id) == (NOTIFICATION_CONVERSATION, FIRST_MESSAGE_ID)

        run = await turn_of(hosted, stream, thread_reply(conversation, card_id), StreamingTurns())

        assert (run["session_id"], run["user_message"], run["channel"]) == (RAISED, "roll it back", "slack")
        assert await session_of(hosted, f"slack:{conversation}:{card_id}") == RAISED

    async def test_the_answer_goes_to_the_thread_of_the_reply(self, hosted):
        stream = await notify_stream(hosted)
        conversation, card_id = await card(hosted)

        await turn_of(hosted, stream, thread_reply(conversation, card_id), StreamingTurns())

        thread = in_thread(conversation, card_id)
        assert await answers(hosted) == [("⏳", thread), ("The report is fine.", thread)]
        [typing] = hosted.gateway.sent_operations("reaction")
        assert typing["target"] == {**thread, "message": {"id": REPLY_ID}}
        [placeholder] = hosted.gateway.sent_operations("delete")
        assert placeholder["target"]["thread"] == {"id": card_id}

    async def test_every_reply_in_the_thread_finds_the_session_again(self, hosted):
        stream = await notify_stream(hosted)
        conversation, card_id = await card(hosted)
        key = f"slack:{conversation}:{card_id}"
        await deliver(hosted, stream, thread_reply(conversation, card_id))
        # Past the sticky period, and the thread's key names another session.
        await hosted.db.update_session_fields(RAISED, {"status": "idle", "last_activity_at": LONG_AGO})
        other = await hosted.router.create_session(key, source="slack")

        run = await deliver(hosted, stream, thread_reply(conversation, card_id, message_id="1700000950.000200"))

        assert run["session_id"] == RAISED != other
        assert await session_of(hosted, key) == RAISED

    async def test_a_connection_that_keeps_sessions_does_not_keep_the_reply_from_it(self, hosted):
        stream = await hosted.stream(advertise=False)
        await stream.advertise(
            operations=NOTIFY_OPERATIONS, provider="slack", behaviour={"answer": "post", "session": "keep"},
        )
        await hosted.gateway.wait_for(lambda: hosted.runtime.streams.connections("notify"))
        conversation, card_id = await card(hosted)
        kept = await hosted.router.create_session(f"slack:{conversation}:{card_id}", source="slack")

        run = await deliver(hosted, stream, thread_reply(conversation, card_id))

        assert run["session_id"] == RAISED != kept

    async def test_the_turn_runs_as_the_person_who_replied(self, external, hosted):
        stream = await notify_stream(hosted)
        conversation, card_id = await card(hosted)

        run = await deliver(hosted, stream, invoked_by(thread_reply(conversation, card_id), BOB))

        assert (run["session_id"], run["actor"]) == (RAISED, person(BOB))

    @pytest.mark.parametrize(("conversation", "thread"), [
        (NOTIFICATION_CONVERSATION, "1700000700.000100"),
        ("C_FIXTURE_CHANNEL", FIRST_MESSAGE_ID),
    ], ids=["another_thread", "another_conversation"])
    async def test_a_thread_that_no_card_roots_has_its_own_session(self, hosted, conversation, thread):
        stream = await notify_stream(hosted)
        assert await card(hosted) == (NOTIFICATION_CONVERSATION, FIRST_MESSAGE_ID)

        run = await turn_of(hosted, stream, thread_reply(conversation, thread), StreamingTurns())

        assert run["session_id"] == await session_of(hosted, f"slack:{conversation}:{thread}") != RAISED
        assert (await answers(hosted))[-1] == ("The report is fine.", in_thread(conversation, thread))

    @pytest.mark.parametrize("closed", ["archived", "deleted"])
    async def test_a_session_that_is_closed_does_not_take_the_reply(self, hosted, closed):
        stream = await notify_stream(hosted)
        conversation, card_id = await card(hosted)
        if closed == "archived":
            await hosted.db.change_session_status(RAISED, "archived", event_type="archived")
        else:
            await hosted.db.delete_session(RAISED)

        run = await deliver(hosted, stream, thread_reply(conversation, card_id))

        assert run["user_message"] == "roll it back"
        assert run["session_id"] == await session_of(hosted, f"slack:{conversation}:{card_id}") != RAISED

    async def test_a_lookup_that_fails_leaves_the_reply_for_the_next_read(self, hosted, monkeypatch):
        stream = await notify_stream(hosted)
        conversation, card_id = await card(hosted)
        calls = 0
        original = hosted.notifications.reply_session

        async def failing_once(**kwargs: Any) -> str | None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("database is locked")
            return await original(**kwargs)

        monkeypatch.setattr(hosted.notifications, "reply_session", failing_once)

        run = await deliver(hosted, stream, thread_reply(conversation, card_id))

        assert calls == 2
        assert run["session_id"] == RAISED
        assert len(hosted.turns.runs) == 1


# ---------------------------------------------------------------------- #
#  Copies of a card in direct conversations                               #
# ---------------------------------------------------------------------- #


class TestReplyUnderACopy:
    async def test_a_reply_that_names_the_card_goes_to_the_session_that_raised_the_notification(self, hosted):
        stream = await notify_stream(hosted)
        conversation, card_id = await card(hosted, copies=True)
        assert (conversation, card_id) == ("card-copies", "card-0001")

        run = await turn_of(hosted, stream, copy_reply(card_id), StreamingTurns())

        assert (run["session_id"], run["user_message"]) == (RAISED, "roll it back")
        # The thread has the session. The conversation has none.
        assert await session_of(hosted, f"slack:{MEMBER_DM}:{COPY_THREAD}") == RAISED
        assert await session_of(hosted, f"slack:{MEMBER_DM}") is None

    async def test_the_answer_goes_to_the_thread_under_the_copy(self, hosted):
        stream = await notify_stream(hosted)
        _, card_id = await card(hosted, copies=True)

        await turn_of(hosted, stream, copy_reply(card_id), StreamingTurns())

        thread = in_thread(MEMBER_DM, COPY_THREAD)
        assert await answers(hosted) == [("⏳", thread), ("The report is fine.", thread)]
        [typing] = hosted.gateway.sent_operations("reaction")
        assert typing["target"] == {**thread, "message": {"id": REPLY_ID}}

    async def test_a_reaction_and_a_file_of_the_session_go_to_the_thread_too(self, hosted, tmp_path):
        stream = await notify_stream(hosted)
        _, card_id = await card(hosted, copies=True)
        await deliver(hosted, stream, copy_reply(card_id))
        report = tmp_path / "report.txt"
        report.write_text("all green")

        assert await hosted.router.set_reaction(RAISED, "white_check_mark")
        assert await hosted.router.send_file(RAISED, str(report), channel="slack")

        thread = in_thread(MEMBER_DM, COPY_THREAD)
        [done] = [
            reaction for reaction in hosted.gateway.sent_operations("reaction")
            if reaction["reaction"]["name"] == "✅"
        ]
        assert done["target"] == {**thread, "message": {"id": REPLY_ID}}
        [upload] = hosted.gateway.sent_operations("file_send")
        assert upload["destination"] == thread

    async def test_a_message_outside_the_thread_goes_to_the_session_of_the_conversation(self, hosted):
        stream = await notify_stream(hosted)
        _, card_id = await card(hosted, copies=True)
        turns = StreamingTurns()

        before = await turn_of(hosted, stream, direct_message(
            text="good morning", message_id="1700000700.000100",
        ), turns)
        routed = await turn_of(hosted, stream, copy_reply(card_id), turns)
        after = await turn_of(hosted, stream, direct_message(
            text="and now?", message_id="1700000950.000100",
        ), turns)
        elsewhere = await turn_of(hosted, stream, direct_message(
            text="and here?", message_id="1700000960.000200", thread="1700000700.000100",
        ), turns)

        assert routed["session_id"] == RAISED
        assert before["session_id"] == after["session_id"] == elsewhere["session_id"] != RAISED
        assert await session_of(hosted, f"slack:{MEMBER_DM}") == before["session_id"]
        # Only the reply under the copy is answered in a thread.
        conversation, thread = {"conversation": {"id": MEMBER_DM}}, in_thread(MEMBER_DM, COPY_THREAD)
        assert [destination for _, destination in await answers(hosted, 4)] == [
            conversation, conversation, thread, thread, conversation, conversation, conversation, conversation,
        ]

    @pytest.mark.parametrize("reply_to", [None, "card-0002"], ids=["older_gateway", "another_message"])
    async def test_a_reply_that_names_no_card_is_a_message_of_the_conversation(self, hosted, reply_to):
        """A gateway that sends no ``reply_to``, and one that names a message that is no card."""
        stream = await notify_stream(hosted)
        await card(hosted, copies=True)

        run = await turn_of(hosted, stream, copy_reply(reply_to), StreamingTurns())

        assert run["session_id"] == await session_of(hosted, f"slack:{MEMBER_DM}") != RAISED
        assert (await answers(hosted))[-1] == ("The report is fine.", {"conversation": {"id": MEMBER_DM}})

    async def test_a_message_id_that_was_delivered_in_two_targets_names_no_notification(self, hosted):
        stream = await notify_stream(hosted)
        _, card_id = await card(hosted, copies=True)
        await hosted.db.create_notification("notif-other", RAISED, "notify", "Another")
        await hosted.db.record_notification_delivery("notif-other", "slack", target="C0ELSEWHERE", message_id=card_id)

        run = await deliver(hosted, stream, copy_reply(card_id))

        assert run["session_id"] == await session_of(hosted, f"slack:{MEMBER_DM}") != RAISED

    async def test_a_reply_to_wins_over_the_thread(self, hosted):
        """The event names the card, and its thread is the root of another card."""
        stream = await notify_stream(hosted)
        _, card_id = await card(hosted, copies=True)
        await hosted.db.create_session("s-other", actor=None)
        await hosted.db.create_notification("notif-other", "s-other", "notify", "Another")
        await hosted.db.record_notification_delivery("notif-other", "slack", target=MEMBER_DM, message_id=COPY_THREAD)

        named = await deliver(hosted, stream, copy_reply(card_id))
        unnamed = await deliver(hosted, stream, direct_message(thread=COPY_THREAD, message_id="1700000950.000200"))

        assert (named["session_id"], unnamed["session_id"]) == (RAISED, "s-other")


# ---------------------------------------------------------------------- #
#  A session that closes between the check and the turn                   #
# ---------------------------------------------------------------------- #


class ClosingTurns(Turns):
    """Turns whose notification session is archived just before its turn, which the engine then refuses."""

    def __init__(self, db: Any) -> None:
        super().__init__()
        self.db = db
        self.refused: list[str] = []

    async def run(self, **kwargs: Any) -> str:
        if kwargs["session_id"] == RAISED:
            self.refused.append(kwargs["user_message"])
            await self.db.change_session_status(RAISED, "archived", event_type="archived")
            raise SessionClosedError("archived")
        return await super().run(**kwargs)


def reaction_to(event: dict[str, Any]) -> dict[str, Any]:
    """A reaction to the message of *event*."""
    reaction = message_event(
        kind="reaction_added", conversation=event["conversation"]["id"],
        conversation_kind=event["conversation"]["kind"], message_id=event["message"]["id"],
    )
    reaction["reaction"] = {"action": "add", "name": "thumbsup"}
    return reaction


class TestSessionClosesBeforeTheTurn:
    async def test_a_reply_under_a_copy_goes_to_the_session_of_the_conversation(self, hosted):
        stream = await notify_stream(hosted)
        _, card_id = await card(hosted, copies=True)
        conversation_session = await hosted.router.create_session(f"slack:{MEMBER_DM}", source="slack")
        hosted.turns = ClosingTurns(hosted.db)
        hosted.router.engine.run = hosted.turns.run
        reply = copy_reply(card_id)

        run = await deliver(hosted, stream, reply)

        assert hosted.turns.refused == ["roll it back"]
        assert (run["session_id"], run["user_message"]) == (conversation_session, "roll it back")
        assert len(hosted.turns.runs) == 1
        # A reaction to the reply goes where the reply went.
        reaction = await deliver(hosted, stream, reaction_to(reply))
        assert reaction["session_id"] == conversation_session
        assert reaction["user_message"] == '[Reaction: thumbsup on message: "roll it back"]'

    async def test_a_session_deleted_while_the_turn_starts_does_not_lose_the_reply(self, hosted, monkeypatch):
        stream = await notify_stream(hosted)
        _, card_id = await card(hosted, copies=True)
        resolve = hosted.router.actor_for_principal

        async def deleting(*args: Any, **kwargs: Any) -> Any:
            # After the check of the session, and before the router gets the message.
            await hosted.db.delete_session(RAISED)
            return await resolve(*args, **kwargs)

        monkeypatch.setattr(hosted.router, "actor_for_principal", deleting)

        run = await deliver(hosted, stream, copy_reply(card_id))

        assert run["user_message"] == "roll it back"
        assert run["session_id"] == await session_of(hosted, f"slack:{MEMBER_DM}") != RAISED
        assert await session_of(hosted, f"slack:{MEMBER_DM}:{COPY_THREAD}") is None

    async def test_a_reply_under_a_card_starts_a_session_for_its_thread(self, hosted):
        stream = await notify_stream(hosted)
        conversation, card_id = await card(hosted)
        hosted.turns = ClosingTurns(hosted.db)
        hosted.router.engine.run = hosted.turns.run

        run = await deliver(hosted, stream, thread_reply(conversation, card_id))

        assert hosted.turns.refused == ["roll it back"]
        assert run["user_message"] == "roll it back"
        assert run["session_id"] == await session_of(hosted, f"slack:{conversation}:{card_id}") != RAISED
        assert len(hosted.turns.runs) == 1


# ---------------------------------------------------------------------- #
#  The lookups                                                            #
# ---------------------------------------------------------------------- #


class TestLookups:
    async def test_a_message_id_finds_its_notification_in_whichever_target(self, db):
        await db.create_notification("notif-1", RAISED, "notify", "Deploy finished")
        await db.record_notification_delivery("notif-1", "slack", target="card-copies", message_id="card-0001")

        found = await db.find_notification_for_message("slack", "card-0001")

        assert (found["id"], found["session_id"]) == ("notif-1", RAISED)
        assert await db.find_notification_for_message("slack", "card-0002") is None
        assert await db.find_notification_for_message("telegram", "card-0001") is None
        assert await db.find_notification_for_message("slack", "") is None

    async def test_a_message_id_in_two_targets_finds_nothing(self, db):
        for notification_id, target in (("notif-1", "C0FIRST"), ("notif-2", "C0SECOND")):
            await db.create_notification(notification_id, RAISED, "notify", "Deploy finished")
            await db.record_notification_delivery(notification_id, "slack", target=target, message_id="1.1")

        assert await db.find_notification_for_message("slack", "1.1") is None
        assert (await db.find_notification_for_delivery("slack", "C0FIRST", "1.1"))["id"] == "notif-1"

    async def test_the_service_names_the_session_only_while_it_can_take_a_reply(self, hosted):
        service, db = hosted.notifications, hosted.db
        await db.create_notification("notif-1", RAISED, "notify", "Deploy finished")
        await db.record_notification_delivery("notif-1", "slack", target="C0FIRST", message_id="1.1")

        assert await service.reply_session(channel="slack", message_id="1.1") == RAISED
        assert await service.reply_session(channel="slack", message_id="1.1", target="C0FIRST") == RAISED
        assert await service.reply_session(channel="slack", message_id="1.1", target="C0SECOND") is None
        assert await service.reply_session(channel="slack", message_id="1.2") is None

        await db.change_session_status(RAISED, "archived", event_type="archived")
        assert await service.reply_session(channel="slack", message_id="1.1") is None
        await db.delete_session(RAISED)
        assert await service.reply_session(channel="slack", message_id="1.1", target="C0FIRST") is None
