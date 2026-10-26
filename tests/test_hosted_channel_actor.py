"""Who a hosted channel turn runs as.

An invoke event names the person that the channel gateway resolved from its
author. In external mode, the router resolves that person to a human actor
(``ChannelRouter.actor_for_principal``), the hosted channel puts it on the
message, and the router runs the turn as that actor: the session that the
turn creates and the stored message carry it. The session's cause is then
the principal that its model requests name. A turn in an existing session
keeps the session's cause. An interaction answers its notification as that
person. In local mode, a nil principal, or the system actor's ID, the turn
runs without an actor. Self-hosted channels set no actor.

A batch of messages for one session runs as one turn for each consecutive run
of messages from one actor.

The author of an event can also name the person that the gateway linked to
it. That person is attribution only. An observation stores and shows it, but
the sender rules ignore it and it adds no actor. On an invoke event, its
display name fills a missing name of the resolved person, only when it is
that person.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

import nerve.config as config_module
from nerve.agent.backends import claude as claude_backend
from nerve.agent.engine import AgentEngine
from nerve.channels.base import ChannelCapability, InboundMessage
from nerve.channels.hosted.runtime import HostedChannelRuntime
from nerve.channels.router import ChannelRouter
from nerve.config import NerveConfig, set_config
from nerve.gateway import server as gateway_server
from nerve.gateway.auth import AUTH_MODE_ENV, AUTH_MODE_EXTERNAL, pin_auth_mode
from nerve.identity import ACTOR_KIND_HUMAN, Actor
from nerve.inference_principal import CUSTOM_HEADERS_ENV
from nerve.sources.registry import build_source_runners

from tests.fake_channel_gateway import EVENT_TIMEOUT, FakeChannelGateway, NerveServer, message_event
from tests.test_hosted_channel import (
    FAST_OPERATIONS,
    FAST_READER,
    FAST_STREAM,
    Hosted,
    hosted_config,
    start_hosted,
    stop_hosted,
)
from tests.test_hosted_channel_outbound import TOOL_TARGET, interaction_event, question_card
from tests.test_inference_principal import _FakeCli, _principals

pytestmark = pytest.mark.asyncio

ADA = "6f1c2a8e-3b4d-4e5f-8a9b-0c1d2e3f4a5b"
BOB = "0a9b8c7d-6e5f-4a3b-9c2d-1e0f9a8b7c6d"
NIL = "00000000-0000-0000-0000-000000000000"


@pytest.fixture
def external(monkeypatch):
    """Run the test in external mode, as ``create_app()`` would pin it."""
    monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
    pin_auth_mode(AUTH_MODE_EXTERNAL)


def person(actor_id: str, display_name: str | None = None) -> Actor:
    return Actor(actor_id=actor_id, kind=ACTOR_KIND_HUMAN, account_id=None, display_name=display_name)


def invoked_by(event: dict[str, Any], principal_id: str) -> dict[str, Any]:
    event["admission"]["invoke"]["resolved_principal_id"] = principal_id
    return event


async def humans(db) -> list[str]:
    return [row["id"] for row in await db.list_actor_refs(kind=ACTOR_KIND_HUMAN)]


# ---------------------------------------------------------------------- #
#  The router resolves the person                                         #
# ---------------------------------------------------------------------- #


def bare_router(db) -> ChannelRouter:
    engine = MagicMock()
    engine.db = db
    return ChannelRouter(engine)


class TestActorForPrincipal:
    async def test_a_new_person_gets_a_human_row_without_a_name(self, db, external):
        actor = await bare_router(db).actor_for_principal(uuid.UUID(ADA))

        assert actor == person(ADA)
        row = await db.get_actor_ref(ADA)
        assert (row["kind"], row["display_name"]) == (ACTOR_KIND_HUMAN, None)

    async def test_the_name_from_a_web_request_is_kept(self, db, external):
        await db.upsert_external_actor(ADA, "Ada")

        actor = await bare_router(db).actor_for_principal(uuid.UUID(ADA))

        assert actor == person(ADA, "Ada")
        assert (await db.get_actor_ref(ADA))["display_name"] == "Ada"

    async def test_a_nil_principal_has_no_actor(self, db, external, caplog):
        before = await humans(db)

        with caplog.at_level(logging.WARNING, logger="nerve.channels.router"):
            actor = await bare_router(db).actor_for_principal(uuid.UUID(NIL))

        assert actor is None
        assert await humans(db) == before
        assert "names no person" in caplog.text

    async def test_the_system_actor_has_no_actor_and_its_id_is_not_logged(self, db, external, caplog):
        with caplog.at_level(logging.WARNING, logger="nerve.channels.router"):
            actor = await bare_router(db).actor_for_principal(uuid.UUID(db.system_actor_id))

        assert actor is None
        assert (await db.get_actor_ref(db.system_actor_id))["kind"] == "system"
        assert "system actor" in caplog.text
        assert db.system_actor_id not in caplog.text

    async def test_a_failed_write_has_no_actor(self, db, external, monkeypatch, caplog):
        monkeypatch.setattr(db, "ensure_external_actor", AsyncMock(side_effect=RuntimeError("disk full")))

        with caplog.at_level(logging.WARNING, logger="nerve.channels.router"):
            actor = await bare_router(db).actor_for_principal(uuid.UUID(ADA))

        assert actor is None
        assert "not resolved" in caplog.text
        assert ADA not in caplog.text

    async def test_local_mode_has_no_gateway_principals(self, db):
        actor = await bare_router(db).actor_for_principal(uuid.UUID(ADA))

        assert actor is None
        assert await db.get_actor_ref(ADA) is None

    async def test_the_event_name_names_a_new_person(self, db, external):
        actor = await bare_router(db).actor_for_principal(uuid.UUID(ADA), display_name="Ada Lovelace")

        assert actor == person(ADA, "Ada Lovelace")
        assert (await db.get_actor_ref(ADA))["display_name"] == "Ada Lovelace"

    async def test_the_event_name_fills_a_missing_name(self, db, external):
        await bare_router(db).actor_for_principal(uuid.UUID(ADA))

        actor = await bare_router(db).actor_for_principal(uuid.UUID(ADA), display_name="Ada Lovelace")

        assert actor == person(ADA, "Ada Lovelace")

    async def test_the_event_name_never_replaces_a_stored_name(self, db, external):
        router = bare_router(db)
        await db.upsert_external_actor(ADA, "Ada")
        await router.actor_for_principal(uuid.UUID(BOB), display_name="Bob")

        ada = await router.actor_for_principal(uuid.UUID(ADA), display_name="Ada Lovelace")
        bob = await router.actor_for_principal(uuid.UUID(BOB), display_name="Robert")

        assert (ada, bob) == (person(ADA, "Ada"), person(BOB, "Bob"))

    async def test_a_web_request_replaces_the_event_name(self, db, external):
        await bare_router(db).actor_for_principal(uuid.UUID(ADA), display_name="Ada Lovelace")

        await db.upsert_external_actor(ADA, "Ada")

        assert (await db.get_actor_ref(ADA))["display_name"] == "Ada"

    async def test_the_system_actor_keeps_its_name(self, db, external):
        before = await db.get_actor_ref(db.system_actor_id)

        actor = await bare_router(db).actor_for_principal(uuid.UUID(db.system_actor_id), display_name="Mallory")

        assert actor is None
        assert await db.get_actor_ref(db.system_actor_id) == before


# ---------------------------------------------------------------------- #
#  The session and the message carry the actor                            #
# ---------------------------------------------------------------------- #


@pytest_asyncio.fixture
async def engine(tmp_path, db, monkeypatch):
    """A real engine and router whose turns store the message and then fail."""
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
    })
    set_config(config)
    engine = AgentEngine(config, db)

    async def no_model(*args, **kwargs):
        raise RuntimeError("no model in this test")

    async def noop(*args, **kwargs):
        return None

    monkeypatch.setattr(engine, "_get_or_create_client", no_model)
    router = engine.router
    router._channels["slack"] = SimpleNamespace(
        name="slack", capabilities=set(), format_response=lambda text: text,
    )
    monkeypatch.setattr(router, "_setup_streaming", noop)
    monkeypatch.setattr(router, "_teardown_streaming", noop)
    monkeypatch.setattr(router, "BATCH_DEBOUNCE", 0)
    try:
        yield engine
    finally:
        set_config(NerveConfig())


async def said_in(db, session_id: str) -> list[tuple[str, str | None]]:
    rows = await db.get_messages(session_id)
    return [(row["content"], row["actor_id"]) for row in rows if row["role"] == "user"]


class TestStoredAttribution:
    async def test_a_hosted_message_creates_the_session_and_the_message_as_its_person(
        self, engine, db, external,
    ):
        actor = await engine.router.actor_for_principal(uuid.UUID(ADA))

        await engine.router.handle_message(InboundMessage(
            channel_name="slack", channel_key="slack:D0DIRECT", sender_id="D0DIRECT",
            text="what is on today?", actor=actor,
        ))

        session_id = await engine.sessions.get_last_session("slack:D0DIRECT")
        session = await db.get_session(session_id)
        assert (session["created_by_actor_id"], session["caused_by_actor_id"]) == (ADA, ADA)
        assert await said_in(db, session_id) == [("what is on today?", ADA)]

    async def test_a_message_without_an_actor_stays_unattributed_in_external_mode(
        self, engine, db, external,
    ):
        await engine.router.handle_message(InboundMessage(
            channel_name="slack", channel_key="slack:D0OTHER", sender_id="D0OTHER", text="hello",
        ))

        session_id = await engine.sessions.get_last_session("slack:D0OTHER")
        session = await db.get_session(session_id)
        assert (session["created_by_actor_id"], session["caused_by_actor_id"]) == (None, None)
        assert await said_in(db, session_id) == [("hello", None)]


# ---------------------------------------------------------------------- #
#  Batches split by actor                                                 #
# ---------------------------------------------------------------------- #


class Recorder:
    """The engine's run: records each turn and replies with its text."""

    def __init__(self, fail_for: str | None = None) -> None:
        self.turns: list[tuple[str, str | None]] = []
        self.fail_for = fail_for

    async def run(self, **kwargs: Any) -> str:
        actor = kwargs["actor"]
        self.turns.append((kwargs["user_message"], actor.actor_id if actor else None))
        if actor is not None and actor.actor_id == self.fail_for:
            raise RuntimeError("the turn failed")
        return f"reply to {kwargs['user_message']}"


def batching_router(recorder: Recorder, channel: Any = None) -> ChannelRouter:
    engine = MagicMock()
    engine.sessions.get_active_session = AsyncMock(return_value="s1")
    engine.run = recorder.run
    router = ChannelRouter(engine)
    router.BATCH_DEBOUNCE = 0.05
    router._channels["slack"] = channel or SimpleNamespace(name="slack", capabilities=set())

    async def noop(*args, **kwargs):
        return None

    router._setup_streaming = noop
    router._teardown_streaming = noop
    return router


def said(text: str, actor_id: str | None) -> InboundMessage:
    return InboundMessage(
        channel_name="slack", channel_key="slack:C1:1.1", sender_id="C1:1.1", text=text,
        actor=person(actor_id) if actor_id else None,
    )


THREAD = [
    ("a1", ADA), ("a2", ADA), ("b1", BOB), ("n1", None), ("n2", None), ("a3", ADA),
]


class TestBatchesSplitByActor:
    async def test_each_run_of_one_actor_is_its_own_turn_in_order(self):
        recorder = Recorder()
        router = batching_router(recorder)

        results = await asyncio.gather(*(router.handle_message(said(t, a)) for t, a in THREAD))

        assert recorder.turns == [
            ("a1\n\na2", ADA), ("b1", BOB), ("n1\n\nn2", None), ("a3", ADA),
        ]
        assert results == [
            "reply to a1\n\na2", "reply to a1\n\na2", "reply to b1",
            "reply to n1\n\nn2", "reply to n1\n\nn2", "reply to a3",
        ]

    async def test_messages_of_one_actor_stay_one_turn(self):
        recorder = Recorder()
        router = batching_router(recorder)

        results = await asyncio.gather(*(router.handle_message(said(t, ADA)) for t in ("x", "y")))

        assert recorder.turns == [("x\n\ny", ADA)]
        assert results == ["reply to x\n\ny"] * 2

    async def test_a_failed_turn_fails_only_its_own_messages(self):
        recorder = Recorder(fail_for=BOB)
        router = batching_router(recorder)

        results = await asyncio.gather(
            *(router.handle_message(said(t, a)) for t, a in THREAD), return_exceptions=True,
        )

        assert [t for t, _ in recorder.turns] == ["a1\n\na2", "b1", "n1\n\nn2", "a3"]
        assert results[:2] == ["reply to a1\n\na2"] * 2
        assert isinstance(results[2], RuntimeError)
        assert results[3:] == ["reply to n1\n\nn2", "reply to n1\n\nn2", "reply to a3"]

    async def test_a_cancelled_turn_cancels_the_turns_after_it(self):
        async def typing(target: str) -> None:
            if recorder.turns:
                raise asyncio.CancelledError

        recorder = Recorder()
        channel = SimpleNamespace(
            name="slack", capabilities=ChannelCapability.TYPING_INDICATOR, send_typing=typing,
        )
        router = batching_router(recorder, channel)

        results = await asyncio.gather(
            *(router.handle_message(said(t, a)) for t, a in THREAD), return_exceptions=True,
        )

        assert recorder.turns == [("a1\n\na2", ADA)]
        assert results[1] == "reply to a1\n\na2"
        # The call of the first message runs the turns, so the cancellation
        # comes out of it.
        assert all(isinstance(r, asyncio.CancelledError) for r in [results[0], *results[2:]])


# ---------------------------------------------------------------------- #
#  The hosted channel end to end                                          #
# ---------------------------------------------------------------------- #


@pytest_asyncio.fixture
async def hosted_external(tmp_path, db, monkeypatch, external):
    harness = await start_hosted(tmp_path, db, monkeypatch)
    try:
        yield harness
    finally:
        await stop_hosted(harness)


@pytest_asyncio.fixture
async def hosted_local(tmp_path, db, monkeypatch):
    harness = await start_hosted(tmp_path, db, monkeypatch)
    try:
        yield harness
    finally:
        await stop_hosted(harness)


async def deliver(hosted: Hosted, *events: dict[str, Any]) -> None:
    stream = await hosted.stream()
    inbox_ids = [hosted.gateway.store(event) for event in events]
    await stream.nudge("invoke")
    acknowledged = await hosted.acknowledged(*inbox_ids)
    assert set(acknowledged.values()) == {"accepted"}


def direct(principal_id: str, *, text: str = "what is on today?", message_id: str = "1700000200.000100"):
    return invoked_by(message_event(
        text=text, conversation="D0DIRECT", conversation_kind="direct", message_id=message_id,
    ), principal_id)


class TestHostedTurns:
    async def test_a_message_runs_as_the_resolved_person(self, hosted_external):
        hosted = hosted_external

        await deliver(hosted, direct(ADA))

        await hosted.turns.wait_for(1)
        assert hosted.turns.runs[0]["actor"] == person(ADA)
        mapping = await hosted.db.get_channel_session("slack:D0DIRECT")
        session = await hosted.db.get_session(mapping["session_id"])
        assert (session["created_by_actor_id"], session["caused_by_actor_id"]) == (ADA, ADA)
        assert (await hosted.db.get_actor_ref(ADA))["kind"] == ACTOR_KIND_HUMAN

    async def test_the_person_keeps_the_name_from_a_web_request(self, hosted_external):
        hosted = hosted_external
        await hosted.db.upsert_external_actor(ADA, "Ada")

        await deliver(hosted, direct(ADA))

        await hosted.turns.wait_for(1)
        assert hosted.turns.runs[0]["actor"] == person(ADA, "Ada")
        assert (await hosted.db.get_actor_ref(ADA))["display_name"] == "Ada"

    @pytest.mark.parametrize("principal", ["nil", "system"])
    async def test_a_nil_or_system_principal_runs_without_an_actor(self, hosted_external, principal):
        hosted = hosted_external
        principal_id = NIL if principal == "nil" else hosted.db.system_actor_id

        await deliver(hosted, direct(principal_id))

        await hosted.turns.wait_for(1)
        assert hosted.turns.runs[0]["actor"] is None
        mapping = await hosted.db.get_channel_session("slack:D0DIRECT")
        session = await hosted.db.get_session(mapping["session_id"])
        assert (session["created_by_actor_id"], session["caused_by_actor_id"]) == (None, None)

    async def test_local_mode_runs_without_an_actor(self, hosted_local):
        hosted = hosted_local

        await deliver(hosted, direct(ADA))

        await hosted.turns.wait_for(1)
        assert hosted.turns.runs[0]["actor"] is None
        mapping = await hosted.db.get_channel_session("slack:D0DIRECT")
        assert (await hosted.db.get_session(mapping["session_id"]))["created_by_actor_id"] is None
        assert await hosted.db.get_actor_ref(ADA) is None

    async def test_a_reaction_runs_as_the_person_who_reacted(self, hosted_external):
        hosted = hosted_external
        await deliver(hosted, direct(ADA, message_id="1700000300.000100"))
        await hosted.turns.wait_for(1)
        reaction = invoked_by(message_event(
            kind="reaction_added", conversation="D0DIRECT", conversation_kind="direct",
            message_id="1700000300.000100",
        ), BOB)
        reaction["reaction"] = {"action": "add", "name": "thumbsup"}

        await deliver(hosted, reaction)

        await hosted.turns.wait_for(2)
        assert hosted.turns.runs[1]["user_message"].startswith("[Reaction: thumbsup")
        assert hosted.turns.runs[1]["actor"] == person(BOB)


@pytest_asyncio.fixture
async def noticed_external(tmp_path, db, monkeypatch, external):
    harness = await start_hosted(tmp_path, db, monkeypatch, notifications=True)
    harness.config.notifications.slack_channel_id = TOOL_TARGET
    await db.create_session("s1", actor=None)
    try:
        yield harness
    finally:
        await stop_hosted(harness)


class TestHostedAnswers:
    async def test_a_press_answers_as_the_resolved_person(self, noticed_external):
        hosted = noticed_external
        stream = await hosted.stream()
        notification_id, message_id = await question_card(hosted)
        press = invoked_by(interaction_event(f"notif:{notification_id}:yes", message_id=message_id), ADA)
        inbox_id = hosted.gateway.store(press)

        await stream.nudge("invoke")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        notification = await hosted.db.get_notification(notification_id)
        assert (notification["status"], notification["answered_by_actor_id"]) == ("answered", ADA)
        await hosted.turns.wait_for(1)
        assert hosted.turns.runs[0]["actor"] == person(ADA)

    async def test_a_press_in_local_mode_answers_without_an_actor(self, tmp_path, db, monkeypatch):
        hosted = await start_hosted(tmp_path, db, monkeypatch, notifications=True)
        hosted.config.notifications.slack_channel_id = TOOL_TARGET
        await db.create_session("s1", actor=None)
        try:
            stream = await hosted.stream()
            notification_id, message_id = await question_card(hosted)
            press = invoked_by(interaction_event(f"notif:{notification_id}:yes", message_id=message_id), ADA)
            inbox_id = hosted.gateway.store(press)

            await stream.nudge("invoke")

            assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
            notification = await hosted.db.get_notification(notification_id)
            assert (notification["status"], notification["answered_by_actor_id"]) == ("answered", None)
            await hosted.turns.wait_for(1)
            assert hosted.turns.runs[0]["actor"] is None
        finally:
            await stop_hosted(hosted)


# ---------------------------------------------------------------------- #
#  The person that the gateway linked to the author                       #
# ---------------------------------------------------------------------- #


def linked_to(event: dict[str, Any], principal_id: str, display_name: str | None = None) -> dict[str, Any]:
    linked = {"id": principal_id}
    if display_name is not None:
        linked["display_name"] = display_name
    event["author"]["linked_principal"] = linked
    return event


OBSERVED_MESSAGE = "1700001000.000100"


def observation(**changes: Any) -> dict[str, Any]:
    return message_event(
        purpose="observe", text="the build is red again", conversation="C_FIXTURE_OBSERVATION",
        message_id=OBSERVED_MESSAGE, **changes,
    )


async def observe(hosted: Hosted, event: dict[str, Any]) -> str:
    """Deliver an observation and return its acknowledgement outcome."""
    stream = await hosted.stream()
    inbox_id = hosted.gateway.store(event)
    await stream.nudge("observe")
    return (await hosted.acknowledged(inbox_id))[inbox_id]


async def drained(hosted: Hosted) -> dict[str, Any] | None:
    """Drain the observation buffer and return the inbox record of the observation."""
    (runner,) = [
        r for r in build_source_runners(hosted.config, hosted.db)
        if r.source.source_name == "slack:observed"
    ]
    await runner.run()
    return await hosted.db.get_source_message("slack:observed", f"C_FIXTURE_OBSERVATION:{OBSERVED_MESSAGE}")


class TestObservedPerson:
    async def test_an_observation_stores_and_shows_the_linked_person(self, hosted_external):
        hosted = hosted_external

        assert await observe(hosted, linked_to(observation(), ADA, "Ada Lovelace")) == "accepted"

        ((_, stored),) = await hosted.db.read_channel_observations("slack")
        assert (stored["principal_id"], stored["principal_name"]) == (ADA, "Ada Lovelace")
        assert (stored["sender_id"], stored["sender_name"]) == ("U_FIXTURE_MEMBER", "Example Member")
        record = await drained(hosted)
        assert record["summary"] == "[deployments] Example Member (Ada Lovelace): the build is red again"
        assert record["metadata"]["principal_id"] == ADA
        assert record["metadata"]["principal_name"] == "Ada Lovelace"
        assert record["metadata"]["sender_id"] == "U_FIXTURE_MEMBER"

    async def test_an_observation_never_adds_an_actor(self, hosted_external):
        hosted = hosted_external
        before = await humans(hosted.db)

        assert await observe(hosted, linked_to(observation(), ADA, "Ada Lovelace")) == "accepted"

        await drained(hosted)
        assert await humans(hosted.db) == before
        assert await hosted.db.get_actor_ref(ADA) is None
        assert hosted.turns.runs == []

    async def test_an_observation_from_an_older_gateway_names_no_person(self, hosted_external):
        hosted = hosted_external

        assert await observe(hosted, observation()) == "accepted"

        ((_, stored),) = await hosted.db.read_channel_observations("slack")
        assert (stored["principal_id"], stored["principal_name"]) == ("", "")
        record = await drained(hosted)
        assert record["summary"] == "[deployments] Example Member: the build is red again"
        assert "principal_id" not in record["metadata"]

    async def test_a_linked_person_that_is_not_valid_is_left_out(self, hosted_external, caplog):
        hosted = hosted_external

        with caplog.at_level(logging.WARNING, logger="nerve.channels.hosted.channel"):
            outcome = await observe(hosted, linked_to(observation(), NIL, "Ada Lovelace"))

        assert outcome == "accepted"
        ((_, stored),) = await hosted.db.read_channel_observations("slack")
        assert (stored["principal_id"], stored["principal_name"]) == ("", "")
        assert "not valid" in caplog.text
        assert "Ada Lovelace" not in caplog.text

    @pytest.mark.parametrize(("rules", "outcome"), [
        ({"allow_senders": [ADA]}, "rejected"),
        ({"allow_senders": ["Ada Lovelace"]}, "rejected"),
        ({"allow_senders": ["U_FIXTURE_MEMBER"]}, "accepted"),
        ({"deny_senders": [ADA]}, "accepted"),
        ({"deny_senders": ["Ada*"]}, "accepted"),
        ({"deny_senders": ["Example Member"]}, "rejected"),
    ], ids=[
        "allow_person_id", "allow_person_name", "allow_member_id",
        "deny_person_id", "deny_person_name", "deny_member_name",
    ])
    async def test_sender_rules_match_only_the_provider_identity(
        self, tmp_path, db, monkeypatch, external, rules, outcome,
    ):
        hosted = await start_hosted(tmp_path, db, monkeypatch, **rules)
        try:
            event = linked_to(observation(), ADA, "Ada Lovelace")

            assert await observe(hosted, event) == outcome

            record = await drained(hosted)
            assert (record is not None) == (outcome == "accepted")
        finally:
            await stop_hosted(hosted)


class TestInvokeName:
    async def test_the_linked_name_fills_a_missing_name(self, hosted_external):
        hosted = hosted_external

        await deliver(hosted, linked_to(direct(ADA), ADA, "Ada Lovelace"))

        await hosted.turns.wait_for(1)
        assert hosted.turns.runs[0]["actor"] == person(ADA, "Ada Lovelace")
        assert (await hosted.db.get_actor_ref(ADA))["display_name"] == "Ada Lovelace"

    async def test_the_name_from_a_web_request_is_kept(self, hosted_external):
        hosted = hosted_external
        await hosted.db.upsert_external_actor(ADA, "Ada")

        await deliver(hosted, linked_to(direct(ADA), ADA, "Ada Lovelace"))

        await hosted.turns.wait_for(1)
        assert hosted.turns.runs[0]["actor"] == person(ADA, "Ada")
        assert (await hosted.db.get_actor_ref(ADA))["display_name"] == "Ada"

    async def test_another_linked_person_gives_no_name_and_no_actor(self, hosted_external):
        hosted = hosted_external

        await deliver(hosted, linked_to(direct(ADA), BOB, "Bob"))

        await hosted.turns.wait_for(1)
        assert hosted.turns.runs[0]["actor"] == person(ADA)
        assert await hosted.db.get_actor_ref(BOB) is None

    async def test_a_linked_person_without_a_resolved_person_is_no_actor(self, hosted_external):
        hosted = hosted_external

        await deliver(hosted, linked_to(direct(NIL), ADA, "Ada Lovelace"))

        await hosted.turns.wait_for(1)
        assert hosted.turns.runs[0]["actor"] is None
        assert await hosted.db.get_actor_ref(ADA) is None

    async def test_local_mode_ignores_the_linked_person(self, hosted_local):
        hosted = hosted_local

        await deliver(hosted, linked_to(direct(ADA), ADA, "Ada Lovelace"))

        await hosted.turns.wait_for(1)
        assert hosted.turns.runs[0]["actor"] is None
        assert await hosted.db.get_actor_ref(ADA) is None

    async def test_a_press_fills_a_missing_name(self, noticed_external):
        hosted = noticed_external
        stream = await hosted.stream()
        notification_id, message_id = await question_card(hosted)
        press = invoked_by(interaction_event(f"notif:{notification_id}:yes", message_id=message_id), ADA)
        inbox_id = hosted.gateway.store(linked_to(press, ADA, "Ada Lovelace"))

        await stream.nudge("invoke")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        notification = await hosted.db.get_notification(notification_id)
        assert notification["answered_by_actor_id"] == ADA
        assert (await hosted.db.get_actor_ref(ADA))["display_name"] == "Ada Lovelace"


# ---------------------------------------------------------------------- #
#  Model requests name the session's cause                                #
# ---------------------------------------------------------------------- #


@pytest.fixture
def cli(monkeypatch) -> list[_FakeCli]:
    """The Claude CLI processes that the engine starts, as recording stand-ins."""
    started: list[_FakeCli] = []
    monkeypatch.setattr(_FakeCli, "started", started)
    monkeypatch.setattr(claude_backend, "ClaudeClient", _FakeCli)
    monkeypatch.setattr(
        claude_backend.ClaudeBackend, "validate_resume_target",
        lambda self, native_id, cwd: native_id.startswith("native-"),
    )
    return started


@pytest_asyncio.fixture
async def hosted_engine(tmp_path, db, monkeypatch, external, cli):
    """The hosted channel in front of a real engine, whose CLI process is ``cli``."""
    monkeypatch.delenv(CUSTOM_HEADERS_ENV, raising=False)
    config = hosted_config(tmp_path)
    config.agent.backend = "claude"
    config.codex.home_dir = str(tmp_path / "codex-home")
    monkeypatch.setattr(config_module, "_config", config)
    engine = AgentEngine(config, db)
    engine.router.BATCH_DEBOUNCE = 0.0
    gateway = FakeChannelGateway(jwks_file=config.channels.hosted.gateway_jwks_file)
    runtime = HostedChannelRuntime(
        config, engine.router, lambda: config, stream_timing=FAST_STREAM,
        reader_settings=FAST_READER, operation_timing=FAST_OPERATIONS,
    )
    await runtime.start()
    monkeypatch.setattr(gateway_server, "_hosted_channels", runtime)
    server = NerveServer(gateway_server.create_app())
    await server.__aenter__()
    try:
        yield Hosted(config, db, engine.router, None, gateway, runtime, server)
    finally:
        await runtime.stop()
        await gateway.close_all()
        await server.__aexit__(None, None, None)
        for session_id in list(engine._idle_watchers):
            engine._stop_idle_watcher(session_id)


async def user_messages(db, channel_key: str, count: int) -> tuple[str, list[tuple[str, str | None]]]:
    """Wait until the channel's session has *count* user messages."""
    deadline = asyncio.get_running_loop().time() + EVENT_TIMEOUT
    while True:
        mapping = await db.get_channel_session(channel_key)
        if mapping is not None:
            said = await said_in(db, mapping["session_id"])
            if len(said) >= count:
                return mapping["session_id"], said
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"{channel_key} did not get {count} user messages")
        await asyncio.sleep(0.02)


class TestInferencePrincipal:
    async def test_a_session_that_a_hosted_message_starts_names_its_person(self, hosted_engine, cli):
        await deliver(hosted_engine, direct(ADA))

        session_id, said = await user_messages(hosted_engine.db, "slack:D0DIRECT", 1)

        assert said == [("what is on today?", ADA)]
        assert (await hosted_engine.db.get_session(session_id))["caused_by_actor_id"] == ADA
        (process,) = cli
        assert process.spec.inference_principal == ADA
        assert _principals(process.env) == [ADA]

    async def test_a_hosted_turn_in_an_existing_session_keeps_its_cause(self, hosted_engine, cli):
        await deliver(hosted_engine, direct(BOB, text="start", message_id="1700000200.000100"))
        await user_messages(hosted_engine.db, "slack:D0DIRECT", 1)

        await deliver(hosted_engine, direct(ADA, text="my turn", message_id="1700000200.000200"))

        session_id, said = await user_messages(hosted_engine.db, "slack:D0DIRECT", 2)
        assert said == [("start", BOB), ("my turn", ADA)]
        session = await hosted_engine.db.get_session(session_id)
        assert (session["created_by_actor_id"], session["caused_by_actor_id"]) == (BOB, BOB)
        (process,) = cli
        assert process.spec.inference_principal == BOB
        assert _principals(process.env) == [BOB]
        assert process.turns == 2
