"""Hosted channels end to end: the fake gateway against a real Nerve endpoint.

The stream endpoint is the one ``create_app`` registers, served by uvicorn on
a loopback port. The router, the session manager, and the database are real;
only the agent turn is a stand-in, so each test can see which session a
message reached and whether the acknowledgement waited for the turn.

Covered here: authentication at the upgrade, several streams with the
preferred one closing or draining, invoke into a session, observe into the
source inbox, duplicate pages, lost acknowledgements, nudges during an
outstanding page, and the separation from the web UI's own authentication.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
import pytest_asyncio
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

import nerve.config as config_module
from nerve.agent.sessions import SessionManager
from nerve.channels.hosted.intake import ReaderSettings
from nerve.channels.hosted.runtime import HostedChannelRuntime
from nerve.channels.hosted.stream import StreamTiming
from nerve.channels.router import ChannelRouter
from nerve.config import (
    ChannelSourceConfig,
    HostedChannelsConfig,
    NerveConfig,
    SlackConfig,
)
from nerve.gateway import server as gateway_server
from nerve.sources.registry import build_source_runners

from tests.fake_channel_gateway import (
    AGENT_ID,
    CONNECTION_ID,
    EVENT_TIMEOUT,
    ISSUER,
    TENANT_ID,
    FakeChannelGateway,
    FakeStream,
    NerveServer,
    message_event,
)

pytestmark = pytest.mark.asyncio

WEB_SECRET = "web-ui-session-secret-for-the-hosted-channel-tests"
FAST_READER = ReaderSettings(
    request_timeout=1.5,
    poll_interval=30.0,
    observe_interval=0.5,
    deferred_retry=30.0,
    backoff_initial=0.05,
    backoff_maximum=0.2,
    capacity_poll=0.01,
    stop_grace=0.5,
)
FAST_STREAM = StreamTiming(heartbeat_interval=1.0, negotiation_timeout=2.0)


class Turns:
    """The agent turn stand-in: records each run, and can hold it open."""

    def __init__(self) -> None:
        self.runs: list[dict[str, Any]] = []
        self.release = asyncio.Event()
        self.release.set()
        self.started = asyncio.Event()

    async def run(self, **kwargs: Any) -> str:
        self.runs.append(kwargs)
        self.started.set()
        await self.release.wait()
        return "done"

    async def wait_for(self, count: int) -> None:
        deadline = asyncio.get_running_loop().time() + EVENT_TIMEOUT
        while len(self.runs) < count:
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"expected {count} turns, saw {len(self.runs)}")
            await asyncio.sleep(0.01)


@dataclass
class Hosted:
    config: NerveConfig
    db: Any
    router: ChannelRouter
    turns: Turns
    gateway: FakeChannelGateway
    runtime: HostedChannelRuntime
    server: NerveServer

    async def stream(self, **kwargs: Any) -> FakeStream:
        return await self.gateway.open_stream(self.server.stream_url, **kwargs)

    async def refused_status(self, **kwargs: Any) -> tuple[int, bytes]:
        with pytest.raises(InvalidStatus) as error:
            await self.gateway.open_stream(self.server.stream_url, **kwargs)
        return error.value.response.status_code, error.value.response.body or b""

    async def acknowledged(self, *inbox_ids: str) -> dict[str, str]:
        await self.gateway.wait_for(lambda: all(i in self.gateway.acknowledged for i in inbox_ids))
        return {i: self.gateway.acknowledged[i] for i in inbox_ids}


def hosted_config(tmp_path, **source: Any) -> NerveConfig:
    config = NerveConfig()
    config.workspace = tmp_path / "workspace"
    config.workspace.mkdir(parents=True, exist_ok=True)
    config.auth.jwt_secret = WEB_SECRET
    config.telegram.enabled = False
    source_settings = {
        "enabled": True,
        "allow_conversations": ["C_FIXTURE_OBSERVATION", "C_FIXTURE_CHANNEL"],
    }
    source_settings.update(source)
    config.slack = SlackConfig(
        enabled=True, mode="hosted", source=ChannelSourceConfig(**source_settings),
    )
    config.channels.hosted = HostedChannelsConfig(
        gateway_jwks_file=tmp_path / "gateway-jwks.json", issuer=ISSUER,
        tenant_id=TENANT_ID, agent_id=AGENT_ID, max_streams=2,
    )
    return config


async def start_hosted(tmp_path, db, monkeypatch, reader: ReaderSettings = FAST_READER, **source: Any):
    config = hosted_config(tmp_path, **source)
    monkeypatch.setattr(config_module, "_config", config)
    turns = Turns()
    engine = MagicMock()
    engine.db = db
    engine.sessions = SessionManager(db)
    engine.run = turns.run
    router = ChannelRouter(engine)
    router.BATCH_DEBOUNCE = 0.0
    gateway = FakeChannelGateway(jwks_file=config.channels.hosted.gateway_jwks_file)
    runtime = HostedChannelRuntime(
        config, router, lambda: config, stream_timing=FAST_STREAM, reader_settings=reader,
    )
    await runtime.start()
    monkeypatch.setattr(gateway_server, "_hosted_channels", runtime)
    server = NerveServer(gateway_server.create_app())
    await server.__aenter__()
    return Hosted(config, db, router, turns, gateway, runtime, server)


async def stop_hosted(hosted: Hosted) -> None:
    hosted.turns.release.set()
    await hosted.runtime.stop()
    await hosted.gateway.close_all()
    await hosted.server.__aexit__(None, None, None)


@pytest_asyncio.fixture
async def hosted(tmp_path, db, monkeypatch):
    harness = await start_hosted(tmp_path, db, monkeypatch)
    try:
        yield harness
    finally:
        await stop_hosted(harness)


# ---------------------------------------------------------------------- #
#  Authentication at the upgrade                                          #
# ---------------------------------------------------------------------- #


class TestUpgradeAuthentication:
    async def test_a_gateway_token_opens_a_stream(self, hosted):
        stream = await hosted.stream()

        negotiation = stream.sent("negotiation")[0]["payload"]["negotiation"]
        assert negotiation["delivery_modes"] == ["pull"]
        assert negotiation["supported_versions"] == ["1"]
        assert negotiation["receive_limits"]["frame_bytes"] == 262144

    @pytest.mark.parametrize("claims", [
        {"aud": "nerve-local-inference"},
        {"sub": f"tenants/{TENANT_ID}/agents/{uuid.uuid4()}"},
        {"agent_id": str(uuid.uuid4())},
        {"tenant_id": str(uuid.uuid4())},
        {"exp": 1_700_000_000, "iat": 1_699_999_800, "nbf": 1_699_999_800},
        {"iss": "https://cp.test/workload-identity"},
    ], ids=["wrong_audience", "wrong_subject", "wrong_agent", "wrong_tenant", "expired", "wrong_issuer"])
    async def test_a_bad_token_is_refused_with_401_and_no_detail(self, hosted, claims):
        status, body = await hosted.refused_status(token=hosted.gateway.token(**claims))

        assert status == 401
        assert body == b""

    async def test_a_lifetime_above_300_seconds_is_refused(self, hosted):
        now = int(time.time())
        token = hosted.gateway.token(iat=now, nbf=now, exp=now + 301)

        status, body = await hosted.refused_status(token=token)

        assert status == 401
        assert body == b""

    async def test_an_unknown_kid_is_refused(self, hosted):
        stranger = hosted.gateway.add_key("unpublished", publish=False)

        status, _ = await hosted.refused_status(token=hosted.gateway.token(kid=stranger))

        assert status == 401

    async def test_an_origin_header_is_refused_even_with_a_good_token(self, hosted):
        headers = {"Authorization": f"Bearer {hosted.gateway.token()}", "Origin": "https://nerve.example"}

        status, _ = await hosted.refused_status(headers=headers)

        assert status == 401

    async def test_the_token_is_read_from_the_header_only(self, hosted):
        token = hosted.gateway.token()
        url = f"{hosted.server.stream_url}?token={token}"

        with pytest.raises(InvalidStatus) as error:
            await connect(url, additional_headers={"Cookie": f"nerve_token={token}"})

        assert error.value.response.status_code == 401

    async def test_two_authorization_headers_are_refused(self, hosted):
        token = hosted.gateway.token()
        headers = [("Authorization", f"Bearer {token}"), ("Authorization", f"Bearer {token}")]

        status, _ = await hosted.refused_status(headers=headers)

        assert status == 401

    async def test_the_web_session_token_does_not_open_a_stream(self, hosted):
        from nerve.gateway.auth import create_session_token

        status, _ = await hosted.refused_status(
            token=create_session_token(WEB_SECRET, "an-account"),
        )

        assert status == 401

    async def test_streams_above_the_limit_are_refused_with_503(self, hosted):
        await hosted.stream()
        await hosted.stream()

        status, _ = await hosted.refused_status()

        assert status == 503


class TestSeparationFromTheWebUI:
    async def test_a_stream_token_does_not_open_the_web_socket(self, hosted):
        token = hosted.gateway.token()
        url = f"ws://127.0.0.1:{hosted.server.port}/ws?token={token}"

        async with connect(url) as websocket:
            with pytest.raises(ConnectionClosed) as closed:
                await asyncio.wait_for(websocket.recv(), EVENT_TIMEOUT)

        assert closed.value.rcvd.code == 4001

    async def test_a_stream_token_does_not_reach_the_api(self, hosted):
        async with httpx.AsyncClient(base_url=hosted.server.base) as client:
            response = await client.get(
                "/api/sessions", headers={"Authorization": f"Bearer {hosted.gateway.token()}"},
            )

        assert response.status_code == 401

    async def test_plain_http_under_internal_is_not_found(self, hosted):
        async with httpx.AsyncClient(base_url=hosted.server.base) as client:
            response = await client.get("/_internal/channel/v1/stream")

        assert response.status_code == 404
        assert response.content == b""

    async def test_the_endpoint_refuses_every_upgrade_when_hosted_mode_is_off(self, tmp_path, monkeypatch):
        config = NerveConfig()
        config.workspace = tmp_path
        monkeypatch.setattr(config_module, "_config", config)
        monkeypatch.setattr(gateway_server, "_hosted_channels", None)
        gateway = FakeChannelGateway()

        async with NerveServer(gateway_server.create_app()) as server:
            with pytest.raises(InvalidStatus) as error:
                await gateway.open_stream(server.stream_url)

        assert error.value.response.status_code == 404


# ---------------------------------------------------------------------- #
#  Streams                                                                #
# ---------------------------------------------------------------------- #


class TestStreams:
    async def test_nerve_reads_after_each_negotiation(self, hosted):
        first = await hosted.stream()
        await hosted.gateway.wait_for(lambda: len(hosted.gateway.reads) >= 1)
        second = await hosted.stream()

        await hosted.gateway.wait_for(lambda: len(hosted.gateway.reads) >= 2)

        assert first.sent("negotiation") and second.sent("negotiation")

    async def test_the_preferred_stream_is_the_oldest_and_its_successor_takes_over(self, hosted):
        gateway = hosted.gateway
        first = await hosted.stream()
        second = await hosted.stream()
        await gateway.wait_for(lambda: len(gateway.reads) >= 2)
        reads_before = len(gateway.reads)

        gateway.store(message_event(conversation="D0DIRECT", conversation_kind="direct", message_id="1.1"))
        await second.nudge("invoke")
        await gateway.wait_for(lambda: len(gateway.acknowledged) == 1)
        assert all(read["stream"] is first for read in gateway.reads[reads_before:])

        await first.close()
        inbox_id = gateway.store(message_event(
            conversation="D0DIRECT", conversation_kind="direct", message_id="1.2",
        ))
        await second.nudge("invoke")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        assert gateway.reads[-1]["stream"] is second

    async def test_a_read_lost_with_its_stream_is_sent_again_on_another(self, hosted):
        gateway = hosted.gateway
        first = await hosted.stream()
        second = await hosted.stream()
        await gateway.wait_for(lambda: len(gateway.reads) >= 2)
        inbox_id = gateway.store(message_event(
            conversation="D0DIRECT", conversation_kind="direct", message_id="2.1",
        ))

        gateway.hold_reads = True
        await first.nudge("invoke")
        await gateway.wait_for(lambda: bool(first.held))
        gateway.hold_reads = False
        await first.close()

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        assert gateway.reads[-1]["stream"] is second

    async def test_a_draining_stream_gets_no_new_requests(self, hosted):
        gateway = hosted.gateway
        first = await hosted.stream()
        second = await hosted.stream()
        await gateway.wait_for(lambda: len(gateway.reads) >= 2)

        await first.drain()
        await asyncio.sleep(0.1)
        inbox_id = gateway.store(message_event(
            conversation="D0DIRECT", conversation_kind="direct", message_id="3.1",
        ))
        await first.nudge("invoke")

        await hosted.acknowledged(inbox_id)
        assert gateway.reads[-1]["stream"] is second

    async def test_a_rejected_frame_closes_the_stream_with_its_reason(self, hosted):
        stream = await hosted.stream()

        await stream.send("inbox_read", {"maximum_events": 1, "maximum_bytes": 16384})
        await asyncio.wait_for(stream.closed.wait(), EVENT_TIMEOUT)

        assert (stream.close_code, stream.close_reason) == (1002, "kind_unsupported")

    async def test_a_frame_before_negotiation_closes_the_stream(self, hosted):
        stream = await hosted.stream(negotiate=False)

        await stream.nudge("invoke")
        await asyncio.wait_for(stream.closed.wait(), EVENT_TIMEOUT)

        assert (stream.close_code, stream.close_reason) == (1002, "malformed_frame")

    async def test_a_response_to_a_one_way_frame_closes_the_stream(self, hosted):
        stream = await hosted.stream()
        negotiation_id = stream.sent("negotiation")[0]["request_id"]

        await stream.respond(negotiation_id, "inbox_ack_result", {"outcome": "succeeded"})
        await asyncio.wait_for(stream.closed.wait(), EVENT_TIMEOUT)

        assert negotiation_id.startswith("o-")
        assert (stream.close_code, stream.close_reason) == (1002, "malformed_frame")

    async def test_a_second_negotiation_closes_the_stream(self, hosted):
        stream = await hosted.stream()

        await stream.negotiate()
        await asyncio.wait_for(stream.closed.wait(), EVENT_TIMEOUT)

        assert (stream.close_code, stream.close_reason) == (1002, "malformed_frame")

    async def test_nerve_sends_heartbeats(self, hosted):
        stream = await hosted.stream()

        deadline = asyncio.get_running_loop().time() + EVENT_TIMEOUT
        while not stream.sent("heartbeat") and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)

        beats = stream.sent("heartbeat")
        assert beats and beats[0]["payload"]["heartbeat"]["sequence"] == 1

    async def test_shutdown_drains_then_closes_every_stream(self, hosted):
        stream = await hosted.stream()
        await hosted.gateway.wait_for(lambda: len(hosted.gateway.reads) >= 1)

        await hosted.runtime.stop()
        await asyncio.wait_for(stream.closed.wait(), EVENT_TIMEOUT)

        assert stream.sent("drain")[0]["payload"]["drain"]["reason"] == "shutdown"
        assert stream.close_code == 1001


# ---------------------------------------------------------------------- #
#  Invoke and observe                                                     #
# ---------------------------------------------------------------------- #


class TestInvoke:
    async def test_a_direct_message_starts_a_turn_in_its_session(self, hosted):
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(message_event(
            text="what is on today?", conversation="D0DIRECT", conversation_kind="direct",
            message_id="1700000200.000100",
        ))

        await stream.nudge("invoke")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        await hosted.turns.wait_for(1)
        mapping = await hosted.db.get_channel_session("slack:D0DIRECT")
        assert mapping is not None
        run = hosted.turns.runs[0]
        assert (run["session_id"], run["user_message"], run["channel"]) == (
            mapping["session_id"], "what is on today?", "slack",
        )

    async def test_a_mention_in_a_channel_roots_a_thread_session(self, hosted):
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(message_event(
            text="summarize the deploys", mention_agent=True, message_id="1700000300.000100",
        ))

        await stream.nudge("invoke")

        await hosted.acknowledged(inbox_id)
        await hosted.turns.wait_for(1)
        assert await hosted.db.get_channel_session("slack:C_FIXTURE_CHANNEL:1700000300.000100")
        assert hosted.turns.runs[0]["user_message"] == "summarize the deploys"

    async def test_acceptance_does_not_wait_for_the_turn(self, hosted):
        hosted.turns.release.clear()
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(message_event(
            conversation="D0DIRECT", conversation_kind="direct", message_id="1700000400.000100",
        ))

        await stream.nudge("invoke")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        await asyncio.wait_for(hosted.turns.started.wait(), EVENT_TIMEOUT)
        assert not hosted.turns.release.is_set()

    async def test_an_unaddressed_thread_reply_is_rejected_without_a_session(self, hosted):
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(message_event(
            message_id="1700000500.000200", thread="1700000500.000100",
        ))

        await stream.nudge("invoke")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "rejected"
        assert hosted.gateway.acks[-1][0]["reason_code"] == "admission_rejected"
        assert hosted.turns.runs == []

    async def test_a_reply_in_a_thread_with_a_session_continues_it(self, hosted):
        session_id = await hosted.router.create_session(
            "slack:C_FIXTURE_CHANNEL:1700000600.000100", source="slack",
        )
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(message_event(
            text="and the next step?", message_id="1700000600.000200", thread="1700000600.000100",
        ))

        await stream.nudge("invoke")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        await hosted.turns.wait_for(1)
        assert hosted.turns.runs[0]["session_id"] == session_id

    async def test_a_mention_waits_for_the_agent_identity(self, hosted):
        stream = await hosted.stream(advertise=False)
        inbox_id = hosted.gateway.store(message_event(mention_agent=True, message_id="1700000700.000100"))
        await stream.nudge("invoke")
        await hosted.gateway.wait_for(
            lambda: any(inbox_id in read.get("served", ()) for read in hosted.gateway.reads),
        )
        await asyncio.sleep(0.1)
        assert inbox_id not in hosted.gateway.acknowledged

        await stream.advertise()

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        await hosted.turns.wait_for(1)

    async def test_an_unsupported_kind_is_rejected(self, hosted):
        stream = await hosted.stream()
        event = message_event(conversation="D0DIRECT", conversation_kind="direct", message_id="9.1")
        event["kind"] = "message_edited"
        inbox_id = hosted.gateway.store(event)

        await stream.nudge("invoke")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "rejected"

    async def test_every_connection_of_the_provider_is_taken_and_remembered(self, hosted):
        other_connection = str(uuid.uuid4())
        stream = await hosted.stream()
        await stream.advertise(connection_id=other_connection)
        first = hosted.gateway.store(message_event(
            conversation="D0FIRST", conversation_kind="direct", message_id="8.1",
        ))
        second = hosted.gateway.store(message_event(
            conversation="D0SECOND", conversation_kind="direct", message_id="8.2",
            connection_id=other_connection,
        ))

        await stream.nudge("invoke")

        assert await hosted.acknowledged(first, second) == {first: "accepted", second: "accepted"}
        channel = hosted.runtime.channels["slack"]
        assert str(channel.connection_for("D0FIRST")) == CONNECTION_ID
        assert str(channel.connection_for("D0SECOND")) == other_connection

    async def test_turning_the_provider_off_by_reload_stops_intake(self, hosted):
        stream = await hosted.stream()
        await hosted.gateway.wait_for(lambda: len(hosted.gateway.reads) >= 1)
        hosted.config.slack.enabled = False
        reads_before = len(hosted.gateway.reads)
        inbox_id = hosted.gateway.store(message_event(
            conversation="D0DIRECT", conversation_kind="direct", message_id="8.3",
        ))

        await stream.nudge("invoke")
        await asyncio.sleep(0.3)
        assert len(hosted.gateway.reads) == reads_before

        hosted.config.slack.enabled = True
        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"

    async def test_a_provider_off_before_the_first_stream_takes_events_once_turned_on(self, hosted):
        hosted.config.slack.enabled = False
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(message_event(
            conversation="D0DIRECT", conversation_kind="direct", message_id="8.4",
        ))

        await stream.nudge("invoke")
        await asyncio.sleep(0.3)
        assert hosted.gateway.reads == []

        hosted.config.slack.enabled = True
        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        await hosted.turns.wait_for(1)


class TestObserve:
    async def test_an_observation_reaches_the_source_inbox(self, hosted):
        stream = await hosted.stream()
        event = message_event(
            purpose="observe", text="the build is red again", conversation="C_FIXTURE_OBSERVATION",
            message_id="1700001000.000100", author="U_FIXTURE_UNLINKED",
        )
        inbox_id = hosted.gateway.store(event)

        await stream.nudge("observe")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        rows = await hosted.db.read_channel_observations("slack")
        assert len(rows) == 1
        observed = rows[0][1]
        assert observed["text"] == "the build is red again"
        assert observed["channel_key"] == "slack:C_FIXTURE_OBSERVATION:1700001000.000100"

        (runner,) = [
            r for r in build_source_runners(hosted.config, hosted.db)
            if r.source.source_name == "slack:observed"
        ]
        result = await runner.run()
        assert result.records_ingested == 1
        record = await hosted.db.get_source_message(
            "slack:observed", "C_FIXTURE_OBSERVATION:1700001000.000100",
        )
        assert record["content"] == "the build is red again"
        assert record["metadata"]["sender_id"] == "U_FIXTURE_UNLINKED"

    async def test_the_acknowledgement_waits_for_the_stored_observation(self, hosted, monkeypatch):
        calls = 0
        original = hosted.db.insert_channel_observation

        async def failing_once(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("database is locked")
            return await original(*args, **kwargs)

        monkeypatch.setattr(hosted.db, "insert_channel_observation", failing_once)
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(message_event(
            purpose="observe", conversation="C_FIXTURE_OBSERVATION", message_id="1700001100.000100",
        ))

        await stream.nudge("observe")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        assert calls == 2
        assert len(hosted.gateway.acks) == 1

    async def test_nudges_do_not_reread_a_deferred_row(self, tmp_path, db, monkeypatch):
        patient = ReaderSettings(**{**FAST_READER.__dict__, "backoff_initial": 20.0, "backoff_maximum": 20.0})
        hosted = await start_hosted(tmp_path, db, monkeypatch, reader=patient)
        try:
            async def failing(*args, **kwargs):
                raise RuntimeError("database is locked")

            monkeypatch.setattr(hosted.db, "insert_channel_observation", failing)
            stream = await hosted.stream()
            inbox_id = hosted.gateway.store(message_event(
                purpose="observe", conversation="C_FIXTURE_OBSERVATION", message_id="1700001150.000100",
            ))
            await stream.nudge("observe")
            await hosted.gateway.wait_for(
                lambda: any(inbox_id in read.get("served", ()) for read in hosted.gateway.reads),
            )
            reads_after_deferral = len(hosted.gateway.reads)

            for purpose in ("invoke", None, "observe", "invoke"):
                await stream.nudge(purpose)
            await asyncio.sleep(0.5)

            assert len(hosted.gateway.reads) == reads_after_deferral
            assert inbox_id not in hosted.gateway.acknowledged
        finally:
            await stop_hosted(hosted)

    @pytest.mark.parametrize(("conversation", "kind"), [
        ("D0DIRECT", "direct"), ("G0GROUP", "group"), ("C_UNWATCHED", "channel"),
    ])
    async def test_conversations_outside_the_source_grant_are_rejected(self, hosted, conversation, kind):
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(message_event(
            purpose="observe", conversation=conversation, conversation_kind=kind, message_id="1.5",
        ))

        await stream.nudge("observe")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "rejected"
        assert hosted.gateway.acks[-1][0]["reason_code"] == "policy_denied"
        assert await hosted.db.read_channel_observations("slack") == []

    async def test_the_observe_copy_of_a_handled_message_is_left_out(self, hosted):
        stream = await hosted.stream()
        invoke = hosted.gateway.store(message_event(mention_agent=True, message_id="1700001200.000100"))
        observe = hosted.gateway.store(message_event(
            purpose="observe", mention_agent=True, message_id="1700001200.000100",
        ))

        await stream.nudge("invoke")

        assert await hosted.acknowledged(invoke, observe) == {invoke: "accepted", observe: "rejected"}
        assert await hosted.db.read_channel_observations("slack") == []


# ---------------------------------------------------------------------- #
#  Pull delivery                                                          #
# ---------------------------------------------------------------------- #


class TestPullDelivery:
    async def test_nerve_reads_until_a_page_is_empty(self, tmp_path, db, monkeypatch):
        small_pages = ReaderSettings(**{**FAST_READER.__dict__, "maximum_events": 2})
        hosted = await start_hosted(tmp_path, db, monkeypatch, reader=small_pages)
        try:
            stream = await hosted.stream()
            await hosted.gateway.wait_for(lambda: len(hosted.gateway.reads) >= 1)
            reads_before = len(hosted.gateway.reads)
            ids = [
                hosted.gateway.store(message_event(
                    conversation="D0DIRECT", conversation_kind="direct", message_id=f"5.{n}",
                ))
                for n in range(5)
            ]

            await stream.nudge("invoke")

            await hosted.acknowledged(*ids)
            await hosted.gateway.wait_for(lambda: len(hosted.gateway.reads) >= reads_before + 4)
            pages = [len(ack) for ack in hosted.gateway.acks]
            assert pages == [2, 2, 1]
        finally:
            await stop_hosted(hosted)

    async def test_a_duplicate_page_is_acknowledged_without_a_second_turn(self, hosted):
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(message_event(
            conversation="D0DIRECT", conversation_kind="direct", message_id="6.1",
        ))
        await stream.nudge("invoke")
        await hosted.acknowledged(inbox_id)
        await hosted.turns.wait_for(1)

        hosted.gateway.acknowledged.clear()
        await stream.nudge("invoke")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "duplicate"
        await asyncio.sleep(0.1)
        assert len(hosted.turns.runs) == 1

    async def test_a_new_row_of_an_accepted_event_is_a_duplicate(self, hosted):
        stream = await hosted.stream()
        event = message_event(conversation="D0DIRECT", conversation_kind="direct", message_id="6.2")
        first = hosted.gateway.store(event)
        await stream.nudge("invoke")
        await hosted.acknowledged(first)
        await hosted.turns.wait_for(1)

        second = hosted.gateway.store(event)
        await stream.nudge("invoke")

        assert (await hosted.acknowledged(second))[second] == "duplicate"
        await asyncio.sleep(0.1)
        assert len(hosted.turns.runs) == 1

    async def test_one_event_id_on_two_connections_names_two_events(self, hosted):
        other_connection = str(uuid.uuid4())
        stream = await hosted.stream()
        await stream.advertise(connection_id=other_connection)
        first = hosted.gateway.store(message_event(
            conversation="D0FIRST", conversation_kind="direct", message_id="6.3", event_id="ev-shared",
        ))
        second = hosted.gateway.store(message_event(
            conversation="D0SECOND", conversation_kind="direct", message_id="6.4", event_id="ev-shared",
            connection_id=other_connection,
        ))

        await stream.nudge("invoke")

        assert await hosted.acknowledged(first, second) == {first: "accepted", second: "accepted"}

    async def test_a_lost_acknowledgement_is_sent_again(self, hosted):
        hosted.gateway.forget_acks = 1
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(message_event(
            conversation="D0DIRECT", conversation_kind="direct", message_id="7.1",
        ))

        await stream.nudge("invoke")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        assert [[item["inbox_id"] for item in ack] for ack in hosted.gateway.acks] == [[inbox_id], [inbox_id]]
        assert len(hosted.turns.runs) <= 1
        await hosted.turns.wait_for(1)

    async def test_a_lost_acknowledgement_result_is_sent_again(self, hosted):
        hosted.gateway.drop_ack_results = 1
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(message_event(
            conversation="D0DIRECT", conversation_kind="direct", message_id="7.2",
        ))

        await stream.nudge("invoke")
        await hosted.gateway.wait_for(lambda: len(hosted.gateway.acks) >= 2)

        assert hosted.gateway.acks[0] == hosted.gateway.acks[1]
        assert hosted.gateway.acknowledged[inbox_id] == "accepted"

    async def test_unavailable_reads_and_acknowledgements_are_retried(self, hosted):
        hosted.gateway.unavailable_reads = 2
        hosted.gateway.unavailable_acks = 1
        stream = await hosted.stream()
        inbox_id = hosted.gateway.store(message_event(
            conversation="D0DIRECT", conversation_kind="direct", message_id="7.3",
        ))

        await stream.nudge("invoke")

        assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        await hosted.turns.wait_for(1)

    async def test_a_nudge_during_an_outstanding_read_causes_one_more_read(self, hosted):
        stream = await hosted.stream(auto_serve=False)
        first = await stream.next_request("inbox_read")
        await stream.respond(first["request_id"], "inbox_read_result", {"outcome": "succeeded"})

        await stream.nudge("invoke")
        outstanding = await stream.next_request("inbox_read")
        await stream.nudge("invoke")
        await asyncio.sleep(0.05)
        await stream.respond(outstanding["request_id"], "inbox_read_result", {"outcome": "succeeded"})

        again = await stream.next_request("inbox_read")
        await stream.respond(again["request_id"], "inbox_read_result", {"outcome": "succeeded"})
        with pytest.raises(TimeoutError):
            await stream.next_request("inbox_read", timeout=0.5)

    async def test_no_second_read_is_sent_while_a_page_is_unacknowledged(self, hosted):
        stream = await hosted.stream(auto_serve=False)
        first = await stream.next_request("inbox_read")
        page = hosted.gateway.page(20)
        inbox_id = hosted.gateway.store(message_event(
            conversation="D0DIRECT", conversation_kind="direct", message_id="7.4",
        ))
        page = hosted.gateway.page(20)
        await stream.nudge("invoke")
        await stream.respond(first["request_id"], "inbox_read_result", page)

        ack = await stream.next_request("inbox_ack")
        await stream.nudge("invoke")
        await asyncio.sleep(0.1)
        assert stream.requests.empty()
        await stream.respond(ack["request_id"], "inbox_ack_result", {"outcome": "succeeded"})

        assert ack["payload"]["inbox_ack"]["items"] == [{"inbox_id": inbox_id, "outcome": "accepted"}]
        await stream.next_request("inbox_read")

    async def test_observe_nudges_are_combined(self, hosted):
        stream = await hosted.stream()
        await hosted.gateway.wait_for(lambda: len(hosted.gateway.reads) >= 1)
        reads_before = len(hosted.gateway.reads)

        started = asyncio.get_running_loop().time()
        for _ in range(6):
            await stream.nudge("observe")
            await asyncio.sleep(0.02)
        await hosted.gateway.wait_for(lambda: len(hosted.gateway.reads) > reads_before)
        await asyncio.sleep(0.1)

        # One read per observe interval at most: the first at once, then one
        # more for the nudges that arrived while it was outstanding.
        window = asyncio.get_running_loop().time() - started
        allowed = 2 + int(window / FAST_READER.observe_interval)
        assert 1 <= len(hosted.gateway.reads) - reads_before <= allowed

    async def test_nerve_reads_on_its_own_when_nudges_are_lost(self, tmp_path, db, monkeypatch):
        polling = ReaderSettings(**{**FAST_READER.__dict__, "poll_interval": 0.2})
        hosted = await start_hosted(tmp_path, db, monkeypatch, reader=polling)
        try:
            await hosted.stream()
            inbox_id = hosted.gateway.store(message_event(
                conversation="D0DIRECT", conversation_kind="direct", message_id="7.5",
            ))

            assert (await hosted.acknowledged(inbox_id))[inbox_id] == "accepted"
        finally:
            await stop_hosted(hosted)

    async def test_a_read_past_its_deadline_is_sent_again(self, hosted):
        stream = await hosted.stream(auto_serve=False)
        first = await stream.next_request("inbox_read")

        again = await stream.next_request("inbox_read")
        await stream.respond(first["request_id"], "inbox_read_result", {"outcome": "succeeded"})
        await stream.respond(again["request_id"], "inbox_read_result", {"outcome": "succeeded"})

        assert again["request_id"] != first["request_id"]
        await asyncio.sleep(0.1)
        assert not stream.closed.is_set()
