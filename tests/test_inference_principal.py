"""The principal that each model request names, for a proxy in front of the API.

In external mode, every model request of a session carries one
``X-Nerve-Inference-Principal`` header with the session's cause: the person
who started the session, also in the turns of another person. The Claude CLI
takes the header from ``ANTHROPIC_CUSTOM_HEADERS`` in its environment, which
is fixed when the process starts; the session's cause never changes.

The engine tests drive the real ``run()``, ``_get_or_create_client`` and
``ClaudeBackend._build_options``; only the CLI process is replaced, by a
client that records the options it was started with.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import logging
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import pytest_asyncio
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

from nerve.agent.backends import claude as claude_backend
from nerve.agent.backends.claude import translate_message
from nerve.agent.engine import AgentEngine
from nerve.config import NerveConfig
from nerve.gateway.auth import AUTH_MODE_ENV, AUTH_MODE_EXTERNAL, pin_auth_mode
from nerve.identity import Actor
from nerve.inference_principal import (
    CUSTOM_HEADERS_ENV,
    INFERENCE_PRINCIPAL_HEADER,
    custom_headers_env,
    inference_principal,
    principal_headers,
)
from tests.actor_rows import ensure_actor_row

ALICE = Actor(
    actor_id="6f1c2a8e-3b4d-4e5f-8a9b-0c1d2e3f4a5b", kind="human",
    display_name="Alice",
)
BOB = Actor(
    actor_id="0a9b8c7d-6e5f-4a3b-9c2d-1e0f9a8b7c6d", kind="human",
    display_name="Bob",
)


def _cli_headers(value: str) -> list[tuple[str, str]]:
    """Parse ``ANTHROPIC_CUSTOM_HEADERS`` the way the bundled Claude CLI does.

    The CLI splits the value on ``\\n`` or ``\\r\\n``, skips blank lines and
    lines with no ``:``, and splits each line on its first ``:``; name and
    value are trimmed.
    """
    headers = []
    for line in re.split(r"\n|\r\n", value):
        if not line.strip() or ":" not in line:
            continue
        name, _, rest = line.partition(":")
        headers.append((name.strip(), rest.strip()))
    return headers


def _principals(env: dict[str, str]) -> list[str]:
    return [
        value for name, value in _cli_headers(env.get(CUSTOM_HEADERS_ENV, ""))
        if name.lower() == INFERENCE_PRINCIPAL_HEADER.lower()
    ]


@pytest.fixture
def external(monkeypatch):
    monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
    pin_auth_mode(AUTH_MODE_EXTERNAL)


@pytest.fixture(autouse=True)
def _no_inherited_custom_headers(monkeypatch):
    monkeypatch.delenv(CUSTOM_HEADERS_ENV, raising=False)


# --------------------------------------------------------------------------- #
#  The header value and the CLI environment                                   #
# --------------------------------------------------------------------------- #


class TestPrincipal:
    def test_local_mode_names_no_principal(self):
        assert inference_principal(ALICE.actor_id) is None
        assert principal_headers(ALICE.actor_id) == {}

    def test_external_mode_names_the_actor(self, external):
        assert inference_principal(ALICE.actor_id) == ALICE.actor_id
        assert principal_headers(ALICE.actor_id) == {
            INFERENCE_PRINCIPAL_HEADER: ALICE.actor_id,
        }

    def test_no_actor_names_no_principal(self, external):
        assert inference_principal(None) is None
        assert principal_headers(None) == {}

    @pytest.mark.parametrize("bad", [
        ALICE.actor_id.upper(),
        "not-a-uuid",
        f"{ALICE.actor_id}\nX-Other: 1",
        "",
    ])
    def test_an_id_that_is_not_a_canonical_uuid_names_no_principal(
        self, external, bad, caplog,
    ):
        with caplog.at_level(logging.WARNING, logger="nerve.inference_principal"):
            assert inference_principal(bad) is None
        assert "not a canonical lower-case UUID" in caplog.text


class TestCliEnvironment:
    def test_local_mode_removes_an_inherited_principal_and_keeps_the_rest(self):
        environ = {CUSTOM_HEADERS_ENV: (
            f"X-Proxy-Tag: blue\nx-nerve-inference-principal: {BOB.actor_id}"
        )}
        assert custom_headers_env(ALICE.actor_id, environ) == {
            CUSTOM_HEADERS_ENV: "X-Proxy-Tag: blue",
        }

    def test_local_mode_leaves_other_custom_headers_alone(self):
        environ = {CUSTOM_HEADERS_ENV: "X-Proxy-Tag: blue\n\nX-Other: 1"}
        assert custom_headers_env(ALICE.actor_id, environ) == {}
        assert custom_headers_env(None, {}) == {}

    def test_one_header_line_for_the_principal(self, external):
        env = custom_headers_env(ALICE.actor_id, {})
        assert env == {
            CUSTOM_HEADERS_ENV: f"{INFERENCE_PRINCIPAL_HEADER}: {ALICE.actor_id}",
        }
        assert _principals(env) == [ALICE.actor_id]

    def test_inherited_headers_are_kept_and_an_inherited_principal_replaced(
        self, external,
    ):
        environ = {CUSTOM_HEADERS_ENV: (
            "X-Proxy-Tag: blue\r\n"
            f"x-nerve-inference-principal: {BOB.actor_id}\n"
            "\n"
            "X-Other: 1"
        )}
        env = custom_headers_env(ALICE.actor_id, environ)
        assert _cli_headers(env[CUSTOM_HEADERS_ENV]) == [
            ("X-Proxy-Tag", "blue"),
            ("X-Other", "1"),
            (INFERENCE_PRINCIPAL_HEADER, ALICE.actor_id),
        ]

    def test_no_principal_removes_an_inherited_one(self, external):
        environ = {CUSTOM_HEADERS_ENV: f"{INFERENCE_PRINCIPAL_HEADER}: {BOB.actor_id}"}
        assert custom_headers_env(None, environ) == {CUSTOM_HEADERS_ENV: ""}

    def test_no_principal_and_nothing_to_remove_changes_nothing(self, external):
        assert custom_headers_env(None, {}) == {}
        assert custom_headers_env(None, {CUSTOM_HEADERS_ENV: "X-Proxy-Tag: blue"}) == {}

    def test_an_inherited_line_that_already_names_the_principal_changes_nothing(
        self, external,
    ):
        environ = {CUSTOM_HEADERS_ENV: (
            f"X-Proxy-Tag: blue\n{INFERENCE_PRINCIPAL_HEADER}: {ALICE.actor_id}"
        )}
        assert custom_headers_env(ALICE.actor_id, environ) == {}

    def test_a_value_that_is_not_a_canonical_uuid_is_never_written(self, external):
        assert custom_headers_env(f"{ALICE.actor_id}\nX-Injected: 1", {}) == {}

    def test_the_process_environment_is_the_default(self, external, monkeypatch):
        monkeypatch.setenv(CUSTOM_HEADERS_ENV, "X-Proxy-Tag: blue")
        env = custom_headers_env(ALICE.actor_id)
        assert env[CUSTOM_HEADERS_ENV] == (
            f"X-Proxy-Tag: blue\n{INFERENCE_PRINCIPAL_HEADER}: {ALICE.actor_id}"
        )


# --------------------------------------------------------------------------- #
#  Sessions: the CLI process names the session's cause                        #
# --------------------------------------------------------------------------- #


class _FakeCli:
    """Stands in for ``ClaudeClient``: one per CLI process the engine starts.

    Records the SDK options (environment, resume target). Each turn answers
    one text block and a result that carries this process's native session
    id, which the engine stores for resume.
    """

    started: list[_FakeCli] = []

    def __init__(self, spec, options):
        self.spec = spec
        self.options = options
        self.env = dict(options.env)
        self.model = options.model or ""
        self.index = len(self.started)
        self._native_id = spec.resume_native_id or f"native-{self.index}"
        self.turns = 0
        self.disconnected = False
        self.started.append(self)

    @property
    def native_session_id(self):
        return self._native_id

    async def connect(self):
        pass

    async def start_turn(self, turn):
        self.turns += 1

    async def receive_turn(self):
        for msg in (
            AssistantMessage(content=[TextBlock(text="ok")], model=self.model),
            ResultMessage(
                subtype="success", duration_ms=1, duration_api_ms=1,
                is_error=False, num_turns=1, session_id=self._native_id,
                total_cost_usd=0.0, usage={"input_tokens": 1},
            ),
        ):
            for event in translate_message(msg):
                yield event

    async def interrupt(self):
        pass

    def is_alive(self):
        return not self.disconnected

    async def disconnect(self, timeout: float = 5.0):
        self.disconnected = True

    def try_receive_idle_events(self):
        return None

    def buffer_used(self):
        return 0


@pytest.fixture
def cli(monkeypatch) -> list[_FakeCli]:
    started: list[_FakeCli] = []
    monkeypatch.setattr(_FakeCli, "started", started)
    monkeypatch.setattr(claude_backend, "ClaudeClient", _FakeCli)
    # The real check looks for the CLI's transcript under ~/.claude.
    monkeypatch.setattr(
        claude_backend.ClaudeBackend, "validate_resume_target",
        lambda self, native_id, cwd: native_id.startswith("native-"),
    )
    return started


@pytest_asyncio.fixture
async def engine(tmp_path, db):
    await ensure_actor_row(db, ALICE, BOB)
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "agent": {"backend": "claude"},
        "codex": {"home_dir": str(tmp_path / "codex-home")},
    })
    config.workspace.mkdir(parents=True, exist_ok=True)
    engine = AgentEngine(config, db)
    yield engine
    for session_id in list(engine._idle_watchers):
        engine._stop_idle_watcher(session_id)


async def _turn(engine: AgentEngine, session_id: str, actor, text: str = "hi", **kw):
    return await engine.run(
        session_id, text, source="web", channel="web", actor=actor, **kw,
    )


@pytest.mark.asyncio
async def test_a_session_started_by_alice_names_alice_on_every_request(
    external, engine, cli, db,
):
    await _turn(engine, "s1", ALICE)
    # Bob's turns in Alice's session run in the same process, for Alice.
    await _turn(engine, "s1", BOB, "my turn")
    await _turn(engine, "s1", BOB, "again")

    (process,) = cli
    assert _principals(process.env) == [ALICE.actor_id]
    assert process.spec.inference_principal == ALICE.actor_id
    assert process.turns == 3
    assert (await db.get_session("s1"))["caused_by_actor_id"] == ALICE.actor_id


@pytest.mark.asyncio
async def test_a_restarted_engine_names_the_stored_cause_on_its_first_client(
    external, engine, cli, db,
):
    from nerve.db import Database

    # Alice started the session before a restart.
    await db.create_session("s1", source="web", actor=ALICE)
    restarted_db = Database(db.db_path)
    await restarted_db.connect()
    restarted = AgentEngine(engine.config, restarted_db)
    try:
        await _turn(restarted, "s1", BOB)
    finally:
        for session_id in list(restarted._idle_watchers):
            restarted._stop_idle_watcher(session_id)
        await restarted_db.close()

    (process,) = cli
    assert _principals(process.env) == [ALICE.actor_id]


@pytest.mark.asyncio
async def test_a_cron_session_names_the_system_actor(external, engine, cli, db):
    await engine.run_cron(job_id="nightly", prompt="tidy up")

    (process,) = cli
    assert _principals(process.env) == [db.system_actor_id]


@pytest.mark.asyncio
async def test_a_session_with_no_cause_names_no_principal(
    external, engine, cli, monkeypatch,
):
    # A channel session has no known person; an inherited principal line is
    # removed, so the CLI sends none.
    monkeypatch.setenv(CUSTOM_HEADERS_ENV, f"{INFERENCE_PRINCIPAL_HEADER}: {BOB.actor_id}")
    await engine.run(
        "chat-app", "hello from a chat app", source="telegram", actor=None,
    )

    (process,) = cli
    assert process.env[CUSTOM_HEADERS_ENV] == ""
    assert _principals(process.env) == []
    assert process.spec.inference_principal is None


@pytest.mark.asyncio
async def test_a_model_change_rebuild_keeps_the_sessions_principal(
    external, engine, cli,
):
    await _turn(engine, "s1", ALICE)
    await _turn(engine, "s1", BOB, "use another model", model="claude-other-model")

    first, second = cli
    assert first.disconnected
    assert second.model == "claude-other-model"
    assert second.options.resume == "native-0"
    assert _principals(second.env) == [ALICE.actor_id]


@pytest.mark.asyncio
async def test_a_dead_client_rebuild_keeps_the_sessions_principal(
    external, engine, cli,
):
    await _turn(engine, "s1", ALICE)
    cli[0].disconnected = True  # the CLI process ended

    await _turn(engine, "s1", BOB)

    assert len(cli) == 2
    assert cli[1].options.resume == "native-0"
    assert _principals(cli[1].env) == [ALICE.actor_id]


@pytest.mark.asyncio
async def test_inherited_custom_headers_reach_the_cli(external, engine, cli, monkeypatch):
    monkeypatch.setenv(CUSTOM_HEADERS_ENV, "X-Proxy-Tag: blue")
    await _turn(engine, "s1", ALICE)

    (process,) = cli
    assert _cli_headers(process.env[CUSTOM_HEADERS_ENV]) == [
        ("X-Proxy-Tag", "blue"), (INFERENCE_PRINCIPAL_HEADER, ALICE.actor_id),
    ]


async def _process_env(options, monkeypatch) -> dict[str, str]:
    """The environment that the SDK gives the CLI process it starts for ``options``.

    Runs the SDK's own spawn path up to the process start, which is replaced.
    """
    from claude_agent_sdk._internal.transport import subprocess_cli

    captured: dict[str, str] = {}

    async def _open_process(cmd, **kwargs):
        captured.update(kwargs["env"])
        raise OSError("no CLI process in tests")

    monkeypatch.setattr(subprocess_cli.anyio, "open_process", _open_process)
    monkeypatch.setenv("CLAUDE_AGENT_SDK_SKIP_VERSION_CHECK", "1")
    transport = subprocess_cli.SubprocessCLITransport(
        prompt="", options=dataclasses.replace(options, cli_path="/bin/true"),
    )
    with contextlib.suppress(Exception):
        await transport.connect()
    assert captured, "the SDK did not reach the process start"
    return captured


_INHERITED = f"X-Proxy-Tag: blue\nx-nerve-inference-principal: {BOB.actor_id}"


@pytest.mark.asyncio
async def test_local_mode_cli_process_inherits_no_principal(engine, cli, monkeypatch):
    monkeypatch.setenv(CUSTOM_HEADERS_ENV, _INHERITED)
    await _turn(engine, "s1", ALICE)

    env = await _process_env(cli[0].options, monkeypatch)
    assert _cli_headers(env[CUSTOM_HEADERS_ENV]) == [("X-Proxy-Tag", "blue")]


@pytest.mark.asyncio
async def test_external_mode_cli_process_names_only_the_sessions_cause(
    external, engine, cli, monkeypatch,
):
    monkeypatch.setenv(CUSTOM_HEADERS_ENV, _INHERITED)
    await _turn(engine, "s1", ALICE)

    env = await _process_env(cli[0].options, monkeypatch)
    assert _cli_headers(env[CUSTOM_HEADERS_ENV]) == [
        ("X-Proxy-Tag", "blue"), (INFERENCE_PRINCIPAL_HEADER, ALICE.actor_id),
    ]


@pytest.mark.asyncio
async def test_local_mode_sends_no_header(engine, cli):
    await _turn(engine, "s1", ALICE)
    await _turn(engine, "s1", BOB)

    (process,) = cli
    assert CUSTOM_HEADERS_ENV not in process.env
    assert process.spec.inference_principal is None
    assert process.turns == 2


# --------------------------------------------------------------------------- #
#  Nerve's own model clients name the system actor                            #
# --------------------------------------------------------------------------- #


@pytest.fixture
def global_db(db, monkeypatch):
    """The database as the server opens it (``init_db``)."""
    import nerve.db

    monkeypatch.setattr(nerve.db, "_db", db)
    return db


def _direct_clients(config: NerveConfig) -> list:
    return [config.create_anthropic_client(), config.create_async_anthropic_client()]


def _bedrock_config() -> NerveConfig:
    return NerveConfig.from_dict({
        "provider": {"type": "bedrock", "aws_region": "us-east-1"},
    })


def test_the_global_system_actor_id(db, monkeypatch):
    import nerve.db

    monkeypatch.setattr(nerve.db, "_db", None)
    assert nerve.db.global_system_actor_id() is None
    monkeypatch.setattr(nerve.db, "_db", db)
    assert nerve.db.global_system_actor_id() == db.system_actor_id


@pytest.mark.asyncio
async def test_a_closed_global_database_has_no_system_actor_id(tmp_path, monkeypatch):
    import nerve.db
    from nerve.db import Database

    closed = Database(tmp_path / "closed.db")
    await closed.connect()
    await closed.close()
    monkeypatch.setattr(nerve.db, "_db", closed)
    assert nerve.db.global_system_actor_id() is None


def test_direct_clients_name_the_system_actor(external, global_db):
    for config in (NerveConfig(), _bedrock_config()):
        for client in _direct_clients(config):
            assert client.default_headers[INFERENCE_PRINCIPAL_HEADER] == (
                global_db.system_actor_id
            ), type(client).__name__


def test_direct_clients_name_no_principal_in_local_mode(global_db):
    for config in (NerveConfig(), _bedrock_config()):
        for client in _direct_clients(config):
            assert INFERENCE_PRINCIPAL_HEADER not in client.default_headers


def test_direct_clients_without_a_database_name_no_principal(external, monkeypatch):
    import nerve.db

    monkeypatch.setattr(nerve.db, "_db", None)
    for client in _direct_clients(NerveConfig()):
        assert INFERENCE_PRINCIPAL_HEADER not in client.default_headers


def test_memory_bedrock_clients_name_the_system_actor(external, global_db):
    from nerve.memory.memu_bridge import MemUBridge

    bridge = MemUBridge.__new__(MemUBridge)
    bridge.config = _bedrock_config()
    client = bridge._make_bedrock_client("claude-test")
    assert client._bedrock.default_headers[INFERENCE_PRINCIPAL_HEADER] == (
        global_db.system_actor_id
    )


class _MemoryService:
    """memU's cache of OpenAI clients, one per profile, made when first asked.

    Each client sends its requests to an in-process transport that records
    them in ``sent``.
    """

    def __init__(self, sent: list) -> None:
        from types import SimpleNamespace

        self.llm_profiles = SimpleNamespace(
            profiles=dict.fromkeys(("default", "fast", "embedding")),
        )
        self._llm_clients: dict = {}
        self._sent = sent

    def _get_llm_base_client(self, profile: str | None = None):
        import httpx
        from memu.llm.openai_sdk import OpenAISDKClient
        from openai import AsyncOpenAI

        name = profile or "default"
        if name not in self.llm_profiles.profiles:
            raise KeyError(f"Unknown llm profile '{name}'")
        if name in self._llm_clients:
            return self._llm_clients[name]

        def handler(request: httpx.Request) -> httpx.Response:
            self._sent.append(request)
            return httpx.Response(200, json={
                "id": "chatcmpl-1", "object": "chat.completion", "created": 0,
                "model": "claude-test",
                "choices": [{
                    "index": 0, "finish_reason": "stop",
                    "message": {"role": "assistant", "content": "ok"},
                }],
            })

        client = OpenAISDKClient(
            base_url="https://api.example.test/v1", api_key="test",
            chat_model="claude-test", embed_model="",
        )
        client.client = AsyncOpenAI(
            api_key="test", base_url="https://api.example.test/v1",
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        self._llm_clients[name] = client
        return client


def _memory_bridge(sent: list):
    from nerve.memory.memu_bridge import MemUBridge

    bridge = MemUBridge(NerveConfig())
    bridge._service = _MemoryService(sent)
    return bridge


@pytest.mark.asyncio
async def test_memory_openai_clients_name_the_system_actor(external, global_db):
    sent: list = []
    bridge = _memory_bridge(sent)
    embedding = bridge._service._get_llm_base_client("embedding")
    bridge._name_system_principal()
    for profile in ("default", "fast"):
        await bridge._service._get_llm_base_client(profile).chat("hello")

    assert [r.headers.get(INFERENCE_PRINCIPAL_HEADER) for r in sent] == (
        [global_db.system_actor_id] * 2
    )
    assert INFERENCE_PRINCIPAL_HEADER not in embedding.client.default_headers


@pytest.mark.asyncio
async def test_memory_openai_clients_name_no_principal_in_local_mode(global_db):
    sent: list = []
    bridge = _memory_bridge(sent)
    bridge._name_system_principal()
    await bridge._service._get_llm_base_client("fast").chat("hello")

    (request,) = sent
    assert INFERENCE_PRINCIPAL_HEADER not in request.headers


@pytest.mark.asyncio
async def test_memory_openai_clients_name_the_system_actor_after_a_reset(
    external, global_db,
):
    """A reset evicts the clients, and memU makes new ones without headers."""
    sent: list = []
    bridge = _memory_bridge(sent)
    bridge._name_system_principal()
    before = bridge._service._get_llm_base_client("fast")
    await bridge._reset_llm_clients_impl()
    after = bridge._service._get_llm_base_client("fast")
    await after.chat("hello")

    assert after is not before
    # The health probe, the warm-up, and the chat.
    assert len(sent) == 3
    assert {r.headers.get(INFERENCE_PRINCIPAL_HEADER) for r in sent} == {
        global_db.system_actor_id,
    }


def _keyed_config() -> NerveConfig:
    return NerveConfig.from_dict({"anthropic_api_key": "test-key-not-real"})


def _sent_headers(client) -> list[tuple[str, str]]:
    """The headers that a request of an Anthropic SDK client carries."""
    from anthropic._models import FinalRequestOptions

    request = client._build_request(FinalRequestOptions.construct(
        method="post", url="/v1/messages", json_data={},
    ))
    return list(request.headers.multi_items())


def _principal_values(headers: list[tuple[str, str]]) -> list[str]:
    return [
        value for name, value in headers
        if name.lower() == INFERENCE_PRINCIPAL_HEADER.lower()
    ]


def test_direct_requests_in_local_mode_drop_an_inherited_principal(
    global_db, monkeypatch,
):
    monkeypatch.setenv(CUSTOM_HEADERS_ENV, _INHERITED)
    for client in _direct_clients(_keyed_config()):
        headers = _sent_headers(client)
        assert _principal_values(headers) == []
        assert ("x-proxy-tag", "blue") in headers


def test_direct_requests_in_external_mode_name_the_system_actor_once(
    external, global_db, monkeypatch,
):
    monkeypatch.setenv(CUSTOM_HEADERS_ENV, _INHERITED)
    for client in _direct_clients(_keyed_config()):
        headers = _sent_headers(client)
        assert _principal_values(headers) == [global_db.system_actor_id]
        assert ("x-proxy-tag", "blue") in headers


# --------------------------------------------------------------------------- #
#  The model probe of `nerve doctor` names the stored system actor            #
# --------------------------------------------------------------------------- #


class _MessagesApi(BaseHTTPRequestHandler):
    """A stand-in for the Messages API that records request headers."""

    received: list[list[tuple[str, str]]] = []

    def do_POST(self):  # noqa: N802 - http.server naming
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        type(self).received.append(list(self.headers.items()))
        body = json.dumps({
            "id": "msg_test", "type": "message", "role": "assistant",
            "model": "claude-test", "content": [{"type": "text", "text": "ok"}],
            "stop_reason": "end_turn", "stop_sequence": None,
            "usage": {"input_tokens": 1, "output_tokens": 1},
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def messages_api(monkeypatch):
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy",
                 "https_proxy", "all_proxy"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(_MessagesApi, "received", [])
    server = ThreadingHTTPServer(("127.0.0.1", 0), _MessagesApi)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield NerveConfig.from_dict({
            "anthropic_base_url": f"http://127.0.0.1:{server.server_port}",
            "anthropic_api_key": "test-key-not-real",
        })
    finally:
        server.shutdown()
        server.server_close()


async def _stored_system_actor_id() -> str:
    """Create the server's database where the doctor reads it."""
    from nerve import paths
    from nerve.db import Database

    database = Database(paths.db_path())
    await database.connect()
    try:
        return database.system_actor_id
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_the_doctor_probe_names_the_stored_system_actor(
    messages_api, monkeypatch,
):
    from nerve.cli import _check_api_connectivity

    system_actor_id = await _stored_system_actor_id()
    # The doctor reads the mode from its own shell; nothing is pinned.
    monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
    monkeypatch.setenv(CUSTOM_HEADERS_ENV, _INHERITED)

    ok, _ = await asyncio.to_thread(_check_api_connectivity, messages_api)

    assert ok
    (headers,) = _MessagesApi.received
    assert _principal_values(headers) == [system_actor_id]


@pytest.mark.asyncio
async def test_the_doctor_probe_names_no_principal_in_local_mode(
    messages_api, monkeypatch,
):
    from nerve.cli import _check_api_connectivity

    await _stored_system_actor_id()
    monkeypatch.setenv(CUSTOM_HEADERS_ENV, _INHERITED)

    ok, _ = await asyncio.to_thread(_check_api_connectivity, messages_api)

    assert ok
    (headers,) = _MessagesApi.received
    assert _principal_values(headers) == []


def test_the_doctor_probe_without_a_database_names_no_principal(
    messages_api, monkeypatch,
):
    from nerve.cli import _check_api_connectivity

    monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)

    ok, _ = _check_api_connectivity(messages_api)

    assert ok
    (headers,) = _MessagesApi.received
    assert _principal_values(headers) == []


@pytest.mark.asyncio
async def test_the_stored_system_actor_is_read_only_when_there_is_exactly_one(tmp_path):
    from nerve.db import Database
    from nerve.db.accounts import read_system_actor_id

    assert read_system_actor_id(tmp_path / "missing.db") is None
    database = Database(tmp_path / "nerve.db")
    await database.connect()
    try:
        assert read_system_actor_id(tmp_path / "nerve.db") == database.system_actor_id
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_a_cli_command_with_its_own_connection_names_the_system_actor(
    messages_api, monkeypatch, tmp_path,
):
    """`nerve sync`: its own database connection, no global one."""
    import nerve.db
    from nerve.migrate import open_production_db
    from nerve.sources.registry import build_source_runners

    assert nerve.db.global_system_actor_id() is None
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "anthropic_base_url": messages_api.anthropic_base_url,
        "anthropic_api_key": "test-key-not-real",
        "memory": {"fast_model": "claude-test"},
        "sync": {"github": {"enabled": True}},
    })
    monkeypatch.setenv(AUTH_MODE_ENV, AUTH_MODE_EXTERNAL)
    db = await open_production_db(config)
    try:
        (runner,) = build_source_runners(config, db)
        client = runner._client_factory()
        await client.messages.create(
            model="claude-test", max_tokens=1,
            messages=[{"role": "user", "content": "condense"}],
        )
        system_actor_id = db.system_actor_id
    finally:
        await db.close()

    (headers,) = _MessagesApi.received
    assert _principal_values(headers) == [system_actor_id]
