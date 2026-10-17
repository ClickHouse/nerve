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

import logging
import re

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
    def test_local_mode_leaves_the_environment_alone(self):
        environ = {CUSTOM_HEADERS_ENV: f"{INFERENCE_PRINCIPAL_HEADER}: {BOB.actor_id}"}
        assert custom_headers_env(ALICE.actor_id, environ) == {}

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


@pytest.mark.asyncio
async def test_local_mode_sends_no_header(engine, cli):
    await _turn(engine, "s1", ALICE)
    await _turn(engine, "s1", BOB)

    (process,) = cli
    assert CUSTOM_HEADERS_ENV not in process.env
    assert process.spec.inference_principal is None
    assert process.turns == 2
