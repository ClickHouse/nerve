"""The per-chat Daybreak switch of the Codex backend.

Daybreak is Codex's account-gated cyber access program: not a model, but a
treatment that a client asks for per turn (``turn/start``
``cyberAccessProgram``). The switch is session metadata (``codex_daybreak``).

The first rule is that off changes nothing: the requests of a chat with the
switch off are pinned here parameter by parameter. Like
``test_codex_appserver.py``, the client tests run against
``tests/fixtures/fake_codex_appserver.py``. Everything runs offline.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from fastapi import HTTPException

from nerve.agent.backends import events as ev
from nerve.agent.backends.base import (
    BackendCapabilities, TransportDiedError, TurnInput, TurnRefusedError,
)
from nerve.agent.backends.codex import CodexBackend, daybreak, worker_wrapper
from nerve.agent.backends.codex.appserver import (
    _OPT_OUT_NOTIFICATIONS, CodexRpcError,
)
from nerve.agent.engine import AgentEngine
from nerve.config import NerveConfig
from tests.actor_rows import ensure_actor_row, ensure_system_principal
from tests.conftest import TEST_ACTOR
from tests.test_codex_appserver import _collect_turn, _config, _deps, _mode, _spec

# The fake app-server's catalog: one visible model and one hidden model.
SOL = "gpt-5.6-sol"
ASTRA = "gpt-6-astra"

BLUE = {"cyber": ["standard", "daybreakBlue"]}
STANDARD = {"cyber": ["standard"]}

HINT = "Daybreak is available for this chat and can be turned on in the composer."


def _access_programs(monkeypatch, programs: dict) -> None:
    """What the fake app-server's ``model/list`` advertises per model."""
    monkeypatch.setenv("FAKE_CODEX_ACCESS_PROGRAMS", json.dumps(programs))


def _saved_on_the_thread(monkeypatch, saved) -> None:
    """What the thread of the fake app-server's ``thread/resume`` and
    ``thread/fork`` answers has saved as ``daybreakEnabled``. ``...``: the
    thread says nothing of it, as a Codex from before the field."""
    if saved is ...:
        monkeypatch.delenv("FAKE_CODEX_SAVED_DAYBREAK", raising=False)
    else:
        monkeypatch.setenv("FAKE_CODEX_SAVED_DAYBREAK", json.dumps(saved))


def _requests(tmp_path: Path) -> list[tuple]:
    """Every request the fake app-server received, in order, as
    ``(method, params)``."""
    log = tmp_path / "codex-home" / "requests.jsonl"
    requests = [json.loads(line) for line in log.read_text().splitlines()]
    return [(request["method"], request["params"]) for request in requests]


def _sent(tmp_path: Path, method: str | None = None) -> list:
    """The requests the fake app-server received: the methods in order, or
    the parameters of each request of one method."""
    requests = _requests(tmp_path)
    if method is None:
        return [name for name, _ in requests]
    return [params for name, params in requests if name == method]


# What a client sends before it has a thread: the transport's handshake, the
# account and the catalog. Nothing of Daybreak is in it.
_BEFORE_THE_THREAD = [
    ("initialize", {
        "clientInfo": {"name": "nerve", "title": "Nerve", "version": "1.0.0"},
        "capabilities": {
            "optOutNotificationMethods": _OPT_OUT_NOTIFICATIONS,
            "experimentalApi": True,
            "mcpServerOpenaiFormElicitation": True,
        },
    }),
    ("initialized", None),
    ("account/read", {}),
    ("model/list", {"includeHidden": True}),
]


def _turn_params(text: str, *, effort: str = "high", **extra) -> dict:
    """The ``turn/start`` parameters of a text turn, plus ``extra``."""
    return {
        "threadId": "th_fake_1",
        "input": [{"type": "text", "text": text}],
        "effort": effort,
        **extra,
    }


# --------------------------------------------------------------------------- #
#  The catalog digest                                                          #
# --------------------------------------------------------------------------- #


def _entry(model: str, **fields) -> dict:
    return {"id": model, "model": model, "hidden": False,
            "supportedReasoningEfforts": [], **fields}


class TestCatalogDigest:
    @pytest.mark.parametrize("programs, expected", [
        ({"cyber": ["standard", "daybreakBlue"]}, "daybreakBlue"),
        ({"cyber": ["standard", "daybreakRed"]}, "daybreakRed"),
        # Blue wins over Red, whatever the order of the list.
        ({"cyber": ["daybreakRed", "standard", "daybreakBlue"]}, "daybreakBlue"),
        ({"cyber": ["standard"]}, None),
        ({"cyber": []}, None),
        (None, None),
        # A program that this Nerve does not know is ignored.
        ({"cyber": ["standard", "daybreakGreen"]}, None),
        ({"cyber": ["daybreakGreen", "daybreakRed"]}, "daybreakRed"),
        # Not the documented shape: no program.
        ({"cyber": "daybreakBlue"}, None),
        (["daybreakBlue"], None),
    ])
    def test_the_program_of_a_model(self, tmp_path, programs, expected):
        backend = CodexBackend(_deps(_config(tmp_path)))
        catalog = backend.catalog([_entry("gpt-6-sol", availableAccessPrograms=programs)])
        assert catalog["daybreak"] == ({"gpt-6-sol": expected} if expected else {})

    def test_an_entry_without_the_key_has_no_program(self, tmp_path):
        backend = CodexBackend(_deps(_config(tmp_path)))
        catalog = backend.catalog([_entry("gpt-6-sol")])
        assert catalog["daybreak"] == {}
        assert catalog["access_metadata"] is False

    def test_each_model_has_its_own_program(self, tmp_path):
        backend = CodexBackend(_deps(_config(tmp_path)))
        catalog = backend.catalog([
            _entry("gpt-6-sol", availableAccessPrograms=BLUE),
            _entry("gpt-6-astra", hidden=True, availableAccessPrograms=STANDARD),
            _entry("gpt-6-luna", availableAccessPrograms={"cyber": ["daybreakRed"]}),
            _entry("gpt-6-mini", availableAccessPrograms=None),
        ])
        assert catalog["daybreak"] == {
            "gpt-6-sol": "daybreakBlue", "gpt-6-luna": "daybreakRed",
        }
        assert catalog["access_metadata"] is True

    @pytest.mark.parametrize("programs, metadata", [
        ({"cyber": ["standard"]}, True),
        ({"cyber": []}, True),          # an empty list is metadata
        (None, False),
        ({}, False),
    ])
    def test_which_catalogs_carry_access_metadata(self, tmp_path, programs, metadata):
        backend = CodexBackend(_deps(_config(tmp_path)))
        catalog = backend.catalog([_entry("gpt-6-sol", availableAccessPrograms=programs)])
        assert catalog["access_metadata"] is metadata

    @pytest.mark.asyncio
    async def test_preflight_reports_the_programs(self, tmp_path, monkeypatch):
        _mode(monkeypatch, "basic")
        monkeypatch.delenv("FAKE_CODEX_ACCESS_PROGRAMS", raising=False)
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        assert (await backend.preflight(force=True))["daybreak_programs"] == {}

        _access_programs(monkeypatch, {SOL: BLUE, ASTRA: STANDARD})
        status = await backend.preflight(force=True)
        assert status["daybreak_programs"] == {SOL: "daybreakBlue"}
        assert backend.daybreak_program(SOL) == "daybreakBlue"
        assert backend.daybreak_program(ASTRA) is None


# --------------------------------------------------------------------------- #
#  What the workers of a chat are told                                         #
# --------------------------------------------------------------------------- #

# A worker command of the kind that Ultracode builds.
_WORKER = ["exec", "--json", "-m", "gpt-6-sol", "-"]


def _codex_version(monkeypatch, version: str) -> None:
    """The version that the fake Codex CLI reports."""
    monkeypatch.setenv("FAKE_CODEX_VERSION", version)


@pytest.mark.asyncio
class TestWorkerEntry:
    """The entry of the worker-token answer for a chat that has the switch
    on, and what the worker wrapper makes of it. ``codex exec`` has the
    ``--cyber-access-program`` flag from 0.161.0; the catalog lists access
    programs from 0.156.0."""

    # The entry on a Codex whose ``exec`` has the flag.
    ENTRY = {
        "model": "gpt-6-sol",
        "programs": {"gpt-6-sol": "daybreak_blue", "gpt-6-luna": "daybreak_red"},
    }

    def _backend(self, tmp_path) -> CodexBackend:
        backend = CodexBackend(_deps(_config(tmp_path)))
        backend.note_access_programs(backend.catalog([
            _entry("gpt-6-sol", availableAccessPrograms=BLUE),
            _entry("gpt-6-luna", availableAccessPrograms={"cyber": ["daybreakRed"]}),
            _entry("gpt-6-astra", availableAccessPrograms=STANDARD),
        ]))
        return backend

    @pytest.mark.parametrize("version", ["0.161.0", "0.162.0", "0.164.0"])
    async def test_a_codex_whose_exec_has_the_flag(self, tmp_path, monkeypatch, version):
        """The programs in the spelling of the flag, and nothing else: the
        wrapper adds the flag to the worker."""
        _codex_version(monkeypatch, version)
        backend = self._backend(tmp_path)
        await backend._check_cli_version()

        entry = backend.worker_daybreak("gpt-6-sol")
        assert entry == self.ENTRY
        assert worker_wrapper._follow_daybreak(_WORKER, entry) == [
            "exec", "--cyber-access-program", "daybreak_blue", *_WORKER[1:],
        ]

    @pytest.mark.parametrize("version", ["0.156.0", "0.160.0", "0.160.1"])
    async def test_a_codex_whose_exec_has_no_flag(
        self, tmp_path, monkeypatch, capsys, version,
    ):
        """Such a Codex lists access programs, so the chat can have the
        switch on, and its ``exec`` stops on the flag. The entry says which
        version workers need, and the wrapper starts no worker."""
        _codex_version(monkeypatch, version)
        backend = self._backend(tmp_path)
        await backend._check_cli_version()

        entry = backend.worker_daybreak("gpt-6-sol")
        assert entry == {**self.ENTRY, "workers_need_codex": "0.161.0"}
        assert worker_wrapper._follow_daybreak(_WORKER, entry) is None
        assert "needs Codex 0.161.0 or newer" in capsys.readouterr().err

    async def test_a_codex_whose_version_is_not_known(self, tmp_path, capsys):
        """No version was read: nothing says that ``exec`` has the flag."""
        entry = self._backend(tmp_path).worker_daybreak("gpt-6-sol")
        assert entry == {**self.ENTRY, "workers_need_codex": "0.161.0"}
        assert worker_wrapper._follow_daybreak(_WORKER, entry) is None
        assert "needs Codex 0.161.0 or newer" in capsys.readouterr().err

    async def test_the_version_is_the_one_that_was_read_last(self, tmp_path, monkeypatch):
        """A client build and a preflight both read the version, as they
        always did: no process is started to answer a worker."""
        _mode(monkeypatch, "basic")
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))

        _codex_version(monkeypatch, "0.160.1")
        client = await backend.create_client(_spec(cfg))
        await client.disconnect()
        assert backend.worker_daybreak(SOL)["workers_need_codex"] == "0.161.0"

        _codex_version(monkeypatch, "0.164.0")
        assert (await backend.preflight(force=True))["version"] == "codex-cli 0.164.0"
        assert "workers_need_codex" not in backend.worker_daybreak(SOL)

        # Answering a worker reads nothing.
        _codex_version(monkeypatch, "0.160.1")
        assert "workers_need_codex" not in backend.worker_daybreak(SOL)


# --------------------------------------------------------------------------- #
#  The turn                                                                    #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestTurns:
    async def test_off_sends_what_it_sent_before_the_switch(self, tmp_path, monkeypatch):
        """Off is the default, also on an account that has Daybreak: the
        requests are the ones of a Nerve without the switch."""
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: BLUE})
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        spec = _spec(cfg)
        assert spec.extra == {}
        client = await backend.create_client(spec)
        try:
            await client.start_turn(TurnInput(text="hello"))
            done = (await _collect_turn(client))[-1]
            assert done.status == "completed"
        finally:
            await client.disconnect()

        # Every request, with its parameters.
        assert _requests(tmp_path) == [
            *_BEFORE_THE_THREAD,
            ("thread/start", backend.thread_params(spec)),
            ("turn/start", _turn_params("hello")),
        ]
        assert set(backend.thread_params(spec)) == {
            "cwd", "model", "sandbox", "approvalPolicy", "developerInstructions",
        }

    async def test_on_sends_the_program_of_the_model(self, tmp_path, monkeypatch, caplog):
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: BLUE})
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        spec = _spec(cfg, extra={"daybreak": True})
        client = await backend.create_client(spec)
        try:
            with caplog.at_level(logging.INFO, logger="nerve.agent.backends.codex.backend"):
                await client.start_turn(TurnInput(text="look at this binary"))
            done = (await _collect_turn(client))[-1]
            assert done.status == "completed"
        finally:
            await client.disconnect()

        assert _sent(tmp_path, "turn/start") == [
            _turn_params("look at this binary", cyberAccessProgram="daybreakBlue"),
        ]
        # Codex also saves the choice on the thread that it starts.
        assert _sent(tmp_path, "thread/start") == [
            {**backend.thread_params(spec), "daybreakEnabled": True},
        ]
        assert _sent(tmp_path, "thread/metadata/update") == []
        # One line per turn: the short session id, the model and the program,
        # and nothing of the prompt.
        assert [r.getMessage() for r in caplog.records if "program" in r.getMessage()] == [
            f"Codex session s1: turn sent on {SOL} with cyber access program daybreakBlue",
        ]
        assert "binary" not in caplog.text

    async def test_on_without_a_program_does_not_send_the_turn(self, tmp_path, monkeypatch):
        """The account has Daybreak on another model only. The turn is
        refused before ``turn/start``, and the error names the model. The
        runtime stays, and with the switch off its next turn is sent."""
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: BLUE, ASTRA: STANDARD})
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        client = await backend.create_client(
            _spec(cfg, model=ASTRA, extra={"daybreak": True}),
        )
        try:
            with pytest.raises(TurnRefusedError) as refused:
                await client.start_turn(TurnInput(text="hello"))
            assert _sent(tmp_path, "turn/start") == []
            assert not (tmp_path / "codex-home" / "last_turn_params.json").exists()
            assert client.is_alive()

            await client.set_daybreak(False)
            await client.start_turn(TurnInput(text="hello again"))
            assert (await _collect_turn(client))[-1].status == "completed"
        finally:
            await client.disconnect()

        assert str(refused.value) == (
            f"Daybreak is on for this chat, but model '{ASTRA}' has no Daybreak "
            "program for the signed-in Codex account. Turn Daybreak off or "
            "choose another model."
        )
        assert _sent(tmp_path, "turn/start") == [_turn_params("hello again")]
        assert _sent(tmp_path).count("initialize") == 1

    async def test_a_turn_is_refused_only_after_this_runtime_read_the_catalog_again(
        self, tmp_path, monkeypatch,
    ):
        """The backend keeps the catalog that an app-server listed last, and
        that can be another one (a preflight, another chat). A copy without
        the program does not refuse the turn: the runtime of the chat reads
        its own catalog first."""
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: BLUE})
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        client = await backend.create_client(_spec(cfg, extra={"daybreak": True}))
        try:
            backend.note_access_programs(backend.catalog([]))
            assert backend.daybreak_program(SOL) is None
            await client.start_turn(TurnInput(text="hello"))
            assert (await _collect_turn(client))[-1].status == "completed"
        finally:
            await client.disconnect()

        assert _sent(tmp_path, "turn/start") == [
            _turn_params("hello", cyberAccessProgram="daybreakBlue"),
        ]
        assert _sent(tmp_path) == [
            "initialize", "initialized", "account/read", "model/list",
            "thread/start", "model/list", "turn/start",
        ]

    @pytest.mark.parametrize("failure", [
        TransportDiedError("codex app-server did not answer model/list within 10s"),
        CodexRpcError(-32603, "the model catalog is unavailable"),
    ], ids=["no-answer", "error-answer"])
    async def test_a_catalog_that_cannot_be_read_again_still_refuses_with_the_reason(
        self, tmp_path, monkeypatch, caplog, failure,
    ):
        """The look at the catalog before a refusal can fail: no answer in
        time, an error answer. The turn is then refused for what the backend
        knows, with the Daybreak reason, and the backend keeps what it knows."""
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: BLUE, ASTRA: STANDARD})
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        client = await backend.create_client(
            _spec(cfg, model=ASTRA, extra={"daybreak": True}),
        )
        request = client._transport.request

        async def no_catalog(method, params=None, timeout=None):
            if method == "model/list":
                raise failure
            return await request(method, params, timeout)

        monkeypatch.setattr(client._transport, "request", no_catalog)
        try:
            with caplog.at_level(logging.WARNING, logger="nerve.agent.backends.codex.backend"):
                reason = await client.turn_refusal()
                with pytest.raises(TurnRefusedError) as refused:
                    await client.start_turn(TurnInput(text="hello"))
        finally:
            await client.disconnect()

        assert reason == str(refused.value) == (
            f"Daybreak is on for this chat, but model '{ASTRA}' has no Daybreak "
            "program for the signed-in Codex account. Turn Daybreak off or "
            "choose another model."
        )
        assert "the model catalog could not be read again" in caplog.text
        assert backend.daybreak_program(SOL) == "daybreakBlue"
        assert _sent(tmp_path, "turn/start") == []

    async def test_a_runtime_that_died_refuses_with_the_reason_too(
        self, tmp_path, monkeypatch,
    ):
        """Nothing can be asked of a runtime that is gone. The check of the
        turn still answers with the Daybreak reason, not with a crash."""
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: STANDARD})
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        client = await backend.create_client(_spec(cfg, extra={"daybreak": True}))
        try:
            await client._transport.close()
            assert not client.is_alive()
            assert "has no Daybreak program" in await client.turn_refusal()
        finally:
            await client.disconnect()

    async def test_the_setter_changes_the_next_turn_of_the_same_client(
        self, tmp_path, monkeypatch,
    ):
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: BLUE})
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        client = await backend.create_client(_spec(cfg))
        try:
            for switch, text in ((None, "one"), (True, "two"), (False, "three")):
                if switch is not None:
                    await client.set_daybreak(switch)
                await client.start_turn(TurnInput(text=text))
                assert (await _collect_turn(client))[-1].status == "completed"
            assert client.native_session_id == "th_fake_1"
        finally:
            await client.disconnect()

        assert _sent(tmp_path, "turn/start") == [
            _turn_params("one"),
            _turn_params("two", cyberAccessProgram="daybreakBlue"),
            _turn_params("three"),
        ]
        # One runtime and one thread for all three turns.
        assert _sent(tmp_path).count("initialize") == 1
        assert len(_sent(tmp_path, "thread/start")) == 1
        # Each change is saved on the live thread.
        assert _sent(tmp_path, "thread/metadata/update") == [
            {"threadId": "th_fake_1", "daybreakEnabled": True},
            {"threadId": "th_fake_1", "daybreakEnabled": False},
        ]

    async def test_the_program_follows_the_model_of_the_thread_after_a_reroute(
        self, tmp_path, monkeypatch,
    ):
        """A reroute changes the model that served one answer. The next turn
        still asks for the thread's model, and for its program."""
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: BLUE})
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        client = await backend.create_client(_spec(cfg, extra={"daybreak": True}))
        try:
            await client._map_notification("model/rerouted", {"toModel": "gpt-5.2"})
            assert client.model == "gpt-5.2"
            await client.start_turn(TurnInput(text="hello"))
            await _collect_turn(client)
        finally:
            await client.disconnect()
        assert _sent(tmp_path, "turn/start")[0]["cyberAccessProgram"] == "daybreakBlue"


# --------------------------------------------------------------------------- #
#  The choice saved on the Codex thread                                        #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestThreadLevelSave:
    async def test_a_catalog_without_access_metadata_gets_no_thread_field(
        self, tmp_path, monkeypatch,
    ):
        """An older Codex lists no access programs and does not know the
        thread field: with the switch on, nothing of it is sent."""
        _mode(monkeypatch, "basic")
        monkeypatch.delenv("FAKE_CODEX_ACCESS_PROGRAMS", raising=False)
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        spec = _spec(cfg, extra={"daybreak": True})
        client = await backend.create_client(spec)
        try:
            await client.set_daybreak(False)
            await client.set_daybreak(True)
            with pytest.raises(TurnRefusedError, match=f"model '{SOL}' has no Daybreak"):
                await client.start_turn(TurnInput(text="hello"))
        finally:
            await client.disconnect()

        # The second ``model/list`` is the look before the refusal.
        assert _sent(tmp_path) == [
            "initialize", "initialized", "account/read", "model/list",
            "thread/start", "model/list",
        ]
        assert _sent(tmp_path, "thread/start") == [backend.thread_params(spec)]

    async def test_a_resumed_thread_gets_the_choice_after_the_resume(
        self, tmp_path, monkeypatch,
    ):
        """``thread/resume`` has no Daybreak field. The choice goes to the
        thread in a ``thread/metadata/update`` after it."""
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: BLUE})
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        spec = _spec(cfg, resume_native_id="th_old", extra={"daybreak": True})
        client = await backend.create_client(spec)
        await client.disconnect()

        assert _sent(tmp_path, "thread/resume") == [
            {**backend.thread_params(spec), "threadId": "th_old"},
        ]
        assert _sent(tmp_path)[-2:] == ["thread/resume", "thread/metadata/update"]
        assert _sent(tmp_path, "thread/metadata/update") == [
            {"threadId": "th_old", "daybreakEnabled": True},
        ]

    @pytest.mark.parametrize("fork", [False, True], ids=["resume", "fork"])
    @pytest.mark.parametrize("saved", [..., None, False], ids=["unset", "null", "false"])
    async def test_a_chat_that_never_used_daybreak_resumes_as_before_the_switch(
        self, tmp_path, monkeypatch, fork, saved,
    ):
        """The switch is off and the thread has not saved ``true``: that is
        every chat that never used Daybreak, also on an account that has it.
        Each request of its resume, with its parameters, is the one of a
        Nerve without the switch."""
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: BLUE})
        _saved_on_the_thread(monkeypatch, saved)
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        spec = _spec(cfg, resume_native_id="th_old", fork=fork)
        client = await backend.create_client(spec)
        await client.disconnect()

        assert _requests(tmp_path) == [
            *_BEFORE_THE_THREAD,
            ("thread/read", {"threadId": "th_old", "includeTurns": False}),
            ("thread/fork" if fork else "thread/resume",
             {**backend.thread_params(spec), "threadId": "th_old"}),
        ]

    @pytest.mark.parametrize("fork, method, thread_id", [
        ({}, "thread/resume", "th_old"),
        ({"fork": True, "fork_last_turn_id": "turn_9"}, "thread/fork", "fork:turn_9"),
    ], ids=["resume", "fork"])
    async def test_a_thread_that_still_says_on_is_told_that_the_switch_is_off(
        self, tmp_path, monkeypatch, fork, method, thread_id,
    ):
        """The switch went off while the chat had no runtime, or the thread
        is a fork of one that had it: the thread still says ``true``, and
        Codex's helper agents read that. One request after the resume sets
        it right."""
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: BLUE})
        _saved_on_the_thread(monkeypatch, True)
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        spec = _spec(cfg, resume_native_id="th_old", **fork)
        client = await backend.create_client(spec)
        try:
            assert client.native_session_id == thread_id
            await client.start_turn(TurnInput(text="hello"))
            assert (await _collect_turn(client))[-1].status == "completed"
        finally:
            await client.disconnect()

        assert _sent(tmp_path)[4:] == [
            "thread/read", method, "thread/metadata/update", "turn/start",
        ]
        assert _sent(tmp_path, "thread/metadata/update") == [
            {"threadId": thread_id, "daybreakEnabled": False},
        ]
        assert "cyberAccessProgram" not in _sent(tmp_path, "turn/start")[0]

    async def test_a_thread_that_already_says_on_gets_no_second_save(
        self, tmp_path, monkeypatch,
    ):
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: BLUE})
        _saved_on_the_thread(monkeypatch, True)
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        client = await backend.create_client(
            _spec(cfg, resume_native_id="th_old", extra={"daybreak": True}),
        )
        await client.disconnect()

        assert _sent(tmp_path)[4:] == ["thread/read", "thread/resume"]

    async def test_a_stale_choice_stays_while_the_catalog_has_no_access_metadata(
        self, tmp_path, monkeypatch,
    ):
        """A catalog without access-program metadata is a Codex that may not
        know the field: nothing of it is sent, in either direction."""
        _mode(monkeypatch, "basic")
        monkeypatch.delenv("FAKE_CODEX_ACCESS_PROGRAMS", raising=False)
        _saved_on_the_thread(monkeypatch, True)
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        client = await backend.create_client(_spec(cfg, resume_native_id="th_old"))
        await client.disconnect()

        assert _sent(tmp_path)[4:] == ["thread/read", "thread/resume"]

    @pytest.mark.parametrize("mode", ["resume_fail", "resume_gone"])
    @pytest.mark.parametrize("switch", [True, False], ids=["on", "off"])
    async def test_a_chat_whose_thread_is_gone_starts_a_new_one_with_the_choice(
        self, tmp_path, monkeypatch, mode, switch,
    ):
        """The stored thread is gone, which the read says, or only the
        resume. The new thread is started like any new thread: with the
        choice when the switch is on, and as before the switch when it is
        off. Nothing is saved after it."""
        _mode(monkeypatch, mode)
        _access_programs(monkeypatch, {SOL: BLUE})
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        spec = _spec(
            cfg, resume_native_id="th_old",
            extra={"daybreak": True} if switch else {},
        )
        client = await backend.create_client(spec)
        try:
            assert client.resume_dropped is True
        finally:
            await client.disconnect()

        started = backend.thread_params(spec)
        assert _sent(tmp_path, "thread/start") == [
            {**started, "daybreakEnabled": True} if switch else started,
        ]
        assert _sent(tmp_path, "thread/metadata/update") == []

    async def test_a_runtime_that_died_fails_no_toggle(self, tmp_path, monkeypatch, caplog):
        """The engine gives a client the stored switch before it looks
        whether the runtime still lives. The save on the thread cannot be
        sent then, and that fails nothing: the engine replaces the runtime
        next."""
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: BLUE})
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        client = await backend.create_client(_spec(cfg))
        try:
            await client._transport.close()
            with caplog.at_level(logging.WARNING, logger="nerve.agent.backends.codex.backend"):
                await client.set_daybreak(True)
        finally:
            await client.disconnect()

        assert "did not save the Daybreak choice" in caplog.text
        assert _sent(tmp_path, "thread/metadata/update") == []

    async def test_a_failed_save_fails_neither_the_toggle_nor_the_turn(
        self, tmp_path, monkeypatch, caplog,
    ):
        _mode(monkeypatch, "metadata_update_fail")
        _access_programs(monkeypatch, {SOL: BLUE})
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        client = await backend.create_client(_spec(cfg))
        try:
            with caplog.at_level(logging.WARNING, logger="nerve.agent.backends.codex.backend"):
                await client.set_daybreak(True)
            assert "did not save the Daybreak choice" in caplog.text
            await client.start_turn(TurnInput(text="hello"))
            assert (await _collect_turn(client))[-1].status == "completed"
        finally:
            await client.disconnect()

        assert _sent(tmp_path, "thread/metadata/update") == [
            {"threadId": "th_fake_1", "daybreakEnabled": True},
        ]
        assert _sent(tmp_path, "turn/start") == [
            _turn_params("hello", cyberAccessProgram="daybreakBlue"),
        ]

    @pytest.mark.parametrize("switch, saved", [(True, ...), (False, True)], ids=["on", "off"])
    async def test_a_failed_save_does_not_fail_a_resume(
        self, tmp_path, monkeypatch, caplog, switch, saved,
    ):
        _mode(monkeypatch, "metadata_update_fail")
        _access_programs(monkeypatch, {SOL: BLUE})
        _saved_on_the_thread(monkeypatch, saved)
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        with caplog.at_level(logging.WARNING, logger="nerve.agent.backends.codex.backend"):
            client = await backend.create_client(_spec(
                cfg, resume_native_id="th_old",
                extra={"daybreak": True} if switch else {},
            ))
        try:
            assert client.native_session_id == "th_old"
        finally:
            await client.disconnect()

        assert "did not save the Daybreak choice" in caplog.text
        assert _sent(tmp_path, "thread/metadata/update") == [
            {"threadId": "th_old", "daybreakEnabled": switch},
        ]


# --------------------------------------------------------------------------- #
#  The hint on a cyber-policy refusal                                          #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
class TestRefusalHint:
    REFUSAL = "This request was refused under the cyber policy."

    async def _failed_turn(self, tmp_path, monkeypatch, *, mode, programs, on) -> ev.TurnCompleted:
        _mode(monkeypatch, mode)
        _access_programs(monkeypatch, programs)
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        client = await backend.create_client(
            _spec(cfg, extra={"daybreak": True} if on else {}),
        )
        try:
            await client.start_turn(TurnInput(text="hello"))
            done = (await _collect_turn(client))[-1]
        finally:
            await client.disconnect()
        assert isinstance(done, ev.TurnCompleted)
        assert done.status == "failed"
        return done

    async def test_a_refusal_says_that_daybreak_can_be_turned_on(self, tmp_path, monkeypatch):
        done = await self._failed_turn(
            tmp_path, monkeypatch, mode="cyber_refusal", programs={SOL: BLUE}, on=False,
        )
        assert done.error == f"{self.REFUSAL} {HINT}"

    async def test_no_hint_while_the_switch_is_on(self, tmp_path, monkeypatch):
        done = await self._failed_turn(
            tmp_path, monkeypatch, mode="cyber_refusal", programs={SOL: BLUE}, on=True,
        )
        assert done.error == self.REFUSAL

    async def test_no_hint_for_a_model_without_a_program(self, tmp_path, monkeypatch):
        done = await self._failed_turn(
            tmp_path, monkeypatch, mode="cyber_refusal", programs={SOL: STANDARD}, on=False,
        )
        assert done.error == self.REFUSAL

    async def test_no_hint_for_another_error(self, tmp_path, monkeypatch):
        done = await self._failed_turn(
            tmp_path, monkeypatch, mode="failed_turn", programs={SOL: BLUE}, on=False,
        )
        assert done.error == "model exploded"

    async def test_the_error_notification_alone_carries_the_refusal(self, tmp_path):
        """A ``turn/completed`` without its own error takes the message, and
        the reason, of the ``error`` notification before it."""
        cfg = _config(tmp_path)
        backend = CodexBackend(_deps(cfg))
        backend.note_access_programs(backend.catalog([
            _entry(SOL, availableAccessPrograms=BLUE),
        ]))
        from nerve.agent.backends.codex.backend import CodexClient

        client = CodexClient(backend, _spec(cfg))
        await client._map_notification("error", {
            "error": {"message": self.REFUSAL, "codexErrorInfo": "cyberPolicy"},
            "willRetry": False,
        })
        done = client._map_turn_completed({"turn": {"status": "failed"}})
        assert done.error == f"{self.REFUSAL} {HINT}"


# --------------------------------------------------------------------------- #
#  The engine: metadata, a new client, the live client                         #
# --------------------------------------------------------------------------- #


def _engine(tmp_path, db) -> AgentEngine:
    cfg = NerveConfig.from_dict({
        "workspace": str(tmp_path / "ws"),
        "codex": {
            "home_dir": str(tmp_path / "codex-home"),
            "model": "gpt-6-sol",
            "models": ["gpt-6-astra", "gpt-6-luna"],
        },
    })
    return AgentEngine(cfg, db)


class _LiveClient:
    """A live Codex client, as far as the engine knows it. Like the real
    one, it does nothing for a switch that it has already."""

    model = "gpt-6-sol"

    def __init__(self, daybreak: bool = False):
        self.daybreak = daybreak
        # The changes of the switch that the client was given, in order.
        self.switches: list[bool] = []

    def is_alive(self) -> bool:
        return True

    async def disconnect(self) -> None:
        pass

    async def set_daybreak(self, enabled: bool) -> None:
        if enabled != self.daybreak:
            self.daybreak = enabled
            self.switches.append(enabled)


class _RecordingBackend:
    """A Codex backend that keeps the spec of each client it builds."""

    name = "codex"
    capabilities = BackendCapabilities(
        cost_is_cumulative=False, supports_idle_stream=False,
        supports_cache_ttl=False, interactive_builtins=False,
        reports_context_window=True,
    )

    def __init__(self):
        self.specs = []
        self.clients: list[_LiveClient] = []
        # Set both to hold a build: ``building`` is set when the build
        # starts, and the build waits for ``release``.
        self.building: asyncio.Event | None = None
        self.release: asyncio.Event | None = None

    def default_model(self, source):
        return "gpt-6-sol"

    def excluded_tools(self):
        return set()

    def validate_resume_target(self, native_id, cwd):
        return True

    async def create_client(self, spec):
        self.specs.append(spec)
        if self.building is not None and self.release is not None:
            self.building.set()
            await self.release.wait()
        self.clients.append(_LiveClient(spec.extra.get("daybreak") is True))
        return self.clients[-1]


async def _metadata(db, session_id: str) -> dict:
    return json.loads((await db.get_session(session_id)).get("metadata") or "{}")


@pytest.mark.asyncio
class TestEngine:
    async def test_a_new_client_reads_the_switch_from_the_metadata(self, tmp_path, db):
        engine = _engine(tmp_path, db)
        backend = engine._backends["codex"] = _RecordingBackend()
        await db.create_session("s-day", source="web", backend="codex", actor=None)

        await engine._get_or_create_client("s-day", "web", None)
        assert backend.specs[0].extra == {}        # off: as before the switch

        await engine.set_session_daybreak("s-day", True)
        assert daybreak.enabled(await db.get_session("s-day"))
        engine.sessions.remove_client("s-day")
        await engine._get_or_create_client("s-day", "web", None)
        assert backend.specs[1].extra == {"daybreak": True}

        await engine.set_session_daybreak("s-day", False)
        engine.sessions.remove_client("s-day")
        await engine._get_or_create_client("s-day", "web", None)
        assert backend.specs[2].extra == {}

    async def test_the_switch_keeps_the_other_metadata(self, tmp_path, db):
        engine = _engine(tmp_path, db)
        backend = engine._backends["codex"] = _RecordingBackend()
        await db.create_session(
            "s-box", source="web", backend="codex", actor=None,
            metadata={"codex_sandbox": "workspace-write"},
        )
        await engine.set_session_daybreak("s-box", True)
        assert await _metadata(db, "s-box") == {
            "codex_sandbox": "workspace-write", "codex_daybreak": True,
        }
        await engine._get_or_create_client("s-box", "web", None)
        assert backend.specs[0].extra == {"sandbox": "workspace-write", "daybreak": True}

        # Off is the absence of the key.
        await engine.set_session_daybreak("s-box", False)
        assert "codex_daybreak" not in await _metadata(db, "s-box")

    async def test_the_live_client_gets_the_change_without_a_new_runtime(self, tmp_path, db):
        engine = _engine(tmp_path, db)
        backend = engine._backends["codex"] = _RecordingBackend()
        await db.create_session("s-live", source="web", backend="codex", actor=None)
        client = await engine._get_or_create_client("s-live", "web", None)

        await engine.set_session_daybreak("s-live", True)
        await engine.set_session_daybreak("s-live", False)
        assert client.switches == [True, False]
        assert engine.sessions.get_client("s-live") is client
        assert len(backend.specs) == 1

    async def test_a_live_client_takes_the_stored_switch_at_its_next_turn(self, tmp_path, db):
        """The row is the truth: a client that missed a change gets it when
        the next turn asks for the client."""
        engine = _engine(tmp_path, db)
        engine._backends["codex"] = _RecordingBackend()
        await db.create_session("s-sync", source="web", backend="codex", actor=None)
        client = await engine._get_or_create_client("s-sync", "web", None)

        meta = await _metadata(db, "s-sync")
        await db.update_session_metadata("s-sync", {**meta, "codex_daybreak": True})
        assert await engine._get_or_create_client("s-sync", "web", None) is client
        assert client.switches == [True]

    async def test_a_change_during_a_first_build_reaches_the_new_client(self, tmp_path, db):
        """The first build holds the session's lock: the change waits for it,
        and the client that the build made gets the switch."""
        engine = _engine(tmp_path, db)
        backend = engine._backends["codex"] = _RecordingBackend()
        backend.building, backend.release = asyncio.Event(), asyncio.Event()
        await db.create_session("s-first", source="web", backend="codex", actor=None)

        build = asyncio.create_task(engine._get_or_create_client("s-first", "web", None))
        await backend.building.wait()
        change = asyncio.create_task(engine.set_session_daybreak("s-first", True))
        await asyncio.sleep(0.05)
        assert not change.done()
        backend.release.set()
        client = await build
        await change

        assert backend.specs[0].extra == {}
        assert client.switches == [True]
        assert (await _metadata(db, "s-first"))["codex_daybreak"] is True

    async def test_a_rebuild_keeps_a_switch_that_changed_while_it_ran(self, tmp_path, db):
        """A rebuild lets go of the session's lock, so the switch can change
        while it runs. The rebuild writes its own metadata into the row as it
        is then: the switch stays, and the new client is built with it."""
        engine = _engine(tmp_path, db)
        backend = engine._backends["codex"] = _RecordingBackend()
        await db.create_session("s-race", source="web", backend="codex", actor=None)
        await engine._get_or_create_client("s-race", "web", None)

        # The rebuild (a model switch) waits in its memory recall, after it
        # read the row and before it writes the recall into the metadata.
        recalling, release = asyncio.Event(), asyncio.Event()

        async def recall(query, limit):
            recalling.set()
            await release.wait()
            return [{"summary": "an earlier note"}]

        engine._memory_bridge = MagicMock(available=True, recall=recall)
        rebuild = asyncio.create_task(
            engine._get_or_create_client("s-race", "web", "gpt-6-astra"),
        )
        await recalling.wait()
        await engine.set_session_daybreak("s-race", True)
        release.set()
        await rebuild

        meta = await _metadata(db, "s-race")
        assert meta["codex_daybreak"] is True
        assert meta["recalled_memories"] == ["an earlier note"]
        assert backend.specs[-1].extra == {"daybreak": True}

    async def test_a_change_during_a_rebuild_reaches_the_new_client(self, tmp_path, db):
        """A rebuild lets go of the session's lock, so a change does not
        wait for it, and finds no client to tell. The client that the
        rebuild makes was built without the change. It takes the stored
        switch as soon as it is registered: before the turn that the
        rebuild is for."""
        engine = _engine(tmp_path, db)
        backend = engine._backends["codex"] = _RecordingBackend()
        await db.create_session("s-re", source="web", backend="codex", actor=None)
        await engine._get_or_create_client("s-re", "web", None)

        backend.building, backend.release = asyncio.Event(), asyncio.Event()
        rebuild = asyncio.create_task(
            engine._get_or_create_client("s-re", "web", "gpt-6-astra"),
        )
        await backend.building.wait()
        await engine.set_session_daybreak("s-re", True)
        backend.release.set()
        client = await rebuild

        assert len(backend.specs) == 2
        assert backend.specs[-1].extra == {}          # built before the change
        assert client.switches == [True]
        assert (await _metadata(db, "s-re"))["codex_daybreak"] is True

    async def test_a_client_without_the_switch_is_left_alone(self, tmp_path, db):
        """Another backend's client has no switch to set."""
        engine = _engine(tmp_path, db)
        await db.create_session("s-other", source="web", backend="claude", actor=None)
        client = SimpleNamespace()
        engine.sessions.set_client("s-other", client)
        await engine.set_session_daybreak("s-other", True)
        assert vars(client) == {}


@pytest.mark.asyncio
class TestEngineWithCodex:
    """The engine and the real Codex client, against the fake app-server."""

    def _engine(self, tmp_path, db) -> AgentEngine:
        return AgentEngine(_config(tmp_path), db)

    async def test_the_switch_reaches_the_live_client_for_its_next_turn(
        self, tmp_path, db, monkeypatch,
    ):
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: BLUE})
        engine = self._engine(tmp_path, db)
        await db.create_session("s-codex", source="web", backend="codex", actor=None)
        client = await engine._get_or_create_client("s-codex", "web", None)
        try:
            await client.start_turn(TurnInput(text="one"))
            await _collect_turn(client)
            # Off, through the engine: the requests of a Nerve without the
            # switch, and nothing else.
            assert _sent(tmp_path) == [
                "initialize", "initialized", "account/read", "model/list",
                "thread/start", "turn/start",
            ]
            await engine.set_session_daybreak("s-codex", True)
            assert await engine._get_or_create_client("s-codex", "web", None) is client
            await client.start_turn(TurnInput(text="two"))
            await _collect_turn(client)
        finally:
            await client.disconnect()

        # The engine's default effort for a web session is xhigh.
        assert _sent(tmp_path, "turn/start") == [
            _turn_params("one", effort="xhigh"),
            _turn_params("two", effort="xhigh", cyberAccessProgram="daybreakBlue"),
        ]
        assert "daybreakEnabled" not in _sent(tmp_path, "thread/start")[0]
        assert _sent(tmp_path).count("initialize") == 1

    async def test_a_client_built_for_a_chat_with_the_switch_on_sends_the_program(
        self, tmp_path, db, monkeypatch,
    ):
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: BLUE})
        engine = self._engine(tmp_path, db)
        await db.create_session(
            "s-codex", source="web", backend="codex", actor=None,
            metadata={"codex_daybreak": True},
        )
        client = await engine._get_or_create_client("s-codex", "web", None)
        try:
            await client.start_turn(TurnInput(text="hello"))
            await _collect_turn(client)
        finally:
            await client.disconnect()

        assert _sent(tmp_path, "thread/start")[0]["daybreakEnabled"] is True
        assert _sent(tmp_path, "turn/start") == [
            _turn_params("hello", effort="xhigh", cyberAccessProgram="daybreakBlue"),
        ]

    async def test_a_refused_turn_fails_and_keeps_the_runtime(
        self, tmp_path, db, monkeypatch, caplog,
    ):
        """The switch is on and the model has no program: the engine's turn
        fails with the reason and nothing is sent. It is no crash: the
        runtime is not replaced, and the refused turn uses up nothing. With
        the switch off, the next turn runs and carries the session context
        that the first message of a conversation gets."""
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: STANDARD})
        await ensure_system_principal(db)
        engine = self._engine(tmp_path, db)
        await db.create_session(
            "s-codex", source="web", backend="codex", actor=None,
            metadata={"codex_daybreak": True},
        )
        try:
            with caplog.at_level(logging.ERROR, logger="nerve.agent.engine"):
                refused = await engine._run_turn(
                    "s-codex", "hello", source="web", channel="web", actor=None,
                )
            client = engine.sessions.get_client("s-codex")
            assert _sent(tmp_path, "turn/start") == []
            assert client is not None and client.is_alive()

            await engine.set_session_daybreak("s-codex", False)
            sent = await engine._run_turn(
                "s-codex", "hello", source="web", channel="web", actor=None,
            )
            assert engine.sessions.get_client("s-codex") is client
        finally:
            live = engine.sessions.remove_client("s-codex")
            if live is not None:
                await live.disconnect()

        reason = (
            f"Daybreak is on for this chat, but model '{SOL}' has no Daybreak "
            "program for the signed-in Codex account. Turn Daybreak off or "
            "choose another model."
        )
        assert (refused.outcome.status, refused.outcome.error) == ("failed", reason)
        assert refused.text == f"⚠️ Turn failed: {reason}"
        assert sent.outcome.ok
        (turn,) = _sent(tmp_path, "turn/start")
        assert "cyberAccessProgram" not in turn
        assert turn["input"][0]["text"].startswith("<session-context>")
        assert _sent(tmp_path).count("initialize") == 1
        assert (await db.get_session("s-codex"))["status"] != "error"
        assert [r.getMessage() for r in caplog.records] == []

    async def test_a_turn_is_refused_with_the_reason_when_the_catalog_gives_no_answer(
        self, tmp_path, db, monkeypatch, caplog,
    ):
        """The look at the catalog before the refusal gets no answer in
        time. The person still reads why the turn was not sent: the turn
        fails with the Daybreak reason, it is no crash of the agent, and
        the runtime stays."""
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: STANDARD})
        await ensure_system_principal(db)
        engine = self._engine(tmp_path, db)
        await db.create_session(
            "s-codex", source="web", backend="codex", actor=None,
            metadata={"codex_daybreak": True},
        )
        client = await engine._get_or_create_client("s-codex", "web", None)
        request = client._transport.request

        async def no_catalog(method, params=None, timeout=None):
            if method == "model/list":
                raise TransportDiedError(
                    "codex app-server did not answer model/list within 10s",
                )
            return await request(method, params, timeout)

        monkeypatch.setattr(client._transport, "request", no_catalog)
        try:
            with caplog.at_level(logging.ERROR, logger="nerve.agent.engine"):
                refused = await engine._run_turn(
                    "s-codex", "hello", source="web", channel="web", actor=None,
                )
            assert engine.sessions.get_client("s-codex") is client
        finally:
            live = engine.sessions.remove_client("s-codex")
            if live is not None:
                await live.disconnect()

        assert (refused.outcome.status, refused.outcome.error) == ("failed", (
            f"Daybreak is on for this chat, but model '{SOL}' has no Daybreak "
            "program for the signed-in Codex account. Turn Daybreak off or "
            "choose another model."
        ))
        assert _sent(tmp_path, "turn/start") == []
        assert (await db.get_session("s-codex"))["status"] != "error"
        assert [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR] == []

    async def test_a_switch_change_at_the_end_of_a_turn_is_kept(
        self, tmp_path, db, monkeypatch,
    ):
        """The end of a turn stores its usage in the same metadata as the
        switch. A change of the switch that lands after the turn read the
        row, and before it writes, is still there afterwards. So is the
        usage of the turn."""
        _mode(monkeypatch, "basic")
        _access_programs(monkeypatch, {SOL: BLUE})
        await ensure_system_principal(db)
        engine = self._engine(tmp_path, db)
        await db.create_session("s-codex", source="web", backend="codex", actor=None)
        get_session = db.get_session
        landed = []

        async def get_session_then_switch(session_id):
            row = await get_session(session_id)
            # The first read of the turn's end: the switch changes right
            # after it, so the turn holds a row from before the change.
            if not landed and sys._getframe(1).f_code.co_name == "_persist_turn":
                landed.append(session_id)
                await engine.set_session_daybreak(session_id, True)
            return row

        monkeypatch.setattr(db, "get_session", get_session_then_switch)
        try:
            turn = await engine._run_turn(
                "s-codex", "hello", source="web", channel="web", actor=None,
            )
        finally:
            live = engine.sessions.remove_client("s-codex")
            if live is not None:
                await live.disconnect()

        assert turn.outcome.ok
        assert landed == ["s-codex"]
        metadata = await _metadata(db, "s-codex")
        assert metadata["codex_daybreak"] is True
        assert metadata["last_usage"]["num_turns"] == 1


# --------------------------------------------------------------------------- #
#  The session API                                                             #
# --------------------------------------------------------------------------- #

PROGRAMS = {"gpt-6-sol": "daybreakBlue", "gpt-6-luna": "daybreakRed"}


@pytest_asyncio.fixture
async def api(tmp_path, db, monkeypatch):
    """The session routes on a real engine. The Codex account has Daybreak
    on ``gpt-6-sol`` and ``gpt-6-luna``, and not on ``gpt-6-astra``."""
    import nerve.config as cfg_mod
    from nerve.gateway.routes import sessions as routes

    engine = _engine(tmp_path, db)
    preflight = AsyncMock(return_value={
        "available": True,
        "models": ["gpt-6-astra", "gpt-6-luna", "gpt-6-sol"],
        "daybreak_programs": dict(PROGRAMS),
    })
    engine._backends["codex"].preflight = preflight
    monkeypatch.setattr(cfg_mod, "_config", engine.config)
    monkeypatch.setattr(routes, "get_deps", lambda: SimpleNamespace(engine=engine, db=db))
    await ensure_actor_row(db, TEST_ACTOR)

    async def create(**fields) -> dict:
        return await routes.create_session(
            routes.SessionCreateRequest(**fields), actor=TEST_ACTOR,
        )

    async def patch(session_id: str, **fields) -> dict:
        return await routes.update_session(session_id, fields, actor=TEST_ACTOR)

    return SimpleNamespace(
        engine=engine, db=db, routes=routes, preflight=preflight,
        create=create, patch=patch,
    )


@pytest.mark.asyncio
class TestSessionApi:
    async def test_turn_it_on_and_off(self, api):
        session = await api.create(backend="codex", model="gpt-6-sol")
        assert session["codex_daybreak"] is False
        client = _LiveClient()
        api.engine.sessions.set_client(session["id"], client)

        on = await api.patch(session["id"], codex_daybreak=True)
        assert on["codex_daybreak"] is True
        assert "codex_daybreak_notice" not in on
        assert (await _metadata(api.db, session["id"]))["codex_daybreak"] is True
        # The live client has it for its next turn.
        assert client.switches == [True]

        off = await api.patch(session["id"], codex_daybreak=False)
        assert off["codex_daybreak"] is False
        assert "codex_daybreak" not in await _metadata(api.db, session["id"])
        assert client.switches == [True, False]

    async def test_a_model_without_a_program_refuses_it(self, api):
        session = await api.create(backend="codex", model="gpt-6-astra")
        with pytest.raises(HTTPException) as refused:
            await api.patch(session["id"], codex_daybreak=True)
        assert refused.value.status_code == 400
        assert refused.value.detail == (
            "Daybreak cannot be turned on: model 'gpt-6-astra' has no Daybreak "
            "program for the signed-in Codex account"
        )
        assert "codex_daybreak" not in await _metadata(api.db, session["id"])

    @pytest.mark.parametrize("value", [True, False])
    async def test_a_session_of_another_backend_refuses_the_field(self, api, value):
        session = await api.create(backend="claude")
        with pytest.raises(HTTPException) as refused:
            await api.patch(session["id"], codex_daybreak=value)
        assert refused.value.status_code == 400
        assert refused.value.detail == "Daybreak is only available for Codex sessions"
        assert "codex_daybreak" not in await _metadata(api.db, session["id"])

    async def test_a_session_of_another_backend_has_no_such_field(self, api):
        """Only a Codex session has a switch to report. No route adds the
        key to the payload of a session of another backend."""
        session = await api.create(backend="claude")
        await api.db.update_session_fields(session["id"], {"sdk_session_id": "native-1"})
        fetched = await api.routes.get_session(session["id"], actor=TEST_ACTOR)
        renamed = await api.patch(session["id"], title="Renamed")
        moved = await api.patch(session["id"], model="claude-opus-5")
        fork = await api.routes.fork_session(
            api.routes.ForkRequest(source_session_id=session["id"]), actor=TEST_ACTOR,
        )
        resumed = await api.routes.resume_session(session["id"], actor=TEST_ACTOR)
        listed = (await api.routes.list_sessions(offset=0, actor=TEST_ACTOR))["sessions"]
        found = (await api.routes.search_sessions(q="Renamed", actor=TEST_ACTOR))["sessions"]

        assert {s["id"] for s in listed} == {session["id"], fork["id"]}
        assert {s["id"] for s in found} == {session["id"], fork["id"]}
        for payload in (session, fetched, renamed, moved, fork, resumed, *listed, *found):
            assert [key for key in payload if "daybreak" in key] == []

    async def test_every_payload_of_a_codex_session_has_the_field(self, api):
        """Absent reads as off, so a route that left the key out would show
        a chat that has Daybreak on as off."""
        session = await api.create(backend="codex", model="gpt-6-sol", codex_daybreak=True)
        await api.db.update_session_fields(session["id"], {"sdk_session_id": "th_1"})
        fetched = await api.routes.get_session(session["id"], actor=TEST_ACTOR)
        renamed = await api.patch(session["id"], title="Renamed")
        resumed = await api.routes.resume_session(session["id"], actor=TEST_ACTOR)
        listed = (await api.routes.list_sessions(offset=0, actor=TEST_ACTOR))["sessions"]
        found = (await api.routes.search_sessions(q="Renamed", actor=TEST_ACTOR))["sessions"]

        assert [s["id"] for s in listed] == [s["id"] for s in found] == [session["id"]]
        for payload in (session, fetched, renamed, resumed, *listed, *found):
            assert payload["codex_daybreak"] is True

    async def test_its_payloads_are_the_ones_from_before_the_switch(self, api, monkeypatch):
        """The routes add the switch to a payload in one place. Without that
        place, as in a Nerve from before the switch, each payload of a
        session of another backend is the same, key by key."""
        session = await api.create(backend="claude")

        async def payloads() -> list:
            return [
                await api.patch(session["id"], title="Renamed"),
                await api.routes.get_session(session["id"], actor=TEST_ACTOR),
                await api.routes.list_sessions(offset=0, actor=TEST_ACTOR),
            ]

        await payloads()                     # the row is renamed from here on
        now = await payloads()
        monkeypatch.setattr(api.routes, "_stamp_daybreak", lambda payload: payload)
        assert now == await payloads()
        assert now[1]["backend"] == "claude"

    async def test_a_fork_starts_with_the_switch_off(self, api):
        """The switch is the chat's own: a fork of a chat that has it on is
        a Codex chat with the switch off."""
        session = await api.create(backend="codex", model="gpt-6-sol", codex_daybreak=True)
        await api.db.update_session_fields(session["id"], {"sdk_session_id": "th_1"})
        fork = await api.routes.fork_session(
            api.routes.ForkRequest(source_session_id=session["id"]), actor=TEST_ACTOR,
        )
        assert (fork["backend"], fork["codex_daybreak"]) == ("codex", False)
        assert "codex_daybreak" not in await _metadata(api.db, fork["id"])

    async def test_only_a_boolean_sets_it(self, api):
        session = await api.create(backend="codex", model="gpt-6-sol")
        with pytest.raises(HTTPException) as refused:
            await api.patch(session["id"], codex_daybreak="yes")
        assert refused.value.status_code == 400
        assert "codex_daybreak" not in await _metadata(api.db, session["id"])

    async def test_a_model_change_to_a_model_without_a_program_turns_it_off(self, api):
        session = await api.create(backend="codex", model="gpt-6-sol", codex_daybreak=True)
        client = _LiveClient(daybreak=True)
        api.engine.sessions.set_client(session["id"], client)

        moved = await api.patch(session["id"], model="gpt-6-astra")
        assert moved["model"] == "gpt-6-astra"
        assert moved["codex_daybreak"] is False
        assert moved["codex_daybreak_notice"] == (
            "Daybreak was turned off: model 'gpt-6-astra' has no Daybreak "
            "program for the signed-in Codex account."
        )
        assert "codex_daybreak" not in await _metadata(api.db, session["id"])
        assert client.switches == [False]

    async def test_a_model_change_to_a_model_with_a_program_keeps_it(self, api):
        session = await api.create(backend="codex", model="gpt-6-sol", codex_daybreak=True)
        moved = await api.patch(session["id"], model="gpt-6-luna")
        assert moved["model"] == "gpt-6-luna"
        assert moved["codex_daybreak"] is True
        assert "codex_daybreak_notice" not in moved

    async def test_an_unreadable_catalog_neither_turns_it_on_nor_off(self, api):
        """While the Codex catalog cannot be read, nothing says which models
        have a program: on is refused, and a model change keeps the switch
        (the turn itself is refused later if the model has no program)."""
        plain = await api.create(backend="codex", model="gpt-6-sol")
        on = await api.create(backend="codex", model="gpt-6-sol", codex_daybreak=True)
        api.preflight.return_value = {"available": False, "reason": "codex not found"}

        with pytest.raises(HTTPException) as refused:
            await api.patch(plain["id"], codex_daybreak=True)
        assert refused.value.status_code == 503
        assert "codex_daybreak" not in await _metadata(api.db, plain["id"])

        moved = await api.patch(on["id"], model="gpt-6-astra")
        assert moved["codex_daybreak"] is True
        assert "codex_daybreak_notice" not in moved

    async def test_turning_it_on_is_checked_against_the_model_of_the_same_request(self, api):
        session = await api.create(backend="codex", model="gpt-6-astra")
        both = await api.patch(session["id"], model="gpt-6-sol", codex_daybreak=True)
        assert (both["model"], both["codex_daybreak"]) == ("gpt-6-sol", True)

        with pytest.raises(HTTPException) as refused:
            await api.patch(session["id"], model="gpt-6-astra", codex_daybreak=True)
        assert refused.value.status_code == 400
        row = await api.db.get_session(session["id"])
        assert row["model"] == "gpt-6-sol"       # the refused request changed nothing
        assert daybreak.enabled(row)

    async def test_a_new_session_is_created_with_it_on(self, api):
        session = await api.create(backend="codex", model="gpt-6-sol", codex_daybreak=True)
        assert session["codex_daybreak"] is True
        assert await _metadata(api.db, session["id"]) == {
            "backend_override": "codex", "codex_daybreak": True,
        }
        fetched = await api.routes.get_session(session["id"], actor=TEST_ACTOR)
        assert fetched["codex_daybreak"] is True
        listed = await api.routes.list_sessions(offset=0, actor=TEST_ACTOR)
        assert [
            s["codex_daybreak"] for s in listed["sessions"] if s["id"] == session["id"]
        ] == [True]

    @pytest.mark.parametrize("fields", [
        {"backend": "codex", "model": "gpt-6-astra", "codex_daybreak": True},
        {"backend": "claude", "codex_daybreak": True},
        {"backend": "claude", "codex_daybreak": False},
    ])
    async def test_a_new_session_that_cannot_have_it_is_not_created(self, api, fields):
        with pytest.raises(HTTPException) as refused:
            await api.create(**fields)
        assert refused.value.status_code == 400
        assert await api.db.list_sessions() == []

    async def test_a_session_without_the_switch_is_created_as_before(self, api):
        session = await api.create(backend="codex", model="gpt-6-sol")
        assert await _metadata(api.db, session["id"]) == {"backend_override": "codex"}
        explicit_off = await api.create(
            backend="codex", model="gpt-6-sol", codex_daybreak=False,
        )
        assert await _metadata(api.db, explicit_off["id"]) == {"backend_override": "codex"}

    async def test_another_update_does_not_read_the_catalog(self, api):
        session = await api.create(backend="codex", model="gpt-6-sol")
        before = api.preflight.await_count
        renamed = await api.patch(session["id"], title="Renamed")
        assert (renamed["title"], renamed["codex_daybreak"]) == ("Renamed", False)
        # A model change with the switch off does not ask either.
        await api.patch(session["id"], model="gpt-6-astra")
        assert api.preflight.await_count == before
