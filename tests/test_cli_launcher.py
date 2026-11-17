"""A launcher for the agent CLI (``agent.cli_launcher``).

The CLI starts every command the agent runs, so whatever group, limit or
priority the CLI starts in is the one the agent's whole work runs in. With a
launcher set, Nerve runs that program where it would have run the CLI, with
the CLI's arguments, and names the CLI in ``NERVE_AGENT_CLI_PATH``; the
program ends by replacing itself with the CLI.

These tests pin what the SDK is asked to start (through its own command-line
builder), what the launcher is told, that a launcher which cannot be used
never keeps the agent from starting, and the contract itself with a real
launcher and a stand-in CLI.
"""

import logging
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport

from nerve.agent.backends import claude as claude_backend
from nerve.agent.backends import claude_transport
from nerve.agent.backends.base import SessionSpec
from nerve.agent.backends.claude import CLI_PATH_ENV, ClaudeBackend
from nerve.config import CLI_LAUNCHER_ENV, NerveConfig

FAKE_CLI = "/opt/example/claude"


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    """No launcher from the test runner's environment, and every refusal
    logged afresh."""
    monkeypatch.delenv(CLI_LAUNCHER_ENV, raising=False)
    claude_backend._refused_launchers.clear()
    yield
    claude_backend._refused_launchers.clear()


def _backend(tmp_path: Path, launcher: str | None = None) -> ClaudeBackend:
    data: dict = {"workspace": str(tmp_path)}
    if launcher is not None:
        data["agent"] = {"cli_launcher": launcher}
    cfg = NerveConfig.from_dict(data)
    return ClaudeBackend(SimpleNamespace(
        config=lambda: cfg,
        claude_plugins=lambda: [],
        managed_mcp=False,
    ))


def _build(backend: ClaudeBackend, tmp_path: Path):
    spec = SessionSpec(
        session_id="sess-1234",
        source="web",
        model="claude-opus-5",
        effort="high",
        system_prompt="prompt",
        cwd=str(tmp_path),
    )
    with patch.object(backend, "_build_mcp_servers", return_value={}), \
         patch.object(backend, "_build_hooks", return_value={}), \
         patch.object(claude_transport, "default_cli_path", return_value=FAKE_CLI):
        return backend._build_options(spec)


def _launcher(tmp_path: Path, mode: int = 0o755) -> str:
    path = tmp_path / "launch"
    path.write_text('#!/bin/sh\nexec "$NERVE_AGENT_CLI_PATH" "$@"\n')
    path.chmod(mode)
    return str(path)


# --------------------------------------------------------------------- #
#  What the SDK starts                                                   #
# --------------------------------------------------------------------- #


def test_no_launcher_by_default(tmp_path):
    options = _build(_backend(tmp_path), tmp_path)

    assert options.cli_path is None
    assert CLI_PATH_ENV not in options.env


def test_launcher_stands_where_the_cli_stood(tmp_path):
    """The SDK's own command line starts with the launcher, the CLI's
    arguments follow it unchanged, and the launcher finds the CLI in its
    environment."""
    launcher = _launcher(tmp_path)

    plain = _build(_backend(tmp_path), tmp_path)
    launched = _build(_backend(tmp_path, launcher), tmp_path)

    assert launched.cli_path == launcher
    assert launched.env[CLI_PATH_ENV] == FAKE_CLI

    plain.cli_path = FAKE_CLI  # what the SDK's search would have answered
    plain_cmd = SubprocessCLITransport(prompt="", options=plain)._build_command()
    launched_cmd = SubprocessCLITransport(prompt="", options=launched)._build_command()
    assert launched_cmd[0] == launcher
    assert launched_cmd[1:] == plain_cmd[1:]


def test_home_relative_launcher(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    launcher = _launcher(tmp_path)

    options = _build(_backend(tmp_path, "~/launch"), tmp_path)

    assert options.cli_path == launcher


# --------------------------------------------------------------------- #
#  A launcher that cannot be used                                        #
# --------------------------------------------------------------------- #


@pytest.mark.parametrize("case", ["relative", "missing", "directory", "not-executable"])
def test_unusable_launcher_is_not_used(tmp_path, case, caplog):
    """The agent starts without it, and the log says why, once."""
    if case == "relative":
        launcher = "bin/launch"
    elif case == "missing":
        launcher = str(tmp_path / "nothing-here")
    elif case == "directory":
        launcher = str(tmp_path)
    else:
        launcher = _launcher(tmp_path, mode=0o644)
    backend = _backend(tmp_path, launcher)

    with caplog.at_level(logging.WARNING, logger="nerve.agent.backends.claude"):
        first = _build(backend, tmp_path)
        second = _build(backend, tmp_path)

    for options in (first, second):
        assert options.cli_path is None
        assert CLI_PATH_ENV not in options.env
    refusals = [r for r in caplog.records if "agent.cli_launcher" in r.getMessage()]
    assert len(refusals) == 1
    assert launcher in refusals[0].getMessage()


def test_cli_search_failure_starts_without_the_launcher(tmp_path, caplog):
    """When Nerve cannot say which CLI the launcher is to start (the SDK finds
    none, or its search moved), the SDK is left to start the CLI itself and
    to report what it finds."""
    backend = _backend(tmp_path, _launcher(tmp_path))
    spec = SessionSpec(
        session_id="sess-1234", source="web", model="claude-opus-5",
        effort="high", system_prompt="prompt", cwd=str(tmp_path),
    )

    with patch.object(backend, "_build_mcp_servers", return_value={}), \
         patch.object(backend, "_build_hooks", return_value={}), \
         patch.object(claude_transport, "default_cli_path", side_effect=RuntimeError("no CLI")), \
         caplog.at_level(logging.WARNING, logger="nerve.agent.backends.claude"):
        options = backend._build_options(spec)

    assert options.cli_path is None
    assert CLI_PATH_ENV not in options.env
    assert "no CLI" in caplog.text


# --------------------------------------------------------------------- #
#  The setting                                                           #
# --------------------------------------------------------------------- #


def test_setting_and_environment(tmp_path, monkeypatch):
    def launcher(agent: dict | None) -> str:
        data: dict = {"workspace": str(tmp_path)}
        if agent is not None:
            data["agent"] = agent
        return NerveConfig.from_dict(data).agent.cli_launcher

    assert launcher(None) == ""
    assert launcher({"cli_launcher": "/usr/local/bin/launch"}) == "/usr/local/bin/launch"

    monkeypatch.setenv(CLI_LAUNCHER_ENV, "/etc/example/launch")
    assert launcher(None) == "/etc/example/launch"
    # A blank value is unset; a named one wins over the environment.
    assert launcher({"cli_launcher": "  "}) == "/etc/example/launch"
    assert launcher({"cli_launcher": "/usr/local/bin/launch"}) == "/usr/local/bin/launch"

    monkeypatch.setenv(CLI_LAUNCHER_ENV, "  ")
    assert launcher(None) == ""


# --------------------------------------------------------------------- #
#  The SDK seam and the contract                                         #
# --------------------------------------------------------------------- #


def test_default_cli_path_is_the_sdk_search(tmp_path):
    """``default_cli_path`` answers what the SDK's own search answers, once."""
    claude_transport.default_cli_path.cache_clear()
    try:
        with patch.object(
            SubprocessCLITransport, "_find_cli", return_value=FAKE_CLI,
        ) as find:
            assert claude_transport.default_cli_path() == FAKE_CLI
            assert claude_transport.default_cli_path() == FAKE_CLI
        assert find.call_count == 1
    finally:
        claude_transport.default_cli_path.cache_clear()


@pytest.mark.skipif(os.name == "nt", reason="exec replaces a process on POSIX only")
def test_launcher_becomes_the_cli(tmp_path):
    """The contract, with a real launcher: the CLI runs with the arguments the
    launcher was given, in the process the caller started, so the caller's
    pipes and signals reach the CLI itself."""
    cli = tmp_path / "cli"
    cli.write_text(
        f"#!{sys.executable}\n"
        "import os, sys\n"
        "print(os.getpid(), *sys.argv[1:])\n"
    )
    cli.chmod(0o755)

    process = subprocess.Popen(
        [_launcher(tmp_path), "--output-format", "stream-json"],
        env={**os.environ, CLI_PATH_ENV: str(cli)},
        stdout=subprocess.PIPE,
        text=True,
    )
    out, _ = process.communicate(timeout=30)

    assert process.returncode == 0
    assert out.split() == [str(process.pid), "--output-format", "stream-json"]
