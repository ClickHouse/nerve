"""A server started from a session's shell does not pass that session's
identity to the CLIs it spawns.

``nerve restart`` run in a session's Bash tool starts the server with that
shell's environment: the session's NERVE_SESSION_ID, MCP token, CLI messaging
socket and so on. The server removes them at startup, and each CLI gets its
own NERVE_SESSION_ID.
"""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

import nerve._env as env_mod
from nerve.agent.backends.claude import ClaudeBackend
from nerve.agent.backends.codex import CodexBackend
from nerve.config import NerveConfig
from tests.test_codex_appserver import _config, _deps, _spec

# What a CLI session's shell holds (values are synthetic).
SESSION_VARS = {
    "CLAUDECODE": "1",
    "CLAUDE_PID": "4242",
    "CLAUDE_EFFORT": "high",
    "CLAUDE_AGENT_SDK_VERSION": "0.0.1",
    "CLAUDE_CODE_SESSION_ID": "00000000-0000-0000-0000-000000000001",
    "CLAUDE_CODE_SESSION_ATTENDED": "0",
    "CLAUDE_CODE_CHILD_SESSION": "1",
    "CLAUDE_CODE_ENTRYPOINT": "sdk-py",
    "CLAUDE_CODE_EXECPATH": "/opt/old/claude",
    "CLAUDE_CODE_MESSAGING_SOCKET": "/run/old.sock",
    "CLAUDE_CODE_MESSAGING_TOKEN": "old-messaging-token",
    "CODEX_THREAD_ID": "old-thread",
    "CODEX_CI": "1",
    "CODEX_CLI_PATH": "/tmp/old/ultracode-wrapper",
    "ULTRACODE_MAX_AGENTS": "99",
    "NERVE_SESSION_ID": "old-session",
    "NERVE_MCP_TOKEN": "old-mcp-token",
    "NERVE_MCP_PARENT_SESSION_ID": "old-parent",
    "NERVE_CODEX_REAL_BIN": "codex",
    "NERVE_CODEX_CHILD_CONFIG": "[]",
}

# Configuration a user may export for the server: kept.
CONFIG_VARS = {
    "CLAUDE_CODE_OAUTH_TOKEN": "sk-ant-oat01-REPLACE",
    "CLAUDE_CODE_USE_BEDROCK": "1",
    "CLAUDE_CODE_MAX_OUTPUT_TOKENS": "4096",
    "ANTHROPIC_BASE_URL": "http://192.0.2.1:3128",
    "NERVE_HOME": "/tmp/nerve-home",
}


@pytest.fixture
def session_shell(monkeypatch):
    # Start from the variables below only, whatever shell runs the tests.
    for name in list(os.environ):
        if env_mod.is_session_env_name(name):
            monkeypatch.delenv(name)
    for name, value in {**SESSION_VARS, **CONFIG_VARS}.items():
        monkeypatch.setenv(name, value)


class TestStrip:
    def test_removes_session_variables_and_reports_them(self, session_shell):
        dropped = env_mod.strip_inherited_session_env()

        assert dropped == sorted(SESSION_VARS)
        for name in SESSION_VARS:
            assert name not in os.environ

    def test_keeps_configuration(self, session_shell):
        env_mod.strip_inherited_session_env()

        for name, value in CONFIG_VARS.items():
            assert os.environ[name] == value

    def test_nothing_to_remove(self, monkeypatch):
        for name in list(os.environ):
            if env_mod.is_session_env_name(name):
                monkeypatch.delenv(name)

        assert env_mod.strip_inherited_session_env() == []


class TestSpawnedCliEnvironment:
    """The SDK starts the CLI with ``{**os.environ, **options.env}``."""

    def test_claude_cli_gets_its_own_session_id(self, session_shell):
        env_mod.strip_inherited_session_env()
        cfg = NerveConfig.from_dict({"anthropic_api_key": "sk-ant-test"})
        backend = ClaudeBackend(SimpleNamespace(config=lambda: cfg))

        cli_env = {**os.environ, **backend._build_env(session_id="new-session")}

        assert cli_env["NERVE_SESSION_ID"] == "new-session"
        for name, value in SESSION_VARS.items():
            if name != "NERVE_SESSION_ID":
                assert cli_env.get(name) != value, name

    def test_claude_without_a_session_id_sets_none(self):
        cfg = NerveConfig.from_dict({"anthropic_api_key": "sk-ant-test"})
        backend = ClaudeBackend(SimpleNamespace(config=lambda: cfg))

        assert "NERVE_SESSION_ID" not in backend._build_env()

    def test_codex_gets_its_own_session_id(self, session_shell, tmp_path):
        env_mod.strip_inherited_session_env()
        cfg = _config(tmp_path)

        env = CodexBackend(_deps(cfg)).build_env(_spec(cfg, session_id="new-session"))

        assert env["NERVE_SESSION_ID"] == "new-session"
        assert env["NERVE_MCP_TOKEN"] == "tok-new-session"
        for name, value in SESSION_VARS.items():
            if name not in ("NERVE_CODEX_REAL_BIN", "NERVE_CODEX_CHILD_CONFIG"):
                # Ultracode sets these two itself, with this session's values.
                assert env.get(name) != value, name
