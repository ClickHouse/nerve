"""Managed Ultracode install, worker inheritance, and accounting tests."""

from __future__ import annotations

import json
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from nerve.agent.backends import BackendDeps, SessionSpec
from nerve.agent.backends.codex import CodexBackend
from nerve.agent.backends.codex import mcp_stdio_wrapper
from nerve.agent.backends.codex import worker_wrapper
from nerve.agent.backends.codex.ultracode import (
    _CLI_UI_OLD,
    _ENGINE_AGENTS_OLD,
    _ENGINE_BUDGET_OLD,
    _ENGINE_CONCURRENCY_OLD,
    apply_policy_overlay,
    installation_status,
    list_dashboard_runs,
    materialize_worker_wrapper,
    read_dashboard_run,
)
from nerve.config import McpServerConfig, NerveConfig
from nerve.gateway.auth import (
    MCP_AUDIENCE,
    MCP_SESSION_CLAIM,
    MCP_WORKER_CLAIM,
    create_mcp_session_token,
    decode_token,
)


def _config(tmp_path: Path) -> NerveConfig:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    return NerveConfig.from_dict({
        "workspace": str(workspace),
        "codex": {
            "home_dir": str(tmp_path / "codex-home"),
            "ultracode": {"enabled": True, "dashboard": True},
        },
    })


def _backend(
    cfg: NerveConfig,
    external_mcp_servers: list[McpServerConfig] | None = None,
) -> CodexBackend:
    return CodexBackend(BackendDeps(
        config=lambda: cfg,
        db=None,
        registry=None,
        tool_ctx_factory=lambda sid: None,
        external_mcp_servers=lambda: list(external_mcp_servers or []),
        gateway_port=lambda: 49152,
        mint_session_token=lambda sid: f"parent-{sid}",
    ))


def _write_run(cfg: NerveConfig, record: dict) -> Path:
    path = (
        Path(cfg.codex.home_dir) / "ultracode" / "runs"
        / f"{record['id']}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record), encoding="utf-8")
    path.chmod(0o600)
    return path


def _spec(cfg: NerveConfig) -> SessionSpec:
    return SessionSpec(
        session_id="s1", source="web", model=cfg.codex.model,
        effort="high", system_prompt="test", cwd=str(cfg.workspace),
    )


def test_worker_wrapper_is_owner_only(tmp_path):
    wrapper = materialize_worker_wrapper(tmp_path)
    assert wrapper.is_file()
    assert stat.S_IMODE(wrapper.stat().st_mode) == 0o700
    assert "worker_wrapper import main" in wrapper.read_text()


def test_child_environment_inherits_full_mcp_without_persisting_token(tmp_path):
    external = McpServerConfig(
        name="external_http", type="http", url="https://mcp.invalid/",
        headers={"Authorization": "http-secret-value"},
    )
    cfg = _config(tmp_path)
    backend = _backend(cfg, [external])
    from nerve.agent.backends.codex.backend import CodexClient

    client = CodexClient(backend, _spec(cfg))
    env = client._transport._env
    child = json.loads(env["NERVE_CODEX_CHILD_CONFIG"])
    assert any(value.startswith("mcp_servers.nerve.url=") for value in child)
    # Workers inherit the parent session's full MCP surface: external
    # servers included, no worker-side tool allowlist, and every server
    # pre-approved (workers are non-interactive and cannot answer prompts).
    assert any(value.startswith("mcp_servers.external_http.") for value in child)
    assert 'mcp_servers.nerve.default_tools_approval_mode="approve"' in child
    assert (
        'mcp_servers.external_http.default_tools_approval_mode="approve"'
        in child
    )
    assert not any("enabled_tools" in value for value in child)
    # Secrets stay env-referenced — never inline in the child config.
    assert all("http-secret-value" not in value for value in child)
    assert all("parent-s1" not in value for value in child)
    assert env["ULTRACODE_NO_AUTO_UPDATE"] == "1"
    assert env["CODEX_CLI_PATH"].endswith("codex-worker")
    assert env["NERVE_MCP_WORKER_TOKEN_URL"].endswith("/api/codex/worker-token")
    assert env["ULTRACODE_MAX_CONCURRENCY"] == "2"
    assert env["ULTRACODE_DEFAULT_TOKEN_BUDGET"] == "250000"
    assert env["ULTRACODE_MAX_AGENTS"] == "8"


def test_worker_wrapper_exchanges_token_and_injects_config(monkeypatch):
    class Response:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def read(self):
            return b'{"token":"worker-token"}'

    captured = {}
    monkeypatch.setenv("NERVE_CODEX_REAL_BIN", "/usr/bin/codex-real")
    monkeypatch.setenv("NERVE_MCP_TOKEN", "parent-token")
    monkeypatch.setenv("NERVE_MCP_WORKER_TOKEN_URL", "http://127.0.0.1/token")
    monkeypatch.setenv("NERVE_CODEX_CHILD_CONFIG", '["mcp_servers.nerve.required=true"]')
    monkeypatch.setattr(worker_wrapper.urllib.request, "urlopen", lambda *a, **k: Response())
    monkeypatch.setattr(worker_wrapper.sys, "argv", ["codex-worker", "exec", "--json"])

    def fake_exec(file, argv, env):
        captured.update(file=file, argv=argv, env=env)
        raise RuntimeError("stop")

    monkeypatch.setattr(worker_wrapper.os, "execvpe", fake_exec)
    with pytest.raises(RuntimeError, match="stop"):
        worker_wrapper.main()
    assert captured["argv"][:4] == [
        "/usr/bin/codex-real", "exec", "--config",
        "mcp_servers.nerve.required=true",
    ]
    assert captured["env"]["NERVE_MCP_TOKEN"] == "worker-token"
    assert captured["env"]["NERVE_ULTRACODE_WORKER_ID"].startswith("ultracode-")
    assert captured["env"]["ULTRACODE_NO_AUTO_UPDATE"] == "1"


# -- Daybreak: workers follow the switch of their parent chat ------------- #

_REAL_BIN = "/usr/bin/codex-real"
_CONFIG = ["--config", "mcp_servers.nerve.required=true"]
# The command lines that Ultracode builds for a new worker and a warm one.
_EXEC = [
    "exec", "--json", "--ephemeral", "--skip-git-repo-check",
    "--sandbox", "read-only", "-c", 'approval_policy="never"',
    "--output-last-message", "/tmp/last.txt", "--cd", "/tmp/ws",
]
_RESUME = [
    "exec", "resume", "--json", "--skip-git-repo-check", "th_1",
    "-o", "/tmp/last.txt",
]
_GRANT = {
    "token": "worker-token", "worker_id": "ultracode-0123456789abcdef",
    "expires_in": 7200,
}
# The broker's entry for a chat on gpt-6-sol that has Daybreak on.
_DAYBREAK = {
    "model": "gpt-6-sol",
    "programs": {"gpt-6-sol": "daybreak_blue", "gpt-6-luna": "daybreak_red"},
}
_BLUE = ["--cyber-access-program", "daybreak_blue"]
# The same entry from a Codex whose ``exec`` has no flag for the program.
_NO_FLAG = {**_DAYBREAK, "workers_need_codex": "0.161.0"}
# Root-level options before the subcommand.
_OPTIONS_FIRST = ["-c", 'model_reasoning_effort="high"', *_EXEC, "-m", "gpt-6-sol", "-"]


def _as_before_the_switch(worker_argv):
    """The command line that a Nerve without the Daybreak switch starts:
    Nerve's MCP configuration right after ``exec`` or ``app-server``, and
    before any other command."""
    if worker_argv[0] in ("exec", "app-server"):
        return [_REAL_BIN, worker_argv[0], *_CONFIG, *worker_argv[1:]]
    return [_REAL_BIN, *_CONFIG, *worker_argv]


class _CodexStarted(Exception):
    pass


def _run_worker_wrapper(monkeypatch, worker_argv, grant):
    """Run the wrapper with a broker that answers ``grant``.

    Returns the exit code and the command line it starts Codex with. A
    started Codex has no exit code; a wrapper that returns started none.
    """
    class Response:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def read(self):
            return json.dumps(grant).encode()

    started = []
    monkeypatch.setenv("NERVE_CODEX_REAL_BIN", _REAL_BIN)
    monkeypatch.setenv("NERVE_MCP_TOKEN", "parent-token")
    monkeypatch.setenv("NERVE_MCP_WORKER_TOKEN_URL", "http://127.0.0.1/token")
    monkeypatch.setenv("NERVE_CODEX_CHILD_CONFIG", json.dumps(_CONFIG[1:]))
    monkeypatch.setattr(worker_wrapper.urllib.request, "urlopen", lambda *a, **k: Response())
    monkeypatch.setattr(worker_wrapper.sys, "argv", ["codex-worker", *worker_argv])

    def fake_exec(file, argv, env):
        started.append(argv)
        raise _CodexStarted

    monkeypatch.setattr(worker_wrapper.os, "execvpe", fake_exec)
    try:
        return worker_wrapper.main(), None
    except _CodexStarted:
        return None, started[0]


@pytest.mark.parametrize("worker_argv", [
    [*_EXEC, "-m", "gpt-6-sol", "-"],
    [*_EXEC, "-mgpt-6-sol", "-"],
    [*_EXEC, "-m=gpt-6-astra", "-"],
    [*_EXEC, "-"],
    [*_RESUME, "-m", "gpt-6-astra", "-"],
    ["e", *_EXEC[1:], "-m", "gpt-6-sol", "-"],
    _OPTIONS_FIRST,
    ["app-server"],
    ["--version"],
])
def test_worker_command_line_is_unchanged_without_daybreak(monkeypatch, worker_argv):
    """Off is the default: the broker's answer has no ``daybreak`` entry, and
    the worker starts with the command line it always had."""
    code, argv = _run_worker_wrapper(monkeypatch, worker_argv, _GRANT)
    assert code is None
    assert argv == _as_before_the_switch(worker_argv)
    if worker_argv[0] == "exec":
        assert argv == [_REAL_BIN, "exec", *_CONFIG, *worker_argv[1:]]


@pytest.mark.parametrize("model_args", [
    ["-m", "gpt-6-sol"], ["-mgpt-6-sol"], ["-m=gpt-6-sol"],
    ["--model", "gpt-6-sol"], ["--model=gpt-6-sol"],
])
def test_worker_gets_the_program_of_its_model(monkeypatch, model_args):
    """Every way to name the model: the program is the model's, and the
    wrapper adds no model of its own."""
    worker_argv = [*_EXEC, *model_args, "-"]
    code, argv = _run_worker_wrapper(
        monkeypatch, worker_argv, {**_GRANT, "daybreak": _DAYBREAK},
    )
    assert code is None
    assert argv == [_REAL_BIN, "exec", *_CONFIG, *_BLUE, *worker_argv[1:]]


@pytest.mark.parametrize("model_args", [
    ["-m", "gpt-6-luna"], ["-mgpt-6-luna"], ["-m=gpt-6-luna"],
])
def test_worker_program_is_the_one_of_the_workers_own_model(monkeypatch, model_args):
    worker_argv = [*_EXEC, *model_args, "-"]
    _, argv = _run_worker_wrapper(
        monkeypatch, worker_argv, {**_GRANT, "daybreak": _DAYBREAK},
    )
    assert argv == [
        _REAL_BIN, "exec", *_CONFIG, "--cyber-access-program", "daybreak_red",
        *worker_argv[1:],
    ]


def test_worker_without_a_model_runs_on_the_model_of_the_chat(monkeypatch):
    worker_argv = [*_EXEC, "-"]
    _, argv = _run_worker_wrapper(
        monkeypatch, worker_argv, {**_GRANT, "daybreak": _DAYBREAK},
    )
    assert argv == [
        _REAL_BIN, "exec", *_CONFIG, *_BLUE, "-m", "gpt-6-sol", *worker_argv[1:],
    ]


@pytest.mark.parametrize("model_args, added", [
    (["-m", "gpt-6-sol"], _BLUE),
    (["-mgpt-6-sol"], _BLUE),
    ([], [*_BLUE, "-m", "gpt-6-sol"]),
])
def test_warm_worker_gets_the_program_after_resume(monkeypatch, model_args, added):
    worker_argv = [*_RESUME, *model_args, "-"]
    _, argv = _run_worker_wrapper(
        monkeypatch, worker_argv, {**_GRANT, "daybreak": _DAYBREAK},
    )
    assert argv == [_REAL_BIN, "exec", *_CONFIG, "resume", *added, *worker_argv[2:]]


@pytest.mark.parametrize("worker_argv, added_at", [
    (["e", *_EXEC[1:], "-m", "gpt-6-sol", "-"], 1),
    (["e", *_RESUME[1:], "-m", "gpt-6-sol", "-"], 2),
])
def test_worker_started_with_the_alias_of_exec_gets_the_program(
    monkeypatch, worker_argv, added_at,
):
    """``codex e`` is ``codex exec``."""
    code, argv = _run_worker_wrapper(
        monkeypatch, worker_argv, {**_GRANT, "daybreak": _DAYBREAK},
    )
    assert code is None
    assert argv == [
        _REAL_BIN, *_CONFIG, *worker_argv[:added_at], *_BLUE, *worker_argv[added_at:],
    ]


@pytest.mark.parametrize("worker_argv, daybreak, model", [
    # The worker names a model that has no program.
    ([*_EXEC, "-m", "gpt-6-astra", "-"], _DAYBREAK, "gpt-6-astra"),
    ([*_EXEC, "-mgpt-6-astra", "-"], _DAYBREAK, "gpt-6-astra"),
    ([*_EXEC, "-m=gpt-6-astra", "-"], _DAYBREAK, "gpt-6-astra"),
    ([*_RESUME, "--model=gpt-6-astra", "-"], _DAYBREAK, "gpt-6-astra"),
    (["e", *_EXEC[1:], "-m", "gpt-6-astra", "-"], _DAYBREAK, "gpt-6-astra"),
    # The worker takes the chat's model, which has none (access was lost).
    ([*_EXEC, "-"], {"model": "gpt-6-sol", "programs": {}}, "gpt-6-sol"),
    # An entry that is not what the broker sends.
    ([*_EXEC, "-m", "gpt-6-sol", "-"], "on", "gpt-6-sol"),
])
def test_worker_whose_model_has_no_program_is_not_started(
    monkeypatch, capsys, worker_argv, daybreak, model,
):
    code, argv = _run_worker_wrapper(
        monkeypatch, worker_argv, {**_GRANT, "daybreak": daybreak},
    )
    assert code == 1
    assert argv is None                     # Codex was not started
    message = capsys.readouterr().err
    assert f"model '{model}' has no Daybreak program" in message
    assert "Turn Daybreak off or choose another model." in message


@pytest.mark.parametrize("worker_argv", [
    _OPTIONS_FIRST,
    ["--config", "features.plan=false", *_RESUME, "-"],
    ["-c", "features.plan=false", "e", *_EXEC[1:], "-m", "gpt-6-sol", "-"],
])
def test_worker_with_options_before_exec_is_not_started(monkeypatch, capsys, worker_argv):
    """Options before ``exec`` are a form that the wrapper cannot put the
    flag into. It does not guess, and it does not let the worker run
    without the program."""
    code, argv = _run_worker_wrapper(
        monkeypatch, worker_argv, {**_GRANT, "daybreak": _DAYBREAK},
    )
    assert code == 1
    assert argv is None                     # Codex was not started
    assert capsys.readouterr().err.strip() == (
        "nerve: Daybreak is on for this chat, but Nerve cannot add its program "
        "to a worker command that has options before `exec`. The worker was "
        "not started. Turn Daybreak off to run such a worker."
    )


@pytest.mark.parametrize("worker_argv", [
    [*_EXEC, "-m", "gpt-6-sol", "-"],
    [*_EXEC, "-"],
    [*_RESUME, "-m", "gpt-6-sol", "-"],
])
def test_worker_is_not_started_on_a_codex_whose_exec_has_no_flag(
    monkeypatch, capsys, worker_argv,
):
    """``codex exec`` takes ``--cyber-access-program`` from 0.161.0, and the
    broker marks an older Codex (and one of an unknown version). The model
    has a program here, and the worker is still not started: with the flag
    that Codex stops on its command line, and without it the worker runs
    without the program."""
    code, argv = _run_worker_wrapper(
        monkeypatch, worker_argv, {**_GRANT, "daybreak": _NO_FLAG},
    )
    assert code == 1
    assert argv is None                     # Codex was not started
    assert capsys.readouterr().err.strip() == (
        "nerve: Daybreak is on for this chat, but this Codex version cannot "
        "pass it to workers: that needs Codex 0.161.0 or newer. The worker "
        "was not started. Turn Daybreak off or update Codex."
    )


@pytest.mark.parametrize("daybreak", [_DAYBREAK, _NO_FLAG], ids=["flag", "no-flag"])
@pytest.mark.parametrize("worker_argv", [
    ["app-server"],
    ["-c", "features.plan=false", "app-server"],
])
def test_worker_of_the_app_server_transport_is_not_started(
    monkeypatch, capsys, worker_argv, daybreak,
):
    """A worker of the ``app-server`` transport takes its turns over its own
    connection: the wrapper cannot add the program there, and it does not let
    the worker run without it. (Ultracode then runs the worker on ``exec``,
    where the wrapper adds the program.)"""
    code, argv = _run_worker_wrapper(
        monkeypatch, worker_argv, {**_GRANT, "daybreak": daybreak},
    )
    assert code == 1
    assert argv is None                     # Codex was not started
    assert capsys.readouterr().err.strip() == (
        "nerve: Daybreak is on for this chat, but Nerve cannot give its program "
        "to a worker of the `app-server` transport. The worker was not started. "
        "Run the worker on the `exec` transport, or turn Daybreak off."
    )


@pytest.mark.parametrize("daybreak", [_DAYBREAK, _NO_FLAG], ids=["flag", "no-flag"])
@pytest.mark.parametrize("worker_argv", [
    ["--version"],
    ["login", "status"],
])
def test_command_that_runs_no_worker_is_left_alone(monkeypatch, worker_argv, daybreak):
    """A command with neither ``exec`` nor ``app-server`` runs no worker: it
    starts as it always did, also for a chat that has Daybreak on."""
    code, argv = _run_worker_wrapper(
        monkeypatch, worker_argv, {**_GRANT, "daybreak": daybreak},
    )
    assert code is None
    assert argv == _as_before_the_switch(worker_argv)


def test_external_mcp_secrets_are_env_referenced_and_inherited_by_worker(
    tmp_path, monkeypatch,
):
    stdio = McpServerConfig(
        name="private_stdio", command="server-bin", args=["serve"],
        env={"SERVICE_TOKEN": "stdio-secret-value"},
    )
    http = McpServerConfig(
        name="private_http", type="http", url="https://mcp.invalid/",
        headers={"Authorization": "http-secret-value"},
    )
    backend = _backend(_config(tmp_path), [stdio, http])
    spec = _spec(backend.config)
    overrides = backend.build_config_overrides(spec)
    rendered = "\n".join(overrides)
    assert "stdio-secret-value" not in rendered
    assert "http-secret-value" not in rendered
    assert "env_vars=" in rendered
    assert "env_http_headers" in rendered

    env = backend.build_env(spec)
    secret_names = [
        key for key in env
        if key.startswith("NERVE_CODEX_MCP_EXTERNAL_")
    ]
    assert len(secret_names) == 2
    assert {env[key] for key in secret_names} == {
        "stdio-secret-value", "http-secret-value",
    }

    class Response:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def read(self):
            return b'{"token":"worker-token"}'

    captured = {}
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("NERVE_CODEX_REAL_BIN", "/usr/bin/codex-real")
    monkeypatch.setenv("NERVE_MCP_WORKER_TOKEN_URL", "http://127.0.0.1/token")
    monkeypatch.setenv("NERVE_CODEX_CHILD_CONFIG", "[]")
    monkeypatch.setattr(
        worker_wrapper.urllib.request, "urlopen", lambda *a, **k: Response(),
    )
    monkeypatch.setattr(worker_wrapper.sys, "argv", ["codex-worker", "exec"])

    def fake_exec(file, argv, child_env):
        captured["env"] = child_env
        raise RuntimeError("stop")

    monkeypatch.setattr(worker_wrapper.os, "execvpe", fake_exec)
    with pytest.raises(RuntimeError, match="stop"):
        worker_wrapper.main()
    # Workers inherit the parent's external MCP servers, so the synthetic
    # credential variables must survive into the worker environment — the
    # stdio launcher maps + strips them later, at MCP server spawn.
    assert {captured["env"].get(name) for name in secret_names} == {
        "stdio-secret-value", "http-secret-value",
    }


def test_stdio_mcp_wrapper_maps_synthetic_secret_only_for_server(monkeypatch):
    source = "NERVE_CODEX_MCP_EXTERNAL_STDIO_ABC123"
    monkeypatch.setenv(source, "secret-value")
    captured = {}

    def fake_exec(file, argv, env):
        captured.update(file=file, argv=argv, env=env)
        raise RuntimeError("stop")

    monkeypatch.setattr(mcp_stdio_wrapper.os, "execvpe", fake_exec)
    with pytest.raises(RuntimeError, match="stop"):
        mcp_stdio_wrapper.main([
            "--env", "SERVICE_TOKEN", source,
            "--", "server-bin", "serve",
        ])
    assert captured["argv"] == ["server-bin", "serve"]
    assert captured["env"]["SERVICE_TOKEN"] == "secret-value"
    assert source not in captured["env"]


def test_worker_token_is_scoped_and_expires():
    secret = "x" * 32
    token = create_mcp_session_token(
        secret, "session-1", ttl_seconds=120, worker_id="ultracode-0123456789abcdef",
    )
    payload = decode_token(token, secret, audience=MCP_AUDIENCE)
    assert payload[MCP_SESSION_CLAIM] == "session-1"
    assert payload[MCP_WORKER_CLAIM] == "ultracode-0123456789abcdef"
    assert 0 < payload["exp"] - payload["iat"] <= 120


def test_installation_status_finds_pinned_cache(tmp_path):
    cfg = _config(tmp_path)
    plugin = (
        Path(cfg.codex.home_dir) / "plugins" / "cache" / "nerve-ultracode"
        / "ultracode" / cfg.codex.ultracode.version / ".codex-plugin"
    )
    plugin.mkdir(parents=True)
    (plugin / "plugin.json").write_text(json.dumps({
        "name": "ultracode", "version": cfg.codex.ultracode.version,
    }))
    git_dir = plugin.parent / ".git"
    git_dir.mkdir()
    (git_dir / "HEAD").write_text(cfg.codex.ultracode.revision)
    scripts = plugin.parent / "scripts"
    scripts.mkdir()
    (scripts / "ultracode-engine.js").write_text("\n".join([
        _ENGINE_CONCURRENCY_OLD,
        _ENGINE_BUDGET_OLD,
        _ENGINE_AGENTS_OLD,
        "  await fs.writeFile(tmpPath, `${JSON.stringify(value, null, 2)}\\n`, \"utf8\");",
    ]))
    (scripts / "ultracode-cli.js").write_text(_CLI_UI_OLD)
    (scripts / "ultracode-script-runner.js").write_text(
        "async function writeJson(filePath, value) {\n"
        "  await fs.mkdir(path.dirname(filePath), { recursive: true });\n"
        "  const tmpPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;\n"
        "  await fs.writeFile(tmpPath, `${JSON.stringify(value, null, 2)}\\n`, \"utf8\");\n"
        "  await fs.rename(tmpPath, filePath);\n"
        "}\n"
    )
    apply_policy_overlay(cfg, plugin.parent)
    status = installation_status(cfg)
    assert status["installed"] is True
    assert status["auto_update"] is False
    assert status["policy_overlay"] is True


def test_ultracode_usage_is_added_once(tmp_path):
    cfg = _config(tmp_path)
    backend = _backend(cfg)
    from nerve.agent.backends.codex.backend import CodexClient

    client = CodexClient(backend, _spec(cfg))
    record = {
        "id": "ultra-1",
        "status": "completed",
        "aggregate_usage": {
            "input_tokens": 100, "cached_input_tokens": 20,
            "output_tokens": 30, "reasoning_output_tokens": 10,
        },
        "workers": [],
    }
    _write_run(cfg, record)
    line = "progress: workers complete\nrun ultra-1 finished\n"
    client._capture_ultracode_accounting(line)
    client._capture_ultracode_accounting(line)
    assert client._ultracode_usage["input_tokens"] == 100
    assert client._ultracode_usage["output_tokens"] == 30


@pytest.mark.asyncio
async def test_dynamic_exec_content_items_are_visible_and_accounted(tmp_path):
    cfg = _config(tmp_path)
    backend = _backend(cfg)
    from nerve.agent.backends.codex.backend import CodexClient

    client = CodexClient(backend, _spec(cfg))
    _write_run(cfg, {
        "id": "ultra-dynamic-1",
        "status": "partial",
        "aggregate_usage": {
            "input_tokens": 120,
            "cached_input_tokens": 20,
            "output_tokens": 40,
            "reasoning_output_tokens": 5,
        },
        "workers": [],
    })
    events = await client._map_item_completed({
        "id": "tool-1",
        "type": "dynamicToolCall",
        "tool": "exec",
        "namespace": None,
        "status": "completed",
        "contentItems": [
            {"type": "inputText", "text": "first line"},
            {"type": "inputText", "text": "ultra-dynamic-1"},
        ],
    })
    assert events[0].content == "first line\nultra-dynamic-1"
    assert client._ultracode_usage["input_tokens"] == 120

    # A namespaced tool named exec is not Codex's code-mode primitive.
    other = CodexClient(backend, _spec(cfg))
    await other._map_item_completed({
        "id": "tool-2",
        "type": "dynamicToolCall",
        "tool": "exec",
        "namespace": "functions",
        "status": "completed",
        "contentItems": [{"type": "inputText", "text": "ultra-dynamic-1"}],
    })
    assert other._ultracode_usage is None


def test_malformed_or_nonterminal_journal_never_counts(tmp_path):
    cfg = _config(tmp_path)
    backend = _backend(cfg)
    from nerve.agent.backends.codex.backend import CodexClient

    client = CodexClient(backend, _spec(cfg))
    _write_run(cfg, {
        "id": "ultra-malformed",
        "status": "completed",
        "aggregate_usage": {
            "input_tokens": "100",
            "cached_input_tokens": 0,
            "output_tokens": -1,
            "reasoning_output_tokens": 0,
        },
    })
    _write_run(cfg, {
        "id": "ultra-running",
        "status": "running",
        "aggregate_usage": {
            "input_tokens": 10,
            "cached_input_tokens": 0,
            "output_tokens": 1,
            "reasoning_output_tokens": 0,
        },
    })
    client._capture_ultracode_accounting(
        "ultra-malformed ultra-running ultra-forged-without-a-journal",
    )
    assert client._ultracode_usage is None


def test_child_usage_survives_missing_parent_usage(tmp_path):
    cfg = _config(tmp_path)
    backend = _backend(cfg)
    from nerve.agent.backends.codex.backend import CodexClient

    client = CodexClient(backend, _spec(cfg))
    client._effective_auth = "chatgpt"
    _write_run(cfg, {
        "id": "ultra-child-only",
        "status": "completed",
        "aggregate_usage": {
            "input_tokens": 100,
            "cached_input_tokens": 25,
            "output_tokens": 30,
            "reasoning_output_tokens": 4,
        },
        "workers": [],
    })
    client._capture_ultracode_accounting("ultra-child-only")
    done = client._map_turn_completed({"turn": {"status": "completed"}})
    assert done.usage is not None
    assert done.usage.input_tokens == 75
    assert done.usage.cache_read_tokens == 25
    assert done.usage.output_tokens == 30


def test_dashboard_reader_is_bounded_read_only_and_redacts(tmp_path):
    cfg = _config(tmp_path)
    record = {
        "id": "ultra-dashboard-1",
        "name": "Dashboard test",
        "status": "completed",
        "task": "inspect integration",
        "workers": [{
            "id": "worker-1", "status": "completed",
            "result": {"api_token": "must-not-leak", "answer": "ok"},
        }],
        "events": [{"at": "2026-07-11T12:00:00Z", "type": "step.completed"}],
        "aggregate_usage": {"input_tokens": 10, "output_tokens": 2},
        "controller": {"command_line": "contains private invocation"},
        "state_path": "/private/state/path",
    }
    path = _write_run(cfg, record)
    detail = read_dashboard_run(cfg, record["id"])
    assert detail is not None
    assert "controller" not in detail
    assert "state_path" not in detail
    assert detail["workers"][0]["result"]["api_token"] == "[redacted]"
    summaries = list_dashboard_runs(cfg)
    assert summaries[0]["workers"] == 1

    assert read_dashboard_run(cfg, "../outside") is None
    path.chmod(0o644)
    assert read_dashboard_run(cfg, record["id"]) is None


def test_dashboard_routes_are_feature_gated(monkeypatch, tmp_path, bypass_auth):
    from nerve.gateway.routes import codex as codex_routes

    cfg = _config(tmp_path)
    _write_run(cfg, {
        "id": "ultra-route-1",
        "status": "completed",
        "workers": [],
        "events": [],
    })
    engine = SimpleNamespace(config=cfg, _backends={})
    monkeypatch.setattr(
        codex_routes, "get_deps",
        lambda: SimpleNamespace(engine=engine),
    )
    app = FastAPI()
    app.include_router(codex_routes.router)
    bypass_auth(app)  # these routes are about the feature gate, not about auth
    client = TestClient(app)

    status = client.get("/api/codex/ultracode/dashboard")
    assert status.status_code == 200
    assert status.json()["read_only"] is True
    assert client.get("/api/codex/ultracode/runs").json()["runs"][0]["id"] == "ultra-route-1"
    assert client.get(
        "/api/codex/ultracode/runs/ultra-route-1",
    ).json()["run"]["id"] == "ultra-route-1"

    cfg.codex.ultracode.dashboard = False
    assert client.get("/api/codex/ultracode/dashboard").status_code == 404
