"""Executable shim used by Ultracode's child ``codex`` processes."""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from uuid import uuid4

from nerve.agent.backends.codex.mcp_stdio_wrapper import EXTERNAL_MCP_ENV_PREFIX


def _worker_token(worker_id: str) -> str:
    url = os.environ.get("NERVE_MCP_WORKER_TOKEN_URL", "")
    parent = os.environ.get("NERVE_MCP_TOKEN", "")
    if not url:
        raise RuntimeError("Nerve worker token broker is not configured")
    body = json.dumps({
        "worker_id": worker_id,
        "parent_session_id": os.environ.get("NERVE_MCP_PARENT_SESSION_ID", ""),
    }).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if parent:
        headers["Authorization"] = f"Bearer {parent}"
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, ValueError, urllib.error.HTTPError) as e:
        raise RuntimeError(f"Could not mint Nerve MCP worker token: {e}") from e
    token = payload.get("token")
    if token is None:
        raise RuntimeError("Nerve MCP worker token response contained no token")
    return str(token)


def _inject_config_overrides(argv: list[str], overrides: list[str]) -> list[str]:
    """Place ``--config`` flags where the selected Codex command reads them.

    ``codex exec`` and ``codex app-server`` each own a command-local config
    parser.  When either command also receives one of its own ``-c`` flags,
    root-level overrides placed before the subcommand are silently ignored by
    Codex 0.144.x.  Ultracode always adds an exec-local approval override, so
    Nerve's MCP configuration must be inserted immediately after the command.
    """
    config_args = [part for value in overrides for part in ("--config", value)]
    if argv and argv[0] in {"exec", "app-server"}:
        return [argv[0], *config_args, *argv[1:]]
    return [*config_args, *argv]


def main() -> int:
    real_bin = os.environ.get("NERVE_CODEX_REAL_BIN") or "codex"
    try:
        overrides = json.loads(os.environ.get("NERVE_CODEX_CHILD_CONFIG", "[]"))
    except ValueError as e:
        raise RuntimeError("NERVE_CODEX_CHILD_CONFIG is invalid JSON") from e
    if not isinstance(overrides, list) or not all(isinstance(v, str) for v in overrides):
        raise RuntimeError("NERVE_CODEX_CHILD_CONFIG must be a JSON string list")

    worker_id = f"ultracode-{uuid4().hex[:16]}"
    env = dict(os.environ)
    env["NERVE_MCP_TOKEN"] = _worker_token(worker_id)
    env["NERVE_ULTRACODE_WORKER_ID"] = worker_id
    # Prevent nested plugin refreshes even if upstream defaults change.
    env["ULTRACODE_NO_AUTO_UPDATE"] = "1"
    # The parent app-server needs synthetic external-MCP credential variables,
    # but Ultracode workers have no external MCP configuration and must not
    # inherit the corresponding secrets.
    for key in tuple(env):
        if key.startswith(EXTERNAL_MCP_ENV_PREFIX):
            env.pop(key, None)
    argv = [real_bin, *_inject_config_overrides(sys.argv[1:], overrides)]
    os.execvpe(real_bin, argv, env)
    return 127


if __name__ == "__main__":  # pragma: no cover - executable module
    raise SystemExit(main())
