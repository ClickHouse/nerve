"""Executable shim used by Ultracode's child ``codex`` processes."""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from uuid import uuid4


def _worker_grant(worker_id: str) -> dict:
    """Exchange the parent session token for the broker's answer: the worker
    token and, when the parent chat has Daybreak on, a ``daybreak`` entry."""
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
    if payload.get("token") is None:
        raise RuntimeError("Nerve MCP worker token response contained no token")
    return payload


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


# ``codex exec`` and its alias.
_EXEC = ("exec", "e")


def _worker_model(argv: list[str]) -> str | None:
    """The model that a worker command names: ``-m X``, ``-mX``, ``-m=X``,
    ``--model X`` or ``--model=X``."""
    for index, arg in enumerate(argv):
        if arg in ("-m", "--model"):
            if index + 1 < len(argv):
                return argv[index + 1]
        elif arg.startswith("--model="):
            return arg.partition("=")[2]
        elif arg.startswith("-m") and not arg.startswith("--"):
            return arg[2:].removeprefix("=")
    return None


def _not_started(reason: str, advice: str) -> None:
    """Say why a worker of a chat that has Daybreak on is not started."""
    print(
        f"nerve: Daybreak is on for this chat, but {reason}. The worker was "
        f"not started. {advice}",
        file=sys.stderr,
    )


def _follow_daybreak(argv: list[str], daybreak: dict) -> list[str] | None:
    """Make an ``exec`` worker follow the Daybreak switch of its parent chat.

    ``daybreak`` is the broker's entry for a chat that has the switch on: the
    chat's ``model`` and the ``programs`` of the models that have one, as
    ``--cyber-access-program`` spells them. The flag goes after ``exec`` (or
    its alias ``e``), or after ``resume`` for ``exec resume``. A worker that
    names no model gets the chat's.

    ``None``: the worker must not start, because it would run without what
    the chat asked for. Its model has no program; or the ``exec`` of this
    Codex has no such flag (``workers_need_codex`` in the entry); or
    ``exec`` comes after other options, where this cannot place the flag; or
    it is a worker of the ``app-server`` transport, which takes its turns
    over its own connection, where this cannot add the program. (Ultracode
    runs a worker whose ``app-server`` did not start on ``exec`` instead,
    unless the run asked for that transport strictly.)

    A command that runs no worker (neither ``exec`` nor ``app-server``)
    stays as it is.
    """
    if not argv:
        return argv
    if argv[0] not in _EXEC:
        if any(arg in _EXEC for arg in argv):
            _not_started(
                "Nerve cannot add its program to a worker command that has "
                "options before `exec`",
                "Turn Daybreak off to run such a worker.",
            )
            return None
        if "app-server" in argv:
            _not_started(
                "Nerve cannot give its program to a worker of the "
                "`app-server` transport",
                "Run the worker on the `exec` transport, or turn Daybreak off.",
            )
            return None
        return argv
    needs = daybreak.get("workers_need_codex")
    if needs:
        _not_started(
            "this Codex version cannot pass it to workers: that needs Codex "
            f"{needs} or newer",
            "Turn Daybreak off or update Codex.",
        )
        return None
    programs = daybreak.get("programs")
    extra: list[str] = []
    model = _worker_model(argv)
    if model is None:
        model = str(daybreak.get("model") or "")
        extra = ["-m", model]
    program = programs.get(model) if isinstance(programs, dict) else None
    if not program:
        _not_started(
            f"model {model!r} has no Daybreak program for the signed-in "
            "Codex account",
            "Turn Daybreak off or choose another model.",
        )
        return None
    at = 2 if argv[1:2] == ["resume"] else 1
    return [*argv[:at], "--cyber-access-program", str(program), *extra, *argv[at:]]


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
    grant = _worker_grant(worker_id)
    env["NERVE_MCP_TOKEN"] = str(grant["token"])
    env["NERVE_ULTRACODE_WORKER_ID"] = worker_id
    # Prevent nested plugin refreshes even if upstream defaults change.
    env["ULTRACODE_NO_AUTO_UPDATE"] = "1"
    worker_argv = sys.argv[1:]
    # The entry is in the answer only for a parent chat that has Daybreak on.
    daybreak = grant.get("daybreak")
    if daybreak is not None:
        worker_argv = _follow_daybreak(
            worker_argv, daybreak if isinstance(daybreak, dict) else {},
        )
        if worker_argv is None:
            return 1
    # Workers inherit the parent's external MCP servers, so the synthetic
    # credential variables stay in the environment — the stdio MCP launcher
    # maps and strips them at server spawn (never in argv).
    argv = [real_bin, *_inject_config_overrides(worker_argv, overrides)]
    os.execvpe(real_bin, argv, env)
    return 127


if __name__ == "__main__":  # pragma: no cover - executable module
    raise SystemExit(main())
