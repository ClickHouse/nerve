"""Launch a Codex stdio MCP server without placing secrets in argv.

Codex can whitelist environment variable names, but it cannot rename them for
the child server. Nerve stores each configured value under a synthetic parent
environment name and this tiny launcher maps it to the server's expected name
immediately before ``exec``.
"""

from __future__ import annotations

import os
import sys

EXTERNAL_MCP_ENV_PREFIX = "NERVE_CODEX_MCP_EXTERNAL_"


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    mappings: list[tuple[str, str]] = []
    while args[:1] == ["--env"]:
        if len(args) < 3:
            raise RuntimeError("--env requires TARGET and SOURCE")
        mappings.append((args[1], args[2]))
        del args[:3]
    if not args or args[0] != "--" or len(args) < 2:
        raise RuntimeError("expected -- followed by an MCP server command")

    command = args[1:]
    env = dict(os.environ)
    for target, source in mappings:
        if source not in env:
            raise RuntimeError(f"missing MCP credential environment {source}")
        env[target] = env[source]
    for key in tuple(env):
        if key.startswith(EXTERNAL_MCP_ENV_PREFIX):
            env.pop(key, None)
    os.execvpe(command[0], command, env)
    return 127


if __name__ == "__main__":  # pragma: no cover - executable module
    raise SystemExit(main())
