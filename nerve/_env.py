"""Process-wide environment defaults — import before numpy loads.

This module is imported for its side effects at the top of Nerve's entry
points (``nerve.cli``) and before the first ``import numpy`` in the
codebase (``nerve.memory.memu_bridge``).  BLAS thread-pool sizing is read
once when the BLAS shared library is loaded, so these defaults are only
effective if they are in the environment *before* numpy is imported
anywhere in the process.

Why cap BLAS at a single thread
===============================

OpenBLAS (bundled with numpy wheels) spawns a worker thread pool sized to
the machine's core count on first parallel operation.  On many-core hosts
this interacts catastrophically with subprocess spawning:

1. **fork vs. BLAS atfork collision.**  glibc ``fork()`` runs registered
   ``pthread_atfork`` prepare handlers.  OpenBLAS registers one
   (``blas_thread_shutdown_``) that ``pthread_join``\\ s its *entire*
   worker pool before allowing the fork to proceed.  Under uvloop
   (``gateway.loop: uvloop``) the event loop spawns agent CLI subprocesses
   via ``fork()`` (libuv), while the
   dedicated memU thread keeps the BLAS pool busy with vector-index
   mat-vecs over a multi-hundred-MB embedding matrix.  Under a recall
   storm the pool never quiesces, so a spawn on the loop thread can block
   for minutes inside ``fork()`` — freezing the entire server (HTTP,
   WebSocket, all sessions).  Observed in production on a many-core
   deployment; diagnosed via native stack: ``uv__spawn_and_init_child_fork
   → __libc_fork → __run_prefork_handlers → blas_thread_shutdown_ →
   pthread_join``.

2. **Multi-threaded BLAS buys us nothing.**  Nerve's only BLAS workload
   is one mat-vec per memory recall/dedup — memory-bandwidth-bound, ~tens
   of ms single-threaded even on large indexes.

With ``OPENBLAS_NUM_THREADS=1`` the pool is never created, the atfork
handler returns immediately, and forks never stall.

The default loop (``gateway.loop: asyncio``) spawns with ``vfork()``, which
skips the atfork handlers — and the page-table copy — altogether.  The cap
stays as defense in depth: for the uvloop opt-in, and for any other
``fork()`` user in the process.

``os.environ.setdefault`` is used throughout: an explicitly configured
environment always wins over these defaults.
"""

from __future__ import annotations

import os


def apply_env_defaults() -> None:
    """Apply process-env defaults.  Idempotent; explicit env wins."""
    # Cap the BLAS worker pool (see module docstring).  Must be set
    # before numpy/OpenBLAS loads.
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")

    # The Claude Agent SDK spawns an extra ``claude -v`` subprocess on
    # every connect just to warn about outdated CLIs.  With many
    # concurrent sessions this doubles fork pressure on the event loop
    # for a warning nothing acts on.  The SDK honors this variable to
    # skip the check; export it as an empty string to re-enable.
    os.environ.setdefault("CLAUDE_AGENT_SDK_SKIP_VERSION_CHECK", "1")


# Variables that describe one CLI session or process, not the machine. A
# server started from a session's shell (``nerve restart`` in a Bash tool)
# inherits them, and every CLI it spawns would then see that session's
# identity, MCP token and messaging socket. Configuration such as
# CLAUDE_CODE_OAUTH_TOKEN or CLAUDE_CODE_USE_BEDROCK is not listed.
_SESSION_ENV_NAMES = frozenset({
    # Claude Code, for the commands one CLI runs.
    "CLAUDECODE",
    "CLAUDE_PID",
    "CLAUDE_EFFORT",
    "CLAUDE_AGENT_SDK_VERSION",
    "CLAUDE_CODE_SESSION_ID",
    "CLAUDE_CODE_SESSION_ATTENDED",
    "CLAUDE_CODE_CHILD_SESSION",
    "CLAUDE_CODE_ENTRYPOINT",
    "CLAUDE_CODE_EXECPATH",
    # Codex, for the commands one thread runs.
    "CODEX_THREAD_ID",
    "CODEX_CI",
    # Set by the Codex backend for one process when Ultracode is on. Left
    # over with Ultracode off, CODEX_CLI_PATH still points at the wrapper.
    "CODEX_CLI_PATH",
    "ULTRACODE_NO_AUTO_UPDATE",
    "ULTRACODE_UI",
    "ULTRACODE_TRANSPORT",
    "ULTRACODE_MAX_CONCURRENCY",
    "ULTRACODE_DEFAULT_TOKEN_BUDGET",
    "ULTRACODE_MAX_AGENTS",
    "NERVE_SESSION_ID",
})
_SESSION_ENV_PREFIXES = (
    "CLAUDE_CODE_MESSAGING_",
    "NERVE_MCP_",
    "NERVE_CODEX_",
)


def is_session_env_name(name: str) -> bool:
    """Whether ``name`` belongs to a single CLI session (see above)."""
    return name in _SESSION_ENV_NAMES or name.startswith(_SESSION_ENV_PREFIXES)


def strip_inherited_session_env() -> list[str]:
    """Remove the per-session variables from ``os.environ``.

    Called when the server starts, before it spawns any CLI. Returns the
    removed names, sorted, for the caller to log.
    """
    dropped = sorted(name for name in os.environ if is_session_env_name(name))
    for name in dropped:
        os.environ.pop(name, None)
    return dropped


apply_env_defaults()
