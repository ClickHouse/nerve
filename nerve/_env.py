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
   worker pool before allowing the fork to proceed.  The event loop
   spawns agent CLI subprocesses via ``fork()`` (libuv/uvloop), while the
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


apply_env_defaults()
