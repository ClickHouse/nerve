"""Narrow monkey-patch for anyio 4.13.0 ``_deliver_cancellation`` zombie-scope
hot-loop.

Upstream bug (anyio 4.13.0, ``_backends/_asyncio.py`` ~line 572-616)::

    for task in self._tasks:
        should_retry = True                 # ← set unconditionally
        if task._must_cancel:
            continue
        if task is not current and (task is self._host_task or _task_started(task)):
            waiter = task._fut_waiter
            if not isinstance(waiter, asyncio.Future) or not waiter.done():
                task.cancel(origin._cancel_reason)
                ...
    ...
    if origin is self and should_retry:
        self._cancel_handle = get_running_loop().call_soon(
            self._deliver_cancellation, origin
        )

When a task lingers in ``CancelScope._tasks`` but cannot be cancelled
(``task.done()`` is True — finished task whose ``task_done`` cleanup hasn't
run yet) the upstream loop nevertheless sets ``should_retry=True`` and the
scope re-arms ``call_soon(_deliver_cancellation)`` on every event-loop tick.
Result: one CPU core pinned at ~100% with tens of thousands of
``epoll_pwait`` syscalls per second and no forward progress.

Observed live on 2026-04-24: three simultaneous zombie-scopes in one nerve
process, each holding a single ``done=True`` task, ~55k epoll_pwait/sec
combined (load 1.6, cpu-thermal 60°C). Diagnosed via ``py-spy dump`` and a
GC scan of ``CancelScope`` instances.

This patch is intentionally **as narrow as possible**: it adds a single
``if task.done(): continue`` skip at the top of the loop body and is
otherwise byte-for-byte identical to upstream anyio 4.13.0. Earlier wider
patches that also skipped ``current_task()`` and ``_must_cancel`` were
reverted (see ``ClickHouse/nerve#128``) because they broke legitimate
anyio cancellation semantics:

* **Deferred self-delivery**: ``with CancelScope() as s: s.cancel(); await
  sleep(5)``. ``s.cancel()`` calls ``_deliver_cancellation`` synchronously
  with ``current_task()`` pointing at the host task; anyio relies on the
  ``call_soon`` reschedule to redeliver on the *next* tick (when
  ``current_task()`` is ``None``) and actually cancel. Skipping the current
  task without setting ``should_retry`` strands the cancel — the sleep
  runs to completion.

* **Re-delivery after swallowed CancelledError**: anyio's contract is to
  keep redelivering until the scope exits. Skipping tasks with
  ``_must_cancel=True`` without ``should_retry=True`` strands tasks that
  catch the first ``CancelledError`` and loop.

The done-task skip avoids both problems: ``should_retry`` is set
unconditionally for every live (non-done) task, exactly like upstream, so
deferred self-delivery and re-delivery still work. Only zombie tasks are
elided.

Import this module once, before anyio is used — see ``nerve/__init__.py``.
"""

from __future__ import annotations

import asyncio
import logging
from asyncio import current_task
from asyncio import get_running_loop

from anyio._backends import _asyncio as _anyio_asyncio

logger = logging.getLogger(__name__)

_APPLIED = False


def _patched_deliver_cancellation(self, origin):  # type: ignore[no-untyped-def]
    """Drop-in replacement for ``CancelScope._deliver_cancellation``.

    Identical to anyio 4.13.0 except for one early ``continue`` on
    ``task.done()`` — the zombie-scope skip. Every other branch matches
    upstream so legitimate cancellation (deferred self-delivery,
    re-delivery on swallowed CancelledError, task-group cancel, timer
    cancel) keeps working unchanged.
    """
    should_retry = False
    current = current_task()
    for task in self._tasks:
        # ONLY deviation from upstream: a done task can never be
        # cancelled (``task.cancel()`` is a no-op), so letting upstream
        # set ``should_retry=True`` for it produces the zombie-scope
        # hot-loop. Skip it before ``should_retry`` is touched.
        if task.done():
            continue

        should_retry = True
        if task._must_cancel:  # type: ignore[attr-defined]
            continue

        # The task is eligible for cancellation if it has started.
        if task is not current and (
            task is self._host_task
            or _anyio_asyncio._task_started(task)
        ):
            waiter = task._fut_waiter  # type: ignore[attr-defined]
            if not isinstance(waiter, asyncio.Future) or not waiter.done():
                task.cancel(origin._cancel_reason)
                if (
                    task is origin._host_task
                    and origin._pending_uncancellations is not None
                ):
                    origin._pending_uncancellations += 1

    # Deliver cancellation to child scopes that aren't shielded or
    # running their own cancellation callbacks.
    for scope in self._child_scopes:
        if not scope._shield and not scope.cancel_called:
            should_retry = scope._deliver_cancellation(origin) or should_retry

    # Schedule another callback if there are still tasks left.
    if origin is self:
        if should_retry:
            self._cancel_handle = get_running_loop().call_soon(
                self._deliver_cancellation, origin
            )
        else:
            self._cancel_handle = None

    return should_retry


def apply() -> bool:
    """Install the patch. Safe to call multiple times.

    Returns True if the patch was applied in this call, False if it was
    already applied (or the target symbol is missing).
    """
    global _APPLIED
    if _APPLIED:
        return False

    cancel_scope_cls = getattr(_anyio_asyncio, "CancelScope", None)
    if cancel_scope_cls is None:
        logger.warning(
            "anyio patch: CancelScope not found in %s; patch NOT applied",
            _anyio_asyncio.__name__,
        )
        return False

    if not hasattr(cancel_scope_cls, "_deliver_cancellation"):
        logger.warning(
            "anyio patch: _deliver_cancellation not found on CancelScope; "
            "patch NOT applied (anyio API changed?)"
        )
        return False

    if not hasattr(_anyio_asyncio, "_task_started"):
        logger.warning(
            "anyio patch: _task_started helper not found; patch NOT applied"
        )
        return False

    cancel_scope_cls._deliver_cancellation = _patched_deliver_cancellation
    _APPLIED = True
    logger.info(
        "anyio patch applied: CancelScope._deliver_cancellation now skips "
        "done tasks (prevents zombie-scope CPU spin)"
    )
    return True


# Apply on import so anyone who imports this module gets the patch.
apply()
