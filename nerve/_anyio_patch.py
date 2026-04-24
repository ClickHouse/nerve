"""Monkey-patch for anyio 4.13.0 _deliver_cancellation hot-loop bug.

Upstream bug (anyio 4.13.0, _backends/_asyncio.py:582-616):

    for task in self._tasks:
        should_retry = True          # ← set unconditionally
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

``should_retry`` is set to True simply because there is *any* task in the
scope's ``_tasks`` set, regardless of whether cancellation could actually be
delivered. When every task in the scope is the *current* task (a scope that
contains only itself, or a TaskGroup whose only live member is running the
cancel) nothing gets ``task.cancel()``-ed, but the scheduler reschedules
``_deliver_cancellation`` via ``call_soon`` on every event-loop tick. Result:
one CPU core pinned at 100%, tens of thousands of ``epoll_pwait`` syscalls per
second, and no forward progress.

We have seen this trigger at least three times (April 22, 23, and 24, 2026).
The SDK-side mitigation in ``nerve.agent.engine._safe_disconnect`` only runs
during ``client.disconnect()``, so spins originating elsewhere (telegram
polling, cron, an active SDK request hitting a broken pipe) are not covered.

The fix below sets ``should_retry = True`` only when we *actually* called
``task.cancel()`` on a task that could still receive a cancellation. We
skip three categories of tasks that upstream blindly retries on:

1. **Done tasks** (``task.done() is True``). Root cause of the April 24
   regression: if a task completes while the scope has ``cancel_called``
   and the scope never pops it out of ``_tasks``, every subsequent pass
   sees ``_must_cancel=False`` (cleared when the task finished),
   ``_task_started()=True``, ``_fut_waiter=None``. Our "waiter not done"
   branch fires, ``task.cancel()`` is a no-op on a done task, and yet we
   flagged ``should_retry=True`` anyway. Result: infinite ``call_soon``
   on a scope whose only inhabitant is already a corpse. We observed
   three such zombie-scopes running simultaneously, all spinning at
   ~55k epoll_pwait/sec combined (100% CPU, load 1.6, 60°C).

2. **Tasks already flagged with ``_must_cancel``**. asyncio's
   ``Task.__step`` checks the flag when the task next resumes and will
   raise ``CancelledError`` without our help. Retrying in that case means
   we re-queue ourselves on every event-loop tick while the task is
   blocked (shielded, awaiting I/O). This was the second iteration of
   the bug (April 23 evening).

3. **The current task**, because a task cannot cancel itself from inside
   a callback. (``current_task()`` is often ``None`` here because the
   callback runs from ``call_soon`` outside any task context — the check
   is a belt-and-suspenders guard for the other case.)

Import this module once, before anyio is used (i.e. very early in the
process entry point — see ``nerve/__main__.py``).
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

    Semantics: ``should_retry`` is True **only when we actually called
    ``task.cancel()`` on some task in this pass**. A task already flagged
    with ``_must_cancel`` does not justify a retry — asyncio's
    ``Task.__step`` checks the flag when the task next resumes and will
    raise ``CancelledError`` without our help. Retrying in that case means
    we re-queue ourselves on every event-loop tick while the task is
    blocked (shielded, awaiting I/O, or — critically — is the *current
    task* running this very callback), which is the hot loop we're trying
    to kill.
    """
    should_retry = False
    current = current_task()
    for task in self._tasks:
        # Already finished. anyio doesn't always remove tasks from
        # ``_tasks`` before the scope's cancel callback fires (done
        # tasks can linger until the task group unwinds). ``task.cancel()``
        # is a no-op on a done task, so retrying is pure busy-loop.
        # This is the root cause of the April 24, 2026 spin.
        if task.done():
            continue

        # The current task is running this callback; it can't cancel
        # itself from inside it. Whatever state it's in (including
        # ``_must_cancel``) will be handled once we return and control
        # flows back to ``Task.__step``.
        if task is current:
            continue

        # Already flagged for cancellation. asyncio will deliver the
        # exception when the task resumes; no retry needed from us.
        # (Upstream anyio 4.13.0 set should_retry=True here — that's the
        # root-cause bug.)
        if task._must_cancel:  # type: ignore[attr-defined]
            continue

        # The task is eligible for cancellation if it has started.
        if task is self._host_task or _anyio_asyncio._task_started(task):
            waiter = task._fut_waiter  # type: ignore[attr-defined]
            if not isinstance(waiter, asyncio.Future) or not waiter.done():
                task.cancel(origin._cancel_reason)
                # We actually delivered a cancel — re-check next tick.
                should_retry = True
                if (
                    task is origin._host_task
                    and origin._pending_uncancellations is not None
                ):
                    origin._pending_uncancellations += 1

    # Deliver cancellation to child scopes that aren't shielded or running
    # their own cancellation callbacks.
    for scope in self._child_scopes:
        if not scope._shield and not scope.cancel_called:
            should_retry = scope._deliver_cancellation(origin) or should_retry

    # Schedule another callback only if we actually did work this pass.
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
        "anyio patch applied: CancelScope._deliver_cancellation fixed "
        "(prevents 100%% CPU spin on unrecoverable cancel)"
    )
    return True


# Apply on import so anyone who imports this module gets the patch.
apply()
