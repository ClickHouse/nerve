"""Regression tests for the narrow anyio ``_deliver_cancellation`` patch.

The patch only deviates from upstream anyio 4.13.0 in one place: it skips
tasks where ``task.done()`` is True. Every other branch (deferred
self-delivery, re-delivery on swallowed ``CancelledError``, task-group
cancel, timer cancel) must behave exactly like upstream.

The bulk of these tests are *real* anyio behavior tests, not stub tests:
they exercise the patched ``_deliver_cancellation`` through normal anyio
APIs (``CancelScope``, ``create_task_group``, ``fail_after``,
``move_on_after``) and assert observable behavior — cancellation arrives
in <0.2s, ``CancelledError`` is redelivered after swallowing, no CPU
spin. The earlier wider patch (skipping current task + ``_must_cancel``)
broke three of these patterns, which is why it was reverted.

The single stub-based test (``test_no_hot_loop_when_only_task_is_done``)
covers the zombie-scope shape that the patch is *for*. Done tasks are
hard to construct in real anyio without racing the task-group unwind, so
a focused stub keeps the regression reliable.

See ``nerve/_anyio_patch.py`` for the root-cause writeup.
"""

from __future__ import annotations

import asyncio
import time

import anyio
import pytest

# Importing nerve applies the patch as a side effect of nerve/__init__.py.
import nerve  # noqa: F401
from nerve import _anyio_patch


# --------------------------------------------------------------------------- #
#  Module wiring                                                              #
# --------------------------------------------------------------------------- #


def test_patch_is_applied():
    """The patch must be active after ``import nerve``."""
    from anyio._backends import _asyncio as anyio_asyncio

    assert _anyio_patch._APPLIED is True
    assert (
        anyio_asyncio.CancelScope._deliver_cancellation.__module__
        == "nerve._anyio_patch"
    )


def test_apply_is_idempotent():
    """Calling apply() twice must not raise and must return False the 2nd time."""
    assert _anyio_patch.apply() is False


# --------------------------------------------------------------------------- #
#  Upstream behavior that must keep working                                   #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_fail_after_still_works():
    """Cancellation via fail_after must still raise TimeoutError."""
    with pytest.raises(TimeoutError):
        with anyio.fail_after(0.01):
            await anyio.sleep(1)


@pytest.mark.asyncio
async def test_move_on_after_still_works():
    """Cancellation via move_on_after must exit cleanly."""
    with anyio.move_on_after(0.01) as scope:
        await anyio.sleep(1)
    assert scope.cancelled_caught


@pytest.mark.asyncio
async def test_task_group_cancellation_still_works():
    """Cancelling a task group must cancel its children."""
    cancelled = []

    async def child(index):
        try:
            await anyio.sleep(10)
        except asyncio.CancelledError:
            cancelled.append(index)
            raise

    async with anyio.create_task_group() as tg:
        tg.start_soon(child, 0)
        tg.start_soon(child, 1)
        await anyio.sleep(0.01)
        tg.cancel_scope.cancel()

    assert sorted(cancelled) == [0, 1]


# --------------------------------------------------------------------------- #
#  Regressions Artem caught on PR #128 — must pass on the narrow patch        #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_self_cancel_then_await_sleep_cancels_immediately():
    """Regression: ``with CancelScope() as s: s.cancel(); await sleep(5)``
    must exit immediately, not sleep the full 5 seconds.

    ``s.cancel()`` calls ``_deliver_cancellation`` synchronously with
    ``current_task()`` pointing at the host task. anyio can't cancel the
    host task in place, so it reschedules ``_deliver_cancellation`` via
    ``call_soon``; on the next tick ``current_task()`` is None and the
    cancel actually lands. The earlier wider patch skipped the current
    task without setting ``should_retry`` → the reschedule never
    happened → the sleep ran to completion.
    """
    start = time.monotonic()
    cancelled = False

    try:
        with anyio.CancelScope() as scope:
            scope.cancel()
            await anyio.sleep(5)
            pytest.fail(
                "Unreachable: ``sleep(5)`` must raise CancelledError "
                "immediately after ``s.cancel()``."
            )
    except BaseException as exc:  # pragma: no cover — anyio handles internally
        # anyio swallows the CancelledError at scope exit; we only get here
        # if something escapes. Treat any escape as a failure.
        raise AssertionError(f"unexpected exception escaped scope: {exc!r}")

    elapsed = time.monotonic() - start
    cancelled = scope.cancel_called and scope.cancelled_caught
    assert elapsed < 0.5, (
        f"Self-cancel + await must return in <0.5s, took {elapsed:.3f}s. "
        "Likely regression: deferred self-delivery is broken."
    )
    assert cancelled, "Scope must report cancelled_caught after self-cancel."


@pytest.mark.asyncio
async def test_task_cancels_own_taskgroup_scope_then_awaits():
    """Regression: a task that cancels its own task-group's scope, then
    awaits, must be cancelled immediately.

    Same shape as the previous test but routed through a TaskGroup, which
    is the production pattern (e.g. SDK Query teardown). The earlier
    wider patch made the inner ``await sleep(5)`` run to completion.
    """
    start = time.monotonic()
    completed_sleep = False

    async def child(tg):
        nonlocal completed_sleep
        tg.cancel_scope.cancel()
        try:
            await anyio.sleep(5)
            completed_sleep = True
        except BaseException:
            raise

    async with anyio.create_task_group() as tg:
        tg.start_soon(child, tg)

    elapsed = time.monotonic() - start
    assert elapsed < 0.5, (
        f"Self-cancel of own task-group scope must return in <0.5s, "
        f"took {elapsed:.3f}s."
    )
    assert not completed_sleep, (
        "``await sleep(5)`` after self-cancelling the task group must "
        "raise, not run to completion."
    )


@pytest.mark.asyncio
async def test_swallowed_cancelled_error_is_redelivered():
    """Regression: a task that swallows the first ``CancelledError`` must
    be re-cancelled on the next pass.

    anyio's contract is to keep redelivering until the scope exits. The
    earlier wider patch skipped tasks with ``_must_cancel=True`` without
    setting ``should_retry``, so a task that caught the first cancel and
    looped never received a second one — the scope hung until its
    timeout, which in production took 15+ seconds.
    """
    deliveries = 0
    start = time.monotonic()

    try:
        with anyio.fail_after(2.0):
            with anyio.CancelScope() as scope:
                scope.cancel()
                for _ in range(20):
                    try:
                        await anyio.sleep(0.05)
                    except asyncio.CancelledError:
                        deliveries += 1
                        if deliveries >= 3:
                            raise
                        # Swallow and loop — anyio must re-cancel.
                        continue
                pytest.fail(
                    "Loop should have been re-cancelled before 20 iterations."
                )
    except TimeoutError:  # pragma: no cover — only if redelivery is broken
        pytest.fail(
            "fail_after(2.0) tripped — anyio stopped redelivering after the "
            "first swallowed CancelledError."
        )

    elapsed = time.monotonic() - start
    assert deliveries >= 3, (
        f"Expected >=3 deliveries of CancelledError, got {deliveries}."
    )
    assert elapsed < 1.5, (
        f"Re-delivery loop should complete quickly, took {elapsed:.3f}s."
    )


# --------------------------------------------------------------------------- #
#  Zombie-scope regression (the reason this patch exists)                     #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_no_hot_loop_when_only_task_is_done():
    """Regression (April 24, 2026 production observation): a CancelScope
    whose only task is already ``done()==True`` must not re-queue
    ``_deliver_cancellation`` forever.

    Three such zombie-scopes were found in a live process dump (scope IDs
    0x7ffec1774f50, 0x7ffec17ae090, 0x7ffec17ad6d0), each spinning at
    ~18k epoll_pwait/sec. Upstream sets ``should_retry=True`` for any
    task in ``_tasks`` regardless of ``done()``, then ``task.cancel()``
    is a no-op on a done task → infinite ``call_soon``.

    The patched version skips done tasks before ``should_retry`` is
    touched. With no other tasks, ``should_retry`` stays False, no
    reschedule, scope falls idle.

    Done tasks are hard to engineer reliably in real anyio without
    racing task-group cleanup, so this test stubs the task shape.
    """
    from anyio._backends._asyncio import CancelScope

    scope = CancelScope()
    current = asyncio.current_task()
    scope._host_task = current
    scope._cancel_called = True
    scope._cancel_reason = "regression test (done task)"
    scope._cancel_handle = None

    class _DoneTask:
        """Mimics an asyncio.Task that has already completed."""

        def __init__(self):
            self._must_cancel = False  # cleared when task finished
            self._fut_waiter = None  # done tasks have no waiter
            self._cancel_calls = 0

        def done(self):
            return True

        def cancel(self, *_args, **_kwargs):
            self._cancel_calls += 1
            return False  # cancel() returns False on done tasks

    done_task = _DoneTask()
    scope._tasks = {done_task}

    should_retry = scope._deliver_cancellation(scope)

    assert should_retry is False, (
        "Patched _deliver_cancellation must not retry when the only task "
        "in the scope is done. Retrying on a done task is the April 24 "
        "zombie-scope spin: cancel() is a no-op, but the callback keeps "
        "re-queuing itself every tick."
    )
    assert done_task._cancel_calls == 0, (
        "Must not call task.cancel() on a done task — it's a no-op and "
        "only serves to flip should_retry=True."
    )
    assert scope._cancel_handle is None, (
        "No retry → no pending handle. Otherwise the zombie spin "
        "returns on the next rebuild."
    )


@pytest.mark.asyncio
async def test_live_task_still_reschedules_alongside_done_task():
    """A scope with a mix of done + live tasks must still reschedule for
    the live task. The done-task skip must not short-circuit re-delivery
    for other tasks.
    """
    from anyio._backends._asyncio import CancelScope

    scope = CancelScope()
    current = asyncio.current_task()
    scope._host_task = current
    scope._cancel_called = True
    scope._cancel_reason = "regression test (mixed)"
    scope._cancel_handle = None

    class _DoneTask:
        _must_cancel = False
        _fut_waiter = None

        def done(self):
            return True

        def cancel(self, *_args, **_kwargs):
            return False

    class _LiveBlockedTask:
        """Live task with ``_must_cancel=True`` (already flagged) — same
        shape as upstream's re-delivery path. Must keep should_retry alive."""

        _must_cancel = True
        _fut_waiter = None

        def done(self):
            return False

        def cancel(self, *_args, **_kwargs):
            return True

    scope._tasks = {_DoneTask(), _LiveBlockedTask()}

    should_retry = scope._deliver_cancellation(scope)

    assert should_retry is True, (
        "Live task with _must_cancel=True must keep should_retry=True so "
        "anyio's re-delivery contract holds (this is the test that the "
        "earlier wider patch failed)."
    )
    assert scope._cancel_handle is not None, (
        "should_retry=True → must arm _cancel_handle for the next tick."
    )
    scope._cancel_handle.cancel()
    scope._cancel_handle = None
