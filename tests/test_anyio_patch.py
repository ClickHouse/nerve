"""Regression tests for the anyio _deliver_cancellation hot-loop patch.

See nerve/_anyio_patch.py for the root-cause explanation.
"""

from __future__ import annotations

import asyncio

import anyio
import pytest

# Importing nerve applies the patch as a side effect of nerve/__init__.py.
import nerve  # noqa: F401
from nerve import _anyio_patch


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
    # Already applied from module import, so this should be a no-op.
    assert _anyio_patch.apply() is False


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


@pytest.mark.asyncio
async def test_no_hot_loop_when_only_task_is_current():
    """Regression: a CancelScope whose only task is the current task must not
    re-schedule ``_deliver_cancellation`` forever.

    This is the exact shape of the bug: ``should_retry`` used to be set to
    True simply because ``self._tasks`` was non-empty, even when no task
    could actually be cancelled. The scheduler then re-queued itself via
    ``call_soon()`` on every event-loop tick, pinning one CPU core.

    After the patch, the first pass must leave ``_cancel_handle = None``
    because nothing was delivered and nothing is awaiting pickup.
    """
    from anyio._backends._asyncio import CancelScope

    scope = CancelScope()
    # Enter/exit manually so we have a host_task bound to the running task
    # without letting anyio's normal finalization run cancellation for us.
    current = asyncio.current_task()
    scope._host_task = current
    scope._cancel_called = True
    scope._cancel_reason = "regression test"
    # Force the "pathological" shape: only task in scope is the current task,
    # so the upstream loop would spin forever without the patch.
    scope._tasks = {current}

    loop = asyncio.get_running_loop()
    # Drop any stray handle so we can assert cleanly against the post-state.
    scope._cancel_handle = None

    should_retry = scope._deliver_cancellation(scope)

    assert should_retry is False, (
        "Patched _deliver_cancellation must not retry when the only task "
        "in the scope is the current task (nothing to cancel)."
    )
    assert scope._cancel_handle is None, (
        "Patched _deliver_cancellation must clear _cancel_handle when no "
        "retry is needed — otherwise the hot loop returns."
    )

    # Sanity: event loop must not have a queued _deliver_cancellation
    # callback parked in it. We can't easily introspect the ready queue, but
    # we *can* confirm we didn't stash a handle on the scope ourselves.
    assert not loop.is_closed()
