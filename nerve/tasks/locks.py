"""One lock per task for the changes that write a task file and its row.

A task change reads the row, writes or moves the markdown file, and writes
the row. Two changes of one task that interleave can strand an edit: one of
them writes the path it read after the other has moved the file into or out
of ``done/``, so the edit goes to a file that the row no longer names. Every
such change runs inside :func:`task_mutation`, so the changes of one task
run one at a time. Changes of different tasks do not wait for each other.

The lock is re-entrant for the asyncio task that holds it: a handler that
calls another handler for the same task (``task_update`` calls
``task_done``) does not wait for itself. Another asyncio task waits. The
locks live in this process; Nerve runs one gateway process.
"""

from __future__ import annotations

import asyncio
import contextlib
import weakref
from collections.abc import AsyncIterator

# A lock exists while some caller holds or waits for it.
_locks: "weakref.WeakValueDictionary[str, asyncio.Lock]" = weakref.WeakValueDictionary()
# The asyncio task that holds each task's lock.
_owners: dict[str, asyncio.Task] = {}


@contextlib.asynccontextmanager
async def task_mutation(task_id: str) -> AsyncIterator[None]:
    """Hold the lock of ``task_id`` for one file-plus-row change."""
    me = asyncio.current_task()
    if me is not None and _owners.get(task_id) is me:
        yield
        return
    lock = _locks.get(task_id)
    if lock is None:
        lock = asyncio.Lock()
        _locks[task_id] = lock
    async with lock:
        _owners[task_id] = me
        try:
            yield
        finally:
            _owners.pop(task_id, None)
