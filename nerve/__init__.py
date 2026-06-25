"""Nerve — Personal AI Assistant."""

# Apply anyio hot-loop patch as early as possible — monkey-patches
# CancelScope._deliver_cancellation to prevent 100% CPU spins on
# unrecoverable cancellations. See nerve/_anyio_patch.py for details.
from nerve import _anyio_patch as _anyio_patch  # noqa: F401

__version__ = "0.1.0"
