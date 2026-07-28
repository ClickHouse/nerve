"""Asyncio helpers for services that run supervised background tasks.

Lives here rather than in ``nerve.gateway.server`` so a service can shut its
own background task down the same way the lifespan does without importing the
FastAPI app.
"""

from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)

# How long shutdown gives a background task to stop on its own after being
# asked, before falling back to cancellation. Sized for one external-agents
# sweep to finish, and short enough that a wedged task cannot hold the process
# open for a user watching it exit.
GRACEFUL_STOP_SECONDS = 5.0


async def stop_background_task(
    task: asyncio.Task,
    stop_event: asyncio.Event | None,
    name: str,
    timeout: float = GRACEFUL_STOP_SECONDS,
) -> None:
    """Stop a background task via its stop event, cancelling only if it hangs.

    Setting the event and cancelling in the same breath makes the event
    decorative: the CancelledError is delivered before the task next gets to
    look at it, so the task dies wherever it happened to be suspended instead
    of at the end of the cycle it was in. The external-agents sync can be
    halfway through a sweep at that moment, leaving some bundles rendered from
    the new sources and some from the old. So signal, wait a bounded moment for
    the task to wind itself down, and only cancel if it doesn't.
    """
    if stop_event is not None:
        stop_event.set()
    else:
        # No cooperative signal to give — cancellation is the only lever.
        task.cancel()
    try:
        # wait_for cancels the task itself once the timeout expires and awaits
        # the cancellation, so the backstop needs no separate cancel() and the
        # task is never left pending on either path.
        await asyncio.wait_for(task, timeout)
    except asyncio.TimeoutError:
        logger.warning(
            "%s did not stop within %.1fs of being asked; cancelled it",
            name, timeout,
        )
    except asyncio.CancelledError:
        # The task's cancellation, not ours: either we asked for it above or
        # something else already had. Nothing left to wait for.
        pass
    except Exception as e:
        logger.warning("%s raised while shutting down: %s", name, e)
