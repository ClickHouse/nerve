"""Graceful shutdown of supervised background tasks (nerve.utils.aio)."""

import asyncio
import logging

import pytest

from nerve.utils.aio import stop_background_task


async def _cycling_worker(
    stop: asyncio.Event,
    started: asyncio.Event,
    finished: list[int],
    cycle_seconds: float = 0.2,
) -> None:
    """A background task shaped like the real ones: a loop whose body is a
    critical section that must not be abandoned partway through."""
    while not stop.is_set():
        started.set()
        await asyncio.sleep(cycle_seconds)  # stands in for the real work
        finished.append(1)


class TestStopBackgroundTask:
    """The stop event is the mechanism and cancellation is the backstop, not
    the other way around: a task cancelled where it stands can be halfway
    through a unit of work that has to be all-or-nothing."""

    @pytest.mark.asyncio
    async def test_the_stop_event_alone_ends_the_task(self):
        """No cancellation involved anywhere: signalling has to be enough on
        its own. If it isn't, the event is decorative and shutdown is really
        just killing the task wherever it stands."""
        stop, started, finished = asyncio.Event(), asyncio.Event(), []
        task = asyncio.create_task(
            _cycling_worker(stop, started, finished, cycle_seconds=0.01)
        )
        await asyncio.wait_for(started.wait(), timeout=2)

        await stop_background_task(task, stop, "Test worker")
        assert task.done()
        assert not task.cancelled()  # it left through its own exit

    @pytest.mark.asyncio
    async def test_a_cycle_in_flight_is_allowed_to_finish(self):
        """The reason the stop event has to come first. Shutdown lands while a
        cycle is mid-flight; cancelling there abandons the work half-done, so
        the cycle gets to complete."""
        stop, started, finished = asyncio.Event(), asyncio.Event(), []
        task = asyncio.create_task(_cycling_worker(stop, started, finished))
        await asyncio.wait_for(started.wait(), timeout=2)

        await stop_background_task(task, stop, "Test worker")
        assert finished == [1]
        assert not task.cancelled()

    @pytest.mark.asyncio
    async def test_a_task_that_will_not_stop_is_cancelled_after_the_timeout(
        self, caplog
    ):
        """The backstop still exists: a wedged task must not hold shutdown open
        indefinitely, and giving up on one should say so out loud."""
        async def deaf_to_the_event():
            while True:
                await asyncio.sleep(0.01)

        task = asyncio.create_task(deaf_to_the_event())
        with caplog.at_level(logging.WARNING, logger="nerve.utils.aio"):
            await stop_background_task(
                task, asyncio.Event(), "Test worker", timeout=0.2
            )
        assert task.cancelled()
        assert "did not stop within" in caplog.text

    @pytest.mark.asyncio
    async def test_without_a_stop_event_cancellation_is_the_only_lever(self):
        """Setup can fail after the task exists but before the event does."""
        async def forever():
            while True:
                await asyncio.sleep(0.01)

        task = asyncio.create_task(forever())
        await stop_background_task(task, None, "Test worker")
        assert task.cancelled()

    @pytest.mark.asyncio
    async def test_a_task_that_already_died_is_logged_not_raised(self, caplog):
        """Shutdown continues past a background task that crashed earlier —
        there is a whole teardown sequence after this call."""
        async def boom():
            raise RuntimeError("worker died hours ago")

        task = asyncio.create_task(boom())
        with caplog.at_level(logging.WARNING, logger="nerve.utils.aio"):
            await stop_background_task(task, asyncio.Event(), "Test worker")
        assert "worker died hours ago" in caplog.text
