"""LocalRolloutOrigin — tail rollout JSONL files.

The tests use small synthetic fixtures (under tests/fixtures/codex/)
to avoid relying on a live Codex install. Each test exercises one
contract:

  * in-scope file → emits a thread_in_scope + the events that follow
  * out-of-scope file → emits only a thread_out_of_scope sentinel
  * partial trailing line → not emitted, picked up on next pass after
    Codex flushes the newline
  * cursor round-trip → resumes from where we left off
  * file moved to ``archived_sessions`` → emits thread_archived once
"""

from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path
from typing import AsyncIterator

import pytest

from nerve.sources.codex_threads.base import (
    ThreadEvent,
    WorkspaceFilter,
)
from nerve.sources.codex_threads.origins.local_rollout import (
    LocalRolloutOrigin,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "codex" / "rollouts"
TEST_WORKSPACE = Path("/tmp/nerve-test-ws")


class _StreamPump:
    """Wrap an async iterator in a queue-backed reader so a test can
    drain it multiple times with separate deadlines without killing the
    underlying generator."""

    def __init__(self, stream: AsyncIterator[ThreadEvent]):
        self._stream = stream
        self._queue: asyncio.Queue[ThreadEvent] = asyncio.Queue()
        self._task = asyncio.create_task(self._pump())

    async def _pump(self) -> None:
        try:
            async for evt in self._stream:
                await self._queue.put(evt)
        except asyncio.CancelledError:
            return

    async def drain(self, n: int, timeout: float) -> list[ThreadEvent]:
        out: list[ThreadEvent] = []
        deadline = asyncio.get_event_loop().time() + timeout
        while len(out) < n:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                break
            try:
                evt = await asyncio.wait_for(self._queue.get(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            out.append(evt)
        return out

    async def aclose(self) -> None:
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass


async def _drain(stream: AsyncIterator[ThreadEvent], n: int, timeout: float = 1.0) -> list[ThreadEvent]:
    """One-shot drain helper for tests that only need a single read."""
    pump = _StreamPump(stream)
    try:
        return await pump.drain(n, timeout)
    finally:
        await pump.aclose()


@pytest.fixture
def workspace_filter():
    return WorkspaceFilter(
        mode="nerve_workspace",
        nerve_workspace_path=TEST_WORKSPACE,
    )


@pytest.fixture
def sessions_dir(tmp_path: Path) -> Path:
    p = tmp_path / "sessions"
    p.mkdir()
    return p


@pytest.fixture
def archive_dir(tmp_path: Path) -> Path:
    p = tmp_path / "archived_sessions"
    p.mkdir()
    return p


def _copy_fixture(name: str, dest_dir: Path, new_name: str | None = None) -> Path:
    src = FIXTURE_DIR / name
    dst = dest_dir / (new_name or name)
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(src, dst)
    return dst


@pytest.mark.asyncio
async def test_in_scope_file_emits_session_then_events(
    workspace_filter, sessions_dir, archive_dir,
):
    _copy_fixture("in_scope.jsonl", sessions_dir / "2026" / "05" / "19")
    origin = LocalRolloutOrigin(
        id="t",
        sessions_path=sessions_dir,
        archive_path=archive_dir,
        workspace_filter=workspace_filter,
        poll_interval_seconds=0.1,
    )
    await origin.initialize()
    try:
        events = await _drain(origin.stream(cursor=None), n=20, timeout=2.0)
    finally:
        await origin.close()

    types = [e.type for e in events]
    assert types[0] == "thread_in_scope"
    # Subsequent events follow the fixture order — we skipped the
    # developer + auto-injected AGENTS user message, kept the rest.
    assert "user_message" in types
    assert "assistant_message" in types
    assert "reasoning" in types
    assert "tool_call" in types
    assert "tool_result" in types


@pytest.mark.asyncio
async def test_out_of_scope_file_emits_marker_and_skips_rest(
    workspace_filter, sessions_dir, archive_dir,
):
    _copy_fixture("out_of_scope.jsonl", sessions_dir / "2026" / "05" / "19")
    origin = LocalRolloutOrigin(
        id="t",
        sessions_path=sessions_dir,
        archive_path=archive_dir,
        workspace_filter=workspace_filter,
        poll_interval_seconds=0.1,
    )
    await origin.initialize()
    try:
        events = await _drain(origin.stream(cursor=None), n=4, timeout=1.0)
    finally:
        await origin.close()

    # We should see exactly ONE thread_out_of_scope event — nothing else
    # from the rollout (no user/assistant/etc.)
    assert any(e.type == "thread_out_of_scope" for e in events)
    assert not any(e.type in ("user_message", "assistant_message") for e in events)


@pytest.mark.asyncio
async def test_partial_trailing_line_is_not_emitted_until_flushed(
    workspace_filter, sessions_dir, archive_dir, tmp_path,
):
    rollout = _copy_fixture(
        "partial_tail.jsonl", sessions_dir / "2026" / "05" / "19",
    )
    origin = LocalRolloutOrigin(
        id="t",
        sessions_path=sessions_dir,
        archive_path=archive_dir,
        workspace_filter=workspace_filter,
        poll_interval_seconds=0.1,
    )
    await origin.initialize()
    try:
        events = await _drain(origin.stream(cursor=None), n=20, timeout=1.0)
    finally:
        await origin.close()

    # The fixture has TWO complete lines after session_meta plus a
    # partial third. We must see the two complete ones and NOT the
    # third (which is "third incomplete" without a newline).
    user_messages = [e for e in events if e.type == "user_message"]
    contents = [e.payload.get("message") for e in user_messages]
    assert "first complete line" in contents
    assert "third incomplete" not in contents


@pytest.mark.asyncio
async def test_cursor_round_trip_resumes_from_offset(
    workspace_filter, sessions_dir, archive_dir,
):
    _copy_fixture("in_scope.jsonl", sessions_dir / "2026" / "05" / "19")
    origin1 = LocalRolloutOrigin(
        id="t",
        sessions_path=sessions_dir,
        archive_path=archive_dir,
        workspace_filter=workspace_filter,
        poll_interval_seconds=0.1,
    )
    await origin1.initialize()
    try:
        # Drain everything from pass 1 — the test exercises restart-
        # resumption, not partial-consumer behaviour.
        events1 = await _drain(origin1.stream(cursor=None), n=100, timeout=2.0)
    finally:
        await origin1.close()
    cursor = origin1.cursor()
    parsed = json.loads(cursor)
    assert len(parsed["files"]) == 1
    assert len(parsed["in_scope"]) == 1
    initial_count = len(events1)
    assert initial_count > 0

    # Second pass with the saved cursor should see zero new events.
    origin2 = LocalRolloutOrigin(
        id="t",
        sessions_path=sessions_dir,
        archive_path=archive_dir,
        workspace_filter=workspace_filter,
        poll_interval_seconds=0.1,
    )
    await origin2.initialize()
    try:
        events2 = await _drain(origin2.stream(cursor=cursor), n=2, timeout=0.5)
    finally:
        await origin2.close()
    assert events2 == [], f"resumed origin emitted unexpected events: {events2}"


@pytest.mark.asyncio
async def test_archived_file_emits_thread_archived(
    workspace_filter, sessions_dir, archive_dir,
):
    # Drop the in_scope fixture into archive — origin should emit a
    # one-time thread_archived sentinel after tailing.
    _copy_fixture("in_scope.jsonl", archive_dir)
    origin = LocalRolloutOrigin(
        id="t",
        sessions_path=sessions_dir,
        archive_path=archive_dir,
        workspace_filter=workspace_filter,
        poll_interval_seconds=0.1,
    )
    await origin.initialize()
    try:
        events = await _drain(origin.stream(cursor=None), n=20, timeout=2.0)
    finally:
        await origin.close()

    archived = [e for e in events if e.type == "thread_archived"]
    assert len(archived) == 1


@pytest.mark.asyncio
async def test_new_file_in_subdirectory_detected_on_next_scan(
    workspace_filter, sessions_dir, archive_dir,
):
    origin = LocalRolloutOrigin(
        id="t",
        sessions_path=sessions_dir,
        archive_path=archive_dir,
        workspace_filter=workspace_filter,
        poll_interval_seconds=0.1,
    )
    await origin.initialize()
    pump = _StreamPump(origin.stream(cursor=None))
    try:
        # Drain initial scan — empty directory, so nothing yet.
        initial = await pump.drain(n=1, timeout=0.3)
        assert initial == []

        # Drop a rollout while the stream is running.
        target_dir = sessions_dir / "2026" / "05" / "19"
        _copy_fixture("in_scope.jsonl", target_dir)

        events = await pump.drain(n=4, timeout=2.0)
    finally:
        await pump.aclose()
        await origin.close()

    types = [e.type for e in events]
    assert "thread_in_scope" in types
