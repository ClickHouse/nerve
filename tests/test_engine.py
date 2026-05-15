"""Tests for nerve.agent.engine — pure helpers (no SDK state)."""

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from nerve.agent.engine import AgentEngine


@pytest.mark.parametrize(
    "value, model, expected",
    [
        # Opus 4.7 supports every level
        ("max",    "claude-opus-4-7",           "max"),
        ("xhigh",  "claude-opus-4-7",           "xhigh"),
        ("high",   "claude-opus-4-7",           "high"),
        # Dated alias resolves via substring match
        ("max",    "claude-opus-4-7-20260416",  "max"),
        # Opus 4.6: max OK, xhigh caps to high (not registered)
        ("max",    "claude-opus-4-6",           "max"),
        ("xhigh",  "claude-opus-4-6",           "high"),
        # Sonnet 4.6 tops out at high
        ("max",    "claude-sonnet-4-6",         "high"),
        ("xhigh",  "claude-sonnet-4-6",         "high"),
        ("high",   "claude-sonnet-4-6",         "high"),
        ("medium", "claude-sonnet-4-6",         "medium"),
        ("low",    "claude-sonnet-4-6",         "low"),
        # Unknown models (including Haiku which uses budget_tokens, not levels)
        # pass through unchanged — capping is a no-op for non-level-based thinking
        ("max",    "claude-haiku-4-5-20251001", "max"),
        ("max",    "some-future-model",         "max"),
        ("max",    None,                        "max"),
        ("max",    "",                          "max"),
        # Invalid effort string → None (same as the pre-existing behaviour)
        ("invalid", "claude-opus-4-7",          None),
        ("",        "claude-sonnet-4-6",        None),
    ],
)
def test_effective_effort(value, model, expected):
    assert AgentEngine._effective_effort(value, model) == expected


def test_effective_effort_model_default_none():
    # Signature symmetry with _parse_thinking_config
    assert AgentEngine._effective_effort("max") == "max"


# ---------------------------------------------------------------------------
# _iter_response_with_timeout — hung-CLI detection
# ---------------------------------------------------------------------------


class _StubClient:
    """Minimal SDK-shaped client whose receive_response yields a fixed list.

    If ``hang`` is True, the generator sleeps after yielding all real
    messages instead of exiting cleanly — simulating a CLI that streams
    initial output then goes silent forever.

    Tracks whether ``aclose`` was called on the returned generator so the
    timeout path can assert cleanup.
    """

    def __init__(self, messages, hang=False, hang_seconds=10.0):
        self._messages = messages
        self._hang = hang
        self._hang_seconds = hang_seconds
        self.aclose_calls = 0

    def receive_response(self):
        outer = self

        async def _gen():
            try:
                for msg in outer._messages:
                    yield msg
                if outer._hang:
                    await asyncio.sleep(outer._hang_seconds)
            finally:
                outer.aclose_calls += 1

        return _gen()


@pytest.mark.asyncio
async def test_iter_response_yields_messages_normally():
    """Fast SDK stream completes without timing out."""
    client = _StubClient(["a", "b", "c"])
    seen = []
    async for msg in AgentEngine._iter_response_with_timeout(
        client, "sess-1", idle_timeout=5.0,
    ):
        seen.append(msg)
    assert seen == ["a", "b", "c"]
    # Generator was closed cleanly when it ran to completion.
    assert client.aclose_calls == 1


@pytest.mark.asyncio
async def test_iter_response_raises_on_idle_timeout():
    """If the SDK goes silent past idle_timeout, raise TimeoutError."""
    # Yields one message, then hangs long enough to trip a 50ms timeout.
    client = _StubClient(["a"], hang=True, hang_seconds=2.0)
    seen = []
    with pytest.raises(asyncio.TimeoutError):
        async for msg in AgentEngine._iter_response_with_timeout(
            client, "sess-2", idle_timeout=0.05,
        ):
            seen.append(msg)
    # The first message arrived before the hang.
    assert seen == ["a"]
    # The underlying iterator was closed before the exception propagated.
    assert client.aclose_calls == 1


@pytest.mark.asyncio
async def test_iter_response_disabled_when_timeout_zero():
    """idle_timeout <= 0 disables the timeout (legacy behaviour)."""
    # Hangs forever after 1 message.  Without a timeout we'd wait forever;
    # to verify "disabled" we wrap the whole call in our own short outer
    # timeout and assert that's what fired (not the inner one).
    client = _StubClient(["a"], hang=True, hang_seconds=10.0)
    seen = []
    with pytest.raises(asyncio.TimeoutError):
        async with asyncio.timeout(0.1):
            async for msg in AgentEngine._iter_response_with_timeout(
                client, "sess-3", idle_timeout=0,
            ):
                seen.append(msg)
    assert seen == ["a"]
    # Outer-cancel still triggers the finally block → aclose() runs.
    assert client.aclose_calls == 1


@pytest.mark.asyncio
async def test_iter_response_handles_empty_stream():
    """Empty receive_response (e.g. CLI exits immediately) returns cleanly."""
    client = _StubClient([])
    seen = []
    async for msg in AgentEngine._iter_response_with_timeout(
        client, "sess-4", idle_timeout=5.0,
    ):
        seen.append(msg)
    assert seen == []
    assert client.aclose_calls == 1
# _sdk_resume_file_exists
# ---------------------------------------------------------------------------

def _make_engine(workspace: str = "/root/nerve-workspace") -> AgentEngine:
    """Minimal AgentEngine stub (only config.workspace is needed)."""
    engine = AgentEngine.__new__(AgentEngine)
    engine.config = SimpleNamespace(workspace=Path(workspace))
    return engine


class TestSdkResumeFileExists:
    def test_returns_true_when_file_present(self):
        engine = _make_engine()
        with patch("nerve.agent.engine.os.path.isfile", return_value=True):
            assert engine._sdk_resume_file_exists("some-session-id") is True

    def test_returns_false_when_file_missing(self):
        engine = _make_engine()
        with patch("nerve.agent.engine.os.path.isfile", return_value=False):
            assert engine._sdk_resume_file_exists("some-session-id") is False

    def test_fail_open_on_exception(self):
        """Any unexpected error returns True rather than crashing the turn."""
        engine = _make_engine()
        with patch("nerve.agent.engine.os.path.isfile", side_effect=OSError("denied")):
            assert engine._sdk_resume_file_exists("some-session-id") is True

    def test_path_encodes_workspace_slashes(self):
        """'/' in the workspace path are replaced with '-' in the projects subdir."""
        engine = _make_engine("/root/nerve-workspace")
        captured: dict = {}

        def _capture(path: str) -> bool:
            captured["path"] = path
            return True

        with patch("nerve.agent.engine.os.path.isfile", side_effect=_capture):
            engine._sdk_resume_file_exists("sid-abc")

        assert "-root-nerve-workspace" in captured["path"]
        assert "sid-abc.jsonl" in captured["path"]

    def test_path_ends_with_jsonl(self):
        """The constructed path always ends with <session_id>.jsonl."""
        engine = _make_engine("/workspace")
        captured: dict = {}

        def _capture(path: str) -> bool:
            captured["path"] = path
            return True

        with patch("nerve.agent.engine.os.path.isfile", side_effect=_capture):
            engine._sdk_resume_file_exists("myid")

        assert captured["path"].endswith("myid.jsonl")
