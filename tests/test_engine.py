"""Tests for nerve.agent.engine — pure helpers (no SDK state)."""

import asyncio
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from nerve.agent.engine import AgentEngine


@pytest.mark.parametrize(
    "value, model, expected",
    [
        # Fable 5 (Mythos-class) supports the full effort ladder
        ("max",    "claude-fable-5",            "max"),
        ("xhigh",  "claude-fable-5",            "xhigh"),
        ("low",    "claude-fable-5",            "low"),
        # Opus 4.8 supports every level (same ladder as 4.7)
        ("max",    "claude-opus-4-8",           "max"),
        ("xhigh",  "claude-opus-4-8",           "xhigh"),
        ("high",   "claude-opus-4-8",           "high"),
        # Bedrock dateless ID resolves via substring match
        ("max",    "us.anthropic.claude-opus-4-8", "max"),
        # Opus 4.7 still resolves correctly for legacy configs
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

        # Pin realpath to identity so this test exercises only the
        # slash-encoding, independent of any host symlinks.  Symlink
        # resolution itself is covered by the symlinked-workspace tests.
        with patch("nerve.agent.engine.os.path.realpath", side_effect=lambda p: p), \
             patch("nerve.agent.engine.os.path.isfile", side_effect=_capture):
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

    def test_symlinked_workspace_checks_realpath(self, tmp_path, monkeypatch):
        """A symlinked workspace finds its history under the *resolved*
        (realpath) directory, matching where the CLI actually writes it.

        Regression test for resume-history loss on the Docker deployment,
        where config.workspace (/root/nerve-workspace) is a symlink and
        the guard previously checked the unresolved-path-encoded dir,
        which never exists, then wiped the stored sdk_session_id.
        """
        real_ws = tmp_path / "real-workspace"
        real_ws.mkdir()
        link_ws = tmp_path / "linked-workspace"
        link_ws.symlink_to(real_ws)

        fake_home = tmp_path / "home"
        monkeypatch.setenv("HOME", str(fake_home))

        sid = "11111111-2222-3333-4444-555555555555"
        encoded = os.path.realpath(str(link_ws)).replace("/", "-")
        proj_dir = fake_home / ".claude" / "projects" / encoded
        proj_dir.mkdir(parents=True)
        (proj_dir / f"{sid}.jsonl").write_text("{}")

        engine = _make_engine(str(link_ws))
        assert engine._sdk_resume_file_exists(sid) is True

    def test_symlinked_workspace_missing_returns_false(self, tmp_path, monkeypatch):
        """Symlinked workspace, but the .jsonl genuinely does not exist:
        the guard returns False so the caller starts a fresh conversation."""
        real_ws = tmp_path / "real-workspace"
        real_ws.mkdir()
        link_ws = tmp_path / "linked-workspace"
        link_ws.symlink_to(real_ws)

        fake_home = tmp_path / "home"
        monkeypatch.setenv("HOME", str(fake_home))

        encoded = os.path.realpath(str(link_ws)).replace("/", "-")
        (fake_home / ".claude" / "projects" / encoded).mkdir(parents=True)

        engine = _make_engine(str(link_ws))
        assert engine._sdk_resume_file_exists("does-not-exist") is False

    def test_falls_back_to_unresolved_path(self, tmp_path, monkeypatch):
        """If history lives under the unresolved (symlink) encoding, the
        fallback still finds it (non-symlinked layouts, or a future CLI
        that stops resolving the cwd)."""
        real_ws = tmp_path / "real-workspace"
        real_ws.mkdir()
        link_ws = tmp_path / "linked-workspace"
        link_ws.symlink_to(real_ws)

        fake_home = tmp_path / "home"
        monkeypatch.setenv("HOME", str(fake_home))

        sid = "abcd"
        encoded_unresolved = str(link_ws).replace("/", "-")
        proj_dir = fake_home / ".claude" / "projects" / encoded_unresolved
        proj_dir.mkdir(parents=True)
        (proj_dir / f"{sid}.jsonl").write_text("{}")

        engine = _make_engine(str(link_ws))
        assert engine._sdk_resume_file_exists(sid) is True


# ---------------------------------------------------------------------------
# _build_hooks — background-agent permission parity
# ---------------------------------------------------------------------------

def _make_hook_engine(background_agent_permissions: bool) -> AgentEngine:
    """Minimal engine stub for exercising _build_hooks's PreToolUse wiring."""
    engine = AgentEngine.__new__(AgentEngine)
    engine.config = SimpleNamespace(
        agent=SimpleNamespace(
            background_agent_permissions=background_agent_permissions,
        ),
    )
    return engine


def _catch_all_grant_hook(hooks: dict):
    """Return the catch-all (matcher=None) PreToolUse hook callback, or None."""
    for matcher in hooks.get("PreToolUse", []):
        if matcher.matcher is None:
            return matcher.hooks[0]
    return None


class TestBuildHooksBackgroundPermissions:
    """The catch-all PreToolUse hook gives background sub-agents (whose nested
    tool calls never reach can_use_tool) the same permissions as foreground."""

    @pytest.mark.asyncio
    async def test_grants_non_interactive_tools_when_enabled(self):
        engine = _make_hook_engine(True)
        hooks = engine._build_hooks("sess-x")
        grant = _catch_all_grant_hook(hooks)
        assert grant is not None, "permission-grant hook should be registered"

        # Permission-requiring, non-interactive tools are pre-approved so a
        # detached background sub-agent can run them without a prompt.
        for tool in ("Bash", "Write", "Edit", "NotebookEdit", "Glob",
                     "mcp__some_server__write_thing"):
            out = await grant({"tool_name": tool}, "tid", None)
            spec = out["hookSpecificOutput"]
            assert spec.get("permissionDecision") == "allow", tool

    @pytest.mark.asyncio
    async def test_defers_interactive_and_read_when_enabled(self):
        engine = _make_hook_engine(True)
        grant = _catch_all_grant_hook(engine._build_hooks("sess-x"))

        # Interactive tools defer to can_use_tool (pause / inject / deny):
        # the hook must NOT pre-decide them, or the web pause-for-input breaks.
        for tool in ("AskUserQuestion", "ExitPlanMode", "EnterPlanMode"):
            out = await grant({"tool_name": tool}, "tid", None)
            assert "permissionDecision" not in out["hookSpecificOutput"], tool

        # Read defers to the image validator (a deny there must win), so the
        # catch-all hook leaves it untouched.
        out = await grant({"tool_name": "Read"}, "tid", None)
        assert "permissionDecision" not in out["hookSpecificOutput"]

    @pytest.mark.asyncio
    async def test_no_grant_hook_when_disabled(self):
        engine = _make_hook_engine(False)
        hooks = engine._build_hooks("sess-y")
        assert _catch_all_grant_hook(hooks) is None
        # Snapshot + image-validator hooks stay registered regardless.
        matchers = {m.matcher for m in hooks["PreToolUse"]}
        assert "Edit|Write|NotebookEdit" in matchers
        assert "Read" in matchers
