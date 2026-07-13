"""Tests for nerve.agent.engine and nerve.agent.backends.claude — pure
helpers (no SDK subprocess)."""

import asyncio
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from claude_agent_sdk import AssistantMessage, TextBlock

from nerve.agent.backends.base import SessionSpec
from nerve.agent.backends.claude import ClaudeBackend, ClaudeClient, translate_message
from nerve.agent.engine import AgentEngine, _TurnState, _model_family
from nerve.config import AgentConfig, NerveConfig


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
    assert ClaudeBackend._effective_effort(value, model) == expected


def test_effective_effort_model_default_none():
    # Signature symmetry with _parse_thinking_config
    assert ClaudeBackend._effective_effort("max") == "max"


@pytest.mark.parametrize(
    "source, expected",
    [
        # Interactive sources keep the full interactive effort.
        ("web",      "max"),
        ("telegram", "max"),
        ("wakeup",   "max"),
        ("api",      "max"),
        # Cron and hook turns drop to cron_effort.
        ("cron",     "medium"),
        ("hook",     "medium"),
    ],
)
def test_base_effort_for_source(source, expected):
    assert AgentEngine._base_effort_for_source(source, "max", "medium") == expected


def test_base_effort_for_source_then_capped():
    # A cron turn at the default cron_effort stays "medium" after the model-cap
    # pass on Sonnet 4.6 (which tops out at "high", so medium is unaffected).
    base = AgentEngine._base_effort_for_source("cron", "max", "medium")
    assert ClaudeBackend._effective_effort(base, "claude-sonnet-4-6") == "medium"
    # Sonnet 5 is not in the cap table, so cron_effort passes through unchanged.
    assert ClaudeBackend._effective_effort(base, "claude-sonnet-5") == "medium"
    # An interactive turn keeps "max", which caps to "high" on Sonnet 4.6.
    base = AgentEngine._base_effort_for_source("web", "max", "medium")
    assert ClaudeBackend._effective_effort(base, "claude-sonnet-4-6") == "high"
    # A cron turn whose cron_effort is left at "max" still caps to the model max.
    base = AgentEngine._base_effort_for_source("cron", "max", "max")
    assert ClaudeBackend._effective_effort(base, "claude-sonnet-4-6") == "high"


def test_agent_config_cron_effort_default_and_override():
    # Default is medium when unset.
    assert AgentConfig.from_dict({}).cron_effort == "medium"
    # Explicit value is respected, and interactive effort stays independent.
    cfg = AgentConfig.from_dict({"cron_effort": "low"})
    assert cfg.cron_effort == "low"
    assert cfg.effort == "max"


def test_claude_system_prompt_excludes_codex_runbook_policy(tmp_path):
    """Codex-only runbook semantics must never alter Claude's prompt."""
    cfg = NerveConfig.from_dict({"workspace": str(tmp_path)})
    backend = ClaudeBackend(SimpleNamespace(
        config=cfg,
        claude_plugins=lambda: [],
    ))
    marker = "exact claude system prompt"
    spec = SessionSpec(
        session_id="claude-prompt-isolation",
        source="web",
        model=cfg.agent.model,
        effort="high",
        system_prompt=marker,
        cwd=str(tmp_path),
    )

    with patch.object(backend, "_build_mcp_servers", return_value={}), \
         patch.object(backend, "_build_hooks", return_value={}):
        options = backend._build_options(spec)

    assert options.system_prompt == marker
    assert "Nerve runbooks" not in str(options.system_prompt)
    assert "Codex-native skills" not in str(options.system_prompt)


# ---------------------------------------------------------------------------
# ClaudeClient.receive_turn — per-message idle timeout (hung-CLI detection)
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


def _turn_client(sdk: _StubClient, session_id: str, idle_timeout: float) -> ClaudeClient:
    """A ClaudeClient wired to a stub SDK (no subprocess)."""
    client = ClaudeClient.__new__(ClaudeClient)
    client._sdk = sdk
    client._spec = SessionSpec(
        session_id=session_id, source="web", model="m", effort="high",
        system_prompt="", cwd="/tmp", idle_timeout=idle_timeout,
    )
    client._native_session_id = None
    return client


def _text_msg(text: str) -> AssistantMessage:
    return AssistantMessage(content=[TextBlock(text=text)], model="claude-test")


def _translated(messages: list) -> list:
    """The normalized events the given SDK messages translate into."""
    return [event for m in messages for event in translate_message(m)]


@pytest.mark.asyncio
async def test_receive_turn_yields_events_normally():
    """Fast SDK stream completes without timing out."""
    messages = [_text_msg("a"), _text_msg("b"), _text_msg("c")]
    sdk = _StubClient(messages)
    client = _turn_client(sdk, "sess-1", idle_timeout=5.0)
    seen = []
    async for event in client.receive_turn():
        seen.append(event)
    assert seen == _translated(messages)
    # Generator was closed cleanly when it ran to completion.
    assert sdk.aclose_calls == 1


@pytest.mark.asyncio
async def test_receive_turn_raises_on_idle_timeout():
    """If the SDK goes silent past idle_timeout, raise TimeoutError."""
    # Yields one message, then hangs long enough to trip a 50ms timeout.
    messages = [_text_msg("a")]
    sdk = _StubClient(messages, hang=True, hang_seconds=2.0)
    client = _turn_client(sdk, "sess-2", idle_timeout=0.05)
    seen = []
    with pytest.raises(asyncio.TimeoutError):
        async for event in client.receive_turn():
            seen.append(event)
    # The first message's events arrived before the hang.
    assert seen == _translated(messages)
    # The underlying iterator was closed before the exception propagated.
    assert sdk.aclose_calls == 1


@pytest.mark.asyncio
async def test_receive_turn_disabled_when_timeout_zero():
    """idle_timeout <= 0 disables the timeout (legacy behaviour)."""
    # Hangs forever after 1 message.  Without a timeout we'd wait forever;
    # to verify "disabled" we wrap the whole call in our own short outer
    # timeout and assert that's what fired (not the inner one).
    messages = [_text_msg("a")]
    sdk = _StubClient(messages, hang=True, hang_seconds=10.0)
    client = _turn_client(sdk, "sess-3", idle_timeout=0)
    seen = []
    with pytest.raises(asyncio.TimeoutError):
        async with asyncio.timeout(0.1):
            async for event in client.receive_turn():
                seen.append(event)
    assert seen == _translated(messages)
    # Outer-cancel still triggers the finally block → aclose() runs.
    assert sdk.aclose_calls == 1


@pytest.mark.asyncio
async def test_receive_turn_handles_empty_stream():
    """Empty receive_response (e.g. CLI exits immediately) returns cleanly."""
    sdk = _StubClient([])
    client = _turn_client(sdk, "sess-4", idle_timeout=5.0)
    seen = []
    async for event in client.receive_turn():
        seen.append(event)
    assert seen == []
    assert sdk.aclose_calls == 1


# ---------------------------------------------------------------------------
# ClaudeBackend.validate_resume_target
# ---------------------------------------------------------------------------

def _make_backend() -> ClaudeBackend:
    """Minimal ClaudeBackend (validate_resume_target reads no config)."""
    return ClaudeBackend(SimpleNamespace(config=SimpleNamespace()))


class TestValidateResumeTarget:
    WORKSPACE = "/root/nerve-workspace"

    def test_returns_true_when_file_present(self):
        backend = _make_backend()
        with patch("nerve.agent.backends.claude.os.path.isfile", return_value=True):
            assert backend.validate_resume_target(
                "some-session-id", self.WORKSPACE,
            ) is True

    def test_returns_false_when_file_missing(self):
        backend = _make_backend()
        with patch("nerve.agent.backends.claude.os.path.isfile", return_value=False):
            assert backend.validate_resume_target(
                "some-session-id", self.WORKSPACE,
            ) is False

    def test_fail_open_on_exception(self):
        """Any unexpected error returns True rather than crashing the turn."""
        backend = _make_backend()
        with patch(
            "nerve.agent.backends.claude.os.path.isfile",
            side_effect=OSError("denied"),
        ):
            assert backend.validate_resume_target(
                "some-session-id", self.WORKSPACE,
            ) is True

    def test_path_encodes_workspace_slashes(self):
        """'/' in the workspace path are replaced with '-' in the projects subdir."""
        backend = _make_backend()
        captured: dict = {}

        def _capture(path: str) -> bool:
            captured["path"] = path
            return True

        # Pin realpath to identity so this test exercises only the
        # slash-encoding, independent of any host symlinks.  Symlink
        # resolution itself is covered by the symlinked-workspace tests.
        with patch("nerve.agent.backends.claude.os.path.realpath",
                   side_effect=lambda p: p), \
             patch("nerve.agent.backends.claude.os.path.isfile",
                   side_effect=_capture):
            backend.validate_resume_target("sid-abc", "/root/nerve-workspace")

        assert "-root-nerve-workspace" in captured["path"]
        assert "sid-abc.jsonl" in captured["path"]

    def test_path_ends_with_jsonl(self):
        """The constructed path always ends with <session_id>.jsonl."""
        backend = _make_backend()
        captured: dict = {}

        def _capture(path: str) -> bool:
            captured["path"] = path
            return True

        with patch("nerve.agent.backends.claude.os.path.isfile",
                   side_effect=_capture):
            backend.validate_resume_target("myid", "/workspace")

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

        backend = _make_backend()
        assert backend.validate_resume_target(sid, str(link_ws)) is True

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

        backend = _make_backend()
        assert backend.validate_resume_target("does-not-exist", str(link_ws)) is False

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

        backend = _make_backend()
        assert backend.validate_resume_target(sid, str(link_ws)) is True


# ---------------------------------------------------------------------------
# ClaudeBackend._build_hooks — background-agent permission parity
# ---------------------------------------------------------------------------

def _make_hook_backend(background_agent_permissions: bool) -> ClaudeBackend:
    """Minimal backend stub for exercising _build_hooks's PreToolUse wiring."""
    config = SimpleNamespace(
        agent=SimpleNamespace(
            background_agent_permissions=background_agent_permissions,
        ),
    )
    return ClaudeBackend(SimpleNamespace(config=config))


def _hook_spec(session_id: str) -> SessionSpec:
    return SessionSpec(
        session_id=session_id, source="web", model="m", effort="high",
        system_prompt="", cwd="/tmp",
    )


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
        backend = _make_hook_backend(True)
        hooks = backend._build_hooks(_hook_spec("sess-x"))
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
        backend = _make_hook_backend(True)
        grant = _catch_all_grant_hook(backend._build_hooks(_hook_spec("sess-x")))

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
        backend = _make_hook_backend(False)
        hooks = backend._build_hooks(_hook_spec("sess-y"))
        assert _catch_all_grant_hook(hooks) is None
        # Snapshot + image-validator hooks stay registered regardless.
        matchers = {m.matcher for m in hooks["PreToolUse"]}
        assert "Edit|Write|NotebookEdit" in matchers
        assert "Read" in matchers


# ---------------------------------------------------------------------------
# _model_family — serving-model identifier normalization
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "model, expected",
    [
        # Bare alias passes through
        ("claude-fable-5",                    "claude-fable-5"),
        ("claude-opus-4-8",                   "claude-opus-4-8"),
        # Dated release ids collapse onto the alias
        ("claude-fable-5-20260601",           "claude-fable-5"),
        ("claude-opus-4-8-20260115",          "claude-opus-4-8"),
        # Bedrock inference-profile spellings
        ("us.anthropic.claude-fable-5-20260601-v1:0", "claude-fable-5"),
        ("global.anthropic.claude-fable-5",   "claude-fable-5"),
        # Context-window suffix
        ("claude-sonnet-4-5[1m]",             "claude-sonnet-4-5"),
        # "-latest" alias
        ("claude-haiku-4-5-latest",           "claude-haiku-4-5"),
        # Case / whitespace robustness
        ("  Claude-Fable-5-20260601 ",        "claude-fable-5"),
        # Version-looking tails that are NOT dates stay intact
        ("claude-opus-4",                     "claude-opus-4"),
        ("claude-3-5-sonnet-20241022",        "claude-3-5-sonnet"),
    ],
)
def test_model_family(model, expected):
    assert _model_family(model) == expected


def test_model_family_distinguishes_real_changes():
    # The pair that matters: a frontier model downgrading to the prior tier
    # must NOT normalize to the same family.
    assert _model_family("claude-fable-5-20260601") != _model_family(
        "claude-opus-4-8-20260115",
    )


# ---------------------------------------------------------------------------
# _track_serving_model — downgrade / recovery detection via
# translate_message + _process_agent_event (the shared user-run +
# autonomous-turn path)
# ---------------------------------------------------------------------------


def _make_model_engine(configured: str | None = "claude-fable-5") -> AgentEngine:
    """Minimal AgentEngine stub for serving-model tracking tests."""
    engine = AgentEngine.__new__(AgentEngine)
    engine._session_models = {"s1": configured} if configured else {}
    engine._observed_models = {}
    engine._workflows = {}
    return engine


def _assistant(model: str, parent_tool_use_id: str | None = None) -> AssistantMessage:
    return AssistantMessage(
        content=[], model=model, parent_tool_use_id=parent_tool_use_id,
    )


async def _process_sdk_message(engine: AgentEngine, session_id, message, st) -> bool:
    """Feed one SDK message through the live pipeline: translate it into
    normalized events, dispatch each to the engine. Returns True when the
    message completed the turn (TurnCompleted event)."""
    done = False
    for event in translate_message(message):
        done = await engine._process_agent_event(session_id, event, st)
    return done


class TestServingModelTracking:
    @pytest.mark.asyncio
    async def test_first_message_downgrade_fires_event(self):
        engine = _make_model_engine()
        st = _TurnState()
        with patch("nerve.agent.engine.broadcaster") as bc:
            bc.broadcast_model_changed = AsyncMock()
            await _process_sdk_message(
                engine, "s1", _assistant("claude-opus-4-8-20260115"), st,
            )
        assert st.last_model == "claude-opus-4-8-20260115"
        assert st.ordered_blocks == [{
            "type": "model_change",
            "from": "claude-fable-5",
            "to": "claude-opus-4-8-20260115",
            "downgrade": True,
        }]
        bc.broadcast_model_changed.assert_awaited_once_with(
            "s1",
            from_model="claude-fable-5",
            to_model="claude-opus-4-8-20260115",
            downgrade=True,
        )

    @pytest.mark.asyncio
    async def test_same_family_dated_id_is_not_a_change(self):
        engine = _make_model_engine()
        st = _TurnState()
        with patch("nerve.agent.engine.broadcaster") as bc:
            bc.broadcast_model_changed = AsyncMock()
            await _process_sdk_message(
                engine, "s1", _assistant("claude-fable-5-20260601"), st,
            )
        assert st.ordered_blocks == []
        bc.broadcast_model_changed.assert_not_awaited()
        # Baseline still updated for subsequent comparisons
        assert engine._observed_models["s1"] == "claude-fable-5-20260601"

    @pytest.mark.asyncio
    async def test_recovery_back_to_configured_is_not_downgrade(self):
        engine = _make_model_engine()
        engine._observed_models["s1"] = "claude-opus-4-8-20260115"
        st = _TurnState()
        with patch("nerve.agent.engine.broadcaster") as bc:
            bc.broadcast_model_changed = AsyncMock()
            await _process_sdk_message(
                engine, "s1", _assistant("claude-fable-5-20260601"), st,
            )
        assert st.ordered_blocks == [{
            "type": "model_change",
            "from": "claude-opus-4-8-20260115",
            "to": "claude-fable-5-20260601",
            "downgrade": False,
        }]

    @pytest.mark.asyncio
    async def test_mid_session_transition_uses_observed_baseline(self):
        engine = _make_model_engine()
        st = _TurnState()
        with patch("nerve.agent.engine.broadcaster") as bc:
            bc.broadcast_model_changed = AsyncMock()
            await _process_sdk_message(
                engine, "s1", _assistant("claude-fable-5-20260601"), st,
            )
            await _process_sdk_message(
                engine, "s1", _assistant("claude-opus-4-8-20260115"), st,
            )
        # Only the second message fires; "from" is the observed dated id,
        # not the configured alias.
        assert len(st.ordered_blocks) == 1
        assert st.ordered_blocks[0]["from"] == "claude-fable-5-20260601"
        assert st.ordered_blocks[0]["downgrade"] is True

    @pytest.mark.asyncio
    async def test_subagent_messages_are_ignored(self):
        engine = _make_model_engine()
        st = _TurnState()
        with patch("nerve.agent.engine.broadcaster") as bc:
            bc.broadcast_model_changed = AsyncMock()
            await _process_sdk_message(
                engine, "s1",
                _assistant("claude-haiku-4-5", parent_tool_use_id="tu_1"),
                st,
            )
        # Sub-agents legitimately run other models — no event, no baseline
        # pollution, and no cost-attribution model override.
        assert st.last_model is None
        assert st.ordered_blocks == []
        assert "s1" not in engine._observed_models
        bc.broadcast_model_changed.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_configured_model_first_message_is_quiet(self):
        engine = _make_model_engine(configured=None)
        st = _TurnState()
        with patch("nerve.agent.engine.broadcaster") as bc:
            bc.broadcast_model_changed = AsyncMock()
            await _process_sdk_message(
                engine, "s1", _assistant("claude-opus-4-8-20260115"), st,
            )
        # Nothing to compare against — record the baseline silently.
        assert st.ordered_blocks == []
        bc.broadcast_model_changed.assert_not_awaited()
        assert engine._observed_models["s1"] == "claude-opus-4-8-20260115"
