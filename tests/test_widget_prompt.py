"""The widget sentences of the system prompt's "Rendering" line.

The web chat draws a fenced ```widget block as a live piece of HTML, in the
sandbox frame of MCP Apps. The model hears of widgets only where they are
drawn: on a source that renders, of an instance whose sandbox is active.
Everywhere else the line says nothing of widgets, and in every case it
depends on the source and the configuration only, so the prompt stays static
(the prompt cache is exact-prefix).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

from nerve.agent import prompts
from nerve.agent.backends.base import BackendCapabilities
from nerve.agent.backends.claude import translate_message
from nerve.agent.engine import AgentEngine
from nerve.agent.prompts import (
    RICH_RENDERING_NOTE,
    TEXT_RENDERING_NOTE,
    WIDGET_RENDERING_NOTE,
    build_system_prompt,
    rendering_note,
)
from nerve.config import NerveConfig

# The "Rendering" line of a web chat without the widget sentences, word for word.
RENDERING_LINE_WITHOUT_WIDGETS = (
    "- **Rendering:** this chat renders fenced ```mermaid (diagrams), "
    "```vega-lite (charts — inline `values` only, no `url` data) and ```svg "
    "code blocks inline, source one click away. Prefer them to ASCII-art "
    "diagrams; everything else is GitHub-flavoured markdown. Drawing well: "
    "`flowchart TB` for chains longer than five nodes (LR only for short "
    "ones), node labels of a few words, one idea per diagram; charts with a "
    "`title`, no `width` (they fill the column), and typed fields."
)
TEXT_ONLY_LINE = (
    "- **Rendering:** this channel shows text and basic markdown only — no "
    "diagrams or charts. Do not emit ```mermaid, ```vega-lite or ```svg "
    "blocks here; give the structure as words or a list."
)

RENDERING = ("web", "api")
TEXT_ONLY = ("telegram", "slack", "cron", "wakeup", "hook")


def _rendering_line(prompt: str) -> str:
    return next(ln for ln in prompt.splitlines() if ln.startswith("- **Rendering:**"))


# --------------------------------------------------------------------------- #
#  rendering_note
# --------------------------------------------------------------------------- #


def test_without_widgets_the_line_is_these_bytes():
    assert RICH_RENDERING_NOTE == RENDERING_LINE_WITHOUT_WIDGETS
    assert TEXT_RENDERING_NOTE == TEXT_ONLY_LINE
    for source in RENDERING:
        assert rendering_note(source) == RENDERING_LINE_WITHOUT_WIDGETS
        assert rendering_note(source, widgets=False) == RENDERING_LINE_WITHOUT_WIDGETS
    for source in TEXT_ONLY:
        assert rendering_note(source) == TEXT_ONLY_LINE


def test_with_widgets_a_rendering_source_gets_the_widget_sentences():
    for source in RENDERING:
        line = rendering_note(source, widgets=True)
        assert line == RENDERING_LINE_WITHOUT_WIDGETS + WIDGET_RENDERING_NOTE
        assert "```widget" in line


def test_a_text_channel_never_hears_of_widgets():
    """So its "do not emit" list needs no ```widget entry."""
    for source in TEXT_ONLY:
        line = rendering_note(source, widgets=True)
        assert line == TEXT_ONLY_LINE
        assert "widget" not in line


def test_the_widget_sentences_say_what_a_widget_is_and_is_not():
    note = WIDGET_RENDERING_NOTE
    # One bullet of the Session Context list: no line break.
    assert "\n" not in note and note.startswith(" ")
    for words in (
        # What it is.
        "```widget", "self-contained HTML", "`<style>`", "`<script>`",
        # What it cannot do.
        "no network", "no external scripts, images or fonts", "no clipboard",
        "links do not open", "800px",
        # What it keeps: Nerve holds a widget's localStorage for it, with the
        # chat, so what the person did in a widget is there after a reload
        # and on another device.
        "`localStorage` works", "kept with the chat", "save what the person changes there",
        "ticks", "so a reload keeps it",
        # The theme contract.
        "transparent background", "--color-{text,background,border}-", "light and dark",
        # When to prefer it, and when not.
        "tabs", "sort or filter", "sliders", "what-if", "timeline", "custom drawing",
        "mermaid, vega-lite or svg cannot express", "Do not use one for what plain markdown",
        # It is part of the answer, written in one go: at high effort a model
        # otherwise builds and screenshots it with tools for many minutes first.
        "Write it directly, like a diagram", "no files or tool runs to build or test it",
    ):
        assert words in note, words
    # A widget has storage: the note must not say it has none, nor that the
    # browser keeps it.
    assert "no storage" not in note
    assert "kept with the widget" not in note
    # Short enough to read in every session's prompt.
    assert len(note.split()) <= 210


# --------------------------------------------------------------------------- #
#  build_system_prompt
# --------------------------------------------------------------------------- #


def test_system_prompt_gains_only_the_widget_sentences(tmp_path: Path):
    prompts._PROMPT_TOOL_REGISTRY = None
    for static in (True, False):
        off = build_system_prompt(workspace=tmp_path, source="web", static=static)
        assert off == build_system_prompt(workspace=tmp_path, source="web", static=static, widgets=False)
        on = build_system_prompt(workspace=tmp_path, source="web", static=static, widgets=True)
        assert "```widget" not in off
        assert _rendering_line(off) == RENDERING_LINE_WITHOUT_WIDGETS
        assert _rendering_line(on) == RENDERING_LINE_WITHOUT_WIDGETS + WIDGET_RENDERING_NOTE
        # Nothing else of the prompt changes.
        assert on.replace(WIDGET_RENDERING_NOTE, "") == off


def test_system_prompt_with_widgets_is_static_per_source_and_configuration(tmp_path: Path):
    prompts._PROMPT_TOOL_REGISTRY = None
    one = build_system_prompt(
        workspace=tmp_path, session_id="s-one", source="web", widgets=True,
        recalled_memories=["alpha"],
    )
    two = build_system_prompt(
        workspace=tmp_path, session_id="s-two", source="web", widgets=True,
        recalled_memories=["beta", "gamma"],
    )
    assert one == two
    assert "```widget" in one


def test_widgets_follow_the_session_surface_not_the_turn(tmp_path: Path):
    """A wakeup turn of a web session is read in the web chat; a turn of a
    text channel is not, whatever the instance draws."""
    prompts._PROMPT_TOOL_REGISTRY = None
    wakeup = build_system_prompt(
        workspace=tmp_path, source="wakeup", rendering_source="web", widgets=True,
    )
    assert "```widget" in _rendering_line(wakeup)
    for source in TEXT_ONLY:
        prompt = build_system_prompt(workspace=tmp_path, source=source, widgets=True)
        assert "widget" not in _rendering_line(prompt), source
        assert prompt == build_system_prompt(workspace=tmp_path, source=source), source


# --------------------------------------------------------------------------- #
#  The engine passes the fact on
# --------------------------------------------------------------------------- #


class _Client:
    """A runtime stand-in: each turn answers one text block and a result."""

    model = "claude-test"
    native_session_id = "native-0"

    async def connect(self) -> None:
        pass

    async def start_turn(self, turn) -> None:
        pass

    async def receive_turn(self):
        for message in (
            AssistantMessage(content=[TextBlock(text="ok")], model="claude-test"),
            ResultMessage(
                subtype="success", duration_ms=1, duration_api_ms=1, is_error=False,
                num_turns=1, session_id="native-0", total_cost_usd=0.0,
                usage={"input_tokens": 1},
            ),
        ):
            for event in translate_message(message):
                yield event

    async def interrupt(self) -> None:
        pass

    def is_alive(self) -> bool:
        return True

    async def disconnect(self, timeout: float = 5.0) -> None:
        pass

    def try_receive_idle_events(self):
        return None

    def buffer_used(self) -> int:
        return 0


class _Backend:
    """Records the system prompt of every client the engine builds."""

    name = "claude"
    capabilities = BackendCapabilities(supports_idle_stream=False, supports_cache_ttl=False)

    def __init__(self) -> None:
        self.system_prompts: list[str] = []

    def default_model(self, source):
        return "claude-test"

    def excluded_tools(self):
        return set()

    def validate_resume_target(self, native_id, cwd):
        return True

    async def create_client(self, spec):
        self.system_prompts.append(spec.system_prompt)
        return _Client()


def _engine(tmp_path: Path, db, mcp_apps: dict | None) -> tuple[AgentEngine, _Backend]:
    workspace = tmp_path / "ws"
    workspace.mkdir(exist_ok=True)
    config = NerveConfig.from_dict({
        "workspace": str(workspace),
        "gateway": {"port": 8900},
        "agent": {"backend": "claude"},
        **({"mcp_apps": mcp_apps} if mcp_apps is not None else {}),
    })
    engine = AgentEngine(config, db)
    backend = _Backend()
    engine._backends["claude"] = backend
    return engine, backend


async def _prompt(engine: AgentEngine, backend: _Backend, session_id: str, source: str) -> str:
    await engine.run(session_id, "hello", source=source, channel=source, actor=None)
    return backend.system_prompts[-1]


SANDBOX_ON = {"enabled": True}


@pytest.mark.asyncio
async def test_engine_tells_web_sessions_of_widgets_when_the_sandbox_is_active(tmp_path, db):
    engine, backend = _engine(tmp_path, db, SANDBOX_ON)
    assert engine.config.mcp_apps_active is True

    web = await _prompt(engine, backend, "s-web", "web")
    assert _rendering_line(web) == RENDERING_LINE_WITHOUT_WIDGETS + WIDGET_RENDERING_NOTE
    # Static: another web session of the same instance gets the same bytes.
    assert await _prompt(engine, backend, "s-web-2", "web") == web

    telegram = await _prompt(engine, backend, "s-telegram", "telegram")
    assert _rendering_line(telegram) == TEXT_ONLY_LINE


@pytest.mark.asyncio
@pytest.mark.parametrize("mcp_apps", [
    None,
    {},
    {"enabled": False},
])
async def test_engine_says_nothing_of_widgets_when_the_sandbox_is_not_active(tmp_path, db, mcp_apps):
    engine, backend = _engine(tmp_path, db, mcp_apps)
    assert engine.config.mcp_apps_active is False
    web = await _prompt(engine, backend, "s-web", "web")
    assert _rendering_line(web) == RENDERING_LINE_WITHOUT_WIDGETS
    assert "```widget" not in web


@pytest.mark.asyncio
async def test_engine_says_nothing_of_widgets_in_external_mode(tmp_path, db):
    """The sandbox does not run behind the sign-in gateway, whatever the flag says."""
    engine, backend = _engine(tmp_path, db, SANDBOX_ON)
    engine._managed_mcp = True
    web = await _prompt(engine, backend, "s-web", "web")
    assert _rendering_line(web) == RENDERING_LINE_WITHOUT_WIDGETS
    assert "```widget" not in web
