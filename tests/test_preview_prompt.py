"""The preview sentences of the system prompt's "Rendering" line.

With ``artifacts.preview`` the web chat shows a sent ``.html``, ``.md`` or
``.csv`` file as a page under its card, and opens it in the Preview tab. The
model hears of it only where it shows: on a source that renders, of an
instance with the Preview on, in local and in external mode. Everywhere else
the line says nothing of it, and it depends on the source and the
configuration only, so the prompt stays static (the prompt cache is
exact-prefix).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from nerve.agent import prompts
from nerve.agent.prompts import (
    PREVIEW_RENDERING_NOTE,
    RICH_RENDERING_NOTE,
    TEXT_RENDERING_NOTE,
    WIDGET_RENDERING_NOTE,
    build_system_prompt,
    rendering_note,
)
from nerve.agent.tools.handlers.notifications import SEND_FILE_SPEC
from nerve.agent.engine import AgentEngine
from nerve.config import NerveConfig
from tests.test_widget_prompt import (
    RENDERING,
    RENDERING_LINE_WITHOUT_WIDGETS,
    TEXT_ONLY,
    TEXT_ONLY_LINE,
    _Backend,
    _prompt,
    _rendering_line,
)


def test_with_previews_a_rendering_source_gets_the_preview_sentences():
    for source in RENDERING:
        assert rendering_note(source, previews=True) == RICH_RENDERING_NOTE + PREVIEW_RENDERING_NOTE
        assert rendering_note(source) == RICH_RENDERING_NOTE


def test_previews_come_after_widgets():
    assert rendering_note("web", widgets=True, previews=True) == (
        RICH_RENDERING_NOTE + WIDGET_RENDERING_NOTE + PREVIEW_RENDERING_NOTE
    )


def test_a_text_channel_never_hears_of_previews():
    for source in TEXT_ONLY:
        assert rendering_note(source, previews=True) == TEXT_RENDERING_NOTE


def test_the_preview_sentences_say_how_to_show_a_page():
    for words in (
        "`send_file`", "`.html`", "`.md`", "`.csv`", "search and sort", "`index.html`",
        "Preview tab", "does not need to be published", "`.uploads/`",
        "block as code", "alt text", "inline `<svg>`",
    ):
        assert words in PREVIEW_RENDERING_NOTE, words


def test_send_file_points_to_the_rendering_line_for_pages():
    description = SEND_FILE_SPEC.description
    assert "Rendering line" in description
    assert ".html, .md or .csv" in description


def test_system_prompt_gains_only_the_preview_sentences(tmp_path: Path):
    prompts._PROMPT_TOOL_REGISTRY = None
    for static in (True, False):
        off = build_system_prompt(workspace=tmp_path, source="web", static=static)
        on = build_system_prompt(workspace=tmp_path, source="web", static=static, previews=True)
        assert _rendering_line(off) == RENDERING_LINE_WITHOUT_WIDGETS
        assert _rendering_line(on) == RENDERING_LINE_WITHOUT_WIDGETS + PREVIEW_RENDERING_NOTE
        assert on.replace(PREVIEW_RENDERING_NOTE, "") == off


def test_previews_follow_the_session_surface_not_the_turn(tmp_path: Path):
    prompts._PROMPT_TOOL_REGISTRY = None
    wakeup = build_system_prompt(
        workspace=tmp_path, source="wakeup", rendering_source="web", previews=True,
    )
    assert PREVIEW_RENDERING_NOTE in _rendering_line(wakeup)
    for source in TEXT_ONLY:
        prompt = build_system_prompt(workspace=tmp_path, source=source, previews=True)
        assert prompt == build_system_prompt(workspace=tmp_path, source=source), source


# --------------------------------------------------------------------------- #
#  The engine passes the fact on
# --------------------------------------------------------------------------- #


def _engine(tmp_path: Path, db, artifacts: dict | None) -> tuple[AgentEngine, _Backend]:
    workspace = tmp_path / "ws"
    workspace.mkdir(exist_ok=True)
    config = NerveConfig.from_dict({
        "workspace": str(workspace),
        "gateway": {"port": 8900},
        "agent": {"backend": "claude"},
        **({"artifacts": artifacts} if artifacts is not None else {}),
    })
    engine = AgentEngine(config, db)
    backend = _Backend()
    engine._backends["claude"] = backend
    return engine, backend


@pytest.mark.asyncio
async def test_engine_tells_web_sessions_of_previews_when_the_preview_is_on(tmp_path, db):
    engine, backend = _engine(tmp_path, db, {"preview": True})
    web = await _prompt(engine, backend, "s-web", "web")
    assert _rendering_line(web) == RENDERING_LINE_WITHOUT_WIDGETS + PREVIEW_RENDERING_NOTE
    telegram = await _prompt(engine, backend, "s-telegram", "telegram")
    assert _rendering_line(telegram) == TEXT_ONLY_LINE


@pytest.mark.asyncio
async def test_engine_tells_of_previews_in_external_mode(tmp_path, db):
    """The Preview works behind the sign-in gateway too, unlike widgets."""
    engine, backend = _engine(tmp_path, db, {"preview": True})
    engine._managed_mcp = True
    web = await _prompt(engine, backend, "s-web", "web")
    assert _rendering_line(web) == RENDERING_LINE_WITHOUT_WIDGETS + PREVIEW_RENDERING_NOTE


@pytest.mark.asyncio
@pytest.mark.parametrize("artifacts", [None, {}, {"preview": False}])
async def test_engine_says_nothing_of_previews_when_the_preview_is_off(tmp_path, db, artifacts):
    engine, backend = _engine(tmp_path, db, artifacts)
    web = await _prompt(engine, backend, "s-web", "web")
    assert _rendering_line(web) == RENDERING_LINE_WITHOUT_WIDGETS
    assert PREVIEW_RENDERING_NOTE not in web
