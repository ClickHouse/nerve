"""The skills Nerve ships in ``nerve/templates/skills/``.

:func:`nerve.workspace.install_bundled_skills` copies each of them into a new
workspace, so each must parse, say when to use it, and suit every memory
backend.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from nerve.skills.manager import _parse_skill_md

SKILLS = Path(__file__).resolve().parents[1] / "nerve" / "templates" / "skills"
BUNDLED = sorted(p.name for p in SKILLS.iterdir() if (p / "SKILL.md").is_file())

# The general skills every new workspace starts with, next to nerve-dev and
# nerve-workspace.
GENERAL = (
    "browser-testing",
    "diagrams-and-charts",
    "research-report",
    "shareable-html",
    "slide-deck",
)

# Nerve's memory tools, as a skill would name them (tests/test_loom_prompts.py).
MEMU_TOOLS = re.compile(
    r"`(memorize|memory_recall|memory_expand_category|session_context|conversation_history"
    r"|memory_records_by_date|memory_update|memory_delete|category_update)`"
    r"|\b(memorize|memory_recall|memory_records_by_date|memory_update|conversation_history)\("
)


def test_the_general_skills_ship() -> None:
    assert set(GENERAL) <= set(BUNDLED)


@pytest.mark.parametrize("skill", BUNDLED)
def test_the_frontmatter_names_and_describes_the_skill(skill: str) -> None:
    fm, body = _parse_skill_md((SKILLS / skill / "SKILL.md").read_text(encoding="utf-8"))
    assert isinstance(fm.get("name"), str) and 0 < len(fm["name"]) <= 64
    description = fm.get("description")
    assert isinstance(description, str) and 0 < len(description.strip()) <= 1024
    assert str(fm.get("version", "")).count(".") == 2
    assert body.startswith("# ")


@pytest.mark.parametrize("skill", GENERAL)
def test_a_general_skill_says_when_to_use_it(skill: str) -> None:
    fm, _ = _parse_skill_md((SKILLS / skill / "SKILL.md").read_text(encoding="utf-8"))
    assert fm["description"].startswith("This skill should be used when")


@pytest.mark.parametrize("skill", GENERAL)
def test_a_general_skill_names_no_memory_tool(skill: str) -> None:
    """Setup copies it unchanged under every memory backend, so it names none
    of them; nerve-dev and nerve-workspace get Loom's names through
    nerve.memory.loom_prompts."""
    found = MEMU_TOOLS.search((SKILLS / skill / "SKILL.md").read_text(encoding="utf-8"))
    assert not found, found.group(0)
