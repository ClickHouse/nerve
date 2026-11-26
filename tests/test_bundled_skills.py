"""The skills Nerve ships in ``nerve/templates/skills/``.

:func:`nerve.workspace.install_bundled_skills` copies each of them into a new
workspace. A file that a SKILL.md names must ship with it, a file that ships
must be named, and the text must suit every memory backend.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from nerve.skills.manager import _parse_skill_md
from nerve.workspace import install_bundled_skills

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

# A file of a skill as a SKILL.md names it: `scripts/x.py` for its own, or a
# path under `skills/<id>/` for its own or another bundled skill's.
_PART = r"((?:scripts|assets|references)/[\w.-]+(?:/[\w.-]+)*)"
OWN_FILE = re.compile(r"`" + _PART)
SKILL_FILE = re.compile(r"skills/([\w-]+)/" + _PART)


def _files(skill: str) -> list[str]:
    root = SKILLS / skill
    return sorted(
        str(p.relative_to(root)) for p in root.rglob("*")
        if p.is_file() and "__pycache__" not in p.parts and p.name != "SKILL.md"
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
def test_every_file_a_skill_names_ships_with_it(skill: str) -> None:
    """Only the general skills: nerve-dev and nerve-workspace name a
    repository's own `scripts/`."""
    text = (SKILLS / skill / "SKILL.md").read_text(encoding="utf-8")
    named = {(skill, m.rstrip(".")) for m in OWN_FILE.findall(text)}
    named |= {(owner, m.rstrip(".")) for owner, m in SKILL_FILE.findall(text)}
    missing = sorted(f"{owner}/{rel}" for owner, rel in named if rel not in _files(owner))
    assert not missing, missing


@pytest.mark.parametrize("skill", BUNDLED)
def test_every_file_that_ships_is_named_in_the_skill(skill: str) -> None:
    text = (SKILLS / skill / "SKILL.md").read_text(encoding="utf-8")
    unnamed = [f for f in _files(skill) if f not in text]
    assert not unnamed, unnamed


def test_a_new_workspace_gets_every_file_of_every_skill(tmp_path: Path) -> None:
    installed = install_bundled_skills(tmp_path)
    assert installed == BUNDLED
    for skill in BUNDLED:
        for rel in ["SKILL.md", *_files(skill)]:
            copy = tmp_path / "skills" / skill / rel
            assert copy.read_bytes() == (SKILLS / skill / rel).read_bytes(), (skill, rel)


def test_an_existing_skill_is_not_overwritten(tmp_path: Path) -> None:
    mine = tmp_path / "skills" / GENERAL[0] / "SKILL.md"
    mine.parent.mkdir(parents=True)
    mine.write_text("---\nname: Mine\ndescription: my own\n---\n# Mine\n", encoding="utf-8")
    installed = install_bundled_skills(tmp_path)
    assert GENERAL[0] not in installed
    assert mine.read_text(encoding="utf-8").startswith("---\nname: Mine")


@pytest.mark.parametrize("backend", ["memu", "loom"])
def test_a_general_skill_names_no_memory_tool(tmp_path: Path, backend: str) -> None:
    """A general skill names no memory tool at all, so the same text suits
    every memory backend; nerve-dev and nerve-workspace get Loom's names
    through nerve.memory.loom_prompts."""
    install_bundled_skills(tmp_path, memory_backend=backend)
    for path in sorted((tmp_path / "skills").rglob("*")):
        skill = path.relative_to(tmp_path / "skills").parts[0]
        if not path.is_file() or (backend == "memu" and skill not in GENERAL):
            continue
        found = MEMU_TOOLS.search(path.read_text(encoding="utf-8", errors="replace"))
        assert not found, (str(path.relative_to(tmp_path)), found.group(0))
