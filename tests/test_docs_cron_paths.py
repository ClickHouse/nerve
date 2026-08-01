"""Guard against docs drifting back to the pre-move cron location.

Cron config moved from the machine-local state dir into the workspace's
git-syncable ``config/cron/`` subtree. The legacy path is still *read* when the
workspace one is absent, so a stale doc doesn't fail loudly — it quietly tells
the reader to author a file that nothing will load. Worse, an explicit
``cron.jobs_file`` in a config example wins outright over the resolver, so a
copied example disables both the workspace location *and* the fallback.

A mention of the legacy path is fine when it is explicitly labelled as the old
location. This test only rejects unqualified ones.
"""

from __future__ import annotations

import re
from pathlib import Path
from textwrap import dedent

import pytest

REPO = Path(__file__).resolve().parent.parent

# Matches the legacy cron dir however it is spelled: `~/.nerve/cron`,
# `$HOME/.nerve/cron`, `/root/.nerve/cron`, and the f-string interpolations
# used in generated agent docs (`{_DOCKER_NERVE_HOME}/cron`,
# `${NERVE_HOME}/cron`) — those render to the same place, and missing them is
# how the Docker TOOLS.md section stayed wrong through the first sweep.
LEGACY = re.compile(
    r"(?:\.nerve/cron"
    r"|\$\{?NERVE_HOME\}?/cron"
    r"|\{_?DOCKER_NERVE_HOME\}/cron)"
)

# Words that mark a mention as "this is the old location" rather than an
# instruction to the reader. The arrow forms cover migration mappings, where
# naming the legacy path *is* the point.
_QUALIFIERS = (
    "legacy", "falls back", "fallback", "un-migrated", "no job files",
    "→ workspace/config/cron", "-> workspace/config/cron",
    "→ `workspace/config/cron", "→ <workspace>/config/cron",
)


# Start of a table row or list item — a scope boundary, because each row or
# item is an independent claim and must carry its own qualifier. Ordered markers
# are matched by *shape* (``1.``, ``2.``, ``10.``, ``1)``) rather than by the
# literal ``"1. "``: a numbered migration list whose legacy path sits under item
# ``2.`` would otherwise be scoped to the whole list and inherit the "legacy"
# from item ``1.`` — a silent free pass, the one direction a guard must not fail
# in. Leading whitespace is stripped before matching, so nested items count too.
# A marker must be followed by space or end-of-line, which is what keeps `---`
# rules and `**bold**` lead-ins from reading as items and cutting a paragraph in
# half.
_ITEM_START = re.compile(r"(?:\||(?:[-*+]|\d+[.)])(?=[ \t]|$))")


def _scope(lines: list[str], i: int) -> str:
    """The text a qualifier has to appear in to excuse line ``i``.

    A fixed ±N-line window is the obvious choice and it's wrong in both
    directions: too small and it misses a qualifier three lines up in wrapped
    prose, too large and the fallback documentation — exactly where someone
    would add a new cron path — hands out a free pass to its neighbours.

    So: the enclosing paragraph (contiguous non-blank lines), clipped at table
    rows and list items. The clip runs both ways, which is what keeps a wrapped
    item honest: a continuation line is scoped to *its own* item (back to the
    marker, forward to the next one) instead of to the whole list, so it can use
    the qualifier its item states on another line but not a sibling's.
    """
    start = i
    while (
        not _ITEM_START.match(lines[start].lstrip())
        and start > 0
        and lines[start - 1].strip()
    ):
        start -= 1
    end = i
    while (
        end + 1 < len(lines)
        and lines[end + 1].strip()
        and not _ITEM_START.match(lines[end + 1].lstrip())
    ):
        end += 1
    return " ".join(lines[start:end + 1])


def _label(path: Path) -> str:
    """Repo-relative so failures are copy-pasteable, absolute otherwise — the
    scope tests below feed in tmp files, which have no repo-relative form."""
    return str(path.relative_to(REPO) if path.is_relative_to(REPO) else path)


def _offenders(paths: list[Path]) -> list[str]:
    out = []
    for path in paths:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeDecodeError):
            continue
        for i, line in enumerate(lines):
            if not LEGACY.search(line):
                continue
            if any(q in _scope(lines, i).lower() for q in _QUALIFIERS):
                continue
            out.append(f"{_label(path)}:{i + 1}: {line.strip()}")
    return out


def _doc_files() -> list[Path]:
    return [
        *sorted((REPO / "docs").rglob("*.md")),
        REPO / "README.md",
        REPO / "config.example.yaml",
    ]


def test_docs_do_not_send_readers_to_the_legacy_cron_dir():
    docs = [p for p in _doc_files() if p.exists()]
    assert len(docs) > 5, "doc set looks wrong — did the layout change?"
    offenders = _offenders(docs)
    assert not offenders, (
        "docs point at the legacy cron location without saying it's the "
        "old one:\n" + "\n".join(offenders)
    )


def test_docs_name_the_current_cron_location():
    """Negative-only checks pass when a doc drops the path entirely, or names
    a third wrong one. Assert the real location is present where it matters."""
    for rel in ("docs/cron.md", "docs/config.md", "docs/migration.md"):
        body = (REPO / rel).read_text(encoding="utf-8")
        assert "config/cron/" in body, f"{rel} never names <workspace>/config/cron/"


def test_python_strings_do_not_send_the_agent_to_the_legacy_cron_dir():
    """Same rule for text the agent reads as fact — prompts, generated docs,
    docstrings. Scans the whole package rather than a hand-picked list, since
    the file that stayed wrong through the first sweep wasn't on that list."""
    sources = [
        p for p in (REPO / "nerve").rglob("*.py")
        # paths.py *defines* the legacy accessor and migrate.py exists to read
        # it; both name the old location as their subject, not as advice.
        if p.name not in {"paths.py", "migrate.py"}
    ]
    sources += sorted((REPO / "nerve" / "templates").rglob("*.md"))
    offenders = _offenders(sources)
    assert not offenders, "\n".join(offenders)


def _scan(tmp_path: Path, body: str) -> list[str]:
    doc = tmp_path / "sample.md"
    doc.write_text(dedent(body).strip("\n") + "\n", encoding="utf-8")
    return _offenders([doc])


# Every marker shape that appears in — or could plausibly land in — these docs.
# Only `1. ` used to count, so `2.`, `10.` and friends fell back to paragraph
# scope and borrowed the previous item's qualifier.
@pytest.mark.parametrize(
    "marker", ["2.", "10.", "2)", "-", "*", "+", "   2.", "   -"]
)
def test_a_qualifier_does_not_leak_from_one_item_to_the_next(tmp_path, marker):
    offenders = _scan(tmp_path, f"""
        Steps:

        1. Old installs kept their jobs in the legacy `~/.nerve/cron/`.
        {marker} Author new jobs in `~/.nerve/cron/jobs.yaml`.
    """)
    assert len(offenders) == 1, f"expected only the {marker!r} item: {offenders}"
    assert "jobs.yaml" in offenders[0]


def test_a_qualifier_does_not_leak_between_table_rows(tmp_path):
    offenders = _scan(tmp_path, """
        | Key | Default |
        |-----|---------|
        | `cron.system_file` | `<workspace>/config/cron/system.yaml` (falls back to `~/.nerve/cron/system.yaml`) |
        | `cron.jobs_file` | `~/.nerve/cron/jobs.yaml` |
    """)
    assert len(offenders) == 1
    assert "jobs_file" in offenders[0]


def test_a_wrapped_item_keeps_the_qualifier_it_states_on_another_line(tmp_path):
    """The clip is not "one line per item": an item that wraps is still one
    claim, so its own qualifier must count wherever inside it it lands."""
    assert not _scan(tmp_path, """
        1. Jobs used to live in `~/.nerve/cron/jobs.yaml`, which Nerve now reads
           only as a fallback.
        2. Move them into `<workspace>/config/cron/`.
    """)
    assert not _scan(tmp_path, """
        1. Nerve falls back to
           `~/.nerve/cron/jobs.yaml` when the workspace copy has no jobs.
        2. Move them into `<workspace>/config/cron/`.
    """)


def test_a_wrapped_item_cannot_borrow_a_siblings_qualifier(tmp_path):
    offenders = _scan(tmp_path, """
        1. Old installs kept their jobs in the legacy location.
        2. Author new jobs in
           `~/.nerve/cron/jobs.yaml`.
    """)
    assert len(offenders) == 1
    assert "jobs.yaml" in offenders[0]


def test_migration_guidance_still_passes(tmp_path):
    """The docs are *supposed* to name the old location when explaining the
    fallback. Over-eagerness would push authors into dropping that guidance."""
    assert not _scan(tmp_path, """
        > Nerve still reads the legacy `~/.nerve/cron/` when the workspace
        > location has no job files, so an older guide that put them there is
        > not broken. But once you create `$NERVE_WS/config/cron/jobs.yaml`,
        > the fallback stops firing — don't split jobs across both.
    """)
