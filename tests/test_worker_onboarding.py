"""The worker's first-boot onboarding: what starts it and what it is told.

"Pending" is the marker comment on line 2 of ``nerve/templates/worker/TASK.md``,
which the onboarding session deletes last. The session's prompt carries the
worker guide in full, read from the checkout's ``docs/`` with a packaged copy
as the fallback for wheel installs — and the two copies must not drift.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from nerve import workspace as workspace_module
from nerve.agent.engine import AgentEngine
from nerve.agent.prompts import WORKER_GUIDE_URL, worker_onboarding_prompt
from nerve.setup_writer import TASK_ONBOARDING_MARKER, apply_mode, render_task_md
from nerve.workspace import read_worker_guide, worker_guide_paths

REPO = Path(__file__).resolve().parent.parent
SOURCE_GUIDE = REPO / "docs" / "worker-best-practices.md"
PACKAGED_GUIDE = REPO / "nerve" / "templates" / "guides" / "worker-best-practices.md"
DESCRIPTION = "Watch the nightly build for example/repo and propose fixes before changing anything."
MISSION_PLACEHOLDER = "_(onboarding fills this in: one paragraph ending in the quality bar)_"


def _section_headings(text: str) -> list[str]:
    return [line for line in text.splitlines() if line.startswith("## ")]


def _bare_engine(ws: Path) -> AgentEngine:
    """An engine with only what the onboarding methods touch."""
    engine = AgentEngine.__new__(AgentEngine)
    engine.config = SimpleNamespace(workspace=ws, memory=SimpleNamespace(backend="memu"))
    return engine


class TestGuideCopy:
    def test_packaged_copy_is_byte_identical_to_the_source(self) -> None:
        assert SOURCE_GUIDE.read_bytes() == PACKAGED_GUIDE.read_bytes(), (
            "docs/worker-best-practices.md and nerve/templates/guides/worker-best-practices.md "
            "differ. The docs file is the source: copy it over the packaged one."
        )

    def test_the_source_copy_is_read_first(self) -> None:
        source, packaged = worker_guide_paths()
        assert (source, packaged) == (SOURCE_GUIDE, PACKAGED_GUIDE)
        assert read_worker_guide() == SOURCE_GUIDE.read_text(encoding="utf-8")

    def test_falls_back_to_the_packaged_copy(self, tmp_path: Path, monkeypatch) -> None:
        """A wheel install has no ``docs/`` beside the package."""
        monkeypatch.setattr(
            workspace_module, "worker_guide_paths",
            lambda: (tmp_path / "docs" / "worker-best-practices.md", PACKAGED_GUIDE),
        )
        assert read_worker_guide() == PACKAGED_GUIDE.read_text(encoding="utf-8")
        monkeypatch.setattr(
            workspace_module, "worker_guide_paths",
            lambda: (tmp_path / "missing-a.md", tmp_path / "missing-b.md"),
        )
        assert read_worker_guide() == ""


class TestPrompt:
    def test_carries_the_description_the_whole_guide_and_the_steps(self) -> None:
        guide = SOURCE_GUIDE.read_text(encoding="utf-8")
        prompt = worker_onboarding_prompt(DESCRIPTION, guide)
        assert prompt.startswith("You are running the **first-boot onboarding** for this Nerve worker")
        assert f"> {DESCRIPTION}" in prompt
        # The guide, in full: its H1, its last section, every section between.
        assert guide.splitlines()[0] == "# Worker Best Practices"
        assert _section_headings(guide)[-1] == "## Further Reading"
        assert "# Worker Best Practices" in prompt and "## Further Reading" in prompt
        assert guide.strip() in prompt, "the guide is injected whole, not summarised"
        # Role line → description → guide → steps, in that order.
        assert (
            prompt.index("## The task, as the operator described it")
            < prompt.index("## The worker guide — apply it")
            < prompt.index("# Worker Best Practices")
            < prompt.index("## Steps")
        )
        for needle in (
            "### 1. Research",
            "### 2. Fill TASK.md in place",
            "Keep every `##` heading",
            "### ⛔ HARD RULE:",
            "[operator directive",
            "[added YYYY-MM-DD]",
            "Keep the closing `Read the full guide:` link",
            "`skill_create`",
            "`<workspace>/config/cron/jobs.yaml`",
            "run_if",
            "`task_create`",
            "`memorize`",
            "`notify`",
            "### 7. Delete the marker — last",
            "<!-- nerve:onboarding pending",
        ):
            assert needle in prompt, needle
        assert prompt.index("### 7. Delete the marker — last") > prompt.index(
            "### 6. Walk through everything you did, then give next steps"
        )

    def test_step_four_decides_on_the_planner(self) -> None:
        """task-planner ships disabled for a worker: the session enables it —
        overridden by id, scoped to the worker's tag — or says why not."""
        steps = worker_onboarding_prompt(DESCRIPTION, "guide text").split("### 4. Configure cron jobs", 1)[1]
        step_four = steps.split("### 5.", 1)[0]
        for needle in (
            "`task-planner` ships disabled",
            "overriding it by id in `jobs.yaml`",
            "`enabled: true`",
            "a `tasks` gate",
            "this worker's `tag`",
            "Leave it off when a work job with a standing grant covers the routine items",
            "`TASK.md` § Approval Required",
        ):
            assert needle in step_four, needle

    def test_step_six_walks_through_every_change_and_ends_with_next_steps(self) -> None:
        prompt = worker_onboarding_prompt(DESCRIPTION, "guide text")
        step_six = prompt.split("### 6. Walk through everything you did, then give next steps", 1)[1]
        step_six = step_six.split("### 7.", 1)[0]
        assert "Next steps" in prompt
        for needle in (
            "`memorize`",
            "`notify` (priority `high`)",
            "final message",
            "the `TASK.md` sections you filled",
            "the statuses you created",
            "the skills you created, by name",
            "id, schedule, gate, session mode",
            "whether it is enabled",
            "their ids and what the operator must do",
            "what you memorized",
            "**Next steps for the operator**",
            "credentials and access to grant",
            "plans to approve",
            "jobs to review or enable",
            "`nerve cron <id>`",
            "what to watch during the first runs",
        ):
            assert needle in step_six, needle
        # The final message comes after the marker is gone; the marker stays last among the changes.
        assert "after step 7" in step_six

    def test_multi_line_descriptions_are_quoted_line_by_line(self) -> None:
        prompt = worker_onboarding_prompt("First line.\n\nSecond paragraph.", "guide text")
        assert "> First line.\n>\n> Second paragraph." in prompt

    def test_without_a_guide_the_prompt_points_at_the_published_copy(self) -> None:
        prompt = worker_onboarding_prompt(DESCRIPTION, "")
        assert WORKER_GUIDE_URL in prompt
        assert "## Steps" in prompt


class TestEngine:
    def test_needs_onboarding_keys_on_the_marker(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        engine = _bare_engine(ws)
        assert engine._needs_worker_onboarding() is False, "no TASK.md: not a worker"
        apply_mode(ws, "worker", DESCRIPTION)
        task = ws / "TASK.md"
        assert engine._needs_worker_onboarding() is True
        text = task.read_text(encoding="utf-8")
        marker_line = text.splitlines()[1]
        assert marker_line.startswith(TASK_ONBOARDING_MARKER)
        # Sections filled but the marker left standing: still due.
        task.write_text(text.replace(MISSION_PLACEHOLDER, "Keep CI green."), encoding="utf-8")
        assert engine._needs_worker_onboarding() is True
        # Marker deleted: done.
        task.write_text(text.replace(marker_line + "\n", "", 1), encoding="utf-8")
        assert engine._needs_worker_onboarding() is False
        # Files from older versions: raw until ``## Mission`` appears.
        task.write_text("# Task\n\nKeep the build green.\n", encoding="utf-8")
        assert engine._needs_worker_onboarding() is True
        task.write_text("# Task\n\n## Mission\nKeep the build green.\n", encoding="utf-8")
        assert engine._needs_worker_onboarding() is False

    @pytest.mark.asyncio
    async def test_run_hands_the_full_prompt_to_the_onboarding_cron(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        apply_mode(ws, "worker", DESCRIPTION)
        engine = _bare_engine(ws)
        engine.run_cron = AsyncMock(return_value="done")
        await engine._run_worker_onboarding()
        assert engine.run_cron.await_count == 1
        kwargs = engine.run_cron.await_args.kwargs
        assert kwargs["job_id"] == "worker-onboarding"
        prompt = kwargs["prompt"]
        assert prompt == worker_onboarding_prompt(DESCRIPTION, SOURCE_GUIDE.read_text(encoding="utf-8"))
        assert "# Worker Best Practices" in prompt and "## Further Reading" in prompt
        # The engine leaves TASK.md to the session.
        assert (ws / "TASK.md").read_text(encoding="utf-8") == render_task_md(DESCRIPTION)

    @pytest.mark.asyncio
    async def test_a_legacy_raw_task_md_is_given_the_template_shape_first(self, tmp_path: Path) -> None:
        """An older install's ``# Task`` + description: the session is told to
        fill the template in place, so it gets the template, text kept."""
        ws = tmp_path / "ws"
        ws.mkdir()
        (ws / "TASK.md").write_text("# Task\n\nKeep the build green.\n", encoding="utf-8")
        engine = _bare_engine(ws)
        engine.run_cron = AsyncMock(return_value="done")
        await engine._run_worker_onboarding()
        assert (ws / "TASK.md").read_text(encoding="utf-8") == render_task_md("Keep the build green.")
        assert "> Keep the build green." in engine.run_cron.await_args.kwargs["prompt"]
        assert engine._needs_worker_onboarding() is True, "pending until the session deletes the marker"

    @pytest.mark.asyncio
    async def test_a_completed_task_md_starts_no_session(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws"
        ws.mkdir()
        (ws / "TASK.md").write_text("# Task\n\n## Mission\nKeep the build green.\n", encoding="utf-8")
        engine = _bare_engine(ws)
        engine.run_cron = AsyncMock(return_value="done")
        await engine._run_worker_onboarding()
        assert engine.run_cron.await_count == 0
