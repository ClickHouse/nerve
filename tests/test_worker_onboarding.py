"""The worker's autonomous onboarding prompt, and the worker guide it carries.

A worker that ``nerve init`` set up with a task description sets itself up in
one session at the next start; its prompt carries the worker guide in full,
read from the checkout's ``docs/`` with a packaged copy as the fallback for
wheel installs, and the two copies must not drift. The onboarding state and
the conversation are tested in ``tests/test_onboarding.py``.
"""

from __future__ import annotations

from pathlib import Path

from nerve import workspace as workspace_module
from nerve.agent.prompts import WORKER_GUIDE_URL, worker_onboarding_prompt
from nerve.workspace import read_worker_guide, worker_guide_paths

REPO = Path(__file__).resolve().parent.parent
SOURCE_GUIDE = REPO / "docs" / "worker-best-practices.md"
PACKAGED_GUIDE = REPO / "nerve" / "templates" / "guides" / "worker-best-practices.md"
DESCRIPTION = "Watch the nightly build for example/repo and propose fixes before changing anything."


def _section_headings(text: str) -> list[str]:
    return [line for line in text.splitlines() if line.startswith("## ")]


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
            "### 7. Mark onboarding done, last",
            "`onboarding_done`",
            "<!-- nerve:onboarding pending",
            "`backlog`",
            "`plan_review`",
        ):
            assert needle in prompt, needle
        assert prompt.index("### 7. Mark onboarding done, last") > prompt.index(
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
            "A planner prompt sends no `notify`",
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
        # The final message comes after onboarding is marked done, the last change.
        assert "after step 7" in step_six

    def test_multi_line_descriptions_are_quoted_line_by_line(self) -> None:
        prompt = worker_onboarding_prompt("First line.\n\nSecond paragraph.", "guide text")
        assert "> First line.\n>\n> Second paragraph." in prompt

    def test_without_a_guide_the_prompt_points_at_the_published_copy(self) -> None:
        prompt = worker_onboarding_prompt(DESCRIPTION, "")
        assert WORKER_GUIDE_URL in prompt
        assert "## Steps" in prompt
