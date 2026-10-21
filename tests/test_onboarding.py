"""The onboarding conversation: its prompt, its state, its start and its end.

After setup the agent is set up by talking to it. The conversation is one web
session with a hidden first message; the agent ends it with
``onboarding_done``, and a person can skip it. Installs from before keep their
worker rule: no ``TASK.md`` marker means done.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from nerve import onboarding
from nerve.agent.prompts import ONBOARDING_OFFERS, WORKER_GUIDE_URL, onboarding_prompt
from nerve.agent.tools.handlers.onboarding import onboarding_done_handler
from nerve.agent.tools.registry import ToolContext
from nerve.setup_writer import (
    TASK_ONBOARDING_MARKER,
    apply_mode,
    list_optional_crons,
    plan_optional_crons,
    render_task_md,
    system_cron_file,
)
from tests.actor_rows import ensure_system_principal

DESCRIPTION = "Watch the nightly build for example/repo and propose fixes."
PERSONAL_ITEM = ONBOARDING_OFFERS[-1]


@pytest_asyncio.fixture
async def db(db):  # noqa: F811 - the conftest database, with an identity
    await ensure_system_principal(db)
    return db


def _engine(db, *, outcome: str = "completed", slow: bool = False) -> MagicMock:
    """Real session rows; turns that only record their arguments and end with ``outcome``."""
    engine = MagicMock()
    engine.db = db
    engine.config.credential_available.return_value = True
    turn = SimpleNamespace(outcome=None)

    async def _run(**kwargs):
        turn.outcome = SimpleNamespace(status=outcome)
        return ""

    engine.run = AsyncMock(side_effect=_run)

    @contextmanager
    def _watch_turns(session_id):
        yield turn

    engine.watch_turns = _watch_turns

    async def _get_or_create(session_id, title=None, source="web", *, actor, cause=None):
        if slow:
            await asyncio.sleep(0.05)
        await db.create_session(session_id, title=title, source=source, actor=actor, cause=cause)
        return await db.get_session(session_id)

    engine.sessions.get_or_create = _get_or_create
    engine.register_task = lambda session_id, task: None
    return engine


async def _first_turn(engine) -> dict:
    for _ in range(20):
        if engine.run.await_count:
            break
        await asyncio.sleep(0)
    return engine.run.await_args.kwargs


class TestPrompt:
    @pytest.mark.parametrize("mode", ["personal", "worker"])
    def test_offers_the_whole_list_and_the_setup_rules(self, mode):
        prompt = onboarding_prompt(mode, DESCRIPTION, "# Worker Best Practices")
        for item in ONBOARDING_OFFERS:
            assert f"- {item}" in prompt
        for part in (
            "## Your first message", "you speak first", "which repositories to onboard",
            "Change nothing and start no real work before the person confirms",
            "`backlog`", "`plan_review`", "`task-planner`", "`task_status_create`",
            "`skill_create`", "`memorize`", "config/cron/jobs.yaml", "run_if",
            "### 4. Summarize", "`onboarding_done`",
        ):
            assert part in prompt, part

    def test_a_personal_install_leads_with_the_personal_assistant(self):
        lines = [line for line in onboarding_prompt("personal").splitlines() if line.startswith("- ")]
        assert lines[0] == f"- {PERSONAL_ITEM}"
        assert "Worker Best Practices" not in onboarding_prompt("personal")
        assert "## TASK.md" not in onboarding_prompt("personal")

    def test_a_worker_gets_its_description_the_task_md_rules_and_the_guide(self):
        prompt = onboarding_prompt("worker", DESCRIPTION + "\nNever merge.", "# Worker Best Practices")
        offers = [line for line in prompt.splitlines() if line.startswith("- ") and line[2:] in ONBOARDING_OFFERS]
        assert offers[-1] == f"- {PERSONAL_ITEM}"
        assert f"> {DESCRIPTION}\n> Never merge." in prompt
        assert "## TASK.md" in prompt and "# Worker Best Practices" in prompt
        assert "> (no description was given at setup)" in onboarding_prompt("worker", "", "x")
        assert WORKER_GUIDE_URL in onboarding_prompt("worker", DESCRIPTION, "")


@pytest.mark.asyncio
class TestState:
    async def test_no_mode_is_not_applicable(self, db, tmp_path):
        assert (await onboarding.read_state(db, tmp_path)).status == "not_applicable"

    async def test_a_fresh_install_is_pending_and_an_onboarded_worker_is_done(self, db, tmp_path):
        apply_mode(tmp_path / "p", "personal")
        assert (await onboarding.read_state(db, tmp_path / "p")).status == "pending"
        ws = tmp_path / "w"
        apply_mode(ws, "worker", DESCRIPTION)
        assert (await onboarding.read_state(db, ws)).status == "pending"
        # A worker onboarded before the onboarding state existed: the marker is gone.
        task = ws / "TASK.md"
        task.write_text("".join(
            line for line in task.read_text().splitlines(keepends=True)
            if TASK_ONBOARDING_MARKER not in line
        ))
        assert (await onboarding.read_state(db, ws)).status == "done"

    async def test_running_needs_its_session(self, db, tmp_path):
        apply_mode(tmp_path, "personal")
        engine = _engine(db)
        state = await onboarding.start(engine, tmp_path, actor=None, cause=None)
        assert state.status == "running"
        await db.delete_session(state.session_id)
        # The session is gone: pending again, and the next start opens a new one.
        assert (await onboarding.read_state(db, tmp_path)).status == "pending"
        again = await onboarding.start(engine, tmp_path, actor=None, cause=None)
        assert again.status == "running" and again.session_id != state.session_id


@pytest.mark.asyncio
class TestStart:
    async def test_opens_one_web_session_with_a_hidden_first_turn(self, db, tmp_path):
        apply_mode(tmp_path, "personal")
        engine = _engine(db)
        states = await asyncio.gather(*(
            onboarding.start(engine, tmp_path, actor=None, cause=None) for _ in range(3)
        ))
        assert {s.session_id for s in states} == {states[0].session_id}
        turn = await _first_turn(engine)
        assert engine.run.await_count == 1
        assert turn["internal"] is True and turn["source"] == "web"
        assert turn["actor"] == db.system_actor
        assert turn["user_message"] == onboarding_prompt("personal")
        assert (await db.get_session(states[0].session_id))["title"] == "Onboarding"

    async def test_a_start_during_another_start_sees_its_session(self, db, tmp_path):
        apply_mode(tmp_path, "personal")
        engine = _engine(db, slow=True)
        first, second = await asyncio.gather(
            onboarding.start(engine, tmp_path, actor=None, cause=None),
            onboarding.start(engine, tmp_path, actor=None, cause=None),
        )
        assert first.session_id == second.session_id
        await _first_turn(engine)
        assert engine.run.await_count == 1

    async def test_a_failed_first_turn_asks_again(self, db, tmp_path):
        apply_mode(tmp_path, "personal")
        engine = _engine(db, outcome="failed")
        state = await onboarding.start(engine, tmp_path, actor=None, cause=None)
        await _first_turn(engine)
        for _ in range(20):
            if (await db.get_onboarding())["status"] != "running":
                break
            await asyncio.sleep(0)
        assert (await db.get_onboarding())["status"] == "requested"
        assert (await onboarding.read_state(db, tmp_path)).status == "pending"
        # The next start opens a new conversation.
        again = await onboarding.start(_engine(db), tmp_path, actor=None, cause=None)
        assert again.status == "running" and again.session_id != state.session_id

    async def test_a_worker_without_task_md_gets_the_template_first(self, db, tmp_path):
        apply_mode(tmp_path, "worker")
        (tmp_path / "TASK.md").unlink()
        await onboarding.start(_engine(db), tmp_path, actor=None, cause=None)
        assert (tmp_path / "TASK.md").read_text() == render_task_md("")

    async def test_a_legacy_raw_task_md_gets_the_template_shape(self, db, tmp_path):
        apply_mode(tmp_path, "worker")
        (tmp_path / "TASK.md").write_text("# Task\n\nKeep the build green.\n")
        engine = _engine(db)
        await onboarding.start(engine, tmp_path, actor=None, cause=None)
        assert (tmp_path / "TASK.md").read_text() == render_task_md("Keep the build green.")
        assert "> Keep the build green." in (await _first_turn(engine))["user_message"]


@pytest.mark.asyncio
class TestBoot:
    async def test_a_requested_conversation_opens_at_start_for_its_person(self, db, tmp_path):
        apply_mode(tmp_path, "personal")
        person = db.system_actor.actor_id
        await onboarding.request(db, tmp_path, actor_id=person)
        state = await onboarding.start_on_boot(_engine(db), tmp_path)
        assert state.status == "running"
        assert (await db.get_session(state.session_id))["caused_by_actor_id"] == person

    async def test_a_pending_worker_from_before_opens_at_start_for_the_system(self, db, tmp_path):
        apply_mode(tmp_path, "worker", DESCRIPTION)
        state = await onboarding.start_on_boot(_engine(db), tmp_path)
        assert state is not None and state.status == "running"
        session = await db.get_session(state.session_id)
        assert session["caused_by_actor_id"] == db.system_actor.actor_id

    async def test_nothing_opens_at_start_without_a_model(self, db, tmp_path):
        apply_mode(tmp_path, "personal")
        await onboarding.request(db, tmp_path, actor_id=None)
        engine = _engine(db)
        engine.config.credential_available.return_value = False
        assert await onboarding.start_on_boot(engine, tmp_path) is None
        assert (await db.get_onboarding())["status"] == "requested"

    async def test_nothing_opens_without_a_request(self, db, tmp_path):
        apply_mode(tmp_path, "personal")
        assert await onboarding.start_on_boot(_engine(db), tmp_path) is None
        await onboarding.request(db, tmp_path, actor_id=None)
        await onboarding.skip(db, tmp_path, actor_id=None)
        assert await onboarding.start_on_boot(_engine(db), tmp_path) is None


@pytest.mark.asyncio
class TestEnd:
    async def test_the_tool_marks_it_done_from_its_own_session_only(self, db, tmp_path):
        apply_mode(tmp_path, "worker", DESCRIPTION)
        state = await onboarding.start(_engine(db), tmp_path, actor=None, cause=None)

        def ctx(session_id):
            return ToolContext(session_id=session_id, workspace=tmp_path, db=db, actor=db.system_actor)

        other = await onboarding_done_handler(ctx("sess-other"), {})
        assert other.is_error and (await onboarding.read_state(db, tmp_path)).status == "running"
        done = await onboarding_done_handler(ctx(state.session_id), {})
        assert not done.is_error
        assert (await onboarding.read_state(db, tmp_path)).status == "done"
        assert TASK_ONBOARDING_MARKER not in (tmp_path / "TASK.md").read_text()
        assert (await onboarding_done_handler(ctx(state.session_id), {})).is_error

    async def test_skip_keeps_the_session_and_ends_the_reminders(self, db, tmp_path):
        apply_mode(tmp_path, "personal")
        state = await onboarding.start(_engine(db), tmp_path, actor=None, cause=None)
        skipped = await onboarding.skip(db, tmp_path, actor_id=db.system_actor.actor_id)
        assert skipped.status == "skipped" and skipped.session_id == state.session_id
        assert await db.get_session(state.session_id) is not None
        # Done stays done.
        assert (await onboarding.skip(db, tmp_path, actor_id=None)).status == "skipped"


class TestEffectiveCronState:
    def _override(self, ws: Path, text: str) -> None:
        (system_cron_file(ws).parent / "jobs.yaml").write_text(text)

    def test_a_jobs_yaml_job_replaces_the_built_in_one(self, tmp_path):
        apply_mode(tmp_path, "worker", DESCRIPTION)
        self._override(tmp_path, (
            "jobs:\n  - id: task-planner\n    schedule: '0 */4 * * *'\n"
            "    prompt: Plan one task tagged source:ci.\n"
        ))
        crons = {c.id: (c.enabled, c.overridden) for c in list_optional_crons(tmp_path)}
        assert crons["task-planner"] == (True, True)
        assert crons["skill-extractor"] == (True, False)

    def test_a_job_whose_gate_does_not_build_replaces_nothing(self, tmp_path):
        apply_mode(tmp_path, "worker", DESCRIPTION)
        self._override(tmp_path, (
            "jobs:\n  - id: task-planner\n    schedule: '0 */4 * * *'\n"
            "    prompt: Plan one task.\n    run_if:\n      - type: no-such-gate\n"
        ))
        crons = {c.id: (c.enabled, c.overridden) for c in list_optional_crons(tmp_path)}
        assert crons["task-planner"] == (False, False)

    def test_the_automation_step_leaves_an_overridden_job_alone(self, tmp_path):
        apply_mode(tmp_path, "worker", DESCRIPTION)
        self._override(tmp_path, (
            "jobs:\n  - id: skill-reviser\n    schedule: '0 3 * * 0'\n"
            "    prompt: Review the skills.\n    enabled: false\n"
        ))
        before = system_cron_file(tmp_path).read_text()
        plan = plan_optional_crons(tmp_path, {"skill-extractor"})
        assert "skill-reviser" not in plan.disabled and "skill-reviser" not in plan.enabled
        assert plan.text is None and system_cron_file(tmp_path).read_text() == before
