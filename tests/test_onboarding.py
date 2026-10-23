"""The onboarding: its prompts, its state, its start and its end.

After setup in the browser the agent is set up by talking to it. The
conversation is one web session with a hidden first message; the agent ends it
with ``onboarding_done``, and a person can skip it. A worker that ``nerve
init`` set up with a task description sets itself up once at the next start,
in one session without a person, and ends it with ``onboarding_done`` too.
Installs from before keep their worker rule: no ``TASK.md`` marker means done.
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
from nerve.agent.prompts import (
    ONBOARDING_OFFERS,
    WORKER_GUIDE_URL,
    onboarding_prompt,
    worker_onboarding_prompt,
)
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
from nerve.identity import ACTOR_KIND_HUMAN, Actor
from nerve.workspace import read_worker_guide
from tests.actor_rows import ensure_actor_row, ensure_system_principal

DESCRIPTION = "Watch the nightly build for example/repo and propose fixes."
PERSONAL_ITEM = ONBOARDING_OFFERS[-1]
ALICE = Actor(
    actor_id="11111111-2222-4333-8444-555555555555", kind=ACTOR_KIND_HUMAN, display_name="Alice Example",
)


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


GUIDE = "# Worker Best Practices"
PLAN_HEADINGS = [
    "# Setup proposal — <the work, in one line>",
    "## What I understood", "## What I found", "## TASK.md", "## Jobs",
    "## Statuses and tags", "## Skills", "## What I will remember",
    "## What I need from you", "## Approval boundaries", "## Later, if you want",
]
VARIANTS = [
    pytest.param(mode, description, id=f"{mode}-{'described' if description else 'blank'}")
    for mode in ("personal", "worker") for description in ("", DESCRIPTION)
]


def _between(text: str, start: str, end: str | None = None) -> str:
    """The part of ``text`` from ``start`` up to ``end`` (or the end)."""
    assert start in text, start
    tail = text.split(start, 1)[1]
    if end is None:
        return tail
    assert end in tail, end
    return tail.split(end, 1)[0]


def _first_message(prompt: str) -> str:
    return _between(prompt, "## Your first message", "## Rules")


def _flat(text: str) -> str:
    return " ".join(text.split())


def _describes_work(prompt: str) -> str:
    """The description variant's branch for a note that describes work."""
    return _between(prompt, "**When the note above describes work**", "**When the note names no work**")


def _names_no_work(prompt: str) -> str:
    """The description variant's branch for a note with only rules or targets."""
    return _between(_first_message(prompt), "**When the note names no work**")


def _offered(text: str) -> list[str]:
    return [line[2:] for line in text.splitlines() if line[2:] in ONBOARDING_OFFERS]


def _shared(prompt: str) -> str:
    """Everything after the first message: the same for both of its variants."""
    return _between(prompt, "## Rules")


def _plan_skeleton(prompt: str) -> list[str]:
    return _between(prompt, "```markdown\n", "\n```").splitlines()


class TestPrompt:
    """The hidden first message: the description picks the first message, the
    steps after it are shared."""

    @pytest.mark.parametrize("mode", ["personal", "worker"])
    def test_without_a_description_the_first_message_offers_the_whole_list(self, mode):
        prompt = onboarding_prompt(mode, "", GUIDE)
        first = _first_message(prompt)
        for item in ONBOARDING_OFFERS:
            assert f"\n- {item}\n" in first, item
        for part in (
            "Greet the person without a name. In two or three sentences",
            "say that you are set up by talking to you, now and at any time later",
            "offer this list, as it is, and ask which items apply",
            "Change nothing in this first message.",
        ):
            assert part in first, part
        assert "Restate the work" not in prompt
        assert "you speak first" in prompt

    def test_a_personal_install_leads_with_the_personal_assistant(self):
        lines = [line for line in onboarding_prompt("personal").splitlines() if line.startswith("- ")]
        assert lines[0] == f"- {PERSONAL_ITEM}"
        assert "Worker Best Practices" not in onboarding_prompt("personal")
        assert "## TASK.md" not in onboarding_prompt("personal")
        worker = [
            line for line in onboarding_prompt("worker", "", GUIDE).splitlines()
            if line.startswith("- ") and line[2:] in ONBOARDING_OFFERS
        ]
        assert worker == [f"- {item}" for item in ONBOARDING_OFFERS]

    @pytest.mark.parametrize("mode", ["personal", "worker"])
    def test_a_note_that_describes_work_is_explained_before_anything_is_asked(self, mode):
        """No menu, and the text comes first: greeting, the work in one
        sentence, what the explorer studies, the intended setup and why the
        questions matter; then the explorer and the question card."""
        prompt = onboarding_prompt(mode, DESCRIPTION, GUIDE)
        work = _flat(_describes_work(prompt))
        assert f"> {DESCRIPTION}" in _between(prompt, "wrote at setup", "## Your first message")
        assert not _offered(_describes_work(prompt))
        assert "offer no menu" in work and "set up by talking to you" not in work
        steps = [
            "Write the message text first, before any tool call",
            "1. Greet the person without a name.",
            "2. Restate the work as you understand it, in one sentence",
            "3. Say in one line what the explorer will study (step 1 below)",
            "4. In two to four lines, sketch how you intend to set this up",
            "and why the questions that follow matter",
            "Then start the explorer on the targets the note names",
            "ask your first questions with `AskUserQuestion` (step 2 below)",
        ]
        positions = [work.index(step) for step in steps]
        assert positions == sorted(positions)
        assert "Targets the note leaves out you ask for as step 1 says" in work

    @pytest.mark.parametrize("mode", ["personal", "worker"])
    def test_a_note_that_names_no_work_still_gets_the_list(self, mode):
        """Only rules ("never merge anything") or only targets ("org/repo-a"):
        the agent knows no work yet, so it offers the list, in the mode's
        order, and carries the note into the plan."""
        prompt = onboarding_prompt(mode, DESCRIPTION, GUIDE)
        no_work = _names_no_work(prompt)
        assert _offered(no_work) == _offered(_first_message(onboarding_prompt(mode, "", GUIDE)))
        assert len(_offered(no_work)) == len(ONBOARDING_OFFERS)
        for part in (
            'only rules, such as "never merge anything"', 'only targets, such as "org/repo-a"',
            "offer the list. Greet the person without a name, say in one sentence what you noted",
            "say that you are set up by talking to you, now and at any time later",
            "offer this list, as it is, and ask which items apply",
            "its rules become hard rules and approval boundaries",
            "its targets are what the explorer studies once the work is known",
        ):
            assert part in _flat(no_work), part

    def test_a_blank_description_is_no_description(self):
        for mode in ("personal", "worker"):
            assert onboarding_prompt(mode, " \n\t\n", GUIDE) == onboarding_prompt(mode, "", GUIDE)

    @pytest.mark.parametrize("mode", ["personal", "worker"])
    def test_both_first_messages_lead_into_the_same_steps(self, mode):
        assert _shared(onboarding_prompt(mode, "", GUIDE)) == _shared(onboarding_prompt(mode, DESCRIPTION, GUIDE))

    @pytest.mark.parametrize("mode, description", VARIANTS)
    def test_the_setup_rules_and_the_steps_after_the_plan(self, mode, description):
        shared = _shared(onboarding_prompt(mode, description, GUIDE))
        for part in (
            "Change nothing and start no real work before the person approves your proposal",
            "Nerve plans (`plan_propose`)", "you do not `memorize`",
            "the one file you write is your plan file",
            "never invent a repository", "skip onboarding on the setup page",
            "`backlog`", "`plan_review`", "`task-planner`", "`task_status_create`",
            "`skill_create`", "`task_create`", "`memorize`", "`propose_config_change`",
            "config/cron/jobs.yaml", "config/cron/system.yaml", "run_if", "prompt_file",
            "### 4. Apply what the person approved", "Only after the approval",
            "Apply it yourself, in this session, without teammates or sub-agents",
            "### 5. Summarize", "### 6. Mark onboarding done", "`onboarding_done`",
        ):
            assert part in shared, part

    @pytest.mark.parametrize("mode, description", VARIANTS)
    def test_one_background_explorer_with_a_brief(self, mode, description):
        explore = _between(onboarding_prompt(mode, description, GUIDE), "### 1. Explore in the background", "### 2.")
        for part in (
            "start **one** explorer", "`Agent` tool call with `run_in_background: true`",
            "the brackets and `<workspace>` filled in",
            "its progress shows on the agent line below the chat",
            # Partly named targets: explore what is named, ask for the rest.
            "Start it on the targets you know, and ask for the ones still missing",
            "in the question card, where the person can type them under Other…",
            "or in one plain message in a turn without a card",
            "Do not study the same targets yourself", "a second explorer studies only those",
            "Skip the explorer when there is nothing to clone or fetch",
            # A failed or partial explorer does not stall the proposal.
            "If the explorer fails or returns partial findings, continue with what you have "
            "and say so in the plan; the gap goes under What I need from you",
        ):
            assert part in explore, part
        brief = _between(explore, "```text\n", "\n```")
        for part in (
            "Study [the targets] for a Nerve agent that will [the work items]",
            # Read only, on the person's machine: no installs, builds or test runs.
            "Only read: write nothing but the clones under <workspace>/repos/",
            "install no dependencies, build nothing, and run no tests or linters",
            "Stop after about ten minutes and report what you have",
            "Ask the person nothing: open questions go under Risks and missing access",
            "Clone each repository into <workspace>/repos/<name>", "gh repo clone",
            "-- --filter=blob:none", "`git clone --filter=blob:none`",
            "Report what you could not access; do not guess",
            "Learn from the CI configuration, scripts and docs: how to build and test",
            "CI's own run times where they are visible (`gh run list`)",
            "CODEOWNERS", "open PRs and issues", "access mode",
            "a Findings report of at most 60 lines", '"not found" is a valid finding',
        ):
            assert part in brief, part
        headings = [
            "Repositories", "Build and test", "CI", "Rules and conventions", "Volume",
            "Risks and missing access", "Suggested first task",
        ]
        assert ", ".join(headings) + "." in " ".join(brief.split())

    @pytest.mark.parametrize("mode, description", VARIANTS)
    def test_questions_in_the_foreground(self, mode, description):
        ask = _between(onboarding_prompt(mode, description, GUIDE), "### 2. Ask in the foreground", "### 3.")
        for part in (
            "`AskUserQuestion`", "one to four questions per call",
            'the recommended default first with "(recommended)"', "At most two rounds",
            # Every question says why it is asked, every option what it leads to.
            'Each question carries its reason in one clause ("so the review job knows how often to run")',
            "each option's description says what choosing it leads to",
            "the account you act as on the code host", "the approval boundary", "the cadence",
            "where results go",
            # What setup already knows is never asked.
            "the mode, the person's name, the time zone and quiet hours",
            "the connected channels and sources", "read them, never ask",
            # The explorer is not polled.
            "Do not check on the explorer or poll it", "say so in one line and end your turn",
        ):
            assert part in ask, part

    @pytest.mark.parametrize("mode, description", VARIANTS)
    def test_every_turn_that_asks_explains_first(self, mode, description):
        """A real run sent a question card with no text at all: the rule is an
        explicit order (text, then the card), and the text asks nothing."""
        ask = _between(onboarding_prompt(mode, description, GUIDE), "### 2. Ask in the foreground", "### 3.")
        explain = _between(ask, "Explain before you ask.", "Ask only what neither")
        for part in (
            "In every turn that asks, write the text first and call `AskUserQuestion` after it",
            "a question card without text before it is a mistake",
            "The text says what you understood, what the explorer studies, how you intend to set "
            "this up and why the questions matter",
            "The questions go in the card, and the text explains and asks none of them",
        ):
            assert part in explain, part
        assert "Ask nothing in plain text" not in ask

    @pytest.mark.parametrize("mode, description", VARIANTS)
    def test_the_proposal_is_a_native_plan(self, mode, description):
        prompt = onboarding_prompt(mode, description, GUIDE)
        plan = _between(prompt, "### 3. Propose the setup as a plan", "### 4.")
        wait, enter, write, exit_ = (
            plan.index("Exploring and asking end before plan mode: first wait for the answers, "
                       "and for the explorer's Findings when one runs"),
            plan.index("Then call `EnterPlanMode`"),
            plan.index("write the proposal to the plan file path that plan mode names"),
            plan.index("and call `ExitPlanMode`"),
        )
        assert wait < enter < write < exit_
        # Plan mode's own reminder pushes Explore agents and questions; in
        # plan mode there are none, unless a gap blocks the proposal.
        assert (
            "in plan mode start no further explorer and ask nothing more, unless a gap in the "
            "findings blocks the proposal, and then say so in one line"
        ) in plan
        # The plan-mode tools are deferred in the CLI: loaded before use.
        assert "load them with `ToolSearch` before their first use" in prompt
        expected = [h for h in PLAN_HEADINGS if mode == "worker" or h != "## TASK.md"]
        assert _plan_skeleton(prompt) == expected
        # Plan mode's reminder brings its own format; the fixed headings win.
        assert (
            "Use only these headings, in this order; plan mode's own plan format "
            "(context, files, verification) does not apply here"
        ) in plan
        assert ("- **TASK.md**:" in plan) == (mode == "worker")
        assert "**Later, if you want**: optional; at most three one-liners" in plan
        # Request changes: the person's words come back as the refusal.
        for part in (
            "`ExitPlanMode` comes back refused with their words", "Stay in plan mode",
            "revise the plan file and call `ExitPlanMode` again", "never ask what their words already answer",
        ):
            assert part in plan, part

    @pytest.mark.parametrize("mode, description", VARIANTS)
    def test_a_timeout_is_no_answer_and_plan_mode_waits_with_exit_plan_mode(self, mode, description):
        rules = _flat(_between(onboarding_prompt(mode, description, GUIDE), "## Rules", "## Nerve features"))
        for part in (
            "A question card that times out (no answer within the hour) is no answer: say in one "
            "line that you wait for the person and end your turn; ask again when they write",
            "A plan decision that times out is no answer either: even in plan mode, say in one "
            "line that the proposal waits for them and end your turn",
            "when they write, call `ExitPlanMode` again instead of asking anything",
        ):
            assert part in rules, part

    @pytest.mark.parametrize("mode, description", VARIANTS)
    def test_features_are_applied_not_advertised(self, mode, description):
        features = _between(
            onboarding_prompt(mode, description, GUIDE),
            "## Nerve features: applied, not advertised", "## How the conversation goes",
        )
        for part in (
            "only as the answer to a need of this work",
            "never list what you can do in general: no menu, no feature tour",
            "Pitch nothing that setup already configured (channels, sources, quiet hours, memory)",
            'the plan\'s last section, "Later, if you want": at most three one-liners',
            # No re-offer of the menu items the person passed over.
            "none of them an item of the first message's list that the person saw and did not choose",
            "Outside it, pitch nothing",
        ):
            assert part in features, part

    def test_a_worker_gets_its_description_the_task_md_rules_and_the_guide(self):
        prompt = onboarding_prompt("worker", DESCRIPTION + "\nNever merge.", GUIDE)
        assert f"## What the operator wrote at setup\n\n> {DESCRIPTION}\n> Never merge." in prompt
        assert prompt.index("wrote at setup") < prompt.index("## Your first message")
        task_md = _between(prompt, "\n## TASK.md\n", "\n---\n")
        assert "Fill it when you apply the setup (step 4)" in task_md
        assert "### ⛔ HARD RULE" in task_md and "`onboarding_done` removes the marker" in task_md
        assert prompt.endswith(f"---\n\n{GUIDE}\n\n---")
        assert "> (no description was given at setup)" in onboarding_prompt("worker", "", "x")
        assert WORKER_GUIDE_URL in onboarding_prompt("worker", DESCRIPTION, "")

    def test_a_personal_install_with_a_description_gets_it_without_the_worker_parts(self):
        prompt = onboarding_prompt("personal", DESCRIPTION)
        assert f"## What the person wrote at setup\n\n> {DESCRIPTION}" in prompt
        assert "Restate the work as you understand it" in prompt
        assert "## TASK.md" not in prompt and "Worker Best Practices" not in prompt
        assert "wrote at setup" not in onboarding_prompt("personal")

    @pytest.mark.parametrize("mode, description", VARIANTS)
    def test_the_first_turn_begins_with_the_visible_message(self, mode, description):
        """A live run read ``USER.md`` and thought before it said a word; the
        first turn opens with the visible message, from what it already has."""
        prompt = onboarding_prompt(mode, description, GUIDE)
        intro = _between(prompt, "This is the **onboarding conversation**", "## ")
        assert (
            "Your first turn begins with the visible message: no tool call precedes it, and "
            "everything that message needs is in your system prompt and in this message"
        ) in intro
        assert "USER.md" not in _first_message(prompt)

    @pytest.mark.parametrize("mode, description", VARIANTS)
    def test_the_person_is_greeted_by_the_name_given(self, mode, description):
        named = onboarding_prompt(mode, description, GUIDE, person_name="  Alice \n Example ")
        assert "The person's name is Alice Example." in _between(named, "This is the", "## ")
        assert "Greet the person by name" in _first_message(named)
        assert "without a name" not in named
        unnamed = onboarding_prompt(mode, description, GUIDE)
        assert "The person's name is" not in unnamed
        assert "Greet the person without a name" in _first_message(unnamed)
        assert onboarding_prompt(mode, description, GUIDE, person_name=" \n ") == unnamed
        # The steps after the first message do not depend on the name.
        assert _shared(named) == _shared(unnamed)
        long = onboarding_prompt(mode, description, GUIDE, person_name="A" * 200)
        assert f"The person's name is {'A' * 80}." in long

    @pytest.mark.parametrize("mode, description", VARIANTS)
    def test_a_change_request_never_widens_what_was_allowed(self, mode, description):
        plan = _between(onboarding_prompt(mode, description, GUIDE), "### 3. Propose the setup as a plan", "### 4.")
        assert (
            "A change request never widens what the person allowed earlier: when their words "
            "seem to, keep the narrower choice and ask about it in one line"
        ) in plan

    @pytest.mark.parametrize("mode, description", VARIANTS)
    def test_new_jobs_are_loaded_with_nerve_reload(self, mode, description):
        apply = _between(onboarding_prompt(mode, description, GUIDE), "### 4. Apply", "### 5.")
        for part in (
            "After editing `config/cron/jobs.yaml`, run `nerve reload` so this instance loads the jobs",
            "If the CLI does not find the instance, `nerve doctor` prints its config directory; "
            "then run `nerve -c <that directory> reload`",
            "use `propose_config_change` instead of editing the files, and skip the reload",
        ):
            assert part in apply, part


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

    async def test_a_workers_note_starts_on_the_work(self, db, tmp_path):
        """The note from the Mode step, read back from ``TASK.md``, picks the
        first message that starts on the work (the list is kept only for a
        note that names no work)."""
        apply_mode(tmp_path, "worker", DESCRIPTION)
        engine = _engine(db)
        await onboarding.start(engine, tmp_path, actor=None, cause=None)
        message = (await _first_turn(engine))["user_message"]
        assert message == onboarding_prompt("worker", DESCRIPTION, read_worker_guide())
        assert "Restate the work as you understand it" in message
        assert not _offered(_describes_work(message))
        assert _offered(_names_no_work(message)) == list(ONBOARDING_OFFERS)

    async def test_a_worker_without_a_note_is_offered_the_list(self, db, tmp_path):
        apply_mode(tmp_path, "worker")
        engine = _engine(db)
        await onboarding.start(engine, tmp_path, actor=None, cause=None)
        message = (await _first_turn(engine))["user_message"]
        assert all(f"- {item}" in message for item in ONBOARDING_OFFERS)
        assert "> (no description was given at setup)" in message

    async def test_the_agent_greets_the_person_who_finished_setup_by_name(self, db, tmp_path):
        """Finish passes the person as the cause; their display name goes into
        the prompt, so a worker (no ``USER.md``) needs no file to greet them."""
        apply_mode(tmp_path, "worker", DESCRIPTION)
        await ensure_actor_row(db, ALICE)
        engine = _engine(db)
        await onboarding.start(engine, tmp_path, actor=ALICE, cause=ALICE)
        message = (await _first_turn(engine))["user_message"]
        assert message == onboarding_prompt(
            "worker", DESCRIPTION, read_worker_guide(), person_name="Alice Example",
        )
        assert "The person's name is Alice Example." in message

    async def test_without_a_known_name_the_agent_greets_without_one(self, db, tmp_path):
        apply_mode(tmp_path, "personal")
        engine = _engine(db)
        await onboarding.start(engine, tmp_path, actor=db.system_actor, cause=db.system_actor)
        message = (await _first_turn(engine))["user_message"]
        assert "The person's name is" not in message
        assert "Greet the person without a name" in message


@pytest.mark.asyncio
class TestBoot:
    async def test_a_requested_conversation_opens_at_start_for_its_person(self, db, tmp_path):
        apply_mode(tmp_path, "personal")
        person = db.system_actor.actor_id
        await onboarding.request(db, tmp_path, actor_id=person)
        state = await onboarding.start_on_boot(_engine(db), tmp_path)
        assert state.status == "running"
        assert (await db.get_session(state.session_id))["caused_by_actor_id"] == person

    async def test_at_start_the_stored_person_is_greeted_by_name(self, db, tmp_path):
        """A restart came first: the stored actor id of the person who
        finished setup is resolved to their display name."""
        apply_mode(tmp_path, "personal")
        await ensure_actor_row(db, ALICE)
        await onboarding.request(db, tmp_path, actor_id=ALICE.actor_id)
        engine = _engine(db)
        await onboarding.start_on_boot(engine, tmp_path)
        assert "The person's name is Alice Example." in (await _first_turn(engine))["user_message"]

    async def test_a_worker_from_nerve_init_sets_itself_up_once(self, db, tmp_path):
        """``nerve init`` with a task description: one autonomous session at
        the next start, for the system, with the old prompt in the
        transcript; it ends with ``onboarding_done``."""
        apply_mode(tmp_path, "worker", DESCRIPTION)
        assert await onboarding.request_autonomous(db, tmp_path)
        pending = await onboarding.read_state(db, tmp_path)
        assert (pending.status, pending.kind) == ("pending", "autonomous")
        engine = _engine(db)
        state = await onboarding.start_on_boot(engine, tmp_path)
        assert (state.status, state.kind) == ("running", "autonomous")
        turn = await _first_turn(engine)
        assert turn["user_message"] == worker_onboarding_prompt(DESCRIPTION, read_worker_guide())
        assert turn["internal"] is False and turn["source"] == "cron"
        session = await db.get_session(state.session_id)
        assert session["source"] == "cron"
        assert session["caused_by_actor_id"] == db.system_actor.actor_id
        # Once: the next start opens nothing, and nothing asks again.
        assert await onboarding.start_on_boot(engine, tmp_path) is None
        assert not await onboarding.request_autonomous(db, tmp_path)
        assert engine.run.await_count == 1

        ctx = ToolContext(session_id=state.session_id, workspace=tmp_path, db=db, actor=db.system_actor)
        assert not (await onboarding_done_handler(ctx, {})).is_error
        assert (await onboarding.read_state(db, tmp_path)).status == "done"
        assert TASK_ONBOARDING_MARKER not in (tmp_path / "TASK.md").read_text()

    async def test_a_worker_without_a_description_waits_for_the_conversation(self, db, tmp_path):
        """No task description, or an install from before the row: nothing
        runs at start; the web UI offers the conversation."""
        apply_mode(tmp_path, "worker")
        assert not await onboarding.request_autonomous(db, tmp_path)
        apply_mode(tmp_path / "old", "worker", DESCRIPTION)
        for ws in (tmp_path, tmp_path / "old"):
            state = await onboarding.read_state(db, ws)
            assert (state.status, state.kind) == ("pending", "conversation")
            assert await onboarding.start_on_boot(_engine(db), ws) is None
        assert await db.get_onboarding() is None

    async def test_setup_in_the_browser_asks_for_the_conversation(self, db, tmp_path):
        apply_mode(tmp_path, "worker", DESCRIPTION)
        await onboarding.request_autonomous(db, tmp_path)
        await onboarding.request(db, tmp_path, actor_id=None)
        assert (await onboarding.read_state(db, tmp_path)).kind == "conversation"
        engine = _engine(db)
        state = await onboarding.start_on_boot(engine, tmp_path)
        assert state.kind == "conversation"
        assert (await _first_turn(engine))["internal"] is True

    async def test_finish_opens_the_conversation_instead_of_the_autonomous_run(self, db, tmp_path):
        apply_mode(tmp_path, "worker", DESCRIPTION)
        await onboarding.request_autonomous(db, tmp_path)
        engine = _engine(db)
        state = await onboarding.start(engine, tmp_path, actor=None, cause=None)
        assert (state.status, state.kind) == ("running", "conversation")
        assert (await _first_turn(engine))["source"] == "web"

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

    async def test_a_session_that_removed_the_marker_itself_can_still_end(self, db, tmp_path):
        apply_mode(tmp_path, "worker", DESCRIPTION)
        await onboarding.request_autonomous(db, tmp_path)
        state = await onboarding.start_on_boot(_engine(db), tmp_path)
        task = tmp_path / "TASK.md"
        task.write_text("".join(
            line for line in task.read_text().splitlines(keepends=True)
            if TASK_ONBOARDING_MARKER not in line
        ))
        ctx = ToolContext(session_id=state.session_id, workspace=tmp_path, db=db, actor=db.system_actor)
        assert not (await onboarding_done_handler(ctx, {})).is_error
        assert (await db.get_onboarding())["status"] == "done"

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
