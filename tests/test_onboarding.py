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
import json
import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from nerve import onboarding, onboarding_environment
from nerve.agent.prompts import (
    WORKER_GUIDE_URL,
    first_form_arguments,
    onboarding_prompt,
    worker_onboarding_prompt,
)
from nerve.agent.tools.handlers.forms import validate_form
from nerve.agent.tools.handlers.onboarding import onboarding_done_handler
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig
from nerve.onboarding_catalog import (
    CATALOG,
    FIRST_FORM_ITEMS,
    GROUP_LABELS,
    GROUPS,
    ITEMS,
    PERSONAL_ASSISTANT,
    SHARED_QUESTIONS,
    WorkSelection,
)
from nerve.onboarding_environment import InstanceFacts
from nerve.setup_writer import (
    TASK_ONBOARDING_MARKER,
    apply_mode,
    list_optional_crons,
    plan_optional_crons,
    render_task_md,
    render_work_description,
    system_cron_file,
    work_selection_from,
)
from nerve.identity import ACTOR_KIND_HUMAN, Actor
from nerve.workspace import read_worker_guide
from tests.actor_rows import ensure_actor_row, ensure_system_principal

DESCRIPTION = "Watch the nightly build for example/repo and propose fixes."
# The catalog items of the first form, in order.
FORM_LABELS = [ITEMS[item_id].label for item_id in FIRST_FORM_ITEMS]
# What the Mode step's checkboxes chose: two items, custom work and rules.
CHOSEN = WorkSelection(
    items=("review-prs", "triage-issues"), custom="also watch the release branch", rules="never merge anything",
)
ALICE = Actor(
    actor_id="11111111-2222-4333-8444-555555555555", kind=ACTOR_KIND_HUMAN, display_name="Alice Example",
)
# What Nerve knows about the instance (``nerve.onboarding_environment``; the
# builder has its own tests). The starts here get these facts.
FACTS = InstanceFacts(
    section=(
        "## This instance\n\n"
        "- Mode: worker (an agent for a team).\n"
        "- GitHub CLI (`gh`): authenticated as `alice`.\n"
        "- Slack: provided by the platform (hosted mode: the shared channel gateway holds the tokens); "
        "the gateway is connected.\n"
        "- Telegram channel: not configured."
    ),
    github_access="GitHub access: `gh` as `alice`.",
)
WITH_FACTS = {"environment": FACTS.section, "github_access": FACTS.github_access}
# A stored first reply: the greeting, then the first form (still open).
_GREETING = {"type": "text", "content": "Hello."}
_FORM_CALL = {
    "type": "tool_call", "tool": "mcp__nerve__ask_form", "input": first_form_arguments(False),
    "tool_use_id": "toolu_form",
}
NONE_KNOWN = "GitHub access: none known — clone public URLs only; report what needs a credential."
_REAL_GATHER = onboarding_environment.gather


@pytest_asyncio.fixture
async def db(db):  # noqa: F811 - the conftest database, with an identity
    await ensure_system_principal(db)
    return db


@pytest.fixture(autouse=True)
def gathered(monkeypatch) -> list[Path]:
    """Every start here gets :data:`FACTS`; the workspaces it gathered them for."""
    calls: list[Path] = []

    def _gather(config, workspace, **kwargs):
        calls.append(Path(workspace))
        return FACTS

    monkeypatch.setattr(onboarding_environment, "gather", _gather)
    return calls


def _engine(
    db, *, outcome: str = "completed", slow: bool = False, reply_blocks: list | None = None,
) -> MagicMock:
    """Real session rows; turns that only record their arguments and end with
    ``outcome``, after storing a reply with ``reply_blocks`` when given."""
    engine = MagicMock()
    engine.db = db
    engine.config.credential_available.return_value = True
    turn = SimpleNamespace(outcome=None)

    async def _run(**kwargs):
        if reply_blocks is not None:
            await db.add_message(kwargs["session_id"], "assistant", "", blocks=reply_blocks, actor=None)
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


def _form(text: str) -> dict | None:
    """The ``ask_form`` arguments that ``text`` gives as JSON, else ``None``."""
    if "```json\n" not in text:
        return None
    return json.loads(_between(text, "```json\n", "\n```"))


def _form_labels(text: str) -> list[str]:
    """The labels of the first form's options in ``text``, in order."""
    form = _form(text)
    return [option["label"] for option in form["questions"][0]["options"]] if form else []


def _form_options(mode: str) -> list[str]:
    """The first form's options: a personal install's end with the personal
    assistant; a worker's leave it out."""
    return FORM_LABELS if mode == "worker" else [*FORM_LABELS, PERSONAL_ASSISTANT]


def _message_text(first: str) -> str:
    """A first message's instructions, without the catalog reference."""
    return first.split("### The work catalog", 1)[0]


def _reference(text: str) -> str:
    """The catalog reference after the first form."""
    return _between(text, "### The work catalog: reference only, do not list\n")


def _groups(text: str) -> dict[str, str]:
    """Each listed label and the group heading above it."""
    heading, found = None, {}
    for line in text.splitlines():
        if line.startswith("**") and line.endswith("**"):
            heading = line.strip("*")
        elif line.startswith("- ") and heading:
            found[line[2:].split(" — ", 1)[0]] = heading
    return found


def _chosen(prompt: str) -> str:
    """The section with what the operator chose at setup."""
    return _between(prompt, "## What the operator chose at setup", "\n## ")


def _shared(prompt: str) -> str:
    """Everything after the first message: the same for both of its variants."""
    return _between(prompt, "## Rules")


def _plan_skeleton(prompt: str) -> list[str]:
    return _between(prompt, "```markdown\n", "\n```").splitlines()


class TestPrompt:
    """The hidden first message: the description picks the first message, the
    steps after it are shared."""

    @pytest.mark.parametrize("mode", ["personal", "worker"])
    def test_without_a_description_the_first_message_is_a_greeting_and_one_form(self, mode):
        """Two sentences, then one ``ask_form`` call with fixed arguments: one
        ``multi`` question, its options in order, Other… on, an answer
        required. The message lists nothing and does not say that more than
        one may be chosen: the form's checkboxes say that."""
        prompt = onboarding_prompt(mode, "", GUIDE)
        first = _first_message(prompt)
        text = _flat(_message_text(first))
        steps = [
            "Greet the person without a name, in one sentence.",
            "Then say in one sentence that you are set up by talking to you, now and at any time later.",
            "Then call `ask_form` once, with exactly these arguments:",
            "Write no other text: no list of what you can do, and no other question.",
            "Step 2's rule on the text before a form does not apply to this first form.",
            "The answer comes back as the result of the call, in this turn; then go on with steps 1 "
            "and 2 below.",
            "Change nothing in this first message.",
        ]
        positions = [text.index(step) for step in steps]
        assert positions == sorted(positions)
        form = _form(first)
        assert form == first_form_arguments(mode == "worker")
        (question,) = form["questions"]
        assert question == {
            "id": "work", "kind": "multi", "label": "Work",
            "question": "What should I help you with?",
            "help": "So that I set up only what you need.",
            "options": question["options"], "allow_other": True, "required": True,
        }
        assert _form_labels(first) == _form_options(mode)
        for option in question["options"][:len(FORM_LABELS)]:
            item = next(item for item in CATALOG if item.label == option["label"])
            assert option["description"] == item.description, item.id
        # ask_form shows the form as given.
        shown, problems = validate_form(form)
        assert problems == []
        assert [o["label"] for o in shown["questions"][0]["options"]] == _form_options(mode)
        assert not [line for line in _message_text(first).splitlines() if line.startswith("- ")]
        assert "more than one" not in prompt and "offer this list" not in prompt
        assert "Restate the work" not in prompt
        assert "you speak first" in prompt
        assert "chose at setup" not in prompt

    @pytest.mark.parametrize("mode", ["personal", "worker"])
    def test_the_catalog_follows_the_form_as_a_reference_never_listed(self, mode):
        """The whole catalog, by group in the catalog's order, each item with
        its one-line description, for answers given in Other…; the agent
        lists none of it."""
        first = _first_message(onboarding_prompt(mode, "", GUIDE))
        assert first.index("```json") < first.index("### The work catalog")
        reference = _reference(first)
        assert (
            "What Nerve can set up: use it to understand an answer given in Other… and to plan the "
            "work. Never list it or offer it, in this message or later."
        ) in _flat(reference)
        for item in CATALOG:
            assert f"\n- {item.label} — {item.description}" in reference, item.id
            assert _groups(reference)[item.label] == GROUP_LABELS[item.group], item.id
        headings = [reference.index(f"\n**{GROUP_LABELS[group]}**\n") for group in GROUPS]
        assert headings == sorted(headings)
        # The alert item carries the connection it needs.
        assert (
            "- First response to alerts — Reads where alerts land, does the first investigation, "
            "posts findings, escalates by rule. Needs an alert source; a Slack channel is the "
            "first-class one today.\n"
        ) in reference
        assert PERSONAL_ASSISTANT not in reference

    def test_a_personal_installs_form_ends_with_the_personal_assistant(self):
        """The personal assistant is the last option of a personal install's
        form, and never a worker's (nor a catalog item)."""
        personal = onboarding_prompt("personal")
        assert _form(_first_message(personal))["questions"][0]["options"][-1] == {
            "label": "Personal assistant", "description": "Inbox triage, reminders, planning your week.",
        }
        assert PERSONAL_ASSISTANT not in [item.label for item in CATALOG]
        assert "Worker Best Practices" not in personal and "## TASK.md" not in personal
        worker = onboarding_prompt("worker", "", GUIDE)
        assert _form_labels(_first_message(worker)) == FORM_LABELS
        assert PERSONAL_ASSISTANT not in worker

    def test_a_personal_install_is_a_nerve_agent_and_a_worker_keeps_its_wording(self):
        """No mode is chosen in the browser, so a personal install's prompt
        does not call the agent a personal assistant."""
        personal = _between(onboarding_prompt("personal"), "This is the", "## ")
        assert personal.startswith(
            " **onboarding conversation** of a Nerve agent. The person who set it up opens this chat."
        )
        assert "personal assistant" not in personal.lower()
        worker = _between(onboarding_prompt("worker", "", GUIDE), "This is the", "## ")
        assert worker.startswith(
            " **onboarding conversation** of this Nerve agent, which is set up as a worker for a "
            "team. The person who set it up opens this chat."
        )

    @pytest.mark.parametrize("mode", ["personal", "worker"])
    def test_a_note_that_describes_work_is_explained_before_anything_is_asked(self, mode):
        """No menu, and the text comes first: greeting, the work in one
        sentence, what the explorer studies, the intended setup and why the
        questions matter; then the explorer and the question card."""
        prompt = onboarding_prompt(mode, DESCRIPTION, GUIDE)
        work = _flat(_describes_work(prompt))
        assert f"> {DESCRIPTION}" in _between(prompt, "wrote at setup", "## Your first message")
        assert _form(_describes_work(prompt)) is None
        assert "offer no menu" in work and "set up by talking to you" not in work
        steps = [
            "Write the message text first, before any tool call",
            "1. Greet the person without a name.",
            "2. Restate the work as you understand it, in one sentence",
            "3. Say in one line what the explorer will study (step 1 below)",
            "4. In two to four lines, sketch how you intend to set this up",
            "and why the questions that follow matter",
            "Then start the explorer on the targets the note names",
            "ask your first questions with `ask_form` (step 2 below)",
        ]
        positions = [work.index(step) for step in steps]
        assert positions == sorted(positions)
        assert "Targets the note leaves out you ask for as step 1 says" in work

    @pytest.mark.parametrize("mode", ["personal", "worker"])
    def test_a_note_that_names_no_work_gets_the_first_form(self, mode):
        """Only rules ("never merge anything") or only targets ("org/repo-a"):
        the agent knows no work yet, so it asks with the first form, the same
        as without a note, and carries the note into the plan."""
        prompt = onboarding_prompt(mode, DESCRIPTION, GUIDE)
        no_work = _names_no_work(prompt)
        blank = _first_message(onboarding_prompt(mode, "", GUIDE))
        assert _form(no_work) == _form(blank) == first_form_arguments(mode == "worker")
        assert _reference(no_work) == _reference(blank)
        steps = [
            'only rules, such as "never merge anything"', 'only targets, such as "org/repo-a"',
            "ask what the work is. Greet the person without a name and say what you noted, in one sentence.",
            "Then say in one sentence that you are set up by talking to you, now and at any time later.",
            "Then call `ask_form` once, with exactly these arguments:",
            "Write no other text: no list of what you can do, and no other question.",
            "its rules become hard rules and approval boundaries",
            "its targets are what the explorer studies once the work is known",
        ]
        positions = [_flat(no_work).index(step) for step in steps]
        assert positions == sorted(positions)
        assert not [line for line in _message_text(no_work).splitlines() if line.startswith("- ")]

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
            "in the form, as a `repos` or `list` question",
            "or in one plain message in a turn without a form",
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
        explicit order (text, then the form), and the text asks nothing."""
        ask = _between(onboarding_prompt(mode, description, GUIDE), "### 2. Ask in the foreground", "### 3.")
        explain = _flat(_between(ask, "Explain before you ask.", "Ask only what neither"))
        for part in (
            "In every turn that asks, write the text first and call `ask_form` after it",
            "a form without text before it is a mistake",
            "The text says what you understood, what the explorer studies, how you intend to set "
            "this up and why the questions matter",
            "The questions go in the form, and the text explains and asks none of them",
        ):
            assert part in explain, part
        assert "Ask nothing in plain text" not in ask

    @pytest.mark.parametrize("mode, description", VARIANTS)
    def test_questions_come_in_a_form_of_the_kind_each_answer_needs(self, mode, description):
        """Free-text facts (repository names) were forced into AskUserQuestion's
        two to four options. The questions go into one ``ask_form`` form, each
        of the kind its answer needs; AskUserQuestion is only the fallback."""
        ask = _flat(_between(onboarding_prompt(mode, description, GUIDE), "### 2. Ask in the foreground", "### 3."))
        for part in (
            "with `ask_form`: one form per round, with up to six questions. At most two rounds",
            "repositories: `repos` (the person enters each as `owner/name`)",
            "a decision, such as the account you act as, the approval boundary or the cadence: "
            "`choice`, with two to eight concrete options, the recommended default first",
            # The login Nerve knows is the recommended account (B8).
            "for the account, the GitHub login you know is that option",
            "`multi` when several options can apply at once",
            "channels, topics and other names: `list`",
            "a link: `url`",
            "anything else, in the person's own words: `text`",
            'its reason in one clause ("so the review job knows how often to run") as its `help`',
            "Mark a question `required: false` when the setup works without its answer",
            "an optional question left empty comes back under `skipped`",
        ):
            assert part in ask, part
        fallback = ask.index("Use `AskUserQuestion` only when `ask_form` is not available")
        assert ask.index("with `ask_form`") < fallback
        assert "one to four questions per call, each with two to four options" in ask[fallback:]

    @pytest.mark.parametrize("mode, description", VARIANTS)
    def test_ask_form_loads_with_the_nerve_tools_and_falls_back(self, mode, description):
        """Nerve's server is always loaded (``alwaysLoad``), the CLI's
        interactive built-ins may be deferred; without ``ask_form`` the agent
        asks with AskUserQuestion, without both in a message."""
        rules = _flat(_between(onboarding_prompt(mode, description, GUIDE), "## Rules", "## Nerve features"))
        for part in (
            "`AskUserQuestion`, `EnterPlanMode` and `ExitPlanMode` may be deferred tools: load them "
            "with `ToolSearch` before their first use",
            "`ask_form` (`mcp__nerve__ask_form`) loads with the other Nerve tools; if it is not among "
            "your tools, look for it with `ToolSearch` as well",
            "ask with `AskUserQuestion` when `ask_form` is missing, and in a message when both are",
            "send the proposal as one message with the plan's headings",
        ):
            assert part in rules, part

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
            "A form or question card that times out (no answer within the hour) or is closed "
            "without an answer is no answer: say in one line that you wait for the person and end "
            "your turn; ask again when they write",
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
            # No re-offer of the kinds of work the person passed over, in
            # the Mode step's checkboxes or in the first message's list.
            "none of them a kind of work from the list that the person saw, at setup or in your "
            "first message, and did not choose",
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

    @pytest.mark.parametrize("mode, description", VARIANTS)
    def test_the_instance_facts_come_before_the_first_message_as_facts(self, mode, description):
        """A real run asked about `gh` without checking anything: the first
        turn calls no tool before its message, so Nerve's facts about the
        instance follow the note, ahead of the first message, with the rules
        that make them facts."""
        prompt = onboarding_prompt(mode, description, GUIDE, **WITH_FACTS)
        assert prompt.index(FACTS.section) < prompt.index("## Your first message")
        start = prompt.index("## This instance")
        if mode == "worker" or description:
            assert prompt.index("wrote at setup") < start
        else:
            assert prompt.index("This is the **onboarding conversation**") < start
            assert "wrote at setup" not in prompt
        rules = _flat(_between(prompt, FACTS.section, "## Your first message"))
        for part in (
            "These lines are facts: Nerve read them when this conversation opened",
            "Never ask the person about them, and never check them again with a tool before your first message",
            "A line that says unknown is yours to ask about, or for the explorer to find out",
            # A known login is the recommendation, never an open question.
            'when a line above names a GitHub login, offer it as the recommended option '
            '("act as `<login>` (recommended)") and ask only to confirm it or to name another',
            "Never ask which account as an open question",
        ):
            assert part in rules, part
        bare = onboarding_prompt(mode, description, GUIDE)
        for absent in ("## This instance", "These lines are facts", "GitHub access:"):
            assert absent not in bare, absent
        assert onboarding_prompt(mode, description, GUIDE, environment=" \n ", github_access="x") == bare

    @pytest.mark.parametrize("mode, description", VARIANTS)
    def test_the_explorer_brief_carries_the_github_access(self, mode, description):
        brief = _between(onboarding_prompt(mode, description, GUIDE, **WITH_FACTS), "```text\n", "\n```")
        assert "ten minutes and report what you have.\n\nGitHub access: `gh` as `alice`.\n\n- Clone each" in brief
        unknown = onboarding_prompt(mode, description, GUIDE, environment=FACTS.section, github_access=NONE_KNOWN)
        assert f"\n\n{NONE_KNOWN}\n\n- Clone each" in _between(unknown, "```text\n", "\n```")
        # The access line comes with the facts only.
        assert "GitHub access:" not in onboarding_prompt(mode, description, GUIDE, github_access=FACTS.github_access)

    @pytest.mark.parametrize("mode, description", VARIANTS)
    def test_the_facts_change_nothing_else(self, mode, description):
        """Take the section, its rules and the brief's access line away, and
        the prompt is the one without facts, byte for byte."""
        prompt = onboarding_prompt(mode, description, GUIDE, person_name="Alice Example", **WITH_FACTS)
        block = "## This instance" + _between(prompt, "## This instance", "## Your first message")
        rest = prompt.replace(block, "", 1).replace(FACTS.github_access + "\n\n", "", 1)
        assert rest == onboarding_prompt(mode, description, GUIDE, person_name="Alice Example")
        # Both first messages still lead into the same steps.
        assert _shared(prompt) == _shared(onboarding_prompt(mode, "", GUIDE, **WITH_FACTS))


class TestChosenWork:
    """The Mode step's selection (catalog items, custom work, rules) in the
    hidden first message: what was chosen, with the questions it raises, and
    no menu; with only rules, the menu and the rules carried."""

    def test_chosen_items_with_their_questions_and_no_menu(self):
        prompt = onboarding_prompt("worker", "", GUIDE, person_name="Alice Example", work=CHOSEN)
        chosen = _chosen(prompt)
        assert "wrote at setup" not in prompt
        assert prompt.index("## What the operator chose at setup") < prompt.index("## Your first message")
        # Each chosen item: what it does and what it needs.
        work = _between(chosen, "Work:\n", "\n\n")
        assert work.splitlines() == [
            "- Review pull requests — Reads diffs, runs targeted checks, posts review comments. "
            "Needs GitHub access.",
            "- Triage issues — Labels, finds duplicates, asks reporters for missing info, routes, "
            "flags stale. Needs GitHub access.",
        ]
        assert "Also:\n> also watch the release branch\n" in chosen
        assert "Must know or never do:\n> never merge anything\n" in chosen
        for part in (
            "The operator ticked these in the list of work Nerve offers and added what follows Also",
            "In the explorer's brief, [the work items] are this work",
            # A connection the instance lacks is named in the plan.
            "When this instance lacks the connection an item needs, the plan names it under What I need from you",
            # Rules become hard rules and approval boundaries, never loosened.
            'The rules under "Must know or never do" become hard rules and approval boundaries in the plan',
            "When an answer seems to loosen one, keep the rule and ask about it in one line",
            # The list the questions come from, bounded by the two rounds.
            "Your questions in step 2 come from this list",
            "Ask only what neither setup, the facts about this instance nor the explorer's findings answer",
            "one question covers what several items share",
            "what does not fit into two rounds goes into the plan as your recommended default",
        ):
            assert part in _flat(chosen), part
        # The first message: no menu, the work explained before anything is asked.
        first = _first_message(prompt)
        assert _form(first) is None and "**Code**" not in prompt
        assert "### The work catalog" not in prompt
        steps = [
            "offer no menu, and do not ask again what was chosen",
            "Write the message text first, before any tool call",
            "1. Greet the person by name.",
            "2. Restate the work as you understand it, in one sentence",
            "3. Say in one line what the explorer will study (step 1 below)",
            "4. In two to four lines, sketch how you intend to set this up",
            "ask your first questions with `ask_form` (step 2 below), from the list above",
            "The first round asks for the targets still missing (which repositories, which channels)",
        ]
        positions = [_flat(first).index(step) for step in steps]
        assert positions == sorted(positions)
        assert "When what was written names no work" not in first
        # The steps after the first message are the shared ones.
        assert _shared(prompt) == _shared(onboarding_prompt("worker", "", GUIDE))

    def test_shared_questions_come_once_and_each_item_brings_its_own(self):
        chosen = _chosen(onboarding_prompt("worker", "", GUIDE, work=CHOSEN))
        once = _between(chosen, "For all of the work, once:\n", "\n\n").splitlines()
        assert once == [
            f"- {SHARED_QUESTIONS['account']};",
            f"- {SHARED_QUESTIONS['approval']};",
            f"- {SHARED_QUESTIONS['destination']}.",
        ]
        assert chosen.count("For all of the work, once:") == 1
        for item_id in CHOSEN.items:
            item = ITEMS[item_id]
            block = _between(chosen, f"\n{item.label}:\n").split("\n\n", 1)[0]
            own = [f.text for f in item.follow_ups if f.shared is None]
            assert [line[2:].rstrip(";.") for line in block.splitlines()] == own, item_id
        # A follow-up that is a shared question is not repeated per item.
        for item_id in CHOSEN.items:
            for follow_up in ITEMS[item_id].follow_ups:
                if follow_up.shared:
                    assert follow_up.text not in chosen, follow_up.text

    def test_the_account_question_only_for_work_on_github(self):
        chosen = _chosen(onboarding_prompt("worker", "", GUIDE, work=WorkSelection(
            items=("channel-digests", "research-reports"),
        )))
        once = _between(chosen, "For all of the work, once:\n", "\n\n")
        assert SHARED_QUESTIONS["account"] not in chosen
        assert SHARED_QUESTIONS["approval"] in once and SHARED_QUESTIONS["destination"] in once
        assert "where to post" not in chosen and "where reports go" not in chosen
        assert "Needs a Slack or Telegram connection." in chosen and "Needs web access." in chosen
        alerts = _chosen(onboarding_prompt("worker", "", GUIDE, work=WorkSelection(items=("alert-response",))))
        assert "Needs an alert source; a Slack channel is the first-class one today." in alerts

    def test_custom_work_alone_is_the_work_without_a_menu(self):
        work = WorkSelection(custom="Watch the release notes of org/repo-a.\nSummarize them weekly.")
        prompt = onboarding_prompt("worker", "", GUIDE, work=work)
        chosen = _chosen(prompt)
        assert "Nothing was ticked in the list of work Nerve offers." in chosen
        assert "Also:\n> Watch the release notes of org/repo-a.\n> Summarize them weekly." in chosen
        assert "What follows Also is the work, in the operator's words" in chosen
        assert "Work:" not in chosen and "For all of the work, once:" not in chosen
        assert "Must know" not in chosen
        assert _form(_first_message(prompt)) is None
        first = _flat(_first_message(prompt))
        assert "offer no menu" in first
        assert "(step 2 below). The first round asks for the targets still missing" in first
        # Words that name no work: ask what the work is, still without a menu.
        assert (
            "When what was written names no work you can start on (only targets, or only rules), "
            "ask in this message what the work is; still offer no menu, because the person saw "
            "the list of work at setup"
        ) in first

    def test_rules_alone_get_the_first_form_and_carry_the_rules(self):
        prompt = onboarding_prompt("worker", "", GUIDE, work=WorkSelection(rules="Never merge anything."))
        chosen = _chosen(prompt)
        assert "Nothing was ticked in the list of work Nerve offers." in chosen
        assert "Must know or never do:\n> Never merge anything." in chosen
        assert "become hard rules and approval boundaries in the plan" in chosen
        assert "Also:" not in chosen and "explorer's brief" not in chosen
        first = _first_message(prompt)
        blank = _first_message(onboarding_prompt("worker", "", GUIDE))
        assert _form(first) == _form(blank) == first_form_arguments(True)
        assert _reference(first) == _reference(blank)
        steps = [
            "Nothing was ticked at setup, only rules. Greet the person without a name and say "
            "that you noted the rules, in one sentence.",
            "Then say in one sentence that you are set up by talking to you, now and at any time later.",
            "Then call `ask_form` once, with exactly these arguments:",
            "Write no other text: no list of what you can do, and no other question.",
            "Change nothing in this first message.",
        ]
        positions = [_flat(first).index(step) for step in steps]
        assert positions == sorted(positions)
        assert "Restate the work" not in first

    @pytest.mark.parametrize("mode", ["personal", "worker"])
    def test_nothing_chosen_is_no_description(self, mode):
        bare = onboarding_prompt(mode, "", GUIDE)
        assert onboarding_prompt(mode, "", GUIDE, work=WorkSelection()) == bare
        assert onboarding_prompt(mode, "", GUIDE, work=work_selection_from(None)) == bare
        assert onboarding_prompt(mode, "", GUIDE, work=WorkSelection(custom=" \n ", rules="\t")) == bare

    @pytest.mark.parametrize("mode", ["personal", "worker"])
    def test_a_free_text_note_keeps_what_was_written(self, mode):
        """A note from an older client or ``nerve init --terminal`` is no
        selection: the prompt is the one for the description, either way it
        is handed over."""
        note = work_selection_from(DESCRIPTION)
        assert note.free_text and note.custom == DESCRIPTION
        written = onboarding_prompt(mode, DESCRIPTION, GUIDE)
        assert onboarding_prompt(mode, "", GUIDE, work=note) == written
        assert onboarding_prompt(mode, DESCRIPTION, GUIDE, work=note) == written

    def test_the_selection_wins_over_a_description(self):
        assert onboarding_prompt("worker", DESCRIPTION, GUIDE, work=CHOSEN) == onboarding_prompt(
            "worker", "", GUIDE, work=CHOSEN,
        )

    def test_unknown_items_are_left_out(self):
        known = onboarding_prompt("worker", "", GUIDE, work=WorkSelection(items=("review-prs",)))
        assert onboarding_prompt(
            "worker", "", GUIDE, work=WorkSelection(items=("no-such-work", "review-prs", "review-prs")),
        ) == known
        assert onboarding_prompt("worker", "", GUIDE, work=WorkSelection(items=("no-such-work",))) == (
            onboarding_prompt("worker", "", GUIDE)
        )

    def test_the_facts_follow_what_was_chosen(self):
        """``## This instance`` comes after the chosen work, before the first
        message; taking it away leaves the prompt without facts."""
        prompt = onboarding_prompt("worker", "", GUIDE, work=CHOSEN, **WITH_FACTS)
        assert (
            prompt.index("## What the operator chose at setup") < prompt.index("## This instance")
            < prompt.index("## Your first message")
        )
        block = "## This instance" + _between(prompt, "## This instance", "## Your first message")
        rest = prompt.replace(block, "", 1).replace(FACTS.github_access + "\n\n", "", 1)
        assert rest == onboarding_prompt("worker", "", GUIDE, work=CHOSEN)

    def test_a_personal_install_has_no_worker_parts_with_a_selection(self):
        prompt = onboarding_prompt("personal", "", "", work=WorkSelection(items=("research-reports",)))
        assert "## What the person chose at setup" in prompt
        assert "## TASK.md" not in prompt and "Worker Best Practices" not in prompt


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
    async def test_opens_one_web_session_with_a_hidden_first_turn(self, db, tmp_path, gathered):
        apply_mode(tmp_path, "personal")
        engine = _engine(db)
        states = await asyncio.gather(*(
            onboarding.start(engine, tmp_path, actor=None, cause=None) for _ in range(3)
        ))
        assert {s.session_id for s in states} == {states[0].session_id}
        turn = await _first_turn(engine)
        assert engine.run.await_count == 1
        assert gathered == [tmp_path], "the facts are gathered once, for the one opening"
        assert turn["internal"] is True and turn["source"] == "web"
        # The chat opens with the greeting: no thinking from before it.
        assert turn["drop_leading_thinking"] is True
        assert turn["actor"] == db.system_actor
        assert turn["user_message"] == onboarding_prompt("personal", **WITH_FACTS)
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

    @pytest.mark.parametrize("blocks, kept", [
        pytest.param(None, False, id="nothing-stored"),
        pytest.param([{"type": "text", "content": "Turn failed: overloaded"}], False, id="text-only"),
        pytest.param([_GREETING, {**_FORM_CALL, "result": "ask_form: the call is invalid", "is_error": True}],
                     False, id="invalid-form"),
        pytest.param([_GREETING, _FORM_CALL], True, id="form-open"),
        pytest.param([_GREETING, {**_FORM_CALL, "result": "The person answered the form.", "is_error": False},
                      {"type": "text", "content": "Thanks."}], True, id="form-answered"),
    ])
    async def test_a_failed_first_turn_asks_again_only_before_its_form(self, db, tmp_path, blocks, kept):
        """The first turn goes on after the person answers the form. A failure
        before the person saw a form asks again; a failure after it keeps the
        conversation running, so the person continues in the same chat and
        its agent can still end the onboarding."""
        apply_mode(tmp_path, "personal")
        engine = _engine(db, outcome="failed", reply_blocks=blocks)
        tasks: list[asyncio.Task] = []
        engine.register_task = lambda session_id, task: tasks.append(task)
        state = await onboarding.start(engine, tmp_path, actor=None, cause=None)
        await tasks[0]
        stored = await db.get_onboarding()
        if not kept:
            assert stored["status"] == "requested"
            assert (await onboarding.read_state(db, tmp_path)).status == "pending"
            return
        assert (stored["status"], stored["session_id"]) == ("running", state.session_id)
        assert (await onboarding.read_state(db, tmp_path)).session_id == state.session_id
        ctx = ToolContext(session_id=state.session_id, workspace=tmp_path, db=db, actor=db.system_actor)
        assert not (await onboarding_done_handler(ctx, {})).is_error
        assert (await onboarding.read_state(db, tmp_path)).status == "done"

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
        first message that starts on the work (the first form is only for a
        note that names no work)."""
        apply_mode(tmp_path, "worker", DESCRIPTION)
        engine = _engine(db)
        await onboarding.start(engine, tmp_path, actor=None, cause=None)
        message = (await _first_turn(engine))["user_message"]
        assert message == onboarding_prompt("worker", DESCRIPTION, read_worker_guide(), **WITH_FACTS)
        assert "Restate the work as you understand it" in message
        assert _form(_describes_work(message)) is None
        assert _form_labels(_names_no_work(message)) == FORM_LABELS
        # A free-text note keeps its section: it is no selection.
        assert f"## What the operator wrote at setup\n\n> {DESCRIPTION}" in message
        assert "chose at setup" not in message

    async def test_a_worker_without_a_note_gets_the_first_form(self, db, tmp_path):
        apply_mode(tmp_path, "worker")
        engine = _engine(db)
        await onboarding.start(engine, tmp_path, actor=None, cause=None)
        message = (await _first_turn(engine))["user_message"]
        assert _form(_first_message(message)) == first_form_arguments(True)
        assert "> (no description was given at setup)" in message

    async def test_the_work_chosen_at_setup_opens_the_conversation(self, db, tmp_path):
        """The Mode step's checkboxes, written to ``TASK.md`` and read back:
        the conversation starts on the chosen work, with its questions."""
        apply_mode(tmp_path, "worker", render_work_description(CHOSEN))
        await ensure_actor_row(db, ALICE)
        engine = _engine(db)
        await onboarding.start(engine, tmp_path, actor=ALICE, cause=ALICE)
        message = (await _first_turn(engine))["user_message"]
        assert message == onboarding_prompt(
            "worker", "", read_worker_guide(), person_name="Alice Example", work=CHOSEN, **WITH_FACTS,
        )
        chosen = _chosen(message)
        assert "- Review pull requests — Reads diffs" in chosen and "- Triage issues — Labels" in chosen
        assert "> also watch the release branch" in chosen and "> never merge anything" in chosen
        assert "wrote at setup" not in message and _form(_first_message(message)) is None
        # TASK.md keeps the selection as written; the prompt only reads it.
        assert work_selection_from(render_work_description(CHOSEN)) == CHOSEN

    async def test_the_agent_greets_the_person_who_finished_setup_by_name(self, db, tmp_path):
        """Finish passes the person as the cause; their display name goes into
        the prompt, so a worker (no ``USER.md``) needs no file to greet them."""
        apply_mode(tmp_path, "worker", DESCRIPTION)
        await ensure_actor_row(db, ALICE)
        engine = _engine(db)
        await onboarding.start(engine, tmp_path, actor=ALICE, cause=ALICE)
        message = (await _first_turn(engine))["user_message"]
        assert message == onboarding_prompt(
            "worker", DESCRIPTION, read_worker_guide(), person_name="Alice Example", **WITH_FACTS,
        )
        assert "The person's name is Alice Example." in message

    async def test_without_a_known_name_the_agent_greets_without_one(self, db, tmp_path):
        apply_mode(tmp_path, "personal")
        engine = _engine(db)
        await onboarding.start(engine, tmp_path, actor=db.system_actor, cause=db.system_actor)
        message = (await _first_turn(engine))["user_message"]
        assert "The person's name is" not in message
        assert "Greet the person without a name" in message

    async def test_the_first_turn_knows_the_instance(self, db, tmp_path, monkeypatch, gathered):
        """The real builder, on the engine's configuration, with the GitHub CLI
        probe replaced: the hidden first message names what is configured and
        the login, and the explorer's brief carries the access."""
        monkeypatch.setattr(onboarding_environment, "gather", _REAL_GATHER)
        probes: list[float] = []

        def _signed_in(timeout):
            probes.append(timeout)
            return {"cli": True, "authenticated": True, "login": "alice", "hint": None}

        monkeypatch.setattr(onboarding_environment, "_github_cli", _signed_in)
        apply_mode(tmp_path, "worker", DESCRIPTION)
        engine = _engine(db)
        engine.config = NerveConfig(workspace=tmp_path, config_dir=tmp_path / "config")
        await onboarding.start(engine, tmp_path, actor=None, cause=None)
        message = (await _first_turn(engine))["user_message"]
        facts = _between(message, "## This instance\n\n", "\n\nThese lines are facts")
        assert facts.splitlines()[0] == "- Mode: worker (an agent for a team)."
        for line in (
            f"- Workspace: `{tmp_path}`; clone repositories into `{tmp_path}/repos/<name>`.",
            "- GitHub CLI (`gh`): authenticated as `alice`.",
            "- Slack: not configured.",
            "- Telegram channel: not configured.",
        ):
            assert line in facts.splitlines(), line
        assert "GitHub access: `gh` as `alice`." in _between(message, "```text\n", "\n```")
        assert len(probes) == 1 and 0 < probes[0] <= onboarding_environment.BUDGET_SECONDS

    async def test_a_failed_build_costs_the_facts_not_the_conversation(self, db, tmp_path, monkeypatch):
        apply_mode(tmp_path, "personal")

        def _fails(config, workspace, **kwargs):
            raise RuntimeError("no facts")

        monkeypatch.setattr(onboarding_environment, "gather", _fails)
        engine = _engine(db)
        state = await onboarding.start(engine, tmp_path, actor=None, cause=None)
        assert state.status == "running"
        assert (await _first_turn(engine))["user_message"] == onboarding_prompt("personal")

    async def test_slow_facts_are_given_up(self, db, tmp_path, monkeypatch):
        """The start waits for the facts no longer than their budget and a
        margin; past that, the conversation opens without them."""
        apply_mode(tmp_path, "personal")

        def _slow(config, workspace, **kwargs):
            time.sleep(0.5)
            return FACTS

        monkeypatch.setattr(onboarding_environment, "gather", _slow)
        monkeypatch.setattr(onboarding_environment, "BUDGET_SECONDS", 0.0)
        monkeypatch.setattr(onboarding, "_FACTS_MARGIN_SECONDS", 0.05)
        engine = _engine(db)
        began = time.monotonic()
        await onboarding.start(engine, tmp_path, actor=None, cause=None)
        assert time.monotonic() - began < 0.4
        assert (await _first_turn(engine))["user_message"] == onboarding_prompt("personal")


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

    async def test_a_worker_from_nerve_init_sets_itself_up_once(self, db, tmp_path, gathered):
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
        assert gathered == [], "the autonomous kind is unchanged: no facts are gathered for it"
        assert turn["internal"] is False and turn["source"] == "cron"
        assert turn["drop_leading_thinking"] is False
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

    async def test_a_selection_that_names_work_is_a_description_for_the_autonomous_kind(self, db, tmp_path):
        """A Mode step selection with items or custom work counts as the task
        description (the session gets the section as written); rules alone
        name no work, so the worker waits for the conversation."""
        for name, work, describes in (
            ("items", WorkSelection(items=("ci-health",)), True),
            ("custom", WorkSelection(custom="Keep the build green."), True),
            ("rules", WorkSelection(rules="Never merge anything."), False),
        ):
            apply_mode(tmp_path / name, "worker", render_work_description(work))
            assert onboarding._describes_work(tmp_path / name) is describes, name
        assert not await onboarding.request_autonomous(db, tmp_path / "rules")
        assert await onboarding.request_autonomous(db, tmp_path / "items")
        engine = _engine(db)
        await onboarding.start_on_boot(engine, tmp_path / "items")
        assert (await _first_turn(engine))["user_message"] == worker_onboarding_prompt(
            render_work_description(WorkSelection(items=("ci-health",))), read_worker_guide(),
        )

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
