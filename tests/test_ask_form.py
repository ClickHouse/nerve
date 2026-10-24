"""ask_form: one form of up to six questions in the web chat, answered through
the interaction hub.

The handler checks the call (what the JSON schema cannot say, with the same
limits), pauses on the session's hub as a ``form`` interaction, and returns
the answers twice: a readable line per question and one line of JSON. A
refusal, a stop, the hub's timeout, a channel without forms and an invalid
call are error results that say what to do next. The Claude CLI's per-call
budget for Nerve's server outlasts the pause.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import jsonschema
import pytest

from nerve.agent import interactive
from nerve.agent.interactive import (
    INTERACTION_TIMEOUT,
    InteractiveToolHandler,
    register_handler,
    unregister_handler,
)
from nerve.agent.tools import build_default_registry
from nerve.agent.tools.claude_sdk_adapter import (
    NERVE_TOOL_TIMEOUT_MS,
    _wrap_for_sdk,
    build_nerve_mcp_server,
    build_session_mcp_server,
)
from nerve.agent.tools.handlers import forms
from nerve.agent.tools.handlers.forms import (
    ANSWER_MAX,
    NOT_INTERACTIVE,
    STOPPED,
    ask_form_handler,
    read_answers,
    render_answers,
    validate_form,
)
from nerve.agent.tools.registry import ToolContext
from nerve.agent.tools.schemas import ASK_FORM_SCHEMA, FORM_KINDS

SID = "sess-form"

REPOS = {"id": "repos", "kind": "repos", "label": "Repositories", "question": "Which repositories should I review?"}
CADENCE = {
    "id": "cadence", "kind": "choice", "label": "Cadence",
    "question": "How often should I look for new pull requests?",
    "options": [{"label": "Every 30 minutes (recommended)"}, {"label": "Hourly"}],
}
NOTES = {"id": "notes", "kind": "text", "label": "Notes", "question": "Anything else?", "required": False}
CALL = {"questions": [REPOS, CADENCE, NOTES]}
ANSWERS = {"answers": {"repos": ["org/repo-a", "org/repo-b"], "cadence": "Every 30 minutes (recommended)"}}

# The exact text the web parses (JSON on the last line, the lines as fallback).
ANSWERED_TEXT = (
    "The person answered the form.\n"
    "Repositories: org/repo-a, org/repo-b\n"
    "Cadence: Every 30 minutes (recommended)\n"
    "Notes: (skipped)\n"
    "\n"
    '{"answers": {"repos": ["org/repo-a", "org/repo-b"], "cadence": "Every 30 minutes (recommended)"}, '
    '"skipped": ["notes"]}'
)


def _call(*questions: dict, **top) -> dict:
    return {"questions": list(questions), **top}


def _problems(args: dict) -> list[str]:
    return validate_form(args)[1]


def _form(args: dict) -> dict:
    form, problems = validate_form(args)
    assert problems == []
    return form


def _choice(**extra) -> dict:
    return {**CADENCE, **extra}


# ------------------------------------------------------------------ #
#  The call                                                            #
# ------------------------------------------------------------------ #


class TestTheCall:
    def test_a_valid_call_is_normalized_for_the_web(self):
        form = _form(_call(
            {**REPOS, "label": "  Repositories ", "help": " so the explorer can clone them ",
             "placeholder": "org/repo"},
            {**CADENCE, "options": [{"label": " Every 30 minutes (recommended) ", "description": " fast "},
                                    {"label": "Hourly"}], "default": "Hourly"},
            {"id": "Notes_2-x", "kind": "text", "label": "Notes", "question": "Anything else?",
             "required": False, "allow_other": True},
            title=" Setup questions ",
        ))
        assert form == {
            "title": "Setup questions",
            "questions": [
                {"id": "repos", "kind": "repos", "label": "Repositories",
                 "question": "Which repositories should I review?", "help": "so the explorer can clone them",
                 "placeholder": "org/repo", "required": True, "options": [], "allow_other": False,
                 "default": None},
                {"id": "cadence", "kind": "choice", "label": "Cadence",
                 "question": "How often should I look for new pull requests?", "help": "", "placeholder": "",
                 "required": True,
                 "options": [{"label": "Every 30 minutes (recommended)", "description": "fast"},
                             {"label": "Hourly", "description": ""}],
                 "allow_other": True, "default": "Hourly"},
                # The id stays verbatim; allow_other means nothing for text.
                {"id": "Notes_2-x", "kind": "text", "label": "Notes", "question": "Anything else?",
                 "help": "", "placeholder": "", "required": False, "options": [], "allow_other": False,
                 "default": None},
            ],
        }

    @pytest.mark.parametrize("kind", FORM_KINDS)
    def test_every_kind_is_a_question(self, kind):
        question = {"id": "q", "kind": kind, "label": "Q", "question": "?"}
        if kind in ("choice", "multi"):
            question["options"] = [{"label": "a"}, {"label": "b"}]
        assert _form(_call(question))["questions"][0]["kind"] == kind

    def test_one_to_six_questions(self):
        many = [{**NOTES, "id": f"q{n}"} for n in range(7)]
        assert _problems(_call()) == ["questions: 1 to 6 questions (got 0)"]
        assert _problems(_call(*many)) == ["questions: 1 to 6 questions (got 7)"]
        assert _problems(_call(*many[:6])) == []
        assert _problems({}) == ["questions: a list of 1 to 6 questions"]
        assert _problems({"questions": "repos"}) == ["questions: a list of 1 to 6 questions"]

    @pytest.mark.parametrize("bad", [" repos", "repos ", "re pos", "-repos", "_x", "", "a" * 65, "rép", 7, None])
    def test_an_id_is_a_slug_never_rewritten(self, bad):
        problems = _problems(_call({**REPOS, "id": bad}))
        assert len(problems) == 1 and problems[0].startswith("questions[0].id: a slug"), problems

    def test_ids_are_unique(self):
        assert _problems(_call(REPOS, {**NOTES, "id": "repos"})) == [
            "questions[1].id: 'repos' is the id of an earlier question; ids are unique",
        ]
        assert _form(_call({**REPOS, "id": "A" * 64}))["questions"][0]["id"] == "A" * 64

    @pytest.mark.parametrize("bad", ["select", "Choice", "", None, ["choice"], {"kind": "text"}])
    def test_the_kind_is_one_of_six(self, bad):
        problems = _problems(_call({**CADENCE, "kind": bad}))
        assert problems == [
            f"questions[0].kind: one of choice, multi, text, list, repos, url (got {bad!r})",
        ]

    def test_a_label_is_a_short_chip(self):
        assert _problems(_call({**REPOS, "label": "x" * 40})) == []
        assert _problems(_call({**REPOS, "label": "x" * 41})) == [
            "questions[0].label: at most 40 characters (got 41)",
        ]
        assert _problems(_call({**REPOS, "label": "   "})) == ["questions[0].label: must not be empty"]
        without = {key: value for key, value in REPOS.items() if key != "label"}
        assert _problems(_call(without)) == ["questions[0].label: required"]
        assert _problems(_call({**REPOS, "question": 3})) == ["questions[0].question: must be a string"]

    @pytest.mark.parametrize("count", [2, 8])
    def test_choice_and_multi_take_two_to_eight_options(self, count):
        for kind in ("choice", "multi"):
            options = [{"label": f"option {n}"} for n in range(count)]
            assert _problems(_call(_choice(kind=kind, options=options))) == []

    @pytest.mark.parametrize("count", [0, 1, 9])
    def test_fewer_than_two_or_more_than_eight_options_is_invalid(self, count):
        options = [{"label": f"option {n}"} for n in range(count)]
        assert _problems(_call(_choice(options=options))) == [
            f"questions[0].options: a choice question has 2 to 8 options (got {count})",
        ]
        missing = {key: value for key, value in CADENCE.items() if key != "options"}
        assert _problems(_call({**missing, "kind": "multi"})) == [
            "questions[0].options: a multi question has 2 to 8 options",
        ]

    def test_option_labels_are_present_and_unique(self):
        assert _problems(_call(_choice(options=[{"label": "a"}, {"label": " a "}]))) == [
            "questions[0].options[1].label: 'a' is already an option; labels are unique",
        ]
        assert _problems(_call(_choice(options=[{"label": "a"}, {"description": "b"}]))) == [
            "questions[0].options[1].label: required",
        ]
        assert _problems(_call(_choice(options=[{"label": "a"}, "b"]))) == [
            "questions[0].options[1]: an object with a label",
        ]

    def test_only_choice_and_multi_have_options(self):
        assert _problems(_call({**NOTES, "options": [{"label": "a"}, {"label": "b"}]})) == [
            "questions[0].options: only choice and multi questions have options",
        ]
        assert _form(_call({**NOTES, "options": []}))["questions"][0]["options"] == []

    def test_flags_are_booleans(self):
        assert _problems(_call({**NOTES, "required": "no"})) == [
            "questions[0].required: true or false (got 'no')",
        ]
        assert _form(_call(_choice(allow_other=False)))["questions"][0]["allow_other"] is False

    def test_a_default_fits_its_question(self):
        def default(question: dict) -> object:
            return _form(_call(question))["questions"][0]["default"]

        assert default(_choice(default="Hourly")) == "Hourly"
        assert default(_choice(default="Every 15 minutes")) == "Every 15 minutes"  # Other…
        assert default({**REPOS, "default": "org/repo-a, org/repo-b\norg/repo-a"}) == ["org/repo-a", "org/repo-b"]
        assert default({**REPOS, "kind": "list", "default": [" #ops ", "", "#alerts"]}) == ["#ops", "#alerts"]
        assert default({**NOTES, "default": "  "}) is None
        assert default({**NOTES, "kind": "url", "default": "https://docs.example.com/a?b=c"}) == (
            "https://docs.example.com/a?b=c"
        )
        assert _problems(_call(_choice(allow_other=False, default="Every 15 minutes"))) == [
            "questions[0].default: not one of the options ('Every 15 minutes')",
        ]
        assert _problems(_call({**REPOS, "default": ["org/repo-a", "repo-b"]})) == [
            "questions[0].default: not owner/name: repo-b",
        ]
        assert _problems(_call({**NOTES, "kind": "url", "default": "docs.example.com"})) == [
            "questions[0].default: not an http(s) link ('docs.example.com')",
        ]
        assert _problems(_call({**NOTES, "default": ["a"]})) == [
            "questions[0].default: a text question's default is a string",
        ]
        assert _problems(_call({**REPOS, "default": [1]})) == [
            "questions[0].default: a repos question's default is a list of strings",
        ]
        assert _problems(_call({**NOTES, "default": "x" * (ANSWER_MAX + 1)})) == [
            f"questions[0].default: at most {ANSWER_MAX} characters (got {ANSWER_MAX + 1})",
        ]

    def test_every_problem_is_named_at_once(self):
        problems = _problems(_call(
            {**REPOS, "label": "x" * 41},
            {**CADENCE, "options": [{"label": "only one"}]},
            {**NOTES, "id": "repos"},
            title=5,
        ))
        assert problems == [
            "title: must be a string",
            "questions[0].label: at most 40 characters (got 41)",
            "questions[1].options: a choice question has 2 to 8 options (got 1)",
            "questions[2].id: 'repos' is the id of an earlier question; ids are unique",
        ]

    def test_the_json_schema_carries_the_same_limits(self):
        """The SDK checks the schema before the handler runs; both agree."""
        jsonschema.validate(CALL, ASK_FORM_SCHEMA)
        for bad in (
            _call(),
            _call(*[{**NOTES, "id": f"q{n}"} for n in range(7)]),
            _call({**CADENCE, "kind": "select"}),
            _call({**REPOS, "label": "x" * 41}),
            _call(_choice(options=[{"label": "a"}])),
            _call(_choice(options=[{"label": str(n)} for n in range(9)])),
            _call({**REPOS, "id": "re pos"}),
            _call({key: value for key, value in REPOS.items() if key != "question"}),
        ):
            with pytest.raises(jsonschema.ValidationError):
                jsonschema.validate(bad, ASK_FORM_SCHEMA)
            assert _problems(bad), bad


# ------------------------------------------------------------------ #
#  The answers                                                         #
# ------------------------------------------------------------------ #


def _read(args: dict, answers: dict) -> tuple[dict, list[str], dict]:
    return read_answers(_form(args), {"answers": answers})


class TestTheAnswers:
    def test_lines_then_json(self):
        form = _form(CALL)
        answers, skipped, notes = read_answers(form, ANSWERS)
        assert render_answers(form, answers, skipped, notes) == ANSWERED_TEXT
        assert json.loads(ANSWERED_TEXT.splitlines()[-1]) == {
            "answers": {"repos": ["org/repo-a", "org/repo-b"], "cadence": "Every 30 minutes (recommended)"},
            "skipped": ["notes"],
        }

    @pytest.mark.parametrize("empty", [None, "", "   ", [], ["", "  "]])
    def test_an_empty_answer_is_skipped(self, empty):
        answers, skipped, notes = _read(_call(REPOS, NOTES), {"repos": empty, "notes": empty})
        assert (answers, skipped, notes) == ({}, ["repos", "notes"], {})

    def test_strings_for_choice_text_and_url_lists_for_the_rest(self):
        answers, skipped, _notes = _read(
            _call(
                CADENCE,
                {**NOTES, "id": "text"},
                {**NOTES, "id": "link", "kind": "url"},
                {**CADENCE, "id": "multi", "kind": "multi"},
                {**NOTES, "id": "channels", "kind": "list"},
                REPOS,
            ),
            {
                "cadence": " Hourly ", "text": " line ", "link": "https://example.com/x",
                "multi": ["Hourly", "Every 15 minutes", "Hourly"], "channels": [" #ops", "#ops ", "#alerts"],
                "repos": ["org/repo-a", " org/repo-b ", "org/repo-a"],
            },
        )
        assert answers == {
            "cadence": "Hourly", "text": "line", "link": "https://example.com/x",
            # Other… text is one more item; duplicates and blanks go.
            "multi": ["Hourly", "Every 15 minutes"], "channels": ["#ops", "#alerts"],
            "repos": ["org/repo-a", "org/repo-b"],
        }
        assert skipped == []

    def test_what_breaks_the_contract_is_left_out_with_a_note(self):
        """The web checks before it sends; another client's mistakes never
        reach the JSON, and the line says what was left out."""
        args = _call(
            REPOS,
            {**NOTES, "id": "link", "kind": "url", "label": "Docs"},
            _choice(allow_other=False),
            _choice(id="multi", kind="multi", label="Kinds", allow_other=False),
        )
        form = _form(args)
        answers, skipped, notes = read_answers(form, {"answers": {
            "repos": ["org/repo-a", "not a repo", "https://github.com/org/repo-b"],
            "link": "ftp://example.com",
            "cadence": "Every 15 minutes",
            "multi": ["Hourly", "Daily"],
        }})
        assert answers == {"repos": ["org/repo-a"], "multi": ["Hourly"]}
        assert skipped == ["link", "cadence"]
        assert render_answers(form, answers, skipped, notes).splitlines()[1:5] == [
            "Repositories: org/repo-a (left out, not owner/name: not a repo, https://github.com/org/repo-b)",
            "Docs: (skipped) (left out, not an http(s) link: ftp://example.com)",
            "Cadence: (skipped) (left out, not one of the options: Every 15 minutes)",
            "Kinds: Hourly (left out, not one of the options: Daily)",
        ]

    def test_other_text_is_any_answer_while_allow_other_holds(self):
        answers, _skipped, notes = _read(_call(CADENCE), {"cadence": "Every 15 minutes"})
        assert answers == {"cadence": "Every 15 minutes"} and notes == {}

    def test_a_long_answer_is_cut(self):
        answers, _skipped, notes = _read(_call(NOTES), {"notes": "x" * (ANSWER_MAX + 10)})
        assert answers == {"notes": "x" * ANSWER_MAX}
        assert notes == {"notes": [f"cut to {ANSWER_MAX} characters"]}

    def test_a_multi_line_answer_continues_indented(self):
        form = _form(_call(NOTES))
        answers, skipped, notes = read_answers(form, {"answers": {"notes": "first line\nsecond line"}})
        text = render_answers(form, answers, skipped, notes)
        assert "Notes: first line\n  second line\n\n" in text
        assert json.loads(text.splitlines()[-1])["answers"] == {"notes": "first line\nsecond line"}

    def test_unicode_stays_readable_in_the_json(self):
        form = _form(_call(NOTES))
        answers, skipped, notes = read_answers(form, {"answers": {"notes": "Grüße — 你好"}})
        assert render_answers(form, answers, skipped, notes).endswith(
            '{"answers": {"notes": "Grüße — 你好"}, "skipped": []}'
        )

    @pytest.mark.parametrize("result", [None, {}, {"answers": "org/repo-a"}, ["org/repo-a"], {"repos": ["org/a"]}])
    def test_no_answers_object_is_all_skipped(self, result):
        answers, skipped, _notes = read_answers(_form(CALL), result)
        assert answers == {} and skipped == ["repos", "cadence", "notes"]

    def test_unknown_ids_are_ignored(self):
        answers, skipped, _notes = _read(_call(NOTES), {"notes": "hi", "other": "x"})
        assert answers == {"notes": "hi"} and skipped == []


# ------------------------------------------------------------------ #
#  Through the hub                                                     #
# ------------------------------------------------------------------ #


@pytest.fixture
def hub():
    """A web session's hub, registered; ``hub.messages`` holds what it broadcast."""
    messages: list[tuple[str, dict]] = []

    async def broadcast(channel: str, message: dict) -> None:
        messages.append((channel, message))

    handler = InteractiveToolHandler(SID, broadcast, interactive_capable=True)
    handler.messages = messages  # type: ignore[attr-defined]
    register_handler(SID, handler)
    yield handler
    unregister_handler(SID)


async def _ask(hub: InteractiveToolHandler, args: dict) -> tuple[asyncio.Task, dict]:
    """Start the tool call, wait until the form is out, return it and the event."""
    task = asyncio.create_task(ask_form_handler(ToolContext(session_id=SID), args))
    for _ in range(400):
        if hub.has_pending:
            break
        await asyncio.sleep(0.005)
    else:
        raise AssertionError(f"the form never paused: {task}")
    event = next(m for (_c, m) in hub.messages if m["type"] == "interaction")
    return task, event


@pytest.mark.asyncio
class TestThroughTheHub:
    async def test_the_form_waits_for_the_person_and_returns_the_answers(self, hub):
        task, event = await _ask(hub, CALL)
        assert event == {
            "type": "interaction", "session_id": SID, "interaction_id": event["interaction_id"],
            "interaction_type": "form", "tool_name": "ask_form", "tool_input": _form(CALL), "agent": None,
        }
        assert hub.pending_interaction_types() == ["form"]
        assert ("__global__", {"type": "session_awaiting_input", "session_id": SID, "awaiting": True}) in hub.messages
        assert not task.done()

        assert hub.resolve(event["interaction_id"], ANSWERS)
        result = await task
        assert result.is_error is False
        assert result.content == [{"type": "text", "text": ANSWERED_TEXT}]
        assert result.structured == json.loads(ANSWERED_TEXT.splitlines()[-1])
        assert hub.pending_interaction_types() == []
        assert hub.messages[-1][1] == {
            "type": "interaction_resolved", "session_id": SID, "interaction_id": event["interaction_id"],
        }

    async def test_a_refusal_is_no_answer(self, hub):
        task, event = await _ask(hub, CALL)
        hub.deny(event["interaction_id"], "Not now.")
        result = await task
        assert result.is_error is True
        assert result.text_content == (
            "No answer: the form was closed without an answer (Not now). "
            "Ask again only when the person writes."
        )

    async def test_stop_closes_the_form(self, hub):
        """Stop cancels every pending interaction (``cancel_all``)."""
        task, _event = await _ask(hub, CALL)
        hub.cancel_all()
        result = await task
        assert result.is_error is True
        assert result.text_content.startswith("No answer: the form was closed without an answer (Session stopped).")

    async def test_a_cancelled_call_is_no_answer(self, hub):
        task, _event = await _ask(hub, CALL)
        task.cancel()
        result = await task
        assert result.is_error is True and result.text_content == STOPPED

    async def test_no_answer_within_the_timeout(self, hub, monkeypatch):
        monkeypatch.setattr(forms, "FORM_TIMEOUT", 0.1)
        task, _event = await _ask(hub, CALL)
        result = await asyncio.wait_for(task, timeout=5)
        assert result.is_error is True
        assert result.text_content.startswith("No answer within ")
        assert "Do not ask again in this turn" in result.text_content
        assert hub.pending_interaction_types() == []

    async def test_the_timeout_text_names_the_hour(self):
        assert forms.FORM_TIMEOUT is None
        assert forms._timed_out() == (
            "No answer within 1 hour: the person did not answer the form. Do not ask again in this "
            "turn: say in one line that you wait for them and end your turn, and ask again when they write."
        )

    @pytest.mark.parametrize("capable", [False, None])
    async def test_a_channel_without_forms_gets_guidance(self, capable):
        """Telegram, cron and hooks have a hub that cannot pause (or none):
        nothing is broadcast, and the error says how to ask instead."""
        messages: list = []

        async def broadcast(channel: str, message: dict) -> None:
            messages.append(message)

        if capable is not None:
            register_handler(SID, InteractiveToolHandler(SID, broadcast, interactive_capable=capable))
        try:
            result = await ask_form_handler(ToolContext(session_id=SID), CALL)
        finally:
            unregister_handler(SID)
        assert result.is_error is True and result.text_content == NOT_INTERACTIVE
        assert "plain message" in NOT_INTERACTIVE and "ask_user" in NOT_INTERACTIVE
        assert messages == []

    async def test_the_permission_callback_lets_the_call_through(self, hub):
        """The handler pauses; the CLI's permission callback must not pause
        first (``ask_form`` is no CLI built-in), or the person would be asked
        twice."""
        from claude_agent_sdk.types import PermissionResultAllow

        from nerve.agent.backends.claude import ClaudeToolPermissions

        result = await ClaudeToolPermissions(hub).can_use_tool("mcp__nerve__ask_form", CALL, None)
        assert isinstance(result, PermissionResultAllow) and result.updated_input is None
        assert hub.messages == [] and not hub.has_pending

    async def test_an_invalid_call_shows_nothing(self, hub):
        result = await ask_form_handler(
            ToolContext(session_id=SID), _call({**REPOS, "label": "x" * 41}, {**NOTES, "id": "repos"}),
        )
        assert result.is_error is True
        assert result.text_content == (
            "ask_form: the call is invalid, so nothing was shown to the person.\n"
            "- questions[0].label: at most 40 characters (got 41)\n"
            "- questions[1].id: 'repos' is the id of an earlier question; ids are unique\n"
            "Fix these and call ask_form again."
        )
        assert hub.messages == [] and not hub.has_pending


# ------------------------------------------------------------------ #
#  Registration and the Claude CLI's budget                            #
# ------------------------------------------------------------------ #


class TestRegistration:
    def test_a_default_tool(self):
        spec = build_default_registry().get("ask_form")
        assert spec is not None and spec.input_schema is ASK_FORM_SCHEMA
        assert spec.description.split("\n")[0] == (
            "Ask the person a form of one to six questions in the web chat and wait for the answers "
            "(up to one hour)."
        )

    @pytest.mark.asyncio
    async def test_the_sdk_wrapper_returns_the_error_flag(self):
        spec = build_default_registry().get("ask_form")
        out = await _wrap_for_sdk(spec, ToolContext(session_id="no-hub-here")).handler(CALL)
        assert out == {"content": [{"type": "text", "text": NOT_INTERACTIVE}], "is_error": True}


def _claude_backend(tmp_path: Path):
    from nerve.agent.backends.claude import ClaudeBackend
    from nerve.config import NerveConfig

    cfg = NerveConfig.from_dict({"workspace": str(tmp_path)})
    return ClaudeBackend(SimpleNamespace(
        config=lambda: cfg,
        claude_plugins=lambda: [],
        managed_mcp=False,
        registry=build_default_registry(),
        tool_ctx_factory=lambda session_id: ToolContext(session_id=session_id),
        external_mcp_servers=lambda: [],
    ))


class TestTheCliBudget:
    """The CLI ends any MCP call that runs past the server's ``timeout`` (or
    ``MCP_TOOL_TIMEOUT``); Nerve's server outlasts the hub's own timeout."""

    def test_nerve_servers_outlast_the_pause(self):
        assert NERVE_TOOL_TIMEOUT_MS == 3_700_000
        assert type(NERVE_TOOL_TIMEOUT_MS) is int  # the CLI takes a positive integer only
        assert NERVE_TOOL_TIMEOUT_MS > INTERACTION_TIMEOUT * 1000
        session = build_session_mcp_server(build_default_registry(), ToolContext(session_id=SID))
        legacy = build_nerve_mcp_server(build_default_registry())
        for server in (session, legacy):
            assert server["timeout"] == NERVE_TOOL_TIMEOUT_MS
            assert server["alwaysLoad"] is True and server["type"] == "sdk" and server["name"] == "nerve"

    def test_the_budget_reaches_the_cli_command_line(self, tmp_path):
        """Through the backend's options and the SDK's own argv builder."""
        from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport

        from nerve.agent.backends.base import SessionSpec

        backend = _claude_backend(tmp_path)
        spec = SessionSpec(
            session_id=SID, source="web", model="claude-opus-5", effort="high",
            system_prompt="prompt", cwd=str(tmp_path),
        )
        with patch.object(backend, "_build_hooks", return_value={}):
            options = backend._build_options(spec)
        options.cli_path = "/nonexistent/claude"  # skip the CLI discovery walk
        argv = SubprocessCLITransport(prompt="hi", options=options)._build_command()
        servers = json.loads(argv[argv.index("--mcp-config") + 1])["mcpServers"]
        assert servers["nerve"] == {
            "type": "sdk", "name": "nerve", "alwaysLoad": True, "timeout": NERVE_TOOL_TIMEOUT_MS,
        }

    def test_the_hub_names_a_form(self):
        assert interactive._interaction_type("ask_form") == "form"
