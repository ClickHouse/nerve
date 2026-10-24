"""Form tool handler: ask_form.

``ask_form`` shows the person one form of up to six questions in the web chat
and waits for the answers. It pauses through the session's interaction hub
(:mod:`nerve.agent.interactive`), as ``AskUserQuestion`` and ``ExitPlanMode``
do, but from inside a Nerve tool call instead of the CLI's permission
callback:

1. The call is checked for what the schema cannot say (unique ids and option
   labels, options only on choice and multi, a default that fits its
   question) and normalized: every key present, defaults applied, ids
   verbatim.
2. The hub broadcasts an ``interaction`` event of type ``form`` whose
   ``tool_input`` is the normalized form, and waits. The web answers with
   ``answer_interaction`` and ``result: {"answers": {<id>: str | [str]}}``.
3. The answers are checked against their questions and come back twice: one
   readable line per question, then the same answers as one line of JSON,
   ``{"answers": {...}, "skipped": [...]}``.

An invalid call, a channel that cannot show a form (Telegram, cron), a
refusal, a stop and the hub's timeout (one hour) each end the call with an
error result that says what to do next. The Claude CLI ends any MCP tool call
that outlasts its budget; the Claude backend gives Nerve's own server one
above the hub's timeout (``NERVE_TOOL_TIMEOUT_MS`` in
:mod:`nerve.agent.tools.claude_sdk_adapter`), so an unanswered form ends here,
with the result below, and never as a CLI timeout.
"""

from __future__ import annotations

import json
import re
from typing import Any, Iterable

from nerve.agent import interactive
from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    ASK_FORM_SCHEMA,
    FORM_ID_PATTERN,
    FORM_INTRO_MAX,
    FORM_KINDS,
    FORM_LABEL_MAX,
    FORM_MAX_OPTIONS,
    FORM_MAX_QUESTIONS,
    FORM_MIN_OPTIONS,
    FORM_OPTION_KINDS,
    FORM_SINGLE_KINDS,
)

TOOL_NAME = "ask_form"

#: The longest answer kept from one field (a text or url answer, an Other…
#: answer, one list item) and the longest default; the web's inputs stop there.
ANSWER_MAX = 4000

#: Seconds the hub waits for the answers; ``None`` is the hub's own timeout,
#: :data:`nerve.agent.interactive.INTERACTION_TIMEOUT` (one hour).
FORM_TIMEOUT: float | None = None

_ID_RE = re.compile(FORM_ID_PATTERN)
#: ``owner/name``; nothing is looked up on GitHub.
_REPO_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
#: ``http://`` or ``https://`` (lowercase), a host, and no whitespace.
_URL_RE = re.compile(r"https?://[^\s/?#]+\S*")
#: What separates the items of a list given as one string.
_ITEM_SEPARATORS = re.compile(r"[,\n]")

ANSWERED = "The person answered the form."
SKIPPED = "(skipped)"
NOT_INTERACTIVE = (
    "ask_form needs the web chat, and this session runs in a channel without "
    "forms (Telegram, a cron job or a hook). Ask in a plain message instead, or "
    "use ask_user (its answer arrives later as a message in this session)."
)
STOPPED = "No answer: the session stopped while the form was open."

Answer = str | list[str]


# ------------------------------------------------------------------ #
#  The call                                                            #
# ------------------------------------------------------------------ #

def validate_form(args: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """The form the web shows, normalized, and what is wrong with the call.

    The form is ``{"title", "intro", "questions"}``, ``title`` and ``intro``
    stripped (``""`` when absent); every question has every key:
    ``id`` verbatim; ``kind``; ``label``, ``question``, ``help`` and
    ``placeholder`` stripped (``""`` when absent); ``required`` (default
    true); ``options`` as ``{"label", "description"}`` (``[]`` unless the kind
    is choice or multi); ``allow_other`` (default true for choice and multi,
    false for the other kinds); ``default`` as a string (choice, text, url), a
    list of strings (multi, list, repos) or ``None``.

    The problems name the field (``questions[1].options``); none for a valid
    call. A form with problems is never shown.
    """
    problems: list[str] = []
    title = args.get("title")
    if title is None:
        title = ""
    elif not isinstance(title, str):
        problems.append("title: must be a string")
        title = ""
    intro = args.get("intro")
    if intro is None:
        intro = ""
    elif not isinstance(intro, str):
        problems.append("intro: must be a string")
        intro = ""
    intro = intro.strip()
    if len(intro) > FORM_INTRO_MAX:
        problems.append(f"intro: at most {FORM_INTRO_MAX} characters (got {len(intro)})")
    form: dict[str, Any] = {"title": title.strip(), "intro": intro, "questions": []}
    raw_questions = args.get("questions")
    if not isinstance(raw_questions, list):
        problems.append(f"questions: a list of 1 to {FORM_MAX_QUESTIONS} questions")
        return form, problems
    if not 1 <= len(raw_questions) <= FORM_MAX_QUESTIONS:
        problems.append(
            f"questions: 1 to {FORM_MAX_QUESTIONS} questions (got {len(raw_questions)})"
        )
        return form, problems
    seen: set[str] = set()
    for index, raw in enumerate(raw_questions):
        question = _question(f"questions[{index}]", raw, seen, problems)
        if question is not None:
            form["questions"].append(question)
    return form, problems


def _question(
    where: str, raw: Any, seen: set[str], problems: list[str],
) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        problems.append(f"{where}: must be an object")
        return None
    qid = raw.get("id")
    if not isinstance(qid, str) or not _ID_RE.fullmatch(qid):
        problems.append(
            f"{where}.id: a slug of letters, digits, '-' and '_' that starts with a "
            f"letter or digit, at most 64 characters (got {qid!r})"
        )
    elif qid in seen:
        problems.append(f"{where}.id: {qid!r} is the id of an earlier question; ids are unique")
    else:
        seen.add(qid)
    kind = raw.get("kind")
    known = isinstance(kind, str) and kind in FORM_KINDS
    if not known:
        problems.append(f"{where}.kind: one of {', '.join(FORM_KINDS)} (got {kind!r})")
    question: dict[str, Any] = {
        "id": qid,
        "kind": kind,
        "label": _text(raw, "label", where, problems, required=True, limit=FORM_LABEL_MAX),
        "question": _text(raw, "question", where, problems, required=True),
        "help": _text(raw, "help", where, problems),
        "placeholder": _text(raw, "placeholder", where, problems),
        "required": _flag(raw, "required", where, problems),
        "options": _options(raw, kind, where, problems) if known else [],
        "allow_other": _flag(raw, "allow_other", where, problems),
        "default": None,
    }
    if not known or kind not in FORM_OPTION_KINDS:
        question["allow_other"] = False
    if known:
        question["default"] = _default(raw.get("default"), question, f"{where}.default", problems)
    return question


def _text(
    raw: dict[str, Any], key: str, where: str, problems: list[str],
    *, required: bool = False, limit: int | None = None,
) -> str:
    value = raw.get(key)
    if value is None:
        if required:
            problems.append(f"{where}.{key}: required")
        return ""
    if not isinstance(value, str):
        problems.append(f"{where}.{key}: must be a string")
        return ""
    value = value.strip()
    if required and not value:
        problems.append(f"{where}.{key}: must not be empty")
    elif limit is not None and len(value) > limit:
        problems.append(f"{where}.{key}: at most {limit} characters (got {len(value)})")
    return value


def _flag(raw: dict[str, Any], key: str, where: str, problems: list[str]) -> bool:
    """``required`` and ``allow_other``: both default to true."""
    value = raw.get(key)
    if value is None:
        return True
    if not isinstance(value, bool):
        problems.append(f"{where}.{key}: true or false (got {value!r})")
        return True
    return value


def _options(raw: dict[str, Any], kind: str, where: str, problems: list[str]) -> list[dict[str, str]]:
    value = raw.get("options")
    if kind not in FORM_OPTION_KINDS:
        if value not in (None, []):
            problems.append(f"{where}.options: only choice and multi questions have options")
        return []
    if not isinstance(value, list) or not FORM_MIN_OPTIONS <= len(value) <= FORM_MAX_OPTIONS:
        got = f" (got {len(value)})" if isinstance(value, list) else ""
        problems.append(
            f"{where}.options: a {kind} question has {FORM_MIN_OPTIONS} to "
            f"{FORM_MAX_OPTIONS} options{got}"
        )
        return []
    options: list[dict[str, str]] = []
    labels: set[str] = set()
    for index, option in enumerate(value):
        at = f"{where}.options[{index}]"
        if not isinstance(option, dict):
            problems.append(f"{at}: an object with a label")
            continue
        label = _text(option, "label", at, problems, required=True)
        if label and label in labels:
            problems.append(f"{at}.label: {label!r} is already an option; labels are unique")
        labels.add(label)
        options.append({"label": label, "description": _text(option, "description", at, problems)})
    return options


def _default(raw: Any, question: dict[str, Any], at: str, problems: list[str]) -> Answer | None:
    """The prefilled answer, checked like an answer; ``None`` when absent."""
    if raw is None:
        return None
    kind = question["kind"]
    if kind in FORM_SINGLE_KINDS:
        if not isinstance(raw, str):
            problems.append(f"{at}: a {kind} question's default is a string")
            return None
        value = raw.strip()
        if len(value) > ANSWER_MAX:
            problems.append(f"{at}: at most {ANSWER_MAX} characters (got {len(value)})")
            return None
        problem = _value_problem(question, value) if value else ""
        if problem:
            problems.append(f"{at}: {problem} ({value!r})")
            return None
        return value or None
    if isinstance(raw, str):
        raw = _ITEM_SEPARATORS.split(raw)
    if not isinstance(raw, list) or not all(isinstance(item, str) for item in raw):
        problems.append(f"{at}: a {kind} question's default is a list of strings")
        return None
    items = _unique(raw)
    if any(len(item) > ANSWER_MAX for item in items):
        problems.append(f"{at}: at most {ANSWER_MAX} characters per item")
        return None
    bad = [item for item in items if _item_problem(question, item)]
    if bad:
        problems.append(f"{at}: {_item_problem(question, bad[0])}: {', '.join(bad)}")
        return None
    return items or None


def _labels(question: dict[str, Any]) -> set[str]:
    return {option["label"] for option in question["options"]}


def _value_problem(question: dict[str, Any], value: str) -> str:
    """Why a choice, text or url answer is not one; ``""`` when it is."""
    kind = question["kind"]
    if kind == "choice" and not question["allow_other"] and value not in _labels(question):
        return "not one of the options"
    if kind == "url" and not _URL_RE.fullmatch(value):
        return "not an http(s) link"
    return ""


def _item_problem(question: dict[str, Any], item: str) -> str:
    """Why an item of a multi, list or repos answer is not one; ``""`` when it is."""
    kind = question["kind"]
    if kind == "multi" and not question["allow_other"] and item not in _labels(question):
        return "not one of the options"
    if kind == "repos" and not _REPO_RE.fullmatch(item):
        return "not owner/name"
    return ""


def _unique(items: Iterable[str]) -> list[str]:
    """Stripped, without blanks, each once, in the order given."""
    kept: list[str] = []
    for item in items:
        item = item.strip()
        if item and item not in kept:
            kept.append(item)
    return kept


# ------------------------------------------------------------------ #
#  The answers                                                         #
# ------------------------------------------------------------------ #

def read_answers(
    form: dict[str, Any], result: Any,
) -> tuple[dict[str, Answer], list[str], dict[str, list[str]]]:
    """The person's answers, checked against their questions.

    ``result`` is what the web sent with ``answer_interaction``:
    ``{"answers": {<id>: str | [str]}}``; anything else counts as no answers.
    Returns ``answers`` (in question order; a string for choice, text and url,
    a list for multi, list and repos), ``skipped`` (the ids answered with
    nothing: missing, blank or an empty list) and notes, by id, on what was
    cut to :data:`ANSWER_MAX` or left out (a choice that is not an option
    when ``allow_other`` is false, an item that is not ``owner/name``, a link
    that is not http(s)). The web checks all of that before it sends, so only
    a client that skips its checks gets notes.
    """
    raw = result.get("answers") if isinstance(result, dict) else None
    if not isinstance(raw, dict):
        raw = {}
    answers: dict[str, Answer] = {}
    skipped: list[str] = []
    notes: dict[str, list[str]] = {}
    for question in form["questions"]:
        value, found = _answer(question, raw.get(question["id"]))
        if found:
            notes[question["id"]] = found
        if value is None:
            skipped.append(question["id"])
        else:
            answers[question["id"]] = value
    return answers, skipped, notes


def _answer(question: dict[str, Any], raw: Any) -> tuple[Answer | None, list[str]]:
    notes: list[str] = []
    if raw is None:
        return None, notes
    if question["kind"] in FORM_SINGLE_KINDS:
        if isinstance(raw, list):
            raw = ", ".join(_unique(item for item in raw if isinstance(item, str)))
        if not isinstance(raw, str):
            return None, ["left out: not text"]
        value = raw.strip()
        if not value:
            return None, notes
        if len(value) > ANSWER_MAX:
            value = value[:ANSWER_MAX].rstrip()
            notes.append(f"cut to {ANSWER_MAX} characters")
        problem = _value_problem(question, value)
        if problem:
            return None, [*notes, f"left out, {problem}: {value}"]
        return value, notes
    if isinstance(raw, str):
        raw = _ITEM_SEPARATORS.split(raw)
    if not isinstance(raw, list):
        return None, ["left out: not a list"]
    if not all(isinstance(item, str) for item in raw):
        notes.append("left out what was not text")
    items = _unique(item for item in raw if isinstance(item, str))
    if any(len(item) > ANSWER_MAX for item in items):
        items = _unique(item[:ANSWER_MAX] for item in items)
        notes.append(f"cut to {ANSWER_MAX} characters")
    bad = [item for item in items if _item_problem(question, item)]
    if bad:
        notes.append(f"left out, {_item_problem(question, bad[0])}: {', '.join(bad)}")
    kept = [item for item in items if item not in bad]
    return kept or None, notes


def render_answers(
    form: dict[str, Any],
    answers: dict[str, Answer],
    skipped: list[str],
    notes: dict[str, list[str]] | None = None,
) -> str:
    """The tool result: :data:`ANSWERED`, one line per question in order
    (``<label>: <answer>``, a list joined with ", ", :data:`SKIPPED` for no
    answer, a multi-line answer continued on lines indented by two spaces,
    notes in parentheses), a blank line, and the answers as one line of JSON,
    ``{"answers": {...}, "skipped": [...]}``."""
    notes = notes or {}
    lines = [ANSWERED]
    for question in form["questions"]:
        value = answers.get(question["id"])
        if value is None:
            shown = SKIPPED
        else:
            shown = "\n  ".join((", ".join(value) if isinstance(value, list) else value).splitlines())
        line = f"{' '.join(question['label'].split())}: {shown}"
        if question["id"] in notes:
            line += " (" + "; ".join(" ".join(note.split()) for note in notes[question["id"]]) + ")"
        lines.append(line)
    payload = json.dumps({"answers": answers, "skipped": skipped}, ensure_ascii=False)
    return "\n".join(lines) + "\n\n" + payload


# ------------------------------------------------------------------ #
#  The tool                                                            #
# ------------------------------------------------------------------ #

def _invalid(problems: list[str]) -> str:
    return (
        "ask_form: the call is invalid, so nothing was shown to the person.\n"
        + "\n".join(f"- {problem}" for problem in problems)
        + "\nFix these and call ask_form again."
    )


def _timed_out() -> str:
    seconds = interactive.INTERACTION_TIMEOUT if FORM_TIMEOUT is None else FORM_TIMEOUT
    return (
        f"No answer within {interactive._humanize_seconds(int(seconds))}: the person did not "
        "answer the form. Do not ask again in this turn: say in one line that you wait for "
        "them and end your turn, and ask again when they write."
    )


def _closed(message: str) -> str:
    reason = " ".join((message or "").split()).rstrip(".") or "declined"
    return (
        f"No answer: the form was closed without an answer ({reason}). "
        "Ask again only when the person writes."
    )


async def ask_form_handler(ctx: ToolContext, args: dict) -> ToolResult:
    hub = interactive.get_handler(ctx.session_id)
    if hub is None or not hub.interactive_capable:
        return ToolResult.text(NOT_INTERACTIVE, is_error=True)
    form, problems = validate_form(args)
    if problems:
        return ToolResult.text(_invalid(problems), is_error=True)
    outcome = await hub.request_interaction(TOOL_NAME, form, timeout=FORM_TIMEOUT)
    if outcome.cancelled:
        return ToolResult.text(STOPPED, is_error=True)
    if outcome.timed_out:
        return ToolResult.text(_timed_out(), is_error=True)
    if outcome.denied:
        return ToolResult.text(_closed(outcome.message), is_error=True)
    answers, skipped, notes = read_answers(form, outcome.result)
    return ToolResult.text(
        render_answers(form, answers, skipped, notes),
        structured={"answers": answers, "skipped": skipped},
    )


ASK_FORM_SPEC = ToolSpec(
    name=TOOL_NAME,
    description=(
        "Ask the person a form of one to six questions in the web chat and wait for "
        "the answers (up to one hour).\n"
        "Kinds, mixed as the answers need: choice (one option), multi (any of the "
        "options), text (free text), list (several free-text items, such as channels "
        "or topics), repos (GitHub repositories as owner/name) and url (an http(s) "
        "link). Give each question a short label, the question, and its reason in "
        "help. For choice and multi, put the recommended option first with "
        "'(recommended)' at the end of its label; Other… lets the person type an "
        "answer of their own unless allow_other is false.\n"
        "Put what the person should read first (what you understood, what you "
        "intend, why you ask) in intro: the card shows it above the questions, and "
        "the person does not see your thinking.\n"
        "The result has one line per question ('Repositories: org/repo-a, "
        "org/repo-b') and ends with the same answers as JSON: "
        '{"answers": {<id>: string | [string]}, "skipped": [<id>]}: a string for '
        "choice, text and url, a list for multi, list and repos; skipped holds the "
        "optional questions left empty.\n"
        "Only the web chat shows forms: in other channels (Telegram, cron) the call "
        "fails, so ask in a plain message there, or use ask_user. When the person "
        "does not answer within the hour or closes the form, the call fails: say in "
        "one line that you wait, end your turn, and ask again when they write."
    ),
    input_schema=ASK_FORM_SCHEMA,
    handler=ask_form_handler,
)

FORM_SPECS = [ASK_FORM_SPEC]
