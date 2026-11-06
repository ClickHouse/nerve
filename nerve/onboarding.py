"""The onboarding: its state, its start and its end.

After setup in the browser, the agent is set up by talking to it (the
``conversation`` kind). :func:`start` opens one web chat session for that and
gives the agent a hidden first message
(:func:`nerve.agent.prompts.onboarding_prompt`), so the agent speaks first and
the person sees only its answer. The message carries the work a worker's
operator chose at setup (the Mode step's selection, read back from
``TASK.md``) and what Nerve knows about the instance
(:mod:`nerve.onboarding_environment`), because the agent's first turn calls no
tool before it speaks. The agent calls the ``onboarding_done`` tool
after the person confirmed the setup and the agent applied it
(:func:`complete`). A person can skip onboarding on the setup page
(:func:`skip`).

A worker set up without the browser (``nerve init --terminal`` or
``--non-interactive``) with a task description gets the ``autonomous`` kind
(:func:`request_autonomous`): at the next daemon start with a model
credential, one session without a person reads the description and sets the
worker up (:func:`nerve.agent.prompts.worker_onboarding_prompt`), then calls
``onboarding_done`` too.

The state is the ``onboarding`` row (migration v060). Without a row, the kind
is ``conversation``, and a worker whose ``TASK.md`` no longer carries the
onboarding marker is done (installs from before the row).

:func:`read_state` gives one of ``not_applicable`` (no mode chosen), ``pending``,
``running``, ``done`` or ``skipped``, and the kind.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
import weakref
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from nerve.identity import ACTOR_KIND_HUMAN, Actor, actor_id_of
from nerve.setup_writer import (
    TASK_ONBOARDING_MARKER,
    publish_text,
    render_task_md,
    task_description_from,
    task_onboarding_pending,
    work_selection_from,
    workspace_mode,
)

if TYPE_CHECKING:
    from nerve.agent.engine import AgentEngine
    from nerve.db import Database
    from nerve.identity import ActorOrId
    from nerve.onboarding_catalog import WorkSelection
    from nerve.onboarding_environment import InstanceFacts

logger = logging.getLogger(__name__)

NOT_APPLICABLE = "not_applicable"
PENDING = "pending"
RUNNING = "running"
DONE = "done"
SKIPPED = "skipped"
# Stored only: the next daemon start opens the onboarding (reads as pending).
REQUESTED = "requested"

CONVERSATION = "conversation"
AUTONOMOUS = "autonomous"

SESSION_TITLE = "Onboarding"

#: How much longer than the facts' own budget a start waits for them.
_FACTS_MARGIN_SECONDS = 2.0

#: Nerve's form tool as a stored reply names it (both backends).
_FORM_TOOL = "mcp__nerve__ask_form"

# One start at a time in this process, so a second caller sees the session
# that the first one created. One lock per event loop: tests run many loops.
_start_locks: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock]" = (
    weakref.WeakKeyDictionary()
)


def _start_lock() -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    if loop not in _start_locks:
        _start_locks[loop] = asyncio.Lock()
    return _start_locks[loop]


@dataclass(frozen=True)
class OnboardingState:
    status: str
    session_id: str | None = None
    #: The stored ``(status, session_id)``, for a compare-and-set write.
    stored: tuple[str | None, str | None] = (None, None)
    kind: str = CONVERSATION


def _task_md(workspace: Path) -> Path:
    return Path(workspace) / "TASK.md"


def _worker_task_pending(workspace: Path) -> bool:
    """A worker's ``TASK.md`` still waits for onboarding (or does not exist)."""
    try:
        return task_onboarding_pending(_task_md(workspace).read_text(encoding="utf-8"))
    except OSError:
        return True


async def read_state(db: "Database", workspace: Path) -> OnboardingState:
    """The onboarding state of this install (see the module docstring)."""
    mode = workspace_mode(Path(workspace))
    if mode is None:
        return OnboardingState(NOT_APPLICABLE)
    row = await db.get_onboarding()
    stored = (row["status"], row["session_id"]) if row else (None, None)
    kind = (row or {}).get("kind") or CONVERSATION
    status, session_id = stored
    if status in (DONE, SKIPPED):
        return OnboardingState(status, session_id, stored, kind)
    if mode == "worker" and not await asyncio.to_thread(_worker_task_pending, workspace):
        return OnboardingState(DONE, session_id, stored, kind)
    if status == RUNNING and session_id and await db.get_session(session_id):
        return OnboardingState(RUNNING, session_id, stored, kind)
    return OnboardingState(PENDING, None, stored, kind)


def _prompt_inputs(workspace: Path, mode: str) -> "tuple[str, str, WorkSelection | None]":
    """The worker's description, guide and work selection for the prompt.
    Blocking file I/O.

    The description is the ``TASK.md`` section as written; the selection is
    what it holds (:func:`nerve.setup_writer.work_selection_from`: the Mode
    step's catalog items, custom work and rules, or a free-text note). A
    worker without ``TASK.md``, or with the raw ``# Task`` file of an older
    install, gets the template first, so the agent fills it in place.
    """
    from nerve.workspace import read_worker_guide

    if mode != "worker":
        return "", "", None
    path = _task_md(workspace)
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raw = ""
    description = task_description_from(raw) or "" if raw else ""
    if TASK_ONBOARDING_MARKER not in raw and (not raw or task_onboarding_pending(raw)):
        publish_text(path, render_task_md(description))
    return description, read_worker_guide(), work_selection_from(description)


#: The line of ``IDENTITY.md`` that names the agent.
_IDENTITY_NAME = "- **Name:**"


def _agent_name(workspace: Path) -> str:
    """The name the agent gives itself in its first message, else ``""``.
    Blocking file I/O.

    The name line of ``IDENTITY.md`` once it is filled in (About you), else
    ``NERVE_DISPLAY_NAME``, the agent's name that a hosting platform sets.
    A template placeholder is no name.
    """
    try:
        lines = (Path(workspace) / "IDENTITY.md").read_text(encoding="utf-8").splitlines()
    except OSError:
        lines = []
    for line in lines:
        if line.startswith(_IDENTITY_NAME):
            value = line[len(_IDENTITY_NAME):].strip()
            if value and not value.startswith("*(") and "{{" not in value:
                return value
            break
    return os.environ.get("NERVE_DISPLAY_NAME", "").strip()


async def _instance_facts(engine: "AgentEngine", workspace: Path) -> "InstanceFacts | None":
    """What the conversation's agent knows about this instance from the start.

    :func:`nerve.onboarding_environment.gather` in a thread: a few small
    files and one ``gh auth status``, within
    :data:`nerve.onboarding_environment.BUDGET_SECONDS`. ``None`` when it
    cannot be had in time; the prompt then goes without it.
    """
    from nerve import onboarding_environment

    config = getattr(engine, "config", None)
    if config is None:
        return None
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(onboarding_environment.gather, config, workspace),
            onboarding_environment.BUDGET_SECONDS + _FACTS_MARGIN_SECONDS,
        )
    except Exception:  # noqa: BLE001 - the conversation opens without the facts
        logger.warning("Onboarding: the facts about this instance are unavailable", exc_info=True)
        return None


async def _person_name(db: "Database", cause: "ActorOrId | None") -> str:
    """The display name of the person the conversation is for, else ``""``.

    ``cause`` is the person who finished setup: the resolved :class:`Actor`
    at Finish, or their stored actor id at a daemon start. The system actor,
    an unknown id and a person without a display name give ``""`` (the agent
    then greets without a name).
    """
    if isinstance(cause, Actor):
        if not cause.is_human:
            return ""
        if cause.display_name:
            return cause.display_name
    actor_id = actor_id_of(cause) if isinstance(cause, (Actor, str)) else None
    if not actor_id:
        return ""
    try:
        ref = await db.get_actor_ref(actor_id)
    except Exception:  # noqa: BLE001 - a missing name never stops the start
        logger.debug("Onboarding: no name for actor %s", actor_id, exc_info=True)
        return ""
    if not ref or ref.get("kind") != ACTOR_KIND_HUMAN:
        return ""
    return ref.get("display_name") or ""


async def _form_shown(db: "Database", session_id: str) -> bool:
    """The session's stored replies hold an ``ask_form`` call that did not
    fail: the person saw a form, answered or still open. An invalid call
    shows nothing, so it does not count; in a session without the web chat
    (the autonomous kind) every call fails."""
    try:
        messages = await db.get_messages(session_id)
    except Exception:  # noqa: BLE001 - unknown counts as not shown
        logger.debug("Onboarding: could not read the replies of %s", session_id, exc_info=True)
        return False
    return any(
        isinstance(block, dict) and block.get("type") == "tool_call"
        and block.get("tool") == _FORM_TOOL and not block.get("is_error")
        for message in messages if message.get("role") == "assistant"
        for block in message.get("blocks") or ()
    )


async def start(
    engine: "AgentEngine",
    workspace: Path,
    *,
    actor,
    cause: "ActorOrId | None",
    kind: str = CONVERSATION,
) -> OnboardingState:
    """Open the onboarding if it is pending. Idempotent.

    ``kind`` ``conversation`` opens a web chat with a hidden first message,
    and the chat shows no thinking from before the agent's first text or
    tool call; ``autonomous`` runs the worker's onboarding session without a
    person, with its prompt in the transcript, as a ``cron`` source (the
    backend and model of scheduled jobs).

    Starts in this process run one at a time, and the claim of the row is a
    compare-and-set from the state that was read, so there is one session
    for each opening. The agent's first turn runs in the background. When
    that turn fails (a model error) before the person saw a form, the row
    goes back to ``requested``: the next Finish or daemon start opens a new
    one. A conversation whose first turn fails after the person saw its
    form (:func:`_form_shown`) stays ``running``: the turn goes on after the
    form's answer, so the person continues in the same chat.
    """
    from nerve.agent.prompts import onboarding_prompt, worker_onboarding_prompt

    db = engine.db
    async with _start_lock():
        state = await read_state(db, workspace)
        if state.status != PENDING:
            return state
        session_id = f"onboarding-{uuid.uuid4().hex[:8]}"
        actor_id = actor_id_of(cause)
        if not await db.set_onboarding(
            RUNNING, session_id=session_id, actor_id=actor_id, expect=state.stored, kind=kind,
        ):
            return await read_state(db, workspace)
        source = "web" if kind == CONVERSATION else "cron"
        await engine.sessions.get_or_create(
            session_id, title=SESSION_TITLE, source=source, actor=actor, cause=cause,
        )
    mode = workspace_mode(Path(workspace)) or "personal"
    description, guide, work = await asyncio.to_thread(_prompt_inputs, workspace, mode)
    if kind == CONVERSATION:
        name = await _person_name(db, cause)
        agent_name = await asyncio.to_thread(_agent_name, workspace)
        # The first turn calls no tool before its message: Nerve hands the
        # agent what is configured, and from where, instead.
        facts = await _instance_facts(engine, workspace)
        prompt = onboarding_prompt(
            mode, description, guide, person_name=name,
            environment=facts.section if facts else "",
            github_access=facts.github_access if facts else "",
            work=work,
            memory_backend=engine.config.memory.backend,
            agent_name=agent_name,
        )
    else:
        # The description as written: a free-text one from ``nerve init``,
        # or the Mode step's selection (its lines name the work).
        prompt = worker_onboarding_prompt(description, guide, engine.config.memory.backend)

    async def _first_turn() -> None:
        try:
            with engine.watch_turns(session_id) as turn:
                await engine.run(
                    session_id=session_id,
                    user_message=prompt,
                    source=source,
                    # Nerve composes this message. In the conversation,
                    # ``internal`` keeps it out of the transcript, and the
                    # thinking before the greeting is dropped, so the
                    # agent's greeting opens the chat.
                    internal=kind == CONVERSATION,
                    drop_leading_thinking=kind == CONVERSATION,
                    actor=db.system_actor,
                )
            failed = turn.outcome is not None and turn.outcome.status == "failed"
        except Exception:
            logger.exception("Onboarding: the first turn in %s failed", session_id)
            failed = True
        if not failed:
            return
        if await _form_shown(db, session_id):
            # The person has the chat and its form: they go on there, and
            # its agent can still end the onboarding.
            logger.warning(
                "Onboarding: the first turn in %s failed after its form; the session stays open",
                session_id,
            )
        elif await db.set_onboarding(
            REQUESTED, session_id=None, actor_id=actor_id, expect=(RUNNING, session_id),
        ):
            logger.warning("Onboarding: the first turn in %s failed; asked again", session_id)

    task = asyncio.create_task(_first_turn())
    register = getattr(engine, "register_task", None)
    if register:
        register(session_id, task)
    logger.info("Onboarding: %s %s opened (%s)", kind, session_id, mode)
    return OnboardingState(RUNNING, session_id, (RUNNING, session_id), kind)


async def request(db: "Database", workspace: Path, *, actor_id: str | None) -> OnboardingState:
    """Ask the next daemon start to open the conversation (a restart is pending).

    Setup in the browser asks for a conversation, also when ``nerve init``
    asked for the autonomous kind before.
    """
    state = await read_state(db, workspace)
    if state.status == PENDING and (state.stored[0] != REQUESTED or state.kind != CONVERSATION):
        await db.set_onboarding(
            REQUESTED, session_id=None, actor_id=actor_id, expect=state.stored, kind=CONVERSATION,
        )
    return state


def _task_description(workspace: Path) -> str:
    """The description in a worker's pending ``TASK.md``, else ``""``. Blocking file I/O."""
    try:
        text = _task_md(workspace).read_text(encoding="utf-8")
    except OSError:
        return ""
    return (task_description_from(text) or "").strip()


def _describes_work(workspace: Path) -> bool:
    """A worker's pending ``TASK.md`` names work to set up. Blocking file I/O.

    A free-text description does; a Mode step selection does with catalog
    items or custom work, and not with rules alone.
    """
    description = _task_description(workspace)
    if not description:
        return False
    work = work_selection_from(description)
    return work.free_text or bool(work.items or work.custom)


async def request_autonomous(db: "Database", workspace: Path) -> bool:
    """Ask the next daemon start for the autonomous onboarding. True when asked.

    For a worker that ``nerve init --terminal`` or ``--non-interactive`` set
    up with a task description, and only while onboarding has no row. A
    worker without a description waits for the conversation in the web UI.
    """
    if workspace_mode(Path(workspace)) != "worker":
        return False
    if not await asyncio.to_thread(_describes_work, workspace):
        return False
    return await db.set_onboarding(
        REQUESTED, session_id=None, actor_id=None, expect=(None, None), kind=AUTONOMOUS,
    )


def request_autonomous_sync(config) -> bool:
    """:func:`request_autonomous` on a new connection, for ``nerve init``."""
    from nerve.migrate import open_production_db

    async def _request() -> bool:
        db = await open_production_db(config)
        try:
            return await request_autonomous(db, Path(config.workspace))
        finally:
            await db.close()

    return asyncio.run(_request())


async def start_on_boot(engine: "AgentEngine", workspace: Path) -> OnboardingState | None:
    """Open the onboarding at daemon start when a ``requested`` row asks for it.

    A conversation (setup finished while a restart was pending), or the
    autonomous onboarding of a worker that ``nerve init`` set up. Nothing
    opens without a model credential, as at Finish. The session's cause is
    the person who finished setup, when known, else the system actor. The
    gateway calls this once at start-up; one-shot CLI engines do not.
    """
    try:
        if not engine.config.credential_available():
            return None
        state = await read_state(engine.db, workspace)
        if state.status != PENDING or state.stored[0] != REQUESTED:
            return None
        row = await engine.db.get_onboarding()
        return await start(
            engine, workspace, actor=engine.db.system_actor,
            cause=(row or {}).get("actor_id") or engine.db.system_actor, kind=state.kind,
        )
    except Exception:
        logger.exception("Onboarding: could not open the onboarding at start-up")
        return None


def _remove_task_marker(workspace: Path) -> None:
    """Delete the marker line from a worker's ``TASK.md``. Blocking file I/O."""
    path = _task_md(workspace)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return
    if TASK_ONBOARDING_MARKER not in text:
        return
    kept = [line for line in text.splitlines(keepends=True) if TASK_ONBOARDING_MARKER not in line]
    publish_text(path, "".join(kept))


async def complete(
    db: "Database", workspace: Path, session_id: str, *, actor_id: str | None,
) -> bool:
    """Mark onboarding done from its own session. False otherwise.

    For a worker, also removes the marker line from ``TASK.md``. The stored
    row decides, so a session that removed the marker itself can still mark
    onboarding done.
    """
    state = await read_state(db, workspace)
    if state.stored != (RUNNING, session_id):
        return False
    if not await db.set_onboarding(
        DONE, session_id=session_id, actor_id=actor_id, expect=state.stored,
    ):
        return False
    if workspace_mode(Path(workspace)) == "worker":
        await asyncio.to_thread(_remove_task_marker, workspace)
    logger.info("Onboarding: done in %s", session_id)
    return True


async def skip(db: "Database", workspace: Path, *, actor_id: str | None) -> OnboardingState:
    """Skip onboarding while it is pending or running. Keeps the session."""
    state = await read_state(db, workspace)
    if state.status in (PENDING, RUNNING):
        if await db.set_onboarding(
            SKIPPED, session_id=state.stored[1], actor_id=actor_id, expect=state.stored,
        ):
            logger.info("Onboarding: skipped")
    return await read_state(db, workspace)
