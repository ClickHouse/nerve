"""The onboarding conversation: its state, its start and its end.

After setup, the agent is set up by talking to it. :func:`start` opens one web
chat session for that and gives the agent a hidden first message
(:func:`nerve.agent.prompts.onboarding_prompt`), so the agent speaks first and
the person sees only its answer. The agent calls the ``onboarding_done`` tool
after the person confirmed the setup and the agent applied it
(:func:`complete`). A person can skip onboarding on the setup page
(:func:`skip`).

The state is the ``onboarding`` row (migration v060) plus two rules for
installs from before it:

- A worker whose ``TASK.md`` no longer carries the onboarding marker is done.
- A worker whose ``TASK.md`` still carries it is pending, and the daemon opens
  the conversation when it starts.

:func:`read_state` gives one of ``not_applicable`` (no mode chosen), ``pending``,
``running``, ``done`` or ``skipped``.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from nerve.identity import actor_id_of
from nerve.setup_writer import (
    TASK_ONBOARDING_MARKER,
    publish_text,
    render_task_md,
    task_description_from,
    task_onboarding_pending,
    workspace_mode,
)

if TYPE_CHECKING:
    from nerve.agent.engine import AgentEngine
    from nerve.db import Database
    from nerve.identity import ActorOrId

logger = logging.getLogger(__name__)

NOT_APPLICABLE = "not_applicable"
PENDING = "pending"
RUNNING = "running"
DONE = "done"
SKIPPED = "skipped"
# Stored only: setup finished while a restart was pending (reads as pending).
REQUESTED = "requested"

SESSION_TITLE = "Onboarding"


@dataclass(frozen=True)
class OnboardingState:
    status: str
    session_id: str | None = None
    #: The stored ``(status, session_id)``, for a compare-and-set write.
    stored: tuple[str | None, str | None] = (None, None)


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
    status, session_id = stored
    if status in (DONE, SKIPPED):
        return OnboardingState(status, session_id, stored)
    if mode == "worker" and not await asyncio.to_thread(_worker_task_pending, workspace):
        return OnboardingState(DONE, session_id, stored)
    if status == RUNNING and session_id and await db.get_session(session_id):
        return OnboardingState(RUNNING, session_id, stored)
    return OnboardingState(PENDING, None, stored)


def _prompt_inputs(workspace: Path, mode: str) -> tuple[str, str]:
    """The worker's description and guide for the prompt. Blocking file I/O.

    A worker without ``TASK.md``, or with the raw ``# Task`` file of an older
    install, gets the template first, so the agent fills it in place.
    """
    from nerve.workspace import read_worker_guide

    if mode != "worker":
        return "", ""
    path = _task_md(workspace)
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raw = ""
    description = task_description_from(raw) or "" if raw else ""
    if TASK_ONBOARDING_MARKER not in raw and (not raw or task_onboarding_pending(raw)):
        publish_text(path, render_task_md(description))
    return description, read_worker_guide()


async def start(
    engine: "AgentEngine",
    workspace: Path,
    *,
    actor,
    cause: "ActorOrId | None",
) -> OnboardingState:
    """Open the onboarding conversation if it is pending. Idempotent.

    One caller claims the row (a compare-and-set from the state it read);
    every other caller, also at the same time, gets the state that the
    winner wrote. The agent's first turn runs in the background.
    """
    from nerve.agent.prompts import onboarding_prompt

    db = engine.db
    state = await read_state(db, workspace)
    if state.status != PENDING:
        return state
    session_id = f"onboarding-{uuid.uuid4().hex[:8]}"
    claimed = await db.set_onboarding(
        RUNNING, session_id=session_id, actor_id=actor_id_of(cause), expect=state.stored,
    )
    if not claimed:
        # Another caller claimed it, maybe before its session row exists.
        row = await db.get_onboarding()
        if row and row["status"] == RUNNING:
            return OnboardingState(RUNNING, row["session_id"], (RUNNING, row["session_id"]))
        return await read_state(db, workspace)

    await engine.sessions.get_or_create(
        session_id, title=SESSION_TITLE, source="web", actor=actor, cause=cause,
    )
    mode = workspace_mode(Path(workspace)) or "personal"
    description, guide = await asyncio.to_thread(_prompt_inputs, workspace, mode)

    async def _first_turn() -> None:
        try:
            await engine.run(
                session_id=session_id,
                user_message=onboarding_prompt(
                    mode, description, guide, memory_backend=engine.config.memory.backend,
                ),
                source="web",
                # Nerve composes this message; ``internal`` keeps it out of
                # the transcript, so the agent's answer opens the chat.
                internal=True,
                actor=db.system_actor,
            )
        except Exception:
            logger.exception("Onboarding: the first turn in %s failed", session_id)

    task = asyncio.create_task(_first_turn())
    register = getattr(engine, "register_task", None)
    if register:
        register(session_id, task)
    logger.info("Onboarding: conversation %s opened (%s)", session_id, mode)
    return OnboardingState(RUNNING, session_id, (RUNNING, session_id))


async def request(db: "Database", workspace: Path, *, actor_id: str | None) -> OnboardingState:
    """Ask the next daemon start to open the conversation (a restart is pending)."""
    state = await read_state(db, workspace)
    if state.status == PENDING and state.stored[0] != REQUESTED:
        await db.set_onboarding(REQUESTED, session_id=None, actor_id=actor_id, expect=state.stored)
    return state


async def start_on_boot(engine: "AgentEngine", workspace: Path) -> OnboardingState | None:
    """Open the conversation at daemon start when setup asked for it.

    That is a ``requested`` row, or a worker whose ``TASK.md`` still carries
    the marker and that never had a conversation. The session's cause is
    the person who finished setup, when known.
    """
    state = await read_state(engine.db, workspace)
    if state.status != PENDING:
        return None
    status = state.stored[0]
    legacy_worker = (
        status is None and workspace_mode(Path(workspace)) == "worker"
        and _task_md(workspace).exists()
    )
    if status != REQUESTED and not legacy_worker:
        return None
    row = await engine.db.get_onboarding()
    return await start(
        engine, workspace, actor=engine.db.system_actor,
        cause=(row or {}).get("actor_id"),
    )


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
    """Mark onboarding done from its own conversation. False otherwise.

    For a worker, also removes the marker line from ``TASK.md``.
    """
    state = await read_state(db, workspace)
    if state.status != RUNNING or state.session_id != session_id:
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
