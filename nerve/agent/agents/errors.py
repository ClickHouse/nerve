"""Errors of the agent stop API and the turn queue.

Each carries the HTTP status and the error ``code`` of the REST contract
(``{"detail": {"code": ..., "message": ...}}``, plus :attr:`extra` keys), so
the routes map them without a table of their own.
"""

from __future__ import annotations

from typing import Any


class AgentControlError(Exception):
    """Base class: an agent or queue request that cannot be carried out."""

    status: int = 400
    code: str = "error"

    def __init__(self, message: str = "", **extra: Any) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code
        self.extra: dict[str, Any] = {k: v for k, v in extra.items() if v is not None}

    def detail(self) -> dict[str, Any]:
        """The ``detail`` body of the error response."""
        return {"code": self.code, "message": self.message, **self.extra}


class AgentNotFound(AgentControlError):
    """No such agent in the session. When the id belongs to another session
    (a fork inherited the spawn card), ``owner_session_id`` names it and the
    code is ``agent_in_other_session``."""

    status = 404
    code = "agent_not_found"

    def __init__(self, agent_id: str, *, owner_session_id: str | None = None) -> None:
        if owner_session_id:
            super().__init__(
                f"Agent {agent_id} belongs to another session",
                owner_session_id=owner_session_id,
            )
            self.code = "agent_in_other_session"
        else:
            super().__init__(f"Agent {agent_id} not found")
        self.agent_id = agent_id
        self.owner_session_id = owner_session_id


class NoRuntime(AgentControlError):
    """The session has no live runtime: nothing can be stopped (its records
    were reconciled to ``interrupted``)."""

    status = 409
    code = "no_runtime"


class Unsupported(AgentControlError):
    """The session's runtime cannot stop one agent on its own (Codex)."""

    status = 501
    code = "unsupported"


class QueueEntryNotFound(AgentControlError):
    status = 404
    code = "queue_entry_not_found"


class AlreadyStarted(AgentControlError):
    """The queued message already got the run lock: it is the turn now."""

    status = 409
    code = "already_started"


class NotOwner(AgentControlError):
    """Only the person who queued a message can cancel it (relays and
    internal turns can never be cancelled from the web)."""

    status = 403
    code = "not_owner"
