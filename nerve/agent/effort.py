"""Per-session reasoning effort.

A session can carry its own effort level, picked when the chat is created or
changed later (``POST``/``PATCH /api/sessions``). It lives in the session's
metadata under :data:`METADATA_KEY`; no key means Default, which is the
source default the engine has always used (``agent.effort``, or
``agent.cron_effort`` for cron and hook turns).

The engine passes the level to the runtime when it builds the session's
client. For Claude that is the CLI's ``--effort``, and sub-agents the CLI
starts inherit it unless a call or an agent definition asks for its own
level. Only :meth:`nerve.db.sessions.SessionStore.set_session_effort` writes
the key, so a metadata write from a stale copy cannot undo a change (see
``update_session_metadata``).
"""

from __future__ import annotations

import json
from typing import Any

# The levels a session can pick, weakest first. The same vocabulary as
# ``agent.effort``; the Codex backend maps it through ``codex.effort_map``.
EFFORT_LEVELS: tuple[str, ...] = ("low", "medium", "high", "xhigh", "max")

# Where a session keeps its pick in ``sessions.metadata``.
METADATA_KEY = "effort"


def normalize_effort(value: Any) -> str | None:
    """Validate a requested effort.

    ``None`` and an empty string mean Default and return ``None``. A level is
    accepted in any case and with surrounding spaces. Anything else raises
    :class:`ValueError` with a message fit for an HTTP 400.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(
            f"effort must be one of {', '.join(EFFORT_LEVELS)}, or null for "
            "the default",
        )
    level = value.strip().lower()
    if not level:
        return None
    if level not in EFFORT_LEVELS:
        raise ValueError(
            f"Unknown effort {value!r}: use one of {', '.join(EFFORT_LEVELS)}, "
            "or null for the default",
        )
    return level


def effort_from_metadata(metadata: Any) -> str | None:
    """The effort stored in a session's metadata (a dict or its JSON text).

    Returns ``None`` (Default) for no key, unreadable metadata or a value
    that is not a known level.
    """
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata or "{}")
        except (TypeError, ValueError):
            return None
    if not isinstance(metadata, dict):
        return None
    value = metadata.get(METADATA_KEY)
    return value if isinstance(value, str) and value in EFFORT_LEVELS else None


def session_effort(session: dict | None) -> str | None:
    """The effort a session row carries, or ``None`` for Default."""
    if not session:
        return None
    return effort_from_metadata(session.get("metadata"))
