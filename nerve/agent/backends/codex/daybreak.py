"""Daybreak — Codex's account-gated cyber access program, as a per-chat switch.

Daybreak is not a model. A client asks for it per turn (``turn/start``
carries ``cyberAccessProgram``), the server decides whether the account may
use it, and the model catalog (``model/list``) says which programs each
model accepts for the signed-in account.

The switch is session metadata (:data:`METADATA_KEY`). Off is the default,
and with it off Nerve sends what it sent before the switch existed.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

# Session metadata key of the switch: ``true`` is on, anything else is off.
METADATA_KEY = "codex_daybreak"

# The Daybreak programs, preferred first: the name that ``turn/start`` takes,
# and the spelling of ``codex exec --cyber-access-program``.
PROGRAMS = {"daybreakBlue": "daybreak_blue", "daybreakRed": "daybreak_red"}

# The first Codex whose ``exec`` (and ``exec resume``) has the
# ``--cyber-access-program`` flag. The catalog lists access programs from
# 0.156.0, so on the versions in between a chat can have the switch on while
# its ``exec`` workers cannot be given the program.
EXEC_FLAG_SINCE = (0, 161, 0)


def advertised(entry: Mapping[str, Any]) -> list | None:
    """The cyber programs a ``model/list`` entry accepts for the account.

    ``None`` when the entry carries no access-program metadata: a Codex from
    before the metadata, or a catalog without it. An empty list is metadata.
    """
    programs = entry.get("availableAccessPrograms")
    cyber = programs.get("cyber") if isinstance(programs, Mapping) else None
    return cyber if isinstance(cyber, list) else None


def program(entry: Mapping[str, Any]) -> str | None:
    """The Daybreak program of a ``model/list`` entry: the first of
    :data:`PROGRAMS` that it advertises. Other names are ignored."""
    offered = advertised(entry) or []
    return next((name for name in PROGRAMS if name in offered), None)


def enabled(session: Mapping[str, Any] | None) -> bool:
    """Whether a session row has the switch on."""
    metadata = (session or {}).get("metadata")
    if isinstance(metadata, str):
        if METADATA_KEY not in metadata:
            return False  # the usual case, without a parse
        try:
            metadata = json.loads(metadata)
        except ValueError:
            return False
    return isinstance(metadata, Mapping) and metadata.get(METADATA_KEY) is True


def no_program(model: str) -> str:
    """Why the switch cannot work on *model*, as a clause for a message."""
    return f"model {model!r} has no Daybreak program for the signed-in Codex account"
