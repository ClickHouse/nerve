"""What the setup checklist remembers between requests.

Almost nothing, on purpose. A step is "done" because the thing it does is
*true* — the account has a password, a provider credential is configured —
not because a flag says so, so an install set up at the terminal shows the
same checklist as one set up in a browser, and a step re-entered after a
change still reads correctly.

Two things cannot be derived, and only those are stored:

* **skipped** — "I do not want Telegram" is indistinguishable from "I have not
  got to Telegram yet" unless somebody writes it down, and a checklist that
  keeps nagging about a decision you already made is a checklist people
  abandon.
* **applied** — which configuration paths a step wrote, and when. Comparing
  them against the *running* config object is what makes "a restart is
  pending" true until the restart and false afterwards, without anything
  having to remember to clear it. Secret paths are recorded as
  ``true``/``false``, never as their value: this file is bookkeeping, not a
  credential store — which is why the timestamp is there too, since a key
  pasted over an existing one reads as "present" either way.

The file lives in the machine-local state directory beside the database. A
missing or unreadable one reads as empty state — the wizard's own scratch file
must never be the reason setup cannot continue.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from nerve import paths

logger = logging.getLogger(__name__)

_VERSION = 1

# Paths whose *value* must not be written here. Recorded as a boolean instead:
# enough to answer "is the running process using what the wizard wrote", which
# is all the checklist asks.
_SECRET_PATHS = frozenset({
    "anthropic_api_key",
    "openai_api_key",
    "claude_oauth_token",
    "github_token",
    "telegram.bot_token",
    "sync.telegram.api_id",
    "sync.telegram.api_hash",
})

_UNSET = object()

# When this process started, near enough: the module is imported while the
# gateway is starting. It is what makes a *secret* that was replaced count as
# pending — the value comparison below cannot see that, because a secret is
# recorded as "present" rather than as itself, and a key pasted over an
# existing one is present either way while only the old one is in force.
PROCESS_STARTED = datetime.now(timezone.utc)


def state_file() -> Path:
    """Resolved per call, never at import: ``NERVE_HOME`` moves under tests."""
    return paths.nerve_path("setup-state.json")


@dataclass
class SetupState:
    skipped: set[str] = field(default_factory=set)
    done: set[str] = field(default_factory=set)
    # dotted config path -> the value written (or True/False for a secret)
    applied: dict[str, Any] = field(default_factory=dict)
    # dotted config path -> when it was written, so a secret replaced during
    # this process is known to be on disk and not yet in force
    applied_at: dict[str, str] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": _VERSION,
            "skipped": sorted(self.skipped),
            "done": sorted(self.done),
            "applied": dict(self.applied),
            "applied_at": dict(self.applied_at),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }


def _string_set(value: Any) -> set[str]:
    """The strings in ``value``, or an empty set if it is not a list of them."""
    if not isinstance(value, (list, tuple, set)):
        return set()
    return {item for item in value if isinstance(item, str)}


def _string_map(value: Any, *, coerce: bool = False) -> dict[str, Any]:
    """The string-keyed entries of ``value``, or ``{}`` if it is not a mapping."""
    if not isinstance(value, dict):
        return {}
    if coerce:
        return {k: str(v) for k, v in value.items() if isinstance(k, str)}
    return {k: v for k, v in value.items() if isinstance(k, str)}


def load_state() -> SetupState:
    """Read the checklist's notes. **Never raises** — every field is checked.

    This file is the wizard's own scratch pad, written by the wizard and read
    by nobody else, so anything malformed in it is either a partially written
    file or a version that did not exist yet. Neither is a reason for
    ``GET /api/setup`` to answer 500 until somebody deletes a file they have
    never heard of, so an unusable field reads as absent and an unusable
    *version* reads as an empty state.

    "Never raises" was previously a claim rather than a property:
    ``{"applied": []}`` is valid JSON, a valid object, and raised
    ``AttributeError`` on ``.items()``.
    """
    path = state_file()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return SetupState()
    except (OSError, ValueError) as e:
        logger.warning("Setup state at %s is unreadable (%s); starting empty", path, e)
        return SetupState()
    if not isinstance(raw, dict):
        logger.warning("Setup state at %s is not an object; starting empty", path)
        return SetupState()

    version = raw.get("version")
    if not isinstance(version, int) or version > _VERSION:
        # Written by a newer Nerve. Its fields may mean something else, and
        # guessing is how a downgrade corrupts the newer version's file.
        logger.warning(
            "Setup state at %s has version %r (this build writes %d); "
            "starting empty", path, version, _VERSION,
        )
        return SetupState()

    return SetupState(
        skipped=_string_set(raw.get("skipped")),
        done=_string_set(raw.get("done")),
        applied=_string_map(raw.get("applied")),
        applied_at=_string_map(raw.get("applied_at"), coerce=True),
    )


def save_state(state: SetupState) -> bool:
    """Persist the notes, owner-only and atomically. False if it could not be.

    Returns rather than raises because the caller has usually just written
    configuration successfully: failing the whole step over its bookkeeping
    would report a write that happened as a write that did not.
    """
    paths.ensure_nerve_home()
    try:
        paths.write_private_text(
            state_file(), json.dumps(state.as_dict(), indent=2) + "\n",
        )
        return True
    except (paths.InsecureFileError, OSError) as e:
        logger.warning("Could not save setup state to %s: %s", state_file(), e)
        return False


def record_applied(state: SetupState, updates: dict[str, Any]) -> None:
    """Note what a step wrote, with secrets reduced to "present"."""
    now = datetime.now(timezone.utc).isoformat()
    for dotted, value in updates.items():
        state.applied[dotted] = (
            bool(value) if dotted in _SECRET_PATHS else value
        )
        state.applied_at[dotted] = now


def dotted_value(config, dotted: str):
    """Follow a dotted path on the config object, or ``_UNSET``."""
    node = config
    for part in dotted.split("."):
        if not hasattr(node, part):
            return _UNSET
        node = getattr(node, part)
    return node


def pending_paths(state: SetupState, config) -> list[str]:
    """Paths the wizard wrote that this process is not running yet.

    The honest definition of "a restart is pending": not a flag somebody set,
    but a comparison between what is on disk and what the daemon is using. A
    reload that picks a value up clears it just as a restart does, and nothing
    has to remember to.
    """
    pending = []
    for dotted, written in sorted(state.applied.items()):
        live = dotted_value(config, dotted)
        if live is _UNSET:
            continue
        if dotted in _SECRET_PATHS:
            # Two ways a secret is not in force: there is none where one was
            # written, or one was *replaced* while this process was running —
            # which the value comparison cannot see, since both the old and the
            # new one read as "present".
            if bool(live) != bool(written) or _written_this_process(state, dotted):
                pending.append(dotted)
        elif live != written:
            pending.append(dotted)
    return pending


def _written_this_process(state: SetupState, dotted: str) -> bool:
    stamp = state.applied_at.get(dotted)
    if not stamp:
        # Written before this field existed, or by an older version: fall back
        # to the value comparison, which is what the caller already did.
        return False
    try:
        return datetime.fromisoformat(stamp) > PROCESS_STARTED
    except ValueError:
        return False
