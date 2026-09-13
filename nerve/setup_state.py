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
* **applied** — which configuration paths a step wrote. Comparing them against
  the *running* config object is what makes "a restart is pending" true until
  the restart and false afterwards, without anything having to remember to
  clear it. Secret paths are recorded as ``true``/``false``, never as their
  value: this file is bookkeeping, not a credential store.

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


def state_file() -> Path:
    """Resolved per call, never at import: ``NERVE_HOME`` moves under tests."""
    return paths.nerve_path("setup-state.json")


@dataclass
class SetupState:
    skipped: set[str] = field(default_factory=set)
    done: set[str] = field(default_factory=set)
    # dotted config path -> the value written (or True/False for a secret)
    applied: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": _VERSION,
            "skipped": sorted(self.skipped),
            "done": sorted(self.done),
            "applied": dict(self.applied),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }


def load_state() -> SetupState:
    """Read the checklist's notes. Never raises."""
    path = state_file()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return SetupState()
    except (OSError, ValueError) as e:
        logger.warning("Setup state at %s is unreadable (%s); starting empty", path, e)
        return SetupState()
    if not isinstance(raw, dict):
        return SetupState()
    return SetupState(
        skipped={str(s) for s in raw.get("skipped") or [] if isinstance(s, str)},
        done={str(s) for s in raw.get("done") or [] if isinstance(s, str)},
        applied={
            str(k): v for k, v in (raw.get("applied") or {}).items()
            if isinstance(k, str)
        },
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
    for dotted, value in updates.items():
        state.applied[dotted] = (
            bool(value) if dotted in _SECRET_PATHS else value
        )


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
            if bool(live) != bool(written):
                pending.append(dotted)
        elif live != written:
            pending.append(dotted)
    return pending
