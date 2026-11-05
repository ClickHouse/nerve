"""What the setup checklist remembers between requests.

Almost nothing, on purpose. A step is "done" because the thing it does is
*true* — a provider credential is configured, the mode templates exist — not
because a flag says so, so an install set up at the terminal shows the same
checklist as one set up in a browser, and a step re-entered after a change
still reads correctly.

Only what cannot be derived is stored, and **no configuration value ever is**:

* **skipped** — "I do not want Telegram" is indistinguishable from "I have not
  got to Telegram yet" unless somebody writes it down. A decision; outlives
  every restart.
* **answered** — steps answered with a value nothing on disk can distinguish
  from the default (the timezone that was already there, "continue without
  embeddings", "no sources"). A decision; outlives every restart.
* **debts** — things waiting on a restart that are not configuration keys: a
  cron file the running scheduler could not re-read, embeddings that start
  with the next process. **Boot-scoped**: an entry written by an earlier
  process describes a restart that has since happened.
* **boot_errors** — subsystems that failed to start in the boot named by
  ``boot`` (today only the Telegram bot), with the token scrubbed. Boot-scoped
  like the debts; on disk rather than in memory because ``nerve doctor`` is a
  separate process.
* **telegram_bot_username** — the non-secret bot name from the last
  successful server-side ``getMe``, so the page after a restart can offer the
  ``t.me`` link before the new process has looked it up.
* **telegram_sync_user** — who the Telegram *message sync* is signed in as
  (``{"id", "username", "first_name"}``), from the last sign-in or session
  check, so the pure checklist and ``nerve doctor`` can say "signed in as
  @name" without opening the session file. Cleared on sign-out or when a
  check finds the session unauthorised. Not a secret, not a configuration
  value.

Whether a *restart* is pending is not stored at all: it is derived by
comparing the configuration the process started with against the one on disk
(:func:`nerve.config_reload.restart_pending`).

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

from nerve import boot, paths

logger = logging.getLogger(__name__)

_VERSION = 3


def state_file() -> Path:
    """Resolved per call, never at import: ``NERVE_HOME`` moves under tests."""
    return paths.nerve_path("setup-state.json")


@dataclass
class SetupState:
    #: Steps declined on purpose. A decision, so it outlives every restart.
    skipped: set[str] = field(default_factory=set)
    #: Steps answered in a way nothing on disk can distinguish from never
    #: having been asked. A decision, kept across restarts like a skip.
    answered: set[str] = field(default_factory=set)
    #: Non-config things waiting on a restart: name -> sentence for a person.
    #: Only ever this process's: retired when a new boot loads the file.
    debts: dict[str, str] = field(default_factory=dict)
    #: Subsystems that failed to start in this boot: name -> scrubbed text.
    boot_errors: dict[str, str] = field(default_factory=dict)
    #: The bot's username from the last successful getMe, or ``None``.
    telegram_bot_username: str | None = None
    #: Who the Telegram message sync is signed in as, or ``None``.
    telegram_sync_user: dict[str, Any] | None = None
    #: The generation that wrote the boot-scoped entries above, or ``""``.
    boot: str = ""
    #: Whether :func:`retire_on_boot` dropped anything since the load, so the
    #: caller knows there is something to persist.
    retired: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": _VERSION,
            "skipped": sorted(self.skipped),
            "answered": sorted(self.answered),
            "debts": {k: str(v) for k, v in sorted(self.debts.items())},
            "boot_errors": {k: str(v) for k, v in sorted(self.boot_errors.items())},
            "telegram_bot_username": self.telegram_bot_username or None,
            "telegram_sync_user": _sync_user(self.telegram_sync_user),
            "boot": boot.boot_id(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }


def _sync_user(value: Any) -> dict[str, Any] | None:
    """``{"id": int, "username": str | None, "first_name": str}`` or ``None``
    for anything that is not that shape (an older or hand-edited file)."""
    if not isinstance(value, dict):
        return None
    raw_id = value.get("id")
    if isinstance(raw_id, bool) or not isinstance(raw_id, int) or raw_id <= 0:
        return None
    username = value.get("username")
    first_name = value.get("first_name")
    return {
        "id": raw_id,
        "username": username if isinstance(username, str) and username else None,
        "first_name": first_name if isinstance(first_name, str) else "",
    }


def _string_set(value: Any) -> set[str]:
    """The strings in ``value``, or an empty set if it is not a list of them."""
    if not isinstance(value, (list, tuple, set)):
        return set()
    return {item for item in value if isinstance(item, str)}


def _string_map(value: Any, *, legacy_list: bool = False) -> dict[str, str]:
    """The string-to-string entries of ``value``, or ``{}`` if it is not a mapping.

    A v2 file stored ``debts`` as a list of sentences; with ``legacy_list``
    those are kept under themselves as the key so nothing a person wrote down
    is lost on upgrade. Everywhere else a list is a wrong type and reads empty.
    """
    if isinstance(value, (list, tuple, set)):
        if not legacy_list:
            return {}
        return {item: item for item in value if isinstance(item, str)}
    if not isinstance(value, dict):
        return {}
    return {
        k: str(v) for k, v in value.items()
        if isinstance(k, str) and isinstance(v, (str, int, float, bool))
    }


def load_state() -> SetupState:
    """Read the checklist's notes. **Never raises** — every field is checked.

    This file is the wizard's own scratch pad, written by the wizard and read
    by nobody else, so anything malformed in it is either a partially written
    file or a version that did not exist yet. Neither is a reason for
    ``GET /api/setup`` to answer 500 until somebody deletes a file they have
    never heard of, so an unusable field reads as absent and an unusable
    *version* reads as an empty state. A v1/v2 file keeps its ``skipped`` and
    ``answered``; the rest of it (``done``, ``applied``) is dropped.
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

    written_by = raw.get("boot")
    username = raw.get("telegram_bot_username")
    return SetupState(
        skipped=_string_set(raw.get("skipped")),
        answered=_string_set(raw.get("answered")),
        debts=_string_map(raw.get("debts"), legacy_list=True),
        boot_errors=_string_map(raw.get("boot_errors")),
        telegram_bot_username=username if isinstance(username, str) and username else None,
        telegram_sync_user=_sync_user(raw.get("telegram_sync_user")),
        boot=written_by if isinstance(written_by, str) else "",
    )


def retire_on_boot(state: SetupState) -> bool:
    """Forget what an earlier process left in flight. True if anything went.

    ``debts`` and ``boot_errors`` exist to describe the running process; a
    process that has since started *is* the answer to both. ``skipped`` and
    ``answered`` are decisions and are never retired.
    """
    if state.boot == boot.boot_id():
        return False
    dropped = bool(state.debts) or bool(state.boot_errors)
    state.debts = {}
    state.boot_errors = {}
    state.boot = boot.boot_id()
    state.retired = state.retired or dropped
    return dropped


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


def record_boot_error(name: str, text: str) -> bool:
    """Note that ``name`` failed to start in this boot. Loads, retires, saves.

    Called from the lifespan, outside the wizard's lock (nothing else can be
    writing the file while the gateway is still starting). ``text`` must
    already have every secret scrubbed out of it.
    """
    state = load_state()
    retire_on_boot(state)
    state.boot_errors[name] = text
    return save_state(state)


def boot_errors_for_current_boot(state: SetupState | None = None) -> dict[str, str]:
    """The start-up failures recorded by *this* boot (a separate process reads
    an empty dict if the file names an earlier one)."""
    if state is None:
        state = load_state()
    if state.boot != boot.boot_id():
        return {}
    return dict(state.boot_errors)
