"""What the setup checklist remembers between requests.

Almost nothing, on purpose. A step is "done" because the thing it does is
*true* — the account has a password, a provider credential is configured —
not because a flag says so, so an install set up at the terminal shows the
same checklist as one set up in a browser, and a step re-entered after a
change still reads correctly.

Two things cannot be derived, and only those are stored. Decisions are scoped
to the account that made them; restart bookkeeping is shared because it
describes the one running daemon:

* **skipped** — "I do not want Telegram" is indistinguishable from "I have not
  got to Telegram yet" unless somebody writes it down, and a checklist that
  keeps nagging about a decision you already made is a checklist people
  abandon.
* **applied** — which configuration paths a step wrote *in this run of the
  daemon*. That is what makes "a restart is pending" true until the restart
  and false afterwards, and the boot generation is what makes it expire: an
  entry loaded by a later process describes a write that has already been
  picked up or already been undone, and either way the live configuration is
  the better answer. Secret paths are recorded as ``true``/``false``, never as
  their value — this file is bookkeeping, not a credential store.

Everything here is therefore scoped to a **boot generation**
(:mod:`nerve.boot`). A file written by an earlier process keeps only the
decisions that are still decisions — what was skipped, and the steps whose
completion nothing else records — and forgets what it thought was in flight.
Without that, a credential removed by hand months later leaves its step
reading "done", and a restart that has happened keeps being reported as
pending, forever.

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
_UNSCOPED_ACCOUNT = "_unscoped"

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
class AccountSetupState:
    """The checklist decisions made by one signed-in account."""

    #: Steps declined on purpose. A decision, so it outlives every restart.
    skipped: set[str] = field(default_factory=set)
    #: Steps recorded as answered. Transitional for every step whose
    #: completion the live configuration can state on its own; see
    #: :func:`retire_transitional`.
    done: set[str] = field(default_factory=set)
    #: Steps answered in a way nothing on disk can distinguish from never
    #: having been asked — choosing the default that was already there. A
    #: *decision*, so it is kept across restarts like a skip, and it is the
    #: only thing that tells "set the timezone back to UTC" from "nobody ever
    #: touched the timezone".
    answered: set[str] = field(default_factory=set)

    def as_dict(self) -> dict[str, Any]:
        return {
            "skipped": sorted(self.skipped),
            "done": sorted(self.done),
            "answered": sorted(self.answered),
        }


@dataclass
class SetupState:
    #: The selected account's decisions. These remain direct attributes so the
    #: route code cannot accidentally mutate a detached copy.
    skipped: set[str] = field(default_factory=set)
    done: set[str] = field(default_factory=set)
    answered: set[str] = field(default_factory=set)
    #: dotted config path -> the value written (or True/False for a secret).
    #: Only ever this process's writes: an entry that outlived its writer says
    #: nothing a restart has not already answered.
    applied: dict[str, Any] = field(default_factory=dict)
    #: Things this run changed that the running process has not picked up and
    #: that are not configuration keys — a cron file the scheduler has not
    #: re-read. Transitional in exactly the same way as ``applied``.
    debts: set[str] = field(default_factory=set)
    #: The generation that wrote the entries above, or ``""`` for a state that
    #: has not been written yet.
    boot: str = ""
    #: Whether :func:`retire_transitional` dropped anything from this object
    #: since it was loaded, so the caller knows there is something to persist.
    retired: bool = False
    #: Which account the three decision sets above belong to. Product callers
    #: always provide a real account id; the sentinel only supports direct
    #: state-file callers and old tests that have no account context.
    account_id: str | None = None
    #: Every other account's decisions, retained during this read/modify/write
    #: so saving Alice cannot erase Bob.
    accounts: dict[str, AccountSetupState] = field(default_factory=dict, repr=False)

    def _sync_account(self) -> None:
        scope = self.account_id or _UNSCOPED_ACCOUNT
        self.accounts[scope] = AccountSetupState(
            skipped=self.skipped,
            done=self.done,
            answered=self.answered,
        )

    def as_dict(self) -> dict[str, Any]:
        self._sync_account()
        return {
            "version": _VERSION,
            "accounts": {
                account_id: decisions.as_dict()
                for account_id, decisions in sorted(self.accounts.items())
            },
            "applied": dict(self.applied),
            "debts": sorted(self.debts),
            "boot": boot.boot_id(),
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


def _account_states(value: Any) -> dict[str, AccountSetupState]:
    """Valid account decision objects in a v3 state file."""
    if not isinstance(value, dict):
        return {}
    return {
        account_id: AccountSetupState(
            skipped=_string_set(raw.get("skipped")),
            done=_string_set(raw.get("done")),
            answered=_string_set(raw.get("answered")),
        )
        for account_id, raw in value.items()
        if isinstance(account_id, str) and isinstance(raw, dict)
    }


def load_state(
    account_id: str | None = None,
    *,
    adopt_legacy_decisions: bool = True,
) -> SetupState:
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
        return SetupState(account_id=account_id)
    except (OSError, ValueError) as e:
        logger.warning("Setup state at %s is unreadable (%s); starting empty", path, e)
        return SetupState(account_id=account_id)
    if not isinstance(raw, dict):
        logger.warning("Setup state at %s is not an object; starting empty", path)
        return SetupState(account_id=account_id)

    version = raw.get("version")
    if not isinstance(version, int) or version > _VERSION:
        # Written by a newer Nerve. Its fields may mean something else, and
        # guessing is how a downgrade corrupts the newer version's file.
        logger.warning(
            "Setup state at %s has version %r (this build writes %d); "
            "starting empty", path, version, _VERSION,
        )
        return SetupState(account_id=account_id)

    written_by = raw.get("boot")
    migrated = version < 3
    if migrated:
        # Versions 1 and 2 predate multi-account checklist state. A sole
        # account can safely inherit those decisions. With multiple accounts
        # authorship is unknowable: re-prompting is safer than assigning one
        # person's skip or completion to somebody else. Shared restart facts
        # below survive either way.
        scope = account_id or _UNSCOPED_ACCOUNT
        selected = AccountSetupState()
        if adopt_legacy_decisions:
            selected = AccountSetupState(
                skipped=_string_set(raw.get("skipped")),
                done=_string_set(raw.get("done")),
                answered=_string_set(raw.get("answered")),
            )
        elif any(raw.get(field) for field in ("skipped", "done", "answered")):
            logger.warning(
                "Setup state at %s predates account scoping and this instance "
                "has multiple accounts; discarding ambiguous checklist decisions",
                path,
            )
        accounts = {scope: selected}
    else:
        accounts = _account_states(raw.get("accounts"))
        if account_id is not None:
            scope = account_id
        elif len(accounts) == 1:
            # A convenience for direct state-file diagnostics. Routes always
            # pass an account id, so product behavior never guesses.
            scope = next(iter(accounts))
        else:
            scope = _UNSCOPED_ACCOUNT
        selected = accounts.setdefault(scope, AccountSetupState())

    return SetupState(
        skipped=selected.skipped,
        done=selected.done,
        answered=selected.answered,
        applied=_string_map(raw.get("applied")),
        debts=_string_set(raw.get("debts")),
        boot=written_by if isinstance(written_by, str) else "",
        retired=migrated,
        account_id=scope,
        accounts=accounts,
    )


def retire_transitional(state: SetupState, transitional_done: set[str]) -> bool:
    """Forget what an earlier process thought was in flight. True if anything went.

    Call it with the steps whose completion the live configuration can state
    on its own — a provider credential is either configured or it is not — so
    that after a restart those read from the instance rather than from a note
    the wizard left about it. A step nothing else records (which crons an
    operator wanted) is not transitional and is kept.

    ``applied`` goes entirely: every entry there exists to answer "is a
    restart pending", and a process that has since started *is* the answer.
    Keeping them is how a restart that happened stays reported as pending and
    a credential removed by hand keeps its step green.

    ``answered`` is never retired. It records a decision the instance cannot
    state for itself — that somebody chose the value that was already there —
    and a decision is not transitional.
    """
    if state.boot == boot.boot_id():
        return False
    state._sync_account()
    dropped = (
        bool(state.applied) or bool(state.debts)
        or any(
            bool(decisions.done & transitional_done)
            for decisions in state.accounts.values()
        )
    )
    state.applied = {}
    state.debts = set()
    for decisions in state.accounts.values():
        decisions.done -= transitional_done
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


def record_applied(state: SetupState, updates: dict[str, Any]) -> None:
    """Note what a step wrote, with secrets reduced to "present"."""
    state.boot = boot.boot_id()
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
    if state.boot and state.boot != boot.boot_id():  # pragma: no cover - retired on load
        return []
    pending = []
    for dotted, written in sorted(state.applied.items()):
        live = dotted_value(config, dotted)
        if live is _UNSET:
            continue
        if dotted in _SECRET_PATHS:
            # A secret is recorded as "present", never as itself, so the value
            # comparison cannot tell a key that is in force from one pasted
            # over it. Every entry here was written by *this* process, which
            # has not re-read its configuration since, so a recorded secret is
            # by construction not yet live.
            pending.append(dotted)
        elif live != written:
            pending.append(dotted)
    return pending
