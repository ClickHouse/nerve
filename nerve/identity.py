"""Request and autonomous-work identity.

An :class:`Actor` is the identity carried through an operation: the person who
made the request, or the agent's system principal when the work is autonomous
(cron, hooks, background agents). Human actors are resolved per request and
passed down the call chain. The database caches the system actor at connect
(``Database.system_actor``); there is no process-global "current actor". The
ID of the system actor changes only in the identity bootstrap at startup, in
external mode, when the environment names the ID of the agent's system
principal (:data:`SYSTEM_PRINCIPAL_ENV`) and no row refers to the old ID yet.
The bootstrap then refreshes the cache.

This module sits at the top level, outside both :mod:`nerve.db` and
:mod:`nerve.gateway`, because both ends need it: the gateway resolves an actor
on the request, and the data-access layer stores its id. It therefore imports
neither at runtime — the database handle arrives as an argument and is typed
only for the checker — so ``nerve.db`` can import this module without a cycle.

``actor_id`` is permanent identity. ``display_name`` is a presentation snapshot
taken when the actor was resolved. Renaming an account changes what later
requests carry and rewrites nothing stored before. Names are never identity or
authorization keys.

A write records two IDs. The *actor* made the change: a person through the UI,
REST or the browser WebSocket, or the system actor for a tool call, a cron job
or any other internal process. The *cause* is the principal on whose behalf
the work happens: the person for a change that the person made, and for agent
work the cause of its session (``sessions.caused_by_actor_id``), which is the
person who started the session or the system actor for a session that the
instance started for itself. ``None`` in either position means unknown.
:func:`attribution_for` builds the pair for one write.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias
from uuid import UUID

if TYPE_CHECKING:  # pragma: no cover - typing only, never imported at runtime
    from nerve.db.accounts import AccountStore

# ``actor_refs.kind``. A human is a person: the owner of a local account, or,
# in external mode, a person that the gateway names. The system principal is
# the identity used for the agent's autonomous work.
ACTOR_KIND_HUMAN = "human"
ACTOR_KIND_SYSTEM = "system"
ACTOR_KINDS = (ACTOR_KIND_HUMAN, ACTOR_KIND_SYSTEM)

# In external mode, the platform that runs the agent can give the ID of the
# agent's system principal in this variable. The identity bootstrap reads it
# once at startup and gives that ID to the system actor (see
# ``nerve.migrate.bootstrap_identity``). Local mode ignores it.
SYSTEM_PRINCIPAL_ENV = "NERVE_SYSTEM_PRINCIPAL_ID"


class InvalidSystemPrincipalId(ValueError):
    """The system principal ID is not a canonical lower-case UUID."""


def parse_system_principal_id(value: str | None) -> str | None:
    """Return the system principal ID in ``value``, or ``None`` when it is unset.

    Unset or blank is ``None``. Any other value must be a UUID in its
    canonical lower-case form (``8-4-4-4-12`` hex digits), because the ID is
    compared and sent as a string. Raises :class:`InvalidSystemPrincipalId`
    for a value in another form, also for a valid UUID in upper case: the
    platform sends the canonical form, so another form is a configuration
    error.
    """
    if value is None or not value.strip():
        return None
    try:
        canonical = str(UUID(value))
    except ValueError:
        canonical = None
    if canonical != value:
        raise InvalidSystemPrincipalId(
            f"{SYSTEM_PRINCIPAL_ENV} must be a UUID in canonical lower-case form, "
            f"got {value!r}"
        )
    return canonical


def system_principal_id_from_env() -> str | None:
    """Read and parse ``NERVE_SYSTEM_PRINCIPAL_ID``."""
    return parse_system_principal_id(os.environ.get(SYSTEM_PRINCIPAL_ENV))


class ActorResolutionError(Exception):
    """A verified credential names no actor this instance can act for."""


@dataclass(frozen=True, slots=True)
class Actor:
    """The immutable identity an authenticated request or autonomous run uses."""

    actor_id: str
    kind: str
    # The local login behind a human actor. ``None`` for the system principal,
    # which has no account and cannot log in, and for a person that the
    # gateway names in external mode.
    account_id: str | None = None
    # Presentation snapshot, never an identity or authorization key.
    display_name: str | None = None

    def __post_init__(self) -> None:
        if not self.actor_id:
            raise ValueError("an actor needs an actor_id")
        if self.kind not in ACTOR_KINDS:
            raise ValueError(f"actor kind must be one of {ACTOR_KINDS}, got {self.kind!r}")
        if self.kind == ACTOR_KIND_SYSTEM and self.account_id is not None:
            raise ValueError("the system principal has no account")

    @property
    def is_system(self) -> bool:
        return self.kind == ACTOR_KIND_SYSTEM

    @property
    def is_human(self) -> bool:
        return self.kind == ACTOR_KIND_HUMAN


# An actor, or the ``actor_refs`` ID of one. A cause that comes from storage
# (a session's stored cause, for example) is an ID; a cause that comes from a
# request is the resolved :class:`Actor`.
ActorOrId: TypeAlias = "Actor | str"


class CauseFromActor:
    """Type of the :data:`CAUSE_FROM_ACTOR` sentinel."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "CAUSE_FROM_ACTOR"


# Default for a cause argument: the cause is the actor. A person acts for
# themselves, the system actor acts for itself, and no actor gives no cause.
# A separate sentinel is necessary because ``None`` is a real cause: unknown.
CAUSE_FROM_ACTOR = CauseFromActor()

CauseArg: TypeAlias = "ActorOrId | None | CauseFromActor"


def actor_id_of(ref: ActorOrId | None) -> str | None:
    """The actor ID that ``ref`` names, or ``None`` for no actor."""
    if ref is None:
        return None
    if isinstance(ref, Actor):
        return ref.actor_id
    if isinstance(ref, str) and ref:
        return ref
    raise ValueError(f"an actor reference must be an Actor or an actor ID, got {ref!r}")


def resolve_cause(actor: ActorOrId | None, cause: CauseArg = CAUSE_FROM_ACTOR) -> str | None:
    """The cause actor ID for one write, or for a session that is created."""
    if isinstance(cause, CauseFromActor):
        return actor_id_of(actor)
    return actor_id_of(cause)


@dataclass(frozen=True, slots=True)
class Attribution:
    """The ``(actor_id, caused_by_actor_id)`` pair that one write records.

    Every store method that records attribution takes one of these, so that
    all writes get the two values from the same rule. The store maps the two
    fields to its own column names.
    """

    actor_id: str | None = None
    caused_by_actor_id: str | None = None


# Nothing is known about who made the change.
UNATTRIBUTED = Attribution()


def attribution_for(actor: ActorOrId | None, cause: CauseArg = CAUSE_FROM_ACTOR) -> Attribution:
    """Build the pair for a write by ``actor`` on behalf of ``cause``."""
    return Attribution(
        actor_id=actor_id_of(actor),
        caused_by_actor_id=resolve_cause(actor, cause),
    )


def _actor_for_account_row(account: dict) -> Actor:
    """Resolve an account row, checking disablement on every request."""
    if not account.get("enabled"):
        raise ActorResolutionError("This account is disabled")
    if account["actor_id"] is None:
        # The schema's foreign key makes this unreachable; fail closed rather
        # than invent an identity if it ever is reached.
        raise ActorResolutionError("This account has no actor identity")
    return Actor(
        actor_id=account["actor_id"],
        kind=account["actor_kind"],
        account_id=account["account_id"],
        display_name=account["display_name"],
    )


async def actor_for_account(store: "AccountStore", account_id: str) -> Actor:
    """Resolve an account id without caching."""
    if not isinstance(account_id, str) or not account_id:
        raise ActorResolutionError("This credential names no account")
    account = await store._account_identity(account_id)
    if account is None:
        raise ActorResolutionError("This credential names an account that no longer exists")
    return _actor_for_account_row(account)


async def actor_for_sole_account(store: "AccountStore") -> Actor:
    """Resolve the sole account.

    A legacy credential names no person, so zero or multiple accounts are
    ambiguous and must fail rather than select a row.
    """
    account = await store._sole_account_identity()
    if account is None:
        raise ActorResolutionError(
            "This credential predates per-account logins and no longer resolves "
            "to a single account; sign in again"
        )
    return _actor_for_account_row(account)
