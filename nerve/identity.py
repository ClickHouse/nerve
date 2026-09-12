"""Request and autonomous-work identity.

This top-level module imports neither the database nor gateway at runtime, so
both may use :class:`Actor`. Actors are resolved per request and passed down;
there is no process-global current actor. ``actor_id`` is permanent identity,
while ``display_name`` is only a presentation snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only, never imported at runtime
    from nerve.db.accounts import AccountStore

# ``actor_refs.kind``. A human is a person with a local account; the system
# principal is what the agent's own autonomous work acts as (0.6).
ACTOR_KIND_HUMAN = "human"
ACTOR_KIND_SYSTEM = "system"
ACTOR_KINDS = (ACTOR_KIND_HUMAN, ACTOR_KIND_SYSTEM)


class ActorResolutionError(Exception):
    """A verified credential names no actor this instance can act for."""


@dataclass(frozen=True, slots=True)
class Actor:
    """The immutable identity an authenticated request or autonomous run uses."""

    actor_id: str
    kind: str
    # The local login behind a human actor. ``None`` for the system principal,
    # which has no account and cannot log in.
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


async def system_actor(store: "AccountStore") -> Actor:
    """The agent's system principal — the identity autonomous work acts as.

    Scheduled runs, channel traffic, background agents and the agent's own
    calls into its API are attributed to this actor rather than to a person
    (0.6). It has no account and never logs in.

    Raises rather than degrading, and every persistent autonomous write goes
    through it. Every production opener bootstraps the identity before it can
    serve, so the only way this fails is a regression or a corrupted database —
    and a run that wrote its rows *without* an actor in that state would leave
    audit gaps indistinguishable from history that predates attribution, which
    nothing can later repair. Failing the run is recoverable; a permanent NULL
    is not. Resolve before the first write, so a failure costs nothing.
    """
    row = await store.get_system_principal()
    if row is None:
        raise ActorResolutionError(
            "This instance has no system principal; identity bootstrap has not run"
        )
    return Actor(
        actor_id=row["id"],
        kind=row["kind"],
        account_id=None,
        display_name=row["display_name"],
    )
