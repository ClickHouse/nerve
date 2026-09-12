"""Request and autonomous-work identity.

This top-level module imports neither the database nor gateway at runtime, so
both may use :class:`Actor`. Actors are resolved per request and passed down;
there is no process-global current actor. ``actor_id`` is permanent identity,
while ``display_name`` is only a presentation snapshot.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only, never imported at runtime
    from nerve.db.accounts import AccountStore

logger = logging.getLogger(__name__)

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
    """Resolve the account-less principal used for autonomous work."""
    try:
        row = await store.get_system_principal()
    except RuntimeError as e:
        # AccountStore validates this singleton when the database opens and
        # raises if later external mutation breaks the cached invariant. Turn
        # that storage failure into the same fail-closed resolution error every
        # authenticated ingress already knows how to render.
        raise ActorResolutionError(
            "This instance has no valid system principal"
        ) from e
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


async def system_actor_or_none(
    store: "AccountStore", *, context: str = "",
) -> Actor | None:
    """The system principal for work that must proceed without one.

    :func:`system_actor` raises when identity bootstrap has not run, which is
    right at an ingress: refusing a credential that resolves to nobody is the
    safe direction. Attribution is the opposite case. A cron job, a channel
    reply or a thread sync is real work with a real result, and the actor id is
    metadata *about* that work — so a missing system principal leaves the row
    unattributed and says so in the log, instead of failing the run and losing
    the work as well as the attribution.

    Production never takes that branch: every path that opens the database
    bootstraps the identity first (``nerve.migrate.open_production_db`` and the
    gateway lifespan), so the principal exists before anything can run.

    Every failure is caught, not just the missing-principal one, for the same
    reason: a database hiccup while reading two rows of metadata must not be
    able to cancel a scheduled job or drop a channel reply that would have
    worked a moment ago. This is deliberately *not* how a person's actor is
    resolved — ``require_auth`` raises and the request is refused, because
    there the identity is the authorization.
    """
    try:
        return await system_actor(store)
    except Exception as e:  # noqa: BLE001 — attribution must not fail the work
        logger.warning(
            "No system principal%s: %s — the row will be unattributed",
            f" for {context}" if context else "", e,
        )
        return None
