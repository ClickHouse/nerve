"""Who a piece of work is attributed to.

An :class:`Actor` is the identity carried through an operation: the person who
made the request, or the agent's system principal when the work is autonomous
(cron, channel traffic, background agents). It is resolved **per request** from
the database and passed down the call chain. There is deliberately no
process-global "current actor" and no cache: a shared agent serves two people
at once, and a module-level actor is how one of them ends up attributed to the
other.

This module sits at the top level, outside both :mod:`nerve.db` and
:mod:`nerve.gateway`, because both ends need it: the gateway resolves an actor
on the request, and the data-access layer stores its id. It therefore imports
neither at runtime — the database handle arrives as an argument and is typed
only for the checker — so ``nerve.db`` can import this module without a cycle.

Identity vs presentation (RFC 3.3): ``actor_id`` is the identity and is
permanent. ``display_name`` is a *snapshot* taken when the actor was resolved;
renaming an account changes what later requests carry and rewrites nothing that
was stored before. Names are never identity or authorization keys.
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
    """A verified credential names no actor this instance can act for.

    Raised, never returned, so a caller cannot forget to check: the token's
    signature was good but the account behind it is gone, disabled, or
    ambiguous. Ingress code turns it into a 401 (the credential is no longer
    usable, so re-authenticating is the remedy).
    """


@dataclass(frozen=True, slots=True)
class Actor:
    """The identity an authenticated request or an autonomous run acts as.

    Frozen: once resolved, an actor cannot be edited. A long-lived WebSocket
    fixes one at accept and carries that exact value for its whole life, so
    nothing can swap an identity under an open connection.
    """

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


async def _actor_for_account_row(store: "AccountStore", account: dict) -> Actor:
    """Turn an ``accounts`` row into an :class:`Actor`, refusing a disabled one.

    Disablement is checked **here**, on the way to every actor, rather than at
    each ingress: a token minted before the account was disabled keeps
    verifying (it is signed and unexpired), so the account row is the only
    thing that can stop it, and it takes effect on the next request.
    """
    if not account.get("enabled"):
        raise ActorResolutionError("This account is disabled")
    actor = await store.get_actor_ref(account["actor_id"])
    if actor is None:
        # The schema's foreign key makes this unreachable; fail closed rather
        # than invent an identity if it ever is reached.
        raise ActorResolutionError("This account has no actor identity")
    return Actor(
        actor_id=actor["id"],
        kind=actor["kind"],
        account_id=account["id"],
        display_name=actor["display_name"],
    )


async def actor_for_account(store: "AccountStore", account_id: str) -> Actor:
    """The actor of the account with this id.

    Looked up on every call. Nothing is cached: an account disabled a second
    ago must not keep being served from a previous lookup.
    """
    if not isinstance(account_id, str) or not account_id:
        raise ActorResolutionError("This credential names no account")
    account = await store.get_account(account_id)
    if account is None:
        raise ActorResolutionError("This credential names an account that no longer exists")
    return await _actor_for_account_row(store, account)


async def actor_for_sole_account(store: "AccountStore") -> Actor:
    """The actor of the single local account, when exactly one exists.

    The bound that 0.5 puts on passwordless access and PR 2 puts on
    grandfathered session tokens: a credential that names no particular person
    is only meaningful while there is only one person it could mean. With two
    accounts it names nobody, and resolving it to whichever row sorts first
    would attribute one person's work to another.
    """
    account = await store.get_sole_account()
    if account is None:
        raise ActorResolutionError(
            "This credential predates per-account logins and no longer resolves "
            "to a single account; sign in again"
        )
    return await _actor_for_account_row(store, account)


async def system_actor(store: "AccountStore") -> Actor:
    """The agent's system principal — the identity autonomous work acts as.

    Scheduled runs, channel traffic, background agents and the agent's own
    calls into its API are attributed to this actor rather than to a person
    (0.6). It has no account and never logs in.
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
