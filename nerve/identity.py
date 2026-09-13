"""Request and autonomous-work identity.

An :class:`Actor` is the identity carried through an operation: the person who
made the request, or the agent's system principal when the work is autonomous
(cron, hooks, background agents). Human actors are resolved per request and
passed down the call chain. Only the migration-guaranteed system actor may be
cached by the database; there is no process-global "current actor".

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
    # The account's session epoch **as the credential stated it** — not as the
    # row says now. It is what makes "admitted before the claim" answerable
    # after the claim: a mutation carries this into its transaction and the row
    # refuses it if the two have diverged (v049). ``None`` for a credential
    # with no account behind it, which has no epoch to be stale against.
    session_epoch: int | None = None

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


def _actor_for_account_row(
    account: dict, *, session_epoch: int | None = None,
) -> Actor:
    """Resolve a joined account identity row, refusing a stale one.

    Disablement is checked **here**, on the way to every actor, rather than at
    each ingress: a token minted before the account was disabled keeps
    verifying (it is signed and unexpired), so the account row is the only
    thing that can stop it, and it takes effect on the next request.

    The session epoch is checked in the same place and for the same reason. A
    session minted while the instance was passwordless names this account, is
    signed and has thirty days left; the row is the only thing that can end it,
    and claiming the account bumps the row (v049). ``session_epoch`` is what
    the credential carried — ``None`` for credentials that are not sessions
    (the system principal, MCP), which have no account and no epoch to check.
    """
    if not account.get("enabled"):
        raise ActorResolutionError("This account is disabled")
    if session_epoch is not None and session_epoch < int(account.get("session_epoch") or 0):
        raise ActorResolutionError(
            "This session predates the password on this account; sign in again"
        )
    if account["actor_id"] is None:
        # The schema's foreign key makes this unreachable; fail closed rather
        # than invent an identity if it ever is reached.
        raise ActorResolutionError("This account has no actor identity")
    return Actor(
        actor_id=account["actor_id"],
        kind=account["actor_kind"],
        account_id=account["account_id"],
        display_name=account["display_name"],
        # The credential's own epoch where there is one. Reading it back off
        # the row here instead would hand every request the *current* value —
        # which is the promotion bug: a session admitted before a claim would
        # carry the epoch the claim had just written, and every later check
        # against it would pass.
        session_epoch=(
            session_epoch if session_epoch is not None
            else int(account.get("session_epoch") or 0)
        ),
    )


async def actor_for_account(
    store: "AccountStore", account_id: str, *, session_epoch: int | None = None,
) -> Actor:
    """The actor of the account with this id.

    Looked up on every call. Nothing is cached: an account disabled a second
    ago must not keep being served from a previous lookup — and neither must a
    session the claim ended a second ago, which is why the epoch the credential
    carried is checked against the same row rather than in a query of its own.
    """
    if not isinstance(account_id, str) or not account_id:
        raise ActorResolutionError("This credential names no account")
    account = await store._account_identity(account_id)
    if account is None:
        raise ActorResolutionError("This credential names an account that no longer exists")
    return _actor_for_account_row(account, session_epoch=session_epoch)


async def actor_for_sole_account(
    store: "AccountStore", *, session_epoch: int | None = None,
) -> Actor:
    """Resolve the actor of the single local account, when exactly one exists.

    A legacy credential names no person, so zero or multiple accounts are
    ambiguous and must fail rather than select a row.
    """
    account = await store._sole_account_identity()
    if account is None:
        raise ActorResolutionError(
            "This credential predates per-account logins and no longer resolves "
            "to a single account; sign in again"
        )
    return _actor_for_account_row(account, session_epoch=session_epoch)


async def system_actor(store: "AccountStore") -> Actor:
    """The agent's system principal — the identity autonomous work acts as.

    Scheduled runs, hooks, background agents and the agent's own
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
