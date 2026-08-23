"""Channel access guardrails — who may talk to the agent, and where.

A channel that reaches a shared workspace (Slack, Discord) sees traffic the
operator never meant for the agent. This module is the choke point between
the transport and :class:`~nerve.channels.router.ChannelRouter`: a message
that fails the policy never becomes an
:class:`~nerve.channels.base.InboundMessage`, so it costs no tokens and
cannot reach a tool.

A policy holds two independent gates — one for the *sender*, one for the
*conversation*. Both must pass. Each gate follows the same semantics as
:mod:`nerve.sources.filters`:

* **deny wins** — a value matching any deny pattern is refused, whatever the
  allow list says.
* **allow is a gate** — when the allow list is non-empty the value MUST match
  one of its patterns.
* an empty allow list means "allow anything not denied".

Matching is case-insensitive and supports shell-style globs (``eng-*``). A
subject is matched by every name it is known by — its opaque platform id
(``U0123ABC``), its handle, its display name, its email — so
``allow_users: ["U0123ABC"]`` and ``allow_users: ["alex.soffronow"]`` both
work.

Two rules make the failure modes safe rather than convenient:

* **Nothing configured means nobody.** A policy with no allow patterns on
  either gate refuses everything. An operator who enables the channel and
  forgets the lists gets silence and a startup warning, not an agent open to
  the whole workspace.
* **Unverifiable means denied.** Deny patterns are matched against names the
  transport has to look up. Unless the candidate set is known to cover every
  name a pattern could match, the gate cannot prove the subject is *not*
  denied, so it refuses. An allow list already fails closed on its own (an
  unknown name matches no pattern), but a deny list would otherwise fail
  open — the asymmetry :func:`nerve.coerce._scalar_to_list` documents for
  exclude lists.

  This is what :attr:`Identity.complete` records, and it is a stronger claim
  than "the lookup returned 200". A lookup that succeeds while silently
  omitting an alias — Slack drops ``profile.email`` when the token lacks
  ``users:read.email`` — leaves the set incomplete, and a deny list naming
  that email would pass a subject it was written to stop.
"""

from __future__ import annotations

import fnmatch
import logging
from dataclasses import dataclass, field
from typing import Callable

logger = logging.getLogger(__name__)


def _norm(value: object) -> str:
    """Normalize a value for case-insensitive matching."""
    return str(value).strip().lower()


def _matches(value: str, pattern: str) -> bool:
    """Case-insensitive shell-glob match of one value against one pattern."""
    return fnmatch.fnmatchcase(_norm(value), _norm(pattern))


@dataclass(frozen=True)
class Identity:
    """The names one subject may be matched by.

    ``id`` is the opaque platform identifier (``U0123ABC``, ``C0456DEF``);
    ``names`` are the human-facing spellings — handle, display name, email,
    channel name. A pattern matches the subject if it matches any of them.

    ``complete`` asserts that :attr:`candidates` covers every name a
    configured pattern could match. It is False when a lookup failed, when a
    lookup returned but omitted an alias the patterns need, and whenever the
    transport is unsure. Gates with a deny list refuse an incomplete subject
    rather than guess.

    Note that ``complete`` is a claim about the patterns in force, not about
    the subject in the abstract: when every pattern is a platform id, the id
    alone is a complete candidate set and no lookup is needed.
    """

    id: str = ""
    names: tuple[str, ...] = ()
    complete: bool = True

    @property
    def candidates(self) -> tuple[str, ...]:
        """Every non-empty string this subject can be matched by."""
        return tuple(v for v in (self.id, *self.names) if v)

    def __str__(self) -> str:
        if self.names and self.id:
            return f"{self.names[0]} ({self.id})"
        return self.names[0] if self.names else (self.id or "unknown")


@dataclass(frozen=True)
class Decision:
    """The outcome of a policy check, with a reason fit for a log line."""

    allowed: bool
    reason: str = ""

    def __bool__(self) -> bool:
        return self.allowed


@dataclass
class Gate:
    """Allow/deny matching for one kind of subject.

    ``subject`` names the kind ("user", "channel") and appears in
    :attr:`Decision.reason`, so a refusal log says which gate refused.
    """

    subject: str
    allow: list[str] = field(default_factory=list)
    deny: list[str] = field(default_factory=list)

    @property
    def active(self) -> bool:
        """Whether this gate constrains anything."""
        return bool(self.allow or self.deny)

    def deny_needs(self, predicate: Callable[[str], bool]) -> bool:
        """Whether any deny pattern satisfies *predicate*.

        Lets a transport ask "does my deny list rest on an alias I might not
        be able to read?" — an email, say, which Slack withholds without the
        ``users:read.email`` scope — so it can mark the identity incomplete
        instead of matching against a set it knows is short.
        """
        return any(predicate(p) for p in self.deny if p)

    def check(self, who: Identity) -> Decision:
        """Decide whether *who* passes this gate."""
        candidates = who.candidates

        for pattern in self.deny:
            for value in candidates:
                if _matches(value, pattern):
                    return Decision(
                        False,
                        f"{self.subject} {who} matches deny pattern {pattern!r}",
                    )

        # A deny list is only meaningful against a candidate set known to
        # cover it. Refuse rather than let an unread name walk past the list
        # that names it.
        if self.deny and not who.complete:
            return Decision(
                False,
                f"{self.subject} {who} could not be fully identified, so the "
                f"deny list cannot be checked",
            )

        if self.allow:
            if not candidates:
                return Decision(
                    False, f"{self.subject} is unidentified and an allow list is set",
                )
            for pattern in self.allow:
                for value in candidates:
                    if _matches(value, pattern):
                        return Decision(
                            True,
                            f"{self.subject} {who} matches allow pattern {pattern!r}",
                        )
            return Decision(False, f"{self.subject} {who} is not on the allow list")

        return Decision(True, "")


@dataclass
class AccessPolicy:
    """Sender and conversation gates, applied together.

    Build one per channel from its config and re-read it per message, so a
    config reload that tightens the lists takes effect without a restart.
    """

    users: Gate = field(default_factory=lambda: Gate("user"))
    conversations: Gate = field(default_factory=lambda: Gate("channel"))

    @property
    def configured(self) -> bool:
        """Whether either gate has an allow list.

        False means the policy refuses everything — see the module docstring.
        """
        return bool(self.users.allow or self.conversations.allow)

    def check(self, user: Identity, conversation: Identity) -> Decision:
        """Decide whether *user* may talk to the agent in *conversation*."""
        if not self.configured:
            return Decision(
                False,
                "no allow_users or allow_channels configured — refusing "
                f"{user} in {conversation}",
            )
        verdict = self.users.check(user)
        if not verdict.allowed:
            return verdict
        return self.conversations.check(conversation)

    @classmethod
    def from_lists(
        cls,
        allow_users: list[str] | None = None,
        deny_users: list[str] | None = None,
        allow_channels: list[str] | None = None,
        deny_channels: list[str] | None = None,
    ) -> AccessPolicy:
        """Build a policy from four flat pattern lists (the config shape)."""
        return cls(
            users=Gate("user", allow=list(allow_users or []), deny=list(deny_users or [])),
            conversations=Gate(
                "channel",
                allow=list(allow_channels or []),
                deny=list(deny_channels or []),
            ),
        )

    def describe(self) -> str:
        """One-line summary for the startup log (no secrets involved)."""
        return (
            f"users(allow={len(self.users.allow)}, deny={len(self.users.deny)}) "
            f"channels(allow={len(self.conversations.allow)}, "
            f"deny={len(self.conversations.deny)})"
        )


def needs_name_resolution(
    *gates: Gate, is_id: Callable[[str], bool] | None = None,
) -> bool:
    """Whether any pattern in *gates* can only match a looked-up name.

    When every pattern is a literal platform id, the subject's own id is a
    complete candidate set and the transport can skip the lookup — one fewer
    API call per message, and one fewer scope to grant. Any other pattern
    means handles and channel names must be resolved first.

    ``is_id`` decides what a literal platform id looks like; the caller owns
    that because the shape is platform-specific. **Omitting it means nothing
    is treated as an id and every pattern forces a lookup** — the safe
    direction, since a wrong "this is an id" guess skips the lookup and lets
    a deny list pass the subject it names.

    A pattern containing a glob is never an id, whatever ``is_id`` says: it
    stands for a set, and the members have to be read to be compared.
    """
    for gate in gates:
        for pattern in (*gate.allow, *gate.deny):
            if not pattern:
                continue
            if any(c in pattern for c in "*?["):
                return True
            if is_id is None or not is_id(pattern):
                return True
    return False


__all__ = [
    "AccessPolicy",
    "Decision",
    "Gate",
    "Identity",
    "needs_name_resolution",
]
