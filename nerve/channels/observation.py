"""Who the agent may watch — a grant distinct from who may command it.

An access policy answers "may this person drive the agent?". This one
answers "may this conversation feed the agent's inbox?". They are not the
same question, and conflating them fails in both directions: reusing the
access policy either blocks watching a channel the agent takes no orders
from, or silently widens command access to everything worth watching.

So the source gets its own gate, composed from the same
:mod:`nerve.channels.access` primitives, and the two are evaluated
independently: a message may be live-routed, collected, both, or neither. This
gate is off unless configured, and a conversation must be named explicitly —
there is no "watch everything the bot can see" by omission, because that is
what a misconfiguration looks like.

Most of what it approves comes from people who are *not* authorized to
instruct the agent — that is the usual reason to watch a room. Everything
buffered is untrusted. This gate limits which messages are stored; neither it
nor the inbox filter validates message content.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from nerve.channels.access import Decision, Identity, PatternGate


@dataclass
class ObservationPolicy:
    """Whether a conversation and sender may be buffered to the inbox.

    ``conversations`` is fail-closed by design: an empty allow list collects
    nothing at all, rather than everything. That inverts
    :class:`~nerve.channels.access.PatternGate`'s default, which is right for
    an access check composed after a user gate and wrong for a standing grant
    to record other people's messages.

    ``senders`` is the opposite — usually empty, meaning "anyone talking in an
    approved conversation". Narrowing it to specific people is possible but
    unusual; the interesting unit here is the room, not the speaker. A deny
    list is the common use: skip a noisy bot.
    """

    enabled: bool = False
    conversations: PatternGate = field(
        default_factory=lambda: PatternGate("conversation"),
    )
    senders: PatternGate = field(default_factory=lambda: PatternGate("sender"))

    @property
    def active(self) -> bool:
        """Whether this policy can ever approve anything."""
        return self.enabled and bool(self.conversations.allow)

    def check(self, conversation: Identity, sender: Identity) -> Decision:
        """Decide whether one message may be buffered."""
        if not self.enabled:
            return Decision(False, "the channel source is not enabled")
        if not self.conversations.allow:
            return Decision(
                False,
                "no conversations are approved for the channel source",
            )
        verdict = self.conversations.check(conversation)
        if not verdict.allowed:
            return verdict
        return self.senders.check(sender)


__all__ = ["ObservationPolicy"]
