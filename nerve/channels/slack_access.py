"""Slack-specific composition of the shared access matching primitives."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from nerve.channels.access import Decision, Identity, PatternGate

if TYPE_CHECKING:
    from nerve.config import SlackConfig


@dataclass
class SlackAccessPolicy:
    """Apply Slack's user, channel, and direct-message guardrails."""

    users: PatternGate = field(default_factory=lambda: PatternGate("user"))
    channels: PatternGate = field(default_factory=lambda: PatternGate("channel"))
    allow_direct_messages: bool = False

    @classmethod
    def from_config(cls, config: SlackConfig) -> SlackAccessPolicy:
        """Build a policy from the live Slack configuration."""
        return cls(
            users=PatternGate(
                "user",
                allow=list(config.allow_users),
                deny=list(config.deny_users),
            ),
            channels=PatternGate(
                "channel",
                allow=list(config.allow_channels),
                deny=list(config.deny_channels),
            ),
            allow_direct_messages=config.allow_direct_messages,
        )

    @property
    def configured(self) -> bool:
        """Whether Slack has any explicit access grant."""
        return bool(
            self.users.allow
            or self.channels.allow
            or self.allow_direct_messages
        )

    def preflight(self, *, direct_message: bool) -> Decision | None:
        """Return a decision that can be made before resolving Slack aliases."""
        if not self.configured:
            return Decision(
                False,
                "no slack.allow_users, slack.allow_channels, or "
                "slack.allow_direct_messages configured",
            )
        if direct_message and not self.allow_direct_messages:
            return Decision(False, "direct messages are not allowed")
        return None

    def check(
        self, user: Identity, channel: Identity, *, direct_message: bool = False,
    ) -> Decision:
        """Decide whether a Slack user may interact in this conversation."""
        early = self.preflight(direct_message=direct_message)
        if early is not None:
            return early

        verdict = self.users.check(user)
        if not verdict.allowed:
            return verdict

        if direct_message:
            return Decision(True, "direct messages are allowed")

        # A DM grant alone must not open shared channels.
        if not (self.users.allow or self.channels.allow):
            return Decision(
                False,
                "slack.allow_direct_messages does not allow shared channels",
            )
        return self.channels.check(channel)

    def check_outbound(self, channel: Identity) -> Decision:
        """Decide whether the agent may post to a shared conversation unasked.

        Read in the write direction the policy is short one term: there is no
        sender to run through :attr:`users`. An allow list of users therefore
        grants nothing here — it says who may drive the agent, not where the
        agent may broadcast — so an explicit :attr:`channels` grant is
        required, the same instinct as refusing shared channels to a lone
        ``allow_direct_messages``.
        """
        if not self.channels.allow:
            return Decision(
                False,
                "no slack.allow_channels configured, so no conversation is "
                "approved for addressed delivery",
            )
        return self.channels.check(channel)

    def describe(self) -> str:
        """Summarize the policy without exposing configured patterns."""
        return (
            f"users(allow={len(self.users.allow)}, deny={len(self.users.deny)}) "
            f"channels(allow={len(self.channels.allow)}, "
            f"deny={len(self.channels.deny)}) "
            f"direct_messages={'allowed' if self.allow_direct_messages else 'refused'}"
        )


__all__ = ["SlackAccessPolicy"]
