"""Slack-specific composition of user, channel, and DM access rules."""

from nerve.channels.access import Identity
from nerve.channels.slack_access import SlackAccessPolicy
from nerve.config import SlackConfig


def _policy(**values) -> SlackAccessPolicy:
    return SlackAccessPolicy.from_config(SlackConfig(**values))


class TestSlackAccessPolicy:
    def test_nothing_configured_means_nobody(self):
        policy = _policy()
        assert not policy.configured
        verdict = policy.check(Identity(id="U1"), Identity(id="C1"))
        assert not verdict.allowed
        assert "slack.allow_direct_messages" in verdict.reason

    def test_a_deny_list_alone_still_refuses_everyone(self):
        policy = _policy(deny_users=["*-bot"])
        assert not policy.configured
        assert not policy.check(Identity(id="U1"), Identity(id="C1")).allowed

    def test_both_gates_must_pass(self):
        policy = _policy(allow_users=["U1"], allow_channels=["eng-*"])
        allowed_channel = Identity(id="C1", names=("eng-platform",))
        other_channel = Identity(id="C2", names=("sales",))
        assert policy.check(Identity(id="U1"), allowed_channel).allowed
        assert not policy.check(Identity(id="U2"), allowed_channel).allowed
        assert not policy.check(Identity(id="U1"), other_channel).allowed

    def test_allow_users_alone_admits_every_shared_channel(self):
        policy = _policy(allow_users=["U1"])
        assert policy.check(Identity(id="U1"), Identity(id="C9")).allowed

    def test_allow_channels_alone_admits_every_member(self):
        policy = _policy(allow_channels=["eng-*"])
        user = Identity(id="U-anyone")
        assert policy.check(user, Identity(id="C1", names=("eng-x",))).allowed
        assert not policy.check(user, Identity(id="C2", names=("hr",))).allowed

    def test_direct_messages_need_the_explicit_setting(self):
        policy = _policy(allow_users=["U1"])
        assert not policy.check(
            Identity(id="U1"), Identity(id="D1"), direct_message=True,
        ).allowed

    def test_the_direct_message_setting_admits_a_dm(self):
        policy = _policy(allow_direct_messages=True)
        assert policy.check(
            Identity(id="U1"), Identity(id="D1"), direct_message=True,
        ).allowed

    def test_the_direct_message_setting_still_respects_the_user_gate(self):
        policy = _policy(allow_users=["U1"], allow_direct_messages=True)
        assert policy.check(
            Identity(id="U1"), Identity(id="D1"), direct_message=True,
        ).allowed
        assert not policy.check(
            Identity(id="U2"), Identity(id="D1"), direct_message=True,
        ).allowed

    def test_the_direct_message_setting_does_not_open_shared_channels(self):
        policy = _policy(allow_direct_messages=True)
        assert not policy.check(
            Identity(id="U1"), Identity(id="C1", names=("general",)),
        ).allowed

    def test_the_user_gate_is_reported_before_the_channel_gate(self):
        policy = _policy(allow_users=["U1"], allow_channels=["eng-*"])
        verdict = policy.check(
            Identity(id="U2"), Identity(id="C2", names=("hr",)),
        )
        assert "user" in verdict.reason

    def test_preflight_refuses_without_resolving_aliases(self):
        assert _policy().preflight(direct_message=False) is not None
        assert _policy(allow_users=["U1"]).preflight(
            direct_message=True,
        ) is not None
        assert _policy(allow_channels=["C1"]).preflight(
            direct_message=False,
        ) is None

    def test_describe_counts_patterns_without_leaking_them(self):
        policy = _policy(
            allow_users=["U1", "U2"], deny_channels=["secret-*"],
        )
        summary = policy.describe()
        assert "allow=2" in summary
        assert "direct_messages=refused" in summary
        assert "U1" not in summary
        assert "secret-*" not in summary
