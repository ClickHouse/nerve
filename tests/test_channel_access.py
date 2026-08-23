"""Access guardrails for chat channels — allow/deny semantics.

The names describe the refusal each case is here to keep working.
"""

from __future__ import annotations

import re

import pytest

from nerve.channels.access import (
    AccessPolicy,
    Gate,
    Identity,
    needs_name_resolution,
)


class TestIdentity:
    def test_candidates_drop_empty_names(self):
        who = Identity(id="U1", names=("", "alex", ""))
        assert who.candidates == ("U1", "alex")

    def test_str_prefers_a_name_but_keeps_the_id(self):
        assert str(Identity(id="U1", names=("alex",))) == "alex (U1)"
        assert str(Identity(id="U1")) == "U1"
        assert str(Identity()) == "unknown"


class TestGate:
    def test_an_empty_gate_lets_everyone_through(self):
        assert Gate("user").check(Identity(id="U1")).allowed

    def test_allow_matches_the_opaque_id(self):
        gate = Gate("user", allow=["U0123ABC"])
        assert gate.check(Identity(id="U0123ABC")).allowed
        assert not gate.check(Identity(id="U9999ZZZ")).allowed

    def test_allow_matches_a_resolved_name(self):
        gate = Gate("user", allow=["alex.soffronow"])
        assert gate.check(Identity(id="U1", names=("alex.soffronow",))).allowed

    def test_matching_is_case_insensitive(self):
        gate = Gate("channel", allow=["ENG-Platform"])
        assert gate.check(Identity(id="C1", names=("eng-platform",))).allowed

    def test_globs_match_a_family_of_channels(self):
        gate = Gate("channel", allow=["eng-*"])
        assert gate.check(Identity(id="C1", names=("eng-platform",))).allowed
        assert not gate.check(Identity(id="C2", names=("sales-emea",))).allowed

    def test_deny_beats_allow(self):
        gate = Gate("channel", allow=["eng-*"], deny=["eng-secret"])
        assert not gate.check(Identity(id="C1", names=("eng-secret",))).allowed

    def test_deny_alone_admits_everything_else(self):
        gate = Gate("user", deny=["*-bot"])
        assert gate.check(Identity(id="U1", names=("alex",))).allowed
        assert not gate.check(Identity(id="U2", names=("deploy-bot",))).allowed

    def test_an_unidentified_subject_cannot_satisfy_an_allow_list(self):
        gate = Gate("user", allow=["alex"])
        assert not gate.check(Identity()).allowed

    def test_an_incomplete_name_set_is_refused_when_a_deny_list_exists(self):
        # The whole point: a deny list that cannot be evaluated must not
        # quietly pass the subject it was written to stop.
        gate = Gate("user", deny=["*-bot"])
        assert not gate.check(Identity(id="U1", complete=False)).allowed

    def test_an_incomplete_name_set_is_fine_when_only_allow_ids_are_used(self):
        gate = Gate("user", allow=["U1"])
        assert gate.check(Identity(id="U1", complete=False)).allowed

    def test_deny_needs_reports_patterns_the_lookup_may_not_cover(self):
        gate = Gate("user", allow=["a@b.c"], deny=["blocked@example.com"])
        assert gate.deny_needs(lambda p: "@" in p)
        # An allow pattern is not a deny pattern: allow already fails closed.
        assert not Gate("user", allow=["a@b.c"]).deny_needs(lambda p: "@" in p)

    def test_the_reason_names_the_gate_and_the_pattern(self):
        gate = Gate("channel", deny=["*-random"])
        verdict = gate.check(Identity(id="C1", names=("eng-random",)))
        assert "channel" in verdict.reason
        assert "*-random" in verdict.reason


class TestAccessPolicy:
    def test_nothing_configured_means_nobody(self):
        policy = AccessPolicy.from_lists()
        assert not policy.configured
        verdict = policy.check(Identity(id="U1"), Identity(id="C1"))
        assert not verdict.allowed
        assert "no allow_users or allow_channels" in verdict.reason

    def test_a_deny_list_alone_still_refuses_everyone(self):
        # A deny list is not an opt-in. Without an allow list the operator
        # has not said who may talk to the agent.
        policy = AccessPolicy.from_lists(deny_users=["*-bot"])
        assert not policy.configured
        assert not policy.check(Identity(id="U1"), Identity(id="C1")).allowed

    def test_both_gates_must_pass(self):
        policy = AccessPolicy.from_lists(
            allow_users=["U1"], allow_channels=["eng-*"],
        )
        allowed_channel = Identity(id="C1", names=("eng-platform",))
        other_channel = Identity(id="C2", names=("sales",))
        assert policy.check(Identity(id="U1"), allowed_channel).allowed
        assert not policy.check(Identity(id="U2"), allowed_channel).allowed
        assert not policy.check(Identity(id="U1"), other_channel).allowed

    def test_allow_users_alone_admits_every_conversation(self):
        policy = AccessPolicy.from_lists(allow_users=["U1"])
        assert policy.check(Identity(id="U1"), Identity(id="C9")).allowed

    def test_allow_channels_alone_admits_every_member_of_them(self):
        policy = AccessPolicy.from_lists(allow_channels=["eng-*"])
        who = Identity(id="U-anyone")
        assert policy.check(who, Identity(id="C1", names=("eng-x",))).allowed
        assert not policy.check(who, Identity(id="C2", names=("hr",))).allowed

    def test_a_dm_is_matched_by_the_synthetic_dm_name(self):
        policy = AccessPolicy.from_lists(allow_channels=["dm"])
        assert policy.check(
            Identity(id="U1"), Identity(id="D1", names=("dm",)),
        ).allowed

    def test_allow_channels_without_dm_shuts_direct_messages_out(self):
        policy = AccessPolicy.from_lists(allow_channels=["eng-*"])
        assert not policy.check(
            Identity(id="U1"), Identity(id="D1", names=("dm",)),
        ).allowed

    def test_the_user_gate_is_reported_before_the_channel_gate(self):
        policy = AccessPolicy.from_lists(
            allow_users=["U1"], allow_channels=["eng-*"],
        )
        verdict = policy.check(Identity(id="U2"), Identity(id="C2", names=("hr",)))
        assert "user" in verdict.reason

    def test_describe_counts_patterns_without_leaking_them(self):
        policy = AccessPolicy.from_lists(
            allow_users=["U1", "U2"], deny_channels=["secret-*"],
        )
        summary = policy.describe()
        assert "allow=2" in summary
        assert "U1" not in summary
        assert "secret-*" not in summary


def _is_id(pattern: str) -> bool:
    """Stand-in for a platform's id predicate (Slack-shaped)."""
    return bool(re.fullmatch(r"[UWBCDGT][A-Z0-9]{7,}", pattern))


class TestNeedsNameResolution:
    def test_plain_ids_need_no_lookup(self):
        assert not needs_name_resolution(
            Gate("user", allow=["U0123ABC", "W0123ABC"]), is_id=_is_id,
        )

    def test_a_glob_needs_a_lookup(self):
        assert needs_name_resolution(Gate("channel", allow=["ENG-*"]), is_id=_is_id)

    def test_a_lowercase_handle_needs_a_lookup(self):
        assert needs_name_resolution(
            Gate("user", allow=["alex.soffronow"]), is_id=_is_id,
        )

    def test_a_deny_pattern_counts_too(self):
        assert needs_name_resolution(
            Gate("user", allow=["U0123ABC"], deny=["*-bot"]), is_id=_is_id,
        )

    def test_an_empty_gate_needs_nothing(self):
        assert not needs_name_resolution(Gate("user"), is_id=_is_id)

    def test_an_uppercase_name_is_not_mistaken_for_an_id(self):
        # A case heuristic read ALICE as an id, skipped the lookup, and let
        # the deny list pass the person it named.
        assert needs_name_resolution(Gate("user", deny=["ALICE"]), is_id=_is_id)
        assert needs_name_resolution(
            Gate("channel", deny=["ENGINEERING"]), is_id=_is_id,
        )

    def test_a_glob_is_never_an_id_however_it_is_spelled(self):
        assert needs_name_resolution(
            Gate("user", allow=["U0123AB*"]), is_id=lambda p: True,
        )

    def test_omitting_the_predicate_forces_a_lookup(self):
        # The safe direction: a wrong "this is an id" guess skips a lookup
        # the deny list depends on, so no guess means always look up.
        assert needs_name_resolution(Gate("user", allow=["U0123ABC"]))


# ---------------------------------------------------------------------- #
#  The matrix                                                             #
#                                                                         #
#  Both fail-open bugs found in review were cells nobody had written: a   #
#  deny list checked against a candidate set that was never fetched, and  #
#  one fetched but missing the alias the pattern named. So enumerate the  #
#  lookup outcomes against the pattern kinds and assert the rule that     #
#  holds in every cell — a deny list is only honoured when the candidate  #
#  set is known to cover it, and is refused otherwise.                    #
# ---------------------------------------------------------------------- #

# How the transport's name lookup turned out, as the Identity it produces
# for user U0123ABC whose handle is "alice" and whose email is "a@corp.com".
LOOKUPS = {
    # Skipped: every pattern was a literal id, so the id alone suffices.
    "skipped": Identity(id="U0123ABC", complete=True),
    # Complete: every alias the patterns need came back.
    "complete": Identity(
        id="U0123ABC", names=("alice", "a@corp.com"), complete=True,
    ),
    # Partial: 200 OK, but an alias the deny list needs was withheld
    # (Slack drops profile.email without users:read.email).
    "partial": Identity(id="U0123ABC", names=("alice",), complete=False),
    # Failed: the lookup raised.
    "failed": Identity(id="U0123ABC", complete=False),
}

# A deny pattern of each kind, and whether the "complete" identity matches it.
DENY_PATTERNS = {
    "id": ("U0123ABC", True),
    "handle": ("alice", True),
    "email": ("a@corp.com", True),
    "glob": ("al*", True),
    "uppercase-name": ("ALICE", True),
    "unrelated": ("mallory", False),
}


@pytest.mark.parametrize("lookup_name", sorted(LOOKUPS))
@pytest.mark.parametrize("pattern_name", sorted(DENY_PATTERNS))
def test_a_deny_list_is_never_evaluated_against_an_incomplete_name_set(
    lookup_name, pattern_name,
):
    """A deny list must refuse unless it can prove the subject is not on it.

    Only a complete candidate set can prove that. Anything else — a lookup
    that failed, or one that came back short — is refused, whether or not
    the pattern happens to match what little was read.
    """
    who = LOOKUPS[lookup_name]
    pattern, matches_complete_identity = DENY_PATTERNS[pattern_name]
    gate = Gate("user", allow=["*"], deny=[pattern])

    verdict = gate.check(who)

    if not who.complete:
        assert not verdict.allowed, (
            f"{lookup_name} lookup + {pattern_name} deny pattern was admitted; "
            "an incomplete candidate set cannot clear a deny list"
        )
        return

    # A complete set is evaluated on its merits. The skipped-lookup identity
    # carries only the id, which is complete precisely when the pattern is
    # an id — the case that let the lookup be skipped in the first place.
    expected_match = (
        matches_complete_identity if who.names else pattern_name == "id"
    )
    assert verdict.allowed is not expected_match


@pytest.mark.parametrize("lookup_name", sorted(LOOKUPS))
def test_an_allow_list_fails_closed_on_every_lookup_outcome(lookup_name):
    """An allow list needs no completeness rule — an unread name matches
    nothing, so a short candidate set refuses on its own."""
    who = LOOKUPS[lookup_name]
    assert not Gate("user", allow=["mallory"]).check(who).allowed


@pytest.mark.parametrize("lookup_name", sorted(LOOKUPS))
def test_an_id_allow_list_admits_on_every_lookup_outcome(lookup_name):
    """The id is the one candidate always present, so an id allow list works
    even when the name lookup failed."""
    who = LOOKUPS[lookup_name]
    assert Gate("user", allow=["U0123ABC"]).check(who).allowed
