"""Transport-neutral identity and access-pattern matching."""

from __future__ import annotations

import re

import pytest

from nerve.channels.access import (
    Identity,
    PatternGate,
    needs_name_resolution,
)


class TestIdentity:
    def test_candidates_drop_empty_names(self):
        who = Identity(id="opaque-1", names=("", "alex", ""))
        assert who.candidates == ("opaque-1", "alex")

    def test_str_prefers_a_name_but_keeps_the_id(self):
        assert str(Identity(id="opaque-1", names=("alex",))) == "alex (opaque-1)"
        assert str(Identity(id="opaque-1")) == "opaque-1"
        assert str(Identity()) == "unknown"

    def test_str_falls_back_to_a_self_set_name(self):
        who = Identity(id="opaque-1", self_set_names=("Alex S",))
        assert str(who) == "Alex S (opaque-1)"

    def test_only_deny_candidates_carry_self_set_names(self):
        who = Identity(
            id="opaque-1", names=("alex",), self_set_names=("Alex S",),
        )
        assert who.candidates == ("opaque-1", "alex")
        assert who.deny_candidates == ("opaque-1", "alex", "Alex S")


class TestPatternGate:
    def test_an_empty_gate_lets_everyone_through(self):
        assert PatternGate("subject").check(Identity(id="opaque-1")).allowed

    def test_allow_matches_the_opaque_id(self):
        gate = PatternGate("subject", allow=["ID-0123ABC"])
        assert gate.check(Identity(id="ID-0123ABC")).allowed
        assert not gate.check(Identity(id="ID-9999ZZZ")).allowed

    def test_allow_matches_a_resolved_name(self):
        gate = PatternGate("subject", allow=["alex.soffronow"])
        assert gate.check(
            Identity(id="opaque-1", names=("alex.soffronow",)),
        ).allowed

    def test_matching_is_case_insensitive(self):
        gate = PatternGate("resource", allow=["ENG-Platform"])
        assert gate.check(Identity(id="opaque-1", names=("eng-platform",))).allowed

    def test_globs_match_a_family_of_names(self):
        gate = PatternGate("resource", allow=["eng-*"])
        assert gate.check(Identity(id="opaque-1", names=("eng-platform",))).allowed
        assert not gate.check(
            Identity(id="opaque-2", names=("sales-emea",)),
        ).allowed

    def test_deny_beats_allow(self):
        gate = PatternGate("resource", allow=["eng-*"], deny=["eng-secret"])
        assert not gate.check(
            Identity(id="opaque-1", names=("eng-secret",)),
        ).allowed

    def test_deny_alone_admits_everything_else(self):
        gate = PatternGate("subject", deny=["*-bot"])
        assert gate.check(Identity(id="opaque-1", names=("alex",))).allowed
        assert not gate.check(
            Identity(id="opaque-2", names=("deploy-bot",)),
        ).allowed

    def test_an_unidentified_subject_cannot_satisfy_an_allow_list(self):
        gate = PatternGate("subject", allow=["alex"])
        assert not gate.check(Identity()).allowed

    def test_a_self_set_name_cannot_satisfy_an_allow_list(self):
        # Otherwise the subject picks its own access: it renames itself to
        # whatever the allow list happens to say.
        gate = PatternGate("subject", allow=["alex.soffronow"])
        mallory = Identity(
            id="opaque-2",
            names=("mallory",),
            self_set_names=("alex.soffronow", "Alex Soffronow"),
        )
        verdict = gate.check(mallory)
        assert not verdict.allowed
        assert "sets itself" in verdict.reason
        # The real holder of the handle is unaffected.
        assert gate.check(
            Identity(id="opaque-1", names=("alex.soffronow",)),
        ).allowed

    def test_a_self_set_name_still_satisfies_a_deny_list(self):
        # Refusing on more names than a grant may rest on is always safe.
        gate = PatternGate("subject", deny=["*-bot"])
        assert not gate.check(
            Identity(
                id="opaque-1",
                names=("integration-42",),
                self_set_names=("deploy-bot",),
            ),
        ).allowed

    def test_an_incomplete_name_set_is_refused_when_a_deny_list_exists(self):
        # The whole point: a deny list that cannot be evaluated must not
        # quietly pass the subject it was written to stop.
        gate = PatternGate("subject", deny=["*-bot"])
        assert not gate.check(Identity(id="opaque-1", complete=False)).allowed

    def test_an_incomplete_name_set_is_fine_when_only_allow_ids_are_used(self):
        gate = PatternGate("subject", allow=["opaque-1"])
        assert gate.check(Identity(id="opaque-1", complete=False)).allowed

    def test_any_deny_pattern_ignores_the_allow_list(self):
        gate = PatternGate(
            "subject", allow=["a@b.c"], deny=["blocked@example.com"],
        )
        assert gate.any_deny_pattern(lambda p: "@" in p)
        # An allow pattern is not a deny pattern: allow already fails closed.
        assert not PatternGate("subject", allow=["a@b.c"]).any_deny_pattern(
            lambda p: "@" in p,
        )

    def test_the_reason_names_the_gate_and_the_pattern(self):
        gate = PatternGate("resource", deny=["*-random"])
        verdict = gate.check(Identity(id="opaque-1", names=("eng-random",)))
        assert "resource" in verdict.reason
        assert "*-random" in verdict.reason


def _is_id(pattern: str) -> bool:
    """Stand-in for a transport's literal-ID predicate."""
    return bool(re.fullmatch(r"ID-[A-Z0-9]+", pattern))


class TestNeedsNameResolution:
    def test_plain_ids_need_no_lookup(self):
        assert not needs_name_resolution(
            PatternGate("subject", allow=["ID-0123ABC", "ID-456DEF"]),
            is_id=_is_id,
        )

    def test_a_glob_needs_a_lookup(self):
        assert needs_name_resolution(
            PatternGate("resource", allow=["ENG-*"]), is_id=_is_id,
        )

    def test_a_lowercase_handle_needs_a_lookup(self):
        assert needs_name_resolution(
            PatternGate("subject", allow=["alex.soffronow"]), is_id=_is_id,
        )

    def test_a_deny_pattern_counts_too(self):
        assert needs_name_resolution(
            PatternGate(
                "subject", allow=["ID-0123ABC"], deny=["*-bot"],
            ),
            is_id=_is_id,
        )

    def test_an_empty_gate_needs_nothing(self):
        assert not needs_name_resolution(PatternGate("subject"), is_id=_is_id)

    def test_an_uppercase_name_is_not_mistaken_for_an_id(self):
        # A case heuristic read ALICE as an id, skipped the lookup, and let
        # the deny list pass the person it named.
        assert needs_name_resolution(
            PatternGate("subject", deny=["ALICE"]), is_id=_is_id,
        )
        assert needs_name_resolution(
            PatternGate("resource", deny=["ENGINEERING"]), is_id=_is_id,
        )

    def test_a_glob_is_never_an_id_however_it_is_spelled(self):
        assert needs_name_resolution(
            PatternGate("subject", allow=["ID-0123AB*"]), is_id=lambda p: True,
        )

    def test_omitting_the_predicate_forces_a_lookup(self):
        # The safe direction: a wrong "this is an id" guess skips a lookup
        # the deny list depends on, so no guess means always look up.
        assert needs_name_resolution(
            PatternGate("subject", allow=["ID-0123ABC"]),
        )


# Deny rules must fail closed for every lookup outcome and pattern kind.

# How the transport's name lookup turned out, as the Identity it produces
# for one subject whose handle is "alice" and whose email is "a@corp.com".
LOOKUPS = {
    # Skipped: every pattern was a literal id, so the id alone suffices.
    "skipped": Identity(id="ID-0123ABC", complete=True),
    # Complete: every alias the patterns need came back.
    "complete": Identity(
        id="ID-0123ABC", names=("alice", "a@corp.com"), complete=True,
    ),
    # Partial: the provider returned an identity without a required alias.
    "partial": Identity(id="ID-0123ABC", names=("alice",), complete=False),
    # Failed: the lookup raised.
    "failed": Identity(id="ID-0123ABC", complete=False),
}

# A deny pattern of each kind, and whether the "complete" identity matches it.
DENY_PATTERNS = {
    "id": ("ID-0123ABC", True),
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
    gate = PatternGate("subject", allow=["*"], deny=[pattern])

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
    assert not PatternGate("subject", allow=["mallory"]).check(who).allowed


@pytest.mark.parametrize("lookup_name", sorted(LOOKUPS))
def test_an_id_allow_list_admits_on_every_lookup_outcome(lookup_name):
    """The id is the one candidate always present, so an id allow list works
    even when the name lookup failed."""
    who = LOOKUPS[lookup_name]
    assert PatternGate("subject", allow=["ID-0123ABC"]).check(who).allowed
