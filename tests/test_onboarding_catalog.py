"""The worker work catalog: one source for the Mode step, TASK.md and the prompt.

Integrity of :data:`nerve.onboarding_catalog.CATALOG` (ids, groups, needs,
follow-up questions), its wire shape, the first form's options, and the
:class:`WorkSelection` value.
"""

from __future__ import annotations

import json
import re

from nerve.onboarding_catalog import (
    CATALOG,
    CUSTOM_MAX,
    FIRST_FORM_ITEMS,
    FIRST_FORM_OPTIONS,
    GROUP_LABELS,
    GROUPS,
    ITEMS,
    NEEDS,
    PERSONAL_ASSISTANT,
    PERSONAL_ASSISTANT_DESCRIPTION,
    RULES_MAX,
    SHARED_QUESTIONS,
    WorkSelection,
    in_catalog_order,
    work_catalog,
)

# The contract's nine items, in display order.
CONTRACT_IDS = [
    "review-prs", "triage-issues", "fix-issues", "ci-health", "dependency-updates",
    "alert-response", "channel-digests", "research-reports", "docs-release-notes",
]


class TestCatalog:
    def test_the_nine_items_in_display_order(self):
        assert [item.id for item in CATALOG] == CONTRACT_IDS
        assert list(ITEMS) == CONTRACT_IDS

    def test_ids_are_unique_and_kebab_case(self):
        ids = [item.id for item in CATALOG]
        assert len(ids) == len(set(ids))
        assert all(re.fullmatch(r"[a-z]+(-[a-z]+)*", i) for i in ids), ids

    def test_groups_are_known_and_in_their_display_order(self):
        assert GROUPS == ("code", "operations", "communication", "knowledge")
        assert GROUP_LABELS == {
            "code": "Code", "operations": "Operations", "communication": "Communication", "knowledge": "Knowledge",
        }
        order = [GROUPS.index(item.group) for item in CATALOG]
        assert order == sorted(order), "the items of a group stand together, groups in display order"
        assert {item.group for item in CATALOG} == set(GROUPS), "no empty group"

    def test_every_item_has_at_least_three_follow_ups(self):
        for item in CATALOG:
            assert len(item.follow_ups) >= 3, item.id
            texts = [f.text for f in item.follow_ups]
            assert len(texts) == len(set(texts)), item.id
            assert all(t and t == t.strip() and not t.endswith((".", ";")) for t in texts), item.id
            # Each item brings at least one question of its own.
            assert any(f.shared is None for f in item.follow_ups), item.id

    def test_shared_follow_ups_name_a_shared_question(self):
        assert list(SHARED_QUESTIONS) == ["account", "approval", "destination"]
        for item in CATALOG:
            for follow_up in item.follow_ups:
                assert follow_up.shared is None or follow_up.shared in SHARED_QUESTIONS, (item.id, follow_up)

    def test_labels_and_descriptions(self):
        labels = [item.label for item in CATALOG]
        assert len(labels) == len(set(labels))
        for item in CATALOG:
            assert item.label != item.description, item.id
            assert item.label and item.label[0].isupper() and not item.label.endswith("."), item.id
            assert item.description.endswith(".") and "\n" not in item.description, item.id
        assert PERSONAL_ASSISTANT not in labels, "the personal assistant is no kind of team work"

    def test_needs_are_known_and_the_alert_item_carries_its_note(self):
        assert set(NEEDS) == {"github", "alerts", "slack-or-telegram", "web"}
        assert {item.needs for item in CATALOG} <= set(NEEDS)
        expected = {
            "review-prs": "github", "triage-issues": "github", "fix-issues": "github",
            "ci-health": "github", "dependency-updates": "github", "alert-response": "alerts",
            "channel-digests": "slack-or-telegram", "research-reports": "web",
            "docs-release-notes": "github",
        }
        assert {item.id: item.needs for item in CATALOG} == expected
        notes = {item.id: item.note for item in CATALOG if item.note}
        assert notes == {
            "alert-response": "Needs an alert source; a Slack channel is the first-class one today.",
        }
        assert all(item.needs_text.startswith("Needs ") for item in CATALOG)

    def test_the_wire_shape(self):
        wire = work_catalog()
        assert json.loads(json.dumps(wire)) == wire
        assert [entry["id"] for entry in wire] == CONTRACT_IDS
        for entry, item in zip(wire, CATALOG):
            assert set(entry) == {"id", "group", "label", "description", "needs", "follow_ups", "note"}
            assert entry["follow_ups"] == [f.text for f in item.follow_ups]
            assert isinstance(entry["needs"], str)
        assert wire[0] == {
            "id": "review-prs", "group": "code", "label": "Review pull requests",
            "description": "Reads diffs, runs targeted checks, posts review comments.",
            "needs": "github",
            "follow_ups": [
                "which repositories",
                "which account it acts as",
                "what a review may do: comment only, request changes, or approve",
                "drafts the person approves, or comments posted directly",
                "which pull requests: all, only on request, or by label, and whether to skip bots and drafts",
                "how often to look for new pull requests",
            ],
            "note": "",
        }

    def test_the_text_limits(self):
        assert (CUSTOM_MAX, RULES_MAX) == (4000, 2000)

    def test_the_first_form_has_four_items_then_the_personal_assistant(self):
        """The onboarding conversation's first form: four catalog items, in
        this order, with their catalog labels and descriptions, then the
        personal assistant."""
        assert FIRST_FORM_ITEMS == ("review-prs", "fix-issues", "ci-health", "research-reports")
        assert FIRST_FORM_OPTIONS == (
            ("Review pull requests", ITEMS["review-prs"].description),
            ("Fix issues and open pull requests", ITEMS["fix-issues"].description),
            ("Keep CI healthy", ITEMS["ci-health"].description),
            ("Research and reports", ITEMS["research-reports"].description),
            ("Personal assistant", "Inbox triage, reminders, planning your week."),
        )
        assert (PERSONAL_ASSISTANT, PERSONAL_ASSISTANT_DESCRIPTION) == FIRST_FORM_OPTIONS[-1]
        labels = [label for label, _ in FIRST_FORM_OPTIONS]
        assert len(labels) == len(set(labels))
        assert all(description.endswith(".") for _, description in FIRST_FORM_OPTIONS)


class TestWorkSelection:
    def test_empty_and_the_wire(self):
        assert WorkSelection().empty
        assert WorkSelection().as_dict() == {"items": [], "custom": "", "rules": ""}
        selection = WorkSelection(items=("review-prs",), custom="x", rules="y")
        assert not selection.empty
        assert selection.as_dict() == {"items": ["review-prs"], "custom": "x", "rules": "y"}
        assert not WorkSelection(rules="y").empty

    def test_a_free_text_note_equals_the_same_custom_work(self):
        assert WorkSelection(custom="x", free_text=True) == WorkSelection(custom="x")
        assert "free_text" not in WorkSelection(custom="x", free_text=True).as_dict()

    def test_chosen_items_in_catalog_order(self):
        selection = WorkSelection(items=("docs-release-notes", "no-such-work", "review-prs"))
        assert [item.id for item in selection.chosen] == ["review-prs", "docs-release-notes"]

    def test_in_catalog_order(self):
        assert in_catalog_order(["triage-issues", "review-prs", "triage-issues", "nope"]) == (
            "review-prs", "triage-issues",
        )
        assert in_catalog_order([]) == ()
