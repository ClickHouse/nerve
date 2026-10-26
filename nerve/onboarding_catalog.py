"""What a worker can be set up to do: the work catalog.

The one source of truth for three places:

- the Mode step's checkboxes (``GET /api/setup`` → the mode step's
  ``detail.work_catalog``, :func:`work_catalog`);
- the worker's ``TASK.md``, where a selection is written as the
  ``## Task description (as given)`` section
  (:func:`nerve.setup_writer.render_work_description`) and read back
  (:func:`nerve.setup_writer.work_selection_from`);
- the onboarding conversation's hidden first message
  (:func:`nerve.agent.prompts.onboarding_prompt`): the chosen items with
  their follow-up questions, or, with nothing chosen, the first form
  (:data:`FIRST_FORM_OPTIONS`) and the catalog as reference.

Each item names the connection it needs, so the conversation can say what
the instance lacks, and the questions the conversation settles. Three
questions concern all of the work and are asked once, not per item: the
account the agent acts as, the approval boundary and where results go
(:data:`SHARED_QUESTIONS`). An item's follow-up that is one of them carries
its key in :attr:`FollowUp.shared`.

No imports from the rest of Nerve: the writer, the checklist and the
prompts all read this module.
"""

from __future__ import annotations

from dataclasses import dataclass, field

#: The groups, in display order.
GROUPS = ("code", "operations", "communication", "knowledge")
#: A group's heading: its id, title-cased.
GROUP_LABELS = {group: group.title() for group in GROUPS}

#: What an item needs from the instance, and how the prompt says it.
NEEDS = {
    "github": "Needs GitHub access.",
    "alerts": "Needs an alert source; a Slack channel is the first-class one today.",
    "slack-or-telegram": "Needs a Slack or Telegram connection.",
    "web": "Needs web access.",
}

#: The questions asked once for all of the work, by key, in the order the
#: conversation lists them. The account is asked only when the work acts on
#: GitHub; the other two always.
SHARED_ACCOUNT = "account"
SHARED_APPROVAL = "approval"
SHARED_DESTINATION = "destination"
SHARED_QUESTIONS = {
    SHARED_ACCOUNT: "the account you act as on the code host, and the git author to use",
    SHARED_APPROVAL: "the approval boundary: what you do on your own, and what waits for the person",
    SHARED_DESTINATION: "where results go: this chat, a channel, or notifications only",
}

#: The longest custom work and rules the Mode step takes, in characters.
CUSTOM_MAX = 4000
RULES_MAX = 2000

#: The first form's option for a personal install (never a worker's): not a
#: kind of team work, so not in the catalog.
PERSONAL_ASSISTANT = "Personal assistant"
PERSONAL_ASSISTANT_DESCRIPTION = "Inbox triage, reminders, planning your week."


@dataclass(frozen=True)
class FollowUp:
    """A question the conversation settles for an item."""

    text: str
    #: The key of :data:`SHARED_QUESTIONS` this question is an instance of,
    #: asked once for all of the work; ``None`` for the item's own.
    shared: str | None = None


@dataclass(frozen=True)
class WorkItem:
    """One kind of work in the catalog."""

    id: str
    group: str
    label: str
    #: One line: what the agent does.
    description: str
    #: A key of :data:`NEEDS`.
    needs: str
    follow_ups: tuple[FollowUp, ...]
    #: Shown with the description where a connection is easy to lack.
    note: str = ""

    @property
    def needs_text(self) -> str:
        """What the item needs, in one sentence."""
        return self.note or NEEDS[self.needs]

    def as_dict(self) -> dict:
        """The item on the wire (``detail.work_catalog``)."""
        return {
            "id": self.id,
            "group": self.group,
            "label": self.label,
            "description": self.description,
            "needs": self.needs,
            "follow_ups": [f.text for f in self.follow_ups],
            "note": self.note,
        }


def _ask(text: str, shared: str | None = None) -> FollowUp:
    return FollowUp(text, shared)


#: The catalog, in display order: grouped, the groups in :data:`GROUPS` order.
CATALOG: tuple[WorkItem, ...] = (
    WorkItem(
        id="review-prs", group="code", label="Review pull requests",
        description="Reads diffs, runs targeted checks, posts review comments.",
        needs="github",
        follow_ups=(
            _ask("which repositories"),
            _ask("which account it acts as", SHARED_ACCOUNT),
            _ask("what a review may do: comment only, request changes, or approve"),
            _ask("drafts the person approves, or comments posted directly", SHARED_APPROVAL),
            _ask("which pull requests: all, only on request, or by label, and whether to skip bots and drafts"),
            _ask("how often to look for new pull requests"),
        ),
    ),
    WorkItem(
        id="triage-issues", group="code", label="Triage issues",
        description="Labels, finds duplicates, asks reporters for missing info, routes, flags stale.",
        needs="github",
        follow_ups=(
            _ask("which repositories"),
            _ask("the label vocabulary"),
            _ask("whether it may label, comment or close"),
            _ask("how often to triage new issues"),
        ),
    ),
    WorkItem(
        id="fix-issues", group="code", label="Fix issues and open pull requests",
        description="Picks ready issues, implements, opens PRs, follows CI and review.",
        needs="github",
        follow_ups=(
            _ask("which repositories"),
            _ask("which issues: by label, milestone or assignee"),
            _ask("draft pull requests, or ready for review"),
            _ask("branch naming"),
            _ask("the git author of its commits", SHARED_ACCOUNT),
            _ask("what needs approval", SHARED_APPROVAL),
            _ask("a daily limit on new pull requests"),
        ),
    ),
    WorkItem(
        id="ci-health", group="code", label="Keep CI healthy",
        description="Failures on main, flaky tests, quarantines, perf regressions where benchmarks exist.",
        needs="github",
        follow_ups=(
            _ask("which repositories"),
            _ask("the CI system"),
            _ask("which branches"),
            _ask("report failures only, or also open fix pull requests"),
            _ask("where failure data lives"),
        ),
    ),
    WorkItem(
        id="dependency-updates", group="code", label="Dependency and security updates",
        description="Reviews bot PRs (dependabot/renovate), checks advisories, prepares upgrades.",
        needs="github",
        follow_ups=(
            _ask("which repositories"),
            _ask("which bots: dependabot, renovate"),
            _ask("the merge policy"),
            _ask("which advisory sources"),
        ),
    ),
    WorkItem(
        id="alert-response", group="operations", label="First response to alerts",
        description=(
            "Reads where alerts land, does the first investigation, posts findings, escalates by rule."
        ),
        needs="alerts",
        note=NEEDS["alerts"],
        follow_ups=(
            _ask("the alert source: a Slack channel, email, a webhook or a monitoring MCP server"),
            _ask("the runbooks"),
            _ask("read-only, or which remediations are allowed"),
            _ask("the escalation path"),
        ),
    ),
    WorkItem(
        id="channel-digests", group="communication", label="Channel digests and answers",
        description="Summarizes busy Slack/Telegram channels, answers recurring questions from docs.",
        needs="slack-or-telegram",
        follow_ups=(
            _ask("which channels"),
            _ask("how often to send a digest"),
            _ask("where to post", SHARED_DESTINATION),
            _ask("whether it may reply in threads"),
        ),
    ),
    WorkItem(
        id="research-reports", group="knowledge", label="Research and reports",
        description="Tracks topics, competitors, releases; periodic reports.",
        needs="web",
        follow_ups=(
            _ask("which topics"),
            _ask("which sources"),
            _ask("how often to report"),
            _ask("the format"),
            _ask("where reports go", SHARED_DESTINATION),
        ),
    ),
    WorkItem(
        id="docs-release-notes", group="knowledge", label="Docs and release notes",
        description="Keeps docs in step with merged changes, drafts changelogs.",
        needs="github",
        follow_ups=(
            _ask("which repositories and docs"),
            _ask("what triggers an update"),
            _ask("drafts, or direct changes", SHARED_APPROVAL),
        ),
    ),
)

ITEMS: dict[str, WorkItem] = {item.id: item for item in CATALOG}

#: The catalog items of the onboarding conversation's first form, in order.
FIRST_FORM_ITEMS = ("review-prs", "fix-issues", "ci-health", "research-reports")

#: The first form's options, in order, as ``(label, description)``: the
#: items of :data:`FIRST_FORM_ITEMS`, then :data:`PERSONAL_ASSISTANT`, which
#: a worker's form leaves out. The agent reads an answer given in Other…
#: against the whole catalog.
FIRST_FORM_OPTIONS: tuple[tuple[str, str], ...] = (
    *((ITEMS[item_id].label, ITEMS[item_id].description) for item_id in FIRST_FORM_ITEMS),
    (PERSONAL_ASSISTANT, PERSONAL_ASSISTANT_DESCRIPTION),
)


def work_catalog() -> list[dict]:
    """The catalog on the wire, in display order."""
    return [item.as_dict() for item in CATALOG]


def in_catalog_order(ids) -> tuple[str, ...]:
    """The known ids among ``ids``, once each, in display order."""
    wanted = set(ids)
    return tuple(item.id for item in CATALOG if item.id in wanted)


@dataclass(frozen=True)
class WorkSelection:
    """What the operator chose at setup: catalog items, custom work, rules.

    ``free_text`` marks a description that is not a selection: a note from an
    older client, ``nerve init --terminal`` or ``NERVE_TASK``, kept whole in
    ``custom``. Equality ignores it, so the same words compare equal in
    either form.
    """

    items: tuple[str, ...] = ()
    custom: str = ""
    rules: str = ""
    free_text: bool = field(default=False, compare=False)

    @property
    def empty(self) -> bool:
        return not (self.items or self.custom or self.rules)

    @property
    def chosen(self) -> tuple[WorkItem, ...]:
        """The chosen items, in display order (unknown ids left out)."""
        return tuple(ITEMS[i] for i in in_catalog_order(self.items))

    def as_dict(self) -> dict:
        """The selection on the wire (``detail.work``)."""
        return {"items": list(self.items), "custom": self.custom, "rules": self.rules}
