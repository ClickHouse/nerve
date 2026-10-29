"""The Linear source: the issue notifications of the user the API key belongs to.

A fake Linear GraphQL API (an :class:`httpx.MockTransport`) serves the
viewer and its notifications, filtered by type and ``createdAt`` and newest
first, as Linear does. Each test lists once for the baseline, adds
notifications, and lists again.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from nerve.config import LinearSyncConfig, NerveConfig
from nerve.sources import linear as module
from nerve.sources.linear import (
    LinearSource,
    linear_authorization,
    linear_inbox_filter,
    notification_record,
)

START = datetime(2026, 10, 27, 10, 0, tzinfo=UTC)
VIEWER = "user-agent"
KEY = "lin_api_secret"


def at(minutes: float) -> str:
    """A Linear timestamp *minutes* after the start of the test day."""
    return (START + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def person(name: str = "Alice", *, uid: str | None = None, app: bool = False) -> dict[str, Any]:
    return {
        "id": uid or f"user-{name.lower()}", "name": name, "displayName": name.lower(),
        "email": f"{name.lower()}@example.com", "app": app,
    }


def issue(identifier: str = "SCA-1", *, team: str = "SCA", title: str = "Fix the thing") -> dict[str, Any]:
    return {
        "id": f"issue-{identifier}", "identifier": identifier, "title": title,
        "url": f"https://linear.app/acme/issue/{identifier}",
        "description": "The thing is broken.", "priorityLabel": "High",
        "state": {"name": "Todo", "type": "unstarted"}, "team": {"key": team},
        "assignee": {"name": "Agent", "displayName": "agent"},
    }


def note(
    nid: str, kind: str, created: float, *, actor: dict[str, Any] | None = None,
    comment: str | None = None, comment_user: str | None = None, parent: str | None = None,
    the_issue: dict[str, Any] | None = None, bot: dict[str, Any] | None = None,
    external: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A notification as the query returns it."""
    the_issue = the_issue or issue()
    node: dict[str, Any] = {
        "id": nid, "type": kind, "createdAt": at(created), "isLinearActor": False,
        "actor": actor if actor is not None or bot or external else person(),
        "botActor": bot, "externalUserActor": external,
        "team": {"key": the_issue["team"]["key"]}, "issue": the_issue,
        "comment": None, "parentComment": None,
    }
    if comment:
        node["comment"] = {
            "id": comment, "body": f"Body of {comment}",
            "url": f"{the_issue['url']}#comment-{comment}",
            "user": {"id": comment_user or (node["actor"] or {}).get("id")},
        }
    if parent:
        node["parentComment"] = {"body": "The question", "user": {"name": "Bob", "displayName": "bob"}}
    return node


class FakeLinear:
    """Linear's GraphQL API, as far as the source uses it."""

    def __init__(self) -> None:
        self.viewer = VIEWER
        self.notifications: list[dict[str, Any]] = []
        self.requests: list[httpx.Request] = []
        self.bodies: list[dict[str, Any]] = []
        self.page_size: int | None = None
        self.answer: httpx.Response | None = None

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        body = json.loads(request.content)
        self.bodies.append(body)
        if self.answer is not None:
            return self.answer
        variables = body["variables"]
        wanted = set(variables["filter"]["type"]["in"])
        since = datetime.fromisoformat(variables["filter"]["createdAt"]["gte"])
        matching = sorted(
            (n for n in self.notifications
             if n["type"] in wanted and datetime.fromisoformat(n["createdAt"]) >= since),
            key=lambda n: n["createdAt"], reverse=True,
        )
        size = self.page_size or variables["first"]
        start = int(variables["after"] or 0)
        page = matching[start:start + size]
        more = start + size < len(matching)
        data: dict[str, Any] = {"notifications": {
            "nodes": page,
            "pageInfo": {"hasNextPage": more, "endCursor": str(start + size) if more else None},
        }}
        if variables["withViewer"]:
            data["viewer"] = {"id": self.viewer}
        return httpx.Response(200, json={"data": data})


class Clock:
    def __init__(self, minutes: float = 0) -> None:
        self.now = START + timedelta(minutes=minutes)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, minutes: float) -> None:
        self.now += timedelta(minutes=minutes)


def make(fake: FakeLinear, clock: Clock, **config: Any) -> LinearSource:
    return LinearSource(
        {"api_key": KEY, **config}, clock=clock, transport=fake.transport(),
    )


async def baseline(source: LinearSource) -> str:
    result = await source.fetch(None)
    assert result.records == []
    return result.next_cursor


# ---------------------------------------------------------------------- #
#  Cursor                                                                 #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_baseline_reports_nothing_and_a_later_run_only_what_is_newer():
    fake, clock = FakeLinear(), Clock(30)
    fake.notifications = [note("n1", "issueAssignedToYou", 10)]
    source = make(fake, clock)

    cursor = await baseline(source)
    assert json.loads(cursor) == {
        "v": 1, "since": at(10), "at": ["n1"], "listed_at": "2026-10-27T10:30:00.000Z",
        "viewer": VIEWER,
    }

    fake.notifications.append(note("n2", "issueAssignedToYou", 35, the_issue=issue("SCA-2")))
    clock.advance(10)
    result = await source.fetch(cursor)

    assert [r.id for r in result.records] == ["SCA-2:notification:n2"]
    assert json.loads(result.next_cursor)["since"] == at(35)
    assert (await source.fetch(result.next_cursor)).records == []


@pytest.mark.asyncio
async def test_a_baseline_without_notifications_starts_an_hour_back():
    """So a notification created after the baseline is reported, whatever the
    difference between this clock and Linear's."""
    fake, clock = FakeLinear(), Clock(120)
    source = make(fake, clock)

    cursor = await baseline(source)
    assert json.loads(cursor)["since"] == at(60)
    assert fake.bodies[0]["variables"]["filter"]["createdAt"] == {"gte": at(60)}

    fake.notifications.append(note("n1", "issueMention", 119.5))
    assert [r.id for r in (await source.fetch(cursor)).records] == ["SCA-1:notification:n1"]


@pytest.mark.asyncio
async def test_a_notification_in_the_cursor_s_millisecond_is_new_unless_it_was_listed():
    fake, clock = FakeLinear(), Clock(30)
    fake.notifications = [note("n1", "issueAssignedToYou", 10)]
    source = make(fake, clock)
    cursor = await baseline(source)

    fake.notifications.append(note("n0", "issueAssignedToYou", 10, the_issue=issue("SCA-2")))
    result = await source.fetch(cursor)

    assert [r.id for r in result.records] == ["SCA-2:notification:n0"]
    assert json.loads(result.next_cursor)["at"] == ["n0", "n1"]
    assert (await source.fetch(result.next_cursor)).records == []


@pytest.mark.asyncio
async def test_every_page_is_read_and_the_viewer_asked_on_the_first_only():
    fake, clock = FakeLinear(), Clock(0)
    fake.page_size = 2
    source = make(fake, clock)
    cursor = await baseline(source)
    fake.notifications = [
        note(f"n{i}", "issueAssignedToYou", 1 + i, the_issue=issue(f"SCA-{i}")) for i in range(5)
    ]
    fake.bodies.clear()

    result = await source.fetch(cursor)

    assert [r.id for r in result.records] == [f"SCA-{i}:notification:n{i}" for i in range(5)]
    assert [b["variables"]["withViewer"] for b in fake.bodies] == [True, False, False]
    assert [b["variables"]["after"] for b in fake.bodies] == [None, "2", "4"]
    assert all(b["variables"]["first"] == module.PAGE_SIZE for b in fake.bodies)
    assert json.loads(result.next_cursor)["since"] == at(5)


@pytest.mark.asyncio
async def test_reading_stops_at_the_page_cap_with_a_warning(monkeypatch, caplog):
    monkeypatch.setattr(module, "MAX_PAGES", 2)
    fake, clock = FakeLinear(), Clock(0)
    fake.page_size = 1
    source = make(fake, clock)
    cursor = await baseline(source)
    fake.notifications = [
        note(f"n{i}", "issueAssignedToYou", 1 + i, the_issue=issue(f"SCA-{i}")) for i in range(4)
    ]
    fake.bodies.clear()

    with caplog.at_level(logging.WARNING, logger="nerve.sources.linear"):
        result = await source.fetch(cursor)

    assert len(fake.bodies) == 2
    # Newest first: the two newest are read.
    assert [r.id for r in result.records] == ["SCA-2:notification:n2", "SCA-3:notification:n3"]
    assert "the rest are not reported" in caplog.text


@pytest.mark.asyncio
async def test_after_a_gap_only_the_last_day_is_reported():
    fake, clock = FakeLinear(), Clock(0)
    source = make(fake, clock)
    cursor = await baseline(source)
    day = 24 * 60
    fake.notifications = [
        note("old", "issueAssignedToYou", 2 * 60, the_issue=issue("SCA-1")),
        note("recent", "issueAssignedToYou", 3 * day - 30, the_issue=issue("SCA-2")),
    ]
    clock.advance(3 * day)

    result = await source.fetch(cursor)

    assert [r.id for r in result.records] == ["SCA-2:notification:recent"]
    assert fake.bodies[-1]["variables"]["filter"]["createdAt"] == {"gte": at(2 * day - 60)}
    # The skipped notifications stay skipped on the next run.
    clock.advance(5)
    assert (await source.fetch(result.next_cursor)).records == []


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor", [
    "not json", '{"v": 2, "since": "x"}', '{"v": 1, "since": "2026-10-27T10:00:00Z"}',
])
async def test_an_unreadable_cursor_takes_a_baseline(cursor):
    fake, clock = FakeLinear(), Clock(30)
    fake.notifications = [note("n1", "issueAssignedToYou", 10)]
    result = await make(fake, clock).fetch(cursor)
    assert result.records == []
    assert json.loads(result.next_cursor)["since"] == at(10)


@pytest.mark.asyncio
async def test_a_key_of_another_user_takes_a_baseline():
    fake, clock = FakeLinear(), Clock(0)
    source = make(fake, clock)
    cursor = await baseline(source)
    fake.notifications = [note("n1", "issueAssignedToYou", 5)]
    fake.viewer = "someone-else"

    result = await source.fetch(cursor)

    assert result.records == []
    assert json.loads(result.next_cursor)["viewer"] == "someone-else"
    assert (await source.fetch(result.next_cursor)).records == []


@pytest.mark.asyncio
@pytest.mark.parametrize(("response", "message"), [
    (httpx.Response(401, json={"errors": [{"message": "no"}]}), "authentication failed"),
    (httpx.Response(200, json={"errors": [{"message": "x", "extensions": {"code": "AUTHENTICATION_ERROR"}}]}),
     "authentication failed"),
    (httpx.Response(429, text="slow down"), "rate limit"),
    (httpx.Response(400, json={"errors": [{"message": "limited", "extensions": {"code": "RATELIMITED"}}]}),
     "rate limit"),
    (httpx.Response(200, json={"errors": [{"message": "Field 'x' unknown"}], "data": None}),
     "Field 'x' unknown"),
    (httpx.Response(502, text="bad gateway"), "HTTP 502"),
    (httpx.Response(200, json={"data": {"notifications": {}}}), "no viewer"),
])
async def test_a_failure_raises_and_the_runner_keeps_the_cursor(db, response, message):
    from nerve.sources.runner import SourceRunner

    fake, clock = FakeLinear(), Clock(0)
    source = make(fake, clock)
    cursor = await baseline(source)
    fake.answer = response

    with pytest.raises(RuntimeError, match=message):
        await source.fetch(cursor)

    await db.set_sync_cursor("linear", cursor)
    result = await SourceRunner(source, db).run()
    assert result.error and message.lower() in result.error.lower()
    assert await db.get_sync_cursor("linear") == cursor


@pytest.mark.asyncio
async def test_a_timeout_raises():
    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    source = LinearSource({"api_key": KEY}, clock=Clock(), transport=httpx.MockTransport(timeout))
    with pytest.raises(RuntimeError, match="did not answer"):
        await source.fetch(None)


# ---------------------------------------------------------------------- #
#  What is asked for, and what is reported                                #
# ---------------------------------------------------------------------- #


@pytest.mark.asyncio
@pytest.mark.parametrize(("types", "asked"), [
    (None, ["issueAssignedToYou", "issueMention", "issueCommentMention", "issueNewComment"]),
    (["assigned"], ["issueAssignedToYou"]),
    (["mentioned"], ["issueMention", "issueCommentMention"]),
    (["comments", "assigned"], ["issueAssignedToYou", "issueNewComment"]),
])
async def test_the_types_ask_for_their_notification_types(types, asked):
    fake = FakeLinear()
    config = {} if types is None else {"types": types}
    await make(fake, Clock(), **config).fetch(None)
    assert fake.bodies[0]["variables"]["filter"]["type"] == {"in": asked}


@pytest.mark.parametrize(("key", "header"), [
    ("lin_api_abc", "lin_api_abc"),
    ("lin_oauth_abc", "Bearer lin_oauth_abc"),
    ("  token  ", "Bearer token"),
    ("Bearer token", "Bearer token"),
])
def test_the_authorization_header_for_each_kind_of_key(key, header):
    assert linear_authorization(key) == header


@pytest.mark.asyncio
async def test_the_request_carries_the_key():
    fake = FakeLinear()
    await make(fake, Clock()).fetch(None)
    (request,) = fake.requests
    assert request.headers["authorization"] == KEY
    assert str(request.url) == module.API_URL


async def _reported(nodes: list[dict[str, Any]], **config: Any) -> list[str]:
    fake, clock = FakeLinear(), Clock(0)
    source = make(fake, clock, **config)
    cursor = await baseline(source)
    fake.notifications = nodes
    return [r.id for r in (await source.fetch(cursor)).records]


@pytest.mark.asyncio
async def test_the_agent_s_own_actions_are_dropped():
    me = person("Agent", uid=VIEWER)
    assert await _reported([
        note("n1", "issueAssignedToYou", 1, actor=me),
        note("n2", "issueNewComment", 2, actor=me, comment="c1"),
        note("n3", "issueNewComment", 3, actor=person(), comment="c2", comment_user=VIEWER),
        note("n4", "issueNewComment", 4, comment="c3"),
    ]) == ["SCA-1:comment:c3"]


@pytest.mark.asyncio
async def test_apps_bots_and_external_users_are_dropped_unless_asked_for_but_assignments_pass():
    nodes = [
        note("n1", "issueNewComment", 1, actor=person("Zapier", app=True), comment="c1"),
        note("n2", "issueCommentMention", 2, actor=None, bot={"id": "b", "name": "GitHub"}, comment="c2"),
        note("n3", "issueNewComment", 3, actor=None, external={"id": "e", "name": "Cust", "displayName": "c", "email": "c@x"}, comment="c3"),
        note("n4", "issueAssignedToYou", 4, actor=None, bot={"id": "b", "name": "Triage"}),
        note("n5", "issueNewComment", 5, comment="c5"),
    ]
    assert await _reported(nodes) == ["SCA-1:notification:n4", "SCA-1:comment:c5"]
    assert len(await _reported(nodes, include_bots=True)) == 5


@pytest.mark.asyncio
async def test_deny_actors_matches_the_name_the_display_name_and_the_e_mail():
    nodes = [
        note("n1", "issueNewComment", 1, actor=person("Alice"), comment="c1"),
        note("n2", "issueNewComment", 2, actor=person("Bob"), comment="c2"),
        note("n3", "issueNewComment", 3, actor=person("Carol"), comment="c3"),
        note("n4", "issueNewComment", 4, actor=person("Dave"), comment="c4"),
    ]
    assert await _reported(
        nodes, deny_actors=["ALICE", "bob", "carol@*"],
    ) == ["SCA-1:comment:c4"]


@pytest.mark.asyncio
async def test_the_team_lists_filter_by_team_key():
    nodes = [
        note("n1", "issueAssignedToYou", 1, the_issue=issue("ENG-1", team="ENG")),
        note("n2", "issueAssignedToYou", 2, the_issue=issue("OPS-1", team="OPS")),
        note("n3", "issueAssignedToYou", 3, the_issue=issue("ENGX-1", team="ENGX")),
    ]
    assert await _reported(nodes, allow_teams=["eng*"], deny_teams=["ENGX"]) == [
        "ENG-1:notification:n1",
    ]


@pytest.mark.asyncio
async def test_a_notification_without_an_issue_is_dropped():
    node = note("n1", "issueAssignedToYou", 1)
    node["issue"] = None
    assert await _reported([node]) == []


@pytest.mark.asyncio
async def test_a_mention_in_a_comment_and_the_new_comment_are_one_record_the_mention():
    fake, clock = FakeLinear(), Clock(0)
    source = make(fake, clock)
    cursor = await baseline(source)
    fake.notifications = [
        note("n1", "issueNewComment", 1, comment="c1"),
        note("n2", "issueCommentMention", 1, comment="c1"),
        note("n3", "issueNewComment", 2, comment="c2"),
    ]

    records = (await source.fetch(cursor)).records

    assert [(r.id, r.metadata["event"], r.metadata["notification_id"]) for r in records] == [
        ("SCA-1:comment:c1", "mentioned", "n2"), ("SCA-1:comment:c2", "commented", "n3"),
    ]


# ---------------------------------------------------------------------- #
#  Records                                                                #
# ---------------------------------------------------------------------- #


def test_the_record_of_a_reply_is_pinned():
    node = note("n9", "issueNewComment", 5, comment="c9", parent="p1")

    record = notification_record(node)

    assert record.id == "SCA-1:comment:c9"
    assert record.source == "linear" and record.record_type == "linear_notification"
    assert record.summary == "[SCA-1] Alice replied: Fix the thing"
    assert record.timestamp == at(5)
    assert record.content == "\n".join([
        "Issue: SCA-1 Fix the thing",
        "Event: replied",
        "Team: SCA",
        "State: Todo (unstarted)",
        "Priority: High",
        "Assignee: Agent",
        "Actor: Alice <alice@example.com>",
        "Issue URL: https://linear.app/acme/issue/SCA-1",
        "Comment URL: https://linear.app/acme/issue/SCA-1#comment-c9",
        "",
        "--- In reply to Bob ---",
        "The question",
        "",
        "--- Comment ---",
        "Body of c9",
    ])
    assert record.metadata == {
        "identifier": "SCA-1",
        "issue_id": "issue-SCA-1",
        "team_key": "SCA",
        "event": "replied",
        "notification_type": "issueNewComment",
        "notification_id": "n9",
        "actor": "Alice",
        "actor_email": "alice@example.com",
        "actor_id": "user-alice",
        "actor_kind": "user",
        "state": "Todo",
        "state_type": "unstarted",
        "assignee": "Agent",
        "comment_id": "c9",
        "issue_url": "https://linear.app/acme/issue/SCA-1",
        "comment_url": "https://linear.app/acme/issue/SCA-1#comment-c9",
        "subject_url": "https://linear.app/acme/issue/SCA-1#comment-c9",
    }
    assert all(isinstance(value, str) for value in record.metadata.values())


@pytest.mark.parametrize(("kind", "comment", "summary", "section"), [
    ("issueAssignedToYou", None, "[SCA-1] Assigned to you by Alice: Fix the thing", "Description"),
    ("issueMention", None, "[SCA-1] Alice mentioned you: Fix the thing", "Description"),
    ("issueCommentMention", "c1", "[SCA-1] Alice mentioned you: Fix the thing", "Comment"),
    ("issueNewComment", "c1", "[SCA-1] Alice commented: Fix the thing", "Comment"),
])
def test_each_event_has_its_summary(kind, comment, summary, section):
    record = notification_record(note("n1", kind, 0, comment=comment))
    assert record.summary == summary
    assert f"--- {section} ---" in record.content
    assert list(record.metadata) == [
        "identifier", "issue_id", "team_key", "event", "notification_type", "notification_id",
        "actor", "actor_email", "actor_id", "actor_kind", "state", "state_type", "assignee",
        "comment_id", "issue_url", "comment_url", "subject_url",
    ]
    assert all(isinstance(value, str) for value in record.metadata.values())


def test_a_long_body_is_capped():
    node = note("n1", "issueNewComment", 0, comment="c1")
    node["comment"]["body"] = "x" * 10_000
    content = notification_record(node).content
    assert content.endswith("x" * 10 + "\n[... truncated]")
    assert len(content) < 5_000


def test_an_actor_of_another_kind_is_named_and_marked():
    record = notification_record(note("n1", "issueAssignedToYou", 0, actor=None, bot={"id": "b", "name": "Triage"}))
    assert record.summary == "[SCA-1] Assigned to you by Triage: Fix the thing"
    assert "Actor: Triage (bot)" in record.content
    assert record.metadata["actor_kind"] == "bot"


# ---------------------------------------------------------------------- #
#  Guardrail and wiring                                                   #
# ---------------------------------------------------------------------- #


def test_the_inbox_filter_repeats_the_settings():
    settings = LinearSyncConfig.from_dict({
        "types": ["assigned", "comments"], "deny_actors": ["mallory*"],
        "allow_teams": ["ENG"], "deny_teams": ["SEC"],
    })
    guard = linear_inbox_filter(settings)

    def record(**changes: Any):
        node = note("n1", changes.pop("kind", "issueNewComment"), 0, comment="c1", **changes)
        return notification_record(node)

    eng = issue("ENG-1", team="ENG")
    assert guard.passes(record(the_issue=eng))
    assert not guard.passes(record(the_issue=issue("OPS-1", team="OPS")))
    assert not guard.passes(record(the_issue=issue("SEC-1", team="SEC")))
    assert not guard.passes(record(the_issue=eng, actor=person("Mallory")))
    assert not guard.passes(record(the_issue=eng, kind="issueCommentMention"))
    bot = {"id": "b", "name": "GitHub"}
    assert not guard.passes(record(the_issue=eng, actor=None, bot=bot))
    assigned = notification_record(note("n2", "issueAssignedToYou", 0, actor=None, bot=bot, the_issue=eng))
    assert guard.passes(assigned)

    open_guard = linear_inbox_filter(LinearSyncConfig.from_dict({"include_bots": True}))
    assert open_guard.passes(record(actor=None, bot=bot))


def test_the_registry_hands_the_settings_to_the_source():
    from nerve.sources.registry import build_source_runners

    config = NerveConfig.from_dict({"sync": {"linear": {
        "enabled": True, "api_key": KEY, "types": ["mentioned"], "include_bots": True,
        "deny_actors": ["noisy-*"], "allow_teams": ["ENG"], "deny_teams": ["SEC"],
        "batch_size": 7,
    }}})

    (runner,) = [r for r in build_source_runners(config, db=None) if r.job_id == "source:linear"]

    src = runner.source
    assert isinstance(src, LinearSource) and src.push_fed is False
    assert (src._api_key, src._notification_types, src._include_bots, src._deny_actors,
            src._allow_teams, src._deny_teams) == (
        KEY, ["issueMention", "issueCommentMention"], True, ["noisy-*"], ["ENG"], ["SEC"],
    )
    assert runner.batch_size == 7
    assert [rule.field for rule in runner.inbox_filter.rules if rule.active] == [
        "team_key", "actor", "actor_email", "notification_type",
    ]


def test_the_registry_skips_the_source_without_a_key(caplog):
    from nerve.sources.registry import build_source_runners

    config = NerveConfig.from_dict({"sync": {"linear": {"enabled": True}}})
    with caplog.at_level(logging.WARNING, logger="nerve.sources.registry"):
        runners = build_source_runners(config, db=None)
    assert not [r for r in runners if r.job_id == "source:linear"]
    assert "sync.linear.api_key is empty" in caplog.text

    off = NerveConfig.from_dict({"sync": {"linear": {"api_key": KEY}}})
    assert not [r for r in build_source_runners(off, db=None) if r.job_id == "source:linear"]
