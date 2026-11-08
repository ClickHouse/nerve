"""The origin of a session: its trigger and the origin details.

Each creating code path stores a trigger from the closed vocabulary and the
details for display. A row from before the trigger column gets a derived
origin when it is read. The tests use the real routes, engine and database
of ``test_cause_attribution``; no model runs.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from nerve.agent.backends import TurnCompleted
from nerve.db.migrations.v065_session_origin import up as v065_up
from nerve.identity import Actor
from nerve.mcp_server.http import build_ctx_resolver
from nerve.mcp_server.session import SatelliteSessionResolver
from nerve.session_origin import (
    TRIGGERS,
    Origin,
    channel_origin,
    cron_origin,
    derive_origin,
    origin_fields,
    origin_label,
    session_origin,
)
from tests import test_cause_attribution
from tests.test_cause_attribution import _client, _pending_plan, _record_runs

# The install of test_cause_attribution: real routes, engine and database,
# with the people alice and bob.
install = test_cause_attribution.install

# --------------------------------------------------------------------------- #
#  The vocabulary, the derivation and the labels                              #
# --------------------------------------------------------------------------- #


def test_the_vocabulary_is_closed():
    assert TRIGGERS == {
        "chat", "api", "slack", "telegram", "email", "cron", "plan",
        "workflow", "external", "system", "unknown",
    }
    with pytest.raises(ValueError):
        Origin("web")


def test_empty_details_are_left_out():
    origin = Origin("slack", {"channel": "C1", "thread": "", "channel_name": None})
    assert origin.detail == {"channel": "C1"}
    assert origin.detail_json() == '{"channel": "C1"}'
    assert Origin("chat").detail_json() is None


@pytest.mark.parametrize("name, key, title, expected", [
    ("slack", "slack:C0123:1700000000.000100", "general",
     Origin("slack", {"channel": "C0123", "thread": "1700000000.000100",
                      "channel_name": "general"})),
    ("slack", "slack:D0456", "", Origin("slack", {"channel": "D0456"})),
    ("telegram", "telegram:12345", "", Origin("telegram", {"channel": "12345"})),
    ("web", "web:default", "", Origin("chat")),
    ("api", "api:default", "", Origin("api")),
    ("discord", "discord:1", "", Origin("unknown", {"channel": "1"})),
])
def test_channel_origin(name, key, title, expected):
    assert channel_origin(name, key, title) == expected


@pytest.mark.parametrize("row, channel_key, expected", [
    ({"id": "cron:daily:20261106-080000-1a2b3c4d", "source": "cron"}, None,
     Origin("cron", {"cron_job": "daily"})),
    ({"id": "cron:daily", "source": "cron"}, None, Origin("cron", {"cron_job": "daily"})),
    ({"id": "hook:deploy:9f3c", "source": "hook"}, None, Origin("api", {"hook": "deploy"})),
    ({"id": "workflow:wfr-1", "source": "workflow"}, None,
     Origin("workflow", {"workflow_run": "wfr-1"})),
    ({"id": "impl-1a2b3c4d", "source": "web"}, None, Origin("plan")),
    # The onboarding conversation, and the autonomous onboarding.
    ({"id": "onboarding-1a2b", "source": "web"}, None, Origin("chat")),
    ({"id": "onboarding-1a2b", "source": "cron"}, None, Origin("system")),
    # A fork copies the source of its parent.
    ({"id": "fork-1a2b", "source": "telegram", "parent_session_id": "p1"}, None,
     Origin("chat", {"fork_of": "p1"})),
    ({"id": "a1", "source": "slack"}, "slack:C1:1.1",
     Origin("slack", {"channel": "C1", "thread": "1.1"})),
    ({"id": "a1", "source": "slack"}, None, Origin("slack")),
    ({"id": "a1", "source": "telegram"}, "telegram:7", Origin("telegram", {"channel": "7"})),
    ({"id": "a1", "source": "web"}, "web:default", Origin("chat")),
    ({"id": "a1", "source": "api"}, None, Origin("api")),
    ({"id": "external:codex:x", "source": "external",
      "metadata": '{"client_name": "codex"}'}, None, Origin("external", {"client": "codex"})),
    ({"id": "e1", "source": "external", "metadata": {"client_name": "codex"}}, None,
     Origin("external", {"client": "codex"})),
    ({"id": "e1", "source": "external", "metadata": "not json"}, None, Origin("external")),
    ({"id": "a1", "source": "something-else"}, None, Origin("unknown")),
])
def test_derive_origin(row, channel_key, expected):
    assert derive_origin(row, channel_key) == expected


@pytest.mark.parametrize("origin, label", [
    (Origin("slack", {"channel": "C1", "channel_name": "general"}), "Started from Slack #general"),
    (Origin("slack", {"channel": "D1"}), "Started from a Slack direct message"),
    (Origin("slack", {"channel": "C1"}), "Started from Slack"),
    (Origin("telegram"), "Started from Telegram"),
    (Origin("email"), "Started from email"),
    (Origin("api"), "Started through the API"),
    (Origin("api", {"hook": "deploy"}), "Started by webhook deploy"),
    (Origin("cron", {"cron_job": "daily"}), "Started by cron job daily"),
    (Origin("cron", {"run_later": "1h"}), "Scheduled with Run later"),
    (Origin("cron"), "Started by a scheduled run"),
    (Origin("plan", {"plan_id": "plan-1"}), "Started by a plan"),
    (Origin("workflow"), "Started by a workflow run"),
    (Origin("external", {"client": "codex"}), "Started in codex"),
    (Origin("external"), "Started in an external client"),
    (Origin("chat", {"fork_of": "p1"}), "Forked from another chat"),
    (Origin("chat"), "Started in the web chat"),
    (Origin("system"), "Started by Nerve"),
    (Origin("unknown"), "Started from an unknown origin"),
])
def test_origin_label(origin, label):
    assert origin_label(origin) == label


def test_a_stored_trigger_wins_over_the_derivation():
    row = {"id": "cron:daily:1", "source": "cron", "trigger": "plan",
           "origin_detail": '{"plan_id": "plan-1"}'}
    assert session_origin(row) == Origin("plan", {"plan_id": "plan-1"})
    assert origin_fields(row) == {
        "trigger": "plan", "origin_label": "Started by a plan",
        "origin_detail": {"plan_id": "plan-1"},
    }


@pytest.mark.parametrize("stored", [None, "", "web", 3])
def test_a_missing_or_foreign_trigger_is_derived(stored):
    row = {"id": "cron:daily:1", "source": "cron", "trigger": stored}
    assert session_origin(row) == cron_origin("daily")


@pytest.mark.parametrize("detail", ["not json", "[1, 2]", None])
def test_unreadable_details_are_empty(detail):
    assert session_origin({"trigger": "slack", "origin_detail": detail}) == Origin("slack")


# --------------------------------------------------------------------------- #
#  The store                                                                   #
# --------------------------------------------------------------------------- #


async def _origin_of(db, session_id: str) -> tuple:
    row = await db.get_session(session_id)
    return row["trigger"], json.loads(row["origin_detail"] or "{}")


@pytest.mark.asyncio
async def test_v065_adds_the_columns_once(install):
    db = install.db
    await v065_up(db.db)
    async with db.db.execute("PRAGMA table_info(sessions)") as cursor:
        columns = [row[1] for row in await cursor.fetchall()]
    assert columns.count("trigger") == 1
    assert columns.count("origin_detail") == 1


@pytest.mark.asyncio
async def test_create_session_stores_the_given_origin_only(install):
    db = install.db
    given = await db.create_session(
        "given", source="cron", actor=None, origin=Origin("plan", {"plan_id": "p1"}),
    )
    unstated = await db.create_session("cron:nightly:1", source="cron", actor=None)

    assert await _origin_of(db, "given") == ("plan", {"plan_id": "p1"})
    assert (given["trigger"], given["origin_detail"]) == ("plan", '{"plan_id": "p1"}')
    # No origin: NULL columns, and a reader derives the origin.
    assert await _origin_of(db, "cron:nightly:1") == (None, {})
    assert session_origin(unstated) == cron_origin("nightly")


@pytest.mark.asyncio
async def test_a_second_create_keeps_the_first_origin(install):
    db = install.db
    await db.create_session("same", actor=None, origin=Origin("api"))
    await db.create_session("same", actor=None, origin=Origin("cron"))
    assert await _origin_of(db, "same") == ("api", {})


@pytest.mark.asyncio
async def test_set_session_origin_changes_only_a_session_without_messages(install):
    db = install.db
    await db.create_session("empty", actor=None, origin=Origin("chat"))
    await db.create_session("used", actor=None, origin=Origin("chat"))
    await db.add_message("used", "user", "hello", actor=None)

    assert await db.set_session_origin("empty", Origin("cron", {"run_later": "1h"}))
    assert not await db.set_session_origin("used", Origin("cron", {"run_later": "1h"}))

    assert await _origin_of(db, "empty") == ("cron", {"run_later": "1h"})
    assert await _origin_of(db, "used") == ("chat", {})


@pytest.mark.asyncio
async def test_channel_keys_for_sessions(install):
    db = install.db
    for session_id in ("s1", "s2", "s3"):
        await db.create_session(session_id, actor=None)
    await db.set_channel_session("slack:C1:1.1", "s1")
    await db.set_channel_session("telegram:7", "s2")
    assert await db.channel_keys_for_sessions(["s1", "s2", "s3"]) == {
        "s1": "slack:C1:1.1", "s2": "telegram:7",
    }
    assert await db.channel_keys_for_sessions([]) == {}


# --------------------------------------------------------------------------- #
#  The creating code paths                                                     #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_web_and_rest_sessions(install):
    headers = install.headers("alice")
    async with _client(install.app()) as client:
        web = (await client.post("/api/sessions", headers=headers, json={})).json()
        rest = (await client.post(
            "/api/sessions", headers=headers, json={"source": "nightly-script"},
        )).json()
        await client.post("/api/chat", headers=headers, json={"message": "hi"})
        # A new session id from the caller.
        await client.post(
            "/api/chat", headers=headers, json={"session_id": "rest-new", "message": "hi"},
        )
        await install.db.update_session_fields(web["id"], {"sdk_session_id": "native-1"})
        fork = (await client.post(
            "/api/sessions/fork", headers=headers, json={"source_session_id": web["id"]},
        )).json()
        listed = {
            s["id"]: s for s in
            (await client.get("/api/sessions", headers=headers)).json()["sessions"]
        }
        single = (await client.get(f"/api/sessions/{fork['id']}", headers=headers)).json()

    api_session = await install.engine.sessions.get_last_session("api:default")
    assert await _origin_of(install.db, web["id"]) == ("chat", {})
    assert await _origin_of(install.db, rest["id"]) == ("api", {})
    assert await _origin_of(install.db, api_session) == ("api", {})
    assert await _origin_of(install.db, "rest-new") == ("api", {})
    assert await _origin_of(install.db, fork["id"]) == ("chat", {"fork_of": web["id"]})
    # Every session payload has the three origin fields.
    for payload in (web, rest, fork, single, listed[web["id"]], listed[api_session]):
        assert set(payload) >= {"trigger", "origin_label", "origin_detail"}
    assert (web["trigger"], web["origin_label"], web["origin_detail"]) == (
        "chat", "Started in the web chat", {},
    )
    assert (fork["origin_label"], fork["origin_detail"]) == (
        "Forked from another chat", {"fork_of": web["id"]},
    )
    assert single["origin_detail"] == {"fork_of": web["id"]}
    assert listed[api_session]["origin_label"] == "Started through the API"


@pytest.mark.asyncio
async def test_run_later_gives_a_new_session_the_cron_trigger(install):
    headers = install.headers("alice")
    async with _client(install.app()) as client:
        fresh = (await client.post("/api/sessions", headers=headers, json={})).json()["id"]
        used = (await client.post("/api/sessions", headers=headers, json={})).json()["id"]
        await install.db.add_message(used, "user", "earlier", actor=None)
        for session_id in (fresh, used):
            response = await client.post(
                "/api/sessions/run-later", headers=headers,
                json={"session_id": session_id, "message": "later", "delay": "1h"},
            )
            assert response.status_code == 200
        listed = {
            s["id"]: s for s in
            (await client.get("/api/sessions", headers=headers)).json()["sessions"]
        }

    assert await _origin_of(install.db, fresh) == ("cron", {"run_later": "1h"})
    assert await _origin_of(install.db, used) == ("chat", {})
    assert listed[fresh]["origin_label"] == "Scheduled with Run later"


@pytest.mark.asyncio
async def test_run_later_without_a_delay_gives_the_web_chat_trigger(install):
    """The option "Just accept it" schedules nothing: the person runs the turn
    from the chat, so the session gets the trigger of a web chat."""
    headers = install.headers("alice")
    async with _client(install.app()) as client:
        session_id = (await client.post("/api/sessions", headers=headers, json={})).json()["id"]
        response = await client.post(
            "/api/sessions/run-later", headers=headers,
            json={"session_id": session_id, "message": "later", "delay": "none"},
        )
        assert response.status_code == 200
        single = (await client.get(f"/api/sessions/{session_id}", headers=headers)).json()

    assert await _origin_of(install.db, session_id) == ("chat", {})
    assert (single["trigger"], single["origin_label"]) == ("chat", "Started in the web chat")


@pytest.mark.asyncio
async def test_an_old_row_gets_a_derived_origin_with_its_channel(install):
    """A row from before the column has a NULL trigger and no details."""
    db = install.db
    await db.create_session("old-slack", source="slack", actor=None)
    await db.create_session("old-chat", source="web", actor=None)
    await db._write(
        "UPDATE sessions SET trigger = NULL, origin_detail = NULL"
        " WHERE id IN ('old-slack', 'old-chat')",
    )
    await db.set_channel_session("slack:C0OLD:1.1", "old-slack")

    async with _client(install.app()) as client:
        listed = {
            s["id"]: s for s in (await client.get(
                "/api/sessions", headers=install.headers("alice"),
            )).json()["sessions"]
        }

    assert (listed["old-slack"]["trigger"], listed["old-slack"]["origin_detail"]) == (
        "slack", {"channel": "C0OLD", "thread": "1.1"},
    )
    assert listed["old-chat"]["trigger"] == "chat"


class _QuietRuntime:
    """A model runtime client whose turns end at once, with no tool call."""

    native_session_id = None

    def try_receive_idle_events(self):
        return None

    async def start_turn(self, turn_input) -> None:
        return None

    async def disconnect(self) -> None:
        return None

    async def receive_turn(self):
        yield TurnCompleted()


@pytest.mark.asyncio
async def test_cron_and_hook_sessions(install, monkeypatch):
    async def _client_for(session_id, source, model, **kwargs):
        return _QuietRuntime()

    monkeypatch.setattr(install.engine, "_get_or_create_client", _client_for)
    await install.engine.run_cron(job_id="nightly", prompt="tidy", run_id="r1")
    await install.engine.run_persistent_cron(
        job_id="digest", prompt="sum up", session_id="cron:digest:g1",
    )
    hook = await install.engine.sessions.create_hook_session(
        "deploy", "h1", actor=install.db.system_actor,
    )

    assert await _origin_of(install.db, "cron:nightly:r1") == ("cron", {"cron_job": "nightly"})
    assert await _origin_of(install.db, "cron:digest:g1") == ("cron", {"cron_job": "digest"})
    assert await _origin_of(install.db, hook["id"]) == ("api", {"hook": "deploy"})


@pytest.mark.asyncio
async def test_channel_sessions(install):
    sessions = install.engine.sessions
    slack = await sessions.get_active_session(
        "slack:C0123:1.1", source="slack", actor=None, conversation_title="general",
    )
    telegram = await sessions.get_active_session("telegram:42", source="telegram", actor=None)
    web = await sessions.get_active_session(
        "web:default", source="web", actor=Actor(install.actor_id("alice"), "human"),
    )
    command = await install.engine.router.create_session("slack:D0456", source="slack")

    assert await _origin_of(install.db, slack) == (
        "slack", {"channel": "C0123", "thread": "1.1", "channel_name": "general"},
    )
    assert await _origin_of(install.db, telegram) == ("telegram", {"channel": "42"})
    assert await _origin_of(install.db, web) == ("chat", {})
    assert await _origin_of(install.db, command) == ("slack", {"channel": "D0456"})


@pytest.mark.asyncio
async def test_plan_sessions(install, monkeypatch):
    _record_runs(install, monkeypatch)
    approved = await _pending_plan(install, "origin-approve")
    revised = await _pending_plan(install, "origin-revise")
    async with _client(install.app()) as client:
        response = await client.post(
            f"/api/plans/{approved}/approve", headers=install.headers("bob"),
        )
        assert response.status_code == 200
        assert (await client.post(
            f"/api/plans/{revised}/revise", headers=install.headers("bob"),
            json={"feedback": "add tests"},
        )).status_code == 200
    await asyncio.wait_for(install.ran.wait(), timeout=5)

    impl = response.json()["impl_session_id"]
    assert await _origin_of(install.db, impl) == (
        "plan", {"plan_id": approved, "task_id": "task-origin-approve"},
    )
    assert await _origin_of(install.db, "planner-origin-revise") == (
        "plan", {"plan_id": revised, "task_id": "task-origin-revise"},
    )


@pytest.mark.asyncio
async def test_external_sessions(install):
    resolver = SatelliteSessionResolver(install.db)
    satellite = await resolver.resolve(client_name="claude-code", mcp_session_id="m1")
    codex = await resolver.resolve(
        client_name="codex", mcp_session_id="m2",
        client_session_id="aaaaaaaa-1234-5678-9abc-def012345678",
    )
    # The MCP endpoint creates a satellite session for a request without one.
    resolve = build_ctx_resolver(install.engine, resolver)
    endpoint = await resolve(SimpleNamespace(request=None, session=None))

    assert await _origin_of(install.db, satellite) == ("external", {"client": "claude-code"})
    assert await _origin_of(install.db, codex) == ("external", {"client": "codex"})
    assert (await _origin_of(install.db, endpoint.session_id))[0] == "external"
