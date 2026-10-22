"""The derivation: a step is done because a fact says so.

Pure tests over :mod:`nerve.setup_checklist` — one row per rule in the status
table, in both auth modes, plus the lockdown branch, the arithmetic for
``finished`` / ``steps_left``, the masking rules and the etag subset.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from nerve import setup_checklist as checklist
from nerve.config import NerveConfig
from nerve.setup_state import SetupState
from nerve.setup_writer import apply_mode, apply_profile_templates, set_optional_crons


_SOURCES_OFF = {
    "github": {"enabled": False}, "github_events": {"enabled": False},
    "gmail": {"enabled": False}, "telegram": {"enabled": False},
}


def cfg(ws: Path, **top) -> NerveConfig:
    """A config shaped like a slim install: every source explicitly off.

    ``NerveConfig()`` alone enables the GitHub source by default, which would
    make the Sources step read done on an instance nobody configured.
    """
    data = {"workspace": str(ws), "sync": dict(_SOURCES_OFF)}
    sync = top.pop("sync", None)
    if sync:
        data["sync"] = {**_SOURCES_OFF, **{k: {**_SOURCES_OFF.get(k, {}), **v} for k, v in sync.items()}}
    data.update(top)
    return NerveConfig.from_dict(data)


def facts(ws: Path, config: NerveConfig | None = None, **overrides) -> checklist.Facts:
    config = config or cfg(ws)
    base = dict(
        config=config, workspace=ws, state=SetupState(), external_auth=False,
        unclaimed=False, lockdown=False, writable_problem=None,
        restart_pending=[], telegram_live=False,
    )
    base.update(overrides)
    if base.get("lockdown") and base.get("writable_problem") is None:
        base["writable_problem"] = checklist.LOCKDOWN_TEXT
    return checklist.Facts(**base)


def step(f: checklist.Facts, step_id: str) -> dict:
    return next(s for s in checklist.summarize(f)["steps"] if s["id"] == step_id)


def statuses(f: checklist.Facts) -> dict[str, str]:
    return {s["id"]: s["status"] for s in checklist.summarize(f)["steps"]}


@pytest.fixture
def personal(tmp_path) -> Path:
    ws = tmp_path / "ws"
    apply_mode(ws, "personal")
    return ws


@pytest.fixture
def worker(tmp_path) -> Path:
    ws = tmp_path / "ws"
    apply_mode(ws, "worker", "Watch the nightly build for example/repo and propose fixes.")
    return ws


class TestCatalogue:
    def test_before_a_mode_the_personal_list_is_sent_with_dependents_blocked(self, tmp_path):
        ws = tmp_path / "ws"
        ws.mkdir()
        s = statuses(facts(ws, unclaimed=True))
        assert list(s) == ["claim", "mode", "provider", "profile", "memory", "channels",
                           "sources", "automation", "agents"]
        assert s["claim"] == "pending" and s["mode"] == "pending"
        assert s["profile"] == s["sources"] == s["automation"] == s["agents"] == "blocked"
        assert step(facts(ws), "profile")["blocked_reason"].startswith("Choose a mode first")
        assert "job catalogue" in step(facts(ws), "automation")["blocked_reason"]
        # Waiting for the Mode step is sequencing, not a lock: still work to do.
        for step_id in ("profile", "sources", "automation", "agents"):
            assert step(facts(ws), step_id)["blocked_kind"] == "sequencing", step_id
        assert step(facts(ws), "mode")["blocked_kind"] is None

    def test_external_mode_has_no_claim_step(self, personal):
        s = statuses(facts(personal, external_auth=True))
        assert "claim" not in s
        assert checklist.summarize(facts(personal, external_auth=True))["auth_mode"] == "external"

    def test_worker_has_no_sources_or_agents_and_an_operator_step(self, worker):
        summary = checklist.summarize(facts(worker))
        ids = [s["id"] for s in summary["steps"]]
        assert "sources" not in ids and "agents" not in ids
        assert next(s["title"] for s in summary["steps"] if s["id"] == "profile") == "Operator"
        assert summary["mode"] == "worker"

    def test_titles_are_short_labels(self, personal):
        titles = {s["id"]: s["title"] for s in checklist.summarize(facts(personal))["steps"]}
        assert titles == {
            "claim": "Claim", "mode": "Mode", "provider": "Model", "profile": "About you",
            "memory": "Memory", "channels": "Channels", "sources": "Sources",
            "automation": "Automation", "agents": "Agents",
        }


class TestDoneRules:
    def test_claim(self, personal):
        assert statuses(facts(personal, unclaimed=True))["claim"] == "pending"
        done = step(facts(personal, account_username="alice"), "claim")
        assert done["status"] == "done"
        assert done["summary"] == "alice · password set"
        assert done["detail"] == {"username": "alice", "passwordless": False}

    def test_mode_is_done_and_read_only_once_chosen(self, personal):
        s = step(facts(personal), "mode")
        assert s["status"] == "done" and s["read_only"] is True
        assert s["read_only_reason"] == checklist.MODE_LOCK_REASON
        assert s["detail"]["mode"] == "personal"
        assert "SOUL.md" in s["detail"]["templates_present"]
        assert s["summary"] == "Personal assistant"

    @pytest.mark.parametrize("field, value", [
        ("anthropic_api_key", "sk-ant-REPLACE0000000000"),
        ("claude_oauth_token", "sk-ant-oat01-REPLACE"),
    ])
    def test_provider_done_from_config_secrets(self, personal, field, value):
        config = cfg(personal, **{field: value})
        s = step(facts(personal, config), "provider")
        assert s["status"] == "done"
        assert s["detail"][field] == {"configured": True, "last4": value[-4:], "source": "file"}
        assert s["detail"]["credential_source"] == "file"
        assert value not in str(s)

    def test_provider_done_from_bedrock_proxy_env_or_host_login_answer(self, personal):
        bedrock = cfg(personal, provider={"type": "bedrock", "aws_region": "eu-central-1"})
        assert step(facts(personal, bedrock), "provider")["detail"]["credential"] == "bedrock"
        proxy = cfg(personal, proxy={"enabled": True})
        assert step(facts(personal, proxy), "provider")["detail"]["credential"] == "proxy"
        env = step(facts(personal, env_credential=True), "provider")
        assert env["status"] == "done" and env["detail"]["credential"] == "environment"
        oauth_env = step(facts(personal, env_credential=True, env_oauth_only=True), "provider")
        assert oauth_env["detail"]["warning"] == "oauth_background_jobs"
        assert oauth_env["detail"]["type"] == "oauth"
        answered = facts(personal, state=SetupState(answered={"provider"}))
        assert statuses(answered)["provider"] == "done"
        assert statuses(facts(personal))["provider"] == "pending"

    def test_subscription_token_warns_only_without_an_api_key(self, personal):
        token = cfg(personal, claude_oauth_token="sk-ant-oat01-REPLACE")
        alone = step(facts(personal, token), "provider")["detail"]
        assert alone["credential"] == "oauth_token" and alone["warning"] == "oauth_background_jobs"
        # ANTHROPIC_API_KEY in the environment serves the background jobs: no warning.
        with_env_key = step(facts(personal, token, env_credential=True), "provider")["detail"]
        assert with_env_key["credential"] == "oauth_token" and with_env_key["warning"] is None
        # An environment that holds only an OAuth token still warns.
        env_oauth = step(facts(personal, token, env_credential=True, env_oauth_only=True), "provider")["detail"]
        assert env_oauth["warning"] == "oauth_background_jobs"

    def test_provider_detection_alone_is_not_done(self, personal):
        """Detection is an offer, not a decision (C6)."""
        assert statuses(facts(personal))["provider"] == "pending"

    def test_profile_done_rules(self, personal):
        assert statuses(facts(personal))["profile"] == "pending"
        tz = cfg(personal, timezone="Europe/Berlin")
        assert statuses(facts(personal, tz))["profile"] == "done"
        assert statuses(facts(personal, state=SetupState(answered={"profile"})))["profile"] == "done"
        apply_profile_templates(personal, name="Alice")
        s = step(facts(personal), "profile")
        assert s["status"] == "done"
        assert s["detail"]["user_md_pristine"]["name"] is False
        assert s["detail"]["timezone_is_default"] is True
        assert s["detail"]["quiet_is_default"] is True
        skipped = facts(personal, state=SetupState(skipped={"profile"}))
        assert statuses(skipped)["profile"] == "done", "done beats skipped"

    def test_profile_is_never_derived_from_local_account_facts(self, personal):
        """Only config and workspace facts: a display name on the account changes nothing."""
        with_name = facts(personal, display_name="Alice", account_username="alice")
        assert statuses(with_name)["profile"] == "pending"
        assert step(with_name, "profile")["detail"]["name"] == "Alice"

    def test_profile_in_external_mode_names_the_header(self, personal):
        s = step(facts(personal, external_auth=True, display_name="Alice Example"), "profile")
        assert s["detail"]["name"] == "Alice Example"
        assert s["detail"]["name_source"] == "actor_header"
        assert s["detail"]["display_name_editable"] is False

    def test_memory(self, personal):
        assert statuses(facts(personal))["memory"] == "pending"
        key = cfg(personal, openai_api_key="sk-REPLACE00000000")
        s = step(facts(personal, key), "memory")
        assert s["status"] == "done" and s["detail"]["embeddings_chosen"] is True
        assert s["detail"]["openai_api_key"]["last4"] == "0000"
        chosen = step(facts(personal, state=SetupState(answered={"memory"})), "memory")
        assert chosen["status"] == "done" and chosen["summary"] == "LLM-ranked recall (chosen)"

    def test_channels(self, personal):
        assert statuses(facts(personal))["channels"] == "pending"
        tg = cfg(personal, telegram={"bot_token": "123456:REPLACEabcdefghijklmnopqrstuvwxyz"})
        s = step(facts(personal, tg), "channels")
        assert s["status"] == "done"
        assert s["detail"]["telegram"]["pairing_available"] is False
        assert s["detail"]["telegram"]["pairing_unavailable_reason"] == checklist.PAIRING_AFTER_RESTART
        live = step(facts(personal, tg, telegram_live=True, bot_username="example_assistant_bot"), "channels")
        assert live["detail"]["telegram"]["pairing_available"] is True
        assert live["detail"]["telegram"]["bot_username"] == "example_assistant_bot"
        assert "@example_assistant_bot" in live["summary"]
        slack = cfg(personal, slack={"bot_token": "xoxb-REPLACE", "app_token": "xapp-REPLACE"})
        assert statuses(facts(personal, slack))["channels"] == "done"
        half = cfg(personal, slack={"bot_token": "xoxb-REPLACE"})
        assert statuses(facts(personal, half))["channels"] == "pending"

    def test_channels_start_error_is_surfaced(self, personal):
        tg = cfg(personal, telegram={"bot_token": "123456:REPLACEabcdefghijklmnopqrstuvwxyz"})
        s = step(facts(personal, tg, telegram_start_error="InvalidToken: <token>"), "channels")
        assert s["detail"]["telegram"]["start_error"] == "InvalidToken: <token>"

    def test_sources(self, personal):
        assert statuses(facts(personal))["sources"] == "pending"
        gh = cfg(personal, sync={"github": {"enabled": True}})
        assert statuses(facts(personal, gh))["sources"] == "done"
        assert statuses(facts(personal, state=SetupState(answered={"sources"})))["sources"] == "done"
        api = cfg(personal, sync={"telegram": {"api_id": 1234567, "api_hash": "REPLACE_HASH_VALUE"}})
        d = step(facts(personal, api), "sources")["detail"]
        assert d["telegram"]["api_id"] == {"configured": True, "last4": None, "source": "file"}, "ints never expose digits"
        assert d["telegram"]["api_hash"]["configured"] is True
        assert d["telegram"]["session"] == {"present": False, "authorized": False, "user": None}
        assert d["github"] == {"enabled": False, "source": None}
        assert d["gmail"] == {"enabled": False, "accounts": [], "source": None}
        # Where the page may store credentials every source is on offer.
        assert d["available"] == ["github", "gmail", "telegram"] and d["unavailable_reason"] is None

    def test_sources_show_the_gateway_when_only_github_is_hosted(self, personal):
        """As the Channels step does for a hosted Slack; volatile, so outside
        the etag. An open stream is not a connected GitHub: records also need
        a GitHub credential connected to the agent, which Nerve cannot see."""
        hosted = cfg(personal, sync={"github": {"enabled": True, "mode": "hosted"}})
        live = {"streams": 1, "connected": True, "last_event_at": "2026-10-21T09:00:00+00:00"}

        connected = step(facts(personal, hosted, hosted_gateway=live), "sources")
        waiting = step(facts(personal, hosted), "sources")

        assert connected["detail"]["github"] == {
            "enabled": True, "source": None, "mode": "hosted", "gateway": live,
        }
        assert connected["summary"] == (
            "GitHub from the channel gateway · stream connected; records arrive once a GitHub "
            "credential is connected to this agent"
        )
        assert waiting["detail"]["github"]["gateway"] == {"streams": 0, "connected": False, "last_event_at": None}
        assert waiting["summary"] == "GitHub from the channel gateway · waiting for the gateway"
        assert connected["etag"] == waiting["etag"]
        assert step(facts(personal, hosted), "channels")["detail"]["slack"]["gateway"] is None

    def test_automation(self, personal):
        s = step(facts(personal), "automation")
        assert s["status"] == "pending"
        assert s["detail"]["file_present"] is True
        assert s["summary"] == "2 of 4 optional jobs on"
        ids = [o["id"] for o in s["detail"]["optional"]]
        assert ids == ["inbox-processor", "task-planner", "skill-extractor", "skill-reviser"]
        inbox = s["detail"]["optional"][0]
        assert inbox["requirement_met"] is False and inbox["schedule_text"] == "every 30 minutes"
        assert inbox["default"] is True and inbox["enabled"] is True
        # An operator-edited file is a decision.
        set_optional_crons(personal, {"inbox-processor"})
        assert statuses(facts(personal))["automation"] == "done"

    def test_automation_worker_defaults(self, worker):
        """task-planner is offered but off by default for a worker — the
        detail says so per job, which is what the page's hint keys on."""
        s = step(facts(worker), "automation")
        optional = {o["id"]: o for o in s["detail"]["optional"]}
        assert list(optional) == ["task-planner", "skill-extractor", "skill-reviser"]
        assert {i: (o["enabled"], o["default_enabled"]) for i, o in optional.items()} == {
            "task-planner": (False, False),
            "skill-extractor": (True, True),
            "skill-reviser": (True, True),
        }
        assert all(o["default"] == o["default_enabled"] for o in optional.values()), "the older name stays"
        assert s["status"] == "pending", "the file matches the worker defaults"
        assert s["summary"] == "2 of 3 optional jobs on"
        # Turning the planner on is a decision, as any other change is.
        set_optional_crons(worker, {"task-planner", "skill-extractor", "skill-reviser"})
        assert statuses(facts(worker))["automation"] == "done"

    def test_automation_personal_defaults_are_unchanged(self, personal):
        optional = {o["id"]: o["default_enabled"] for o in step(facts(personal), "automation")["detail"]["optional"]}
        assert optional == {
            "inbox-processor": True, "task-planner": True, "skill-extractor": False, "skill-reviser": False,
        }

    def test_quiet_hours_detail_follows_the_mode_default(self, tmp_path):
        """``quiet_enabled`` is "both times set"; ``quiet_is_default`` compares
        against the mode's own default — personal 02:00–08:00, a worker none."""
        personal, worker = tmp_path / "personal", tmp_path / "worker"
        apply_mode(personal, "personal")
        apply_mode(worker, "worker", "Watch the nightly build for example/repo and propose fixes.")
        cases = [
            (personal, {}, (True, "02:00", "08:00", True)),
            (personal, {"quiet_start": "22:00", "quiet_end": "07:00"}, (True, "22:00", "07:00", False)),
            (personal, {"quiet_start": "", "quiet_end": ""}, (False, "", "", False)),
            (worker, {"quiet_start": "", "quiet_end": ""}, (False, "", "", True)),
            (worker, {"quiet_start": "22:00", "quiet_end": "07:00"}, (True, "22:00", "07:00", False)),
            # A key no layer states is the loader's window: on, and not a worker's default.
            (worker, {}, (True, "02:00", "08:00", False)),
            # Half a pair is no quiet hours at all (the representation is both or neither).
            (worker, {"quiet_start": "22:00", "quiet_end": ""}, (False, "22:00", "", False)),
        ]
        for ws, quiet, expected in cases:
            detail = step(facts(ws, cfg(ws, **quiet)), "profile")["detail"]
            got = (detail["quiet_enabled"], detail["quiet_start"], detail["quiet_end"], detail["quiet_is_default"])
            assert got == expected, (ws.name, quiet)

    def test_switching_quiet_hours_off_changes_the_profile_etag(self, worker):
        on = step(facts(worker, cfg(worker, quiet_start="22:00", quiet_end="07:00")), "profile")
        off = step(facts(worker, cfg(worker, quiet_start="", quiet_end="")), "profile")
        assert on["etag"] != off["etag"]
        assert "quiet_enabled" in checklist.ETAG_FIELDS["profile"]

    def test_agents(self, personal):
        assert statuses(facts(personal))["agents"] == "pending"
        agents_cfg = cfg(personal, external_agents={"targets": [{"name": "codex"}]})
        s = step(facts(personal, agents_cfg, available_agents=("codex", "claude-code")), "agents")
        assert s["status"] == "done" and s["detail"]["enabled"] == ["codex"]
        assert s["detail"]["available"] == ["codex", "claude-code"]
        assert statuses(facts(personal, state=SetupState(answered={"agents"})))["agents"] == "done"

    def test_onboarding_flows_into_the_mode_detail(self, worker):
        s = step(facts(worker, onboarding="running", onboarding_session_id="onboarding-1"), "mode")
        assert s["detail"]["onboarding"] == "running"
        assert s["detail"]["onboarding_session_id"] == "onboarding-1"
        assert s["detail"]["task_present"] is True
        assert checklist.onboarding_from_task(worker, "worker") == "pending"
        (worker / "TASK.md").write_text("# Task\n\n## Mission\nx\n", encoding="utf-8")
        assert checklist.onboarding_from_task(worker, "worker") == "done"
        assert checklist.onboarding_from_task(worker, "personal") == "not_applicable"

    def test_the_onboarding_block_carries_what_it_waits_for_only_while_running(self, worker):
        running = checklist.summarize(facts(
            worker, onboarding="running", onboarding_session_id="onboarding-1",
            onboarding_kind="conversation", onboarding_awaiting="plan",
        ))
        assert running["onboarding"] == {
            "status": "running", "session_id": "onboarding-1", "kind": "conversation", "awaiting": "plan",
        }
        for status in ("pending", "done", "skipped", "not_applicable"):
            block = checklist.summarize(facts(worker, onboarding=status, onboarding_awaiting="question"))["onboarding"]
            assert block["status"] == status and block["awaiting"] is None
        assert checklist.summarize(facts(worker))["onboarding"]["awaiting"] is None

    def test_onboarding_is_done_once_the_marker_is_gone(self, worker):
        """The template's marker comment decides, not the headings: a filled
        file with the marker still standing is pending; without it, done."""
        task = worker / "TASK.md"
        text = task.read_text(encoding="utf-8")
        lines = text.splitlines()
        assert lines[1].startswith("<!-- nerve:onboarding pending")
        assert "## Mission" in text, "the template already carries the heading"
        task.write_text(text.replace(
            "_(onboarding fills this in: one paragraph ending in the quality bar)_",
            "Keep CI green on org/repo-a.",
        ), encoding="utf-8")
        assert checklist.onboarding_from_task(worker, "worker") == "pending"
        task.write_text("\n".join(lines[:1] + lines[2:]) + "\n", encoding="utf-8")
        assert checklist.onboarding_from_task(worker, "worker") == "done"
        task.unlink()
        assert checklist.onboarding_from_task(worker, "worker") == "pending"


class TestFinishedAndStepsLeft:
    def test_steps_left_counts_pending_only(self, personal):
        state = SetupState(skipped={"memory", "channels", "sources", "agents"})
        summary = checklist.summarize(facts(personal, state=state))
        pending = [s["id"] for s in summary["steps"] if s["status"] == "pending"]
        assert pending == ["provider", "profile", "automation"]
        assert summary["steps_left"] == 3

    def test_steps_left_counts_the_steps_waiting_for_the_mode(self, tmp_path):
        """Before Mode is chosen the four steps that depend on it are blocked
        only by sequencing — they are still left to do. Counting pending steps
        alone read "4 left" after the claim and "6 left" after Mode and Model
        were done: a number that rises as the person progresses."""
        ws = tmp_path / "ws"
        ws.mkdir()
        local = checklist.summarize(facts(ws, account_username="alice"))
        assert local["steps_left"] == 8  # everything but the claim
        external = checklist.summarize(facts(ws, external_auth=True))
        assert external["steps_left"] == 8  # no claim step at all

    def test_steps_left_never_rises_as_the_person_progresses(self, tmp_path):
        ws = tmp_path / "ws"
        ws.mkdir()
        counts = [checklist.summarize(facts(ws, account_username="alice"))["steps_left"]]
        apply_mode(ws, "personal")  # Mode: four blocked steps turn pending
        counts.append(checklist.summarize(facts(ws, account_username="alice"))["steps_left"])
        config = cfg(ws, anthropic_api_key="sk-ant-REPLACE0000000000")  # Model
        counts.append(checklist.summarize(facts(ws, config, account_username="alice"))["steps_left"])
        config = cfg(ws, anthropic_api_key="sk-ant-REPLACE0000000000", timezone="UTC")  # About you
        counts.append(checklist.summarize(facts(ws, config, account_username="alice"))["steps_left"])
        state = SetupState(skipped={"memory", "channels", "sources", "agents"}, answered={"automation"})
        counts.append(checklist.summarize(facts(ws, config, state=state, account_username="alice"))["steps_left"])
        assert counts == [8, 7, 6, 5, 0]
        assert counts == sorted(counts, reverse=True)

    def test_finished_requires_required_done_and_no_restart(self, personal):
        config = cfg(personal, anthropic_api_key="sk-ant-REPLACE0000000000", timezone="UTC")
        state = SetupState(skipped={"memory", "channels", "sources", "agents"}, answered={"automation"})
        summary = checklist.summarize(facts(personal, config, state=state, account_username="alice"))
        assert summary["steps_left"] == 0
        assert summary["finished"] is True
        pending_restart = checklist.summarize(facts(
            personal, config, state=state, restart_pending=["timezone: 'A' → 'B'"],
        ))
        assert pending_restart["finished"] is False
        assert pending_restart["restart_pending"] is True
        assert pending_restart["restart_reasons"] == ["timezone: 'A' → 'B'"]
        debt = SetupState(skipped=state.skipped, answered=state.answered, debts={"cron": "x"})
        with_debt = checklist.summarize(facts(personal, config, state=debt))
        assert with_debt["finished"] is False and with_debt["restart_reasons"] == ["cron: x"]

    def test_a_required_pending_step_is_never_finished(self, personal):
        state = SetupState(skipped={"memory", "channels", "sources", "agents", "profile"}, answered={"automation"})
        assert checklist.summarize(facts(personal, state=state))["finished"] is False


class TestLockdown:
    """One row per step (C33): a config step that is not done is ``blocked``,
    never ``pending``, so the banner stops nagging and ``finished`` is reachable."""

    def test_mode_without_templates_is_blocked_with_the_repo_hint(self, tmp_path):
        ws = tmp_path / "ws"
        ws.mkdir()
        s = step(facts(ws, lockdown=True), "mode")
        assert s["status"] == "blocked" and s["read_only"] is True
        assert "config repository" in s["blocked_reason"] and "SOUL.md" in s["blocked_reason"]

    def test_mode_with_templates_is_done_and_read_only(self, personal):
        s = step(facts(personal, lockdown=True), "mode")
        assert s["status"] == "done" and s["read_only"] is True

    @pytest.mark.parametrize("step_id", ["provider", "memory", "channels", "sources", "automation", "agents"])
    def test_config_steps_not_done_are_blocked(self, personal, step_id):
        s = step(facts(personal, lockdown=True), step_id)
        assert s["status"] == "blocked"
        assert s["blocked_reason"].startswith(checklist.LOCKDOWN_TEXT)
        assert s["read_only"] is True
        assert s["blocked_kind"] == "lockdown"

    def test_locked_without_templates_counts_nothing(self, tmp_path):
        """Locked and no SOUL.md yet: Mode itself is lockdown-blocked, so the
        steps waiting for it cannot be answered here either — none of them is
        "left" (C13), even though their reason still says to choose a mode."""
        ws = tmp_path / "ws"
        ws.mkdir()
        summary = checklist.summarize(facts(ws, lockdown=True, account_username="alice"))
        kinds = {s["id"]: s["blocked_kind"] for s in summary["steps"] if s["status"] == "blocked"}
        assert kinds["mode"] == "lockdown"
        assert kinds["profile"] == kinds["sources"] == kinds["automation"] == kinds["agents"] == "lockdown"
        assert summary["steps_left"] == 0

    def test_blocked_beats_skipped(self, personal):
        s = step(facts(personal, lockdown=True, state=SetupState(skipped={"memory"})), "memory")
        assert s["status"] == "blocked"

    def test_done_config_steps_stay_done_but_read_only(self, personal):
        config = cfg(personal, anthropic_api_key="sk-ant-REPLACE0000000000")
        s = step(facts(personal, config, lockdown=True), "provider")
        assert s["status"] == "done" and s["read_only"] is True
        assert s["read_only_reason"] == checklist.LOCKDOWN_TEXT

    def test_profile_local_stays_pending_with_name_writable(self, personal):
        s = step(facts(personal, lockdown=True), "profile")
        assert s["status"] == "pending"
        assert s["detail"]["writable_fields"] == ["name"]
        assert s["read_only"] is False

    def test_profile_external_is_blocked(self, personal):
        s = step(facts(personal, lockdown=True, external_auth=True), "profile")
        assert s["status"] == "blocked"
        assert "organisation's sign-in" in s["blocked_reason"]

    def test_locked_instance_can_finish(self, personal):
        """Fully provisioned from the repository and the environment: nothing left."""
        config = cfg(personal, anthropic_api_key="sk-ant-REPLACE0000000000", timezone="UTC")
        summary = checklist.summarize(facts(personal, config, lockdown=True, external_auth=True))
        assert summary["steps_left"] == 0
        assert summary["finished"] is True
        assert summary["writable"] is False
        assert summary["read_only_reason"] == checklist.LOCKDOWN_TEXT

    def test_steps_left_ignores_blocked_steps(self, personal):
        config = cfg(personal, anthropic_api_key="sk-ant-REPLACE0000000000")
        summary = checklist.summarize(facts(personal, config, lockdown=True, external_auth=True))
        assert summary["steps_left"] == 0
        local = checklist.summarize(facts(personal, config, lockdown=True, account_username="alice"))
        assert local["steps_left"] == 1  # profile (name) is the only pending step


def keyless(ws: Path, **top) -> NerveConfig:
    """A config whose setup page may not store a credential (``setup.local_secrets: false``)."""
    return cfg(ws, setup={"local_secrets": False}, **top)


# The operator provides the GitHub token from the environment (GH_TOKEN /
# GITHUB_TOKEN in the service environment, or a ${VAR} reference).
GITHUB_ENV = {"github_token": "environment"}


class TestNoLocalSecrets:
    """``setup.local_secrets: false``: a step whose only purpose is to store a
    credential on this machine is ``blocked`` with ``policy`` — never pending,
    never counted — unless a fact that stores none already made it done."""

    POLICY_STEPS = ("memory", "channels", "sources", "agents")

    @pytest.mark.parametrize("step_id", POLICY_STEPS)
    def test_steps_that_store_a_credential_are_closed(self, personal, step_id):
        s = step(facts(personal, keyless(personal)), step_id)
        assert s["status"] == "blocked"
        assert s["blocked_kind"] == "policy"
        assert s["blocked_reason"] == checklist.NO_LOCAL_SECRETS_TEXT
        assert s["read_only"] is True and s["read_only_reason"] == checklist.NO_LOCAL_SECRETS_TEXT

    def test_the_other_steps_are_untouched(self, personal):
        s = statuses(facts(personal, keyless(personal), account_username="alice"))
        assert s["claim"] == "done" and s["mode"] == "done"
        assert s["provider"] == "pending" and s["profile"] == "pending" and s["automation"] == "pending"
        provider = step(facts(personal, keyless(personal)), "provider")
        assert provider["read_only"] is False and provider["blocked_kind"] is None

    def test_the_policy_beats_a_skip_and_the_wait_for_mode(self, tmp_path):
        """Choosing a mode would not open these steps, so they are not work
        left to do either: ``policy``, not ``sequencing``."""
        ws = tmp_path / "ws"
        ws.mkdir()
        summary = checklist.summarize(facts(ws, keyless(ws), state=SetupState(skipped={"memory"})))
        kinds = {s["id"]: s["blocked_kind"] for s in summary["steps"] if s["status"] == "blocked"}
        assert kinds["memory"] == kinds["sources"] == kinds["agents"] == "policy"
        assert kinds["profile"] == kinds["automation"] == "sequencing"
        assert summary["steps_left"] == 4  # mode, provider, profile, automation

    def test_hosted_slack_keeps_the_channels_step_done_and_its_switch(self, personal):
        config = keyless(personal, slack={"mode": "hosted"})
        s = step(facts(personal, config), "channels")
        assert s["status"] == "done" and s["read_only"] is False
        assert s["detail"]["slack"]["bot_token"]["source"] == "gateway"
        assert s["detail"]["telegram"]["pairing_available"] is False
        assert s["detail"]["telegram"]["pairing_unavailable_reason"] == checklist.NO_LOCAL_SECRETS_TEXT

    def test_a_credential_the_operator_provided_stays_done_but_read_only(self, personal):
        config = keyless(
            personal, openai_api_key="sk-REPLACE00000000",
            telegram={"bot_token": "123456:REPLACEabcdefghijklmnopqrstuvwxyz0123"},
        )
        sources = {"openai_api_key": "environment", "telegram.bot_token": "environment"}
        f = facts(personal, config, credential_sources=sources)
        memory = step(f, "memory")
        assert memory["status"] == "done" and memory["read_only"] is True
        assert memory["read_only_reason"] == checklist.NO_LOCAL_SECRETS_TEXT
        assert memory["detail"]["openai_api_key"]["source"] == "environment"
        channels = step(f, "channels")
        assert channels["status"] == "done" and channels["read_only"] is True
        assert channels["detail"]["telegram"]["pairing_unavailable_reason"] == checklist.NO_LOCAL_SECRETS_TEXT

    def test_an_answer_that_stores_nothing_stays_done(self, personal):
        f = facts(personal, keyless(personal), state=SetupState(answered={"memory", "sources", "agents"}))
        for step_id in ("memory", "sources", "agents"):
            s = step(f, step_id)
            assert s["status"] == "done" and s["read_only"] is True, step_id

    def test_the_model_step_is_not_blocked(self, personal):
        """Bedrock needs no secret on disk and a credential can arrive from the
        environment, so the required step stays answerable."""
        pending = step(facts(personal, keyless(personal)), "provider")
        assert pending["status"] == "pending"
        env = step(facts(personal, keyless(personal), env_credential=True), "provider")
        assert env["status"] == "done" and env["detail"]["credential_source"] == "environment"
        assert env["read_only"] is False  # the model picker stays open
        bedrock = step(facts(personal, keyless(personal, provider={"type": "bedrock", "aws_region": "eu-central-1"})), "provider")
        assert bedrock["status"] == "done" and bedrock["read_only"] is False

    def test_steps_left_and_finished_ignore_the_closed_steps(self, personal):
        config = keyless(personal, slack={"mode": "hosted"})
        f = facts(personal, config, env_credential=True, state=SetupState(answered={"profile", "automation"}))
        summary = checklist.summarize(f)
        assert summary["local_secrets"] is False
        assert summary["steps_left"] == 0
        assert summary["finished"] is True
        # Still writable: the policy governs what the page stores, not the files.
        assert summary["writable"] is True and summary["read_only_reason"] is None

    def test_the_summary_says_when_secrets_may_be_kept(self, personal):
        assert checklist.summarize(facts(personal))["local_secrets"] is True

    def test_a_github_token_from_the_environment_keeps_that_toggle_open(self, personal):
        """Switching the GitHub source on stores nothing here — the operator
        holds the token — so the step stays open for that one decision:
        pending, counted, skippable; Gmail and Telegram stay closed."""
        f = facts(personal, keyless(personal), credential_sources=GITHUB_ENV)
        s = step(f, "sources")
        assert s["status"] == "pending" and s["blocked_kind"] is None and s["blocked_reason"] is None
        assert s["read_only"] is False and s["read_only_reason"] is None
        assert s["detail"]["available"] == ["github"]
        assert s["detail"]["unavailable_reason"] == checklist.SOURCES_GITHUB_ONLY_TEXT
        assert s["detail"]["github"] == {"enabled": False, "source": "environment"}
        summary = checklist.summarize(f)
        assert summary["steps_left"] == 4  # provider, profile, sources, automation
        kinds = {x["id"]: x["blocked_kind"] for x in summary["steps"] if x["status"] == "blocked"}
        assert kinds == {"memory": "policy", "channels": "policy", "agents": "policy"}
        skipped = facts(personal, keyless(personal), credential_sources=GITHUB_ENV, state=SetupState(skipped={"sources"}))
        assert step(skipped, "sources")["status"] == "skipped"
        assert checklist.summarize(skipped)["steps_left"] == 3

    def test_a_hosted_github_keeps_that_toggle_open_without_a_token(self, personal):
        """The channel gateway polls GitHub, so the switch needs no credential
        on this machine and stores nothing here."""
        f = facts(personal, keyless(personal, sync={"github": {"mode": "hosted"}}))
        s = step(f, "sources")
        assert s["status"] == "pending" and s["blocked_kind"] is None
        assert s["read_only"] is False and s["read_only_reason"] is None
        assert s["detail"]["available"] == ["github"]
        assert s["detail"]["unavailable_reason"] == checklist.SOURCES_GITHUB_HOSTED_TEXT
        on = keyless(personal, sync={"github": {"enabled": True, "mode": "hosted"}})
        done = step(facts(personal, on), "sources")
        assert done["status"] == "done" and done["read_only"] is False and done["read_only_reason"] is None

    def test_github_on_with_a_token_from_the_environment_is_done_and_keeps_its_switch(self, personal):
        """Done like any switched-on source, but not read-only: the switch is
        the one control, as the hosted Slack's is."""
        on = keyless(personal, sync={"github": {"enabled": True}})
        s = step(facts(personal, on, credential_sources=GITHUB_ENV), "sources")
        assert s["status"] == "done" and s["read_only"] is False and s["read_only_reason"] is None
        assert s["summary"] == "GitHub" and s["detail"]["available"] == ["github"]
        # "No" is an answer too, and still open to change.
        answered = facts(personal, keyless(personal), credential_sources=GITHUB_ENV, state=SetupState(answered={"sources"}))
        s = step(answered, "sources")
        assert s["status"] == "done" and s["read_only"] is False and s["summary"] == "None"

    def test_a_source_the_operator_switched_on_is_shown_beside_the_github_toggle(self, personal):
        config = keyless(personal, sync={"gmail": {"enabled": True, "accounts": ["alice@example.com"]}})
        sources = {**GITHUB_ENV, "sync.gmail.accounts": "config-repo"}
        s = step(facts(personal, config, credential_sources=sources), "sources")
        assert s["status"] == "done" and s["read_only"] is False
        assert s["detail"]["available"] == ["github"]
        assert s["detail"]["gmail"] == {"enabled": True, "accounts": ["alice@example.com"], "source": "config-repo"}

    @pytest.mark.parametrize("sources", [{}, {"github_token": "file"}, {"github_token": "config-repo"}])
    def test_without_a_token_from_the_environment_the_sources_step_stays_closed(self, personal, sources):
        """No token, or one in a file on this machine or in the repository: the policy as before."""
        s = step(facts(personal, keyless(personal), credential_sources=sources), "sources")
        assert s["status"] == "blocked" and s["blocked_kind"] == "policy"
        assert s["detail"]["available"] == [] and s["detail"]["unavailable_reason"] == checklist.NO_LOCAL_SECRETS_TEXT
        on = keyless(personal, sync={"github": {"enabled": True}})
        done = step(facts(personal, on, credential_sources=sources), "sources")
        assert done["status"] == "done" and done["read_only"] is True
        assert done["read_only_reason"] == checklist.NO_LOCAL_SECRETS_TEXT and done["detail"]["available"] == []

    def test_the_github_toggle_still_waits_for_a_mode_and_yields_to_lockdown(self, tmp_path, personal):
        fresh = tmp_path / "fresh"  # a workspace that has never chosen a mode
        fresh.mkdir()
        s = step(facts(fresh, keyless(fresh), credential_sources=GITHUB_ENV), "sources")
        assert s["status"] == "blocked" and s["blocked_kind"] == "sequencing"
        locked = step(facts(personal, keyless(personal), credential_sources=GITHUB_ENV, lockdown=True), "sources")
        assert locked["status"] == "blocked" and locked["blocked_kind"] == "lockdown"
        assert locked["detail"]["available"] == ["github"]

    def test_what_is_on_offer_is_part_of_the_sources_etag(self, personal):
        """A policy flip changes the set of cards, so the form remounts."""
        every = step(facts(personal), "sources")["etag"]
        github_only = step(facts(personal, keyless(personal), credential_sources=GITHUB_ENV), "sources")["etag"]
        none = step(facts(personal, keyless(personal)), "sources")["etag"]
        assert len({every, github_only, none}) == 3


# The operator states ``sync.github.enabled`` in a machine layer.
GITHUB_HELD = {"sync.github.enabled": "config.local.yaml"}
_QUIET_GATEWAY = {"streams": 0, "connected": False, "last_event_at": None}
_GMAIL_TELEGRAM_CLOSED = " Gmail and Telegram are not available on this instance."


class TestGitHubSwitchHeldOnTheMachine:
    """Hosted GitHub with ``sync.github.enabled`` in config.yaml or
    config.local.yaml: those outrank the settings.yaml the page writes, so the
    switch is the operator's. The card is shown and not offered, and the step
    says that a GitHub credential connected to the agent is what decides."""

    def test_shown_not_offered_beside_the_other_sources(self, personal):
        """Where the page may store credentials, Gmail and Telegram stay on
        offer and the step stays open for them."""
        on = cfg(personal, sync={"github": {"enabled": True, "mode": "hosted"}})
        s = step(facts(personal, on, machine_switches=GITHUB_HELD), "sources")
        assert s["detail"]["available"] == ["gmail", "telegram"]
        assert s["detail"]["unavailable_reason"] == checklist.SOURCES_GITHUB_MANAGED_TEXT
        assert s["detail"]["github"] == {
            "enabled": True, "source": None, "mode": "hosted", "gateway": _QUIET_GATEWAY, "managed": True,
        }
        assert s["status"] == "done" and s["read_only"] is False and s["read_only_reason"] is None

    def test_the_summary_never_claims_a_connected_credential(self, personal):
        """Nerve cannot tell a credential that is not connected from a gateway
        it cannot reach, so the waiting line names both. Once a stream is open
        the line names the stream, and the credential stays a condition."""
        on = cfg(personal, sync={"github": {"enabled": True, "mode": "hosted"}})
        waiting = step(facts(personal, on, machine_switches=GITHUB_HELD), "sources")
        assert waiting["summary"] == (
            "GitHub · waiting for the channel gateway — if no GitHub credential is connected "
            "to this agent yet, connect one where your operator manages it"
        )
        live = {"streams": 1, "connected": True, "last_event_at": "2026-10-21T09:00:00+00:00"}
        connected = step(facts(personal, on, machine_switches=GITHUB_HELD, hosted_gateway=live), "sources")
        assert connected["summary"] == (
            "GitHub from the channel gateway · stream connected; records arrive once a GitHub "
            "credential is connected to this agent"
        )
        assert connected["etag"] == waiting["etag"], "the gateway's state is volatile: never remounts the form"

    def test_a_stream_a_hosted_slack_opens_is_not_a_connected_github(self, personal):
        """The gateway's streams carry every hosted provider, so a hosted Slack
        opens them whether or not a GitHub credential is connected to the
        agent. The Channels step may say Slack is connected; the Sources step
        names only the stream."""
        config = cfg(personal, slack={"mode": "hosted"}, sync={"github": {"enabled": True, "mode": "hosted"}})
        live = {"streams": 2, "connected": True, "last_event_at": "2026-10-21T09:00:00+00:00"}
        f = facts(personal, config, machine_switches=GITHUB_HELD, hosted_gateway=live)
        assert step(f, "channels")["summary"] == "Slack managed by the channel gateway · connected"
        assert step(f, "sources")["summary"] == (
            "GitHub from the channel gateway · stream connected; records arrive once a GitHub "
            "credential is connected to this agent"
        )

    def test_without_local_secrets_nothing_is_on_offer_and_the_done_step_is_read_only(self, personal):
        on = keyless(personal, sync={"github": {"enabled": True, "mode": "hosted"}})
        f = facts(personal, on, machine_switches=GITHUB_HELD)
        s = step(f, "sources")
        reason = checklist.SOURCES_GITHUB_MANAGED_TEXT + _GMAIL_TELEGRAM_CLOSED
        assert s["detail"]["available"] == [] and s["detail"]["unavailable_reason"] == reason
        assert s["status"] == "done" and s["blocked_kind"] is None
        assert s["read_only"] is True and s["read_only_reason"] == reason
        assert checklist.summarize(f)["steps_left"] == 3  # provider, profile, automation

    def test_held_off_says_so(self, personal):
        """Not claimed to arrive: the operator switched it off there."""
        off = keyless(personal, sync={"github": {"enabled": False, "mode": "hosted"}})
        f = facts(personal, off, machine_switches=GITHUB_HELD)
        s = step(f, "sources")
        reason = checklist.SOURCES_GITHUB_MANAGED_OFF_TEXT + _GMAIL_TELEGRAM_CLOSED
        assert s["status"] == "blocked" and s["blocked_kind"] == "policy" and s["blocked_reason"] == reason
        assert s["read_only"] is True and s["read_only_reason"] == reason
        assert s["detail"]["available"] == [] and s["detail"]["github"]["managed"] is True
        assert checklist.summarize(f)["steps_left"] == 3, "nothing on the page can open it"
        # Where the page may store credentials, the step stays open for Gmail and Telegram.
        local = cfg(personal, sync={"github": {"mode": "hosted"}})
        s = step(facts(personal, local, machine_switches=GITHUB_HELD), "sources")
        assert s["status"] == "pending" and s["detail"]["available"] == ["gmail", "telegram"]
        assert s["detail"]["unavailable_reason"] == checklist.SOURCES_GITHUB_MANAGED_OFF_TEXT

    def test_a_hosted_switch_no_machine_layer_states_stays_the_pages(self, personal):
        """As before: offered and open, with the hosted sentence and no ``managed`` key."""
        on = keyless(personal, sync={"github": {"enabled": True, "mode": "hosted"}})
        s = step(facts(personal, on, machine_switches={}), "sources")
        assert s["detail"]["available"] == ["github"]
        assert s["detail"]["unavailable_reason"] == checklist.SOURCES_GITHUB_HOSTED_TEXT
        assert "managed" not in s["detail"]["github"]
        assert s["status"] == "done" and s["read_only"] is False
        assert s["summary"] == "GitHub from the channel gateway · waiting for the gateway"

    def test_poll_mode_is_unchanged_by_a_machine_layer_switch(self, personal):
        on = cfg(personal, sync={"github": {"enabled": True}})
        s = step(facts(personal, on, machine_switches=GITHUB_HELD), "sources")
        assert s["detail"]["available"] == ["github", "gmail", "telegram"] and s["detail"]["unavailable_reason"] is None
        assert s["detail"]["github"] == {"enabled": True, "source": None}
        assert s["summary"] == "GitHub" and s["read_only"] is False
        env = facts(personal, keyless(personal), credential_sources=GITHUB_ENV, machine_switches=GITHUB_HELD)
        s = step(env, "sources")
        assert s["detail"]["available"] == ["github"]
        assert s["detail"]["unavailable_reason"] == checklist.SOURCES_GITHUB_ONLY_TEXT

    def test_the_switch_reason_answers_for_the_held_switch_only(self, personal):
        hosted = cfg(personal, sync={"github": {"enabled": True, "mode": "hosted"}})
        held = facts(personal, hosted, machine_switches=GITHUB_HELD)
        assert checklist.managed_switch_reason(held, "sync.github.enabled") == checklist.SOURCES_GITHUB_MANAGED_TEXT
        assert checklist.managed_switch_reason(held, "sync.gmail.enabled") is None
        assert checklist.managed_switch_reason(facts(personal, hosted), "sync.github.enabled") is None
        poll = facts(personal, cfg(personal, sync={"github": {"enabled": True}}), machine_switches=GITHUB_HELD)
        assert checklist.managed_switch_reason(poll, "sync.github.enabled") is None

    def test_holding_the_switch_remounts_the_form(self, personal):
        on = keyless(personal, sync={"github": {"enabled": True, "mode": "hosted"}})
        offered = step(facts(personal, on), "sources")["etag"]
        held = step(facts(personal, on, machine_switches=GITHUB_HELD), "sources")["etag"]
        assert offered != held


class TestMaskingAndEtag:
    def test_a_credential_from_the_environment_reads_as_managed(self, personal):
        env = step(facts(personal, env_credential=True), "provider")["detail"]
        assert env["anthropic_api_key"] == {"configured": True, "last4": None, "source": "environment"}
        assert env["claude_oauth_token"] == {"configured": False, "last4": None, "source": None}
        assert env["credential_source"] == "environment"
        oauth = step(facts(personal, env_credential=True, env_oauth_only=True), "provider")["detail"]
        assert oauth["claude_oauth_token"]["source"] == "environment" and oauth["anthropic_api_key"]["configured"] is False
        none = step(facts(personal), "provider")["detail"]
        assert none["credential_source"] is None
        bedrock = cfg(personal, provider={"type": "bedrock", "aws_region": "eu-central-1"})
        assert step(facts(personal, bedrock), "provider")["detail"]["credential_source"] is None

    def test_resolved_sources_flow_into_every_masked_value(self, personal):
        config = cfg(
            personal, anthropic_api_key="sk-ant-REPLACE0000000000", openai_api_key="sk-REPLACE00000000",
            telegram={"bot_token": "123456:REPLACEabcdefghijklmnopqrstuvwxyz0123"},
            slack={"bot_token": "xoxb-REPLACE", "app_token": "xapp-REPLACE"},
            sync={"telegram": {"api_id": 1234567, "api_hash": "REPLACE_HASH_VALUE"}, "gmail": {"accounts": ["alice@example.com"]}},
        )
        sources = {
            "anthropic_api_key": "environment", "openai_api_key": "config-repo", "telegram.bot_token": "environment",
            "slack.bot_token": "file", "slack.app_token": "environment", "sync.telegram.api_id": "file",
            "sync.telegram.api_hash": "environment", "github_token": "environment", "sync.gmail.accounts": "config-repo",
        }
        f = facts(personal, config, credential_sources=sources)
        provider = step(f, "provider")["detail"]
        assert provider["anthropic_api_key"]["source"] == "environment" and provider["credential_source"] == "environment"
        assert step(f, "memory")["detail"]["openai_api_key"]["source"] == "config-repo"
        channels = step(f, "channels")["detail"]
        assert channels["telegram"]["bot_token"]["source"] == "environment"
        assert channels["slack"]["bot_token"]["source"] == "file" and channels["slack"]["app_token"]["source"] == "environment"
        assert channels["slack"]["gateway"] is None
        sources_detail = step(f, "sources")["detail"]
        assert sources_detail["telegram"]["api_id"]["source"] == "file"
        assert sources_detail["telegram"]["api_hash"]["source"] == "environment"
        assert sources_detail["github"]["source"] == "environment"
        assert sources_detail["gmail"]["source"] == "config-repo"
        # Nothing resolved: a configured value is a file, an absent one has no source.
        plain = step(facts(personal, config), "sources")["detail"]
        assert plain["gmail"]["source"] == "file" and plain["github"]["source"] is None

    def test_hosted_slack_is_done_and_reports_the_gateway(self, personal):
        hosted = cfg(personal, slack={"mode": "hosted"})
        quiet = step(facts(personal, hosted), "channels")
        assert quiet["status"] == "done"
        assert quiet["detail"]["slack"]["bot_token"] == {"configured": True, "last4": None, "source": "gateway"}
        assert quiet["detail"]["slack"]["app_token"]["source"] == "gateway"
        assert quiet["detail"]["slack"]["gateway"] == {"streams": 0, "connected": False, "last_event_at": None}
        assert quiet["summary"] == "Slack managed by the channel gateway · waiting for the gateway"
        live = step(facts(personal, hosted, hosted_gateway={
            "streams": 2, "connected": True, "last_event_at": "2026-10-11T09:00:00Z",
        }), "channels")
        assert live["detail"]["slack"]["gateway"]["connected"] is True
        assert live["summary"] == "Slack managed by the channel gateway · connected"
        assert live["etag"] == quiet["etag"], "the gateway's state is volatile: never remounts the form"
        socket = step(facts(personal, cfg(personal, slack={"bot_token": "xoxb-REPLACE", "app_token": "xapp-REPLACE"})), "channels")
        assert socket["detail"]["slack"]["gateway"] is None and socket["summary"] == "Slack"

    def test_the_sources_summary_says_whether_telegram_is_signed_in(self, personal):
        on = cfg(personal, sync={"telegram": {"enabled": True, "api_id": 1234567, "api_hash": "REPLACE_HASH_VALUE"}})
        absent = step(facts(personal, on), "sources")
        assert absent["detail"]["telegram"]["session"] == {"present": False, "authorized": False, "user": None}
        assert absent["summary"] == "Telegram sync · not signed in"
        unverified = step(facts(personal, on, telegram_session_present=True), "sources")
        assert unverified["detail"]["telegram"]["session"]["authorized"] is None
        assert unverified["summary"] == "Telegram sync · sign-in not verified"
        user = {"id": 424242, "username": "alice_example", "first_name": "Alice"}
        signed = step(facts(
            personal, on, telegram_session_present=True, state=SetupState(telegram_sync_user=user),
        ), "sources")
        assert signed["detail"]["telegram"]["session"] == {"present": True, "authorized": True, "user": user}
        assert signed["summary"] == "Telegram sync · signed in as @alice_example"
        assert signed["etag"] == absent["etag"], "the session is volatile: never remounts the form"
        no_handle = step(facts(
            personal, on, telegram_session_present=True,
            state=SetupState(telegram_sync_user={"id": 7, "username": None, "first_name": "Alice"}),
        ), "sources")
        assert no_handle["summary"] == "Telegram sync · signed in as Alice"
        # A stale note without the file means nothing: the file is the session.
        stale = step(facts(personal, on, state=SetupState(telegram_sync_user=user)), "sources")
        assert stale["detail"]["telegram"]["session"]["authorized"] is False
        with_github = cfg(personal, sync={"github": {"enabled": True}, "telegram": {"enabled": True, "api_id": 1, "api_hash": "x"}})
        assert step(facts(personal, with_github), "sources")["summary"] == "GitHub, Telegram sync · not signed in"

    def test_mask_rules(self):
        assert checklist.mask("") == {"configured": False, "last4": None, "source": None}
        assert checklist.mask(None) == {"configured": False, "last4": None, "source": None}
        assert checklist.mask("short") == {"configured": True, "last4": None, "source": "file"}
        assert checklist.mask("sk-ant-REPLACE0000ab12") == {"configured": True, "last4": "ab12", "source": "file"}
        assert checklist.mask(1234567) == {"configured": True, "last4": None, "source": "file"}
        assert checklist.mask(0) == {"configured": False, "last4": None, "source": None}
        # Where the value came from rides along; nothing for an absent value.
        assert checklist.mask("sk-ant-REPLACE0000ab12", "environment")["source"] == "environment"
        assert checklist.mask("", "environment")["source"] is None
        assert checklist.managed("gateway") == {"configured": True, "last4": None, "source": "gateway"}

    def test_etag_stable_across_test_pairing_onboarding_changes(self, personal):
        config = cfg(personal, anthropic_api_key="sk-ant-REPLACE0000000000")
        a = step(facts(personal, config), "provider")["etag"]
        b = step(facts(personal, config, provider_last_test={"ok": True, "checked_at": "now"}), "provider")["etag"]
        assert a == b
        failed = {"ok": False, "code": "invalid_key", "detail": "Anthropic rejected this key (401).", "checked_at": "now"}
        c = step(facts(personal, config, provider_last_test=failed), "provider")
        assert c["etag"] == a, "a remembered failure is volatile too"
        assert c["detail"]["last_test"] == failed
        other_model = cfg(
            personal, anthropic_api_key="sk-ant-REPLACE0000000000",
            agent={"model": "claude-sonnet-4-6"},
        )
        assert step(facts(personal, other_model), "provider")["etag"] != a

        tg = cfg(personal, telegram={"bot_token": "123456:REPLACEabcdefghijklmnopqrstuvwxyz"})
        c1 = step(facts(personal, tg), "channels")["etag"]
        c2 = step(facts(personal, tg, telegram_live=True, bot_username="x", telegram_start_error="boom"), "channels")["etag"]
        assert c1 == c2
        m1 = step(facts(personal, onboarding="pending"), "mode")["etag"]
        m2 = step(facts(personal, onboarding="running", onboarding_session_id="s"), "mode")["etag"]
        assert m1 == m2
        assert len(a) == 12

    def test_files_for(self):
        assert checklist.files_for("claim", "personal") == []
        assert "workspace/TASK.md" in checklist.files_for("mode", "worker")
        assert "workspace/USER.md" in checklist.files_for("profile", "personal")
        assert "workspace/USER.md" not in checklist.files_for("profile", "worker")
        assert checklist.files_for("automation", None) == ["workspace/config/cron/system.yaml"]

    def test_no_secret_in_any_detail(self, personal):
        secrets = {
            "anthropic_api_key": "sk-ant-REPLACE0000000000",
            "openai_api_key": "sk-REPLACE0000000000",
            "claude_oauth_token": "sk-ant-oat01-REPLACE000",
        }
        config = cfg(
            personal, **secrets,
            telegram={"bot_token": "123456:REPLACEabcdefghijklmnopqrstuvwxyz"},
            slack={"bot_token": "xoxb-REPLACE000000", "app_token": "xapp-REPLACE000000"},
            sync={"telegram": {"api_id": 1234567, "api_hash": "REPLACE_HASH_VALUE"}},
        )
        text = str(checklist.summarize(facts(personal, config)))
        for value in (*secrets.values(), "123456:REPLACE", "xoxb-REPLACE000000", "xapp-REPLACE000000", "REPLACE_HASH_VALUE", "1234567"):
            assert value not in text
