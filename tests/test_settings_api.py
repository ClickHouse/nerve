"""The Settings API over HTTP: field states, where a value is written,
refusals, the configuration check, reload, the test notification, and the
notification levels in the fan-out.

The instance is the slim install of ``tests/test_setup_api.py``, driven
through ``httpx`` like the browser does. No test sends to a real channel.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from nerve.config import NerveConfig, get_config
from nerve.config_validate import ValidationResult
from nerve.gateway.routes import _deps as deps_module
from nerve.gateway.routes import settings as settings_routes
from nerve.notifications.service import NotificationService
from nerve.setup_writer import merge_machine_paths, merge_settings_paths
from tests.test_setup_api import _fresh_module_state, _lock, _make_install, claim  # noqa: F401

URL = "/api/settings/notifications"
LEVEL_SLACK = "notifications.levels.slack"
EXPIRY = "notifications.default_expiry_hours"


@pytest_asyncio.fixture
async def inst(tmp_path, monkeypatch):
    install = await _make_install(tmp_path, monkeypatch)
    install.app.include_router(settings_routes.router)
    await claim(install)
    try:
        yield install
    finally:
        await install.db.close()


async def _get(inst) -> dict:
    async with inst.client() as http:
        response = await http.get(URL)
    assert response.status_code == 200, response.text
    return response.json()


async def _put(inst, body: dict):
    async with inst.client() as http:
        return await http.put(URL, json=body)


@pytest.mark.asyncio
class TestStates:
    async def test_every_field_is_editable_in_settings_yaml_on_a_fresh_install(self, inst):
        body = await _get(inst)
        assert body["read_only_reason"] is None
        assert body["fields"][LEVEL_SLACK] == {
            "value": "all", "state": "editable", "file": "settings.yaml", "reason": None,
        }
        assert body["fields"]["notifications.channels"]["value"] == ["web", "telegram", "slack"]
        assert body["fields"]["notifications.slack_channel_id"]["value"] is None
        assert body["slack"] == {"connected": False, "channel_name": None}
        assert body["telegram"] == {"connected": False}

    async def test_a_channel_list_without_the_panel_shows_it(self, inst):
        merge_settings_paths(inst.workspace, {"notifications.channels": ["telegram"]})
        inst.reload()
        assert (await _get(inst))["fields"]["notifications.channels"]["value"] == ["web", "telegram"]

    async def test_a_value_in_config_yaml_is_machine_and_is_written_there(self, inst):
        merge_machine_paths(inst.config_dir / "config.yaml", {EXPIRY: 24})
        inst.reload()
        field = (await _get(inst))["fields"][EXPIRY]
        assert field == {"value": 24, "state": "machine", "file": "config.yaml", "reason": None}

        response = await _put(inst, {EXPIRY: 12})
        assert response.status_code == 200, response.text
        assert inst.machine_yaml()["notifications"]["default_expiry_hours"] == 12
        assert "notifications" not in inst.settings_yaml()
        assert get_config().notifications.default_expiry_hours == 12

    async def test_an_environment_reference_is_managed_and_refused(self, inst, monkeypatch):
        monkeypatch.setenv("NOTIFY_SLACK_LEVEL", "important")
        merge_settings_paths(inst.workspace, {LEVEL_SLACK: "${NOTIFY_SLACK_LEVEL}"})
        inst.reload()
        field = (await _get(inst))["fields"][LEVEL_SLACK]
        assert field["state"] == "managed" and field["value"] == "important"
        assert "environment" in field["reason"]

        response = await _put(inst, {LEVEL_SLACK: "all"})
        assert response.status_code == 409
        assert response.json()["field"] == LEVEL_SLACK
        assert inst.settings_yaml()["notifications"]["levels"]["slack"] == "${NOTIFY_SLACK_LEVEL}"

    async def test_a_hosted_slack_destination_is_set_by_the_control_plane(self, inst):
        merge_settings_paths(inst.workspace, {"slack.mode": "hosted"})
        inst.reload()
        body = await _get(inst)
        field = body["fields"]["notifications.slack_channel_id"]
        assert field["state"] == "managed"
        assert field["reason"] == settings_routes.CONTROL_PLANE_TEXT
        assert body["slack"]["connected"] is True
        # The other Slack fields stay this page's.
        assert body["fields"][LEVEL_SLACK]["state"] == "editable"

        response = await _put(inst, {"notifications.slack_channel_id": "C0123ABCD"})
        assert response.status_code == 409

    async def test_lockdown_makes_every_field_read_only_and_refuses_a_write(self, inst, monkeypatch):
        _lock(inst, monkeypatch)
        body = await _get(inst)
        assert body["read_only_reason"].startswith("Configuration is managed")
        assert {f["state"] for f in body["fields"].values()} == {"read_only"}

        response = await _put(inst, {LEVEL_SLACK: "important"})
        assert response.status_code == 409
        assert "notifications" not in inst.settings_yaml()

    async def test_a_system_token_is_refused(self, inst):
        async with inst.client(headers=inst.system_headers()) as http:
            assert (await http.get(URL)).status_code == 403
            assert (await http.put(URL, json={})).status_code == 403


    async def test_the_slack_destination_comes_with_its_channel_name(self, inst, monkeypatch):
        merge_settings_paths(inst.workspace, {"notifications.slack_channel_id": "C0123ABCD"})
        inst.reload()
        slack = MagicMock(is_available=True)
        slack.conversation_name = AsyncMock(return_value="general")
        engine = MagicMock()
        engine.channel.side_effect = lambda name: slack if name == "slack" else None
        monkeypatch.setattr(deps_module, "_deps", deps_module.RouteDeps(engine=engine, db=inst.db))
        body = await _get(inst)
        assert body["fields"]["notifications.slack_channel_id"]["value"] == "C0123ABCD"
        assert body["slack"]["channel_name"] == "general"
        slack.conversation_name.assert_awaited_once_with("C0123ABCD")


@pytest.mark.asyncio
async def test_a_slack_channel_names_a_conversation():
    from nerve.channels.slack import SlackChannel
    from nerve.config import SlackConfig

    config = NerveConfig()
    config.slack = SlackConfig(enabled=True, bot_token="xoxb-test", app_token="xapp-test")
    channel = SlackChannel(config, router=MagicMock())
    channel._web = MagicMock()
    channel._web.conversations_info = AsyncMock(return_value={"channel": {"name": "general"}})
    assert await channel.conversation_name("C0123ABCD") == "general"
    channel._web.conversations_info = AsyncMock(side_effect=RuntimeError("no access"))
    assert await channel.conversation_name("C0456DEFG") is None

@pytest.mark.asyncio
class TestSave:
    async def test_a_new_value_goes_to_settings_yaml_and_is_live(self, inst):
        response = await _put(inst, {LEVEL_SLACK: "important"})
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["fields"][LEVEL_SLACK]["value"] == "important"
        assert body["reload_errors"] == {}
        assert inst.settings_yaml()["notifications"]["levels"] == {"slack": "important"}
        assert get_config().notifications.levels == {"slack": "important"}

    async def test_switching_a_channel_off_writes_the_list(self, inst):
        response = await _put(inst, {"notifications.channels": ["web", "telegram"]})
        assert response.status_code == 200, response.text
        assert inst.settings_yaml()["notifications"]["channels"] == ["web", "telegram"]

    async def test_clearing_the_telegram_chat_removes_the_key(self, inst):
        assert (await _put(inst, {"notifications.telegram_chat_id": "-1000000000001"})).status_code == 200
        assert inst.settings_yaml()["notifications"]["telegram_chat_id"] == -1000000000001
        assert (await _put(inst, {"notifications.telegram_chat_id": ""})).status_code == 200
        assert "telegram_chat_id" not in inst.settings_yaml().get("notifications", {})

    @pytest.mark.parametrize("path, value", [
        ("notifications.channels", ["telegram"]),
        ("notifications.channels", ["web", "email"]),
        (LEVEL_SLACK, "some"),
        ("notifications.slack_channel_id", "general"),
        ("notifications.telegram_chat_id", "abc"),
        (EXPIRY, 0),
        (EXPIRY, "48"),
        ("notifications.max_redeliveries", 11),
        ("notifications.unknown", 1),
    ])
    async def test_a_bad_value_is_refused_with_its_field_and_nothing_is_written(self, inst, path, value):
        response = await _put(inst, {path: value})
        assert response.status_code == 400
        assert response.json()["field"] == path
        assert "notifications" not in inst.settings_yaml()

    async def test_a_configuration_the_validator_refuses_is_not_written(self, inst, monkeypatch):
        def fake_validate(config_dir, workspace_override=None, **_):
            text = (workspace_override / "config" / "settings.yaml").read_text()
            errors = ["an old problem"] + (["levels are not allowed here"] if "important" in text else [])
            return ValidationResult(errors=errors)

        monkeypatch.setattr(settings_routes, "validate_config_bundle", fake_validate)
        response = await _put(inst, {LEVEL_SLACK: "important"})
        assert response.status_code == 400
        assert response.json() == {
            "detail": "With this value the configuration is not valid: levels are not allowed here",
            "field": LEVEL_SLACK,
        }
        assert "notifications" not in inst.settings_yaml()
        # An error the configuration has already does not block a change.
        assert (await _put(inst, {LEVEL_SLACK: "all"})).status_code == 200

    async def test_the_response_names_a_part_that_did_not_reload(self, inst, monkeypatch):
        monkeypatch.setattr(settings_routes, "_reload", AsyncMock(return_value={
            "config": "reloaded", "cron": "error: the cron file is not valid",
        }))
        response = await _put(inst, {LEVEL_SLACK: "important"})
        assert response.status_code == 200
        assert response.json()["reload_errors"] == {"cron": "the cron file is not valid"}


@pytest.mark.asyncio
class TestSendTest:
    async def test_each_listed_channel_says_whether_it_took_the_test(self, inst, monkeypatch):
        engine = MagicMock()
        service = NotificationService(get_config(), inst.db, engine)
        monkeypatch.setattr(service, "_deliver_web", AsyncMock())
        monkeypatch.setattr(service, "_deliver_telegram", AsyncMock(return_value=101))
        monkeypatch.setattr(service, "_deliver_slack", AsyncMock(return_value=None))
        monkeypatch.setattr(deps_module, "_deps", deps_module.RouteDeps(
            engine=engine, db=inst.db, notification_service=service,
        ))
        async with inst.client() as http:
            response = await http.post(f"{URL}/test")
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["silenced"] is False
        assert body["results"] == [
            {"channel": "web", "delivered": True},
            {"channel": "telegram", "delivered": True},
            {"channel": "slack", "delivered": False},
        ]
        row = await inst.db.get_notification(body["notification_id"])
        assert row["title"] == settings_routes.TEST_TITLE and row["session_id"] == "system"

    async def test_without_a_notification_service_the_test_is_refused(self, inst):
        async with inst.client() as http:
            response = await http.post(f"{URL}/test")
        assert response.status_code == 503


@pytest.mark.asyncio
class TestQuietHours:
    async def test_the_section_has_both_times_and_the_time_zone(self, inst, monkeypatch):
        monkeypatch.setattr(settings_routes, "in_quiet_hours", lambda config: True)
        body = await _get(inst)
        for path in ("quiet_start", "quiet_end"):
            assert body["fields"][path]["state"] == "editable"
            assert isinstance(body["fields"][path]["value"], str)
        assert body["quiet_hours"] == {"timezone": get_config().timezone, "active": True}

    async def test_two_times_go_to_settings_yaml_and_are_live(self, inst):
        response = await _put(inst, {"quiet_start": "22:00", "quiet_end": "07:00"})
        assert response.status_code == 200, response.text
        fields = response.json()["fields"]
        assert (fields["quiet_start"]["value"], fields["quiet_end"]["value"]) == ("22:00", "07:00")
        assert (inst.settings_yaml()["quiet_start"], inst.settings_yaml()["quiet_end"]) == ("22:00", "07:00")
        assert (get_config().quiet_start, get_config().quiet_end) == ("22:00", "07:00")

        assert (await _put(inst, {"quiet_end": "06:30"})).status_code == 200
        assert get_config().quiet_end == "06:30"

    async def test_off_writes_both_times_empty_so_no_default_comes_back(self, inst):
        response = await _put(inst, {"quiet_start": "", "quiet_end": ""})
        assert response.status_code == 200, response.text
        assert (inst.settings_yaml()["quiet_start"], inst.settings_yaml()["quiet_end"]) == ("", "")
        assert (get_config().quiet_start, get_config().quiet_end) == ("", "")
        assert response.json()["quiet_hours"]["active"] is False

    async def test_a_time_in_config_yaml_is_written_there(self, inst):
        merge_machine_paths(inst.config_dir / "config.yaml", {"quiet_start": "23:00", "quiet_end": "06:00"})
        inst.reload()
        field = (await _get(inst))["fields"]["quiet_start"]
        assert (field["state"], field["file"]) == ("machine", "config.yaml")
        assert (await _put(inst, {"quiet_start": "21:00"})).status_code == 200
        assert inst.machine_yaml()["quiet_start"] == "21:00"
        assert "quiet_start" not in inst.settings_yaml()

    @pytest.mark.parametrize("body, field", [
        ({"quiet_start": "24:00"}, "quiet_start"),
        ({"quiet_start": "25:00"}, "quiet_start"),
        ({"quiet_end": "08:60"}, "quiet_end"),
        ({"quiet_end": "7am"}, "quiet_end"),
        ({"quiet_start": 2200}, "quiet_start"),
        ({"quiet_start": None}, "quiet_start"),
        ({"quiet_start": "22:00", "quiet_end": ""}, "quiet_end"),
        ({"quiet_start": "", "quiet_end": "07:00"}, "quiet_start"),
        ({"quiet_start": "22:00", "quiet_end": "22:00"}, "quiet_end"),
    ])
    async def test_a_bad_time_or_half_a_pair_is_refused(self, inst, body, field):
        before = inst.settings_yaml()
        response = await _put(inst, body)
        assert response.status_code == 400
        assert response.json()["field"] == field
        assert inst.settings_yaml() == before

    async def test_one_time_alone_must_not_switch_quiet_hours_half_off(self, inst):
        assert (await _put(inst, {"quiet_start": "22:00", "quiet_end": "07:00"})).status_code == 200
        response = await _put(inst, {"quiet_end": ""})
        assert response.status_code == 400
        assert response.json()["field"] == "quiet_end"
        assert get_config().quiet_end == "07:00"
