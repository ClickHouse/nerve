"""``notifications.levels``: which items a channel gets, and the fan-out
that skips a channel for an item below its level. The panel is always a
channel, whatever ``notifications.channels`` lists."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.config import NerveConfig, NotificationsConfig
from nerve.notifications.service import NotificationService
from tests.actor_rows import ensure_system_principal


class TestLevels:
    def test_an_unknown_level_reads_as_all(self):
        config = NotificationsConfig.from_dict({"levels": {"slack": "Important", "telegram": "loud"}})
        assert config.levels == {"slack": "important", "telegram": "all"}
        assert NotificationsConfig.from_dict({"levels": "important"}).levels == {}

    @pytest.mark.parametrize("notif_type, priority, wanted", [
        ("notify", "normal", False),
        ("notify", "low", False),
        ("notify", "high", True),
        ("notify", "urgent", True),
        ("question", "normal", True),
        ("approval", "normal", True),
    ])
    def test_an_important_channel_gets_questions_approvals_and_high_items(self, notif_type, priority, wanted):
        config = NotificationsConfig(levels={"telegram": "important"})
        assert config.wants("telegram", notif_type, priority) is wanted
        assert config.wants("slack", notif_type, priority) is True
        assert config.wants("web", notif_type, priority) is True


@pytest.mark.asyncio
async def test_the_fanout_skips_a_channel_below_its_level(db):
    await ensure_system_principal(db)
    config = NerveConfig()
    config.notifications = NotificationsConfig(
        channels=["web", "telegram", "slack"], levels={"telegram": "important", "web": "important"},
    )
    service = NotificationService(config, db, MagicMock())
    service._deliver_web = AsyncMock()
    service._deliver_telegram = AsyncMock(return_value=7)
    service._deliver_slack = AsyncMock(return_value="ts-1")
    await db.create_session("s1", actor=None)

    await db.create_notification(notification_id="n1", session_id="s1", type="notify", title="t")
    await service._fanout("n1", "s1", "notify", "t", "", "normal")
    assert json.loads((await db.get_notification("n1"))["channels_delivered"]) == ["web", "slack"]
    service._deliver_telegram.assert_not_awaited()

    await db.create_notification(notification_id="n2", session_id="s1", type="question", title="q")
    await service._fanout("n2", "s1", "question", "q", "", "normal", options=["Yes", "No"])
    assert json.loads((await db.get_notification("n2"))["channels_delivered"]) == ["web", "telegram", "slack"]


@pytest.mark.parametrize("raw, channels", [
    (["telegram"], ["web", "telegram"]),
    ([], ["web"]),
    (None, ["web"]),
    ("slack", ["web", "slack"]),
    (["slack", "web"], ["slack", "web"]),
])
def test_the_panel_is_always_in_the_channel_list(raw, channels):
    assert NotificationsConfig.from_dict({"channels": raw}).channels == channels


@pytest.mark.asyncio
async def test_an_install_that_lists_only_telegram_still_delivers_to_the_panel(db):
    """A settings.yaml written before the panel was always on."""
    await ensure_system_principal(db)
    config = NerveConfig()
    config.notifications = NotificationsConfig.from_dict({"channels": ["telegram"]})
    service = NotificationService(config, db, MagicMock())
    service._deliver_web = AsyncMock()
    service._deliver_telegram = AsyncMock(return_value=7)
    await db.create_session("s1", actor=None)
    await db.create_notification(notification_id="n1", session_id="s1", type="notify", title="t")

    await service._fanout("n1", "s1", "notify", "t", "", "normal")

    service._deliver_web.assert_awaited_once()
    assert json.loads((await db.get_notification("n1"))["channels_delivered"]) == ["web", "telegram"]
