"""Quiet hours: the window in the configured time zone, and the fan-out
that sends each item below ``urgent`` without sound inside it.

No test sends to a real channel: Telegram is a mock, and the web event goes
to a fake broadcaster.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
import yaml

from nerve.config import NerveConfig, NotificationsConfig
from nerve.notifications import quiet_hours
from nerve.notifications.quiet_hours import deliver_silently, in_quiet_hours, minutes_of_day
from nerve.notifications.service import NotificationService
from tests.actor_rows import ensure_system_principal


def _at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 11, hour, minute, tzinfo=UTC)


def _clock(fixed: datetime) -> type[datetime]:
    """A ``datetime`` whose ``now()`` is ``fixed``."""
    class _Fixed(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz)

    return _Fixed


def _config(start: str | None, end: str | None, timezone: str = "UTC") -> NerveConfig:
    data: dict = {"timezone": timezone}
    if start is not None:
        data["quiet_start"] = start
    if end is not None:
        data["quiet_end"] = end
    return NerveConfig.from_dict(data)


class TestInQuietHours:
    @pytest.mark.parametrize(("start", "end"), [("", ""), ("02:00", ""), ("", "08:00"), ("  ", "08:00")])
    def test_empty_values_mean_no_quiet_hours(self, start, end):
        assert in_quiet_hours(_config(start, end), _at(3)) is False

    def test_a_window_applies_and_may_cross_midnight(self):
        assert in_quiet_hours(_config("02:00", "08:00"), _at(3)) is True
        assert in_quiet_hours(_config(None, None), _at(3)) is True, "a key no layer states: 02:00 to 08:00"
        assert in_quiet_hours(_config("22:00", "07:00"), _at(23, 30)) is True
        assert in_quiet_hours(_config("22:00", "07:00"), _at(12)) is False
        assert in_quiet_hours(_config("02:00", "08:00"), _at(12)) is False

    def test_the_start_is_inside_and_the_end_is_outside(self):
        assert in_quiet_hours(_config("22:00", "07:00"), _at(22)) is True
        assert in_quiet_hours(_config("22:00", "07:00"), _at(7)) is False

    def test_the_times_are_in_the_configured_time_zone(self):
        # 21:30 UTC is 06:30 in Tokyo (UTC+9), inside 22:00 to 07:00 there.
        assert in_quiet_hours(_config("22:00", "07:00", "Asia/Tokyo"), _at(21, 30)) is True
        assert in_quiet_hours(_config("22:00", "07:00", "UTC"), _at(21, 30)) is False

    @pytest.mark.parametrize(("start", "end", "timezone"), [
        ("2am", "8am", "UTC"), ("02:00:00", "08:00", "UTC"), ("02:00", "08:00", "Not/AZone"),
        ("24:00", "08:00", "UTC"), ("25:00", "08:00", "UTC"), ("02:00", "08:60", "UTC"), ("2:00", "08:00", "UTC"),
    ])
    def test_an_unreadable_value_is_not_quiet(self, start, end, timezone):
        assert in_quiet_hours(_config(start, end, timezone), _at(3)) is False

    def test_an_unquoted_yaml_time_is_the_time_written(self):
        data = yaml.safe_load("timezone: UTC\nquiet_start: 22:00\nquiet_end: 9:30\n")
        assert (data["quiet_start"], data["quiet_end"]) == (1320, 570)
        config = NerveConfig.from_dict(data)
        assert (config.quiet_start, config.quiet_end) == ("22:00", "09:30")
        assert in_quiet_hours(config, _at(23)) is True
        assert in_quiet_hours(config, _at(9, 30)) is False


@pytest.mark.parametrize(("text", "minutes"), [("00:00", 0), ("07:30", 450), ("23:59", 1439)])
def test_a_24_hour_time_gives_its_minutes(text, minutes):
    assert minutes_of_day(text) == minutes


@pytest.mark.parametrize("text", ["24:00", "25:00", "08:60", "8:00", "08:00:00", "", " 08:00", "٠٨:٠٠"])
def test_any_other_text_is_refused(text):
    with pytest.raises(ValueError):
        minutes_of_day(text)

    def test_without_a_time_it_reads_the_clock(self, monkeypatch):
        monkeypatch.setattr(quiet_hours, "datetime", _clock(_at(3)))
        assert in_quiet_hours(_config("02:00", "08:00")) is True


@pytest.mark.parametrize(("priority", "silent"), [
    ("low", True), ("normal", True), ("high", True), ("urgent", False),
])
def test_only_urgent_makes_a_sound_in_quiet_hours(priority, silent):
    assert deliver_silently(_config("02:00", "08:00"), priority, _at(3)) is silent
    assert deliver_silently(_config("02:00", "08:00"), priority, _at(12)) is False


@pytest.fixture
def broadcasts(monkeypatch) -> list[dict]:
    captured: list[dict] = []

    class _FakeBroadcaster:
        async def broadcast(self, _channel: str, message: dict) -> None:
            captured.append(message)

    from nerve.agent import streaming
    monkeypatch.setattr(streaming, "broadcaster", _FakeBroadcaster())
    return captured


@pytest_asyncio.fixture
async def service(db) -> NotificationService:
    await ensure_system_principal(db)
    await db.create_session("s1", actor=None)
    config = _config("22:00", "07:00")
    config.notifications = NotificationsConfig(channels=["web", "telegram"], telegram_chat_id=1001)
    svc = NotificationService(config, db, MagicMock())
    svc._deliver_telegram = AsyncMock(return_value="7")
    return svc


def _quiet(monkeypatch, quiet: bool) -> None:
    monkeypatch.setattr(quiet_hours, "in_quiet_hours", lambda config, now=None: quiet)


@pytest.mark.asyncio
class TestFanout:
    @pytest.mark.parametrize("priority", ["normal", "high"])
    async def test_inside_quiet_hours_an_item_arrives_without_sound(
        self, service, broadcasts, monkeypatch, priority,
    ):
        _quiet(monkeypatch, True)
        await service.send_notification("s1", "Build finished", priority=priority)
        assert service._deliver_telegram.await_args.kwargs["silent"] is True
        assert broadcasts[0]["silent"] is True
        (row,) = await service.db.list_notifications()
        assert row["status"] == "pending"

    async def test_an_urgent_item_still_makes_a_sound(self, service, broadcasts, monkeypatch):
        _quiet(monkeypatch, True)
        await service.send_notification("s1", "Disk full", priority="urgent")
        assert service._deliver_telegram.await_args.kwargs["silent"] is False
        assert "silent" not in broadcasts[0]

    @pytest.mark.parametrize(("start", "end"), [("24:00", "07:00"), ("25:00", "07:00"), ("22:00", "08:60")])
    async def test_an_out_of_range_time_keeps_the_sound(self, service, broadcasts, monkeypatch, start, end):
        service.config.quiet_start, service.config.quiet_end = start, end
        monkeypatch.setattr(quiet_hours, "datetime", _clock(_at(3)))
        await service.send_notification("s1", "Build finished")
        assert service._deliver_telegram.await_args.kwargs["silent"] is False
        assert "silent" not in broadcasts[0]

    async def test_outside_quiet_hours_nothing_changes(self, service, broadcasts, monkeypatch):
        _quiet(monkeypatch, False)
        await service.send_notification("s1", "Build finished")
        assert service._deliver_telegram.await_args.kwargs["silent"] is False
        assert "silent" not in broadcasts[0]

    async def test_a_question_is_silent_too(self, service, broadcasts, monkeypatch):
        _quiet(monkeypatch, True)
        await service.ask_question("s1", "Merge it?", options=["Yes", "No"])
        assert service._deliver_telegram.await_args.kwargs["silent"] is True
        assert broadcasts[0]["silent"] is True

    async def test_a_re_delivery_reads_the_rule_again(self, service, broadcasts, monkeypatch):
        _quiet(monkeypatch, True)
        await service.db.create_notification(
            notification_id="ask-1", session_id="s1", type="question", title="Merge it?",
            options=["Yes", "No"],
        )
        await service._redeliver_one(await service.db.get_notification("ask-1"))
        assert service._deliver_telegram.await_args.kwargs["silent"] is True
        assert broadcasts[0]["silent"] is True and broadcasts[0]["redelivered"] is True
