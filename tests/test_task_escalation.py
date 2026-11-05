"""Deadline escalation and quiet hours.

Quiet hours are optional: ``quiet_start`` and ``quiet_end`` both empty (a
worker's default) means there are none, so the escalation check never pauses.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from nerve.config import NerveConfig
from nerve.tasks import escalation


def _at(monkeypatch, hour: int, minute: int = 0) -> None:
    """Pin ``datetime.now`` inside the escalation module to a UTC wall time."""
    fixed = datetime(2026, 10, 11, hour, minute, tzinfo=UTC)

    class _Fixed(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz) if tz is not None else fixed

    monkeypatch.setattr(escalation, "datetime", _Fixed)


def _config(start: str | None, end: str | None) -> NerveConfig:
    data: dict = {"timezone": "UTC"}
    if start is not None:
        data["quiet_start"] = start
    if end is not None:
        data["quiet_end"] = end
    return NerveConfig.from_dict(data)


class TestIsQuietHour:
    @pytest.mark.parametrize(("start", "end"), [("", ""), ("02:00", ""), ("", "08:00"), ("  ", "08:00")])
    def test_empty_values_mean_no_quiet_hours(self, monkeypatch, start, end):
        _at(monkeypatch, 3)
        assert escalation._is_quiet_hour(_config(start, end)) is False

    def test_a_window_applies_and_may_cross_midnight(self, monkeypatch):
        _at(monkeypatch, 3)
        assert escalation._is_quiet_hour(_config("02:00", "08:00")) is True
        assert escalation._is_quiet_hour(_config(None, None)) is True, "a key no layer states: 02:00–08:00"
        _at(monkeypatch, 23, 30)
        assert escalation._is_quiet_hour(_config("22:00", "07:00")) is True
        _at(monkeypatch, 12)
        assert escalation._is_quiet_hour(_config("22:00", "07:00")) is False
        assert escalation._is_quiet_hour(_config("02:00", "08:00")) is False

    def test_an_unreadable_value_is_not_quiet(self, monkeypatch):
        _at(monkeypatch, 3)
        assert escalation._is_quiet_hour(_config("2am", "8am")) is False


class _Tasks:
    def __init__(self, rows: list[dict]):
        self.rows = rows
        self.calls = 0

    async def list_tasks(self, status: str):
        self.calls += 1
        return [row for row in self.rows if row.get("status", "pending") == status]


@pytest.mark.asyncio
class TestCheckEscalations:
    async def test_without_quiet_hours_a_due_task_escalates_at_any_hour(self, monkeypatch):
        _at(monkeypatch, 3)
        db = _Tasks([{"id": "t1", "title": "Renew the certificate", "deadline": "2026-10-11T01:00:00+00:00"}])
        due = await escalation.check_escalations(db, _config("", ""))
        assert db.calls == 1
        assert [(d["task_id"], d["level"]) for d in due] == [("t1", 1)]

    async def test_inside_quiet_hours_nothing_is_checked(self, monkeypatch):
        _at(monkeypatch, 3)
        db = _Tasks([{"id": "t1", "title": "Renew the certificate", "deadline": "2026-10-11T01:00:00+00:00"}])
        assert await escalation.check_escalations(db, _config("02:00", "08:00")) == []
        assert db.calls == 0
