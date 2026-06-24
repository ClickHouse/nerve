"""Tests for ISO date placeholder rendering in notification text.

The rendering layer fixes a recurring failure mode in cron-driven
notifications: the model would write "24 июня (вт)" when 24 June 2026 is
actually a Wednesday. We can't fix the arithmetic by asking nicely, so
the model now declares intent via ``<YYYY-MM-DD>`` placeholders and
``datetime.weekday()`` does the math.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from nerve.notifications.date_render import render_iso_dates


# Fixed reference: 2026-06-24 was a Wednesday. The whole suite anchors
# its "today/tomorrow/yesterday" assertions to this date so changes in
# wall-clock time can't drift the expectations.
_NOW = datetime(2026, 6, 24, 10, 0)


class TestBasicRendering:
    def test_plain_date_renders_with_weekday(self):
        assert render_iso_dates("<2026-06-24>", now=_NOW) == "сегодня, 24 июня (ср)"

    def test_date_with_time(self):
        assert render_iso_dates("<2026-06-24 19:00>", now=_NOW) == "сегодня, 24 июня (ср), 19:00"

    def test_iso_t_separator_also_accepted(self):
        # The model occasionally writes the canonical ISO 8601 T separator —
        # accept it so we don't punish well-formed input.
        assert render_iso_dates("<2026-06-24T19:00>", now=_NOW) == "сегодня, 24 июня (ср), 19:00"

    def test_far_future_date_no_relative_label(self):
        # Far enough that no "сегодня/завтра/вчера" applies.
        assert render_iso_dates("<2027-01-15>", now=_NOW) == "15 января (пт)"

    def test_date_inside_sentence(self):
        text = "Прибывает <2026-06-24> в Packstation 536"
        expected = "Прибывает сегодня, 24 июня (ср) в Packstation 536"
        assert render_iso_dates(text, now=_NOW) == expected


class TestRelativeLabels:
    def test_today(self):
        # 24 июня 2026 == _NOW date
        assert "сегодня" in render_iso_dates("<2026-06-24>", now=_NOW)

    def test_tomorrow(self):
        assert render_iso_dates("<2026-06-25>", now=_NOW) == "завтра, 25 июня (чт)"

    def test_yesterday(self):
        assert render_iso_dates("<2026-06-23>", now=_NOW) == "вчера, 23 июня (вт)"

    def test_two_days_ahead_no_label(self):
        # +2 days should NOT be labelled "послезавтра" — it's relative-arithmetic
        # territory we deliberately avoid. Absolute date only.
        result = render_iso_dates("<2026-06-26>", now=_NOW)
        assert result == "26 июня (пт)"
        assert "послезавтра" not in result

    def test_two_days_back_no_label(self):
        result = render_iso_dates("<2026-06-22>", now=_NOW)
        assert result == "22 июня (пн)"
        assert "позавчера" not in result


class TestWeekdayArithmetic:
    """Anchor the weekday math against known references.

    These tests would have caught the original bug: 24 June 2026 is a
    Wednesday (ср), NOT a Tuesday (вт). The model got this wrong; the
    renderer must not.
    """

    @pytest.mark.parametrize("iso,expected_weekday", [
        ("2026-06-22", "пн"),
        ("2026-06-23", "вт"),
        ("2026-06-24", "ср"),
        ("2026-06-25", "чт"),
        ("2026-06-26", "пт"),
        ("2026-06-27", "сб"),
        ("2026-06-28", "вс"),
        # Different month, different year — same arithmetic.
        ("2027-12-31", "пт"),
        # Leap day in a real leap year — sanity check.
        ("2028-02-29", "вт"),
    ])
    def test_known_weekdays(self, iso, expected_weekday):
        result = render_iso_dates(f"<{iso}>", now=_NOW)
        assert f"({expected_weekday})" in result, f"{iso}: expected ({expected_weekday}) in {result!r}"


class TestMonthNames:
    @pytest.mark.parametrize("month_num,month_name", [
        (1, "января"), (2, "февраля"), (3, "марта"), (4, "апреля"),
        (5, "мая"), (6, "июня"), (7, "июля"), (8, "августа"),
        (9, "сентября"), (10, "октября"), (11, "ноября"), (12, "декабря"),
    ])
    def test_all_months_genitive(self, month_num, month_name):
        result = render_iso_dates(f"<2027-{month_num:02d}-15>", now=_NOW)
        assert month_name in result


class TestEdgeCases:
    def test_empty_string(self):
        assert render_iso_dates("", now=_NOW) == ""

    def test_text_without_placeholders_unchanged(self):
        text = "Просто текст без дат. (вт) тоже не трогаем."
        assert render_iso_dates(text, now=_NOW) == text

    def test_invalid_date_kept_as_placeholder(self):
        # 31 February doesn't exist — render must leave the placeholder
        # visible so the model can see its own bad output rather than
        # silently shipping something half-broken.
        text = "Срок: <2026-02-31>"
        assert render_iso_dates(text, now=_NOW) == text

    def test_invalid_hour_kept_as_placeholder(self):
        text = "Время: <2026-06-24 25:00>"
        assert render_iso_dates(text, now=_NOW) == text

    def test_invalid_minute_kept_as_placeholder(self):
        text = "Время: <2026-06-24 19:99>"
        assert render_iso_dates(text, now=_NOW) == text

    def test_partial_placeholder_not_matched(self):
        # Missing closing bracket — leave alone.
        text = "Дата: <2026-06-24"
        assert render_iso_dates(text, now=_NOW) == text

    def test_html_like_tags_not_matched(self):
        # The regex requires digits, so HTML tags pass through untouched.
        text = "<div>Привет</div>"
        assert render_iso_dates(text, now=_NOW) == text

    def test_multiple_placeholders_in_one_string(self):
        text = "С <2026-06-24> по <2026-06-26>"
        result = render_iso_dates(text, now=_NOW)
        assert "сегодня, 24 июня (ср)" in result
        assert "26 июня (пт)" in result

    def test_none_text_returns_falsy(self):
        # The service layer may pass empty title — make sure we don't crash.
        assert render_iso_dates("", now=_NOW) == ""


class TestRealWorldNotifications:
    """End-to-end shape matching the inbox-processor's actual output."""

    def test_amazon_dispatch_today(self):
        body = (
            "🍁 Kindle Scribe 64 GB отправлен\n"
            "🌿 Прибывает <2026-06-24> в Packstation 536\n"
            "🌿 [Отследить посылку](https://example.com)"
        )
        result = render_iso_dates(body, now=_NOW)
        # The original bug: this would have said "(вт)". Verify it now says "(ср)".
        assert "сегодня, 24 июня (ср)" in result
        assert "(вт)" not in result
        # Markdown link and emojis must survive untouched.
        assert "🍁" in result
        assert "🌿" in result
        assert "[Отследить посылку]" in result

    def test_concert_with_time(self):
        body = "🍁 Kaneko Ayano\n🌿 <2026-06-26 19:00>, Funkhaus Berlin"
        result = render_iso_dates(body, now=_NOW)
        assert "26 июня (пт), 19:00" in result

    def test_yesterday_reminder(self):
        body = "🍁 Дедлайн прошёл <2026-06-23>"
        result = render_iso_dates(body, now=_NOW)
        assert "вчера, 23 июня (вт)" in result


class TestDefaultNowFallback:
    def test_default_now_does_not_crash(self):
        # When no ``now`` is passed, the function reads the system clock.
        # We can't assert the relative label, but we can assert the
        # absolute date+weekday is correct.
        result = render_iso_dates("<2030-01-01>")
        # 2030-01-01 is a Tuesday.
        assert "1 января (вт)" in result
