"""Tests for ISO date placeholder rendering in notification text.

The rendering layer fixes a recurring failure mode in cron-driven
notifications: the model would write "24 June (Tue)" when 24 June 2026 is
actually a Wednesday. We can't fix the arithmetic by asking nicely, so
the model now declares intent via ``<YYYY-MM-DD>`` placeholders and
``datetime.weekday()`` does the math.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from nerve.notifications.date_render import (
    DEFAULT_LOCALE,
    render_iso_dates,
)


# Fixed reference: 2026-06-24 was a Wednesday. The whole suite anchors
# its "today/tomorrow/yesterday" assertions to this date so changes in
# wall-clock time can't drift the expectations.
_NOW = datetime(2026, 6, 24, 10, 0)


class TestBasicRendering:
    def test_plain_date_renders_with_weekday(self):
        assert render_iso_dates("<2026-06-24>", now=_NOW) == "today, 24 June (Wed)"

    def test_date_with_time(self):
        assert render_iso_dates("<2026-06-24 19:00>", now=_NOW) == (
            "today, 24 June (Wed), 19:00"
        )

    def test_iso_t_separator_also_accepted(self):
        # The model occasionally writes the canonical ISO 8601 T separator —
        # accept it so we don't punish well-formed input.
        assert render_iso_dates("<2026-06-24T19:00>", now=_NOW) == (
            "today, 24 June (Wed), 19:00"
        )

    def test_far_future_date_no_relative_label(self):
        # Far enough that no "today/tomorrow/yesterday" applies.
        assert render_iso_dates("<2027-01-15>", now=_NOW) == "15 January (Fri)"

    def test_date_inside_sentence(self):
        text = "Arriving <2026-06-24> at the pickup point"
        expected = "Arriving today, 24 June (Wed) at the pickup point"
        assert render_iso_dates(text, now=_NOW) == expected


class TestRelativeLabels:
    def test_today(self):
        # 24 June 2026 == _NOW date
        assert "today" in render_iso_dates("<2026-06-24>", now=_NOW)

    def test_tomorrow(self):
        assert render_iso_dates("<2026-06-25>", now=_NOW) == "tomorrow, 25 June (Thu)"

    def test_yesterday(self):
        assert render_iso_dates("<2026-06-23>", now=_NOW) == "yesterday, 23 June (Tue)"

    def test_two_days_ahead_no_label(self):
        # +2 days gets no label — relative-arithmetic territory we
        # deliberately avoid. Absolute date only.
        result = render_iso_dates("<2026-06-26>", now=_NOW)
        assert result == "26 June (Fri)"

    def test_two_days_back_no_label(self):
        result = render_iso_dates("<2026-06-22>", now=_NOW)
        assert result == "22 June (Mon)"


class TestWeekdayArithmetic:
    """Anchor the weekday math against known references.

    These tests would have caught the original bug: 24 June 2026 is a
    Wednesday, NOT a Tuesday. The model got this wrong; the renderer
    must not.
    """

    @pytest.mark.parametrize("iso,expected_weekday", [
        ("2026-06-22", "Mon"),
        ("2026-06-23", "Tue"),
        ("2026-06-24", "Wed"),
        ("2026-06-25", "Thu"),
        ("2026-06-26", "Fri"),
        ("2026-06-27", "Sat"),
        ("2026-06-28", "Sun"),
        # Different month, different year — same arithmetic.
        ("2027-12-31", "Fri"),
        # Leap day in a real leap year — sanity check.
        ("2028-02-29", "Tue"),
    ])
    def test_known_weekdays(self, iso, expected_weekday):
        result = render_iso_dates(f"<{iso}>", now=_NOW)
        assert f"({expected_weekday})" in result, (
            f"{iso}: expected ({expected_weekday}) in {result!r}"
        )


class TestWeekdayNamePlaceholder:
    """`<dow:Weekday>` resolves a bare weekday name to the nearest upcoming date.

    Covers the second failure mode: given a source that says only "arriving
    Tuesday", the model would reuse a stale date it had seen earlier in the
    thread instead of computing which Tuesday was meant. Now it copies the
    weekday name into a placeholder and the renderer does the arithmetic.
    Anchored to Wed 2026-06-24 (_NOW).
    """

    def test_today_weekday_resolves_to_today(self):
        # _NOW is a Wednesday.
        assert render_iso_dates("<dow:Wednesday>", now=_NOW) == "today, 24 June (Wed)"

    def test_tomorrow_weekday(self):
        assert render_iso_dates("<dow:Thursday>", now=_NOW) == (
            "tomorrow, 25 June (Thu)"
        )

    def test_next_tuesday_from_wednesday(self):
        # From Wed 24 June, the next Tuesday is 30 June (6 days ahead), no label.
        assert render_iso_dates("<dow:Tuesday>", now=_NOW) == "30 June (Tue)"

    def test_next_monday_from_wednesday(self):
        assert render_iso_dates("<dow:Monday>", now=_NOW) == "29 June (Mon)"

    def test_resolves_from_an_arbitrary_reference_day(self):
        # A notice that arrives Thu 16 July 2026 saying "arriving Tuesday".
        now = datetime(2026, 7, 16, 5, 30)  # Thursday
        assert render_iso_dates("<dow:Tuesday>", now=now) == "21 July (Tue)"

    @pytest.mark.parametrize("name", ["Tuesday", "tuesday", "TUESDAY", "Tue", "tues"])
    def test_english_forms_case_insensitive(self, name):
        assert render_iso_dates(f"<dow:{name}>", now=_NOW) == "30 June (Tue)"

    @pytest.mark.parametrize("name", ["Dienstag", "dienstag", "Di"])
    def test_german_forms(self, name):
        assert render_iso_dates(f"<dow:{name}>", now=_NOW) == "30 June (Tue)"

    @pytest.mark.parametrize("name", ["вторник", "вт", "Вторник"])
    def test_russian_forms(self, name):
        assert render_iso_dates(f"<dow:{name}>", now=_NOW) == "30 June (Tue)"

    def test_whitespace_tolerated(self):
        assert render_iso_dates("<dow: Tuesday >", now=_NOW) == "30 June (Tue)"

    def test_unrecognized_name_kept_as_placeholder(self):
        text = "Arriving <dow:someday>"
        assert render_iso_dates(text, now=_NOW) == text

    def test_inside_a_notification_body(self):
        body = (
            "📦 Order dispatched\n"
            "🌿 Arriving <dow:Tuesday> at the pickup point\n"
            "🌿 [Track it](https://example.com)"
        )
        now = datetime(2026, 7, 16, 5, 30)  # Thursday
        result = render_iso_dates(body, now=now)
        assert "Arriving 21 July (Tue) at the pickup point" in result
        # Markdown link and emoji must survive untouched.
        assert "📦" in result and "[Track it]" in result

    def test_iso_and_dow_placeholders_coexist(self):
        text = "Ordered <2026-06-25>, delivery <dow:Tuesday>"
        result = render_iso_dates(text, now=_NOW)
        assert "tomorrow, 25 June (Thu)" in result
        assert "30 June (Tue)" in result


class TestMonthNames:
    @pytest.mark.parametrize("month_num,month_name", [
        (1, "January"), (2, "February"), (3, "March"), (4, "April"),
        (5, "May"), (6, "June"), (7, "July"), (8, "August"),
        (9, "September"), (10, "October"), (11, "November"), (12, "December"),
    ])
    def test_all_months(self, month_num, month_name):
        result = render_iso_dates(f"<2027-{month_num:02d}-15>", now=_NOW)
        assert month_name in result


class TestEdgeCases:
    def test_empty_string(self):
        assert render_iso_dates("", now=_NOW) == ""

    def test_text_without_placeholders_unchanged(self):
        text = "Just text, no dates. (Tue) stays put too."
        assert render_iso_dates(text, now=_NOW) == text

    def test_invalid_date_kept_as_placeholder(self):
        # 31 February doesn't exist — render must leave the placeholder
        # visible so the model can see its own bad output rather than
        # silently shipping something half-broken.
        text = "Due: <2026-02-31>"
        assert render_iso_dates(text, now=_NOW) == text

    def test_invalid_hour_kept_as_placeholder(self):
        text = "At: <2026-06-24 25:00>"
        assert render_iso_dates(text, now=_NOW) == text

    def test_invalid_minute_kept_as_placeholder(self):
        text = "At: <2026-06-24 19:99>"
        assert render_iso_dates(text, now=_NOW) == text

    def test_partial_placeholder_not_matched(self):
        # Missing closing bracket — leave alone.
        text = "Date: <2026-06-24"
        assert render_iso_dates(text, now=_NOW) == text

    def test_html_like_tags_not_matched(self):
        # The regex requires digits, so HTML tags pass through untouched.
        text = "<div>Hello</div>"
        assert render_iso_dates(text, now=_NOW) == text

    def test_multiple_placeholders_in_one_string(self):
        text = "From <2026-06-24> to <2026-06-26>"
        result = render_iso_dates(text, now=_NOW)
        assert "today, 24 June (Wed)" in result
        assert "26 June (Fri)" in result

    def test_none_text_returns_falsy(self):
        # The service layer may pass empty title — make sure we don't crash.
        assert render_iso_dates("", now=_NOW) == ""


class TestRealWorldNotifications:
    """End-to-end shape matching what a cron session actually emits."""

    def test_dispatch_notice_today(self):
        body = (
            "📦 Order dispatched\n"
            "🌿 Arriving <2026-06-24> at the pickup point\n"
            "🌿 [Track it](https://example.com)"
        )
        result = render_iso_dates(body, now=_NOW)
        # The original bug: this would have said "(Tue)". Verify it says "(Wed)".
        assert "today, 24 June (Wed)" in result
        assert "(Tue)" not in result
        # Markdown link and emoji must survive untouched.
        assert "📦" in result
        assert "🌿" in result
        assert "[Track it]" in result

    def test_event_with_time(self):
        body = "🎟 Concert\n🌿 <2026-06-26 19:00>, main hall"
        result = render_iso_dates(body, now=_NOW)
        assert "26 June (Fri), 19:00" in result

    def test_yesterday_reminder(self):
        body = "⏰ Deadline passed <2026-06-23>"
        result = render_iso_dates(body, now=_NOW)
        assert "yesterday, 23 June (Tue)" in result


class TestDefaultNowFallback:
    def test_default_now_does_not_crash(self):
        # When no ``now`` is passed, the function reads the system clock.
        # We can't assert the relative label, but we can assert the
        # absolute date+weekday is correct.
        result = render_iso_dates("<2030-01-01>")
        # 2030-01-01 is a Tuesday.
        assert "1 January (Tue)" in result


# ---------------------------------------------------------------------------
# Locale selection — the language is config, not a hardcode
# ---------------------------------------------------------------------------


class TestLocaleSelection:
    """Output language comes from notifications.date_locale.

    Placeholder *parsing* stays multilingual whatever the output locale is,
    because the source the model copied a weekday from may be in any
    language.
    """

    def test_module_default_is_english(self):
        assert DEFAULT_LOCALE == "en"
        assert render_iso_dates("<2026-06-24>", now=_NOW) == "today, 24 June (Wed)"

    def test_german(self):
        assert render_iso_dates("<2026-06-24>", now=_NOW, locale="de") == (
            "heute, 24 Juni (Mi)"
        )

    def test_russian(self):
        assert render_iso_dates("<2026-06-24>", now=_NOW, locale="ru") == (
            "сегодня, 24 июня (ср)"
        )

    def test_unknown_locale_falls_back_to_english(self):
        assert render_iso_dates("<2026-06-24>", now=_NOW, locale="klingon") == (
            "today, 24 June (Wed)"
        )

    def test_none_locale_falls_back(self):
        assert render_iso_dates("<2026-06-24>", now=_NOW, locale=None) == (
            "today, 24 June (Wed)"
        )

    @pytest.mark.parametrize("locale", ["en", "ru", "de"])
    def test_weekday_parsing_is_locale_independent(self, locale):
        # A German weekday name resolves even when rendering in English.
        out = render_iso_dates("<dow:Dienstag>", now=_NOW, locale=locale)
        assert "<dow:" not in out
        # 2026-06-24 is a Wednesday, so the next Tuesday is 30 June.
        assert "30" in out
