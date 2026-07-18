"""Tests for the /sessions inline-keyboard builder (nerve.channels.telegram)."""

from nerve.channels.telegram import build_sessions_view


def _flat(markup):
    return [btn for row in markup.inline_keyboard for btn in row]


def test_current_session_marked_and_ids_ride_in_callback_data():
    sessions = [
        {"id": "aaaa1111", "title": "First", "source": "telegram"},
        {"id": "bbbb2222", "title": "Second", "source": "web"},
    ]
    text, markup = build_sessions_view(sessions, current_id="bbbb2222")
    btns = _flat(markup)
    by_cb = {b.callback_data: b for b in btns}
    # Each session is a tap-to-switch button carrying its id (no copy-paste).
    assert by_cb["sess:aaaa1111"].text == "First"
    assert by_cb["sess:bbbb2222"].text == "✓ Second"   # current marked
    assert "Current: Second" in text


def test_new_session_button_always_present():
    text, markup = build_sessions_view([], current_id=None)
    cbs = [b.callback_data for b in _flat(markup)]
    assert cbs == ["sess:new"]
    assert "No sessions" in text


def test_new_session_note_reassures_current_keeps_running():
    # The New-session button must not read like /new (which stops the current);
    # the view states the current session keeps running.
    text, _markup = build_sessions_view(
        [{"id": "aaaa1111", "title": "Work", "source": "telegram"}],
        current_id="aaaa1111",
    )
    assert "keeps the current one running" in text


def test_long_title_truncated():
    long = "x" * 100
    _text, markup = build_sessions_view(
        [{"id": "cccc3333", "title": long, "source": "telegram"}], current_id=None,
    )
    label = _flat(markup)[0].text
    assert label.endswith("…")
    assert len(label) <= 40


def test_missing_title_falls_back_to_id():
    _text, markup = build_sessions_view(
        [{"id": "dddd4444", "source": "telegram"}], current_id="dddd4444",
    )
    assert _flat(markup)[0].text == "✓ dddd4444"


def test_button_limit_caps_sessions_but_keeps_new():
    sessions = [
        {"id": f"id{n:06d}", "title": f"S{n}", "source": "web"} for n in range(20)
    ]
    _text, markup = build_sessions_view(sessions, current_id=None)
    rows = markup.inline_keyboard
    # 8 session buttons + 1 trailing "New session" row.
    assert len(rows) == 9
    assert rows[-1][0].callback_data == "sess:new"


def test_oversized_callback_data_is_skipped():
    huge = "z" * 70  # sess:<70z> > 64 bytes → cannot round-trip, must be dropped
    sessions = [
        {"id": huge, "title": "too big", "source": "web"},
        {"id": "eeee5555", "title": "ok", "source": "web"},
    ]
    _text, markup = build_sessions_view(sessions, current_id=None)
    cbs = [b.callback_data for b in _flat(markup)]
    assert f"sess:{huge}" not in cbs
    assert "sess:eeee5555" in cbs
    assert "sess:new" in cbs
