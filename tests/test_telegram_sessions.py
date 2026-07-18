"""Tests for the /sessions inline-keyboard builder (nerve.channels.telegram)."""

from nerve.channels.telegram import build_session_tail_view, build_sessions_view


def _flat(markup):
    return [btn for row in markup.inline_keyboard for btn in row]


def _cbs(markup):
    return [b.callback_data for b in _flat(markup)]


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


# --- catch-up tail (build_session_tail_view) ------------------------------- #

_SESSION = {"id": "aaaa1111", "title": "Work", "status": "idle"}


def _msg(role, content, iso):
    return {"role": role, "content": content, "created_at": iso}


def test_tail_native_order_oldest_top_recent_bottom():
    msgs = [
        _msg("user", "FIRST question", "2026-07-18T09:00:00+00:00"),
        _msg("assistant", "SECOND answer", "2026-07-18T09:05:00+00:00"),
        _msg("user", "THIRD followup", "2026-07-18T09:10:00+00:00"),
    ]
    text, _markup = build_session_tail_view(_SESSION, msgs, total=3, window=6, tzname="UTC")
    # Native chat order: earlier messages appear above later ones.
    assert text.index("FIRST") < text.index("SECOND") < text.index("THIRD")
    assert "🧑" in text and "🤖" in text


def test_tail_timestamps_in_user_timezone():
    # 09:00 UTC rendered for a UTC+3 zone must read 12:00 (matches the user's client).
    msgs = [_msg("user", "hi", "2026-07-18T09:00:00+00:00")]
    text, _m = build_session_tail_view(_SESSION, msgs, total=1, window=6, tzname="Europe/Moscow")
    assert "[12:00]" in text
    # And the same instant is 09:00 in UTC.
    text_utc, _ = build_session_tail_view(_SESSION, msgs, total=1, window=6, tzname="UTC")
    assert "[09:00]" in text_utc


def test_tail_load_more_button_when_more_history():
    msgs = [_msg("user", f"m{n}", "2026-07-18T09:00:00+00:00") for n in range(6)]
    text, markup = build_session_tail_view(_SESSION, msgs, total=20, window=6, tzname="UTC")
    cbs = _cbs(markup)
    assert "sesstail:aaaa1111:12" in cbs   # next window = 6 + step(6)
    assert "sess:list" in cbs              # back-to-list always present
    assert "14 earlier" in text            # 20 - 6 shown


def test_tail_no_load_more_when_all_shown():
    msgs = [_msg("user", "only", "2026-07-18T09:00:00+00:00")]
    _text, markup = build_session_tail_view(_SESSION, msgs, total=1, window=6, tzname="UTC")
    assert _cbs(markup) == ["sess:list"]   # no Load more


def test_tail_capped_offers_no_more_but_hints_full_history():
    msgs = [_msg("assistant", f"m{n}", "2026-07-18T09:00:00+00:00") for n in range(18)]
    text, markup = build_session_tail_view(_SESSION, msgs, total=100, window=18, tzname="UTC")
    assert not any(c.startswith("sesstail:") for c in _cbs(markup))
    assert "open the session for the rest" in text


def test_tail_empty_session():
    text, markup = build_session_tail_view(
        {"id": "x", "status": "running"}, [], total=0, window=6, tzname="UTC",
    )
    assert "no messages yet" in text
    assert _cbs(markup) == ["sess:list"]
