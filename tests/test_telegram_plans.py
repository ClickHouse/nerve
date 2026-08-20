"""Tests for the /plans inline-keyboard builders (nerve.channels.telegram)."""

import pytest

from nerve.channels.telegram import (
    _PLANS_PAGE_SIZE,
    build_plan_confirm_view,
    build_plan_detail_view,
    build_plans_view,
)


def _flat(markup):
    return [b for row in markup.inline_keyboard for b in row]


def _cbs(markup):
    return [b.callback_data for b in _flat(markup)]


# --- list view (build_plans_view) ----------------------------------------- #

def test_plan_ids_ride_in_callback_data_and_refresh_always_present():
    plans = [
        {"id": "plan-aaaa1111", "task_title": "Add dark mode", "status": "pending", "version": 1},
        {"id": "plan-bbbb2222", "task_title": "Cache API", "status": "implementing", "version": 1},
    ]
    text, markup = build_plans_view(plans)
    by_cb = {b.callback_data: b for b in _flat(markup)}
    # One tap-to-open button per plan (id carried, no copy-paste).
    assert by_cb["plan:view:plan-aaaa1111"].text == "🟠 Add dark mode"
    assert by_cb["plan:view:plan-bbbb2222"].text == "⚙️ Cache API"
    # Refresh is always the last button so the keyboard is never empty.
    assert markup.inline_keyboard[-1][0].callback_data == "plan:list:0"
    assert "pending" in text


def test_version_suffix_only_when_above_one():
    plans = [{"id": "plan-v", "task_title": "T", "status": "pending", "version": 3}]
    _text, markup = build_plans_view(plans)
    assert _flat(markup)[0].text == "🟠 T  v3"


def test_empty_queue_has_only_refresh():
    text, markup = build_plans_view([])
    assert _cbs(markup) == ["plan:list:0"]
    assert "No plans awaiting review" in text


def test_long_title_truncated():
    plans = [{"id": "plan-x", "task_title": "x" * 100, "status": "pending", "version": 1}]
    _text, markup = build_plans_view(plans)
    label = _flat(markup)[0].text
    assert label.endswith("…")
    assert len(label) <= 42   # emoji + space + clipped title


def test_oversized_callback_data_is_skipped():
    huge = "plan-" + "z" * 70   # plan:view:<huge> > 64 bytes → must be dropped
    plans = [
        {"id": huge, "task_title": "too big", "status": "pending", "version": 1},
        {"id": "plan-ok", "task_title": "ok", "status": "pending", "version": 1},
    ]
    _text, markup = build_plans_view(plans)
    cbs = _cbs(markup)
    assert f"plan:view:{huge}" not in cbs
    assert "plan:view:plan-ok" in cbs


def test_page_size_caps_rows_but_keeps_refresh():
    plans = [
        {"id": f"plan-{n:06d}", "task_title": f"P{n}", "status": "pending", "version": 1}
        for n in range(50)
    ]
    _text, markup = build_plans_view(plans, has_next=True)
    view_btns = [c for c in _cbs(markup) if c.startswith("plan:view:")]
    assert len(view_btns) == _PLANS_PAGE_SIZE
    assert "plan:list:0" in _cbs(markup)   # refresh still present


def test_first_page_offers_more_not_prev():
    plans = [{"id": f"plan-{n:03d}", "task_title": f"P{n}", "status": "pending", "version": 1}
             for n in range(_PLANS_PAGE_SIZE)]
    _text, markup = build_plans_view(plans, offset=0, has_prev=False, has_next=True)
    cbs = _cbs(markup)
    assert f"plan:list:{_PLANS_PAGE_SIZE}" in cbs   # ➡️ More → page 2
    # No ⬅️ Prev target other than the always-present refresh (plan:list:0).
    assert "⬅️ Prev" not in [b.text for b in _flat(markup)]


def test_middle_page_offers_prev_and_more():
    off = _PLANS_PAGE_SIZE
    plans = [{"id": f"plan-{n:03d}", "task_title": f"P{n}", "status": "pending", "version": 1}
             for n in range(_PLANS_PAGE_SIZE)]
    text, markup = build_plans_view(plans, offset=off, has_prev=True, has_next=True)
    cbs = _cbs(markup)
    assert f"plan:list:{max(0, off - _PLANS_PAGE_SIZE)}" in cbs   # ⬅️ Prev
    assert f"plan:list:{off + _PLANS_PAGE_SIZE}" in cbs           # ➡️ More
    assert "Page 2" in text


# --- detail view (build_plan_detail_view) --------------------------------- #

_PENDING = {
    "id": "plan-detail1", "task_title": "Add dark mode", "task_id": "t1",
    "status": "pending", "version": 2, "plan_type": "generic",
    "content": "1. add toggle\n2. persist choice",
    "created_at": "2026-08-20T09:00:00+00:00",
}


def test_detail_pending_shows_all_actions():
    text, markup = build_plan_detail_view(_PENDING, tzname="UTC")
    cbs = _cbs(markup)
    assert cbs == [
        "plan:approve:plan-detail1",
        "plan:decline:plan-detail1",
        "plan:revise:plan-detail1",
        "plan:list:0",
    ]
    assert "<blockquote expandable>" in text     # body collapses/expands natively
    assert "Add dark mode" in text
    assert "v2" in text


def test_detail_non_pending_is_read_only_and_shows_impl():
    plan = {
        "id": "plan-impl", "task_title": "Cache API", "status": "implementing",
        "version": 1, "content": "do it", "impl_session_id": "impl-1234abcd",
        "created_at": "2026-08-20T09:00:00+00:00",
    }
    text, markup = build_plan_detail_view(plan, tzname="UTC")
    # No approve/decline/revise on a non-pending plan — only back-to-list.
    assert _cbs(markup) == ["plan:list:0"]
    assert "impl-1234abcd" in text


def test_detail_escapes_html_in_content():
    plan = {**_PENDING, "content": "<b>&danger</b>"}
    text, _m = build_plan_detail_view(plan, tzname="UTC")
    assert "&lt;b&gt;&amp;danger&lt;/b&gt;" in text


def test_detail_shows_revision_feedback():
    plan = {**_PENDING, "feedback": "please add tests"}
    text, _m = build_plan_detail_view(plan, tzname="UTC")
    assert "please add tests" in text


def test_detail_stays_within_telegram_limit():
    plan = {**_PENDING, "content": "y" * 8000}
    text, _m = build_plan_detail_view(plan, tzname="UTC")
    assert len(text) <= 4096


# --- confirm view (build_plan_confirm_view) ------------------------------- #

def test_confirm_approve_buttons():
    text, markup = build_plan_confirm_view(_PENDING, "approve")
    cbs = _cbs(markup)
    assert "plan:approveok:plan-detail1" in cbs
    assert "plan:view:plan-detail1" in cbs   # ◀️ Back to detail
    assert "implementation session" in text.lower()


def test_confirm_decline_buttons():
    text, markup = build_plan_confirm_view(_PENDING, "decline")
    cbs = _cbs(markup)
    assert "plan:declineok:plan-detail1" in cbs
    assert "plan:view:plan-detail1" in cbs
    assert "closes the task" in text.lower()


# --- _plans_view_for paging (pending-first, implementing after) ----------- #

class _FakeRouter:
    """Stands in for the channel router: returns plans by status like the
    store would (each already newest-first)."""

    def __init__(self, pending, implementing=None):
        self._pending = pending
        self._impl = implementing or []
        self.calls = []

    async def list_plans(self, status=None, limit=100):
        self.calls.append((status, limit))
        if status == "pending":
            return list(self._pending)
        if status == "implementing":
            return list(self._impl)
        return list(self._pending) + list(self._impl)


def _make_channel(router):
    from nerve.channels.telegram import TelegramChannel
    ch = TelegramChannel.__new__(TelegramChannel)   # bypass __init__; only .router needed
    ch.router = router
    return ch


@pytest.mark.asyncio
async def test_plans_view_lists_pending_then_implementing():
    router = _FakeRouter(
        pending=[{"id": "plan-p1", "task_title": "P1", "status": "pending", "version": 1}],
        implementing=[{"id": "plan-i1", "task_title": "I1", "status": "implementing", "version": 1}],
    )
    ch = _make_channel(router)
    _text, markup = await ch._plans_view_for()
    cbs = _cbs(markup)
    # pending first, implementing next, refresh last.
    assert cbs == ["plan:view:plan-p1", "plan:view:plan-i1", "plan:list:0"]


@pytest.mark.asyncio
async def test_plans_view_paginates():
    pending = [
        {"id": f"plan-{n:03d}", "task_title": f"P{n}", "status": "pending", "version": 1}
        for n in range(_PLANS_PAGE_SIZE * 2 + 1)
    ]
    ch = _make_channel(_FakeRouter(pending=pending))

    # Page 1: More but no Prev.
    _t, m1 = await ch._plans_view_for(0)
    cbs1 = _cbs(m1)
    assert len([c for c in cbs1 if c.startswith("plan:view:")]) == _PLANS_PAGE_SIZE
    assert f"plan:list:{_PLANS_PAGE_SIZE}" in cbs1

    # Last page: Prev but no More.
    _t, m3 = await ch._plans_view_for(_PLANS_PAGE_SIZE * 2)
    cbs3 = _cbs(m3)
    assert len([c for c in cbs3 if c.startswith("plan:view:")]) == 1   # remainder
    assert f"plan:list:{_PLANS_PAGE_SIZE}" in cbs3    # ⬅️ Prev → page 2
    # No ➡️ More button target beyond the last page.
    assert not any(c == f"plan:list:{_PLANS_PAGE_SIZE * 3}" for c in cbs3)


@pytest.mark.asyncio
async def test_plans_view_empty():
    ch = _make_channel(_FakeRouter(pending=[]))
    text, markup = await ch._plans_view_for()
    assert _cbs(markup) == ["plan:list:0"]
    assert "No plans awaiting review" in text
