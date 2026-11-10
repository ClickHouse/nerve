"""telegram.topics.auto: a topic for every session, opened and let go of by Nerve.

The channel opens a topic when a turn of an eligible session starts, mirrors
finished turns into it, renames it once the title is known, and lets go of it
when the session is archived, deleted, or (a one-run cron session) done. Each
session gets at most one such topic, and only those topics are closed by it.
"""

from __future__ import annotations

import json

import pytest
from telegram.error import BadRequest, RetryAfter

from nerve.channels.session_events import (
    SESSION_ARCHIVED,
    SESSION_DELETED,
    TurnFinished,
    TurnStarted,
)
from nerve.channels.telegram_auto_topics import MARKER, is_one_run_cron, mirror_text
from nerve.config import (
    TelegramAutoTopicsConfig,
    TelegramTopicsConfig,
    validate_config_keys,
)
from tests.test_telegram_topics import CHAT, OWNER, _channel, _Router


class _Db:
    def __init__(self, router):
        self.router = router

    async def set_session_metadata_key(self, sid, key, value):
        session = self.router.sessions.get(sid)
        if session is None:
            return False
        meta = json.loads(session.get("metadata") or "{}")
        if value is None:
            meta.pop(key, None)
        else:
            meta[key] = value
        session["metadata"] = json.dumps(meta)
        return True


class _AutoRouter(_Router):
    def __init__(self):
        super().__init__()
        self.engine = type("E", (), {})()
        self.engine.db = _Db(self)


def _setup(*, chat=None, **auto):
    router = _AutoRouter()
    ch = _channel(router)
    ch.config.telegram.topics = TelegramTopicsConfig(
        enabled=True, auto=TelegramAutoTopicsConfig(enabled=True, **auto),
    )
    if chat is not None:
        ch.config.notifications.telegram_chat_id = chat
    return router, ch, ch._app.bot


def _session(router, sid="web00001", source="web", title="Quarterly taxes", **fields):
    router.sessions[sid] = {
        "id": sid, "title": title, "status": "idle", "source": source, **fields,
    }
    return sid


def _marker(router, sid):
    return json.loads(router.sessions[sid].get("metadata") or "{}").get(MARKER)


def _started(sid, source="web", channel=None):
    return TurnStarted(session_id=sid, source=source, channel=channel)


def _finished(sid, text="The answer.", *, source="web", channel=None, user="What is it?",
              status="completed", autonomous=False, work_pending=False):
    return TurnFinished(
        session_id=sid, source=source, channel=channel, user_message=user, text=text,
        status=status, autonomous=autonomous, work_pending=work_pending,
    )


async def _turn(ch, event):
    if isinstance(event, TurnStarted):
        ch.on_turn_started(event)
    else:
        ch.on_turn_finished(event)
    await ch._auto_topics.wait_idle()


def _sent_texts(bot):
    return [k.get("text") or json.dumps(k) for k in bot.texts_sent()]


# --------------------------------------------------------------------------- #
#  Opening                                                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_web_session_gets_a_topic_when_its_turn_starts():
    router, ch, bot = _setup()
    sid = _session(router)

    await _turn(ch, _started(sid))

    key = f"telegram:{OWNER}:501"
    assert bot.named("create_forum_topic") == [{"chat_id": OWNER, "name": "Quarterly taxes"}]
    assert router.bound == {key: sid}
    marker = _marker(router, sid)
    assert marker["channel_key"] == key and marker["title"] == "Quarterly taxes"


@pytest.mark.asyncio
async def test_a_session_gets_one_topic_however_many_turns():
    router, ch, bot = _setup()
    sid = _session(router)
    await _turn(ch, _started(sid))
    cards = len(bot.texts_sent())

    await _turn(ch, _started(sid))
    await _turn(ch, _started(sid))

    assert len(bot.named("create_forum_topic")) == 1
    assert len(bot.texts_sent()) == cards  # no card posted again


@pytest.mark.asyncio
@pytest.mark.parametrize("auto_on,topics_on", [(False, True), (True, False)])
async def test_nothing_opens_with_the_switch_off(auto_on, topics_on):
    router, ch, bot = _setup()
    ch.config.telegram.topics = TelegramTopicsConfig(
        enabled=topics_on, auto=TelegramAutoTopicsConfig(enabled=auto_on),
    )
    sid = _session(router)
    await _turn(ch, _started(sid))
    assert bot.calls == [] and _marker(router, sid) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("sid,source", [
    ("cron:nightly-report:20261110-1", "cron"),   # excluded by pattern
    ("wf-leg-1", "workflow"),               # source not listed
    ("tg000001", "telegram"),               # lives in Telegram already
])
async def test_ineligible_sessions_get_no_topic(sid, source):
    router, ch, bot = _setup(exclude=["cron:nightly-report:*"])
    _session(router, sid, source=source)
    await _turn(ch, _started(sid, source=source))
    assert bot.named("create_forum_topic") == []


@pytest.mark.asyncio
async def test_a_cron_run_and_a_hook_get_topics():
    router, ch, bot = _setup(exclude=["cron:nightly-report:*"])
    cron = _session(router, "cron:awe-wonder:20261110-1", source="cron", title="Cron: awe-wonder")
    hook = _session(router, "hook:gh:1", source="hook", title="Hook: gh")
    await _turn(ch, _started(cron, source="cron"))
    await _turn(ch, _started(hook, source="hook"))
    assert [c["name"] for c in bot.named("create_forum_topic")] == ["Cron: awe-wonder", "Hook: gh"]


@pytest.mark.asyncio
async def test_a_session_already_on_telegram_is_left_alone():
    router, ch, bot = _setup()
    sid = _session(router)
    router.bound[f"telegram:{OWNER}:77"] = sid  # opened by hand earlier
    await _turn(ch, _started(sid))
    assert bot.named("create_forum_topic") == [] and _marker(router, sid) is None


@pytest.mark.asyncio
async def test_a_topic_let_go_of_is_not_opened_again():
    router, ch, bot = _setup()
    sid = _session(router)
    await _turn(ch, _started(sid))
    await ch.on_session_ending(sid, SESSION_ARCHIVED)
    router.sessions[sid]["status"] = "idle"  # unarchived

    await _turn(ch, _started(sid))

    assert len(bot.named("create_forum_topic")) == 1


@pytest.mark.asyncio
async def test_a_refusal_is_recorded_and_not_retried():
    router, ch, bot = _setup()
    bot.refuse_topics = BadRequest("Chat is not a forum")
    sid = _session(router)
    await _turn(ch, _started(sid))
    await _turn(ch, _started(sid))
    assert len(bot.named("create_forum_topic")) == 1
    assert "not a forum" in _marker(router, sid)["error"]


@pytest.mark.asyncio
async def test_a_rate_limit_is_retried_on_the_next_turn():
    router, ch, bot = _setup()
    bot.refuse_topics = RetryAfter(30)
    sid = _session(router)
    await _turn(ch, _started(sid))
    assert _marker(router, sid) is None

    bot.refuse_topics = None
    await _turn(ch, _started(sid))
    assert _marker(router, sid)["channel_key"] == f"telegram:{OWNER}:501"


@pytest.mark.asyncio
async def test_the_home_chat_is_the_notification_chat_when_set():
    router, ch, bot = _setup(chat=CHAT)
    sid = _session(router)
    await _turn(ch, _started(sid))
    assert bot.named("create_forum_topic")[0]["chat_id"] == CHAT


def test_a_failing_channel_hook_does_not_reach_the_turn():
    from nerve.channels.router import ChannelRouter

    class _Boom:
        name = "boom"

        def on_turn_started(self, event):
            raise RuntimeError("boom")

        def on_turn_finished(self, event):
            raise RuntimeError("boom")

    router = ChannelRouter.__new__(ChannelRouter)
    router._channels = {"boom": _Boom()}
    router.turn_started(_started("s1"))          # no raise
    router.turn_finished(_finished("s1"))


# --------------------------------------------------------------------------- #
#  Mirroring and renaming                                                       #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_finished_web_turn_is_mirrored_into_the_topic():
    router, ch, bot = _setup(mirror_turns=True)
    sid = _session(router)
    await _turn(ch, _started(sid))
    before = len(bot.texts_sent())

    await _turn(ch, _finished(sid, "Pay by the 31st.", channel="web", user="When are taxes due?"))

    sent = bot.texts_sent()[before:]
    assert len(sent) == 1
    assert sent[0].get("message_thread_id") == 501
    body = json.dumps(sent[0])
    assert "When are taxes due?" in body and "Pay by the 31st." in body


@pytest.mark.asyncio
async def test_nothing_is_mirrored_with_mirroring_off():
    router, ch, bot = _setup(mirror_turns=False)
    sid = _session(router)
    await _turn(ch, _started(sid))
    before = len(bot.texts_sent())
    await _turn(ch, _finished(sid))
    assert len(bot.texts_sent()) == before


@pytest.mark.asyncio
async def test_a_turn_from_telegram_is_not_mirrored_back():
    router, ch, bot = _setup(mirror_turns=True)
    sid = _session(router)
    await _turn(ch, _started(sid))
    before = len(bot.texts_sent())
    await _turn(ch, _finished(sid, channel="telegram", source="telegram"))
    assert len(bot.texts_sent()) == before


@pytest.mark.asyncio
async def test_no_mirror_without_an_auto_topic():
    router, ch, bot = _setup(mirror_turns=True)
    sid = _session(router)
    router.bound[f"telegram:{OWNER}:77"] = sid  # opened by hand: not ours to mirror into
    await _turn(ch, _finished(sid))
    assert bot.texts_sent() == []


def test_mirror_text_quotes_cuts_and_marks():
    long = "x" * 50
    text = mirror_text(_finished("s", long, channel="web", user="q\nline2"), max_chars=20)
    assert text.startswith("> **web:** q\n> line2")
    assert "x" * 20 in text and "x" * 21 not in text and "full answer is in the web UI" in text
    auto = mirror_text(_finished("s", "done", user=None, autonomous=True), 100)
    assert auto.startswith("_Background continuation_")
    failed = mirror_text(_finished("s", "Agent error: x", status="failed"), 100)
    assert "⚠️ Agent error: x" in failed


@pytest.mark.asyncio
async def test_a_title_that_came_late_renames_the_topic_once():
    router, ch, bot = _setup()
    sid = _session(router, title=None)
    await _turn(ch, _started(sid))
    router.sessions[sid]["title"] = "Tax deadlines"

    await _turn(ch, _finished(sid))
    await _turn(ch, _finished(sid))

    renames = bot.named("edit_forum_topic")
    assert [r["name"] for r in renames] == ["Tax deadlines"]
    assert _marker(router, sid)["title"] == "Tax deadlines"


# --------------------------------------------------------------------------- #
#  Letting go                                                                   #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_archiving_in_a_dm_posts_a_note_and_unbinds():
    router, ch, bot = _setup()
    sid = _session(router)
    await _turn(ch, _started(sid))
    before = len(bot.texts_sent())

    await ch.on_session_ending(sid, SESSION_ARCHIVED)

    assert router.bound == {}
    note = bot.texts_sent()[before:]
    assert len(note) == 1 and "archived" in json.dumps(note[0])
    assert bot.named("close_forum_topic") == []  # Telegram can't, in a DM
    marker = _marker(router, sid)
    assert marker["outcome"] == "left_open" and marker["closed_on"] == "archive"


@pytest.mark.asyncio
async def test_archiving_in_a_forum_closes_the_topic():
    router, ch, bot = _setup(chat=CHAT)
    sid = _session(router)
    await _turn(ch, _started(sid))
    await ch.on_session_ending(sid, SESSION_ARCHIVED)
    assert bot.named("close_forum_topic") == [{"chat_id": CHAT, "message_thread_id": 501}]
    assert _marker(router, sid)["outcome"] == "closed"


@pytest.mark.asyncio
async def test_deleting_lets_go_without_writing_the_marker():
    router, ch, bot = _setup()
    sid = _session(router)
    await _turn(ch, _started(sid))
    opened = _marker(router, sid)
    await ch.on_session_ending(sid, SESSION_DELETED)
    assert router.bound == {} and _marker(router, sid) == opened


@pytest.mark.asyncio
async def test_private_chat_close_delete_deletes_the_topic():
    router, ch, bot = _setup(private_chat_close="delete")
    sid = _session(router)
    await _turn(ch, _started(sid))
    before = len(bot.texts_sent())
    await ch.on_session_ending(sid, SESSION_ARCHIVED)
    assert bot.named("delete_forum_topic") == [{"chat_id": OWNER, "message_thread_id": 501}]
    assert len(bot.texts_sent()) == before  # no note in a topic about to vanish
    assert _marker(router, sid)["outcome"] == "deleted"


@pytest.mark.asyncio
async def test_a_topic_opened_by_hand_is_not_closed():
    router, ch, bot = _setup()
    sid = _session(router)
    key = f"telegram:{OWNER}:77"
    router.bound[key] = sid
    await ch.on_session_ending(sid, SESSION_ARCHIVED)
    assert router.bound == {key: sid} and bot.calls == []


@pytest.mark.asyncio
async def test_close_on_limits_the_events():
    router, ch, bot = _setup(close_on=["delete"])
    sid = _session(router)
    await _turn(ch, _started(sid))
    await ch.on_session_ending(sid, SESSION_ARCHIVED)
    assert router.bound != {}
    await ch.on_session_ending(sid, SESSION_DELETED)
    assert router.bound == {}


@pytest.mark.asyncio
async def test_a_one_run_cron_session_lets_go_when_its_run_ends():
    router, ch, bot = _setup(mirror_turns=True)
    sid = _session(router, "cron:awe-wonder:20261110-1", source="cron", title="Cron: awe-wonder")
    await _turn(ch, _started(sid, source="cron"))
    before = len(bot.texts_sent())

    await _turn(ch, _finished(sid, "Posted today's item.", source="cron", channel=None, user=None))

    texts = [json.dumps(t) for t in bot.texts_sent()[before:]]
    assert len(texts) == 2
    assert "Posted today" in texts[0] and "Run finished" in texts[1]  # output first
    assert router.bound == {}
    assert _marker(router, sid)["closed_on"] == "cron_done"


@pytest.mark.asyncio
async def test_a_cron_run_with_background_work_stays_until_it_settles():
    router, ch, bot = _setup()
    sid = _session(router, "cron:awe-wonder:20261110-1", source="cron")
    await _turn(ch, _started(sid, source="cron"))
    await _turn(ch, _finished(sid, source="cron", user=None, work_pending=True))
    assert router.bound != {}
    await _turn(ch, _finished(sid, source="cron", user=None, autonomous=True))
    assert router.bound == {}


@pytest.mark.asyncio
async def test_a_persistent_cron_session_keeps_its_topic():
    router, ch, bot = _setup()
    sid = _session(router, "cron:inbox", source="cron")
    await _turn(ch, _started(sid, source="cron"))
    await _turn(ch, _finished(sid, source="cron", user=None))
    assert router.bound != {}


def test_one_run_cron_shape():
    assert is_one_run_cron({"id": "cron:nightly-report:20261110-1", "source": "cron"})
    assert not is_one_run_cron({"id": "cron:inbox", "source": "cron"})
    assert not is_one_run_cron({"id": "cron:x:y", "source": "web"})


# --------------------------------------------------------------------------- #
#  Config, store and engine seams                                               #
# --------------------------------------------------------------------------- #


def test_config_parse():
    cfg = TelegramTopicsConfig.from_dict({"auto": {
        "enabled": "true", "exclude": "cron:nightly-report:*", "mirror_turns": True,
        "close_on": ["archive", "cron_done"],
    }}).auto
    assert cfg.enabled is True and cfg.exclude == ["cron:nightly-report:*"]
    assert cfg.sources == ["web", "cron", "hook"] and cfg.close_on == ["archive", "cron_done"]
    assert TelegramTopicsConfig.from_dict({}).auto.enabled is False
    assert TelegramTopicsConfig.from_dict({"auto": True}).auto.enabled is True


@pytest.mark.asyncio
async def test_unknown_config_values_are_harmless():
    """Kept as given (the coercion sweep needs that), warned about, never matched."""
    router, ch, bot = _setup(close_on=["stop"], private_chat_close="close")
    sid = _session(router)
    await _turn(ch, _started(sid))
    await ch.on_session_ending(sid, SESSION_ARCHIVED)  # "archive" not listed: nothing
    assert router.bound != {}
    cfg = TelegramAutoTopicsConfig.from_dict({"close_on": ["archive"], "private_chat_close": "close"})
    ch.config.telegram.topics.auto = cfg
    await ch.on_session_ending(sid, SESSION_ARCHIVED)
    assert bot.named("delete_forum_topic") == [] and router.bound == {}  # unbound, not deleted


def test_config_keys_validate():
    ok = {"telegram": {"topics": {"auto": {"enabled": True, "mirror_turns": True}}}}
    assert validate_config_keys(ok) == []
    assert validate_config_keys({"telegram": {"topics": {"auto": {"mirror": True}}}}) != []


@pytest.mark.asyncio
async def test_the_marker_survives_a_stale_metadata_write(db):
    await db.create_session("m1", title="t", source="web", actor=db.system_actor)
    stale = json.loads((await db.get_session("m1")).get("metadata") or "{}")
    await db.set_session_metadata_key("m1", MARKER, {"channel_key": "telegram:1:2"})
    await db.update_session_metadata("m1", {**stale, "last_usage": {"x": 1}})
    meta = json.loads((await db.get_session("m1"))["metadata"])
    assert meta[MARKER] == {"channel_key": "telegram:1:2"} and meta["last_usage"] == {"x": 1}
    with pytest.raises(ValueError):
        await db.set_session_metadata_key("m1", "effort", "high")
    assert await db.set_session_metadata_key("nope", MARKER, {}) is False


@pytest.mark.asyncio
async def test_archive_runs_the_archived_hook(db):
    import asyncio

    from nerve.agent.sessions import SessionManager

    mgr = SessionManager(db)
    seen = []

    async def _hook(sid):
        seen.append(sid)

    mgr._on_archived = _hook
    await db.create_session("a1", title="t", source="web", actor=db.system_actor)
    assert await mgr.archive_session("a1") is True
    await asyncio.sleep(0.01)
    assert seen == ["a1"]
    assert await mgr.archive_session("a1") is False  # already archived: no second call
    await asyncio.sleep(0.01)
    assert seen == ["a1"]


def test_the_engine_emits_turn_events_to_the_router():
    from nerve.agent.engine import AgentEngine, _TurnState

    class _Rec:
        def __init__(self):
            self.events = []

        def turn_started(self, ev):
            self.events.append(ev)

        def turn_finished(self, ev):
            self.events.append(ev)

    engine = AgentEngine.__new__(AgentEngine)
    engine._router = _Rec()
    engine.agents = type("A", (), {"has_live": staticmethod(lambda sid: True)})()
    st = _TurnState()
    st.full_response_text = "hi"
    engine._emit_turn_started("s1", "web", None, False)
    engine._emit_turn_finished("s1", "web", "web", user_message="q", st=st, autonomous=False)
    started, finished = engine._router.events
    assert started == TurnStarted("s1", "web", None, False)
    assert finished.text == "hi" and finished.work_pending is True and finished.status == "completed"
