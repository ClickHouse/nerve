"""Tests for reply → originating-session routing (nerve.channels.telegram).

When a user replies (Telegram reply) to a message the bot sent, the reply is
routed to the session that produced that message instead of the chat's active
session. The channel records ``message_id -> (chat_id, session_id)`` on every
outbound send and resolves it back on inbound replies.
"""

from types import SimpleNamespace

import pytest

from nerve.channels.base import OutboundMessage
from nerve.channels.telegram import TelegramChannel


class _FakeRouter:
    """Minimal router exposing the one method the channel calls here."""

    def __init__(self, sessions: dict | None = None):
        self._sessions = sessions or {}

    async def get_session(self, session_id: str):
        return self._sessions.get(session_id)


def _make_channel(router: _FakeRouter | None = None) -> TelegramChannel:
    cfg = SimpleNamespace(telegram=SimpleNamespace(allowed_users=[]))
    return TelegramChannel(lambda: cfg, router or _FakeRouter())


# --------------------------------------------------------------------------- #
#  Record / lookup                                                            #
# --------------------------------------------------------------------------- #

def test_record_and_lookup_hit():
    ch = _make_channel()
    ch._record_reply_route(100, 42, "sessA")
    assert ch._lookup_reply_route(100, 42) == "sessA"


def test_lookup_unknown_message_is_none():
    ch = _make_channel()
    assert ch._lookup_reply_route(999, 42) is None


def test_lookup_requires_matching_chat():
    # A message_id recorded for chat 42 must never route a reply seen in
    # another chat (message ids are only unique per chat).
    ch = _make_channel()
    ch._record_reply_route(100, 42, "sessA")
    assert ch._lookup_reply_route(100, 43) is None


def test_empty_session_is_not_recorded():
    ch = _make_channel()
    ch._record_reply_route(100, 42, "")
    assert ch._lookup_reply_route(100, 42) is None


def test_reply_routes_are_lru_bounded():
    ch = _make_channel()
    ch._reply_routes_max = 3
    for mid in range(1, 6):  # 1..5, cap 3 → 1 and 2 evicted
        ch._record_reply_route(mid, 42, f"s{mid}")
    assert ch._lookup_reply_route(1, 42) is None
    assert ch._lookup_reply_route(2, 42) is None
    assert ch._lookup_reply_route(3, 42) == "s3"
    assert ch._lookup_reply_route(5, 42) == "s5"
    assert len(ch._reply_routes) == 3


def test_re_record_refreshes_lru_recency():
    ch = _make_channel()
    ch._reply_routes_max = 2
    ch._record_reply_route(1, 42, "s1")
    ch._record_reply_route(2, 42, "s2")
    ch._record_reply_route(1, 42, "s1")  # touch 1 → 2 is now oldest
    ch._record_reply_route(3, 42, "s3")  # evicts 2
    assert ch._lookup_reply_route(2, 42) is None
    assert ch._lookup_reply_route(1, 42) == "s1"
    assert ch._lookup_reply_route(3, 42) == "s3"


# --------------------------------------------------------------------------- #
#  Resolve (with the live-session guard)                                      #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_resolve_routes_to_live_session():
    ch = _make_channel(_FakeRouter({"sessA": {"status": "active"}}))
    ch._record_reply_route(100, 42, "sessA")
    assert await ch._resolve_reply_session(100, 42) == "sessA"


@pytest.mark.asyncio
async def test_resolve_none_when_not_a_reply():
    ch = _make_channel(_FakeRouter({"sessA": {"status": "active"}}))
    ch._record_reply_route(100, 42, "sessA")
    assert await ch._resolve_reply_session(None, 42) is None


@pytest.mark.asyncio
async def test_resolve_falls_back_when_session_gone():
    # Mapping exists but the session no longer exists → fall back (None).
    ch = _make_channel(_FakeRouter({}))
    ch._record_reply_route(100, 42, "sessA")
    assert await ch._resolve_reply_session(100, 42) is None


@pytest.mark.asyncio
async def test_resolve_falls_back_when_session_archived():
    ch = _make_channel(_FakeRouter({"sessA": {"status": "archived"}}))
    ch._record_reply_route(100, 42, "sessA")
    assert await ch._resolve_reply_session(100, 42) is None


@pytest.mark.asyncio
async def test_resolve_none_for_unmapped_reply():
    ch = _make_channel(_FakeRouter({"sessA": {"status": "active"}}))
    # Replying to a message the bot never recorded (e.g. one the user sent).
    assert await ch._resolve_reply_session(555, 42) is None


# --------------------------------------------------------------------------- #
#  Outbound send() records the route                                          #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_send_records_reply_route():
    ch = _make_channel()

    class _Bot:
        async def send_message(self, **kwargs):
            return SimpleNamespace(message_id=777)

    ch._app = SimpleNamespace(bot=_Bot())
    await ch.send(OutboundMessage(target="42", text="hello", session_id="sX"))
    assert ch._lookup_reply_route(777, 42) == "sX"


@pytest.mark.asyncio
async def test_send_without_session_id_records_nothing():
    ch = _make_channel()

    class _Bot:
        async def send_message(self, **kwargs):
            return SimpleNamespace(message_id=778)

    ch._app = SimpleNamespace(bot=_Bot())
    await ch.send(OutboundMessage(target="42", text="hello"))  # session_id=""
    assert ch._lookup_reply_route(778, 42) is None
