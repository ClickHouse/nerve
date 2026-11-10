"""What the engine tells channels about a session's turns and its end.

The engine hands these to :class:`~nerve.channels.router.ChannelRouter`,
which passes them to every channel (``on_turn_started``, ``on_turn_finished``,
``on_session_ending``). They carry only what a channel needs to mirror a
session somewhere else, such as a Telegram topic opened for a web session.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TurnStarted:
    session_id: str
    # The turn's source and channel: "web", "cron", "telegram", ...
    source: str
    channel: str | None
    internal: bool = False


@dataclass(frozen=True)
class TurnFinished:
    session_id: str
    source: str
    channel: str | None
    # The message the turn answered; None for an internal trigger or an
    # autonomous continuation (a background task settling).
    user_message: str | None
    # The final answer as stored ("" when the turn showed nothing).
    text: str
    status: str          # "completed", "failed", ...
    autonomous: bool
    # Background agents or tasks of the session still run: another
    # (autonomous) turn may follow.
    work_pending: bool


#: Why a session is going away, for ``on_session_ending``.
SESSION_ARCHIVED = "archived"
SESSION_DELETED = "deleted"
