"""V73: what the widgets of a chat saved.

A ```widget block of an answer runs in the web chat and has a
``localStorage`` (``docs/widgets.md``). What its script saves there is kept
with the chat, in the table ``widget_states`` this migration adds. One row
is one widget of one session that has saved something:

- ``session_id``: the chat. The row goes with it: ``delete_session`` removes
  it, and the foreign key's cascade is the second line of defence.
- ``widget_id``: the first 32 hex digits of the SHA-256 of the widget's
  source, which the web UI computes. The same source in another chat is
  another row.
- ``items``: the widget's keys and values, a JSON list of ``[key, value]``
  pairs in ASCII. The texts are the widget's own; Nerve reads nothing in
  them.
- ``size``: the characters of the keys and values together (UTF-16 code
  units, as the widget's script counts them). The limits of
  ``nerve/widget_state.py`` are checked against it.
- ``updated_at``: the time of the last change (ISO 8601, UTC).

``CREATE ... IF NOT EXISTS`` makes a second run do nothing.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

SQL = """
CREATE TABLE IF NOT EXISTS widget_states (
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    widget_id  TEXT NOT NULL,
    items      TEXT NOT NULL,
    size       INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (session_id, widget_id)
);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    logger.info("v073: widget_states ready")
