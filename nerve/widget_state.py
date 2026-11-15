"""What a widget keeps: the saved state of a ```widget block, with its chat.

A widget is a piece of HTML the agent writes into an answer; the web chat
runs it in a sandbox frame (``docs/widgets.md``). Its script has a
``localStorage``, which the page stands in for: the frame has no origin and
no storage of the browser's. What the script saves there (the ticks of a
checklist, an entered value, the chosen tab) the page sends to Nerve, and
Nerve keeps it with the chat: in the table ``widget_states``, one row for
each widget of a session that has saved something.

This module holds what the routes, the database and the agent's tool share:
the limits, and the one check every write goes through.

- A widget is named by its id: the first 32 hex digits of the SHA-256 of
  its source, which the page computes (``widget_id``).
- Its state is a list of pairs of texts, key and value, each key once. The
  texts are the widget's own. Nerve reads nothing in them.
- Sizes count as the widget's script counts them and as browsers count
  storage: in UTF-16 code units, keys and values together.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

#: The agent's tool that reads the saved state of a chat's widgets
#: (``nerve/agent/tools/handlers/widgets.py``).
WIDGET_STATE_TOOL = "widget_state"

#: What one widget may keep: the characters of its keys and values together.
#: The widget's own script is refused more with a ``QuotaExceededError``, so a
#: write within these bounds is all an honest page ever sends.
MAX_WIDGET_CHARS = 65_536
#: How many keys one widget may have.
MAX_WIDGET_KEYS = 1_024
#: What the widgets of one chat may keep together.
MAX_CHAT_CHARS = 1_048_576
#: How many widgets of one chat may have saved state.
MAX_CHAT_WIDGETS = 200
#: The body of a write. A state within the bounds above is smaller: JSON
#: takes at most six bytes for a character and a few for each pair.
MAX_BODY_BYTES = 512 * 1024

_WIDGET_ID = re.compile(r"[0-9a-f]{32}")


class WidgetStateError(Exception):
    """A write that is refused: ``code`` says why, ``status`` is its HTTP status."""

    def __init__(self, code: str, status: int = 400) -> None:
        super().__init__(code)
        self.code = code
        self.status = status


def widgets_drawn(config: Any) -> bool:
    """True on an instance whose web chat draws widgets: MCP Apps is on, in
    local mode. The switch the system prompt's widget sentences follow
    (``AgentEngine``)."""
    from nerve.gateway.auth import is_external_mode  # local import: no cycle

    return bool(getattr(config, "mcp_apps_active", False)) and not is_external_mode()


def is_widget_id(value: Any) -> bool:
    """True for the id of a widget: 32 lowercase hex digits."""
    return isinstance(value, str) and _WIDGET_ID.fullmatch(value) is not None


def widget_id(source: str) -> str:
    """The id of the widget with this source, as the web UI computes it:
    of the text's UTF-16 code units, low byte first."""
    return hashlib.sha256(source.encode("utf-16-le", "surrogatepass")).hexdigest()[:32]


def text_length(text: str) -> int:
    """The length of a text as a script counts it: in UTF-16 code units."""
    return len(text.encode("utf-16-le", "surrogatepass")) // 2


def read_items(raw: Any) -> tuple[list[list[str]], int]:
    """The items of a write, checked, and their size in characters.

    Raises :class:`WidgetStateError`: 400 ``bad_items`` for anything that is
    not a list of pairs of texts with each key once, 413 ``too_many_keys``
    or ``widget_too_large`` for more than one widget may keep.
    """
    if not isinstance(raw, list):
        raise WidgetStateError("bad_items")
    if len(raw) > MAX_WIDGET_KEYS:
        raise WidgetStateError("too_many_keys", 413)
    items: list[list[str]] = []
    keys: set[str] = set()
    size = 0
    for pair in raw:
        if not isinstance(pair, list) or len(pair) != 2:
            raise WidgetStateError("bad_items")
        key, value = pair
        if not isinstance(key, str) or not isinstance(value, str) or key in keys:
            raise WidgetStateError("bad_items")
        size += text_length(key) + text_length(value)
        if size > MAX_WIDGET_CHARS:
            raise WidgetStateError("widget_too_large", 413)
        keys.add(key)
        items.append([key, value])
    return items, size


def encode_items(items: list[list[str]]) -> str:
    """The items as the text the database keeps: JSON in ASCII, so that a
    text with half a surrogate pair or a NUL in it is stored as it came."""
    return json.dumps(items, ensure_ascii=True, separators=(",", ":"))


def decode_items(text: str) -> list[list[str]]:
    """The items of a stored row."""
    return json.loads(text)
