"""Links to pages of the web UI, for text that leaves it.

A message in Telegram or Slack can point back at the web UI only with a full
address, and Nerve cannot tell its own from the address it binds: a reverse
proxy, a tunnel or a host name can stand in front. The address comes from
``gateway.public_url``; without it there are no links.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING
from urllib.parse import quote

if TYPE_CHECKING:
    from nerve.config import NerveConfig

# The longest label of a link, in characters. A session title can be a whole
# sentence, and the label is one line of a chat message.
_LABEL_MAX = 80

# What the Markdown conversions for Telegram and Slack take out of a text
# before they look for links, and how they find a link. Both take out fenced
# code and then inline code. Telegram takes out an expandable blockquote
# before those.
_QUOTE_RE = re.compile(r"<blockquote\s+expandable>(.*?)</blockquote>", re.DOTALL)
_FENCE_RE = re.compile(r"```(\w*)\n?(.*?)```", re.DOTALL)
_CODE_RE = re.compile(r"`([^`]+)`")
_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")


def web_url(config: NerveConfig, path: str) -> str | None:
    """The address of ``path`` in the web UI, or ``None`` without ``gateway.public_url``.

    ``path`` starts with ``/`` and is already encoded.
    """
    base = config.gateway.public_url
    return f"{base}{path}" if base else None


def session_url(config: NerveConfig, session_id: str) -> str | None:
    """The address of a session's page in the web UI, or ``None`` without ``gateway.public_url``.

    The ID is one path segment: a cron session's ID holds ``:``, and every
    character outside the unreserved set is encoded, as the web UI encodes it.
    """
    if not session_id:
        return None
    return web_url(config, f"/chat/{quote(session_id, safe='')}")


def markdown_link(label: str, url: str) -> str:
    """``[label](url)`` in a form that every conversion reads as one link.

    The conversions end the label at the first ``]`` and the address at the
    first ``)``. Brackets in the label become parentheses and parentheses in
    the address are encoded. A backtick in the label becomes ``'``, because
    Telegram refuses code inside a link. The label is one line of at most
    ``_LABEL_MAX`` characters.
    """
    text = " ".join(label.split()).replace("[", "(").replace("]", ")").replace("`", "'")
    if len(text) > _LABEL_MAX:
        text = text[: _LABEL_MAX - 1].rstrip() + "…"
    address = url.replace("(", "%28").replace(")", "%29")
    return f"[{text}]({address})"


def without_code(text: str) -> str:
    """``text`` with its fenced and inline code taken out, as the conversions
    take it out before they read anything else."""
    return _CODE_RE.sub("\x00", _FENCE_RE.sub("\x00", text))


def link_stays_whole(before: str, link: str) -> bool:
    """Whether ``link``, written after ``before``, converts to that link alone.

    The conversions pair the first ``[`` that has no ``]`` after it with the
    next ``](``, and an address without its ``)`` runs on to the next one. So
    an open bracket or an unfinished link in ``before``, which a cut text or a
    log line can hold, would take the appended link as its own end and turn
    both into something else. What a conversion takes out first pairs with
    nothing, and it can hold the ``]`` that would have closed a bracket. The
    link must stay whole both ways: with code taken out, and with an
    expandable blockquote taken out before the code.
    """
    return all(
        _ends_with(plain, link)
        for plain in (without_code(before), without_code(_QUOTE_RE.sub("\x00", before)))
    )


def _ends_with(plain: str, link: str) -> bool:
    """Whether the last link that the link pattern finds in ``plain + link`` is ``link``."""
    whole = plain + link
    last = None
    for last in _LINK_RE.finditer(whole):
        pass
    return last is not None and last.start() == len(plain) and last.end() == len(whole)
