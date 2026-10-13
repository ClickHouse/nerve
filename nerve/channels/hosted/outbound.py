"""Text, target, reaction, and button helpers for hosted outbound operations.

The gateway closes the whole stream on a frame that fails its checks. The
helpers here keep the values that the agent chooses (targets, file names,
reaction names, and button labels and answers) inside those checks, so a bad
value fails one operation locally.
"""

from __future__ import annotations

import re
from typing import Callable, Mapping

from nerve.channels.hosted.contract.model import (
    MAX_ACTION_ELEMENTS,
    MAX_DISPLAY_NAME_BYTES,
    MAX_IDENTIFIER_BYTES,
    MAX_TEXT_BYTES,
    ActionElement,
    ActionsContent,
    ContentPart,
    ConversationReference,
    MessageContainer,
    MessageReference,
    MessageTarget,
    ThreadReference,
    byte_length,
)

# One code point is at most four UTF-8 bytes, so a part of this many code
# points always fits the contract's text byte limit.
_SAFE_CODE_POINTS = MAX_TEXT_BYTES // 4
# The action ID prefix of a notification answer button, as in the
# self-hosted Slack channel.
NOTIFICATION_ACTION_PREFIX = "notif:"
CUSTOM_REACTION_PREFIX = "custom:"
# Longer emoji names do not fit the gateway's reaction name limit.
_MAX_EMOJI_BYTES = 128
_CONTROL_RE = re.compile("[\x00-\x1f\x7f-\x9f]")


def valid_identifier(value: str, maximum: int = MAX_IDENTIFIER_BYTES) -> bool:
    """Whether the gateway takes *value* as an ID or a label.

    That is 1 to *maximum* UTF-8 bytes, not only white space, and no control
    characters.
    """
    return bool(value.strip()) and byte_length(value) <= maximum and not _CONTROL_RE.search(value)


def file_name(name: str) -> str:
    """*name* as an upload file name: control characters replaced, and cut to the limit."""
    cleaned = clip_bytes(_CONTROL_RE.sub("_", name), MAX_IDENTIFIER_BYTES)
    return cleaned if valid_identifier(cleaned) else "file"


def parse_target(target: str) -> tuple[str, str | None]:
    """Split ``<conversation>[:<thread>]`` into its conversation and thread.

    A target with an ID that the gateway does not take gives ``("", None)``.
    """
    conversation, _, thread = target.partition(":")
    if not valid_identifier(conversation) or (thread and not valid_identifier(thread)):
        return "", None
    return conversation, thread or None


def container_for(conversation_id: str, thread_id: str | None) -> MessageContainer:
    return MessageContainer(
        conversation=ConversationReference(id=conversation_id),
        thread=ThreadReference(id=thread_id) if thread_id else None,
    )


def message_target(conversation_id: str, thread_id: str | None, message_id: str) -> MessageTarget:
    return MessageTarget(
        conversation=ConversationReference(id=conversation_id),
        thread=ThreadReference(id=thread_id) if thread_id else None,
        message=MessageReference(id=message_id),
    )


def split_text(text: str, limit: int) -> list[str]:
    """Split *text* into parts of at most *limit* code points.

    A part ends at the last line break that fits, else at the last space,
    else at the limit. Parts that hold only white space are left out.
    """
    if any(ord(character) > 0x7F for character in text):
        limit = min(limit, _SAFE_CODE_POINTS)
    limit = max(1, min(limit, MAX_TEXT_BYTES))
    parts: list[str] = []
    rest = text
    while len(rest) > limit:
        window = rest[:limit + 1]
        cut = window.rfind("\n")
        if cut <= 0:
            cut = window.rfind(" ")
        if cut <= 0:
            cut = limit
        parts.append(rest[:cut])
        rest = rest[cut:]
        if rest[:1] in ("\n", " "):
            rest = rest[1:]
    parts.append(rest)
    return [part for part in parts if part.strip()]


def truncate_text(text: str, limit: int) -> str:
    """Shorten *text* to at most *limit* code points, with a closing ellipsis."""
    # The ellipsis takes three bytes.
    limit = min(limit, MAX_TEXT_BYTES - 2)
    if any(ord(character) > 0x7F for character in text):
        limit = min(limit, _SAFE_CODE_POINTS)
    if len(text) <= limit:
        return text
    return text[:max(0, limit - 1)] + "…"


def reaction_name(emoji: str, short_names: Mapping[str, str]) -> str | None:
    """The contract reaction name for *emoji*, or ``None``.

    *short_names* maps provider short names, such as ``eyes``, to emoji. A
    short name that is not in it becomes a ``custom:`` provider emoji name.
    An emoji that has a short name becomes the emoji that the short name maps
    to, which is its fully qualified form.
    """
    value = emoji.strip()
    short = value.strip(":")
    if short and short.isascii() and all(c.isalnum() or c in "-_+" for c in short):
        custom = f"{CUSTOM_REACTION_PREFIX}{short}"
        return short_names.get(short) or (custom if len(custom) <= MAX_IDENTIFIER_BYTES else None)
    for name, mapped in short_names.items():
        if value in (mapped, mapped.replace("️", "")):
            return short_names[name]
    if byte_length(value) > _MAX_EMOJI_BYTES or _emoji_sequences(value) != 1:
        return None
    return value


# One emoji sequence, as the gateway reads a reaction name: a flag, a keycap,
# or an emoji base with variation selectors, skin tones, tag characters, and
# zero width joiners to more emoji bases.
_KEYCAP_BASES = frozenset(b"#*0123456789")


def _regional_indicator(c: int) -> bool:
    return 0x1F1E6 <= c <= 0x1F1FF


def _skin_tone(c: int) -> bool:
    return 0x1F3FB <= c <= 0x1F3FF


def _emoji_base(c: int) -> bool:
    if (chr(c).isalpha() and c != 0x2139) or _regional_indicator(c) or _skin_tone(c):
        return False
    return (
        c in (0x00A9, 0x00AE, 0x203C, 0x2049, 0x3030, 0x303D, 0x3297, 0x3299)
        or 0x2100 <= c <= 0x2BFF
        or 0x1F000 <= c <= 0x1FAFF
    )


def _emoji_sequence_length(runes: list[int], start: int) -> int:
    first = runes[start]
    remaining = len(runes) - start
    if _regional_indicator(first):
        return 2 if remaining >= 2 and _regional_indicator(runes[start + 1]) else 0
    if first in _KEYCAP_BASES:
        ok = remaining >= 3 and runes[start + 1] == 0xFE0F and runes[start + 2] == 0x20E3
        return 3 if ok else 0
    if not _emoji_base(first):
        return 0
    length = 1
    while length < remaining:
        current = runes[start + length]
        if current == 0xFE0F or _skin_tone(current):
            length += 1
        elif 0xE0020 <= current <= 0xE007F and first == 0x1F3F4:
            length += 1
        elif current == 0x200D and length + 1 < remaining and _emoji_base(runes[start + length + 1]):
            length += 2
        else:
            return length
    return length


def _emoji_sequences(value: str) -> int:
    """The number of emoji sequences in *value*, or -1 if it holds anything else."""
    runes = [ord(character) for character in value]
    index = sequences = 0
    while index < len(runes):
        length = _emoji_sequence_length(runes, index)
        if length == 0:
            return -1
        index += length
        sequences += 1
    return sequences


def clip_bytes(text: str, maximum: int) -> str:
    """*text* cut to at most *maximum* UTF-8 bytes, on a code point boundary."""
    return text.encode("utf-8", errors="replace")[:maximum].decode("utf-8", errors="ignore")


def notification_actions(
    notification_id: str,
    options: list[tuple[str, str]] | None,
    style: Callable[[str], str] = lambda value: "",
) -> tuple[ContentPart, ...]:
    """One button for each ``(label, value)`` option of a notification.

    The action ID is ``notif:<notification>:<value>`` and the button value
    is the answer. At most the contract's element limit is kept. An option
    whose value or action ID does not fit the contract's limits gets no
    button, so a press never gives a cut answer.
    """
    elements: list[ActionElement] = []
    seen: set[str] = set()
    for label, value in options or []:
        action_id = f"{NOTIFICATION_ACTION_PREFIX}{notification_id}:{value}"
        label = clip_bytes(_CONTROL_RE.sub(" ", label), MAX_DISPLAY_NAME_BYTES)
        fits = (
            valid_identifier(value) and valid_identifier(action_id)
            and valid_identifier(label, MAX_DISPLAY_NAME_BYTES)
        )
        if not fits or action_id in seen:
            continue
        seen.add(action_id)
        elements.append(ActionElement(
            kind="button",
            action_id=action_id,
            label=label,
            value=value,
            style=style(value),
        ))
        if len(elements) == MAX_ACTION_ELEMENTS:
            break
    if not elements:
        return ()
    return (ContentPart(kind="actions", actions=ActionsContent(elements=tuple(elements))),)


def notification_answer(action_id: str, selections: tuple[str, ...]) -> tuple[str, str] | None:
    """The notification ID and the answer of a button press, else ``None``."""
    if not action_id.startswith(NOTIFICATION_ACTION_PREFIX):
        return None
    notification_id, separator, value = action_id[len(NOTIFICATION_ACTION_PREFIX):].partition(":")
    answer = selections[0] if selections else value
    if not notification_id or not separator or not answer:
        return None
    return notification_id, answer


__all__ = [
    "NOTIFICATION_ACTION_PREFIX",
    "clip_bytes",
    "container_for",
    "file_name",
    "message_target",
    "notification_actions",
    "notification_answer",
    "parse_target",
    "reaction_name",
    "split_text",
    "truncate_text",
    "valid_identifier",
]
