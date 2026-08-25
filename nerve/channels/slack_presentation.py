"""Pure Slack text conversion and Block Kit rendering."""

from __future__ import annotations

import re
from typing import Any

MAX_MSG_LEN = 3900
_MAX_ACTION_ELEMENTS = 25
_SESSIONS_BUTTON_LIMIT = 8
_SESSION_LABEL_MAX = 70
_MAX_SECTION_LEN = 3000
_MAX_SECTION_BLOCKS = 45

_EMOJI_TO_SLACK: dict[str, str] = {
    "👍": "thumbsup",
    "👎": "thumbsdown",
    "❤": "heart",
    "❤️": "heart",
    "🔥": "fire",
    "🥰": "smiling_face_with_3_hearts",
    "👏": "clap",
    "😁": "grin",
    "🤔": "thinking_face",
    "🤯": "exploding_head",
    "😱": "scream",
    "😢": "cry",
    "🎉": "tada",
    "🤩": "star-struck",
    "🙏": "pray",
    "👌": "ok_hand",
    "🥱": "yawning_face",
    "😍": "heart_eyes",
    "🌚": "new_moon_with_face",
    "💯": "100",
    "🤣": "rolling_on_the_floor_laughing",
    "⚡": "zap",
    "🏆": "trophy",
    "💔": "broken_heart",
    "🤨": "face_with_raised_eyebrow",
    "😐": "neutral_face",
    "🍾": "champagne",
    "👀": "eyes",
    "🙈": "see_no_evil",
    "😇": "innocent",
    "🤝": "handshake",
    "🤗": "hugging_face",
    "🫡": "saluting_face",
    "🆒": "cool",
    "😎": "sunglasses",
    "✅": "white_check_mark",
    "❌": "x",
    "⏳": "hourglass_flowing_sand",
    "🚀": "rocket",
    "✍": "writing_hand",
    "🤡": "clown_face",
    "💩": "hankey",
    "😴": "sleeping",
    "👻": "ghost",
}

_APPROVAL_STYLES: dict[str, str] = {
    "approve": "primary",
    "yes": "primary",
    "allow": "primary",
    "decline": "danger",
    "deny": "danger",
    "no": "danger",
    "reject": "danger",
}

_BARE_AMPERSAND_RE = re.compile(r"&(?!(?:amp|lt|gt);)")


def _escape_ampersands(text: str) -> str:
    """Escape ``&`` without double-escaping an existing entity."""
    return _BARE_AMPERSAND_RE.sub("&amp;", text)


def _md_to_slack(text: str) -> str:
    """Convert standard Markdown to Slack mrkdwn."""
    protected: list[str] = []

    def protect(replacement: str) -> str:
        index = len(protected)
        protected.append(replacement)
        return f"\x00{index}\x00"

    def fence(match: re.Match) -> str:
        return protect("```\n" + match.group(2).strip("\n") + "\n```")

    text = re.sub(r"```(\w*)\n?(.*?)```", fence, text, flags=re.DOTALL)
    text = re.sub(r"`([^`]+)`", lambda match: protect(f"`{match.group(1)}`"), text)

    def link(match: re.Match) -> str:
        label = _escape_ampersands(match.group(1))
        url = _escape_ampersands(match.group(2))
        return protect(f"<{url}|{label}>")

    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", link, text)
    text = _escape_ampersands(text).replace("<", "&lt;").replace(">", "&gt;")
    text = re.sub(
        r"^\s{0,3}#{1,6}\s+(.+?)\s*$",
        lambda match: f"\x01{match.group(1)}\x01",
        text,
        flags=re.MULTILINE,
    )
    text = re.sub(
        r"\*\*(.+?)\*\*",
        lambda match: f"\x01{match.group(1)}\x01",
        text,
        flags=re.DOTALL,
    )
    text = re.sub(
        r"(?<![\w*])\*(?!\s)([^*\n]+?)(?<!\s)\*(?![\w*])",
        r"_\1_",
        text,
    )
    text = text.replace("\x01", "*")
    text = re.sub(r"^(\s*)[-+]\s+", r"\1• ", text, flags=re.MULTILINE)

    for index, replacement in enumerate(protected):
        text = text.replace(f"\x00{index}\x00", replacement)
    return text


def slack_to_plain(text: str, bot_user_id: str = "") -> str:
    """Turn Slack wire markup into readable prompt text."""
    if bot_user_id:
        text = re.sub(rf"<@{re.escape(bot_user_id)}(\|[^>]*)?>", "", text)
    text = re.sub(r"<#C[A-Z0-9]+\|([^>]+)>", r"#\1", text)
    text = re.sub(r"<#(C[A-Z0-9]+)>", r"#\1", text)
    text = re.sub(r"<@([UW][A-Z0-9]+)\|([^>]+)>", r"@\2", text)
    text = re.sub(r"<@([UW][A-Z0-9]+)>", r"@\1", text)
    text = re.sub(r"<!subteam\^[A-Z0-9]+\|@?([^>]+)>", r"@\1", text)
    text = re.sub(r"<!(here|channel|everyone)(\|[^>]*)?>", r"@\1", text)
    text = re.sub(r"<([^|>]+)\|([^>]+)>", r"\2 (\1)", text)
    text = re.sub(r"<((?:https?|mailto):[^>]+)>", r"\1", text)
    return text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&").strip()


def split_message(text: str, limit: int = MAX_MSG_LEN) -> list[str]:
    """Split text under *limit*, preferring line boundaries."""
    if len(text) <= limit:
        return [text] if text else []

    chunks: list[str] = []
    current = ""
    for line in text.split("\n"):
        while len(line) > limit:
            if current:
                chunks.append(current)
                current = ""
            chunks.append(line[:limit])
            line = line[limit:]
        if not current:
            current = line
        elif len(current) + 1 + len(line) <= limit:
            current = f"{current}\n{line}"
        else:
            chunks.append(current)
            current = line
    if current:
        chunks.append(current)
    return chunks


def slack_emoji_name(emoji: str) -> str | None:
    """Map a unicode emoji or existing short name to a Slack short name."""
    cleaned = emoji.strip().strip(":")
    if cleaned and all(char.isalnum() or char in "-_+" for char in cleaned):
        return cleaned
    return _EMOJI_TO_SLACK.get(emoji.strip()) or _EMOJI_TO_SLACK.get(
        emoji.strip().rstrip("️"),
    )


def _session_label(session: dict, current_id: str | None) -> str:
    title = (session.get("title") or "").strip() or session.get("id", "?")
    prefix = "✓ " if session.get("id") == current_id else ""
    if session.get("starred"):
        prefix += "⭐ "
    label = f"{prefix}{title}"
    return (
        label[: _SESSION_LABEL_MAX - 1] + "…"
        if len(label) > _SESSION_LABEL_MAX
        else label
    )


def build_sessions_blocks(
    sessions: list[dict],
    current_id: str | None,
) -> list[dict[str, Any]]:
    """Render the ``/nerve sessions`` Block Kit view."""
    blocks: list[dict[str, Any]] = []
    shown = sessions[:_SESSIONS_BUTTON_LIMIT]
    if not shown:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "No sessions yet — start one below.",
                },
            }
        )
    else:
        current_title = next(
            (
                (session.get("title") or session.get("id"))
                for session in shown
                if session.get("id") == current_id
            ),
            None,
        )
        header = "*Sessions* — tap to switch."
        if current_title:
            header += f"\nCurrent: {current_title}"
        header += "\n⭐ keeps a session alive (never auto-closed)."
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": header},
            }
        )
        for session in shown:
            session_id = session.get("id")
            if not session_id:
                continue
            blocks.append(
                {
                    "type": "actions",
                    "block_id": f"sess_row:{session_id}",
                    "elements": [
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": _session_label(session, current_id),
                                "emoji": True,
                            },
                            "action_id": f"sess:{session_id}",
                            "value": session_id,
                        },
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "⭐" if session.get("starred") else "☆",
                                "emoji": True,
                            },
                            "action_id": f"sessstar:{session_id}",
                            "value": session_id,
                        },
                    ],
                }
            )
    blocks.append(
        {
            "type": "actions",
            "block_id": "sess_new",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "➕ New session",
                        "emoji": True,
                    },
                    "action_id": "sess:new",
                    "value": "new",
                    "style": "primary",
                }
            ],
        }
    )
    return blocks


def build_notification_blocks(
    text: str,
    notification_id: str,
    options: list[tuple[str, str]] | None = None,
) -> list[dict[str, Any]]:
    """Render notification text and option buttons as Block Kit."""
    chunks = split_message(_md_to_slack(text), _MAX_SECTION_LEN)
    if len(chunks) > _MAX_SECTION_BLOCKS:
        dropped = sum(len(chunk) for chunk in chunks[_MAX_SECTION_BLOCKS - 1 :])
        chunks = chunks[: _MAX_SECTION_BLOCKS - 1]
        chunks.append(
            f"_… {dropped} more characters — open the notification in the "
            "web UI to read the rest._",
        )
    blocks: list[dict[str, Any]] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": chunk}}
        for chunk in chunks
    ]
    elements = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": label[:75], "emoji": True},
            "action_id": f"notif:{notification_id}:{value}"[:255],
            "value": value[:2000],
            **(
                {"style": _APPROVAL_STYLES[value.lower()]}
                if value.lower() in _APPROVAL_STYLES
                else {}
            ),
        }
        for label, value in (options or [])
    ]
    for start in range(0, len(elements), _MAX_ACTION_ELEMENTS):
        blocks.append(
            {
                "type": "actions",
                "block_id": f"notif:{notification_id}:{start}",
                "elements": elements[start : start + _MAX_ACTION_ELEMENTS],
            }
        )
    return blocks


__all__ = [
    "MAX_MSG_LEN",
    "_MAX_ACTION_ELEMENTS",
    "_MAX_SECTION_BLOCKS",
    "_SESSIONS_BUTTON_LIMIT",
    "_md_to_slack",
    "build_notification_blocks",
    "build_sessions_blocks",
    "slack_emoji_name",
    "slack_to_plain",
    "split_message",
]
