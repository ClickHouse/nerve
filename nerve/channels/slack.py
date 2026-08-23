"""Slack bot channel — receive messages, run agent, respond.

Uses slack_sdk Socket Mode: the bot opens an outbound WebSocket to Slack, so
no public URL and no inbound firewall hole are needed. That is the same shape
as the Telegram channel's long-polling transport, and it keeps a self-hosted
Nerve reachable from behind NAT.

Session management is delegated to ChannelRouter. Access control is not:
a Slack workspace carries traffic the operator never meant for the agent, so
every inbound event passes :class:`~nerve.channels.access.AccessPolicy`
before it becomes an InboundMessage. See :mod:`nerve.channels.access`.

Addressing
----------
A Slack conversation is a channel id, optionally narrowed to one thread. The
two are packed into a single ``target`` string — ``C0456DEF`` or
``C0456DEF:1699887766.123456`` — because :class:`BaseChannel` gives a channel
one opaque address per destination. ``channel_key`` is ``slack:<target>``, so
with ``reply_in_thread`` on, each thread is its own session and two people can
run separate conversations in one channel.
"""

from __future__ import annotations

import asyncio
import base64
import collections
import logging
import re
import time
from pathlib import Path
from typing import Any, Callable, TYPE_CHECKING

from nerve.channels.access import AccessPolicy, Identity, needs_name_resolution
from nerve.channels.archives import (
    IMAGE_EXT_TO_MIME,
    MAX_TEXT_SIZE,
    TEXT_EXTENSIONS,
    extract_zip,
)
from nerve.channels.base import (
    BaseChannel,
    ChannelCapability,
    ChannelConstraints,
    InboundMessage,
    OutboundMessage,
)
from nerve.config import (
    SLACK_ALL_COMMANDS,
    SLACK_DEFAULT_COMMANDS,
    NerveConfig,
)

if TYPE_CHECKING:
    from nerve.channels.router import ChannelRouter

logger = logging.getLogger(__name__)

# chat.postMessage accepts 40k chars but renders only the first ~4k as a
# single block, so split well below that and let each chunk stand alone.
MAX_MSG_LEN = 3900
# chat.update is limited to roughly one call per second per channel.
EDIT_INTERVAL = 1.2
# Watchdog: check every 30s, log heartbeat every ~5 min.
WATCHDOG_INTERVAL = 30
WATCHDOG_HEARTBEAT_EVERY = 10
# Bounded caches: event dedupe, message text for reaction context, resolved names.
_DEDUPE_MAX = 500
_MESSAGE_CACHE_MAX = 200
_NAME_CACHE_MAX = 500
_INBOUND_TS_MAX = 500
_NAME_CACHE_TTL = 600.0
# Concurrent dispatch tasks. The router serialises per session, so this only
# bounds envelopes not yet routed — including ones headed for a refusal.
_MAX_INFLIGHT = 100
# Slack renders at most 25 elements in one actions block.
_MAX_ACTION_ELEMENTS = 25
# Star picker action ids: ``starpick:<1|0>:<session id>``. Distinct from the
# session card's ``sessstar:`` toggle, which flips whatever the row holds —
# a picker has to set the state the command asked for.
_STAR_ACTION_PREFIX = "starpick:"

# Message subtypes that are not a person talking. ``file_share`` and
# ``thread_broadcast`` are absent on purpose: the first is a real message that
# happens to carry an attachment, the second is a thread reply the sender also
# sent to the channel ("also send to #channel"), which is exactly how someone
# continues a running agent thread in the open.
_IGNORED_SUBTYPES = frozenset({
    "bot_message",
    "message_changed",
    "message_deleted",
    "message_replied",
    "channel_join",
    "channel_leave",
    "channel_topic",
    "channel_purpose",
    "channel_name",
    "channel_archive",
    "channel_unarchive",
})

_MAX_TEXT_SIZE = MAX_TEXT_SIZE     # inline text cap
_MAX_DOWNLOAD_SIZE = 20_000_000    # refuse to pull anything larger into a prompt

_TEXT_EXTENSIONS = TEXT_EXTENSIONS
_IMAGE_EXT_TO_MIME = IMAGE_EXT_TO_MIME

# Unicode emoji → Slack short name. The agent's set_reaction tool speaks the
# Telegram reaction vocabulary; reactions.add only accepts short names. An
# emoji outside this table is skipped rather than guessed at, so a reaction
# never silently lands as the wrong one.
_EMOJI_TO_SLACK: dict[str, str] = {
    "👍": "thumbsup", "👎": "thumbsdown", "❤": "heart", "❤️": "heart",
    "🔥": "fire", "🥰": "smiling_face_with_3_hearts", "👏": "clap",
    "😁": "grin", "🤔": "thinking_face", "🤯": "exploding_head",
    "😱": "scream", "😢": "cry", "🎉": "tada", "🤩": "star-struck",
    "🙏": "pray", "👌": "ok_hand", "🥱": "yawning_face", "😍": "heart_eyes",
    "🌚": "new_moon_with_face", "💯": "100", "🤣": "rolling_on_the_floor_laughing",
    "⚡": "zap", "🏆": "trophy", "💔": "broken_heart", "🤨": "face_with_raised_eyebrow",
    "😐": "neutral_face", "🍾": "champagne", "👀": "eyes", "🙈": "see_no_evil",
    "😇": "innocent", "🤝": "handshake", "🤗": "hugging_face", "🫡": "saluting_face",
    "🆒": "cool", "😎": "sunglasses", "✅": "white_check_mark", "❌": "x",
    "⏳": "hourglass_flowing_sand", "🚀": "rocket", "✍": "writing_hand",
    "🤡": "clown_face", "💩": "hankey", "😴": "sleeping", "👻": "ghost",
}


# ---------------------------------------------------------------------- #
#  Pure helpers — module level so they are testable without a transport   #
# ---------------------------------------------------------------------- #


# Slack object ids: a type letter then uppercase alphanumerics. U/W users,
# B bots, C/G/D/T conversations and teams. Used to decide whether an
# allow/deny pattern can be matched against the id alone, so the shape has to
# be exact — anything looser skips a name lookup a deny list depends on.
_SLACK_ID_RE = re.compile(r"^[UWBCDGT][A-Z0-9]{7,}$")


def is_slack_id(pattern: str) -> bool:
    """Whether *pattern* is a literal Slack object id rather than a name."""
    return bool(_SLACK_ID_RE.match(pattern))


# A bare & — one that is not already the start of an escape Slack recognises.
_BARE_AMPERSAND_RE = re.compile(r"&(?!(?:amp|lt|gt);)")


def _escape_ampersands(text: str) -> str:
    """Escape ``&`` without double-escaping one that is already an entity."""
    return _BARE_AMPERSAND_RE.sub("&amp;", text)


def format_target(channel_id: str, thread_ts: str | None = None) -> str:
    """Pack a conversation address into one opaque target string."""
    return f"{channel_id}:{thread_ts}" if thread_ts else channel_id


def parse_target(target: str) -> tuple[str, str | None]:
    """Unpack :func:`format_target` into ``(channel_id, thread_ts)``.

    Slack channel ids never contain a colon and a thread ts is always
    ``<seconds>.<microseconds>``, so the split is unambiguous.
    """
    channel_id, sep, thread_ts = target.partition(":")
    return channel_id, (thread_ts if sep and thread_ts else None)


def _md_to_slack(text: str) -> str:
    """Convert standard Markdown to Slack mrkdwn.

    Slack's flavour collides with Markdown on the two most common markers:
    ``*text*`` is bold rather than italic, and ``_text_`` is the only italic.
    Links are ``<url|label>``. Headings and tables do not exist, so headings
    become bold lines.

    Code spans and fences are lifted out first and restored last, so the
    substitutions never rewrite code — the failure that makes a snippet of
    Python containing ``**kwargs`` render as bold.
    """
    protected: list[str] = []

    def _protect(replacement: str) -> str:
        idx = len(protected)
        protected.append(replacement)
        return f"\x00{idx}\x00"

    def _fence(m: re.Match) -> str:
        # Slack has no language tag — it would render as the first line of
        # the block — and needs the newline after the opening fence kept,
        # or the whole block collapses onto one line.
        return _protect("```\n" + m.group(2).strip("\n") + "\n```")
    text = re.sub(r"```(\w*)\n?(.*?)```", _fence, text, flags=re.DOTALL)

    def _code(m: re.Match) -> str:
        return _protect(f"`{m.group(1)}`")
    text = re.sub(r"`([^`]+)`", _code, text)

    def _link(m: re.Match) -> str:
        # Slack escapes & inside a link too, and rewrites the message if we
        # do not. Doing it here keeps what we send byte-identical to what
        # Slack stores, so a later edit does not fight the normalisation.
        label = _escape_ampersands(m.group(1))
        url = _escape_ampersands(m.group(2))
        return _protect(f"<{url}|{label}>")
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", _link, text)

    # Slack requires these three escaped in message text; everything else is
    # literal. Do it before adding markup so the markup itself survives.
    text = _escape_ampersands(text).replace("<", "&lt;").replace(">", "&gt;")

    # Headings have no equivalent — render the text as a bold line. Bold is
    # staged behind \x01 until the italic pass has run, so a bold marker is
    # never re-read as a pair of italic ones.
    text = re.sub(
        r"^\s{0,3}#{1,6}\s+(.+?)\s*$",
        lambda m: f"\x01{m.group(1)}\x01",
        text,
        flags=re.MULTILINE,
    )
    text = re.sub(
        r"\*\*(.+?)\*\*", lambda m: f"\x01{m.group(1)}\x01", text, flags=re.DOTALL,
    )
    text = re.sub(r"(?<![\w*])\*(?!\s)([^*\n]+?)(?<!\s)\*(?![\w*])", r"_\1_", text)
    text = text.replace("\x01", "*")

    # Markdown bullets render as literal dashes in Slack.
    text = re.sub(r"^(\s*)[-+]\s+", r"\1• ", text, flags=re.MULTILINE)

    for i, repl in enumerate(protected):
        text = text.replace(f"\x00{i}\x00", repl)

    return text


def slack_to_plain(text: str, bot_user_id: str = "") -> str:
    """Turn Slack's wire format into something worth putting in a prompt.

    Unwraps ``<url|label>`` and ``<@U123>`` markup, drops the bot's own
    mention (the agent does not need to be told it was addressed), and
    unescapes the three reserved entities.
    """
    if bot_user_id:
        text = re.sub(rf"<@{re.escape(bot_user_id)}(\|[^>]*)?>", "", text)
    # Entity forms first — each is a <…|…> too, so the generic link rule
    # would otherwise claim them and render "#general" as "general (#C1)".
    text = re.sub(r"<#C[A-Z0-9]+\|([^>]+)>", r"#\1", text)
    text = re.sub(r"<#(C[A-Z0-9]+)>", r"#\1", text)
    text = re.sub(r"<@([UW][A-Z0-9]+)\|([^>]+)>", r"@\2", text)
    text = re.sub(r"<@([UW][A-Z0-9]+)>", r"@\1", text)
    text = re.sub(r"<!subteam\^[A-Z0-9]+\|@?([^>]+)>", r"@\1", text)
    text = re.sub(r"<!(here|channel|everyone)(\|[^>]*)?>", r"@\1", text)
    text = re.sub(r"<([^|>]+)\|([^>]+)>", r"\2 (\1)", text)
    text = re.sub(r"<((?:https?|mailto):[^>]+)>", r"\1", text)
    text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
    return text.strip()


def split_message(text: str, limit: int = MAX_MSG_LEN) -> list[str]:
    """Split *text* into chunks under *limit*, preferring line boundaries.

    A hard slice mid-line breaks code fences and lists across messages, so
    lines are packed greedily and only a single over-long line is cut.
    """
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
    """Map a unicode emoji (or an already-short name) to a Slack short name."""
    cleaned = emoji.strip().strip(":")
    if cleaned and all(c.isalnum() or c in "-_+" for c in cleaned):
        return cleaned
    return _EMOJI_TO_SLACK.get(emoji.strip()) or _EMOJI_TO_SLACK.get(
        emoji.strip().rstrip("️"),
    )


# Block Kit rendering ---------------------------------------------------- #

_SESSIONS_BUTTON_LIMIT = 8
_SESSION_LABEL_MAX = 70          # Slack button text is capped at 75 chars


def _session_label(session: dict, current_id: str | None) -> str:
    """Button label for one session: current marked ✓, starred marked ⭐."""
    title = (session.get("title") or "").strip() or session.get("id", "?")
    prefix = "✓ " if session.get("id") == current_id else ""
    if session.get("starred"):
        prefix += "⭐ "
    label = f"{prefix}{title}"
    if len(label) > _SESSION_LABEL_MAX:
        label = label[: _SESSION_LABEL_MAX - 1] + "…"
    return label


def build_sessions_blocks(
    sessions: list[dict], current_id: str | None,
) -> list[dict[str, Any]]:
    """Render the ``/nerve sessions`` Block Kit view (pure, sync — testable).

    One tap-to-switch button per session with the id carried in ``value``,
    a ⭐ toggle beside it, and a trailing "New session" button. Switching
    away leaves the previous session running; its output still reaches the
    conversation it was bound to.
    """
    blocks: list[dict[str, Any]] = []
    shown = sessions[:_SESSIONS_BUTTON_LIMIT]

    if not shown:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "No sessions yet — start one below."},
        })
    else:
        current_title = next(
            (
                (s.get("title") or s.get("id"))
                for s in shown
                if s.get("id") == current_id
            ),
            None,
        )
        header = "*Sessions* — tap to switch."
        if current_title:
            header += f"\nCurrent: {current_title}"
        header += "\n⭐ keeps a session alive (never auto-closed)."
        blocks.append({
            "type": "section", "text": {"type": "mrkdwn", "text": header},
        })
        for s in shown:
            sid = s.get("id")
            if not sid:
                continue
            blocks.append({
                "type": "actions",
                "block_id": f"sess_row:{sid}",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": _session_label(s, current_id),
                            "emoji": True,
                        },
                        "action_id": f"sess:{sid}",
                        "value": sid,
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "⭐" if s.get("starred") else "☆",
                            "emoji": True,
                        },
                        "action_id": f"sessstar:{sid}",
                        "value": sid,
                    },
                ],
            })

    blocks.append({
        "type": "actions",
        "block_id": "sess_new",
        "elements": [{
            "type": "button",
            "text": {"type": "plain_text", "text": "➕ New session", "emoji": True},
            "action_id": "sess:new",
            "value": "new",
            "style": "primary",
        }],
    })
    return blocks


# One section block holds 3000 chars, and one message holds 50 blocks. The
# section budget leaves room for the option rows below them.
_MAX_SECTION_LEN = 3000
_MAX_SECTION_BLOCKS = 45

# Slack renders a styled button in green or red. Keys are the canonical
# approval ``value`` strings that NotificationService sends.
_APPROVAL_STYLES: dict[str, str] = {
    "approve": "primary", "yes": "primary", "allow": "primary",
    "decline": "danger", "deny": "danger", "no": "danger", "reject": "danger",
}


def build_notification_blocks(
    text: str,
    notification_id: str,
    options: list[tuple[str, str]] | None = None,
) -> list[dict[str, Any]]:
    """Render a notification card, with one button per option.

    ``options`` is a list of ``(label, value)``. The value rides in the
    button's ``value`` field and the notification id in ``action_id``;
    Slack allows 2000 chars for each, so neither needs the truncation
    Telegram's 64-byte ``callback_data`` forces.

    3000 characters is the limit on one section, not on the message, so a
    long body is spread over several sections at line boundaries. Only a
    body past the whole-message block limit loses anything, and it says so.
    """
    chunks = split_message(_md_to_slack(text), _MAX_SECTION_LEN)
    if len(chunks) > _MAX_SECTION_BLOCKS:
        dropped = sum(len(c) for c in chunks[_MAX_SECTION_BLOCKS - 1:])
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
    # Slack rejects the whole message with invalid_blocks past 25 elements in
    # one actions block, so a long option list is spread over several rows.
    for start in range(0, len(elements), _MAX_ACTION_ELEMENTS):
        chunk = elements[start:start + _MAX_ACTION_ELEMENTS]
        blocks.append({
            "type": "actions",
            "block_id": f"notif:{notification_id}:{start}",
            "elements": chunk,
        })
    return blocks


class SlackChannel(BaseChannel):
    """Slack bot channel over Socket Mode.

    Owns the WebSocket transport, event dispatch, and authorization.
    Session management and agent execution belong to the ChannelRouter.
    """

    def __init__(
        self, config: Callable[[], NerveConfig], router: ChannelRouter,
    ):
        self._config = config
        self.router = router
        self._client: Any = None            # AsyncSocketModeClient
        self._web: Any = None               # AsyncWebClient
        self._bot_user_id: str = ""
        self._bot_id: str = ""     # the app's own bot_id, to spot our own posts
        self._notification_service = None   # Set after service is created
        self._watchdog_task: asyncio.Task | None = None
        self._stopping = False
        self._last_event_time: float = 0.0  # monotonic, set on any inbound envelope
        # Envelopes are dispatched off the ack path; hold a strong reference so
        # the loop cannot collect a task mid-flight.
        self._inflight: set[asyncio.Task] = set()
        # Slack redelivers an event when an ack is slow, and a workspace
        # subscribed to both message.channels and app_mention sees the same
        # message twice. Both collapse to one run here.
        self._seen_events: collections.OrderedDict[str, None] = collections.OrderedDict()
        # message ts -> (target, text snippet) for reaction context.
        self._message_cache: collections.OrderedDict[str, tuple[str, str]] = (
            collections.OrderedDict()
        )
        # target -> ts of the last inbound message, for the read-receipt ack.
        # With reply_in_thread on, every thread is a distinct target, so this
        # and the name cache are bounded rather than left to grow per thread.
        self._last_inbound_ts: collections.OrderedDict[str, str] = (
            collections.OrderedDict()
        )
        # Resolved names: id -> (Identity, monotonic deadline).
        self._name_cache: collections.OrderedDict[str, tuple[Identity, float]] = (
            collections.OrderedDict()
        )

    def set_notification_service(self, service) -> None:
        """Wire the notification service for button presses."""
        self._notification_service = service

    @property
    def config(self) -> NerveConfig:
        """The live config, resolved per read rather than captured.

        The bot outlives every reload and the guardrail lists decide, on each
        event, whether a message reaches the agent. Reading them per use means
        a reload that tightens ``deny_users`` takes effect immediately. The
        tokens are handed to the transport at connect time, so those still
        need a restart — they are listed in ``config_reload`` as such.
        """
        return self._config()

    @property
    def policy(self) -> AccessPolicy:
        """The access policy, rebuilt per read so reloads apply at once."""
        cfg = self.config.slack
        return AccessPolicy.from_lists(
            allow_users=cfg.allow_users,
            deny_users=cfg.deny_users,
            allow_channels=cfg.allow_channels,
            deny_channels=cfg.deny_channels,
        )

    @property
    def enabled_commands(self) -> frozenset[str]:
        """The `/nerve` subcommands this workspace may run.

        Resolved per use so narrowing the list takes effect on a reload.
        """
        configured = self.config.slack.commands
        if configured is None:
            return frozenset(SLACK_DEFAULT_COMMANDS)
        # "all" is expanded here as well as in config parsing: this is the
        # enforcement point, and it is reached by any SlackConfig, including
        # one built directly rather than through from_dict.
        names = {str(n).strip().lstrip("/").lower() for n in configured}
        if names & {"all", "*"}:
            return frozenset(SLACK_ALL_COMMANDS)
        return frozenset(names & set(SLACK_ALL_COMMANDS))

    @property
    def name(self) -> str:
        return "slack"

    @property
    def capabilities(self) -> ChannelCapability:
        caps = (
            ChannelCapability.SEND_TEXT
            | ChannelCapability.MARKDOWN
            | ChannelCapability.TYPING_INDICATOR
            | ChannelCapability.REACTIONS
            | ChannelCapability.SEND_FILES
        )
        if self.config.slack.stream_mode == "partial":
            caps |= ChannelCapability.STREAMING
        return caps

    @property
    def constraints(self) -> ChannelConstraints:
        return ChannelConstraints(
            max_message_length=MAX_MSG_LEN,
            min_edit_interval=EDIT_INTERVAL,
            supports_message_edit=True,
        )

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Connect the Socket Mode client and start dispatching events."""
        cfg = self.config.slack
        if not cfg.bot_token or not cfg.app_token:
            logger.warning(
                "Slack needs both bot_token (xoxb-…) and app_token (xapp-…) — "
                "channel not started",
            )
            return

        from slack_sdk.http_retry.builtin_async_handlers import (
            AsyncRateLimitErrorRetryHandler,
        )
        from slack_sdk.web.async_client import AsyncWebClient

        self._stopping = False
        self._web = AsyncWebClient(token=cfg.bot_token)
        # The SDK retries connection errors out of the box but not 429s, and
        # Slack rate limits chat.postMessage to roughly one call per second
        # per channel. A streamed reply arrives as a burst of edits followed
        # by a post, so without this the last message of a long answer is the
        # one most likely to be dropped.
        self._web.retry_handlers.append(
            AsyncRateLimitErrorRetryHandler(max_retry_count=3),
        )

        auth = await self._web.auth_test()
        self._bot_user_id = auth.get("user_id", "")
        self._bot_id = auth.get("bot_id", "")
        logger.info(
            "Slack authenticated as %s (%s, %s) in workspace %s",
            auth.get("user"), self._bot_user_id, self._bot_id, auth.get("team"),
        )

        self._client = self._build_socket_client()
        await self._client.connect()
        self._last_event_time = time.monotonic()
        logger.info("Slack Socket Mode connected")

        self._announce_auth_state()

        self._watchdog_task = asyncio.create_task(
            self._run_watchdog(), name="slack-socket-watchdog",
        )

    def _build_socket_client(self):
        """A fresh Socket Mode client wired to this channel's dispatcher."""
        from slack_sdk.socket_mode.aiohttp import SocketModeClient

        client = SocketModeClient(
            app_token=self.config.slack.app_token,
            web_client=self._web,
            # Slack closes and reissues a socket roughly hourly; without this
            # the channel goes quiet until the daemon restarts.
            auto_reconnect_enabled=True,
        )
        client.socket_mode_request_listeners.append(self._on_request)
        return client

    def _announce_auth_state(self) -> None:
        """Log how access is configured — loudly when it lets nobody in.

        An unconfigured policy refuses every message. That is the safe
        default but an invisible one, so say it plainly at startup instead
        of leaving the operator to wonder why the bot never answers.
        """
        policy = self.policy
        if not policy.configured:
            logger.warning(
                "Slack: no slack.allow_users or slack.allow_channels configured "
                "— every message will be refused. Add your Slack member id "
                "(Profile → ⋮ → Copy member ID) to slack.allow_users.",
            )
            return
        logger.info("Slack access policy: %s", policy.describe())

    async def stop(self) -> None:
        self._stopping = True
        if self._watchdog_task and not self._watchdog_task.done():
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
        # Cancel and then wait: closing the socket out from under a dispatch
        # still touching the router or the web client races teardown.
        inflight = list(self._inflight)
        for task in inflight:
            task.cancel()
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)
        if self._client:
            try:
                await self._client.close()
            except Exception as e:
                logger.warning("Slack socket close raised: %s", e)

    # ------------------------------------------------------------------ #
    #  Watchdog                                                            #
    # ------------------------------------------------------------------ #

    async def _run_watchdog(self) -> None:
        """Reconnect the socket when Slack's own auto-reconnect gives up."""
        check_count = 0
        while not self._stopping:
            try:
                await asyncio.sleep(WATCHDOG_INTERVAL)
            except asyncio.CancelledError:
                break
            if self._client is None or self._stopping:
                break

            check_count += 1
            # is_connected() is a coroutine: it pings the socket rather than
            # reading a flag.
            connected = bool(await self._client.is_connected())
            if check_count % WATCHDOG_HEARTBEAT_EVERY == 0:
                since = time.monotonic() - self._last_event_time
                logger.info(
                    "Slack watchdog: %s (check #%d, last event %.0fs ago)",
                    "connected" if connected else "disconnected", check_count, since,
                )
            if connected:
                continue

            logger.warning("Slack socket is down — rebuilding")
            try:
                await self._rebuild()
                self._last_event_time = time.monotonic()
                logger.info("Slack socket reconnected")
            except Exception as e:
                logger.error("Slack reconnect failed: %s", e, exc_info=True)

    async def _rebuild(self) -> None:
        """Replace the socket, closing the old one first.

        Calling ``connect()`` again on a live client leaves the previous
        session running: Slack hands each event to exactly one of an app's
        open connections, so the orphan silently takes a share of the
        traffic and the agent sees only part of its own conversation. The
        old client is therefore closed before a new one is built.

        A brief gap is the safe trade. Slack redelivers an unacked envelope,
        while a split connection loses events with no sign anything is wrong.
        """
        old = self._client
        if old is not None:
            try:
                await asyncio.wait_for(old.close(), timeout=10)
            except Exception as e:
                logger.warning("Slack: closing the old socket raised: %s", e)

        self._client = self._build_socket_client()
        await self._client.connect()

    def _touch(self) -> None:
        """Record that an envelope arrived from Slack."""
        self._last_event_time = time.monotonic()

    # ------------------------------------------------------------------ #
    #  Envelope dispatch                                                   #
    # ------------------------------------------------------------------ #

    async def _on_request(self, client: Any, req: Any) -> None:
        """Ack the envelope, then handle it off the ack path.

        Slack retries anything not acked within three seconds, and an agent
        turn takes far longer than that. Acking first and dispatching to a
        task is what stops one message becoming three runs.
        """
        self._touch()
        from slack_sdk.socket_mode.response import SocketModeResponse

        try:
            await client.send_socket_mode_response(
                SocketModeResponse(envelope_id=req.envelope_id),
            )
        except Exception as e:
            logger.warning("Slack ack failed for %s: %s", req.envelope_id, e)

        # The envelope is acked, so Slack will not resend it. Dropping past
        # the cap is therefore a real loss, but a bounded one: without it a
        # burst — including one made entirely of messages the policy will
        # refuse — allocates dispatch tasks and Slack lookups without limit.
        if len(self._inflight) >= _MAX_INFLIGHT:
            logger.warning(
                "Slack: %d envelopes already in flight — dropping %s",
                len(self._inflight), req.type,
            )
            return

        task = asyncio.create_task(self._dispatch(req))
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)

    async def _dispatch(self, req: Any) -> None:
        """Route one Socket Mode envelope to its handler."""
        try:
            if req.type == "events_api":
                await self._handle_event(req.payload.get("event") or {})
            elif req.type == "interactive":
                await self._handle_interactive(req.payload or {})
            elif req.type == "slash_commands":
                await self._handle_slash_command(req.payload or {})
        except Exception as e:
            logger.error(
                "Slack dispatch failed for %s: %s", req.type, e, exc_info=True,
            )

    def _is_duplicate(self, key: str) -> bool:
        """True if this event id was already handled (bounded LRU)."""
        if key in self._seen_events:
            return True
        self._seen_events[key] = None
        while len(self._seen_events) > _DEDUPE_MAX:
            self._seen_events.popitem(last=False)
        return False

    # ------------------------------------------------------------------ #
    #  Authorization                                                       #
    # ------------------------------------------------------------------ #

    async def _identify_user(
        self, user_id: str, resolve: bool, need_email: bool = False,
    ) -> Identity:
        """Build the Identity for a member, looking up names only if needed.

        ``need_email`` says the deny list names an email address. Slack omits
        ``profile.email`` when the token lacks ``users:read.email`` and still
        answers 200, so an absent email there is indistinguishable from a
        user who has none — either way the candidate set is short of what the
        deny list needs, and the identity is marked incomplete.
        """
        if not resolve:
            return Identity(id=user_id)
        cache_key = f"u:{user_id}:{int(need_email)}"
        cached = self._name_cache.get(cache_key)
        if cached and cached[1] > time.monotonic():
            return cached[0]
        try:
            info = await self._web.users_info(user=user_id)
            user = info.get("user") or {}
            profile = user.get("profile") or {}
            email = profile.get("email")
            names = tuple(
                n for n in (
                    user.get("name"),
                    profile.get("display_name"),
                    profile.get("real_name"),
                    email,
                ) if n
            )
            complete = bool(names) and (email is not None or not need_email)
            if need_email and email is None:
                logger.warning(
                    "Slack users.info returned no email for %s — the deny list "
                    "names one, so the user is refused. Grant users:read.email "
                    "or write the deny rule against the handle or id instead.",
                    user_id,
                )
            identity = Identity(id=user_id, names=names, complete=complete)
        except Exception as e:
            logger.warning("Slack users.info failed for %s: %s", user_id, e)
            identity = Identity(id=user_id, complete=False)
        self._remember(
            self._name_cache, cache_key,
            (identity, time.monotonic() + _NAME_CACHE_TTL), _NAME_CACHE_MAX,
        )
        return identity

    async def _identify_conversation(
        self, channel_id: str, channel_type: str, resolve: bool,
    ) -> Identity:
        """Build the Identity for a conversation.

        A direct message has no name, so it is given the synthetic name
        ``dm`` — that is how ``allow_channels: ["dm", "eng-*"]`` admits
        direct messages alongside a set of channels.
        """
        if channel_type == "im":
            return Identity(id=channel_id, names=("dm",))
        if not resolve:
            return Identity(id=channel_id)
        cached = self._name_cache.get(f"c:{channel_id}")
        if cached and cached[1] > time.monotonic():
            return cached[0]
        try:
            info = await self._web.conversations_info(channel=channel_id)
            channel = info.get("channel") or {}
            if channel.get("is_im"):
                identity = Identity(id=channel_id, names=("dm",))
            else:
                name = channel.get("name") or ""
                identity = Identity(
                    id=channel_id,
                    names=(name,) if name else (),
                    complete=bool(name),
                )
        except Exception as e:
            logger.warning(
                "Slack conversations.info failed for %s: %s", channel_id, e,
            )
            identity = Identity(id=channel_id, complete=False)
        self._remember(
            self._name_cache, f"c:{channel_id}",
            (identity, time.monotonic() + _NAME_CACHE_TTL), _NAME_CACHE_MAX,
        )
        return identity

    async def _authorize(
        self, user_id: str, channel_id: str, channel_type: str,
    ) -> bool:
        """Run the access policy for one event, logging any refusal."""
        policy = self.policy
        if not policy.configured:
            logger.warning(
                "Slack: refusing %s in %s — no allow list configured",
                user_id, channel_id,
            )
            return False

        user = await self._identify_user(
            user_id,
            needs_name_resolution(policy.users, is_id=is_slack_id),
            need_email=policy.users.deny_needs(lambda p: "@" in p),
        )
        conversation = await self._identify_conversation(
            channel_id,
            channel_type,
            needs_name_resolution(policy.conversations, is_id=is_slack_id),
        )
        verdict = policy.check(user, conversation)
        if not verdict.allowed:
            logger.info("Slack refused a message: %s", verdict.reason)
        return verdict.allowed

    # ------------------------------------------------------------------ #
    #  Events API                                                          #
    # ------------------------------------------------------------------ #

    async def _handle_event(self, event: dict[str, Any]) -> None:
        """Route one Events API event."""
        etype = event.get("type")
        if etype in ("message", "app_mention"):
            await self._handle_message_event(event)
        elif etype == "reaction_added":
            await self._handle_reaction_event(event)

    def _is_own_message(self, event: dict[str, Any]) -> bool:
        """True only for messages this app itself posted.

        Treating every ``bot_id`` as our own is too broad: Slack stamps one
        onto a message a *person* sent through any app or integration —
        a workflow, a scheduled send, a client posting with a user token —
        while still naming them in ``user``. Ignoring those drops real people
        mid-conversation.

        Other bots are turned away by the ``bot_message`` subtype instead,
        which is what a message with no human behind it carries. A person
        posting through an app is a person, and the access policy judges
        them on their own id.
        """
        if self._bot_id and event.get("bot_id") == self._bot_id:
            return True
        return bool(self._bot_user_id and event.get("user") == self._bot_user_id)

    async def _should_answer(
        self, event: dict[str, Any], channel_type: str, channel_key: str,
    ) -> bool:
        """Whether a channel message is addressed to the agent.

        A direct message always is. In a shared channel the bot answers only
        when mentioned, or when the message continues a thread it is already
        running a session for — otherwise adding the bot to a busy channel
        would start an agent turn for every remark in it.
        """
        if channel_type == "im":
            return True
        if event.get("type") == "app_mention":
            return True
        if self._bot_user_id and f"<@{self._bot_user_id}>" in (event.get("text") or ""):
            return True
        if event.get("thread_ts"):
            return bool(await self.router.get_last_session(channel_key))
        return False

    async def _handle_message_event(self, event: dict[str, Any]) -> None:
        """Turn a Slack message into an InboundMessage and hand it to the router."""
        if self._is_own_message(event):
            return
        subtype = event.get("subtype")
        if subtype in _IGNORED_SUBTYPES:
            return

        channel_id = event.get("channel") or ""
        user_id = event.get("user") or ""
        ts = event.get("ts") or ""
        if not channel_id or not user_id or not ts:
            return

        if self._is_duplicate(f"{channel_id}:{ts}"):
            return

        channel_type = event.get("channel_type") or (
            "im" if channel_id.startswith("D") else "channel"
        )
        cfg = self.config.slack
        thread_ts = event.get("thread_ts") if cfg.reply_in_thread else None
        # A first reply in a channel opens a thread on the message itself, so
        # the conversation stays out of the channel's main flow.
        if cfg.reply_in_thread and not thread_ts and channel_type != "im":
            thread_ts = ts
        target = format_target(channel_id, thread_ts)
        channel_key = f"slack:{target}"

        if not await self._should_answer(event, channel_type, channel_key):
            return
        if not await self._authorize(user_id, channel_id, channel_type):
            return

        text = slack_to_plain(event.get("text") or "", self._bot_user_id)
        self._cache_message(ts, target, text)

        images: list[dict[str, str]] = []
        file_context, file_blocks = await self._extract_files(event.get("files") or [])
        if file_context:
            text = f"{file_context}\n\n{text}" if text else file_context
        images.extend(file_blocks)

        if not text and not images:
            return

        logger.info(
            "Slack message from %s in %s: %s%s",
            user_id, target,
            (text[:80] + ("..." if len(text) > 80 else "")) if text else "(no text)",
            f" [{len(images)} attachment(s)]" if images else "",
        )

        metadata: dict[str, Any] = {"message_id": ts, "slack_user_id": user_id}
        if images:
            metadata["images"] = images
        self._remember(self._last_inbound_ts, target, ts, _INBOUND_TS_MAX)

        msg = InboundMessage(
            channel_name="slack",
            channel_key=channel_key,
            sender_id=target,
            text=text,
            metadata=metadata,
        )

        try:
            await self.router.handle_message(msg)
        except Exception as e:
            logger.error("Agent error for %s: %s", target, e, exc_info=True)
            await self._post(target, f"Error: {e}")

    async def _handle_reaction_event(self, event: dict[str, Any]) -> None:
        """Forward an emoji reaction on a cached message as text."""
        if self._is_own_message(event):
            return
        item = event.get("item") or {}
        channel_id = item.get("channel") or ""
        ts = item.get("ts") or ""
        user_id = event.get("user") or ""
        reaction = event.get("reaction") or ""
        if not (channel_id and ts and user_id and reaction):
            return
        if self._is_duplicate(f"reaction:{channel_id}:{ts}:{user_id}:{reaction}"):
            return

        cached = self._message_cache.get(ts)
        if not cached:
            # Only react to reactions on messages from this conversation that
            # we still hold context for; anything else has no session to
            # attach to and would open one from a stray emoji.
            return
        target, original_text = cached

        channel_type = "im" if channel_id.startswith("D") else "channel"
        if not await self._authorize(user_id, channel_id, channel_type):
            return

        text = f'[Reaction: :{reaction}: on message: "{original_text}"]'
        logger.info("Slack reaction from %s in %s: :%s:", user_id, target, reaction)

        msg = InboundMessage(
            channel_name="slack",
            channel_key=f"slack:{target}",
            sender_id=target,
            text=text,
            metadata={},
        )
        try:
            await self.router.handle_message(msg)
        except Exception as e:
            logger.error("Agent error for reaction in %s: %s", target, e, exc_info=True)

    # ------------------------------------------------------------------ #
    #  Attachments                                                         #
    # ------------------------------------------------------------------ #

    async def _download_file(self, url: str) -> bytes | None:
        """Fetch a private Slack file with the bot token."""
        import httpx

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(
                    url,
                    headers={"Authorization": f"Bearer {self.config.slack.bot_token}"},
                    follow_redirects=True,
                )
                resp.raise_for_status()
                return resp.content
        except Exception as e:
            logger.warning("Slack file download failed for %s: %s", url, e)
            return None

    async def _extract_files(
        self, files: list[dict[str, Any]],
    ) -> tuple[str, list[dict[str, str]]]:
        """Pull message attachments into prompt text and content blocks.

        Returns ``(context_text, blocks)``. Text files are inlined, images and
        PDFs become base64 blocks, ZIPs are unpacked one level, and anything
        else contributes a metadata line only.
        """
        if not files:
            return "", []

        parts: list[str] = []
        blocks: list[dict[str, str]] = []

        for f in files:
            name = f.get("name") or "unnamed"
            mime = f.get("mimetype") or ""
            size = int(f.get("size") or 0)
            ext = f".{name.rsplit('.', 1)[-1].lower()}" if "." in name else ""
            size_str = (
                f"{size / 1024:.0f} KB" if size < 1_000_000
                else f"{size / 1_000_000:.1f} MB"
            )
            meta = f"[File: {name} ({size_str}, {mime or 'unknown type'})]"
            url = f.get("url_private_download") or f.get("url_private") or ""

            if size > _MAX_DOWNLOAD_SIZE or not url:
                parts.append(f"{meta}\n(Too large or not downloadable)")
                continue

            is_text = (
                mime.startswith("text/")
                or ext in _TEXT_EXTENSIONS
                or f.get("mode") == "snippet"
            )
            if is_text:
                if size > _MAX_TEXT_SIZE:
                    parts.append(f"{meta}\n(Text file too large to inline — {size_str})")
                    continue
                data = await self._download_file(url)
                if data is None:
                    parts.append(meta)
                    continue
                parts.append(
                    f"{meta}\n```\n{data.decode('utf-8', errors='replace')}\n```",
                )
                continue

            if mime in _IMAGE_EXT_TO_MIME.values() or ext in _IMAGE_EXT_TO_MIME:
                data = await self._download_file(url)
                if data is None:
                    parts.append(meta)
                    continue
                blocks.append({
                    "type": "base64",
                    "media_type": mime or _IMAGE_EXT_TO_MIME.get(ext, "image/png"),
                    "data": base64.b64encode(data).decode("utf-8"),
                })
                parts.append(meta)
                continue

            if mime == "application/pdf" or ext == ".pdf":
                data = await self._download_file(url)
                if data is None:
                    parts.append(meta)
                    continue
                blocks.append({
                    "type": "base64",
                    "media_type": "application/pdf",
                    "data": base64.b64encode(data).decode("utf-8"),
                })
                parts.append(meta)
                continue

            if ext == ".zip" or mime in ("application/zip", "application/x-zip-compressed"):
                data = await self._download_file(url)
                if data is None:
                    parts.append(meta)
                    continue
                zip_blocks, zip_text = extract_zip(data, meta)
                blocks.extend(zip_blocks)
                parts.append(zip_text)
                continue

            parts.append(meta)

        return "\n".join(parts), blocks

    # ------------------------------------------------------------------ #
    #  Outbound                                                            #
    # ------------------------------------------------------------------ #

    async def _post(
        self, target: str, text: str, blocks: list[dict] | None = None,
    ) -> str | None:
        """Post one message to a target. Returns its ts.

        Raises on an API failure rather than reporting success. Slack rate
        limits ``chat.postMessage`` to roughly one call per second per
        channel, so failure here is ordinary, and a caller that cannot see it
        will either drop the agent's reply or record an undelivered
        notification as delivered. Callers that genuinely want best-effort
        catch it themselves.
        """
        if self._web is None:
            return None
        channel_id, thread_ts = parse_target(target)
        resp = await self._web.chat_postMessage(
            channel=channel_id,
            text=text,
            blocks=blocks,
            thread_ts=thread_ts,
            unfurl_links=False,
            unfurl_media=False,
        )
        return resp.get("ts")

    async def send(self, message: OutboundMessage) -> None:
        """Send a complete message, split to fit Slack's render limit.

        Propagates a failure so StreamAdapter can fall back to editing the
        streaming placeholder; swallowing it loses the whole turn.
        """
        if self._web is None:
            return
        for chunk in split_message(message.text, MAX_MSG_LEN):
            ts = await self._post(message.target, _md_to_slack(chunk))
            if ts:
                self._cache_message(ts, message.target, chunk)

    def format_response(self, text: str) -> str:
        """Return text unchanged — :meth:`send` splits and converts it."""
        return text

    # ------------------------------------------------------------------ #
    #  Streaming protocol                                                  #
    # ------------------------------------------------------------------ #

    async def send_placeholder(self, target: str, session_id: str) -> str | None:
        """Post the placeholder that streaming updates will edit in place.

        Returns None if the post fails, which makes StreamAdapter fall back
        to sending the finished reply as one message. Raising instead would
        abort the turn over a rate-limited placeholder.
        """
        try:
            return await self._post(target, "⏳")
        except Exception as e:
            logger.warning(
                "Slack placeholder failed for %s (%s) — streaming this turn "
                "without one", target, e,
            )
            return None

    async def edit_message(self, target: str, message_id: str, text: str) -> None:
        """Rewrite a previously sent message with the latest streamed text."""
        if self._web is None:
            return
        channel_id, _ = parse_target(target)
        body = _md_to_slack(text)
        if len(body) > MAX_MSG_LEN:
            body = body[:MAX_MSG_LEN] + "…"
        try:
            await self._web.chat_update(
                channel=channel_id, ts=message_id, text=body,
            )
        except Exception as e:
            logger.debug("Slack chat.update failed for %s: %s", target, e)
        self._cache_message(message_id, target, text)

    async def delete_message(self, target: str, message_id: str) -> None:
        """Remove a message — used to clear the streaming placeholder."""
        if self._web is None:
            return
        channel_id, _ = parse_target(target)
        try:
            await self._web.chat_delete(channel=channel_id, ts=message_id)
        except Exception as e:
            logger.debug("Slack chat.delete failed for %s: %s", target, e)

    async def send_typing(self, target: str) -> None:
        """Acknowledge receipt with an 👀 reaction.

        Slack has no typing indicator a bot may raise, and a "thinking…"
        message would be one more post to clean up. A reaction on the message
        being answered says the same thing and disappears with it.
        """
        if self._web is None:
            return
        ts = self._last_inbound_ts.get(target)
        if not ts:
            return
        channel_id, _ = parse_target(target)
        try:
            await self._web.reactions_add(
                channel=channel_id, timestamp=ts, name="eyes",
            )
        except Exception as e:
            # already_reacted is the normal case on a follow-up message.
            logger.debug("Slack reactions.add (ack) failed: %s", e)

    async def set_reaction(self, target: str, message_id: Any, emoji: str) -> None:
        """Set an emoji reaction on a message."""
        if self._web is None:
            return
        name = slack_emoji_name(emoji)
        if not name:
            logger.info("Slack has no short name for reaction %r — skipped", emoji)
            return
        channel_id, _ = parse_target(target)
        try:
            await self._web.reactions_add(
                channel=channel_id, timestamp=str(message_id), name=name,
            )
        except Exception as e:
            logger.warning("Slack reactions.add failed for %s: %s", target, e)

    async def send_file(self, target: str, file_path: str) -> bool:
        """Upload a file into the conversation as an attachment."""
        if self._web is None or not target:
            return False
        path = Path(file_path)
        if not path.is_file():
            return False
        channel_id, thread_ts = parse_target(target)
        try:
            await self._web.files_upload_v2(
                channel=channel_id,
                file=str(path),
                filename=path.name,
                thread_ts=thread_ts,
            )
            return True
        except Exception as e:
            logger.warning("Slack files_upload_v2 failed for %s: %s", target, e)
            return False

    @staticmethod
    def _remember(
        cache: collections.OrderedDict, key: str, value: Any, limit: int,
    ) -> None:
        """Insert into an LRU cache, evicting the oldest entries past *limit*."""
        cache[key] = value
        cache.move_to_end(key)
        while len(cache) > limit:
            cache.popitem(last=False)

    def _cache_message(self, ts: str, target: str, text: str) -> None:
        """Store a message snippet in the LRU cache for reaction lookups."""
        snippet = (text or "")[:200]
        if not snippet:
            return
        self._remember(
            self._message_cache, ts, (target, snippet), _MESSAGE_CACHE_MAX,
        )

    # ------------------------------------------------------------------ #
    #  Slash commands — /nerve <subcommand>                                #
    # ------------------------------------------------------------------ #

    async def _handle_slash_command(self, payload: dict[str, Any]) -> None:
        """Handle ``/nerve <subcommand>``.

        One command with subcommands rather than one command per action:
        Slack registers commands per workspace, so ``/new`` and ``/stop``
        would collide with every other app installed there.
        """
        user_id = payload.get("user_id") or ""
        channel_id = payload.get("channel_id") or ""
        if not user_id or not channel_id:
            return

        channel_type = "im" if channel_id.startswith("D") else "channel"
        if not await self._authorize(user_id, channel_id, channel_type):
            await self._respond_ephemeral(
                channel_id, user_id, "You are not authorized to use this bot.",
            )
            return

        args = (payload.get("text") or "").strip().split()
        sub = args[0].lower() if args else "help"
        rest = args[1:]
        target = format_target(channel_id)
        channel_key = f"slack:{target}"

        enabled = self.enabled_commands
        if sub == "session":
            sub = "sessions"
        if sub != "help" and sub not in enabled:
            known = sub in SLACK_ALL_COMMANDS
            await self._respond_ephemeral(
                channel_id, user_id,
                f"`/nerve {sub}` is turned off for this workspace."
                if known
                else f"No such command `/nerve {sub}`.",
            )
            return

        # `sessions` and `new` bind a session to the command's own key. In a
        # threaded channel nothing ever reads that key, so they would answer
        # as if they had worked and change nothing.
        if sub in ("sessions", "new") and not self._binds_to_channel_key(channel_id):
            await self._respond_ephemeral(
                channel_id, user_id, self._THREADED_CHANNEL_REFUSAL.format(sub=sub),
            )
            return

        if sub == "sessions":
            await self._send_sessions_view(channel_id, user_id, channel_key)
        elif sub == "new":
            await self._cmd_new(channel_id, user_id, channel_key, rest)
        elif sub == "stop":
            await self._cmd_stop(channel_id, user_id, channel_key)
        elif sub in ("star", "unstar"):
            await self._cmd_star(channel_id, user_id, channel_key, sub == "star")
        elif sub == "doctor":
            from nerve.cli import doctor_report
            await self._respond_ephemeral(
                channel_id, user_id, f"```\n{doctor_report(self.config)}\n```",
            )
        elif sub == "restart":
            import subprocess
            import sys

            await self._respond_ephemeral(channel_id, user_id, "Restarting Nerve…")
            logger.info("Restart requested by Slack user %s", user_id)
            subprocess.Popen(
                [sys.executable, "-m", "nerve", "restart"],
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        elif sub == "reply":
            await self._cmd_reply(channel_id, user_id, " ".join(rest))
        else:
            await self._respond_ephemeral(
                channel_id, user_id, self._help_text(enabled),
            )

    @staticmethod
    def _help_text(enabled: frozenset[str]) -> str:
        """Help listing only what this workspace can actually run."""
        lines = [
            (name, f"• `/nerve {usage}` — {what}")
            for name, usage, what in (
                ("sessions", "sessions", "list and switch sessions"),
                ("new", "new [title]", "stop the current session, start a new one"),
                ("stop", "stop", "stop a running session in this channel"),
                ("star", "star", "keep a session alive"),
                ("unstar", "unstar", "let a session auto-close again"),
                ("reply", "reply <text>", "answer the latest pending question"),
                ("doctor", "doctor", "health report"),
                ("restart", "restart", "restart the daemon"),
            )
            if name in enabled
        ]
        if not lines:
            return "No `/nerve` commands are enabled for this workspace."
        return "*Nerve commands*\n" + "\n".join(line for _, line in lines)

    _THREADED_CHANNEL_REFUSAL = (
        "`/nerve {sub}` needs a thread to bind the session to, and Slack does "
        "not run `/nerve` inside one. Every new mention in this channel "
        "already opens its own thread and its own session. Use `/nerve stop` "
        "to end one, or set `slack.reply_in_thread: false` to keep a single "
        "session per channel."
    )

    def _binds_to_channel_key(self, channel_id: str) -> bool:
        """Whether ordinary messages here land on the command's own key.

        A slash command carries no thread reference, so it can only name
        ``slack:<channel>``. A direct message routes there, and so does a
        channel message while ``reply_in_thread`` is off. With it on, a
        channel message opens a thread and routes to
        ``slack:<channel>:<ts>`` instead, so a session bound at channel level
        is never read again: the command reports success and the next message
        starts somewhere else.
        """
        if not channel_id:
            return False
        if channel_id.startswith("D"):
            return True
        return not self.config.slack.reply_in_thread

    async def _cmd_new(
        self, channel_id: str, user_id: str, channel_key: str, args: list[str],
    ) -> None:
        prev = await self.router.get_last_session(channel_key)
        if prev:
            await self.router.engine.stop_session(prev)
        title = " ".join(args) or None
        session_id = await self.router.create_session(
            channel_key, title=title, source="slack",
        )
        await self._respond_ephemeral(
            channel_id, user_id,
            f"New session `{session_id}`" + (f" — {title}" if title else ""),
        )

    async def _live_sessions_for_channel(self, channel_id: str) -> list[dict[str, Any]]:
        """Every live session reachable from this channel, newest first.

        Slack refuses to run a slash command inside a thread — it answers
        "/nerve is not supported in threads" — so a command never carries
        thread context and cannot simply read the session for its own key.
        With per-thread routing that key usually owns nothing while the
        threads beside it are busy, which is how ``/nerve stop`` came to
        report "No active session" with three turns still running.

        The prefix is re-checked per row because ``slack:C123`` is also a
        prefix of ``slack:C1234``.
        """
        rows = await self.router.list_conversation_sessions(f"slack:{channel_id}")
        matching: list[dict[str, Any]] = []
        for row in rows:
            key = row.get("channel_key") or ""
            if not key.startswith("slack:"):
                continue
            row_channel, thread_ts = parse_target(key[len("slack:"):])
            if row_channel != channel_id:
                continue
            matching.append({**row, "thread_ts": thread_ts})
        return matching

    @staticmethod
    def _session_choice_label(row: dict[str, Any]) -> str:
        """Button label naming one session, and the thread it belongs to."""
        title = (row.get("title") or "").strip() or row.get("session_id", "?")
        where = "in thread" if row.get("thread_ts") else "in channel"
        label = f"{title} ({where})"
        return label[:74] + "…" if len(label) > 75 else label

    def _session_picker_blocks(
        self,
        candidates: list[dict[str, Any]],
        prompt: str,
        action_prefix: str,
        style: str | None = None,
    ) -> list[dict[str, Any]]:
        """One button per live session, so the caller names the target.

        Slack does not allow ``/nerve`` inside a thread, so a command cannot
        say which of a channel's threads was meant. Asking is safer than
        acting on whichever row the query returned first.
        """
        return [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*{len(candidates)} sessions are live in this "
                        f"channel.* {prompt}"
                    ),
                },
            },
            *[
                {
                    "type": "actions",
                    "block_id": f"{action_prefix}row:{row['session_id']}",
                    "elements": [{
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": self._session_choice_label(row),
                            "emoji": True,
                        },
                        "action_id": f"{action_prefix}{row['session_id']}",
                        "value": row["session_id"],
                        **({"style": style} if style else {}),
                    }],
                }
                for row in candidates[:_MAX_ACTION_ELEMENTS]
            ],
        ]

    async def _cmd_stop(
        self, channel_id: str, user_id: str, channel_key: str,
    ) -> None:
        candidates = await self._live_sessions_for_channel(channel_id)
        if not candidates:
            await self._respond_ephemeral(
                channel_id, user_id, "No active session in this channel.",
            )
            return

        if len(candidates) == 1:
            await self._stop_and_report(channel_id, user_id, candidates[0])
            return

        await self._respond_ephemeral_blocks(
            channel_id, user_id,
            text="Which session should I stop?",
            blocks=self._session_picker_blocks(
                candidates, "Pick the one to stop:", "sessstop:", style="danger",
            ),
        )

    async def _stop_and_report(
        self, channel_id: str, user_id: str, row: dict[str, Any],
    ) -> None:
        """Stop one session and say which one, so the answer is checkable."""
        session_id = row["session_id"]
        stopped = await self.router.engine.stop_session(session_id)
        where = "the thread" if row.get("thread_ts") else "this channel"
        await self._respond_ephemeral(
            channel_id, user_id,
            f"Stopped `{session_id}` in {where}."
            if stopped
            else f"`{session_id}` was not running.",
        )

    async def _cmd_star(
        self, channel_id: str, user_id: str, channel_key: str, starred: bool,
    ) -> None:
        # Same thread-blindness as stop: resolve across the conversation
        # rather than the command's own key, which usually owns nothing.
        candidates = await self._live_sessions_for_channel(channel_id)
        verb = "star" if starred else "unstar"
        if not candidates:
            await self._respond_ephemeral(
                channel_id, user_id,
                f"No active session to {verb} in this channel.",
            )
            return

        if len(candidates) == 1:
            await self._star_and_report(
                channel_id, user_id, candidates[0]["session_id"], starred,
            )
            return

        await self._respond_ephemeral_blocks(
            channel_id, user_id,
            text=f"Which session should I {verb}?",
            blocks=self._session_picker_blocks(
                candidates,
                f"Pick the one to {verb}:",
                f"{_STAR_ACTION_PREFIX}{int(starred)}:",
            ),
        )

    async def _star_and_report(
        self, channel_id: str, user_id: str, session_id: str, starred: bool,
    ) -> None:
        """Star or unstar one session and name it in the reply."""
        try:
            await self.router.set_session_starred(session_id, starred)
        except ValueError as e:
            await self._respond_ephemeral(channel_id, user_id, str(e))
            return
        await self._respond_ephemeral(
            channel_id, user_id,
            f"⭐ Starred `{session_id}` — it won't auto-close when idle."
            if starred
            else f"☆ Unstarred `{session_id}` — normal auto-close applies.",
        )

    async def _cmd_reply(self, channel_id: str, user_id: str, answer: str) -> None:
        if not answer:
            await self._respond_ephemeral(
                channel_id, user_id, "Usage: `/nerve reply <your answer>`",
            )
            return
        if not self._notification_service:
            await self._respond_ephemeral(
                channel_id, user_id, "Notification service not available.",
            )
            return
        pending = await self._notification_service.db.list_notifications(
            status="pending", type="question", limit=1,
        )
        if not pending:
            await self._respond_ephemeral(channel_id, user_id, "No pending questions.")
            return
        ok = await self._notification_service.handle_answer(
            notification_id=pending[0]["id"], answer=answer, answered_by="slack",
        )
        await self._respond_ephemeral(
            channel_id, user_id,
            f"Answer recorded for: {pending[0]['title']}"
            if ok
            else "Failed to record answer.",
        )

    async def _respond_ephemeral(
        self, channel_id: str, user_id: str, text: str,
    ) -> None:
        """Reply so only the person who ran the command sees it."""
        if self._web is None:
            return
        try:
            await self._web.chat_postEphemeral(
                channel=channel_id, user=user_id, text=_md_to_slack(text),
            )
        except Exception as e:
            logger.warning("Slack chat.postEphemeral failed: %s", e)

    async def _respond_ephemeral_blocks(
        self, channel_id: str, user_id: str, text: str, blocks: list[dict],
    ) -> None:
        """Ephemeral reply carrying Block Kit, for the pickers."""
        if self._web is None:
            return
        try:
            await self._web.chat_postEphemeral(
                channel=channel_id, user=user_id, text=text, blocks=blocks,
            )
        except Exception as e:
            logger.warning("Slack ephemeral blocks failed: %s", e)

    async def _send_sessions_view(
        self, channel_id: str, user_id: str, channel_key: str,
    ) -> None:
        """Post the session switcher, visible only to the requester."""
        if self._web is None:
            return
        blocks = await self._sessions_blocks_for(channel_key)
        try:
            await self._web.chat_postEphemeral(
                channel=channel_id, user=user_id,
                text="Sessions", blocks=blocks,
            )
        except Exception as e:
            logger.warning("Slack sessions view failed: %s", e)

    async def _sessions_blocks_for(self, channel_key: str) -> list[dict[str, Any]]:
        """Build the session switcher for a conversation.

        Only interactive sessions with history are offered as switch targets;
        an empty session is nothing to switch to and cron sessions are never
        switch targets.
        """
        current = await self.router.get_last_session(channel_key)
        sessions = await self.router.list_sessions(limit=30)
        non_empty: list[dict] = []
        for s in sessions:
            if s.get("source") not in ("slack", "telegram", "web"):
                continue
            if await self.router.count_session_messages(s["id"]) > 0:
                non_empty.append(s)
                if len(non_empty) >= _SESSIONS_BUTTON_LIMIT:
                    break
        return build_sessions_blocks(non_empty, current)

    # ------------------------------------------------------------------ #
    #  Interactive — Block Kit button presses                              #
    # ------------------------------------------------------------------ #

    async def _handle_interactive(self, payload: dict[str, Any]) -> None:
        """Handle a Block Kit button press."""
        if payload.get("type") != "block_actions":
            return
        actions = payload.get("actions") or []
        if not actions:
            return
        action_id = actions[0].get("action_id") or ""
        value = actions[0].get("value") or ""

        user_id = (payload.get("user") or {}).get("id") or ""
        channel_id = (payload.get("channel") or {}).get("id") or ""
        response_url = payload.get("response_url") or ""
        if not user_id:
            return

        channel_type = "im" if channel_id.startswith("D") else "channel"
        if channel_id and not await self._authorize(user_id, channel_id, channel_type):
            return

        if action_id.startswith("sessstop:"):
            stopped = await self.router.engine.stop_session(value)
            await self._replace_via_url(
                response_url,
                f"Stopped `{value}`." if stopped else f"`{value}` was not running.",
            )
            return

        if action_id.startswith(_STAR_ACTION_PREFIX):
            await self._handle_star_button(action_id, value, response_url)
            return

        if action_id.startswith("sess:") or action_id.startswith("sessstar:"):
            await self._handle_session_button(
                action_id, value, channel_id, user_id, response_url,
            )
            return

        if action_id.startswith("notif:"):
            await self._handle_notification_button(
                action_id, value, payload, response_url,
            )

    async def _handle_star_button(
        self, action_id: str, value: str, response_url: str,
    ) -> None:
        """Star or unstar the session picked from the `/nerve star` card."""
        parts = action_id.split(":", 2)
        if len(parts) < 3:
            return
        starred = parts[1] == "1"
        session_id = value or parts[2]
        try:
            await self.router.set_session_starred(session_id, starred)
        except ValueError:
            await self._replace_via_url(
                response_url, "That session is no longer available.",
            )
            return
        await self._replace_via_url(
            response_url,
            f"⭐ Starred `{session_id}`."
            if starred
            else f"☆ Unstarred `{session_id}`.",
        )

    async def _handle_session_button(
        self,
        action_id: str,
        value: str,
        channel_id: str,
        user_id: str,
        response_url: str,
    ) -> None:
        """Switch, create, or star a session from the switcher card."""
        channel_key = f"slack:{format_target(channel_id)}"

        # A card posted before `reply_in_thread` was turned on, or one kept
        # open in a threaded channel, would bind the session to a key no
        # message ever reads. Starring does not touch the mapping, so it is
        # still allowed.
        if not action_id.startswith("sessstar:") and not self._binds_to_channel_key(
            channel_id,
        ):
            await self._replace_via_url(
                response_url,
                self._THREADED_CHANNEL_REFUSAL.format(sub="sessions"),
            )
            return

        if action_id.startswith("sessstar:"):
            try:
                await self.router.toggle_session_starred(value)
            except ValueError:
                await self._replace_via_url(
                    response_url, "That session is no longer available.",
                )
                return
        elif value == "new":
            await self.router.create_session(channel_key, source="slack")
        else:
            try:
                await self.router.switch_session(channel_key, value)
            except ValueError:
                await self._replace_via_url(
                    response_url, "That session is no longer available.",
                )
                return

        blocks = await self._sessions_blocks_for(channel_key)
        await self._replace_via_url(response_url, "Sessions", blocks)

    async def _handle_notification_button(
        self,
        action_id: str,
        value: str,
        payload: dict[str, Any],
        response_url: str,
    ) -> None:
        """Record an answer to a notification and settle the card."""
        parts = action_id.split(":", 2)
        if len(parts) < 3:
            return
        notification_id = parts[1]
        answer = value or parts[2]

        if not self._notification_service:
            await self._replace_via_url(response_url, "Service unavailable.")
            return

        actor = (payload.get("user") or {}).get("id") or ""
        success = await self._notification_service.handle_answer(
            notification_id=notification_id, answer=answer, answered_by="slack",
        )
        if not success:
            await self._replace_via_url(
                response_url, "Already answered or expired.",
            )
            return

        # The card's section text is the mrkdwn Slack already stores. Running
        # it through the Markdown converter a second time turns *bold* into
        # _italic_ and escapes the link markup, so it is carried across
        # verbatim and only the status line is converted.
        original = "\n".join(
            (block.get("text") or {}).get("text") or ""
            for block in (payload.get("message") or {}).get("blocks") or []
            if block.get("type") == "section"
        ).strip("\n")

        status = f"✅ Answered: {_md_to_slack(answer)}"
        snoozed_until = await self._get_snoozed_until(notification_id)
        if snoozed_until:
            status = f"💤 Snoozed until {snoozed_until} — will resurface"
        # Written as raw mention markup: the converter would escape it, and
        # the card is the only place a reader sees who pressed the button.
        if actor:
            status += f" (by <@{actor}>)"
        await self._replace_via_url(
            response_url,
            f"{original}\n\n{status}" if original else status,
            already_mrkdwn=True,
        )

    async def _get_snoozed_until(self, notification_id: str) -> str | None:
        """Human-readable re-delivery time if the row was snoozed.

        A snoozed approval is the only outcome that leaves the row pending
        with ``redeliver_at`` set. Cosmetic, so every failure yields None.
        """
        try:
            notif = await self._notification_service.db.get_notification(
                notification_id,
            )
            if (
                not notif
                or notif.get("status") != "pending"
                or not notif.get("redeliver_at")
            ):
                return None
            from datetime import datetime
            dt = datetime.fromisoformat(notif["redeliver_at"])
            return dt.astimezone().strftime("%Y-%m-%d %H:%M %Z")
        except Exception:
            return None

    async def _replace_via_url(
        self,
        response_url: str,
        text: str,
        blocks: list[dict] | None = None,
        already_mrkdwn: bool = False,
    ) -> None:
        """Replace the card a button lives on.

        ``response_url`` is the only way to edit an ephemeral message, and it
        works for in-channel cards too, so both paths use it.

        ``already_mrkdwn`` says *text* came back off a card Slack rendered,
        so it must go out untouched. Converting it again reads the mrkdwn as
        Markdown and rewrites the message.
        """
        if not response_url:
            return
        import httpx

        body: dict[str, Any] = {
            "replace_original": True,
            "text": text if (blocks or already_mrkdwn) else _md_to_slack(text),
        }
        if blocks:
            body["blocks"] = blocks
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                await client.post(response_url, json=body)
        except Exception as e:
            logger.debug("Slack response_url update failed: %s", e)
