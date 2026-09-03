"""Slack bot channel over Socket Mode.

Inbound messages are authorized before routing. Targets encode a channel and
optional thread as ``C0456DEF[:timestamp]``; channel keys add the ``slack:``
prefix. Shared channels always use thread-scoped sessions.
"""

from __future__ import annotations

import asyncio
import base64
import collections
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TYPE_CHECKING

from nerve.channels.access import (
    Decision,
    Identity,
    PatternGate,
    needs_name_resolution,
)
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
    ObservedMessage,
    OutboundMessage,
)
from nerve.channels.observation import ObservationPolicy
from nerve.channels.slack_access import SlackAccessPolicy
from nerve.channels.slack_presentation import (
    MAX_MSG_LEN,
    _MAX_ACTION_ELEMENTS,
    _SESSIONS_BUTTON_LIMIT,
    _md_to_slack,
    build_notification_blocks,
    build_sessions_blocks,
    slack_emoji_name,
    slack_to_plain,
    split_message,
)
from nerve.config import (
    SLACK_ALL_COMMANDS,
    SLACK_DEFAULT_COMMANDS,
    NerveConfig,
)

if TYPE_CHECKING:
    from nerve.channels.router import ChannelRouter

logger = logging.getLogger(__name__)

# chat.update is limited to roughly one call per second per channel.
EDIT_INTERVAL = 1.2
# Watchdog: check every 30s, log heartbeat every ~5 min.
WATCHDOG_INTERVAL = 30
WATCHDOG_HEARTBEAT_EVERY = 10
# The SDK retries a rejected Socket Mode handshake forever. A reload must
# return a failure and restore the previous transport instead of hanging the
# config endpoint indefinitely.
SOCKET_CONNECT_TIMEOUT = 30
# Give acknowledged work a short chance to finish when Slack is disabled live.
_STOP_DRAIN_TIMEOUT = 30.0
# Bounded caches: event dedupe, message text for reaction context, resolved names.
_DEDUPE_MAX = 500
_MESSAGE_CACHE_MAX = 200
_NAME_CACHE_MAX = 500
_INBOUND_TS_MAX = 500
_EDIT_CLOCK_MAX = 500
_NAME_CACHE_TTL = 600.0
# Concurrent dispatch tasks. The router serialises per session, so this only
# bounds envelopes not yet routed — including ones headed for a refusal.
_MAX_INFLIGHT = 100
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

# ---------------------------------------------------------------------- #
#  Pure helpers — module level so they are testable without a transport   #
# ---------------------------------------------------------------------- #


# Slack object ids: a type letter then alphanumerics. Canonical IDs are
# uppercase, but config matching is case-insensitive. U/W users, B bots,
# C/G/D/T conversations and teams. Used to decide whether an allow/deny pattern
# can be matched against the id alone, so the shape has to be exact — anything
# looser skips a name lookup a deny list depends on.
_SLACK_ID_RE = re.compile(r"^[UWBCDGT][A-Z0-9]{7,}$", re.IGNORECASE)


def is_slack_id(pattern: str) -> bool:
    """Whether *pattern* is a literal Slack object id rather than a name."""
    return bool(_SLACK_ID_RE.match(pattern))


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


def slack_ts_to_iso(ts: str) -> str:
    """Turn a Slack ``<seconds>.<microseconds>`` stamp into ISO 8601 UTC.

    Falls back to now rather than raising. An unreadable stamp should cost an
    observation its exact time, not drop the message.
    """
    try:
        seconds = float(ts)
    except (AttributeError, TypeError, ValueError):
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat()


class SlackUnavailable(RuntimeError):
    """The channel cannot carry traffic for the generation now running."""


class SlackChannel(BaseChannel):
    """Slack bot channel over Socket Mode.

    Owns the WebSocket transport, event dispatch, and authorization.
    Session management and agent execution belong to the ChannelRouter.
    """

    def __init__(
        self, config: NerveConfig, router: ChannelRouter,
    ):
        self._config = config
        self.router = router
        self._client: Any = None            # AsyncSocketModeClient
        self._web: Any = None               # AsyncWebClient
        # Credentials from the runtime's active config generation.
        # A watchdog reconnect must reuse this coherent pair.
        self._active_bot_token = ""
        self._active_app_token = ""
        self._transport_lock = asyncio.Lock()
        self._bot_user_id: str = ""
        self._bot_id: str = ""     # the app's own bot_id, to spot our own posts
        self._team_id: str = ""
        self._notification_service = None   # Set after service is created
        self._stopping = False
        self._state = "stopped"
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
        # Every shared-channel thread is a distinct target, so this and the
        # name cache are bounded rather than left to grow per thread.
        self._last_inbound_ts: collections.OrderedDict[str, str] = (
            collections.OrderedDict()
        )
        # conversation -> monotonic time of the last droppable edit. Slack
        # rate limits chat.update per conversation, not per thread.
        self._last_channel_edit: collections.OrderedDict[str, float] = (
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
        """The config generation active for this Slack runtime."""
        return self._config

    def apply_config(self, config: NerveConfig) -> None:
        """Atomically advance behavior that needs no transport work."""
        self._config = config

    @property
    def is_available(self) -> bool:
        """Whether external delivery may use this channel."""
        return self._state == "running" and self._web is not None

    @property
    def policy(self) -> SlackAccessPolicy:
        """The access policy, rebuilt per read so reloads apply at once."""
        return SlackAccessPolicy.from_config(self.config.slack)

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
            raise RuntimeError(
                "Slack needs both bot_token (xoxb-…) and app_token (xapp-…)",
            )

        self._stopping = False
        self._state = "starting"
        async with self._transport_lock:
            try:
                web, client, auth = await self._prepare_transport(
                    cfg.bot_token,
                    cfg.app_token,
                )
                self._activate_transport(
                    web,
                    client,
                    auth,
                    cfg.bot_token,
                    cfg.app_token,
                )
                await self._connect_socket(client)
            except (Exception, asyncio.CancelledError):
                await self._close_socket_quietly(self._client)
                self._client = None
                self._web = None
                self._active_bot_token = ""
                self._active_app_token = ""
                self._bot_user_id = ""
                self._bot_id = ""
                self._team_id = ""
                self._state = "stopped"
                raise

        self._last_event_time = time.monotonic()
        self._state = "running"
        self._log_auth(auth, "Slack authenticated")
        logger.info("Slack Socket Mode connected")

        self._announce_auth_state()

    @staticmethod
    def _build_web_client(bot_token: str):
        """Build the Web API half of one credential pair."""
        from slack_sdk.http_retry.builtin_async_handlers import (
            AsyncRateLimitErrorRetryHandler,
        )
        from slack_sdk.web.async_client import AsyncWebClient

        web = AsyncWebClient(token=bot_token)
        # The SDK retries connection errors out of the box but not 429s, and
        # Slack rate limits chat.postMessage to roughly one call per second
        # per channel. A streamed reply arrives as a burst of edits followed
        # by a post, so without this the last message of a long answer is the
        # one most likely to be dropped.
        web.retry_handlers.append(
            AsyncRateLimitErrorRetryHandler(max_retry_count=3),
        )
        return web

    async def _prepare_transport(self, bot_token: str, app_token: str):
        """Validate both tokens and build, but do not connect, both clients."""
        web = self._build_web_client(bot_token)
        try:
            auth = await web.auth_test()
        except Exception as e:
            raise RuntimeError(
                f"the Slack bot token failed validation ({type(e).__name__})",
            ) from e
        client = self._build_socket_client(
            app_token=app_token, web_client=web,
        )
        try:
            client.wss_uri = await asyncio.wait_for(
                client.issue_new_wss_url(), timeout=SOCKET_CONNECT_TIMEOUT,
            )
        except (Exception, asyncio.CancelledError) as e:
            await self._close_socket_quietly(client)
            if isinstance(e, asyncio.CancelledError):
                raise
            raise RuntimeError(
                f"the Slack app token failed validation ({type(e).__name__})",
            ) from e
        return web, client, auth

    def _activate_transport(
        self, web, client, auth: dict, bot_token: str, app_token: str,
    ) -> None:
        """Publish one coherent Web API and Socket Mode credential pair."""
        self._web = web
        self._client = client
        self._active_bot_token = bot_token
        self._active_app_token = app_token
        self._bot_user_id = auth.get("user_id", "")
        self._bot_id = auth.get("bot_id", "")
        self._team_id = auth.get("team_id") or self._team_id

    def _log_auth(self, auth: dict, action: str) -> None:
        logger.info(
            "%s as %s (%s, %s) in workspace %s",
            action,
            auth.get("user"), self._bot_user_id, self._bot_id, auth.get("team"),
        )

    def _build_socket_client(
        self, *, app_token: str | None = None, web_client=None,
    ):
        """A fresh Socket Mode client wired to this channel's dispatcher."""
        from slack_sdk.socket_mode.aiohttp import SocketModeClient

        client = SocketModeClient(
            app_token=(
                self._active_app_token if app_token is None else app_token
            ),
            web_client=self._web if web_client is None else web_client,
            # Slack closes and reissues a socket roughly hourly; without this
            # the channel goes quiet until the daemon restarts.
            auto_reconnect_enabled=True,
        )
        client.socket_mode_request_listeners.append(self._on_request)
        return client

    @staticmethod
    async def _close_socket_quietly(client) -> None:
        """Best-effort cleanup for a client that will not be reused."""
        if client is None:
            return
        try:
            await asyncio.wait_for(client.close(), timeout=10)
        except Exception as e:
            logger.warning("Slack socket close raised: %s", e)

    @staticmethod
    async def _connect_socket(client) -> None:
        """Connect with a bound wait; the Slack SDK otherwise retries forever."""
        try:
            await asyncio.wait_for(
                client.connect(), timeout=SOCKET_CONNECT_TIMEOUT,
            )
        except TimeoutError as e:
            raise RuntimeError("the Slack socket connection timed out") from e

    @staticmethod
    async def _close_socket_for_replacement(client) -> None:
        """Close *client*, refusing to create a competing connection on failure."""
        if client is None:
            return
        try:
            await asyncio.wait_for(client.close(), timeout=10)
        except Exception as e:
            raise RuntimeError(
                "the existing Slack socket could not be closed",
            ) from e

    def needs_credential_reload(self, bot_token: str, app_token: str) -> bool:
        """Whether the connected clients differ from the desired token pair."""
        return (
            bot_token != self._active_bot_token or app_token != self._active_app_token
        )

    async def reload_credentials(self, config: NerveConfig) -> None:
        """Apply a config generation while replacing its credential pair.

        Validate before closing the old socket, but connect only after closing
        it to avoid competing consumers. The behavior snapshot changes before
        the new socket can deliver and rolls back with the previous transport.
        """
        bot_token = config.slack.bot_token
        app_token = config.slack.app_token
        if not bot_token or not app_token:
            raise RuntimeError("the new Slack credentials are incomplete")

        async with self._transport_lock:
            if self._stopping:
                raise RuntimeError("the Slack channel is stopping")
            if not self.needs_credential_reload(bot_token, app_token):
                return

            web, client, auth = await self._prepare_transport(
                bot_token, app_token,
            )
            team_id = auth.get("team_id", "")
            if self._team_id and team_id and team_id != self._team_id:
                await self._close_socket_quietly(client)
                raise RuntimeError(
                    "the new Slack credentials belong to a different workspace; "
                    "restart required",
                )

            old_client = self._client
            old_web = self._web
            old_bot_token = self._active_bot_token
            old_app_token = self._active_app_token
            old_bot_user_id = self._bot_user_id
            old_bot_id = self._bot_id
            old_team_id = self._team_id
            old_config = self._config
            old_state = self._state
            self._state = "rotating"

            try:
                await self._close_socket_for_replacement(old_client)
            except (Exception, asyncio.CancelledError):
                self._state = old_state
                await self._close_socket_quietly(client)
                raise

            # Publish before connect so an envelope arriving immediately after
            # the handshake sees the matching Web client and bot identity.
            self._activate_transport(
                web,
                client,
                auth,
                bot_token,
                app_token,
            )
            self._config = config
            # Names, dedupe keys, and message ids belong to the credential
            # generation. A failed rotation leaves these disposable caches empty.
            self._seen_events.clear()
            self._message_cache.clear()
            self._last_inbound_ts.clear()
            self._name_cache.clear()
            try:
                await self._connect_socket(client)
            except (Exception, asyncio.CancelledError) as connect_error:
                await self._close_socket_quietly(client)
                self._web = old_web
                self._active_bot_token = old_bot_token
                self._active_app_token = old_app_token
                self._bot_user_id = old_bot_user_id
                self._bot_id = old_bot_id
                self._team_id = old_team_id
                self._client = old_client
                self._config = old_config

                if old_web is not None and old_app_token:
                    try:
                        rollback = self._build_socket_client(
                            app_token=old_app_token, web_client=old_web,
                        )
                        self._client = rollback
                        await self._connect_socket(rollback)
                    except Exception as rollback_error:
                        self._state = "stopped"
                        self._stopping = True
                        self._web = None
                        self._client = None
                        logger.error(
                            "Slack credential rollback failed (%s)",
                            type(rollback_error).__name__,
                        )
                        raise RuntimeError(
                            "the new Slack credentials failed and the previous "
                            "connection could not be restored",
                        ) from connect_error
                    self._state = old_state
                    if isinstance(connect_error, asyncio.CancelledError):
                        raise
                    raise RuntimeError(
                        "the new Slack app token failed to connect; the previous "
                        "connection was restored",
                    ) from connect_error

                if isinstance(connect_error, asyncio.CancelledError):
                    raise
                self._state = "stopped"
                self._stopping = True
                raise RuntimeError(
                    "the new Slack app token failed to connect and no previous "
                    "connection was available",
                ) from connect_error

            self._last_event_time = time.monotonic()
            self._state = "running"
            self._log_auth(auth, "Slack credentials reloaded")

    def _announce_auth_state(self) -> None:
        """Log how access is configured — loudly when it lets nobody in.

        An unconfigured policy refuses every message. That is the safe
        default but an invisible one, so say it plainly at startup instead
        of leaving the operator to wonder why the bot never answers.
        """
        policy = self.policy
        if not policy.configured:
            logger.warning(
                "Slack: no slack.allow_users, slack.allow_channels, or "
                "slack.allow_direct_messages configured — every message will "
                "be refused. Add your Slack member id (Profile → ⋮ → Copy "
                "member ID) to slack.allow_users.",
            )
            return
        logger.info("Slack access policy: %s", policy.describe())

    async def stop(self, *, drain: bool = False) -> None:
        """Stop receiving, optionally draining acknowledged dispatches first."""
        self._stopping = True
        self._state = "quiescing"

        if drain:
            async with self._transport_lock:
                await self._close_socket_quietly(self._client)

        inflight = list(self._inflight)
        if drain and inflight:
            _, pending = await asyncio.wait(
                inflight, timeout=_STOP_DRAIN_TIMEOUT,
            )
            if pending:
                logger.warning(
                    "Slack: cancelling %d dispatch(es) after drain timeout",
                    len(pending),
                )
            for task in pending:
                task.cancel()
            await asyncio.gather(*inflight, return_exceptions=True)
        elif not drain:
            # Normal process shutdown stays prompt and closes the socket only
            # after dispatches stop touching the router and Web client.
            for task in inflight:
                task.cancel()
            if inflight:
                await asyncio.gather(*inflight, return_exceptions=True)
            async with self._transport_lock:
                await self._close_socket_quietly(self._client)

        self._web = None
        self._active_bot_token = ""
        self._active_app_token = ""
        self._bot_user_id = ""
        self._bot_id = ""
        self._team_id = ""
        self._client = None
        self._state = "stopped"

    # ------------------------------------------------------------------ #
    #  Watchdog                                                            #
    # ------------------------------------------------------------------ #

    async def transport_connected(self) -> bool:
        """Check whether the active Socket Mode client answers a ping."""
        if self._client is None or self._stopping:
            return False
        try:
            return bool(await self._client.is_connected())
        except Exception:
            return False

    @property
    def seconds_since_last_event(self) -> float:
        return time.monotonic() - self._last_event_time

    async def rebuild_transport(self) -> None:
        """Replace a disconnected socket after closing the old client.

        Connecting twice can leave competing Slack consumers, so a brief gap
        is safer than overlapping connections.
        """
        async with self._transport_lock:
            # The watchdog repairs outside the lifecycle lock, so a stop may
            # have started while it waited here. Connecting now would leave
            # a socket behind the channel that is going away.
            if self._stopping:
                return
            # A credential reload may have repaired the socket while the
            # watchdog was waiting for the transport lock.
            if self._client is not None:
                try:
                    if await self._client.is_connected():
                        return
                except Exception:
                    pass

            old = self._client
            await self._close_socket_for_replacement(old)
            client = self._build_socket_client(
                app_token=self._active_app_token, web_client=self._web,
            )
            self._client = client
            try:
                await self._connect_socket(client)
            except Exception:
                await self._close_socket_quietly(client)
                raise
            self._last_event_time = time.monotonic()

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

        if self._stopping:
            return

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

        A member edits ``display_name`` and ``real_name`` at will, so those
        reach the policy as self-set names that only a deny rule may match.
        The handle and the email are provisioned or verified, so a grant may
        rest on them.
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
                n for n in (user.get("name"), email) if n
            )
            self_set_names = tuple(
                n for n in (
                    profile.get("display_name"),
                    profile.get("real_name"),
                ) if n
            )
            complete = (
                bool(names or self_set_names)
                and (email is not None or not need_email)
            )
            if need_email and email is None:
                logger.warning(
                    "Slack users.info returned no email for %s — the deny list "
                    "names one, so the user is refused. Grant users:read.email "
                    "or write the deny rule against the handle or id instead.",
                    user_id,
                )
            identity = Identity(
                id=user_id,
                names=names,
                self_set_names=self_set_names,
                complete=complete,
            )
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
        """Build the Identity for a conversation."""
        if channel_type == "im" or channel_id.startswith("D"):
            return Identity(id=channel_id)
        if not resolve:
            return Identity(id=channel_id)
        cached = self._name_cache.get(f"c:{channel_id}")
        if cached and cached[1] > time.monotonic():
            return cached[0]
        try:
            info = await self._web.conversations_info(channel=channel_id)
            channel = info.get("channel") or {}
            if channel.get("is_im"):
                identity = Identity(id=channel_id)
            else:
                name = channel.get("name") or ""
                identity = Identity(
                    id=channel_id,
                    names=(name,) if name else (),
                    complete=bool(name),
                )
        except Exception as e:
            logger.warning(
                "Slack conversations.info failed for %s: %s",
                channel_id,
                e,
            )
            identity = Identity(id=channel_id, complete=False)
        self._remember(
            self._name_cache,
            f"c:{channel_id}",
            (identity, time.monotonic() + _NAME_CACHE_TTL),
            _NAME_CACHE_MAX,
        )
        return identity

    async def _authorize(
        self,
        user_id: str,
        channel_id: str,
        channel_type: str,
    ) -> bool:
        """Run the access policy for one event, logging any refusal."""
        policy = self.policy
        direct_message = channel_type == "im" or channel_id.startswith("D")
        early = policy.preflight(direct_message=direct_message)
        if early is not None:
            log = logger.warning if not policy.configured else logger.info
            log("Slack refused a message: %s", early.reason)
            return early.allowed

        user = await self._identify_user(
            user_id,
            needs_name_resolution(policy.users, is_id=is_slack_id),
            need_email=policy.users.any_deny_pattern(lambda p: "@" in p),
        )
        conversation = await self._identify_conversation(
            channel_id,
            channel_type,
            needs_name_resolution(policy.channels, is_id=is_slack_id),
        )
        verdict = policy.check(
            user, conversation, direct_message=direct_message,
        )
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
        """Whether this app posted the message."""
        if self._bot_id and event.get("bot_id") == self._bot_id:
            return True
        return bool(self._bot_user_id and event.get("user") == self._bot_user_id)

    async def _is_another_app_talking(self, event: dict[str, Any]) -> bool:
        """Whether another app authored this message itself.

        ``bot_id`` alone does not answer it: a person posting through an
        integration keeps their own ``user`` id and gains the app's
        ``bot_id``, and those messages are meant to reach the agent. What
        separates the two is whether the sender is a bot user, which only
        ``users.info`` can say.

        Answering another app is what lets two agents in one channel reply
        to each other without end, because a reply continues an owned thread
        with no further mention needed. An unresolved sender beside a
        ``bot_id`` is therefore treated as an app.
        """
        if not event.get("bot_id"):
            return False
        user_id = event.get("user") or ""
        if not user_id:
            return True

        cache_key = f"bot:{user_id}"
        cached = self._name_cache.get(cache_key)
        if cached and cached[1] > time.monotonic():
            return bool(cached[0])
        try:
            info = await self._web.users_info(user=user_id)
            is_bot = bool((info.get("user") or {}).get("is_bot"))
        except Exception as e:
            logger.warning(
                "Slack users.info failed for %s beside bot_id %s, so the "
                "message is treated as another app's: %s",
                user_id, event.get("bot_id"), e,
            )
            is_bot = True
        self._remember(
            self._name_cache, cache_key,
            (is_bot, time.monotonic() + _NAME_CACHE_TTL), _NAME_CACHE_MAX,
        )
        return is_bot

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

    @property
    def observation(self) -> ObservationPolicy:
        """The source grant, rebuilt per read so reloads apply at once."""
        source = self.config.slack.source
        return ObservationPolicy(
            enabled=source.enabled,
            conversations=PatternGate(
                "conversation",
                allow=list(source.allow_conversations),
                deny=list(source.deny_conversations),
            ),
            senders=PatternGate(
                "sender",
                allow=list(source.allow_senders),
                deny=list(source.deny_senders),
            ),
        )

    async def _observe(
        self,
        event: dict[str, Any],
        channel_id: str,
        user_id: str,
        ts: str,
        channel_key: str,
        handled: bool = False,
    ) -> None:
        """Buffer a message for the source inbox, if the source grant allows.

        Asked of every shared-channel message, whatever the live route
        decided: the two grants are independent, so being answered neither
        earns nor forfeits a place in the inbox. ``handled`` says the live
        route accepted this message, and by default that is where it stops —
        one message should not arrive twice. ``include_handled_messages``
        turns the copy back on for a source meant as a record.

        Private conversations are never a source. A DM the agent declined to
        answer is a refusal, and quietly filing it away is not what "we do not
        talk to you" led the sender to expect. That covers multi-person DMs,
        which arrive as ``channel_type="mpim"`` on a ``G`` id — checking only
        for ``D`` would file a group DM away as if it were a channel.

        Raw IDs are buffered and names are resolved only when a pattern needs
        one, so watching a busy channel costs no Slack API call per message.
        """
        # Cheapest gates first: this runs on every shared-channel message
        # now, not only the ones the live route declined.
        source = self.config.slack.source
        if not source.enabled:
            return
        if handled and not source.include_handled_messages:
            return
        policy = self.observation
        if not policy.active:
            return
        # The *raw* type, not the caller's derived one: that default turns an
        # absent type into "channel", which is exactly the ambiguity this has
        # to refuse rather than resolve.
        declared = event.get("channel_type") or ""
        if declared in ("im", "mpim") or channel_id.startswith("D"):
            return
        # A `G` is either a legacy private channel or a multi-person DM, and
        # only the declared type distinguishes them cheaply. Without one,
        # decline: the source is opt-in, so not recording something is
        # always the safe outcome.
        if channel_id.startswith("G") and declared not in ("channel", "group"):
            logger.debug(
                "Slack did not observe a message in %s: conversation type %r "
                "does not distinguish a private channel from a group DM",
                channel_id, declared or "unset",
            )
            return

        resolve = needs_name_resolution(
            policy.conversations, is_id=is_slack_id,
        )
        conversation = await self._identify_conversation(
            channel_id, "channel", resolve,
        )
        sender = await self._identify_user(
            user_id,
            needs_name_resolution(policy.senders, is_id=is_slack_id),
            need_email=policy.senders.any_deny_pattern(lambda p: "@" in p),
        )

        verdict = policy.check(conversation, sender)
        if not verdict.allowed:
            logger.debug("Slack did not observe a message: %s", verdict.reason)
            return

        observed = ObservedMessage(
            channel_name="slack",
            channel_key=channel_key,
            conversation_id=channel_id,
            sender_id=user_id,
            text=slack_to_plain(event.get("text") or "", self._bot_user_id),
            message_id=ts,
            timestamp=slack_ts_to_iso(ts),
            conversation_title=next(iter(conversation.names), ""),
            sender_name=next(
                iter((*sender.names, *sender.self_set_names)), "",
            ),
            # thread_ts lets a reader pull the parent later. A reply without
            # its thread is often meaningless, and expanding it here would
            # cost an API call per observation.
            metadata={
                "thread_ts": event.get("thread_ts") or "",
                "subtype": event.get("subtype") or "",
            },
        )
        await self.router.observe(
            observed,
            ttl_days=self.config.sync.message_ttl_days,
            max_stored_messages=source.max_stored_messages,
        )

    async def _handle_message_event(self, event: dict[str, Any]) -> None:
        """Turn a Slack message into an InboundMessage and hand it to the router."""
        if self._is_own_message(event):
            return
        subtype = event.get("subtype")
        if subtype in _IGNORED_SUBTYPES:
            return
        if await self._is_another_app_talking(event):
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
        # Shared channels are containers for thread sessions, never sessions
        # themselves. A top-level mention becomes its own thread root.
        thread_ts = None
        if channel_type != "im":
            thread_ts = event.get("thread_ts") or ts
        target = format_target(channel_id, thread_ts)
        channel_key = f"slack:{target}"

        # Two independent routes. Live handling needs the message to be
        # addressed to the agent *and* its sender authorized; the source
        # needs its own grant and neither of those. Both are asked, then
        # `handled` reconciles them, so an addressed message from someone
        # refused still reaches a source that asked for that channel.
        #
        # The authorize call stays behind the addressed check: it is the
        # expensive one, and running it for every remark in a busy channel
        # would cost a lookup and a refusal log line per message.
        handled = (
            await self._should_answer(event, channel_type, channel_key)
            and await self._authorize(user_id, channel_id, channel_type)
        )
        # Sits below the early returns above on purpose, so our own posts,
        # join/leave noise, and other apps never reach the inbox.
        await self._observe(
            event, channel_id, user_id, ts, channel_key, handled=handled,
        )
        if not handled:
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

        cached = self._message_cache.get(format_target(channel_id, ts))
        if not cached:
            # Only react to reactions on messages from this conversation that
            # we still hold context for; anything else has no session to
            # attach to and would open one from a stray emoji.
            return
        target, original_text = cached

        _, thread_ts = parse_target(target)
        if not channel_id.startswith("D") and thread_ts is None:
            # A shared channel has no conversation-wide session: each thread
            # owns one. A message cached at channel level, such as a
            # notification card, has no thread for a reaction to join, and
            # opening one would write a slack:<channel> mapping that the
            # session pickers deliberately do not list. A DM is one
            # conversation, so it has no thread to require.
            return

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
            token = self._active_bot_token or self.config.slack.bot_token
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(
                    url,
                    headers={"Authorization": f"Bearer {token}"},
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
                f"{size / 1024:.0f} KB"
                if size < 1_000_000
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
                    parts.append(
                        f"{meta}\n(Text file too large to inline — {size_str})"
                    )
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

            if ext == ".zip" or mime in (
                "application/zip",
                "application/x-zip-compressed",
            ):
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

    def _available_web(self) -> Any:
        """The Web client of the running generation, or refuse to use one.

        ``_web`` is set while starting and again while a rotation validates
        the next credential pair, so a caller that checks only the attribute
        can post through a client whose generation is not the one serving
        events. Refusing is what lets StreamAdapter keep the placeholder and
        the notification service record nothing as delivered.
        """
        if not self.is_available:
            raise SlackUnavailable(f"the Slack channel is {self._state}")
        return self._web

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
        web = self._available_web()
        channel_id, thread_ts = parse_target(target)
        resp = await web.chat_postMessage(
            channel=channel_id,
            text=text,
            blocks=blocks,
            thread_ts=thread_ts,
            unfurl_links=False,
            unfurl_media=False,
        )
        return resp.get("ts")

    async def authorize_outbound(self, target: str) -> Decision:
        """Whether an agent may post to *target* unprompted.

        The grant is ``slack.allow_channels`` read in the write direction: the
        agent may post to a conversation an operator already named. No new
        config keys, so writes cannot be widened by accident while reads are
        narrowed.

        Unsolicited direct messages are refused outright. An inbound DM comes
        from someone who chose to write; an outbound one does not, and
        ``allow_direct_messages`` was never asked to authorize a recipient the
        agent names for itself. Gating that properly means resolving the
        conversation's member and running it through ``policy.users``, which
        is a separate decision with its own test surface.

        This is deliberately stricter than :meth:`_notification_target`, which
        does accept a ``D``: that is an operator writing one config value, not
        an agent choosing a destination at runtime.

        The refusal reason returned here is deliberately coarse. It goes back
        to the agent, which may repeat it into a chat, and the detailed
        verdict names the resolved conversation and the pattern that matched
        — the same reasoning behind :meth:`SlackAccessPolicy.describe`. The
        detail goes to the log instead.
        """
        if not self.config.slack.allow_outbound:
            return Decision(
                False,
                "slack.allow_outbound is not enabled, so the agent may not "
                "post to a conversation it names",
            )

        channel_id, _ = parse_target(target)
        if not channel_id:
            return Decision(False, "no Slack conversation id in the target")

        if not is_slack_id(channel_id):
            return Decision(
                False,
                "target must be a Slack conversation id, not a name",
            )
        if channel_id[0] not in "CG":
            return Decision(
                False,
                "unsolicited direct messages are not supported; address a "
                "channel the policy allows instead"
                if channel_id[0] == "D"
                else f"{channel_id!r} is not a Slack conversation id",
            )

        # A `G` is ambiguous: legacy private channel or multi-person DM. Only
        # conversations.info can say which, so refusing `D` alone would let a
        # group DM through the door marked "no unsolicited DMs".
        if channel_id[0] == "G":
            private = await self._is_private_conversation(channel_id)
            if private is None:
                return Decision(
                    False,
                    f"could not establish what kind of conversation "
                    f"{channel_id} is",
                )
            if private:
                return Decision(
                    False,
                    "unsolicited direct messages are not supported; address a "
                    "channel the policy allows instead",
                )

        policy = self.policy
        if not policy.channels.allow:
            # Skip the lookup a refusal cannot use. Not redacted: naming an
            # unset config key tells the operator what to do and discloses
            # nothing about what is in it.
            return policy.check_outbound(Identity(id=channel_id))

        conversation = await self._identify_conversation(
            channel_id,
            "channel",
            needs_name_resolution(policy.channels, is_id=is_slack_id),
        )
        return self._public_verdict(policy.check_outbound(conversation))

    @staticmethod
    def _public_verdict(verdict: Decision) -> Decision:
        """Log the policy's detailed reason; hand back a coarse one.

        A refusal reason from :class:`PatternGate` names the conversation it
        resolved and the glob that matched it. That is what a log wants and
        the opposite of what should travel back to an agent that may be
        talking to whoever prompted the send.
        """
        if verdict.allowed:
            return verdict
        logger.info("Slack refused addressed delivery: %s", verdict.reason)
        return Decision(
            False, "the destination is not approved by the Slack channel policy",
        )

    async def _is_private_conversation(self, channel_id: str) -> bool | None:
        """Whether *channel_id* is a DM or multi-person DM.

        Returns None when Slack could not say, so the caller can fail closed
        rather than guess. Cached beside the resolved names, since the answer
        is a property of the conversation and does not change.
        """
        cache_key = f"kind:{channel_id}"
        cached = self._name_cache.get(cache_key)
        if cached and cached[1] > time.monotonic():
            return cached[0]
        try:
            info = await self._web.conversations_info(channel=channel_id)
            conversation = info.get("channel") or {}
            private = bool(
                conversation.get("is_mpim") or conversation.get("is_im"),
            )
        except Exception as e:
            logger.warning(
                "Slack conversations.info failed for %s, so its kind is "
                "unknown and delivery is refused: %s",
                channel_id, e,
            )
            return None
        self._remember(
            self._name_cache, cache_key,
            (private, time.monotonic() + _NAME_CACHE_TTL), _NAME_CACHE_MAX,
        )
        return private

    def _notification_target(self) -> str | None:
        """Resolve a concrete conversation from the active config generation."""
        configured = self.config.notifications.slack_channel_id.strip()
        if configured:
            if is_slack_id(configured) and configured[0] in "CGD":
                return configured
            logger.warning(
                "notifications.slack_channel_id is not a Slack conversation id",
            )
            return None

        for entry in self.config.slack.allow_channels:
            if is_slack_id(entry) and entry[0] in "CG":
                return entry
        logger.warning(
            "No notifications.slack_channel_id is set and slack.allow_channels "
            "has no literal conversation id",
        )
        return None

    async def post_notification(
        self,
        notification_id: str,
        text: str,
        options: list[tuple[str, str]] | None = None,
    ) -> tuple[str, str] | None:
        """Render and post one notification using the active Slack config."""
        if not self.is_available:
            return None
        target = self._notification_target()
        if not target:
            return None
        blocks = build_notification_blocks(text, notification_id, options)
        message_id = await self._post(target, text, blocks)
        if not message_id:
            return None
        self._cache_message(message_id, target, text)
        return target, message_id

    async def expire_notification(
        self,
        target: str,
        message_id: str,
        text: str,
    ) -> None:
        """Replace a notification card with its expired state."""
        if not self.is_available:
            return
        channel_id, _ = parse_target(target)
        try:
            await self._web.chat_update(
                channel=channel_id,
                ts=message_id,
                text=_md_to_slack(text),
                blocks=[],
            )
        except Exception as exc:
            logger.debug(
                "Slack expiry edit failed for %s: %s", message_id, exc,
            )

    async def send(self, message: OutboundMessage) -> None:
        """Send a complete message, split to fit Slack's render limit.

        Propagates a failure so StreamAdapter can fall back to editing the
        streaming placeholder; swallowing it loses the whole turn. An
        unavailable channel is one of those failures.
        """
        self._available_web()
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

    def _claim_channel_edit(self, channel_id: str) -> bool:
        """Whether a droppable edit may go out for this conversation now.

        Slack rate limits ``chat.update`` per conversation rather than per
        thread, so every thread streaming in one channel shares one budget.
        The SDK answers a 429 by sleeping inside the request, and the
        streaming listener is awaited from the agent's token loop, so those
        sleeps stall the run itself. Dropping the edit costs less: the next
        token brings another.
        """
        now = time.monotonic()
        if now - self._last_channel_edit.get(channel_id, 0.0) < EDIT_INTERVAL:
            return False
        self._remember(
            self._last_channel_edit, channel_id, now, _EDIT_CLOCK_MAX,
        )
        return True

    async def edit_message(
        self, target: str, message_id: str, text: str,
        *, throttle: bool = False,
    ) -> None:
        """Rewrite a previously sent message with the latest streamed text."""
        if not self.is_available:
            return
        channel_id, _ = parse_target(target)
        if throttle and not self._claim_channel_edit(channel_id):
            return
        body = _md_to_slack(text)
        if len(body) > MAX_MSG_LEN:
            body = body[:MAX_MSG_LEN] + "…"
        try:
            await self._web.chat_update(
                channel=channel_id, ts=message_id, text=body,
            )
        except Exception as e:
            logger.warning("Slack chat.update failed for %s: %s", target, e)
        self._cache_message(message_id, target, text)

    async def delete_message(self, target: str, message_id: str) -> None:
        """Remove a message — used to clear the streaming placeholder."""
        if not self.is_available:
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
        if not self.is_available:
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
        if not self.is_available:
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
        if not self.is_available or not target:
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
        """Store a message snippet in the LRU cache for reaction lookups.

        A Slack ts is unique inside one conversation rather than across the
        workspace, so the key carries the conversation too. The stored
        target keeps the thread, which the key does not.
        """
        snippet = (text or "")[:200]
        if not snippet:
            return
        channel_id, _ = parse_target(target)
        self._remember(
            self._message_cache,
            format_target(channel_id, ts),
            (target, snippet),
            _MESSAGE_CACHE_MAX,
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
                channel_id,
                user_id,
                "You are not authorized to use this bot.",
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

        # Slack slash commands carry no thread context. Commands that require
        # one exact conversation therefore remain DM-only.
        if sub in ("sessions", "new") and not self._has_slash_session_key(channel_id):
            await self._respond_ephemeral(
                channel_id,
                user_id,
                self._THREADED_CHANNEL_REFUSAL.format(sub=sub),
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
        "opens its own thread and session. Use `/nerve stop` to select a "
        "running thread, or use this command in a DM."
    )

    @staticmethod
    def _has_slash_session_key(channel_id: str) -> bool:
        """Whether a slash command names an exact conversation session."""
        return bool(channel_id and channel_id.startswith("D"))

    async def _cmd_new(
        self,
        channel_id: str,
        user_id: str,
        channel_key: str,
        args: list[str],
    ) -> None:
        prev = await self.router.get_last_session(channel_key)
        if prev:
            await self.router.stop_session(prev)
        title = " ".join(args) or None
        session_id = await self.router.create_session(
            channel_key,
            title=title,
            source="slack",
        )
        await self._respond_ephemeral(
            channel_id,
            user_id,
            f"New session `{session_id}`" + (f" — {title}" if title else ""),
        )

    async def _live_sessions_for_channel(self, channel_id: str) -> list[dict[str, Any]]:
        """Return live sessions in this exact Slack conversation."""
        rows = await self.router.list_conversation_sessions(f"slack:{channel_id}")
        matching: list[dict[str, Any]] = []
        for row in rows:
            key = row.get("channel_key") or ""
            if not key.startswith("slack:"):
                continue
            row_channel, thread_ts = parse_target(key[len("slack:") :])
            if row_channel != channel_id:
                continue
            if not channel_id.startswith("D") and thread_ts is None:
                continue
            matching.append({**row, "thread_ts": thread_ts})
        return matching

    @staticmethod
    def _session_choice_label(row: dict[str, Any]) -> str:
        """Button label naming one session, and the thread it belongs to."""
        title = (row.get("title") or "").strip() or row.get("session_id", "?")
        where = "in thread" if row.get("thread_ts") else "in conversation"
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
            channel_id,
            user_id,
            text="Which session should I stop?",
            blocks=self._session_picker_blocks(
                candidates,
                "Pick the one to stop:",
                "sessstop:",
                style="danger",
            ),
        )

    async def _stop_and_report(
        self,
        channel_id: str,
        user_id: str,
        row: dict[str, Any],
    ) -> None:
        """Stop one session and say which one, so the answer is checkable."""
        session_id = row["session_id"]
        stopped = await self.router.stop_session(session_id)
        where = "the thread" if row.get("thread_ts") else "this channel"
        await self._respond_ephemeral(
            channel_id,
            user_id,
            f"Stopped `{session_id}` in {where}."
            if stopped
            else f"`{session_id}` was not running.",
        )

    async def _cmd_star(
        self,
        channel_id: str,
        user_id: str,
        channel_key: str,
        starred: bool,
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
                channel_id,
                user_id,
                "Usage: `/nerve reply <your answer>`",
            )
            return
        if not self._notification_service:
            await self._respond_ephemeral(
                channel_id,
                user_id,
                "Notification service not available.",
            )
            return
        result = await self._notification_service.answer_latest_question(
            answer,
            channel="slack",
            target=channel_id,
            actor=user_id,
        )
        await self._respond_ephemeral(
            channel_id,
            user_id,
            f"Answer recorded for: {result['title']}"
            if result
            else "No pending questions in this conversation.",
        )

    async def _respond_ephemeral(
        self,
        channel_id: str,
        user_id: str,
        text: str,
    ) -> None:
        """Reply so only the person who ran the command sees it."""
        if not self.is_available:
            return
        try:
            await self._web.chat_postEphemeral(
                channel=channel_id,
                user=user_id,
                text=_md_to_slack(text),
            )
        except Exception as e:
            logger.warning("Slack chat.postEphemeral failed: %s", e)

    async def _respond_ephemeral_blocks(
        self,
        channel_id: str,
        user_id: str,
        text: str,
        blocks: list[dict],
    ) -> None:
        """Ephemeral reply carrying Block Kit, for the pickers."""
        if not self.is_available:
            return
        try:
            await self._web.chat_postEphemeral(
                channel=channel_id,
                user=user_id,
                text=text,
                blocks=blocks,
            )
        except Exception as e:
            logger.warning("Slack ephemeral blocks failed: %s", e)

    async def _send_sessions_view(
        self,
        channel_id: str,
        user_id: str,
        channel_key: str,
    ) -> None:
        """Post the session switcher, visible only to the requester."""
        if not self.is_available:
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
        # Both halves of the policy need a subject and a conversation. Slack
        # omits the conversation for interactions on a view surface, which
        # this app does not publish, so a press without one is refused rather
        # than run against half a policy.
        if not user_id or not channel_id:
            logger.warning(
                "Slack refused a %s press with no %s",
                action_id or "button",
                "sender" if not user_id else "conversation",
            )
            return

        channel_type = "im" if channel_id.startswith("D") else "channel"
        if not await self._authorize(user_id, channel_id, channel_type):
            return

        if action_id.startswith("sessstop:"):
            stopped = await self.router.stop_session(value)
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

        # Session cards are only actionable where Slack gives the interaction
        # an exact session key. Starring does not change the mapping.
        if not action_id.startswith("sessstar:") and not self._has_slash_session_key(
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
        # A card is posted at conversation level, so the target recorded for
        # it is the bare conversation. Slack fills in thread_ts on any
        # message that has replies, so carrying it across from the press
        # would stop matching that record the moment somebody replied under
        # the card, and every later press would read as already answered.
        target = format_target((payload.get("channel") or {}).get("id") or "")
        result = await self._notification_service.answer_delivered_notification(
            notification_id,
            answer,
            channel="slack",
            target=target,
            actor=actor,
        )
        if not result:
            await self._replace_via_url(
                response_url,
                "Already answered or expired.",
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
        snoozed_until = self._snoozed_until(result)
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

    @staticmethod
    def _snoozed_until(notification: dict[str, Any]) -> str | None:
        """Render the next delivery time when an answer snoozed the row."""
        try:
            if notification.get("status") != "pending" or not notification.get(
                "redeliver_at"
            ):
                return None
            from datetime import datetime

            dt = datetime.fromisoformat(notification["redeliver_at"])
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
