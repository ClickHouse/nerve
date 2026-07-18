"""Notification service — centralized fanout, answer routing, and persistence.

Coordinates between MCP tools (agent-side), channels (delivery), and the
answer routing mechanism (user-side). Supports fire-and-forget notifications,
async questions with multi-channel delivery (web UI + Telegram), and
``approval``-kind notifications that route to a server-side dispatcher when
the user picks an inline option (see ``nerve.notifications.handlers``).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from nerve.notifications import handlers as _handlers
from nerve.notifications.date_render import render_iso_dates

if TYPE_CHECKING:
    from nerve.agent.engine import AgentEngine
    from nerve.config import NerveConfig
    from nerve.db import Database

logger = logging.getLogger(__name__)


# Emoji decoration applied to the canonical approval decisions when we
# render their buttons. Keys are the option ``value`` strings; missing
# values fall back to the raw label.
_APPROVAL_EMOJIS: dict[str, str] = {
    "approve": "✅",       # white heavy check mark
    "decline": "❌",       # cross mark
    "snooze_24h": "\U0001F4A4",  # zzz
}


def _resolve_workspace(config: NerveConfig | None) -> Path | None:
    """Resolve the workspace directory.

    Mirrors ``handlers._resolve_workspace`` so the service can locate
    ``scripts/_mechanical_action.py`` for audit-log writes. Priority:
    ``$NERVE_WORKSPACE_PATH`` first (test override), then
    ``config.workspace``.
    """
    override = os.environ.get("NERVE_WORKSPACE_PATH")
    if override:
        return Path(override).expanduser()
    if config is not None and getattr(config, "workspace", None):
        return Path(config.workspace).expanduser()
    return None


def _load_mechanical_action_helper(workspace: Path):
    """Import the workspace's ``_mechanical_action`` helper by path.

    The helper is a workspace-side stdlib module, not part of the Nerve
    package, so we load it via ``importlib.util`` the same way the
    workspace scripts do. Cached on first load via a module-level dict
    so repeated approval answers do not re-spec the file each time.
    """
    cached = _HELPER_CACHE.get(str(workspace))
    if cached is not None:
        return cached

    import importlib.util

    helper_path = workspace / "scripts" / "_mechanical_action.py"
    if not helper_path.is_file():
        raise FileNotFoundError(
            f"mechanical-action helper not found at {helper_path}"
        )
    spec = importlib.util.spec_from_file_location(
        f"_mechanical_action__{abs(hash(str(workspace))) & 0xFFFFFF:06x}",
        helper_path,
    )
    if spec is None or spec.loader is None:
        raise ImportError(
            f"cannot build module spec for {helper_path}"
        )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _HELPER_CACHE[str(workspace)] = module
    return module


_HELPER_CACHE: dict[str, Any] = {}


class NotificationService:
    """Manages notification lifecycle: create, deliver, answer, route."""

    def __init__(self, config: NerveConfig, db: Database, engine: AgentEngine):
        self.config = config
        self.db = db
        self.engine = engine
        self._hide_session_label: set[str] = set()  # Session ID prefixes that suppress the label
        # In-memory cache of active silence rules (compiled regexes).
        # ``None`` = not loaded yet; rebuilt lazily and invalidated on any
        # silence mutation (tool / API). Silences are few (tens), so a
        # full reload on change is cheap.
        self._silence_cache: list[dict] | None = None
        self._silence_cache_lock = asyncio.Lock()

    def hide_session_label_for(self, session_prefix: str) -> None:
        """Register a session ID (or prefix) that should not show the session label."""
        self._hide_session_label.add(session_prefix)

    # ------------------------------------------------------------------ #
    #  Silence matching (deterministic, server-side alert suppression)     #
    # ------------------------------------------------------------------ #

    def invalidate_silence_cache(self) -> None:
        """Drop the compiled-rule cache so the next match reloads from DB.

        Called by the management tool and the REST routes after any
        add/remove so a new rule takes effect immediately.
        """
        self._silence_cache = None

    async def _get_silence_rules(self) -> list[dict]:
        """Return the cached list of compiled silence rules, loading lazily.

        Each entry is ``{id, pattern, regex, reason, action}``. A rule
        whose pattern fails to compile is dropped (logged) — a bad regex
        disables only that rule and never blocks delivery (fail-open).
        """
        if self._silence_cache is not None:
            return self._silence_cache
        async with self._silence_cache_lock:
            # Re-check under the lock: another coroutine may have loaded it.
            if self._silence_cache is not None:
                return self._silence_cache
            try:
                rows = await self.db.get_active_silences()
            except Exception as exc:  # defensive: never block delivery
                logger.warning("silence cache load failed: %s", exc)
                self._silence_cache = []
                return self._silence_cache
            compiled: list[dict] = []
            for row in rows:
                pattern = row.get("pattern") or ""
                try:
                    regex = re.compile(pattern, re.IGNORECASE)
                except re.error as exc:
                    logger.warning(
                        "silence %s has invalid regex %r: %s — skipping",
                        row.get("id"), pattern, exc,
                    )
                    continue
                compiled.append({
                    "id": row.get("id"),
                    "pattern": pattern,
                    "regex": regex,
                    "reason": row.get("reason") or "",
                    "action": row.get("action") or "silence",
                })
            self._silence_cache = compiled
            return compiled

    async def _match_silence(self, title: str, body: str) -> dict | None:
        """Return the first active silence rule matching ``title``+``body``.

        Matching is ``re.search`` (case-insensitive) over
        ``f"{title}\\n{body}"``. First rule wins (oldest-first order).
        Returns ``None`` if nothing matches. Never raises — a defensive
        try/except keeps a pathological rule from blocking delivery.
        """
        rules = await self._get_silence_rules()
        if not rules:
            return None
        haystack = f"{title}\n{body}"
        for rule in rules:
            try:
                if rule["regex"].search(haystack):
                    return rule
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning(
                    "silence %s match raised: %s", rule.get("id"), exc,
                )
                continue
        return None

    def _should_show_session_label(self, session_id: str) -> bool:
        """Check whether the session label should be appended to this notification."""
        for prefix in self._hide_session_label:
            if session_id == prefix or session_id.startswith(prefix + ":"):
                return False
        return True

    # ------------------------------------------------------------------ #
    #  Core API (called by MCP tools)                                      #
    # ------------------------------------------------------------------ #

    async def send_notification(
        self,
        session_id: str,
        title: str,
        body: str = "",
        priority: str = "normal",
        channels: list[str] | None = None,
        silent: bool = False,
        force: bool = False,
    ) -> str:
        """Fire-and-forget notification. Returns notification_id.

        Before delivery, the title+body is checked against active
        **silence** rules (deterministic suppression of known-benign
        alert classes — see :meth:`_match_silence`). A matched ``notify``
        is persisted with ``status='silenced'`` and **not delivered**;
        priority is never modified. The caller learns it was silenced via
        the notification metadata (and the tool layer surfaces the reason
        + pattern), and can re-send with ``force=True`` to bypass a match
        it judges incorrect.

        Silences apply to ``notify`` only — ``ask_question`` /
        ``propose_action`` go through their own paths and are never
        suppressed (a silenced question would hang its session forever).

        Args:
            channels: Override default notification channels (e.g. ["telegram"]).
            silent: If True, deliver without sound (Telegram disable_notification).
            force: If True, bypass silence matching and deliver normally.
                A forced send that *still* matched a rule is delivered but
                stamped + counted as an override for audit.
        """
        notification_id = f"notif-{uuid.uuid4().hex[:8]}"
        # Render <YYYY-MM-DD[ HH:MM]> placeholders before silence matching
        # so rules can target the human-readable form ("24 June") and so
        # persisted/delivered text matches what the user sees.
        locale = self.config.notifications.date_locale
        title = render_iso_dates(title, locale=locale)
        body = render_iso_dates(body, locale=locale)
        match = await self._match_silence(title, body)

        if match and not force:
            # --- SILENCE: record + persist, do NOT deliver ---
            hit_count = await self.db.record_silence_hit(match["id"])
            await self.db.create_notification(
                notification_id=notification_id,
                session_id=session_id,
                type="notify",
                title=title,
                body=body,
                priority=priority,            # priority UNCHANGED
                status="silenced",
                metadata={
                    "silenced_by": match["id"],
                    "silence_reason": match["reason"],
                    "silence_action": match["action"],
                    "silence_pattern": match["pattern"],
                    "silence_hit_count": hit_count,
                },
            )
            await self._broadcast_silenced_web(
                notification_id, session_id, title, body, priority, match,
            )
            return notification_id  # no _fanout → no telegram ping

        # --- DELIVER (normal, or force-override of a match) ---
        extra_meta: dict[str, Any] = {}
        if match and force:
            override_count = await self.db.record_silence_override(match["id"])
            extra_meta = {
                "force_sent_over_silence": match["id"],
                "force_override_count": override_count,
                "silence_pattern": match["pattern"],
            }
        await self.db.create_notification(
            notification_id=notification_id,
            session_id=session_id,
            type="notify",
            title=title,
            body=body,
            priority=priority,
            metadata=extra_meta or None,
        )

        await self._fanout(
            notification_id, session_id, "notify", title, body, priority,
            channels=channels, silent=silent,
        )

        return notification_id

    async def ask_question(
        self,
        session_id: str,
        title: str,
        body: str = "",
        options: list[str] | None = None,
        priority: str = "normal",
        expiry_hours: int | None = None,
    ) -> dict:
        """Pose a question to the user (always async).

        Returns immediately with notification_id. When the user answers,
        the answer is injected as a user message into the originating session.
        """
        notification_id = f"ask-{uuid.uuid4().hex[:8]}"
        title = render_iso_dates(title)
        body = render_iso_dates(body)
        hours = expiry_hours or self.config.notifications.default_expiry_hours
        expires_at = (
            datetime.now(timezone.utc) + timedelta(hours=hours)
        ).isoformat()

        await self.db.create_notification(
            notification_id=notification_id,
            session_id=session_id,
            type="question",
            title=title,
            body=body,
            priority=priority,
            options=options,
            expires_at=expires_at,
        )

        await self._fanout(
            notification_id, session_id, "question", title, body,
            priority, options=options,
        )

        return {"notification_id": notification_id, "status": "sent"}

    async def propose_action(
        self,
        session_id: str,
        target_kind: str,
        target_id: str,
        title: str,
        body: str = "",
        options: list[dict[str, str]] | None = None,
        priority: str = "high",
        expires_at: str | None = None,
        expiry_hours: int | None = None,
    ) -> dict:
        """File an actionable ``approval``-kind notification.

        ``target_kind`` + ``target_id`` route the user's answer through
        ``nerve.notifications.handlers`` instead of the question
        answer-injection path. ``options`` accepts a list of
        ``{"label": ..., "value": ...}`` dicts; when omitted, the
        dispatcher's canonical option set is used (Approve / Decline /
        Snooze 24h for the mechanical-action dispatcher).

        Returns ``{"notification_id": <id>, "status": "sent"}``.
        """
        notification_id = f"approval-{uuid.uuid4().hex[:8]}"
        title = render_iso_dates(title)
        body = render_iso_dates(body)

        # Resolve options. Default to the registered dispatcher's
        # canonical set when none was passed. Falling back to the
        # mechanical-action default keeps PR 1's only wired path
        # working without forcing the caller to recite the same triplet.
        if options is None:
            options = _handlers.default_approval_options()
        elif not options:
            raise ValueError("propose_action: options must not be empty")

        # Normalize: store as a list of value strings (matching the
        # existing question-kind contract) plus a parallel label map in
        # metadata so the Telegram + web layers can render the labels
        # without re-parsing options on every send.
        option_values = [str(opt["value"]) for opt in options]
        option_labels = {
            str(opt["value"]): str(opt.get("label", opt["value"]))
            for opt in options
        }

        if expires_at is None:
            hours = (
                expiry_hours
                or self.config.notifications.default_expiry_hours
            )
            expires_at = (
                datetime.now(timezone.utc) + timedelta(hours=hours)
            ).isoformat()

        await self.db.create_notification(
            notification_id=notification_id,
            session_id=session_id,
            type="approval",
            title=title,
            body=body,
            priority=priority,
            options=option_values,
            expires_at=expires_at,
            metadata={
                "target_kind": target_kind,
                "target_id": target_id,
                "option_labels": option_labels,
            },
            target_kind=target_kind,
            target_id=target_id,
        )

        await self._fanout(
            notification_id, session_id, "approval", title, body,
            priority, options=option_values, option_labels=option_labels,
        )

        return {"notification_id": notification_id, "status": "sent"}

    # ------------------------------------------------------------------ #
    #  Answer routing (called by REST API / Telegram callback)             #
    # ------------------------------------------------------------------ #

    async def handle_answer(
        self,
        notification_id: str,
        answer: str,
        answered_by: str,
    ) -> bool:
        """Process a user's answer to a question or approval.

        - For ``type=approval`` rows: look up the dispatcher in the
          handler registry, run it, audit-log the outcome, then flip
          the row's status. Snooze answers keep the row pending and
          stamp ``redeliver_at`` so the periodic maintenance tick
          (:meth:`redeliver_due`) fans it out again with a fresh card.
        - For ``type=question`` rows (legacy): persist the answer,
          inject it back into the originating session, broadcast.
        - Fire-and-forget ``type=notify`` rows do not flow through this
          method; the dismiss endpoint handles those.
        """
        notif = await self.db.get_notification(notification_id)
        if not notif or notif["status"] != "pending":
            return False

        if notif.get("type") == "approval":
            return await self._handle_approval_answer(
                notif, answer, answered_by,
            )

        success = await self.db.answer_notification(
            notification_id, answer, answered_by,
        )
        if not success:
            return False

        session_id = notif["session_id"]

        from nerve.agent.streaming import broadcaster

        # External (satellite) sessions are MCP-driven by an outside agent
        # (Codex, Claude Code, ...). Nerve doesn't own their conversation
        # loop, so injecting a user message and calling engine.run() would
        # spin up a stray native turn that the external agent never sees.
        # Mark the answer stored, broadcast to the UI, and stop.
        session_record = await self.db.get_session(session_id)
        is_external = bool(
            session_record and session_record.get("source") == "external"
        )

        if is_external:
            await broadcaster.broadcast("__global__", {
                "type": "notification_answered",
                "notification_id": notification_id,
                "session_id": session_id,
                "answer": answer,
                "answered_by": answered_by,
            })
            return True

        # Inject answer as user message into the session
        injected_message = f"[Answer to: {notif['title']}]\n\n{answer}"

        # Broadcast the injected user message to the chat UI
        await broadcaster.broadcast(session_id, {
            "type": "answer_injected",
            "session_id": session_id,
            "notification_id": notification_id,
            "title": notif["title"],
            "answer": answer,
            "answered_by": answered_by,
            "content": injected_message,
        })

        # Dispatch unconditionally — ``engine.run`` serializes per
        # session, so a mid-turn session picks the answer up when its
        # current turn finishes (FIFO), same pattern as the wakeup
        # dispatcher. The old ``is_running`` skip here silently dropped
        # answers that arrived while the session was busy.
        try:
            self._dispatch_into_session(
                session_id,
                injected_message,
                source=f"notification:{answered_by}",
                channel=answered_by,
            )
        except Exception as e:
            logger.error(
                "Failed to inject answer for %s into session %s: %s",
                notification_id, session_id, e,
            )

        # Broadcast answer event to web UI (notifications page)
        await broadcaster.broadcast("__global__", {
            "type": "notification_answered",
            "notification_id": notification_id,
            "session_id": session_id,
            "answer": answer,
            "answered_by": answered_by,
        })

        return True

    async def _handle_approval_answer(
        self,
        notif: dict[str, Any],
        answer: str,
        answered_by: str,
    ) -> bool:
        """Route an approval answer through the dispatcher registry."""
        notification_id = notif["id"]
        session_id = notif["session_id"]
        target_kind = notif.get("target_kind") or ""
        target_id = notif.get("target_id") or ""

        dispatcher = _handlers.get(target_kind) if target_kind else None
        if dispatcher is None:
            logger.warning(
                "approval %s has no dispatcher for target_kind=%r; "
                "marking answered without action",
                notification_id, target_kind,
            )
            result = _handlers.DispatchResult(
                ok=False,
                audit_event={
                    "event": "approval-acted",
                    "notification_id": notification_id,
                    "target_kind": target_kind,
                    "target_id": target_id,
                    "decision": answer,
                    "ok": False,
                    "error": (
                        f"no dispatcher registered for {target_kind!r}"
                    ),
                },
            )
        else:
            try:
                result = await asyncio.to_thread(
                    dispatcher, notif, target_id, answer, self.config,
                )
            except Exception as exc:  # defensive: never crash the route
                logger.exception(
                    "approval dispatch raised for %s (target=%s:%s, "
                    "decision=%s): %s",
                    notification_id, target_kind, target_id, answer, exc,
                )
                result = _handlers.DispatchResult(
                    ok=False,
                    audit_event={
                        "event": "approval-acted",
                        "notification_id": notification_id,
                        "target_kind": target_kind,
                        "target_id": target_id,
                        "decision": answer,
                        "ok": False,
                        "error": f"dispatcher raised: {exc}",
                    },
                )

        await self._append_approval_audit(result.audit_event)

        # Snooze keeps the row pending and stamps ``redeliver_at`` so
        # the periodic maintenance tick (:meth:`redeliver_due`) fans it
        # out again at the snooze time. ``expires_at`` moves past the
        # re-delivery point so the expiry sweep cannot kill the row
        # before it resurfaces.
        snoozed = result.snooze_until is not None and result.ok
        snooze_until = result.snooze_until
        if snoozed:
            try:
                snooze_dt = datetime.fromisoformat(snooze_until)
            except ValueError:
                # A dispatcher returned a malformed timestamp. Fall back
                # to a 24h snooze rather than crash the answer route
                # (which would leave the row pending with its Telegram
                # buttons already consumed — the exact silent-loss shape
                # this path exists to prevent).
                logger.warning(
                    "dispatcher returned malformed snooze_until %r for %s; "
                    "defaulting to +24h",
                    snooze_until, notification_id,
                )
                snooze_dt = datetime.now(timezone.utc) + timedelta(hours=24)
                snooze_until = snooze_dt.isoformat()
            expiry_hours = self.config.notifications.default_expiry_hours
            new_expires_at = (
                snooze_dt + timedelta(hours=expiry_hours)
            ).isoformat()
            await self.db.snooze_notification(
                notification_id, snooze_until, new_expires_at,
            )
        else:
            await self.db.answer_notification(
                notification_id, answer, answered_by,
            )

        from nerve.agent.streaming import broadcaster
        payload = {
            "type": "notification_answered",
            "notification_id": notification_id,
            "session_id": session_id,
            "answer": answer,
            "answered_by": answered_by,
            "approval_status": "snoozed" if snoozed else "answered",
            "dispatch_ok": result.ok,
        }
        if snoozed:
            payload["snooze_until"] = snooze_until
        await broadcaster.broadcast("__global__", payload)

        return True

    async def _append_approval_audit(self, event: dict[str, Any]) -> None:
        """Append an approval-lifecycle record to the mechanical-actions log.

        Uses the same audit log that the propose-mechanical-action
        primitive writes to (``~/.nerve/mechanical-actions/audit.jsonl``)
        so the proposal lifecycle (``proposed`` -> ``approval-acted``
        -> ``approved``/``declined``/``executed``, plus
        ``approval-expired`` when a card dies unanswered) is visible in
        one place. The shared helper module
        ``scripts/_mechanical_action.py`` lives under the workspace, so
        we import it dynamically by path rather than as a real Python
        package.
        """
        workspace = _resolve_workspace(self.config)
        if workspace is None:
            logger.debug(
                "approval audit: no workspace configured; event=%s",
                event.get("event"),
            )
            return
        try:
            helper = _load_mechanical_action_helper(workspace)
        except Exception as exc:  # defensive: never crash the route
            logger.warning(
                "approval audit: cannot load helper at %s: %s",
                workspace, exc,
            )
            return

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        # Re-use the helper's append path. Use a tag the helper hasn't
        # whitelisted yet by patching VALID_EVENTS just for our event,
        # so the helper validation stays strict for everything else.
        record = {"ts": ts, **event}
        valid = getattr(helper, "VALID_EVENTS", None)
        event_name = event.get("event")
        if isinstance(valid, set) and event_name and event_name not in valid:
            valid.add(event_name)

        try:
            await asyncio.to_thread(helper.append_audit, record, None)
        except Exception as exc:
            logger.warning(
                "approval audit append failed: %s (event=%s)",
                exc, event.get("event"),
            )

    def _dispatch_into_session(
        self,
        session_id: str,
        message: str,
        *,
        source: str,
        channel: str | None = None,
        internal: bool = False,
    ) -> None:
        """Fire-and-forget ``engine.run()`` into a session.

        Safe to call while the session is mid-turn: ``engine.run``
        holds a per-session lock, so the injected message waits behind
        any in-flight turn and runs when it finishes (FIFO) — the
        wakeup-dispatcher pattern. Never skip-on-busy here; that drops
        the message.
        """
        task = asyncio.create_task(
            self.engine.run(
                session_id=session_id,
                user_message=message,
                source=source,
                channel=channel,
                internal=internal,
            )
        )
        task.add_done_callback(self._on_answer_task_done)

    def _on_answer_task_done(self, task: asyncio.Task) -> None:
        """Log errors from answer injection tasks.

        Attached as a done_callback so exceptions from fire-and-forget
        asyncio.create_task() calls are surfaced instead of silently lost.
        """
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error("Answer injection task failed: %s", exc)

    async def handle_dismiss(self, notification_id: str) -> bool:
        """Dismiss a notification (no answer routing needed)."""
        notif = await self.db.get_notification(notification_id)
        if not notif or notif["status"] != "pending":
            return False

        await self.db.dismiss_notification(notification_id)
        return True

    # ------------------------------------------------------------------ #
    #  Fanout to channels                                                  #
    # ------------------------------------------------------------------ #

    async def _fanout(
        self,
        notification_id: str,
        session_id: str,
        notif_type: str,
        title: str,
        body: str,
        priority: str,
        options: list[str] | None = None,
        channels: list[str] | None = None,
        silent: bool = False,
        option_labels: dict[str, str] | None = None,
        extra_web: dict[str, Any] | None = None,
    ) -> None:
        """Deliver notification to all configured channels in parallel.

        ``option_labels`` is used by ``approval``-kind notifications: it
        maps the canonical option ``value`` (sent back on the callback
        as the answer string) to the human-facing label rendered on the
        button. ``None`` for the legacy ``question`` path, where the
        label and the value are identical.

        ``extra_web`` merges additional fields into the web broadcast
        payload only — the re-delivery tick uses it to flag
        ``redelivered: true`` so the UI can badge a resurfaced card.
        """
        target_channels = channels or self.config.notifications.channels

        async def _deliver(channel_name: str) -> str | None:
            """Deliver to a single channel, return name on success."""
            try:
                if channel_name == "web":
                    await self._deliver_web(
                        notification_id, session_id, notif_type,
                        title, body, priority, options,
                        option_labels=option_labels,
                        extra=extra_web,
                    )
                    return "web"
                elif channel_name == "telegram":
                    msg_id = await self._deliver_telegram(
                        notification_id, session_id, notif_type,
                        title, body, priority, options,
                        silent=silent, option_labels=option_labels,
                    )
                    if msg_id:
                        await self.db.update_notification(
                            notification_id,
                            telegram_message_id=str(msg_id),
                        )
                    return "telegram"
            except Exception as e:
                logger.error(
                    "Failed to deliver %s to %s: %s",
                    notification_id, channel_name, e,
                )
            return None

        results = await asyncio.gather(
            *(_deliver(ch) for ch in target_channels),
            return_exceptions=True,
        )
        channels_delivered = [r for r in results if isinstance(r, str)]

        await self.db.update_notification(
            notification_id,
            channels_delivered=json.dumps(channels_delivered),
        )

    async def _deliver_web(
        self,
        notification_id: str,
        session_id: str,
        notif_type: str,
        title: str,
        body: str,
        priority: str,
        options: list[str] | None,
        option_labels: dict[str, str] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """Broadcast notification to web UI via the global broadcaster.

        For approval-kind rows we also include ``option_labels`` so the
        web NotificationCard can render readable button text while the
        button click still sends the canonical ``value`` back through
        the answer endpoint. ``extra`` fields (e.g. ``redelivered``)
        are merged into the payload verbatim.
        """
        from nerve.agent.streaming import broadcaster
        message = {
            "type": "notification",
            "notification_id": notification_id,
            "notification_type": notif_type,
            "session_id": session_id,
            "title": title,
            "body": body,
            "priority": priority,
            "options": options,
        }
        if option_labels:
            message["option_labels"] = option_labels
        if extra:
            message.update(extra)
        await broadcaster.broadcast("__global__", message)

    async def _broadcast_silenced_web(
        self,
        notification_id: str,
        session_id: str,
        title: str,
        body: str,
        priority: str,
        match: dict | None = None,
    ) -> None:
        """Surface a silenced ``notify`` on the web UI without delivering it.

        Reuses the ``__global__`` notification event shape but flags
        ``silenced=True`` so the web client renders a greyed row and skips
        the toast/sound. We also stamp ``channels_delivered=["web"]`` so
        the row appears in the web notifications list (which filters by
        the ``web`` channel) — the suppression must stay *visible*, only
        the escalation is filtered.
        """
        from nerve.agent.streaming import broadcaster

        await self.db.update_notification(
            notification_id, channels_delivered=json.dumps(["web"]),
        )

        message: dict[str, Any] = {
            "type": "notification",
            "notification_id": notification_id,
            "notification_type": "notify",
            "session_id": session_id,
            "title": title,
            "body": body,
            "priority": priority,
            "options": None,
            "silenced": True,
        }
        if match:
            message["silence_reason"] = match.get("reason", "")
            message["silence_pattern"] = match.get("pattern", "")
            message["silenced_by"] = match.get("id", "")
        await broadcaster.broadcast("__global__", message)

    def _resolve_telegram_chat_id(self) -> int | None:
        """Resolve the Telegram chat ID for notification delivery."""
        chat_id = self.config.notifications.telegram_chat_id
        if chat_id:
            return chat_id
        allowed = self.config.telegram.allowed_users
        if allowed:
            return allowed[0]
        logger.warning("No telegram_chat_id configured for notifications")
        return None

    def _get_telegram_channel(self):
        """Get the TelegramChannel instance, or None if unavailable."""
        channel = self.engine.router.get_channel("telegram")
        if not channel or not hasattr(channel, '_app') or channel._app is None:
            return None
        return channel

    def _get_telegram_bot(self):
        """Get the Telegram bot instance, or None if unavailable."""
        channel = self._get_telegram_channel()
        if not channel:
            return None
        return channel._app.bot

    def _build_telegram_text(
        self, session_id: str, title: str, body: str, priority: str,
    ) -> str:
        """Compose the Telegram message text for a notification.

        Shared by the initial delivery, the re-delivery tick, and the
        expiry edit (which rebuilds the original text to append a
        status line).
        """
        priority_prefix = self.config.notifications.priority_prefixes.get(priority, "")
        if title:
            text = f"{priority_prefix}{title}"
            if body:
                text += f"\n\n{body}"
        else:
            text = body or ""
        if self._should_show_session_label(session_id):
            text += f"\n\nSession: {session_id}"
        return text

    async def _deliver_telegram(
        self,
        notification_id: str,
        session_id: str,
        notif_type: str,
        title: str,
        body: str,
        priority: str,
        options: list[str] | None,
        silent: bool = False,
        option_labels: dict[str, str] | None = None,
    ) -> str | None:
        """Send notification to Telegram, with inline keyboard for questions/approvals."""
        bot = self._get_telegram_bot()
        if not bot:
            logger.warning("Telegram bot not available for notification %s", notification_id)
            return None

        chat_id = self._resolve_telegram_chat_id()
        if not chat_id:
            return None

        text = self._build_telegram_text(session_id, title, body, priority)

        if notif_type in ("question", "approval") and options:
            button_labels: list[tuple[str, str]] = []
            for value in options:
                if notif_type == "approval":
                    label = (
                        (option_labels or {}).get(value)
                        or value.replace("_", " ").title()
                    )
                    emoji = _APPROVAL_EMOJIS.get(value, "")
                    rendered = f"{emoji} {label}".strip() if emoji else label
                else:
                    rendered = value
                button_labels.append((rendered, value))
            msg_id = await self._send_telegram_inline(
                chat_id, notification_id, text, button_labels, silent=silent,
            )
        else:
            msg = await self._send_telegram_html(bot, chat_id, text, silent=silent)
            msg_id = str(msg.message_id)

        # Cache for reaction context lookups
        if msg_id:
            channel = self._get_telegram_channel()
            if channel:
                channel._cache_message(int(msg_id), chat_id, text)

        return msg_id

    @staticmethod
    async def _send_telegram_html(
        bot: object,
        chat_id: int,
        text: str,
        *,
        reply_markup: object | None = None,
        silent: bool = False,
    ) -> object:
        """Send a message with markdown→HTML conversion and plain-text fallback."""
        from nerve.channels.telegram import _md_to_tg_html
        from telegram.constants import ParseMode

        html_text = _md_to_tg_html(text)
        try:
            return await bot.send_message(
                chat_id=chat_id, text=html_text,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
                disable_notification=silent,
            )
        except Exception:
            return await bot.send_message(
                chat_id=chat_id, text=text,
                reply_markup=reply_markup,
                disable_notification=silent,
            )

    async def _send_telegram_inline(
        self,
        chat_id: int,
        notification_id: str,
        text: str,
        options: list[str] | list[tuple[str, str]],
        silent: bool = False,
    ) -> str | None:
        """Send Telegram message with inline keyboard buttons.

        ``options`` accepts either a flat list of strings (legacy
        question kind: label == callback value) or a list of
        ``(label, value)`` tuples (approval kind: emoji-prefixed label,
        canonical value sent back on the callback).
        """
        bot = self._get_telegram_bot()
        if not bot:
            return None

        from telegram import InlineKeyboardButton, InlineKeyboardMarkup

        buttons = []
        for entry in options:
            if isinstance(entry, tuple):
                label, value = entry
            else:
                label = value = entry
            callback_data = f"notif:{notification_id}:{value}"
            # Telegram callback_data max 64 bytes — truncate option if needed
            if len(callback_data.encode("utf-8")) > 64:
                max_opt_len = 64 - len(f"notif:{notification_id}:".encode("utf-8"))
                truncated = value.encode("utf-8")[:max_opt_len].decode("utf-8", errors="ignore")
                callback_data = f"notif:{notification_id}:{truncated}"
            buttons.append([InlineKeyboardButton(label, callback_data=callback_data)])

        keyboard = InlineKeyboardMarkup(buttons)

        msg = await self._send_telegram_html(
            bot, chat_id, text, reply_markup=keyboard, silent=silent,
        )

        await self.db.update_notification(
            notification_id, telegram_chat_id=str(chat_id),
        )

        return str(msg.message_id)

    # ------------------------------------------------------------------ #
    #  Maintenance (called by the periodic background tick)                #
    # ------------------------------------------------------------------ #

    async def redeliver_due(self) -> int:
        """Re-fan-out pending rows whose ``redeliver_at`` has passed.

        This is the re-delivery tick that makes "Snooze 24h" a real
        round trip: the snoozed row resurfaces as a fresh Telegram card
        (new inline keyboard, new message id stored) and a fresh web
        broadcast flagged ``redelivered: true``. Each cycle bumps
        ``redelivery_count``; at ``config.notifications.max_redeliveries``
        the row expires (with reporting) instead of re-sending, so an
        auto-snoozing loop can't nag forever.

        Returns the number of notifications re-delivered.
        """
        rows = await self.db.get_due_redeliveries()
        if not rows:
            return 0

        max_redeliveries = self.config.notifications.max_redeliveries
        redelivered = 0
        capped: list[dict[str, Any]] = []

        for notif in rows:
            if (notif.get("redelivery_count") or 0) >= max_redeliveries:
                capped.append(notif)
                continue
            try:
                await self._redeliver_one(notif)
                redelivered += 1
            except Exception as exc:  # defensive: one bad row ≠ dead tick
                logger.error(
                    "re-delivery failed for %s: %s", notif.get("id"), exc,
                )

        if capped:
            expired: list[dict[str, Any]] = []
            for notif in capped:
                if await self.db.expire_notification(notif["id"]):
                    row = dict(notif)
                    row["status"] = "expired"
                    expired.append(row)
            logger.info(
                "%d snoozed notification(s) hit the re-delivery cap (%d) "
                "and expired: %s",
                len(expired), max_redeliveries,
                ", ".join(r["id"] for r in expired),
            )
            await self._report_expired(expired)

        return redelivered

    async def _redeliver_one(self, notif: dict[str, Any]) -> None:
        """Fan a single snoozed row back out to its channels."""
        notification_id = notif["id"]
        options = json.loads(notif["options"]) if notif.get("options") else None
        try:
            metadata = json.loads(notif["metadata"]) if notif.get("metadata") else {}
        except (TypeError, ValueError):
            metadata = {}
        option_labels = metadata.get("option_labels") or None
        new_count = (notif.get("redelivery_count") or 0) + 1

        # Restart the expiry window: a resurfaced card needs a full
        # answering window, and (edge case) a row whose original expiry
        # already passed must not be reaped by the expire pass of the
        # very sweep that just re-delivered it.
        new_expires_at = (
            datetime.now(timezone.utc)
            + timedelta(hours=self.config.notifications.default_expiry_hours)
        ).isoformat()

        # Bump the counter *before* the fanout so a mid-fanout crash
        # can't replay the send every 15 minutes.
        marked = await self.db.mark_notification_redelivered(
            notification_id, new_expires_at,
        )
        if not marked:
            return  # answered/expired between select and mark

        logger.info(
            "Re-delivering snoozed notification %s (cycle %d)",
            notification_id, new_count,
        )
        await self._fanout(
            notification_id,
            notif["session_id"],
            notif["type"],
            notif.get("title") or "",
            notif.get("body") or "",
            notif.get("priority") or "normal",
            options=options,
            option_labels=option_labels,
            extra_web={"redelivered": True, "redelivery_count": new_count},
        )

    async def expire_stale(self) -> int:
        """Expire pending notifications past their expiry time.

        Unlike the original blind status flip, every expired
        ``question``/``approval`` is reported: the asking session gets
        an internal note (questions), the mechanical-actions audit log
        gets an ``approval-expired`` event (approvals), the web UI gets
        a ``notification_expired`` broadcast, and the Telegram card is
        edited to show it expired. ``notify``-kind expiry stays silent.
        """
        rows = await self.db.expire_due_notifications()
        if rows:
            await self._report_expired(rows)
        return len(rows)

    async def _report_expired(self, rows: list[dict[str, Any]]) -> None:
        """Report expired questions/approvals to every interested party.

        Silent-by-construction expiry was the bug: the asking session
        believed the user simply never replied, and the user's card just
        vanished. Each reporting leg is individually best-effort so one
        failure (e.g. a Telegram edit on an old message) never blocks
        the others.
        """
        reportable = [
            r for r in rows if r.get("type") in ("question", "approval")
        ]
        if not reportable:
            return

        from nerve.agent.streaming import broadcaster

        for notif in reportable:
            # Web: gray the card live.
            try:
                await broadcaster.broadcast("__global__", {
                    "type": "notification_expired",
                    "notification_id": notif["id"],
                    "session_id": notif["session_id"],
                    "notification_type": notif["type"],
                    "title": notif.get("title") or "",
                })
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning(
                    "expiry broadcast failed for %s: %s", notif["id"], exc,
                )

            # Telegram: mark the card expired, drop dead buttons.
            await self._edit_telegram_expired(notif)

            # Approvals: the proposer is the mechanical pipeline, not a
            # conversation — record the expiry in its audit log.
            if notif.get("type") == "approval":
                await self._append_approval_audit({
                    "event": "approval-expired",
                    "notification_id": notif["id"],
                    "target_kind": notif.get("target_kind") or "",
                    "target_id": notif.get("target_id") or "",
                    "redelivery_count": notif.get("redelivery_count") or 0,
                })

        # Questions: tell the asking session its question died, so the
        # agent can adapt (re-ask, escalate, or record "no decision").
        # Batched per session — one injection per sweep, not per row.
        questions = [r for r in reportable if r.get("type") == "question"]
        by_session: dict[str, list[dict[str, Any]]] = {}
        for notif in questions:
            by_session.setdefault(notif["session_id"], []).append(notif)
        for session_id, session_rows in by_session.items():
            await self._inject_expiry_note(session_id, session_rows)

    async def _inject_expiry_note(
        self, session_id: str, rows: list[dict[str, Any]],
    ) -> None:
        """Inject an expired-unanswered note into the asking session.

        Dispatched unconditionally — ``engine.run`` serializes per
        session, so a mid-turn session just processes the note when its
        current turn finishes (same pattern as the wakeup dispatcher
        and ``handle_answer``). Skipped for external (satellite)
        sessions, whose conversation loop Nerve doesn't own, and
        archived/missing sessions — those got the web broadcast and
        nothing more.
        """
        try:
            session = await self.db.get_session(session_id)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "expiry injection: session lookup failed for %s: %s",
                session_id, exc,
            )
            return
        if not session:
            logger.info(
                "expiry injection: session %s missing — broadcast only",
                session_id,
            )
            return
        if session.get("status") == "archived":
            logger.info(
                "expiry injection: session %s archived — broadcast only",
                session_id,
            )
            return
        if session.get("source") == "external":
            logger.info(
                "expiry injection: session %s is external — broadcast only",
                session_id,
            )
            return

        if len(rows) == 1:
            message = (
                f"[Question expired unanswered: {rows[0].get('title') or ''}]"
            )
        else:
            titles = "\n".join(
                f"- {r.get('title') or ''}" for r in rows
            )
            message = f"[Questions expired unanswered]\n{titles}"

        self._dispatch_into_session(
            session_id,
            message,
            source="notification:expiry",
            internal=True,
        )

    async def _edit_telegram_expired(self, notif: dict[str, Any]) -> None:
        """Best-effort edit of the Telegram card to show it expired.

        Rebuilds the original message text from the row (same builder
        the delivery used) and appends the status line, dropping the
        now-dead inline keyboard. Telegram refuses edits on old
        messages (>48h) — all failures are swallowed by design.
        """
        message_id = notif.get("telegram_message_id")
        if not message_id:
            return
        bot = self._get_telegram_bot()
        if not bot:
            return
        chat_id = notif.get("telegram_chat_id") or self._resolve_telegram_chat_id()
        if not chat_id:
            return

        text = self._build_telegram_text(
            notif["session_id"],
            notif.get("title") or "",
            notif.get("body") or "",
            notif.get("priority") or "normal",
        )
        text += "\n\n⏰ Expired unanswered"

        from nerve.channels.telegram import _md_to_tg_html
        from telegram.constants import ParseMode

        try:
            await bot.edit_message_text(
                chat_id=int(chat_id), message_id=int(message_id),
                text=_md_to_tg_html(text), parse_mode=ParseMode.HTML,
            )
        except Exception:
            try:
                await bot.edit_message_text(
                    chat_id=int(chat_id), message_id=int(message_id),
                    text=text,
                )
            except Exception as exc:
                logger.debug(
                    "telegram expiry edit failed for %s: %s",
                    notif["id"], exc,
                )
