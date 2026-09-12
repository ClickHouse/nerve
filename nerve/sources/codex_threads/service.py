"""Top-level lifecycle for the Codex thread sync.

One :class:`CodexThreadSyncService` owns N :class:`CodexOrigin` workers,
each running in its own asyncio Task. The service is wired into the
gateway lifespan so origins start with Nerve and stop cleanly on
shutdown.

Cursor persistence reuses the existing ``sync_cursors`` table — each
origin gets its own row keyed on ``codex:<origin_id>``. The service
saves the cursor after every **successfully ingested** event, so a crash
never loses more than one event of progress and a failure never skips one:
see :class:`_OriginWorker`.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from nerve.sources.codex_threads.base import (
    CodexOrigin,
    ThreadEvent,
    WorkspaceFilter,
)
from nerve.sources.codex_threads.ingester import CodexIngester
from nerve.sources.codex_threads.origins import LocalRolloutOrigin

if TYPE_CHECKING:
    from nerve.agent.streaming import StreamBroadcaster
    from nerve.config import (
        CodexOriginConfig,
        CodexSyncConfig,
        NerveConfig,
    )
    from nerve.db import Database

logger = logging.getLogger(__name__)


class _OriginWorker:
    """Pairs one origin with its ingester, and checkpoints what was ingested.

    The origin advances its own offset *before* it yields an event (so a
    consumer that stops mid-stream has a correct cursor), which means
    ``origin.cursor()`` during a failed ingest already covers work that was
    never persisted. Saving it there would skip the event permanently: the
    next scan starts past it and nothing ever replays it.

    So the worker keeps its own ``_checkpoint`` — the cursor as of the last
    event that actually landed — and that is the only value ever written. On
    the first failure it stops advancing for the rest of the run, because a
    later event's cursor also covers the failed one. Ingestion is idempotent
    (sessions are looked up first, messages are keyed on ``external_id``), so
    the replay a restart performs is safe, and re-doing work is the only
    outcome that cannot lose it.

    The trade-off, stated plainly: an event that fails *every* time stalls the
    checkpoint until it is dealt with, rather than being skipped silently. A
    stuck sync with an exception in the log is the better failure — the old
    behaviour dropped the event and left no trace but a gap.
    """

    def __init__(
        self,
        origin: CodexOrigin,
        ingester: CodexIngester,
        db: "Database",
    ) -> None:
        self.origin = origin
        self.ingester = ingester
        self.db = db
        self.task: asyncio.Task | None = None
        self.cursor_key = f"codex:{origin.id}"
        # The cursor covering only events this worker has ingested. None
        # until the run starts; never advanced past a failure.
        self._checkpoint: str | None = None
        self._stalled = False

    async def run(self) -> None:
        try:
            await self.origin.initialize()
        except NotImplementedError as e:
            logger.error(
                "Codex origin %s disabled — %s", self.origin.id, e,
            )
            return
        except Exception:
            logger.exception(
                "Codex origin %s failed to initialise", self.origin.id,
            )
            return

        cursor = await self.db.get_sync_cursor(self.cursor_key)
        # Start from where the last run left off; until an event lands, that
        # is also the furthest this run may checkpoint to.
        self._checkpoint = cursor
        self._stalled = False
        try:
            async for event in self.origin.stream(cursor):
                await self._handle(event)
        except asyncio.CancelledError:
            logger.info("Codex origin %s cancelled", self.origin.id)
            raise
        except Exception:
            logger.exception("Codex origin %s crashed", self.origin.id)
        finally:
            await self._save_cursor()
            try:
                await self.origin.close()
            except Exception:
                logger.exception("Codex origin %s close() failed", self.origin.id)

    async def _handle(self, event: ThreadEvent) -> None:
        try:
            await self.ingester.ingest(event)
        except Exception:
            logger.exception(
                "Codex ingest failed (origin=%s thread=%s seq=%d type=%s) — "
                "the cursor stays where it was, so a restart replays it",
                self.origin.id, event.thread_id, event.sequence, event.type,
            )
            self._stalled = True
            return
        # Checkpoint after every event that landed — cheap (one row update),
        # and now it means what it says.
        self._advance_checkpoint()
        await self._save_cursor()

    def _advance_checkpoint(self) -> None:
        """Take the origin's cursor as the new checkpoint, unless stalled."""
        if self._stalled:
            return
        try:
            self._checkpoint = self.origin.cursor()
        except Exception:
            logger.exception("Codex origin %s cursor() failed", self.origin.id)

    async def _save_cursor(self) -> None:
        if self._checkpoint is None:
            return
        try:
            await self.db.set_sync_cursor(self.cursor_key, self._checkpoint)
        except Exception:
            logger.exception(
                "Codex origin %s set_sync_cursor failed", self.origin.id,
            )


class CodexThreadSyncService:
    """Owner of the origin workers and their lifecycle."""

    def __init__(
        self,
        db: "Database",
        workers: list[_OriginWorker],
        *,
        broadcaster: "StreamBroadcaster | None" = None,
    ) -> None:
        self.db = db
        self._workers = workers
        self._broadcaster = broadcaster
        self._tasks: list[asyncio.Task] = []
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        for worker in self._workers:
            task = asyncio.create_task(worker.run(), name=f"codex-origin:{worker.origin.id}")
            worker.task = task
            self._tasks.append(task)
        logger.info(
            "Codex thread sync started (%d origin(s))", len(self._workers),
        )

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._started = False
        logger.info("Codex thread sync stopped")

    # ------------------------------------------------------------------
    # Diagnostics surface
    # ------------------------------------------------------------------

    def status(self) -> dict:
        """Per-origin health summary, safe to serialize over HTTP."""
        items = []
        for worker in self._workers:
            task = worker.task
            done = task.done() if task is not None else True
            cancelled = task.cancelled() if (task is not None and done) else False
            error: str | None = None
            if task is not None and done and not cancelled:
                exc = task.exception()
                if exc is not None:
                    error = repr(exc)
            items.append({
                "origin_id": worker.origin.id,
                "running": not done,
                "cancelled": cancelled,
                "error": error,
                "stats": dict(worker.ingester.stats),
            })
        return {
            "started": self._started,
            "origins": items,
        }


def build_service(
    config: "NerveConfig",
    db: "Database",
    *,
    broadcaster: "StreamBroadcaster | None" = None,
) -> CodexThreadSyncService | None:
    """Construct the service from a :class:`NerveConfig`.

    Returns ``None`` when the feature is disabled or no origins are
    configured. Failure to build a single origin is logged but does
    not prevent the rest from starting.
    """
    cfg: "CodexSyncConfig" = config.sync.codex
    if not cfg.enabled:
        return None
    if not cfg.origins:
        logger.warning("Codex thread sync enabled but no origins configured")
        return None

    workspace_filter = _build_workspace_filter(config)
    workers: list[_OriginWorker] = []
    for origin_cfg in cfg.origins:
        if not origin_cfg.enabled:
            continue
        try:
            origin = _build_origin(origin_cfg, workspace_filter)
        except ValueError as e:
            logger.error("Codex origin %s: %s — skipping", origin_cfg.id, e)
            continue
        ingester = CodexIngester(
            db,
            origin_id=origin_cfg.id,
            workspace_filter=workspace_filter,
            broadcaster=broadcaster,
            store_encrypted_reasoning=cfg.store_encrypted_reasoning,
        )
        workers.append(_OriginWorker(origin, ingester, db))

    if not workers:
        logger.warning("Codex thread sync: no enabled origins")
        return None

    return CodexThreadSyncService(db, workers, broadcaster=broadcaster)


def _build_workspace_filter(config: "NerveConfig") -> WorkspaceFilter:
    f = config.sync.codex.workspace_filter
    return WorkspaceFilter(
        mode=f.mode,                       # type: ignore[arg-type]
        nerve_workspace_path=config.workspace,
        explicit_paths=[Path(p) for p in f.explicit_paths],
    )


def _build_origin(
    cfg: "CodexOriginConfig", workspace_filter: WorkspaceFilter,
) -> CodexOrigin:
    if cfg.type == "local_rollout":
        return LocalRolloutOrigin(
            id=cfg.id,
            sessions_path=Path(cfg.path),
            archive_path=Path(cfg.archive_path),
            workspace_filter=workspace_filter,
            poll_interval_seconds=cfg.poll_interval_seconds,
        )
    if cfg.type == "app_server":
        # Import here so an unconfigured cloud/app_server origin doesn't
        # require optional deps to load the rest of the package.
        from nerve.sources.codex_threads.origins.app_server import AppServerOrigin
        return AppServerOrigin(
            id=cfg.id,
            transport=cfg.transport,
            workspace_filter=workspace_filter,
        )
    if cfg.type == "cloud":
        from nerve.sources.codex_threads.origins.cloud import CloudCodexOrigin
        return CloudCodexOrigin(
            id=cfg.id,
            transport=cfg.transport,
            workspace_filter=workspace_filter,
        )
    raise ValueError(f"unknown origin type {cfg.type!r}")
