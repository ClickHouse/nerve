"""Background sync service — keeps external-agent memory bundles fresh.

Runs as an ``asyncio`` task spawned in the gateway lifespan. Every
``sync_interval_minutes`` it walks the configured agents, asks each
one for its ``default_file_targets()``, and re-renders any whose
source files (SOUL.md, USER.md, MEMORY.md, ...) have changed since
the last write.

Two reasons this is a Python service rather than a YAML cron prompt:

1. **No LLM in the loop.** Spinning up a Claude session just to call
   one tool that concatenates files would burn tokens for no reason.
2. **Deterministic latency.** Cron prompts share an LLM worker pool;
   a backlog upstream would delay file syncs by minutes. A direct
   coroutine runs in O(1).

The service is also exposed via :func:`run_once` so the HTTP
``/api/external-agents/sync`` route and the bootstrap apply step can
both reuse the same logic.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from nerve.config import NerveConfig
from nerve.external_agents.registry import AGENT_REGISTRY, FileTarget
from nerve.external_agents.renderers import get_renderer
from nerve.external_agents.writer import ConfigWriter

logger = logging.getLogger(__name__)


@dataclass
class FileSyncStatus:
    """Per-output-file status reported back to the UI."""

    path: str
    hash: str = ""                          # short hex of latest render
    written_at: str | None = None           # ISO-8601 UTC
    skipped: bool = False                   # True when sidecar matched
    error: str | None = None


@dataclass
class AgentSyncStatus:
    """Per-agent status block returned by :meth:`SyncService.status`."""

    name: str
    display_name: str
    enabled: bool = True
    cli_installed: bool = False
    cli_version: str | None = None
    last_run_at: str | None = None
    last_error: str | None = None
    files: list[FileSyncStatus] = field(default_factory=list)


class SyncService:
    """Periodic memory-bundle sync for external agents.

    Lifecycle:

    - :meth:`start` spawns the background loop. Returns immediately.
    - :meth:`stop` cancels the loop and waits for it to finish.
    - :meth:`run_once` does one sweep synchronously — used by the
      manual ``/api/external-agents/sync`` route and by tests.

    Status is cached in ``_status`` so the HTTP endpoint can return it
    without doing any work — important because the diagnostics page
    polls it.
    """

    def __init__(self, config: NerveConfig) -> None:
        self._config = config
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._status: dict[str, AgentSyncStatus] = {}
        # Cached writer so allowlist checks aren't repeated per file.
        self._writer = ConfigWriter(
            conflict_policy=config.external_agents.conflict_policy,
        )

    # ---- Lifecycle -------------------------------------------------

    async def start(self) -> None:
        """Begin the background loop.

        Runs one immediate sweep so freshly-bootstrapped workspaces
        get their bundles populated without waiting for the first
        interval to elapse.
        """
        if self._task is not None and not self._task.done():
            logger.debug("SyncService already running — start() ignored.")
            return

        # Eager first sweep so the user sees bundle content right
        # after bootstrap. Errors logged, not raised — startup
        # mustn't fail because one renderer threw.
        try:
            await self.run_once()
        except Exception as e:  # pragma: no cover - defensive
            logger.exception("Initial external-agents sync failed: %s", e)

        self._stop_event.clear()
        self._task = asyncio.create_task(
            self._loop(), name="external-agents-sync-loop",
        )

    async def stop(self) -> None:
        """Stop the background loop and wait for the in-flight sweep."""
        self._stop_event.set()
        task = self._task
        if task is None:
            return
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
        self._task = None

    async def _loop(self) -> None:
        interval = max(
            60,
            self._config.external_agents.sync_interval_minutes * 60,
        )
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=interval,
                )
                # If wait() returned, stop was requested — exit.
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self.run_once()
            except Exception as e:  # pragma: no cover - defensive
                logger.exception("External-agents sync sweep failed: %s", e)

    # ---- Sweep -----------------------------------------------------

    async def run_once(self) -> dict[str, AgentSyncStatus]:
        """Run one sync sweep across all configured agents.

        Returns the fresh status map. Idempotent: if a target's
        rendered bundle hashes the same as the sidecar, the file is
        left alone.
        """
        workspace = self._config.workspace.expanduser().resolve()
        result: dict[str, AgentSyncStatus] = {}

        for target_cfg in self._config.external_agents.targets:
            agent = AGENT_REGISTRY.get(target_cfg.name)
            if agent is None:
                logger.warning(
                    "Unknown external agent in config: %s",
                    target_cfg.name,
                )
                continue

            status = AgentSyncStatus(
                name=agent.name,
                display_name=agent.display_name,
                enabled=target_cfg.enabled,
            )
            version = agent.smoke_check()
            status.cli_installed = version is not None
            status.cli_version = version
            status.last_run_at = datetime.now(timezone.utc).isoformat()

            if not target_cfg.enabled:
                result[agent.name] = status
                continue

            for target in agent.default_file_targets(workspace):
                status.files.append(
                    self._sync_one(target, workspace=workspace),
                )
            # Surface the first error so the UI has a top-level signal.
            for file_status in status.files:
                if file_status.error:
                    status.last_error = file_status.error
                    break

            result[agent.name] = status

        self._status = result
        return result

    def _sync_one(self, target: FileTarget, *, workspace: Path) -> FileSyncStatus:
        renderer = get_renderer(target.style)
        try:
            rendered = renderer.render(target, workspace=workspace)
        except Exception as e:
            logger.exception(
                "Renderer %s failed for %s: %s", target.style, target.output, e,
            )
            return FileSyncStatus(path=str(target.output), error=str(e))

        if self._writer.is_up_to_date(target.output, rendered):
            return FileSyncStatus(
                path=str(target.output),
                hash=_short_hash(rendered),
                skipped=True,
            )

        try:
            self._writer.write(target.output, rendered)
        except Exception as e:
            logger.exception("Failed to write %s: %s", target.output, e)
            return FileSyncStatus(path=str(target.output), error=str(e))

        return FileSyncStatus(
            path=str(target.output),
            hash=_short_hash(rendered),
            written_at=datetime.now(timezone.utc).isoformat(),
            skipped=False,
        )

    # ---- Status accessor ------------------------------------------

    def status(self) -> dict[str, AgentSyncStatus]:
        """Return the cached status map for HTTP/diagnostics consumers."""
        return self._status

    def status_for_api(self) -> dict[str, Any]:
        """Serializable variant of :meth:`status` for the REST route."""
        return {
            name: {
                "name": s.name,
                "display_name": s.display_name,
                "enabled": s.enabled,
                "cli_installed": s.cli_installed,
                "cli_version": s.cli_version,
                "last_run_at": s.last_run_at,
                "last_error": s.last_error,
                "files": [
                    {
                        "path": f.path,
                        "hash": f.hash,
                        "written_at": f.written_at,
                        "skipped": f.skipped,
                        "error": f.error,
                    }
                    for f in s.files
                ],
            }
            for name, s in self._status.items()
        }


def _short_hash(text: str) -> str:
    import hashlib
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
