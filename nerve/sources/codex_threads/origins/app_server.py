"""App-server origin — JSON-RPC over the local Unix socket or HTTP.

Codex exposes a local control socket at
``~/.codex/app-server-control/app-server-control.sock`` and (when run
as a daemon) a remote HTTP endpoint. Either flavour exposes the same
JSON-RPC protocol surface: ``thread/list``, ``thread/get``, and a
notification stream we subscribe to.

This origin is implemented as a thin scaffold today: it ships with
clear ``NotImplementedError`` raises so the service can fail with a
useful message if someone enables it before the protocol bindings
land. The LocalRolloutOrigin covers 100% of the Pi use case without
this path, so it's intentionally scoped down for the first PR.
"""

from __future__ import annotations

import logging
from typing import AsyncIterator

from nerve.sources.codex_threads.base import (
    CodexOrigin,
    ThreadEvent,
    WorkspaceFilter,
)

logger = logging.getLogger(__name__)


class AppServerOrigin(CodexOrigin):
    """Placeholder for the JSON-RPC origin.

    Once the protocol bindings exist, ``stream()`` should:

      1. Connect to the socket / HTTP endpoint.
      2. ``thread/list`` with workspace filtering at the API layer.
      3. ``thread/get`` to backfill any items we haven't ingested.
      4. Subscribe to the notification stream and translate live items
         into :class:`ThreadEvent`s.

    Until then, enabling this origin is an explicit configuration error.
    """

    def __init__(
        self,
        *,
        id: str,
        transport: dict,
        workspace_filter: WorkspaceFilter,
    ) -> None:
        self.id = id
        self.transport = transport
        self.filter = workspace_filter

    async def initialize(self) -> None:
        raise NotImplementedError(
            "AppServerOrigin is not implemented yet — use a local_rollout "
            "origin instead. Tracked in the Codex thread sync follow-up."
        )

    async def close(self) -> None:
        return

    async def stream(self, cursor: str | None) -> AsyncIterator[ThreadEvent]:
        raise NotImplementedError("AppServerOrigin.stream not implemented")
        yield  # pragma: no cover - to keep the type checker happy

    def cursor(self) -> str:
        return ""
