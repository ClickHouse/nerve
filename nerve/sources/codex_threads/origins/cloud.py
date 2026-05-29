"""Cloud Codex origin — placeholder for OpenAI's hosted Codex thread API.

OpenAI hasn't (as of this writing) shipped a public thread API for
cloud Codex sessions. This stub exists so the config schema is stable
and downstream callers can wire ``cloud`` origins now and have them
start working when the API lands.

Until then, enabling this origin is an explicit configuration error —
the service refuses to initialise rather than silently doing nothing.
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


class CloudCodexOrigin(CodexOrigin):
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
            "CloudCodexOrigin requires a public OpenAI Codex thread API "
            "that doesn't exist yet. Use a local_rollout origin instead."
        )

    async def close(self) -> None:
        return

    async def stream(self, cursor: str | None) -> AsyncIterator[ThreadEvent]:
        raise NotImplementedError("CloudCodexOrigin.stream not implemented")
        yield  # pragma: no cover

    def cursor(self) -> str:
        return ""
