"""A Claude CLI transport that declares host-only ``initialize`` flags.

The CLI decides at its first ``initialize`` control request whether an
interrupt spares background agents (``perTaskStopAffordance``) and whether it
writes a one-line progress summary per running agent
(``agentProgressSummaries``). The Python SDK has no option for either, so
:class:`NerveCLITransport` adds them to that one frame on its way to the
CLI's stdin. Every other frame passes through byte for byte.

Private SDK seams, pinned by ``tests/test_claude_transport.py``:

* ``SubprocessCLITransport`` (the SDK's default transport) is subclassed;
  ``ClaudeSDKClient(transport=...)`` uses a custom transport as-is.
* The initialize request is written as one
  ``transport.write(json.dumps(control_request) + "\\n")`` call
  (``Query._send_control_request``), so the frame is one JSON line that
  contains the ``"control_request"`` and ``"initialize"`` substrings.
* ``_configure_can_use_tool`` builds the options copy the transport needs
  (``permission_prompt_tool_name="stdio"`` stays on the command line).

When a seam moves, nothing is patched: :attr:`NerveCLITransport.init_patched`
stays False, ``ClaudeClient.interrupt_spares_background`` reports False, and
Stop asks before it would end background agents instead of killing them.
"""

from __future__ import annotations

import json
import logging
from typing import Any, AsyncIterator

from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport

logger = logging.getLogger(__name__)


async def _never_yields() -> AsyncIterator[dict[str, Any]]:
    """An empty prompt stream (the SDK's own ``_empty_stream``): streaming
    mode never reads it for the command line, and turns go through
    ``ClaudeSDKClient.query``."""
    return
    yield {}  # pragma: no cover - makes this an async generator


class NerveCLITransport(SubprocessCLITransport):
    """Adds host-only initialize flags the Python SDK cannot express
    (perTaskStopAffordance, agentProgressSummaries). Patches only the first
    initialize control_request; every other frame passes through byte for
    byte."""

    def __init__(self, prompt: Any, options: Any, *, init_extras: dict[str, Any]):
        super().__init__(prompt=prompt, options=options)
        self.init_extras = dict(init_extras)
        self.init_patched = False

    async def write(self, data: str) -> None:
        if (
            not self.init_patched
            and isinstance(data, str)
            and '"control_request"' in data
            and '"initialize"' in data
        ):
            try:
                msg = json.loads(data)
                req = msg.get("request") if isinstance(msg, dict) else None
                if (
                    isinstance(msg, dict)
                    and msg.get("type") == "control_request"
                    and isinstance(req, dict)
                    and req.get("subtype") == "initialize"
                ):
                    for key, value in self.init_extras.items():
                        req.setdefault(key, value)
                    data = json.dumps(msg) + "\n"
                    self.init_patched = True
                    logger.info(
                        "claude transport: initialize patched with %s",
                        sorted(self.init_extras),
                    )
            except (ValueError, TypeError):
                logger.warning(
                    "claude transport: initialize frame not parseable; sent unpatched",
                )
        await super().write(data)
