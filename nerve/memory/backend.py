"""What every memory backend shares.

The memory backends live in their own modules; this one holds what belongs
to none of them, so a module can name it without loading memU's numpy
stack.
"""

from __future__ import annotations


class MemoryBackendUnavailable(RuntimeError):
    """Raised when a memory operation fails because the backend is
    transiently unavailable (an unreachable service, a timeout, HTTP 429/5xx,
    an auth blip) rather than because the query genuinely had no results.

    The tool layer surfaces this LOUDLY so a session never mistakes "memory
    is DOWN" for "no relevant memories", and the indexing sweep keeps its
    watermark so the window is offered again instead of skipped.
    """
