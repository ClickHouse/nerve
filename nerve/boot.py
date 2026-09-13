"""Which run of the daemon this is.

One random id, generated when this module is first imported — which in a
server is while it is starting — and constant for the life of the process.

It exists so the setup checklist's bookkeeping can tell what an *earlier* run
wrote: an entry recorded under a boot id that is no longer this one describes
a process that has already been replaced, so the value it was about has since
been picked up or been undone by hand, and the live configuration is the
better witness either way. Without it a restart that has happened keeps being
reported as pending forever (see :mod:`nerve.setup_state`).

Opaque and random rather than a counter or a timestamp: it is not published to
anyone, but it does reach a state file on disk, and a counter there would say
how often this instance falls over. Nothing needs the ordering.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timezone

#: This process's generation. New on every start; never persisted.
BOOT_ID: str = secrets.token_hex(8)

#: When this process started, near enough — module import happens during
#: startup. Kept locally; not published.
STARTED_AT = datetime.now(timezone.utc)


def boot_id() -> str:
    """The current generation. A function so a test can monkeypatch the module."""
    return BOOT_ID
