"""Which run of the daemon this is.

One random id, generated when this module is first imported — which in a
server is while it is starting — and constant for the life of the process.

It exists because "did the instance restart?" cannot otherwise be answered
from outside. The setup wizard ends in a restart and the browser has to know
when the *new* process is answering: polling for "is anything there" accepts
the old one, which is still serving while it shuts down, and would report a
restart that had not happened yet and settings that are not in force. A
generation that changes is the only honest signal.

It is also what tells the checklist's bookkeeping that a value written by an
earlier run has since been picked up: an entry recorded under a boot id that
is no longer this one describes a process that has already been replaced.

Opaque and random rather than a counter or a timestamp: it is published to
unauthenticated callers on ``/health``, where "this process has been up since
09:12" is a fact about the box that nobody needs, and where a counter would
say how often this instance falls over.
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
